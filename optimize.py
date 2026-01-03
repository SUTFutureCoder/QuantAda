import argparse
import ast
import copy
import importlib
import os
import re
import sys
import logging
import datetime

import optuna
import optuna.visualization as vis
import pandas as pd
import backtrader as bt

# 需要同时导入 BacktraderStrategyWrapper 用于 QuietBacktester
from backtest.backtester import Backtester, BacktraderStrategyWrapper
from data_providers.manager import DataManager

import config

# 关闭日志
config.LOG = False
logging.getLogger("optuna").setLevel(logging.WARNING)

# 复用 run.py 的环境设置
python_install_dir = os.path.dirname(os.path.dirname(os.__file__))
tcl_library_path = os.path.join(python_install_dir, 'tcl', 'tcl8.6')
tk_library_path = os.path.join(python_install_dir, 'tcl', 'tk8.6')
os.environ['TCL_LIBRARY'] = tcl_library_path
os.environ['TK_LIBRARY'] = tk_library_path


# --- 辅助函数 ---
def _pascal_to_snake(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_class_from_name(name_string: str, search_paths: list):
    """(同 run.py 逻辑)"""
    name_string = name_string.replace('.py', '')
    if '.' in name_string:
        try:
            module_path, class_name = name_string.rsplit('.', 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError, ValueError):
            try:
                module_name = name_string
                class_name_base = module_name.split('.')[-1]
                class_name = "".join(word.capitalize() for word in class_name_base.split('_'))
                module = importlib.import_module(module_name)
                return getattr(module, class_name)
            except (ImportError, AttributeError) as e:
                raise ImportError(f"Could not import '{name_string}': {e}")

    if '_' in name_string or name_string.islower():
        module_name = name_string
        class_name = "".join(word.capitalize() for word in module_name.split('_'))
    else:
        class_name = name_string
        module_name = _pascal_to_snake(class_name)

    for path in search_paths:
        try:
            module_path = f'{path}.{module_name}'
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError):
            continue
    raise ImportError(f"Could not find class '{class_name}' in paths: {search_paths}")


def prepare_data_index(df: pd.DataFrame) -> pd.DataFrame:
    """确保 DataFrame 的索引是 DatetimeIndex"""
    if isinstance(df.index, pd.DatetimeIndex):
        return df

    date_cols = ['date', 'datetime', 'trade_date', 'Date', 'Datetime']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
            df.set_index(col, inplace=True)
            return df

    try:
        df.index = pd.to_datetime(df.index)
        return df
    except:
        pass
    return df


def slice_datas(datas: dict, start_date: str, end_date: str):
    """根据日期切分数据字典"""
    sliced = {}
    if not start_date and not end_date:
        return datas

    s = pd.to_datetime(start_date) if start_date else pd.Timestamp.min
    e = pd.to_datetime(end_date) if end_date else pd.Timestamp.max

    for symbol, df in datas.items():
        df = prepare_data_index(df)
        try:
            mask = (df.index >= s) & (df.index <= e)
            sub_df = df.loc[mask]
            if not sub_df.empty:
                sliced[symbol] = sub_df
            else:
                pass
        except Exception as e:
            print(f"Error slicing data for {symbol}: {e}")

    return sliced


# --- 核心修复：安静的回测器 (支持 Calmar/Sortino) ---
class QuietBacktester(Backtester):
    """
    专门用于优化过程的回测器。
    重写 run 方法，只执行配置和计算，不执行 display_results (打印日志) 和 plot (弹窗)。
    """

    def run(self):
        # 1. 添加数据
        for symbol, df in self.datas.items():
            feed = bt.feeds.PandasData(
                dataname=df,
                fromdate=pd.to_datetime(self.start_date),
                todate=pd.to_datetime(self.end_date),
                name=symbol,
                timeframe=self.timeframe,
                compression=self.compression
            )
            self.cerebro.adddata(feed)

        # 2. 添加策略
        self.cerebro.addstrategy(
            BacktraderStrategyWrapper,
            strategy_class=self.strategy_class,
            params=self.params,
            risk_control_class=self.risk_control_class,
            risk_control_params=self.risk_control_params
        )

        # 3. 资金与手续费
        self.cerebro.broker.setcash(self.cash)
        self.cerebro.broker.setcommission(commission=self.commission)

        # 4. Sizer
        if self.sizer_class:
            self.cerebro.addsizer(self.sizer_class, **(self.sizer_params or {}))

        # 5. 添加分析器 (核心：加入 DrawDown 用于计算 Calmar)
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.0,
                                 timeframe=self.timeframe, compression=self.compression)
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        # 如果需要更高级的，可以加 VWR (Volatility Weighted Return)
        # self.cerebro.addanalyzer(bt.analyzers.VWR, _name='vwr')

        # 6. 仅运行计算
        self.results = self.cerebro.run()
        return self.results

    def get_custom_metric(self, metric_name):
        """
        专门计算优化所需的指标，包含 Calmar
        """
        if not self.results: return -999.0
        strat = self.results[0]

        if metric_name == 'sharpe':
            s = strat.analyzers.sharpe.get_analysis().get('sharperatio')
            return s if s is not None else -999.0

        elif metric_name == 'return':
            # 总收益率
            init_cash = self.cash
            final_cash = self.cerebro.broker.getvalue()
            return (final_cash - init_cash) / init_cash

        elif metric_name == 'calmar':
            # Calmar = 年化收益 / 最大回撤
            # 1. 获取年化收益 (近似值)
            init_cash = self.cash
            final_cash = self.cerebro.broker.getvalue()

            # 计算天数
            start_dt = pd.to_datetime(self.start_date) if self.start_date else pd.to_datetime('20000101')
            end_dt = pd.to_datetime(self.end_date) if self.end_date else datetime.datetime.now()
            days = (end_dt - start_dt).days
            if days <= 0: days = 1

            total_ret = (final_cash / init_cash) - 1
            annual_ret = (1 + total_ret) ** (365.0 / days) - 1

            # 2. 获取最大回撤 (百分比，如 10% -> 0.1)
            dd_stats = strat.analyzers.drawdown.get_analysis()
            max_dd = dd_stats.get('max', {}).get('drawdown', 0) / 100.0  # backtrader返回的是0-100的数

            if max_dd == 0:
                return annual_ret * 100  # 如果没有回撤，直接返回放大的收益率作为分数

            return annual_ret / abs(max_dd)

        elif metric_name == 'final_value':
            return self.cerebro.broker.getvalue()

        return 0.0


# --- 优化任务 ---

class OptimizationJob:
    def __init__(self, args, fixed_params, opt_params_def, risk_params):
        self.args = args
        self.fixed_params = fixed_params
        self.opt_params_def = opt_params_def
        self.risk_params = risk_params
        self.strategy_class = get_class_from_name(args.strategy, ['strategies'])
        self.risk_control_class = None
        if args.risk:
            self.risk_control_class = get_class_from_name(args.risk, ['risk_controls', 'strategies'])

        self.data_manager = DataManager()
        self.raw_datas = self._fetch_all_data()
        self.train_datas, self.test_datas, self.train_range, self.test_range = self._split_data()

        self.has_debugged_data = False

    def _fetch_all_data(self):
        print("\n--- Fetching Data for Optimization ---")
        req_start = self.args.start_date
        req_end = self.args.end_date

        if self.args.train_period and self.args.test_period:
            # 自动计算覆盖整个训练+测试的日期范围
            train_s, train_e = self.args.train_period.split('-')
            test_s, test_e = self.args.test_period.split('-')
            dates = [pd.to_datetime(d) for d in [train_s, train_e, test_s, test_e]]
            req_start = min(dates).strftime('%Y%m%d')
            req_end = max(dates).strftime('%Y%m%d')

        symbol_list = [s.strip() for s in self.args.symbols.split(',')]

        datas = {}
        for symbol in symbol_list:
            df = self.data_manager.get_data(
                symbol,
                start_date=req_start,
                end_date=req_end,
                specified_sources=self.args.data_source,
                timeframe=self.args.timeframe,
                compression=self.args.compression
            )
            if df is not None and not df.empty:
                datas[symbol] = df

        if not datas:
            raise ValueError("No data fetched. Check symbols or date range.")
        return datas

    def _split_data(self):
        if self.args.train_period and self.args.test_period:
            tr_s, tr_e = self.args.train_period.split('-')
            te_s, te_e = self.args.test_period.split('-')

            print(f"Split Mode: Explicit Period")
            print(f"  Train: {tr_s} -> {tr_e}")
            print(f"  Test:  {te_s} -> {te_e}")

            train_d = slice_datas(self.raw_datas, tr_s, tr_e)
            test_d = slice_datas(self.raw_datas, te_s, te_e)
            return train_d, test_d, (tr_s, tr_e), (te_s, te_e)

        elif self.args.train_ratio:
            ratio = float(self.args.train_ratio)
            print(f"Split Mode: Ratio ({ratio * 100}% Train)")

            all_dates = sorted(list(set().union(*[prepare_data_index(df).index for df in self.raw_datas.values()])))
            if not all_dates:
                raise ValueError("Data has no valid dates.")

            split_idx = int(len(all_dates) * ratio)
            split_date = all_dates[split_idx]

            start_date_str = all_dates[0].strftime('%Y%m%d')
            split_date_str = split_date.strftime('%Y%m%d')
            end_date_str = all_dates[-1].strftime('%Y%m%d')

            print(f"  Split Date: {split_date_str}")

            train_d = slice_datas(self.raw_datas, start_date_str, split_date_str)
            test_d = slice_datas(self.raw_datas, split_date_str, end_date_str)
            return train_d, test_d, (start_date_str, split_date_str), (split_date_str, end_date_str)

        else:
            print("Warning: No split method defined. Running optimization on FULL dataset.")
            return self.raw_datas, {}, (self.args.start_date, self.args.end_date), (None, None)

    def objective(self, trial):
        current_params = copy.deepcopy(self.fixed_params)
        for param_name, config in self.opt_params_def.items():
            p_type = config.get('type')
            if p_type == 'int':
                val = trial.suggest_int(param_name, config['low'], config['high'], step=config.get('step', 1))
            elif p_type == 'float':
                val = trial.suggest_float(param_name, config['low'], config['high'], step=config.get('step', None))
            elif p_type == 'categorical':
                val = trial.suggest_categorical(param_name, config['choices'])
            else:
                val = config.get('value')
            current_params[param_name] = val

        if not self.train_datas:
            return -9999.0

        if not self.has_debugged_data:
            first_symbol = list(self.train_datas.keys())[0]
            print(f"[DEBUG] Training Data Check: {first_symbol} has {len(self.train_datas[first_symbol])} rows.")
            self.has_debugged_data = True

        try:
            # 使用修正后的 QuietBacktester
            bt_instance = QuietBacktester(
                datas=self.train_datas,
                strategy_class=self.strategy_class,
                params=current_params,
                start_date=self.train_range[0],
                end_date=self.train_range[1],
                cash=self.args.cash,
                commission=self.args.commission,
                risk_control_class=self.risk_control_class,
                risk_control_params=self.risk_params,
                timeframe=self.args.timeframe,
                compression=self.args.compression
            )

            bt_instance.run()

            # 使用新的 get_custom_metric 支持 calmar
            metric_val = bt_instance.get_custom_metric(self.args.metric)

            # 回退逻辑
            if metric_val == -999.0 and self.args.metric == 'sharpe':
                ret = bt_instance.get_custom_metric('return')
                metric_val = ret * 0.1 if ret > 0 else ret

            return metric_val

        except Exception as e:
            # print(f"Trial failed: {e}")
            return -9999.0

    def run(self):
        study = optuna.create_study(direction='maximize', study_name=self.args.study_name)

        print(f"\n--- Starting Optimization ({self.args.n_trials} trials) ---")
        study.optimize(self.objective, n_trials=self.args.n_trials)

        best_params = study.best_params
        best_value = study.best_value

        print("\n" + "=" * 60)
        print(">>> FINAL REPORT & OUT-OF-SAMPLE VALIDATION <<<")
        print("=" * 60)

        final_params = copy.deepcopy(self.fixed_params)
        final_params.update(best_params)

        print(f"Best Parameters Found (Train Set):")
        for k, v in best_params.items():
            print(f"  {k}: {v}")
        print(f"Best Training Score ({self.args.metric}): {best_value:.4f}")

        if self.test_datas:
            print("-" * 60)
            print(f"Running Validation on Test Set: {self.test_range[0]} to {self.test_range[1]}")
            print("-" * 60)

            bt_test = Backtester(
                datas=self.test_datas,
                strategy_class=self.strategy_class,
                params=final_params,
                start_date=self.test_range[0],
                end_date=self.test_range[1],
                cash=self.args.cash,
                commission=self.args.commission,
                risk_control_class=self.risk_control_class,
                risk_control_params=self.risk_params,
                timeframe=self.args.timeframe,
                compression=self.args.compression
            )
            bt_test.run()
        else:
            print("\n(No Test Set Configured)")

        print("\n" + "=" * 60)
        print(" SUMMARY OF BEST CONFIGURATION")
        print("=" * 60)
        print(f" Strategy: {self.args.strategy}")
        print(f" Params:   {final_params}")
        print("=" * 60 + "\n")

        # --- 可视化 ---
        try:
            fig1 = vis.plot_optimization_history(study)
            fig1.show()
            fig2 = vis.plot_slice(study)
            fig2.show()
            if len(self.opt_params_def) > 1:
                fig3 = vis.plot_param_importances(study)
                fig3.show()
        except Exception as e:
            print(f"Visualization skipped: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="量化策略参数优化器 (Optuna)",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('strategy', type=str, help="策略文件名")
    parser.add_argument('--params', type=str, default='{}', help="固定参数")
    parser.add_argument('--selection', type=str, default=None)
    parser.add_argument('--data_source', type=str, default=None)
    parser.add_argument('--symbols', type=str, default='SHSE.510300')
    parser.add_argument('--cash', type=float, default=100000.0)
    parser.add_argument('--commission', type=float, default=0.0003)
    parser.add_argument('--start_date', type=str, default=None)
    parser.add_argument('--end_date', type=str, default=None)
    parser.add_argument('--risk', type=str, default=None)
    parser.add_argument('--risk_params', type=str, default='{}')
    parser.add_argument('--timeframe', type=str, default='Days')
    parser.add_argument('--compression', type=int, default=1)

    parser.add_argument('--opt_params', type=str, required=True, help="优化参数空间定义 JSON")
    parser.add_argument('--n_trials', type=int, default=50, help="尝试次数")
    # 修改默认 metric 为 calmar，或者在命令行显式指定
    parser.add_argument('--metric', type=str, default='calmar', choices=['sharpe', 'return', 'final_value', 'calmar'],
                        help="优化目标指标 (推荐 calmar)")
    parser.add_argument('--study_name', type=str, default='quant_ada_study')

    parser.add_argument('--train_ratio', type=float, default=None)
    parser.add_argument('--train_period', type=str, default='20180101-20181231', help="默认A股震荡市训练")
    parser.add_argument('--test_period', type=str, default='20190101-20201231', help="默认A股震荡市测试")

    args = parser.parse_args()

    try:
        fixed_p = ast.literal_eval(args.params)
        opt_p_def = ast.literal_eval(args.opt_params)
        risk_p = ast.literal_eval(args.risk_params)
    except Exception as e:
        print(f"Error parsing JSON args: {e}")
        sys.exit(1)

    job = OptimizationJob(args, fixed_params=fixed_p, opt_params_def=opt_p_def, risk_params=risk_p)
    job.run()