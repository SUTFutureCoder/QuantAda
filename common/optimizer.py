"""
QuantAda 启发式并行贝叶斯优化器
===============================

基于 TPE (Tree-structured Parzen Estimator) 算法的高性能参数寻优框架，
专为解决非凸、高维的金融时间序列参数优化问题而设计。

核心特性：
1. **贝叶斯内核**：利用 TPE 算法建模目标函数的后验概率分布，高效定位高潜参数区域。
2. **启发式算力评估**：基于参数空间复杂度（熵）与硬件算力（CPU核数），
   通过非线性公式动态估算最佳尝试次数 ($N_{trials}$)，拒绝盲目穷举。
3. **随机并发探索**：引入 `Constant-Liar` 采样策略与哈希去重机制，
   解决多核环境下的"并发踩踏"问题，模拟退火特性以有效跳出局部最优陷阱。
4. **工程鲁棒性**：内置跨平台文件锁管理、异常自动降级及全自动环境清理机制。
"""

import copy
import math
import os
import sys
import threading
import time
import webbrowser

import optuna
import optuna.visualization as vis
import pandas as pd
from optuna.samplers import TPESampler

import config
from backtest.backtester import Backtester
from common.loader import get_class_from_name
from data_providers.manager import DataManager

try:
    from optuna.storages import JournalStorage
    try:
        # Optuna 4.0+ 新版路径
        from optuna.storages.journal import JournalFileBackend
        # 给它起个通用的别名
        JournalFileBackendCls = JournalFileBackend
    except ImportError:
        # 旧版路径 (兼容老环境)
        from optuna.storages import JournalFileStorage
        JournalFileBackendCls = JournalFileStorage
    HAS_JOURNAL = True
except ImportError:
    HAS_JOURNAL = False

try:
    from optuna_dashboard import run_server
    HAS_DASHBOARD = True
except ImportError:
    HAS_DASHBOARD = False

class OptimizationJob:
    def __init__(self, args, fixed_params, opt_params_def, risk_params):
        self.args = args
        self.fixed_params = fixed_params
        self.opt_params_def = opt_params_def
        self.risk_params = risk_params
        self.strategy_class = get_class_from_name(args.strategy, ['strategies'])
        self.risk_control_classes = []
        if args.risk:
            # 支持逗号分隔
            risk_names = args.risk.split(',')
            for r_name in risk_names:
                r_name = r_name.strip()
                if r_name:
                    cls = get_class_from_name(r_name, ['risk_controls', 'strategies'])
                    self.risk_control_classes.append(cls)

        self.data_manager = DataManager()

        # Selection Logic
        self.target_symbols = []
        if self.args.selection:
            print(f"\n--- Running Selection Phase: {self.args.selection} ---")
            try:
                selector_class = get_class_from_name(self.args.selection, ['stock_selectors', 'stock_selectors_custom'])
                selector_instance = selector_class(data_manager=self.data_manager)
                selection_result = selector_instance.run_selection()
                if isinstance(selection_result, list):
                    self.target_symbols = selection_result
                elif isinstance(selection_result, pd.DataFrame):
                    self.target_symbols = selection_result.index.tolist()
                print(f"  Selector returned {len(self.target_symbols)} symbols: {self.target_symbols}")
            except Exception as e:
                print(f"Error during selection execution: {e}")
                sys.exit(1)
        else:
            if self.args.symbols:
                self.target_symbols = [s.strip() for s in self.args.symbols.split(',')]

        if not self.target_symbols:
            print("\nError: No symbols found for optimization.")
            sys.exit(1)

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

        datas = {}
        for symbol in self.target_symbols:
            # 优先使用缓存
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
            else:
                print(f"Warning: No data for {symbol}, skipping.")

        if not datas:
            raise ValueError("No data fetched. Check symbols, selection or date range.")
        return datas

    def _split_data(self):
        if self.args.train_period and self.args.test_period:
            tr_s, tr_e = self.args.train_period.split('-')
            te_s, te_e = self.args.test_period.split('-')

            print(f"Split Mode: Explicit Period")
            print(f"  Train: {tr_s} -> {tr_e}")
            print(f"  Test:  {te_s} -> {te_e}")

            train_d = self.slice_datas(tr_s, tr_e)
            test_d = self.slice_datas(te_s, te_e)
            return train_d, test_d, (tr_s, tr_e), (te_s, te_e)

        elif self.args.train_ratio:
            ratio = float(self.args.train_ratio)
            print(f"Split Mode: Ratio ({ratio * 100}% Train)")

            all_dates = sorted(list(set().union(*[self.prepare_data_index(df).index for df in self.raw_datas.values()])))
            if not all_dates:
                raise ValueError("Data has no valid dates.")

            split_idx = int(len(all_dates) * ratio)

            # 防止训练集和测试集重叠
            train_end_date = all_dates[split_idx]

            # 测试集从训练结束的下一条数据开始
            if split_idx + 1 < len(all_dates):
                test_start_date = all_dates[split_idx + 1]
            else:
                test_start_date = train_end_date  # 极端情况，无测试集

            start_date_str = all_dates[0].strftime('%Y%m%d')
            split_date_str = train_end_date.strftime('%Y%m%d')
            test_start_str = test_start_date.strftime('%Y%m%d')
            end_date_str = all_dates[-1].strftime('%Y%m%d')

            print(f"  Train End: {split_date_str}")
            print(f"  Test Start: {test_start_str}")

            train_d = self.slice_datas(start_date_str, split_date_str)
            test_d = self.slice_datas(test_start_str, end_date_str)
            return train_d, test_d, (start_date_str, split_date_str), (test_start_str, end_date_str)

        else:
            print("Warning: No split method defined. Running optimization on FULL dataset.")
            return self.raw_datas, {}, (self.args.start_date, self.args.end_date), (None, None)

    def _launch_dashboard(self, log_file, port=8080):
        """
        [线程版] 直接在代码中运行 Optuna Dashboard
        """
        if not HAS_DASHBOARD:
            print("[Warning] 'optuna-dashboard' not installed. Skipping.")
            return

        import logging
        import http.server
        import wsgiref.simple_server

        # 直接覆盖标准库 http.server 的日志方法，彻底消除访问日志
        def silent_log_message(self, format, *args):
            return  # 什么都不做，直接返回

        # 覆盖 http.server 的日志方法 (bottle 默认 server 基于此)
        http.server.BaseHTTPRequestHandler.log_message = silent_log_message
        # 同时也覆盖 wsgiref 的日志方法 (双重保险)
        wsgiref.simple_server.WSGIRequestHandler.log_message = silent_log_message

        print("\n" + "=" * 60)
        print(">>> STARTING DASHBOARD (Thread Mode) <<<")
        print("=" * 60)

        def start_server():
            # 静默日志
            loggers_to_silence = [
                "optuna",
                "optuna_dashboard",
                "sqlalchemy",
                "bottle",
                "waitress",
                "werkzeug"
            ]
            for name in loggers_to_silence:
                logging.getLogger(name).setLevel(logging.ERROR)

            # 1. 在线程内部初始化存储对象
            # 这样可以确保它读取的是最新的文件
            try:
                storage = JournalStorage(JournalFileBackendCls(log_file))

                # 2. 启动服务 (这是一个阻塞操作，会一直运行)
                run_server(storage, host="127.0.0.1", port=port)
            except OSError as e:
                if "Address already in use" in str(e):
                    print(f"\n[Error] Port {port} is occupied! Dashboard failed to start.")
                else:
                    print(f"\n[Error] Dashboard thread failed: {e}")
            except Exception as e:
                print(f"\n[Error] Dashboard crashed: {e}")

        # 3. 创建并启动守护线程
        t = threading.Thread(target=start_server, daemon=True)
        t.start()

        dashboard_url = f"http://127.0.0.1:{port}"
        print(f"[Success] Dashboard is running at: {dashboard_url}")

        # 4. 尝试打开浏览器
        try:
            time.sleep(1.5)
            webbrowser.open(dashboard_url)
        except:
            pass

        print("[INFO] Dashboard running in background thread.")
        print("=" * 60 + "\n")

    def _estimate_n_trials(self):
        """
        [激进版] 启发式算法：根据参数复杂度和算力自动估算 n_trials。
        逻辑：算力越强，我们越有资本进行地毯式轰炸（更接近 Grid Search 的密度）。
        """
        # 1. 确定实际可用核心数
        requested_jobs = getattr(self.args, 'n_jobs', 1)
        if requested_jobs == -1:
            n_cores = os.cpu_count() or 1
        else:
            n_cores = max(1, requested_jobs)

        # 2. 计算基础复杂度 (Base Complexity)
        complexity_score = 0
        n_params = len(self.opt_params_def)

        for param, config in self.opt_params_def.items():
            p_type = config.get('type')
            if p_type == 'int':
                # (high - low) / step
                range_len = (config['high'] - config['low']) / config.get('step', 1)
                complexity_score += math.log(max(range_len, 2)) * 30
            elif p_type == 'float':
                # 浮点数权重
                complexity_score += 60
            elif p_type == 'categorical':
                # 离散选项权重
                complexity_score += len(config['choices']) * 15
            else:
                complexity_score += 10

        # 维度惩罚：参数越多，相互干扰越大，需要的次数应略微呈非线性增长
        # 比如 1个参数乘数是1.0，4个参数乘数是2.0，9个参数乘数是3.0
        dimension_penalty = math.sqrt(n_params)

        # 基础次数：起步 100 次 + (复杂度 * 维度惩罚)
        base_estimated = int(100 + (complexity_score * dimension_penalty))

        # 3. 算力加成 (Hardware Scaling)
        # 逻辑：利用多核优势扩大搜索范围。
        scaling_factor = 1.0 + math.sqrt(n_cores)

        final_estimated = int(base_estimated * scaling_factor)

        # 4. 保底机制
        # 确保每个核心至少有 30 个任务 (稍微提高保底阈值)
        min_saturation = n_cores * 30
        final_estimated = max(final_estimated, min_saturation)

        # 5. 设定上限 (防止无限膨胀)
        final_estimated = min(final_estimated, 10000)

        return final_estimated

    def objective(self, trial):
        current_params = copy.deepcopy(self.fixed_params)
        # 获取当前试验的参数
        trial_params_dict = {}

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
            trial_params_dict[param_name] = val

        try:
            # 1. 获取所有之前的 Trial (包括 Running, Complete, Pruned)
            existing_trials = trial.study.get_trials(deepcopy=False)

            for t in existing_trials:
                # 跳过自己
                if t.number == trial.number:
                    continue

                # 如果参数完全一致
                if t.params == trial_params_dict:

                    # 情况 A: 之前已经有人跑完了 -> 直接抄作业 (Cache Hit)
                    if t.state == optuna.trial.TrialState.COMPLETE:
                        # print(f"  [Cache] Trial {trial.number} hit cache from {t.number}")
                        return t.value

                    # 情况 B: 此时此刻有人正在跑 -> 我是多余的，自我了断 (Prune)
                    elif t.state == optuna.trial.TrialState.RUNNING:
                        # print(f"  [Prune] Trial {trial.number} is a duplicate of running {t.number}")
                        # 抛出 Pruned 异常，Optuna 会标记此 Trial 为 PRUNED 并跳过
                        raise optuna.TrialPruned("Duplicate of a running trial")

        except optuna.TrialPruned:
            raise  # 必须把 Pruned 异常往外抛，Optuna 才能捕获
        except Exception as e:
            pass  # 其他查询错误忽略，兜底跑回测

        if not self.train_datas:
            return -9999.0

        if not self.has_debugged_data:
            print(f"\n[DEBUG] Training Data Overview (Total {len(self.train_datas)} symbols):")
            for symbol, data in self.train_datas.items():
                print(f"  - {symbol}: {len(data)} rows")
            self.has_debugged_data = True

        try:
            bt_instance = Backtester(
                datas=self.train_datas,
                strategy_class=self.strategy_class,
                params=current_params,
                start_date=self.train_range[0],
                end_date=self.train_range[1],
                cash=self.args.cash,
                commission=self.args.commission,
                risk_control_classes=self.risk_control_classes,
                risk_control_params=self.risk_params,
                timeframe=self.args.timeframe,
                compression=self.args.compression,
                enable_plot=False,
                verbose=False
            )

            bt_instance.run()

            # =========================================================
            # Mix Score 计算核心
            # =========================================================
            if self.args.metric == 'mix_score':
                # 直接从 strategy 实例中获取分析器，不依赖 get_custom_metric
                # 这样可以获取交易次数等丰富信息
                strat = bt_instance.results[0]

                # 1. 获取核心数据
                # Calmar
                calmar = bt_instance.get_custom_metric('calmar')  # 复用现成逻辑
                if calmar == -999.0 or calmar is None: calmar = 0.0

                # Sharpe
                sharpe = bt_instance.get_custom_metric('sharpe')  # 复用现成逻辑
                if sharpe is None: sharpe = 0.0

                # Total Return
                total_return = bt_instance.get_custom_metric('return')  # 复用现成逻辑

                # Trades Count (必须直接读 Analyzer)
                trade_analyzer = strat.analyzers.getbyname('tradeanalyzer')
                trade_analysis = trade_analyzer.get_analysis()
                total_trades = trade_analysis.get('total', {}).get('total', 0)

                # 2. 熔断/惩罚机制 (Sanity Check)
                # 交易次数过少的惩罚 (Penalty)
                # 只有这个是硬伤，需要重罚，因为样本太少没有统计意义
                penalty = 0.0
                if total_trades < 10:
                    penalty = -10.0

                # 让分数连续变化，即使是负数，优化器也能找到上升方向
                # 亏 5% (Score -0.5) 优于 亏 50% (Score -5.0)

                # 权重微调：“盯着回撤（Calmar）和总收益（Return），几乎完全无视波动（Sharpe）” 的进攻型猛兽。
                # Calmar: 2.0 (生存第一)
                # Return: 2.0 (收益第二，不用给太高，防止大起大落)
                # Sharpe: 1.0 (平滑第三 $\sqrt{252} \approx 15.8$，因此权重除以16，防止变为保守派老头（重波动率平滑）)

                # 保护逻辑：防止 infinite
                if math.isinf(calmar): calmar = 0.0
                if math.isinf(sharpe): sharpe = 0.0

                raw_score = (calmar * 2.0) + (total_return * 2.0) + (sharpe * 1.0 / 16)

                metric_val = raw_score + penalty

            # =========================================================
            # 传统模式 (单独指定 calmar, sharpe 等)
            # =========================================================
            else:
                metric_val = bt_instance.get_custom_metric(self.args.metric)
                # 回退逻辑
                if metric_val == -999.0 and self.args.metric == 'sharpe':
                    ret = bt_instance.get_custom_metric('return')
                    metric_val = ret * 0.1 if ret > 0 else ret

            return metric_val

        except Exception as e:
            print(f"Trial failed: {e}")
            return -9999.0

    def prepare_data_index(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def slice_datas(self, start_date: str, end_date: str):
        """根据日期切分数据字典"""
        sliced = {}
        if not start_date and not end_date:
            return self.raw_datas

        s = pd.to_datetime(start_date) if start_date else pd.Timestamp.min
        e = pd.to_datetime(end_date) if end_date else pd.Timestamp.max

        for symbol, df in self.raw_datas.items():
            df = self.prepare_data_index(df)
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

    def run(self):
        # 1. 配置存储 (支持多核)
        storage = None
        n_jobs = getattr(self.args, 'n_jobs', 1)

        log_file = None

        if n_jobs != 1:
            if HAS_JOURNAL:
                log_dir = os.path.join(os.getcwd(), config.DATA_PATH, 'optuna')
                os.makedirs(log_dir, exist_ok=True)

                log_file = os.path.join(log_dir, f"{self.args.study_name}.log")

                try:
                    # 尝试清除旧文件和锁
                    lock_file = log_file + ".lock"
                    if os.path.exists(log_file):
                        os.remove(log_file)
                        print(f"[Optimizer] Cleaned up old journal: {log_file}")
                    if os.path.exists(lock_file):
                        os.remove(lock_file)
                except Exception as e:
                    print(f"[Warning] Failed to clean logs: {e}")

                try:
                    # 尝试创建文件存储
                    storage = JournalStorage(JournalFileBackendCls(log_file))
                    print(f"\n[Optimizer] Multi-core mode enabled (n_jobs={n_jobs}).")
                    print(f"[Optimizer] Using JournalStorage: {log_file}")
                except OSError as e:
                    # 专门捕获 Windows 权限错误 (WinError 1314)
                    if hasattr(e, 'winerror') and e.winerror == 1314:
                        print("\n" + "!" * 60)
                        print("[ERROR] Windows Permission Error (WinError 1314)")
                        print(
                            "Multi-core optimization on Windows (using JournalStorage) requires symbolic link privileges.")
                        print("\nPLEASE TRY ONE OF THE FOLLOWING:")
                        print("  1. Run your PowerShell/Terminal as Administrator.")
                        print(
                            "  2. OR Enable 'Developer Mode' in Windows Settings (Privacy & security -> For developers).")
                        print("  3. OR Run with --n_jobs 1 to use single-core mode.")
                        print("!" * 60 + "\n")
                        sys.exit(1)
                    else:
                        raise e
            else:
                print("\n[Warning] optuna.storages.JournalStorage not found.")
                print("[Warning] Fallback to single-core to avoid SQLite dependency.")
                n_jobs = 1

        # 使用 TPESampler(constant_liar=True)
        # 这会防止多个 Worker 同时采样到同一个点（并发踩踏）
        sampler = TPESampler(constant_liar=True)

        # 2. 创建 Study (包裹 try-except 以捕获 Windows 权限错误)
        try:
            study = optuna.create_study(
                direction='maximize',
                study_name=self.args.study_name,
                storage=storage,
                load_if_exists=True,
                sampler=sampler,
            )
        except OSError as e:
            # 捕获 WinError 1314 (Symlink 权限不足)
            if hasattr(e, 'winerror') and e.winerror == 1314:
                print("\n" + "!" * 60)
                print("[WARNING] Windows Permission Error (WinError 1314).")
                print(
                    "          Multi-core optimization requires Administrator privileges to create lock files.")
                print(
                    "          请使用管理员权限运行终端后执行，以进行多核优化")
                print("          >> AUTOMATICALLY FALLING BACK TO SINGLE-CORE MODE. <<")
                print("          >> 自动降级为单核优化模式. <<")
                print("!" * 60 + "\n")

                # 降级：重置为单核 + 内存存储
                n_jobs = 1
                storage = None
                study = optuna.create_study(
                    direction='maximize',
                    study_name=self.args.study_name,
                    storage=None,
                    load_if_exists=True,
                    sampler=sampler,
                )
            else:
                # 其他错误照常抛出
                raise e

        # 2. 确定 n_trials
        n_trials = self.args.n_trials
        if n_trials is None:
            n_trials = self._estimate_n_trials()
            print(f"[Optimizer] Auto-inferred n_trials: {n_trials} (based on param complexity)")

        if log_file and os.path.exists(log_file):
            self._launch_dashboard(log_file, port=config.OPTUNA_DASHBOARD_PORT)

        print(f"\n--- Starting Optimization ({n_trials} trials, {n_jobs} parallel jobs) ---")

        # 3. 执行优化
        try:
            study.optimize(self.objective, n_trials=n_trials, n_jobs=n_jobs)
        except KeyboardInterrupt:
            print("\n[Optimizer] Optimization stopped by user.")

        if len(study.trials) == 0:
            print("No trials finished.")
            return

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
                risk_control_classes=self.risk_control_classes,
                risk_control_params=self.risk_params,
                timeframe=self.args.timeframe,
                compression=self.args.compression,
                enable_plot=False,
                verbose=True,
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

