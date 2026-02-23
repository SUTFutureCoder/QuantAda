import argparse
import ast
import datetime
import logging
import os
import sys

import pandas
import pandas as pd

import config
from backtest.backtester import Backtester
from common import optimizer
from common.formatters import format_float, format_recent_backtest_metrics
from common.loader import get_class_from_name, pascal_to_snake
from data_providers.manager import DataManager
from recorders.db_recorder import DBRecorder
from recorders.http_recorder import HttpRecorder
from recorders.manager import RecorderManager


def run_backtest(selection_filename, strategy_filename, symbols, cash, commission, slippage, data_source, start_date, end_date,
                 risk_filename, risk_params, params, timeframe, compression, recorder=None, enable_plot=True):
    """执行回测"""
    # --- 1. 自动发现并加载所有数据提供者 ---
    data_manager = DataManager()

    # --- 2. 执行选股 ---
    if selection_filename:
        print("--- Running Selection Phase ---")
        selector_class = get_class_from_name(selection_filename, ['stock_selectors'])
        selector_instance = selector_class(data_manager=data_manager)
        selection_result = selector_instance.run_selection()
        if isinstance(selection_result, list):
            symbols = selection_result
        if isinstance(selection_result, pandas.DataFrame):
            symbols = selection_result.index.tolist()

        if not symbols:
            print("\nFatal: The selector did not return any symbols. Aborting.")
            return
        print(f"  Selector '{selection_filename}' selected {len(symbols)} symbols: {', '.join(symbols)}")
    elif not symbols:
        print("\nFatal: You must provide either --selection or --symbols. Aborting.")
        return

    print("--- Starting Backtest ---")
    print(f"  Selection: {selection_filename}")
    print(f"  Strategy: {strategy_filename}")
    print(f"  Risk Control: {risk_filename or 'None'}")
    print(f"  Symbols: {symbols}")
    print(f"  Backtest Period: {start_date} to {end_date}")
    print(f"  Initial Cash: {cash:,.2f}")
    print(f"  Commission: {commission:.4f}")

    # --- 3. 获取数据 ---
    print("\n--- Fetching Data ---")
    print(f"  Requesting data from: {start_date or 'origin'} to {end_date or 'latest'}")

    datas = {}
    for symbol in symbols:
        print(f"  Fetching data for: {symbol}")
        df = data_manager.get_data(
            symbol,
            start_date=start_date,
            end_date=end_date,
            specified_sources=data_source,
            timeframe=timeframe,
            compression=compression,
            refresh=args.refresh
        )
        if df is not None and not df.empty:
            datas[symbol] = df
        else:
            print(f"  Warning: Failed to fetch data for {symbol}. It will be excluded from the backtest.")

    if not datas:
        print("\nFatal: Could not fetch data for any of the specified symbols. Aborting.")
        return

    # --- 4. 初始化回测器并运行 ---
    print("\n--- Initializing Backtester ---")
    strategy_class = get_class_from_name(strategy_filename, ['strategies'])

    risk_control_classes = []
    if risk_filename:
        # 支持逗号分隔的多个风控策略
        risk_names = risk_filename.split(',')
        for r_name in risk_names:
            r_name = r_name.strip()
            if r_name:
                cls = get_class_from_name(r_name, ['risk_controls', 'strategies'])
                risk_control_classes.append(cls)
        print(f"  Risk Control Modules: {risk_names}")
        print(f"  Risk Control Params: {risk_params}")

    backtester = Backtester(
        datas,
        strategy_class,
        params=params,
        start_date=start_date,
        end_date=end_date,
        cash=cash,
        commission=commission,
        slippage=slippage,
        risk_control_classes=risk_control_classes,
        risk_control_params=risk_params,
        timeframe=timeframe,
        compression=compression,
        recorder=recorder,
        enable_plot=enable_plot,
    )
    backtester.run()

    return backtester


if __name__ == '__main__':
    # 1. 创建命令行解析器
    parser = argparse.ArgumentParser(
        description="量化回测框架",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # 2. 添加命令行参数
    parser.add_argument('strategy', type=str,
                        help="要运行的策略文件名 (例如: sample_macd_cross_strategy.py 或 sample_macd_cross_strategy 或 my_pkg.my_strategy.MyStrategyClass)")
    parser.add_argument('--params', type=str, default='{}',
                        help="策略参数 (JSON字符串, 例如: \"{\'selectTopK\': 2, \'target_buffer\': 0.95}\")")
    parser.add_argument('--selection', type=str, default=None, help="选股器文件名 (位于selectors目录 或 自定义包路径)")
    parser.add_argument('--data_source', type=str, default=None,
                        help="指定数据源 (例如: csv yf akshare tushare sxsc_tushare gm)")
    parser.add_argument('--symbols', type=str, default='SHSE.510300', help="以,分割的回测标的代码 (默认: SHSE.510300)")
    parser.add_argument('--cash', type=float, default=None, help="初始资金 (回测默认: 100000.0、实盘默认全仓)")
    parser.add_argument('--commission', type=float, default=0, help="手续费率，例如：万1.5为:0.00015 (默认：0)")
    parser.add_argument('--slippage', type=float, default=0.001, help="滑点，模拟真实市场的冲击成本 (默认: 0.001)")
    parser.add_argument('--start_date', type=str, default=None, help="回测起始日期 (例如: 20241111)")
    parser.add_argument('--end_date', type=str, default=None, help="回测结束日期 (例如: 20250101)")
    parser.add_argument('--risk', type=str, default=None, help="风控模块名称 (位于 risk_controls目录 或 自定义包路径)")
    parser.add_argument('--risk_params', type=str, default='{}',
                        help="风控参数 (JSON字符串, 例如: \"{\'stop_loss\': 0.05}\")")
    bt_timeframes = ['Days', 'Weeks', 'Months', 'Minutes', 'Seconds']
    parser.add_argument('--timeframe', type=str, default='Days', choices=bt_timeframes,
                        help=f"K线时间维度 (默认: Days). 支持: {', '.join(bt_timeframes)}")
    parser.add_argument('--compression', type=int, default=1,
                        help="K线时间周期 (默认: 1). 结合 timeframe, 例如 30 Minutes")
    parser.add_argument('--desc', type=str, default=None,
                        help="本次回测的描述信息 (默认为不带 .py 的策略文件名)")

    parser.add_argument('--no_plot', action='store_true', help="在服务器环境下禁用绘图")
    parser.add_argument('--refresh', action='store_true', help="强制刷新CACHE_DATA数据")
    parser.add_argument('--config', type=str, default='{}',
                        help="覆盖config.py配置 (JSON字符串, 例如: \"{'GM_TOKEN':'xxx','LOG':False}\")")
    # Optimizer 专用参数
    parser.add_argument('--opt_params', type=str, default=None, help="[优化模式] 优化参数空间定义 JSON")
    parser.add_argument('--n_trials', type=int, default=None, help="[优化模式] 尝试次数 (默认: 自动推断)")
    parser.add_argument(
        '--n_jobs',
        type=int,
        default=-1,
        help="[优化模式] 并行核心数: >0=指定worker数, -1=自动保留15%且至少2核系统冗余, <-1=保留(abs(n_jobs)-1)核"
    )
    parser.add_argument('--metric', type=str, default='mix_score_origin',
                        help="[优化模式] 优化目标 (支持逗号分隔的多私有指标串行执行)")
    parser.add_argument('--train_roll_period', type=str, default=None,
                        help="[优化模式] 训练集滚动周期 (从测试集开始时间往前推)。例如：1y, 3y")
    parser.add_argument('--test_roll_period', type=str, default=None,
                        help="[优化模式] 测试集滚动周期 (从当前时间/end_date往前推)。例如：1y, 3m, 6m。默认为无独立测试集")
    parser.add_argument('--train_ratio', type=float, default=None, help="[优化模式] 比例切分训练集、测试集，例如0.5")
    parser.add_argument('--train_period', type=str, default=None, help="[优化模式] 训练集时段")
    parser.add_argument('--test_period', type=str, default=None, help="[优化模式] 测试集时段")

    # 实盘参数
    parser.add_argument('--connect', type=str, default=None,
                        help="实盘连接配置，格式 'broker:env' (例如: 'gm_broker:sim')")

    # 3. 解析参数
    args = parser.parse_args()

    # ==========================================
    # 全局时间自动推断逻辑 (Auto-Inference)
    # 作用：支持缺省 start_date/end_date 的自动化回测
    # ==========================================
    # 1. 自动补全 end_date (默认为当前系统时间)
    if not args.end_date:
        args.end_date = datetime.datetime.now().strftime('%Y%m%d')

    # 2. 自动补全 start_date
    # 注意：如果是实盘模式(--connect)，引擎有自己的 1 年预热逻辑，无需在此强行干预
    if not args.start_date and not args.connect:
        # 默认最大公约数回溯周期：3年
        # 使用 pd.DateOffset 可以完美处理闰年(Leap Year)的天数差异
        end_dt = pd.to_datetime(args.end_date)
        start_dt = end_dt - pandas.DateOffset(years=3)
        args.start_date = start_dt.strftime('%Y%m%d')
        print(f"\n[System] 💡 start_date omitted. Auto-inferred to: {args.start_date} (3 years lookback).")

    # 覆盖config.py
    if args.config:
        override_config = ast.literal_eval(args.config)
        print(f"\n--- Applying Config Overrides ---")
        for key, value in override_config.items():
            if hasattr(config, key):
                setattr(config, key, value)
                print(f"  [Config] Overriding {key} = {value}")

    # 将逗号分隔的字符串转换为列表
    symbol_list = [s.strip() for s in args.symbols.split(',')]

    # 在这里解析 JSON 字符串为字典
    try:
        s_params = ast.literal_eval(args.params)
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing params JSON: {e}")
        s_params = {}

    try:
        r_params = ast.literal_eval(args.risk_params)
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing risk_params JSON: {e}")
        r_params = {}

    # ==========================
    # 实盘模式
    # ==========================
    if args.connect:
        if ':' not in args.connect:
            print("Error: --connect format must be 'broker:env' (e.g. gm_broker:sim)")
            sys.exit(1)

        broker_name, conn_name = args.connect.split(':', 1)

        # 收集执行参数 (Execution Args)
        # 这些参数之前只用于内部回测，现在我们也传给 Broker
        exec_args = {
            'start_date': args.start_date,
            'end_date': args.end_date,
            'cash': args.cash,
            'commission': args.commission,
            'slippage': args.slippage,
            # 透传选股器和标的参数
            'selection': args.selection,
            # 同时也处理 symbols (转为列表)，以防没有选股器时使用
            'symbols': [s.strip() for s in args.symbols.split(',')] if args.symbols else []
        }

        # 延迟导入 launcher，避免回测时引入不必要的依赖
        from live_trader.engine import launch_live

        launch_live(broker_name, conn_name, args.strategy, s_params, **exec_args)
        sys.exit(0)

    # ==========================
    # 优化模式
    # ==========================
    if args.opt_params:
        import copy
        import time

        print(f"\n>>> Mode: PARAMETER OPTIMIZATION (Target: {args.metric}) <<<")

        # 1. 解析传入的 metric (支持单个或逗号分隔的多个)
        # 自动过滤空字符串，避免出现如 "sharpe,,calmar," 的脏输入
        metrics_list = [m.strip() for m in args.metric.split(',') if m.strip()]
        if not metrics_list:
            print("Error: --metric contains no valid metric after filtering empty entries.")
            sys.exit(1)

        config.LOG = False
        logging.getLogger("optuna").setLevel(logging.INFO)

        try:
            opt_p_def = ast.literal_eval(args.opt_params)
        except Exception as e:
            print(f"Error parsing opt_params JSON: {e}")
            sys.exit(1)

        final_reports = []
        total_metrics = len(metrics_list)
        is_multi_metric = total_metrics > 1
        explicit_params_passed = any(
            (arg == '--params') or arg.startswith('--params=')
            for arg in sys.argv[1:]
        )
        baseline_report = None
        baseline_elapsed_hours = None
        shared_context = None
        bootstrap_job = None
        dashboard_launcher_job = None
        shared_dashboard_log_file = None
        log_dir = None

        if is_multi_metric:
            log_dir = os.path.join(os.getcwd(), config.DATA_PATH, 'optuna')
            os.makedirs(log_dir, exist_ok=True)

        def build_shared_optuna_log_file(train_range, test_range, symbols):
            if not log_dir:
                return None

            name_tag = optimizer.OptimizationJob.build_optuna_name_tag(
                metric="multi_metric",
                train_period=args.train_roll_period,
                test_period=args.test_roll_period,
                train_range=train_range,
                test_range=test_range,
                data_source=args.data_source,
                symbols=symbols,
                selection=args.selection,
                run_dt=datetime.datetime.now(),
                run_pid=os.getpid(),
            )
            return os.path.join(log_dir, f"optuna_{name_tag}.log")

        # 先构建一次共享上下文，确保基准与多指标训练处于同一数据宇宙（同一选股与同一数据切分）
        try:
            bootstrap_args = copy.deepcopy(args)
            bootstrap_args.metric = metrics_list[0]
            bootstrap_args.auto_launch_dashboard = not is_multi_metric
            bootstrap_job = optimizer.OptimizationJob(
                args=bootstrap_args,
                fixed_params=s_params,
                opt_params_def=opt_p_def,
                risk_params=r_params
            )
            shared_context = bootstrap_job.export_shared_context()
            dashboard_launcher_job = bootstrap_job
            if is_multi_metric:
                shared_dashboard_log_file = build_shared_optuna_log_file(
                    train_range=bootstrap_job.train_range,
                    test_range=bootstrap_job.test_range,
                    symbols=bootstrap_job.target_symbols,
                )
        except Exception as e:
            print(f"[警告] 共享上下文构建失败，将降级为逐metric独立初始化: {e}")
            if is_multi_metric and not shared_dashboard_log_file:
                fallback_train_range = (args.start_date, args.end_date)
                fallback_test_range = (args.end_date, args.end_date) if args.test_roll_period else (None, None)
                shared_dashboard_log_file = build_shared_optuna_log_file(
                    train_range=fallback_train_range,
                    test_range=fallback_test_range,
                    symbols=symbol_list,
                )

        if explicit_params_passed:
            print("\n--- Running Baseline Backtest from --params (Recent 3Y) ---")
            baseline_start = time.time()
            try:
                if bootstrap_job is not None:
                    baseline_report = bootstrap_job._run_recent_3y_backtest(copy.deepcopy(s_params))
                else:
                    baseline_args = copy.deepcopy(args)
                    baseline_args.metric = metrics_list[0]
                    baseline_args.auto_launch_dashboard = not is_multi_metric
                    if shared_dashboard_log_file:
                        baseline_args.shared_journal_log_file = shared_dashboard_log_file
                    baseline_job = optimizer.OptimizationJob(
                        args=baseline_args,
                        fixed_params=s_params,
                        opt_params_def=opt_p_def,
                        risk_params=r_params
                    )
                    baseline_report = baseline_job._run_recent_3y_backtest(copy.deepcopy(s_params))
            except Exception as e:
                print(f"[警告] 当前基准回测失败: {e}")
            finally:
                baseline_elapsed_hours = (time.time() - baseline_start) / 3600.0

        for idx, current_metric in enumerate(metrics_list, 1):
            print(f"\n\n{'=' * 65}")
            print(f"🚀 [指标 {idx}/{total_metrics} 正在训练]: {current_metric}")
            print(f"{'=' * 65}")

            # 深拷贝 args，确保物理隔离
            current_args = copy.deepcopy(args)
            current_args.metric = current_metric
            current_args.auto_launch_dashboard = not is_multi_metric
            if shared_dashboard_log_file:
                current_args.shared_journal_log_file = shared_dashboard_log_file

            start_time = time.time()

            try:
                job_kwargs = {
                    "args": current_args,
                    "fixed_params": s_params,
                    "opt_params_def": opt_p_def,
                    "risk_params": r_params,
                }
                if shared_context is not None:
                    job_kwargs["shared_context"] = shared_context

                job = optimizer.OptimizationJob(**job_kwargs)
                if dashboard_launcher_job is None:
                    dashboard_launcher_job = job

                # 执行优化并接收返回的字典战报
                result_dict = job.run()
                elapsed_hours = (time.time() - start_time) / 3600.0

                if result_dict and isinstance(result_dict, dict):
                    result_dict['metric_name'] = current_args.metric
                    result_dict['elapsed_hours'] = elapsed_hours
                    result_dict['study_db'] = getattr(current_args, 'study_name', 'N/A')
                    final_reports.append(result_dict)

            except Exception as e:
                print(f"\n[致命错误] 指标 '{current_metric}' 训练崩溃: {e}")
                import traceback

                traceback.print_exc()
                print(">>> 引擎防宕机保护触发，强行切入下一个指标...")
                continue

        if final_reports or explicit_params_passed:
            print("=== 请忽略上文日志输出，请将下文提供给AI辅助分析 ===")
            print(">>> 多臂赌博机训练结果汇总(MULTI-METRIC BANDIT SUMMARY)  <<<")

            header = (
                f"| {'指标 (Metric)':<30} | {'年化收益':<10} | {'回撤':<10} | "
                f"{'Calmar':<8} | {'Sharpe':<8} | {'交易数':<8} | {'胜率':<10} | {'PF':<8} | "
                f"{'耗时(h)':<8} | {'最优参数 (Params)':<22} | {'关联日志 (Log)'}"
            )
            table_width = len(header)
            print("-" * table_width)
            print(header)
            print("-" * table_width)

            if explicit_params_passed:
                baseline_recent = baseline_report or {}
                baseline_fmt = format_recent_backtest_metrics(baseline_recent)
                m_str = "当前基准"
                ret_str = baseline_fmt['annual_return']
                dd_str = baseline_fmt['max_drawdown']
                calmar_str = baseline_fmt['calmar_ratio']
                sharpe_str = baseline_fmt['sharpe_ratio']
                trades_str = baseline_fmt['total_trades']
                winrate_str = baseline_fmt['win_rate']
                pf_str = baseline_fmt['profit_factor']
                t_str = format_float(baseline_elapsed_hours, digits=1)
                b_str = str(s_params)
                db_str = "N/A"
                print(
                    f"| {m_str:<30} | {ret_str:<10} | {dd_str:<10} | "
                    f"{calmar_str:<8} | {sharpe_str:<8} | {trades_str:<8} | {winrate_str:<10} | {pf_str:<8} | "
                    f"{t_str:<8} | {b_str:<22} | {db_str}"
                )
                if final_reports:
                    print("-" * table_width)

            for r in final_reports:
                recent = r.get('recent_backtest') or {}
                recent_fmt = format_recent_backtest_metrics(recent)
                metric_name = str(r.get('metric_name', 'Unknown'))
                score_str = str(r.get('best_score', 'N/A'))
                metric_with_score = f"{metric_name} ({score_str})" if score_str != "N/A" else metric_name
                m_str = metric_with_score[:30]
                ret_str = recent_fmt['annual_return']
                dd_str = recent_fmt['max_drawdown']
                calmar_str = recent_fmt['calmar_ratio']
                sharpe_str = recent_fmt['sharpe_ratio']
                trades_str = recent_fmt['total_trades']
                winrate_str = recent_fmt['win_rate']
                pf_str = recent_fmt['profit_factor']
                t_str = format_float(r.get('elapsed_hours', 0), digits=1)
                b_str = str(r.get('best_params', 'N/A'))
                db_str = str(r.get('log_file', 'N/A'))
                print(
                    f"| {m_str:<30} | {ret_str:<10} | {dd_str:<10} | "
                    f"{calmar_str:<8} | {sharpe_str:<8} | {trades_str:<8} | {winrate_str:<10} | {pf_str:<8} | "
                    f"{t_str:<8} | {b_str:<22} | {db_str}"
                )

            print("-" * table_width + "\n")

            if final_reports:
                print("请在 Dashboard 中回放并排查孤点: ")
                dashboard_logs = []
                for r in final_reports:
                    log_file = r.get('log_file')
                    if log_file and log_file not in dashboard_logs:
                        dashboard_logs.append(log_file)
                for log_file in dashboard_logs:
                    print(f"optuna-dashboard {log_file}")

                # 多 metric 场景只在末尾弹一次 Dashboard（共享 Journal 可聚合全部 metric）
                if is_multi_metric and dashboard_launcher_job and dashboard_logs:
                    final_log = shared_dashboard_log_file or dashboard_logs[0]
                    if os.path.exists(final_log):
                        base_port = getattr(config, 'OPTUNA_DASHBOARD_PORT', 8090)
                        target_port = base_port
                        for _ in range(100):
                            if not optimizer.is_port_in_use(target_port):
                                break
                            target_port += 1
                        else:
                            print(f"[Warning] Could not find an available port starting from {base_port}.")
                            target_port = base_port
                        print(f"[Info] Multi-metric training completed. Launching aggregated dashboard: {final_log}")
                        print("[Info] Dashboard will run in foreground. Analyze results, then press Ctrl-C to exit.")
                        dashboard_launcher_job._launch_dashboard(final_log, port=target_port, background=False)
                    else:
                        print(f"[Warning] Aggregated dashboard log file not found: {final_log}")
            else:
                print("[警告] 当前仅有基准回测结果，训练指标未返回结果。")
        else:
            print("\n[警告] 所有指标均未返回结果")

        sys.exit(0)

    # ==========================
    # 回测模式
    # ==========================
    recorder_manager = RecorderManager()
    if config.DB_ENABLED:
        try:
            recorder_manager.add_recorder(DBRecorder(
                strategy_name=args.strategy, description=args.desc, params=s_params,
                start_date=args.start_date, end_date=args.end_date,
                initial_cash=args.cash if args.cash is not None else 100000.0, commission=args.commission
            ))
        except Exception as e:
            print(f"Failed to init DBRecorder: {e}")

    if hasattr(config, 'HTTP_LOG_URL') and config.HTTP_LOG_URL:
        recorder_manager.add_recorder(HttpRecorder(endpoint_url=config.HTTP_LOG_URL))

    run_backtest(
        selection_filename=args.selection,
        strategy_filename=args.strategy,
        symbols=symbol_list,
        cash=args.cash if args.cash is not None else 100000.0,
        commission=args.commission,
        slippage=args.slippage,
        data_source=args.data_source,
        start_date=args.start_date,
        end_date=args.end_date,
        risk_filename=args.risk,
        risk_params=r_params,
        params=s_params,
        timeframe=args.timeframe,
        compression=args.compression,
        recorder=recorder_manager,
        enable_plot=not args.no_plot,
    )
    print("\n--- Backtest Finished ---")
