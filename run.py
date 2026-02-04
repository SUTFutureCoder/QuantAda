import argparse
import ast
import datetime
import logging
import sys

import pandas

import config
from backtest.backtester import Backtester
from common import optimizer
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
    parser.add_argument('--cash', type=float, default=100000.0, help="初始资金 (默认: 100000.0)")
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
    parser.add_argument('--n_jobs', type=int, default=-1, help="[优化模式] 并行核心数 (-1 表示使用所有核心)")
    parser.add_argument('--metric', type=str, default='calmar',
                        choices=['sharpe', 'return', 'final_value', 'calmar', 'mix_score'],  # 添加 mix_score
                        help="[优化模式] 优化目标")
    parser.add_argument('--study_name', type=str, default=None, help="[优化模式] 训练名称")
    parser.add_argument('--train_roll_period', type=str, default=None, help="[优化模式] 动态滚动训练，start_date往前使用T-X作为训练集。例如：1w、1m、6m、1y。需要配合start_date、end_date一起使用")
    parser.add_argument('--train_ratio', type=float, default=None, help="[优化模式] 比例切分训练集、测试集")
    parser.add_argument('--train_period', type=str, default=None, help="[优化模式] 训练集时段")
    parser.add_argument('--test_period', type=str, default=None, help="[优化模式] 测试集时段")

    # 实盘参数
    parser.add_argument('--connect', type=str, default=None,
                        help="实盘连接配置，格式 'broker:env' (例如: 'gm_broker:sim')")

    # 3. 解析参数
    args = parser.parse_args()

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
        from live_trader.launcher import launch_live

        launch_live(broker_name, conn_name, args.strategy, s_params, **exec_args)
        sys.exit(0)

    # ==========================
    # 优化模式
    # ==========================
    if args.opt_params:
        print(f"\n>>> Mode: PARAMETER OPTIMIZATION (Target: {args.metric}) <<<")

        if not args.study_name:
            # 提取策略名，并将 '.' 替换为 '_' 以适配文件名规范
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            args.study_name = f"study_{pascal_to_snake(args.strategy)}_{timestamp}"
            print(f"[System] Auto-generated study name: {args.study_name}")

        # --- 变更点 2: 显式关闭日志 (从 optimizer.py 移至此处) ---
        config.LOG = False
        logging.getLogger("optuna").setLevel(logging.INFO)

        try:
            opt_p_def = ast.literal_eval(args.opt_params)
        except Exception as e:
            print(f"Error parsing opt_params JSON: {e}")
            sys.exit(1)

        job = optimizer.OptimizationJob(
            args=args,
            fixed_params=s_params,
            opt_params_def=opt_p_def,
            risk_params=r_params
        )
        job.run()
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
                initial_cash=args.cash, commission=args.commission
            ))
        except Exception as e:
            print(f"Failed to init DBRecorder: {e}")

    if hasattr(config, 'HTTP_LOG_URL') and config.HTTP_LOG_URL:
        recorder_manager.add_recorder(HttpRecorder(endpoint_url=config.HTTP_LOG_URL))

    run_backtest(
        selection_filename=args.selection,
        strategy_filename=args.strategy,
        symbols=symbol_list,
        cash=args.cash,
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