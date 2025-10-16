import argparse
import importlib
import os
import re

import pandas

from backtest.backtester import Backtester
from data_providers.manager import DataManager

# 动态获取 Python 安装目录，并构建 Tcl/Tk 库路径
python_install_dir = os.path.dirname(os.path.dirname(os.__file__))
tcl_library_path = os.path.join(python_install_dir, 'tcl', 'tcl8.6')  # 版本号请根据实际情况调整
tk_library_path = os.path.join(python_install_dir, 'tcl', 'tk8.6')

# 设置环境变量
os.environ['TCL_LIBRARY'] = tcl_library_path
os.environ['TK_LIBRARY'] = tk_library_path


def _pascal_to_snake(name: str) -> str:
    """
    将 PascalCase (大驼峰) 字符串转换为 snake_case (下划线) 字符串。
    例如: 'SampleMacdCrossStrategy' -> 'sample_macd_cross_strategy'
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_class_from_name(name_string: str, search_paths: list):
    """
    根据给定的名称字符串（文件名或类名）动态导入类。
    - 如果输入是 snake_case (e.g., 'sample_macd_cross_strategy'), 自动推断出类名 'SampleMacdCrossStrategy'。
    - 如果输入是 PascalCase (e.g., 'SampleMacdCrossStrategy'), 自动推断出文件名 'sample_macd_cross_strategy'。

    :param name_string: 文件名或类名字符串。
    :param search_paths: 搜索的目录列表, e.g., ['stock_selectors', 'strategies']
    :return: 动态导入的类
    """
    # 清理输入，移除可能的 .py 后缀
    name_string = name_string.replace('.py', '')

    # 启发式判断输入格式
    if '_' in name_string or name_string.islower():
        # 认为是 snake_case 文件名
        module_name = name_string
        class_name = "".join(word.capitalize() for word in module_name.split('_'))
    else:
        # 认为是 PascalCase 类名
        class_name = name_string
        module_name = _pascal_to_snake(class_name)

    # 遍历搜索路径尝试导入
    for path in search_paths:
        try:
            module_path = f'{path}.{module_name}'
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError):
            # 如果在一个路径中找不到，继续在下一个路径中寻找
            continue

    # 如果所有路径都尝试完毕仍未找到，则抛出异常
    raise ImportError(
        f"Could not find class '{class_name}' from module '{module_name}' "
        f"derived from input '{name_string}' in any of the search paths: {search_paths}"
    )


def run_backtest(selection_filename, strategy_filename, symbols, cash, commission, data_source, start_date, end_date):
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
            specified_sources=data_source
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

    backtester = Backtester(
        datas,
        strategy_class,
        start_date=start_date,
        end_date=end_date,
        cash=cash,
        commission=commission,
    )
    backtester.run()


if __name__ == '__main__':
    # 1. 创建命令行解析器
    parser = argparse.ArgumentParser(
        description="量化回测框架",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # 2. 添加命令行参数
    parser.add_argument('strategy', type=str,
                        help="要运行的策略文件名 (例如: sample_macd_cross_strategy.py 或 sample_macd_cross_strategy)")
    parser.add_argument('--selection', type=str, default=None, help="选股器文件名 (位于selectors目录)")
    parser.add_argument('--data_source', type=str, default=None,
                        help="指定数据源 (例如: csv akshare tushare sxsc_tushare)")
    parser.add_argument('--symbols', type=str, default='SHSE.510300', help="以,分割的回测标的代码 (默认: SHSE.510300)")
    parser.add_argument('--cash', type=float, default=100000.0, help="初始资金 (默认: 100000.0)")
    parser.add_argument('--commission', type=float, default=0, help="手续费率，例如：万1.5为:0.00015 (默认：0)")
    parser.add_argument('--start_date', type=str, default=None, help="回测起始日期 (例如: 20241111)")
    parser.add_argument('--end_date', type=str, default=None, help="回测结束日期 (例如: 20250101)")

    # 3. 解析参数
    args = parser.parse_args()

    # 将逗号分隔的字符串转换为列表
    symbol_list = [s.strip() for s in args.symbols.split(',')]

    # 4. 调用回测函数
    run_backtest(
        selection_filename=args.selection,
        strategy_filename=args.strategy,
        symbols=symbol_list,
        cash=args.cash,
        commission=args.commission,
        data_source=args.data_source,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    print("\n--- Backtest Finished ---")
