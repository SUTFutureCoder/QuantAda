import argparse
import importlib
import os

from data_providers.manager import DataManager
from data_providers.csv_provider import CsvDataProvider
from data_providers.akshare_provider import AkshareDataProvider
from data_providers.tushare_provider import TushareDataProvider

from backtest.backtester import Backtester

# 动态获取 Python 安装目录，并构建 Tcl/Tk 库路径
python_install_dir = os.path.dirname(os.path.dirname(os.__file__))
tcl_library_path = os.path.join(python_install_dir, 'tcl', 'tcl8.6')  # 版本号请根据实际情况调整
tk_library_path = os.path.join(python_install_dir, 'tcl', 'tk8.6')

# 设置环境变量
os.environ['TCL_LIBRARY'] = tcl_library_path
os.environ['TK_LIBRARY'] = tk_library_path


def get_strategy_class(strategy_filename):
    """动态导入策略类"""
    module_name = strategy_filename.replace('.py', '')
    module_path = f'strategies.{module_name}'
    strategy_module = importlib.import_module(module_path)

    class_name_parts = [s.capitalize() for s in module_name.split('_')]
    class_name = "".join(class_name_parts)

    try:
        strategy_class = getattr(strategy_module, class_name)
    except AttributeError:
        class_name = module_name.replace('_', ' ').title().replace(' ', '')
        strategy_class = getattr(strategy_module, class_name)

    return strategy_class

def run_backtest(strategy_filename, symbol, cash, commission):
    """执行回测"""
    print("--- Starting Backtest ---")
    print(f"  Strategy: {strategy_filename}")
    print(f"  Symbol: {symbol}")
    print(f"  Initial Cash: {cash:,.2f}")
    print(f"  Commission: {commission:.4f}")

    # 1. 定义数据提供者列表，按优先级排序
    data_providers = [
        CsvDataProvider(),
        AkshareDataProvider(),
        TushareDataProvider()
    ]
    # 2. 实例化数据管理器
    data_manager = DataManager(providers=data_providers)

    # 3. 获取数据
    print("\n--- Fetching Data ---")
    data = data_manager.get_data(symbol)

    if data is None or data.empty:
        print("\nFatal: Could not get data for the specified symbol. Aborting backtest.")
        return

    print("\n--- Initializing Backtester ---")
    strategy_class = get_strategy_class(strategy_filename)

    backtester = Backtester(
        data,
        strategy_class,
        cash=cash,
        commission=commission
    )
    backtester.run()


if __name__ == '__main__':
    # 1. 创建命令行解析器
    parser = argparse.ArgumentParser(
        description="量化策略回测框架运行器",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # 2. 添加命令行参数
    parser.add_argument(
        'strategy',
        type=str,
        help="要运行的策略文件名 (例如: sample_macd_cross_strategy.py 或 sample_macd_cross_strategy)"
    )

    parser.add_argument(
        '--symbol',
        type=str,
        default='SHSE.510300',
        help="回测标的代码 (默认: SHSE.510300)"
    )

    parser.add_argument(
        '--cash',
        type=float,
        default=100000.0,
        help="初始资金 (默认: 100000.0)"
    )

    parser.add_argument(
        '--commission',
        type=float,
        default=0.00015,
        help="手续费率 (默认: 0.00015)"
    )

    # 3. 解析参数
    args = parser.parse_args()

    # 4. 调用回测函数
    run_backtest(
        strategy_filename=args.strategy,
        symbol=args.symbol,
        cash=args.cash,
        commission=args.commission
    )

    print("\n--- Backtest Finished ---")
