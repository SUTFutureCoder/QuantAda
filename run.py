import argparse
import importlib
import os

import akshare as ak
import pandas as pd

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


def get_data(symbol, end_date='20250101', data_path='data'):
    print("Downloading data from akshare...")
    ak_symbol = symbol.split('.')[1]
    df = ak.fund_etf_hist_em(symbol=ak_symbol, period='daily', end_date=end_date, adjust='hfq')
    df.rename(columns={'日期': 'datetime', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close',
                       '成交量': 'volume'}, inplace=True)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    df['openinterest'] = 0
    return df


def run_backtest(strategy_filename, symbol, cash, commission):
    """执行回测"""
    print("--- Starting Backtest ---")
    print(f"  Strategy: {strategy_filename}")
    print(f"  Symbol: {symbol}")
    print(f"  Initial Cash: {cash:,.2f}")
    print(f"  Commission: {commission:.4f}")

    strategy_class = get_strategy_class(strategy_filename)
    data = get_data(symbol)

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
