# ==============================================================================
#  掘金量化实盘入口文件
#  使用方法:
#  1. 将本项目中的 common/, strategies/, live/adapters/ 文件夹手动复制到
#     掘金策略的根目录。
#  2. 将本文件的内容复制到掘金策略的 main.py 中。
#  3. 确保掘金环境已安装 talib (如果指标需要)。
# ==============================================================================
from __future__ import print_function, absolute_import, unicode_literals

try:
    from gm.api import *
except ImportError:
    print("警告：gm未安装。掘金实盘的交易将失败。")

# --- 引入框架代码 ---
# 假设 common, strategies, live/adapters 文件夹已被复制到掘金策略根目录
from strategies.sample_macd_cross_strategy import SampleMACDCrossStrategy  # <--【重要】在这里导入您想运行的策略
from live.adapters.gm_broker import GMBroker


# --- 掘金平台回调函数 ---

def init(context):
    """策略启动时初始化"""
    print("Initializing strategy for live trading...")

    # 订阅数据
    context.symbol = 'SHSE.510300'  # 在这里修改您的交易标的
    subscribe(symbols=context.symbol, frequency='60s', count=120)  # 订阅分钟线，并获取前120根bar用于计算指标

    # 实例化Broker和策略
    gm_broker = GMBroker(context, context.symbol)

    # 【重要】在这里实例化您想运行的策略
    context.strategy = SampleMACDCrossStrategy(broker=gm_broker, indicators=gm_broker.indicators)

    # 更新数据并初始化策略
    gm_broker.update()
    context.strategy.init()

    print("Strategy Initialized Successfully.")


def on_bar(context, bars):
    """每个Bar结束时触发"""
    bar = bars[0]
    print(f"New Bar Received: {bar.bob.strftime('%Y-%m-%d %H:%M:%S')}, Close: {bar.close}")

    # 1. 更新Broker状态（获取新数据和仓位）
    context.strategy.broker.update()

    # 2. 调用策略的next()核心逻辑
    context.strategy.next()


def on_order_status(context, order):
    """订单状态变更时触发"""
    print(f"Order Status Change: {order.cl_ord_id}, Status: {order.status}, Symbol: {order.symbol}")
    # 未来可以扩展这里的逻辑，以调用 strategy.notify_order


def on_backtest_finished(context, indicator):
    print('*' * 50)
    print('回测已完成。')


# --- 掘金运行配置 ---
if __name__ == '__main__':
    run(strategy_id='your_strategy_id',
        filename='main.py',
        mode=MODE_LIVE,  # 或 MODE_BACKTEST
        token='your_token',
        backtest_start_time='2022-01-01 09:30:00',
        backtest_end_time='2022-12-31 15:00:00')
