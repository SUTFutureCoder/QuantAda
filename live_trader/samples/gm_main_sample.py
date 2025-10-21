# 掘金实盘样例

# coding=utf-8
from __future__ import print_function, absolute_import

try:
    from gm.api import schedule, run, MODE_LIVE
except ImportError:
    print("This script is intended to be run within the GM Quant platform.")

# 1. 引入实盘引擎 (需先配置好PYTHONPATH)
from live_trader.engine import LiveTrader

# 2. 定义交易配置 (与回测时的命令行参数几乎完全对应)
# 对应命令为：python ./run.py ReverseTraderMultipleActionsStrategy --selection=ReverseTraderMultipleActionsSelector --cash=500000 --start_date=20230101
config = {
    'platform': 'gm',  # 指定平台为掘金

    # --- 策略与选股 (与回测命令一致) ---
    'strategy_name': 'ReverseTraderMultipleActionsStrategy',
    'selection_name': 'ReverseTraderMultipleActionsSelector',
    # 'symbols': ['SHSE.600519', 'SZSE.000001'], # 如果不用选股器，则手动指定

    # --- 资金与佣金 (与回测命令一致) ---
    # 'cash': 500000.0, # 【虚拟分仓】若指定，则使用此金额作为策略资金；若注释掉，则使用真实账户全部可用资金(实盘)或run函数初始资金(回测)
    'commission': 0.0001,  # 由于框架无法获取掘金框架的backtest_commission_ratio，因此如果回测有佣金，也请设置同样的数值

    # --- 数据相关 ---
    # 'start_date': '2023-01-01',  # 定义获取历史数据进行指标计算的起始点

    # --- 策略自定义参数 ---
    'params': {
        'selectTopK': 2,
        'target_buffer': 0.98,
    }
}

# 3. 创建引擎实例
trader = LiveTrader(config)


# 4. 对接掘金的生命周期函数
def init(context):
    trader.init(context)
    # 设置策略执行频率
    schedule(schedule_func=trader.run, date_rule='1d', time_rule='14:45:00')


# 掘金平台默认的回测/实盘运行入口，创建策略时由掘金平台自动创建，无需修改
if __name__ == '__main__':
    run(strategy_id='your_strategy_id',
        filename='main.py',
        mode=MODE_LIVE,  # 或 MODE_BACKTEST
        token='your_token',
        # ... 其他掘金所需参数
        )
