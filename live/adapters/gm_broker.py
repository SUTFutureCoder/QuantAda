import pandas as pd

from .base_broker import BaseBroker

try:
    from gm.api import order_target_percent, order_volume, get_positions, OrderSide_Buy, OrderSide_Sell
except ImportError:
    print("警告：gm未安装。掘金实盘的交易将失败。")


class GMOrderProxy:
    """模拟Backtrader的Order对象，用于notify_order"""

    def __init__(self, gm_order_obj):
        self._order = gm_order_obj

    def is_pending(self):
        # 状态1=已报, 2=部成
        return self._order and self._order.status in [1, 2]


class GMBroker(BaseBroker):
    """掘金(GM)平台的Broker适配器"""

    def __init__(self, context, symbol):
        self.context = context
        self.symbol = symbol
        self._position = None
        self._history_data = pd.DataFrame()

    def update(self):
        """每个bar开始时，由主循环调用，用以更新内部状态"""
        self._update_position()
        self._update_history_data()

    def _update_position(self):
        positions = get_positions()
        pos_obj = next((p for p in positions if p['symbol'] == self.symbol), None)
        # 简化处理：用一个简单对象来模拟backtrader的position
        if pos_obj and pos_obj['volume'] > 0:
            self._position = type('Position', (object,), {'size': pos_obj['volume']})()
        else:
            self._position = None

    def _update_history_data(self, length=100):
        """获取最新的历史数据用于指标计算"""
        df = self.context.data(symbol=self.symbol, count=length, fields='close')
        self._history_data = df

    # 实现 BaseBroker 接口
    def buy(self, size=None, price=None, exectype=None):
        if size is None:
            self.log("警告：调用buy()但未指定size，不执行任何操作。")
            return None

        self.log(f"GM_BROKER：收到买入信号，目标股数：{size}")
        order = order_volume(symbol=self.symbol, volume=size, side=OrderSide_Buy, order_type=2, price=0)
        return GMOrderProxy(order)

    def sell(self, size=None, price=None, exectype=None):
        """按股数卖出。如果size为None则不执行任何操作"""
        if size is None:
            self.log(f"警告：调用sell()但未指定size，不执行任何操作")
            return None

        self.log(f"GM_BROKER：收到卖出信号，目标股数：{size}")
        order = order_volume(symbol=self.symbol, volume=size, side=OrderSide_Sell, order_type=2, price=0)
        return GMOrderProxy(order)

    def order_target_percent(self, target=None):
        if target is None:
            self.log(f"警告：order_target_percent()但未指定target，不执行任何操作")
            return None

        self.log(f"GM_BROKER: 收到调仓信号，目标仓位百分比: {target:.2%}")
        order = order_target_percent(symbol=self.symbol, percent=target, side=OrderSide_Buy, order_type=2, price=0)
        return GMOrderProxy(order)
