import backtrader as bt

from .base_strategy import BaseStrategy


class SampleMomentumStrategy(BaseStrategy):
    """
    动量策略示例：
    当价格高于 N 日前的价格时买入，否则卖出。
    参数：
      momentum_period: 动量周期 (默认 20)
    """
    # 定义默认参数，这些参数可以被 Optuna 覆盖
    params = {
        'momentum_period': 20
    }

    def init(self):
        # 计算动量：当前收盘价 - N日前收盘价
        # period=self.p.momentum_period 读取参数
        self.mom = bt.ind.Momentum(self.broker.datas[0], period=self.p.momentum_period)

    def next(self):
        # 简单的交易逻辑
        if not self.broker.position:
            if self.mom[0] > 0:
                self.log(f'BUY CREATE, Momentum: {self.mom[0]:.2f}')
                self.broker.order_target_percent(target=0.95)
        else:
            if self.mom[0] < 0:
                self.log(f'SELL CREATE, Momentum: {self.mom[0]:.2f}')
                self.broker.order_target_percent(target=0.0)