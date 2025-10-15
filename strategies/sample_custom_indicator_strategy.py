import pandas as pd

from common.indicators import MACD, CrossOver
from .base_strategy import BaseStrategy


class SampleCustomIndicatorStrategy(BaseStrategy):
    """
    MACD金叉死叉策略的纯净实现，使用自定义计算方法。
    计算指标更灵活，但需要从backtrader.Line转换为pd.Series和np.Array
    """
    params = {
        'exitbars': 5,
        'SHORT': 12,
        'LONG': 26,
        'M': 9,
    }

    def init(self):
        """
        初始化指标。
        注意这里我们通过 self.broker.indicators 来获取指标计算器。
        回测时，broker就是backtrader的strategy实例，它有indicators属性。
        实盘时，broker是我们的适配器，我们也需要为它实现indicators属性。
        """
        self.dataclose = self.broker.dataclose

        close_series = pd.Series(self.dataclose.array)
        dif, dea, macd_bar = MACD(close_series, self.params['SHORT'], self.params['LONG'], self.params['M'])
        self.crossover = CrossOver(dif, dea)
        print(self.crossover)

        self.order = None
        self.min_period = self.params['LONG'] + self.params['M']

    def next(self):
        current_idx = len(self.broker.data) - 1

        # 如果数据不足以产生有效的指标值，则直接返回
        if current_idx < self.min_period:
            return

        if self.order:
            return

        # 通过索引访问预计算好的指标结果
        current_crossover = self.crossover[current_idx]

        if not self.broker.position:
            if current_crossover > 0:
                size_to_buy = 100  # 或根据资金动态计算
                self.log(f'BUY CREATE, Buying {size_to_buy} shares, Price: {self.dataclose[0]:.2f}')
                self.order = self.broker.buy(size=size_to_buy)
        else:
            if current_crossover < 0:
                self.log(f'SELL SIGNAL: Closing position.')
                self.order = self.broker.order_target_percent(target=0.0)

    def notify_order(self, order):
        super().notify_order(order)
        if not order.is_pending():
            self.order = None
