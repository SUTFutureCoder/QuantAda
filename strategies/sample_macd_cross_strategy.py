from common import rules
from .base_strategy import BaseStrategy


class SampleMacdCrossStrategy(BaseStrategy):
    """
    MACD金叉死叉策略的纯净实现。
    """
    params = {
        'exitbars': 5,
    }

    def init(self):
        """
        初始化指标。
        注意这里我们通过 self.broker.indicators 来获取指标计算器。
        回测时，broker就是backtrader的strategy实例，它有indicators属性。
        实盘时，broker是我们的适配器，我们也需要为它实现indicators属性。
        """
        self.dataclose = self.broker.dataclose

        # 使用broker提供的指标工厂来创建指标
        macd_indicator = self.indicators.MACD()
        self.macd = macd_indicator.macd
        self.signal = macd_indicator.signal
        self.histo = self.macd - self.signal
        self.crossover = self.indicators.CrossOver(self.macd, self.signal)

        self.order = None

    def next(self):
        if self.order and self.order.is_pending():
            return

        if not self.broker.position:
            if rules.entry_signal_macd_golden_cross(self):
                size_to_buy = 100  # 或根据资金动态计算
                self.log(f'BUY CREATE, Buying {size_to_buy} shares, Price: {self.dataclose[0]:.2f}')
                self.order = self.broker.buy(size=size_to_buy)
        else:
            if rules.exit_signal_macd_dead_cross(self):
                self.log(f'SELL SIGNAL: Closing position.')
                self.order = self.broker.order_target_percent(target=0.0)
