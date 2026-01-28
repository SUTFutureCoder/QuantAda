from common.indicators import MACD, CrossOver
from .base_strategy import BaseStrategy


class SampleCustomIndicatorStrategy(BaseStrategy):
    """
    MACD金叉死叉策略的纯净实现，使用自定义计算方法。
    计算指标更灵活，但需要自行处理K线索引，稍微麻烦一些
    """
    params = {
        'exitbars': 5,
        'SHORT': 12,
        'LONG': 26,
        'M': 9,
    }

    # !!!注意，初始化方法只会执行一次，如果将计算逻辑写到这里会有不重新计算的风险!!!
    def init(self):
        try:
            # 1. 获取完整的原始DataFrame，它的索引是我们将要使用的DatetimeIndex
            dataframe = self.broker.data.p.dataname
        except AttributeError:
            raise Exception("数据必须通过 bt.feeds.PandasData 加载才能在init中进行完整计算")

        close_series = dataframe['close']
        dif, dea, macd_bar = MACD(close_series, self.p.SHORT, self.p.LONG, self.p.M)
        self.crossover = CrossOver(dif, dea)

        self.order = None
        self.min_period = self.p.LONG + self.p.M

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
                self.log(f'BUY CREATE, Buying {size_to_buy} shares, Price: {self.broker.dataclose[0]:.2f}')
                self.order = self.broker.buy(size=size_to_buy)
        else:
            if current_crossover < 0:
                self.log(f'SELL SIGNAL: Closing position.')
                self.order = self.broker.order_target_percent(target=0.0)

    def notify_order(self, order):
        super().notify_order(order)
        if not order.is_pending():
            self.order = None
