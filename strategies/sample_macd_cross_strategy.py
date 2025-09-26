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
            if self.crossover[0] > 0:
                size_to_buy = 100  # 或根据资金动态计算
                self.log(f'BUY CREATE, Buying {size_to_buy} shares, Price: {self.dataclose[0]:.2f}')
                self.order = self.broker.buy(size=size_to_buy)
        else:
            if self.crossover[0] < 0:
                self.log(f'SELL SIGNAL: Closing position.')
                self.order = self.broker.order_target_percent(target=0.0)

    def notify_order(self, order):
        if order.is_completed():
            if order.is_buy():
                self.log(
                    f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
            elif order.is_sell():
                self.log(
                    f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
        elif order.is_rejected():
            self.log(f'Order Canceled/Rejected/Margin')

        # 重置order状态，允许下新单
        if not order.is_pending():
            self.order = None

    def notify_trade(self, trade):
        if trade.is_closed():
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')
