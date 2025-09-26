import backtrader as bt

from .indicators import CustomMACD, CustomCrossOver


class OrderProxy:
    def __init__(self, bt_order): self._order = bt_order

    def is_buy(self): return self._order.isbuy()

    def is_sell(self): return self._order.issell()

    def is_pending(self): return self._order.status in [self._order.Submitted, self._order.Accepted]

    def is_completed(self): return self._order.status == self._order.Completed

    def is_rejected(self): return self._order.status in [self._order.Canceled, self._order.Margin,
                                                         self._order.Rejected]

    @property
    def executed(self): return self._order.executed


class TradeProxy:
    """将backtrader的trade对象适配成通用接口"""

    def __init__(self, bt_trade): self._trade = bt_trade

    def is_closed(self): return self._trade.isclosed

    @property
    def pnl(self): return self._trade.pnl

    @property
    def pnlcomm(self): return self._trade.pnlcomm


class BacktraderIndicatorFactory:
    """为Backtrader环境创建指标的工厂"""

    def __init__(self, data):
        self._data = data

    def MACD(self):
        return CustomMACD(self._data)

    def CrossOver(self, a, b):
        return CustomCrossOver(a, b)


class BacktraderStrategyWrapper(bt.Strategy):
    """
    Backtrader的包装器策略
    唯一职责是加载我们的纯策略，并将Backtrader的环境传递给它
    """

    def __init__(self, strategy_class, strategy_params=None):
        self.dataclose = self.datas[0].close

        indicator_factory = BacktraderIndicatorFactory(self)
        self.strategy = strategy_class(broker=self, indicators=indicator_factory, params=strategy_params)
        self.strategy.init()

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def next(self):
        self.strategy.next()

    def notify_order(self, order):
        self.strategy.notify_order(OrderProxy(order))

    def notify_trade(self, trade):
        self.strategy.notify_trade(TradeProxy(trade))


class Backtester:
    # 回测执行器
    def __init__(self, data, strategy_class, strategy_params=None, cash=100000.0, commission=0.00015,
                 sizer_class=bt.sizers.PercentSizer, sizer_params={'percents': 95}):
        self.cerebro = bt.Cerebro()
        self.data = data
        self.strategy_class = strategy_class
        self.strategy_params = strategy_params
        self.cash = cash
        self.commission = commission
        self.sizer_class = sizer_class
        self.sizer_params = sizer_params

    def run(self):
        # 将数据添加到Cerebro
        feed = bt.feeds.PandasData(dataname=self.data)
        self.cerebro.adddata(feed)

        # 添加包装后的策略
        self.cerebro.addstrategy(
            BacktraderStrategyWrapper,
            strategy_class=self.strategy_class,
            strategy_params=self.strategy_params
        )

        # 设置初始资金和手续费
        self.cerebro.broker.setcash(self.cash)
        self.cerebro.broker.setcommission(commission=self.commission)

        # 动态添加Sizer
        self.cerebro.addsizer(self.sizer_class, **self.sizer_params)

        print(f"Initial Portfolio Value: {self.cerebro.broker.getvalue():.2f}")
        self.cerebro.run()
        print(f"Final Portfolio Value: {self.cerebro.broker.getvalue():.2f}")

        self.cerebro.plot()
