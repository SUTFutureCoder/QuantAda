from abc import ABC, abstractmethod


class BaseStrategy(ABC):
    """
    策略抽象基类
    策略作者只需要继承这个类，并实现其核心逻辑。
    'broker'对象将由外部引擎（回测或实盘）注入，它提供了所有交易和数据访问的接口。
    """

    params = {}

    def __init__(self, broker, params=None):
        """
        初始化策略参数
        :param params:
        """
        self.broker = broker
        if params:
            self.params.update(params)

    def log(self, txt, dt=None):
        """
        通用日志记录
        """
        self.broker.log(txt, dt)

    @abstractmethod
    def init(self):
        """
        策略初始化，在这里准备指标等
        """
        pass

    @abstractmethod
    def next(self):
        """
        每个K线周期调用的核心逻辑。
        """
        pass

    def notify_order(self, order):
        """
        订单状态通知
        """
        if order.is_completed():
            if order.is_buy():
                self.log(
                    f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
            elif order.is_sell():
                self.log(
                    f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
        elif order.is_rejected():
            self.log(f'Order Canceled/Rejected/Margin')

    def notify_trade(self, trade):
        """
        交易成交通知
        """
        if trade.is_closed():
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')
