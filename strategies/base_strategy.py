from abc import ABC, abstractmethod


class BaseStrategy(ABC):
    """
    策略抽象基类
    策略作者只需要继承这个类，并实现其核心逻辑。
    'broker'对象将由外部引擎（回测或实盘）注入，它提供了所有交易和数据访问的接口。
    """

    params = {}

    def __init__(self, broker, indicators, params=None):
        """
        初始化策略参数
        :param params:
        """
        self.broker = broker
        self.indicators = indicators
        if params:
            self.params.update(params)

    def log(self, txt, dt=None):
        """
        通用日志记录
        """
        dt = dt or dt.utcnow()
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
        pass

    def notify_trade(self, trade):
        """
        交易成交通知
        """
        pass
