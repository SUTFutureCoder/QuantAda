from abc import ABC, abstractmethod
from types import SimpleNamespace

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
        # 1. 合并类级别定义的默认参数和实例化时传入的参数
        final_params = self.params.copy()
        if params:
            final_params.update(params)

        # 2. 使用辅助类将最终的参数字典转换为一个对象
        self.params = SimpleNamespace(**final_params)

        # 3. 创建 'p' 作为 'params' 的快捷方式，以符合Backtrader的惯例
        self.p = self.params

    def log(self, txt, dt=None):
        """
        通用日志记录
        """
        self.broker.log(txt, dt)

    @abstractmethod
    def init(self):
        """
        策略初始化，在这里准备指标等
        !!!注意，初始化方法只会执行一次，如果将计算逻辑写到这里实盘会有不重新计算的风险，请抽象计算方法并放置于next中!!!
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
                    f'BUY EXECUTED, Size: {order.executed.size:.2f}, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
            elif order.is_sell():
                self.log(
                    f'SELL EXECUTED, Size: {order.executed.size:.2f}, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
        elif order.is_rejected():
            self.log(f'Order Canceled/Rejected/Margin')

    def notify_trade(self, trade):
        """
        交易成交通知
        """
        if trade.is_closed():
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')
