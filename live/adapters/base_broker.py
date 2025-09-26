from abc import ABC, abstractmethod


class BaseBroker(ABC):
    """
    实盘交易Broker的抽象基类
    每个实盘平台（掘金、QMT等）都需要实现这个接口
    """

    @abstractmethod
    def buy(self, size=None, price=None, exectype=None):
        pass

    @abstractmethod
    def sell(self, size=None, price=None, exectype=None):
        pass

    @abstractmethod
    def close(self):
        self.order_target_percent(target=0.0)

    @abstractmethod
    def order_target_percent(self, target=None):
        """按目标百分比调仓。"""
        pass

    @property
    @abstractmethod
    def positions(self):
        pass

    @property
    @abstractmethod
    def dataclose(self):
        pass

    @property
    @abstractmethod
    def indicators(self):
        pass

    @abstractmethod
    def log(self, txt, dt=None):
        pass
