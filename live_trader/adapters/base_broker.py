from abc import ABC, abstractmethod

import pandas as pd


class BaseOrderProxy(ABC):
    """
    订单代理的抽象基类。
    所有平台的具体订单代理都必须实现这些与 backtrader 兼容的方法。
    """

    @abstractmethod
    def is_completed(self) -> bool: pass

    @abstractmethod
    def is_canceled(self) -> bool: pass

    @abstractmethod
    def is_rejected(self) -> bool: pass

    @abstractmethod
    def is_pending(self) -> bool: pass

    @abstractmethod
    def is_accepted(self) -> bool: pass

    @abstractmethod
    def is_buy(self) -> bool: pass

    @abstractmethod
    def is_sell(self) -> bool: pass


class BaseLiveDataProvider(ABC):
    """数据提供者适配器的抽象基类"""

    @abstractmethod
    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:
        """获取指定标的的历史日线数据"""
        pass


class BaseLiveBroker(ABC):
    """交易执行器适配器的抽象基类，模拟 backtrader 的 broker 接口"""

    def __init__(self, context, cash_override=None, commission_override=None):
        self._context = context
        self.datas = []
        self._datetime = None
        self._cash_override = cash_override
        self._commission_override = commission_override
        self._cash = self._init_cash()
        self._commission = self._init_commission()

    @staticmethod
    @abstractmethod
    def is_live_mode(context) -> bool:
        """
        判断当前是否为实盘模式
        """
        pass

    @staticmethod
    def extract_run_config(context) -> dict:
        """
        静态方法：从特定平台的上下文中提取运行配置。
        默认返回空字典，子类应重写此方法以实现特定逻辑。
        """
        return {}

    @abstractmethod
    def order_target_percent(self, data, target) -> BaseOrderProxy:
        """
        下单方法必须返回一个 BaseOrderProxy 的实例。
        """
        pass

    def _init_cash(self):
        """初始化资金：优先使用自定义cash，否则获取真实账户资金"""
        if self._cash_override is not None:
            print(f"[Live Broker] Using custom cash override: {self._cash_override:,.2f}")
            return self._cash_override
        real_cash = self._fetch_real_cash()
        print(f"[Live Broker] Using real account available cash: {real_cash:,.2f}")
        return real_cash

    def _init_commission(self):
        """初始化：使用费率"""
        if self._commission_override is not None:
            print(f"[Live Broker] Using custom commission override: {self._commission_override:,.2f}")
            return self._commission_override
        return 0.0

    @abstractmethod
    def _fetch_real_cash(self) -> float:
        """子类必须实现，用于获取真实账户的可用资金"""
        pass

    @abstractmethod
    def get_position(self, data):
        """子类必须实现，用于获取指定标的的持仓"""
        pass

    def getposition(self, data):
        """
        [API兼容写法]为了与backtrader的API（self.getposition()）保持一致
        策略代码应不感知实盘系统，直接调用此代码，自动调用子类实现的get_position()
        """
        return self.get_position(data)

    def get_cash(self) -> float:
        """返回当前策略可用的资金（可能是真实的或虚拟的）"""
        return self._cash

    def set_datas(self, datas):
        self.datas = datas

    def set_datetime(self, dt):
        self._datetime = dt

    @property
    def datetime(self):
        """模拟 backtrader 的 datetime 属性，使 asof() 等能工作"""

        class dt_proxy:
            def __init__(self, dt): self._dt = dt

            def datetime(self, ago=0): return self._dt

        return dt_proxy(self._datetime)

    def log(self, txt, dt=None):
        log_time = dt or self._datetime or pd.Timestamp.now()
        print(f"[{log_time.strftime('%Y-%m-%d %H:%M:%S')}] {txt}")
