from abc import ABC, abstractmethod


class BaseAlarm(ABC):
    """报警器基类"""

    @abstractmethod
    def push_text(self, content: str, level: str = 'INFO'):
        """推送普通文本消息"""
        pass

    @abstractmethod
    def push_exception(self, context: str, error: Exception):
        """推送异常信息"""
        pass

    @abstractmethod
    def push_trade(self, order_info: dict):
        """推送交易成交信息"""
        pass

    @abstractmethod
    def push_status(self, status: str, detail: str = ""):
        """推送系统状态变更 (启动/停止/死信)"""
        pass