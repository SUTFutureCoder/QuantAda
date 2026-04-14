from abc import ABC, abstractmethod


class BaseAlarm(ABC):
    """报警器基类"""

    # 语义标签常量:
    # - 用于 AlarmManager 侧的策略判定（如时间窗绕过、未来按标签定制配置）
    # - 不强制要求各报警通道自行解析；默认由 AlarmManager 消化
    TAG_GENERAL = "general"
    TAG_LIFECYCLE = "lifecycle"
    TAG_PLAN = "plan"
    TAG_TRADE = "trade"
    TAG_ORDER_EVENT = "order_event"
    TAG_RUNTIME_WARNING = "runtime_warning"

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
