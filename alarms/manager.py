import atexit
import signal
import socket
import sys
import threading
from typing import Dict, Tuple

import config
from .dingtalk_alarm import DingTalkAlarm
from .wecom_alarm import WeComAlarm


class AlarmManager:
    _instance = None
    _lock = threading.Lock()
    _TEXT_AGGREGATION_WINDOW_SECONDS = 60

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(AlarmManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return

        self.alarms = []
        if config.ALARMS_ENABLED:
            # 加载钉钉
            if config.DINGTALK_WEBHOOK:
                self.alarms.append(DingTalkAlarm())
            # 加载企业微信
            if config.WECOM_WEBHOOK:
                self.alarms.append(WeComAlarm())

        # 运行时身份上下文
        self.host_name = socket.gethostname()
        self.context_tag = ""  # 例如: [IB:7497]
        self.context_detail = ""  # 例如: 策略参数详情

        # 文本报警聚合: 默认开启，固定 60 秒窗口内合并相同(level, content)
        self._text_aggregation_window_seconds = self._TEXT_AGGREGATION_WINDOW_SECONDS
        self._text_aggregation_lock = threading.Lock()
        self._text_aggregation_buffer: Dict[Tuple[str, str], int] = {}
        self._text_flush_timer = None

        # 异常报警聚合: 默认开启，固定 60 秒窗口内合并相同(context, error)
        self._exception_aggregation_window_seconds = self._TEXT_AGGREGATION_WINDOW_SECONDS
        self._exception_aggregation_lock = threading.Lock()
        self._exception_aggregation_buffer: Dict[Tuple[str, str], int] = {}
        self._exception_flush_timer = None

        self._register_dead_letter_handlers()
        self._initialized = True
        print(f"[AlarmManager] Initialized with {len(self.alarms)} channels.")

    # 供 Launcher 调用，注入身份信息
    def set_runtime_context(self, broker, conn_id, strategy, params, market_scope=""):
        """
        设置运行时上下文，用于区分多实例
        """
        self.context_tag = f"[{broker.upper()}:{conn_id}]"

        # 格式化参数详情，便于在报警中查看
        param_str = ", ".join([f"{k}={v}" for k, v in params.items()]) if params else "None"
        market_str = market_scope if market_scope else "N/A"
        self.context_detail = (
            f"Machine: {self.host_name}\n"
            f"Strategy: {strategy}\n"
            f"Market: {market_str}\n"
            f"Params: {param_str}"
        )

    def _register_dead_letter_handlers(self):
        """注册死信监听 (Ctrl+C, Kill, 正常退出)"""

        # 1. 正常退出钩子
        atexit.register(self._on_exit)

        # 2. 信号监听 (Ctrl+C, Kill)
        # 注意：在 Windows 下某些信号可能受限，但在 Linux/Docker 中很有效
        try:
            signal.signal(signal.SIGINT, self._signal_handler)  # Ctrl+C
            signal.signal(signal.SIGTERM, self._signal_handler)  # Kill
        except ValueError:
            # 说明可能不在主线程运行，跳过信号注册
            pass

    def _on_exit(self):
        """程序正常结束的回调"""
        # 只有在非异常退出时才发 Finish，如果是 crash，会被 Exception 捕获
        # 这里可以发一个简单的结束报告
        self._flush_text_aggregation()
        self._flush_exception_aggregation()
        self.push_status("STOPPED", "Program exited normally.")
        pass

    def _signal_handler(self, sig, frame):
        """捕获中断信号"""
        sig_name = "SIGINT (Ctrl+C)" if sig == signal.SIGINT else "SIGTERM"
        print(f"\n[AlarmManager] Caught signal {sig_name}. Sending Dead Letter...")
        self.push_status("DEAD", f"Process killed by {sig_name}")
        sys.exit(0)

    # --- 统一推送接口 ---

    def push_text(self, content, level='INFO'):
        if not self.alarms:
            return

        if self.context_tag:
            content = f"""### {self.context_tag}
{content}         
"""

        self._buffer_text_alarm(content, level)

    def push_exception(self, context, error):
        if not self.alarms:
            return

        full_context = f"{self.context_tag} {context} @ {self.host_name}"
        error_text = str(error).strip()
        self._buffer_exception_alarm(full_context, error_text)

    def push_trade(self, order_info):
        for alarm in self.alarms:
            threading.Thread(target=alarm.push_trade, args=(order_info,)).start()

    def push_start(self, strategy_name):
        detail = self.context_detail if self.context_detail else f"Strategy: {strategy_name}"
        self.push_status("STARTED", detail)

    def push_status(self, status, detail=""):
        full_status = f"{status} {self.context_tag}" if self.context_tag else status

        # 如果 detail 里没有包含机器信息，自动补全 (防止重复叠加)
        full_detail = detail
        if self.host_name not in detail:
            if self.context_detail:
                full_detail = f"{detail}\n---\n{self.context_detail}"
            else:
                full_detail = f"{detail}\nHost: {self.host_name}"

        for alarm in self.alarms:
            threading.Thread(target=alarm.push_status, args=(full_status, full_detail)).start()

    def _dispatch_text(self, content, level):
        for alarm in self.alarms:
            threading.Thread(target=alarm.push_text, args=(content, level)).start()

    def _buffer_text_alarm(self, content, level):
        key = (level, content)
        with self._text_aggregation_lock:
            self._text_aggregation_buffer[key] = self._text_aggregation_buffer.get(key, 0) + 1

            if self._text_flush_timer and self._text_flush_timer.is_alive():
                return

            self._text_flush_timer = threading.Timer(
                self._text_aggregation_window_seconds,
                self._flush_text_aggregation
            )
            self._text_flush_timer.daemon = True
            self._text_flush_timer.start()

    def _flush_text_aggregation(self):
        with self._text_aggregation_lock:
            if self._text_flush_timer and self._text_flush_timer.is_alive():
                self._text_flush_timer.cancel()
            buffered_items = list(self._text_aggregation_buffer.items())
            self._text_aggregation_buffer.clear()
            self._text_flush_timer = None

        for (level, content), count in buffered_items:
            merged_content = content
            if count > 1:
                merged_content = f"{content}\n\n重复次数: {count}"
            self._dispatch_text(merged_content, level)

    def _dispatch_exception(self, context, error):
        for alarm in self.alarms:
            # 异常推送保持同步发送，提升进程异常场景下的送达概率
            try:
                alarm.push_exception(context, error)
            except:
                pass

    def _buffer_exception_alarm(self, context, error_text):
        key = (context, error_text)
        with self._exception_aggregation_lock:
            self._exception_aggregation_buffer[key] = self._exception_aggregation_buffer.get(key, 0) + 1

            if self._exception_flush_timer and self._exception_flush_timer.is_alive():
                return

            self._exception_flush_timer = threading.Timer(
                self._exception_aggregation_window_seconds,
                self._flush_exception_aggregation
            )
            self._exception_flush_timer.daemon = True
            self._exception_flush_timer.start()

    def _flush_exception_aggregation(self):
        with self._exception_aggregation_lock:
            if self._exception_flush_timer and self._exception_flush_timer.is_alive():
                self._exception_flush_timer.cancel()
            buffered_items = list(self._exception_aggregation_buffer.items())
            self._exception_aggregation_buffer.clear()
            self._exception_flush_timer = None

        for (context, error_text), count in buffered_items:
            merged_error = error_text
            if count > 1:
                merged_error = f"{error_text}\n重复次数: {count}"
            self._dispatch_exception(context, merged_error)
