import atexit
import signal
import sys
import threading
from .dingtalk_alarm import DingTalkAlarm
from .wecom_alarm import WeComAlarm
import config


class AlarmManager:
    _instance = None
    _lock = threading.Lock()

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

        self._register_dead_letter_handlers()
        self._initialized = True
        print(f"[AlarmManager] Initialized with {len(self.alarms)} channels.")

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
        self.push_status("STOPPED", "Program exited normally.")
        pass

    def _signal_handler(self, sig, frame):
        """捕获中断信号"""
        sig_name = "SIGINT (Ctrl+C)" if sig == signal.SIGINT else "SIGTERM"
        print(f"\n[AlarmManager] Caught signal {sig_name}. Sending Dead Letter...")
        self.push_status("DEAD", f"Process killed by {sig_name}")
        sys.exit(0)

    # --- 统一推送接口 ---

    def push_text(self, content):
        for alarm in self.alarms:
            threading.Thread(target=alarm.push_text, args=(content,)).start()

    def push_exception(self, context, error):
        for alarm in self.alarms:
            # 异常推送建议同步发送，防止主进程崩溃导致发不出去
            try:
                alarm.push_exception(context, error)
            except:
                pass

    def push_trade(self, order_info):
        for alarm in self.alarms:
            threading.Thread(target=alarm.push_trade, args=(order_info,)).start()

    def push_start(self, strategy_name):
        self.push_status("STARTED", f"Strategy: {strategy_name}")

    def push_status(self, status, detail=""):
        for alarm in self.alarms:
            # 使用线程发送，避免阻塞系统启动或退出流程
            threading.Thread(target=alarm.push_status, args=(status, detail)).start()