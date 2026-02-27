import atexit
import math
import signal
import socket
import sys
import threading
import time
from typing import Dict, Tuple

import config
from .dingtalk_alarm import DingTalkAlarm
from .wecom_alarm import WeComAlarm


class AlarmManager:
    _instance = None
    _lock = threading.Lock()
    _TEXT_AGGREGATION_WINDOW_SECONDS = 60
    _COOLDOWN_BASE_DELAY_SECONDS = 30
    _COOLDOWN_MAX_DELAY_SECONDS = 10 * 60
    _COOLDOWN_RESET_WINDOW_SECONDS = 15 * 60

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

        # 二级冷却(分钟聚合后): 对重复报警进行对数回退推送，降低告警疲劳
        self._cooldown_base_delay_seconds = self._COOLDOWN_BASE_DELAY_SECONDS
        self._cooldown_max_delay_seconds = self._COOLDOWN_MAX_DELAY_SECONDS
        self._cooldown_reset_window_seconds = self._COOLDOWN_RESET_WINDOW_SECONDS

        self._text_cooldown_lock = threading.Lock()
        self._text_cooldown_state: Dict[Tuple[str, str], Dict[str, float]] = {}
        self._text_cooldown_timer = None
        self._text_cooldown_day = time.strftime("%Y-%m-%d", time.localtime())

        self._exception_cooldown_lock = threading.Lock()
        self._exception_cooldown_state: Dict[Tuple[str, str], Dict[str, float]] = {}
        self._exception_cooldown_timer = None
        self._exception_cooldown_day = self._text_cooldown_day

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
        self._flush_text_cooldown_pending(force=True)
        self._flush_exception_cooldown_pending(force=True)
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

    @staticmethod
    def _merge_text_content(content, count):
        if count > 1:
            return f"{content}\n\n重复次数: {count}"
        return content

    @staticmethod
    def _merge_exception_content(error_text, count):
        if count > 1:
            return f"{error_text}\n重复次数: {count}"
        return error_text

    def _compute_log_cooldown_delay(self, k):
        delay = self._cooldown_base_delay_seconds * (1 + math.log1p(max(0, k)))
        return min(self._cooldown_max_delay_seconds, delay)

    @staticmethod
    def _build_new_cooldown_state(now):
        return {
            "k": 0,
            "pending_count": 0,
            "next_allowed_at": now,
            "last_seen_at": now,
        }

    def _maybe_reset_text_cooldown_day_locked(self):
        current_day = time.strftime("%Y-%m-%d", time.localtime())
        if current_day == self._text_cooldown_day:
            return
        self._text_cooldown_day = current_day
        self._text_cooldown_state.clear()
        if self._text_cooldown_timer and self._text_cooldown_timer.is_alive():
            self._text_cooldown_timer.cancel()
        self._text_cooldown_timer = None

    def _maybe_reset_exception_cooldown_day_locked(self):
        current_day = time.strftime("%Y-%m-%d", time.localtime())
        if current_day == self._exception_cooldown_day:
            return
        self._exception_cooldown_day = current_day
        self._exception_cooldown_state.clear()
        if self._exception_cooldown_timer and self._exception_cooldown_timer.is_alive():
            self._exception_cooldown_timer.cancel()
        self._exception_cooldown_timer = None

    def _prune_text_cooldown_state_locked(self, now):
        stale_keys = []
        for key, state in self._text_cooldown_state.items():
            if state["pending_count"] > 0:
                continue
            if now - state["last_seen_at"] >= self._cooldown_reset_window_seconds:
                stale_keys.append(key)
        for key in stale_keys:
            self._text_cooldown_state.pop(key, None)

    def _prune_exception_cooldown_state_locked(self, now):
        stale_keys = []
        for key, state in self._exception_cooldown_state.items():
            if state["pending_count"] > 0:
                continue
            if now - state["last_seen_at"] >= self._cooldown_reset_window_seconds:
                stale_keys.append(key)
        for key in stale_keys:
            self._exception_cooldown_state.pop(key, None)

    def _reschedule_text_cooldown_timer_locked(self, now):
        if self._text_cooldown_timer and self._text_cooldown_timer.is_alive():
            self._text_cooldown_timer.cancel()
        self._text_cooldown_timer = None

        next_fire_at = None
        for state in self._text_cooldown_state.values():
            if state["pending_count"] <= 0:
                continue
            next_allowed_at = state["next_allowed_at"]
            if next_fire_at is None or next_allowed_at < next_fire_at:
                next_fire_at = next_allowed_at

        if next_fire_at is None:
            return

        delay = max(0.01, next_fire_at - now)
        self._text_cooldown_timer = threading.Timer(delay, self._flush_text_cooldown_pending)
        self._text_cooldown_timer.daemon = True
        self._text_cooldown_timer.start()

    def _reschedule_exception_cooldown_timer_locked(self, now):
        if self._exception_cooldown_timer and self._exception_cooldown_timer.is_alive():
            self._exception_cooldown_timer.cancel()
        self._exception_cooldown_timer = None

        next_fire_at = None
        for state in self._exception_cooldown_state.values():
            if state["pending_count"] <= 0:
                continue
            next_allowed_at = state["next_allowed_at"]
            if next_fire_at is None or next_allowed_at < next_fire_at:
                next_fire_at = next_allowed_at

        if next_fire_at is None:
            return

        delay = max(0.01, next_fire_at - now)
        self._exception_cooldown_timer = threading.Timer(delay, self._flush_exception_cooldown_pending)
        self._exception_cooldown_timer.daemon = True
        self._exception_cooldown_timer.start()

    def _flush_text_cooldown_pending(self, force=False):
        dispatch_items = []
        now = time.time()
        with self._text_cooldown_lock:
            if self._text_cooldown_timer and self._text_cooldown_timer.is_alive():
                self._text_cooldown_timer.cancel()
            self._text_cooldown_timer = None
            self._maybe_reset_text_cooldown_day_locked()

            for (level, content), state in self._text_cooldown_state.items():
                if state["pending_count"] <= 0:
                    continue
                if not force and now < state["next_allowed_at"]:
                    continue

                dispatch_count = int(state["pending_count"])
                state["pending_count"] = 0
                state["last_seen_at"] = now
                dispatch_items.append((level, content, dispatch_count))

                if force:
                    state["k"] = 0
                    state["next_allowed_at"] = now
                else:
                    state["k"] += 1
                    state["next_allowed_at"] = now + self._compute_log_cooldown_delay(state["k"])

            if force:
                self._text_cooldown_state.clear()
            else:
                self._prune_text_cooldown_state_locked(now)
            self._reschedule_text_cooldown_timer_locked(now)

        for level, content, count in dispatch_items:
            self._dispatch_text(self._merge_text_content(content, count), level)

    def _flush_exception_cooldown_pending(self, force=False):
        dispatch_items = []
        now = time.time()
        with self._exception_cooldown_lock:
            if self._exception_cooldown_timer and self._exception_cooldown_timer.is_alive():
                self._exception_cooldown_timer.cancel()
            self._exception_cooldown_timer = None
            self._maybe_reset_exception_cooldown_day_locked()

            for (context, error_text), state in self._exception_cooldown_state.items():
                if state["pending_count"] <= 0:
                    continue
                if not force and now < state["next_allowed_at"]:
                    continue

                dispatch_count = int(state["pending_count"])
                state["pending_count"] = 0
                state["last_seen_at"] = now
                dispatch_items.append((context, error_text, dispatch_count))

                if force:
                    state["k"] = 0
                    state["next_allowed_at"] = now
                else:
                    state["k"] += 1
                    state["next_allowed_at"] = now + self._compute_log_cooldown_delay(state["k"])

            if force:
                self._exception_cooldown_state.clear()
            else:
                self._prune_exception_cooldown_state_locked(now)
            self._reschedule_exception_cooldown_timer_locked(now)

        for context, error_text, count in dispatch_items:
            self._dispatch_exception(context, self._merge_exception_content(error_text, count))

    def _process_text_cooldown_batch(self, buffered_items):
        if not buffered_items:
            return

        dispatch_items = []
        now = time.time()
        with self._text_cooldown_lock:
            self._maybe_reset_text_cooldown_day_locked()
            for (level, content), count in buffered_items:
                if level == "CRITICAL":
                    dispatch_items.append((level, content, count))
                    continue

                key = (level, content)
                state = self._text_cooldown_state.get(key)
                if not state or now - state["last_seen_at"] >= self._cooldown_reset_window_seconds:
                    state = self._build_new_cooldown_state(now)
                    self._text_cooldown_state[key] = state

                state["last_seen_at"] = now
                state["pending_count"] += count

                if now >= state["next_allowed_at"]:
                    dispatch_count = int(state["pending_count"])
                    state["pending_count"] = 0
                    dispatch_items.append((level, content, dispatch_count))
                    state["k"] += 1
                    state["next_allowed_at"] = now + self._compute_log_cooldown_delay(state["k"])

            self._prune_text_cooldown_state_locked(now)
            self._reschedule_text_cooldown_timer_locked(now)

        for level, content, count in dispatch_items:
            self._dispatch_text(self._merge_text_content(content, count), level)

    def _process_exception_cooldown_batch(self, buffered_items):
        if not buffered_items:
            return

        dispatch_items = []
        now = time.time()
        with self._exception_cooldown_lock:
            self._maybe_reset_exception_cooldown_day_locked()
            for (context, error_text), count in buffered_items:
                key = (context, error_text)
                state = self._exception_cooldown_state.get(key)
                if not state or now - state["last_seen_at"] >= self._cooldown_reset_window_seconds:
                    state = self._build_new_cooldown_state(now)
                    self._exception_cooldown_state[key] = state

                state["last_seen_at"] = now
                state["pending_count"] += count

                if now >= state["next_allowed_at"]:
                    dispatch_count = int(state["pending_count"])
                    state["pending_count"] = 0
                    dispatch_items.append((context, error_text, dispatch_count))
                    state["k"] += 1
                    state["next_allowed_at"] = now + self._compute_log_cooldown_delay(state["k"])

            self._prune_exception_cooldown_state_locked(now)
            self._reschedule_exception_cooldown_timer_locked(now)

        for context, error_text, count in dispatch_items:
            self._dispatch_exception(context, self._merge_exception_content(error_text, count))

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

        self._process_text_cooldown_batch(buffered_items)

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

        self._process_exception_cooldown_batch(buffered_items)
