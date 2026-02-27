import time

import pytest

import alarms.manager as manager_module
from alarms.manager import AlarmManager
from live_trader.engine import _format_market_scope


class FakeAlarmChannel:
    def __init__(self):
        self.text_calls = []
        self.exception_calls = []
        self.status_calls = []

    def push_text(self, content: str, level: str = 'INFO'):
        self.text_calls.append((content, level))

    def push_exception(self, context: str, error):
        self.exception_calls.append((context, str(error)))

    def push_status(self, status: str, detail: str = ''):
        self.status_calls.append((status, detail))


def _wait_until(predicate, timeout_sec=2.0):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def fresh_alarm_manager(monkeypatch):
    AlarmManager._instance = None

    # 单测中禁用死信注册，避免重复绑定 atexit/signal 干扰
    monkeypatch.setattr(manager_module.AlarmManager, "_register_dead_letter_handlers", lambda self: None)
    monkeypatch.setattr(manager_module.config, "ALARMS_ENABLED", False)

    mgr = AlarmManager()
    # 生产默认 60 秒窗口，单测缩短到 1 秒以避免等待过久
    mgr._text_aggregation_window_seconds = 1
    mgr._exception_aggregation_window_seconds = 1
    yield mgr

    timer = getattr(mgr, "_text_flush_timer", None)
    if timer:
        timer.cancel()
    timer = getattr(mgr, "_exception_flush_timer", None)
    if timer:
        timer.cancel()
    timer = getattr(mgr, "_text_cooldown_timer", None)
    if timer:
        timer.cancel()
    timer = getattr(mgr, "_exception_cooldown_timer", None)
    if timer:
        timer.cancel()
    AlarmManager._instance = None


def test_push_text_aggregates_same_alarm_with_count(fresh_alarm_manager):
    mgr = fresh_alarm_manager
    fake = FakeAlarmChannel()
    mgr.alarms = [fake]

    mgr.push_text("订单被拒绝: SHSE.600000", level="WARNING")
    mgr.push_text("订单被拒绝: SHSE.600000", level="WARNING")

    assert fake.text_calls == [], "聚合窗口内不应立即发送重复文本报警。"
    assert _wait_until(lambda: len(fake.text_calls) == 1), "聚合窗口到期后应触发一次合并推送。"

    content, level = fake.text_calls[0]
    assert level == "WARNING", "合并推送应保留原始报警级别。"
    assert "订单被拒绝: SHSE.600000" in content, "合并推送应保留原始报警内容。"
    assert "重复次数: 2" in content, "相同报警应附带聚合数量。"


def test_runtime_context_status_detail_contains_market_scope(fresh_alarm_manager):
    mgr = fresh_alarm_manager
    fake = FakeAlarmChannel()
    mgr.alarms = [fake]

    market_scope = "selector=us_growth_selector"
    mgr.set_runtime_context(
        broker="ib_broker",
        conn_id="7497",
        strategy="my_strategy",
        params={"lookback": 20},
        market_scope=market_scope,
    )
    mgr.push_status("STARTED", "Booting")

    assert _wait_until(lambda: len(fake.status_calls) == 1), "状态推送应到达报警通道。"
    status, detail = fake.status_calls[0]
    assert status == "STARTED [IB_BROKER:7497]", "状态标题应包含 broker/连接身份上下文。"
    assert f"Market: {market_scope}" in detail, "状态详情应包含 selector/symbols 市场上下文。"


def test_push_exception_aggregates_same_error_with_count(fresh_alarm_manager):
    mgr = fresh_alarm_manager
    fake = FakeAlarmChannel()
    mgr.alarms = [fake]

    mgr.push_exception("GM Kernel Error", "Code: 1100, Msg: 交易消息服务连接失败")
    mgr.push_exception("GM Kernel Error", "Code: 1100, Msg: 交易消息服务连接失败")

    assert fake.exception_calls == [], "聚合窗口内不应立即发送重复异常报警。"
    assert _wait_until(lambda: len(fake.exception_calls) == 1), "聚合窗口到期后应触发一次异常合并推送。"

    context, error_text = fake.exception_calls[0]
    assert "GM Kernel Error" in context, "异常合并推送应保留原始模块上下文。"
    assert "Code: 1100, Msg: 交易消息服务连接失败" in error_text, "异常合并推送应保留原始错误内容。"
    assert "重复次数: 2" in error_text, "相同异常应附带聚合数量。"


def test_text_log_cooldown_merges_counts_across_windows(fresh_alarm_manager):
    mgr = fresh_alarm_manager
    fake = FakeAlarmChannel()
    mgr.alarms = [fake]
    mgr._text_aggregation_window_seconds = 0.2
    mgr._cooldown_base_delay_seconds = 0.2
    mgr._cooldown_max_delay_seconds = 0.4
    mgr._cooldown_reset_window_seconds = 5

    mgr.push_text("风控告警: 持仓超限", level="WARNING")
    assert _wait_until(lambda: len(fake.text_calls) == 1, timeout_sec=1.0), "首轮窗口到期后应立即推送。"

    mgr.push_text("风控告警: 持仓超限", level="WARNING")
    mgr.push_text("风控告警: 持仓超限", level="WARNING")
    time.sleep(0.25)
    assert len(fake.text_calls) == 1, "冷却期内不应立即推送第二轮重复告警。"
    assert _wait_until(lambda: len(fake.text_calls) == 2, timeout_sec=1.5), "冷却到期后应推送累计数量。"

    content, level = fake.text_calls[1]
    assert level == "WARNING", "冷却后推送应保留原始级别。"
    assert "风控告警: 持仓超限" in content, "冷却后推送应保留原始内容。"
    assert "重复次数: 2" in content, "冷却期间应累计重复数量后再推送。"


def test_exception_log_cooldown_merges_counts_across_windows(fresh_alarm_manager):
    mgr = fresh_alarm_manager
    fake = FakeAlarmChannel()
    mgr.alarms = [fake]
    mgr._exception_aggregation_window_seconds = 0.2
    mgr._cooldown_base_delay_seconds = 0.2
    mgr._cooldown_max_delay_seconds = 0.4
    mgr._cooldown_reset_window_seconds = 5

    error_text = "Code: 1100, Msg: 交易消息服务连接失败"
    mgr.push_exception("GM Kernel Error", error_text)
    assert _wait_until(lambda: len(fake.exception_calls) == 1, timeout_sec=1.0), "首轮异常窗口到期后应立即推送。"

    mgr.push_exception("GM Kernel Error", error_text)
    mgr.push_exception("GM Kernel Error", error_text)
    time.sleep(0.25)
    assert len(fake.exception_calls) == 1, "异常冷却期内不应立即推送第二轮重复告警。"
    assert _wait_until(lambda: len(fake.exception_calls) == 2, timeout_sec=1.5), "异常冷却到期后应推送累计数量。"

    context, merged_error = fake.exception_calls[1]
    assert "GM Kernel Error" in context, "冷却后异常推送应保留原始模块上下文。"
    assert error_text in merged_error, "冷却后异常推送应保留原始错误内容。"
    assert "重复次数: 2" in merged_error, "异常冷却期间应累计重复数量后再推送。"


def test_critical_text_bypasses_log_cooldown(fresh_alarm_manager):
    mgr = fresh_alarm_manager
    fake = FakeAlarmChannel()
    mgr.alarms = [fake]
    mgr._text_aggregation_window_seconds = 0.2
    mgr._cooldown_base_delay_seconds = 0.3
    mgr._cooldown_max_delay_seconds = 1.0
    mgr._cooldown_reset_window_seconds = 5

    mgr.push_text("交易通道全断开", level="CRITICAL")
    assert _wait_until(lambda: len(fake.text_calls) == 1, timeout_sec=1.0), "CRITICAL 首次窗口到期后应推送。"
    assert mgr._text_cooldown_state == {}, "CRITICAL 不应进入冷却状态。"

    mgr.push_text("交易通道全断开", level="CRITICAL")
    assert _wait_until(lambda: len(fake.text_calls) == 2, timeout_sec=0.5), "CRITICAL 不应被冷却延迟。"
    assert mgr._text_cooldown_state == {}, "CRITICAL 重复推送也不应进入冷却状态。"


def test_format_market_scope_prefers_selector_only():
    scope = _format_market_scope(selection="cn_topn_selector", symbols=["SHSE.510300", "SZSE.159915"])
    assert scope == "selector=cn_topn_selector"
