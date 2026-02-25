import time

import pytest

import alarms.manager as manager_module
from alarms.manager import AlarmManager
from live_trader.engine import _format_market_scope


class FakeAlarmChannel:
    def __init__(self):
        self.text_calls = []
        self.status_calls = []

    def push_text(self, content: str, level: str = 'INFO'):
        self.text_calls.append((content, level))

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
    yield mgr

    timer = getattr(mgr, "_text_flush_timer", None)
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

    market_scope = "selector=us_growth_selector | symbols=QQQ,SPY,IWM"
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


def test_format_market_scope_prefers_selector_and_symbols():
    scope = _format_market_scope(selection="cn_topn_selector", symbols=["SHSE.510300", "SZSE.159915"])
    assert scope == "selector=cn_topn_selector | symbols=SHSE.510300,SZSE.159915"
