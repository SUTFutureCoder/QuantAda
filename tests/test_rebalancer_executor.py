from types import SimpleNamespace


def test_order_executor_waits_for_sell_settlement_then_buys(monkeypatch):
    import common.rebalancer as rebalancer_module

    clock = {"t": 0.0}

    def _fake_time():
        return clock["t"]

    def _fake_sleep(seconds):
        clock["t"] += float(seconds)

    monkeypatch.setattr(rebalancer_module.time, "time", _fake_time)
    monkeypatch.setattr(rebalancer_module.time, "sleep", _fake_sleep)

    class DummyBroker:
        def __init__(self):
            self.calls = []
            self.sync_calls = 0

        def order_target_value(self, data, target):
            self.calls.append((data._name, float(target)))
            return object()

        def get_pending_orders(self):
            if clock["t"] < 3.0:
                return [{"id": "S1", "symbol": "SPY.ARCA", "direction": "SELL", "size": 100}]
            return []

        def sync_balance(self):
            self.sync_calls += 1

    broker = DummyBroker()
    executor = rebalancer_module.OrderExecutor(broker)
    plan = {
        "sell_clear": [SimpleNamespace(_name="SPY.ARCA")],
        "reduce": [],
        "increase": [(SimpleNamespace(_name="EWJ.ARCA"), 100000.0)],
    }

    executor.execute_plan(plan)

    assert broker.calls == [("SPY.ARCA", 0.0), ("EWJ.ARCA", 100000.0)], "应先卖后买，且卖单终态后再买入。"
    assert clock["t"] >= 3.0, "应等待卖单终态。"
    assert broker.sync_calls == 1, "卖单终态后应同步资金。"


def test_order_executor_skips_buy_when_sell_not_settled_in_60s(monkeypatch):
    import common.rebalancer as rebalancer_module

    clock = {"t": 0.0}

    def _fake_time():
        return clock["t"]

    def _fake_sleep(seconds):
        clock["t"] += float(seconds)

    monkeypatch.setattr(rebalancer_module.time, "time", _fake_time)
    monkeypatch.setattr(rebalancer_module.time, "sleep", _fake_sleep)

    pushed = []

    class DummyAlarmManager:
        def push_text(self, content, level="INFO"):
            pushed.append({"content": content, "level": level})

    monkeypatch.setattr(rebalancer_module, "AlarmManager", lambda: DummyAlarmManager())

    class DummyBroker:
        def __init__(self):
            self.calls = []

        def order_target_value(self, data, target):
            self.calls.append((data._name, float(target)))
            return object()

        def get_pending_orders(self):
            return [{"id": "S1", "symbol": "SPY.ARCA", "direction": "SELL", "size": 100}]

    broker = DummyBroker()
    executor = rebalancer_module.OrderExecutor(broker)
    plan = {
        "sell_clear": [SimpleNamespace(_name="SPY.ARCA")],
        "reduce": [],
        "increase": [(SimpleNamespace(_name="EWJ.ARCA"), 100000.0)],
    }

    executor.execute_plan(plan)

    assert broker.calls == [("SPY.ARCA", 0.0)], "卖单超时未终态时，本轮应跳过买入。"
    assert clock["t"] >= 60.0, "卖单等待应持续到 60 秒超时。"
    assert len(pushed) == 1, "超时应推送一次告警。"
    assert pushed[0]["level"] == "WARNING"
    assert "跳过买入" in pushed[0]["content"]


def test_order_executor_waits_local_pending_sells_even_if_remote_empty(monkeypatch):
    import common.rebalancer as rebalancer_module

    clock = {"t": 0.0}

    def _fake_time():
        return clock["t"]

    def _fake_sleep(seconds):
        clock["t"] += float(seconds)
        if clock["t"] >= 2.0:
            broker._pending_sells.clear()

    monkeypatch.setattr(rebalancer_module.time, "time", _fake_time)
    monkeypatch.setattr(rebalancer_module.time, "sleep", _fake_sleep)

    class DummyBroker:
        def __init__(self):
            self.calls = []
            self.sync_calls = 0
            self._pending_sells = set()

        def order_target_value(self, data, target):
            self.calls.append((data._name, float(target)))
            if float(target) == 0.0:
                self._pending_sells.add("SELL_LOCAL_1")
                return SimpleNamespace(id="SELL_LOCAL_1")
            return object()

        def get_pending_orders(self):
            # 模拟券商 open orders 可见性延迟：短时间内返回空。
            return []

        def sync_balance(self):
            self.sync_calls += 1

    broker = DummyBroker()
    executor = rebalancer_module.OrderExecutor(broker)
    plan = {
        "sell_clear": [SimpleNamespace(_name="SPY.ARCA")],
        "reduce": [],
        "increase": [(SimpleNamespace(_name="EWJ.ARCA"), 100000.0)],
    }

    executor.execute_plan(plan)

    assert broker.calls == [("SPY.ARCA", 0.0), ("EWJ.ARCA", 100000.0)], "本地 pending_sells 未清空前不应放行买单。"
    assert clock["t"] >= 2.0, "应等待本地 pending_sells 进入终态。"
    assert broker.sync_calls == 1
