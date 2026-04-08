from datetime import datetime
from types import SimpleNamespace

import pytest

import config
from strategies.base_strategy import BaseStrategy


class DummyStrategy(BaseStrategy):
    def init(self):
        pass

    def next(self):
        pass


class DummyData:
    def __init__(self, name):
        self._name = name


class DummyBroker:
    def __init__(self, cash=0.0, rebalance_cash=None):
        self.is_live = True
        self.datas = [DummyData("AAPL.SMART")]
        self._cash = cash
        self._rebalance_cash = rebalance_cash

    def log(self, txt, dt=None):
        return None

    def get_pending_orders(self):
        return []

    def getposition(self, data):
        return SimpleNamespace(size=3.0, price=0.0)

    def get_current_price(self, data):
        return 100.0

    def get_cash(self):
        return self._cash

    def get_rebalance_cash(self):
        if self._rebalance_cash is None:
            raise RuntimeError("rebalance cash unavailable")
        return self._rebalance_cash


def test_strategy_isolated_capital_prefers_rebalance_cash(monkeypatch):
    """
    资金口径回归:
    策略分配资金应优先使用 broker.get_rebalance_cash，避免与下单资金语义撕裂。
    """
    monkeypatch.setattr(config, "IGNORED_SYMBOLS", [])
    broker = DummyBroker(cash=1000.0, rebalance_cash=200.0)
    strategy = DummyStrategy(broker=broker, params={})

    allocatable, current_positions = strategy.get_strategy_isolated_capital()

    # managed_market_value = 3 * 100 = 300
    assert allocatable == pytest.approx(500.0), "策略分配资金应采用 rebalance_cash + managed_value。"
    assert len(current_positions) == 1, "应识别到 1 个受管持仓。"


def test_strategy_isolated_capital_fallbacks_to_get_cash_on_rebalance_error(monkeypatch):
    """
    稳定性回归:
    get_rebalance_cash 异常时，策略层应回退 get_cash，不能中断调仓流程。
    """
    monkeypatch.setattr(config, "IGNORED_SYMBOLS", [])
    broker = DummyBroker(cash=1000.0, rebalance_cash=None)
    strategy = DummyStrategy(broker=broker, params={})

    allocatable, _ = strategy.get_strategy_isolated_capital()

    # fallback: get_cash + managed_market_value = 1000 + 300
    assert allocatable == pytest.approx(1300.0), "rebalance 口径异常时应回退到 get_cash 口径。"


def test_notify_order_prefers_execution_dt_for_logs(monkeypatch):
    """
    成交日志时间回归:
    notify_order 应优先使用订单真实执行时间，而不是 broker 当前触发时间。
    """
    monkeypatch.setattr(config, "IGNORED_SYMBOLS", [])

    class RecordingBroker(DummyBroker):
        def __init__(self):
            super().__init__(cash=0.0, rebalance_cash=0.0)
            self.log_calls = []

        def log(self, txt, dt=None):
            self.log_calls.append({"txt": txt, "dt": dt})

    class DummyOrder:
        def __init__(self):
            self.executed = SimpleNamespace(
                size=100.0,
                price=10.0,
                value=1000.0,
                comm=0.0,
                dt=datetime(2026, 4, 8, 14, 45, 33),
            )

        def is_completed(self):
            return True

        def is_buy(self):
            return False

        def is_sell(self):
            return True

        def is_rejected(self):
            return False

    broker = RecordingBroker()
    strategy = DummyStrategy(broker=broker, params={})

    strategy.notify_order(DummyOrder())

    assert len(broker.log_calls) == 1, "成交日志应被记录一次。"
    assert broker.log_calls[0]["dt"].isoformat() == "2026-04-08T14:45:33", "成交日志应使用真实执行时间。"
