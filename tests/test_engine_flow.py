from datetime import datetime
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

import config
from data_providers.base_provider import BaseDataProvider
from live_trader.engine import LiveTrader
from strategies.base_strategy import BaseStrategy


# 动态加载 Phase1/2 的单测桩，避免依赖 tests 目录作为可导入包
_broker_state_path = Path(__file__).with_name("test_broker_state.py")
_spec = importlib.util.spec_from_file_location("test_broker_state_for_e2e", _broker_state_path)
_mod = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader, "无法加载 test_broker_state.py，E2E 测试缺少基础 MockBroker。"
_spec.loader.exec_module(_mod)
MockBroker = _mod.MockBroker
MockOrderProxy = _mod.MockOrderProxy


class DummyDataProvider(BaseDataProvider):
    """E2E 集成测试专用数据源：仅返回稳定的内存 K 线。"""

    PRIORITY = 1

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None,
                 timeframe: str = "Days", compression: int = 1) -> pd.DataFrame:
        end = pd.to_datetime(end_date) if end_date else pd.Timestamp.now().normalize()
        idx = pd.date_range(end=end, periods=5, freq="D")
        df = pd.DataFrame(
            {
                "open": [10.0, 10.0, 10.0, 10.0, 10.0],
                "high": [10.2, 10.2, 10.2, 10.2, 10.2],
                "low": [9.8, 9.8, 9.8, 9.8, 9.8],
                "close": [10.0, 10.0, 10.0, 10.0, 10.0],
                "volume": [10000, 10000, 10000, 10000, 10000],
            },
            index=idx,
        )
        return df

    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = "Days", compression: int = 1) -> pd.DataFrame:
        return self.get_data(symbol, start_date, end_date, timeframe, compression)


class MockEngineBroker(MockBroker):
    """
    引擎集成测试 Broker:
    - 继承单元测试阶段的 MockBroker
    - 支持在途单跟踪与手动撤单/状态回放
    """

    def __init__(self, context, cash_override=None, commission_override=None, slippage_override=None):
        initial_cash = float(cash_override if cash_override is not None else 100000.0)
        super().__init__(initial_cash=initial_cash)
        self._context = context
        self._commission_override = commission_override
        self._slippage_override = slippage_override

        # E2E 场景: 账户初始持有 1000 股，便于 14:45 触发清仓 sell
        self.mock_position = 1000

        # 内存订单簿: 记录每笔单的状态，支持 pending/cancelled/filled 手工推进
        self._order_book = {}
        self.cancel_requests = []

    def _submit_order(self, data, volume, side, price):
        oid = f"ORDER_{len(self.submitted_orders) + 1}"
        self.submitted_orders.append(
            {
                "id": oid,
                "side": side,
                "volume": volume,
                "symbol": data._name,
                "status": "Submitted",
            }
        )
        self._order_book[oid] = self.submitted_orders[-1]
        return MockOrderProxy(oid, is_buy_order=(side == "BUY"), status="Submitted")

    def get_pending_orders(self):
        pending = []
        for order in self.submitted_orders:
            if order.get("status") in {"PendingSubmit", "Submitted", "PendingCancel"}:
                pending.append(
                    {
                        "id": order["id"],
                        "symbol": order["symbol"],
                        "direction": order["side"],
                        "size": order["volume"],
                    }
                )
        return pending

    def cancel_order(self, oid: str):
        order = self._order_book.get(oid)
        if not order:
            return None
        if order.get("status") in {"Filled", "Canceled", "Rejected"}:
            return None
        order["status"] = "PendingCancel"
        self.cancel_requests.append(oid)
        return oid

    def simulate_order_status(self, oid: str, status: str):
        order = self._order_book.get(oid)
        if not order:
            return None

        order["status"] = status
        proxy = MockOrderProxy(oid, is_buy_order=(order["side"] == "BUY"), status=status)
        self.on_order_status(proxy)

        # 若测试需要推进真实持仓，可通过 Filled 回放更新持仓
        if status == "Filled":
            if order["side"] == "BUY":
                self.mock_position += order["volume"]
            elif order["side"] == "SELL":
                self.mock_position = max(0, self.mock_position - order["volume"])
        return proxy

    @staticmethod
    def is_live_mode(context):
        return True


class DummyHeartbeatStrategy(BaseStrategy):
    """
    心跳策略:
    - 09:35 建仓 target=0.5
    - 10:00 遍历 pending 并发起撤单
    - 14:45 清仓 target=0
    """

    def init(self):
        self.order = None
        self._did_open = False
        self._did_cancel = False
        self._did_close = False

    def next(self):
        now = self.broker.datetime.datetime()
        data = self.broker.datas[0]
        hhmm = now.strftime("%H:%M")

        if hhmm == "09:35" and not self._did_open:
            self.order = self.broker.order_target_percent(data=data, target=0.5)
            self._did_open = True

        elif hhmm == "10:00" and not self._did_cancel:
            for po in list(self.broker.get_pending_orders()):
                self.broker.cancel_order(po["id"])
            self._did_cancel = True

        elif hhmm == "14:45" and not self._did_close:
            self.order = self.broker.order_target_percent(data=data, target=0.0)
            self._did_close = True

    def notify_order(self, order):
        super().notify_order(order)
        # 集成测试中不锁单，保证下一个心跳仍会执行 next
        self.order = None


class MockContext:
    def __init__(self, now):
        self.now = now


@pytest.fixture(autouse=True)
def _force_lot_size(monkeypatch):
    monkeypatch.setattr(config, "LOT_SIZE", 100)


def test_full_day_engine_lifecycle(monkeypatch):
    """
    E2E Red Team Test:
    还原完整日内链路: 09:35 买入 -> 10:00 撤单并回调 -> 14:45 清仓。
    """
    import live_trader.engine as engine_module

    # 1) 劫持引擎动态装配，接入内存版 Broker/DataProvider/Strategy
    monkeypatch.setattr(
        engine_module.LiveTrader,
        "_load_adapter_classes",
        lambda self, platform: (MockEngineBroker, DummyDataProvider),
    )
    monkeypatch.setattr(
        engine_module,
        "get_class_from_name",
        lambda class_name, paths: DummyHeartbeatStrategy,
    )

    cfg = {
        "strategy_name": "DummyHeartbeatStrategy",
        "platform": "mock_engine",
        "symbols": ["SHSE.600000"],
        "cash": 100000.0,
        "params": {},
    }

    engine = LiveTrader(cfg)
    context = MockContext(now=datetime(2026, 2, 17, 9, 30, 0))
    engine.init(context)

    # Step 1: 09:35 开盘建仓
    context.now = datetime(2026, 2, 17, 9, 35, 0)
    engine.run(context)

    buy_orders = [o for o in engine.broker.submitted_orders if o["side"] == "BUY"]
    assert len(buy_orders) == 1, "开盘阶段未触发买单，E2E 流程在第一步即失败！"

    buy_order = buy_orders[0]
    expected_deduct = buy_order["volume"] * 10.0 * engine.broker.safety_multiplier
    assert engine.broker._virtual_spent_cash == pytest.approx(expected_deduct), (
        "开盘建仓后虚拟账本扣减异常，资金安全垫记账与下单量不一致！"
    )
    assert buy_order["id"] in engine.broker._active_buys, "开盘买单未进入 _active_buys，后续撤单/拒单回调将失效！"

    # Step 2: 10:00 主动撤单 + 券商回调 Canceled
    context.now = datetime(2026, 2, 17, 10, 0, 0)
    engine.run(context)

    assert buy_order["id"] in engine.broker.cancel_requests, "10:00 未触发主动撤单，策略心跳逻辑失效！"

    canceled_proxy = engine.broker.simulate_order_status(buy_order["id"], "Canceled")
    engine.notify_order(canceled_proxy)

    assert engine.broker._virtual_spent_cash == pytest.approx(0.0), (
        "买单撤销后资金未回滚，发生幽灵账本冻结，可能导致后续无法下单！"
    )
    assert buy_order["id"] not in engine.broker._active_buys, "_active_buys 未清理，状态机会被旧订单污染！"
    assert len(engine.broker.get_pending_orders()) == 0, "撤单回调后 pending 队列未清空，存在死锁风险！"

    # Step 3: 14:45 收盘清仓
    context.now = datetime(2026, 2, 17, 14, 45, 0)
    engine.run(context)

    sell_orders = [o for o in engine.broker.submitted_orders if o["side"] == "SELL"]
    assert len(sell_orders) >= 1, "收盘阶段未触发清仓卖单，目标仓位归零链路失效！"
    assert sell_orders[-1]["volume"] == 1000, "清仓卖单数量异常，应与当前真实持仓 1000 股对齐！"


def test_backtest_mode_auto_warmup_start_date(monkeypatch):
    """
    回测模式默认预热:
    传入 start_date 后，引擎应自动向前拉取 ANNUAL_FACTOR 天历史。
    """
    import live_trader.engine as engine_module

    class RecordingDataProvider(DummyDataProvider):
        def __init__(self):
            super().__init__()
            self.history_calls = []

        def get_history(self, symbol: str, start_date: str, end_date: str,
                        timeframe: str = "Days", compression: int = 1) -> pd.DataFrame:
            self.history_calls.append(
                {
                    "symbol": symbol,
                    "start_date": start_date,
                    "end_date": end_date,
                    "timeframe": timeframe,
                    "compression": compression,
                }
            )
            return super().get_history(symbol, start_date, end_date, timeframe, compression)

    class MockBacktestBroker(MockEngineBroker):
        @staticmethod
        def is_live_mode(context):
            return False

        @staticmethod
        def extract_run_config(context):
            return {
                "start_date": "20260201",
                "end_date": "20260213",
                "cash": 100000.0,
            }

    monkeypatch.setattr(config, "ANNUAL_FACTOR", 252)
    monkeypatch.setattr(
        engine_module.LiveTrader,
        "_load_adapter_classes",
        lambda self, platform: (MockBacktestBroker, RecordingDataProvider),
    )
    monkeypatch.setattr(
        engine_module,
        "get_class_from_name",
        lambda class_name, paths: DummyHeartbeatStrategy,
    )

    cfg = {
        "strategy_name": "DummyHeartbeatStrategy",
        "platform": "mock_engine",
        "symbols": ["SHSE.600000"],
        "cash": 100000.0,
        "params": {},
    }

    engine = LiveTrader(cfg)
    context = MockContext(now=datetime(2026, 2, 17, 9, 30, 0))
    engine.init(context)

    calls = engine.data_provider.history_calls
    assert calls, "回测初始化阶段未触发历史数据拉取。"

    expected_warmup_start = (pd.Timestamp("2026-02-01") - pd.Timedelta(days=252)).strftime("%Y-%m-%d")
    assert calls[0]["start_date"] == expected_warmup_start, "回测预热起点不正确，未按默认窗口前推。"
    assert calls[0]["end_date"] == "20260213", "回测结束日期透传异常。"
