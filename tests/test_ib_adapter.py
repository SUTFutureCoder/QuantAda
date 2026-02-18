import sys
import types

import pytest
from unittest.mock import MagicMock


# 拦截 ib_insync 导入，避免任何真实 TWS/Gateway 连接依赖
mock_ib_insync = types.ModuleType("ib_insync")
mock_ib_insync.IB = MagicMock(name="IB")
mock_ib_insync.Stock = MagicMock(name="Stock")
mock_ib_insync.MarketOrder = MagicMock(name="MarketOrder")
mock_ib_insync.Trade = MagicMock(name="Trade")
mock_ib_insync.Forex = MagicMock(name="Forex")
mock_ib_insync.Contract = MagicMock(name="Contract")
sys.modules["ib_insync"] = mock_ib_insync

# 重新导入被测模块，确保使用上面的 mock ib_insync
sys.modules.pop("live_trader.adapters.ib_broker", None)
from live_trader.adapters.ib_broker import IBOrderProxy


class DummyIBOrderStatus:
    def __init__(self, status, filled=0, avg_fill_price=0.0):
        self.status = status
        self.filled = filled
        self.avgFillPrice = avg_fill_price


class DummyIBOrder:
    def __init__(self, order_id=1, action="BUY", total_quantity=0):
        self.orderId = order_id
        self.action = action
        self.totalQuantity = total_quantity


class DummyCommissionReport:
    def __init__(self, commission=0.0):
        self.commission = commission


class DummyFill:
    def __init__(self, commission=None):
        self.commissionReport = None if commission is None else DummyCommissionReport(commission=commission)


class DummyIBTrade:
    def __init__(self, status, action="BUY", order_id=1, total_quantity=0, filled=0, avg_fill_price=0.0, commissions=None):
        self.order = DummyIBOrder(order_id=order_id, action=action, total_quantity=total_quantity)
        self.orderStatus = DummyIBOrderStatus(status=status, filled=filled, avg_fill_price=avg_fill_price)
        self.fills = [DummyFill(c) for c in (commissions or [])]


def test_ib_status_translation_accuracy():
    """
    Red Team Test:
    覆盖 IB 关键状态映射，确保不会把在途误判为完成，或把终态误判为 pending。
    """
    pre_submitted = IBOrderProxy(DummyIBTrade(status="PreSubmitted"), data=None)
    submitted = IBOrderProxy(DummyIBTrade(status="Submitted"), data=None)
    filled = IBOrderProxy(DummyIBTrade(status="Filled"), data=None)
    cancelled = IBOrderProxy(DummyIBTrade(status="Cancelled"), data=None)
    api_cancelled = IBOrderProxy(DummyIBTrade(status="ApiCancelled"), data=None)
    inactive = IBOrderProxy(DummyIBTrade(status="Inactive"), data=None)

    assert pre_submitted.is_pending(), "状态映射错误：PreSubmitted 必须是 pending，避免漏管在途单！"
    assert not pre_submitted.is_completed(), "状态映射错误：PreSubmitted 不能被视为 completed！"
    assert submitted.is_pending(), "状态映射错误：Submitted 必须是 pending，订单仍在排队！"
    assert not submitted.is_completed(), "状态映射错误：Submitted 不能被视为 completed！"

    assert filled.is_completed(), "状态映射错误：Filled 必须映射为 completed！"
    assert not filled.is_pending(), "状态映射错误：Filled 不应继续处于 pending！"

    assert cancelled.is_canceled(), "状态映射错误：Cancelled 必须映射为 canceled！"
    assert not cancelled.is_pending(), "状态映射错误：Cancelled 不应继续 pending，避免死锁！"
    assert api_cancelled.is_canceled(), "状态映射错误：ApiCancelled 必须映射为 canceled！"
    assert not api_cancelled.is_pending(), "状态映射错误：ApiCancelled 不应继续 pending，避免死锁！"

    # Inactive 在当前实现中归类为 rejected，不允许停留在 pending
    assert inactive.is_rejected(), "状态映射错误：Inactive 必须被安全映射为 rejected/canceled 终态！"
    assert not inactive.is_pending(), "致命错误：Inactive 不能被视为 pending，否则可能导致状态机卡死！"


def test_ib_partial_fill_handling():
    """
    Red Team Test:
    海外部成防线：状态仍 Submitted 时，不可提前向引擎报告 completed。
    """
    trade = DummyIBTrade(
        status="Submitted",
        total_quantity=1000,
        filled=200,
        avg_fill_price=151.8,
        commissions=[1.23],
    )
    proxy = IBOrderProxy(trade, data=None)
    executed = proxy.executed

    assert executed.size == 200, "部成提取错误：executed.size 必须精准等于已成交 200！"
    assert not proxy.is_completed(), "核心防御失效：Submitted(部成) 绝不能被视为 completed！"
    assert proxy.is_pending(), "状态映射错误：Submitted(部成) 应保持 pending 直至 Filled/Cancelled！"


def test_ib_executed_stats_extraction():
    """
    Red Team Test:
    成交均价/金额/手续费提取校验，确保适配层统计结果可直接用于风控与记账。
    """
    trade = DummyIBTrade(
        status="Filled",
        action="BUY",
        total_quantity=500,
        filled=500,
        avg_fill_price=150.25,
        commissions=[1.2, 0.8],
    )
    proxy = IBOrderProxy(trade, data=None)
    executed = proxy.executed

    assert executed.price == pytest.approx(150.25), "成交均价提取错误：executed.price 应为 150.25！"
    assert executed.value == pytest.approx(75125.0), "成交金额计算错误：executed.value 应为 500*150.25=75125.0！"
    assert executed.comm == pytest.approx(2.0), "手续费提取错误：executed.comm 应等于 commissionReport 合计 2.0！"
