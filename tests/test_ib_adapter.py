import sys
import types

import pytest
from unittest.mock import MagicMock
from types import SimpleNamespace


# 拦截 ib_insync 导入，避免任何真实 TWS/Gateway 连接依赖
mock_ib_insync = types.ModuleType("ib_insync")
mock_ib_insync.IB = MagicMock(name="IB")
mock_ib_insync.Stock = MagicMock(name="Stock")
mock_ib_insync.MarketOrder = MagicMock(name="MarketOrder")
mock_ib_insync.Trade = MagicMock(name="Trade")
mock_ib_insync.Forex = MagicMock(name="Forex")
mock_ib_insync.Contract = MagicMock(name="Contract")
mock_ib_insync.Crypto = MagicMock(name="Crypto")
sys.modules["ib_insync"] = mock_ib_insync

# 重新导入被测模块，确保使用上面的 mock ib_insync
sys.modules.pop("live_trader.adapters.ib_broker", None)
from live_trader.adapters.ib_broker import IBBrokerAdapter, IBOrderProxy
from data_providers.ibkr_provider import IbkrDataProvider


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


class DummyAccountValue:
    def __init__(self, tag, currency, value):
        self.tag = tag
        self.currency = currency
        self.value = value


class DummyIBForCash:
    def __init__(self, cash_usd):
        self.cash_usd = cash_usd

    def isConnected(self):
        return True

    def accountSummary(self):
        return [DummyAccountValue(tag="TotalCashValue", currency="USD", value=str(self.cash_usd))]

    def accountValues(self):
        return self.accountSummary()

    def openTrades(self):
        return []


class DummyIBForCashWithOpenTrades(DummyIBForCash):
    def __init__(self, cash_usd, open_trades):
        super().__init__(cash_usd=cash_usd)
        self._open_trades = open_trades

    def openTrades(self):
        return self._open_trades


class DummyIBForPositions(DummyIBForCash):
    def __init__(self, cash_usd, positions):
        super().__init__(cash_usd=cash_usd)
        self._positions = positions

    def positions(self):
        return self._positions


class DummyTicker:
    def __init__(self, price):
        self._price = price
        self.close = None
        self.last = None

    def marketPrice(self):
        return self._price


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


def test_ib_get_cash_respects_virtual_ledger():
    """
    资金口径回归:
    get_cash 必须扣减 _virtual_spent_cash，防止 openTrades 回报延迟窗口内重复下单。
    """
    context = types.SimpleNamespace(ib_instance=DummyIBForCash(cash_usd=10000.0))
    broker = IBBrokerAdapter(context=context)
    broker._virtual_spent_cash = 2500.0

    assert broker.get_cash() == pytest.approx(7500.0), (
        "IB get_cash 未扣减虚拟账本，占资保护失效。"
    )


def test_ib_get_cash_dedupes_open_orders_and_local_ledger():
    """
    占资去重回归:
    openTrades 已覆盖的本地订单不应重复扣减，但本地额外订单仍应继续扣减。
    """
    open_trade = SimpleNamespace(
        order=SimpleNamespace(orderId=1, action="BUY"),
        contract=SimpleNamespace(symbol="AAPL"),
        orderStatus=SimpleNamespace(remaining=10),
    )
    context = types.SimpleNamespace(ib_instance=DummyIBForCashWithOpenTrades(cash_usd=10000.0, open_trades=[open_trade]))
    broker = IBBrokerAdapter(context=context)

    broker._tickers = {"AAPL": DummyTicker(100.0)}
    broker._active_buys = {
        "1": {"data": None, "shares": 10, "price": 100.0, "lot_size": 1, "retries": 0},
        "2": {"data": None, "shares": 5, "price": 200.0, "lot_size": 1, "retries": 0},
    }
    broker._virtual_spent_cash = (
        10 * 100.0 * broker.safety_multiplier
        + 5 * 200.0 * broker.safety_multiplier
    )

    expected_reserved = (
        10 * 100.0 * 1.015  # openTrades 估算冻结 (id=1)
        + 5 * 200.0 * broker.safety_multiplier  # 本地额外订单 (id=2)
    )
    expected_cash = 10000.0 - expected_reserved

    assert broker.get_cash() == pytest.approx(expected_cash), (
        "IB get_cash 占资去重异常：应扣减 openTrades 冻结 + 本地额外订单占资。"
    )


def test_ib_parse_contract_accepts_smart_suffix():
    """
    EWJ.SMART 这类符号必须拆分为 ticker + SMART 路由，不能把 '.SMART' 当作 ticker 一部分。
    """
    mock_ib_insync.Stock.reset_mock()

    IBBrokerAdapter.parse_contract("EWJ.SMART")

    mock_ib_insync.Stock.assert_called_with("EWJ", "SMART", "USD")


def test_ib_parse_contract_supports_generic_exchange_suffix():
    """
    任意常见交易所后缀应可直接识别，无需每次扩白名单。
    """
    mock_ib_insync.Stock.reset_mock()

    IBBrokerAdapter.parse_contract("AAPL.IEX")

    mock_ib_insync.Stock.assert_called_with("AAPL", "SMART", "USD", primaryExchange="IEX")


def test_ib_parse_contract_supports_exchange_prefix_variant():
    """
    兼容 Exchange.Ticker 写法（如 NASDAQ.AAPL）。
    """
    mock_ib_insync.Stock.reset_mock()

    IBBrokerAdapter.parse_contract("NASDAQ.AAPL")

    mock_ib_insync.Stock.assert_called_with("AAPL", "SMART", "USD", primaryExchange="NASDAQ")


def test_ib_parse_contract_keeps_share_class_symbols():
    """
    BRK.B 这类一位后缀是股票代码本体，不应误判为交易所。
    """
    mock_ib_insync.Stock.reset_mock()

    IBBrokerAdapter.parse_contract("BRK.B")

    mock_ib_insync.Stock.assert_called_with("BRK.B", "SMART", "USD")


def test_ib_get_position_matches_symbol_suffix_variants():
    """
    持仓匹配回归:
    data._name='EWJ.SMART' 时，应能命中 IB 返回的 symbol='EWJ' 持仓。
    """
    ib_positions = [
        SimpleNamespace(
            contract=SimpleNamespace(symbol="EWJ", secType="STK", localSymbol="EWJ"),
            position=140,
            avgCost=92.01,
        )
    ]
    context = types.SimpleNamespace(ib_instance=DummyIBForPositions(cash_usd=10000.0, positions=ib_positions))
    broker = IBBrokerAdapter(context=context)

    data = SimpleNamespace(_name="EWJ.SMART")
    pos = broker.get_position(data)

    assert pos.size == pytest.approx(140), "后缀容错失效：EWJ.SMART 必须匹配到 EWJ 持仓。"
    assert pos.price == pytest.approx(92.01), "持仓成本提取异常：应返回 IB 的 avgCost。"


def test_ib_provider_parse_contract_supports_generic_exchange_suffix():
    """
    数据源与实盘 Broker 必须共享同一解析语义，避免一边能下单一边拉不到行情。
    """
    provider = IbkrDataProvider(ib_instance=MagicMock())
    mock_ib_insync.Stock.reset_mock()

    provider._parse_contract("MSFT.MEMX")

    mock_ib_insync.Stock.assert_called_with("MSFT", "SMART", "USD", primaryExchange="MEMX")
