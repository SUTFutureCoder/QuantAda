import sys
import types
import datetime

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

    def qualifyContracts(self, contract):
        return [contract]

    def reqMktData(self, contract, genericTickList="", snapshot=False, regulatorySnapshot=False):
        return DummyTicker(float("nan"))

    def reqHistoricalData(self, *args, **kwargs):
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
        self.bid = None
        self.ask = None

    def marketPrice(self):
        return self._price


class DummyIBForMissingFx(DummyIBForCash):
    def __init__(self):
        super().__init__(cash_usd=0.0)

    def accountSummary(self):
        return [
            DummyAccountValue(tag="TotalCashValue", currency="USD", value="1000"),
            DummyAccountValue(tag="TotalCashValue", currency="HKD", value="7800"),
            DummyAccountValue(tag="TotalCashValue", currency="BASE", value="2000"),
        ]


class DummyIBForSubmit(DummyIBForCash):
    def __init__(self):
        super().__init__(cash_usd=10000.0)
        self.last_contract = None
        self.last_order = None
        self._oid = 900

    def placeOrder(self, contract, order):
        self._oid += 1
        self.last_contract = contract
        self.last_order = order
        return DummyIBTrade(
            status="Submitted",
            action=getattr(order, "action", "BUY"),
            order_id=self._oid,
            total_quantity=getattr(order, "totalQuantity", 0),
        )


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


def test_ib_fetch_smart_value_falls_back_to_base_when_fx_missing():
    """
    汇率兜底回归:
    若非 USD 货币无法换汇，但券商提供 BASE 汇总，必须回退 BASE，避免低估资金。
    """
    context = types.SimpleNamespace(ib_instance=DummyIBForMissingFx())
    broker = IBBrokerAdapter(context=context)

    val = broker._fetch_smart_value(["TotalCashValue"])

    assert val == pytest.approx(2000.0), "汇率缺口存在时应回退 BASE 汇总口径。"


def test_ib_get_rebalance_cash_uses_conservative_cash_floor():
    """
    调仓资金口径回归:
    get_rebalance_cash 应使用 min(get_cash, TotalCashValue)，避免计划层误用杠杆。
    """
    context = types.SimpleNamespace(ib_instance=DummyIBForCash(cash_usd=10000.0))
    broker = IBBrokerAdapter(context=context)

    # spendable 口径 = 9000, TotalCashValue 口径 = 7000 => 调仓应取 7000
    broker.get_cash = lambda: 9000.0
    broker._fetch_smart_value = lambda tags=None: 7000.0 if tags == ["TotalCashValue"] else 9000.0

    assert broker.get_rebalance_cash() == pytest.approx(7000.0), (
        "调仓现金口径应取更保守值。"
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


def test_ib_daily_schedule_parse_and_validation():
    """
    调度解析回归:
    - 支持 1d:HH:MM(:SS)
    - 非法时间抛出异常
    """
    parsed = IBBrokerAdapter._parse_daily_schedule("1d:15:45:00")
    assert parsed == (15, 45, 0, "15:45:00"), "应正确解析带秒的 daily schedule。"

    parsed_no_sec = IBBrokerAdapter._parse_daily_schedule("1d:09:30")
    assert parsed_no_sec == (9, 30, 0, "09:30"), "应兼容不带秒的 daily schedule。"

    with pytest.raises(ValueError):
        IBBrokerAdapter._parse_daily_schedule("1d:25:61:00")


def test_ib_daily_schedule_should_only_trigger_in_tolerance_window():
    """
    调度触发回归:
    - 到点后短窗口内可触发
    - 错过窗口后同日不补跑
    - 同日已跑过不再重复触发
    """
    now = datetime.datetime(2026, 2, 27, 15, 45, 2)
    should_run, delta, day_str = IBBrokerAdapter._should_trigger_daily_schedule(
        now=now,
        target_h=15,
        target_m=45,
        target_s=0,
        last_schedule_run_date=None,
    )
    assert should_run, "目标时间后的容忍窗口内应触发。"
    assert delta == pytest.approx(2.0), "delta 应为当前与目标时刻的秒差。"
    assert day_str == "2026-02-27"

    late_now = datetime.datetime(2026, 2, 27, 15, 50, 0)
    should_run_late, _, _ = IBBrokerAdapter._should_trigger_daily_schedule(
        now=late_now,
        target_h=15,
        target_m=45,
        target_s=0,
        last_schedule_run_date=None,
    )
    assert not should_run_late, "错过容忍窗口后不应补跑。"

    should_run_again, _, _ = IBBrokerAdapter._should_trigger_daily_schedule(
        now=now,
        target_h=15,
        target_m=45,
        target_s=0,
        last_schedule_run_date="2026-02-27",
    )
    assert not should_run_again, "同一自然日只允许运行一次。"


def test_ib_submit_order_allows_fractional_sell_for_full_close(monkeypatch):
    """
    清仓碎股回归:
    SELL 委托在存在小数股时应允许透传小数，避免尾仓永远无法清掉。
    """
    context = types.SimpleNamespace(ib_instance=DummyIBForSubmit())
    broker = IBBrokerAdapter(context=context)

    monkeypatch.setattr(
        mock_ib_insync,
        "MarketOrder",
        lambda action, qty: SimpleNamespace(action=action, totalQuantity=qty),
    )
    # 同步替换模块内引用，确保 _submit_order 使用到 patched 构造器
    import live_trader.adapters.ib_broker as ib_module
    monkeypatch.setattr(
        ib_module,
        "MarketOrder",
        lambda action, qty: SimpleNamespace(action=action, totalQuantity=qty),
    )
    monkeypatch.setattr(
        ib_module.config,
        "IBKR_ALLOW_FRACTIONAL_SELL",
        True,
        raising=False,
    )

    data = SimpleNamespace(_name="AAPL.SMART")
    proxy = broker._submit_order(data=data, volume=0.6441, side="SELL", price=100.0)

    assert proxy is not None, "分数股卖出应提交订单，不应被 <1 直接跳过。"
    assert context.ib_instance.last_order.totalQuantity == pytest.approx(0.6441, rel=1e-6), (
        "SELL 小数股应按原始数量提交，避免残留尾仓。"
    )


def test_ib_submit_order_fractional_sell_disabled_by_default(monkeypatch):
    """
    安全默认值回归:
    未显式开启时，SELL 小数股应向下取整为整数，避免触发 IB API 10243。
    """
    context = types.SimpleNamespace(ib_instance=DummyIBForSubmit())
    broker = IBBrokerAdapter(context=context)

    import live_trader.adapters.ib_broker as ib_module
    monkeypatch.setattr(
        ib_module,
        "MarketOrder",
        lambda action, qty: SimpleNamespace(action=action, totalQuantity=qty),
    )
    monkeypatch.setattr(
        ib_module.config,
        "IBKR_ALLOW_FRACTIONAL_SELL",
        False,
        raising=False,
    )

    data = SimpleNamespace(_name="AAPL.SMART")
    proxy = broker._submit_order(data=data, volume=104.617, side="SELL", price=100.0)

    assert proxy is not None, "禁用小数股时也应提交整数卖单。"
    assert context.ib_instance.last_order.totalQuantity == 104, (
        "禁用小数股时，SELL 数量应向下取整为整数。"
    )
