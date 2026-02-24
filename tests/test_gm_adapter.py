import sys
from unittest.mock import MagicMock
from types import SimpleNamespace

import pytest


# 1. 拦截 gm 模块导入，注入 Mock 模块
mock_gm = MagicMock()
mock_gm_api = MagicMock()

# 定义掘金状态常量 (模拟值)
mock_gm_api.OrderStatus_New = 1
mock_gm_api.OrderStatus_PartiallyFilled = 2
mock_gm_api.OrderStatus_Filled = 3
mock_gm_api.OrderStatus_Canceled = 4
mock_gm_api.OrderStatus_Rejected = 5
mock_gm_api.OrderStatus_PendingNew = 6
mock_gm_api.OrderSide_Buy = 1
mock_gm_api.OrderSide_Sell = 2
mock_gm_api.OrderType_Market = 11
mock_gm_api.OrderType_Limit = 12

sys.modules["gm"] = mock_gm
sys.modules["gm.api"] = mock_gm_api

# 延迟导入被测模块，确保它使用上述 Mock 的 gm.api
sys.modules.pop("live_trader.adapters.gm_broker", None)
from live_trader.adapters.gm_broker import GmOrderProxy, GmBrokerAdapter


# 2. 构造掘金底层订单替身
class DummyGMOrder:
    def __init__(self, status, side=1, filled_volume=0, filled_vwap=0.0, commission=0.0):
        self.cl_ord_id = "GM_TEST_001"
        self.status = status
        self.side = side
        self.filled_volume = filled_volume
        self.filled_vwap = filled_vwap
        self.commission = commission
        # 故意不设置 filled_amount，用于测试 fallback 逻辑


def test_gm_status_translation_accuracy():
    """
    Red Team Test:
    验证 GmOrderProxy 对部成/拒单/撤单状态的翻译是否准确，避免状态机误判。
    """
    # 1) 部成 (PartiallyFilled): 应保持 pending，不可视作 completed
    partial_order = DummyGMOrder(status=mock_gm_api.OrderStatus_PartiallyFilled)
    partial_proxy = GmOrderProxy(partial_order, is_live=True)
    assert partial_proxy.is_pending(), "状态翻译错误：部成单必须保持 pending，不能提前释放监控！"
    assert not partial_proxy.is_completed(), "状态翻译错误：部成单不能被判定为 completed！"

    # 2) 拒单 (Rejected): 应标记 rejected，且不再 pending
    rejected_order = DummyGMOrder(status=mock_gm_api.OrderStatus_Rejected)
    rejected_proxy = GmOrderProxy(rejected_order, is_live=True)
    assert rejected_proxy.is_rejected(), "状态翻译错误：拒单必须触发 is_rejected()=True！"
    assert not rejected_proxy.is_pending(), "状态翻译错误：拒单不应继续处于 pending！"

    # 3) 撤单 (Canceled): 应标记 canceled，且不再 pending
    canceled_order = DummyGMOrder(status=mock_gm_api.OrderStatus_Canceled)
    canceled_proxy = GmOrderProxy(canceled_order, is_live=True)
    assert canceled_proxy.is_canceled(), "状态翻译错误：撤单必须触发 is_canceled()=True！"
    assert not canceled_proxy.is_pending(), "状态翻译错误：撤单不应继续处于 pending！"


def test_gm_executed_stats_fallback():
    """
    Red Team Test:
    验证成交金额缺失字段时的兜底逻辑: executed.value = filled_volume * filled_vwap。
    """
    filled_order = DummyGMOrder(
        status=mock_gm_api.OrderStatus_Filled,
        side=mock_gm_api.OrderSide_Buy,
        filled_volume=1000,
        filled_vwap=10.5,
        commission=12.3,
    )
    proxy = GmOrderProxy(filled_order, is_live=True)
    executed = proxy.executed

    assert executed.size == 1000, "成交统计错误：executed.size 应等于 filled_volume=1000！"
    assert executed.price == 10.5, "成交统计错误：executed.price 应等于 filled_vwap=10.5！"
    assert executed.value == pytest.approx(10500.0), "容错失败：缺失 filled_amount 时，executed.value 应回退为 1000*10.5=10500！"
    assert executed.comm == pytest.approx(12.3), "成交统计错误：executed.comm 应等于 commission 字段！"


def test_gm_live_vs_backtest_completion_logic():
    """
    Red Team Test:
    验证 PendingNew 在实盘与回测模式下的 completed 判定分歧，防止实盘误判已完成。
    """
    pending_new_order_live = DummyGMOrder(status=mock_gm_api.OrderStatus_PendingNew)
    pending_new_order_backtest = DummyGMOrder(status=mock_gm_api.OrderStatus_PendingNew)

    live_proxy = GmOrderProxy(pending_new_order_live, is_live=True)
    backtest_proxy = GmOrderProxy(pending_new_order_backtest, is_live=False)

    assert not live_proxy.is_completed(), "致命错误：实盘模式下 PendingNew 不能被视为已完成！"
    assert backtest_proxy.is_completed(), "兼容性错误：回测模式下 PendingNew 应被视为已完成！"


def test_gm_is_accepted_only_for_active_status():
    """
    适配器语义回归:
    is_accepted 仅应对在途态返回 True，终态(Filled/Canceled/Rejected)必须返回 False。
    """
    active_new = GmOrderProxy(DummyGMOrder(status=mock_gm_api.OrderStatus_New), is_live=True)
    active_partial = GmOrderProxy(DummyGMOrder(status=mock_gm_api.OrderStatus_PartiallyFilled), is_live=True)
    active_pending_new = GmOrderProxy(DummyGMOrder(status=mock_gm_api.OrderStatus_PendingNew), is_live=True)
    terminal_filled = GmOrderProxy(DummyGMOrder(status=mock_gm_api.OrderStatus_Filled), is_live=True)
    terminal_canceled = GmOrderProxy(DummyGMOrder(status=mock_gm_api.OrderStatus_Canceled), is_live=True)
    terminal_rejected = GmOrderProxy(DummyGMOrder(status=mock_gm_api.OrderStatus_Rejected), is_live=True)

    assert active_new.is_accepted(), "OrderStatus_New 应被视为 accepted。"
    assert active_partial.is_accepted(), "OrderStatus_PartiallyFilled 应被视为 accepted。"
    assert active_pending_new.is_accepted(), "OrderStatus_PendingNew 应被视为 accepted。"
    assert not terminal_filled.is_accepted(), "OrderStatus_Filled 不应被视为 accepted。"
    assert not terminal_canceled.is_accepted(), "OrderStatus_Canceled 不应被视为 accepted。"
    assert not terminal_rejected.is_accepted(), "OrderStatus_Rejected 不应被视为 accepted。"


def test_gm_submit_order_live_limit_with_auto_downsize(monkeypatch):
    """
    实盘分支测试:
    - BUY 使用限价单
    - 资金不足时自动降仓并按整手取整
    """
    import live_trader.adapters.gm_broker as gm_module

    order_calls = []

    # 覆盖导入降级路径下被置空的常量，确保测试聚焦交易逻辑本身。
    monkeypatch.setattr(gm_module, "OrderType_Market", mock_gm_api.OrderType_Market, raising=False)
    monkeypatch.setattr(gm_module, "OrderType_Limit", mock_gm_api.OrderType_Limit, raising=False)

    def _fake_order_volume(**kwargs):
        order_calls.append(kwargs)
        return [DummyGMOrder(status=mock_gm_api.OrderStatus_New, side=kwargs["side"])]

    monkeypatch.setattr(gm_module, "order_volume", _fake_order_volume)

    # 避免 __init__ 阶段访问真实 SDK 返回值
    monkeypatch.setattr(gm_module, "get_cash", lambda: SimpleNamespace(available=0.0, nav=0.0))

    broker = GmBrokerAdapter(context=MagicMock(), slippage_override=0.01, commission_override=0.0003)
    broker.is_live = True
    # 测试阶段固定可用资金，强制触发降仓
    monkeypatch.setattr(broker, "_fetch_real_cash", lambda: 20000.0)

    data = SimpleNamespace(_name="SHSE.600000")
    proxy = broker._submit_order(data=data, volume=3000, side="BUY", price=10.0)

    assert proxy is not None, "实盘下单应返回有效代理对象。"
    assert len(order_calls) == 1, "应实际调用一次 order_volume。"
    call = order_calls[0]

    expected_freeze_price = round(10.0 * (1 + 0.01), 4)  # 实盘 BUY 限价
    expected_volume = int(20000.0 / (expected_freeze_price * broker.safety_multiplier) // 100) * 100

    assert call["order_type"] == mock_gm_api.OrderType_Limit, "实盘应使用限价单。"
    assert call["price"] == pytest.approx(expected_freeze_price), "实盘 BUY 限价计算不正确。"
    assert call["volume"] == expected_volume, "资金不足时应按可用资金自动降仓并整手取整。"
    assert 0 < call["volume"] < 3000, "该场景应发生实质降仓。"


def test_gm_submit_order_backtest_market_mode(monkeypatch):
    """
    回测分支测试:
    - BUY 使用市价单
    - 市价单价格应传 0（交由引擎撮合）
    """
    import live_trader.adapters.gm_broker as gm_module

    order_calls = []

    # 覆盖导入降级路径下被置空的常量，确保测试聚焦交易逻辑本身。
    monkeypatch.setattr(gm_module, "OrderType_Market", mock_gm_api.OrderType_Market, raising=False)
    monkeypatch.setattr(gm_module, "OrderType_Limit", mock_gm_api.OrderType_Limit, raising=False)

    def _fake_order_volume(**kwargs):
        order_calls.append(kwargs)
        return [DummyGMOrder(status=mock_gm_api.OrderStatus_New, side=kwargs["side"])]

    monkeypatch.setattr(gm_module, "order_volume", _fake_order_volume)
    monkeypatch.setattr(gm_module, "get_cash", lambda: SimpleNamespace(available=0.0, nav=0.0))

    broker = GmBrokerAdapter(context=MagicMock(), slippage_override=0.01, commission_override=0.0003)
    broker.is_live = False
    monkeypatch.setattr(broker, "_fetch_real_cash", lambda: 1_000_000.0)

    data = SimpleNamespace(_name="SHSE.600000")
    proxy = broker._submit_order(data=data, volume=1000, side="BUY", price=10.0)

    assert proxy is not None, "回测下单应返回有效代理对象。"
    assert len(order_calls) == 1, "应实际调用一次 order_volume。"
    call = order_calls[0]

    assert call["order_type"] == mock_gm_api.OrderType_Market, "回测应使用市价单。"
    assert call["price"] == 0, "回测市价单应传 price=0 交由撮合引擎决定。"
    assert call["volume"] == 1000, "资金充足场景不应降仓。"
