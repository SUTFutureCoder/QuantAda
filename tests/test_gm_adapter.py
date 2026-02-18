import sys
from unittest.mock import MagicMock

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

sys.modules["gm"] = mock_gm
sys.modules["gm.api"] = mock_gm_api

# 延迟导入被测模块，确保它使用上述 Mock 的 gm.api
sys.modules.pop("live_trader.adapters.gm_broker", None)
from live_trader.adapters.gm_broker import GmOrderProxy


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
