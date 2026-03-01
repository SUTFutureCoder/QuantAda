from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import config
from live_trader.adapters.base_broker import BaseLiveBroker, BaseOrderProxy


class MockOrderProxy(BaseOrderProxy):
    def __init__(self, oid, is_buy_order, status="PendingSubmit"):
        self._id = oid
        self._is_buy = is_buy_order
        self._status = status
        self.executed = MagicMock(size=0, price=0, value=0, comm=0)

    @property
    def id(self):
        return self._id

    def is_completed(self):
        return self._status == "Filled"

    def is_canceled(self):
        return self._status == "Canceled"

    def is_rejected(self):
        return self._status == "Rejected"

    def is_pending(self):
        return self._status in ["PendingSubmit", "Submitted"]

    def is_accepted(self):
        return True

    def is_buy(self):
        return self._is_buy

    def is_sell(self):
        return not self._is_buy


class MockBroker(BaseLiveBroker):
    def __init__(self, initial_cash):
        self.mock_cash = initial_cash
        super().__init__(context=MagicMock())
        self.submitted_orders = []
        self.mock_position = 0
        self._datetime = None

    def _fetch_real_cash(self):
        return self.mock_cash

    def get_position(self, data):
        return MagicMock(size=self.mock_position)

    def get_current_price(self, data):
        return 10.0

    def get_pending_orders(self):
        # 将 submitted_orders 中暂未流转为最终状态的单子模拟为在途单返回。
        # 在该测试桩里，submitted_orders 默认全部视作在途单，除非测试显式模拟完成/撤销语义。
        pending = []
        for order in self.submitted_orders:
            status = order.get("status", "Submitted")
            if status not in {"PendingSubmit", "Submitted", "PendingCancel"}:
                continue
            pending.append(
                {
                    "id": order["id"],
                    "symbol": "SHSE.600000",
                    "direction": order["side"],
                    "size": order["volume"],
                }
            )
        return pending

    def getvalue(self):
        return self.mock_cash

    def _submit_order(self, data, volume, side, price):
        oid = f"ORDER_{len(self.submitted_orders) + 1}"
        proxy = MockOrderProxy(oid, is_buy_order=(side == "BUY"))
        self.submitted_orders.append({"id": oid, "side": side, "volume": volume, "status": "Submitted"})
        return proxy

    def convert_order_proxy(self, raw_order):
        return raw_order

    @staticmethod
    def is_live_mode(context):
        return True


def _make_data(symbol="SHSE.600000"):
    data = MagicMock()
    data._name = symbol
    return data


@pytest.fixture(autouse=True)
def _force_lot_size_100(monkeypatch):
    monkeypatch.setattr(config, "LOT_SIZE", 100)


def test_async_order_race_condition():
    """
    核心回归:
    卖单在途时买单先进入 deferred；卖单 Filled 回调不会自动触发 deferred 重放。
    必须显式 sync_balance + process_deferred_orders 才会真正发出买单。
    """
    broker = MockBroker(initial_cash=100.0)
    data = _make_data()

    broker._pending_sells.add("SELL_1")

    deferred_proxy = broker.order_target_value(data, target=1000)
    assert deferred_proxy is not None, "在卖单在途时，买单应返回延迟代理对象"
    assert deferred_proxy.id == "DEFERRED_VIRTUAL_ID", "延迟队列应返回虚拟订单代理"
    assert len(broker._deferred_orders) == 1, "买单未满足资金条件时应进入 _deferred_orders"
    assert len(broker.submitted_orders) == 0, "延迟买单阶段不应发送真实委托"

    broker.on_order_status(MockOrderProxy("SELL_1", is_buy_order=False, status="Filled"))
    assert "SELL_1" not in broker._pending_sells, "卖单 Filled 后应从 _pending_sells 移除"
    assert len(broker._deferred_orders) == 1, "卖单 Filled 回调不应直接触发 deferred 队列"
    assert len(broker.submitted_orders) == 0, "回调阶段不应偷偷发出买单"

    broker.mock_cash = 5000.0
    broker.sync_balance()
    broker.process_deferred_orders()

    assert len(broker._deferred_orders) == 0, "process_deferred_orders 后 deferred 队列应被清空"
    assert len(broker.submitted_orders) == 1, "显式处理 deferred 后应发出真实买单"
    assert broker.submitted_orders[0]["side"] == "BUY", "重放后的订单方向应为 BUY"
    assert broker.submitted_orders[0]["volume"] == 100, "重放后的买单数量应为 100 股"


def test_auto_downgrade_and_refund():
    """
    买单被拒绝后:
    1) 先回退上一笔虚拟资金预扣
    2) 再按 lot_size 自动降级重试并重新预扣
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()

    first_proxy = broker.order_target_value(data, target=2000)  # 200 股
    assert first_proxy is not None, "首笔买单应提交成功"
    assert first_proxy.id == "ORDER_1", "首笔订单 ID 应为 ORDER_1"
    assert broker.submitted_orders[0]["volume"] == 200, "首笔订单应为 200 股"

    expected_before_reject = 200 * 10.0 * broker.safety_multiplier
    assert broker._virtual_spent_cash == pytest.approx(expected_before_reject), "首笔订单的虚拟预扣金额异常"

    # 模拟柜台在 rejected 前已将原单从在途列表移除，允许立即重试。
    broker.submitted_orders[0]["status"] = "Inactive"
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))

    expected_after_reject = 100 * 10.0 * broker.safety_multiplier
    assert broker._virtual_spent_cash == pytest.approx(expected_after_reject), "拒单后资金回退/重扣计算不正确"
    assert len(broker.submitted_orders) == 2, "拒单后应自动触发降级重试订单"
    assert broker.submitted_orders[1]["id"] == "ORDER_2", "降级重试订单 ID 应为 ORDER_2"
    assert broker.submitted_orders[1]["side"] == "BUY", "降级重试订单方向应为 BUY"
    assert broker.submitted_orders[1]["volume"] == 100, "降级后股数应按 lot_size 减少为 100"
    assert "ORDER_1" not in broker._active_buys, "被拒订单应从 _active_buys 移除"
    assert "ORDER_2" in broker._active_buys, "降级重试后的新订单应进入 _active_buys"


def test_rejected_buy_recalculates_shares_by_available_cash(monkeypatch):
    """
    拒单后重算:
    LOT_SIZE=1 时，应按当前可用资金一次重算到可承受股数，而非仅减 1 股。
    """
    monkeypatch.setattr(config, "LOT_SIZE", 1)

    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    first_proxy = broker.order_target_value(data, target=290)  # 29 股
    assert first_proxy is not None, "首笔买单应提交成功"
    assert broker.submitted_orders[0]["volume"] == 29, "前置条件失败: 首笔应为 29 股"

    # 模拟柜台返回更紧的可用资金窗口，触发重算。
    broker.mock_cash = 271.0
    broker.submitted_orders[0]["status"] = "Inactive"
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))

    assert len(broker.submitted_orders) == 2, "拒单后应触发重试订单"
    assert broker.submitted_orders[1]["side"] == "BUY", "重算后应继续提交 BUY 订单"
    assert broker.submitted_orders[1]["volume"] == 27, "拒单后应按可用资金直接重算为 27 股"
    assert "ORDER_2" in broker._active_buys, "重算重试后的订单应进入 _active_buys"
    assert broker._virtual_spent_cash == pytest.approx(27 * 10.0 * broker.safety_multiplier), (
        "重算后的虚拟占资应与 27 股一致。"
    )


def test_rejected_buy_waits_for_cancel_before_retry(monkeypatch):
    """
    竞态修复:
    若拒单时原单仍在途，不应立即重提；应等待该单进入 Canceled 终态后再重试。
    """
    monkeypatch.setattr(config, "LOT_SIZE", 1)

    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    first_proxy = broker.order_target_value(data, target=290)  # 29 股
    assert first_proxy is not None, "首笔买单应提交成功"
    assert broker.submitted_orders[0]["volume"] == 29, "前置条件失败: 首笔应为 29 股"
    assert broker.submitted_orders[0]["status"] == "Submitted", "前置条件失败: 首笔应仍在途"

    # 1) 先收到 Rejected(Inactive)，此时仍在途，应进入缓冲，不立即重试。
    broker.mock_cash = 280.0
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))
    assert len(broker.submitted_orders) == 1, "拒单但原单在途时，不应立即提交重试单"
    assert "ORDER_1" in broker._buffered_rejected_retries, "拒单重试应被缓冲等待终态"

    # 2) 原单进入 Canceled 终态，才执行缓冲重试。
    broker.submitted_orders[0]["status"] = "Canceled"
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Canceled"))
    assert len(broker.submitted_orders) == 2, "Canceled 终态到达后应执行缓冲重试"
    assert broker.submitted_orders[1]["side"] == "BUY", "缓冲重试后的方向应为 BUY"
    assert broker.submitted_orders[1]["volume"] == 27, "重试应基于拒单后可用资金重算到 27 股"


def test_rejected_buy_buffer_released_when_order_not_pending_without_cancel(monkeypatch):
    """
    兼容 IB 异步状态:
    若拒单后已进入缓冲，但后续没有 Canceled，仅重复 Rejected/Inactive，
    且原单已不在在途列表，应自动释放缓冲重试，避免调仓卡死。
    """
    monkeypatch.setattr(config, "LOT_SIZE", 1)

    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    first_proxy = broker.order_target_value(data, target=290)  # 29 股
    assert first_proxy is not None, "首笔买单应提交成功"
    assert broker.submitted_orders[0]["status"] == "Submitted", "前置条件失败: 首笔应仍在途"

    # 第一次 Rejected 时原单仍在途 -> 进入缓冲
    broker.mock_cash = 280.0
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))
    assert len(broker.submitted_orders) == 1, "首次拒单时不应立即重提"
    assert "ORDER_1" in broker._buffered_rejected_retries, "拒单应进入缓冲等待"

    # 模拟柜台已将原单移出在途，但没有推送 Canceled，仅再次推 Rejected/Inactive。
    broker.submitted_orders[0]["status"] = "Inactive"
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))

    assert len(broker.submitted_orders) == 2, "原单已不在途时应释放缓冲并重试"
    assert broker.submitted_orders[1]["side"] == "BUY", "释放缓冲后的订单方向应为 BUY"
    assert broker.submitted_orders[1]["volume"] == 27, "释放缓冲后应按可用资金重算到 27 股"
    assert "ORDER_1" not in broker._buffered_rejected_retries, "缓冲执行后应清理旧 key"


def test_is_order_still_pending_symbol_fallback_with_mixed_id_snapshot(monkeypatch):
    """
    混合快照兜底:
    当在途快照里“部分订单有 id、部分订单无 id”时，
    目标订单若只能按 symbol 匹配，仍应视为 pending，避免误释放缓冲重试。
    """
    broker = MockBroker(initial_cash=10000.0)

    def _mixed_pending_orders():
        return [
            {"id": "OTHER_1", "symbol": "QQQ", "direction": "BUY", "size": 10},
            {"id": None, "symbol": "SMH", "direction": "BUY", "size": 34},
        ]

    monkeypatch.setattr(broker, "get_pending_orders", _mixed_pending_orders)

    assert broker._is_order_still_pending("ORDER_138", symbol="SMH.ISLAND", side="BUY") is True, (
        "混合 id 快照下应保守判定为 pending，避免提前释放缓冲重试。"
    )


def test_has_pending_order_matches_cn_symbol_alias(monkeypatch):
    """
    跨市场符号归一化:
    柜台返回裸代码(600000)时，应能匹配策略侧 SHSE.600000，避免误判“无在途”。
    """
    broker = MockBroker(initial_cash=10000.0)

    monkeypatch.setattr(
        broker,
        "get_pending_orders",
        lambda: [{"id": None, "symbol": "600000", "direction": "SELL", "size": 100}],
    )

    assert broker.has_pending_order(symbol="SHSE.600000", side="SELL") is True, (
        "应支持 SHSE.600000 与 600000 的别名匹配。"
    )


def test_stale_state_reset_cross_day():
    """
    跨日推进时，清理陈旧状态:
    - _deferred_orders
    - _pending_sells
    """
    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    broker.set_datetime(datetime(2026, 2, 16, 14, 55, 0))
    broker._deferred_orders.append({"func": broker.order_target_value, "kwargs": {"data": data, "target": 1000}})
    broker._pending_sells.add("SELL_STALE_1")

    assert len(broker._deferred_orders) == 1, "预置的脏 deferred 状态注入失败"
    assert len(broker._pending_sells) == 1, "预置的脏 pending_sells 状态注入失败"

    broker.set_datetime(datetime(2026, 2, 17, 9, 31, 0))

    assert len(broker._deferred_orders) == 0, "跨日后 _deferred_orders 必须被清空"
    assert len(broker._pending_sells) == 0, "跨日后 _pending_sells 必须被清空"


def test_risk_block_buy():
    """
    风控锁命中后，买单必须被物理拦截，不进入任何真实下单流程。
    这里使用 order_target_value 验证底层拦截分支。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data("SHSE.600000")
    broker.lock_for_risk("SHSE.600000")

    ret = broker.order_target_value(data, target=20000)

    assert ret is None, "风控锁命中时应直接返回 None"
    assert len(broker.submitted_orders) == 0, "风控拦截后不应发出真实订单"
    assert len(broker._deferred_orders) == 0, "风控拦截后不应写入延迟队列"


def test_risk_block_buy_target_percent():
    """
    风控锁命中后，order_target_percent 也必须被物理拦截。
    防止目标仓位接口绕过风控锁重新买回。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data("SHSE.600000")
    broker.set_datas([data])
    broker.lock_for_risk("SHSE.600000")

    ret = broker.order_target_percent(data, target=0.5)

    assert ret is None, "风控锁命中时 order_target_percent 应直接返回 None"
    assert len(broker.submitted_orders) == 0, "风控拦截后不应发出真实订单"
    assert len(broker._deferred_orders) == 0, "风控拦截后不应写入延迟队列"


def test_lot_size_truncation():
    """
    碎片股拦截:
    计算得到 50 股，LOT_SIZE=100 时应截断为 0 并取消委托。
    """
    broker = MockBroker(initial_cash=1000.0)
    data = _make_data()

    ret = broker.order_target_value(data, target=500)  # 500/10 = 50 股

    assert ret is None, "不足一手时应直接取消下单并返回 None"
    assert len(broker.submitted_orders) == 0, "碎片股拦截后不应有真实委托"
    assert broker._virtual_spent_cash == pytest.approx(0.0), "订单未提交时 _virtual_spent_cash 应保持 0"


def test_target_percent_rebalance():
    """
    目标仓位再平衡:
    - 组合总资产 = 10 万 (现金 5 万 + 持仓市值 5 万)
    - 当前持仓 = 5000 股, 价格 = 10
    - target=0.8 => 目标市值 8 万 => 目标股数 8000 => 应买入 3000
    """
    broker = MockBroker(initial_cash=50000.0)
    data = _make_data()
    broker.mock_position = 5000
    broker.set_datas([data])

    ret = broker.order_target_percent(data, target=0.8)

    assert ret is not None, "目标仓位再平衡应产生买单"
    assert len(broker.submitted_orders) == 1, "再平衡应只产生 1 笔订单"
    assert broker.submitted_orders[0]["side"] == "BUY", "再平衡方向应为 BUY"
    assert broker.submitted_orders[0]["volume"] == 3000, "target=0.8 时应精确买入 3000 股"


def test_smart_sell_anti_shorting():
    """
    卖空物理拦截:
    真实持仓 5000，策略试图卖 8000 时，系统必须截断为最多卖出 5000。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()
    broker.mock_position = 5000

    ret = broker._smart_sell(data, shares=8000, price=10.0)

    assert ret is not None, "合法可卖持仓存在时应返回卖单代理"
    assert len(broker.submitted_orders) == 1, "应只发出 1 笔卖单"
    assert broker.submitted_orders[0]["side"] == "SELL", "卖出路径应提交 SELL 方向"
    assert broker.submitted_orders[0]["volume"] == 5000, "卖空拦截失败: 卖出量必须被截断到真实持仓 5000"


def test_smart_sell_odd_lot_release():
    """
    清仓碎股放行:
    真实持仓 150，LOT_SIZE=100；卖出 150(或更多)时应允许直接卖 150，确保可完全清仓。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()
    broker.mock_position = 150

    ret = broker._smart_sell(data, shares=999, price=10.0)

    assert ret is not None, "清仓路径应发出卖单"
    assert len(broker.submitted_orders) == 1, "清仓场景应只发出 1 笔卖单"
    assert broker.submitted_orders[0]["side"] == "SELL", "清仓提交方向应为 SELL"
    assert broker.submitted_orders[0]["volume"] == 150, "清仓碎股应放行 150 股，不应被截断为 100"


def test_expected_size_with_pending_orders():
    """
    在途穿透防重下单:
    第一次 target=0.5 发出 5000 股 BUY 后，在未成交前再次下同目标，
    应识别在途仓位并避免重复下单。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()

    first = broker.order_target_percent(data, target=0.5)
    assert first is not None, "第一次再平衡应发出买单"
    assert len(broker.submitted_orders) == 1, "第一次调用后应有 1 笔订单"
    assert broker.submitted_orders[0]["side"] == "BUY", "第一次订单方向应为 BUY"
    assert broker.submitted_orders[0]["volume"] == 5000, "第一次 target=0.5 应买入 5000 股"
    assert broker.get_expected_size(data) == 5000, "get_expected_size 应识别到 5000 股在途买单"

    # 将 mock 现金上调等于已预扣虚拟金额，保持 NAV 在第二次调用时不失真。
    # 该步骤用于隔离测试目标：验证 expected_size 穿透计算可令 delta_shares 归零并阻止重复下单。
    broker.mock_cash = 100000.0 + broker._virtual_spent_cash

    second = broker.order_target_percent(data, target=0.5)
    assert second is None, "在途仓位已覆盖目标时应返回 None"
    assert len(broker.submitted_orders) == 1, "第二次调用不应新增任何订单"


def test_has_pending_sells_falls_back_to_broker_pending_list():
    """
    回调缺失兜底:
    即使本地 _pending_sells 为空，只要柜台仍有 SELL 在途，也必须识别为卖单未完成。
    """
    broker = MockBroker(initial_cash=100000.0)
    broker._pending_sells.clear()
    broker.submitted_orders.append({
        "id": "SELL_1",
        "side": "SELL",
        "volume": 1000,
        "status": "Submitted",
    })

    assert broker._has_pending_sells() is True, "_has_pending_sells 应回退到柜台在途列表判定。"


def test_has_pending_sells_conservative_when_snapshot_query_fails(monkeypatch):
    """
    保守兜底:
    在途快照查询异常时，_has_pending_sells 应返回 True，避免误判“无卖单在途”导致抢跑买入。
    """
    broker = MockBroker(initial_cash=100000.0)
    broker._pending_sells.clear()

    def _raise():
        raise RuntimeError("simulated pending snapshot failure")

    monkeypatch.setattr(broker, "get_pending_orders", _raise)

    assert broker._has_pending_sells() is True, "快照异常时应保守视为仍有卖单在途。"


def test_process_deferred_orders_requeues_failed_items():
    """
    自愈回放:
    deferred 重放出现异常时，失败项必须回队列，避免一次异常导致永远丢单。
    """
    broker = MockBroker(initial_cash=100000.0)
    call_log = []

    def _ok_task():
        call_log.append("ok")

    def _bad_task():
        call_log.append("bad")
        raise RuntimeError("simulated replay failure")

    broker._deferred_orders = [
        {"func": _bad_task, "kwargs": {}, "fail_count": 0},
        {"func": _ok_task, "kwargs": {}, "fail_count": 0},
    ]

    broker.process_deferred_orders()

    assert call_log == ["bad", "ok"], "deferred 任务应按队列顺序执行。"
    assert len(broker._deferred_orders) == 1, "仅失败项应被重新入队。"
    assert broker._deferred_orders[0]["func"] is _bad_task, "回队列对象应是失败任务本身。"
    assert broker._deferred_orders[0]["fail_count"] == 1, "失败任务 fail_count 应递增。"


def test_self_heal_reconciles_pending_sells_with_broker_snapshot():
    """
    状态对账:
    self_heal 应修复 _pending_sells 漂移（清理陈旧 id，并补回柜台真实在途 id）。
    """
    broker = MockBroker(initial_cash=100000.0)
    broker._pending_sells = {"SELL_STALE"}
    broker.submitted_orders = [
        {"id": "SELL_LIVE", "side": "SELL", "volume": 100, "status": "Submitted"},
    ]

    changed = broker.self_heal(reason="unit_test", force=True)

    assert changed >= 1, "存在漂移时 self_heal 应报告状态变化。"
    assert broker._pending_sells == {"SELL_LIVE"}, "_pending_sells 应与柜台在途快照对齐。"


def test_self_heal_clears_stale_pending_sells_when_broker_snapshot_empty(monkeypatch):
    """
    回调缺失 + 快照空集兜底:
    若本地 _pending_sells 残留，但柜台连续快照无 SELL 在途，
    self_heal 应自动清理陈旧标记，避免买单永久被阻塞。
    """
    monkeypatch.setattr(config, "BROKER_PENDING_SELL_CLEAR_EMPTY_SECONDS", 0.0, raising=False)

    broker = MockBroker(initial_cash=100000.0)
    broker._pending_sells = {"SELL_STALE_1"}
    broker.submitted_orders = []  # get_pending_orders() 将返回空

    # 第一次空快照: 仅计数，不立即清理（防抖）
    changed_first = broker.self_heal(reason="unit_test", force=True)
    assert changed_first == 0, "首次空快照不应立即清理，避免瞬时抖动误判。"
    assert broker._pending_sells == {"SELL_STALE_1"}, "首次空快照后本地标记应保留。"

    # 第二次连续空快照: 触发清理
    changed_second = broker.self_heal(reason="unit_test", force=True)
    assert changed_second >= 1, "连续空快照后应清理陈旧 pending-sell 标记。"
    assert len(broker._pending_sells) == 0, "陈旧 pending-sell 标记应被清空。"


def test_self_heal_clears_stale_active_buys_when_buy_snapshot_empty(monkeypatch):
    """
    幽灵占资恢复:
    若 _active_buys 残留但柜台连续快照无 BUY 在途，
    self_heal 应自动清理活跃买单跟踪并释放虚拟占资。
    """
    monkeypatch.setattr(config, "BROKER_ACTIVE_BUY_CLEAR_EMPTY_SECONDS", 0.0, raising=False)

    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()

    order = broker.order_target_value(data, target=1000.0)  # 100 股
    assert order is not None, "前置失败：应成功创建活跃买单。"
    assert "ORDER_1" in broker._active_buys, "前置失败：_active_buys 应包含 ORDER_1。"
    assert broker._virtual_spent_cash > 0, "前置失败：应存在虚拟占资。"

    # 模拟柜台已无该 BUY 在途，但回调缺失
    broker.submitted_orders[0]["status"] = "Inactive"

    changed_first = broker.self_heal(reason="unit_test", force=True)
    assert changed_first == 0, "首次空快照应仅计数，不应立即清理。"
    assert "ORDER_1" in broker._active_buys, "首次空快照后 active_buys 应暂时保留。"

    changed_second = broker.self_heal(reason="unit_test", force=True)
    assert changed_second >= 1, "连续空快照后应触发 active_buys 清理。"
    assert "ORDER_1" not in broker._active_buys, "陈旧 active_buys 标记应被清空。"
    assert broker._virtual_spent_cash == pytest.approx(0.0), "清理后虚拟占资应被释放。"


def test_self_heal_replays_deferred_without_trading_session_guard(monkeypatch):
    """
    回调缺失兜底:
    self_heal 应周期性重放 deferred，不依赖交易时段门控。
    """
    broker = MockBroker(initial_cash=100000.0)
    call_log = []

    def _task():
        call_log.append("run")

    broker._deferred_orders = [
        {"func": _task, "kwargs": {}, "fail_count": 0},
    ]

    broker.self_heal(reason="unit_test", force=True)

    assert call_log == ["run"], "self_heal 应触发 deferred 重放，避免调仓卡住。"
    assert len(broker._deferred_orders) == 0, "重放成功后 deferred 队列应清空。"


def test_buffered_retry_keeps_waiting_when_pending_snapshot_keeps_failing(monkeypatch):
    """
    查询异常兜底:
    在途快照持续不可用时，不应盲目强制释放缓冲重试，避免原单仍在途时重复买入。
    """
    broker = MockBroker(initial_cash=100000.0)
    releases = []

    broker._buffered_rejected_retries = {
        "ORDER_138": {
            "symbol": "SMH.ISLAND",
            "queued_at": datetime.now().timestamp(),
        }
    }

    def _raise():
        raise RuntimeError("pending snapshot unavailable")

    monkeypatch.setattr(broker, "get_pending_orders", _raise)

    def _fake_submit(source_oid):
        releases.append(str(source_oid))
        broker._buffered_rejected_retries.pop(str(source_oid), None)

    monkeypatch.setattr(broker, "_submit_buffered_rejected_retry", _fake_submit)

    with broker._ledger_lock:
        drained_first = broker._drain_buffered_rejected_retries(reason="unit_test")
    assert drained_first == 0, "首次快照异常应先保守等待。"
    assert releases == [], "首次快照异常不应立即强制释放。"

    with broker._ledger_lock:
        drained_second = broker._drain_buffered_rejected_retries(reason="unit_test")
    assert drained_second == 0, "连续异常时也应继续等待，不得强制释放。"
    assert releases == [], "快照不可用时不得盲目触发缓冲重试。"
    assert "ORDER_138" in broker._buffered_rejected_retries, "缓冲任务应继续保留。"


def test_buffered_retry_released_when_snapshot_fails_but_terminal_state_known(monkeypatch):
    """
    安全自愈:
    在途快照失败时，若已通过回调明确观察到该订单终态，应允许释放缓冲重试。
    """
    monkeypatch.setattr(config, "LOT_SIZE", 1)

    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    first_proxy = broker.order_target_value(data, target=290)  # 29 股
    assert first_proxy is not None, "首笔买单应提交成功"

    broker.mock_cash = 280.0
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))
    assert "ORDER_1" in broker._buffered_rejected_retries, "拒单后应进入缓冲等待"
    assert len(broker.submitted_orders) == 1, "首次拒单时不应立即重提"

    def _raise():
        raise RuntimeError("pending snapshot unavailable")

    monkeypatch.setattr(broker, "get_pending_orders", _raise)

    # 再次收到该单 Rejected（终态已明确），即使快照故障也应从状态记忆安全释放。
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))

    assert len(broker.submitted_orders) == 2, "已知终态时应释放缓冲并重试"
    assert broker.submitted_orders[1]["side"] == "BUY", "释放后的重试方向应为 BUY"
    assert broker.submitted_orders[1]["volume"] == 27, "重试应按可用资金重算到 27 股"
    assert "ORDER_1" not in broker._buffered_rejected_retries, "释放后应清理旧缓冲键"


def test_buffered_retry_requeued_when_submit_fails(monkeypatch):
    """
    自愈持久性:
    缓冲重试执行时若提交失败，任务不应丢失，必须留在缓冲队列等待下次自愈。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data("SMH.ISLAND")
    broker._buffered_rejected_retries = {
        "ORDER_138": {
            "data": data,
            "symbol": "SMH.ISLAND",
            "new_shares": 100,
            "price": 10.0,
            "lot_size": 1,
            "next_retries": 1,
            "queued_at": datetime.now().timestamp(),
        }
    }

    monkeypatch.setattr(broker, "_finalize_and_submit", lambda *args, **kwargs: None)
    with broker._ledger_lock:
        broker._submit_buffered_rejected_retry("ORDER_138")

    assert "ORDER_138" in broker._buffered_rejected_retries, "提交失败时缓冲任务不应被弹出丢失。"
    assert broker._buffered_rejected_retries["ORDER_138"].get("submit_fail_count") == 1, "失败计数应递增。"
    assert broker._virtual_spent_cash == pytest.approx(0.0), "失败后虚拟占资必须回退。"

    monkeypatch.setattr(
        broker,
        "_finalize_and_submit",
        lambda *args, **kwargs: MockOrderProxy("ORDER_2", is_buy_order=True, status="Submitted")
    )
    with broker._ledger_lock:
        broker._submit_buffered_rejected_retry("ORDER_138")

    assert "ORDER_138" not in broker._buffered_rejected_retries, "重试提交成功后应清理缓冲任务。"


def test_buffered_retry_logging_is_gbk_safe(monkeypatch):
    """
    编码兼容:
    缓冲重试日志在 GBK 终端下不应因 emoji 编码失败而抛异常。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data("SMH.ISLAND")
    broker._buffered_rejected_retries = {
        "ORDER_139": {
            "data": data,
            "symbol": "SMH.ISLAND",
            "new_shares": 100,
            "price": 10.0,
            "lot_size": 1,
            "next_retries": 1,
            "queued_at": datetime.now().timestamp(),
        }
    }
    monkeypatch.setattr(broker, "_finalize_and_submit", lambda *args, **kwargs: None)

    captured = []

    def _gbk_print(*args, **kwargs):
        text = " ".join(str(a) for a in args)
        text.encode("gbk")
        captured.append(text)

    monkeypatch.setattr("builtins.print", _gbk_print)
    with broker._ledger_lock:
        broker._submit_buffered_rejected_retry("ORDER_139")

    assert captured, "应输出日志且不触发编码异常。"


def test_on_order_status_rejected_logging_is_gbk_safe(monkeypatch):
    """
    编码兼容:
    on_order_status 的拒单/缓冲日志在 GBK 终端下不应抛编码异常。
    """
    monkeypatch.setattr(config, "LOT_SIZE", 1)

    broker = MockBroker(initial_cash=10000.0)
    data = _make_data("SHSE.600000")
    first_proxy = broker.order_target_value(data, target=290)  # 29 股
    assert first_proxy is not None, "前置失败：应成功创建首笔买单。"

    broker.mock_cash = 280.0
    captured = []

    def _gbk_print(*args, **kwargs):
        text = " ".join(str(a) for a in args)
        text.encode("gbk")
        captured.append(text)

    monkeypatch.setattr("builtins.print", _gbk_print)
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))

    assert captured, "拒单日志应可在 GBK 终端安全输出。"
    assert "ORDER_1" in broker._buffered_rejected_retries, "拒单后应进入缓冲等待。"


def test_self_heal_clears_strategy_deferred_placeholder_without_engine_run(monkeypatch):
    """
    schedule 低频兜底:
    即使不进入下一次 engine.run，broker 心跳 self_heal 也应能清理空转的 DEFERRED_VIRTUAL_ID。
    """
    broker = MockBroker(initial_cash=100000.0)
    strategy = SimpleNamespace(order=SimpleNamespace(id="DEFERRED_VIRTUAL_ID"))
    broker._context.strategy_instance = strategy

    monkeypatch.setattr(config, "BROKER_DEFERRED_CLEAR_GRACE_SECONDS", 0.0, raising=False)

    broker.self_heal(reason="unit_test", force=True)
    changed = broker.self_heal(reason="unit_test", force=True)

    assert changed >= 1, "满足清理条件时 self_heal 应报告状态变化。"
    assert strategy.order is None, "DEFERRED_VIRTUAL_ID 应在 broker 心跳中被自动清理。"


def test_intraday_long_gap_reset():
    """
    日内长中断(>600s)防御:
    虽未跨日，但 10:00 -> 10:15 的长间隔应触发 stale state reset，清空 deferred。
    """
    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    broker._deferred_orders.append({"func": broker.order_target_value, "kwargs": {"data": data, "target": 1000}})
    assert len(broker._deferred_orders) == 1, "长中断测试前置失败: deferred 注入失败"

    broker.set_datetime(datetime(2026, 2, 17, 10, 0, 0))
    broker.set_datetime(datetime(2026, 2, 17, 10, 15, 0))

    assert len(broker._deferred_orders) == 0, "日内长中断后 _deferred_orders 必须被强制清空"


def test_virtual_ledger_not_cleared_by_intraday_bar_progress():
    """
    占资口径回归:
    _virtual_spent_cash 只能在跨日时清零，日内 bar 推进(例如 10:00 -> 10:01)不应清零。
    """
    broker = MockBroker(initial_cash=10000.0)

    broker.set_datetime(datetime(2026, 2, 17, 10, 0, 0))
    broker._virtual_spent_cash = 1234.5

    # 日内正常推进
    broker.set_datetime(datetime(2026, 2, 17, 10, 1, 0))
    assert broker._virtual_spent_cash == pytest.approx(1234.5), (
        "日内 bar 推进不应清零 _virtual_spent_cash。"
    )

    # 跨日推进
    broker.set_datetime(datetime(2026, 2, 18, 9, 31, 0))
    assert broker._virtual_spent_cash == pytest.approx(0.0), (
        "跨日时必须清零 _virtual_spent_cash。"
    )


def test_cross_day_reset_without_deferred_still_cleans_pending_and_active():
    """
    跨日恢复兜底:
    即使 _deferred_orders 为空，只要存在 _pending_sells/_active_buys 脏状态，也必须触发 reset。
    """
    broker = MockBroker(initial_cash=10000.0)
    data = _make_data()

    broker.set_datetime(datetime(2026, 2, 16, 14, 55, 0))
    broker._pending_sells.add("SELL_STALE_1")
    broker._active_buys["BUY_STALE_1"] = {
        "data": data,
        "shares": 100,
        "price": 10.0,
        "lot_size": 100,
        "retries": 0,
    }
    broker._virtual_spent_cash = 1000.0

    assert len(broker._deferred_orders) == 0, "前置失败：该用例要求 deferred 为空。"
    assert len(broker._pending_sells) == 1, "前置失败：pending_sells 注入失败。"
    assert len(broker._active_buys) == 1, "前置失败：active_buys 注入失败。"

    broker.set_datetime(datetime(2026, 2, 17, 9, 31, 0))

    assert len(broker._pending_sells) == 0, "跨日后 _pending_sells 必须被清空。"
    assert len(broker._active_buys) == 0, "跨日后 _active_buys 必须被清空。"


def test_cash_override_and_virtual_ledger_exhaustion():
    """
    资金覆写 + 虚拟账本耗尽:
    - 总现金 10 万，但策略可用额度 override=2 万
    - 第 1 单买 1500 股(~15000)
    - 第 2 单再买 1000 股(~10000)时应因剩余额度不足触发自动降级并 lot 向下取整
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()
    broker._cash_override = 20000.0

    assert broker.get_cash() == pytest.approx(20000.0), "cash_override 生效失败: 初始可用资金应被限制为 20000"

    first = broker.order_target_value(data, target=15000.0)  # 1500 股
    assert first is not None, "第一笔买单应成功发出"
    assert len(broker.submitted_orders) == 1, "第一笔买单后应有 1 笔订单"
    assert broker.submitted_orders[0]["side"] == "BUY", "第一笔订单方向应为 BUY"
    assert broker.submitted_orders[0]["volume"] == 1500, "第一笔订单数量应为 1500 股"

    # 模拟策略资金池只保留 override 额度口径，确保虚拟账本可直接消耗该额度。
    broker.mock_cash = 20000.0
    cash_after_first = broker.get_cash()
    assert cash_after_first < 5000.0 + 1.0, "第一笔后剩余可用额度应约为 5000（含安全垫误差）"

    # 这里直接调用 _smart_buy_value，隔离验证“资金不足 -> 自动降级”逻辑，
    # 避免被 expected_size 的在途仓位穿透规则改写为卖出分支。
    second = broker._smart_buy_value(data, shares=1000.0, price=10.0, target_value=10000.0)
    assert second is not None, "第二笔应触发降级后继续发单，而不是直接丢弃"
    assert len(broker.submitted_orders) == 2, "第二笔降级订单应成功提交"
    assert broker.submitted_orders[1]["side"] == "BUY", "第二笔订单方向应为 BUY"
    assert broker.submitted_orders[1]["volume"] == 400, "第二笔应按剩余额度降级并 lot 取整到 400 股"


def test_buy_order_canceled_virtual_cash_leak():
    """
    Red Team Test:
    高危漏洞检测 - 买单被人工撤销(Canceled)后，验证虚拟账本是否正确释放资金。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()

    # 先发起一笔 1000 股买单，预期在虚拟账本里预扣 1000 * 10 * safety_multiplier
    first = broker.order_target_value(data, target=10000.0)
    assert first is not None, "前置失败：1000 股买单应成功发出"
    assert first.id == "ORDER_1", "前置失败：首笔订单 ID 应为 ORDER_1"
    assert "ORDER_1" in broker._active_buys, "前置失败：活跃买单跟踪器中应包含 ORDER_1"

    pre_deduct = 1000 * 10.0 * broker.safety_multiplier
    assert broker._virtual_spent_cash == pytest.approx(pre_deduct), "前置失败：首笔买单的虚拟预扣金额不正确"

    # 模拟用户在柜台端手动撤单 -> 回调 Canceled
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Canceled"))

    # 断言1：活跃订单应移除
    assert "ORDER_1" not in broker._active_buys, "买单撤销后，_active_buys 未清理，存在状态机脏数据风险！"

    # 断言2（核心）：虚拟资金必须回退，否则会出现“幽灵占资”
    assert broker._virtual_spent_cash == pytest.approx(0.0), (
        "买单撤销后，虚拟资金未回退，发生幽灵账本泄漏！"
    )


def test_buy_order_filled_releases_virtual_cash():
    """
    占资终态回归:
    买单 Filled 后，_virtual_spent_cash 必须回退到 0，避免与柜台已扣现金发生双重扣减。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()

    first = broker.order_target_value(data, target=10000.0)  # 1000 股
    assert first is not None, "前置失败：首笔买单应成功发出"
    assert first.id == "ORDER_1", "前置失败：首笔订单 ID 应为 ORDER_1"
    assert "ORDER_1" in broker._active_buys, "前置失败：活跃买单跟踪器中应包含 ORDER_1"

    pre_deduct = 1000 * 10.0 * broker.safety_multiplier
    assert broker._virtual_spent_cash == pytest.approx(pre_deduct), "前置失败：首笔买单虚拟预扣金额异常"

    # 模拟柜台成交后物理现金已扣减
    broker.mock_cash = 90000.0
    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Filled"))

    assert "ORDER_1" not in broker._active_buys, "买单成交后，_active_buys 未清理，存在状态机脏数据风险！"
    assert broker._virtual_spent_cash == pytest.approx(0.0), (
        "买单成交后，虚拟资金未回退，发生可用资金双重扣减风险！"
    )
    assert broker.get_cash() == pytest.approx(90000.0), (
        "买单成交后 get_cash 应与柜台实扣现金对齐，不能继续被虚拟账本二次扣减。"
    )


def test_manual_force_reset_recovery():
    """
    Red Team Test:
    极端灾难恢复 - 在内部状态机乱套后，force_reset_state 应兜底清理并恢复可用资金。
    """
    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()

    # 构造“乱套状态”：虚拟占资异常 + 延迟队列脏单 + 卖单监控残留
    broker._virtual_spent_cash = 43210.0
    broker._deferred_orders.append({"func": broker.order_target_value, "kwargs": {"data": data, "target": 2000}})
    broker._pending_sells.add("SELL_STUCK_1")

    # 前置校验，确保脏状态确实存在
    assert broker._virtual_spent_cash > 0, "前置失败：虚拟占资注入失败"
    assert len(broker._deferred_orders) == 1, "前置失败：_deferred_orders 注入失败"
    assert len(broker._pending_sells) == 1, "前置失败：_pending_sells 注入失败"

    # 执行灾备重置，并立即同步余额
    broker.force_reset_state()
    broker.sync_balance()

    # 队列清空断言
    assert len(broker._deferred_orders) == 0, "强制重置失败：_deferred_orders 未被清空，可能导致重复下单！"
    assert len(broker._pending_sells) == 0, "强制重置失败：_pending_sells 未被清空，可能导致买单永久阻塞！"

    # 现金恢复断言（核心）：可用现金必须回到真实资金水平
    real_cash = broker._fetch_real_cash()
    assert broker.get_cash() == pytest.approx(real_cash), (
        "强制重置后可用现金未恢复到真实余额，虚拟账本仍在错误占资！"
    )


def test_on_sell_filled_waits_for_sell_clear_before_replay():
    """
    卖单出清闸门:
    on_sell_filled 不应因“部分成交事件”立刻重放 deferred。
    只有确认无 SELL 在途后，才允许重放 BUY 延迟单。
    """
    broker = MockBroker(initial_cash=100000.0)
    replay_calls = []

    def _task():
        replay_calls.append("run")

    broker._deferred_orders = [
        {"func": _task, "kwargs": {}, "created_at": datetime.now().timestamp(), "fail_count": 0}
    ]
    broker._pending_sells.add("SELL_1")
    broker.submitted_orders = [
        {"id": "SELL_1", "side": "SELL", "volume": 100, "status": "Submitted"}
    ]

    # 第一次: 仍有 SELL 在途，不应重放 deferred BUY。
    broker.on_sell_filled()
    assert replay_calls == [], "SELL 未出清时不应重放 deferred BUY。"
    assert len(broker._deferred_orders) == 1, "SELL 未出清时 deferred 队列应保留。"

    # 第二次: SELL 出清后才允许重放。
    broker.submitted_orders[0]["status"] = "Filled"
    broker._pending_sells.clear()
    broker.on_sell_filled()

    assert replay_calls == ["run"], "SELL 出清后应允许重放 deferred BUY。"
    assert len(broker._deferred_orders) == 0, "重放成功后 deferred 队列应清空。"


def test_self_heal_avoids_refetch_under_lock_when_snapshot_unavailable(monkeypatch):
    """
    自愈健壮性:
    self_heal 快照失败时，应避免在持锁路径重复请求快照，防止放大阻塞。
    """
    monkeypatch.setattr(config, "BROKER_PENDING_SNAPSHOT_RETRY_ATTEMPTS", 1, raising=False)

    broker = MockBroker(initial_cash=100000.0)
    data = _make_data()
    broker._pending_sells = {"SELL_STALE"}
    broker._active_buys = {
        "BUY_STALE": {
            "data": data,
            "shares": 100,
            "price": 10.0,
            "lot_size": 100,
            "retries": 0,
            "created_at": datetime.now().timestamp(),
        }
    }

    stat = {"total_calls": 0, "lock_owned_calls": 0}

    def _raise_pending_snapshot():
        stat["total_calls"] += 1
        if hasattr(broker._ledger_lock, "_is_owned") and broker._ledger_lock._is_owned():
            stat["lock_owned_calls"] += 1
        raise RuntimeError("pending snapshot unavailable")

    monkeypatch.setattr(broker, "get_pending_orders", _raise_pending_snapshot)

    broker.self_heal(reason="unit_test", force=True)

    assert stat["total_calls"] == 1, "self_heal 失败路径应仅进行一次快照尝试。"
    assert stat["lock_owned_calls"] == 0, "持锁路径不应再次请求快照。"


def test_self_heal_skips_pending_snapshot_poll_when_no_runtime_backlog(monkeypatch):
    """
    自愈降噪:
    无任何运行时积压时，self_heal 不应主动轮询在途快照。
    """
    broker = MockBroker(initial_cash=100000.0)
    broker.SELF_HEAL_MIN_INTERVAL_SECONDS = 0.0

    stat = {"calls": 0}

    def _pending():
        stat["calls"] += 1
        return []

    monkeypatch.setattr(broker, "get_pending_orders", _pending)

    broker.self_heal(reason="unit_test")
    broker.self_heal(reason="unit_test")

    assert stat["calls"] == 0, "空闲状态不应触发在途快照轮询。"


def test_self_heal_throttles_pending_snapshot_poll_under_backlog(monkeypatch):
    """
    自愈限流:
    有运行时积压时，快照轮询也应受最小间隔限制，避免心跳放大 API 压力。
    """
    monkeypatch.setattr(config, "BROKER_PENDING_SNAPSHOT_MIN_INTERVAL_SECONDS", 60.0, raising=False)

    broker = MockBroker(initial_cash=100000.0)
    broker.SELF_HEAL_MIN_INTERVAL_SECONDS = 0.0
    broker._pending_sells = {"SELL_STALE"}

    stat = {"calls": 0}

    def _pending():
        stat["calls"] += 1
        return []

    monkeypatch.setattr(broker, "get_pending_orders", _pending)

    broker.self_heal(reason="unit_test")
    broker.self_heal(reason="unit_test")

    assert stat["calls"] == 1, "同一节流窗口内不应重复拉取在途快照。"


def test_reconcile_active_buys_mixed_id_snapshot_clears_stale_tracker(monkeypatch):
    """
    混合快照对账:
    当 BUY 快照存在有效 id 时，应优先按 id 对账，不应被 symbol 兜底掩盖陈旧 tracker。
    """
    monkeypatch.setattr(config, "BROKER_ACTIVE_BUY_CLEAR_EMPTY_SECONDS", 0.0, raising=False)
    monkeypatch.setattr(config, "BROKER_ACTIVE_BUY_CLEAR_EMPTY_SNAPSHOTS", 1, raising=False)

    broker = MockBroker(initial_cash=100000.0)
    data = _make_data("SMH.ISLAND")
    created_at = datetime.now().timestamp() - 60.0
    broker._active_buys = {
        "BUY_STALE": {
            "data": data,
            "shares": 100,
            "price": 10.0,
            "lot_size": 1,
            "retries": 0,
            "created_at": created_at,
        },
        "BUY_LIVE": {
            "data": data,
            "shares": 200,
            "price": 10.0,
            "lot_size": 1,
            "retries": 0,
            "created_at": created_at,
        },
    }
    broker._virtual_spent_cash = 300 * 10.0 * broker.safety_multiplier

    pending_orders = [
        {"id": "BUY_LIVE", "symbol": "SMH", "direction": "BUY", "size": 200},
    ]

    with broker._ledger_lock:
        changed = broker._reconcile_active_buys_from_broker(pending_orders=pending_orders)

    assert changed >= 1, "存在陈旧 active-buy tracker 时应触发清理。"
    assert "BUY_STALE" not in broker._active_buys, "陈旧 tracker 应被清理。"
    assert "BUY_LIVE" in broker._active_buys, "仍在途的 tracker 应保留。"
    assert broker._virtual_spent_cash == pytest.approx(200 * 10.0 * broker.safety_multiplier), (
        "清理陈旧 tracker 后应仅保留在途 BUY 对应的虚拟占资。"
    )


def test_reconcile_active_buys_mixed_id_snapshot_keeps_tracker_when_no_id_symbol_matches(monkeypatch):
    """
    混合快照保守兜底:
    当快照同时包含“有 id BUY”和“无 id BUY”时，
    若本地 tracker 能按 symbol 命中无 id BUY，不应被提前清理释放占资。
    """
    monkeypatch.setattr(config, "BROKER_ACTIVE_BUY_CLEAR_EMPTY_SECONDS", 0.0, raising=False)
    monkeypatch.setattr(config, "BROKER_ACTIVE_BUY_CLEAR_EMPTY_SNAPSHOTS", 1, raising=False)

    broker = MockBroker(initial_cash=100000.0)
    data = _make_data("SMH.ISLAND")
    created_at = datetime.now().timestamp() - 60.0
    broker._active_buys = {
        "BUY_TARGET": {
            "data": data,
            "shares": 100,
            "price": 10.0,
            "lot_size": 1,
            "retries": 0,
            "created_at": created_at,
        },
    }
    broker._virtual_spent_cash = 100 * 10.0 * broker.safety_multiplier

    pending_orders = [
        {"id": "BUY_OTHER", "symbol": "QQQ", "direction": "BUY", "size": 10},
        {"id": None, "symbol": "SMH", "direction": "BUY", "size": 100},
    ]

    with broker._ledger_lock:
        changed = broker._reconcile_active_buys_from_broker(pending_orders=pending_orders)

    assert changed == 0, "混合快照命中 symbol 兜底时不应清理 tracker。"
    assert "BUY_TARGET" in broker._active_buys, "匹配到无 id BUY 时应保留本地 tracker。"
    assert broker._virtual_spent_cash == pytest.approx(100 * 10.0 * broker.safety_multiplier), (
        "symbol 命中保留 tracker 时不应释放虚拟占资。"
    )
