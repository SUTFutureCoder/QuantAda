from datetime import datetime
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
            pending.append(
                {
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
        self.submitted_orders.append({"id": oid, "side": side, "volume": volume})
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

    broker.on_order_status(MockOrderProxy("ORDER_1", is_buy_order=True, status="Rejected"))

    expected_after_reject = 100 * 10.0 * broker.safety_multiplier
    assert broker._virtual_spent_cash == pytest.approx(expected_after_reject), "拒单后资金回退/重扣计算不正确"
    assert len(broker.submitted_orders) == 2, "拒单后应自动触发降级重试订单"
    assert broker.submitted_orders[1]["id"] == "ORDER_2", "降级重试订单 ID 应为 ORDER_2"
    assert broker.submitted_orders[1]["side"] == "BUY", "降级重试订单方向应为 BUY"
    assert broker.submitted_orders[1]["volume"] == 100, "降级后股数应按 lot_size 减少为 100"
    assert "ORDER_1" not in broker._active_buys, "被拒订单应从 _active_buys 移除"
    assert "ORDER_2" in broker._active_buys, "降级重试后的新订单应进入 _active_buys"


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
