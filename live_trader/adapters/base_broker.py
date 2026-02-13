import threading
from abc import ABC, abstractmethod

import pandas as pd

from common import log


class BaseOrderProxy(ABC):
    """
    订单代理的抽象基类。
    所有平台的具体订单代理都必须实现这些与 backtrader 兼容的方法。
    """

    @property
    @abstractmethod
    def id(self): pass
    @abstractmethod
    def is_completed(self) -> bool: pass

    @abstractmethod
    def is_canceled(self) -> bool: pass

    @abstractmethod
    def is_rejected(self) -> bool: pass

    @abstractmethod
    def is_pending(self) -> bool: pass

    @abstractmethod
    def is_accepted(self) -> bool: pass

    @abstractmethod
    def is_buy(self) -> bool: pass

    @abstractmethod
    def is_sell(self) -> bool: pass


# 内置虚拟订单，用于延迟队列占位，对子类透明
class _DeferredOrderProxy(BaseOrderProxy):
    def __init__(self, data): self._data = data
    @property
    def id(self): return "DEFERRED_VIRTUAL_ID"
    def is_completed(self): return False
    def is_pending(self): return True
    def is_sell(self): return False
    def is_canceled(self): return False
    def is_rejected(self): return False
    def is_buy(self): return True
    def is_accepted(self): return True
    @property
    def executed(self):
        class Dummy: size=0; price=0; value=0; comm=0
        return Dummy()

class BaseLiveDataProvider(ABC):
    """数据提供者适配器的抽象基类"""

    @abstractmethod
    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:
        """获取指定标的的历史日线数据"""
        pass


class BaseLiveBroker(ABC):
    """交易执行器适配器的抽象基类，模拟 backtrader 的 broker 接口"""

    def __init__(self, context, cash_override=None, commission_override=None, slippage_override=None,):
        self.is_live = True
        self._context = context
        self.datas = []
        self._datetime = None
        self._cash_override = cash_override
        self._commission_override = commission_override
        self._slippage_override = slippage_override
        # 内部状态机
        self._cash = self._init_cash()
        self._deferred_orders = []
        self._pending_sells = set()
        # 虚拟账本，类似backtester能快速回笼资金
        self._virtual_spent_cash = 0.0
        # 活跃买单追踪器，用于被拒单时的降级重试
        self._active_buys = {}
        # 虚拟账本读写锁
        self._ledger_lock = threading.RLock()

    @property
    def safety_multiplier(self):
        """
        动态计算买入资金安全垫：
        1.0 + 委托滑点 + 手续费率 + 绝对防线(0.2%，抵御A股不足5元收5元等边缘情况)
        """
        comm = self._commission_override if self._commission_override is not None else 0.0003
        slip = self._slippage_override if self._slippage_override is not None else 0.001
        return 1.0 + slip + comm + 0.002

    def log(self, txt, dt=None):
        """
        兼容 Backtrader 的日志接口。
        供策略层调用 (self.broker.log)。
        在实盘模式下，如果没有传入时间，log.info 会自动使用当前系统时间。
        """
        log.info(txt, dt=dt)

    # =========================================================
    #  用户只需实现下述原子接口 (The Minimum Set)
    # =========================================================
    @abstractmethod
    def getvalue(self):
        """
        兼容 Backtrader 接口: 获取当前账户总权益 (Net Liquidation Value)
        默认实现: 现金 + 所有持仓的市值
        """
        return self._get_portfolio_nav()

    @abstractmethod
    def _fetch_real_cash(self) -> float:
        """子类必须实现，用于获取真实账户的可用资金"""
        pass

    @abstractmethod
    def get_position(self, data):
        """子类必须实现，用于获取指定标的的持仓"""
        pass

    @abstractmethod
    def get_current_price(self, data) -> float:
        """子类必须实现，用于获取指定标的实时价格"""
        pass

    @abstractmethod
    def _submit_order(self, data, volume, side, price):
        """子类必须实现，用于提交指定标的买入或卖出操作"""
        pass

    @abstractmethod
    def convert_order_proxy(self, raw_order) -> 'BaseOrderProxy':
        """
        将券商的原始订单对象（raw_order）转换为框架标准的 BaseOrderProxy。
        Engine 会调用此方法，从而无需知道具体券商的实现细节。
        """
        raise NotImplementedError("Broker adapter must implement convert_order_proxy(raw_order)")


    # 实盘启动协议
    @classmethod
    def launch(cls, conn_cfg: dict, strategy_path: str, params: dict, **kwargs):
        """
        [可选协议] 实盘启动入口。

        如果通过 `run.py --connect` 启动，框架会调用此方法。
        如果是被动模式或不需要启动器，子类可以不覆盖此方法。
        """
        raise NotImplementedError(
            f"Broker '{cls.__name__}' has not implemented the 'launch' method.\n"
            f"It cannot be started via the 'run.py --connect' command."
        )

    @staticmethod
    @abstractmethod
    def is_live_mode(context) -> bool:
        """
        判断当前是否为实盘模式
        """
        pass

    @staticmethod
    def extract_run_config(context) -> dict:
        """
        静态方法：从特定平台的上下文中提取运行配置。
        默认返回空字典，子类应重写此方法以实现特定逻辑。
        """
        return {}

    def order_target_percent(self, data, target, **kwargs):
        # 1. 原子操作：查价
        price = self.get_current_price(data)
        if not price or price <= 0: return None

        # 2. 通用逻辑：算净值 (支持子类覆盖优化)
        portfolio_value = self._get_portfolio_nav()

        # 3. 核心算法：算股数
        target_value = portfolio_value * target
        expected_shares = target_value / price
        pos_obj = self.get_position(data)
        delta_shares = expected_shares - pos_obj.size

        # 4. 决策分发
        if delta_shares > 0:
            return self._smart_buy(data, delta_shares, price, target, **kwargs)
        elif delta_shares < 0:
            return self._smart_sell(data, abs(delta_shares), price, **kwargs)
        return None

    def order_target_value(self, data, target, **kwargs):
        """
        按目标市值金额下单
        target: 目标持仓金额 (例如 1000 USD)
        """
        # 1. 原子操作：查价
        price = self.get_current_price(data)
        if not price or price <= 0: return None

        # 2. 核心算法：直接用目标金额除以价格
        expected_shares = target / price
        pos_obj = self.get_position(data)
        delta_shares = expected_shares - pos_obj.size

        # 3. 决策分发
        if delta_shares > 0:
            # 使用针对 Value 模式的智能买入逻辑
            return self._smart_buy_value(data, delta_shares, price, target, **kwargs)
        elif delta_shares < 0:
            return self._smart_sell(data, abs(delta_shares), price, **kwargs)
        return None

    # =========================================================
    #  智能执行逻辑 (Smart Execution)
    # =========================================================

    def _smart_buy(self, data, shares, price, target_pct, **kwargs):
        """智能买入 (Percent模式)：资金检查 + 延迟重试 + 自动降级"""
        lot_size = kwargs.get('lot_size', 100)
        cash = self.get_cash()

        # 动态安全垫
        buffer_rate = self.safety_multiplier
        estimated_cost = shares * price * buffer_rate

        if cash < estimated_cost:
            if self._has_pending_sells():
                # 有卖单在途 -> 存入延迟队列 (重试 order_target_percent)
                retry_kwargs = {'data': data, 'target': target_pct}
                retry_kwargs.update(kwargs)
                self._add_deferred(self.order_target_percent, retry_kwargs)
                return _DeferredOrderProxy(data)
            else:
                # 没钱了 -> 降级购买
                max_shares = cash / (price * buffer_rate)
                shares = min(shares, max_shares)
                if shares < 1:
                    print(f"[Broker Warning] Buy {data._name} skipped. Cash ({cash:.2f}) insufficient.")

        # 将提交和记账包裹在同一把锁内，拒绝间隙抢占
        with self._ledger_lock:
            proxy = self._finalize_and_submit(data, shares, price, lot_size)
            # 记账到虚拟账本
            if proxy:
                with self._ledger_lock:
                    self._virtual_spent_cash += (shares * price * buffer_rate)
        return proxy

    def _smart_buy_value(self, data, shares, price, target_value, **kwargs):
        """智能买入 (Value模式)：资金检查 + 延迟重试 + 自动降级"""
        lot_size = kwargs.get('lot_size', 100)
        cash = self.get_cash()

        # 动态安全垫
        buffer_rate = self.safety_multiplier
        estimated_cost = shares * price * buffer_rate

        if cash < estimated_cost:
            if self._has_pending_sells():
                # 有卖单在途 -> 存入延迟队列 (重试 order_target_value)
                retry_kwargs = {'data': data, 'target': target_value}
                retry_kwargs.update(kwargs)
                self._add_deferred(self.order_target_value, retry_kwargs)
                return _DeferredOrderProxy(data)
            else:
                # 没钱了 -> 降级购买
                max_shares = cash / (price * buffer_rate)
                shares = min(shares, max_shares)
                if shares < 1:
                    print(f"[Broker Warning] Buy {data._name} skipped. Cash ({cash:.2f}) insufficient.")

        # 将提交和记账包裹在同一把锁内，拒绝间隙抢占
        with self._ledger_lock:
            proxy = self._finalize_and_submit(data, shares, price, lot_size)
            if proxy:
                with self._ledger_lock:
                    self._virtual_spent_cash += (shares * price * buffer_rate)
        return proxy

    def _finalize_and_submit(self, data, shares, price, lot_size, retries=0):
        """通用的下单收尾逻辑：取整 + 提交"""
        if lot_size > 1:
            shares = int(shares // lot_size) * lot_size
        else:
            shares = int(shares)

        if shares > 0:
            # 根据是否为重试改变日志标签
            tag = "实盘降级重试" if retries > 0 else "实盘信号"
            log.signal('BUY', data._name, shares, price, tag=tag)

            with self._ledger_lock:
                proxy = self._submit_order(data, shares, 'BUY', price)
                if proxy:
                    # 注册到活跃买单库，记录当前的参数和重试次数
                    self._active_buys[proxy.id] = {
                        'data': data,
                        'shares': shares,
                        'price': price,
                        'lot_size': lot_size,
                        'retries': retries
                    }
            return proxy
        return None

    def _smart_sell(self, data, shares, price, **kwargs):
        """智能卖出：自动注册监控"""
        lot_size = kwargs.get('lot_size', 100)
        if lot_size > 1:
            shares = int(shares // lot_size) * lot_size
        else:
            shares = int(shares)

        if shares > 0:
            log.signal('SELL', data._name, shares, price, tag="实盘信号")

            with self._ledger_lock:
                proxy = self._submit_order(data, shares, 'SELL', price)
                if proxy:
                    self._pending_sells.add(proxy.id)  # 自动监控
            return proxy
        return None

    def on_order_status(self, proxy: BaseOrderProxy):
        """由 Engine 回调，自动维护在途单状态与降级重试"""
        oid = proxy.id

        # 整个回调必须排队，防止抢占主线程刚发出的订单
        with self._ledger_lock:
            # ==========================================
            # 1. 买单异步降级逻辑 (Buy Order Downgrade)
            # ==========================================
            if proxy.is_buy():
                if proxy.is_completed() or proxy.is_canceled():
                    self._active_buys.pop(oid, None)

                elif proxy.is_rejected():
                    with self._ledger_lock:
                        buy_info = self._active_buys.pop(oid, None)
                        if buy_info:
                            retries = buy_info['retries']
                            max_retries = 3  # 默认允许尝试降级 3 次

                            # A. 退回上一笔订单预扣的虚拟资金 (使用动态滑点)
                            refund_amount = buy_info['shares'] * buy_info['price'] * self.safety_multiplier
                            self._virtual_spent_cash = max(0.0, getattr(self, '_virtual_spent_cash', 0.0) - refund_amount)

                            # B. 检查是否还有重试机会
                            if retries < max_retries:
                                lot_size = buy_info['lot_size']
                                data = buy_info['data']
                                price = buy_info['price']

                                # 降级递减
                                new_shares = buy_info['shares'] - lot_size

                                print(f"⚠️ [Broker] 买单 {oid} 被拒绝。触发自动降级 {retries + 1}/{max_retries}...")
                                print(f"   => {data._name} 尝试数量: {buy_info['shares']} -> {new_shares}")

                                if new_shares > 0:
                                    # 再次预扣降级后的虚拟资金
                                    deduct_amount = new_shares * price * self.safety_multiplier
                                    self._virtual_spent_cash += deduct_amount

                                    # 带着新的 retries 计数再次发单，获取返回值
                                    new_proxy = self._finalize_and_submit(data, new_shares, price, lot_size,
                                                                          retries + 1)

                                    # 如果同步发单失败(比如断网)，必须把预扣的钱退回来
                                    if not new_proxy:
                                        self._virtual_spent_cash = max(0.0, getattr(self, '_virtual_spent_cash',
                                                                                    0.0) - deduct_amount)
                                        print(f"❌ [Broker] 降级发单同步失败，资金已回退。")
                                else:
                                    print(f"❌ [Broker] 降级终止: {data._name} 数量已降至 0。")
                return

            # ==========================================
            # 2. 卖单在途维护逻辑 (Sell Order Pending)
            # ==========================================
            if not proxy.is_sell(): return

            if proxy.is_completed():
                self._pending_sells.discard(oid)
            elif proxy.is_canceled() or proxy.is_rejected():
                self._pending_sells.discard(oid)
                if self._deferred_orders:
                    print(
                        f"[Broker] WARNING: Sell order {oid} failed. Cancelling {len(self._deferred_orders)} deferred buy orders.")
                    self._deferred_orders.clear()
            elif proxy.is_pending():
                self._pending_sells.add(oid)

    def process_deferred_orders(self):
        """资金回笼触发重试"""
        if not self._deferred_orders: return
        print(f"[Broker] 资金回笼，重试 {len(self._deferred_orders)} 个延迟单...")
        retry_list = self._deferred_orders[:]
        self._deferred_orders.clear()

        # 这里的 item 结构现在是通用的 {'func': func, 'kwargs': kwargs}
        for item in retry_list:
            func = item.get('func')
            kwargs = item.get('kwargs', {})
            if func:
                func(**kwargs)

    def _add_deferred(self, func, kwargs):        # 捕获闭包参数
        self._deferred_orders.append({
            'func': func,
            'kwargs': kwargs
        })

    def get_cash(self):
        """公有接口：获取资金"""
        # 扣除本地已经花掉的钱，防止穿透
        with self._ledger_lock:
            real_cash = self._fetch_real_cash() - getattr(self, '_virtual_spent_cash', 0.0)
            if real_cash < 0:
                real_cash = 0.0

        if self._cash_override is not None:
            return min(real_cash, self._cash_override)
        return real_cash

    def _has_pending_sells(self):
        return len(self._pending_sells) > 0

    def sync_balance(self):
        self._cash = self._fetch_real_cash()

    def _get_portfolio_nav(self):
        """默认 NAV 计算 (Cash + MtM)"""
        val = self.get_cash()
        for d in self.datas:
            pos = self.get_position(d)
            if pos.size:
                p = self.get_current_price(d)
                val += pos.size * p
        return val

    def _init_cash(self):
        real_cash = self._fetch_real_cash()
        if self._cash_override is not None:
            return min(real_cash, self._cash_override)
        return real_cash

    def _init_commission(self):
        """初始化：使用费率"""
        if self._commission_override is not None:
            print(f"[Live Broker] Using custom commission override: {self._commission_override:,.5f}")
            return self._commission_override
        return 0.0


    def getposition(self, data):
        """
        [API兼容写法]为了与backtrader的API（self.getposition()）保持一致
        策略代码应不感知实盘系统，直接调用此代码，自动调用子类实现的get_position()
        """
        return self.get_position(data)

    def set_datas(self, datas):
        self.datas = datas

    def set_datetime(self, dt):
        """设置当前时间，并进行跨周期检查"""
        # 检查时间是否推进 (进入了新的 Bar/Day，跨周期)
        if self._datetime and dt > self._datetime:
            # 跨周期时清空虚拟账本
            self._virtual_spent_cash = 0.0

            # 不要因为 tick/bar 的更新就清理订单（会误杀 HFT 买单）。
            # 只有在以下两种情况才清理：
            # 1. 跨日了 (New Trading Day) -> 昨天的单子肯定是死单
            # 2. 两次心跳间隔太久 (例如 > 10分钟) -> 说明程序可能断线重启过，状态不可信

            is_new_day = dt.date() > self._datetime.date()

            # 计算时间差 (秒)
            time_delta = (dt - self._datetime).total_seconds()
            is_long_gap = time_delta > 600  # 10分钟无心跳视为异常

            if is_new_day or is_long_gap:
                if self._deferred_orders:
                    print(f"[Broker] {'New Day' if is_new_day else 'Long Gap'} detected. "
                          f"Clearing {len(self._deferred_orders)} stale deferred orders.")
                    self._reset_stale_state(new_dt=dt)

            # 注意：对于同一个交易日内的正常 Bar 更新（比如 10:00 -> 10:01），
            # 我们保留 deferred_orders。因为 process_deferred_orders 会在资金到位时
            # 重新计算 target_percent，所以即使保留下来，也会用最新的价格重新下单，是安全的。

        self._datetime = dt

    @property
    def datetime(self):
        """模拟 backtrader 的 datetime 属性，使 asof() 等能工作"""
        class dt_proxy:
            def __init__(self, dt): self._dt = dt
            def datetime(self, ago=0): return self._dt
        return dt_proxy(self._datetime)

    def _reset_stale_state(self, new_dt):
        """
        清理陈旧/卡死的状态，防止死锁。
        被 set_datetime 内部调用。
        """
        print(f"[Broker Recovery] Resetting stale state at {new_dt}...")

        # 1. 清理积压的买单 (这些单子是基于旧价格/旧时间的，必须作废)
        if self._deferred_orders:
            count = len(self._deferred_orders)
            self._deferred_orders.clear()
            print(f"  >>> Auto-cleared {count} stale deferred orders (Expired).")

        # 2. 清理积压的卖单监控
        # 如果发生了跨日或长中断，旧的卖单监控大概率也失效了，重置以防误判
        if self._pending_sells:
            count = len(self._pending_sells)
            self._pending_sells.clear()
            print(f"  >>> Auto-cleared {count} pending sell monitors (Reset).")

        # 3. 清理买单跟踪器
        if hasattr(self, '_active_buys'):
            self._active_buys.clear()
        print("  >>> Broker state reset completed.")

    def force_reset_state(self):
        """
        外部强制重置接口。
        供 Engine 在捕获到 CRITICAL 异常时调用，进行兜底恢复。
        """
        print("[Broker] Force reset state requested by Engine...")
        self._deferred_orders.clear()
        self._pending_sells.clear()
        try:
            self.sync_balance()
            print(f"  >>> Balance re-synced: {self.get_cash():.2f}")
        except Exception as e:
            print(f"  >>> Warning: Failed to sync balance during reset: {e}")
        print("[Broker] Force reset state completed.")
