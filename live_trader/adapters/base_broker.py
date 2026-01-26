from abc import ABC, abstractmethod

import pandas as pd


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

    def __init__(self, context, cash_override=None, commission_override=None):
        self._context = context
        self.datas = []
        self._datetime = None
        self._cash_override = cash_override
        self._commission_override = commission_override
        # 内部状态机
        self._cash = self._init_cash()
        self._deferred_orders = []
        self._pending_sells = set()

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
        price = self._get_current_price(data)
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

    def _smart_buy(self, data, shares, price, target_pct, **kwargs):
        """智能买入：资金检查 + 延迟重试 + 自动降级"""
        lot_size = kwargs.get('lot_size', 100)
        cash = self.get_cash()
        estimated_cost = shares * price * 1.01  # 1% 缓冲

        # 资金不足时的决策树
        if cash < estimated_cost:
            if self._has_pending_sells():
                # 场景A: 有卖单在途 -> 存入延迟队列
                self._add_deferred(self.order_target_percent, locals())
                return _DeferredOrderProxy(data)
            else:
                # 场景B: 真没钱了 -> 降级购买 (Buying Power Cut)
                max_shares = cash / (price * 1.01)
                shares = min(shares, max_shares)

                # 如果降级后股数变为0，打印原因
                if shares < 1:  # 假设最小买入单位是1
                    print(
                        f"[Broker Warning] Buy {data._name} skipped. Cash ({cash:.2f}) insufficient for price {price:.2f}.")

        # 取整逻辑
        if lot_size > 1:
            shares = int(shares // lot_size) * lot_size
        else:
            shares = int(shares)

        if shares > 0:
            return self._submit_order(data, shares, 'BUY', price)
        return None

    def _smart_sell(self, data, shares, price, **kwargs):
        """智能卖出：自动注册监控"""
        lot_size = kwargs.get('lot_size', 100)
        if lot_size > 1:
            shares = int(shares // lot_size) * lot_size
        else:
            shares = int(shares)

        if shares > 0:
            proxy = self._submit_order(data, shares, 'SELL', price)
            if proxy: self._pending_sells.add(proxy.id)  # 自动监控
            return proxy
        return None

    def on_order_status(self, proxy: BaseOrderProxy):
        """由 Engine 回调，自动维护在途单状态"""
        if not proxy.is_sell(): return

        oid = proxy.id

        # 1. 正常完成
        if proxy.is_completed():
            self._pending_sells.discard(oid)

        # 2. 卖单失败 (撤单/拒单) -> 触发自动解除死锁
        elif proxy.is_canceled() or proxy.is_rejected():
            self._pending_sells.discard(oid)

            # 自动解除死锁 (Auto-Resolve Deadlock)
            # 之前我们在这里抛出 RuntimeError 终止程序以暴露问题。
            # 现在我们将其改为“优雅降级”：
            # 既然卖单挂了（钱回不来了），那么依赖这笔钱的延迟买单也必须作废。
            if self._deferred_orders:
                print(f"[Broker] WARNING: Sell order {oid} failed (Status: {getattr(proxy, 'status', 'Unknown')}). "
                      f"Cancelling {len(self._deferred_orders)} deferred buy orders due to funding failure.")

                # 直接清空延迟队列，防止它们变成第二天的幽灵单
                self._deferred_orders.clear()

                # 不抛出异常，允许程序继续运行（活着才有机会！）
                # raise RuntimeError(
                #     f"CRITICAL: Sell order {oid} failed (Status: {proxy.status}), "
                #     f"and no other sells are pending. "
                #     f"{len(self._deferred_orders)} deferred buy orders are stranded! "
                #     f"Execution terminated to prevent ghost orders."
                # )

        # 3. 挂单中
        elif proxy.is_pending():
            self._pending_sells.add(oid)

    def process_deferred_orders(self):
        """资金回笼触发重试"""
        if not self._deferred_orders: return
        print(f"[Broker] 资金回笼，重试 {len(self._deferred_orders)} 个延迟单...")
        retry_list = self._deferred_orders[:]
        self._deferred_orders.clear()
        for item in retry_list:
            # 过滤掉不需要透传的参数
            kwargs = {k: v for k, v in item['kwargs'].items()
                      if k not in ['data', 'shares', 'price', 'target_pct', 'cash', 'estimated_cost']}
            # 重新进入 Template Method
            self.order_target_percent(item['data'], item['target_pct'], **kwargs)

    def _add_deferred(self, func, local_vars):
        # 捕获闭包参数
        self._deferred_orders.append({
            'data': local_vars['data'],
            'target_pct': local_vars['target_pct'],
            'kwargs': local_vars['kwargs']
        })

    def get_cash(self):
        """公有接口：获取资金"""
        return self._fetch_real_cash()

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
                p = self._get_current_price(d)
                val += pos.size * p
        return val

    def _init_cash(self):
        if self._cash_override: return self._cash_override
        return self._fetch_real_cash()

    def _init_commission(self):
        """初始化：使用费率"""
        if self._commission_override is not None:
            print(f"[Live Broker] Using custom commission override: {self._commission_override:,.5f}")
            return self._commission_override
        return 0.0

    # =========================================================
    #  用户只需实现这 4 个原子接口 (The Minimum Set)
    # =========================================================
    @abstractmethod
    def _fetch_real_cash(self) -> float:
        """子类必须实现，用于获取真实账户的可用资金"""
        pass

    @abstractmethod
    def get_position(self, data):
        """子类必须实现，用于获取指定标的的持仓"""
        pass

    @abstractmethod
    def _get_current_price(self, data) -> float:
        """子类必须实现，用于获取指定标的实时价格"""
        pass

    @abstractmethod
    def _submit_order(self, data, volume, side, price):
        """子类必须实现，用于提交指定标的买入或卖出操作"""
        pass

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
        # 检查时间是否推进 (进入了新的 Bar/Day)
        if self._datetime and dt > self._datetime:

            # 【新增】不再悄悄清理，而是直接“爆破”
            # 如果新的一天开始了，但兜里还揣着昨天的延迟单，说明逻辑严重泄漏
            if self._deferred_orders:
                raise RuntimeError(
                    f"CRITICAL: Ghost Orders Detected at Market Open! "
                    f"{len(self._deferred_orders)} deferred orders were left over from {self._datetime}. "
                    f"This usually means a Sell order didn't complete by market close. "
                    f"Current time: {dt}"
                )

            # 清理卖单监控（可选，防止跨日ID污染，但通常上面的检查已经足够拦截问题）
            # self._pending_sells.clear()

        self._datetime = dt

    @property
    def datetime(self):
        """模拟 backtrader 的 datetime 属性，使 asof() 等能工作"""

        class dt_proxy:
            def __init__(self, dt): self._dt = dt

            def datetime(self, ago=0): return self._dt

        return dt_proxy(self._datetime)

    def log(self, txt, dt=None):
        log_time = dt or self._datetime or pd.Timestamp.now()
        print(f"[{log_time.strftime('%Y-%m-%d %H:%M:%S')}] {txt}")
