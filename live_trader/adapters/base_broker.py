import threading
import time
from abc import ABC, abstractmethod

import pandas as pd

import config
from common import log

from alarms.manager import AlarmManager


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
    # 自愈节流：1 秒一轮足够覆盖常见回调抖动，又不会产生高频噪音
    SELF_HEAL_MIN_INTERVAL_SECONDS = 1.0
    # 在存在运行时积压时，最短快照轮询间隔（降低心跳场景下的快照压力）
    PENDING_SNAPSHOT_MIN_INTERVAL_SECONDS = 2.0
    # 无回调兜底：deferred 至少每 2 秒尝试一次重放，避免“只剩现金”卡住
    DEFERRED_REPLAY_INTERVAL_SECONDS = 2.0
    # 缓冲重试等待告警阈值
    BUFFERED_RETRY_WARN_SECONDS = 20.0
    # 在途快照查询的轻量重试参数（用于吸收短暂网络抖动）
    PENDING_SNAPSHOT_RETRY_ATTEMPTS = 2
    PENDING_SNAPSHOT_RETRY_SLEEP_SECONDS = 0.05
    # 连续快照失败触发“不确定模式”。
    # 夜间无人值守策略下，不确定状态里宁可少交易，也不能新增风险敞口。
    PENDING_SNAPSHOT_UNCERTAIN_FAILS = 3
    PENDING_SNAPSHOT_UNCERTAIN_TTL_SECONDS = 60.0
    # 空快照清理的最短等待时间（配合连续次数阈值使用）
    PENDING_SELL_CLEAR_EMPTY_MIN_SECONDS = 20.0
    ACTIVE_BUY_CLEAR_EMPTY_MIN_SECONDS = 20.0
    # 订单状态记忆容量与生存期（用于快照不可用时的安全回退）
    ORDER_STATE_MEMORY_MAX_ITEMS = 5000
    ORDER_STATE_MEMORY_TTL_SECONDS = 12 * 3600
    # 可交易资金输入退化后的 fast-fail 窗口（秒）
    CASH_DEGRADED_TTL_SECONDS = 30.0

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
        # IB 等柜台会先推 Inactive 再推 Cancelled；Rejected 重试需等待原单真正出清
        self._buffered_rejected_retries = {}
        self._active_buy_empty_snapshots = 0
        self._strategy_deferred_empty_since = None
        self._last_deferred_replay_ts = 0.0
        # 虚拟账本读写锁
        self._ledger_lock = threading.RLock()
        # 风控锁定黑名单
        self._risk_locked_symbols = set()
        # 自愈心跳节流
        self._last_self_heal_ts = 0.0
        # 连续“卖单在途为空”快照计数，用于防止单次快照抖动误清理
        self._pending_sell_empty_snapshots = 0
        self._pending_sell_empty_since = None
        self._active_buy_empty_since = None
        # 订单状态记忆：在途快照异常时，仅基于“已观察到的终态”做安全回退
        self._order_state_memory = {}
        # 在途快照健康状态与不确定模式窗口
        self._pending_snapshot_fail_count = 0
        self._pending_snapshot_fail_since = None
        self._last_pending_snapshot_ts = 0.0
        self._uncertain_mode_until = 0.0
        self._last_uncertain_replay_skip_log_ts = 0.0
        self._last_buffered_snapshot_skip_log_ts = 0.0
        # 资金输入退化状态（用于 strategy fast-fail 闸门）
        self._cash_degraded_until = 0.0
        self._cash_degraded_reason = ""

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
        # 如果没有传入时间，优先使用当前 Broker 所在的仿真时间
        if dt is None:
            dt = getattr(self, '_datetime', None)

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
    def get_pending_orders(self) -> list:
        """
        [实盘防爆仓] 子类必须实现。获取所有未完成的在途订单。
        返回统一格式: [{'symbol': 'SHSE.510300', 'direction': 'BUY', 'size': 1000}, ...]
        """
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

        # 改用预期仓位计算差额
        current_size = self.get_expected_size(data)
        delta_shares = expected_shares - current_size

        # 风控拦截：Percent 模式与 Value 模式保持一致
        if data._name in self._risk_locked_symbols and delta_shares > 0:
            print(f"[Broker Risk Block] 🚫 风控拦截: {data._name} 触发风控，买单已被底层静默吃掉。")
            return None

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

        # 改用预期仓位计算差额
        current_size = self.get_expected_size(data)
        delta_shares = expected_shares - current_size

        # 风控拦截
        if data._name in self._risk_locked_symbols and delta_shares > 0:
            print(f"[Broker Risk Block] 🚫 风控拦截: {data._name} 触发风控，买单已被底层静默吃掉。")
            return None

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
        lot_size = config.LOT_SIZE
        cash = self.get_cash()

        # 策略约束：状态不确定时禁止新增敞口，只允许入队等待恢复。
        if self.is_uncertain_mode():
            return self._queue_uncertain_buy_retry(self.order_target_percent, data, target_pct, **kwargs)

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
                submitted_shares = self._active_buys.get(proxy.id, {}).get('shares', shares)
                self._virtual_spent_cash += (submitted_shares * price * buffer_rate)
        return proxy

    def _smart_buy_value(self, data, shares, price, target_value, **kwargs):
        """智能买入 (Value模式)：资金检查 + 延迟重试 + 自动降级"""
        lot_size = config.LOT_SIZE
        cash = self.get_cash()

        # 策略约束：状态不确定时禁止新增敞口，只允许入队等待恢复。
        if self.is_uncertain_mode():
            return self._queue_uncertain_buy_retry(self.order_target_value, data, target_value, **kwargs)

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
                submitted_shares = self._active_buys.get(proxy.id, {}).get('shares', shares)
                self._virtual_spent_cash += (submitted_shares * price * buffer_rate)
        return proxy

    def _infer_submitted_shares(self, proxy, fallback_shares):
        """
        推断券商最终受理的委托数量。
        某些适配器会在 _submit_order 内做二次降仓，必须以真实数量记账。
        """
        try:
            fallback = int(abs(float(fallback_shares)))
        except Exception:
            fallback = 0

        if not proxy:
            return fallback

        def _read_path(obj, path):
            cur = obj
            for attr in path:
                if not hasattr(cur, attr):
                    return None
                cur = getattr(cur, attr)
            return cur

        candidate_paths = [
            ('submitted_size',),              # 适配器可选显式字段
            ('requested_size',),              # 适配器可选显式字段
            ('trade', 'order', 'totalQuantity'),
            ('platform_order', 'volume'),
            ('raw_order', 'volume'),
            ('order', 'totalQuantity'),
        ]

        for path in candidate_paths:
            raw = _read_path(proxy, path)
            try:
                val = int(abs(float(raw)))
                if val > 0:
                    return val
            except Exception:
                continue

        return fallback

    @staticmethod
    def _resolve_proxy_symbol(proxy, fallback='Unknown'):
        """尽可能从不同代理结构中提取标的名，用于人类可读日志。"""
        try:
            data = getattr(proxy, 'data', None)
            if data is not None:
                name = getattr(data, '_name', None)
                if name:
                    return str(name)
        except Exception:
            pass

        candidate_paths = [
            ('trade', 'contract', 'symbol'),
            ('platform_order', 'symbol'),
            ('raw_order', 'symbol'),
            ('symbol',),
        ]
        for path in candidate_paths:
            cur = proxy
            ok = True
            for attr in path:
                if not hasattr(cur, attr):
                    ok = False
                    break
                cur = getattr(cur, attr)
            if ok and cur:
                return str(cur)
        return fallback

    @staticmethod
    def _symbol_aliases(symbol):
        """
        生成跨市场符号别名集合，用于在途对账与状态匹配。
        目标：
        - 兼容 AAPL.SMART / QQQ.ISLAND 与 AAPL / QQQ
        - 兼容 SHSE.600000 / SEHK.00700 与 600000 / 700
        """
        raw = str(symbol or '').strip().upper()
        if not raw:
            return set()

        aliases = {raw}
        try:
            from common.ib_symbol_parser import resolve_ib_contract_spec
            spec = resolve_ib_contract_spec(raw)
            kind = str(spec.get('kind', '')).lower()
            if kind == 'stock':
                core = str(spec.get('symbol', '') or '').strip().upper()
                if core:
                    aliases.add(core)
                    if core.isdigit():
                        aliases.add(str(int(core)))
            elif kind == 'forex':
                pair = str(spec.get('pair', '') or '').strip().upper()
                if pair:
                    aliases.add(pair)
            elif kind == 'crypto':
                core = str(spec.get('symbol', '') or '').strip().upper()
                if core:
                    aliases.add(core)
        except Exception:
            pass

        parts = raw.split('.')
        if len(parts) == 2:
            left, right = parts
            # Exchange.Ticker: SHSE.600000 / SEHK.00700
            if left in {'SHSE', 'SZSE', 'SEHK', 'HK'} and right:
                aliases.add(right)
                if right.isdigit():
                    aliases.add(str(int(right)))
        return {a for a in aliases if a}

    def _symbols_match(self, left, right):
        if not left or not right:
            return False
        left_aliases = self._symbol_aliases(left)
        right_aliases = self._symbol_aliases(right)
        return bool(left_aliases and right_aliases and (left_aliases & right_aliases))

    def _trim_order_state_memory(self, now_ts=None):
        now = float(now_ts if now_ts is not None else time.time())
        mem = getattr(self, '_order_state_memory', None)
        if not isinstance(mem, dict) or not mem:
            return

        ttl_cfg = getattr(config, 'BROKER_ORDER_STATE_MEMORY_TTL_SECONDS', self.ORDER_STATE_MEMORY_TTL_SECONDS)
        try:
            ttl = float(ttl_cfg)
        except Exception:
            ttl = float(self.ORDER_STATE_MEMORY_TTL_SECONDS)
        if ttl > 0:
            stale_keys = [
                k for k, v in mem.items()
                if now - float((v or {}).get('updated_at', 0.0) or 0.0) > ttl
            ]
            for k in stale_keys:
                mem.pop(k, None)

        max_items_cfg = getattr(config, 'BROKER_ORDER_STATE_MEMORY_MAX_ITEMS', self.ORDER_STATE_MEMORY_MAX_ITEMS)
        try:
            max_items = int(max_items_cfg)
        except Exception:
            max_items = int(self.ORDER_STATE_MEMORY_MAX_ITEMS)
        max_items = max(100, max_items)
        overflow = len(mem) - max_items
        if overflow > 0:
            ordered = sorted(mem.items(), key=lambda kv: float((kv[1] or {}).get('updated_at', 0.0) or 0.0))
            for key, _ in ordered[:overflow]:
                mem.pop(key, None)

    def _remember_order_state(self, proxy):
        """记录订单最近状态，用于快照不可用时的保守/安全回退。"""
        oid = str(getattr(proxy, 'id', '') or '').strip()
        if not oid:
            return

        symbol = self._resolve_proxy_symbol(proxy)
        side = ''
        try:
            if proxy.is_buy():
                side = 'BUY'
            elif proxy.is_sell():
                side = 'SELL'
        except Exception:
            side = ''

        terminal = False
        try:
            terminal = bool(proxy.is_completed() or proxy.is_canceled() or proxy.is_rejected())
        except Exception:
            terminal = False

        pending = False
        if not terminal:
            try:
                pending = bool(proxy.is_pending() or proxy.is_accepted())
            except Exception:
                pending = False

        self._order_state_memory[oid] = {
            'symbol': str(symbol or ''),
            'side': side,
            'terminal': terminal,
            'pending': pending,
            'updated_at': time.time(),
        }
        self._trim_order_state_memory()

    def _pending_state_from_memory(self, order_id, symbol=None, side=None):
        oid = str(order_id or '').strip()
        if not oid:
            return None

        item = getattr(self, '_order_state_memory', {}).get(oid)
        if not isinstance(item, dict):
            return None

        side_norm = str(side).upper() if side else ''
        item_side = str(item.get('side', '')).upper()
        if side_norm and item_side and side_norm != item_side:
            return None

        symbol_norm = str(symbol).upper() if symbol else ''
        item_symbol = str(item.get('symbol', '')).upper()
        if symbol_norm and item_symbol:
            # Unknown 视为“无法提供符号约束”，不用于否决该条状态记忆。
            if item_symbol not in {'UNKNOWN', 'UNK', '?'}:
                if not self._symbols_match(item_symbol, symbol_norm):
                    return None

        if bool(item.get('terminal')):
            return False
        if bool(item.get('pending')):
            return True
        return None

    def _mark_pending_snapshot_success(self):
        with self._ledger_lock:
            self._pending_snapshot_fail_count = 0
            self._pending_snapshot_fail_since = None

    def _mark_pending_snapshot_failure(self, reason="unknown"):
        now_ts = time.time()
        with self._ledger_lock:
            self._pending_snapshot_fail_count = int(getattr(self, '_pending_snapshot_fail_count', 0)) + 1
            if self._pending_snapshot_fail_since is None:
                self._pending_snapshot_fail_since = now_ts

            fail_cfg = getattr(
                config,
                'BROKER_PENDING_SNAPSHOT_UNCERTAIN_FAILS',
                self.PENDING_SNAPSHOT_UNCERTAIN_FAILS
            )
            ttl_cfg = getattr(
                config,
                'BROKER_PENDING_SNAPSHOT_UNCERTAIN_TTL_SECONDS',
                self.PENDING_SNAPSHOT_UNCERTAIN_TTL_SECONDS
            )

            try:
                fail_threshold = int(fail_cfg)
            except Exception:
                fail_threshold = int(self.PENDING_SNAPSHOT_UNCERTAIN_FAILS)
            fail_threshold = max(1, fail_threshold)

            try:
                ttl_seconds = float(ttl_cfg)
            except Exception:
                ttl_seconds = float(self.PENDING_SNAPSHOT_UNCERTAIN_TTL_SECONDS)
            ttl_seconds = max(0.0, ttl_seconds)

            if self._pending_snapshot_fail_count >= fail_threshold:
                prev_until = float(getattr(self, '_uncertain_mode_until', 0.0) or 0.0)
                self._uncertain_mode_until = max(prev_until, now_ts + ttl_seconds)
                if prev_until <= now_ts:
                    print(
                        f"[Broker Safety] Entered uncertain mode for {ttl_seconds:.1f}s "
                        f"(pending snapshot unstable, reason={reason})."
                    )

    def is_uncertain_mode(self):
        until = float(getattr(self, '_uncertain_mode_until', 0.0) or 0.0)
        return until > time.time()

    def mark_cash_degraded(self, reason="unknown", ttl_seconds=None):
        """
        标记“可交易资金输入退化”。
        语义：策略层应 fast-fail，避免在资金口径不可信时继续执行调仓逻辑。
        """
        ttl_cfg = getattr(config, 'BROKER_CASH_DEGRADED_TTL_SECONDS', self.CASH_DEGRADED_TTL_SECONDS)
        if ttl_seconds is None:
            ttl_seconds = ttl_cfg
        try:
            ttl = float(ttl_seconds)
        except Exception:
            ttl = float(self.CASH_DEGRADED_TTL_SECONDS)
        ttl = max(0.0, ttl)

        now_ts = time.time()
        with self._ledger_lock:
            prev_until = float(getattr(self, '_cash_degraded_until', 0.0) or 0.0)
            self._cash_degraded_until = max(prev_until, now_ts + ttl)
            self._cash_degraded_reason = str(reason or "unknown")
            if prev_until <= now_ts:
                print(
                    f"[Broker Safety] Cash input degraded for {ttl:.1f}s "
                    f"(reason={self._cash_degraded_reason})."
                )

    def clear_cash_degraded(self):
        with self._ledger_lock:
            self._cash_degraded_until = 0.0
            self._cash_degraded_reason = ""

    def is_cash_degraded(self):
        until = float(getattr(self, '_cash_degraded_until', 0.0) or 0.0)
        return until > time.time()

    def get_cash_degraded_reason(self):
        if not self.is_cash_degraded():
            return ""
        return str(getattr(self, '_cash_degraded_reason', "") or "")

    def _queue_uncertain_buy_retry(self, retry_func, data, target, **kwargs):
        """
        不确定模式下，买单只排队不执行。
        这是有意设计：夜间无人值守时优先“防扩大亏损”，而不是“追求信号不丢失”。
        """
        retry_kwargs = {'data': data, 'target': target}
        retry_kwargs.update(kwargs)
        symbol = getattr(data, '_name', 'Unknown')
        func_name = getattr(retry_func, '__name__', '')

        with self._ledger_lock:
            replaced = False
            for item in self._deferred_orders:
                if not isinstance(item, dict):
                    continue
                item_func = item.get('func')
                item_kwargs = item.get('kwargs') or {}
                queued_data = item_kwargs.get('data')
                queued_symbol = getattr(queued_data, '_name', None)
                if getattr(item_func, '__name__', '') == func_name and queued_symbol == symbol:
                    # 只保留同 symbol 的最新目标，避免队列膨胀和陈旧目标重放。
                    item['kwargs'] = retry_kwargs
                    item['updated_at'] = time.time()
                    replaced = True
                    break

        if not replaced:
            self._add_deferred(retry_func, retry_kwargs)
            print(f"[Broker Safety] BUY {symbol} deferred (uncertain mode).")
        else:
            print(f"[Broker Safety] BUY {symbol} deferred target refreshed (uncertain mode).")
        return _DeferredOrderProxy(data)

    def _fetch_pending_orders_with_retry(self, reason="unknown"):
        """
        轻量重试获取在途快照，吸收短暂网络抖动。
        失败时抛出原始异常，由上层选择保守/降级行为。
        """
        attempts_cfg = getattr(config, 'BROKER_PENDING_SNAPSHOT_RETRY_ATTEMPTS', self.PENDING_SNAPSHOT_RETRY_ATTEMPTS)
        sleep_cfg = getattr(config, 'BROKER_PENDING_SNAPSHOT_RETRY_SLEEP_SECONDS', self.PENDING_SNAPSHOT_RETRY_SLEEP_SECONDS)

        try:
            attempts = int(attempts_cfg)
        except Exception:
            attempts = int(self.PENDING_SNAPSHOT_RETRY_ATTEMPTS)
        attempts = max(1, attempts)

        try:
            sleep_s = float(sleep_cfg)
        except Exception:
            sleep_s = float(self.PENDING_SNAPSHOT_RETRY_SLEEP_SECONDS)
        sleep_s = max(0.0, sleep_s)

        last_exc = None
        for idx in range(attempts):
            try:
                pending_orders = self.get_pending_orders()
                self._mark_pending_snapshot_success()
                if pending_orders is None:
                    return []
                if isinstance(pending_orders, list):
                    return pending_orders
                return list(pending_orders)
            except Exception as e:
                last_exc = e
                if idx + 1 < attempts:
                    if idx == 0:
                        print(f"[Broker Heal] Pending snapshot query failed ({reason}), retrying...")
                    if sleep_s > 0:
                        time.sleep(sleep_s)
                    continue
                break
        if last_exc is not None:
            self._mark_pending_snapshot_failure(reason=reason)
            raise last_exc
        return []

    def _recalc_rejected_buy_shares(self, old_shares, price, lot_size):
        """
        买单拒绝后按当前可用资金重算可下单数量。
        返回值会严格小于 old_shares，避免重复提交同等数量导致死循环拒单。
        """
        try:
            old_int = int(abs(float(old_shares)))
            lot_int = int(abs(float(lot_size)))
            px = float(price)
        except Exception:
            return 0

        if old_int <= 0 or px <= 0:
            return 0

        lot_int = max(1, lot_int)
        try:
            cash_now = float(self.get_cash())
        except Exception:
            return 0

        if cash_now <= 0:
            return 0

        max_affordable = cash_now / (px * self.safety_multiplier)
        if lot_int > 1:
            recalc_shares = int(max_affordable // lot_int) * lot_int
        else:
            recalc_shares = int(max_affordable)

        # 拒单后重试必须收缩到更小的数量，防止重复被拒。
        upper_bound = old_int - lot_int
        recalc_shares = min(recalc_shares, upper_bound)
        return max(0, recalc_shares)

    def _is_order_still_pending(self, order_id, symbol=None, side=None, pending_orders=None, snapshot_unavailable=False):
        """
        检查订单是否仍在柜台在途。
        优先按订单 id 精准匹配；
        若券商不返回 id（例如部分 API），则降级按 symbol + side 粗匹配。
        返回值:
        - True: 明确仍在途
        - False: 明确不在途
        - None: 在途快照不可用（查询异常）
        """
        if not order_id and not symbol:
            return False
        if pending_orders is None and not snapshot_unavailable:
            try:
                pending_orders = self._fetch_pending_orders_with_retry(reason="is_order_still_pending")
            except Exception:
                memory_state = self._pending_state_from_memory(order_id, symbol=symbol, side=side)
                if memory_state is not None:
                    return memory_state
                return None
        elif snapshot_unavailable:
            memory_state = self._pending_state_from_memory(order_id, symbol=symbol, side=side)
            if memory_state is not None:
                return memory_state
            return None

        found_id_field = False
        symbol_matched_without_id = False
        oid = str(order_id) if order_id else ''
        sym_norm = str(symbol).upper() if symbol else ''
        side_norm = str(side).upper() if side else ''

        for po in pending_orders or []:
            if not isinstance(po, dict):
                continue
            direction = str(po.get('direction', '')).upper()
            if side_norm and direction and direction != side_norm:
                continue

            poid = po.get('id')
            if poid is not None and str(poid).strip():
                found_id_field = True
                if oid and str(poid) == oid:
                    return True
                continue

            if sym_norm:
                po_sym = str(po.get('symbol', '')).upper()
                if self._symbols_match(po_sym, sym_norm):
                    symbol_matched_without_id = True

        if symbol_matched_without_id:
            # 混合快照（部分订单有 id、部分无 id）时，仍允许 symbol 级保守兜底，
            # 避免因返回顺序导致“误判不在途”而提前释放缓冲重试。
            return True

        if found_id_field:
            return False
        return False

    def _reconcile_pending_sells_from_broker(self, pending_orders=None, snapshot_error=None):
        """
        与柜台在途订单对账 _pending_sells，修复回调缺失导致的本地状态漂移。
        要求调用方已持有 _ledger_lock。
        """
        # 快照明确不可用时保持保守，不在持锁路径重复打网络。
        if snapshot_error is not None and pending_orders is None:
            return 0
        if pending_orders is None:
            try:
                pending_orders = self._fetch_pending_orders_with_retry(reason="reconcile_pending_sells")
            except Exception:
                return 0

        live_sell_orders = []
        live_sell_ids = set()
        for po in pending_orders or []:
            if not isinstance(po, dict):
                continue
            direction = str(po.get('direction', '')).upper()
            if direction != 'SELL':
                continue
            try:
                remain = float(po.get('size', 0) or 0)
            except Exception:
                remain = 0.0
            if remain <= 0:
                continue
            live_sell_orders.append(po)
            poid = po.get('id')
            if poid is None or str(poid).strip() == '':
                continue
            live_sell_ids.add(str(poid))

        # 情况 A: 柜台快照显示当前无任何 SELL 在途
        # 采用“连续空快照 + 最短等待时长”再清理，降低网络抖动误判概率。
        if not live_sell_orders:
            if not self._pending_sells:
                self._pending_sell_empty_snapshots = 0
                self._pending_sell_empty_since = None
                return 0

            self._pending_sell_empty_snapshots = int(getattr(self, '_pending_sell_empty_snapshots', 0)) + 1
            if self._pending_sell_empty_since is None:
                self._pending_sell_empty_since = time.time()
            clear_threshold = int(
                getattr(config, 'BROKER_PENDING_SELL_CLEAR_EMPTY_SNAPSHOTS', 2) or 2
            )
            clear_threshold = max(1, clear_threshold)
            min_wait_cfg = getattr(
                config,
                'BROKER_PENDING_SELL_CLEAR_EMPTY_SECONDS',
                self.PENDING_SELL_CLEAR_EMPTY_MIN_SECONDS
            )
            try:
                min_wait_seconds = float(min_wait_cfg)
            except Exception:
                min_wait_seconds = float(self.PENDING_SELL_CLEAR_EMPTY_MIN_SECONDS)
            min_wait_seconds = max(0.0, min_wait_seconds)
            waited = max(0.0, time.time() - float(self._pending_sell_empty_since or time.time()))

            if (
                self._pending_sells
                and self._pending_sell_empty_snapshots >= clear_threshold
                and waited >= min_wait_seconds
            ):
                stale_cnt = len(self._pending_sells)
                self._pending_sells.clear()
                self._pending_sell_empty_snapshots = 0
                self._pending_sell_empty_since = None
                print(f"[Broker Heal] Cleared {stale_cnt} stale pending-sell markers (broker snapshot empty).")
                return stale_cnt
            return 0

        # 只要看到真实 SELL 在途，空快照计数归零
        self._pending_sell_empty_snapshots = 0
        self._pending_sell_empty_since = None

        # 情况 B: 柜台有 SELL 在途，但不提供 id，无法做精确集合对账
        if not live_sell_ids:
            return 0

        removed = self._pending_sells - live_sell_ids
        added = live_sell_ids - self._pending_sells
        if removed:
            self._pending_sells -= removed
            print(f"[Broker Heal] Cleared {len(removed)} stale pending-sell markers.")
        if added:
            self._pending_sells |= added
            print(f"[Broker Heal] Synced {len(added)} pending-sell markers from broker snapshot.")
        return len(removed) + len(added)

    def _reconcile_active_buys_from_broker(self, pending_orders=None, snapshot_error=None):
        """
        与柜台在途 BUY 快照对账 _active_buys，修复回调缺失导致的幽灵占资。
        要求调用方已持有 _ledger_lock。
        """
        if not self._active_buys:
            self._active_buy_empty_snapshots = 0
            self._active_buy_empty_since = None
            return 0

        # 快照明确不可用时保持保守，不在持锁路径重复打网络。
        if snapshot_error is not None and pending_orders is None:
            return 0
        if pending_orders is None:
            try:
                pending_orders = self._fetch_pending_orders_with_retry(reason="reconcile_active_buys")
            except Exception:
                return 0

        live_buy_ids = set()
        live_buy_symbol_aliases = set()
        has_live_buy_without_id = False
        for po in pending_orders or []:
            if not isinstance(po, dict):
                continue
            direction = str(po.get('direction', '')).upper()
            if direction != 'BUY':
                continue
            try:
                remain = float(po.get('size', 0) or 0)
            except Exception:
                remain = 0.0
            if remain <= 0:
                continue
            poid = po.get('id')
            if poid is not None and str(poid).strip():
                live_buy_ids.add(str(poid))
            else:
                # 混合快照场景下保留标记：
                # 若存在“无 id BUY”，后续不能完全关闭 symbol 兜底，否则可能提前释放占资。
                has_live_buy_without_id = True
            po_symbol = str(po.get('symbol', '')).upper().strip()
            if po_symbol:
                live_buy_symbol_aliases |= self._symbol_aliases(po_symbol)
        has_live_buy_ids = bool(live_buy_ids)

        now_ts = time.time()
        clear_threshold = int(
            getattr(config, 'BROKER_ACTIVE_BUY_CLEAR_EMPTY_SNAPSHOTS', 2) or 2
        )
        clear_threshold = max(1, clear_threshold)
        min_wait_cfg = getattr(
            config,
            'BROKER_ACTIVE_BUY_CLEAR_EMPTY_SECONDS',
            self.ACTIVE_BUY_CLEAR_EMPTY_MIN_SECONDS
        )
        try:
            min_wait_seconds = float(min_wait_cfg)
        except Exception:
            min_wait_seconds = float(self.ACTIVE_BUY_CLEAR_EMPTY_MIN_SECONDS)
        min_wait_seconds = max(0.0, min_wait_seconds)

        stale_keys = []
        stale_items = []
        for oid, info in list(self._active_buys.items()):
            if not isinstance(info, dict):
                continue

            symbol_name = getattr(info.get('data'), '_name', '') if info.get('data') is not None else ''
            symbol_aliases = self._symbol_aliases(symbol_name)

            seen_on_broker = False
            if str(oid) in live_buy_ids:
                seen_on_broker = True
            elif (
                symbol_aliases
                and (symbol_aliases & live_buy_symbol_aliases)
                and ((not has_live_buy_ids) or has_live_buy_without_id)
            ):
                # 仅 id 快照时仍优先 id 精确对账；
                # 混合快照（有些单缺 id）下允许 symbol 级保守兜底，避免误清理仍在途 BUY。
                seen_on_broker = True

            if seen_on_broker:
                info.pop('miss_snapshots', None)
                info.pop('miss_since', None)
                continue

            miss = int(info.get('miss_snapshots', 0) or 0) + 1
            info['miss_snapshots'] = miss
            if info.get('miss_since') is None:
                info['miss_since'] = now_ts
            miss_waited = max(0.0, now_ts - float(info.get('miss_since') or now_ts))
            created_at = float(info.get('created_at', now_ts) or now_ts)
            age = max(0.0, now_ts - created_at)

            # 双条件释放：连续缺失且订单已存在足够时间。
            if miss < clear_threshold or age < min_wait_seconds:
                continue
            if miss_waited < min_wait_seconds:
                continue

            stale_keys.append(str(oid))
            stale_items.append(info)

        if not stale_keys:
            return 0

        release_amount = 0.0
        for info in stale_items:
            try:
                release_amount += (
                    float(info.get('shares', 0) or 0)
                    * float(info.get('price', 0) or 0)
                    * self.safety_multiplier
                )
            except Exception:
                continue
        for oid in stale_keys:
            self._active_buys.pop(oid, None)
        self._active_buy_empty_snapshots = 0
        self._active_buy_empty_since = None
        if release_amount > 0:
            self._virtual_spent_cash = max(
                0.0,
                getattr(self, '_virtual_spent_cash', 0.0) - release_amount
            )
        stale_cnt = len(stale_keys)
        print(
            f"[Broker Heal] Cleared {stale_cnt} stale active-buy trackers "
            f"(broker BUY snapshot mismatch). Released virtual cash: {release_amount:.2f}"
        )
        return stale_cnt

    def _drain_buffered_rejected_retries(self, reason="unknown", pending_orders=None, snapshot_error=None):
        """
        尝试释放已不在途的“拒单缓冲重试”。
        要求调用方已持有 _ledger_lock。
        """
        if not self._buffered_rejected_retries:
            return 0

        snapshot_unavailable = snapshot_error is not None
        if pending_orders is None and not snapshot_unavailable:
            lock_owned = False
            if hasattr(self._ledger_lock, '_is_owned'):
                try:
                    lock_owned = bool(self._ledger_lock._is_owned())
                except Exception:
                    lock_owned = False

            # 在已持锁路径下避免发起网络查询，防止把回调线程一并堵住。
            if lock_owned:
                snapshot_unavailable = True
                now_ts = time.time()
                if now_ts - float(getattr(self, '_last_buffered_snapshot_skip_log_ts', 0.0) or 0.0) >= 10.0:
                    self._last_buffered_snapshot_skip_log_ts = now_ts
                    print(f"[Broker Heal] Skip buffered retry snapshot query while lock is held (reason={reason}).")
                return 0
            else:
                try:
                    pending_orders = self._fetch_pending_orders_with_retry(reason=f"drain_buffered:{reason}")
                except Exception as e:
                    snapshot_unavailable = True
                    snapshot_error = e

        max_wait = float(self.BUFFERED_RETRY_WARN_SECONDS)
        drained = 0
        now_ts = time.time()
        keys = list(self._buffered_rejected_retries.keys())
        for key in keys:
            payload = self._buffered_rejected_retries.get(key)
            if not payload:
                continue
            symbol = payload.get('symbol')
            pending_state = self._is_order_still_pending(
                key,
                symbol=symbol,
                side='BUY',
                pending_orders=pending_orders,
                snapshot_unavailable=snapshot_unavailable,
            )

            if pending_state is None:
                fail_count = int(payload.get('pending_query_fail_count', 0)) + 1
                payload['pending_query_fail_count'] = fail_count
                fail_since = payload.get('pending_query_fail_since')
                if fail_since is None:
                    fail_since = now_ts
                    payload['pending_query_fail_since'] = fail_since
                fail_waited = max(0.0, now_ts - float(fail_since))

                if not payload.get('warned_query_unavailable'):
                    payload['warned_query_unavailable'] = True
                    print(
                        f"[Broker Heal] Pending snapshot unavailable for buffered retry "
                        f"{symbol or 'Unknown'} (reason={reason}). Waiting..."
                    )
                elif fail_count % 10 == 0:
                    print(
                        f"[Broker Heal] Pending snapshot still unavailable for buffered retry "
                        f"{symbol or 'Unknown'} (count={fail_count}, waited={fail_waited:.1f}s). Keeping buffered."
                    )
                continue

            if pending_state:
                payload.pop('pending_query_fail_count', None)
                payload.pop('pending_query_fail_since', None)
                payload.pop('warned_query_unavailable', None)
                queued_at = payload.get('queued_at')
                if queued_at is not None:
                    waited = max(0.0, now_ts - float(queued_at))
                    # 超时仅告警，不强行重提，避免真实在途单被重复占资。
                    if waited > max_wait and not payload.get('warned_timeout'):
                        payload['warned_timeout'] = True
                        print(
                            f"[Broker Heal] Buffered retry for {symbol or 'Unknown'} waiting {waited:.1f}s "
                            f"(reason={reason}), still pending on broker."
                        )
                continue

            # 安全策略：不确定模式下默认不释放 BUY 重试，避免在快照不稳定时增加敞口。
            # 例外：已在本地状态记忆中明确看到终态，释放属于“解卡死”，不是“盲目加仓”。
            state_item = self._order_state_memory.get(str(key), {})
            known_terminal = isinstance(state_item, dict) and bool(state_item.get('terminal'))
            if self.is_uncertain_mode() and not known_terminal:
                if not payload.get('warned_uncertain_mode'):
                    payload['warned_uncertain_mode'] = True
                    print(f"[Broker Safety] Buffered retry for {symbol or 'Unknown'} paused in uncertain mode.")
                continue
            payload.pop('warned_uncertain_mode', None)
            self._submit_buffered_rejected_retry(key)
            drained += 1

        return drained

    def has_deferred_orders(self):
        with self._ledger_lock:
            return bool(self._deferred_orders)

    def has_runtime_backlog(self):
        with self._ledger_lock:
            return bool(
                self._deferred_orders
                or self._pending_sells
                or self._active_buys
                or self._buffered_rejected_retries
            )

    def has_pending_order(self, symbol, side=None):
        """
        通用在途订单查询（按 symbol + side）。
        返回值:
        - True: 明确在途
        - False: 明确不在途
        - None: 快照不可用
        """
        if not symbol:
            return False
        side_norm = str(side).upper() if side else ''
        symbol_norm = str(symbol).upper()
        try:
            pending_orders = self._fetch_pending_orders_with_retry(reason="has_pending_order")
        except Exception:
            return None

        for po in pending_orders or []:
            if not isinstance(po, dict):
                continue
            direction = str(po.get('direction', '')).upper()
            if side_norm and direction and direction != side_norm:
                continue
            po_symbol = str(po.get('symbol', '')).upper()
            if not self._symbols_match(po_symbol, symbol_norm):
                continue
            try:
                if float(po.get('size', 0) or 0) <= 0:
                    continue
            except Exception:
                pass
            return True
        return False

    def _pick_deferred_symbol(self):
        with self._ledger_lock:
            for item in self._deferred_orders:
                if not isinstance(item, dict):
                    continue
                kwargs = item.get('kwargs') or {}
                data = kwargs.get('data')
                name = getattr(data, '_name', None)
                if name:
                    return str(name)
        return None

    def pre_strategy_check(self):
        """
        策略执行前的轻量健康检查。
        默认只看“资金输入是否处于退化窗口”。
        子类可覆盖，接入更严格的实盘快照健康检查。
        """
        return not self.is_cash_degraded()

    @staticmethod
    def _snapshot_has_pending_sell(pending_orders):
        for po in pending_orders or []:
            if not isinstance(po, dict):
                continue
            if str(po.get('direction', '')).upper() != 'SELL':
                continue
            try:
                if float(po.get('size', 0) or 0) <= 0:
                    continue
            except Exception:
                return True
            return True
        return False

    def _can_replay_deferred(self, reason="unknown", pending_orders=None, snapshot_error=None):
        # 不确定模式或快照不可用时，禁止新增 BUY 风险。
        if self.is_uncertain_mode():
            return False

        if pending_orders is not None:
            has_pending_sell = self._snapshot_has_pending_sell(pending_orders)
        elif snapshot_error is not None:
            return False
        else:
            has_pending_sell = self._has_pending_sells()

        # 本地 pending-sell 标记是额外保守闸门，防止单次假空快照提前重放。
        with self._ledger_lock:
            if self._pending_sells:
                has_pending_sell = True

        return not has_pending_sell

    def _reconcile_strategy_deferred_virtual_order(self, now_ts):
        """
        主动回收策略层 DEFERRED_VIRTUAL_ID，避免 schedule 低频下仅依赖下一次 run 才解锁。
        要求调用方已持有 _ledger_lock。
        """
        ctx = getattr(self, '_context', None)
        strategy = getattr(ctx, 'strategy_instance', None) if ctx else None
        if strategy is None:
            self._strategy_deferred_empty_since = None
            return 0

        order = getattr(strategy, 'order', None)
        if not order or getattr(order, 'id', None) != "DEFERRED_VIRTUAL_ID":
            self._strategy_deferred_empty_since = None
            return 0

        has_backlog = bool(
            self._deferred_orders
            or self._pending_sells
            or self._active_buys
            or self._buffered_rejected_retries
        )
        if has_backlog:
            self._strategy_deferred_empty_since = None
            return 0

        if self._strategy_deferred_empty_since is None:
            self._strategy_deferred_empty_since = now_ts
            return 0

        grace_cfg = getattr(config, 'BROKER_DEFERRED_CLEAR_GRACE_SECONDS', 5.0)
        try:
            grace = float(grace_cfg)
        except Exception:
            grace = 5.0
        grace = max(0.0, grace)
        waited = max(0.0, now_ts - float(self._strategy_deferred_empty_since))
        if waited < grace:
            return 0

        strategy.order = None
        self._strategy_deferred_empty_since = None
        print(f"[Broker Heal] Cleared stale strategy deferred placeholder after {waited:.1f}s.")
        return 1

    def self_heal(self, reason="heartbeat", force=False):
        """
        轻量自愈入口：
        - 对账 _pending_sells
        - 尝试释放可执行的拒单缓冲重试
        """
        now_ts = time.time()
        min_interval = float(self.SELF_HEAL_MIN_INTERVAL_SECONDS)
        snapshot_min_interval_cfg = getattr(
            config,
            'BROKER_PENDING_SNAPSHOT_MIN_INTERVAL_SECONDS',
            self.PENDING_SNAPSHOT_MIN_INTERVAL_SECONDS
        )
        try:
            snapshot_min_interval = float(snapshot_min_interval_cfg)
        except Exception:
            snapshot_min_interval = float(self.PENDING_SNAPSHOT_MIN_INTERVAL_SECONDS)
        snapshot_min_interval = max(0.0, snapshot_min_interval)

        pending_orders = None
        pending_snapshot_error = None
        should_fetch_snapshot = False
        snapshot_throttled = False
        with self._ledger_lock:
            if not force and now_ts - self._last_self_heal_ts < min_interval:
                return 0
            self._last_self_heal_ts = now_ts

            should_fetch_snapshot = bool(
                force
                or self._deferred_orders
                or self._pending_sells
                or self._active_buys
                or self._buffered_rejected_retries
            )

            if should_fetch_snapshot and not force:
                last_snapshot_ts = float(getattr(self, '_last_pending_snapshot_ts', 0.0) or 0.0)
                if now_ts - last_snapshot_ts < snapshot_min_interval:
                    snapshot_throttled = True

            if should_fetch_snapshot and not snapshot_throttled:
                self._last_pending_snapshot_ts = now_ts

        # 网络快照查询放到锁外，避免把订单回调和主循环一并阻塞。
        if should_fetch_snapshot and not snapshot_throttled:
            try:
                pending_orders = self._fetch_pending_orders_with_retry(reason=f"self_heal:{reason}")
            except Exception as e:
                pending_snapshot_error = e
        elif snapshot_throttled:
            pending_snapshot_error = RuntimeError("pending snapshot throttled")
        else:
            pending_snapshot_error = RuntimeError("pending snapshot skipped: no runtime backlog")

        with self._ledger_lock:
            changed = 0
            changed += self._reconcile_pending_sells_from_broker(
                pending_orders=pending_orders,
                snapshot_error=pending_snapshot_error,
            )
            changed += self._reconcile_active_buys_from_broker(
                pending_orders=pending_orders,
                snapshot_error=pending_snapshot_error,
            )
            changed += self._drain_buffered_rejected_retries(
                reason=reason,
                pending_orders=pending_orders,
                snapshot_error=pending_snapshot_error,
            )
            changed += self._reconcile_strategy_deferred_virtual_order(now_ts)

        should_replay = (
            self.has_deferred_orders()
            and self._can_replay_deferred(
                reason=reason,
                pending_orders=pending_orders,
                snapshot_error=pending_snapshot_error,
            )
        )
        if should_replay:
            replay_min_interval = float(self.DEFERRED_REPLAY_INTERVAL_SECONDS)
            if now_ts - float(getattr(self, '_last_deferred_replay_ts', 0.0)) >= replay_min_interval:
                self._last_deferred_replay_ts = now_ts
                print(f"[Broker Heal] No pending sells. Replaying deferred orders (reason={reason}).")
                try:
                    # 竞态防护：self_heal 预判与实际回放之间，可能有新的 SELL 在途写入。
                    # 这里显式走二次闸门校验，避免“旧快照判定 + 新状态”导致提前回放 BUY。
                    self.process_deferred_orders(assume_sell_cleared=False)
                    changed += 1
                except Exception as e:
                    print(f"[Broker Heal] Warning: deferred replay failed in self_heal: {e}")
        elif self.has_deferred_orders() and self.is_uncertain_mode():
            warn_interval = float(getattr(config, 'BROKER_UNCERTAIN_REPLAY_LOG_INTERVAL_SECONDS', 30.0) or 30.0)
            last_warn_ts = float(getattr(self, '_last_uncertain_replay_skip_log_ts', 0.0) or 0.0)
            if now_ts - last_warn_ts >= max(1.0, warn_interval):
                self._last_uncertain_replay_skip_log_ts = now_ts
                print("[Broker Safety] Deferred replay paused: uncertain mode active.")
        return changed

    def on_sell_filled(self):
        """
        卖单成交后的统一后处理入口（去耦 engine 回调逻辑）。
        """
        print("[Broker] Sell filled hook: syncing balance and validating sell-clear before deferred replay...")
        try:
            self.sync_balance()
            print(f"[Broker] Cash after sync: {self.get_cash():.2f}")
        except Exception as e:
            print(f"[Broker] Warning: sync_balance failed after sell fill: {e}")

        try:
            # 统一通过 self_heal 进行“卖单是否出清”的确认与重放闸门决策。
            self.self_heal(reason="sell_filled", force=True)
        except Exception as e:
            print(f"[Broker] Warning: self_heal failed after sell fill: {e}")

        return None

    def _submit_buffered_rejected_retry(self, source_oid):
        """
        在原拒单进入终态后，执行缓冲的降级重试。
        要求调用方已持有 _ledger_lock。
        """
        key = str(source_oid)
        payload = self._buffered_rejected_retries.get(key)
        if not payload:
            return

        data = payload['data']
        symbol = payload['symbol']
        new_shares = payload['new_shares']
        price = payload['price']
        lot_size = payload['lot_size']
        next_retries = payload['next_retries']
        queued_at = payload.get('queued_at')

        wait_s = 0.0
        if queued_at is not None:
            wait_s = max(0.0, time.time() - float(queued_at))
        print(f"[Broker] 原拒单已终态，执行缓冲重试: {symbol} -> {new_shares} (waited {wait_s:.2f}s)")

        deduct_amount = new_shares * price * self.safety_multiplier
        self._virtual_spent_cash += deduct_amount

        new_proxy = self._finalize_and_submit(data, new_shares, price, lot_size, next_retries)
        if new_proxy:
            self._buffered_rejected_retries.pop(key, None)
            return

        if not new_proxy:
            self._virtual_spent_cash = max(
                0.0,
                getattr(self, '_virtual_spent_cash', 0.0) - deduct_amount
            )
            payload['submit_fail_count'] = int(payload.get('submit_fail_count', 0) or 0) + 1
            payload['last_submit_fail_at'] = time.time()
            print(
                f"[Broker] 缓冲重试发单失败，资金已回退。"
                f"保持在缓冲队列中等待下次自愈重放 (attempt={payload['submit_fail_count']})."
            )

    def _finalize_and_submit(self, data, shares, price, lot_size, retries=0):
        """通用的下单收尾逻辑：取整 + 提交"""
        raw_shares = shares
        if lot_size > 1:
            shares = int(shares // lot_size) * lot_size
        else:
            shares = int(shares)

        # lot取整异常
        if raw_shares > 0 >= shares:
            error_msg = (f"[Broker Warning] {data._name} 订单取整后股数为0！\n"
                         f"原始需求: {raw_shares:.2f} 股\n"
                         f"当前最小交易单位 (LotSize): {lot_size}\n"
                         f"原因: 原始需求不足一手，订单已自动取消。请检查 LOT_SIZE 配置。")

            print(f"\n{'-' * 30}\n{error_msg}\n{'-' * 30}")

            try:
                AlarmManager().push_text(error_msg, level='WARNING')
            except Exception as e:
                print(f"[Alarm Error] 无法发送截断警告: {e}")

            return None

        if shares > 0:
            # 根据是否为重试改变日志标签
            tag = "实盘降级重试" if retries > 0 else "实盘信号"
            log.signal('BUY', data._name, shares, price, tag=tag, dt=self._datetime)

            with self._ledger_lock:
                proxy = self._submit_order(data, shares, 'BUY', price)
                if proxy:
                    final_submitted_shares = self._infer_submitted_shares(proxy, shares)
                    # 注册到活跃买单库，记录当前的参数和重试次数
                    self._active_buys[proxy.id] = {
                        'data': data,
                        'shares': final_submitted_shares,
                        'price': price,
                        'lot_size': lot_size,
                        'retries': retries,
                        'created_at': time.time(),
                    }
            return proxy
        return None

    def _smart_sell(self, data, shares, price, **kwargs):
        """智能卖出：自动注册监控"""
        lot_size = config.LOT_SIZE

        # 获取当前【真实的已结算仓位】
        current_pos = self.get_position(data).size

        # 防止做空。你最多只能卖出现有持仓！(防止在途买单导致超额卖出)
        shares = min(shares, current_pos)

        # 碎股放行逻辑。如果是清仓(或卖出量等于当前持仓)，无视 A股 100手 限制，直接全卖
        if shares >= current_pos > 0:
            shares = current_pos
        else:
            if lot_size > 1:
                shares = int(shares // lot_size) * lot_size
            else:
                shares = int(shares)

        if shares > 0:
            log.signal('SELL', data._name, shares, price, tag="实盘信号", dt=self._datetime)
            with self._ledger_lock:
                proxy = self._submit_order(data, shares, 'SELL', price)
                if proxy:
                    self._pending_sells.add(proxy.id)
            return proxy
        return None

    def _pop_buy_and_refund_virtual_cash(self, oid, proxy):
        """
        从活跃买单表弹出并回退对应虚拟占资。
        要求调用方已持有 _ledger_lock。
        返回 (buy_info, symbol, refund_amount) 或 None。
        """
        buy_info = self._active_buys.pop(oid, None)
        if not buy_info:
            return None

        refund_amount = (
            float(buy_info.get('shares', 0) or 0)
            * float(buy_info.get('price', 0) or 0)
            * self.safety_multiplier
        )
        symbol = (
            getattr(buy_info.get('data'), '_name', None)
            or getattr(getattr(proxy, 'data', None), '_name', 'Unknown')
        )
        self._virtual_spent_cash = max(
            0.0,
            float(getattr(self, '_virtual_spent_cash', 0.0) or 0.0) - refund_amount
        )
        return buy_info, symbol, refund_amount

    def on_order_status(self, proxy: BaseOrderProxy):
        """由 Engine 回调，自动维护在途单状态与降级重试"""
        oid = proxy.id
        proxy_symbol = self._resolve_proxy_symbol(proxy)
        drain_reason = None

        # 整个回调必须排队，防止抢占主线程刚发出的订单
        with self._ledger_lock:
            # 先记录状态快照，供后续快照失败时做安全回退判定
            self._remember_order_state(proxy)
            # ==========================================
            # 1. 买单异步降级逻辑 (Buy Order Downgrade)
            # ==========================================
            if proxy.is_buy():
                if proxy.is_completed():
                    # 买单终态(Filled): 物理现金已结算，必须回退本地虚拟预扣，避免双重扣减可用资金
                    released = self._pop_buy_and_refund_virtual_cash(oid, proxy)
                    if released:
                        _, symbol, refund_amount = released
                        print(f"[Broker] Buy {symbol} filled. Released virtual cash: {refund_amount:.2f}")

                elif proxy.is_canceled():
                    # 撤单防御：精准回退被冻结的虚拟预扣资金（不触发降级重试）
                    released = self._pop_buy_and_refund_virtual_cash(oid, proxy)
                    if released:
                        _, symbol, refund_amount = released
                        print(f"[Broker] Buy {symbol} canceled. Refunded virtual cash: {refund_amount:.2f}")
                    # AGENT_NOTE (accepted risk):
                    # Maintainer-approved behavior: cancel terminal callback may release buffered BUY retry
                    # even during uncertain mode, prioritizing queue liveness/self-heal over strict consistency.
                    # Do not gate this call by uncertain mode unless trading policy is changed explicitly.
                    self._submit_buffered_rejected_retry(oid)

                elif proxy.is_rejected():
                    released = self._pop_buy_and_refund_virtual_cash(oid, proxy)
                    if released:
                        buy_info, _, _ = released
                        retries = int(buy_info.get('retries', 0) or 0)
                        max_retries = 3  # 默认允许尝试降级 3 次

                        # B. 检查是否还有重试机会
                        if retries < max_retries:
                            lot_size = int(buy_info.get('lot_size', 1) or 1)
                            data = buy_info.get('data')
                            symbol = getattr(data, '_name', None) or getattr(getattr(proxy, 'data', None), '_name', 'Unknown')
                            price = float(buy_info.get('price', 0) or 0)

                            # 优先按当前可用资金重算；失败时再走逐手降级兜底。
                            old_shares = int(buy_info.get('shares', 0) or 0)
                            recalculated = self._recalc_rejected_buy_shares(old_shares, price, lot_size)
                            if recalculated > 0:
                                new_shares = recalculated
                                downgrade_reason = "资金重算"
                            else:
                                new_shares = old_shares - lot_size
                                downgrade_reason = "逐手降级"

                            print(f"[Broker] Buy {symbol} rejected. Trigger downgrade {retries + 1}/{max_retries}...")
                            print(f"   => {symbol} 尝试数量: {old_shares} -> {new_shares} ({downgrade_reason})")

                            if new_shares > 0:
                                # 回调路径只做缓冲，释放由锁外对账完成，避免持锁网络查询阻塞主循环。
                                key = str(oid)
                                if key not in self._buffered_rejected_retries:
                                    self._buffered_rejected_retries[key] = {
                                        'data': data,
                                        'symbol': symbol,
                                        'new_shares': new_shares,
                                        'price': price,
                                        'lot_size': lot_size,
                                        'next_retries': retries + 1,
                                        'queued_at': time.time(),
                                    }
                                    print(f"[Broker] {symbol} buffered downgrade retry queued, waiting release.")
                                else:
                                    print(f"[Broker] {symbol} buffered retry already exists, duplicate rejected callback ignored.")
                            else:
                                print(f"[Broker] Downgrade stopped: {symbol} shares reduced to 0.")
                    else:
                        # 兜底：某些柜台会重复推 Rejected/Inactive，但不再推 Canceled。
                        # 这里仅记录等待，实际释放由锁外快照对账统一执行。
                        key = str(oid)
                        if key in self._buffered_rejected_retries:
                            buffered_symbol = self._buffered_rejected_retries.get(key, {}).get('symbol') or proxy_symbol
                            print(f"[Broker] {buffered_symbol} buffered retry waiting lock-free reconciliation.")
                drain_reason = f"buy_callback:{proxy_symbol}"
            elif proxy.is_sell():
                # ==========================================
                # 2. 卖单在途维护逻辑 (Sell Order Pending)
                # ==========================================
                if proxy.is_completed():
                    self._pending_sells.discard(oid)

                elif proxy.is_canceled() or proxy.is_rejected():
                    self._pending_sells.discard(oid)
                    if self._deferred_orders:
                        print(
                            f"[Broker] WARNING: Sell {proxy_symbol} failed. Cancelling {len(self._deferred_orders)} deferred buy orders.")
                        self._deferred_orders.clear()
                elif proxy.is_pending():
                    self._pending_sells.add(oid)
                drain_reason = f"sell_callback:{proxy_symbol}"

        if not drain_reason:
            return

        with self._ledger_lock:
            has_buffered = bool(self._buffered_rejected_retries)
        if not has_buffered:
            return

        pending_orders = None
        pending_snapshot_error = None
        try:
            pending_orders = self._fetch_pending_orders_with_retry(reason=f"order_callback_drain:{proxy_symbol}")
        except Exception as e:
            pending_snapshot_error = e

        with self._ledger_lock:
            self._drain_buffered_rejected_retries(
                reason=drain_reason,
                pending_orders=pending_orders,
                snapshot_error=pending_snapshot_error,
            )

    def get_expected_size(self, data):
        """获取包含在途订单的【预期仓位】，防止底层下单方法出现认知撕裂"""
        pos_size = self.get_position(data).size
        try:
            pending_orders = self._fetch_pending_orders_with_retry(reason="get_expected_size")
            for po in pending_orders:
                sym = str(po['symbol']).upper()
                data_name = data._name.upper()
                if self._symbols_match(sym, data_name):
                    if po['direction'] == 'BUY': pos_size += po['size']
                    if po['direction'] == 'SELL': pos_size -= po['size']
        except Exception as e:
            print(f"[Broker] 获取预期仓位异常: {e}")
        return pos_size

    def process_deferred_orders(self, assume_sell_cleared=False):
        """资金回笼触发重试"""
        # 无人值守风控约束：
        # 不确定模式下允许恢复/对账，但禁止通过 deferred 重放新增 BUY 风险。
        if self.is_uncertain_mode():
            print("[Broker Safety] Deferred replay skipped due to uncertain mode.")
            return

        # 只有在明确“卖单已出清”时才允许回放 deferred 买单。
        if not assume_sell_cleared:
            if not self._can_replay_deferred(reason="process_deferred_orders"):
                print("[Broker Safety] Deferred replay skipped: pending sell not cleared.")
                return

        with self._ledger_lock:
            if not self._deferred_orders:
                self._drain_buffered_rejected_retries(reason="deferred_empty")
                return
            print(f"[Broker] 资金回笼，重试 {len(self._deferred_orders)} 个延迟单...")
            retry_list = self._deferred_orders[:]
            self._deferred_orders.clear()

        # 这里的 item 结构现在是通用的 {'func': func, 'kwargs': kwargs}
        failed_items = []
        for item in retry_list:
            func = item.get('func')
            kwargs = item.get('kwargs', {})
            if func:
                try:
                    func(**kwargs)
                except Exception as e:
                    item['fail_count'] = int(item.get('fail_count', 0) or 0) + 1
                    failed_items.append(item)
                    print(
                        f"[Broker] WARNING: Deferred replay failed ({func.__name__ if hasattr(func, '__name__') else 'unknown'}) "
                        f"attempt={item['fail_count']}. Error: {e}"
                    )

        if failed_items:
            with self._ledger_lock:
                self._deferred_orders.extend(failed_items)
            print(f"[Broker] Deferred replay recovered: re-queued {len(failed_items)} failed item(s).")

        with self._ledger_lock:
            self._drain_buffered_rejected_retries(reason="deferred_replayed")

    def _add_deferred(self, func, kwargs):        # 捕获闭包参数
        with self._ledger_lock:
            self._deferred_orders.append({
                'func': func,
                'kwargs': kwargs,
                'created_at': time.time(),
                'fail_count': 0,
            })

    def get_cash(self):
        """公有接口：获取资金"""
        # 先锁外查真实资金，再锁内扣虚拟占资，避免长耗时 I/O 把状态机锁住。
        real_cash = self._fetch_real_cash()
        with self._ledger_lock:
            real_cash -= getattr(self, '_virtual_spent_cash', 0.0)
        if real_cash < 0:
            real_cash = 0.0

        if self._cash_override is not None:
            return min(real_cash, self._cash_override)
        return real_cash

    def get_rebalance_cash(self):
        """
        策略层用于“调仓计划总资金”的现金口径。
        默认与 get_cash 一致，子类可覆盖为更保守或更贴合券商语义的实现。
        """
        return self.get_cash()

    def _has_pending_sells(self):
        if len(self._pending_sells) > 0:
            return True

        # 回调丢失兜底：直接询问柜台是否仍存在卖单在途
        try:
            pending_orders = self._fetch_pending_orders_with_retry(reason="has_pending_sells")
        except Exception:
            # 保守优先：在途快照异常时视为“仍可能有卖单在途”。
            return True

        for po in pending_orders or []:
            if not isinstance(po, dict):
                continue
            direction = str(po.get('direction', '')).upper()
            if direction == 'SELL':
                try:
                    if float(po.get('size', 0) or 0) > 0:
                        return True
                except Exception:
                    return True
        return False

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

    def lock_for_risk(self, symbol: str):
        """风控专用：锁定标的，禁止买入"""
        self._risk_locked_symbols.add(symbol)

    def unlock_for_risk(self, symbol: str):
        """风控专用：解除标的锁定"""
        self._risk_locked_symbols.discard(symbol)

    def set_datetime(self, dt):
        """设置当前时间，并进行跨周期检查"""
        # 检查时间是否推进 (进入了新的 Bar/Day，跨周期)
        if self._datetime and dt > self._datetime:
            # 不要因为 tick/bar 的更新就清理订单（会误杀 HFT 买单）。
            # 只有在以下两种情况才清理：
            # 1. 跨日了 (New Trading Day) -> 昨天的单子肯定是死单
            # 2. 两次心跳间隔太久 (例如 > 10分钟) -> 说明程序可能断线重启过，状态不可信

            is_new_day = dt.date() > self._datetime.date()

            # 仅跨日清空虚拟占资，避免日内 bar 推进误释放占资保护。
            if is_new_day:
                self._virtual_spent_cash = 0.0

            # 计算时间差 (秒)
            time_delta = (dt - self._datetime).total_seconds()
            is_long_gap = time_delta > 600  # 10分钟无心跳视为异常

            if is_new_day or is_long_gap:
                has_stale_state = bool(
                    self._deferred_orders
                    or self._pending_sells
                    or self._active_buys
                    or self._buffered_rejected_retries
                    or self._virtual_spent_cash > 0
                )
                if has_stale_state:
                    print(f"[Broker] {'New Day' if is_new_day else 'Long Gap'} detected. "
                          f"Resetting stale broker state.")
                    self._reset_stale_state(new_dt=dt)

            # 注意：对于同一个交易日内的正常 Bar 更新（比如 10:00 -> 10:01），
            # 我们保留 deferred_orders。因为 process_deferred_orders 会在资金到位时
            # 重新计算 target_percent，所以即使保留下来，也会用最新的价格重新下单，是安全的。

        self._datetime = dt
        try:
            # 每次时间推进都尝试做一次轻量自愈，降低“依赖单点回调”的风险。
            self.self_heal(reason="set_datetime")
        except Exception as e:
            print(f"[Broker Heal] Warning: self_heal failed at set_datetime: {e}")

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
        if hasattr(self, '_buffered_rejected_retries'):
            self._buffered_rejected_retries.clear()
        if hasattr(self, '_order_state_memory'):
            self._order_state_memory.clear()
        if hasattr(self, '_active_buy_empty_snapshots'):
            self._active_buy_empty_snapshots = 0
        if hasattr(self, '_pending_sell_empty_snapshots'):
            self._pending_sell_empty_snapshots = 0
        if hasattr(self, '_pending_sell_empty_since'):
            self._pending_sell_empty_since = None
        if hasattr(self, '_active_buy_empty_since'):
            self._active_buy_empty_since = None
        if hasattr(self, '_strategy_deferred_empty_since'):
            self._strategy_deferred_empty_since = None
        if hasattr(self, '_last_deferred_replay_ts'):
            self._last_deferred_replay_ts = 0.0
        if hasattr(self, '_pending_snapshot_fail_count'):
            self._pending_snapshot_fail_count = 0
        if hasattr(self, '_pending_snapshot_fail_since'):
            self._pending_snapshot_fail_since = None
        if hasattr(self, '_uncertain_mode_until'):
            self._uncertain_mode_until = 0.0
        if hasattr(self, '_last_uncertain_replay_skip_log_ts'):
            self._last_uncertain_replay_skip_log_ts = 0.0
        if hasattr(self, '_cash_degraded_until'):
            self._cash_degraded_until = 0.0
        if hasattr(self, '_cash_degraded_reason'):
            self._cash_degraded_reason = ""

        # 4. 清理虚拟占资，避免长中断后出现幽灵冻结资金
        self._virtual_spent_cash = 0.0
        print("  >>> Broker state reset completed.")

    def force_reset_state(self):
        """
        外部强制重置接口。
        供 Engine 在捕获到 CRITICAL 异常时调用，进行兜底恢复。
        """
        print("[Broker] Force reset state requested by Engine...")
        self._deferred_orders.clear()
        self._pending_sells.clear()

        # 补丁：彻底清空买单追踪器和虚拟账本占资，防止幽灵占资残留
        if hasattr(self, '_active_buys'):
            self._active_buys.clear()
        if hasattr(self, '_buffered_rejected_retries'):
            self._buffered_rejected_retries.clear()
        if hasattr(self, '_order_state_memory'):
            self._order_state_memory.clear()
        if hasattr(self, '_active_buy_empty_snapshots'):
            self._active_buy_empty_snapshots = 0
        if hasattr(self, '_pending_sell_empty_snapshots'):
            self._pending_sell_empty_snapshots = 0
        if hasattr(self, '_pending_sell_empty_since'):
            self._pending_sell_empty_since = None
        if hasattr(self, '_active_buy_empty_since'):
            self._active_buy_empty_since = None
        if hasattr(self, '_strategy_deferred_empty_since'):
            self._strategy_deferred_empty_since = None
        if hasattr(self, '_last_deferred_replay_ts'):
            self._last_deferred_replay_ts = 0.0
        if hasattr(self, '_pending_snapshot_fail_count'):
            self._pending_snapshot_fail_count = 0
        if hasattr(self, '_pending_snapshot_fail_since'):
            self._pending_snapshot_fail_since = None
        if hasattr(self, '_uncertain_mode_until'):
            self._uncertain_mode_until = 0.0
        if hasattr(self, '_last_uncertain_replay_skip_log_ts'):
            self._last_uncertain_replay_skip_log_ts = 0.0
        if hasattr(self, '_cash_degraded_until'):
            self._cash_degraded_until = 0.0
        if hasattr(self, '_cash_degraded_reason'):
            self._cash_degraded_reason = ""
        self._virtual_spent_cash = 0.0

        try:
            self.sync_balance()
            print(f"  >>> Balance re-synced: {self.get_cash():.2f}")
        except Exception as e:
            print(f"  >>> Warning: Failed to sync balance during reset: {e}")
        print("[Broker] Force reset state completed.")
