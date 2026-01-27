import datetime

import pandas as pd

from data_providers.gm_provider import GmDataProvider as UnifiedGmDataProvider
from .base_broker import BaseLiveBroker, BaseOrderProxy

try:
    from gm.api import order_target_percent, order_volume, current, get_cash, OrderType_Market, MODE_LIVE, MODE_BACKTEST, \
        OrderStatus_New, OrderStatus_PartiallyFilled, OrderStatus_Filled, \
        OrderStatus_Canceled, OrderStatus_Rejected, OrderStatus_PendingNew, \
        OrderSide_Buy, OrderSide_Sell
except ImportError:
    print("Warning: 'gm' module not found. GmAdapter will not be available.")
    order_target_percent = get_cash = OrderType_Market = MODE_BACKTEST = None


class GmOrderProxy(BaseOrderProxy):
    """掘金平台的订单代理具体实现"""

    def __init__(self, order, is_live, data=None):
        self.platform_order = order
        self.is_live = is_live
        self.data = data

    @property
    def id(self):
        return self.platform_order.cl_ord_id

    @property
    def status(self):
        return self.platform_order.status

    @property
    def executed(self):
        """
        构造一个临时对象，模拟 Backtrader order.executed 的接口
        供策略层读取 size, price, value, comm
        """

        # 定义一个简单的类来承载数据
        class ExecutedStats:
            def __init__(self, gm_order):
                # 1. 成交数量
                self.size = gm_order.filled_volume

                # 2. 成交均价 (filled_vwap 是掘金的成交均价字段)
                self.price = gm_order.filled_vwap

                # 3. 成交金额 (Cost/Value)
                # 掘金通常有 filled_amount，如果没有则用 数量*均价 计算
                if hasattr(gm_order, 'filled_amount'):
                    self.value = gm_order.filled_amount
                else:
                    self.value = gm_order.filled_volume * gm_order.filled_vwap

                # 4. 手续费
                self.comm = getattr(gm_order, 'commission', 0.0)

        return ExecutedStats(self.platform_order)

    # 根据模式动态判断
    def is_completed(self) -> bool:
        if self.is_live:
            # 实盘模式：必须是最终成交
            return self.platform_order.status == OrderStatus_Filled
        else:
            # 回测模式：放行 PendingNew (兼容掘金回测)
            # 因为回测框架不负责实盘的回测，且掘金的下单是异步过程无法实时获取订单状态，因此修改is_completed检查的常量。
            # 在实盘环境下仅触发信号，因此暂且放行OrderStatus_PendingNew挂单状态
            return self.platform_order.status == OrderStatus_Filled \
                or self.platform_order.status == OrderStatus_PendingNew

    def is_canceled(self) -> bool: return self.platform_order.status == OrderStatus_Canceled

    def is_rejected(self) -> bool: return self.platform_order.status == OrderStatus_Rejected

    def is_pending(self) -> bool:
        terminal_states = [OrderStatus_Filled, OrderStatus_Canceled, OrderStatus_Rejected, OrderStatus_PendingNew]
        return self.platform_order.status not in terminal_states

    def is_accepted(self) -> bool:
        return self.platform_order.status not in [OrderStatus_New, OrderStatus_Rejected]

    def is_buy(self) -> bool:
        return hasattr(self.platform_order, 'side') and self.platform_order.side == OrderSide_Buy

    def is_sell(self) -> bool:
        return hasattr(self.platform_order, 'side') and self.platform_order.side == OrderSide_Sell

class GmDataProvider(UnifiedGmDataProvider):
    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:
        # 直接透传调用父类的 get_data
        return self.get_data(symbol, start_date, end_date, timeframe, compression)

class GmBrokerAdapter(BaseLiveBroker):
    """掘金平台的交易执行器实现"""

    def __init__(self, context, cash_override=None, commission_override=None):
        super().__init__(context, cash_override, commission_override)
        self.is_live = self.is_live_mode(context)  # 保存当前是否为实盘

    @staticmethod
    def is_live_mode(context) -> bool:
        """掘金平台实盘模式的具体判断逻辑"""
        if MODE_LIVE is None: return False
        return hasattr(context, 'mode') and context.mode == MODE_LIVE

    @staticmethod
    def extract_run_config(context) -> dict:
        """从掘金的context中提取回测参数，并转换为框架的标准配置格式"""
        if MODE_BACKTEST is not None and hasattr(context, 'mode') and context.mode == MODE_BACKTEST:
            print("[GmAdapter] Backtest mode detected. Extracting parameters from context.")
            config = {
                'start_date': context.backtest_start_time,
                'end_date': context.backtest_end_time,
                'cash': context.account().cash.available,
            }
            return config
        return {}

    # 1. 查钱
    def _fetch_real_cash(self):
        return get_cash().available

    # 2. 查持仓
    def get_position(self, data):
        class Pos:
            size = 0; price = 0.0

        if hasattr(self._context, 'account'):
            for p in self._context.account().positions():
                if p.symbol == data._name:
                    o = Pos();
                    o.size = p.volume;
                    o.price = p.vwap;
                    return o
        return Pos()

    # 3. 查价
    def _get_current_price(self, data):
        ticks = current(symbols=data._name)
        return ticks[0]['price'] if ticks else 0.0

    # 4. 发单
    def _submit_order(self, data, volume, side, price):
        gm_side = OrderSide_Buy if side == 'BUY' else OrderSide_Sell

        upper_limit, lower_limit = self._get_upper_lower_limit(data, price)
        actual_price = upper_limit if side == 'BUY' else lower_limit

        # 资金预检查，防止资金不足
        if side == 'BUY':
            available_cash = self._fetch_real_cash()
            # 预估冻结资金 (加 0.05% 缓冲)
            estimated_cost = volume * actual_price * 1.0005

            if estimated_cost > available_cash:
                # 资金不够覆盖涨停价冻结，自动降仓
                old_volume = volume
                volume = int(available_cash / (actual_price * 1.0005) // 100) * 100

                if volume < 100:
                    print(
                        f"[GmBroker] Skip Buy {data._name}: Cash {available_cash:.2f} < LimitCost {estimated_cost:.2f}")
                    return None

                print(f"[GmBroker] Auto-Downsize {data._name}: {old_volume} -> {volume} (Reason: LimitPrice Freeze)")

        if volume <= 0: return None

        try:
            # 1=Open, 2=Close
            effect = 1 if side == 'BUY' else 2
            ords = order_volume(
                symbol=data._name, volume=volume, side=gm_side,
                order_type=OrderType_Market, position_effect=effect, price=actual_price
            )
            return GmOrderProxy(ords[-1], self.is_live, data=data) if ords else None
        except Exception as e:
            print(f"[GM Error] {e}")
            return None

    # 计算涨停和跌停保护价
    def _get_upper_lower_limit(self, data, price):
        # 获取前一天收盘价用于市价单保护
        current_dt = self._datetime
        lastday_dt = data.p.dataname.asof(current_dt - datetime.timedelta(days=1))
        pre_close = 0.0
        if not lastday_dt.empty:
            pre_close = lastday_dt.close

        # 确定计算基准价：优先昨收，其次开盘，最后现价
        base_price_for_calc = pre_close if pre_close > 0 else price

        # 估算/确定保护价
        limit_ratio = 0.20 if data._name.startswith(('SHSE.688', 'SZSE.300')) else 0.10

        upper_limit = base_price_for_calc * (1 + limit_ratio - 0.015)
        lower_limit = base_price_for_calc * (1 - limit_ratio + 0.015)

        return upper_limit, lower_limit

