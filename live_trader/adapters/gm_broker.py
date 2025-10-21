import pandas as pd

from .base_broker import BaseLiveBroker, BaseLiveDataProvider, BaseOrderProxy

try:
    from gm.api import history, order_target_percent, get_cash, OrderType_Market, MODE_LIVE, MODE_BACKTEST, \
        OrderStatus_New, OrderStatus_PartiallyFilled, OrderStatus_Filled, \
        OrderStatus_Canceled, OrderStatus_Rejected, OrderStatus_PendingNew, \
        OrderSide_Buy, OrderSide_Sell
except ImportError:
    print("Warning: 'gm' module not found. GmAdapter will not be available.")
    history = order_target_percent = get_cash = OrderType_Market = MODE_BACKTEST = None


class GmOrderProxy(BaseOrderProxy):
    """掘金平台的订单代理具体实现"""

    def __init__(self, platform_order):
        self.platform_order = platform_order

    # 因为回测框架不负责实盘的回测，在实盘环境下仅触发信号，因此暂且放行OrderStatus_PendingNew挂单状态
    def is_completed(self) -> bool: return self.platform_order.status == OrderStatus_Filled \
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


class GmDataProvider(BaseLiveDataProvider):
    """掘金平台的数据提供者实现"""

    def get_history(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        if history is None: raise ImportError("'gm' module is required for GmDataProvider.")
        df = history(symbol=symbol, frequency='1d', start_time=start_date, end_time=end_date,
                     fields='open,high,low,close,volume,eob', df=True)
        if df.empty: return df
        df.rename(columns={'eob': 'datetime'}, inplace=True)
        df.set_index('datetime', inplace=True)
        return df


class GmBrokerAdapter(BaseLiveBroker):
    """掘金平台的交易执行器实现"""

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

    def _fetch_real_cash(self) -> float:
        if get_cash is None: raise ImportError("'gm' module is required for GmBrokerAdapter.")
        return get_cash().available

    def order_target_percent(self, data, target) -> GmOrderProxy:
        """
        下单，并返回一个包装好的 GmOrderProxy 实例。
        """
        if order_target_percent is None: raise ImportError("'gm' is required.")
        symbol = data._name
        print(f"[Live Trade] Placing order: target {target * 100:.2f}% for {symbol}")
        # 1. 调用真实API下单，获取平台原生的订单对象
        platform_order_list = order_target_percent(symbol=symbol, percent=target, order_type=OrderType_Market,
                                                   position_side=1)
        # 2. 将原生订单对象包装成我们自己的代理并返回
        return GmOrderProxy(platform_order_list[-1]) if platform_order_list else None

    def get_position(self, data):
        class Pos:
            size = 0

        if not hasattr(self._context, 'account'):
            print("Warning: context object in GmBrokerAdapter is not valid or missing 'account' attribute.")
            return Pos
        positions = self._context.account().positions()
        for p in positions:
            if p.symbol == data._name:
                Pos.size = p.volume
                return Pos
        return Pos
