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

    def __init__(self, platform_order, is_live: bool):
        self.platform_order = platform_order
        self.is_live = is_live  # 保存模式

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


class GmDataProvider(BaseLiveDataProvider):
    """掘金平台的数据提供者实现"""

    def _map_gm_frequency(self, timeframe: str, compression: int) -> str:
        """将Backtrader时间框架映射到掘金的frequency参数"""
        if timeframe == 'Days':
            if compression == 1:
                return '1d'
            else:
                #
                print(f"Warning: GM Provider: {compression} Days timeframe not directly supported. Using '1d'.")
                return '1d'

        if timeframe == 'Minutes':
            #
            supported_compressions = [1, 5, 15, 30, 60]
            if compression in supported_compressions:
                return f'{compression}m'
            else:
                print(f"Warning: GM Provider: Unsupported compression {compression} for Minutes. Defaulting to '60m'.")
                return '60m'

        if timeframe == 'Weeks':
            return '1w'
        if timeframe == 'Months':
            return '1M'

        print(f"Warning: GM Provider: Unsupported timeframe {timeframe}. Defaulting to '1d'.")
        return '1d'

    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:
        if history is None:
            raise ImportError("'gm' module is required for GmDataProvider.")

        frequency = self._map_gm_frequency(timeframe, compression)

        df = history(symbol=symbol, frequency=frequency, start_time=start_date, end_time=end_date,
                     fields='open,high,low,close,volume,eob', df=True)
        if df.empty: return df
        df.rename(columns={'eob': 'datetime'}, inplace=True)
        df.set_index('datetime', inplace=True)
        return df


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
        return GmOrderProxy(platform_order_list[-1], self.is_live) if platform_order_list else None

    def get_position(self, data):
        class Pos:
            size = 0
            # 持仓均价
            price = 0.0

        if not hasattr(self._context, 'account'):
            print("Warning: context object in GmBrokerAdapter is not valid or missing 'account' attribute.")
            return Pos()

        positions = self._context.account().positions()
        for p in positions:
            if p.symbol == data._name:
                pos_obj = Pos()
                pos_obj.size = p.volume
                pos_obj.price = p.vwap
                return pos_obj
        return Pos()
