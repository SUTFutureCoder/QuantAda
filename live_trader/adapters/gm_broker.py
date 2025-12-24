import pandas as pd

from .base_broker import BaseLiveBroker, BaseLiveDataProvider, BaseOrderProxy

try:
    from gm.api import history, order_target_percent, order_volume, current, get_cash, OrderType_Market, MODE_LIVE, MODE_BACKTEST, \
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

    def order_target_percent(self, data, target, **kwargs) -> GmOrderProxy | None:
        """
        下单，并返回一个包装好的 GmOrderProxy 实例。
        替代原生的 order_target_percent 以支持 lot_size 和现金向下取整。
        """
        if order_volume is None: raise ImportError("'gm' is required.")

        symbol = data._name
        lot_size = kwargs.get('lot_size', 100)  # 默认为A股 100股

        # 1. 获取最新价格
        # 使用 gm.api.current 获取实时 Tick 数据
        tick_list = current(symbols=symbol)
        if not tick_list:
            print(f"[Live Trade Error] Cannot get current price for {symbol}")
            return None
        price = tick_list[0]['price']

        if price <= 0:
            print(f"[Live Trade Error] Invalid price {price} for {symbol}")
            return None

        # 2. 获取当前持仓量
        # 注意：需要从 account 对象中获取，而不是 self.get_position(data) 的缓存，确保实时性
        # 或者直接使用我们封装的 get_position
        pos_obj = self.get_position(data)
        current_pos_size = pos_obj.size

        # 3. 计算账户总资产 (现金 + 持仓市值)
        # 掘金 account 对象通常包含 total_assets 或 net_assets，但为了稳健，我们手动累加或使用 available cash + pos value
        # 这里为了计算 target amount，我们需要 Total Portfolio Value
        # 简化计算：Total = Available Cash + Sum(Position Value)

        acct = self._context.account()
        cash = acct.cash.available

        # 获取所有持仓计算市值（为了准确计算 target 对应的金额）
        # 也可以尝试直接使用 acct.cash.nav (净值)，如果存在
        if hasattr(acct.cash, 'nav'):
            portfolio_value = acct.cash.nav
        else:
            # 回退：手动计算
            positions = acct.positions()
            market_value = sum([p.volume * p.price for p in positions])  # p.price 是最新价
            portfolio_value = cash + market_value

        # 4. 计算目标市值和目标股数
        target_value = portfolio_value * target
        expected_shares = target_value / price

        # 5. 计算差额
        delta_shares = expected_shares - current_pos_size

        final_volume = 0
        side = 0  # 1=Buy, 2=Sell

        if delta_shares > 0:  # Buy
            # 现金约束
            max_buy_by_cash = cash / price
            shares_to_buy = min(delta_shares, max_buy_by_cash)

            # Lot Size 整手
            if lot_size > 1:
                shares_to_buy = int(shares_to_buy // lot_size) * lot_size
            else:
                shares_to_buy = int(shares_to_buy)

            if shares_to_buy > 0:
                final_volume = shares_to_buy
                side = OrderSide_Buy

        elif delta_shares < 0:  # Sell
            shares_to_sell = abs(delta_shares)

            if target == 0.0:
                # 清仓逻辑：直接卖出所有持仓，包括零股
                final_volume = current_pos_size
                side = OrderSide_Sell
            else:
                # 调仓逻辑：通常也按整手卖出
                if lot_size > 1:
                    shares_to_sell = int(shares_to_sell // lot_size) * lot_size
                else:
                    shares_to_sell = int(shares_to_sell)

                if shares_to_sell > 0:
                    final_volume = shares_to_sell
                    side = OrderSide_Sell

        # 6. 下单
        if final_volume > 0 and side != 0:
            print(
                f"[Live Trade] Placing order: {symbol} {'BUY' if side == OrderSide_Buy else 'SELL'} {final_volume} (Target% {target:.2f})")
            # position_side=1 (多仓)，order_type=Market
            platform_order_list = order_volume(symbol=symbol, volume=final_volume, side=side,
                                               order_type=OrderType_Market, position_effect=1)
            return GmOrderProxy(platform_order_list[-1], self.is_live) if platform_order_list else None

        return None

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
