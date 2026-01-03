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

    def __init__(self, platform_order, is_live: bool, data=None):
        self.platform_order = platform_order
        self.is_live = is_live  # 保存模式
        self.data = data  # 存储对应的 Backtrader 数据源对象

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
        tick_list = current(symbols=symbol)
        if not tick_list:
            print(f"[Live Trade Error] Cannot get current price for {symbol}")
            return None
        tick = tick_list[0]
        price = tick['price']

        # 获取涨跌停价用于市价单保护
        upper_limit = tick.get('upper_limit', 0.0)
        lower_limit = tick.get('lower_limit', 0.0)
        pre_close = tick.get('pre_close', 0.0)
        open_price = tick.get('open', 0.0)

        if price <= 0:
            print(f"[Live Trade Error] Invalid price {price} for {symbol}")
            return None

        # 2. 获取当前持仓量
        pos_obj = self.get_position(data)
        current_pos_size = pos_obj.size

        # 3. 计算账户总资产 (现金 + 持仓市值)
        acct = self._context.account()
        cash = acct.cash.available

        if hasattr(acct.cash, 'nav'):
            portfolio_value = acct.cash.nav
        else:
            positions = acct.positions()
            market_value = sum([p.volume * p.price for p in positions])
            portfolio_value = cash + market_value

        # 4. 计算目标市值和目标股数
        target_value = portfolio_value * target
        expected_shares = target_value / price

        # 5. 计算差额
        delta_shares = expected_shares - current_pos_size

        final_volume = 0
        side = 0  # 1=Buy, 2=Sell
        # 定义开平仓标志 1=Open, 2=Close
        position_effect = 1

        # --- 提前计算保护价变量，供后续逻辑使用 ---
        # 确定计算基准价：优先昨收，其次开盘，最后现价
        base_price_for_calc = pre_close if pre_close > 0 else (open_price if open_price > 0 else price)

        # 估算/确定保护价
        if upper_limit > 0:
            buy_protection_price = upper_limit
        else:
            # 只有当拿不到 upper_limit 时才估算，使用 1.1 (10%) 比较稳妥
            buy_protection_price = base_price_for_calc * 1.1

        if lower_limit > 0:
            sell_protection_price = lower_limit
        else:
            # 只有当拿不到 lower_limit 时才估算，使用 0.9 (10%) 比较稳妥
            sell_protection_price = base_price_for_calc * 0.9

        if delta_shares > 0:  # Buy
            # 现金约束
            # 使用买入保护价(涨停价)计算最大可买股数
            # 如果按当前价计算，市价单冻结涨停价资金时会不足
            # 增加 0.0002 的手续费缓冲
            safe_price_for_calculation = buy_protection_price * 1.0005
            max_buy_by_cash = cash / safe_price_for_calculation

            shares_to_buy = min(delta_shares, max_buy_by_cash)

            if lot_size > 1:
                shares_to_buy = int(shares_to_buy // lot_size) * lot_size
            else:
                shares_to_buy = int(shares_to_buy)

            if shares_to_buy > 0:
                final_volume = shares_to_buy
                side = OrderSide_Buy
                position_effect = 1  # 买入开仓

        elif delta_shares < 0:  # Sell
            shares_to_sell = abs(delta_shares)

            if target == 0.0:
                final_volume = current_pos_size
                side = OrderSide_Sell
            else:
                if lot_size > 1:
                    shares_to_sell = int(shares_to_sell // lot_size) * lot_size
                else:
                    shares_to_sell = int(shares_to_sell)

                if shares_to_sell > 0:
                    final_volume = shares_to_sell
                    # 如果要卖出的量 > 可用量
                    if final_volume > pos_obj.available:
                        print(f"[Warn] T+1 Limit: Try to sell {final_volume}, but only {pos_obj.available} available.")
                        final_volume = pos_obj.available  # 强制降级为卖出可用部分
                    side = OrderSide_Sell

            # 卖出平仓
            position_effect = 2

        # 6. 下单
        if final_volume > 0 and side != 0:
            # 根据方向选择之前算好的保护价
            actual_protection_price = buy_protection_price if side == OrderSide_Buy else sell_protection_price

            print(
                f"[Live Trade] Placing order: {symbol} {'BUY' if side == OrderSide_Buy else 'SELL'} {final_volume} (Target% {target:.2f})")

            try:
                # 动态传入 position_effect，并增加 position_side=1 (多头仓位) 显式声明
                platform_order_list = order_volume(
                    symbol=symbol,
                    volume=final_volume,
                    side=side,
                    order_type=OrderType_Market,
                    position_effect=position_effect,  # 1=Open, 2=Close
                    price=actual_protection_price
                )
                return GmOrderProxy(platform_order_list[-1], self.is_live, data=data) if platform_order_list else None

            except Exception as e:
                # 捕获 API 报错 (如 1018 或 无效标的)，防止炸毁整个回测
                print(f"[Live Trade Error] GM Order Failed: {e}")
                return None

        return None

    def get_position(self, data):
        class Pos:
            size = 0
            # 持仓均价
            price = 0.0
            available = 0

        if not hasattr(self._context, 'account'):
            print("Warning: context object in GmBrokerAdapter is not valid or missing 'account' attribute.")
            return Pos()

        positions = self._context.account().positions()
        for p in positions:
            if p.symbol == data._name:
                pos_obj = Pos()
                pos_obj.size = p.volume
                pos_obj.price = p.vwap
                pos_obj.available = p.available
                return pos_obj
        return Pos()
