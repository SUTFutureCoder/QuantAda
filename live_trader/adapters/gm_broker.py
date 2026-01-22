import datetime

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
                     fields='open,high,low,close,volume,eob', adjust=1, df=True)
        if df.empty: return df
        df.rename(columns={'eob': 'datetime'}, inplace=True)
        df.set_index('datetime', inplace=True)
        df.index = pd.to_datetime(df.index)

        if frequency == '1d':
            try:
                ticks = current(symbols=symbol)
                if ticks:
                    tick = ticks[0]
                    tick_price = tick['price']

                    # tick['created_at'] 通常已经是带时区的 datetime 对象
                    tick_time = tick['created_at']
                    if isinstance(tick_time, str):
                        tick_time = pd.to_datetime(tick_time)

                    # 获取日期用于比较 (date() 对象是无时区的，可以直接比)
                    tick_date = tick_time.date() if hasattr(tick_time, 'date') else tick_time.date()

                    # 构造当日 Bar
                    open_p = tick['open'] if tick['open'] > 0 else tick_price
                    high_p = tick['high'] if tick['high'] > 0 else tick_price
                    low_p = tick['low'] if tick['low'] > 0 else tick_price

                    # 先创建一个初始 DataFrame
                    today_bar = pd.DataFrame({
                        'open': [open_p],
                        'high': [high_p],
                        'low': [low_p],
                        'close': [tick_price],
                        'volume': [tick['cum_volume']],
                        'datetime': [tick_time]  # 直接使用原始带时区时间，或后续处理
                    })
                    today_bar.set_index('datetime', inplace=True)

                    # 【核心修正】：对齐时区 (Timezone Alignment)
                    if not df.empty:
                        # 获取历史数据的时区
                        target_tz = df.index.tz

                        if target_tz is not None:
                            # 如果 today_bar 也是带时区的，则转换；如果是 naive 的，则本地化
                            if today_bar.index.tz is None:
                                today_bar.index = today_bar.index.tz_localize(target_tz)
                            else:
                                today_bar.index = today_bar.index.tz_convert(target_tz)

                        # 归一化时间到 00:00:00 (保持时区不变)
                        # 掘金历史数据通常是 00:00:00+08:00
                        today_bar.index = today_bar.index.normalize()

                    # --- 拼接判断 ---
                    should_append = False
                    if df.empty:
                        should_append = True
                    else:
                        # 比较日期 (使用 .date() 安全比较)
                        last_hist_date = df.index[-1].date()
                        if tick_date > last_hist_date:
                            should_append = True

                    if should_append:
                        df = pd.concat([df, today_bar])
                        # print(f"[Data] Stitched real-time bar for {symbol} (TZ-Aware)")

            except Exception as e:
                print(f"[GmDataProvider] Warning: Failed to stitch real-time data for {symbol}: {e}")

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

