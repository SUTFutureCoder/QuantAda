import pandas as pd
import datetime
import config
import asyncio
from ib_insync import IB, Stock, MarketOrder, LimitOrder, OrderStatus, Trade, Forex, Contract

from .base_broker import BaseLiveBroker, BaseOrderProxy
from data_providers.ibkr_provider import IbkrDataProvider
from alarms.manager import AlarmManager


class IBOrderProxy(BaseOrderProxy):
    """IBKR 订单代理"""

    def __init__(self, trade: Trade, data=None):
        self.trade = trade
        self.data = data

    @property
    def id(self):
        # 使用 permId (永久ID) 或 orderId
        return str(self.trade.order.permId)

    @property
    def status(self):
        return self.trade.orderStatus.status

    @property
    def executed(self):
        class ExecutedStats:
            def __init__(self, trade):
                fill = trade.orderStatus
                self.size = fill.filled
                self.price = fill.avgFillPrice
                self.value = self.size * self.price
                # IBKR佣金通常在 completed 后才准确，早期可能为 None
                self.comm = 0.0
                if trade.fills:
                    self.comm = sum(f.commission for f in trade.fills)

        return ExecutedStats(self.trade)

    def is_completed(self) -> bool:
        return self.trade.orderStatus.status == 'Filled'

    def is_canceled(self) -> bool:
        return self.trade.orderStatus.status in ['Cancelled', 'ApiCancelled']

    def is_rejected(self) -> bool:
        return self.trade.orderStatus.status == 'Inactive'  # 或者是 Rejected

    def is_pending(self) -> bool:
        return self.trade.orderStatus.status in ['Submitted', 'PreSubmitted', 'PendingSubmit', 'PendingCancel']

    def is_accepted(self) -> bool:
        # PreSubmitted 意味着已经被 IB 系统接收
        return self.trade.orderStatus.status in ['PreSubmitted', 'Submitted', 'Filled']

    def is_buy(self) -> bool:
        return self.trade.order.action == 'BUY'

    def is_sell(self) -> bool:
        return self.trade.order.action == 'SELL'


class IBDataProvider(IbkrDataProvider):
    """
    继承自 data_providers.ibkr_provider.IbkrDataProvider
    保留在当前模块定义，以便 engine.py 能够通过反射自动发现。
    """

    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:
        """
        适配 engine.py 的接口调用
        直接透传调用父类的 get_data
        """
        return self.get_data(symbol, start_date, end_date, timeframe, compression)


class IBBrokerAdapter(BaseLiveBroker):
    """Interactive Brokers 适配器"""

    def __init__(self, context, cash_override=None, commission_override=None):
        # 从 context 中获取由 launch 注入的 ib 实例
        self.ib: IB = getattr(context, 'ib_instance', None)
        self._tickers = {}  # 缓存实时行情 snapshot
        super().__init__(context, cash_override, commission_override)

    def getcash(self):
        """兼容 Backtrader 标准接口: getcash -> get_cash"""
        return self.get_cash()

    def getvalue(self):
        """兼容 Backtrader 标准接口: getvalue -> get_value"""
        return self.get_value()

    @staticmethod
    def is_live_mode(context) -> bool:
        # IB Adapter 只要被调用基本都是为了实盘 (paper or live)
        # 回测建议使用 Backtrader 原生或 CSV
        return True

    @staticmethod
    def extract_run_config(context) -> dict:
        return {}

    @staticmethod
    def parse_contract(symbol: str) -> Contract:
        """
        [关键] 将框架的代码字符串转换为 IB Contract 对象
        规则可根据你的交易品种自定义：
        - AAPL -> Stock('AAPL', 'SMART', 'USD')
        - 00700 -> Stock('700', 'SEHK', 'HKD')
        - EURUSD -> Forex('EURUSD')
        """
        symbol = symbol.upper()

        if symbol.startswith('SHSE.') or symbol.startswith('SZSE.'):
            # 提取代码 (如 510300)
            code = symbol.split('.')[-1]
            # IBKR 上交易 A 股通常走 SEHK (Stock Connect)，货币为 CNH
            return Stock(code, 'SEHK', 'CNH')

        # 简单规则示例：
        if symbol == 'EURUSD':
            return Forex('EURUSD')

        if symbol.isdigit() or (len(symbol) == 5 and symbol.startswith('0')):
            # 假设纯数字是港股 (去除 .HK 后缀)
            code = int(symbol)  # 00700 -> 700
            return Stock(str(code), 'SEHK', 'HKD')

        # 默认美股
        return Stock(symbol, 'SMART', 'USD')

    # 1. 查钱
    def _fetch_real_cash(self) -> float:
        """
        获取账户净资产(NetLiquidation)并强制转换为 USD。
        支持: 直接读取 USD -> 自动汇率转换 (如 HKD/USD)
        """
        if not hasattr(self, 'ib') or not self.ib: return 0.0

        # 检测当前是否在事件循环中 (例如在回调函数中)
        in_loop = False
        try:
            if asyncio.get_running_loop():
                in_loop = True
        except RuntimeError:
            pass

        tags_priority = ['NetLiquidation', 'TotalCashValue', 'AvailableFunds', 'TotalCashBalance']

        # --- Method A: 尝试通过 accountSummary 直接请求 USD ---
        if not in_loop:
            try:
                summary = self.ib.accountSummary()
                if not summary:
                    self.ib.sleep(0.5)
                    summary = self.ib.accountSummary()

                for tag in tags_priority:
                    items = [v for v in summary if v.tag == tag and v.currency == 'USD']
                    if items:
                        return float(items[0].value)
            except Exception:
                pass

        # --- Method B: 降级到 accountValues (查找任意基础货币) ---
        print("[IB Debug] Fallback to raw accountValues (Auto-FX Mode)...")
        account_values = self.ib.accountValues()
        if not account_values: return 0.0

        base_cash = 0.0
        base_currency = None
        found_tag = None

        # 1. 先找到一个有钱的非 USD 货币
        for tag in tags_priority:
            # 排除 'BASE' 这种虚拟单位，找具体的 currency 如 'HKD', 'CNH'
            items = [v for v in account_values if
                     v.tag == tag and v.currency and v.currency != 'USD' and v.currency != 'BASE']
            for item in items:
                try:
                    val = float(item.value)
                    if val > 0:
                        base_cash = val
                        base_currency = item.currency
                        found_tag = tag
                        break
                except:
                    continue
            if base_currency: break

        if not base_currency:
            print("[IB Error] No positive cash balance found in ANY currency.")
            return 0.0

        print(f"[IB Debug] Found {base_cash} {base_currency} ({found_tag}). Fetching exchange rate...")

        # --- Method C: 实时查询汇率并转换 ---
        try:
            # 构造外汇对：通常 IB 的格式是 "USD" + "Base" (例如 USDHKD)
            # 我们需要知道 1 USD = ? Base，然后用 Base Cash 除以这个汇率
            pair_symbol = f"USD{base_currency}"
            contract = Forex(pair_symbol)

            # 获取实时行情
            # 如果在 Loop 中，不能调用 qualifyContracts (阻塞)
            if not in_loop:
                self.ib.qualifyContracts(contract)

            # reqMktData 是非阻塞的，可以安全调用
            # 如果没有 qualify，IB 通常也能识别简单的 Forex 对
            ticker = self.ib.reqMktData(contract, '', False, False)

            # 等待数据回包 (最多等 2 秒)
            exchange_rate = 0.0

            if not in_loop:
                # 正常模式：可以 sleep 等待数据
                start_wait = datetime.datetime.now()
                while (datetime.datetime.now() - start_wait).total_seconds() < 2.0:
                    self.ib.sleep(0.1)
                    rate = self._extract_rate_from_ticker(ticker)
                    if rate > 0:
                        exchange_rate = rate
                        break
            else:
                # 回调模式：不能 sleep，只能看一眼当前数据
                # print("[IB Debug] Inside EventLoop, attempting immediate rate fetch...")
                exchange_rate = self._extract_rate_from_ticker(ticker)

            # 针对 HKD 的强锚定硬兜底
            if exchange_rate <= 0:
                if base_currency == 'HKD':
                    # HKD 锚定区间 7.75 - 7.85
                    # 换算公式: USD = HKD / Rate
                    # 为了风控安全，除以最大值 7.85 (得到最小的 USD 估值)
                    exchange_rate = 7.85
                    print(
                        f"[IB Warning] Failed to fetch rates. Using conservative fallback for HKD: {exchange_rate}")

            if exchange_rate > 0:
                usd_value = base_cash / exchange_rate
                print(
                    f"[IB FX] Rate {pair_symbol}: {exchange_rate:.4f} | Converted: {base_cash} {base_currency} -> {usd_value:.2f} USD")
                return usd_value
            else:
                print(f"[IB Error] Failed to fetch valid rate for {pair_symbol}. Ticker state: {ticker}")
                return 0.0

        except Exception as e:
            print(f"[IB Error] FX Conversion failed: {e}")
            return 0.0

    def _extract_rate_from_ticker(self, ticker):
        """辅助方法：从 ticker 中提取有效汇率，含 Close/Last 兜底"""
        rate = ticker.marketPrice()
        if not (rate and rate > 0 and rate == rate):
            if ticker.close and ticker.close > 0:
                return ticker.close
            elif ticker.last and ticker.last > 0:
                return ticker.last
        return rate

    # 2. 查持仓
    def get_position(self, data):
        class Pos:
            size = 0;
            price = 0.0

        if not self.ib: return Pos()

        symbol = data._name
        # 遍历 ib.positions()
        # 注意：IB position 的 symbol 格式可能和 data._name 不完全一致，需要模糊匹配
        positions = self.ib.positions()
        target_contract = self.parse_contract(symbol)

        for p in positions:
            # 简单对比 symbol
            if p.contract.symbol == target_contract.symbol and p.contract.secType == target_contract.secType:
                o = Pos()
                o.size = p.position
                o.price = p.avgCost
                return o
        return Pos()

    # 3. 查价
    def _get_current_price(self, data):
        """
        获取标的当前价格。
        增强版：支持周末/休市期间使用 Close/Last 价格兜底，防止无法计算下单数量。
        """
        if not hasattr(self, 'ib') or not self.ib or not self.ib.isConnected():
            return 0.0

        symbol = data._name
        ticker = self._tickers.get(symbol)

        # 1. 如果缓存里没有 ticker (防御性逻辑，防止动态添加的标的没订阅)
        if not ticker:
            # print(f"[IB Debug] Ticker not found for {symbol}, requesting subscription...")
            contract = self.parse_contract(symbol)
            self.ib.qualifyContracts(contract)
            # snapshot=False 建立流式订阅
            ticker = self.ib.reqMktData(contract, '', False, False)
            self._tickers[symbol] = ticker
            # 如果在 Loop 里，这句 sleep 可能会报错，所以加个 try
            try:
                self.ib.sleep(0.5)
            except:
                pass

        # 2. 获取价格 (优先 marketPrice)
        price = ticker.marketPrice()

        # 如果 marketPrice 无效 (NaN/0/-1)，尝试使用 close 或 last
        # 这种情况常见于周末、盘前盘后或停牌
        if not (price and 0 < price == price):
            # 优先用昨日收盘价 (Close)
            if ticker.close and ticker.close > 0:
                print(
                    f"[IB Debug] {symbol} marketPrice invalid ({price}). Using CLOSE price for execution: {ticker.close}")
                price = ticker.close
            # 其次用最后成交价 (Last)
            elif ticker.last and ticker.last > 0:
                print(f"[IB Debug] {symbol} marketPrice invalid. Using LAST price: {ticker.last}")
                price = ticker.last
            else:
                # 极少数情况：刚订阅连快照都没回来，打印警告
                print(f"[IB Warning] No valid price (Market/Close/Last) for {symbol}. Ticker: {ticker}")
                pass

        return price

    # 4. 发单
    def _submit_order(self, data, volume, side, price):
        if not self.ib: return None

        contract = self.parse_contract(data._name)
        action = 'BUY' if side == 'BUY' else 'SELL'

        # 使用市价单 (MarketOrder) 或 限价单 (LimitOrder)
        # 此处简单起见使用市价单，你可以根据 price 参数决定是否发限价单
        if price > 0:
            # 加上一点滑点保护
            # lmt_price = price * 1.01 if side == 'BUY' else price * 0.99
            # order = LimitOrder(action, abs(volume), lmt_price)
            order = MarketOrder(action, abs(volume))  # 暂时全用市价
        else:
            order = MarketOrder(action, abs(volume))

        # 防止零股交易 (IB部分账户不支持小于1股)
        if abs(volume) < 1:
            print(f"[IB Warning] Order size < 1 ({volume}), skipped.")
            return None

        trade = self.ib.placeOrder(contract, order)
        return IBOrderProxy(trade, data=data)

    # 5. 将券商的原始订单对象（raw_order）转换为框架标准的 BaseOrderProxy
    def convert_order_proxy(self, raw_trade_or_order) -> 'BaseOrderProxy':
        """
        注意：IB 的回调有时候传回 Trade 对象，有时候是 Order 对象，需要这里做判断处理
        """
        # 假设 raw_trade_or_order 是 ib_insync 的 Trade 对象
        # 如果 Engine 里的回调传的是 order，这里需要适配一下

        trade = raw_trade_or_order
        # 如果传入的只是 Order 对象（没有 Trade 包装），可能需要特殊处理或者在 IB 回调入口处统一封装

        # 查找 Data
        target_symbol = ""
        if hasattr(trade, 'contract'):
            target_symbol = trade.contract.symbol
        elif hasattr(trade, 'symbol'):  # 万一是 Contract
            target_symbol = trade.symbol

        matched_data = None
        # 简单的符号匹配逻辑 (可能需要根据 IBBrokerAdapter.parse_contract 的逆逻辑来匹配)
        for d in self.datas:
            # 这里的匹配逻辑取决于你 IB 的 symbol 命名习惯
            if target_symbol in d._name:
                matched_data = d
                break

        return IBOrderProxy(trade, data=matched_data)

    # 5. IB 特有的启动协议
    @classmethod
    def launch(cls, conn_cfg: dict, strategy_path: str, params: dict, **kwargs):
        """
        IBKR 全天候启动入口
        """
        import config
        host = conn_cfg.get('host', config.IBKR_HOST)
        port = int(conn_cfg.get('port', config.IBKR_PORT))
        client_id = int(conn_cfg.get('client_id', config.IBKR_CLIENT_ID))

        symbols = kwargs.get('symbols', [])
        selection_name = kwargs.get('selection')

        # 获取定时配置
        schedule_rule = conn_cfg.get('schedule')
        schedule_time = None
        target_timezone = conn_cfg.get('timezone', None)
        if target_timezone:
            print(f"\n>>> 🌍 Timezone Override: Forces {target_timezone} <<<")

        if schedule_rule:
            try:
                # 解析格式 "1d:14:50:00" -> 提取 "14:50:00"
                if ':' in schedule_rule:
                    _, time_part = schedule_rule.split(':', 1)
                    schedule_time = datetime.datetime.strptime(time_part, '%H:%M:%S').time()
                    print(f"\n>>> ⏰ Schedule Enabled: Run daily at {schedule_time} <<<")
                else:
                    print(
                        f"\n[Warning] Invalid schedule format '{schedule_rule}'. Expected '1d:HH:MM:SS'. Using default Heartbeat.")
            except Exception as e:
                print(f"\n[Error] Failed to parse schedule: {e}")

        print(f"\n>>> Launching {cls.__name__} connecting to {host}:{port} <<<")

        ib = IB()
        try:
            ib.connect(host, port, clientId=client_id)
        except Exception as e:
            print(f"[Critical] Cannot connect to IBKR: {e}")
            return

        # 注入 context
        class Context:
            now = pd.Timestamp.now()
            ib_instance = ib
            strategy_instance = None

        ctx = Context()

        # 初始化 Engine
        import config
        from live_trader.engine import LiveTrader, on_order_status_callback

        engine_config = config.__dict__.copy()
        engine_config['strategy_name'] = strategy_path
        engine_config['params'] = params
        engine_config['platform'] = 'ib'  # 标记平台
        engine_config['symbols'] = symbols

        if selection_name:
            print(f"[IB] Selection Strategy enabled: {selection_name}")
            engine_config['selection_name'] = selection_name

        trader = LiveTrader(engine_config)
        if hasattr(trader.data_provider, 'ib'):
            print("[IB] Injecting IB connection into DataProvider...")
            trader.data_provider.ib = ib

        trader.init(ctx)

        # 将策略实例注入到 Context 中，解决回调报错
        ctx.strategy_instance = trader.strategy

        target_symbols = []
        if hasattr(trader.broker, 'datas'):
            target_symbols = [d._name for d in trader.broker.datas]
            print(f"[IB] Strategy loaded {len(target_symbols)} symbols: {target_symbols}")
        else:
            target_symbols = symbols

        # 订阅行情 (关键步骤)
        print("[IB] Requesting Market Data subscriptions...")
        active_tickers = {}
        for sym in target_symbols:  # 使用最终确定的标的列表
            contract = cls.parse_contract(sym)
            ib.qualifyContracts(contract)
            ticker = ib.reqMktData(contract, '', False, False)
            active_tickers[sym] = ticker

        trader.broker._tickers = active_tickers

        # 注册订单回调
        def on_trade_update(trade):
            on_order_status_callback(ctx, trade)

        ib.orderStatusEvent += on_trade_update

        last_run_date = None

        # 统一获取当前时间的方法，确保时区一致
        def get_now_aware():
            if target_timezone:
                return pd.Timestamp.now(tz=target_timezone).to_pydatetime()
            else:
                return datetime.datetime.now()

        # 使用统一的方法获取当前时间
        now = get_now_aware()

        if schedule_time:
            # 检查：如果启动时已经超过了当天的计划时间
            if now.time() >= schedule_time:
                # 检查是否开启了调试模式 (从 params 或 kwargs 获取 debug 标记)
                # 可以在启动命令中加入 --params debug=True
                is_debug = params.get('debug', False) or kwargs.get('debug', False)

                if str(is_debug).lower() in ['true', '1', 'yes']:
                    print(
                        f"\n[⚠️ Debug Mode] Current time {now.strftime('%H:%M:%S')} is past schedule {schedule_time}.")
                    print(f"[⚠️ Debug Mode] System WILL execute strategy immediately as requested.")
                    # last_run_date 保持为 None，这会导致下方的循环立即触发一次 run
                else:
                    print(
                        f"\n[🛡️ Safety Check] System started at {now.strftime('%H:%M:%S')}, which is past schedule {schedule_time}.")
                    print(
                        f"[🛡️ Safety Check] Today's run is SKIPPED to prevent accidental double-execution (Restart Risk).")
                    print(f"[🛡️ Safety Check] System will standby for tomorrow's schedule.")

                    # 关键操作：将今天标记为"已运行"，从而让循环跳过今天的触发
                    last_run_date = now.date()

        # 主循环
        print("[IB] Starting Event Loop...")
        if schedule_time:
            print(f"     Mode: Scheduled (Daily @ {schedule_time})")
        else:
            print(f"     Mode: Heartbeat (Every 60s)")

        last_check = get_now_aware()

        try:
            while ib.isConnected():
                # 1. 驱动 IB 事件循环
                ib.sleep(1)  # 休眠1秒，允许后台线程处理数据

                # 2. 定时运行 Engine 逻辑 (模拟 Bar 事件)
                if target_timezone:
                    # 使用 pandas 转换到目标时区，再转回 python datetime (带时区信息)
                    now = pd.Timestamp.now(tz=target_timezone).to_pydatetime()
                else:
                    # 默认行为：使用服务器本地时间
                    now = get_now_aware()

                ctx.now = pd.Timestamp(now)

                # --- 调度逻辑分支 ---
                if schedule_time:
                    # [模式 A] 定时执行
                    # 只有当：现在时间到了 AND 今天还没跑过 (last_run_date != today) 时才触发
                    if now.time() >= schedule_time and now.date() != last_run_date:
                        print(f"\n[Schedule Trigger] {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        trader.run(ctx)
                        # 运行完立刻标记今天已完成
                        last_run_date = now.date()
                        print(f"[Schedule] Finished. Next run date: {last_run_date + datetime.timedelta(days=1)}")

                    # 心跳日志
                    if (now - last_check).total_seconds() >= 60:
                        last_check = now

                else:
                    # [模式 B] 默认每分钟轮询
                    if (now - last_check).total_seconds() >= 60:
                        print(f"[Heartbeat] {now.strftime('%H:%M:%S')}")
                        trader.run(ctx)
                        last_check = now

        except KeyboardInterrupt:
            print("\n[Stop] User interrupted")
        except Exception as e:
            print(f"[Error] IB Loop crash: {e}")
        finally:
            ib.disconnect()