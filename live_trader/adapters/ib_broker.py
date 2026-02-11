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
        return str(self.trade.order.orderId)

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
                # IBKR佣金信息在 commissionReport 对象中
                # 必须检查 commissionReport 是否存在，否则会报 AttributeError
                self.comm = 0.0
                if trade.fills:
                    try:
                        self.comm = sum(
                            (f.commissionReport.commission if f.commissionReport else 0.0)
                            for f in trade.fills
                        )
                    except AttributeError:
                        # 防御性编程：万一结构有变，默认为0不崩亏
                        self.comm = 0.0

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
        self._fx_tickers = {}  # 缓存汇率行情
        super().__init__(context, cash_override, commission_override)

    def getcash(self):
        """兼容 Backtrader 标准接口: getcash -> get_cash"""
        return self.get_cash()

    def getvalue(self):
        """
        兼容 Backtrader 标准接口: 获取账户总权益
        注意：IB Adapter 的 _fetch_real_cash 实现取的就是 NetLiquidation
        """
        return self._fetch_real_cash()

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
        合约解析器
        支持格式:
        1. "QQQ.ISLAND" -> 美股指定主交易所 (PrimaryExchange)
        2. "SHSE.600000" -> A股 (保持兼容)
        3. "00700" -> 港股 (保持兼容)
        4. "AAPL" -> 默认 SMART/USD
        """
        symbol = symbol.upper()

        # --- A. 特殊前缀处理 (A股/外汇等) ---
        if symbol.startswith('SHSE.') or symbol.startswith('SZSE.'):
            code = symbol.split('.')[-1]
            return Stock(code, 'SEHK', 'CNH')  # A股走深港/沪港通

        if symbol.startswith('CASH.'):
            # CASH.EUR.USD -> Forex('EURUSD')
            parts = symbol.split('.')
            return Forex(f"{parts[1]}{parts[2]}")

        # --- B. 核心升级：支持 SYMBOL.EXCHANGE 格式 ---
        # 识别逻辑：如果包含点，且点后面的是已知的交易所代码
        if '.' in symbol:
            parts = symbol.split('.')
            # 确保切分后只有两部分，防止干扰其他复杂格式
            if len(parts) == 2:
                code, exch = parts

                # 定义美股常用主交易所白名单 (防止误判)
                # ISLAND=Nasdaq, ARCA=NYSE Arca, BATS=Cboe BZX
                us_exchanges = ['ISLAND', 'NASDAQ', 'ARCA', 'NYSE', 'AMEX', 'BATS', 'PINK']

                if exch in us_exchanges:
                    # 关键点：Routing 依然用 SMART (保证流动性)，但指定 primaryExchange (消除歧义)
                    return Stock(code, 'SMART', 'USD', primaryExchange=exch)

        # --- C. 港股纯数字逻辑 (保持兼容) ---
        if symbol.isdigit() or (len(symbol) == 5 and symbol.startswith('0')):
            code = int(symbol)
            return Stock(str(code), 'SEHK', 'HKD')

        # --- D. 默认兜底 (Fall back to SMART) ---
        # 这是你要求的：仅当没有交易所信息时，才使用默认 SMART
        return Stock(symbol, 'SMART', 'USD')

    # 1. 查钱
    def _fetch_real_cash(self) -> float:
        """
        获取账户净资产(NetLiquidation)并强制转换为 USD。
        逻辑升级：
        1. 优先获取 NetLiquidation (无论基准货币是 USD/HKD/JPY)。
        2. 如果不是 USD，则自动查询汇率进行折算。
        3. 增加 FX Ticker 缓存，解决在 EventLoop 回调中无法获取实时汇率的问题。
        """
        if not hasattr(self, 'ib') or not self.ib: return 0.0

        # 检测当前是否在事件循环中
        in_loop = False
        try:
            if asyncio.get_running_loop():
                in_loop = True
        except RuntimeError:
            pass

        base_cash = 0.0
        base_currency = None
        found_tag = None

        tags_priority = ['NetLiquidation', 'TotalCashValue', 'AvailableFunds']

        # --- Method A: 通过 accountSummary 获取 NetLiquidation ---
        if not in_loop:
            try:
                summary = self.ib.accountSummary()
                if not summary:
                    self.ib.sleep(0.5)
                    summary = self.ib.accountSummary()

                # 1. 优先找 NetLiquidation (这是真正的 NAV)
                for tag in tags_priority:
                    # 先找 USD
                    items_usd = [v for v in summary if v.tag == tag and v.currency == 'USD']
                    if items_usd:
                        return float(items_usd[0].value)

                    # 没找到 USD，找任意货币
                    items_any = [v for v in summary if v.tag == tag and v.currency]
                    if items_any:
                        item = items_any[0]
                        val = float(item.value)
                        if tag == 'NetLiquidation' or val > 0:
                            base_cash = val
                            base_currency = item.currency
                            found_tag = tag
                            break
            except Exception:
                pass

        # --- Method B: 降级到 accountValues (兜底) ---
        if not base_currency:
            # print("[IB Debug] Fallback to raw accountValues (Auto-FX Mode)...")
            account_values = self.ib.accountValues()
            if not account_values: return 0.0

            for tag in tags_priority:
                items = [v for v in account_values if
                         v.tag == tag and v.currency and v.currency != 'USD' and v.currency != 'BASE']
                for item in items:
                    try:
                        val = float(item.value)
                        if tag == 'NetLiquidation' or val > 0:
                            base_cash = val
                            base_currency = item.currency
                            found_tag = tag
                            break
                    except:
                        continue
                if base_currency: break

        if not base_currency:
            print("[IB Error] No NetLiquidation or positive cash found in ANY currency.")
            return 0.0

        # print(f"[IB Debug] Found {base_cash} {base_currency} ({found_tag}). Fetching exchange rate...")

        # --- Method C: 实时查询汇率并转换 (FX Conversion) ---
        try:
            if base_currency == 'USD':
                return base_cash

            pair_symbol = f"USD{base_currency}"
            # 简单处理：如果是 EUR/GBP/AUD/NZD，通常是 EURUSD 格式
            inverse_pair = False
            if base_currency in ['EUR', 'GBP', 'AUD', 'NZD']:
                pair_symbol = f"{base_currency}USD"
                inverse_pair = True

            # 使用缓存的 Ticker，避免重复创建和订阅
            # 在 Loop 中重复 reqMktData 而不 yield 会导致数据永远无法返回
            ticker = self._fx_tickers.get(pair_symbol)

            if not ticker:
                contract = Forex(pair_symbol)
                # 只有不在 Loop 中时才 qualify，否则可能会阻塞或报错
                if not in_loop:
                    self.ib.qualifyContracts(contract)

                # 建立订阅并缓存
                ticker = self.ib.reqMktData(contract, '', False, False)
                self._fx_tickers[pair_symbol] = ticker

                # 首次订阅，稍微等待数据 (如果在 Loop 中则无法等待，只能依赖下一次调用或 Fallback)
                if not in_loop:
                    start_wait = datetime.datetime.now()
                    while (datetime.datetime.now() - start_wait).total_seconds() < 2.0:
                        self.ib.sleep(0.1)
                        if self._extract_rate_from_ticker(ticker) > 0:
                            break

            exchange_rate = self._extract_rate_from_ticker(ticker)

            # --- 针对 HKD/JPY/CNH 的强锚定硬兜底 ---
            # 使用 not (rate > 0) 这种判断方式，可以同时捕获 0、负数 以及 NaN
            # 因为 NaN <= 0 是 False，会导致代码跳过兜底逻辑
            if not (exchange_rate > 0):
                bc = base_currency.strip().upper()
                if bc == 'HKD':
                    exchange_rate = 7.85
                    print(f"[IB Warning] Using hardcoded fallback for HKD: {exchange_rate}")
                elif bc == 'JPY':
                    exchange_rate = 150.0
                    print(f"[IB Warning] Using hardcoded fallback for JPY: {exchange_rate}")
                elif bc == 'CNH':
                    exchange_rate = 7.3
                    print(f"[IB Warning] Using hardcoded fallback for CNH: {exchange_rate}")
                else:
                    pass

            if exchange_rate > 0:
                if inverse_pair:
                    usd_value = base_cash * exchange_rate
                else:
                    usd_value = base_cash / exchange_rate

                # print(f"[IB FX] {pair_symbol}: {exchange_rate:.4f} | NAV: {base_cash} {base_currency} -> {usd_value:.2f} USD")
                return usd_value
            else:
                print(f"[IB Error] Failed to fetch valid rate for {pair_symbol}. Ticker: {ticker}")
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
            # 尝试 midPoint (Forex 有时用这个)
            elif ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                return (ticker.bid + ticker.ask) / 2
        return rate

    # 2. 查持仓
    def get_position(self, data):
        class Pos:
            size = 0
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
        import time
        import asyncio
        import pytz
        from ib_insync import IB

        host = conn_cfg.get('host', config.IBKR_HOST)
        port = int(conn_cfg.get('port', config.IBKR_PORT))
        client_id = int(conn_cfg.get('client_id', config.IBKR_CLIENT_ID))

        # 默认为空，表示使用服务器本地时间
        timezone_str = conn_cfg.get('timezone')
        target_tz = pytz.timezone(timezone_str) if timezone_str else None

        # 1. 获取调度配置 (格式示例: "1d:14:50:00")
        schedule_rule = conn_cfg.get('schedule')
        if not schedule_rule:
            # 尝试从 kwargs 获取 (兼容命令行传参)
            schedule_rule = kwargs.get('schedule')

        symbols = kwargs.get('symbols', [])
        selection_name = kwargs.get('selection')

        print(f"\n>>> 🛡️ Launching IBKR Phoenix Mode (Host: {host}:{port}) <<<")
        if schedule_rule:
            tz_info = timezone_str if timezone_str else "Server Local Time"
            print(f">>> ⏰ Schedule Active: {schedule_rule} (Zone: {tz_info})")
        else:
            print(f">>> ⚠️ No Schedule Found: Strategy will NOT run automatically. (Heartbeat Only)")

        # 1. 创建全局唯一的 IB 实例
        ib = IB()

        # 2. 预初始化 Engine Context
        class Context:
            now = pd.Timestamp.now()
            ib_instance = ib
            strategy_instance = None

        ctx = Context()

        # 初始化 Engine (只做一次)
        from live_trader.engine import LiveTrader, on_order_status_callback
        engine_config = config.__dict__.copy()
        engine_config['strategy_name'] = strategy_path
        engine_config['params'] = params
        engine_config['platform'] = 'ib'
        engine_config['symbols'] = symbols
        if selection_name: engine_config['selection_name'] = selection_name

        trader = LiveTrader(engine_config)
        # 注入 IB 实例到 data_provider (如果有)
        if hasattr(trader.data_provider, 'ib'):
            trader.data_provider.ib = ib

        trader.init(ctx)
        ctx.strategy_instance = trader.strategy

        # 确定标的列表
        target_symbols = []
        if hasattr(trader.broker, 'datas'):
            target_symbols = [d._name for d in trader.broker.datas]
        else:
            target_symbols = symbols

        # 注册回调
        def on_trade_update(trade):
            on_order_status_callback(ctx, trade)

        ib.orderStatusEvent += on_trade_update

        # --- 调度器状态变量 ---
        last_schedule_run_date = None  # 记录上次运行的日期 (防止同一分钟重复运行)
        is_first_connect = True

        # --- 3. 进入“不死鸟”主循环 ---
        while True:
            try:
                # --- A. 连接阶段 ---
                if not ib.isConnected():
                    print(f"[System] Connecting to IB Gateway ({host}:{port})...")
                    try:
                        ib.connect(host, port, clientId=client_id)
                        print("[System] ✅ Connected successfully.")
                    except Exception as e:
                        # 捕获所有连接时的异常 (如 ConnectionRefusedError)
                        print(f"[System] ⏳ Connection failed: {e}. Retrying in 10s...")
                        time.sleep(10)
                        continue

                # --- B. 状态恢复 (Re-Subscribe) ---
                if is_first_connect or not ib.tickers():  # 如果没有 tickers 说明订阅丢了
                    print(f"[System] 📡 (Re)Subscribing market data for {len(target_symbols)} symbols...")
                    active_tickers = {}
                    for sym in target_symbols:
                        try:
                            contract = cls.parse_contract(sym)
                            ib.qualifyContracts(contract)
                            # snapshot=False 建立流式订阅
                            ticker = ib.reqMktData(contract, '', False, False)
                            active_tickers[sym] = ticker
                        except Exception as e:
                            print(f"[Warning] Failed to subscribe {sym}: {e}")

                    # 更新 Broker 的引用
                    trader.broker._tickers = active_tickers

                    if not is_first_connect:
                        print("[System] 🔄 Re-connection logic triggered (Data Stream Restored).")

                is_first_connect = False

                # --- C. 运行阶段 (Event Loop) ---
                print("[System] Entering Event Loop...")

                while ib.isConnected():
                    # 1. 驱动 IB 事件
                    # 如果断线，ib.sleep 会抛出 OSError 或 ConnectionResetError
                    ib.sleep(1)

                    # 基于时区的时间计算
                    if target_tz:
                        # 如果配置了时区，获取带时区的当前时间
                        now = datetime.datetime.now(target_tz)
                    else:
                        # 否则使用本地时间
                        now = datetime.datetime.now()

                    # 2. 执行策略
                    ctx.now = pd.Timestamp(now)


                    # (B) 调度检查逻辑
                    if schedule_rule:
                        try:
                            # 解析 "1d:HH:MM:SS" (仅处理 1d 每日任务)
                            # 如果你的 schedule_rule 格式是 "1d:14:50:00"
                            if schedule_rule.startswith('1d:'):
                                _, target_time_str = schedule_rule.split(':', 1)

                                parts = target_time_str.split(':')
                                target_h = int(parts[0])
                                target_m = int(parts[1])
                                target_s = int(parts[2]) if len(parts) > 2 else 0

                                target_dt = now.replace(hour=target_h, minute=target_m, second=target_s,
                                                        microsecond=0)

                                # 2. 计算当前时间与目标时间的偏差 (秒)
                                delta = (now - target_dt).total_seconds()

                                # 3. 判定触发条件：
                                #    (a) 时间落在 [0, 5] 秒的窗口内 (允许迟到 5 秒)
                                #    (b) 今天还没跑过 (防止 5 秒内重复触发)
                                TOLERANCE_WINDOW = 5.0

                                current_date_str = now.strftime('%Y-%m-%d')

                                if 0 <= delta <= TOLERANCE_WINDOW:
                                    if last_schedule_run_date != current_date_str:
                                        print(
                                            f"\n>>> ⏰ Schedule Triggered: {schedule_rule} (Delta: {delta:.2f}s) <<<")

                                        # === 触发策略运行 ===
                                        trader.run(ctx)

                                        # === 更新状态锁 ===
                                        last_schedule_run_date = current_date_str
                                        print(f">>> Run Finished. Next run: Tomorrow {target_time_str}\n")
                                    else:
                                        # (可选) 如果在窗口内但已经跑过，说明正在窗口期内sleep，无需操作
                                        pass
                            else:
                                # 如果以后支持其他频率 (如 1h)，在这里扩展
                                pass

                        except Exception as e:
                            print(f"[Schedule Error] Check failed: {e}")

            # --- D. 异常处理 ---
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, TimeoutError, ConnectionError,
                    asyncio.TimeoutError) as e:
                # 捕获这些明确的网络层异常
                print(f"\n[⚠️ Disconnect] Network Error: {e}")
                print("[System] Entering Recovery Mode. Waiting for TWS/Gateway...")

                try:
                    ib.disconnect()
                except:
                    pass

                time.sleep(10)  # 稍微长一点的冷却
                continue

            except Exception as e:
                # 捕获其他未知的崩溃 (如数据解析错误)
                print(f"[CRITICAL] Unexpected crash in Main Loop: {e}")
                import traceback
                traceback.print_exc()

                # 防止死循环刷屏
                time.sleep(5)
                # 尝试重启
                try:
                    ib.disconnect()
                except:
                    pass
                continue

            except KeyboardInterrupt:
                print("\n[Stop] User interrupted. Exiting.")
                ib.disconnect()
                break