import asyncio
import datetime

import pandas as pd
from ib_insync import IB, Stock, MarketOrder, Trade, Forex, Contract

from data_providers.ibkr_provider import IbkrDataProvider
from .base_broker import BaseLiveBroker, BaseOrderProxy


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

    def __init__(self, context, cash_override=None, commission_override=None, slippage_override=None):
        # 从 context 中获取由 launch 注入的 ib 实例
        self.ib: IB = getattr(context, 'ib_instance', None)
        self._tickers = {}  # 缓存实时行情 snapshot
        self._fx_tickers = {}  # 缓存汇率行情
        # 最后已知有效汇率缓存 (Last Known Good Rate)
        self._last_valid_fx_rates = {}
        super().__init__(context, cash_override, commission_override, slippage_override)

    def _fetch_real_cash(self) -> float:
        """
        [必须实现] 基类要求的底层查钱接口
        用于初始化(_init_cash)和资金同步(sync_balance)
        """
        # 优先获取 TotalCashValue (现金账户) 或 AvailableFunds (保证金账户可用资金)
        return self._fetch_smart_value(['TotalCashValue', 'AvailableFunds'])

    def getcash(self):
        """兼容 Backtrader 标准接口: 获取可用资金 (Buying Power)"""
        return self.get_cash()

    def get_cash(self):
        """
        覆盖 BaseLiveBroker 的 get_cash，确保策略层调用的是真实的可用购买力。
        由于盈透的 TotalCashValue 在挂单阶段不会扣减，
        此处必须手动减去在途买单(Pending BUY)的预期消耗，防止产生无限杠杆幻觉。
        """
        # 1. 获取物理账面现金
        # 明确获取可用资金 (优先 TotalCashValue，其次 AvailableFunds)，而非总资产
        # 如果是现金账户，必须用 TotalCashValue；如果是保证金账户，AvailableFunds 包含融资额度
        raw_cash = self._fetch_smart_value(['TotalCashValue', 'AvailableFunds'])

        # 2. 盘点所有在途买单，计算尚未物理扣款的“虚拟冻结资金”
        virtual_frozen_cash = 0.0
        try:
            pending_orders = self.get_pending_orders()
            for po in pending_orders:
                if po['direction'] == 'BUY':
                    symbol = po['symbol']
                    size = po['size']

                    price = 0.0
                    # 方案 A：优先从框架维护的数据流 (datas) 中精准提取最新价
                    for d in self.datas:
                        # 兼容 'AAPL.SMART' 和 'AAPL' 的命名匹配
                        if symbol == d._name or symbol == d._name.split('.')[0]:
                            price = self.get_current_price(d)
                            break

                    # 方案 B：如果 datas 未命中，从 IB 实时行情快照 _tickers 兜底获取
                    if price == 0.0 and symbol in self._tickers:
                        ticker = self._tickers[symbol]
                        p = ticker.marketPrice()
                        # 规避 NaN 或 0 等无效报价，启用盘前/周末休市兜底
                        if not (p and p > 0):
                            p = ticker.close if (ticker.close and ticker.close > 0) else ticker.last
                        if p and p > 0:
                            price = p

                    # 如果成功获取价格，累加冻结金额
                    # 附加 1.5% 的乘数作为防爆仓安全垫（覆盖滑点与 IBKR 佣金）
                    if price > 0:
                        virtual_frozen_cash += size * price * 1.015

        except Exception as e:
            print(f"[IBBroker] 计算买单虚拟冻结资金时发生异常: {e}")

        # 3. 真实购买力 = 账面资金 - 虚拟冻结资金 (防止透支显示)
        real_available_cash = raw_cash - virtual_frozen_cash
        return max(0.0, real_available_cash)

    def getvalue(self):
        """
        兼容 Backtrader 标准接口: 获取账户总权益 (NetLiquidation)
        """
        # 明确获取净清算价值
        return self._fetch_smart_value(['NetLiquidation'])

    def get_pending_orders(self) -> list:
        """盈透：获取在途订单"""
        if not hasattr(self, 'ib') or not self.ib or not self.ib.isConnected():
            return []

        res = []
        try:
            open_trades = self.ib.openTrades()
            for t in open_trades:
                if t.orderStatus:
                    rem = t.orderStatus.remaining
                    if rem > 0:
                        res.append({
                            # 从 Trade 对象中提取 contract 和 order 信息
                            'symbol': t.contract.symbol,
                            'direction': 'BUY' if t.order.action == 'BUY' else 'SELL',
                            'size': rem
                        })
        except Exception as e:
            print(f"[IBBroker] 获取在途订单失败: {e}")
        return res

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

    # 1. 查钱 (重构为通用方法，支持指定 Tag)
    def _fetch_smart_value(self, target_tags=None) -> float:
        """
        获取账户特定价值（如现金或净值），支持多币种自动加总并统一转换为 USD。
        修复了因单一币种（如USD）为负债时，忽略其他币种正资产的问题。
        """
        if not hasattr(self, 'ib') or not self.ib: return 0.0

        in_loop = False
        try:
            if asyncio.get_running_loop():
                in_loop = True
        except RuntimeError:
            pass

        tags_priority = target_tags if target_tags else ['NetLiquidation', 'TotalCashValue', 'AvailableFunds']

        # 尝试获取账户数据源
        source_data = []
        if not in_loop:
            try:
                source_data = self.ib.accountSummary()
                if not source_data:
                    self.ib.sleep(0.5)
                    source_data = self.ib.accountSummary()
            except Exception:
                pass

        # 兜底到 accountValues
        if not source_data:
            try:
                source_data = self.ib.accountValues()
            except:
                pass
            if not source_data: return 0.0

        for tag in tags_priority:
            # 提取该 tag 下所有的币种记录 (排除 BASE，由我们自己精准换算 USD)
            items = [v for v in source_data if v.tag == tag and v.currency and v.currency != 'BASE']
            if not items:
                continue

            total_usd = 0.0
            found_valid = False

            for item in items:
                try:
                    val = float(item.value)
                    # 忽略为0的货币项 (除非是查净值)
                    if val == 0 and tag != 'NetLiquidation':
                        continue

                    if item.currency == 'USD':
                        total_usd += val
                        found_valid = True
                    else:
                        # --- 汇率转换逻辑 ---
                        pair_symbol = f"USD{item.currency}"
                        inverse_pair = False
                        if item.currency in ['EUR', 'GBP', 'AUD', 'NZD']:
                            pair_symbol = f"{item.currency}USD"
                            inverse_pair = True

                        ticker = self._fx_tickers.get(pair_symbol)
                        if not ticker:
                            contract = Forex(pair_symbol)
                            if not in_loop:
                                self.ib.qualifyContracts(contract)
                            ticker = self.ib.reqMktData(contract, '', False, False)
                            self._fx_tickers[pair_symbol] = ticker
                            if not in_loop:
                                start_wait = datetime.datetime.now()
                                while (datetime.datetime.now() - start_wait).total_seconds() < 1.0:
                                    self.ib.sleep(0.1)
                                    if self._extract_rate_from_ticker(ticker) > 0:
                                        break

                        exchange_rate = self._extract_rate_from_ticker(ticker)

                        # LKGR 和 历史兜底
                        if not (exchange_rate > 0):
                            if pair_symbol in self._last_valid_fx_rates:
                                exchange_rate = self._last_valid_fx_rates[pair_symbol]
                            else:
                                if not in_loop:
                                    try:
                                        bars = self.ib.reqHistoricalData(
                                            Forex(pair_symbol), endDateTime='', durationStr='2 D',
                                            barSizeSetting='1 day', whatToShow='MIDPOINT', useRTH=True
                                        )
                                        if bars: exchange_rate = bars[-1].close
                                    except:
                                        pass

                        if exchange_rate > 0:
                            self._last_valid_fx_rates[pair_symbol] = exchange_rate
                            if inverse_pair:
                                total_usd += val * exchange_rate
                            else:
                                total_usd += val / exchange_rate
                            found_valid = True
                        else:
                            if val != 0:
                                print(f"[IB Warning] 无法获取 {item.currency} 汇率, 金额 {val} 未计入。")
                except Exception:
                    continue

            # 只要在这个 tag 下成功计算了哪怕一个有效条目（即便加总是负数），都直接返回
            if found_valid:
                return total_usd

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
    def get_current_price(self, data):
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

            import time
            start_time = time.time()
            while time.time() - start_time < 1.0:
                self.ib.sleep(0.01)  # 允许较短的协作式让出
                if ticker.marketPrice() == ticker.marketPrice() and ticker.marketPrice() > 0:
                    break

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
            # 提取策略层命名中的基础代码 (例如将 'AAPL.SMART' 提取为 'AAPL')
            base_name = d._name.split('.')[0].upper()

            # 使用精确等于 (==) 而非包含 (in)
            if base_name == target_symbol.upper():
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

        host = config.IBKR_HOST
        port = config.IBKR_PORT
        client_id = config.IBKR_CLIENT_ID

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
                    print(f"[System] Connecting to IB Gateway ({host}:{port}) with clientId={client_id}...")
                    try:
                        ib.connect(host, port, clientId=client_id)
                        print("[System] ✅ Connected successfully.")
                    except Exception as e:
                        # 🔴 关键修复：使用 repr(e) 捕获空字面量异常
                        err_msg = repr(e)
                        print(f"[System] ⏳ Connection failed: {err_msg}")

                        # 幽灵占用与超时自愈逻辑
                        if "already in use" in err_msg or "326" in err_msg:
                            print(f"[System] 🔄 发现幽灵占用，坚持使用 client_id={client_id} 每 5 秒尝试抢占 Session...")
                            time.sleep(5)
                            continue

                        # 其他真网络错误保持较长的冷却
                        print("[System] ⏳ Retrying in 10s...")
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