import pandas as pd
import datetime
import asyncio
from ib_insync import IB, Stock, MarketOrder, LimitOrder, OrderStatus, Trade, Forex, Contract

from .base_broker import BaseLiveBroker, BaseOrderProxy
from data_providers.base_provider import BaseDataProvider
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


class IBDataProvider(BaseDataProvider):
    """IBKR 数据源 (用于获取历史数据)"""

    def __init__(self, ib_instance: IB):
        self.ib = ib_instance

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None,
                 timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:

        contract = IBBrokerAdapter.parse_contract(symbol)

        # 映射 timeframe 到 IB 格式
        bar_size_setting = '1 day'
        duration_str = '1 Y'  # 默认请求一年

        if timeframe == 'Minutes':
            bar_size_setting = f'{compression} min'
            duration_str = '1 W'

        # 注意：IBKR获取历史数据是异步的，这里如果必须同步返回，需要利用 ib.run()
        # 但由于 Engine 初始化时 asyncio loop 可能还没完全跑起来，这部分比较棘手。
        # 在实盘中，建议还是用 CSVProvider 做预热，或者在这里做一个简单的阻塞等待。

        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=duration_str,
            barSizeSetting=bar_size_setting,
            whatToShow='TRADES',
            useRTH=True
        )

        if not bars:
            return None

        # 转换为 DataFrame
        df = pd.DataFrame(bars)
        df['datetime'] = pd.to_datetime(df['date'])
        df.set_index('datetime', inplace=True)
        # 重命名列以符合框架标准
        df = df[['open', 'high', 'low', 'close', 'volume']]

        # 时区处理：IB 返回的数据通常带时区，转为 Naive
        if df.index.tz is not None:
            df.index = df.index.tz_convert(None)

        return df


class IBBrokerAdapter(BaseLiveBroker):
    """Interactive Brokers 适配器"""

    def __init__(self, context, cash_override=None, commission_override=None):
        super().__init__(context, cash_override, commission_override)
        # 从 context 中获取由 launch 注入的 ib 实例
        self.ib: IB = getattr(context, 'ib_instance', None)
        self._tickers = {}  # 缓存实时行情 snapshot

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
        if not self.ib: return 0.0
        # 查找基准货币的 CashBalance
        tags = [v for v in self.ib.accountValues() if v.tag == 'CashBalance' and v.currency == 'USD']  # 假设账户基准是 USD
        if tags:
            return float(tags[0].value)
        # 备选：TotalCashBalance
        return 0.0

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
    def _get_current_price(self, data) -> float:
        if data._name in self._tickers:
            ticker = self._tickers[data._name]
            # 优先取最后价，如果没有则取买一卖一中间价
            price = ticker.last if ticker.last and ticker.last > 0 else ticker.marketPrice()
            # print(f"[IB Price] {data._name}: {price}")
            return price
        return 0.0

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

        trade = self.ib.placeOrder(contract, order)
        return IBOrderProxy(trade, data=data)

    # 5. IB 特有的启动协议
    @classmethod
    def launch(cls, conn_cfg: dict, strategy_path: str, params: dict, **kwargs):
        """
        IBKR 全天候启动入口
        """
        host = conn_cfg.get('host', '127.0.0.1')
        port = int(conn_cfg.get('port', 7497))  # 7497 paper, 4001 gateway
        client_id = int(conn_cfg.get('client_id', 1))

        symbols = kwargs.get('symbols', [])

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

        ctx = Context()

        # 初始化 Engine
        import config
        from live_trader.engine import LiveTrader, on_order_status_callback

        engine_config = config.__dict__.copy()
        engine_config['strategy_name'] = strategy_path
        engine_config['params'] = params
        engine_config['platform'] = 'ib'  # 标记平台
        engine_config['symbols'] = symbols

        trader = LiveTrader(engine_config)
        trader.init(ctx)

        # 订阅行情 (关键步骤)
        print("[IB] Requesting Market Data subscriptions...")
        active_tickers = {}
        for sym in symbols:
            contract = cls.parse_contract(sym)
            # 必须 qualify contract 才能获得正确的数据
            ib.qualifyContracts(contract)
            ticker = ib.reqMktData(contract, '', False, False)
            active_tickers[sym] = ticker

        # 将 ticker 引用传递给 broker 实例，以便 _get_current_price 读取
        trader.broker._tickers = active_tickers

        # 注册订单回调
        def on_trade_update(trade):
            # 将 IB trade 转换为 OrderProxy 并通知 Engine
            # 注意：这里需要从 symbols 反推 data 对象，或者简化处理
            # 这里的逻辑与 gm_broker 的 callback 类似
            # 由于 ib_insync 会频繁推送状态，Engine 内部最好有防抖或状态判断

            # 为了简单，我们手动触发一个简单的对象封装
            # 实际生产中，IBOrderProxy 最好能直接包装 Trade 对象
            on_order_status_callback(ctx, trade.order)  # 这里偷懒传了 order，实际回调需要适配

        ib.tradeEvent += on_trade_update

        # 主循环
        print("[IB] Starting Event Loop (All-Weather Mode)...")
        last_check = datetime.datetime.now()

        try:
            while ib.isConnected():
                # 1. 驱动 IB 事件循环
                ib.sleep(1)  # 休眠1秒，允许后台线程处理数据

                # 2. 定时运行 Engine 逻辑 (模拟 Bar 事件)
                now = datetime.datetime.now()
                ctx.now = pd.Timestamp(now)

                # 假设每分钟运行一次策略 (或根据 schedule)
                # 全天候模式下，应该判断当前是否在交易时间，但简单起见我们一直轮询
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