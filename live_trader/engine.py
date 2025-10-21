import pandas as pd

from run import get_class_from_name
from .adapters.gm_broker import GmBrokerAdapter, GmDataProvider

ADAPTERS = {'gm': {'broker': GmBrokerAdapter, 'data_provider': GmDataProvider}}


class LiveTrader:
    """实盘交易引擎"""

    def __init__(self, config: dict):
        self.user_config = config
        platform = config.get('platform', 'gm')
        adapter_map = ADAPTERS.get(platform)
        if not adapter_map: raise ValueError(f"Unsupported platform: {platform}")

        self.data_provider = adapter_map['data_provider']()
        self.BrokerClass = adapter_map['broker']
        self.strategy_class = None
        self.selector_class = None
        self.strategy = None
        self.broker = None
        self.config = None

    def init(self, context):
        print("--- LiveTrader Engine Initializing ---")

        # 1. 【核心改动】静态调用 is_live_mode 来判断模式
        is_live = self.BrokerClass.is_live_mode(context)

        # 2. 根据模式决定配置合并策略
        if is_live:
            print("[Engine] Live Trading Mode Detected.")
            platform_config = {}
        else:
            print("[Engine] Platform Backtest Mode Detected.")
            platform_config = self.BrokerClass.extract_run_config(context)

        # 合并配置：平台配置为默认，用户配置有更高优先级
        self.config = {**platform_config, **self.user_config}
        print("[Engine] Effective configuration:", self.config)

        # 3. 使用最终配置实例化所有组件
        self.strategy_class = get_class_from_name(self.config['strategy_name'], ['strategies'])
        if self.config.get('selection_name'):
            self.selector_class = get_class_from_name(self.config['selection_name'], ['stock_selectors'])

        # 后续流程使用 self.config
        self.broker = self.BrokerClass(context, cash_override=self.config.get('cash'),
                                       commission_override=self.config.get('commission'))
        symbols = self._determine_symbols()
        if not symbols: raise ValueError("No symbols to trade.")

        # 4. 传入 is_live 标志来获取数据
        datas = self._fetch_all_history_data(symbols, context, is_live=is_live)
        self.broker.set_datas(list(datas.values()))
        params = self.config.get('params', {})
        self.strategy = self.strategy_class(broker=self.broker, params=params)
        self.strategy.init()
        print("--- LiveTrader Engine Initialized Successfully ---")

    def run(self, context):
        print(f"--- LiveTrader Running at {context.now.strftime('%Y-%m-%d %H:%M:%S')} ---")
        self.broker.set_datetime(context.now)
        self.strategy.next()

        if self.strategy.order:
            print("[Engine] New order created by strategy. Notifying...")
            self.strategy.notify_order(self.strategy.order)

        print("--- LiveTrader Run Finished ---")

    def _determine_symbols(self) -> list:
        """根据最终配置决定交易的标的列表"""
        if self.selector_class:
            selector_instance = self.selector_class(data_manager=None)
            symbols = selector_instance.run_selection()
            print(f"Selector selected symbols: {symbols}")
            return symbols
        return self.config.get('symbols', [])

    def _fetch_all_history_data(self, symbols: list, context, is_live: bool) -> dict:
        """根据模式获取数据：实盘模式获取预热数据，回测模式获取全部历史"""
        datas = {}

        if is_live:
            # 实盘模式: 仅获取最近的预热数据，用于计算指标
            end_date = context.now.strftime('%Y-%m-%d')
            # 默认一个慷慨的预热期(约2年)以适应各种长周期指标，无需用户配置
            start_date = (context.now - pd.Timedelta(days=730)).strftime('%Y-%m-%d')
            print(f"[Engine] Live mode data fetch (warm-up): from {start_date} to {end_date}")
        else:
            # 平台回测模式: 使用配置的完整时间段
            start_date = self.config.get('start_date')
            end_date = self.config.get('end_date')
            print(f"[Engine] Backtest mode data fetch: from {start_date} to {end_date}")

        for symbol in symbols:
            df = self.data_provider.get_history(symbol, start_date, end_date)
            if df is not None and not df.empty:
                class DataFeedProxy:
                    def __init__(self, df, name):
                        self.p = type('Params', (), {'dataname': df})()
                        self._name = name

                datas[symbol] = DataFeedProxy(df, symbol)
        return datas
