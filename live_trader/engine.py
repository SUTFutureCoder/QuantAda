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

        # 合并配置：平台配置为默认，用户配置有更高优先级
        platform_config = self.BrokerClass.extract_run_config(context)
        self.config = {**platform_config, **self.user_config}

        print("[Engine] Effective configuration:", self.config)

        # 使用合并后的最终配置来加载类
        self.strategy_class = get_class_from_name(self.config['strategy_name'], ['strategies'])
        if self.config.get('selection_name'):
            self.selector_class = get_class_from_name(self.config['selection_name'], ['stock_selectors'])

        # 后续流程使用 self.config
        self.broker = self.BrokerClass(context, cash_override=self.config.get('cash'), commission_override=self.config.get('commission'))
        symbols = self._determine_symbols()
        if not symbols: raise ValueError("No symbols to trade.")

        datas = self._fetch_all_history_data(symbols, context)
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

    def _fetch_all_history_data(self, symbols: list, context) -> dict:
        """为所有标的获取历史数据并创建代理对象"""
        datas = {}
        start_date = self.config.get('start_date')
        end_date = self.config.get('end_date')

        for symbol in symbols:
            df = self.data_provider.get_history(symbol, start_date, end_date)
            if df is not None and not df.empty:
                class DataFeedProxy:
                    def __init__(self, df, name):
                        self.p = type('Params', (), {'dataname': df})()
                        self._name = name

                datas[symbol] = DataFeedProxy(df, symbol)
        return datas