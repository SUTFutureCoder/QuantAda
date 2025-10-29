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
        self.risk_control = None

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

        # 5. 加载风控模块 ---
        self.risk_control = None
        risk_name = self.config.get('risk')  # 对应 run.py 的 --risk
        risk_params = self.config.get('risk_params', {})  # 对应 --risk_params

        if risk_name:
            try:
                print(f"[Engine] Loading Risk Control: {risk_name}")
                # 确保搜索 'risk_controls' 目录
                risk_control_class = get_class_from_name(risk_name, ['risk_controls', 'strategies'])
                self.risk_control = risk_control_class(broker=self.broker, params=risk_params)
                print("[Engine] Risk Control loaded successfully.")
            except Exception as e:
                print(f"Warning: Failed to load risk control '{risk_name}'. Error: {e}")
                self.risk_control = None

        print("--- LiveTrader Engine Initialized Successfully ---")

    def run(self, context):
        print(f"--- LiveTrader Running at {context.now.strftime('%Y-%m-%d %H:%M:%S')} ---")
        self.broker.set_datetime(context.now)

        # 顶层异常捕获，防止策略因单次错误而崩溃
        try:
            # 1. 检查策略是否有挂单
            strategy_order = getattr(self.strategy, 'order', None)

            if strategy_order:
                print("[Engine] Strategy has a pending order. Notifying and skipping logic.")
                if self.risk_control:
                    self.risk_control.notify_order(strategy_order)
                self.strategy.notify_order(strategy_order)
                print("--- LiveTrader Run Finished (Pending Order) ---")
                return

            # 2. 执行风控检查
            if self.risk_control and self._check_risk_controls():
                # 风控已触发平仓
                print("[Engine] Risk control triggered an exit. Skipping strategy.next().")
                # 检查风控是否设置了 self.strategy.order
                risk_order = getattr(self.strategy, 'order', None)
                if risk_order:
                    print("[Engine] Notifying about risk-triggered order...")
                    if self.risk_control:
                        self.risk_control.notify_order(risk_order)
                    self.strategy.notify_order(risk_order)
                print("--- LiveTrader Run Finished (Risk Triggered) ---")
                return

            # 3. 执行策略的 'next'
            self.strategy.next()

            # 4. 通知策略的新订单
            strategy_order = getattr(self.strategy, 'order', None)  # 重新获取，策略可能已创建新订单
            if strategy_order:
                print("[Engine] New order created by strategy. Notifying...")
                if self.risk_control:
                    self.risk_control.notify_order(strategy_order)
                self.strategy.notify_order(strategy_order)

        except Exception as e:
            # 捕获所有异常，打印错误，然后安全退出当前bar
            # 这样策略在下一个bar才能继续运行
            self.broker.log(f"CRITICAL ERROR in engine.run: {e}", dt=context.now)
            import traceback
            self.broker.log(traceback.format_exc())
            # 即使出错，也打印 "Finished"，表示此bar安全退出

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

    # 风控检查辅助方法
    def _check_risk_controls(self) -> bool:
        """
        辅助方法：执行风控检查并采取行动。
        :return: True 如果风控被触发并执行了平仓, False 否则。
        """
        current_dt = self.broker.datetime.datetime()  # 获取当前模拟时间

        for data_feed in self.broker.datas:  # data_feed 是 DataFeedProxy
            df = data_feed.p.dataname
            feed_proxy = None
            try:
                # 查找当前时间或之前的最新K线数据
                current_bar_data = df.loc[df.index <= current_dt]
                if current_bar_data.empty:
                    continue

                # 获取最新的K线
                bar = current_bar_data.iloc[-1]

                # 创建一个模拟 backtrader feed 的代理对象，以支持 data.close[0] 访问
                class BtFeedProxy:
                    def __init__(self, name, bar_data):
                        self._name = name
                        # 仅支持 [0] 索引，返回当前K线的值
                        self.close = {0: bar_data['close']}
                        self.open = {0: bar_data['open']}
                        self.high = {0: bar_data['high']}
                        self.low = {0: bar_data['low']}

                feed_proxy = BtFeedProxy(data_feed._name, bar)

            except Exception as e:
                self.broker.log(f"[Risk] Error creating bar proxy for {data_feed._name}: {e}")
                continue

            # 1. 检查是否有仓位
            position = self.broker.getposition(data_feed)
            data_name = data_feed._name

            if not position.size:
                # 如果没有仓位，清理风控的"已触发"状态 (模拟notify_trade)
                if hasattr(self.risk_control, 'exit_triggered') and data_name in self.risk_control.exit_triggered:
                    self.broker.log(f"[Risk] Clearing trigger for {data_name} as position is zero.")
                    self.risk_control.exit_triggered.remove(data_name)
                continue

            # 仓位存在
            # 2. 模拟 notify_trade(trade.is_open) 逻辑：
            # 如果仓位存在，但风控模块认为它已被触发（例如，上一个平仓单尚未成交）
            # 我们需要一种方式来重置它，以防这是一个*新*的开仓。
            if hasattr(self.risk_control, 'exit_triggered') and data_name in self.risk_control.exit_triggered:
                # 这是一个HACK：我们检查当前价格是否已*脱离*止损区
                # 如果脱离了，我们假定这是一个新仓，并清除触发器
                entry_price = position.price
                current_price = feed_proxy.close[0]
                stop_loss_pct = getattr(self.risk_control.p, 'stop_loss_pct', 0)

                if stop_loss_pct > 0:
                    stop_loss_price = entry_price * (1 - stop_loss_pct)
                    if current_price > stop_loss_price:
                        self.broker.log(
                            f"[Risk] Position {data_name} exists and is not stopped. Clearing stale trigger.")
                        self.risk_control.exit_triggered.remove(data_name)
                    else:
                        # 触发器仍然有效（价格仍在止损区或更低）
                        self.broker.log(f"[Risk] Trigger for {data_name} is active. Waiting for exit.")
                        continue  # 跳过检查，等待平仓单成交
                else:
                    # 止损未启用，也清除触发器
                    self.risk_control.exit_triggered.remove(data_name)

            # 3. 对持仓标的执行风控检查
            action = self.risk_control.check(feed_proxy)

            # 4. 如果触发平仓
            if action == 'SELL':
                self.broker.log(f"Risk module triggered SELL for {data_feed._name}")

                # 执行平仓
                order = self.broker.order_target_percent(data=data_feed, target=0.0)

                # 将订单句柄存入策略的 'order' 属性，以实现锁
                if hasattr(self.strategy, 'order'):
                    self.strategy.order = order

                return True  # 风控已触发，停止检查并返回True

        return False  # 所有标的检查完毕，未触发风控