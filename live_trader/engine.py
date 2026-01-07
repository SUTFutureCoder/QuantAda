import pandas as pd
from types import SimpleNamespace

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

        # 获取 timeframe 和 compression
        timeframe = self.config.get('timeframe', 'Days')
        compression = self.config.get('compression', 1)
        print(f"[Engine] Using timeframe: {compression} {timeframe}")

        # 4. 传入 is_live 标志来获取数据
        datas = self._fetch_all_history_data(symbols, context, is_live=is_live, timeframe=timeframe, compression=compression)
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
            # --- 实盘数据热更新逻辑 ---
            # 只有在实盘模式下，每次 schedule 触发 run 时，才需要重新拉取数据
            if self.broker.is_live:
                print("[Engine] Live Mode: Refreshing data...")
                self._refresh_live_data(context)

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

    def _fetch_all_history_data(self, symbols: list, context, is_live: bool,
                                timeframe: str, compression: int) -> dict:
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
            df = self.data_provider.get_history(symbol, start_date, end_date,
                                                timeframe=timeframe, compression=compression)
            if df is not None and not df.empty:
                class DataFeedProxy:
                    def __init__(self, df, name):
                        self.p = SimpleNamespace(dataname=df)
                        self._name = name

                datas[symbol] = DataFeedProxy(df, symbol)
        return datas

    def _refresh_live_data(self, context):
        """
        实盘数据刷新
        重新获取包含最新 K 线的数据，并【原地更新】策略中的 DataFeed 对象
        """
        # 获取配置
        timeframe = self.config.get('timeframe', 'Days')
        compression = self.config.get('compression', 1)

        # 重新计算时间窗口 (Warmup ~ Now)
        end_date = context.now.strftime('%Y-%m-%d')
        # 保持与 init 一致的预热长度
        start_date = (context.now - pd.Timedelta(days=730)).strftime('%Y-%m-%d')

        # 遍历 Broker 中已有的 DataFeed
        for data_feed in self.broker.datas:
            symbol = data_feed._name

            # 重新拉取数据
            new_df = self.data_provider.get_history(symbol, start_date, end_date,
                                                    timeframe=timeframe, compression=compression)

            if new_df is not None and not new_df.empty:
                # 原地更新：不创建新对象，而是替换对象内部的 DataFrame
                # 这样 self.strategy.datas 中的引用会自动指向新数据
                # 假设 DataFeedProxy 使用 .p.dataname 存储数据 (参考 _fetch_all_history_data)
                if hasattr(data_feed, 'p') and hasattr(data_feed.p, 'dataname'):
                    data_feed.p.dataname = new_df
                    print(f"  Data refreshed for {symbol}: {len(new_df)} bars (Last: {new_df.index[-1]})")
                else:
                    print(f"  Warning: Cannot update data for {symbol}. Structure mismatch.")
            else:
                print(f"  Warning: No new data fetched for {symbol} during refresh.")

    # 风控检查辅助方法
    def _check_risk_controls(self) -> bool:
        current_dt = self.broker.datetime.datetime()
        triggered_action = False

        # 1. 初始化风控订单跟踪字典 (如果尚未存在)
        if not hasattr(self, '_pending_risk_orders'):
            self._pending_risk_orders = {}

        for data_feed in self.broker.datas:
            data_name = data_feed._name

            # --- A. 仓位检查与状态重置 ---
            # 无论之前状态如何，只要当前仓位为 0，就说明风控已完成或无风险
            position = self.broker.getposition(data_feed)

            if not position.size:
                # 如果有遗留的风控状态，清理掉
                if data_name in self._pending_risk_orders:
                    self.broker.log(f"[Risk] Position is closed for {data_name}. Clearing pending risk status.")
                    del self._pending_risk_orders[data_name]

                # 同步清理 risk_control 内部可能存在的标记 (兼容旧有的 exit_triggered 逻辑)
                if hasattr(self.risk_control, 'exit_triggered') and isinstance(self.risk_control.exit_triggered, set):
                    if data_name in self.risk_control.exit_triggered:
                        self.risk_control.exit_triggered.remove(data_name)
                continue

            # --- B. 检查是否存在正在进行的风控订单 (异步处理核心) ---
            if data_name in self._pending_risk_orders:
                pending_order = self._pending_risk_orders[data_name]

                # 使用 BaseOrderProxy 的标准接口检查状态
                # 注意：这里依赖 callback 或 broker 自动更新 pending_order 对象的内部状态

                if pending_order.is_pending() or pending_order.is_accepted():
                    # 关键点：订单正在交易所排队或处理中。
                    # 此时绝对不能再次发送订单，也不能清除状态。
                    self.broker.log(f"[Risk] Pending exit order for {data_name} is active. Waiting for execution...")
                    triggered_action = True  # 标记为 True，告诉 engine 跳过 strategy.next()
                    continue

                elif pending_order.is_completed():
                    # 订单已完成
                    # 理论上仓位会在下一次循环被判定为 0，从而走入 A 步骤清理状态。
                    # 这里先移除追踪，允许逻辑继续
                    self.broker.log(f"[Risk] Exit order for {data_name} reported Completed.")
                    del self._pending_risk_orders[data_name]
                    # 不 return，允许本次循环继续检查（双重保险）

                elif pending_order.is_rejected() or pending_order.is_canceled():
                    # 订单失败或被撤销
                    # 清除状态，这样下一行代码就会重新执行 risk_control.check()
                    # 从而尝试再次发起平仓
                    self.broker.log(
                        f"[Risk] Exit order for {data_name} failed (Rejected/Canceled). Resetting to retry.")
                    del self._pending_risk_orders[data_name]

                else:
                    # 其他未知状态，保守起见视为 Pending
                    triggered_action = True
                    continue

            # --- C. 准备数据代理 (保持原逻辑) ---
            feed_proxy = None
            try:
                df = data_feed.p.dataname
                # 优化：实盘模式下，DataFrame 的最后一行通常即为最新数据
                # 依然做时间过滤以防万一
                current_bar_data = df.loc[df.index <= current_dt]
                if current_bar_data.empty:
                    continue

                bar = current_bar_data.iloc[-1]

                class BtFeedProxy:
                    def __init__(self, name, bar_data):
                        self._name = name
                        self.close = {0: bar_data['close']}
                        self.open = {0: bar_data['open']}
                        self.high = {0: bar_data['high']}
                        self.low = {0: bar_data['low']}
                        # 补充 datetime，部分风控指标可能需要时间
                        self.datetime = {0: pd.Timestamp(bar_data.name)}

                feed_proxy = BtFeedProxy(data_feed._name, bar)

            except Exception as e:
                self.broker.log(f"[Risk] Error creating bar proxy for {data_feed._name}: {e}")
                continue

            # --- D. 执行风控逻辑检查 ---
            # 只有在当前该标的没有 Pending 订单时，才执行检查
            if data_name not in self._pending_risk_orders:
                action = self.risk_control.check(feed_proxy)

                if action == 'SELL':
                    self.broker.log(f"Risk module triggered SELL for {data_feed._name}")

                    # 执行平仓
                    order = self.broker.order_target_percent(data=data_feed, target=0.0)

                    if order:
                        # 记录订单对象，而不是依赖价格
                        self._pending_risk_orders[data_name] = order

                        # 同步给策略（可选，保持兼容性）
                        if hasattr(self.strategy, 'order'):
                            self.strategy.order = order

                        triggered_action = True

                        # 记录到 risk_control 内部 (兼容)
                        if hasattr(self.risk_control, 'exit_triggered') and isinstance(self.risk_control.exit_triggered,
                                                                                       set):
                            self.risk_control.exit_triggered.add(data_name)
                    else:
                        self.broker.log(f"[Risk] Error: Failed to submit sell order for {data_name}")

        return triggered_action

def on_order_status_callback(context, order):
    """
    掘金实盘的订单状态回调函数。
    当订单状态发生变化（部成、全成、撤单、废单）时触发。
    """
    if hasattr(context, 'strategy_instance') and context.strategy_instance:
        try:
            from .adapters.gm_broker import GmOrderProxy

            # 不再依赖 context.mode，而是直接问 broker
            # 之前的逻辑可能导致 is_live=False，从而把 PendingNew (status 10) 误判为 Completed
            broker = context.strategy_instance.broker
            is_live = broker.is_live if hasattr(broker, 'is_live') else True

            # 反向查找 Data Feed 对象
            # 策略中存储的 self.orders 的 key 是 data 对象，而不是 symbol 字符串
            # 所以我们必须在这里找到 data 对象，赋值给 Proxy
            target_symbol = order.symbol
            matched_data = None

            if hasattr(broker, 'datas'):
                for d in broker.datas:
                    # 在 engine.py 中创建的 DataFeedProxy 有 _name 属性
                    if hasattr(d, '_name') and d._name == target_symbol:
                        matched_data = d
                        break

            # 将掘金的原生 order 包装成 Proxy，并传入 data
            order_proxy = GmOrderProxy(order, is_live, data=matched_data)

            # 调用策略通知
            context.strategy_instance.notify_order(order_proxy)

            # 安全访问 statusMsg，防止 AttributeError
            # 掘金 order 对象可能是动态属性，statusMsg 不一定存在
            msg = getattr(order, 'statusMsg', None)
            if not msg:
                msg = getattr(order, 'ord_rej_reason_detail', '')  # 尝试获取拒单原因

            print(f"[Engine Callback] Notified strategy of order status: {order.status} ({msg})")

        except Exception as e:
            # 打印完整的堆栈以便调试
            import traceback
            print(f"[Engine Callback Error] Failed to notify strategy: {e}")
            traceback.print_exc()
    else:
        print("[Engine Callback Warning] No strategy_instance found in context.")
