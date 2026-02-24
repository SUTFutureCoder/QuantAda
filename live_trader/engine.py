import importlib
import inspect
import sys
import time
import traceback
from types import SimpleNamespace

import pandas as pd

import config
from alarms.manager import AlarmManager
from data_providers.base_provider import BaseDataProvider
from data_providers.manager import DataManager
from live_trader.adapters.base_broker import BaseLiveBroker
from run import get_class_from_name


class LiveTrader:
    """实盘交易引擎"""

    def __init__(self, config: dict):
        self.user_config = config
        platform = config.get('platform', 'gm')

        # 2. 动态加载对应的 Broker 和 DataProvider 类
        print(f"[Engine] Loading adapter for platform: {platform}...")
        self.BrokerClass, DataProviderClass = self._load_adapter_classes(platform)

        # 3. 实例化组件
        self.data_provider = DataProviderClass()

        self.strategy_class = None
        self.selector_class = None
        self.strategy = None
        self.broker = None
        self.config = None
        self.risk_control = None
        self._data_manager = None
        self._resolved_symbols = None

    def _load_adapter_classes(self, platform: str):
        """
        根据平台名称动态加载对应的模块和类
        约定: platform='ib' -> 模块='live_trader.adapters.ib_broker'
        """
        # 处理模块名称约定 (例如 ib -> ib_broker)
        module_name = platform if platform.endswith('_broker') else f"{platform}_broker"

        try:
            # 动态导入模块 (相对于当前包)
            # 注意：engine.py 位于 live_trader 包下，adapters 也是同级子包
            module_path = f".adapters.{module_name}"
            mod = importlib.import_module(module_path, package=__package__)
        except ImportError as e:
            raise ValueError(
                f"无法加载平台 '{platform}' 的适配器模块 ({module_name}.py)。请确保文件存在于 adapters 目录下。\n错误信息: {e}")

        broker_cls = None
        provider_cls = None

        # 遍历模块成员，自动查找符合条件的类
        # 过滤条件: 必须是定义在该模块中的类 (排除 import 进来的)，且继承自基类
        for name, obj in inspect.getmembers(mod, inspect.isclass):
            # 查找 Broker 类
            if issubclass(obj, BaseLiveBroker) and obj is not BaseLiveBroker:
                # 确保只加载当前模块定义的，防止加载了从其他地方 import 的基类
                if obj.__module__ == mod.__name__:
                    broker_cls = obj

            # 查找 DataProvider 类
            # 注意：有些 Provider 可能是在 adapter 文件中定义的，也可能是 import 的
            # 这里我们放宽限制，只要该模块有这个类且符合接口即可
            if issubclass(obj, BaseDataProvider) and obj is not BaseDataProvider:
                if obj.__module__ == mod.__name__:
                    provider_cls = obj

        if not broker_cls:
            raise ValueError(f"在模块 {module_name} 中未找到继承自 BaseLiveBroker 的类。")

        if not provider_cls:
            # 如果 adapter 文件中没有定义 Provider，尝试容错或使用通用 Provider
            # 但通常我们要求 Adapter 必须提供配套的数据源封装
            raise ValueError(f"在模块 {module_name} 中未找到继承自 BaseDataProvider 的类。")

        print(f"[Engine] Adapter loaded: Broker={broker_cls.__name__}, Provider={provider_cls.__name__}")
        return broker_cls, provider_cls

    def init(self, context):
        print("--- LiveTrader Engine Initializing ---")

        # 初始化报警器
        self.alarm_manager = AlarmManager()

        # 1. 静态调用 is_live_mode 来判断模式
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

        # 从掘金 context 中动态提取 run() 方法传入的 token 并注入到 data_provider
        if hasattr(context, 'token') and context.token:
            # 掘金 SDK 内部的 context.token 默认带有 'bearer ' 前缀，需要清洗掉，否则 set_token 会报错
            raw_token = context.token
            if isinstance(raw_token, str) and raw_token.lower().startswith('bearer '):
                raw_token = raw_token[7:].strip()

            self.config['token'] = raw_token

            # 覆盖 DataProvider 的外部模式状态，使其获得正确的 Token
            if getattr(self.data_provider, 'is_external_mode', False) or getattr(self.data_provider, 'token',
                                                                                 None) == 'EXTERNAL_MODE':
                self.data_provider.token = raw_token
                self.data_provider.is_external_mode = False
                try:
                    from gm.api import set_token
                    set_token(raw_token)
                    print(f"[Engine] Token correctly loaded and cleaned from context: {raw_token[:6]}***")
                except ImportError:
                    pass

        # 3. 使用最终配置实例化所有组件
        self.strategy_class = get_class_from_name(self.config['strategy_name'], ['strategies'])
        if self.config.get('selection_name'):
            self.selector_class = get_class_from_name(self.config['selection_name'], ['stock_selectors'])

        # 后续流程使用 self.config
        self.broker = self.BrokerClass(context, cash_override=self.config.get('cash'),
                                       commission_override=self.config.get('commission'),
                                       slippage_override=self.config.get('slippage'))
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
        # 将环境级的豁免名单透传给策略
        if 'ignored_symbols' in self.config:
            params['env_ignored_symbols'] = self.config['ignored_symbols']
        self.strategy = self.strategy_class(broker=self.broker, params=params)
        self.strategy.init()

        # 发送启动死信/通知
        self.alarm_manager.push_start(self.config['strategy_name'])

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

            # 如果策略持有“虚拟延迟单”，但 Broker 的延迟队列已经被清空
            # 需要引入“宽限期”（Grace Period），防止交易所回调慢于本地队列清理导致重复下单 (Double Spend)
            if strategy_order and getattr(strategy_order, 'id', None) == "DEFERRED_VIRTUAL_ID":
                deferred_queue = getattr(self.broker, '_deferred_orders', [])
                if len(deferred_queue) == 0:
                    import time
                    current_time = time.time()

                    # 1. 如果这是首次发现队列为空，打上时间戳并开始等待
                    if not hasattr(self, '_deferred_empty_time'):
                        self._deferred_empty_time = current_time
                        print("[Engine] ⏳ 内部延迟队列已排空。正在等待柜台真实订单回调 (进入 5 秒宽限期)...")
                        # 强行返回，继续保持策略锁定，给网络回调一点时间
                        return

                    # 2. 检查宽限期是否超时 (例如设定为 5.0 秒，可根据券商网络状况调整)
                    grace_period = 5.0
                    if current_time - self._deferred_empty_time > grace_period:
                        print(f"[Engine] ⚠️ 宽限期 ({grace_period}s) 结束，未收到柜台回调。确认为僵尸单，强制复位状态！")
                        self.strategy.order = None
                        strategy_order = None
                        delattr(self, '_deferred_empty_time')  # 清除计时器
                    else:
                        # 还在宽限期内，继续静默等待
                        print(f"[Engine] ⏳ 宽限期等待中... (已等待 {current_time - self._deferred_empty_time:.1f}s)")
                        return
                else:
                    # 防御性逻辑：如果队列里又有了新的延迟单，重置并清除可能的倒计时
                    if hasattr(self, '_deferred_empty_time'):
                        delattr(self, '_deferred_empty_time')

            if strategy_order:
                print("[Engine] Strategy has a pending order. Notifying and skipping logic.")
                if self.risk_control:
                    self.risk_control.notify_order(strategy_order)
                self.strategy.notify_order(strategy_order)
                print("--- LiveTrader Run Finished (Pending Order) ---")
                return

            # 2. 执行风控检查
            if self.risk_control and self._check_risk_controls():
                print("[Engine] 🛡️ 发现风控动作。底层已自动物理上锁，策略流水线继续向下执行...")

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
            # 推送异常报警
            if hasattr(self, 'alarm_manager'):
                self.alarm_manager.push_exception("Engine Main Loop", e)

        print("--- LiveTrader Run Finished ---")

    def notify_order(self, order):
        """
        [系统回调入口] 接收来自底层 Broker 的订单状态更新，并转发给策略和风控。
        """
        # 1. 转发给用户策略 (最重要)
        if self.strategy:
            self.strategy.notify_order(order)

        # 2. 转发给风控模块 (如果有)
        if self.risk_control:
            self.risk_control.notify_order(order)

    def _determine_symbols(self) -> list:
        """根据最终配置决定交易的标的列表"""
        if self._resolved_symbols is not None:
            return list(self._resolved_symbols)

        symbols = []
        if self.selector_class:
            if self._data_manager is None:
                self._data_manager = DataManager()

            selector_instance = self.selector_class(data_manager=self._data_manager)
            raw_symbols = selector_instance.run_selection()

            if isinstance(raw_symbols, pd.DataFrame):
                symbols = [str(s) for s in raw_symbols.index.tolist()]
            elif isinstance(raw_symbols, (pd.Index, list, tuple, set)):
                symbols = [str(s) for s in raw_symbols]
            else:
                raise ValueError(
                    f"Selector '{self.selector_class.__name__}' returned unsupported type: {type(raw_symbols).__name__}"
                )
            print(f"Selector selected symbols: {symbols}")
        else:
            configured_symbols = self.config.get('symbols', [])
            if isinstance(configured_symbols, str):
                symbols = [configured_symbols]
            elif isinstance(configured_symbols, (list, tuple, set, pd.Index)):
                symbols = [str(s) for s in configured_symbols]
            else:
                raise ValueError(f"Unsupported symbols type in config: {type(configured_symbols).__name__}")

        # 去重并过滤空值，保持原始顺序
        deduped = []
        seen = set()
        for sym in symbols:
            sym_norm = str(sym).strip()
            if not sym_norm or sym_norm in seen:
                continue
            deduped.append(sym_norm)
            seen.add(sym_norm)

        self._resolved_symbols = deduped
        return list(self._resolved_symbols)

    def _fetch_all_history_data(self, symbols: list, context, is_live: bool,
                                timeframe: str, compression: int) -> dict:
        """根据模式获取数据：实盘模式获取预热数据，回测模式获取全部历史"""
        datas = {}

        if is_live:
            # 实盘模式: 仅获取最近的预热数据，用于计算指标
            if timeframe == 'Minutes':
                end_date = context.now.strftime('%Y-%m-%d %H:%M:%S')
            else:
                end_date = context.now.strftime('%Y-%m-%d')
            # 默认使用年交易日以适应各种长周期指标，无需用户配置
            if timeframe == 'Minutes':
                start_date = (context.now - pd.Timedelta(days=config.ANNUAL_FACTOR)).strftime('%Y-%m-%d %H:%M:%S')
            else:
                start_date = (context.now - pd.Timedelta(days=config.ANNUAL_FACTOR)).strftime('%Y-%m-%d')
            print(f"[Engine] Live mode data fetch (warm-up): from {start_date} to {end_date}")
        else:
            # 平台回测模式: 默认从 start_date 往前预热，保证长周期指标可用
            raw_start_date = self.config.get('start_date')
            end_date = self.config.get('end_date')
            start_date = raw_start_date

            warmup_days = config.ANNUAL_FACTOR
            if raw_start_date and warmup_days > 0:
                try:
                    warmup_start_ts = pd.to_datetime(raw_start_date) - pd.Timedelta(days=warmup_days)
                    if timeframe == 'Minutes':
                        start_date = warmup_start_ts.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        start_date = warmup_start_ts.strftime('%Y-%m-%d')

                    print(
                        "[Engine] Backtest mode data fetch "
                        f"(warm-up {warmup_days}d): from {start_date} to {end_date} (run starts at {raw_start_date})"
                    )
                except Exception as e:
                    start_date = raw_start_date
                    print(
                        f"[Engine Warning] Backtest warm-up failed for start_date={raw_start_date}: {e}. "
                        "Fallback to raw start_date."
                    )
                    print(f"[Engine] Backtest mode data fetch: from {start_date} to {end_date}")
            else:
                print(f"[Engine] Backtest mode data fetch: from {start_date} to {end_date}")

        for symbol in symbols:
            df = self.data_provider.get_history(symbol, start_date, end_date,
                                                timeframe=timeframe, compression=compression)
            if df is not None and not df.empty:
                class DataFeedProxy:
                    def __init__(self, df, name):
                        self.p = SimpleNamespace(dataname=df)
                        self._name = name

                    def __repr__(self):
                        return self._name

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
        now_ts = pd.Timestamp(context.now)
        end_date = now_ts.strftime('%Y-%m-%d %H:%M:%S') if timeframe == 'Minutes' else now_ts.strftime('%Y-%m-%d')

        def _align_to_index_tz(dt_input, index):
            dt = pd.Timestamp(dt_input)
            idx_tz = getattr(index, 'tz', None)
            if idx_tz is not None:
                if dt.tzinfo is None:
                    return dt.tz_localize(idx_tz)
                return dt.tz_convert(idx_tz)
            if dt.tzinfo is not None:
                return dt.tz_convert(None)
            return dt

        def _build_incremental_start(existing_df: pd.DataFrame) -> str:
            # 首次/空数据回退到预热窗口
            if existing_df is None or existing_df.empty:
                warmup_start = now_ts - pd.Timedelta(days=config.ANNUAL_FACTOR)
                return warmup_start.strftime('%Y-%m-%d %H:%M:%S') if timeframe == 'Minutes' else warmup_start.strftime('%Y-%m-%d')

            last_bar_ts = pd.Timestamp(existing_df.index[-1])
            if timeframe == 'Minutes':
                backoff = pd.Timedelta(minutes=max(1, int(compression)) * 3)
                start_ts = last_bar_ts - backoff
                return start_ts.strftime('%Y-%m-%d %H:%M:%S')

            # 日线模式保留少量回看，覆盖供应商延迟修订
            start_ts = last_bar_ts - pd.Timedelta(days=2)
            return start_ts.strftime('%Y-%m-%d')

        # 遍历 Broker 中已有的 DataFeed
        for data_feed in self.broker.datas:
            symbol = data_feed._name
            old_df = None
            if hasattr(data_feed, 'p') and hasattr(data_feed.p, 'dataname'):
                old_df = data_feed.p.dataname

            # 重新拉取数据
            start_date = _build_incremental_start(old_df)
            new_df = self.data_provider.get_history(symbol, start_date, end_date,
                                                    timeframe=timeframe, compression=compression)

            if new_df is not None and not new_df.empty:
                # 原地更新：不创建新对象，而是替换对象内部的 DataFrame
                # 这样 self.strategy.datas 中的引用会自动指向新数据
                # 假设 DataFeedProxy 使用 .p.dataname 存储数据 (参考 _fetch_all_history_data)
                if hasattr(data_feed, 'p') and hasattr(data_feed.p, 'dataname'):
                    if old_df is not None and not old_df.empty:
                        merged_df = pd.concat([old_df, new_df])
                        merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                        merged_df = merged_df.sort_index()

                        # 保持固定预热窗口，避免数据无限增长
                        cutoff_ts = now_ts - pd.Timedelta(days=config.ANNUAL_FACTOR)
                        cutoff_ts = _align_to_index_tz(cutoff_ts, merged_df.index)
                        merged_df = merged_df[merged_df.index >= cutoff_ts]
                        data_feed.p.dataname = merged_df
                        print(f"  Data refreshed for {symbol}: {len(merged_df)} bars (Last: {merged_df.index[-1]})")
                    else:
                        data_feed.p.dataname = new_df.sort_index()
                        print(f"  Data refreshed for {symbol}: {len(new_df)} bars (Last: {new_df.index[-1]})")
                else:
                    print(f"  Warning: Cannot update data for {symbol}. Structure mismatch.")
            else:
                print(f"  Warning: No new data fetched for {symbol} during refresh.")

    # 风控检查辅助方法
    def _check_risk_controls(self) -> bool:
        current_dt = self.broker.datetime.datetime()
        if hasattr(current_dt, 'tzinfo') and current_dt.tzinfo is not None:
            current_dt = current_dt.replace(tzinfo=None)
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

                # 仓位已清零，风控解除，解锁该标的，允许策略重新考察它
                if hasattr(self.broker, 'unlock_for_risk'):
                    self.broker.unlock_for_risk(data_name)

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

                # 终态优先判断，避免 is_accepted=True 的对象吞掉已撤单/拒单状态
                if pending_order.is_completed():
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

                elif pending_order.is_pending() or pending_order.is_accepted():
                    # 关键点：订单正在交易所排队或处理中。
                    # 此时绝对不能再次发送订单，也不能清除状态。
                    self.broker.log(f"[Risk] Pending exit order for {data_name} is active. Waiting for execution...")
                    triggered_action = True  # 标记为 True，告诉 engine 跳过 strategy.next()
                    continue

                else:
                    # 其他未知状态，保守起见视为 Pending
                    triggered_action = True
                    continue

            # --- C. 准备数据代理 (保持原逻辑) ---
            feed_proxy = None
            try:
                df = data_feed.p.dataname
                if df is None or df.empty:
                    continue

                current_dt_ts = pd.Timestamp(current_dt)
                idx_tz = getattr(df.index, 'tz', None)
                if idx_tz is not None:
                    if current_dt_ts.tzinfo is None:
                        current_dt_ts = current_dt_ts.tz_localize(idx_tz)
                    else:
                        current_dt_ts = current_dt_ts.tz_convert(idx_tz)
                elif current_dt_ts.tzinfo is not None:
                    current_dt_ts = current_dt_ts.tz_convert(None)

                # O(logN) 定位 <= 当前时间的最后一根K线，避免每次全表切片
                pos = df.index.searchsorted(current_dt_ts, side='right') - 1
                if pos < 0:
                    continue

                bar = df.iloc[pos]

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

                    # 底层物理上锁，瞬间切断策略层买入该标的的可能
                    if hasattr(self.broker, 'lock_for_risk'):
                        self.broker.lock_for_risk(data_name)

                    # 执行平仓
                    order = self.broker.order_target_percent(data=data_feed, target=0.0)

                    if order:
                        # 记录订单对象，而不是依赖价格
                        self._pending_risk_orders[data_name] = order

                        triggered_action = True

                        # 记录到 risk_control 内部 (兼容)
                        if hasattr(self.risk_control, 'exit_triggered') and isinstance(self.risk_control.exit_triggered,
                                                                                       set):
                            self.risk_control.exit_triggered.add(data_name)
                    else:
                        self.broker.log(f"[Risk] Error: Failed to submit sell order for {data_name}")

        return triggered_action

def on_order_status_callback(context, raw_order):
    """
    实盘的订单状态回调函数。
    当订单状态发生变化（部成、全成、撤单、废单）时触发。
    """
    # 获取报警器单例
    alarm_manager = AlarmManager()

    if hasattr(context, 'strategy_instance') and context.strategy_instance:
        try:
            # 1. 获取 Broker 抽象实例
            strategy = context.strategy_instance
            broker = strategy.broker

            # --- 关键修改点 ---
            # 不再 import GmOrderProxy，而是让 broker 自己去“装箱”
            # 也不需要在 engine 里去遍历 datas 找 matched_data，这也应该是 broker 的责任
            order_proxy = broker.convert_order_proxy(raw_order)

            # 2. 喂给 Broker，让它去维护在途单
            if hasattr(broker, 'on_order_status'):
                broker.on_order_status(order_proxy)

            # 调用策略通知
            strategy.notify_order(order_proxy)

            # 安全访问 statusMsg，防止 AttributeError
            # 掘金 order 对象可能是动态属性，statusMsg 不一定存在
            msg = getattr(raw_order, 'statusMsg', None)
            if not msg:
                msg = getattr(raw_order, 'ord_rej_reason_detail', '')  # 尝试获取拒单原因

            current_status = getattr(order_proxy, 'status', 'Unknown')
            print(f"[Engine Callback] Notified strategy of order status: {current_status} ({msg})")
            # 如果状态是 "已提交" 但还没 "成交"，且未被拒绝，则推送一条消息
            if current_status in ['PreSubmitted', 'Submitted', 'PendingSubmit']:
                # 为了防止刷屏，只有当成交量为0时才推送这个"提交确认"
                # (如果成交量>0，下面的成交逻辑会接管)
                if order_proxy.executed.size == 0:
                    # 尝试获取目标下单数量
                    total_qty = 0
                    # 针对 IB: trade.order.totalQuantity
                    if hasattr(order_proxy, 'trade') and hasattr(order_proxy.trade, 'order'):
                        total_qty = order_proxy.trade.order.totalQuantity
                    # 针对其他 Broker (通用回退)
                    elif hasattr(order_proxy, 'raw_order') and hasattr(order_proxy.raw_order, 'volume'):
                        total_qty = order_proxy.raw_order.volume
                    elif hasattr(order_proxy, 'platform_order') and hasattr(order_proxy.platform_order, 'volume'):
                        total_qty = order_proxy.platform_order.volume

                    action = "BUY" if order_proxy.is_buy() else "SELL"
                    symbol = order_proxy.data._name if order_proxy.data else "Unknown"

                    # 构造消息: ⏳ 代表等待/进行中
                    alarm_msg = f"⏳ 订单已提交 ({current_status}): {action} {total_qty} {symbol}"
                    # 使用 push_text 发送普通文本通知
                    alarm_manager.push_text(alarm_msg)


            # 报警通知
            # A. 交易成交推送 (完全成交、部分成交、以及部成后撤单)
            if order_proxy.executed.size > 0:
                # 排除已被拒绝的废单(虽然废单size通常为0，为了严谨双重检查)
                if not order_proxy.is_rejected():
                    trade_info = {
                        'symbol': order_proxy.data._name if order_proxy.data else "Unknown",
                        'action': 'BUY' if order_proxy.is_buy() else 'SELL',
                        'price': order_proxy.executed.price,
                        'size': order_proxy.executed.size,
                        'value': order_proxy.executed.value,
                        'dt': context.now.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    alarm_manager.push_trade(trade_info)

            # B. 异常状态推送 (拒单)
            if order_proxy.is_rejected():
                symbol = order_proxy.data._name if order_proxy.data else "Unknown"
                alarm_manager.push_text(f"⚠️ 订单被拒绝: {symbol} - {msg}", level='WARNING')

            # 3. 如果卖单成交（有钱回笼），触发重试
            if order_proxy.is_sell() and order_proxy.executed.size > 0:
                # 再次确认不是撤单导致的 size>0 (虽然撤单通常 size=0，但为了严谨)
                if not order_proxy.is_canceled() and not order_proxy.is_rejected():
                    print("[Engine] Sell filled. Waiting for cash settlement (1s)...")

                    if hasattr(broker, 'sync_balance'):
                        broker.sync_balance()
                        print(f"[Debug] Cash after sync: {broker.get_cash():.2f}")

                    if hasattr(broker, 'process_deferred_orders'):
                        try:
                            broker.process_deferred_orders()
                        except Exception as e:
                            print(f"[Error] Failed to process deferred orders: {e}")
                            import traceback
                            traceback.print_exc()

        except Exception as e:
            # 记录所有未预期的异常，并确保主循环不退出即可。
            import traceback
            error_msg = f"[Engine Callback Error] Unexpected exception: {e}"

            # 打印醒目的错误日志
            print(f"\n{'=' * 40}")
            print(error_msg)
            print(f"{'=' * 40}")
            traceback.print_exc()

            # 极端防御：如果遇到未知的严重错误，也可以尝试盲调用一次重置
            # 这样即使是代码其他地方写的 Bug 导致状态脏了，也能在下一次心跳前恢复
            try:
                if hasattr(context, 'strategy_instance'):
                    context.strategy_instance.broker.force_reset_state()
            except:
                # 不抛出异常，让程序继续运行
                # 这样下一个 Bar 到来时，Broker 会有机会再次自我修正
                pass

    else:
        print("[Engine Callback Warning] No strategy_instance found in context.")


def launch_live(broker_name: str, conn_name: str, strategy_path: str, params: dict, **kwargs):
    """
    通用实盘启动器
    动态加载 live_trader.adapters.{broker_name} 模块并执行其 launch 方法
    """

    # 1. 配置检查 (保持不变)
    if not hasattr(config, 'BROKER_ENVIRONMENTS'):
        print("[Error] 'BROKER_ENVIRONMENTS' missing in config.py")
        sys.exit(1)

    broker_conf = config.BROKER_ENVIRONMENTS.get(broker_name)
    if not broker_conf:
        print(f"[Error] Broker '{broker_name}' not in BROKER_ENVIRONMENTS")
        sys.exit(1)

    conn_cfg = broker_conf.get(conn_name)
    if not conn_cfg:
        print(f"[Error] Connection '{conn_name}' not found")
        sys.exit(1)

    # 2. 动态加载模块
    module_path = f"live_trader.adapters.{broker_name}"
    try:
        adapter_module = importlib.import_module(module_path)
    except Exception:
        print(f"[Error] Failed to import module '{module_path}':")
        traceback.print_exc()
        sys.exit(1)

    # 3. 自动发现 Broker 类
    broker_class = None
    for name, obj in inspect.getmembers(adapter_module):
        # 查找逻辑：是类 + 是BaseLiveBroker的子类 + 不是BaseLiveBroker本身
        if (inspect.isclass(obj)
                and issubclass(obj, BaseLiveBroker)
                and obj is not BaseLiveBroker
                and obj.__module__ == adapter_module.__name__):
            broker_class = obj
            break

    if not broker_class:
        print(f"[Error] No subclass of 'BaseLiveBroker' found in {module_path}")
        sys.exit(1)

    # 4. 执行协议 (Lazy Check)
    # 这里直接调用，如果用户没覆盖，会抛出基类定义的 NotImplementedError
    try:
        identity = conn_name
        if 'client_id' in conn_cfg:
            identity = str(conn_cfg['client_id'])
        elif 'strategy_id' in conn_cfg:
            # 掘金ID太长，截取前8位
            identity = str(conn_cfg['strategy_id'])[:8] + "..."

        # 这里的 params 就是运行时透传的策略参数
        AlarmManager().set_runtime_context(
            broker=broker_name,
            conn_id=identity,
            strategy=strategy_path,
            params=params
        )

        # 净化 sys.argv
        sys.argv = [sys.argv[0]]

        broker_class.launch(conn_cfg, strategy_path, params, **kwargs)
    except NotImplementedError as e:
        print(f"[Error] Protocol not implemented: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[Crash] Launch execution failed:")
        AlarmManager().push_exception("Launcher Crash", str(e))
        traceback.print_exc()
        sys.exit(1)
