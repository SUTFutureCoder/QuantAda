import datetime
import os

import pandas as pd

from alarms.manager import AlarmManager
from data_providers.gm_provider import GmDataProvider as UnifiedGmDataProvider
from .base_broker import BaseLiveBroker, BaseOrderProxy

import config
from live_trader.engine import LiveTrader, on_order_status_callback

try:
    from gm.api import order_target_percent, order_target_value, order_volume, current, get_cash, subscribe, OrderType_Market, OrderType_Limit, MODE_LIVE, MODE_BACKTEST, \
        OrderStatus_New, OrderStatus_PartiallyFilled, OrderStatus_Filled, \
        OrderStatus_Canceled, OrderStatus_Rejected, OrderStatus_PendingNew, \
        OrderSide_Buy, OrderSide_Sell
    from gm.api import set_serv_addr, set_token, ADJUST_PREV
    from gm.csdk.c_sdk import (
        py_gmi_set_strategy_id, gmi_set_mode, py_gmi_set_data_callback,
        py_gmi_set_backtest_config, py_gmi_run, gmi_init, gmi_poll,
        py_gmi_set_backtest_intraday
    )
    from gm.model.storage import context  # 掘金全局上下文
    from gm.callback import callback_controller  # 掘金回调控制器
    from gm.api._errors import check_gm_status
except ImportError:
    print("Warning: 'gm' module not found. GmAdapter will not be available.")
    order_target_percent = order_target_value = get_cash = subscribe = OrderType_Market = MODE_BACKTEST = None


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

class GmDataProvider(UnifiedGmDataProvider):
    def get_history(self, symbol: str, start_date: str, end_date: str,
                    timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:
        # 直接透传调用父类的 get_data
        return self.get_data(symbol, start_date, end_date, timeframe, compression)

class GmBrokerAdapter(BaseLiveBroker):
    """掘金平台的交易执行器实现"""

    def __init__(self, context, cash_override=None, commission_override=None):
        super().__init__(context, cash_override, commission_override)
        self.is_live = self.is_live_mode(context)  # 保存当前是否为实盘

    def getcash(self):
        """ 获取可用资金 (Backtrader 命名风格)"""
        return self._fetch_real_cash()

    # 实盘引擎调用此方法设置当前时间时，我们将其转换为无时区的北京时间
    # 这样 engine.py 中对比 df.index (无时区) 和 current_dt (无时区) 就不会报错了
    def set_datetime(self, dt):
        if dt is not None:
            # 1. 掘金传回来的是 python datetime，先转为 pandas Timestamp
            #    这样才能使用 .tz_convert 方法
            dt = pd.Timestamp(dt)

            if dt.tzinfo is not None:
                # 2. 先转为北京时间 (确保数值是 +8 区的)
                # 3. 再剥离时区 (变成 Naive，适配 Backtrader)
                dt = dt.tz_convert('Asia/Shanghai').tz_localize(None)

        super().set_datetime(dt)

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

        # === 核心分歧逻辑 ===
        # 回测 (Backtest): 使用 市价单 (Market)。
        #   尽可能和backtester回测结果对齐，券商回测的唯一作用是：测试代码有没有 Bug（会不会报错）。 至于它跑出来的收益率是 59% 还是 86%，已经不重要。
        #   理由: 掘金回测引擎的市价单能以 Open 价成交，还原真实低开红利。限价单在回测中可能以 Limit 价成交，导致低开时买贵。
        # 实盘 (Live): 使用 限价单 (Limit)。backtester的真实结果，如果gm_broker实盘用市价则第二天成交、大幅降低收益
        #   理由: 实盘中限价单能以最优价成交，且能避免市价单导致的巨额资金冻结。

        if self.is_live:
            # --- 实盘逻辑 (Limit) ---
            slippage = getattr(config, 'LIVE_LIMIT_ORDER_SLIPPAGE', 0.02)
            if side == 'BUY':
                actual_price = price * (1 + slippage)
                actual_price = float(round(actual_price, 4))  # 保留精度
                freeze_price = actual_price  # 实盘按委托价冻结
            else:
                actual_price = price * (1 - slippage)
                actual_price = float(round(actual_price, 4))
            order_type = OrderType_Limit

        else:
            # --- 回测逻辑 (Market) ---
            # 即使是 Market 单，我们也需要预估冻结资金来做 Auto-Downsize
            # A股通常冻结涨停价，回测中我们保守估算 1.1 倍 (10%涨停) 作为冻结基准
            freeze_buffer = 1.1
            if side == 'BUY':
                freeze_price = price * freeze_buffer
            else:
                freeze_price = 0  # 卖出不查钱

            # 回测中市价单不需要指定 price (或者传0)，引擎按 Open 撮合
            actual_price = 0
            order_type = OrderType_Market

        # 2. 资金预检查与自动降级 (仅买入)
        if side == 'BUY':
            available_cash = self._fetch_real_cash()
            # 预估成本
            estimated_cost = volume * freeze_price * 1.0005

            if estimated_cost > available_cash:
                old_volume = volume
                # 倒推最大股数
                volume = int(available_cash / (freeze_price * 1.0005) // 100) * 100

                if volume < 100:
                    # 只有真的买不起了才打印 (避免刷屏)
                    # print(f"[GmBroker] Skip Buy ...")
                    return None

                # 仅在发生实质性降仓时打印
                if old_volume != volume:
                    print(
                        f"[GmBroker] Auto-Downsize {data._name}: {old_volume} -> {volume} (Reason: Cash Fit, Mode: {'Live' if self.is_live else 'Backtest'})")

        if volume <= 0: return None

        try:
            effect = 1 if side == 'BUY' else 2

            ords = order_volume(
                symbol=data._name, volume=volume, side=gm_side,
                order_type=order_type,
                position_effect=effect,
                price=actual_price
            )
            return GmOrderProxy(ords[-1], self.is_live, data=data) if ords else None
        except Exception as e:
            print(f"[GM Error] {e}")
            return None

    # 5. 将券商的原始订单对象（raw_order）转换为框架标准的 BaseOrderProxy
    def convert_order_proxy(self, raw_order) -> 'BaseOrderProxy':
        """
        掘金专用实现：找到对应的 DataFeed 并包装成 GmOrderProxy
        """
        target_symbol = raw_order.symbol
        matched_data = None

        # 在 Broker 内部查找 data 对象
        for d in self.datas:
            if d._name == target_symbol:
                matched_data = d
                break

        # 返回包装好的对象
        return GmOrderProxy(raw_order, self.is_live, data=matched_data)

    # --- 实现 BaseLiveBroker 的启动协议 ---
    @classmethod
    def launch(cls, conn_cfg: dict, strategy_path: str, params: dict, **kwargs):
        """
        实现掘金启动逻辑：手动注册回调，绕过 SDK 的 filename 加载机制
        """
        import time
        import traceback
        import sys

        print(f"\n>>> Launching {cls.__name__} (Phoenix Mode) <<<")

        token = conn_cfg.get('token')
        serv_addr = conn_cfg.get('serv_addr')
        strategy_id = conn_cfg.get('strategy_id')
        schedule_rule = conn_cfg.get('schedule')

        # --- 1. 处理回测参数与模式判断 ---
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        mode = MODE_LIVE
        gm_start_time = ''
        gm_end_time = ''

        if start_date:
            mode = MODE_BACKTEST
            print(f"  Mode: BACKTEST")
            try:
                dt_start = pd.to_datetime(str(start_date))
                gm_start_time = dt_start.strftime('%Y-%m-%d 08:00:00')
                if end_date:
                    dt_end = pd.to_datetime(str(end_date))
                    gm_end_time = dt_end.strftime('%Y-%m-%d 16:00:00')
                else:
                    gm_end_time = datetime.datetime.now().strftime('%Y-%m-%d 23:59:59')
            except Exception as e:
                print(f"[Error] Date format error: {e}")
                return
        else:
            print(f"  Mode: LIVE")

        # 资金与费率
        initial_cash = float(kwargs.get('cash', 100000))
        commission = float(kwargs.get('commission', 0.0003))
        slippage = float(kwargs.get('slippage', 0.0001))

        # 提取选股器和标的
        selection_name = kwargs.get('selection')
        symbols = kwargs.get('symbols')

        # 设置全局配置
        if serv_addr: set_serv_addr(serv_addr)
        set_token(token)

        # --- 2. 核心运行逻辑 ---
        def run_session():
            # 每次启动前重置 context 身份
            py_gmi_set_strategy_id(strategy_id)
            gmi_set_mode(mode)
            context.mode = mode
            context.strategy_id = strategy_id


            def init(ctx):
                print(f"[Phoenix] Initializing Strategy '{strategy_path}'...")
                engine_config = config.__dict__.copy()
                engine_config['strategy_name'] = strategy_path
                engine_config['params'] = params
                engine_config['platform'] = 'gm'

                # 注入选股器或标的
                if selection_name:
                    engine_config['selection_name'] = selection_name
                if symbols:
                    engine_config['symbols'] = symbols

                if mode == MODE_BACKTEST:
                    engine_config['start_date'] = start_date

                # 实例化引擎
                trader = LiveTrader(engine_config)
                trader.init(ctx)
                ctx.strategy_instance = trader

                # 订阅行情
                current_symbols = ctx.strategy_instance._determine_symbols()
                if current_symbols:
                    print(f"[GmBroker] Subscribing to {len(ctx.strategy_instance._determine_symbols())} symbols...")
                    subscribe(symbols=ctx.strategy_instance._determine_symbols(), frequency='1d', count=1, wait_group=True)

                # 实盘定时任务配置
                if mode == MODE_LIVE and schedule_rule:
                    try:
                        from gm.api import schedule
                        # 解析格式 "1d:14:50:00" -> freq="1d", time="14:50:00"
                        if ':' in schedule_rule:
                            rule_type, rule_time = schedule_rule.split(':', 1)
                            print(f"[GmBroker] ⏰ 定时任务已启用 (来自配置): {rule_type} @ {rule_time}")
                            print(f"            策略将在指定时间主动运行，忽略 on_bar 事件。")

                            schedule(schedule_func=trader.run, date_rule=rule_type, time_rule=rule_time)
                            ctx.use_schedule = True
                        else:
                            print(f"[GmBroker Warning] 定时配置格式错误 (应为 freq:time): {schedule_rule}")

                    except Exception as e:
                        print(f"[GmBroker Error] 定时任务注册失败: {e}")

            def on_bar(ctx, bars):
                if hasattr(ctx, 'strategy_instance'):
                    ctx.strategy_instance.run(ctx)

            def on_order_status(ctx, order):
                on_order_status_callback(ctx, order)

            def on_error(ctx, code, info):
                msg = f"Code: {code}, Msg: {info}"
                print(f"[GM Error] {msg}")

                # 【报警接入】异常推送
                # 过滤掉一些非致命的错误码 (视情况而定)
                AlarmManager().push_exception("GM Kernel Error", msg)

            def on_shutdown(ctx):
                print("[System] Strategy Shutdown")

                # 【报警接入】停止推送
                if mode == MODE_LIVE:
                    AlarmManager().push_status("INFO", "GM Session Shutdown (Preparing to Restart)")

            def on_backtest_finished(ctx, indicator):
                print("\n" + "=" * 50)
                print("[System] Backtest Finished Report")
                print("=" * 50)

                # 直接展示原生指标，不画蛇添足
                pnl_ratio = indicator.get('pnl_ratio', 0)
                pnl_ratio_annual = indicator.get('pnl_ratio_annual', 0)
                sharpe_ratio = indicator.get('sharpe_ratio', 0)
                max_drawdown = indicator.get('max_drawdown', 0)
                win_ratio = indicator.get('win_ratio', 0)
                open_count = indicator.get('open_count', 0)

                print(f"  💰 总收益率 (Total Return):    {pnl_ratio:>.2%}")
                print(f"  📅 年化收益 (Annual Return):   {pnl_ratio_annual:>.2%}")
                print(f"  📉 最大回撤 (Max Drawdown):    {max_drawdown:>.2%}")
                print(f"  🎯 胜率 (Win Rate):           {win_ratio:>.2%}")
                print(f"  🔢 开仓次数 (Trade Count):     {int(open_count)}")

                print("-" * 50)
                print("  注意: 详细的回测报告（包含资金曲线、Alpha等）请登录掘金终端后查看。")
                print("=" * 50 + "\n")

            # --- 3. 绑定回调 ---
            context.init_fun = init
            context.on_bar_fun = on_bar
            context.on_order_status_fun = on_order_status
            context.on_error_fun = on_error
            context.on_shutdown_fun = on_shutdown
            context.on_backtest_finished_fun = on_backtest_finished

            py_gmi_set_data_callback(callback_controller)

            # --- 4. 启动运行 ---
            if mode == MODE_BACKTEST:
                print(f"  Period: {gm_start_time} -> {gm_end_time}")
                print(f"  Cash: {initial_cash}")

                py_gmi_set_backtest_config(
                    start_time=gm_start_time,
                    end_time=gm_end_time,
                    initial_cash=initial_cash,
                    transaction_ratio=1,
                    commission_ratio=commission,
                    commission_unit=0,
                    slippage_ratio=slippage,
                    option_float_margin_ratio1=0.2,  # 补全参数防止报错
                    option_float_margin_ratio2=0.4,
                    adjust=ADJUST_PREV,
                    check_cache=1,
                    match_mode=0
                )

                status = py_gmi_run()
                check_gm_status(status)
                return False

            else:  # 实盘模式
                print("  Status: Connecting to GM terminal...")
                status = gmi_init()
                if status != 0:
                    print(f"[Phoenix] ⚠️ Init failed (Code: {status}). Retrying in 10s...")
                    return True  # 初始化失败，要求重试

                check_gm_status(status)

                print("[Phoenix] Entering Event Loop (Ctrl+C to stop)...")

                try:
                    # 这是一个阻塞循环，通常 gmi_poll 会一直运行
                    # 如果 gmi_poll 返回，说明连接断开或 shutdown 触发
                    while True:
                        gmi_poll()
                        # 稍微休眠，释放 CPU，同时检测外部中断
                        time.sleep(1)

                except Exception as e:
                    print(f"[Phoenix] ⚠️ Event Loop Crashed: {e}")
                    raise e  # 抛出异常给外层处理


        # --- 3. 守护进程主循环 (The Phoenix Loop) ---
        # 只要不是回测或手动停止，这里会永远运行
        while True:
            try:
                should_retry = run_session()
                if not should_retry:
                    print(">>> GM Broker Exited Normally.")
                    break  # 回测结束或正常退出

                # 如果 run_session 返回 True，说明是异常退出或断线，需要冷却后重启
                print("[Phoenix] ⏳ Waiting 10s before restart...")
                time.sleep(10)
                print("[Phoenix] 🔄 Restarting now...")

            except KeyboardInterrupt:
                print("\n[Stop] User interrupted (Ctrl+C). Exiting Phoenix Loop.")
                if mode == MODE_LIVE:
                    AlarmManager().push_status("STOPPED", "User Manually Stopped")
                break

            except Exception as e:
                print(f"\n[CRITICAL] Unexpected Crash: {e}")
                traceback.print_exc()

                # 严重错误推送
                if mode == MODE_LIVE:
                    try:
                        AlarmManager().push_exception("Phoenix Crash", str(e))
                    except:
                        pass

                print("[Phoenix] ⚠️ Critical error. Restarting in 15s...")
                time.sleep(15)
                continue
