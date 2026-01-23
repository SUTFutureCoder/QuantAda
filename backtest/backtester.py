import datetime

import backtrader as bt
import pandas as pd

import config


class OrderProxy:
    def __init__(self, bt_order): self._order = bt_order

    def is_buy(self): return self._order.isbuy()

    def is_sell(self): return self._order.issell()

    def is_pending(self): return self._order.status in [self._order.Submitted, self._order.Accepted]

    def is_completed(self): return self._order.status == self._order.Completed

    def is_rejected(self): return self._order.status in [self._order.Canceled, self._order.Margin,
                                                         self._order.Rejected]

    def getstatusname(self):
        """Delegates call to the original backtrader order object."""
        return self._order.getstatusname()

    @property
    def executed(self): return self._order.executed

    @property
    def data(self):
        """Exposes the data feed associated with the order."""
        return self._order.data


class TradeProxy:
    """将backtrader的trade对象适配成通用接口"""

    def __init__(self, bt_trade): self._trade = bt_trade

    def is_closed(self): return self._trade.isclosed

    @property
    def pnl(self): return self._trade.pnl

    @property
    def pnlcomm(self): return self._trade.pnlcomm

    @property
    def data(self): return self._trade.data


class BacktraderStrategyWrapper(bt.Strategy):
    """
    Backtrader的包装器策略
    唯一职责是加载我们的纯策略，并将Backtrader的环境传递给它
    """

    def __init__(self, strategy_class, params=None, risk_control_class=None, risk_control_params=None, recorder=None):
        self.recorder = recorder
        # 增加一个属性用于存储实际开始日期，面向解决多标的数据就绪问题
        self.actual_start_date = None
        # 用于记录单次 next 循环中，卖单预计释放的资金
        self.expected_freed_cash = 0.0
        self.dataclose = self.datas[0].close
        self.strategy = strategy_class(broker=self, params=params)
        self.risk_control = None
        if risk_control_class:
            self.risk_control = risk_control_class(broker=self, params=risk_control_params)
        self.strategy.init()

    def log(self, txt, dt=None):
        if config.LOG:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()} {txt}')

    def next(self):
        # 每次进入新的 K 线周期，重置预计释放资金为 0
        self.expected_freed_cash = 0.0

        if self.actual_start_date is None:
            self.actual_start_date = self.datas[0].datetime.datetime(0)

        # 检查策略是否有挂单。
        # 注意：如果策略逻辑是多标的并发的，建议策略内部维护一个订单列表或字典，
        # 而不是依赖单一的 self.strategy.order 锁。
        # 这里保留原逻辑的兼容性，但建议您后续在策略类中改进订单管理。
        if hasattr(self.strategy, 'order') and self.strategy.order:
            return  # 策略有全局挂单锁，跳过逻辑

        # 1. 执行风控检查 (获取被风控接管的标的列表)
        risk_handled_symbols = self._check_risk_controls()

        # 2. 将风控状态注入策略
        # 策略在 next() 中可以通过 checking self.risk_handled_symbols 来决定
        # 是否要跳过对某些标的的操作
        self.strategy.risk_handled_symbols = risk_handled_symbols

        # 3. 始终执行策略逻辑
        # 即使 A 标的触发了止损，B 标的依然可能有信号需要处理
        self.strategy.next()

    def buy(self, *args, **kwargs):
        """
        重写 buy 方法，在下单瞬间记录决策
        """
        order = super().buy(*args, **kwargs)
        if order:
            self._log_decision(order)
        return order

    def sell(self, *args, **kwargs):
        """
        重写 sell 方法，在下单瞬间记录决策
        """
        order = super().sell(*args, **kwargs)
        if order:
            self._log_decision(order)
        return order

    def notify_order(self, order):
        if self.risk_control:
            self.risk_control.notify_order(OrderProxy(order))

        self.strategy.notify_order(OrderProxy(order))

    def notify_trade(self, trade):
        if self.risk_control:
            self.risk_control.notify_trade(TradeProxy(trade))

        self.strategy.notify_trade(TradeProxy(trade))

    def order_target_percent(self, data=None, target=0.0, **kwargs):
        data = data or self.datas[0]
        lot_size = kwargs.get('lot_size', config.DEFAULT_LOT_SIZE)

        # 防守逻辑：如果该标的正在被风控接管，且策略试图买入，则拦截
        if hasattr(self.strategy, 'risk_handled_symbols'):
            if data._name in self.strategy.risk_handled_symbols and target > 0:
                self.log(f"IGNORED BUY order for {data._name} due to Risk Control Lock.")
                return None

        # 1. 获取当前持仓和价格
        pos_size = self.getposition(data).size
        price = data.close[0]

        if price <= 0:
            return None  # 价格异常，不操作

        # 2. 获取账户总价值 (现金 + 持仓市值)
        # self.broker.getvalue() 返回的是当前回测时刻的总资产
        portfolio_value = self.broker.getvalue()

        # 3. 计算目标市值和目标股数
        target_value = portfolio_value * target
        expected_shares = target_value / price

        # 4. 计算需要变化的股数
        delta_shares = expected_shares - pos_size

        # 5. 执行下单逻辑
        if delta_shares > 0:  # 买入
            # 获取可用现金
            # 可用现金 = 账户当前现金 + 本次循环中卖单预计回笼的资金
            current_cash = self.broker.getcash()
            total_purchasing_power = current_cash + self.expected_freed_cash

            # 估算包含手续费的最大购买量 (假设 commission 是比例，如 0.0003)
            commission_ratio = self.broker.getcommissioninfo(data).p.commission
            max_buy_by_cash = total_purchasing_power / (price * (1 + commission_ratio))

            # 取 目标买入量 和 现金最大买入量 的较小值
            shares_to_buy = min(delta_shares, max_buy_by_cash)

            # 向下取整到 lot_size
            if lot_size > 1:
                shares_to_buy = int(shares_to_buy // lot_size) * lot_size
            else:
                shares_to_buy = int(shares_to_buy)  # 即使是美股也通常是整数股

            if shares_to_buy > 0:
                # 打印日志方便调试
                # self.log(f"ORDER TARGET: Target% {target:.2f}, Cash {cash:.0f}, Buying {shares_to_buy}")
                return self.buy(data=data, size=shares_to_buy)

        elif delta_shares < 0:  # 卖出
            shares_to_sell = abs(delta_shares)

            # 无论是否清仓，都要先计算预计释放的资金
            if shares_to_sell > 0:
                estimated_value = shares_to_sell * price
                self.expected_freed_cash += estimated_value

            # 如果目标是 0，通常意味着清仓
            if target == 0.0:
                # 如果是清仓，直接使用 close()，它会处理所有持仓
                # 注意：self.close() 内部逻辑可能不保证 100 整手，但在清仓时通常需要卖出所有零股
                # 如果需要严格整手卖出，可以使用下面的逻辑，但会残留零股
                return self.close(data=data)

            # 向下取整到 lot_size
            if lot_size > 1:
                shares_to_sell = int(shares_to_sell // lot_size) * lot_size
            else:
                shares_to_sell = int(shares_to_sell)

            if shares_to_sell > 0:
                return self.sell(data=data, size=shares_to_sell)

        return None

    def _check_risk_controls(self) -> list:
        """
        辅助方法：检查所有标的，执行风控检查并采取行动。
        封装了所有风控相关的循环和判断逻辑。

        :return: list 返回所有触发了风控操作的 symbol 名称列表。
        """
        triggered_symbols = []

        if not self.risk_control:
            return triggered_symbols

        for data in self.datas:
            # 1. 检查是否有仓位 (使用卫语句优化)
            if not self.getposition(data).size:
                continue  # 没有仓位，检查下一个标的

            # 2. 对持仓标的执行风控检查
            action = self.risk_control.check(data)

            # 3. 如果触发平仓
            if action == 'SELL':
                self.log(f"Risk module triggered SELL for {data._name}")

                # 执行平仓
                order = self.order_target_percent(data=data, target=0.0)

                # 将订单句柄存入策略的 'order' 属性 (注意：多标的同时触发时，这里只存了最后一个)
                # 这是一个简化的锁机制，如果策略需要精细控制，应该维护一个 order 列表
                if hasattr(self.strategy, 'order'):
                    self.strategy.order = order

                # 记录触发风控的标的
                triggered_symbols.append(data._name)

        return triggered_symbols

    def _log_decision(self, order):
        """
        辅助方法：立即记录交易决策
        """
        if not self.recorder:
            return

        try:
            action = 'BUY' if order.isbuy() else 'SELL'

            # 1. 获取决策时间 (当前 Bar 的时间)
            current_dt = order.data.datetime.datetime(0)

            # 2. 获取决策价格 (当前 Close)
            decision_price = order.data.close[0]

            # 3. 获取决策数量 (Created 中的 size)
            decision_size = order.created.size

            # 4. 估算手续费 (假定成交)
            comminfo = self.broker.getcommissioninfo(order.data)
            estimated_comm = comminfo.getcommission(decision_price, decision_size)

            # 5. 获取账户快照
            current_cash = self.broker.getcash()
            current_value = self.broker.getvalue()

            self.recorder.log_trade(
                dt=current_dt,
                symbol=order.data._name,
                action=action,
                price=decision_price,
                size=decision_size,
                comm=estimated_comm,
                order_ref=order.ref,
                cash=current_cash,
                value=current_value
            )
        except Exception as e:
            print(f"Error logging decision: {e}")

class Backtester:
    # 回测执行器
    def __init__(self, datas, strategy_class, params=None, start_date=None, end_date=None, cash=100000.0,
                 commission=0.0, sizer_class=None, sizer_params=None,
                 risk_control_class=None, risk_control_params=None,
                 timeframe: str = 'Days', compression: int = 1,
                 recorder = None, enable_plot = True):
        self.cerebro = bt.Cerebro()
        self.datas = datas
        self.strategy_class = strategy_class
        self.params = params
        self.start_date = start_date
        self.end_date = end_date
        self.cash = cash
        self.commission = commission
        self.sizer_class = sizer_class
        self.sizer_params = sizer_params
        self.risk_control_class = risk_control_class
        self.risk_control_params = risk_control_params
        self.timeframe_str = timeframe
        self.compression = compression
        self.recorder = recorder
        self.enable_plot = enable_plot
        self.timeframe = self._get_bt_timeframe(timeframe)

    def _get_bt_timeframe(self, timeframe_str: str) -> int:
        """将字符串时间维度映射到backtrader的TimeFrame枚举值"""
        mapping = {
            'Days': bt.TimeFrame.Days,
            'Weeks': bt.TimeFrame.Weeks,
            'Months': bt.TimeFrame.Months,
            'Minutes': bt.TimeFrame.Minutes,
            'Seconds': bt.TimeFrame.Seconds,
        }
        return mapping.get(timeframe_str, bt.TimeFrame.Days)

    def run(self):
        # 将数据添加到Cerebro
        for symbol, df in self.datas.items():
            feed = bt.feeds.PandasData(
                dataname=df,
                fromdate=pd.to_datetime(self.start_date) if self.start_date else None,
                todate=pd.to_datetime(self.end_date) if self.end_date else None,
                name=symbol,  # 为每个数据源命名，方便策略内部通过名称访问
                timeframe = self.timeframe,
                compression = self.compression
            )
            self.cerebro.adddata(feed)
            print(f"  Data feed for '{symbol}' added.")

        # 添加包装后的策略
        self.cerebro.addstrategy(
            BacktraderStrategyWrapper,
            strategy_class=self.strategy_class,
            params=self.params,
            risk_control_class=self.risk_control_class,
            risk_control_params=self.risk_control_params,
            recorder=self.recorder,
        )

        # 设置初始资金和手续费
        self.cerebro.broker.setcash(self.cash)
        self.cerebro.broker.setcommission(commission=self.commission)

        # 仅在 sizer_class 被提供时才添加全局 Sizer
        if self.sizer_class:
            # 使用 `**(self.sizer_params or {})` 以安全地处理 sizer_params 为 None 的情况
            self.cerebro.addsizer(self.sizer_class, **(self.sizer_params or {}))

        # 添加性能分析器
        # PyFolio分析器用于提取详细的交易数据，即使不使用pyfolio绘图，它也非常有用
        self.cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.0,
                                 timeframe=self.timeframe, compression=self.compression)
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='tradeanalyzer')

        print(f"Starting Portfolio Value: {self.cerebro.broker.getvalue():.2f}")
        self.results = self.cerebro.run()
        final_val = self.cerebro.broker.getvalue()
        print(f"Final Portfolio Value: {self.cerebro.broker.getvalue():.2f}")

        if self.recorder and self.recorder.active:
            # 复用 display_results 中的计算逻辑，或者简单提取
            # 这里为了简洁，重新提取关键指标，你也可以重构 display_results 返回这些值
            strat = self.results[0]
            sharpe_analyzer = strat.analyzers.getbyname('sharpe')
            sharpe = sharpe_analyzer.get_analysis().get('sharperatio', 0.0)
            if sharpe is None: sharpe = 0.0

            drawdown_analyzer = strat.analyzers.getbyname('drawdown')
            max_dd = drawdown_analyzer.get_analysis().max.drawdown / 100

            total_ret = (final_val / self.cash) - 1
            # 简单的年化估算，详细逻辑参考 display_results
            start_dt = pd.to_datetime(self.start_date) if self.start_date else datetime.datetime.now()  # 简化
            end_dt = pd.to_datetime(self.end_date) if self.end_date else datetime.datetime.now()
            days = (end_dt - start_dt).days
            ann_ret = 0.0
            if days > 0:
                ann_ret = ((1 + total_ret) ** (365.0 / days)) - 1

            self.recorder.finish_execution(
                final_value=final_val,
                total_return=total_ret,
                sharpe=sharpe,
                max_drawdown=max_dd,
                annual_return=ann_ret
            )

        # --- 替换 cerebro.plot() 为我们自己的展示函数 ---
        self.display_results()

        if self.enable_plot:
            print("Generating plot...")
            try:
                self.cerebro.plot()
            except Exception as e:
                # 捕获 tkinter 缺失或其他绘图错误，防止整个脚本最后报错
                error_msg = str(e).lower()
                if "tkinter" in error_msg or "tkagg" in error_msg:
                    print("\n[Warning] Plotting Skipped: 'tkinter' module not found.")
                    print("  - On Server: Use --no_plot to suppress this.")
                    print("  - To Fix: Install python3-tk (e.g., yum install python3-tk / apt install python3-tk)")
                else:
                    print(f"\n[Warning] Plotting Failed: {e}")
        else:
            print("Plotting disabled via --no_plot.")

    def get_performance_metric(self, metric_name='sharpe'):
        """
        供外部调用的获取回测结果接口 (专为 Optuna 设计)
        """
        if not hasattr(self, 'results') or not self.results:
            return -999.0

        strat = self.results[0]

        if metric_name == 'sharpe':
            # 获取夏普比率
            sharpe_analyzer = strat.analyzers.getbyname('sharpe')
            # get_analysis() 返回字典 {'sharperatio': float}
            s = sharpe_analyzer.get_analysis().get('sharperatio')
            # 如果夏普计算失败(如无交易或波动为0)，返回负值惩罚
            return s if s is not None else -999.0

        elif metric_name == 'return':
            # 获取总收益率
            initial_value = self.cash
            final_value = self.cerebro.broker.getvalue()
            return (final_value / initial_value) - 1.0

        elif metric_name == 'final_value':
            return self.cerebro.broker.getvalue()

        return 0.0

    def display_results(self):
        """
        计算并展示详细的回测性能指标和图表。
        """
        if not self.results:
            print("Please run the backtest first.")
            return

        # 从回测结果中提取第一个策略（我们只有一个）
        strat = self.results[0]

        # 提取分析器数据
        pyfolio_analyzer = strat.analyzers.getbyname('pyfolio')
        returns, positions, transactions, gross_leverage = pyfolio_analyzer.get_pf_items()

        returns_analyzer = strat.analyzers.getbyname('returns')
        drawdown_analyzer = strat.analyzers.getbyname('drawdown')
        sharpe_analyzer = strat.analyzers.getbyname('sharpe')
        trade_analyzer = strat.analyzers.getbyname('tradeanalyzer')

        # --- 1. 计算核心指标 ---
        # 1. 从策略实例中获取准确的开始日期
        # 如果策略没有运行（没有交易和收益），则start_date可能不存在
        if hasattr(strat, 'actual_start_date') and strat.actual_start_date is not None:
            start_date = strat.actual_start_date
        else:
            # 回退方案：在没有实际开始日期的情况下使用returns的起始
            if returns.empty:
                print("Backtest generated no returns. Cannot calculate performance.")
                return
            start_date = returns.index[0].to_pydatetime().replace(tzinfo=None)

        end_date = returns.index[-1].to_pydatetime().replace(tzinfo=None)

        # 2. 使用这个准确的日期来计算年化收益率
        total_return = (self.cerebro.broker.getvalue() / self.cash) - 1
        time_period_years = (end_date - start_date).days / 365.25

        if time_period_years > 0:
            annual_return = ((1 + total_return) ** (1 / time_period_years)) - 1
        else:
            annual_return = 0.0

        # 夏普比率 (注意: 内置的SharpeRatio分析器也可能受时间跨度影响，但通常偏差不大)
        sharpe_ratio = sharpe_analyzer.get_analysis().get('sharperatio', 0.0)
        if sharpe_ratio is None: sharpe_ratio = 0.0

        # 最大回撤
        max_drawdown = drawdown_analyzer.get_analysis().max.drawdown / 100  # 转换为小数

        # 卡玛比率 (年化收益 / 最大回撤)
        calmar_ratio = (annual_return * 100) / (abs(max_drawdown) * 100) if max_drawdown != 0 else 0.0

        # 交易统计
        trade_analysis = trade_analyzer.get_analysis()
        total_trades = trade_analysis.get('total', {}).get('total', 0)

        # 安全地获取盈利交易的统计数据
        if 'won' in trade_analysis and trade_analysis.won.total > 0:
            win_trades = trade_analysis.won.total
            total_win_pnl = trade_analysis.won.pnl.total
            avg_win_pnl = trade_analysis.won.pnl.average
        else:
            win_trades = 0
            total_win_pnl = 0.0
            avg_win_pnl = 0.0

        # 安全地获取亏损交易的统计数据
        if 'lost' in trade_analysis and trade_analysis.lost.total > 0:
            loss_trades = trade_analysis.lost.total
            total_loss_pnl = trade_analysis.lost.pnl.total
            avg_loss_pnl = trade_analysis.lost.pnl.average
        else:
            loss_trades = 0
            total_loss_pnl = 0.0
            avg_loss_pnl = 0.0

        # 在确保数据安全后进行计算
        win_rate = (win_trades / total_trades) * 100 if total_trades > 0 else 0.0
        profit_factor = total_win_pnl / abs(total_loss_pnl) if total_loss_pnl != 0 else float('inf')
        pnl_ratio = avg_win_pnl / abs(avg_loss_pnl) if avg_loss_pnl != 0 else float('inf')

        # --- 2. 打印性能报告 ---
        print("\n" + "=" * 50)
        print("            Backtest Performance Metrics")
        print("=" * 50)
        print(f" Time Frame:           {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f" Initial Portfolio:    {self.cash:,.2f}")
        print(f" Final Portfolio:      {self.cerebro.broker.getvalue():,.2f}")
        print("-" * 50)
        print(f" Total Return:         {total_return: .2%}")
        print(f" Annualized Return:    {annual_return: .2%}")
        print(f" Sharpe Ratio:         {sharpe_ratio: .2f}")
        print(f" Max Drawdown:         {max_drawdown: .2%}")
        print(f" Calmar Ratio:         {calmar_ratio: .2f}")
        print("-" * 50)
        print(f" Total Trades:         {total_trades}")
        print(f" Win Rate:             {win_rate: .2f}%")
        print(f" Profit Factor:        {profit_factor: .2f}")
        print(f" Avg. Win / Avg. Loss: {pnl_ratio: .2f}")
        print("=" * 50 + "\n")
