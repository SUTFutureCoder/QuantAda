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

    def __init__(self, strategy_class, strategy_params=None, risk_control_class=None, risk_control_params=None):
        # 增加一个属性用于存储实际开始日期，面向解决多标的数据就绪问题
        self.actual_start_date = None
        self.dataclose = self.datas[0].close
        self.strategy = strategy_class(broker=self, params=strategy_params)
        self.risk_control = None
        if risk_control_class:
            self.risk_control = risk_control_class(broker=self, params=risk_control_params)
        self.strategy.init()

    def getvalue(self):
        return self.broker.getvalue()

    def log(self, txt, dt=None):
        if config.LOG:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()} {txt}')

    def next(self):
        if self.actual_start_date is None:
            self.actual_start_date = self.datas[0].datetime.datetime(0)

        # 检查策略是否有挂单。
        # 假设策略在创建订单时会设置 self.strategy.order，并在 notify_order 中清除它
        # (如 SampleMacdCrossStrategy 所示)
        if hasattr(self.strategy, 'order') and self.strategy.order:
            return  # 策略有挂单，跳过所有逻辑（包括风控）

        # 执行风控检查
        if self._check_risk_controls():
                return

        self.strategy.next()

    def notify_order(self, order):
        if self.risk_control:
            self.risk_control.notify_order(OrderProxy(order))

        self.strategy.notify_order(OrderProxy(order))

    def notify_trade(self, trade):
        if self.risk_control:
            self.risk_control.notify_trade(TradeProxy(trade))

        self.strategy.notify_trade(TradeProxy(trade))

    def _check_risk_controls(self) -> bool:
        """
        辅助方法：执行风控检查并采取行动。
        封装了所有风控相关的循环和判断逻辑。

        :return: True 如果风控被触发并执行了平仓, False 否则。
        """
        if not self.risk_control:
            return False  # 没有风控模块，直接返回

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

                # 将订单句柄存入策略的 'order' 属性，以实现锁
                if hasattr(self.strategy, 'order'):
                    self.strategy.order = order

                return True  # 【重要】风控已触发，停止检查并返回True

        return False  # 所有标的检查完毕，未触发风控

class Backtester:
    # 回测执行器
    def __init__(self, datas, strategy_class, strategy_params=None, start_date=None, end_date=None, cash=100000.0,
                 commission=0.0, sizer_class=None, sizer_params=None,
                 risk_control_class=None, risk_control_params=None,
                 timeframe: str = 'Days', compression: int = 1):
        self.cerebro = bt.Cerebro()
        self.datas = datas
        self.strategy_class = strategy_class
        self.strategy_params = strategy_params
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
                fromdate=pd.to_datetime(self.start_date),
                todate=pd.to_datetime(self.end_date),
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
            strategy_params=self.strategy_params,
            risk_control_class = self.risk_control_class,
            risk_control_params = self.risk_control_params
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
        print(f"Final Portfolio Value: {self.cerebro.broker.getvalue():.2f}")

        # --- 替换 cerebro.plot() 为我们自己的展示函数 ---
        self.display_results()

        self.cerebro.plot()

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
