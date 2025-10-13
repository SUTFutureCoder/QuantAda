import backtrader as bt
import pandas as pd

import config
from .indicators import CustomMACD, CustomCrossOver


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


class BacktraderIndicatorFactory:
    """为Backtrader环境创建指标的工厂"""

    def __init__(self, data):
        self._data = data

    def MACD(self):
        return CustomMACD(self._data)

    def CrossOver(self, a, b):
        return CustomCrossOver(a, b)


class BacktraderStrategyWrapper(bt.Strategy):
    """
    Backtrader的包装器策略
    唯一职责是加载我们的纯策略，并将Backtrader的环境传递给它
    """

    def __init__(self, strategy_class, strategy_params=None):
        self.dataclose = self.datas[0].close

        indicator_factory = BacktraderIndicatorFactory(self.datas[0])
        self.strategy = strategy_class(broker=self, indicators=indicator_factory, params=strategy_params)
        self.strategy.init()

    def log(self, txt, dt=None):
        if config.LOG:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()} {txt}')

    def next(self):
        self.strategy.next()

    def notify_order(self, order):
        self.strategy.notify_order(OrderProxy(order))

    def notify_trade(self, trade):
        self.strategy.notify_trade(TradeProxy(trade))


class Backtester:
    # 回测执行器
    def __init__(self, datas, strategy_class, strategy_params=None, start_date=None, end_date=None, cash=100000.0,
                 commission=0.00015
                 , sizer_class=bt.sizers.PercentSizer, sizer_params={'percents': 95}):
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

    def run(self):
        # 将数据添加到Cerebro
        for symbol, df in self.datas.items():
            feed = bt.feeds.PandasData(
                dataname=df,
                fromdate=pd.to_datetime(self.start_date),
                todate=pd.to_datetime(self.end_date),
                name=symbol  # 为每个数据源命名，方便策略内部通过名称访问
            )
            self.cerebro.adddata(feed)
            print(f"  Data feed for '{symbol}' added.")

        # 添加包装后的策略
        self.cerebro.addstrategy(
            BacktraderStrategyWrapper,
            strategy_class=self.strategy_class,
            strategy_params=self.strategy_params
        )

        # 设置初始资金和手续费
        self.cerebro.broker.setcash(self.cash)
        self.cerebro.broker.setcommission(commission=self.commission)

        # 动态添加Sizer
        self.cerebro.addsizer(self.sizer_class, **self.sizer_params)

        # PyFolio分析器用于提取详细的交易数据
        self.cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.0,
                                 timeframe=bt.TimeFrame.Days, compression=1)
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='tradeanalyzer')

        print(f"Starting Portfolio Value: {self.cerebro.broker.getvalue():.2f}")
        self.results = self.cerebro.run()
        print(f"Final Portfolio Value: {self.cerebro.broker.getvalue():.2f}")

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
        # 时间范围
        start_date_str = self.start_date if self.start_date else returns.index[0].strftime('%Y-%m-%d')
        end_date_str = self.end_date if self.end_date else returns.index[-1].strftime('%Y-%m-%d')

        # 总收益率
        total_return = (self.cerebro.broker.getvalue() / self.cash) - 1

        # 年化收益率
        # 从returns分析器获取年化收益率 (rnorm100)
        annual_return = returns_analyzer.get_analysis().get('rnorm100', 0.0)

        # 夏普比率
        sharpe_ratio = sharpe_analyzer.get_analysis().get('sharperatio', 0.0)
        if sharpe_ratio is None: sharpe_ratio = 0.0

        # 最大回撤
        max_drawdown = drawdown_analyzer.get_analysis().max.drawdown / 100  # 转换为小数

        # 卡玛比率 (年化收益 / 最大回撤)
        calmar_ratio = annual_return / (abs(max_drawdown) * 100) if max_drawdown != 0 else 0.0

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
        print(f" Time Frame:           {start_date_str} to {end_date_str}")
        print(f" Initial Portfolio:    {self.cash:,.2f}")
        print(f" Final Portfolio:      {self.cerebro.broker.getvalue():,.2f}")
        print("-" * 50)
        print(f" Total Return:         {total_return: .2%}")
        print(f" Annualized Return:    {annual_return / 100: .2%}")
        print(f" Sharpe Ratio:         {sharpe_ratio: .2f}")
        print(f" Max Drawdown:         {max_drawdown: .2%}")
        print(f" Calmar Ratio:         {calmar_ratio: .2f}")
        print("-" * 50)
        print(f" Total Trades:         {total_trades}")
        print(f" Win Rate:             {win_rate: .2f}%")
        print(f" Profit Factor:        {profit_factor: .2f}")
        print(f" Avg. Win / Avg. Loss: {pnl_ratio: .2f}")
        print("=" * 50 + "\n")

