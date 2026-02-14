from abc import ABC, abstractmethod
from types import SimpleNamespace

import config


class BaseStrategy(ABC):
    """
    策略抽象基类
    策略作者只需要继承这个类，并实现其核心逻辑。
    'broker'对象将由外部引擎（回测或实盘）注入，它提供了所有交易和数据访问的接口。
    """

    params = {}

    def __init__(self, broker, params=None):
        """
        初始化策略参数
        :param params:
        """
        self.broker = broker
        # 1. 合并类级别定义的默认参数和实例化时传入的参数
        final_params = self.params.copy()
        if params:
            final_params.update(params)

        # 2. 使用辅助类将最终的参数字典转换为一个对象
        self.params = SimpleNamespace(**final_params)

        # 3. 创建 'p' 作为 'params' 的快捷方式，以符合Backtrader的惯例
        self.p = self.params

        # =========================================================
        # 4. 三级级联豁免标的保护逻辑 (Configuration Cascading)
        # =========================================================
        # Level 1: 全局默认底仓豁免
        ignored = getattr(config, 'IGNORED_SYMBOLS', [])

        # Level 2: 环境级专属豁免 (来自 engine 透传)
        if hasattr(self.p, 'env_ignored_symbols') and self.p.env_ignored_symbols is not None:
            ignored = self.p.env_ignored_symbols

        # Level 3: 策略专属定制豁免 (最高优)
        if hasattr(self.p, 'ignored_symbols') and self.p.ignored_symbols is not None:
            ignored = self.p.ignored_symbols

        # 统一转为大写 Set，极大提升后续查表性能
        self.active_ignored_symbols = {str(sym).upper() for sym in ignored}
        # =========================================================


    def log(self, txt, dt=None):
        """
        通用日志记录
        """
        self.broker.log(txt, dt)

    @abstractmethod
    def init(self):
        """
        策略初始化，在这里准备指标等
        !!!注意，初始化方法只会执行一次，如果将计算逻辑写到这里实盘会有不重新计算的风险，请抽象计算方法并放置于next中!!!
        """
        pass

    @abstractmethod
    def next(self):
        """
        每个K线周期调用的核心逻辑。
        """
        pass

    @property
    def tradable_datas(self):
        """
        [框架属性] 返回过滤掉所有豁免底仓后的可交易数据源列表。
        策略端只需遍历 self.tradable_datas，无需手动判断豁免。
        """
        valid_datas = []
        for d in self.broker.datas:
            base_name = d._name.split('.')[0].upper()
            full_name = d._name.upper()

            # 只要不在三级级联的豁免名单中，就视为可交易
            if base_name not in self.active_ignored_symbols and full_name not in self.active_ignored_symbols:
                valid_datas.append(d)

        return valid_datas

    def notify_order(self, order):
        """
        订单状态通知
        """
        if order.is_completed() and order.executed.size > 0:
            if order.is_buy():
                self.log(
                    f'BUY EXECUTED, Size: {order.executed.size:.2f}, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
            elif order.is_sell():
                self.log(
                    f'SELL EXECUTED, Size: {order.executed.size:.2f}, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
        elif order.is_rejected():
            self.log(f'Order Canceled/Rejected/Margin')

    def notify_trade(self, trade):
        """
        交易成交通知
        """
        if trade.is_closed():
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')

    def get_strategy_isolated_capital(self):
        """
        获取策略隔离的真实可用资金 (Bottom-Up 盘点法)
        完美无视未订阅及被豁免的底仓资产（如 SGOV）
        返回: (allocatable_capital, current_positions_dict)
        """
        current_positions = {}
        managed_market_value = 0.0

        for d in self.broker.datas:
            base_name = d._name.split('.')[0].upper()
            full_name = d._name.upper()

            # 若在豁免名单中，实行物理隔离，不计入策略仓位
            if base_name in self.active_ignored_symbols or full_name in self.active_ignored_symbols:
                continue

            pos = self.broker.getposition(d)
            if pos.size > 0:
                if hasattr(self.broker, 'get_current_price'):
                    price = self.broker.get_current_price(d)
                elif len(d) > 0:
                    price = d.close[0]
                else:
                    price = pos.price

                market_value = pos.size * price
                current_positions[d] = market_value
                managed_market_value += market_value

        # 策略真实购买力 = 账户剩余现金 + 本策略正在管控的仓位市值
        available_cash = self.broker.get_cash()
        allocatable_capital = available_cash + managed_market_value

        return allocatable_capital, current_positions

    # 声明式全自动调仓接口
    def execute_rebalance(self, target_symbols, top_k, rebalance_threshold=0.2):
        """
        框架级自动调仓流水线。
        包含：自动底层隔离盘点 -> 计划生成 -> 智能发单。
        策略端只需提供目标标的列表，其余一概不用操心。
        """
        # 延迟导入以防止循环依赖
        from common.rebalancer import PortfolioRebalancer, OrderExecutor

        # 1. 底层框架全自动盘点真实可用资金 (已完美无视所有豁免底仓)
        allocatable_capital, current_positions = self.get_strategy_isolated_capital()

        # 2. 生成调仓计划
        plan = PortfolioRebalancer.calculate_plan(
            current_positions=current_positions,
            target_symbols=target_symbols,
            total_capital=allocatable_capital,
            select_top_k=top_k,
            rebalance_threshold=rebalance_threshold
        )

        # 3. 执行发单
        if not hasattr(self, 'executor'):
            self.executor = OrderExecutor(self.broker)

        self.executor.execute_plan(plan)