from .base_strategy import BaseStrategy


class SampleMultiPortfolioStrategy(BaseStrategy):
    """
    一个简单的等权重投资组合策略。
    在回测开始时，将资金平均买入所有可用的标的并持有。
    """

    def init(self):
        self.log(f"Strategy initialized for {len(self.broker.datas)} assets.")
        self.bought = False

    def next(self):
        # 确保只在第一个有效的bar执行一次
        if self.bought:
            return

        # 计算每个标的应分配的目标百分比
        target_percent = 0.95 / len(self.broker.datas)

        self.log(f"Placing initial orders. Target percent per asset: {target_percent:.2%}")

        # 遍历所有数据源（即所有标的）并下单
        for data in self.broker.datas:
            self.broker.order_target_percent(data=data, target=target_percent)

        self.bought = True  # 标记为已下单，防止重复执行
