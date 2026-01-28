from data_extra_providers.http_extra_provider import HttpExtraProvider
from .base_strategy import BaseStrategy


class SampleExtraDataStrategy(BaseStrategy):
    """
    简化版HTTP数据策略
    仅基于市场整体情绪进行交易
    """

    # !!!注意，初始化方法只会执行一次，如果将计算逻辑写到这里实盘会有不重新计算的风险，请抽象计算方法并放置于next中!!!
    def init(self):
        # 主标的数据
        self.main_data = self.broker.datas[0]

        # HTTP请求的额外数据
        self.stock_provider = HttpExtraProvider()
        self.stock_df = self.stock_provider.fetch()

        self.has_data = self.stock_df is not None and not self.stock_df.empty
        self.order = None

        # 策略参数
        self.buy_threshold = 0.5  # 买入阈值
        self.sell_threshold = 0.3  # 卖出阈值

    def next(self):
        if self.order:
            return

        if not self.has_data:
            return

        # 计算简单市场情绪（上涨股票比例）
        df = self.stock_df
        up_ratio = len(df[df['change_percent'] > 0]) / len(df)

        current_pos = self.broker.position

        if not current_pos and up_ratio >= self.buy_threshold:
            # 买入：多数股票上涨
            self.log(f'BUY - Up ratio: {up_ratio:.2%}')
            self.order = self.broker.buy()

        elif current_pos and up_ratio <= self.sell_threshold:
            # 卖出：多数股票下跌
            self.log(f'SELL - Up ratio: {up_ratio:.2%}')
            self.order = self.broker.close()

    def notify_order(self, order):
        super().notify_order(order)
        if not order.is_pending():
            self.order = None
