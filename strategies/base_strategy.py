from abc import ABC, abstractmethod


class _Params(object):
    """一个简单的辅助类，用于将字典键转换为对象属性，以支持点操作符访问。"""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


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
        self.params = _Params(**final_params)

        # 3. 创建 'p' 作为 'params' 的快捷方式，以符合Backtrader的惯例
        self.p = self.params

    def log(self, txt, dt=None):
        """
        通用日志记录
        """
        self.broker.log(txt, dt)

    @abstractmethod
    def init(self):
        """
        策略初始化，在这里准备指标等
        """
        pass

    @abstractmethod
    def next(self):
        """
        每个K线周期调用的核心逻辑。
        """
        pass

    def notify_order(self, order):
        """
        订单状态通知
        """
        if order.is_completed():
            if order.is_buy():
                self.log(
                    f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
            elif order.is_sell():
                self.log(
                    f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.5f}')
        elif order.is_rejected():
            self.log(f'Order Canceled/Rejected/Margin')

    def notify_trade(self, trade):
        """
        交易成交通知
        """
        if trade.is_closed():
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')

    def smart_order_target_percent(self, data, target):
        """
        兼容回测与实盘的智能下单函数：强制执行A股100股取整逻辑
        """
        symbol = data._name

        # 1. 获取总资产 (Backtrader标准接口)
        value = self.broker.getvalue()

        # 2. 获取当前价格
        # 注意：在next()中调用时，data.close[0]是当前K线收盘价
        price = data.close[0]
        if price <= 0:
            return None

        # 3. 计算目标市值 -> 目标股数
        target_value = value * target
        target_size = target_value / price

        # 4. A股取整逻辑 (向下取整到100的倍数)
        if symbol.startswith('SHSE') or symbol.startswith('SZSE'):
            # 例如: 198.5 -> 1.985 -> int(1) -> 100
            target_size = int(target_size / 100) * 100
        else:
            # 美股/港股等只需取整
            target_size = int(target_size)

        # 5. 获取当前持仓
        current_position = self.broker.getposition(data).size

        # 6. 计算差额 (Delta)
        delta_size = target_size - current_position

        # 7. 执行下单 (使用 buy/sell 接口更通用)
        if delta_size != 0:
            if delta_size > 0:
                self.log(f"智能调仓: {symbol} 目标{target * 100:.1f}% -> 买入 {delta_size} 股")
                return self.broker.buy(data=data, size=delta_size)
            else:
                self.log(f"智能调仓: {symbol} 目标{target * 100:.1f}% -> 卖出 {abs(delta_size)} 股")
                return self.broker.sell(data=data, size=abs(delta_size))

        return None