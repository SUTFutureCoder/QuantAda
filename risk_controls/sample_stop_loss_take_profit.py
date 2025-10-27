from .base_risk_control import BaseRiskControl


class SampleStopLossTakeProfit(BaseRiskControl):
    """
    一个简单的基于百分比的止盈止损风控模块。
    它使用 backtrader 仓位的平均价格 (position.price) 作为入场成本。
    """
    params = {
        'stop_loss_pct': 0.05,  # 默认 5% 止损
        'take_profit_pct': 0.10,  # 默认 10% 止盈
    }

    def __init__(self, broker, params=None):
        super().__init__(broker, params)
        # 存储每个标的的平均入场价格
        # 我们不再手动管理，转而依赖 broker.getposition(data).price
        self.broker.log(f"SampleStopLossTakeProfit initialized with: "
                        f"StopLoss={self.p.stop_loss_pct:.2%}, "
                        f"TakeProfit={self.p.take_profit_pct:.2%}")
        # 用于防止在触发后立即再次触发
        self.exit_triggered = set()

    def notify_trade(self, trade):
        """
        使用 notify_trade 来监控仓位变化。
        """
        data_name = trade.data._name

        if not trade.is_closed():
            # 交易未关闭（开仓或加仓），仓位成本已更新
            current_position = self.broker.getposition(trade.data)
            self.broker.log(f"[RiskControl] Position updated for {data_name}. "
                            f"New Avg Price: {current_position.price:.2f}, Size: {current_position.size}")
            # 如果是新开仓，确保它不在"已触发"集合中
            if data_name in self.exit_triggered:
                self.exit_triggered.remove(data_name)

        if trade.is_closed():
            # 交易已关闭，清除状态
            self.broker.log(f"[RiskControl] Position closed for {data_name}.")
            # 仓位已平，将其从"已触发"集合中移除
            if data_name in self.exit_triggered:
                self.exit_triggered.remove(data_name)

    def check(self, data) -> str | None:
        """
        在每个 bar 上检查止盈止损条件。
        """
        data_name = data._name

        # 0. 检查是否已触发退出（防止在订单成交前重复触发）
        if data_name in self.exit_triggered:
            return None

        # 1. 获取仓位和平均成本价
        # broker.getposition() 会返回 backtrader 的仓位对象
        position = self.broker.getposition(data)
        if not position.size:
            return None  # 没有仓位，不检查

        entry_price = position.price  # 获取平均持仓成本
        current_price = data.close[0]  # 使用当前bar的收盘价

        # 2. 检查止损
        if self.p.stop_loss_pct > 0:
            stop_loss_price = entry_price * (1 - self.p.stop_loss_pct)
            if current_price < stop_loss_price:
                self.broker.log(f"[RiskControl] STOP-LOSS triggered for {data_name}. "
                                f"Entry: {entry_price:.2f}, Current: {current_price:.2f}, "
                                f"Target: < {stop_loss_price:.2f}")
                self.exit_triggered.add(data_name)  # 标记为已触发
                return 'SELL'

        # 3. 检查止盈
        if self.p.take_profit_pct > 0:
            take_profit_price = entry_price * (1 + self.p.take_profit_pct)
            if current_price > take_profit_price:
                self.broker.log(f"[RiskControl] TAKE-PROFIT triggered for {data_name}. "
                                f"Entry: {entry_price:.2f}, Current: {current_price:.2f}, "
                                f"Target: > {take_profit_price:.2f}")
                self.exit_triggered.add(data_name)  # 标记为已触发
                return 'SELL'

        return None