class PortfolioRebalancer:
    """
    纯粹的持仓平衡计算器。
    它不持有 Broker，不发单，只负责数学计算。
    """

    @staticmethod
    def calculate_plan(current_positions: dict,
                       target_symbols: list,
                       total_capital: float,
                       select_top_k: int,
                       rebalance_threshold: float = 0.05) -> dict:
        """
        生成调仓计划
        :param current_positions: 当前持仓字典 {data_object: market_value}
        :param target_symbols: 目标持仓列表 [data_object]
        :param total_capital: 分配给这些标的的总资金 (已扣除 SGOV 等保留资金)
        :param select_top_k: 目标份数
        :param rebalance_threshold: 调仓阈值 (默认 0.05 即 5%)。只有当持仓偏离目标超过此比例时才触发平衡操作。
        :return: 交易计划字典 {'sell_clear': [], 'reduce': [], 'increase': [], 'target_val_per_stock': float}"""

        # 1. 计算单股目标市值
        if select_top_k <= 0:
            target_value = 0
        else:
            target_value = total_capital / select_top_k

        plan = {
            'sell_clear': [],  # 需要清仓的
            'reduce': [],  # 需要减仓的 (data, target_value)
            'increase': [],  # 需要加仓的 (data, target_value)
            'target_per_stock': target_value
        }

        # 2. 识别清仓与减仓
        for data, current_val in current_positions.items():
            if data not in target_symbols:
                plan['sell_clear'].append(data)
            else:
                diff = target_value - current_val
                # 只有当持仓偏离度超过阈值，才纳入调整计划
                if abs(diff / target_value) > rebalance_threshold:
                    if diff < 0:
                        plan['reduce'].append((data, target_value))
                    else:
                        plan['increase'].append((data, target_value))

        # 3. 识别新开仓
        for data in target_symbols:
            if data not in current_positions:
                if target_value > 0:
                    plan['increase'].append((data, target_value))

        return plan


class OrderExecutor:
    """
    专门的执行器，负责把计划变成订单。
    """

    def __init__(self, broker, debug=True):
        self.broker = broker
        self.debug = debug

    def execute_plan(self, plan):
        """执行由 Rebalancer 生成的计划"""

        # 1. 先执行清仓 (释放资金)
        for data in plan['sell_clear']:
            self._log(f"清仓: {data._name}")
            self.broker.order_target_percent(data=data, target=0.0)

        # 2. 执行减仓 (释放资金)
        for data, target in plan['reduce']:
            self._log(f"减仓: {data._name} -> {target:.2f}")
            self.broker.order_target_value(data=data, target=target)

        # 3. 执行加仓 (消耗资金)
        for data, target in plan['increase']:
            self._log(f"买入: {data._name} -> {target:.2f}")
            self.broker.order_target_value(data=data, target=target)

    def _log(self, txt):
        if self.debug:
            print(f"[Executor] {txt}")


from abc import ABC, abstractmethod
import numpy as np

class BaseSizingMethod(ABC):
    @abstractmethod
    def calculate_weights(self, target_symbols, context_data) -> dict:
        """返回 {symbol: weight_percent}"""
        pass

# 1. 等权
class EqualWeightSizing(BaseSizingMethod):
    def calculate_weights(self, target_symbols, context_data):
        count = len(target_symbols)
        return {s: 1.0/count for s in target_symbols} if count > 0 else {}

# 2. 波动率倒数加权
class VolatilityWeightedSizing(BaseSizingMethod):
    def calculate_weights(self, target_symbols, context_data):
        # context_data 需包含 ATR 或 std 数据
        inverses = {s: 1.0/context_data[s]['atr'] for s in target_symbols}
        total_inv = sum(inverses.values())
        return {s: val/total_inv for s, val in inverses.items()}