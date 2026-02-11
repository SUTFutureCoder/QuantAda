import config

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

        # PortfolioRebalancer._log_plan(plan, current_positions, target_symbols, target_value, rebalance_threshold)

        return plan

    @staticmethod
    def _log_plan(plan, current_positions, target_symbols, target_value, rebalance_threshold):
        """格式化输出调仓摘要，受 config.LOG 控制"""
        if not getattr(config, 'LOG', True):
            return

        # 辅助工具：提取名称并格式化数值
        _n = lambda x: x._name if hasattr(x, '_name') else str(x)
        _fmt_list = lambda items: [_n(i) for i in items]
        _fmt_pair = lambda items: [f"{_n(i[0])}→{i[1]:,.0f}" for i in items]

        print(f"\n{'=' * 20} 调仓计划生成 {'=' * 20}")
        print(f"目标市值/股: {target_value:,.2f} | 偏离阈值: {rebalance_threshold:.1%}")
        print(f"当前持仓: {[f'{_n(k)}:{v:,.0f}' for k, v in current_positions.items()]}")
        print(f"目标标的: {_fmt_list(target_symbols)}")
        print(f"执行清单: ")

        if plan['sell_clear']:
            print(f"  - [清仓]: {_fmt_list(plan['sell_clear'])}")

        if plan['reduce']:
            print(f"  - [减仓]: {_fmt_pair(plan['reduce'])}")

        if plan['increase']:
            print(f"  - [加仓]: {_fmt_pair(plan['increase'])}")

        if not any([plan['sell_clear'], plan['reduce'], plan['increase']]):
            print("  - 无需调整 (未触及偏离阈值)")

        print(f"{'=' * 54}\n")


class OrderExecutor:
    """
    专门的执行器，负责把计划变成订单。
    """

    def __init__(self, broker, debug=True):
        self.broker = broker
        self.debug = debug

    def execute_plan(self, plan):
        """执行调仓计划：利用 order_target_value 及其内部智能逻辑"""

        # 第一步：处理所有卖出动作 (清仓 + 减仓)
        # 必须先执行卖出，以注册在途资金，激活后续买单的延迟重试逻辑
        for data in plan['sell_clear']:
            self._log(f"执行清仓: {data._name}")
            self.broker.order_target_value(data=data, target=0.0)

        for data, target in plan['reduce']:
            self._log(f"执行减仓: {data._name} -> {target:.2f}")
            self.broker.order_target_value(data=data, target=target)

        # 第二步：处理所有买入动作 (补仓/开仓)
        # 如果此时现金不足，BaseLiveBroker 会检测到上面的卖单，自动进入 Deferred 队列
        for data, target in plan['increase']:
            self._log(f"执行补仓/开仓: {data._name} -> {target:.2f}")
            self.broker.order_target_value(data=data, target=target)

    def _log(self, txt):
        if self.debug:
            print(f"[Executor] {txt}")


from abc import ABC, abstractmethod

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