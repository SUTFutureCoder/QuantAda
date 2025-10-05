def entry_signal_macd_golden_cross(strategy) -> bool:
    """
    可复用的入场规则：判断是否发生MACD金叉。
    :param strategy: 策略实例 (用于访问其指标)。
    :return: 如果是金叉信号，返回True。
    """
    return strategy.crossover[0] > 0

def exit_signal_macd_dead_cross(strategy) -> bool:
    """
    可复用的出场规则：判断是否发生MACD死叉。
    :param strategy: 策略实例。
    :return: 如果是死叉信号，返回True。
    """
    return strategy.crossover[0] < 0
