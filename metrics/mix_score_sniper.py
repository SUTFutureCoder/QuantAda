def evaluate(stats, strat=None, args=None):
    """
    混合评分 高胜率猎手 精准狙击版 (Sniper)
    不管牛熊，只做胜率极高的确定性交易。
    目标：不求总收益最高，但求每一枪都不虚发。极度奖励胜率和盈亏因子。
    """
    win_rate = stats['win_rate']
    profit_factor = stats['profit_factor']
    total_trades = stats['total_trades']
    mdd = stats['mdd']

    # 1. 极其严苛的质量红线
    if win_rate < 0.55: return -20.0      # 胜率低于 55% 直接杀
    if profit_factor < 1.5: return -20.0  # 盈亏比低于 1.5 直接杀
    if total_trades < 30: return -20.0    # 样本太少不算数

    # 2. 质量核心打分：盈亏因子与胜率的共振
    quality_score = (profit_factor * 3.0) + (win_rate * 100.0 * 0.5)

    # 3. 辅助风控扣分
    if mdd > 15.0:
        quality_score -= (mdd - 15.0) * 2.0

    return quality_score