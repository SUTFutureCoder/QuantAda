def evaluate(stats, strat=None, args=None):
    """
    混合评分 个人账户冲锋 狂暴推进版 (Turbo)
    激进进取，放宽回撤容忍度，寻找极致 Alpha，适用于A股狂暴模式。
    目标：收益主导，容忍最高 25% 的回撤。通过对收益的线性奖励逼迫AI重仓冲锋。
    """
    mdd = stats['mdd']
    total_return_pct = stats['total_return_pct']
    years = stats['years']
    total_trades = stats['total_trades']
    safe_mdd = stats['safe_mdd']
    win_rate = stats['win_rate']
    profit_factor = stats['profit_factor']

    # 1. 宽容的死亡红线 (25% 是 A 股宽止损的极限)
    if mdd > 25.0: return -20.0
    if total_trades < 20: return -20.0

    # 2. 收益主导 (年化收益越高，分数线性放大)
    ann_return = total_return_pct / years
    score_return = ann_return * 2.0

    # 3. 弱化的风险惩罚 (使用 1.0 次方代替原本红队的 1.5 次方)
    if total_return_pct > 0:
        score_risk = total_return_pct / (safe_mdd ** 1.0)
    else:
        score_risk = total_return_pct * safe_mdd

    # 4. 高胜率/高盈亏比彩蛋奖励
    bonus = 0.0
    if win_rate > 0.55: bonus += 5.0
    if profit_factor > 2.0: bonus += 5.0

    return score_return + score_risk + bonus