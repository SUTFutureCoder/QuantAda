import math


def evaluate(stats, strat=None, args=None):
    """
    混合评分 公司账户保活 保活稳健版 (Robust Defender)
    绝对防御机制，把回撤卡死在反人类级别，适用于资金体量大的底仓。
    目标：绝对防御，利用 MDD 的 1.5 次方惩罚，榨取平滑如直线的净值曲线。
    """
    mdd = stats['mdd']
    years = stats['years']
    total_trades = stats['total_trades']
    sharpe = stats['sharpe']
    total_return_pct = stats['total_return_pct']
    safe_mdd = stats['safe_mdd']

    # 1. 动态交易频率门槛 (拒绝空仓装死)
    min_req_trades = int(12 * years)
    penalty = 0.0

    if total_trades < min_req_trades:
        penalty -= (min_req_trades - total_trades)

    # 2. 严厉的回撤红线 (超 20% 即死刑)
    if mdd > 20.0:
        penalty -= (mdd - 20.0) * 2.0

    if penalty < 0: return -20.0 + penalty

    # 3. 对数夏普 (奖励交易次数，但边际递减)
    capped_n = min(max(total_trades, 1), int(120 * years))
    stat_sharpe = max(sharpe, 0.0) * math.log10(capped_n)

    # 4. 1.5 次方回撤惩罚 (最痛苦的风控记忆)
    if total_return_pct >= 0:
        p_calmar = total_return_pct / (safe_mdd ** 1.5)
    else:
        p_calmar = total_return_pct * math.sqrt(safe_mdd)

    return stat_sharpe + p_calmar