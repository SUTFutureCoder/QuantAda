def evaluate(stats, strat=None, args=None):
    """
    混合评分 负对照组 怀旧原始版 (Sharpe/16)
    测试策略绝对收益上限的“野生”基准线。
    目标：无脑刷分，暴露策略在没有任何回撤惩罚下的绝对收益上限。
    """
    """
        混合评分 负对照组 怀旧原始版 (Sharpe/16)
        完全还原系统最早的硬编码打分逻辑，保证数学上 1:1 等价。
        目标：盯着回撤（Calmar）和总收益（Return），几乎完全无视波动（Sharpe）。
        """
    # 1. 提取标准化特征
    # 注意：系统底层 stats['total_return_pct'] 是百分比 (如 504.0)
    # 原始逻辑中使用的是小数形式 (如 5.04)，所以需要除以 100.0 还原量纲
    total_return_raw = stats['total_return_pct'] / 100.0

    calmar = stats['calmar']
    sharpe = stats['sharpe']
    total_trades = stats['total_trades']

    # 2. 原始权重精确还原
    # Calmar: 2.0 (生存第一)
    # Return: 2.0 (收益第二)
    # Sharpe: 1.0 / 16.0 (平滑第三)
    raw_score = (calmar * 2.0) + (total_return_raw * 2.0) + (sharpe / 16.0)

    # 3. 惩罚逻辑还原 (保留梯度)
    penalty = 0.0
    if total_trades < 10:
        penalty = -10.0

    return raw_score + penalty