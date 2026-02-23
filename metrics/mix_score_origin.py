def evaluate(stats, strat=None, args=None):
    """
    混合评分 负对照组 适合A股的原始版 (Sharpe/16)
    测试策略绝对收益上限的“野生”基准线。
    目标：无脑刷分，暴露策略在没有任何回撤惩罚下的绝对收益上限。

    尽管错误地除以了 16（255个交易日求根号），Sharpe 比率在总分中的权重被彻底“物理阉割”了，它的贡献度甚至不到 1%，完美契合了 A 股“牛短熊长、暴涨暴跌、情绪市”的底层生态。。
    公式在实质上变成了一个纯粹的“双因子暴力驱动模型”：只看总收益（Return）和抗回撤能力（Calmar），完全无视波动率。
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