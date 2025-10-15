from .mytt import *

"""
请在回测和实盘策略引入这个指标库，包含了talib和mytt的指标
存放所有与平台无关的、可复用的指标计算逻辑
"""


def CrossOver(S1, S2):
    """
    计算两个序列的交叉情况。
    :param S1: 快线 (e.g., DIF)
    :param S2: 慢线 (e.g., DEA)
    :return: 一个Numpy数组，金叉为1，死叉为-1，无交叉为0。
    """
    # 确保输入是Pandas Series，以便使用矢量化操作
    s1_series = pd.Series(S1)
    s2_series = pd.Series(S2)

    # 金叉条件: 今天快线 > 慢线，且昨天快线 < 慢线
    golden_cross = (s1_series > s2_series) & (s1_series.shift(1) < s2_series.shift(1))

    # 死叉条件: 今天快线 < 慢线，且昨天快线 > 慢线
    dead_cross = (s1_series < s2_series) & (s1_series.shift(1) > s2_series.shift(1))

    # 生成结果序列：金叉为1，死叉为-1，其他为0
    # .astype(int) 会将布尔值的 True/False 转换为 1/0
    crossover_signal = golden_cross.astype(int) - dead_cross.astype(int)

    return np.array(crossover_signal)
