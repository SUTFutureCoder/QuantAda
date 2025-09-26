import pandas as pd

# 假设您已经安装了 talib
try:
    import talib
except ImportError:
    print("警告：TA-Lib未安装，部分公共指标将无法计算。")
    talib = None


class CommonIndicators:
    """
    存放所有与平台无关的、可复用的指标计算逻辑
    所有方法都应该是静态的，并接收pandas.Series或numpy.ndarray作为输入
    """

    @staticmethod
    def macd(close_series: pd.Series, fastperiod=12, slowperiod=26, signalperiod=9):
        """
        计算MACD指标
        :param close_series: 收盘价序列
        :param fastperiod:
        :param slowperiod:
        :param signalperiod:
        :return:
        """
        if talib is None:
            raise ImportError("talib库未安装，无法计算MACD。")

        # TA-Lib 返回numpy数组，将其转换为带索引的Pandas Series
        macd, signal, hist = talib.MACD(close_series,
                                        fastperiod=fastperiod,
                                        slowperiod=slowperiod,
                                        signalperiod=signalperiod)

        macd_series = pd.Series(macd, index=close_series.index, name="macd")
        signal_series = pd.Series(signal, index=close_series.index, name="signal")
        hist_series = pd.Series(hist, index=close_series.index, name="hist")

        return macd_series, signal_series, hist_series

    @staticmethod
    def crossover(series1: pd.Series, series2: pd.Series) -> pd.Series:
        """
        计算两个序列的交叉信号。
        :param series1: 第一个序列 (例如 MACD线)
        :param series2: 第二个序列 (例如 Signal线)
        :return: 一个新的pandas.Series，金叉为1.0，死叉为-1.0，其他为0.0
        """
        # 金叉条件: 前一个bar s1 < s2, 当前bar s1 > s2
        crossover_signal = (series1.shift(1) < series2.shift(1)) & (series1 > series2)

        # 死叉条件: 前一个bar s1 > s2, 当前bar s1 < s2
        crossunder_signal = (series1.shift(1) > series2.shift(1)) & (series1 < series2)

        # 将布尔信号转换为 1, -1, 0
        signals = pd.Series(0.0, index=series1.index)
        signals[crossover_signal] = 1.0
        signals[crossunder_signal] = -1.0

        return signals
