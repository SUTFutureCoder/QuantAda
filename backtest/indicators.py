import backtrader as bt
import pandas as pd

from common.indicators import CommonIndicators


class CustomMACD(bt.Indicator):
    """Backtrader封装：使用通用计算逻辑的MACD指标"""
    lines = ('macd', 'signal', 'histo')

    def __init__(self):
        close_series = pd.Series(self.data.close.get(size=len(self.data)))
        macd_s, signal_s, hist_s = CommonIndicators.macd(close_series)
        self.lines.macd.extend(macd_s.values)
        self.lines.signal.extend(signal_s.values)
        self.lines.histo.extend(hist_s.values)


class CustomCrossOver(bt.Indicator):
    """Backtrader封装：使用通用计算逻辑的CrossOver指标"""
    lines = ('crossover',)
    params = (('a', None), ('b', None),)

    def __init__(self):
        series_a = pd.Series(self.p.a.get(size=len(self.p.a)))
        series_b = pd.Series(self.p.b.get(size=len(self.p.b)))
        cross_series = CommonIndicators.crossover(series_a, series_b)
        self.lines.crossover.extend(cross_series.values)
