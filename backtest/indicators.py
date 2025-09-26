import backtrader as bt


class CustomMACD(bt.indicators.MACD):
    """
    Backtrader封装：直接使用backtrader内置的MACD指标。
    它完美支持绘图和性能优化。
    """
    pass

class CustomCrossOver(bt.indicators.CrossOver):
    """
    Backtrader封装：直接使用backtrader内置的CrossOver指标。
    """
    pass
