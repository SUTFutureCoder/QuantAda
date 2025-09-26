import backtrader as bt


class CustomMACD(bt.indicators.MACD):
    """
    Backtrader封装：直接使用backtrader内置的MACD指标。
    它完美支持绘图和性能优化。
    """
    # 为了在图表主窗口上显示而不是作为子图，可以添加以下设置
    plotinfo = dict(subplot=False)
    # 为了美观，可以自定义颜色
    plotlines = dict(
        macd=dict(color='blue', _fill_gt=0, _fill_lt=0),
        signal=dict(color='orange'),
    )


class CustomCrossOver(bt.indicators.CrossOver):
    """
    Backtrader封装：直接使用backtrader内置的CrossOver指标。
    """
    pass
