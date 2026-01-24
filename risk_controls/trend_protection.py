import numpy as np

from common import mytt
from risk_controls.base_risk_control import BaseRiskControl


class TrendProtection(BaseRiskControl):
    """
    趋势保护风控模块 (Trend Protection)

    用于判断当前市场/个股的形势。当形势恶化时，强制空仓(SELL)，
    防止在主跌浪中'接飞刀'。

    支持两种判定方法 (通过 'method' 参数切换):
    1. 'ma': 价格与均线关系。
       - 逻辑: Close < MA(period) -> SELL
       - 适用: 简单的熊市过滤器。

    2. 'trend_score': 趋势得分 (Annualized Return * R^2)。
       - 逻辑: TrendScore < threshold -> SELL
       - 适用: 识别平稳下跌的趋势，过滤震荡市。
    """

    params = {
        'method': 'ma',  # 'ma' 或 'trend_score'
        'period': 60,  # 均线周期 或 趋势得分的计算窗口
        'threshold': 0.0,  # 趋势得分阈值 (仅 method='trend_score' 时有效)
        'strict_slope': False  # (仅 ma 模式) 是否要求均线本身必须向上
    }

    def check(self, data) -> str:
        # 1. 准备数据
        # 获取该标的截至当前的所有收盘价 (Backtrader中 len(data) 为当前已处理bar数)
        # 转换为 numpy array 供 mytt 使用
        current_len = len(data)

        # 兜底: 数据不足时不操作
        if current_len < self.p.period + 5:
            return None

        close = np.array(data.close.get(ago=0, size=current_len))

        # 2. 根据方法执行检查
        signal = None

        if self.p.method == 'ma':
            signal = self._check_ma(close)
        elif self.p.method == 'trend_score':
            signal = self._check_trend_score(close)

        return signal

    def _check_ma(self, close):
        """MA均线逻辑"""
        period = self.p.period

        # 计算均线
        ma_values = mytt.MA(close, period)

        current_close = close[-1]
        current_ma = ma_values[-1]

        # 核心逻辑: 价格在均线之下，视为弱势，清仓
        if current_close < current_ma:
            return 'SELL'

        # 可选逻辑: 均线本身是否向下 (均线拐头向下)
        if self.p.strict_slope:
            # 比较当前MA和昨日MA
            prev_ma = ma_values[-2]
            if current_ma < prev_ma:
                return 'SELL'

        return None
