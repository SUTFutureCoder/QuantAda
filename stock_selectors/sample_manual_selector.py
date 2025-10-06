from .base_selector import BaseSelector


class SampleManualSelector(BaseSelector):
    """
    手动指定选股器。
    在 run_selection 方法中返回一个预定义的股票列表。
    """
    def run_selection(self) -> list[str]:
        print("[Selector] Returning a predefined manual list of symbols.")
        portfolio = [
            'SHSE.510300',  # 沪深300 ETF
            'SZSE.000001',  # 平安银行
            'SHSE.600519',  # 贵州茅台
        ]
        return portfolio
