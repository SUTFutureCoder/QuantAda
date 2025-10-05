from abc import ABC, abstractmethod

import pandas as pd


class BaseDataProvider(ABC):
    """
    数据提供者的抽象基类
    """

    @abstractmethod
    def get_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame | None:
        """
        获取指定交易标的的历史行情数据

        :param symbol: 交易标的代码，例如：'SHSE.510300'
        :param start_date: 开始日期，例如：'2020101'
        :param end_date: 结束日期，例如：'20250101'
        :return: 标准化后的Pandas DataFrame，若获取失败则返回None。
        DataFrame必须包含['datetime', 'open', 'high', 'low', 'close', 'volume']
        且以'datetime'为索引
        """
        pass
