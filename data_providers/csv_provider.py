import os

import pandas as pd

from config import DATA_PATH
from .base_provider import BaseDataProvider


class CsvDataProvider(BaseDataProvider):
    """
    从本地CSV文件加载数据
    """
    PRIORITY = 10  # 最高优先级，先从本地CSV读取

    def __init__(self, data_path: str = DATA_PATH):
        self.data_path = data_path
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

    @staticmethod
    def get_cache_filepath(data_path: str, symbol: str, timeframe: str, compression: int) -> str:
        """辅助函数：根据时间框架生成唯一的缓存文件名"""
        if timeframe == 'Days' and compression == 1:
            tf_str = ""
        else:
            tf_str = f"_{timeframe}_{compression}"

        csv_filename = f"{symbol.replace('.', '_')}{tf_str}.csv"
        return os.path.join(data_path, csv_filename)

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None,
                 timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame:

        csv_filepath = self.get_cache_filepath(self.data_path, symbol, timeframe, compression)

        if not os.path.exists(csv_filepath):
            return None

        try:
            df = pd.read_csv(csv_filepath, index_col='datetime', parse_dates=True)

            # 根据日期筛选
            if start_date:
                df = df[df.index >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df.index <= pd.to_datetime(end_date)]

            return df
        except Exception as e:
            print(f"Error reading CSV file {csv_filepath}: {e}")
            return None
