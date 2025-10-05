import os

import pandas as pd

from config import DATA_PATH
from .base_provider import BaseDataProvider


class CsvDataProvider(BaseDataProvider):
    """
    从本地CSV文件加载数据
    """

    def __init__(self, data_path: str = DATA_PATH):
        self.data_path = data_path
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame | None:
        csv_filename = f"{symbol.replace('.', '_')}.csv"
        csv_filepath = os.path.join(self.data_path, csv_filename)

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
