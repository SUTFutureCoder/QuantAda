import os
import pandas as pd
from typing import List
from .base_provider import BaseDataProvider
from .csv_provider import CsvDataProvider
from config import DATA_PATH, CACHE_DATA


class DataManager:
    def __init__(self, providers: List[BaseDataProvider]):
        self.providers = providers
        self.data_path = DATA_PATH

    def get_data(self, symbol: str, start_date: str = None, end_date: str = '20250101') -> pd.DataFrame | None:
        """
        按优先级依次尝试从数据提供者获取数据。
        如果从非CSV源获取成功，则自动缓存到本地CSV文件。
        """
        for provider in self.providers:
            provider_name = provider.__class__.__name__
            print(f"Attempting to fetch data for {symbol} using {provider_name}...")

            try:
                df = provider.get_data(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    print(f"Successfully fetched data using {provider_name}")

                    if not isinstance(provider, CsvDataProvider):
                        self._cache_data(df, symbol)

                    return df
            except Exception as e:
                print(f"Error in {provider_name}: {e}")
                continue

        print(f"Error: All data providers failed for symbol {symbol}.")
        return None

    def _cache_data(self, df: pd.DataFrame, symbol: str):
        if not CACHE_DATA:
            return

        """将DataFrame保存为CSV文件"""
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

        csv_filename = f"{symbol.replace('.', '_')}.csv"
        csv_filepath = os.path.join(self.data_path, csv_filename)

        try:
            df.to_csv(csv_filepath)
            print(f"Data for {symbol} cached to {csv_filepath}")
        except Exception as e:
            print(f"Failed to cache data for {symbol}: {e}")