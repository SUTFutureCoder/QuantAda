import importlib
import inspect
import os
from collections import OrderedDict
from datetime import datetime

import pandas as pd
from pandas.tseries.offsets import BDay

from config import DATA_PATH, CACHE_DATA
from .base_provider import BaseDataProvider
from .csv_provider import CsvDataProvider


class DataManager:
    def __init__(self):
        self.providers = self.auto_discover_and_sort_providers()
        self.data_path = DATA_PATH
        # 创建一个从字符串名称到提供者实例的映射
        self.provider_map = OrderedDict(
            (p.__class__.__name__.replace('DataProvider', '').lower(), p)
            for p in self.providers
        )
        # 兼容 sxsc_tushare 这种带下划线的命名
        if 'sxsctushare' in self.provider_map:
            self.provider_map['sxsc_tushare'] = self.provider_map.pop('sxsctushare')

    def auto_discover_and_sort_providers(self, provider_dir="data_providers"):
        """
        自动扫描、加载并根据PRIORITY属性排序所有数据提供者。
        """
        print("\n--- Auto-discovering Data Providers ---")
        discovered_providers = []

        # ... (文件扫描和动态导入的逻辑保持不变) ...
        for filename in os.listdir(provider_dir):
            if filename.endswith(".py") and not filename.startswith(("__", "base_", "manager.")):
                module_name = filename[:-3]
                module_path = f"{provider_dir.replace('/', '.')}.{module_name}"
                try:
                    module = importlib.import_module(module_path)
                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if issubclass(obj, BaseDataProvider) and obj is not BaseDataProvider:
                            discovered_providers.append(obj())
                            print(f"  Discovered provider: {name} (Priority: {obj.PRIORITY})")
                            break
                except Exception as e:
                    print(f"  Warning: Failed to load provider from {filename}: {e}")

        # --- 核心改动：根据PRIORITY属性进行排序 ---
        # 数值越小，优先级越高
        discovered_providers.sort(key=lambda p: p.PRIORITY)

        print("\n--- Data Provider Chain (sorted by priority) ---")
        for i, p in enumerate(discovered_providers):
            print(f"  {i + 1}. {p.__class__.__name__}")

        return discovered_providers

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None,
                 specified_sources: str = None, timeframe: str = 'Days', compression: int = 1) -> pd.DataFrame | None:
        """
        智能获取数据。
        - 如果指定了 specified_sources，则按指定顺序尝试。
        - 否则，执行默认的责任链逻辑（带增量更新）。
        """
        final_df = None

        # 路径一: 用户指定了数据源
        if specified_sources:
            print(f"--- Using specified data sources: {specified_sources} ---")
            source_names = specified_sources.lower().split()
            providers_to_use = [self.provider_map[name] for name in source_names if name in self.provider_map]
            if not providers_to_use:
                print(f"Error: None of the specified sources '{specified_sources}' are valid.")
                return None
            final_df = self._fetch_from_providers(symbol, start_date, end_date, providers_to_use, timeframe, compressio)

        # 路径二: 执行默认的责任链逻辑
        else:
            print("--- Using default data provider chain (with incremental update) ---")
            final_df = self._get_data_with_incremental_update(symbol, start_date, end_date, timeframe, compression)

        # 【最终切片】确保返回的数据在请求的日期范围内
        if final_df is not None and not final_df.empty:
            print(f"Filtering final data from {start_date} to {end_date}...")
            if start_date:
                final_df = final_df[final_df.index >= pd.to_datetime(start_date)]
            if end_date:
                final_df = final_df[final_df.index <= pd.to_datetime(end_date)]
            return final_df

        print(f"Error: All data providers failed for symbol {symbol}.")
        return None

    def _get_data_with_incremental_update(self, symbol, start_date, end_date, timeframe: str = 'Days', compression: int = 1):
        """默认的获取数据逻辑，包含检查和更新本地缓存。"""
        csv_provider = self.provider_map.get('csv')
        online_providers = [
            p for p in self.providers
            if p.__class__.__name__.replace('DataProvider', '').lower() != 'csv'
        ]

        # 如果请求的不是日线，则跳过复杂的增量更新，直接从网络获取
        if timeframe != 'Days' or compression != 1:
            print(f"Non-daily timeframe requested ({compression} {timeframe}). Bypassing incremental cache.")
            return self._fetch_from_providers(symbol, start_date, end_date, online_providers,
                                              timeframe, compression)

        # 1. 尝试从本地CSV加载完整数据以检查时效性
        df_local = csv_provider.get_data(symbol, None, None, timeframe, compression) if csv_provider else None

        if df_local is not None and not df_local.empty:
            last_date_in_csv = df_local.index[-1]
            last_trading_day = pd.to_datetime((datetime.today() - BDay(1)).date())

            # 2. 如果数据陈旧，获取增量数据并合并
            if last_date_in_csv < last_trading_day:
                print(f"Local data is stale. Fetching incremental data...")
                incremental_start_date = (last_date_in_csv + BDay(1)).strftime('%Y%m%d')
                df_incremental = self._fetch_from_providers(symbol, incremental_start_date, end_date, online_providers,
                                                            timeframe, compression)

                if df_incremental is not None and not df_incremental.empty:
                    self._append_to_cache(df_incremental, symbol, timeframe, compression)
                    return pd.concat([df_local, df_incremental])
                else:
                    print("Warning: Failed to fetch incremental data. Using stale local data.")
                    return df_local
            else:
                print("Local data is up-to-date.")
                return df_local

        # 3. 如果本地无数据，进行全量下载
        print("Local data not found or empty. Attempting full download...")
        return self._fetch_from_providers(symbol, start_date, end_date, online_providers, timeframe, compression)

    def _fetch_from_providers(self, symbol, start_date, end_date, providers,
                              timeframe: str = 'Days', compression: int = 1):
        """
        遍历给定的提供者列表获取数据。
        如果成功且来源不是CSV，则执行缓存。
        """
        for provider in providers:
            provider_name = provider.__class__.__name__
            print(f"Attempting to fetch data for {symbol} using {provider_name}...")
            try:
                df = provider.get_data(symbol, start_date, end_date, timeframe, compression)
                if df is not None and not df.empty:
                    print(f"Successfully fetched data using {provider_name}.")

                    if not isinstance(provider, CsvDataProvider):
                        self._cache_data(df, symbol, timeframe, compression)

                    return df
            except Exception as e:
                print(f"Error in {provider_name}: {e}")
                continue
        return None

    def _get_cache_filepath(self, symbol: str, timeframe: str, compression: int) -> str:
        """辅助函数：根据时间框架生成唯一的缓存文件名"""
        # 日线使用旧文件名，保持兼容
        if timeframe == 'Days' and compression == 1:
            tf_str = ""
        else:
            tf_str = f"_{timeframe}_{compression}"

        csv_filename = f"{symbol.replace('.', '_')}{tf_str}.csv"
        return os.path.join(self.data_path, csv_filename)

    def _cache_data(self, df: pd.DataFrame, symbol: str, timeframe: str = 'Days', compression: int = 1):
        """将DataFrame完整写入CSV文件（覆盖）"""
        if not CACHE_DATA: return
        if not os.path.exists(self.data_path): os.makedirs(self.data_path)
        csv_filepath = self._get_cache_filepath(symbol, timeframe, compression)
        try:
            df.to_csv(csv_filepath)
            print(f"Data for {symbol} cached to {csv_filepath}")
        except Exception as e:
            print(f"Failed to cache data for {symbol}: {e}")

    def _append_to_cache(self, df: pd.DataFrame, symbol: str):
        """将增量DataFrame追加到现有CSV文件的末尾"""
        if not CACHE_DATA: return
        csv_filepath = self._get_cache_filepath(symbol, timeframe, compression)
        if not os.path.exists(csv_filepath):
            self._cache_data(df, symbol)
            return
        try:
            df.to_csv(csv_filepath, mode='a', header=False)
            print(f"Incremental data for {symbol} appended to cache file.")
        except Exception as e:
            print(f"Failed to append data for {symbol}: {e}")
