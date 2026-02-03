import importlib
import inspect
import os
from collections import OrderedDict

import pandas as pd

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
                 specified_sources: str = None, timeframe: str = 'Days', compression: int = 1, refresh: bool = False) -> pd.DataFrame:
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
            final_df = self._fetch_from_providers(symbol, start_date, end_date, providers_to_use, timeframe, compression)

        # 路径二: 执行默认的责任链逻辑
        else:
            print(f"--- Using default data provider chain (Refresh={refresh}) ---")
            final_df = self._get_data_smart(symbol, start_date, end_date, timeframe, compression, refresh)

        # 【最终切片】确保返回的数据在请求的日期范围内
        if final_df is not None and not final_df.empty:
            print(f"Filtering final data from {start_date} to {end_date}...")

            # 辅助函数：将输入的日期字符串对齐到 df 索引的时区，防止比较报错
            def align_date(dt_input, index):
                dt = pd.to_datetime(dt_input)
                if index.tz is not None:
                    # 如果索引有时区，输入时间也加上同样的时区
                    if dt.tz is None:
                        return dt.tz_localize(index.tz)
                    else:
                        return dt.tz_convert(index.tz)
                else:
                    # 如果索引无时区，输入时间也去掉时区
                    if dt.tz is not None:
                        return dt.tz_convert(None)
                return dt

            if start_date:
                start_dt = align_date(start_date, final_df.index)
                final_df = final_df[final_df.index >= start_dt]
            if end_date:
                end_dt = align_date(end_date, final_df.index)
                final_df = final_df[final_df.index <= end_dt]

            return final_df

        print(f"Error: All data providers failed for symbol {symbol}.")
        return None

    def _get_data_smart(self, symbol, start_date, end_date, timeframe: str, compression: int, refresh: bool):
        csv_provider = self.provider_map.get('csv')
        online_providers = [
            p for p in self.providers
            if p.__class__.__name__.replace('DataProvider', '').lower() != 'csv'
        ]

        # 1. 如果是非日线，或者是强制刷新模式 -> 直接走网络
        if refresh or not csv_provider:
            if refresh:
                print(f"Force refresh requested. Bypassing cache for {symbol}...")

            return self._fetch_from_providers(symbol, start_date, end_date, online_providers, timeframe, compression)

        # 2. 默认模式：尝试读取缓存
        df_local = csv_provider.get_data(symbol, None, None, timeframe, compression) if csv_provider else None

        if df_local is not None and not df_local.empty:
            print("Local cache found. Using it (add --refresh to force update).")
            return df_local

        # 3. 缓存不存在 -> 走网络
        print("Local cache not found. Attempting download...")
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

    def _cache_data(self, df: pd.DataFrame, symbol: str, timeframe: str = 'Days', compression: int = 1):
        """将DataFrame完整写入CSV文件（覆盖）"""
        if not CACHE_DATA: return
        if not os.path.exists(self.data_path): os.makedirs(self.data_path)
        csv_filepath = CsvDataProvider.get_cache_filepath(self.data_path, symbol, timeframe, compression)
        try:
            df.to_csv(csv_filepath, mode='w')
            print(f"Data for {symbol} cached to {csv_filepath}")
        except Exception as e:
            print(f"Failed to cache data for {symbol}: {e}")
