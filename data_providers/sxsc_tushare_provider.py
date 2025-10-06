from functools import partial

import pandas as pd
import requests

from config import SXSC_TUSHARE_TOKEN
from .base_provider import BaseDataProvider

"""
基于山西证券tushare数据源代理
https://github.com/ActiveIce/sxscts
"""


class SxscTushareDataProvider(BaseDataProvider):
    PRIORITY = 30

    __http_url = 'http://221.204.19.233:7172'
    __timeout = 30  # 增加一个默认的请求超时时间

    def __init__(self, token: str = SXSC_TUSHARE_TOKEN):
        if not token or token == 'your_sxsc_tushare_token_here':
            print("Warning: SXSC Tushare token is not configured. SxscTushareProvider will be skipped.")
            self.token = None
        else:
            self.token = token

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame | None:
        """
        使用 sxsc-tushare 代理的 query 方法获取股票或ETF数据。
        """
        if not self.token:
            return None

        try:
            # 1. 转换symbol格式, e.g., 'SHSE.600519' -> '600519.SH'
            parts = symbol.split('.')
            ts_code = f"{parts[1]}.{parts[0].replace('SHSE', 'SH').replace('SZSE', 'SZ')}"
            stock_code = parts[1]
            df = None

            # 统一需要的字段
            fields_to_get = 'ts_code,trade_date,open,high,low,close,vol'

            # 2. 识别股票或ETF，并调用相应的API
            if len(stock_code) == 6 and not stock_code.startswith(('5', '15', '16', '18')):
                print(f"SxscTushareProvider: Detected stock symbol '{ts_code}'. Using 'daily' API.")
                df = self.daily(ts_code=ts_code,
                                start_date=start_date,
                                end_date=end_date,
                                fields=fields_to_get)
            else:
                print(f"SxscTushareProvider: Detected fund/ETF symbol '{ts_code}'. Using 'fund_daily' API.")
                df = self.fund_daily(ts_code=ts_code,
                                     start_date=start_date,
                                     end_date=end_date,
                                     fields=fields_to_get)

            if df is None or df.empty:
                return None

            # 3. 数据标准化
            df.rename(columns={'trade_date': 'datetime', 'vol': 'volume'}, inplace=True)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            df = df[['open', 'high', 'low', 'close', 'volume']]
            df['openinterest'] = 0

            # 确保数据按时间升序排列
            return df.sort_index(ascending=True)

        except Exception as e:
            print(f"SxscTushareProvider data provider failed for {symbol}: {e}")
            return None

    def query(self, api_name, fields='', **kwargs):
        """ 走通用API配置接口 """
        data = {
            'api_name': api_name,
            'token': self.token,  # 修正：使用 self.token
            'params': kwargs,
            'fields': fields
        }
        url = self.__http_url
        resp = requests.post(url, json=data, timeout=self.__timeout, headers={'Connection': 'close'})

        if resp.status_code != 200:
            raise Exception(f"HTTP Error {resp.status_code}: {resp.text}")

        result = resp.json()
        if result['code'] != 0:
            raise Exception(result['msg'])

        result_data = result['data']
        columns = result_data['fields']
        items = result_data['items']
        return pd.DataFrame(items, columns=columns)

    def __getattr__(self, name):
        """
        一个方便的语法糖，允许调用 self.daily(...) 来代替 self.query('daily', ...)。
        """
        return partial(self.query, name)
