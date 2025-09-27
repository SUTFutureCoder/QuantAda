import pandas as pd
import akshare as ak
from .base_provider import BaseDataProvider


class AkshareDataProvider(BaseDataProvider):

    def get_data(self, symbol: str, start_date: str = None, end_date: str = '20250101') -> pd.DataFrame | None:
        try:
            # 转换symbol格式，SHSE.510300 -> 510300
            ak_symbol = symbol
            if symbol.find('.'):
                ak_symbol = symbol.split('.')[1]

            # 简单的股票代码识别逻辑：A股主板、创业板、科创板、北交所等
            if ak_symbol.startswith(('6', '0', '3', '8', '4')):
                print(f"Akshare: Detected stock symbol '{ak_symbol}'. Using 'stock_zh_a_hist'.")
                df = ak.stock_zh_a_hist(symbol=ak_symbol, period='daily', end_date=end_date, adjust='hfq')
            else:
                print(f"Akshare: Detected ETF/fund symbol '{ak_symbol}'. Using 'fund_etf_hist_em'.")
                df = ak.fund_etf_hist_em(symbol=ak_symbol, period='daily', end_date=end_date, adjust='hfq')

            if df is None or df.empty:
                return None

            # --- 数据标准化 ---
            df.rename(columns={
                '日期': 'datetime',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume'
            }, inplace=True)

            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)

            # 筛选和添加必要列
            df = df[['open', 'high', 'low', 'close', 'volume']]
            df['openinterest'] = 0

            if start_date:
                df = df[df.index >= pd.to_datetime(start_date)]

            return df.sort_index(ascending=True)

        except Exception as e:
            print(f"Akshare data provider failed for {symbol}: {e}")
            return None
