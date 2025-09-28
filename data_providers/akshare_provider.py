import pandas as pd
import akshare as ak
from .base_provider import BaseDataProvider


class AkshareDataProvider(BaseDataProvider):

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame | None:
        try:
            # 1. 对symbol进行更健壮的解析
            if '.' in symbol:
                ak_symbol = symbol.split('.')[1]
            else:
                ak_symbol = symbol

            # 2. 优雅地构建API参数字典
            # 仅当start_date或end_date不为None时，才将其添加到参数字典中
            api_params = {
                'symbol': ak_symbol,
                'period': 'daily',
                'adjust': 'hfq'
            }
            if start_date:
                api_params['start_date'] = start_date
            if end_date:
                api_params['end_date'] = end_date

            # 3. 使用字典解包（**）调用akshare函数
            if ak_symbol.startswith(('6', '0', '3', '8', '4')):
                print(f"Akshare: Detected stock symbol '{ak_symbol}'. Using 'stock_zh_a_hist'.")
                df = ak.stock_zh_a_hist(**api_params)
            else:
                print(f"Akshare: Detected ETF/fund symbol '{ak_symbol}'. Using 'fund_etf_hist_em'.")
                df = ak.fund_etf_hist_em(**api_params)

            if df is None or df.empty:
                return None

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

            df = df[['open', 'high', 'low', 'close', 'volume']]
            df['openinterest'] = 0

            return df.sort_index(ascending=True)

        except Exception as e:
            print(f"Akshare data provider failed for {symbol}: {e}")
            return None
