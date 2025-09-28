import pandas as pd
import tushare as ts
from .base_provider import BaseDataProvider
from config import TUSHARE_TOKEN


class TushareDataProvider(BaseDataProvider):
    """
    使用Tushare Pro获取数据
    """
    def __init__(self, token: str = TUSHARE_TOKEN):
        if not token or token == 'your_tushare_token_here':
            print("Warning: Tushare token is not configured. Tushare provider will be skipped.")
            self.pro = None
        else:
            ts.set_token(token)
            self.pro = ts.pro_api()

    def get_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame | None:
        if not self.pro:
            return None

        try:
            # 转换symbol格式, e.g., 'SHSE.600519' -> '600519.SH'
            parts = symbol.split('.')
            ts_symbol = f"{parts[1]}.{parts[0].replace('SHSE', 'SH').replace('SZSE', 'SZ')}"
            stock_code = parts[1]

            # 基金/股票识别逻辑
            if len(stock_code) == 6 and not stock_code.startswith(('5', '15', '16', '18')):
                print(f"Tushare: Detected stock symbol '{ts_symbol}'. Using 'daily' API.")
                df = self.pro.daily(ts_code=ts_symbol, start_date=start_date, end_date=end_date)
            else:
                print(f"Tushare: Detected fund symbol '{ts_symbol}'. Using 'fund_daily' API.")
                df = self.pro.fund_daily(ts_code=ts_symbol, start_date=start_date, end_date=end_date)

            if df is None or df.empty:
                return None

            # --- 数据标准化 ---
            df.rename(columns={
                'trade_date': 'datetime',
                'vol': 'volume'
            }, inplace=True)

            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)

            df = df[['open', 'high', 'low', 'close', 'volume']]
            df['openinterest'] = 0

            # Tushare返回的数据是降序的，需要反转
            return df.sort_index(ascending=True)

        except Exception as e:
            print(f"Tushare data provider failed for {symbol}: {e}")
            return None


