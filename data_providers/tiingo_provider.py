import pandas as pd
from tiingo import TiingoClient

import config
from data_providers.base_provider import BaseDataProvider


class TiingoDataProvider(BaseDataProvider):
    """
    Tiingo 数据源 (付费/高质量)
    """

    PRIORITY = 50

    def __init__(self, token: str = config.TIINGO_TOKEN):
        if not token or token == 'your_token_here':
            print("Warning: TIINGO_KEY not found in config. TiingoProvider unavailable.")
            self.client = None
        else:
            self.client = TiingoClient({'api_key': token, 'session': True})

    def _map_symbol(self, symbol):
        sym = symbol.upper()
        if 'SHSE.' in sym or 'SZSE.' in sym:
            return sym.split('.')[-1]
        if 'STK.' in sym and 'USD' in sym:
            return sym.split('.')[1]
        if sym.isdigit():
            return sym
        if '.' not in sym:
            return sym
        return sym.split(".")[0]

    def get_data(self, symbol, start_date=None, end_date=None, timeframe='Days', compression=1):
        if not self.client:
            return None

        if timeframe != 'Days':
            print(f"[Tiingo] Warning: Tiingo best supports Daily data. Intraday might vary.")

        tiingo_symbol = self._map_symbol(symbol)
        print(f"[Tiingo] Fetching {tiingo_symbol}...")

        try:
            data = self.client.get_ticker_price(
                tiingo_symbol,
                fmt='json',
                startDate=start_date,
                endDate=end_date,
                frequency='daily'
            )

            if not data:
                print(f"[Tiingo] No data returned for {tiingo_symbol}")
                return None

            df = pd.DataFrame(data)

            # === 明确列选择，防止产生重复列 ===
            # Tiingo 同时返回 'open' 和 'adjOpen'。
            # 直接 rename 可能会导致 DataFrame 中存在两个 'open' 列。
            # 这里的做法是：只提取我们需要的 adj 列，并直接赋值给新列名。

            # 1. 检查是否存在复权数据 (股票通常有，加密货币/外汇可能没有)
            if 'adjClose' in df.columns:
                # 股票模式：强制使用复权数据
                # 显式构造新 DataFrame，丢弃未复权数据，杜绝 duplicates
                clean_df = df[['date', 'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'adjVolume']].copy()
                clean_df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            else:
                # 兜底模式：使用原始数据
                base_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
                # 取交集防止列缺失
                existing_cols = [c for c in base_cols if c in df.columns]
                clean_df = df[existing_cols].copy()
                clean_df.rename(columns={'date': 'datetime'}, inplace=True)

            # 2. 标准化处理
            clean_df['datetime'] = pd.to_datetime(clean_df['datetime']).dt.tz_localize(None)
            clean_df.set_index('datetime', inplace=True)
            clean_df.sort_index(inplace=True)

            # 3. 再次确保只返回标准 OHLCV
            return clean_df[['open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            print(f"[Tiingo] Error fetching {symbol}: {e}")
            return None