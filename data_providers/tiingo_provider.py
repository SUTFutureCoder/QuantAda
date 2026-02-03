import pandas as pd
from tiingo import TiingoClient
import config
from data_providers.base_provider import BaseDataProvider


class TiingoProvider(BaseDataProvider):
    """
    Tiingo 数据源 (付费/高质量)

    特点：
    1. 价格低廉 ($10/mo)，适合个人量化。
    2. 数据极度干净，自动处理复权。
    3. 覆盖美股 (全量)、A股 (部分)、外汇、加密货币。
    """

    PRIORITY = 50

    def __init__(self, token: str = config.TIINGO_TOKEN):
        # 请在 config.py 中添加 TIINGO_KEY = 'your_key'
        if not token or token == 'your_token_here':
            print("Warning: TIINGO_KEY not found in config. TiingoProvider unavailable.")
            self.client = None
        else:
            self.client = TiingoClient({'api_key': token, 'session': True})

    def _map_symbol(self, symbol):
        """
        映射规则：
        - A股: SHSE.510300 -> 510300 (Tiingo A股直接用6位数字)
        - 美股: STK.AAPL.USD -> AAPL
        - 其他: 纯数字直接透传
        """
        sym = symbol.upper()

        # --- 1. A股处理 (去除 SHSE./SZSE.) ---
        if 'SHSE.' in sym or 'SZSE.' in sym:
            # 提取最后一段数字代码
            return sym.split('.')[-1]

        # --- 2. 美股处理 (STK.NVDA.USD -> NVDA) ---
        if 'STK.' in sym and 'USD' in sym:
            return sym.split('.')[1]

        # --- 3. 纯数字兜底 (防止传入纯数字代码被忽略) ---
        if sym.isdigit():
            return sym

        # --- 4. 默认兜底 (如 AAPL) ---
        # 如果不含 '.'，假设是美股 Ticker 直接返回
        if '.' not in sym:
            return sym

        # 其他情况保持原样 (可能用户传了特殊格式)
        return sym

    def get_data(self, symbol, start_date=None, end_date=None, timeframe='Days', compression=1):
        if not self.client:
            return None

        # Tiingo 主要优势是日线数据 (EOD)
        # 如果需要分钟线，Tiingo IEX 接口也支持，但历史较短
        if timeframe != 'Days':
            print(f"[Tiingo] Warning: Tiingo best supports Daily data. Intraday might vary.")

        tiingo_symbol = self._map_symbol(symbol)
        print(f"[Tiingo] Fetching {tiingo_symbol}...")

        try:
            # Tiingo API 返回的是 json list
            data = self.client.get_ticker_price(
                tiingo_symbol,
                fmt='json',
                startDate=start_date,
                endDate=end_date,
                frequency='daily'  # or 'resample' for intraday if paid IEX
            )

            if not data:
                print(f"[Tiingo] No data returned for {tiingo_symbol}")
                return None

            df = pd.DataFrame(data)

            # Tiingo 返回列: date, adjClose, adjHigh, adjLow, adjOpen, adjVolume, ...
            # 我们直接用复权后的数据 (adj*) 作为主力数据，防止回测出现价格跳空
            df.rename(columns={
                'date': 'datetime',
                'adjOpen': 'open',
                'adjHigh': 'high',
                'adjLow': 'low',
                'adjClose': 'close',
                'adjVolume': 'volume'
            }, inplace=True)

            df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(None)  # 移除时区
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)

            return df[['open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            print(f"[Tiingo] Error fetching {symbol}: {e}")
            return None


if __name__ == '__main__':
    # 单元测试
    p = TiingoProvider()

    # 测试 1: A股
    print("\n--- Test A-Share ---")
    df_cn = p.get_data("SHSE.510300", start_date="20240101")
    if df_cn is not None: print(df_cn.tail(2))

    # 测试 2: 美股 (QuantAda 格式)
    print("\n--- Test US Stock (IB Style) ---")
    df_us = p.get_data("STK.NVDA.USD", start_date="20240101") # 等价于NVDA
    if df_us is not None: print(df_us.tail(2))
