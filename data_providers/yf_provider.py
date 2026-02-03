import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from data_providers.base_provider import BaseDataProvider


class YFinanceProvider(BaseDataProvider):
    """
    Yahoo Finance 数据源 (基于 yfinance)

    功能特点：
    1. 全球覆盖：支持 A股、美股、港股、外汇等。
    2. 智能映射：自动将 QuantAda/IB/GM 格式代码转换为 Yahoo 格式。
       - A股: SHSE.600000 -> 600000.SS
       - 美股: STK.AAPL.USD -> AAPL
       - 港股: STK.0700.HKD -> 0700.HK
    3. 免费备份：无需 API Key，适合作为灾备或开发测试源。

    注意：
    - 分钟线数据只能获取最近 60 天 (yfinance 限制)。
    - 建议主要用于日线 (Days) 回测。
    """

    PRIORITY = 60

    def _map_symbol(self, symbol):
        """
        将通用代码转换为 Yahoo Finance 格式
        """
        symbol = symbol.upper()

        # --- 1. 处理 A股 (SHSE/SZSE) ---
        if 'SHSE' in symbol or 'SZSE' in symbol:
            # 格式如: SHSE.600000, SZSE.300059
            code = symbol.split('.')[-1]
            if 'SHSE' in symbol:
                return f"{code}.SS"
            elif 'SZSE' in symbol:
                return f"{code}.SZ"

        # 纯数字兜底 (猜测 A股)
        if symbol.isdigit() and len(symbol) == 6:
            if symbol.startswith('6'):
                return f"{symbol}.SS"
            else:
                return f"{symbol}.SZ"

        # --- 2. 处理 IBKR 格式 (STK.AAPL.USD) ---
        if 'STK.' in symbol:
            parts = symbol.split('.')
            ticker = parts[1]
            currency = parts[2] if len(parts) > 2 else 'USD'

            if currency == 'HKD':
                # 港股: 0700 -> 0700.HK
                # 移除前导零? Yahoo 通常需要完整 4 位，如 0700.HK
                return f"{ticker.zfill(4)}.HK"
            elif currency in ['USD', 'US']:
                # 美股直接返回 Ticker
                return ticker
            else:
                # 其他市场尝试直接用 Ticker
                return ticker

        # --- 3. 处理 港股/其他 (HKSE.0700) ---
        if 'HKSE' in symbol:
            code = symbol.split('.')[-1]
            return f"{code.zfill(4)}.HK"

        # --- 4. 外汇 (CASH.EUR.USD) ---
        if 'CASH.' in symbol:
            # IB: CASH.EUR.USD -> Yahoo: EURUSD=X
            parts = symbol.split('.')
            base = parts[1]
            quote = parts[2]
            return f"{base}{quote}=X"

        # --- 5. 默认直接透传 (如 AAPL, QQQ, BTC-USD) ---
        return symbol

    def _map_interval(self, timeframe, compression):
        """
        将 Backtrader 周期转换为 yfinance interval
        Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        """
        tf = timeframe.lower()

        if 'minute' in tf:
            if compression == 1: return '1m'
            if compression == 2: return '2m'
            if compression == 5: return '5m'
            if compression == 15: return '15m'
            if compression == 30: return '30m'
            if compression == 60: return '1h'
            if compression == 90: return '90m'
            # 兜底：yfinance 不支持任意分钟，回落到 1m 或报错
            return '1m'

        elif 'hour' in tf:
            return '1h'

        elif 'day' in tf:
            if compression == 1: return '1d'
            if compression == 5: return '5d'
            return '1d'

        elif 'week' in tf:
            return '1wk'

        elif 'month' in tf:
            return '1mo'

        return '1d'

    def get_data(self, symbol, start_date=None, end_date=None, timeframe='Days', compression=1):
        """获取数据主函数"""
        yf_symbol = self._map_symbol(symbol)
        yf_interval = self._map_interval(timeframe, compression)

        print(f"[YFinance] Fetching {yf_symbol} (Interval: {yf_interval})...")

        try:
            # 处理日期格式 YYYY-MM-DD
            start_str = pd.to_datetime(str(start_date)).strftime('%Y-%m-%d') if start_date else None
            end_str = pd.to_datetime(str(end_date)).strftime('%Y-%m-%d') if end_date else None

            # 下载数据
            # auto_adjust=False: 获取原始 OHLC (和 IBKR/A股行情一致)，不进行复权调整
            # 如果需要前复权，改设为 True
            df = yf.download(
                tickers=yf_symbol,
                start=start_str,
                end=end_str,
                interval=yf_interval,
                auto_adjust=False,
                progress=False,
                multi_level_index=False  # 防止新版 yfinance 返回多层索引
            )

            if df is None or df.empty:
                print(f"[YFinance] No data found for {yf_symbol}")
                return None

            # --- 数据清洗 ---
            # 1. 扁平化索引 (应对新版 yfinance 偶尔返回 Ticker 作为列名层级的问题)
            if isinstance(df.columns, pd.MultiIndex):
                try:
                    # 尝试提取 'Close', 'Open' 等层级
                    df.columns = df.columns.get_level_values(0)
                except:
                    pass

            # 2. 重命名列 (统一为小写，适配 Backtrader)
            # yfinance 返回: Open, High, Low, Close, Adj Close, Volume
            rename_map = {
                'Open': 'open', 'High': 'high', 'Low': 'low',
                'Close': 'close', 'Volume': 'volume',
                'Adj Close': 'adj_close'
            }
            df = df.rename(columns=rename_map)

            # 3. 必须包含的列检查
            required = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required):
                print(f"[YFinance] Missing columns in data. Got: {df.columns}")
                return None

            # 4. 时区处理 (移除时区，转为 Naive，防止 Backtrader 报错)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            df.index.name = 'datetime'

            # 5. 返回标准 DataFrame
            print(f"[YFinance] Success: {len(df)} bars.")
            return df[required]

        except Exception as e:
            print(f"[YFinance] Error fetching {yf_symbol}: {e}")
            return None


if __name__ == '__main__':
    # 单元测试
    p = YFinanceProvider()

    # 测试 1: A股
    print("\n--- Test A-Share ---")
    df_cn = p.get_data("SHSE.510300", start_date="20240101")
    if df_cn is not None: print(df_cn.tail(2))

    # 测试 2: 美股 (QuantAda 格式)
    print("\n--- Test US Stock (IB Style) ---")
    df_us = p.get_data("STK.NVDA.USD", start_date="20240101")
    if df_us is not None: print(df_us.tail(2))

    # 测试 3: 港股
    print("\n--- Test HK Stock ---")
    df_hk = p.get_data("STK.0700.HKD", start_date="20240101")
    if df_hk is not None: print(df_hk.tail(2))