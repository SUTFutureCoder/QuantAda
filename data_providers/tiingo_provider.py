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
        self.post_adjust_enabled = True

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

    @staticmethod
    def _as_naive_datetime(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, utc=True).dt.tz_convert(None)

    @staticmethod
    def _safe_first_non_nan(series: pd.Series):
        s = series.dropna()
        return None if s.empty else float(s.iloc[0])

    @staticmethod
    def _safe_last_non_nan(series: pd.Series):
        s = series.dropna()
        return None if s.empty else float(s.iloc[-1])

    @staticmethod
    def _safe_ratio(numerator, denominator, default=1.0):
        try:
            denominator = float(denominator)
            if denominator == 0:
                return default
            ratio = float(numerator) / denominator
            if pd.isna(ratio) or ratio <= 0:
                return default
            return ratio
        except Exception:
            return default

    def _should_stitch_intraday(self, end_date: str = None) -> bool:
        """
        仅在请求区间覆盖“今天”时才尝试拼接盘中数据。
        """
        today = pd.Timestamp.now().normalize()
        if not end_date:
            return True
        try:
            req_end = pd.to_datetime(end_date).normalize()
            return req_end >= today
        except Exception:
            return True

    def _normalize_daily_dataframe(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        规范化 Tiingo 日线数据：
        1) 优先使用 adjOHLCV
        2) 转为后复权口径 (价格乘固定缩放因子)
        """
        if raw_df is None or raw_df.empty:
            return None, None

        raw_df = raw_df.copy()

        # 用于计算“后复权缩放因子”的原始价量快照
        daily_factor_df = None
        factor_cols = ['date', 'close', 'adjClose', 'volume', 'adjVolume']
        existing_factor_cols = [c for c in factor_cols if c in raw_df.columns]
        if existing_factor_cols:
            daily_factor_df = raw_df[existing_factor_cols].copy()
            daily_factor_df['date'] = self._as_naive_datetime(daily_factor_df['date'])
            daily_factor_df.sort_values('date', inplace=True)

        if 'adjClose' in raw_df.columns and all(c in raw_df.columns for c in ['adjOpen', 'adjHigh', 'adjLow', 'adjVolume']):
            clean_df = raw_df[['date', 'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'adjVolume']].copy()
            clean_df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']

            if self.post_adjust_enabled and daily_factor_df is not None:
                valid = daily_factor_df.dropna(subset=['close', 'adjClose'])
                if not valid.empty:
                    # 后复权缩放：以区间首日为基准，将前复权价转为后复权价
                    first_row = valid.iloc[0]
                    post_scale = self._safe_ratio(first_row['close'], first_row['adjClose'], default=1.0)
                    clean_df[['open', 'high', 'low', 'close']] = clean_df[['open', 'high', 'low', 'close']] * post_scale
        else:
            base_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            existing_cols = [c for c in base_cols if c in raw_df.columns]
            clean_df = raw_df[existing_cols].copy()
            clean_df.rename(columns={'date': 'datetime'}, inplace=True)

        clean_df['datetime'] = self._as_naive_datetime(clean_df['datetime'])
        clean_df.set_index('datetime', inplace=True)
        clean_df.sort_index(inplace=True)
        return clean_df[['open', 'high', 'low', 'close', 'volume']], daily_factor_df

    def _build_today_intraday_daily_bar(self, tiingo_symbol: str, daily_factor_df: pd.DataFrame) -> pd.DataFrame:
        """
        将 IEX 1min 数据聚合为“今日临时日线”并转换到与历史相同复权口径后返回。
        """
        today = pd.Timestamp.now().normalize()
        day_str = today.strftime('%Y-%m-%d')

        intraday = self.client.get_ticker_price(
            tiingo_symbol,
            fmt='json',
            startDate=day_str,
            endDate=day_str,
            frequency='1min'
        )
        if not intraday:
            return None

        intraday_df = pd.DataFrame(intraday)
        if intraday_df.empty or 'date' not in intraday_df.columns:
            return None

        intraday_df['date'] = self._as_naive_datetime(intraday_df['date'])
        intraday_df.sort_values('date', inplace=True)
        intraday_df = intraday_df[intraday_df['date'].dt.normalize() == today]
        if intraday_df.empty:
            return None

        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in intraday_df.columns:
                intraday_df[col] = pd.NA

        open_px = self._safe_first_non_nan(intraday_df['open'])
        if open_px is None:
            open_px = self._safe_first_non_nan(intraday_df['close'])
        close_px = self._safe_last_non_nan(intraday_df['close'])
        if close_px is None:
            close_px = self._safe_last_non_nan(intraday_df['open'])

        high_px = intraday_df[['high', 'open', 'close']].apply(pd.to_numeric, errors='coerce').max(axis=1).max()
        low_px = intraday_df[['low', 'open', 'close']].apply(pd.to_numeric, errors='coerce').min(axis=1).min()
        volume = pd.to_numeric(intraday_df['volume'], errors='coerce').fillna(0).sum()

        if any(pd.isna(v) for v in [open_px, high_px, low_px, close_px]):
            return None

        # 原始盘中 -> 与日线一致的复权口径：
        # raw -> adj -> post
        raw_to_adj_price_factor = 1.0
        raw_to_adj_volume_factor = 1.0
        post_scale = 1.0

        if daily_factor_df is not None and not daily_factor_df.empty:
            valid_latest = daily_factor_df.dropna(subset=['close', 'adjClose'])
            if not valid_latest.empty:
                latest = valid_latest.iloc[-1]
                raw_to_adj_price_factor = self._safe_ratio(latest['adjClose'], latest['close'], default=1.0)

                if 'adjVolume' in latest.index and 'volume' in latest.index:
                    raw_to_adj_volume_factor = self._safe_ratio(latest['adjVolume'], latest['volume'], default=1.0)
                else:
                    raw_to_adj_volume_factor = 1.0

            if self.post_adjust_enabled:
                valid_first = daily_factor_df.dropna(subset=['close', 'adjClose'])
                if not valid_first.empty:
                    first = valid_first.iloc[0]
                    post_scale = self._safe_ratio(first['close'], first['adjClose'], default=1.0)

        raw_to_post_price_factor = raw_to_adj_price_factor * post_scale

        row = pd.DataFrame(
            {
                'open': [float(open_px) * raw_to_post_price_factor],
                'high': [float(high_px) * raw_to_post_price_factor],
                'low': [float(low_px) * raw_to_post_price_factor],
                'close': [float(close_px) * raw_to_post_price_factor],
                'volume': [float(volume) * raw_to_adj_volume_factor],
            },
            index=[today]
        )
        return row

    def _stitch_intraday_to_daily(self, tiingo_symbol: str, daily_df: pd.DataFrame, daily_factor_df: pd.DataFrame,
                                  end_date: str = None) -> pd.DataFrame:
        if daily_df is None or daily_df.empty:
            return daily_df
        if not self._should_stitch_intraday(end_date):
            return daily_df

        today = pd.Timestamp.now().normalize()
        if (daily_df.index.normalize() == today).any():
            return daily_df

        try:
            today_bar = self._build_today_intraday_daily_bar(tiingo_symbol, daily_factor_df)
            if today_bar is None or today_bar.empty:
                return daily_df

            merged = pd.concat([daily_df, today_bar], axis=0)
            merged = merged[~merged.index.duplicated(keep='last')]
            merged.sort_index(inplace=True)
            print(f"[Tiingo] Stitched intraday bar for {tiingo_symbol} on {today.strftime('%Y-%m-%d')}.")
            return merged
        except Exception as e:
            print(f"[Tiingo] Intraday stitch skipped for {tiingo_symbol}: {e}")
            return daily_df

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
                print(f"[Tiingo] No daily data returned for {tiingo_symbol}")
                if timeframe == 'Days' and self._should_stitch_intraday(end_date):
                    fallback_today = self._build_today_intraday_daily_bar(tiingo_symbol, daily_factor_df=None)
                    if fallback_today is not None and not fallback_today.empty:
                        print(f"[Tiingo] Fallback to intraday-only synthetic daily bar for {tiingo_symbol}.")
                        return fallback_today[['open', 'high', 'low', 'close', 'volume']]
                return None

            daily_raw_df = pd.DataFrame(data)
            clean_df, daily_factor_df = self._normalize_daily_dataframe(daily_raw_df)
            if clean_df is None or clean_df.empty:
                return None

            # 将“今日盘中”聚合并黏合为临时日线，解决 daily 接口盘中只能到 T-1 的问题
            if timeframe == 'Days':
                clean_df = self._stitch_intraday_to_daily(
                    tiingo_symbol=tiingo_symbol,
                    daily_df=clean_df,
                    daily_factor_df=daily_factor_df,
                    end_date=end_date
                )

            return clean_df[['open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            print(f"[Tiingo] Error fetching {symbol}: {e}")
            return None