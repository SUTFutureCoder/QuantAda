import pandas as pd

from data_providers.base_provider import BaseDataProvider
from data_providers.manager import DataManager


def test_data_manager_parses_comma_separated_sources(monkeypatch):
    calls = []

    class TiingoDataProvider(BaseDataProvider):
        PRIORITY = 1

        def get_data(self, symbol, start_date=None, end_date=None,
                     timeframe="Days", compression=1):
            calls.append("tiingo")
            return None

    class AkshareDataProvider(BaseDataProvider):
        PRIORITY = 2

        def get_data(self, symbol, start_date=None, end_date=None,
                     timeframe="Days", compression=1):
            calls.append("akshare")
            idx = pd.date_range("2026-01-10", periods=3, freq="D")
            return pd.DataFrame(
                {
                    "open": [10.0, 10.1, 10.2],
                    "high": [10.2, 10.3, 10.4],
                    "low": [9.8, 9.9, 10.0],
                    "close": [10.0, 10.1, 10.2],
                    "volume": [10000, 10000, 10000],
                },
                index=idx,
            )

    monkeypatch.setattr(
        DataManager,
        "auto_discover_and_sort_providers",
        lambda self, provider_dir=None: [TiingoDataProvider(), AkshareDataProvider()],
    )

    dm = DataManager()
    df = dm.get_data(
        "AAPL",
        start_date="2026-01-01",
        end_date="2026-02-01",
        specified_sources="tiingo, akshare",
    )

    assert calls == ["tiingo", "akshare"], "Comma-separated data_source should be tried in order."
    assert df is not None and not df.empty, "Should return data from the available provider."
