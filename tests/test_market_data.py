from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.data.market_data import (
    CboeVixPriceProvider,
    CsvDataCache,
    FmpPriceProvider,
    PriceRequest,
    normalize_cboe_vix_prices,
    normalize_fmp_prices,
    normalize_fred_rates,
    normalize_marketstack_prices,
    normalize_yfinance_prices,
)


def test_normalize_yfinance_prices_with_ticker_first_multiindex() -> None:
    index = pd.to_datetime(["2026-04-29", "2026-04-30"])
    columns = pd.MultiIndex.from_product(
        [["MSFT", "NVDA"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
    )
    raw = pd.DataFrame(
        [
            [100, 110, 90, 105, 104, 1000, 800, 820, 780, 810, 809, 2000],
            [106, 112, 101, 108, 107, 1100, 812, 830, 800, 825, 824, 2100],
        ],
        index=index,
        columns=columns,
    )

    prices = normalize_yfinance_prices(raw, ["NVDA", "MSFT"])

    assert list(prices.columns) == [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ]
    assert prices["ticker"].tolist() == ["MSFT", "MSFT", "NVDA", "NVDA"]
    assert prices.loc[0, "date"] == "2026-04-29"
    assert prices.loc[0, "adj_close"] == 104


def test_normalize_yfinance_prices_with_field_first_multiindex() -> None:
    index = pd.to_datetime(["2026-04-30"])
    columns = pd.MultiIndex.from_product(
        [["Open", "High", "Low", "Close", "Adj Close", "Volume"], ["GOOG"]]
    )
    raw = pd.DataFrame([[150, 155, 149, 154, 153, 3000]], index=index, columns=columns)

    prices = normalize_yfinance_prices(raw, ["GOOG"])

    assert prices.loc[0, "ticker"] == "GOOG"
    assert prices.loc[0, "close"] == 154


def test_normalize_fred_rates_to_long_format() -> None:
    raw = pd.DataFrame(
        {"DGS2": [4.5, None], "DGS10": [4.6, 4.7]},
        index=pd.to_datetime(["2026-04-29", "2026-04-30"]),
    )
    raw.index.name = "DATE"

    rates = normalize_fred_rates(raw)

    assert rates.to_dict(orient="records") == [
        {"date": "2026-04-29", "series": "DGS10", "value": 4.6},
        {"date": "2026-04-30", "series": "DGS10", "value": 4.7},
        {"date": "2026-04-29", "series": "DGS2", "value": 4.5},
    ]


def test_normalize_marketstack_prices_maps_provider_symbol_to_internal_ticker() -> None:
    prices = normalize_marketstack_prices(
        [
            {
                "date": "2026-05-01T00:00:00+0000",
                "symbol": "NVDA",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "adj_close": 100.5,
                "volume": 1234,
            },
            {
                "date": "2026-05-01T00:00:00+0000",
                "symbol": "UNREQUESTED",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
            },
        ],
        {"NVDA": "NVDA"},
    )

    assert prices.to_dict(orient="records") == [
        {
            "date": "2026-05-01",
            "ticker": "NVDA",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "adj_close": 100.5,
            "volume": 1234,
        }
    ]


def test_normalize_fmp_prices_requires_adjusted_close_and_maps_alias() -> None:
    prices = normalize_fmp_prices(
        [
            {
                "_provider_symbol": "GOOGL",
                "date": "2026-05-01",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "adjClose": 100.25,
                "volume": 1234,
            }
        ],
        {"GOOGL": "GOOG"},
    )

    assert prices.to_dict(orient="records") == [
        {
            "date": "2026-05-01",
            "ticker": "GOOG",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "adj_close": 100.25,
            "volume": 1234,
        }
    ]


def test_normalize_cboe_vix_prices_filters_range_and_sets_internal_ticker() -> None:
    raw = pd.DataFrame(
        [
            {"DATE": "04/30/2026", "OPEN": "16.5", "HIGH": "17.2", "LOW": "15.9", "CLOSE": "16.8"},
            {"DATE": "05/01/2026", "OPEN": "18.0", "HIGH": "19.0", "LOW": "17.5", "CLOSE": "18.2"},
        ]
    )

    prices = normalize_cboe_vix_prices(
        raw,
        ticker="^VIX",
        start=date(2026, 5, 1),
        end=date(2026, 5, 1),
    )

    assert prices.loc[0, "date"] == "2026-05-01"
    assert prices.loc[0, "ticker"] == "^VIX"
    assert prices.loc[0, "close"] == 18.2
    assert prices.loc[0, "adj_close"] == 18.2
    assert pd.isna(prices.loc[0, "volume"])


def test_fmp_price_provider_skips_unsupported_vix_and_uses_provider_alias() -> None:
    fake_requests = _FakeRequests(
        [
            {
                "date": "2026-05-01",
                "adjOpen": 100.0,
                "adjHigh": 102.0,
                "adjLow": 99.0,
                "adjClose": 101.0,
                "volume": 1234,
            }
        ]
    )
    provider = FmpPriceProvider(api_key="test-key", requests_module=fake_requests)

    prices = provider.download_prices(
        PriceRequest(tickers=["GOOG", "^VIX"], start=date(2026, 5, 1), end=date(2026, 5, 1))
    )

    assert prices["ticker"].tolist() == ["GOOG"]
    assert prices.loc[0, "close"] == 101.0
    assert prices.loc[0, "adj_close"] == 101.0
    expected_params = {
        "symbol": "GOOGL",
        "from": "2026-05-01",
        "to": "2026-05-01",
        "apikey": "test-key",
    }
    assert fake_requests.calls == [expected_params, expected_params]


def test_cboe_vix_price_provider_reads_official_csv() -> None:
    fake_requests = _FakeTextRequests(
        "DATE,OPEN,HIGH,LOW,CLOSE\n"
        "04/30/2026,16.5,17.2,15.9,16.8\n"
        "05/01/2026,18.0,19.0,17.5,18.2\n"
    )
    provider = CboeVixPriceProvider(requests_module=fake_requests)

    prices = provider.download_prices(
        PriceRequest(tickers=["^VIX"], start=date(2026, 5, 1), end=date(2026, 5, 1))
    )

    assert prices["ticker"].tolist() == ["^VIX"]
    assert prices.loc[0, "close"] == 18.2
    assert fake_requests.calls == [provider.base_url]


def test_csv_data_cache_writes_prices_and_rates(tmp_path: Path) -> None:
    cache = CsvDataCache(tmp_path)
    prices = pd.DataFrame(
        [{"date": date(2026, 4, 30).isoformat(), "ticker": "MSFT", "close": 100.0}]
    )
    rates = pd.DataFrame([{"date": "2026-04-30", "series": "DGS10", "value": 4.7}])

    prices_path = cache.write_prices(prices)
    rates_path = cache.write_rates(rates)

    assert prices_path.read_text(encoding="utf-8").startswith("date,ticker,close")
    assert rates_path.read_text(encoding="utf-8").startswith("date,series,value")


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.ok = True
        self.status_code = 200

    def json(self) -> object:
        return self._payload


class _FakeRequests:
    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        _url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> _FakeResponse:
        assert timeout == 30
        self.calls.append(params)
        return _FakeResponse(self._payload)


class _FakeTextResponse:
    def __init__(self, text: str, *, ok: bool = True, status_code: int = 200) -> None:
        self.text = text
        self.ok = ok
        self.status_code = status_code


class _FakeTextRequests:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[str] = []

    def get(
        self,
        url: str,
        *,
        timeout: int,
    ) -> _FakeTextResponse:
        assert timeout == 30
        self.calls.append(url)
        return _FakeTextResponse(self.text)
