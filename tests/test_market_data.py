from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.data.market_data import (
    CsvDataCache,
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
