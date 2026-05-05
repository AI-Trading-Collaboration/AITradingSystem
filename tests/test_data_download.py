from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.config import load_universe
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.market_data import PriceRequest, RateRequest


@dataclass(frozen=True)
class FakePriceProvider:
    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        rows = [
            {
                "date": request.start.isoformat(),
                "ticker": ticker,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "adj_close": 1.0,
                "volume": 1,
            }
            for ticker in request.tickers
        ]
        return pd.DataFrame(rows)


@dataclass(frozen=True)
class FakeRateProvider:
    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        rows = [
            {"date": request.end.isoformat(), "series": series, "value": 4.5}
            for series in request.series_ids
        ]
        return pd.DataFrame(rows)


def test_download_daily_data_writes_core_universe_cache(tmp_path: Path) -> None:
    summary = download_daily_data(
        load_universe(),
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=FakePriceProvider(),
        rate_provider=FakeRateProvider(),
    )

    assert summary.prices_path == tmp_path / "prices_daily.csv"
    assert summary.rates_path == tmp_path / "rates_daily.csv"
    assert summary.manifest_path == tmp_path / "download_manifest.csv"
    assert summary.price_rows == len(summary.price_tickers)
    assert summary.rate_rows == len(summary.rate_series)
    assert "MSFT" in summary.price_tickers
    assert "NVDA" in summary.price_tickers
    assert "ASML" not in summary.price_tickers
    assert summary.rate_series == ("DGS2", "DGS10")

    manifest = pd.read_csv(summary.manifest_path)
    assert list(manifest["source_id"]) == ["fake_price_provider", "fake_rate_provider"]
    assert set(manifest["row_count"]) == {summary.price_rows, summary.rate_rows}
    assert all(manifest["checksum_sha256"].str.len() == 64)


def test_download_daily_data_writes_secondary_price_cache(tmp_path: Path) -> None:
    summary = download_daily_data(
        load_universe(),
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=FakePriceProvider(),
        secondary_price_provider=FakePriceProvider(),
        rate_provider=FakeRateProvider(),
    )

    assert summary.secondary_prices_path == tmp_path / "prices_marketstack_daily.csv"
    assert summary.secondary_price_rows == summary.price_rows
    assert summary.secondary_prices_path.exists()

    manifest = pd.read_csv(summary.manifest_path)
    assert list(manifest["source_id"]) == [
        "fake_price_provider",
        "fake_rate_provider",
        "fake_price_provider",
    ]
    assert str(summary.secondary_prices_path) in set(manifest["output_path"].astype(str))
