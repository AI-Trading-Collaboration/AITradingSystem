from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_universe
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.market_data import FmpPriceProvider, PriceRequest, RateRequest


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
    assert summary.rate_series == ("DGS2", "DGS10", "DTWEXBGS")

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


def test_download_daily_data_records_fmp_primary_source_without_key(tmp_path: Path) -> None:
    summary = download_daily_data(
        load_universe(),
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=FmpPriceProvider(
            api_key="test-key",
            requests_module=_FakeRequests(
                [
                    {
                        "date": "2026-04-30",
                        "adjOpen": 1.0,
                        "adjHigh": 1.0,
                        "adjLow": 1.0,
                        "adjClose": 1.0,
                        "volume": 1,
                    }
                ]
            ),
        ),
        rate_provider=FakeRateProvider(),
    )

    manifest = pd.read_csv(summary.manifest_path)
    price_manifest = manifest.loc[manifest["source_id"] == "fmp_eod_daily_prices"].iloc[0]
    request_parameters = json.loads(str(price_manifest["request_parameters"]))

    assert price_manifest["provider"] == "Financial Modeling Prep"
    assert "test-key" not in str(price_manifest["request_parameters"])
    assert request_parameters["provider_symbol_aliases"]["GOOG"] == "GOOGL"
    assert request_parameters["provider_symbol_aliases"]["^VIX"] is None


def test_download_data_cli_requires_fmp_key_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    result = CliRunner().invoke(
        app,
        [
            "download-data",
            "--output-dir",
            str(tmp_path),
            "--without-marketstack",
        ],
    )

    assert result.exit_code == 1
    assert "未设置 FMP_API_KEY" in result.output


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

    def get(
        self,
        _url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> _FakeResponse:
        assert params["apikey"] == "test-key"
        assert timeout == 30
        return _FakeResponse(self._payload)
