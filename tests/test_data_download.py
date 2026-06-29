from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import configured_price_tickers, load_universe
from ai_trading_system.data.download import (
    IncrementalPriceWindow,
    ProviderQuotaBudgetError,
    _estimate_marketstack_increment_usage,
    _price_fetch_windows,
    _provider_request_budget_status,
    download_daily_data,
    write_download_failure_report,
)
from ai_trading_system.data.market_data import (
    CboeVixPriceProvider,
    FmpPriceProvider,
    MarketstackPriceProvider,
    PriceRequest,
    ProviderDownloadError,
    ProviderRequestDiagnostic,
    RateRequest,
)
from ai_trading_system.external_request_cache import write_external_request_cache_response


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
class FakeNoVixPriceProvider:
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
            if ticker != "^VIX"
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


class RecordingRangePriceProvider:
    def __init__(self) -> None:
        self.calls: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.calls.append(request)
        rows: list[dict[str, object]] = []
        for current in pd.date_range(request.start, request.end, freq="D"):
            for ticker in request.tickers:
                rows.append(
                    {
                        "date": current.strftime("%Y-%m-%d"),
                        "ticker": ticker,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "adj_close": 1.0,
                        "volume": 1,
                    }
                )
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
    assert "ASML" in summary.price_tickers
    assert "AMZN" in summary.price_tickers
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


def test_download_daily_data_incrementally_requests_only_missing_tail(
    tmp_path: Path,
) -> None:
    primary_provider = RecordingRangePriceProvider()
    secondary_provider = RecordingRangePriceProvider()
    universe = load_universe()

    first = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=primary_provider,
        secondary_price_provider=secondary_provider,
        rate_provider=FakeRateProvider(),
    )
    second = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 5, 1),
        output_dir=tmp_path,
        price_provider=primary_provider,
        secondary_price_provider=secondary_provider,
        rate_provider=FakeRateProvider(),
    )

    assert primary_provider.calls[0].start == date(2026, 4, 29)
    assert primary_provider.calls[0].end == date(2026, 4, 30)
    assert primary_provider.calls[1].start == date(2026, 5, 1)
    assert primary_provider.calls[1].end == date(2026, 5, 1)
    assert secondary_provider.calls[1].start == date(2026, 5, 1)
    assert secondary_provider.calls[1].end == date(2026, 5, 1)

    assert second.price_rows == first.price_rows + len(second.price_tickers)
    assert second.secondary_price_rows == first.secondary_price_rows + len(second.price_tickers)

    manifest = pd.read_csv(second.manifest_path)
    latest_primary = manifest.loc[
        manifest["output_path"].astype(str) == str(second.prices_path)
    ].iloc[-1]
    request_parameters = json.loads(str(latest_primary["request_parameters"]))
    incremental = request_parameters["incremental_refresh"]

    assert incremental["reused_row_count"] == first.price_rows
    assert incremental["fetched_row_count"] == len(second.price_tickers)
    assert incremental["fetch_window_count"] == 1
    assert incremental["fetch_windows"][0]["start"] == "2026-05-01"


def test_marketstack_incremental_windows_ignore_head_gap_for_existing_cache() -> None:
    provider = MarketstackPriceProvider(api_key="test-key")
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )
    rows = []
    for ticker in supported_tickers:
        first_date = "2021-02-22" if ticker in {"PLTR", "SGOV"} else "2018-01-02"
        rows.append({"date": first_date, "ticker": ticker})
        rows.append({"date": "2026-06-25", "ticker": ticker})
    existing = pd.DataFrame(rows)

    windows = _price_fetch_windows(
        existing,
        tickers=supported_tickers,
        start=date(2018, 1, 1),
        end=date(2026, 6, 26),
    )

    assert [(window.start, window.end, window.tickers) for window in windows] == [
        (date(2026, 6, 26), date(2026, 6, 26), tuple(sorted(supported_tickers)))
    ]
    assert _estimate_marketstack_increment_usage(provider, windows) == len(
        supported_tickers
    )


def test_download_daily_data_blocks_marketstack_when_quota_preflight_fails(
    tmp_path: Path,
) -> None:
    request_cache_dir = tmp_path / "request_cache"
    write_external_request_cache_response(
        provider="Marketstack",
        api_family="eod_daily_prices",
        method="GET",
        url="https://api.marketstack.com/v2/eod",
        params={"symbols": "NVDA"},
        status_code=200,
        response_headers={
            "x-quota-limit": "10000",
            "x-quota-remaining": "0",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    fake_marketstack_requests = _NeverRequests()

    with pytest.raises(ProviderQuotaBudgetError, match="Marketstack quota preflight blocked"):
        download_daily_data(
            load_universe(),
            start=date(2026, 4, 29),
            end=date(2026, 5, 1),
            output_dir=tmp_path,
            price_provider=FakePriceProvider(),
            secondary_price_provider=MarketstackPriceProvider(
                api_key="test-key",
                requests_module=fake_marketstack_requests,
                request_cache_dir=request_cache_dir,
                page_limit=1,
            ),
            rate_provider=FakeRateProvider(),
        )

    assert fake_marketstack_requests.calls == []


def test_marketstack_owner_approved_small_daily_overage_allows_tail_preflight(
    tmp_path: Path,
) -> None:
    request_cache_dir = tmp_path / "request_cache"
    write_external_request_cache_response(
        provider="Marketstack",
        api_family="eod_daily_prices",
        method="GET",
        url="https://api.marketstack.com/v2/eod",
        params={"symbols": "NVDA"},
        status_code=200,
        response_headers={
            "x-quota-limit": "10000",
            "x-quota-remaining": "-688",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    provider = MarketstackPriceProvider(
        api_key="test-key",
        request_cache_dir=request_cache_dir,
    )
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )

    status = _provider_request_budget_status(
        provider,
        (
            IncrementalPriceWindow(
                tickers=supported_tickers,
                start=date(2026, 6, 26),
                end=date(2026, 6, 26),
            ),
        ),
    )

    assert status is not None
    assert status["status"] == "OWNER_APPROVED_SMALL_DAILY_OVERAGE"
    assert status["estimated_increment_usage"] == len(supported_tickers)
    owner_approved_overage = status["owner_approved_overage"]
    assert isinstance(owner_approved_overage, dict)
    assert owner_approved_overage["approved"] is True
    assert owner_approved_overage["policy_version"] == "data_source_request_budget_policy_v1"
    assert owner_approved_overage["max_estimated_increment_usage"] == 50
    assert owner_approved_overage["max_quota_overage_ratio"] == 0.10
    assert owner_approved_overage["quota_shortfall"] == 713
    assert owner_approved_overage["quota_overage_ratio"] == pytest.approx(0.0713)
    assert owner_approved_overage["window_calendar_days"] == [1]


def test_marketstack_owner_approved_overage_blocks_above_ratio_cap(
    tmp_path: Path,
) -> None:
    request_cache_dir = tmp_path / "request_cache"
    write_external_request_cache_response(
        provider="Marketstack",
        api_family="eod_daily_prices",
        method="GET",
        url="https://api.marketstack.com/v2/eod",
        params={"symbols": "NVDA"},
        status_code=200,
        response_headers={
            "x-quota-limit": "10000",
            "x-quota-remaining": "-1000",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    provider = MarketstackPriceProvider(
        api_key="test-key",
        request_cache_dir=request_cache_dir,
    )
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )

    with pytest.raises(ProviderQuotaBudgetError, match="Marketstack quota preflight blocked"):
        _provider_request_budget_status(
            provider,
            (
                IncrementalPriceWindow(
                    tickers=supported_tickers,
                    start=date(2026, 6, 26),
                    end=date(2026, 6, 26),
                ),
            ),
        )


def test_marketstack_negative_quota_does_not_block_no_live_request(
    tmp_path: Path,
) -> None:
    request_cache_dir = tmp_path / "request_cache"
    write_external_request_cache_response(
        provider="Marketstack",
        api_family="eod_daily_prices",
        method="GET",
        url="https://api.marketstack.com/v2/eod",
        params={"symbols": "NVDA"},
        status_code=200,
        response_headers={
            "x-quota-limit": "10000",
            "x-quota-remaining": "-713",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    provider = MarketstackPriceProvider(
        api_key="test-key",
        request_cache_dir=request_cache_dir,
    )

    status = _provider_request_budget_status(provider, ())

    assert status is not None
    assert status["status"] == "NO_LIVE_REQUEST_NEEDED"
    assert status["estimated_increment_usage"] == 0
    assert status["quota_remaining"] == -713
    assert "owner_approved_overage" not in status


def test_download_daily_data_adds_cboe_vix_when_primary_skips_it(tmp_path: Path) -> None:
    summary = download_daily_data(
        load_universe(),
        start=date(2026, 4, 30),
        end=date(2026, 5, 1),
        output_dir=tmp_path,
        price_provider=FakeNoVixPriceProvider(),
        vix_price_provider=CboeVixPriceProvider(requests_module=_FakeCboeRequests()),
        rate_provider=FakeRateProvider(),
    )

    prices = pd.read_csv(summary.prices_path)
    assert "^VIX" in set(prices["ticker"])
    assert summary.price_rows == len(prices)

    manifest = pd.read_csv(summary.manifest_path)
    assert "cboe_vix_daily_prices" in set(manifest["source_id"])
    vix_manifest = manifest.loc[manifest["source_id"] == "cboe_vix_daily_prices"].iloc[0]
    request_parameters = json.loads(str(vix_manifest["request_parameters"]))
    assert vix_manifest["provider"] == "Cboe Global Markets"
    assert vix_manifest["row_count"] == 2
    assert request_parameters["tickers"] == ["^VIX"]


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
        vix_price_provider=CboeVixPriceProvider(requests_module=_FakeCboeRequests()),
        rate_provider=FakeRateProvider(),
    )

    manifest = pd.read_csv(summary.manifest_path)
    price_manifest = manifest.loc[manifest["source_id"] == "fmp_eod_daily_prices"].iloc[0]
    request_parameters = json.loads(str(price_manifest["request_parameters"]))

    assert price_manifest["provider"] == "Financial Modeling Prep"
    assert "test-key" not in str(price_manifest["request_parameters"])
    assert "GOOG" not in request_parameters["provider_symbol_aliases"]
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


def test_write_download_failure_report_redacts_marketstack_diagnostics(tmp_path: Path) -> None:
    diagnostic = ProviderRequestDiagnostic(
        provider="Marketstack",
        api_family="eod_daily_prices",
        endpoint="https://api.marketstack.com/v2/eod",
        stage="http_request",
        method="GET",
        request_parameters={
            "access_key": "***",
            "symbols": "NVDA",
            "date_from": "2026-05-01",
            "date_to": "2026-05-01",
            "limit": "1000",
            "offset": "0",
        },
        cache_status="MISS_NO_RESPONSE",
        cache_key="abc123",
        cache_metadata_path=tmp_path / "cache" / "metadata.json",
        exception_type="TimeoutError",
        exception_message="timeout access_key=***",
    )
    error = ProviderDownloadError(
        "Marketstack request failed before receiving a cacheable response",
        diagnostic,
    )

    report_path = write_download_failure_report(
        output_path=tmp_path / "download_data_diagnostics_2026-05-11.md",
        start=date(2018, 1, 1),
        end=date(2026, 5, 11),
        raw_output_dir=tmp_path / "raw",
        include_full_ai_chain=False,
        price_provider_name="fmp",
        with_marketstack=True,
        error=error,
    )

    text = report_path.read_text(encoding="utf-8")
    assert "- 状态：FAIL" in text
    assert "Marketstack" in text
    assert "MISS_NO_RESPONSE" in text
    assert "NVDA" in text
    assert 'access_key": "***"' in text
    assert "secret" not in text.lower()


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


class _FakeCboeResponse:
    text = (
        "DATE,OPEN,HIGH,LOW,CLOSE\n"
        "04/30/2026,16.5,17.2,15.9,16.8\n"
        "05/01/2026,18.0,19.0,17.5,18.2\n"
    )
    ok = True
    status_code = 200


class _FakeCboeRequests:
    def get(
        self,
        _url: str,
        *,
        timeout: int,
    ) -> _FakeCboeResponse:
        assert timeout == 30
        return _FakeCboeResponse()


class _NeverRequests:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        raise AssertionError("Marketstack should not be called after quota preflight failure")
