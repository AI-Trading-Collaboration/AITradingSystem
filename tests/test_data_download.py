from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import configured_price_tickers, load_universe
from ai_trading_system.data import download as download_module
from ai_trading_system.data import download_publication as publication_module
from ai_trading_system.data.download import (
    IncrementalPriceWindow,
    ProviderQuotaBudgetError,
    _estimate_marketstack_increment_usage,
    _marketstack_billing_cycle_bounds,
    _price_fetch_windows,
    _provider_request_budget_status,
    download_daily_data,
    write_download_failure_report,
)
from ai_trading_system.data.download_publication import (
    DownloadPublicationIntegrityError,
    resolve_download_publication,
    resolve_download_publication_if_present,
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
from ai_trading_system.platform.artifacts import canonical_json_bytes, sha256_bytes


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


class OldRecordingRangePriceProvider(RecordingRangePriceProvider):
    pass


class NewRecordingRangePriceProvider(RecordingRangePriceProvider):
    pass


class OverlappingVixPriceProvider:
    def __init__(self) -> None:
        self.calls: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.calls.append(request)
        rows = []
        for current in (date(2026, 4, 29), request.start):
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": "^VIX",
                    "open": 42.0,
                    "high": 42.0,
                    "low": 42.0,
                    "close": 42.0,
                    "adj_close": 42.0,
                    "volume": 0,
                }
            )
        return pd.DataFrame(rows)


class EmptyPriceProvider:
    def __init__(self) -> None:
        self.calls: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.calls.append(request)
        return pd.DataFrame(columns=list(_PRICE_TEST_COLUMNS))


@dataclass(frozen=True)
class EmptyRateProvider:
    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        return pd.DataFrame(columns=["date", "series", "value"])


_PRICE_TEST_COLUMNS = (
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
)


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
    assert summary.publication_transaction_id is not None
    assert summary.publication_manifest_path is not None
    assert summary.publication_discovery_path is not None
    assert summary.publication_atomicity_scope == "IMMUTABLE_GENERATION_DISCOVERY_POINTER_ONLY"
    assert summary.legacy_projection_atomicity == "NOT_GUARANTEED"
    assert summary.consumer_cutover_allowed is False

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
    first.prices_path.write_text("stale legacy prices", encoding="utf-8")
    assert first.secondary_prices_path is not None
    first.secondary_prices_path.write_text("stale legacy secondary", encoding="utf-8")
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

    assert second.publication_manifest_path is not None
    latest_primary = _publication_source_event(
        second.publication_manifest_path,
        artifact_role="prices",
        source_id="recording_range_price_provider",
    )
    request_parameters = latest_primary["request_parameters"]
    assert isinstance(request_parameters, dict)
    incremental = request_parameters["incremental_refresh"]

    assert incremental["reused_row_count"] == first.price_rows
    assert incremental["fetched_row_count"] == len(second.price_tickers)
    assert incremental["fetch_window_count"] == 1
    assert incremental["fetch_windows"][0]["start"] == "2026-05-01"


def test_download_retires_validated_canonical_secondary_when_provider_is_omitted(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    primary_provider = RecordingRangePriceProvider()
    secondary_provider = RecordingRangePriceProvider()
    first = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=primary_provider,
        secondary_price_provider=secondary_provider,
        rate_provider=FakeRateProvider(),
    )
    assert first.secondary_prices_path is not None
    retired_projection = first.secondary_prices_path
    assert retired_projection.is_file()

    second = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 5, 1),
        output_dir=tmp_path,
        price_provider=primary_provider,
        secondary_price_provider=None,
        rate_provider=FakeRateProvider(),
    )

    assert second.secondary_prices_path is None
    assert second.secondary_price_rows == 0
    assert not retired_projection.exists()
    resolved = resolve_download_publication(output_dir=tmp_path)
    assert resolved.transaction_id == second.publication_transaction_id
    assert resolved.secondary_prices_path is None
    assert "secondary_prices" not in resolved.artifact_sha256
    transaction = _download_transaction(second)
    assert {
        str(artifact["role"]) for artifact in transaction["artifacts"] if isinstance(artifact, dict)
    } == {"prices", "rates"}


def test_download_relative_output_root_bootstraps_then_reuses_portable_predecessor_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe = load_universe()
    monkeypatch.chdir(tmp_path)
    relative_output = Path("relative-cache")
    absolute_output = (tmp_path / relative_output).resolve(strict=False)
    provider = RecordingRangePriceProvider()

    first = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=relative_output,
        price_provider=provider,
        rate_provider=FakeRateProvider(),
    )
    second = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 5, 1),
        output_dir=relative_output,
        price_provider=provider,
        rate_provider=FakeRateProvider(),
    )

    assert first.prices_path.is_absolute()
    assert second.prices_path.is_absolute()
    assert first.prices_path.parent == absolute_output
    assert second.prices_path.parent == absolute_output
    assert [(call.start, call.end) for call in provider.calls] == [
        (date(2026, 4, 29), date(2026, 4, 30)),
        (date(2026, 5, 1), date(2026, 5, 1)),
    ]
    transaction = _download_transaction(second)
    predecessor = _transaction_source(
        transaction,
        artifact_role="prices",
        source_kind="CANONICAL_PREDECESSOR_REUSE",
    )
    parameters = predecessor["request_parameters"]
    assert isinstance(parameters, dict)
    for field in ("predecessor_transaction_path", "predecessor_artifact_path"):
        value = str(parameters[field])
        assert not Path(value).is_absolute()
        assert "\\" not in value
        assert value.startswith(".download_publications/")
    resolved = resolve_download_publication(output_dir=relative_output)
    assert resolved.transaction_id == second.publication_transaction_id
    assert resolved.legacy_projection_verified is True


def test_download_legacy_bootstrap_binds_exact_cache_bytes_and_secondary_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe = load_universe()
    tickers = tuple(configured_price_tickers(universe))
    primary_cache = tmp_path / "prices_daily.csv"
    secondary_cache = tmp_path / "prices_marketstack_daily.csv"
    historical_dates = (date(2026, 4, 28), date(2026, 4, 29))
    _write_price_cache_dates(primary_cache, tickers=tickers, price_dates=historical_dates)
    _write_price_cache_dates(secondary_cache, tickers=tickers, price_dates=historical_dates)
    original_cache_bytes = {
        primary_cache: primary_cache.read_bytes(),
        secondary_cache: secondary_cache.read_bytes(),
    }
    manifest_path = tmp_path / "download_manifest.csv"
    pd.DataFrame(
        [
            {
                "downloaded_at": "2026-04-29T00:00:00+00:00",
                "source_id": "legacy_test_source",
                "provider": "Legacy test fixture",
                "endpoint": cache_path.name,
                "request_parameters": "{}",
                "output_path": str(cache_path),
                "checksum_sha256": sha256_bytes(raw),
                "row_count": len(tickers) * len(historical_dates),
            }
            for cache_path, raw in original_cache_bytes.items()
        ]
    ).to_csv(manifest_path, index=False)
    original_manifest = manifest_path.read_bytes()

    observed_cache_reads: dict[Path, list[bytes]] = {
        primary_cache: [],
        secondary_cache: [],
    }
    original_contained_read = download_module.read_contained_artifact_bytes

    def tracking_contained_read(*, root: Path, relative_path: str) -> bytes:
        raw = original_contained_read(root=root, relative_path=relative_path)
        path = root / relative_path
        if path in observed_cache_reads:
            observed_cache_reads[path].append(raw)
        return raw

    monkeypatch.setattr(
        download_module,
        "read_contained_artifact_bytes",
        tracking_contained_read,
    )
    primary_provider = RecordingRangePriceProvider()
    secondary_provider = NewRecordingRangePriceProvider()

    summary = download_daily_data(
        universe,
        start=date(2026, 4, 28),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=primary_provider,
        secondary_price_provider=secondary_provider,
        rate_provider=FakeRateProvider(),
    )

    assert [(call.start, call.end) for call in primary_provider.calls] == [
        (date(2026, 4, 30), date(2026, 4, 30))
    ]
    assert [(call.start, call.end) for call in secondary_provider.calls] == [
        (date(2026, 4, 30), date(2026, 4, 30))
    ]
    for cache_path, original in original_cache_bytes.items():
        assert observed_cache_reads[cache_path].count(original) == 1

    transaction = _download_transaction(summary)
    for artifact_role, artifact_path, original in (
        ("prices", summary.prices_path, original_cache_bytes[primary_cache]),
        (
            "secondary_prices",
            summary.secondary_prices_path,
            original_cache_bytes[secondary_cache],
        ),
    ):
        assert artifact_path is not None
        allocations = _assert_exact_source_partition(
            transaction,
            artifact_role=artifact_role,
            artifact_path=artifact_path,
        )
        legacy = _transaction_source(
            transaction,
            artifact_role=artifact_role,
            source_kind="LEGACY_LOCAL_CACHE_IMPORT",
        )
        live = _transaction_source(
            transaction,
            artifact_role=artifact_role,
            source_kind="LIVE_PROVIDER",
        )
        assert legacy["allocation_mode"] == "REMAINDER"
        assert allocations[str(legacy["source_event_id"])] == {
            (ticker, current.isoformat()) for current in historical_dates for ticker in tickers
        }
        assert allocations[str(live["source_event_id"])] == {
            (ticker, "2026-04-30") for ticker in tickers
        }

        parameters = legacy["request_parameters"]
        assert isinstance(parameters, dict)
        assert parameters["cache_relative_path"] == artifact_path.name
        assert parameters["cache_sha256"] == sha256_bytes(original)
        assert parameters["cache_size_bytes"] == len(original)
        assert parameters["cache_row_count"] == len(tickers) * len(historical_dates)
        assert parameters["cache_capture_mode"] == "READ_ONCE_BYTES_THEN_PARSE"
        assert parameters["manifest_relative_path"] == "download_manifest.csv"
        assert parameters["manifest_sha256"] == sha256_bytes(original_manifest)
        assert parameters["manifest_size_bytes"] == len(original_manifest)
        assert parameters["manifest_binding_status"] == "MATCHED"
        assert parameters["manifest_provider_provenance_accepted"] is False
        assert parameters["raw_provider_provenance"] is False
        assert parameters["origin_lineage_complete"] is False
        assert parameters["origin_status"] == "OPAQUE_LEGACY"
        assert parameters["data_quality_provenance"] is False

        replay_inputs = legacy["replay_inputs"]
        assert isinstance(replay_inputs, list)
        assert len(replay_inputs) == 1
        replay_input = replay_inputs[0]
        assert replay_input["sha256"] == sha256_bytes(original)
        assert replay_input["size_bytes"] == len(original)
        assert (tmp_path / str(replay_input["path"])).read_bytes() == original

    rate_allocations = _assert_exact_source_partition(
        transaction,
        artifact_role="rates",
        artifact_path=summary.rates_path,
    )
    rate_source = _transaction_source(
        transaction,
        artifact_role="rates",
        source_kind="LIVE_PROVIDER",
    )
    assert rate_source["allocation_mode"] == "REMAINDER"
    assert len(rate_allocations[str(rate_source["source_event_id"])]) == summary.rate_rows
    assert "__download_source_event_id" not in pd.read_csv(summary.rates_path).columns
    assert "__download_source_event_id" not in pd.read_csv(summary.prices_path).columns
    assert summary.secondary_prices_path is not None
    assert "__download_source_event_id" not in pd.read_csv(summary.secondary_prices_path).columns


def test_download_canonical_provider_switch_binds_predecessor_and_new_tail(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    tickers = tuple(configured_price_tickers(universe))
    old_provider = OldRecordingRangePriceProvider()
    first = download_daily_data(
        universe,
        start=date(2026, 4, 28),
        end=date(2026, 4, 29),
        output_dir=tmp_path,
        price_provider=old_provider,
        rate_provider=FakeRateProvider(),
    )
    assert first.publication_manifest_path is not None
    assert first.publication_discovery_path is not None
    first_transaction_raw = first.publication_manifest_path.read_bytes()
    first_pointer_raw = first.publication_discovery_path.read_bytes()
    first_transaction = json.loads(first_transaction_raw)
    first_prices_artifact = _transaction_artifact(first_transaction, artifact_role="prices")

    new_provider = NewRecordingRangePriceProvider()
    second = download_daily_data(
        universe,
        start=date(2026, 4, 28),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=new_provider,
        rate_provider=FakeRateProvider(),
    )

    assert [(call.start, call.end) for call in new_provider.calls] == [
        (date(2026, 4, 30), date(2026, 4, 30))
    ]
    transaction = _download_transaction(second)
    allocations = _assert_exact_source_partition(
        transaction,
        artifact_role="prices",
        artifact_path=second.prices_path,
    )
    predecessor = _transaction_source(
        transaction,
        artifact_role="prices",
        source_kind="CANONICAL_PREDECESSOR_REUSE",
    )
    current = _transaction_source(
        transaction,
        artifact_role="prices",
        source_kind="LIVE_PROVIDER",
        source_id="new_recording_range_price_provider",
    )
    assert predecessor["allocation_mode"] == "REMAINDER"
    assert allocations[str(predecessor["source_event_id"])] == {
        (ticker, current_date.isoformat())
        for current_date in (date(2026, 4, 28), date(2026, 4, 29))
        for ticker in tickers
    }
    assert allocations[str(current["source_event_id"])] == {
        (ticker, "2026-04-30") for ticker in tickers
    }

    parameters = predecessor["request_parameters"]
    assert isinstance(parameters, dict)
    assert parameters["predecessor_transaction_id"] == first.publication_transaction_id
    assert parameters["predecessor_transaction_path"] == (
        first.publication_manifest_path.relative_to(tmp_path).as_posix()
    )
    assert parameters["predecessor_transaction_sha256"] == sha256_bytes(first_transaction_raw)
    assert parameters["predecessor_discovery_pointer_sha256"] == sha256_bytes(first_pointer_raw)
    assert parameters["predecessor_artifact_role"] == "prices"
    assert parameters["predecessor_artifact_sha256"] == first_prices_artifact["sha256"]
    assert parameters["predecessor_artifact_row_count"] == first_prices_artifact["row_count"]
    assert parameters["predecessor_artifact_path"] == first_prices_artifact["immutable_path"]
    assert predecessor["endpoint"] == first_prices_artifact["immutable_path"]
    assert parameters["lineage_scope"] == "IMMEDIATE_PREDECESSOR_ONLY"
    assert parameters["raw_provider_provenance"] is False
    assert parameters["origin_lineage_complete"] is False
    assert parameters["origin_status"] == "CANONICAL_IMMEDIATE_PREDECESSOR"
    assert parameters["data_quality_provenance"] is False


def test_download_vix_overlap_assigns_winning_key_only_to_fallback(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    tickers = tuple(configured_price_tickers(universe))
    _write_price_cache_dates(
        tmp_path / "prices_daily.csv",
        tickers=tickers,
        price_dates=(date(2026, 4, 29),),
    )
    vix_provider = OverlappingVixPriceProvider()

    summary = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=FakeNoVixPriceProvider(),
        vix_price_provider=vix_provider,
        rate_provider=FakeRateProvider(),
    )

    assert [(call.start, call.end) for call in vix_provider.calls] == [
        (date(2026, 4, 30), date(2026, 4, 30))
    ]
    transaction = _download_transaction(summary)
    allocations = _assert_exact_source_partition(
        transaction,
        artifact_role="prices",
        artifact_path=summary.prices_path,
    )
    fallback = _transaction_source(
        transaction,
        artifact_role="prices",
        source_kind="LIVE_PROVIDER",
        source_id="overlapping_vix_price_provider",
    )
    primary = _transaction_source(
        transaction,
        artifact_role="prices",
        source_kind="LIVE_PROVIDER",
        source_id="fake_no_vix_price_provider",
    )
    fallback_keys = allocations[str(fallback["source_event_id"])]
    primary_keys = allocations[str(primary["source_event_id"])]
    assert fallback["allocation_mode"] == "EXPLICIT_KEYS"
    assert fallback_keys == {
        ("^VIX", "2026-04-29"),
        ("^VIX", "2026-04-30"),
    }
    assert not fallback_keys.intersection(primary_keys)
    legacy = _transaction_source(
        transaction,
        artifact_role="prices",
        source_kind="LEGACY_LOCAL_CACHE_IMPORT",
    )
    assert ("^VIX", "2026-04-29") not in allocations[str(legacy["source_event_id"])]
    parameters = legacy["request_parameters"]
    assert isinstance(parameters, dict)
    assert parameters["manifest_binding_status"] == "MISSING"
    assert parameters["manifest_sha256"] is None
    assert parameters["manifest_size_bytes"] is None
    assert parameters["manifest_row_count"] is None


def test_download_empty_attempts_are_zero_winner_and_empty_rates_have_remainder(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    tickers = tuple(configured_price_tickers(universe))
    _write_price_cache_dates(
        tmp_path / "prices_daily.csv",
        tickers=tickers,
        price_dates=(date(2026, 4, 29),),
    )
    empty_provider = EmptyPriceProvider()

    summary = download_daily_data(
        universe,
        start=date(2026, 4, 29),
        end=date(2026, 4, 30),
        output_dir=tmp_path,
        price_provider=empty_provider,
        vix_price_provider=empty_provider,
        rate_provider=EmptyRateProvider(),
    )

    assert len(empty_provider.calls) == 2
    transaction = _download_transaction(summary)
    price_allocations = _assert_exact_source_partition(
        transaction,
        artifact_role="prices",
        artifact_path=summary.prices_path,
    )
    attempted = [
        source
        for source in transaction["source_event_records"]
        if source["artifact_role"] == "prices"
        and source["source_kind"] == "LIVE_PROVIDER"
        and source["source_id"] == "empty_price_provider"
    ]
    assert len(attempted) == 2
    assert len({source["source_event_id"] for source in attempted}) == 2
    for source in attempted:
        assert source["allocation_mode"] == "EXPLICIT_KEYS"
        assert source["explicit_row_keys"] == []
        assert source["winning_row_count"] == 0
        assert price_allocations[str(source["source_event_id"])] == set()

    rate_allocations = _assert_exact_source_partition(
        transaction,
        artifact_role="rates",
        artifact_path=summary.rates_path,
    )
    rate_source = _transaction_source(
        transaction,
        artifact_role="rates",
        source_kind="LIVE_PROVIDER",
    )
    assert rate_source["allocation_mode"] == "REMAINDER"
    assert rate_source["explicit_row_keys"] == []
    assert rate_source["winning_row_count"] == 0
    assert rate_allocations == {str(rate_source["source_event_id"]): set()}
    assert list(pd.read_csv(summary.rates_path).columns) == ["date", "series", "value"]
    assert "__download_source_event_id" not in pd.read_csv(summary.prices_path).columns


def test_download_incremental_fails_closed_when_canonical_pointer_is_invalid(
    tmp_path: Path,
) -> None:
    pointer = tmp_path / ".download_publications/current/download_composite.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"{}")
    provider = RecordingRangePriceProvider()

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_DISCOVERY_INVALID",
    ):
        download_daily_data(
            load_universe(),
            start=date(2026, 4, 29),
            end=date(2026, 4, 30),
            output_dir=tmp_path,
            price_provider=provider,
            rate_provider=FakeRateProvider(),
        )

    assert provider.calls == []
    assert not (tmp_path / "prices_daily.csv").exists()


@pytest.mark.parametrize(
    "legacy_name",
    [
        "prices_daily.csv",
        "prices_marketstack_daily.csv",
        "download_manifest.csv",
    ],
)
def test_legacy_bootstrap_link_is_rejected_before_provider_or_publication(
    tmp_path: Path,
    legacy_name: str,
) -> None:
    external = tmp_path.parent / f"{tmp_path.name}-{legacy_name}.external"
    external_raw = b"external bytes must remain unchanged"
    external.write_bytes(external_raw)
    _symlink_or_skip(tmp_path / legacy_name, external)
    provider = RecordingRangePriceProvider()

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        download_daily_data(
            load_universe(),
            start=date(2026, 4, 29),
            end=date(2026, 4, 30),
            output_dir=tmp_path,
            price_provider=provider,
            rate_provider=FakeRateProvider(),
        )

    assert exc_info.value.code == "DOWNLOAD_LEGACY_BOOTSTRAP_INPUT_INVALID"
    assert provider.calls == []
    assert external.read_bytes() == external_raw
    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


def test_legacy_bootstrap_link_swap_is_rejected_before_commit(tmp_path: Path) -> None:
    external = tmp_path.parent / f"{tmp_path.name}-late-swap.external"
    external_raw = b"external bytes must remain unchanged"
    external.write_bytes(external_raw)
    legacy_path = tmp_path / "prices_daily.csv"

    class LinkSwappingProvider(RecordingRangePriceProvider):
        def download_prices(self, request: PriceRequest) -> pd.DataFrame:
            _symlink_or_skip(legacy_path, external)
            return super().download_prices(request)

    provider = LinkSwappingProvider()

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        download_daily_data(
            load_universe(),
            start=date(2026, 4, 29),
            end=date(2026, 4, 30),
            output_dir=tmp_path,
            price_provider=provider,
            rate_provider=FakeRateProvider(),
        )

    assert exc_info.value.code == "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_MISMATCH"
    assert len(provider.calls) == 1
    assert external.read_bytes() == external_raw
    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


def test_regular_manifest_replacement_after_outer_check_blocks_pointer_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe = load_universe()
    tickers = tuple(configured_price_tickers(universe))
    prices_path, manifest_path = _write_legacy_bootstrap_fixture(
        tmp_path,
        tickers=tickers,
    )
    original_prices = prices_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    replacement_manifest = original_manifest.replace(
        b"legacy_test_source",
        b"replacement_source",
    )
    original_publish = publication_module.publish_immutable_snapshot
    injected = False

    def replace_manifest_then_publish(**kwargs: Any):
        nonlocal injected
        injected = True
        manifest_path.write_bytes(replacement_manifest)
        return original_publish(**kwargs)

    monkeypatch.setattr(
        publication_module,
        "publish_immutable_snapshot",
        replace_manifest_then_publish,
    )

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        download_daily_data(
            universe,
            start=date(2026, 4, 29),
            end=date(2026, 4, 30),
            output_dir=tmp_path,
            price_provider=RecordingRangePriceProvider(),
            rate_provider=FakeRateProvider(),
        )

    assert injected is True
    assert exc_info.value.code == "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_MISMATCH"
    assert prices_path.read_bytes() == original_prices
    assert manifest_path.read_bytes() == replacement_manifest
    assert resolve_download_publication_if_present(output_dir=tmp_path) is None
    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


def test_prices_link_replacement_after_outer_check_blocks_pointer_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe = load_universe()
    tickers = tuple(configured_price_tickers(universe))
    prices_path, _ = _write_legacy_bootstrap_fixture(
        tmp_path,
        tickers=tickers,
    )
    external = tmp_path.parent / f"{tmp_path.name}-post-check-link.external"
    external_raw = b"external bytes must remain unchanged"
    external.write_bytes(external_raw)
    original_publish = publication_module.publish_immutable_snapshot
    injected = False

    def replace_prices_with_link_then_publish(**kwargs: Any):
        nonlocal injected
        injected = True
        prices_path.unlink()
        _symlink_or_skip(prices_path, external)
        return original_publish(**kwargs)

    monkeypatch.setattr(
        publication_module,
        "publish_immutable_snapshot",
        replace_prices_with_link_then_publish,
    )

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        download_daily_data(
            universe,
            start=date(2026, 4, 29),
            end=date(2026, 4, 30),
            output_dir=tmp_path,
            price_provider=RecordingRangePriceProvider(),
            rate_provider=FakeRateProvider(),
        )

    assert injected is True
    assert exc_info.value.code == "DOWNLOAD_LEGACY_BOOTSTRAP_PRECONDITION_MISMATCH"
    assert prices_path.is_symlink()
    assert external.read_bytes() == external_raw
    assert resolve_download_publication_if_present(output_dir=tmp_path) is None
    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


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
    assert _estimate_marketstack_increment_usage(provider, windows) == len(supported_tickers)


def test_incremental_windows_skip_weekend_tail_for_existing_cache() -> None:
    existing = pd.DataFrame(
        [
            {"date": "2026-06-26", "ticker": "NVDA"},
            {"date": "2026-06-26", "ticker": "MSFT"},
        ]
    )

    windows = _price_fetch_windows(
        existing,
        tickers=("NVDA", "MSFT"),
        start=date(2018, 1, 1),
        end=date(2026, 6, 29),
    )

    assert [(window.start, window.end, window.tickers) for window in windows] == [
        (date(2026, 6, 29), date(2026, 6, 29), ("MSFT", "NVDA"))
    ]


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
    assert owner_approved_overage["policy_version"] == "data_source_request_budget_policy_v2"
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


def test_marketstack_tail_catch_up_splits_missed_trading_days(
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
            "x-quota-remaining": "-738",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    provider = MarketstackPriceProvider(
        api_key="test-key",
        requests_module=_FakeMarketstackCatchUpRequests(),
        request_cache_dir=request_cache_dir,
    )
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )
    _write_price_cache(
        tmp_path / "prices_marketstack_daily.csv",
        tickers=supported_tickers,
        price_date=date(2026, 6, 29),
    )

    summary = download_daily_data(
        load_universe(),
        start=date(2018, 1, 1),
        end=date(2026, 7, 1),
        output_dir=tmp_path,
        price_provider=FakePriceProvider(),
        secondary_price_provider=provider,
        rate_provider=FakeRateProvider(),
    )

    assert isinstance(provider.requests_module, _FakeMarketstackCatchUpRequests)
    assert [call["date_from"] for call in provider.requests_module.calls] == [
        "2026-06-30",
        "2026-07-01",
    ]
    budget = summary.request_budget_statuses[0]
    assert budget["budget_profile"] == "owner_approved_tail_catch_up"
    assert budget["status"] == "OWNER_APPROVED_SMALL_TAIL_CATCH_UP"
    assert budget["estimated_increment_usage"] == 50
    assert budget["fetch_window_count"] == 2
    assert budget["tail_catch_up"]["applied"] is True
    assert budget["tail_catch_up"]["original_fetch_windows"] == [
        {
            "tickers": sorted(supported_tickers),
            "start": "2026-06-30",
            "end": "2026-07-01",
        }
    ]
    assert [window["start"] for window in budget["tail_catch_up"]["split_fetch_windows"]] == [
        "2026-06-30",
        "2026-07-01",
    ]

    assert summary.publication_manifest_path is not None
    marketstack_source = _publication_source_event(
        summary.publication_manifest_path,
        artifact_role="secondary_prices",
        source_id="marketstack_eod_daily_prices",
    )
    request_parameters = marketstack_source["request_parameters"]
    assert isinstance(request_parameters, dict)
    incremental = request_parameters["incremental_refresh"]
    assert incremental["fetch_window_count"] == 2
    assert incremental["request_budget_status"]["budget_profile"] == (
        "owner_approved_tail_catch_up"
    )
    assert incremental["request_budget_status"]["tail_catch_up"]["applied"] is True


def test_marketstack_tail_catch_up_does_not_convert_full_history_refresh(
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
            "x-quota-remaining": "-738",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    fake_marketstack_requests = _NeverRequests()

    with pytest.raises(ProviderQuotaBudgetError) as exc_info:
        download_daily_data(
            load_universe(),
            start=date(2026, 6, 30),
            end=date(2026, 7, 1),
            output_dir=tmp_path,
            price_provider=FakePriceProvider(),
            secondary_price_provider=MarketstackPriceProvider(
                api_key="test-key",
                requests_module=fake_marketstack_requests,
                request_cache_dir=request_cache_dir,
            ),
            rate_provider=FakeRateProvider(),
        )

    assert fake_marketstack_requests.calls == []
    assert exc_info.value.budget_status["budget_profile"] == "owner_approved_overage"
    assert (
        "calendar_window_exceeds_owner_approved_limit"
        in exc_info.value.budget_status["owner_approved_overage"]["violation_reasons"]
    )


def test_marketstack_tail_catch_up_blocks_when_split_shortfall_exceeds_ratio(
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
            "x-quota-remaining": "-960",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
    )
    fake_marketstack_requests = _NeverRequests()
    provider = MarketstackPriceProvider(
        api_key="test-key",
        requests_module=fake_marketstack_requests,
        request_cache_dir=request_cache_dir,
    )
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )
    _write_price_cache(
        tmp_path / "prices_marketstack_daily.csv",
        tickers=supported_tickers,
        price_date=date(2026, 6, 29),
    )

    with pytest.raises(ProviderQuotaBudgetError) as exc_info:
        download_daily_data(
            load_universe(),
            start=date(2018, 1, 1),
            end=date(2026, 7, 1),
            output_dir=tmp_path,
            price_provider=FakePriceProvider(),
            secondary_price_provider=provider,
            rate_provider=FakeRateProvider(),
        )

    assert fake_marketstack_requests.calls == []
    budget = exc_info.value.budget_status
    assert budget["budget_profile"] == "owner_approved_tail_catch_up"
    assert budget["tail_catch_up"]["applied"] is False
    assert (
        "quota_overage_ratio_exceeds_owner_approved_limit"
        in budget["owner_approved_tail_catch_up"]["violation_reasons"]
    )

    report_path = write_download_failure_report(
        output_path=tmp_path / "download_data_diagnostics_2026-07-01.md",
        start=date(2018, 1, 1),
        end=date(2026, 7, 1),
        raw_output_dir=tmp_path,
        include_full_ai_chain=False,
        price_provider_name="fmp",
        with_marketstack=True,
        error=exc_info.value,
    )
    text = report_path.read_text(encoding="utf-8")
    assert "## Provider 请求预算" in text
    assert "owner_approved_tail_catch_up" in text
    assert "quota_overage_ratio_exceeds_owner_approved_limit" in text


def test_marketstack_prior_cycle_quota_header_allows_bounded_tail_bootstrap(
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
            "x-quota-remaining": "-938",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
        requested_at=datetime(2020, 1, 11, tzinfo=UTC),
    )
    fake_requests = _FakeMarketstackRangeRequests()
    provider = MarketstackPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=request_cache_dir,
    )
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )
    _write_price_cache(
        tmp_path / "prices_marketstack_daily.csv",
        tickers=supported_tickers,
        price_date=date(2026, 7, 10),
    )

    summary = download_daily_data(
        load_universe(),
        start=date(2018, 1, 1),
        end=date(2026, 7, 15),
        output_dir=tmp_path,
        price_provider=FakePriceProvider(),
        secondary_price_provider=provider,
        rate_provider=FakeRateProvider(),
    )

    assert len(fake_requests.calls) == 1
    assert fake_requests.calls[0]["date_from"] == "2026-07-13"
    assert fake_requests.calls[0]["date_to"] == "2026-07-15"
    budget = summary.request_budget_statuses[0]
    assert budget["budget_profile"] == "quota_cycle_reset"
    assert budget["status"] == "CURRENT_CYCLE_QUOTA_BOOTSTRAP_ALLOWED"
    assert budget["estimated_increment_usage"] == 25
    assert budget["fetch_window_count"] == 1
    cycle = budget["quota_cycle_reset"]
    assert cycle["approved"] is True
    assert cycle["policy_version"] == "data_source_request_budget_policy_v2"
    assert cycle["stale_header_status"] == "STALE_PREVIOUS_BILLING_CYCLE"
    assert cycle["stale_quota_remaining"] == -938
    expected_start, expected_next = _marketstack_billing_cycle_bounds(
        datetime.now(tz=UTC),
        reset_day_of_month=12,
    )
    assert cycle["current_cycle_start"] == expected_start.date().isoformat()
    assert cycle["current_cycle_end"] < expected_next.date().isoformat()

    assert summary.publication_manifest_path is not None
    marketstack_source = _publication_source_event(
        summary.publication_manifest_path,
        artifact_role="secondary_prices",
        source_id="marketstack_eod_daily_prices",
    )
    request_parameters = marketstack_source["request_parameters"]
    assert isinstance(request_parameters, dict)
    incremental = request_parameters["incremental_refresh"]
    assert incremental["fetch_window_count"] == 1
    assert incremental["request_budget_status"]["budget_profile"] == "quota_cycle_reset"
    assert (
        incremental["request_budget_status"]["quota_cycle_reset"]["evidence_id"]
        == "marketstack_dashboard_billing_cycle_2026-07-12_to_2026-08-11"
    )


def test_marketstack_prior_cycle_quota_header_does_not_allow_full_history(
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
            "x-quota-remaining": "-938",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
        requested_at=datetime(2020, 1, 11, tzinfo=UTC),
    )
    fake_requests = _NeverRequests()

    with pytest.raises(ProviderQuotaBudgetError) as exc_info:
        download_daily_data(
            load_universe(),
            start=date(2026, 7, 13),
            end=date(2026, 7, 15),
            output_dir=tmp_path,
            price_provider=FakePriceProvider(),
            secondary_price_provider=MarketstackPriceProvider(
                api_key="test-key",
                requests_module=fake_requests,
                request_cache_dir=request_cache_dir,
            ),
            rate_provider=FakeRateProvider(),
        )

    assert fake_requests.calls == []
    assert exc_info.value.budget_status["budget_profile"] == "owner_approved_overage"


def test_marketstack_prior_cycle_quota_header_blocks_oversized_tail_bootstrap(
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
            "x-quota-remaining": "-938",
            "x-increment-usage": "25",
        },
        content=b'{"data":[]}',
        cache_dir=request_cache_dir,
        requested_at=datetime(2020, 1, 11, tzinfo=UTC),
    )
    fake_requests = _NeverRequests()
    provider = MarketstackPriceProvider(
        api_key="test-key",
        requests_module=fake_requests,
        request_cache_dir=request_cache_dir,
    )
    supported_tickers = tuple(
        ticker
        for ticker in configured_price_tickers(load_universe())
        if provider.symbol_aliases.get(ticker, ticker) is not None
    )
    _write_price_cache(
        tmp_path / "prices_marketstack_daily.csv",
        tickers=supported_tickers,
        price_date=date(2026, 6, 30),
    )

    with pytest.raises(ProviderQuotaBudgetError) as exc_info:
        download_daily_data(
            load_universe(),
            start=date(2018, 1, 1),
            end=date(2026, 7, 15),
            output_dir=tmp_path,
            price_provider=FakePriceProvider(),
            secondary_price_provider=provider,
            rate_provider=FakeRateProvider(),
        )

    assert fake_requests.calls == []
    budget = exc_info.value.budget_status
    assert budget["budget_profile"] == "quota_cycle_reset"
    assert budget["quota_cycle_reset"]["approved"] is False
    assert (
        "calendar_window_exceeds_quota_cycle_reset_limit"
        in budget["quota_cycle_reset"]["violation_reasons"]
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
    price_manifest = manifest.loc[manifest["output_path"].astype(str) == str(summary.prices_path)]
    assert len(price_manifest) == 1
    assert price_manifest.iloc[0]["source_id"] == "composite_prices_publication"
    assert price_manifest.iloc[0]["row_count"] == summary.price_rows
    request_parameters = json.loads(str(price_manifest.iloc[0]["request_parameters"]))
    assert {item["source_id"] for item in request_parameters["source_events"]} == {
        "cboe_vix_daily_prices",
        "fake_no_vix_price_provider",
    }
    assert summary.publication_manifest_path is not None
    transaction = json.loads(summary.publication_manifest_path.read_text(encoding="utf-8"))
    vix_event = next(
        item
        for item in transaction["source_event_records"]
        if item["source_id"] == "cboe_vix_daily_prices"
    )
    assert vix_event["provider"] == "Cboe Global Markets"
    assert vix_event["winning_row_count"] == 2
    assert vix_event["request_parameters"]["tickers"] == ["^VIX"]


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

    assert summary.publication_manifest_path is not None
    transaction = json.loads(summary.publication_manifest_path.read_text(encoding="utf-8"))
    fmp_event = next(
        item
        for item in transaction["source_event_records"]
        if item["source_id"] == "fmp_eod_daily_prices"
    )

    assert fmp_event["provider"] == "Financial Modeling Prep"
    assert "test-key" not in json.dumps(fmp_event, ensure_ascii=False)
    assert "GOOG" not in fmp_event["request_parameters"]["provider_symbol_aliases"]
    assert fmp_event["request_parameters"]["provider_symbol_aliases"]["^VIX"] is None


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
    def __init__(self, payload: object, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.ok = True
        self.status_code = 200
        self.headers = headers or {}

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


class _FakeMarketstackCatchUpRequests:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        _url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> _FakeResponse:
        assert params["access_key"] == "test-key"
        assert timeout == 30
        self.calls.append(dict(params))
        symbols = str(params["symbols"]).split(",")
        date_from = str(params["date_from"])
        date_to = str(params["date_to"])
        assert date_from == date_to
        records = [
            {
                "date": date_from,
                "symbol": symbol,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "adj_close": 1.0,
                "volume": 1,
            }
            for symbol in symbols
        ]
        return _FakeResponse(
            {"pagination": {"count": len(records), "total": len(records)}, "data": records},
            headers={
                "x-quota-limit": "10000",
                "x-quota-remaining": "-788",
                "x-increment-usage": "25",
            },
        )


class _FakeMarketstackRangeRequests:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        _url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> _FakeResponse:
        assert params["access_key"] == "test-key"
        assert timeout == 30
        self.calls.append(dict(params))
        symbols = str(params["symbols"]).split(",")
        dates = pd.date_range(str(params["date_from"]), str(params["date_to"]), freq="D")
        records = [
            {
                "date": current.strftime("%Y-%m-%d"),
                "symbol": symbol,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "adj_close": 1.0,
                "volume": 1,
            }
            for current in dates
            for symbol in symbols
        ]
        return _FakeResponse(
            {"pagination": {"count": len(records), "total": len(records)}, "data": records},
            headers={
                "x-quota-limit": "10000",
                "x-quota-remaining": "9975",
                "x-increment-usage": "25",
            },
        )


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


def _publication_source_event(
    transaction_path: Path,
    *,
    artifact_role: str,
    source_id: str,
) -> dict[str, object]:
    transaction = json.loads(transaction_path.read_text(encoding="utf-8"))
    sources = transaction["source_event_records"]
    assert isinstance(sources, list)
    matches = [
        source
        for source in sources
        if isinstance(source, dict)
        and source.get("artifact_role") == artifact_role
        and source.get("source_id") == source_id
    ]
    assert len(matches) == 1
    return matches[0]


def _write_price_cache(path: Path, *, tickers: tuple[str, ...], price_date: date) -> None:
    pd.DataFrame(
        [
            {
                "date": price_date.isoformat(),
                "ticker": ticker,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "adj_close": 1.0,
                "volume": 1,
            }
            for ticker in tickers
        ]
    ).to_csv(path, index=False)


def _write_price_cache_dates(
    path: Path,
    *,
    tickers: tuple[str, ...],
    price_dates: tuple[date, ...],
) -> None:
    pd.DataFrame(
        [
            {
                "date": current.isoformat(),
                "ticker": ticker,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "adj_close": 1.0,
                "volume": 1,
            }
            for current in price_dates
            for ticker in tickers
        ],
        columns=list(_PRICE_TEST_COLUMNS),
    ).to_csv(path, index=False)


def _write_legacy_bootstrap_fixture(
    root: Path,
    *,
    tickers: tuple[str, ...],
) -> tuple[Path, Path]:
    prices_path = root / "prices_daily.csv"
    _write_price_cache_dates(
        prices_path,
        tickers=tickers,
        price_dates=(date(2026, 4, 29),),
    )
    prices_raw = prices_path.read_bytes()
    manifest_path = root / "download_manifest.csv"
    pd.DataFrame(
        [
            {
                "downloaded_at": "2026-04-29T00:00:00+00:00",
                "source_id": "legacy_test_source",
                "provider": "Legacy test fixture",
                "endpoint": prices_path.name,
                "request_parameters": "{}",
                "output_path": str(prices_path),
                "checksum_sha256": sha256_bytes(prices_raw),
                "row_count": len(tickers),
            }
        ]
    ).to_csv(manifest_path, index=False)
    return prices_path, manifest_path


def _download_transaction(summary) -> dict[str, object]:
    assert summary.publication_manifest_path is not None
    transaction = json.loads(summary.publication_manifest_path.read_text(encoding="utf-8"))
    assert isinstance(transaction, dict)
    return transaction


def _symlink_or_skip(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symbolic links are unavailable: {exc}")


def _transaction_artifact(
    transaction: dict[str, object],
    *,
    artifact_role: str,
) -> dict[str, object]:
    artifacts = transaction["artifacts"]
    assert isinstance(artifacts, list)
    matches = [
        artifact
        for artifact in artifacts
        if isinstance(artifact, dict) and artifact.get("role") == artifact_role
    ]
    assert len(matches) == 1
    return matches[0]


def _transaction_source(
    transaction: dict[str, object],
    *,
    artifact_role: str,
    source_kind: str,
    source_id: str | None = None,
) -> dict[str, object]:
    sources = transaction["source_event_records"]
    assert isinstance(sources, list)
    matches = [
        source
        for source in sources
        if isinstance(source, dict)
        and source.get("artifact_role") == artifact_role
        and source.get("source_kind") == source_kind
        and (source_id is None or source.get("source_id") == source_id)
    ]
    assert len(matches) == 1
    return matches[0]


def _assert_exact_source_partition(
    transaction: dict[str, object],
    *,
    artifact_role: str,
    artifact_path: Path,
) -> dict[str, set[tuple[str, str]]]:
    identity_column = "series" if artifact_role == "rates" else "ticker"
    frame = pd.read_csv(artifact_path, dtype={"date": str, identity_column: str})
    assert "__download_source_event_id" not in frame.columns
    assert {identity_column, "date"}.issubset(frame.columns)
    final_keys = {
        (str(identity), str(raw_date))
        for identity, raw_date in frame[[identity_column, "date"]].itertuples(
            index=False,
            name=None,
        )
    }
    assert len(final_keys) == len(frame)

    raw_sources = transaction["source_event_records"]
    assert isinstance(raw_sources, list)
    sources = [
        source
        for source in raw_sources
        if isinstance(source, dict) and source.get("artifact_role") == artifact_role
    ]
    assert sources
    remainder_sources = [source for source in sources if source["allocation_mode"] == "REMAINDER"]
    assert len(remainder_sources) == 1

    allocations: dict[str, set[tuple[str, str]]] = {}
    explicit_union: set[tuple[str, str]] = set()
    for source in sources:
        event_id = str(source["source_event_id"])
        if source["allocation_mode"] == "REMAINDER":
            assert source["explicit_row_keys"] == []
            continue
        raw_keys = source["explicit_row_keys"]
        assert isinstance(raw_keys, list)
        keys = {(str(raw[0]), str(raw[1])) for raw in raw_keys}
        assert len(keys) == len(raw_keys)
        assert keys.issubset(final_keys)
        assert explicit_union.isdisjoint(keys)
        explicit_union.update(keys)
        allocations[event_id] = keys

    remainder = remainder_sources[0]
    remainder_id = str(remainder["source_event_id"])
    allocations[remainder_id] = final_keys - explicit_union
    assert set().union(*allocations.values()) == final_keys
    assert sum(len(keys) for keys in allocations.values()) == len(final_keys)
    for source in sources:
        event_id = str(source["source_event_id"])
        keys = allocations[event_id]
        assert source["winning_row_count"] == len(keys)
        assert source["winning_row_keys_sha256"] == _row_keys_digest(keys)
    expected_remainder_id = min(
        allocations,
        key=lambda event_id: (-len(allocations[event_id]), event_id),
    )
    assert remainder_id == expected_remainder_id
    return allocations


def _row_keys_digest(keys: set[tuple[str, str]]) -> str:
    return sha256_bytes(canonical_json_bytes([list(key) for key in sorted(keys)]))
