from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pytest import MonkeyPatch, approx
from typer.testing import CliRunner

import ai_trading_system.cli as cli_module
from ai_trading_system.cli import app
from ai_trading_system.config import load_universe, load_watchlist
from ai_trading_system.fmp_forward_pit import (
    attach_fmp_forward_pit_raw_paths,
    fetch_fmp_forward_pit_snapshots,
    normalize_fmp_forward_pit_payloads,
    write_fmp_forward_pit_normalized_csv,
    write_fmp_forward_pit_raw_payloads,
)
from ai_trading_system.valuation import (
    SnapshotMetric,
    ValuationIssueSeverity,
    ValuationSnapshot,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
)
from ai_trading_system.valuation_sources import (
    EodhdEarningsTrendsFetchReport,
    FmpAnalystEstimateHistorySnapshot,
    FmpHistoricalValuationFetchReport,
    FmpHttpValuationProvider,
    fetch_eodhd_earnings_trend_snapshots,
    fetch_fmp_historical_valuation_snapshots,
    fetch_fmp_valuation_snapshots,
    import_valuation_snapshots_from_csv,
    load_fmp_analyst_estimate_history_snapshots,
    load_fmp_forward_pit_analyst_estimate_history,
    render_eodhd_earnings_trends_fetch_report,
    render_fmp_historical_valuation_fetch_report,
    render_fmp_valuation_fetch_report,
    render_valuation_csv_import_report,
    validate_fmp_analyst_estimate_history,
    write_eodhd_earnings_trends_raw_payload,
    write_fmp_analyst_estimate_history_snapshots,
    write_fmp_historical_valuation_raw_payloads,
    write_valuation_snapshots_as_yaml,
)


def test_import_valuation_snapshots_from_csv_success(tmp_path: Path) -> None:
    csv_path = tmp_path / "valuation_export.csv"
    _write_valuation_csv(csv_path)

    report = import_valuation_snapshots_from_csv(csv_path)

    assert report.status == "PASS"
    assert report.row_count == 1
    assert report.imported_count == 1
    assert len(report.checksum_sha256) == 64
    snapshot = report.snapshots[0]
    assert snapshot.snapshot_id == "nvda_valuation_2026_05_01"
    assert snapshot.ticker == "NVDA"
    assert [metric.metric_id for metric in snapshot.valuation_metrics] == [
        "forward_pe",
        "ev_sales",
        "peg",
    ]
    assert [metric.metric_id for metric in snapshot.expectation_metrics] == [
        "revenue_growth_next_12m_pct",
        "eps_revision_90d_pct",
    ]
    assert snapshot.valuation_metrics[0].unit == "ratio"
    assert snapshot.expectation_metrics[0].unit == "percent"
    assert snapshot.crowding_signals[0].status == "elevated"
    assert "SHA256" in render_valuation_csv_import_report(report)


def test_import_valuation_snapshots_reports_invalid_numeric_and_dates(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "bad_valuation_export.csv"
    _write_valuation_csv(
        csv_path,
        as_of="2026/05/01",
        captured_at="not-a-date",
        forward_pe="not-a-number",
        valuation_percentile="high",
        crowding_updated_at="2026-13-01",
    )

    report = import_valuation_snapshots_from_csv(csv_path)

    assert report.status == "FAIL"
    assert report.imported_count == 0
    assert {
        (issue.code, issue.column)
        for issue in report.issues
        if issue.severity == ValuationIssueSeverity.ERROR
    } >= {
        ("invalid_date", "as_of"),
        ("invalid_date", "captured_at"),
        ("invalid_numeric", "forward_pe"),
        ("invalid_numeric", "valuation_percentile"),
        ("invalid_date", "crowding_updated_at"),
    }


def test_write_imported_valuation_yaml_round_trips_through_existing_validation(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "valuation_export.csv"
    output_dir = tmp_path / "valuation_snapshots"
    _write_valuation_csv(csv_path)
    import_report = import_valuation_snapshots_from_csv(csv_path)

    written_paths = write_valuation_snapshots_as_yaml(import_report.snapshots, output_dir)
    store = load_valuation_snapshot_store(output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=store,
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert written_paths == (output_dir / "nvda_valuation_2026_05_01.yaml",)
    assert store.load_errors == ()
    assert validation_report.status == "PASS"
    assert validation_report.snapshot_count == 1


def test_import_valuation_snapshots_warns_public_convenience_source(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "public_convenience_export.csv"
    _write_valuation_csv(csv_path, source_type="public_convenience")

    import_report = import_valuation_snapshots_from_csv(csv_path)
    output_dir = tmp_path / "valuation_snapshots"
    write_valuation_snapshots_as_yaml(import_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert import_report.status == "PASS_WITH_WARNINGS"
    assert "public_convenience_source" in {issue.code for issue in import_report.issues}
    assert validation_report.status == "PASS_WITH_WARNINGS"
    assert "public_convenience_source" in {issue.code for issue in validation_report.issues}


def test_valuation_import_csv_cli_writes_yaml_and_reports(tmp_path: Path) -> None:
    csv_path = tmp_path / "valuation_export.csv"
    output_dir = tmp_path / "valuation_snapshots"
    import_report_path = tmp_path / "valuation_import.md"
    validation_report_path = tmp_path / "valuation_validation.md"
    _write_valuation_csv(csv_path)

    result = CliRunner().invoke(
        app,
        [
            "valuation",
            "import-csv",
            "--input-path",
            str(csv_path),
            "--output-dir",
            str(output_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(import_report_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "估值 CSV 导入状态：PASS" in result.output
    assert (output_dir / "nvda_valuation_2026_05_01.yaml").exists()
    assert import_report_path.exists()
    assert validation_report_path.exists()


def test_fetch_fmp_valuation_snapshots_builds_paid_vendor_snapshot() -> None:
    report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        captured_at=date(2026, 5, 2),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.imported_count == 1
    assert report.row_count == 5
    assert len(report.checksum_sha256) == 64
    snapshot = report.snapshots[0]
    assert snapshot.snapshot_id == "fmp_nvda_valuation_2026_05_02"
    assert snapshot.source_type == "paid_vendor"
    assert snapshot.source_name == "Financial Modeling Prep"
    valuation_metrics = {metric.metric_id: metric for metric in snapshot.valuation_metrics}
    expectation_metrics = {
        metric.metric_id: metric for metric in snapshot.expectation_metrics
    }
    assert valuation_metrics["forward_pe"].value == 30
    assert valuation_metrics["forward_pe"].source_field == (
        "quote-short.price / analyst-estimates.epsAvg"
    )
    assert valuation_metrics["ev_sales"].value == 18
    assert valuation_metrics["peg"].value == 1.5
    assert expectation_metrics["revenue_growth_next_12m_pct"].value == approx(20)
    assert "eps_revision_90d_pct" in render_fmp_valuation_fetch_report(report)


def test_fetch_fmp_valuation_snapshots_calculates_eps_revision_from_history(
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "fmp_history"
    write_fmp_analyst_estimate_history_snapshots(
        [
            FmpAnalystEstimateHistorySnapshot(
                ticker="NVDA",
                as_of=date(2026, 2, 1),
                captured_at=date(2026, 2, 1),
                downloaded_at=datetime(2026, 2, 1, tzinfo=UTC),
                endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
                request_parameters={
                    "symbol": "NVDA",
                    "period": "annual",
                    "page": 0,
                    "limit": 10,
                },
                row_count=2,
                checksum_sha256="",
                records=(
                    {"symbol": "NVDA", "date": "2027-01-31", "epsAvg": 2.0},
                    {"symbol": "NVDA", "date": "2026-01-31", "epsAvg": 3.0},
                ),
            )
        ],
        history_dir,
    )
    loaded_history = load_fmp_analyst_estimate_history_snapshots(history_dir, ["NVDA"])

    report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        analyst_history_dir=history_dir,
        captured_at=date(2026, 5, 2),
    )

    assert len(loaded_history) == 1
    assert report.historical_analyst_snapshot_count == 1
    snapshot = report.snapshots[0]
    expectation_metrics = {
        metric.metric_id: metric for metric in snapshot.expectation_metrics
    }
    assert expectation_metrics["eps_revision_90d_pct"].value == approx(100)
    assert "missing_eps_revision_history" not in {issue.code for issue in report.issues}


def test_write_fmp_analyst_estimate_history_snapshots_preserves_same_day_runs(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "fmp_history"
    records = ({"symbol": "NVDA", "date": "2027-01-31", "epsAvg": 2.0},)

    written = write_fmp_analyst_estimate_history_snapshots(
        [
            FmpAnalystEstimateHistorySnapshot(
                ticker="NVDA",
                as_of=date(2026, 5, 2),
                captured_at=date(2026, 5, 2),
                downloaded_at=datetime(2026, 5, 2, 10, 0, tzinfo=UTC),
                endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
                request_parameters={"symbol": "NVDA", "period": "annual", "limit": 10},
                row_count=1,
                checksum_sha256="",
                records=records,
            ),
            FmpAnalystEstimateHistorySnapshot(
                ticker="NVDA",
                as_of=date(2026, 5, 2),
                captured_at=date(2026, 5, 2),
                downloaded_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
                endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
                request_parameters={"symbol": "NVDA", "period": "annual", "limit": 10},
                row_count=1,
                checksum_sha256="",
                records=records,
            ),
        ],
        output_dir,
    )

    assert len(written) == 2
    assert len(set(written)) == 2
    assert all(path.exists() for path in written)
    assert all("2026-05-02T" in path.name for path in written)
    loaded = load_fmp_analyst_estimate_history_snapshots(output_dir, ["NVDA"])
    assert len(loaded) == 2


def test_fetch_fmp_valuation_snapshots_calculates_eps_revision_from_pit_asof(
    tmp_path: Path,
) -> None:
    pit_path = _write_fmp_forward_pit_normalized_csv(
        tmp_path,
        [
            (datetime(2026, 2, 1, 12, 0, tzinfo=UTC), 2.0),
            (datetime(2026, 5, 3, 12, 0, tzinfo=UTC), 1.0),
        ],
    )
    pit_history = load_fmp_forward_pit_analyst_estimate_history(
        pit_path,
        tickers=["NVDA"],
        decision_time=datetime(2026, 5, 2, 23, 59, 59, tzinfo=UTC),
    )

    report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        pit_normalized_path=pit_path,
        captured_at=date(2026, 5, 2),
    )

    expectation_metrics = {
        metric.metric_id: metric for metric in report.snapshots[0].expectation_metrics
    }
    assert len(pit_history) == 1
    assert pit_history[0].records[0]["epsAvg"] == 2.0
    assert report.pit_analyst_snapshot_count == 1
    assert report.historical_analyst_snapshot_count == 1
    assert expectation_metrics["eps_revision_90d_pct"].value == approx(100)
    assert "PIT normalized 输入" in render_fmp_valuation_fetch_report(report)


def test_fetch_fmp_valuation_snapshots_downgrades_when_pit_history_not_visible(
    tmp_path: Path,
) -> None:
    pit_path = _write_fmp_forward_pit_normalized_csv(
        tmp_path,
        [(datetime(2026, 5, 3, 12, 0, tzinfo=UTC), 1.0)],
    )

    report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        pit_normalized_path=pit_path,
        captured_at=date(2026, 5, 2),
    )
    expectation_metrics = {
        metric.metric_id: metric for metric in report.snapshots[0].expectation_metrics
    }

    assert report.pit_analyst_snapshot_count == 0
    assert "eps_revision_90d_pct" not in expectation_metrics
    assert "missing_eps_revision_history" in {issue.code for issue in report.issues}


def test_fetch_fmp_valuation_snapshots_calculates_local_valuation_percentile(
    tmp_path: Path,
) -> None:
    valuation_history_dir = tmp_path / "valuation_history"
    write_valuation_snapshots_as_yaml(
        [
            _fmp_history_snapshot(
                as_of=date(2026, 2, 2),
                forward_pe=20,
                ev_sales=10,
                peg=1.0,
            ),
            _fmp_history_snapshot(
                as_of=date(2026, 3, 2),
                forward_pe=25,
                ev_sales=15,
                peg=1.2,
            ),
            _fmp_history_snapshot(
                as_of=date(2026, 4, 2),
                forward_pe=40,
                ev_sales=30,
                peg=2.0,
            ),
        ],
        valuation_history_dir,
    )

    report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        valuation_history_dir=valuation_history_dir,
        captured_at=date(2026, 5, 2),
    )

    assert report.historical_valuation_snapshot_count == 3
    assert report.snapshots[0].valuation_percentile == approx(66.6666667)
    assert "missing_valuation_percentile_history" not in {
        issue.code for issue in report.issues
    }


def test_fetch_fmp_historical_valuation_snapshots_generates_percentile_history(
    tmp_path: Path,
) -> None:
    valuation_history_dir = tmp_path / "valuation_history"
    raw_history_dir = tmp_path / "raw_history"

    history_report = fetch_fmp_historical_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        captured_at=date(2026, 5, 2),
        downloaded_at=datetime(2026, 5, 2, tzinfo=UTC),
        limit=3,
    )

    assert history_report.status == "PASS"
    assert history_report.row_count == 6
    assert len(history_report.raw_payloads) == 1
    assert len(history_report.snapshots) == 3
    assert history_report.snapshots[0].point_in_time_class == (
        "backfilled_history_distribution"
    )
    assert history_report.snapshots[0].history_source_class == "vendor_historical_endpoint"
    assert history_report.snapshots[0].confidence_level == "low"
    assert history_report.snapshots[0].backtest_use == "captured_at_forward_only"
    assert {metric.metric_id for metric in history_report.snapshots[0].valuation_metrics} == {
        "ev_sales",
        "peg",
    }
    assert "historical `key-metrics`" in render_fmp_historical_valuation_fetch_report(
        history_report
    )

    raw_paths = write_fmp_historical_valuation_raw_payloads(
        history_report.raw_payloads,
        raw_history_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(
        history_report.snapshots,
        valuation_history_dir,
    )
    raw_payload = json.loads(raw_paths[0].read_text(encoding="utf-8"))
    assert raw_payload["row_count"] == 6
    assert raw_payload["request_parameters_by_endpoint"]["key-metrics"]["symbol"] == "NVDA"
    assert len(written_paths) == 3

    current_report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        valuation_history_dir=valuation_history_dir,
        captured_at=date(2026, 5, 2),
    )

    assert current_report.historical_valuation_snapshot_count == 3
    assert current_report.snapshots[0].valuation_percentile == approx(66.6666667)
    assert current_report.snapshots[0].point_in_time_class == "captured_snapshot"
    assert current_report.snapshots[0].confidence_level == "medium"


def test_fetch_fmp_valuation_snapshots_uses_provider_symbol_alias() -> None:
    report = fetch_fmp_valuation_snapshots(
        ["GOOG"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(),
        captured_at=date(2026, 5, 2),
    )

    assert report.requested_tickers == ("GOOG",)
    assert report.snapshots[0].ticker == "GOOG"
    assert report.analyst_estimate_history_snapshots[0].request_parameters["symbol"] == "GOOGL"
    assert "GOOG->GOOGL" in render_fmp_valuation_fetch_report(report)


def test_fetch_fmp_valuation_snapshots_redacts_api_key_from_errors() -> None:
    report = fetch_fmp_valuation_snapshots(
        ["GOOG"],
        "test-key",
        date(2026, 5, 2),
        provider=_FailingFmpProvider(),
        captured_at=date(2026, 5, 2),
    )

    assert report.status == "FAIL"
    assert "secret-key" not in report.issues[0].message
    assert "apikey=<redacted>" in report.issues[0].message


def test_fmp_http_provider_retries_transient_ssl_error() -> None:
    fake_requests = _FlakyRequestsModule()
    provider = FmpHttpValuationProvider(
        "test-key",
        requests_module=fake_requests,
        retry_backoff_seconds=0,
    )

    records = provider.fetch_ratios_ttm("INTC")

    assert fake_requests.call_count == 2
    assert records == [{"symbol": "INTC", "forwardPriceToEarningsGrowthRatioTTM": -3.8}]


def test_fetch_fmp_valuation_snapshots_skips_negative_provider_multiple() -> None:
    report = fetch_fmp_valuation_snapshots(
        ["INTC"],
        "test-key",
        date(2026, 5, 2),
        provider=_NegativePegFmpProvider(),
        captured_at=date(2026, 5, 2),
    )

    valuation_metrics = {
        metric.metric_id: metric for metric in report.snapshots[0].valuation_metrics
    }
    assert "peg" not in valuation_metrics
    assert all(metric.value >= 0 for metric in report.snapshots[0].valuation_metrics)
    assert "invalid_fmp_valuation_multiple" in {issue.code for issue in report.issues}


def test_validate_fmp_analyst_estimate_history_checks_cache(
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "fmp_history"
    write_fmp_analyst_estimate_history_snapshots(
        [
            FmpAnalystEstimateHistorySnapshot(
                ticker="NVDA",
                as_of=date(2026, 5, 2),
                captured_at=date(2026, 5, 2),
                downloaded_at=datetime(2026, 5, 2, tzinfo=UTC),
                endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
                request_parameters={
                    "symbol": "NVDA",
                    "period": "annual",
                    "page": 0,
                    "limit": 10,
                },
                row_count=1,
                checksum_sha256="",
                records=(
                    {"symbol": "NVDA", "date": "2027-01-31", "epsAvg": 4.0},
                ),
            )
        ],
        history_dir,
    )

    report = validate_fmp_analyst_estimate_history(
        history_dir,
        date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.snapshot_count == 1
    assert report.record_count == 1


def test_validate_fmp_analyst_estimate_history_rejects_bad_checksum(
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "fmp_history" / "nvda"
    history_dir.mkdir(parents=True)
    history_path = history_dir / "fmp_analyst_estimates_nvda_2026-05-02.json"
    history_path.write_text(
        json.dumps(
            {
                "provider": "Financial Modeling Prep",
                "source_type": "paid_vendor",
                "ticker": "NVDA",
                "as_of": "2026-05-02",
                "captured_at": "2026-05-02",
                "downloaded_at": "2026-05-02T00:00:00+00:00",
                "endpoint": "https://financialmodelingprep.com/stable/analyst-estimates",
                "request_parameters": {
                    "symbol": "NVDA",
                    "period": "annual",
                    "page": 0,
                    "limit": 10,
                },
                "row_count": 1,
                "checksum_sha256": "bad",
                "records": [{"symbol": "NVDA", "date": "2027-01-31", "epsAvg": 4.0}],
            }
        ),
        encoding="utf-8",
    )

    report = validate_fmp_analyst_estimate_history(
        history_dir.parent,
        date(2026, 5, 2),
    )

    assert report.status == "FAIL"
    assert "fmp_history_load_error" in {issue.code for issue in report.issues}


def test_fetch_fmp_valuation_snapshots_reports_missing_core_metrics() -> None:
    report = fetch_fmp_valuation_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpProvider(empty=True),
    )

    assert report.status == "FAIL"
    assert report.imported_count == 0
    assert "missing_fmp_valuation_metrics" in {issue.code for issue in report.issues}


def test_valuation_fetch_fmp_cli_writes_yaml_and_reports(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    output_dir = tmp_path / "valuation_snapshots"
    analyst_history_dir = tmp_path / "fmp_analyst_estimates"
    fetch_report_path = tmp_path / "fmp_fetch.md"
    validation_report_path = tmp_path / "valuation_validation.md"

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        *,
        analyst_history_dir: Path | str | None = None,
        pit_normalized_path: Path | str | None = None,
        valuation_history_dir: Path | str | None = None,
        captured_at: date | None = None,
        analyst_estimate_limit: int = 10,
    ) -> Any:
        return fetch_fmp_valuation_snapshots(
            tickers,
            api_key,
            as_of,
            provider=_FakeFmpProvider(),
            analyst_history_dir=analyst_history_dir,
            pit_normalized_path=pit_normalized_path,
            valuation_history_dir=valuation_history_dir,
            captured_at=captured_at or as_of,
            analyst_estimate_limit=analyst_estimate_limit,
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(cli_module, "fetch_fmp_valuation_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "valuation",
            "fetch-fmp",
            "--tickers",
            "NVDA",
            "--output-dir",
            str(output_dir),
            "--analyst-history-dir",
            str(analyst_history_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(fetch_report_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "FMP 估值拉取状态：PASS_WITH_WARNINGS" in result.output
    assert (output_dir / "fmp_nvda_valuation_2026_05_02.yaml").exists()
    assert (
        len(
            list(
                (analyst_history_dir / "nvda").glob(
                    "fmp_analyst_estimates_nvda_2026-05-02_*.json"
                )
            )
        )
        == 1
    )
    assert fetch_report_path.exists()
    assert validation_report_path.exists()


def test_valuation_fetch_fmp_valuation_history_cli_writes_raw_yaml_and_reports(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    output_dir = tmp_path / "valuation_snapshots"
    raw_output_dir = tmp_path / "fmp_historical_valuation"
    fetch_report_path = tmp_path / "fmp_history_fetch.md"
    validation_report_path = tmp_path / "valuation_validation.md"

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        *,
        captured_at: date | None = None,
        period: str = "annual",
        limit: int = 8,
    ) -> FmpHistoricalValuationFetchReport:
        return fetch_fmp_historical_valuation_snapshots(
            tickers,
            api_key,
            as_of,
            provider=_FakeFmpProvider(),
            captured_at=captured_at or as_of,
            downloaded_at=datetime(2026, 5, 2, tzinfo=UTC),
            period=period,
            limit=limit,
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(cli_module, "fetch_fmp_historical_valuation_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "valuation",
            "fetch-fmp-valuation-history",
            "--tickers",
            "NVDA",
            "--output-dir",
            str(output_dir),
            "--raw-output-dir",
            str(raw_output_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(fetch_report_path),
            "--validation-report-path",
            str(validation_report_path),
            "--limit",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert "FMP 历史估值拉取状态：PASS" in result.output
    assert (
        raw_output_dir / "nvda" / "fmp_historical_valuation_nvda_2026-05-02.json"
    ).exists()
    assert (
        output_dir
        / "fmp_nvda_historical_valuation_2026_01_31_captured_2026_05_02.yaml"
    ).exists()
    assert fetch_report_path.exists()
    assert validation_report_path.exists()


def test_fetch_eodhd_earnings_trends_merges_eps_revision_into_base_snapshot(
    tmp_path: Path,
) -> None:
    base_dir = tmp_path / "valuation_snapshots"
    write_valuation_snapshots_as_yaml(
        [_base_current_valuation_snapshot()],
        base_dir,
    )

    report = fetch_eodhd_earnings_trend_snapshots(
        ["NVDA"],
        "test-token",
        date(2026, 5, 2),
        provider=_FakeEodhdProvider(),
        base_valuation_dir=base_dir,
        captured_at=date(2026, 5, 2),
        downloaded_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    assert report.status == "PASS"
    assert report.row_count == 2
    assert report.imported_count == 1
    assert len(report.checksum_sha256) == 64
    snapshot = report.snapshots[0]
    assert snapshot.snapshot_id == "merged_nvda_valuation_eodhd_trends_2026_05_02"
    assert snapshot.source_type == "paid_vendor"
    assert snapshot.source_name == "EODHD Earnings Trends + Financial Modeling Prep"
    assert snapshot.history_source_class == "vendor_current_trend"
    assert snapshot.backtest_use == "captured_at_forward_only"
    assert snapshot.valuation_percentile == 70
    assert {metric.metric_id for metric in snapshot.valuation_metrics} == {
        "forward_pe",
        "ev_sales",
    }
    expectation_metrics = {
        metric.metric_id: metric for metric in snapshot.expectation_metrics
    }
    assert expectation_metrics["revenue_growth_next_12m_pct"].value == 20
    assert expectation_metrics["eps_revision_90d_pct"].value == approx(20)
    assert "EODHD Earnings Trends" in render_eodhd_earnings_trends_fetch_report(report)

    raw_paths = write_eodhd_earnings_trends_raw_payload(
        report.raw_payload,
        tmp_path / "raw",
    )
    raw_payload = json.loads(raw_paths[0].read_text(encoding="utf-8"))
    assert raw_payload["provider"] == "EODHD Earnings Trends"
    assert raw_payload["request_parameters"]["symbols"] == "NVDA.US"
    assert raw_payload["row_count"] == 2


def test_fetch_eodhd_earnings_trends_reports_missing_90d_input(
    tmp_path: Path,
) -> None:
    base_dir = tmp_path / "valuation_snapshots"
    write_valuation_snapshots_as_yaml(
        [_base_current_valuation_snapshot()],
        base_dir,
    )

    report = fetch_eodhd_earnings_trend_snapshots(
        ["NVDA"],
        "test-token",
        date(2026, 5, 2),
        provider=_FakeEodhdProvider(missing_90d=True),
        base_valuation_dir=base_dir,
        captured_at=date(2026, 5, 2),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.imported_count == 0
    assert "missing_eodhd_eps_trend_90d" in {issue.code for issue in report.issues}


def test_valuation_fetch_eodhd_trends_cli_writes_raw_yaml_and_reports(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    output_dir = tmp_path / "valuation_snapshots"
    raw_output_dir = tmp_path / "eodhd_earnings_trends"
    fetch_report_path = tmp_path / "eodhd_fetch.md"
    validation_report_path = tmp_path / "valuation_validation.md"
    write_valuation_snapshots_as_yaml(
        [_base_current_valuation_snapshot()],
        output_dir,
    )

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        *,
        base_valuation_dir: Path | str | None = None,
        captured_at: date | None = None,
    ) -> EodhdEarningsTrendsFetchReport:
        return fetch_eodhd_earnings_trend_snapshots(
            tickers,
            api_key,
            as_of,
            provider=_FakeEodhdProvider(),
            base_valuation_dir=base_valuation_dir,
            captured_at=captured_at or as_of,
            downloaded_at=datetime(2026, 5, 2, tzinfo=UTC),
        )

    monkeypatch.setenv("EODHD_API_KEY", "test-token")
    monkeypatch.setattr(cli_module, "fetch_eodhd_earnings_trend_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "valuation",
            "fetch-eodhd-trends",
            "--tickers",
            "NVDA",
            "--output-dir",
            str(output_dir),
            "--raw-output-dir",
            str(raw_output_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(fetch_report_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "EODHD trends 拉取状态：PASS" in result.output
    assert (
        output_dir / "merged_nvda_valuation_eodhd_trends_2026_05_02.yaml"
    ).exists()
    assert (
        raw_output_dir
        / "nvda"
        / "eodhd_earnings_trends_nvda_2026-05-02.json"
    ).exists()
    assert fetch_report_path.exists()
    assert validation_report_path.exists()


def test_valuation_validate_fmp_history_cli_writes_report(tmp_path: Path) -> None:
    history_dir = tmp_path / "fmp_history"
    report_path = tmp_path / "fmp_history_validation.md"
    write_fmp_analyst_estimate_history_snapshots(
        [
            FmpAnalystEstimateHistorySnapshot(
                ticker="NVDA",
                as_of=date(2026, 5, 2),
                captured_at=date(2026, 5, 2),
                downloaded_at=datetime(2026, 5, 2, tzinfo=UTC),
                endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
                request_parameters={
                    "symbol": "NVDA",
                    "period": "annual",
                    "page": 0,
                    "limit": 10,
                },
                row_count=1,
                checksum_sha256="",
                records=(
                    {"symbol": "NVDA", "date": "2027-01-31", "epsAvg": 4.0},
                ),
            )
        ],
        history_dir,
    )

    result = CliRunner().invoke(
        app,
        [
            "valuation",
            "validate-fmp-history",
            "--input-path",
            str(history_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "FMP analyst history 校验状态：PASS" in result.output
    assert report_path.exists()


class _FakeEodhdProvider:
    def __init__(self, *, missing_90d: bool = False) -> None:
        self.missing_90d = missing_90d

    def fetch_earnings_trends(self, provider_symbols: tuple[str, ...]) -> dict[str, Any]:
        assert provider_symbols == ("NVDA.US",)
        eps_90d = None if self.missing_90d else "5.0000"
        return {
            "type": "Trends",
            "description": "Historical and upcoming earning trends",
            "symbols": "NVDA.US",
            "trends": [
                [
                    {
                        "code": "NVDA.US",
                        "date": "2027-01-31",
                        "period": "+1y",
                        "epsTrendCurrent": "6.0000",
                        "epsTrend90daysAgo": eps_90d,
                        "revenueEstimateGrowth": "0.25",
                    },
                    {
                        "code": "NVDA.US",
                        "date": "2026-07-31",
                        "period": "+1q",
                        "epsTrendCurrent": "1.5000",
                        "epsTrend90daysAgo": "1.2500",
                    },
                ]
            ],
        }


class _FakeFmpProvider:
    def __init__(self, *, empty: bool = False) -> None:
        self.empty = empty

    def fetch_quote_short(self, ticker: str) -> list[dict[str, Any]]:
        if self.empty:
            return []
        return [{"symbol": ticker, "price": 120.0}]

    def fetch_key_metrics_ttm(self, ticker: str) -> list[dict[str, Any]]:
        if self.empty:
            return []
        return [{"symbol": ticker, "evToSalesTTM": 18.0}]

    def fetch_ratios_ttm(self, ticker: str) -> list[dict[str, Any]]:
        if self.empty:
            return []
        return [{"symbol": ticker, "forwardPriceToEarningsGrowthRatioTTM": 1.5}]

    def fetch_key_metrics(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if self.empty:
            return []
        assert period == "annual"
        return [
            {"symbol": ticker, "date": "2026-01-31", "evToSales": 30.0},
            {"symbol": ticker, "date": "2025-01-31", "evToSales": 15.0},
            {"symbol": ticker, "date": "2024-01-31", "evToSales": 10.0},
        ][:limit]

    def fetch_ratios(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if self.empty:
            return []
        assert period == "annual"
        return [
            {
                "symbol": ticker,
                "date": "2026-01-31",
                "forwardPriceToEarningsGrowthRatio": 2.0,
            },
            {
                "symbol": ticker,
                "date": "2025-01-31",
                "forwardPriceToEarningsGrowthRatio": 1.2,
            },
            {
                "symbol": ticker,
                "date": "2024-01-31",
                "forwardPriceToEarningsGrowthRatio": 1.0,
            },
        ][:limit]

    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if self.empty:
            return []
        return [
            {
                "symbol": ticker,
                "date": "2027-01-31",
                "revenueAvg": 120.0,
                "epsAvg": 4.0,
            },
            {
                "symbol": ticker,
                "date": "2026-01-31",
                "revenueAvg": 100.0,
                "epsAvg": 3.0,
            },
        ]


class _FakeFmpForwardPitProvider(_FakeFmpProvider):
    def __init__(self, *, eps_avg: float) -> None:
        super().__init__()
        self.eps_avg = eps_avg

    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return [
            {
                "symbol": ticker,
                "date": "2027-01-31",
                "revenueAvg": 120.0,
                "epsAvg": self.eps_avg,
            },
            {
                "symbol": ticker,
                "date": "2026-01-31",
                "revenueAvg": 100.0,
                "epsAvg": 3.0,
            },
        ][:limit]

    def fetch_price_target_summary(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "lastMonth": 950.0}]

    def fetch_price_target_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "targetConsensus": 1000.0}]

    def fetch_grades(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "publishedDate": "2026-05-01", "newGrade": "Buy"}]

    def fetch_grades_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "buy": 20}]

    def fetch_ratings_snapshot(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "rating": "A-"}]

    def fetch_earnings_calendar(
        self,
        *,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        return [{"symbol": "NVDA", "date": "2026-05-28", "epsEstimated": 1.0}]


class _FailingFmpProvider:
    def fetch_quote_short(self, ticker: str) -> list[dict[str, Any]]:
        raise RuntimeError(
            "402 Client Error for url: "
            "https://financialmodelingprep.com/stable/quote-short?symbol=GOOGL&apikey=secret-key"
        )

    def fetch_key_metrics_ttm(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_ratios_ttm(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return []


class _NegativePegFmpProvider(_FakeFmpProvider):
    def fetch_ratios_ttm(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "forwardPriceToEarningsGrowthRatioTTM": -3.8}]


class _TransientSslError(Exception):
    pass


class _FlakyRequestsModule:
    def __init__(self) -> None:
        self.call_count = 0

    def get(
        self,
        url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> _FakeResponse:
        self.call_count += 1
        if self.call_count == 1:
            raise _TransientSslError("SSL EOF occurred in violation of protocol")
        assert url.endswith("/ratios-ttm")
        assert params["symbol"] == "INTC"
        assert timeout == 30
        return _FakeResponse(
            [{"symbol": "INTC", "forwardPriceToEarningsGrowthRatioTTM": -3.8}]
        )


class _FakeResponse:
    def __init__(self, payload: list[dict[str, Any]]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict[str, Any]]:
        return self._payload


def _fmp_history_snapshot(
    *,
    as_of: date,
    forward_pe: float,
    ev_sales: float,
    peg: float,
) -> ValuationSnapshot:
    date_token = as_of.isoformat().replace("-", "_")
    return ValuationSnapshot(
        snapshot_id=f"fmp_nvda_valuation_{date_token}",
        ticker="NVDA",
        as_of=as_of,
        source_type="paid_vendor",
        source_name="Financial Modeling Prep",
        captured_at=as_of,
        valuation_metrics=[
            SnapshotMetric(
                metric_id="forward_pe",
                value=forward_pe,
                unit="ratio",
                period="next_annual_estimate",
            ),
            SnapshotMetric(
                metric_id="ev_sales",
                value=ev_sales,
                unit="ratio",
                period="ttm",
            ),
            SnapshotMetric(
                metric_id="peg",
                value=peg,
                unit="ratio",
                period="next_12m",
            ),
        ],
    )


def _base_current_valuation_snapshot() -> ValuationSnapshot:
    return ValuationSnapshot(
        snapshot_id="fmp_nvda_valuation_2026_05_02",
        ticker="NVDA",
        as_of=date(2026, 5, 2),
        source_type="paid_vendor",
        source_name="Financial Modeling Prep",
        source_url="https://financialmodelingprep.com/stable/quote-short",
        captured_at=date(2026, 5, 2),
        point_in_time_class="captured_snapshot",
        history_source_class="captured_snapshot_history",
        confidence_level="medium",
        confidence_reason="FMP current snapshot test input.",
        backtest_use="captured_at_forward_only",
        valuation_metrics=[
            SnapshotMetric(
                metric_id="forward_pe",
                value=30.0,
                unit="ratio",
                period="next_annual_estimate",
            ),
            SnapshotMetric(
                metric_id="ev_sales",
                value=18.0,
                unit="ratio",
                period="ttm",
            ),
        ],
        expectation_metrics=[
            SnapshotMetric(
                metric_id="revenue_growth_next_12m_pct",
                value=20.0,
                unit="percent",
                period="next_annual_estimate",
            ),
            SnapshotMetric(
                metric_id="eps_revision_90d_pct",
                value=1.0,
                unit="percent",
                period="trailing_90d",
                source_field="analyst-estimates.epsAvg",
            ),
        ],
        valuation_percentile=70,
        overall_assessment="expensive",
        notes="Base FMP snapshot.",
    )


def _write_fmp_forward_pit_normalized_csv(
    tmp_path: Path,
    snapshots: list[tuple[datetime, float]],
) -> Path:
    raw_dir = tmp_path / "raw" / "fmp_forward_pit"
    normalized_rows = []
    for downloaded_at, eps_avg in snapshots:
        report = fetch_fmp_forward_pit_snapshots(
            ["NVDA"],
            "test-key",
            downloaded_at.date(),
            provider=_FakeFmpForwardPitProvider(eps_avg=eps_avg),
            captured_at=downloaded_at.date(),
            downloaded_at=downloaded_at,
            analyst_estimate_limit=2,
        )
        raw_paths = write_fmp_forward_pit_raw_payloads(report.raw_payloads, raw_dir)
        attached_payloads = attach_fmp_forward_pit_raw_paths(
            report.raw_payloads,
            raw_paths,
            project_root=tmp_path,
        )
        normalized_rows.extend(normalize_fmp_forward_pit_payloads(attached_payloads))
    return write_fmp_forward_pit_normalized_csv(
        tuple(normalized_rows),
        tmp_path / "processed" / "pit_snapshots" / "fmp_forward_pit.csv",
    )


def _write_valuation_csv(
    path: Path,
    *,
    as_of: str = "2026-05-01",
    source_type: str = "manual_input",
    captured_at: str = "2026-05-01",
    forward_pe: str = "36.0",
    valuation_percentile: str = "82",
    crowding_updated_at: str = "2026-05-01",
) -> None:
    path.write_text(
        "\n".join(
            [
                "snapshot_id,ticker,as_of,source_type,source_name,captured_at,"
                "source_url,valuation_percentile,overall_assessment,notes,"
                "forward_pe,ev_sales,peg,revenue_growth_next_12m_pct,"
                "eps_revision_90d_pct,crowding_status,crowding_signal_name,"
                "crowding_evidence_source,crowding_updated_at,crowding_notes",
                "nvda_valuation_2026_05_01,NVDA,"
                f"{as_of},{source_type},vendor_export,{captured_at},"
                f"https://example.test/nvda,{valuation_percentile},expensive,"
                "CSV import test,"
                f"{forward_pe},18.0,1.2,28.0,3.5,elevated,"
                "valuation_percentile,vendor_export,"
                f"{crowding_updated_at},AI infrastructure crowding elevated",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
