from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

import ai_trading_system.cli as cli_module
from ai_trading_system.cli import app
from ai_trading_system.config import DataSourceConfig, DataSourcesConfig
from ai_trading_system.fmp_forward_pit import (
    FmpForwardPitRawPayload,
    attach_fmp_forward_pit_raw_paths,
    fetch_fmp_forward_pit_snapshots,
    normalize_fmp_forward_pit_payloads,
    render_fmp_forward_pit_fetch_report,
    retarget_fmp_forward_pit_normalized_rows,
    write_fmp_forward_pit_normalized_csv,
    write_fmp_forward_pit_raw_payloads,
)
from ai_trading_system.pit_snapshots import (
    discover_existing_pit_raw_snapshots,
    validate_pit_snapshot_manifest,
    write_pit_snapshot_manifest,
)


def test_fetch_fmp_forward_pit_writes_raw_normalized_and_manifest(
    tmp_path: Path,
) -> None:
    report = fetch_fmp_forward_pit_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpForwardPitProvider(),
        captured_at=date(2026, 5, 2),
        downloaded_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
        analyst_estimate_limit=2,
    )
    raw_dir = tmp_path / "raw" / "fmp_forward_pit"
    raw_paths = write_fmp_forward_pit_raw_payloads(report.raw_payloads, raw_dir)
    attached_payloads = attach_fmp_forward_pit_raw_paths(
        report.raw_payloads,
        raw_paths,
        project_root=tmp_path,
    )
    normalized_rows = normalize_fmp_forward_pit_payloads(attached_payloads)
    normalized_path = write_fmp_forward_pit_normalized_csv(
        normalized_rows,
        tmp_path / "processed" / "fmp_forward_pit.csv",
    )
    manifest_records = discover_existing_pit_raw_snapshots(
        fmp_analyst_history_dir=tmp_path / "missing_fmp_analyst",
        fmp_historical_valuation_dir=tmp_path / "missing_fmp_history",
        eodhd_earnings_trends_dir=tmp_path / "missing_eodhd",
        fmp_forward_pit_dir=raw_dir,
        data_sources=_data_sources(),
        project_root=tmp_path,
    )
    manifest_path = write_pit_snapshot_manifest(
        manifest_records,
        tmp_path / "pit_snapshots" / "manifest.csv",
    )
    pit_report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    raw_payload = json.loads(raw_paths[0].read_text(encoding="utf-8"))
    normalized_csv = list(csv.DictReader(normalized_path.open(encoding="utf-8")))

    assert report.status == "PASS"
    assert report.row_count == 8
    assert len(raw_paths) == 1
    assert raw_payload["records_by_endpoint"]["analyst-estimates"][0]["nested"] == {
        "source": "unit",
        "values": [1, {"inner": True}],
    }
    assert raw_payload["records_by_endpoint"]["earnings-calendar"][0]["symbol"] == "NVDA"
    assert raw_payload["request_parameters_by_endpoint"]["earnings-calendar"][
        "filtered_symbol"
    ] == "NVDA"
    assert len(normalized_rows) == 8
    assert {row.endpoint_category for row in normalized_rows} == {
        "analyst_estimates",
        "price_target",
        "ratings",
        "earnings_calendar",
    }
    assert all(
        row.available_time == "2026-05-02T12:00:00+00:00" for row in normalized_rows
    )
    assert normalized_csv[0]["raw_payload_sha256"] == manifest_records[0].raw_payload_sha256
    assert pit_report.status == "PASS"
    assert "available_time <= decision_time" in render_fmp_forward_pit_fetch_report(report)


def test_normalized_id_handles_non_ascii_long_record_keys() -> None:
    unusual_symbol = "NVDA-中文-🚀-" + ("X" * 5000)
    payload = FmpForwardPitRawPayload(
        ticker="NVDA",
        as_of=date(2026, 5, 11),
        captured_at=date(2026, 5, 11),
        downloaded_at=datetime(2026, 5, 12, 2, 0, tzinfo=UTC),
        provider_symbol="NVDA",
        endpoint_records={
            "analyst-estimates": (
                {
                    "symbol": unusual_symbol,
                    "epsAvg": 4.25,
                    "comment": "保留非 ASCII 审计字段",
                    "nested": {"ignored": "not_scalar"},
                },
            ),
        },
        request_parameters_by_endpoint={"analyst-estimates": {"symbol": "NVDA"}},
        checksum_sha256="b" * 64,
    )

    first = normalize_fmp_forward_pit_payloads((payload,))
    second = normalize_fmp_forward_pit_payloads((payload,))

    assert len(first) == 1
    assert first[0].normalized_id == second[0].normalized_id
    assert len(first[0].normalized_id) <= 153
    assert all(ord(character) < 128 for character in first[0].normalized_id)
    digest_suffix = first[0].normalized_id.rsplit("_", maxsplit=1)[-1]
    assert len(digest_suffix) == 12
    int(digest_suffix, 16)
    assert "保留非 ASCII 审计字段" in first[0].normalized_values_json
    assert "nested" not in first[0].normalized_values_json


def test_retarget_normalized_rows_updates_raw_file_identity(tmp_path: Path) -> None:
    report = fetch_fmp_forward_pit_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FakeFmpForwardPitProvider(),
        captured_at=date(2026, 5, 2),
        downloaded_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
        analyst_estimate_limit=2,
    )
    raw_paths = write_fmp_forward_pit_raw_payloads(
        report.raw_payloads,
        tmp_path / "raw" / "fmp_forward_pit",
    )
    attached_payloads = attach_fmp_forward_pit_raw_paths(
        report.raw_payloads,
        raw_paths,
        project_root=tmp_path,
    )

    retargeted = retarget_fmp_forward_pit_normalized_rows(
        report.normalized_rows,
        attached_payloads,
    )

    assert len(retargeted) == len(report.normalized_rows)
    assert retargeted[0].raw_payload_path == raw_paths[0].relative_to(tmp_path).as_posix()
    assert retargeted[0].raw_payload_sha256 == attached_payloads[0].checksum_sha256
    assert retargeted[0].snapshot_id != report.normalized_rows[0].snapshot_id
    assert retargeted[0].normalized_values_json == report.normalized_rows[0].normalized_values_json


def test_fetch_fmp_forward_pit_redacts_api_key_from_errors() -> None:
    report = fetch_fmp_forward_pit_snapshots(
        ["NVDA"],
        "test-key",
        date(2026, 5, 2),
        provider=_FailingFmpForwardPitProvider(),
        captured_at=date(2026, 5, 2),
        downloaded_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert "secret-key" not in report.issues[0].message
    assert "apikey=<redacted>" in report.issues[0].message


def test_pit_snapshots_fetch_fmp_forward_cli_writes_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    raw_dir = tmp_path / "raw" / "fmp_forward_pit"
    normalized_path = tmp_path / "processed" / "fmp_forward_pit.csv"
    manifest_path = tmp_path / "pit_snapshots" / "manifest.csv"
    fetch_report_path = tmp_path / "reports" / "fmp_forward_pit.md"
    pit_report_path = tmp_path / "reports" / "pit_validation.md"

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        **kwargs: Any,
    ):
        return fetch_fmp_forward_pit_snapshots(
            tickers,
            api_key,
            as_of,
            provider=_FakeFmpForwardPitProvider(),
            downloaded_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
            **kwargs,
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(cli_module, "fetch_fmp_forward_pit_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "fetch-fmp-forward",
            "--tickers",
            "NVDA",
            "--raw-output-dir",
            str(raw_dir),
            "--normalized-output-path",
            str(normalized_path),
            "--manifest-path",
            str(manifest_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(fetch_report_path),
            "--pit-validation-report-path",
            str(pit_report_path),
            "--analyst-estimate-limit",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "FMP PIT 抓取状态：PASS" in result.output
    assert raw_dir.exists()
    assert normalized_path.exists()
    assert manifest_path.exists()
    assert fetch_report_path.exists()
    assert pit_report_path.exists()
    assert "PIT manifest 状态：PASS" in result.output
    assert "fmp_forward_pit" in manifest_path.read_text(encoding="utf-8")


def test_pit_snapshots_fetch_fmp_forward_cli_reports_unhandled_fetch_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fetch_report_path = tmp_path / "reports" / "fmp_forward_pit.md"

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        **kwargs: Any,
    ):
        raise RuntimeError(
            "502 Server Error for url: "
            "https://financialmodelingprep.com/stable/analyst-estimates?"
            "symbol=NVDA&apikey=secret-key"
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(cli_module, "fetch_fmp_forward_pit_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "fetch-fmp-forward",
            "--tickers",
            "NVDA",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(fetch_report_path),
        ],
    )

    assert result.exit_code == 1
    assert "FMP PIT 抓取状态：FAIL" in result.output
    assert "失败阶段：fetch" in result.output
    assert fetch_report_path.exists()
    report_text = fetch_report_path.read_text(encoding="utf-8")
    assert "fmp_forward_pit_unhandled_fetch_error" in report_text
    assert "secret-key" not in report_text
    assert "apikey=<redacted>" in report_text


def test_pit_snapshots_fetch_fmp_forward_cli_can_continue_on_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fetch_report_path = tmp_path / "reports" / "fmp_forward_pit.md"

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        **kwargs: Any,
    ):
        raise RuntimeError("network timeout")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(cli_module, "fetch_fmp_forward_pit_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "fetch-fmp-forward",
            "--tickers",
            "NVDA",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(fetch_report_path),
            "--continue-on-failure",
        ],
    )

    assert result.exit_code == 0
    assert "已启用" in result.output
    assert "--continue-on-failure" in result.output
    assert "后续命令仍必须执行自己的质量门禁" in result.output
    assert fetch_report_path.exists()


class _FakeFmpForwardPitProvider:
    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        assert period == "annual"
        return [
            {
                "symbol": ticker,
                "date": "2027-01-31",
                "epsAvg": 4.0,
                "revenueAvg": 120.0,
                "nested": {"source": "unit", "values": [1, {"inner": True}]},
            },
            {"symbol": ticker, "date": "2026-01-31", "epsAvg": 3.0, "revenueAvg": 100.0},
        ][:limit]

    def fetch_price_target_summary(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "lastMonth": 950.0, "lastQuarter": 900.0}]

    def fetch_price_target_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "targetHigh": 1200.0, "targetConsensus": 1000.0}]

    def fetch_grades(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "publishedDate": "2026-05-01", "newGrade": "Buy"}]

    def fetch_grades_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "strongBuy": 10, "buy": 20, "hold": 5}]

    def fetch_ratings_snapshot(self, ticker: str) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "rating": "A-", "score": 4}]

    def fetch_earnings_calendar(
        self,
        *,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        assert from_date == date(2026, 4, 25)
        assert to_date == date(2026, 7, 31)
        return [
            {"symbol": "NVDA", "date": "2026-05-28", "epsEstimated": 1.0},
            {"symbol": "AAPL", "date": "2026-05-04", "epsEstimated": 2.0},
        ]


class _FailingFmpForwardPitProvider(_FakeFmpForwardPitProvider):
    def fetch_price_target_summary(self, ticker: str) -> list[dict[str, Any]]:
        raise RuntimeError(
            "402 Client Error for url: "
            "https://financialmodelingprep.com/stable/price-target-summary?"
            "symbol=NVDA&apikey=secret-key"
        )


def _data_sources() -> DataSourcesConfig:
    return DataSourcesConfig(
        sources=[
            DataSourceConfig(
                source_id="fmp_valuation_expectations",
                provider="Financial Modeling Prep",
                source_type="paid_vendor",
                status="active",
                domains=["valuation", "fundamentals"],
                endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
                adapter="fetch_fmp_forward_pit_snapshots",
                cadence="daily",
                requires_credentials=True,
                cache_paths=["data/raw/fmp_forward_pit"],
                primary_for=["forward_only_pit_archive"],
                audit_fields=["provider", "endpoint", "request_parameters", "checksum"],
                validation_checks=["schema", "checksum"],
                limitations=["测试来源。"],
                owner_notes="test",
            )
        ]
    )
