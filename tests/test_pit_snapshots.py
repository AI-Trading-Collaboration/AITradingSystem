from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import DataSourceConfig, DataSourcesConfig
from ai_trading_system.pit_snapshots import (
    PIT_SNAPSHOT_PARSER_VERSION,
    PIT_SNAPSHOT_SCHEMA_VERSION,
    PitSnapshotIssueSeverity,
    PitSnapshotManifestRecord,
    discover_existing_pit_raw_snapshots,
    render_pit_snapshot_validation_report,
    validate_pit_snapshot_manifest,
    write_pit_snapshot_manifest,
)


def test_validate_pit_snapshot_manifest_passes_valid_manifest(tmp_path: Path) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    manifest_path = _write_manifest(tmp_path / "manifest.csv", _record(payload_path, tmp_path))

    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )
    markdown = render_pit_snapshot_validation_report(report)

    assert report.status == "PASS"
    assert report.snapshot_count == 1
    assert report.row_count == 1
    assert "PIT 快照归档质量报告" in markdown
    assert "available_time <= decision_time" in markdown


def test_validate_pit_snapshot_manifest_rejects_checksum_mismatch(
    tmp_path: Path,
) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    record = replace(_record(payload_path, tmp_path), raw_payload_sha256="bad")
    manifest_path = _write_manifest(tmp_path / "manifest.csv", record)

    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    assert report.status == "FAIL"
    assert "pit_snapshot_raw_payload_checksum_mismatch" in {
        issue.code for issue in report.issues
    }


def test_validate_pit_snapshot_manifest_rejects_missing_payload(
    tmp_path: Path,
) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    record = _record(payload_path, tmp_path)
    payload_path.unlink()
    manifest_path = _write_manifest(tmp_path / "manifest.csv", record)

    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    assert report.status == "FAIL"
    assert "pit_snapshot_raw_payload_missing" in {issue.code for issue in report.issues}


def test_validate_pit_snapshot_manifest_rejects_future_available_time(
    tmp_path: Path,
) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    record = replace(
        _record(payload_path, tmp_path),
        available_time="2026-05-03T00:00:00+00:00",
    )
    manifest_path = _write_manifest(tmp_path / "manifest.csv", record)

    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    assert not report.passed
    assert "available_time_after_ingested_at" in {issue.code for issue in report.issues}


def test_validate_pit_snapshot_manifest_rejects_duplicate_snapshot_id(
    tmp_path: Path,
) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    record = _record(payload_path, tmp_path)
    manifest_path = tmp_path / "manifest.csv"
    write_pit_snapshot_manifest([record, record], manifest_path)

    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    assert report.status == "FAIL"
    assert any(
        issue.severity == PitSnapshotIssueSeverity.ERROR
        and issue.code == "duplicate_pit_snapshot_id"
        for issue in report.issues
    )


def test_discover_existing_pit_raw_snapshots_from_fmp_cache(tmp_path: Path) -> None:
    raw_dir = tmp_path / "fmp_analyst_estimates"
    payload_path = _write_raw_payload(
        raw_dir / "nvda" / "fmp_analyst_estimates_nvda_2026-05-02.json"
    )

    records = discover_existing_pit_raw_snapshots(
        fmp_analyst_history_dir=raw_dir,
        fmp_historical_valuation_dir=tmp_path / "missing_fmp_history",
        eodhd_earnings_trends_dir=tmp_path / "missing_eodhd",
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    assert len(records) == 1
    assert records[0].source_id == "fmp_valuation_expectations"
    assert records[0].canonical_ticker == "NVDA"
    assert records[0].raw_payload_sha256 == _file_sha256(payload_path)
    assert records[0].point_in_time_class == "captured_snapshot"
    assert records[0].backtest_use == "captured_at_forward_only"


def test_discover_existing_pit_raw_snapshots_keeps_same_day_reruns_unique(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "fmp_analyst_estimates"
    _write_raw_payload(
        raw_dir / "nvda" / "fmp_analyst_estimates_nvda_2026-05-02_0000.json",
        downloaded_at=datetime(2026, 5, 2, 0, 0, tzinfo=UTC),
    )
    _write_raw_payload(
        raw_dir / "nvda" / "fmp_analyst_estimates_nvda_2026-05-02_1200.json",
        downloaded_at=datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
    )

    records = discover_existing_pit_raw_snapshots(
        fmp_analyst_history_dir=raw_dir,
        fmp_historical_valuation_dir=tmp_path / "missing_fmp_history",
        eodhd_earnings_trends_dir=tmp_path / "missing_eodhd",
        data_sources=_data_sources(),
        project_root=tmp_path,
    )
    manifest_path = tmp_path / "manifest.csv"
    write_pit_snapshot_manifest(records, manifest_path)
    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=date(2026, 5, 2),
        data_sources=_data_sources(),
        project_root=tmp_path,
    )

    assert len(records) == 2
    assert len({record.snapshot_id for record in records}) == 2
    assert report.status == "PASS"


def test_pit_snapshots_cli_validate_writes_report(tmp_path: Path) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    manifest_path = _write_manifest(tmp_path / "manifest.csv", _record(payload_path, tmp_path))
    config_path = _write_data_sources_config(tmp_path / "data_sources.yaml")
    report_path = tmp_path / "pit_report.md"

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "validate",
            "--input-path",
            str(manifest_path),
            "--data-sources-path",
            str(config_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "PIT 快照归档状态：PASS" in result.output
    assert report_path.exists()
    assert "PIT 快照归档质量报告" in report_path.read_text(encoding="utf-8")


def test_pit_snapshots_cli_validate_returns_nonzero_on_error(tmp_path: Path) -> None:
    payload_path = _write_raw_payload(tmp_path / "raw" / "nvda.json")
    record = replace(_record(payload_path, tmp_path), raw_payload_sha256="bad")
    manifest_path = _write_manifest(tmp_path / "manifest.csv", record)
    config_path = _write_data_sources_config(tmp_path / "data_sources.yaml")
    report_path = tmp_path / "pit_report.md"

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "validate",
            "--input-path",
            str(manifest_path),
            "--data-sources-path",
            str(config_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert "PIT 快照归档状态：FAIL" in result.output
    assert report_path.exists()


def test_pit_snapshots_cli_build_manifest_from_existing_cache(
    tmp_path: Path,
) -> None:
    _write_raw_payload(
        tmp_path
        / "fmp_analyst_estimates"
        / "nvda"
        / "fmp_analyst_estimates_nvda_2026-05-02.json"
    )
    config_path = _write_data_sources_config(tmp_path / "data_sources.yaml")
    manifest_path = tmp_path / "pit_snapshots" / "manifest.csv"
    report_path = tmp_path / "pit_report.md"

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "build-manifest",
            "--output-path",
            str(manifest_path),
            "--data-sources-path",
            str(config_path),
            "--fmp-analyst-history-dir",
            str(tmp_path / "fmp_analyst_estimates"),
            "--fmp-historical-valuation-dir",
            str(tmp_path / "missing_fmp_history"),
            "--eodhd-earnings-trends-dir",
            str(tmp_path / "missing_eodhd"),
            "--as-of",
            "2026-05-02",
            "--validation-report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "生成 PIT" in result.output
    assert "manifest" in result.output
    assert manifest_path.exists()
    assert report_path.exists()


def _record(payload_path: Path, project_root: Path) -> PitSnapshotManifestRecord:
    raw = json.loads(payload_path.read_text(encoding="utf-8"))
    return PitSnapshotManifestRecord(
        snapshot_id="fmp_analyst_estimates_nvda_2026_05_02",
        source_id="fmp_valuation_expectations",
        source_name="Financial Modeling Prep",
        source_type="paid_vendor",
        source_quality_tier="paid_vendor",
        endpoint="https://financialmodelingprep.com/stable/analyst-estimates",
        request_params=json.dumps(raw["request_parameters"], sort_keys=True),
        provider_symbol="NVDA",
        canonical_ticker="NVDA",
        provider_symbol_alias="none",
        http_status="not_recorded",
        content_type="application/json",
        response_headers="not_recorded",
        raw_payload_path=payload_path.relative_to(project_root).as_posix(),
        raw_payload_sha256=_file_sha256(payload_path),
        raw_payload_bytes=payload_path.stat().st_size,
        snapshot_time="2026-05-02T00:00:00+00:00",
        ingested_at="2026-05-02T00:00:00+00:00",
        vendor_timestamp="not_recorded",
        available_time="2026-05-02T00:00:00+00:00",
        row_count=1,
        parser_version=PIT_SNAPSHOT_PARSER_VERSION,
        schema_version=PIT_SNAPSHOT_SCHEMA_VERSION,
        license_use_class="paid_vendor_terms_not_reviewed_for_external_llm",
        redistribution_allowed=False,
        llm_processing_allowed=False,
        point_in_time_class="captured_snapshot",
        history_source_class="captured_snapshot_history",
        backtest_use="captured_at_forward_only",
        confidence_level="medium",
        confidence_reason="test forward-only raw snapshot",
        validation_status="PASS",
        validation_report_path="not_recorded",
    )


def _write_manifest(path: Path, record: PitSnapshotManifestRecord) -> Path:
    return write_pit_snapshot_manifest([record], path)


def _write_raw_payload(
    path: Path,
    *,
    downloaded_at: datetime | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    captured_at = downloaded_at or datetime(2026, 5, 2, tzinfo=UTC)
    raw = {
        "provider": "Financial Modeling Prep",
        "source_type": "paid_vendor",
        "ticker": "NVDA",
        "as_of": "2026-05-02",
        "captured_at": captured_at.date().isoformat(),
        "downloaded_at": captured_at.isoformat(),
        "endpoint": "https://financialmodelingprep.com/stable/analyst-estimates",
        "request_parameters": {
            "symbol": "NVDA",
            "period": "annual",
            "page": 0,
            "limit": 10,
        },
        "row_count": 1,
        "checksum_sha256": "source-payload-record-checksum",
        "records": [{"symbol": "NVDA", "date": "2027-01-31", "epsAvg": 5.0}],
    }
    path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    return path


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
                adapter="fetch_fmp_valuation_snapshots",
                cadence="daily",
                requires_credentials=True,
                cache_paths=["data/raw/fmp_analyst_estimates"],
                primary_for=["expectation_snapshot"],
                audit_fields=["provider", "endpoint", "request_parameters", "checksum"],
                validation_checks=["schema", "checksum"],
                limitations=["测试来源。"],
                owner_notes="test",
            )
        ]
    )


def _write_data_sources_config(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(_data_sources().model_dump(mode="json"), sort_keys=False),
        encoding="utf-8",
    )
    return path


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
