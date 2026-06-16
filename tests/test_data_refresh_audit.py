from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.data_refresh_audit import (
    AUDIT_STATUS_SUCCESS,
    build_data_refresh_audit_payload,
    validate_data_refresh_audit_payload,
    write_validate_data_audit_sidecar,
)
from ai_trading_system.reports import reader_brief


def test_validate_data_audit_sidecar_records_required_fields(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, as_of=date(2026, 6, 13))
    record_path = write_validate_data_audit_sidecar(
        report=fixture["report"],
        report_path=fixture["report_path"],
        started_at=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
        ended_at=datetime(2026, 6, 13, 10, 1, tzinfo=UTC),
        output_dir=tmp_path / "validation_audit",
    )

    payload = json.loads(record_path.read_text(encoding="utf-8"))

    assert payload["record_type"] == "data_refresh_audit_record"
    assert payload["attempt_type"] == "DATA_VALIDATION"
    assert payload["data_type"] == "cached_market_macro_data"
    assert payload["source"] == "aits validate-data"
    assert payload["as_of"] == "2026-06-13"
    assert payload["status"] == AUDIT_STATUS_SUCCESS
    assert payload["checksum"]
    assert payload["record_count"] == 3
    assert payload["warning_count"] == 0
    assert payload["error_count"] == 0


def test_data_refresh_audit_cli_report_validate_and_reader_brief(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, as_of=date(2026, 6, 13))
    validation_dir = tmp_path / "validation_audit"
    output_dir = tmp_path / "data_refresh_audit"
    market_refresh_root = tmp_path / "empty_market_refresh"
    fallback_policy_report = _write_fallback_policy_report(tmp_path)
    cache_catalog_report = _write_cache_catalog_report(tmp_path)
    market_refresh_root.mkdir()
    write_validate_data_audit_sidecar(
        report=fixture["report"],
        report_path=fixture["report_path"],
        started_at=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
        ended_at=datetime(2026, 6, 13, 10, 1, tzinfo=UTC),
        output_dir=validation_dir,
    )

    report = CliRunner().invoke(
        app,
        [
            "data",
            "refresh-audit",
            "report",
            "--as-of",
            "2026-06-13",
            "--output-dir",
            str(output_dir),
            "--validation-audit-dir",
            str(validation_dir),
            "--market-refresh-root",
            str(market_refresh_root),
            "--price-cache-path",
            str(fixture["price_path"]),
            "--fallback-policy-report-path",
            str(fallback_policy_report),
            "--cache-catalog-report-path",
            str(cache_catalog_report),
        ],
    )
    validation = CliRunner().invoke(
        app,
        [
            "data",
            "refresh-audit",
            "validate",
            "--latest",
            "--output-dir",
            str(output_dir),
        ],
    )

    assert report.exit_code == 0, report.output
    assert validation.exit_code == 0, validation.output
    assert "Data refresh audit status=PASS" in report.output
    assert "Data refresh audit validation status=PASS" in validation.output

    latest_pointer = json.loads(
        (output_dir / "latest_data_refresh_audit.json").read_text(encoding="utf-8")
    )
    audit_path = Path(latest_pointer["audit_path"])
    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
    summary = reader_brief._data_refresh_audit_summary(
        {
            "reports": [
                {
                    "report_id": "data_refresh_audit",
                    "latest_artifact_path": str(audit_path),
                }
            ]
        }
    )

    assert audit_payload["summary"]["skipped_market_closed_count"] == 1
    assert audit_payload["fallback_policy_summary"]["fallback_status"] == "FALLBACK_USED"
    assert audit_payload["fallback_policy_summary"]["fallback_used_count"] == 1
    assert audit_payload["cache_catalog_summary"]["cache_integrity_status"] == "OK"
    assert audit_payload["cache_catalog_summary"]["entry_count"] == 4
    assert "cache_catalog=integrity=OK" in report.output
    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "PASS"
    assert summary["audit_record_count"] == 2
    assert summary["skipped_record_count"] == 1
    assert summary["safety_status"] == "PASS"
    assert summary["production_effect"] == "none"


def test_data_refresh_audit_validation_fails_invalid_status(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, as_of=date(2026, 6, 13))
    validation_dir = tmp_path / "validation_audit"
    write_validate_data_audit_sidecar(
        report=fixture["report"],
        report_path=fixture["report_path"],
        started_at=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
        ended_at=datetime(2026, 6, 13, 10, 1, tzinfo=UTC),
        output_dir=validation_dir,
    )
    payload = build_data_refresh_audit_payload(
        as_of=date(2026, 6, 13),
        validation_audit_dir=validation_dir,
        market_refresh_root=tmp_path / "empty_market_refresh",
        price_cache_path=fixture["price_path"],
    )
    payload["records"][0]["status"] = "BAD_STATUS"

    validation = validate_data_refresh_audit_payload(
        payload,
        audit_path=tmp_path / "data_refresh_audit.json",
    )

    assert validation.status == "FAIL"
    assert any(issue.code == "audit_record_invalid_status" for issue in validation.issues)


def _fixture(tmp_path: Path, *, as_of: date) -> dict[str, object]:
    price_path = tmp_path / "prices_daily.csv"
    rate_path = tmp_path / "rates_daily.csv"
    report_path = tmp_path / f"data_quality_{as_of.isoformat()}.md"
    price_path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        "2026-06-12,NVDA,1,1,1,1,1,100\n"
        "2026-06-12,MSFT,1,1,1,1,1,100\n",
        encoding="utf-8",
    )
    rate_path.write_text("date,series,value\n2026-06-12,DGS10,4.1\n", encoding="utf-8")
    report_path.write_text("# data quality\n", encoding="utf-8")
    return {
        "price_path": price_path,
        "report_path": report_path,
        "report": DataQualityReport(
            checked_at=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
            as_of=as_of,
            price_summary=DataFileSummary(
                path=price_path,
                exists=True,
                rows=2,
                sha256=_sha256(price_path),
                min_date=date(2026, 6, 12),
                max_date=date(2026, 6, 12),
            ),
            rate_summary=DataFileSummary(
                path=rate_path,
                exists=True,
                rows=1,
                sha256=_sha256(rate_path),
                min_date=date(2026, 6, 12),
                max_date=date(2026, 6, 12),
            ),
            expected_price_tickers=("NVDA", "MSFT"),
            expected_rate_series=("DGS10",),
        ),
    }


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _write_fallback_policy_report(tmp_path: Path) -> Path:
    report_path = tmp_path / "data_source_fallback_policy.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "data_source_fallback_policy",
                "report_id": "fallback-policy-test",
                "as_of": "2026-06-13",
                "status": "PASS_WITH_WARNINGS",
                "validation_status": "PASS_WITH_WARNINGS",
                "production_effect": "none",
                "safety_boundary": {
                    "read_only": True,
                    "data_refresh_allowed": False,
                    "cache_mutation_allowed": False,
                    "score_or_backtest_allowed": False,
                    "broker_action_allowed": False,
                    "order_ticket_allowed": False,
                    "production_state_mutation_allowed": False,
                },
                "summary": {
                    "fallback_status": "FALLBACK_USED",
                    "source_group_count": 2,
                    "primary_ok_count": 1,
                    "fallback_used_count": 1,
                    "fallback_unavailable_count": 0,
                    "blocked_no_valid_source_count": 0,
                    "blocking_source_count": 0,
                    "fallback_used_sources": ["marketstack_eod_daily_prices"],
                    "blocking_data_types": [],
                    "next_action": "manual_review_fallback_metadata_before_interpretation",
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report_path


def _write_cache_catalog_report(tmp_path: Path) -> Path:
    report_path = tmp_path / "cache_catalog.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "cache_catalog",
                "catalog_id": "cache-catalog-test",
                "as_of": "2026-06-13",
                "status": "PASS",
                "validation_status": "PASS",
                "cache_integrity_status": "OK",
                "production_effect": "none",
                "summary": {
                    "cache_integrity_status": "OK",
                    "entry_count": 4,
                    "required_entry_count": 4,
                    "missing_required_count": 0,
                    "missing_optional_count": 0,
                    "checksum_mismatch_count": 0,
                    "checksum_changed_without_refresh_count": 0,
                    "blocking_entry_count": 0,
                    "blocking_entry_ids": [],
                    "warning_entry_ids": [],
                    "refresh_audit_id": "data_refresh_audit_test",
                    "validated_at": "2026-06-13T10:01:00+00:00",
                    "next_action": "cache_catalog_clear_for_manual_review",
                },
                "safety_boundary": {
                    "read_only": True,
                    "data_refresh_allowed": False,
                    "cache_mutation_allowed": False,
                    "cache_repair_allowed": False,
                    "score_or_backtest_allowed": False,
                    "broker_action_allowed": False,
                    "order_ticket_allowed": False,
                    "production_state_mutation_allowed": False,
                },
            }
        ),
        encoding="utf-8",
    )
    return report_path
