from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.production_boundary_static_scan import (
    FAIL_STATUS,
    OK_STATUS,
    WARN_STATUS,
    build_production_boundary_static_scan_payload,
    latest_production_boundary_static_scan_json_path,
    render_production_boundary_static_scan_markdown,
    validate_production_boundary_static_scan_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_static_scan_passes_explicit_safe_context(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "src" / "safe_boundary.py",
        "BROKER_ACTION_ALLOWED = False\nORDER_TICKET_GENERATED = False\n",
    )
    _write_file(
        tmp_path / "config" / "safe.yaml",
        "production_effect: none\nnot_official_target_weights: true\n",
    )
    payload = build_production_boundary_static_scan_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_production_boundary_static_scan_payload(payload)
    markdown = render_production_boundary_static_scan_markdown(payload)

    assert payload["scan_status"] == OK_STATUS
    assert payload["summary"]["blocking_finding_count"] == 0
    assert payload["summary"]["warning_finding_count"] == 0
    assert payload["summary"]["allowed_match_count"] >= 2
    assert validation["validation_status"] == OK_STATUS
    assert "Production Boundary Static Scan" in markdown


def test_static_scan_blocks_unsafe_source_integration(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "src" / "unsafe_broker.py",
        "def run_live_broker():\n    broker.submit_order(symbol='QQQ')\n",
    )
    payload = build_production_boundary_static_scan_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_production_boundary_static_scan_payload(payload)

    assert payload["scan_status"] == FAIL_STATUS
    assert payload["summary"]["blocking_finding_count"] >= 1
    assert any(finding["term_family"] == "broker" for finding in payload["blocking_findings"])
    assert validation["validation_status"] == FAIL_STATUS


def test_static_scan_warns_for_documentation_only_mentions(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "docs" / "future_note.md",
        "Future review may discuss broker integration terminology.\n",
    )
    payload = build_production_boundary_static_scan_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_production_boundary_static_scan_payload(payload)

    assert payload["scan_status"] == WARN_STATUS
    assert payload["summary"]["blocking_finding_count"] == 0
    assert payload["summary"]["warning_finding_count"] == 1
    assert payload["warning_findings"][0]["allowlist_rule_id"] == (
        "docs_documentation_only_mentions"
    )
    assert validation["validation_status"] == WARN_STATUS
    assert validation["summary"]["failed_check_count"] == 0
    assert validation["summary"]["warning_check_count"] == 1


def test_static_scan_blocks_secret_like_literal_and_masks_context(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "config" / "secret.yaml",
        'api_key: "live_secret_value_123456789"\n',
    )
    payload = build_production_boundary_static_scan_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
    )

    assert payload["scan_status"] == FAIL_STATUS
    finding = payload["blocking_findings"][0]
    assert finding["term_family"] == "api_key"
    assert "live_secret_value" not in finding["context"]
    assert "<redacted>" in finding["context"]


def test_static_scan_cli_writes_report_and_validation(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "src" / "safe_boundary.py",
        "broker_action_allowed = False\norder_ticket_generated = False\n",
    )
    reports_dir = tmp_path / "outputs" / "reports"
    audit_json = reports_dir / "production_boundary_static_scan_2026-05-04.json"
    audit_md = reports_dir / "production_boundary_static_scan_2026-05-04.md"
    validation_json = (
        reports_dir / "production_boundary_static_scan_validation_2026-05-04.json"
    )
    validation_md = reports_dir / "production_boundary_static_scan_validation_2026-05-04.md"

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "production-boundary-static-scan",
            "--as-of",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-production-boundary-static-scan",
            "--source-json-path",
            str(audit_json),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation.exit_code == 0, validation.output
    assert audit_md.exists()
    assert validation_md.exists()
    assert json.loads(audit_json.read_text(encoding="utf-8"))["scan_status"] == OK_STATUS
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation_payload["validation_status"] == OK_STATUS
    assert validation_payload["input_artifacts"]["production_boundary_static_scan"] == str(
        audit_json
    )

    latest_validation = runner.invoke(
        app,
        [
            "reports",
            "validate-production-boundary-static-scan",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert latest_validation.exit_code == 0, latest_validation.output
    assert latest_production_boundary_static_scan_json_path(reports_dir) == audit_json


def test_reader_brief_static_scan_summary_reads_report_index(tmp_path: Path) -> None:
    _write_file(
        tmp_path / "docs" / "future_note.md",
        "Future review may discuss broker integration terminology.\n",
    )
    payload = build_production_boundary_static_scan_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_production_boundary_static_scan_payload(payload)
    scan_path = tmp_path / "production_boundary_static_scan_2026-05-04.json"
    validation_path = tmp_path / "production_boundary_static_scan_validation_2026-05-04.json"
    scan_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")
    report_index = {
        "reports": [
            {
                "report_id": "production_boundary_static_scan",
                "latest_artifact_path": str(scan_path),
            },
            {
                "report_id": "production_boundary_static_scan_validation",
                "latest_artifact_path": str(validation_path),
            },
        ]
    }

    summary = reader_brief._production_boundary_static_scan_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["scan_status"] == WARN_STATUS
    assert summary["validation_status"] == WARN_STATUS
    assert summary["warning_finding_count"] == 1


def _write_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
