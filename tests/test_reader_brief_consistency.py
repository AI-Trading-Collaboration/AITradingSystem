from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.reader_brief_consistency import (
    build_reader_brief_consistency_payload,
    render_reader_brief_consistency_markdown,
    validate_reader_brief_consistency_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_reader_brief_consistency_passes_standard_sections(tmp_path: Path) -> None:
    report_index_path = _write_report_index(tmp_path, reports_dir=tmp_path)

    payload = build_reader_brief_consistency_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        project_root=tmp_path,
    )
    validation = validate_reader_brief_consistency_payload(payload)
    markdown = render_reader_brief_consistency_markdown(payload)

    assert payload["consistency_status"] == "PASS"
    assert payload["summary"]["checked_report_count"] == 2
    assert payload["summary"]["missing_section_count"] == 0
    assert validation["validation_status"] == "PASS"
    assert "Reader Brief Consistency Pack" in markdown


def test_reader_brief_consistency_warns_for_legacy_section_gap(tmp_path: Path) -> None:
    legacy_path = tmp_path / "legacy_report_2026-05-04.json"
    _write_json(
        legacy_path,
        {
            "schema_version": 1,
            "report_type": "legacy_report",
            "as_of": RUN_DATE.isoformat(),
            "status": "PASS",
            "production_effect": "none",
            "next_action": "continue_observation",
        },
    )
    report_index_path = _write_report_index(
        tmp_path,
        reports_dir=tmp_path,
        extra_reports=[
            {
                "report_id": "legacy_report",
                "title": "Legacy Report",
                "include_in_reader_brief": True,
                "exists": True,
                "latest_artifact_path": str(legacy_path),
                "artifact_production_effect": "none",
            }
        ],
    )

    payload = build_reader_brief_consistency_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        project_root=tmp_path,
    )
    validation = validate_reader_brief_consistency_payload(payload)

    assert payload["consistency_status"] == "PASS_WITH_WARNINGS"
    assert payload["summary"]["missing_section_count"] > 0
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"
    assert validation["summary"]["source_checked_report_count"] == 3
    assert "3 Reader Brief-facing reports" in validation["reader_brief"]["summary"]


def test_reader_brief_consistency_blocks_missing_daily_next_action(tmp_path: Path) -> None:
    report_index_path = _write_report_index(
        tmp_path,
        reports_dir=tmp_path,
        reader_overrides={
            "action_checklist": [],
            "executive_decision": {"not_trade_instruction": True},
        },
    )

    payload = build_reader_brief_consistency_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        project_root=tmp_path,
    )
    validation = validate_reader_brief_consistency_payload(payload)

    assert payload["consistency_status"] == "FAIL"
    assert any(
        issue["report_id"] == "reader_brief" and issue["section"] == "next_action"
        for issue in payload["blocking_issues"]
    )
    assert validation["validation_status"] == "FAIL"


def test_reader_brief_consistency_cli_writes_report_and_validation(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index_path = _write_report_index(tmp_path, reports_dir=reports_dir)
    consistency_json = reports_dir / "reader_brief_consistency_pack_2026-05-04.json"
    consistency_md = reports_dir / "reader_brief_consistency_pack_2026-05-04.md"
    validation_json = reports_dir / "reader_brief_consistency_validation_2026-05-04.json"
    validation_md = reports_dir / "reader_brief_consistency_validation_2026-05-04.md"

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "reader-brief-consistency",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-reader-brief-consistency",
            "--source-json-path",
            str(consistency_json),
            "--reports-dir",
            str(reports_dir),
        ],
    )

    assert validation.exit_code == 0, validation.output
    assert consistency_md.exists()
    assert validation_md.exists()
    assert json.loads(consistency_json.read_text(encoding="utf-8"))["status"] == "PASS"
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation_payload["validation_status"] == "PASS"
    assert validation_payload["input_artifacts"]["reader_brief_consistency_pack"] == str(
        consistency_json
    )


def _write_report_index(
    tmp_path: Path,
    *,
    reports_dir: Path,
    reader_overrides: dict[str, object] | None = None,
    extra_reports: list[dict[str, object]] | None = None,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    reader_path = reports_dir / "reader_brief_2026-05-04.json"
    governance_path = reports_dir / "governance_2026-05-04.json"
    reader_payload = _reader_brief_payload()
    if reader_overrides:
        reader_payload.update(reader_overrides)
    _write_json(reader_path, reader_payload)
    _write_json(
        governance_path,
        {
            "schema_version": 1,
            "report_type": "governance_report",
            "as_of": RUN_DATE.isoformat(),
            "status": "PASS",
            "production_effect": "none",
            "reader_brief": {
                "summary": "Governance report is complete.",
                "key_result": "PASS",
                "blocking_issues": "none",
                "warnings": "none",
                "safety_boundary": "production_effect=none; no broker/order.",
                "next_action": "continue_monitoring",
            },
        },
    )
    reports: list[dict[str, object]] = [
        {
            "report_id": "reader_brief",
            "title": "Reader Brief",
            "include_in_reader_brief": True,
            "exists": True,
            "latest_artifact_path": str(reader_path),
            "artifact_production_effect": "none",
        },
        {
            "report_id": "governance_report",
            "title": "Governance Report",
            "include_in_reader_brief": True,
            "exists": True,
            "latest_artifact_path": str(governance_path),
            "artifact_production_effect": "none",
        },
    ]
    reports.extend(extra_reports or [])
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    _write_json(
        report_index_path,
        {
            "schema_version": 1,
            "report_type": "report_index",
            "as_of": RUN_DATE.isoformat(),
            "status": "PASS",
            "production_effect": "none",
            "reports": reports,
        },
    )
    return report_index_path


def _reader_brief_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "reader_brief",
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS",
        "production_effect": "none",
        "narrative_executive_summary": {
            "today_conclusion": "Daily context is usable.",
            "why": "All required reading artifacts are available.",
        },
        "status_panel": {
            "build_status": "PASS",
            "decision_usability": "OK",
            "research_promotion_status": "NOT_PROMOTABLE",
        },
        "executive_decision": {
            "action": "manual_review",
            "recommended_action": "review_reader_brief",
            "not_trade_instruction": True,
        },
        "missing_limited_artifact_impact": {"status": "OK", "blocking_count": 0},
        "manual_review_queue": {"items": [], "groups": []},
        "action_checklist": [{"action": "review_reader_brief"}],
        "warnings": [],
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
