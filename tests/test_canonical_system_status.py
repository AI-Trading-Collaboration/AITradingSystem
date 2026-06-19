from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.canonical_system_status import (
    build_canonical_system_status_payload,
    validate_canonical_system_status_payload,
)

RUN_DATE = date(2026, 6, 19)


def test_canonical_system_status_first_screen_and_doctor_contract(tmp_path: Path) -> None:
    registry_path, waiver_path = _write_project(tmp_path)

    payload = build_canonical_system_status_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    doctor = validate_canonical_system_status_payload(payload)

    assert payload["report_type"] == "canonical_system_status"
    assert payload["production_effect"] == "none"
    assert payload["first_screen"]["latest_research_gate"] == "V2_RETURN_TO_HYPOTHESIS_BACKLOG"
    assert payload["first_screen"]["data_health"] == "PASS"
    assert payload["first_screen"]["validation_health"] == "PASS"
    assert doctor["validation_status"] == "PASS"
    assert {item["workflow_id"] for item in payload["canonical_workflows"]} >= {
        "system_doctor",
        "system_status",
        "research_status",
        "reports_latest",
        "artifact_reproduce",
    }


def test_canonical_system_cli_writes_status_and_doctor(tmp_path: Path) -> None:
    registry_path, waiver_path = _write_project(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    runner = CliRunner()

    status = runner.invoke(
        app,
        [
            "system",
            "status",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--registry-path",
            str(registry_path),
            "--waiver-path",
            str(waiver_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert status.exit_code == 0, status.output

    doctor = runner.invoke(
        app,
        [
            "system",
            "doctor",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--registry-path",
            str(registry_path),
            "--waiver-path",
            str(waiver_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert doctor.exit_code == 0, doctor.output
    status_json = reports_dir / "canonical_system_status_2026-06-19.json"
    doctor_json = reports_dir / "canonical_system_doctor_2026-06-19.json"
    assert status_json.exists()
    assert doctor_json.exists()
    assert json.loads(doctor_json.read_text(encoding="utf-8"))["validation_status"] == "PASS"


def test_reports_latest_resolves_current_artifact(tmp_path: Path) -> None:
    registry_path, waiver_path = _write_project(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "latest",
            "--report-id",
            "candidate_v2_research_cycle_snapshot",
            "--as-of",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--registry-path",
            str(registry_path),
            "--waiver-path",
            str(waiver_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Report latest：FOUND" in result.output
    assert "candidate_v2_research_cycle_snapshot_2026-06-19.json" in result.output


def _write_project(tmp_path: Path) -> tuple[Path, Path]:
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_json(
        reports_dir / "data_quality_2026-06-19.json",
        {"schema_version": 1, "report_type": "data_quality", "status": "PASS"},
    )
    _write_json(
        reports_dir / "engineering_surface_inventory_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "engineering_surface_inventory",
            "status": "ENGINEERING_SURFACE_INVENTORY_READY",
            "production_effect": "none",
            "summary": {"warning_count": 0},
        },
    )
    _write_json(
        reports_dir / "reader_brief_consistency_validation_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "reader_brief_consistency_validation",
            "status": "PASS",
            "validation_status": "PASS",
            "production_effect": "none",
            "summary": {"warning_check_count": 0},
        },
    )
    _write_json(
        reports_dir / "candidate_v2_research_cycle_snapshot_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "candidate_v2_research_cycle_snapshot",
            "status": "V2_RESEARCH_CYCLE_RETURN_TO_BACKLOG",
            "production_effect": "none",
            "summary": {
                "candidate_id": "candidate_test",
                "source_research_gate_decision": "V2_RETURN_TO_HYPOTHESIS_BACKLOG",
            },
            "reader_brief": {"next_action": "revise_v2_hypothesis_after_owner_review"},
        },
    )
    _write_json(
        reports_dir / "candidate_v2_research_gate_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "candidate_v2_research_gate",
            "status": "V2_RETURN_TO_HYPOTHESIS_BACKLOG",
            "production_effect": "none",
            "summary": {"candidate_id": "candidate_test"},
        },
    )
    registry_path = config_dir / "report_registry.yaml"
    registry_path.write_text(
        yaml.safe_dump(_registry(), sort_keys=False),
        encoding="utf-8",
    )
    waiver_path = config_dir / "report_index_visibility_waivers.yaml"
    waiver_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "policy_id": "test_waivers",
                "policy_metadata": {"owner": "test", "status": "test"},
                "waivers": [],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return registry_path, waiver_path


def _registry() -> dict[str, Any]:
    base = {
        "schema_version": 1,
        "policy_version": "test_report_registry_v1",
        "policy_metadata": {
            "owner": "test",
            "status": "test",
            "rationale": "test",
            "intended_effect": "test",
            "validation_evidence": "test",
            "review_condition": "test",
        },
        "defaults": {
            "production_effect": "none",
            "missing_status": "MISSING",
            "stale_status": "STALE",
        },
        "reports": [],
    }
    for report_id in (
        "data_quality",
        "engineering_surface_inventory",
        "reader_brief_consistency_validation",
        "candidate_v2_research_cycle_snapshot",
        "candidate_v2_research_gate",
        "canonical_system_status",
        "canonical_system_doctor",
    ):
        base["reports"].append(
            {
                "report_id": report_id,
                "title": report_id,
                "group": "governance",
                "cadence": "daily",
                "audience": "test",
                "owner": "system",
                "command": f"aits reports latest --report-id {report_id}",
                "artifact_globs": [f"outputs/reports/{report_id}_*.json"],
                "freshness_sla_days": 1,
                "freshness_rationale": "test",
                "owner_action": "review",
                "include_in_reader_brief": True,
                "include_in_daily_task_dashboard": False,
                "required_for_daily_reading": False,
            }
        )
    return base


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
