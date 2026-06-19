from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.artifact_lifecycle_inventory import (
    build_artifact_lifecycle_inventory_payload,
    validate_artifact_lifecycle_inventory_payload,
)

RUN_DATE = date(2026, 6, 19)


def test_artifact_lifecycle_inventory_tracks_superseded_without_blocking(
    tmp_path: Path,
) -> None:
    registry_path = _write_registry(tmp_path, ("current_report",))
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(reports_dir / "current_report_2026-06-18.json", {"status": "OLD"})
    _write_json(reports_dir / "current_report_2026-06-19.json", {"status": "PASS"})

    payload = build_artifact_lifecycle_inventory_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=None,
    )
    validation = validate_artifact_lifecycle_inventory_payload(payload)

    assert payload["inventory_status"] == "ARTIFACT_LIFECYCLE_READY_WITH_LIMITATIONS"
    assert payload["summary"]["current_count"] == 1
    assert payload["summary"]["superseded_artifact_count"] == 1
    assert payload["summary"]["report_index_unwaived_issue_count"] == 0
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"


def test_artifact_lifecycle_inventory_blocks_unwaived_missing_report(
    tmp_path: Path,
) -> None:
    registry_path = _write_registry(tmp_path, ("missing_report",))

    payload = build_artifact_lifecycle_inventory_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        registry_path=registry_path,
        waiver_path=None,
    )
    validation = validate_artifact_lifecycle_inventory_payload(payload)

    assert payload["inventory_status"] == "ARTIFACT_LIFECYCLE_BLOCKED"
    assert payload["summary"]["invalid_count"] == 1
    assert payload["summary"]["report_index_unwaived_issue_count"] == 1
    assert validation["validation_status"] == "FAIL"


def test_artifact_lifecycle_inventory_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    registry_path = _write_registry(tmp_path, ("current_report",))
    waiver_path = _write_waiver_policy(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    _write_json(reports_dir / "current_report_2026-06-19.json", {"status": "PASS"})
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "artifact-lifecycle-inventory",
            "--as-of",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--registry-path",
            str(registry_path),
            "--waiver-path",
            str(waiver_path),
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    inventory_json = reports_dir / "artifact_lifecycle_inventory_2026-06-19.json"
    assert inventory_json.exists()

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-artifact-lifecycle-inventory",
            "--source-json-path",
            str(inventory_json),
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert validation.exit_code == 0, validation.output
    validation_json = reports_dir / "artifact_lifecycle_inventory_validation_2026-06-19.json"
    assert validation_json.exists()
    assert json.loads(validation_json.read_text(encoding="utf-8"))["validation_status"] == "PASS"


def _write_registry(tmp_path: Path, report_ids: tuple[str, ...]) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    registry = {
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
        "reports": [
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
            for report_id in report_ids
        ],
    }
    registry_path = config_dir / "report_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    return registry_path


def _write_waiver_policy(tmp_path: Path) -> Path:
    waiver_path = tmp_path / "config" / "report_index_visibility_waivers.yaml"
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
    return waiver_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
