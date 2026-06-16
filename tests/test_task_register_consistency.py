from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.reports.task_register_consistency import (
    build_task_register_consistency_payload,
    render_task_register_consistency_markdown,
    validate_task_register_consistency_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_task_register_consistency_passes_minimal_project(tmp_path: Path) -> None:
    registry_path = _write_project(tmp_path)

    payload = build_task_register_consistency_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_registry_path=registry_path,
    )
    validation = validate_task_register_consistency_payload(payload)
    markdown = render_task_register_consistency_markdown(payload)

    assert payload["consistency_status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["summary"]["active_task_count"] == 1
    assert payload["summary"]["completed_task_count"] == 1
    assert payload["reader_brief"]["key_result"] == "PASS"
    assert validation["validation_status"] == "PASS"
    assert "Task Register Consistency" in markdown


def test_task_register_consistency_blocks_terminal_active_task(tmp_path: Path) -> None:
    registry_path = _write_project(tmp_path, active_status="DONE")

    payload = build_task_register_consistency_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_registry_path=registry_path,
    )

    issue_ids = {issue["issue_id"] for issue in payload["blocking_issues"]}
    assert payload["consistency_status"] == "FAIL"
    assert "completed_tasks_not_active" in issue_ids
    assert "archived_completed_tasks_not_missing" in issue_ids


def test_task_register_consistency_allows_multi_segment_task_ids(tmp_path: Path) -> None:
    registry_path = _write_project(tmp_path, active_task_id="DOCS-LEARN-001")

    payload = build_task_register_consistency_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_registry_path=registry_path,
    )

    assert payload["consistency_status"] == "PASS"
    assert payload["task_registers"]["active"][0]["task_id"] == "DOCS-LEARN-001"


def test_task_register_consistency_allows_range_task_ids(tmp_path: Path) -> None:
    registry_path = _write_project(tmp_path, active_task_id="TRADING-336_to_345_SIGNAL_EVIDENCE")

    payload = build_task_register_consistency_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_registry_path=registry_path,
    )

    assert payload["consistency_status"] == "PASS"
    assert payload["task_registers"]["active"][0]["task_id"].startswith("TRADING-336")


def test_task_register_consistency_cli_writes_report_and_validation(tmp_path: Path) -> None:
    registry_path = _write_project(tmp_path)
    reports_dir = tmp_path / "outputs" / "reports"
    consistency_json = reports_dir / "task_register_consistency_2026-05-04.json"
    consistency_md = reports_dir / "task_register_consistency_2026-05-04.md"
    validation_json = reports_dir / "task_register_consistency_validation_2026-05-04.json"
    validation_md = reports_dir / "task_register_consistency_validation_2026-05-04.md"

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "task-register-consistency",
            "run",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--registry-path",
            str(registry_path),
        ],
    )
    assert run.exit_code == 0, run.output

    report = runner.invoke(
        app,
        [
            "reports",
            "task-register-consistency",
            "report",
            "--source-json-path",
            str(consistency_json),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert report.exit_code == 0, report.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "task-register-consistency",
            "validate",
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
    assert validation_payload["input_artifacts"]["task_register_consistency"] == str(
        consistency_json
    )


def test_task_register_consistency_registry_and_reader_brief_summary(
    tmp_path: Path,
) -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    report_ids = {item["report_id"] for item in registry["reports"]}
    assert "task_register_consistency" in report_ids
    assert "task_register_consistency_validation" in report_ids

    consistency_path = tmp_path / "task_register_consistency_2026-05-04.json"
    validation_path = tmp_path / "task_register_consistency_validation_2026-05-04.json"
    _write_json(
        consistency_path,
        {
            "report_type": "task_register_consistency",
            "consistency_status": "PASS_WITH_WARNINGS",
            "status": "PASS_WITH_WARNINGS",
            "production_effect": "none",
            "next_action": "review_task_register_consistency_warnings_before_next_governance_cycle",
            "summary": {
                "active_task_count": 4,
                "completed_task_count": 12,
                "check_count": 8,
                "failed_check_count": 0,
                "blocking_issue_count": 0,
                "warning_issue_count": 1,
                "explicit_docs_link_count": 7,
            },
        },
    )
    _write_json(
        validation_path,
        {
            "report_type": "task_register_consistency_validation",
            "validation_status": "PASS_WITH_WARNINGS",
            "status": "PASS_WITH_WARNINGS",
            "production_effect": "none",
        },
    )

    summary = reader_brief._task_register_consistency_summary(
        {
            "reports": [
                {
                    "report_id": "task_register_consistency",
                    "latest_artifact_path": str(consistency_path),
                    "exists": True,
                },
                {
                    "report_id": "task_register_consistency_validation",
                    "latest_artifact_path": str(validation_path),
                    "exists": True,
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["consistency_status"] == "PASS_WITH_WARNINGS"
    assert summary["validation_status"] == "PASS_WITH_WARNINGS"
    assert summary["active_task_count"] == 4
    assert summary["detail_report"] == str(consistency_path)


def _write_project(
    tmp_path: Path,
    *,
    active_status: str = "IN_PROGRESS",
    active_task_id: str = "TRADING-999_TEST",
) -> Path:
    docs_dir = tmp_path / "docs"
    requirements_dir = docs_dir / "requirements"
    requirements_dir.mkdir(parents=True)
    (requirements_dir / "TRADING-999_Test.md").write_text(
        "# TRADING-999 Test\n",
        encoding="utf-8",
    )
    (requirements_dir / "TRADING-998_Done.md").write_text(
        "# TRADING-998 Done\n",
        encoding="utf-8",
    )
    (docs_dir / "task_register.md").write_text(
        "|ID|Area|Priority|Status|Next Owner|Blocker / Next Step|Acceptance Criteria|Notes|\n"
        "|---|---|---|---|---|---|---|---|\n"
        f"|{active_task_id}|Governance|P1|{active_status}|system|"
        "See `docs/requirements/TRADING-999_Test.md`|CLI pass|2026-05-04|\n",
        encoding="utf-8",
    )
    (docs_dir / "task_register_completed.md").write_text(
        "|ID|Area|Priority|Status|Next Owner|Blocker / Next Step|Acceptance Criteria|Notes|\n"
        "|---|---|---|---|---|---|---|---|\n"
        "|TRADING-998_DONE|Governance|P1|DONE|system|"
        "See `docs/requirements/TRADING-998_Done.md`|Archived|2026-05-04|\n",
        encoding="utf-8",
    )
    (docs_dir / "artifact_catalog.md").write_text(
        "# Artifact Catalog\n\n"
        "|Artifact|Generator|Notes|\n"
        "|---|---|---|\n"
        "|task_register_consistency|aits reports task-register-consistency run|read-only|\n"
        "|task_register_consistency_validation|"
        "aits reports task-register-consistency validate|read-only|\n",
        encoding="utf-8",
    )
    return _write_registry(tmp_path)


def _write_registry(tmp_path: Path) -> Path:
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
            _registry_entry(
                "task_register_consistency",
                "Task Register Consistency",
                "outputs/reports/task_register_consistency_*.json",
            ),
            _registry_entry(
                "task_register_consistency_validation",
                "Task Register Consistency Validation",
                "outputs/reports/task_register_consistency_validation_*.json",
            ),
            _registry_entry(
                "reader_brief",
                "Reader Brief",
                "outputs/reports/reader_brief_*.json",
            ),
        ],
    }
    path = tmp_path / "config" / "report_registry.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    return path


def _registry_entry(report_id: str, title: str, artifact_glob: str) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "group": "governance",
        "cadence": "daily",
        "audience": "project_owner",
        "owner": "system",
        "command": "test command",
        "artifact_globs": [artifact_glob],
        "freshness_sla_days": 1,
        "freshness_rationale": "test rationale",
        "owner_action": "test_action",
        "include_in_reader_brief": True,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
