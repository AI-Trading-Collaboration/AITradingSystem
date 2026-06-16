from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.research_safety_boundary import (
    PASS_STATUS,
    WARN_STATUS,
    build_research_safety_boundary_payload,
    render_research_safety_boundary_markdown,
    validate_research_safety_boundary_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_research_safety_boundary_passes_safe_metadata(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    artifact_path = reports_dir / "safe_report_2026-05-04.json"
    _write_json(artifact_path, _safe_artifact_payload())
    report_index_path = _write_report_index(tmp_path, reports_dir, artifact_path)
    task_register, completed_register = _write_registers(tmp_path)

    payload = build_research_safety_boundary_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        task_register_path=task_register,
        completed_task_register_path=completed_register,
        project_root=tmp_path,
    )
    validation = validate_research_safety_boundary_payload(payload)
    markdown = render_research_safety_boundary_markdown(payload)

    assert payload["safety_status"] == PASS_STATUS
    assert payload["summary"]["unsafe_signal_count"] == 0
    assert payload["summary"]["missing_metadata_count"] == 0
    assert validation["validation_status"] == PASS_STATUS
    assert "Research Safety Boundary Audit" in markdown


def test_research_safety_boundary_warns_for_legacy_missing_metadata(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    artifact_path = reports_dir / "legacy_report_2026-05-04.json"
    _write_json(
        artifact_path,
        {
            "schema_version": 1,
            "report_type": "legacy_report",
            "as_of": RUN_DATE.isoformat(),
            "status": "PASS",
            "production_effect": "none",
        },
    )
    report_index_path = _write_report_index(tmp_path, reports_dir, artifact_path)
    task_register, completed_register = _write_registers(tmp_path)

    payload = build_research_safety_boundary_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        task_register_path=task_register,
        completed_task_register_path=completed_register,
        project_root=tmp_path,
    )
    validation = validate_research_safety_boundary_payload(payload)

    assert payload["safety_status"] == WARN_STATUS
    assert payload["summary"]["missing_metadata_count"] > 0
    assert payload["summary"]["unsafe_signal_count"] == 0
    assert validation["validation_status"] == WARN_STATUS
    assert validation["summary"]["failed_check_count"] == 0
    assert validation["summary"]["warning_check_count"] > 0


def test_research_safety_boundary_blocks_unsafe_artifact_metadata(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    artifact_path = reports_dir / "unsafe_report_2026-05-04.json"
    unsafe_payload = _safe_artifact_payload()
    unsafe_payload["safety_boundary"]["official_target_weights"] = True
    unsafe_payload["safety_boundary"].pop("not_official_target_weights")
    _write_json(artifact_path, unsafe_payload)
    report_index_path = _write_report_index(tmp_path, reports_dir, artifact_path)
    task_register, completed_register = _write_registers(tmp_path)

    payload = build_research_safety_boundary_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        task_register_path=task_register,
        completed_task_register_path=completed_register,
        project_root=tmp_path,
    )
    validation = validate_research_safety_boundary_payload(payload)

    assert payload["safety_status"] == "SAFETY_BLOCKED"
    assert any(
        issue["issue_id"] == "unsafe_official_target_weights"
        for issue in payload["blocking_issues"]
    )
    assert validation["validation_status"] == "SAFETY_BLOCKED"


def test_research_safety_boundary_blocks_unsafe_task_scope(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    artifact_path = reports_dir / "safe_report_2026-05-04.json"
    _write_json(artifact_path, _safe_artifact_payload())
    report_index_path = _write_report_index(tmp_path, reports_dir, artifact_path)
    task_register, completed_register = _write_registers(
        tmp_path,
        active_row=(
            "|TRADING-X|research|P1|IN_PROGRESS|system|write official target weights|"
            "creates production mutation|unsafe|"
        ),
    )

    payload = build_research_safety_boundary_payload(
        as_of=RUN_DATE,
        report_index_path=report_index_path,
        task_register_path=task_register,
        completed_task_register_path=completed_register,
        project_root=tmp_path,
    )

    assert payload["safety_status"] == "SAFETY_BLOCKED"
    assert any(
        issue["issue_id"] == "unsafe_task_boundary_official_target_weights"
        for issue in payload["blocking_issues"]
    )


def test_research_safety_boundary_cli_writes_report_and_validation(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    artifact_path = reports_dir / "safe_report_2026-05-04.json"
    _write_json(artifact_path, _safe_artifact_payload())
    report_index_path = _write_report_index(tmp_path, reports_dir, artifact_path)
    task_register, completed_register = _write_registers(tmp_path)
    audit_json = reports_dir / "research_safety_boundary_audit_2026-05-04.json"
    audit_md = reports_dir / "research_safety_boundary_audit_2026-05-04.md"
    validation_json = reports_dir / "research_safety_boundary_validation_2026-05-04.json"
    validation_md = reports_dir / "research_safety_boundary_validation_2026-05-04.md"

    runner = CliRunner()
    run = runner.invoke(
        app,
        [
            "reports",
            "research-safety-boundary-audit",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--task-register-path",
            str(task_register),
            "--completed-task-register-path",
            str(completed_register),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert run.exit_code == 0, run.output

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-research-safety-boundary",
            "--source-json-path",
            str(audit_json),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation.exit_code == 0, validation.output
    assert audit_md.exists()
    assert validation_md.exists()
    assert json.loads(audit_json.read_text(encoding="utf-8"))["safety_status"] == PASS_STATUS
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"]["research_safety_boundary_audit"] == str(
        audit_json
    )


def _safe_artifact_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "safe_report",
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS",
        "production_effect": "none",
        "output_decision": "PASS",
        "safety_boundary": {
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "order_ticket_generated": False,
            "manual_review_only": True,
            "not_official_target_weights": True,
            "production_state_mutated": False,
        },
        "reader_brief": {
            "summary": "Safe report.",
            "key_result": "PASS",
            "blocking_issues": "none",
            "warnings": "none",
            "safety_boundary": "production_effect=none; no broker/order.",
            "next_action": "continue_monitoring",
        },
    }


def _write_report_index(
    tmp_path: Path,
    reports_dir: Path,
    artifact_path: Path,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    _write_json(
        report_index_path,
        {
            "schema_version": 1,
            "report_type": "report_index",
            "as_of": RUN_DATE.isoformat(),
            "status": "PASS",
            "production_effect": "none",
            "reports": [
                {
                    "report_id": "safe_report",
                    "title": "Safe Report",
                    "include_in_reader_brief": True,
                    "exists": True,
                    "latest_artifact_path": str(artifact_path.relative_to(tmp_path)),
                    "artifact_production_effect": "none",
                }
            ],
        },
    )
    return report_index_path


def _write_registers(
    tmp_path: Path,
    *,
    active_row: str | None = None,
) -> tuple[Path, Path]:
    header = (
        "|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|\n"
        "|---|---|---|---|---|---|---|---|\n"
    )
    active = tmp_path / "task_register.md"
    completed = tmp_path / "task_register_completed.md"
    safe_row = (
        "|TRADING-SAFE|research|P1|IN_PROGRESS|system|不得写 official target weights、"
        "broker action、order ticket 或 production mutation|read-only audit|safe|"
    )
    active.write_text(header + (active_row or safe_row) + "\n", encoding="utf-8")
    completed.write_text(header, encoding="utf-8")
    return active, completed


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
