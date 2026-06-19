from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.engineering_release_candidate import (
    build_engineering_closeout_release_candidate_payload,
    render_engineering_closeout_release_candidate_markdown,
    validate_engineering_closeout_release_candidate_payload,
)

RUN_DATE = date(2026, 6, 19)
RELEASE_TIERS = ("fast-unit", "contract-validation", "report-validation", "reproducibility")


def test_release_candidate_ready_when_all_closeout_evidence_passes(tmp_path: Path) -> None:
    reports_dir, runtime_dir = _write_release_evidence(tmp_path, clean_clone_status="PASS")

    payload = build_engineering_closeout_release_candidate_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        reports_dir=reports_dir,
        validation_runtime_dir=runtime_dir,
        enforce_git_clean=False,
    )
    validation = validate_engineering_closeout_release_candidate_payload(payload)
    markdown = render_engineering_closeout_release_candidate_markdown(payload)

    assert payload["closeout_status"] == "ENGINEERING_CLOSEOUT_READY"
    assert validation["validation_status"] == "PASS"
    assert payload["summary"]["blocking_issue_count"] == 0
    assert payload["release_metadata"]["release_tag"] == "engineering-closeout-2026-06-19-rc1"
    assert "## Changelog" in markdown
    assert "## Compatibility Policy" in markdown
    assert "official target weights" in markdown


def test_release_candidate_blocks_when_clean_clone_is_not_pass(tmp_path: Path) -> None:
    reports_dir, runtime_dir = _write_release_evidence(
        tmp_path,
        clean_clone_status="CLEAN_CLONE_ACCEPTANCE_BLOCKED_UNCOMMITTED_CHANGES",
    )

    payload = build_engineering_closeout_release_candidate_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        reports_dir=reports_dir,
        validation_runtime_dir=runtime_dir,
        enforce_git_clean=False,
    )
    validation = validate_engineering_closeout_release_candidate_payload(payload)

    assert payload["closeout_status"] == "ENGINEERING_CLOSEOUT_BLOCKED"
    assert validation["validation_status"] == "FAIL"
    issue_ids = {issue["issue_id"] for issue in payload["blocking_issues"]}
    assert "clean_clone_minimal_e2e_pass" in issue_ids


def test_release_candidate_cli_writes_report_and_validation(tmp_path: Path) -> None:
    reports_dir, runtime_dir = _write_release_evidence(tmp_path, clean_clone_status="PASS")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "engineering-closeout-release-candidate",
            "--as-of",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--reports-dir",
            str(reports_dir),
            "--validation-runtime-dir",
            str(runtime_dir),
            "--allow-dirty-worktree",
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    report_json = reports_dir / "engineering_closeout_release_candidate_2026-06-19.json"
    assert report_json.exists()

    validation = runner.invoke(
        app,
        [
            "reports",
            "validate-engineering-closeout-release-candidate",
            "--source-json-path",
            str(report_json),
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert validation.exit_code == 0, validation.output
    validation_json = (
        reports_dir / "engineering_closeout_release_candidate_validation_2026-06-19.json"
    )
    assert validation_json.exists()
    payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert payload["validation_status"] == "PASS"


def _write_release_evidence(tmp_path: Path, *, clean_clone_status: str) -> tuple[Path, Path]:
    reports_dir = tmp_path / "outputs" / "reports"
    runtime_dir = tmp_path / "outputs" / "validation_runtime"
    _write_json(
        reports_dir / "canonical_system_status_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "canonical_system_status",
            "status": "ENGINEERING_CONTROL_PLANE_READY",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "canonical_system_doctor_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "canonical_system_doctor",
            "status": "PASS",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "report_index_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "report_index",
            "status": "PASS",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "engineering_stage_b_readiness_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "engineering_stage_b_readiness",
            "readiness_status": "ENGINEERING_STAGE_B_READY",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "engineering_stage_b_readiness_validation_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "engineering_stage_b_readiness_validation",
            "validation_status": "PASS",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "reader_brief_consistency_validation_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "reader_brief_consistency_validation",
            "validation_status": "PASS",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "task_register_consistency_validation_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "task_register_consistency_validation",
            "validation_status": "PASS",
            "production_effect": "none",
        },
    )
    _write_json(
        reports_dir / "clean_clone_release_acceptance_2026-06-19.json",
        {
            "schema_version": 1,
            "report_type": "clean_clone_release_acceptance",
            "release_acceptance_status": (
                "CLEAN_CLONE_ACCEPTANCE_PASS"
                if clean_clone_status == "PASS"
                else clean_clone_status
            ),
            "production_effect": "none",
        },
    )
    for tier in RELEASE_TIERS:
        _write_json(
            runtime_dir / f"{tier}_20260619T000000Z" / "test_runtime_summary.json",
            {
                "schema_version": 1,
                "report_type": "test_runtime_summary",
                "resolved_tier": tier,
                "status": "PASS",
                "promotion_blocking": True,
                "production_effect": "none",
            },
        )
    return reports_dir, runtime_dir


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
