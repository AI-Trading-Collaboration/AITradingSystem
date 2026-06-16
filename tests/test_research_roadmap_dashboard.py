from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.research_roadmap_dashboard import (
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    ROADMAP_BLOCKED,
    ROADMAP_HEALTHY,
    build_research_roadmap_dashboard_payload,
    validate_research_roadmap_dashboard_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_research_roadmap_dashboard_healthy_with_complete_sources(
    tmp_path: Path,
) -> None:
    task_register, completed_register = _task_registers(tmp_path)

    payload = build_research_roadmap_dashboard_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        task_register_path=task_register,
        completed_register_path=completed_register,
        project_root=tmp_path,
    )
    validation = validate_research_roadmap_dashboard_payload(payload)

    assert payload["dashboard_status"] == ROADMAP_HEALTHY
    assert payload["summary"]["active_task_count"] == 1
    assert payload["summary"]["completed_task_count"] == 1
    assert payload["summary"]["open_blocker_count"] == 0
    assert payload["summary"]["paper_shadow_status"] == "CONTINUE_NORMAL_SHADOW"
    assert validation["validation_status"] == PASS_STATUS


def test_research_roadmap_dashboard_blocks_visible_roadmap_risks(
    tmp_path: Path,
) -> None:
    task_register, completed_register = _task_registers(tmp_path, blocked=True)
    report_index = _report_index_payload(
        tmp_path,
        summary_overrides={
            "stale_count": 1,
            "unwaived_warning_count": 1,
        },
        report_overrides={
            "artifact_lineage_graph": {
                "freshness_status": "STALE",
                "owner_action": "repair_lineage",
            },
        },
        payload_overrides={
            "research_monthly_review_pack": {
                "summary": {
                    "active_candidate_count": 2,
                    "paper_shadow_candidate_count": 1,
                    "needs_evidence_candidate_count": 1,
                    "major_blocker_count": 1,
                    "major_warning_count": 0,
                    "data_governance_status": "BLOCKED",
                },
            },
            "paper_shadow_promotion_board": {
                "status": "HOLD_FOR_MORE_DATA",
                "summary": {
                    "board_decision": "HOLD_FOR_MORE_DATA",
                    "blocked_evidence_count": 2,
                    "warning_evidence_count": 0,
                },
            },
            "extended_shadow_protocol": {
                "status": "EXTENDED_SHADOW_BLOCKED",
                "summary": {
                    "eligibility_status": "EXTENDED_SHADOW_BLOCKED",
                    "blocked_check_count": 1,
                    "warning_check_count": 0,
                },
            },
            "artifact_lineage_graph": {
                "summary": {
                    "lineage_status": "FAIL",
                    "blocking_issue_count": 1,
                    "warning_issue_count": 0,
                },
            },
        },
    )

    payload = build_research_roadmap_dashboard_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        task_register_path=task_register,
        completed_register_path=completed_register,
        project_root=tmp_path,
    )
    validation = validate_research_roadmap_dashboard_payload(payload)

    assert payload["dashboard_status"] == ROADMAP_BLOCKED
    assert payload["summary"]["open_blocker_count"] >= 5
    assert payload["summary"]["stale_artifact_count"] == 1
    assert any(
        blocker["blocker_id"] == "promotion_board_hold_for_more_data"
        for blocker in payload["open_blockers"]
    )
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_research_roadmap_dashboard_validation_fails_unsafe_boundary(
    tmp_path: Path,
) -> None:
    task_register, completed_register = _task_registers(tmp_path)
    payload = build_research_roadmap_dashboard_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        task_register_path=task_register,
        completed_register_path=completed_register,
        project_root=tmp_path,
    )
    payload["production_effect"] = "writes_task_state"

    validation = validate_research_roadmap_dashboard_payload(payload)

    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "production_effect_none"
        for issue in validation["blocking_issues"]
    )


def test_research_roadmap_dashboard_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    task_register, completed_register = _task_registers(tmp_path)
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "research-roadmap-dashboard",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--task-register-path",
            str(task_register),
            "--completed-register-path",
            str(completed_register),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-research-roadmap-dashboard",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "research_roadmap_dashboard_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir / "research_roadmap_dashboard_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["dashboard_status"] == ROADMAP_HEALTHY
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"]["research_roadmap_dashboard"].endswith(
        "research_roadmap_dashboard_2026-05-04.json"
    )


def test_reader_brief_research_roadmap_dashboard_summary(tmp_path: Path) -> None:
    task_register, completed_register = _task_registers(tmp_path)
    payload = build_research_roadmap_dashboard_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        task_register_path=task_register,
        completed_register_path=completed_register,
        project_root=tmp_path,
    )
    validation = validate_research_roadmap_dashboard_payload(payload)
    report_path = tmp_path / "research_roadmap_dashboard_2026-05-04.json"
    validation_path = tmp_path / "research_roadmap_dashboard_validation_2026-05-04.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._research_roadmap_dashboard_summary(
        {
            "reports": [
                {
                    "report_id": "research_roadmap_dashboard",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "research_roadmap_dashboard_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["dashboard_status"] == ROADMAP_HEALTHY
    assert summary["validation_status"] == PASS_STATUS
    assert summary["top_next_task"] == "TRADING-TEST_ACTIVE"
    assert summary["production_effect"] == "none"


def _task_registers(tmp_path: Path, *, blocked: bool = False) -> tuple[Path, Path]:
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    task_register = docs_dir / "task_register.md"
    active_status = "BLOCKED_OWNER" if blocked else "IN_PROGRESS"
    task_register.write_text(
        "\n".join(
            [
                "| Task ID | Domain | Priority | Status | Summary |",
                "|---|---|---|---|---|",
                (
                    "| TRADING-TEST_ACTIVE | Governance | P1 | "
                    f"{active_status} | Roadmap fixture active task |"
                ),
            ]
        ),
        encoding="utf-8",
    )
    completed_register = docs_dir / "task_register_completed.md"
    completed_register.write_text(
        "\n".join(
            [
                "| Task ID | Domain | Priority | Status | Summary |",
                "|---|---|---|---|---|",
                "| TRADING-TEST_DONE | Governance | P1 | DONE | Roadmap fixture done |",
            ]
        ),
        encoding="utf-8",
    )
    return task_register, completed_register


def _report_index_payload(
    tmp_path: Path,
    *,
    summary_overrides: dict[str, object] | None = None,
    payload_overrides: dict[str, dict[str, object]] | None = None,
    report_overrides: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    summary_overrides = summary_overrides or {}
    payload_overrides = payload_overrides or {}
    report_overrides = report_overrides or {}
    reports: list[dict[str, object]] = []
    for report_id in (
        "research_monthly_review_pack",
        "paper_shadow_promotion_board",
        "extended_shadow_protocol",
        "candidate_rejection_postmortem_template",
        "research_safety_boundary_audit",
        "artifact_lineage_graph",
        "task_register_consistency",
    ):
        payload = _source_payload(report_id)
        payload.update(payload_overrides.get(report_id, {}))
        source_path = tmp_path / "sources" / report_id / f"{report_id}.json"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        entry: dict[str, object] = {
            "report_id": report_id,
            "latest_artifact_path": str(source_path),
            "artifact_status": "AVAILABLE",
            "freshness_status": "FRESH",
            "production_effect": "none",
        }
        entry.update(report_overrides.get(report_id, {}))
        reports.append(entry)
    summary = {
        "report_count": len(reports),
        "missing_count": 0,
        "stale_count": 0,
        "required_missing_count": 0,
        "unwaived_warning_count": 0,
        "explicit_waiver_count": 0,
    }
    summary.update(summary_overrides)
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS",
        "summary": summary,
        "reports": reports,
        "production_effect": "none",
    }


def _source_payload(report_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "status": "PASS",
        "next_action": "continue_manual_roadmap_review",
        "production_effect": "none",
        "summary": {},
    }
    payload.update(
        {
            "research_monthly_review_pack": {
                "status": "MONTHLY_REVIEW_READY",
                "summary": {
                    "active_candidate_count": 1,
                    "paper_shadow_candidate_count": 1,
                    "needs_evidence_candidate_count": 0,
                    "major_blocker_count": 0,
                    "major_warning_count": 0,
                    "data_governance_status": "PASS",
                },
            },
            "paper_shadow_promotion_board": {
                "status": "CONTINUE_NORMAL_SHADOW",
                "summary": {
                    "board_decision": "CONTINUE_NORMAL_SHADOW",
                    "blocked_evidence_count": 0,
                    "warning_evidence_count": 0,
                },
            },
            "extended_shadow_protocol": {
                "status": "EXTENDED_SHADOW_ELIGIBLE",
                "summary": {
                    "eligibility_status": "EXTENDED_SHADOW_ELIGIBLE",
                    "blocked_check_count": 0,
                    "warning_check_count": 0,
                },
            },
            "candidate_rejection_postmortem_template": {
                "status": "TEMPLATE_READY",
                "summary": {"postmortem_record_provided": False},
            },
            "research_safety_boundary_audit": {
                "status": "SAFETY_PASS",
                "summary": {
                    "safety_status": "SAFETY_PASS",
                    "unsafe_signal_count": 0,
                    "missing_metadata_count": 0,
                },
            },
            "artifact_lineage_graph": {
                "status": "PASS",
                "summary": {
                    "lineage_status": "PASS",
                    "blocking_issue_count": 0,
                    "warning_issue_count": 0,
                },
            },
            "task_register_consistency": {
                "status": "PASS",
                "summary": {
                    "consistency_status": "PASS",
                    "blocking_issue_count": 0,
                    "warning_issue_count": 0,
                },
            },
        }.get(report_id, {})
    )
    return payload
