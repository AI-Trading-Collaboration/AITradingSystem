from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.paper_shadow_promotion_board import (
    CONTINUE_NORMAL_SHADOW,
    EVIDENCE_SPECS,
    EXTEND_SHADOW,
    HOLD_FOR_MORE_DATA,
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    REJECT,
    build_paper_shadow_promotion_board_payload,
    validate_paper_shadow_promotion_board_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_paper_shadow_promotion_board_holds_for_blocking_evidence(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_shadow_continuation_readiness": {
                "shadow_continuation_readiness": "BLOCKED_STALE_DATA",
                "next_required_action": "restore_fresh_inputs_before_shadow_review",
            },
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "INSUFFICIENT_COST_INPUTS",
            },
            "owner_decision_audit_log": {
                "audit_log_status": "AUDIT_LOG_EMPTY",
                "next_action": "append_owner_decision_after_manual_review",
            },
        },
    )

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)

    assert payload["board_decision"] == HOLD_FOR_MORE_DATA
    assert payload["summary"]["blocked_evidence_count"] >= 3
    assert any(
        reason["source_id"] == "owner_decision_audit_log"
        for reason in payload["blocking_reasons"]
    )
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_paper_shadow_promotion_board_rejects_owner_reject(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_owner_review": {
                "owner_decision": "reject_candidate",
                "recommended_action": "record_rejection_postmortem",
            },
        },
    )

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)

    assert payload["board_decision"] == REJECT
    assert any(reason["source_id"] == "owner_review" for reason in payload["blocking_reasons"])
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_paper_shadow_promotion_board_blocks_adverse_cost_and_benchmark_statuses(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "NOT_MEANINGFUL_UNDER_COSTS",
                "next_action": "return_candidate_to_research_until_net_improvement_survives_costs",
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "CANDIDATE_UNDERPERFORMS_BASELINES",
                "next_action": (
                    "return_candidate_to_research_until_it_outperforms_baseline_controls"
                ),
            },
        },
    )

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)

    assert payload["board_decision"] == HOLD_FOR_MORE_DATA
    assert {
        reason["source_id"] for reason in payload["blocking_reasons"]
    } >= {"cost_sensitivity", "benchmark_comparison"}
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_paper_shadow_promotion_board_includes_monthly_review_and_health_inputs(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "research_monthly_review_pack": {
                "monthly_review_status": "MONTHLY_REVIEW_BLOCKED",
            },
            "etf_dynamic_v3_paper_shadow_health": {
                "paper_shadow_health_status": "MANUAL_REVIEW_REQUIRED",
            },
        },
    )

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )

    source_statuses = {
        check["source_id"]: check["check_status"]
        for check in payload["required_evidence_checklist"]
    }
    assert source_statuses["monthly_review"] == "BLOCKED"
    assert source_statuses["paper_shadow_health"] == "WARNING"
    assert payload["board_decision"] == HOLD_FOR_MORE_DATA


def test_paper_shadow_promotion_board_does_not_extend_with_warning_prerequisites(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_shadow_continuation_readiness": {
                "shadow_continuation_readiness": "READY_WITH_WARNINGS",
            },
            "etf_dynamic_v3_owner_review": {
                "owner_decision": "enter_extended_shadow",
                "recommended_action": "enter_extended_shadow",
            },
        },
    )

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)

    assert payload["board_decision"] == HOLD_FOR_MORE_DATA
    assert payload["summary"]["warning_evidence_count"] == 1
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_paper_shadow_promotion_board_extends_only_with_all_required_evidence_pass(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "etf_dynamic_v3_owner_review": {
                "owner_decision": "enter_extended_shadow",
                "recommended_action": "enter_extended_shadow",
            },
        },
    )

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)

    assert payload["board_decision"] == EXTEND_SHADOW
    assert payload["summary"]["blocked_evidence_count"] == 0
    assert payload["summary"]["warning_evidence_count"] == 0
    assert validation["validation_status"] == PASS_STATUS


def test_paper_shadow_promotion_board_validation_fails_missing_required_source(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(tmp_path, omit={"artifact_lineage_graph"})

    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)

    assert payload["board_decision"] == HOLD_FOR_MORE_DATA
    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "required_evidence_artifacts_available"
        for issue in validation["blocking_issues"]
    )


def test_paper_shadow_promotion_board_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "paper-shadow-promotion-board",
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
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-paper-shadow-promotion-board",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "paper_shadow_promotion_board_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir / "paper_shadow_promotion_board_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["board_decision"] == CONTINUE_NORMAL_SHADOW
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"]["paper_shadow_promotion_board"].endswith(
        "paper_shadow_promotion_board_2026-05-04.json"
    )


def test_reader_brief_paper_shadow_promotion_board_summary(tmp_path: Path) -> None:
    payload = build_paper_shadow_promotion_board_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_paper_shadow_promotion_board_payload(payload)
    report_path = tmp_path / "paper_shadow_promotion_board_2026-05-04.json"
    validation_path = tmp_path / "paper_shadow_promotion_board_validation_2026-05-04.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._paper_shadow_promotion_board_summary(
        {
            "reports": [
                {
                    "report_id": "paper_shadow_promotion_board",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "paper_shadow_promotion_board_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["board_decision"] == CONTINUE_NORMAL_SHADOW
    assert summary["validation_status"] == PASS_STATUS
    assert summary["evidence_check_count"] == len(EVIDENCE_SPECS)
    assert summary["production_effect"] == "none"


def _report_index_payload(
    tmp_path: Path,
    *,
    overrides: dict[str, dict[str, object]] | None = None,
    omit: set[str] | None = None,
) -> dict[str, object]:
    overrides = overrides or {}
    omit = omit or set()
    reports: list[dict[str, object]] = []
    for spec in EVIDENCE_SPECS:
        report_id = str(spec["report_id"])
        if report_id in omit:
            continue
        payload = _source_payload(report_id)
        payload.update(overrides.get(report_id, {}))
        source_path = tmp_path / "sources" / report_id / f"{report_id}.json"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        reports.append(
            {
                "report_id": report_id,
                "latest_artifact_path": str(source_path),
                "artifact_status": "AVAILABLE",
                "freshness_status": "FRESH",
                "production_effect": "none",
            }
        )
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "reports": reports,
        "production_effect": "none",
    }


def _source_payload(report_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "candidate": "candidate_a",
        "status": "PASS",
        "next_action": "continue_manual_paper_shadow_review",
        "production_effect": "none",
        "summary": {},
    }
    payload.update(
        {
            "etf_dynamic_v3_paper_shadow_weekly_review": {
                "coverage_status": "FULL_WEEK_REVIEW",
                "weekly_decision": "CONTINUE",
            },
            "research_monthly_review_pack": {
                "monthly_review_status": "MONTHLY_REVIEW_READY",
            },
            "etf_dynamic_v3_paper_shadow_health": {
                "paper_shadow_health_status": "HEALTHY",
            },
            "etf_dynamic_v3_shadow_continuation_readiness": {
                "shadow_continuation_readiness": "READY_TO_CONTINUE",
            },
            "etf_dynamic_v3_paper_shadow_drift_monitor": {
                "drift_status": "LOW",
                "drift_severity": "LOW",
            },
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "COST_REVIEW_PASS",
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "BASELINE_CONTROL_PASS",
            },
            "research_safety_boundary_audit": {
                "safety_status": "SAFETY_PASS",
            },
            "etf_dynamic_v3_owner_review": {
                "owner_decision": "continue_shadow",
                "recommended_action": "continue_normal_shadow",
            },
            "owner_decision_audit_log": {
                "audit_log_status": "AUDIT_LOG_PASS",
            },
            "artifact_lineage_graph": {
                "lineage_status": "PASS",
            },
        }.get(report_id, {})
    )
    return payload
