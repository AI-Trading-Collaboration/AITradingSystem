from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from test_research_governance_recovery_pack import _report_index_payload
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import decision_stage_review as decision_stage
from ai_trading_system.reports.research_governance_recovery_pack import (
    build_research_governance_recovery_pack_payload,
)

RUN_DATE = date(2026, 6, 17)


def test_decision_stage_review_exact_eight_and_return_to_research(
    tmp_path: Path,
) -> None:
    recovery_pack, report_index = _blocked_recovery_pack(tmp_path)
    report_quality_gate = _report_quality_gate_payload()

    eight = decision_stage.build_eight_blocker_decision_review_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        recovery_pack_path=tmp_path / "recovery.json",
    )
    validation = decision_stage.validate_eight_blocker_decision_review_payload(eight)
    gate = decision_stage.build_normal_shadow_gate_gap_analysis_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
        report_index_payload=report_index,
    )
    promotion = decision_stage.build_promotion_blocker_review_payload(
        as_of=RUN_DATE,
        recovery_pack_payload=recovery_pack,
    )
    candidate = decision_stage.build_candidate_research_return_assessment_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        recovery_pack_payload=recovery_pack,
        project_root=tmp_path,
    )
    options = decision_stage.build_owner_decision_options_packet_payload(
        as_of=RUN_DATE,
        eight_blocker_payload=eight,
        gate_gap_payload=gate,
        promotion_review_payload=promotion,
        candidate_assessment_payload=candidate,
    )
    dry_run = decision_stage.build_owner_decision_dry_run_payload(
        as_of=RUN_DATE,
        decision_option="keep_hold",
        owner_options_payload=options,
        log_path=tmp_path / "owner_decision_audit_log.jsonl",
    )
    observation = decision_stage.build_observation_clock_readiness_plan_payload(
        as_of=RUN_DATE,
        gate_gap_payload=gate,
        recovery_pack_payload=recovery_pack,
    )
    quality = decision_stage.build_report_quality_warning_drilldown_payload(
        as_of=RUN_DATE,
        report_quality_gate_payload=report_quality_gate,
        report_quality_gate_path=tmp_path / "report_quality_gate.json",
    )
    snapshot = decision_stage.build_governance_status_snapshot_payload(
        as_of=RUN_DATE,
        eight_blocker_payload=eight,
        gate_gap_payload=gate,
        promotion_review_payload=promotion,
        candidate_assessment_payload=candidate,
        owner_options_payload=options,
        observation_plan_payload=observation,
        report_quality_drilldown_payload=quality,
    )

    assert eight["summary"]["remaining_blocker_count"] == 8
    assert validation["validation_status"] == "PASS"
    assert {row["source_id"] for row in eight["exact_blockers"]} == {
        "normal_paper_shadow_resumption_gate",
        "cost_sensitivity_metrics",
        "benchmark_baseline_metrics",
        "monthly_review",
        "promotion_board",
        "observation_clock",
        "extended_shadow_protocol",
        "roadmap_dashboard",
    }
    assert gate["summary"]["gap_status"] == "NORMAL_SHADOW_GATE_GAP_BLOCKED"
    assert promotion["summary"]["recommended_candidate_action"] == "RETURN_TO_RESEARCH"
    assert candidate["summary"]["candidate_decision_assessment"] == "RETURN_TO_RESEARCH"
    option_rows = {
        row["option_id"]: row for row in options["owner_decision_options"]
    }
    assert option_rows["keep_hold"]["allowed_by_current_gates"] is True
    assert option_rows["approve_resume_normal_shadow"]["allowed_by_current_gates"] is False
    assert option_rows["return_to_research"]["allowed_by_current_gates"] is True
    assert dry_run["summary"]["would_append"] is False
    assert dry_run["summary"]["real_entry_written"] is False
    assert not (tmp_path / "owner_decision_audit_log.jsonl").exists()
    assert observation["summary"]["readiness_plan_status"] == "OBSERVATION_CLOCK_NOT_READY"
    assert quality["summary"]["source_warning_count"] == 2
    assert snapshot["summary"]["recommended_owner_action"] == "return_to_research"
    assert snapshot["summary"]["normal_shadow_may_resume"] is False
    assert snapshot["summary"]["live_trading_remains_forbidden"] is True


def test_decision_stage_review_cli_writes_batch_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    recovery_pack, report_index = _blocked_recovery_pack(tmp_path)
    _write_json(reports_dir / "report_index_2026-06-17.json", report_index)
    _write_json(
        reports_dir / "research_governance_recovery_pack_2026-06-17.json",
        recovery_pack,
    )
    _write_json(
        reports_dir / "report_quality_gate_2026-06-17.json",
        _report_quality_gate_payload(),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "decision-stage-review",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--owner-decision-log-path",
            str(tmp_path / "owner_decision_audit_log.jsonl"),
        ],
    )
    assert result.exit_code == 0, result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-eight-blocker-decision-review",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    snapshot_path = (
        reports_dir / "governance_status_snapshot_after_decision_review_2026-06-17.json"
    )
    dry_run_path = reports_dir / "owner_decision_dry_run_2026-06-17.json"
    validation_path = (
        reports_dir / "eight_blocker_decision_review_validation_2026-06-17.json"
    )
    assert snapshot_path.exists()
    assert dry_run_path.exists()
    assert validation_path.exists()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    dry_run = json.loads(dry_run_path.read_text(encoding="utf-8"))
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert snapshot["status"] == "GOVERNANCE_STATUS_SNAPSHOT_BLOCKED"
    assert snapshot["summary"]["recommended_owner_action"] == "return_to_research"
    assert dry_run["summary"]["real_entry_written"] is False
    assert validation["validation_status"] == "PASS"


def _blocked_recovery_pack(tmp_path: Path) -> tuple[dict[str, object], dict[str, object]]:
    report_index = _report_index_payload(
        tmp_path,
        payload_overrides={
            "etf_dynamic_v3_normal_paper_shadow_resumption_gate": {
                "normal_paper_shadow_resumption_gate_status": "RESUME_NORMAL_SHADOW_BLOCKED",
                "normal_paper_shadow_may_resume": False,
                "owner_action": "hold",
                "resumption_requirements": [
                    {
                        "requirement_id": "owner_action_authorizes_normal_resumption",
                        "status": "BLOCKED",
                        "passed": False,
                        "detail": "owner_action=hold",
                    }
                ],
                "blocking_reasons": ["owner_action:hold"],
                "next_required_action": (
                    "keep_normal_paper_shadow_on_hold_until_owner_records_"
                    "approve_resume_normal_shadow"
                ),
                "summary": {"normal_paper_shadow_may_resume": False},
            },
            "owner_decision_audit_log": {
                "latest_owner_action": "hold",
                "summary": {
                    "latest_owner_action": "hold",
                    "latest_decision_id": "TRADING-413_owner_hold_2026-06-17",
                },
            },
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "NOT_MEANINGFUL_UNDER_COSTS",
                "next_required_action": (
                    "return_candidate_to_research_until_net_improvement_survives_costs"
                ),
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "CANDIDATE_UNDERPERFORMS_BASELINES",
                "next_required_action": (
                    "return_candidate_to_research_until_it_outperforms_baseline_controls"
                ),
            },
            "research_monthly_review_pack": {
                "monthly_review_status": "MONTHLY_REVIEW_BLOCKED",
                "major_blockers": [
                    "cost_sensitivity_status=NOT_MEANINGFUL_UNDER_COSTS",
                    "benchmark_baseline_status=CANDIDATE_UNDERPERFORMS_BASELINES",
                ],
            },
            "paper_shadow_promotion_board": {
                "board_decision": "HOLD_FOR_MORE_DATA",
            },
            "extended_shadow_observation_clock": {
                "observation_clock_status": "OBSERVATION_PERIOD_UNMET",
            },
            "extended_shadow_protocol": {
                "eligibility_status": "EXTENDED_SHADOW_BLOCKED",
            },
            "research_roadmap_dashboard": {
                "dashboard_status": "ROADMAP_BLOCKED",
            },
        },
    )
    recovery_pack = build_research_governance_recovery_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    return recovery_pack, report_index


def _report_quality_gate_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "report_quality_gate",
        "report_quality_status": "PASS_WITH_WARNINGS",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "warning_quality_issue_count": 2,
            "blocking_quality_issue_count": 0,
        },
        "warning_quality_issues": [
            {
                "issue_id": "missing_next_action_legacy_report",
                "report_id": "legacy_report",
                "section": "next_action",
                "message": "legacy report uses next_action alias",
                "recommended_action": "add_next_action_template_alias",
            },
            {
                "issue_id": "missing_safety_boundary_legacy_report",
                "report_id": "legacy_report",
                "section": "safety_boundary",
                "message": "legacy report missing safety boundary section",
                "recommended_action": "migrate_legacy_report_template",
            },
        ],
        "production_effect": "none",
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
