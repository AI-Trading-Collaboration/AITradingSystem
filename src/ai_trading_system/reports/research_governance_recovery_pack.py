from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.recovery_evidence_pack import _source_record
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _records,
    _text,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "research_governance_recovery_pack"
VALIDATION_REPORT_TYPE = "research_governance_recovery_pack_validation"

RECOVERY_GOVERNANCE_HEALTHY = "RECOVERY_GOVERNANCE_HEALTHY"
RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS = "RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS"
RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED = (
    "RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED"
)
RECOVERY_GOVERNANCE_BLOCKED = "RECOVERY_GOVERNANCE_BLOCKED"
RECOVERY_GOVERNANCE_STATUSES = (
    RECOVERY_GOVERNANCE_HEALTHY,
    RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS,
    RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED,
    RECOVERY_GOVERNANCE_BLOCKED,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

REQUIRED_SECTIONS = (
    "source_reports",
    "remaining_blockers",
    "remaining_warnings",
    "manual_review_items",
    "next_owner_action",
    "normal_paper_shadow_boundary",
    "extended_shadow_boundary",
    "live_trading_boundary",
    "reader_brief",
    "safety_boundary",
)

SOURCE_REPORT_SPECS: tuple[dict[str, Any], ...] = (
    {
        "source_id": "signal_input_restoration",
        "report_id": "etf_dynamic_v3_signal_input_recovery",
        "label": "Signal input restoration",
        "preferred_json_names": (
            "signal_input_recovery_report.json",
            "signal_input_recovery_manifest.json",
        ),
        "validation_json_names": ("signal_input_recovery_validation.json",),
        "status_fields": ("restoration_status", "signal_input_status", "status"),
        "pass_statuses": ("SIGNAL_INPUTS_RESTORED",),
        "warning_markers": ("SIGNAL_INPUTS_RESTORED_WITH_WARNINGS", "WARNING"),
        "block_markers": ("SIGNAL_INPUTS_STILL_BLOCKED", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "signal_completeness_recovery",
        "report_id": "etf_dynamic_v3_signal_input_completeness_recovery",
        "label": "Signal completeness recovery",
        "preferred_json_names": (
            "signal_input_completeness_recovery_report.json",
            "signal_input_completeness_recovery_manifest.json",
        ),
        "validation_json_names": ("signal_input_completeness_recovery_validation.json",),
        "status_fields": ("recovery_status", "restoration_status", "signal_input_status"),
        "pass_statuses": ("SIGNAL_INPUTS_RESTORED",),
        "warning_markers": ("SIGNAL_INPUTS_RESTORED_WITH_WARNINGS", "WARNING"),
        "block_markers": ("SIGNAL_INPUTS_STILL_BLOCKED", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "readiness_health_recovery",
        "report_id": "etf_dynamic_v3_readiness_health_recovery",
        "label": "Readiness and health recovery",
        "preferred_json_names": (
            "readiness_health_recovery_report.json",
            "readiness_health_recovery_manifest.json",
        ),
        "validation_json_names": ("readiness_health_recovery_validation.json",),
        "status_fields": ("readiness_health_recovery_status", "status"),
        "pass_statuses": ("PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION",),
        "warning_markers": ("MANUAL_REVIEW_REQUIRED", "WARNING"),
        "block_markers": ("PAPER_SHADOW_STILL_BLOCKED", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "normal_paper_shadow_resumption_gate",
        "report_id": "etf_dynamic_v3_normal_paper_shadow_resumption_gate",
        "label": "Normal paper-shadow resumption gate",
        "preferred_json_names": (
            "normal_paper_shadow_resumption_gate_report.json",
            "normal_paper_shadow_resumption_gate_manifest.json",
        ),
        "validation_json_names": ("normal_paper_shadow_resumption_gate_validation.json",),
        "status_fields": ("normal_paper_shadow_resumption_gate_status", "status"),
        "pass_statuses": ("RESUME_NORMAL_SHADOW_ALLOWED",),
        "warning_markers": ("RESUME_NORMAL_SHADOW_WITH_WARNINGS",),
        "block_markers": ("RESUME_NORMAL_SHADOW_BLOCKED", "FAIL"),
    },
    {
        "source_id": "cost_metrics_materialization",
        "report_id": "etf_dynamic_v3_cost_metrics_materialization",
        "label": "Cost sensitivity metrics materialization",
        "preferred_json_names": (
            "cost_metrics_materialization_report.json",
            "cost_metrics_materialization_manifest.json",
        ),
        "validation_json_names": ("cost_metrics_materialization_validation.json",),
        "status_fields": ("cost_metrics_materialization_status", "status"),
        "pass_statuses": ("COST_INPUTS_AVAILABLE",),
        "warning_markers": ("COST_INPUTS_PARTIAL", "WARNING"),
        "block_markers": ("INSUFFICIENT_COST_INPUTS", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "cost_sensitivity_metrics",
        "report_id": "etf_dynamic_v3_cost_sensitivity_review",
        "label": "Cost sensitivity metrics",
        "preferred_json_names": (
            "cost_sensitivity_review.json",
            "cost_sensitivity_manifest.json",
        ),
        "validation_json_names": ("cost_sensitivity_validation.json",),
        "status_fields": ("cost_sensitivity_status", "status"),
        "pass_statuses": ("MEANINGFUL_ALL_SCENARIOS", "MEANINGFUL_LOW_MEDIUM_ONLY"),
        "warning_markers": ("LOW_MEDIUM_ONLY", "WARNING"),
        "block_markers": ("INSUFFICIENT", "NOT_MEANINGFUL", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "benchmark_baseline_metrics_materialization",
        "report_id": "etf_dynamic_v3_benchmark_baseline_metrics_materialization",
        "label": "Benchmark baseline metrics materialization",
        "preferred_json_names": (
            "benchmark_baseline_metrics_materialization_report.json",
            "benchmark_baseline_metrics_materialization_manifest.json",
        ),
        "validation_json_names": (
            "benchmark_baseline_metrics_materialization_validation.json",
        ),
        "status_fields": ("benchmark_baseline_metrics_status", "status"),
        "pass_statuses": ("BASELINE_METRICS_AVAILABLE",),
        "warning_markers": ("BASELINE_METRICS_PARTIAL", "WARNING"),
        "block_markers": ("INSUFFICIENT_BASELINE_METRICS", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "benchmark_baseline_metrics",
        "report_id": "etf_dynamic_v3_benchmark_baseline_control",
        "label": "Benchmark baseline metrics",
        "preferred_json_names": (
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_manifest.json",
        ),
        "validation_json_names": ("benchmark_baseline_validation.json",),
        "status_fields": ("benchmark_baseline_status", "status"),
        "pass_statuses": ("CANDIDATE_OUTPERFORMS_BASELINES",),
        "warning_markers": ("MIXED_BASELINE_RESULT", "WARNING"),
        "block_markers": ("INSUFFICIENT", "UNDERPERFORMS", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "recovery_evidence_pack",
        "report_id": "recovery_evidence_pack",
        "validation_report_id": "recovery_evidence_pack_validation",
        "label": "Recovery evidence pack",
        "preferred_json_names": ("recovery_evidence_pack.json",),
        "validation_json_names": ("recovery_evidence_pack_validation.json",),
        "status_fields": ("recovery_evidence_status", "status"),
        "pass_statuses": ("RECOVERY_EVIDENCE_COMPLETE",),
        "warning_markers": ("RECOVERY_EVIDENCE_PARTIAL", "WARNING"),
        "block_markers": ("RECOVERY_EVIDENCE_BLOCKED", "FAIL"),
    },
    {
        "source_id": "owner_hold_decision",
        "report_id": "owner_decision_audit_log",
        "validation_report_id": "owner_decision_audit_log_validation",
        "label": "Owner hold decision",
        "preferred_json_names": ("owner_decision_audit_log.json",),
        "validation_json_names": ("owner_decision_audit_log_validation.json",),
        "status_fields": ("audit_log_status", "status"),
        "pass_statuses": ("AUDIT_LOG_PASS", "PASS"),
        "warning_markers": (),
        "block_markers": ("AUDIT_LOG_EMPTY", "AUDIT_LOG_BLOCKED", "FAIL", "MISSING"),
    },
    {
        "source_id": "monthly_review",
        "report_id": "research_monthly_review_pack",
        "validation_report_id": "research_monthly_review_pack_validation",
        "label": "Monthly review",
        "preferred_json_names": ("research_monthly_review_pack.json",),
        "validation_json_names": ("research_monthly_review_pack_validation.json",),
        "status_fields": ("monthly_review_status", "status"),
        "pass_statuses": ("MONTHLY_REVIEW_READY",),
        "warning_markers": ("MONTHLY_REVIEW_READY_WITH_WARNINGS", "WARNING"),
        "block_markers": ("MONTHLY_REVIEW_BLOCKED", "FAIL"),
    },
    {
        "source_id": "promotion_board",
        "report_id": "paper_shadow_promotion_board",
        "validation_report_id": "paper_shadow_promotion_board_validation",
        "label": "Promotion board",
        "preferred_json_names": ("paper_shadow_promotion_board.json",),
        "validation_json_names": ("paper_shadow_promotion_board_validation.json",),
        "status_fields": ("board_decision", "status"),
        "pass_statuses": ("CONTINUE_NORMAL_SHADOW", "EXTEND_SHADOW"),
        "warning_markers": (),
        "block_markers": ("HOLD_FOR_MORE_DATA", "RETURN_TO_RESEARCH", "REJECT", "FAIL"),
    },
    {
        "source_id": "observation_clock",
        "report_id": "extended_shadow_observation_clock",
        "validation_report_id": "extended_shadow_observation_clock_validation",
        "label": "Observation clock",
        "preferred_json_names": ("extended_shadow_observation_clock.json",),
        "validation_json_names": ("extended_shadow_observation_clock_validation.json",),
        "status_fields": ("observation_clock_status", "status"),
        "pass_statuses": ("OBSERVATION_PERIOD_MET",),
        "warning_markers": ("OBSERVATION_PERIOD_PARTIAL",),
        "block_markers": ("OBSERVATION_PERIOD_UNMET", "FAIL"),
    },
    {
        "source_id": "extended_shadow_protocol",
        "report_id": "extended_shadow_protocol",
        "validation_report_id": "extended_shadow_protocol_validation",
        "label": "Extended shadow protocol",
        "preferred_json_names": ("extended_shadow_protocol.json",),
        "validation_json_names": ("extended_shadow_protocol_validation.json",),
        "status_fields": ("eligibility_status", "status"),
        "pass_statuses": ("EXTENDED_SHADOW_ELIGIBLE",),
        "warning_markers": ("EXTENDED_SHADOW_REVIEW_REQUIRED",),
        "block_markers": ("EXTENDED_SHADOW_BLOCKED", "EXTENDED_SHADOW_NOT_READY", "FAIL"),
    },
    {
        "source_id": "roadmap_dashboard",
        "report_id": "research_roadmap_dashboard",
        "validation_report_id": "research_roadmap_dashboard_validation",
        "label": "Roadmap dashboard",
        "preferred_json_names": ("research_roadmap_dashboard.json",),
        "validation_json_names": ("research_roadmap_dashboard_validation.json",),
        "status_fields": ("dashboard_status", "status"),
        "pass_statuses": ("ROADMAP_HEALTHY",),
        "warning_markers": ("ROADMAP_WARNINGS", "ROADMAP_WITH_WARNINGS"),
        "block_markers": ("ROADMAP_BLOCKED", "FAIL"),
    },
    {
        "source_id": "decision_snapshot_lifecycle",
        "report_id": "decision_snapshot_lifecycle_policy",
        "validation_report_id": "decision_snapshot_lifecycle_policy_validation",
        "label": "Decision snapshot lifecycle",
        "preferred_json_names": ("decision_snapshot_lifecycle_policy.json",),
        "validation_json_names": ("decision_snapshot_lifecycle_policy_validation.json",),
        "status_fields": ("snapshot_lifecycle_status", "status"),
        "pass_statuses": ("SNAPSHOT_AVAILABLE", "SNAPSHOT_NOT_DUE"),
        "warning_markers": ("SNAPSHOT_MISSING_NON_BLOCKING",),
        "block_markers": ("SNAPSHOT_MISSING_BLOCKING", "FAIL"),
    },
)


def default_research_governance_recovery_pack_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_governance_recovery_pack_{as_of.isoformat()}.json"


def default_research_governance_recovery_pack_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_governance_recovery_pack_{as_of.isoformat()}.md"


def default_research_governance_recovery_pack_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"research_governance_recovery_pack_validation_{as_of.isoformat()}.json"
    )


def default_research_governance_recovery_pack_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"research_governance_recovery_pack_validation_{as_of.isoformat()}.md"
    )


def latest_research_governance_recovery_pack_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "research_governance_recovery_pack_", ".json")


def build_research_governance_recovery_pack_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if report_index_payload is None:
        source_path = report_index_path or (
            project_root / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
        )
        report_index_payload = _read_json_mapping(source_path)
        report_index_path = source_path

    source_reports = [
        _source_record(spec, report_index_payload, project_root=project_root)
        for spec in SOURCE_REPORT_SPECS
    ]
    structural_blockers = _structural_blockers(source_reports)
    remaining_blockers = _remaining_blockers(source_reports)
    remaining_warnings = _remaining_warnings(source_reports)
    manual_review_items = _manual_review_items(source_reports)
    normal_boundary = _normal_paper_shadow_boundary(source_reports, remaining_blockers)
    extended_boundary = _extended_shadow_boundary(source_reports, remaining_blockers)
    live_boundary = _live_trading_boundary()
    next_owner_action = _next_owner_action(
        structural_blockers,
        remaining_blockers,
        manual_review_items,
        remaining_warnings,
    )
    pack_status = _pack_status(
        structural_blockers,
        remaining_blockers,
        manual_review_items,
        remaining_warnings,
    )
    summary = _summary(
        pack_status,
        source_reports,
        structural_blockers,
        remaining_blockers,
        remaining_warnings,
        manual_review_items,
        next_owner_action,
        normal_boundary,
        extended_boundary,
        live_boundary,
    )
    reader_brief = _reader_brief(
        summary,
        remaining_blockers,
        remaining_warnings,
        normal_boundary,
        extended_boundary,
        live_boundary,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": pack_status,
        "recovery_governance_status": pack_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "research_governance_recovery_pack_only": True,
        "purpose": (
            "Consolidate post-recovery signal, readiness, health, cost, benchmark, "
            "owner, monthly, promotion, observation, extended-shadow, roadmap, and "
            "decision snapshot lifecycle outputs for final owner recovery review."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            **{
                source["source_id"]: source.get("source_payload_path", "")
                for source in source_reports
            },
            **{
                f"{source['source_id']}_validation": source.get(
                    "validation_payload_path",
                    "",
                )
                for source in source_reports
            },
        },
        "summary": summary,
        "source_reports": source_reports,
        "structural_blockers": structural_blockers,
        "remaining_blockers": remaining_blockers,
        "remaining_warnings": remaining_warnings,
        "manual_review_items": manual_review_items,
        "next_owner_action": next_owner_action,
        "normal_paper_shadow_boundary": normal_boundary,
        "extended_shadow_boundary": extended_boundary,
        "live_trading_boundary": live_boundary,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Recovery governance pack is read-only and does not run upstream reports.",
            "Readable recovery evidence does not override source blockers or owner hold.",
            "At most this pack can support owner review of normal paper-shadow resumption.",
            (
                "This pack never approves live trading, official target weights, "
                "broker action, or orders."
            ),
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_report_index_and_latest_recovery_governance_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_artifacts": True,
            "does_not_write_owner_decision": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "normal_paper_shadow_only_after_owner_review": True,
            "extended_shadow_requires_separate_protocol": True,
            "live_trading_approval_possible": False,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_research_governance_recovery_pack_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    source_reports = _records(payload.get("source_reports"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_research_governance_recovery_pack",
    )
    _append_check(
        checks,
        blocking_issues,
        "recovery_governance_status_enum",
        _text(payload.get("recovery_governance_status")) in RECOVERY_GOVERNANCE_STATUSES,
        "Recovery governance status must use the supported enum.",
        "restore_supported_recovery_governance_status",
    )
    missing_sections = [
        section for section in REQUIRED_SECTIONS if not _section_present(payload, section)
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_sections_present",
        not missing_sections,
        "Recovery governance pack must include every required section.",
        "regenerate_pack_with_required_sections",
        details={"missing_sections": missing_sections},
    )
    structural_blockers = _records(payload.get("structural_blockers"))
    _append_check(
        checks,
        blocking_issues,
        "required_sources_readable",
        len(source_reports) == len(SOURCE_REPORT_SPECS) and not structural_blockers,
        "Required recovery governance source reports and validations must be readable.",
        "run_or_repair_missing_recovery_governance_sources_before_pack",
        details={
            "expected_source_count": len(SOURCE_REPORT_SPECS),
            "actual_source_count": len(source_reports),
            "structural_blockers": [
                _text(issue.get("issue_id")) for issue in structural_blockers
            ],
        },
    )
    status = _text(payload.get("recovery_governance_status"))
    blocker_count = _int(summary.get("remaining_blocker_count"))
    _append_check(
        checks,
        blocking_issues,
        "blocked_status_matches_blockers",
        not (blocker_count > 0 and status != RECOVERY_GOVERNANCE_BLOCKED),
        "Source blockers must force RECOVERY_GOVERNANCE_BLOCKED.",
        "restore_recovery_governance_blocked_status",
    )
    live_boundary = _mapping(payload.get("live_trading_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "live_trading_forbidden",
        (
            live_boundary.get("live_trading_remains_forbidden") is True
            and live_boundary.get("live_trading_may_resume") is False
        ),
        "Recovery governance pack must never approve live trading.",
        "restore_live_trading_forbidden_boundary",
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("does_not_run_upstream_commands") is True
            and safety.get("does_not_refresh_data") is True
            and safety.get("does_not_generate_missing_artifacts") is True
            and safety.get("does_not_write_owner_decision") is True
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
            and safety.get("live_trading_allowed") is False
        ),
        "Recovery governance pack must preserve the research-only safety boundary.",
        "restore_recovery_governance_safety_boundary",
    )
    reader_brief = _mapping(payload.get("reader_brief"))
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        all(
            bool(_text(reader_brief.get(field)))
            for field in (
                "summary",
                "key_result",
                "blocking_issues",
                "warnings",
                "safety_boundary",
                "next_action",
            )
        ),
        (
            "Reader Brief section must expose summary, result, blockers, warnings, "
            "safety, and next action."
        ),
        "restore_recovery_governance_reader_brief_fields",
    )
    if blocker_count > 0:
        warning_issues.append(
            {
                "issue_id": "recovery_governance_contains_remaining_blockers",
                "message": "Pack is structurally valid but source conclusions remain blocked.",
                "recommended_action": _text(
                    _mapping(payload.get("next_owner_action")).get("action")
                ),
            }
        )
    if _int(summary.get("remaining_warning_count")) > 0:
        warning_issues.append(
            {
                "issue_id": "recovery_governance_contains_remaining_warnings",
                "message": "Pack contains source warnings requiring owner review.",
                "recommended_action": _text(
                    _mapping(payload.get("next_owner_action")).get("action")
                ),
            }
        )
    if _int(summary.get("manual_review_item_count")) > 0:
        warning_issues.append(
            {
                "issue_id": "recovery_governance_requires_manual_review",
                "message": "Pack contains manual review items before resumption decisions.",
                "recommended_action": _text(
                    _mapping(payload.get("next_owner_action")).get("action")
                ),
            }
        )

    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "warning_check_count": len(warning_issues),
        "source_report_count": len(source_reports),
        "available_source_count": _int(summary.get("available_source_count")),
        "remaining_blocker_count": blocker_count,
        "remaining_warning_count": _int(summary.get("remaining_warning_count")),
        "manual_review_item_count": _int(summary.get("manual_review_item_count")),
        "source_recovery_governance_status": status,
        "normal_paper_shadow_may_resume": bool(
            summary.get("normal_paper_shadow_may_resume")
        ),
        "extended_shadow_remains_forbidden": bool(
            summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            summary.get("live_trading_remains_forbidden")
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_recovery_governance_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "reader_brief": {
            "summary": (
                f"Recovery governance validation is {validation_status}; "
                f"failed_checks={validation_summary['failed_check_count']}."
            ),
            "key_result": validation_status,
            "blocking_issues": (
                "none"
                if not blocking_issues
                else "; ".join(_text(issue.get("issue_id")) for issue in blocking_issues[:5])
            ),
            "warnings": (
                "none"
                if not warning_issues
                else "; ".join(_text(issue.get("issue_id")) for issue in warning_issues[:5])
            ),
            "safety_boundary": "read-only recovery governance validation; production_effect=none",
            "next_action": (
                "repair_recovery_governance_pack"
                if validation_status == FAIL_STATUS
                else "review_recovery_governance_pack_findings"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
    }


def write_research_governance_recovery_pack_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_research_governance_recovery_pack_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_governance_recovery_pack_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_research_governance_recovery_pack_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_research_governance_recovery_pack_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_governance_recovery_pack_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_research_governance_recovery_pack_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Governance Recovery Pack - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- recovery_governance_status: {payload.get('recovery_governance_status')}",
        f"- source_reports: {summary.get('source_report_count')}",
        f"- available_sources: {summary.get('available_source_count')}",
        f"- remaining_blockers: {summary.get('remaining_blocker_count')}",
        f"- remaining_warnings: {summary.get('remaining_warning_count')}",
        f"- manual_review_items: {summary.get('manual_review_item_count')}",
        f"- next_owner_action: {summary.get('next_owner_action')}",
        f"- normal_paper_shadow_may_resume: {summary.get('normal_paper_shadow_may_resume')}",
        f"- extended_shadow_remains_forbidden: {summary.get('extended_shadow_remains_forbidden')}",
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
        f"- production_effect: {payload.get('production_effect')}",
        "",
        "## Source Reports",
        "",
        "|source_id|source_status|validation_status|conclusion|artifact|validation|",
        "|---|---|---|---|---|---|",
    ]
    for source in _records(payload.get("source_reports")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    source.get("source_id"),
                    source.get("source_status"),
                    source.get("validation_status"),
                    source.get("conclusion_status"),
                    source.get("source_payload_path"),
                    source.get("validation_payload_path"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Remaining Blockers", ""])
    if not _records(payload.get("remaining_blockers")):
        lines.append("- none")
    for blocker in _records(payload.get("remaining_blockers")):
        lines.append(_reason_line(blocker))
    lines.extend(["", "## Remaining Warnings", ""])
    if not _records(payload.get("remaining_warnings")):
        lines.append("- none")
    for warning in _records(payload.get("remaining_warnings")):
        lines.append(_reason_line(warning))
    lines.extend(
        [
            "",
            "## Boundaries",
            "",
            f"- normal_paper_shadow_may_resume: {summary.get('normal_paper_shadow_may_resume')}",
            (
                "- extended_shadow_remains_forbidden: "
                f"{summary.get('extended_shadow_remains_forbidden')}"
            ),
            f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
            "",
            "## Safety Boundary",
            "",
            "- recovery governance pack is advisory only.",
            f"- production_effect: {payload.get('production_effect')}",
            "- no upstream rerun, no data refresh, no missing-artifact fabrication, "
            "no owner decision write, no official target, no broker, no order ticket.",
            "",
        ]
    )
    return "\n".join(lines)


def render_research_governance_recovery_pack_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        (
            "# Research Governance Recovery Pack Validation - "
            f"{_text(payload.get('as_of'), 'UNKNOWN')}"
        ),
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_recovery_governance_status: {payload.get('source_recovery_governance_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed_checks: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- remaining_blockers: {summary.get('remaining_blocker_count')}",
        f"- remaining_warnings: {summary.get('remaining_warning_count')}",
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
        f"- production_effect: {payload.get('production_effect')}",
        "",
        "## Checks",
        "",
        "|check_id|status|message|recommended_action|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    check.get("check_id"),
                    check.get("status"),
                    check.get("message"),
                    check.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.append("")
    return "\n".join(lines)


def _structural_blockers(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        if _text(source.get("availability")) != "AVAILABLE":
            blockers.append(
                _issue(
                    f"{source_id}_missing",
                    source_id,
                    "BLOCKING",
                    "Required recovery governance source report is missing or unreadable.",
                    "run_or_restore_required_recovery_governance_source",
                    source,
                )
            )
            continue
        if _text(source.get("validation_availability")) != "AVAILABLE":
            blockers.append(
                _issue(
                    f"{source_id}_validation_missing",
                    source_id,
                    "BLOCKING",
                    "Required source validation artifact is missing or unreadable.",
                    "run_source_validation_before_recovery_governance_pack",
                    source,
                )
            )
        validation_status = _text(source.get("validation_status")).upper()
        if "FAIL" in validation_status:
            blockers.append(
                _issue(
                    f"{source_id}_validation_failed",
                    source_id,
                    "BLOCKING",
                    f"Required source validation failed: {source.get('validation_status')}.",
                    "repair_source_validation_before_recovery_governance_pack",
                    source,
                )
            )
        if _text(source.get("production_effect")) != PRODUCTION_EFFECT:
            blockers.append(
                _issue(
                    f"{source_id}_unsafe_production_effect",
                    source_id,
                    "BLOCKING",
                    "Required source does not declare production_effect=none.",
                    "restore_source_safety_boundary",
                    source,
                )
            )
    return blockers


def _remaining_blockers(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for source in source_reports:
        if _text(source.get("conclusion_status")) == "BLOCKING":
            blockers.append(
                _issue(
                    f"{_text(source.get('source_id'))}_source_blocking",
                    _text(source.get("source_id")),
                    "BLOCKING",
                    (
                        f"{_text(source.get('label'))} conclusion remains blocking: "
                        f"{_text(source.get('source_status'))}."
                    ),
                    _text(source.get("next_action"), "review_recovery_governance_blocker"),
                    source,
                )
            )
    return blockers


def _remaining_warnings(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        validation_status = _text(source.get("validation_status")).upper()
        if _text(source.get("conclusion_status")) == "WARNING":
            warnings.append(
                _issue(
                    f"{source_id}_source_warning",
                    source_id,
                    "WARNING",
                    (
                        f"{_text(source.get('label'))} requires review: "
                        f"{_text(source.get('source_status'))}."
                    ),
                    _text(source.get("next_action"), "review_recovery_governance_warning"),
                    source,
                )
            )
        elif "WARNING" in validation_status:
            warnings.append(
                _issue(
                    f"{source_id}_validation_warning",
                    source_id,
                    "WARNING",
                    f"Source validation has warning status: {source.get('validation_status')}.",
                    "review_source_validation_warning",
                    source,
                )
            )
    return warnings


def _manual_review_items(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    owner_source = _source_by_id(source_reports, "owner_hold_decision")
    owner_payload = _mapping(owner_source.get("payload")) if owner_source else {}
    owner_action = _source_value(owner_source, "latest_owner_action", "owner_action")
    if owner_action in {"", "hold"}:
        items.append(
            _issue(
                "owner_action_requires_manual_recovery_review",
                "owner_hold_decision",
                "MANUAL_REVIEW",
                (
                    "Latest owner action is missing or hold; normal paper-shadow cannot "
                    "resume without explicit continue_normal_shadow owner review."
                ),
                "record_owner_continue_normal_shadow_only_after_blockers_clear",
                owner_source,
            )
        )
    if _text(owner_payload.get("audit_log_status")) == "AUDIT_LOG_EMPTY":
        items.append(
            _issue(
                "owner_decision_log_empty",
                "owner_hold_decision",
                "MANUAL_REVIEW",
                "Owner decision audit log is empty.",
                "append_owner_hold_or_continue_decision_after_manual_review",
                owner_source,
            )
        )
    return items


def _normal_paper_shadow_boundary(
    source_reports: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    gate_source = _source_by_id(source_reports, "normal_paper_shadow_resumption_gate")
    owner_action = _source_value(
        _source_by_id(source_reports, "owner_hold_decision"),
        "latest_owner_action",
        "owner_action",
    )
    may_resume_from_gate = _source_bool(gate_source, "normal_paper_shadow_may_resume")
    gate_status = _text(gate_source.get("source_status")) if gate_source else "MISSING"
    may_resume = (
        not blockers
        and may_resume_from_gate
        and owner_action == "continue_normal_shadow"
        and gate_status in {
            "RESUME_NORMAL_SHADOW_ALLOWED",
            "RESUME_NORMAL_SHADOW_WITH_WARNINGS",
        }
    )
    status = "NORMAL_PAPER_SHADOW_MAY_RESUME_AFTER_OWNER_REVIEW"
    next_action = "resume_normal_paper_shadow_observation_only_after_owner_review"
    if not may_resume:
        status = "NORMAL_PAPER_SHADOW_REMAINS_FORBIDDEN"
        next_action = (
            "resolve_blockers_and_record_continue_normal_shadow_before_resumption"
        )
    return {
        "status": status,
        "normal_paper_shadow_may_resume": may_resume,
        "owner_action": owner_action,
        "resumption_gate_status": gate_status,
        "resumption_gate_may_resume": may_resume_from_gate,
        "blocking_dependency_count": len(blockers),
        "allowed_scope": "normal_paper_shadow_observation_only" if may_resume else "none",
        "forbidden_scope": [
            "promotion",
            "extended_shadow",
            "live_trading",
            "official_target_weights",
            "broker_action",
            "order_ticket",
            "production_mutation",
        ],
        "next_action": next_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _extended_shadow_boundary(
    source_reports: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    protocol_source = _source_by_id(source_reports, "extended_shadow_protocol")
    protocol_status = _text(protocol_source.get("source_status")) if protocol_source else "MISSING"
    allowed = not blockers and protocol_status == "EXTENDED_SHADOW_ELIGIBLE"
    return {
        "status": (
            "EXTENDED_SHADOW_OWNER_REVIEW_POSSIBLE"
            if allowed
            else "EXTENDED_SHADOW_REMAINS_FORBIDDEN"
        ),
        "extended_shadow_remains_forbidden": not allowed,
        "extended_shadow_may_resume": allowed,
        "eligibility_status": protocol_status,
        "blocking_dependency_count": len(blockers),
        "next_action": (
            "owner_review_extended_shadow_plan_separately"
            if allowed
            else "resolve_extended_shadow_blockers_before_owner_review"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _live_trading_boundary() -> dict[str, Any]:
    return {
        "status": "LIVE_TRADING_FORBIDDEN",
        "live_trading_remains_forbidden": True,
        "live_trading_may_resume": False,
        "reason": "Recovery governance pack never approves live trading.",
        "next_action": "do_not_use_recovery_pack_as_live_trading_approval",
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_owner_action(
    structural_blockers: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
    manual_review_items: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ordered = [*structural_blockers, *blockers, *manual_review_items, *warnings]
    if not ordered:
        return {
            "source_id": "owner_review",
            "priority": "P1",
            "action": "review_recovery_pack_and_optionally_resume_normal_paper_shadow",
            "reason": "No blocking source conclusions remain; live trading still forbidden.",
            "production_effect": PRODUCTION_EFFECT,
        }
    first = ordered[0]
    return {
        "source_id": _text(first.get("source_id")),
        "priority": _text(first.get("severity"), "P1"),
        "action": _text(first.get("recommended_action")),
        "reason": _text(first.get("message")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _pack_status(
    structural_blockers: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
    manual_review_items: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> str:
    if structural_blockers or blockers:
        return RECOVERY_GOVERNANCE_BLOCKED
    if manual_review_items:
        return RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED
    if warnings:
        return RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS
    return RECOVERY_GOVERNANCE_HEALTHY


def _summary(
    pack_status: str,
    source_reports: Sequence[Mapping[str, Any]],
    structural_blockers: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
    manual_review_items: Sequence[Mapping[str, Any]],
    next_owner_action: Mapping[str, Any],
    normal_boundary: Mapping[str, Any],
    extended_boundary: Mapping[str, Any],
    live_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    validation_statuses = [_text(source.get("validation_status")) for source in source_reports]
    return {
        "recovery_governance_status": pack_status,
        "source_report_count": len(source_reports),
        "available_source_count": len(
            [source for source in source_reports if source.get("availability") == "AVAILABLE"]
        ),
        "validation_available_count": len(
            [
                source
                for source in source_reports
                if source.get("validation_availability") == "AVAILABLE"
            ]
        ),
        "validation_pass_count": len(
            [status for status in validation_statuses if status in {"PASS", "OK"}]
        ),
        "validation_warning_count": len(
            [status for status in validation_statuses if "WARNING" in status.upper()]
        ),
        "structural_blocker_count": len(structural_blockers),
        "remaining_blocker_count": len(blockers),
        "remaining_warning_count": len(warnings),
        "manual_review_item_count": len(manual_review_items),
        "top_remaining_blocker": _text(blockers[0].get("source_id")) if blockers else "none",
        "next_owner_action": _text(next_owner_action.get("action"), "none"),
        "normal_paper_shadow_may_resume": bool(
            normal_boundary.get("normal_paper_shadow_may_resume")
        ),
        "extended_shadow_remains_forbidden": bool(
            extended_boundary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            live_boundary.get("live_trading_remains_forbidden")
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief(
    summary: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
    normal_boundary: Mapping[str, Any],
    extended_boundary: Mapping[str, Any],
    live_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    status = _text(summary.get("recovery_governance_status"), RECOVERY_GOVERNANCE_BLOCKED)
    return {
        "summary": (
            f"Recovery governance pack is {status}; sources="
            f"{_int(summary.get('available_source_count'))}/"
            f"{_int(summary.get('source_report_count'))}, blockers="
            f"{_int(summary.get('remaining_blocker_count'))}, warnings="
            f"{_int(summary.get('remaining_warning_count'))}."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not blockers
            else "; ".join(
                f"{_text(blocker.get('source_id'))}:{_text(blocker.get('source_status'))}"
                for blocker in blockers[:5]
            )
        ),
        "warnings": (
            "none"
            if not warnings
            else "; ".join(
                f"{_text(warning.get('source_id'))}:{_text(warning.get('source_status'))}"
                for warning in warnings[:5]
            )
        ),
        "normal_paper_shadow": _text(normal_boundary.get("status")),
        "extended_shadow": _text(extended_boundary.get("status")),
        "live_trading": _text(live_boundary.get("status")),
        "safety_boundary": (
            "Recovery governance only; no live trading, no official target weights, "
            "no broker/order, production_effect=none."
        ),
        "next_action": _text(summary.get("next_owner_action"), "review_recovery_pack"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _issue(
    issue_id: str,
    source_id: str,
    severity: str,
    message: str,
    recommended_action: str,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = source or {}
    return {
        "issue_id": issue_id,
        "source_id": source_id,
        "severity": severity,
        "message": message,
        "recommended_action": recommended_action,
        "source_status": _text(source.get("source_status")),
        "validation_status": _text(source.get("validation_status")),
        "artifact_path": _text(source.get("source_payload_path")),
        "production_effect": _text(source.get("production_effect"), PRODUCTION_EFFECT),
    }


def _source_by_id(
    source_reports: Sequence[Mapping[str, Any]],
    source_id: str,
) -> Mapping[str, Any]:
    for source in source_reports:
        if _text(source.get("source_id")) == source_id:
            return source
    return {}


def _source_value(source: Mapping[str, Any], *keys: str) -> str:
    payload = _mapping(source.get("payload"))
    summary = _mapping(payload.get("summary"))
    source_summary = _mapping(source.get("summary"))
    for key in keys:
        value = _text(payload.get(key), _text(summary.get(key), _text(source_summary.get(key))))
        if value:
            return value
    return ""


def _source_bool(source: Mapping[str, Any], key: str) -> bool:
    payload = _mapping(source.get("payload"))
    summary = _mapping(payload.get("summary"))
    source_summary = _mapping(source.get("summary"))
    for value in (payload.get(key), summary.get(key), source_summary.get(key)):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() == "true"
    return False


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_report_index_and_recovery_governance_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_write_owner_decision": True,
        "does_not_modify_strategy_logic": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "automatic_owner_approval": False,
        "automatic_candidate_promotion": False,
        "normal_paper_shadow_auto_resume": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    if details:
        check["details"] = dict(details)
    checks.append(check)
    if not passed:
        issue = {
            "issue_id": check_id,
            "message": message,
            "recommended_action": recommended_action,
        }
        if details:
            issue["details"] = dict(details)
        blocking_issues.append(issue)


def _section_present(payload: Mapping[str, Any], section: str) -> bool:
    value = payload.get(section)
    if isinstance(value, (dict, list)):
        return True
    return bool(_text(value))


def _reason_line(reason: Mapping[str, Any]) -> str:
    return (
        f"- {reason.get('source_id')}: {reason.get('source_status')} "
        f"-> {reason.get('recommended_action')}"
    )


__all__ = [
    "PASS_STATUS",
    "PASS_WITH_WARNINGS_STATUS",
    "RECOVERY_GOVERNANCE_BLOCKED",
    "RECOVERY_GOVERNANCE_HEALTHY",
    "RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS",
    "RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED",
    "SOURCE_REPORT_SPECS",
    "build_research_governance_recovery_pack_payload",
    "default_research_governance_recovery_pack_json_path",
    "default_research_governance_recovery_pack_markdown_path",
    "default_research_governance_recovery_pack_validation_json_path",
    "default_research_governance_recovery_pack_validation_markdown_path",
    "latest_research_governance_recovery_pack_json_path",
    "validate_research_governance_recovery_pack_payload",
    "write_research_governance_recovery_pack_json",
    "write_research_governance_recovery_pack_markdown",
    "write_research_governance_recovery_pack_validation_json",
    "write_research_governance_recovery_pack_validation_markdown",
]
