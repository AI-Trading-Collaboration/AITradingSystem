from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports import owner_decision_audit_log as owner_log
from ai_trading_system.reports.research_governance_recovery_pack import (
    latest_research_governance_recovery_pack_json_path,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _list_values,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _read_optional_json_mapping,
    _records,
    _report_index_entry,
    _resolve_artifact_path,
    _text,
)

SCHEMA_VERSION = 1
PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

EIGHT_BLOCKER_REPORT_TYPE = "eight_blocker_decision_review"
EIGHT_BLOCKER_VALIDATION_REPORT_TYPE = "eight_blocker_decision_review_validation"
NORMAL_GATE_GAP_REPORT_TYPE = "normal_shadow_gate_gap_analysis"
PROMOTION_BLOCKER_REPORT_TYPE = "promotion_blocker_after_metrics_review"
CANDIDATE_ASSESSMENT_REPORT_TYPE = "candidate_research_return_assessment"
OWNER_OPTIONS_REPORT_TYPE = "owner_decision_options_packet"
OWNER_DRY_RUN_REPORT_TYPE = "owner_decision_dry_run"
OBSERVATION_READINESS_REPORT_TYPE = "observation_clock_readiness_plan"
POST_DECISION_RERUN_REPORT_TYPE = "post_decision_rerun_plan"
REPORT_QUALITY_DRILLDOWN_REPORT_TYPE = "report_quality_warning_drilldown"
GOVERNANCE_SNAPSHOT_REPORT_TYPE = "governance_status_snapshot_after_decision_review"

REPORT_PREFIXES: dict[str, str] = {
    EIGHT_BLOCKER_REPORT_TYPE: "eight_blocker_decision_review",
    EIGHT_BLOCKER_VALIDATION_REPORT_TYPE: "eight_blocker_decision_review_validation",
    NORMAL_GATE_GAP_REPORT_TYPE: "normal_shadow_gate_gap_analysis",
    PROMOTION_BLOCKER_REPORT_TYPE: "promotion_blocker_after_metrics_review",
    CANDIDATE_ASSESSMENT_REPORT_TYPE: "candidate_research_return_assessment",
    OWNER_OPTIONS_REPORT_TYPE: "owner_decision_options_packet",
    OWNER_DRY_RUN_REPORT_TYPE: "owner_decision_dry_run",
    OBSERVATION_READINESS_REPORT_TYPE: "observation_clock_readiness_plan",
    POST_DECISION_RERUN_REPORT_TYPE: "post_decision_rerun_plan",
    REPORT_QUALITY_DRILLDOWN_REPORT_TYPE: "report_quality_warning_drilldown",
    GOVERNANCE_SNAPSHOT_REPORT_TYPE: "governance_status_snapshot_after_decision_review",
}

BLOCKING_FIELD_BY_SOURCE_ID = {
    "normal_paper_shadow_resumption_gate": "normal_paper_shadow_resumption_gate_status",
    "cost_sensitivity_metrics": "cost_sensitivity_status",
    "benchmark_baseline_metrics": "benchmark_baseline_status",
    "monthly_review": "monthly_review_status",
    "promotion_board": "board_decision",
    "observation_clock": "observation_clock_status",
    "extended_shadow_protocol": "eligibility_status",
    "roadmap_dashboard": "dashboard_status",
}

BLOCKER_CLASSIFICATION_BY_SOURCE_ID = {
    "normal_paper_shadow_resumption_gate": (
        "owner-decision blocker",
        ("signal/readiness blocker", "safety blocker"),
    ),
    "cost_sensitivity_metrics": (
        "cost/benchmark evidence blocker",
        ("strategy-performance blocker",),
    ),
    "benchmark_baseline_metrics": (
        "strategy-performance blocker",
        ("cost/benchmark evidence blocker",),
    ),
    "monthly_review": (
        "cost/benchmark evidence blocker",
        ("owner-decision blocker", "safety blocker"),
    ),
    "promotion_board": (
        "signal/readiness blocker",
        ("cost/benchmark evidence blocker", "owner-decision blocker"),
    ),
    "observation_clock": ("observation-period blocker", ()),
    "extended_shadow_protocol": (
        "safety blocker",
        ("observation-period blocker", "owner-decision blocker"),
    ),
    "roadmap_dashboard": ("report-quality blocker", ("owner-decision blocker",)),
}

OWNER_DECISION_OPTIONS = (
    "keep_hold",
    "approve_resume_normal_shadow",
    "return_to_research",
    "reject_candidate",
)

CANDIDATE_EVIDENCE_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "source_id": "filtered_candidate_evidence",
        "report_id": "etf_dynamic_v3_filtered_candidate_evidence",
        "preferred_json_names": (
            "filtered_candidate_evidence_summary.json",
            "filtered_candidate_evidence_manifest.json",
        ),
        "status_fields": ("evidence_status", "status"),
    },
    {
        "source_id": "stress_review",
        "report_id": "etf_dynamic_v3_filtered_candidate_stress_backfill",
        "preferred_json_names": (
            "filtered_candidate_stress_summary.json",
            "filtered_candidate_stress_manifest.json",
        ),
        "status_fields": ("stress_status", "status"),
    },
    {
        "source_id": "drawdown_mismatch_reduction",
        "report_id": "etf_dynamic_v3_drawdown_mismatch_reduction",
        "preferred_json_names": (
            "mismatch_reduction_summary.json",
            "drawdown_mismatch_reduction_manifest.json",
        ),
        "status_fields": ("drawdown_mismatch_reduction_status", "status"),
    },
    {
        "source_id": "flip_rotation_reduction",
        "report_id": "etf_dynamic_v3_flip_rotation_reduction",
        "preferred_json_names": (
            "flip_rotation_reduction_summary.json",
            "flip_rotation_reduction_manifest.json",
        ),
        "status_fields": ("flip_rotation_reduction_status", "status"),
    },
    {
        "source_id": "ab_review",
        "report_id": "etf_dynamic_v3_filtered_candidate_ab_review",
        "preferred_json_names": (
            "ab_summary.json",
            "filtered_candidate_ab_manifest.json",
        ),
        "status_fields": ("ab_review_status", "status"),
    },
    {
        "source_id": "signal_gate_confirmation",
        "report_id": "etf_dynamic_v3_signal_gate_confirmation",
        "preferred_json_names": (
            "signal_gate_confirmation_targets.json",
            "signal_gate_confirmation_manifest.json",
        ),
        "status_fields": ("confirmation_status", "status"),
    },
    {
        "source_id": "paper_shadow_health",
        "report_id": "etf_dynamic_v3_paper_shadow_health",
        "preferred_json_names": (
            "paper_shadow_health_report.json",
            "paper_shadow_health_manifest.json",
        ),
        "status_fields": ("paper_shadow_health_status", "status"),
    },
    {
        "source_id": "cost_sensitivity",
        "report_id": "etf_dynamic_v3_cost_sensitivity_review",
        "preferred_json_names": (
            "cost_sensitivity_review.json",
            "cost_sensitivity_manifest.json",
        ),
        "status_fields": ("cost_sensitivity_status", "status"),
    },
    {
        "source_id": "benchmark_baseline",
        "report_id": "etf_dynamic_v3_benchmark_baseline_control",
        "preferred_json_names": (
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_manifest.json",
        ),
        "status_fields": ("benchmark_baseline_status", "status"),
    },
    {
        "source_id": "promotion_board",
        "report_id": "paper_shadow_promotion_board",
        "preferred_json_names": ("paper_shadow_promotion_board.json",),
        "status_fields": ("board_decision", "status"),
    },
    {
        "source_id": "recovery_governance_pack",
        "report_id": "research_governance_recovery_pack",
        "preferred_json_names": ("research_governance_recovery_pack.json",),
        "status_fields": ("recovery_governance_status", "status"),
    },
)


def default_decision_stage_json_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.json"


def default_decision_stage_markdown_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.md"


def latest_decision_stage_json_path(report_type: str, output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, f"{REPORT_PREFIXES[report_type]}_", ".json")


def build_eight_blocker_decision_review_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any],
    recovery_pack_path: Path | None = None,
) -> dict[str, Any]:
    source_by_id = _source_reports_by_id(recovery_pack_payload)
    blockers = _records(recovery_pack_payload.get("remaining_blockers"))
    blocker_reviews = [
        _blocker_review(row, source_by_id.get(_text(row.get("source_id")), {}))
        for row in blockers
    ]
    summary = {
        "review_status": (
            "EIGHT_BLOCKER_REVIEW_READY"
            if len(blocker_reviews) == 8
            else "EIGHT_BLOCKER_REVIEW_COUNT_MISMATCH"
        ),
        "remaining_blocker_count": len(blocker_reviews),
        "expected_blocker_count": 8,
        "remaining_warning_count": _int(
            _mapping(recovery_pack_payload.get("summary")).get("remaining_warning_count")
        ),
        "recovery_governance_status": _text(
            recovery_pack_payload.get("recovery_governance_status"),
            _text(recovery_pack_payload.get("status"), "UNKNOWN"),
        ),
        "normal_paper_shadow_may_resume": bool(
            _mapping(recovery_pack_payload.get("summary")).get(
                "normal_paper_shadow_may_resume"
            )
        ),
        "extended_shadow_remains_forbidden": bool(
            _mapping(recovery_pack_payload.get("summary")).get(
                "extended_shadow_remains_forbidden",
                True,
            )
        ),
        "live_trading_remains_forbidden": True,
        "candidate_should_return_to_research": any(
            row["candidate_should_return_to_research"] for row in blocker_reviews
        ),
        "owner_judgment_required": any(
            row["requires_owner_judgment"] for row in blocker_reviews
        ),
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(
        summary=(
            f"Decision-stage review found {len(blocker_reviews)} exact blockers; "
            f"normal_shadow={summary['normal_paper_shadow_may_resume']}; "
            "live trading remains forbidden."
        ),
        key_result=summary["review_status"],
        blocking_issues="; ".join(
            f"{row['source_id']}={row['exact_current_value']}"
            for row in blocker_reviews
        )
        or "none",
        warnings=(
            f"remaining_warnings={summary['remaining_warning_count']}"
            if summary["remaining_warning_count"]
            else "none"
        ),
        next_action="use_exact_blocker_review_for_owner_decision_options_packet",
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": EIGHT_BLOCKER_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": summary["review_status"],
        "review_status": summary["review_status"],
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "diagnosis_only": True,
        "purpose": (
            "List the exact remaining blockers after recovery reruns without changing "
            "blocker logic or owner decisions."
        ),
        "input_artifacts": {
            "research_governance_recovery_pack": _path_text(recovery_pack_path),
        },
        "output_decision": summary["review_status"],
        "summary": summary,
        "exact_blockers": blocker_reviews,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Diagnosis only; does not change blocker logic.",
            "Does not append owner decisions or generate signoff packets.",
            "Does not resume normal shadow, approve extended shadow, or approve live trading.",
        ],
        "next_action": reader_brief["next_action"],
    }


def validate_eight_blocker_decision_review_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    blockers = _records(payload.get("exact_blockers"))
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == EIGHT_BLOCKER_REPORT_TYPE,
        f"report_type must be {EIGHT_BLOCKER_REPORT_TYPE}.",
        "regenerate_eight_blocker_decision_review",
    )
    _append_check(
        checks,
        blocking_issues,
        "exactly_eight_blockers",
        len(blockers) == 8,
        "Decision review must list exactly eight blockers.",
        "rerun_recovery_governance_pack_or_review_count_mismatch",
    )
    required_fields = {
        "source_report",
        "source_artifact_id",
        "exact_blocking_field",
        "exact_current_value",
        "required_next_action",
        "can_be_fixed_by_code_data_regeneration",
        "requires_owner_judgment",
        "candidate_should_return_to_research",
    }
    _append_check(
        checks,
        blocking_issues,
        "blocker_required_fields",
        all(required_fields.issubset(set(row)) for row in blockers),
        (
            "Every blocker must expose source, field, value, action, "
            "fixability, owner, and research-return fields."
        ),
        "repair_eight_blocker_review_schema",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_locked",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Decision review must preserve no-mutation safety boundary.",
        "restore_decision_stage_safety_boundary",
    )
    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": EIGHT_BLOCKER_VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "purpose": "Validate exact eight-blocker decision review schema and safety boundary.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": status,
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(blocking_issues),
            "exact_blocker_count": len(blockers),
            "production_effect": PRODUCTION_EFFECT,
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": [],
        "reader_brief": _reader_brief(
            summary=f"Eight-blocker decision review validation is {status}.",
            key_result=status,
            blocking_issues=_issue_list(blocking_issues),
            warnings="none",
            next_action=(
                "repair_eight_blocker_decision_review"
                if status == FAIL_STATUS
                else "use_eight_blocker_decision_review_for_owner_packet"
            ),
        ),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not change blocker logic.",
        ],
        "next_action": (
            "repair_eight_blocker_decision_review"
            if status == FAIL_STATUS
            else "use_eight_blocker_decision_review_for_owner_packet"
        ),
    }


def build_normal_shadow_gate_gap_analysis_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any],
    normal_gate_payload: Mapping[str, Any] | None = None,
    report_index_payload: Mapping[str, Any] | None = None,
    recovery_pack_path: Path | None = None,
) -> dict[str, Any]:
    source_by_id = _source_reports_by_id(recovery_pack_payload)
    gate_source = source_by_id.get("normal_paper_shadow_resumption_gate", {})
    if normal_gate_payload is None:
        normal_gate_payload = _read_source_payload(gate_source)
    gate_requirements = {
        _text(row.get("requirement_id")): row
        for row in _records(normal_gate_payload.get("resumption_requirements"))
    }
    conditions = [
        _gate_condition_from_requirement(
            "signal_input_completeness",
            "signal_input_completeness_not_blocking",
            gate_requirements,
            source_by_id.get("signal_completeness_recovery", {}),
            required_value="not BLOCKING",
        ),
        _gate_condition_from_requirement(
            "evidence_staleness",
            "evidence_staleness_not_blocking",
            gate_requirements,
            source_by_id.get("readiness_health_recovery", {}),
            required_value="not BLOCKING and source not missing",
        ),
        _gate_condition_from_requirement(
            "readiness",
            "readiness_not_blocked",
            gate_requirements,
            source_by_id.get("readiness_health_recovery", {}),
            required_value="readiness status not BLOCKED",
        ),
        _gate_condition_from_requirement(
            "paper_shadow_health",
            "canonical_health_not_blocked",
            gate_requirements,
            source_by_id.get("readiness_health_recovery", {}),
            required_value="canonical health not BLOCKED",
        ),
        _gate_condition_from_requirement(
            "safety_boundary",
            "safety_boundary_not_blocked",
            gate_requirements,
            source_by_id.get("readiness_health_recovery", {}),
            required_value="all source validations PASS and no safety block",
        ),
        _gate_condition_from_requirement(
            "owner_action",
            "owner_action_authorizes_normal_resumption",
            gate_requirements,
            source_by_id.get("owner_hold_decision", {}),
            required_value="approve_resume_normal_shadow or continue_normal_shadow",
            owner_action_required=True,
        ),
        _gate_condition_from_requirement(
            "manual_owner_review",
            "manual_owner_review_before_resumption",
            gate_requirements,
            source_by_id.get("owner_hold_decision", {}),
            required_value="manual owner decision recorded",
            owner_action_required=True,
        ),
        _gate_condition_from_source(
            "cost_benchmark_sufficiency",
            "cost metrics meaningful and benchmark comparison not underperforming",
            (
                _text(source_by_id.get("cost_sensitivity_metrics", {}).get("source_status"))
                + " / "
                + _text(source_by_id.get("benchmark_baseline_metrics", {}).get("source_status"))
            ),
            _cost_benchmark_gate_status(source_by_id),
            "cost_sensitivity_metrics;benchmark_baseline_metrics",
            _join_paths(
                source_by_id.get("cost_sensitivity_metrics", {}),
                source_by_id.get("benchmark_baseline_metrics", {}),
            ),
            owner_action_required=True,
        ),
        _gate_condition_from_source(
            "report_index",
            "report index PASS or PASS_WITH_EXPLICIT_WAIVERS with unwaived=0",
            _report_index_actual_value(report_index_payload, recovery_pack_payload),
            _report_index_condition_status(report_index_payload, recovery_pack_payload),
            "report_index",
            _text(_mapping(recovery_pack_payload.get("input_artifacts")).get("report_index")),
        ),
        _gate_condition_from_source(
            "decision_snapshot_lifecycle",
            "SNAPSHOT_AVAILABLE or SNAPSHOT_NOT_DUE",
            _text(source_by_id.get("decision_snapshot_lifecycle", {}).get("source_status")),
            _pass_warning_fail(
                _text(source_by_id.get("decision_snapshot_lifecycle", {}).get("source_status")),
                pass_values={"SNAPSHOT_AVAILABLE", "SNAPSHOT_NOT_DUE"},
                warning_values={"SNAPSHOT_MISSING_NON_BLOCKING"},
            ),
            "decision_snapshot_lifecycle",
            _text(source_by_id.get("decision_snapshot_lifecycle", {}).get("source_payload_path")),
        ),
    ]
    failed = [row for row in conditions if row["status"] == "FAIL"]
    warnings = [row for row in conditions if row["status"] == "WARNING"]
    status = "NORMAL_SHADOW_GATE_GAP_BLOCKED" if failed else (
        "NORMAL_SHADOW_GATE_GAP_WARNING" if warnings else "NORMAL_SHADOW_GATE_READY"
    )
    summary = {
        "gap_status": status,
        "normal_gate_status": _text(
            normal_gate_payload.get("normal_paper_shadow_resumption_gate_status"),
            _text(gate_source.get("source_status"), "UNKNOWN"),
        ),
        "normal_paper_shadow_may_resume": bool(
            normal_gate_payload.get("normal_paper_shadow_may_resume")
        ),
        "condition_count": len(conditions),
        "pass_count": len([row for row in conditions if row["status"] == "PASS"]),
        "warning_count": len(warnings),
        "fail_count": len(failed),
        "owner_action": _text(normal_gate_payload.get("owner_action"), "UNKNOWN"),
        "owner_action_required": any(row["owner_action_required"] for row in failed),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=NORMAL_GATE_GAP_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Explain why RESUME_NORMAL_SHADOW_BLOCKED remains true.",
        input_artifacts={
            "research_governance_recovery_pack": _path_text(recovery_pack_path),
            "normal_paper_shadow_resumption_gate": _text(
                gate_source.get("source_payload_path")
            ),
        },
        output_decision=status,
        summary=summary,
        body={
            "gate_conditions": conditions,
            "blocking_conditions": failed,
            "warning_conditions": warnings,
        },
        reader_brief=_reader_brief(
            summary=(
                f"Normal shadow gate gap is {status}: "
                f"fail={summary['fail_count']}, owner_action={summary['owner_action']}."
            ),
            key_result=status,
            blocking_issues=_condition_list(failed),
            warnings=_condition_list(warnings),
            next_action="keep_normal_shadow_blocked_until_failed_gate_conditions_clear",
        ),
        next_action="keep_normal_shadow_blocked_until_failed_gate_conditions_clear",
    )


def build_promotion_blocker_review_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any],
    recovery_pack_path: Path | None = None,
) -> dict[str, Any]:
    source_by_id = _source_reports_by_id(recovery_pack_payload)
    cost_status = _text(source_by_id.get("cost_sensitivity_metrics", {}).get("source_status"))
    benchmark_status = _text(
        source_by_id.get("benchmark_baseline_metrics", {}).get("source_status")
    )
    cost_metrics_status = _text(
        source_by_id.get("cost_metrics_materialization", {}).get("source_status")
    )
    benchmark_metrics_status = _text(
        source_by_id.get("benchmark_baseline_metrics_materialization", {}).get(
            "source_status"
        )
    )
    promotion_board_status = _text(source_by_id.get("promotion_board", {}).get("source_status"))
    monthly_status = _text(source_by_id.get("monthly_review", {}).get("source_status"))
    owner_action = _owner_action_from_recovery(recovery_pack_payload, source_by_id)
    observation_status = _text(source_by_id.get("observation_clock", {}).get("source_status"))
    readiness_status = _text(source_by_id.get("readiness_health_recovery", {}).get("source_status"))
    safety_status = _text(
        _mapping(source_by_id.get("promotion_board", {}).get("summary")).get("safety_status")
    )
    reasons = [
        _reason_flag(
            "cost_metrics_available_but_unfavorable",
            cost_metrics_status == "COST_INPUTS_AVAILABLE"
            and "NOT_MEANINGFUL" in cost_status,
            cost_status,
        ),
        _reason_flag(
            "benchmark_metrics_available_but_unfavorable",
            benchmark_metrics_status == "BASELINE_METRICS_AVAILABLE"
            and "UNDERPERFORMS" in benchmark_status,
            benchmark_status,
        ),
        _reason_flag(
            "metrics_available_but_insufficient_quality",
            "INSUFFICIENT" in cost_status or "INSUFFICIENT" in benchmark_status,
            f"{cost_status}/{benchmark_status}",
        ),
        _reason_flag(
            "owner_decision_remains_hold",
            owner_action in {"hold", "keep_hold"},
            owner_action,
        ),
        _reason_flag(
            "observation_period_unmet",
            observation_status in {"OBSERVATION_PERIOD_UNMET", "OBSERVATION_NOT_STARTED"},
            observation_status,
        ),
        _reason_flag(
            "readiness_or_health_blocked_or_warning",
            readiness_status not in {
                "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION",
                "",
            },
            readiness_status,
        ),
        _reason_flag(
            "safety_warnings_remain",
            bool(safety_status and safety_status != "SAFETY_PASS"),
            safety_status,
        ),
    ]
    recommendation = _promotion_recommendation(reasons)
    evidence_status = _evidence_sufficiency_status(
        cost_metrics_status,
        benchmark_metrics_status,
        cost_status,
        benchmark_status,
    )
    summary = {
        "promotion_blocker_status": "PROMOTION_BLOCKED_AFTER_METRICS_REVIEW",
        "promotion_board_status": promotion_board_status,
        "monthly_review_status": monthly_status,
        "cost_metrics_materialization_status": cost_metrics_status,
        "cost_sensitivity_status": cost_status,
        "benchmark_metrics_materialization_status": benchmark_metrics_status,
        "benchmark_baseline_status": benchmark_status,
        "evidence_sufficiency_status": evidence_status,
        "recommended_candidate_action": recommendation,
        "owner_action": owner_action,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=PROMOTION_BLOCKER_REPORT_TYPE,
        as_of=as_of,
        status=summary["promotion_blocker_status"],
        purpose=(
            "Explain why promotion remains blocked after cost and benchmark "
            "metrics materialized."
        ),
        input_artifacts={
            "research_governance_recovery_pack": _path_text(recovery_pack_path),
            "cost_sensitivity_review": _text(
                source_by_id.get("cost_sensitivity_metrics", {}).get("source_payload_path")
            ),
            "benchmark_baseline_control": _text(
                source_by_id.get("benchmark_baseline_metrics", {}).get(
                    "source_payload_path"
                )
            ),
            "promotion_board": _text(
                source_by_id.get("promotion_board", {}).get("source_payload_path")
            ),
            "monthly_review": _text(
                source_by_id.get("monthly_review", {}).get("source_payload_path")
            ),
        },
        output_decision=recommendation,
        summary=summary,
        body={"promotion_blocker_reasons": reasons},
        reader_brief=_reader_brief(
            summary=(
                f"Promotion remains blocked: cost={cost_status}, "
                f"benchmark={benchmark_status}, owner={owner_action}."
            ),
            key_result=recommendation,
            blocking_issues="; ".join(
                row["reason_id"] for row in reasons if row["applies"]
            )
            or "none",
            warnings="safety/readiness warnings remain visible",
            next_action="do_not_force_promotion_review_owner_options",
        ),
        next_action="do_not_force_promotion_review_owner_options",
    )


def build_candidate_research_return_assessment_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any],
    recovery_pack_payload: Mapping[str, Any],
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    evidence = [
        _report_index_source_evidence(report_index_payload, spec, project_root=project_root)
        for spec in CANDIDATE_EVIDENCE_SOURCES
    ]
    source_by_id = _source_reports_by_id(recovery_pack_payload)
    cost_status = _text(source_by_id.get("cost_sensitivity_metrics", {}).get("source_status"))
    benchmark_status = _text(
        source_by_id.get("benchmark_baseline_metrics", {}).get("source_status")
    )
    normal_gate_status = _text(
        source_by_id.get("normal_paper_shadow_resumption_gate", {}).get("source_status")
    )
    owner_action = _owner_action_from_recovery(recovery_pack_payload, source_by_id)
    decision = _candidate_decision(
        cost_status=cost_status,
        benchmark_status=benchmark_status,
        normal_gate_status=normal_gate_status,
        owner_action=owner_action,
    )
    strongest_for = _strongest_evidence_for(evidence)
    strongest_against = _strongest_evidence_against(
        cost_status=cost_status,
        benchmark_status=benchmark_status,
        normal_gate_status=normal_gate_status,
    )
    uncertainty = _unresolved_uncertainty(evidence, recovery_pack_payload)
    summary = {
        "candidate_id": _candidate_id_from_evidence(evidence)
        or "median_plus_regime_mismatch_filter",
        "candidate_decision_assessment": decision,
        "owner_action": owner_action,
        "normal_gate_status": normal_gate_status,
        "cost_sensitivity_status": cost_status,
        "benchmark_baseline_status": benchmark_status,
        "evidence_source_count": len(evidence),
        "available_evidence_source_count": len(
            [row for row in evidence if row["availability"] == "AVAILABLE"]
        ),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=CANDIDATE_ASSESSMENT_REPORT_TYPE,
        as_of=as_of,
        status=decision,
        purpose=(
            "Assess whether median_plus_regime_mismatch_filter should remain on hold, "
            "resume normal paper-shadow, return to research, or be rejected."
        ),
        input_artifacts={
            "report_index": _text(
                _mapping(recovery_pack_payload.get("input_artifacts")).get(
                    "report_index"
                )
            ),
            "research_governance_recovery_pack": "",
        },
        output_decision=decision,
        summary=summary,
        body={
            "evidence_sources": evidence,
            "strongest_evidence_in_favor": strongest_for,
            "strongest_evidence_against": strongest_against,
            "unresolved_uncertainty": uncertainty,
            "required_owner_action": _required_owner_action_for_candidate_decision(decision),
        },
        reader_brief=_reader_brief(
            summary=f"Candidate assessment is {decision}; owner action remains {owner_action}.",
            key_result=decision,
            blocking_issues="; ".join(str(item) for item in strongest_against) or "none",
            warnings="; ".join(str(item) for item in uncertainty) or "none",
            next_action=_required_owner_action_for_candidate_decision(decision),
        ),
        next_action=_required_owner_action_for_candidate_decision(decision),
    )


def build_owner_decision_options_packet_payload(
    *,
    as_of: date,
    eight_blocker_payload: Mapping[str, Any],
    gate_gap_payload: Mapping[str, Any],
    promotion_review_payload: Mapping[str, Any],
    candidate_assessment_payload: Mapping[str, Any],
) -> dict[str, Any]:
    gate_summary = _mapping(gate_gap_payload.get("summary"))
    candidate_summary = _mapping(candidate_assessment_payload.get("summary"))
    blocker_count = _int(
        _mapping(eight_blocker_payload.get("summary")).get("remaining_blocker_count")
    )
    owner_action = _text(candidate_summary.get("owner_action"), "hold")
    options = [
        _owner_option(
            "keep_hold",
            allowed=True,
            required_evidence=[
                "current owner action remains hold",
                "normal gate is blocked or owner has not approved resumption",
            ],
            consequences=[
                "normal paper-shadow remains paused",
                "rerun governance monitoring only",
            ],
            risks=["delays new normal-shadow evidence collection"],
        ),
        _owner_option(
            "approve_resume_normal_shadow",
            allowed=gate_summary.get("gap_status") == "NORMAL_SHADOW_GATE_READY",
            required_evidence=[
                "normal gate non-blocking",
                "owner explicitly approves normal paper-shadow resumption",
                "first valid daily paper-shadow artifact generated after approval",
            ],
            consequences=[
                "may resume normal observation only",
                "does not approve promotion, extended shadow, or live trading",
            ],
            risks=[
                "blocked if cost/benchmark or readiness evidence remains unfavorable",
                "requires monitoring for fresh warnings",
            ],
        ),
        _owner_option(
            "return_to_research",
            allowed=True,
            required_evidence=[
                "candidate assessment or promotion review recommends research return",
                "owner accepts that cost/benchmark evidence is not promotion-ready",
            ],
            consequences=[
                "stop promotion checks for this candidate",
                "create candidate research return plan",
            ],
            risks=["may discard a candidate that could improve with more evidence"],
        ),
        _owner_option(
            "reject_candidate",
            allowed=True,
            required_evidence=[
                "owner accepts evidence is sufficient for rejection",
                "rejection postmortem is generated before archival",
            ],
            consequences=[
                "candidate is archived after owner decision",
                "future work requires a new research task or revisit rationale",
            ],
            risks=["irreversible governance interpretation unless reopened manually"],
        ),
    ]
    status = "OWNER_DECISION_OPTIONS_READY"
    summary = {
        "options_status": status,
        "current_owner_action": owner_action,
        "remaining_blocker_count": blocker_count,
        "normal_shadow_gate_gap_status": gate_summary.get("gap_status"),
        "candidate_assessment": candidate_summary.get("candidate_decision_assessment"),
        "recommended_owner_action": _recommended_owner_action(
            candidate_summary.get("candidate_decision_assessment"),
            owner_action,
        ),
        "normal_shadow_may_resume": gate_summary.get("gap_status")
        == "NORMAL_SHADOW_GATE_READY",
        "extended_shadow_remains_forbidden": True,
        "live_trading_remains_forbidden": True,
        "official_target_weights_forbidden": True,
        "broker_order_forbidden": True,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=OWNER_OPTIONS_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Prepare conservative manual owner decision options.",
        input_artifacts={
            "eight_blocker_decision_review": _artifact_id_from_payload(eight_blocker_payload),
            "normal_shadow_gate_gap_analysis": _artifact_id_from_payload(gate_gap_payload),
            "promotion_blocker_after_metrics_review": _artifact_id_from_payload(
                promotion_review_payload
            ),
            "candidate_research_return_assessment": _artifact_id_from_payload(
                candidate_assessment_payload
            ),
        },
        output_decision=summary["recommended_owner_action"],
        summary=summary,
        body={"owner_decision_options": options},
        reader_brief=_reader_brief(
            summary=(
                f"Owner options ready; recommended action is "
                f"{summary['recommended_owner_action']}."
            ),
            key_result=status,
            blocking_issues=(
                "approve_resume_normal_shadow not allowed by current gate"
                if not summary["normal_shadow_may_resume"]
                else "none"
            ),
            warnings=(
                "extended shadow, live trading, official weights, broker/order "
                "remain forbidden"
            ),
            next_action="manual_owner_review_without_auto_append",
        ),
        next_action="manual_owner_review_without_auto_append",
    )


def build_owner_decision_dry_run_payload(
    *,
    as_of: date,
    decision_option: str,
    owner_options_payload: Mapping[str, Any],
    log_path: Path = owner_log.DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    dry_run_decision_id: str | None = None,
) -> dict[str, Any]:
    if decision_option not in OWNER_DECISION_OPTIONS:
        raise ValueError(
            "decision_option must be one of: " + ", ".join(OWNER_DECISION_OPTIONS)
        )
    linked_artifacts = _linked_artifacts_from_options_packet(owner_options_payload)
    proposed = {
        "decision_id": dry_run_decision_id
        or f"TRADING-434_dry_run_{decision_option}_{as_of.isoformat()}",
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "candidate_id": _text(
            _mapping(owner_options_payload.get("summary")).get("candidate_id"),
            "median_plus_regime_mismatch_filter",
        ),
        "input_artifacts": linked_artifacts,
        "owner_action": decision_option,
        "reason_summary": f"Dry-run proposed owner decision option: {decision_option}.",
        "safety_status": "SAFETY_PASS_WITH_WARNINGS",
        "next_action": _owner_option_next_action(decision_option),
        "production_effect": PRODUCTION_EFFECT,
    }
    normalized = owner_log.normalize_owner_decision_record(
        proposed,
        source_record_path=None,
    )
    record_validation = owner_log.validate_owner_decision_record(normalized)
    existing = owner_log.read_owner_decision_audit_log(log_path)
    existing_ids = {
        _text(_mapping(row.get("record")).get("decision_id"))
        for row in _records(existing.get("entries"))
    }
    duplicate = _text(normalized.get("decision_id")) in existing_ids
    append_only_checks = [
        {
            "check_id": "dry_run_does_not_write_real_entry",
            "status": PASS_STATUS,
            "message": "Dry-run does not append to owner decision audit log.",
        },
        {
            "check_id": "proposed_decision_id_unique",
            "status": FAIL_STATUS if duplicate else PASS_STATUS,
            "message": "Proposed decision id must not duplicate existing audit log entries.",
        },
    ]
    option_allowed = _option_allowed_by_current_gates(
        decision_option,
        owner_options_payload,
    )
    status = (
        "OWNER_DECISION_DRY_RUN_BLOCKED"
        if record_validation["status"] == FAIL_STATUS or duplicate
        else (
            "OWNER_DECISION_DRY_RUN_VALID"
            if option_allowed
            else "OWNER_DECISION_DRY_RUN_VALID_WITH_GATE_BLOCKERS"
        )
    )
    summary = {
        "dry_run_status": status,
        "decision_option": decision_option,
        "record_validation_status": record_validation["status"],
        "option_allowed_by_current_gates": option_allowed,
        "would_append": False,
        "real_entry_written": False,
        "existing_log_entry_count": len(_records(existing.get("entries"))),
        "duplicate_decision_id": duplicate,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=OWNER_DRY_RUN_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Dry-run an owner decision audit entry without appending it.",
        input_artifacts={
            "owner_decision_options_packet": _artifact_id_from_payload(
                owner_options_payload
            ),
            "owner_decision_audit_log": str(log_path),
        },
        output_decision=status,
        summary=summary,
        body={
            "proposed_decision_entry": normalized,
            "linked_artifacts": linked_artifacts,
            "append_only_validation": append_only_checks,
            "record_validation": record_validation,
            "consequences": _owner_option_consequences(decision_option),
        },
        reader_brief=_reader_brief(
            summary=f"Owner decision dry-run is {status}; no real entry was written.",
            key_result=status,
            blocking_issues=(
                "none"
                if status != "OWNER_DECISION_DRY_RUN_BLOCKED"
                else _issue_list(record_validation.get("blocking_issues"))
            ),
            warnings=(
                "current gates do not allow this option"
                if not option_allowed
                else "none"
            ),
            next_action="append_only_after_explicit_owner_instruction",
        ),
        next_action="append_only_after_explicit_owner_instruction",
    )


def build_observation_clock_readiness_plan_payload(
    *,
    as_of: date,
    gate_gap_payload: Mapping[str, Any],
    recovery_pack_payload: Mapping[str, Any],
) -> dict[str, Any]:
    gate_summary = _mapping(gate_gap_payload.get("summary"))
    source_by_id = _source_reports_by_id(recovery_pack_payload)
    owner_action = _owner_action_from_recovery(recovery_pack_payload, source_by_id)
    fail_conditions = _records(gate_gap_payload.get("blocking_conditions"))
    cannot_start_reasons = []
    if gate_summary.get("normal_paper_shadow_may_resume") is not True:
        cannot_start_reasons.append("normal_paper_shadow_is_blocked")
    if owner_action in {"hold", "keep_hold", ""}:
        cannot_start_reasons.append("owner_action_is_hold")
    for condition in fail_conditions:
        condition_id = _text(condition.get("condition_id"))
        if condition_id in {"readiness", "paper_shadow_health", "signal_input_completeness"}:
            cannot_start_reasons.append(f"{condition_id}_still_blocked_or_warning")
    if not cannot_start_reasons:
        status = "OBSERVATION_CLOCK_READY_TO_START_AFTER_FIRST_VALID_DAILY_ARTIFACT"
    else:
        status = "OBSERVATION_CLOCK_NOT_READY"
    start_conditions = [
        {
            "condition_id": "normal_paper_shadow_gate_allowed",
            "required_value": "NORMAL_SHADOW_GATE_READY",
            "current_value": gate_summary.get("gap_status"),
            "status": (
                "PASS"
                if gate_summary.get("gap_status") == "NORMAL_SHADOW_GATE_READY"
                else "FAIL"
            ),
        },
        {
            "condition_id": "owner_approves_resume_normal_shadow",
            "required_value": "approve_resume_normal_shadow or continue_normal_shadow",
            "current_value": owner_action,
            "status": (
                "PASS"
                if owner_action in {"approve_resume_normal_shadow", "continue_normal_shadow"}
                else "FAIL"
            ),
        },
        {
            "condition_id": "first_valid_daily_paper_shadow_artifact_after_approval",
            "required_value": "valid daily artifact generated after approval timestamp",
            "current_value": "not_started",
            "status": "FAIL",
        },
    ]
    summary = {
        "readiness_plan_status": status,
        "owner_action": owner_action,
        "normal_gate_gap_status": gate_summary.get("gap_status"),
        "cannot_start_reason_count": len(cannot_start_reasons),
        "observation_clock_should_start_now": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=OBSERVATION_READINESS_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Clarify when the normal paper-shadow observation clock can start.",
        input_artifacts={
            "normal_shadow_gate_gap_analysis": _artifact_id_from_payload(gate_gap_payload),
        },
        output_decision=status,
        summary=summary,
        body={
            "cannot_start_reasons": cannot_start_reasons,
            "start_conditions": start_conditions,
        },
        reader_brief=_reader_brief(
            summary=(
                f"Observation clock readiness is {status}; "
                f"cannot_start={len(cannot_start_reasons)}."
            ),
            key_result=status,
            blocking_issues="; ".join(cannot_start_reasons) or "none",
            warnings="clock must not start before owner approval and first valid daily artifact",
            next_action="do_not_start_clock_until_start_conditions_pass",
        ),
        next_action="do_not_start_clock_until_start_conditions_pass",
    )


def build_post_decision_rerun_plan_payload(
    *,
    as_of: date,
    owner_options_payload: Mapping[str, Any],
) -> dict[str, Any]:
    branches = [
        _rerun_branch(
            "keep_hold",
            [
                "rerun governance monitoring",
                "do not resume paper-shadow",
            ],
        ),
        _rerun_branch(
            "approve_resume_normal_shadow",
            [
                "rerun daily paper-shadow after owner approval and non-blocking gate",
                "rerun drift monitor",
                "rerun weekly review when enough valid days exist",
                "start observation clock after first valid daily artifact",
            ],
        ),
        _rerun_branch(
            "return_to_research",
            [
                "produce candidate research return plan",
                "stop promotion checks for this candidate",
            ],
        ),
        _rerun_branch(
            "reject_candidate",
            [
                "generate rejection postmortem",
                "archive candidate after owner decision",
            ],
        ),
    ]
    status = "POST_DECISION_RERUN_PLAN_READY"
    summary = {
        "rerun_plan_status": status,
        "branch_count": len(branches),
        "extended_shadow_allowed_in_any_branch": False,
        "live_trading_allowed_in_any_branch": False,
        "official_target_weights_allowed_in_any_branch": False,
        "broker_order_allowed_in_any_branch": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=POST_DECISION_RERUN_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Prepare rerun plans for each possible owner decision.",
        input_artifacts={
            "owner_decision_options_packet": _artifact_id_from_payload(
                owner_options_payload
            ),
        },
        output_decision=status,
        summary=summary,
        body={"rerun_branches": branches},
        reader_brief=_reader_brief(
            summary="Post-decision rerun plan is ready; all branches keep live trading forbidden.",
            key_result=status,
            blocking_issues="none",
            warnings=(
                "approve_resume_normal_shadow branch still requires non-blocking "
                "gate and explicit owner approval"
            ),
            next_action="use_after_explicit_owner_decision",
        ),
        next_action="use_after_explicit_owner_decision",
    )


def build_report_quality_warning_drilldown_payload(
    *,
    as_of: date,
    report_quality_gate_payload: Mapping[str, Any],
    report_quality_gate_path: Path | None = None,
) -> dict[str, Any]:
    warnings = [
        _quality_warning(row)
        for row in _records(report_quality_gate_payload.get("warning_quality_issues"))
    ]
    counts = Counter(row["classification"] for row in warnings)
    summary = {
        "drilldown_status": "REPORT_QUALITY_WARNING_DRILLDOWN_READY",
        "source_report_quality_status": _text(
            report_quality_gate_payload.get("report_quality_status"),
            _text(report_quality_gate_payload.get("status"), "UNKNOWN"),
        ),
        "source_warning_count": len(warnings),
        "blocking_quality_issue_count": len(
            _records(report_quality_gate_payload.get("blocking_quality_issues"))
        ),
        "classification_counts": dict(counts),
        "safe_metadata_template_fix_applied": (
            "report_quality_gate Markdown next_action alias recognized by code update"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=REPORT_QUALITY_DRILLDOWN_REPORT_TYPE,
        as_of=as_of,
        status=summary["drilldown_status"],
        purpose="Drill into report quality PASS_WITH_WARNINGS without hiding legitimate warnings.",
        input_artifacts={
            "report_quality_gate": _path_text(report_quality_gate_path),
        },
        output_decision=summary["drilldown_status"],
        summary=summary,
        body={"warning_drilldown": warnings},
        reader_brief=_reader_brief(
            summary=(
                f"Report quality warning drilldown listed {len(warnings)} warnings; "
                f"blocking={summary['blocking_quality_issue_count']}."
            ),
            key_result=summary["drilldown_status"],
            blocking_issues="none"
            if summary["blocking_quality_issue_count"] == 0
            else f"blocking={summary['blocking_quality_issue_count']}",
            warnings="; ".join(
                f"{key}={value}" for key, value in sorted(counts.items())
            )
            or "none",
            next_action="keep_legitimate_quality_warnings_visible",
        ),
        next_action="keep_legitimate_quality_warnings_visible",
    )


def build_governance_status_snapshot_payload(
    *,
    as_of: date,
    eight_blocker_payload: Mapping[str, Any],
    gate_gap_payload: Mapping[str, Any],
    promotion_review_payload: Mapping[str, Any],
    candidate_assessment_payload: Mapping[str, Any],
    owner_options_payload: Mapping[str, Any],
    observation_plan_payload: Mapping[str, Any],
    report_quality_drilldown_payload: Mapping[str, Any],
) -> dict[str, Any]:
    eight_summary = _mapping(eight_blocker_payload.get("summary"))
    gate_summary = _mapping(gate_gap_payload.get("summary"))
    options_summary = _mapping(owner_options_payload.get("summary"))
    quality_summary = _mapping(report_quality_drilldown_payload.get("summary"))
    status = "GOVERNANCE_STATUS_SNAPSHOT_BLOCKED"
    summary = {
        "governance_snapshot_status": status,
        "current_governance_status": eight_summary.get("recovery_governance_status"),
        "blocker_count": _int(eight_summary.get("remaining_blocker_count")),
        "warning_count": _int(eight_summary.get("remaining_warning_count")),
        "report_quality_warning_count": _int(quality_summary.get("source_warning_count")),
        "recommended_owner_action": options_summary.get("recommended_owner_action"),
        "normal_shadow_may_resume": gate_summary.get("gap_status")
        == "NORMAL_SHADOW_GATE_READY",
        "extended_shadow_remains_forbidden": True,
        "live_trading_remains_forbidden": True,
        "official_target_weights_forbidden": True,
        "broker_order_forbidden": True,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _decision_payload(
        report_type=GOVERNANCE_SNAPSHOT_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Create a clean governance status snapshot after decision-stage review.",
        input_artifacts={
            "eight_blocker_decision_review": _artifact_id_from_payload(eight_blocker_payload),
            "normal_shadow_gate_gap_analysis": _artifact_id_from_payload(gate_gap_payload),
            "promotion_blocker_after_metrics_review": _artifact_id_from_payload(
                promotion_review_payload
            ),
            "candidate_research_return_assessment": _artifact_id_from_payload(
                candidate_assessment_payload
            ),
            "owner_decision_options_packet": _artifact_id_from_payload(
                owner_options_payload
            ),
            "observation_clock_readiness_plan": _artifact_id_from_payload(
                observation_plan_payload
            ),
            "report_quality_warning_drilldown": _artifact_id_from_payload(
                report_quality_drilldown_payload
            ),
        },
        output_decision=summary["recommended_owner_action"],
        summary=summary,
        body={
            "current_governance_status": summary,
            "source_reader_briefs": {
                "eight_blocker_decision_review": _mapping(
                    eight_blocker_payload.get("reader_brief")
                ),
                "normal_shadow_gate_gap_analysis": _mapping(
                    gate_gap_payload.get("reader_brief")
                ),
                "promotion_blocker_after_metrics_review": _mapping(
                    promotion_review_payload.get("reader_brief")
                ),
                "candidate_research_return_assessment": _mapping(
                    candidate_assessment_payload.get("reader_brief")
                ),
                "owner_decision_options_packet": _mapping(
                    owner_options_payload.get("reader_brief")
                ),
            },
        },
        reader_brief=_reader_brief(
            summary=(
                f"Governance snapshot is blocked: blockers={summary['blocker_count']}, "
                f"warnings={summary['warning_count']}, "
                f"recommended_owner_action={summary['recommended_owner_action']}."
            ),
            key_result=status,
            blocking_issues=f"blocker_count={summary['blocker_count']}",
            warnings=(
                f"warning_count={summary['warning_count']}; "
                f"report_quality_warning_count={summary['report_quality_warning_count']}"
            ),
            next_action="manual_owner_review_keep_hold_or_return_to_research",
        ),
        next_action="manual_owner_review_keep_hold_or_return_to_research",
    )


def build_decision_stage_review_payloads(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    report_index_path: Path | None = None,
    recovery_pack_path: Path | None = None,
    report_quality_gate_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
    owner_decision_log_path: Path = owner_log.DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    dry_run_decision_option: str = "keep_hold",
) -> dict[str, dict[str, Any]]:
    report_index_path = report_index_path or reports_dir / f"report_index_{as_of.isoformat()}.json"
    report_index_payload = _read_json_mapping(report_index_path)
    recovery_pack_path = recovery_pack_path or (
        reports_dir / f"research_governance_recovery_pack_{as_of.isoformat()}.json"
    )
    if not recovery_pack_path.exists():
        latest_recovery = latest_research_governance_recovery_pack_json_path(reports_dir)
        if latest_recovery is not None:
            recovery_pack_path = latest_recovery
    recovery_pack_payload = _read_json_mapping(recovery_pack_path)
    source_by_id = _source_reports_by_id(recovery_pack_payload)
    normal_gate_payload = _read_source_payload(
        source_by_id.get("normal_paper_shadow_resumption_gate", {})
    )
    report_quality_gate_path = report_quality_gate_path or (
        reports_dir / f"report_quality_gate_{as_of.isoformat()}.json"
    )
    report_quality_gate_payload = _read_json_mapping(report_quality_gate_path)

    eight = build_eight_blocker_decision_review_payload(
        as_of=as_of,
        recovery_pack_payload=recovery_pack_payload,
        recovery_pack_path=recovery_pack_path,
    )
    gate = build_normal_shadow_gate_gap_analysis_payload(
        as_of=as_of,
        recovery_pack_payload=recovery_pack_payload,
        normal_gate_payload=normal_gate_payload,
        report_index_payload=report_index_payload,
        recovery_pack_path=recovery_pack_path,
    )
    promotion = build_promotion_blocker_review_payload(
        as_of=as_of,
        recovery_pack_payload=recovery_pack_payload,
        recovery_pack_path=recovery_pack_path,
    )
    candidate = build_candidate_research_return_assessment_payload(
        as_of=as_of,
        report_index_payload=report_index_payload,
        recovery_pack_payload=recovery_pack_payload,
        project_root=project_root,
    )
    options = build_owner_decision_options_packet_payload(
        as_of=as_of,
        eight_blocker_payload=eight,
        gate_gap_payload=gate,
        promotion_review_payload=promotion,
        candidate_assessment_payload=candidate,
    )
    dry_run = build_owner_decision_dry_run_payload(
        as_of=as_of,
        decision_option=dry_run_decision_option,
        owner_options_payload=options,
        log_path=owner_decision_log_path,
    )
    observation = build_observation_clock_readiness_plan_payload(
        as_of=as_of,
        gate_gap_payload=gate,
        recovery_pack_payload=recovery_pack_payload,
    )
    rerun = build_post_decision_rerun_plan_payload(
        as_of=as_of,
        owner_options_payload=options,
    )
    quality = build_report_quality_warning_drilldown_payload(
        as_of=as_of,
        report_quality_gate_payload=report_quality_gate_payload,
        report_quality_gate_path=report_quality_gate_path,
    )
    snapshot = build_governance_status_snapshot_payload(
        as_of=as_of,
        eight_blocker_payload=eight,
        gate_gap_payload=gate,
        promotion_review_payload=promotion,
        candidate_assessment_payload=candidate,
        owner_options_payload=options,
        observation_plan_payload=observation,
        report_quality_drilldown_payload=quality,
    )
    return {
        EIGHT_BLOCKER_REPORT_TYPE: eight,
        NORMAL_GATE_GAP_REPORT_TYPE: gate,
        PROMOTION_BLOCKER_REPORT_TYPE: promotion,
        CANDIDATE_ASSESSMENT_REPORT_TYPE: candidate,
        OWNER_OPTIONS_REPORT_TYPE: options,
        OWNER_DRY_RUN_REPORT_TYPE: dry_run,
        OBSERVATION_READINESS_REPORT_TYPE: observation,
        POST_DECISION_RERUN_REPORT_TYPE: rerun,
        REPORT_QUALITY_DRILLDOWN_REPORT_TYPE: quality,
        GOVERNANCE_SNAPSHOT_REPORT_TYPE: snapshot,
    }


def write_decision_stage_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_decision_stage_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_decision_stage_markdown(payload), encoding="utf-8")
    return output_path


def render_decision_stage_markdown(payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_title(report_type)} {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- status: {payload.get('status')}",
        f"- output_decision: {payload.get('output_decision')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- next_action: {payload.get('next_action')}",
    ]
    for key, value in summary.items():
        if key == "classification_counts" and isinstance(value, Mapping):
            lines.append(
                "- classification_counts: "
                + "; ".join(f"{item_key}={item_value}" for item_key, item_value in value.items())
            )
        elif isinstance(value, (str, int, float, bool)) or value is None:
            lines.append(f"- {key}: {value}")
    lines.extend(["", "## Reader Brief", ""])
    for key, value in _mapping(payload.get("reader_brief")).items():
        lines.append(f"- {key}: {_md_cell(value)}")
    if report_type == EIGHT_BLOCKER_REPORT_TYPE:
        lines.extend(_table_exact_blockers(payload))
    elif report_type == NORMAL_GATE_GAP_REPORT_TYPE:
        lines.extend(_table_gate_conditions(payload))
    elif report_type == PROMOTION_BLOCKER_REPORT_TYPE:
        lines.extend(_table_reasons(payload, "promotion_blocker_reasons"))
    elif report_type == OWNER_OPTIONS_REPORT_TYPE:
        lines.extend(_table_owner_options(payload))
    elif report_type == OWNER_DRY_RUN_REPORT_TYPE:
        lines.extend(_dry_run_markdown(payload))
    elif report_type == REPORT_QUALITY_DRILLDOWN_REPORT_TYPE:
        lines.extend(_table_quality_warnings(payload))
    elif report_type == POST_DECISION_RERUN_REPORT_TYPE:
        lines.extend(_table_rerun_branches(payload))
    else:
        lines.extend(_generic_body_markdown(payload))
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "|field|value|",
            "|---|---|",
        ]
    )
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def _blocker_review(
    blocker: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    source_id = _text(blocker.get("source_id"))
    source_payload = _read_source_payload(source)
    field = BLOCKING_FIELD_BY_SOURCE_ID.get(source_id, "source_status")
    primary, secondary = BLOCKER_CLASSIFICATION_BY_SOURCE_ID.get(
        source_id,
        ("report-quality blocker", ()),
    )
    current_value = _field_value(source_payload, field) or _text(
        source.get("source_status"),
        _text(blocker.get("source_status")),
    )
    return {
        "source_id": source_id,
        "blocker_classification": primary,
        "secondary_classifications": list(secondary),
        "source_report": _text(source.get("report_id"), source_id),
        "source_artifact_id": _artifact_id(source_payload, source),
        "source_artifact_path": _text(source.get("source_payload_path")),
        "exact_blocking_field": field,
        "exact_current_value": current_value,
        "required_next_action": _text(
            blocker.get("recommended_action"),
            _text(source.get("next_action"), "review_decision_stage_blocker"),
        ),
        "can_be_fixed_by_code_data_regeneration": _code_data_fixable(source_id, current_value),
        "requires_owner_judgment": _requires_owner_judgment(source_id, current_value),
        "candidate_should_return_to_research": _candidate_should_return(source_id, current_value),
        "candidate_return_reason": _candidate_return_reason(source_id, current_value),
        "message": _text(blocker.get("message")),
        "validation_status": _text(source.get("validation_status")),
        "production_effect": _text(source.get("production_effect"), PRODUCTION_EFFECT),
    }


def _source_reports_by_id(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _text(row.get("source_id")): dict(row)
        for row in _records(payload.get("source_reports"))
        if _text(row.get("source_id"))
    }


def _read_source_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    payload = _read_optional_json_mapping(Path(_text(source.get("source_payload_path"))))
    if payload:
        return payload
    summary = _mapping(source.get("summary"))
    return {**summary, "status": _text(source.get("source_status"))}


def _field_value(payload: Mapping[str, Any], field: str) -> str:
    summary = _mapping(payload.get("summary"))
    return _text(payload.get(field), _text(summary.get(field)))


def _artifact_id(payload: Mapping[str, Any], source: Mapping[str, Any]) -> str:
    for key in (
        "artifact_id",
        "gate_id",
        "review_id",
        "control_id",
        "pack_id",
        "dashboard_id",
        "clock_id",
        "protocol_id",
        "run_id",
    ):
        value = _text(payload.get(key), _text(_mapping(payload.get("summary")).get(key)))
        if value:
            return value
    path_text = _text(source.get("source_payload_path"))
    if path_text:
        return Path(path_text).parent.name
    return _text(source.get("source_id"), "UNKNOWN")


def _code_data_fixable(source_id: str, current_value: str) -> bool:
    if current_value in {"MISSING", "BLOCKED_SOURCE", "INSUFFICIENT_COST_INPUTS"}:
        return True
    return source_id in {"roadmap_dashboard"} and "MISSING" in current_value


def _requires_owner_judgment(source_id: str, current_value: str) -> bool:
    if source_id in {
        "normal_paper_shadow_resumption_gate",
        "monthly_review",
        "promotion_board",
        "extended_shadow_protocol",
        "roadmap_dashboard",
    }:
        return True
    if "UNDERPERFORMS" in current_value or "NOT_MEANINGFUL" in current_value:
        return True
    return False


def _candidate_should_return(source_id: str, current_value: str) -> bool:
    return source_id in {
        "cost_sensitivity_metrics",
        "benchmark_baseline_metrics",
        "monthly_review",
        "promotion_board",
    } and (
        "NOT_MEANINGFUL" in current_value
        or "UNDERPERFORMS" in current_value
        or current_value in {"MONTHLY_REVIEW_BLOCKED", "HOLD_FOR_MORE_DATA"}
    )


def _candidate_return_reason(source_id: str, current_value: str) -> str:
    if "NOT_MEANINGFUL" in current_value:
        return "net improvement does not survive configured costs"
    if "UNDERPERFORMS" in current_value:
        return "candidate underperforms required benchmark baselines"
    if source_id == "monthly_review":
        return "monthly review still carries cost/benchmark blockers"
    if source_id == "promotion_board":
        return "promotion board has unresolved required evidence blockers"
    return "not a direct research-return signal"


def _gate_condition_from_requirement(
    condition_id: str,
    requirement_id: str,
    requirements: Mapping[str, Mapping[str, Any]],
    source: Mapping[str, Any],
    *,
    required_value: str,
    owner_action_required: bool = False,
) -> dict[str, Any]:
    requirement = _mapping(requirements.get(requirement_id))
    req_status = _text(requirement.get("status"), "MISSING")
    status = "PASS"
    if req_status == "BLOCKED" or req_status == "MISSING":
        status = "FAIL"
    elif req_status == "WARNING":
        status = "WARNING"
    return {
        "condition_id": condition_id,
        "requirement_id": requirement_id,
        "required_value": required_value,
        "actual_value": _text(requirement.get("detail"), _text(source.get("source_status"))),
        "status": status,
        "upstream_artifact_dependency": _text(source.get("source_id")),
        "upstream_artifact_path": _text(source.get("source_payload_path")),
        "owner_action_required": owner_action_required,
        "production_effect": PRODUCTION_EFFECT,
    }


def _gate_condition_from_source(
    condition_id: str,
    required_value: str,
    actual_value: str,
    status: str,
    dependency: str,
    path: str,
    *,
    owner_action_required: bool = False,
) -> dict[str, Any]:
    return {
        "condition_id": condition_id,
        "required_value": required_value,
        "actual_value": actual_value,
        "status": status,
        "upstream_artifact_dependency": dependency,
        "upstream_artifact_path": path,
        "owner_action_required": owner_action_required,
        "production_effect": PRODUCTION_EFFECT,
    }


def _cost_benchmark_gate_status(source_by_id: Mapping[str, Mapping[str, Any]]) -> str:
    cost = _text(source_by_id.get("cost_sensitivity_metrics", {}).get("source_status"))
    benchmark = _text(source_by_id.get("benchmark_baseline_metrics", {}).get("source_status"))
    if (
        cost in {"MEANINGFUL_ALL_SCENARIOS", "MEANINGFUL_LOW_MEDIUM_ONLY"}
        and benchmark in {"CANDIDATE_OUTPERFORMS_BASELINES", "MIXED_BASELINE_RESULT"}
    ):
        return "PASS"
    if "INSUFFICIENT" in cost or "INSUFFICIENT" in benchmark:
        return "WARNING"
    return "FAIL"


def _pass_warning_fail(
    value: str,
    *,
    pass_values: set[str],
    warning_values: set[str],
) -> str:
    if value in pass_values:
        return "PASS"
    if value in warning_values:
        return "WARNING"
    return "FAIL"


def _report_index_actual_value(
    report_index_payload: Mapping[str, Any] | None,
    recovery_pack_payload: Mapping[str, Any],
) -> str:
    if report_index_payload:
        return (
            f"{_text(report_index_payload.get('status'))}; "
            f"unwaived={_int(_mapping(report_index_payload.get('summary')).get('unwaived_warning_count'))}"
        )
    visibility = _mapping(recovery_pack_payload.get("report_index_visibility"))
    return (
        f"{_text(visibility.get('report_index_status'))}; "
        f"unwaived={_int(visibility.get('unwaived_warning_count'))}"
    )


def _report_index_condition_status(
    report_index_payload: Mapping[str, Any] | None,
    recovery_pack_payload: Mapping[str, Any],
) -> str:
    if report_index_payload:
        status = _text(report_index_payload.get("status"))
        unwaived = _int(_mapping(report_index_payload.get("summary")).get("unwaived_warning_count"))
    else:
        visibility = _mapping(recovery_pack_payload.get("report_index_visibility"))
        status = _text(visibility.get("report_index_status"))
        unwaived = _int(visibility.get("unwaived_warning_count"))
    if status in {"PASS", "PASS_WITH_EXPLICIT_WAIVERS"} and unwaived == 0:
        return "PASS"
    if "WARNING" in status and unwaived == 0:
        return "WARNING"
    return "FAIL"


def _join_paths(*sources: Mapping[str, Any]) -> str:
    return ";".join(_text(source.get("source_payload_path")) for source in sources if source)


def _owner_action_from_recovery(
    recovery_pack_payload: Mapping[str, Any],
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> str:
    boundary = _mapping(recovery_pack_payload.get("normal_paper_shadow_boundary"))
    owner = _text(boundary.get("owner_action"))
    if owner:
        return owner
    owner_source = source_by_id.get("owner_hold_decision", {})
    payload = _read_source_payload(owner_source)
    return _text(_mapping(payload.get("summary")).get("latest_owner_action"), "hold")


def _reason_flag(reason_id: str, applies: bool, actual_value: str) -> dict[str, Any]:
    return {
        "reason_id": reason_id,
        "applies": applies,
        "actual_value": actual_value,
        "production_effect": PRODUCTION_EFFECT,
    }


def _promotion_recommendation(reasons: Sequence[Mapping[str, Any]]) -> str:
    applies = {str(row.get("reason_id")) for row in reasons if row.get("applies") is True}
    if {
        "cost_metrics_available_but_unfavorable",
        "benchmark_metrics_available_but_unfavorable",
    } & applies:
        return "RETURN_TO_RESEARCH"
    if applies:
        return "HOLD_FOR_MORE_DATA"
    return "CONTINUE_NORMAL_SHADOW"


def _evidence_sufficiency_status(
    cost_metrics_status: str,
    benchmark_metrics_status: str,
    cost_status: str,
    benchmark_status: str,
) -> str:
    if (
        cost_metrics_status == "COST_INPUTS_AVAILABLE"
        and benchmark_metrics_status == "BASELINE_METRICS_AVAILABLE"
    ):
        if "NOT_MEANINGFUL" in cost_status or "UNDERPERFORMS" in benchmark_status:
            return "METRICS_AVAILABLE_BUT_UNFAVORABLE"
        return "METRICS_AVAILABLE"
    return "METRICS_INSUFFICIENT_OR_MISSING"


def _report_index_source_evidence(
    report_index_payload: Mapping[str, Any],
    spec: Mapping[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    entry = _report_index_entry(report_index_payload, _text(spec.get("report_id")))
    artifact_path = _resolve_artifact_path(_text(entry.get("latest_artifact_path")), project_root)
    payload_path, payload = _read_preferred_payload(
        artifact_path,
        _list_values(spec.get("preferred_json_names")),
    )
    status = _status_from_payload(
        payload,
        entry,
        _list_values(spec.get("status_fields")),
    )
    return {
        "source_id": _text(spec.get("source_id")),
        "report_id": _text(spec.get("report_id")),
        "availability": (
            "AVAILABLE" if artifact_path is not None and artifact_path.exists() else "MISSING"
        ),
        "status": status,
        "artifact_id": _artifact_id(payload, {"source_payload_path": _path_text(payload_path)}),
        "artifact_path": _path_text(artifact_path),
        "payload_path": _path_text(payload_path),
        "candidate_id": _candidate_id_from_payload(payload),
        "summary": _compact_summary(_mapping(payload.get("summary"))),
        "production_effect": _text(
            payload.get("production_effect"),
            _text(entry.get("production_effect"), PRODUCTION_EFFECT),
        ),
    }


def _read_preferred_payload(
    artifact_path: Path | None,
    preferred_names: Sequence[str],
) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates = [artifact_path.parent / name for name in preferred_names]
    candidates.append(
        artifact_path if artifact_path.suffix == ".json" else artifact_path.with_suffix(".json")
    )
    seen: set[str] = set()
    for candidate in candidates:
        if str(candidate) in seen:
            continue
        seen.add(str(candidate))
        payload = _read_optional_json_mapping(candidate)
        if payload:
            return candidate, payload
    return None, {}


def _status_from_payload(
    payload: Mapping[str, Any],
    entry: Mapping[str, Any],
    status_fields: Sequence[str],
) -> str:
    summary = _mapping(payload.get("summary"))
    for field in status_fields:
        value = _text(payload.get(field), _text(summary.get(field)))
        if value:
            return value
    for field in ("status", "artifact_status", "freshness_status"):
        value = _text(entry.get(field))
        if value:
            return value
    return "MISSING" if not entry else "UNKNOWN"


def _candidate_id_from_payload(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    for key in ("candidate_id", "candidate"):
        value = _text(payload.get(key), _text(summary.get(key)))
        if value:
            return value
    return ""


def _candidate_id_from_evidence(evidence: Sequence[Mapping[str, Any]]) -> str:
    for row in evidence:
        value = _text(row.get("candidate_id"))
        if value:
            return value
    return ""


def _compact_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            compact[_text(key)] = value
        if len(compact) >= 12:
            break
    return compact


def _candidate_decision(
    *,
    cost_status: str,
    benchmark_status: str,
    normal_gate_status: str,
    owner_action: str,
) -> str:
    if "REJECT" in benchmark_status:
        return "REJECT_CANDIDATE"
    if "NOT_MEANINGFUL" in cost_status or "UNDERPERFORMS" in benchmark_status:
        return "RETURN_TO_RESEARCH"
    if normal_gate_status in {"RESUME_NORMAL_SHADOW_ALLOWED", "RESUME_NORMAL_SHADOW_WITH_WARNINGS"}:
        return "RESUME_NORMAL_SHADOW_CANDIDATE"
    if owner_action in {"hold", "keep_hold", ""}:
        return "CONTINUE_HOLD"
    return "CONTINUE_HOLD"


def _strongest_evidence_for(evidence: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    favorable: list[dict[str, Any]] = []
    for row in evidence:
        status = _text(row.get("status")).upper()
        if any(
            marker in status
            for marker in ("PASS", "READY", "CONFIRMED", "REDUCTION", "AVAILABLE")
        ):
            favorable.append(
                {
                    "source_id": row.get("source_id"),
                    "status": row.get("status"),
                    "artifact_path": row.get("payload_path") or row.get("artifact_path"),
                }
            )
        if len(favorable) >= 5:
            break
    return favorable


def _strongest_evidence_against(
    *,
    cost_status: str,
    benchmark_status: str,
    normal_gate_status: str,
) -> list[str]:
    items: list[str] = []
    if cost_status:
        items.append(f"cost_sensitivity_status={cost_status}")
    if benchmark_status:
        items.append(f"benchmark_baseline_status={benchmark_status}")
    if normal_gate_status:
        items.append(f"normal_gate_status={normal_gate_status}")
    return items


def _unresolved_uncertainty(
    evidence: Sequence[Mapping[str, Any]],
    recovery_pack_payload: Mapping[str, Any],
) -> list[str]:
    missing = [row["source_id"] for row in evidence if row["availability"] != "AVAILABLE"]
    summary = _mapping(recovery_pack_payload.get("summary"))
    items = []
    if missing:
        items.append("missing_sources=" + ",".join(missing))
    if _int(summary.get("remaining_warning_count")):
        items.append(f"remaining_recovery_warnings={_int(summary.get('remaining_warning_count'))}")
    if summary.get("normal_paper_shadow_may_resume") is not True:
        items.append("normal_paper_shadow_not_resumed")
    return items


def _required_owner_action_for_candidate_decision(decision: str) -> str:
    if decision == "RETURN_TO_RESEARCH":
        return "owner_review_return_to_research_or_keep_hold"
    if decision == "REJECT_CANDIDATE":
        return "owner_review_reject_candidate_and_generate_postmortem"
    if decision == "RESUME_NORMAL_SHADOW_CANDIDATE":
        return "owner_review_approve_resume_normal_shadow_after_gate_non_blocked"
    return "owner_keep_hold_until_evidence_or_gate_changes"


def _recommended_owner_action(candidate_decision: Any, owner_action: str) -> str:
    if candidate_decision == "RETURN_TO_RESEARCH":
        return "return_to_research"
    if candidate_decision == "REJECT_CANDIDATE":
        return "reject_candidate"
    if candidate_decision == "RESUME_NORMAL_SHADOW_CANDIDATE":
        return "approve_resume_normal_shadow"
    if owner_action in {"hold", "keep_hold", ""}:
        return "keep_hold"
    return "keep_hold"


def _owner_option(
    option_id: str,
    *,
    allowed: bool,
    required_evidence: Sequence[str],
    consequences: Sequence[str],
    risks: Sequence[str],
) -> dict[str, Any]:
    return {
        "option_id": option_id,
        "required_evidence": list(required_evidence),
        "consequences": list(consequences),
        "risks": list(risks),
        "allowed_by_current_gates": allowed,
        "extended_shadow_remains_forbidden": True,
        "live_trading_remains_forbidden": True,
        "official_target_weights_forbidden": True,
        "broker_order_forbidden": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _owner_option_next_action(option: str) -> str:
    return {
        "keep_hold": "rerun_governance_monitoring_only",
        "approve_resume_normal_shadow": (
            "rerun_daily_paper_shadow_after_gate_non_blocked_and_owner_approval"
        ),
        "return_to_research": "produce_candidate_research_return_plan_and_stop_promotion_checks",
        "reject_candidate": "generate_rejection_postmortem_and_archive_candidate",
    }[option]


def _owner_option_consequences(option: str) -> list[str]:
    return {
        "keep_hold": [
            "normal paper-shadow remains paused",
            "governance monitoring continues",
        ],
        "approve_resume_normal_shadow": [
            "normal observation only can resume after gates clear",
            "extended shadow and live trading remain forbidden",
        ],
        "return_to_research": [
            "candidate leaves promotion path",
            "research plan must define next evidence requirements",
        ],
        "reject_candidate": [
            "candidate archived after postmortem",
            "future revisit requires new owner-reviewed rationale",
        ],
    }[option]


def _linked_artifacts_from_options_packet(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    artifacts = _mapping(payload.get("input_artifacts"))
    result = []
    for key, value in artifacts.items():
        result.append(
            {
                "artifact_id": _text(key),
                "artifact_path": _text(value),
                "artifact_type": "report",
            }
        )
    if not result:
        result.append(
            {
                "artifact_id": "owner_decision_options_packet",
                "artifact_path": _artifact_id_from_payload(payload),
                "artifact_type": "report",
            }
        )
    return result


def _option_allowed_by_current_gates(option: str, payload: Mapping[str, Any]) -> bool:
    options = _records(payload.get("owner_decision_options"))
    if not options:
        options = _records(_mapping(payload.get("body")).get("owner_decision_options"))
    for row in options:
        if _text(row.get("option_id")) == option:
            return row.get("allowed_by_current_gates") is True
    return option in {"keep_hold", "return_to_research", "reject_candidate"}


def _rerun_branch(option_id: str, steps: Sequence[str]) -> dict[str, Any]:
    return {
        "owner_decision_option": option_id,
        "rerun_steps": list(steps),
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_allowed": False,
        "broker_order_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _quality_warning(issue: Mapping[str, Any]) -> dict[str, Any]:
    report_id = _text(issue.get("report_id"))
    issue_id = _text(issue.get("issue_id"))
    section = _text(issue.get("section"))
    message = _text(issue.get("message"))
    classification = "legacy artifact warning"
    if _text(issue.get("scope")) == "reader_brief":
        classification = "missing optional Reader Brief field"
    elif "stale" in issue_id.lower() or "stale" in message.lower():
        classification = "stale artifact visibility warning"
    elif "owner" in report_id.lower() or "owner" in issue_id.lower():
        classification = "owner review warning"
    elif "format" in issue_id.lower() or "unsupported" in issue_id.lower():
        classification = "harmless formatting warning"
    return {
        "issue_id": issue_id,
        "classification": classification,
        "scope": _text(issue.get("scope")),
        "report_id": report_id,
        "section": section,
        "message": message,
        "recommended_action": _text(issue.get("recommended_action")),
        "artifact_path": _text(issue.get("artifact_path")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _decision_payload(
    *,
    report_type: str,
    as_of: date,
    status: str,
    purpose: str,
    input_artifacts: Mapping[str, Any],
    output_decision: str,
    summary: Mapping[str, Any],
    body: Mapping[str, Any],
    reader_brief: Mapping[str, Any],
    next_action: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "purpose": purpose,
        "input_artifacts": dict(input_artifacts),
        "output_decision": output_decision,
        "summary": dict(summary),
        **dict(body),
        "reader_brief": dict(reader_brief),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Read-only decision-stage report.",
            "Does not append owner decisions unless a separate explicit append command is used.",
            (
                "Does not generate signoff packets, start observation clocks, "
                "or mutate paper-shadow/production state."
            ),
            (
                "Does not approve extended shadow, live trading, official target "
                "weights, broker action, or orders."
            ),
        ],
        "next_action": next_action,
        "methodology": {
            "collector_mode": "read_existing_governance_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_artifacts": True,
            "does_not_write_owner_decision": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def _reader_brief(
    *,
    summary: str,
    key_result: str,
    blocking_issues: str,
    warnings: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "summary": summary,
        "key_result": key_result,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "safety_boundary": (
            "decision-stage governance only; no signoff packet, no observation-clock start, "
            "no extended shadow, no live trading, no official target weights, no broker/order, "
            "production_effect=none."
        ),
        "next_action": next_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_decision_stage_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_append_owner_decision": True,
        "normal_shadow_signoff_packet_generated": False,
        "observation_clock_started": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
    }


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("does_not_append_owner_decision") is True
        and safety.get("normal_shadow_signoff_packet_generated") is False
        and safety.get("observation_clock_started") is False
        and safety.get("candidate_state_mutated") is False
        and safety.get("paper_shadow_state_mutated") is False
        and safety.get("production_state_mutated") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("extended_shadow_approved") is False
        and safety.get("live_trading_allowed") is False
    )


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    checks.append(check)
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _issue_list(issues: Any) -> str:
    rows = _records(issues)
    return "none" if not rows else "; ".join(_text(row.get("issue_id")) for row in rows)


def _condition_list(conditions: Sequence[Mapping[str, Any]]) -> str:
    return "none" if not conditions else "; ".join(
        f"{row.get('condition_id')}={row.get('actual_value')}" for row in conditions
    )


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)


def _artifact_id_from_payload(payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"), "artifact")
    as_of = _text(payload.get("as_of"), "unknown")
    return f"{report_type}:{as_of}"


def _title(report_type: str) -> str:
    return report_type.replace("_", " ").title()


def _table_exact_blockers(payload: Mapping[str, Any]) -> list[str]:
    lines = [
        "",
        "## Exact Blockers",
        "",
        "|source_id|classification|field|current_value|next_action|owner|return_to_research|",
        "|---|---|---|---|---|---|---|",
    ]
    for row in _records(payload.get("exact_blockers")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("source_id"),
                    row.get("blocker_classification"),
                    row.get("exact_blocking_field"),
                    row.get("exact_current_value"),
                    row.get("required_next_action"),
                    row.get("requires_owner_judgment"),
                    row.get("candidate_should_return_to_research"),
                )
            )
            + "|"
        )
    return lines


def _table_gate_conditions(payload: Mapping[str, Any]) -> list[str]:
    lines = [
        "",
        "## Gate Conditions",
        "",
        "|condition_id|required|actual|status|owner_action_required|",
        "|---|---|---|---|---|",
    ]
    for row in _records(payload.get("gate_conditions")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("condition_id"),
                    row.get("required_value"),
                    row.get("actual_value"),
                    row.get("status"),
                    row.get("owner_action_required"),
                )
            )
            + "|"
        )
    return lines


def _table_reasons(payload: Mapping[str, Any], key: str) -> list[str]:
    lines = [
        "",
        "## Reasons",
        "",
        "|reason_id|applies|actual_value|",
        "|---|---|---|",
    ]
    for row in _records(payload.get(key)):
        lines.append(
            f"|{_md_cell(row.get('reason_id'))}|{_md_cell(row.get('applies'))}|"
            f"{_md_cell(row.get('actual_value'))}|"
        )
    return lines


def _table_owner_options(payload: Mapping[str, Any]) -> list[str]:
    options = _records(payload.get("owner_decision_options"))
    lines = [
        "",
        "## Owner Options",
        "",
        "|option_id|allowed_by_current_gates|consequences|risks|",
        "|---|---|---|---|",
    ]
    for row in options:
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("option_id"),
                    row.get("allowed_by_current_gates"),
                    "; ".join(_list_values(row.get("consequences"))),
                    "; ".join(_list_values(row.get("risks"))),
                )
            )
            + "|"
        )
    return lines


def _dry_run_markdown(payload: Mapping[str, Any]) -> list[str]:
    proposed = _mapping(payload.get("proposed_decision_entry"))
    return [
        "",
        "## Proposed Decision Entry",
        "",
        f"- decision_id: {proposed.get('decision_id')}",
        f"- owner_action: {proposed.get('owner_action')}",
        f"- safety_status: {proposed.get('safety_status')}",
        f"- next_action: {proposed.get('next_action')}",
        f"- real_entry_written: {_mapping(payload.get('summary')).get('real_entry_written')}",
    ]


def _table_quality_warnings(payload: Mapping[str, Any]) -> list[str]:
    lines = [
        "",
        "## Warning Drilldown",
        "",
        "|issue_id|classification|report_id|section|message|recommended_action|",
        "|---|---|---|---|---|---|",
    ]
    for row in _records(payload.get("warning_drilldown")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("issue_id"),
                    row.get("classification"),
                    row.get("report_id"),
                    row.get("section"),
                    row.get("message"),
                    row.get("recommended_action"),
                )
            )
            + "|"
        )
    return lines


def _table_rerun_branches(payload: Mapping[str, Any]) -> list[str]:
    lines = [
        "",
        "## Rerun Branches",
        "",
        "|owner_decision_option|rerun_steps|live_trading_allowed|",
        "|---|---|---|",
    ]
    for row in _records(payload.get("rerun_branches")):
        lines.append(
            f"|{_md_cell(row.get('owner_decision_option'))}|"
            f"{_md_cell('; '.join(_list_values(row.get('rerun_steps'))))}|"
            f"{_md_cell(row.get('live_trading_allowed'))}|"
        )
    return lines


def _generic_body_markdown(payload: Mapping[str, Any]) -> list[str]:
    lines = ["", "## Details", ""]
    for key, value in payload.items():
        if key in {
            "schema_version",
            "report_type",
            "as_of",
            "generated_at",
            "status",
            "production_effect",
            "manual_review_only",
            "advisory_only",
            "purpose",
            "input_artifacts",
            "output_decision",
            "summary",
            "reader_brief",
            "safety_boundary",
            "limitations",
            "next_action",
            "methodology",
        }:
            continue
        if isinstance(value, list):
            lines.append(f"### {key}")
            for item in value[:30]:
                lines.append(f"- {_md_cell(item)}")
        elif isinstance(value, Mapping):
            lines.append(f"### {key}")
            for item_key, item_value in value.items():
                lines.append(f"- {item_key}: {_md_cell(item_value)}")
    return lines


__all__ = [
    "CANDIDATE_ASSESSMENT_REPORT_TYPE",
    "EIGHT_BLOCKER_REPORT_TYPE",
    "EIGHT_BLOCKER_VALIDATION_REPORT_TYPE",
    "GOVERNANCE_SNAPSHOT_REPORT_TYPE",
    "NORMAL_GATE_GAP_REPORT_TYPE",
    "OBSERVATION_READINESS_REPORT_TYPE",
    "OWNER_DRY_RUN_REPORT_TYPE",
    "OWNER_OPTIONS_REPORT_TYPE",
    "POST_DECISION_RERUN_REPORT_TYPE",
    "PROMOTION_BLOCKER_REPORT_TYPE",
    "REPORT_PREFIXES",
    "REPORT_QUALITY_DRILLDOWN_REPORT_TYPE",
    "build_candidate_research_return_assessment_payload",
    "build_decision_stage_review_payloads",
    "build_eight_blocker_decision_review_payload",
    "build_governance_status_snapshot_payload",
    "build_normal_shadow_gate_gap_analysis_payload",
    "build_observation_clock_readiness_plan_payload",
    "build_owner_decision_dry_run_payload",
    "build_owner_decision_options_packet_payload",
    "build_post_decision_rerun_plan_payload",
    "build_promotion_blocker_review_payload",
    "build_report_quality_warning_drilldown_payload",
    "default_decision_stage_json_path",
    "default_decision_stage_markdown_path",
    "latest_decision_stage_json_path",
    "render_decision_stage_markdown",
    "validate_eight_blocker_decision_review_payload",
    "write_decision_stage_json",
    "write_decision_stage_markdown",
]
