from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports import decision_stage_review
from ai_trading_system.reports import owner_decision_audit_log as owner_log

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"

CANDIDATE_ID = "median_plus_regime_mismatch_filter"
NEXT_RESEARCH_CANDIDATE_ID = "median_plus_regime_mismatch_filter_research_redesign_v2_draft"
OWNER_DECISION_ID_PREFIX = "TRADING-439_return_to_research"
OWNER_REASON_SUMMARY = (
    "Decision-stage governance review recommends return_to_research because normal shadow "
    "remains blocked, owner action remains hold, observation clock has not started, and "
    "current candidate evidence does not justify resumption or escalation."
)

OWNER_DECISION_RECORD_REPORT_TYPE = "owner_return_to_research_decision_record"
CANDIDATE_TRANSITION_PACK_REPORT_TYPE = "candidate_return_to_research_transition_pack"
FAILURE_MODE_ATTRIBUTION_REPORT_TYPE = "candidate_failure_mode_attribution"
REUSABLE_EVIDENCE_REPORT_TYPE = "reusable_evidence_extraction"
HYPOTHESIS_BACKLOG_REPORT_TYPE = "return_to_research_hypothesis_backlog"
NEXT_CANDIDATE_SPEC_REPORT_TYPE = "next_candidate_spec_draft"
RESEARCH_BACKFILL_PLAN_REPORT_TYPE = "research_backfill_plan_for_next_candidate"
ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE = "archived_candidate_status_update"
RESEARCH_CYCLE_RESET_PACK_REPORT_TYPE = "research_cycle_reset_pack"
GOVERNANCE_SNAPSHOT_REPORT_TYPE = "return_to_research_governance_snapshot"
GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE = (
    "return_to_research_governance_snapshot_validation"
)

REPORT_PREFIXES: dict[str, str] = {
    OWNER_DECISION_RECORD_REPORT_TYPE: "owner_return_to_research_decision_record",
    CANDIDATE_TRANSITION_PACK_REPORT_TYPE: "candidate_return_to_research_transition_pack",
    FAILURE_MODE_ATTRIBUTION_REPORT_TYPE: "candidate_failure_mode_attribution",
    REUSABLE_EVIDENCE_REPORT_TYPE: "reusable_evidence_extraction",
    HYPOTHESIS_BACKLOG_REPORT_TYPE: "return_to_research_hypothesis_backlog",
    NEXT_CANDIDATE_SPEC_REPORT_TYPE: "next_candidate_spec_draft",
    RESEARCH_BACKFILL_PLAN_REPORT_TYPE: "research_backfill_plan_for_next_candidate",
    ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE: "archived_candidate_status_update",
    RESEARCH_CYCLE_RESET_PACK_REPORT_TYPE: "research_cycle_reset_pack",
    GOVERNANCE_SNAPSHOT_REPORT_TYPE: "return_to_research_governance_snapshot",
    GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE: (
        "return_to_research_governance_snapshot_validation"
    ),
}

RESET_REPORT_TYPES: tuple[str, ...] = (
    OWNER_DECISION_RECORD_REPORT_TYPE,
    CANDIDATE_TRANSITION_PACK_REPORT_TYPE,
    FAILURE_MODE_ATTRIBUTION_REPORT_TYPE,
    REUSABLE_EVIDENCE_REPORT_TYPE,
    HYPOTHESIS_BACKLOG_REPORT_TYPE,
    NEXT_CANDIDATE_SPEC_REPORT_TYPE,
    RESEARCH_BACKFILL_PLAN_REPORT_TYPE,
    ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE,
    RESEARCH_CYCLE_RESET_PACK_REPORT_TYPE,
    GOVERNANCE_SNAPSHOT_REPORT_TYPE,
)

DECISION_STAGE_INPUT_TYPES: tuple[str, ...] = (
    decision_stage_review.EIGHT_BLOCKER_REPORT_TYPE,
    decision_stage_review.NORMAL_GATE_GAP_REPORT_TYPE,
    decision_stage_review.PROMOTION_BLOCKER_REPORT_TYPE,
    decision_stage_review.CANDIDATE_ASSESSMENT_REPORT_TYPE,
    decision_stage_review.OWNER_OPTIONS_REPORT_TYPE,
    decision_stage_review.OWNER_DRY_RUN_REPORT_TYPE,
    decision_stage_review.OBSERVATION_READINESS_REPORT_TYPE,
    decision_stage_review.POST_DECISION_RERUN_REPORT_TYPE,
    decision_stage_review.REPORT_QUALITY_DRILLDOWN_REPORT_TYPE,
    decision_stage_review.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
)


def default_return_to_research_json_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.json"


def default_return_to_research_markdown_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.md"


def latest_return_to_research_json_path(report_type: str, output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, f"{REPORT_PREFIXES[report_type]}_", ".json")


def default_owner_return_to_research_decision_source_path(
    decision_dir: Path,
    as_of: date,
) -> Path:
    return decision_dir / f"TRADING-439_return_to_research_decision_{as_of.isoformat()}.json"


def build_return_to_research_reset_payloads(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    decision_source_dir: Path = PROJECT_ROOT / "docs" / "decisions",
    owner_decision_log_path: Path = owner_log.DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    append_owner_decision: bool = True,
) -> dict[str, dict[str, Any]]:
    decision_stage_inputs = _load_decision_stage_inputs(reports_dir, as_of)
    append_result = ensure_owner_return_to_research_decision(
        as_of=as_of,
        decision_stage_inputs=decision_stage_inputs,
        decision_source_dir=decision_source_dir,
        owner_decision_log_path=owner_decision_log_path,
        append_owner_decision=append_owner_decision,
    )
    owner_audit_payload = owner_log.build_owner_decision_audit_log_payload(
        as_of=as_of,
        log_path=owner_decision_log_path,
    )
    owner_audit_validation = owner_log.validate_owner_decision_audit_log_payload(
        owner_audit_payload
    )

    owner_decision = build_owner_return_to_research_decision_payload(
        as_of=as_of,
        append_result=append_result,
        owner_audit_payload=owner_audit_payload,
        owner_audit_validation=owner_audit_validation,
    )
    transition = build_candidate_transition_pack_payload(
        as_of=as_of,
        owner_decision_payload=owner_decision,
        decision_stage_inputs=decision_stage_inputs,
    )
    failure = build_failure_mode_attribution_payload(
        as_of=as_of,
        decision_stage_inputs=decision_stage_inputs,
    )
    reusable = build_reusable_evidence_extraction_payload(
        as_of=as_of,
        decision_stage_inputs=decision_stage_inputs,
        failure_mode_payload=failure,
    )
    backlog = build_hypothesis_backlog_payload(
        as_of=as_of,
        failure_mode_payload=failure,
    )
    spec = build_next_candidate_spec_payload(
        as_of=as_of,
        hypothesis_backlog_payload=backlog,
    )
    backfill = build_research_backfill_plan_payload(
        as_of=as_of,
        next_candidate_spec_payload=spec,
    )
    archived = build_archived_candidate_status_payload(
        as_of=as_of,
        owner_decision_payload=owner_decision,
        transition_pack_payload=transition,
    )
    reset = build_research_cycle_reset_pack_payload(
        as_of=as_of,
        owner_decision_payload=owner_decision,
        transition_pack_payload=transition,
        failure_mode_payload=failure,
        reusable_evidence_payload=reusable,
        hypothesis_backlog_payload=backlog,
        next_candidate_spec_payload=spec,
        backfill_plan_payload=backfill,
        archived_candidate_payload=archived,
    )
    snapshot = build_return_to_research_governance_snapshot_payload(
        as_of=as_of,
        owner_decision_payload=owner_decision,
        transition_pack_payload=transition,
        failure_mode_payload=failure,
        reusable_evidence_payload=reusable,
        hypothesis_backlog_payload=backlog,
        next_candidate_spec_payload=spec,
        backfill_plan_payload=backfill,
        archived_candidate_payload=archived,
        reset_pack_payload=reset,
    )
    return {
        OWNER_DECISION_RECORD_REPORT_TYPE: owner_decision,
        CANDIDATE_TRANSITION_PACK_REPORT_TYPE: transition,
        FAILURE_MODE_ATTRIBUTION_REPORT_TYPE: failure,
        REUSABLE_EVIDENCE_REPORT_TYPE: reusable,
        HYPOTHESIS_BACKLOG_REPORT_TYPE: backlog,
        NEXT_CANDIDATE_SPEC_REPORT_TYPE: spec,
        RESEARCH_BACKFILL_PLAN_REPORT_TYPE: backfill,
        ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE: archived,
        RESEARCH_CYCLE_RESET_PACK_REPORT_TYPE: reset,
        GOVERNANCE_SNAPSHOT_REPORT_TYPE: snapshot,
    }


def ensure_owner_return_to_research_decision(
    *,
    as_of: date,
    decision_stage_inputs: Mapping[str, Mapping[str, Any]],
    decision_source_dir: Path,
    owner_decision_log_path: Path,
    append_owner_decision: bool,
) -> dict[str, Any]:
    source_path = default_owner_return_to_research_decision_source_path(
        decision_source_dir,
        as_of,
    )
    decision_record = build_owner_return_to_research_decision_record(
        as_of=as_of,
        decision_stage_inputs=decision_stage_inputs,
    )
    if source_path.exists():
        existing_source = _read_json_mapping(source_path)
        if _text(existing_source.get("decision_id")) != _text(
            decision_record.get("decision_id")
        ):
            raise ValueError(f"owner return-to-research source decision mismatch: {source_path}")
        decision_record = {**decision_record, **existing_source}
    else:
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(
            json.dumps(decision_record, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    existing_log = owner_log.read_owner_decision_audit_log(owner_decision_log_path)
    decision_id = _text(decision_record.get("decision_id"))
    existing_record = _existing_log_record(existing_log, decision_id)
    appended = False
    normalized_record: dict[str, Any] = {}
    append_status = "OWNER_DECISION_ALREADY_RECORDED"
    if existing_record:
        normalized_record = existing_record
    elif append_owner_decision:
        normalized_record = owner_log.append_owner_decision_record(
            decision_record,
            log_path=owner_decision_log_path,
            source_record_path=_portable_path(source_path),
        )
        appended = True
        append_status = "OWNER_DECISION_APPENDED"
    else:
        normalized_record = owner_log.normalize_owner_decision_record(
            decision_record,
            source_record_path=_portable_path(source_path),
        )
        append_status = "OWNER_DECISION_APPEND_SKIPPED"

    record_validation = owner_log.validate_owner_decision_record(normalized_record)
    return {
        "decision_id": decision_id,
        "source_record_path": _display_path(source_path),
        "owner_decision_log_path": _display_path(owner_decision_log_path),
        "append_requested": append_owner_decision,
        "append_performed": appended,
        "already_recorded": bool(existing_record),
        "append_status": append_status,
        "source_record": decision_record,
        "normalized_record": normalized_record,
        "record_validation": record_validation,
    }


def build_owner_return_to_research_decision_record(
    *,
    as_of: date,
    decision_stage_inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    input_artifacts = [
        {
            "artifact_id": report_type,
            "artifact_path": _display_path(item.get("path")),
            "artifact_type": "decision_stage_report",
        }
        for report_type, item in decision_stage_inputs.items()
        if report_type
        in {
            decision_stage_review.EIGHT_BLOCKER_REPORT_TYPE,
            decision_stage_review.NORMAL_GATE_GAP_REPORT_TYPE,
            decision_stage_review.PROMOTION_BLOCKER_REPORT_TYPE,
            decision_stage_review.CANDIDATE_ASSESSMENT_REPORT_TYPE,
            decision_stage_review.OWNER_OPTIONS_REPORT_TYPE,
            decision_stage_review.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
        }
    ]
    return {
        "decision_id": f"{OWNER_DECISION_ID_PREFIX}_{as_of.isoformat()}",
        "timestamp": f"{as_of.isoformat()}T05:00:00+09:00",
        "candidate_id": CANDIDATE_ID,
        "input_artifacts": input_artifacts,
        "owner_action": "return_to_research",
        "reason_summary": OWNER_REASON_SUMMARY,
        "safety_status": "SAFETY_PASS_WITH_WARNINGS",
        "next_action": "create_return_to_research_transition_pack",
        "source_review_template_version": "owner_review_template_v2",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "official_target_weights_generated": False,
    }


def build_owner_return_to_research_decision_payload(
    *,
    as_of: date,
    append_result: Mapping[str, Any],
    owner_audit_payload: Mapping[str, Any],
    owner_audit_validation: Mapping[str, Any],
) -> dict[str, Any]:
    record = _mapping(append_result.get("normalized_record"))
    record_validation = _mapping(append_result.get("record_validation"))
    audit_summary = _mapping(owner_audit_payload.get("summary"))
    validation_status = _text(owner_audit_validation.get("validation_status"))
    decision_recorded = (
        _text(record.get("owner_action")) == "return_to_research"
        and _text(record_validation.get("status")) == PASS_STATUS
        and validation_status == PASS_STATUS
        and _text(audit_summary.get("latest_decision_id"))
        == _text(append_result.get("decision_id"))
    )
    status = (
        "OWNER_RETURN_TO_RESEARCH_DECISION_RECORDED"
        if decision_recorded
        else "OWNER_RETURN_TO_RESEARCH_DECISION_PENDING"
    )
    summary = {
        "owner_decision_artifact_id": _text(append_result.get("decision_id")),
        "candidate_id": _text(record.get("candidate_id"), CANDIDATE_ID),
        "owner_action": _text(record.get("owner_action")),
        "append_status": _text(append_result.get("append_status")),
        "append_requested": bool(append_result.get("append_requested")),
        "append_performed": bool(append_result.get("append_performed")),
        "already_recorded": bool(append_result.get("already_recorded")),
        "record_validation_status": _text(record_validation.get("status")),
        "owner_decision_audit_status": _text(owner_audit_payload.get("audit_log_status")),
        "owner_decision_audit_validation_status": validation_status,
        "latest_owner_action": _text(audit_summary.get("latest_owner_action")),
        "latest_decision_id": _text(audit_summary.get("latest_decision_id")),
        "current_candidate_status": "RETURN_TO_RESEARCH" if decision_recorded else "PENDING",
        "normal_paper_shadow_enabled": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=OWNER_DECISION_RECORD_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Record the explicit owner decision to return the current candidate "
            "to research after the decision-stage review."
        ),
        input_artifacts={
            "source_owner_decision": _text(append_result.get("source_record_path")),
            "owner_decision_audit_log": _text(append_result.get("owner_decision_log_path")),
        },
        output_decision=status,
        summary=summary,
        body={
            "owner_decision_record": dict(record),
            "record_validation": dict(record_validation),
            "owner_decision_audit_summary": dict(audit_summary),
            "owner_decision_audit_validation_summary": dict(
                _mapping(owner_audit_validation.get("summary"))
            ),
        },
        reader_brief=_reader_brief(
            summary=(
                "Owner decision recorded as return_to_research; normal shadow remains disabled."
            ),
            key_result=status,
            blocking_issues="normal_paper_shadow_disabled; extended_and_live_forbidden",
            warnings=(
                f"audit_validation={validation_status}; "
                f"record_validation={_text(record_validation.get('status'))}"
            ),
            next_action="create_return_to_research_transition_pack",
        ),
        next_action="create_return_to_research_transition_pack",
        safety_boundary=_safety_boundary(
            mode="append_owner_decision_audit_log_only",
            owner_decision_audit_log_appended=bool(append_result.get("append_performed")),
        ),
        limitations=[
            "This records an owner governance decision only.",
            "It does not resume normal paper-shadow.",
            (
                "It does not approve extended shadow, live trading, official target "
                "weights, broker action, or orders."
            ),
            "It does not mark the candidate rejected.",
        ],
    )


def build_candidate_transition_pack_payload(
    *,
    as_of: date,
    owner_decision_payload: Mapping[str, Any],
    decision_stage_inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    owner_summary = _mapping(owner_decision_payload.get("summary"))
    decision_stage_snapshot = _payload_from_inputs(
        decision_stage_inputs,
        decision_stage_review.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
    )
    snapshot_summary = _mapping(decision_stage_snapshot.get("summary"))
    confirmed = (
        _text(owner_summary.get("owner_action")) == "return_to_research"
        and _text(owner_summary.get("owner_decision_audit_validation_status")) == PASS_STATUS
    )
    status = (
        "RETURN_TO_RESEARCH_CONFIRMED"
        if confirmed
        else "RETURN_TO_RESEARCH_PENDING_OWNER_DECISION"
    )
    if _text(owner_decision_payload.get("status")).endswith("PENDING"):
        status = "RETURN_TO_RESEARCH_PENDING_OWNER_DECISION"
    reasons = [
        {
            "reason_id": "normal_shadow_not_resumed",
            "explanation": (
                "normal paper-shadow resumption gate remains blocked and owner did not "
                "approve continuation."
            ),
            "actual_value": snapshot_summary.get("normal_shadow_may_resume"),
        },
        {
            "reason_id": "extended_shadow_forbidden",
            "explanation": "extended-shadow protocol remains blocked and is outside this decision.",
            "actual_value": snapshot_summary.get("extended_shadow_remains_forbidden"),
        },
        {
            "reason_id": "live_trading_forbidden",
            "explanation": (
                "no live trading, broker, order, or official target-weight path is approved."
            ),
            "actual_value": snapshot_summary.get("live_trading_remains_forbidden"),
        },
    ]
    useful_evidence = [
        "filtered candidate diagnostics",
        "stress review",
        "drawdown mismatch reduction",
        "flip / rotation reduction",
        "A/B review",
        "signal gate confirmation",
    ]
    failed_evidence = [
        "cost sensitivity did not remain meaningful under configured costs",
        "benchmark baseline control underperformed required baselines",
        "normal resumption gate remained blocked",
        "observation clock did not start",
    ]
    summary = {
        "candidate_id": CANDIDATE_ID,
        "transition_status": status,
        "owner_decision_id": owner_summary.get("owner_decision_artifact_id"),
        "owner_action": owner_summary.get("owner_action"),
        "normal_paper_shadow_resumed": False,
        "normal_paper_shadow_enabled": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "candidate_removed_from_resumption_path": confirmed,
        "next_research_action": "build_return_to_research_hypothesis_backlog",
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CANDIDATE_TRANSITION_PACK_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Move the candidate from paper-shadow recovery back to research.",
        input_artifacts={
            "owner_return_to_research_decision_record": _artifact_id(owner_decision_payload),
            "governance_status_snapshot_after_decision_review": _text(
                _mapping(
                    decision_stage_inputs.get(
                        decision_stage_review.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
                        {},
                    )
                ).get("path")
            ),
        },
        output_decision=status,
        summary=summary,
        body={
            "transition_reasons": reasons,
            "useful_evidence": useful_evidence,
            "failed_evidence": failed_evidence,
            "reuse_in_next_cycle": [
                "diagnostic classifications",
                "event casebooks",
                "cost and benchmark blockers as rejection criteria",
                "gate and owner-decision audit trail",
            ],
        },
        reader_brief=_reader_brief(
            summary=f"Candidate transition status is {status}; resumption path remains closed.",
            key_result=status,
            blocking_issues="normal_shadow_disabled; extended_shadow_forbidden; live_forbidden",
            warnings="candidate is not rejected; future research requires new validation",
            next_action="attribute_failure_modes_before_next_research_cycle",
        ),
        next_action="attribute_failure_modes_before_next_research_cycle",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Transition pack archives the resumption path only; it is not a rejection.",
            "No strategy behavior or paper-shadow account state is modified.",
        ],
    )


def build_failure_mode_attribution_payload(
    *,
    as_of: date,
    decision_stage_inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate = _payload_from_inputs(
        decision_stage_inputs,
        decision_stage_review.CANDIDATE_ASSESSMENT_REPORT_TYPE,
    )
    promotion = _payload_from_inputs(
        decision_stage_inputs,
        decision_stage_review.PROMOTION_BLOCKER_REPORT_TYPE,
    )
    gate = _payload_from_inputs(
        decision_stage_inputs,
        decision_stage_review.NORMAL_GATE_GAP_REPORT_TYPE,
    )
    quality = _payload_from_inputs(
        decision_stage_inputs,
        decision_stage_review.REPORT_QUALITY_DRILLDOWN_REPORT_TYPE,
    )
    failure_modes = [
        _failure_mode(
            "cost_survival_failure",
            "candidate-performance issue",
            "cost sensitivity",
            _summary_value(candidate, "cost_sensitivity_status"),
            1,
            ["candidate redesign", "stricter rejection", "better metrics"],
        ),
        _failure_mode(
            "benchmark_relative_failure",
            "candidate-performance issue",
            "benchmark comparison",
            _summary_value(candidate, "benchmark_baseline_status"),
            2,
            ["candidate redesign", "stricter rejection"],
        ),
        _failure_mode(
            "normal_resumption_gate_blocked",
            "governance-policy issue",
            "readiness / staleness",
            _summary_value(candidate, "normal_gate_status"),
            3,
            ["data regeneration", "more observation days", "owner decision"],
        ),
        _failure_mode(
            "owner_decision_hold_until_return",
            "owner-decision issue",
            "owner decision",
            _summary_value(candidate, "owner_action"),
            4,
            ["owner decision"],
        ),
        _failure_mode(
            "observation_clock_not_started",
            "governance-policy issue",
            "observation clock",
            _condition_value(gate, "owner_action"),
            5,
            ["more observation days", "owner decision"],
        ),
        _failure_mode(
            "paper_shadow_health_manual_review",
            "data-pipeline issue",
            "paper-shadow health",
            _source_status(candidate, "paper_shadow_health"),
            6,
            ["data regeneration", "better metrics"],
        ),
        _failure_mode(
            "promotion_board_hold",
            "governance-policy issue",
            "promotion board",
            _summary_value(promotion, "promotion_board_status"),
            7,
            ["stricter rejection", "owner decision"],
        ),
        _failure_mode(
            "signal_input_stability_warning",
            "data-pipeline issue",
            "signal input stability",
            _condition_value(gate, "signal_input_completeness"),
            8,
            ["data regeneration", "candidate redesign"],
        ),
        _failure_mode(
            "report_quality_warning_load",
            "governance-policy issue",
            "report quality warnings",
            _summary_value(quality, "source_warning_count"),
            9,
            ["better metrics", "metadata cleanup"],
        ),
    ]
    summary = {
        "candidate_id": CANDIDATE_ID,
        "attribution_status": "FAILURE_MODE_ATTRIBUTION_READY",
        "failure_mode_count": len(failure_modes),
        "top_failure_mode": failure_modes[0]["failure_mode_id"],
        "recommended_research_direction": (
            "redesign_candidate_for_cost_survival_and_benchmark_relative_strength"
        ),
        "strategy_behavior_modified": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=FAILURE_MODE_ATTRIBUTION_REPORT_TYPE,
        as_of=as_of,
        status="FAILURE_MODE_ATTRIBUTION_READY",
        purpose="Diagnose why the candidate failed to continue into normal paper-shadow.",
        input_artifacts=_decision_stage_input_paths(decision_stage_inputs),
        output_decision="FAILURE_MODE_ATTRIBUTION_READY",
        summary=summary,
        body={
            "ranked_failure_modes": failure_modes,
            "recommended_research_direction": {
                "direction_id": summary["recommended_research_direction"],
                "rationale": (
                    "Cost and benchmark failures are direct candidate-performance blockers; "
                    "readiness, observation, owner, and report warnings explain why the "
                    "candidate cannot remain in the resumption path."
                ),
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "Failure attribution ranks cost survival and benchmark-relative "
                "weakness first."
            ),
            key_result="FAILURE_MODE_ATTRIBUTION_READY",
            blocking_issues="cost_survival_failure; benchmark_relative_failure",
            warnings="diagnostic only; no strategy behavior changed",
            next_action="extract_reusable_evidence_before_hypothesis_backlog",
        ),
        next_action="extract_reusable_evidence_before_hypothesis_backlog",
        safety_boundary=_safety_boundary(),
        limitations=["Diagnostic only; does not modify strategy behavior."],
    )


def build_reusable_evidence_extraction_payload(
    *,
    as_of: date,
    decision_stage_inputs: Mapping[str, Mapping[str, Any]],
    failure_mode_payload: Mapping[str, Any],
) -> dict[str, Any]:
    candidate = _payload_from_inputs(
        decision_stage_inputs,
        decision_stage_review.CANDIDATE_ASSESSMENT_REPORT_TYPE,
    )
    sources = _records(candidate.get("evidence_sources"))
    classification_by_source = {
        "filtered_candidate_evidence": "weak but informative",
        "stress_review": "reusable",
        "drawdown_mismatch_reduction": "reusable",
        "flip_rotation_reduction": "reusable",
        "ab_review": "weak but informative",
        "signal_gate_confirmation": "stale",
        "paper_shadow_health": "weak but informative",
        "cost_sensitivity": "invalidated",
        "benchmark_baseline": "invalidated",
        "promotion_board": "not comparable",
        "recovery_governance_pack": "not comparable",
    }
    evidence_rows = [
        {
            "source_id": _text(source.get("source_id")),
            "status": _text(source.get("status")),
            "classification": classification_by_source.get(
                _text(source.get("source_id")),
                "weak but informative",
            ),
            "artifact_path": _text(source.get("payload_path"), _text(source.get("artifact_path"))),
            "next_research_implication": _evidence_implication(
                classification_by_source.get(_text(source.get("source_id")), "weak but informative")
            ),
            "production_effect": PRODUCTION_EFFECT,
        }
        for source in sources
    ]
    reusable = [row for row in evidence_rows if row["classification"] == "reusable"]
    invalidated = [row for row in evidence_rows if row["classification"] == "invalidated"]
    summary = {
        "candidate_id": CANDIDATE_ID,
        "evidence_extraction_status": "REUSABLE_EVIDENCE_EXTRACTION_READY",
        "source_count": len(evidence_rows),
        "reusable_count": len(reusable),
        "invalidated_count": len(invalidated),
        "gap_count": 4,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=REUSABLE_EVIDENCE_REPORT_TYPE,
        as_of=as_of,
        status="REUSABLE_EVIDENCE_EXTRACTION_READY",
        purpose="Classify which failed-candidate evidence remains useful for future research.",
        input_artifacts={
            **_decision_stage_input_paths(decision_stage_inputs),
            "candidate_failure_mode_attribution": _artifact_id(failure_mode_payload),
        },
        output_decision="REUSABLE_EVIDENCE_EXTRACTION_READY",
        summary=summary,
        body={
            "evidence_classification": evidence_rows,
            "reusable_evidence": reusable,
            "invalidated_evidence": invalidated,
            "evidence_gaps": [
                "cost-adjusted improvement needs stronger survival margin",
                "benchmark-relative behavior needs direct objective gate",
                "fresh signal completeness must be regenerated before future eligibility",
                "observation evidence must accumulate after owner-approved normal shadow only",
            ],
            "next_research_implications": [
                "Reuse event diagnostics and mismatch/rotation reductions as diagnostics only.",
                "Do not re-promote the failed candidate from reusable diagnostics alone.",
                "Treat cost and benchmark failures as design constraints for the next cycle.",
            ],
        },
        reader_brief=_reader_brief(
            summary="Reusable evidence is diagnostic; cost and benchmark evidence is invalidating.",
            key_result="REUSABLE_EVIDENCE_EXTRACTION_READY",
            blocking_issues="invalidated_cost_and_benchmark_evidence",
            warnings="some evidence is stale or not comparable",
            next_action="build_return_to_research_hypothesis_backlog",
        ),
        next_action="build_return_to_research_hypothesis_backlog",
        safety_boundary=_safety_boundary(),
        limitations=["Does not re-promote the failed candidate."],
    )


def build_hypothesis_backlog_payload(
    *,
    as_of: date,
    failure_mode_payload: Mapping[str, Any],
) -> dict[str, Any]:
    hypotheses = [
        _hypothesis(
            "RTR-HYP-001",
            "P0",
            "cost_survival_objective",
            "Improve net improvement survival under low/medium/high cost assumptions.",
            "cost sensitivity report, turnover path, candidate metrics",
            "Run cost sensitivity before any promotion eligibility.",
            "fails high-cost and medium-cost net improvement proxy",
            "stop if cost-adjusted proxy remains not meaningful",
        ),
        _hypothesis(
            "RTR-HYP-002",
            "P0",
            "benchmark_relative_strength_gate",
            (
                "Require candidate to outperform static allocation, QQQ/SPY, "
                "and equal-weight baselines."
            ),
            "benchmark baseline control pack and candidate metrics",
            "Run benchmark control before owner review.",
            "underperforms required baselines",
            "stop if benchmark deltas remain negative after redesign",
        ),
        _hypothesis(
            "RTR-HYP-003",
            "P0",
            "fresh_signal_input_robustness",
            "Reduce stale-data sensitivity and fail closed before readiness interpretation.",
            "signal input completeness, source map, freshness monitor",
            "Regenerate signal inputs and validate completeness before backfill.",
            "stale or missing feature/signal inputs",
            "stop if signal completeness is warning/blocking",
        ),
        _hypothesis(
            "RTR-HYP-004",
            "P1",
            "turnover_and_rotation_dampener",
            "Lower false flip/rotation and turnover without hiding drawdown risk.",
            "flip/rotation casebook, turnover metrics, event windows",
            "Compare false rotation count and turnover against original filtered candidate.",
            "turnover falls only by suppressing useful risk-off behavior",
            "stop if drawdown mismatch worsens materially",
        ),
        _hypothesis(
            "RTR-HYP-005",
            "P1",
            "drawdown_mismatch_guardrail",
            "Preserve useful drawdown mismatch reduction while avoiding false risk-off clusters.",
            "drawdown casebook, stress review, regime tags",
            "Backfill rapid/slow drawdown and false risk-off windows.",
            "improvement only appears in narrow historical events",
            "stop if AI-cycle drawdown windows fail",
        ),
        _hypothesis(
            "RTR-HYP-006",
            "P2",
            "conservative_promotion_readiness",
            "Expose stronger pre-owner evidence floor before any future normal-shadow eligibility.",
            "readiness report, owner audit log, report quality gate",
            "Require all required source validations to pass or explicitly block.",
            "readiness depends on warnings or unresolved owner action",
            "stop if manual review cannot distinguish warning vs blocker",
        ),
    ]
    summary = {
        "backlog_status": "RETURN_TO_RESEARCH_HYPOTHESIS_BACKLOG_READY",
        "hypothesis_count": len(hypotheses),
        "p0_count": len([item for item in hypotheses if item["priority"] == "P0"]),
        "source_failure_mode_count": _int(
            _mapping(failure_mode_payload.get("summary")).get("failure_mode_count")
        ),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=HYPOTHESIS_BACKLOG_REPORT_TYPE,
        as_of=as_of,
        status="RETURN_TO_RESEARCH_HYPOTHESIS_BACKLOG_READY",
        purpose="Generate research hypotheses from the failure-mode attribution.",
        input_artifacts={"candidate_failure_mode_attribution": _artifact_id(failure_mode_payload)},
        output_decision="RETURN_TO_RESEARCH_HYPOTHESIS_BACKLOG_READY",
        summary=summary,
        body={"hypotheses": hypotheses},
        reader_brief=_reader_brief(
            summary="Hypothesis backlog created with three P0 redesign hypotheses.",
            key_result="RETURN_TO_RESEARCH_HYPOTHESIS_BACKLOG_READY",
            blocking_issues="none",
            warnings="research hypotheses only; no candidate promotion",
            next_action="draft_next_candidate_spec_from_p0_hypotheses",
        ),
        next_action="draft_next_candidate_spec_from_p0_hypotheses",
        safety_boundary=_safety_boundary(),
        limitations=["Research hypotheses only; no promotion or paper-shadow activation."],
    )


def build_next_candidate_spec_payload(
    *,
    as_of: date,
    hypothesis_backlog_payload: Mapping[str, Any],
) -> dict[str, Any]:
    p0_hypotheses = [
        item
        for item in _records(hypothesis_backlog_payload.get("hypotheses"))
        if _text(item.get("priority")) == "P0"
    ]
    spec = {
        "candidate_id": NEXT_RESEARCH_CANDIDATE_ID,
        "status": "RESEARCH_SPEC_DRAFT",
        "source_hypotheses": [_text(item.get("hypothesis_id")) for item in p0_hypotheses],
        "signal_inputs": [
            "fresh dynamic v3 signal series",
            "regime mismatch feature set",
            "signal input completeness monitor output",
            "cost and turnover metrics",
        ],
        "regime_filter_assumptions": [
            "AI after ChatGPT regime is the default conclusion window",
            "pre-2022 data can be used only for warm-up or stress comparison",
            "freshness warnings block eligibility until resolved",
        ],
        "drawdown_handling": [
            "preserve mismatch reduction evidence",
            "test rapid drawdown, slow drawdown, and AI/semiconductor correction windows",
        ],
        "rotation_handling": [
            "dampen false flips without suppressing valid risk-off response",
            "record rotation and turnover attribution per event window",
        ],
        "turnover_constraints": [
            "lower turnover than failed candidate",
            "must survive low/medium/high configured cost assumptions",
        ],
        "cost_sensitivity_expectations": [
            "no paper-shadow eligibility if status is NOT_MEANINGFUL_UNDER_COSTS",
        ],
        "benchmark_comparison_expectations": [
            "must not underperform required static, QQQ, SPY, and equal-weight baselines",
        ],
    }
    validations = [
        "stress test",
        "cost sensitivity",
        "benchmark baseline comparison",
        "signal completeness",
        "readiness",
        "owner review",
    ]
    summary = {
        "candidate_spec_status": "NEXT_CANDIDATE_SPEC_DRAFT_READY",
        "candidate_id": NEXT_RESEARCH_CANDIDATE_ID,
        "selected_p0_hypothesis_count": len(p0_hypotheses),
        "paper_shadow_activation_allowed": False,
        "official_target_weights_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=NEXT_CANDIDATE_SPEC_REPORT_TYPE,
        as_of=as_of,
        status="NEXT_CANDIDATE_SPEC_DRAFT_READY",
        purpose="Draft the next research-only candidate spec from P0 hypotheses.",
        input_artifacts={
            "return_to_research_hypothesis_backlog": _artifact_id(
                hypothesis_backlog_payload
            )
        },
        output_decision="NEXT_CANDIDATE_SPEC_DRAFT_READY",
        summary=summary,
        body={
            "candidate_specs": [spec],
            "required_validation_before_future_paper_shadow_eligibility": validations,
        },
        reader_brief=_reader_brief(
            summary="Next candidate spec draft is research-only and not paper-shadow active.",
            key_result="NEXT_CANDIDATE_SPEC_DRAFT_READY",
            blocking_issues="paper_shadow_activation_forbidden",
            warnings="future backfill required before eligibility",
            next_action="create_research_backfill_plan_for_next_candidate",
        ),
        next_action="create_research_backfill_plan_for_next_candidate",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Draft spec only; no official weights, live trading, or paper-shadow activation."
        ],
    )


def build_research_backfill_plan_payload(
    *,
    as_of: date,
    next_candidate_spec_payload: Mapping[str, Any],
) -> dict[str, Any]:
    windows = [
        _backfill_window("normal_market_regime", "2023-01-03", "2023-07-31"),
        _backfill_window("rapid_drawdown", "2024-07-10", "2024-08-09"),
        _backfill_window("slow_drawdown", "2025-01-02", "2025-04-30"),
        _backfill_window("high_volatility_sideways_market", "2023-08-01", "2023-11-15"),
        _backfill_window("ai_semiconductor_correction", "2024-03-08", "2024-04-19"),
        _backfill_window("false_risk_off_cluster", "2023-09-01", "2023-10-31"),
    ]
    metrics = [
        "turnover",
        "drawdown mismatch",
        "false flip / rotation",
        "benchmark-relative performance",
        "cost-adjusted proxy",
        "signal completeness",
    ]
    rules = [
        {
            "rule_id": "pass",
            "condition": (
                "all required source validations pass, cost sensitivity remains meaningful, "
                "benchmark control is not underperforming, and signal completeness is "
                "not warning/blocking"
            ),
        },
        {
            "rule_id": "fail",
            "condition": (
                "cost sensitivity is NOT_MEANINGFUL_UNDER_COSTS or benchmark status is "
                "CANDIDATE_UNDERPERFORMS_BASELINES"
            ),
        },
        {
            "rule_id": "needs_more_evidence",
            "condition": (
                "observation sample or source coverage is insufficient but not invalidating"
            ),
        },
    ]
    summary = {
        "backfill_plan_status": "RESEARCH_BACKFILL_PLAN_READY",
        "candidate_id": NEXT_RESEARCH_CANDIDATE_ID,
        "window_count": len(windows),
        "metric_count": len(metrics),
        "plan_only": True,
        "paper_shadow_outputs_generated": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=RESEARCH_BACKFILL_PLAN_REPORT_TYPE,
        as_of=as_of,
        status="RESEARCH_BACKFILL_PLAN_READY",
        purpose="Define backfill and validation plan for the next candidate spec.",
        input_artifacts={"next_candidate_spec_draft": _artifact_id(next_candidate_spec_payload)},
        output_decision="RESEARCH_BACKFILL_PLAN_READY",
        summary=summary,
        body={
            "required_backfill_windows": windows,
            "metrics": metrics,
            "pass_fail_needs_more_evidence_rules": rules,
            "cli_report_plan": [
                "reuse existing dynamic-v3 rescue backfill/report framework where safe",
                "do not emit paper-shadow outputs until future owner eligibility review",
            ],
        },
        reader_brief=_reader_brief(
            summary="Backfill plan is ready; it is plan-only and creates no paper-shadow output.",
            key_result="RESEARCH_BACKFILL_PLAN_READY",
            blocking_issues="none",
            warnings="execution requires future safe backfill command",
            next_action="archive_current_candidate_status_as_returned_to_research",
        ),
        next_action="archive_current_candidate_status_as_returned_to_research",
        safety_boundary=_safety_boundary(),
        limitations=["Plan only; no backfill execution or paper-shadow output is produced."],
    )


def build_archived_candidate_status_payload(
    *,
    as_of: date,
    owner_decision_payload: Mapping[str, Any],
    transition_pack_payload: Mapping[str, Any],
) -> dict[str, Any]:
    owner_summary = _mapping(owner_decision_payload.get("summary"))
    transition_summary = _mapping(transition_pack_payload.get("summary"))
    status = "CANDIDATE_RETURNED_TO_RESEARCH"
    summary = {
        "candidate_id": CANDIDATE_ID,
        "candidate_status": "RETURNED_TO_RESEARCH",
        "ledger_status": status,
        "owner_decision_id": owner_summary.get("owner_decision_artifact_id"),
        "owner_action": owner_summary.get("owner_action"),
        "not_active_normal_shadow": True,
        "not_extended_shadow_eligible": True,
        "not_live_eligible": True,
        "candidate_rejected": False,
        "next_research_action": transition_summary.get("next_research_action"),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Update current candidate status so it is no longer an active resumption "
            "candidate."
        ),
        input_artifacts={
            "owner_return_to_research_decision_record": _artifact_id(owner_decision_payload),
            "candidate_return_to_research_transition_pack": _artifact_id(transition_pack_payload),
        },
        output_decision=status,
        summary=summary,
        body={
            "candidate_ledger_entry": {
                "candidate_id": CANDIDATE_ID,
                "status": "RETURNED_TO_RESEARCH",
                "reason_summary": OWNER_REASON_SUMMARY,
                "linked_artifacts": [
                    _artifact_id(owner_decision_payload),
                    _artifact_id(transition_pack_payload),
                ],
                "next_research_action": "use_return_to_research_hypothesis_backlog",
                "candidate_rejected": False,
                "normal_shadow_active": False,
                "extended_shadow_eligible": False,
                "live_eligible": False,
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary="Current candidate status is RETURNED_TO_RESEARCH, not rejected.",
            key_result=status,
            blocking_issues="normal_shadow_inactive; extended_live_forbidden",
            warnings="candidate may be reused only through a new research cycle",
            next_action="create_research_cycle_reset_pack",
        ),
        next_action="create_research_cycle_reset_pack",
        safety_boundary=_safety_boundary(),
        limitations=["This is an auditable status artifact, not a production state mutation."],
    )


def build_research_cycle_reset_pack_payload(
    *,
    as_of: date,
    owner_decision_payload: Mapping[str, Any],
    transition_pack_payload: Mapping[str, Any],
    failure_mode_payload: Mapping[str, Any],
    reusable_evidence_payload: Mapping[str, Any],
    hypothesis_backlog_payload: Mapping[str, Any],
    next_candidate_spec_payload: Mapping[str, Any],
    backfill_plan_payload: Mapping[str, Any],
    archived_candidate_payload: Mapping[str, Any],
) -> dict[str, Any]:
    required = [
        owner_decision_payload,
        transition_pack_payload,
        failure_mode_payload,
        reusable_evidence_payload,
        hypothesis_backlog_payload,
        next_candidate_spec_payload,
        backfill_plan_payload,
        archived_candidate_payload,
    ]
    blocked = [payload for payload in required if not _status_ready(payload)]
    status = "RESEARCH_CYCLE_RESET_READY" if not blocked else "RESEARCH_CYCLE_RESET_BLOCKED"
    summary = {
        "reset_status": status,
        "closed_candidate": CANDIDATE_ID,
        "next_candidate_spec": NEXT_RESEARCH_CANDIDATE_ID,
        "required_artifact_count": len(required),
        "blocked_artifact_count": len(blocked),
        "normal_paper_shadow_active": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=RESEARCH_CYCLE_RESET_PACK_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Close the failed recovery cycle and open the next research cycle.",
        input_artifacts=_artifact_map(required),
        output_decision=status,
        summary=summary,
        body={
            "closed_candidate": {
                "candidate_id": CANDIDATE_ID,
                "status": "RETURNED_TO_RESEARCH",
                "rejected": False,
            },
            "reusable_artifacts": _records(reusable_evidence_payload.get("reusable_evidence")),
            "next_research_tasks": [
                "execute safe backfill plan when implemented",
                "validate cost survival and benchmark-relative behavior",
                "regenerate signal completeness before eligibility review",
                "return to owner review only after all required validations pass",
            ],
            "blocked_items": [
                "normal paper-shadow remains disabled",
                "extended shadow remains forbidden",
                "live trading remains forbidden",
                "official target weights and broker/order artifacts remain forbidden",
            ],
        },
        reader_brief=_reader_brief(
            summary=f"Research-cycle reset status is {status}; next step is research redesign.",
            key_result=status,
            blocking_issues="none" if not blocked else "missing_or_blocked_reset_artifact",
            warnings="no active paper-shadow resumption",
            next_action="generate_final_return_to_research_governance_snapshot",
        ),
        next_action="generate_final_return_to_research_governance_snapshot",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Reset pack is a research handoff; it does not run backfills or activate candidates."
        ],
    )


def build_return_to_research_governance_snapshot_payload(
    *,
    as_of: date,
    owner_decision_payload: Mapping[str, Any],
    transition_pack_payload: Mapping[str, Any],
    failure_mode_payload: Mapping[str, Any],
    reusable_evidence_payload: Mapping[str, Any],
    hypothesis_backlog_payload: Mapping[str, Any],
    next_candidate_spec_payload: Mapping[str, Any],
    backfill_plan_payload: Mapping[str, Any],
    archived_candidate_payload: Mapping[str, Any],
    reset_pack_payload: Mapping[str, Any],
) -> dict[str, Any]:
    owner_summary = _mapping(owner_decision_payload.get("summary"))
    archive_summary = _mapping(archived_candidate_payload.get("summary"))
    backlog_summary = _mapping(hypothesis_backlog_payload.get("summary"))
    spec_summary = _mapping(next_candidate_spec_payload.get("summary"))
    checks = {
        "owner_decision_recorded": _text(owner_summary.get("owner_action"))
        == "return_to_research",
        "current_candidate_no_longer_active_in_resumption_path": bool(
            archive_summary.get("not_active_normal_shadow")
        ),
        "normal_paper_shadow_disabled": True,
        "extended_shadow_forbidden": True,
        "live_trading_forbidden": True,
        "next_research_backlog_exists": _int(backlog_summary.get("hypothesis_count")) > 0,
        "next_candidate_spec_exists": bool(spec_summary.get("candidate_id")),
        "research_cycle_reset_ready": _text(reset_pack_payload.get("status"))
        == "RESEARCH_CYCLE_RESET_READY",
    }
    status = (
        "RETURN_TO_RESEARCH_COMPLETE"
        if all(checks.values())
        else "RETURN_TO_RESEARCH_INCOMPLETE"
    )
    summary = {
        "return_to_research_status": status,
        **checks,
        "candidate_id": CANDIDATE_ID,
        "candidate_status": archive_summary.get("candidate_status", "RETURNED_TO_RESEARCH"),
        "owner_decision_id": owner_summary.get("owner_decision_artifact_id"),
        "owner_action": owner_summary.get("owner_action"),
        "normal_paper_shadow_active": False,
        "normal_paper_shadow_enabled": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "candidate_rejected": False,
        "hypothesis_count": backlog_summary.get("hypothesis_count"),
        "next_candidate_id": spec_summary.get("candidate_id"),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=GOVERNANCE_SNAPSHOT_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Generate the final governance snapshot after returning the candidate to research.",
        input_artifacts=_artifact_map(
            [
                owner_decision_payload,
                transition_pack_payload,
                failure_mode_payload,
                reusable_evidence_payload,
                hypothesis_backlog_payload,
                next_candidate_spec_payload,
                backfill_plan_payload,
                archived_candidate_payload,
                reset_pack_payload,
            ]
        ),
        output_decision=status,
        summary=summary,
        body={
            "confirmation_checks": [
                {"check_id": key, "passed": value} for key, value in checks.items()
            ],
            "source_reader_briefs": {
                _text(payload.get("report_type")): _mapping(payload.get("reader_brief"))
                for payload in [
                    owner_decision_payload,
                    transition_pack_payload,
                    failure_mode_payload,
                    reusable_evidence_payload,
                    hypothesis_backlog_payload,
                    next_candidate_spec_payload,
                    backfill_plan_payload,
                    archived_candidate_payload,
                    reset_pack_payload,
                ]
            },
        },
        reader_brief=_reader_brief(
            summary=(
                f"Return-to-research snapshot is {status}; no active paper-shadow "
                "resumption remains."
            ),
            key_result=status,
            blocking_issues=(
                "none"
                if status == "RETURN_TO_RESEARCH_COMPLETE"
                else "incomplete_reset_check"
            ),
            warnings="candidate is returned to research, not rejected",
            next_action="continue_with_research_redesign_backlog_only",
        ),
        next_action="continue_with_research_redesign_backlog_only",
        safety_boundary=_safety_boundary(),
        limitations=["Final snapshot is a governance state report, not a trading approval."],
    )


def validate_return_to_research_governance_snapshot_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == GOVERNANCE_SNAPSHOT_REPORT_TYPE,
        f"report_type must be {GOVERNANCE_SNAPSHOT_REPORT_TYPE}.",
        "regenerate_return_to_research_governance_snapshot",
    )
    for field_id in (
        "owner_decision_recorded",
        "current_candidate_no_longer_active_in_resumption_path",
        "normal_paper_shadow_disabled",
        "extended_shadow_forbidden",
        "live_trading_forbidden",
        "next_research_backlog_exists",
        "next_candidate_spec_exists",
        "research_cycle_reset_ready",
    ):
        _append_check(
            checks,
            blocking_issues,
            field_id,
            summary.get(field_id) is True,
            f"{field_id} must be true.",
            "rerun_return_to_research_reset_batch",
        )
    _append_check(
        checks,
        blocking_issues,
        "candidate_not_rejected",
        summary.get("candidate_rejected") is False,
        "Candidate must not be marked rejected by return-to-research reset.",
        "remove_rejection_state_without_separate_owner_decision",
    )
    _append_check(
        checks,
        blocking_issues,
        "no_trading_or_broker_outputs",
        (
            summary.get("normal_paper_shadow_active") is False
            and summary.get("extended_shadow_allowed") is False
            and summary.get("live_trading_allowed") is False
            and summary.get("official_target_weights_generated") is False
            and summary.get("broker_order_allowed") is False
        ),
        (
            "Return-to-research snapshot must not enable paper-shadow, live, weights, "
            "broker, or orders."
        ),
        "restore_return_to_research_safety_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_valid",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must preserve no trading/production mutation fields.",
        "restore_return_to_research_safety_boundary",
    )
    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len(blocking_issues),
        "return_to_research_status": _text(payload.get("status")),
        "candidate_status": _text(summary.get("candidate_status")),
        "production_effect": PRODUCTION_EFFECT,
    }
    validation_payload = _payload(
        report_type=GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate final return-to-research governance snapshot and safety boundary.",
        input_artifacts=dict(_mapping(payload.get("input_artifacts"))),
        output_decision=status,
        summary=validation_summary,
        body={
            "checks": checks,
            "blocking_issues": blocking_issues,
            "warning_issues": [],
            "source_summary": dict(summary),
        },
        reader_brief=_reader_brief(
            summary=f"Return-to-research governance snapshot validation is {status}.",
            key_result=status,
            blocking_issues=_issue_list(blocking_issues),
            warnings="none",
            next_action=(
                "use_return_to_research_snapshot_as_current_governance_state"
                if status == PASS_STATUS
                else "repair_return_to_research_snapshot"
            ),
        ),
        next_action=(
            "use_return_to_research_snapshot_as_current_governance_state"
            if status == PASS_STATUS
            else "repair_return_to_research_snapshot"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not mutate governance state."],
    )
    validation_payload["validation_status"] = status
    return validation_payload


def write_return_to_research_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_return_to_research_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_return_to_research_markdown(payload), encoding="utf-8")
    return output_path


def render_return_to_research_markdown(payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_title(report_type)} {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- status: {payload.get('status')}",
        f"- output_decision: {payload.get('output_decision')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- next_action: {payload.get('next_action')}",
    ]
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            lines.append(f"- {key}: {_md_cell(value)}")
    lines.extend(["", "## Reader Brief", ""])
    for key, value in _mapping(payload.get("reader_brief")).items():
        lines.append(f"- {key}: {_md_cell(value)}")
    if report_type == FAILURE_MODE_ATTRIBUTION_REPORT_TYPE:
        lines.extend(_table_records("Ranked Failure Modes", payload.get("ranked_failure_modes")))
    elif report_type == REUSABLE_EVIDENCE_REPORT_TYPE:
        lines.extend(
            _table_records("Evidence Classification", payload.get("evidence_classification"))
        )
    elif report_type == HYPOTHESIS_BACKLOG_REPORT_TYPE:
        lines.extend(_table_records("Hypotheses", payload.get("hypotheses")))
    elif report_type == RESEARCH_BACKFILL_PLAN_REPORT_TYPE:
        lines.extend(_table_records("Backfill Windows", payload.get("required_backfill_windows")))
    elif report_type == GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE:
        lines.extend(_table_records("Checks", payload.get("checks")))
    else:
        lines.extend(_generic_body_markdown(payload))
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def _payload(
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
    safety_boundary: Mapping[str, Any],
    limitations: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "purpose": purpose,
        "input_artifacts": dict(input_artifacts),
        "output_decision": output_decision,
        "summary": dict(summary),
        **dict(body),
        "reader_brief": dict(reader_brief),
        "safety_boundary": dict(safety_boundary),
        "limitations": list(limitations),
        "next_action": next_action,
        "methodology": {
            "collector_mode": "read_existing_return_to_research_context",
            "does_not_refresh_data": True,
            "does_not_generate_missing_evidence": True,
            "does_not_resume_normal_paper_shadow": True,
            "does_not_approve_extended_shadow": True,
            "does_not_approve_live_trading": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_mutate_production": True,
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
            "return-to-research governance only; no normal shadow resume, no extended "
            "shadow, no live trading, no official target weights, no broker/order, "
            "production_effect=none."
        ),
        "next_action": next_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary(
    *,
    mode: str = "return_to_research_reset_reports_only",
    owner_decision_audit_log_appended: bool = False,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "owner_decision_audit_log_appended": owner_decision_audit_log_appended,
        "normal_paper_shadow_resumed": False,
        "normal_shadow_signoff_packet_generated": False,
        "observation_clock_started": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "candidate_rejected": False,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "data_refreshed": False,
    }


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("normal_paper_shadow_resumed") is False
        and safety.get("normal_shadow_signoff_packet_generated") is False
        and safety.get("observation_clock_started") is False
        and safety.get("extended_shadow_approved") is False
        and safety.get("live_trading_allowed") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("candidate_rejected") is False
        and safety.get("strategy_outputs_mutated") is False
        and safety.get("candidate_state_mutated") is False
        and safety.get("paper_shadow_state_mutated") is False
        and safety.get("production_state_mutated") is False
    )


def _portable_path(value: Any) -> Path:
    path = Path(_text(value))
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError:
        return path
    except OSError:
        return path


def _display_path(value: Any) -> str:
    path = _portable_path(value)
    return path.as_posix() if not path.is_absolute() else str(path)


def _load_decision_stage_inputs(reports_dir: Path, as_of: date) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for report_type in DECISION_STAGE_INPUT_TYPES:
        path = decision_stage_review.default_decision_stage_json_path(
            report_type,
            reports_dir,
            as_of,
        )
        result[report_type] = {
            "path": _display_path(path),
            "payload": _read_json_mapping(path),
        }
    return result


def _payload_from_inputs(
    inputs: Mapping[str, Mapping[str, Any]],
    report_type: str,
) -> dict[str, Any]:
    return _mapping(_mapping(inputs.get(report_type)).get("payload"))


def _decision_stage_input_paths(
    inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    return {report_type: _display_path(item.get("path")) for report_type, item in inputs.items()}


def _existing_log_record(read_result: Mapping[str, Any], decision_id: str) -> dict[str, Any]:
    for entry in _records(read_result.get("entries")):
        record = _mapping(entry.get("record"))
        if _text(record.get("decision_id")) == decision_id:
            return record
    return {}


def _failure_mode(
    failure_mode_id: str,
    classification: str,
    domain: str,
    evidence: Any,
    rank: int,
    fixable_by: Sequence[str],
) -> dict[str, Any]:
    return {
        "rank": rank,
        "failure_mode_id": failure_mode_id,
        "classification": classification,
        "domain": domain,
        "evidence": _text(evidence, "UNKNOWN"),
        "fixable_by": list(fixable_by),
        "recommended_action": _recommended_action_for_failure(failure_mode_id),
        "production_effect": PRODUCTION_EFFECT,
    }


def _recommended_action_for_failure(failure_mode_id: str) -> str:
    if failure_mode_id == "cost_survival_failure":
        return "redesign_candidate_until_net_improvement_survives_configured_costs"
    if failure_mode_id == "benchmark_relative_failure":
        return "require_benchmark_relative_strength_before_future_owner_review"
    if failure_mode_id == "normal_resumption_gate_blocked":
        return "keep_normal_shadow_disabled_until_gate_and_owner_decision_clear"
    if failure_mode_id == "owner_decision_hold_until_return":
        return "use_append_only_owner_decision_as_governance_state"
    if failure_mode_id == "observation_clock_not_started":
        return "do_not_count_observation_days_without_authorized_normal_shadow"
    return "carry_as_research_backlog_input"


def _evidence_implication(classification: str) -> str:
    if classification == "reusable":
        return "reuse as diagnostic evidence only"
    if classification == "invalidated":
        return "convert into hard redesign constraint"
    if classification == "stale":
        return "regenerate before eligibility review"
    if classification == "not comparable":
        return "use only as governance context"
    return "use as weak supporting context"


def _hypothesis(
    hypothesis_id: str,
    priority: str,
    name: str,
    expected_improvement: str,
    required_data: str,
    validation_method: str,
    expected_failure_mode: str,
    stop_condition: str,
) -> dict[str, Any]:
    return {
        "hypothesis_id": hypothesis_id,
        "priority": priority,
        "name": name,
        "expected_improvement": expected_improvement,
        "required_data": required_data,
        "validation_method": validation_method,
        "expected_failure_mode": expected_failure_mode,
        "stop_condition": stop_condition,
        "production_effect": PRODUCTION_EFFECT,
    }


def _backfill_window(window_id: str, start: str, end: str) -> dict[str, Any]:
    return {
        "window_id": window_id,
        "start": start,
        "end": end,
        "market_regime": "ai_after_chatgpt",
        "production_effect": PRODUCTION_EFFECT,
    }


def _status_ready(payload: Mapping[str, Any]) -> bool:
    ready_status_by_report_type = {
        OWNER_DECISION_RECORD_REPORT_TYPE: {
            "OWNER_RETURN_TO_RESEARCH_DECISION_RECORDED",
        },
        CANDIDATE_TRANSITION_PACK_REPORT_TYPE: {
            "RETURN_TO_RESEARCH_CONFIRMED",
        },
        FAILURE_MODE_ATTRIBUTION_REPORT_TYPE: {
            "FAILURE_MODE_ATTRIBUTION_READY",
        },
        REUSABLE_EVIDENCE_REPORT_TYPE: {
            "REUSABLE_EVIDENCE_EXTRACTION_READY",
        },
        HYPOTHESIS_BACKLOG_REPORT_TYPE: {
            "RETURN_TO_RESEARCH_HYPOTHESIS_BACKLOG_READY",
        },
        NEXT_CANDIDATE_SPEC_REPORT_TYPE: {
            "NEXT_CANDIDATE_SPEC_DRAFT_READY",
        },
        RESEARCH_BACKFILL_PLAN_REPORT_TYPE: {
            "RESEARCH_BACKFILL_PLAN_READY",
        },
        ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE: {
            "CANDIDATE_RETURNED_TO_RESEARCH",
        },
    }
    report_type = _text(payload.get("report_type"))
    status = _text(payload.get("status"))
    if not report_type or not status:
        return False
    ready_statuses = ready_status_by_report_type.get(report_type)
    if ready_statuses is None:
        return False
    return status in ready_statuses


def _artifact_id(payload: Mapping[str, Any]) -> str:
    return (
        f"{_text(payload.get('report_type'), 'artifact')}:"
        f"{_text(payload.get('as_of'), 'unknown')}"
    )


def _artifact_map(payloads: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    return {
        _text(payload.get("report_type"), f"artifact_{index}"): _artifact_id(payload)
        for index, payload in enumerate(payloads, start=1)
    }


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    return _mapping(payload.get("summary")).get(key)


def _condition_value(payload: Mapping[str, Any], condition_id: str) -> str:
    for row in _records(payload.get("gate_conditions")):
        if _text(row.get("condition_id")) == condition_id:
            return _text(row.get("actual_value"), _text(row.get("status"), "UNKNOWN"))
    return "UNKNOWN"


def _source_status(payload: Mapping[str, Any], source_id: str) -> str:
    for row in _records(payload.get("evidence_sources")):
        if _text(row.get("source_id")) == source_id:
            return _text(row.get("status"), "UNKNOWN")
    return "UNKNOWN"


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


def _date_from_payload(payload: Mapping[str, Any]) -> date:
    try:
        return date.fromisoformat(_text(payload.get("as_of")))
    except ValueError:
        return date.today()


def _read_json_mapping(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"JSON payload must be an object: {path}")
    return raw


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _md_cell(value: Any) -> str:
    text = _text(value)
    return text.replace("|", "\\|").replace("\n", "<br/>")


def _title(report_type: str) -> str:
    return report_type.replace("_", " ").title()


def _table_records(title: str, value: Any) -> list[str]:
    rows = _records(value)
    if not rows:
        return ["", f"## {title}", "", "No rows."]
    keys = list(rows[0].keys())[:8]
    lines = [
        "",
        f"## {title}",
        "",
        "|" + "|".join(keys) + "|",
        "|" + "|".join(["---"] * len(keys)) + "|",
    ]
    for row in rows:
        lines.append("|" + "|".join(_md_cell(row.get(key)) for key in keys) + "|")
    return lines


def _generic_body_markdown(payload: Mapping[str, Any]) -> list[str]:
    lines = ["", "## Details", ""]
    skip = {
        "schema_version",
        "report_type",
        "as_of",
        "generated_at",
        "status",
        "production_effect",
        "manual_review_only",
        "research_only",
        "purpose",
        "input_artifacts",
        "output_decision",
        "summary",
        "reader_brief",
        "safety_boundary",
        "limitations",
        "next_action",
        "methodology",
    }
    for key, value in payload.items():
        if key in skip:
            continue
        if isinstance(value, list):
            lines.append(f"### {key}")
            for item in value[:20]:
                lines.append(f"- {_md_cell(item)}")
        elif isinstance(value, Mapping):
            lines.append(f"### {key}")
            for item_key, item_value in value.items():
                lines.append(f"- {item_key}: {_md_cell(item_value)}")
    return lines


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}}){re.escape(suffix)}$")
    candidates: list[tuple[date, Path]] = []
    if not output_dir.exists():
        return None
    for path in output_dir.iterdir():
        match = pattern.match(path.name)
        if not match:
            continue
        try:
            candidates.append((date.fromisoformat(match.group(1)), path))
        except ValueError:
            continue
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


__all__ = [
    "ARCHIVED_CANDIDATE_STATUS_REPORT_TYPE",
    "CANDIDATE_TRANSITION_PACK_REPORT_TYPE",
    "FAILURE_MODE_ATTRIBUTION_REPORT_TYPE",
    "GOVERNANCE_SNAPSHOT_REPORT_TYPE",
    "GOVERNANCE_SNAPSHOT_VALIDATION_REPORT_TYPE",
    "HYPOTHESIS_BACKLOG_REPORT_TYPE",
    "NEXT_CANDIDATE_SPEC_REPORT_TYPE",
    "OWNER_DECISION_RECORD_REPORT_TYPE",
    "REPORT_PREFIXES",
    "RESEARCH_BACKFILL_PLAN_REPORT_TYPE",
    "RESEARCH_CYCLE_RESET_PACK_REPORT_TYPE",
    "RESET_REPORT_TYPES",
    "REUSABLE_EVIDENCE_REPORT_TYPE",
    "build_return_to_research_reset_payloads",
    "default_owner_return_to_research_decision_source_path",
    "default_return_to_research_json_path",
    "default_return_to_research_markdown_path",
    "latest_return_to_research_json_path",
    "render_return_to_research_markdown",
    "validate_return_to_research_governance_snapshot_payload",
    "write_return_to_research_json",
    "write_return_to_research_markdown",
]
