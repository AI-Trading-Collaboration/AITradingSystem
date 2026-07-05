from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    load_high_intensity_scheduler_audit_package_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_AUDIT_PACKAGE_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    NEXT_2353_ROUTE as EXPECTED_2352_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    READINESS_STATUS as EXPECTED_2352_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    STATUS as EXPECTED_2352_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    TASK_REGISTER_ID as SOURCE_2352_TASK_REGISTER_ID,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_common import (
    collect_real_scheduler_creation_fields,
    collect_unsafe_fields,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2353"
TASK_REGISTER_ID = "TRADING-2353_OBSERVE_ONLY_SCHEDULER_OWNER_REVIEW_DECISION_RECORD"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_owner_review_decision"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_OWNER_REVIEW_DECISION_RECORDED_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2354_WITH_CAVEATS"
NEXT_2354_ROUTE = (
    "TRADING-2354_Observe_Only_Scheduler_Gap_Closure_And_Readiness_Hardening_Plan"
)
OWNER_DECISION = "KEEP_DISABLED_AND_PROMOTION_BLOCKED"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SOURCE_TASKS = [
    "TRADING-2347",
    "TRADING-2348",
    "TRADING-2349",
    "TRADING-2350",
    "TRADING-2351",
    "TRADING-2352",
]

OWNER_DECISION_REASONS = [
    "NO_EXPLICIT_OWNER_APPROVAL_TO_ENABLE_SCHEDULER",
    "OBSERVE_ONLY_MODE_REMAINS_REQUIRED",
    "MANUAL_REVIEW_REQUIRED",
    "DRY_RUN_ONLY",
    "SCHEDULER_DISABLED",
    "EVENT_APPEND_DISABLED",
    "OUTCOME_BINDING_DISABLED",
    "PAPER_SHADOW_DISABLED",
    "PRODUCTION_DISABLED",
    "BROKER_ACTION_DISABLED",
]

EXPLICIT_NON_APPROVAL_LIST = [
    "Scheduler enablement is not approved.",
    "Automated cadence is not approved.",
    "Event append is not approved.",
    "Outcome binding is not approved.",
    "Paper-shadow mode is not approved.",
    "Production path is not approved.",
    "Broker action is not approved.",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "NO_AUTOMATED_CADENCE",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "GAP_CLOSURE_REQUIRED",
]

GUARDRAIL_SUMMARY: dict[str, Any] = {
    "scheduler_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "event_append_enabled": False,
    "outcome_binding_enabled": False,
    "paper_shadow_enabled": False,
    "production_enabled": False,
    "broker_action_enabled": False,
    "promotion_allowed": False,
}

SIDE_EFFECT_SUMMARY: dict[str, bool] = {
    "real_scheduler_created": False,
    "cron_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "event_append_attempted": False,
    "outcome_binding_attempted": False,
    "paper_shadow_attempted": False,
    "production_attempted": False,
    "broker_action_attempted": False,
}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "owner_review_decision_record_only": True,
    "owner_decision_recorded": True,
    "owner_review_recorded": True,
    "owner_decision": OWNER_DECISION,
    "prior_validated_artifacts_only": True,
    "source_artifacts_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "automated_cadence_enabled": False,
    "automated_cadence_created": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "event_append_enabled": False,
    "event_append_attempted": False,
    "event_append_executed": False,
    "outcome_binding_enabled": False,
    "outcome_binding_attempted": False,
    "outcome_binding_executed": False,
    "paper_shadow_enabled": False,
    "paper_shadow_attempted": False,
    "paper_shadow_allowed": False,
    "production_enabled": False,
    "production_attempted": False,
    "production_allowed": False,
    "broker_action_enabled": False,
    "broker_action_attempted": False,
    "broker_action_allowed": False,
    "broker_action": "none",
    "promotion_allowed": False,
    "promotion_decision": "BLOCKED",
    "manual_review_required": True,
    "owner_review_required": True,
    "real_scheduler_created": False,
    "cron_created": False,
    "cron_entry_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "github_action_schedule_created": False,
    "event_log_mutated": False,
    "outcome_store_mutated": False,
    "fresh_market_data_read": False,
    "new_signal_generated": False,
    "backtest_run": False,
    "daily_report_generated": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

FALSE_SAFETY_FIELDS = {
    "scheduler_enabled",
    "scheduler_default_enabled",
    "automated_cadence_enabled",
    "automated_cadence_created",
    "manual_run_executed",
    "event_append_enabled",
    "event_append_attempted",
    "event_append_executed",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "outcome_binding_executed",
    "paper_shadow_enabled",
    "paper_shadow_attempted",
    "paper_shadow_allowed",
    "production_enabled",
    "production_attempted",
    "production_allowed",
    "broker_action_enabled",
    "broker_action_attempted",
    "broker_action_allowed",
    "promotion_allowed",
    "real_scheduler_created",
    "cron_created",
    "cron_entry_created",
    "windows_task_created",
    "github_actions_schedule_created",
    "github_action_schedule_created",
    "event_log_mutated",
    "historical_event_log_mutated",
    "outcome_store_mutated",
    "fresh_market_data_read",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
    "target_weight_action_allowed",
    "target_weight_generated",
    "target_weight_action_generated",
    "rebalance_instruction_allowed",
    "rebalance_instruction_generated",
    "broker_order_generated",
    "broker_action_requested",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "paper_shadow_ready",
    "production_ready",
    "broker_ready",
}

FORBIDDEN_EMIT_FIELDS = {
    "target_weight",
    "target_weight_action",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
    "automatic_exposure_cap",
}

REAL_SCHEDULER_FIELDS = {
    "external_scheduler_entry_created",
    "real_scheduler_created",
    "cron_created",
    "cron_entry_created",
    "windows_task_created",
    "github_actions_schedule_created",
    "github_action_schedule_created",
    "daily_scheduler_entry_created",
}

FORBIDDEN_TRUE_FIELDS = {
    "event_log_mutated",
    "historical_event_log_mutated",
    "outcome_store_mutated",
    "fresh_market_data_read",
    "scheduled_tasks_config_modified",
    "broker_action_taken",
    "order_sent",
    "send_order",
    "manual_run_executed",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
}


class HighIntensitySchedulerOwnerReviewDecisionError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_owner_review_decision(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    replay_validation_dir: Path = DEFAULT_REPLAY_VALIDATION_ROOT,
    audit_package_dir: Path = DEFAULT_AUDIT_PACKAGE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    owner_decision: str = OWNER_DECISION,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"high-intensity scheduler owner review decision only supports {MODE} mode"
        )
    if owner_decision != OWNER_DECISION:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"TRADING-2353 only supports owner_decision={OWNER_DECISION}"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_owner_review_decision_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
    )
    source_review = build_owner_decision_source_artifact_review(inputs=inputs)
    explicit_non_approval = build_explicit_non_approval_list(
        generated_at=generated_at,
        source_review=source_review,
    )
    decision_reasons = build_owner_decision_reasons(
        generated_at=generated_at,
        explicit_non_approval=explicit_non_approval,
    )
    decision_record = build_owner_review_decision_record(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
        explicit_non_approval=explicit_non_approval,
        decision_reasons=decision_reasons,
        owner_decision=owner_decision,
    )
    route = build_high_intensity_2354_gap_closure_route(
        decision_record=decision_record,
        source_review=source_review,
    )
    interpretation_boundary = build_owner_decision_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_owner_decision_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_owner_decision_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        source_review=source_review,
        explicit_non_approval=explicit_non_approval,
        decision_reasons=decision_reasons,
        decision_record=decision_record,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "decision_record": decision_record,
            "source_review": source_review,
            "explicit_non_approval": explicit_non_approval,
            "decision_reasons": decision_reasons,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_owner_decision_outputs(
        paths=paths,
        summary=summary,
        decision_record=decision_record,
        source_review=source_review,
        explicit_non_approval=explicit_non_approval,
        decision_reasons=decision_reasons,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_owner_review_decision_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_audit_package_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
        )
    except Exception as exc:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"TRADING-2353 upstream 2347/2348/2349/2350/2351 validation failed: {exc}"
        ) from exc

    audit_paths = {
        "summary": audit_package_dir / "high_intensity_scheduler_audit_package_summary.json",
        "package": audit_package_dir
        / "high_intensity_risk_cap_observe_only_scheduler_audit_package.json",
        "source_review": audit_package_dir
        / "high_intensity_scheduler_audit_package_source_artifact_review.json",
        "evidence_chain": audit_package_dir
        / "high_intensity_scheduler_audit_package_evidence_chain.json",
        "guardrail_summary": audit_package_dir
        / "high_intensity_scheduler_audit_package_guardrail_summary.json",
        "side_effect_summary": audit_package_dir
        / "high_intensity_scheduler_audit_package_side_effect_summary.json",
        "owner_review_checklist": audit_package_dir
        / "high_intensity_scheduler_owner_review_checklist.json",
        "promotion_decision": audit_package_dir
        / "high_intensity_scheduler_audit_package_promotion_decision.json",
        "route": audit_package_dir / "high_intensity_2353_owner_review_decision_route.json",
        "interpretation_boundary": audit_package_dir
        / "high_intensity_scheduler_audit_package_interpretation_boundary.json",
        "safety_boundary": audit_package_dir
        / "high_intensity_scheduler_audit_package_safety_boundary.json",
    }
    audit_payloads = _load_required_payloads(
        audit_paths,
        "TRADING-2352 scheduler audit package",
    )
    for key, payload in audit_payloads.items():
        label = f"TRADING-2352 audit package {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_2352_source_contracts(audit_payloads)
    _validate_cross_source_contracts(source_inputs, audit_payloads)
    return {
        **source_inputs,
        "audit_package_dir": str(audit_package_dir),
        "audit_package_paths": _string_paths(audit_paths),
        "audit_package": audit_payloads,
    }


def build_owner_decision_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    audit = mapping(inputs["audit_package"])
    audit_summary = mapping(audit["summary"])
    audit_package = mapping(audit["package"])
    audit_source_review = mapping(audit["source_review"])
    source_task_ids = list(audit_source_review.get("source_task_ids", []))
    source_task_ids.append(SOURCE_2352_TASK_REGISTER_ID)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.source_artifact_review.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_task_ids,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "audit_package_artifacts_read": True,
            "audit_package_artifact_count": len(mapping(inputs["audit_package_paths"])),
            "audit_package_status": audit_summary.get("status"),
            "audit_package_package_status": audit_package.get("status"),
            "audit_package_readiness": audit_summary.get("readiness"),
            "audit_package_next_route": audit_summary.get("next_route"),
            "evidence_chain_complete": audit_summary.get("evidence_chain_complete"),
            "owner_review_required": audit_summary.get("owner_review_required"),
            "manual_review_required": audit_summary.get("manual_review_required"),
            "promotion_decision": audit_summary.get("promotion_decision"),
            "promotion_allowed": audit_summary.get("promotion_allowed"),
            "source_validate_data_executed": audit_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": audit_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": audit_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": audit_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_explicit_non_approval_list(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.explicit_non_approval.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
            "scheduler_enablement_approved": False,
            "automated_cadence_approved": False,
            "event_append_approved": False,
            "outcome_binding_approved": False,
            "paper_shadow_approved": False,
            "production_approved": False,
            "broker_action_approved": False,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2354_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_owner_decision_reasons(
    *,
    generated_at: datetime,
    explicit_non_approval: Mapping[str, Any],
) -> dict[str, Any]:
    complete = set(explicit_non_approval.get("explicit_non_approval_list", [])) == set(
        EXPLICIT_NON_APPROVAL_LIST
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.decision_reasons.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if complete else "OWNER_DECISION_REASONS_BLOCKED",
            "owner_decision": OWNER_DECISION,
            "owner_decision_reasons": list(OWNER_DECISION_REASONS),
            "explicit_non_approval_complete": complete,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2354_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_owner_review_decision_record(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_review: Mapping[str, Any],
    explicit_non_approval: Mapping[str, Any],
    decision_reasons: Mapping[str, Any],
    owner_decision: str,
) -> dict[str, Any]:
    audit = mapping(inputs["audit_package"])
    audit_package = mapping(audit["package"])
    side_effect_summary = mapping(
        mapping(audit["side_effect_summary"]).get("side_effect_summary")
    )
    guardrail_summary = mapping(
        mapping(audit["guardrail_summary"]).get("guardrail_summary")
    )
    ready = (
        source_review.get("evidence_chain_complete") is True
        and owner_decision == OWNER_DECISION
        and decision_reasons.get("explicit_non_approval_complete") is True
        and audit_package.get("promotion_decision") == "BLOCKED"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.decision_record.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": STATUS if ready else "OWNER_REVIEW_DECISION_RECORD_BLOCKED",
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "source_task_evidence": audit_package.get("source_task_evidence"),
            "evidence_chain_complete": source_review.get("evidence_chain_complete"),
            "owner_review_recorded": True,
            "owner_decision_recorded": True,
            "owner_decision": owner_decision,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "scheduler_enabled": False,
            "manual_run_only": True,
            "dry_run_only": True,
            "manual_run_executed": False,
            "manual_review_required": True,
            "owner_review_required": True,
            "guardrail_summary": dict(GUARDRAIL_SUMMARY),
            "source_guardrail_summary": guardrail_summary,
            "side_effect_summary": dict(SIDE_EFFECT_SUMMARY),
            "source_side_effect_summary": side_effect_summary,
            "explicit_non_approval_list": explicit_non_approval.get(
                "explicit_non_approval_list"
            ),
            "owner_decision_reasons": decision_reasons.get(
                "owner_decision_reasons"
            ),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2354_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2354_gap_closure_route(
    *,
    decision_record: Mapping[str, Any],
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if decision_record.get("status") != STATUS:
        blockers.append("OWNER_DECISION_RECORD_STATUS_NOT_READY")
    if source_review.get("evidence_chain_complete") is not True:
        blockers.append("SOURCE_EVIDENCE_CHAIN_INCOMPLETE")
    if decision_record.get("owner_decision") != OWNER_DECISION:
        blockers.append("OWNER_DECISION_NOT_KEEP_DISABLED")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_route = (
        "TRADING-2353_Observe_Only_Scheduler_Owner_Decision_Record_Remediation"
        if blockers
        else NEXT_2354_ROUTE
    )
    rationale = (
        "owner review decision record can enter gap closure and readiness hardening"
        if not blockers
        else "owner review decision record failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2354_route.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": readiness,
            "next_task": next_route,
            "next_route": next_route,
            "route_blockers": blockers,
            "route_caveats": list(ROUTE_CAVEATS) if not blockers else [],
            "route_rationale": rationale,
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            **SAFETY_FIELDS,
        }
    )


def build_owner_decision_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "next_route": route.get("next_route"),
            "interpretation": (
                "owner review decision record only; the recorded decision keeps "
                "scheduler disabled and promotion blocked. It is not scheduler "
                "activation, automated cadence approval, event append, outcome "
                "binding, paper-shadow approval, production readiness, or broker "
                "execution."
            ),
            "not_scheduler_enablement": True,
            "not_automated_cadence": True,
            "not_manual_run_execution": True,
            "not_event_append": True,
            "not_outcome_binding": True,
            "not_paper_shadow_promotion": True,
            "not_production_wiring": True,
            "not_broker_execution": True,
            **SAFETY_FIELDS,
        }
    )


def build_owner_decision_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "next_route": route.get("next_route"),
            "forbidden_actions": [
                "enable_scheduler",
                "create_cron_job",
                "create_windows_task",
                "create_github_actions_schedule",
                "append_historical_event_log",
                "mutate_event_log",
                "bind_outcome",
                "mutate_outcome_store",
                "enable_paper_shadow",
                "enable_production",
                "call_broker_api",
                "send_order",
                "read_fresh_market_data",
                "generate_new_signal",
                "run_backtest",
                "generate_daily_report",
                "emit_target_weight",
                "emit_rebalance_instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_owner_decision_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    source_review: Mapping[str, Any],
    explicit_non_approval: Mapping[str, Any],
    decision_reasons: Mapping[str, Any],
    decision_record: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Observe-Only Scheduler Owner Review Decision",
            "status": decision_record.get("status"),
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "disabled_wiring_dir": str(disabled_wiring_dir),
            "smoke_dry_run_dir": str(smoke_dry_run_dir),
            "manual_review_gate_dir": str(manual_review_gate_dir),
            "manual_run_dry_run_dir": str(manual_run_dry_run_dir),
            "replay_validation_dir": str(replay_validation_dir),
            "audit_package_dir": str(audit_package_dir),
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": source_review.get("evidence_chain_complete"),
            "owner_review_recorded": True,
            "owner_decision_recorded": True,
            "owner_decision": decision_record.get("owner_decision"),
            "explicit_non_approval_list": explicit_non_approval.get(
                "explicit_non_approval_list"
            ),
            "owner_decision_reasons": decision_reasons.get(
                "owner_decision_reasons"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": decision_record.get("guardrail_summary"),
            "side_effect_summary": decision_record.get("side_effect_summary"),
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            "next_task": route.get("next_task"),
            "source_validate_data_executed": source_review.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": source_review.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": source_review.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": source_review.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2353 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, and "
                "TRADING-2352 audit package artifacts; it does not consume fresh "
                "market data, append events, bind outcomes, produce technical "
                "features, score, backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_owner_decision_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    decision_record: Mapping[str, Any],
    source_review: Mapping[str, Any],
    explicit_non_approval: Mapping[str, Any],
    decision_reasons: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["decision_record"], decision_record)
    write_json(paths["source_review"], source_review)
    write_json(paths["explicit_non_approval"], explicit_non_approval)
    write_json(paths["decision_reasons"], decision_reasons)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["decision_doc"], render_owner_decision_doc(decision_record))
    write_markdown(paths["route_doc"], render_2354_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_owner_decision_doc(decision_record: Mapping[str, Any]) -> str:
    source_rows = [
        "|Task|Status|Evidence|Promotion Result|",
        "|---|---|---|---|",
    ]
    for row in decision_record.get("source_task_evidence", []):
        item = mapping(row)
        source_rows.append(
            "|"
            f"{item.get('task')}|"
            f"{item.get('status')}|"
            f"{item.get('evidence')}|"
            f"{item.get('promotion_result')}|"
        )
    guardrails = _markdown_table_from_mapping(decision_record.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(decision_record.get("side_effect_summary", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Owner Review Decision",
            "",
            "## Executive Summary",
            "",
            f"- task_id: `{decision_record.get('task_id')}`",
            f"- task_register_id: `{decision_record.get('task_register_id')}`",
            f"- status: `{decision_record.get('status')}`",
            f"- source_tasks: `{decision_record.get('source_tasks')}`",
            (
                "- evidence_chain_complete: "
                f"`{decision_record.get('evidence_chain_complete')}`"
            ),
            f"- owner_review_recorded: `{decision_record.get('owner_review_recorded')}`",
            f"- owner_decision: `{decision_record.get('owner_decision')}`",
            f"- promotion_decision: `{decision_record.get('promotion_decision')}`",
            f"- promotion_allowed: `{decision_record.get('promotion_allowed')}`",
            f"- readiness: `{decision_record.get('readiness')}`",
            f"- next_route: `{decision_record.get('next_route')}`",
            "",
            "## Source Evidence Chain",
            "",
            *source_rows,
            "",
            "## Owner Review Decision",
            "",
            f"`{decision_record.get('owner_decision')}`",
            "",
            "## Explicit Non-Approval List",
            "",
            *[
                f"- {item}"
                for item in decision_record.get("explicit_non_approval_list", [])
            ],
            "",
            "## Guardrail Status Table",
            "",
            *guardrails,
            "",
            "## Side-Effect Status Table",
            "",
            *side_effects,
            "",
            "## Decision Reasons",
            "",
            *[
                f"- `{reason}`"
                for reason in decision_record.get("owner_decision_reasons", [])
            ],
            "",
            "## Known Caveats",
            "",
            "- Owner decision keeps scheduler disabled and promotion blocked.",
            "- No real scheduler is enabled.",
            "- No automated cadence has been created.",
            "- No event append is allowed.",
            "- No outcome binding is allowed.",
            "- No paper-shadow mode is allowed.",
            "- No production path is allowed.",
            "- No broker action is allowed.",
            "",
            "## Next Route",
            "",
            f"`{decision_record.get('next_route')}`",
        ]
    )


def render_2354_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2354 Gap Closure Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2354 route 只能进入 gap closure and readiness hardening plan。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是 event append、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_scheduler_owner_review_decision_summary.json",
        "decision_record": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision.json",
        "source_review": output_dir
        / "high_intensity_scheduler_owner_review_decision_source_artifact_review.json",
        "explicit_non_approval": output_dir
        / "high_intensity_scheduler_owner_review_explicit_non_approval.json",
        "decision_reasons": output_dir
        / "high_intensity_scheduler_owner_review_decision_reasons.json",
        "route": output_dir / "high_intensity_2354_gap_closure_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_owner_review_decision_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_owner_review_decision_safety_boundary.json",
        "decision_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision.md",
        "route_doc": docs_root / "high_intensity_2354_gap_closure_route.md",
    }


def _validate_2352_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    evidence_chain = mapping(payloads["evidence_chain"])
    guardrail_summary = mapping(payloads["guardrail_summary"])
    side_effect_summary = mapping(payloads["side_effect_summary"])
    owner_review_checklist = mapping(payloads["owner_review_checklist"])
    promotion_decision = mapping(payloads["promotion_decision"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2352_STATUS:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"TRADING-2353 requires 2352 status {EXPECTED_2352_STATUS}"
        )
    if package.get("status") != EXPECTED_2352_STATUS:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 package status"
        )
    if summary.get("readiness") != EXPECTED_2352_READINESS:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 readiness READY_FOR_2353_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2352_NEXT_ROUTE:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 summary route to owner decision record"
        )
    if route.get("next_route") != EXPECTED_2352_NEXT_ROUTE:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 route to owner decision record"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 source contract status PASS"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 evidence_chain_complete=true"
        )
    if evidence_chain.get("evidence_chain_complete") is not True:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 evidence chain artifact complete"
        )
    if summary.get("owner_review_required") is not True:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 owner_review_required=true"
        )
    if owner_review_checklist.get("owner_review_required") is not True:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 owner review checklist required"
        )
    if promotion_decision.get("promotion_decision") != "BLOCKED":
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 promotion decision BLOCKED"
        )
    if promotion_decision.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 promotion_allowed=false"
        )
    if package.get("source_tasks") != SOURCE_TASKS[:-1]:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 source tasks to be 2347..2351"
        )
    _validate_source_data_quality(summary, "TRADING-2352 summary")

    _validate_guardrail_summary(mapping(guardrail_summary.get("guardrail_summary")))
    _validate_side_effect_summary(mapping(side_effect_summary.get("side_effect_summary")))


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    audit_payloads: Mapping[str, Any],
) -> None:
    replay_summary = mapping(mapping(source_inputs["replay_validation"])["summary"])
    audit_source_review = mapping(audit_payloads["source_review"])
    if audit_source_review.get(
        "manual_run_replay_validation_status"
    ) != replay_summary.get("status"):
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            "TRADING-2353 requires 2352 source review replay status to match 2351"
        )


def _validate_guardrail_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in GUARDRAIL_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerOwnerReviewDecisionError(
                f"TRADING-2353 requires 2352 guardrail {field}={expected}"
            )


def _validate_side_effect_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in SIDE_EFFECT_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerOwnerReviewDecisionError(
                f"TRADING-2353 requires 2352 side effect {field}=false"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2353 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerOwnerReviewDecisionError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = collect_unsafe_fields(
        payload,
        false_fields=FALSE_SAFETY_FIELDS,
        forbidden_emit_fields=FORBIDDEN_EMIT_FIELDS,
    )
    if violations:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _validate_no_real_scheduler_creation(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = collect_real_scheduler_creation_fields(
        payload,
        real_scheduler_fields=REAL_SCHEDULER_FIELDS,
    )
    if violations:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerOwnerReviewDecisionError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerOwnerReviewDecisionError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerOwnerReviewDecisionError(
            f"{label} requires broker_action=none"
        )


def _collect_forbidden_true_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in FORBIDDEN_TRUE_FIELDS and item is True:
                violations.append(path)
            violations.extend(_collect_forbidden_true_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_forbidden_true_fields(item, f"{prefix}[{index}]"))
    return violations


def _markdown_table_from_mapping(payload: object) -> list[str]:
    values = mapping(payload)
    lines = ["|Field|Value|", "|---|---|"]
    for key, value in values.items():
        lines.append(f"|`{key}`|`{value}`|")
    return lines


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
