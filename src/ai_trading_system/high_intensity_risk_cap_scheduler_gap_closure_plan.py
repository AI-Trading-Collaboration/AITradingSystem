from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_scheduler_common import (
    collect_real_scheduler_creation_fields,
    collect_unsafe_fields,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_scheduler_owner_review_decision_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_OWNER_DECISION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    NEXT_2354_ROUTE as EXPECTED_2353_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    READINESS_STATUS as EXPECTED_2353_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    STATUS as EXPECTED_2353_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    TASK_REGISTER_ID as SOURCE_2353_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2354"
TASK_REGISTER_ID = (
    "TRADING-2354_OBSERVE_ONLY_SCHEDULER_GAP_CLOSURE_AND_READINESS_"
    "HARDENING_PLAN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_gap_closure_plan"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_GAP_CLOSURE_AND_READINESS_HARDENING_PLAN_READY_"
    "WITH_CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2355_WITH_CAVEATS"
NEXT_2355_ROUTE = (
    "TRADING-2355_Observe_Only_Scheduler_Hardening_Backlog_And_Evidence_Matrix"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SOURCE_TASKS = [
    "TRADING-2347",
    "TRADING-2348",
    "TRADING-2349",
    "TRADING-2350",
    "TRADING-2351",
    "TRADING-2352",
    "TRADING-2353",
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

GAP_CLOSURE_MATRIX: dict[str, dict[str, Any]] = {
    "scheduler_enablement": {
        "current_status": "BLOCKED",
        "missing_evidence": [
            "explicit_owner_approval",
            "automated_cadence_safety_review",
            "scheduler_kill_switch_validation",
            "scheduler_idempotency_validation",
            "scheduler_failure_mode_review",
        ],
        "allowed_in_this_task": False,
    },
    "event_append": {
        "current_status": "BLOCKED",
        "missing_evidence": [
            "event_schema_append_contract",
            "append_idempotency_validation",
            "append_rollback_plan",
            "duplicate_detection_validation",
            "owner_approval_for_event_mutation",
        ],
        "allowed_in_this_task": False,
    },
    "outcome_binding": {
        "current_status": "BLOCKED",
        "missing_evidence": [
            "outcome_binding_contract",
            "outcome_store_mutation_guardrail",
            "binding_replay_validation",
            "owner_approval_for_outcome_mutation",
        ],
        "allowed_in_this_task": False,
    },
    "paper_shadow": {
        "current_status": "BLOCKED",
        "missing_evidence": [
            "paper_shadow_scope_definition",
            "paper_shadow_no_broker_guardrail",
            "paper_shadow_owner_approval",
            "paper_shadow_daily_review_plan",
        ],
        "allowed_in_this_task": False,
    },
    "production": {
        "current_status": "BLOCKED",
        "missing_evidence": [
            "production_risk_review",
            "production_rollback_plan",
            "capital_at_risk_limit",
            "operator_approval_protocol",
        ],
        "allowed_in_this_task": False,
    },
    "broker_action": {
        "current_status": "BLOCKED",
        "missing_evidence": [
            "broker_api_safety_contract",
            "order_dry_run_validation",
            "capital_limit_guardrail",
            "human_confirmation_protocol",
            "explicit_owner_approval",
        ],
        "allowed_in_this_task": False,
    },
}

HARDENING_RECOMMENDATIONS = [
    "Define scheduler kill-switch and disabled-by-default enforcement tests.",
    "Define append-only event mutation contract without enabling event append.",
    "Define outcome binding replay contract without mutating outcome store.",
    (
        "Define paper-shadow scope and no-broker guardrail before any "
        "paper-shadow approval."
    ),
    (
        "Define production and broker non-goals as hard blockers until "
        "explicit owner approval."
    ),
]

EXPLICIT_NON_GOALS = [
    "This task does not enable scheduler.",
    "This task does not create automated cadence.",
    "This task does not append events.",
    "This task does not bind outcomes.",
    "This task does not enable paper-shadow.",
    "This task does not enable production.",
    "This task does not call broker APIs.",
    "This task does not approve promotion.",
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "SCHEDULER_ENABLEMENT_EVIDENCE_GAPS_REMAIN",
    "EVENT_APPEND_GUARDRAIL_GAPS_REMAIN",
    "OUTCOME_BINDING_VALIDATION_GAPS_REMAIN",
    "PAPER_SHADOW_OWNER_APPROVAL_AND_DRY_RUN_GAPS_REMAIN",
    "PRODUCTION_RISK_AND_OPERATOR_APPROVAL_GAPS_REMAIN",
    "BROKER_ACTION_SAFETY_AND_HUMAN_CONFIRMATION_GAPS_REMAIN",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "GAP_CLOSURE_PLAN_ONLY",
    "READINESS_HARDENING_PLAN_ONLY",
    "NO_AUTOMATED_CADENCE",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "HARDENING_BACKLOG_REQUIRED",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "gap_closure_plan_only": True,
    "readiness_hardening_plan_only": True,
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


class HighIntensitySchedulerGapClosurePlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    replay_validation_dir: Path = DEFAULT_REPLAY_VALIDATION_ROOT,
    audit_package_dir: Path = DEFAULT_AUDIT_PACKAGE_ROOT,
    owner_decision_dir: Path = DEFAULT_OWNER_DECISION_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"high-intensity scheduler gap closure plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_gap_closure_plan_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
    )
    source_review = build_gap_closure_source_artifact_review(inputs=inputs)
    gap_closure_matrix = build_gap_closure_matrix(
        generated_at=generated_at,
        source_review=source_review,
    )
    readiness_hardening_plan = build_readiness_hardening_plan(
        generated_at=generated_at,
        gap_closure_matrix=gap_closure_matrix,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
        gap_closure_matrix=gap_closure_matrix,
    )
    plan = build_gap_closure_plan(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
        gap_closure_matrix=gap_closure_matrix,
        readiness_hardening_plan=readiness_hardening_plan,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2355_hardening_backlog_route(plan=plan)
    interpretation_boundary = build_gap_closure_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_gap_closure_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_gap_closure_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
        source_review=source_review,
        gap_closure_matrix=gap_closure_matrix,
        readiness_hardening_plan=readiness_hardening_plan,
        blocked_promotion_rationale=blocked_promotion_rationale,
        plan=plan,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "plan": plan,
            "source_review": source_review,
            "gap_closure_matrix": gap_closure_matrix,
            "readiness_hardening_plan": readiness_hardening_plan,
            "blocked_promotion_rationale": blocked_promotion_rationale,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_gap_closure_outputs(
        paths=paths,
        summary=summary,
        plan=plan,
        source_review=source_review,
        gap_closure_matrix=gap_closure_matrix,
        readiness_hardening_plan=readiness_hardening_plan,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_gap_closure_plan_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    owner_decision_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_owner_review_decision_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
            audit_package_dir=audit_package_dir,
        )
    except Exception as exc:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 upstream 2347/2348/2349/2350/2351/2352 "
            f"validation failed: {exc}"
        ) from exc

    owner_decision_paths = {
        "summary": owner_decision_dir
        / "high_intensity_scheduler_owner_review_decision_summary.json",
        "decision_record": owner_decision_dir
        / "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision.json",
        "source_review": owner_decision_dir
        / "high_intensity_scheduler_owner_review_decision_source_artifact_review.json",
        "explicit_non_approval": owner_decision_dir
        / "high_intensity_scheduler_owner_review_explicit_non_approval.json",
        "decision_reasons": owner_decision_dir
        / "high_intensity_scheduler_owner_review_decision_reasons.json",
        "route": owner_decision_dir / "high_intensity_2354_gap_closure_route.json",
        "interpretation_boundary": owner_decision_dir
        / "high_intensity_scheduler_owner_review_decision_interpretation_boundary.json",
        "safety_boundary": owner_decision_dir
        / "high_intensity_scheduler_owner_review_decision_safety_boundary.json",
    }
    owner_decision_payloads = _load_required_payloads(
        owner_decision_paths,
        "TRADING-2353 scheduler owner review decision",
    )
    for key, payload in owner_decision_payloads.items():
        label = f"TRADING-2353 owner review decision {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_2353_source_contracts(owner_decision_payloads)
    _validate_cross_source_contracts(source_inputs, owner_decision_payloads)
    return {
        **source_inputs,
        "owner_decision_dir": str(owner_decision_dir),
        "owner_decision_paths": _string_paths(owner_decision_paths),
        "owner_decision": owner_decision_payloads,
    }


def build_gap_closure_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    owner = mapping(inputs["owner_decision"])
    owner_summary = mapping(owner["summary"])
    decision_record = mapping(owner["decision_record"])
    owner_source_review = mapping(owner["source_review"])
    audit_summary = mapping(mapping(inputs["audit_package"])["summary"])
    source_task_ids = list(owner_source_review.get("source_task_ids", []))
    source_task_ids.append(SOURCE_2353_TASK_REGISTER_ID)
    source_task_evidence = [
        dict(mapping(row))
        for row in decision_record.get("source_task_evidence", [])
    ]
    evidence_tasks = {
        str(mapping(row).get("task"))
        for row in source_task_evidence
    }
    if "TRADING-2352" not in evidence_tasks:
        source_task_evidence.append(
            {
                "task": "TRADING-2352",
                "status": audit_summary.get("status"),
                "evidence": "scheduler audit package and owner review checklist present",
                "evidence_present": True,
                "promotion_result": "blocked",
            }
        )
    if "TRADING-2353" not in evidence_tasks:
        source_task_evidence.append(
            {
                "task": "TRADING-2353",
                "status": owner_summary.get("status"),
                "evidence": "owner review decision record present",
                "evidence_present": True,
                "promotion_result": "blocked",
            }
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.source_artifact_review.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_task_ids,
            "source_task_evidence": source_task_evidence,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "owner_decision_artifacts_read": True,
            "owner_decision_artifact_count": len(
                mapping(inputs["owner_decision_paths"])
            ),
            "owner_decision_status": owner_summary.get("status"),
            "owner_decision_record_status": decision_record.get("status"),
            "owner_decision_readiness": owner_summary.get("readiness"),
            "owner_decision_next_route": owner_summary.get("next_route"),
            "evidence_chain_complete": owner_summary.get("evidence_chain_complete"),
            "owner_review_recorded": owner_summary.get("owner_review_recorded"),
            "owner_decision_recorded": owner_summary.get("owner_decision_recorded"),
            "owner_decision": owner_summary.get("owner_decision"),
            "promotion_decision": owner_summary.get("promotion_decision"),
            "promotion_allowed": owner_summary.get("promotion_allowed"),
            "source_validate_data_executed": owner_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": owner_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": owner_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": owner_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_gap_closure_matrix(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.gap_closure_matrix.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "gap_closure_plan_ready": True,
            "gap_closure_matrix": dict(GAP_CLOSURE_MATRIX),
            "all_items_blocked": True,
            "all_items_plan_only": True,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2355_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_readiness_hardening_plan(
    *,
    generated_at: datetime,
    gap_closure_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    gap_keys = sorted(mapping(gap_closure_matrix["gap_closure_matrix"]).keys())
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.readiness_hardening_plan.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "readiness_hardening_plan_ready": True,
            "hardening_scope": gap_keys,
            "hardening_recommendations": list(HARDENING_RECOMMENDATIONS),
            "recommended_next_hardening_tasks": [
                "scheduler_kill_switch_and_idempotency_contract",
                "event_append_contract_and_duplicate_detection_plan",
                "outcome_binding_replay_and_mutation_guardrail_plan",
                "paper_shadow_scope_and_no_broker_guardrail_plan",
                "production_and_broker_blocker_evidence_matrix",
            ],
            "execution_allowed_in_this_task": False,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2355_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_blocked_promotion_rationale(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    gap_closure_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.blocked_promotion_rationale.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "owner_decision": source_review.get("owner_decision"),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocked_promotion_reasons": list(BLOCKED_PROMOTION_REASONS),
            "gap_categories": list(
                mapping(gap_closure_matrix["gap_closure_matrix"]).keys()
            ),
            "reason_summary": (
                "Promotion remains blocked because owner decision keeps the "
                "scheduler disabled and the required scheduler, event, outcome, "
                "paper-shadow, production, and broker hardening evidence is not "
                "complete."
            ),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2355_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_gap_closure_plan(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_review: Mapping[str, Any],
    gap_closure_matrix: Mapping[str, Any],
    readiness_hardening_plan: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    ready = (
        source_review.get("evidence_chain_complete") is True
        and source_review.get("owner_decision") == OWNER_DECISION
        and gap_closure_matrix.get("gap_closure_plan_ready") is True
        and readiness_hardening_plan.get("readiness_hardening_plan_ready") is True
        and blocked_promotion_rationale.get("promotion_allowed") is False
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.plan.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": STATUS if ready else "GAP_CLOSURE_PLAN_BLOCKED",
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "source_task_evidence": source_review.get("source_task_evidence"),
            "evidence_chain_complete": source_review.get("evidence_chain_complete"),
            "owner_review_recorded": source_review.get("owner_review_recorded"),
            "owner_decision_recorded": source_review.get("owner_decision_recorded"),
            "owner_decision": source_review.get("owner_decision"),
            "gap_closure_plan_ready": gap_closure_matrix.get(
                "gap_closure_plan_ready"
            ),
            "readiness_hardening_plan_ready": readiness_hardening_plan.get(
                "readiness_hardening_plan_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": dict(GUARDRAIL_SUMMARY),
            "side_effect_summary": dict(SIDE_EFFECT_SUMMARY),
            "gap_closure_matrix": gap_closure_matrix.get("gap_closure_matrix"),
            "hardening_recommendations": readiness_hardening_plan.get(
                "hardening_recommendations"
            ),
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": list(EXPLICIT_NON_GOALS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2355_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2355_hardening_backlog_route(
    *,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if plan.get("status") != STATUS:
        blockers.append("GAP_CLOSURE_PLAN_STATUS_NOT_READY")
    if plan.get("gap_closure_plan_ready") is not True:
        blockers.append("GAP_CLOSURE_PLAN_NOT_READY")
    if plan.get("readiness_hardening_plan_ready") is not True:
        blockers.append("READINESS_HARDENING_PLAN_NOT_READY")
    if plan.get("owner_decision") != OWNER_DECISION:
        blockers.append("OWNER_DECISION_NOT_KEEP_DISABLED")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_route = (
        "TRADING-2354_Observe_Only_Scheduler_Gap_Closure_Remediation"
        if blockers
        else NEXT_2355_ROUTE
    )
    rationale = (
        "gap closure plan can enter hardening backlog and evidence matrix"
        if not blockers
        else "gap closure plan failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2355_route.v1",
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


def build_gap_closure_interpretation_boundary(
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
                "gap closure and readiness hardening plan only. It records "
                "missing evidence and next hardening route while keeping "
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


def build_gap_closure_safety_boundary(
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


def build_gap_closure_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    owner_decision_dir: Path,
    source_review: Mapping[str, Any],
    gap_closure_matrix: Mapping[str, Any],
    readiness_hardening_plan: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    plan: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Scheduler Gap Closure "
                "And Readiness Hardening Plan"
            ),
            "status": plan.get("status"),
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
            "owner_decision_dir": str(owner_decision_dir),
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": source_review.get("evidence_chain_complete"),
            "owner_decision": source_review.get("owner_decision"),
            "gap_closure_plan_ready": gap_closure_matrix.get(
                "gap_closure_plan_ready"
            ),
            "readiness_hardening_plan_ready": readiness_hardening_plan.get(
                "readiness_hardening_plan_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": plan.get("guardrail_summary"),
            "side_effect_summary": plan.get("side_effect_summary"),
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
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
                "aits validate-data not rerun because TRADING-2354 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, and TRADING-2353 owner decision "
                "artifacts; it does not consume fresh market data, append "
                "events, bind outcomes, produce technical features, score, "
                "backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_gap_closure_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    plan: Mapping[str, Any],
    source_review: Mapping[str, Any],
    gap_closure_matrix: Mapping[str, Any],
    readiness_hardening_plan: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["plan"], plan)
    write_json(paths["source_review"], source_review)
    write_json(paths["gap_closure_matrix"], gap_closure_matrix)
    write_json(paths["readiness_hardening_plan"], readiness_hardening_plan)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_gap_closure_plan_doc(plan))
    write_markdown(paths["route_doc"], render_2355_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_gap_closure_plan_doc(plan: Mapping[str, Any]) -> str:
    source_rows = [
        "|Task|Status|Evidence|Promotion Result|",
        "|---|---|---|---|",
    ]
    for row in plan.get("source_task_evidence", []):
        item = mapping(row)
        source_rows.append(
            "|"
            f"{item.get('task')}|"
            f"{item.get('status')}|"
            f"{item.get('evidence')}|"
            f"{item.get('promotion_result')}|"
        )
    gap_rows = [
        "|Gap|Current Status|Missing Evidence|Allowed In This Task|",
        "|---|---|---|---|",
    ]
    for key, value in mapping(plan.get("gap_closure_matrix", {})).items():
        item = mapping(value)
        missing = ", ".join(str(x) for x in item.get("missing_evidence", []))
        gap_rows.append(
            "|"
            f"`{key}`|"
            f"`{item.get('current_status')}`|"
            f"{missing}|"
            f"`{item.get('allowed_in_this_task')}`|"
        )
    guardrails = _markdown_table_from_mapping(plan.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(plan.get("side_effect_summary", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Gap Closure Plan",
            "",
            "## Executive Summary",
            "",
            f"- task_id: `{plan.get('task_id')}`",
            f"- task_register_id: `{plan.get('task_register_id')}`",
            f"- status: `{plan.get('status')}`",
            f"- source_tasks: `{plan.get('source_tasks')}`",
            (
                "- evidence_chain_complete: "
                f"`{plan.get('evidence_chain_complete')}`"
            ),
            f"- owner_decision: `{plan.get('owner_decision')}`",
            (
                "- gap_closure_plan_ready: "
                f"`{plan.get('gap_closure_plan_ready')}`"
            ),
            (
                "- readiness_hardening_plan_ready: "
                f"`{plan.get('readiness_hardening_plan_ready')}`"
            ),
            f"- promotion_decision: `{plan.get('promotion_decision')}`",
            f"- promotion_allowed: `{plan.get('promotion_allowed')}`",
            f"- readiness: `{plan.get('readiness')}`",
            f"- next_route: `{plan.get('next_route')}`",
            "",
            "## Source Evidence Chain",
            "",
            *source_rows,
            "",
            "## Owner Decision Recap",
            "",
            f"`{plan.get('owner_decision')}`",
            "",
            "## Current Blocked Promotion Status",
            "",
            *[f"- `{reason}`" for reason in plan.get("blocked_promotion_reasons", [])],
            "",
            "## Gap Closure Matrix",
            "",
            *gap_rows,
            "",
            "## Readiness Hardening Plan",
            "",
            *[
                f"- {item}"
                for item in plan.get("hardening_recommendations", [])
            ],
            "",
            "## Explicit Non-Goals",
            "",
            *[f"- {item}" for item in plan.get("explicit_non_goals", [])],
            "",
            "## Recommended Next Hardening Tasks",
            "",
            "- Build TRADING-2355 hardening backlog and evidence matrix.",
            "- Keep each evidence item plan-only until owner approval exists.",
            "",
            "## Guardrail Status Table",
            "",
            *guardrails,
            "",
            "## Side-Effect Status Table",
            "",
            *side_effects,
            "",
            "## Known Caveats",
            "",
            "- Owner decision keeps scheduler disabled and promotion blocked.",
            "- Gap closure items are missing-evidence entries, not approvals.",
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
            f"`{plan.get('next_route')}`",
        ]
    )


def render_2355_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2355 Hardening Backlog Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2355 route 只能进入 hardening backlog and evidence matrix。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是 event append、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_scheduler_gap_closure_plan_summary.json",
        "plan": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan.json",
        "source_review": output_dir
        / "high_intensity_scheduler_gap_closure_plan_source_artifact_review.json",
        "gap_closure_matrix": output_dir
        / "high_intensity_scheduler_gap_closure_matrix.json",
        "readiness_hardening_plan": output_dir
        / "high_intensity_scheduler_readiness_hardening_plan.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_scheduler_gap_closure_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2355_hardening_backlog_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_gap_closure_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_gap_closure_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan.md",
        "route_doc": docs_root / "high_intensity_2355_hardening_backlog_route.md",
    }


def _validate_2353_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    decision_record = mapping(payloads["decision_record"])
    source_review = mapping(payloads["source_review"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2353_STATUS:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"TRADING-2354 requires 2353 status {EXPECTED_2353_STATUS}"
        )
    if decision_record.get("status") != EXPECTED_2353_STATUS:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 decision record status"
        )
    if summary.get("readiness") != EXPECTED_2353_READINESS:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 readiness READY_FOR_2354_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2353_NEXT_ROUTE:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 summary route to gap closure plan"
        )
    if route.get("next_route") != EXPECTED_2353_NEXT_ROUTE:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 route to gap closure plan"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 source contract status PASS"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 evidence_chain_complete=true"
        )
    if summary.get("owner_review_recorded") is not True:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 owner_review_recorded=true"
        )
    if summary.get("owner_decision_recorded") is not True:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 owner_decision_recorded=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"TRADING-2354 requires owner_decision={OWNER_DECISION}"
        )
    if decision_record.get("owner_decision") != OWNER_DECISION:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"TRADING-2354 requires decision record owner_decision={OWNER_DECISION}"
        )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 promotion_decision=BLOCKED"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 promotion_allowed=false"
        )
    if summary.get("source_tasks") != SOURCE_TASKS[:-1]:
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 source tasks to be 2347..2352"
        )
    _validate_source_data_quality(summary, "TRADING-2353 summary")
    _validate_guardrail_summary(mapping(summary.get("guardrail_summary")))
    _validate_side_effect_summary(mapping(summary.get("side_effect_summary")))


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    owner_payloads: Mapping[str, Any],
) -> None:
    audit_summary = mapping(mapping(source_inputs["audit_package"])["summary"])
    owner_source_review = mapping(owner_payloads["source_review"])
    if owner_source_review.get("audit_package_status") != audit_summary.get("status"):
        raise HighIntensitySchedulerGapClosurePlanError(
            "TRADING-2354 requires 2353 source review audit status to match 2352"
        )


def _validate_guardrail_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in GUARDRAIL_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerGapClosurePlanError(
                f"TRADING-2354 requires 2353 guardrail {field}={expected}"
            )


def _validate_side_effect_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in SIDE_EFFECT_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerGapClosurePlanError(
                f"TRADING-2354 requires 2353 side effect {field}=false"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2354 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerGapClosurePlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerGapClosurePlanError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = collect_unsafe_fields(
        payload,
        false_fields=FALSE_SAFETY_FIELDS,
        forbidden_emit_fields=FORBIDDEN_EMIT_FIELDS,
    )
    violations = [
        item for item in violations if item != "gap_closure_matrix.broker_action"
    ]
    if violations:
        raise HighIntensitySchedulerGapClosurePlanError(
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
        raise HighIntensitySchedulerGapClosurePlanError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerGapClosurePlanError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerGapClosurePlanError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerGapClosurePlanError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerGapClosurePlanError(
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
