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
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_scheduler_gap_closure_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_GAP_CLOSURE_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    NEXT_2355_ROUTE as EXPECTED_2354_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    READINESS_STATUS as EXPECTED_2354_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    STATUS as EXPECTED_2354_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    TASK_REGISTER_ID as SOURCE_2354_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2355"
TASK_REGISTER_ID = (
    "TRADING-2355_OBSERVE_ONLY_SCHEDULER_HARDENING_BACKLOG_AND_"
    "EVIDENCE_MATRIX"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_hardening_backlog"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_HARDENING_BACKLOG_AND_EVIDENCE_MATRIX_READY_"
    "WITH_CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2356_WITH_CAVEATS"
NEXT_2356_ROUTE = (
    "TRADING-2356_Observe_Only_Scheduler_Kill_Switch_And_Disabled_"
    "Enforcement_Evidence_Plan"
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
    "TRADING-2354",
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

HARDENING_BACKLOG: list[dict[str, Any]] = [
    {
        "task_route": NEXT_2356_ROUTE,
        "category": "scheduler_enablement_guardrail",
        "priority": "P0",
        "purpose": (
            "Define kill-switch and disabled-by-default enforcement evidence "
            "before any scheduler enablement discussion."
        ),
        "required_evidence": [
            "kill_switch_contract",
            "disabled_by_default_assertions",
            "no_real_scheduler_creation_assertions",
            "manual_review_required_assertions",
        ],
        "side_effect_allowed": False,
        "owner_approval_required_for_execution": False,
        "owner_approval_required_for_promotion": True,
        "promotion_allowed_after_task": False,
    },
    {
        "task_route": (
            "TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_"
            "Contract_Plan"
        ),
        "category": "scheduler_idempotency",
        "priority": "P0",
        "purpose": (
            "Define idempotency and replay evidence for repeated "
            "scheduler-like dry-runs."
        ),
        "required_evidence": [
            "stable_semantic_hash_contract",
            "timestamp_exclusion_rule",
            "duplicate_detection_plan",
            "replay_no_side_effect_contract",
        ],
        "side_effect_allowed": False,
        "owner_approval_required_for_execution": False,
        "owner_approval_required_for_promotion": True,
        "promotion_allowed_after_task": False,
    },
    {
        "task_route": "TRADING-2358_Observe_Only_Event_Append_Contract_Plan",
        "category": "event_append_guardrail",
        "priority": "P1",
        "purpose": "Define event append contract without enabling event mutation.",
        "required_evidence": [
            "event_schema_append_contract",
            "append_idempotency_plan",
            "duplicate_detection_plan",
            "rollback_plan",
        ],
        "side_effect_allowed": False,
        "owner_approval_required_for_execution": False,
        "owner_approval_required_for_promotion": True,
        "promotion_allowed_after_task": False,
    },
    {
        "task_route": "TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan",
        "category": "outcome_binding_guardrail",
        "priority": "P1",
        "purpose": "Define outcome binding contract without mutating outcome store.",
        "required_evidence": [
            "outcome_binding_contract",
            "outcome_store_mutation_guardrail",
            "binding_replay_plan",
            "rollback_plan",
        ],
        "side_effect_allowed": False,
        "owner_approval_required_for_execution": False,
        "owner_approval_required_for_promotion": True,
        "promotion_allowed_after_task": False,
    },
    {
        "task_route": (
            "TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_"
            "Guardrail_Plan"
        ),
        "category": "paper_shadow_guardrail",
        "priority": "P2",
        "purpose": (
            "Define paper-shadow scope and no-broker guardrail without "
            "enabling paper-shadow."
        ),
        "required_evidence": [
            "paper_shadow_scope_definition",
            "paper_shadow_no_broker_guardrail",
            "daily_review_plan",
            "owner_approval_requirement",
        ],
        "side_effect_allowed": False,
        "owner_approval_required_for_execution": False,
        "owner_approval_required_for_promotion": True,
        "promotion_allowed_after_task": False,
    },
]

EVIDENCE_MATRIX: dict[str, dict[str, Any]] = {
    "scheduler_kill_switch": {
        "priority": "P0",
        "current_status": "MISSING",
        "required_before_scheduler_enablement": True,
        "side_effect_allowed": False,
    },
    "scheduler_idempotency": {
        "priority": "P0",
        "current_status": "MISSING",
        "required_before_scheduler_enablement": True,
        "side_effect_allowed": False,
    },
    "event_append_contract": {
        "priority": "P1",
        "current_status": "MISSING",
        "required_before_event_append": True,
        "side_effect_allowed": False,
    },
    "outcome_binding_contract": {
        "priority": "P1",
        "current_status": "MISSING",
        "required_before_outcome_binding": True,
        "side_effect_allowed": False,
    },
    "paper_shadow_scope": {
        "priority": "P2",
        "current_status": "MISSING",
        "required_before_paper_shadow": True,
        "side_effect_allowed": False,
    },
    "production_risk_review": {
        "priority": "BLOCKED",
        "current_status": "NOT_IN_SCOPE",
        "required_before_production": True,
        "side_effect_allowed": False,
    },
    "broker_action_contract": {
        "priority": "BLOCKED",
        "current_status": "NOT_IN_SCOPE",
        "required_before_broker_action": True,
        "side_effect_allowed": False,
    },
}

EXPLICIT_NON_GOALS = [
    "This task does not enable scheduler.",
    "This task does not create automated cadence.",
    "This task does not append events.",
    "This task does not bind outcomes.",
    "This task does not enable paper-shadow.",
    "This task does not enable production.",
    "This task does not call broker APIs.",
    "This task does not approve promotion.",
    (
        "This task does not close the gaps; it only turns gaps into a backlog "
        "and evidence matrix."
    ),
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "HARDENING_BACKLOG_IS_PLAN_ONLY",
    "SCHEDULER_KILL_SWITCH_EVIDENCE_MISSING",
    "SCHEDULER_IDEMPOTENCY_EVIDENCE_MISSING",
    "EVENT_APPEND_CONTRACT_MISSING",
    "OUTCOME_BINDING_CONTRACT_MISSING",
    "PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_MISSING",
    "PRODUCTION_AND_BROKER_ACTION_OUT_OF_SCOPE",
]

PRIORITY_RATIONALE = [
    (
        "P0 starts with scheduler kill-switch and disabled-by-default evidence "
        "because scheduler enablement cannot be discussed before fail-closed "
        "disablement evidence exists."
    ),
    (
        "P0 scheduler idempotency follows because repeated scheduler-like dry "
        "runs must be stable before any automated cadence discussion."
    ),
    (
        "P1 event append and outcome binding contracts remain plan-only until "
        "mutation guardrails and owner approval exist."
    ),
    (
        "P2 paper-shadow scope is later because no-broker and owner approval "
        "requirements still block paper-shadow readiness."
    ),
    (
        "Production and broker action remain blocked and out of scope until "
        "earlier observe-only evidence is complete."
    ),
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "HARDENING_BACKLOG_ONLY",
    "EVIDENCE_MATRIX_ONLY",
    "NO_AUTOMATED_CADENCE",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "KILL_SWITCH_EVIDENCE_REQUIRED",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "hardening_backlog_only": True,
    "evidence_matrix_only": True,
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
    "side_effect_allowed",
    "promotion_allowed_after_task",
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


class HighIntensitySchedulerHardeningBacklogError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_hardening_backlog(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    replay_validation_dir: Path = DEFAULT_REPLAY_VALIDATION_ROOT,
    audit_package_dir: Path = DEFAULT_AUDIT_PACKAGE_ROOT,
    owner_decision_dir: Path = DEFAULT_OWNER_DECISION_ROOT,
    gap_closure_dir: Path = DEFAULT_GAP_CLOSURE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerHardeningBacklogError(
            f"high-intensity scheduler hardening backlog only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_hardening_backlog_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
        gap_closure_dir=gap_closure_dir,
    )
    source_review = build_hardening_backlog_source_artifact_review(inputs=inputs)
    hardening_backlog = build_hardening_backlog(
        generated_at=generated_at,
        source_review=source_review,
    )
    evidence_matrix = build_evidence_matrix(
        generated_at=generated_at,
        hardening_backlog=hardening_backlog,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
        hardening_backlog=hardening_backlog,
        evidence_matrix=evidence_matrix,
    )
    package = build_hardening_backlog_package(
        generated_at=generated_at,
        source_review=source_review,
        hardening_backlog=hardening_backlog,
        evidence_matrix=evidence_matrix,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2356_scheduler_kill_switch_route(package=package)
    interpretation_boundary = build_hardening_backlog_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_hardening_backlog_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_hardening_backlog_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
        gap_closure_dir=gap_closure_dir,
        source_review=source_review,
        hardening_backlog=hardening_backlog,
        evidence_matrix=evidence_matrix,
        blocked_promotion_rationale=blocked_promotion_rationale,
        package=package,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "package": package,
            "source_review": source_review,
            "hardening_backlog": hardening_backlog,
            "evidence_matrix": evidence_matrix,
            "blocked_promotion_rationale": blocked_promotion_rationale,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_hardening_backlog_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        hardening_backlog=hardening_backlog,
        evidence_matrix=evidence_matrix,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_hardening_backlog_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    owner_decision_dir: Path,
    gap_closure_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_gap_closure_plan_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
            audit_package_dir=audit_package_dir,
            owner_decision_dir=owner_decision_dir,
        )
    except Exception as exc:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 upstream 2347/2348/2349/2350/2351/2352/2353 "
            f"validation failed: {exc}"
        ) from exc

    gap_closure_paths = {
        "summary": gap_closure_dir
        / "high_intensity_scheduler_gap_closure_plan_summary.json",
        "plan": gap_closure_dir
        / "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan.json",
        "source_review": gap_closure_dir
        / "high_intensity_scheduler_gap_closure_plan_source_artifact_review.json",
        "gap_closure_matrix": gap_closure_dir
        / "high_intensity_scheduler_gap_closure_matrix.json",
        "readiness_hardening_plan": gap_closure_dir
        / "high_intensity_scheduler_readiness_hardening_plan.json",
        "blocked_promotion_rationale": gap_closure_dir
        / "high_intensity_scheduler_gap_closure_blocked_promotion_rationale.json",
        "route": gap_closure_dir / "high_intensity_2355_hardening_backlog_route.json",
        "interpretation_boundary": gap_closure_dir
        / "high_intensity_scheduler_gap_closure_interpretation_boundary.json",
        "safety_boundary": gap_closure_dir
        / "high_intensity_scheduler_gap_closure_safety_boundary.json",
    }
    gap_closure_payloads = _load_required_payloads(
        gap_closure_paths,
        "TRADING-2354 scheduler gap closure plan",
    )
    for key, payload in gap_closure_payloads.items():
        label = f"TRADING-2354 gap closure {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_2354_source_contracts(gap_closure_payloads)
    _validate_cross_source_contracts(source_inputs, gap_closure_payloads)
    return {
        **source_inputs,
        "gap_closure_dir": str(gap_closure_dir),
        "gap_closure_paths": _string_paths(gap_closure_paths),
        "gap_closure": gap_closure_payloads,
    }


def build_hardening_backlog_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    gap = mapping(inputs["gap_closure"])
    gap_summary = mapping(gap["summary"])
    gap_plan = mapping(gap["plan"])
    gap_source_review = mapping(gap["source_review"])
    source_task_ids = list(gap_source_review.get("source_task_ids", []))
    source_task_ids.append(SOURCE_2354_TASK_REGISTER_ID)
    source_task_evidence = [
        dict(mapping(row))
        for row in gap_plan.get("source_task_evidence", [])
    ]
    evidence_tasks = {
        str(mapping(row).get("task"))
        for row in source_task_evidence
    }
    if "TRADING-2354" not in evidence_tasks:
        source_task_evidence.append(
            {
                "task": "TRADING-2354",
                "status": gap_summary.get("status"),
                "evidence": "gap closure and readiness hardening plan present",
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
            "gap_closure_artifacts_read": True,
            "gap_closure_artifact_count": len(mapping(inputs["gap_closure_paths"])),
            "gap_closure_status": gap_summary.get("status"),
            "gap_closure_plan_status": gap_plan.get("status"),
            "gap_closure_readiness": gap_summary.get("readiness"),
            "gap_closure_next_route": gap_summary.get("next_route"),
            "evidence_chain_complete": gap_summary.get("evidence_chain_complete"),
            "owner_decision": gap_summary.get("owner_decision"),
            "gap_closure_plan_ready": gap_summary.get("gap_closure_plan_ready"),
            "readiness_hardening_plan_ready": gap_summary.get(
                "readiness_hardening_plan_ready"
            ),
            "promotion_decision": gap_summary.get("promotion_decision"),
            "promotion_allowed": gap_summary.get("promotion_allowed"),
            "source_validate_data_executed": gap_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": gap_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": gap_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": gap_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_hardening_backlog(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.hardening_backlog.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "hardening_backlog_ready": True,
            "hardening_backlog": list(HARDENING_BACKLOG),
            "backlog_item_count": len(HARDENING_BACKLOG),
            "all_backlog_items_side_effect_free": True,
            "all_backlog_items_promotion_blocked": True,
            "recommended_execution_order": [
                item["task_route"] for item in HARDENING_BACKLOG
            ],
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2356_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_evidence_matrix(
    *,
    generated_at: datetime,
    hardening_backlog: Mapping[str, Any],
) -> dict[str, Any]:
    backlog_categories = [
        mapping(row).get("category")
        for row in hardening_backlog.get("hardening_backlog", [])
    ]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.evidence_matrix.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "source_tasks": list(SOURCE_TASKS),
            "evidence_matrix_ready": True,
            "evidence_matrix": dict(EVIDENCE_MATRIX),
            "evidence_item_count": len(EVIDENCE_MATRIX),
            "backlog_categories": backlog_categories,
            "all_matrix_items_side_effect_free": True,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2356_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_blocked_promotion_rationale(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    hardening_backlog: Mapping[str, Any],
    evidence_matrix: Mapping[str, Any],
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
            "backlog_item_count": hardening_backlog.get("backlog_item_count"),
            "evidence_item_count": evidence_matrix.get("evidence_item_count"),
            "reason_summary": (
                "Promotion remains blocked because TRADING-2355 only turns "
                "2354 gaps into a hardening backlog and evidence matrix; it "
                "does not execute scheduler enablement, event append, outcome "
                "binding, paper-shadow, production, or broker action."
            ),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2356_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_hardening_backlog_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    hardening_backlog: Mapping[str, Any],
    evidence_matrix: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    ready = (
        source_review.get("evidence_chain_complete") is True
        and source_review.get("owner_decision") == OWNER_DECISION
        and source_review.get("gap_closure_plan_ready") is True
        and source_review.get("readiness_hardening_plan_ready") is True
        and hardening_backlog.get("hardening_backlog_ready") is True
        and evidence_matrix.get("evidence_matrix_ready") is True
        and blocked_promotion_rationale.get("promotion_allowed") is False
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.package.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": STATUS if ready else "HARDENING_BACKLOG_BLOCKED",
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "source_task_evidence": source_review.get("source_task_evidence"),
            "evidence_chain_complete": source_review.get("evidence_chain_complete"),
            "owner_decision": source_review.get("owner_decision"),
            "gap_closure_plan_ready": source_review.get("gap_closure_plan_ready"),
            "readiness_hardening_plan_ready": source_review.get(
                "readiness_hardening_plan_ready"
            ),
            "hardening_backlog_ready": hardening_backlog.get(
                "hardening_backlog_ready"
            ),
            "evidence_matrix_ready": evidence_matrix.get("evidence_matrix_ready"),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": dict(GUARDRAIL_SUMMARY),
            "side_effect_summary": dict(SIDE_EFFECT_SUMMARY),
            "hardening_backlog": hardening_backlog.get("hardening_backlog"),
            "evidence_matrix": evidence_matrix.get("evidence_matrix"),
            "priority_rationale": list(PRIORITY_RATIONALE),
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": list(EXPLICIT_NON_GOALS),
            "recommended_execution_order": hardening_backlog.get(
                "recommended_execution_order"
            ),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2356_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2356_scheduler_kill_switch_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if package.get("status") != STATUS:
        blockers.append("HARDENING_BACKLOG_STATUS_NOT_READY")
    if package.get("hardening_backlog_ready") is not True:
        blockers.append("HARDENING_BACKLOG_NOT_READY")
    if package.get("evidence_matrix_ready") is not True:
        blockers.append("EVIDENCE_MATRIX_NOT_READY")
    if package.get("owner_decision") != OWNER_DECISION:
        blockers.append("OWNER_DECISION_NOT_KEEP_DISABLED")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_route = (
        "TRADING-2355_Observe_Only_Scheduler_Hardening_Backlog_Remediation"
        if blockers
        else NEXT_2356_ROUTE
    )
    rationale = (
        "hardening backlog can enter scheduler kill-switch and disabled enforcement evidence plan"
        if not blockers
        else "hardening backlog failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2356_route.v1",
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


def build_hardening_backlog_interpretation_boundary(
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
                "hardening backlog and evidence matrix only. It splits 2354 "
                "gaps into future plan/contract/replay/review tasks while "
                "keeping scheduler disabled and promotion blocked. It is not "
                "scheduler activation, automated cadence approval, event append, "
                "outcome binding, paper-shadow approval, production readiness, "
                "or broker execution."
            ),
            "not_scheduler_enablement": True,
            "not_gap_closure_execution": True,
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


def build_hardening_backlog_safety_boundary(
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


def build_hardening_backlog_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    owner_decision_dir: Path,
    gap_closure_dir: Path,
    source_review: Mapping[str, Any],
    hardening_backlog: Mapping[str, Any],
    evidence_matrix: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    package: Mapping[str, Any],
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
                "High-Intensity Risk-Cap Observe-Only Scheduler Hardening "
                "Backlog And Evidence Matrix"
            ),
            "status": package.get("status"),
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
            "gap_closure_dir": str(gap_closure_dir),
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": source_review.get("evidence_chain_complete"),
            "owner_decision": source_review.get("owner_decision"),
            "hardening_backlog_ready": hardening_backlog.get(
                "hardening_backlog_ready"
            ),
            "evidence_matrix_ready": evidence_matrix.get("evidence_matrix_ready"),
            "backlog_item_count": hardening_backlog.get("backlog_item_count"),
            "evidence_item_count": evidence_matrix.get("evidence_item_count"),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": package.get("guardrail_summary"),
            "side_effect_summary": package.get("side_effect_summary"),
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
                "aits validate-data not rerun because TRADING-2355 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, TRADING-2353 owner decision, and "
                "TRADING-2354 gap closure artifacts; it does not consume fresh "
                "market data, append events, bind outcomes, produce technical "
                "features, score, backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_hardening_backlog_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    hardening_backlog: Mapping[str, Any],
    evidence_matrix: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["hardening_backlog"], hardening_backlog)
    write_json(paths["evidence_matrix"], evidence_matrix)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["backlog_doc"], render_hardening_backlog_doc(package))
    write_markdown(paths["route_doc"], render_2356_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_hardening_backlog_doc(package: Mapping[str, Any]) -> str:
    source_rows = [
        "|Task|Status|Evidence|Promotion Result|",
        "|---|---|---|---|",
    ]
    for row in package.get("source_task_evidence", []):
        item = mapping(row)
        source_rows.append(
            "|"
            f"{item.get('task')}|"
            f"{item.get('status')}|"
            f"{item.get('evidence')}|"
            f"{item.get('promotion_result')}|"
        )
    backlog_rows = [
        (
            "|Task Route|Category|Priority|Required Evidence|Side Effect "
            "Allowed|Owner Approval Required|Promotion After Task|"
        ),
        "|---|---|---|---|---|---|---|",
    ]
    for row in package.get("hardening_backlog", []):
        item = mapping(row)
        required = ", ".join(str(x) for x in item.get("required_evidence", []))
        owner_required = item.get("owner_approval_required_for_promotion")
        backlog_rows.append(
            "|"
            f"`{item.get('task_route')}`|"
            f"`{item.get('category')}`|"
            f"`{item.get('priority')}`|"
            f"{required}|"
            f"`{item.get('side_effect_allowed')}`|"
            f"`{owner_required}`|"
            f"`{item.get('promotion_allowed_after_task')}`|"
        )
    evidence_rows = [
        "|Evidence Item|Priority|Current Status|Side Effect Allowed|",
        "|---|---|---|---|",
    ]
    for key, value in mapping(package.get("evidence_matrix", {})).items():
        item = mapping(value)
        evidence_rows.append(
            "|"
            f"`{key}`|"
            f"`{item.get('priority')}`|"
            f"`{item.get('current_status')}`|"
            f"`{item.get('side_effect_allowed')}`|"
        )
    guardrails = _markdown_table_from_mapping(package.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(package.get("side_effect_summary", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Hardening Backlog",
            "",
            "## Executive Summary",
            "",
            f"- task_id: `{package.get('task_id')}`",
            f"- task_register_id: `{package.get('task_register_id')}`",
            f"- status: `{package.get('status')}`",
            f"- source_tasks: `{package.get('source_tasks')}`",
            (
                "- evidence_chain_complete: "
                f"`{package.get('evidence_chain_complete')}`"
            ),
            f"- owner_decision: `{package.get('owner_decision')}`",
            (
                "- hardening_backlog_ready: "
                f"`{package.get('hardening_backlog_ready')}`"
            ),
            f"- evidence_matrix_ready: `{package.get('evidence_matrix_ready')}`",
            f"- promotion_decision: `{package.get('promotion_decision')}`",
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "## Source Evidence Chain",
            "",
            *source_rows,
            "",
            "## Owner Decision Recap",
            "",
            f"`{package.get('owner_decision')}`",
            "",
            "## Current Blocked Promotion Status",
            "",
            *[f"- `{reason}`" for reason in package.get("blocked_promotion_reasons", [])],
            "",
            "## Hardening Backlog Table",
            "",
            *backlog_rows,
            "",
            "## Evidence Matrix",
            "",
            *evidence_rows,
            "",
            "## Priority Rationale",
            "",
            *[f"- {item}" for item in package.get("priority_rationale", [])],
            "",
            "## Explicit Non-Goals",
            "",
            *[f"- {item}" for item in package.get("explicit_non_goals", [])],
            "",
            "## Recommended Execution Order",
            "",
            *[
                f"{idx}. `{route}`"
                for idx, route in enumerate(
                    package.get("recommended_execution_order", []),
                    start=1,
                )
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
            "## Next Route",
            "",
            f"`{package.get('next_route')}`",
        ]
    )


def render_2356_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2356 Scheduler Kill-Switch Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2356 route 只能进入 scheduler kill-switch and disabled-enforcement evidence plan。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是 event append、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_scheduler_hardening_backlog_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog.json",
        "source_review": output_dir
        / "high_intensity_scheduler_hardening_backlog_source_artifact_review.json",
        "hardening_backlog": output_dir
        / "high_intensity_scheduler_hardening_backlog_items.json",
        "evidence_matrix": output_dir
        / "high_intensity_scheduler_hardening_evidence_matrix.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_scheduler_hardening_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2356_scheduler_kill_switch_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_hardening_backlog_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_hardening_backlog_safety_boundary.json",
        "backlog_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog.md",
        "route_doc": docs_root / "high_intensity_2356_scheduler_kill_switch_route.md",
    }


def _validate_2354_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    plan = mapping(payloads["plan"])
    source_review = mapping(payloads["source_review"])
    matrix = mapping(payloads["gap_closure_matrix"])
    hardening_plan = mapping(payloads["readiness_hardening_plan"])
    rationale = mapping(payloads["blocked_promotion_rationale"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2354_STATUS:
        raise HighIntensitySchedulerHardeningBacklogError(
            f"TRADING-2355 requires 2354 status {EXPECTED_2354_STATUS}"
        )
    if plan.get("status") != EXPECTED_2354_STATUS:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 plan status"
        )
    if summary.get("readiness") != EXPECTED_2354_READINESS:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 readiness READY_FOR_2355_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2354_NEXT_ROUTE:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 summary route to hardening backlog"
        )
    if route.get("next_route") != EXPECTED_2354_NEXT_ROUTE:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 route to hardening backlog"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 source contract status PASS"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 evidence_chain_complete=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensitySchedulerHardeningBacklogError(
            f"TRADING-2355 requires owner_decision={OWNER_DECISION}"
        )
    if summary.get("gap_closure_plan_ready") is not True:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 gap_closure_plan_ready=true"
        )
    if hardening_plan.get("readiness_hardening_plan_ready") is not True:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 readiness_hardening_plan_ready=true"
        )
    if summary.get("readiness_hardening_plan_ready") is not True:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 summary readiness_hardening_plan_ready=true"
        )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 promotion_decision=BLOCKED"
        )
    if rationale.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 blocked rationale promotion_allowed=false"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 promotion_allowed=false"
        )
    if summary.get("source_tasks") != SOURCE_TASKS[:-1]:
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 source tasks to be 2347..2353"
        )
    gap_matrix = mapping(matrix.get("gap_closure_matrix"))
    for key in (
        "scheduler_enablement",
        "event_append",
        "outcome_binding",
        "paper_shadow",
        "production",
        "broker_action",
    ):
        item = mapping(gap_matrix.get(key))
        if item.get("allowed_in_this_task") is not False:
            raise HighIntensitySchedulerHardeningBacklogError(
                f"TRADING-2355 requires 2354 gap {key} to remain disallowed"
            )
    _validate_source_data_quality(summary, "TRADING-2354 summary")
    _validate_guardrail_summary(mapping(summary.get("guardrail_summary")))
    _validate_side_effect_summary(mapping(summary.get("side_effect_summary")))


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    gap_closure_payloads: Mapping[str, Any],
) -> None:
    owner_summary = mapping(mapping(source_inputs["owner_decision"])["summary"])
    gap_source_review = mapping(gap_closure_payloads["source_review"])
    if gap_source_review.get("owner_decision_status") != owner_summary.get("status"):
        raise HighIntensitySchedulerHardeningBacklogError(
            "TRADING-2355 requires 2354 source review owner decision status to match 2353"
        )


def _validate_guardrail_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in GUARDRAIL_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerHardeningBacklogError(
                f"TRADING-2355 requires 2354 guardrail {field}={expected}"
            )


def _validate_side_effect_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in SIDE_EFFECT_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerHardeningBacklogError(
                f"TRADING-2355 requires 2354 side effect {field}=false"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2355 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerHardeningBacklogError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerHardeningBacklogError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerHardeningBacklogError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerHardeningBacklogError(
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
        raise HighIntensitySchedulerHardeningBacklogError(
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
        raise HighIntensitySchedulerHardeningBacklogError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerHardeningBacklogError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerHardeningBacklogError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerHardeningBacklogError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerHardeningBacklogError(
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
