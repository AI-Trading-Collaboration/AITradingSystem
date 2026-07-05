from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_DOCS_ROOT,
    DEFAULT_GAP_CLOSURE_ROOT,
    DEFAULT_HARDENING_BACKLOG_ROOT,
    DEFAULT_IDEMPOTENCY_REPLAY_ROOT,
    DEFAULT_KILL_SWITCH_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_event_append_contract_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_EVENT_APPEND_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    GUARDRAIL_SUMMARY as EXPECTED_2358_GUARDRAIL_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    NEXT_2359_ROUTE as EXPECTED_2358_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    READINESS_STATUS as EXPECTED_2358_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    SIDE_EFFECT_SUMMARY as EXPECTED_2358_SIDE_EFFECT_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    SOURCE_TASKS as SOURCE_2358_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    STATUS as EXPECTED_2358_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    TASK_REGISTER_ID as SOURCE_2358_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2359"
TASK_REGISTER_ID = "TRADING-2359_OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_outcome_binding_contract_plan"

STATUS = "OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
READINESS_STATUS = "READY_FOR_2360_WITH_CAVEATS"
NEXT_2360_ROUTE = (
    "TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE

SOURCE_TASKS = [
    *SOURCE_2358_TASKS,
    "TRADING-2358",
]

GUARDRAIL_SUMMARY: dict[str, Any] = {
    "scheduler_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "event_append_enabled": False,
    "event_append_attempted": False,
    "historical_event_log_mutated": False,
    "outcome_binding_enabled": False,
    "outcome_binding_attempted": False,
    "outcome_store_mutated": False,
    "paper_shadow_enabled": False,
    "production_enabled": False,
    "broker_action_enabled": False,
}

SIDE_EFFECT_SUMMARY: dict[str, bool] = {
    "real_scheduler_created": False,
    "cron_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "manual_run_executed": False,
    "event_append_attempted": False,
    "historical_event_log_mutated": False,
    "outcome_binding_attempted": False,
    "outcome_store_mutated": False,
    "paper_shadow_attempted": False,
    "production_attempted": False,
    "broker_action_attempted": False,
}

OUTCOME_BINDING_SCHEMA_CONTRACT: dict[str, Any] = {
    "required": True,
    "binding_mode": "DISABLED_CONTRACT_ONLY",
    "required_fields": [
        "binding_id",
        "source_event_id",
        "source_task",
        "source_artifact",
        "outcome_type",
        "outcome_window",
        "as_of",
        "stable_semantic_hash",
        "promotion_decision",
        "event_append_enabled",
        "outcome_binding_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_action_enabled",
    ],
    "forbidden_without_owner_approval": [
        "outcome_store_write",
        "outcome_row_mutation",
        "binding_reference_write",
        "historical_event_log_backfill",
        "event_outcome_link_mutation",
    ],
}

BINDING_IDEMPOTENCY_CONTRACT: dict[str, Any] = {
    "required": True,
    "idempotency_key_fields": [
        "source_event_id",
        "source_task",
        "source_artifact",
        "outcome_type",
        "outcome_window",
        "stable_semantic_hash",
        "as_of",
    ],
    "must_reject_duplicate_binding_key": True,
    "must_not_bind_on_replay": True,
    "side_effect_allowed": False,
}

BINDING_REPLAY_CONTRACT: dict[str, Any] = {
    "required": True,
    "replay_count_minimum_future_validation": 3,
    "must_preserve_stable_semantic_hash": True,
    "must_not_append_event": True,
    "must_not_bind_outcome": True,
    "must_not_mutate_outcome_store": True,
    "must_not_enter_paper_shadow": True,
    "must_not_enter_production": True,
    "must_not_call_broker": True,
    "side_effect_allowed": False,
}

OUTCOME_STORE_MUTATION_GUARDRAIL: dict[str, Any] = {
    "outcome_binding_enabled": False,
    "outcome_store_mutation_enabled": False,
    "must_require_owner_approval": True,
    "must_require_manual_review": True,
    "must_require_dry_run_evidence": True,
    "must_require_binding_idempotency_evidence": True,
    "must_require_replay_no_side_effect_evidence": True,
    "must_block_outcome_store_mutation_in_this_task": True,
}

BINDING_ROLLBACK_PLAN: dict[str, Any] = {
    "required_before_any_future_binding": True,
    "rollback_strategy": "PLAN_ONLY_NO_MUTATION",
    "required_future_evidence": [
        "binding_preflight_snapshot",
        "binding_transaction_boundary",
        "binding_failure_recovery_plan",
        "duplicate_binding_rejection_test",
        "owner_approval_for_outcome_mutation",
    ],
    "side_effect_allowed": False,
}

OUTCOME_BINDING_CONTRACT: dict[str, Any] = {
    "outcome_binding_schema_required": True,
    "binding_idempotency_required": True,
    "binding_replay_required": True,
    "outcome_store_mutation_guardrail_required": True,
    "binding_rollback_plan_required": True,
    "owner_approval_required": True,
    "outcome_binding_allowed": False,
    "outcome_store_mutation_allowed": False,
    "paper_shadow_allowed": False,
    "side_effect_allowed": False,
}

REQUIRED_FUTURE_EVIDENCE = [
    "outcome_binding_schema_tests",
    "binding_idempotency_key_tests",
    "binding_replay_no_side_effect_tests",
    "outcome_store_mutation_owner_approval_tests",
    "binding_rollback_preflight_tests",
    "paper_shadow_scope_no_broker_guardrail_plan",
]

EXPLICIT_NON_GOALS = [
    "This task does not bind outcomes.",
    "This task does not mutate the outcome store.",
    "This task does not write outcome rows.",
    "This task does not append events.",
    "This task does not mutate the historical event log.",
    "This task does not enable scheduler.",
    "This task does not create automated cadence.",
    "This task does not execute manual run.",
    "This task does not enable paper-shadow.",
    "This task does not enable production.",
    "This task does not call broker APIs.",
    "This task only defines outcome binding contract requirements.",
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "OUTCOME_BINDING_CONTRACT_PLAN_IS_REQUIREMENT_ONLY",
    "OUTCOME_BINDING_IMPLEMENTATION_NOT_ALLOWED",
    "OUTCOME_STORE_MUTATION_NOT_ALLOWED",
    "PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_NOT_DEFINED",
    "OWNER_APPROVAL_NOT_GRANTED",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "EVENT_APPEND_DISABLED",
    "OUTCOME_BINDING_DISABLED",
    "OUTCOME_BINDING_CONTRACT_PLAN_ONLY",
    "NO_OUTCOME_STORE_MUTATION",
    "NO_HISTORICAL_EVENT_LOG_MUTATION",
    "NO_AUTOMATED_CADENCE",
    "NO_MANUAL_RUN_EXECUTION",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_REQUIRED_NEXT",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "outcome_binding_contract_plan_only": True,
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
    "real_replay_validation_executed": False,
    "event_append_enabled": False,
    "event_append_attempted": False,
    "event_append_executed": False,
    "historical_event_log_mutated": False,
    "event_log_mutated": False,
    "event_row_written": False,
    "event_row_mutated": False,
    "outcome_binding_enabled": False,
    "outcome_binding_attempted": False,
    "outcome_binding_executed": False,
    "outcome_store_mutated": False,
    "outcome_store_write": False,
    "outcome_row_written": False,
    "outcome_row_mutated": False,
    "outcome_reference_updated": False,
    "binding_reference_written": False,
    "event_outcome_link_mutated": False,
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
    "real_replay_validation_executed",
    "event_append_enabled",
    "event_append_attempted",
    "event_append_executed",
    "historical_event_log_mutated",
    "event_log_mutated",
    "event_row_written",
    "event_row_mutated",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "outcome_binding_executed",
    "outcome_store_mutated",
    "outcome_store_write",
    "outcome_row_written",
    "outcome_row_mutated",
    "outcome_reference_updated",
    "binding_reference_written",
    "event_outcome_link_mutated",
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
    "event_row_written",
    "event_row_mutated",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "outcome_binding_executed",
    "outcome_store_mutated",
    "outcome_store_write",
    "outcome_row_written",
    "outcome_row_mutated",
    "outcome_reference_updated",
    "binding_reference_written",
    "event_outcome_link_mutated",
    "fresh_market_data_read",
    "scheduled_tasks_config_modified",
    "broker_action_taken",
    "order_sent",
    "send_order",
    "manual_run_executed",
    "real_replay_validation_executed",
    "target_weight_generated",
    "rebalance_instruction_generated",
    "broker_order_generated",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
}


class HighIntensityOutcomeBindingContractPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_outcome_binding_contract_plan(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    replay_validation_dir: Path = DEFAULT_REPLAY_VALIDATION_ROOT,
    audit_package_dir: Path = DEFAULT_AUDIT_PACKAGE_ROOT,
    owner_decision_dir: Path = DEFAULT_OWNER_DECISION_ROOT,
    gap_closure_dir: Path = DEFAULT_GAP_CLOSURE_ROOT,
    hardening_backlog_dir: Path = DEFAULT_HARDENING_BACKLOG_ROOT,
    kill_switch_dir: Path = DEFAULT_KILL_SWITCH_ROOT,
    idempotency_replay_dir: Path = DEFAULT_IDEMPOTENCY_REPLAY_ROOT,
    event_append_dir: Path = DEFAULT_EVENT_APPEND_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityOutcomeBindingContractPlanError(
            f"high-intensity outcome binding contract plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_outcome_binding_contract_plan_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
        gap_closure_dir=gap_closure_dir,
        hardening_backlog_dir=hardening_backlog_dir,
        kill_switch_dir=kill_switch_dir,
        idempotency_replay_dir=idempotency_replay_dir,
        event_append_dir=event_append_dir,
    )
    source_review = build_outcome_binding_source_artifact_review(inputs=inputs)
    outcome_binding_contract = build_outcome_binding_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    schema_contract = build_outcome_binding_schema_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    idempotency_contract = build_binding_idempotency_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    replay_contract = build_binding_replay_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    mutation_guardrail = build_outcome_store_mutation_guardrail(
        generated_at=generated_at,
        source_review=source_review,
    )
    rollback_plan = build_binding_rollback_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_outcome_binding_contract_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        outcome_binding_contract=outcome_binding_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        replay_contract=replay_contract,
        mutation_guardrail=mutation_guardrail,
        rollback_plan=rollback_plan,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2360_paper_shadow_scope_route(package=package)
    interpretation_boundary = build_outcome_binding_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_outcome_binding_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_outcome_binding_contract_plan_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
        gap_closure_dir=gap_closure_dir,
        hardening_backlog_dir=hardening_backlog_dir,
        kill_switch_dir=kill_switch_dir,
        idempotency_replay_dir=idempotency_replay_dir,
        event_append_dir=event_append_dir,
        source_review=source_review,
        outcome_binding_contract=outcome_binding_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        replay_contract=replay_contract,
        mutation_guardrail=mutation_guardrail,
        rollback_plan=rollback_plan,
        blocked_promotion_rationale=blocked_promotion_rationale,
        package=package,
        route=route,
    )
    payloads = {
        "summary": summary,
        "package": package,
        "source_review": source_review,
        "outcome_binding_contract": outcome_binding_contract,
        "schema_contract": schema_contract,
        "idempotency_contract": idempotency_contract,
        "replay_contract": replay_contract,
        "mutation_guardrail": mutation_guardrail,
        "rollback_plan": rollback_plan,
        "blocked_promotion_rationale": blocked_promotion_rationale,
        "route": route,
        "interpretation_boundary": interpretation_boundary,
        "safety_boundary": safety_boundary,
    }
    _validate_generated_payloads(payloads)
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_outcome_binding_contract_plan_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        outcome_binding_contract=outcome_binding_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        replay_contract=replay_contract,
        mutation_guardrail=mutation_guardrail,
        rollback_plan=rollback_plan,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_outcome_binding_contract_plan_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    owner_decision_dir: Path,
    gap_closure_dir: Path,
    hardening_backlog_dir: Path,
    kill_switch_dir: Path,
    idempotency_replay_dir: Path,
    event_append_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_event_append_contract_plan_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
            audit_package_dir=audit_package_dir,
            owner_decision_dir=owner_decision_dir,
            gap_closure_dir=gap_closure_dir,
            hardening_backlog_dir=hardening_backlog_dir,
            kill_switch_dir=kill_switch_dir,
            idempotency_replay_dir=idempotency_replay_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensityOutcomeBindingContractPlanError(
            f"TRADING-2359 source chain invalid before 2358: {exc}"
        ) from exc

    paths = {
        "summary": event_append_dir
        / "high_intensity_event_append_contract_plan_summary.json",
        "package": event_append_dir
        / "high_intensity_risk_cap_observe_only_event_append_contract_plan.json",
        "source_review": event_append_dir
        / "high_intensity_event_append_contract_plan_source_artifact_review.json",
        "event_append_contract": event_append_dir
        / "high_intensity_event_append_contract.json",
        "schema_contract": event_append_dir
        / "high_intensity_event_schema_append_contract.json",
        "idempotency_contract": event_append_dir
        / "high_intensity_append_idempotency_contract.json",
        "duplicate_contract": event_append_dir
        / "high_intensity_event_duplicate_detection_contract.json",
        "rollback_plan": event_append_dir
        / "high_intensity_append_rollback_plan.json",
        "mutation_guardrail": event_append_dir
        / "high_intensity_event_mutation_guardrail.json",
        "blocked_promotion_rationale": event_append_dir
        / "high_intensity_event_append_blocked_promotion_rationale.json",
        "route": event_append_dir
        / "high_intensity_2359_outcome_binding_contract_route.json",
        "interpretation_boundary": event_append_dir
        / "high_intensity_event_append_interpretation_boundary.json",
        "safety_boundary": event_append_dir
        / "high_intensity_event_append_safety_boundary.json",
    }
    event_append_payloads = _load_required_payloads(paths, "TRADING-2359")
    _validate_2358_source_contracts(event_append_payloads)
    _validate_cross_source_contracts(source_inputs, event_append_payloads)
    return {
        **source_inputs,
        "event_append_plan": event_append_payloads,
        "event_append_plan_paths": _string_paths(paths),
    }


def build_outcome_binding_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    event_append = mapping(inputs["event_append_plan"])
    summary = mapping(event_append["summary"])
    source_review = mapping(event_append["source_review"])
    inherited_rows = list(source_review.get("source_task_evidence", []))
    source_task_evidence = [
        *inherited_rows,
        {
            "task": "TRADING-2358",
            "status": summary.get("status"),
            "evidence": "event append contract plan present",
            "evidence_present": True,
            "promotion_result": "blocked",
        },
    ]
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *summary.get("source_task_ids", []),
                SOURCE_2358_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_task_evidence,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "event_append_artifacts_read": True,
            "event_append_artifact_count": len(mapping(event_append)),
            "event_append_status": summary.get("status"),
            "event_append_readiness": summary.get("readiness"),
            "event_append_next_route": summary.get("next_route"),
            "event_append_contract_ready": summary.get(
                "event_append_contract_ready"
            ),
            "event_schema_append_contract_ready": summary.get(
                "event_schema_append_contract_ready"
            ),
            "append_idempotency_contract_ready": summary.get(
                "append_idempotency_contract_ready"
            ),
            "duplicate_detection_contract_ready": summary.get(
                "duplicate_detection_contract_ready"
            ),
            "append_rollback_plan_ready": summary.get(
                "append_rollback_plan_ready"
            ),
            "event_mutation_guardrail_ready": summary.get(
                "event_mutation_guardrail_ready"
            ),
            "idempotency_replay_status": source_review.get(
                "idempotency_replay_status"
            ),
            "idempotency_replay_readiness": source_review.get(
                "idempotency_replay_readiness"
            ),
            "kill_switch_status": source_review.get("kill_switch_status"),
            "owner_decision": OWNER_DECISION,
            "evidence_chain_complete": True,
            "source_validate_data_executed": summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": summary.get("source_validate_data_as_of"),
            "source_validate_data_status": summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": summary.get(
                "source_validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_outcome_binding_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="outcome_binding_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="outcome_binding_contract_ready",
        payload_key="outcome_binding_contract",
        payload=OUTCOME_BINDING_CONTRACT,
    )


def build_outcome_binding_schema_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="outcome_binding_schema_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="outcome_binding_schema_contract_ready",
        payload_key="outcome_binding_schema_contract",
        payload=OUTCOME_BINDING_SCHEMA_CONTRACT,
    )


def build_binding_idempotency_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="binding_idempotency_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="binding_idempotency_contract_ready",
        payload_key="binding_idempotency_contract",
        payload=BINDING_IDEMPOTENCY_CONTRACT,
    )


def build_binding_replay_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="binding_replay_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="binding_replay_contract_ready",
        payload_key="binding_replay_contract",
        payload=BINDING_REPLAY_CONTRACT,
    )


def build_outcome_store_mutation_guardrail(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="outcome_store_mutation_guardrail",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="outcome_store_mutation_guardrail_ready",
        payload_key="outcome_store_mutation_guardrail",
        payload=OUTCOME_STORE_MUTATION_GUARDRAIL,
    )


def build_binding_rollback_plan(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="binding_rollback_plan",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="binding_rollback_plan_ready",
        payload_key="binding_rollback_plan",
        payload=BINDING_ROLLBACK_PLAN,
    )


def _contract_payload(
    *,
    schema_name: str,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    ready_key: str,
    payload_key: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                f"plan.{schema_name}.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            ready_key: True,
            payload_key: payload,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_blocked_promotion_rationale(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.blocked_promotion_rationale.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocked_promotion_reasons": BLOCKED_PROMOTION_REASONS,
            "required_before_any_outcome_binding": REQUIRED_FUTURE_EVIDENCE,
            **SAFETY_FIELDS,
        }
    )


def build_outcome_binding_contract_plan_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    outcome_binding_contract: Mapping[str, Any],
    schema_contract: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    replay_contract: Mapping[str, Any],
    mutation_guardrail: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Outcome Binding "
                "Contract Plan"
            ),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_evidence": source_review.get("source_task_evidence"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "outcome_binding_contract_ready": outcome_binding_contract.get(
                "outcome_binding_contract_ready"
            ),
            "outcome_binding_schema_contract_ready": schema_contract.get(
                "outcome_binding_schema_contract_ready"
            ),
            "binding_idempotency_contract_ready": idempotency_contract.get(
                "binding_idempotency_contract_ready"
            ),
            "binding_replay_contract_ready": replay_contract.get(
                "binding_replay_contract_ready"
            ),
            "outcome_store_mutation_guardrail_ready": mutation_guardrail.get(
                "outcome_store_mutation_guardrail_ready"
            ),
            "binding_rollback_plan_ready": rollback_plan.get(
                "binding_rollback_plan_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "outcome_binding_contract": outcome_binding_contract.get(
                "outcome_binding_contract"
            ),
            "outcome_binding_schema_contract": schema_contract.get(
                "outcome_binding_schema_contract"
            ),
            "binding_idempotency_contract": idempotency_contract.get(
                "binding_idempotency_contract"
            ),
            "binding_replay_contract": replay_contract.get(
                "binding_replay_contract"
            ),
            "outcome_store_mutation_guardrail": mutation_guardrail.get(
                "outcome_store_mutation_guardrail"
            ),
            "binding_rollback_plan": rollback_plan.get("binding_rollback_plan"),
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2360_ROUTE,
            "next_task": NEXT_2360_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2360_paper_shadow_scope_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.2360_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2360_ROUTE,
            "next_task": NEXT_2360_ROUTE,
            "route_caveats": ROUTE_CAVEATS,
            "route_blockers": [],
            "route_rationale": (
                "outcome binding contract plan can enter paper-shadow scope "
                "and no-broker guardrail planning while outcome binding remains disabled"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_outcome_binding_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2359 defines outcome binding contract requirements only."
            ),
            "not_outcome_binding_implementation": True,
            "not_outcome_store_mutation_approval": True,
            "not_event_append_implementation": True,
            "not_scheduler_enablement": True,
            "not_manual_run_execution": True,
            "not_paper_shadow_readiness": True,
            "not_production_readiness": True,
            "not_broker_readiness": True,
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_outcome_binding_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.safety_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "must_not": [
                "enable_scheduler",
                "create_cron_job",
                "create_windows_task",
                "create_github_actions_schedule",
                "execute_real_manual_run",
                "append_historical_event_log",
                "mutate_event_log",
                "write_event_row",
                "bind_outcome",
                "mutate_outcome_store",
                "write_outcome_row",
                "update_outcome_reference",
                "enable_paper_shadow",
                "enable_production",
                "call_broker_api",
                "send_order",
                "read_fresh_market_data",
                "generate_new_signal",
                "run_backtest",
                "generate_daily_report",
            ],
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_outcome_binding_contract_plan_summary(
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
    hardening_backlog_dir: Path,
    kill_switch_dir: Path,
    idempotency_replay_dir: Path,
    event_append_dir: Path,
    source_review: Mapping[str, Any],
    outcome_binding_contract: Mapping[str, Any],
    schema_contract: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    replay_contract: Mapping[str, Any],
    mutation_guardrail: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_outcome_binding_contract_"
                "plan.summary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": package.get("title"),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "disabled_wiring_dir": str(disabled_wiring_dir),
            "smoke_dry_run_dir": str(smoke_dry_run_dir),
            "manual_review_gate_dir": str(manual_review_gate_dir),
            "manual_run_dry_run_dir": str(manual_run_dry_run_dir),
            "replay_validation_dir": str(replay_validation_dir),
            "audit_package_dir": str(audit_package_dir),
            "owner_decision_dir": str(owner_decision_dir),
            "gap_closure_dir": str(gap_closure_dir),
            "hardening_backlog_dir": str(hardening_backlog_dir),
            "kill_switch_dir": str(kill_switch_dir),
            "idempotency_replay_dir": str(idempotency_replay_dir),
            "event_append_dir": str(event_append_dir),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "outcome_binding_contract_ready": outcome_binding_contract.get(
                "outcome_binding_contract_ready"
            ),
            "outcome_binding_schema_contract_ready": schema_contract.get(
                "outcome_binding_schema_contract_ready"
            ),
            "binding_idempotency_contract_ready": idempotency_contract.get(
                "binding_idempotency_contract_ready"
            ),
            "binding_replay_contract_ready": replay_contract.get(
                "binding_replay_contract_ready"
            ),
            "outcome_store_mutation_guardrail_ready": mutation_guardrail.get(
                "outcome_store_mutation_guardrail_ready"
            ),
            "binding_rollback_plan_ready": rollback_plan.get(
                "binding_rollback_plan_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
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
                "aits validate-data not rerun because TRADING-2359 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, TRADING-2353 owner decision, "
                "TRADING-2354 gap closure, TRADING-2355 hardening backlog, "
                "TRADING-2356 kill-switch plan, TRADING-2357 idempotency "
                "replay contract, and TRADING-2358 event append contract "
                "artifacts; it does not consume fresh market data, append "
                "events, bind outcomes, mutate outcome store, produce "
                "technical features, score, backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_outcome_binding_contract_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    outcome_binding_contract: Mapping[str, Any],
    schema_contract: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    replay_contract: Mapping[str, Any],
    mutation_guardrail: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["outcome_binding_contract"], outcome_binding_contract)
    write_json(paths["schema_contract"], schema_contract)
    write_json(paths["idempotency_contract"], idempotency_contract)
    write_json(paths["replay_contract"], replay_contract)
    write_json(paths["mutation_guardrail"], mutation_guardrail)
    write_json(paths["rollback_plan"], rollback_plan)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_outcome_binding_contract_plan_doc(package))
    write_markdown(paths["route_doc"], render_2360_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_outcome_binding_contract_plan_doc(package: Mapping[str, Any]) -> str:
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
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Outcome Binding Contract Plan",
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
                "- outcome_binding_contract_ready: "
                f"`{package.get('outcome_binding_contract_ready')}`"
            ),
            (
                "- outcome_binding_schema_contract_ready: "
                f"`{package.get('outcome_binding_schema_contract_ready')}`"
            ),
            (
                "- binding_idempotency_contract_ready: "
                f"`{package.get('binding_idempotency_contract_ready')}`"
            ),
            (
                "- binding_replay_contract_ready: "
                f"`{package.get('binding_replay_contract_ready')}`"
            ),
            (
                "- outcome_store_mutation_guardrail_ready: "
                f"`{package.get('outcome_store_mutation_guardrail_ready')}`"
            ),
            (
                "- binding_rollback_plan_ready: "
                f"`{package.get('binding_rollback_plan_ready')}`"
            ),
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
            "## Outcome Binding Schema Contract",
            "",
            *_markdown_table_from_mapping(
                package.get("outcome_binding_schema_contract", {})
            ),
            "",
            "## Binding Idempotency Contract",
            "",
            *_markdown_table_from_mapping(
                package.get("binding_idempotency_contract", {})
            ),
            "",
            "## Binding Replay Contract",
            "",
            *_markdown_table_from_mapping(package.get("binding_replay_contract", {})),
            "",
            "## Outcome Store Mutation Guardrail",
            "",
            *_markdown_table_from_mapping(
                package.get("outcome_store_mutation_guardrail", {})
            ),
            "",
            "## Binding Rollback Plan",
            "",
            *_markdown_table_from_mapping(package.get("binding_rollback_plan", {})),
            "",
            "## Side-Effect Assertion Summary",
            "",
            *_markdown_table_from_mapping(package.get("side_effect_summary", {})),
            "",
            "## Guardrail Status Table",
            "",
            *_markdown_table_from_mapping(package.get("guardrail_summary", {})),
            "",
            "## Explicit Non-Goals",
            "",
            *[f"- {item}" for item in package.get("explicit_non_goals", [])],
            "",
            "## Next Route",
            "",
            f"`{package.get('next_route')}`",
        ]
    )


def render_2360_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2360 Paper-Shadow Scope Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2360 route 只能进入 paper-shadow scope and no-broker guardrail plan。",
            "它不是 outcome binding implementation、不是 outcome store mutation approval、",
            "不是 paper-shadow activation、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_outcome_binding_contract_plan_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.json",
        "source_review": output_dir
        / "high_intensity_outcome_binding_contract_plan_source_artifact_review.json",
        "outcome_binding_contract": output_dir
        / "high_intensity_outcome_binding_contract.json",
        "schema_contract": output_dir
        / "high_intensity_outcome_binding_schema_contract.json",
        "idempotency_contract": output_dir
        / "high_intensity_binding_idempotency_contract.json",
        "replay_contract": output_dir / "high_intensity_binding_replay_contract.json",
        "mutation_guardrail": output_dir
        / "high_intensity_outcome_store_mutation_guardrail.json",
        "rollback_plan": output_dir / "high_intensity_binding_rollback_plan.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_outcome_binding_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2360_paper_shadow_scope_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_outcome_binding_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_outcome_binding_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.md",
        "route_doc": docs_root / "high_intensity_2360_paper_shadow_scope_route.md",
    }


def _validate_2358_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    event_append_contract = mapping(payloads["event_append_contract"])
    schema_contract = mapping(payloads["schema_contract"])
    idempotency_contract = mapping(payloads["idempotency_contract"])
    duplicate_contract = mapping(payloads["duplicate_contract"])
    rollback_plan = mapping(payloads["rollback_plan"])
    mutation_guardrail = mapping(payloads["mutation_guardrail"])
    rationale = mapping(payloads["blocked_promotion_rationale"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2358_STATUS:
        raise HighIntensityOutcomeBindingContractPlanError(
            f"TRADING-2359 requires 2358 status {EXPECTED_2358_STATUS}"
        )
    if package.get("status") != EXPECTED_2358_STATUS:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 package status"
        )
    if summary.get("readiness") != EXPECTED_2358_READINESS:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 readiness READY_FOR_2359_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2358_NEXT_ROUTE:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 summary route to outcome binding plan"
        )
    if route.get("next_route") != EXPECTED_2358_NEXT_ROUTE:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 route to outcome binding plan"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 source contract status PASS"
        )
    if summary.get("source_tasks") != SOURCE_2358_TASKS:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 source tasks to be 2347..2357"
        )
    if package.get("source_tasks") != SOURCE_2358_TASKS:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 package source tasks to be 2347..2357"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 evidence_chain_complete=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensityOutcomeBindingContractPlanError(
            f"TRADING-2359 requires owner_decision={OWNER_DECISION}"
        )
    ready_fields = {
        "event_append_contract_ready": summary.get("event_append_contract_ready"),
        "event_schema_append_contract_ready": summary.get(
            "event_schema_append_contract_ready"
        ),
        "append_idempotency_contract_ready": summary.get(
            "append_idempotency_contract_ready"
        ),
        "duplicate_detection_contract_ready": summary.get(
            "duplicate_detection_contract_ready"
        ),
        "append_rollback_plan_ready": summary.get("append_rollback_plan_ready"),
        "event_mutation_guardrail_ready": summary.get(
            "event_mutation_guardrail_ready"
        ),
        "event_append_artifact_ready": event_append_contract.get(
            "event_append_contract_ready"
        ),
        "schema_artifact_ready": schema_contract.get(
            "event_schema_append_contract_ready"
        ),
        "idempotency_artifact_ready": idempotency_contract.get(
            "append_idempotency_contract_ready"
        ),
        "duplicate_artifact_ready": duplicate_contract.get(
            "duplicate_detection_contract_ready"
        ),
        "rollback_artifact_ready": rollback_plan.get("append_rollback_plan_ready"),
        "mutation_artifact_ready": mutation_guardrail.get(
            "event_mutation_guardrail_ready"
        ),
    }
    for field, value in ready_fields.items():
        if value is not True:
            raise HighIntensityOutcomeBindingContractPlanError(
                f"TRADING-2359 requires 2358 {field}=true"
            )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 promotion_decision=BLOCKED"
        )
    if rationale.get("promotion_allowed") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 blocked rationale promotion_allowed=false"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 promotion_allowed=false"
        )
    if mapping(package.get("event_append_contract")).get("side_effect_allowed") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 event append side_effect_allowed=false"
        )
    append_idempotency = mapping(package.get("append_idempotency_contract"))
    if "stable_semantic_hash" not in append_idempotency.get(
        "idempotency_key_fields",
        [],
    ):
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 append idempotency stable_semantic_hash"
        )
    duplicate = mapping(package.get("duplicate_detection_contract"))
    if duplicate.get("duplicate_resolution") != "BLOCK_APPEND_AND_REPORT_DUPLICATE":
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 duplicate detection to block append"
        )
    if duplicate.get("side_effect_allowed") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 duplicate side_effect_allowed=false"
        )
    if mapping(package.get("append_rollback_plan")).get("rollback_strategy") != (
        "PLAN_ONLY_NO_MUTATION"
    ):
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 rollback plan-only/no-mutation"
        )
    if mapping(package.get("event_mutation_guardrail")).get(
        "event_append_enabled"
    ) is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 event_append_enabled=false"
        )
    _validate_source_data_quality(summary, "TRADING-2358 summary")
    _validate_expected_summary(
        mapping(summary.get("guardrail_summary")),
        EXPECTED_2358_GUARDRAIL_SUMMARY,
        "TRADING-2358 guardrail",
    )
    _validate_expected_summary(
        mapping(summary.get("side_effect_summary")),
        EXPECTED_2358_SIDE_EFFECT_SUMMARY,
        "TRADING-2358 side effect",
    )


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    event_append_payloads: Mapping[str, Any],
) -> None:
    idempotency_summary = mapping(
        mapping(source_inputs["idempotency_replay_plan"])["summary"]
    )
    event_append_source_review = mapping(event_append_payloads["source_review"])
    if event_append_source_review.get("idempotency_replay_status") != (
        idempotency_summary.get("status")
    ):
        raise HighIntensityOutcomeBindingContractPlanError(
            "TRADING-2359 requires 2358 source review idempotency status to match 2357"
        )


def _validate_expected_summary(
    payload: Mapping[str, Any],
    expected: Mapping[str, Any],
    label: str,
) -> None:
    for field, expected_value in expected.items():
        if payload.get(field) is not expected_value:
            raise HighIntensityOutcomeBindingContractPlanError(
                f"TRADING-2359 requires {label} {field}={expected_value}"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2359 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_outcome_binding_contracts(mapping(payloads["package"]))


def _validate_outcome_binding_contracts(package: Mapping[str, Any]) -> None:
    idempotency = mapping(package.get("binding_idempotency_contract"))
    replay = mapping(package.get("binding_replay_contract"))
    mutation = mapping(package.get("outcome_store_mutation_guardrail"))
    rollback = mapping(package.get("binding_rollback_plan"))
    if "stable_semantic_hash" not in idempotency.get("idempotency_key_fields", []):
        raise HighIntensityOutcomeBindingContractPlanError(
            "binding idempotency contract must include stable_semantic_hash"
        )
    if replay.get("side_effect_allowed") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "binding replay contract must not allow side effects"
        )
    if replay.get("must_not_bind_outcome") is not True:
        raise HighIntensityOutcomeBindingContractPlanError(
            "binding replay contract must block outcome binding"
        )
    if replay.get("must_not_mutate_outcome_store") is not True:
        raise HighIntensityOutcomeBindingContractPlanError(
            "binding replay contract must block outcome store mutation"
        )
    if mutation.get("outcome_binding_enabled") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "outcome store guardrail requires outcome_binding_enabled=false"
        )
    if mutation.get("outcome_store_mutation_enabled") is not False:
        raise HighIntensityOutcomeBindingContractPlanError(
            "outcome store guardrail requires mutation disabled"
        )
    if rollback.get("rollback_strategy") != "PLAN_ONLY_NO_MUTATION":
        raise HighIntensityOutcomeBindingContractPlanError(
            "binding rollback plan must be plan-only/no-mutation"
        )


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensityOutcomeBindingContractPlanError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensityOutcomeBindingContractPlanError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityOutcomeBindingContractPlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityOutcomeBindingContractPlanError(
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
        raise HighIntensityOutcomeBindingContractPlanError(
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
        raise HighIntensityOutcomeBindingContractPlanError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensityOutcomeBindingContractPlanError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensityOutcomeBindingContractPlanError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensityOutcomeBindingContractPlanError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensityOutcomeBindingContractPlanError(
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
