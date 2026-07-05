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
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_DOCS_ROOT,
    DEFAULT_GAP_CLOSURE_ROOT,
    DEFAULT_HARDENING_BACKLOG_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_scheduler_kill_switch_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_KILL_SWITCH_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    GUARDRAIL_SUMMARY as EXPECTED_2356_GUARDRAIL_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    NEXT_2357_ROUTE as EXPECTED_2356_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    READINESS_STATUS as EXPECTED_2356_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    SIDE_EFFECT_SUMMARY as EXPECTED_2356_SIDE_EFFECT_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    SOURCE_TASKS as SOURCE_2356_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    STATUS as EXPECTED_2356_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    TASK_REGISTER_ID as SOURCE_2356_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2357"
TASK_REGISTER_ID = (
    "TRADING-2357_OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_"
    "CONTRACT_PLAN"
)
REPORT_TYPE = (
    "high_intensity_risk_cap_observe_only_scheduler_"
    "idempotency_replay_contract_plan"
)
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_idempotency_replay_contract_plan"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_CONTRACT_PLAN_READY_"
    "WITH_CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2358_WITH_CAVEATS"
NEXT_2358_ROUTE = "TRADING-2358_Observe_Only_Event_Append_Contract_Plan"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE

SOURCE_TASKS = [
    *SOURCE_2356_TASKS,
    "TRADING-2356",
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
    "manual_run_executed": False,
    "event_append_attempted": False,
    "outcome_binding_attempted": False,
    "paper_shadow_attempted": False,
    "production_attempted": False,
    "broker_action_attempted": False,
}

STABLE_SEMANTIC_HASH_CONTRACT: dict[str, Any] = {
    "hash_algorithm": "sha256",
    "canonicalization": "json_sorted_keys_without_volatile_fields",
    "stable_semantic_fields": [
        "task_id",
        "status",
        "source_tasks",
        "evidence_chain_complete",
        "owner_decision",
        "promotion_decision",
        "promotion_allowed",
        "scheduler_enabled",
        "manual_run_only",
        "dry_run_only",
        "manual_run_executed",
        "guardrail_summary",
        "side_effect_summary",
        "next_route",
    ],
    "must_not_hash_full_json_byte_for_byte": True,
}

VOLATILE_FIELD_EXCLUSION_RULE: dict[str, Any] = {
    "excluded_fields": [
        "generated_at",
        "created_at",
        "updated_at",
        "runtime_id",
        "runtime_artifact",
        "runtime_artifact_path",
        "duration_ms",
        "elapsed_seconds",
        "local_path",
        "absolute_path",
        "host",
        "machine",
        "process_id",
        "git_dirty_state_when_generated",
    ],
    "reason": (
        "These fields can vary between runs without changing scheduler "
        "safety semantics."
    ),
}

DUPLICATE_DETECTION_PLAN: dict[str, Any] = {
    "required": True,
    "duplicate_key_candidates": [
        "task_id",
        "source_tasks",
        "stable_semantic_hash",
        "next_route",
    ],
    "must_not_append_duplicate_event": True,
    "must_not_bind_duplicate_outcome": True,
    "side_effect_allowed": False,
}

REPLAY_NO_SIDE_EFFECT_CONTRACT: dict[str, Any] = {
    "replay_count_minimum_future_validation": 3,
    "must_preserve_stable_semantic_hash": True,
    "must_not_create_real_scheduler": True,
    "must_not_create_cron": True,
    "must_not_create_windows_task": True,
    "must_not_create_github_actions_schedule": True,
    "must_not_execute_manual_run": True,
    "must_not_append_event": True,
    "must_not_bind_outcome": True,
    "must_not_enter_paper_shadow": True,
    "must_not_enter_production": True,
    "must_not_call_broker": True,
}

IDEMPOTENCY_CONTRACT: dict[str, Any] = {
    "semantic_hash_required": True,
    "stable_semantic_fields_required": True,
    "volatile_fields_must_be_excluded": True,
    "duplicate_detection_required": True,
    "replay_no_side_effect_required": True,
    "event_append_allowed": False,
    "outcome_binding_allowed": False,
    "side_effect_allowed": False,
}

REQUIRED_FUTURE_EVIDENCE = [
    "idempotency_contract_tests",
    "stable_semantic_hash_tests",
    "volatile_field_exclusion_tests",
    "duplicate_detection_tests",
    "replay_no_side_effect_tests",
    "owner_approval_required_assertions",
    "event_append_contract_plan",
]

EXPLICIT_NON_GOALS = [
    "This task does not enable scheduler.",
    "This task does not create automated cadence.",
    "This task does not execute manual run.",
    "This task does not append events.",
    "This task does not bind outcomes.",
    "This task does not enable paper-shadow.",
    "This task does not enable production.",
    "This task does not call broker APIs.",
    "This task does not run real replay validation.",
    "This task only defines idempotency and replay contract requirements.",
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "IDEMPOTENCY_AND_REPLAY_CONTRACT_PLAN_IS_REQUIREMENT_ONLY",
    "REAL_REPLAY_VALIDATION_NOT_EXECUTED",
    "EVENT_APPEND_CONTRACT_NOT_DEFINED",
    "OUTCOME_BINDING_CONTRACT_NOT_DEFINED",
    "OWNER_APPROVAL_NOT_GRANTED",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "IDEMPOTENCY_CONTRACT_PLAN_ONLY",
    "REPLAY_CONTRACT_PLAN_ONLY",
    "NO_REAL_REPLAY_VALIDATION",
    "NO_AUTOMATED_CADENCE",
    "NO_MANUAL_RUN_EXECUTION",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "EVENT_APPEND_CONTRACT_REQUIRED_NEXT",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "idempotency_contract_plan_only": True,
    "replay_contract_plan_only": True,
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
    "real_replay_validation_executed",
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
    "real_replay_validation_executed",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
}


class HighIntensitySchedulerIdempotencyReplayContractPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan(
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
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "high-intensity scheduler idempotency replay contract plan only "
            f"supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs(
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
    )
    source_review = build_idempotency_replay_source_artifact_review(inputs=inputs)
    idempotency_contract = build_idempotency_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    stable_semantic_hash_contract = build_stable_semantic_hash_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    volatile_field_exclusion_rule = build_volatile_field_exclusion_rule(
        generated_at=generated_at,
        source_review=source_review,
    )
    duplicate_detection_plan = build_duplicate_detection_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    replay_no_side_effect_contract = build_replay_no_side_effect_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_idempotency_replay_contract_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        idempotency_contract=idempotency_contract,
        stable_semantic_hash_contract=stable_semantic_hash_contract,
        volatile_field_exclusion_rule=volatile_field_exclusion_rule,
        duplicate_detection_plan=duplicate_detection_plan,
        replay_no_side_effect_contract=replay_no_side_effect_contract,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2358_event_append_contract_route(package=package)
    interpretation_boundary = build_idempotency_replay_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_idempotency_replay_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_idempotency_replay_contract_plan_summary(
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
        source_review=source_review,
        idempotency_contract=idempotency_contract,
        stable_semantic_hash_contract=stable_semantic_hash_contract,
        volatile_field_exclusion_rule=volatile_field_exclusion_rule,
        duplicate_detection_plan=duplicate_detection_plan,
        replay_no_side_effect_contract=replay_no_side_effect_contract,
        blocked_promotion_rationale=blocked_promotion_rationale,
        package=package,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "package": package,
            "source_review": source_review,
            "idempotency_contract": idempotency_contract,
            "stable_semantic_hash_contract": stable_semantic_hash_contract,
            "volatile_field_exclusion_rule": volatile_field_exclusion_rule,
            "duplicate_detection_plan": duplicate_detection_plan,
            "replay_no_side_effect_contract": replay_no_side_effect_contract,
            "blocked_promotion_rationale": blocked_promotion_rationale,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_idempotency_replay_contract_plan_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        idempotency_contract=idempotency_contract,
        stable_semantic_hash_contract=stable_semantic_hash_contract,
        volatile_field_exclusion_rule=volatile_field_exclusion_rule,
        duplicate_detection_plan=duplicate_detection_plan,
        replay_no_side_effect_contract=replay_no_side_effect_contract,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs(
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
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_kill_switch_plan_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
            audit_package_dir=audit_package_dir,
            owner_decision_dir=owner_decision_dir,
            gap_closure_dir=gap_closure_dir,
            hardening_backlog_dir=hardening_backlog_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"TRADING-2357 source chain invalid before 2356: {exc}"
        ) from exc

    paths = {
        "summary": kill_switch_dir
        / "high_intensity_scheduler_kill_switch_plan_summary.json",
        "package": kill_switch_dir
        / "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.json",
        "source_review": kill_switch_dir
        / "high_intensity_scheduler_kill_switch_plan_source_artifact_review.json",
        "kill_switch_contract": kill_switch_dir
        / "high_intensity_scheduler_kill_switch_contract.json",
        "disabled_enforcement_matrix": kill_switch_dir
        / "high_intensity_scheduler_disabled_enforcement_matrix.json",
        "no_real_scheduler_assertions": kill_switch_dir
        / "high_intensity_scheduler_no_real_scheduler_creation_assertions.json",
        "manual_review_assertions": kill_switch_dir
        / "high_intensity_scheduler_manual_review_required_assertions.json",
        "blocked_promotion_rationale": kill_switch_dir
        / "high_intensity_scheduler_kill_switch_blocked_promotion_rationale.json",
        "route": kill_switch_dir / "high_intensity_2357_scheduler_idempotency_route.json",
        "interpretation_boundary": kill_switch_dir
        / "high_intensity_scheduler_kill_switch_plan_interpretation_boundary.json",
        "safety_boundary": kill_switch_dir
        / "high_intensity_scheduler_kill_switch_plan_safety_boundary.json",
    }
    kill_switch_payloads = _load_required_payloads(paths, "TRADING-2357")
    _validate_2356_source_contracts(kill_switch_payloads)
    _validate_cross_source_contracts(source_inputs, kill_switch_payloads)
    return {
        **source_inputs,
        "kill_switch_plan": kill_switch_payloads,
        "kill_switch_plan_paths": _string_paths(paths),
    }


def build_idempotency_replay_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    kill_switch = mapping(inputs["kill_switch_plan"])
    kill_summary = mapping(kill_switch["summary"])
    kill_source_review = mapping(kill_switch["source_review"])
    inherited_rows = list(kill_source_review.get("source_task_evidence", []))
    source_task_evidence = [
        *inherited_rows,
        {
            "task": "TRADING-2356",
            "status": kill_summary.get("status"),
            "evidence": "kill-switch and disabled-enforcement plan present",
            "evidence_present": True,
            "promotion_result": "blocked",
        },
    ]
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *kill_summary.get("source_task_ids", []),
                SOURCE_2356_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_task_evidence,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "kill_switch_artifacts_read": True,
            "kill_switch_artifact_count": len(mapping(kill_switch)),
            "kill_switch_status": kill_summary.get("status"),
            "kill_switch_readiness": kill_summary.get("readiness"),
            "kill_switch_next_route": kill_summary.get("next_route"),
            "kill_switch_contract_ready": kill_summary.get(
                "kill_switch_contract_ready"
            ),
            "disabled_enforcement_evidence_plan_ready": kill_summary.get(
                "disabled_enforcement_evidence_plan_ready"
            ),
            "no_real_scheduler_creation_assertions_ready": kill_summary.get(
                "no_real_scheduler_creation_assertions_ready"
            ),
            "manual_review_required_assertions_ready": kill_summary.get(
                "manual_review_required_assertions_ready"
            ),
            "hardening_backlog_status": kill_source_review.get(
                "hardening_backlog_status"
            ),
            "hardening_backlog_readiness": kill_source_review.get(
                "hardening_backlog_readiness"
            ),
            "owner_decision": OWNER_DECISION,
            "evidence_chain_complete": True,
            "source_validate_data_executed": kill_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": kill_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": kill_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": kill_summary.get(
                "source_validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_idempotency_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.idempotency_contract.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "idempotency_contract_ready": True,
            "replay_contract_ready": True,
            "idempotency_contract": IDEMPOTENCY_CONTRACT,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_stable_semantic_hash_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.stable_semantic_hash.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "stable_semantic_hash_contract_ready": True,
            "stable_semantic_hash_contract": STABLE_SEMANTIC_HASH_CONTRACT,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_volatile_field_exclusion_rule(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.volatile_field_exclusion.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "volatile_field_exclusion_rule_ready": True,
            "volatile_field_exclusion_rule": VOLATILE_FIELD_EXCLUSION_RULE,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_duplicate_detection_plan(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.duplicate_detection.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "duplicate_detection_plan_ready": True,
            "duplicate_detection_plan": DUPLICATE_DETECTION_PLAN,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_replay_no_side_effect_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.replay_no_side_effect.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "replay_no_side_effect_contract_ready": True,
            "replay_no_side_effect_contract": REPLAY_NO_SIDE_EFFECT_CONTRACT,
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
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.blocked_promotion_rationale.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocked_promotion_reasons": BLOCKED_PROMOTION_REASONS,
            "required_before_any_enablement_discussion": REQUIRED_FUTURE_EVIDENCE,
            **SAFETY_FIELDS,
        }
    )


def build_idempotency_replay_contract_plan_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    stable_semantic_hash_contract: Mapping[str, Any],
    volatile_field_exclusion_rule: Mapping[str, Any],
    duplicate_detection_plan: Mapping[str, Any],
    replay_no_side_effect_contract: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Scheduler Idempotency "
                "And Replay Contract Plan"
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
            "idempotency_contract_ready": idempotency_contract.get(
                "idempotency_contract_ready"
            ),
            "replay_contract_ready": idempotency_contract.get(
                "replay_contract_ready"
            ),
            "stable_semantic_hash_contract_ready": (
                stable_semantic_hash_contract.get(
                    "stable_semantic_hash_contract_ready"
                )
            ),
            "volatile_field_exclusion_rule_ready": (
                volatile_field_exclusion_rule.get(
                    "volatile_field_exclusion_rule_ready"
                )
            ),
            "duplicate_detection_plan_ready": duplicate_detection_plan.get(
                "duplicate_detection_plan_ready"
            ),
            "replay_no_side_effect_contract_ready": (
                replay_no_side_effect_contract.get(
                    "replay_no_side_effect_contract_ready"
                )
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "idempotency_contract": idempotency_contract.get(
                "idempotency_contract"
            ),
            "stable_semantic_hash_contract": stable_semantic_hash_contract.get(
                "stable_semantic_hash_contract"
            ),
            "volatile_field_exclusion_rule": volatile_field_exclusion_rule.get(
                "volatile_field_exclusion_rule"
            ),
            "duplicate_detection_plan": duplicate_detection_plan.get(
                "duplicate_detection_plan"
            ),
            "replay_no_side_effect_contract": replay_no_side_effect_contract.get(
                "replay_no_side_effect_contract"
            ),
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2358_ROUTE,
            "next_task": NEXT_2358_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2358_event_append_contract_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.2358_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2358_ROUTE,
            "next_task": NEXT_2358_ROUTE,
            "route_caveats": ROUTE_CAVEATS,
            "route_blockers": [],
            "route_rationale": (
                "idempotency and replay contract plan can enter event append "
                "contract planning while scheduler remains disabled"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_idempotency_replay_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2357 defines idempotency and replay contract "
                "requirements only."
            ),
            "not_scheduler_enablement": True,
            "not_real_replay_validation": True,
            "not_manual_run_execution": True,
            "not_event_append": True,
            "not_outcome_binding": True,
            "not_paper_shadow_readiness": True,
            "not_production_readiness": True,
            "not_broker_readiness": True,
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_idempotency_replay_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.safety_boundary.v1"
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
            ],
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_idempotency_replay_contract_plan_summary(
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
    source_review: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    stable_semantic_hash_contract: Mapping[str, Any],
    volatile_field_exclusion_rule: Mapping[str, Any],
    duplicate_detection_plan: Mapping[str, Any],
    replay_no_side_effect_contract: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_"
                "idempotency_replay_contract_plan.summary.v1"
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
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "idempotency_contract_ready": idempotency_contract.get(
                "idempotency_contract_ready"
            ),
            "replay_contract_ready": idempotency_contract.get(
                "replay_contract_ready"
            ),
            "stable_semantic_hash_contract_ready": (
                stable_semantic_hash_contract.get(
                    "stable_semantic_hash_contract_ready"
                )
            ),
            "volatile_field_exclusion_rule_ready": (
                volatile_field_exclusion_rule.get(
                    "volatile_field_exclusion_rule_ready"
                )
            ),
            "duplicate_detection_plan_ready": duplicate_detection_plan.get(
                "duplicate_detection_plan_ready"
            ),
            "replay_no_side_effect_contract_ready": (
                replay_no_side_effect_contract.get(
                    "replay_no_side_effect_contract_ready"
                )
            ),
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
                "aits validate-data not rerun because TRADING-2357 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, TRADING-2353 owner decision, "
                "TRADING-2354 gap closure, TRADING-2355 hardening backlog, "
                "and TRADING-2356 kill-switch plan artifacts; it does not "
                "consume fresh market data, append events, bind outcomes, "
                "produce technical features, score, backtest, or generate "
                "daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_idempotency_replay_contract_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    stable_semantic_hash_contract: Mapping[str, Any],
    volatile_field_exclusion_rule: Mapping[str, Any],
    duplicate_detection_plan: Mapping[str, Any],
    replay_no_side_effect_contract: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["idempotency_contract"], idempotency_contract)
    write_json(paths["stable_semantic_hash_contract"], stable_semantic_hash_contract)
    write_json(paths["volatile_field_exclusion_rule"], volatile_field_exclusion_rule)
    write_json(paths["duplicate_detection_plan"], duplicate_detection_plan)
    write_json(paths["replay_no_side_effect_contract"], replay_no_side_effect_contract)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(
        paths["plan_doc"],
        render_idempotency_replay_contract_plan_doc(package),
    )
    write_markdown(paths["route_doc"], render_2358_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_idempotency_replay_contract_plan_doc(package: Mapping[str, Any]) -> str:
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
    stable = mapping(package.get("stable_semantic_hash_contract", {}))
    volatile_rule = mapping(package.get("volatile_field_exclusion_rule", {}))
    duplicate_plan = mapping(package.get("duplicate_detection_plan", {}))
    replay_contract = mapping(package.get("replay_no_side_effect_contract", {}))
    guardrails = _markdown_table_from_mapping(package.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(package.get("side_effect_summary", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Idempotency Replay Contract Plan",
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
                "- idempotency_contract_ready: "
                f"`{package.get('idempotency_contract_ready')}`"
            ),
            (
                "- stable_semantic_hash_contract_ready: "
                f"`{package.get('stable_semantic_hash_contract_ready')}`"
            ),
            (
                "- volatile_field_exclusion_rule_ready: "
                f"`{package.get('volatile_field_exclusion_rule_ready')}`"
            ),
            (
                "- duplicate_detection_plan_ready: "
                f"`{package.get('duplicate_detection_plan_ready')}`"
            ),
            (
                "- replay_no_side_effect_contract_ready: "
                f"`{package.get('replay_no_side_effect_contract_ready')}`"
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
            "## Stable Semantic Hash Contract",
            "",
            f"- hash_algorithm: `{stable.get('hash_algorithm')}`",
            f"- canonicalization: `{stable.get('canonicalization')}`",
            (
                "- must_not_hash_full_json_byte_for_byte: "
                f"`{stable.get('must_not_hash_full_json_byte_for_byte')}`"
            ),
            "",
            "Stable semantic fields:",
            "",
            *[f"- `{field}`" for field in stable.get("stable_semantic_fields", [])],
            "",
            "## Volatile Field Exclusion Rule",
            "",
            f"- reason: {volatile_rule.get('reason')}",
            "",
            *[f"- `{field}`" for field in volatile_rule.get("excluded_fields", [])],
            "",
            "## Duplicate Detection Plan",
            "",
            *[
                f"- `{key}`: `{value}`"
                for key, value in duplicate_plan.items()
                if key != "duplicate_key_candidates"
            ],
            "",
            "Duplicate key candidates:",
            "",
            *[
                f"- `{field}`"
                for field in duplicate_plan.get("duplicate_key_candidates", [])
            ],
            "",
            "## Replay No-Side-Effect Contract",
            "",
            *[f"- `{key}`: `{value}`" for key, value in replay_contract.items()],
            "",
            "## Required Future Evidence",
            "",
            *[f"- `{item}`" for item in package.get("required_future_evidence", [])],
            "",
            "## Side-Effect Assertion Summary",
            "",
            *side_effects,
            "",
            "## Guardrail Status Table",
            "",
            *guardrails,
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


def render_2358_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2358 Event Append Contract Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2358 route 只能进入 event append contract plan。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是真实 replay validation、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_scheduler_idempotency_replay_contract_plan_summary.json",
        "package": output_dir
        / (
            "high_intensity_risk_cap_observe_only_scheduler_"
            "idempotency_replay_contract_plan.json"
        ),
        "source_review": output_dir
        / "high_intensity_scheduler_idempotency_replay_contract_plan_source_artifact_review.json",
        "idempotency_contract": output_dir
        / "high_intensity_scheduler_idempotency_contract.json",
        "stable_semantic_hash_contract": output_dir
        / "high_intensity_scheduler_stable_semantic_hash_contract.json",
        "volatile_field_exclusion_rule": output_dir
        / "high_intensity_scheduler_volatile_field_exclusion_rule.json",
        "duplicate_detection_plan": output_dir
        / "high_intensity_scheduler_duplicate_detection_plan.json",
        "replay_no_side_effect_contract": output_dir
        / "high_intensity_scheduler_replay_no_side_effect_contract.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_scheduler_idempotency_replay_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2358_event_append_contract_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_idempotency_replay_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_idempotency_replay_safety_boundary.json",
        "plan_doc": docs_root
        / (
            "high_intensity_risk_cap_observe_only_scheduler_"
            "idempotency_replay_contract_plan.md"
        ),
        "route_doc": docs_root / "high_intensity_2358_event_append_contract_route.md",
    }


def _validate_2356_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    kill_switch_contract = mapping(payloads["kill_switch_contract"])
    disabled_matrix = mapping(payloads["disabled_enforcement_matrix"])
    no_scheduler = mapping(payloads["no_real_scheduler_assertions"])
    manual_review = mapping(payloads["manual_review_assertions"])
    rationale = mapping(payloads["blocked_promotion_rationale"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2356_STATUS:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"TRADING-2357 requires 2356 status {EXPECTED_2356_STATUS}"
        )
    if package.get("status") != EXPECTED_2356_STATUS:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 package status"
        )
    if summary.get("readiness") != EXPECTED_2356_READINESS:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 readiness READY_FOR_2357_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2356_NEXT_ROUTE:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 summary route to idempotency plan"
        )
    if route.get("next_route") != EXPECTED_2356_NEXT_ROUTE:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 route to idempotency plan"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 source contract status PASS"
        )
    if summary.get("source_tasks") != SOURCE_2356_TASKS:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 source tasks to be 2347..2355"
        )
    if package.get("source_tasks") != SOURCE_2356_TASKS:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 package source tasks to be 2347..2355"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 evidence_chain_complete=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"TRADING-2357 requires owner_decision={OWNER_DECISION}"
        )
    required_ready_fields = {
        "kill_switch_contract_ready": summary.get("kill_switch_contract_ready"),
        "disabled_enforcement_evidence_plan_ready": summary.get(
            "disabled_enforcement_evidence_plan_ready"
        ),
        "no_real_scheduler_creation_assertions_ready": summary.get(
            "no_real_scheduler_creation_assertions_ready"
        ),
        "manual_review_required_assertions_ready": summary.get(
            "manual_review_required_assertions_ready"
        ),
        "contract_artifact_ready": kill_switch_contract.get(
            "kill_switch_contract_ready"
        ),
        "matrix_artifact_ready": disabled_matrix.get(
            "disabled_enforcement_evidence_plan_ready"
        ),
        "no_scheduler_artifact_ready": no_scheduler.get(
            "no_real_scheduler_creation_assertions_ready"
        ),
        "manual_review_artifact_ready": manual_review.get(
            "manual_review_required_assertions_ready"
        ),
    }
    for field, value in required_ready_fields.items():
        if value is not True:
            raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
                f"TRADING-2357 requires 2356 {field}=true"
            )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 promotion_decision=BLOCKED"
        )
    if rationale.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 blocked rationale promotion_allowed=false"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 promotion_allowed=false"
        )
    for item in mapping(disabled_matrix.get("disabled_enforcement_matrix")).values():
        if mapping(item).get("side_effect_allowed") is not False:
            raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
                "TRADING-2357 requires 2356 disabled matrix side_effect_allowed=false"
            )
    _validate_source_data_quality(summary, "TRADING-2356 summary")
    _validate_summary_fields(
        mapping(summary.get("guardrail_summary")),
        EXPECTED_2356_GUARDRAIL_SUMMARY,
        "TRADING-2356 guardrail",
    )
    _validate_summary_fields(
        mapping(summary.get("side_effect_summary")),
        EXPECTED_2356_SIDE_EFFECT_SUMMARY,
        "TRADING-2356 side effect",
    )


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    kill_switch_payloads: Mapping[str, Any],
) -> None:
    hardening_summary = mapping(mapping(source_inputs["hardening_backlog"])["summary"])
    kill_source_review = mapping(kill_switch_payloads["source_review"])
    if kill_source_review.get("hardening_backlog_status") != hardening_summary.get(
        "status"
    ):
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "TRADING-2357 requires 2356 source review hardening backlog status "
            "to match 2355"
        )


def _validate_summary_fields(
    payload: Mapping[str, Any],
    expected: Mapping[str, Any],
    label: str,
) -> None:
    for field, expected_value in expected.items():
        if payload.get(field) is not expected_value:
            raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
                f"TRADING-2357 requires {label} {field}={expected_value}"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2357 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    package = mapping(payloads["package"])
    stable = mapping(package.get("stable_semantic_hash_contract"))
    volatile_rule = mapping(package.get("volatile_field_exclusion_rule"))
    _validate_stable_and_volatile_contract(stable, volatile_rule)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _validate_stable_and_volatile_contract(
    stable: Mapping[str, Any],
    volatile_rule: Mapping[str, Any],
) -> None:
    stable_fields = set(stable.get("stable_semantic_fields", []))
    volatile_fields = set(volatile_rule.get("excluded_fields", []))
    overlap = stable_fields & volatile_fields
    if overlap:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"stable semantic fields overlap volatile fields: {sorted(overlap)}"
        )
    if stable.get("must_not_hash_full_json_byte_for_byte") is not True:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            "stable semantic hash contract must reject full JSON byte equality"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
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
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
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
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerIdempotencyReplayContractPlanError(
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
