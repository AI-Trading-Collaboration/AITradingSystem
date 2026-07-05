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
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_DOCS_ROOT,
    DEFAULT_GAP_CLOSURE_ROOT,
    DEFAULT_HARDENING_BACKLOG_ROOT,
    DEFAULT_KILL_SWITCH_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_IDEMPOTENCY_REPLAY_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    GUARDRAIL_SUMMARY as EXPECTED_2357_GUARDRAIL_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    NEXT_2358_ROUTE as EXPECTED_2357_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    READINESS_STATUS as EXPECTED_2357_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    SIDE_EFFECT_SUMMARY as EXPECTED_2357_SIDE_EFFECT_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    SOURCE_TASKS as SOURCE_2357_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    STATUS as EXPECTED_2357_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    TASK_REGISTER_ID as SOURCE_2357_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2358"
TASK_REGISTER_ID = "TRADING-2358_OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_event_append_contract_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_event_append_contract_plan"

STATUS = "OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
READINESS_STATUS = "READY_FOR_2359_WITH_CAVEATS"
NEXT_2359_ROUTE = "TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE

SOURCE_TASKS = [
    *SOURCE_2357_TASKS,
    "TRADING-2357",
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
    "paper_shadow_attempted": False,
    "production_attempted": False,
    "broker_action_attempted": False,
}

EVENT_SCHEMA_APPEND_CONTRACT: dict[str, Any] = {
    "required": True,
    "append_mode": "DISABLED_CONTRACT_ONLY",
    "required_fields": [
        "event_id",
        "event_type",
        "source_task",
        "source_artifact",
        "as_of",
        "stable_semantic_hash",
        "promotion_decision",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_action_enabled",
    ],
    "forbidden_without_owner_approval": [
        "event_log_write",
        "historical_event_log_append",
        "event_row_mutation",
        "outcome_binding_reference_write",
    ],
}

APPEND_IDEMPOTENCY_CONTRACT: dict[str, Any] = {
    "required": True,
    "idempotency_key_fields": [
        "event_type",
        "source_task",
        "source_artifact",
        "stable_semantic_hash",
        "as_of",
    ],
    "must_reject_duplicate_idempotency_key": True,
    "must_not_append_on_replay": True,
    "side_effect_allowed": False,
}

DUPLICATE_DETECTION_CONTRACT: dict[str, Any] = {
    "required": True,
    "duplicate_key_candidates": [
        "event_id",
        "idempotency_key",
        "source_task",
        "stable_semantic_hash",
    ],
    "duplicate_resolution": "BLOCK_APPEND_AND_REPORT_DUPLICATE",
    "must_not_mutate_existing_event": True,
    "side_effect_allowed": False,
}

APPEND_ROLLBACK_PLAN: dict[str, Any] = {
    "required_before_any_future_append": True,
    "rollback_strategy": "PLAN_ONLY_NO_MUTATION",
    "required_future_evidence": [
        "append_preflight_snapshot",
        "append_transaction_boundary",
        "append_failure_recovery_plan",
        "duplicate_append_rejection_test",
        "owner_approval_for_event_mutation",
    ],
    "side_effect_allowed": False,
}

EVENT_MUTATION_GUARDRAIL: dict[str, Any] = {
    "event_append_enabled": False,
    "must_require_owner_approval": True,
    "must_require_manual_review": True,
    "must_require_dry_run_evidence": True,
    "must_require_idempotency_evidence": True,
    "must_require_duplicate_detection_evidence": True,
    "must_block_event_log_mutation_in_this_task": True,
}

EVENT_APPEND_CONTRACT: dict[str, Any] = {
    "event_schema_required": True,
    "append_idempotency_required": True,
    "duplicate_detection_required": True,
    "rollback_plan_required": True,
    "owner_approval_required": True,
    "event_mutation_guardrail_required": True,
    "event_append_allowed": False,
    "historical_event_log_mutation_allowed": False,
    "outcome_binding_allowed": False,
    "side_effect_allowed": False,
}

REQUIRED_FUTURE_EVIDENCE = [
    "event_append_schema_tests",
    "append_idempotency_key_tests",
    "duplicate_append_rejection_tests",
    "append_rollback_preflight_tests",
    "event_mutation_owner_approval_tests",
    "outcome_binding_contract_plan",
]

EXPLICIT_NON_GOALS = [
    "This task does not append events.",
    "This task does not mutate the historical event log.",
    "This task does not bind outcomes.",
    "This task does not enable scheduler.",
    "This task does not create automated cadence.",
    "This task does not execute manual run.",
    "This task does not enable paper-shadow.",
    "This task does not enable production.",
    "This task does not call broker APIs.",
    "This task only defines event append contract requirements.",
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "EVENT_APPEND_CONTRACT_PLAN_IS_REQUIREMENT_ONLY",
    "EVENT_APPEND_IMPLEMENTATION_NOT_ALLOWED",
    "HISTORICAL_EVENT_LOG_MUTATION_NOT_ALLOWED",
    "OUTCOME_BINDING_CONTRACT_NOT_DEFINED",
    "OWNER_APPROVAL_NOT_GRANTED",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "EVENT_APPEND_DISABLED",
    "EVENT_APPEND_CONTRACT_PLAN_ONLY",
    "NO_HISTORICAL_EVENT_LOG_MUTATION",
    "NO_AUTOMATED_CADENCE",
    "NO_MANUAL_RUN_EXECUTION",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "OUTCOME_BINDING_CONTRACT_REQUIRED_NEXT",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "event_append_contract_plan_only": True,
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
    "historical_event_log_mutated",
    "event_log_mutated",
    "event_row_written",
    "event_row_mutated",
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
    "event_row_written",
    "event_row_mutated",
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


class HighIntensityEventAppendContractPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_event_append_contract_plan(
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
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityEventAppendContractPlanError(
            f"high-intensity event append contract plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_event_append_contract_plan_inputs(
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
    source_review = build_event_append_source_artifact_review(inputs=inputs)
    event_append_contract = build_event_append_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    schema_contract = build_event_schema_append_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    idempotency_contract = build_append_idempotency_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    duplicate_contract = build_duplicate_detection_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    rollback_plan = build_append_rollback_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    mutation_guardrail = build_event_mutation_guardrail(
        generated_at=generated_at,
        source_review=source_review,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_event_append_contract_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        event_append_contract=event_append_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        duplicate_contract=duplicate_contract,
        rollback_plan=rollback_plan,
        mutation_guardrail=mutation_guardrail,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2359_outcome_binding_contract_route(package=package)
    interpretation_boundary = build_event_append_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_event_append_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_event_append_contract_plan_summary(
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
        source_review=source_review,
        event_append_contract=event_append_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        duplicate_contract=duplicate_contract,
        rollback_plan=rollback_plan,
        mutation_guardrail=mutation_guardrail,
        blocked_promotion_rationale=blocked_promotion_rationale,
        package=package,
        route=route,
    )
    payloads = {
        "summary": summary,
        "package": package,
        "source_review": source_review,
        "event_append_contract": event_append_contract,
        "schema_contract": schema_contract,
        "idempotency_contract": idempotency_contract,
        "duplicate_contract": duplicate_contract,
        "rollback_plan": rollback_plan,
        "mutation_guardrail": mutation_guardrail,
        "blocked_promotion_rationale": blocked_promotion_rationale,
        "route": route,
        "interpretation_boundary": interpretation_boundary,
        "safety_boundary": safety_boundary,
    }
    _validate_generated_payloads(payloads)
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_event_append_contract_plan_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        event_append_contract=event_append_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        duplicate_contract=duplicate_contract,
        rollback_plan=rollback_plan,
        mutation_guardrail=mutation_guardrail,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_event_append_contract_plan_inputs(
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
) -> dict[str, Any]:
    try:
        source_inputs = (
            load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs(
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
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensityEventAppendContractPlanError(
            f"TRADING-2358 source chain invalid before 2357: {exc}"
        ) from exc

    paths = {
        "summary": idempotency_replay_dir
        / "high_intensity_scheduler_idempotency_replay_contract_plan_summary.json",
        "package": idempotency_replay_dir
        / (
            "high_intensity_risk_cap_observe_only_scheduler_"
            "idempotency_replay_contract_plan.json"
        ),
        "source_review": idempotency_replay_dir
        / (
            "high_intensity_scheduler_idempotency_replay_contract_plan_"
            "source_artifact_review.json"
        ),
        "idempotency_contract": idempotency_replay_dir
        / "high_intensity_scheduler_idempotency_contract.json",
        "stable_semantic_hash_contract": idempotency_replay_dir
        / "high_intensity_scheduler_stable_semantic_hash_contract.json",
        "volatile_field_exclusion_rule": idempotency_replay_dir
        / "high_intensity_scheduler_volatile_field_exclusion_rule.json",
        "duplicate_detection_plan": idempotency_replay_dir
        / "high_intensity_scheduler_duplicate_detection_plan.json",
        "replay_no_side_effect_contract": idempotency_replay_dir
        / "high_intensity_scheduler_replay_no_side_effect_contract.json",
        "blocked_promotion_rationale": idempotency_replay_dir
        / "high_intensity_scheduler_idempotency_replay_blocked_promotion_rationale.json",
        "route": idempotency_replay_dir
        / "high_intensity_2358_event_append_contract_route.json",
        "interpretation_boundary": idempotency_replay_dir
        / "high_intensity_scheduler_idempotency_replay_interpretation_boundary.json",
        "safety_boundary": idempotency_replay_dir
        / "high_intensity_scheduler_idempotency_replay_safety_boundary.json",
    }
    idempotency_payloads = _load_required_payloads(paths, "TRADING-2358")
    _validate_2357_source_contracts(idempotency_payloads)
    _validate_cross_source_contracts(source_inputs, idempotency_payloads)
    return {
        **source_inputs,
        "idempotency_replay_plan": idempotency_payloads,
        "idempotency_replay_plan_paths": _string_paths(paths),
    }


def build_event_append_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    idempotency = mapping(inputs["idempotency_replay_plan"])
    summary = mapping(idempotency["summary"])
    source_review = mapping(idempotency["source_review"])
    inherited_rows = list(source_review.get("source_task_evidence", []))
    source_task_evidence = [
        *inherited_rows,
        {
            "task": "TRADING-2357",
            "status": summary.get("status"),
            "evidence": "idempotency and replay contract plan present",
            "evidence_present": True,
            "promotion_result": "blocked",
        },
    ]
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_event_append_contract_"
                "plan.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *summary.get("source_task_ids", []),
                SOURCE_2357_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_task_evidence,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "idempotency_replay_artifacts_read": True,
            "idempotency_replay_artifact_count": len(mapping(idempotency)),
            "idempotency_replay_status": summary.get("status"),
            "idempotency_replay_readiness": summary.get("readiness"),
            "idempotency_replay_next_route": summary.get("next_route"),
            "idempotency_contract_ready": summary.get("idempotency_contract_ready"),
            "stable_semantic_hash_contract_ready": summary.get(
                "stable_semantic_hash_contract_ready"
            ),
            "volatile_field_exclusion_rule_ready": summary.get(
                "volatile_field_exclusion_rule_ready"
            ),
            "duplicate_detection_plan_ready": summary.get(
                "duplicate_detection_plan_ready"
            ),
            "replay_no_side_effect_contract_ready": summary.get(
                "replay_no_side_effect_contract_ready"
            ),
            "kill_switch_status": source_review.get("kill_switch_status"),
            "kill_switch_readiness": source_review.get("kill_switch_readiness"),
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


def build_event_append_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="event_append_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="event_append_contract_ready",
        payload_key="event_append_contract",
        payload=EVENT_APPEND_CONTRACT,
    )


def build_event_schema_append_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="event_schema_append_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="event_schema_append_contract_ready",
        payload_key="event_schema_append_contract",
        payload=EVENT_SCHEMA_APPEND_CONTRACT,
    )


def build_append_idempotency_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="append_idempotency_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="append_idempotency_contract_ready",
        payload_key="append_idempotency_contract",
        payload=APPEND_IDEMPOTENCY_CONTRACT,
    )


def build_duplicate_detection_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="duplicate_detection_contract",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="duplicate_detection_contract_ready",
        payload_key="duplicate_detection_contract",
        payload=DUPLICATE_DETECTION_CONTRACT,
    )


def build_append_rollback_plan(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="append_rollback_plan",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="append_rollback_plan_ready",
        payload_key="append_rollback_plan",
        payload=APPEND_ROLLBACK_PLAN,
    )


def build_event_mutation_guardrail(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _contract_payload(
        schema_name="event_mutation_guardrail",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="event_mutation_guardrail_ready",
        payload_key="event_mutation_guardrail",
        payload=EVENT_MUTATION_GUARDRAIL,
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
                "high_intensity_risk_cap_observe_only_event_append_contract_"
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
                "high_intensity_risk_cap_observe_only_event_append_contract_"
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
            "required_before_any_event_append": REQUIRED_FUTURE_EVIDENCE,
            **SAFETY_FIELDS,
        }
    )


def build_event_append_contract_plan_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    schema_contract: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    duplicate_contract: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    mutation_guardrail: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_event_append_contract_"
                "plan.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Event Append Contract Plan"
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
            "event_append_contract_ready": event_append_contract.get(
                "event_append_contract_ready"
            ),
            "event_schema_append_contract_ready": schema_contract.get(
                "event_schema_append_contract_ready"
            ),
            "append_idempotency_contract_ready": idempotency_contract.get(
                "append_idempotency_contract_ready"
            ),
            "duplicate_detection_contract_ready": duplicate_contract.get(
                "duplicate_detection_contract_ready"
            ),
            "append_rollback_plan_ready": rollback_plan.get(
                "append_rollback_plan_ready"
            ),
            "event_mutation_guardrail_ready": mutation_guardrail.get(
                "event_mutation_guardrail_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "event_append_contract": event_append_contract.get(
                "event_append_contract"
            ),
            "event_schema_append_contract": schema_contract.get(
                "event_schema_append_contract"
            ),
            "append_idempotency_contract": idempotency_contract.get(
                "append_idempotency_contract"
            ),
            "duplicate_detection_contract": duplicate_contract.get(
                "duplicate_detection_contract"
            ),
            "append_rollback_plan": rollback_plan.get("append_rollback_plan"),
            "event_mutation_guardrail": mutation_guardrail.get(
                "event_mutation_guardrail"
            ),
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2359_ROUTE,
            "next_task": NEXT_2359_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2359_outcome_binding_contract_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_event_append_contract_"
                "plan.2359_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2359_ROUTE,
            "next_task": NEXT_2359_ROUTE,
            "route_caveats": ROUTE_CAVEATS,
            "route_blockers": [],
            "route_rationale": (
                "event append contract plan can enter outcome binding contract "
                "planning while event append remains disabled"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_event_append_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_event_append_contract_"
                "plan.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2358 defines event append contract requirements only."
            ),
            "not_event_append_implementation": True,
            "not_event_mutation_approval": True,
            "not_scheduler_enablement": True,
            "not_manual_run_execution": True,
            "not_outcome_binding": True,
            "not_paper_shadow_readiness": True,
            "not_production_readiness": True,
            "not_broker_readiness": True,
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_event_append_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_event_append_contract_"
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


def build_event_append_contract_plan_summary(
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
    source_review: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    schema_contract: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    duplicate_contract: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    mutation_guardrail: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_event_append_contract_"
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
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "event_append_contract_ready": event_append_contract.get(
                "event_append_contract_ready"
            ),
            "event_schema_append_contract_ready": schema_contract.get(
                "event_schema_append_contract_ready"
            ),
            "append_idempotency_contract_ready": idempotency_contract.get(
                "append_idempotency_contract_ready"
            ),
            "duplicate_detection_contract_ready": duplicate_contract.get(
                "duplicate_detection_contract_ready"
            ),
            "append_rollback_plan_ready": rollback_plan.get(
                "append_rollback_plan_ready"
            ),
            "event_mutation_guardrail_ready": mutation_guardrail.get(
                "event_mutation_guardrail_ready"
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
                "aits validate-data not rerun because TRADING-2358 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, TRADING-2353 owner decision, "
                "TRADING-2354 gap closure, TRADING-2355 hardening backlog, "
                "TRADING-2356 kill-switch plan, and TRADING-2357 idempotency "
                "replay contract artifacts; it does not consume fresh market "
                "data, append events, bind outcomes, produce technical "
                "features, score, backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_event_append_contract_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    schema_contract: Mapping[str, Any],
    idempotency_contract: Mapping[str, Any],
    duplicate_contract: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    mutation_guardrail: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["event_append_contract"], event_append_contract)
    write_json(paths["schema_contract"], schema_contract)
    write_json(paths["idempotency_contract"], idempotency_contract)
    write_json(paths["duplicate_contract"], duplicate_contract)
    write_json(paths["rollback_plan"], rollback_plan)
    write_json(paths["mutation_guardrail"], mutation_guardrail)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_event_append_contract_plan_doc(package))
    write_markdown(paths["route_doc"], render_2359_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_event_append_contract_plan_doc(package: Mapping[str, Any]) -> str:
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
    guardrails = _markdown_table_from_mapping(package.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(package.get("side_effect_summary", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Event Append Contract Plan",
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
                "- event_append_contract_ready: "
                f"`{package.get('event_append_contract_ready')}`"
            ),
            (
                "- event_schema_append_contract_ready: "
                f"`{package.get('event_schema_append_contract_ready')}`"
            ),
            (
                "- append_idempotency_contract_ready: "
                f"`{package.get('append_idempotency_contract_ready')}`"
            ),
            (
                "- duplicate_detection_contract_ready: "
                f"`{package.get('duplicate_detection_contract_ready')}`"
            ),
            (
                "- append_rollback_plan_ready: "
                f"`{package.get('append_rollback_plan_ready')}`"
            ),
            (
                "- event_mutation_guardrail_ready: "
                f"`{package.get('event_mutation_guardrail_ready')}`"
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
            "## Event Schema Append Contract",
            "",
            *_markdown_table_from_mapping(
                package.get("event_schema_append_contract", {})
            ),
            "",
            "## Append Idempotency Contract",
            "",
            *_markdown_table_from_mapping(
                package.get("append_idempotency_contract", {})
            ),
            "",
            "## Duplicate Detection Contract",
            "",
            *_markdown_table_from_mapping(
                package.get("duplicate_detection_contract", {})
            ),
            "",
            "## Append Rollback Plan",
            "",
            *_markdown_table_from_mapping(package.get("append_rollback_plan", {})),
            "",
            "## Event Mutation Guardrail",
            "",
            *_markdown_table_from_mapping(
                package.get("event_mutation_guardrail", {})
            ),
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


def render_2359_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2359 Outcome Binding Contract Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2359 route 只能进入 outcome binding contract plan。",
            "它不是 event append implementation、不是 event mutation approval、",
            "不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_event_append_contract_plan_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_event_append_contract_plan.json",
        "source_review": output_dir
        / "high_intensity_event_append_contract_plan_source_artifact_review.json",
        "event_append_contract": output_dir
        / "high_intensity_event_append_contract.json",
        "schema_contract": output_dir
        / "high_intensity_event_schema_append_contract.json",
        "idempotency_contract": output_dir
        / "high_intensity_append_idempotency_contract.json",
        "duplicate_contract": output_dir
        / "high_intensity_event_duplicate_detection_contract.json",
        "rollback_plan": output_dir / "high_intensity_append_rollback_plan.json",
        "mutation_guardrail": output_dir
        / "high_intensity_event_mutation_guardrail.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_event_append_blocked_promotion_rationale.json",
        "route": output_dir
        / "high_intensity_2359_outcome_binding_contract_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_event_append_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_event_append_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_event_append_contract_plan.md",
        "route_doc": docs_root
        / "high_intensity_2359_outcome_binding_contract_route.md",
    }


def _validate_2357_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    idempotency_contract = mapping(payloads["idempotency_contract"])
    stable_hash = mapping(payloads["stable_semantic_hash_contract"])
    volatile_rule = mapping(payloads["volatile_field_exclusion_rule"])
    duplicate_plan = mapping(payloads["duplicate_detection_plan"])
    replay_contract = mapping(payloads["replay_no_side_effect_contract"])
    rationale = mapping(payloads["blocked_promotion_rationale"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2357_STATUS:
        raise HighIntensityEventAppendContractPlanError(
            f"TRADING-2358 requires 2357 status {EXPECTED_2357_STATUS}"
        )
    if package.get("status") != EXPECTED_2357_STATUS:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 package status"
        )
    if summary.get("readiness") != EXPECTED_2357_READINESS:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 readiness READY_FOR_2358_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2357_NEXT_ROUTE:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 summary route to event append plan"
        )
    if route.get("next_route") != EXPECTED_2357_NEXT_ROUTE:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 route to event append plan"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 source contract status PASS"
        )
    if summary.get("source_tasks") != SOURCE_2357_TASKS:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 source tasks to be 2347..2356"
        )
    if package.get("source_tasks") != SOURCE_2357_TASKS:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 package source tasks to be 2347..2356"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 evidence_chain_complete=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensityEventAppendContractPlanError(
            f"TRADING-2358 requires owner_decision={OWNER_DECISION}"
        )
    ready_fields = {
        "idempotency_contract_ready": summary.get("idempotency_contract_ready"),
        "stable_semantic_hash_contract_ready": summary.get(
            "stable_semantic_hash_contract_ready"
        ),
        "volatile_field_exclusion_rule_ready": summary.get(
            "volatile_field_exclusion_rule_ready"
        ),
        "duplicate_detection_plan_ready": summary.get(
            "duplicate_detection_plan_ready"
        ),
        "replay_no_side_effect_contract_ready": summary.get(
            "replay_no_side_effect_contract_ready"
        ),
        "idempotency_artifact_ready": idempotency_contract.get(
            "idempotency_contract_ready"
        ),
        "stable_hash_artifact_ready": stable_hash.get(
            "stable_semantic_hash_contract_ready"
        ),
        "volatile_artifact_ready": volatile_rule.get(
            "volatile_field_exclusion_rule_ready"
        ),
        "duplicate_artifact_ready": duplicate_plan.get(
            "duplicate_detection_plan_ready"
        ),
        "replay_artifact_ready": replay_contract.get(
            "replay_no_side_effect_contract_ready"
        ),
    }
    for field, value in ready_fields.items():
        if value is not True:
            raise HighIntensityEventAppendContractPlanError(
                f"TRADING-2358 requires 2357 {field}=true"
            )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 promotion_decision=BLOCKED"
        )
    if rationale.get("promotion_allowed") is not False:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 blocked rationale promotion_allowed=false"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 promotion_allowed=false"
        )
    if (
        mapping(duplicate_plan.get("duplicate_detection_plan")).get(
            "side_effect_allowed"
        )
        is not False
    ):
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 duplicate detection side_effect_allowed=false"
        )
    replay = mapping(replay_contract.get("replay_no_side_effect_contract"))
    if replay.get("must_not_append_event") is not True:
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 replay contract to block event append"
        )
    _validate_source_data_quality(summary, "TRADING-2357 summary")
    _validate_expected_summary(
        mapping(summary.get("guardrail_summary")),
        EXPECTED_2357_GUARDRAIL_SUMMARY,
        "TRADING-2357 guardrail",
    )
    _validate_expected_summary(
        mapping(summary.get("side_effect_summary")),
        EXPECTED_2357_SIDE_EFFECT_SUMMARY,
        "TRADING-2357 side effect",
    )


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    idempotency_payloads: Mapping[str, Any],
) -> None:
    kill_summary = mapping(mapping(source_inputs["kill_switch_plan"])["summary"])
    idempotency_source_review = mapping(idempotency_payloads["source_review"])
    if idempotency_source_review.get("kill_switch_status") != kill_summary.get(
        "status"
    ):
        raise HighIntensityEventAppendContractPlanError(
            "TRADING-2358 requires 2357 source review kill-switch status to match 2356"
        )


def _validate_expected_summary(
    payload: Mapping[str, Any],
    expected: Mapping[str, Any],
    label: str,
) -> None:
    for field, expected_value in expected.items():
        if payload.get(field) is not expected_value:
            raise HighIntensityEventAppendContractPlanError(
                f"TRADING-2358 requires {label} {field}={expected_value}"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2358 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    package = mapping(payloads["package"])
    _validate_event_append_contracts(package)


def _validate_event_append_contracts(package: Mapping[str, Any]) -> None:
    idempotency = mapping(package.get("append_idempotency_contract"))
    duplicate = mapping(package.get("duplicate_detection_contract"))
    rollback = mapping(package.get("append_rollback_plan"))
    mutation = mapping(package.get("event_mutation_guardrail"))
    if "stable_semantic_hash" not in idempotency.get("idempotency_key_fields", []):
        raise HighIntensityEventAppendContractPlanError(
            "append idempotency contract must include stable_semantic_hash"
        )
    if duplicate.get("duplicate_resolution") != "BLOCK_APPEND_AND_REPORT_DUPLICATE":
        raise HighIntensityEventAppendContractPlanError(
            "duplicate detection contract must block append"
        )
    if duplicate.get("side_effect_allowed") is not False:
        raise HighIntensityEventAppendContractPlanError(
            "duplicate detection contract must not allow side effects"
        )
    if rollback.get("rollback_strategy") != "PLAN_ONLY_NO_MUTATION":
        raise HighIntensityEventAppendContractPlanError(
            "append rollback plan must be plan-only/no-mutation"
        )
    if mutation.get("event_append_enabled") is not False:
        raise HighIntensityEventAppendContractPlanError(
            "event mutation guardrail requires event_append_enabled=false"
        )


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensityEventAppendContractPlanError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensityEventAppendContractPlanError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityEventAppendContractPlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityEventAppendContractPlanError(
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
        raise HighIntensityEventAppendContractPlanError(
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
        raise HighIntensityEventAppendContractPlanError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensityEventAppendContractPlanError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensityEventAppendContractPlanError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensityEventAppendContractPlanError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensityEventAppendContractPlanError(
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
