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
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_DOCS_ROOT,
    DEFAULT_GAP_CLOSURE_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_scheduler_hardening_backlog_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_HARDENING_BACKLOG_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    NEXT_2356_ROUTE as EXPECTED_2355_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    READINESS_STATUS as EXPECTED_2355_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    SOURCE_TASKS as SOURCE_2355_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    STATUS as EXPECTED_2355_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    TASK_REGISTER_ID as SOURCE_2355_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2356"
TASK_REGISTER_ID = (
    "TRADING-2356_OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_"
    "ENFORCEMENT_EVIDENCE_PLAN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_kill_switch_plan"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_ENFORCEMENT_EVIDENCE_"
    "PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2357_WITH_CAVEATS"
NEXT_2357_ROUTE = (
    "TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_Contract_Plan"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE

SOURCE_TASKS = [
    "TRADING-2347",
    "TRADING-2348",
    "TRADING-2349",
    "TRADING-2350",
    "TRADING-2351",
    "TRADING-2352",
    "TRADING-2353",
    "TRADING-2354",
    "TRADING-2355",
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

KILL_SWITCH_CONTRACT: dict[str, dict[str, Any]] = {
    "scheduler_global_kill_switch": {
        "required": True,
        "default_state": "OFF",
        "must_block_real_scheduler_creation": True,
        "must_block_cron_creation": True,
        "must_block_windows_task_creation": True,
        "must_block_github_actions_schedule_creation": True,
        "owner_approval_required_to_change": True,
    },
    "manual_run_kill_switch": {
        "required": True,
        "default_state": "DRY_RUN_ONLY",
        "must_block_manual_run_execution": True,
        "must_allow_preview_only": True,
        "owner_approval_required_to_change": True,
    },
    "mutation_kill_switch": {
        "required": True,
        "default_state": "NO_MUTATION",
        "must_block_event_append": True,
        "must_block_outcome_binding": True,
        "must_block_outcome_store_mutation": True,
        "owner_approval_required_to_change": True,
    },
    "trading_path_kill_switch": {
        "required": True,
        "default_state": "NO_TRADING_PATH",
        "must_block_paper_shadow": True,
        "must_block_production": True,
        "must_block_broker_action": True,
        "owner_approval_required_to_change": True,
    },
}

DISABLED_ENFORCEMENT_MATRIX: dict[str, dict[str, Any]] = {
    "config_default": {
        "expected": "scheduler_enabled_false",
        "required_evidence": "static_config_assertion",
        "side_effect_allowed": False,
    },
    "cli_entry": {
        "expected": "dry_run_preview_only",
        "required_evidence": "cli_guardrail_assertion",
        "side_effect_allowed": False,
    },
    "runtime_override": {
        "expected": "override_to_enable_rejected",
        "required_evidence": "runtime_override_rejection_assertion",
        "side_effect_allowed": False,
    },
    "environment_override": {
        "expected": "env_override_to_enable_rejected",
        "required_evidence": "environment_override_rejection_assertion",
        "side_effect_allowed": False,
    },
    "artifact_replay": {
        "expected": "stable_no_side_effect_replay",
        "required_evidence": "semantic_replay_assertion",
        "side_effect_allowed": False,
    },
}

NO_REAL_SCHEDULER_ASSERTION_PLAN: dict[str, dict[str, Any]] = {
    "cron_creation_assertion": {
        "required": True,
        "expected": "cron_created_false",
        "required_evidence": "filesystem_and_config_no_cron_assertion",
        "side_effect_allowed": False,
    },
    "windows_task_creation_assertion": {
        "required": True,
        "expected": "windows_task_created_false",
        "required_evidence": "windows_task_no_creation_assertion",
        "side_effect_allowed": False,
    },
    "github_actions_schedule_assertion": {
        "required": True,
        "expected": "github_actions_schedule_created_false",
        "required_evidence": "workflow_schedule_no_creation_assertion",
        "side_effect_allowed": False,
    },
    "scheduled_tasks_config_assertion": {
        "required": True,
        "expected": "scheduled_tasks_config_not_modified",
        "required_evidence": "config_scheduled_tasks_no_enablement_assertion",
        "side_effect_allowed": False,
    },
}

MANUAL_REVIEW_REQUIRED_ASSERTION_PLAN: dict[str, dict[str, Any]] = {
    "owner_decision_assertion": {
        "required": True,
        "expected": OWNER_DECISION,
        "required_evidence": "owner_decision_record_assertion",
        "side_effect_allowed": False,
    },
    "manual_review_gate_assertion": {
        "required": True,
        "expected": "manual_review_required_true",
        "required_evidence": "manual_review_gate_status_assertion",
        "side_effect_allowed": False,
    },
    "promotion_rejection_assertion": {
        "required": True,
        "expected": "promotion_allowed_false",
        "required_evidence": "blocked_promotion_decision_assertion",
        "side_effect_allowed": False,
    },
}

REQUIRED_FUTURE_EVIDENCE = [
    "kill_switch_contract_tests",
    "disabled_by_default_assertions",
    "no_real_scheduler_creation_assertions",
    "runtime_override_rejection_tests",
    "environment_override_rejection_tests",
    "manual_review_required_assertions",
    "owner_approval_required_assertions",
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
    "This task does not approve promotion.",
    (
        "This task only defines kill-switch and disabled-enforcement evidence "
        "requirements."
    ),
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "KILL_SWITCH_PLAN_IS_EVIDENCE_REQUIREMENT_ONLY",
    "KILL_SWITCH_CONTRACT_TESTS_NOT_IMPLEMENTED",
    "DISABLED_BY_DEFAULT_ASSERTIONS_NOT_IMPLEMENTED",
    "NO_REAL_SCHEDULER_CREATION_ASSERTIONS_NOT_IMPLEMENTED",
    "RUNTIME_OVERRIDE_REJECTION_TESTS_NOT_IMPLEMENTED",
    "ENVIRONMENT_OVERRIDE_REJECTION_TESTS_NOT_IMPLEMENTED",
    "IDEMPOTENCY_AND_REPLAY_CONTRACT_NOT_DEFINED",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "KILL_SWITCH_PLAN_ONLY",
    "DISABLED_ENFORCEMENT_EVIDENCE_PLAN_ONLY",
    "NO_AUTOMATED_CADENCE",
    "NO_MANUAL_RUN_EXECUTION",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "IDEMPOTENCY_EVIDENCE_REQUIRED_NEXT",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "kill_switch_plan_only": True,
    "disabled_enforcement_evidence_plan_only": True,
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


class HighIntensitySchedulerKillSwitchPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan(
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
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"high-intensity scheduler kill-switch plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_kill_switch_plan_inputs(
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
    source_review = build_kill_switch_source_artifact_review(inputs=inputs)
    kill_switch_contract = build_kill_switch_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    disabled_enforcement_matrix = build_disabled_enforcement_matrix(
        generated_at=generated_at,
        source_review=source_review,
    )
    no_real_scheduler_assertions = build_no_real_scheduler_creation_assertions(
        generated_at=generated_at,
        source_review=source_review,
    )
    manual_review_assertions = build_manual_review_required_assertions(
        generated_at=generated_at,
        source_review=source_review,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_kill_switch_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        kill_switch_contract=kill_switch_contract,
        disabled_enforcement_matrix=disabled_enforcement_matrix,
        no_real_scheduler_assertions=no_real_scheduler_assertions,
        manual_review_assertions=manual_review_assertions,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2357_scheduler_idempotency_route(package=package)
    interpretation_boundary = build_kill_switch_plan_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_kill_switch_plan_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_kill_switch_plan_summary(
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
        source_review=source_review,
        kill_switch_contract=kill_switch_contract,
        disabled_enforcement_matrix=disabled_enforcement_matrix,
        no_real_scheduler_assertions=no_real_scheduler_assertions,
        manual_review_assertions=manual_review_assertions,
        blocked_promotion_rationale=blocked_promotion_rationale,
        package=package,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "package": package,
            "source_review": source_review,
            "kill_switch_contract": kill_switch_contract,
            "disabled_enforcement_matrix": disabled_enforcement_matrix,
            "no_real_scheduler_assertions": no_real_scheduler_assertions,
            "manual_review_assertions": manual_review_assertions,
            "blocked_promotion_rationale": blocked_promotion_rationale,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_kill_switch_plan_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        kill_switch_contract=kill_switch_contract,
        disabled_enforcement_matrix=disabled_enforcement_matrix,
        no_real_scheduler_assertions=no_real_scheduler_assertions,
        manual_review_assertions=manual_review_assertions,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_kill_switch_plan_inputs(
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
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_hardening_backlog_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
            audit_package_dir=audit_package_dir,
            owner_decision_dir=owner_decision_dir,
            gap_closure_dir=gap_closure_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"TRADING-2356 source chain invalid before 2355: {exc}"
        ) from exc

    paths = {
        "summary": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_backlog_summary.json",
        "package": hardening_backlog_dir
        / "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog.json",
        "source_review": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_backlog_source_artifact_review.json",
        "hardening_backlog": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_backlog_items.json",
        "evidence_matrix": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_evidence_matrix.json",
        "blocked_promotion_rationale": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_blocked_promotion_rationale.json",
        "route": hardening_backlog_dir
        / "high_intensity_2356_scheduler_kill_switch_route.json",
        "interpretation_boundary": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_backlog_interpretation_boundary.json",
        "safety_boundary": hardening_backlog_dir
        / "high_intensity_scheduler_hardening_backlog_safety_boundary.json",
    }
    hardening_payloads = _load_required_payloads(paths, "TRADING-2356")
    _validate_2355_source_contracts(hardening_payloads)
    _validate_cross_source_contracts(source_inputs, hardening_payloads)
    return {
        **source_inputs,
        "hardening_backlog": hardening_payloads,
        "hardening_backlog_paths": _string_paths(paths),
    }


def build_kill_switch_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    hardening = mapping(inputs["hardening_backlog"])
    hardening_summary = mapping(hardening["summary"])
    hardening_source_review = mapping(hardening["source_review"])
    inherited_rows = list(hardening_source_review.get("source_task_evidence", []))
    source_task_evidence = [
        *inherited_rows,
        {
            "task": "TRADING-2355",
            "status": hardening_summary.get("status"),
            "evidence": "hardening backlog and evidence matrix present",
            "evidence_present": True,
            "promotion_result": "blocked",
        },
    ]
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *hardening_summary.get("source_task_ids", []),
                SOURCE_2355_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_task_evidence,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "hardening_backlog_artifacts_read": True,
            "hardening_backlog_artifact_count": len(mapping(hardening)),
            "hardening_backlog_status": hardening_summary.get("status"),
            "hardening_backlog_readiness": hardening_summary.get("readiness"),
            "hardening_backlog_next_route": hardening_summary.get("next_route"),
            "hardening_backlog_ready": hardening_summary.get(
                "hardening_backlog_ready"
            ),
            "evidence_matrix_ready": hardening_summary.get("evidence_matrix_ready"),
            "owner_decision": OWNER_DECISION,
            "evidence_chain_complete": True,
            "source_validate_data_executed": hardening_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": hardening_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": hardening_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": hardening_summary.get(
                "source_validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_kill_switch_contract(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.kill_switch_contract.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "kill_switch_contract_ready": True,
            "kill_switch_contract": KILL_SWITCH_CONTRACT,
            "contract_paths": list(KILL_SWITCH_CONTRACT),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_disabled_enforcement_matrix(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.disabled_enforcement_matrix.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "disabled_enforcement_evidence_plan_ready": True,
            "disabled_enforcement_matrix": DISABLED_ENFORCEMENT_MATRIX,
            "matrix_item_count": len(DISABLED_ENFORCEMENT_MATRIX),
            "all_matrix_items_side_effect_allowed": False,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_no_real_scheduler_creation_assertions(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.no_real_scheduler_creation_assertions.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "no_real_scheduler_creation_assertions_ready": True,
            "assertion_plan": NO_REAL_SCHEDULER_ASSERTION_PLAN,
            "assertion_count": len(NO_REAL_SCHEDULER_ASSERTION_PLAN),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_manual_review_required_assertions(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.manual_review_required_assertions.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "manual_review_required_assertions_ready": True,
            "assertion_plan": MANUAL_REVIEW_REQUIRED_ASSERTION_PLAN,
            "assertion_count": len(MANUAL_REVIEW_REQUIRED_ASSERTION_PLAN),
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
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
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
            "required_before_any_enablement_discussion": REQUIRED_FUTURE_EVIDENCE,
            **SAFETY_FIELDS,
        }
    )


def build_kill_switch_plan_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    kill_switch_contract: Mapping[str, Any],
    disabled_enforcement_matrix: Mapping[str, Any],
    no_real_scheduler_assertions: Mapping[str, Any],
    manual_review_assertions: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Scheduler Kill-Switch "
                "And Disabled-Enforcement Evidence Plan"
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
            "kill_switch_contract_ready": True,
            "disabled_enforcement_evidence_plan_ready": True,
            "no_real_scheduler_creation_assertions_ready": True,
            "manual_review_required_assertions_ready": True,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "kill_switch_contract": kill_switch_contract.get("kill_switch_contract"),
            "disabled_enforcement_matrix": disabled_enforcement_matrix.get(
                "disabled_enforcement_matrix"
            ),
            "no_real_scheduler_creation_assertion_plan": (
                no_real_scheduler_assertions.get("assertion_plan")
            ),
            "manual_review_required_assertion_plan": manual_review_assertions.get(
                "assertion_plan"
            ),
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2357_ROUTE,
            "next_task": NEXT_2357_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2357_scheduler_idempotency_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.2357_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2357_ROUTE,
            "next_task": NEXT_2357_ROUTE,
            "route_caveats": ROUTE_CAVEATS,
            "route_blockers": [],
            "route_rationale": (
                "kill-switch and disabled-enforcement evidence plan can enter "
                "scheduler idempotency and replay contract planning"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_kill_switch_plan_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
                "plan.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2356 defines evidence requirements for scheduler "
                "kill-switch and disabled-by-default enforcement only."
            ),
            "not_scheduler_enablement": True,
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


def build_kill_switch_plan_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
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


def build_kill_switch_plan_summary(
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
    source_review: Mapping[str, Any],
    kill_switch_contract: Mapping[str, Any],
    disabled_enforcement_matrix: Mapping[str, Any],
    no_real_scheduler_assertions: Mapping[str, Any],
    manual_review_assertions: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_scheduler_kill_switch_"
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
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "kill_switch_contract_ready": kill_switch_contract.get(
                "kill_switch_contract_ready"
            ),
            "disabled_enforcement_evidence_plan_ready": (
                disabled_enforcement_matrix.get(
                    "disabled_enforcement_evidence_plan_ready"
                )
            ),
            "no_real_scheduler_creation_assertions_ready": (
                no_real_scheduler_assertions.get(
                    "no_real_scheduler_creation_assertions_ready"
                )
            ),
            "manual_review_required_assertions_ready": (
                manual_review_assertions.get(
                    "manual_review_required_assertions_ready"
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
                "aits validate-data not rerun because TRADING-2356 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, TRADING-2353 owner decision, "
                "TRADING-2354 gap closure, and TRADING-2355 hardening backlog "
                "artifacts; it does not consume fresh market data, append "
                "events, bind outcomes, produce technical features, score, "
                "backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_kill_switch_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    kill_switch_contract: Mapping[str, Any],
    disabled_enforcement_matrix: Mapping[str, Any],
    no_real_scheduler_assertions: Mapping[str, Any],
    manual_review_assertions: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["kill_switch_contract"], kill_switch_contract)
    write_json(paths["disabled_enforcement_matrix"], disabled_enforcement_matrix)
    write_json(paths["no_real_scheduler_assertions"], no_real_scheduler_assertions)
    write_json(paths["manual_review_assertions"], manual_review_assertions)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_kill_switch_plan_doc(package))
    write_markdown(paths["route_doc"], render_2357_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_kill_switch_plan_doc(package: Mapping[str, Any]) -> str:
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
    contract_rows = [
        "|Switch|Default State|Required|Owner Approval Required|",
        "|---|---|---|---|",
    ]
    for key, value in mapping(package.get("kill_switch_contract", {})).items():
        item = mapping(value)
        contract_rows.append(
            "|"
            f"`{key}`|"
            f"`{item.get('default_state')}`|"
            f"`{item.get('required')}`|"
            f"`{item.get('owner_approval_required_to_change')}`|"
        )
    matrix_rows = [
        "|Surface|Expected|Required Evidence|Side Effect Allowed|",
        "|---|---|---|---|",
    ]
    for key, value in mapping(package.get("disabled_enforcement_matrix", {})).items():
        item = mapping(value)
        matrix_rows.append(
            "|"
            f"`{key}`|"
            f"`{item.get('expected')}`|"
            f"`{item.get('required_evidence')}`|"
            f"`{item.get('side_effect_allowed')}`|"
        )
    guardrails = _markdown_table_from_mapping(package.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(package.get("side_effect_summary", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Kill-Switch Plan",
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
                "- kill_switch_contract_ready: "
                f"`{package.get('kill_switch_contract_ready')}`"
            ),
            (
                "- disabled_enforcement_evidence_plan_ready: "
                f"`{package.get('disabled_enforcement_evidence_plan_ready')}`"
            ),
            (
                "- no_real_scheduler_creation_assertions_ready: "
                f"`{package.get('no_real_scheduler_creation_assertions_ready')}`"
            ),
            (
                "- manual_review_required_assertions_ready: "
                f"`{package.get('manual_review_required_assertions_ready')}`"
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
            "## Kill-Switch Contract",
            "",
            *contract_rows,
            "",
            "## Disabled-By-Default Enforcement Matrix",
            "",
            *matrix_rows,
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


def render_2357_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2357 Scheduler Idempotency Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2357 route 只能进入 scheduler idempotency and replay contract plan。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是 event append、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_kill_switch_plan_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.json",
        "source_review": output_dir
        / "high_intensity_scheduler_kill_switch_plan_source_artifact_review.json",
        "kill_switch_contract": output_dir
        / "high_intensity_scheduler_kill_switch_contract.json",
        "disabled_enforcement_matrix": output_dir
        / "high_intensity_scheduler_disabled_enforcement_matrix.json",
        "no_real_scheduler_assertions": output_dir
        / "high_intensity_scheduler_no_real_scheduler_creation_assertions.json",
        "manual_review_assertions": output_dir
        / "high_intensity_scheduler_manual_review_required_assertions.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_scheduler_kill_switch_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2357_scheduler_idempotency_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_kill_switch_plan_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_kill_switch_plan_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.md",
        "route_doc": docs_root / "high_intensity_2357_scheduler_idempotency_route.md",
    }


def _validate_2355_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    hardening_backlog = mapping(payloads["hardening_backlog"])
    evidence_matrix = mapping(payloads["evidence_matrix"])
    rationale = mapping(payloads["blocked_promotion_rationale"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2355_STATUS:
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"TRADING-2356 requires 2355 status {EXPECTED_2355_STATUS}"
        )
    if package.get("status") != EXPECTED_2355_STATUS:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 package status"
        )
    if summary.get("readiness") != EXPECTED_2355_READINESS:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 readiness READY_FOR_2356_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2355_NEXT_ROUTE:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 summary route to kill-switch plan"
        )
    if route.get("next_route") != EXPECTED_2355_NEXT_ROUTE:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 route to kill-switch plan"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 source contract status PASS"
        )
    if summary.get("source_tasks") != SOURCE_2355_TASKS:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 source tasks to be 2347..2354"
        )
    if package.get("source_tasks") != SOURCE_2355_TASKS:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 package source tasks to be 2347..2354"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 evidence_chain_complete=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"TRADING-2356 requires owner_decision={OWNER_DECISION}"
        )
    if summary.get("hardening_backlog_ready") is not True:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 hardening_backlog_ready=true"
        )
    if summary.get("evidence_matrix_ready") is not True:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 evidence_matrix_ready=true"
        )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 promotion_decision=BLOCKED"
        )
    if rationale.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 blocked rationale promotion_allowed=false"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 promotion_allowed=false"
        )
    backlog_items = hardening_backlog.get("hardening_backlog", [])
    categories = {mapping(item).get("category") for item in backlog_items}
    required_categories = {
        "scheduler_enablement_guardrail",
        "scheduler_idempotency",
        "event_append_guardrail",
        "outcome_binding_guardrail",
        "paper_shadow_guardrail",
    }
    if not required_categories.issubset(categories):
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 hardening backlog categories"
        )
    for item in backlog_items:
        row = mapping(item)
        if row.get("side_effect_allowed") is not False:
            raise HighIntensitySchedulerKillSwitchPlanError(
                "TRADING-2356 requires 2355 backlog side_effect_allowed=false"
            )
        if row.get("promotion_allowed_after_task") is not False:
            raise HighIntensitySchedulerKillSwitchPlanError(
                "TRADING-2356 requires 2355 backlog promotion_allowed_after_task=false"
            )
    matrix = mapping(evidence_matrix.get("evidence_matrix"))
    if "scheduler_kill_switch" not in matrix:
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 evidence matrix scheduler_kill_switch"
        )
    for item in matrix.values():
        if mapping(item).get("side_effect_allowed") is not False:
            raise HighIntensitySchedulerKillSwitchPlanError(
                "TRADING-2356 requires 2355 evidence matrix side_effect_allowed=false"
            )
    _validate_source_data_quality(summary, "TRADING-2355 summary")
    _validate_guardrail_summary(mapping(summary.get("guardrail_summary")))
    _validate_side_effect_summary(mapping(summary.get("side_effect_summary")))


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    hardening_payloads: Mapping[str, Any],
) -> None:
    gap_summary = mapping(mapping(source_inputs["gap_closure"])["summary"])
    hardening_source_review = mapping(hardening_payloads["source_review"])
    if hardening_source_review.get("gap_closure_status") != gap_summary.get("status"):
        raise HighIntensitySchedulerKillSwitchPlanError(
            "TRADING-2356 requires 2355 source review gap closure status to match 2354"
        )


def _validate_guardrail_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in GUARDRAIL_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerKillSwitchPlanError(
                f"TRADING-2356 requires 2355 guardrail {field}={expected}"
            )


def _validate_side_effect_summary(payload: Mapping[str, Any]) -> None:
    for field, expected in SIDE_EFFECT_SUMMARY.items():
        if payload.get(field) is not expected:
            raise HighIntensitySchedulerKillSwitchPlanError(
                f"TRADING-2356 requires 2355 side effect {field}=false"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2356 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerKillSwitchPlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerKillSwitchPlanError(
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
        raise HighIntensitySchedulerKillSwitchPlanError(
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
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerKillSwitchPlanError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerKillSwitchPlanError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerKillSwitchPlanError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerKillSwitchPlanError(
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
