from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    DEFAULT_CONTINUE_DECISION_ROOT,
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_RUNTIME_INTEGRATION_PLAN_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
    HighIntensityRuntimeDryRunError,
    load_trading_2335_runtime_dry_run_context,
    load_trading_2336_runtime_dry_run_context,
    load_trading_2341_runtime_dry_run_context,
    load_trading_2342_runtime_dry_run_context,
)
from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_RUNTIME_DRY_RUN_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    records,
    write_json,
    write_markdown,
)

TASK_ID = (
    "TRADING-2344_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_RUNTIME_"
    "SCHEDULER_INTEGRATION_PLAN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_runtime_scheduler_integration_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_integration_plan"
READY_STATUS = (
    "OBSERVE_ONLY_SCHEDULER_INTEGRATION_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)

EXPECTED_2343_STATUS = (
    "OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)
READY_2344_STATUSES = {
    "READY_FOR_2344_OBSERVE_ONLY_RUNTIME_SCHEDULER_INTEGRATION_PLAN",
    "READY_FOR_2344_WITH_CAVEATS",
}
EXPECTED_2344_NEXT_TASK = (
    "TRADING-2344_High_Intensity_Risk_Cap_Observe_Only_Runtime_"
    "Scheduler_Integration_Plan"
)
EXPECTED_SELECTED_RULE = "COMPOSITE_HIGH_INTENSITY_RULE"
KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
PIT_POLICY = "PIT_APPROXIMATION_READY"

NEXT_2345_SCHEDULER_DRY_RUN_TASK = (
    "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
)
NEXT_2345_PLAN_REMEDIATION_TASK = (
    "TRADING-2345_High_Intensity_Risk_Cap_Scheduler_Plan_Remediation"
)
NEXT_2345_SAFETY_REMEDIATION_TASK = (
    "TRADING-2345_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
)
NEXT_2345_ARCHIVE_TASK = "TRADING-2345_Archive_High_Intensity_Risk_Cap_Runtime_Line"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

HORIZONS = ["1d", "5d", "10d", "20d"]
ROUTE_CAVEATS = [
    "OBSERVE_ONLY",
    "SCHEDULER_DISABLED_BY_DEFAULT",
    "PARTIAL_COVERAGE_CAVEAT",
    "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "scheduler_integration_plan_only": True,
    "observe_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "scheduler_activation_allowed_in_2344": False,
    "event_append_executed": False,
    "outcome_binding_executed": False,
    "automatic_exposure_cap_allowed": False,
    "target_weight_action_allowed": False,
    "rebalance_instruction_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

INPUT_SAFETY_FALSE_FIELDS = {
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "runtime_scheduler_enabled",
    "scheduler_enabled",
    "scheduler_default_enabled",
    "scheduler_activation_allowed_in_2344",
    "runtime_observe_started",
    "target_weight_action_allowed",
    "rebalance_instruction_allowed",
    "target_weight_generated",
    "target_weight_action_generated",
    "rebalance_instruction_generated",
    "broker_order_generated",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "paper_shadow_ready",
    "production_ready",
    "broker_ready",
    "broker_action_requested",
    "paper_shadow_enabled",
    "production_enabled",
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


class HighIntensitySchedulerIntegrationPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_runtime_scheduler_integration_plan(
    *,
    runtime_dry_run_dir: Path = DEFAULT_RUNTIME_DRY_RUN_ROOT,
    runtime_integration_plan_dir: Path = DEFAULT_RUNTIME_INTEGRATION_PLAN_ROOT,
    continue_decision_dir: Path = DEFAULT_CONTINUE_DECISION_ROOT,
    event_logger_dir: Path = DEFAULT_EVENT_LOGGER_ROOT,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerIntegrationPlanError(
            f"high-intensity scheduler integration plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_integration_plan_inputs(
        runtime_dry_run_dir=runtime_dry_run_dir,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
    )
    scope_contract = build_high_intensity_scheduler_scope_contract(inputs)
    cadence_plan = build_high_intensity_scheduler_cadence_plan()
    input_contract = build_high_intensity_scheduler_input_contract()
    event_detection_contract = (
        build_high_intensity_scheduler_event_detection_job_contract(inputs)
    )
    event_append_contract = build_high_intensity_scheduler_event_append_job_contract()
    cluster_update_contract = (
        build_high_intensity_scheduler_cluster_update_job_contract()
    )
    pending_outcome_contract = (
        build_high_intensity_scheduler_pending_outcome_update_job_contract()
    )
    outcome_update_contract = (
        build_high_intensity_scheduler_outcome_update_job_contract()
    )
    manual_review_contract = (
        build_high_intensity_scheduler_manual_review_context_contract()
    )
    monthly_contract = (
        build_high_intensity_scheduler_monthly_concentration_monitoring_contract(
            inputs
        )
    )
    artifact_path_plan = build_high_intensity_scheduler_artifact_path_plan()
    registry_update_plan = build_high_intensity_scheduler_registry_update_plan()
    disabled_policy = build_high_intensity_scheduler_disabled_by_default_policy()
    dry_run_execution_plan = build_high_intensity_scheduler_dry_run_execution_plan()
    failure_mode_matrix = build_high_intensity_scheduler_failure_mode_matrix()
    risk_register = build_high_intensity_scheduler_integration_risk_register()
    fail_closed_gate = build_high_intensity_scheduler_fail_closed_safety_gate(
        {
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
            "event_append_executed": False,
            "outcome_binding_executed": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )
    readiness = build_high_intensity_2345_readiness_checklist(
        scope_contract=scope_contract,
        cadence_plan=cadence_plan,
        input_contract=input_contract,
        event_detection_contract=event_detection_contract,
        event_append_contract=event_append_contract,
        cluster_update_contract=cluster_update_contract,
        pending_outcome_contract=pending_outcome_contract,
        outcome_update_contract=outcome_update_contract,
        manual_review_contract=manual_review_contract,
        monthly_contract=monthly_contract,
        artifact_path_plan=artifact_path_plan,
        disabled_policy=disabled_policy,
        dry_run_execution_plan=dry_run_execution_plan,
        failure_mode_matrix=failure_mode_matrix,
        fail_closed_gate=fail_closed_gate,
        inputs=inputs,
    )
    task_route = build_high_intensity_2345_task_route(readiness)
    interpretation_boundary = (
        build_high_intensity_scheduler_integration_interpretation_boundary(
            generated_at=generated_at,
            inputs=inputs,
        )
    )
    safety_boundary = build_high_intensity_scheduler_integration_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_scheduler_integration_plan_summary(
        generated_at=generated_at,
        runtime_dry_run_dir=runtime_dry_run_dir,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        inputs=inputs,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_scheduler_integration_plan_outputs(
        paths=paths,
        summary=summary,
        scope_contract=scope_contract,
        cadence_plan=cadence_plan,
        input_contract=input_contract,
        event_detection_contract=event_detection_contract,
        event_append_contract=event_append_contract,
        cluster_update_contract=cluster_update_contract,
        pending_outcome_contract=pending_outcome_contract,
        outcome_update_contract=outcome_update_contract,
        manual_review_contract=manual_review_contract,
        monthly_contract=monthly_contract,
        artifact_path_plan=artifact_path_plan,
        registry_update_plan=registry_update_plan,
        fail_closed_gate=fail_closed_gate,
        disabled_policy=disabled_policy,
        dry_run_execution_plan=dry_run_execution_plan,
        failure_mode_matrix=failure_mode_matrix,
        risk_register=risk_register,
        readiness=readiness,
        task_route=task_route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_integration_plan_inputs(
    *,
    runtime_dry_run_dir: Path,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    try:
        inputs = {
            "runtime_dry_run": load_trading_2343_scheduler_plan_context(
                runtime_dry_run_dir
            ),
            "runtime_plan": load_trading_2342_runtime_dry_run_context(
                runtime_integration_plan_dir
            ),
            "continue_decision": load_trading_2341_runtime_dry_run_context(
                continue_decision_dir
            ),
            "event_logger": load_trading_2336_runtime_dry_run_context(
                event_logger_dir
            ),
            "threshold_selection": load_trading_2335_runtime_dry_run_context(
                threshold_selection_dir
            ),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
        }
    except HighIntensityRuntimeDryRunError as exc:
        raise HighIntensitySchedulerIntegrationPlanError(str(exc)) from exc
    _validate_scheduler_cross_source_contracts(inputs)
    return inputs


def load_trading_2343_scheduler_plan_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_runtime_dry_run_summary.json",
        "contract_validation": root
        / "high_intensity_runtime_contract_validation_report.json",
        "input_validation": root
        / "high_intensity_runtime_input_validation_matrix.json",
        "event_detection": root
        / "high_intensity_runtime_event_detection_dry_run_result.json",
        "event_append": root
        / "high_intensity_runtime_event_append_dry_run_result.json",
        "cluster_update": root
        / "high_intensity_runtime_cluster_update_dry_run_result.json",
        "pending_outcome_update": root
        / "high_intensity_runtime_pending_outcome_update_dry_run_result.json",
        "manual_review_context": root
        / "high_intensity_runtime_manual_review_context_dry_run_result.json",
        "monthly_monitoring": root
        / "high_intensity_runtime_monthly_concentration_monitoring_dry_run.json",
        "safety_gate": root / "high_intensity_runtime_fail_closed_safety_gate_result.json",
        "artifact_registry": root
        / "high_intensity_runtime_artifact_registry_dry_run_report.json",
        "data_quality": root / "high_intensity_runtime_dry_run_data_quality_report.json",
        "interpretation_boundary": root
        / "high_intensity_runtime_dry_run_interpretation_boundary.json",
        "readiness": root / "high_intensity_2344_readiness_checklist.json",
        "task_route": root / "high_intensity_2344_task_route.json",
        "safety_boundary": root / "high_intensity_runtime_dry_run_safety_boundary.json",
    }
    csv_paths = [
        root / "high_intensity_runtime_input_validation_matrix.csv",
        root / "high_intensity_runtime_event_detection_dry_run_result.csv",
        root / "high_intensity_runtime_event_append_dry_run_result.csv",
        root / "high_intensity_runtime_cluster_update_dry_run_result.csv",
        root / "high_intensity_runtime_pending_outcome_update_dry_run_result.csv",
        root / "high_intensity_runtime_manual_review_context_dry_run_result.csv",
    ]
    payloads = _load_required_payloads(paths, "TRADING-2343 runtime dry-run")
    _require_existing_files(csv_paths, "TRADING-2343 runtime dry-run CSV")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2343 {key}", payload)

    summary = mapping(payloads["summary"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    safety_gate = mapping(payloads["safety_gate"])
    contract_validation = mapping(payloads["contract_validation"])
    if summary.get("status") != EXPECTED_2343_STATUS:
        raise HighIntensitySchedulerIntegrationPlanError(
            f"TRADING-2344 requires 2343 status {EXPECTED_2343_STATUS}"
        )
    if summary.get("next_task") != EXPECTED_2344_NEXT_TASK:
        raise HighIntensitySchedulerIntegrationPlanError(
            f"TRADING-2344 requires 2343 next task {EXPECTED_2344_NEXT_TASK}"
        )
    readiness_status = readiness.get("readiness_status") or summary.get(
        "2344_readiness_status"
    )
    if readiness_status not in READY_2344_STATUSES:
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires 2343 readiness for scheduler integration plan"
        )
    if task_route.get("next_task") != EXPECTED_2344_NEXT_TASK:
        raise HighIntensitySchedulerIntegrationPlanError(
            f"TRADING-2344 requires 2343 task route {EXPECTED_2344_NEXT_TASK}"
        )
    if safety_gate.get("safety_gate_status") != "PASS":
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires 2343 fail-closed safety gate PASS"
        )
    if contract_validation.get("contract_validation_status") not in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }:
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires 2343 contract validation PASS or PASS_WITH_WARNINGS"
        )
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def build_high_intensity_scheduler_scope_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])
    return clean_for_yaml(
        {
            "scope_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_SCOPE_V1",
            "scope_version": "v1",
            "observe_line": "high_intensity_risk_cap",
            "scheduler_integration_plan_only": True,
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
            "observe_only": True,
            "manual_review_context_allowed": True,
            "automatic_exposure_cap_allowed": False,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_type": EXPECTED_SELECTED_RULE,
            "partial_coverage_caveat_required": True,
            "monthly_concentration_monitoring_required": True,
            "promotion_allowed": False,
        }
    )


def build_high_intensity_scheduler_cadence_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "cadence_plan_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_CADENCE_V1",
            "scheduler_enabled_in_2344": False,
            "candidate_cadence": {
                "event_detection": {
                    "frequency": "trading_day",
                    "preferred_time": "after_daily_signal_generation",
                    "market_calendar_required": True,
                },
                "event_append": {
                    "frequency": "trading_day",
                    "preferred_time": "after_event_detection",
                },
                "pending_outcome_update": {
                    "frequency": "trading_day",
                    "preferred_time": "after_event_append",
                },
                "outcome_update": {
                    "frequency": "trading_day",
                    "preferred_time": "after_market_data_refresh",
                },
                "monthly_concentration_monitoring": {
                    "frequency": "trading_day",
                    "preferred_time": "after_event_append",
                },
            },
            "calendar_policy": {
                "use_trading_calendar": True,
                "skip_non_trading_days": True,
                "next_session_policy": True,
            },
            "timezone_policy": {
                "default": "America/New_York",
                "reporting_timezone": "Asia/Tokyo",
            },
            "manual_review_context_refresh": {"frequency": "trading_day"},
            "scheduler_activation_allowed_in_2344": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_input_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_INPUT_V1",
            "contract_version": "v1",
            "required_scheduler_inputs": [
                "selected_trigger_rule",
                "risk_cap_trigger_series",
                "target_asset",
                "as_of_timestamp",
                "decision_timestamp",
                "known_at_policy",
                "pit_policy",
                "trading_calendar",
                "prior_event_log",
                "prior_cluster_registry",
                "prior_pending_outcome_registry",
                "monthly_concentration_state",
            ],
            "optional_scheduler_inputs": [
                "latest_market_data_for_outcome_update",
                "manual_review_note",
            ],
            "blocked_inputs": [
                "broker_position",
                "real_portfolio_weight",
                "live_order_state",
                "broker_order_history",
            ],
            "input_validation_required": True,
            "fail_closed_if_missing_required_input": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_event_detection_job_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    summary = mapping(inputs["runtime_dry_run"]["summary"])
    return clean_for_yaml(
        {
            "job_id": "HIGH_INTENSITY_OBSERVE_ONLY_EVENT_DETECTION_JOB_V1",
            "job_type": "observe_only_event_detection",
            "scheduler_enabled_in_2344": False,
            "selected_rule_id": summary.get("selected_rule_id"),
            "selected_rule_hash": summary.get("selected_rule_hash"),
            "required_fields": [
                "risk_cap_triggered",
                "risk_cap_intensity",
                "risk_cap_score",
                "scope_active",
                "signal_direction",
                "as_of_timestamp",
                "decision_timestamp",
                "known_at_policy",
                "pit_policy",
            ],
            "job_output": [
                "detection_result",
                "high_intensity_triggered",
                "high_intensity_reason",
            ],
            "blocked_output": [
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "broker_action",
            ],
            "fail_closed_required": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_event_append_job_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "job_id": "HIGH_INTENSITY_OBSERVE_ONLY_EVENT_APPEND_JOB_V1",
            "job_type": "observe_only_event_append",
            "scheduler_enabled_in_2344": False,
            "append_mode": "append_only",
            "dedup_required": True,
            "deterministic_event_id_required": True,
            "original_event_log_mutation_allowed": False,
            "prior_event_log_required": True,
            "prior_cluster_registry_required": True,
            "event_status_on_create": "OBSERVE_PENDING",
            "manual_review_observation_flag_on_create": True,
            "job_output": [
                "observe_event_log_increment",
                "cluster_registry_increment",
                "manual_review_context_queue_increment",
            ],
            "blocked_output": [
                "target_weight",
                "rebalance_instruction",
                "reduce_position_instruction",
                "broker_action",
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_cluster_update_job_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "job_id": "HIGH_INTENSITY_OBSERVE_ONLY_CLUSTER_UPDATE_JOB_V1",
            "job_type": "observe_only_cluster_update",
            "scheduler_enabled_in_2344": False,
            "cluster_update_mode": "append_or_extend_open_cluster",
            "cluster_id_deterministic": True,
            "open_cluster_policy": {
                "extend_if_same_asset_rule_and_consecutive_trigger": True,
                "create_new_if_gap_detected": True,
            },
            "monthly_concentration_tracking_required": True,
            "job_output": [
                "cluster_registry_increment",
                "monthly_cluster_state_update",
            ],
            "blocked_output": [
                "target_weight",
                "rebalance_instruction",
                "broker_action",
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_pending_outcome_update_job_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "job_id": "HIGH_INTENSITY_OBSERVE_ONLY_PENDING_OUTCOME_UPDATE_JOB_V1",
            "job_type": "observe_only_pending_outcome_registry_update",
            "scheduler_enabled_in_2344": False,
            "pending_registry_update_mode": "append_only",
            "new_event_generates_pending_outcomes": True,
            "horizons": HORIZONS,
            "outcome_status_on_create": "OUTCOME_PENDING",
            "outcome_binding_allowed_in_event_append_job": False,
            "job_output": ["pending_outcome_registry_increment"],
            "blocked_output": [
                "actual_path_outcome_value",
                "target_weight",
                "rebalance_instruction",
                "broker_action",
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_outcome_update_job_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "job_id": "HIGH_INTENSITY_OBSERVE_ONLY_OUTCOME_UPDATE_JOB_V1",
            "job_type": "observe_only_actual_path_outcome_update",
            "scheduler_enabled_in_2344": False,
            "future_scheduler_candidate": True,
            "requires_market_data": True,
            "requires_validate_data": True,
            "validate_data_policy": {
                "canonical_validate_data_required": True,
                "no_rule_relaxation": True,
                "as_of_policy": "latest_price_cache_or_explicit_as_of",
            },
            "input": [
                "pending_outcome_registry",
                "market_data_cache",
                "trading_calendar",
            ],
            "output": [
                "outcome_registry_update",
                "event_actual_path_outcome_increment",
                "cluster_actual_path_outcome_increment",
                "outcome_quality_report",
            ],
            "original_event_log_mutation_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_manual_review_context_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_OBSERVE_ONLY_MANUAL_REVIEW_CONTEXT_V1",
            "scheduler_enabled_in_2344": False,
            "manual_review_context_allowed": True,
            "display_mode": "risk_warning_context_only",
            "allowed_display_fields": [
                "event_id",
                "event_date",
                "target_asset",
                "high_intensity_reason",
                "selected_rule_id",
                "risk_cap_intensity",
                "risk_cap_score",
                "cluster_context",
                "partial_coverage_caveat",
                "monthly_concentration_warning",
                "manual_review_reason",
            ],
            "blocked_display_fields": [
                "target_weight",
                "rebalance_instruction",
                "reduce_position_instruction",
                "increase_cash_instruction",
                "buy_signal",
                "sell_signal",
                "broker_action",
            ],
            "display_label_options": [
                "HIGH_INTENSITY_RISK_WARNING_CONTEXT",
                "MANUAL_REVIEW_CONTEXT_ONLY",
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_monthly_concentration_monitoring_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    monthly_report = mapping(inputs["event_logger"]["monthly_report"])
    warnings = monthly_report.get("monthly_concentration_warnings") or [
        "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
    ]
    warning_items = _string_list(warnings)
    inherited_warning = (
        warning_items[0]
        if warning_items
        else "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
    )
    return clean_for_yaml(
        {
            "contract_id": (
                "HIGH_INTENSITY_OBSERVE_ONLY_MONTHLY_CONCENTRATION_MONITORING_V1"
            ),
            "scheduler_enabled_in_2344": False,
            "monitoring_required": True,
            "inherited_warning": inherited_warning,
            "runtime_metrics": [
                "monthly_trigger_day_count",
                "monthly_event_count",
                "monthly_cluster_count",
                "new_cluster_count",
                "existing_cluster_continuation_count",
                "max_consecutive_trigger_days",
                "false_warning_by_month",
                "downside_capture_by_month",
                "missed_upside_by_month",
            ],
            "guardrails": {
                "max_monthly_event_count": monthly_report.get(
                    "monthly_event_guardrail", 3
                ),
                "max_monthly_cluster_count": monthly_report.get(
                    "monthly_cluster_guardrail", 3
                ),
                "max_consecutive_trigger_days": monthly_report.get(
                    "max_consecutive_trigger_days_guardrail", 3
                ),
            },
            "guardrail_action": {
                "warning": "flag_for_review",
                "blocking": "pause_scheduler_integration_until_review",
            },
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_artifact_path_plan() -> dict[str, Any]:
    root = "outputs/research_trends/high_intensity_risk_cap_runtime_observe"
    return clean_for_yaml(
        {
            "plan_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_ARTIFACT_PATHS_V1",
            "runtime_artifact_root": {"proposed": f"{root}/"},
            "scheduler_artifact_paths": {
                "detection_result_increment": f"{root}/detection_result_increment.json",
                "event_log_increment": f"{root}/event_log_increment.json",
                "cluster_registry_increment": f"{root}/cluster_registry_increment.json",
                "pending_outcome_registry_increment": (
                    f"{root}/pending_outcome_registry_increment.json"
                ),
                "manual_review_context_increment": (
                    f"{root}/manual_review_context_increment.json"
                ),
                "monthly_concentration_monitoring_update": (
                    f"{root}/monthly_concentration_monitoring_update.json"
                ),
                "outcome_update_increment": f"{root}/outcome_update_increment.json",
                "scheduler_run_summary": f"{root}/scheduler_run_summary.json",
            },
            "git_policy": "outputs_ignored",
            "registry_required": True,
            "artifact_catalog_required": True,
            "system_flow_required": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_registry_update_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "plan_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_REGISTRY_UPDATE_V1",
            "report_registry_entry_required": True,
            "artifact_catalog_entry_required": True,
            "system_flow_update_required": True,
            "task_register_update_required": True,
            "runtime_artifacts_ignored_by_git": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_disabled_by_default_policy() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "policy_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_DISABLED_V1",
            "scheduler_default_enabled": False,
            "activation_requires_future_task": True,
            "activation_task_not_2344": True,
            "activation_requires_owner_review": True,
            "activation_requires_dry_run_pass": True,
            "activation_requires_safety_gate_pass": True,
            "activation_requires_no_paper_shadow": True,
            "activation_requires_no_production": True,
            "activation_requires_no_broker_action": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_dry_run_execution_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "dry_run_plan_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_DRY_RUN_V1",
            "next_task": NEXT_2345_SCHEDULER_DRY_RUN_TASK,
            "scheduler_enabled": False,
            "dry_run_mode": [
                "historical_replay_scheduler_cycle",
                "single_day_scheduler_cycle_fixture",
                "fail_closed_safety_fixture",
            ],
            "required_inputs": [
                "scheduler_cadence_plan",
                "scheduler_input_contract",
                "event_detection_job_contract",
                "event_append_job_contract",
                "cluster_update_job_contract",
                "pending_outcome_update_job_contract",
                "manual_review_context_contract",
                "fail_closed_safety_gate",
            ],
            "expected_outputs": [
                "scheduler_dry_run_summary",
                "detection_job_dry_run_result",
                "append_job_dry_run_result",
                "cluster_job_dry_run_result",
                "pending_outcome_job_dry_run_result",
                "manual_review_context_job_dry_run_result",
                "scheduler_safety_gate_result",
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    )


def build_high_intensity_scheduler_failure_mode_matrix() -> list[dict[str, Any]]:
    modes = [
        ("missing selected trigger rule", "selected_trigger_rule is missing"),
        ("missing risk-cap trigger series", "risk_cap_trigger_series is missing"),
        ("missing known-at timestamp", "as_of_timestamp is missing"),
        ("missing PIT policy", "pit_policy is missing"),
        ("missing prior event log", "prior_event_log is missing"),
        ("duplicate event id", "deterministic event id already exists"),
        ("cluster update conflict", "open cluster cannot be resolved deterministically"),
        (
            "monthly concentration blocking breach",
            "monthly guardrail status is blocking",
        ),
        (
            "pending outcome registry conflict",
            "pending outcome id already exists with incompatible payload",
        ),
        (
            "manual-review context forbidden field",
            "manual-review output contains trading action field",
        ),
        ("target weight generated", "target weight field is present"),
        ("rebalance instruction generated", "rebalance instruction field is present"),
        ("paper-shadow enabled", "paper_shadow_enabled is true"),
        ("production enabled", "production_enabled is true"),
        ("broker action requested", "broker_action != none"),
    ]
    return [
        clean_for_yaml(
            {
                "failure_mode": mode,
                "failure_detection_rule": rule,
                "fail_closed_action": "emit_scheduler_safety_failure_and_stop",
                "artifact_to_emit": "high_intensity_scheduler_fail_closed_safety_gate",
                "next_task_if_triggered": NEXT_2345_SAFETY_REMEDIATION_TASK,
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            }
        )
        for mode, rule in modes
    ]


def build_high_intensity_scheduler_integration_risk_register() -> list[dict[str, Any]]:
    risks = [
        ("scheduler_accidentally_enabled", "scheduler accidentally enabled", "critical"),
        (
            "manual_review_context_trade_signal_misread",
            "manual-review context misread as trade signal",
            "high",
        ),
        ("event_duplication_append_bug", "event duplication due to append bug", "high"),
        ("cluster_over_extension", "cluster over-extension", "medium"),
        ("outcome_update_lag", "outcome update lag", "medium"),
        ("monthly_concentration_overcount", "monthly concentration overcount", "medium"),
        ("data_validation_drift", "data validation drift", "high"),
        (
            "accidental_paper_shadow_promotion",
            "accidental paper-shadow promotion",
            "critical",
        ),
        ("accidental_broker_action_path", "accidental broker action path", "critical"),
    ]
    return [
        clean_for_yaml(
            {
                "risk_id": risk_id,
                "risk_name": name,
                "risk_description": name,
                "severity": severity,
                "mitigation": "fail_closed_contract_and_owner_review_required",
                "blocked_if_unmitigated": severity in {"critical", "high"},
                "owner_review_required": True,
            }
        )
        for risk_id, name, severity in risks
    ]


def build_high_intensity_scheduler_fail_closed_safety_gate(
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    blocked = {
        "scheduler_enabled_blocked": candidate.get("scheduler_enabled") is True,
        "scheduler_default_enabled_blocked": candidate.get("scheduler_default_enabled")
        is True,
        "target_weight_generated_blocked": bool(
            candidate.get("target_weight_generated")
            or candidate.get("target_weight")
            or candidate.get("target_weight_action")
        ),
        "rebalance_instruction_generated_blocked": bool(
            candidate.get("rebalance_instruction_generated")
            or candidate.get("rebalance_instruction")
        ),
        "broker_action_requested_blocked": (
            str(candidate.get("broker_action", "none")).lower() not in {"", "none"}
            or candidate.get("broker_action_requested") is True
        ),
        "paper_shadow_enabled_blocked": bool(
            candidate.get("paper_shadow_enabled")
            or candidate.get("paper_shadow_allowed")
        ),
        "production_enabled_blocked": bool(
            candidate.get("production_enabled") or candidate.get("production_allowed")
        ),
    }
    error_count = sum(1 for value in blocked.values() if value)
    return clean_for_yaml(
        {
            "safety_gate_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_FAIL_CLOSED_V1",
            "safety_gate_status": "FAIL" if error_count else "PASS",
            **blocked,
            "safety_error_count": error_count,
            "next_task_if_failed": NEXT_2345_SAFETY_REMEDIATION_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2345_readiness_checklist(
    *,
    scope_contract: Mapping[str, Any],
    cadence_plan: Mapping[str, Any],
    input_contract: Mapping[str, Any],
    event_detection_contract: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    cluster_update_contract: Mapping[str, Any],
    pending_outcome_contract: Mapping[str, Any],
    outcome_update_contract: Mapping[str, Any],
    manual_review_contract: Mapping[str, Any],
    monthly_contract: Mapping[str, Any],
    artifact_path_plan: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
    dry_run_execution_plan: Mapping[str, Any],
    failure_mode_matrix: Sequence[Mapping[str, Any]],
    fail_closed_gate: Mapping[str, Any],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings = list(_route_caveats_from_inputs(inputs))
    if scope_contract.get("scheduler_enabled") is not False:
        blockers.append("SCHEDULER_ENABLED_NOT_FALSE")
    if scope_contract.get("scheduler_default_enabled") is not False:
        blockers.append("SCHEDULER_DEFAULT_ENABLED_NOT_FALSE")
    if fail_closed_gate.get("safety_gate_status") != "PASS":
        blockers.append("SCHEDULER_SAFETY_GATE_NOT_PASS")
    required_generated = {
        "scheduler_scope_contract_generated": bool(scope_contract),
        "scheduler_cadence_plan_generated": bool(cadence_plan),
        "scheduler_input_contract_generated": bool(input_contract),
        "event_detection_job_contract_generated": bool(event_detection_contract),
        "event_append_job_contract_generated": bool(event_append_contract),
        "cluster_update_job_contract_generated": bool(cluster_update_contract),
        "pending_outcome_update_job_contract_generated": bool(pending_outcome_contract),
        "outcome_update_job_contract_generated": bool(outcome_update_contract),
        "manual_review_context_contract_generated": bool(manual_review_contract),
        "monthly_monitoring_contract_generated": bool(monthly_contract),
        "artifact_path_plan_generated": bool(artifact_path_plan),
        "disabled_by_default_policy_generated": bool(disabled_policy),
        "dry_run_execution_plan_generated": bool(dry_run_execution_plan),
        "failure_mode_matrix_generated": bool(failure_mode_matrix),
    }
    blockers.extend(
        key.upper()
        for key, generated in required_generated.items()
        if not generated
    )
    if blockers:
        status = "SCHEDULER_PLAN_BLOCKED"
    elif warnings:
        status = "READY_FOR_2345_WITH_CAVEATS"
    else:
        status = "READY_FOR_2345_OBSERVE_ONLY_SCHEDULER_DRY_RUN"
    return clean_for_yaml(
        {
            **required_generated,
            "safety_boundary_passed": fail_closed_gate.get("safety_gate_status")
            == "PASS",
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
            "event_append_executed": False,
            "outcome_binding_executed": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2345_task_route(
    readiness: Mapping[str, Any],
    *,
    safety_issue: bool = False,
    plan_remediation_needed: bool = False,
) -> dict[str, Any]:
    status = readiness.get("readiness_status")
    blockers = _string_list(readiness.get("readiness_blockers"))
    if safety_issue:
        next_task = NEXT_2345_SAFETY_REMEDIATION_TASK
        rationale = "scheduler safety issue requires remediation"
    elif plan_remediation_needed or status == "SCHEDULER_PLAN_REMEDIATION_REQUIRED":
        next_task = NEXT_2345_PLAN_REMEDIATION_TASK
        rationale = "scheduler plan remediation required"
    elif status in {
        "READY_FOR_2345_OBSERVE_ONLY_SCHEDULER_DRY_RUN",
        "READY_FOR_2345_WITH_CAVEATS",
    }:
        next_task = NEXT_2345_SCHEDULER_DRY_RUN_TASK
        rationale = "observe-only scheduler dry-run route allowed"
    elif blockers:
        next_task = NEXT_2345_SAFETY_REMEDIATION_TASK
        rationale = "scheduler plan blockers require safety remediation"
    else:
        next_task = NEXT_2345_ARCHIVE_TASK
        rationale = "scheduler line not ready and no remediation route selected"
    return clean_for_yaml(
        {
            "readiness_status": status,
            "next_task": next_task,
            "allowed_routes": [
                NEXT_2345_SCHEDULER_DRY_RUN_TASK,
                NEXT_2345_PLAN_REMEDIATION_TASK,
                NEXT_2345_SAFETY_REMEDIATION_TASK,
                NEXT_2345_ARCHIVE_TASK,
            ],
            "route_caveats": ROUTE_CAVEATS
            if status == "READY_FOR_2345_WITH_CAVEATS"
            else [],
            "route_blockers": blockers,
            "route_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_scheduler_integration_interpretation_boundary(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "generated_at": generated_at.isoformat(),
            "known_at_policy": KNOWN_AT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "partial_coverage_caveat_required": True,
            "monthly_concentration_monitoring_required": True,
            "source_2343_status": mapping(inputs["runtime_dry_run"]["summary"]).get(
                "status"
            ),
            "forbidden_interpretations": [
                "real_account_performance",
                "real_position_advice",
                "reduce_position_signal",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
                "scheduler_enabled",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_scheduler_integration_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task"),
            "forbidden_outputs": [
                "target_weight_action",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "reduce_position_instruction",
                "increase_cash_instruction",
                "paper_shadow_ready",
                "production_ready",
                "broker_action",
                "automatic_exposure_cap",
                "scheduler_enabled",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_scheduler_integration_plan_summary(
    *,
    generated_at: datetime,
    runtime_dry_run_dir: Path,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    inputs: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    runtime_summary = mapping(inputs["runtime_dry_run"]["summary"])
    data_quality = mapping(inputs["runtime_dry_run"]["data_quality"])
    event_summary = mapping(inputs["event_logger"]["summary"])
    return clean_for_yaml(
        {
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Observe-Only Runtime Scheduler Integration Plan",
            "status": READY_STATUS,
            "scheduler_integration_plan_status": READY_STATUS,
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "runtime_dry_run_dir": str(runtime_dry_run_dir),
            "runtime_integration_plan_dir": str(runtime_integration_plan_dir),
            "continue_decision_dir": str(continue_decision_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "source_2343_status": runtime_summary.get("status"),
            "source_2343_readiness": runtime_summary.get("2344_readiness_status"),
            "source_2343_next_task": runtime_summary.get("next_task"),
            "selected_rule_id": runtime_summary.get("selected_rule_id"),
            "selected_rule_hash": runtime_summary.get("selected_rule_hash"),
            "record_count": runtime_summary.get("record_count"),
            "detected_event_count": runtime_summary.get("detected_event_count"),
            "would_append_event_count": runtime_summary.get("would_append_event_count"),
            "event_logger_trigger_day_count": event_summary.get("trigger_day_count"),
            "event_logger_cluster_count": event_summary.get("cluster_count"),
            "2345_readiness_status": readiness.get("readiness_status"),
            "next_task": task_route.get("next_task"),
            "route_caveats": task_route.get("route_caveats"),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2344 only reads "
                "prior validated TRADING-2343 / 2342 artifacts and does not "
                "consume market data directly."
            ),
            "source_validate_data_executed": data_quality.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": data_quality.get("source_validate_data_as_of"),
            "source_validate_data_status": data_quality.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": data_quality.get(
                "source_validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_scheduler_integration_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    scope_contract: Mapping[str, Any],
    cadence_plan: Mapping[str, Any],
    input_contract: Mapping[str, Any],
    event_detection_contract: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    cluster_update_contract: Mapping[str, Any],
    pending_outcome_contract: Mapping[str, Any],
    outcome_update_contract: Mapping[str, Any],
    manual_review_contract: Mapping[str, Any],
    monthly_contract: Mapping[str, Any],
    artifact_path_plan: Mapping[str, Any],
    registry_update_plan: Mapping[str, Any],
    fail_closed_gate: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
    dry_run_execution_plan: Mapping[str, Any],
    failure_mode_matrix: Sequence[Mapping[str, Any]],
    risk_register: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["scope_contract"], scope_contract)
    write_json(paths["cadence_plan"], cadence_plan)
    write_json(paths["input_contract"], input_contract)
    write_json(paths["event_detection_contract"], event_detection_contract)
    write_json(paths["event_append_contract"], event_append_contract)
    write_json(paths["cluster_update_contract"], cluster_update_contract)
    write_json(paths["pending_outcome_contract"], pending_outcome_contract)
    write_json(paths["outcome_update_contract"], outcome_update_contract)
    write_json(paths["manual_review_contract"], manual_review_contract)
    write_json(paths["monthly_contract"], monthly_contract)
    write_json(paths["artifact_path_plan"], artifact_path_plan)
    write_json(paths["registry_update_plan"], registry_update_plan)
    write_json(paths["fail_closed_gate"], fail_closed_gate)
    write_json(paths["disabled_policy"], disabled_policy)
    write_json(paths["dry_run_execution_plan"], dry_run_execution_plan)
    write_json(paths["failure_mode_matrix"], {"rows": list(failure_mode_matrix)})
    write_json(paths["risk_register"], {"rows": list(risk_register)})
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], _render_main_doc(summary, scope_contract))
    write_markdown(paths["cadence_doc"], _render_cadence_doc(cadence_plan, input_contract))
    write_markdown(
        paths["event_jobs_doc"],
        _render_event_jobs_doc(
            event_append_contract,
            cluster_update_contract,
            pending_outcome_contract,
            outcome_update_contract,
        ),
    )
    write_markdown(paths["safety_doc"], _render_safety_doc(fail_closed_gate))
    write_markdown(paths["route_doc"], _render_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_integration_plan_summary.json",
        "scope_contract": output_dir
        / "high_intensity_scheduler_scope_contract.json",
        "cadence_plan": output_dir / "high_intensity_scheduler_cadence_plan.json",
        "input_contract": output_dir / "high_intensity_scheduler_input_contract.json",
        "event_detection_contract": output_dir
        / "high_intensity_scheduler_event_detection_job_contract.json",
        "event_append_contract": output_dir
        / "high_intensity_scheduler_event_append_job_contract.json",
        "cluster_update_contract": output_dir
        / "high_intensity_scheduler_cluster_update_job_contract.json",
        "pending_outcome_contract": output_dir
        / "high_intensity_scheduler_pending_outcome_update_job_contract.json",
        "outcome_update_contract": output_dir
        / "high_intensity_scheduler_outcome_update_job_contract.json",
        "manual_review_contract": output_dir
        / "high_intensity_scheduler_manual_review_context_contract.json",
        "monthly_contract": output_dir
        / "high_intensity_scheduler_monthly_concentration_monitoring_contract.json",
        "artifact_path_plan": output_dir
        / "high_intensity_scheduler_artifact_path_plan.json",
        "registry_update_plan": output_dir
        / "high_intensity_scheduler_registry_update_plan.json",
        "fail_closed_gate": output_dir
        / "high_intensity_scheduler_fail_closed_safety_gate.json",
        "disabled_policy": output_dir
        / "high_intensity_scheduler_disabled_by_default_policy.json",
        "dry_run_execution_plan": output_dir
        / "high_intensity_scheduler_dry_run_execution_plan.json",
        "failure_mode_matrix": output_dir
        / "high_intensity_scheduler_failure_mode_matrix.json",
        "risk_register": output_dir
        / "high_intensity_scheduler_integration_risk_register.json",
        "readiness": output_dir / "high_intensity_2345_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2345_task_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_integration_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_integration_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_observe_only_runtime_scheduler_integration_plan.md",
        "cadence_doc": docs_root
        / "high_intensity_scheduler_cadence_and_input_contract.md",
        "event_jobs_doc": docs_root
        / "high_intensity_scheduler_event_append_and_outcome_jobs.md",
        "safety_doc": docs_root / "high_intensity_scheduler_fail_closed_safety_gate.md",
        "route_doc": docs_root / "high_intensity_2345_readiness_route.md",
    }


def _validate_scheduler_cross_source_contracts(inputs: Mapping[str, Any]) -> None:
    runtime_summary = mapping(inputs["runtime_dry_run"]["summary"])
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])
    event_logger_summary = mapping(inputs["event_logger"]["summary"])
    if runtime_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires 2343 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if selected_rule.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires 2335 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if runtime_summary.get("selected_rule_id") != selected_rule.get("selected_rule_id"):
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 selected rule mismatch between 2343 and 2335"
        )
    if not records(mapping(inputs["event_logger"]["event_log"]).get("rows")):
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires existing 2336 observe event log"
        )
    if not records(mapping(inputs["event_logger"]["cluster_registry"]).get("rows")):
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires existing 2336 cluster registry"
        )
    if not records(mapping(inputs["event_logger"]["pending_outcome_registry"]).get("rows")):
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires existing 2336 pending outcome registry"
        )
    if int(event_logger_summary.get("trigger_day_count", 0) or 0) <= 0:
        raise HighIntensitySchedulerIntegrationPlanError(
            "TRADING-2344 requires historical trigger-day lineage"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerIntegrationPlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _require_existing_files(paths: Sequence[Path], label: str) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise HighIntensitySchedulerIntegrationPlanError(
            f"{label} missing required files: {missing}"
        )


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerIntegrationPlanError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerIntegrationPlanError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _collect_unsafe_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in INPUT_SAFETY_FALSE_FIELDS and item is True:
                violations.append(path)
            if key_text == "broker_action" and str(item).lower() not in {"", "none"}:
                violations.append(path)
            if key_text in FORBIDDEN_EMIT_FIELDS and item not in {
                None,
                "",
                False,
                "none",
            }:
                violations.append(path)
            violations.extend(_collect_unsafe_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_unsafe_fields(item, f"{prefix}[{index}]"))
    return violations


def _route_caveats_from_inputs(inputs: Mapping[str, Any]) -> list[str]:
    route_caveats = _string_list(
        mapping(inputs["runtime_dry_run"]["task_route"]).get("route_caveats")
    )
    readiness_warnings = _string_list(
        mapping(inputs["runtime_dry_run"]["readiness"]).get("readiness_warnings")
    )
    selected = [*route_caveats, *readiness_warnings]
    inherited = [
        "OBSERVE_ONLY",
        "SCHEDULER_DISABLED_BY_DEFAULT",
        "PARTIAL_COVERAGE_CAVEAT",
        "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
        "NO_PAPER_SHADOW",
        "NO_PRODUCTION",
    ]
    seen: set[str] = set()
    result: list[str] = []
    for caveat in [*selected, *inherited]:
        if caveat and caveat not in seen:
            seen.add(caveat)
            result.append(caveat)
    return result


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value is None or value == "":
        return []
    return [str(value)]


def _render_main_doc(
    summary: Mapping[str, Any],
    scope_contract: Mapping[str, Any],
) -> str:
    boundary_sentence = (
        "本报告只生成 observe-only scheduler integration plan；不启用 scheduler，"
        "不写每日自动任务，不生成或 append event，不绑定 outcome，"
        "不输出 target weight / rebalance / broker action。"
    )
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Runtime Scheduler Integration Plan",
            "",
            f"- status: `{summary.get('status')}`",
            f"- selected_rule_id: `{summary.get('selected_rule_id')}`",
            f"- scheduler_enabled: `{scope_contract.get('scheduler_enabled')}`",
            f"- scheduler_default_enabled: `{scope_contract.get('scheduler_default_enabled')}`",
            f"- next_task: `{summary.get('next_task')}`",
            "",
            boundary_sentence,
        ]
    )


def _render_cadence_doc(
    cadence_plan: Mapping[str, Any],
    input_contract: Mapping[str, Any],
) -> str:
    cadence = mapping(cadence_plan.get("candidate_cadence"))
    event_frequency = mapping(cadence.get("event_detection")).get("frequency")
    outcome_frequency = mapping(cadence.get("outcome_update")).get("frequency")
    required_input_count = len(
        _string_list(input_contract.get("required_scheduler_inputs"))
    )
    boundary_sentence = (
        "Cadence plan 只定义 future observe-only scheduler 的候选运行顺序，"
        "并要求 trading-calendar gating；2344 不激活该 scheduler。"
    )
    return "\n".join(
        [
            "# High-Intensity Scheduler Cadence And Input Contract",
            "",
            f"- scheduler_enabled_in_2344: `{cadence_plan.get('scheduler_enabled_in_2344')}`",
            f"- event_detection_frequency: `{event_frequency}`",
            f"- outcome_update_frequency: `{outcome_frequency}`",
            f"- required_input_count: `{required_input_count}`",
            "",
            boundary_sentence,
        ]
    )


def _render_event_jobs_doc(
    event_append_contract: Mapping[str, Any],
    cluster_update_contract: Mapping[str, Any],
    pending_outcome_contract: Mapping[str, Any],
    outcome_update_contract: Mapping[str, Any],
) -> str:
    pending_mode = pending_outcome_contract.get("pending_registry_update_mode")
    outcome_requires_validation = outcome_update_contract.get("requires_validate_data")
    boundary_sentence = (
        "Event append / cluster / pending outcome jobs 只定义 future observe-only "
        "increments；actual-path outcome update job 只是 future candidate，"
        "启用时必须先运行 canonical validate-data。"
    )
    return "\n".join(
        [
            "# High-Intensity Scheduler Event Append And Outcome Jobs",
            "",
            f"- append_mode: `{event_append_contract.get('append_mode')}`",
            f"- cluster_update_mode: `{cluster_update_contract.get('cluster_update_mode')}`",
            f"- pending_registry_update_mode: `{pending_mode}`",
            f"- outcome_update_requires_validate_data: `{outcome_requires_validation}`",
            "",
            boundary_sentence,
        ]
    )


def _render_safety_doc(fail_closed_gate: Mapping[str, Any]) -> str:
    target_blocked = fail_closed_gate.get("target_weight_generated_blocked")
    rebalance_blocked = fail_closed_gate.get("rebalance_instruction_generated_blocked")
    broker_blocked = fail_closed_gate.get("broker_action_requested_blocked")
    boundary_sentence = (
        "Safety gate 对 scheduler enabled、target weight、rebalance、"
        "paper-shadow、production 和 broker action 全部 fail closed。"
    )
    return "\n".join(
        [
            "# High-Intensity Scheduler Fail-Closed Safety Gate",
            "",
            f"- safety_gate_status: `{fail_closed_gate.get('safety_gate_status')}`",
            f"- scheduler_enabled_blocked: `{fail_closed_gate.get('scheduler_enabled_blocked')}`",
            f"- target_weight_generated_blocked: `{target_blocked}`",
            f"- rebalance_instruction_generated_blocked: `{rebalance_blocked}`",
            f"- broker_action_requested_blocked: `{broker_blocked}`",
            "",
            boundary_sentence,
        ]
    )


def _render_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    boundary_sentence = (
        "2345 route 只允许 observe-only scheduler dry-run 或 remediation/archive；"
        "它不是 scheduler enabled、paper-shadow、production 或 broker readiness。"
    )
    return "\n".join(
        [
            "# High-Intensity 2345 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            f"- scheduler_enabled: `{readiness.get('scheduler_enabled')}`",
            f"- broker_action: `{task_route.get('broker_action')}`",
            "",
            boundary_sentence,
        ]
    )
