from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_FORWARD_OUTCOME_REVIEW_ROOT,
    DEFAULT_OUTCOME_BINDER_ROOT,
    DEFAULT_PARTIAL_READINESS_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
    HighIntensityContinueObserveDecisionError,
    load_trading_2334_continue_observe_context,
    load_trading_2335_continue_observe_context,
    load_trading_2337_continue_observe_context,
    load_trading_2339_continue_observe_context,
    load_trading_2340_forward_outcome_review_outputs,
)
from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CONTINUE_DECISION_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2342_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_runtime_integration_plan"
ARTIFACT_ROLE = "high_intensity_risk_cap_observe_only_runtime_integration_plan"
MODE = "observe_only_runtime_integration_plan"

EXPECTED_2341_DECISION = "CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT"
EXPECTED_2341_ROUTE = (
    "TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan"
)
EXPECTED_SELECTED_RULE = "COMPOSITE_HIGH_INTENSITY_RULE"
READY_2342_STATUSES = {
    "READY_FOR_2342_OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN",
    "READY_FOR_2342_WITH_CAVEATS",
}

NEXT_2343_DRY_RUN_TASK = (
    "TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run"
)
NEXT_2343_REMEDIATION_TASK = (
    "TRADING-2343_High_Intensity_Risk_Cap_Runtime_Prerequisite_Remediation"
)
NEXT_2343_OUTCOME_JOB_TASK = (
    "TRADING-2343_High_Intensity_Risk_Cap_Outcome_Update_Job_Plan"
)
NEXT_2343_ARCHIVE_TASK = "TRADING-2343_Archive_High_Intensity_Risk_Cap_Runtime_Line"

HORIZONS = ["1d", "5d", "10d", "20d"]

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "runtime_integration_plan_only": True,
    "observe_only": True,
    "runtime_scheduler_enabled": False,
    "new_event_logging_executed": False,
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
    "target_weight_action",
    "target_weight",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
}


class HighIntensityRuntimeIntegrationPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_runtime_integration_plan(
    *,
    continue_decision_dir: Path = DEFAULT_CONTINUE_DECISION_ROOT,
    forward_outcome_review_dir: Path = DEFAULT_FORWARD_OUTCOME_REVIEW_ROOT,
    partial_readiness_dir: Path = DEFAULT_PARTIAL_READINESS_ROOT,
    outcome_binder_dir: Path = DEFAULT_OUTCOME_BINDER_ROOT,
    event_logger_dir: Path = DEFAULT_EVENT_LOGGER_ROOT,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityRuntimeIntegrationPlanError(
            f"high-intensity runtime integration plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_runtime_integration_plan_inputs(
        continue_decision_dir=continue_decision_dir,
        forward_outcome_review_dir=forward_outcome_review_dir,
        partial_readiness_dir=partial_readiness_dir,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
    )
    scope_contract = build_high_intensity_runtime_scope_contract(inputs)
    input_contract = build_high_intensity_runtime_input_contract(inputs)
    event_detection_contract = build_high_intensity_runtime_event_detection_contract(inputs)
    event_append_contract = build_high_intensity_runtime_event_append_contract()
    cluster_update_contract = build_high_intensity_runtime_cluster_update_contract()
    pending_outcome_contract = build_high_intensity_runtime_pending_outcome_update_contract()
    outcome_update_job_plan = build_high_intensity_runtime_outcome_update_job_plan()
    manual_review_contract = build_high_intensity_runtime_manual_review_context_contract(
        inputs
    )
    monthly_contract = build_high_intensity_runtime_monthly_concentration_monitoring_contract(
        inputs
    )
    artifact_path_plan = build_high_intensity_runtime_artifact_path_registry_plan()
    report_registry_plan = build_high_intensity_runtime_report_registry_update_plan()
    fail_closed_gate = build_high_intensity_runtime_fail_closed_safety_gate()
    dry_run_plan = build_high_intensity_runtime_observe_only_dry_run_plan()
    risk_register = build_high_intensity_runtime_integration_risk_register()
    readiness = build_high_intensity_2343_readiness_checklist(
        scope_contract=scope_contract,
        input_contract=input_contract,
        event_detection_contract=event_detection_contract,
        event_append_contract=event_append_contract,
        cluster_update_contract=cluster_update_contract,
        pending_outcome_contract=pending_outcome_contract,
        outcome_update_job_plan=outcome_update_job_plan,
        manual_review_contract=manual_review_contract,
        monthly_contract=monthly_contract,
        artifact_path_plan=artifact_path_plan,
        fail_closed_gate=fail_closed_gate,
        dry_run_plan=dry_run_plan,
        risk_register=risk_register,
    )
    task_route = build_high_intensity_2343_task_route(readiness)
    interpretation_boundary = build_high_intensity_runtime_integration_interpretation_boundary(
        generated_at=generated_at,
        inputs=inputs,
    )
    safety_boundary = build_high_intensity_runtime_integration_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_runtime_integration_plan_summary(
        generated_at=generated_at,
        continue_decision_dir=continue_decision_dir,
        forward_outcome_review_dir=forward_outcome_review_dir,
        partial_readiness_dir=partial_readiness_dir,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        inputs=inputs,
        scope_contract=scope_contract,
        input_contract=input_contract,
        event_detection_contract=event_detection_contract,
        event_append_contract=event_append_contract,
        cluster_update_contract=cluster_update_contract,
        pending_outcome_contract=pending_outcome_contract,
        outcome_update_job_plan=outcome_update_job_plan,
        manual_review_contract=manual_review_contract,
        monthly_contract=monthly_contract,
        artifact_path_plan=artifact_path_plan,
        report_registry_plan=report_registry_plan,
        fail_closed_gate=fail_closed_gate,
        dry_run_plan=dry_run_plan,
        risk_register=risk_register,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_runtime_integration_plan_outputs(
        paths=paths,
        summary=summary,
        scope_contract=scope_contract,
        input_contract=input_contract,
        event_detection_contract=event_detection_contract,
        event_append_contract=event_append_contract,
        cluster_update_contract=cluster_update_contract,
        pending_outcome_contract=pending_outcome_contract,
        outcome_update_job_plan=outcome_update_job_plan,
        manual_review_contract=manual_review_contract,
        monthly_contract=monthly_contract,
        artifact_path_plan=artifact_path_plan,
        report_registry_plan=report_registry_plan,
        fail_closed_gate=fail_closed_gate,
        dry_run_plan=dry_run_plan,
        risk_register=risk_register,
        readiness=readiness,
        task_route=task_route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_runtime_integration_plan_inputs(
    *,
    continue_decision_dir: Path,
    forward_outcome_review_dir: Path,
    partial_readiness_dir: Path,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    try:
        lower_inputs = {
            "forward_outcome_review": load_trading_2340_forward_outcome_review_outputs(
                forward_outcome_review_dir
            ),
            "partial_readiness": load_trading_2339_continue_observe_context(
                partial_readiness_dir
            ),
            "outcome_binder": load_trading_2337_continue_observe_context(
                outcome_binder_dir
            ),
            "threshold_selection": load_trading_2335_continue_observe_context(
                threshold_selection_dir
            ),
            "forward_observe_plan": load_trading_2334_continue_observe_context(
                forward_observe_plan_dir
            ),
        }
    except HighIntensityContinueObserveDecisionError as exc:
        raise HighIntensityRuntimeIntegrationPlanError(str(exc)) from exc

    inputs = {
        "continue_decision": load_trading_2341_runtime_integration_context(
            continue_decision_dir
        ),
        "event_logger": load_trading_2336_runtime_integration_context(event_logger_dir),
        **lower_inputs,
    }
    _validate_cross_source_contracts(inputs)
    return inputs


def load_trading_2341_runtime_integration_context(
    continue_decision_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": continue_decision_dir / "high_intensity_continue_observe_decision_summary.json",
        "decision_matrix": continue_decision_dir
        / "high_intensity_continue_observe_decision_matrix.json",
        "selected_rule_contract": continue_decision_dir
        / "high_intensity_selected_rule_continuation_contract.json",
        "observe_scope": continue_decision_dir / "high_intensity_observe_continuation_scope.json",
        "partial_caveat": continue_decision_dir
        / "high_intensity_partial_coverage_carryforward_caveat.json",
        "monthly_plan": continue_decision_dir
        / "high_intensity_monthly_concentration_monitoring_plan.json",
        "event_logger_contract": continue_decision_dir
        / "high_intensity_event_logger_continuation_contract.json",
        "outcome_update_policy": continue_decision_dir
        / "high_intensity_outcome_update_policy.json",
        "manual_review_policy": continue_decision_dir
        / "high_intensity_manual_review_context_policy.json",
        "stop_policy": continue_decision_dir / "high_intensity_stop_refine_archive_policy.json",
        "prerequisite": continue_decision_dir
        / "high_intensity_runtime_integration_prerequisite_checklist.json",
        "readiness": continue_decision_dir / "high_intensity_2342_readiness_checklist.json",
        "task_route": continue_decision_dir / "high_intensity_2342_task_route.json",
        "interpretation_boundary": continue_decision_dir
        / "high_intensity_continue_observe_interpretation_boundary.json",
        "safety_boundary": continue_decision_dir
        / "high_intensity_continue_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2341 continuation decision")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2341 {key}", payload)
    summary = payloads["summary"]
    route = payloads["task_route"]
    readiness = payloads["readiness"]
    selected_rule_contract = payloads["selected_rule_contract"]
    if summary.get("overall_decision") != EXPECTED_2341_DECISION:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires 2341 decision "
            f"{EXPECTED_2341_DECISION}"
        )
    if summary.get("next_task") != EXPECTED_2341_ROUTE or route.get("next_task") != (
        EXPECTED_2341_ROUTE
    ):
        raise HighIntensityRuntimeIntegrationPlanError(
            f"TRADING-2342 requires 2341 route {EXPECTED_2341_ROUTE}"
        )
    if str(readiness.get("readiness_status")) not in READY_2342_STATUSES:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires 2341 readiness for observe-only runtime integration"
        )
    if str(route.get("readiness_status")) not in READY_2342_STATUSES:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires 2341 task route readiness for runtime integration"
        )
    if selected_rule_contract.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if selected_rule_contract.get("rule_continued") is not True:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires continued selected rule contract"
        )
    if selected_rule_contract.get("rule_changed") is not False:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 blocks selected rule changes"
        )
    if not selected_rule_contract.get("selected_rule_hash"):
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires selected rule hash"
        )
    _require_false(summary, "runtime_scheduler_enabled", "TRADING-2341 summary")
    _require_false(summary, "new_event_logging_executed", "TRADING-2341 summary")
    _require_false(summary, "outcome_binding_executed", "TRADING-2341 summary")
    return {"source_dir": str(continue_decision_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2336_runtime_integration_context(event_logger_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": event_logger_dir / "high_intensity_event_logger_summary.json",
        "event_log": event_logger_dir / "high_intensity_observe_event_log.json",
        "cluster_registry": event_logger_dir / "high_intensity_observe_event_cluster_registry.json",
        "pending_outcome_registry": event_logger_dir
        / "high_intensity_pending_outcome_registry.json",
        "outcome_schedule": event_logger_dir / "high_intensity_outcome_collection_schedule.json",
        "manual_review_queue": event_logger_dir / "high_intensity_manual_review_event_queue.json",
        "monthly_report": event_logger_dir / "high_intensity_monthly_concentration_report.json",
        "interpretation_boundary": event_logger_dir
        / "high_intensity_event_logger_interpretation_boundary.json",
        "safety_boundary": event_logger_dir / "high_intensity_event_logger_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2336 event logger lineage")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2336 {key}", payload)
    summary = payloads["summary"]
    if int(summary.get("cluster_count") or 0) <= 0:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires non-empty 2336 event logger lineage"
        )
    event_log_count = int(
        payloads["event_log"].get("row_count") or len(payloads["event_log"].get("rows") or [])
    )
    if event_log_count <= 0:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires non-empty 2336 observe event log"
        )
    return {"source_dir": str(event_logger_dir), "paths": _string_paths(paths), **payloads}


def build_high_intensity_runtime_scope_contract(inputs: Mapping[str, Any]) -> dict[str, Any]:
    selected = inputs["continue_decision"]["selected_rule_contract"]
    partial = inputs["continue_decision"]["partial_caveat"]
    monthly = inputs["continue_decision"]["monthly_plan"]
    return clean_for_yaml(
        {
            "scope_id": "HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_RUNTIME_SCOPE_V1",
            "scope_version": "v1",
            "observe_line": "high_intensity_risk_cap",
            "runtime_mode": "observe_only",
            "runtime_integration_plan_only": True,
            "runtime_scheduler_enabled": False,
            "event_detection_allowed_for_next_task": True,
            "event_append_allowed_for_next_task": True,
            "outcome_update_allowed_for_next_task": True,
            "manual_review_context_allowed": True,
            "automatic_exposure_cap_allowed": False,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "selected_rule_id": selected.get("selected_rule_id"),
            "selected_rule_hash": selected.get("selected_rule_hash"),
            "selected_rule_type": EXPECTED_SELECTED_RULE,
            "partial_coverage_caveat_required": partial.get(
                "partial_coverage_caveat_required"
            )
            is True,
            "monthly_concentration_monitoring_required": monthly.get(
                "monitoring_required"
            )
            is True,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_input_contract(inputs: Mapping[str, Any]) -> dict[str, Any]:
    selected = inputs["threshold_selection"]["selected_rule"]
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_INPUT_CONTRACT_V1",
            "contract_version": "v1",
            "required_runtime_inputs": [
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
            ],
            "optional_runtime_inputs": [
                "manual_review_note",
                "latest_market_data_for_outcome_update",
            ],
            "input_source_requirements": {
                "selected_trigger_rule": {
                    "source": "high_intensity_selected_trigger_rule",
                    "source_path": inputs["threshold_selection"]["paths"]["selected_rule"],
                    "required": True,
                },
                "risk_cap_trigger_series": {
                    "source": "canonical_risk_cap_signal_output",
                    "required": True,
                },
                "trading_calendar": {
                    "source": "canonical_calendar",
                    "required": True,
                },
                "known_at_policy": {
                    "source": selected.get("trigger_rule", {}).get("known_at_policy"),
                    "required": True,
                },
                "pit_policy": {
                    "source": selected.get("trigger_rule", {}).get("pit_policy"),
                    "required": True,
                },
            },
            "blocked_inputs": [
                "broker_position",
                "real_portfolio_weight",
                "live_order_state",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_event_detection_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rule = inputs["threshold_selection"]["selected_rule"]
    trigger_rule = selected_rule.get("trigger_rule", {})
    required_fields = list(trigger_rule.get("required_fields") or [])
    if not required_fields:
        required_fields = list(
            inputs["threshold_selection"]["selected_contract"].get(
                "required_input_fields"
            )
            or inputs["continue_decision"]["selected_rule_contract"].get(
                "required_input_fields"
            )
            or []
        )
    for timestamp_field in [
        "as_of_timestamp",
        "decision_timestamp",
        "known_at_policy",
        "pit_policy",
    ]:
        if timestamp_field not in required_fields:
            required_fields.append(timestamp_field)
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_EVENT_DETECTION_CONTRACT_V1",
            "contract_version": "v1",
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_hash": inputs["continue_decision"]["selected_rule_contract"].get(
                "selected_rule_hash"
            ),
            "selected_rule_type": EXPECTED_SELECTED_RULE,
            "required_fields": required_fields,
            "boolean_expression": trigger_rule.get("boolean_expression")
            or (
                "risk_cap_triggered == true AND scope_active == true "
                "AND risk_cap_score >= 1.0 AND signal_direction != none"
            ),
            "event_detection_output": [
                "trigger_day_id",
                "high_intensity_triggered",
                "high_intensity_reason",
                "detection_status",
            ],
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "blocked_outputs": [
                "target_weight",
                "rebalance_instruction",
                "reduce_position_instruction",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_event_append_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_EVENT_APPEND_CONTRACT_V1",
            "contract_version": "v1",
            "append_mode": "append_only",
            "prior_event_log_required": True,
            "dedup_required": True,
            "event_id_deterministic": True,
            "event_id_rule": {
                "hash_inputs": [
                    "selected_rule_id",
                    "target_asset",
                    "event_cluster_id",
                    "cluster_start_date",
                ]
            },
            "event_status_on_create": "OBSERVE_PENDING",
            "allowed_appended_fields": [
                "event_id",
                "event_date",
                "target_asset",
                "selected_rule_id",
                "event_cluster_id",
                "is_new_event",
                "is_cluster_primary_event",
                "manual_review_observation_flag",
                "event_status",
            ],
            "blocked_appended_fields": [
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "broker_action",
            ],
            "original_event_log_mutation_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_cluster_update_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_CLUSTER_UPDATE_CONTRACT_V1",
            "contract_version": "v1",
            "cluster_update_mode": "append_or_extend_open_cluster",
            "prior_cluster_registry_required": True,
            "cluster_id_deterministic": True,
            "new_cluster_condition": [
                "no open cluster for same selected_rule_id / target_asset",
                "trigger not consecutive with previous cluster",
            ],
            "extend_cluster_condition": [
                "same selected_rule_id",
                "same target_asset",
                "consecutive trigger day",
            ],
            "cluster_fields": [
                "event_cluster_id",
                "cluster_start_date",
                "cluster_end_date",
                "cluster_active_days",
                "trigger_day_count",
                "primary_event_id",
                "monthly_bucket",
                "consecutive_trigger_days",
            ],
            "cluster_status_allowed": ["OPEN", "CLOSED", "HISTORICAL_CLOSED"],
            "monthly_concentration_tracking_required": True,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_pending_outcome_update_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_PENDING_OUTCOME_UPDATE_CONTRACT_V1",
            "contract_version": "v1",
            "pending_registry_update_mode": "append_only",
            "new_event_generates_pending_outcomes": True,
            "horizons": HORIZONS,
            "pending_outcome_fields": [
                "pending_outcome_id",
                "event_id",
                "event_date",
                "target_asset",
                "horizon",
                "outcome_due_date",
                "outcome_status",
            ],
            "outcome_status_on_create": "OUTCOME_PENDING",
            "outcome_binding_allowed_in_event_append_job": False,
            "outcome_binding_task": "runtime_outcome_update_job",
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_outcome_update_job_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "job_plan_id": "HIGH_INTENSITY_RUNTIME_OUTCOME_UPDATE_JOB_PLAN_V1",
            "job_type": "observe_only_outcome_update",
            "scheduler_enabled_in_2342": False,
            "future_scheduler_candidate": True,
            "input_registry": [
                "pending_outcome_registry",
                "market_data_cache",
                "trading_calendar",
            ],
            "output_artifacts": [
                "updated_outcome_registry",
                "event_actual_path_outcome_increment",
                "cluster_actual_path_outcome_increment",
                "outcome_quality_report",
            ],
            "update_horizons": HORIZONS,
            "data_validation_required_when_enabled": True,
            "validate_data_policy": {
                "use_canonical_validate_data": True,
                "no_rule_relaxation": True,
            },
            "original_event_log_mutation_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_manual_review_context_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    manual_policy = inputs["continue_decision"]["manual_review_policy"]
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_MANUAL_REVIEW_CONTEXT_CONTRACT_V1",
            "contract_version": "v1",
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
            "display_label": manual_policy.get(
                "display_label", "HIGH_INTENSITY_RISK_WARNING_CONTEXT"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_monthly_concentration_monitoring_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    monthly = inputs["continue_decision"]["monthly_plan"]
    guardrails = monthly.get("guardrails") or {}
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_RUNTIME_MONTHLY_CONCENTRATION_MONITORING_CONTRACT_V1",
            "contract_version": "v1",
            "monitoring_required": True,
            "inherited_warning": monthly.get(
                "inherited_warning", "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
            ),
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
                "max_monthly_event_count": guardrails.get("max_monthly_event_count", 3),
                "max_monthly_cluster_count": guardrails.get("max_monthly_cluster_count", 3),
                "max_consecutive_trigger_days": guardrails.get(
                    "max_consecutive_trigger_days", 5
                ),
            },
            "guardrail_action": {
                "warning": "flag_for_review",
                "blocking": "pause_new_runtime_integration_until_review",
            },
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_artifact_path_registry_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "plan_id": "HIGH_INTENSITY_RUNTIME_ARTIFACT_PATH_REGISTRY_PLAN_V1",
            "runtime_artifact_root": {
                "proposed": "outputs/research_trends/high_intensity_risk_cap_runtime_observe/"
            },
            "runtime_artifacts": {
                "observe_event_log_increment": (
                    "high_intensity_runtime_observe_event_log_increment.json"
                ),
                "observe_event_cluster_registry_increment": (
                    "high_intensity_runtime_observe_event_cluster_registry_increment.json"
                ),
                "pending_outcome_registry_increment": (
                    "high_intensity_runtime_pending_outcome_registry_increment.json"
                ),
                "manual_review_context_queue_increment": (
                    "high_intensity_runtime_manual_review_context_queue_increment.json"
                ),
                "monthly_concentration_monitoring_update": (
                    "high_intensity_runtime_monthly_concentration_monitoring_update.json"
                ),
                "outcome_update_increment": (
                    "high_intensity_runtime_outcome_update_increment.json"
                ),
            },
            "registry_requirements": {
                "report_registry_entry_required": True,
                "artifact_catalog_entry_required": True,
                "system_flow_update_required": True,
                "task_register_update_required": True,
            },
            "outputs_directory_git_policy": "ignored_runtime_artifacts",
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_report_registry_update_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "plan_id": "HIGH_INTENSITY_RUNTIME_REPORT_REGISTRY_UPDATE_PLAN_V1",
            "new_report_families": [
                "high_intensity_runtime_observe_event_log",
                "high_intensity_runtime_pending_outcome_registry",
                "high_intensity_runtime_outcome_update",
                "high_intensity_runtime_manual_review_context",
                "high_intensity_runtime_monitoring",
            ],
            "registry_update_mode": "planned_not_applied_to_runtime_scheduler",
            "documentation_required": True,
            "safety_boundary_required": True,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_fail_closed_safety_gate() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "gate_id": "HIGH_INTENSITY_RUNTIME_FAIL_CLOSED_SAFETY_GATE_V1",
            "fail_closed_required": True,
            "block_if_missing_selected_rule": True,
            "block_if_missing_known_at_timestamp": True,
            "block_if_missing_pit_policy": True,
            "block_if_missing_event_schema": True,
            "block_if_missing_manual_review_boundary": True,
            "block_if_safety_gate_open": True,
            "block_if_broker_action_requested": True,
            "block_if_target_weight_generated": True,
            "block_if_rebalance_instruction_generated": True,
            "block_if_paper_shadow_enabled": True,
            "block_if_production_enabled": True,
            "allowed_effects": [
                "observe_event_log_append",
                "pending_outcome_registry_append",
                "manual_review_context_update",
            ],
            "blocked_effects": [
                "portfolio_effect",
                "production_effect",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def evaluate_high_intensity_runtime_fail_closed_safety_gate(
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if not candidate.get("selected_rule_id"):
        blockers.append("MISSING_SELECTED_RULE")
    if not candidate.get("known_at_timestamp") and not candidate.get("as_of_timestamp"):
        blockers.append("MISSING_KNOWN_AT_TIMESTAMP")
    if not candidate.get("pit_policy"):
        blockers.append("MISSING_PIT_POLICY")
    if _truthy(candidate.get("target_weight_generated")) or _emits_action(
        candidate.get("target_weight")
    ):
        blockers.append("TARGET_WEIGHT_GENERATED")
    if _truthy(candidate.get("rebalance_instruction_generated")) or _emits_action(
        candidate.get("rebalance_instruction")
    ):
        blockers.append("REBALANCE_INSTRUCTION_GENERATED")
    if _emits_action(candidate.get("broker_action_requested")) or (
        str(candidate.get("broker_action", "none")).lower() not in {"", "none"}
    ):
        blockers.append("BROKER_ACTION_REQUESTED")
    if _truthy(candidate.get("paper_shadow_enabled")) or _truthy(
        candidate.get("paper_shadow_allowed")
    ):
        blockers.append("PAPER_SHADOW_ENABLED")
    if _truthy(candidate.get("production_enabled")) or _truthy(
        candidate.get("production_allowed")
    ):
        blockers.append("PRODUCTION_ENABLED")
    return clean_for_yaml(
        {
            "gate_status": "BLOCKED" if blockers else "PASS",
            "blocked": bool(blockers),
            "blockers": blockers,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_observe_only_dry_run_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "dry_run_plan_id": "HIGH_INTENSITY_RUNTIME_OBSERVE_ONLY_DRY_RUN_PLAN_V1",
            "dry_run_mode": "observe_only_runtime_integration_dry_run",
            "scheduler_enabled": False,
            "historical_replay_allowed": True,
            "daily_append_simulation_allowed": True,
            "real_daily_scheduler_allowed": False,
            "input_contracts": [
                "runtime_input_contract",
                "event_detection_contract",
                "event_append_contract",
                "cluster_update_contract",
                "pending_outcome_update_contract",
            ],
            "expected_outputs": [
                "dry_run_event_append_result",
                "dry_run_cluster_update_result",
                "dry_run_pending_outcome_result",
                "dry_run_manual_review_context_result",
                "dry_run_safety_gate_result",
            ],
            "next_task_candidate": NEXT_2343_DRY_RUN_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_integration_risk_register() -> dict[str, Any]:
    rows = [
        (
            "EVENT_DUPLICATION_RISK",
            "event duplication risk",
            "Runtime append could duplicate an existing cluster-primary event.",
            "high",
            "Require deterministic event ids and dedup against prior event log.",
            True,
            True,
        ),
        (
            "CLUSTER_EXTENSION_RISK",
            "cluster extension risk",
            "Consecutive trigger days could be misclassified as new independent clusters.",
            "high",
            "Use append_or_extend_open_cluster contract and consecutive-day checks.",
            True,
            True,
        ),
        (
            "MONTHLY_CONCENTRATION_OVERCOUNT_RISK",
            "monthly concentration overcount risk",
            "A single risk episode could inflate monthly event counts.",
            "high",
            "Track monthly trigger/event/cluster counts and continuation counts.",
            True,
            True,
        ),
        (
            "MANUAL_CONTEXT_MISINTERPRETATION_RISK",
            "manual-review context misinterpretation risk",
            "Review context could be mistaken for a reduce-position instruction.",
            "high",
            "Block target-weight and rebalance display fields.",
            True,
            True,
        ),
        (
            "OUTCOME_UPDATE_LAG_RISK",
            "outcome update lag risk",
            "Matured horizons could remain pending and bias evidence review timing.",
            "medium",
            "Require scheduled outcome update job with validate-data gate when enabled.",
            True,
            False,
        ),
        (
            "PIT_CAVEAT_MISINTERPRETATION_RISK",
            "PIT caveat misinterpretation risk",
            "PIT approximation could be read as strict PIT readiness.",
            "medium",
            "Expose strict_pit_ready=false and pit_approximation_ready=true.",
            True,
            False,
        ),
        (
            "ACCIDENTAL_PAPER_SHADOW_PROMOTION_RISK",
            "accidental paper-shadow promotion risk",
            "Observe-only artifacts could be routed as paper-shadow signals.",
            "critical",
            "Fail closed on any paper_shadow flag and keep route observe-only.",
            True,
            True,
        ),
        (
            "ACCIDENTAL_BROKER_ACTION_RISK",
            "accidental broker action risk",
            "Runtime context could accidentally request broker action.",
            "critical",
            "Fail closed on broker_action != none or requested broker action.",
            True,
            True,
        ),
    ]
    return clean_for_yaml(
        {
            "register_id": "HIGH_INTENSITY_RUNTIME_INTEGRATION_RISK_REGISTER_V1",
            "rows": [
                {
                    "risk_id": risk_id,
                    "risk_name": name,
                    "risk_description": description,
                    "risk_severity": severity,
                    "mitigation": mitigation,
                    "owner_review_required": owner_review_required,
                    "blocked_if_unmitigated": blocked_if_unmitigated,
                }
                for (
                    risk_id,
                    name,
                    description,
                    severity,
                    mitigation,
                    owner_review_required,
                    blocked_if_unmitigated,
                ) in rows
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2343_readiness_checklist(
    *,
    scope_contract: Mapping[str, Any],
    input_contract: Mapping[str, Any],
    event_detection_contract: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    cluster_update_contract: Mapping[str, Any],
    pending_outcome_contract: Mapping[str, Any],
    outcome_update_job_plan: Mapping[str, Any],
    manual_review_contract: Mapping[str, Any],
    monthly_contract: Mapping[str, Any],
    artifact_path_plan: Mapping[str, Any],
    fail_closed_gate: Mapping[str, Any],
    dry_run_plan: Mapping[str, Any],
    risk_register: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if event_detection_contract.get("strict_pit_ready") is not True:
        warnings.append("PIT_APPROXIMATION_CAVEAT")
    if monthly_contract.get("monitoring_required") is True:
        warnings.append("MONTHLY_CONCENTRATION_MONITORING_REQUIRED")
    if scope_contract.get("partial_coverage_caveat_required") is True:
        warnings.append("PARTIAL_COVERAGE_CAVEAT")
    if not risk_register.get("rows"):
        blockers.append("RISK_REGISTER_MISSING")
    required = {
        "runtime_scope_contract_generated": bool(scope_contract),
        "runtime_input_contract_generated": bool(input_contract),
        "event_detection_contract_generated": bool(event_detection_contract),
        "event_append_contract_generated": bool(event_append_contract),
        "cluster_update_contract_generated": bool(cluster_update_contract),
        "pending_outcome_update_contract_generated": bool(pending_outcome_contract),
        "outcome_update_job_plan_generated": bool(outcome_update_job_plan),
        "manual_review_context_contract_generated": bool(manual_review_contract),
        "monthly_concentration_monitoring_contract_generated": bool(monthly_contract),
        "artifact_path_registry_plan_generated": bool(artifact_path_plan),
        "fail_closed_safety_gate_generated": bool(fail_closed_gate),
        "observe_only_dry_run_plan_generated": bool(dry_run_plan),
    }
    for key, generated in required.items():
        if not generated:
            blockers.append(key.replace("_generated", "").upper() + "_MISSING")
    if blockers:
        readiness_status = "RUNTIME_PREREQUISITE_REMEDIATION_REQUIRED"
    elif warnings:
        readiness_status = "READY_FOR_2343_WITH_CAVEATS"
    else:
        readiness_status = "READY_FOR_2343_OBSERVE_ONLY_RUNTIME_DRY_RUN"
    return clean_for_yaml(
        {
            **required,
            "runtime_scheduler_enabled": False,
            "new_event_logging_executed": False,
            "outcome_binding_executed": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": readiness_status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2343_task_route(readiness: Mapping[str, Any]) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    blockers = list(readiness.get("readiness_blockers") or [])
    warnings = list(readiness.get("readiness_warnings") or [])
    if status == "READY_FOR_2343_OBSERVE_ONLY_RUNTIME_DRY_RUN":
        next_task = NEXT_2343_DRY_RUN_TASK
    elif status == "READY_FOR_2343_WITH_CAVEATS":
        next_task = NEXT_2343_DRY_RUN_TASK
        if "OBSERVE_ONLY_NO_PAPER_SHADOW" not in warnings:
            warnings.append("OBSERVE_ONLY_NO_PAPER_SHADOW")
    elif status in {
        "RUNTIME_PREREQUISITE_REMEDIATION_REQUIRED",
        "RUNTIME_INMEDIATION_REQUIRED",
    }:
        next_task = NEXT_2343_REMEDIATION_TASK
    elif status == "OUTCOME_UPDATE_JOB_PLAN_REQUIRED":
        next_task = NEXT_2343_OUTCOME_JOB_TASK
    else:
        next_task = NEXT_2343_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "allowed_routes": [
                NEXT_2343_DRY_RUN_TASK,
                NEXT_2343_REMEDIATION_TASK,
                NEXT_2343_OUTCOME_JOB_TASK,
                NEXT_2343_ARCHIVE_TASK,
            ],
            "readiness_status": status,
            "next_task": next_task,
            "route_blockers": blockers,
            "route_caveats": warnings,
            "route_rationale": _route_rationale(status, warnings, blockers),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_integration_interpretation_boundary(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "boundary_id": "HIGH_INTENSITY_RUNTIME_INTEGRATION_INTERPRETATION_BOUNDARY_V1",
            "generated_at": generated_at.isoformat(),
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "partial_coverage_caveat_required": inputs["continue_decision"][
                "partial_caveat"
            ].get("partial_coverage_caveat_required")
            is True,
            "monthly_concentration_monitoring_required": inputs["continue_decision"][
                "monthly_plan"
            ].get("monitoring_required")
            is True,
            "forbidden_interpretations": [
                "real account performance",
                "real position advice",
                "reduce position signal",
                "paper-shadow signal",
                "production strategy",
                "broker action",
                "scheduler enabled",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_integration_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "boundary_id": "HIGH_INTENSITY_RUNTIME_INTEGRATION_SAFETY_BOUNDARY_V1",
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


def build_high_intensity_runtime_integration_plan_summary(
    *,
    generated_at: datetime,
    continue_decision_dir: Path,
    forward_outcome_review_dir: Path,
    partial_readiness_dir: Path,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    inputs: Mapping[str, Any],
    scope_contract: Mapping[str, Any],
    input_contract: Mapping[str, Any],
    event_detection_contract: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    cluster_update_contract: Mapping[str, Any],
    pending_outcome_contract: Mapping[str, Any],
    outcome_update_job_plan: Mapping[str, Any],
    manual_review_contract: Mapping[str, Any],
    monthly_contract: Mapping[str, Any],
    artifact_path_plan: Mapping[str, Any],
    report_registry_plan: Mapping[str, Any],
    fail_closed_gate: Mapping[str, Any],
    dry_run_plan: Mapping[str, Any],
    risk_register: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    continue_summary = inputs["continue_decision"]["summary"]
    outcome_summary = inputs["outcome_binder"]["summary"]
    event_summary = inputs["event_logger"]["summary"]
    status = (
        "OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
        if readiness.get("readiness_status") == "READY_FOR_2343_WITH_CAVEATS"
        else "OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN_READY_PROMOTION_BLOCKED"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Observe-Only Runtime Integration Plan",
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": status,
            "runtime_integration_plan_status": status,
            "continue_decision_dir": str(continue_decision_dir),
            "forward_outcome_review_dir": str(forward_outcome_review_dir),
            "partial_readiness_dir": str(partial_readiness_dir),
            "outcome_binder_dir": str(outcome_binder_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "source_2341_decision": continue_summary.get("overall_decision"),
            "source_2341_readiness": continue_summary.get("2342_readiness_status"),
            "source_2341_next_task": continue_summary.get("next_task"),
            "selected_rule_id": scope_contract.get("selected_rule_id"),
            "selected_rule_hash": scope_contract.get("selected_rule_hash"),
            "selected_rule_type": scope_contract.get("selected_rule_type"),
            "partial_coverage_caveat_required": scope_contract.get(
                "partial_coverage_caveat_required"
            ),
            "monthly_concentration_monitoring_required": scope_contract.get(
                "monthly_concentration_monitoring_required"
            ),
            "runtime_scope_contract_generated": bool(scope_contract),
            "runtime_input_contract_generated": bool(input_contract),
            "runtime_event_detection_contract_generated": bool(event_detection_contract),
            "runtime_event_append_contract_generated": bool(event_append_contract),
            "runtime_cluster_update_contract_generated": bool(cluster_update_contract),
            "runtime_pending_outcome_update_contract_generated": bool(
                pending_outcome_contract
            ),
            "runtime_outcome_update_job_plan_generated": bool(outcome_update_job_plan),
            "runtime_manual_review_context_contract_generated": bool(
                manual_review_contract
            ),
            "runtime_monthly_concentration_monitoring_contract_generated": bool(
                monthly_contract
            ),
            "runtime_artifact_path_registry_plan_generated": bool(artifact_path_plan),
            "runtime_report_registry_update_plan_generated": bool(report_registry_plan),
            "runtime_fail_closed_safety_gate_generated": bool(fail_closed_gate),
            "observe_only_dry_run_plan_generated": bool(dry_run_plan),
            "runtime_integration_risk_register_generated": bool(risk_register),
            "2343_readiness_checklist_generated": bool(readiness),
            "2343_readiness_status": readiness.get("readiness_status"),
            "2343_task_route_generated": bool(task_route),
            "next_task": task_route.get("next_task"),
            "route_caveats": task_route.get("route_caveats"),
            "event_logger_trigger_day_count": event_summary.get("trigger_day_count"),
            "event_logger_event_count_after_dedup": event_summary.get(
                "event_count_after_dedup"
            ),
            "event_logger_cluster_count": event_summary.get("cluster_count"),
            "pending_outcome_count": outcome_summary.get("pending_outcome_count"),
            "bound_outcome_count": continue_summary.get("bound_outcome_count"),
            "expected_outcome_count": continue_summary.get("expected_outcome_count"),
            "not_due_outcome_count": continue_summary.get("not_due_outcome_count"),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "not rerun because TRADING-2342 only reads prior validated "
                "TRADING-2341 / 2340 / 2337 artifacts and does not consume "
                "market data directly"
            ),
            "source_validate_data_executed": outcome_summary.get("validate_data_executed"),
            "source_validate_data_as_of": outcome_summary.get("validate_data_as_of"),
            "source_validate_data_status": outcome_summary.get("validate_data_status"),
            "source_validate_data_error_count": outcome_summary.get(
                "validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_runtime_integration_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    scope_contract: Mapping[str, Any],
    input_contract: Mapping[str, Any],
    event_detection_contract: Mapping[str, Any],
    event_append_contract: Mapping[str, Any],
    cluster_update_contract: Mapping[str, Any],
    pending_outcome_contract: Mapping[str, Any],
    outcome_update_job_plan: Mapping[str, Any],
    manual_review_contract: Mapping[str, Any],
    monthly_contract: Mapping[str, Any],
    artifact_path_plan: Mapping[str, Any],
    report_registry_plan: Mapping[str, Any],
    fail_closed_gate: Mapping[str, Any],
    dry_run_plan: Mapping[str, Any],
    risk_register: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["scope_contract"], scope_contract)
    write_json(paths["input_contract"], input_contract)
    write_json(paths["event_detection_contract"], event_detection_contract)
    write_json(paths["event_append_contract"], event_append_contract)
    write_json(paths["cluster_update_contract"], cluster_update_contract)
    write_json(paths["pending_outcome_contract"], pending_outcome_contract)
    write_json(paths["outcome_update_job_plan"], outcome_update_job_plan)
    write_json(paths["manual_review_contract"], manual_review_contract)
    write_json(paths["monthly_contract"], monthly_contract)
    write_json(paths["artifact_path_plan"], artifact_path_plan)
    write_json(paths["report_registry_plan"], report_registry_plan)
    write_json(paths["fail_closed_gate"], fail_closed_gate)
    write_json(paths["dry_run_plan"], dry_run_plan)
    write_json(paths["risk_register"], risk_register)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], _render_main_doc(summary, scope_contract, task_route))
    write_markdown(
        paths["event_detection_doc"],
        _render_event_detection_doc(event_detection_contract),
    )
    write_markdown(
        paths["event_append_doc"],
        _render_event_append_doc(event_append_contract, outcome_update_job_plan),
    )
    write_markdown(
        paths["manual_review_doc"],
        _render_manual_review_doc(manual_review_contract),
    )
    write_markdown(paths["route_doc"], _render_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_runtime_integration_plan_summary.json",
        "scope_contract": output_dir / "high_intensity_runtime_scope_contract.json",
        "input_contract": output_dir / "high_intensity_runtime_input_contract.json",
        "event_detection_contract": output_dir
        / "high_intensity_runtime_event_detection_contract.json",
        "event_append_contract": output_dir / "high_intensity_runtime_event_append_contract.json",
        "cluster_update_contract": output_dir
        / "high_intensity_runtime_cluster_update_contract.json",
        "pending_outcome_contract": output_dir
        / "high_intensity_runtime_pending_outcome_update_contract.json",
        "outcome_update_job_plan": output_dir
        / "high_intensity_runtime_outcome_update_job_plan.json",
        "manual_review_contract": output_dir
        / "high_intensity_runtime_manual_review_context_contract.json",
        "monthly_contract": output_dir
        / "high_intensity_runtime_monthly_concentration_monitoring_contract.json",
        "artifact_path_plan": output_dir
        / "high_intensity_runtime_artifact_path_registry_plan.json",
        "report_registry_plan": output_dir
        / "high_intensity_runtime_report_registry_update_plan.json",
        "fail_closed_gate": output_dir
        / "high_intensity_runtime_fail_closed_safety_gate.json",
        "dry_run_plan": output_dir / "high_intensity_runtime_observe_only_dry_run_plan.json",
        "risk_register": output_dir
        / "high_intensity_runtime_integration_risk_register.json",
        "readiness": output_dir / "high_intensity_2343_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2343_task_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_runtime_integration_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_runtime_integration_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_observe_only_runtime_integration_plan.md",
        "event_detection_doc": docs_root
        / "high_intensity_runtime_event_detection_contract.md",
        "event_append_doc": docs_root
        / "high_intensity_runtime_event_append_and_outcome_update_plan.md",
        "manual_review_doc": docs_root
        / "high_intensity_runtime_manual_review_context_boundary.md",
        "route_doc": docs_root / "high_intensity_2343_readiness_route.md",
    }


def _validate_cross_source_contracts(inputs: Mapping[str, Any]) -> None:
    continue_selected = inputs["continue_decision"]["selected_rule_contract"]
    selected = inputs["threshold_selection"]["selected_rule"]
    if continue_selected.get("selected_rule_id") != selected.get("selected_rule_id"):
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 selected rule mismatch between 2341 and 2335"
        )
    if not inputs["forward_observe_plan"]["event_schema"]:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires 2334 event schema"
        )
    if not inputs["forward_observe_plan"]["actual_path_outcome_contract"]:
        raise HighIntensityRuntimeIntegrationPlanError(
            "TRADING-2342 requires 2334 actual-path outcome contract"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityRuntimeIntegrationPlanError(f"{label} missing {key}: {path}")
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityRuntimeIntegrationPlanError(f"{path}: expected JSON object")
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensityRuntimeIntegrationPlanError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _collect_unsafe_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in INPUT_SAFETY_FALSE_FIELDS and _truthy(item):
                violations.append(path)
            if key_text == "broker_action" and str(item).lower() not in {"", "none"}:
                violations.append(path)
            if key_text in FORBIDDEN_EMIT_FIELDS and _emits_action(item):
                violations.append(path)
            violations.extend(_collect_unsafe_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_unsafe_fields(item, f"{prefix}[{index}]"))
    return violations


def _require_false(payload: Mapping[str, Any], key: str, label: str) -> None:
    if payload.get(key) is not False:
        raise HighIntensityRuntimeIntegrationPlanError(
            f"{label} requires {key}=false"
        )


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _emits_action(value: object) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "none", "false", "not_applicable"}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return bool(value)
    if isinstance(value, Mapping):
        return bool(value)
    return bool(value)


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _route_rationale(status: str, warnings: Sequence[str], blockers: Sequence[str]) -> str:
    if blockers:
        return f"blocked by prerequisite blockers: {', '.join(map(str, blockers))}"
    if status == "READY_FOR_2343_WITH_CAVEATS":
        return "observe-only dry-run route allowed with caveats: " + ", ".join(
            map(str, warnings)
        )
    if status == "READY_FOR_2343_OBSERVE_ONLY_RUNTIME_DRY_RUN":
        return "observe-only runtime dry-run prerequisites are complete"
    if status == "OUTCOME_UPDATE_JOB_PLAN_REQUIRED":
        return "outcome update job plan must be completed before dry-run"
    return "runtime integration plan is not ready for dry-run"


def _render_main_doc(
    summary: Mapping[str, Any],
    scope: Mapping[str, Any],
    route: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Risk-Cap Observe-Only Runtime Integration Plan\n\n"
        f"- status: `{summary.get('status')}`\n"
        f"- selected_rule: `{summary.get('selected_rule_id')}`\n"
        f"- readiness: `{summary.get('2343_readiness_status')}`\n"
        f"- next_task: `{summary.get('next_task')}`\n"
        f"- source_validate_data: `{summary.get('source_validate_data_as_of')}` / "
        f"`{summary.get('source_validate_data_status')}` / "
        f"error_count=`{summary.get('source_validate_data_error_count')}`\n\n"
        "本报告只生成 observe-only runtime integration plan；不启动 runtime scheduler，"
        "不生成新 event，不 append runtime log，不绑定 outcome，不输出 target weight / "
        "rebalance / broker action。\n\n"
        "## Runtime Scope\n\n"
        f"- runtime_mode: `{scope.get('runtime_mode')}`\n"
        f"- runtime_scheduler_enabled: `{scope.get('runtime_scheduler_enabled')}`\n"
        f"- event_append_allowed_for_next_task: "
        f"`{scope.get('event_append_allowed_for_next_task')}`\n"
        f"- partial_coverage_caveat_required: "
        f"`{scope.get('partial_coverage_caveat_required')}`\n"
        f"- monthly_concentration_monitoring_required: "
        f"`{scope.get('monthly_concentration_monitoring_required')}`\n\n"
        "## Route\n\n"
        f"- route_caveats: `{route.get('route_caveats')}`\n"
        "- promotion / paper-shadow / production / broker action: blocked\n"
    )


def _render_event_detection_doc(contract: Mapping[str, Any]) -> str:
    return (
        "# High-Intensity Runtime Event Detection Contract\n\n"
        f"- selected_rule_id: `{contract.get('selected_rule_id')}`\n"
        f"- selected_rule_hash: `{contract.get('selected_rule_hash')}`\n"
        f"- boolean_expression: `{contract.get('boolean_expression')}`\n"
        f"- known_at_policy: `{contract.get('known_at_policy')}`\n"
        f"- strict_pit_ready: `{contract.get('strict_pit_ready')}`\n\n"
        "Detection output is observe-only context. Target weight, rebalance, reduce "
        "position and broker action outputs are blocked.\n"
    )


def _render_event_append_doc(
    append_contract: Mapping[str, Any],
    outcome_plan: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Runtime Event Append And Outcome Update Plan\n\n"
        f"- append_mode: `{append_contract.get('append_mode')}`\n"
        f"- dedup_required: `{append_contract.get('dedup_required')}`\n"
        f"- original_event_log_mutation_allowed: "
        f"`{append_contract.get('original_event_log_mutation_allowed')}`\n"
        f"- update_horizons: `{outcome_plan.get('update_horizons')}`\n"
        f"- scheduler_enabled_in_2342: "
        f"`{outcome_plan.get('scheduler_enabled_in_2342')}`\n\n"
        "2342 only plans the append and outcome update contracts. The actual dry-run "
        "or job implementation is routed to 2343 or later.\n"
    )


def _render_manual_review_doc(contract: Mapping[str, Any]) -> str:
    return (
        "# High-Intensity Runtime Manual Review Context Boundary\n\n"
        f"- display_mode: `{contract.get('display_mode')}`\n"
        f"- display_label: `{contract.get('display_label')}`\n"
        f"- manual_review_context_allowed: "
        f"`{contract.get('manual_review_context_allowed')}`\n\n"
        "Manual review context is risk-warning context only. It must not display or "
        "imply target weight, rebalance, buy/sell, reduce-position or broker action.\n"
    )


def _render_route_doc(
    readiness: Mapping[str, Any],
    route: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity 2343 Readiness Route\n\n"
        f"- readiness_status: `{readiness.get('readiness_status')}`\n"
        f"- readiness_warnings: `{readiness.get('readiness_warnings')}`\n"
        f"- readiness_blockers: `{readiness.get('readiness_blockers')}`\n"
        f"- next_task: `{route.get('next_task')}`\n\n"
        "2343 may only run observe-only runtime dry-run or prerequisite remediation. "
        "Scheduler, paper-shadow, production and broker action remain blocked.\n"
    )
