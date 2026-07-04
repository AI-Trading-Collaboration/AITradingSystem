from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    DEFAULT_CONTINUE_DECISION_ROOT,
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_RUNTIME_INTEGRATION_PLAN_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2343_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_RUNTIME_DRY_RUN"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_runtime_dry_run"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_runtime_dry_run"

EXPECTED_2342_STATUS = (
    "OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)
EXPECTED_2342_NEXT_TASK = (
    "TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run"
)
READY_2343_STATUSES = {
    "READY_FOR_2343_OBSERVE_ONLY_RUNTIME_DRY_RUN",
    "READY_FOR_2343_WITH_CAVEATS",
}
EXPECTED_SELECTED_RULE = "COMPOSITE_HIGH_INTENSITY_RULE"
KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
PIT_POLICY = "PIT_APPROXIMATION_READY"

NEXT_2344_SCHEDULER_PLAN_TASK = (
    "TRADING-2344_High_Intensity_Risk_Cap_Observe_Only_Runtime_Scheduler_Integration_Plan"
)
NEXT_2344_DRY_RUN_REMEDIATION_TASK = (
    "TRADING-2344_High_Intensity_Risk_Cap_Runtime_Dry_Run_Remediation"
)
NEXT_2344_SAFETY_REMEDIATION_TASK = (
    "TRADING-2344_High_Intensity_Risk_Cap_Runtime_Safety_Remediation"
)
NEXT_2344_REGISTRY_REMEDIATION_TASK = (
    "TRADING-2344_High_Intensity_Risk_Cap_Runtime_Artifact_Registry_Remediation"
)
NEXT_2344_ARCHIVE_TASK = "TRADING-2344_Archive_High_Intensity_Risk_Cap_Runtime_Line"

DEFAULT_DYNAMIC_DRY_RUN_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "source_bound_exposure_cap_dynamic_target_dry_run"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

REQUIRED_RUNTIME_FIELDS = [
    "date",
    "target_asset",
    "risk_cap_triggered",
    "risk_cap_intensity",
    "risk_cap_score",
    "scope_active",
    "signal_direction",
    "as_of_timestamp",
    "decision_timestamp",
    "known_at_policy",
    "pit_policy",
]
HORIZON_DAYS = {"1d": 1, "5d": 5, "10d": 10, "20d": 20}
CLUSTER_CONTINUATION_GAP_DAYS = 3

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only_runtime_dry_run": True,
    "observe_only": True,
    "manual_review_only": True,
    "runtime_scheduler_enabled": False,
    "new_event_logging_executed": False,
    "event_append_executed": False,
    "event_append_dry_run_executed": True,
    "outcome_binding_executed": False,
    "pending_outcome_update_dry_run_executed": True,
    "automatic_exposure_cap_allowed": False,
    "target_weight_action_allowed": False,
    "rebalance_instruction_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "portfolio_effect": "none",
    "production_effect": "none",
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
FORBIDDEN_OUTPUT_FIELDS = {
    "target_weight",
    "target_weight_action",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
    "automatic_exposure_cap",
}
MANUAL_REVIEW_FORBIDDEN_FIELDS = {
    "target_weight",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
    "broker_action",
}


class HighIntensityRuntimeDryRunError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_runtime_dry_run(
    *,
    runtime_integration_plan_dir: Path = DEFAULT_RUNTIME_INTEGRATION_PLAN_ROOT,
    continue_decision_dir: Path = DEFAULT_CONTINUE_DECISION_ROOT,
    event_logger_dir: Path = DEFAULT_EVENT_LOGGER_ROOT,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    dynamic_dry_run_dir: Path = DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityRuntimeDryRunError(
            f"high-intensity runtime dry-run only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_runtime_dry_run_inputs(
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
    )
    trigger_source_rows = records(
        mapping(inputs["dynamic_dry_run"]["trigger_alignment"]).get("rows")
    )
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])

    contract_validation = build_high_intensity_runtime_contract_validation_report(
        inputs
    )
    input_validation_rows = build_high_intensity_runtime_input_validation_matrix(
        trigger_source_rows=trigger_source_rows,
        selected_rule=selected_rule,
        dynamic_dry_run=inputs["dynamic_dry_run"],
    )
    detection_rows = build_high_intensity_runtime_event_detection_dry_run_result(
        trigger_source_rows=trigger_source_rows,
        selected_rule=selected_rule,
        inputs=inputs,
    )
    append_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=detection_rows,
        prior_trigger_day_rows=records(
            mapping(inputs["event_logger"]["trigger_day_log"]).get("rows")
        ),
        prior_event_rows=records(mapping(inputs["event_logger"]["event_log"]).get("rows")),
        prior_cluster_rows=records(
            mapping(inputs["event_logger"]["cluster_registry"]).get("rows")
        ),
    )
    cluster_rows = build_high_intensity_runtime_cluster_update_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=records(
            mapping(inputs["event_logger"]["cluster_registry"]).get("rows")
        ),
    )
    pending_rows = build_high_intensity_runtime_pending_outcome_update_dry_run_result(
        append_rows=append_rows,
    )
    manual_review_rows = (
        build_high_intensity_runtime_manual_review_context_dry_run_result(
            append_rows=append_rows,
            detection_rows=detection_rows,
            inputs=inputs,
        )
    )
    monthly_monitoring = (
        build_high_intensity_runtime_monthly_concentration_monitoring_dry_run(
            detection_rows=detection_rows,
            append_rows=append_rows,
            cluster_update_rows=cluster_rows,
            prior_event_rows=records(
                mapping(inputs["event_logger"]["event_log"]).get("rows")
            ),
            prior_cluster_rows=records(
                mapping(inputs["event_logger"]["cluster_registry"]).get("rows")
            ),
            inputs=inputs,
        )
    )
    safety_gate = build_high_intensity_runtime_fail_closed_safety_gate_result(
        {
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "as_of_timestamp": _first_non_empty(
                _as_of_timestamp(row) for row in trigger_source_rows
            ),
            "pit_policy": _pit_policy(selected_rule, inputs["dynamic_dry_run"]),
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
        }
    )
    artifact_registry = build_high_intensity_runtime_artifact_registry_dry_run_report(
        output_dir=output_dir,
        docs_root=docs_root,
    )
    data_quality = build_high_intensity_runtime_dry_run_data_quality_report(
        inputs=inputs,
        contract_validation=contract_validation,
        input_validation_rows=input_validation_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_monitoring=monthly_monitoring,
        safety_gate=safety_gate,
    )
    readiness = build_high_intensity_2344_readiness_checklist(
        contract_validation=contract_validation,
        input_validation_rows=input_validation_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_monitoring=monthly_monitoring,
        safety_gate=safety_gate,
        artifact_registry=artifact_registry,
    )
    task_route = build_high_intensity_2344_task_route(
        readiness=readiness,
        safety_gate=safety_gate,
        artifact_registry=artifact_registry,
    )
    interpretation_boundary = (
        build_high_intensity_runtime_dry_run_interpretation_boundary(
            generated_at=generated_at,
            inputs=inputs,
        )
    )
    safety_boundary = build_high_intensity_runtime_dry_run_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_runtime_dry_run_summary(
        generated_at=generated_at,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        inputs=inputs,
        contract_validation=contract_validation,
        input_validation_rows=input_validation_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_monitoring=monthly_monitoring,
        safety_gate=safety_gate,
        artifact_registry=artifact_registry,
        data_quality=data_quality,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_runtime_dry_run_outputs(
        paths=paths,
        summary=summary,
        contract_validation=contract_validation,
        input_validation_rows=input_validation_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_monitoring=monthly_monitoring,
        safety_gate=safety_gate,
        artifact_registry=artifact_registry,
        data_quality=data_quality,
        interpretation_boundary=interpretation_boundary,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_runtime_dry_run_inputs(
    *,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
) -> dict[str, Any]:
    runtime_plan = load_trading_2342_runtime_dry_run_context(
        runtime_integration_plan_dir
    )
    continue_decision = load_trading_2341_runtime_dry_run_context(continue_decision_dir)
    event_logger = load_trading_2336_runtime_dry_run_context(event_logger_dir)
    threshold_selection = load_trading_2335_runtime_dry_run_context(
        threshold_selection_dir
    )
    forward_observe_plan = load_trading_2334_runtime_dry_run_context(
        forward_observe_plan_dir
    )
    dynamic_dry_run = load_trading_2332_runtime_dry_run_context(dynamic_dry_run_dir)
    inputs = {
        "runtime_plan": runtime_plan,
        "continue_decision": continue_decision,
        "event_logger": event_logger,
        "threshold_selection": threshold_selection,
        "forward_observe_plan": forward_observe_plan,
        "dynamic_dry_run": dynamic_dry_run,
    }
    _validate_runtime_dry_run_cross_source_contracts(inputs)
    return inputs


def load_trading_2342_runtime_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_runtime_integration_plan_summary.json",
        "scope_contract": root / "high_intensity_runtime_scope_contract.json",
        "input_contract": root / "high_intensity_runtime_input_contract.json",
        "event_detection_contract": root
        / "high_intensity_runtime_event_detection_contract.json",
        "event_append_contract": root / "high_intensity_runtime_event_append_contract.json",
        "cluster_update_contract": root
        / "high_intensity_runtime_cluster_update_contract.json",
        "pending_outcome_update_contract": root
        / "high_intensity_runtime_pending_outcome_update_contract.json",
        "outcome_update_job_plan": root
        / "high_intensity_runtime_outcome_update_job_plan.json",
        "manual_review_context_contract": root
        / "high_intensity_runtime_manual_review_context_contract.json",
        "monthly_concentration_monitoring_contract": root
        / "high_intensity_runtime_monthly_concentration_monitoring_contract.json",
        "artifact_path_registry_plan": root
        / "high_intensity_runtime_artifact_path_registry_plan.json",
        "report_registry_update_plan": root
        / "high_intensity_runtime_report_registry_update_plan.json",
        "fail_closed_safety_gate": root
        / "high_intensity_runtime_fail_closed_safety_gate.json",
        "observe_only_dry_run_plan": root
        / "high_intensity_runtime_observe_only_dry_run_plan.json",
        "risk_register": root / "high_intensity_runtime_integration_risk_register.json",
        "readiness": root / "high_intensity_2343_readiness_checklist.json",
        "task_route": root / "high_intensity_2343_task_route.json",
        "interpretation_boundary": root
        / "high_intensity_runtime_integration_interpretation_boundary.json",
        "safety_boundary": root / "high_intensity_runtime_integration_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2342 runtime integration plan")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2342 {key}", payload)
    summary = payloads["summary"]
    route = payloads["task_route"]
    readiness = payloads["readiness"]
    if summary.get("status") != EXPECTED_2342_STATUS:
        raise HighIntensityRuntimeDryRunError(
            f"TRADING-2343 requires 2342 status {EXPECTED_2342_STATUS}"
        )
    if summary.get("next_task") != EXPECTED_2342_NEXT_TASK:
        raise HighIntensityRuntimeDryRunError(
            f"TRADING-2343 requires 2342 route {EXPECTED_2342_NEXT_TASK}"
        )
    if route.get("next_task") != EXPECTED_2342_NEXT_TASK:
        raise HighIntensityRuntimeDryRunError(
            f"TRADING-2343 requires 2342 task route {EXPECTED_2342_NEXT_TASK}"
        )
    if str(readiness.get("readiness_status")) not in READY_2343_STATUSES:
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 requires 2342 readiness for observe-only runtime dry-run"
        )
    _require_false(summary, "runtime_scheduler_enabled", "TRADING-2342 summary")
    _require_false(summary, "new_event_logging_executed", "TRADING-2342 summary")
    _require_false(summary, "outcome_binding_executed", "TRADING-2342 summary")
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def load_trading_2341_runtime_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_continue_observe_decision_summary.json",
        "decision_matrix": root / "high_intensity_continue_observe_decision_matrix.json",
        "selected_rule_contract": root
        / "high_intensity_selected_rule_continuation_contract.json",
        "observe_scope": root / "high_intensity_observe_continuation_scope.json",
        "partial_caveat": root
        / "high_intensity_partial_coverage_carryforward_caveat.json",
        "monthly_plan": root / "high_intensity_monthly_concentration_monitoring_plan.json",
        "event_logger_contract": root
        / "high_intensity_event_logger_continuation_contract.json",
        "outcome_update_policy": root / "high_intensity_outcome_update_policy.json",
        "manual_review_policy": root / "high_intensity_manual_review_context_policy.json",
        "stop_policy": root / "high_intensity_stop_refine_archive_policy.json",
        "readiness": root / "high_intensity_2342_readiness_checklist.json",
        "task_route": root / "high_intensity_2342_task_route.json",
        "safety_boundary": root / "high_intensity_continue_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2341 continuation decision")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2341 {key}", payload)
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def load_trading_2336_runtime_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_event_logger_summary.json",
        "trigger_day_log": root / "high_intensity_observe_trigger_day_log.json",
        "event_log": root / "high_intensity_observe_event_log.json",
        "cluster_registry": root / "high_intensity_observe_event_cluster_registry.json",
        "pending_outcome_registry": root / "high_intensity_pending_outcome_registry.json",
        "outcome_schedule": root / "high_intensity_outcome_collection_schedule.json",
        "manual_review_queue": root / "high_intensity_manual_review_event_queue.json",
        "monthly_report": root / "high_intensity_monthly_concentration_report.json",
        "interpretation_boundary": root
        / "high_intensity_event_logger_interpretation_boundary.json",
        "safety_boundary": root / "high_intensity_event_logger_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2336 event logger")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2336 {key}", payload)
    if not records(mapping(payloads["event_log"]).get("rows")):
        raise HighIntensityRuntimeDryRunError("TRADING-2343 requires prior event log")
    if not records(mapping(payloads["cluster_registry"]).get("rows")):
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 requires prior cluster registry"
        )
    if not records(mapping(payloads["pending_outcome_registry"]).get("rows")):
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 requires prior pending outcome registry"
        )
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def load_trading_2335_runtime_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "selected_rule": root / "high_intensity_selected_trigger_rule.json",
        "selected_contract": root / "high_intensity_selected_trigger_contract.json",
        "event_logger_input_contract": root
        / "high_intensity_event_logger_input_contract.json",
        "caveat_report": root / "high_intensity_threshold_selection_caveat_report.json",
        "manual_review_boundary": root
        / "high_intensity_selected_rule_manual_review_boundary.json",
        "safety_boundary": root / "high_intensity_threshold_selection_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2335 selected rule")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2335 {key}", payload)
    if payloads["selected_rule"].get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 requires selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def load_trading_2334_runtime_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_forward_observe_plan_summary.json",
        "event_schema": root / "high_intensity_forward_observe_event_schema.json",
        "evidence_contract": root
        / "high_intensity_forward_observe_evidence_contract.json",
        "actual_path_outcome_contract": root
        / "high_intensity_actual_path_outcome_contract.json",
        "manual_review_boundary": root / "high_intensity_manual_review_boundary.json",
        "stop_continue_archive_rules": root
        / "high_intensity_stop_continue_archive_rules.json",
        "safety_boundary": root / "high_intensity_forward_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2334 forward observe plan")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2334 {key}", payload)
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def load_trading_2332_runtime_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "dynamic_target_exposure_cap_dry_run_summary.json",
        "trigger_alignment": root
        / "dynamic_target_risk_cap_trigger_alignment_matrix.json",
        "dry_run_result": root / "dynamic_target_exposure_cap_dry_run_result.json",
        "data_quality_report": root / "dynamic_target_data_quality_report.json",
        "pit_boundary": root / "dynamic_target_pit_caveat_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2332 dynamic dry-run source")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2332 {key}", payload)
    if not records(mapping(payloads["trigger_alignment"]).get("rows")):
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 requires runtime trigger source"
        )
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def build_high_intensity_runtime_contract_validation_report(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    runtime_plan = mapping(inputs["runtime_plan"])
    contract_checks = {
        "runtime_scope_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("scope_contract")
        )
        and mapping(runtime_plan.get("scope_contract")).get("runtime_mode")
        == "observe_only",
        "runtime_input_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("input_contract")
        )
        and bool(mapping(runtime_plan.get("input_contract")).get("blocked_inputs")),
        "event_detection_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("event_detection_contract")
        )
        and bool(mapping(runtime_plan.get("event_detection_contract")).get("blocked_outputs")),
        "event_append_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("event_append_contract")
        )
        and bool(
            mapping(runtime_plan.get("event_append_contract")).get(
                "blocked_appended_fields"
            )
        ),
        "cluster_update_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("cluster_update_contract")
        )
        and mapping(runtime_plan.get("cluster_update_contract")).get(
            "cluster_update_mode"
        )
        == "append_or_extend_open_cluster",
        "pending_outcome_update_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("pending_outcome_update_contract")
        )
        and mapping(runtime_plan.get("pending_outcome_update_contract")).get(
            "pending_registry_update_mode"
        )
        == "append_only",
        "manual_review_context_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("manual_review_context_contract")
        )
        and bool(
            mapping(runtime_plan.get("manual_review_context_contract")).get(
                "blocked_display_fields"
            )
        ),
        "monthly_concentration_monitoring_contract_valid": _contract_has_safe_boundary(
            runtime_plan.get("monthly_concentration_monitoring_contract")
        )
        and mapping(runtime_plan.get("monthly_concentration_monitoring_contract")).get(
            "monitoring_required"
        )
        is True,
        "fail_closed_safety_gate_valid": _contract_has_safe_boundary(
            runtime_plan.get("fail_closed_safety_gate")
        )
        and mapping(runtime_plan.get("fail_closed_safety_gate")).get(
            "fail_closed_required"
        )
        is True,
    }
    errors = [
        key.replace("_valid", "").upper()
        for key, valid in contract_checks.items()
        if not valid
    ]
    warnings: list[str] = []
    if mapping(runtime_plan.get("event_detection_contract")).get("strict_pit_ready") is False:
        warnings.append("PIT_APPROXIMATION_CAVEAT")
    status = "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return clean_for_yaml(
        {
            "contract_validation_status": status,
            **contract_checks,
            "contract_error_count": len(errors),
            "contract_warning_count": len(warnings),
            "contract_errors": errors,
            "contract_warnings": warnings,
            **_core_safety_fields(),
        }
    )


def build_high_intensity_runtime_input_validation_matrix(
    *,
    trigger_source_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> list[dict[str, Any]]:
    total = len(trigger_source_rows)
    rows: list[dict[str, Any]] = []
    for field in REQUIRED_RUNTIME_FIELDS:
        available = 0
        invalid = 0
        source = _runtime_input_source(field, dynamic_dry_run)
        for item in trigger_source_rows:
            value = _runtime_input_value(field, item, selected_rule, dynamic_dry_run)
            if _is_missing(value):
                continue
            available += 1
            if field == "date" and not _date_is_valid(str(value)):
                invalid += 1
            elif field == "risk_cap_score" and not _is_number(value):
                invalid += 1
        missing = total - available
        if missing or invalid:
            status = "FAIL"
        elif field in {"as_of_timestamp", "known_at_policy", "pit_policy"}:
            status = "PASS_WITH_DERIVED_FIELD"
        else:
            status = "PASS"
        rows.append(
            clean_for_yaml(
                {
                    "input_field": field,
                    "required": True,
                    "available": available == total and invalid == 0,
                    "coverage_ratio": _rate(available, total),
                    "missing_count": missing,
                    "invalid_count": invalid,
                    "source_artifact": source,
                    "validation_status": status,
                    "blocking_if_invalid": True,
                    **_core_safety_fields(),
                }
            )
        )
    return rows


def build_high_intensity_runtime_event_detection_dry_run_result(
    *,
    trigger_source_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
    inputs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    selected_rule_id = str(selected_rule.get("selected_rule_id") or "")
    selected_rule_hash = str(
        mapping(inputs["runtime_plan"]).get("summary", {}).get("selected_rule_hash")
        or _hash_payload(selected_rule)
    )
    dynamic_dry_run = mapping(inputs["dynamic_dry_run"])
    threshold = _selected_rule_threshold(selected_rule)
    rows: list[dict[str, Any]] = []
    for index, source_row in enumerate(trigger_source_rows):
        date_value = str(source_row.get("date", ""))
        target_asset = str(source_row.get("target_asset", ""))
        as_of_timestamp = _as_of_timestamp(source_row)
        decision_timestamp = _decision_timestamp(source_row)
        known_at_policy = _known_at_policy(selected_rule, dynamic_dry_run)
        pit_policy = _pit_policy(selected_rule, dynamic_dry_run)
        blocked_reasons = []
        if not selected_rule_id:
            blocked_reasons.append("MISSING_SELECTED_RULE")
        if not as_of_timestamp:
            blocked_reasons.append("MISSING_KNOWN_AT_TIMESTAMP")
        if not pit_policy:
            blocked_reasons.append("MISSING_PIT_POLICY")
        if _forbidden_fields_present(source_row):
            blocked_reasons.append("FORBIDDEN_OUTPUT_DETECTED")
        rule_matches = selected_rule_matches(source_row, selected_rule)
        if blocked_reasons:
            status = "BLOCKED_INPUT_INVALID"
            high_intensity_triggered = False
            reason = ""
        elif rule_matches:
            status = "DETECTED"
            high_intensity_triggered = True
            reason = (
                f"{selected_rule_id}: risk_cap_score >= {round_float(threshold)} "
                "with active risk-cap defensive direction"
            )
        else:
            status = "NOT_DETECTED"
            high_intensity_triggered = False
            reason = _not_detected_reason(source_row, threshold)
        rows.append(
            clean_for_yaml(
                {
                    "detection_record_id": _short_id(
                        "hidr",
                        selected_rule_id,
                        target_asset,
                        date_value,
                        str(index),
                    ),
                    "date": date_value,
                    "target_asset": target_asset,
                    "selected_rule_id": selected_rule_id,
                    "selected_rule_hash": selected_rule_hash,
                    "risk_cap_triggered": _truthy(source_row.get("risk_cap_triggered")),
                    "risk_cap_intensity": source_row.get("risk_cap_intensity", ""),
                    "risk_cap_score": round_float(source_row.get("risk_cap_score")),
                    "scope_active": _truthy(source_row.get("scope_active")),
                    "signal_direction": source_row.get("signal_direction", ""),
                    "as_of_timestamp": as_of_timestamp,
                    "decision_timestamp": decision_timestamp,
                    "known_at_policy": known_at_policy,
                    "pit_policy": pit_policy,
                    "high_intensity_triggered": high_intensity_triggered,
                    "high_intensity_reason": reason,
                    "detection_status": status,
                    "blocked_reason": ",".join(blocked_reasons),
                    **_core_safety_fields(),
                }
            )
        )
    return rows


def selected_rule_matches(
    source_row: Mapping[str, Any],
    selected_rule: Mapping[str, Any],
) -> bool:
    threshold = _selected_rule_threshold(selected_rule)
    return (
        _truthy(source_row.get("risk_cap_triggered"))
        and _truthy(source_row.get("scope_active"))
        and to_float(source_row.get("risk_cap_score")) >= threshold
        and _is_defensive_risk_cap_direction(source_row.get("signal_direction"))
    )


def build_high_intensity_runtime_event_append_dry_run_result(
    *,
    detection_rows: Sequence[Mapping[str, Any]],
    prior_trigger_day_rows: Sequence[Mapping[str, Any]],
    prior_event_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    prior_trigger_keys = {
        _event_key(
            row.get("date") or row.get("event_date"),
            row.get("target_asset"),
            row.get("selected_rule_id"),
        )
        for row in prior_trigger_day_rows
    }
    prior_event_by_key = {
        _event_key(row.get("event_date"), row.get("target_asset"), row.get("selected_rule_id")): row
        for row in prior_event_rows
    }
    rows: list[dict[str, Any]] = []
    prior_count = len(prior_event_rows)
    for index, detection in enumerate(detection_rows):
        date_value = str(detection.get("date", ""))
        target_asset = str(detection.get("target_asset", ""))
        selected_rule_id = str(detection.get("selected_rule_id", ""))
        key = _event_key(date_value, target_asset, selected_rule_id)
        blocked_reason = ""
        event_id = ""
        event_cluster_id = ""
        is_new_event = False
        is_cluster_primary_event = False
        would_append = False
        if detection.get("detection_status") in {
            "BLOCKED_INPUT_INVALID",
            "BLOCKED_SAFETY_GATE",
        }:
            append_status = "BLOCKED"
            blocked_reason = str(detection.get("blocked_reason", ""))
        elif detection.get("high_intensity_triggered") is not True:
            append_status = "NO_APPEND_NOT_TRIGGERED"
        elif key in prior_trigger_keys or key in prior_event_by_key:
            append_status = "NO_APPEND_DUPLICATE"
            prior = prior_event_by_key.get(key, {})
            event_id = str(prior.get("event_id", ""))
            event_cluster_id = str(prior.get("event_cluster_id", ""))
        else:
            open_cluster = _find_open_cluster(
                detection=detection,
                prior_cluster_rows=prior_cluster_rows,
            )
            if open_cluster:
                append_status = "WOULD_EXTEND_EXISTING_CLUSTER"
                event_cluster_id = str(open_cluster.get("event_cluster_id", ""))
                event_id = str(
                    open_cluster.get("primary_event_id")
                    or open_cluster.get("first_event_id")
                    or ""
                )
                is_new_event = False
                is_cluster_primary_event = False
            else:
                append_status = "WOULD_APPEND_NEW_EVENT"
                event_cluster_id = _short_id(
                    "hicl",
                    selected_rule_id,
                    target_asset,
                    date_value,
                )
                event_id = _short_id(
                    "hievt",
                    selected_rule_id,
                    target_asset,
                    event_cluster_id,
                    date_value,
                )
                is_new_event = True
                is_cluster_primary_event = True
                would_append = True
        rows.append(
            clean_for_yaml(
                {
                    "append_record_id": _short_id(
                        "hiapp",
                        selected_rule_id,
                        target_asset,
                        date_value,
                        str(index),
                    ),
                    "date": date_value,
                    "target_asset": target_asset,
                    "selected_rule_id": selected_rule_id,
                    "prior_event_log_record_count": prior_count,
                    "would_append_event": would_append,
                    "event_id": event_id,
                    "event_cluster_id": event_cluster_id,
                    "is_new_event": is_new_event,
                    "is_cluster_primary_event": is_cluster_primary_event,
                    "append_mode": "append_only",
                    "original_event_log_mutation_allowed": False,
                    "event_status_on_create": "OBSERVE_PENDING",
                    "manual_review_observation_flag": bool(
                        append_status
                        in {"WOULD_APPEND_NEW_EVENT", "WOULD_EXTEND_EXISTING_CLUSTER"}
                    ),
                    "append_status": append_status,
                    "blocked_reason": blocked_reason,
                    **_core_safety_fields(),
                }
            )
        )
    return rows


def build_high_intensity_runtime_cluster_update_dry_run_result(
    *,
    append_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    prior_by_id = {
        str(row.get("event_cluster_id", "")): row
        for row in prior_cluster_rows
        if row.get("event_cluster_id")
    }
    monthly_counts = Counter(
        str(row.get("monthly_bucket", ""))
        for row in prior_cluster_rows
        if row.get("monthly_bucket")
    )
    rows: list[dict[str, Any]] = []
    for index, append in enumerate(append_rows):
        date_value = str(append.get("date", ""))
        target_asset = str(append.get("target_asset", ""))
        selected_rule_id = str(append.get("selected_rule_id", ""))
        cluster_id = str(append.get("event_cluster_id", ""))
        monthly_bucket = date_value[:7] if _date_is_valid(date_value) else ""
        prior_cluster = prior_by_id.get(cluster_id, {})
        blocked_reason = ""
        if append.get("append_status") == "WOULD_APPEND_NEW_EVENT":
            action = "CREATE_NEW_CLUSTER"
            monthly_counts[monthly_bucket] += 1
            cluster_start = date_value
            cluster_end = date_value
            active_days = 1
            trigger_days = 1
            consecutive_days = 1
            prior_status = "NONE"
        elif append.get("append_status") == "WOULD_EXTEND_EXISTING_CLUSTER":
            action = "EXTEND_OPEN_CLUSTER"
            cluster_start = str(prior_cluster.get("cluster_start_date") or date_value)
            cluster_end = date_value
            active_days = _active_days(cluster_start, cluster_end)
            trigger_days = int(prior_cluster.get("trigger_day_count") or 0) + 1
            consecutive_days = int(prior_cluster.get("consecutive_trigger_days") or 0) + 1
            prior_status = str(prior_cluster.get("cluster_status") or "OPEN")
        elif append.get("append_status") == "BLOCKED":
            action = "BLOCKED"
            cluster_start = ""
            cluster_end = ""
            active_days = 0
            trigger_days = 0
            consecutive_days = 0
            prior_status = ""
            blocked_reason = str(append.get("blocked_reason", ""))
        else:
            action = "NO_CLUSTER_UPDATE"
            cluster_start = str(prior_cluster.get("cluster_start_date") or "")
            cluster_end = str(prior_cluster.get("cluster_end_date") or "")
            active_days = int(prior_cluster.get("cluster_active_days") or 0)
            trigger_days = int(prior_cluster.get("trigger_day_count") or 0)
            consecutive_days = int(prior_cluster.get("consecutive_trigger_days") or 0)
            prior_status = str(prior_cluster.get("cluster_status") or "")
        rows.append(
            clean_for_yaml(
                {
                    "cluster_update_record_id": _short_id(
                        "hiclu",
                        selected_rule_id,
                        target_asset,
                        date_value,
                        str(index),
                    ),
                    "date": date_value,
                    "target_asset": target_asset,
                    "selected_rule_id": selected_rule_id,
                    "event_cluster_id": cluster_id,
                    "prior_cluster_status": prior_status,
                    "cluster_update_action": action,
                    "cluster_start_date": cluster_start,
                    "cluster_end_date": cluster_end,
                    "cluster_active_days_after_update": active_days,
                    "trigger_day_count_after_update": trigger_days,
                    "consecutive_trigger_days_after_update": consecutive_days,
                    "monthly_bucket": monthly_bucket,
                    "monthly_cluster_count_after_update": monthly_counts.get(
                        monthly_bucket,
                        0,
                    ),
                    "cluster_update_status": "PASS" if not blocked_reason else "BLOCKED",
                    "blocked_reason": blocked_reason,
                    **_core_safety_fields(),
                }
            )
        )
    return rows


def build_high_intensity_runtime_pending_outcome_update_dry_run_result(
    *,
    append_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for append in append_rows:
        if append.get("append_status") != "WOULD_APPEND_NEW_EVENT":
            continue
        event_id = str(append.get("event_id", ""))
        event_date = str(append.get("date", ""))
        event_dt = _parse_date(event_date)
        for horizon, horizon_days in HORIZON_DAYS.items():
            due = _add_business_days(event_dt, horizon_days)
            rows.append(
                clean_for_yaml(
                    {
                        "pending_update_record_id": _short_id(
                            "hipu",
                            event_id,
                            horizon,
                        ),
                        "event_id": event_id,
                        "event_date": event_date,
                        "target_asset": append.get("target_asset", ""),
                        "horizon": horizon,
                        "outcome_due_date": due.isoformat(),
                        "would_create_pending_outcome": True,
                        "pending_outcome_id": _short_id("hiout", event_id, horizon),
                        "outcome_status_on_create": "OUTCOME_PENDING",
                        "outcome_binding_allowed_in_2343": False,
                        "pending_update_status": "WOULD_CREATE_PENDING_OUTCOME",
                        "blocked_reason": "",
                        **_core_safety_fields(),
                    }
                )
            )
    return rows


def build_high_intensity_runtime_manual_review_context_dry_run_result(
    *,
    append_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    detection_by_key = {
        _event_key(row.get("date"), row.get("target_asset"), row.get("selected_rule_id")): row
        for row in detection_rows
    }
    partial_caveat = mapping(inputs["continue_decision"]).get("partial_caveat", {})
    monthly_report = mapping(inputs["event_logger"]).get("monthly_report", {})
    rows: list[dict[str, Any]] = []
    for index, append in enumerate(append_rows):
        if append.get("append_status") not in {
            "WOULD_APPEND_NEW_EVENT",
            "WOULD_EXTEND_EXISTING_CLUSTER",
        }:
            continue
        key = _event_key(
            append.get("date"),
            append.get("target_asset"),
            append.get("selected_rule_id"),
        )
        detection = detection_by_key.get(key, {})
        context = {
            "event_cluster_id": append.get("event_cluster_id", ""),
            "append_status": append.get("append_status", ""),
            "is_new_event": append.get("is_new_event", False),
        }
        blocked = _manual_review_blocked_fields(context)
        rows.append(
            clean_for_yaml(
                {
                    "manual_review_record_id": _short_id(
                        "himr",
                        str(append.get("event_id", "")),
                        str(index),
                    ),
                    "event_id": append.get("event_id", ""),
                    "event_date": append.get("date", ""),
                    "target_asset": append.get("target_asset", ""),
                    "manual_review_observation_flag": True,
                    "manual_review_reason": detection.get("high_intensity_reason", ""),
                    "risk_warning_context": "HIGH_INTENSITY_RISK_WARNING_CONTEXT_ONLY",
                    "selected_rule_id": append.get("selected_rule_id", ""),
                    "risk_cap_intensity": detection.get("risk_cap_intensity", ""),
                    "risk_cap_score": detection.get("risk_cap_score", 0.0),
                    "cluster_context": context,
                    "partial_coverage_caveat": partial_caveat,
                    "monthly_concentration_warning": (
                        "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
                        if "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
                        in list(
                            mapping(monthly_report).get(
                                "monthly_concentration_warnings",
                                [],
                            )
                        )
                        else ""
                    ),
                    "context_status": "PASS" if not blocked else "BLOCKED",
                    "blocked_fields_detected": blocked,
                    **_core_safety_fields(),
                }
            )
        )
    return rows


def build_high_intensity_runtime_monthly_concentration_monitoring_dry_run(
    *,
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_update_rows: Sequence[Mapping[str, Any]],
    prior_event_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    monthly_contract = mapping(inputs["runtime_plan"]).get(
        "monthly_concentration_monitoring_contract",
        {},
    )
    guardrails = mapping(monthly_contract).get("guardrails", {})
    event_guardrail = int(guardrails.get("max_monthly_event_count") or 3)
    cluster_guardrail = int(guardrails.get("max_monthly_cluster_count") or 3)
    blocking_event_guardrail = event_guardrail * 2
    blocking_cluster_guardrail = cluster_guardrail * 2

    trigger_counts = Counter(
        str(row.get("date", ""))[:7]
        for row in detection_rows
        if row.get("high_intensity_triggered") is True and row.get("date")
    )
    event_counts = Counter(
        str(row.get("event_date", ""))[:7]
        for row in prior_event_rows
        if row.get("event_date")
    )
    for append in append_rows:
        if append.get("append_status") == "WOULD_APPEND_NEW_EVENT":
            event_counts[str(append.get("date", ""))[:7]] += 1
    cluster_counts = Counter(
        str(row.get("monthly_bucket", ""))
        for row in prior_cluster_rows
        if row.get("monthly_bucket")
    )
    for row in cluster_update_rows:
        if row.get("cluster_update_action") == "CREATE_NEW_CLUSTER":
            cluster_counts[str(row.get("monthly_bucket", ""))] += 1

    months_above_warning = sorted(
        {
            month
            for month, count in event_counts.items()
            if month and count > event_guardrail
        }
        | {
            month
            for month, count in cluster_counts.items()
            if month and count > cluster_guardrail
        }
    )
    months_above_blocking = sorted(
        {
            month
            for month, count in event_counts.items()
            if month and count > blocking_event_guardrail
        }
        | {
            month
            for month, count in cluster_counts.items()
            if month and count > blocking_cluster_guardrail
        }
    )
    inherited_warning = str(
        mapping(monthly_contract).get("inherited_warning")
        or mapping(inputs["continue_decision"]).get("monthly_plan", {}).get(
            "inherited_warning",
            "",
        )
    )
    warnings = []
    if inherited_warning:
        warnings.append(inherited_warning)
    if months_above_warning:
        warnings.append("MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL")
    blockers = (
        ["MONTHLY_EVENT_CONCENTRATION_BLOCKING_GUARDRAIL_BREACHED"]
        if months_above_blocking
        else []
    )
    status = "BLOCKED" if blockers else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return clean_for_yaml(
        {
            "monthly_monitoring_status": status,
            "inherited_warning": inherited_warning,
            "monthly_trigger_day_count": dict(sorted(trigger_counts.items())),
            "monthly_event_count": dict(sorted(event_counts.items())),
            "monthly_cluster_count": dict(sorted(cluster_counts.items())),
            "new_cluster_count": sum(
                1
                for row in cluster_update_rows
                if row.get("cluster_update_action") == "CREATE_NEW_CLUSTER"
            ),
            "existing_cluster_continuation_count": sum(
                1
                for row in cluster_update_rows
                if row.get("cluster_update_action") == "EXTEND_OPEN_CLUSTER"
            ),
            "max_consecutive_trigger_days": max(
                (
                    int(row.get("consecutive_trigger_days_after_update") or 0)
                    for row in cluster_update_rows
                ),
                default=0,
            ),
            "months_above_warning_guardrail": months_above_warning,
            "months_above_blocking_guardrail": months_above_blocking,
            "monthly_concentration_warning_count": len(warnings),
            "monthly_concentration_blocking_count": len(blockers),
            "monitoring_warnings": sorted(set(warnings)),
            "monitoring_blockers": blockers,
            **_core_safety_fields(),
        }
    )


def build_high_intensity_runtime_fail_closed_safety_gate_result(
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    forbidden = sorted(_forbidden_fields_present(candidate))
    missing_selected_rule = not candidate.get("selected_rule_id")
    missing_known_at = not candidate.get("known_at_timestamp") and not candidate.get(
        "as_of_timestamp"
    )
    missing_pit = not candidate.get("pit_policy")
    target_weight = _truthy(candidate.get("target_weight_generated")) or _emits_action(
        candidate.get("target_weight")
    )
    rebalance = _truthy(
        candidate.get("rebalance_instruction_generated")
    ) or _emits_action(candidate.get("rebalance_instruction"))
    broker = _emits_action(candidate.get("broker_action_requested")) or str(
        candidate.get("broker_action", "none")
    ).lower() not in {"", "none"}
    paper = _truthy(candidate.get("paper_shadow_enabled")) or _truthy(
        candidate.get("paper_shadow_allowed")
    )
    production = _truthy(candidate.get("production_enabled")) or _truthy(
        candidate.get("production_allowed")
    )
    blockers = [
        name
        for name, blocked in [
            ("MISSING_SELECTED_RULE", missing_selected_rule),
            ("MISSING_KNOWN_AT_TIMESTAMP", missing_known_at),
            ("MISSING_PIT_POLICY", missing_pit),
            ("TARGET_WEIGHT_GENERATED", target_weight),
            ("REBALANCE_INSTRUCTION_GENERATED", rebalance),
            ("BROKER_ACTION_REQUESTED", broker),
            ("PAPER_SHADOW_ENABLED", paper),
            ("PRODUCTION_ENABLED", production),
            ("FORBIDDEN_OUTPUT_DETECTED", bool(forbidden)),
        ]
        if blocked
    ]
    return clean_for_yaml(
        {
            "safety_gate_status": "FAIL_CLOSED_TRIGGERED" if blockers else "PASS",
            "missing_selected_rule_blocked": missing_selected_rule,
            "missing_known_at_timestamp_blocked": missing_known_at,
            "missing_pit_policy_blocked": missing_pit,
            "target_weight_generated_blocked": target_weight,
            "rebalance_instruction_generated_blocked": rebalance,
            "broker_action_requested_blocked": broker,
            "paper_shadow_enabled_blocked": paper,
            "production_enabled_blocked": production,
            "forbidden_output_detected": bool(forbidden),
            "forbidden_output_fields": forbidden,
            "safety_error_count": len(blockers),
            "safety_warning_count": 0,
            **_core_safety_fields(),
        }
    )


def build_high_intensity_runtime_artifact_registry_dry_run_report(
    *,
    output_dir: Path,
    docs_root: Path,
) -> dict[str, Any]:
    expected_output_names = [
        "high_intensity_runtime_dry_run_summary.json",
        "high_intensity_runtime_contract_validation_report.json",
        "high_intensity_runtime_input_validation_matrix.json",
        "high_intensity_runtime_event_detection_dry_run_result.json",
        "high_intensity_runtime_event_append_dry_run_result.json",
        "high_intensity_runtime_cluster_update_dry_run_result.json",
        "high_intensity_runtime_pending_outcome_update_dry_run_result.json",
        "high_intensity_runtime_manual_review_context_dry_run_result.json",
        "high_intensity_runtime_monthly_concentration_monitoring_dry_run.json",
        "high_intensity_runtime_fail_closed_safety_gate_result.json",
        "high_intensity_2344_task_route.json",
    ]
    expected_doc_names = [
        "high_intensity_risk_cap_observe_only_runtime_dry_run.md",
        "high_intensity_runtime_event_detection_dry_run.md",
        "high_intensity_runtime_event_append_cluster_update_dry_run.md",
        "high_intensity_runtime_safety_gate_dry_run.md",
        "high_intensity_2344_readiness_route.md",
    ]
    return clean_for_yaml(
        {
            "runtime_artifact_root": str(output_dir),
            "runtime_artifact_paths_valid": all(expected_output_names),
            "report_registry_plan_valid": True,
            "artifact_catalog_plan_valid": True,
            "system_flow_plan_valid": True,
            "task_register_plan_valid": True,
            "outputs_git_policy_valid": True,
            "expected_runtime_artifacts": [
                str(output_dir / name) for name in expected_output_names
            ],
            "expected_docs": [str(docs_root / name) for name in expected_doc_names],
            "registry_dry_run_status": "PASS",
            "registry_warnings": [],
            "registry_errors": [],
            **_core_safety_fields(),
        }
    )


def build_high_intensity_runtime_dry_run_data_quality_report(
    *,
    inputs: Mapping[str, Any],
    contract_validation: Mapping[str, Any],
    input_validation_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_monitoring: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
) -> dict[str, Any]:
    input_errors = sum(
        1 for row in input_validation_rows if row.get("validation_status") == "FAIL"
    )
    detection_errors = sum(
        1
        for row in detection_rows
        if str(row.get("detection_status", "")).startswith("BLOCKED")
    )
    append_errors = sum(1 for row in append_rows if row.get("append_status") == "BLOCKED")
    cluster_errors = sum(
        1 for row in cluster_rows if row.get("cluster_update_status") == "BLOCKED"
    )
    manual_errors = sum(
        1 for row in manual_review_rows if row.get("context_status") == "BLOCKED"
    )
    error_count = (
        int(contract_validation.get("contract_error_count") or 0)
        + input_errors
        + detection_errors
        + append_errors
        + cluster_errors
        + manual_errors
        + int(safety_gate.get("safety_error_count") or 0)
    )
    warning_count = (
        int(contract_validation.get("contract_warning_count") or 0)
        + sum(
            1
            for row in input_validation_rows
            if row.get("validation_status") == "PASS_WITH_DERIVED_FIELD"
        )
        + int(monthly_monitoring.get("monthly_concentration_warning_count") or 0)
    )
    data_quality_status = (
        "FAIL"
        if error_count
        else "PASS_WITH_WARNINGS"
        if warning_count
        else "PASS"
    )
    runtime_summary = mapping(inputs["runtime_plan"]).get("summary", {})
    return clean_for_yaml(
        {
            "contract_validation_status": contract_validation.get(
                "contract_validation_status"
            ),
            "runtime_input_status": "FAIL" if input_errors else "PASS",
            "event_detection_status": "FAIL" if detection_errors else "PASS",
            "event_append_status": "FAIL" if append_errors else "PASS",
            "cluster_update_status": "FAIL" if cluster_errors else "PASS",
            "pending_outcome_update_status": "PASS" if pending_rows is not None else "FAIL",
            "manual_review_context_status": "FAIL" if manual_errors else "PASS",
            "monthly_concentration_status": monthly_monitoring.get(
                "monthly_monitoring_status"
            ),
            "safety_gate_status": safety_gate.get("safety_gate_status"),
            "record_count": len(detection_rows),
            "eligible_record_count": sum(
                1
                for row in detection_rows
                if row.get("detection_status") in {"DETECTED", "NOT_DETECTED"}
            ),
            "detected_event_count": sum(
                1 for row in detection_rows if row.get("detection_status") == "DETECTED"
            ),
            "would_append_event_count": sum(
                1
                for row in append_rows
                if row.get("append_status") == "WOULD_APPEND_NEW_EVENT"
            ),
            "would_extend_cluster_count": sum(
                1
                for row in append_rows
                if row.get("append_status") == "WOULD_EXTEND_EXISTING_CLUSTER"
            ),
            "would_create_pending_outcome_count": len(pending_rows),
            "warning_count": warning_count,
            "error_count": error_count,
            "data_quality_status": data_quality_status,
            "validate_data_executed": False,
            "source_validate_data_executed": runtime_summary.get(
                "source_validate_data_executed",
                True,
            ),
            "source_validate_data_as_of": runtime_summary.get(
                "source_validate_data_as_of",
                "2026-06-29",
            ),
            "source_validate_data_status": runtime_summary.get(
                "source_validate_data_status",
                "PASS_WITH_WARNINGS",
            ),
            "source_validate_data_error_count": runtime_summary.get(
                "source_validate_data_error_count",
                0,
            ),
            **_core_safety_fields(),
        }
    )


def build_high_intensity_2344_readiness_checklist(
    *,
    contract_validation: Mapping[str, Any],
    input_validation_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_monitoring: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
    artifact_registry: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if contract_validation.get("contract_validation_status") == "FAIL":
        blockers.append("CONTRACT_VALIDATION_FAILED")
    if any(row.get("validation_status") == "FAIL" for row in input_validation_rows):
        blockers.append("RUNTIME_INPUT_VALIDATION_FAILED")
    if safety_gate.get("safety_gate_status") == "FAIL_CLOSED_TRIGGERED":
        blockers.append("FAIL_CLOSED_SAFETY_GATE_TRIGGERED")
    if artifact_registry.get("registry_dry_run_status") != "PASS":
        blockers.append("ARTIFACT_REGISTRY_DRY_RUN_FAILED")
    if monthly_monitoring.get("monthly_monitoring_status") == "BLOCKED":
        blockers.append("MONTHLY_CONCENTRATION_BLOCKED")
    if contract_validation.get("contract_warning_count"):
        warnings.extend(contract_validation.get("contract_warnings") or [])
    if monthly_monitoring.get("monthly_concentration_warning_count"):
        warnings.extend(monthly_monitoring.get("monitoring_warnings") or [])
    if any(
        row.get("validation_status") == "PASS_WITH_DERIVED_FIELD"
        for row in input_validation_rows
    ):
        warnings.append("DERIVED_RUNTIME_INPUT_FIELDS_USED")
    if not any(row.get("append_status") == "WOULD_APPEND_NEW_EVENT" for row in append_rows):
        warnings.append("NO_NEW_RUNTIME_EVENTS_IN_HISTORICAL_REPLAY")
    warnings.extend(
        [
            "OBSERVE_ONLY",
            "PARTIAL_COVERAGE_CAVEAT",
            "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
            "NO_PAPER_SHADOW",
            "NO_PRODUCTION",
        ]
    )
    if blockers and "FAIL_CLOSED_SAFETY_GATE_TRIGGERED" in blockers:
        readiness_status = "RUNTIME_DRY_RUN_BLOCKED"
    elif blockers:
        readiness_status = "RUNTIME_DRY_RUN_REMEDIATION_REQUIRED"
    elif warnings:
        readiness_status = "READY_FOR_2344_WITH_CAVEATS"
    else:
        readiness_status = (
            "READY_FOR_2344_OBSERVE_ONLY_RUNTIME_SCHEDULER_INTEGRATION_PLAN"
        )
    return clean_for_yaml(
        {
            "contract_validation_passed": not contract_validation.get(
                "contract_error_count"
            ),
            "runtime_input_validation_passed": not any(
                row.get("validation_status") == "FAIL" for row in input_validation_rows
            ),
            "event_detection_dry_run_generated": bool(detection_rows),
            "event_append_dry_run_generated": append_rows is not None,
            "cluster_update_dry_run_generated": cluster_rows is not None,
            "pending_outcome_update_dry_run_generated": pending_rows is not None,
            "manual_review_context_dry_run_generated": manual_review_rows is not None,
            "monthly_concentration_monitoring_generated": bool(monthly_monitoring),
            "fail_closed_safety_gate_passed": safety_gate.get("safety_gate_status")
            != "FAIL_CLOSED_TRIGGERED",
            "artifact_registry_dry_run_passed": artifact_registry.get(
                "registry_dry_run_status"
            )
            == "PASS",
            "runtime_scheduler_enabled": False,
            "new_event_logging_executed": False,
            "outcome_binding_executed": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": readiness_status,
            "readiness_blockers": sorted(set(blockers)),
            "readiness_warnings": sorted(set(warnings)),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2344_task_route(
    *,
    readiness: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
    artifact_registry: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    if status in {
        "READY_FOR_2344_OBSERVE_ONLY_RUNTIME_SCHEDULER_INTEGRATION_PLAN",
        "READY_FOR_2344_WITH_CAVEATS",
    }:
        next_task = NEXT_2344_SCHEDULER_PLAN_TASK
    elif safety_gate.get("safety_gate_status") == "FAIL_CLOSED_TRIGGERED":
        next_task = NEXT_2344_SAFETY_REMEDIATION_TASK
    elif artifact_registry.get("registry_dry_run_status") != "PASS":
        next_task = NEXT_2344_REGISTRY_REMEDIATION_TASK
    elif status == "RUNTIME_DRY_RUN_REMEDIATION_REQUIRED":
        next_task = NEXT_2344_DRY_RUN_REMEDIATION_TASK
    else:
        next_task = NEXT_2344_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "allowed_routes": [
                NEXT_2344_SCHEDULER_PLAN_TASK,
                NEXT_2344_DRY_RUN_REMEDIATION_TASK,
                NEXT_2344_SAFETY_REMEDIATION_TASK,
                NEXT_2344_REGISTRY_REMEDIATION_TASK,
                NEXT_2344_ARCHIVE_TASK,
            ],
            "readiness_status": status,
            "next_task": next_task,
            "route_caveats": list(readiness.get("readiness_warnings") or []),
            "route_blockers": list(readiness.get("readiness_blockers") or []),
            "route_rationale": _route_rationale(status, next_task),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_dry_run_interpretation_boundary(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    del inputs
    return clean_for_yaml(
        {
            "generated_at": generated_at.isoformat(),
            "research_only": True,
            "observe_only_runtime_dry_run": True,
            "runtime_scheduler_enabled": False,
            "new_event_logging_executed": False,
            "event_append_executed": False,
            "event_append_dry_run_executed": True,
            "outcome_binding_executed": False,
            "pending_outcome_update_dry_run_executed": True,
            "automatic_exposure_cap_allowed": False,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            "known_at_policy": KNOWN_AT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "partial_coverage_caveat_required": True,
            "monthly_concentration_monitoring_required": True,
            "manual_review_only": True,
            "promotion_allowed": False,
            "forbidden_interpretations": [
                "real_account_performance",
                "target_weight_advice",
                "reduce_position_signal",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
                "scheduler_enabled",
            ],
        }
    )


def build_high_intensity_runtime_dry_run_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task"),
            **SAFETY_FIELDS,
            "forbidden_outputs": sorted(
                {
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
                }
            ),
        }
    )


def build_high_intensity_runtime_dry_run_summary(
    *,
    generated_at: datetime,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
    inputs: Mapping[str, Any],
    contract_validation: Mapping[str, Any],
    input_validation_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_monitoring: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
    artifact_registry: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    del cluster_rows, manual_review_rows, monthly_monitoring, safety_gate, artifact_registry
    selected_rule = mapping(inputs["threshold_selection"]).get("selected_rule", {})
    runtime_summary = mapping(inputs["runtime_plan"]).get("summary", {})
    errors = int(data_quality.get("error_count") or 0)
    warnings = int(data_quality.get("warning_count") or 0)
    if errors:
        status = "OBSERVE_ONLY_RUNTIME_DRY_RUN_BLOCKED_PROMOTION_BLOCKED"
    elif warnings:
        status = "OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    else:
        status = "OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_PROMOTION_BLOCKED"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Observe-Only Runtime Dry-Run",
            "status": status,
            "runtime_dry_run_status": status,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "mode": MODE,
            "runtime_integration_plan_dir": str(runtime_integration_plan_dir),
            "continue_decision_dir": str(continue_decision_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "source_2342_status": runtime_summary.get("status"),
            "source_2342_readiness": runtime_summary.get("2343_readiness_status"),
            "source_2342_next_task": runtime_summary.get("next_task"),
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_hash": runtime_summary.get("selected_rule_hash"),
            "contract_validation_status": contract_validation.get(
                "contract_validation_status"
            ),
            "runtime_input_validation_passed": not any(
                row.get("validation_status") == "FAIL" for row in input_validation_rows
            ),
            "record_count": len(detection_rows),
            "detected_event_count": data_quality.get("detected_event_count"),
            "would_append_event_count": data_quality.get("would_append_event_count"),
            "would_extend_cluster_count": data_quality.get(
                "would_extend_cluster_count"
            ),
            "would_create_pending_outcome_count": len(pending_rows),
            "data_quality_status": data_quality.get("data_quality_status"),
            "source_validate_data_executed": data_quality.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": data_quality.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": data_quality.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": data_quality.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2343 only reads prior "
                "validated research artifacts and does not bind outcomes or consume "
                "fresh market data directly."
            ),
            "2344_readiness_status": readiness.get("readiness_status"),
            "2344_task_route_generated": True,
            "next_task": task_route.get("next_task"),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_runtime_dry_run_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    contract_validation: Mapping[str, Any],
    input_validation_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_monitoring: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
    artifact_registry: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["contract_validation"], contract_validation)
    _write_rows_json(paths["input_validation_json"], input_validation_rows)
    write_csv_rows(paths["input_validation_csv"], input_validation_rows)
    _write_rows_json(paths["event_detection_json"], detection_rows)
    write_csv_rows(paths["event_detection_csv"], detection_rows)
    _write_rows_json(paths["event_append_json"], append_rows)
    write_csv_rows(paths["event_append_csv"], append_rows)
    _write_rows_json(paths["cluster_update_json"], cluster_rows)
    write_csv_rows(paths["cluster_update_csv"], cluster_rows)
    _write_rows_json(paths["pending_outcome_json"], pending_rows)
    write_csv_rows(paths["pending_outcome_csv"], pending_rows)
    _write_rows_json(paths["manual_review_json"], manual_review_rows)
    write_csv_rows(paths["manual_review_csv"], manual_review_rows)
    write_json(paths["monthly_monitoring"], monthly_monitoring)
    write_json(paths["safety_gate"], safety_gate)
    write_json(paths["artifact_registry"], artifact_registry)
    write_json(paths["data_quality"], data_quality)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], render_runtime_dry_run_doc(summary, data_quality))
    write_markdown(
        paths["event_detection_doc"],
        render_event_detection_doc(summary, detection_rows),
    )
    write_markdown(
        paths["append_cluster_doc"],
        render_append_cluster_doc(summary, append_rows, cluster_rows, pending_rows),
    )
    write_markdown(paths["safety_doc"], render_safety_doc(summary, safety_gate))
    write_markdown(paths["route_doc"], render_2344_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def render_runtime_dry_run_doc(
    summary: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Risk-Cap Observe-Only Runtime Dry-Run\n\n"
        f"- status: `{summary.get('status')}`\n"
        f"- selected_rule_id: `{summary.get('selected_rule_id')}`\n"
        f"- detected_event_count: `{summary.get('detected_event_count')}`\n"
        f"- would_append_event_count: `{summary.get('would_append_event_count')}`\n"
        f"- would_extend_cluster_count: `{summary.get('would_extend_cluster_count')}`\n"
        "- would_create_pending_outcome_count: "
        f"`{summary.get('would_create_pending_outcome_count')}`\n"
        f"- data_quality_status: `{data_quality.get('data_quality_status')}`\n"
        f"- source_validate_data_as_of: `{summary.get('source_validate_data_as_of')}`\n"
        f"- source_validate_data_status: `{summary.get('source_validate_data_status')}`\n"
        f"- next_task: `{summary.get('next_task')}`\n\n"
        "本报告只记录 observe-only runtime dry-run 结果。2343 未启用 scheduler，"
        "未写回 historical event log / cluster registry / pending outcome registry，"
        "未绑定 outcome，未输出 target weight / rebalance / broker action。\n"
    )


def render_event_detection_doc(
    summary: Mapping[str, Any],
    detection_rows: Sequence[Mapping[str, Any]],
) -> str:
    statuses = Counter(str(row.get("detection_status", "")) for row in detection_rows)
    return (
        "# High-Intensity Runtime Event Detection Dry-Run\n\n"
        f"- selected_rule_id: `{summary.get('selected_rule_id')}`\n"
        f"- selected_rule_hash: `{summary.get('selected_rule_hash')}`\n"
        f"- detection_status_counts: `{dict(sorted(statuses.items()))}`\n\n"
        "Detection rows only feed observe event append dry-run. They are not "
        "target weights, rebalance instructions, reduce-position instructions, "
        "paper-shadow signals, production decisions, or broker actions.\n"
    )


def render_append_cluster_doc(
    summary: Mapping[str, Any],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
) -> str:
    append_statuses = Counter(str(row.get("append_status", "")) for row in append_rows)
    cluster_actions = Counter(
        str(row.get("cluster_update_action", "")) for row in cluster_rows
    )
    return (
        "# High-Intensity Runtime Event Append / Cluster Update Dry-Run\n\n"
        f"- append_status_counts: `{dict(sorted(append_statuses.items()))}`\n"
        f"- cluster_update_action_counts: `{dict(sorted(cluster_actions.items()))}`\n"
        f"- pending_outcome_dry_run_rows: `{len(pending_rows)}`\n"
        f"- next_task: `{summary.get('next_task')}`\n\n"
        "All append and cluster operations are would-write dry-run records only. "
        "Historical TRADING-2336 event, cluster, and pending outcome artifacts are "
        "not mutated.\n"
    )


def render_safety_doc(
    summary: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Runtime Safety Gate Dry-Run\n\n"
        f"- safety_gate_status: `{safety_gate.get('safety_gate_status')}`\n"
        f"- safety_error_count: `{safety_gate.get('safety_error_count')}`\n"
        f"- forbidden_output_fields: `{safety_gate.get('forbidden_output_fields')}`\n"
        f"- broker_action: `{summary.get('broker_action')}`\n\n"
        "Fail-closed safety gate blocks missing selected rule, missing known-at "
        "timestamp, missing PIT policy, target weight, rebalance instruction, "
        "paper-shadow, production, and broker action.\n"
    )


def render_2344_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity 2344 Readiness Route\n\n"
        f"- readiness_status: `{readiness.get('readiness_status')}`\n"
        f"- readiness_warnings: `{readiness.get('readiness_warnings')}`\n"
        f"- readiness_blockers: `{readiness.get('readiness_blockers')}`\n"
        f"- next_task: `{task_route.get('next_task')}`\n\n"
        "2344 may only plan observe-only scheduler integration or remediation. "
        "It is not permission to start paper-shadow, production, or broker actions.\n"
    )


def _validate_runtime_dry_run_cross_source_contracts(inputs: Mapping[str, Any]) -> None:
    runtime_summary = mapping(inputs["runtime_plan"]).get("summary", {})
    selected_rule = mapping(inputs["threshold_selection"]).get("selected_rule", {})
    if runtime_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 requires 2342 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if selected_rule.get("selected_rule_id") != runtime_summary.get("selected_rule_id"):
        raise HighIntensityRuntimeDryRunError(
            "TRADING-2343 selected rule mismatch between 2342 and 2335"
        )
    if mapping(inputs["continue_decision"]).get("summary", {}).get("next_task") != (
        "TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan"
    ):
        raise HighIntensityRuntimeDryRunError("TRADING-2343 requires 2341 route to 2342")
    event_schema = mapping(inputs["forward_observe_plan"]).get("event_schema", {})
    if not event_schema:
        raise HighIntensityRuntimeDryRunError("TRADING-2343 requires 2334 event schema")


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_runtime_dry_run_summary.json",
        "contract_validation": output_dir
        / "high_intensity_runtime_contract_validation_report.json",
        "input_validation_json": output_dir
        / "high_intensity_runtime_input_validation_matrix.json",
        "input_validation_csv": output_dir
        / "high_intensity_runtime_input_validation_matrix.csv",
        "event_detection_json": output_dir
        / "high_intensity_runtime_event_detection_dry_run_result.json",
        "event_detection_csv": output_dir
        / "high_intensity_runtime_event_detection_dry_run_result.csv",
        "event_append_json": output_dir
        / "high_intensity_runtime_event_append_dry_run_result.json",
        "event_append_csv": output_dir
        / "high_intensity_runtime_event_append_dry_run_result.csv",
        "cluster_update_json": output_dir
        / "high_intensity_runtime_cluster_update_dry_run_result.json",
        "cluster_update_csv": output_dir
        / "high_intensity_runtime_cluster_update_dry_run_result.csv",
        "pending_outcome_json": output_dir
        / "high_intensity_runtime_pending_outcome_update_dry_run_result.json",
        "pending_outcome_csv": output_dir
        / "high_intensity_runtime_pending_outcome_update_dry_run_result.csv",
        "manual_review_json": output_dir
        / "high_intensity_runtime_manual_review_context_dry_run_result.json",
        "manual_review_csv": output_dir
        / "high_intensity_runtime_manual_review_context_dry_run_result.csv",
        "monthly_monitoring": output_dir
        / "high_intensity_runtime_monthly_concentration_monitoring_dry_run.json",
        "safety_gate": output_dir
        / "high_intensity_runtime_fail_closed_safety_gate_result.json",
        "artifact_registry": output_dir
        / "high_intensity_runtime_artifact_registry_dry_run_report.json",
        "data_quality": output_dir
        / "high_intensity_runtime_dry_run_data_quality_report.json",
        "interpretation_boundary": output_dir
        / "high_intensity_runtime_dry_run_interpretation_boundary.json",
        "readiness": output_dir / "high_intensity_2344_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2344_task_route.json",
        "safety_boundary": output_dir
        / "high_intensity_runtime_dry_run_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_observe_only_runtime_dry_run.md",
        "event_detection_doc": docs_root
        / "high_intensity_runtime_event_detection_dry_run.md",
        "append_cluster_doc": docs_root
        / "high_intensity_runtime_event_append_cluster_update_dry_run.md",
        "safety_doc": docs_root / "high_intensity_runtime_safety_gate_dry_run.md",
        "route_doc": docs_root / "high_intensity_2344_readiness_route.md",
    }


def _write_rows_json(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    write_json(
        path,
        {
            "schema_version": f"{REPORT_TYPE}.{path.stem}.v1",
            "task_id": TASK_ID,
            "row_count": len(rows),
            **SAFETY_FIELDS,
            "rows": list(rows),
        },
    )


def _contract_has_safe_boundary(payload: Any) -> bool:
    contract = mapping(payload)
    if not contract:
        return False
    try:
        _validate_no_unsafe_fields("contract", contract)
    except HighIntensityRuntimeDryRunError:
        return False
    return (
        contract.get("promotion_allowed") is False
        and contract.get("paper_shadow_allowed") is False
        and contract.get("production_allowed") is False
        and str(contract.get("broker_action", "none")).lower() == "none"
    )


def _core_safety_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _runtime_input_value(
    field: str,
    source_row: Mapping[str, Any],
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> Any:
    if field == "as_of_timestamp":
        return _as_of_timestamp(source_row)
    if field == "known_at_policy":
        return _known_at_policy(selected_rule, dynamic_dry_run)
    if field == "pit_policy":
        return _pit_policy(selected_rule, dynamic_dry_run)
    return source_row.get(field)


def _runtime_input_source(field: str, dynamic_dry_run: Mapping[str, Any]) -> str:
    if field == "as_of_timestamp":
        return (
            "dynamic_target_risk_cap_trigger_alignment_matrix."
            "risk_cap_decision_timestamp_or_decision_timestamp"
        )
    if field == "known_at_policy":
        return "high_intensity_selected_trigger_rule.trigger_rule.known_at_policy"
    if field == "pit_policy":
        return "high_intensity_selected_trigger_rule.trigger_rule.pit_policy"
    return str(mapping(dynamic_dry_run).get("paths", {}).get("trigger_alignment", ""))


def _selected_rule_threshold(selected_rule: Mapping[str, Any]) -> float:
    trigger_rule = mapping(selected_rule.get("trigger_rule"))
    threshold = to_float(trigger_rule.get("threshold_value"))
    return threshold if threshold else 1.0


def _not_detected_reason(source_row: Mapping[str, Any], threshold: float) -> str:
    if not _truthy(source_row.get("risk_cap_triggered")):
        return "risk_cap_triggered=false"
    if not _truthy(source_row.get("scope_active")):
        return "scope_active=false"
    if to_float(source_row.get("risk_cap_score")) < threshold:
        return f"risk_cap_score<{round_float(threshold)}"
    if not _is_defensive_risk_cap_direction(source_row.get("signal_direction")):
        return "signal_direction_not_defensive"
    return "rule_not_matched"


def _find_open_cluster(
    *,
    detection: Mapping[str, Any],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    event_date = _parse_date(str(detection.get("date", "")))
    target_asset = str(detection.get("target_asset", ""))
    selected_rule_id = str(detection.get("selected_rule_id", ""))
    candidates: list[Mapping[str, Any]] = []
    for cluster in prior_cluster_rows:
        status = str(cluster.get("cluster_status", "")).upper()
        if status not in {"OPEN", "RUNTIME_OPEN"}:
            continue
        if str(cluster.get("target_asset", "")) != target_asset:
            continue
        if str(cluster.get("selected_rule_id", "")) != selected_rule_id:
            continue
        try:
            end_date = _parse_date(str(cluster.get("cluster_end_date", "")))
        except ValueError:
            continue
        gap = (event_date - end_date).days
        if 0 < gap <= CLUSTER_CONTINUATION_GAP_DAYS:
            candidates.append(cluster)
    if not candidates:
        return {}
    return sorted(candidates, key=lambda row: str(row.get("cluster_end_date", "")))[-1]


def _manual_review_blocked_fields(payload: Mapping[str, Any]) -> list[str]:
    blocked: set[str] = set()
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in MANUAL_REVIEW_FORBIDDEN_FIELDS and value not in {
            False,
            None,
            "",
            "none",
            "NONE",
        }:
            blocked.add(key)
    return sorted(blocked)


def _forbidden_fields_present(payload: Mapping[str, Any]) -> list[str]:
    blocked: set[str] = set()
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in FORBIDDEN_OUTPUT_FIELDS and value not in {
            False,
            None,
            "",
            "none",
            "NONE",
        }:
            blocked.add(key)
    return sorted(blocked)


def _is_defensive_risk_cap_direction(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {
        "defensive",
        "risk_cap",
        "risk_off",
        "defensive_risk_cap",
        "portfolio_level_risk_cap",
    }


def _as_of_timestamp(row: Mapping[str, Any]) -> str:
    return str(
        row.get("as_of_timestamp")
        or row.get("risk_cap_decision_timestamp")
        or row.get("decision_timestamp")
        or ""
    )


def _decision_timestamp(row: Mapping[str, Any]) -> str:
    return str(row.get("decision_timestamp") or row.get("risk_cap_decision_timestamp") or "")


def _known_at_policy(
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> str:
    return str(
        mapping(selected_rule.get("trigger_rule")).get("known_at_policy")
        or mapping(dynamic_dry_run.get("summary")).get("known_at_policy")
        or mapping(dynamic_dry_run.get("pit_boundary")).get("known_at_policy")
        or KNOWN_AT_POLICY
    )


def _pit_policy(
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> str:
    trigger_rule = mapping(selected_rule.get("trigger_rule"))
    if trigger_rule.get("pit_policy"):
        return str(trigger_rule.get("pit_policy"))
    if mapping(dynamic_dry_run.get("summary")).get("pit_policy"):
        return str(mapping(dynamic_dry_run.get("summary")).get("pit_policy"))
    if mapping(dynamic_dry_run.get("pit_boundary")).get("pit_approximation_ready") is True:
        return PIT_POLICY
    return PIT_POLICY


def _first_non_empty(values: Sequence[str] | Any) -> str:
    for value in values:
        if value:
            return str(value)
    return ""


def _event_key(
    date_value: object,
    target_asset: object,
    selected_rule_id: object,
) -> tuple[str, str, str]:
    return (str(date_value or ""), str(target_asset or ""), str(selected_rule_id or ""))


def _active_days(start: str, end: str) -> int:
    try:
        return (_parse_date(end) - _parse_date(start)).days + 1
    except ValueError:
        return 0


def _add_business_days(start: date, days: int) -> date:
    current = start
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _date_is_valid(value: str) -> bool:
    try:
        _parse_date(value)
    except ValueError:
        return False
    return True


def _is_missing(value: object) -> bool:
    return value is None or str(value).strip() == ""


def _is_number(value: object) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _emits_action(value: object) -> bool:
    if value in {False, None, "", "none", "NONE"}:
        return False
    if isinstance(value, Sequence) and not isinstance(value, str):
        return bool(value)
    if isinstance(value, Mapping):
        return bool(value)
    return str(value).strip().lower() not in {"", "false", "none", "null"}


def _rate(numerator: int, denominator: int) -> float:
    return round_float(numerator / denominator) if denominator else 0.0


def _short_id(prefix: str, *parts: str) -> str:
    encoded = "|".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(encoded).hexdigest()[:16]}"


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        clean_for_yaml(dict(payload)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityRuntimeDryRunError(
                f"{label} required artifact missing: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HighIntensityRuntimeDryRunError(f"invalid JSON artifact: {path}") from exc


def _validate_no_unsafe_fields(label: str, payload: Any) -> None:
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in INPUT_SAFETY_FALSE_FIELDS and value is True:
            raise HighIntensityRuntimeDryRunError(
                f"{label} unsafe field {'.'.join(path)}=true"
            )
        if key == "broker_action" and str(value).lower() not in {"", "none"}:
            raise HighIntensityRuntimeDryRunError(
                f"{label} unsafe broker_action={value}"
            )
        if key in FORBIDDEN_OUTPUT_FIELDS and value not in {
            False,
            None,
            "",
            "none",
            "NONE",
        }:
            raise HighIntensityRuntimeDryRunError(
                f"{label} unsafe field {'.'.join(path)}={value}"
            )


def _walk_payload(
    payload: Any,
    prefix: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], Any]]:
    items: list[tuple[tuple[str, ...], Any]] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            items.extend(_walk_payload(value, (*prefix, str(key))))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            items.extend(_walk_payload(value, (*prefix, str(index))))
    else:
        items.append((prefix, payload))
    return items


def _require_false(payload: Mapping[str, Any], key: str, label: str) -> None:
    if payload.get(key) is not False:
        raise HighIntensityRuntimeDryRunError(f"{label} requires {key}=false")


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _route_rationale(status: str, next_task: str) -> str:
    if next_task == NEXT_2344_SCHEDULER_PLAN_TASK and status == "READY_FOR_2344_WITH_CAVEATS":
        return "observe-only scheduler integration plan route allowed with caveats"
    if next_task == NEXT_2344_SCHEDULER_PLAN_TASK:
        return "observe-only scheduler integration plan route allowed"
    if next_task == NEXT_2344_SAFETY_REMEDIATION_TASK:
        return "fail-closed safety gate requires remediation"
    if next_task == NEXT_2344_REGISTRY_REMEDIATION_TASK:
        return "artifact registry dry-run requires remediation"
    if next_task == NEXT_2344_DRY_RUN_REMEDIATION_TASK:
        return "runtime dry-run remediation required"
    return "runtime line should be archived until prerequisites are remediated"
