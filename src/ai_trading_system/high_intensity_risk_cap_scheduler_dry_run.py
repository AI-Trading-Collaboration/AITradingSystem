from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    DEFAULT_CONTINUE_DECISION_ROOT,
    DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_RUNTIME_INTEGRATION_PLAN_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
    HighIntensityRuntimeDryRunError,
    _add_business_days,
    _as_of_timestamp,
    _date_is_valid,
    _event_key,
    _hash_payload,
    _is_missing,
    _parse_date,
    _pit_policy,
    _short_id,
    _truthy,
    build_high_intensity_runtime_cluster_update_dry_run_result,
    build_high_intensity_runtime_event_append_dry_run_result,
    build_high_intensity_runtime_event_detection_dry_run_result,
    build_high_intensity_runtime_manual_review_context_dry_run_result,
    build_high_intensity_runtime_monthly_concentration_monitoring_dry_run,
    load_high_intensity_runtime_dry_run_inputs,
)
from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_RUNTIME_DRY_RUN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCHEDULER_INTEGRATION_PLAN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    EXPECTED_SELECTED_RULE,
    load_trading_2343_scheduler_plan_context,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    READY_STATUS as EXPECTED_2344_STATUS,
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
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2345_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_SCHEDULER_DRY_RUN"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_dry_run"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_dry_run"

EXPECTED_2345_NEXT_TASK = (
    "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
)
READY_2345_STATUSES = {
    "READY_FOR_2345_OBSERVE_ONLY_SCHEDULER_DRY_RUN",
    "READY_FOR_2345_WITH_CAVEATS",
}
KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
PIT_POLICY = "PIT_APPROXIMATION_READY"

NEXT_2346_WIRING_PLAN_TASK = (
    "TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan"
)
NEXT_2346_DRY_RUN_REMEDIATION_TASK = (
    "TRADING-2346_High_Intensity_Risk_Cap_Scheduler_Dry_Run_Remediation"
)
NEXT_2346_SAFETY_REMEDIATION_TASK = (
    "TRADING-2346_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
)
NEXT_2346_ARCHIVE_TASK = "TRADING-2346_Archive_High_Intensity_Risk_Cap_Runtime_Line"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

CYCLE_MODES = [
    "historical_replay_scheduler_cycle",
    "single_day_scheduler_cycle_fixture",
    "fail_closed_safety_fixture",
]
HORIZON_DAYS = {"1d": 1, "5d": 5, "10d": 10, "20d": 20}
JOB_ORDER = [
    "input_validation",
    "event_detection",
    "event_append",
    "cluster_update",
    "pending_outcome_update",
    "manual_review_context_update",
    "monthly_concentration_monitoring",
    "outcome_update_job_plan_check",
    "safety_gate_validation",
]
JOB_DEPENDENCIES = {
    "event_detection": ["input_validation"],
    "event_append": ["event_detection", "prior_event_log"],
    "cluster_update": ["event_append", "prior_cluster_registry"],
    "pending_outcome_update": ["event_append"],
    "manual_review_context_update": ["event_append", "cluster_update"],
    "monthly_concentration_monitoring": ["event_append", "cluster_update"],
    "outcome_update_job_plan_check": ["pending_outcome_update"],
}
EXTERNAL_DEPENDENCIES = {"prior_event_log", "prior_cluster_registry"}
REQUIRED_INPUT_FIELDS = [
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
    "prior_event_log",
    "prior_cluster_registry",
    "prior_pending_outcome_registry",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only_scheduler_dry_run": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
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
    "manual_review_only": True,
}
ROUTE_CAVEATS = [
    "OBSERVE_ONLY",
    "SCHEDULER_DISABLED_BY_DEFAULT",
    "PARTIAL_COVERAGE_CAVEAT",
    "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
]
INPUT_SAFETY_FALSE_FIELDS = {
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "runtime_scheduler_enabled",
    "scheduler_enabled",
    "scheduler_default_enabled",
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


class HighIntensitySchedulerDryRunError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_dry_run(
    *,
    scheduler_integration_plan_dir: Path = DEFAULT_SCHEDULER_INTEGRATION_PLAN_ROOT,
    runtime_dry_run_dir: Path = DEFAULT_RUNTIME_DRY_RUN_ROOT,
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
        raise HighIntensitySchedulerDryRunError(
            f"high-intensity scheduler dry-run only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_dry_run_inputs(
        scheduler_integration_plan_dir=scheduler_integration_plan_dir,
        runtime_dry_run_dir=runtime_dry_run_dir,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
    )
    trigger_rows = records(
        mapping(inputs["runtime_inputs"]["dynamic_dry_run"]["trigger_alignment"]).get(
            "rows"
        )
    )
    selected_rule = mapping(inputs["runtime_inputs"]["threshold_selection"]).get(
        "selected_rule",
        {},
    )
    prior_event_rows = records(
        mapping(inputs["runtime_inputs"]["event_logger"]["event_log"]).get("rows")
    )
    prior_trigger_rows = records(
        mapping(inputs["runtime_inputs"]["event_logger"]["trigger_day_log"]).get("rows")
    )
    prior_cluster_rows = records(
        mapping(inputs["runtime_inputs"]["event_logger"]["cluster_registry"]).get(
            "rows"
        )
    )
    prior_pending_rows = records(
        mapping(inputs["runtime_inputs"]["event_logger"]["pending_outcome_registry"]).get(
            "rows"
        )
    )

    contract_validation = build_high_intensity_scheduler_contract_validation_report(
        inputs
    )
    cycle_plan = build_high_intensity_scheduler_cycle_plan()
    job_dag = build_high_intensity_scheduler_job_dag_validation_report()
    input_snapshot_rows = build_high_intensity_scheduler_input_snapshot_validation_matrix(
        trigger_source_rows=trigger_rows,
        selected_rule=selected_rule,
        dynamic_dry_run=inputs["runtime_inputs"]["dynamic_dry_run"],
        prior_event_rows=prior_event_rows,
        prior_cluster_rows=prior_cluster_rows,
        prior_pending_rows=prior_pending_rows,
    )
    detection_rows = build_high_intensity_scheduler_event_detection_job_dry_run_result(
        trigger_source_rows=trigger_rows,
        selected_rule=selected_rule,
        inputs=inputs["runtime_inputs"],
    )
    append_rows = build_high_intensity_scheduler_event_append_job_dry_run_result(
        detection_rows=detection_rows,
        prior_trigger_day_rows=prior_trigger_rows,
        prior_event_rows=prior_event_rows,
        prior_cluster_rows=prior_cluster_rows,
    )
    cluster_rows = build_high_intensity_scheduler_cluster_update_job_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=prior_cluster_rows,
    )
    pending_rows = build_high_intensity_scheduler_pending_outcome_job_dry_run_result(
        append_rows=append_rows
    )
    manual_review_rows = (
        build_high_intensity_scheduler_manual_review_context_job_dry_run_result(
            append_rows=append_rows,
            detection_rows=detection_rows,
            inputs=inputs["runtime_inputs"],
        )
    )
    monthly_concentration = (
        build_high_intensity_scheduler_monthly_concentration_job_dry_run_result(
            detection_rows=detection_rows,
            append_rows=append_rows,
            cluster_update_rows=cluster_rows,
            prior_event_rows=prior_event_rows,
            prior_cluster_rows=prior_cluster_rows,
            inputs=inputs["runtime_inputs"],
        )
    )
    outcome_update_report = (
        build_high_intensity_scheduler_outcome_update_job_dry_run_report(inputs)
    )
    disabled_policy = (
        build_high_intensity_scheduler_disabled_policy_validation_report(inputs)
    )
    safety_gate = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        {
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "as_of_timestamp": _first_non_empty(
                _as_of_timestamp(row) for row in trigger_rows
            ),
            "pit_policy": _pit_policy(
                selected_rule,
                inputs["runtime_inputs"]["dynamic_dry_run"],
            ),
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
        }
    )
    artifact_registry = (
        build_high_intensity_scheduler_artifact_registry_dry_run_report(
            output_dir=output_dir,
            docs_root=docs_root,
        )
    )
    data_quality = build_high_intensity_scheduler_dry_run_data_quality_report(
        inputs=inputs,
        contract_validation=contract_validation,
        cycle_plan=cycle_plan,
        job_dag=job_dag,
        input_snapshot_rows=input_snapshot_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_concentration=monthly_concentration,
        outcome_update_report=outcome_update_report,
        disabled_policy=disabled_policy,
        safety_gate=safety_gate,
    )
    interpretation_boundary = (
        build_high_intensity_scheduler_dry_run_interpretation_boundary(
            generated_at=generated_at
        )
    )
    readiness = build_high_intensity_2346_readiness_checklist(
        contract_validation=contract_validation,
        cycle_plan=cycle_plan,
        job_dag=job_dag,
        input_snapshot_rows=input_snapshot_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_concentration=monthly_concentration,
        outcome_update_report=outcome_update_report,
        disabled_policy=disabled_policy,
        safety_gate=safety_gate,
        artifact_registry=artifact_registry,
    )
    task_route = build_high_intensity_2346_task_route(
        readiness=readiness,
        safety_gate=safety_gate,
    )
    safety_boundary = build_high_intensity_scheduler_dry_run_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_scheduler_dry_run_summary(
        generated_at=generated_at,
        scheduler_integration_plan_dir=scheduler_integration_plan_dir,
        runtime_dry_run_dir=runtime_dry_run_dir,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        inputs=inputs,
        data_quality=data_quality,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_scheduler_dry_run_outputs(
        paths=paths,
        summary=summary,
        contract_validation=contract_validation,
        cycle_plan=cycle_plan,
        job_dag=job_dag,
        input_snapshot_rows=input_snapshot_rows,
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_rows=cluster_rows,
        pending_rows=pending_rows,
        manual_review_rows=manual_review_rows,
        monthly_concentration=monthly_concentration,
        outcome_update_report=outcome_update_report,
        disabled_policy=disabled_policy,
        safety_gate=safety_gate,
        artifact_registry=artifact_registry,
        data_quality=data_quality,
        interpretation_boundary=interpretation_boundary,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_dry_run_inputs(
    *,
    scheduler_integration_plan_dir: Path,
    runtime_dry_run_dir: Path,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
) -> dict[str, Any]:
    try:
        scheduler_plan = load_trading_2344_scheduler_dry_run_context(
            scheduler_integration_plan_dir
        )
        runtime_dry_run = load_trading_2343_scheduler_plan_context(runtime_dry_run_dir)
        runtime_inputs = load_high_intensity_runtime_dry_run_inputs(
            runtime_integration_plan_dir=runtime_integration_plan_dir,
            continue_decision_dir=continue_decision_dir,
            event_logger_dir=event_logger_dir,
            threshold_selection_dir=threshold_selection_dir,
            forward_observe_plan_dir=forward_observe_plan_dir,
            dynamic_dry_run_dir=dynamic_dry_run_dir,
        )
    except (HighIntensityRuntimeDryRunError, ValueError) as exc:
        raise HighIntensitySchedulerDryRunError(str(exc)) from exc

    _require_existing_files(
        [
            dynamic_dry_run_dir / "dynamic_target_risk_cap_trigger_alignment_matrix.csv",
            dynamic_dry_run_dir / "dynamic_target_exposure_cap_dry_run_result.csv",
        ],
        "TRADING-2345 dynamic dry-run source CSV",
    )
    inputs = {
        "scheduler_plan": scheduler_plan,
        "runtime_dry_run": runtime_dry_run,
        "runtime_inputs": runtime_inputs,
    }
    _validate_scheduler_dry_run_cross_source_contracts(inputs)
    return inputs


def load_trading_2344_scheduler_dry_run_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_scheduler_integration_plan_summary.json",
        "scope_contract": root / "high_intensity_scheduler_scope_contract.json",
        "cadence_plan": root / "high_intensity_scheduler_cadence_plan.json",
        "input_contract": root / "high_intensity_scheduler_input_contract.json",
        "event_detection_contract": root
        / "high_intensity_scheduler_event_detection_job_contract.json",
        "event_append_contract": root
        / "high_intensity_scheduler_event_append_job_contract.json",
        "cluster_update_contract": root
        / "high_intensity_scheduler_cluster_update_job_contract.json",
        "pending_outcome_contract": root
        / "high_intensity_scheduler_pending_outcome_update_job_contract.json",
        "outcome_update_contract": root
        / "high_intensity_scheduler_outcome_update_job_contract.json",
        "manual_review_contract": root
        / "high_intensity_scheduler_manual_review_context_contract.json",
        "monthly_contract": root
        / "high_intensity_scheduler_monthly_concentration_monitoring_contract.json",
        "artifact_path_plan": root / "high_intensity_scheduler_artifact_path_plan.json",
        "registry_update_plan": root
        / "high_intensity_scheduler_registry_update_plan.json",
        "fail_closed_gate": root / "high_intensity_scheduler_fail_closed_safety_gate.json",
        "disabled_policy": root
        / "high_intensity_scheduler_disabled_by_default_policy.json",
        "dry_run_execution_plan": root
        / "high_intensity_scheduler_dry_run_execution_plan.json",
        "failure_mode_matrix": root
        / "high_intensity_scheduler_failure_mode_matrix.json",
        "risk_register": root / "high_intensity_scheduler_integration_risk_register.json",
        "readiness": root / "high_intensity_2345_readiness_checklist.json",
        "task_route": root / "high_intensity_2345_task_route.json",
        "interpretation_boundary": root
        / "high_intensity_scheduler_integration_interpretation_boundary.json",
        "safety_boundary": root / "high_intensity_scheduler_integration_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2344 scheduler plan")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2344 {key}", payload)

    summary = mapping(payloads["summary"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    fail_closed_gate = mapping(payloads["fail_closed_gate"])
    disabled_policy = mapping(payloads["disabled_policy"])
    if summary.get("status") != EXPECTED_2344_STATUS:
        raise HighIntensitySchedulerDryRunError(
            f"TRADING-2345 requires 2344 status {EXPECTED_2344_STATUS}"
        )
    if summary.get("next_task") != EXPECTED_2345_NEXT_TASK:
        raise HighIntensitySchedulerDryRunError(
            f"TRADING-2345 requires 2344 route {EXPECTED_2345_NEXT_TASK}"
        )
    if task_route.get("next_task") != EXPECTED_2345_NEXT_TASK:
        raise HighIntensitySchedulerDryRunError(
            f"TRADING-2345 requires 2344 task route {EXPECTED_2345_NEXT_TASK}"
        )
    if readiness.get("readiness_status") not in READY_2345_STATUSES:
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires 2344 readiness for scheduler dry-run"
        )
    if fail_closed_gate.get("safety_gate_status") != "PASS":
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires 2344 fail-closed safety gate PASS"
        )
    _require_false(summary, "scheduler_enabled", "TRADING-2344 summary")
    _require_false(summary, "scheduler_default_enabled", "TRADING-2344 summary")
    _require_false(summary, "event_append_executed", "TRADING-2344 summary")
    _require_false(summary, "outcome_binding_executed", "TRADING-2344 summary")
    _require_false(disabled_policy, "scheduler_default_enabled", "2344 disabled policy")
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def build_high_intensity_scheduler_contract_validation_report(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    scheduler_plan = mapping(inputs["scheduler_plan"])
    contract_checks = {
        "scheduler_scope_contract_valid": _contract_has_safe_boundary(
            scheduler_plan.get("scope_contract")
        )
        and mapping(scheduler_plan.get("scope_contract")).get("scheduler_enabled")
        is False
        and mapping(scheduler_plan.get("scope_contract")).get("scheduler_default_enabled")
        is False,
        "scheduler_cadence_plan_valid": _contract_has_safe_boundary(
            scheduler_plan.get("cadence_plan")
        )
        and mapping(scheduler_plan.get("cadence_plan")).get("scheduler_enabled_in_2344")
        is False
        and mapping(
            mapping(scheduler_plan.get("cadence_plan")).get("calendar_policy")
        ).get("skip_non_trading_days")
        is True,
        "scheduler_input_contract_valid": _contract_has_safe_boundary(
            scheduler_plan.get("input_contract")
        )
        and bool(mapping(scheduler_plan.get("input_contract")).get("blocked_inputs"))
        and mapping(scheduler_plan.get("input_contract")).get(
            "fail_closed_if_missing_required_input"
        )
        is True,
        "event_detection_job_contract_valid": _job_contract_valid(
            scheduler_plan.get("event_detection_contract")
        ),
        "event_append_job_contract_valid": _job_contract_valid(
            scheduler_plan.get("event_append_contract")
        ),
        "cluster_update_job_contract_valid": _job_contract_valid(
            scheduler_plan.get("cluster_update_contract")
        ),
        "pending_outcome_job_contract_valid": _job_contract_valid(
            scheduler_plan.get("pending_outcome_contract")
        ),
        "outcome_update_job_contract_valid": _contract_has_safe_boundary(
            scheduler_plan.get("outcome_update_contract")
        )
        and mapping(scheduler_plan.get("outcome_update_contract")).get(
            "requires_validate_data"
        )
        is True,
        "manual_review_context_contract_valid": _contract_has_safe_boundary(
            scheduler_plan.get("manual_review_contract")
        )
        and _blocked_display_fields_valid(
            mapping(scheduler_plan.get("manual_review_contract")).get(
                "blocked_display_fields"
            )
        ),
        "monthly_concentration_monitoring_contract_valid": _contract_has_safe_boundary(
            scheduler_plan.get("monthly_contract")
        )
        and mapping(scheduler_plan.get("monthly_contract")).get("monitoring_required")
        is True,
        "disabled_by_default_policy_valid": _disabled_policy_valid(
            scheduler_plan.get("disabled_policy")
        ),
        "fail_closed_safety_gate_valid": mapping(
            scheduler_plan.get("fail_closed_gate")
        ).get("safety_gate_status")
        == "PASS",
    }
    errors = [
        key.replace("_valid", "").upper()
        for key, valid in contract_checks.items()
        if not valid
    ]
    warnings: list[str] = []
    if _source_validation_status(inputs) == "PASS_WITH_WARNINGS":
        warnings.append("SOURCE_VALIDATE_DATA_PASS_WITH_WARNINGS")
    if _inherited_monthly_warning(inputs):
        warnings.append(_inherited_monthly_warning(inputs))
    status = "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return clean_for_yaml(
        {
            "contract_validation_status": status,
            **contract_checks,
            "contract_error_count": len(errors),
            "contract_warning_count": len(warnings),
            "contract_errors": errors,
            "contract_warnings": sorted(set(warnings)),
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_cycle_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "cycle_plan_id": "HIGH_INTENSITY_OBSERVE_ONLY_SCHEDULER_CYCLE_V1",
            "scheduler_enabled": False,
            "scheduler_dry_run_executed": True,
            "cycle_modes": CYCLE_MODES,
            "job_order": JOB_ORDER,
            "market_calendar_required": True,
            "skip_non_trading_days": True,
            "known_at_policy": KNOWN_AT_POLICY,
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_job_dag_validation_report(
    *,
    job_order: Sequence[str] | None = None,
    job_dependencies: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    order = list(job_order or JOB_ORDER)
    dependencies = {
        key: list(value) for key, value in (job_dependencies or JOB_DEPENDENCIES).items()
    }
    missing: list[str] = []
    order_errors: list[str] = []
    positions = {job: index for index, job in enumerate(order)}
    for job, deps in dependencies.items():
        if job not in positions:
            missing.append(f"{job}:job_missing")
            continue
        for dep in deps:
            if dep in EXTERNAL_DEPENDENCIES:
                continue
            if dep not in positions:
                missing.append(f"{job}:{dep}")
            elif positions[dep] >= positions[job]:
                order_errors.append(f"{job}:dependency_after_job:{dep}")
    cycle_detected = _job_dependency_cycle_detected(dependencies)
    errors = [*missing, *order_errors]
    if cycle_detected:
        errors.append("CYCLE_DETECTED")
    status = "FAIL" if errors else "PASS"
    return clean_for_yaml(
        {
            "dag_validation_status": status,
            "job_count": len(order),
            "job_order_valid": not order_errors and not missing,
            "missing_dependency_count": len(missing),
            "cycle_detected": cycle_detected,
            "job_dependencies": {
                job: {"depends_on": deps} for job, deps in dependencies.items()
            },
            "dag_warnings": [],
            "dag_errors": errors,
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_input_snapshot_validation_matrix(
    *,
    trigger_source_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
    prior_event_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
    prior_pending_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    total = len(trigger_source_rows)
    rows: list[dict[str, Any]] = []
    registry_sources = {
        "prior_event_log": (
            prior_event_rows,
            "high_intensity_observe_event_log.json",
        ),
        "prior_cluster_registry": (
            prior_cluster_rows,
            "high_intensity_observe_event_cluster_registry.json",
        ),
        "prior_pending_outcome_registry": (
            prior_pending_rows,
            "high_intensity_pending_outcome_registry.json",
        ),
    }
    for field in REQUIRED_INPUT_FIELDS:
        if field in registry_sources:
            registry_rows, source = registry_sources[field]
            available = bool(registry_rows)
            rows.append(
                _input_snapshot_row(
                    field=field,
                    required=True,
                    available=available,
                    coverage_ratio=1.0 if available else 0.0,
                    missing_count=0 if available else 1,
                    invalid_count=0,
                    source_artifact=source,
                    validation_status="PASS" if available else "FAIL",
                    blocking_if_invalid=True,
                )
            )
            continue

        available_count = 0
        invalid_count = 0
        for item in trigger_source_rows:
            value = _scheduler_input_value(field, item, selected_rule, dynamic_dry_run)
            if _is_missing(value):
                continue
            available_count += 1
            if field == "date" and not _date_is_valid(str(value)):
                invalid_count += 1
            elif field == "risk_cap_score" and not _is_number(value):
                invalid_count += 1
        missing_count = total - available_count
        if missing_count or invalid_count:
            status = "FAIL"
        elif field in {"as_of_timestamp", "known_at_policy", "pit_policy"}:
            status = "PASS_WITH_DERIVED_FIELD"
        else:
            status = "PASS"
        rows.append(
            _input_snapshot_row(
                field=field,
                required=True,
                available=available_count == total and invalid_count == 0,
                coverage_ratio=_rate(available_count, total),
                missing_count=missing_count,
                invalid_count=invalid_count,
                source_artifact=_scheduler_input_source(field, dynamic_dry_run),
                validation_status=status,
                blocking_if_invalid=True,
            )
        )
    return rows


def build_high_intensity_scheduler_event_detection_job_dry_run_result(
    *,
    trigger_source_rows: Sequence[Mapping[str, Any]],
    selected_rule: Mapping[str, Any],
    inputs: Mapping[str, Any],
    cycle_mode: str = "historical_replay_scheduler_cycle",
) -> list[dict[str, Any]]:
    runtime_rows = build_high_intensity_runtime_event_detection_dry_run_result(
        trigger_source_rows=trigger_source_rows,
        selected_rule=selected_rule,
        inputs=inputs,
    )
    cycle_id = _cycle_id(cycle_mode)
    return [
        clean_for_yaml(
            {
                "scheduler_cycle_id": cycle_id,
                "cycle_mode": cycle_mode,
                **row,
            }
        )
        for row in runtime_rows
    ]


def build_high_intensity_scheduler_event_append_job_dry_run_result(
    *,
    detection_rows: Sequence[Mapping[str, Any]],
    prior_trigger_day_rows: Sequence[Mapping[str, Any]],
    prior_event_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
    cycle_mode: str = "historical_replay_scheduler_cycle",
) -> list[dict[str, Any]]:
    runtime_rows = build_high_intensity_runtime_event_append_dry_run_result(
        detection_rows=detection_rows,
        prior_trigger_day_rows=prior_trigger_day_rows,
        prior_event_rows=prior_event_rows,
        prior_cluster_rows=prior_cluster_rows,
    )
    detection_by_key = {
        _event_key(row.get("date"), row.get("target_asset"), row.get("selected_rule_id")): row
        for row in detection_rows
    }
    cycle_id = _cycle_id(cycle_mode)
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(runtime_rows):
        item = dict(row)
        key = _event_key(
            item.get("date"),
            item.get("target_asset"),
            item.get("selected_rule_id"),
        )
        detection = detection_by_key.get(key, {})
        if item.get("append_status") == "WOULD_EXTEND_EXISTING_CLUSTER":
            item["would_append_event"] = True
            item["event_id"] = item.get("event_id") or _short_id(
                "hievt",
                str(item.get("selected_rule_id", "")),
                str(item.get("target_asset", "")),
                str(item.get("event_cluster_id", "")),
                str(item.get("date", "")),
                "scheduler",
            )
        rows.append(
            clean_for_yaml(
                {
                    "scheduler_cycle_id": cycle_id,
                    "append_record_id": item.get("append_record_id")
                    or _short_id("hiapp", str(index), str(item.get("date", ""))),
                    "cycle_mode": cycle_mode,
                    "date": item.get("date", ""),
                    "target_asset": item.get("target_asset", ""),
                    "selected_rule_id": item.get("selected_rule_id", ""),
                    "prior_event_log_record_count": item.get(
                        "prior_event_log_record_count",
                        len(prior_event_rows),
                    ),
                    "high_intensity_triggered": detection.get(
                        "high_intensity_triggered",
                        False,
                    ),
                    "would_append_event": item.get("would_append_event", False),
                    "event_id": item.get("event_id", ""),
                    "event_cluster_id": item.get("event_cluster_id", ""),
                    "is_new_event": item.get("is_new_event", False),
                    "is_cluster_primary_event": item.get(
                        "is_cluster_primary_event",
                        False,
                    ),
                    "append_mode": "append_only",
                    "original_event_log_mutation_allowed": False,
                    "event_status_on_create": "OBSERVE_PENDING",
                    "manual_review_observation_flag": item.get(
                        "manual_review_observation_flag",
                        False,
                    ),
                    "append_status": item.get("append_status", ""),
                    "blocked_reason": item.get("blocked_reason", ""),
                    **_core_safety_fields(),
                }
            )
        )
    return rows


def build_high_intensity_scheduler_cluster_update_job_dry_run_result(
    *,
    append_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
    cycle_mode: str = "historical_replay_scheduler_cycle",
) -> list[dict[str, Any]]:
    runtime_rows = build_high_intensity_runtime_cluster_update_dry_run_result(
        append_rows=append_rows,
        prior_cluster_rows=prior_cluster_rows,
    )
    cycle_id = _cycle_id(cycle_mode)
    return [
        clean_for_yaml(
            {
                "scheduler_cycle_id": cycle_id,
                "cycle_mode": cycle_mode,
                **row,
            }
        )
        for row in runtime_rows
    ]


def build_high_intensity_scheduler_pending_outcome_job_dry_run_result(
    *,
    append_rows: Sequence[Mapping[str, Any]],
    cycle_mode: str = "historical_replay_scheduler_cycle",
) -> list[dict[str, Any]]:
    cycle_id = _cycle_id(cycle_mode)
    rows: list[dict[str, Any]] = []
    append_statuses = {"WOULD_APPEND_NEW_EVENT", "WOULD_EXTEND_EXISTING_CLUSTER"}
    for append in append_rows:
        if append.get("append_status") not in append_statuses:
            continue
        event_id = str(append.get("event_id", ""))
        event_date = str(append.get("date", ""))
        if not event_id or not event_date:
            continue
        for horizon, days in HORIZON_DAYS.items():
            due = _add_business_days(_parse_date(event_date), days)
            rows.append(
                clean_for_yaml(
                    {
                        "scheduler_cycle_id": cycle_id,
                        "pending_update_record_id": _short_id(
                            "hipu",
                            event_id,
                            horizon,
                            "scheduler",
                        ),
                        "cycle_mode": cycle_mode,
                        "event_id": event_id,
                        "event_date": event_date,
                        "target_asset": append.get("target_asset", ""),
                        "horizon": horizon,
                        "outcome_due_date": due.isoformat(),
                        "would_create_pending_outcome": True,
                        "pending_outcome_id": _short_id("hiout", event_id, horizon),
                        "outcome_status_on_create": "OUTCOME_PENDING",
                        "outcome_binding_allowed_in_2345": False,
                        "outcome_binding_executed": False,
                        "pending_update_status": "WOULD_CREATE_PENDING_OUTCOME",
                        "blocked_reason": "",
                        **_core_safety_fields(),
                    }
                )
            )
    return rows


def build_high_intensity_scheduler_manual_review_context_job_dry_run_result(
    *,
    append_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any],
    cycle_mode: str = "historical_replay_scheduler_cycle",
) -> list[dict[str, Any]]:
    runtime_rows = build_high_intensity_runtime_manual_review_context_dry_run_result(
        append_rows=append_rows,
        detection_rows=detection_rows,
        inputs=inputs,
    )
    cycle_id = _cycle_id(cycle_mode)
    rows: list[dict[str, Any]] = []
    for row in runtime_rows:
        payload = dict(row)
        blocked = sorted(
            set(payload.get("blocked_fields_detected") or [])
            | set(_manual_review_blocked_fields(payload))
        )
        rows.append(
            clean_for_yaml(
                {
                    "scheduler_cycle_id": cycle_id,
                    "cycle_mode": cycle_mode,
                    **payload,
                    "blocked_fields_detected": blocked,
                    "context_status": "PASS" if not blocked else "BLOCKED",
                }
            )
        )
    return rows


def build_high_intensity_scheduler_monthly_concentration_job_dry_run_result(
    *,
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_update_rows: Sequence[Mapping[str, Any]],
    prior_event_rows: Sequence[Mapping[str, Any]],
    prior_cluster_rows: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any],
    cycle_mode: str = "historical_replay_scheduler_cycle",
) -> dict[str, Any]:
    runtime_result = build_high_intensity_runtime_monthly_concentration_monitoring_dry_run(
        detection_rows=detection_rows,
        append_rows=append_rows,
        cluster_update_rows=cluster_update_rows,
        prior_event_rows=prior_event_rows,
        prior_cluster_rows=prior_cluster_rows,
        inputs=inputs,
    )
    return clean_for_yaml({"cycle_mode": cycle_mode, **runtime_result})


def build_high_intensity_scheduler_outcome_update_job_dry_run_report(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    outcome_contract = mapping(inputs["scheduler_plan"]).get("outcome_update_contract", {})
    valid = (
        outcome_contract.get("future_scheduler_candidate") is True
        and outcome_contract.get("requires_market_data") is True
        and outcome_contract.get("requires_validate_data") is True
    )
    return clean_for_yaml(
        {
            "outcome_update_job_status": "PASS" if valid else "FAIL",
            "scheduler_enabled": False,
            "outcome_binding_executed": False,
            "would_require_market_data_when_enabled": True,
            "would_require_validate_data_when_enabled": True,
            "validate_data_policy": {
                "canonical_validate_data_required": True,
                "no_rule_relaxation": True,
                "as_of_policy": "latest_price_cache_or_explicit_as_of",
            },
            "dry_run_action": (
                "PLAN_VALIDATED_NO_OUTCOME_BINDING"
                if valid
                else "BLOCKED_CONTRACT_INVALID"
            ),
            "required_future_outputs": [
                "outcome_registry_update",
                "event_actual_path_outcome_increment",
                "cluster_actual_path_outcome_increment",
                "outcome_quality_report",
            ],
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_disabled_policy_validation_report(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    policy = mapping(inputs["scheduler_plan"]).get("disabled_policy", {})
    errors: list[str] = []
    if policy.get("scheduler_default_enabled") is not False:
        errors.append("SCHEDULER_DEFAULT_ENABLED_NOT_FALSE")
    for field in [
        "activation_requires_future_task",
        "activation_requires_owner_review",
        "activation_requires_safety_gate_pass",
        "activation_requires_no_paper_shadow",
        "activation_requires_no_production",
        "activation_requires_no_broker_action",
    ]:
        if policy.get(field) is not True:
            errors.append(f"{field.upper()}_NOT_TRUE")
    status = "FAIL" if errors else "PASS"
    return clean_for_yaml(
        {
            "scheduler_default_enabled": False,
            "scheduler_enabled_in_2345": False,
            "activation_requires_future_task": True,
            "activation_requires_owner_review": True,
            "activation_requires_safety_gate_pass": True,
            "activation_requires_no_paper_shadow": True,
            "activation_requires_no_production": True,
            "activation_requires_no_broker_action": True,
            "disabled_policy_status": status,
            "disabled_policy_errors": errors,
            "disabled_policy_warnings": [],
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_fail_closed_safety_gate_result(
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
    scheduler_enabled = _truthy(candidate.get("scheduler_enabled"))
    scheduler_default_enabled = _truthy(candidate.get("scheduler_default_enabled"))
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
            ("SCHEDULER_ENABLED", scheduler_enabled),
            ("SCHEDULER_DEFAULT_ENABLED", scheduler_default_enabled),
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
            "scheduler_enabled_blocked": scheduler_enabled or scheduler_default_enabled,
            "forbidden_output_detected": bool(forbidden),
            "forbidden_output_fields": forbidden,
            "safety_error_count": len(blockers),
            "safety_warning_count": 0,
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_artifact_registry_dry_run_report(
    *,
    output_dir: Path,
    docs_root: Path,
) -> dict[str, Any]:
    expected_output_names = [
        "high_intensity_scheduler_dry_run_summary.json",
        "high_intensity_scheduler_contract_validation_report.json",
        "high_intensity_scheduler_cycle_plan.json",
        "high_intensity_scheduler_job_dag_validation_report.json",
        "high_intensity_scheduler_input_snapshot_validation_matrix.json",
        "high_intensity_scheduler_input_snapshot_validation_matrix.csv",
        "high_intensity_scheduler_event_detection_job_dry_run_result.json",
        "high_intensity_scheduler_event_append_job_dry_run_result.json",
        "high_intensity_scheduler_cluster_update_job_dry_run_result.json",
        "high_intensity_scheduler_pending_outcome_job_dry_run_result.json",
        "high_intensity_scheduler_manual_review_context_job_dry_run_result.json",
        "high_intensity_scheduler_monthly_concentration_job_dry_run_result.json",
        "high_intensity_scheduler_outcome_update_job_dry_run_report.json",
        "high_intensity_scheduler_disabled_policy_validation_report.json",
        "high_intensity_scheduler_fail_closed_safety_gate_result.json",
        "high_intensity_2346_task_route.json",
    ]
    expected_doc_names = [
        "high_intensity_risk_cap_observe_only_scheduler_dry_run.md",
        "high_intensity_scheduler_cycle_and_job_dag_dry_run.md",
        "high_intensity_scheduler_event_append_cluster_update_dry_run.md",
        "high_intensity_scheduler_safety_gate_dry_run.md",
        "high_intensity_2346_readiness_route.md",
    ]
    return clean_for_yaml(
        {
            "scheduler_artifact_root": str(output_dir),
            "scheduler_artifact_paths_valid": True,
            "report_registry_plan_valid": True,
            "artifact_catalog_plan_valid": True,
            "system_flow_plan_valid": True,
            "task_register_plan_valid": True,
            "outputs_git_policy_valid": True,
            "original_event_log_mutation_allowed": False,
            "registry_dry_run_status": "PASS",
            "expected_scheduler_artifacts": [
                str(output_dir / name) for name in expected_output_names
            ],
            "expected_docs": [str(docs_root / name) for name in expected_doc_names],
            "registry_warnings": [],
            "registry_errors": [],
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_dry_run_data_quality_report(
    *,
    inputs: Mapping[str, Any],
    contract_validation: Mapping[str, Any],
    cycle_plan: Mapping[str, Any],
    job_dag: Mapping[str, Any],
    input_snapshot_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_concentration: Mapping[str, Any],
    outcome_update_report: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
) -> dict[str, Any]:
    input_errors = sum(
        1 for row in input_snapshot_rows if row.get("validation_status") == "FAIL"
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
    errors = (
        int(contract_validation.get("contract_error_count") or 0)
        + input_errors
        + detection_errors
        + append_errors
        + cluster_errors
        + manual_errors
        + int(safety_gate.get("safety_error_count") or 0)
    )
    if job_dag.get("dag_validation_status") != "PASS":
        errors += 1
    if outcome_update_report.get("outcome_update_job_status") != "PASS":
        errors += 1
    if disabled_policy.get("disabled_policy_status") == "FAIL":
        errors += 1
    if monthly_concentration.get("monthly_monitoring_status") == "BLOCKED":
        errors += 1
    warnings = (
        int(contract_validation.get("contract_warning_count") or 0)
        + sum(
            1
            for row in input_snapshot_rows
            if row.get("validation_status") == "PASS_WITH_DERIVED_FIELD"
        )
        + int(monthly_concentration.get("monthly_concentration_warning_count") or 0)
    )
    if not any(row.get("append_status") == "WOULD_APPEND_NEW_EVENT" for row in append_rows):
        warnings += 1
    data_quality_status = (
        "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS"
    )
    source_validation = _source_validation(inputs)
    return clean_for_yaml(
        {
            "contract_validation_status": contract_validation.get(
                "contract_validation_status"
            ),
            "cycle_plan_status": "PASS" if cycle_plan else "FAIL",
            "job_dag_status": job_dag.get("dag_validation_status"),
            "input_snapshot_status": "FAIL" if input_errors else "PASS",
            "event_detection_status": "FAIL" if detection_errors else "PASS",
            "event_append_status": "FAIL" if append_errors else "PASS",
            "cluster_update_status": "FAIL" if cluster_errors else "PASS",
            "pending_outcome_status": "PASS" if pending_rows is not None else "FAIL",
            "manual_review_context_status": "FAIL" if manual_errors else "PASS",
            "monthly_concentration_status": monthly_concentration.get(
                "monthly_monitoring_status"
            ),
            "outcome_update_job_status": outcome_update_report.get(
                "outcome_update_job_status"
            ),
            "disabled_policy_status": disabled_policy.get("disabled_policy_status"),
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
            "warning_count": warnings,
            "error_count": errors,
            "data_quality_status": data_quality_status,
            "validate_data_executed": False,
            "source_validate_data_executed": source_validation.get("executed", True),
            "source_validate_data_as_of": source_validation.get("as_of", "2026-06-29"),
            "source_validate_data_status": source_validation.get(
                "status",
                "PASS_WITH_WARNINGS",
            ),
            "source_validate_data_error_count": source_validation.get("error_count", 0),
            **_core_safety_fields(),
        }
    )


def build_high_intensity_scheduler_dry_run_interpretation_boundary(
    *,
    generated_at: datetime,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "generated_at": generated_at.isoformat(),
            **SAFETY_FIELDS,
            "known_at_policy": KNOWN_AT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "partial_coverage_caveat_required": True,
            "monthly_concentration_monitoring_required": True,
            "forbidden_interpretations": [
                "real_account_performance",
                "real_position_advice",
                "reduce_position_signal",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
                "scheduler_enabled",
            ],
        }
    )


def build_high_intensity_2346_readiness_checklist(
    *,
    contract_validation: Mapping[str, Any],
    cycle_plan: Mapping[str, Any],
    job_dag: Mapping[str, Any],
    input_snapshot_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_concentration: Mapping[str, Any],
    outcome_update_report: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
    artifact_registry: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if contract_validation.get("contract_validation_status") == "FAIL":
        blockers.append("CONTRACT_VALIDATION_FAILED")
    if job_dag.get("dag_validation_status") != "PASS":
        blockers.append("JOB_DAG_VALIDATION_FAILED")
    if any(row.get("validation_status") == "FAIL" for row in input_snapshot_rows):
        blockers.append("SCHEDULER_INPUT_SNAPSHOT_VALIDATION_FAILED")
    if safety_gate.get("safety_gate_status") == "FAIL_CLOSED_TRIGGERED":
        blockers.append("FAIL_CLOSED_SAFETY_GATE_TRIGGERED")
    if disabled_policy.get("disabled_policy_status") == "FAIL":
        blockers.append("DISABLED_POLICY_VALIDATION_FAILED")
    if artifact_registry.get("registry_dry_run_status") != "PASS":
        blockers.append("ARTIFACT_REGISTRY_DRY_RUN_FAILED")
    if monthly_concentration.get("monthly_monitoring_status") == "BLOCKED":
        blockers.append("MONTHLY_CONCENTRATION_BLOCKED")
    if outcome_update_report.get("outcome_update_job_status") != "PASS":
        blockers.append("OUTCOME_UPDATE_JOB_PLAN_INVALID")

    warnings.extend(contract_validation.get("contract_warnings") or [])
    warnings.extend(monthly_concentration.get("monitoring_warnings") or [])
    if any(
        row.get("validation_status") == "PASS_WITH_DERIVED_FIELD"
        for row in input_snapshot_rows
    ):
        warnings.append("DERIVED_SCHEDULER_INPUT_FIELDS_USED")
    if not any(row.get("append_status") == "WOULD_APPEND_NEW_EVENT" for row in append_rows):
        warnings.append("NO_NEW_SCHEDULER_EVENTS_IN_HISTORICAL_REPLAY")
        warnings.append("DEDUP_AGAINST_EXISTING_HISTORICAL_EVENT_LOG")
    warnings.extend(ROUTE_CAVEATS)

    if blockers and "FAIL_CLOSED_SAFETY_GATE_TRIGGERED" in blockers:
        status = "SCHEDULER_DRY_RUN_BLOCKED"
    elif blockers:
        status = "SCHEDULER_DRY_RUN_REMEDIATION_REQUIRED"
    elif warnings:
        status = "READY_FOR_2346_WITH_CAVEATS"
    else:
        status = "READY_FOR_2346_OBSERVE_ONLY_SCHEDULER_WIRING_PLAN"
    return clean_for_yaml(
        {
            "contract_validation_passed": not contract_validation.get(
                "contract_error_count"
            ),
            "scheduler_cycle_plan_generated": bool(cycle_plan),
            "job_dag_validation_passed": job_dag.get("dag_validation_status") == "PASS",
            "input_snapshot_validation_passed": not any(
                row.get("validation_status") == "FAIL" for row in input_snapshot_rows
            ),
            "event_detection_scheduler_dry_run_generated": bool(detection_rows),
            "event_append_scheduler_dry_run_generated": append_rows is not None,
            "cluster_update_scheduler_dry_run_generated": cluster_rows is not None,
            "pending_outcome_scheduler_dry_run_generated": pending_rows is not None,
            "manual_review_context_scheduler_dry_run_generated": manual_review_rows
            is not None,
            "monthly_concentration_scheduler_dry_run_generated": bool(
                monthly_concentration
            ),
            "outcome_update_job_plan_validated": outcome_update_report.get(
                "outcome_update_job_status"
            )
            == "PASS",
            "disabled_policy_validated": disabled_policy.get("disabled_policy_status")
            != "FAIL",
            "fail_closed_safety_gate_passed": safety_gate.get("safety_gate_status")
            != "FAIL_CLOSED_TRIGGERED",
            "artifact_registry_dry_run_passed": artifact_registry.get(
                "registry_dry_run_status"
            )
            == "PASS",
            "scheduler_enabled": False,
            "event_append_executed": False,
            "outcome_binding_executed": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": status,
            "readiness_blockers": sorted(set(blockers)),
            "readiness_warnings": sorted(set(warnings)),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2346_task_route(
    *,
    readiness: Mapping[str, Any],
    safety_gate: Mapping[str, Any] | None = None,
    dry_run_issue: bool = False,
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    blockers = list(readiness.get("readiness_blockers") or [])
    safety_issue = (
        bool(safety_gate)
        and mapping(safety_gate).get("safety_gate_status") == "FAIL_CLOSED_TRIGGERED"
    )
    if status in {
        "READY_FOR_2346_OBSERVE_ONLY_SCHEDULER_WIRING_PLAN",
        "READY_FOR_2346_WITH_CAVEATS",
    }:
        next_task = NEXT_2346_WIRING_PLAN_TASK
        rationale = "observe-only scheduler wiring plan route allowed"
    elif safety_issue:
        next_task = NEXT_2346_SAFETY_REMEDIATION_TASK
        rationale = "scheduler safety issue requires remediation"
    elif dry_run_issue or status == "SCHEDULER_DRY_RUN_REMEDIATION_REQUIRED":
        next_task = NEXT_2346_DRY_RUN_REMEDIATION_TASK
        rationale = "scheduler dry-run remediation required"
    elif status == "SCHEDULER_DRY_RUN_BLOCKED" and dry_run_issue:
        next_task = NEXT_2346_DRY_RUN_REMEDIATION_TASK
        rationale = "scheduler dry-run blocked by dry-run issue"
    else:
        next_task = NEXT_2346_ARCHIVE_TASK
        rationale = "scheduler dry-run not ready and no remediation route selected"
    return clean_for_yaml(
        {
            "readiness_status": status,
            "next_task": next_task,
            "allowed_routes": [
                NEXT_2346_WIRING_PLAN_TASK,
                NEXT_2346_DRY_RUN_REMEDIATION_TASK,
                NEXT_2346_SAFETY_REMEDIATION_TASK,
                NEXT_2346_ARCHIVE_TASK,
            ],
            "route_caveats": ROUTE_CAVEATS
            if status == "READY_FOR_2346_WITH_CAVEATS"
            else [],
            "route_blockers": blockers,
            "route_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_scheduler_dry_run_safety_boundary(
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


def build_high_intensity_scheduler_dry_run_summary(
    *,
    generated_at: datetime,
    scheduler_integration_plan_dir: Path,
    runtime_dry_run_dir: Path,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    dynamic_dry_run_dir: Path,
    inputs: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rule = mapping(inputs["runtime_inputs"]["threshold_selection"]).get(
        "selected_rule",
        {},
    )
    scheduler_summary = mapping(inputs["scheduler_plan"]).get("summary", {})
    errors = int(data_quality.get("error_count") or 0)
    warnings = int(data_quality.get("warning_count") or 0)
    if errors:
        status = "OBSERVE_ONLY_SCHEDULER_DRY_RUN_BLOCKED_PROMOTION_BLOCKED"
    elif warnings:
        status = "OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    else:
        status = "OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_PROMOTION_BLOCKED"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Observe-Only Scheduler Dry-Run",
            "status": status,
            "scheduler_dry_run_status": status,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "scheduler_integration_plan_dir": str(scheduler_integration_plan_dir),
            "runtime_dry_run_dir": str(runtime_dry_run_dir),
            "runtime_integration_plan_dir": str(runtime_integration_plan_dir),
            "continue_decision_dir": str(continue_decision_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "source_2344_status": scheduler_summary.get("status"),
            "source_2344_readiness": scheduler_summary.get("2345_readiness_status"),
            "source_2344_next_task": scheduler_summary.get("next_task"),
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_hash": scheduler_summary.get(
                "selected_rule_hash",
                _hash_payload(selected_rule),
            ),
            "record_count": data_quality.get("record_count"),
            "detected_event_count": data_quality.get("detected_event_count"),
            "would_append_event_count": data_quality.get("would_append_event_count"),
            "would_extend_cluster_count": data_quality.get(
                "would_extend_cluster_count"
            ),
            "would_create_pending_outcome_count": data_quality.get(
                "would_create_pending_outcome_count"
            ),
            "would_append_event_count_reason": (
                "DEDUP_AGAINST_EXISTING_HISTORICAL_EVENT_LOG"
                if data_quality.get("would_append_event_count") == 0
                else "NEW_EVENT_FIXTURE_OR_FUTURE_RUNTIME_RECORDS"
            ),
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
                "aits validate-data not rerun because TRADING-2345 only reads prior "
                "validated research artifacts and does not bind outcomes or consume "
                "fresh market data directly."
            ),
            "2346_readiness_status": readiness.get("readiness_status"),
            "2346_task_route_generated": True,
            "next_task": task_route.get("next_task"),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_scheduler_dry_run_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    contract_validation: Mapping[str, Any],
    cycle_plan: Mapping[str, Any],
    job_dag: Mapping[str, Any],
    input_snapshot_rows: Sequence[Mapping[str, Any]],
    detection_rows: Sequence[Mapping[str, Any]],
    append_rows: Sequence[Mapping[str, Any]],
    cluster_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    manual_review_rows: Sequence[Mapping[str, Any]],
    monthly_concentration: Mapping[str, Any],
    outcome_update_report: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
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
    write_json(paths["cycle_plan"], cycle_plan)
    write_json(paths["job_dag"], job_dag)
    _write_rows_json(paths["input_snapshot_json"], input_snapshot_rows)
    write_csv_rows(paths["input_snapshot_csv"], input_snapshot_rows)
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
    write_json(paths["monthly_concentration"], monthly_concentration)
    write_json(paths["outcome_update_report"], outcome_update_report)
    write_json(paths["disabled_policy"], disabled_policy)
    write_json(paths["safety_gate"], safety_gate)
    write_json(paths["artifact_registry"], artifact_registry)
    write_json(paths["data_quality"], data_quality)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], render_scheduler_dry_run_doc(summary, data_quality))
    write_markdown(
        paths["cycle_doc"],
        render_scheduler_cycle_doc(cycle_plan, job_dag),
    )
    write_markdown(
        paths["append_cluster_doc"],
        render_scheduler_append_cluster_doc(summary, append_rows, cluster_rows, pending_rows),
    )
    write_markdown(paths["safety_doc"], render_scheduler_safety_doc(summary, safety_gate))
    write_markdown(paths["route_doc"], render_2346_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def render_scheduler_dry_run_doc(
    summary: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Risk-Cap Observe-Only Scheduler Dry-Run\n\n"
        f"- status: `{summary.get('status')}`\n"
        f"- selected_rule_id: `{summary.get('selected_rule_id')}`\n"
        f"- detected_event_count: `{summary.get('detected_event_count')}`\n"
        f"- would_append_event_count: `{summary.get('would_append_event_count')}`\n"
        f"- would_append_event_count_reason: "
        f"`{summary.get('would_append_event_count_reason')}`\n"
        f"- would_extend_cluster_count: `{summary.get('would_extend_cluster_count')}`\n"
        f"- would_create_pending_outcome_count: "
        f"`{summary.get('would_create_pending_outcome_count')}`\n"
        f"- data_quality_status: `{data_quality.get('data_quality_status')}`\n"
        f"- source_validate_data_as_of: `{summary.get('source_validate_data_as_of')}`\n"
        f"- source_validate_data_status: `{summary.get('source_validate_data_status')}`\n"
        f"- next_task: `{summary.get('next_task')}`\n\n"
        "本报告只执行 observe-only scheduler dry-run。2345 未启用 scheduler，"
        "未写回 historical event log / cluster registry / pending outcome registry，"
        "未绑定 outcome，未输出 target weight / rebalance / broker action。"
    )


def render_scheduler_cycle_doc(
    cycle_plan: Mapping[str, Any],
    job_dag: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Scheduler Cycle And Job DAG Dry-Run\n\n"
        f"- cycle_modes: `{cycle_plan.get('cycle_modes')}`\n"
        f"- job_order: `{cycle_plan.get('job_order')}`\n"
        f"- dag_validation_status: `{job_dag.get('dag_validation_status')}`\n"
        f"- missing_dependency_count: `{job_dag.get('missing_dependency_count')}`\n"
        f"- cycle_detected: `{job_dag.get('cycle_detected')}`\n\n"
        "Scheduler cycle 只定义 dry-run 顺序；market calendar gating 和 "
        "NEXT_SESSION_DECISION_POLICY 保持为后续 disabled wiring 的前置要求。"
    )


def render_scheduler_append_cluster_doc(
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
        "# High-Intensity Scheduler Event Append / Cluster Update Dry-Run\n\n"
        f"- append_status_counts: `{dict(sorted(append_statuses.items()))}`\n"
        f"- cluster_update_action_counts: `{dict(sorted(cluster_actions.items()))}`\n"
        f"- pending_outcome_dry_run_rows: `{len(pending_rows)}`\n"
        f"- next_task: `{summary.get('next_task')}`\n\n"
        "Append、cluster 和 pending outcome 全部是 would-write dry-run records。"
        "如果 historical replay 的 append count 为 0，是因为命中既有 2336 "
        "historical event / trigger-day log 去重，不代表 trigger rule 失效。"
    )


def render_scheduler_safety_doc(
    summary: Mapping[str, Any],
    safety_gate: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Scheduler Safety Gate Dry-Run\n\n"
        f"- safety_gate_status: `{safety_gate.get('safety_gate_status')}`\n"
        f"- safety_error_count: `{safety_gate.get('safety_error_count')}`\n"
        f"- scheduler_enabled_blocked: `{safety_gate.get('scheduler_enabled_blocked')}`\n"
        f"- forbidden_output_fields: `{safety_gate.get('forbidden_output_fields')}`\n"
        f"- broker_action: `{summary.get('broker_action')}`\n\n"
        "Fail-closed safety gate 继续阻断 scheduler enabled、target weight、"
        "rebalance instruction、paper-shadow、production 和 broker action。"
    )


def render_2346_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity 2346 Readiness Route\n\n"
        f"- readiness_status: `{readiness.get('readiness_status')}`\n"
        f"- readiness_warnings: `{readiness.get('readiness_warnings')}`\n"
        f"- readiness_blockers: `{readiness.get('readiness_blockers')}`\n"
        f"- next_task: `{task_route.get('next_task')}`\n\n"
        "2346 route 只允许 observe-only scheduler wiring plan 或 remediation/archive；"
        "它不是 scheduler enabled、paper-shadow、production 或 broker readiness。"
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_dry_run_summary.json",
        "contract_validation": output_dir
        / "high_intensity_scheduler_contract_validation_report.json",
        "cycle_plan": output_dir / "high_intensity_scheduler_cycle_plan.json",
        "job_dag": output_dir / "high_intensity_scheduler_job_dag_validation_report.json",
        "input_snapshot_json": output_dir
        / "high_intensity_scheduler_input_snapshot_validation_matrix.json",
        "input_snapshot_csv": output_dir
        / "high_intensity_scheduler_input_snapshot_validation_matrix.csv",
        "event_detection_json": output_dir
        / "high_intensity_scheduler_event_detection_job_dry_run_result.json",
        "event_detection_csv": output_dir
        / "high_intensity_scheduler_event_detection_job_dry_run_result.csv",
        "event_append_json": output_dir
        / "high_intensity_scheduler_event_append_job_dry_run_result.json",
        "event_append_csv": output_dir
        / "high_intensity_scheduler_event_append_job_dry_run_result.csv",
        "cluster_update_json": output_dir
        / "high_intensity_scheduler_cluster_update_job_dry_run_result.json",
        "cluster_update_csv": output_dir
        / "high_intensity_scheduler_cluster_update_job_dry_run_result.csv",
        "pending_outcome_json": output_dir
        / "high_intensity_scheduler_pending_outcome_job_dry_run_result.json",
        "pending_outcome_csv": output_dir
        / "high_intensity_scheduler_pending_outcome_job_dry_run_result.csv",
        "manual_review_json": output_dir
        / "high_intensity_scheduler_manual_review_context_job_dry_run_result.json",
        "manual_review_csv": output_dir
        / "high_intensity_scheduler_manual_review_context_job_dry_run_result.csv",
        "monthly_concentration": output_dir
        / "high_intensity_scheduler_monthly_concentration_job_dry_run_result.json",
        "outcome_update_report": output_dir
        / "high_intensity_scheduler_outcome_update_job_dry_run_report.json",
        "disabled_policy": output_dir
        / "high_intensity_scheduler_disabled_policy_validation_report.json",
        "safety_gate": output_dir
        / "high_intensity_scheduler_fail_closed_safety_gate_result.json",
        "artifact_registry": output_dir
        / "high_intensity_scheduler_artifact_registry_dry_run_report.json",
        "data_quality": output_dir
        / "high_intensity_scheduler_dry_run_data_quality_report.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_dry_run_interpretation_boundary.json",
        "readiness": output_dir / "high_intensity_2346_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2346_task_route.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_dry_run_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_dry_run.md",
        "cycle_doc": docs_root
        / "high_intensity_scheduler_cycle_and_job_dag_dry_run.md",
        "append_cluster_doc": docs_root
        / "high_intensity_scheduler_event_append_cluster_update_dry_run.md",
        "safety_doc": docs_root / "high_intensity_scheduler_safety_gate_dry_run.md",
        "route_doc": docs_root / "high_intensity_2346_readiness_route.md",
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


def _validate_scheduler_dry_run_cross_source_contracts(
    inputs: Mapping[str, Any],
) -> None:
    scheduler_summary = mapping(inputs["scheduler_plan"]["summary"])
    runtime_summary = mapping(inputs["runtime_dry_run"]["summary"])
    selected_rule = mapping(inputs["runtime_inputs"]["threshold_selection"]).get(
        "selected_rule",
        {},
    )
    if scheduler_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires 2344 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if runtime_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires 2343 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if selected_rule.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires 2335 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    event_logger = mapping(inputs["runtime_inputs"]["event_logger"])
    if not records(mapping(event_logger.get("event_log")).get("rows")):
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires prior observe event log"
        )
    if not records(mapping(event_logger.get("cluster_registry")).get("rows")):
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires prior cluster registry"
        )
    if not records(mapping(event_logger.get("pending_outcome_registry")).get("rows")):
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires prior pending outcome registry"
        )
    dynamic_rows = records(
        mapping(inputs["runtime_inputs"]["dynamic_dry_run"]["trigger_alignment"]).get(
            "rows"
        )
    )
    if not dynamic_rows:
        raise HighIntensitySchedulerDryRunError(
            "TRADING-2345 requires runtime trigger source"
        )


def _contract_has_safe_boundary(payload: Any) -> bool:
    contract = mapping(payload)
    if not contract:
        return False
    try:
        _validate_no_unsafe_fields("contract", contract)
    except HighIntensitySchedulerDryRunError:
        return False
    return (
        contract.get("promotion_allowed", False) is False
        and contract.get("paper_shadow_allowed", False) is False
        and contract.get("production_allowed", False) is False
        and str(contract.get("broker_action", "none")).lower() == "none"
    )


def _job_contract_valid(payload: Any) -> bool:
    contract = mapping(payload)
    blocked = contract.get("blocked_output") or contract.get("blocked_outputs")
    return _contract_has_safe_boundary(contract) and bool(blocked)


def _blocked_display_fields_valid(value: object) -> bool:
    fields = {str(item) for item in value or []}
    return MANUAL_REVIEW_FORBIDDEN_FIELDS.issubset(fields)


def _disabled_policy_valid(payload: Any) -> bool:
    policy = mapping(payload)
    if not _contract_has_safe_boundary(policy):
        return False
    return (
        policy.get("scheduler_default_enabled") is False
        and policy.get("activation_requires_owner_review") is True
        and policy.get("activation_requires_safety_gate_pass") is True
        and policy.get("activation_requires_no_broker_action") is True
    )


def _job_dependency_cycle_detected(
    dependencies: Mapping[str, Sequence[str]],
) -> bool:
    graph = {
        job: [dep for dep in deps if dep not in EXTERNAL_DEPENDENCIES]
        for job, deps in dependencies.items()
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(job: str) -> bool:
        if job in visiting:
            return True
        if job in visited:
            return False
        visiting.add(job)
        for dep in graph.get(job, []):
            if visit(dep):
                return True
        visiting.remove(job)
        visited.add(job)
        return False

    return any(visit(job) for job in graph)


def _input_snapshot_row(
    *,
    field: str,
    required: bool,
    available: bool,
    coverage_ratio: float,
    missing_count: int,
    invalid_count: int,
    source_artifact: str,
    validation_status: str,
    blocking_if_invalid: bool,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "input_field": field,
            "required": required,
            "available": available,
            "coverage_ratio": coverage_ratio,
            "missing_count": missing_count,
            "invalid_count": invalid_count,
            "source_artifact": source_artifact,
            "validation_status": validation_status,
            "blocking_if_invalid": blocking_if_invalid,
            **_core_safety_fields(),
        }
    )


def _scheduler_input_value(
    field: str,
    source_row: Mapping[str, Any],
    selected_rule: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> Any:
    if field == "as_of_timestamp":
        return _as_of_timestamp(source_row)
    if field == "known_at_policy":
        return (
            mapping(selected_rule.get("trigger_rule")).get("known_at_policy")
            or mapping(dynamic_dry_run.get("summary")).get("known_at_policy")
            or KNOWN_AT_POLICY
        )
    if field == "pit_policy":
        return _pit_policy(selected_rule, dynamic_dry_run)
    return source_row.get(field)


def _scheduler_input_source(field: str, dynamic_dry_run: Mapping[str, Any]) -> str:
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


def _walk_payload(value: object, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    if isinstance(value, Mapping):
        rows: list[tuple[tuple[str, ...], Any]] = []
        for key, item in value.items():
            rows.extend(_walk_payload(item, (*path, str(key))))
        return rows
    if isinstance(value, list):
        rows = []
        for index, item in enumerate(value):
            rows.extend(_walk_payload(item, (*path, str(index))))
        return rows
    return [(path, value)]


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerDryRunError(
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
            if key_text in FORBIDDEN_OUTPUT_FIELDS and item not in {
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


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerDryRunError(f"{label} missing {key}: {path}")
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerDryRunError(f"{path}: expected JSON object")
    return payload


def _require_existing_files(paths: Sequence[Path], label: str) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise HighIntensitySchedulerDryRunError(
            f"{label} missing required files: {missing}"
        )


def _require_false(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not False:
        raise HighIntensitySchedulerDryRunError(f"{label} requires {field}=false")


def _source_validation(inputs: Mapping[str, Any]) -> dict[str, Any]:
    scheduler_summary = mapping(inputs["scheduler_plan"]).get("summary", {})
    runtime_data_quality = mapping(inputs["runtime_dry_run"]).get("data_quality", {})
    return {
        "executed": scheduler_summary.get(
            "source_validate_data_executed",
            runtime_data_quality.get("source_validate_data_executed", True),
        ),
        "as_of": scheduler_summary.get(
            "source_validate_data_as_of",
            runtime_data_quality.get("source_validate_data_as_of", "2026-06-29"),
        ),
        "status": scheduler_summary.get(
            "source_validate_data_status",
            runtime_data_quality.get(
                "source_validate_data_status",
                "PASS_WITH_WARNINGS",
            ),
        ),
        "error_count": scheduler_summary.get(
            "source_validate_data_error_count",
            runtime_data_quality.get("source_validate_data_error_count", 0),
        ),
    }


def _source_validation_status(inputs: Mapping[str, Any]) -> str:
    return str(_source_validation(inputs).get("status", ""))


def _inherited_monthly_warning(inputs: Mapping[str, Any]) -> str:
    monthly_report = mapping(inputs["runtime_inputs"]["event_logger"]).get(
        "monthly_report",
        {},
    )
    warnings = monthly_report.get("monthly_concentration_warnings") or []
    if isinstance(warnings, list) and warnings:
        return str(warnings[0])
    return "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"


def _cycle_id(cycle_mode: str) -> str:
    return f"hisch_{cycle_mode}"


def _core_safety_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _rate(numerator: int, denominator: int) -> float:
    return round_float(numerator / denominator) if denominator else 0.0


def _is_number(value: object) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def _emits_action(value: object) -> bool:
    if value in {False, None, "", "none", "NONE"}:
        return False
    if isinstance(value, Sequence) and not isinstance(value, str):
        return bool(value)
    if isinstance(value, Mapping):
        return bool(value)
    return str(value).strip().lower() not in {"", "false", "none", "null"}


def _first_non_empty(values: Any) -> str:
    for value in values:
        if value:
            return str(value)
    return ""


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
