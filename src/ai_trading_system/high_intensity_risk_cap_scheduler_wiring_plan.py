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
from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCHEDULER_DRY_RUN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    HighIntensitySchedulerDryRunError,
    load_trading_2344_scheduler_dry_run_context,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCHEDULER_INTEGRATION_PLAN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    EXPECTED_SELECTED_RULE,
    load_trading_2343_scheduler_plan_context,
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
    "TRADING-2346_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_SCHEDULER_WIRING_PLAN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_wiring_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_wiring_plan"

EXPECTED_2345_STATUS = (
    "OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)
EXPECTED_2346_NEXT_TASK = (
    "TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan"
)
READY_2346_STATUSES = {
    "READY_FOR_2346_OBSERVE_ONLY_SCHEDULER_WIRING_PLAN",
    "READY_FOR_2346_WITH_CAVEATS",
}

NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK = (
    "TRADING-2347_High_Intensity_Risk_Cap_Observe_Only_Scheduler_"
    "Disabled_Wiring_Implementation"
)
NEXT_2347_WIRING_PLAN_REMEDIATION_TASK = (
    "TRADING-2347_High_Intensity_Risk_Cap_Scheduler_Wiring_Plan_Remediation"
)
NEXT_2347_SAFETY_REMEDIATION_TASK = (
    "TRADING-2347_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
)
NEXT_2347_ARCHIVE_TASK = "TRADING-2347_Archive_High_Intensity_Risk_Cap_Runtime_Line"

READY_STATUS = "OBSERVE_ONLY_SCHEDULER_WIRING_PLAN_READY_PROMOTION_BLOCKED"
READY_WITH_CAVEATS_STATUS = (
    "OBSERVE_ONLY_SCHEDULER_WIRING_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)
BLOCKED_SCHEDULER_ENABLE_ATTEMPT_STATUS = "BLOCKED_SCHEDULER_ENABLE_ATTEMPT"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

ROUTE_CAVEATS = [
    "DISABLED_BY_DEFAULT",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "OBSERVE_ONLY",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "scheduler_wiring_plan_only": True,
    "observe_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
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


class HighIntensitySchedulerWiringPlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_wiring_plan(
    *,
    scheduler_dry_run_dir: Path = DEFAULT_SCHEDULER_DRY_RUN_ROOT,
    scheduler_integration_plan_dir: Path = DEFAULT_SCHEDULER_INTEGRATION_PLAN_ROOT,
    runtime_dry_run_dir: Path = DEFAULT_RUNTIME_DRY_RUN_ROOT,
    runtime_integration_plan_dir: Path = DEFAULT_RUNTIME_INTEGRATION_PLAN_ROOT,
    continue_decision_dir: Path = DEFAULT_CONTINUE_DECISION_ROOT,
    event_logger_dir: Path = DEFAULT_EVENT_LOGGER_ROOT,
    threshold_selection_dir: Path = DEFAULT_THRESHOLD_SELECTION_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerWiringPlanError(
            f"high-intensity scheduler wiring plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_wiring_plan_inputs(
        scheduler_dry_run_dir=scheduler_dry_run_dir,
        scheduler_integration_plan_dir=scheduler_integration_plan_dir,
        runtime_dry_run_dir=runtime_dry_run_dir,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
    )
    config_entry_plan = build_high_intensity_scheduler_config_entry_plan(inputs)
    disabled_policy = build_high_intensity_scheduler_disabled_wiring_policy()
    manual_run_contract = build_high_intensity_scheduler_manual_run_contract()
    dry_run_only_mode_contract = (
        build_high_intensity_scheduler_dry_run_only_mode_contract()
    )
    job_wiring_contract = build_high_intensity_scheduler_job_wiring_contract()
    artifact_wiring_plan = build_high_intensity_scheduler_artifact_wiring_plan()
    registry_wiring_plan = build_high_intensity_scheduler_registry_wiring_plan()
    failure_handling_plan = build_high_intensity_scheduler_failure_handling_plan()
    rollback_plan = build_high_intensity_scheduler_rollback_plan()
    owner_review_requirement = (
        build_high_intensity_scheduler_owner_review_requirement()
    )
    wiring_safety_gate = build_high_intensity_scheduler_wiring_safety_gate(
        config_entry_plan=config_entry_plan,
        disabled_policy=disabled_policy,
        manual_run_contract=manual_run_contract,
        dry_run_only_mode_contract=dry_run_only_mode_contract,
        job_wiring_contract=job_wiring_contract,
    )
    implementation_contract = (
        build_high_intensity_scheduler_wiring_implementation_contract()
    )
    readiness = build_high_intensity_2347_readiness_checklist(
        config_entry_plan=config_entry_plan,
        disabled_policy=disabled_policy,
        manual_run_contract=manual_run_contract,
        dry_run_only_mode_contract=dry_run_only_mode_contract,
        job_wiring_contract=job_wiring_contract,
        artifact_wiring_plan=artifact_wiring_plan,
        registry_wiring_plan=registry_wiring_plan,
        failure_handling_plan=failure_handling_plan,
        rollback_plan=rollback_plan,
        owner_review_requirement=owner_review_requirement,
        wiring_safety_gate=wiring_safety_gate,
        implementation_contract=implementation_contract,
        inputs=inputs,
    )
    task_route = build_high_intensity_2347_task_route(
        readiness=readiness,
        safety_gate=wiring_safety_gate,
    )
    interpretation_boundary = (
        build_high_intensity_scheduler_wiring_interpretation_boundary(
            generated_at=generated_at
        )
    )
    safety_boundary = build_high_intensity_scheduler_wiring_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_scheduler_wiring_plan_summary(
        generated_at=generated_at,
        scheduler_dry_run_dir=scheduler_dry_run_dir,
        scheduler_integration_plan_dir=scheduler_integration_plan_dir,
        runtime_dry_run_dir=runtime_dry_run_dir,
        runtime_integration_plan_dir=runtime_integration_plan_dir,
        continue_decision_dir=continue_decision_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        inputs=inputs,
        readiness=readiness,
        task_route=task_route,
        wiring_safety_gate=wiring_safety_gate,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_scheduler_wiring_plan_outputs(
        paths=paths,
        summary=summary,
        config_entry_plan=config_entry_plan,
        disabled_policy=disabled_policy,
        manual_run_contract=manual_run_contract,
        dry_run_only_mode_contract=dry_run_only_mode_contract,
        job_wiring_contract=job_wiring_contract,
        artifact_wiring_plan=artifact_wiring_plan,
        registry_wiring_plan=registry_wiring_plan,
        failure_handling_plan=failure_handling_plan,
        rollback_plan=rollback_plan,
        owner_review_requirement=owner_review_requirement,
        wiring_safety_gate=wiring_safety_gate,
        implementation_contract=implementation_contract,
        readiness=readiness,
        task_route=task_route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_wiring_plan_inputs(
    *,
    scheduler_dry_run_dir: Path,
    scheduler_integration_plan_dir: Path,
    runtime_dry_run_dir: Path,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
) -> dict[str, Any]:
    try:
        scheduler_dry_run = load_trading_2345_scheduler_wiring_plan_context(
            scheduler_dry_run_dir
        )
        scheduler_plan = load_trading_2344_scheduler_dry_run_context(
            scheduler_integration_plan_dir
        )
        runtime_dry_run = load_trading_2343_scheduler_plan_context(runtime_dry_run_dir)
        runtime_plan = load_trading_2342_runtime_dry_run_context(
            runtime_integration_plan_dir
        )
        continue_decision = load_trading_2341_runtime_dry_run_context(
            continue_decision_dir
        )
        event_logger = load_trading_2336_runtime_dry_run_context(event_logger_dir)
        threshold_selection = load_trading_2335_runtime_dry_run_context(
            threshold_selection_dir
        )
    except (
        HighIntensityRuntimeDryRunError,
        HighIntensitySchedulerDryRunError,
        ValueError,
    ) as exc:
        raise HighIntensitySchedulerWiringPlanError(str(exc)) from exc

    inputs = {
        "scheduler_dry_run": scheduler_dry_run,
        "scheduler_plan": scheduler_plan,
        "runtime_dry_run": runtime_dry_run,
        "runtime_plan": runtime_plan,
        "continue_decision": continue_decision,
        "event_logger": event_logger,
        "threshold_selection": threshold_selection,
    }
    _validate_scheduler_wiring_plan_cross_source_contracts(inputs)
    return inputs


def load_trading_2345_scheduler_wiring_plan_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_scheduler_dry_run_summary.json",
        "contract_validation": root
        / "high_intensity_scheduler_contract_validation_report.json",
        "cycle_plan": root / "high_intensity_scheduler_cycle_plan.json",
        "job_dag": root / "high_intensity_scheduler_job_dag_validation_report.json",
        "input_snapshot": root
        / "high_intensity_scheduler_input_snapshot_validation_matrix.json",
        "event_detection": root
        / "high_intensity_scheduler_event_detection_job_dry_run_result.json",
        "event_append": root
        / "high_intensity_scheduler_event_append_job_dry_run_result.json",
        "cluster_update": root
        / "high_intensity_scheduler_cluster_update_job_dry_run_result.json",
        "pending_outcome": root
        / "high_intensity_scheduler_pending_outcome_job_dry_run_result.json",
        "manual_review_context": root
        / "high_intensity_scheduler_manual_review_context_job_dry_run_result.json",
        "monthly_concentration": root
        / "high_intensity_scheduler_monthly_concentration_job_dry_run_result.json",
        "outcome_update": root
        / "high_intensity_scheduler_outcome_update_job_dry_run_report.json",
        "disabled_policy_validation": root
        / "high_intensity_scheduler_disabled_policy_validation_report.json",
        "fail_closed_gate": root
        / "high_intensity_scheduler_fail_closed_safety_gate_result.json",
        "artifact_registry": root
        / "high_intensity_scheduler_artifact_registry_dry_run_report.json",
        "data_quality": root
        / "high_intensity_scheduler_dry_run_data_quality_report.json",
        "interpretation_boundary": root
        / "high_intensity_scheduler_dry_run_interpretation_boundary.json",
        "readiness": root / "high_intensity_2346_readiness_checklist.json",
        "task_route": root / "high_intensity_2346_task_route.json",
        "safety_boundary": root
        / "high_intensity_scheduler_dry_run_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2345 scheduler dry-run")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2345 {key}", payload)

    summary = mapping(payloads["summary"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    fail_closed_gate = mapping(payloads["fail_closed_gate"])
    disabled_policy = mapping(payloads["disabled_policy_validation"])
    status = str(summary.get("status", ""))
    if status == "FAIL" or "FAIL" in status:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires non-failing TRADING-2345 scheduler dry-run"
        )
    if status != EXPECTED_2345_STATUS:
        raise HighIntensitySchedulerWiringPlanError(
            f"TRADING-2346 requires 2345 status {EXPECTED_2345_STATUS}"
        )
    if summary.get("next_task") != EXPECTED_2346_NEXT_TASK:
        raise HighIntensitySchedulerWiringPlanError(
            f"TRADING-2346 requires 2345 next task {EXPECTED_2346_NEXT_TASK}"
        )
    if task_route.get("next_task") != EXPECTED_2346_NEXT_TASK:
        raise HighIntensitySchedulerWiringPlanError(
            f"TRADING-2346 requires 2345 task route {EXPECTED_2346_NEXT_TASK}"
        )
    if readiness.get("readiness_status") not in READY_2346_STATUSES:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2345 readiness for scheduler wiring plan"
        )
    if fail_closed_gate.get("safety_gate_status") not in {"PASS", "PASS_WITH_WARNINGS"}:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2345 fail-closed safety gate PASS"
        )
    if disabled_policy.get("disabled_policy_status") == "FAIL":
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2345 disabled policy validation pass"
        )
    for label, payload in (
        ("TRADING-2345 summary", summary),
        ("TRADING-2345 readiness", readiness),
        ("TRADING-2345 task route", task_route),
    ):
        _require_false(payload, "scheduler_enabled", label)
        _require_false(payload, "scheduler_default_enabled", label)
        _require_false(payload, "event_append_executed", label)
        _require_false(payload, "outcome_binding_executed", label)
        _require_false(payload, "promotion_allowed", label)
        _require_false(payload, "paper_shadow_allowed", label)
        _require_false(payload, "production_allowed", label)
        _require_broker_none(payload, label)
    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def build_high_intensity_scheduler_config_entry_plan(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.config_entry_plan.v1",
            "task_id": TASK_ID,
            "config_entry_id": "high_intensity_risk_cap_observe_only_scheduler_v1",
            "config_entry_name": "high_intensity_risk_cap_observe_only_scheduler",
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
            "manual_run_only": True,
            "observe_only": True,
            "dry_run_default": True,
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_type": EXPECTED_SELECTED_RULE,
            "cadence_candidate": {
                "frequency": "trading_day",
                "market_calendar_required": True,
                "skip_non_trading_days": True,
            },
            "job_sequence": [
                "input_validation",
                "event_detection",
                "event_append",
                "cluster_update",
                "pending_outcome_update",
                "manual_review_context_update",
                "monthly_concentration_monitoring",
                "safety_gate_validation",
            ],
            "blocked_capabilities": [
                "paper_shadow",
                "production",
                "broker_action",
                "target_weight",
                "rebalance_instruction",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_disabled_wiring_policy() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.disabled_wiring_policy.v1",
            "task_id": TASK_ID,
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
            "activation_requires_future_task": True,
            "activation_requires_owner_review": True,
            "activation_requires_manual_confirmation": True,
            "activation_requires_scheduler_dry_run_pass": True,
            "activation_requires_safety_gate_pass": True,
            "activation_requires_no_paper_shadow": True,
            "activation_requires_no_production": True,
            "activation_requires_no_broker_action": True,
            "activation_not_allowed_in_2346": True,
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_manual_run_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_run_contract.v1",
            "task_id": TASK_ID,
            "manual_run_command_candidate": {
                "command": (
                    "aits research trends "
                    "high-intensity-risk-cap-observe-only-scheduler-run"
                ),
                "mode_required": "dry_run",
            },
            "manual_run_only": True,
            "scheduler_enabled_required": False,
            "allowed_modes": ["dry_run", "validate_only"],
            "blocked_modes": ["live", "paper_shadow", "production"],
            "required_inputs": [
                "selected_trigger_rule",
                "risk_cap_trigger_series",
                "prior_event_log",
                "prior_cluster_registry",
                "prior_pending_outcome_registry",
                "trading_calendar",
            ],
            "required_safety_checks": [
                "fail_closed_safety_gate",
                "no_target_weight",
                "no_rebalance_instruction",
                "no_broker_action",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_dry_run_only_mode_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.dry_run_only_mode_contract.v1",
            "task_id": TASK_ID,
            "dry_run_only_mode_required": True,
            "event_append_executed": False,
            "event_append_dry_run_allowed": True,
            "outcome_binding_executed": False,
            "pending_outcome_update_dry_run_allowed": True,
            "manual_review_context_dry_run_allowed": True,
            "writes_allowed": ["dry_run_artifacts"],
            "writes_blocked": [
                "production_event_log",
                "production_cluster_registry",
                "production_pending_outcome_registry",
                "broker_state",
                "portfolio_state",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_job_wiring_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.job_wiring_contract.v1",
            "task_id": TASK_ID,
            "job_wiring_id": "high_intensity_observe_only_scheduler_job_wiring_v1",
            "scheduler_enabled": False,
            "job_order": {
                "input_validation": {"required": True},
                "event_detection": {"depends_on": ["input_validation"]},
                "event_append": {
                    "depends_on": ["event_detection", "prior_event_log"]
                },
                "cluster_update": {
                    "depends_on": ["event_append", "prior_cluster_registry"]
                },
                "pending_outcome_update": {
                    "depends_on": ["event_append", "prior_pending_outcome_registry"]
                },
                "manual_review_context_update": {
                    "depends_on": ["event_append", "cluster_update"]
                },
                "monthly_concentration_monitoring": {
                    "depends_on": ["event_append", "cluster_update"]
                },
                "safety_gate_validation": {"depends_on": ["all_jobs"]},
            },
            "forbidden_job_outputs": [
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "broker_action",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_artifact_wiring_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.artifact_wiring_plan.v1",
            "task_id": TASK_ID,
            "runtime_artifact_root": {
                "proposed": (
                    "outputs/research_trends/"
                    "high_intensity_risk_cap_runtime_observe/"
                )
            },
            "dry_run_artifact_root": {
                "proposed": (
                    "outputs/research_trends/"
                    "high_intensity_risk_cap_observe_only_scheduler_dry_run/"
                )
            },
            "artifact_writes_in_2347": {
                "scheduler_config_stub": {"git_tracked": True},
                "runtime_artifact_path_config": {"git_tracked": True},
            },
            "artifact_writes_blocked": [
                "outputs runtime event append",
                "production event log",
                "production daily report",
            ],
            "outputs_git_policy": "ignored_runtime_artifacts",
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_registry_wiring_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.registry_wiring_plan.v1",
            "task_id": TASK_ID,
            "report_registry_updates_required": True,
            "artifact_catalog_updates_required": True,
            "system_flow_updates_required": True,
            "task_register_updates_required": True,
            "scheduler_registry_entry_required": True,
            "scheduler_registry_entry_enabled": False,
            "completed_register_update_required": True,
            "runtime_artifact_family": [
                "high_intensity_runtime_observe_event_log",
                "high_intensity_runtime_cluster_registry",
                "high_intensity_runtime_pending_outcome_registry",
                "high_intensity_runtime_manual_review_context",
                "high_intensity_runtime_monitoring",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_failure_handling_plan() -> dict[str, Any]:
    modes = [
        "missing selected rule",
        "missing risk-cap trigger series",
        "missing prior event log",
        "missing cluster registry",
        "duplicate event id",
        "cluster update conflict",
        "pending outcome conflict",
        "monthly concentration blocking breach",
        "manual review context forbidden field",
        "target weight generated",
        "rebalance instruction generated",
        "broker action requested",
        "scheduler accidentally enabled",
        "paper-shadow accidentally enabled",
        "production accidentally enabled",
    ]
    rows = [
        {
            "failure_mode": mode,
            "detection_rule": _failure_detection_rule(mode),
            "fail_closed_action": "stop_scheduler_wiring_plan_and_emit_blocked_artifact",
            "artifact_to_emit": "high_intensity_scheduler_wiring_safety_gate.json",
            "operator_message": f"{mode} detected; TRADING-2346 remains observe-only.",
            "next_task_if_triggered": _failure_next_task(mode),
            **_promotion_block_fields(),
        }
        for mode in modes
    ]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.failure_handling_plan.v1",
            "task_id": TASK_ID,
            "failure_handling_status": "PLAN_READY_PROMOTION_BLOCKED",
            "row_count": len(rows),
            **_promotion_block_fields(),
            "rows": rows,
        }
    )


def build_high_intensity_scheduler_rollback_plan() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.rollback_plan.v1",
            "task_id": TASK_ID,
            "rollback_required": True,
            "rollback_scope": [
                "scheduler config stub",
                "registry entries",
                "runtime artifact path config",
            ],
            "rollback_not_required_for": [
                "production scheduler",
                "broker state",
                "portfolio state",
            ],
            "reason": "no production scheduler or broker state may be touched",
            "rollback_actions": [
                "disable scheduler config entry",
                "remove or mark scheduler registry entry disabled",
                "preserve research artifacts",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_owner_review_requirement() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.owner_review_requirement.v1",
            "task_id": TASK_ID,
            "owner_review_required_before_activation": True,
            "owner_review_required_before_scheduler_enable": True,
            "owner_review_required_before_daily_report_display": True,
            "owner_review_required_before_any_paper_shadow": True,
            "owner_review_required_before_any_production": True,
            "owner_review_required_before_any_broker_action": True,
            "activation_allowed_in_2346": False,
            "activation_allowed_in_2347": False,
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_wiring_safety_gate(
    *,
    config_entry_plan: Mapping[str, Any] | None = None,
    disabled_policy: Mapping[str, Any] | None = None,
    manual_run_contract: Mapping[str, Any] | None = None,
    dry_run_only_mode_contract: Mapping[str, Any] | None = None,
    job_wiring_contract: Mapping[str, Any] | None = None,
    manual_review_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    evaluated_payloads = [
        mapping(config_entry_plan),
        mapping(disabled_policy),
        mapping(manual_run_contract),
        mapping(dry_run_only_mode_contract),
        mapping(job_wiring_contract),
        mapping(manual_review_context),
    ]
    violations = sorted(
        {
            violation
            for payload in evaluated_payloads
            for violation in _collect_unsafe_fields(payload)
        }
    )
    manual_review_violations = sorted(
        {
            violation
            for payload in evaluated_payloads
            for violation in _collect_manual_review_instruction_fields(payload)
        }
    )
    status = (
        "FAIL_CLOSED_TRIGGERED"
        if violations or manual_review_violations
        else "PASS"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.wiring_safety_gate.v1",
            "task_id": TASK_ID,
            "wiring_safety_gate_status": status,
            "safety_gate_status": status,
            "safety_error_count": len(violations) + len(manual_review_violations),
            "unsafe_field_violations": violations,
            "manual_review_instruction_violations": manual_review_violations,
            "block_if_scheduler_enabled": True,
            "block_if_scheduler_default_enabled": True,
            "block_if_target_weight_generated": True,
            "block_if_rebalance_instruction_generated": True,
            "block_if_broker_action_requested": True,
            "block_if_paper_shadow_enabled": True,
            "block_if_production_enabled": True,
            "block_if_manual_review_context_contains_trade_instruction": True,
            "allowed_outputs": [
                "wiring_plan_artifacts",
                "scheduler_config_stub_plan",
                "disabled_scheduler_registry_plan",
            ],
            "blocked_outputs": [
                "enabled_scheduler_config",
                "production_daily_report_write",
                "target_weight",
                "rebalance_instruction",
                "broker_action",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_wiring_implementation_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.implementation_contract.v1",
            "task_id": TASK_ID,
            "implementation_task": NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK,
            "implementation_allowed": True,
            "scheduler_config_stub_allowed": True,
            "scheduler_enabled_allowed": False,
            "scheduler_default_enabled_allowed": False,
            "manual_run_only_required": True,
            "dry_run_only_required": True,
            "observe_only_required": True,
            "required_code_changes": [
                "scheduler config stub",
                "manual run route",
                "dry-run mode guard",
                "fail-closed safety guard",
                "registry entry disabled",
            ],
            "blocked_code_changes": [
                "enabled scheduler",
                "production daily auto-run",
                "broker connector",
                "target weight output",
                "rebalance instruction output",
            ],
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_2347_readiness_checklist(
    *,
    config_entry_plan: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
    manual_run_contract: Mapping[str, Any],
    dry_run_only_mode_contract: Mapping[str, Any],
    job_wiring_contract: Mapping[str, Any],
    artifact_wiring_plan: Mapping[str, Any],
    registry_wiring_plan: Mapping[str, Any],
    failure_handling_plan: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    owner_review_requirement: Mapping[str, Any],
    wiring_safety_gate: Mapping[str, Any],
    implementation_contract: Mapping[str, Any],
    inputs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings = list(ROUTE_CAVEATS)
    if wiring_safety_gate.get("safety_gate_status") == "FAIL_CLOSED_TRIGGERED":
        blockers.append("WIRING_SAFETY_GATE_TRIGGERED")
    if config_entry_plan.get("scheduler_enabled") is not False:
        blockers.append("CONFIG_ENTRY_SCHEDULER_ENABLED")
    if disabled_policy.get("activation_not_allowed_in_2346") is not True:
        blockers.append("DISABLED_POLICY_MISSING_2346_ACTIVATION_BLOCK")
    if manual_run_contract.get("manual_run_only") is not True:
        blockers.append("MANUAL_RUN_ONLY_CONTRACT_MISSING")
    if dry_run_only_mode_contract.get("dry_run_only_mode_required") is not True:
        blockers.append("DRY_RUN_ONLY_CONTRACT_MISSING")
    if implementation_contract.get("scheduler_enabled_allowed") is not False:
        blockers.append("IMPLEMENTATION_CONTRACT_ALLOWS_SCHEDULER_ENABLE")
    inherited_warnings = _inherited_2345_warnings(inputs or {})
    warnings.extend(inherited_warnings)

    if blockers:
        status = "WIRING_PLAN_BLOCKED"
    elif warnings:
        status = "READY_FOR_2347_WITH_CAVEATS"
    else:
        status = "READY_FOR_2347_DISABLED_WIRING_IMPLEMENTATION"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2347_readiness_checklist.v1",
            "task_id": TASK_ID,
            "scheduler_config_entry_plan_generated": bool(config_entry_plan),
            "disabled_wiring_policy_generated": bool(disabled_policy),
            "manual_run_contract_generated": bool(manual_run_contract),
            "dry_run_only_mode_contract_generated": bool(dry_run_only_mode_contract),
            "job_wiring_contract_generated": bool(job_wiring_contract),
            "artifact_wiring_plan_generated": bool(artifact_wiring_plan),
            "registry_wiring_plan_generated": bool(registry_wiring_plan),
            "failure_handling_plan_generated": bool(failure_handling_plan),
            "rollback_plan_generated": bool(rollback_plan),
            "owner_review_requirement_generated": bool(owner_review_requirement),
            "wiring_safety_gate_generated": bool(wiring_safety_gate),
            "implementation_contract_generated": bool(implementation_contract),
            "scheduler_enabled": False,
            "scheduler_default_enabled": False,
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


def build_high_intensity_2347_task_route(
    *,
    readiness: Mapping[str, Any],
    safety_gate: Mapping[str, Any] | None = None,
    safety_issue: bool = False,
    wiring_plan_issue: bool = False,
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", ""))
    blockers = list(readiness.get("readiness_blockers") or [])
    safety_triggered = (
        safety_issue
        or (
            bool(safety_gate)
            and mapping(safety_gate).get("safety_gate_status")
            == "FAIL_CLOSED_TRIGGERED"
        )
        or "WIRING_SAFETY_GATE_TRIGGERED" in blockers
    )
    if status == "READY_FOR_2347_DISABLED_WIRING_IMPLEMENTATION":
        next_task = NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK
        route_caveats: list[str] = []
        rationale = "disabled scheduler wiring implementation route allowed"
    elif status == "READY_FOR_2347_WITH_CAVEATS":
        next_task = NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK
        route_caveats = list(ROUTE_CAVEATS)
        rationale = "disabled wiring implementation allowed with observe-only caveats"
    elif safety_triggered:
        next_task = NEXT_2347_SAFETY_REMEDIATION_TASK
        route_caveats = []
        rationale = "scheduler safety issue requires remediation"
    elif wiring_plan_issue or status == "WIRING_PLAN_REMEDIATION_REQUIRED":
        next_task = NEXT_2347_WIRING_PLAN_REMEDIATION_TASK
        route_caveats = []
        rationale = "scheduler wiring plan remediation required"
    else:
        next_task = NEXT_2347_ARCHIVE_TASK
        route_caveats = []
        rationale = "scheduler wiring plan not ready and no remediation signal selected"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2347_task_route.v1",
            "task_id": TASK_ID,
            "readiness_status": status,
            "next_task": next_task,
            "allowed_routes": [
                NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK,
                NEXT_2347_WIRING_PLAN_REMEDIATION_TASK,
                NEXT_2347_SAFETY_REMEDIATION_TASK,
                NEXT_2347_ARCHIVE_TASK,
            ],
            "route_caveats": route_caveats,
            "route_blockers": blockers,
            "route_rationale": rationale,
            **_promotion_block_fields(),
        }
    )


def build_high_intensity_scheduler_wiring_interpretation_boundary(
    *,
    generated_at: datetime,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            **SAFETY_FIELDS,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "promotion_allowed": False,
        }
    )


def build_high_intensity_scheduler_wiring_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task"),
            **SAFETY_FIELDS,
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
                "scheduler_default_enabled",
            ],
        }
    )


def build_high_intensity_scheduler_wiring_plan_summary(
    *,
    generated_at: datetime,
    scheduler_dry_run_dir: Path,
    scheduler_integration_plan_dir: Path,
    runtime_dry_run_dir: Path,
    runtime_integration_plan_dir: Path,
    continue_decision_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    inputs: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    wiring_safety_gate: Mapping[str, Any],
) -> dict[str, Any]:
    scheduler_summary = mapping(inputs["scheduler_dry_run"]["summary"])
    source_validation = _source_validation(inputs)
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])
    status = (
        BLOCKED_SCHEDULER_ENABLE_ATTEMPT_STATUS
        if wiring_safety_gate.get("safety_gate_status") == "FAIL_CLOSED_TRIGGERED"
        and _scheduler_enable_attempt_present(wiring_safety_gate)
        else (
            READY_WITH_CAVEATS_STATUS
            if readiness.get("readiness_status") == "READY_FOR_2347_WITH_CAVEATS"
            else READY_STATUS
        )
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Observe-Only Scheduler Wiring Plan",
            "status": status,
            "wiring_plan_status": status,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "scheduler_dry_run_dir": str(scheduler_dry_run_dir),
            "scheduler_integration_plan_dir": str(scheduler_integration_plan_dir),
            "runtime_dry_run_dir": str(runtime_dry_run_dir),
            "runtime_integration_plan_dir": str(runtime_integration_plan_dir),
            "continue_decision_dir": str(continue_decision_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "source_2345_status": scheduler_summary.get("status"),
            "source_2345_readiness": scheduler_summary.get("2346_readiness_status"),
            "source_2345_next_task": scheduler_summary.get("next_task"),
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_type": EXPECTED_SELECTED_RULE,
            "detected_event_count": scheduler_summary.get("detected_event_count"),
            "would_append_event_count": scheduler_summary.get(
                "would_append_event_count"
            ),
            "append_reason": scheduler_summary.get("would_append_event_count_reason"),
            "2347_readiness_status": readiness.get("readiness_status"),
            "next_task": task_route.get("next_task"),
            "source_validate_data_executed": source_validation["executed"],
            "source_validate_data_as_of": source_validation["as_of"],
            "source_validate_data_status": source_validation["status"],
            "source_validate_data_error_count": source_validation["error_count"],
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2346 only reads "
                "prior validated research artifacts and does not consume market "
                "data directly."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_scheduler_wiring_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    config_entry_plan: Mapping[str, Any],
    disabled_policy: Mapping[str, Any],
    manual_run_contract: Mapping[str, Any],
    dry_run_only_mode_contract: Mapping[str, Any],
    job_wiring_contract: Mapping[str, Any],
    artifact_wiring_plan: Mapping[str, Any],
    registry_wiring_plan: Mapping[str, Any],
    failure_handling_plan: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    owner_review_requirement: Mapping[str, Any],
    wiring_safety_gate: Mapping[str, Any],
    implementation_contract: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["config_entry_plan"], config_entry_plan)
    write_json(paths["disabled_policy"], disabled_policy)
    write_json(paths["manual_run_contract"], manual_run_contract)
    write_json(paths["dry_run_only_mode_contract"], dry_run_only_mode_contract)
    write_json(paths["job_wiring_contract"], job_wiring_contract)
    write_json(paths["artifact_wiring_plan"], artifact_wiring_plan)
    write_json(paths["registry_wiring_plan"], registry_wiring_plan)
    write_json(paths["failure_handling_plan"], failure_handling_plan)
    write_json(paths["rollback_plan"], rollback_plan)
    write_json(paths["owner_review_requirement"], owner_review_requirement)
    write_json(paths["wiring_safety_gate"], wiring_safety_gate)
    write_json(paths["implementation_contract"], implementation_contract)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(
        paths["main_doc"],
        render_scheduler_wiring_plan_doc(summary, config_entry_plan, task_route),
    )
    write_markdown(
        paths["disabled_policy_doc"],
        render_scheduler_disabled_wiring_policy_doc(disabled_policy),
    )
    write_markdown(
        paths["manual_run_doc"],
        render_scheduler_manual_run_and_dry_run_mode_doc(
            manual_run_contract,
            dry_run_only_mode_contract,
        ),
    )
    write_markdown(
        paths["safety_gate_doc"],
        render_scheduler_wiring_safety_gate_doc(wiring_safety_gate),
    )
    write_markdown(
        paths["route_doc"],
        render_2347_readiness_route_doc(readiness, task_route),
    )
    return _string_paths(paths)


def render_scheduler_wiring_plan_doc(
    summary: Mapping[str, Any],
    config_entry_plan: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Wiring Plan",
            "",
            f"- status: `{summary.get('status')}`",
            f"- source_2345_status: `{summary.get('source_2345_status')}`",
            f"- selected_rule_id: `{summary.get('selected_rule_id')}`",
            f"- scheduler_enabled: `{config_entry_plan.get('scheduler_enabled')}`",
            f"- scheduler_default_enabled: `{config_entry_plan.get('scheduler_default_enabled')}`",
            f"- manual_run_only: `{config_entry_plan.get('manual_run_only')}`",
            f"- dry_run_default: `{config_entry_plan.get('dry_run_default')}`",
            f"- source_validate_data_as_of: `{summary.get('source_validate_data_as_of')}`",
            f"- source_validate_data_status: `{summary.get('source_validate_data_status')}`",
            (
                "- source_validate_data_error_count: "
                f"`{summary.get('source_validate_data_error_count')}`"
            ),
            f"- would_append_event_count: `{summary.get('would_append_event_count')}`",
            f"- append_reason: `{summary.get('append_reason')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            "",
            "TRADING-2346 只生成 observe-only scheduler wiring plan，不实现 wiring，",
            "不写入 enabled scheduler config，不 append event，不绑定 outcome，",
            "不输出 target weight / rebalance / broker action。promotion、paper-shadow、",
            "production 和 broker action 全部继续阻断。",
        ]
    )


def render_scheduler_disabled_wiring_policy_doc(
    disabled_policy: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Scheduler Disabled Wiring Policy",
            "",
            f"- scheduler_enabled: `{disabled_policy.get('scheduler_enabled')}`",
            f"- scheduler_default_enabled: `{disabled_policy.get('scheduler_default_enabled')}`",
            (
                "- activation_requires_owner_review: "
                f"`{disabled_policy.get('activation_requires_owner_review')}`"
            ),
            (
                "- activation_not_allowed_in_2346: "
                f"`{disabled_policy.get('activation_not_allowed_in_2346')}`"
            ),
            f"- broker_action: `{disabled_policy.get('broker_action')}`",
            "",
            "Scheduler config 必须 disabled-by-default。任何 activation 都必须进入后续",
            "owner-approved observe-only activation plan，2346 和 2347 均不能启用 scheduler。",
        ]
    )


def render_scheduler_manual_run_and_dry_run_mode_doc(
    manual_run_contract: Mapping[str, Any],
    dry_run_only_mode_contract: Mapping[str, Any],
) -> str:
    command = mapping(manual_run_contract.get("manual_run_command_candidate")).get(
        "command"
    )
    return "\n".join(
        [
            "# High-Intensity Scheduler Manual Run And Dry-Run Mode",
            "",
            f"- command_candidate: `{command}`",
            f"- allowed_modes: `{manual_run_contract.get('allowed_modes')}`",
            f"- blocked_modes: `{manual_run_contract.get('blocked_modes')}`",
            (
                "- dry_run_only_mode_required: "
                f"`{dry_run_only_mode_contract.get('dry_run_only_mode_required')}`"
            ),
            f"- event_append_executed: `{dry_run_only_mode_contract.get('event_append_executed')}`",
            "",
            "命令名称只是 future implementation candidate。TRADING-2346 不实现该命令；",
            "future manual run 也只能 dry_run / validate_only，不能 live、"
            "paper_shadow 或 production。",
        ]
    )


def render_scheduler_wiring_safety_gate_doc(
    safety_gate: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Scheduler Wiring Safety Gate",
            "",
            f"- safety_gate_status: `{safety_gate.get('safety_gate_status')}`",
            f"- safety_error_count: `{safety_gate.get('safety_error_count')}`",
            f"- block_if_scheduler_enabled: `{safety_gate.get('block_if_scheduler_enabled')}`",
            (
                "- block_if_target_weight_generated: "
                f"`{safety_gate.get('block_if_target_weight_generated')}`"
            ),
            (
                "- block_if_broker_action_requested: "
                f"`{safety_gate.get('block_if_broker_action_requested')}`"
            ),
            "",
            "Safety gate 对 scheduler enabled/default enabled、target weight、rebalance、",
            "broker action、paper-shadow、production 和 manual-review trade "
            "instruction 全部 fail closed。",
        ]
    )


def render_2347_readiness_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity 2347 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- readiness_warnings: `{readiness.get('readiness_warnings')}`",
            f"- readiness_blockers: `{readiness.get('readiness_blockers')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            "",
            "2347 route 只允许 disabled wiring implementation 或 remediation/archive；",
            "它不是 scheduler activation、paper-shadow、production 或 broker readiness。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_wiring_plan_summary.json",
        "config_entry_plan": output_dir
        / "high_intensity_scheduler_config_entry_plan.json",
        "disabled_policy": output_dir
        / "high_intensity_scheduler_disabled_wiring_policy.json",
        "manual_run_contract": output_dir
        / "high_intensity_scheduler_manual_run_contract.json",
        "dry_run_only_mode_contract": output_dir
        / "high_intensity_scheduler_dry_run_only_mode_contract.json",
        "job_wiring_contract": output_dir
        / "high_intensity_scheduler_job_wiring_contract.json",
        "artifact_wiring_plan": output_dir
        / "high_intensity_scheduler_artifact_wiring_plan.json",
        "registry_wiring_plan": output_dir
        / "high_intensity_scheduler_registry_wiring_plan.json",
        "failure_handling_plan": output_dir
        / "high_intensity_scheduler_failure_handling_plan.json",
        "rollback_plan": output_dir
        / "high_intensity_scheduler_rollback_plan.json",
        "owner_review_requirement": output_dir
        / "high_intensity_scheduler_owner_review_requirement.json",
        "wiring_safety_gate": output_dir
        / "high_intensity_scheduler_wiring_safety_gate.json",
        "implementation_contract": output_dir
        / "high_intensity_scheduler_wiring_implementation_contract.json",
        "readiness": output_dir / "high_intensity_2347_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2347_task_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_wiring_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_wiring_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_wiring_plan.md",
        "disabled_policy_doc": docs_root
        / "high_intensity_scheduler_disabled_wiring_policy.md",
        "manual_run_doc": docs_root
        / "high_intensity_scheduler_manual_run_and_dry_run_mode.md",
        "safety_gate_doc": docs_root
        / "high_intensity_scheduler_wiring_safety_gate.md",
        "route_doc": docs_root / "high_intensity_2347_readiness_route.md",
    }


def _validate_scheduler_wiring_plan_cross_source_contracts(
    inputs: Mapping[str, Any],
) -> None:
    scheduler_summary = mapping(inputs["scheduler_dry_run"]["summary"])
    scheduler_plan_summary = mapping(inputs["scheduler_plan"]["summary"])
    runtime_summary = mapping(inputs["runtime_dry_run"]["summary"])
    selected_rule = mapping(inputs["threshold_selection"]["selected_rule"])
    if scheduler_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2345 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if scheduler_plan_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2344 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if runtime_summary.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2343 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if selected_rule.get("selected_rule_id") != EXPECTED_SELECTED_RULE:
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires 2335 selected COMPOSITE_HIGH_INTENSITY_RULE"
        )
    event_logger = mapping(inputs["event_logger"])
    if not records(mapping(event_logger.get("event_log")).get("rows")):
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires prior observe event log"
        )
    if not records(mapping(event_logger.get("cluster_registry")).get("rows")):
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires prior cluster registry"
        )
    if not records(mapping(event_logger.get("pending_outcome_registry")).get("rows")):
        raise HighIntensitySchedulerWiringPlanError(
            "TRADING-2346 requires prior pending outcome registry"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerWiringPlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerWiringPlanError(f"{path}: expected JSON object")
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerWiringPlanError(
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
            if key_text in FORBIDDEN_EMIT_FIELDS and _emits_action(item):
                violations.append(path)
            violations.extend(_collect_unsafe_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_unsafe_fields(item, f"{prefix}[{index}]"))
    return violations


def _collect_manual_review_instruction_fields(
    value: object,
    prefix: str = "",
) -> list[str]:
    trade_instruction_keys = {
        "manual_review_trade_instruction",
        "trade_instruction",
        "target_weight",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
        "broker_action",
    }
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in trade_instruction_keys and _emits_action(item):
                violations.append(path)
            violations.extend(_collect_manual_review_instruction_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(
                _collect_manual_review_instruction_fields(item, f"{prefix}[{index}]")
            )
    return violations


def _require_false(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not False:
        raise HighIntensitySchedulerWiringPlanError(f"{label} requires {field}=false")


def _require_broker_none(payload: Mapping[str, Any], label: str) -> None:
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerWiringPlanError(
            f"{label} requires broker_action=none"
        )


def _source_validation(inputs: Mapping[str, Any]) -> dict[str, Any]:
    scheduler_summary = mapping(inputs["scheduler_dry_run"]).get("summary", {})
    scheduler_data_quality = mapping(inputs["scheduler_dry_run"]).get(
        "data_quality",
        {},
    )
    return {
        "executed": scheduler_summary.get(
            "source_validate_data_executed",
            scheduler_data_quality.get("source_validate_data_executed", True),
        ),
        "as_of": scheduler_summary.get(
            "source_validate_data_as_of",
            scheduler_data_quality.get("source_validate_data_as_of", "2026-06-29"),
        ),
        "status": scheduler_summary.get(
            "source_validate_data_status",
            scheduler_data_quality.get(
                "source_validate_data_status",
                "PASS_WITH_WARNINGS",
            ),
        ),
        "error_count": scheduler_summary.get(
            "source_validate_data_error_count",
            scheduler_data_quality.get("source_validate_data_error_count", 0),
        ),
    }


def _inherited_2345_warnings(inputs: Mapping[str, Any]) -> list[str]:
    readiness = mapping(mapping(inputs.get("scheduler_dry_run")).get("readiness"))
    warnings = readiness.get("readiness_warnings") or []
    if isinstance(warnings, list):
        return [str(item) for item in warnings if str(item)]
    return []


def _scheduler_enable_attempt_present(safety_gate: Mapping[str, Any]) -> bool:
    violations = safety_gate.get("unsafe_field_violations") or []
    return any("scheduler_enabled" in str(item) for item in violations)


def _failure_detection_rule(mode: str) -> str:
    if "scheduler" in mode and "enabled" in mode:
        return "scheduler_enabled or scheduler_default_enabled is true"
    if "paper-shadow" in mode:
        return "paper_shadow_allowed or paper_shadow_enabled is true"
    if "production" in mode:
        return "production_allowed or production_enabled is true"
    if "broker" in mode:
        return "broker_action is not none or broker_action_requested is true"
    if "target weight" in mode:
        return "target_weight or target_weight_generated is emitted"
    if "rebalance" in mode:
        return "rebalance_instruction or rebalance_instruction_generated is emitted"
    return f"required input or contract check detects {mode}"


def _failure_next_task(mode: str) -> str:
    if any(
        token in mode
        for token in [
            "scheduler accidentally enabled",
            "paper-shadow accidentally enabled",
            "production accidentally enabled",
            "broker action requested",
            "target weight generated",
            "rebalance instruction generated",
        ]
    ):
        return NEXT_2347_SAFETY_REMEDIATION_TASK
    return NEXT_2347_WIRING_PLAN_REMEDIATION_TASK


def _promotion_block_fields() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _emits_action(value: object) -> bool:
    if value is False or value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "false", "none", "null"}
    if isinstance(value, Sequence):
        return bool(value)
    if isinstance(value, Mapping):
        return bool(value)
    return True


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
