from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_OUTCOME_BINDER_ROOT,
    DEFAULT_PARTIAL_READINESS_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FORWARD_OUTCOME_REVIEW_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    records,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2341_HIGH_INTENSITY_RISK_CAP_CONTINUE_FORWARD_OBSERVE_DECISION"
REPORT_TYPE = "high_intensity_risk_cap_continue_forward_observe_decision"
ARTIFACT_ROLE = "high_intensity_risk_cap_continue_forward_observe_decision"
MODE = "continue_forward_observe_decision"

EXPECTED_2340_STATUS = "FORWARD_OUTCOME_REVIEW_COMPLETE_WITH_PARTIAL_COVERAGE_CAVEAT"
EXPECTED_2340_RECOMMENDATION = "CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE"
EXPECTED_2340_ROUTE = "TRADING-2341_High_Intensity_Risk_Cap_Continue_Forward_Observe_Decision"
NEXT_2342_INTEGRATION_TASK = (
    "TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan"
)
NEXT_2342_REMEDIATION_TASK = (
    "TRADING-2342_High_Intensity_Risk_Cap_Runtime_Prerequisite_Remediation"
)
NEXT_2342_WAIT_TASK = "TRADING-2342_High_Intensity_Risk_Cap_Wait_For_Full_20D_Coverage"
NEXT_2342_REFINEMENT_TASK = (
    "TRADING-2342_High_Intensity_Risk_Cap_Threshold_Refinement_Plan"
)
NEXT_2342_ARCHIVE_TASK = "TRADING-2342_Archive_High_Intensity_Risk_Cap_Observe_Line"

# TRADING-2341 owner attachment pilot review baselines. These are not trading
# thresholds; they only decide when the next observe-only review should run.
MIN_NEW_EVENT_COUNT_BEFORE_NEXT_REVIEW = 20
MIN_NEW_CLUSTER_COUNT_BEFORE_NEXT_REVIEW = 10
MIN_OBSERVE_MONTHS_BEFORE_NEXT_REVIEW = 3

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "continue_forward_observe_decision_only": True,
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
}
FORBIDDEN_EMIT_FIELDS = {
    "target_weight_action",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
}


class HighIntensityContinueObserveDecisionError(ValueError):
    pass


def run_high_intensity_risk_cap_continue_forward_observe_decision(
    *,
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
        raise HighIntensityContinueObserveDecisionError(
            f"high-intensity continue observe decision only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_continue_observe_decision_inputs(
        forward_outcome_review_dir=forward_outcome_review_dir,
        partial_readiness_dir=partial_readiness_dir,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
    )
    decision_matrix = build_high_intensity_continue_observe_decision_matrix(inputs)
    selected_rule_contract = build_high_intensity_selected_rule_continuation_contract(inputs)
    observe_scope = build_high_intensity_observe_continuation_scope(decision_matrix)
    partial_caveat = build_high_intensity_partial_coverage_carryforward_caveat(inputs)
    monthly_plan = build_high_intensity_monthly_concentration_monitoring_plan(inputs)
    event_logger_contract = build_high_intensity_event_logger_continuation_contract(
        inputs=inputs,
        decision_matrix=decision_matrix,
    )
    outcome_policy = build_high_intensity_outcome_update_policy()
    manual_review_policy = build_high_intensity_manual_review_context_policy()
    stop_policy = build_high_intensity_stop_refine_archive_policy(inputs)
    prerequisite = build_high_intensity_runtime_integration_prerequisite_checklist(
        selected_rule_contract=selected_rule_contract,
        observe_scope=observe_scope,
        partial_caveat=partial_caveat,
        monthly_plan=monthly_plan,
        event_logger_contract=event_logger_contract,
        outcome_policy=outcome_policy,
        manual_review_policy=manual_review_policy,
        stop_policy=stop_policy,
    )
    readiness = build_high_intensity_2342_readiness_checklist(
        decision_matrix=decision_matrix,
        selected_rule_contract=selected_rule_contract,
        observe_scope=observe_scope,
        partial_caveat=partial_caveat,
        monthly_plan=monthly_plan,
        event_logger_contract=event_logger_contract,
        outcome_policy=outcome_policy,
        manual_review_policy=manual_review_policy,
        stop_policy=stop_policy,
        prerequisite=prerequisite,
    )
    task_route = build_high_intensity_2342_task_route(readiness)
    interpretation_boundary = build_high_intensity_continue_observe_interpretation_boundary(
        generated_at=generated_at,
        partial_caveat=partial_caveat,
    )
    safety_boundary = build_high_intensity_continue_observe_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_continue_observe_decision_summary(
        generated_at=generated_at,
        forward_outcome_review_dir=forward_outcome_review_dir,
        partial_readiness_dir=partial_readiness_dir,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        inputs=inputs,
        decision_matrix=decision_matrix,
        selected_rule_contract=selected_rule_contract,
        observe_scope=observe_scope,
        partial_caveat=partial_caveat,
        monthly_plan=monthly_plan,
        event_logger_contract=event_logger_contract,
        outcome_policy=outcome_policy,
        manual_review_policy=manual_review_policy,
        stop_policy=stop_policy,
        prerequisite=prerequisite,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_continue_observe_decision_outputs(
        paths=paths,
        summary=summary,
        decision_matrix=decision_matrix,
        selected_rule_contract=selected_rule_contract,
        observe_scope=observe_scope,
        partial_caveat=partial_caveat,
        monthly_plan=monthly_plan,
        event_logger_contract=event_logger_contract,
        outcome_policy=outcome_policy,
        manual_review_policy=manual_review_policy,
        stop_policy=stop_policy,
        prerequisite=prerequisite,
        readiness=readiness,
        task_route=task_route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_continue_observe_decision_inputs(
    *,
    forward_outcome_review_dir: Path,
    partial_readiness_dir: Path,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    return {
        "forward_outcome_review": load_trading_2340_forward_outcome_review_outputs(
            forward_outcome_review_dir
        ),
        "partial_readiness": load_trading_2339_continue_observe_context(partial_readiness_dir),
        "outcome_binder": load_trading_2337_continue_observe_context(outcome_binder_dir),
        "event_logger": load_trading_2336_continue_observe_context(event_logger_dir),
        "threshold_selection": load_trading_2335_continue_observe_context(
            threshold_selection_dir
        ),
        "forward_observe_plan": load_trading_2334_continue_observe_context(
            forward_observe_plan_dir
        ),
    }


def load_trading_2340_forward_outcome_review_outputs(
    forward_outcome_review_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": forward_outcome_review_dir
        / "high_intensity_forward_outcome_review_summary.json",
        "cluster_matrix": forward_outcome_review_dir
        / "high_intensity_cluster_outcome_review_matrix.json",
        "horizon_matrix": forward_outcome_review_dir
        / "high_intensity_horizon_outcome_review_matrix.json",
        "false_warning_review": forward_outcome_review_dir
        / "high_intensity_false_warning_review.json",
        "missed_upside_review": forward_outcome_review_dir
        / "high_intensity_missed_upside_review.json",
        "downside_capture_review": forward_outcome_review_dir
        / "high_intensity_downside_capture_review.json",
        "manual_review": forward_outcome_review_dir
        / "high_intensity_manual_review_usefulness_review.json",
        "rebound_stress_review": forward_outcome_review_dir
        / "high_intensity_rebound_stress_review.json",
        "partial_caveat": forward_outcome_review_dir
        / "high_intensity_partial_coverage_caveat_report.json",
        "monthly_review": forward_outcome_review_dir
        / "high_intensity_monthly_concentration_effect_review.json",
        "weighted_evidence": forward_outcome_review_dir
        / "high_intensity_cluster_weighted_evidence_summary.json",
        "threshold_assessment": forward_outcome_review_dir
        / "high_intensity_threshold_rule_outcome_assessment.json",
        "decision_matrix": forward_outcome_review_dir
        / "high_intensity_continue_refine_archive_decision_matrix.json",
        "readiness": forward_outcome_review_dir / "high_intensity_2341_readiness_checklist.json",
        "task_route": forward_outcome_review_dir / "high_intensity_2341_task_route.json",
        "interpretation_boundary": forward_outcome_review_dir
        / "high_intensity_forward_outcome_interpretation_boundary.json",
        "safety_boundary": forward_outcome_review_dir
        / "high_intensity_forward_outcome_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2340 forward outcome review")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2340 {key}", payload)
    summary = payloads["summary"]
    decision = payloads["decision_matrix"]
    route = payloads["task_route"]
    if summary.get("status") != EXPECTED_2340_STATUS:
        raise HighIntensityContinueObserveDecisionError(
            f"TRADING-2341 requires 2340 status {EXPECTED_2340_STATUS}"
        )
    if summary.get("overall_recommendation") != EXPECTED_2340_RECOMMENDATION:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 route mismatch: 2340 recommendation is not "
            f"{EXPECTED_2340_RECOMMENDATION}"
        )
    if decision.get("overall_recommendation") != EXPECTED_2340_RECOMMENDATION:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires 2340 decision matrix continue recommendation"
        )
    if summary.get("next_task") != EXPECTED_2340_ROUTE or route.get("next_task") != (
        EXPECTED_2340_ROUTE
    ):
        raise HighIntensityContinueObserveDecisionError(
            f"TRADING-2341 requires 2340 route {EXPECTED_2340_ROUTE}"
        )
    if decision.get("next_task_recommendation") != EXPECTED_2340_ROUTE:
        raise HighIntensityContinueObserveDecisionError(
            f"TRADING-2341 requires 2340 decision next_task {EXPECTED_2340_ROUTE}"
        )
    if payloads["partial_caveat"].get("partial_coverage_caveat_required") is not True:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires 2340 partial coverage caveat"
        )
    return {
        "source_dir": str(forward_outcome_review_dir),
        "paths": _string_paths(paths),
        **payloads,
    }


def load_trading_2339_continue_observe_context(partial_readiness_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": partial_readiness_dir / "high_intensity_partial_outcome_readiness_summary.json",
        "coverage_matrix": partial_readiness_dir
        / "high_intensity_partial_outcome_coverage_matrix.json",
        "not_due_matrix": partial_readiness_dir / "high_intensity_not_due_horizon_matrix.json",
        "not_due_impact_report": partial_readiness_dir
        / "high_intensity_not_due_cluster_impact_report.json",
        "horizon_readiness": partial_readiness_dir / "high_intensity_horizon_readiness_matrix.json",
        "cluster_readiness": partial_readiness_dir / "high_intensity_cluster_readiness_matrix.json",
        "sufficiency_report": partial_readiness_dir
        / "high_intensity_partial_outcome_sufficiency_report.json",
        "decision_matrix": partial_readiness_dir
        / "high_intensity_wait_vs_review_decision_matrix.json",
        "input_contract": partial_readiness_dir
        / "high_intensity_partial_review_input_contract.json",
        "interpretation_boundary": partial_readiness_dir
        / "high_intensity_partial_outcome_interpretation_boundary.json",
        "task_route": partial_readiness_dir / "high_intensity_2340_task_route.json",
        "safety_boundary": partial_readiness_dir
        / "high_intensity_partial_outcome_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2339 partial readiness")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2339 {key}", payload)
    summary = payloads["summary"]
    if int(summary.get("blocked_outcome_count") or 0) != 0:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 blocks 2339 covered outcome gaps"
        )
    if int(summary.get("critical_clusters_with_not_due") or 0) != 0:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 blocks critical clusters with not-due horizons"
        )
    return {"source_dir": str(partial_readiness_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2337_continue_observe_context(outcome_binder_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": outcome_binder_dir / "high_intensity_outcome_binder_summary.json",
        "cluster_matrix": outcome_binder_dir
        / "high_intensity_cluster_actual_path_outcome_matrix.json",
        "false_warning_report": outcome_binder_dir
        / "high_intensity_false_warning_classification_report.json",
        "missed_upside_report": outcome_binder_dir
        / "high_intensity_missed_upside_classification_report.json",
        "downside_capture_report": outcome_binder_dir
        / "high_intensity_downside_capture_classification_report.json",
        "manual_review_report": outcome_binder_dir
        / "high_intensity_manual_review_usefulness_proxy_report.json",
        "data_quality_report": outcome_binder_dir
        / "high_intensity_actual_path_data_quality_report.json",
        "interpretation_boundary": outcome_binder_dir
        / "high_intensity_outcome_binder_interpretation_boundary.json",
        "safety_boundary": outcome_binder_dir
        / "high_intensity_outcome_binder_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2337 outcome binder")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2337 {key}", payload)
    summary = payloads["summary"]
    data_quality = payloads["data_quality_report"]
    if summary.get("validate_data_executed") is not True:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires source validate_data_executed=true"
        )
    if str(summary.get("validate_data_status", "")) not in {"PASS", "PASS_WITH_WARNINGS"}:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires source validate_data_status PASS/PASS_WITH_WARNINGS"
        )
    if int(summary.get("validate_data_error_count") or 0) != 0:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires source validate_data_error_count=0"
        )
    if str(data_quality.get("data_quality_status", "")) == "FAIL":
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 blocks data_quality_status=FAIL"
        )
    return {"source_dir": str(outcome_binder_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2336_continue_observe_context(event_logger_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": event_logger_dir / "high_intensity_event_logger_summary.json",
        "event_log": event_logger_dir / "high_intensity_observe_event_log.json",
        "cluster_registry": event_logger_dir / "high_intensity_observe_event_cluster_registry.json",
        "monthly_report": event_logger_dir / "high_intensity_monthly_concentration_report.json",
        "pending_outcome_registry": event_logger_dir
        / "high_intensity_pending_outcome_registry.json",
        "manual_review_queue": event_logger_dir / "high_intensity_manual_review_event_queue.json",
        "interpretation_boundary": event_logger_dir
        / "high_intensity_event_logger_interpretation_boundary.json",
        "safety_boundary": event_logger_dir / "high_intensity_event_logger_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2336 event logger")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2336 {key}", payload)
    if int(payloads["summary"].get("cluster_count") or 0) <= 0:
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires non-empty 2336 event logger lineage"
        )
    return {"source_dir": str(event_logger_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2335_continue_observe_context(threshold_selection_dir: Path) -> dict[str, Any]:
    paths = {
        "selected_rule": threshold_selection_dir / "high_intensity_selected_trigger_rule.json",
        "selected_contract": threshold_selection_dir
        / "high_intensity_selected_trigger_contract.json",
        "event_logger_input_contract": threshold_selection_dir
        / "high_intensity_event_logger_input_contract.json",
        "caveat_report": threshold_selection_dir
        / "high_intensity_threshold_selection_caveat_report.json",
        "manual_review_boundary": threshold_selection_dir
        / "high_intensity_selected_rule_manual_review_boundary.json",
        "safety_boundary": threshold_selection_dir
        / "high_intensity_threshold_selection_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2335 threshold selection")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2335 {key}", payload)
    if payloads["selected_rule"].get("selected_rule_id") != "COMPOSITE_HIGH_INTENSITY_RULE":
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires selected_rule_id=COMPOSITE_HIGH_INTENSITY_RULE"
        )
    if not payloads["selected_contract"].get("selected_rule_hash"):
        raise HighIntensityContinueObserveDecisionError(
            "TRADING-2341 requires selected trigger contract hash"
        )
    return {"source_dir": str(threshold_selection_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2334_continue_observe_context(forward_observe_plan_dir: Path) -> dict[str, Any]:
    paths = {
        "event_schema": forward_observe_plan_dir
        / "high_intensity_forward_observe_event_schema.json",
        "evidence_contract": forward_observe_plan_dir
        / "high_intensity_forward_observe_evidence_contract.json",
        "actual_path_outcome_contract": forward_observe_plan_dir
        / "high_intensity_actual_path_outcome_contract.json",
        "false_warning_framework": forward_observe_plan_dir
        / "high_intensity_false_warning_missed_stress_framework.json",
        "stop_continue_archive_rules": forward_observe_plan_dir
        / "high_intensity_stop_continue_archive_rules.json",
        "manual_review_boundary": forward_observe_plan_dir
        / "high_intensity_manual_review_boundary.json",
        "safety_boundary": forward_observe_plan_dir
        / "high_intensity_forward_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2334 forward observe plan")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2334 {key}", payload)
    return {
        "source_dir": str(forward_observe_plan_dir),
        "paths": _string_paths(paths),
        **payloads,
    }


def build_high_intensity_continue_observe_decision_matrix(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    forward = inputs["forward_outcome_review"]
    partial_caveat = forward["partial_caveat"]
    monthly_review = forward["monthly_review"]
    selected_rule = inputs["threshold_selection"]["selected_rule"]
    partial_required = partial_caveat.get("partial_coverage_caveat_required") is True
    monthly_required = _monthly_monitoring_required(
        inputs["event_logger"]["monthly_report"],
        monthly_review,
    )
    has_caveats = partial_required or monthly_required
    decision_status = (
        "CONTINUE_DECISION_CONFIRMED_WITH_CAVEATS_PROMOTION_BLOCKED"
        if has_caveats
        else "CONTINUE_DECISION_CONFIRMED_PROMOTION_BLOCKED"
    )
    if partial_required:
        overall_decision = "CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT"
    elif monthly_required:
        overall_decision = "CONTINUE_OBSERVE_ONLY_WITH_MONITORING_REQUIREMENTS"
    else:
        overall_decision = "CONTINUE_OBSERVE_ONLY"
    return clean_for_yaml(
        {
            "decision_status": decision_status,
            "source_recommendation": EXPECTED_2340_RECOMMENDATION,
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_type": "COMPOSITE_HIGH_INTENSITY_RULE",
            "evidence_basis": [
                "TRADING-2336_event_logger",
                "TRADING-2337_outcome_binder",
                "TRADING-2339_partial_readiness",
                "TRADING-2340_forward_outcome_review",
            ],
            "partial_coverage_caveat_required": partial_required,
            "monthly_concentration_monitoring_required": monthly_required,
            "manual_review_context_allowed": True,
            "runtime_observe_integration_allowed": True,
            "overall_decision": overall_decision,
            "next_task_recommendation": NEXT_2342_INTEGRATION_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_selected_rule_continuation_contract(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    threshold = inputs["threshold_selection"]
    selected_rule = threshold["selected_rule"]
    selected_contract = threshold["selected_contract"]
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_SELECTED_RULE_CONTINUATION_CONTRACT_V1",
            "contract_version": "v1",
            "selected_rule_id": selected_rule.get("selected_rule_id"),
            "selected_rule_hash": selected_contract.get("selected_rule_hash"),
            "selected_rule_type": "COMPOSITE_HIGH_INTENSITY_RULE",
            "rule_continued": True,
            "rule_changed": False,
            "rule_change_allowed_in_2341": False,
            "required_input_fields": list(selected_contract.get("required_input_fields", [])),
            "allowed_usage": [
                "observe_only_event_detection",
                "manual_review_context",
            ],
            "blocked_usage": [
                "automatic_exposure_cap",
                "target_weight_action",
                "rebalance_instruction",
                "paper_shadow",
                "production",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_observe_continuation_scope(
    decision_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "scope_id": "HIGH_INTENSITY_RISK_CAP_CONTINUE_OBSERVE_SCOPE_V1",
            "observe_line": "high_intensity_risk_cap",
            "continuation_mode": "observe_only",
            "event_logger_runtime_integration_allowed_for_next_task": True,
            "manual_review_context_allowed": bool(
                decision_matrix.get("manual_review_context_allowed")
            ),
            "automatic_exposure_cap_allowed": False,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "in_scope": [
                "future high-intensity event detection",
                "observe event logging",
                "pending outcome registry update",
                "actual-path outcome update",
                "monthly concentration monitoring",
                "manual-review context display",
            ],
            "out_of_scope": [
                "exposure cap execution",
                "target weight generation",
                "broker order generation",
                "paper-shadow portfolio decision",
                "production strategy activation",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_partial_coverage_carryforward_caveat(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    partial_summary = inputs["partial_readiness"]["summary"]
    forward_caveat = inputs["forward_outcome_review"]["partial_caveat"]
    materiality = str(forward_caveat.get("coverage_caveat_materiality", "MODERATE"))
    return clean_for_yaml(
        {
            "coverage_ratio": forward_caveat.get("coverage_ratio"),
            "bound_outcome_count": forward_caveat.get("bound_outcome_count"),
            "expected_outcome_count": forward_caveat.get("expected_outcome_count"),
            "not_due_outcome_count": forward_caveat.get("not_due_outcome_count"),
            "critical_clusters_with_not_due": forward_caveat.get(
                "critical_clusters_with_not_due"
            ),
            "partial_coverage_caveat_required": True,
            "caveat_carryforward_required_for_2342": True,
            "caveat_materiality": materiality,
            "source_2339_coverage_ratio": partial_summary.get("coverage_ratio"),
            "source_2340_caveat_label": forward_caveat.get("caveat_label"),
            "allowed_interpretation": [
                "continue observe with partial coverage caveat",
                "231 of 240 outcome slots are bound",
                "not-due horizons must remain visible in downstream review",
            ],
            "blocked_interpretation": [
                "full outcome coverage",
                "paper-shadow readiness",
                "production readiness",
                "broker action",
                "real position instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_monthly_concentration_monitoring_plan(
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    monthly_report = inputs["event_logger"]["monthly_report"]
    monthly_review = inputs["forward_outcome_review"]["monthly_review"]
    event_rows = records(inputs["event_logger"]["event_log"].get("rows"))
    cluster_rows = records(inputs["event_logger"]["cluster_registry"].get("rows"))
    warning = _inherited_monthly_warning(monthly_report, monthly_review)
    guardrail = int(monthly_report.get("monthly_event_guardrail") or 0)
    max_consecutive = max(
        [int(row.get("consecutive_trigger_days") or 0) for row in event_rows + cluster_rows]
        or [0]
    )
    monitoring_status = (
        "MONITORING_REQUIRED_WITH_STRICT_GUARDRAILS"
        if warning == "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
        else "MONITORING_REQUIRED"
    )
    return clean_for_yaml(
        {
            "monitoring_required": True,
            "inherited_warning": warning,
            "monitoring_metrics": [
                "monthly_event_count",
                "monthly_cluster_count",
                "consecutive_trigger_days",
                "new_cluster_count",
                "existing_cluster_continuation_count",
                "false_warning_by_month",
                "downside_capture_by_month",
                "missed_upside_by_month",
            ],
            "guardrails": {
                "max_monthly_event_count": guardrail,
                "max_monthly_cluster_count": guardrail,
                "max_consecutive_trigger_days": max(1, max_consecutive),
            },
            "monitoring_frequency": "per_observe_event_and_monthly_rollup",
            "monitoring_status": monitoring_status,
            "source_monthly_event_count_max": monthly_report.get(
                "observed_monthly_event_count_max"
            ),
            "source_monthly_cluster_count_max": monthly_review.get("max_monthly_cluster_count"),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_event_logger_continuation_contract(
    *,
    inputs: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    threshold = inputs["threshold_selection"]
    plan = inputs["forward_observe_plan"]
    forward = inputs["forward_outcome_review"]
    return clean_for_yaml(
        {
            "contract_id": "HIGH_INTENSITY_EVENT_LOGGER_CONTINUATION_CONTRACT_V1",
            "contract_version": "v1",
            "selected_rule_contract_path": threshold["paths"]["selected_contract"],
            "event_schema_path": plan["paths"]["event_schema"],
            "evidence_contract_path": plan["paths"]["evidence_contract"],
            "actual_path_outcome_contract_path": plan["paths"]["actual_path_outcome_contract"],
            "manual_review_boundary_path": plan["paths"]["manual_review_boundary"],
            "continuation_decision_path": forward["paths"]["decision_matrix"],
            "runtime_event_logger_allowed_for_next_task": bool(
                decision_matrix.get("runtime_observe_integration_allowed")
            ),
            "runtime_scheduler_enabled_in_2341": False,
            "required_runtime_inputs": [
                "selected_trigger_rule",
                "risk_cap_trigger_series",
                "known_at_timestamp",
                "target_asset",
                "trading_calendar",
            ],
            "required_runtime_outputs": [
                "observe_event_log_increment",
                "pending_outcome_registry_increment",
                "monthly_concentration_monitoring_update",
                "manual_review_context_queue_update",
            ],
            "blocked_runtime_outputs": [
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_outcome_update_policy() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "outcome_update_policy_id": "HIGH_INTENSITY_OUTCOME_UPDATE_POLICY_V1",
            "outcome_update_mode": "scheduled_actual_path_binding",
            "update_horizons": ["1d", "5d", "10d", "20d"],
            "update_frequency": "after_required_horizon_matures",
            "outcome_binding_task": {
                "current": "TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder",
                "future_runtime_equivalent": "future_outcome_update_job",
            },
            "not_due_handling": "retain OUTCOME_NOT_DUE until horizon matures",
            "partial_coverage_handling": "carry forward caveat until every required horizon binds",
            "original_event_log_mutation_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_manual_review_context_policy() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "manual_review_context_allowed": True,
            "manual_review_context_scope": [
                "risk warning context",
                "event id",
                "cluster context",
                "selected rule context",
                "outcome caveat context",
            ],
            "manual_review_context_not_allowed": [
                "reduce position instruction",
                "increase cash instruction",
                "target weight",
                "rebalance instruction",
                "broker order",
            ],
            "display_label": "HIGH_INTENSITY_RISK_WARNING_CONTEXT",
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_stop_refine_archive_policy(inputs: Mapping[str, Any]) -> dict[str, Any]:
    rules = inputs["forward_observe_plan"]["stop_continue_archive_rules"]
    return clean_for_yaml(
        {
            "policy_id": "HIGH_INTENSITY_STOP_REFINE_ARCHIVE_POLICY_V1",
            "policy_version": "v1",
            "continue_conditions": list(rules.get("continue_conditions", []))
            + [
                "downside capture remains supportive",
                "false warning remains acceptable",
                "missed upside remains acceptable",
                "monthly concentration controlled",
            ],
            "refine_threshold_conditions": list(rules.get("refine_threshold_conditions", []))
            + [
                "false warning rises",
                "missed upside rises",
                "monthly concentration worsens",
                "trigger density exceeds guardrail",
            ],
            "pause_conditions": [
                "data quality issue",
                "incomplete outcome update",
                "cluster dedup issue",
            ],
            "archive_conditions": list(rules.get("archive_conditions", []))
            + [
                "downside capture weakens",
                "false warning high",
                "manual review context weak",
                "no incremental warning value",
            ],
            "owner_review_conditions": list(rules.get("owner_review_conditions", [])),
            "minimum_new_event_count_before_next_review": MIN_NEW_EVENT_COUNT_BEFORE_NEXT_REVIEW,
            "minimum_new_cluster_count_before_next_review": (
                MIN_NEW_CLUSTER_COUNT_BEFORE_NEXT_REVIEW
            ),
            "minimum_observe_months_before_next_review": MIN_OBSERVE_MONTHS_BEFORE_NEXT_REVIEW,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_runtime_integration_prerequisite_checklist(
    *,
    selected_rule_contract: Mapping[str, Any],
    observe_scope: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    monthly_plan: Mapping[str, Any],
    event_logger_contract: Mapping[str, Any],
    outcome_policy: Mapping[str, Any],
    manual_review_policy: Mapping[str, Any],
    stop_policy: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if partial_caveat.get("partial_coverage_caveat_required") is True:
        warnings.append("PARTIAL_COVERAGE_CAVEAT")
    if monthly_plan.get("monitoring_required") is True:
        warnings.append("MONTHLY_CONCENTRATION_MONITORING_REQUIRED")
    if selected_rule_contract.get("rule_changed") is True:
        blockers.append("SELECTED_RULE_CHANGED_IN_2341")
    if observe_scope.get("runtime_scheduler_enabled") is True:
        blockers.append("RUNTIME_SCHEDULER_ENABLED_IN_2341")
    ready = all(
        [
            selected_rule_contract.get("rule_continued") is True,
            event_logger_contract.get("runtime_event_logger_allowed_for_next_task") is True,
            partial_caveat.get("caveat_carryforward_required_for_2342") is True,
            monthly_plan.get("monitoring_required") is True,
            outcome_policy.get("original_event_log_mutation_allowed") is False,
            manual_review_policy.get("manual_review_context_allowed") is True,
            bool(stop_policy.get("continue_conditions")),
        ]
    )
    if blockers:
        status = "BLOCKED"
    elif ready and warnings:
        status = "READY_FOR_2342_WITH_CAVEATS"
    elif ready:
        status = "READY_FOR_2342_RUNTIME_INTEGRATION_PLAN"
    else:
        status = "PREREQUISITE_REMEDIATION_REQUIRED"
    return clean_for_yaml(
        {
            "selected_rule_continuation_ready": selected_rule_contract.get("rule_continued"),
            "event_logger_contract_ready": event_logger_contract.get(
                "runtime_event_logger_allowed_for_next_task"
            ),
            "event_schema_ready": bool(event_logger_contract.get("event_schema_path")),
            "pending_outcome_registry_contract_ready": bool(outcome_policy.get("update_horizons")),
            "manual_review_boundary_ready": manual_review_policy.get(
                "manual_review_context_allowed"
            ),
            "monthly_concentration_monitoring_ready": monthly_plan.get("monitoring_required"),
            "partial_coverage_caveat_ready": partial_caveat.get(
                "caveat_carryforward_required_for_2342"
            ),
            "stop_refine_archive_policy_ready": bool(stop_policy.get("continue_conditions")),
            "safety_boundary_ready": True,
            "paper_shadow_started": False,
            "production_started": False,
            "prerequisite_status": status,
            "prerequisite_blockers": blockers,
            "prerequisite_warnings": warnings,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2342_readiness_checklist(
    *,
    decision_matrix: Mapping[str, Any],
    selected_rule_contract: Mapping[str, Any],
    observe_scope: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    monthly_plan: Mapping[str, Any],
    event_logger_contract: Mapping[str, Any],
    outcome_policy: Mapping[str, Any],
    manual_review_policy: Mapping[str, Any],
    stop_policy: Mapping[str, Any],
    prerequisite: Mapping[str, Any],
) -> dict[str, Any]:
    warnings = list(prerequisite.get("prerequisite_warnings", []))
    blockers = list(prerequisite.get("prerequisite_blockers", []))
    if blockers:
        status = "CONTINUATION_DECISION_BLOCKED"
    elif warnings:
        status = "READY_FOR_2342_WITH_CAVEATS"
    else:
        status = "READY_FOR_2342_OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN"
    return clean_for_yaml(
        {
            "continue_decision_confirmed": decision_matrix.get("overall_decision")
            != "BLOCKED",
            "selected_rule_continuation_contract_generated": bool(
                selected_rule_contract.get("contract_id")
            ),
            "observe_continuation_scope_generated": bool(observe_scope.get("scope_id")),
            "partial_coverage_caveat_generated": bool(partial_caveat),
            "monthly_concentration_monitoring_plan_generated": bool(monthly_plan),
            "event_logger_continuation_contract_generated": bool(
                event_logger_contract.get("contract_id")
            ),
            "outcome_update_policy_generated": bool(outcome_policy.get("outcome_update_policy_id")),
            "manual_review_context_policy_generated": manual_review_policy.get(
                "manual_review_context_allowed"
            ),
            "stop_refine_archive_policy_generated": bool(stop_policy.get("policy_id")),
            "runtime_integration_prerequisite_checklist_generated": bool(prerequisite),
            "paper_shadow_started": False,
            "production_started": False,
            "readiness_status": status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2342_task_route(readiness: Mapping[str, Any]) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", "CONTINUATION_DECISION_BLOCKED"))
    blockers = list(readiness.get("readiness_blockers", []))
    warnings = list(readiness.get("readiness_warnings", []))
    if status == "READY_FOR_2342_OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN":
        next_task = NEXT_2342_INTEGRATION_TASK
    elif status == "READY_FOR_2342_WITH_CAVEATS":
        next_task = NEXT_2342_INTEGRATION_TASK
    elif status == "PREREQUISITE_REMEDIATION_REQUIRED":
        next_task = NEXT_2342_REMEDIATION_TASK
    elif any("PARTIAL" in str(blocker) for blocker in blockers):
        next_task = NEXT_2342_WAIT_TASK
    else:
        next_task = NEXT_2342_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "allowed_routes": [
                NEXT_2342_INTEGRATION_TASK,
                NEXT_2342_REMEDIATION_TASK,
                NEXT_2342_WAIT_TASK,
                NEXT_2342_REFINEMENT_TASK,
                NEXT_2342_ARCHIVE_TASK,
            ],
            "readiness_status": status,
            "next_task": next_task,
            "route_caveats": warnings,
            "route_blockers": blockers,
            "route_rationale": _route_rationale(status, warnings, blockers),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_continue_observe_interpretation_boundary(
    *,
    generated_at: datetime,
    partial_caveat: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "research_only": True,
            "continue_forward_observe_decision_only": True,
            "runtime_scheduler_enabled": False,
            "new_event_logging_executed": False,
            "outcome_binding_executed": False,
            "automatic_exposure_cap_allowed": False,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "partial_coverage_caveat_required": partial_caveat.get(
                "partial_coverage_caveat_required"
            ),
            "manual_review_only": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "blocked_interpretation": [
                "real account performance",
                "real position advice",
                "reduce position signal",
                "paper-shadow signal",
                "production strategy",
                "broker action",
            ],
        }
    )


def build_high_intensity_continue_observe_safety_boundary(
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
            "blocked_outputs": [
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
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_continue_observe_decision_summary(
    *,
    generated_at: datetime,
    forward_outcome_review_dir: Path,
    partial_readiness_dir: Path,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    inputs: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    selected_rule_contract: Mapping[str, Any],
    observe_scope: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    monthly_plan: Mapping[str, Any],
    event_logger_contract: Mapping[str, Any],
    outcome_policy: Mapping[str, Any],
    manual_review_policy: Mapping[str, Any],
    stop_policy: Mapping[str, Any],
    prerequisite: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    source_summary = inputs["outcome_binder"]["summary"]
    forward_summary = inputs["forward_outcome_review"]["summary"]
    event_summary = inputs["event_logger"]["summary"]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Continue Forward Observe Decision",
            "status": "CONTINUE_FORWARD_OBSERVE_DECISION_CONFIRMED_WITH_CAVEATS_PROMOTION_BLOCKED",
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "forward_outcome_review_dir": str(forward_outcome_review_dir),
            "partial_readiness_dir": str(partial_readiness_dir),
            "outcome_binder_dir": str(outcome_binder_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "source_2340_status": forward_summary.get("status"),
            "source_2340_recommendation": forward_summary.get("overall_recommendation"),
            "source_2340_next_task": forward_summary.get("next_task"),
            "decision_status": decision_matrix.get("decision_status"),
            "overall_decision": decision_matrix.get("overall_decision"),
            "selected_rule_id": selected_rule_contract.get("selected_rule_id"),
            "selected_rule_hash": selected_rule_contract.get("selected_rule_hash"),
            "selected_rule_continued": selected_rule_contract.get("rule_continued"),
            "selected_rule_changed": selected_rule_contract.get("rule_changed"),
            "partial_coverage_caveat_required": partial_caveat.get(
                "partial_coverage_caveat_required"
            ),
            "partial_coverage_ratio": partial_caveat.get("coverage_ratio"),
            "bound_outcome_count": partial_caveat.get("bound_outcome_count"),
            "expected_outcome_count": partial_caveat.get("expected_outcome_count"),
            "not_due_outcome_count": partial_caveat.get("not_due_outcome_count"),
            "critical_clusters_with_not_due": partial_caveat.get(
                "critical_clusters_with_not_due"
            ),
            "monthly_concentration_monitoring_required": monthly_plan.get(
                "monitoring_required"
            ),
            "monthly_concentration_monitoring_status": monthly_plan.get(
                "monitoring_status"
            ),
            "inherited_monthly_warning": monthly_plan.get("inherited_warning"),
            "event_logger_trigger_day_count": event_summary.get("trigger_day_count"),
            "event_logger_event_count_after_dedup": event_summary.get(
                "event_count_after_dedup"
            ),
            "event_logger_cluster_count": event_summary.get("cluster_count"),
            "runtime_integration_prerequisite_status": prerequisite.get(
                "prerequisite_status"
            ),
            "2342_readiness_status": readiness.get("readiness_status"),
            "next_task": task_route.get("next_task"),
            "continue_observe_decision_matrix_generated": bool(decision_matrix),
            "selected_rule_continuation_contract_generated": bool(selected_rule_contract),
            "observe_continuation_scope_generated": bool(observe_scope),
            "partial_coverage_carryforward_caveat_generated": bool(partial_caveat),
            "monthly_concentration_monitoring_plan_generated": bool(monthly_plan),
            "event_logger_continuation_contract_generated": bool(event_logger_contract),
            "outcome_update_policy_generated": bool(outcome_policy),
            "manual_review_context_policy_generated": bool(manual_review_policy),
            "stop_refine_archive_policy_generated": bool(stop_policy),
            "runtime_integration_prerequisite_checklist_generated": bool(prerequisite),
            "2342_readiness_checklist_generated": bool(readiness),
            "2342_task_route_generated": bool(task_route),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "not rerun because TRADING-2341 only reads prior validated "
                "TRADING-2340 / 2339 / 2337 artifacts and does not consume "
                "market data directly"
            ),
            "source_validate_data_executed": source_summary.get("validate_data_executed"),
            "source_validate_data_as_of": source_summary.get("validate_data_as_of"),
            "source_validate_data_status": source_summary.get("validate_data_status"),
            "source_validate_data_error_count": source_summary.get(
                "validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_continue_observe_decision_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    selected_rule_contract: Mapping[str, Any],
    observe_scope: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    monthly_plan: Mapping[str, Any],
    event_logger_contract: Mapping[str, Any],
    outcome_policy: Mapping[str, Any],
    manual_review_policy: Mapping[str, Any],
    stop_policy: Mapping[str, Any],
    prerequisite: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["decision_matrix"], decision_matrix)
    write_json(paths["selected_rule_contract"], selected_rule_contract)
    write_json(paths["observe_scope"], observe_scope)
    write_json(paths["partial_caveat"], partial_caveat)
    write_json(paths["monthly_plan"], monthly_plan)
    write_json(paths["event_logger_contract"], event_logger_contract)
    write_json(paths["outcome_policy"], outcome_policy)
    write_json(paths["manual_review_policy"], manual_review_policy)
    write_json(paths["stop_policy"], stop_policy)
    write_json(paths["prerequisite"], prerequisite)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], _render_main_doc(summary, decision_matrix, task_route))
    write_markdown(
        paths["selected_rule_doc"],
        _render_selected_rule_doc(selected_rule_contract),
    )
    write_markdown(paths["scope_doc"], _render_scope_doc(observe_scope, manual_review_policy))
    write_markdown(paths["monthly_doc"], _render_monthly_doc(monthly_plan))
    write_markdown(paths["route_doc"], _render_route_doc(readiness, task_route, stop_policy))
    return {key: str(path) for key, path in paths.items()}


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_continue_observe_decision_summary.json",
        "decision_matrix": output_dir
        / "high_intensity_continue_observe_decision_matrix.json",
        "selected_rule_contract": output_dir
        / "high_intensity_selected_rule_continuation_contract.json",
        "observe_scope": output_dir / "high_intensity_observe_continuation_scope.json",
        "partial_caveat": output_dir
        / "high_intensity_partial_coverage_carryforward_caveat.json",
        "monthly_plan": output_dir
        / "high_intensity_monthly_concentration_monitoring_plan.json",
        "event_logger_contract": output_dir
        / "high_intensity_event_logger_continuation_contract.json",
        "outcome_policy": output_dir / "high_intensity_outcome_update_policy.json",
        "manual_review_policy": output_dir / "high_intensity_manual_review_context_policy.json",
        "stop_policy": output_dir / "high_intensity_stop_refine_archive_policy.json",
        "prerequisite": output_dir
        / "high_intensity_runtime_integration_prerequisite_checklist.json",
        "readiness": output_dir / "high_intensity_2342_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2342_task_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_continue_observe_interpretation_boundary.json",
        "safety_boundary": output_dir / "high_intensity_continue_observe_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_continue_forward_observe_decision.md",
        "selected_rule_doc": docs_root
        / "high_intensity_selected_rule_continuation_contract.md",
        "scope_doc": docs_root / "high_intensity_observe_continuation_scope.md",
        "monthly_doc": docs_root
        / "high_intensity_monthly_concentration_monitoring_plan.md",
        "route_doc": docs_root / "high_intensity_2342_readiness_route.md",
    }


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityContinueObserveDecisionError(f"{label} missing {key}: {path}")
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityContinueObserveDecisionError(f"{path}: expected JSON object")
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensityContinueObserveDecisionError(
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


def _monthly_monitoring_required(
    monthly_report: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
) -> bool:
    return bool(_inherited_monthly_warning(monthly_report, monthly_review)) or str(
        monthly_review.get("monthly_concentration_effect_label", "")
    ) != "CONCENTRATION_LOW_IMPACT"


def _inherited_monthly_warning(
    monthly_report: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
) -> str:
    warnings = monthly_report.get("monthly_concentration_warnings") or []
    if isinstance(warnings, Sequence) and not isinstance(warnings, str | bytes):
        for warning in warnings:
            if str(warning):
                return str(warning)
    if monthly_report.get("inherited_2335_warning") is True:
        return "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
    if monthly_review.get("monthly_concentration_warning_inherited") is True:
        return "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
    return ""


def _route_rationale(status: str, warnings: Sequence[str], blockers: Sequence[str]) -> str:
    if blockers:
        return f"blocked by prerequisite blockers: {', '.join(map(str, blockers))}"
    if status == "READY_FOR_2342_WITH_CAVEATS":
        caveats = ", ".join(map(str, warnings))
        return f"observe-only integration plan allowed with caveats: {caveats}"
    if status == "READY_FOR_2342_OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN":
        return "observe-only integration plan prerequisites are complete"
    return "continuation decision is not ready for runtime integration"


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


def _render_main_doc(
    summary: Mapping[str, Any],
    decision: Mapping[str, Any],
    route: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Risk-Cap Continue Forward Observe Decision\n\n"
        f"- status: `{summary.get('status')}`\n"
        f"- source_2340_recommendation: `{summary.get('source_2340_recommendation')}`\n"
        f"- overall_decision: `{decision.get('overall_decision')}`\n"
        f"- selected_rule_id: `{summary.get('selected_rule_id')}`\n"
        f"- partial_coverage_ratio: `{summary.get('partial_coverage_ratio')}`\n"
        f"- inherited_monthly_warning: `{summary.get('inherited_monthly_warning')}`\n"
        f"- next_task: `{route.get('next_task')}`\n\n"
        "本报告只确认 continue observe decision 和 2342 route；不启动 runtime，"
        "不生成新 event，不重新绑定 outcome，不输出 target weight / rebalance / broker action。\n"
    )


def _render_selected_rule_doc(contract: Mapping[str, Any]) -> str:
    return (
        "# High-Intensity Selected Rule Continuation Contract\n\n"
        f"- selected_rule_id: `{contract.get('selected_rule_id')}`\n"
        f"- selected_rule_hash: `{contract.get('selected_rule_hash')}`\n"
        f"- rule_continued: `{contract.get('rule_continued')}`\n"
        f"- rule_changed: `{contract.get('rule_changed')}`\n"
        f"- rule_change_allowed_in_2341: `{contract.get('rule_change_allowed_in_2341')}`\n\n"
        "2341 不修改 selected rule；threshold refinement 必须走独立 route。\n"
    )


def _render_scope_doc(
    scope: Mapping[str, Any],
    manual_review_policy: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Observe Continuation Scope\n\n"
        f"- continuation_mode: `{scope.get('continuation_mode')}`\n"
        f"- manual_review_context_allowed: "
        f"`{manual_review_policy.get('manual_review_context_allowed')}`\n"
        f"- automatic_exposure_cap_allowed: `{scope.get('automatic_exposure_cap_allowed')}`\n"
        f"- runtime_scheduler_enabled: `{scope.get('runtime_scheduler_enabled')}`\n\n"
        "Manual-review context 只能展示 warning / event / cluster / caveat context，"
        "不能解释为减仓、target weight、rebalance 或 broker order。\n"
    )


def _render_monthly_doc(monthly_plan: Mapping[str, Any]) -> str:
    return (
        "# High-Intensity Monthly Concentration Monitoring Plan\n\n"
        f"- monitoring_required: `{monthly_plan.get('monitoring_required')}`\n"
        f"- inherited_warning: `{monthly_plan.get('inherited_warning')}`\n"
        f"- monitoring_status: `{monthly_plan.get('monitoring_status')}`\n"
        f"- guardrails: `{monthly_plan.get('guardrails')}`\n\n"
        "Monthly concentration warning 不阻断继续 observe，但必须成为 2342 runtime "
        "integration plan 的 hard monitoring field。\n"
    )


def _render_route_doc(
    readiness: Mapping[str, Any],
    route: Mapping[str, Any],
    stop_policy: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity 2342 Readiness Route\n\n"
        f"- readiness_status: `{readiness.get('readiness_status')}`\n"
        f"- next_task: `{route.get('next_task')}`\n"
        f"- route_caveats: `{route.get('route_caveats')}`\n"
        f"- minimum_new_event_count_before_next_review: "
        f"`{stop_policy.get('minimum_new_event_count_before_next_review')}`\n"
        f"- minimum_new_cluster_count_before_next_review: "
        f"`{stop_policy.get('minimum_new_cluster_count_before_next_review')}`\n"
        f"- minimum_observe_months_before_next_review: "
        f"`{stop_policy.get('minimum_observe_months_before_next_review')}`\n\n"
        "2342 仍只能设计 observe-only runtime integration plan；promotion、paper-shadow、"
        "production 和 broker action 继续关闭。\n"
    )
