from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    DEFAULT_EVENT_LOGGER_ROOT,
    DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    DEFAULT_OUTCOME_BINDER_ROOT,
    DEFAULT_THRESHOLD_SELECTION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PARTIAL_READINESS_ROOT,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2340_HIGH_INTENSITY_RISK_CAP_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT"
REPORT_TYPE = "high_intensity_risk_cap_forward_outcome_review"
ARTIFACT_ROLE = "high_intensity_risk_cap_forward_outcome_review"
MODE = "forward_outcome_review_with_partial_coverage_caveat"

EXPECTED_2339_STATUS = "READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT"
EXPECTED_2339_DECISION = "PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT"
EXPECTED_2339_ROUTE = (
    "TRADING-2340_High_Intensity_Risk_Cap_Forward_Outcome_Review_With_Partial_Coverage_Caveat"
)

OUTCOME_HORIZONS = ("1d", "5d", "10d", "20d")

NEXT_2341_CONTINUE_TASK = "TRADING-2341_High_Intensity_Risk_Cap_Continue_Forward_Observe_Decision"
NEXT_2341_REFINE_TASK = "TRADING-2341_High_Intensity_Risk_Cap_Threshold_Refinement_Plan"
NEXT_2341_MANUAL_TASK = "TRADING-2341_High_Intensity_Risk_Cap_Manual_Review_Only_Continuation_Plan"
NEXT_2341_WAIT_TASK = "TRADING-2341_High_Intensity_Risk_Cap_Wait_For_Full_20D_Coverage"
NEXT_2341_ARCHIVE_TASK = "TRADING-2341_Archive_High_Intensity_Risk_Cap_Observe_Line"
NEXT_2341_DATA_TASK = "TRADING-2341_High_Intensity_Risk_Cap_Outcome_Data_Remediation"

# TRADING-2340 owner attachment baseline for research review materiality only.
# These rates do not produce target weights, rebalance instructions, or runtime gates.
FALSE_WARNING_MODERATE_RATE = 0.25
FALSE_WARNING_HIGH_RATE = 0.45
FALSE_WARNING_BLOCKING_RATE = 0.65
# The missed-upside route is a would-have-cost context. TRADING-2340 mirrors the
# false-warning bands, with 0.50 as the first high-cost review threshold so the
# existing 2337 moderate label remains stable at 0.466667.
MISSED_UPSIDE_MODERATE_RATE = 0.25
MISSED_UPSIDE_HIGH_RATE = 0.50
MISSED_UPSIDE_BLOCKING_RATE = 0.65
# TRADING-2340 owner attachment baseline for downside-capture review labels.
DOWNSIDE_CAPTURE_WEAK_RATE = 0.15
DOWNSIDE_CAPTURE_MODERATE_RATE = 0.30
DOWNSIDE_CAPTURE_STRONG_RATE = 0.50
MANUAL_REVIEW_USEFUL_RATE = 0.50
MANUAL_REVIEW_MIXED_RATE = 0.30
PARTIAL_COVERAGE_HIGH_RATIO = 0.95
PARTIAL_COVERAGE_ACCEPTABLE_RATIO = 0.90
MONTHLY_DOMINANCE_SHARE = 0.25
MONTHLY_HIGH_CONCENTRATION_SHARE = 0.50
MONTHLY_MODERATE_CONCENTRATION_SHARE = 0.33

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "forward_outcome_review_only": True,
    "partial_coverage_caveat_required": True,
    "outcome_binding_executed": False,
    "original_event_log_mutated": False,
    "runtime_scheduler_enabled": False,
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
    "rebalance_instruction_generated",
    "broker_order_generated",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "paper_shadow_ready",
    "production_ready",
    "original_event_log_mutated",
}
FORBIDDEN_EMIT_FIELDS = {
    "target_weight_action",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
}


class HighIntensityForwardOutcomeReviewError(ValueError):
    pass


def run_high_intensity_risk_cap_forward_outcome_review(
    *,
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
        raise HighIntensityForwardOutcomeReviewError(
            f"high-intensity forward outcome review only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_forward_outcome_review_inputs(
        partial_readiness_dir=partial_readiness_dir,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
    )
    cluster_outcomes = records(inputs["outcome_binder"]["cluster_matrix"].get("rows"))
    event_logger_clusters = records(inputs["event_logger"]["cluster_registry"].get("rows"))
    trigger_context = records(inputs["outcome_binder"]["trigger_context"].get("rows"))
    cluster_review = build_high_intensity_cluster_outcome_review_matrix(
        cluster_matrix=cluster_outcomes,
        event_logger_clusters=event_logger_clusters,
    )
    horizon_review = build_high_intensity_horizon_outcome_review_matrix(
        cluster_matrix=cluster_outcomes,
    )
    false_warning_review = build_high_intensity_false_warning_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )
    missed_upside_review = build_high_intensity_missed_upside_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )
    downside_capture_review = build_high_intensity_downside_capture_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )
    manual_review = build_high_intensity_manual_review_usefulness_review(
        cluster_review=cluster_review,
        source_manual_review_report=inputs["outcome_binder"]["manual_review_report"],
        event_logger_event_count=len(records(inputs["event_logger"]["event_log"].get("rows"))),
    )
    rebound_stress_review = build_high_intensity_rebound_stress_review(
        cluster_review=cluster_review,
    )
    partial_caveat = build_high_intensity_partial_coverage_caveat_report(
        partial_summary=inputs["partial_readiness"]["summary"],
        not_due_impact_report=inputs["partial_readiness"]["not_due_impact_report"],
    )
    monthly_review = build_high_intensity_monthly_concentration_effect_review(
        cluster_review=cluster_review,
        event_logger_monthly_report=inputs["event_logger"]["monthly_report"],
    )
    weighted_evidence = build_high_intensity_cluster_weighted_evidence_summary(
        cluster_review=cluster_review,
        false_warning_review=false_warning_review,
        missed_upside_review=missed_upside_review,
        downside_capture_review=downside_capture_review,
        manual_review=manual_review,
        monthly_review=monthly_review,
    )
    threshold_assessment = build_high_intensity_threshold_rule_outcome_assessment(
        threshold_selection=inputs["threshold_selection"],
        false_warning_review=false_warning_review,
        missed_upside_review=missed_upside_review,
        downside_capture_review=downside_capture_review,
        manual_review=manual_review,
        monthly_review=monthly_review,
        partial_caveat=partial_caveat,
        weighted_evidence=weighted_evidence,
    )
    decision_matrix = build_high_intensity_continue_refine_archive_decision_matrix(
        source_data_quality=inputs["outcome_binder"]["data_quality_report"],
        partial_caveat=partial_caveat,
        false_warning_review=false_warning_review,
        missed_upside_review=missed_upside_review,
        downside_capture_review=downside_capture_review,
        manual_review=manual_review,
        monthly_review=monthly_review,
        selected_rule_assessment=threshold_assessment,
    )
    readiness = build_high_intensity_2341_readiness_checklist(
        decision_matrix=decision_matrix,
        source_data_quality=inputs["outcome_binder"]["data_quality_report"],
        partial_caveat=partial_caveat,
    )
    task_route = build_high_intensity_2341_task_route(decision_matrix)
    interpretation_boundary = build_high_intensity_forward_outcome_interpretation_boundary(
        generated_at=generated_at,
        source_data_quality=inputs["outcome_binder"]["data_quality_report"],
    )
    safety_boundary = build_high_intensity_forward_outcome_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_high_intensity_forward_outcome_review_summary(
        generated_at=generated_at,
        partial_readiness_dir=partial_readiness_dir,
        outcome_binder_dir=outcome_binder_dir,
        event_logger_dir=event_logger_dir,
        threshold_selection_dir=threshold_selection_dir,
        forward_observe_plan_dir=forward_observe_plan_dir,
        partial_summary=inputs["partial_readiness"]["summary"],
        source_summary=inputs["outcome_binder"]["summary"],
        source_data_quality=inputs["outcome_binder"]["data_quality_report"],
        trigger_context=trigger_context,
        cluster_review=cluster_review,
        horizon_review=horizon_review,
        false_warning_review=false_warning_review,
        missed_upside_review=missed_upside_review,
        downside_capture_review=downside_capture_review,
        manual_review=manual_review,
        partial_caveat=partial_caveat,
        monthly_review=monthly_review,
        weighted_evidence=weighted_evidence,
        threshold_assessment=threshold_assessment,
        decision_matrix=decision_matrix,
        readiness=readiness,
        task_route=task_route,
    )

    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_high_intensity_forward_outcome_review_outputs(
        paths=paths,
        summary=summary,
        cluster_review=cluster_review,
        horizon_review=horizon_review,
        false_warning_review=false_warning_review,
        missed_upside_review=missed_upside_review,
        downside_capture_review=downside_capture_review,
        manual_review=manual_review,
        rebound_stress_review=rebound_stress_review,
        partial_caveat=partial_caveat,
        monthly_review=monthly_review,
        weighted_evidence=weighted_evidence,
        threshold_assessment=threshold_assessment,
        decision_matrix=decision_matrix,
        readiness=readiness,
        task_route=task_route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_forward_outcome_review_inputs(
    *,
    partial_readiness_dir: Path,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    return {
        "partial_readiness": load_trading_2339_partial_readiness_outputs(partial_readiness_dir),
        "outcome_binder": load_trading_2337_forward_review_outputs(outcome_binder_dir),
        "event_logger": load_trading_2336_forward_review_lineage(event_logger_dir),
        "threshold_selection": load_trading_2335_forward_review_context(threshold_selection_dir),
        "forward_observe_plan": load_trading_2334_forward_review_context(forward_observe_plan_dir),
    }


def load_trading_2339_partial_readiness_outputs(partial_readiness_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": partial_readiness_dir / "high_intensity_partial_outcome_readiness_summary.json",
        "coverage_matrix": partial_readiness_dir
        / "high_intensity_partial_outcome_coverage_matrix.json",
        "not_due_matrix": partial_readiness_dir / "high_intensity_not_due_horizon_matrix.json",
        "not_due_impact_report": partial_readiness_dir
        / "high_intensity_not_due_cluster_impact_report.json",
        "not_due_distribution": partial_readiness_dir
        / "high_intensity_not_due_asset_horizon_distribution.json",
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
        "readiness": partial_readiness_dir / "high_intensity_2340_readiness_checklist.json",
        "task_route": partial_readiness_dir / "high_intensity_2340_task_route.json",
        "safety_boundary": partial_readiness_dir
        / "high_intensity_partial_outcome_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2339 partial readiness")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2339 {key}", payload)

    summary = payloads["summary"]
    route = payloads["task_route"]
    if summary.get("status") != EXPECTED_2339_STATUS:
        raise HighIntensityForwardOutcomeReviewError(
            f"TRADING-2340 requires 2339 status {EXPECTED_2339_STATUS}"
        )
    if summary.get("decision") != EXPECTED_2339_DECISION:
        raise HighIntensityForwardOutcomeReviewError(
            f"TRADING-2340 requires 2339 decision {EXPECTED_2339_DECISION}"
        )
    if route.get("next_task") != EXPECTED_2339_ROUTE:
        raise HighIntensityForwardOutcomeReviewError(
            f"TRADING-2340 requires 2339 route next_task {EXPECTED_2339_ROUTE}"
        )
    if summary.get("next_task") != EXPECTED_2339_ROUTE:
        raise HighIntensityForwardOutcomeReviewError(
            f"TRADING-2340 requires 2339 summary next_task {EXPECTED_2339_ROUTE}"
        )
    if int(summary.get("blocked_outcome_count") or 0) > 0:
        raise HighIntensityForwardOutcomeReviewError("TRADING-2340 blocks covered outcome gaps")
    if int(summary.get("critical_clusters_with_not_due") or 0) > 0:
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 blocks critical clusters with not-due horizons"
        )
    if to_float(summary.get("coverage_ratio")) < PARTIAL_COVERAGE_ACCEPTABLE_RATIO:
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires acceptable partial coverage"
        )
    return {"source_dir": str(partial_readiness_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2337_forward_review_outputs(outcome_binder_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": outcome_binder_dir / "high_intensity_outcome_binder_summary.json",
        "event_matrix": outcome_binder_dir / "high_intensity_event_actual_path_outcome_matrix.json",
        "cluster_matrix": outcome_binder_dir
        / "high_intensity_cluster_actual_path_outcome_matrix.json",
        "trigger_context": outcome_binder_dir
        / "high_intensity_trigger_day_actual_path_context.json",
        "coverage_report": outcome_binder_dir / "high_intensity_outcome_coverage_report.json",
        "horizon_quality_report": outcome_binder_dir
        / "high_intensity_horizon_outcome_quality_report.json",
        "rebound_stress_matrix": outcome_binder_dir
        / "high_intensity_rebound_stress_classification_matrix.json",
        "false_warning_report": outcome_binder_dir
        / "high_intensity_false_warning_classification_report.json",
        "missed_upside_report": outcome_binder_dir
        / "high_intensity_missed_upside_classification_report.json",
        "downside_capture_report": outcome_binder_dir
        / "high_intensity_downside_capture_classification_report.json",
        "manual_review_report": outcome_binder_dir
        / "high_intensity_manual_review_usefulness_proxy_report.json",
        "cluster_policy": outcome_binder_dir / "high_intensity_cluster_weighting_policy.json",
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
    cluster_policy = payloads["cluster_policy"]
    if summary.get("validate_data_executed") is not True:
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires source validate_data_executed=true"
        )
    if str(summary.get("validate_data_status", "")) not in {"PASS", "PASS_WITH_WARNINGS"}:
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires source validate_data_status PASS/PASS_WITH_WARNINGS"
        )
    if int(summary.get("validate_data_error_count") or 0) != 0:
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires source validate_data_error_count=0"
        )
    if str(data_quality.get("data_quality_status", "")) == "FAIL":
        raise HighIntensityForwardOutcomeReviewError("TRADING-2340 blocks data_quality_status=FAIL")
    if cluster_policy.get("primary_analysis_level") != "cluster":
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires 2337 primary_analysis_level=cluster"
        )
    if cluster_policy.get("trigger_day_level_usage") != "context_only":
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires trigger_day_level_usage=context_only"
        )
    if not records(payloads["cluster_matrix"].get("rows")):
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires non-empty cluster outcome matrix"
        )
    return {"source_dir": str(outcome_binder_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2336_forward_review_lineage(event_logger_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": event_logger_dir / "high_intensity_event_logger_summary.json",
        "event_log": event_logger_dir / "high_intensity_observe_event_log.json",
        "cluster_registry": event_logger_dir / "high_intensity_observe_event_cluster_registry.json",
        "monthly_report": event_logger_dir / "high_intensity_monthly_concentration_report.json",
        "manual_review_queue": event_logger_dir / "high_intensity_manual_review_event_queue.json",
        "interpretation_boundary": event_logger_dir
        / "high_intensity_event_logger_interpretation_boundary.json",
        "safety_boundary": event_logger_dir / "high_intensity_event_logger_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2336 event logger lineage")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2336 {key}", payload)
    if not records(payloads["cluster_registry"].get("rows")):
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires non-empty 2336 cluster registry"
        )
    return {"source_dir": str(event_logger_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2335_forward_review_context(threshold_selection_dir: Path) -> dict[str, Any]:
    paths = {
        "selected_rule": threshold_selection_dir / "high_intensity_selected_trigger_rule.json",
        "caveat_report": threshold_selection_dir
        / "high_intensity_threshold_selection_caveat_report.json",
        "backtest_context": threshold_selection_dir
        / "high_intensity_selected_rule_backtest_context.json",
        "manual_review_boundary": threshold_selection_dir
        / "high_intensity_selected_rule_manual_review_boundary.json",
        "safety_boundary": threshold_selection_dir
        / "high_intensity_threshold_selection_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2335 threshold context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2335 {key}", payload)
    selected_rule = str(payloads["selected_rule"].get("selected_rule_id", ""))
    if selected_rule != "COMPOSITE_HIGH_INTENSITY_RULE":
        raise HighIntensityForwardOutcomeReviewError(
            "TRADING-2340 requires selected_rule_id=COMPOSITE_HIGH_INTENSITY_RULE"
        )
    return {"source_dir": str(threshold_selection_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2334_forward_review_context(forward_observe_plan_dir: Path) -> dict[str, Any]:
    paths = {
        "false_warning_framework": forward_observe_plan_dir
        / "high_intensity_false_warning_missed_stress_framework.json",
        "stop_continue_archive_rules": forward_observe_plan_dir
        / "high_intensity_stop_continue_archive_rules.json",
        "manual_review_boundary": forward_observe_plan_dir
        / "high_intensity_manual_review_boundary.json",
        "safety_boundary": forward_observe_plan_dir
        / "high_intensity_forward_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2334 forward observe context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2334 {key}", payload)
    return {"source_dir": str(forward_observe_plan_dir), "paths": _string_paths(paths), **payloads}


def build_high_intensity_cluster_outcome_review_matrix(
    *,
    cluster_matrix: Sequence[Mapping[str, Any]],
    event_logger_clusters: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows_by_cluster: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in cluster_matrix:
        rows_by_cluster[str(row.get("event_cluster_id"))].append(row)
    logger_by_cluster = {
        str(row.get("event_cluster_id")): row
        for row in event_logger_clusters
        if row.get("event_cluster_id")
    }
    review_rows: list[dict[str, Any]] = []
    for cluster_id, rows in sorted(
        rows_by_cluster.items(), key=lambda item: _cluster_sort_key(item[1])
    ):
        horizon_by_id = {str(row.get("horizon")): row for row in rows}
        base = _first_mapping(rows, logger_by_cluster.get(cluster_id, {}))
        bound_count = sum(
            1 for horizon in OUTCOME_HORIZONS if _is_bound_row(horizon_by_id.get(horizon, {}))
        )
        not_due_count = sum(
            1 for horizon in OUTCOME_HORIZONS if _is_not_due_row(horizon_by_id.get(horizon, {}))
        )
        stress_any = any(_bool(row.get("cluster_stress_detected")) for row in rows)
        rebound_any = any(_bool(row.get("cluster_rebound_detected")) for row in rows)
        false_any = any(_bool(row.get("cluster_false_warning_candidate")) for row in rows)
        missed_any = any(_bool(row.get("cluster_missed_upside_candidate")) for row in rows)
        downside_any = any(_bool(row.get("cluster_downside_capture_candidate")) for row in rows)
        manual_any = any(
            _bool(row.get("cluster_manual_review_would_have_helped_candidate")) for row in rows
        )
        review_rows.append(
            clean_for_yaml(
                {
                    "event_cluster_id": cluster_id,
                    "target_asset": base.get("target_asset", ""),
                    "cluster_start_date": base.get("cluster_start_date", ""),
                    "cluster_end_date": base.get("cluster_end_date", ""),
                    "cluster_active_days": int(base.get("cluster_active_days") or 0),
                    "trigger_day_count": int(base.get("trigger_day_count") or 0),
                    "selected_rule_id": base.get(
                        "selected_rule_id", "COMPOSITE_HIGH_INTENSITY_RULE"
                    ),
                    "horizon_1d_status": _horizon_status(horizon_by_id.get("1d", {})),
                    "horizon_5d_status": _horizon_status(horizon_by_id.get("5d", {})),
                    "horizon_10d_status": _horizon_status(horizon_by_id.get("10d", {})),
                    "horizon_20d_status": _horizon_status(horizon_by_id.get("20d", {})),
                    "bound_horizon_count": bound_count,
                    "not_due_horizon_count": not_due_count,
                    "cluster_forward_return_1d": _horizon_float(
                        horizon_by_id, "1d", "cluster_forward_return"
                    ),
                    "cluster_forward_return_5d": _horizon_float(
                        horizon_by_id, "5d", "cluster_forward_return"
                    ),
                    "cluster_forward_return_10d": _horizon_float(
                        horizon_by_id, "10d", "cluster_forward_return"
                    ),
                    "cluster_forward_return_20d": _horizon_float(
                        horizon_by_id, "20d", "cluster_forward_return"
                    ),
                    "cluster_max_drawdown_1d": _horizon_float(
                        horizon_by_id, "1d", "cluster_forward_max_drawdown"
                    ),
                    "cluster_max_drawdown_5d": _horizon_float(
                        horizon_by_id, "5d", "cluster_forward_max_drawdown"
                    ),
                    "cluster_max_drawdown_10d": _horizon_float(
                        horizon_by_id, "10d", "cluster_forward_max_drawdown"
                    ),
                    "cluster_max_drawdown_20d": _horizon_float(
                        horizon_by_id, "20d", "cluster_forward_max_drawdown"
                    ),
                    "stress_detected_any_horizon": stress_any,
                    "rebound_detected_any_horizon": rebound_any,
                    "false_warning_candidate_any_horizon": false_any,
                    "missed_upside_candidate_any_horizon": missed_any,
                    "downside_capture_candidate_any_horizon": downside_any,
                    "manual_review_would_have_helped_candidate": manual_any,
                    "cluster_evidence_label": _cluster_evidence_label(
                        downside_any=downside_any,
                        false_any=false_any,
                        missed_any=missed_any,
                        rebound_any=rebound_any,
                        stress_any=stress_any,
                        not_due_count=not_due_count,
                    ),
                    "cluster_review_weight": round_float(bound_count / len(OUTCOME_HORIZONS)),
                    **SAFETY_FIELDS,
                }
            )
        )
    return review_rows


def build_high_intensity_horizon_outcome_review_matrix(
    *,
    cluster_matrix: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon in OUTCOME_HORIZONS:
        horizon_rows = [row for row in cluster_matrix if str(row.get("horizon")) == horizon]
        bound_rows = [row for row in horizon_rows if _is_bound_row(row)]
        expected = len(horizon_rows)
        bound = len(bound_rows)
        not_due = sum(1 for row in horizon_rows if _is_not_due_row(row))
        downside = sum(
            1 for row in bound_rows if _bool(row.get("cluster_downside_capture_candidate"))
        )
        false_warning = sum(
            1 for row in bound_rows if _bool(row.get("cluster_false_warning_candidate"))
        )
        missed = sum(1 for row in bound_rows if _bool(row.get("cluster_missed_upside_candidate")))
        rebound = sum(1 for row in bound_rows if _bool(row.get("cluster_rebound_detected")))
        stress = sum(1 for row in bound_rows if _bool(row.get("cluster_stress_detected")))
        forward_returns = [to_float(row.get("cluster_forward_return")) for row in bound_rows]
        drawdowns = [to_float(row.get("cluster_forward_max_drawdown")) for row in bound_rows]
        false_rate = _rate(false_warning, bound)
        missed_rate = _rate(missed, bound)
        downside_rate = _rate(downside, bound)
        coverage_ratio = _rate(bound, expected)
        rows.append(
            clean_for_yaml(
                {
                    "horizon": horizon,
                    "expected_cluster_count": expected,
                    "bound_cluster_count": bound,
                    "not_due_cluster_count": not_due,
                    "coverage_ratio": coverage_ratio,
                    "downside_capture_cluster_count": downside,
                    "false_warning_cluster_count": false_warning,
                    "missed_upside_cluster_count": missed,
                    "rebound_cluster_count": rebound,
                    "stress_cluster_count": stress,
                    "downside_capture_rate": downside_rate,
                    "false_warning_rate": false_rate,
                    "missed_upside_rate": missed_rate,
                    "rebound_rate": _rate(rebound, bound),
                    "stress_rate": _rate(stress, bound),
                    "average_forward_return": _average(forward_returns),
                    "average_forward_max_drawdown": _average(drawdowns),
                    "worst_forward_max_drawdown": min(drawdowns) if drawdowns else 0.0,
                    "horizon_evidence_label": _horizon_evidence_label(
                        coverage_ratio=coverage_ratio,
                        downside_rate=downside_rate,
                        false_warning_rate=false_rate,
                        missed_upside_rate=missed_rate,
                        bound_count=bound,
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_false_warning_review(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
    horizon_review: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ready = _ready_clusters(cluster_review)
    flagged = [row for row in ready if _bool(row.get("false_warning_candidate_any_horizon"))]
    rate = _rate(len(flagged), len(ready))
    label = _false_warning_label(rate, bool(ready))
    return clean_for_yaml(
        {
            "cluster_count": len(cluster_review),
            "outcome_ready_cluster_count": len(ready),
            "false_warning_cluster_count": len(flagged),
            "false_warning_cluster_rate": rate,
            "false_warning_by_horizon": {
                str(row.get("horizon")): int(row.get("false_warning_cluster_count") or 0)
                for row in horizon_review
            },
            "false_warning_by_asset": _count_by(flagged, "target_asset"),
            "false_warning_by_month": _count_by_month(flagged),
            "false_warning_context_summary": (
                "false warning is measured as cluster-level would-have-warning context; "
                "no exposure cap was executed"
            ),
            "false_warning_materiality": _materiality_from_label(label),
            "false_warning_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_missed_upside_review(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
    horizon_review: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ready = _ready_clusters(cluster_review)
    flagged = [row for row in ready if _bool(row.get("missed_upside_candidate_any_horizon"))]
    rate = _rate(len(flagged), len(ready))
    label = _missed_upside_label(rate, bool(ready))
    returns = [value for row in flagged for value in _cluster_return_values(row) if value > 0]
    return clean_for_yaml(
        {
            "cluster_count": len(cluster_review),
            "outcome_ready_cluster_count": len(ready),
            "missed_upside_cluster_count": len(flagged),
            "missed_upside_cluster_rate": rate,
            "missed_upside_by_horizon": {
                str(row.get("horizon")): int(row.get("missed_upside_cluster_count") or 0)
                for row in horizon_review
            },
            "missed_upside_by_asset": _count_by(flagged, "target_asset"),
            "missed_upside_by_month": _count_by_month(flagged),
            "missed_upside_return_context": {
                "average_forward_return": _average(returns),
                "max_forward_return": max(returns) if returns else 0.0,
                "sample_count": len(returns),
                "interpretation": "would-have-cost context only; no exposure cap was executed",
            },
            "missed_upside_materiality": _materiality_from_label(label),
            "missed_upside_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_downside_capture_review(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
    horizon_review: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ready = _ready_clusters(cluster_review)
    downside = [row for row in ready if _bool(row.get("downside_capture_candidate_any_horizon"))]
    stress = [row for row in ready if _bool(row.get("stress_detected_any_horizon"))]
    rate = _rate(len(downside), len(ready))
    stress_rate = _rate(len(stress), len(ready))
    drawdowns = [value for row in ready for value in _cluster_drawdown_values(row)]
    label = _downside_capture_label(rate, bool(ready))
    return clean_for_yaml(
        {
            "cluster_count": len(cluster_review),
            "outcome_ready_cluster_count": len(ready),
            "downside_capture_cluster_count": len(downside),
            "downside_capture_cluster_rate": rate,
            "stress_detected_cluster_count": len(stress),
            "stress_detected_cluster_rate": stress_rate,
            "average_forward_max_drawdown_after_warning": _average(drawdowns),
            "worst_forward_max_drawdown_after_warning": min(drawdowns) if drawdowns else 0.0,
            "downside_capture_by_horizon": {
                str(row.get("horizon")): int(row.get("downside_capture_cluster_count") or 0)
                for row in horizon_review
            },
            "downside_capture_by_asset": _count_by(downside, "target_asset"),
            "downside_capture_materiality": _downside_materiality(label),
            "downside_capture_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_manual_review_usefulness_review(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
    source_manual_review_report: Mapping[str, Any],
    event_logger_event_count: int,
) -> dict[str, Any]:
    ready = _ready_clusters(cluster_review)
    helped = [row for row in ready if _bool(row.get("manual_review_would_have_helped_candidate"))]
    false_warning = [row for row in ready if _bool(row.get("false_warning_candidate_any_horizon"))]
    missed = [row for row in ready if _bool(row.get("missed_upside_candidate_any_horizon"))]
    proxy = _rate(len(helped), len(ready))
    label = _manual_review_label(proxy, bool(ready))
    return clean_for_yaml(
        {
            "manual_review_event_count": int(
                source_manual_review_report.get("manual_review_event_count")
                or event_logger_event_count
            ),
            "outcome_ready_manual_review_cluster_count": len(ready),
            "manual_review_would_have_helped_cluster_count": len(helped),
            "manual_review_false_warning_cluster_count": len(false_warning),
            "manual_review_missed_upside_cluster_count": len(missed),
            "manual_review_usefulness_proxy": proxy,
            "manual_review_usefulness_label": label,
            "manual_review_context_recommendation": _manual_review_recommendation(label),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_rebound_stress_review(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ready = _ready_clusters(cluster_review)
    stress = [row for row in ready if _bool(row.get("stress_detected_any_horizon"))]
    rebound = [row for row in ready if _bool(row.get("rebound_detected_any_horizon"))]
    stress_rate = _rate(len(stress), len(ready))
    rebound_rate = _rate(len(rebound), len(ready))
    ratio = round_float(len(stress) / len(rebound)) if rebound else float(len(stress))
    return clean_for_yaml(
        {
            "stress_cluster_count": len(stress),
            "stress_cluster_rate": stress_rate,
            "rebound_cluster_count": len(rebound),
            "rebound_cluster_rate": rebound_rate,
            "stress_to_rebound_ratio": ratio,
            "stress_rebound_balance_label": _stress_rebound_label(
                stress_rate=stress_rate,
                rebound_rate=rebound_rate,
                ready_count=len(ready),
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_partial_coverage_caveat_report(
    *,
    partial_summary: Mapping[str, Any],
    not_due_impact_report: Mapping[str, Any],
) -> dict[str, Any]:
    coverage_ratio = round_float(partial_summary.get("coverage_ratio"))
    blocked = int(partial_summary.get("blocked_outcome_count") or 0)
    critical = int(partial_summary.get("critical_clusters_with_not_due") or 0)
    materiality = _partial_coverage_materiality(
        coverage_ratio=coverage_ratio,
        blocked=blocked,
        critical=critical,
    )
    return clean_for_yaml(
        {
            "coverage_ratio": coverage_ratio,
            "bound_outcome_count": int(partial_summary.get("bound_outcome_count") or 0),
            "expected_outcome_count": int(partial_summary.get("expected_outcome_count") or 0),
            "not_due_outcome_count": int(partial_summary.get("not_due_outcome_count") or 0),
            "blocked_outcome_count": blocked,
            "critical_clusters_with_not_due": critical,
            "partial_coverage_caveat_required": True,
            "coverage_caveat_materiality": materiality,
            "caveat_label": _partial_caveat_label(materiality),
            "source_not_due_cluster_impact_label": not_due_impact_report.get(
                "not_due_cluster_impact_label", ""
            ),
            "allowed_interpretation": [
                "forward_outcome_review_with_partial_coverage_caveat",
                "cluster_level_research_context",
                "manual_review_only_context",
            ],
            "blocked_interpretation": [
                "final_signal_validity_conclusion",
                "paper_shadow_readiness",
                "production_readiness",
                "broker_action",
                "real_position_instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_monthly_concentration_effect_review(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
    event_logger_monthly_report: Mapping[str, Any],
) -> dict[str, Any]:
    monthly_distribution = _count_by_month(cluster_review)
    max_monthly = max(monthly_distribution.values(), default=0)
    false_rows = [
        row for row in cluster_review if _bool(row.get("false_warning_candidate_any_horizon"))
    ]
    downside_rows = [
        row for row in cluster_review if _bool(row.get("downside_capture_candidate_any_horizon"))
    ]
    missed_rows = [
        row for row in cluster_review if _bool(row.get("missed_upside_candidate_any_horizon"))
    ]
    false_concentration = _month_concentration(false_rows)
    downside_concentration = _month_concentration(downside_rows)
    missed_concentration = _month_concentration(missed_rows)
    inherited_warning = bool(
        event_logger_monthly_report.get("monthly_concentration_warnings")
        or event_logger_monthly_report.get("inherited_2335_warning")
    )
    dominated = (
        bool(cluster_review) and max_monthly / len(cluster_review) >= MONTHLY_DOMINANCE_SHARE
    )
    label = _monthly_concentration_label(
        inherited_warning=inherited_warning,
        dominated=dominated,
        max_share=max(
            false_concentration["max_share"],
            downside_concentration["max_share"],
            missed_concentration["max_share"],
        ),
    )
    return clean_for_yaml(
        {
            "monthly_concentration_warning_inherited": inherited_warning,
            "max_monthly_cluster_count": max_monthly,
            "monthly_cluster_distribution": dict(sorted(monthly_distribution.items())),
            "outcome_dominated_by_single_month": dominated,
            "false_warning_month_concentration": false_concentration,
            "downside_capture_month_concentration": downside_concentration,
            "missed_upside_month_concentration": missed_concentration,
            "monthly_concentration_effect_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_cluster_weighted_evidence_summary(
    *,
    cluster_review: Sequence[Mapping[str, Any]],
    false_warning_review: Mapping[str, Any],
    missed_upside_review: Mapping[str, Any],
    downside_capture_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
) -> dict[str, Any]:
    ready = _ready_clusters(cluster_review)
    total_weight = sum(to_float(row.get("cluster_review_weight")) for row in ready)
    downside_score = _weighted_flag_rate(
        ready, "downside_capture_candidate_any_horizon", total_weight
    )
    false_score = _weighted_flag_rate(ready, "false_warning_candidate_any_horizon", total_weight)
    missed_score = _weighted_flag_rate(ready, "missed_upside_candidate_any_horizon", total_weight)
    manual_score = _weighted_flag_rate(
        ready, "manual_review_would_have_helped_candidate", total_weight
    )
    balance_score = round_float(
        downside_score + (0.5 * manual_score) - (0.5 * false_score) - (0.5 * missed_score)
    )
    label = _evidence_balance_label(
        downside_capture_review=downside_capture_review,
        false_warning_review=false_warning_review,
        missed_upside_review=missed_upside_review,
        manual_review=manual_review,
        monthly_review=monthly_review,
    )
    return clean_for_yaml(
        {
            "primary_analysis_level": "cluster",
            "cluster_count": len(cluster_review),
            "outcome_ready_cluster_count": len(ready),
            "weighted_downside_capture_score": downside_score,
            "weighted_false_warning_score": false_score,
            "weighted_missed_upside_score": missed_score,
            "weighted_manual_review_usefulness_score": manual_score,
            "evidence_balance_score": balance_score,
            "evidence_balance_label": label,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_threshold_rule_outcome_assessment(
    *,
    threshold_selection: Mapping[str, Any],
    false_warning_review: Mapping[str, Any],
    missed_upside_review: Mapping[str, Any],
    downside_capture_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    weighted_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rule = threshold_selection["selected_rule"]
    backtest_context = threshold_selection["backtest_context"]
    false_label = str(false_warning_review.get("false_warning_label", ""))
    missed_label = str(missed_upside_review.get("missed_upside_label", ""))
    downside_label = str(downside_capture_review.get("downside_capture_label", ""))
    rule_label = _rule_outcome_label(
        false_label=false_label,
        missed_label=missed_label,
        downside_label=downside_label,
        monthly_label=str(monthly_review.get("monthly_concentration_effect_label", "")),
    )
    return clean_for_yaml(
        {
            "selected_rule_id": selected_rule.get("selected_rule_id", ""),
            "selected_rule_type": "COMPOSITE_HIGH_INTENSITY_RULE",
            "trigger_density": round_float(
                backtest_context.get("trigger_density_estimate")
                or threshold_selection["caveat_report"].get("trigger_density")
            ),
            "cluster_count": int(weighted_evidence.get("cluster_count") or 0),
            "false_warning_assessment": false_label,
            "missed_upside_assessment": missed_label,
            "downside_capture_assessment": downside_label,
            "manual_review_assessment": manual_review.get("manual_review_usefulness_label", ""),
            "monthly_concentration_assessment": monthly_review.get(
                "monthly_concentration_effect_label", ""
            ),
            "partial_coverage_assessment": partial_caveat.get("caveat_label", ""),
            "rule_outcome_label": rule_label,
            "rule_outcome_recommendation": _rule_outcome_recommendation(
                rule_label=rule_label,
                evidence_label=str(weighted_evidence.get("evidence_balance_label", "")),
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_continue_refine_archive_decision_matrix(
    *,
    source_data_quality: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    false_warning_review: Mapping[str, Any],
    missed_upside_review: Mapping[str, Any],
    downside_capture_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
    selected_rule_assessment: Mapping[str, Any],
) -> dict[str, Any]:
    data_quality_status = str(
        source_data_quality.get("validate_data_status")
        or source_data_quality.get("data_quality_status")
        or ""
    )
    false_label = str(false_warning_review.get("false_warning_label", ""))
    missed_label = str(missed_upside_review.get("missed_upside_label", ""))
    downside_label = str(downside_capture_review.get("downside_capture_label", ""))
    manual_label = str(manual_review.get("manual_review_usefulness_label", ""))
    monthly_label = str(monthly_review.get("monthly_concentration_effect_label", ""))
    caveat_materiality = str(partial_caveat.get("coverage_caveat_materiality", ""))
    if data_quality_status == "FAIL":
        recommendation = "DATA_REMEDIATION_REQUIRED"
        rationale = "source data quality failed"
    elif caveat_materiality in {"HIGH", "BLOCKING"}:
        recommendation = "WAIT_FOR_FULL_20D_COVERAGE"
        rationale = "partial coverage caveat is material"
    elif (
        downside_label in {"DOWNSIDE_CAPTURE_STRONG", "DOWNSIDE_CAPTURE_MODERATE"}
        and not _high_or_blocking(false_label, missed_label)
        and caveat_materiality == "LOW"
    ):
        recommendation = "CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE"
        rationale = (
            "downside capture is moderate/strong while false-warning and missed-upside "
            "labels are not high"
        )
    elif downside_label in {
        "DOWNSIDE_CAPTURE_STRONG",
        "DOWNSIDE_CAPTURE_MODERATE",
        "DOWNSIDE_CAPTURE_WEAK",
    } and (
        _high_or_blocking(false_label, missed_label) or monthly_label == "CONCENTRATION_HIGH_IMPACT"
    ):
        recommendation = "REFINE_HIGH_INTENSITY_THRESHOLD"
        rationale = "some downside capture exists but cost or concentration risk is material"
    elif manual_label in {
        "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY",
        "MANUAL_REVIEW_CONTEXT_MIXED_PROXY",
    }:
        recommendation = "MANUAL_REVIEW_ONLY_CONTINUE"
        rationale = "automatic cap is not supported, but manual-review context has proxy value"
    elif (
        downside_label in {"DOWNSIDE_CAPTURE_WEAK", "DOWNSIDE_CAPTURE_ABSENT"}
        and _high_or_blocking(false_label, missed_label)
        and manual_label == "MANUAL_REVIEW_CONTEXT_WEAK_PROXY"
    ):
        recommendation = "ARCHIVE_HIGH_INTENSITY_RISK_CAP_LINE"
        rationale = "downside capture is weak/absent and warning cost is high"
    else:
        recommendation = "INCONCLUSIVE"
        rationale = "evidence balance is not strong enough for continue/refine/archive routing"
    next_task = _next_task_for_recommendation(recommendation)
    review_status = (
        "FORWARD_OUTCOME_REVIEW_COMPLETE_WITH_PARTIAL_COVERAGE_CAVEAT"
        if recommendation not in {"DATA_REMEDIATION_REQUIRED", "WAIT_FOR_FULL_20D_COVERAGE"}
        else "FORWARD_OUTCOME_REVIEW_BLOCKED_BY_COVERAGE"
        if recommendation == "WAIT_FOR_FULL_20D_COVERAGE"
        else "FORWARD_OUTCOME_REVIEW_BLOCKED_BY_DATA_QUALITY"
    )
    return clean_for_yaml(
        {
            "review_status": review_status,
            "data_quality_status": data_quality_status,
            "partial_coverage_status": partial_caveat.get("caveat_label", ""),
            "false_warning_assessment": false_label,
            "missed_upside_assessment": missed_label,
            "downside_capture_assessment": downside_label,
            "manual_review_usefulness_assessment": manual_label,
            "monthly_concentration_assessment": monthly_label,
            "selected_rule_assessment": selected_rule_assessment.get("rule_outcome_label", ""),
            "overall_recommendation": recommendation,
            "next_task_recommendation": next_task,
            "decision_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2341_readiness_checklist(
    *,
    decision_matrix: Mapping[str, Any],
    source_data_quality: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings = ["PARTIAL_COVERAGE_CAVEAT_REQUIRED"]
    data_quality_status = str(
        source_data_quality.get("validate_data_status")
        or source_data_quality.get("data_quality_status")
        or ""
    )
    if data_quality_status == "FAIL":
        blockers.append("SOURCE_DATA_QUALITY_FAIL")
    if partial_caveat.get("coverage_caveat_materiality") in {"HIGH", "BLOCKING"}:
        warnings.append("FULL_20D_COVERAGE_NOT_READY")
    recommendation = str(decision_matrix.get("overall_recommendation", ""))
    if blockers:
        readiness_status = "DATA_REMEDIATION_REQUIRED"
    elif recommendation == "WAIT_FOR_FULL_20D_COVERAGE":
        readiness_status = "WAIT_FOR_FULL_COVERAGE_BEFORE_2341"
    else:
        readiness_status = "READY_FOR_2341_WITH_PARTIAL_COVERAGE_CAVEAT"
    return clean_for_yaml(
        {
            "forward_outcome_review_summary_generated": True,
            "cluster_outcome_review_generated": True,
            "horizon_outcome_review_generated": True,
            "false_warning_review_generated": True,
            "missed_upside_review_generated": True,
            "downside_capture_review_generated": True,
            "manual_review_usefulness_review_generated": True,
            "partial_coverage_caveat_report_generated": True,
            "decision_matrix_generated": True,
            "data_quality_passed": data_quality_status in {"PASS", "PASS_WITH_WARNINGS"},
            "safety_boundary_passed": True,
            "outcome_binding_executed": False,
            "original_event_log_mutated": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": readiness_status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
        }
    )


def build_high_intensity_2341_task_route(decision_matrix: Mapping[str, Any]) -> dict[str, Any]:
    next_task = str(decision_matrix.get("next_task_recommendation", ""))
    return clean_for_yaml(
        {
            "allowed_routes": [
                NEXT_2341_CONTINUE_TASK,
                NEXT_2341_REFINE_TASK,
                NEXT_2341_MANUAL_TASK,
                NEXT_2341_WAIT_TASK,
                NEXT_2341_ARCHIVE_TASK,
                NEXT_2341_DATA_TASK,
            ],
            "next_task": next_task,
            "overall_recommendation": decision_matrix.get("overall_recommendation", ""),
            "review_status": decision_matrix.get("review_status", ""),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_outcome_interpretation_boundary(
    *,
    generated_at: datetime,
    source_data_quality: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "source_validate_data_executed": source_data_quality.get(
                "validate_data_executed", True
            ),
            "source_validate_data_as_of": source_data_quality.get("validate_data_as_of", ""),
            "source_validate_data_status": source_data_quality.get("validate_data_status", ""),
            "forbidden_interpretations": [
                "real_account_performance",
                "real_position_instruction",
                "reduce_position_signal",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_outcome_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task", ""),
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
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_outcome_review_summary(
    *,
    generated_at: datetime,
    partial_readiness_dir: Path,
    outcome_binder_dir: Path,
    event_logger_dir: Path,
    threshold_selection_dir: Path,
    forward_observe_plan_dir: Path,
    partial_summary: Mapping[str, Any],
    source_summary: Mapping[str, Any],
    source_data_quality: Mapping[str, Any],
    trigger_context: Sequence[Mapping[str, Any]],
    cluster_review: Sequence[Mapping[str, Any]],
    horizon_review: Sequence[Mapping[str, Any]],
    false_warning_review: Mapping[str, Any],
    missed_upside_review: Mapping[str, Any],
    downside_capture_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
    weighted_evidence: Mapping[str, Any],
    threshold_assessment: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Forward Outcome Review",
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "status": decision_matrix.get("review_status", ""),
            "review_status": decision_matrix.get("review_status", ""),
            "overall_recommendation": decision_matrix.get("overall_recommendation", ""),
            "next_task": task_route.get("next_task", ""),
            "partial_readiness_dir": str(partial_readiness_dir),
            "outcome_binder_dir": str(outcome_binder_dir),
            "event_logger_dir": str(event_logger_dir),
            "threshold_selection_dir": str(threshold_selection_dir),
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "not rerun because TRADING-2340 only reads prior validated "
                "TRADING-2337 / TRADING-2339 artifacts and does not consume market data directly"
            ),
            "source_validate_data_executed": source_summary.get("validate_data_executed", True),
            "source_validate_data_as_of": source_summary.get("validate_data_as_of", ""),
            "source_validate_data_status": source_summary.get("validate_data_status", ""),
            "source_validate_data_error_count": int(
                source_summary.get("validate_data_error_count") or 0
            ),
            "source_data_quality_status": source_data_quality.get(
                "validate_data_status", source_data_quality.get("data_quality_status", "")
            ),
            "partial_2339_status": partial_summary.get("status", ""),
            "partial_coverage_ratio": partial_summary.get("coverage_ratio", 0.0),
            "bound_outcome_count": partial_summary.get("bound_outcome_count", 0),
            "expected_outcome_count": partial_summary.get("expected_outcome_count", 0),
            "not_due_outcome_count": partial_summary.get("not_due_outcome_count", 0),
            "critical_clusters_with_not_due": partial_summary.get(
                "critical_clusters_with_not_due", 0
            ),
            "primary_analysis_level": "cluster",
            "trigger_day_level_usage": "context_only",
            "trigger_day_context_count": len(trigger_context),
            "cluster_count": len(cluster_review),
            "horizon_review_row_count": len(horizon_review),
            "false_warning_cluster_rate": false_warning_review.get(
                "false_warning_cluster_rate", 0.0
            ),
            "missed_upside_cluster_rate": missed_upside_review.get(
                "missed_upside_cluster_rate", 0.0
            ),
            "downside_capture_cluster_rate": downside_capture_review.get(
                "downside_capture_cluster_rate", 0.0
            ),
            "manual_review_usefulness_proxy": manual_review.get(
                "manual_review_usefulness_proxy", 0.0
            ),
            "partial_coverage_caveat_label": partial_caveat.get("caveat_label", ""),
            "monthly_concentration_effect_label": monthly_review.get(
                "monthly_concentration_effect_label", ""
            ),
            "evidence_balance_label": weighted_evidence.get("evidence_balance_label", ""),
            "rule_outcome_label": threshold_assessment.get("rule_outcome_label", ""),
            "rule_outcome_recommendation": threshold_assessment.get(
                "rule_outcome_recommendation", ""
            ),
            "readiness_status": readiness.get("readiness_status", ""),
            "2341_readiness_checklist_generated": True,
            "2341_task_route_generated": True,
            "cluster_outcome_review_matrix_generated": True,
            "horizon_outcome_review_matrix_generated": True,
            "false_warning_review_generated": True,
            "missed_upside_review_generated": True,
            "downside_capture_review_generated": True,
            "manual_review_usefulness_review_generated": True,
            "partial_coverage_caveat_report_generated": True,
            "monthly_concentration_effect_review_generated": True,
            "cluster_weighted_evidence_summary_generated": True,
            "threshold_rule_outcome_assessment_generated": True,
            "continue_refine_archive_decision_matrix_generated": True,
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_forward_outcome_review_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    cluster_review: Sequence[Mapping[str, Any]],
    horizon_review: Sequence[Mapping[str, Any]],
    false_warning_review: Mapping[str, Any],
    missed_upside_review: Mapping[str, Any],
    downside_capture_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    rebound_stress_review: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
    weighted_evidence: Mapping[str, Any],
    threshold_assessment: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    _write_rows_artifacts(
        paths["cluster_matrix_json"],
        paths["cluster_matrix_csv"],
        f"{REPORT_TYPE}.cluster_outcome_review_matrix.v1",
        cluster_review,
    )
    _write_rows_artifacts(
        paths["horizon_matrix_json"],
        paths["horizon_matrix_csv"],
        f"{REPORT_TYPE}.horizon_outcome_review_matrix.v1",
        horizon_review,
    )
    write_json(paths["false_warning_review"], false_warning_review)
    write_json(paths["missed_upside_review"], missed_upside_review)
    write_json(paths["downside_capture_review"], downside_capture_review)
    write_json(paths["manual_review"], manual_review)
    write_json(paths["rebound_stress_review"], rebound_stress_review)
    write_json(paths["partial_caveat"], partial_caveat)
    write_json(paths["monthly_review"], monthly_review)
    write_json(paths["weighted_evidence"], weighted_evidence)
    write_json(paths["threshold_assessment"], threshold_assessment)
    write_json(paths["decision_matrix"], decision_matrix)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], _render_main_doc(summary, decision_matrix))
    write_markdown(paths["cluster_doc"], _render_cluster_doc(summary, cluster_review))
    write_markdown(
        paths["false_missed_doc"],
        _render_false_missed_doc(false_warning_review, missed_upside_review, partial_caveat),
    )
    write_markdown(
        paths["downside_manual_doc"],
        _render_downside_manual_doc(downside_capture_review, manual_review),
    )
    write_markdown(paths["decision_doc"], _render_decision_doc(decision_matrix, task_route))
    return {key: str(path) for key, path in paths.items()}


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_forward_outcome_review_summary.json",
        "cluster_matrix_json": output_dir / "high_intensity_cluster_outcome_review_matrix.json",
        "cluster_matrix_csv": output_dir / "high_intensity_cluster_outcome_review_matrix.csv",
        "horizon_matrix_json": output_dir / "high_intensity_horizon_outcome_review_matrix.json",
        "horizon_matrix_csv": output_dir / "high_intensity_horizon_outcome_review_matrix.csv",
        "false_warning_review": output_dir / "high_intensity_false_warning_review.json",
        "missed_upside_review": output_dir / "high_intensity_missed_upside_review.json",
        "downside_capture_review": output_dir / "high_intensity_downside_capture_review.json",
        "manual_review": output_dir / "high_intensity_manual_review_usefulness_review.json",
        "rebound_stress_review": output_dir / "high_intensity_rebound_stress_review.json",
        "partial_caveat": output_dir / "high_intensity_partial_coverage_caveat_report.json",
        "monthly_review": output_dir / "high_intensity_monthly_concentration_effect_review.json",
        "weighted_evidence": output_dir / "high_intensity_cluster_weighted_evidence_summary.json",
        "threshold_assessment": output_dir
        / "high_intensity_threshold_rule_outcome_assessment.json",
        "decision_matrix": output_dir
        / "high_intensity_continue_refine_archive_decision_matrix.json",
        "readiness": output_dir / "high_intensity_2341_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2341_task_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_forward_outcome_interpretation_boundary.json",
        "safety_boundary": output_dir / "high_intensity_forward_outcome_safety_boundary.json",
        "main_doc": docs_root / "high_intensity_risk_cap_forward_outcome_review.md",
        "cluster_doc": docs_root / "high_intensity_cluster_outcome_review.md",
        "false_missed_doc": docs_root / "high_intensity_false_warning_missed_upside_review.md",
        "downside_manual_doc": docs_root
        / "high_intensity_downside_capture_manual_review_usefulness.md",
        "decision_doc": docs_root / "high_intensity_continue_refine_archive_decision.md",
    }


def _write_rows_artifacts(
    json_path: Path,
    csv_path: Path,
    schema_version: str,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    write_json(
        json_path,
        {
            "schema_version": schema_version,
            "task_id": TASK_ID,
            "row_count": len(rows),
            "rows": list(rows),
            **SAFETY_FIELDS,
        },
    )
    write_csv_rows(csv_path, rows)


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityForwardOutcomeReviewError(f"{label} missing {key}: {path}")
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityForwardOutcomeReviewError(f"{path}: expected JSON object")
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensityForwardOutcomeReviewError(
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


def _is_bound_row(row: Mapping[str, Any]) -> bool:
    return str(row.get("cluster_outcome_binding_status") or row.get("outcome_binding_status")) == (
        "OUTCOME_BOUND"
    )


def _is_not_due_row(row: Mapping[str, Any]) -> bool:
    status = str(row.get("cluster_outcome_binding_status") or row.get("outcome_binding_status"))
    due_status = str(row.get("outcome_due_status", "")).upper()
    return status == "OUTCOME_NOT_DUE" or due_status == "NOT_DUE"


def _horizon_status(row: Mapping[str, Any]) -> str:
    return str(
        row.get("cluster_outcome_binding_status")
        or row.get("outcome_binding_status")
        or "OUTCOME_MISSING"
    )


def _horizon_float(
    horizon_by_id: Mapping[str, Mapping[str, Any]],
    horizon: str,
    key: str,
) -> float | None:
    row = horizon_by_id.get(horizon, {})
    if not row or not _is_bound_row(row):
        return None
    return round_float(row.get(key))


def _first_mapping(
    rows: Sequence[Mapping[str, Any]], fallback: Mapping[str, Any]
) -> dict[str, Any]:
    merged = dict(fallback)
    if rows:
        merged.update(dict(rows[0]))
    return merged


def _cluster_sort_key(rows: Sequence[Mapping[str, Any]]) -> tuple[str, str, str]:
    row = rows[0] if rows else {}
    return (
        str(row.get("cluster_start_date", "")),
        str(row.get("target_asset", "")),
        str(row.get("event_cluster_id", "")),
    )


def _cluster_evidence_label(
    *,
    downside_any: bool,
    false_any: bool,
    missed_any: bool,
    rebound_any: bool,
    stress_any: bool,
    not_due_count: int,
) -> str:
    signal_count = sum([downside_any, false_any, missed_any])
    if signal_count > 1:
        return "MIXED_OUTCOME"
    if downside_any:
        return "DOWNSIDE_CAPTURE_SUPPORTIVE"
    if false_any:
        return "FALSE_WARNING_DOMINANT"
    if missed_any:
        return "MISSED_UPSIDE_DOMINANT"
    if not_due_count > 0:
        return "PARTIAL_HORIZON_INCOMPLETE"
    if rebound_any or stress_any:
        return "MIXED_OUTCOME"
    return "NO_MATERIAL_OUTCOME"


def _horizon_evidence_label(
    *,
    coverage_ratio: float,
    downside_rate: float,
    false_warning_rate: float,
    missed_upside_rate: float,
    bound_count: int,
) -> str:
    if bound_count <= 0:
        return "HORIZON_INCONCLUSIVE"
    if coverage_ratio < PARTIAL_COVERAGE_ACCEPTABLE_RATIO:
        return "HORIZON_INCOMPLETE"
    if false_warning_rate >= FALSE_WARNING_HIGH_RATE:
        return "HORIZON_FALSE_WARNING_HIGH"
    if missed_upside_rate >= MISSED_UPSIDE_HIGH_RATE:
        return "HORIZON_MISSED_UPSIDE_HIGH"
    if downside_rate >= DOWNSIDE_CAPTURE_MODERATE_RATE:
        return "HORIZON_SUPPORTS_WARNING_VALUE"
    if downside_rate > 0 or false_warning_rate > 0 or missed_upside_rate > 0:
        return "HORIZON_MIXED"
    return "HORIZON_INCONCLUSIVE"


def _false_warning_label(rate: float, has_ready: bool) -> str:
    if not has_ready:
        return "FALSE_WARNING_INCONCLUSIVE"
    if rate >= FALSE_WARNING_BLOCKING_RATE:
        return "FALSE_WARNING_BLOCKING"
    if rate >= FALSE_WARNING_HIGH_RATE:
        return "FALSE_WARNING_HIGH"
    if rate >= FALSE_WARNING_MODERATE_RATE:
        return "FALSE_WARNING_MODERATE"
    return "FALSE_WARNING_ACCEPTABLE"


def _missed_upside_label(rate: float, has_ready: bool) -> str:
    if not has_ready:
        return "MISSED_UPSIDE_INCONCLUSIVE"
    if rate >= MISSED_UPSIDE_BLOCKING_RATE:
        return "MISSED_UPSIDE_BLOCKING"
    if rate >= MISSED_UPSIDE_HIGH_RATE:
        return "MISSED_UPSIDE_HIGH"
    if rate >= MISSED_UPSIDE_MODERATE_RATE:
        return "MISSED_UPSIDE_MODERATE"
    return "MISSED_UPSIDE_ACCEPTABLE"


def _downside_capture_label(rate: float, has_ready: bool) -> str:
    if not has_ready:
        return "DOWNSIDE_CAPTURE_INCONCLUSIVE"
    if rate >= DOWNSIDE_CAPTURE_STRONG_RATE:
        return "DOWNSIDE_CAPTURE_STRONG"
    if rate >= DOWNSIDE_CAPTURE_MODERATE_RATE:
        return "DOWNSIDE_CAPTURE_MODERATE"
    if rate >= DOWNSIDE_CAPTURE_WEAK_RATE:
        return "DOWNSIDE_CAPTURE_WEAK"
    return "DOWNSIDE_CAPTURE_ABSENT"


def _manual_review_label(proxy: float, has_ready: bool) -> str:
    if not has_ready:
        return "MANUAL_REVIEW_CONTEXT_INCONCLUSIVE"
    if proxy >= MANUAL_REVIEW_USEFUL_RATE:
        return "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY"
    if proxy >= MANUAL_REVIEW_MIXED_RATE:
        return "MANUAL_REVIEW_CONTEXT_MIXED_PROXY"
    return "MANUAL_REVIEW_CONTEXT_WEAK_PROXY"


def _manual_review_recommendation(label: str) -> str:
    if label == "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY":
        return "KEEP_MANUAL_REVIEW_CONTEXT_ONLY"
    if label == "MANUAL_REVIEW_CONTEXT_MIXED_PROXY":
        return "REFINE_MANUAL_REVIEW_CONTEXT"
    if label == "MANUAL_REVIEW_CONTEXT_WEAK_PROXY":
        return "DROP_MANUAL_REVIEW_CONTEXT"
    return "INCONCLUSIVE"


def _stress_rebound_label(*, stress_rate: float, rebound_rate: float, ready_count: int) -> str:
    if ready_count <= 0:
        return "INCONCLUSIVE"
    if stress_rate == 0 and rebound_rate == 0:
        return "NO_MATERIAL_FOLLOW_THROUGH"
    if stress_rate >= rebound_rate * 1.25:
        return "STRESS_DOMINANT_AFTER_WARNING"
    if rebound_rate >= stress_rate * 1.25:
        return "REBOUND_DOMINANT_AFTER_WARNING"
    return "MIXED_STRESS_REBOUND"


def _partial_coverage_materiality(*, coverage_ratio: float, blocked: int, critical: int) -> str:
    if blocked > 0 or critical > 0:
        return "BLOCKING"
    if coverage_ratio < PARTIAL_COVERAGE_ACCEPTABLE_RATIO:
        return "HIGH"
    if coverage_ratio < PARTIAL_COVERAGE_HIGH_RATIO:
        return "MODERATE"
    return "LOW"


def _partial_caveat_label(materiality: str) -> str:
    return {
        "LOW": "PARTIAL_COVERAGE_LOW_IMPACT",
        "MODERATE": "PARTIAL_COVERAGE_MODERATE_IMPACT",
        "HIGH": "PARTIAL_COVERAGE_HIGH_IMPACT",
        "BLOCKING": "PARTIAL_COVERAGE_BLOCKING",
    }.get(materiality, "PARTIAL_COVERAGE_BLOCKING")


def _monthly_concentration_label(
    *,
    inherited_warning: bool,
    dominated: bool,
    max_share: float,
) -> str:
    if max_share >= MONTHLY_HIGH_CONCENTRATION_SHARE:
        return "CONCENTRATION_HIGH_IMPACT"
    if dominated:
        return "CONCENTRATION_HIGH_IMPACT"
    if inherited_warning or max_share >= MONTHLY_MODERATE_CONCENTRATION_SHARE:
        return "CONCENTRATION_MODERATE_IMPACT"
    return "CONCENTRATION_LOW_IMPACT"


def _evidence_balance_label(
    *,
    downside_capture_review: Mapping[str, Any],
    false_warning_review: Mapping[str, Any],
    missed_upside_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    monthly_review: Mapping[str, Any],
) -> str:
    false_label = str(false_warning_review.get("false_warning_label", ""))
    missed_label = str(missed_upside_review.get("missed_upside_label", ""))
    downside_label = str(downside_capture_review.get("downside_capture_label", ""))
    manual_label = str(manual_review.get("manual_review_usefulness_label", ""))
    monthly_label = str(monthly_review.get("monthly_concentration_effect_label", ""))
    if downside_label in {
        "DOWNSIDE_CAPTURE_STRONG",
        "DOWNSIDE_CAPTURE_MODERATE",
    } and not _high_or_blocking(false_label, missed_label):
        return "EVIDENCE_SUPPORTS_CONTINUE_OBSERVE"
    if downside_label in {
        "DOWNSIDE_CAPTURE_STRONG",
        "DOWNSIDE_CAPTURE_MODERATE",
        "DOWNSIDE_CAPTURE_WEAK",
    } and (
        _high_or_blocking(false_label, missed_label) or monthly_label == "CONCENTRATION_HIGH_IMPACT"
    ):
        return "EVIDENCE_SUPPORTS_THRESHOLD_REFINEMENT"
    if manual_label in {"MANUAL_REVIEW_CONTEXT_USEFUL_PROXY", "MANUAL_REVIEW_CONTEXT_MIXED_PROXY"}:
        return "EVIDENCE_SUPPORTS_MANUAL_REVIEW_ONLY"
    if downside_label in {"DOWNSIDE_CAPTURE_ABSENT", "DOWNSIDE_CAPTURE_WEAK"} and _high_or_blocking(
        false_label, missed_label
    ):
        return "EVIDENCE_SUPPORTS_ARCHIVE"
    return "EVIDENCE_INCONCLUSIVE"


def _rule_outcome_label(
    *,
    false_label: str,
    missed_label: str,
    downside_label: str,
    monthly_label: str,
) -> str:
    if false_label == "FALSE_WARNING_BLOCKING":
        return "RULE_TOO_FALSE_WARNING_PRONE"
    if missed_label == "MISSED_UPSIDE_BLOCKING":
        return "RULE_TOO_MISSED_UPSIDE_PRONE"
    if downside_label in {
        "DOWNSIDE_CAPTURE_STRONG",
        "DOWNSIDE_CAPTURE_MODERATE",
    } and not _high_or_blocking(false_label, missed_label):
        return "RULE_SHOWS_FORWARD_OBSERVE_VALUE"
    if downside_label in {"DOWNSIDE_CAPTURE_WEAK", "DOWNSIDE_CAPTURE_ABSENT"}:
        return "RULE_NO_DOWNSIDE_CAPTURE"
    if monthly_label == "CONCENTRATION_HIGH_IMPACT":
        return "RULE_NEEDS_STRICTER_THRESHOLD"
    return "RULE_INCONCLUSIVE"


def _rule_outcome_recommendation(*, rule_label: str, evidence_label: str) -> str:
    if rule_label == "RULE_SHOWS_FORWARD_OBSERVE_VALUE":
        return "CONTINUE_SELECTED_RULE_FORWARD_OBSERVE"
    if rule_label in {"RULE_NEEDS_STRICTER_THRESHOLD", "RULE_TOO_FALSE_WARNING_PRONE"}:
        return "REFINE_TO_STRICTER_COMPOSITE_RULE"
    if rule_label == "RULE_TOO_MISSED_UPSIDE_PRONE":
        return "REFINE_THRESHOLD_DENSITY"
    if evidence_label == "EVIDENCE_SUPPORTS_MANUAL_REVIEW_ONLY":
        return "MANUAL_REVIEW_ONLY_KEEP_NO_RUNTIME"
    if rule_label == "RULE_NO_DOWNSIDE_CAPTURE":
        return "ARCHIVE_SELECTED_RULE"
    return "INCONCLUSIVE_WAIT_FOR_FULL_COVERAGE"


def _next_task_for_recommendation(recommendation: str) -> str:
    return {
        "CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE": NEXT_2341_CONTINUE_TASK,
        "REFINE_HIGH_INTENSITY_THRESHOLD": NEXT_2341_REFINE_TASK,
        "MANUAL_REVIEW_ONLY_CONTINUE": NEXT_2341_MANUAL_TASK,
        "WAIT_FOR_FULL_20D_COVERAGE": NEXT_2341_WAIT_TASK,
        "ARCHIVE_HIGH_INTENSITY_RISK_CAP_LINE": NEXT_2341_ARCHIVE_TASK,
        "DATA_REMEDIATION_REQUIRED": NEXT_2341_DATA_TASK,
    }.get(recommendation, NEXT_2341_MANUAL_TASK)


def _ready_clusters(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [row for row in rows if int(row.get("bound_horizon_count") or 0) > 0]


def _count_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key, "")) for row in rows if row.get(key)).items()))


def _count_by_month(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return dict(
        sorted(Counter(_month_of(row.get("cluster_start_date", "")) for row in rows).items())
    )


def _month_concentration(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = _count_by_month(rows)
    if not counts:
        return {"max_month": "", "max_count": 0, "max_share": 0.0, "distribution": {}}
    max_month, max_count = max(counts.items(), key=lambda item: item[1])
    return {
        "max_month": max_month,
        "max_count": max_count,
        "max_share": round_float(max_count / len(rows)),
        "distribution": counts,
    }


def _month_of(value: object) -> str:
    text = str(value)
    return text[:7] if len(text) >= 7 else ""


def _cluster_return_values(row: Mapping[str, Any]) -> list[float]:
    return [
        to_float(row.get(f"cluster_forward_return_{horizon}"))
        for horizon in OUTCOME_HORIZONS
        if row.get(f"cluster_forward_return_{horizon}") is not None
    ]


def _cluster_drawdown_values(row: Mapping[str, Any]) -> list[float]:
    return [
        to_float(row.get(f"cluster_max_drawdown_{horizon}"))
        for horizon in OUTCOME_HORIZONS
        if row.get(f"cluster_max_drawdown_{horizon}") is not None
    ]


def _average(values: Sequence[float]) -> float:
    return round_float(sum(values) / len(values)) if values else 0.0


def _rate(numerator: int, denominator: int) -> float:
    return round_float(numerator / denominator) if denominator else 0.0


def _weighted_flag_rate(rows: Sequence[Mapping[str, Any]], key: str, total_weight: float) -> float:
    if total_weight <= 0:
        return 0.0
    value = sum(to_float(row.get("cluster_review_weight")) for row in rows if _bool(row.get(key)))
    return round_float(value / total_weight)


def _materiality_from_label(label: str) -> str:
    if label.endswith("BLOCKING"):
        return "BLOCKING"
    if label.endswith("HIGH"):
        return "HIGH"
    if label.endswith("MODERATE"):
        return "MODERATE"
    if label.endswith("ACCEPTABLE"):
        return "LOW"
    return "INCONCLUSIVE"


def _downside_materiality(label: str) -> str:
    return {
        "DOWNSIDE_CAPTURE_STRONG": "HIGH",
        "DOWNSIDE_CAPTURE_MODERATE": "MODERATE",
        "DOWNSIDE_CAPTURE_WEAK": "LOW",
        "DOWNSIDE_CAPTURE_ABSENT": "ABSENT",
    }.get(label, "INCONCLUSIVE")


def _high_or_blocking(false_label: str, missed_label: str) -> bool:
    return false_label in {"FALSE_WARNING_HIGH", "FALSE_WARNING_BLOCKING"} or missed_label in {
        "MISSED_UPSIDE_HIGH",
        "MISSED_UPSIDE_BLOCKING",
    }


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _truthy(value: object) -> bool:
    return _bool(value)


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


def _render_main_doc(summary: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
    return (
        "# High-Intensity Risk-Cap Forward Outcome Review\n\n"
        f"- status: `{summary.get('status')}`\n"
        f"- coverage: `{summary.get('bound_outcome_count')}/"
        f"{summary.get('expected_outcome_count')}`\n"
        f"- false_warning_cluster_rate: `{summary.get('false_warning_cluster_rate')}`\n"
        f"- missed_upside_cluster_rate: `{summary.get('missed_upside_cluster_rate')}`\n"
        f"- downside_capture_cluster_rate: `{summary.get('downside_capture_cluster_rate')}`\n"
        f"- manual_review_usefulness_proxy: `{summary.get('manual_review_usefulness_proxy')}`\n"
        f"- overall_recommendation: `{decision.get('overall_recommendation')}`\n"
        f"- next_task: `{decision.get('next_task_recommendation')}`\n\n"
        "本报告只做 forward outcome review；不重新绑定 outcome，不读取 market data，"
        "不输出 target weight / rebalance / broker action。\n"
    )


def _render_cluster_doc(summary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> str:
    labels = Counter(str(row.get("cluster_evidence_label", "")) for row in rows)
    lines = [
        "# High-Intensity Cluster Outcome Review",
        "",
        f"主分析单位为 cluster；cluster_count=`{summary.get('cluster_count')}`。",
        "",
        "|cluster_evidence_label|count|",
        "|---|---:|",
    ]
    lines.extend(f"|`{label}`|{count}|" for label, count in sorted(labels.items()))
    return "\n".join(lines) + "\n"


def _render_false_missed_doc(
    false_review: Mapping[str, Any],
    missed_review: Mapping[str, Any],
    partial_caveat: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity False Warning / Missed Upside Review\n\n"
        f"- false_warning_label: `{false_review.get('false_warning_label')}`\n"
        f"- false_warning_cluster_rate: `{false_review.get('false_warning_cluster_rate')}`\n"
        f"- missed_upside_label: `{missed_review.get('missed_upside_label')}`\n"
        f"- missed_upside_cluster_rate: `{missed_review.get('missed_upside_cluster_rate')}`\n"
        f"- partial_coverage_caveat: `{partial_caveat.get('caveat_label')}`\n\n"
        "Missed upside 是 would-have-cost context，不是实际交易损失。\n"
    )


def _render_downside_manual_doc(
    downside_review: Mapping[str, Any],
    manual_review: Mapping[str, Any],
) -> str:
    return (
        "# High-Intensity Downside Capture / Manual Review Usefulness\n\n"
        f"- downside_capture_label: `{downside_review.get('downside_capture_label')}`\n"
        f"- downside_capture_cluster_rate: "
        f"`{downside_review.get('downside_capture_cluster_rate')}`\n"
        f"- worst_forward_max_drawdown_after_warning: "
        f"`{downside_review.get('worst_forward_max_drawdown_after_warning')}`\n"
        f"- manual_review_usefulness_label: "
        f"`{manual_review.get('manual_review_usefulness_label')}`\n"
        f"- manual_review_context_recommendation: "
        f"`{manual_review.get('manual_review_context_recommendation')}`\n"
    )


def _render_decision_doc(decision: Mapping[str, Any], route: Mapping[str, Any]) -> str:
    return (
        "# High-Intensity Continue / Refine / Archive Decision\n\n"
        f"- review_status: `{decision.get('review_status')}`\n"
        f"- overall_recommendation: `{decision.get('overall_recommendation')}`\n"
        f"- next_task: `{route.get('next_task')}`\n"
        f"- rationale: {decision.get('decision_rationale')}\n\n"
        "Promotion、paper-shadow、production 和 broker action 全部保持关闭。\n"
    )
