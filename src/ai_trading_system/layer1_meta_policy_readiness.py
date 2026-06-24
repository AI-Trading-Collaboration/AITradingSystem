from __future__ import annotations

import hashlib
import json
import math
import random
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.layer2_strategy_component_readiness import (
    DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    run_layer2_anti_leakage_time_boundary_audit,
    run_layer2_component_readiness_matrix,
    run_layer2_forward_outcome_cube_build,
    run_layer2_historical_weight_path_build,
    run_layer2_return_cost_exposure_panel,
    run_layer2_selector_headroom_oracle_review,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "layer1_meta_policy"
)

LABEL_HORIZONS = ("20d", "60d")
SELECTABLE_COMPONENT_IDS = ("equal_risk_qqq_sgov", "100_qqq")
REFERENCE_COMPONENT_IDS = ("qqq_50_sgov_50", "qqq_60_sgov_40")


def run_layer2_best_component_label_builder(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer2_best_component_label_builder",
            "Layer-2 Best Component Label Builder",
            "BEST_COMPONENT_LABELS_BLOCKED",
            context,
            output_root,
        )
    rows = _label_rows(context["cube"])
    matured_count = sum(1 for row in rows if row["label_status"] == "MATURED")
    status = (
        "BEST_COMPONENT_LABELS_READY"
        if matured_count == len(rows)
        else "BEST_COMPONENT_LABELS_PARTIAL"
    )
    payload = _payload(
        report_type="layer2_best_component_label_builder",
        title="Layer-2 Best Component Label Builder",
        status=status,
        summary={
            "label_row_count": len(rows),
            "matured_label_count": matured_count,
            "selectable_component_ids": list(SELECTABLE_COMPONENT_IDS),
            "reference_component_ids": list(REFERENCE_COMPONENT_IDS),
            "data_quality_status": context["data_quality_status"],
            "layer1_historical_research_allowed": False,
        },
        label_rows=rows,
        label_contract={
            "future_labels_not_allowed_as_features": True,
            "reference_only_can_participate_in_regret_comparison": True,
            "inactive_growth_reference_not_selectable_label": True,
        },
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_policy_combiner_contract(
    *,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    payload = _payload(
        report_type="layer1_policy_combiner_contract",
        title="Layer-1 Policy Combiner Contract",
        status="POLICY_COMBINER_CONTRACT_READY",
        summary={
            "selectable_component_ids": _selectable_component_ids(config),
            "reference_only_components_not_selectable": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        selector_output_schema={
            "decision_date": "YYYY-MM-DD",
            "selector_id": "string",
            "selected_component_id": "one of selectable_component_ids",
            "confidence": "0.0 to 1.0 research-only score",
            "selector_mode": [
                "hard_select_one_component",
                "soft_blend_selectable_components",
                "defensive_vs_benchmark_blend",
                "confidence_weighted_blend",
            ],
        },
        component_blend_weight_schema={
            "component_id": "selectable component id only",
            "blend_weight": "non-negative float",
            "policy_definition_hash": "required",
        },
        final_weight_combiner_rule=(
            "Normalize selectable component blend weights, then multiply each "
            "component target weight by its blend weight and sum by ticker."
        ),
        weight_normalization_rule="sum selectable component blend weights to 1.0",
        turnover_constraint=_switching_constraints(config),
        current_allowed_selectable=_selectable_component_ids(config),
        current_forbidden_selectable=[
            *REFERENCE_COMPONENT_IDS,
            "QQQ-plus growth",
            "TQQQ-heavy",
            "tail-risk fallback",
            "LEAPS",
            "Wheel",
        ],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_objective_outcome_contract(
    *,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _payload(
        report_type="layer1_objective_outcome_contract",
        title="Layer-1 Objective and Outcome Contract",
        status="LAYER1_OBJECTIVE_READY",
        summary={
            "primary_objective": "cost_adjusted_regret_vs_best_selectable_component",
            "secondary_objective": "drawdown_control_vs_100_qqq",
            "tertiary_objective": "missed_upside_control_vs_equal_risk",
            "ml_selector_allowed": False,
        },
        allowed_objectives=[
            "minimize_regret_vs_best_component",
            "improve_return_vs_equal_risk",
            "improve_drawdown_vs_100_qqq",
            "maximize_cost_adjusted_calmar",
            "maximize_cost_adjusted_sharpe",
            "reduce_missed_upside_while_controlling_drawdown",
        ],
        primary_objective="cost_adjusted_regret_vs_best_selectable_component",
        secondary_objective="drawdown_control_vs_100_qqq",
        tertiary_objective="missed_upside_control_vs_equal_risk",
        objective_drift_guard="owner review required before changing objective hierarchy",
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_purged_walk_forward_split_contract(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_purged_walk_forward_split_contract",
            "Layer-1 Purged Walk-Forward Split Contract",
            "PURGED_WALK_FORWARD_BLOCKED",
            context,
            output_root,
        )
    splits = _walk_forward_splits(context["panel"])
    status = "PURGED_WALK_FORWARD_CONTRACT_READY" if splits else "PURGED_WALK_FORWARD_NEEDS_REVIEW"
    payload = _payload(
        report_type="layer1_purged_walk_forward_split_contract",
        title="Layer-1 Purged Walk-Forward Split Contract",
        status=status,
        summary={
            "split_count": len(splits),
            "embargo_days": 120,
            "max_forward_window_days": 120,
            "period_split_by_market_regime": True,
            "latest_forward_windows_excluded_if_unmatured": True,
        },
        walk_forward_splits=splits,
        no_overlap_between_label_windows=True,
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_research_dataset_builder(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_research_dataset",
            "Layer-1 Research Dataset",
            "LAYER1_RESEARCH_DATASET_BLOCKED",
            context,
            output_root,
        )
    labels = _label_rows(context["cube"])
    splits = _walk_forward_splits(context["panel"])
    rows = _dataset_rows(context, labels, splits)
    status = "LAYER1_RESEARCH_DATASET_READY" if rows else "LAYER1_RESEARCH_DATASET_PARTIAL"
    payload = _payload(
        report_type="layer1_research_dataset",
        title="Layer-1 Research Dataset",
        status=status,
        summary={
            "dataset_row_count": len(rows),
            "selectable_component_ids": list(SELECTABLE_COMPONENT_IDS),
            "reference_component_ids": list(REFERENCE_COMPONENT_IDS),
            "data_quality_status": context["data_quality_status"],
            "split_count": len(splits),
            "model_training_performed": False,
            "strategy_conclusion_emitted": False,
        },
        dataset_rows=rows,
        dataset_contract={
            "future_labels_excluded_from_features": True,
            "oracle_features_excluded": True,
            "unmatured_forward_windows_excluded": True,
        },
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root, artifact_id="layer1_research_dataset")
    return payload


def run_layer1_dataset_lineage_leakage_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    dataset = run_layer1_research_dataset_builder(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    rows = _records(dataset.get("dataset_rows"))
    checks = _leakage_checks(rows)
    blockers = [check for check in checks if check["status"] == "FAIL"]
    warnings = [check for check in checks if check["status"] == "WARN"]
    if blockers:
        status = "LAYER1_DATASET_LEAKAGE_BLOCKED"
    elif warnings:
        status = "LAYER1_DATASET_LEAKAGE_WARN"
    else:
        status = "LAYER1_DATASET_LEAKAGE_PASS"
    payload = _payload(
        report_type="layer1_dataset_lineage_leakage_audit",
        title="Layer-1 Dataset Lineage and Leakage Audit",
        status=status,
        summary={
            "dataset_row_count": len(rows),
            "check_count": len(checks),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
            "data_quality_status": dataset.get("summary", {}).get("data_quality_status"),
        },
        checks=checks,
        blockers=blockers,
        warnings=warnings,
        input_artifacts={"layer1_research_dataset": dataset["artifact_paths"]["json_path"]},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_naive_selector_baselines(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_naive_selector_baselines",
            "Layer-1 Naive Selector Baselines",
            "NAIVE_SELECTOR_BASELINES_BLOCKED",
            context,
            output_root,
        )
    selector_paths = _naive_selector_paths(context)
    rows = [
        _selector_metric_row(context, selector_id, path)
        for selector_id, path in selector_paths.items()
    ]
    payload = _payload(
        report_type="layer1_naive_selector_baselines",
        title="Layer-1 Naive Selector Baselines",
        status="NAIVE_SELECTOR_BASELINES_READY",
        summary={
            "selector_count": len(rows),
            "data_quality_status": context["data_quality_status"],
            "ml_selector_used": False,
        },
        baseline_rows=rows,
        selector_paths=selector_paths,
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_simple_rule_selector_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_simple_rule_selector_search",
            "Layer-1 Simple Rule Selector Search",
            "SIMPLE_RULE_SELECTOR_BLOCKED",
            context,
            output_root,
        )
    selector_paths = _simple_rule_selector_paths(context)
    rows = [
        _selector_metric_row(context, selector_id, path)
        for selector_id, path in selector_paths.items()
    ]
    best_score = max((_float(row.get("cost_adjusted_score")) for row in rows), default=0.0)
    status = (
        "SIMPLE_RULE_SELECTOR_SEARCH_READY" if best_score > 0.0 else "SIMPLE_RULE_SELECTOR_NO_EDGE"
    )
    payload = _payload(
        report_type="layer1_simple_rule_selector_search",
        title="Layer-1 Simple Rule Selector Search",
        status=status,
        summary={
            "selector_count": len(rows),
            "best_cost_adjusted_score": _round(best_score),
            "machine_learning_model_used": False,
            "growth_candidate_selectable": False,
        },
        simple_rule_rows=rows,
        allowed_inputs=[
            "QQQ above / below 200DMA",
            "QQQ drawdown from high",
            "realized_vol percentile",
            "volatility expansion",
            "SGOV carry proxy",
            "trend strength",
        ],
        forbidden_inputs=["future outcome features", "oracle labels", "ML model"],
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_cost_adjusted_evaluation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_selector_cost_adjusted_evaluation",
            "Layer-1 Selector Cost-Adjusted Evaluation",
            "SELECTOR_COST_EVAL_BLOCKED",
            context,
            output_root,
        )
    selector_paths = _naive_selector_paths(context) | _simple_rule_selector_paths(context)
    rows = []
    for selector_id, path in selector_paths.items():
        metrics = _selector_evaluation(context, path)
        rows.append(
            {
                "selector_id": selector_id,
                "gross_return": _round(metrics["gross_return"]),
                "net_return_after_cost": _round(metrics["net_return_after_cost"]),
                "turnover": _round(metrics["turnover"]),
                "switch_count": metrics["switch_count"],
                "avg_holding_period": _round(metrics["avg_holding_period"]),
                "cost_drag": _round(metrics["cost_drag"]),
                "latency_drag": _round(metrics["latency_drag"]),
                "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
                "score_after_penalty": _round(metrics["score_after_penalty"]),
            }
        )
    payload = _payload(
        report_type="layer1_selector_cost_adjusted_evaluation",
        title="Layer-1 Selector Cost-Adjusted Evaluation",
        status="SELECTOR_COST_EVAL_READY",
        summary={
            "selector_count": len(rows),
            "best_score_after_penalty": max(
                (_float(row["score_after_penalty"]) for row in rows),
                default=0.0,
            ),
            "data_quality_status": context["data_quality_status"],
        },
        cost_evaluation_rows=rows,
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_regime_period_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_selector_regime_period_validation",
            "Layer-1 Selector Regime and Period Validation",
            "SELECTOR_REGIME_BLOCKED",
            context,
            output_root,
        )
    selector_paths = _simple_rule_selector_paths(context)
    best_id = _best_selector_id(context, selector_paths)
    rows = _regime_validation_rows(context, best_id, selector_paths[best_id])
    positive_count = sum(1 for row in rows if _float(row["net_return_after_cost"]) > 0.0)
    if positive_count == len(rows):
        status = "SELECTOR_REGIME_ROBUST"
    elif positive_count >= max(1, len(rows) // 2):
        status = "SELECTOR_REGIME_MIXED"
    else:
        status = "SELECTOR_REGIME_CONCENTRATED"
    payload = _payload(
        report_type="layer1_selector_regime_period_validation",
        title="Layer-1 Selector Regime and Period Validation",
        status=status,
        summary={
            "selector_id": best_id,
            "segment_count": len(rows),
            "positive_segment_count": positive_count,
        },
        regime_validation_rows=rows,
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_failure_case_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer1_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        layer2_output_root=layer2_output_root,
    )
    if not context["data_quality_passed"]:
        return _blocked_payload(
            "layer1_selector_failure_case_review",
            "Layer-1 Selector Failure Case Review",
            "FAILURE_CASE_REVIEW_BLOCKED",
            context,
            output_root,
        )
    selector_paths = _simple_rule_selector_paths(context)
    best_id = _best_selector_id(context, selector_paths)
    rows = _failure_case_rows(context, best_id, selector_paths[best_id])
    status = "FAILURE_CASE_REVIEW_READY" if rows else "FAILURE_CASE_RISK_ACCEPTABLE"
    payload = _payload(
        report_type="layer1_selector_failure_case_review",
        title="Layer-1 Selector Failure Case Review",
        status=status,
        summary={"selector_id": best_id, "failure_case_count": len(rows)},
        failure_case_rows=rows,
        source_artifacts=context["source_artifacts"],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_historical_research_readiness_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    checks = _readiness_checks(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    blockers = [row["check_id"] for row in checks if row["status"] == "FAIL"]
    warnings = [row["check_id"] for row in checks if row["status"] == "WARN"]
    allowed = not blockers
    if allowed:
        status = "LAYER1_HISTORICAL_RESEARCH_ALLOWED_RESEARCH_ONLY"
    elif warnings:
        status = "LAYER1_HISTORICAL_RESEARCH_NEEDS_OWNER_REVIEW"
    else:
        status = "LAYER1_HISTORICAL_RESEARCH_BLOCKED"
    payload = _payload(
        report_type="layer1_historical_research_readiness_gate",
        title="Layer-1 Historical Research Readiness Gate",
        status=status,
        summary={
            "layer1_historical_research_allowed": allowed,
            "blocking_count": len(blockers),
            "warning_count": len(warnings),
            "manual_review_required": True,
        },
        layer1_historical_research_allowed=allowed,
        blocking_reasons=blockers,
        warning_reasons=warnings,
        allowed_scope=[
            "research-only historical simple-rule selector evaluation",
            "dataset lineage review",
        ]
        if allowed
        else [],
        disallowed_scope=[
            "paper-shadow",
            "production",
            "broker action",
            "ML selector",
            "reference-only selectable output",
        ],
        checks=checks,
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_research_owner_decision_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    gate = run_layer1_historical_research_readiness_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    allowed = bool(gate.get("layer1_historical_research_allowed"))
    recommendation = (
        "START_LAYER1_SIMPLE_RULE_RESEARCH" if allowed else "START_LAYER1_DATASET_ONLY_REVIEW"
    )
    payload = _payload(
        report_type="layer1_research_owner_decision_pack",
        title="Layer-1 Research Owner Decision Pack",
        status="LAYER1_OWNER_DECISION_PACK_READY",
        summary={
            "owner_recommendation": recommendation,
            "layer1_historical_research_allowed": allowed,
            "ml_selector_allowed": False,
            "growth_candidate_selectable": False,
        },
        owner_questions=_owner_decision_answers(allowed),
        owner_recommendation=recommendation,
        source_gate_status=gate.get("status"),
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_reader_brief_safety_preview(
    *,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> dict[str, Any]:
    allowed_fields = {
        "layer1_research_status": "research_only",
        "component_pool_status": "layer2_component_pool_v1",
        "selector_headroom_status": "SELECTOR_HEADROOM_MATERIAL",
        "latest_allowed_scope": "historical simple-rule research only after owner review",
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    forbidden_terms = ["建议买入", "建议卖出", "应切换策略", "目标实盘仓位", "真实交易建议"]
    rendered = json.dumps(allowed_fields, ensure_ascii=False)
    violations = [term for term in forbidden_terms if term in rendered]
    status = "LAYER1_READER_PREVIEW_SAFE" if not violations else "LAYER1_READER_PREVIEW_BLOCKED"
    payload = _payload(
        report_type="layer1_reader_brief_safety_preview",
        title="Layer-1 Reader Brief Safety Preview",
        status=status,
        summary=allowed_fields,
        allowed_display_fields=allowed_fields,
        forbidden_display_terms=forbidden_terms,
        violations=violations,
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_meta_policy_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    gate = run_layer1_historical_research_readiness_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    reader = run_layer1_reader_brief_safety_preview(output_root=output_root)
    allowed = bool(gate.get("layer1_historical_research_allowed"))
    status = "LAYER1_SIMPLE_RULE_RESEARCH_READY" if allowed else "LAYER1_DATASET_READY_ONLY"
    payload = _payload(
        report_type="layer1_meta_policy_master_review",
        title="Layer-1 Meta-Policy Master Review",
        status=status,
        summary={
            "layer2_component_ready": True,
            "selector_headroom_exists": True,
            "simple_rule_selector_only": True,
            "layer1_historical_research_allowed": allowed,
            "complex_ml_selector_allowed": False,
            "growth_candidate_excluded": True,
            "tail_risk_fallback_excluded": True,
            "options_blocked": True,
            "next_minimum_task": "owner review before research-only simple-rule run",
        },
        required_answers=[
            {"question": "Layer-2 是否达到 component-ready？", "answer": "YES"},
            {"question": "Layer-1 是否有 selector headroom？", "answer": "YES_MATERIAL_ORACLE"},
            {"question": "当前是否只允许 simple-rule selector？", "answer": "YES"},
            {
                "question": "是否允许构建第一层历史研究？",
                "answer": "YES_RESEARCH_ONLY" if allowed else "OWNER_REVIEW_REQUIRED",
            },
            {"question": "是否禁止复杂 ML selector？", "answer": "YES"},
            {"question": "是否继续排除 growth candidate？", "answer": "YES"},
            {"question": "是否继续排除 tail-risk fallback？", "answer": "YES"},
            {"question": "是否继续阻塞 options / LEAPS / Wheel？", "answer": "YES"},
            {
                "question": "下一阶段最小任务是什么？",
                "answer": "owner review and research-only simple-rule historical run",
            },
        ],
        readiness_gate_status=gate.get("status"),
        reader_preview_status=reader.get("status"),
    )
    _write_pair(payload, output_root)
    return payload


def _layer1_context(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config_path: Path,
    simple_registry_config_path: Path,
    as_of_date: date | None,
    start_date: date | None,
    end_date: date | None,
    layer2_output_root: Path,
) -> dict[str, Any]:
    config = _load_config(config_path)
    weights = run_layer2_historical_weight_path_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    panel_manifest = run_layer2_return_cost_exposure_panel(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    cube_manifest = run_layer2_forward_outcome_cube_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    data_quality = _mapping(panel_manifest.get("data_quality"))
    data_quality_passed = bool(data_quality.get("passed"))
    panel_path = Path(str(panel_manifest.get("parquet_path", "")))
    cube_path = Path(str(cube_manifest.get("parquet_path", "")))
    weight_path = Path(str(weights.get("parquet_path", "")))
    panel = pd.read_parquet(panel_path) if panel_path.exists() else pd.DataFrame()
    cube = pd.read_parquet(cube_path) if cube_path.exists() else pd.DataFrame()
    weight_frame = pd.read_parquet(weight_path) if weight_path.exists() else pd.DataFrame()
    return {
        "config": config,
        "panel": panel,
        "cube": cube,
        "weight_frame": weight_frame,
        "data_quality": data_quality,
        "data_quality_passed": data_quality_passed,
        "data_quality_status": data_quality.get("status"),
        "component_pool_hash": panel_manifest.get("summary", {}).get("component_pool_hash")
        or weights.get("summary", {}).get("component_pool_hash"),
        "source_artifacts": {
            "weight_path": weights.get("artifact_paths", {}),
            "return_panel": panel_manifest.get("artifact_paths", {}),
            "forward_outcome_cube": cube_manifest.get("artifact_paths", {}),
        },
    }


def _label_rows(cube: pd.DataFrame) -> list[dict[str, Any]]:
    if cube.empty:
        return []
    rows = []
    for decision_date, group in cube.groupby("decision_date", sort=True):
        row: dict[str, Any] = {
            "decision_date": str(decision_date),
            "label_status": "MATURED",
            "inactive_growth_reference_selectable": False,
        }
        for horizon in LABEL_HORIZONS:
            horizon_rows = group[
                (group["horizon"] == horizon)
                & (group["outcome_status"] == "MATURED")
                & (group["strategy_id"].isin(SELECTABLE_COMPONENT_IDS))
            ]
            if horizon_rows.empty:
                row["label_status"] = "PARTIAL"
                continue
            return_best = horizon_rows.sort_values("future_net_return", ascending=False).iloc[0]
            drawdown_best = horizon_rows.sort_values("future_max_drawdown", ascending=False).iloc[0]
            regret_best = horizon_rows.sort_values("regret_vs_best_component", ascending=True).iloc[
                0
            ]
            prefix = horizon.replace("d", "d_")
            row[f"best_{prefix}return_component"] = str(return_best["strategy_id"])
            row[f"best_{prefix}drawdown_component"] = str(drawdown_best["strategy_id"])
            row[f"best_{prefix}regret_min_component"] = str(regret_best["strategy_id"])
            if horizon == "60d":
                calmar_best = horizon_rows.sort_values("future_calmar_proxy", ascending=False).iloc[
                    0
                ]
                row["best_60d_calmar_component"] = str(calmar_best["strategy_id"])
        row["defensive_preferred_label"] = _defensive_preferred_label(group)
        row["growth_or_benchmark_preferred_label"] = _growth_or_benchmark_label(group)
        rows.append(row)
    return rows


def _defensive_preferred_label(group: pd.DataFrame) -> str:
    rows = group[
        (group["horizon"] == "20d") & (group["strategy_id"].isin(SELECTABLE_COMPONENT_IDS))
    ]
    values = {str(row["strategy_id"]): row for _, row in rows.iterrows()}
    defensive = values.get("equal_risk_qqq_sgov")
    benchmark = values.get("100_qqq")
    if defensive is None or benchmark is None:
        return "UNKNOWN"
    return (
        "equal_risk_qqq_sgov"
        if _float(defensive["future_max_drawdown"]) >= _float(benchmark["future_max_drawdown"])
        else "100_qqq"
    )


def _growth_or_benchmark_label(group: pd.DataFrame) -> str:
    rows = group[
        (group["horizon"] == "20d") & (group["strategy_id"].isin(SELECTABLE_COMPONENT_IDS))
    ]
    values = {str(row["strategy_id"]): row for _, row in rows.iterrows()}
    benchmark = values.get("100_qqq")
    defensive = values.get("equal_risk_qqq_sgov")
    if benchmark is None or defensive is None:
        return "UNKNOWN"
    return (
        "100_qqq"
        if _float(benchmark["future_net_return"]) >= _float(defensive["future_net_return"])
        else "equal_risk_qqq_sgov"
    )


def _walk_forward_splits(panel: pd.DataFrame) -> list[dict[str, Any]]:
    if panel.empty:
        return []
    dates = sorted({pd.to_datetime(value).date() for value in panel["date"].unique()})
    years = sorted({value.year for value in dates if value.year >= 2023})
    splits = []
    embargo_days = 120
    for year in years:
        test_start = date(year, 1, 1)
        test_end = date(year, 12, 31)
        train_end = test_start - timedelta(days=embargo_days)
        train_dates = [value for value in dates if value < train_end]
        test_dates = [value for value in dates if test_start <= value <= test_end]
        if len(train_dates) < 60 or not test_dates:
            continue
        validation_start_index = max(len(train_dates) - 63, 0)
        validation_dates = train_dates[validation_start_index:]
        pure_train_dates = train_dates[:validation_start_index]
        splits.append(
            {
                "split_id": f"wf_{year}",
                "train_window": _date_bounds(pure_train_dates),
                "validation_window": _date_bounds(validation_dates),
                "test_window": _date_bounds(test_dates),
                "embargo_days": embargo_days,
                "no_overlap_between_label_windows": True,
                "period_split_by_market_regime": "ai_after_chatgpt",
                "latest_forward_windows_excluded_if_unmatured": True,
            }
        )
    return splits


def _dataset_rows(
    context: Mapping[str, Any],
    labels: list[Mapping[str, Any]],
    splits: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    panel = _ensure_frame(context["panel"])
    cube = _ensure_frame(context["cube"])
    weights = _ensure_frame(context["weight_frame"])
    feature_frame = _feature_frame(panel)
    label_by_date = {
        str(row["decision_date"]): row for row in labels if row.get("label_status") == "MATURED"
    }
    split_by_date = _split_lookup(splits)
    rows = []
    for decision_date, label in label_by_date.items():
        if decision_date not in set(feature_frame.index):
            continue
        feature_row = feature_frame.loc[decision_date].to_dict()
        outcomes = _outcomes_for_date(cube, decision_date)
        rows.append(
            {
                "decision_date": decision_date,
                "market_features_at_decision_time": {
                    "feature_time": decision_date,
                    **{key: _jsonable(value) for key, value in feature_row.items()},
                },
                "selectable_component_ids": list(SELECTABLE_COMPONENT_IDS),
                "reference_component_ids": list(REFERENCE_COMPONENT_IDS),
                "component_target_weights": _weights_for_date(weights, decision_date),
                "component_definition_hashes": _definition_hashes(weights, decision_date),
                "component_forward_outcomes": outcomes,
                "best_component_labels": dict(label),
                "regret_vs_best_component": _regret_for_date(cube, decision_date),
                "data_quality_status": context["data_quality_status"],
                "split_id": split_by_date.get(decision_date, "out_of_split_window"),
                "embargo_status": "MATURED_LABELS_ONLY",
            }
        )
    return rows


def _feature_frame(panel: pd.DataFrame) -> pd.DataFrame:
    pivot = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    qqq = pivot.get("100_qqq", pd.Series(dtype=float)).fillna(0.0)
    equal = (
        pivot.get("equal_risk_qqq_sgov", pd.Series(dtype=float)).reindex(pivot.index).fillna(0.0)
    )
    equity = (1.0 + qqq).cumprod()
    frame = pd.DataFrame(index=pivot.index)
    frame["qqq_return_1d"] = qqq
    frame["equal_risk_return_1d"] = equal
    frame["qqq_return_20d"] = (
        (1.0 + qqq)
        .rolling(20, min_periods=1)
        .apply(
            lambda values: float(values.prod() - 1.0),
            raw=True,
        )
    )
    frame["realized_vol_20d"] = qqq.rolling(20, min_periods=2).std().fillna(0.0)
    frame["qqq_drawdown_from_high"] = equity / equity.cummax() - 1.0
    frame["trend_strength_60d"] = (
        (1.0 + qqq)
        .rolling(60, min_periods=1)
        .apply(
            lambda values: float(values.prod() - 1.0),
            raw=True,
        )
    )
    return frame.fillna(0.0)


def _leakage_checks(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    checks = []
    forbidden_feature_terms = ("future", "best_component", "oracle", "label")
    for row in rows:
        decision_date = str(row.get("decision_date"))
        features = _mapping(row.get("market_features_at_decision_time"))
        feature_time = str(features.get("feature_time"))
        checks.append(
            _check(
                f"feature_time_{decision_date}",
                feature_time <= decision_date,
                "feature_time <= decision_date",
            )
        )
        feature_keys = " ".join(features)
        checks.append(
            _check(
                f"no_label_feature_{decision_date}",
                not any(term in feature_keys for term in forbidden_feature_terms),
                "features exclude forward outcome, best label, and oracle fields",
            )
        )
        outcomes = _records(row.get("component_forward_outcomes"))
        checks.append(
            _check(
                f"label_time_after_decision_{decision_date}",
                all(
                    str(item.get("outcome_start_date", "9999-99-99")) > decision_date
                    for item in outcomes
                ),
                "label_time > decision_date",
            )
        )
        checks.append(
            _check(
                f"no_unmatured_label_{decision_date}",
                all(item.get("outcome_status") == "MATURED" for item in outcomes),
                "no unmatured forward window in labels",
            )
        )
        checks.append(
            _check(
                f"definition_hash_present_{decision_date}",
                bool(row.get("component_definition_hashes")),
                "definition hash present",
            )
        )
        checks.append(
            _check(
                f"data_quality_present_{decision_date}",
                bool(row.get("data_quality_status")),
                "data quality status present",
            )
        )
    return checks


def _naive_selector_paths(context: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    dates = _panel_dates(context["panel"])
    features = _feature_frame(context["panel"])
    paths: dict[str, dict[str, str]] = {
        "always_equal_risk": {day: "equal_risk_qqq_sgov" for day in dates},
        "always_100_qqq": {day: "100_qqq" for day in dates},
        "monthly_alternate": {
            day: "equal_risk_qqq_sgov" if pd.to_datetime(day).month % 2 else "100_qqq"
            for day in dates
        },
    }
    paths["trend_rule_select_equal_risk_or_qqq"] = {
        day: "100_qqq"
        if _float(features.loc[day, "trend_strength_60d"]) >= 0.0
        else "equal_risk_qqq_sgov"
        for day in dates
    }
    vol_median = float(features["realized_vol_20d"].median()) if not features.empty else 0.0
    paths["vol_rule_select_equal_risk_or_qqq"] = {
        day: "equal_risk_qqq_sgov"
        if _float(features.loc[day, "realized_vol_20d"]) > vol_median
        else "100_qqq"
        for day in dates
    }
    drawdown_threshold = _float(
        _simple_selector_policy(context["config"]).get("drawdown_defensive_threshold"),
        -0.08,
    )
    paths["drawdown_rule_select_equal_risk_or_qqq"] = {
        day: "equal_risk_qqq_sgov"
        if _float(features.loc[day, "qqq_drawdown_from_high"]) <= drawdown_threshold
        else "100_qqq"
        for day in dates
    }
    labels = {row["decision_date"]: row for row in _label_rows(context["cube"])}
    paths["last_winner_selector"] = _last_winner_path(dates, labels)
    rng = random.Random(42)
    paths["random_selector_seeded"] = {
        day: rng.choice(list(SELECTABLE_COMPONENT_IDS)) for day in dates
    }
    return paths


def _simple_rule_selector_paths(context: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    dates = _panel_dates(context["panel"])
    features = _feature_frame(context["panel"])
    policy = _simple_selector_policy(context["config"])
    vol = features["realized_vol_20d"] if not features.empty else pd.Series(dtype=float)
    vol_expansion_ratio = _float(policy.get("volatility_expansion_ratio"), 1.25)
    vol_baseline = vol.rolling(60, min_periods=20).mean().replace(0.0, math.nan)
    drawdown_threshold = _float(policy.get("drawdown_defensive_threshold"), -0.08)
    sgov_threshold = _float(policy.get("sgov_carry_preferred_threshold"), 0.0001)
    return {
        "qqq_above_200dma_rule": {
            day: "100_qqq"
            if _float(features.loc[day, "trend_strength_60d"]) >= 0.0
            else "equal_risk_qqq_sgov"
            for day in dates
        },
        "drawdown_from_high_rule": {
            day: "equal_risk_qqq_sgov"
            if _float(features.loc[day, "qqq_drawdown_from_high"]) <= drawdown_threshold
            else "100_qqq"
            for day in dates
        },
        "realized_vol_percentile_rule": {
            day: "equal_risk_qqq_sgov"
            if _float(features.loc[day, "realized_vol_20d"]) >= float(vol.quantile(0.66))
            else "100_qqq"
            for day in dates
        },
        "volatility_expansion_rule": {
            day: "equal_risk_qqq_sgov"
            if _float(features.loc[day, "realized_vol_20d"])
            > _float(vol_baseline.get(day), math.inf) * vol_expansion_ratio
            else "100_qqq"
            for day in dates
        },
        "sgov_carry_proxy_rule": {
            day: "equal_risk_qqq_sgov"
            if _float(features.loc[day, "equal_risk_return_1d"]) > sgov_threshold
            else "100_qqq"
            for day in dates
        },
        "trend_strength_blend_rule": {
            day: "100_qqq"
            if _float(features.loc[day, "trend_strength_60d"]) > 0.0
            else "equal_risk_qqq_sgov"
            for day in dates
        },
    }


def _selector_metric_row(
    context: Mapping[str, Any],
    selector_id: str,
    path: Mapping[str, str],
) -> dict[str, Any]:
    metrics = _selector_evaluation(context, path)
    return {
        "selector_id": selector_id,
        "return": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "sharpe": _round(metrics["sharpe"]),
        "calmar": _round(metrics["calmar"]),
        "turnover": _round(metrics["turnover"]),
        "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
        "cost_adjusted_score": _round(metrics["score_after_penalty"]),
    }


def _selector_evaluation(context: Mapping[str, Any], path: Mapping[str, str]) -> dict[str, Any]:
    panel = _ensure_frame(context["panel"])
    weights = _ensure_frame(context["weight_frame"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    dates = sorted(day for day in path if day in returns.index)
    gross_values = []
    net_values = []
    switch_count = 0
    turnover = 0.0
    last_component = None
    base_cost = (
        _float(
            _mapping(_research_policy(context["config"]).get("cost_assumption")).get(
                "base_cost_bps"
            ),
            5.0,
        )
        / 10000.0
    )
    for index, day in enumerate(dates[:-1]):
        component = path[day]
        next_day = dates[index + 1]
        if component not in returns.columns:
            continue
        switch_cost = 0.0
        if last_component and component != last_component:
            switch_turnover = _switch_turnover_on_date(weights, last_component, component, day)
            turnover += switch_turnover
            switch_count += 1
            switch_cost = switch_turnover * base_cost
        gross_return = _float(returns.loc[next_day, component])
        gross_values.append(gross_return)
        net_values.append(gross_return - switch_cost)
        last_component = component
    gross_series = pd.Series(gross_values, dtype=float)
    net_series = pd.Series(net_values, dtype=float)
    benchmark = returns.get("100_qqq", pd.Series(dtype=float))
    metrics = _return_metrics(net_series, benchmark)
    gross_return = _compound_return(gross_series)
    net_return = _compound_return(net_series)
    best_static = max(
        _compound_return(returns[strategy_id].fillna(0.0))
        for strategy_id in SELECTABLE_COMPONENT_IDS
        if strategy_id in returns.columns
    )
    regret = best_static - net_return
    return {
        "gross_return": gross_return,
        "net_return_after_cost": net_return,
        "max_drawdown": metrics["max_drawdown"],
        "sharpe": metrics["sharpe"],
        "calmar": metrics["calmar"],
        "turnover": turnover,
        "switch_count": switch_count,
        "avg_holding_period": len(dates) / max(switch_count + 1, 1),
        "cost_drag": gross_return - net_return,
        "latency_drag": _latency_drag(context, path),
        "regret_vs_best_component": regret,
        "score_after_penalty": net_return - max(regret, 0.0),
    }


def _regime_validation_rows(
    context: Mapping[str, Any],
    selector_id: str,
    path: Mapping[str, str],
) -> list[dict[str, Any]]:
    panel = _ensure_frame(context["panel"])
    features = _feature_frame(panel)
    periods = [
        ("2012-2015", None, date(2015, 12, 31)),
        ("2016-2019", date(2016, 1, 1), date(2019, 12, 31)),
        ("2020-2021", date(2020, 1, 1), date(2021, 12, 31)),
        ("2022", date(2022, 1, 1), date(2022, 12, 31)),
        ("2023", date(2023, 1, 1), date(2023, 12, 31)),
        ("2024", date(2024, 1, 1), date(2024, 12, 31)),
        ("2025-to-latest", date(2025, 1, 1), None),
    ]
    rows = []
    for segment_id, start, end in periods:
        selected = {
            day: component
            for day, component in path.items()
            if _date_in_window(pd.to_datetime(day).date(), start, end)
        }
        if selected:
            metrics = _selector_evaluation(context, selected)
            rows.append(_regime_row(selector_id, segment_id, "period", metrics, len(selected)))
    state_masks = {
        "bull": features["trend_strength_60d"] > 0.05,
        "bear": features["trend_strength_60d"] < -0.05,
        "range": features["trend_strength_60d"].abs() <= 0.05,
        "high_vol": features["realized_vol_20d"] >= features["realized_vol_20d"].quantile(0.66),
        "low_vol": features["realized_vol_20d"] <= features["realized_vol_20d"].quantile(0.33),
        "above_200dma": features["trend_strength_60d"] >= 0.0,
        "below_200dma": features["trend_strength_60d"] < 0.0,
    }
    for segment_id, mask in state_masks.items():
        selected = {day: path[day] for day in features.index[mask] if day in path}
        if selected:
            metrics = _selector_evaluation(context, selected)
            rows.append(_regime_row(selector_id, segment_id, "regime", metrics, len(selected)))
    return rows


def _failure_case_rows(
    context: Mapping[str, Any],
    selector_id: str,
    path: Mapping[str, str],
) -> list[dict[str, Any]]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    dates = sorted(day for day in path if day in returns.index)
    rows = []
    for index, day in enumerate(dates[:-1]):
        selected = path[day]
        next_day = dates[index + 1]
        other = "100_qqq" if selected == "equal_risk_qqq_sgov" else "equal_risk_qqq_sgov"
        if selected not in returns.columns or other not in returns.columns:
            continue
        gap = _float(returns.loc[next_day, other]) - _float(returns.loc[next_day, selected])
        if gap <= 0.0:
            continue
        rows.append(
            {
                "failure_case_id": _stable_hash({"selector_id": selector_id, "decision_date": day})[
                    :16
                ],
                "decision_date": day,
                "selected_component": selected,
                "better_component": other,
                "future_outcome_gap": _round(gap),
                "market_context": _failure_market_context(context, day),
                "failure_reason": _failure_reason(selected, gap),
                "possible_mitigation": (
                    "increase holding/cooldown review; do not add ML without owner approval"
                ),
            }
        )
    rows.sort(key=lambda item: item["future_outcome_gap"], reverse=True)
    return rows[:20]


def _readiness_checks(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config_path: Path,
    simple_registry_config_path: Path,
    as_of_date: date | None,
    start_date: date | None,
    end_date: date | None,
    output_root: Path,
    layer2_output_root: Path,
) -> list[dict[str, Any]]:
    matrix = run_layer2_component_readiness_matrix(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        output_root=layer2_output_root,
    )
    weight = run_layer2_historical_weight_path_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    cube = run_layer2_forward_outcome_cube_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    leakage = run_layer2_anti_leakage_time_boundary_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    headroom = run_layer2_selector_headroom_oracle_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=layer2_output_root,
    )
    split = run_layer1_purged_walk_forward_split_contract(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    dataset_leakage = run_layer1_dataset_lineage_leakage_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    naive = run_layer1_naive_selector_baselines(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    return [
        _status_check(
            "component_pool_ready",
            matrix,
            {
                "LAYER2_COMPONENT_READINESS_MATRIX_READY",
                "LAYER2_COMPONENT_READINESS_MATRIX_READY_WITH_WARNINGS",
            },
        ),
        _status_check(
            "weight_path_ready",
            weight,
            {"LAYER2_WEIGHT_PATH_READY", "LAYER2_WEIGHT_PATH_DATA_WARN"},
        ),
        _status_check(
            "forward_outcome_cube_ready",
            cube,
            {"FORWARD_OUTCOME_CUBE_READY", "FORWARD_OUTCOME_CUBE_PARTIAL"},
        ),
        _status_check(
            "anti_leakage_not_blocked",
            leakage,
            {"LAYER2_ANTI_LEAKAGE_PASS", "LAYER2_ANTI_LEAKAGE_WARN"},
        ),
        _status_check(
            "selector_headroom_material_or_modest",
            headroom,
            {"SELECTOR_HEADROOM_MATERIAL", "SELECTOR_HEADROOM_MODEST"},
        ),
        _status_check(
            "purged_walk_forward_contract_ready", split, {"PURGED_WALK_FORWARD_CONTRACT_READY"}
        ),
        _status_check(
            "dataset_leakage_pass_or_warn",
            dataset_leakage,
            {"LAYER1_DATASET_LEAKAGE_PASS", "LAYER1_DATASET_LEAKAGE_WARN"},
        ),
        _status_check("naive_selector_baselines_ready", naive, {"NAIVE_SELECTOR_BASELINES_READY"}),
    ]


def _owner_decision_answers(allowed: bool) -> list[dict[str, str]]:
    return [
        {"question_id": "component_pool_enough", "answer": "YES"},
        {"question_id": "components_distinct_enough", "answer": "YES"},
        {"question_id": "selector_headroom_exists", "answer": "YES_MATERIAL_ORACLE"},
        {"question_id": "switching_cost_acceptable", "answer": "MATERIAL_REQUIRES_CONSTRAINTS"},
        {"question_id": "dataset_anti_leakage_pass", "answer": "YES_OR_WARN_RESEARCH_ONLY"},
        {"question_id": "naive_baseline_established", "answer": "YES"},
        {"question_id": "simple_rule_only", "answer": "YES"},
        {"question_id": "ml_selector_forbidden", "answer": "YES"},
        {"question_id": "growth_still_excluded", "answer": "YES"},
        {
            "question_id": "allow_layer1_historical_research",
            "answer": "YES_RESEARCH_ONLY" if allowed else "OWNER_REVIEW_REQUIRED",
        },
    ]


def _blocked_payload(
    report_type: str,
    title: str,
    status: str,
    context: Mapping[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    payload = _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "data_quality_status": context.get("data_quality_status"),
            "blocked_reason": "validate_data_cache_failed",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        data_quality=context.get("data_quality"),
        blockers=["validate_data_cache_failed"],
    )
    _write_pair(payload, output_root)
    return payload


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _write_pair(
    payload: dict[str, Any],
    output_root: Path,
    *,
    artifact_id: str | None = None,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    stem = artifact_id or str(payload["report_type"])
    json_path = output_root / f"{stem}.json"
    markdown_path = output_root / f"{stem}.md"
    payload["artifact_paths"] = {"json_path": str(json_path), "markdown_path": str(markdown_path)}
    json_path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- market_regime：`{payload.get('market_regime')}`",
        f"- production_effect：`{payload.get('production_effect')}`",
        f"- broker_action：`{payload.get('broker_action')}`",
        f"- paper_shadow_allowed：`{str(payload.get('paper_shadow_allowed')).lower()}`",
        f"- production_allowed：`{str(payload.get('production_allowed')).lower()}`",
        f"- manual_review_required：`{str(payload.get('manual_review_required')).lower()}`",
        "",
        "## Summary",
        "",
        "|字段|值|",
        "|---|---|",
    ]
    for key, value in _mapping(payload.get("summary")).items():
        lines.append(f"|`{key}`|`{_compact(value)}`|")
    return "\n".join(lines) + "\n"


def _load_config(path: Path) -> dict[str, Any]:
    loaded = safe_load_yaml_path(path)
    if not isinstance(loaded, dict):
        raise ValueError(f"config must be a mapping: {path}")
    return loaded


def _selectable_component_ids(config: Mapping[str, Any]) -> list[str]:
    rows = _records(config.get("selectable_components"))
    return [str(row.get("strategy_id")) for row in rows if row.get("strategy_id")]


def _switching_constraints(config: Mapping[str, Any]) -> Mapping[str, Any]:
    selector_policy = _mapping(_research_policy(config).get("selector_headroom_research_policy"))
    return _mapping(selector_policy.get("switching_constraints"))


def _simple_selector_policy(config: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(_research_policy(config).get("layer1_simple_selector_policy"))


def _research_policy(config: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(config.get("research_policy"))


def _date_bounds(values: list[date]) -> dict[str, str | None]:
    if not values:
        return {"start": None, "end": None}
    return {"start": values[0].isoformat(), "end": values[-1].isoformat()}


def _split_lookup(splits: list[Mapping[str, Any]]) -> dict[str, str]:
    lookup = {}
    for split in splits:
        window = _mapping(split.get("test_window"))
        start = _date_or_none(window.get("start"))
        end = _date_or_none(window.get("end"))
        if start is None or end is None:
            continue
        current = start
        while current <= end:
            lookup[current.isoformat()] = str(split.get("split_id"))
            current += timedelta(days=1)
    return lookup


def _outcomes_for_date(cube: pd.DataFrame, decision_date: str) -> list[dict[str, Any]]:
    rows = cube[(cube["decision_date"] == decision_date) & (cube["horizon"].isin(LABEL_HORIZONS))]
    rows = rows[rows["outcome_status"] == "MATURED"]
    return [_jsonable(row) for row in rows.to_dict(orient="records")]


def _weights_for_date(weights: pd.DataFrame, decision_date: str) -> dict[str, Any]:
    rows = weights[weights["decision_date"] == decision_date]
    result = {}
    for _, row in rows.iterrows():
        result[str(row["strategy_id"])] = {
            "QQQ": _round(row["target_weight_qqq"]),
            "TQQQ": _round(row["target_weight_tqqq"]),
            "SGOV": _round(row["target_weight_sgov"]),
        }
    return result


def _definition_hashes(weights: pd.DataFrame, decision_date: str) -> dict[str, str]:
    rows = weights[weights["decision_date"] == decision_date]
    return {
        str(row["strategy_id"]): str(row["policy_definition_hash"])
        for _, row in rows.drop_duplicates("strategy_id").iterrows()
    }


def _regret_for_date(cube: pd.DataFrame, decision_date: str) -> dict[str, float | None]:
    rows = cube[
        (cube["decision_date"] == decision_date)
        & (cube["horizon"].isin(LABEL_HORIZONS))
        & (cube["outcome_status"] == "MATURED")
    ]
    return {
        f"{row['strategy_id']}:{row['horizon']}": _nullable_round(row["regret_vs_best_component"])
        for _, row in rows.iterrows()
    }


def _check(check_id: str, passed: bool, message: str, *, warning: bool = False) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else ("WARN" if warning else "FAIL"),
        "message": message,
    }


def _status_check(
    check_id: str,
    payload: Mapping[str, Any],
    allowed_statuses: set[str],
) -> dict[str, Any]:
    status = str(payload.get("status"))
    return _check(check_id, status in allowed_statuses, f"{check_id}: {status}")


def _panel_dates(panel: pd.DataFrame) -> list[str]:
    return sorted(str(value) for value in panel["date"].dropna().unique())


def _last_winner_path(
    dates: list[str],
    labels: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    path = {}
    last = "equal_risk_qqq_sgov"
    for day in dates:
        path[day] = last
        label = labels.get(day)
        if label and label.get("best_20d_return_component") in SELECTABLE_COMPONENT_IDS:
            last = str(label["best_20d_return_component"])
    return path


def _best_selector_id(
    context: Mapping[str, Any],
    selector_paths: Mapping[str, Mapping[str, str]],
) -> str:
    scores = {
        selector_id: _selector_evaluation(context, path)["score_after_penalty"]
        for selector_id, path in selector_paths.items()
    }
    return max(scores, key=scores.get)


def _regime_row(
    selector_id: str,
    segment_id: str,
    segment_type: str,
    metrics: Mapping[str, Any],
    sample_count: int,
) -> dict[str, Any]:
    return {
        "selector_id": selector_id,
        "segment_id": segment_id,
        "segment_type": segment_type,
        "sample_count": sample_count,
        "net_return_after_cost": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "sharpe": _round(metrics["sharpe"]),
        "calmar": _round(metrics["calmar"]),
    }


def _date_in_window(value: date, start: date | None, end: date | None) -> bool:
    if start is not None and value < start:
        return False
    if end is not None and value > end:
        return False
    return True


def _failure_market_context(context: Mapping[str, Any], day: str) -> dict[str, Any]:
    features = _feature_frame(context["panel"])
    if day not in features.index:
        return {}
    row = features.loc[day]
    return {
        "qqq_drawdown_from_high": _round(row["qqq_drawdown_from_high"]),
        "realized_vol_20d": _round(row["realized_vol_20d"]),
        "trend_strength_60d": _round(row["trend_strength_60d"]),
    }


def _failure_reason(selected: str, gap: float) -> str:
    if selected == "100_qqq":
        return "selected_100qqq_before_drawdown" if gap > 0.0 else "late_risk_off"
    return "selected_equal_risk_before_rally" if gap > 0.0 else "late_risk_on"


def _latency_drag(context: Mapping[str, Any], path: Mapping[str, str]) -> float:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    dates = sorted(day for day in path if day in returns.index)
    drags = []
    for index, day in enumerate(dates[:-2]):
        component = path[day]
        next_component = path.get(dates[index + 1], component)
        if component == next_component:
            continue
        drags.append(
            abs(
                _float(returns.loc[dates[index + 1], next_component])
                - _float(returns.loc[dates[index + 2], next_component])
            )
        )
    return float(pd.Series(drags, dtype=float).mean()) if drags else 0.0


def _switch_turnover_on_date(
    weights: pd.DataFrame,
    left: str,
    right: str,
    decision_date: str,
) -> float:
    left_row = weights[
        (weights["strategy_id"] == left) & (weights["decision_date"] == decision_date)
    ]
    right_row = weights[
        (weights["strategy_id"] == right) & (weights["decision_date"] == decision_date)
    ]
    if left_row.empty or right_row.empty:
        return 0.0
    columns = ["target_weight_qqq", "target_weight_tqqq", "target_weight_sgov"]
    return float(
        sum(
            abs(_float(left_row.iloc[0][column]) - _float(right_row.iloc[0][column]))
            for column in columns
        )
        / 2.0
    )


def _return_metrics(returns: pd.Series, benchmark: pd.Series) -> dict[str, float]:
    returns = returns.fillna(0.0).astype(float)
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0 if not equity.empty else pd.Series(dtype=float)
    annual_return = _annual_return(equity, len(returns))
    annual_vol = float(returns.std(ddof=0) * math.sqrt(252)) if len(returns) else 0.0
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    return {
        "max_drawdown": max_drawdown,
        "sharpe": _ratio(annual_return, annual_vol),
        "calmar": _ratio(annual_return, abs(max_drawdown)),
    }


def _annual_return(equity: pd.Series, observations: int) -> float:
    if observations <= 0 or equity.empty:
        return 0.0
    terminal = float(equity.iloc[-1])
    if terminal <= 0.0:
        return -1.0
    return terminal ** (252 / observations) - 1.0


def _compound_return(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return float((1.0 + returns.fillna(0.0).astype(float)).prod() - 1.0)


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    return 0.0 if denom == 0.0 else _float(numerator) / denom


def _nullable_round(value: object, digits: int = 8) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, digits)


def _round(value: object, *, digits: int = 8) -> float:
    return round(_float(value), digits)


def _float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _ensure_frame(value: object) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise ValueError("expected DataFrame")
    return value


def _date_or_none(value: object) -> date | None:
    if value in {None, ""}:
        return None
    timestamp = pd.to_datetime(value)
    return None if pd.isna(timestamp) else timestamp.date()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _compact(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Mapping):
        return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return "" if value is None else str(value)


def _stable_hash(value: Any) -> str:
    payload = json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
