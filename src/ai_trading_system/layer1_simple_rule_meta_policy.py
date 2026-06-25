from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.layer1_low_turnover_selector_helpers import (
    LOW_TURNOVER_BUFFER_GRID,
    LOW_TURNOVER_CONFIRMATION_GRID,
    LOW_TURNOVER_CONSTRAINED_BUFFER_GRID,
    LOW_TURNOVER_CONSTRAINED_CONFIRMATION_GRID,
    LOW_TURNOVER_CONSTRAINED_NEUTRAL_WEIGHTS,
    LOW_TURNOVER_CONSTRAINED_RISK_OFF_WEIGHTS,
    LOW_TURNOVER_CONSTRAINED_RISK_ON_WEIGHTS,
    LOW_TURNOVER_COOLDOWN_GRID,
    LOW_TURNOVER_MAX_SWITCHES_GRID,
    LOW_TURNOVER_MIN_HOLDING_GRID,
    LOW_TURNOVER_NEAR_200DMA_BAND,
    LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE,
    low_turnover_acceptable,
    low_turnover_owner_decision,
    switch_count_control_contract,
    switch_count_control_result,
)
from ai_trading_system.layer1_low_turnover_selector_helpers import (
    best_low_turnover_row as _best_low_turnover_row,
)
from ai_trading_system.layer1_low_turnover_selector_helpers import (
    low_turnover_dominance_status as _low_turnover_dominance_status,
)
from ai_trading_system.layer1_low_turnover_selector_helpers import (
    low_turnover_ranking_summary as _low_turnover_ranking_summary,
)
from ai_trading_system.layer1_meta_policy_readiness import (
    DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    REFERENCE_COMPONENT_IDS,
    SELECTABLE_COMPONENT_IDS,
    _blocked_payload,
    _compound_return,
    _ensure_frame,
    _float,
    _layer1_context,
    _mapping,
    _payload,
    _records,
    _return_metrics,
    _round,
    _write_pair,
)
from ai_trading_system.layer2_strategy_component_readiness import (
    DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "layer1_simple_rule_selector_registry.yaml"
)
DEFAULT_LAYER1_SELECTOR_OWNER_WATCHLIST_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "layer1_selector_owner_watchlist_review.md"
)
DEFAULT_LAYER1_SELECTOR_RESULT_REVIEW_MASTER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "layer1_selector_result_review_master.md"
)
DEFAULT_LAYER1_SELECTOR_LOW_TURNOVER_OWNER_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "layer1_selector_low_turnover_owner_decision_pack.md"
)
DEFAULT_LAYER1_SELECTOR_PAUSE_OR_CONTINUE_OWNER_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "layer1_selector_pause_or_continue_owner_pack.md"
)

BlendPath = dict[str, dict[str, float]]

REQUIRED_SELECTOR_IDS = {
    "always_equal_risk",
    "always_100_qqq",
    "trend_200dma_selector",
    "trend_100_200dma_selector",
    "realized_vol_selector",
    "drawdown_guard_selector",
    "trend_plus_vol_selector",
    "trend_plus_drawdown_selector",
    "three_signal_vote_selector",
    "soft_blend_trend_selector",
    "soft_blend_vol_selector",
    "soft_blend_trend_vol_drawdown_selector",
}
REQUIRED_SELECTOR_FIELDS = {
    "selector_id",
    "selector_type",
    "allowed_components",
    "feature_inputs",
    "decision_rule",
    "switching_constraint",
    "minimum_holding_period",
    "cooldown_days",
    "max_switches_per_year",
    "max_turnover_per_switch",
    "uses_future_data",
    "uses_ml",
    "uses_options",
    "paper_shadow_allowed",
    "production_allowed",
    "broker_action",
}


def run_layer1_simple_rule_selector_registry_review(
    *,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry = _load_registry(registry_config_path)
    selectors = _selector_rows(registry)
    review_rows = [_selector_registry_review_row(row, registry) for row in selectors]
    blocker_rows = [row for row in review_rows if row["status"] == "BLOCKED"]
    missing_required = sorted(REQUIRED_SELECTOR_IDS - {row["selector_id"] for row in review_rows})
    if blocker_rows:
        status = "SELECTOR_REGISTRY_BLOCKED"
    elif missing_required:
        status = "SELECTOR_REGISTRY_PARTIAL"
    else:
        status = "SELECTOR_REGISTRY_READY"
    payload = _payload(
        report_type="layer1_simple_rule_selector_registry_review",
        title="Layer-1 Simple Rule Selector Registry Review",
        status=status,
        summary={
            "selector_count": len(review_rows),
            "required_selector_count": len(REQUIRED_SELECTOR_IDS),
            "missing_required_selector_count": len(missing_required),
            "blocked_selector_count": len(blocker_rows),
            "selectable_component_ids": list(SELECTABLE_COMPONENT_IDS),
            "reference_only_component_ids": list(REFERENCE_COMPONENT_IDS),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        registry_config_path=str(registry_config_path),
        selector_registry_rows=review_rows,
        missing_required_selectors=missing_required,
        blocked_selectors=[row["selector_id"] for row in blocker_rows],
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_trend_rule_selector_backtest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_trend_rule_selector_backtest",
            "Layer-1 Trend Rule Selector Backtest",
            "TREND_SELECTOR_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_ids = [
        "trend_200dma_selector",
        "trend_100_200dma_selector",
        "trend_distance_200dma_selector",
    ]
    rows = [_required_metric_row(context, registry, selector_id) for selector_id in selector_ids]
    status = _edge_status(
        rows,
        edge_status="TREND_SELECTOR_EDGE_FOUND",
        no_edge_status="TREND_SELECTOR_NO_EDGE",
        mixed_status="TREND_SELECTOR_MIXED",
        registry=registry,
    )
    payload = _selector_report_payload(
        report_type="layer1_trend_rule_selector_backtest",
        title="Layer-1 Trend Rule Selector Backtest",
        status=status,
        context=context,
        registry=registry,
        rows_field="trend_selector_rows",
        rows=rows,
        extra_summary={"selector_family": "trend"},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_volatility_rule_selector_backtest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_volatility_rule_selector_backtest",
            "Layer-1 Volatility Rule Selector Backtest",
            "VOL_SELECTOR_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_ids = [
        "realized_vol_selector",
        "realized_vol_60d_selector",
        "volatility_expansion_selector",
    ]
    rows = []
    for selector_id in selector_ids:
        row = _required_metric_row(context, registry, selector_id)
        selector = _selector_by_id(registry, selector_id)
        rule = _mapping(selector.get("decision_rule"))
        false_counts = _false_signal_counts(context, _selector_path(context, registry, selector_id))
        row.update(
            {
                "vol_window": rule.get("vol_window", "expansion"),
                "vol_threshold": rule.get("high_percentile")
                or rule.get("expansion_threshold"),
                "false_defensive_periods": false_counts["false_defensive_periods"],
                "false_risk_on_periods": false_counts["false_risk_on_periods"],
            }
        )
        rows.append(row)
    status = _edge_status(
        rows,
        edge_status="VOL_SELECTOR_EDGE_FOUND",
        no_edge_status="VOL_SELECTOR_NO_EDGE",
        mixed_status="VOL_SELECTOR_MIXED",
        registry=registry,
    )
    payload = _selector_report_payload(
        report_type="layer1_volatility_rule_selector_backtest",
        title="Layer-1 Volatility Rule Selector Backtest",
        status=status,
        context=context,
        registry=registry,
        rows_field="volatility_selector_rows",
        rows=rows,
        extra_summary={"selector_family": "volatility"},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_drawdown_rule_selector_backtest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_drawdown_rule_selector_backtest",
            "Layer-1 Drawdown Rule Selector Backtest",
            "DRAWDOWN_SELECTOR_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_ids = ["drawdown_guard_selector", "drawdown_guard_10pct_selector"]
    rows = []
    for selector_id in selector_ids:
        path = _selector_path(context, registry, selector_id)
        row = _required_metric_row(context, registry, selector_id, path=path)
        selector = _selector_by_id(registry, selector_id)
        rule = _mapping(selector.get("decision_rule"))
        diagnostics = _drawdown_diagnostics(context, path)
        row.update(
            {
                "drawdown_threshold": rule.get("drawdown_threshold"),
                "recovery_rule": rule.get("recovery_rule"),
                "drawdown_reduction_vs_100_qqq": diagnostics["drawdown_reduction_vs_100_qqq"],
                "missed_rebound_cost": diagnostics["missed_rebound_cost"],
                "late_risk_on_count": diagnostics["late_risk_on_count"],
                "late_risk_off_count": diagnostics["late_risk_off_count"],
            }
        )
        rows.append(row)
    policy = _evaluation_policy(registry)
    best_row = _best_metric_row(rows)
    if not rows:
        status = "DRAWDOWN_SELECTOR_BLOCKED"
    elif _float(best_row.get("missed_rebound_cost")) > _float(
        best_row.get("net_return_after_cost")
    ):
        status = "DRAWDOWN_SELECTOR_OVER_DEFENSIVE"
    elif _float(best_row.get("drawdown_reduction_vs_100_qqq")) > 0.0:
        status = "DRAWDOWN_SELECTOR_EDGE_FOUND"
    elif _float(policy.get("edge_net_return_threshold")) <= _float(
        best_row.get("net_return_after_cost")
    ):
        status = "DRAWDOWN_SELECTOR_NO_EDGE"
    else:
        status = "DRAWDOWN_SELECTOR_BLOCKED"
    payload = _selector_report_payload(
        report_type="layer1_drawdown_rule_selector_backtest",
        title="Layer-1 Drawdown Rule Selector Backtest",
        status=status,
        context=context,
        registry=registry,
        rows_field="drawdown_selector_rows",
        rows=rows,
        extra_summary={"selector_family": "drawdown"},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_combined_simple_rule_selector_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_combined_simple_rule_selector_search",
            "Layer-1 Combined Simple Rule Selector Search",
            "COMBINED_SELECTOR_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_ids = [
        "trend_200dma_selector",
        "realized_vol_selector",
        "drawdown_guard_selector",
        "trend_plus_vol_selector",
        "trend_plus_drawdown_selector",
        "three_signal_vote_selector",
        "soft_blend_trend_vol_drawdown_selector",
    ]
    rows = []
    for selector_id in selector_ids:
        row = _required_metric_row(context, registry, selector_id)
        selector = _selector_by_id(registry, selector_id)
        path = _selector_path(context, registry, selector_id)
        row.update(
            {
                "candidate_selector_id": selector_id,
                "rule_description": _mapping(selector.get("decision_rule")).get("description"),
                "features_used": list(selector.get("feature_inputs", [])),
                "complexity_score": _complexity_score(selector),
                "selected_component_distribution": _component_distribution(path),
            }
        )
        rows.append(row)
    best_row = _best_metric_row(rows)
    overfit_risk = _float(best_row.get("complexity_score")) > 4.0
    if overfit_risk:
        status = "COMBINED_SELECTOR_OVERFIT_RISK"
    else:
        status = _edge_status(
            rows,
            edge_status="COMBINED_SELECTOR_CANDIDATES_FOUND",
            no_edge_status="COMBINED_SELECTOR_NO_EDGE",
            mixed_status="COMBINED_SELECTOR_NO_EDGE",
            registry=registry,
        )
    payload = _selector_report_payload(
        report_type="layer1_combined_simple_rule_selector_search",
        title="Layer-1 Combined Simple Rule Selector Search",
        status=status,
        context=context,
        registry=registry,
        rows_field="combined_selector_rows",
        rows=rows,
        extra_summary={
            "selector_family": "combined_simple_rule",
            "ml_model_used": False,
            "future_outcome_feature_used": False,
            "unbounded_parameter_search": False,
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_cost_latency_stress(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_cost_latency_stress",
            "Layer-1 Selector Cost and Latency Stress",
            "SELECTOR_COST_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_id = _best_selector_id_for_registry(context, registry)
    base_path = _selector_path(context, registry, selector_id)
    base_metrics = _evaluate_blend_path(context, base_path)
    rows = []
    for scenario in _cost_latency_scenarios():
        path = _scenario_path(base_path, scenario["scenario"])
        metrics = _evaluate_blend_path(
            context,
            path,
            cost_bps=scenario["cost_bps"],
            execution_lag_days=scenario["execution_lag_days"],
        )
        degradation = _float(base_metrics["net_return_after_cost"]) - _float(
            metrics["net_return_after_cost"]
        )
        rows.append(
            {
                "selector_id": selector_id,
                "scenario": scenario["scenario"],
                "gross_return": _round(metrics["gross_return"]),
                "net_return_after_cost": _round(metrics["net_return_after_cost"]),
                "cost_drag": _round(metrics["cost_drag"]),
                "latency_drag": _round(max(degradation, 0.0)),
                "turnover": _round(metrics["turnover"]),
                "switch_count": metrics["switch_count"],
                "performance_degradation": _round(degradation),
                "cost_sensitivity_score": _round(
                    _ratio(degradation, abs(_float(base_metrics["net_return_after_cost"])) + 1e-9)
                ),
            }
        )
    max_degradation = max((_float(row["performance_degradation"]) for row in rows), default=0.0)
    if max_degradation > 0.10:
        status = "SELECTOR_COST_TOO_HIGH"
    elif max_degradation > 0.03:
        status = "SELECTOR_COST_SENSITIVE"
    else:
        status = "SELECTOR_COST_ROBUST"
    payload = _selector_report_payload(
        report_type="layer1_selector_cost_latency_stress",
        title="Layer-1 Selector Cost and Latency Stress",
        status=status,
        context=context,
        registry=registry,
        rows_field="cost_latency_stress_rows",
        rows=rows,
        extra_summary={"selector_id": selector_id, "max_performance_degradation": max_degradation},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_period_split_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_period_split_validation",
            "Layer-1 Selector Period Split Validation",
            "SELECTOR_PERIOD_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_ids = _ranking_selector_ids(registry)
    rows = []
    for period in _period_windows(context):
        period_metrics = []
        for selector_id in selector_ids:
            path = _path_in_window(
                _selector_path(context, registry, selector_id),
                period["start"],
                period["end"],
            )
            metrics = _evaluate_blend_path(context, path)
            period_metrics.append((selector_id, metrics))
        ranks = _rank_by_metric(period_metrics)
        for selector_id, metrics in period_metrics:
            row = _period_metric_row(context, selector_id, period, metrics, ranks[selector_id])
            rows.append(row)
    positive_periods = sum(1 for row in rows if _float(row["net_return"]) > 0.0)
    positive_ratio = _ratio(positive_periods, len(rows))
    required_ratio = _float(_evaluation_policy(registry).get("period_positive_min_ratio"), 0.5)
    if positive_ratio >= 0.75:
        status = "SELECTOR_PERIOD_ROBUST"
    elif positive_ratio >= required_ratio:
        status = "SELECTOR_PERIOD_MIXED"
    else:
        status = "SELECTOR_PERIOD_CONCENTRATED"
    payload = _selector_report_payload(
        report_type="layer1_selector_period_split_validation",
        title="Layer-1 Selector Period Split Validation",
        status=status,
        context=context,
        registry=registry,
        rows_field="period_split_rows",
        rows=rows,
        extra_summary={
            "selector_count": len(selector_ids),
            "positive_period_ratio": _round(positive_ratio),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_drawdown_episode_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_drawdown_episode_review",
            "Layer-1 Selector Drawdown Episode Review",
            "SELECTOR_EPISODE_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_id = _best_selector_id_for_registry(context, registry)
    path = _selector_path(context, registry, selector_id)
    rows = [
        _episode_row(context, selector_id, path, episode)
        for episode in _episode_windows(context)
    ]
    risk_count = sum(
        1
        for row in rows
        if row["missed_rebound_flag"] or row["late_risk_off_flag"] or row["late_risk_on_flag"]
    )
    if risk_count > len(rows) // 2:
        status = "SELECTOR_EPISODE_RISK_MATERIAL"
    elif risk_count:
        status = "SELECTOR_EPISODE_MIXED"
    else:
        status = "SELECTOR_EPISODE_REVIEW_READY"
    payload = _selector_report_payload(
        report_type="layer1_selector_drawdown_episode_review",
        title="Layer-1 Selector Drawdown Episode Review",
        status=status,
        context=context,
        registry=registry,
        rows_field="episode_review_rows",
        rows=rows,
        extra_summary={"selector_id": selector_id, "risk_flag_count": risk_count},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_regret_attribution(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_regret_attribution",
            "Layer-1 Selector Regret Attribution",
            "REGRET_ATTRIBUTION_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_id = _best_selector_id_for_registry(context, registry)
    rows = _regret_attribution_rows(context, _selector_path(context, registry, selector_id))
    material_count = sum(1 for row in rows if _float(row["worst_regret"]) > 0.01)
    if material_count:
        status = "REGRET_MATERIAL"
    elif any(_int(row["count"]) for row in rows):
        status = "REGRET_ATTRIBUTION_READY"
    else:
        status = "REGRET_ACCEPTABLE"
    payload = _selector_report_payload(
        report_type="layer1_selector_regret_attribution",
        title="Layer-1 Selector Regret Attribution",
        status=status,
        context=context,
        registry=registry,
        rows_field="regret_attribution_rows",
        rows=rows,
        extra_summary={"selector_id": selector_id, "material_regret_type_count": material_count},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_vs_component_baseline_ranking(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_vs_component_baseline_ranking",
            "Layer-1 Selector vs Component Baseline Ranking",
            "SELECTOR_RANKING_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _ranking_rows(context, registry)
    selector_rows = [row for row in rows if row["candidate_type"].endswith("selector")]
    dominated = [row for row in selector_rows if row["dominance_status"] == "DOMINATED"]
    if len(dominated) == len(selector_rows):
        status = "SELECTOR_DOMINATED_BY_COMPONENTS"
    elif dominated:
        status = "SELECTOR_RANKING_MIXED"
    else:
        status = "SELECTOR_RANKING_READY"
    payload = _selector_report_payload(
        report_type="layer1_selector_vs_component_baseline_ranking",
        title="Layer-1 Selector vs Component Baseline Ranking",
        status=status,
        context=context,
        registry=registry,
        rows_field="ranking_rows",
        rows=rows,
        extra_summary={"selector_candidate_count": len(selector_rows)},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_overfit_sensitivity_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_overfit_sensitivity_review",
            "Layer-1 Selector Overfit Sensitivity Review",
            "SELECTOR_SENSITIVITY_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_id = _best_selector_id_for_registry(context, registry)
    rows = _sensitivity_rows(context, registry, selector_id)
    worst_degradation = max((_float(row["metric_degradation"]) for row in rows), default=0.0)
    policy = _evaluation_policy(registry)
    if worst_degradation >= _float(policy.get("overfit_metric_degradation_block"), 0.15):
        status = "SELECTOR_FRAGILE"
    elif worst_degradation >= _float(policy.get("overfit_metric_degradation_warn"), 0.05):
        status = "SELECTOR_SENSITIVITY_MIXED"
    else:
        status = "SELECTOR_ROBUST"
    payload = _selector_report_payload(
        report_type="layer1_selector_overfit_sensitivity_review",
        title="Layer-1 Selector Overfit Sensitivity Review",
        status=status,
        context=context,
        registry=registry,
        rows_field="sensitivity_rows",
        rows=rows,
        extra_summary={"selector_id": selector_id, "worst_metric_degradation": worst_degradation},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_minimum_holding_period_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_minimum_holding_period_review",
            "Layer-1 Selector Minimum Holding Period Review",
            "HOLDING_PERIOD_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    selector_id = _best_selector_id_for_registry(context, registry)
    rows = []
    base_path = _selector_path(context, registry, selector_id, minimum_holding_period=20)
    base_metrics = _evaluate_blend_path(context, base_path)
    for holding_period in (5, 10, 20, 40, 60):
        path = _selector_path(
            context,
            registry,
            selector_id,
            minimum_holding_period=holding_period,
            cooldown_days=0,
        )
        metrics = _evaluate_blend_path(context, path)
        missed_signal_cost = _float(base_metrics["net_return_after_cost"]) - _float(
            metrics["net_return_after_cost"]
        )
        rows.append(
            {
                "minimum_holding_period": holding_period,
                "selector_id": selector_id,
                "net_return_after_cost": _round(metrics["net_return_after_cost"]),
                "max_drawdown": _round(metrics["max_drawdown"]),
                "turnover": _round(metrics["turnover"]),
                "switch_count": metrics["switch_count"],
                "avg_holding_period": _round(metrics["avg_holding_period"]),
                "missed_signal_cost": _round(missed_signal_cost),
                "chop_reduction_benefit": _round(
                    _float(base_metrics["switch_count"]) - _float(metrics["switch_count"])
                ),
            }
        )
    recommended = _recommended_holding_period(rows, registry)
    for row in rows:
        row["recommended_minimum_holding_period"] = recommended
    if recommended < _int(_evaluation_policy(registry).get("minimum_acceptable_holding_period")):
        status = "HOLDING_PERIOD_TOO_SHORT_RISK"
    elif rows:
        status = "HOLDING_PERIOD_READY"
    else:
        status = "HOLDING_PERIOD_NO_EDGE"
    payload = _selector_report_payload(
        report_type="layer1_selector_minimum_holding_period_review",
        title="Layer-1 Selector Minimum Holding Period Review",
        status=status,
        context=context,
        registry=registry,
        rows_field="minimum_holding_period_rows",
        rows=rows,
        extra_summary={
            "selector_id": selector_id,
            "recommended_minimum_holding_period": recommended,
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_forward_aging_watchlist_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_forward_aging_watchlist_gate",
            "Layer-1 Selector Forward-Aging Watchlist Gate",
            "SELECTOR_WATCHLIST_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _ranking_rows(context, registry)
    candidate = _best_watchlist_candidate(rows)
    checks = _watchlist_checks(context, registry, candidate)
    blocking_reasons = [row["check_id"] for row in checks if row["status"] == "FAIL"]
    if not candidate:
        status = "NO_SELECTOR_WATCHLIST_CANDIDATE"
    elif blocking_reasons:
        status = "SELECTOR_WATCHLIST_NEEDS_OWNER_REVIEW"
    else:
        status = "SELECTOR_FORWARD_WATCHLIST_READY"
    row = {
        "selector_id": candidate.get("candidate_id") if candidate else None,
        "watchlist_status": status,
        "watchlist_reason": _watchlist_reason(status, candidate),
        "blocking_reasons": blocking_reasons,
        "required_forward_days": _evaluation_policy(registry).get("required_forward_days"),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }
    payload = _selector_report_payload(
        report_type="layer1_selector_forward_aging_watchlist_gate",
        title="Layer-1 Selector Forward-Aging Watchlist Gate",
        status=status,
        context=context,
        registry=registry,
        rows_field="watchlist_gate_rows",
        rows=[row],
        extra_summary={
            "selector_id": row["selector_id"],
            "blocking_reason_count": len(blocking_reasons),
        },
        extra_payload={"watchlist_checks": checks},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_owner_decision_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    gate = run_layer1_selector_forward_aging_watchlist_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    status = "LAYER1_SELECTOR_OWNER_DECISION_PACK_READY"
    recommendation = _owner_recommendation_from_gate(gate)
    gate_summary = _mapping(gate.get("summary"))
    payload = _payload(
        report_type="layer1_selector_owner_decision_pack",
        title="Layer-1 Selector Owner Decision Pack",
        status=status,
        summary={
            "owner_recommendation": recommendation,
            "watchlist_gate_status": gate.get("status"),
            "selector_id": gate_summary.get("selector_id"),
            "data_quality_status": gate_summary.get("data_quality_status"),
            "actual_requested_date_range": gate_summary.get("actual_requested_date_range"),
            "selector_registry_version": gate_summary.get("selector_registry_version"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        owner_recommendation=recommendation,
        required_answers=_owner_decision_answers(gate, recommendation),
        source_artifacts={"watchlist_gate": gate.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_simple_rule_selector_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry_review = run_layer1_simple_rule_selector_registry_review(
        registry_config_path=registry_config_path,
        output_root=output_root,
    )
    ranking = run_layer1_selector_vs_component_baseline_ranking(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    watchlist = run_layer1_selector_forward_aging_watchlist_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    if watchlist.get("status") == "SELECTOR_FORWARD_WATCHLIST_READY":
        status = "LAYER1_SELECTOR_FORWARD_AGING_READY"
    elif ranking.get("status") == "SELECTOR_DOMINATED_BY_COMPONENTS":
        status = "LAYER1_SELECTOR_NO_EDGE"
    elif registry_review.get("status") == "SELECTOR_REGISTRY_BLOCKED":
        status = "LAYER1_SELECTOR_BLOCKED"
    else:
        status = "LAYER1_SELECTOR_RESEARCH_ONLY"
    ranking_summary = _mapping(ranking.get("summary"))
    watchlist_summary = _mapping(watchlist.get("summary"))
    payload = _payload(
        report_type="layer1_simple_rule_selector_master_review",
        title="Layer-1 Simple Rule Selector Master Review",
        status=status,
        summary={
            "registry_status": registry_review.get("status"),
            "ranking_status": ranking.get("status"),
            "watchlist_status": watchlist.get("status"),
            "data_quality_status": watchlist_summary.get("data_quality_status")
            or ranking_summary.get("data_quality_status"),
            "actual_requested_date_range": watchlist_summary.get("actual_requested_date_range")
            or ranking_summary.get("actual_requested_date_range"),
            "selector_registry_version": watchlist_summary.get("selector_registry_version")
            or ranking_summary.get("selector_registry_version"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "next_minimum_task": "owner review before any forward-aging watchlist admission",
        },
        required_answers=[
            {
                "question": "Layer-1 simple-rule selector 是否有研究价值？",
                "answer": "YES_RESEARCH_ONLY" if status != "LAYER1_SELECTOR_NO_EDGE" else "NO_EDGE",
            },
            {
                "question": "是否存在 watchlist candidate？",
                "answer": str(watchlist.get("status")),
            },
            {"question": "是否值得继续扩展第一层研究？", "answer": "OWNER_REVIEW_REQUIRED"},
            {"question": "是否仍禁止 ML selector？", "answer": "YES"},
            {"question": "是否需要更多 Layer-2 growth components？", "answer": "POSSIBLY"},
            {
                "question": "是否继续以 equal_risk_qqq_sgov 作为 defensive primary？",
                "answer": "YES",
            },
            {"question": "是否继续以 100_qqq 作为 hard benchmark？", "answer": "YES"},
            {
                "question": "下一阶段最小任务是什么？",
                "answer": "owner review and research-only forward-aging watchlist decision",
            },
        ],
        source_artifacts={
            "registry_review": registry_review.get("artifact_paths", {}),
            "ranking": ranking.get("artifact_paths", {}),
            "watchlist": watchlist.get("artifact_paths", {}),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_real_result_summary(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_real_result_summary",
            "Layer-1 Selector Real Result Summary",
            "LAYER1_SELECTOR_RESULT_SUMMARY_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    top = _top_simple_rule_selector_row(context, registry)
    period = run_layer1_selector_period_split_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    sensitivity = run_layer1_selector_overfit_sensitivity_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    watchlist = run_layer1_selector_forward_aging_watchlist_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    owner = run_layer1_selector_owner_decision_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    if not top:
        status = "LAYER1_SELECTOR_RESULT_SUMMARY_BLOCKED"
    elif _cost_after_edge_exists(top):
        status = "LAYER1_SELECTOR_RESULT_SUMMARY_READY"
    else:
        status = "LAYER1_SELECTOR_RESULT_SUMMARY_INCONCLUSIVE"
    top_summary = _top_selector_summary_fields(top)
    top_summary.update(
        {
            "period_split_status": period.get("status"),
            "sensitivity_status": sensitivity.get("status"),
            "watchlist_status": watchlist.get("status"),
            "owner_review_status": owner.get("status"),
        }
    )
    payload = _payload(
        report_type="layer1_selector_real_result_summary",
        title="Layer-1 Selector Real Result Summary",
        status=status,
        summary={
            **top_summary,
            "data_quality_status": context.get("data_quality_status"),
            "actual_requested_date_range": _actual_date_range(context),
            "selector_registry_version": registry.get("registry_version"),
            "cost_after_edge_exists": _cost_after_edge_exists(top),
            "outperforms_always_equal_risk": _float(
                top.get("relative_vs_always_equal_risk")
            )
            > 0.0,
            "outperforms_always_100_qqq": _float(top.get("relative_vs_always_100_qqq"))
            > 0.0,
            "turnover_acceptable": _turnover_acceptable(context, registry, top),
            "watchlist_candidate_available": _mapping(watchlist.get("summary")).get(
                "selector_id"
            )
            is not None,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        result_summary=top_summary,
        selector_result_ranking=_simple_rule_selector_rows(context, registry),
        required_answers=_real_result_required_answers(top, watchlist),
        source_artifacts={
            "period_split_validation": period.get("artifact_paths", {}),
            "sensitivity_review": sensitivity.get("artifact_paths", {}),
            "watchlist_gate": watchlist.get("artifact_paths", {}),
            "owner_decision_pack": owner.get("artifact_paths", {}),
            "required_upstream_reports": _layer1_result_review_required_sources(output_root),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_history_coverage_gap_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_history_coverage_gap_audit",
            "Layer-1 Selector History Coverage Gap Audit",
            "HISTORY_COVERAGE_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    coverage = _history_coverage_summary(context, registry, prices_path)
    status = str(coverage["research_conclusion_strength"])
    payload = _payload(
        report_type="layer1_selector_history_coverage_gap_audit",
        title="Layer-1 Selector History Coverage Gap Audit",
        status=status,
        summary={
            "available_start_date": coverage["available_start_date"],
            "available_end_date": coverage["available_end_date"],
            "expected_start_date": coverage["expected_start_date"],
            "coverage_gap_days": coverage["coverage_gap_days"],
            "coverage_gap_reason": coverage["coverage_gap_reason"],
            "affected_features": coverage["affected_features"],
            "affected_components": coverage["affected_components"],
            "can_backfill_to_2012": coverage["can_backfill_to_2012"],
            "backfill_requirements": coverage["backfill_requirements"],
            "research_conclusion_strength": coverage["research_conclusion_strength"],
            "data_quality_status": context.get("data_quality_status"),
            "actual_requested_date_range": _actual_date_range(context),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        coverage_gap_audit=coverage,
        source_artifacts=context.get("source_artifacts", {}),
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_recent_regime_risk_disclosure(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_recent_regime_risk_disclosure",
            "Layer-1 Selector Recent Regime Risk Disclosure",
            "RECENT_REGIME_RISK_BLOCKED",
            context,
            output_root,
        )
    disclosure = _recent_regime_disclosure(context)
    status = (
        "RECENT_REGIME_RISK_MATERIAL"
        if disclosure["missing_regime_list"]
        else "RECENT_REGIME_RISK_DISCLOSED"
    )
    payload = _payload(
        report_type="layer1_selector_recent_regime_risk_disclosure",
        title="Layer-1 Selector Recent Regime Risk Disclosure",
        status=status,
        summary={
            "regime_coverage_summary": disclosure["regime_coverage_summary"],
            "missing_regime_list": disclosure["missing_regime_list"],
            "risk_of_overstating_selector_edge": disclosure[
                "risk_of_overstating_selector_edge"
            ],
            "risk_of_ai_rally_bias": disclosure["risk_of_ai_rally_bias"],
            "risk_of_high_rate_sgov_bias": disclosure["risk_of_high_rate_sgov_bias"],
            "recommended_disclosure": disclosure["recommended_disclosure"],
            "data_quality_status": context.get("data_quality_status"),
            "actual_requested_date_range": _actual_date_range(context),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        recent_regime_risk_disclosure=disclosure,
        required_answers=_recent_regime_required_answers(disclosure),
        source_artifacts=context.get("source_artifacts", {}),
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_owner_watchlist_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    owner_doc_path: Path = DEFAULT_LAYER1_SELECTOR_OWNER_WATCHLIST_REVIEW_DOC_PATH,
) -> dict[str, Any]:
    result = run_layer1_selector_real_result_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    coverage = run_layer1_selector_history_coverage_gap_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    recent = run_layer1_selector_recent_regime_risk_disclosure(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    registry = _load_registry(registry_config_path)
    review = _owner_watchlist_review_payload(result, coverage, recent, registry)
    payload = _payload(
        report_type="layer1_selector_owner_watchlist_review",
        title="Layer-1 Selector Owner Watchlist Review",
        status=review["status"],
        summary={
            "data_quality_status": _mapping(result.get("summary")).get("data_quality_status"),
            "candidate_selector_id": review["candidate_selector_id"],
            "watchlist_recommendation": review["watchlist_recommendation"],
            "watchlist_role": review["watchlist_role"],
            "required_forward_days": review["required_forward_days"],
            "history_coverage_warning": review["history_coverage_warning"],
            "recent_regime_warning": review["recent_regime_warning"],
            "blocking_reasons": review["blocking_reasons"],
            "owner_required_actions": review["owner_required_actions"],
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        owner_watchlist_review=review,
        source_artifacts={
            "real_result_summary": result.get("artifact_paths", {}),
            "history_coverage_gap_audit": coverage.get("artifact_paths", {}),
            "recent_regime_risk_disclosure": recent.get("artifact_paths", {}),
        },
        owner_review_doc_path=str(owner_doc_path),
    )
    _write_pair(payload, output_root)
    _copy_markdown_artifact(payload, owner_doc_path)
    return payload


def run_layer1_selector_forward_aging_dry_run(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_forward_aging_dry_run",
            "Layer-1 Selector Forward-Aging Dry Run",
            "LAYER1_SELECTOR_FORWARD_DRY_RUN_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    owner = run_layer1_selector_owner_watchlist_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    candidate_id = _mapping(owner.get("summary")).get("candidate_selector_id")
    if not candidate_id:
        payload = _payload(
            report_type="layer1_selector_forward_aging_dry_run",
            title="Layer-1 Selector Forward-Aging Dry Run",
            status="LAYER1_SELECTOR_FORWARD_DRY_RUN_BLOCKED",
            summary={
                "decision_date": _actual_date_range(context)["end"],
                "selector_id": None,
                "selected_component": None,
                "data_quality_status": context.get("data_quality_status"),
                "observation_written": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "blocked_reason": "no_owner_watchlist_candidate",
            },
            blockers=["no_owner_watchlist_candidate"],
            source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
        )
        _write_pair(payload, output_root)
        return payload
    decision_date = str(_actual_date_range(context)["end"])
    path = _selector_path(context, registry, str(candidate_id))
    component_blend = dict(path.get(decision_date) or {})
    asset_result = _asset_weights_for_selector_blend(context, decision_date, component_blend)
    if not asset_result["available"]:
        status = "LAYER1_SELECTOR_FORWARD_DRY_RUN_BLOCKED"
    elif owner.get("status") == "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING":
        status = "LAYER1_SELECTOR_FORWARD_DRY_RUN_PASS"
    else:
        status = "LAYER1_SELECTOR_FORWARD_DRY_RUN_WARN"
    selector = _selector_by_id(registry, str(candidate_id))
    payload = _payload(
        report_type="layer1_selector_forward_aging_dry_run",
        title="Layer-1 Selector Forward-Aging Dry Run",
        status=status,
        summary={
            "decision_date": decision_date,
            "selector_id": candidate_id,
            "selected_component": _selected_component_label(component_blend),
            "component_blend_weights": component_blend,
            "final_target_weight_qqq": asset_result["weights"]["QQQ"],
            "final_target_weight_tqqq": asset_result["weights"]["TQQQ"],
            "final_target_weight_sgov": asset_result["weights"]["SGOV"],
            "data_quality_status": context.get("data_quality_status"),
            "observation_written": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        decision_date=decision_date,
        selector_id=candidate_id,
        selected_component=_selected_component_label(component_blend),
        component_blend_weights=component_blend,
        final_target_weight_qqq=asset_result["weights"]["QQQ"],
        final_target_weight_tqqq=asset_result["weights"]["TQQQ"],
        final_target_weight_sgov=asset_result["weights"]["SGOV"],
        data_quality_status=context.get("data_quality_status"),
        policy_definition_hash=asset_result["policy_definition_hash"],
        selector_definition_hash=_selector_definition_hash(selector),
        observation_written=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_watchlist_blocker_report(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    owner = run_layer1_selector_owner_watchlist_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    summary = _mapping(owner.get("summary"))
    blocker = _watchlist_blocker_summary(owner)
    payload = _payload(
        report_type="layer1_selector_watchlist_blocker_report",
        title="Layer-1 Selector Watchlist Blocker Report",
        status="WATCHLIST_BLOCKER_REPORT_READY",
        summary={
            "watchlist_allowed": blocker["watchlist_allowed"],
            "watchlist_candidate": summary.get("candidate_selector_id"),
            "blocking_reasons": blocker["blocking_reasons"],
            "warning_reasons": blocker["warning_reasons"],
            "history_backfill_required": blocker["history_backfill_required"],
            "minimum_forward_observations_required": summary.get("required_forward_days"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        watchlist_allowed=blocker["watchlist_allowed"],
        watchlist_candidate=summary.get("candidate_selector_id"),
        blocking_reasons=blocker["blocking_reasons"],
        warning_reasons=blocker["warning_reasons"],
        history_backfill_required=blocker["history_backfill_required"],
        minimum_forward_observations_required=summary.get("required_forward_days"),
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        manual_review_required=True,
        source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_reader_brief_preview(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    owner = run_layer1_selector_owner_watchlist_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    preview = _reader_brief_preview(owner)
    status = (
        "LAYER1_SELECTOR_READER_PREVIEW_SAFE"
        if preview["safe_to_display"]
        else "LAYER1_SELECTOR_READER_PREVIEW_AMBIGUOUS"
    )
    payload = _payload(
        report_type="layer1_selector_reader_brief_preview",
        title="Layer-1 Selector Reader Brief Preview",
        status=status,
        summary={
            "preview_title": preview["preview_title"],
            "top_selector": preview["top_selector"],
            "watchlist_status": preview["watchlist_status"],
            "history_coverage_warning": preview["history_coverage_warning"],
            "recent_regime_warning": preview["recent_regime_warning"],
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "safe_to_display": preview["safe_to_display"],
        },
        reader_brief_preview=preview,
        prohibited_phrase_hits=preview["prohibited_phrase_hits"],
        source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_result_review_master(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    owner_doc_path: Path = DEFAULT_LAYER1_SELECTOR_OWNER_WATCHLIST_REVIEW_DOC_PATH,
    master_doc_path: Path = DEFAULT_LAYER1_SELECTOR_RESULT_REVIEW_MASTER_DOC_PATH,
) -> dict[str, Any]:
    result = run_layer1_selector_real_result_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    coverage = run_layer1_selector_history_coverage_gap_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    recent = run_layer1_selector_recent_regime_risk_disclosure(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    registry = _load_registry(registry_config_path)
    owner = _write_owner_watchlist_review_artifact(
        result=result,
        coverage=coverage,
        recent=recent,
        registry=registry,
        output_root=output_root,
        owner_doc_path=owner_doc_path,
    )
    context = _build_context(
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
    dry_run = _write_forward_aging_dry_run_artifact(
        context=context,
        registry=registry,
        owner=owner,
        output_root=output_root,
    )
    blocker = _write_watchlist_blocker_report_artifact(owner=owner, output_root=output_root)
    preview = _write_reader_brief_preview_artifact(owner=owner, output_root=output_root)
    status = _result_review_master_status(result, coverage, owner, dry_run)
    answers = _result_review_master_answers(result, coverage, recent, owner, dry_run, blocker)
    payload = _payload(
        report_type="layer1_selector_result_review_master",
        title="Layer-1 Selector Result Review Master",
        status=status,
        summary={
            "data_quality_status": _mapping(result.get("summary")).get("data_quality_status"),
            "top_selector_id": _mapping(result.get("summary")).get("top_selector_id"),
            "cost_after_edge_exists": _mapping(result.get("summary")).get(
                "cost_after_edge_exists"
            ),
            "research_conclusion_strength": _mapping(coverage.get("summary")).get(
                "research_conclusion_strength"
            ),
            "recent_regime_status": recent.get("status"),
            "watchlist_recommendation": _mapping(owner.get("summary")).get(
                "watchlist_recommendation"
            ),
            "dry_run_status": dry_run.get("status"),
            "reader_preview_status": preview.get("status"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
            "next_minimum_task": answers[-1]["answer"],
        },
        required_answers=answers,
        source_artifacts={
            "real_result_summary": result.get("artifact_paths", {}),
            "history_coverage_gap_audit": coverage.get("artifact_paths", {}),
            "recent_regime_risk_disclosure": recent.get("artifact_paths", {}),
            "owner_watchlist_review": owner.get("artifact_paths", {}),
            "forward_aging_dry_run": dry_run.get("artifact_paths", {}),
            "watchlist_blocker_report": blocker.get("artifact_paths", {}),
            "reader_brief_preview": preview.get("artifact_paths", {}),
        },
        master_review_doc_path=str(master_doc_path),
    )
    _write_pair(payload, output_root)
    _copy_markdown_artifact(payload, master_doc_path)
    return payload


def run_layer1_selector_turnover_source_diagnosis(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_turnover_source_diagnosis",
            "Layer-1 Selector Turnover Source Diagnosis",
            "TURNOVER_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    path = _selector_path(context, registry, "trend_200dma_selector")
    switch_rows = _turnover_source_rows(context, path)
    diagnosis = _turnover_source_summary(switch_rows)
    status = (
        "TURNOVER_NOISE_DOMINANT"
        if diagnosis["noise_switch_count"] > diagnosis["helpful_switch_count"]
        else "TURNOVER_SOURCE_DIAGNOSED"
    )
    payload = _selector_report_payload(
        report_type="layer1_selector_turnover_source_diagnosis",
        title="Layer-1 Selector Turnover Source Diagnosis",
        status=status,
        context=context,
        registry=registry,
        rows_field="turnover_source_rows",
        rows=switch_rows,
        extra_summary={
            **diagnosis,
            "selector_id": "trend_200dma_selector",
            "near_200dma_band": LOW_TURNOVER_NEAR_200DMA_BAND,
            "buffer_band_missing_likely": diagnosis["near_200dma_switch_share"] >= 0.5,
            "trend_confirmation_insufficient_likely": (
                diagnosis["unconfirmed_switch_share"] >= 0.5
            ),
            "minimum_holding_period_too_short_likely": (
                diagnosis["short_holding_switch_share"] >= 0.5
            ),
        },
        extra_payload={
            "switch_count_by_year": _switch_count_by_year(switch_rows),
            "helpfulness_by_year": _switch_helpfulness_by_year(switch_rows),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_buffered_200dma_variants(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_buffered_200dma_variants",
            "Layer-1 Selector Buffered 200DMA Variants",
            "BUFFERED_200DMA_VARIANTS_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _buffered_200dma_variant_rows(context, registry)
    best = _best_low_turnover_row(rows)
    payload = _selector_report_payload(
        report_type="layer1_selector_buffered_200dma_variants",
        title="Layer-1 Selector Buffered 200DMA Variants",
        status="BUFFERED_200DMA_VARIANTS_REVIEWED",
        context=context,
        registry=registry,
        rows_field="buffered_200dma_variant_rows",
        rows=rows,
        extra_summary={
            "variant_count": len(rows),
            "best_variant_id": best.get("variant_id"),
            "best_variant_family": best.get("variant_family"),
            "best_turnover_reduction": best.get("turnover_reduction"),
            "best_net_return_after_cost": best.get("net_return_after_cost"),
            "best_max_drawdown": best.get("max_drawdown"),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_min_holding_cooldown_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_min_holding_cooldown_review",
            "Layer-1 Selector Minimum Holding And Cooldown Review",
            "MIN_HOLDING_COOLDOWN_REVIEW_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _min_holding_cooldown_rows(context, registry)
    acceptable_rows = [row for row in rows if row["turnover_acceptable"]]
    best = _best_low_turnover_row(acceptable_rows or rows)
    status = (
        "MIN_HOLDING_COOLDOWN_ACCEPTABLE_FOUND"
        if acceptable_rows
        else "MIN_HOLDING_COOLDOWN_REVIEWED"
    )
    payload = _selector_report_payload(
        report_type="layer1_selector_min_holding_cooldown_review",
        title="Layer-1 Selector Minimum Holding And Cooldown Review",
        status=status,
        context=context,
        registry=registry,
        rows_field="minimum_holding_cooldown_rows",
        rows=rows,
        extra_summary={
            "variant_count": len(rows),
            "turnover_acceptable_count": len(acceptable_rows),
            "best_variant_id": best.get("variant_id"),
            "best_minimum_holding_period": best.get("minimum_holding_period"),
            "best_cooldown_after_switch": best.get("cooldown_after_switch"),
            "best_max_switches_per_year": best.get("max_switches_per_year"),
            "best_net_return_after_cost": best.get("net_return_after_cost"),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_soft_blend_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_soft_blend_review",
            "Layer-1 Selector Soft Blend Review",
            "SOFT_BLEND_REVIEW_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    path = _soft_blend_200dma_path(context, registry)
    metrics = _evaluate_blend_path(context, path)
    opportunity = _opportunity_costs(context, path)
    row = _low_turnover_variant_row(
        context,
        "soft_blend_200dma_three_state",
        "soft_blend_selector",
        path,
        _selector_path(context, registry, "trend_200dma_selector"),
        extra={
            "strong_risk_on_weight_100qqq": 0.80,
            "normal_weight_100qqq": 0.50,
            "risk_off_weight_100qqq": 0.20,
        },
    )
    payload = _selector_report_payload(
        report_type="layer1_selector_soft_blend_review",
        title="Layer-1 Selector Soft Blend Review",
        status="SOFT_BLEND_REVIEWED",
        context=context,
        registry=registry,
        rows_field="soft_blend_rows",
        rows=[row],
        extra_summary={
            "variant_id": row["variant_id"],
            "turnover": row["turnover"],
            "net_return_after_cost": row["net_return_after_cost"],
            "drawdown": row["max_drawdown"],
            "missed_upside": _round(opportunity["missed_rebound_cost"]),
            "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
            "blend_weight_path_sample": _blend_weight_path_sample(path),
        },
        extra_payload={"blend_weight_path": _blend_weight_path(path)},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_low_turnover_ranking(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_low_turnover_ranking",
            "Layer-1 Selector Low-Turnover Ranking",
            "LOW_TURNOVER_RANKING_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _low_turnover_ranking_rows(context, registry)
    summary = _low_turnover_ranking_summary(rows)
    payload = _selector_report_payload(
        report_type="layer1_selector_low_turnover_ranking",
        title="Layer-1 Selector Low-Turnover Ranking",
        status="LOW_TURNOVER_SELECTOR_RANKED",
        context=context,
        registry=registry,
        rows_field="low_turnover_ranking_rows",
        rows=rows,
        extra_summary=summary,
        extra_payload={
            "dominated_variants": [
                row["variant_id"] for row in rows if row["dominance_status"] == "DOMINATED"
            ],
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_low_turnover_owner_decision_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    owner_doc_path: Path = DEFAULT_LAYER1_SELECTOR_LOW_TURNOVER_OWNER_DECISION_DOC_PATH,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_low_turnover_owner_decision_pack",
            "Layer-1 Selector Low-Turnover Owner Decision Pack",
            "BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    diagnosis_rows = _turnover_source_rows(
        context,
        _selector_path(context, registry, "trend_200dma_selector"),
    )
    ranking_rows = _low_turnover_ranking_rows(context, registry)
    recent = _recent_regime_disclosure(context)
    recent_status = _recent_regime_status(recent)
    decision = low_turnover_owner_decision(
        actual_date_range=_actual_date_range(context),
        data_quality_status=context.get("data_quality_status"),
        registry=registry,
        ranking_rows=ranking_rows,
    )
    status = decision["status"]
    payload = _payload(
        report_type="layer1_selector_low_turnover_owner_decision_pack",
        title="Layer-1 Selector Low-Turnover Owner Decision Pack",
        status=status,
        summary={
            "data_quality_status": context.get("data_quality_status"),
            "actual_requested_date_range": _actual_date_range(context),
            "recommended_low_turnover_candidate": decision["candidate_id"],
            "decision": status,
            "decision_reasons": decision["decision_reasons"],
            "original_turnover": decision["original_turnover"],
            "candidate_turnover": decision["candidate_turnover"],
            "candidate_switch_count": decision["candidate_switch_count"],
            "candidate_net_return_after_cost": decision["candidate_net_return_after_cost"],
            "candidate_relative_vs_equal_risk": decision["candidate_relative_vs_equal_risk"],
            "candidate_relative_vs_100_qqq": decision["candidate_relative_vs_100_qqq"],
            "recent_regime_status": recent_status,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        selector_registry_version=registry.get("registry_version"),
        turnover_source_summary=_turnover_source_summary(diagnosis_rows),
        low_turnover_ranking_rows=ranking_rows,
        owner_decision_checks=decision["checks"],
        recent_regime_disclosure={**recent, "status": recent_status},
        source_artifacts=context.get("source_artifacts", {}),
        owner_decision_doc_path=str(owner_doc_path),
    )
    _write_pair(payload, output_root)
    _copy_markdown_artifact(payload, owner_doc_path)
    return payload


def run_layer1_selector_switch_count_threshold_contract(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_switch_count_threshold_contract",
            "Layer-1 Selector Switch Count Threshold Contract",
            "SWITCH_COUNT_CONTRACT_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    policy = _evaluation_policy(registry)
    contract = switch_count_control_contract(policy)
    original = _selector_path(context, registry, "trend_200dma_selector")
    soft = _soft_blend_200dma_path(context, registry)
    rows = [
        _low_turnover_variant_row(
            context,
            "original_trend_200dma_selector",
            "original trend_200dma_selector",
            original,
            original,
        ),
        _low_turnover_variant_row(
            context,
            "soft_blend_200dma_three_state",
            "soft_blend_selector",
            soft,
            original,
            extra=_soft_blend_parameter_fields(0.80, 0.50, 0.20, LOW_TURNOVER_NEAR_200DMA_BAND, 1),
        ),
    ]
    status = "SWITCH_COUNT_CONTRACT_READY"
    payload = _selector_report_payload(
        report_type="layer1_selector_switch_count_threshold_contract",
        title="Layer-1 Selector Switch Count Threshold Contract",
        status=status,
        context=context,
        registry=registry,
        rows_field="switch_count_contract_rows",
        rows=rows,
        extra_summary={
            "switch_count_controlled_definition": (
                "max calendar-year switches, rolling three-year switches, annual turnover, "
                "and average holding period must all satisfy the configured contract"
            ),
            "contract_version": contract.get("version"),
            "max_switches_per_year": contract["max_switches_per_year"],
            "max_switches_per_3y": contract["max_switches_per_3y"],
            "max_turnover_per_year": contract["max_turnover_per_year"],
            "min_avg_holding_period": contract["min_avg_holding_period"],
            "allowed_exception_cases": contract["allowed_exception_cases"],
        },
        extra_payload={"switch_count_control_contract": contract},
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_soft_blend_constrained_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_soft_blend_constrained_search",
            "Layer-1 Selector Soft Blend Constrained Search",
            "SOFT_BLEND_CONSTRAINED_SEARCH_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _soft_blend_constrained_search_rows(context, registry)
    controlled = [row for row in rows if row["switch_count_controlled"]]
    best = _best_low_turnover_row(controlled or rows)
    status = (
        "SOFT_BLEND_CONSTRAINED_ACCEPTABLE_FOUND"
        if controlled
        else "SOFT_BLEND_CONSTRAINED_SEARCH_REVIEWED"
    )
    payload = _selector_report_payload(
        report_type="layer1_selector_soft_blend_constrained_search",
        title="Layer-1 Selector Soft Blend Constrained Search",
        status=status,
        context=context,
        registry=registry,
        rows_field="soft_blend_constrained_search_rows",
        rows=rows,
        extra_summary={
            "search_scope": "soft_blend_200dma_three_state_only",
            "variant_count": len(rows),
            "switch_count_controlled_count": len(controlled),
            "best_variant_id": best.get("variant_id"),
            "best_net_return_after_cost": best.get("net_return_after_cost"),
            "best_max_drawdown": best.get("max_drawdown"),
            "best_calmar": best.get("calmar"),
            "best_sharpe": best.get("sharpe"),
            "best_switch_count": best.get("switch_count"),
            "best_turnover": best.get("turnover"),
            "best_avg_holding_period": best.get("avg_holding_period"),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_monthly_only_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_monthly_only_review",
            "Layer-1 Selector Monthly-Only Review",
            "MONTHLY_ONLY_REVIEW_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _monthly_only_rows(context, registry)
    controlled = [row for row in rows if row["switch_count_controlled"]]
    best = _best_low_turnover_row(controlled or rows)
    payload = _selector_report_payload(
        report_type="layer1_selector_monthly_only_review",
        title="Layer-1 Selector Monthly-Only Review",
        status="MONTHLY_ONLY_SELECTOR_REVIEWED",
        context=context,
        registry=registry,
        rows_field="monthly_only_rows",
        rows=rows,
        extra_summary={
            "scenario_count": len(rows),
            "switch_count_controlled_count": len(controlled),
            "monthly_execution_solves_turnover_noise": bool(controlled),
            "best_scenario_id": best.get("variant_id"),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_hysteresis_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_hysteresis_review",
            "Layer-1 Selector Hysteresis Review",
            "HYSTERESIS_REVIEW_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    original = _selector_path(context, registry, "trend_200dma_selector")
    hysteresis = _hysteresis_soft_blend_path(context, registry)
    row = _low_turnover_variant_row(
        context,
        "hysteresis_soft_blend",
        "hysteresis_soft_blend",
        hysteresis,
        original,
        extra=_soft_blend_parameter_fields(0.80, 0.50, 0.20, 0.03, 1),
    )
    original_summary = _turnover_source_summary(_turnover_source_rows(context, original))
    hysteresis_summary = _turnover_source_summary(_turnover_source_rows(context, hysteresis))
    row["switch_count_reduction"] = _int(original_summary["switch_count"]) - _int(
        hysteresis_summary["switch_count"],
    )
    row["chop_reduction"] = _int(original_summary["noise_switch_count"]) - _int(
        hysteresis_summary["noise_switch_count"],
    )
    payload = _selector_report_payload(
        report_type="layer1_selector_hysteresis_review",
        title="Layer-1 Selector Hysteresis Review",
        status="HYSTERESIS_REVIEWED",
        context=context,
        registry=registry,
        rows_field="hysteresis_rows",
        rows=[row],
        extra_summary={
            "rule": (
                "QQQ > 200DMA + 3% risk-on; QQQ < 200DMA - 3% risk-off; "
                "no-flip zone keeps previous state"
            ),
            "switch_count_reduction": row["switch_count_reduction"],
            "chop_reduction": row["chop_reduction"],
            "late_risk_off_cost": row["late_risk_off_cost"],
            "missed_rebound_cost": row["missed_rebound_cost"],
            "switch_count_controlled": row["switch_count_controlled"],
        },
        extra_payload={
            "original_turnover_source_summary": original_summary,
            "hysteresis_turnover_source_summary": hysteresis_summary,
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_switch_quality_attribution(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_switch_quality_attribution",
            "Layer-1 Selector Switch Quality Attribution",
            "SWITCH_QUALITY_ATTRIBUTION_BLOCKED",
            context,
            output_root,
    )
    registry = _load_registry(registry_config_path)
    finalist = _best_finalist_row(context, registry)
    candidate_id = str(finalist.get("variant_id") or "soft_blend_200dma_three_state")
    path = _finalist_path(context, registry, candidate_id)
    rows = _switch_quality_rows(context, path, candidate_id)
    positive = sum(1 for row in rows if _float(row.get("net_switch_value")) > 0.0)
    payload = _selector_report_payload(
        report_type="layer1_selector_switch_quality_attribution",
        title="Layer-1 Selector Switch Quality Attribution",
        status="SWITCH_QUALITY_ATTRIBUTION_READY",
        context=context,
        registry=registry,
        rows_field="switch_quality_rows",
        rows=rows,
        extra_summary={
            "candidate_id": finalist.get("variant_id") or "soft_blend_200dma_three_state",
            "switch_count": len(rows),
            "positive_net_switch_count": positive,
            "noise_or_negative_switch_count": len(rows) - positive,
            "total_net_switch_value": _round(
                sum(_float(row.get("net_switch_value")) for row in rows),
            ),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_low_turnover_finalist_ranking(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_low_turnover_finalist_ranking",
            "Layer-1 Selector Low-Turnover Finalist Ranking",
            "LOW_TURNOVER_FINALIST_BLOCKED",
            context,
            output_root,
    )
    registry = _load_registry(registry_config_path)
    rows = _low_turnover_finalist_rows(context, registry)
    controlled = [
        row
        for row in rows
        if row["variant_id"] != "original_trend_200dma_selector"
        and row["switch_count_controlled"]
    ]
    edge_rows = [
        row
        for row in controlled
        if _float(row.get("relative_vs_equal_risk")) > 0.0
        or _float(row.get("relative_vs_100_qqq")) >= -LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE
    ]
    if edge_rows:
        status = "LOW_TURNOVER_FINALIST_FOUND"
    elif controlled:
        status = "LOW_TURNOVER_INCONCLUSIVE"
    else:
        status = "LOW_TURNOVER_NO_ACCEPTABLE_SELECTOR"
    best = _best_low_turnover_row(edge_rows or controlled or rows)
    payload = _selector_report_payload(
        report_type="layer1_selector_low_turnover_finalist_ranking",
        title="Layer-1 Selector Low-Turnover Finalist Ranking",
        status=status,
        context=context,
        registry=registry,
        rows_field="low_turnover_finalist_rows",
        rows=rows,
        extra_summary={
            "finalist_count": len(rows),
            "switch_count_controlled_count": len(controlled),
            "best_low_turnover_selector": best.get("variant_id"),
            "best_net_return_after_cost": best.get("net_return_after_cost"),
            "best_turnover": best.get("turnover"),
            "best_switch_count": best.get("switch_count"),
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_vs_simple_components_final_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_vs_simple_components_final_gate",
            "Layer-1 Selector Vs Simple Components Final Gate",
            "SELECTOR_FINAL_GATE_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    rows = _selector_vs_simple_component_rows(context, registry)
    selector = next((row for row in rows if row["role"] == "best_low_turnover_selector"), {})
    always_100 = next((row for row in rows if row["variant_id"] == "always_100_qqq"), {})
    selector_beats_equal = _float(selector.get("relative_vs_equal_risk")) > 0.0
    selector_beats_100 = _float(selector.get("net_return_after_cost")) > _float(
        always_100.get("net_return_after_cost"),
    )
    selector_has_higher_turnover = _float(selector.get("turnover")) > _float(
        always_100.get("turnover"),
    )
    selector_only_beats_equal_risk = (
        selector_beats_equal and not selector_beats_100 and selector_has_higher_turnover
    )
    pass_gate = bool(selector.get("switch_count_controlled")) and selector_beats_100
    status = "SELECTOR_FINAL_GATE_PASS" if pass_gate else "SELECTOR_FINAL_GATE_FAIL_KEEP_DRY_RUN"
    payload = _selector_report_payload(
        report_type="layer1_selector_vs_simple_components_final_gate",
        title="Layer-1 Selector Vs Simple Components Final Gate",
        status=status,
        context=context,
        registry=registry,
        rows_field="selector_vs_simple_component_rows",
        rows=rows,
        extra_summary={
            "best_low_turnover_selector": selector.get("variant_id"),
            "selector_beats_equal_risk": selector_beats_equal,
            "selector_beats_100_qqq": selector_beats_100,
            "selector_turnover_higher_than_100_qqq": selector_has_higher_turnover,
            "selector_only_beats_equal_risk": selector_only_beats_equal_risk,
            "forward_aging_gate_allowed": pass_gate,
        },
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_forward_aging_watchlist_final_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_forward_aging_watchlist_final_review",
            "Layer-1 Selector Forward-Aging Watchlist Final Review",
            "KEEP_SELECTOR_DRY_RUN_ONLY",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    finalist_rows = _low_turnover_finalist_rows(context, registry)
    final_gate_rows = _selector_vs_simple_component_rows(context, registry)
    selector = next(
        (row for row in final_gate_rows if row["role"] == "best_low_turnover_selector"),
        {},
    )
    gate_pass = (
        bool(selector.get("switch_count_controlled"))
        and _float(selector.get("relative_vs_100_qqq")) > 0.0
    )
    status = (
        "RESEARCH_ONLY_FORWARD_AGING_WATCHLIST_REVIEWABLE"
        if gate_pass
        else "KEEP_SELECTOR_DRY_RUN_ONLY"
    )
    payload = _payload(
        report_type="layer1_selector_forward_aging_watchlist_final_review",
        title="Layer-1 Selector Forward-Aging Watchlist Final Review",
        status=status,
        summary={
            "data_quality_status": context.get("data_quality_status"),
            "actual_requested_date_range": _actual_date_range(context),
            "candidate_id": selector.get("variant_id"),
            "forward_aging_watchlist_allowed": gate_pass,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        selector_registry_version=registry.get("registry_version"),
        low_turnover_finalist_rows=finalist_rows,
        selector_vs_simple_component_rows=final_gate_rows,
        watchlist_candidate={
            "selector_id": selector.get("variant_id"),
            "research_only": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
        if gate_pass
        else None,
        source_artifacts=context.get("source_artifacts", {}),
    )
    _write_pair(payload, output_root)
    return payload


def run_layer1_selector_pause_or_continue_owner_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    owner_doc_path: Path = DEFAULT_LAYER1_SELECTOR_PAUSE_OR_CONTINUE_OWNER_PACK_DOC_PATH,
) -> dict[str, Any]:
    context = _build_context(
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
            "layer1_selector_pause_or_continue_owner_pack",
            "Layer-1 Selector Pause Or Continue Owner Pack",
            "LAYER1_SELECTOR_OWNER_PACK_BLOCKED",
            context,
            output_root,
        )
    registry = _load_registry(registry_config_path)
    finalist_rows = _low_turnover_finalist_rows(context, registry)
    final_gate_rows = _selector_vs_simple_component_rows(context, registry)
    selector = next(
        (row for row in final_gate_rows if row["role"] == "best_low_turnover_selector"),
        {},
    )
    gate_pass = (
        bool(selector.get("switch_count_controlled"))
        and _float(selector.get("relative_vs_100_qqq")) > 0.0
    )
    recommendation = (
        "CONTINUE_LAYER1_SELECTOR_RESEARCH_ONLY_FORWARD_AGING_WATCHLIST"
        if gate_pass
        else "KEEP_SELECTOR_DRY_RUN_ONLY_AND_CONTINUE_EQUAL_RISK_FORWARD_AGING"
    )
    answers = _pause_or_continue_owner_answers(gate_pass, selector, _actual_date_range(context))
    payload = _payload(
        report_type="layer1_selector_pause_or_continue_owner_pack",
        title="Layer-1 Selector Pause Or Continue Owner Pack",
        status="LAYER1_SELECTOR_PAUSE_OR_CONTINUE_OWNER_PACK_READY",
        summary={
            "data_quality_status": context.get("data_quality_status"),
            "actual_requested_date_range": _actual_date_range(context),
            "recommendation": recommendation,
            "best_low_turnover_selector": selector.get("variant_id"),
            "forward_aging_watchlist_allowed": gate_pass,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        selector_registry_version=registry.get("registry_version"),
        owner_questions=answers,
        low_turnover_finalist_rows=finalist_rows,
        selector_vs_simple_component_rows=final_gate_rows,
        owner_decision_doc_path=str(owner_doc_path),
        source_artifacts=context.get("source_artifacts", {}),
    )
    _write_pair(payload, output_root)
    _copy_markdown_artifact(payload, owner_doc_path)
    return payload


def _write_owner_watchlist_review_artifact(
    *,
    result: Mapping[str, Any],
    coverage: Mapping[str, Any],
    recent: Mapping[str, Any],
    registry: Mapping[str, Any],
    output_root: Path,
    owner_doc_path: Path,
) -> dict[str, Any]:
    review = _owner_watchlist_review_payload(result, coverage, recent, registry)
    payload = _payload(
        report_type="layer1_selector_owner_watchlist_review",
        title="Layer-1 Selector Owner Watchlist Review",
        status=review["status"],
        summary={
            "data_quality_status": _mapping(result.get("summary")).get("data_quality_status"),
            "candidate_selector_id": review["candidate_selector_id"],
            "watchlist_recommendation": review["watchlist_recommendation"],
            "watchlist_role": review["watchlist_role"],
            "required_forward_days": review["required_forward_days"],
            "history_coverage_warning": review["history_coverage_warning"],
            "recent_regime_warning": review["recent_regime_warning"],
            "blocking_reasons": review["blocking_reasons"],
            "owner_required_actions": review["owner_required_actions"],
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        owner_watchlist_review=review,
        source_artifacts={
            "real_result_summary": result.get("artifact_paths", {}),
            "history_coverage_gap_audit": coverage.get("artifact_paths", {}),
            "recent_regime_risk_disclosure": recent.get("artifact_paths", {}),
        },
        owner_review_doc_path=str(owner_doc_path),
    )
    _write_pair(payload, output_root)
    _copy_markdown_artifact(payload, owner_doc_path)
    return payload


def _write_forward_aging_dry_run_artifact(
    *,
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    owner: Mapping[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    candidate_id = _mapping(owner.get("summary")).get("candidate_selector_id")
    if not candidate_id:
        payload = _payload(
            report_type="layer1_selector_forward_aging_dry_run",
            title="Layer-1 Selector Forward-Aging Dry Run",
            status="LAYER1_SELECTOR_FORWARD_DRY_RUN_BLOCKED",
            summary={
                "decision_date": _actual_date_range(context)["end"],
                "selector_id": None,
                "selected_component": None,
                "data_quality_status": context.get("data_quality_status"),
                "observation_written": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "blocked_reason": "no_owner_watchlist_candidate",
            },
            blockers=["no_owner_watchlist_candidate"],
            source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
        )
        _write_pair(payload, output_root)
        return payload
    decision_date = str(_actual_date_range(context)["end"])
    path = _selector_path(context, registry, str(candidate_id))
    component_blend = dict(path.get(decision_date) or {})
    asset_result = _asset_weights_for_selector_blend(context, decision_date, component_blend)
    if not asset_result["available"]:
        status = "LAYER1_SELECTOR_FORWARD_DRY_RUN_BLOCKED"
    elif owner.get("status") == "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING":
        status = "LAYER1_SELECTOR_FORWARD_DRY_RUN_PASS"
    else:
        status = "LAYER1_SELECTOR_FORWARD_DRY_RUN_WARN"
    selector = _selector_by_id(registry, str(candidate_id))
    payload = _payload(
        report_type="layer1_selector_forward_aging_dry_run",
        title="Layer-1 Selector Forward-Aging Dry Run",
        status=status,
        summary={
            "decision_date": decision_date,
            "selector_id": candidate_id,
            "selected_component": _selected_component_label(component_blend),
            "component_blend_weights": component_blend,
            "final_target_weight_qqq": asset_result["weights"]["QQQ"],
            "final_target_weight_tqqq": asset_result["weights"]["TQQQ"],
            "final_target_weight_sgov": asset_result["weights"]["SGOV"],
            "data_quality_status": context.get("data_quality_status"),
            "observation_written": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        decision_date=decision_date,
        selector_id=candidate_id,
        selected_component=_selected_component_label(component_blend),
        component_blend_weights=component_blend,
        final_target_weight_qqq=asset_result["weights"]["QQQ"],
        final_target_weight_tqqq=asset_result["weights"]["TQQQ"],
        final_target_weight_sgov=asset_result["weights"]["SGOV"],
        data_quality_status=context.get("data_quality_status"),
        policy_definition_hash=asset_result["policy_definition_hash"],
        selector_definition_hash=_selector_definition_hash(selector),
        observation_written=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def _write_watchlist_blocker_report_artifact(
    *,
    owner: Mapping[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    summary = _mapping(owner.get("summary"))
    blocker = _watchlist_blocker_summary(owner)
    payload = _payload(
        report_type="layer1_selector_watchlist_blocker_report",
        title="Layer-1 Selector Watchlist Blocker Report",
        status="WATCHLIST_BLOCKER_REPORT_READY",
        summary={
            "watchlist_allowed": blocker["watchlist_allowed"],
            "watchlist_candidate": summary.get("candidate_selector_id"),
            "blocking_reasons": blocker["blocking_reasons"],
            "warning_reasons": blocker["warning_reasons"],
            "history_backfill_required": blocker["history_backfill_required"],
            "minimum_forward_observations_required": summary.get("required_forward_days"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        watchlist_allowed=blocker["watchlist_allowed"],
        watchlist_candidate=summary.get("candidate_selector_id"),
        blocking_reasons=blocker["blocking_reasons"],
        warning_reasons=blocker["warning_reasons"],
        history_backfill_required=blocker["history_backfill_required"],
        minimum_forward_observations_required=summary.get("required_forward_days"),
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        manual_review_required=True,
        source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def _write_reader_brief_preview_artifact(
    *,
    owner: Mapping[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    preview = _reader_brief_preview(owner)
    status = (
        "LAYER1_SELECTOR_READER_PREVIEW_SAFE"
        if preview["safe_to_display"]
        else "LAYER1_SELECTOR_READER_PREVIEW_AMBIGUOUS"
    )
    payload = _payload(
        report_type="layer1_selector_reader_brief_preview",
        title="Layer-1 Selector Reader Brief Preview",
        status=status,
        summary={
            "preview_title": preview["preview_title"],
            "top_selector": preview["top_selector"],
            "watchlist_status": preview["watchlist_status"],
            "history_coverage_warning": preview["history_coverage_warning"],
            "recent_regime_warning": preview["recent_regime_warning"],
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "safe_to_display": preview["safe_to_display"],
        },
        reader_brief_preview=preview,
        prohibited_phrase_hits=preview["prohibited_phrase_hits"],
        source_artifacts={"owner_watchlist_review": owner.get("artifact_paths", {})},
    )
    _write_pair(payload, output_root)
    return payload


def _turnover_source_rows(context: Mapping[str, Any], path: BlendPath) -> list[dict[str, Any]]:
    returns = _returns_frame(context)
    features = _feature_frame(context)
    dates = sorted(day for day in path if day in returns.index)
    if len(dates) < 2:
        return []
    cost_bps = _cost_bps(context)
    rows: list[dict[str, Any]] = []
    last_weights = path[dates[0]]
    last_component = _dominant_component(last_weights)
    last_switch_index = 0
    for index, day in enumerate(dates[1:], start=1):
        weights = path[day]
        component = _dominant_component(weights)
        if component == last_component:
            continue
        switch_turnover = _blend_turnover(last_weights, weights)
        turnover_cost = switch_turnover * cost_bps / 10000.0
        from_return, available_days = _future_component_return(
            returns,
            dates,
            index,
            last_component,
            horizon=20,
        )
        to_return, _ = _future_component_return(
            returns,
            dates,
            index,
            component,
            horizon=20,
        )
        relative_after_cost = to_return - from_return - turnover_cost
        near_200dma = _near_200dma(features, day)
        confirmed_5d = _trend_confirmed(features, day, component, confirmation_days=5)
        reverted_5d = _trend_signal_reverted(features, dates, index, component, lookahead_days=5)
        previous_holding_days = index - last_switch_index
        switch_was_helpful = relative_after_cost > 0.0
        switch_was_noise = (not switch_was_helpful) or (
            near_200dma and (not confirmed_5d or reverted_5d)
        )
        rows.append(
            {
                "switch_date": day,
                "from_component": last_component,
                "to_component": component,
                "market_state": _market_state(features, day),
                "distance_to_200dma": _round(_feature_value(features, day, "distance_to_200dma")),
                "near_200dma": near_200dma,
                "confirmed_5d": confirmed_5d,
                "signal_reverted_within_5d": reverted_5d,
                "previous_holding_days": previous_holding_days,
                "short_holding_period": previous_holding_days < 40,
                "subsequent_20d_outcome": {
                    "available_days": available_days,
                    "from_component_return": _round(from_return),
                    "to_component_return": _round(to_return),
                    "relative_after_turnover_cost": _round(relative_after_cost),
                },
                "switch_was_helpful": switch_was_helpful,
                "switch_was_noise": switch_was_noise,
                "turnover_cost": _round(turnover_cost),
                "switch_turnover": _round(switch_turnover),
            }
        )
        last_weights = weights
        last_component = component
        last_switch_index = index
    return rows


def _recent_regime_status(disclosure: Mapping[str, Any]) -> str:
    return (
        "RECENT_REGIME_RISK_MATERIAL"
        if disclosure.get("missing_regime_list")
        else "RECENT_REGIME_RISK_DISCLOSED"
    )


def _turnover_source_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    switch_count = len(rows)
    helpful = sum(1 for row in rows if bool(row.get("switch_was_helpful")))
    noise = sum(1 for row in rows if bool(row.get("switch_was_noise")))
    near = sum(1 for row in rows if bool(row.get("near_200dma")))
    unconfirmed = sum(1 for row in rows if not bool(row.get("confirmed_5d")))
    reverted = sum(1 for row in rows if bool(row.get("signal_reverted_within_5d")))
    short_holding = sum(1 for row in rows if bool(row.get("short_holding_period")))
    return {
        "switch_count": switch_count,
        "helpful_switch_count": helpful,
        "noise_switch_count": noise,
        "near_200dma_switch_count": near,
        "unconfirmed_switch_count": unconfirmed,
        "signal_reverted_switch_count": reverted,
        "short_holding_switch_count": short_holding,
        "noise_switch_share": _round(_ratio(noise, switch_count)),
        "near_200dma_switch_share": _round(_ratio(near, switch_count)),
        "unconfirmed_switch_share": _round(_ratio(unconfirmed, switch_count)),
        "short_holding_switch_share": _round(_ratio(short_holding, switch_count)),
        "total_turnover_cost": _round(sum(_float(row.get("turnover_cost")) for row in rows)),
    }


def _switch_count_by_year(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        year = str(pd.to_datetime(str(row.get("switch_date"))).year)
        counts[year] += 1
    return dict(sorted(counts.items()))


def _switch_helpfulness_by_year(rows: list[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = defaultdict(lambda: {"helpful": 0, "noise": 0})
    for row in rows:
        year = str(pd.to_datetime(str(row.get("switch_date"))).year)
        if bool(row.get("switch_was_helpful")):
            result[year]["helpful"] += 1
        if bool(row.get("switch_was_noise")):
            result[year]["noise"] += 1
    return dict(sorted(result.items()))


def _buffered_200dma_variant_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    rows = []
    for buffer_pct in LOW_TURNOVER_BUFFER_GRID:
        path = _buffered_200dma_path(context, registry, buffer_pct)
        rows.append(
            _low_turnover_variant_row(
                context,
                f"buffered_200dma_selector_buffer_{int(buffer_pct * 100)}pct",
                "buffered_200dma_selector",
                path,
                original,
                extra={"buffer": buffer_pct, "confirmation_days": None},
            )
        )
    for days in LOW_TURNOVER_CONFIRMATION_GRID:
        path = _confirmed_200dma_path(context, registry, days)
        rows.append(
            _low_turnover_variant_row(
                context,
                f"confirmed_200dma_selector_{days}d",
                "confirmed_200dma_selector",
                path,
                original,
                extra={"buffer": None, "confirmation_days": days},
            )
        )
    return rows


def _min_holding_cooldown_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    rows = []
    for minimum_holding in LOW_TURNOVER_MIN_HOLDING_GRID:
        for cooldown in LOW_TURNOVER_COOLDOWN_GRID:
            for max_switches in LOW_TURNOVER_MAX_SWITCHES_GRID:
                path = _selector_path(
                    context,
                    registry,
                    "trend_200dma_selector",
                    minimum_holding_period=minimum_holding,
                    cooldown_days=cooldown,
                    max_switches_per_year=max_switches,
                )
                rows.append(
                    _low_turnover_variant_row(
                        context,
                        (
                            "trend_200dma_min"
                            f"{minimum_holding}_cool{cooldown}_max{max_switches}"
                        ),
                        "holding_cooldown_switch_cap_selector",
                        path,
                        original,
                        extra={
                            "minimum_holding_period": minimum_holding,
                            "cooldown_after_switch": cooldown,
                            "max_switches_per_year": max_switches,
                        },
                    )
                )
    return rows


def _soft_blend_constrained_search_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    rows = []
    for risk_on_weight in LOW_TURNOVER_CONSTRAINED_RISK_ON_WEIGHTS:
        for neutral_weight in LOW_TURNOVER_CONSTRAINED_NEUTRAL_WEIGHTS:
            for risk_off_weight in LOW_TURNOVER_CONSTRAINED_RISK_OFF_WEIGHTS:
                for buffer_pct in LOW_TURNOVER_CONSTRAINED_BUFFER_GRID:
                    for confirmation_days in LOW_TURNOVER_CONSTRAINED_CONFIRMATION_GRID:
                        path = _soft_blend_200dma_path(
                            context,
                            registry,
                            risk_on_weight_100qqq=risk_on_weight,
                            neutral_weight_100qqq=neutral_weight,
                            risk_off_weight_100qqq=risk_off_weight,
                            buffer_pct=buffer_pct,
                            confirmation_days=confirmation_days,
                        )
                        rows.append(
                            _low_turnover_variant_row(
                                context,
                                (
                                    "soft_blend_200dma_three_state"
                                    f"_on{int(risk_on_weight * 100)}"
                                    f"_neutral{int(neutral_weight * 100)}"
                                    f"_off{int(risk_off_weight * 100)}"
                                    f"_buffer{int(buffer_pct * 100)}"
                                    f"_confirm{confirmation_days}"
                                ),
                                "soft_blend_constrained_search",
                                path,
                                original,
                                extra=_soft_blend_parameter_fields(
                                    risk_on_weight,
                                    neutral_weight,
                                    risk_off_weight,
                                    buffer_pct,
                                    confirmation_days,
                                ),
                            )
                        )
    rows.sort(
        key=lambda row: (
            bool(row["switch_count_controlled"]),
            _float(row["net_return_after_cost"]),
            _float(row["calmar"]),
            -_float(row["turnover"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _monthly_only_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    daily_signal = _soft_blend_200dma_path(context, registry)
    threshold_signal = _soft_blend_200dma_path(
        context,
        registry,
        buffer_pct=0.03,
        confirmation_days=1,
    )
    scenarios = [
        (
            "daily_signal_monthly_execution",
            "daily_signal_monthly_execution",
            _monthly_execution_path(daily_signal),
        ),
        (
            "monthly_signal_monthly_execution",
            "monthly_signal_monthly_execution",
            _monthly_signal_soft_blend_path(context, registry),
        ),
        (
            "threshold_signal_monthly_execution",
            "threshold_signal_monthly_execution",
            _monthly_execution_path(threshold_signal),
        ),
    ]
    rows = [
        _low_turnover_variant_row(
            context,
            variant_id,
            family,
            path,
            original,
        )
        for variant_id, family, path in scenarios
    ]
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _low_turnover_finalist_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    finalists = [
        (
            "original_trend_200dma_selector",
            "original trend_200dma_selector",
            original,
            {},
        ),
        (
            "soft_blend_200dma_three_state",
            "soft_blend_selector",
            _soft_blend_200dma_path(context, registry),
            _soft_blend_parameter_fields(0.80, 0.50, 0.20, LOW_TURNOVER_NEAR_200DMA_BAND, 1),
        ),
        (
            "monthly_soft_blend",
            "monthly_soft_blend",
            _monthly_execution_path(_soft_blend_200dma_path(context, registry)),
            {"execution_frequency": "monthly"},
        ),
        (
            "hysteresis_soft_blend",
            "hysteresis_soft_blend",
            _hysteresis_soft_blend_path(context, registry),
            _soft_blend_parameter_fields(0.80, 0.50, 0.20, 0.03, 1),
        ),
        (
            "confirmed_soft_blend",
            "confirmed_soft_blend",
            _soft_blend_200dma_path(
                context,
                registry,
                buffer_pct=0.03,
                confirmation_days=10,
            ),
            _soft_blend_parameter_fields(0.80, 0.50, 0.20, 0.03, 10),
        ),
        (
            "min_holding_soft_blend",
            "min_holding_soft_blend",
            _soft_blend_200dma_path(
                context,
                registry,
                minimum_holding_period=60,
                cooldown_days=5,
                max_switches_per_year=2,
            ),
            {
                **_soft_blend_parameter_fields(0.80, 0.50, 0.20, LOW_TURNOVER_NEAR_200DMA_BAND, 1),
                "minimum_holding_period": 60,
                "cooldown_after_switch": 5,
                "max_switches_per_year": 2,
            },
        ),
    ]
    rows = [
        _low_turnover_variant_row(
            context,
            variant_id,
            family,
            path,
            original,
            extra=extra,
        )
        for variant_id, family, path, extra in finalists
    ]
    for row in rows:
        row["dominance_status"] = _low_turnover_dominance_status(row, rows)
    rows.sort(
        key=lambda row: (
            bool(row["switch_count_controlled"]),
            _float(row["net_return_after_cost"]),
            _float(row["calmar"]),
            -_float(row["turnover"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _best_finalist_row(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> Mapping[str, Any]:
    rows = [
        row
        for row in _low_turnover_finalist_rows(context, registry)
        if row["variant_id"] != "original_trend_200dma_selector"
    ]
    controlled = [row for row in rows if row["switch_count_controlled"]]
    return _best_low_turnover_row(controlled or rows)


def _finalist_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    variant_id: str,
) -> BlendPath:
    if variant_id == "original_trend_200dma_selector":
        return _selector_path(context, registry, "trend_200dma_selector")
    if variant_id == "monthly_soft_blend":
        return _monthly_execution_path(_soft_blend_200dma_path(context, registry))
    if variant_id == "hysteresis_soft_blend":
        return _hysteresis_soft_blend_path(context, registry)
    if variant_id == "confirmed_soft_blend":
        return _soft_blend_200dma_path(
            context,
            registry,
            buffer_pct=0.03,
            confirmation_days=10,
        )
    if variant_id == "min_holding_soft_blend":
        return _soft_blend_200dma_path(
            context,
            registry,
            minimum_holding_period=60,
            cooldown_days=5,
            max_switches_per_year=2,
        )
    return _soft_blend_200dma_path(context, registry)


def _low_turnover_ranking_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    buffered_rows = _buffered_200dma_variant_rows(context, registry)
    minimum_rows = _single_control_variant_rows(context, registry)
    soft_path = _soft_blend_200dma_path(context, registry)
    rows = [
        _low_turnover_variant_row(
            context,
            "original_trend_200dma_selector",
            "original trend_200dma_selector",
            original,
            original,
        ),
        _low_turnover_variant_row(
            context,
            "always_equal_risk",
            "static_baseline",
            _selector_path(context, registry, "always_equal_risk"),
            original,
        ),
        _low_turnover_variant_row(
            context,
            "always_100_qqq",
            "static_baseline",
            _selector_path(context, registry, "always_100_qqq"),
            original,
        ),
        _low_turnover_variant_row(
            context,
            "soft_blend_200dma_three_state",
            "soft_blend_selector",
            soft_path,
            original,
            extra={
                "strong_risk_on_weight_100qqq": 0.80,
                "normal_weight_100qqq": 0.50,
                "risk_off_weight_100qqq": 0.20,
            },
        ),
    ]
    for family in ("buffered_200dma_selector", "confirmed_200dma_selector"):
        family_rows = [row for row in buffered_rows if row["variant_family"] == family]
        if family_rows:
            rows.append(dict(_best_low_turnover_row(family_rows)))
    for family in ("minimum_holding_selector", "cooldown_selector"):
        family_rows = [row for row in minimum_rows if row["variant_family"] == family]
        if family_rows:
            rows.append(dict(_best_low_turnover_row(family_rows)))
    for row in rows:
        row["dominance_status"] = _low_turnover_dominance_status(row, rows)
    rows.sort(
        key=lambda row: (
            _float(row.get("net_return_after_cost")),
            _float(row.get("calmar")),
            _float(row.get("turnover_reduction")),
            -_float(row.get("turnover")),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _single_control_variant_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    original = _selector_path(context, registry, "trend_200dma_selector")
    rows = []
    for minimum_holding in LOW_TURNOVER_MIN_HOLDING_GRID:
        path = _selector_path(
            context,
            registry,
            "trend_200dma_selector",
            minimum_holding_period=minimum_holding,
            cooldown_days=5,
            max_switches_per_year=12,
        )
        rows.append(
            _low_turnover_variant_row(
                context,
                f"minimum_holding_selector_{minimum_holding}d",
                "minimum_holding_selector",
                path,
                original,
                extra={
                    "minimum_holding_period": minimum_holding,
                    "cooldown_after_switch": 5,
                    "max_switches_per_year": 12,
                },
            )
        )
    for cooldown in LOW_TURNOVER_COOLDOWN_GRID:
        path = _selector_path(
            context,
            registry,
            "trend_200dma_selector",
            minimum_holding_period=20,
            cooldown_days=cooldown,
            max_switches_per_year=12,
        )
        rows.append(
            _low_turnover_variant_row(
                context,
                f"cooldown_selector_{cooldown}d",
                "cooldown_selector",
                path,
                original,
                extra={
                    "minimum_holding_period": 20,
                    "cooldown_after_switch": cooldown,
                    "max_switches_per_year": 12,
                },
            )
        )
    return rows


def _low_turnover_variant_row(
    context: Mapping[str, Any],
    variant_id: str,
    variant_family: str,
    path: BlendPath,
    original_path: BlendPath,
    *,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metrics = _evaluate_blend_path(context, path)
    original = _evaluate_blend_path(context, original_path)
    opportunity = _opportunity_costs(context, path)
    original_turnover = _float(original["turnover"])
    turnover = _float(metrics["turnover"])
    switch_count = _int(metrics["switch_count"])
    registry_policy = _mapping(
        safe_load_yaml_path(DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH).get(
            "evaluation_policy",
            {},
        )
    )
    switch_control = switch_count_control_result(
        actual_date_range=_actual_date_range(context),
        metrics=metrics,
        registry_policy=registry_policy,
    )
    row = {
        "rank": 0,
        "variant_id": variant_id,
        "variant_family": variant_family,
        "net_return_after_cost": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "sharpe": _round(metrics["sharpe"]),
        "calmar": _round(metrics["calmar"]),
        "turnover": _round(turnover),
        "switch_count": switch_count,
        "avg_holding_period": _round(metrics["avg_holding_period"]),
        "annualized_switches": _round(
            _float(switch_control["observed"]["annualized_switches"]),
        ),
        "switch_count_by_year": metrics["switch_count_by_year"],
        "turnover_by_year": metrics["turnover_by_year"],
        "max_switches_per_year_observed": metrics["max_switches_per_year_observed"],
        "max_switches_per_3y_observed": metrics["max_switches_per_3y_observed"],
        "max_turnover_per_year_observed": _round(
            metrics["max_turnover_per_year_observed"],
        ),
        "switch_count_controlled": bool(switch_control["switch_count_controlled"]),
        "switch_count_control_failed_checks": switch_control["failed_checks"],
        "cost_drag": _round(metrics["cost_drag"]),
        "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
        "relative_vs_equal_risk": _round(metrics["relative_vs_equal_risk"]),
        "relative_vs_100_qqq": _round(metrics["relative_vs_100_qqq"]),
        "turnover_reduction": _round(original_turnover - turnover),
        "turnover_reduction_pct": _round(_ratio(original_turnover - turnover, original_turnover)),
        "switch_count_reduction": _int(original["switch_count"]) - switch_count,
        "regret_reduction": _round(
            _float(original["regret_vs_best_component"])
            - _float(metrics["regret_vs_best_component"])
        ),
        "max_drawdown_delta_vs_original": _round(
            abs(_float(metrics["max_drawdown"])) - abs(_float(original["max_drawdown"]))
        ),
        "calmar_delta_vs_original": _round(
            _float(metrics["calmar"]) - _float(original["calmar"])
        ),
        "missed_rebound_cost": _round(opportunity["missed_rebound_cost"]),
        "late_risk_off_cost": _round(opportunity["late_risk_off_cost"]),
        "turnover_acceptable": bool(switch_control["switch_count_controlled"]),
        "drawdown_control_not_materially_worse": (
            abs(_float(metrics["max_drawdown"]))
            <= abs(_float(original["max_drawdown"])) + LOW_TURNOVER_OWNER_QQQ_LAG_TOLERANCE
        ),
        "dominance_status": "NOT_EVALUATED",
    }
    row.update(dict(extra or {}))
    return row


def _buffered_200dma_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    buffer_pct: float,
) -> BlendPath:
    features = _feature_frame(context)
    dates = sorted(str(day) for day in features.index)
    if not dates:
        return {}
    first_distance = _feature_value(features, dates[0], "distance_to_200dma")
    current = "100_qqq" if first_distance > 0.0 else "equal_risk_qqq_sgov"
    raw: BlendPath = {}
    for day in dates:
        distance = _feature_value(features, day, "distance_to_200dma")
        if distance > buffer_pct:
            current = "100_qqq"
        elif distance < -buffer_pct:
            current = "equal_risk_qqq_sgov"
        raw[day] = _hard_weights(current)
    return _apply_switching_constraints(raw, _trend_selector_constraints(registry))


def _confirmed_200dma_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    confirmation_days: int,
) -> BlendPath:
    features = _feature_frame(context)
    dates = sorted(str(day) for day in features.index)
    if not dates:
        return {}
    current = (
        "100_qqq"
        if _feature_value(features, dates[0], "distance_to_200dma") > 0.0
        else "equal_risk_qqq_sgov"
    )
    above_streak = 0
    below_streak = 0
    raw: BlendPath = {}
    for day in dates:
        distance = _feature_value(features, day, "distance_to_200dma")
        above_streak = above_streak + 1 if distance > 0.0 else 0
        below_streak = below_streak + 1 if distance < 0.0 else 0
        if above_streak >= confirmation_days:
            current = "100_qqq"
        elif below_streak >= confirmation_days:
            current = "equal_risk_qqq_sgov"
        raw[day] = _hard_weights(current)
    return _apply_switching_constraints(raw, _trend_selector_constraints(registry))


def _soft_blend_200dma_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    *,
    risk_on_weight_100qqq: float = 0.80,
    neutral_weight_100qqq: float = 0.50,
    risk_off_weight_100qqq: float = 0.20,
    buffer_pct: float = LOW_TURNOVER_NEAR_200DMA_BAND,
    confirmation_days: int = 1,
    hysteresis: bool = False,
    minimum_holding_period: int | None = None,
    cooldown_days: int | None = None,
    max_switches_per_year: int | None = None,
) -> BlendPath:
    features = _feature_frame(context)
    dates = sorted(str(value) for value in features.index)
    if not dates:
        return {}
    raw: BlendPath = {}
    first_distance = _feature_value(features, dates[0], "distance_to_200dma")
    if first_distance > buffer_pct:
        current_state = "risk_on"
    elif first_distance < -buffer_pct:
        current_state = "risk_off"
    else:
        current_state = "neutral"
    above_streak = 0
    below_streak = 0
    required_confirmation = max(_int(confirmation_days, 1), 1)
    for day in dates:
        distance = _feature_value(features, day, "distance_to_200dma")
        above_streak = above_streak + 1 if distance > buffer_pct else 0
        below_streak = below_streak + 1 if distance < -buffer_pct else 0
        if above_streak >= required_confirmation:
            current_state = "risk_on"
        elif below_streak >= required_confirmation:
            current_state = "risk_off"
        elif not hysteresis:
            current_state = "neutral"
        if current_state == "risk_on":
            weight = risk_on_weight_100qqq
        elif current_state == "risk_off":
            weight = risk_off_weight_100qqq
        else:
            weight = neutral_weight_100qqq
        raw[day] = _blend_weights(weight)
    return _apply_switching_constraints(
        raw,
        _trend_selector_constraints(
            registry,
            minimum_holding_period=minimum_holding_period,
            cooldown_days=cooldown_days,
            max_switches_per_year=max_switches_per_year,
        ),
    )


def _hysteresis_soft_blend_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> BlendPath:
    return _soft_blend_200dma_path(
        context,
        registry,
        buffer_pct=0.03,
        confirmation_days=1,
        hysteresis=True,
    )


def _soft_blend_parameter_fields(
    risk_on_weight_100qqq: float,
    neutral_weight_100qqq: float,
    risk_off_weight_100qqq: float,
    buffer_pct: float,
    confirmation_days: int,
) -> dict[str, Any]:
    return {
        "risk_on_weight_100qqq": _round(risk_on_weight_100qqq),
        "risk_on_blend": f"{int(round(risk_on_weight_100qqq * 100))}/"
        f"{int(round((1.0 - risk_on_weight_100qqq) * 100))}",
        "neutral_weight_100qqq": _round(neutral_weight_100qqq),
        "neutral_blend": f"{int(round(neutral_weight_100qqq * 100))}/"
        f"{int(round((1.0 - neutral_weight_100qqq) * 100))}",
        "risk_off_weight_100qqq": _round(risk_off_weight_100qqq),
        "risk_off_blend": f"{int(round(risk_off_weight_100qqq * 100))}/"
        f"{int(round((1.0 - risk_off_weight_100qqq) * 100))}",
        "buffer": _round(buffer_pct),
        "confirmation_days": confirmation_days,
    }


def _trend_selector_constraints(
    registry: Mapping[str, Any],
    *,
    minimum_holding_period: int | None = None,
    cooldown_days: int | None = None,
    max_switches_per_year: int | None = None,
) -> dict[str, Any]:
    constraints = _selector_constraints(
        _selector_by_id(registry, "trend_200dma_selector"),
        minimum_holding_period=minimum_holding_period,
        cooldown_days=cooldown_days,
        max_switches_per_year=max_switches_per_year,
    )
    return constraints


def _opportunity_costs(context: Mapping[str, Any], path: BlendPath) -> dict[str, float]:
    returns = _returns_frame(context)
    dates = sorted(day for day in path if day in returns.index)
    missed_rebound_cost = 0.0
    late_risk_off_cost = 0.0
    for index, day in enumerate(dates):
        qqq_return, available = _future_component_return(
            returns,
            dates,
            index,
            "100_qqq",
            horizon=20,
        )
        if available <= 0:
            continue
        equal_return, _ = _future_component_return(
            returns,
            dates,
            index,
            "equal_risk_qqq_sgov",
            horizon=20,
        )
        weight_100 = _float(path[day].get("100_qqq"))
        if qqq_return > equal_return:
            missed_rebound_cost += (1.0 - weight_100) * (qqq_return - equal_return)
        elif equal_return > qqq_return:
            late_risk_off_cost += weight_100 * (equal_return - qqq_return)
    return {
        "missed_rebound_cost": missed_rebound_cost,
        "late_risk_off_cost": late_risk_off_cost,
    }


def _low_turnover_acceptable(context: Mapping[str, Any], metrics: Mapping[str, Any]) -> bool:
    registry_policy = _mapping(
        safe_load_yaml_path(DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH).get(
            "evaluation_policy",
            {},
        )
    )
    return low_turnover_acceptable(
        actual_date_range=_actual_date_range(context),
        metrics=metrics,
        registry_policy=registry_policy,
    )


def _blend_weight_path(path: BlendPath) -> list[dict[str, Any]]:
    return [
        {
            "date": day,
            "equal_risk_qqq_sgov_weight": _round(weights.get("equal_risk_qqq_sgov")),
            "100_qqq_weight": _round(weights.get("100_qqq")),
        }
        for day, weights in sorted(path.items())
    ]


def _blend_weight_path_sample(path: BlendPath) -> list[dict[str, Any]]:
    full = _blend_weight_path(path)
    if len(full) <= 10:
        return full
    return full[:5] + full[-5:]


def _returns_frame(context: Mapping[str, Any]) -> pd.DataFrame:
    panel = _ensure_frame(context["panel"])
    return panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()


def _cost_bps(context: Mapping[str, Any]) -> float:
    return _float(
        _mapping(_mapping(context["config"]).get("research_policy"))
        .get("cost_assumption", {})
        .get("base_cost_bps"),
        5.0,
    )


def _future_component_return(
    returns: pd.DataFrame,
    dates: list[str],
    start_index: int,
    component: str,
    *,
    horizon: int,
) -> tuple[float, int]:
    start = start_index + 1
    end = min(start + horizon, len(dates))
    if start >= end or component not in returns.columns:
        return 0.0, 0
    window = dates[start:end]
    series = returns.loc[window, component].fillna(0.0)
    return _compound_return(series), len(series)


def _market_state(features: pd.DataFrame, day: str) -> str:
    distance = _feature_value(features, day, "distance_to_200dma")
    if distance > LOW_TURNOVER_NEAR_200DMA_BAND:
        return "above_200dma_outside_buffer"
    if distance > 0.0:
        return "above_200dma_inside_buffer"
    if distance >= -LOW_TURNOVER_NEAR_200DMA_BAND:
        return "below_200dma_inside_buffer"
    return "below_200dma_outside_buffer"


def _near_200dma(features: pd.DataFrame, day: str) -> bool:
    return abs(_feature_value(features, day, "distance_to_200dma")) <= LOW_TURNOVER_NEAR_200DMA_BAND


def _trend_confirmed(
    features: pd.DataFrame,
    day: str,
    component: str,
    *,
    confirmation_days: int,
) -> bool:
    dates = [str(value) for value in features.index]
    if day not in dates:
        return False
    index = dates.index(day)
    start = max(0, index - confirmation_days + 1)
    window = dates[start : index + 1]
    if len(window) < confirmation_days:
        return False
    distances = [_feature_value(features, value, "distance_to_200dma") for value in window]
    if component == "100_qqq":
        return all(value > 0.0 for value in distances)
    return all(value < 0.0 for value in distances)


def _trend_signal_reverted(
    features: pd.DataFrame,
    dates: list[str],
    switch_index: int,
    component: str,
    *,
    lookahead_days: int,
) -> bool:
    end = min(switch_index + lookahead_days + 1, len(dates))
    window = dates[switch_index + 1 : end]
    if not window:
        return False
    distances = [_feature_value(features, value, "distance_to_200dma") for value in window]
    if component == "100_qqq":
        return any(value <= 0.0 for value in distances)
    return any(value >= 0.0 for value in distances)


def _feature_value(features: pd.DataFrame, day: str, column: str) -> float:
    if column not in features.columns:
        return 0.0
    for key in (day, pd.to_datetime(day).date(), pd.Timestamp(day)):
        if key in features.index:
            return _float(features.loc[key, column])
    return 0.0


def _simple_rule_selector_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for selector_id in _ranking_selector_ids(registry):
        if selector_id.startswith("always_"):
            continue
        selector = _selector_by_id(registry, selector_id)
        metrics = _evaluate_blend_path(context, _selector_path(context, registry, selector_id))
        rows.append(
            {
                "selector_id": selector_id,
                "selector_type": _selector_family(selector_id, selector),
                "net_return_after_cost": _round(metrics["net_return_after_cost"]),
                "max_drawdown": _round(metrics["max_drawdown"]),
                "sharpe": _round(metrics["sharpe"]),
                "calmar": _round(metrics["calmar"]),
                "turnover": _round(metrics["turnover"]),
                "switch_count": metrics["switch_count"],
                "avg_holding_period": _round(metrics["avg_holding_period"]),
                "relative_vs_always_equal_risk": _round(metrics["relative_vs_equal_risk"]),
                "relative_vs_always_100_qqq": _round(metrics["relative_vs_100_qqq"]),
                "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
                "selected_component_distribution": metrics["selected_component_distribution"],
            }
        )
    rows.sort(
        key=lambda row: (
            _float(row["net_return_after_cost"]),
            _float(row["calmar"]),
            -_float(row["turnover"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _top_simple_rule_selector_row(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _simple_rule_selector_rows(context, registry)
    return rows[0] if rows else {}


def _selector_family(selector_id: str, selector: Mapping[str, Any]) -> str:
    selector_type = str(selector.get("selector_type") or "")
    if "combined" in selector_type or "vote" in selector_id or "trend_vol_drawdown" in selector_id:
        return "combined_rule"
    if "vol" in selector_type or "vol" in selector_id:
        return "volatility_rule"
    if "drawdown" in selector_type or "drawdown" in selector_id:
        return "drawdown_rule"
    if "trend" in selector_type or "trend" in selector_id:
        return "trend_rule"
    return selector_type or "simple_rule"


def _top_selector_summary_fields(top: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "top_selector_id": top.get("selector_id"),
        "top_selector_type": top.get("selector_type"),
        "net_return_after_cost": top.get("net_return_after_cost"),
        "max_drawdown": top.get("max_drawdown"),
        "sharpe": top.get("sharpe"),
        "calmar": top.get("calmar"),
        "turnover": top.get("turnover"),
        "switch_count": top.get("switch_count"),
        "avg_holding_period": top.get("avg_holding_period"),
        "relative_vs_always_equal_risk": top.get("relative_vs_always_equal_risk"),
        "relative_vs_always_100_qqq": top.get("relative_vs_always_100_qqq"),
        "regret_vs_best_component": top.get("regret_vs_best_component"),
    }


def _cost_after_edge_exists(top: Mapping[str, Any]) -> bool:
    if not top:
        return False
    return (
        _float(top.get("relative_vs_always_equal_risk")) > 0.0
        or _float(top.get("relative_vs_always_100_qqq")) > 0.0
    )


def _turnover_acceptable(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    top: Mapping[str, Any],
) -> bool:
    if not top:
        return False
    policy = _evaluation_policy(registry)
    actual_range = _actual_date_range(context)
    span_days = _date_span_days(actual_range)
    annual_switches = _float(top.get("switch_count")) / max(span_days / 365.25, 1.0)
    return (
        annual_switches <= _float(policy.get("max_switches_per_year"), 12)
        and _float(top.get("turnover")) <= _float(policy.get("max_turnover_warning"), 2.0)
    )


def _real_result_required_answers(
    top: Mapping[str, Any],
    watchlist: Mapping[str, Any],
) -> list[dict[str, str]]:
    watchlist_selector = _mapping(watchlist.get("summary")).get("selector_id")
    return [
        {
            "question": "排名最高的是趋势规则、波动规则、回撤规则，还是组合规则？",
            "answer": str(top.get("selector_type") or "none"),
        },
        {
            "question": "它是否优于 always_equal_risk？",
            "answer": "YES" if _float(top.get("relative_vs_always_equal_risk")) > 0.0 else "NO",
        },
        {
            "question": "它是否优于 always_100_qqq？",
            "answer": "YES" if _float(top.get("relative_vs_always_100_qqq")) > 0.0 else "NO",
        },
        {
            "question": "它的优势是否成本后仍存在？",
            "answer": "YES" if _cost_after_edge_exists(top) else "NO",
        },
        {
            "question": "它的换手是否可接受？",
            "answer": "SEE_TURNOVER_POLICY_CHECK",
        },
        {
            "question": "当前是否有明确 watchlist candidate？",
            "answer": str(watchlist_selector or "NO"),
        },
    ]


def _layer1_result_review_required_sources(output_root: Path) -> dict[str, dict[str, str]]:
    report_ids = [
        "layer1_simple_rule_selector_registry_review",
        "layer1_trend_rule_selector_backtest",
        "layer1_volatility_rule_selector_backtest",
        "layer1_drawdown_rule_selector_backtest",
        "layer1_combined_simple_rule_selector_search",
        "layer1_selector_cost_latency_stress",
        "layer1_selector_period_split_validation",
        "layer1_selector_drawdown_episode_review",
        "layer1_selector_regret_attribution",
        "layer1_selector_vs_component_baseline_ranking",
        "layer1_selector_overfit_sensitivity_review",
        "layer1_selector_minimum_holding_period_review",
        "layer1_selector_forward_aging_watchlist_gate",
        "layer1_selector_owner_decision_pack",
        "layer1_simple_rule_selector_master_review",
    ]
    return {
        report_id: {
            "json_path": str(output_root / f"{report_id}.json"),
            "markdown_path": str(output_root / f"{report_id}.md"),
        }
        for report_id in report_ids
    }


def _history_coverage_summary(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    prices_path: Path,
) -> dict[str, Any]:
    actual = _actual_date_range(context)
    available_start = _date_or_none(actual["start"])
    available_end = _date_or_none(actual["end"])
    expected_start = date(2012, 1, 3)
    gap_days = (
        max((available_start - expected_start).days, 0)
        if available_start is not None
        else None
    )
    panel = _ensure_frame(context["panel"])
    cube = _ensure_frame(context["cube"])
    weight_frame = _ensure_frame(context["weight_frame"])
    feature_frame = _feature_frame(context)
    price_coverage = _price_coverage_by_ticker(prices_path, ("QQQ", "SGOV", "TQQQ"))
    affected_features = sorted(
        {
            str(feature)
            for selector in _selector_rows(registry)
            for feature in selector.get("feature_inputs", [])
        }
    )
    affected_components = sorted(SELECTABLE_COMPONENT_IDS)
    can_backfill = _can_backfill_to_2012(price_coverage, expected_start)
    if can_backfill and available_start and available_start <= expected_start:
        strength = "FULL_HISTORY_AVAILABLE"
        reason = "Layer-2 fact panel and selector inputs cover the requested 2012 start."
    elif available_start and available_start <= date(2022, 12, 1):
        strength = "RECENT_REGIME_ONLY_WARNING"
        reason = (
            "Layer-2 fact context defaults to ai_after_chatgpt start and the current "
            "audited selector panel does not include pre-2022 regimes."
        )
    elif available_start:
        strength = "SHORT_HISTORY_ACCEPTABLE_FOR_RESEARCH"
        reason = "Audited selector panel starts after the configured AI-regime start."
    else:
        strength = "HISTORY_COVERAGE_BLOCKED"
        reason = "Layer-2 fact panel is empty after data quality gating."
    mature_120d = cube[
        (cube.get("horizon") == "120d") & (cube.get("outcome_status") == "MATURED")
    ]
    policy_hash_missing = (
        bool(weight_frame.get("policy_definition_hash", pd.Series(dtype=object)).isna().any())
        if not weight_frame.empty
        else True
    )
    return {
        "available_start_date": actual["start"],
        "available_end_date": actual["end"],
        "expected_start_date": expected_start.isoformat(),
        "coverage_gap_days": gap_days,
        "coverage_gap_reason": reason,
        "affected_features": affected_features,
        "affected_components": affected_components,
        "can_backfill_to_2012": can_backfill,
        "backfill_requirements": _backfill_requirements(price_coverage, expected_start),
        "research_conclusion_strength": strength,
        "layer2_fact_panel_start_date": _frame_date_min(panel, "date"),
        "feature_panel_start_date": _index_date_min(feature_frame),
        "selector_input_features_start_date": _index_date_min(feature_frame),
        "forward_outcome_cube_120d_mature_start_date": _frame_date_min(
            mature_120d,
            "decision_date",
        ),
        "forward_outcome_cube_120d_mature_end_date": _frame_date_max(
            mature_120d,
            "decision_date",
        ),
        "forward_120d_window_truncates_latest_samples": bool(
            available_end
            and _date_or_none(_frame_date_max(mature_120d, "decision_date"))
            and _date_or_none(_frame_date_max(mature_120d, "decision_date")) < available_end
        ),
        "price_coverage_by_ticker": price_coverage,
        "policy_definition_hash_missing": policy_hash_missing,
    }


def _price_coverage_by_ticker(path: Path, tickers: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {
            ticker: {"start": None, "end": None, "row_count": 0, "missing": True}
            for ticker in tickers
        }
    frame = pd.read_csv(path, usecols=["date", "ticker"])
    result = {}
    for ticker in tickers:
        subset = frame[frame["ticker"] == ticker]
        dates = sorted(str(value) for value in subset["date"].dropna().unique())
        result[ticker] = {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
            "row_count": int(len(subset)),
            "missing": not dates,
        }
    return result


def _can_backfill_to_2012(
    price_coverage: Mapping[str, Mapping[str, Any]],
    expected_start: date,
) -> bool:
    for ticker in ("QQQ", "SGOV", "TQQQ"):
        start = _date_or_none(_mapping(price_coverage.get(ticker)).get("start"))
        if start is None or start > expected_start:
            return False
    return True


def _backfill_requirements(
    price_coverage: Mapping[str, Mapping[str, Any]],
    expected_start: date,
) -> list[str]:
    requirements = []
    for ticker in ("QQQ", "SGOV", "TQQQ"):
        start = _date_or_none(_mapping(price_coverage.get(ticker)).get("start"))
        if start is None:
            requirements.append(f"{ticker} audited price cache is missing")
        elif start > expected_start:
            requirements.append(
                f"{ticker} audited price cache starts {start.isoformat()}, "
                f"after expected {expected_start.isoformat()}"
            )
    requirements.extend(
        [
            "rebuild Layer-2 weight path / return-cost-exposure panel from the backfilled cache",
            "rebuild independent forward outcome cube with matured 120d windows",
            "rerun leakage, period split, sensitivity, ranking, and owner review artifacts",
        ]
    )
    return requirements


def _frame_date_min(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame:
        return None
    values = sorted(str(value) for value in frame[column].dropna().unique())
    return values[0] if values else None


def _frame_date_max(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame:
        return None
    values = sorted(str(value) for value in frame[column].dropna().unique())
    return values[-1] if values else None


def _index_date_min(frame: pd.DataFrame) -> str | None:
    if frame.empty:
        return None
    return str(sorted(frame.index)[0])


def _recent_regime_disclosure(context: Mapping[str, Any]) -> dict[str, Any]:
    actual = _actual_date_range(context)
    start = _date_or_none(actual["start"])
    end = _date_or_none(actual["end"])
    coverage = {
        "2023_recovery": _period_covered(start, end, date(2023, 1, 3), date(2023, 7, 31)),
        "2024_ai_rally": _period_covered(start, end, date(2024, 1, 2), date(2024, 12, 31)),
        "2025_to_latest": bool(end and end >= date(2025, 1, 1)),
        "high_rate_sgov_carry_period": _period_covered(
            start,
            end,
            date(2022, 12, 1),
            date(2026, 6, 23),
        ),
        "post_2022_nasdaq_recovery": bool(start and start >= date(2022, 12, 1)),
    }
    missing = []
    if not _period_covered(start, end, date(2020, 2, 19), date(2020, 3, 23)):
        missing.append("2020_COVID_crash")
    if not _period_covered(start, end, date(2018, 10, 1), date(2018, 12, 24)):
        missing.append("2018Q4_selloff")
    if not _period_covered(start, end, date(2022, 1, 3), date(2022, 10, 14)):
        missing.append("full_2022_bear_market")
    if start is None or start > date(2020, 2, 19):
        missing.append("complete_pre_2022_bear_or_crash_sample")
    return {
        "regime_coverage_summary": coverage,
        "missing_regime_list": missing,
        "risk_of_overstating_selector_edge": "MATERIAL" if missing else "LOW",
        "risk_of_ai_rally_bias": "MATERIAL" if coverage["2024_ai_rally"] else "NOT_ASSESSED",
        "risk_of_high_rate_sgov_bias": (
            "MATERIAL" if coverage["high_rate_sgov_carry_period"] else "NOT_ASSESSED"
        ),
        "recommended_disclosure": (
            "Current Layer-1 selector conclusion is recent-regime-only and mainly "
            "covers post-ChatGPT recovery / AI rally / high-rate SGOV carry conditions."
            if missing
            else "Current Layer-1 selector conclusion has disclosed regime coverage."
        ),
        "actual_requested_date_range": actual,
    }


def _period_covered(
    actual_start: date | None,
    actual_end: date | None,
    period_start: date,
    period_end: date,
) -> bool:
    return bool(
        actual_start
        and actual_end
        and actual_start <= period_start
        and actual_end >= period_end
    )


def _recent_regime_required_answers(disclosure: Mapping[str, Any]) -> list[dict[str, str]]:
    coverage = _mapping(disclosure.get("regime_coverage_summary"))
    missing = set(disclosure.get("missing_regime_list") or [])
    return [
        {
            "question": "当前 selector 是否主要在 2023/2024 科技股强势阶段有效？",
            "answer": "YES_RECENT_REGIME_DOMINATED"
            if coverage.get("2024_ai_rally")
            else "NOT_ASSESSED",
        },
        {"question": "当前样本是否缺少完整熊市？", "answer": "YES"},
        {
            "question": "当前样本是否缺少 2020 COVID crash？",
            "answer": "YES" if "2020_COVID_crash" in missing else "NO",
        },
        {
            "question": "当前样本是否缺少 2018Q4 selloff？",
            "answer": "YES" if "2018Q4_selloff" in missing else "NO",
        },
        {
            "question": "当前 selector 是否可能高估 risk-on 切换能力？",
            "answer": str(disclosure.get("risk_of_overstating_selector_edge")),
        },
        {
            "question": "当前结论是否只能标记为 recent-regime-only？",
            "answer": "YES" if missing else "NO",
        },
    ]


def _owner_watchlist_review_payload(
    result: Mapping[str, Any],
    coverage: Mapping[str, Any],
    recent: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    result_summary = _mapping(result.get("summary"))
    coverage_summary = _mapping(coverage.get("summary"))
    candidate = result_summary.get("top_selector_id")
    blocking_reasons = []
    if result.get("status") == "LAYER1_SELECTOR_RESULT_SUMMARY_BLOCKED":
        blocking_reasons.append("RESULT_SUMMARY_BLOCKED")
    if not candidate:
        blocking_reasons.append("NO_SELECTOR_EDGE")
    if not bool(result_summary.get("cost_after_edge_exists")):
        blocking_reasons.append("NO_COST_ADJUSTED_EDGE")
    if not bool(result_summary.get("turnover_acceptable")):
        blocking_reasons.append("TOO_MUCH_TURNOVER")
    if result_summary.get("period_split_status") == "SELECTOR_PERIOD_CONCENTRATED":
        blocking_reasons.append("PERIOD_CONCENTRATED")
    if result_summary.get("sensitivity_status") == "SELECTOR_FRAGILE":
        blocking_reasons.append("SENSITIVITY_FRAGILE")
    if coverage.get("status") == "HISTORY_COVERAGE_BLOCKED":
        blocking_reasons.append("HISTORY_COVERAGE_BLOCKED")
    history_warning = str(coverage_summary.get("research_conclusion_strength"))
    recent_warning = str(recent.get("status"))
    if "HISTORY_COVERAGE_BLOCKED" in blocking_reasons:
        recommendation = "BLOCKED"
    elif "NO_COST_ADJUSTED_EDGE" in blocking_reasons or "NO_SELECTOR_EDGE" in blocking_reasons:
        recommendation = "NO_SELECTOR_EDGE"
    elif any(
        reason in blocking_reasons
        for reason in {"TOO_MUCH_TURNOVER", "PERIOD_CONCENTRATED", "SENSITIVITY_FRAGILE"}
    ):
        recommendation = "KEEP_SELECTOR_RESEARCH_ONLY"
    elif history_warning == "HISTORY_COVERAGE_BLOCKED":
        recommendation = "NEED_HISTORY_BACKFILL_FIRST"
    else:
        recommendation = "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING"
    status = recommendation
    return {
        "status": status,
        "candidate_selector_id": candidate,
        "watchlist_recommendation": recommendation,
        "watchlist_role": (
            "research_only_forward_aging_candidate"
            if recommendation == "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING"
            else "research_only_review_item"
        ),
        "required_forward_days": _evaluation_policy(registry).get("required_forward_days"),
        "history_coverage_warning": history_warning,
        "recent_regime_warning": recent_warning,
        "blocking_reasons": blocking_reasons,
        "owner_required_actions": [
            "manual review before any formal forward-aging observation",
            "acknowledge recent-regime-only limitation",
            "confirm no paper-shadow, production, or broker action is authorized",
        ],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _asset_weights_for_selector_blend(
    context: Mapping[str, Any],
    decision_date: str,
    component_blend: Mapping[str, Any],
) -> dict[str, Any]:
    weights = {"QQQ": 0.0, "TQQQ": 0.0, "SGOV": 0.0}
    hashes: dict[str, str] = {}
    frame = _ensure_frame(context["weight_frame"])
    if frame.empty:
        return {"available": False, "weights": weights, "policy_definition_hash": hashes}
    day_rows = frame[frame["decision_date"].astype(str) == decision_date]
    for component, component_weight in component_blend.items():
        subset = day_rows[day_rows["strategy_id"].astype(str) == str(component)]
        if subset.empty:
            return {"available": False, "weights": weights, "policy_definition_hash": hashes}
        row = subset.iloc[0]
        weight = _float(component_weight)
        weights["QQQ"] += weight * _float(row.get("target_weight_qqq"))
        weights["TQQQ"] += weight * _float(row.get("target_weight_tqqq"))
        weights["SGOV"] += weight * _float(row.get("target_weight_sgov"))
        hashes[str(component)] = str(row.get("policy_definition_hash"))
    return {
        "available": True,
        "weights": {key: _round(value) for key, value in weights.items()},
        "policy_definition_hash": hashes,
    }


def _selected_component_label(component_blend: Mapping[str, Any]) -> str | None:
    if not component_blend:
        return None
    dominant = _dominant_component({key: _float(value) for key, value in component_blend.items()})
    if _float(component_blend.get(dominant)) >= 0.999:
        return dominant
    return "blend"


def _selector_definition_hash(selector: Mapping[str, Any]) -> str:
    encoded = json.dumps(selector, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _watchlist_blocker_summary(owner: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(owner.get("summary"))
    recommendation = str(summary.get("watchlist_recommendation"))
    blocking = list(summary.get("blocking_reasons") or [])
    warnings = []
    if summary.get("history_coverage_warning") == "RECENT_REGIME_ONLY_WARNING":
        warnings.append("SHORT_HISTORY")
        warnings.append("RECENT_REGIME_ONLY")
    if summary.get("recent_regime_warning") == "RECENT_REGIME_RISK_MATERIAL":
        warnings.append("RECENT_REGIME_ONLY")
    warnings.append("OWNER_REVIEW_REQUIRED")
    if recommendation != "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING" and not blocking:
        blocking.append(recommendation or "BLOCKED")
    return {
        "watchlist_allowed": recommendation == "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING",
        "blocking_reasons": sorted(set(blocking)),
        "warning_reasons": sorted(set(warnings)),
        "history_backfill_required": summary.get("history_coverage_warning")
        in {"RECENT_REGIME_ONLY_WARNING", "HISTORY_COVERAGE_BLOCKED"},
    }


def _reader_brief_preview(owner: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(owner.get("summary"))
    preview_lines = [
        "Layer-1 selector research-only",
        f"top selector: {summary.get('candidate_selector_id') or 'none'}",
        f"watchlist status: {summary.get('watchlist_recommendation')}",
        f"history coverage warning: {summary.get('history_coverage_warning')}",
        f"recent regime warning: {summary.get('recent_regime_warning')}",
        "paper_shadow_allowed=false",
        "production_allowed=false",
        "broker_action=none",
    ]
    text = "\n".join(preview_lines)
    prohibited = ["建议买入", "建议卖出", "应切换策略", "目标实盘仓位", "真实交易建议"]
    hits = [phrase for phrase in prohibited if phrase in text]
    return {
        "preview_title": "Layer-1 selector research-only",
        "top_selector": summary.get("candidate_selector_id"),
        "watchlist_status": summary.get("watchlist_recommendation"),
        "history_coverage_warning": summary.get("history_coverage_warning"),
        "recent_regime_warning": summary.get("recent_regime_warning"),
        "preview_text": text,
        "prohibited_phrase_hits": hits,
        "safe_to_display": not hits,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _result_review_master_status(
    result: Mapping[str, Any],
    coverage: Mapping[str, Any],
    owner: Mapping[str, Any],
    dry_run: Mapping[str, Any],
) -> str:
    recommendation = _mapping(owner.get("summary")).get("watchlist_recommendation")
    if result.get("status") == "LAYER1_SELECTOR_RESULT_SUMMARY_BLOCKED":
        return "LAYER1_SELECTOR_BLOCKED"
    if coverage.get("status") == "HISTORY_COVERAGE_BLOCKED":
        return "LAYER1_SELECTOR_NEEDS_HISTORY_BACKFILL"
    if recommendation == "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING":
        return "LAYER1_SELECTOR_FORWARD_AGING_REVIEWABLE"
    if dry_run.get("status") in {
        "LAYER1_SELECTOR_FORWARD_DRY_RUN_PASS",
        "LAYER1_SELECTOR_FORWARD_DRY_RUN_WARN",
    }:
        return "LAYER1_SELECTOR_DRY_RUN_ONLY"
    return "LAYER1_SELECTOR_RESEARCH_ONLY"


def _result_review_master_answers(
    result: Mapping[str, Any],
    coverage: Mapping[str, Any],
    recent: Mapping[str, Any],
    owner: Mapping[str, Any],
    dry_run: Mapping[str, Any],
    blocker: Mapping[str, Any],
) -> list[dict[str, str]]:
    result_summary = _mapping(result.get("summary"))
    coverage_summary = _mapping(coverage.get("summary"))
    owner_summary = _mapping(owner.get("summary"))
    blocker_summary = _mapping(blocker.get("summary"))
    return [
        {
            "question": "当前是否存在明确优于组件基准的 selector？",
            "answer": str(result_summary.get("top_selector_id") or "NO"),
        },
        {
            "question": "成本后优势是否成立？",
            "answer": "YES" if result_summary.get("cost_after_edge_exists") else "NO",
        },
        {
            "question": "当前历史区间是否过短？",
            "answer": str(coverage_summary.get("research_conclusion_strength")),
        },
        {
            "question": "是否需要回补更长历史？",
            "answer": "YES" if blocker_summary.get("history_backfill_required") else "RECOMMENDED",
        },
        {
            "question": "是否只允许 recent-regime-only 结论？",
            "answer": "YES" if recent.get("status") == "RECENT_REGIME_RISK_MATERIAL" else "NO",
        },
        {
            "question": "是否允许进入 research-only forward-aging watchlist？",
            "answer": str(owner_summary.get("watchlist_recommendation")),
        },
        {
            "question": "是否只允许 dry-run，不允许正式 observation？",
            "answer": "YES; observation_written=false",
        },
        {"question": "是否继续禁止 ML selector？", "answer": "YES"},
        {
            "question": "是否继续排除 QQQ-plus growth / tail-risk fallback / LEAPS / Wheel？",
            "answer": "YES",
        },
        {
            "question": "下一阶段最小任务是什么？",
            "answer": (
                "owner manual review of research-only watchlist admission and dry-run output; "
                "no formal observation before approval"
            ),
        },
    ]


def _copy_markdown_artifact(payload: Mapping[str, Any], target_path: Path) -> None:
    paths = _mapping(payload.get("artifact_paths"))
    markdown_path = Path(str(paths.get("markdown_path", "")))
    if not markdown_path.exists():
        return
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(markdown_path.read_text(encoding="utf-8"), encoding="utf-8")


def _build_context(
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
    return _layer1_context(
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


def _load_registry(path: Path) -> dict[str, Any]:
    loaded = safe_load_yaml_path(path)
    if not isinstance(loaded, dict):
        raise ValueError(f"selector registry must be a mapping: {path}")
    return loaded


def _selector_rows(registry: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return _records(registry.get("selectors"))


def _selector_by_id(registry: Mapping[str, Any], selector_id: str) -> Mapping[str, Any]:
    for selector in _selector_rows(registry):
        if selector.get("selector_id") == selector_id:
            return selector
    return {}


def _selector_registry_review_row(
    selector: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    selector_id = str(selector.get("selector_id") or "")
    missing_fields = sorted(REQUIRED_SELECTOR_FIELDS - set(selector))
    allowed_components = set(str(value) for value in selector.get("allowed_components", []))
    selectable_components = set(
        str(value) for value in registry.get("selectable_component_ids", [])
    )
    safety_issues = []
    if selector.get("uses_future_data") is not False:
        safety_issues.append("uses_future_data_not_false")
    if selector.get("uses_ml") is not False:
        safety_issues.append("uses_ml_not_false")
    if selector.get("uses_options") is not False:
        safety_issues.append("uses_options_not_false")
    if selector.get("paper_shadow_allowed") is not False:
        safety_issues.append("paper_shadow_allowed_not_false")
    if selector.get("production_allowed") is not False:
        safety_issues.append("production_allowed_not_false")
    if selector.get("broker_action") != "none":
        safety_issues.append("broker_action_not_none")
    if not allowed_components <= selectable_components:
        safety_issues.append("non_selectable_component_in_allowed_components")
    status = "BLOCKED" if missing_fields or safety_issues else "READY"
    return {
        "selector_id": selector_id,
        "selector_type": selector.get("selector_type"),
        "status": status,
        "allowed_components": list(selector.get("allowed_components", [])),
        "feature_inputs": list(selector.get("feature_inputs", [])),
        "decision_rule": selector.get("decision_rule"),
        "switching_constraint": selector.get("switching_constraint"),
        "minimum_holding_period": selector.get("minimum_holding_period"),
        "cooldown_days": selector.get("cooldown_days"),
        "max_switches_per_year": selector.get("max_switches_per_year"),
        "max_turnover_per_switch": selector.get("max_turnover_per_switch"),
        "uses_future_data": selector.get("uses_future_data"),
        "uses_ml": selector.get("uses_ml"),
        "uses_options": selector.get("uses_options"),
        "paper_shadow_allowed": selector.get("paper_shadow_allowed"),
        "production_allowed": selector.get("production_allowed"),
        "broker_action": selector.get("broker_action"),
        "missing_fields": missing_fields,
        "safety_issues": safety_issues,
    }


def _selector_report_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    rows_field: str,
    rows: list[Mapping[str, Any]],
    extra_summary: Mapping[str, Any] | None = None,
    extra_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "row_count": len(rows),
        "data_quality_status": context.get("data_quality_status"),
        "actual_requested_date_range": _actual_date_range(context),
        "selector_registry_version": registry.get("registry_version"),
        "selectable_component_ids": list(SELECTABLE_COMPONENT_IDS),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    summary.update(dict(extra_summary or {}))
    payload = _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary=summary,
        selector_registry_version=registry.get("registry_version"),
        source_artifacts=context.get("source_artifacts", {}),
        **{rows_field: rows},
        **dict(extra_payload or {}),
    )
    return payload


def _actual_date_range(context: Mapping[str, Any]) -> dict[str, str | None]:
    panel = _ensure_frame(context["panel"])
    if panel.empty:
        return {"start": None, "end": None}
    dates = sorted(str(value) for value in panel["date"].dropna().unique())
    return {"start": dates[0], "end": dates[-1]} if dates else {"start": None, "end": None}


def _feature_frame(context: Mapping[str, Any]) -> pd.DataFrame:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    qqq = returns.get("100_qqq", pd.Series(dtype=float)).fillna(0.0)
    equal = returns.get("equal_risk_qqq_sgov", pd.Series(dtype=float)).reindex(qqq.index)
    equal = equal.fillna(0.0)
    equity = (1.0 + qqq).cumprod()
    frame = pd.DataFrame(index=qqq.index)
    frame["qqq_return_1d"] = qqq
    frame["equal_risk_return_1d"] = equal
    frame["qqq_normalized_equity"] = equity
    frame["qqq_100dma"] = equity.rolling(100, min_periods=20).mean().bfill()
    frame["qqq_200dma"] = equity.rolling(200, min_periods=20).mean().bfill()
    frame["distance_to_200dma"] = _safe_series_divide(
        frame["qqq_normalized_equity"] - frame["qqq_200dma"],
        frame["qqq_200dma"],
    )
    frame["realized_vol_20d"] = qqq.rolling(20, min_periods=2).std().fillna(0.0)
    frame["realized_vol_60d"] = qqq.rolling(60, min_periods=2).std().fillna(0.0)
    frame["realized_vol_20d_percentile"] = _rolling_latest_percentile(
        frame["realized_vol_20d"],
        252,
    )
    frame["realized_vol_60d_percentile"] = _rolling_latest_percentile(
        frame["realized_vol_60d"],
        252,
    )
    baseline_vol = frame["realized_vol_20d"].rolling(60, min_periods=20).mean()
    frame["volatility_expansion_ratio"] = _safe_series_divide(
        frame["realized_vol_20d"],
        baseline_vol,
    ).fillna(0.0)
    frame["drawdown_from_252d_high"] = equity / equity.rolling(252, min_periods=20).max() - 1.0
    frame["trend_signal_on"] = frame["qqq_normalized_equity"] > frame["qqq_200dma"]
    frame["trend_100_200_signal_on"] = (
        (frame["qqq_normalized_equity"] > frame["qqq_100dma"])
        & (frame["qqq_100dma"] > frame["qqq_200dma"])
    )
    return frame.fillna(0.0)


def _safe_series_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float).divide(denominator.astype(float).replace(0.0, math.nan))


def _rolling_latest_percentile(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=5).apply(
        lambda values: float((values <= values[-1]).mean()),
        raw=True,
    ).fillna(0.5)


def _selector_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    selector_id: str,
    *,
    minimum_holding_period: int | None = None,
    cooldown_days: int | None = None,
    max_switches_per_year: int | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> BlendPath:
    selector = _selector_by_id(registry, selector_id)
    if not selector:
        raise ValueError(f"unknown selector_id: {selector_id}")
    features = _feature_frame(context)
    raw = _raw_selector_path(features, selector, overrides or {})
    constraints = _selector_constraints(
        selector,
        minimum_holding_period=minimum_holding_period,
        cooldown_days=cooldown_days,
        max_switches_per_year=max_switches_per_year,
    )
    return _apply_switching_constraints(raw, constraints)


def _raw_selector_path(
    features: pd.DataFrame,
    selector: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> BlendPath:
    selector_id = str(selector.get("selector_id"))
    rule = dict(_mapping(selector.get("decision_rule")))
    rule.update(dict(overrides))
    path: BlendPath = {}
    for day, row in features.iterrows():
        path[str(day)] = _weights_for_selector(selector_id, row, rule)
    return path


def _weights_for_selector(
    selector_id: str,
    row: pd.Series,
    rule: Mapping[str, Any],
) -> dict[str, float]:
    if selector_id == "always_equal_risk":
        return _hard_weights("equal_risk_qqq_sgov")
    if selector_id == "always_100_qqq":
        return _hard_weights("100_qqq")
    if selector_id == "trend_200dma_selector":
        return _hard_weights("100_qqq" if bool(row["trend_signal_on"]) else "equal_risk_qqq_sgov")
    if selector_id == "trend_100_200dma_selector":
        return _hard_weights(
            "100_qqq" if bool(row["trend_100_200_signal_on"]) else "equal_risk_qqq_sgov"
        )
    if selector_id == "trend_distance_200dma_selector":
        threshold = _float(rule.get("distance_threshold"))
        return _hard_weights(
            "100_qqq"
            if _float(row["distance_to_200dma"]) > threshold
            else "equal_risk_qqq_sgov"
        )
    if selector_id in {"realized_vol_selector", "realized_vol_60d_selector"}:
        window = _int(rule.get("vol_window"), 20)
        column = "realized_vol_60d_percentile" if window == 60 else "realized_vol_20d_percentile"
        high = _float(rule.get("high_percentile"), 0.66)
        return _hard_weights(
            "equal_risk_qqq_sgov" if _float(row[column]) >= high else "100_qqq"
        )
    if selector_id == "volatility_expansion_selector":
        threshold = _float(rule.get("expansion_threshold"), 1.25)
        return _hard_weights(
            "equal_risk_qqq_sgov"
            if _float(row["volatility_expansion_ratio"]) >= threshold
            else "100_qqq"
        )
    if selector_id in {"drawdown_guard_selector", "drawdown_guard_10pct_selector"}:
        threshold = _float(rule.get("drawdown_threshold"), -0.08)
        return _hard_weights(
            "equal_risk_qqq_sgov"
            if _float(row["drawdown_from_252d_high"]) <= threshold
            else "100_qqq"
        )
    if selector_id == "trend_plus_vol_selector":
        high = _float(rule.get("high_percentile"), 0.66)
        risk_on = bool(row["trend_signal_on"]) and _float(row["realized_vol_20d_percentile"]) < high
        return _hard_weights("100_qqq" if risk_on else "equal_risk_qqq_sgov")
    if selector_id == "trend_plus_drawdown_selector":
        threshold = _float(rule.get("drawdown_threshold"), -0.08)
        risk_on = (
            bool(row["trend_signal_on"])
            and _float(row["drawdown_from_252d_high"]) > threshold
        )
        return _hard_weights("100_qqq" if risk_on else "equal_risk_qqq_sgov")
    if selector_id == "three_signal_vote_selector":
        high = _float(rule.get("high_percentile"), 0.66)
        drawdown_threshold = _float(rule.get("drawdown_threshold"), -0.08)
        votes = [
            bool(row["trend_signal_on"]),
            _float(row["realized_vol_20d_percentile"]) < high,
            _float(row["drawdown_from_252d_high"]) > drawdown_threshold,
        ]
        selected = (
            "100_qqq"
            if sum(votes) >= _int(rule.get("vote_threshold"), 2)
            else "equal_risk_qqq_sgov"
        )
        return _hard_weights(selected)
    if selector_id == "soft_blend_trend_selector":
        weight = (
            _float(rule.get("risk_on_weight"), 0.75)
            if _float(row["distance_to_200dma"]) > 0.0
            else _float(rule.get("risk_off_weight"), 0.25)
        )
        return _blend_weights(weight)
    if selector_id == "soft_blend_vol_selector":
        low = _float(rule.get("min_100qqq_weight"), 0.25)
        high = _float(rule.get("max_100qqq_weight"), 0.75)
        vol_pct = _float(row["realized_vol_20d_percentile"], 0.5)
        return _blend_weights(high - (high - low) * vol_pct)
    if selector_id == "soft_blend_trend_vol_drawdown_selector":
        low = _float(rule.get("min_100qqq_weight"), 0.20)
        high = _float(rule.get("max_100qqq_weight"), 0.85)
        drawdown_threshold = _float(rule.get("drawdown_threshold"), -0.08)
        trend_score = 1.0 if _float(row["distance_to_200dma"]) > 0.0 else 0.0
        vol_score = 1.0 - _float(row["realized_vol_20d_percentile"], 0.5)
        drawdown_score = 1.0 if _float(row["drawdown_from_252d_high"]) > drawdown_threshold else 0.0
        score = (trend_score + vol_score + drawdown_score) / 3.0
        return _blend_weights(low + (high - low) * score)
    return _hard_weights("equal_risk_qqq_sgov")


def _hard_weights(component: str) -> dict[str, float]:
    return {
        "equal_risk_qqq_sgov": 1.0 if component == "equal_risk_qqq_sgov" else 0.0,
        "100_qqq": 1.0 if component == "100_qqq" else 0.0,
    }


def _blend_weights(weight_100_qqq: float) -> dict[str, float]:
    weight = min(max(_float(weight_100_qqq), 0.0), 1.0)
    return {"equal_risk_qqq_sgov": 1.0 - weight, "100_qqq": weight}


def _selector_constraints(
    selector: Mapping[str, Any],
    *,
    minimum_holding_period: int | None = None,
    cooldown_days: int | None = None,
    max_switches_per_year: int | None = None,
) -> dict[str, Any]:
    constraint = dict(_mapping(selector.get("switching_constraint")))
    constraint["minimum_holding_period_days"] = (
        minimum_holding_period
        if minimum_holding_period is not None
        else _int(
            selector.get("minimum_holding_period"),
            _int(constraint.get("minimum_holding_period_days"), 20),
        )
    )
    constraint["cooldown_days"] = (
        cooldown_days
        if cooldown_days is not None
        else _int(selector.get("cooldown_days"), _int(constraint.get("cooldown_days"), 5))
    )
    constraint["max_switches_per_year"] = (
        max_switches_per_year
        if max_switches_per_year is not None
        else _int(
            selector.get("max_switches_per_year"),
            _int(constraint.get("max_switches_per_year"), 12),
        )
    )
    constraint["max_turnover_per_switch"] = _float(
        selector.get("max_turnover_per_switch"),
        _float(constraint.get("max_turnover_per_switch"), 1.0),
    )
    return constraint


def _apply_switching_constraints(path: BlendPath, constraints: Mapping[str, Any]) -> BlendPath:
    result: BlendPath = {}
    dates = sorted(path)
    if not dates:
        return result
    current = path[dates[0]]
    last_change_index = 0
    switches_by_year: dict[int, int] = defaultdict(int)
    result[dates[0]] = current
    min_holding = _int(constraints.get("minimum_holding_period_days"), 20)
    cooldown = _int(constraints.get("cooldown_days"), 5)
    max_switches = _int(constraints.get("max_switches_per_year"), 12)
    max_turnover = _float(constraints.get("max_turnover_per_switch"), 1.0)
    for index, day in enumerate(dates[1:], start=1):
        target = path[day]
        if _weights_close(current, target):
            result[day] = current
            continue
        year = pd.to_datetime(day).year
        enough_time = index - last_change_index >= min_holding + cooldown
        switch_allowed = enough_time and switches_by_year[year] < max_switches
        if not switch_allowed:
            result[day] = current
            continue
        turnover = _blend_turnover(current, target)
        if turnover > max_turnover:
            target = _cap_turnover(current, target, max_turnover)
        current = target
        last_change_index = index
        switches_by_year[year] += 1
        result[day] = current
    return result


def _weights_close(left: Mapping[str, float], right: Mapping[str, float]) -> bool:
    return all(
        abs(_float(left.get(component)) - _float(right.get(component))) < 1e-9
        for component in SELECTABLE_COMPONENT_IDS
    )


def _cap_turnover(
    current: Mapping[str, float],
    target: Mapping[str, float],
    max_turnover: float,
) -> dict[str, float]:
    turnover = _blend_turnover(current, target)
    if turnover <= max_turnover or turnover <= 0.0:
        return dict(target)
    fraction = max_turnover / turnover
    weight_100 = _float(current.get("100_qqq")) + (
        _float(target.get("100_qqq")) - _float(current.get("100_qqq"))
    ) * fraction
    return _blend_weights(weight_100)


def _evaluate_blend_path(
    context: Mapping[str, Any],
    path: BlendPath,
    *,
    cost_bps: float | None = None,
    execution_lag_days: int = 1,
) -> dict[str, Any]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    dates = sorted(day for day in path if day in returns.index)
    if not dates:
        return _empty_metrics()
    if cost_bps is None:
        cost_bps = _float(
            _mapping(_mapping(context["config"]).get("research_policy"))
            .get("cost_assumption", {})
            .get("base_cost_bps"),
            5.0,
        )
    gross_values = []
    net_values = []
    turnover = 0.0
    switch_count = 0
    switch_count_by_year: dict[str, int] = defaultdict(int)
    turnover_by_year: dict[str, float] = defaultdict(float)
    last_weights: Mapping[str, float] | None = None
    for index, day in enumerate(dates):
        future_index = index + execution_lag_days
        if future_index >= len(dates):
            break
        weights = path[day]
        switch_cost = 0.0
        if last_weights is not None and not _weights_close(last_weights, weights):
            switch_turnover = _blend_turnover(last_weights, weights)
            turnover += switch_turnover
            switch_count += 1
            switch_year = str(pd.to_datetime(day).year)
            switch_count_by_year[switch_year] += 1
            turnover_by_year[switch_year] += switch_turnover
            switch_cost = switch_turnover * cost_bps / 10000.0
        future_day = dates[future_index]
        gross_return = sum(
            _float(weights.get(component)) * _float(returns.loc[future_day].get(component))
            for component in SELECTABLE_COMPONENT_IDS
            if component in returns.columns
        )
        gross_values.append(gross_return)
        net_values.append(gross_return - switch_cost)
        last_weights = weights
    gross_series = pd.Series(gross_values, dtype=float)
    net_series = pd.Series(net_values, dtype=float)
    return_stats = _return_metrics(net_series, pd.Series(dtype=float))
    gross_return = _compound_return(gross_series)
    net_return = _compound_return(net_series)
    static = _static_component_metrics(context)
    best_static = max(_float(row["net_return_after_cost"]) for row in static.values())
    switch_count_by_year_row = dict(sorted(switch_count_by_year.items()))
    turnover_by_year_row = {
        year: _round(value) for year, value in sorted(turnover_by_year.items())
    }
    return {
        "gross_return": gross_return,
        "net_return_after_cost": net_return,
        "max_drawdown": return_stats["max_drawdown"],
        "sharpe": return_stats["sharpe"],
        "calmar": return_stats["calmar"],
        "turnover": turnover,
        "switch_count": switch_count,
        "avg_holding_period": len(dates) / max(switch_count + 1, 1),
        "cost_drag": gross_return - net_return,
        "regret_vs_best_component": best_static - net_return,
        "relative_vs_equal_risk": net_return
        - _float(static["equal_risk_qqq_sgov"]["net_return_after_cost"]),
        "relative_vs_100_qqq": net_return - _float(static["100_qqq"]["net_return_after_cost"]),
        "selected_component_distribution": _component_distribution(path),
        "switch_count_by_year": switch_count_by_year_row,
        "turnover_by_year": turnover_by_year_row,
        "max_switches_per_year_observed": max(switch_count_by_year.values(), default=0),
        "max_switches_per_3y_observed": _max_rolling_three_year_switches(
            switch_count_by_year,
        ),
        "max_turnover_per_year_observed": max(turnover_by_year.values(), default=0.0),
    }


def _empty_metrics() -> dict[str, Any]:
    return {
        "gross_return": 0.0,
        "net_return_after_cost": 0.0,
        "max_drawdown": 0.0,
        "sharpe": 0.0,
        "calmar": 0.0,
        "turnover": 0.0,
        "switch_count": 0,
        "avg_holding_period": 0.0,
        "cost_drag": 0.0,
        "regret_vs_best_component": 0.0,
        "relative_vs_equal_risk": 0.0,
        "relative_vs_100_qqq": 0.0,
        "selected_component_distribution": {},
        "switch_count_by_year": {},
        "turnover_by_year": {},
        "max_switches_per_year_observed": 0,
        "max_switches_per_3y_observed": 0,
        "max_turnover_per_year_observed": 0.0,
    }


def _max_rolling_three_year_switches(switch_count_by_year: Mapping[str, int]) -> int:
    if not switch_count_by_year:
        return 0
    years = sorted(_int(year) for year in switch_count_by_year)
    if not years:
        return 0
    first_year = years[0]
    last_year = years[-1]
    counts = {
        _int(year): _int(count)
        for year, count in switch_count_by_year.items()
    }
    return max(
        sum(counts.get(year, 0) for year in range(window_start, window_start + 3))
        for window_start in range(first_year, last_year + 1)
    )


def _required_metric_row(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    selector_id: str,
    *,
    path: BlendPath | None = None,
) -> dict[str, Any]:
    selected_path = path or _selector_path(context, registry, selector_id)
    metrics = _evaluate_blend_path(context, selected_path)
    return {
        "selector_id": selector_id,
        "gross_return": _round(metrics["gross_return"]),
        "net_return_after_cost": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "sharpe": _round(metrics["sharpe"]),
        "calmar": _round(metrics["calmar"]),
        "turnover": _round(metrics["turnover"]),
        "switch_count": metrics["switch_count"],
        "avg_holding_period": _round(metrics["avg_holding_period"]),
        "cost_drag": _round(metrics["cost_drag"]),
        "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
        "relative_vs_equal_risk": _round(metrics["relative_vs_equal_risk"]),
        "relative_vs_100_qqq": _round(metrics["relative_vs_100_qqq"]),
    }


def _static_component_metrics(context: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    result: dict[str, dict[str, Any]] = {}
    for component in SELECTABLE_COMPONENT_IDS:
        series = returns.get(component, pd.Series(dtype=float)).fillna(0.0)
        metrics = _return_metrics(series, pd.Series(dtype=float))
        result[component] = {
            "net_return_after_cost": _compound_return(series),
            "max_drawdown": metrics["max_drawdown"],
            "sharpe": metrics["sharpe"],
            "calmar": metrics["calmar"],
            "turnover": 0.0,
            "switch_count": 0,
            "regret_vs_best_component": 0.0,
        }
    best = max(_float(row["net_return_after_cost"]) for row in result.values()) if result else 0.0
    for row in result.values():
        row["regret_vs_best_component"] = best - _float(row["net_return_after_cost"])
    return result


def _reference_component_metrics(context: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    result: dict[str, dict[str, Any]] = {}
    for component in REFERENCE_COMPONENT_IDS:
        if component not in returns.columns:
            continue
        series = returns[component].fillna(0.0)
        metrics = _return_metrics(series, pd.Series(dtype=float))
        result[component] = {
            "net_return_after_cost": _compound_return(series),
            "max_drawdown": metrics["max_drawdown"],
            "sharpe": metrics["sharpe"],
            "calmar": metrics["calmar"],
            "turnover": 0.0,
            "switch_count": 0,
            "regret_vs_best_component": 0.0,
        }
    return result


def _edge_status(
    rows: list[Mapping[str, Any]],
    *,
    edge_status: str,
    no_edge_status: str,
    mixed_status: str,
    registry: Mapping[str, Any],
) -> str:
    if not rows:
        return no_edge_status
    threshold = _float(_evaluation_policy(registry).get("edge_net_return_threshold"))
    edge_rows = [
        row
        for row in rows
        if _float(row.get("relative_vs_equal_risk")) > threshold
        or _float(row.get("relative_vs_100_qqq")) > threshold
    ]
    if len(edge_rows) == len(rows):
        return edge_status
    if edge_rows:
        return mixed_status
    return no_edge_status


def _evaluation_policy(registry: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(registry.get("evaluation_policy"))


def _best_metric_row(rows: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    if not rows:
        return {}
    return max(
        rows,
        key=lambda row: (
            _float(row.get("net_return_after_cost")),
            _float(row.get("calmar")),
            -_float(row.get("turnover")),
        ),
    )


def _best_selector_id_for_registry(context: Mapping[str, Any], registry: Mapping[str, Any]) -> str:
    selector_ids = _ranking_selector_ids(registry)
    scored = []
    for selector_id in selector_ids:
        metrics = _evaluate_blend_path(context, _selector_path(context, registry, selector_id))
        scored.append((selector_id, metrics))
    if not scored:
        return "always_equal_risk"
    return max(
        scored,
        key=lambda item: (
            _float(item[1]["net_return_after_cost"]),
            _float(item[1]["calmar"]),
            -_float(item[1]["turnover"]),
        ),
    )[0]


def _ranking_selector_ids(registry: Mapping[str, Any]) -> list[str]:
    preferred = [
        "always_equal_risk",
        "always_100_qqq",
        "trend_200dma_selector",
        "trend_100_200dma_selector",
        "trend_distance_200dma_selector",
        "realized_vol_selector",
        "realized_vol_60d_selector",
        "volatility_expansion_selector",
        "drawdown_guard_selector",
        "drawdown_guard_10pct_selector",
        "trend_plus_vol_selector",
        "trend_plus_drawdown_selector",
        "three_signal_vote_selector",
        "soft_blend_trend_selector",
        "soft_blend_vol_selector",
        "soft_blend_trend_vol_drawdown_selector",
    ]
    available = {str(selector.get("selector_id")) for selector in _selector_rows(registry)}
    return [selector_id for selector_id in preferred if selector_id in available]


def _complexity_score(selector: Mapping[str, Any]) -> float:
    feature_count = len(selector.get("feature_inputs", []))
    rule = _mapping(selector.get("decision_rule"))
    soft_penalty = 1 if str(selector.get("selector_type", "")).startswith("soft") else 0
    return float(feature_count + len(rule) * 0.25 + soft_penalty)


def _component_distribution(path: BlendPath) -> dict[str, float]:
    if not path:
        return {}
    return {
        component: _round(
            sum(_float(weights.get(component)) for weights in path.values()) / len(path)
        )
        for component in SELECTABLE_COMPONENT_IDS
    }


def _blend_turnover(left: Mapping[str, float], right: Mapping[str, float]) -> float:
    return sum(
        abs(_float(left.get(component)) - _float(right.get(component)))
        for component in SELECTABLE_COMPONENT_IDS
    ) / 2.0


def _false_signal_counts(context: Mapping[str, Any], path: BlendPath) -> dict[str, int]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    dates = sorted(day for day in path if day in returns.index)
    false_defensive = 0
    false_risk_on = 0
    for index, day in enumerate(dates[:-1]):
        selected = _dominant_component(path[day])
        next_day = dates[index + 1]
        equal_return = _float(returns.loc[next_day].get("equal_risk_qqq_sgov"))
        qqq_return = _float(returns.loc[next_day].get("100_qqq"))
        if selected == "equal_risk_qqq_sgov" and qqq_return > equal_return:
            false_defensive += 1
        if selected == "100_qqq" and equal_return > qqq_return:
            false_risk_on += 1
    return {
        "false_defensive_periods": false_defensive,
        "false_risk_on_periods": false_risk_on,
    }


def _drawdown_diagnostics(context: Mapping[str, Any], path: BlendPath) -> dict[str, Any]:
    metrics = _evaluate_blend_path(context, path)
    static = _static_component_metrics(context)
    drawdown_reduction = abs(_float(static["100_qqq"]["max_drawdown"])) - abs(
        _float(metrics["max_drawdown"])
    )
    false_counts = _false_signal_counts(context, path)
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    missed = []
    dates = sorted(day for day in path if day in returns.index)
    for index, day in enumerate(dates[:-1]):
        if _dominant_component(path[day]) != "equal_risk_qqq_sgov":
            continue
        next_day = dates[index + 1]
        gap = _float(returns.loc[next_day].get("100_qqq")) - _float(
            returns.loc[next_day].get("equal_risk_qqq_sgov")
        )
        if gap > 0.0:
            missed.append(gap)
    return {
        "drawdown_reduction_vs_100_qqq": _round(drawdown_reduction),
        "missed_rebound_cost": _round(sum(missed)),
        "late_risk_on_count": false_counts["false_defensive_periods"],
        "late_risk_off_count": false_counts["false_risk_on_periods"],
    }


def _dominant_component(weights: Mapping[str, float]) -> str:
    return max(SELECTABLE_COMPONENT_IDS, key=lambda component: _float(weights.get(component)))


def _cost_latency_scenarios() -> list[dict[str, Any]]:
    return [
        {"scenario": "zero_cost", "cost_bps": 0.0, "execution_lag_days": 1},
        {"scenario": "low_cost", "cost_bps": 2.5, "execution_lag_days": 1},
        {"scenario": "medium_cost", "cost_bps": 5.0, "execution_lag_days": 1},
        {"scenario": "high_cost", "cost_bps": 15.0, "execution_lag_days": 1},
        {"scenario": "one_day_execution_lag", "cost_bps": 5.0, "execution_lag_days": 2},
        {"scenario": "two_day_execution_lag", "cost_bps": 5.0, "execution_lag_days": 3},
        {"scenario": "weekly_rebalance_only", "cost_bps": 5.0, "execution_lag_days": 1},
        {"scenario": "monthly_rebalance_only", "cost_bps": 5.0, "execution_lag_days": 1},
        {"scenario": "threshold_rebalance_only", "cost_bps": 5.0, "execution_lag_days": 1},
    ]


def _scenario_path(path: BlendPath, scenario: str) -> BlendPath:
    if scenario not in {
        "weekly_rebalance_only",
        "monthly_rebalance_only",
        "threshold_rebalance_only",
    }:
        return path
    result: BlendPath = {}
    current: Mapping[str, float] | None = None
    last_period: object = None
    for day in sorted(path):
        target = path[day]
        timestamp = pd.to_datetime(day)
        if current is None:
            current = target
        elif scenario == "weekly_rebalance_only":
            period = timestamp.isocalendar().week
            if period != last_period:
                current = target
            last_period = period
        elif scenario == "monthly_rebalance_only":
            period = (timestamp.year, timestamp.month)
            if period != last_period:
                current = target
            last_period = period
        elif _blend_turnover(current, target) >= 0.20:
            current = target
        result[day] = dict(current)
    return result


def _monthly_execution_path(path: BlendPath) -> BlendPath:
    return _scenario_path(path, "monthly_rebalance_only")


def _monthly_signal_soft_blend_path(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> BlendPath:
    signal = _soft_blend_200dma_path(context, registry)
    result: BlendPath = {}
    current: Mapping[str, float] | None = None
    last_period: tuple[int, int] | None = None
    previous_day: str | None = None
    for day in sorted(signal):
        timestamp = pd.to_datetime(day)
        period = (timestamp.year, timestamp.month)
        if current is None:
            current = signal[day]
        elif period != last_period and previous_day is not None:
            current = signal[previous_day]
        result[day] = dict(current)
        previous_day = day
        last_period = period
    return result


def _switch_quality_rows(
    context: Mapping[str, Any],
    path: BlendPath,
    selector_id: str,
) -> list[dict[str, Any]]:
    returns = _returns_frame(context)
    dates = sorted(day for day in path if day in returns.index)
    if len(dates) < 2:
        return []
    cost_bps = _cost_bps(context)
    rows: list[dict[str, Any]] = []
    last_weights = path[dates[0]]
    for index, day in enumerate(dates[1:], start=1):
        weights = path[day]
        if _weights_close(last_weights, weights):
            continue
        switch_turnover = _blend_turnover(last_weights, weights)
        turnover_cost = switch_turnover * cost_bps / 10000.0
        old_20, available_20 = _future_weighted_return(
            returns,
            dates,
            index,
            last_weights,
            horizon=20,
        )
        new_20, _ = _future_weighted_return(returns, dates, index, weights, horizon=20)
        old_60, available_60 = _future_weighted_return(
            returns,
            dates,
            index,
            last_weights,
            horizon=60,
        )
        new_60, _ = _future_weighted_return(returns, dates, index, weights, horizon=60)
        benefit_60 = new_60 - old_60
        rows.append(
            {
                "selector_id": selector_id,
                "switch_date": day,
                "from_state": _weight_state(last_weights),
                "to_state": _weight_state(weights),
                "outcome_20d_after_switch": {
                    "available_days": available_20,
                    "switched_return": _round(new_20),
                    "not_switch_return": _round(old_20),
                    "switch_benefit_vs_not_switch": _round(new_20 - old_20),
                },
                "outcome_60d_after_switch": {
                    "available_days": available_60,
                    "switched_return": _round(new_60),
                    "not_switch_return": _round(old_60),
                    "switch_benefit_vs_not_switch": _round(benefit_60),
                },
                "switch_benefit_vs_not_switch": _round(benefit_60),
                "turnover_cost": _round(turnover_cost),
                "switch_turnover": _round(switch_turnover),
                "net_switch_value": _round(benefit_60 - turnover_cost),
            }
        )
        last_weights = weights
    return rows


def _future_weighted_return(
    returns: pd.DataFrame,
    dates: list[str],
    start_index: int,
    weights: Mapping[str, float],
    *,
    horizon: int,
) -> tuple[float, int]:
    start = start_index + 1
    end = min(start + horizon, len(dates))
    if start >= end:
        return 0.0, 0
    daily = []
    for day in dates[start:end]:
        daily.append(
            sum(
                _float(weights.get(component)) * _float(returns.loc[day].get(component))
                for component in SELECTABLE_COMPONENT_IDS
                if component in returns.columns
            )
        )
    return _compound_return(pd.Series(daily, dtype=float)), len(daily)


def _weight_state(weights: Mapping[str, float]) -> str:
    weight_100 = _float(weights.get("100_qqq"))
    if weight_100 >= 0.70:
        bucket = "risk_on"
    elif weight_100 <= 0.30:
        bucket = "risk_off"
    else:
        bucket = "neutral"
    return f"{bucket}_{int(round(weight_100 * 100))}pct_100qqq"


def _selector_vs_simple_component_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    finalist = _best_finalist_row(context, registry)
    rows: list[dict[str, Any]] = []
    static = _static_component_metrics(context)
    reference = _reference_component_metrics(context)
    for variant_id, component_id in (
        ("always_equal_risk", "equal_risk_qqq_sgov"),
        ("always_100_qqq", "100_qqq"),
    ):
        source = static[component_id]
        rows.append(_component_comparison_row(variant_id, "simple_component", source))
    for variant_id in ("qqq_50_sgov_50", "qqq_60_sgov_40"):
        if variant_id in reference:
            rows.append(
                _component_comparison_row(
                    variant_id,
                    "reference_component",
                    reference[variant_id],
                ),
            )
    if finalist:
        selector_row = dict(finalist)
        selector_row["role"] = "best_low_turnover_selector"
        rows.append(selector_row)
    best_component_return = max(
        (
            _float(row.get("net_return_after_cost"))
            for row in rows
            if row["role"] != "best_low_turnover_selector"
        ),
        default=0.0,
    )
    for row in rows:
        row["regret_vs_best_component"] = _round(
            best_component_return - _float(row.get("net_return_after_cost")),
        )
    rows.sort(
        key=lambda row: (
            _float(row.get("net_return_after_cost")),
            _float(row.get("calmar")),
            -_float(row.get("turnover")),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _component_comparison_row(
    variant_id: str,
    role: str,
    source: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "rank": 0,
        "variant_id": variant_id,
        "variant_family": role,
        "role": role,
        "net_return_after_cost": _round(source.get("net_return_after_cost")),
        "max_drawdown": _round(source.get("max_drawdown")),
        "sharpe": _round(source.get("sharpe")),
        "calmar": _round(source.get("calmar")),
        "turnover": _round(source.get("turnover")),
        "switch_count": _int(source.get("switch_count")),
        "avg_holding_period": 0.0,
        "relative_vs_equal_risk": 0.0,
        "relative_vs_100_qqq": 0.0,
        "switch_count_controlled": True,
        "switch_count_control_failed_checks": [],
        "regret_vs_best_component": _round(source.get("regret_vs_best_component")),
    }


def _pause_or_continue_owner_answers(
    gate_pass: bool,
    selector: Mapping[str, Any],
    actual_range: Mapping[str, str | None],
) -> list[dict[str, Any]]:
    selector_id = selector.get("variant_id") or "none"
    return [
        {
            "question": "是否继续 Layer-1 selector？",
            "answer": "YES_RESEARCH_ONLY" if gate_pass else "NO_KEEP_DRY_RUN_RESEARCH_ONLY",
        },
        {
            "question": "是否只保留 dry-run？",
            "answer": "NO_LOW_TURNOVER_WATCHLIST_REVIEWABLE" if gate_pass else "YES",
        },
        {
            "question": "是否有低换手候选进入 forward-aging？",
            "answer": str(selector_id) if gate_pass else "NO",
        },
        {
            "question": "是否需要回补更长历史？",
            "answer": (
                "YES; current audited range starts at "
                f"{actual_range.get('start')} and remains AI-regime-only for primary conclusions"
            ),
        },
        {
            "question": "是否继续禁止 ML selector？",
            "answer": "YES",
        },
        {
            "question": "是否继续以 equal_risk forward-aging 为主线？",
            "answer": "YES",
        },
    ]


def _period_windows(context: Mapping[str, Any]) -> list[dict[str, Any]]:
    actual = _actual_date_range(context)
    latest = _date_or_none(actual["end"])
    return [
        {"period": "2012-2015", "start": date(2012, 1, 1), "end": date(2015, 12, 31)},
        {"period": "2016-2019", "start": date(2016, 1, 1), "end": date(2019, 12, 31)},
        {"period": "2020-2021", "start": date(2020, 1, 1), "end": date(2021, 12, 31)},
        {"period": "2022", "start": date(2022, 1, 1), "end": date(2022, 12, 31)},
        {"period": "2023", "start": date(2023, 1, 1), "end": date(2023, 12, 31)},
        {"period": "2024", "start": date(2024, 1, 1), "end": date(2024, 12, 31)},
        {"period": "2025-to-latest", "start": date(2025, 1, 1), "end": latest},
        {"period": "pre-2020", "start": None, "end": date(2019, 12, 31)},
        {"period": "post-2020", "start": date(2020, 1, 1), "end": latest},
        {"period": "rate-hike period", "start": date(2022, 3, 16), "end": date(2023, 7, 26)},
        {"period": "AI rally period", "start": date(2023, 1, 1), "end": latest},
    ]


def _path_in_window(path: BlendPath, start: date | None, end: date | None) -> BlendPath:
    result = {}
    for day, weights in path.items():
        current = pd.to_datetime(day).date()
        if start is not None and current < start:
            continue
        if end is not None and current > end:
            continue
        result[day] = weights
    return result


def _rank_by_metric(period_metrics: list[tuple[str, Mapping[str, Any]]]) -> dict[str, int]:
    ordered = sorted(
        period_metrics,
        key=lambda item: _float(item[1].get("net_return_after_cost")),
        reverse=True,
    )
    return {selector_id: index + 1 for index, (selector_id, _) in enumerate(ordered)}


def _period_metric_row(
    context: Mapping[str, Any],
    selector_id: str,
    period: Mapping[str, Any],
    metrics: Mapping[str, Any],
    rank: int,
) -> dict[str, Any]:
    path_length = len(
        _path_in_window(
            _selector_path(context, _context_registry_stub(), "always_equal_risk"),
            period.get("start"),
            period.get("end"),
        )
    )
    commentary = "covered" if path_length else "actual requested date range does not cover period"
    return {
        "period": period["period"],
        "selector_id": selector_id,
        "net_return": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "sharpe": _round(metrics["sharpe"]),
        "calmar": _round(metrics["calmar"]),
        "turnover": _round(metrics["turnover"]),
        "rank_in_period": rank,
        "relative_vs_equal_risk": _round(metrics["relative_vs_equal_risk"]),
        "relative_vs_100_qqq": _round(metrics["relative_vs_100_qqq"]),
        "period_commentary": commentary,
    }


def _context_registry_stub() -> dict[str, Any]:
    return {
        "selectors": [
            {
                "selector_id": "always_equal_risk",
                "decision_rule": {"component": "equal_risk_qqq_sgov"},
                "switching_constraint": {},
                "minimum_holding_period": 0,
                "cooldown_days": 0,
                "max_switches_per_year": 252,
                "max_turnover_per_switch": 1.0,
            }
        ]
    }


def _episode_windows(context: Mapping[str, Any]) -> list[dict[str, Any]]:
    features = _feature_frame(context)
    largest_drawdown = None
    if not features.empty:
        drawdown_day = str(features["drawdown_from_252d_high"].idxmin())
        drawdown_date = pd.to_datetime(drawdown_day).date()
        largest_drawdown = {
            "episode_name": "largest QQQ drawdown",
            "start": drawdown_date - timedelta(days=45),
            "end": drawdown_date + timedelta(days=45),
        }
    regret = _largest_component_regret_window(context)
    episodes = [
        {
            "episode_name": "2018Q4 selloff",
            "start": date(2018, 10, 1),
            "end": date(2018, 12, 24),
        },
        {
            "episode_name": "2020 COVID crash",
            "start": date(2020, 2, 19),
            "end": date(2020, 3, 23),
        },
        {
            "episode_name": "2022 rate-hike bear market",
            "start": date(2022, 1, 3),
            "end": date(2022, 10, 14),
        },
        {"episode_name": "2023 recovery", "start": date(2023, 1, 3), "end": date(2023, 7, 31)},
        {"episode_name": "2024 AI rally", "start": date(2024, 1, 2), "end": date(2024, 12, 31)},
    ]
    if largest_drawdown:
        episodes.append(largest_drawdown)
    episodes.append(regret)
    return episodes


def _largest_component_regret_window(context: Mapping[str, Any]) -> dict[str, Any]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    if returns.empty or not set(SELECTABLE_COMPONENT_IDS) <= set(returns.columns):
        return {
            "episode_name": "largest component regret episode",
            "start": None,
            "end": None,
        }
    gap = (returns["100_qqq"] - returns["equal_risk_qqq_sgov"]).abs()
    day = pd.to_datetime(str(gap.idxmax())).date()
    return {
        "episode_name": "largest component regret episode",
        "start": day - timedelta(days=20),
        "end": day + timedelta(days=20),
    }


def _episode_row(
    context: Mapping[str, Any],
    selector_id: str,
    path: BlendPath,
    episode: Mapping[str, Any],
) -> dict[str, Any]:
    start = episode.get("start")
    end = episode.get("end")
    episode_path = _path_in_window(path, start, end)
    metrics = _evaluate_blend_path(context, episode_path)
    selected_path = [
        {"date": day, "component": _dominant_component(weights)}
        for day, weights in sorted(episode_path.items())
    ]
    switches = _switch_dates(episode_path)
    flags = _episode_flags(context, episode_path)
    return {
        "episode_name": episode["episode_name"],
        "start_date": start.isoformat() if isinstance(start, date) else None,
        "end_date": end.isoformat() if isinstance(end, date) else None,
        "selector_id": selector_id,
        "selected_component_path": selected_path[:120],
        "switch_dates": switches,
        "return_during_episode": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "recovery_days": _recovery_days(context, episode_path),
        "selected_equal_risk_before_drawdown": flags["selected_equal_risk_before_drawdown"],
        "selected_100qqq_before_rally": flags["selected_100qqq_before_rally"],
        "missed_rebound_flag": flags["missed_rebound_flag"],
        "late_risk_off_flag": flags["late_risk_off_flag"],
        "late_risk_on_flag": flags["late_risk_on_flag"],
        "relative_vs_equal_risk": _round(metrics["relative_vs_equal_risk"]),
        "relative_vs_100_qqq": _round(metrics["relative_vs_100_qqq"]),
        "episode_commentary": (
            "covered" if episode_path else "actual requested date range does not cover episode"
        ),
    }


def _switch_dates(path: BlendPath) -> list[str]:
    dates = sorted(path)
    switches = []
    last = None
    for day in dates:
        current = _dominant_component(path[day])
        if last is not None and current != last:
            switches.append(day)
        last = current
    return switches


def _episode_flags(context: Mapping[str, Any], path: BlendPath) -> dict[str, bool]:
    if not path:
        return {
            "selected_equal_risk_before_drawdown": False,
            "selected_100qqq_before_rally": False,
            "missed_rebound_flag": False,
            "late_risk_off_flag": False,
            "late_risk_on_flag": False,
        }
    features = _feature_frame(context)
    first_day = sorted(path)[0]
    first_component = _dominant_component(path[first_day])
    false_counts = _false_signal_counts(context, path)
    rally_return = 0.0
    if first_day in features.index:
        rally_return = _float(features.loc[first_day, "qqq_return_1d"])
    return {
        "selected_equal_risk_before_drawdown": first_component == "equal_risk_qqq_sgov",
        "selected_100qqq_before_rally": first_component == "100_qqq" and rally_return > 0.0,
        "missed_rebound_flag": false_counts["false_defensive_periods"] > 0,
        "late_risk_off_flag": false_counts["false_risk_on_periods"] > 0,
        "late_risk_on_flag": false_counts["false_defensive_periods"] > 0,
    }


def _recovery_days(context: Mapping[str, Any], path: BlendPath) -> int | None:
    if not path:
        return None
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    values = []
    for index, day in enumerate(sorted(path)[:-1]):
        next_day = sorted(path)[index + 1]
        component = _dominant_component(path[day])
        values.append(_float(returns.loc[next_day].get(component)))
    if not values:
        return None
    equity = (1.0 + pd.Series(values, dtype=float)).cumprod()
    peak = equity.cummax()
    under_water = equity < peak
    if not under_water.any():
        return 0
    min_index = int((equity / peak - 1.0).idxmin())
    recovered = equity.iloc[min_index:] >= peak.iloc[min_index]
    if not recovered.any():
        return None
    return int(recovered.idxmax()) - min_index


def _regret_attribution_rows(context: Mapping[str, Any], path: BlendPath) -> list[dict[str, Any]]:
    panel = _ensure_frame(context["panel"])
    returns = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    features = _feature_frame(context)
    buckets: dict[str, list[float]] = defaultdict(list)
    dates = sorted(day for day in path if day in returns.index)
    for index, day in enumerate(dates[:-1]):
        next_day = dates[index + 1]
        selected = _dominant_component(path[day])
        equal_ret = _float(returns.loc[next_day].get("equal_risk_qqq_sgov"))
        qqq_ret = _float(returns.loc[next_day].get("100_qqq"))
        gap = abs(qqq_ret - equal_ret)
        if selected == "equal_risk_qqq_sgov" and qqq_ret > equal_ret:
            buckets["selected_equal_risk_but_100qqq_better"].append(gap)
            if day in features.index and _float(features.loc[day, "trend_signal_on"]):
                buckets["late_switch_to_risk_on"].append(gap)
            if day in features.index and _float(features.loc[day, "qqq_return_1d"]) > 0.0:
                buckets["over_defensive_in_rally"].append(gap)
        if selected == "100_qqq" and equal_ret > qqq_ret:
            buckets["selected_100qqq_but_equal_risk_better"].append(gap)
            if (
                day in features.index
                and _float(features.loc[day, "drawdown_from_252d_high"]) < -0.05
            ):
                buckets["late_switch_to_defensive"].append(gap)
            if day in features.index and _float(features.loc[day, "qqq_return_1d"]) < 0.0:
                buckets["over_risk_on_before_drawdown"].append(gap)
    switch_count = len(_switch_dates(path))
    if switch_count:
        buckets["high_turnover_chop"].append(switch_count / max(len(dates), 1))
    metrics = _evaluate_blend_path(context, path)
    if _float(metrics["cost_drag"]) > 0.0:
        buckets["cost_loss"].append(_float(metrics["cost_drag"]))
    buckets.setdefault("latency_loss", [])
    rows = []
    for regret_type in [
        "selected_equal_risk_but_100qqq_better",
        "selected_100qqq_but_equal_risk_better",
        "late_switch_to_defensive",
        "late_switch_to_risk_on",
        "over_defensive_in_rally",
        "over_risk_on_before_drawdown",
        "high_turnover_chop",
        "latency_loss",
        "cost_loss",
    ]:
        values = buckets.get(regret_type, [])
        rows.append(
            {
                "selector_id": "best_simple_rule_selector",
                "regret_type": regret_type,
                "count": len(values),
                "avg_regret": _round(sum(values) / len(values)) if values else 0.0,
                "median_regret": _round(float(pd.Series(values).median())) if values else 0.0,
                "worst_regret": _round(max(values)) if values else 0.0,
                "period_concentration": "review_required" if values else "none",
                "regime_concentration": "review_required" if values else "none",
                "mitigation_hint": _mitigation_hint(regret_type),
            }
        )
    return rows


def _mitigation_hint(regret_type: str) -> str:
    hints = {
        "high_turnover_chop": "increase minimum holding period or cooldown",
        "cost_loss": "stress higher cost and require lower turnover",
        "latency_loss": "stress t+2 execution lag before watchlist review",
    }
    return hints.get(regret_type, "owner review before changing simple-rule inputs")


def _ranking_rows(context: Mapping[str, Any], registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    static = _static_component_metrics(context)
    reference = _reference_component_metrics(context)
    for component, metrics in static.items():
        rows.append(_ranking_row(component, "static_component", metrics, "COMPONENT_BASELINE"))
    for component, metrics in reference.items():
        rows.append(_ranking_row(component, "reference_component", metrics, "REFERENCE_ONLY"))
    for selector_id in _ranking_selector_ids(registry):
        selector = _selector_by_id(registry, selector_id)
        metrics = _evaluate_blend_path(context, _selector_path(context, registry, selector_id))
        selector_type = (
            "soft_selector"
            if str(selector.get("selector_type")).startswith("soft")
            else "hard_selector"
        )
        dominance = _dominance_status(metrics, static)
        rows.append(_ranking_row(selector_id, selector_type, metrics, dominance))
    rows.sort(
        key=lambda row: (
            _float(row["net_return_after_cost"]),
            _float(row["calmar"]),
            -_float(row["turnover"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
        row["research_recommendation"] = _research_recommendation(row)
    return rows


def _ranking_row(
    candidate_id: str,
    candidate_type: str,
    metrics: Mapping[str, Any],
    dominance_status: str,
) -> dict[str, Any]:
    return {
        "rank": 0,
        "candidate_id": candidate_id,
        "candidate_type": candidate_type,
        "net_return_after_cost": _round(metrics["net_return_after_cost"]),
        "max_drawdown": _round(metrics["max_drawdown"]),
        "sharpe": _round(metrics["sharpe"]),
        "calmar": _round(metrics["calmar"]),
        "turnover": _round(metrics["turnover"]),
        "switch_count": metrics["switch_count"],
        "regret_vs_best_component": _round(metrics["regret_vs_best_component"]),
        "dominance_status": dominance_status,
        "research_recommendation": "manual_review",
    }


def _dominance_status(
    metrics: Mapping[str, Any],
    static: Mapping[str, Mapping[str, Any]],
) -> str:
    candidate_return = _float(metrics["net_return_after_cost"])
    candidate_drawdown = abs(_float(metrics["max_drawdown"]))
    for component_metrics in static.values():
        component_return = _float(component_metrics["net_return_after_cost"])
        component_drawdown = abs(_float(component_metrics["max_drawdown"]))
        if component_return >= candidate_return and component_drawdown <= candidate_drawdown:
            return "DOMINATED"
    return "NOT_DOMINATED"


def _research_recommendation(row: Mapping[str, Any]) -> str:
    if row["candidate_type"] == "reference_component":
        return "REFERENCE_ONLY_NOT_SELECTABLE"
    if row["dominance_status"] == "DOMINATED":
        return "DO_NOT_ADVANCE"
    if str(row["candidate_type"]).endswith("selector"):
        return "OWNER_REVIEW_FOR_RESEARCH_ONLY_FORWARD_AGING"
    return "BASELINE_COMPONENT"


def _sensitivity_rows(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    selector_id: str,
) -> list[dict[str, Any]]:
    base_path = _selector_path(context, registry, selector_id)
    base_metrics = _evaluate_blend_path(context, base_path)
    base_score = _selector_score(base_metrics)
    rows = []
    for perturbation_id, overrides in _selector_perturbations(selector_id):
        path = _selector_path(context, registry, selector_id, overrides=overrides)
        metrics = _evaluate_blend_path(context, path)
        score = _selector_score(metrics)
        degradation = max(base_score - score, 0.0)
        rows.append(
            {
                "selector_id": selector_id,
                "perturbation_id": perturbation_id,
                "base_score": _round(base_score),
                "perturbed_score_distribution": [_round(score)],
                "rank_stability": "STABLE" if degradation <= 0.05 else "UNSTABLE",
                "metric_degradation": _round(degradation),
                "fragile_parameter_list": list(overrides),
                "overfit_risk_score": _round(degradation),
            }
        )
    if not rows:
        rows.append(
            {
                "selector_id": selector_id,
                "perturbation_id": "no_applicable_parameter",
                "base_score": _round(base_score),
                "perturbed_score_distribution": [_round(base_score)],
                "rank_stability": "STABLE",
                "metric_degradation": 0.0,
                "fragile_parameter_list": [],
                "overfit_risk_score": 0.0,
            }
        )
    return rows


def _selector_score(metrics: Mapping[str, Any]) -> float:
    return (
        _float(metrics["net_return_after_cost"])
        + 0.25 * _float(metrics["calmar"])
        + 0.10 * _float(metrics["sharpe"])
        - 0.05 * _float(metrics["turnover"])
    )


def _selector_perturbations(selector_id: str) -> list[tuple[str, dict[str, Any]]]:
    perturbations: list[tuple[str, dict[str, Any]]] = []
    if "trend" in selector_id:
        perturbations.extend(
            [
                ("moving_average_window_minus_20", {"distance_threshold": -0.02}),
                ("moving_average_window_plus_20", {"distance_threshold": 0.02}),
            ]
        )
    if "vol" in selector_id:
        perturbations.extend(
            [
                ("vol_threshold_minus_10pct", {"high_percentile": 0.56}),
                ("vol_threshold_plus_10pct", {"high_percentile": 0.76}),
                ("volatility_expansion_ratio_low", {"expansion_threshold": 1.125}),
                ("volatility_expansion_ratio_high", {"expansion_threshold": 1.375}),
            ]
        )
    if "drawdown" in selector_id:
        perturbations.extend(
            [
                ("drawdown_threshold_minus_2_5pp", {"drawdown_threshold": -0.105}),
                ("drawdown_threshold_plus_2_5pp", {"drawdown_threshold": -0.055}),
                ("drawdown_threshold_minus_5pp", {"drawdown_threshold": -0.13}),
                ("drawdown_threshold_plus_5pp", {"drawdown_threshold": -0.03}),
            ]
        )
    perturbations.extend(
        [
            ("cooldown_days_0", {"cooldown_days": 0}),
            ("cooldown_days_10", {"cooldown_days": 10}),
            ("cooldown_days_20", {"cooldown_days": 20}),
        ]
    )
    return perturbations


def _recommended_holding_period(rows: list[Mapping[str, Any]], registry: Mapping[str, Any]) -> int:
    minimum = _int(_evaluation_policy(registry).get("minimum_acceptable_holding_period"), 20)
    qualified = [
        row
        for row in rows
        if _int(row["minimum_holding_period"]) >= minimum
        and _float(row["turnover"])
        <= _float(_evaluation_policy(registry).get("max_turnover_warning"), 2.0)
    ]
    if not qualified:
        return minimum
    best = max(
        qualified,
        key=lambda row: (
            _float(row["net_return_after_cost"]),
            _float(row["avg_holding_period"]),
            -_float(row["turnover"]),
        ),
    )
    return _int(best["minimum_holding_period"], minimum)


def _best_watchlist_candidate(rows: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    candidates = [
        row
        for row in rows
        if str(row.get("candidate_type")).endswith("selector")
        and row.get("dominance_status") != "DOMINATED"
    ]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: (
            _float(row["net_return_after_cost"]),
            _float(row["calmar"]),
            -_float(row["turnover"]),
        ),
    )


def _watchlist_checks(
    context: Mapping[str, Any],
    registry: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not candidate:
        return [
            _watchlist_check("candidate_exists", False, "no selector candidate survives ranking")
        ]
    policy = _evaluation_policy(registry)
    static = _static_component_metrics(context)
    best_static_return = max(_float(row["net_return_after_cost"]) for row in static.values())
    candidate_return = _float(candidate["net_return_after_cost"])
    candidate_drawdown = abs(_float(candidate["max_drawdown"]))
    best_static_drawdown = min(abs(_float(row["max_drawdown"])) for row in static.values())
    switch_limit = _float(policy.get("max_switches_per_year"), 12)
    actual_range = _actual_date_range(context)
    days = _date_span_days(actual_range)
    annual_switches = _float(candidate["switch_count"]) / max(days / 365.25, 1.0)
    return [
        _watchlist_check(
            "beats_at_least_one_static_component",
            candidate_return
            > min(_float(row["net_return_after_cost"]) for row in static.values()),
            "cost-after return beats at least one static selectable component",
        ),
        _watchlist_check(
            "not_directly_dominated",
            candidate.get("dominance_status") != "DOMINATED",
            "ranking dominance status is not dominated",
        ),
        _watchlist_check(
            "drawdown_not_materially_worse",
            candidate_drawdown
            <= best_static_drawdown
            + _float(policy.get("drawdown_worsening_tolerance"), 0.02),
            "max drawdown is not materially worse than the lower-drawdown static component",
        ),
        _watchlist_check(
            "sharpe_or_calmar_improves",
            _float(candidate["sharpe"]) > max(_float(row["sharpe"]) for row in static.values())
            or _float(candidate["calmar"]) > max(_float(row["calmar"]) for row in static.values()),
            "Sharpe or Calmar improves vs static components",
        ),
        _watchlist_check(
            "switch_count_controlled",
            annual_switches <= switch_limit,
            "annualized switch count stays within registry policy",
        ),
        _watchlist_check(
            "holding_period_ready",
            True,
            "minimum holding period review is required before owner admission",
            warning=True,
        ),
        _watchlist_check(
            "period_split_not_concentrated",
            candidate_return >= best_static_return
            or candidate.get("dominance_status") != "DOMINATED",
            "period split concentration requires owner review when edge is narrow",
            warning=candidate_return < best_static_return,
        ),
        _watchlist_check(
            "sensitivity_not_fragile",
            True,
            "sensitivity review is required before owner admission",
            warning=True,
        ),
        _watchlist_check(
            "manual_review_required",
            True,
            "manual_review_required remains true",
        ),
    ]


def _watchlist_check(
    check_id: str,
    passed: bool,
    message: str,
    *,
    warning: bool = False,
) -> dict[str, Any]:
    if passed:
        status = "WARN" if warning else "PASS"
    else:
        status = "FAIL"
    return {"check_id": check_id, "status": status, "message": message}


def _watchlist_reason(status: str, candidate: Mapping[str, Any]) -> str:
    if not candidate:
        return "no non-dominated simple-rule selector candidate"
    if status == "SELECTOR_FORWARD_WATCHLIST_READY":
        return "candidate passes research-only watchlist checks, still owner-review only"
    return "candidate needs owner review before any research-only forward-aging admission"


def _owner_recommendation_from_gate(gate: Mapping[str, Any]) -> str:
    status = str(gate.get("status"))
    if status == "SELECTOR_FORWARD_WATCHLIST_READY":
        return "ADD_SELECTOR_TO_FORWARD_AGING"
    if status == "NO_SELECTOR_WATCHLIST_CANDIDATE":
        return "NO_SELECTOR_EDGE"
    if status == "SELECTOR_WATCHLIST_BLOCKED":
        return "BLOCKED"
    return "KEEP_SELECTOR_RESEARCH_ONLY"


def _owner_decision_answers(
    gate: Mapping[str, Any],
    recommendation: str,
) -> list[dict[str, str]]:
    selector_id = str(_mapping(gate.get("summary")).get("selector_id") or "none")
    return [
        {"question": "是否存在简单规则 selector 比直接持有组件更好？", "answer": selector_id},
        {"question": "该优势是否成本后仍成立？", "answer": str(gate.get("status"))},
        {"question": "该优势是否跨 period / regime 稳定？", "answer": "OWNER_REVIEW_REQUIRED"},
        {"question": "该 selector 是否过拟合？", "answer": "SENSITIVITY_REVIEW_REQUIRED"},
        {"question": "最短持有期建议是多少？", "answer": "see holding period review"},
        {
            "question": "是否允许进入 research-only forward-aging watchlist？",
            "answer": recommendation,
        },
        {"question": "是否继续排除 QQQ-plus growth？", "answer": "YES"},
        {"question": "是否继续排除 tail-risk fallback？", "answer": "YES"},
        {"question": "是否继续阻塞 LEAPS / Wheel？", "answer": "YES"},
        {"question": "是否仍保持 paper_shadow=false / production=false？", "answer": "YES"},
    ]


def _date_span_days(actual_range: Mapping[str, str | None]) -> int:
    start = _date_or_none(actual_range.get("start"))
    end = _date_or_none(actual_range.get("end"))
    if start is None or end is None:
        return 0
    return max((end - start).days + 1, 1)


def _date_or_none(value: object) -> date | None:
    if value in {None, ""}:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _int(value: object, default: int = 0) -> int:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return default
    return number


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    return 0.0 if denom == 0.0 else _float(numerator) / denom
