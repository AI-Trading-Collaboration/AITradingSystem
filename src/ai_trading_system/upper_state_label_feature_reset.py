from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts import (
    CoverageInterval,
    DateRange,
    EffectiveCoverage,
    ResearchEvaluationContext,
)
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _data_quality_gate,
    _load_price_matrix,
)
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
    STATE_ORDER,
    _backtest_probe_predictions,
    _confidence_from_margin,
    _load_rates,
    _normalize_weights,
)
from ai_trading_system.legacy.research_context_adapter import (
    attach_research_context,
    resolve_legacy_research_context,
)
from ai_trading_system.research_window_extension import (
    DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    load_research_window_registry,
    slice_window_prices,
    window_metadata,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

ASSETS = ["QQQ", "SGOV", "TQQQ"]
WINDOW_IDS_FOR_RESET = [
    "exact_three_asset_validated",
    "exact_three_asset_primary_only_extension",
    "legacy_research_window_2022_12",
]
PRIMARY_WINDOW_ID = "exact_three_asset_validated"
LABEL_COLUMNS = [
    "do_not_de_risk_label",
    "stay_constructive_label",
    "add_risk_label",
    "high_confidence_risk_on_label",
]
MODEL_IDS = [
    "do_not_de_risk_model_v1",
    "stay_constructive_model_v1",
    "add_risk_model_v1",
    "high_confidence_risk_on_model_v1",
]
V3_FEATURE_COLUMNS = [
    "qqq_momentum_20d",
    "qqq_momentum_60d",
    "qqq_momentum_120d",
    "qqq_ma_slope_20_60",
    "qqq_ma_slope_60_120",
    "qqq_drawdown_126d",
    "realized_vol_20d",
    "realized_vol_decline_20d",
    "realized_vol_decline_60d",
    "downside_vol_20d",
    "downside_vol_decline_20d",
    "qqq_above_ma60_duration_60d",
    "qqq_drawdown_recovery_20d",
    "qqq_higher_high_proxy_20d",
    "qqq_higher_low_proxy_20d",
    "qqq_distance_from_60d_high",
    "days_since_60d_low",
    "recovery_from_60d_low",
    "qqq_trend_consistency_60d",
    "qqq_pullback_contained_above_ma60",
    "qqq_reclaim_recent_high_20d",
    "qqq_recovery_speed_20d",
    "qqq_vs_sgov_momentum_60d",
    "qqq_vs_tqqq_consistency_20d",
    "yield_curve_10y2y",
    "usd_trend_20d",
]

DEFAULT_ALTERNATING_PROTOCOL_PATH = (
    PROJECT_ROOT / "config" / "research" / "alternating_two_layer_research_protocol.yaml"
)
DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH = (
    PROJECT_ROOT / "config" / "research" / "upper_state_label_taxonomy_v2.yaml"
)
DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH = (
    PROJECT_ROOT / "config" / "research" / "action_value_score_policy_v2.yaml"
)
DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_threshold_policy_v2.yaml"
)
DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_composer_v2.yaml"
)
DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_second_layer_probe_registry_v2.yaml"
)

DEFAULT_UP_STATE_REPAIR_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "up_state_repair_result_review.md"
)
DEFAULT_UP_STATE_REPAIR_REVIEW_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "up_state_repair_result_review.yaml"
)
DEFAULT_ALTERNATING_PROTOCOL_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "alternating_two_layer_research_protocol.md"
)
DEFAULT_TAXONOMY_RESET_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "upper_state_label_taxonomy_reset.md"
)
DEFAULT_LABEL_REGEN_PLAN_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "window_aware_upper_state_label_regeneration_plan.md"
)
DEFAULT_ACTION_VALUE_V2_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "action_value_matrix_v2"
DEFAULT_ACTION_VALUE_V2_CSV_PATH = DEFAULT_ACTION_VALUE_V2_ROOT / "action_value_matrix_v2.csv"
DEFAULT_ACTION_VALUE_V2_SUMMARY_PATH = DEFAULT_ACTION_VALUE_V2_ROOT / "action_value_summary_v2.json"
DEFAULT_LABELS_V2_CSV_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "trend_labels" / "upper_state_labels_v2.csv"
)
DEFAULT_LABELS_V2_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "upper_state_label_v2_summary.yaml"
)
DEFAULT_LABEL_QUALITY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "upper_state_label_quality_review_v2.md"
)
DEFAULT_FEATURE_INVENTORY_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "up_state_feature_inventory_v2.yaml"
)
DEFAULT_FEATURE_INVENTORY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "up_state_feature_inventory_review.md"
)
DEFAULT_FEATURE_INVENTORY_DOC_V3_PATH = (
    PROJECT_ROOT / "docs" / "research" / "up_state_feature_inventory_review_v3.md"
)
DEFAULT_PIT_FEATURE_V3_CSV_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "pit_feature_matrix" / "pit_feature_matrix_v3.csv"
)
DEFAULT_PIT_FEATURE_V3_REPORT_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "pit_feature_matrix" / "pit_feature_matrix_v3_report.json"
)
DEFAULT_FEATURE_PIT_AUDIT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_feature_pit_audit_v3.md"
)
DEFAULT_FEATURE_PIT_AUDIT_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_feature_pit_audit_v3.yaml"
)
DEFAULT_MODEL_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models"
DEFAULT_MODEL_REVIEW_DOCS = {
    "do_not_de_risk_model_v1": PROJECT_ROOT
    / "docs"
    / "research"
    / "do_not_de_risk_model_review.md",
    "stay_constructive_model_v1": PROJECT_ROOT
    / "docs"
    / "research"
    / "stay_constructive_model_review.md",
    "add_risk_model_v1": PROJECT_ROOT / "docs" / "research" / "add_risk_model_review.md",
    "high_confidence_risk_on_model_v1": PROJECT_ROOT
    / "docs"
    / "research"
    / "high_confidence_risk_on_model_review.md",
}
DEFAULT_THRESHOLD_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_threshold_calibration_review_v2.md"
)
DEFAULT_COMPOSER_PREDICTIONS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "first_layer_composer_v2_predictions.csv"
)
DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_walk_forward_review_v3.md"
)
DEFAULT_WALK_FORWARD_MATRIX_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_walk_forward_matrix_v3.yaml"
)
DEFAULT_ACTUAL_PATH_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_frozen_probe_actual_path_review.md"
)
DEFAULT_ACTUAL_PATH_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_frozen_probe_actual_path_matrix.yaml"
)
DEFAULT_FAILURE_ATTRIBUTION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_failure_attribution.md"
)
DEFAULT_FAILURE_ATTRIBUTION_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_failure_attribution.yaml"
)
DEFAULT_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "upper_state_label_feature_reset_owner_review_pack.md"
)
DEFAULT_FIRST_LAYER_V2_FORWARD_WATCH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_forward_watch_plan.md"
)
DEFAULT_RISK_OFF_ONLY_FORWARD_WATCH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_off_only_forward_watch_plan.md"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "upper_state_label_feature_reset_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "upper_state_label_feature_reset_final_matrix.yaml"
)
DEFAULT_FIRST_LAYER_V2_SCOPE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_frozen_probe_scope.md"
)
DEFAULT_FIRST_LAYER_V2_CONTRACT_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_frozen_probe_contract.yaml"
)
DEFAULT_FIRST_LAYER_V2_COVERAGE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_effective_coverage_audit.md"
)
DEFAULT_FIRST_LAYER_V2_COVERAGE_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_effective_coverage_audit.yaml"
)
DEFAULT_FIRST_LAYER_V2_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_owner_review_pack.md"
)
DEFAULT_FIRST_LAYER_V2_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_label_feature_model_closeout.md"
)
DEFAULT_FIRST_LAYER_V2_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_label_feature_model_final_matrix.yaml"
)
DEFAULT_PRIOR_UP_STATE_FINAL_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_up_state_learning_final_matrix.yaml"
)
DEFAULT_PRIOR_ACTUAL_PATH_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "hierarchical_first_layer_actual_path_matrix.yaml"
)

MODEL_SPECS: dict[str, dict[str, Any]] = {
    "do_not_de_risk_model_v1": {
        "label_column": "do_not_de_risk_label",
        "score_column": "do_not_de_risk_score",
        "feature_weights": {
            "qqq_momentum_20d": 0.40,
            "qqq_momentum_60d": 0.30,
            "qqq_drawdown_recovery_20d": 0.30,
            "qqq_recovery_speed_20d": 0.25,
            "qqq_drawdown_126d": 0.35,
            "realized_vol_20d": -0.20,
            "downside_vol_20d": -0.30,
        },
    },
    "stay_constructive_model_v1": {
        "label_column": "stay_constructive_label",
        "score_column": "stay_constructive_score",
        "feature_weights": {
            "qqq_momentum_60d": 0.35,
            "qqq_momentum_120d": 0.30,
            "qqq_ma_slope_20_60": 0.25,
            "qqq_above_ma60_duration_60d": 0.20,
            "qqq_trend_consistency_60d": 0.30,
            "realized_vol_decline_20d": 0.15,
            "qqq_vs_sgov_momentum_60d": 0.20,
        },
    },
    "add_risk_model_v1": {
        "label_column": "add_risk_label",
        "score_column": "add_risk_score",
        "feature_weights": {
            "qqq_momentum_60d": 0.30,
            "qqq_momentum_120d": 0.35,
            "qqq_ma_slope_60_120": 0.25,
            "qqq_distance_from_60d_high": 0.20,
            "qqq_reclaim_recent_high_20d": 0.25,
            "realized_vol_decline_60d": 0.20,
            "downside_vol_decline_20d": 0.20,
        },
    },
    "high_confidence_risk_on_model_v1": {
        "label_column": "high_confidence_risk_on_label",
        "score_column": "high_confidence_risk_on_score",
        "feature_weights": {
            "qqq_momentum_20d": 0.25,
            "qqq_momentum_60d": 0.35,
            "qqq_momentum_120d": 0.30,
            "qqq_distance_from_60d_high": 0.25,
            "qqq_higher_high_proxy_20d": 0.20,
            "qqq_reclaim_recent_high_20d": 0.25,
            "realized_vol_decline_20d": 0.20,
            "qqq_vs_tqqq_consistency_20d": 0.10,
        },
    },
}


def run_upper_state_label_feature_reset_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    alternating_protocol_path: Path = DEFAULT_ALTERNATING_PROTOCOL_PATH,
    upper_state_taxonomy_path: Path = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Path = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Path = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Path = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    first_layer_v2_closeout: bool = False,
) -> dict[str, Any]:
    registry = load_research_window_registry(registry_path)
    protocol = _load_yaml_mapping(alternating_protocol_path)
    taxonomy = _load_yaml_mapping(upper_state_taxonomy_path)
    action_policy = _load_yaml_mapping(action_value_policy_path)
    threshold_policy = _load_yaml_mapping(threshold_policy_path)
    composer_config = _load_yaml_mapping(composer_config_path)
    probe_registry = _load_yaml_mapping(probe_registry_path)
    expanded_config = _load_yaml_mapping(expanded_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=None,
        expected_tickers=ASSETS,
    )
    if not data_gate["passed"]:
        raise RuntimeError(
            f"Cached data quality gate failed for upper-state reset: {data_gate['status']}"
        )

    prices = _load_price_matrix(prices_path, ASSETS)
    rates = _load_rates(rates_path)
    windows = [
        _mapping(registry["windows"].get(window_id))
        for window_id in WINDOW_IDS_FOR_RESET
        if window_id in registry["windows"]
    ]
    primary_window = _mapping(registry["windows"][PRIMARY_WINDOW_ID])
    primary_prices = slice_window_prices(prices, primary_window)
    primary_rates = rates.loc[rates.index >= primary_prices.index.min()].copy()

    frozen_probe_contract = (
        build_first_layer_v2_frozen_probe_contract(
            primary_window=primary_window,
            windows=windows,
            probe_registry=probe_registry,
            probe_registry_path=probe_registry_path,
        )
        if first_layer_v2_closeout
        else None
    )
    repair_review = build_up_state_repair_result_review(
        prior_final_path=DEFAULT_PRIOR_UP_STATE_FINAL_PATH,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    protocol_review = build_alternating_protocol_review(protocol)
    taxonomy_review = build_taxonomy_reset_review(taxonomy, primary_window)
    label_regeneration_plan = build_label_regeneration_plan(primary_window, windows, data_gate)
    action_value = build_upper_state_action_value_matrix_v2(
        windows=windows,
        prices=prices,
        probe_registry=probe_registry,
        score_policy=action_policy,
    )
    labels_v2 = build_upper_state_labels_v2(
        action_value=action_value,
        taxonomy=taxonomy,
        score_policy=action_policy,
    )
    action_summary = build_action_value_summary(
        action_value=action_value,
        labels=labels_v2,
        windows=windows,
        data_gate=data_gate,
    )
    label_summary = build_label_quality_summary(
        labels=labels_v2,
        taxonomy=taxonomy,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    feature_inventory = build_up_state_feature_inventory_v2(primary_window, data_gate)
    feature_matrix, feature_report = build_pit_feature_matrix_v3(
        prices=primary_prices,
        rates=primary_rates,
        window=primary_window,
        data_gate=data_gate,
    )
    feature_audit = build_feature_pit_audit_v3(feature_matrix, feature_report, primary_window)
    model_results = train_first_layer_submodels_v1(
        feature_matrix=feature_matrix,
        labels=labels_v2,
        threshold_policy=threshold_policy,
        primary_window=primary_window,
    )
    composer_predictions = build_first_layer_composer_v2_predictions(
        model_results=model_results,
        composer_config=composer_config,
        feature_matrix=feature_matrix,
        primary_window=primary_window,
    )
    actual_path = build_first_layer_v2_frozen_probe_actual_path_matrix(
        prices=primary_prices,
        predictions=composer_predictions,
        probe_registry=probe_registry,
        primary_window=primary_window,
        prior_actual_path_path=DEFAULT_PRIOR_ACTUAL_PATH_PATH,
    )
    effective_coverage = (
        build_first_layer_v2_effective_coverage_audit(
            primary_window=primary_window,
            labels=labels_v2,
            feature_matrix=feature_matrix,
            composer_predictions=composer_predictions,
            actual_path=actual_path,
            research_context=_build_first_layer_v2_effective_coverage_context(
                primary_window=primary_window,
                labels=labels_v2,
                feature_matrix=feature_matrix,
                composer_predictions=composer_predictions,
                actual_path=actual_path,
                data_gate=data_gate,
                registry_path=registry_path,
            ),
        )
        if first_layer_v2_closeout
        else None
    )
    walk_forward = build_first_layer_walk_forward_review_v3(
        model_results=model_results,
        composer_predictions=composer_predictions,
        labels=labels_v2,
        actual_path=actual_path,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    failure = build_first_layer_v2_failure_attribution(
        label_summary=label_summary,
        model_results=model_results,
        actual_path=actual_path,
        primary_window=primary_window,
        effective_coverage=effective_coverage,
    )
    threshold_review = build_threshold_review(threshold_policy, model_results, primary_window)
    owner_pack = build_owner_review_pack(
        repair_review=repair_review,
        protocol_review=protocol_review,
        taxonomy_review=taxonomy_review,
        label_summary=label_summary,
        feature_audit=feature_audit,
        walk_forward=walk_forward,
        actual_path=actual_path,
        failure=failure,
        primary_window=primary_window,
    )
    final_matrix = build_final_matrix(
        owner_pack=owner_pack,
        label_summary=label_summary,
        actual_path=actual_path,
        failure=failure,
        primary_window=primary_window,
    )
    first_layer_v2_owner_pack = (
        build_first_layer_v2_owner_review_pack(
            frozen_probe_contract=_mapping(frozen_probe_contract),
            effective_coverage=_mapping(effective_coverage),
            label_summary=label_summary,
            feature_audit=feature_audit,
            walk_forward=walk_forward,
            actual_path=actual_path,
            failure=failure,
            primary_window=primary_window,
        )
        if first_layer_v2_closeout
        else None
    )
    first_layer_v2_final_matrix = (
        build_first_layer_v2_label_feature_model_final_matrix(
            frozen_probe_contract=_mapping(frozen_probe_contract),
            effective_coverage=_mapping(effective_coverage),
            label_summary=label_summary,
            feature_audit=feature_audit,
            walk_forward=walk_forward,
            actual_path=actual_path,
            failure=failure,
            owner_pack=_mapping(first_layer_v2_owner_pack),
            primary_window=primary_window,
        )
        if first_layer_v2_closeout
        else None
    )
    write_upper_state_reset_outputs(
        output_root=output_root,
        frozen_probe_contract=frozen_probe_contract,
        effective_coverage=effective_coverage,
        repair_review=repair_review,
        protocol_review=protocol_review,
        taxonomy_review=taxonomy_review,
        label_regeneration_plan=label_regeneration_plan,
        action_value=action_value,
        action_summary=action_summary,
        labels_v2=labels_v2,
        label_summary=label_summary,
        feature_inventory=feature_inventory,
        feature_matrix=feature_matrix,
        feature_report=feature_report,
        feature_audit=feature_audit,
        model_results=model_results,
        threshold_review=threshold_review,
        composer_predictions=composer_predictions,
        walk_forward=walk_forward,
        actual_path=actual_path,
        failure=failure,
        owner_pack=owner_pack,
        final_matrix=final_matrix,
        first_layer_v2_owner_pack=first_layer_v2_owner_pack,
        first_layer_v2_final_matrix=first_layer_v2_final_matrix,
    )
    return _mapping(first_layer_v2_final_matrix) if first_layer_v2_closeout else owner_pack


def run_first_layer_v2_label_feature_model_reset_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    alternating_protocol_path: Path = DEFAULT_ALTERNATING_PROTOCOL_PATH,
    upper_state_taxonomy_path: Path = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Path = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Path = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Path = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Path = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> dict[str, Any]:
    return run_upper_state_label_feature_reset_pack(
        registry_path=registry_path,
        alternating_protocol_path=alternating_protocol_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        first_layer_v2_closeout=True,
    )


def validate_alternating_two_layer_protocol(protocol: Mapping[str, Any]) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    rounds = _mapping(protocol.get("round_types"))
    for round_id, cfg in rounds.items():
        modified = str(_mapping(cfg).get("modified_layer"))
        frozen = str(_mapping(cfg).get("frozen_layer"))
        allowed = set(_string_list(_mapping(cfg).get("allowed_changes")))
        forbidden = set(_string_list(_mapping(cfg).get("forbidden_changes")))
        if modified == "first_layer" and frozen != "second_layer":
            issues.append(_issue(f"{round_id}_does_not_freeze_second_layer"))
        if modified == "second_layer" and frozen != "first_layer":
            issues.append(_issue(f"{round_id}_does_not_freeze_first_layer"))
        if modified == "none" and frozen != "both":
            issues.append(_issue(f"{round_id}_validation_does_not_freeze_both_layers"))
        if allowed & forbidden:
            issues.append(_issue(f"{round_id}_allowed_and_forbidden_overlap"))
        if modified == "first_layer" and "second_layer_probe_weight_change" not in forbidden:
            issues.append(_issue(f"{round_id}_can_modify_second_layer_weights"))
        if modified == "second_layer" and "first_layer_model_change" not in forbidden:
            issues.append(_issue(f"{round_id}_can_modify_first_layer_model"))
    promotion_rules = _mapping(protocol.get("promotion_rules"))
    if bool(promotion_rules.get("target_path_metrics_can_pass_first_layer_gate")):
        issues.append(_issue("target_path_metrics_can_pass_first_layer_gate"))
    if str(promotion_rules.get("dynamic_promotion_status")) != "BLOCKED":
        issues.append(_issue("dynamic_promotion_not_blocked"))
    return {"status": "PASS" if not issues else "FAIL", "issues": issues, **SAFETY_BOUNDARY}


def validate_upper_state_label_taxonomy_v2(taxonomy: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "do_not_de_risk",
        "stay_constructive",
        "add_risk",
        "high_confidence_risk_on",
    }
    definitions = _mapping(taxonomy.get("label_definitions"))
    issues: list[dict[str, str]] = []
    missing = sorted(required - set(definitions))
    if missing:
        issues.append(_issue(f"missing_label_definitions:{','.join(missing)}"))
    risk_on = _mapping(definitions.get("high_confidence_risk_on"))
    if str(risk_on.get("label_role")) != "research_only_diagnostic":
        issues.append(_issue("high_confidence_risk_on_not_research_only_diagnostic"))
    stay = _mapping(_mapping(definitions.get("stay_constructive")).get("positive_condition"))
    add = _mapping(_mapping(definitions.get("add_risk")).get("positive_condition"))
    if stay == add:
        issues.append(_issue("add_risk_not_distinct_from_stay_constructive"))
    if bool(taxonomy.get("target_path_metrics_can_pass_gate")):
        issues.append(_issue("target_path_metrics_can_pass_gate"))
    return {"status": "PASS" if not issues else "FAIL", "issues": issues, **SAFETY_BOUNDARY}


def target_path_metrics_can_pass_first_layer_gate(
    protocol: Mapping[str, Any],
    taxonomy: Mapping[str, Any],
    threshold_policy: Mapping[str, Any] | None = None,
) -> bool:
    promotion_rules = _mapping(protocol.get("promotion_rules"))
    if bool(promotion_rules.get("target_path_metrics_can_pass_first_layer_gate")):
        return True
    if bool(taxonomy.get("target_path_metrics_can_pass_gate")):
        return True
    if threshold_policy is not None and bool(
        threshold_policy.get("target_path_metrics_can_pass_gate")
    ):
        return True
    return False


def upper_state_label_rows_have_window_metadata(labels: pd.DataFrame) -> bool:
    required = {
        "research_window_id",
        "requested_start",
        "actual_start",
        "actual_portfolio_start",
        "window_role",
        "data_quality_contract",
    }
    return required <= set(labels.columns) and not labels[list(required)].isna().any().any()


def first_layer_predictions_contain_weights(predictions: pd.DataFrame) -> bool:
    forbidden_tokens = {"QQQ", "SGOV", "TQQQ", "weight", "target_weight", "actual_weight"}
    return any(any(token in str(column) for token in forbidden_tokens) for column in predictions)


def build_first_layer_v2_frozen_probe_contract(
    *,
    primary_window: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
    probe_registry: Mapping[str, Any],
    probe_registry_path: Path,
) -> dict[str, Any]:
    probes = _records(probe_registry.get("probes"))
    probe_ids = [str(probe.get("probe_id")) for probe in probes]
    policy_id = str(probe_registry.get("policy_id"))
    registry_frozen = policy_id == "dynamic_second_layer_probe_registry_v2"
    blocked_weight_changes = all(
        "first_layer_action_value_approval" not in _string_list(probe.get("blocked_usage"))
        or str(probe.get("probe_id")) == "capped_risk_on_diagnostic_probe"
        for probe in probes
    )
    return _payload(
        report_type="first_layer_v2_frozen_probe_contract",
        title="First-Layer V2 Frozen Probe Contract",
        status=(
            "FIRST_LAYER_V2_FROZEN_PROBE_CONTRACT_READY_PROMOTION_BLOCKED"
            if registry_frozen
            else "FIRST_LAYER_V2_FROZEN_PROBE_CONTRACT_INVALID"
        ),
        summary={
            **window_metadata(primary_window),
            "modified_layer": "first_layer",
            "frozen_second_layer": policy_id,
            "probe_registry_path": str(probe_registry_path),
            "probe_count": len(probes),
            "allowed_probe_count": len(probe_ids),
            "registry_frozen": registry_frozen,
            "second_layer_weight_changes_allowed": False,
            "blocked_weight_changes": blocked_weight_changes,
            "primary_window": "2021-02-22",
            "legacy_window_role": "comparison_only",
            "sensitivity_window_role": "caveated_sensitivity",
            "target_path_metrics_used_for_pass": False,
        },
        probe_ids=probe_ids,
        window_rows=[window_metadata(window) for window in windows],
        forbidden_changes=[
            "second_layer_probe_weight_change",
            "new_second_layer_probe",
            "dynamic_promotion",
            "paper_shadow",
            "production",
            "broker_action",
        ],
    )


def build_first_layer_v2_effective_coverage_audit(
    *,
    primary_window: Mapping[str, Any],
    labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    composer_predictions: pd.DataFrame,
    actual_path: Mapping[str, Any],
    research_context: ResearchEvaluationContext,
) -> dict[str, Any]:
    requested_start = str(window_metadata(primary_window).get("requested_start"))
    label_start = _min_iso_date(
        labels.loc[labels["research_window_id"] == PRIMARY_WINDOW_ID]
        if "research_window_id" in labels
        else labels
    )
    feature_start = _min_iso_date(feature_matrix)
    prediction_start = _min_iso_date(composer_predictions)
    actual_rows = _records(actual_path.get("probe_rows"))
    portfolio_start = min(
        [str(row.get("date_start")) for row in actual_rows if row.get("date_start")],
        default=prediction_start,
    )
    prediction_cutoff = "2022-01-01"
    prediction_late = bool(prediction_start and prediction_start > prediction_cutoff)
    status = (
        "PRIMARY_WINDOW_COVERAGE_INCOMPLETE"
        if prediction_late
        else "FIRST_LAYER_V2_EFFECTIVE_COVERAGE_READY_PROMOTION_BLOCKED"
    )
    coverage_rows = [
        {
            "coverage_item": "label",
            "actual_start": label_start,
            "covers_2021": _covers_year(label_start, labels, 2021),
            "covers_2022": _covers_year(label_start, labels, 2022),
        },
        {
            "coverage_item": "feature",
            "actual_start": feature_start,
            "covers_2021": _covers_year(feature_start, feature_matrix, 2021),
            "covers_2022": _covers_year(feature_start, feature_matrix, 2022),
        },
        {
            "coverage_item": "prediction",
            "actual_start": prediction_start,
            "covers_2021": _covers_year(prediction_start, composer_predictions, 2021),
            "covers_2022": _covers_year(prediction_start, composer_predictions, 2022),
        },
        {
            "coverage_item": "portfolio",
            "actual_start": portfolio_start,
            "covers_2021": bool(portfolio_start and portfolio_start <= "2021-12-31"),
            "covers_2022": bool(portfolio_start and portfolio_start <= "2022-12-31"),
        },
    ]
    payload = _payload(
        report_type="first_layer_v2_effective_coverage_audit",
        title="First-Layer V2 Effective Coverage Audit",
        status=status,
        summary={
            **window_metadata(primary_window),
            "requested_research_window_start": requested_start,
            "actual_label_start": label_start,
            "actual_feature_start": feature_start,
            "actual_prediction_start": prediction_start,
            "actual_portfolio_start_effective": portfolio_start,
            "prediction_late_cutoff": prediction_cutoff,
            "primary_window_coverage_incomplete": prediction_late,
            "covers_2021_predictions": _covers_year(prediction_start, composer_predictions, 2021),
            "covers_2022_predictions": _covers_year(prediction_start, composer_predictions, 2022),
            "late_prediction_reason": (
                "walk_forward_train_window_and_label_horizon_delay"
                if prediction_late
                else "prediction_coverage_reaches_required_cutoff"
            ),
        },
        coverage_rows=coverage_rows,
    )
    return attach_research_context(payload, research_context)


def _build_first_layer_v2_effective_coverage_context(
    *,
    primary_window: Mapping[str, Any],
    labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    composer_predictions: pd.DataFrame,
    actual_path: Mapping[str, Any],
    data_gate: Mapping[str, Any],
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
) -> ResearchEvaluationContext:
    as_of = _required_context_date(data_gate.get("as_of"), "data_quality.as_of")
    requested_start = _required_context_date(
        primary_window.get("requested_start", primary_window.get("start")),
        "research_window.requested_start",
    )
    requested_range = DateRange(requested_start, as_of)
    source_ranges = {
        "labels:upper_state_v2": _frame_date_range(labels),
        "features:pit_matrix_v3": _frame_date_range(feature_matrix),
        "predictions:first_layer_composer_v2": _frame_date_range(composer_predictions),
        "portfolio:frozen_probe_actual_path": _actual_path_date_range(actual_path),
    }
    missing_sources = sorted(
        source_id for source_id, date_range in source_ranges.items() if date_range is None
    )
    coverage = EffectiveCoverage(
        tuple(
            CoverageInterval(source_id=source_id, date_range=date_range)
            for source_id, date_range in source_ranges.items()
            if date_range is not None
        )
    )
    available_ranges = [item.date_range for item in coverage.intervals]
    actual_data_range = (
        DateRange(
            min(item.start for item in available_ranges),
            max(item.end for item in available_ranges),
        )
        if available_ranges
        else None
    )
    feature_range = source_ranges["features:pit_matrix_v3"]
    prediction_range = source_ranges["predictions:first_layer_composer_v2"]
    portfolio_range = source_ranges["portfolio:frozen_probe_actual_path"]
    effective_ranges = [feature_range, prediction_range, portfolio_range]
    blocking_issues = [f"EFFECTIVE_COVERAGE_MISSING:{source_id}" for source_id in missing_sources]
    evaluation_range: DateRange | None = None
    if all(item is not None for item in effective_ranges):
        complete_ranges = [item for item in effective_ranges if item is not None]
        evaluation_start = max(item.start for item in complete_ranges)
        evaluation_end = min(item.end for item in complete_ranges)
        if evaluation_start <= evaluation_end:
            evaluation_range = DateRange(evaluation_start, evaluation_end)
        else:
            blocking_issues.append("NO_COMMON_EFFECTIVE_EVALUATION_RANGE")
    if not bool(data_gate.get("passed")):
        blocking_issues.append("DATA_QUALITY_FAILED")

    common: dict[str, Any] = {
        "market_regime_id": "ai_after_chatgpt",
        "research_window_id": str(primary_window.get("research_window_id")),
        "requested_range": requested_range,
        "as_of": as_of,
        "data_quality_status": str(data_gate.get("status", "UNKNOWN")),
        "data_quality_passed": bool(data_gate.get("passed")),
        "data_quality_contract_id": str(primary_window.get("data_quality_contract")),
        "actual_data_range": actual_data_range,
        "effective_feature_start": None if feature_range is None else feature_range.start,
        "effective_prediction_start": (
            None if prediction_range is None else prediction_range.start
        ),
        "actual_portfolio_start": None if portfolio_range is None else portfolio_range.start,
        "evaluation_range": evaluation_range,
        "effective_coverage": coverage if coverage.intervals else None,
        "declared_research_window_start": _required_context_date(
            primary_window.get("start"), "research_window.start"
        ),
        "research_window_registry_path": registry_path,
    }
    return resolve_legacy_research_context(
        **common,
        blocking_issues=tuple(blocking_issues),
    )


def build_up_state_repair_result_review(
    *,
    prior_final_path: Path,
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    prior = _load_yaml_mapping(prior_final_path) if prior_final_path.exists() else {}
    metrics = _mapping(prior.get("walk_forward_metrics"))
    actual = _mapping(prior.get("actual_path_summary"))
    metadata = window_metadata(primary_window)
    return _payload(
        report_type="up_state_repair_result_review",
        title="Up-State Repair Result Review",
        status="UP_STATE_REPAIR_FAILURE_REVIEW_READY_PROMOTION_BLOCKED",
        summary={
            **metadata,
            "data_quality_status": data_gate.get("status"),
            "prior_status": prior.get("status", "MISSING"),
            "prior_predicted_upper_state_count": metrics.get("predicted_upper_state_count"),
            "prior_upper_state_precision": metrics.get("upper_state_precision"),
            "prior_upper_state_recall": metrics.get("upper_state_recall"),
            "prior_improved_vs_flat_probe_count": actual.get("improved_vs_flat_probe_count"),
            "decision": "PAUSE_UPPER_STATE_DETECTOR_V1_AND_RESET_LABEL_FEATURE_PROBLEM",
        },
        prior_walk_forward_metrics=metrics,
        prior_actual_path_summary=actual,
    )


def build_alternating_protocol_review(protocol: Mapping[str, Any]) -> dict[str, Any]:
    validation = validate_alternating_two_layer_protocol(protocol)
    return _payload(
        report_type="alternating_two_layer_research_protocol",
        title="Alternating Two-Layer Research Protocol",
        status="ALTERNATING_TWO_LAYER_PROTOCOL_READY_PROMOTION_BLOCKED",
        summary={
            "validation_status": validation["status"],
            "round_type_count": len(_mapping(protocol.get("round_types"))),
            "dynamic_promotion_status": "BLOCKED",
            "target_path_metrics_can_pass_first_layer_gate": False,
        },
        validation=validation,
        round_types=_mapping(protocol.get("round_types")),
    )


def build_taxonomy_reset_review(
    taxonomy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    validation = validate_upper_state_label_taxonomy_v2(taxonomy)
    return _payload(
        report_type="upper_state_label_taxonomy_reset",
        title="Upper-State Label Taxonomy Reset",
        status="UPPER_STATE_LABEL_TAXONOMY_V2_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "validation_status": validation["status"],
            "label_count": len(_mapping(taxonomy.get("label_definitions"))),
            "risk_on_usage": "research_only_diagnostic",
        },
        validation=validation,
        label_definitions=_mapping(taxonomy.get("label_definitions")),
    )


def build_label_regeneration_plan(
    primary_window: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type="window_aware_upper_state_label_regeneration_plan",
        title="Window-Aware Upper-State Label Regeneration Plan",
        status="WINDOW_AWARE_LABEL_REGENERATION_PLAN_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate.get("status"),
            "window_count": len(windows),
            "frozen_second_layer_probe_required": True,
            "target_path_metrics_can_pass_gate": False,
        },
        window_rows=[window_metadata(window) for window in windows],
        sequencing=[
            "freeze_second_layer_probes",
            "regenerate_action_value_labels",
            "train_low_complexity_first_layer_submodels",
            "compose_first_layer_predictions_without_weights",
            "run_frozen_probe_actual_path_validation",
        ],
    )


def build_upper_state_action_value_matrix_v2(
    *,
    windows: Sequence[Mapping[str, Any]],
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    score_policy: Mapping[str, Any],
) -> pd.DataFrame:
    horizons = [_int(value) for value in _list(score_policy.get("horizons"))] or [20]
    probes = _records(probe_registry.get("probes"))
    rows: list[dict[str, Any]] = []
    for window in windows:
        metadata = window_metadata(window)
        window_prices = slice_window_prices(prices, window)
        if window_prices.empty:
            continue
        returns = window_prices.pct_change().fillna(0.0)
        row_count = max(0, len(window_prices.index) - max(horizons) - 1)
        metric_cache: dict[tuple[tuple[float, float, float], int], list[dict[str, float]]] = {}
        for probe in probes:
            probe_id = str(probe.get("probe_id"))
            weights_by_state = _mapping(probe.get("weights_by_trend_state"))
            normalized = {
                state: _normalize_weights(_mapping(weights_by_state.get(state)))
                for state in STATE_ORDER
            }
            for horizon in horizons:
                state_metrics = {
                    state: _cached_future_metrics(
                        returns=returns,
                        weights=weights,
                        horizon=horizon,
                        row_count=row_count,
                        cache=metric_cache,
                    )
                    for state, weights in normalized.items()
                }
                for idx in range(row_count):
                    state_rows = {
                        state: {
                            **state_metrics[state][idx],
                            "weights": normalized[state],
                        }
                        for state in STATE_ORDER
                    }
                    rows.append(
                        {
                            **metadata,
                            "date": window_prices.index[idx].date().isoformat(),
                            "probe_id": probe_id,
                            "probe_role": str(probe.get("role")),
                            "return_seeking_probe": bool(probe.get("return_seeking")),
                            "horizon_days": horizon,
                            **_action_scores_from_state_rows(state_rows, score_policy),
                            "label_uses_future_outcome": True,
                            "feature_cutoff_used": False,
                            "second_layer_frozen": bool(probe.get("frozen")),
                            **SAFETY_BOUNDARY,
                        }
                    )
    return pd.DataFrame(rows)


def build_upper_state_labels_v2(
    *,
    action_value: pd.DataFrame,
    taxonomy: Mapping[str, Any],
    score_policy: Mapping[str, Any],
) -> pd.DataFrame:
    if action_value.empty:
        return pd.DataFrame()
    thresholds = _mapping(score_policy.get("thresholds"))
    quality = _mapping(taxonomy.get("label_quality"))
    confidence_floor = _float(quality.get("high_confidence_floor"), default=0.55)
    max_disagreement = _float(quality.get("max_disagreement_score"), default=0.50)
    rows: list[dict[str, Any]] = []
    group_cols = [
        "research_window_id",
        "requested_start",
        "actual_start",
        "actual_portfolio_start",
        "end",
        "window_role",
        "data_quality_contract",
        "exact_or_proxy",
        "date",
        "horizon_days",
    ]
    for keys, frame in action_value.groupby(group_cols, sort=True):
        base = dict(zip(group_cols, keys, strict=False))
        score_means = {
            "do_not_de_risk": float(frame["do_not_de_risk_score"].mean()),
            "stay_constructive": float(frame["stay_constructive_score"].mean()),
            "add_risk": float(frame["add_risk_score"].mean()),
            "high_confidence_risk_on": float(frame["risk_on_diagnostic_score"].mean()),
        }
        label_payload: dict[str, Any] = {}
        confidence_values = []
        disagreement_values = []
        for label_id, score in score_means.items():
            score_column = (
                "risk_on_diagnostic_score"
                if label_id == "high_confidence_risk_on"
                else f"{label_id}_score"
            )
            threshold_key = (
                "risk_on_diagnostic_score"
                if label_id == "high_confidence_risk_on"
                else f"{label_id}_score"
            )
            threshold = _float(thresholds.get(threshold_key))
            probe_positive = frame[score_column].astype(float) >= threshold
            vote_share = float(probe_positive.mean()) if len(probe_positive) else 0.0
            disagreement = round(2.0 * min(vote_share, 1.0 - vote_share), GRID_ROUND_DIGITS)
            confidence = round(
                max(vote_share, _confidence_from_margin(score - threshold, score)),
                GRID_ROUND_DIGITS,
            )
            is_positive = (
                score >= threshold
                and confidence >= confidence_floor
                and disagreement <= max_disagreement
            )
            label_payload[f"{label_id}_score"] = round(score, GRID_ROUND_DIGITS)
            label_payload[f"{label_id}_label"] = bool(is_positive)
            label_payload[f"{label_id}_confidence"] = confidence
            label_payload[f"{label_id}_probe_vote_share"] = round(vote_share, GRID_ROUND_DIGITS)
            confidence_values.append(confidence)
            disagreement_values.append(disagreement)
        label_payload["add_risk_distinct_from_stay_constructive"] = bool(
            label_payload["add_risk_label"] != label_payload["stay_constructive_label"]
            or abs(
                _float(label_payload["add_risk_score"])
                - _float(label_payload["stay_constructive_score"])
            )
            > 1e-9
        )
        label_payload["high_confidence_risk_on_usage"] = "research_only_diagnostic"
        label_payload["label_confidence"] = round(float(np.mean(confidence_values)), 6)
        label_payload["label_disagreement_score"] = round(float(np.mean(disagreement_values)), 6)
        label_payload["high_confidence_label"] = bool(
            label_payload["label_confidence"] >= confidence_floor
            and label_payload["label_disagreement_score"] <= max_disagreement
        )
        label_payload["train_usable"] = True
        label_payload["target_path_metrics_used_for_pass"] = False
        label_payload["allowed_training_usage"] = [
            "first_layer_research_only_label_quality_reported"
        ]
        rows.append({**base, **label_payload, **SAFETY_BOUNDARY})
    labels = pd.DataFrame(rows)
    labels = _apply_risk_on_sample_floor(labels, taxonomy)
    return labels


def build_action_value_summary(
    *,
    action_value: pd.DataFrame,
    labels: pd.DataFrame,
    windows: Sequence[Mapping[str, Any]],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    summary_rows = []
    for window in windows:
        window_id = str(window.get("research_window_id"))
        frame = (
            labels.loc[labels["research_window_id"] == window_id] if not labels.empty else labels
        )
        summary_rows.append(
            {
                **window_metadata(window),
                "action_value_row_count": (
                    int(len(action_value.loc[action_value["research_window_id"] == window_id]))
                    if not action_value.empty
                    else 0
                ),
                "label_row_count": int(len(frame)) if not frame.empty else 0,
                "do_not_de_risk_positive_count": _label_sum(frame, "do_not_de_risk_label"),
                "stay_constructive_positive_count": _label_sum(frame, "stay_constructive_label"),
                "add_risk_positive_count": _label_sum(frame, "add_risk_label"),
                "high_confidence_risk_on_positive_count": _label_sum(
                    frame, "high_confidence_risk_on_label"
                ),
            }
        )
    return _payload(
        report_type="action_value_summary_v2",
        title="Action-Value Summary V2",
        status="ACTION_VALUE_MATRIX_V2_READY_PROMOTION_BLOCKED",
        summary={
            **(window_metadata(windows[0]) if windows else {}),
            "data_quality_status": data_gate.get("status"),
            "action_value_row_count": len(action_value),
            "label_row_count": len(labels),
            "window_count": len(summary_rows),
            "target_path_metrics_used_for_pass": False,
        },
        window_rows=summary_rows,
    )


def build_label_quality_summary(
    *,
    labels: pd.DataFrame,
    taxonomy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    primary = labels.loc[
        (labels["research_window_id"] == PRIMARY_WINDOW_ID)
        & (labels["horizon_days"].astype(int) == 20)
    ].copy()
    sample_floor = _mapping(_mapping(taxonomy.get("label_quality")).get("minimum_positive_samples"))
    rows = []
    for label in LABEL_COLUMNS:
        label_id = label.removesuffix("_label")
        positive_count = _label_sum(primary, label)
        floor = _int(sample_floor.get(label_id), default=0)
        rows.append(
            {
                "label_id": label_id,
                "positive_count": positive_count,
                "positive_share": round(positive_count / max(len(primary), 1), 6),
                "minimum_positive_samples": floor,
                "sample_status": "PASS" if positive_count >= floor else "SAMPLE_INSUFFICIENT",
                "average_confidence": round(
                    _float(primary.get(f"{label_id}_confidence", pd.Series(dtype=float)).mean()),
                    6,
                ),
                "research_only": True,
            }
        )
    high_conf = next(row for row in rows if row["label_id"] == "high_confidence_risk_on")
    status = "UPPER_STATE_LABELS_V2_READY_PROMOTION_BLOCKED"
    if high_conf["sample_status"] == "SAMPLE_INSUFFICIENT":
        status = "UPPER_STATE_LABELS_V2_READY_RISK_ON_SAMPLE_INSUFFICIENT_PROMOTION_BLOCKED"
    return _payload(
        report_type="upper_state_label_v2_summary",
        title="Upper-State Label V2 Summary",
        status=status,
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate.get("status"),
            "label_row_count": len(primary),
            "high_confidence_label_share": round(
                float(primary["label_confidence"].ge(0.55).mean()) if not primary.empty else 0.0,
                6,
            ),
            "average_disagreement": round(
                _float(primary["label_disagreement_score"].mean()) if not primary.empty else 0.0,
                6,
            ),
            "risk_on_sample_status": high_conf["sample_status"],
            "target_path_metrics_used_for_pass": False,
        },
        label_rows=rows,
    )


def build_up_state_feature_inventory_v2(
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    approved_rows = [
        {
            "feature_id": feature,
            "feature_family": _feature_family(feature),
            "PIT_status": "PIT_APPROVED",
            "training_allowed": True,
            "known_at_policy": "daily_close_or_rate_cache_at_or_before_decision",
            "window_required": PRIMARY_WINDOW_ID,
        }
        for feature in V3_FEATURE_COLUMNS
    ]
    blocked_rows = [
        {
            "feature_id": "SMH_or_SOXX_relative_strength",
            "PIT_status": "PIT_BLOCKED",
            "reason": "validated_cache_not_available_for_this_batch",
            "exit_condition": "add explicit provider, schema validation and data-quality gate",
        },
        {
            "feature_id": "VIX_term_structure",
            "PIT_status": "PIT_BLOCKED",
            "reason": "cache not part of required QQQ_SGOV_TQQQ data gate",
            "exit_condition": "register source and freshness validation",
        },
        {
            "feature_id": "AI_news_or_event_score",
            "PIT_status": "PIT_BLOCKED",
            "reason": "manual/event feed not validated as point-in-time",
            "exit_condition": "create ex-ante event taxonomy and runtime provenance",
        },
    ]
    return _payload(
        report_type="up_state_feature_inventory_v2",
        title="Up-State Feature Inventory V2",
        status="UP_STATE_FEATURE_INVENTORY_V2_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate.get("status"),
            "approved_feature_count": len(approved_rows),
            "blocked_feature_count": len(blocked_rows),
        },
        approved_features=approved_rows,
        blocked_features=blocked_rows,
    )


def build_pit_feature_matrix_v3(
    *,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    feature_frame = _compute_feature_frame(prices, rates)
    metadata = window_metadata(window)
    rows = []
    for timestamp, row in feature_frame.iterrows():
        decision_at = timestamp + pd.offsets.BDay(1)
        payload = {
            **metadata,
            "date": timestamp.date().isoformat(),
            "known_at": timestamp.date().isoformat(),
            "available_at": timestamp.date().isoformat(),
            "decision_at": decision_at.date().isoformat(),
            "feature_cutoff_passed": True,
            "pit_status": "PIT_APPROVED",
        }
        payload.update({column: round(_float(row.get(column)), 8) for column in V3_FEATURE_COLUMNS})
        rows.append(payload)
    matrix = pd.DataFrame(rows)
    report = _payload(
        report_type="pit_feature_matrix_v3_report",
        title="PIT Feature Matrix V3 Report",
        status="PIT_FEATURE_MATRIX_V3_READY_PROMOTION_BLOCKED",
        summary={
            **metadata,
            "data_quality_status": data_gate.get("status"),
            "row_count": len(matrix),
            "approved_feature_count": len(V3_FEATURE_COLUMNS),
            "blocked_feature_count": 0,
            "feature_cutoff_passed": (
                bool(matrix["feature_cutoff_passed"].all()) if not matrix.empty else False
            ),
        },
        features=[
            {
                "feature_id": column,
                "PIT_status": "PIT_APPROVED",
                "training_allowed": True,
            }
            for column in V3_FEATURE_COLUMNS
        ],
        excluded_non_pit_or_unavailable_features=[
            "SMH/SOXX relative trend unavailable in validated cache",
            "VIX unavailable in required cache",
            "AI news/event score unavailable as PIT feature",
        ],
    )
    return matrix, report


def build_feature_pit_audit_v3(
    feature_matrix: pd.DataFrame,
    feature_report: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    leakage_columns = [column for column in feature_matrix if "future" in str(column).lower()]
    return _payload(
        report_type="first_layer_feature_pit_audit_v3",
        title="First-Layer Feature PIT Audit V3",
        status=(
            "FIRST_LAYER_FEATURE_PIT_AUDIT_V3_PASS_PROMOTION_BLOCKED"
            if not leakage_columns
            else "FIRST_LAYER_FEATURE_PIT_AUDIT_V3_FAIL_PROMOTION_BLOCKED"
        ),
        summary={
            **window_metadata(primary_window),
            "row_count": len(feature_matrix),
            "approved_feature_count": _mapping(feature_report.get("summary")).get(
                "approved_feature_count"
            ),
            "future_leakage_column_count": len(leakage_columns),
            "feature_cutoff_passed": (
                bool(feature_matrix["feature_cutoff_passed"].all())
                if not feature_matrix.empty
                else False
            ),
        },
        leakage_columns=leakage_columns,
        feature_report_summary=_mapping(feature_report.get("summary")),
    )


def train_first_layer_submodels_v1(
    *,
    feature_matrix: pd.DataFrame,
    labels: pd.DataFrame,
    threshold_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    primary_labels = labels.loc[
        (labels["research_window_id"] == PRIMARY_WINDOW_ID)
        & (labels["horizon_days"].astype(int) == _label_horizon(threshold_policy))
    ].copy()
    merged = feature_matrix.merge(primary_labels, on="date", how="inner", suffixes=("", "_label"))
    merged = merged.sort_values("date").reset_index(drop=True)
    results: dict[str, dict[str, Any]] = {}
    for model_id, spec in MODEL_SPECS.items():
        result = _train_single_submodel(
            model_id=model_id,
            spec=spec,
            merged=merged,
            threshold_policy=threshold_policy,
            primary_window=primary_window,
        )
        results[model_id] = result
    return results


def build_first_layer_composer_v2_predictions(
    *,
    model_results: Mapping[str, Mapping[str, Any]],
    composer_config: Mapping[str, Any],
    feature_matrix: pd.DataFrame,
    primary_window: Mapping[str, Any],
) -> pd.DataFrame:
    frames = []
    for model_id, result in model_results.items():
        predictions = _ensure_frame(result.get("predictions"))
        if predictions.empty:
            continue
        label_prefix = str(MODEL_SPECS[model_id]["label_column"]).removesuffix("_label")
        keep = predictions[
            [
                "date",
                "prediction",
                "score",
                "confidence",
                "label",
                "split_id",
                "threshold",
            ]
        ].copy()
        keep = keep.rename(
            columns={
                "prediction": f"{label_prefix}_pred",
                "score": f"{label_prefix}_model_score",
                "confidence": f"{label_prefix}_model_confidence",
                "label": f"{label_prefix}_true_label",
                "threshold": f"{label_prefix}_threshold",
            }
        )
        frames.append(keep)
    if not frames:
        return pd.DataFrame()
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.merge(frame, on=["date", "split_id"], how="outer")
    feature_meta = feature_matrix[
        ["date", "known_at", "available_at", "decision_at", *V3_FEATURE_COLUMNS]
    ].copy()
    combined = combined.merge(feature_meta, on="date", how="left")
    metadata = window_metadata(primary_window)
    rows = []
    for _, row in combined.sort_values("date").iterrows():
        state = _compose_state(row)
        rule = _mapping(_mapping(composer_config.get("rules")).get(state))
        confidence = _compose_confidence(row, state)
        payload = {
            **metadata,
            "date": str(row["date"]),
            "model_id": "first_layer_composer_v2",
            "trend_state": state,
            "confidence": confidence,
            "expected_horizon_days": 20,
            "validity_days": _int(rule.get("validity_days"), default=10),
            "decay_profile": str(rule.get("decay_profile", "medium")),
            "feature_snapshot_hash": _row_hash(row, V3_FEATURE_COLUMNS),
            "model_version": "first_layer_composer_v2",
            "known_at": str(row.get("known_at")),
            "available_at": str(row.get("available_at")),
            "decision_at": str(row.get("decision_at")),
            "do_not_de_risk_pred": bool(row.get("do_not_de_risk_pred", False)),
            "stay_constructive_pred": bool(row.get("stay_constructive_pred", False)),
            "add_risk_pred": bool(row.get("add_risk_pred", False)),
            "high_confidence_risk_on_pred": bool(row.get("high_confidence_risk_on_pred", False)),
            "target_path_metrics_used_for_pass": False,
            **SAFETY_BOUNDARY,
        }
        rows.append(payload)
    predictions = pd.DataFrame(rows)
    if first_layer_predictions_contain_weights(predictions):
        raise ValueError("First-layer composer predictions contain forbidden weight columns.")
    return predictions


def build_first_layer_v2_frozen_probe_actual_path_matrix(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    prior_actual_path_path: Path,
) -> dict[str, Any]:
    rows = []
    prior = _load_yaml_mapping(prior_actual_path_path) if prior_actual_path_path.exists() else {}
    prior_rows = {str(row.get("probe_id")): row for row in _records(prior.get("probe_rows"))}
    for probe in _records(probe_registry.get("probes")):
        raw = _backtest_probe_predictions(
            prices=prices,
            predictions=predictions,
            probe=probe,
            model_id="first_layer_composer_v2",
        )
        prior_row = prior_rows.get(str(probe.get("probe_id")), {})
        rows.append(
            {
                **window_metadata(primary_window),
                "probe_id": str(probe.get("probe_id")),
                "model_id": "first_layer_composer_v2",
                "date_start": raw["date_start"],
                "date_end": raw["date_end"],
                "v2_annual_return": raw["actual_path_annual_return"],
                "v2_max_drawdown": raw["max_drawdown_daily_equity"],
                "v2_sharpe": raw["sharpe_daily_zero_rf"],
                "v2_calmar": raw["calmar_daily_equity_dd"],
                "v2_turnover": raw["turnover"],
                "flat_annual_return_reference": prior_row.get("flat_annual_return"),
                "hierarchical_annual_return_reference": prior_row.get("hierarchical_annual_return"),
                "flat_calmar_reference": prior_row.get("flat_calmar"),
                "hierarchical_calmar_reference": prior_row.get("hierarchical_calmar"),
                "v2_vs_flat_return_delta_reference": round(
                    _float(raw["actual_path_annual_return"])
                    - _float(prior_row.get("flat_annual_return")),
                    GRID_ROUND_DIGITS,
                ),
                "v2_vs_hierarchical_return_delta_reference": round(
                    _float(raw["actual_path_annual_return"])
                    - _float(prior_row.get("hierarchical_annual_return")),
                    GRID_ROUND_DIGITS,
                ),
                "v2_vs_flat_calmar_delta_reference": round(
                    _float(raw["calmar_daily_equity_dd"]) - _float(prior_row.get("flat_calmar")),
                    GRID_ROUND_DIGITS,
                ),
                "actual_path_improved_vs_flat_reference": bool(
                    _float(raw["actual_path_annual_return"])
                    > _float(prior_row.get("flat_annual_return"))
                    and _float(raw["calmar_daily_equity_dd"]) > _float(prior_row.get("flat_calmar"))
                ),
                "comparison_window_compatibility": "PRIOR_ROWS_LEGACY_REFERENCE_ONLY",
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    improved_count = sum(row["actual_path_improved_vs_flat_reference"] for row in rows)
    return _payload(
        report_type="first_layer_v2_frozen_probe_actual_path_matrix",
        title="First-Layer V2 Frozen-Probe Actual-Path Matrix",
        status="FIRST_LAYER_V2_FROZEN_PROBE_ACTUAL_PATH_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "probe_count": len(rows),
            "improved_vs_flat_reference_count": improved_count,
            "comparison_window_compatibility": "PRIOR_ROWS_LEGACY_REFERENCE_ONLY",
            "target_path_metrics_used_for_pass": False,
        },
        probe_rows=rows,
    )


def build_first_layer_walk_forward_review_v3(
    *,
    model_results: Mapping[str, Mapping[str, Any]],
    composer_predictions: pd.DataFrame,
    labels: pd.DataFrame,
    actual_path: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    model_rows = [_mapping(result.get("metrics")) for result in model_results.values()]
    distribution = (
        _state_counts(composer_predictions["trend_state"]) if not composer_predictions.empty else {}
    )
    primary_labels = labels.loc[
        (labels["research_window_id"] == PRIMARY_WINDOW_ID)
        & (labels["horizon_days"].astype(int) == 20)
    ]
    return _payload(
        report_type="first_layer_walk_forward_matrix_v3",
        title="First-Layer Walk-Forward Review V3",
        status="FIRST_LAYER_WALK_FORWARD_V3_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate.get("status"),
            "submodel_count": len(model_rows),
            "composer_prediction_count": len(composer_predictions),
            "composer_distribution": distribution,
            "primary_label_rows": len(primary_labels),
            "actual_path_improved_vs_flat_reference_count": _mapping(
                actual_path.get("summary")
            ).get("improved_vs_flat_reference_count"),
            "target_path_metrics_used_for_pass": False,
        },
        model_rows=model_rows,
        composer_distribution=distribution,
    )


def build_threshold_review(
    threshold_policy: Mapping[str, Any],
    model_results: Mapping[str, Mapping[str, Any]],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type="first_layer_threshold_calibration_review_v2",
        title="First-Layer Threshold Calibration Review V2",
        status="FIRST_LAYER_THRESHOLD_POLICY_V2_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "model_count": len(model_results),
            "threshold_selection": _mapping(threshold_policy.get("threshold_selection")),
            "target_path_metrics_can_pass_gate": False,
        },
        model_thresholds=[
            {
                "model_id": model_id,
                "status": _mapping(result.get("metrics")).get("status"),
                "threshold_rows": _records(result.get("threshold_rows")),
            }
            for model_id, result in model_results.items()
        ],
    )


def build_first_layer_v2_failure_attribution(
    *,
    label_summary: Mapping[str, Any],
    model_results: Mapping[str, Mapping[str, Any]],
    actual_path: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    effective_coverage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    label_rows = _records(label_summary.get("label_rows"))
    risk_on = next(
        (row for row in label_rows if row.get("label_id") == "high_confidence_risk_on"),
        {},
    )
    model_metrics = [_mapping(result.get("metrics")) for result in model_results.values()]
    weak_precision_count = sum(_float(row.get("precision")) < 0.35 for row in model_metrics)
    improved = _int(_mapping(actual_path.get("summary")).get("improved_vs_flat_reference_count"))
    coverage_incomplete = bool(
        _mapping(_mapping(effective_coverage).get("summary")).get(
            "primary_window_coverage_incomplete"
        )
    )
    reasons = []
    if coverage_incomplete:
        reasons.append("WINDOW_COVERAGE_INCOMPLETE")
    if risk_on.get("sample_status") == "SAMPLE_INSUFFICIENT":
        reasons.append("RISK_ON_SAMPLE_INSUFFICIENT")
    if weak_precision_count:
        reasons.append("SUBMODEL_PRECISION_WEAK")
    if improved <= 0:
        reasons.append("FROZEN_PROBE_ACTUAL_PATH_NO_MATERIAL_IMPROVEMENT")
    if not reasons:
        reasons.append("OWNER_REVIEW_REQUIRED_BEFORE_ANY_ESCALATION")
    status = "FIRST_LAYER_V2_FAILURE_ATTRIBUTION_READY_PROMOTION_BLOCKED"
    return _payload(
        report_type="first_layer_v2_failure_attribution",
        title="First-Layer V2 Failure Attribution",
        status=status,
        summary={
            **window_metadata(primary_window),
            "failure_reason_count": len(reasons),
            "primary_failure_reason": reasons[0],
            "actual_path_improved_vs_flat_reference_count": improved,
            "next_action": _failure_next_action(
                coverage_incomplete=coverage_incomplete,
                improved_count=improved,
            ),
        },
        failure_reasons=reasons,
        model_metric_rows=model_metrics,
        label_quality_summary=_mapping(label_summary.get("summary")),
        actual_path_summary=_mapping(actual_path.get("summary")),
        effective_coverage_summary=_mapping(_mapping(effective_coverage).get("summary")),
    )


def build_owner_review_pack(
    *,
    repair_review: Mapping[str, Any],
    protocol_review: Mapping[str, Any],
    taxonomy_review: Mapping[str, Any],
    label_summary: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    failure: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    improved = _int(_mapping(actual_path.get("summary")).get("improved_vs_flat_reference_count"))
    recommendation = (
        "REVIEW_FIRST_LAYER_V2_FORWARD_WATCH"
        if improved > 0
        else "KEEP_RISK_OFF_ONLY_FORWARD_WATCH_AND_RESEARCH_ONLY_UPPER_STATE"
    )
    return _payload(
        report_type="upper_state_label_feature_reset_owner_review_pack",
        title="Upper-State Label Feature Reset Owner Review Pack",
        status="UPPER_STATE_LABEL_FEATURE_RESET_OWNER_REVIEW_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "repair_review_status": repair_review.get("status"),
            "protocol_status": protocol_review.get("status"),
            "taxonomy_status": taxonomy_review.get("status"),
            "label_status": label_summary.get("status"),
            "feature_audit_status": feature_audit.get("status"),
            "walk_forward_status": walk_forward.get("status"),
            "actual_path_status": actual_path.get("status"),
            "failure_primary_reason": _mapping(failure.get("summary")).get(
                "primary_failure_reason"
            ),
            "owner_recommendation": recommendation,
        },
        owner_answers={
            "should_continue_upper_state_v2": improved > 0,
            "is_high_confidence_risk_on_promotable": False,
            "can_target_path_metrics_pass": False,
            "is_dynamic_promotion_unblocked": False,
            "next_action": recommendation,
        },
        artifact_paths=_artifact_paths(),
    )


def build_final_matrix(
    *,
    owner_pack: Mapping[str, Any],
    label_summary: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    failure: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    improved = _int(_mapping(actual_path.get("summary")).get("improved_vs_flat_reference_count"))
    label_rows = _records(label_summary.get("label_rows"))
    risk_on = next(
        (row for row in label_rows if row.get("label_id") == "high_confidence_risk_on"),
        {},
    )
    if improved > 0:
        final_status = "UPPER_STATE_LABEL_RESET_IMPROVES_ACTUAL_PATH"
    elif risk_on.get("sample_status") == "SAMPLE_INSUFFICIENT":
        final_status = "RISK_ON_DIAGNOSTIC_ONLY"
    else:
        final_status = "NO_MATERIAL_IMPROVEMENT"
    return _payload(
        report_type="upper_state_label_feature_reset_final_matrix",
        title="Upper-State Label Feature Reset Final Matrix",
        status=final_status,
        summary={
            **window_metadata(primary_window),
            "owner_pack_status": owner_pack.get("status"),
            "label_status": label_summary.get("status"),
            "actual_path_status": actual_path.get("status"),
            "failure_status": failure.get("status"),
            "final_status": final_status,
            "dynamic_promotion_status": "BLOCKED",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        final_decision={
            "upper_state_detector_v1": "paused",
            "first_layer_v2": "research_only",
            "high_confidence_risk_on": "diagnostic_only",
            "dynamic_promotion": "blocked",
            "next_action": _mapping(failure.get("summary")).get("next_action"),
        },
    )


def build_first_layer_v2_owner_review_pack(
    *,
    frozen_probe_contract: Mapping[str, Any],
    effective_coverage: Mapping[str, Any],
    label_summary: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    failure: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    failure_summary = _mapping(failure.get("summary"))
    coverage_summary = _mapping(effective_coverage.get("summary"))
    actual_summary = _mapping(actual_path.get("summary"))
    improved = _int(actual_summary.get("improved_vs_flat_reference_count"))
    recommendation = (
        "REVIEW_FIRST_LAYER_V2_FORWARD_WATCH"
        if improved > 0 and not coverage_summary.get("primary_window_coverage_incomplete")
        else "KEEP_FIRST_LAYER_V2_RESEARCH_ONLY_PENDING_COVERAGE"
    )
    return _payload(
        report_type="first_layer_v2_owner_review_pack",
        title="First-Layer V2 Owner Review Pack",
        status="FIRST_LAYER_V2_OWNER_REVIEW_READY_PROMOTION_BLOCKED",
        summary={
            **window_metadata(primary_window),
            "frozen_probe_contract_status": frozen_probe_contract.get("status"),
            "effective_coverage_status": effective_coverage.get("status"),
            "label_status": label_summary.get("status"),
            "feature_audit_status": feature_audit.get("status"),
            "walk_forward_status": walk_forward.get("status"),
            "actual_path_status": actual_path.get("status"),
            "failure_primary_reason": failure_summary.get("primary_failure_reason"),
            "owner_recommendation": recommendation,
        },
        owner_answers={
            "second_layer_is_frozen": _mapping(frozen_probe_contract.get("summary")).get(
                "registry_frozen"
            ),
            "first_layer_v2_research_window": PRIMARY_WINDOW_ID,
            "label_taxonomy_v2_more_stable": label_summary.get("status")
            in {
                "UPPER_STATE_LABELS_V2_READY_PROMOTION_BLOCKED",
                "UPPER_STATE_LABELS_V2_READY_RISK_ON_SAMPLE_INSUFFICIENT_PROMOTION_BLOCKED",
            },
            "learnable_labels": _learnable_label_summary(walk_forward),
            "actual_path_improves": improved > 0,
            "failure_reason": failure_summary.get("primary_failure_reason"),
            "dynamic_promotion_remains_blocked": True,
        },
        artifact_paths=_artifact_paths() | _first_layer_v2_artifact_paths(),
    )


def build_first_layer_v2_label_feature_model_final_matrix(
    *,
    frozen_probe_contract: Mapping[str, Any],
    effective_coverage: Mapping[str, Any],
    label_summary: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    failure: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    coverage_summary = _mapping(effective_coverage.get("summary"))
    actual_summary = _mapping(actual_path.get("summary"))
    label_rows = _records(label_summary.get("label_rows"))
    improved = _int(actual_summary.get("improved_vs_flat_reference_count"))
    risk_on = next(
        (row for row in label_rows if row.get("label_id") == "high_confidence_risk_on"),
        {},
    )
    if coverage_summary.get("primary_window_coverage_incomplete"):
        final_status = "WINDOW_COVERAGE_INCOMPLETE"
    elif improved > 0:
        final_status = "FIRST_LAYER_V2_ACTION_VALUE_IMPROVES"
    elif risk_on.get("sample_status") == "SAMPLE_INSUFFICIENT":
        final_status = "RISK_ON_DIAGNOSTIC_ONLY"
    else:
        final_status = "FIRST_LAYER_V2_NO_MATERIAL_IMPROVEMENT"
    return _payload(
        report_type="first_layer_v2_label_feature_model_final_matrix",
        title="First-Layer V2 Label Feature Model Final Matrix",
        status=final_status,
        summary={
            **window_metadata(primary_window),
            "final_status": final_status,
            "frozen_probe_contract_status": frozen_probe_contract.get("status"),
            "effective_coverage_status": effective_coverage.get("status"),
            "actual_prediction_start": coverage_summary.get("actual_prediction_start"),
            "actual_portfolio_start_effective": coverage_summary.get(
                "actual_portfolio_start_effective"
            ),
            "label_status": label_summary.get("status"),
            "feature_audit_status": feature_audit.get("status"),
            "walk_forward_status": walk_forward.get("status"),
            "actual_path_status": actual_path.get("status"),
            "probe_count": actual_summary.get("probe_count"),
            "label_row_count": _mapping(label_summary.get("summary")).get("label_row_count"),
            "composer_prediction_count": _mapping(walk_forward.get("summary")).get(
                "composer_prediction_count"
            ),
            "actual_path_improved_vs_flat_reference_count": improved,
            "failure_status": failure.get("status"),
            "primary_failure_reason": _mapping(failure.get("summary")).get(
                "primary_failure_reason"
            ),
            "owner_pack_status": owner_pack.get("status"),
            "dynamic_promotion_status": "BLOCKED",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        final_decision={
            "frozen_second_layer_registry": "dynamic_second_layer_probe_registry_v2",
            "first_layer_v2": "research_only",
            "high_confidence_risk_on": "diagnostic_only",
            "dynamic_promotion": "blocked",
            "next_action": _mapping(failure.get("summary")).get("next_action"),
            "target_path_metrics_can_pass": False,
        },
    )


def write_upper_state_reset_outputs(
    *,
    output_root: Path,
    frozen_probe_contract: Mapping[str, Any] | None = None,
    effective_coverage: Mapping[str, Any] | None = None,
    repair_review: Mapping[str, Any],
    protocol_review: Mapping[str, Any],
    taxonomy_review: Mapping[str, Any],
    label_regeneration_plan: Mapping[str, Any],
    action_value: pd.DataFrame,
    action_summary: Mapping[str, Any],
    labels_v2: pd.DataFrame,
    label_summary: Mapping[str, Any],
    feature_inventory: Mapping[str, Any],
    feature_matrix: pd.DataFrame,
    feature_report: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    model_results: Mapping[str, Mapping[str, Any]],
    threshold_review: Mapping[str, Any],
    composer_predictions: pd.DataFrame,
    walk_forward: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    failure: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
    first_layer_v2_owner_pack: Mapping[str, Any] | None = None,
    first_layer_v2_final_matrix: Mapping[str, Any] | None = None,
) -> None:
    if frozen_probe_contract is not None:
        _write_yaml(DEFAULT_FIRST_LAYER_V2_CONTRACT_YAML_PATH, frozen_probe_contract)
        _write_markdown(
            DEFAULT_FIRST_LAYER_V2_SCOPE_DOC_PATH,
            _render_payload_doc(frozen_probe_contract),
        )
    if effective_coverage is not None:
        _write_yaml(DEFAULT_FIRST_LAYER_V2_COVERAGE_YAML_PATH, effective_coverage)
        _write_markdown(
            DEFAULT_FIRST_LAYER_V2_COVERAGE_DOC_PATH,
            _render_payload_doc(effective_coverage),
        )
    _write_yaml(DEFAULT_UP_STATE_REPAIR_REVIEW_YAML_PATH, repair_review)
    _write_markdown(DEFAULT_UP_STATE_REPAIR_REVIEW_DOC_PATH, _render_payload_doc(repair_review))
    _write_markdown(DEFAULT_ALTERNATING_PROTOCOL_DOC_PATH, _render_payload_doc(protocol_review))
    _write_markdown(DEFAULT_TAXONOMY_RESET_DOC_PATH, _render_payload_doc(taxonomy_review))
    _write_markdown(DEFAULT_LABEL_REGEN_PLAN_DOC_PATH, _render_payload_doc(label_regeneration_plan))
    _write_csv(DEFAULT_ACTION_VALUE_V2_CSV_PATH, action_value)
    _write_json(DEFAULT_ACTION_VALUE_V2_SUMMARY_PATH, action_summary)
    _write_csv(DEFAULT_LABELS_V2_CSV_PATH, labels_v2)
    _write_yaml(DEFAULT_LABELS_V2_SUMMARY_PATH, label_summary)
    _write_markdown(DEFAULT_LABEL_QUALITY_DOC_PATH, _render_payload_doc(label_summary))
    _write_yaml(DEFAULT_FEATURE_INVENTORY_YAML_PATH, feature_inventory)
    _write_markdown(DEFAULT_FEATURE_INVENTORY_DOC_PATH, _render_payload_doc(feature_inventory))
    _write_markdown(DEFAULT_FEATURE_INVENTORY_DOC_V3_PATH, _render_payload_doc(feature_inventory))
    _write_csv(DEFAULT_PIT_FEATURE_V3_CSV_PATH, feature_matrix)
    _write_json(DEFAULT_PIT_FEATURE_V3_REPORT_PATH, feature_report)
    _write_yaml(DEFAULT_FEATURE_PIT_AUDIT_YAML_PATH, feature_audit)
    _write_markdown(DEFAULT_FEATURE_PIT_AUDIT_DOC_PATH, _render_payload_doc(feature_audit))
    for model_id, result in model_results.items():
        model_dir = output_root / "models" / model_id
        _write_csv(model_dir / "predictions.csv", _ensure_frame(result.get("predictions")))
        _write_json(model_dir / "metrics.json", _mapping(result.get("metrics")))
        _write_json(model_dir / "thresholds.json", _records(result.get("threshold_rows")))
        _write_markdown(
            DEFAULT_MODEL_REVIEW_DOCS[model_id],
            _render_model_doc(model_id, _mapping(result.get("metrics"))),
        )
    _write_markdown(DEFAULT_THRESHOLD_REVIEW_DOC_PATH, _render_payload_doc(threshold_review))
    _write_csv(DEFAULT_COMPOSER_PREDICTIONS_PATH, composer_predictions)
    _write_yaml(DEFAULT_WALK_FORWARD_MATRIX_YAML_PATH, walk_forward)
    _write_markdown(DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH, _render_payload_doc(walk_forward))
    _write_yaml(DEFAULT_ACTUAL_PATH_MATRIX_YAML_PATH, actual_path)
    _write_markdown(DEFAULT_ACTUAL_PATH_REVIEW_DOC_PATH, _render_payload_doc(actual_path))
    _write_yaml(DEFAULT_FAILURE_ATTRIBUTION_YAML_PATH, failure)
    _write_markdown(DEFAULT_FAILURE_ATTRIBUTION_DOC_PATH, _render_payload_doc(failure))
    _write_markdown(DEFAULT_OWNER_REVIEW_DOC_PATH, _render_owner_doc(owner_pack))
    if first_layer_v2_owner_pack is not None:
        _write_markdown(
            DEFAULT_FIRST_LAYER_V2_OWNER_REVIEW_DOC_PATH,
            _render_owner_doc(first_layer_v2_owner_pack),
        )
    forward_path = (
        DEFAULT_FIRST_LAYER_V2_FORWARD_WATCH_DOC_PATH
        if _int(_mapping(actual_path.get("summary")).get("improved_vs_flat_reference_count")) > 0
        else DEFAULT_RISK_OFF_ONLY_FORWARD_WATCH_DOC_PATH
    )
    forward_payload = first_layer_v2_final_matrix or final_matrix
    _write_markdown(forward_path, _render_forward_watch_doc(forward_payload))
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_payload_doc(final_matrix))
    if first_layer_v2_final_matrix is not None:
        _write_yaml(DEFAULT_FIRST_LAYER_V2_FINAL_MATRIX_YAML_PATH, first_layer_v2_final_matrix)
        _write_markdown(
            DEFAULT_FIRST_LAYER_V2_CLOSEOUT_DOC_PATH,
            _render_payload_doc(first_layer_v2_final_matrix),
        )


def _action_scores_from_state_rows(
    state_rows: Mapping[str, Mapping[str, Any]],
    score_policy: Mapping[str, Any],
) -> dict[str, Any]:
    neutral = _mapping(state_rows.get("neutral"))
    defensive_candidates = [
        _mapping(state_rows.get("risk_off")),
        _mapping(state_rows.get("defensive")),
    ]
    defensive = max(defensive_candidates, key=lambda row: _base_action_score(row, score_policy))
    constructive = _mapping(state_rows.get("constructive"))
    risk_on = _mapping(state_rows.get("risk_on"))
    thresholds = _mapping(score_policy.get("thresholds"))
    weights_cfg = _mapping(score_policy.get("score_weights"))
    keep = max([neutral, constructive], key=lambda row: _base_action_score(row, score_policy))
    keep_score = _base_action_score(keep, score_policy)
    defensive_score = _base_action_score(defensive, score_policy)
    neutral_score = _base_action_score(neutral, score_policy)
    constructive_score = _base_action_score(constructive, score_policy)
    risk_on_score = _base_action_score(risk_on, score_policy)
    keep_dd_gap = abs(_float(keep.get("future_max_drawdown"))) - abs(
        _float(defensive.get("future_max_drawdown"))
    )
    do_not_de_risk = (
        keep_score
        - defensive_score
        - max(0.0, keep_dd_gap) * _float(weights_cfg.get("drawdown_delta"), default=0.70)
    )
    stay_constructive = (
        constructive_score
        - neutral_score
        + (_float(constructive.get("future_return")) - _float(neutral.get("future_return")))
        * _float(weights_cfg.get("return_delta"), default=1.0)
    )
    add_risk_base = max(constructive_score, risk_on_score) - neutral_score
    add_risk = add_risk_base - max(
        _float(constructive.get("stress_penalty")),
        _float(risk_on.get("stress_penalty")),
    ) * _float(weights_cfg.get("stress_penalty"), default=0.45)
    risk_on_diag = (
        risk_on_score
        - constructive_score
        + (_float(risk_on.get("future_return")) - _float(neutral.get("future_return")))
        - _float(_mapping(risk_on.get("weights")).get("TQQQ"))
        * _float(_mapping(score_policy.get("tqqq")).get("penalty_per_weight"), default=0.20)
    )
    state_scores = {
        "risk_off": _base_action_score(_mapping(state_rows.get("risk_off")), score_policy),
        "defensive": _base_action_score(_mapping(state_rows.get("defensive")), score_policy),
        "neutral": neutral_score,
        "constructive": constructive_score,
        "risk_on": risk_on_score,
    }
    best_state = max(state_scores, key=state_scores.get)
    sorted_scores = sorted(state_scores.values(), reverse=True)
    return {
        "do_not_de_risk_score": round(do_not_de_risk, GRID_ROUND_DIGITS),
        "stay_constructive_score": round(stay_constructive, GRID_ROUND_DIGITS),
        "add_risk_score": round(add_risk, GRID_ROUND_DIGITS),
        "risk_on_diagnostic_score": round(risk_on_diag, GRID_ROUND_DIGITS),
        "best_state_by_full_score": best_state,
        "best_state_score": round(state_scores[best_state], GRID_ROUND_DIGITS),
        "second_best_state_score": (
            round(sorted_scores[1], GRID_ROUND_DIGITS)
            if len(sorted_scores) > 1
            else round(sorted_scores[0], GRID_ROUND_DIGITS)
        ),
        "state_score_margin": (
            round(sorted_scores[0] - sorted_scores[1], GRID_ROUND_DIGITS)
            if len(sorted_scores) > 1
            else 0.0
        ),
        "neutral_future_return": round(_float(neutral.get("future_return")), GRID_ROUND_DIGITS),
        "constructive_future_return": round(
            _float(constructive.get("future_return")), GRID_ROUND_DIGITS
        ),
        "risk_on_future_return": round(_float(risk_on.get("future_return")), GRID_ROUND_DIGITS),
        "neutral_max_drawdown": round(
            _float(neutral.get("future_max_drawdown")), GRID_ROUND_DIGITS
        ),
        "constructive_max_drawdown": round(
            _float(constructive.get("future_max_drawdown")), GRID_ROUND_DIGITS
        ),
        "risk_on_max_drawdown": round(
            _float(risk_on.get("future_max_drawdown")), GRID_ROUND_DIGITS
        ),
        "constructive_return_delta_vs_neutral": round(
            _float(constructive.get("future_return")) - _float(neutral.get("future_return")),
            GRID_ROUND_DIGITS,
        ),
        "risk_on_return_delta_vs_neutral": round(
            _float(risk_on.get("future_return")) - _float(neutral.get("future_return")),
            GRID_ROUND_DIGITS,
        ),
        "constructive_tqqq_weight": round(
            _float(_mapping(constructive.get("weights")).get("TQQQ")), GRID_ROUND_DIGITS
        ),
        "risk_on_tqqq_weight": round(
            _float(_mapping(risk_on.get("weights")).get("TQQQ")), GRID_ROUND_DIGITS
        ),
        "max_stress_penalty": round(
            max(
                _float(neutral.get("stress_penalty")),
                _float(constructive.get("stress_penalty")),
                _float(risk_on.get("stress_penalty")),
            ),
            GRID_ROUND_DIGITS,
        ),
        "threshold_do_not_de_risk": _float(thresholds.get("do_not_de_risk_score")),
        "threshold_stay_constructive": _float(thresholds.get("stay_constructive_score")),
        "threshold_add_risk": _float(thresholds.get("add_risk_score")),
        "threshold_risk_on_diagnostic": _float(thresholds.get("risk_on_diagnostic_score")),
    }


def _base_action_score(row: Mapping[str, Any], score_policy: Mapping[str, Any]) -> float:
    weights = _mapping(score_policy.get("score_weights"))
    tqqq = _mapping(score_policy.get("tqqq"))
    row_weights = _mapping(row.get("weights"))
    return (
        _float(row.get("future_return"))
        - _float(weights.get("drawdown_delta"), default=0.70)
        * abs(_float(row.get("future_max_drawdown")))
        - _float(weights.get("stress_penalty"), default=0.45)
        * abs(min(0.0, _float(row.get("worst_5d_return"))))
        - _float(tqqq.get("penalty_per_weight"), default=0.20) * _float(row_weights.get("TQQQ"))
    )


def _cached_future_metrics(
    *,
    returns: pd.DataFrame,
    weights: Mapping[str, float],
    horizon: int,
    row_count: int,
    cache: dict[tuple[tuple[float, float, float], int], list[dict[str, float]]],
) -> list[dict[str, float]]:
    key = (_weight_key(weights), horizon)
    if key not in cache:
        cache[key] = _future_metric_rows(
            returns=returns,
            weights=weights,
            horizon=horizon,
            row_count=row_count,
        )
    return cache[key]


def _future_metric_rows(
    *,
    returns: pd.DataFrame,
    weights: Mapping[str, float],
    horizon: int,
    row_count: int,
) -> list[dict[str, float]]:
    weight_series = pd.Series(weights).reindex(returns.columns).fillna(0.0)
    portfolio_returns = (returns * weight_series).sum(axis=1).to_numpy(dtype=float)
    rows = []
    for idx in range(row_count):
        window = portfolio_returns[idx + 1 : idx + horizon + 1]
        if window.size == 0:
            rows.append(_empty_future_metrics())
            continue
        equity = np.cumprod(1.0 + window)
        running_max = np.maximum.accumulate(equity)
        drawdown = equity / running_max - 1.0
        rows.append(
            {
                "future_return": float(equity[-1] - 1.0),
                "future_max_drawdown": float(drawdown.min()),
                "worst_1d_return": float(window.min()),
                "worst_5d_return": _worst_array_window_return(window, 5),
                "worst_20d_return": _worst_array_window_return(window, 20),
                "stress_penalty": abs(min(0.0, _worst_array_window_return(window, 5))),
            }
        )
    return rows


def _empty_future_metrics() -> dict[str, float]:
    return {
        "future_return": 0.0,
        "future_max_drawdown": 0.0,
        "worst_1d_return": 0.0,
        "worst_5d_return": 0.0,
        "worst_20d_return": 0.0,
        "stress_penalty": 0.0,
    }


def _apply_risk_on_sample_floor(
    labels: pd.DataFrame,
    taxonomy: Mapping[str, Any],
) -> pd.DataFrame:
    if labels.empty:
        return labels
    quality = _mapping(taxonomy.get("label_quality"))
    floors = _mapping(quality.get("minimum_positive_samples"))
    floor = _int(floors.get("high_confidence_risk_on"), default=60)
    primary_mask = (labels["research_window_id"] == PRIMARY_WINDOW_ID) & (
        labels["horizon_days"].astype(int) == 20
    )
    positives = int(labels.loc[primary_mask, "high_confidence_risk_on_label"].sum())
    labels = labels.copy()
    labels["high_confidence_risk_on_sample_floor"] = floor
    labels["high_confidence_risk_on_primary_positive_count"] = positives
    labels["high_confidence_risk_on_sample_status"] = (
        "PASS" if positives >= floor else "RISK_ON_SAMPLE_INSUFFICIENT"
    )
    if positives < floor:
        labels["high_confidence_risk_on_training_allowed"] = False
    else:
        labels["high_confidence_risk_on_training_allowed"] = True
    return labels


def _compute_feature_frame(prices: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    qqq = prices["QQQ"].astype(float)
    sgov = prices["SGOV"].astype(float)
    tqqq = prices["TQQQ"].astype(float)
    returns = qqq.pct_change().fillna(0.0)
    tqqq_returns = tqqq.pct_change().fillna(0.0)
    sgov_returns = sgov.pct_change().fillna(0.0)
    ma20 = qqq.rolling(20, min_periods=5).mean()
    ma60 = qqq.rolling(60, min_periods=10).mean()
    ma120 = qqq.rolling(120, min_periods=20).mean()
    vol20 = returns.rolling(20, min_periods=5).std(ddof=0) * math.sqrt(252.0)
    vol60 = returns.rolling(60, min_periods=10).std(ddof=0) * math.sqrt(252.0)
    downside = returns.clip(upper=0.0)
    downside20 = downside.rolling(20, min_periods=5).std(ddof=0) * math.sqrt(252.0)
    high20 = qqq.rolling(20, min_periods=5).max()
    high60 = qqq.rolling(60, min_periods=10).max()
    low20 = qqq.rolling(20, min_periods=5).min()
    low60 = qqq.rolling(60, min_periods=10).min()
    rate_frame = (
        rates.reindex(prices.index).ffill() if not rates.empty else pd.DataFrame(index=prices.index)
    )
    dgs10 = rate_frame.get("DGS10", pd.Series(0.0, index=prices.index))
    dgs2 = rate_frame.get("DGS2", pd.Series(0.0, index=prices.index))
    usd = rate_frame.get("DTWEXBGS", pd.Series(0.0, index=prices.index))
    features = pd.DataFrame(index=prices.index)
    features["qqq_momentum_20d"] = qqq.pct_change(20)
    features["qqq_momentum_60d"] = qqq.pct_change(60)
    features["qqq_momentum_120d"] = qqq.pct_change(120)
    features["qqq_ma_slope_20_60"] = ma20 / ma60 - 1.0
    features["qqq_ma_slope_60_120"] = ma60 / ma120 - 1.0
    features["qqq_drawdown_126d"] = qqq / qqq.rolling(126, min_periods=20).max() - 1.0
    features["realized_vol_20d"] = vol20
    features["realized_vol_decline_20d"] = vol20.shift(20) - vol20
    features["realized_vol_decline_60d"] = vol60.shift(60) - vol60
    features["downside_vol_20d"] = downside20
    features["downside_vol_decline_20d"] = downside20.shift(20) - downside20
    features["qqq_above_ma60_duration_60d"] = (qqq > ma60).rolling(60, min_periods=10).mean()
    features["qqq_drawdown_recovery_20d"] = features["qqq_drawdown_126d"] - features[
        "qqq_drawdown_126d"
    ].shift(20)
    features["qqq_higher_high_proxy_20d"] = (high20 > high20.shift(20)).astype(float)
    features["qqq_higher_low_proxy_20d"] = (low20 > low20.shift(20)).astype(float)
    features["qqq_distance_from_60d_high"] = qqq / high60 - 1.0
    features["days_since_60d_low"] = _days_since_rolling_low(qqq, 60)
    features["recovery_from_60d_low"] = qqq / low60 - 1.0
    features["qqq_trend_consistency_60d"] = returns.gt(0.0).rolling(60, min_periods=10).mean()
    features["qqq_pullback_contained_above_ma60"] = (
        (features["qqq_distance_from_60d_high"] > -0.08) & (qqq > ma60)
    ).astype(float)
    features["qqq_reclaim_recent_high_20d"] = (qqq >= high20.shift(1)).astype(float)
    features["qqq_recovery_speed_20d"] = (qqq / low20 - 1.0) / 20.0
    features["qqq_vs_sgov_momentum_60d"] = (
        returns.rolling(60, min_periods=10).sum() - sgov_returns.rolling(60, min_periods=10).sum()
    )
    features["qqq_vs_tqqq_consistency_20d"] = (
        (np.sign(returns).eq(np.sign(tqqq_returns))).rolling(20, min_periods=5).mean()
    )
    features["yield_curve_10y2y"] = dgs10 - dgs2
    features["usd_trend_20d"] = usd.pct_change(20)
    return features.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _train_single_submodel(
    *,
    model_id: str,
    spec: Mapping[str, Any],
    merged: pd.DataFrame,
    threshold_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    wf = _mapping(threshold_policy.get("walk_forward"))
    train_window = _int(wf.get("train_window_days"), default=504)
    validation_window = _int(wf.get("validation_window_days"), default=63)
    step = _int(wf.get("step_days"), default=21)
    min_train = _int(wf.get("min_train_samples"), default=300)
    mode = str(wf.get("mode", "rolling_fixed"))
    initial_train_window = (
        _int(wf.get("min_train_days"), default=train_window)
        if mode == "expanding_initial"
        else train_window
    )
    expanding_until = _int(wf.get("expanding_until_days"), default=train_window)
    label_column = str(spec.get("label_column"))
    feature_weights = _mapping(spec.get("feature_weights"))
    floor = _int(_mapping(threshold_policy.get("positive_sample_floor")).get(model_id), default=0)
    threshold_cfg = _mapping(threshold_policy.get("threshold_selection"))
    positive_quantile = _float(threshold_cfg.get("positive_score_quantile"), default=0.65)
    all_scores = _score_rows(merged, feature_weights)
    working = merged.copy()
    working["model_score"] = all_scores
    predictions = []
    thresholds = []
    split_id = 0
    for validation_start in range(
        initial_train_window,
        max(initial_train_window, len(working) - validation_window),
        step,
    ):
        if mode == "expanding_initial":
            if validation_start <= expanding_until:
                train_start = 0
            else:
                train_start = validation_start - expanding_until
        else:
            train_start = validation_start - train_window
        train = working.iloc[train_start:validation_start].copy()
        train = train.loc[train["train_usable"].astype(bool)].copy()
        validation = working.iloc[validation_start : validation_start + validation_window].copy()
        if len(train) < min_train or validation.empty:
            continue
        positives = train.loc[train[label_column].astype(bool), "model_score"]
        positive_count = len(positives)
        if positive_count < floor:
            threshold = math.inf
            sample_status = "SAMPLE_INSUFFICIENT"
        else:
            threshold = float(positives.quantile(positive_quantile))
            sample_status = "PASS"
        thresholds.append(
            {
                **window_metadata(primary_window),
                "model_id": model_id,
                "split_id": split_id,
                "train_start": str(train["date"].iloc[0]),
                "train_end": str(train["date"].iloc[-1]),
                "validation_start": str(validation["date"].iloc[0]),
                "validation_end": str(validation["date"].iloc[-1]),
                "train_sample_count": len(train),
                "positive_train_sample_count": positive_count,
                "positive_sample_floor": floor,
                "threshold": threshold,
                "sample_status": sample_status,
            }
        )
        for _, row in validation.iterrows():
            score = _float(row.get("model_score"))
            prediction = bool(score >= threshold)
            predictions.append(
                {
                    **window_metadata(primary_window),
                    "date": str(row["date"]),
                    "model_id": model_id,
                    "split_id": split_id,
                    "label": bool(row.get(label_column)),
                    "prediction": prediction,
                    "score": round(score, GRID_ROUND_DIGITS),
                    "threshold": threshold,
                    "confidence": _binary_confidence(score, threshold),
                    "sample_status": sample_status,
                    "known_at": str(row.get("known_at")),
                    "available_at": str(row.get("available_at")),
                    "decision_at": str(row.get("decision_at")),
                    "feature_snapshot_hash": _row_hash(row, feature_weights.keys()),
                    "target_path_metrics_used_for_pass": False,
                    **SAFETY_BOUNDARY,
                }
            )
        split_id += 1
    prediction_frame = pd.DataFrame(predictions)
    metrics = _binary_metrics(
        prediction_frame,
        model_id=model_id,
        primary_window=primary_window,
        positive_sample_floor=floor,
        split_count=split_id,
    )
    metrics["status"] = _model_status(model_id, metrics)
    return {
        "predictions": prediction_frame,
        "metrics": metrics,
        "threshold_rows": thresholds,
        "feature_weights": feature_weights,
    }


def _binary_metrics(
    predictions: pd.DataFrame,
    *,
    model_id: str,
    primary_window: Mapping[str, Any],
    positive_sample_floor: int,
    split_count: int,
) -> dict[str, Any]:
    if predictions.empty:
        return {
            **window_metadata(primary_window),
            "model_id": model_id,
            "prediction_count": 0,
            "split_count": split_count,
            "positive_sample_floor": positive_sample_floor,
            "precision": 0.0,
            "recall": 0.0,
            "accuracy": 0.0,
            "positive_count": 0,
            "predicted_positive_count": 0,
            "target_path_metrics_used_for_pass": False,
            **SAFETY_BOUNDARY,
        }
    label = predictions["label"].astype(bool)
    pred = predictions["prediction"].astype(bool)
    tp = int((label & pred).sum())
    fp = int((~label & pred).sum())
    fn = int((label & ~pred).sum())
    positive_count = int(label.sum())
    predicted_count = int(pred.sum())
    return {
        **window_metadata(primary_window),
        "model_id": model_id,
        "prediction_count": len(predictions),
        "split_count": split_count,
        "positive_sample_floor": positive_sample_floor,
        "positive_count": positive_count,
        "predicted_positive_count": predicted_count,
        "precision": round(tp / max(tp + fp, 1), 6),
        "recall": round(tp / max(tp + fn, 1), 6),
        "accuracy": round(float((label == pred).mean()), 6),
        "target_path_metrics_used_for_pass": False,
        **SAFETY_BOUNDARY,
    }


def _model_status(model_id: str, metrics: Mapping[str, Any]) -> str:
    if model_id == "high_confidence_risk_on_model_v1" and _int(
        metrics.get("positive_count")
    ) < _int(metrics.get("positive_sample_floor")):
        return "RISK_ON_SAMPLE_INSUFFICIENT"
    if _int(metrics.get("prediction_count")) <= 0:
        return "NO_WALK_FORWARD_PREDICTIONS"
    if _float(metrics.get("precision")) >= 0.35 and _float(metrics.get("recall")) >= 0.10:
        return "MODEL_DIAGNOSTIC_READY"
    return "MODEL_EDGE_WEAK"


def _compose_state(row: Mapping[str, Any]) -> str:
    do_not = bool(row.get("do_not_de_risk_pred", False))
    stay = bool(row.get("stay_constructive_pred", False))
    add = bool(row.get("add_risk_pred", False))
    risk_on = bool(row.get("high_confidence_risk_on_pred", False))
    do_not_conf = _float(row.get("do_not_de_risk_model_confidence"), default=0.0)
    if not do_not:
        return "risk_off" if do_not_conf >= 0.55 else "defensive"
    if risk_on and add:
        return "risk_on"
    if add or stay:
        return "constructive"
    return "neutral"


def _compose_confidence(row: Mapping[str, Any], state: str) -> float:
    columns = [
        "do_not_de_risk_model_confidence",
        "stay_constructive_model_confidence",
        "add_risk_model_confidence",
        "high_confidence_risk_on_model_confidence",
    ]
    values = [_float(row.get(column), default=0.35) for column in columns]
    if state == "risk_on":
        return round(max(values[2], values[3]), 6)
    if state == "constructive":
        return round(max(values[1], values[2]), 6)
    if state in {"risk_off", "defensive"}:
        return round(values[0], 6)
    return round(float(np.mean(values[:3])), 6)


def _score_rows(frame: pd.DataFrame, feature_weights: Mapping[str, Any]) -> pd.Series:
    score = pd.Series(0.0, index=frame.index)
    for feature, weight in feature_weights.items():
        score = score + frame.get(str(feature), pd.Series(0.0, index=frame.index)).astype(
            float
        ) * _float(weight)
    return score.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _binary_confidence(score: float, threshold: float) -> float:
    if math.isinf(threshold):
        return 0.35
    distance = abs(score - threshold)
    scale = max(abs(threshold), 0.01)
    return round(min(0.95, 0.35 + min(distance / scale, 1.0) * 0.60), 6)


def _days_since_rolling_low(series: pd.Series, window: int) -> pd.Series:
    values = []
    arr = series.to_numpy(dtype=float)
    for idx in range(len(arr)):
        start = max(0, idx - window + 1)
        current = arr[start : idx + 1]
        if len(current) == 0:
            values.append(0.0)
            continue
        low_pos = int(np.argmin(current))
        values.append(float(len(current) - low_pos - 1))
    return pd.Series(values, index=series.index) / float(window)


def _feature_family(feature: str) -> str:
    if "vol" in feature:
        return "volatility_compression"
    if "yield" in feature or "usd" in feature:
        return "macro_context"
    if "sgov" in feature or "tqqq" in feature:
        return "relative_confirmation"
    if "drawdown" in feature or "recovery" in feature or "low" in feature:
        return "recovery_strength"
    return "trend_persistence"


def _label_horizon(threshold_policy: Mapping[str, Any]) -> int:
    return _int(
        _mapping(threshold_policy.get("walk_forward")).get("label_horizon_days"),
        default=20,
    )


def _model_dir(model_id: str) -> Path:
    return DEFAULT_MODEL_ROOT / model_id


def _artifact_paths() -> dict[str, str]:
    paths = {
        "up_state_repair_result_review": DEFAULT_UP_STATE_REPAIR_REVIEW_DOC_PATH,
        "alternating_protocol": DEFAULT_ALTERNATING_PROTOCOL_DOC_PATH,
        "taxonomy_reset": DEFAULT_TAXONOMY_RESET_DOC_PATH,
        "action_value_matrix_v2": DEFAULT_ACTION_VALUE_V2_CSV_PATH,
        "upper_state_labels_v2": DEFAULT_LABELS_V2_CSV_PATH,
        "pit_feature_matrix_v3": DEFAULT_PIT_FEATURE_V3_CSV_PATH,
        "composer_predictions": DEFAULT_COMPOSER_PREDICTIONS_PATH,
        "actual_path_matrix": DEFAULT_ACTUAL_PATH_MATRIX_YAML_PATH,
        "failure_attribution": DEFAULT_FAILURE_ATTRIBUTION_YAML_PATH,
        "final_matrix": DEFAULT_FINAL_MATRIX_YAML_PATH,
    }
    paths.update({model_id: _model_dir(model_id) for model_id in MODEL_IDS})
    return {key: str(value) for key, value in paths.items()}


def _first_layer_v2_artifact_paths() -> dict[str, str]:
    return {
        "frozen_probe_scope": str(DEFAULT_FIRST_LAYER_V2_SCOPE_DOC_PATH),
        "frozen_probe_contract": str(DEFAULT_FIRST_LAYER_V2_CONTRACT_YAML_PATH),
        "effective_coverage_audit": str(DEFAULT_FIRST_LAYER_V2_COVERAGE_YAML_PATH),
        "first_layer_v2_owner_pack": str(DEFAULT_FIRST_LAYER_V2_OWNER_REVIEW_DOC_PATH),
        "first_layer_v2_final_matrix": str(DEFAULT_FIRST_LAYER_V2_FINAL_MATRIX_YAML_PATH),
        "first_layer_v2_closeout": str(DEFAULT_FIRST_LAYER_V2_CLOSEOUT_DOC_PATH),
    }


def _learnable_label_summary(walk_forward: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in _records(walk_forward.get("model_rows")):
        model_id = str(row.get("model_id"))
        label_id = model_id.removesuffix("_model_v1")
        status = str(row.get("status"))
        result[label_id] = "learnable_diagnostic" if status == "MODEL_DIAGNOSTIC_READY" else status
    return result


def _failure_next_action(*, coverage_incomplete: bool, improved_count: int) -> str:
    if coverage_incomplete:
        return "REBUILD_WALK_FORWARD_COVERAGE_BEFORE_OWNER_ESCALATION"
    if improved_count <= 0:
        return "KEEP_RISK_OFF_ONLY_FORWARD_WATCH"
    return "OWNER_REVIEW_FIRST_LAYER_V2_FORWARD_WATCH"


def _first_layer_candidate_count(summary: Mapping[str, Any], extra: Mapping[str, Any]) -> int:
    for key in (
        "probe_count",
        "submodel_count",
        "approved_feature_count",
        "label_row_count",
        "row_count",
        "action_value_row_count",
    ):
        if key in summary:
            return max(0, _int(summary.get(key)))
    for key in ("probe_rows", "model_rows", "label_rows", "features"):
        if key in extra:
            return len(_records(extra.get(key)))
    return 0


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    summary_dict = dict(summary)
    window_fields = {
        key: summary_dict[key]
        for key in (
            "research_window_id",
            "research_window_alias",
            "requested_start",
            "actual_start",
            "actual_portfolio_start",
            "end",
            "window_role",
            "data_quality_contract",
            "exact_or_proxy",
        )
        if key in summary_dict
    }
    candidate_count = _first_layer_candidate_count(summary_dict, extra)
    audit_metadata = (
        {
            "modified_layer": "first_layer",
            "frozen_first_layer_version": "frozen_or_not_applicable",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": window_fields["research_window_id"],
            "label_version": "upper_state_label_taxonomy_v2",
            "feature_set_version": "pit_feature_matrix_v3",
            "model_version": "first_layer_composer_v2",
            "threshold_policy": "first_layer_threshold_policy_v2",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": candidate_count,
            "pre_registered_selection_rule": True,
        }
        if "research_window_id" in window_fields
        else None
    )
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        **window_fields,
        "summary": summary_dict,
        **SAFETY_BOUNDARY,
        **({"research_audit_metadata": audit_metadata} if audit_metadata else {}),
        **extra,
    }


def _render_payload_doc(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- 市场周期：`{payload.get('market_regime')}`",
        f"- promotion_allowed：`{payload.get('promotion_allowed')}`",
        f"- paper_shadow_allowed：`{payload.get('paper_shadow_allowed')}`",
        f"- production_allowed：`{payload.get('production_allowed')}`",
        f"- broker_action：`{payload.get('broker_action')}`",
        "",
        "## 摘要",
    ]
    for key, value in _mapping(payload.get("summary")).items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _render_owner_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Owner Review Questions", ""]
    for key, value in _mapping(payload.get("owner_answers")).items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _render_model_doc(model_id: str, metrics: Mapping[str, Any]) -> str:
    payload = _payload(
        report_type=f"{model_id}_review",
        title=f"{model_id} Review",
        status=str(metrics.get("status", "MODEL_REVIEW_READY_PROMOTION_BLOCKED")),
        summary=metrics,
    )
    return _render_payload_doc(payload)


def _render_forward_watch_doc(final_matrix: Mapping[str, Any]) -> str:
    summary = _mapping(final_matrix.get("summary"))
    decision = _mapping(final_matrix.get("final_decision"))
    lines = [
        "# Upper-State Forward Watch Disposition",
        "",
        f"- 状态：`{final_matrix.get('status')}`",
        f"- final_status：`{summary.get('final_status')}`",
        f"- next_action：`{decision.get('next_action')}`",
        "- paper_shadow_allowed：`False`",
        "- production_allowed：`False`",
        "- broker_action：`none`",
        "",
        (
            "该计划仅用于 research-only forward watch；不得解释为 paper-shadow、"
            "production 或 broker eligibility。"
        ),
    ]
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    formatted = frame.copy()
    for column in formatted.columns:
        formatted[column] = formatted[column].map(_csv_cell)
    formatted.to_csv(path, index=False)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_scalar(payload), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _csv_cell(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(_json_scalar(value), sort_keys=True, ensure_ascii=False)
    return value


def _json_scalar(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _ensure_frame(value: Any) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _records(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _weight_key(weights: Mapping[str, float]) -> tuple[float, float, float]:
    return (
        round(_float(weights.get("QQQ")), GRID_ROUND_DIGITS),
        round(_float(weights.get("SGOV")), GRID_ROUND_DIGITS),
        round(_float(weights.get("TQQQ")), GRID_ROUND_DIGITS),
    )


def _worst_array_window_return(values: np.ndarray, window: int) -> float:
    if len(values) < window:
        return float(values.min()) if len(values) else 0.0
    worst = 0.0
    for idx in range(len(values) - window + 1):
        compounded = float(np.prod(1.0 + values[idx : idx + window]) - 1.0)
        worst = min(worst, compounded)
    return worst


def _label_sum(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].astype(bool).sum())


def _state_counts(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.astype(str).value_counts().items()}


def _min_iso_date(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    values = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if values.empty:
        return ""
    return values.min().date().isoformat()


def _frame_date_range(frame: pd.DataFrame) -> DateRange | None:
    if frame.empty or "date" not in frame.columns:
        return None
    values = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if values.empty:
        return None
    return DateRange(values.min().date(), values.max().date())


def _actual_path_date_range(actual_path: Mapping[str, Any]) -> DateRange | None:
    rows = _records(actual_path.get("probe_rows"))
    starts = [
        parsed
        for row in rows
        if (parsed := _optional_context_date(row.get("date_start"))) is not None
    ]
    ends = [
        parsed
        for row in rows
        if (parsed := _optional_context_date(row.get("date_end"))) is not None
    ]
    if not starts or not ends:
        return None
    return DateRange(min(starts), max(ends))


def _required_context_date(value: object, field: str) -> date:
    parsed = _optional_context_date(value)
    if parsed is None:
        raise ValueError(f"{field} must be an ISO date")
    return parsed


def _optional_context_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _covers_year(start: str, frame: pd.DataFrame, year: int) -> bool:
    if not start or frame.empty or "date" not in frame.columns:
        return False
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return False
    return bool((dates.dt.year == year).any())


def _row_hash(row: Mapping[str, Any], columns: Sequence[str]) -> str:
    payload = {column: _float(row.get(column)) for column in columns}
    return str(abs(hash(json.dumps(payload, sort_keys=True))) % 10**12)


def _issue(code: str) -> dict[str, str]:
    return {"code": code, "severity": "BLOCKER"}
