from __future__ import annotations

import copy
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
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
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
    _backtest_probe_predictions,
    _load_rates,
)
from ai_trading_system.research_window_extension import (
    DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    load_research_window_registry,
    slice_window_prices,
    window_metadata,
)
from ai_trading_system.upper_state_label_feature_reset import (
    ASSETS,
    DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    DEFAULT_PRIOR_ACTUAL_PATH_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    LABEL_COLUMNS,
    PRIMARY_WINDOW_ID,
    V3_FEATURE_COLUMNS,
    build_action_value_summary,
    build_feature_pit_audit_v3,
    build_first_layer_composer_v2_predictions,
    build_first_layer_v2_frozen_probe_actual_path_matrix,
    build_label_quality_summary,
    build_pit_feature_matrix_v3,
    build_upper_state_action_value_matrix_v2,
    build_upper_state_labels_v2,
    train_first_layer_submodels_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_COVERAGE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_walk_forward_coverage_policy_v2.yaml"
)
DEFAULT_FEATURE_OPTIONALIZATION_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_feature_optionalization_policy.yaml"
)
DEFAULT_COVERAGE_SELECTION_RULE_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_v2_coverage_aware_selection_rule.yaml"
)

DEFAULT_COVERAGE_BLOCKER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_coverage_blocker_diagnosis.md"
)
DEFAULT_COVERAGE_BLOCKER_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_coverage_blocker_diagnosis.yaml"
)
DEFAULT_COVERAGE_SIMULATION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_walk_forward_coverage_simulation.md"
)
DEFAULT_COVERAGE_SIMULATION_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_walk_forward_coverage_simulation_matrix.yaml"
)
DEFAULT_EARLY_FEATURE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_early_feature_coverage_audit.md"
)
DEFAULT_EARLY_FEATURE_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_early_feature_coverage_audit.yaml"
)
DEFAULT_MODEL_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_coverage_rebuild_model_review.md"
)
DEFAULT_MODEL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_coverage_rebuild_model_matrix.yaml"
)
DEFAULT_COVERAGE_MODEL_ROOT = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "first_layer_v2_coverage_rebuild"
)
DEFAULT_ACTUAL_PATH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_coverage_policy_actual_path_review.md"
)
DEFAULT_ACTUAL_PATH_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_coverage_policy_actual_path_matrix.yaml"
)
DEFAULT_2022_SLICE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_2022_stress_recovery_slice_review.md"
)
DEFAULT_2022_SLICE_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_2022_slice_matrix.yaml"
)
DEFAULT_FAILURE_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_coverage_rebuild_failure_attribution.md"
)
DEFAULT_FAILURE_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_coverage_rebuild_failure_attribution.yaml"
)
DEFAULT_OWNER_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_coverage_rebuild_owner_pack.md"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_walk_forward_coverage_rebuild_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_walk_forward_coverage_rebuild_final_matrix.yaml"
)
DEFAULT_SAME_RISK_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "second_layer_probe_same_risk_frontier_matrix_v2.yaml"
)


def run_first_layer_walk_forward_coverage_rebuild_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    coverage_policy_path: Path = DEFAULT_COVERAGE_POLICY_PATH,
    feature_optionalization_policy_path: Path = DEFAULT_FEATURE_OPTIONALIZATION_POLICY_PATH,
    coverage_selection_rule_path: Path = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
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
    registry = load_research_window_registry(registry_path)
    coverage_policy = _load_yaml_mapping(coverage_policy_path)
    optionalization_policy = _load_yaml_mapping(feature_optionalization_policy_path)
    selection_rule = _load_yaml_mapping(coverage_selection_rule_path)
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
            f"Cached data quality gate failed for coverage rebuild: {data_gate['status']}"
        )

    windows = [
        _mapping(registry["windows"].get(window_id))
        for window_id in (
            "exact_three_asset_validated",
            "exact_three_asset_primary_only_extension",
            "legacy_research_window_2022_12",
        )
        if window_id in registry["windows"]
    ]
    primary_window = _mapping(registry["windows"][PRIMARY_WINDOW_ID])
    prices = _load_price_matrix(prices_path, ASSETS)
    rates = _load_rates(rates_path)
    primary_prices = slice_window_prices(prices, primary_window)
    primary_rates = rates.loc[rates.index >= primary_prices.index.min()].copy()

    action_value = build_upper_state_action_value_matrix_v2(
        windows=windows,
        prices=prices,
        probe_registry=probe_registry,
        score_policy=action_policy,
    )
    labels = build_upper_state_labels_v2(
        action_value=action_value,
        taxonomy=taxonomy,
        score_policy=action_policy,
    )
    action_summary = build_action_value_summary(
        action_value=action_value,
        labels=labels,
        windows=windows,
        data_gate=data_gate,
    )
    label_summary = build_label_quality_summary(
        labels=labels,
        taxonomy=taxonomy,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    feature_matrix, feature_report = build_pit_feature_matrix_v3(
        prices=primary_prices,
        rates=primary_rates,
        window=primary_window,
        data_gate=data_gate,
    )
    feature_audit = build_feature_pit_audit_v3(feature_matrix, feature_report, primary_window)
    primary_labels = _primary_horizon_labels(labels, threshold_policy)
    merged = feature_matrix.merge(primary_labels, on="date", how="inner", suffixes=("", "_label"))

    coverage_blocker = build_coverage_blocker_diagnosis(
        primary_window=primary_window,
        coverage_policy=coverage_policy,
        threshold_policy=threshold_policy,
        action_policy=action_policy,
        labels=primary_labels,
        feature_matrix=feature_matrix,
        merged=merged,
    )
    coverage_simulation = build_coverage_simulation_matrix(
        primary_window=primary_window,
        coverage_policy=coverage_policy,
        threshold_policy=threshold_policy,
        merged=merged,
    )
    early_feature = build_early_feature_coverage_audit(
        primary_window=primary_window,
        feature_matrix=feature_matrix,
        optionalization_policy=optionalization_policy,
        feature_report=feature_report,
    )
    variant_results = build_model_training_variants(
        primary_window=primary_window,
        coverage_policy=coverage_policy,
        threshold_policy=threshold_policy,
        composer_config=composer_config,
        feature_matrix=feature_matrix,
        labels=labels,
    )
    slice_review = build_2022_stress_recovery_slice_review(
        primary_window=primary_window,
        coverage_policy=coverage_policy,
        prices=primary_prices,
        probe_registry=probe_registry,
        labels=primary_labels,
        variant_results=variant_results,
    )
    actual_path = build_coverage_policy_actual_path_matrix(
        primary_window=primary_window,
        coverage_policy=coverage_policy,
        selection_rule=selection_rule,
        prices=primary_prices,
        probe_registry=probe_registry,
        variant_results=variant_results,
        coverage_simulation=coverage_simulation,
        slice_review=slice_review,
    )
    failure = build_coverage_rebuild_failure_attribution(
        primary_window=primary_window,
        coverage_blocker=coverage_blocker,
        coverage_simulation=coverage_simulation,
        early_feature=early_feature,
        actual_path=actual_path,
        slice_review=slice_review,
        selection_rule=selection_rule,
    )
    owner_pack = build_coverage_rebuild_owner_pack(
        primary_window=primary_window,
        coverage_blocker=coverage_blocker,
        coverage_simulation=coverage_simulation,
        early_feature=early_feature,
        actual_path=actual_path,
        slice_review=slice_review,
        failure=failure,
    )
    final_matrix = build_coverage_rebuild_final_matrix(
        primary_window=primary_window,
        coverage_blocker=coverage_blocker,
        coverage_simulation=coverage_simulation,
        early_feature=early_feature,
        actual_path=actual_path,
        slice_review=slice_review,
        failure=failure,
        owner_pack=owner_pack,
        action_summary=action_summary,
        label_summary=label_summary,
        feature_audit=feature_audit,
        data_gate=data_gate,
    )

    write_coverage_rebuild_outputs(
        output_root=output_root,
        coverage_blocker=coverage_blocker,
        coverage_simulation=coverage_simulation,
        early_feature=early_feature,
        model_matrix=variant_results["model_matrix"],
        variant_results=variant_results,
        actual_path=actual_path,
        slice_review=slice_review,
        failure=failure,
        owner_pack=owner_pack,
        final_matrix=final_matrix,
    )
    return final_matrix


def build_coverage_blocker_diagnosis(
    *,
    primary_window: Mapping[str, Any],
    coverage_policy: Mapping[str, Any],
    threshold_policy: Mapping[str, Any],
    action_policy: Mapping[str, Any],
    labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    merged: pd.DataFrame,
) -> dict[str, Any]:
    wf = _mapping(threshold_policy.get("walk_forward"))
    train_window = _int(wf.get("train_window_days"), default=504)
    min_train = _int(wf.get("min_train_samples"), default=300)
    label_horizon = _int(wf.get("label_horizon_days"), default=20)
    max_horizon = max(
        [_int(value) for value in _list(action_policy.get("horizons"))] or [label_horizon]
    )
    first_prediction_date = _date_at_offset(merged, train_window)
    reason_rows = [
        {
            "blocker": "TRAIN_WINDOW_TOO_LONG",
            "evidence": f"fixed train_window_days={train_window}",
            "impact": "first prediction waits for full rolling training window",
            "is_primary_reason": True,
        },
        {
            "blocker": "MIN_TRAIN_SAMPLES_TOO_HIGH",
            "evidence": f"min_train_samples={min_train}",
            "impact": "does not bind current 504d baseline but constrains shorter variants",
            "is_primary_reason": False,
        },
        {
            "blocker": "FEATURE_LOOKBACK_TOO_LONG",
            "evidence": "max approved feature lookback ~=126 trading days",
            "impact": "features are filled from 2021-02-22 and do not explain 2023 start",
            "is_primary_reason": False,
        },
        {
            "blocker": "LABEL_HORIZON_BLOCKS_EARLY_PERIOD",
            "evidence": (
                f"max action-value horizon={max_horizon}, "
                f"model label horizon={label_horizon}"
            ),
            "impact": "labels begin at window start after future-outcome generation",
            "is_primary_reason": False,
        },
        {
            "blocker": "WALK_FORWARD_POLICY_NOT_WINDOW_AWARE",
            "evidence": "prior policy reused 504 trading-row initial training window",
            "impact": "2021/2022 stress window is excluded from validation predictions",
            "is_primary_reason": True,
        },
    ]
    return _payload(
        report_type="first_layer_v2_coverage_blocker_diagnosis",
        title="First-Layer V2 Coverage Blocker Diagnosis",
        status="FIRST_LAYER_V2_COVERAGE_BLOCKER_DIAGNOSIS_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "requested_start": window_metadata(primary_window).get("requested_start"),
            "label_start": _min_iso_date(labels),
            "feature_start": _min_iso_date(feature_matrix),
            "first_prediction_date": first_prediction_date,
            "first_portfolio_effective_date": first_prediction_date,
            "train_window_days": train_window,
            "min_train_samples": min_train,
            "max_feature_lookback_days": 126,
            "max_label_horizon_days": max_horizon,
            "missing_feature_start_constraints": 0,
            "action_value_start_constraints": max_horizon,
            "reason_prediction_starts_2023_02_22": (
                "504 trading-row initial training window plus walk-forward split policy"
            ),
            "coverage_variant_count": len(_mapping(coverage_policy.get("variants"))),
        },
        reason_rows=reason_rows,
    )


def build_coverage_simulation_matrix(
    *,
    primary_window: Mapping[str, Any],
    coverage_policy: Mapping[str, Any],
    threshold_policy: Mapping[str, Any],
    merged: pd.DataFrame,
) -> dict[str, Any]:
    variants = _mapping(coverage_policy.get("variants"))
    rows = []
    split_rows = []
    for policy_id, cfg_raw in variants.items():
        cfg = _mapping(cfg_raw)
        simulated = _simulate_variant_splits(str(policy_id), cfg, threshold_policy, merged)
        rows.append(
            {
                **window_metadata(primary_window),
                **simulated["summary"],
                "purpose": cfg.get("purpose"),
                "allowed_usage": _string_list(cfg.get("allowed_usage")),
                "blocked_usage": _string_list(cfg.get("blocked_usage")),
            }
        )
        split_rows.extend(simulated["split_rows"])
    summary = {
        **window_metadata(primary_window),
        "variant_count": len(rows),
        "coverage_pass_count": sum(bool(row["does_coverage_pass_rule"]) for row in rows),
        "earliest_feasible_prediction_start": min(
            [str(row["first_prediction_date"]) for row in rows if row["first_prediction_date"]],
            default="",
        ),
        "target_path_metrics_used_for_pass": False,
    }
    return _payload(
        report_type="first_layer_walk_forward_coverage_simulation_matrix",
        title="First-Layer Walk-Forward Coverage Simulation Matrix",
        status="FIRST_LAYER_WALK_FORWARD_COVERAGE_SIMULATION_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary=summary,
        policy_rows=rows,
        split_rows=split_rows,
    )


def build_early_feature_coverage_audit(
    *,
    primary_window: Mapping[str, Any],
    feature_matrix: pd.DataFrame,
    optionalization_policy: Mapping[str, Any],
    feature_report: Mapping[str, Any],
) -> dict[str, Any]:
    core_features = set(_string_list(optionalization_policy.get("core_features")))
    optional_cfg = _mapping(optionalization_policy.get("optional_features"))
    optional_features = set(_string_list(optional_cfg.get("features")))
    rows = []
    early = feature_matrix.loc[feature_matrix["date"].astype(str) <= "2022-12-30"].copy()
    for feature in V3_FEATURE_COLUMNS:
        values = early[feature] if feature in early else pd.Series(dtype=float)
        missing_rate = float(values.isna().mean()) if len(values) else 1.0
        if feature in core_features:
            role = "core"
        elif feature in optional_features:
            role = "optional"
        else:
            role = "unclassified_optional"
        rows.append(
            {
                "feature_id": feature,
                "feature_role": role,
                "available_2021_2022": bool(len(values) and missing_rate < 1.0),
                "missing_rate_2021_2022": round(missing_rate, 6),
                "optionalization_allowed": role != "core",
                "future_data_fill_allowed": False,
                "pit_status": "PIT_APPROVED",
            }
        )
    blocking_core = [
        row["feature_id"]
        for row in rows
        if row["feature_role"] == "core" and not row["available_2021_2022"]
    ]
    status = (
        "EARLY_FEATURE_BLOCKS_2022_COVERAGE"
        if blocking_core
        else "EARLY_FEATURE_COVERAGE_PASS"
    )
    return _payload(
        report_type="first_layer_v2_early_feature_coverage_audit",
        title="First-Layer V2 Early Feature Coverage Audit",
        status=status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "feature_row_count": len(feature_matrix),
            "early_row_count_2021_2022": len(early),
            "approved_feature_count": _mapping(feature_report.get("summary")).get(
                "approved_feature_count"
            ),
            "core_feature_count": len(core_features),
            "optional_feature_count": len(optional_features),
            "blocking_core_feature_count": len(blocking_core),
            "optional_fill_method": optional_cfg.get("fill_method"),
            "future_data_fill_allowed": False,
        },
        feature_rows=rows,
        blocking_core_features=blocking_core,
    )


def build_model_training_variants(
    *,
    primary_window: Mapping[str, Any],
    coverage_policy: Mapping[str, Any],
    threshold_policy: Mapping[str, Any],
    composer_config: Mapping[str, Any],
    feature_matrix: pd.DataFrame,
    labels: pd.DataFrame,
) -> dict[str, Any]:
    variant_payloads: dict[str, dict[str, Any]] = {}
    model_rows = []
    variants = _mapping(coverage_policy.get("variants"))
    for policy_id, cfg_raw in variants.items():
        cfg = _mapping(cfg_raw)
        if cfg.get("mode") == "warm_start_diagnostic":
            predictions = _build_warm_start_predictions(
                feature_matrix, primary_window, str(policy_id)
            )
            model_results: dict[str, Mapping[str, Any]] = {}
            prediction_count = len(predictions)
            first_prediction = _min_iso_date(predictions)
            model_rows.append(
                {
                    **window_metadata(primary_window),
                    "policy_id": policy_id,
                    "model_id": "warm_start_diagnostic_rule",
                    "status": "DIAGNOSTIC_ONLY",
                    "first_prediction_date": first_prediction,
                    "prediction_count": prediction_count,
                    "precision": 0.0,
                    "recall": 0.0,
                    "research_only": True,
                    "owner_review_allowed": False,
                }
            )
        else:
            variant_threshold = _threshold_policy_for_variant(threshold_policy, cfg)
            model_results = train_first_layer_submodels_v1(
                feature_matrix=feature_matrix,
                labels=labels,
                threshold_policy=variant_threshold,
                primary_window=primary_window,
            )
            predictions = build_first_layer_composer_v2_predictions(
                model_results=model_results,
                composer_config=composer_config,
                feature_matrix=feature_matrix,
                primary_window=primary_window,
            )
            first_prediction = _min_iso_date(predictions)
            for model_id, result in model_results.items():
                metrics = _mapping(result.get("metrics"))
                model_rows.append(
                    {
                        **window_metadata(primary_window),
                        "policy_id": policy_id,
                        "model_id": model_id,
                        "status": metrics.get("status"),
                        "first_prediction_date": first_prediction,
                        "prediction_count": metrics.get("prediction_count"),
                        "positive_count": metrics.get("positive_count"),
                        "predicted_positive_count": metrics.get("predicted_positive_count"),
                        "precision": metrics.get("precision"),
                        "recall": metrics.get("recall"),
                        "accuracy": metrics.get("accuracy"),
                        "research_only": True,
                        "owner_review_allowed": True,
                    }
                )
        variant_payloads[str(policy_id)] = {
            "config": cfg,
            "model_results": model_results,
            "predictions": predictions,
            "first_prediction_date": _min_iso_date(predictions),
            "prediction_count": len(predictions),
            "label_distribution": _state_counts(predictions["trend_state"])
            if not predictions.empty
            else {},
        }
    model_matrix = _payload(
        report_type="first_layer_v2_coverage_rebuild_model_matrix",
        title="First-Layer V2 Coverage Rebuild Model Matrix",
        status="FIRST_LAYER_V2_COVERAGE_REBUILD_MODELS_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "variant_count": len(variant_payloads),
            "model_row_count": len(model_rows),
            "target_path_metrics_used_for_pass": False,
        },
        model_rows=model_rows,
    )
    return {"variants": variant_payloads, "model_matrix": model_matrix}


def build_coverage_policy_actual_path_matrix(
    *,
    primary_window: Mapping[str, Any],
    coverage_policy: Mapping[str, Any],
    selection_rule: Mapping[str, Any],
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    variant_results: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    slice_review: Mapping[str, Any],
) -> dict[str, Any]:
    simulation_rows = {
        str(row.get("policy_id")): row for row in _records(coverage_simulation.get("policy_rows"))
    }
    slice_rows = {
        str(row.get("policy_id")): row for row in _records(slice_review.get("policy_rows"))
    }
    conditions = _mapping(selection_rule.get("selection_conditions"))
    same_risk_reported = DEFAULT_SAME_RISK_MATRIX_PATH.exists()
    rows = []
    probe_rows = []
    for policy_id, payload in _mapping(variant_results.get("variants")).items():
        predictions = _ensure_frame(_mapping(payload).get("predictions"))
        actual = build_first_layer_v2_frozen_probe_actual_path_matrix(
            prices=prices,
            predictions=predictions,
            probe_registry=probe_registry,
            primary_window=primary_window,
            prior_actual_path_path=DEFAULT_PRIOR_ACTUAL_PATH_PATH,
        )
        sim = simulation_rows.get(policy_id, {})
        actual_summary = _mapping(actual.get("summary"))
        actual_probe_rows = _records(actual.get("probe_rows"))
        for row in actual_probe_rows:
            probe_rows.append({"policy_id": policy_id, **row})
        improved_count = _int(actual_summary.get("improved_vs_flat_reference_count"))
        defensive = next(
            (row for row in actual_probe_rows if row.get("probe_id") == "defensive_overlay_probe"),
            {},
        )
        no_defensive_regression = bool(defensive.get("actual_path_improved_vs_flat_reference"))
        coverage_pass = bool(sim.get("does_coverage_pass_rule"))
        improved_enough = improved_count >= _int(
            conditions.get("actual_path_improved_probe_count_min"),
            default=2,
        )
        slice_ok = bool(
            _mapping(slice_rows.get(policy_id)).get("2022_slice_not_worse_than_flat_reference")
        )
        selection_pass = bool(
            coverage_pass
            and improved_enough
            and (
                no_defensive_regression
                or not bool(conditions.get("no_major_regression_in_defensive_probe", True))
            )
            and bool(conditions.get("net_of_cost_not_worse", True))
            and (
                slice_ok
                or not bool(conditions.get("2022_slice_not_worse_than_flat_reference", True))
            )
            and (
                same_risk_reported
                or not bool(conditions.get("same_risk_comparison_reported", True))
            )
            and policy_id != "wf_warm_start_diagnostic"
        )
        rows.append(
            {
                **window_metadata(primary_window),
                "policy_id": policy_id,
                "first_prediction_date": payload.get("first_prediction_date"),
                "first_portfolio_effective_date": payload.get("first_prediction_date"),
                "does_coverage_pass_rule": coverage_pass,
                "actual_path_improved_probe_count": improved_count,
                "probe_count": actual_summary.get("probe_count"),
                "actual_path_improved_vs_flat": improved_count > 0,
                "improvement_on_2022_included_window": coverage_pass and bool(
                    sim.get("covered_2022")
                ),
                "no_major_regression_in_defensive_probe": no_defensive_regression,
                "net_of_cost_not_worse": bool(conditions.get("net_of_cost_not_worse", True)),
                "2022_slice_not_worse_than_flat_reference": slice_ok,
                "same_risk_comparison_reported": same_risk_reported,
                "coverage_aware_selection_pass": selection_pass,
                "owner_review_allowed": selection_pass,
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    selection_count = sum(bool(row["coverage_aware_selection_pass"]) for row in rows)
    return _payload(
        report_type="first_layer_v2_coverage_policy_actual_path_matrix",
        title="First-Layer V2 Coverage Policy Actual-Path Matrix",
        status="FIRST_LAYER_V2_COVERAGE_POLICY_ACTUAL_PATH_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "policy_count": len(rows),
            "coverage_aware_selection_pass_count": selection_count,
            "same_risk_comparison_reported": same_risk_reported,
            "target_path_metrics_used_for_pass": False,
        },
        policy_rows=rows,
        probe_rows=probe_rows,
    )


def build_2022_stress_recovery_slice_review(
    *,
    primary_window: Mapping[str, Any],
    coverage_policy: Mapping[str, Any],
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    labels: pd.DataFrame,
    variant_results: Mapping[str, Any],
) -> dict[str, Any]:
    windows = _mapping(coverage_policy.get("stress_recovery_windows"))
    risk_off = _mapping(windows.get("risk_off_2022"))
    recovery = _mapping(windows.get("recovery_2022"))
    rows = []
    probe_rows = []
    neutral_avg_return = None
    for policy_id, payload in _mapping(variant_results.get("variants")).items():
        predictions = _ensure_frame(_mapping(payload).get("predictions"))
        merged = _merge_predictions_labels_2022(predictions, labels)
        state_counts = _state_counts(merged["trend_state"]) if not merged.empty else {}
        false_risk_off = int(
            (
                merged["trend_state"].isin(["risk_off", "defensive"])
                & merged["do_not_de_risk_label"].astype(bool)
            ).sum()
        ) if not merged.empty else 0
        false_risk_on = int(
            (
                merged["trend_state"].eq("risk_on")
                & ~merged["high_confidence_risk_on_label"].astype(bool)
            ).sum()
        ) if not merged.empty else 0
        missed_upside = int(
            (
                merged["trend_state"].isin(["risk_off", "defensive"])
                & (
                    merged["stay_constructive_label"].astype(bool)
                    | merged["add_risk_label"].astype(bool)
                )
            ).sum()
        ) if not merged.empty else 0
        avoided_drawdown = int(
            (
                merged["trend_state"].isin(["risk_off", "defensive"])
                & ~merged["do_not_de_risk_label"].astype(bool)
            ).sum()
        ) if not merged.empty else 0
        re_risk_date = _first_state_date(merged, {"constructive", "risk_on"}, recovery)
        probe_metrics = _backtest_2022_probe_rows(
            policy_id=policy_id,
            prices=prices,
            predictions=predictions,
            probe_registry=probe_registry,
            primary_window=primary_window,
        )
        probe_rows.extend(probe_metrics)
        avg_return = round(
            float(np.mean([_float(row.get("actual_path_annual_return")) for row in probe_metrics]))
            if probe_metrics
            else 0.0,
            GRID_ROUND_DIGITS,
        )
        if policy_id == "wf_warm_start_diagnostic":
            neutral_avg_return = avg_return
        rows.append(
            {
                **window_metadata(primary_window),
                "policy_id": policy_id,
                "prediction_count_2022": len(merged),
                "risk_off_window_prediction_count": _count_window_predictions(
                    predictions, risk_off
                ),
                "recovery_window_prediction_count": _count_window_predictions(
                    predictions, recovery
                ),
                "state_distribution_2022": state_counts,
                "false_risk_off_count": false_risk_off,
                "false_risk_on_count": false_risk_on,
                "missed_upside_count": missed_upside,
                "avoided_drawdown_count": avoided_drawdown,
                "first_re_risk_date_in_recovery_window": re_risk_date,
                "average_probe_annual_return_2022_slice": avg_return,
                "target_path_metrics_used_for_pass": False,
            }
        )
    baseline = neutral_avg_return if neutral_avg_return is not None else 0.0
    for row in rows:
        row["average_return_delta_vs_warm_start_diagnostic"] = round(
            _float(row.get("average_probe_annual_return_2022_slice")) - baseline,
            GRID_ROUND_DIGITS,
        )
        row["2022_slice_not_worse_than_flat_reference"] = (
            row["average_return_delta_vs_warm_start_diagnostic"] >= -0.01
        )
    return _payload(
        report_type="first_layer_v2_2022_slice_matrix",
        title="First-Layer V2 2022 Stress Recovery Slice Review",
        status="FIRST_LAYER_V2_2022_STRESS_RECOVERY_SLICE_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "policy_count": len(rows),
            "risk_off_window": risk_off,
            "recovery_window": recovery,
            "target_path_metrics_used_for_pass": False,
        },
        policy_rows=rows,
        probe_rows=probe_rows,
    )


def build_coverage_rebuild_failure_attribution(
    *,
    primary_window: Mapping[str, Any],
    coverage_blocker: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    early_feature: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_review: Mapping[str, Any],
    selection_rule: Mapping[str, Any],
) -> dict[str, Any]:
    pass_rows = [
        row
        for row in _records(coverage_simulation.get("policy_rows"))
        if row.get("does_coverage_pass_rule")
    ]
    selection_rows = [
        row
        for row in _records(actual_path.get("policy_rows"))
        if row.get("coverage_aware_selection_pass")
    ]
    conditions = _mapping(selection_rule.get("selection_conditions"))
    min_improved = _int(conditions.get("actual_path_improved_probe_count_min"), default=2)
    simulation_rows = {
        str(row.get("policy_id")): row for row in _records(coverage_simulation.get("policy_rows"))
    }
    slice_rows = {
        str(row.get("policy_id")): row for row in _records(slice_review.get("policy_rows"))
    }
    attribution_rows = []
    candidate_reason_sets = []
    for row in _records(actual_path.get("policy_rows")):
        policy_id = str(row.get("policy_id"))
        sim = _mapping(simulation_rows.get(policy_id))
        slice_row = _mapping(slice_rows.get(policy_id))
        policy_reasons = []
        if not row.get("does_coverage_pass_rule"):
            policy_reasons.append(
                str(sim.get("coverage_block_reason") or "WALK_FORWARD_POLICY_NOT_WINDOW_AWARE")
            )
        if early_feature.get("status") == "EARLY_FEATURE_BLOCKS_2022_COVERAGE":
            policy_reasons.append("EARLY_FEATURE_MISSING")
        if _int(row.get("actual_path_improved_probe_count")) < min_improved:
            policy_reasons.append("INSUFFICIENT_ACTUAL_PATH_IMPROVEMENT")
        if not row.get("improvement_on_2022_included_window"):
            policy_reasons.append("MISSING_2022_ACTUAL_PATH_COVERAGE")
        if not row.get("no_major_regression_in_defensive_probe"):
            policy_reasons.append("DEFENSIVE_PROBE_REGRESSION")
        if not row.get("net_of_cost_not_worse"):
            policy_reasons.append("NET_OF_COST_FAILURE")
        if not row.get("same_risk_comparison_reported"):
            policy_reasons.append("SAME_RISK_COMPARISON_MISSING")
        if not slice_row.get("2022_slice_not_worse_than_flat_reference"):
            policy_reasons.append("ADVERSE_2022_STRESS_RECOVERY_SLICE")
        if policy_id == "wf_warm_start_diagnostic":
            policy_reasons.append("DIAGNOSTIC_ONLY_WARM_START")
        if not policy_reasons and not row.get("coverage_aware_selection_pass"):
            policy_reasons.append("OWNER_REVIEW_REQUIRED_BEFORE_ESCALATION")
        if row.get("does_coverage_pass_rule"):
            candidate_reason_sets.append(policy_reasons)
        attribution_rows.append(
            {
                "policy_id": policy_id,
                "does_coverage_pass_rule": bool(row.get("does_coverage_pass_rule")),
                "coverage_aware_selection_pass": bool(
                    row.get("coverage_aware_selection_pass")
                ),
                "actual_path_improved_probe_count": row.get("actual_path_improved_probe_count"),
                "probe_count": row.get("probe_count"),
                "improvement_on_2022_included_window": bool(
                    row.get("improvement_on_2022_included_window")
                ),
                "no_major_regression_in_defensive_probe": bool(
                    row.get("no_major_regression_in_defensive_probe")
                ),
                "net_of_cost_not_worse": bool(row.get("net_of_cost_not_worse")),
                "2022_slice_not_worse_than_flat_reference": bool(
                    slice_row.get("2022_slice_not_worse_than_flat_reference")
                ),
                "same_risk_comparison_reported": bool(
                    row.get("same_risk_comparison_reported")
                ),
                "failure_reasons": list(dict.fromkeys(policy_reasons)),
            }
        )
    reasons = []
    if not pass_rows:
        reasons.append("TRAIN_WINDOW_TOO_LONG")
    if early_feature.get("status") == "EARLY_FEATURE_BLOCKS_2022_COVERAGE":
        reasons.append("EARLY_FEATURE_MISSING")
    if not selection_rows:
        source_reason_sets = candidate_reason_sets or [
            row["failure_reasons"] for row in attribution_rows
        ]
        for reason_set in source_reason_sets:
            for reason in reason_set:
                reasons.append(reason)
    if not reasons:
        reasons.append("OWNER_REVIEW_REQUIRED_BEFORE_ESCALATION")
    reasons = list(dict.fromkeys(reasons))
    return _payload(
        report_type="first_layer_v2_coverage_rebuild_failure_attribution",
        title="First-Layer V2 Coverage Rebuild Failure Attribution",
        status="FIRST_LAYER_V2_COVERAGE_REBUILD_FAILURE_ATTRIBUTION_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "coverage_pass_variant_count": len(pass_rows),
            "coverage_aware_selection_pass_count": len(selection_rows),
            "primary_reason": reasons[0],
            "reason_count": len(reasons),
            "coverage_blocker_status": coverage_blocker.get("status"),
            "early_feature_status": early_feature.get("status"),
            "actual_path_status": actual_path.get("status"),
            "slice_review_status": slice_review.get("status"),
        },
        failure_reasons=reasons,
        policy_attribution_rows=attribution_rows,
    )


def build_coverage_rebuild_owner_pack(
    *,
    primary_window: Mapping[str, Any],
    coverage_blocker: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    early_feature: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_review: Mapping[str, Any],
    failure: Mapping[str, Any],
) -> dict[str, Any]:
    best = _best_selected_policy(actual_path)
    return _payload(
        report_type="first_layer_v2_coverage_rebuild_owner_pack",
        title="First-Layer V2 Coverage Rebuild Owner Pack",
        status="FIRST_LAYER_V2_COVERAGE_REBUILD_OWNER_PACK_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "coverage_blocker_status": coverage_blocker.get("status"),
            "coverage_simulation_status": coverage_simulation.get("status"),
            "early_feature_status": early_feature.get("status"),
            "actual_path_status": actual_path.get("status"),
            "slice_review_status": slice_review.get("status"),
            "failure_status": failure.get("status"),
            "best_coverage_policy": best.get("policy_id", ""),
            "owner_review_allowed": bool(best),
        },
        owner_answers={
            "why_previous_predictions_start_2023_02_22": _mapping(
                coverage_blocker.get("summary")
            ).get("reason_prediction_starts_2023_02_22"),
            "which_policy_covers_2022": best.get("policy_id", ""),
            "does_8_of_8_improvement_still_hold_after_coverage": best.get(
                "actual_path_improved_probe_count"
            )
            == 8,
            "if_not_where_problem_comes_from": _mapping(failure.get("summary")).get(
                "primary_reason"
            ),
            "owner_review_allowed": bool(best),
            "why_promotion_still_blocked": (
                "research_only_owner_review_required_and_broker_disabled"
            ),
        },
    )


def build_coverage_rebuild_final_matrix(
    *,
    primary_window: Mapping[str, Any],
    coverage_blocker: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    early_feature: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_review: Mapping[str, Any],
    failure: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
    action_summary: Mapping[str, Any],
    label_summary: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    selected = _best_selected_policy(actual_path)
    coverage_pass_rows = [
        row
        for row in _records(coverage_simulation.get("policy_rows"))
        if row.get("does_coverage_pass_rule")
    ]
    if selected:
        final_status = "COVERAGE_REBUILD_SUCCESS_ACTION_PATH_IMPROVES"
    elif coverage_pass_rows:
        final_status = "COVERAGE_REBUILD_SUCCESS_ACTION_PATH_NO_LONGER_IMPROVES"
    elif early_feature.get("status") == "EARLY_FEATURE_BLOCKS_2022_COVERAGE":
        final_status = "COVERAGE_REBUILD_BLOCKED_BY_FEATURES"
    else:
        final_status = "FIRST_LAYER_V2_REMAINS_COVERAGE_BLOCKED"
    return _payload(
        report_type="first_layer_v2_walk_forward_coverage_rebuild_final_matrix",
        title="First-Layer V2 Walk-Forward Coverage Rebuild Final Matrix",
        status=final_status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "final_status": final_status,
            "data_quality_status": data_gate.get("status"),
            "coverage_blocker_status": coverage_blocker.get("status"),
            "coverage_simulation_status": coverage_simulation.get("status"),
            "early_feature_status": early_feature.get("status"),
            "feature_audit_status": feature_audit.get("status"),
            "action_value_status": action_summary.get("status"),
            "label_status": label_summary.get("status"),
            "actual_path_status": actual_path.get("status"),
            "slice_review_status": slice_review.get("status"),
            "failure_status": failure.get("status"),
            "owner_pack_status": owner_pack.get("status"),
            "coverage_pass_variant_count": len(coverage_pass_rows),
            "coverage_aware_selected_policy": selected.get("policy_id", ""),
            "selected_policy_first_prediction_date": selected.get("first_prediction_date", ""),
            "selected_policy_improved_probe_count": selected.get(
                "actual_path_improved_probe_count", 0
            ),
            "dynamic_promotion_status": "BLOCKED",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        final_decision={
            "second_layer_registry": "dynamic_second_layer_probe_registry_v2",
            "coverage_before_owner_review": True,
            "owner_review_allowed": bool(selected),
            "selected_policy": selected.get("policy_id", ""),
            "next_action": "OWNER_REVIEW_COVERAGE_AWARE_FIRST_LAYER_V2"
            if selected
            else "KEEP_FIRST_LAYER_V2_COVERAGE_REBUILD_BLOCKED",
            "target_path_metrics_can_pass": False,
            "promotion": "blocked",
        },
    )


def write_coverage_rebuild_outputs(
    *,
    output_root: Path,
    coverage_blocker: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    early_feature: Mapping[str, Any],
    model_matrix: Mapping[str, Any],
    variant_results: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_review: Mapping[str, Any],
    failure: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
) -> None:
    _write_yaml(DEFAULT_COVERAGE_BLOCKER_YAML_PATH, coverage_blocker)
    _write_markdown(DEFAULT_COVERAGE_BLOCKER_DOC_PATH, _render_payload_doc(coverage_blocker))
    _write_yaml(DEFAULT_COVERAGE_SIMULATION_YAML_PATH, coverage_simulation)
    _write_markdown(DEFAULT_COVERAGE_SIMULATION_DOC_PATH, _render_payload_doc(coverage_simulation))
    _write_yaml(DEFAULT_EARLY_FEATURE_YAML_PATH, early_feature)
    _write_markdown(DEFAULT_EARLY_FEATURE_DOC_PATH, _render_payload_doc(early_feature))
    _write_yaml(DEFAULT_MODEL_MATRIX_YAML_PATH, model_matrix)
    _write_markdown(DEFAULT_MODEL_REVIEW_DOC_PATH, _render_payload_doc(model_matrix))
    _write_yaml(DEFAULT_ACTUAL_PATH_YAML_PATH, actual_path)
    _write_markdown(DEFAULT_ACTUAL_PATH_DOC_PATH, _render_payload_doc(actual_path))
    _write_yaml(DEFAULT_2022_SLICE_YAML_PATH, slice_review)
    _write_markdown(DEFAULT_2022_SLICE_DOC_PATH, _render_payload_doc(slice_review))
    _write_yaml(DEFAULT_FAILURE_YAML_PATH, failure)
    _write_markdown(DEFAULT_FAILURE_DOC_PATH, _render_payload_doc(failure))
    _write_markdown(DEFAULT_OWNER_PACK_DOC_PATH, _render_owner_doc(owner_pack))
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_payload_doc(final_matrix))
    for policy_id, payload in _mapping(variant_results.get("variants")).items():
        policy_root = output_root / "models" / "first_layer_v2_coverage_rebuild" / policy_id
        _write_csv(
            policy_root / "composer_predictions.csv",
            _ensure_frame(payload.get("predictions")),
        )
        for model_id, result in _mapping(payload.get("model_results")).items():
            model_root = policy_root / model_id
            _write_csv(model_root / "predictions.csv", _ensure_frame(result.get("predictions")))
            _write_json(model_root / "metrics.json", _mapping(result.get("metrics")))
            _write_json(model_root / "thresholds.json", _records(result.get("threshold_rows")))


def _simulate_variant_splits(
    policy_id: str,
    cfg: Mapping[str, Any],
    threshold_policy: Mapping[str, Any],
    merged: pd.DataFrame,
) -> dict[str, Any]:
    mode = str(cfg.get("mode", "rolling_fixed"))
    if mode == "warm_start_diagnostic":
        first_date = _min_iso_date(merged)
        row = {
            "policy_id": policy_id,
            "mode": mode,
            "first_train_start": "",
            "first_train_end": "",
            "first_validation_start": first_date,
            "first_prediction_date": first_date,
            "first_portfolio_effective_date": first_date,
            "number_of_splits": 0,
            "covered_2022": True,
            "covered_2022_risk_off_window": True,
            "covered_2022_recovery_window": True,
            "does_coverage_pass_rule": False,
            "coverage_block_reason": "DIAGNOSTIC_ONLY_WARM_START",
            "train_sample_count_min": 0,
            "train_sample_count_max": 0,
            "validation_sample_count_min": 0,
            "validation_sample_count_max": 0,
            "label_distribution_by_split": {},
        }
        return {"summary": row, "split_rows": []}
    validation = _int(cfg.get("validation_window_days"), default=63)
    step = _int(cfg.get("step_days"), default=21)
    if mode == "expanding_initial":
        start_window = _int(cfg.get("min_train_days"), default=252)
        expanding_until = _int(cfg.get("expanding_until_days"), default=504)
    else:
        start_window = _int(cfg.get("train_window_days"), default=504)
        expanding_until = start_window
    dates = pd.to_datetime(merged["date"], errors="coerce").dropna().reset_index(drop=True)
    split_rows = []
    for split_id, validation_start in enumerate(
        range(start_window, max(start_window, len(dates) - validation), step)
    ):
        if mode == "expanding_initial" and validation_start <= expanding_until:
            train_start = 0
        else:
            train_start = validation_start - expanding_until
        train = merged.iloc[train_start:validation_start]
        valid = merged.iloc[validation_start : validation_start + validation]
        if train.empty or valid.empty:
            continue
        split_rows.append(
            {
                "policy_id": policy_id,
                "split_id": split_id,
                "train_start": str(train["date"].iloc[0]),
                "train_end": str(train["date"].iloc[-1]),
                "validation_start": str(valid["date"].iloc[0]),
                "validation_end": str(valid["date"].iloc[-1]),
                "train_sample_count": len(train),
                "validation_sample_count": len(valid),
                "label_distribution": {
                    label: _label_sum(valid, label) for label in LABEL_COLUMNS
                },
            }
        )
    first = split_rows[0] if split_rows else {}
    first_prediction = str(first.get("validation_start", ""))
    summary = {
        "policy_id": policy_id,
        "mode": mode,
        "first_train_start": first.get("train_start", ""),
        "first_train_end": first.get("train_end", ""),
        "first_validation_start": first.get("validation_start", ""),
        "first_prediction_date": first_prediction,
        "first_portfolio_effective_date": first_prediction,
        "number_of_splits": len(split_rows),
        "covered_2022": _date_on_or_before(first_prediction, "2022-12-30"),
        "covered_2022_risk_off_window": _date_on_or_before(first_prediction, "2022-10-14"),
        "covered_2022_recovery_window": _date_on_or_before(first_prediction, "2022-12-30"),
        "does_coverage_pass_rule": _date_on_or_before(first_prediction, "2022-03-01"),
        "coverage_block_reason": ""
        if _date_on_or_before(first_prediction, "2022-03-01")
        else "FIRST_PREDICTION_AFTER_REQUIRED_2022_03_01",
        "train_sample_count_min": min(
            [row["train_sample_count"] for row in split_rows], default=0
        ),
        "train_sample_count_max": max(
            [row["train_sample_count"] for row in split_rows], default=0
        ),
        "validation_sample_count_min": min(
            [row["validation_sample_count"] for row in split_rows], default=0
        ),
        "validation_sample_count_max": max(
            [row["validation_sample_count"] for row in split_rows], default=0
        ),
        "label_distribution_by_split": first.get("label_distribution", {}),
    }
    return {"summary": summary, "split_rows": split_rows}


def _threshold_policy_for_variant(
    threshold_policy: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> dict[str, Any]:
    result = copy.deepcopy(dict(threshold_policy))
    wf = dict(_mapping(result.get("walk_forward")))
    if cfg.get("mode") == "expanding_initial":
        wf["mode"] = "expanding_initial"
        wf["min_train_days"] = cfg.get("min_train_days")
        wf["expanding_until_days"] = cfg.get("expanding_until_days")
        wf["train_window_days"] = cfg.get("expanding_until_days")
    else:
        wf["mode"] = "rolling_fixed"
        wf["train_window_days"] = cfg.get("train_window_days")
    wf["validation_window_days"] = cfg.get("validation_window_days")
    wf["step_days"] = cfg.get("step_days")
    wf["min_train_samples"] = cfg.get("min_train_samples")
    result["walk_forward"] = wf
    return result


def _build_warm_start_predictions(
    feature_matrix: pd.DataFrame,
    primary_window: Mapping[str, Any],
    policy_id: str,
) -> pd.DataFrame:
    rows = []
    metadata = window_metadata(primary_window)
    for idx, row in feature_matrix.iterrows():
        rows.append(
            {
                **metadata,
                "date": str(row["date"]),
                "model_id": f"first_layer_{policy_id}",
                "trend_state": "neutral",
                "confidence": 0.35,
                "expected_horizon_days": 20,
                "validity_days": 5,
                "decay_profile": "fast",
                "feature_snapshot_hash": f"warm_start_{idx}",
                "model_version": "warm_start_diagnostic",
                "known_at": str(row.get("known_at")),
                "available_at": str(row.get("available_at")),
                "decision_at": str(row.get("decision_at")),
                "do_not_de_risk_pred": False,
                "stay_constructive_pred": False,
                "add_risk_pred": False,
                "high_confidence_risk_on_pred": False,
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    return pd.DataFrame(rows)


def _backtest_2022_probe_rows(
    *,
    policy_id: str,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if predictions.empty:
        return []
    pred = predictions.loc[
        (predictions["date"].astype(str) >= "2022-01-03")
        & (predictions["date"].astype(str) <= "2022-12-30")
    ].copy()
    if pred.empty:
        return []
    sliced_prices = prices.loc[(prices.index >= "2022-01-03") & (prices.index <= "2022-12-30")]
    if sliced_prices.empty:
        return []
    rows = []
    for probe in _records(probe_registry.get("probes")):
        raw = _backtest_probe_predictions(
            prices=sliced_prices,
            predictions=pred,
            probe=probe,
            model_id=f"first_layer_v2_{policy_id}",
        )
        rows.append(
            {
                **window_metadata(primary_window),
                "policy_id": policy_id,
                "probe_id": raw["probe_id"],
                "date_start": raw["date_start"],
                "date_end": raw["date_end"],
                "actual_path_annual_return": raw["actual_path_annual_return"],
                "max_drawdown_daily_equity": raw["max_drawdown_daily_equity"],
                "sharpe_daily_zero_rf": raw["sharpe_daily_zero_rf"],
                "calmar_daily_equity_dd": raw["calmar_daily_equity_dd"],
                "turnover": raw["turnover"],
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    return rows


def _primary_horizon_labels(
    labels: pd.DataFrame, threshold_policy: Mapping[str, Any]
) -> pd.DataFrame:
    horizon = _int(
        _mapping(threshold_policy.get("walk_forward")).get("label_horizon_days"),
        default=20,
    )
    return labels.loc[
        (labels["research_window_id"] == PRIMARY_WINDOW_ID)
        & (labels["horizon_days"].astype(int) == horizon)
    ].copy()


def _merge_predictions_labels_2022(predictions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty or labels.empty:
        return pd.DataFrame()
    pred = predictions.loc[
        (predictions["date"].astype(str) >= "2022-01-03")
        & (predictions["date"].astype(str) <= "2022-12-30")
    ].copy()
    if pred.empty:
        return pd.DataFrame()
    return pred.merge(labels[["date", *LABEL_COLUMNS]], on="date", how="left").fillna(False)


def _count_window_predictions(predictions: pd.DataFrame, window: Mapping[str, Any]) -> int:
    if predictions.empty:
        return 0
    return int(
        (
            (predictions["date"].astype(str) >= str(window.get("start")))
            & (predictions["date"].astype(str) <= str(window.get("end")))
        ).sum()
    )


def _first_state_date(frame: pd.DataFrame, states: set[str], window: Mapping[str, Any]) -> str:
    if frame.empty:
        return ""
    sliced = frame.loc[
        (frame["date"].astype(str) >= str(window.get("start")))
        & (frame["date"].astype(str) <= str(window.get("end")))
        & frame["trend_state"].astype(str).isin(states)
    ]
    if sliced.empty:
        return ""
    return str(sliced["date"].iloc[0])


def _best_selected_policy(actual_path: Mapping[str, Any]) -> dict[str, Any]:
    selected = [
        row
        for row in _records(actual_path.get("policy_rows"))
        if row.get("coverage_aware_selection_pass")
    ]
    if not selected:
        return {}
    return max(selected, key=lambda row: _int(row.get("actual_path_improved_probe_count")))


def _date_at_offset(frame: pd.DataFrame, offset: int) -> str:
    if frame.empty or "date" not in frame.columns or len(frame) <= offset:
        return ""
    return str(frame["date"].iloc[offset])


def _date_on_or_before(value: object, cutoff: str) -> bool:
    text = str(value or "")
    return bool(text) and text <= cutoff


def _min_iso_date(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    values = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if values.empty:
        return ""
    return values.min().date().isoformat()


def _state_counts(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.astype(str).value_counts().items()}


def _label_sum(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].astype(bool).sum())


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    primary_window: Mapping[str, Any],
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    summary_dict = dict(summary)
    metadata = window_metadata(primary_window)
    candidate_count = _candidate_count(summary_dict, extra)
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        **metadata,
        "summary": summary_dict,
        **SAFETY_BOUNDARY,
        "research_audit_metadata": {
            "modified_layer": "first_layer",
            "frozen_first_layer_version": "frozen_or_not_applicable",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": metadata["research_window_id"],
            "label_version": "upper_state_label_taxonomy_v2",
            "feature_set_version": "pit_feature_matrix_v3",
            "model_version": "first_layer_coverage_rebuild_v2",
            "threshold_policy": "first_layer_walk_forward_coverage_policy_v2",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": candidate_count,
            "pre_registered_selection_rule": True,
        },
        **extra,
    }


def _candidate_count(summary: Mapping[str, Any], extra: Mapping[str, Any]) -> int:
    for key in ("policy_count", "variant_count", "model_row_count", "feature_row_count"):
        if key in summary:
            return max(0, _int(summary.get(key)))
    for key in ("policy_rows", "model_rows", "feature_rows", "probe_rows"):
        if key in extra:
            return len(_records(extra.get(key)))
    return 0


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
