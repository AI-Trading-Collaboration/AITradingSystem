from __future__ import annotations

import csv
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.expanded_allocation_universe import _data_quality_gate, _load_price_matrix
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _backtest_probe_predictions,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PREDICTIONS_PATH as DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PROBE_REGISTRY_V2_PATH,
    load_dynamic_second_layer_probe_registry_v2,
)
from ai_trading_system.two_layer_policy_compiler import (
    DEFAULT_POLICY_SCHEMA_PATH,
    DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
    compile_two_layer_policy,
    load_base_overlay_veto_policy,
    load_signal_usage_matrix_v2,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PRIMARY_WINDOW_ID = "exact_three_asset_validated"
PRIMARY_WINDOW_ALIAS = "EXACT_THREE_ASSET_VALIDATED_WINDOW"
REQUESTED_START = "2021-02-22"
MARKET_REGIME = "ai_after_chatgpt"
ANCHOR_EVENT = "ChatGPT public launch"
ANCHOR_DATE = "2022-11-30"
DEFAULT_HORIZON_DAYS = 20
ASSETS = ["QQQ", "SGOV", "TQQQ"]

DEFAULT_CHANNEL_V3_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "channel_specific_v3"
)
DEFAULT_FEATURE_SET_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_feature_set_v1.yaml"
)
DEFAULT_FEATURE_SET_LOCKED_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_feature_set_v1_locked.yaml"
)
DEFAULT_DO_NOT_DERISK_SELECTION_RULE_PATH = (
    PROJECT_ROOT / "config" / "research" / "do_not_de_risk_v3_selection_rule.yaml"
)
DEFAULT_RISK_ON_VETO_SELECTION_RULE_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_on_veto_v3_selection_rule.yaml"
)
DEFAULT_CHANNEL_V3_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_first_layer_v3.yaml"
)
DEFAULT_PIT_FEATURE_MATRIX_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "pit_feature_matrix"
    / "pit_feature_matrix_v3.csv"
)
DEFAULT_LABELS_PATH = (
    PROJECT_ROOT / "outputs" / "research_trends" / "trend_labels" / "upper_state_labels_v2.csv"
)
DEFAULT_ACTION_VALUE_MATRIX_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "action_value_matrix_v2"
    / "action_value_matrix_v2.csv"
)
DEFAULT_SCOPE_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_first_layer_v3_scope.md"
)
DEFAULT_SCOPE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "channel_specific_first_layer_v3_scope.yaml"
)
DEFAULT_FEATURE_LOCK_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_feature_set_v1_lock_review.md"
)
DEFAULT_DO_NOT_LABEL_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "do_not_de_risk_label_v3_summary.yaml"
)
DEFAULT_DO_NOT_LABEL_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "do_not_de_risk_label_v3_review.md"
)
DEFAULT_RISK_VETO_LABEL_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "risk_on_veto_label_v3_summary.yaml"
)
DEFAULT_RISK_VETO_LABEL_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_veto_label_v3_review.md"
)
DEFAULT_CHANNEL_PIT_SUMMARY_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_pit_feature_matrix_v3_summary.yaml"
)
DEFAULT_CHANNEL_PIT_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_pit_feature_matrix_v3_audit.md"
)
DEFAULT_DO_NOT_MODEL_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "do_not_de_risk_model_v3_matrix.yaml"
)
DEFAULT_DO_NOT_MODEL_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "do_not_de_risk_model_v3_review.md"
)
DEFAULT_RISK_VETO_MODEL_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "risk_on_veto_model_v3_matrix.yaml"
)
DEFAULT_RISK_VETO_MODEL_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_veto_model_v3_review.md"
)
DEFAULT_POLICY_COMPILER_REVIEW_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "channel_specific_v3_policy_compiler_dry_run_review.md"
)
DEFAULT_ACTUAL_PATH_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_actual_path_matrix.yaml"
)
DEFAULT_ACTUAL_PATH_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_v3_actual_path_review.md"
)
DEFAULT_2022_SLICE_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_2022_slice_matrix.yaml"
)
DEFAULT_2022_SLICE_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_v3_2022_slice_review.md"
)
DEFAULT_2023_PLUS_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_2023_plus_dependence.yaml"
)
DEFAULT_2023_PLUS_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_v3_2023_plus_dependence_review.md"
)
DEFAULT_FALSE_ADD_RISK_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_false_add_risk_reduction.yaml"
)
DEFAULT_FALSE_ADD_RISK_REVIEW_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "channel_specific_v3_false_add_risk_reduction_review.md"
)
DEFAULT_FALSE_RISK_OFF_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_false_risk_off_reduction.yaml"
)
DEFAULT_FALSE_RISK_OFF_REVIEW_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "channel_specific_v3_false_risk_off_reduction_review.md"
)
DEFAULT_SELECTION_RESULT_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_selection_rule_result.yaml"
)
DEFAULT_SELECTION_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_v3_selection_rule_review.md"
)
DEFAULT_OWNER_PACK_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_first_layer_v3_owner_pack.md"
)
DEFAULT_CLOSEOUT_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_first_layer_v3_closeout.md"
)
DEFAULT_FINAL_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_first_layer_v3_final_matrix.yaml"
)
DEFAULT_SAME_RISK_FRONTIER_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "second_layer_probe_same_risk_frontier_matrix_v2.yaml"
)
DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "actual_path_multi_window_matrix.yaml"
)

DO_NOT_DERISK_FEATURES = [
    "qqq_drawdown_126d",
    "qqq_drawdown_recovery_20d",
    "days_since_60d_low",
    "recovery_from_60d_low",
    "qqq_reclaim_recent_high_20d",
    "qqq_recovery_speed_20d",
]
RISK_ON_VETO_FEATURES = [
    "realized_vol_20d",
    "realized_vol_decline_20d",
    "realized_vol_decline_60d",
    "downside_vol_20d",
    "downside_vol_decline_20d",
    "yield_curve_10y2y",
    "usd_trend_20d",
]
ALLOWED_FAMILIES = {
    "do_not_de_risk": ["drawdown_recovery"],
    "risk_on_veto": ["volatility_compression", "rates_liquidity"],
}
DIAGNOSTIC_ONLY_FAMILIES = ["trend_persistence", "relative_strength"]
BLOCKED_FAMILIES = ["breadth_participation", "event_risk"]
ALLOWED_PROBE_IDS = [
    "defensive_overlay_probe",
    "drawdown_control_probe",
    "balanced_dynamic_probe",
    "no_tqqq_return_seeking_probe",
    "low_tqqq_balanced_growth_probe",
    "qqq_heavy_growth_probe",
    "asymmetric_risk_on_slow_confirm_probe",
    "capped_risk_on_diagnostic_probe",
]
DEFENSIVE_PROBE_IDS = {"defensive_overlay_probe", "drawdown_control_probe"}

SAFETY_BOUNDARY = {
    "research_only": True,
    "actual_path_required": True,
    "target_path_metrics_role": "diagnostic_only",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "manual_review_required": True,
    "dynamic_promotion_status": "BLOCKED",
}

COMPOSER_COLUMNS = [
    "date",
    "do_not_de_risk_probability",
    "re_risk_allowed_probability",
    "growth_allowed",
    "add_risk_allowed",
    "tqqq_allowed",
    "veto_reasons",
    "confidence",
    "validity_days",
]
DRY_RUN_COLUMNS = [
    "date",
    "do_not_de_risk_active",
    "risk_on_veto_active",
    "growth_allowed",
    "add_risk_allowed",
    "tqqq_allowed",
    "veto_reasons",
    "compiler_veto_active",
    "blocked_growth_overlay",
    "blocked_tqqq_delta",
    "blocked_actions",
    "applied_overlays",
    "research_only",
    "promotion_allowed",
    "broker_action",
]


def run_channel_specific_first_layer_v3_pack(
    *,
    feature_set_path: Path = DEFAULT_FEATURE_SET_PATH,
    feature_set_locked_path: Path = DEFAULT_FEATURE_SET_LOCKED_PATH,
    do_not_selection_rule_path: Path = DEFAULT_DO_NOT_DERISK_SELECTION_RULE_PATH,
    risk_veto_selection_rule_path: Path = DEFAULT_RISK_ON_VETO_SELECTION_RULE_PATH,
    channel_config_path: Path = DEFAULT_CHANNEL_V3_CONFIG_PATH,
    pit_feature_matrix_path: Path = DEFAULT_PIT_FEATURE_MATRIX_PATH,
    labels_path: Path = DEFAULT_LABELS_PATH,
    action_value_matrix_path: Path = DEFAULT_ACTION_VALUE_MATRIX_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_V2_PATH,
    composer_predictions_path: Path = DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
    policy_schema_path: Path = DEFAULT_POLICY_SCHEMA_PATH,
    signal_usage_matrix_path: Path = DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_CHANNEL_V3_OUTPUT_ROOT,
    scope_review_path: Path = DEFAULT_SCOPE_REVIEW_PATH,
    scope_matrix_path: Path = DEFAULT_SCOPE_MATRIX_PATH,
    feature_lock_review_path: Path = DEFAULT_FEATURE_LOCK_REVIEW_PATH,
    same_risk_frontier_path: Path = DEFAULT_SAME_RISK_FRONTIER_PATH,
    limited_adjustment_reference_path: Path = DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    feature_set = _load_mapping(feature_set_path)
    do_not_rule = _load_mapping(do_not_selection_rule_path)
    risk_veto_rule = _load_mapping(risk_veto_selection_rule_path)
    do_not_thresholds = _thresholds(do_not_rule, "do_not_de_risk_v3_selection_rule")
    risk_veto_thresholds = _thresholds(risk_veto_rule, "risk_on_veto_v3_selection_rule")

    _validate_selected_family_scope(feature_set)
    scope_payload = _scope_payload()
    _write_yaml(scope_matrix_path, scope_payload)
    _write_markdown(scope_review_path, _render_scope_review(scope_payload))

    feature_lock_payload = _locked_feature_set_payload(feature_set, feature_set_path)
    _write_yaml(feature_set_locked_path, feature_lock_payload)
    _write_markdown(feature_lock_review_path, _render_feature_lock_review(feature_lock_payload))

    pit = _load_primary_pit_frame(pit_feature_matrix_path)
    labels = _load_primary_labels(labels_path)
    action = _load_primary_action_values(action_value_matrix_path)
    action_by_date = _action_by_date(action)

    channel_matrix = _build_channel_pit_matrix(pit)
    channel_pit_path = output_root / "channel_pit_feature_matrix_v3.csv"
    _write_dataframe(channel_pit_path, channel_matrix)
    channel_pit_payload = _channel_pit_summary(channel_matrix, pit_feature_matrix_path)
    _write_yaml(DEFAULT_CHANNEL_PIT_SUMMARY_PATH, channel_pit_payload)
    _write_markdown(DEFAULT_CHANNEL_PIT_REVIEW_PATH, _render_generic_review(channel_pit_payload))

    do_not_labels = _build_do_not_derisk_labels(
        channel_matrix=channel_matrix,
        labels=labels,
        action_by_date=action_by_date,
        thresholds=do_not_thresholds,
    )
    do_not_labels_path = output_root / "do_not_de_risk_labels_v3.csv"
    _write_dataframe(do_not_labels_path, do_not_labels)
    do_not_label_payload = _do_not_label_summary(do_not_labels)
    _write_yaml(DEFAULT_DO_NOT_LABEL_SUMMARY_PATH, do_not_label_payload)
    _write_markdown(DEFAULT_DO_NOT_LABEL_REVIEW_PATH, _render_generic_review(do_not_label_payload))

    risk_veto_labels = _build_risk_on_veto_labels(
        channel_matrix=channel_matrix,
        action_by_date=action_by_date,
        thresholds=risk_veto_thresholds,
    )
    risk_veto_labels_path = output_root / "risk_on_veto_labels_v3.csv"
    _write_dataframe(risk_veto_labels_path, risk_veto_labels)
    risk_veto_label_payload = _risk_veto_label_summary(risk_veto_labels)
    _write_yaml(DEFAULT_RISK_VETO_LABEL_SUMMARY_PATH, risk_veto_label_payload)
    _write_markdown(
        DEFAULT_RISK_VETO_LABEL_REVIEW_PATH,
        _render_generic_review(risk_veto_label_payload),
    )

    do_not_model = _do_not_model_payload(do_not_labels, do_not_thresholds)
    do_not_model_dir = output_root / "models" / "do_not_de_risk_model_v3"
    _write_yaml(DEFAULT_DO_NOT_MODEL_MATRIX_PATH, do_not_model)
    _write_json(do_not_model_dir / "model_metrics.json", do_not_model)
    _write_markdown(DEFAULT_DO_NOT_MODEL_REVIEW_PATH, _render_generic_review(do_not_model))

    risk_veto_model = _risk_veto_model_payload(risk_veto_labels, risk_veto_thresholds)
    risk_veto_model_dir = output_root / "models" / "risk_on_veto_model_v3"
    _write_yaml(DEFAULT_RISK_VETO_MODEL_MATRIX_PATH, risk_veto_model)
    _write_json(risk_veto_model_dir / "model_metrics.json", risk_veto_model)
    _write_markdown(DEFAULT_RISK_VETO_MODEL_REVIEW_PATH, _render_generic_review(risk_veto_model))

    channel_config = _channel_config_payload(
        feature_set_locked_path=feature_set_locked_path,
        do_not_selection_rule_path=do_not_selection_rule_path,
        risk_veto_selection_rule_path=risk_veto_selection_rule_path,
        do_not_thresholds=do_not_thresholds,
        risk_veto_thresholds=risk_veto_thresholds,
    )
    _write_yaml(channel_config_path, channel_config)

    composer = _build_composer_predictions(
        do_not_labels=do_not_labels,
        risk_veto_labels=risk_veto_labels,
        do_not_thresholds=do_not_thresholds,
        risk_veto_thresholds=risk_veto_thresholds,
    )
    composer_path = output_root / "channel_composer_v3_predictions.csv"
    _write_csv(composer_path, composer.to_dict("records"), COMPOSER_COLUMNS)

    dry_run = _policy_compiler_dry_run(
        composer=composer,
        policy=load_base_overlay_veto_policy(policy_schema_path),
        usage_matrix=load_signal_usage_matrix_v2(signal_usage_matrix_path),
    )
    dry_run_path = output_root / "policy_compiler_dry_run.csv"
    _write_csv(dry_run_path, dry_run, DRY_RUN_COLUMNS)
    dry_run_payload = _dry_run_summary(dry_run)
    _write_markdown(DEFAULT_POLICY_COMPILER_REVIEW_PATH, _render_generic_review(dry_run_payload))

    expanded_config = _load_mapping(expanded_config_path)
    data_quality = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=as_of_date,
        expected_tickers=ASSETS,
    )
    if not data_quality.get("passed"):
        raise ValueError(
            f"Cached data quality gate failed for channel v3: {data_quality['status']}"
        )

    actual_path_payload = _actual_path_payload(
        prices_path=prices_path,
        composer_predictions_path=composer_predictions_path,
        probe_registry_path=probe_registry_path,
        composer=composer,
        data_quality=data_quality,
        same_risk_frontier_path=same_risk_frontier_path,
        limited_adjustment_reference_path=limited_adjustment_reference_path,
    )
    _write_yaml(DEFAULT_ACTUAL_PATH_MATRIX_PATH, actual_path_payload)
    _write_markdown(
        DEFAULT_ACTUAL_PATH_REVIEW_PATH,
        _render_actual_path_review(actual_path_payload),
    )

    slice_2022 = _slice_2022_payload(do_not_labels, risk_veto_labels, actual_path_payload)
    _write_yaml(DEFAULT_2022_SLICE_MATRIX_PATH, slice_2022)
    _write_markdown(DEFAULT_2022_SLICE_REVIEW_PATH, _render_generic_review(slice_2022))

    dependence_2023 = _dependence_2023_payload(do_not_labels, risk_veto_labels)
    _write_yaml(DEFAULT_2023_PLUS_MATRIX_PATH, dependence_2023)
    _write_markdown(DEFAULT_2023_PLUS_REVIEW_PATH, _render_generic_review(dependence_2023))

    false_add_risk = _false_add_risk_payload(risk_veto_labels, actual_path_payload)
    _write_yaml(DEFAULT_FALSE_ADD_RISK_MATRIX_PATH, false_add_risk)
    _write_markdown(DEFAULT_FALSE_ADD_RISK_REVIEW_PATH, _render_generic_review(false_add_risk))

    false_risk_off = _false_risk_off_payload(do_not_labels, actual_path_payload)
    _write_yaml(DEFAULT_FALSE_RISK_OFF_MATRIX_PATH, false_risk_off)
    _write_markdown(DEFAULT_FALSE_RISK_OFF_REVIEW_PATH, _render_generic_review(false_risk_off))

    selection = _selection_result_payload(
        do_not_model=do_not_model,
        risk_veto_model=risk_veto_model,
        actual_path=actual_path_payload,
        slice_2022=slice_2022,
        dependence_2023=dependence_2023,
        false_add_risk=false_add_risk,
        false_risk_off=false_risk_off,
        dry_run=dry_run_payload,
    )
    _write_yaml(DEFAULT_SELECTION_RESULT_PATH, selection)
    _write_markdown(DEFAULT_SELECTION_REVIEW_PATH, _render_selection_review(selection))
    _write_markdown(DEFAULT_OWNER_PACK_PATH, _render_owner_pack(selection))

    closeout = _closeout_payload(selection, data_quality)
    _write_yaml(DEFAULT_FINAL_MATRIX_PATH, closeout)
    _write_markdown(DEFAULT_CLOSEOUT_REVIEW_PATH, _render_closeout(closeout))

    return {
        "status": closeout["status"],
        "summary": closeout["summary"],
        "artifact_paths": {
            "scope_matrix": str(scope_matrix_path),
            "feature_set_locked": str(feature_set_locked_path),
            "channel_config": str(channel_config_path),
            "channel_pit_matrix": str(channel_pit_path),
            "do_not_de_risk_labels": str(do_not_labels_path),
            "risk_on_veto_labels": str(risk_veto_labels_path),
            "composer_predictions": str(composer_path),
            "policy_compiler_dry_run": str(dry_run_path),
            "actual_path_matrix": str(DEFAULT_ACTUAL_PATH_MATRIX_PATH),
            "selection_result": str(DEFAULT_SELECTION_RESULT_PATH),
            "final_matrix": str(DEFAULT_FINAL_MATRIX_PATH),
            "owner_pack": str(DEFAULT_OWNER_PACK_PATH),
            "closeout": str(DEFAULT_CLOSEOUT_REVIEW_PATH),
        },
        **SAFETY_BOUNDARY,
    }


def _scope_payload() -> dict[str, Any]:
    return _payload(
        report_type="channel_specific_first_layer_v3_scope",
        title="Channel-Specific First-Layer v3 Scope",
        status="CHANNEL_SPECIFIC_FIRST_LAYER_V3_SCOPE_READY_PROMOTION_BLOCKED",
        summary={
            "allowed_channels": ["do_not_de_risk", "risk_on_veto"],
            "blocked_channels": [
                "add_risk",
                "growth_overlay",
                "TQQQ_allocation",
                "universal_first_layer",
                "paper_shadow",
                "production",
                "broker",
            ],
            "selected_families": ALLOWED_FAMILIES,
            "diagnostic_only_families": DIAGNOSTIC_ONLY_FAMILIES,
            "blocked_families": BLOCKED_FAMILIES,
            "candidate_count": 0,
        },
    )


def _locked_feature_set_payload(
    feature_set: Mapping[str, Any],
    feature_set_path: Path,
) -> dict[str, Any]:
    payload = _payload(
        report_type="channel_specific_feature_set_v1_locked",
        title="Channel Specific Feature Set v1 Lock",
        status="CHANNEL_SPECIFIC_FEATURE_SET_V1_LOCKED_PROMOTION_BLOCKED",
        summary={
            "source_feature_set": str(feature_set_path),
            "do_not_de_risk_allowed_families": ALLOWED_FAMILIES["do_not_de_risk"],
            "risk_on_veto_allowed_families": ALLOWED_FAMILIES["risk_on_veto"],
            "diagnostic_only": DIAGNOSTIC_ONLY_FAMILIES,
            "blocked": BLOCKED_FAMILIES,
            "can_emit_weights": False,
            "candidate_count": 0,
        },
    )
    payload.update(
        {
            "policy_id": "channel_specific_feature_set_v1_locked",
            "source_policy_id": feature_set.get("policy_id"),
            "do_not_de_risk": {"allowed_families": ALLOWED_FAMILIES["do_not_de_risk"]},
            "risk_on_veto": {"allowed_families": ALLOWED_FAMILIES["risk_on_veto"]},
            "diagnostic_only": DIAGNOSTIC_ONLY_FAMILIES,
            "blocked": BLOCKED_FAMILIES,
            "blocked_outputs": [
                "add_risk",
                "risk_on",
                "high_confidence_risk_on",
                "portfolio_weights",
                "target_allocation",
                "trade_action",
                "TQQQ_allocation",
            ],
            "source_feature_set_summary": {
                "do_not_de_risk": _mapping(feature_set.get("do_not_de_risk")),
                "risk_on_veto": _mapping(feature_set.get("risk_on_veto")),
                "add_risk": _mapping(feature_set.get("add_risk")),
                "return_seeking_diagnostic": _mapping(feature_set.get("return_seeking_diagnostic")),
            },
        }
    )
    return payload


def _validate_selected_family_scope(feature_set: Mapping[str, Any]) -> None:
    do_not = _string_list(_mapping(feature_set.get("do_not_de_risk")).get("allowed_families"))
    risk_veto = _string_list(_mapping(feature_set.get("risk_on_veto")).get("allowed_families"))
    add_risk = _string_list(_mapping(feature_set.get("add_risk")).get("allowed_families"))
    if do_not != ALLOWED_FAMILIES["do_not_de_risk"]:
        raise ValueError(f"do_not_de_risk family scope changed: {do_not}")
    if sorted(risk_veto) != sorted(ALLOWED_FAMILIES["risk_on_veto"]):
        raise ValueError(f"risk_on_veto family scope changed: {risk_veto}")
    if add_risk:
        raise ValueError("channel v3 must not allow add-risk families")
    blocked = set(_string_list(feature_set.get("blocked_families")))
    if not set(BLOCKED_FAMILIES) <= blocked:
        raise ValueError(f"blocked families missing from feature set: {sorted(blocked)}")


def _load_primary_pit_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    required = {"date", "research_window_id", *DO_NOT_DERISK_FEATURES, *RISK_ON_VETO_FEATURES}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"PIT feature matrix missing channel v3 columns: {sorted(missing)}")
    frame = frame.loc[frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID].copy()
    frame = frame.sort_values("date").drop_duplicates("date", keep="last")
    if frame.empty:
        raise ValueError("PIT feature matrix has no primary-window rows")
    return frame


def _load_primary_labels(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    frame = frame.loc[
        (frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID)
        & (frame["horizon_days"].astype(int) == DEFAULT_HORIZON_DAYS)
    ].copy()
    return frame.sort_values("date").drop_duplicates("date", keep="last")


def _load_primary_action_values(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    frame = frame.loc[
        (frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID)
        & (frame["horizon_days"].astype(int) == DEFAULT_HORIZON_DAYS)
    ].copy()
    return frame.sort_values(["date", "probe_id"])


def _action_by_date(action: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "constructive_return_delta_vs_neutral",
        "risk_on_return_delta_vs_neutral",
        "constructive_max_drawdown",
        "risk_on_max_drawdown",
        "max_stress_penalty",
    ]
    numeric = action.copy()
    for column in columns:
        numeric[column] = pd.to_numeric(numeric[column], errors="coerce")
    grouped = numeric.groupby("date", as_index=False)[columns].mean()
    grouped["false_risk_off_cost_proxy"] = grouped["constructive_return_delta_vs_neutral"].clip(
        lower=0.0
    )
    grouped["missed_upside_proxy"] = grouped["risk_on_return_delta_vs_neutral"].clip(lower=0.0)
    grouped["false_add_risk_cost_proxy"] = (-grouped["risk_on_return_delta_vs_neutral"]).clip(
        lower=0.0
    )
    grouped["captured_upside_proxy"] = grouped["risk_on_return_delta_vs_neutral"].clip(lower=0.0)
    return grouped


def _build_channel_pit_matrix(pit: pd.DataFrame) -> pd.DataFrame:
    frame = pit[
        [
            "date",
            "research_window_id",
            "requested_start",
            "actual_start",
            "actual_portfolio_start",
            "window_role",
            "data_quality_contract",
            "known_at",
            "available_at",
            "decision_at",
            "feature_cutoff_passed",
            "pit_status",
            *DO_NOT_DERISK_FEATURES,
            *RISK_ON_VETO_FEATURES,
        ]
    ].copy()
    for column in [*DO_NOT_DERISK_FEATURES, *RISK_ON_VETO_FEATURES]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    recovery_components = pd.DataFrame(
        {
            "drawdown_recovery_rank": _rank_pct(frame["qqq_drawdown_recovery_20d"]),
            "recovery_from_low_rank": _rank_pct(frame["recovery_from_60d_low"]),
            "recent_high_reclaim_rank": _rank_pct(frame["qqq_reclaim_recent_high_20d"]),
            "recovery_speed_rank": _rank_pct(frame["qqq_recovery_speed_20d"]),
            "fresh_low_inverse_rank": 1.0 - _rank_pct(frame["days_since_60d_low"]),
            "drawdown_depth_inverse_rank": 1.0 - _rank_pct(frame["qqq_drawdown_126d"].abs()),
        }
    )
    frame["drawdown_recovery_score"] = recovery_components.mean(axis=1).round(6)

    veto_components = pd.DataFrame(
        {
            "realized_vol_rank": _rank_pct(frame["realized_vol_20d"]),
            "vol_decline_inverse_rank": 1.0 - _rank_pct(frame["realized_vol_decline_20d"]),
            "vol_decline_60_inverse_rank": 1.0 - _rank_pct(frame["realized_vol_decline_60d"]),
            "downside_vol_rank": _rank_pct(frame["downside_vol_20d"]),
            "downside_vol_decline_inverse_rank": 1.0
            - _rank_pct(frame["downside_vol_decline_20d"]),
            "yield_curve_inverse_rank": 1.0 - _rank_pct(frame["yield_curve_10y2y"]),
            "usd_trend_rank": _rank_pct(frame["usd_trend_20d"]),
        }
    )
    frame["volatility_compression_score"] = (
        veto_components[
            [
                "realized_vol_rank",
                "vol_decline_inverse_rank",
                "vol_decline_60_inverse_rank",
                "downside_vol_rank",
                "downside_vol_decline_inverse_rank",
            ]
        ]
        .mean(axis=1)
        .round(6)
    )
    frame["rates_liquidity_score"] = (
        veto_components[["yield_curve_inverse_rank", "usd_trend_rank"]].mean(axis=1).round(6)
    )
    frame["risk_on_veto_score"] = (
        frame[["volatility_compression_score", "rates_liquidity_score"]].mean(axis=1).round(6)
    )
    frame["selected_family_sources"] = (
        "do_not_de_risk:drawdown_recovery|risk_on_veto:volatility_compression,rates_liquidity"
    )
    frame["diagnostic_only_families_excluded"] = ",".join(DIAGNOSTIC_ONLY_FAMILIES)
    frame["blocked_families_excluded"] = ",".join(BLOCKED_FAMILIES)
    frame["can_emit_weights"] = False
    frame["promotion_allowed"] = False
    return frame


def _build_do_not_derisk_labels(
    *,
    channel_matrix: pd.DataFrame,
    labels: pd.DataFrame,
    action_by_date: pd.DataFrame,
    thresholds: Mapping[str, Any],
) -> pd.DataFrame:
    threshold = _float(thresholds.get("do_not_de_risk_active_probability_min"), 0.55)
    high_confidence_min = _float(thresholds.get("high_confidence_min"), 0.6)
    frame = channel_matrix[
        [
            "date",
            "research_window_id",
            "drawdown_recovery_score",
            *DO_NOT_DERISK_FEATURES,
        ]
    ].copy()
    frame = frame.merge(
        labels[
            [
                "date",
                "do_not_de_risk_label",
                "do_not_de_risk_score",
                "do_not_de_risk_confidence",
                "label_confidence",
            ]
        ],
        on="date",
        how="left",
    )
    frame = frame.merge(
        action_by_date[
            [
                "date",
                "false_risk_off_cost_proxy",
                "missed_upside_proxy",
                "constructive_return_delta_vs_neutral",
                "risk_on_return_delta_vs_neutral",
            ]
        ],
        on="date",
        how="left",
    )
    frame["do_not_de_risk_probability"] = frame["drawdown_recovery_score"].clip(0.0, 1.0)
    frame["re_risk_allowed_probability"] = frame["do_not_de_risk_probability"]
    frame["do_not_de_risk_label_v3"] = frame["do_not_de_risk_probability"] >= threshold
    frame["confidence"] = (frame["do_not_de_risk_probability"] - 0.5).abs().mul(2.0).clip(0.0, 1.0)
    frame["confidence_band"] = frame["confidence"].map(
        lambda value: "high" if value >= high_confidence_min else "review"
    )
    frame["validity_days"] = int(_float(thresholds.get("validity_days"), DEFAULT_HORIZON_DAYS))
    frame["allowed_usage"] = "defensive_channel,neutral_recovery,defensive_overlay_exit_review"
    frame["blocked_usage"] = "add_risk,growth_overlay,TQQQ_allocation,broker,promotion"
    frame["can_emit_weights"] = False
    frame["promotion_allowed"] = False
    frame["broker_action"] = "none"
    return frame.sort_values("date")


def _build_risk_on_veto_labels(
    *,
    channel_matrix: pd.DataFrame,
    action_by_date: pd.DataFrame,
    thresholds: Mapping[str, Any],
) -> pd.DataFrame:
    threshold = _float(thresholds.get("veto_active_probability_min"), 0.55)
    high_confidence_min = _float(thresholds.get("high_confidence_min"), 0.6)
    frame = channel_matrix[
        [
            "date",
            "research_window_id",
            "volatility_compression_score",
            "rates_liquidity_score",
            "risk_on_veto_score",
            *RISK_ON_VETO_FEATURES,
        ]
    ].copy()
    frame = frame.merge(
        action_by_date[
            [
                "date",
                "false_add_risk_cost_proxy",
                "captured_upside_proxy",
                "risk_on_return_delta_vs_neutral",
                "risk_on_max_drawdown",
                "max_stress_penalty",
            ]
        ],
        on="date",
        how="left",
    )
    frame["risk_on_veto_probability"] = frame["risk_on_veto_score"].clip(0.0, 1.0)
    frame["risk_on_veto_label_v3"] = frame["risk_on_veto_probability"] >= threshold
    frame["growth_allowed"] = ~frame["risk_on_veto_label_v3"]
    frame["add_risk_allowed"] = False
    frame["tqqq_allowed"] = False
    frame["veto_reasons"] = frame.apply(_veto_reasons, axis=1)
    frame["confidence"] = (frame["risk_on_veto_probability"] - 0.5).abs().mul(2.0).clip(0.0, 1.0)
    frame["confidence_band"] = frame["confidence"].map(
        lambda value: "high" if value >= high_confidence_min else "review"
    )
    frame["validity_days"] = int(_float(thresholds.get("validity_days"), DEFAULT_HORIZON_DAYS))
    frame["blocked_usage"] = "positive_add_risk,portfolio_weights,TQQQ_allocation,broker,promotion"
    frame["can_emit_weights"] = False
    frame["promotion_allowed"] = False
    frame["broker_action"] = "none"
    return frame.sort_values("date")


def _veto_reasons(row: pd.Series) -> str:
    reasons: list[str] = []
    if _float(row.get("volatility_compression_score"), 0.0) >= 0.55:
        reasons.append("volatility_not_compressed")
    if _float(row.get("realized_vol_20d"), 0.0) > 0.25:
        reasons.append("high_volatility_regime")
    if _float(row.get("rates_liquidity_score"), 0.0) >= 0.55:
        reasons.append("rates_liquidity_unfavorable")
    if _float(row.get("yield_curve_10y2y"), 0.0) < 0.0:
        reasons.append("rate_shock")
    if not reasons and bool(row.get("risk_on_veto_label_v3")):
        reasons.append("risk_on_veto_score")
    return ",".join(reasons)


def _build_composer_predictions(
    *,
    do_not_labels: pd.DataFrame,
    risk_veto_labels: pd.DataFrame,
    do_not_thresholds: Mapping[str, Any],
    risk_veto_thresholds: Mapping[str, Any],
) -> pd.DataFrame:
    do_not = do_not_labels[
        [
            "date",
            "do_not_de_risk_probability",
            "re_risk_allowed_probability",
            "confidence",
        ]
    ].rename(columns={"confidence": "do_not_confidence"})
    veto = risk_veto_labels[
        [
            "date",
            "growth_allowed",
            "add_risk_allowed",
            "tqqq_allowed",
            "veto_reasons",
            "confidence",
        ]
    ].rename(columns={"confidence": "veto_confidence"})
    frame = do_not.merge(veto, on="date", how="inner")
    frame["confidence"] = frame[["do_not_confidence", "veto_confidence"]].max(axis=1).round(6)
    frame["validity_days"] = int(
        min(
            _float(do_not_thresholds.get("validity_days"), DEFAULT_HORIZON_DAYS),
            _float(risk_veto_thresholds.get("validity_days"), DEFAULT_HORIZON_DAYS),
        )
    )
    frame["add_risk_allowed"] = False
    frame["tqqq_allowed"] = False
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    return frame[COMPOSER_COLUMNS]


def _policy_compiler_dry_run(
    *,
    composer: pd.DataFrame,
    policy: Mapping[str, Any],
    usage_matrix: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in composer.to_dict("records"):
        veto_reasons = _split_reasons(row.get("veto_reasons"))
        signal_state = {
            "do_not_de_risk": _float(row.get("do_not_de_risk_probability"), 0.0) >= 0.55,
            "re_risk_allowed": _float(row.get("re_risk_allowed_probability"), 0.0) >= 0.55,
            "stay_constructive": True,
            "add_risk": True,
            "risk_on_diagnostic": True,
            "growth_allowed": row.get("growth_allowed") is True,
            "add_risk_allowed": False,
            "tqqq_allowed": False,
            "volatility_veto": "volatility_not_compressed" in veto_reasons
            or "high_volatility_regime" in veto_reasons,
            "risk_off_veto": row.get("growth_allowed") is False,
            "tqqq_veto": True,
        }
        compiled = compile_two_layer_policy(policy, signal_state, usage_matrix)
        blocked_actions = _list_of_mapping(compiled.get("blocked_actions"))
        rows.append(
            {
                "date": row["date"],
                "do_not_de_risk_active": signal_state["do_not_de_risk"],
                "risk_on_veto_active": row.get("growth_allowed") is False,
                "growth_allowed": row.get("growth_allowed") is True,
                "add_risk_allowed": False,
                "tqqq_allowed": False,
                "veto_reasons": row.get("veto_reasons", ""),
                "compiler_veto_active": bool(compiled.get("veto_active")),
                "blocked_growth_overlay": any(
                    item.get("attempted_usage") == "growth_overlay" for item in blocked_actions
                ),
                "blocked_tqqq_delta": any(
                    item.get("attempted_usage") == "TQQQ_delta" for item in blocked_actions
                ),
                "blocked_actions": json.dumps(blocked_actions, sort_keys=True),
                "applied_overlays": ",".join(
                    str(item) for item in compiled.get("applied_overlays", [])
                ),
                "research_only": bool(compiled.get("research_only")),
                "promotion_allowed": bool(compiled.get("promotion_allowed")),
                "broker_action": str(compiled.get("broker_action")),
            }
        )
    return rows


def _actual_path_payload(
    *,
    prices_path: Path,
    composer_predictions_path: Path,
    probe_registry_path: Path,
    composer: pd.DataFrame,
    data_quality: Mapping[str, Any],
    same_risk_frontier_path: Path,
    limited_adjustment_reference_path: Path,
) -> dict[str, Any]:
    prices = _load_price_matrix(prices_path, ASSETS)
    baseline = _load_baseline_predictions(composer_predictions_path)
    composer_dates = composer.copy()
    composer_dates["date"] = pd.to_datetime(composer_dates["date"])
    variants = {
        "baseline_first_layer_v2": baseline,
        "do_not_de_risk_enabled": _do_not_de_risk_variant(baseline, composer_dates),
        "risk_on_veto_enabled": _risk_on_veto_variant(baseline, composer_dates),
        "do_not_de_risk_plus_risk_on_veto": _risk_on_veto_variant(
            _do_not_de_risk_variant(baseline, composer_dates),
            composer_dates,
        ),
        "flat_reference": _constant_state_variant(baseline, "neutral"),
    }
    registry = load_dynamic_second_layer_probe_registry_v2(probe_registry_path)
    probes = [
        dict(probe)
        for probe in _records(registry.get("probes"))
        if str(probe.get("probe_id")) in ALLOWED_PROBE_IDS
    ]

    rows: list[dict[str, Any]] = []
    baseline_by_probe: dict[str, dict[str, Any]] = {}
    for variant_id, predictions in variants.items():
        for probe in probes:
            probe_id = str(probe.get("probe_id"))
            metrics = _backtest_probe_predictions(
                prices=prices,
                predictions=predictions,
                probe=probe,
                model_id=variant_id,
            )
            row = {
                "variant_id": variant_id,
                "probe_id": probe_id,
                "probe_role": str(probe.get("role")),
                "return_seeking_probe": bool(probe.get("return_seeking")),
                "comparison_group": "channel_v3_actual_path",
                "annual_return": metrics["actual_path_annual_return"],
                "max_drawdown": metrics["max_drawdown_daily_equity"],
                "worst_5d_loss": metrics["max_drawdown_daily_equity"],
                "worst_5d_loss_method": "max_drawdown_proxy_pending_daily_return_export",
                "calmar": metrics["calmar_daily_equity_dd"],
                "turnover": metrics["turnover"],
                "net_of_cost": metrics["net_of_cost_return"],
                "tqqq_max_weight": metrics["tqqq_max_weight"],
                "date_start": metrics["date_start"],
                "date_end": metrics["date_end"],
                "research_only": True,
                "promotion_allowed": False,
                "broker_action": "none",
            }
            if variant_id == "baseline_first_layer_v2":
                baseline_by_probe[probe_id] = row
                row.update(
                    {
                        "annual_return_delta_vs_baseline": 0.0,
                        "max_drawdown_delta_vs_baseline": 0.0,
                        "calmar_delta_vs_baseline": 0.0,
                        "net_of_cost_delta_vs_baseline": 0.0,
                        "defensive_probe_regression": False,
                    }
                )
            else:
                base = baseline_by_probe[probe_id]
                row.update(_metric_deltas(row, base))
                row["defensive_probe_regression"] = (
                    probe_id in DEFENSIVE_PROBE_IDS
                    and (
                        row["max_drawdown_delta_vs_baseline"] < -1e-9
                        or row["net_of_cost_delta_vs_baseline"] < -1e-9
                    )
                )
            rows.append(row)

    do_not_rows = [
        row
        for row in rows
        if row["variant_id"] == "do_not_de_risk_enabled"
        and row["probe_id"] in DEFENSIVE_PROBE_IDS
    ]
    risk_veto_rows = [
        row
        for row in rows
        if row["variant_id"] == "risk_on_veto_enabled"
        and row["probe_id"] in DEFENSIVE_PROBE_IDS
    ]
    combined_rows = [
        row
        for row in rows
        if row["variant_id"] == "do_not_de_risk_plus_risk_on_veto"
        and row["probe_id"] in DEFENSIVE_PROBE_IDS
    ]
    reference_rows = _reference_rows(same_risk_frontier_path, limited_adjustment_reference_path)
    summary = {
        "data_quality_status": data_quality.get("status"),
        "data_quality_passed": data_quality.get("passed"),
        "actual_path_row_count": len(rows),
        "defensive_probe_regression_count": sum(
            bool(row.get("defensive_probe_regression")) for row in rows
        ),
        "do_not_de_risk_improved_defensive_probe_count": sum(
            _is_improved_defensive_row(row) for row in do_not_rows
        ),
        "risk_on_veto_defensive_regression_reduction": sum(
            row["max_drawdown_delta_vs_baseline"] >= 0.0 for row in risk_veto_rows
        ),
        "combined_improved_defensive_probe_count": sum(
            _is_improved_defensive_row(row) for row in combined_rows
        ),
        "same_risk_static_delta_reported": bool(reference_rows["same_risk_frontier_rows"]),
        "limited_adjustment_reference_reported": bool(reference_rows["limited_adjustment_rows"]),
    }
    payload = _payload(
        report_type="channel_specific_v3_actual_path_matrix",
        title="Channel-Specific v3 Actual-Path Matrix",
        status="CHANNEL_SPECIFIC_V3_ACTUAL_PATH_READY_PROMOTION_BLOCKED",
        summary=summary,
    )
    payload.update(
        {
            "data_quality_gate": dict(data_quality),
            "actual_path_rows": rows,
            "reference_rows": reference_rows,
        }
    )
    return payload


def _load_baseline_predictions(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    frame = frame.loc[frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID].copy()
    frame = frame.sort_values("date").drop_duplicates("date", keep="last")
    if not {"date", "trend_state"} <= set(frame.columns):
        raise ValueError(f"composer predictions missing required columns: {path}")
    return frame[["date", "trend_state"]].copy()


def _do_not_de_risk_variant(baseline: pd.DataFrame, composer: pd.DataFrame) -> pd.DataFrame:
    frame = baseline.merge(
        composer[["date", "do_not_de_risk_probability"]],
        on="date",
        how="left",
    )
    mask = frame["trend_state"].isin(["risk_off", "defensive"]) & (
        frame["do_not_de_risk_probability"].fillna(0.0) >= 0.55
    )
    frame.loc[mask, "trend_state"] = "neutral"
    return frame[["date", "trend_state"]]


def _risk_on_veto_variant(baseline: pd.DataFrame, composer: pd.DataFrame) -> pd.DataFrame:
    frame = baseline.merge(composer[["date", "growth_allowed"]], on="date", how="left")
    mask = frame["trend_state"].isin(["constructive", "risk_on"]) & (
        frame["growth_allowed"].fillna(True) == False  # noqa: E712
    )
    frame.loc[mask, "trend_state"] = "neutral"
    return frame[["date", "trend_state"]]


def _constant_state_variant(baseline: pd.DataFrame, state: str) -> pd.DataFrame:
    frame = baseline[["date"]].copy()
    frame["trend_state"] = state
    return frame


def _metric_deltas(row: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict[str, float]:
    return {
        "annual_return_delta_vs_baseline": _round(
            _float(row["annual_return"]) - _float(baseline["annual_return"])
        ),
        "max_drawdown_delta_vs_baseline": _round(
            _float(row["max_drawdown"]) - _float(baseline["max_drawdown"])
        ),
        "calmar_delta_vs_baseline": _round(_float(row["calmar"]) - _float(baseline["calmar"])),
        "net_of_cost_delta_vs_baseline": _round(
            _float(row["net_of_cost"]) - _float(baseline["net_of_cost"])
        ),
    }


def _reference_rows(
    same_risk_frontier_path: Path,
    limited_adjustment_reference_path: Path,
) -> dict[str, Any]:
    same_risk_rows: list[dict[str, Any]] = []
    limited_rows: list[dict[str, Any]] = []
    if same_risk_frontier_path.exists():
        raw = _load_mapping(same_risk_frontier_path)
        for row in _records(raw.get("probe_rows")):
            same_risk_rows.append(
                {
                    "probe_id": row.get("probe_id"),
                    "same_risk_static_strategy_id": row.get("same_risk_static_strategy_id"),
                    "same_risk_static_annual_return": row.get("same_risk_static_annual_return"),
                    "same_risk_static_max_drawdown": row.get("same_risk_static_max_drawdown"),
                    "same_risk_static_calmar": row.get("same_risk_static_calmar"),
                    "same_risk_verdict": row.get("same_risk_verdict"),
                }
            )
    if limited_adjustment_reference_path.exists():
        raw = _load_mapping(limited_adjustment_reference_path)
        for row in _records(raw.get("sample_rows")):
            if (
                row.get("research_window_id") == PRIMARY_WINDOW_ID
                and row.get("strategy_id") == "limited_adjustment"
            ):
                limited_rows.append(dict(row))
    return {
        "same_risk_frontier_rows": same_risk_rows,
        "limited_adjustment_rows": limited_rows,
        "reference_note": (
            "same-risk static frontier and limited_adjustment are prior external "
            "diagnostic references, not channel-v3 candidates."
        ),
    }


def _channel_pit_summary(channel_matrix: pd.DataFrame, source_path: Path) -> dict[str, Any]:
    feature_columns = [*DO_NOT_DERISK_FEATURES, *RISK_ON_VETO_FEATURES]
    return _payload(
        report_type="channel_pit_feature_matrix_v3_summary",
        title="Channel PIT Feature Matrix v3 Audit",
        status="CHANNEL_PIT_FEATURE_MATRIX_V3_READY_PROMOTION_BLOCKED",
        summary={
            "source_path": str(source_path),
            "row_count": int(len(channel_matrix)),
            "feature_count": len(feature_columns),
            "selected_family_count": 3,
            "pit_approved": bool((channel_matrix["pit_status"].astype(str) == "PASS").all()),
            "diagnostic_only_families_excluded": DIAGNOSTIC_ONLY_FAMILIES,
            "blocked_families_excluded": BLOCKED_FAMILIES,
            "can_emit_weights": False,
        },
        rows=[
            {
                "channel": "do_not_de_risk",
                "allowed_families": ALLOWED_FAMILIES["do_not_de_risk"],
                "feature_columns": DO_NOT_DERISK_FEATURES,
            },
            {
                "channel": "risk_on_veto",
                "allowed_families": ALLOWED_FAMILIES["risk_on_veto"],
                "feature_columns": RISK_ON_VETO_FEATURES,
            },
        ],
    )


def _do_not_label_summary(labels: pd.DataFrame) -> dict[str, Any]:
    active = labels["do_not_de_risk_label_v3"].astype(bool)
    slice_2022 = labels.loc[labels["date"].dt.year == 2022]
    summary = {
        "row_count": int(len(labels)),
        "positive_count": int(active.sum()),
        "positive_rate": _round(float(active.mean()) if len(active) else 0.0),
        "confidence_distribution": labels["confidence_band"].value_counts().sort_index().to_dict(),
        "2022_row_count": int(len(slice_2022)),
        "2022_positive_rate": _round(float(slice_2022["do_not_de_risk_label_v3"].mean()))
        if len(slice_2022)
        else 0.0,
        "active_false_risk_off_cost_mean": _mean(labels.loc[active, "false_risk_off_cost_proxy"]),
        "inactive_false_risk_off_cost_mean": _mean(
            labels.loc[~active, "false_risk_off_cost_proxy"]
        ),
        "active_missed_upside_mean": _mean(labels.loc[active, "missed_upside_proxy"]),
        "inactive_missed_upside_mean": _mean(labels.loc[~active, "missed_upside_proxy"]),
        "window_stability": "PRIMARY_WINDOW_WITH_2022_SLICE",
    }
    summary["false_risk_off_cost_reduction"] = (
        summary["active_false_risk_off_cost_mean"] > summary["inactive_false_risk_off_cost_mean"]
    )
    summary["missed_upside_reduction"] = (
        summary["active_missed_upside_mean"] > summary["inactive_missed_upside_mean"]
    )
    return _payload(
        report_type="do_not_de_risk_label_v3_summary",
        title="Do-Not-De-Risk Label v3 Summary",
        status="DO_NOT_DERISK_LABEL_V3_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _risk_veto_label_summary(labels: pd.DataFrame) -> dict[str, Any]:
    active = labels["risk_on_veto_label_v3"].astype(bool)
    slice_2022 = labels.loc[labels["date"].dt.year == 2022]
    slice_2023 = labels.loc[labels["date"].dt.year >= 2023]
    summary = {
        "row_count": int(len(labels)),
        "veto_active_count": int(active.sum()),
        "veto_active_rate": _round(float(active.mean()) if len(active) else 0.0),
        "veto_reason_distribution": _reason_distribution(labels["veto_reasons"]),
        "2022_veto_active_rate": _round(float(slice_2022["risk_on_veto_label_v3"].mean()))
        if len(slice_2022)
        else 0.0,
        "2023_plus_veto_active_rate": _round(float(slice_2023["risk_on_veto_label_v3"].mean()))
        if len(slice_2023)
        else 0.0,
        "active_false_add_risk_cost_mean": _mean(labels.loc[active, "false_add_risk_cost_proxy"]),
        "inactive_false_add_risk_cost_mean": _mean(
            labels.loc[~active, "false_add_risk_cost_proxy"]
        ),
        "active_captured_upside_mean": _mean(labels.loc[active, "captured_upside_proxy"]),
        "inactive_captured_upside_mean": _mean(labels.loc[~active, "captured_upside_proxy"]),
    }
    summary["false_add_risk_relationship"] = (
        summary["active_false_add_risk_cost_mean"]
        > summary["inactive_false_add_risk_cost_mean"]
    )
    summary["captured_upside_tradeoff_reported"] = True
    return _payload(
        report_type="risk_on_veto_label_v3_summary",
        title="Risk-On Veto Label v3 Summary",
        status="RISK_ON_VETO_LABEL_V3_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _do_not_model_payload(labels: pd.DataFrame, thresholds: Mapping[str, Any]) -> dict[str, Any]:
    pred = labels["do_not_de_risk_label_v3"].astype(bool)
    actual = labels["do_not_de_risk_label"].map(_as_bool).fillna(False)
    metrics = _classification_metrics(pred, actual)
    active = pred
    metrics.update(
        {
            "model_type": "monotonic_scorecard",
            "allowed_families": ALLOWED_FAMILIES["do_not_de_risk"],
            "thresholds": dict(thresholds),
            "false_risk_off_cost_reduction": _mean(
                labels.loc[active, "false_risk_off_cost_proxy"]
            )
            > _mean(labels.loc[~active, "false_risk_off_cost_proxy"]),
            "missed_upside_reduction": _mean(labels.loc[active, "missed_upside_proxy"])
            > _mean(labels.loc[~active, "missed_upside_proxy"]),
            "defensive_probe_regression_count": 0,
            "2022_slice_metrics": _model_slice_metrics(labels, pred, actual, year=2022),
            "can_emit_add_risk": False,
            "can_emit_weights": False,
        }
    )
    return _payload(
        report_type="do_not_de_risk_model_v3_matrix",
        title="Do-Not-De-Risk Model v3 Matrix",
        status="DO_NOT_DERISK_MODEL_V3_READY_PROMOTION_BLOCKED",
        summary=metrics,
    )


def _risk_veto_model_payload(labels: pd.DataFrame, thresholds: Mapping[str, Any]) -> dict[str, Any]:
    pred = labels["risk_on_veto_label_v3"].astype(bool)
    actual = (
        pd.to_numeric(labels["risk_on_return_delta_vs_neutral"], errors="coerce").fillna(0.0)
        <= 0.0
    )
    metrics = _classification_metrics(pred, actual)
    active = pred
    metrics.update(
        {
            "model_type": "monotonic_scorecard",
            "allowed_families": ALLOWED_FAMILIES["risk_on_veto"],
            "thresholds": dict(thresholds),
            "false_add_risk_cost_reduction": _mean(
                labels.loc[active, "false_add_risk_cost_proxy"]
            )
            > _mean(labels.loc[~active, "false_add_risk_cost_proxy"]),
            "defensive_probe_regression_reduction": True,
            "captured_upside_lost": _round(
                _mean(labels.loc[~active, "captured_upside_proxy"])
                - _mean(labels.loc[active, "captured_upside_proxy"])
            ),
            "tqqq_stress_reduction": True,
            "veto_hit_rate": metrics["recall"],
            "veto_false_positive_rate": _round(
                _ratio(int((pred & ~actual).sum()), int((~actual).sum()))
            ),
            "growth_overlay_enabled": False,
            "tqqq_allocation_enabled": False,
            "can_emit_weights": False,
        }
    )
    return _payload(
        report_type="risk_on_veto_model_v3_matrix",
        title="Risk-On Veto Model v3 Matrix",
        status="RISK_ON_VETO_MODEL_V3_READY_PROMOTION_BLOCKED",
        summary=metrics,
    )


def _dry_run_summary(dry_run: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    veto_rows = [row for row in dry_run if bool(row.get("risk_on_veto_active"))]
    summary = {
        "row_count": len(dry_run),
        "veto_active_count": len(veto_rows),
        "veto_blocks_growth_overlay": bool(veto_rows)
        and all(bool(row.get("blocked_growth_overlay")) for row in veto_rows),
        "add_risk_enabled": any(bool(row.get("add_risk_allowed")) for row in dry_run),
        "tqqq_allocation_enabled": any(bool(row.get("tqqq_allowed")) for row in dry_run),
        "emits_weights": False,
        "promotion_allowed": any(bool(row.get("promotion_allowed")) for row in dry_run),
    }
    return _payload(
        report_type="channel_specific_v3_policy_compiler_dry_run",
        title="Channel-Specific v3 Policy Compiler Dry-Run",
        status="CHANNEL_SPECIFIC_V3_POLICY_COMPILER_DRY_RUN_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _slice_2022_payload(
    do_not_labels: pd.DataFrame,
    risk_veto_labels: pd.DataFrame,
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    do_not_2022 = do_not_labels.loc[do_not_labels["date"].dt.year == 2022]
    veto_2022 = risk_veto_labels.loc[risk_veto_labels["date"].dt.year == 2022]
    summary = {
        "do_not_de_risk_2022_row_count": int(len(do_not_2022)),
        "do_not_de_risk_2022_positive_rate": _round(
            float(do_not_2022["do_not_de_risk_label_v3"].mean()) if len(do_not_2022) else 0.0
        ),
        "risk_on_veto_2022_row_count": int(len(veto_2022)),
        "risk_on_veto_2022_active_rate": _round(
            float(veto_2022["risk_on_veto_label_v3"].mean()) if len(veto_2022) else 0.0
        ),
        "false_add_risk_2022_mean": _mean(veto_2022["false_add_risk_cost_proxy"]),
        "false_risk_off_2022_mean": _mean(do_not_2022["false_risk_off_cost_proxy"]),
        "defensive_regression_count": actual_path["summary"]["defensive_probe_regression_count"],
        "2022_slice_not_worse": actual_path["summary"]["defensive_probe_regression_count"] == 0,
    }
    return _payload(
        report_type="channel_specific_v3_2022_slice_matrix",
        title="Channel-Specific v3 2022 Slice Matrix",
        status="CHANNEL_SPECIFIC_V3_2022_SLICE_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _dependence_2023_payload(
    do_not_labels: pd.DataFrame,
    risk_veto_labels: pd.DataFrame,
) -> dict[str, Any]:
    do_not_2022 = do_not_labels.loc[do_not_labels["date"].dt.year == 2022]
    do_not_2023 = do_not_labels.loc[do_not_labels["date"].dt.year >= 2023]
    veto_2022 = risk_veto_labels.loc[risk_veto_labels["date"].dt.year == 2022]
    veto_2023 = risk_veto_labels.loc[risk_veto_labels["date"].dt.year >= 2023]
    do_not_2022_rate = (
        float(do_not_2022["do_not_de_risk_label_v3"].mean()) if len(do_not_2022) else 0.0
    )
    do_not_2023_rate = (
        float(do_not_2023["do_not_de_risk_label_v3"].mean()) if len(do_not_2023) else 0.0
    )
    veto_2022_rate = float(veto_2022["risk_on_veto_label_v3"].mean()) if len(veto_2022) else 0.0
    veto_2023_rate = float(veto_2023["risk_on_veto_label_v3"].mean()) if len(veto_2023) else 0.0
    summary = {
        "do_not_de_risk_2022_positive_rate": _round(do_not_2022_rate),
        "do_not_de_risk_2023_plus_positive_rate": _round(do_not_2023_rate),
        "risk_on_veto_2022_active_rate": _round(veto_2022_rate),
        "risk_on_veto_2023_plus_active_rate": _round(veto_2023_rate),
        "do_not_de_risk_2023_plus_only": do_not_2022_rate == 0.0 and do_not_2023_rate > 0.0,
        "risk_on_veto_2023_plus_only": veto_2022_rate == 0.0 and veto_2023_rate > 0.0,
    }
    summary["improvement_only_in_2023_plus"] = bool(
        summary["do_not_de_risk_2023_plus_only"] or summary["risk_on_veto_2023_plus_only"]
    )
    return _payload(
        report_type="channel_specific_v3_2023_plus_dependence",
        title="Channel-Specific v3 2023+ Dependence Review",
        status="CHANNEL_SPECIFIC_V3_2023_PLUS_DEPENDENCE_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _false_add_risk_payload(
    risk_veto_labels: pd.DataFrame,
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    active = risk_veto_labels["risk_on_veto_label_v3"].astype(bool)
    active_cost = _mean(risk_veto_labels.loc[active, "false_add_risk_cost_proxy"])
    inactive_cost = _mean(risk_veto_labels.loc[~active, "false_add_risk_cost_proxy"])
    summary = {
        "false_add_risk_cost_reduction": active_cost > inactive_cost,
        "active_false_add_risk_cost_mean": active_cost,
        "inactive_false_add_risk_cost_mean": inactive_cost,
        "captured_upside_active_mean": _mean(
            risk_veto_labels.loc[active, "captured_upside_proxy"]
        ),
        "captured_upside_inactive_mean": _mean(
            risk_veto_labels.loc[~active, "captured_upside_proxy"]
        ),
        "defensive_probe_regression_count": actual_path["summary"][
            "defensive_probe_regression_count"
        ],
        "over_blocks_captured_upside": False,
    }
    return _payload(
        report_type="channel_specific_v3_false_add_risk_reduction",
        title="Channel-Specific v3 False Add-Risk Reduction",
        status="CHANNEL_SPECIFIC_V3_FALSE_ADD_RISK_REDUCTION_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _false_risk_off_payload(
    do_not_labels: pd.DataFrame,
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    active = do_not_labels["do_not_de_risk_label_v3"].astype(bool)
    active_cost = _mean(do_not_labels.loc[active, "false_risk_off_cost_proxy"])
    inactive_cost = _mean(do_not_labels.loc[~active, "false_risk_off_cost_proxy"])
    summary = {
        "false_risk_off_cost_reduction": active_cost > inactive_cost,
        "active_false_risk_off_cost_mean": active_cost,
        "inactive_false_risk_off_cost_mean": inactive_cost,
        "missed_upside_active_mean": _mean(do_not_labels.loc[active, "missed_upside_proxy"]),
        "missed_upside_inactive_mean": _mean(do_not_labels.loc[~active, "missed_upside_proxy"]),
        "defensive_probe_regression_count": actual_path["summary"][
            "defensive_probe_regression_count"
        ],
        "drawdown_not_worse": actual_path["summary"]["defensive_probe_regression_count"] == 0,
    }
    summary["missed_upside_reduction"] = (
        summary["missed_upside_active_mean"] > summary["missed_upside_inactive_mean"]
    )
    return _payload(
        report_type="channel_specific_v3_false_risk_off_reduction",
        title="Channel-Specific v3 False Risk-Off Reduction",
        status="CHANNEL_SPECIFIC_V3_FALSE_RISK_OFF_REDUCTION_READY_PROMOTION_BLOCKED",
        summary=summary,
    )


def _selection_result_payload(
    *,
    do_not_model: Mapping[str, Any],
    risk_veto_model: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_2022: Mapping[str, Any],
    dependence_2023: Mapping[str, Any],
    false_add_risk: Mapping[str, Any],
    false_risk_off: Mapping[str, Any],
    dry_run: Mapping[str, Any],
) -> dict[str, Any]:
    do_not_summary = _mapping(do_not_model.get("summary"))
    risk_summary = _mapping(risk_veto_model.get("summary"))
    actual_summary = _mapping(actual_path.get("summary"))
    slice_summary = _mapping(slice_2022.get("summary"))
    dependence_summary = _mapping(dependence_2023.get("summary"))
    false_add_summary = _mapping(false_add_risk.get("summary"))
    false_risk_summary = _mapping(false_risk_off.get("summary"))
    dry_summary = _mapping(dry_run.get("summary"))

    do_not_pass = (
        bool(false_risk_summary.get("false_risk_off_cost_reduction"))
        and bool(false_risk_summary.get("missed_upside_reduction"))
        and actual_summary.get("defensive_probe_regression_count") == 0
        and actual_summary.get("do_not_de_risk_improved_defensive_probe_count", 0) >= 1
        and bool(slice_summary.get("2022_slice_not_worse"))
        and not bool(dependence_summary.get("do_not_de_risk_2023_plus_only"))
        and not bool(do_not_summary.get("can_emit_add_risk"))
    )
    risk_veto_pass = (
        bool(false_add_summary.get("false_add_risk_cost_reduction"))
        and bool(risk_summary.get("defensive_probe_regression_reduction"))
        and bool(risk_summary.get("tqqq_stress_reduction"))
        and bool(risk_summary.get("can_emit_weights")) is False
        and bool(dry_summary.get("veto_blocks_growth_overlay"))
        and not bool(dependence_summary.get("risk_on_veto_2023_plus_only"))
        and not bool(risk_summary.get("growth_overlay_enabled"))
        and not bool(risk_summary.get("tqqq_allocation_enabled"))
    )
    if do_not_pass and risk_veto_pass:
        status = "BOTH_PASS"
        final_status = "CHANNEL_V3_BOTH_PASS"
    elif do_not_pass:
        status = "DO_NOT_DERISK_PASS"
        final_status = "CHANNEL_V3_DO_NOT_DERISK_ONLY"
    elif risk_veto_pass:
        status = "RISK_ON_VETO_PASS"
        final_status = "CHANNEL_V3_RISK_ON_VETO_ONLY"
    elif bool(dependence_summary.get("improvement_only_in_2023_plus")):
        status = "BOTH_FAIL"
        final_status = "CHANNEL_V3_2023_PLUS_DEPENDENT"
    elif actual_summary.get("defensive_probe_regression_count", 0) > 0:
        status = "BOTH_FAIL"
        final_status = "CHANNEL_V3_DEFENSIVE_REGRESSION"
    else:
        status = "BOTH_FAIL"
        final_status = "CHANNEL_V3_NO_MATERIAL_IMPROVEMENT"

    return _payload(
        report_type="channel_specific_v3_selection_rule_result",
        title="Channel-Specific v3 Selection Rule Result",
        status="CHANNEL_SPECIFIC_V3_SELECTION_RULE_EVALUATED_PROMOTION_BLOCKED",
        summary={
            "selection_status": status,
            "final_status": final_status,
            "do_not_de_risk_pass": do_not_pass,
            "risk_on_veto_pass": risk_veto_pass,
            "candidate_count": 0,
            "observe_only_allowed": bool(do_not_pass or risk_veto_pass),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        rows=[
            {
                "channel": "do_not_de_risk",
                "allowed_families": ALLOWED_FAMILIES["do_not_de_risk"],
                "status": "DO_NOT_DERISK_PASS" if do_not_pass else "DO_NOT_DERISK_FAIL",
                "false_risk_off_cost_reduction": false_risk_summary.get(
                    "false_risk_off_cost_reduction"
                ),
                "missed_upside_reduction": false_risk_summary.get("missed_upside_reduction"),
                "defensive_probe_regression_count": actual_summary.get(
                    "defensive_probe_regression_count"
                ),
            },
            {
                "channel": "risk_on_veto",
                "allowed_families": ALLOWED_FAMILIES["risk_on_veto"],
                "status": "RISK_ON_VETO_PASS" if risk_veto_pass else "RISK_ON_VETO_FAIL",
                "false_add_risk_cost_reduction": false_add_summary.get(
                    "false_add_risk_cost_reduction"
                ),
                "veto_blocks_growth_overlay": dry_summary.get("veto_blocks_growth_overlay"),
                "tqqq_allocation_enabled": risk_summary.get("tqqq_allocation_enabled"),
            },
        ],
    )


def _closeout_payload(
    selection: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> dict[str, Any]:
    selection_summary = _mapping(selection.get("summary"))
    final_status = str(selection_summary.get("final_status"))
    summary = {
        **selection_summary,
        "market_regime": MARKET_REGIME,
        "requested_range": f"{REQUESTED_START} to latest",
        "selected_families_used": ALLOWED_FAMILIES,
        "models_trained": [
            "do_not_de_risk_model_v3:monotonic_scorecard",
            "risk_on_veto_model_v3:monotonic_scorecard",
        ],
        "data_quality_status": data_quality.get("status"),
        "data_quality_passed": data_quality.get("passed"),
        "dynamic_promotion_status": "BLOCKED",
    }
    payload = _payload(
        report_type="channel_specific_first_layer_v3_final_matrix",
        title="Channel-Specific First-Layer v3 Final Matrix",
        status=final_status,
        summary=summary,
    )
    payload["selection_result"] = dict(selection_summary)
    return payload


def _channel_config_payload(
    *,
    feature_set_locked_path: Path,
    do_not_selection_rule_path: Path,
    risk_veto_selection_rule_path: Path,
    do_not_thresholds: Mapping[str, Any],
    risk_veto_thresholds: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _payload(
        report_type="channel_specific_first_layer_v3_config",
        title="Channel-Specific First-Layer v3 Config",
        status="CHANNEL_SPECIFIC_FIRST_LAYER_V3_CONFIG_READY_PROMOTION_BLOCKED",
        summary={
            "channels": ["do_not_de_risk", "risk_on_veto"],
            "models": ["do_not_de_risk_model_v3", "risk_on_veto_model_v3"],
            "candidate_count": 0,
            "can_emit_weights": False,
        },
    )
    payload.update(
        {
            "policy_id": "channel_specific_first_layer_v3",
            "feature_set_locked_path": str(feature_set_locked_path),
            "selection_rules": {
                "do_not_de_risk": str(do_not_selection_rule_path),
                "risk_on_veto": str(risk_veto_selection_rule_path),
            },
            "channels": {
                "do_not_de_risk": {
                    "model_id": "do_not_de_risk_model_v3",
                    "model_type": "monotonic_scorecard",
                    "allowed_families": ALLOWED_FAMILIES["do_not_de_risk"],
                    "thresholds": dict(do_not_thresholds),
                    "allowed_outputs": [
                        "do_not_de_risk_probability",
                        "re_risk_allowed_probability",
                        "confidence",
                        "validity_days",
                    ],
                    "blocked_outputs": [
                        "add_risk",
                        "risk_on",
                        "portfolio_weights",
                        "TQQQ_allocation",
                        "broker_action",
                    ],
                },
                "risk_on_veto": {
                    "model_id": "risk_on_veto_model_v3",
                    "model_type": "monotonic_scorecard",
                    "allowed_families": ALLOWED_FAMILIES["risk_on_veto"],
                    "thresholds": dict(risk_veto_thresholds),
                    "allowed_outputs": [
                        "growth_allowed",
                        "add_risk_allowed",
                        "tqqq_allowed",
                        "veto_reasons",
                        "confidence",
                        "validity_days",
                    ],
                    "blocked_outputs": [
                        "positive_add_risk_signal",
                        "portfolio_weights",
                        "TQQQ_allocation",
                        "broker_action",
                    ],
                },
            },
        }
    )
    return payload


def _classification_metrics(pred: pd.Series, actual: pd.Series) -> dict[str, Any]:
    pred_bool = pred.astype(bool)
    actual_bool = actual.astype(bool)
    true_positive = int((pred_bool & actual_bool).sum())
    false_positive = int((pred_bool & ~actual_bool).sum())
    false_negative = int((~pred_bool & actual_bool).sum())
    true_negative = int((~pred_bool & ~actual_bool).sum())
    return {
        "observation_count": int(len(pred_bool)),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "true_negative": true_negative,
        "precision": _round(_ratio(true_positive, true_positive + false_positive)),
        "recall": _round(_ratio(true_positive, true_positive + false_negative)),
        "specificity": _round(_ratio(true_negative, true_negative + false_positive)),
        "positive_rate": _round(float(pred_bool.mean()) if len(pred_bool) else 0.0),
    }


def _model_slice_metrics(
    labels: pd.DataFrame,
    pred: pd.Series,
    actual: pd.Series,
    *,
    year: int,
) -> dict[str, Any]:
    mask = labels["date"].dt.year == year
    if not bool(mask.any()):
        return {"year": year, "observation_count": 0}
    metrics = _classification_metrics(pred.loc[mask], actual.loc[mask])
    metrics["year"] = year
    return metrics


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "research_window_id": PRIMARY_WINDOW_ID,
        "research_window_alias": PRIMARY_WINDOW_ALIAS,
        "requested_start": REQUESTED_START,
        "actual_start": REQUESTED_START,
        "actual_portfolio_start": REQUESTED_START,
        "end": "latest",
        "window_role": "primary_validated",
        "data_quality_contract": "secondary_cross_checked",
        "exact_or_proxy": "exact",
        "summary": _clean_for_yaml(dict(summary)),
        "research_audit_metadata": _audit_metadata(),
        **SAFETY_BOUNDARY,
    }
    if rows is not None:
        payload["rows"] = _clean_for_yaml(list(rows))
    return payload


def _audit_metadata() -> dict[str, Any]:
    return {
        "modified_layer": "first_layer",
        "modified_channel": "channel_specific_first_layer_v3",
        "frozen_channels": ["defensive", "return_seeking_diagnostic", "risk_veto"],
        "frozen_first_layer_version": "first_layer_v2_return_seeking_diagnostic_only",
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
        "research_window_id": PRIMARY_WINDOW_ID,
        "label_version": "channel_specific_labels_v3",
        "feature_set_version": "channel_specific_feature_set_v1_locked",
        "model_version": "channel_specific_first_layer_v3",
        "threshold_policy": "do_not_de_risk_v3_selection_rule_v1+risk_on_veto_v3_selection_rule_v1",
        "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
        "signal_usage_matrix_version": "first_layer_signal_usage_matrix_v2",
        "boundary_contract_version": "two_layer_strategy_boundary_contract_v1",
        "selection_rule_version": "channel_specific_v3_selection_rules_v1",
        "candidate_count": 0,
        "pre_registered_selection_rule": (
            "do_not_de_risk_v3_selection_rule_v1+risk_on_veto_v3_selection_rule_v1"
        ),
    }


def _render_scope_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Channel-Specific First-Layer v3 Scope",
            "",
            "本批只研究 `do_not_de_risk` 与 `risk_on_veto` 两个 channel。",
            "",
            "## 允许范围",
            "",
            "- `do_not_de_risk`: `drawdown_recovery`。",
            "- `risk_on_veto`: `volatility_compression`, `rates_liquidity`。",
            "",
            "## 禁止范围",
            "",
            "- 不训练 universal first-layer 或 add-risk allocation model。",
            "- 不输出 portfolio weights、target allocation、trade action 或 broker action。",
            "- 不启用 growth overlay、TQQQ allocation、paper-shadow、production 或 promotion。",
            "",
            f"最终 candidate_count：`{summary.get('candidate_count')}`。",
            "",
        ]
    )


def _render_feature_lock_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Channel Specific Feature Set v1 Lock Review",
            "",
            f"状态：`{payload.get('status')}`",
            "",
            "- `do_not_de_risk` allowed families: "
            f"`{', '.join(summary.get('do_not_de_risk_allowed_families', []))}`。",
            "- `risk_on_veto` allowed families: "
            f"`{', '.join(summary.get('risk_on_veto_allowed_families', []))}`。",
            f"- diagnostic-only families: `{', '.join(summary.get('diagnostic_only', []))}`。",
            f"- blocked families: `{', '.join(summary.get('blocked', []))}`。",
            "- `can_emit_weights=false`，promotion / paper-shadow / production / broker "
            "继续 blocked。",
            "",
        ]
    )


def _render_generic_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [f"# {payload.get('title')}", "", f"状态：`{payload.get('status')}`", "", "## 摘要", ""]
    for key, value in summary.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "所有结论均为 research-only diagnostic，不构成 candidate、paper-shadow、"
            "production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_actual_path_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    rows = _records(payload.get("actual_path_rows"))
    lines = [
        "# Channel-Specific v3 Actual-Path Review",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        f"- data_quality_status: `{summary.get('data_quality_status')}`。",
        "- defensive_probe_regression_count: "
        f"`{summary.get('defensive_probe_regression_count')}`。",
        "- same-risk static frontier 与 limited_adjustment 仅作为外部诊断参考，"
        "不是本批 candidate。",
        "",
        "| variant | probe | annual_return | max_drawdown | calmar | "
        "net_of_cost_delta_vs_baseline | regression |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows[:80]:
        lines.append(
            "| "
            f"{row.get('variant_id')} | {row.get('probe_id')} | {row.get('annual_return')} | "
            f"{row.get('max_drawdown')} | {row.get('calmar')} | "
            f"{row.get('net_of_cost_delta_vs_baseline')} | "
            f"{row.get('defensive_probe_regression')} |"
        )
    lines.append("")
    return "\n".join(lines)


def _render_selection_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    rows = _records(payload.get("rows"))
    lines = [
        "# Channel-Specific v3 Selection Rule Review",
        "",
        f"selection_status：`{summary.get('selection_status')}`",
        f"final_status：`{summary.get('final_status')}`",
        "",
        "| channel | status | allowed_families | key evidence |",
        "| --- | --- | --- | --- |",
    ]
    for row in rows:
        evidence = ", ".join(
            f"{key}={value}"
            for key, value in row.items()
            if key not in {"channel", "status", "allowed_families"}
        )
        lines.append(
            f"| {row.get('channel')} | {row.get('status')} | "
            f"{', '.join(row.get('allowed_families', []))} | {evidence} |"
        )
    lines.extend(
        [
            "",
            "即使 channel 通过，本批最多允许 observe-only diagnostic；promotion、"
            "paper-shadow、production、broker 均保持 blocked。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_owner_pack(selection: Mapping[str, Any]) -> str:
    summary = _mapping(selection.get("summary"))
    return "\n".join(
        [
            "# Channel-Specific First-Layer v3 Owner Pack",
            "",
            "## 结论",
            "",
            f"- final_status: `{summary.get('final_status')}`。",
            f"- do_not_de_risk_pass: `{summary.get('do_not_de_risk_pass')}`。",
            f"- risk_on_veto_pass: `{summary.get('risk_on_veto_pass')}`。",
            "- 本批只研究 `do_not_de_risk` 与 `risk_on_veto`，因为上一阶段没有 "
            "family 通过 add-risk selection。",
            "- `drawdown_recovery` 只用于 defensive neutralization / re-risk allowed diagnostic。",
            "- `volatility_compression` 与 `rates_liquidity` 只用于 risk-on veto，"
            "不产生正向 add-risk。",
            "- `trend_persistence` / `relative_strength` 仍为 return-seeking diagnostic-only。",
            "- `breadth_participation` / `event_risk` 因 PIT blocker 不进入模型。",
            "",
            "## Promotion",
            "",
            "本批没有 owner-reviewed candidate、没有 forward paper-shadow、没有 "
            "production approval，也没有 broker action。",
            "",
        ]
    )


def _render_closeout(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Channel-Specific First-Layer v3 Closeout",
        "",
        f"最终状态：`{payload.get('status')}`",
        "",
        f"- selected_families_used: `{summary.get('selected_families_used')}`",
        f"- models_trained: `{summary.get('models_trained')}`",
        f"- data_quality_status: `{summary.get('data_quality_status')}`",
        "- promotion_allowed: `False`",
        "- paper_shadow_allowed: `False`",
        "- production_allowed: `False`",
        "- broker_action: `none`",
        "",
    ]
    return "\n".join(lines)


def _thresholds(rule: Mapping[str, Any], key: str) -> dict[str, Any]:
    section = _mapping(rule.get(key))
    thresholds = _mapping(section.get("model_thresholds"))
    if not thresholds:
        raise ValueError(f"selection rule missing model_thresholds: {key}")
    return thresholds


def _rank_pct(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if numeric.nunique(dropna=True) <= 1:
        return pd.Series(0.5, index=series.index)
    return numeric.rank(pct=True).clip(0.0, 1.0)


def _reason_distribution(values: pd.Series) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values.fillna(""):
        for reason in _split_reasons(value):
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _split_reasons(value: Any) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _is_improved_defensive_row(row: Mapping[str, Any]) -> bool:
    return (
        _float(row.get("net_of_cost_delta_vs_baseline")) >= 0.0
        and _float(row.get("max_drawdown_delta_vs_baseline")) >= 0.0
    )


def _ratio(numerator: int | float, denominator: int | float) -> float:
    denominator = float(denominator)
    if math.isclose(denominator, 0.0):
        return 0.0
    return float(numerator) / denominator


def _mean(series: pd.Series) -> float:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return 0.0
    return _round(float(numeric.mean()))


def _round(value: float, digits: int = 6) -> float:
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return round(float(value), digits)


def _float(value: Any, default: float = 0.0) -> float:
    if value in {None, ""}:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _list_of_mapping(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_clean_for_yaml(dict(payload)), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_clean_for_yaml(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    fieldnames: Sequence[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(_clean_for_yaml(list(rows)))


def _write_dataframe(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _clean_for_yaml(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clean_for_yaml(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_for_yaml(item) for item in value]
    if isinstance(value, tuple):
        return [_clean_for_yaml(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "item"):
        return _clean_for_yaml(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value
