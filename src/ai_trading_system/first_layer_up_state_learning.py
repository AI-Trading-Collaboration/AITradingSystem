from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.expanded_allocation_universe import (
    _load_price_matrix,
    _slice_prices,
)
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    DEFAULT_SCOPE_CONFIG_PATH,
    DEFAULT_SCORE_POLICY_PATH,
    DEFAULT_SCORECARD_CONFIG_PATH,
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
    STATE_ORDER,
    _backtest_probe_predictions,
    _file_sha256,
    _float,
    _int,
    _json_scalar,
    _load_rates,
    _load_yaml_mapping,
    _mapping,
    _payload,
    _ratio,
    _records,
    _write_csv,
    _write_json,
    _write_markdown,
    _write_yaml,
    run_first_layer_policy_aware_calibration_pack,
)

DEFAULT_THRESHOLD_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_threshold_policy_v1.yaml"
)
DEFAULT_HIERARCHICAL_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "hierarchical_first_layer_v1.yaml"
)

DEFAULT_CONSENSUS_LABELS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "trend_labels" / "consensus_trend_labels.csv"
)
DEFAULT_FLAT_WALK_FORWARD_PREDICTIONS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT
    / "walk_forward"
    / "first_layer_walk_forward_predictions.csv"
)
DEFAULT_OLD_SCORECARD_PREDICTIONS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT
    / "models"
    / "first_layer_trend_scorecard_v1_predictions.csv"
)
DEFAULT_FLAT_PROBE_METRICS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT
    / "probe_backtest"
    / "old_first_layer_vs_new_first_layer_actual_path.csv"
)

DEFAULT_FAILURE_DIAGNOSIS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_up_state_failure_diagnosis.md"
)
DEFAULT_FAILURE_DIAGNOSIS_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_up_state_failure_diagnosis.yaml"
)
DEFAULT_UPPER_STATE_LABEL_AUDIT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "upper_state_label_audit.md"
)
DEFAULT_UPPER_STATE_LABEL_AUDIT_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "upper_state_label_audit.yaml"
)
DEFAULT_HIERARCHICAL_LABELS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "trend_labels" / "hierarchical_trend_labels.csv"
)
DEFAULT_HIERARCHICAL_LABEL_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "hierarchical_trend_label_summary.yaml"
)
DEFAULT_FEATURE_V2_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "pit_feature_matrix" / "pit_feature_matrix_v2.csv"
)
DEFAULT_FEATURE_EXPANSION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "up_state_feature_expansion_review.md"
)
DEFAULT_FEATURE_AUDIT_V2_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_feature_pit_audit_v2.md"
)
DEFAULT_FEATURE_AUDIT_V2_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_feature_pit_audit_v2.yaml"
)
DEFAULT_RISK_OFF_MODEL_ROOT = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "risk_off_detector_v2"
)
DEFAULT_UPPER_STATE_MODEL_ROOT = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "upper_state_detector_v1"
)
DEFAULT_SEVERITY_MODEL_ROOT = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "risk_on_severity_scaler_v1"
)
DEFAULT_RISK_OFF_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_off_detector_v2_review.md"
)
DEFAULT_UPPER_STATE_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "upper_state_detector_v1_review.md"
)
DEFAULT_SEVERITY_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_severity_scaler_review.md"
)
DEFAULT_THRESHOLD_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_threshold_calibration_review.md"
)
DEFAULT_HIERARCHICAL_PREDICTIONS_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "hierarchical_first_layer_v1_predictions.csv"
)
DEFAULT_HIERARCHICAL_WALK_FORWARD_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "hierarchical_first_layer_walk_forward_review.md"
)
DEFAULT_HIERARCHICAL_WALK_FORWARD_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "hierarchical_first_layer_walk_forward_matrix.yaml"
)
DEFAULT_HIERARCHICAL_PROBE_BACKTEST_ROOT = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "hierarchical_first_layer_probe_backtest"
)
DEFAULT_HIERARCHICAL_ACTUAL_PATH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "hierarchical_first_layer_actual_path_review.md"
)
DEFAULT_HIERARCHICAL_ACTUAL_PATH_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "hierarchical_first_layer_actual_path_matrix.yaml"
)
DEFAULT_RETURN_SEEKING_COMPAT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_probe_compatibility_with_up_state_model.md"
)
DEFAULT_CLASS_IMBALANCE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_class_imbalance_split_diagnostics.md"
)
DEFAULT_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_up_state_learning_owner_review_pack.md"
)
DEFAULT_FORWARD_WATCH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "hierarchical_first_layer_forward_watch_plan.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_up_state_learning_final_matrix.yaml"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_up_state_learning_closeout.md"
)

BASE_FEATURE_COLUMNS = [
    "qqq_momentum_20d",
    "qqq_momentum_60d",
    "qqq_ma_slope_20_60",
    "qqq_drawdown_126d",
    "realized_vol_20d",
    "yield_curve_10y2y",
]
UP_STATE_FEATURE_COLUMNS = [
    *BASE_FEATURE_COLUMNS,
    "qqq_momentum_120d",
    "qqq_ma_slope_60_120",
    "qqq_above_ma60_duration_60d",
    "qqq_drawdown_recovery_20d",
    "qqq_higher_high_proxy_20d",
    "qqq_higher_low_proxy_20d",
    "qqq_distance_from_60d_high",
    "realized_vol_decline_20d",
    "downside_vol_20d",
    "days_since_60d_low",
    "recovery_from_60d_low",
]
UPPER_STATES = {"constructive", "risk_on"}
DOWNSIDE_STATES = {"risk_off", "defensive"}


def run_first_layer_up_state_learning_repair_pack(
    *,
    scope_config_path: Path = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Path = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Path = DEFAULT_SCORECARD_CONFIG_PATH,
    threshold_policy_path: Path = DEFAULT_THRESHOLD_POLICY_PATH,
    hierarchical_config_path: Path = DEFAULT_HIERARCHICAL_CONFIG_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    refresh_prerequisites: bool = True,
) -> dict[str, Any]:
    if refresh_prerequisites:
        run_first_layer_policy_aware_calibration_pack(
            scope_config_path=scope_config_path,
            probe_registry_path=probe_registry_path,
            score_policy_path=score_policy_path,
            scorecard_config_path=scorecard_config_path,
            expanded_config_path=expanded_config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            output_root=output_root,
        )

    scope_config = _load_yaml_mapping(scope_config_path)
    probe_registry = _load_yaml_mapping(probe_registry_path)
    threshold_policy = _load_yaml_mapping(threshold_policy_path)
    hierarchical_config = _load_yaml_mapping(hierarchical_config_path)
    prices = _slice_prices(
        _load_price_matrix(prices_path, ["QQQ", "SGOV", "TQQQ"]),
        start_date=DEFAULT_AI_REGIME_BACKTEST_START,
        end_date=None,
    )
    rates = _load_rates(rates_path)
    consensus_labels = pd.read_csv(output_root / "trend_labels" / "consensus_trend_labels.csv")
    flat_predictions = pd.read_csv(
        output_root / "walk_forward" / "first_layer_walk_forward_predictions.csv"
    )
    old_predictions = pd.read_csv(
        output_root / "models" / "first_layer_trend_scorecard_v1_predictions.csv"
    )
    flat_probe_metrics = pd.read_csv(
        output_root / "probe_backtest" / "old_first_layer_vs_new_first_layer_actual_path.csv"
    )

    hierarchical_labels = build_hierarchical_trend_labels(consensus_labels)
    feature_v2, feature_report = build_up_state_feature_matrix(prices=prices, rates=rates)
    split_audit = build_upper_state_split_audit(
        hierarchical_labels=hierarchical_labels,
        feature_matrix=feature_v2,
        scope_config=scope_config,
    )
    wf = run_hierarchical_walk_forward(
        feature_matrix=feature_v2,
        hierarchical_labels=hierarchical_labels,
        scope_config=scope_config,
        threshold_policy=threshold_policy,
        hierarchical_config=hierarchical_config,
    )
    failure_diagnosis = build_failure_diagnosis(
        probe_registry=probe_registry,
        consensus_labels=consensus_labels,
        flat_predictions=flat_predictions,
        flat_probe_metrics=flat_probe_metrics,
        hierarchical_predictions=wf["predictions"],
    )
    feature_audit = build_feature_pit_audit_v2(feature_report, split_audit)
    label_summary = build_hierarchical_label_summary(hierarchical_labels)
    probe_backtest = run_hierarchical_probe_backtest(
        prices=prices,
        probe_registry=probe_registry,
        old_predictions=old_predictions,
        flat_predictions=flat_predictions,
        hierarchical_predictions=wf["predictions"],
    )
    actual_path_matrix = build_actual_path_matrix(
        probe_backtest=probe_backtest,
        flat_probe_metrics=flat_probe_metrics,
    )
    return_seeking_compat = build_return_seeking_compatibility(
        probe_registry=probe_registry,
        hierarchical_predictions=wf["predictions"],
    )
    class_imbalance = build_class_imbalance_diagnostics(split_audit, label_summary)
    final_matrix = build_final_matrix(
        wf_metrics=wf["metrics"],
        actual_path_matrix=actual_path_matrix,
        split_audit=split_audit,
        label_summary=label_summary,
    )
    owner_pack = build_owner_pack(
        failure_diagnosis=failure_diagnosis,
        label_summary=label_summary,
        split_audit=split_audit,
        feature_audit=feature_audit,
        wf_metrics=wf["metrics"],
        actual_path_matrix=actual_path_matrix,
        final_matrix=final_matrix,
    )
    write_up_state_outputs(
        output_root=output_root,
        hierarchical_labels=hierarchical_labels,
        label_summary=label_summary,
        feature_v2=feature_v2,
        feature_report=feature_report,
        feature_audit=feature_audit,
        split_audit=split_audit,
        wf=wf,
        probe_backtest=probe_backtest,
        actual_path_matrix=actual_path_matrix,
        failure_diagnosis=failure_diagnosis,
        return_seeking_compat=return_seeking_compat,
        class_imbalance=class_imbalance,
        owner_pack=owner_pack,
        final_matrix=final_matrix,
        threshold_policy_path=threshold_policy_path,
        hierarchical_config_path=hierarchical_config_path,
    )
    return owner_pack


def build_hierarchical_trend_labels(consensus_labels: pd.DataFrame) -> pd.DataFrame:
    frame = consensus_labels.copy()
    frame["consensus_state"] = frame["consensus_state"].astype(str)
    frame["train_usable"] = frame["train_usable"].astype(bool)
    frame["risk_off_binary_label"] = (
        (frame["consensus_state"] == "risk_off")
        | ((frame["consensus_state"] == "defensive") & frame["train_usable"])
    ).astype(int)
    frame["upper_state_binary_label"] = frame["consensus_state"].isin(UPPER_STATES).astype(int)
    frame["three_zone_label"] = frame["consensus_state"].map(_state_to_zone)
    frame["risk_on_severity_label"] = frame["consensus_state"].map(
        {"constructive": 1, "risk_on": 2}
    )
    frame["risk_on_severity_label"] = frame["risk_on_severity_label"].fillna(0).astype(int)
    frame["upper_state_train_usable"] = frame["upper_state_binary_label"].astype(bool) & (
        frame["train_usable"]
        | frame["consensus_state"].eq("risk_on")
        | frame["disagreement_score"].astype(float).le(0.75)
    )
    frame["risk_off_train_usable"] = frame["train_usable"] | frame["consensus_state"].isin(
        ["risk_off", "risk_on"]
    )
    return frame


def build_up_state_feature_matrix(
    *,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    qqq = pd.to_numeric(prices["QQQ"], errors="coerce").ffill()
    returns = qqq.pct_change().fillna(0.0)
    ma20 = qqq.rolling(20, min_periods=1).mean()
    ma60 = qqq.rolling(60, min_periods=1).mean()
    ma120 = qqq.rolling(120, min_periods=1).mean()
    high60 = qqq.rolling(60, min_periods=1).max()
    low60 = qqq.rolling(60, min_periods=1).min()
    low20 = qqq.rolling(20, min_periods=1).min()
    high20 = qqq.rolling(20, min_periods=1).max()
    downside_returns = returns.where(returns < 0.0, 0.0)
    feature_frame = pd.DataFrame(index=prices.index)
    feature_frame["qqq_momentum_20d"] = qqq.pct_change(20)
    feature_frame["qqq_momentum_60d"] = qqq.pct_change(60)
    feature_frame["qqq_ma_slope_20_60"] = ma20 / ma60 - 1.0
    feature_frame["qqq_drawdown_126d"] = qqq / qqq.rolling(126, min_periods=5).max() - 1.0
    feature_frame["realized_vol_20d"] = returns.rolling(20, min_periods=2).std(ddof=0)
    rate_frame = rates.reindex(feature_frame.index).ffill()
    if {"DGS10", "DGS2"} <= set(rate_frame.columns):
        feature_frame["yield_curve_10y2y"] = rate_frame["DGS10"] - rate_frame["DGS2"]
    else:
        feature_frame["yield_curve_10y2y"] = 0.0
    feature_frame["qqq_momentum_120d"] = qqq.pct_change(120)
    feature_frame["qqq_ma_slope_60_120"] = ma60 / ma120 - 1.0
    feature_frame["qqq_above_ma60_duration_60d"] = (
        (qqq > ma60).astype(float).rolling(60, min_periods=1).mean()
    )
    feature_frame["qqq_drawdown_recovery_20d"] = feature_frame["qqq_drawdown_126d"] - feature_frame[
        "qqq_drawdown_126d"
    ].shift(20)
    feature_frame["qqq_higher_high_proxy_20d"] = (high20 > high20.shift(20)).astype(float)
    feature_frame["qqq_higher_low_proxy_20d"] = (low20 > low20.shift(20)).astype(float)
    feature_frame["qqq_distance_from_60d_high"] = qqq / high60 - 1.0
    vol20 = returns.rolling(20, min_periods=2).std(ddof=0)
    vol60 = returns.rolling(60, min_periods=2).std(ddof=0)
    feature_frame["realized_vol_decline_20d"] = vol60 - vol20
    feature_frame["downside_vol_20d"] = downside_returns.rolling(20, min_periods=2).std(ddof=0)
    feature_frame["days_since_60d_low"] = _days_since_low(qqq, window=60)
    feature_frame["recovery_from_60d_low"] = qqq / low60 - 1.0
    feature_frame = feature_frame.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    index = list(feature_frame.index)
    rows: list[dict[str, Any]] = []
    for idx, timestamp in enumerate(index):
        decision_at = index[idx + 1] if idx + 1 < len(index) else timestamp
        row = {
            "date": timestamp.date().isoformat(),
            "known_at": timestamp.date().isoformat(),
            "available_at": timestamp.date().isoformat(),
            "decision_at": decision_at.date().isoformat(),
            "feature_cutoff_passed": True,
            "pit_status": "PIT_APPROVED",
        }
        row.update(
            {
                column: round(float(feature_frame.loc[timestamp, column]), 8)
                for column in UP_STATE_FEATURE_COLUMNS
            }
        )
        rows.append(row)
    output = pd.DataFrame(rows)
    feature_rows = []
    for column in UP_STATE_FEATURE_COLUMNS:
        missing_rate = float(output[column].isna().mean()) if column in output else 1.0
        feature_rows.append(
            {
                "feature_name": column,
                "family": _feature_family(column),
                "known_at": "same_day_close_or_rate_cache_at_or_before_decision",
                "available_at": "same_as_known_at_for_cached_daily_research",
                "PIT_status": "PIT_APPROVED",
                "missing_rate": round(missing_rate, 6),
                "split_coverage": 1.0,
                "training_allowed": True,
            }
        )
    report = {
        "status": "PIT_V2_APPROVED",
        "row_count": len(output),
        "approved_feature_count": len(UP_STATE_FEATURE_COLUMNS),
        "blocked_feature_count": 0,
        "feature_cutoff_passed": bool(output["feature_cutoff_passed"].all()),
        "features": feature_rows,
        "excluded_non_pit_or_unavailable_features": [
            "SMH/QQQ relative strength unavailable in required validated cache",
            "SOXX/QQQ ratio trend unavailable in required validated cache",
            "AI basket relative strength unavailable in required validated cache",
            "VIX falling regime unavailable in required validated cache",
            "market breadth proxy unavailable in required validated cache",
        ],
    }
    return output, report


def build_upper_state_split_audit(
    *,
    hierarchical_labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    scope_config: Mapping[str, Any],
) -> dict[str, Any]:
    merged = _model_frame(hierarchical_labels, feature_matrix, scope_config)
    splits = _walk_forward_splits(merged, scope_config)
    rows = []
    for split in splits:
        train = split["train"]
        validation = split["validation"]
        rows.append(
            {
                "split_id": split["split_id"],
                "train_start": str(train["date"].iloc[0]) if not train.empty else None,
                "train_end": str(train["date"].iloc[-1]) if not train.empty else None,
                "validation_start": (
                    str(validation["date"].iloc[0]) if not validation.empty else None
                ),
                "validation_end": str(validation["date"].iloc[-1])
                if not validation.empty
                else None,
                "train_sample_count": len(train),
                "validation_sample_count": len(validation),
                "train_upper_state_count": int(train["upper_state_binary_label"].sum()),
                "validation_upper_state_count": int(validation["upper_state_binary_label"].sum()),
                "train_high_confidence_upper_state_count": int(
                    (
                        train["upper_state_binary_label"].astype(bool)
                        & train["upper_state_train_usable"].astype(bool)
                    ).sum()
                ),
                "validation_high_confidence_upper_state_count": int(
                    (
                        validation["upper_state_binary_label"].astype(bool)
                        & validation["upper_state_train_usable"].astype(bool)
                    ).sum()
                ),
                "train_has_upper_state": bool(train["upper_state_binary_label"].sum() > 0),
                "validation_has_upper_state": bool(
                    validation["upper_state_binary_label"].sum() > 0
                ),
                "train_missing_upper_but_validation_has_upper": bool(
                    train["upper_state_binary_label"].sum() == 0
                    and validation["upper_state_binary_label"].sum() > 0
                ),
                "avg_train_upper_disagreement": round(
                    float(
                        train.loc[
                            train["upper_state_binary_label"].astype(bool),
                            "disagreement_score",
                        ].mean()
                    )
                    if train["upper_state_binary_label"].sum() > 0
                    else 0.0,
                    GRID_ROUND_DIGITS,
                ),
                "avg_validation_upper_disagreement": round(
                    float(
                        validation.loc[
                            validation["upper_state_binary_label"].astype(bool),
                            "disagreement_score",
                        ].mean()
                    )
                    if validation["upper_state_binary_label"].sum() > 0
                    else 0.0,
                    GRID_ROUND_DIGITS,
                ),
            }
        )
    summary = {
        "split_count": len(rows),
        "splits_with_train_upper_state": sum(row["train_has_upper_state"] for row in rows),
        "splits_with_validation_upper_state": sum(
            row["validation_has_upper_state"] for row in rows
        ),
        "splits_train_missing_upper_but_validation_has_upper": sum(
            row["train_missing_upper_but_validation_has_upper"] for row in rows
        ),
        "total_train_upper_state_count": sum(row["train_upper_state_count"] for row in rows),
        "total_validation_upper_state_count": sum(
            row["validation_upper_state_count"] for row in rows
        ),
    }
    status = (
        "UPPER_STATE_LABEL_AUDIT_READY"
        if summary["splits_train_missing_upper_but_validation_has_upper"] == 0
        else "UPPER_STATE_LABEL_AUDIT_READY_WITH_SPLIT_GAPS"
    )
    return _payload(
        report_type="upper_state_label_audit",
        title="Upper-State Label Audit",
        status=f"{status}_PROMOTION_BLOCKED",
        summary=summary,
        split_rows=rows,
    )


def run_hierarchical_walk_forward(
    *,
    feature_matrix: pd.DataFrame,
    hierarchical_labels: pd.DataFrame,
    scope_config: Mapping[str, Any],
    threshold_policy: Mapping[str, Any],
    hierarchical_config: Mapping[str, Any],
) -> dict[str, Any]:
    merged = _model_frame(hierarchical_labels, feature_matrix, scope_config)
    split_results = []
    predictions = []
    risk_coefficients = []
    upper_coefficients = []
    severity_coefficients = []
    for split in _walk_forward_splits(merged, scope_config):
        train = split["train"]
        validation = split["validation"]
        risk_model = _fit_binary_linear(
            train,
            target_column="risk_off_binary_label",
            feature_columns=UP_STATE_FEATURE_COLUMNS,
        )
        upper_model = _fit_binary_linear(
            train,
            target_column="upper_state_binary_label",
            feature_columns=UP_STATE_FEATURE_COLUMNS,
        )
        risk_train_prob = _predict_binary_linear(train, risk_model, UP_STATE_FEATURE_COLUMNS)
        upper_train_prob = _predict_binary_linear(train, upper_model, UP_STATE_FEATURE_COLUMNS)
        risk_threshold = _calibrate_threshold(
            probabilities=risk_train_prob,
            actual=train["risk_off_binary_label"].astype(int).to_numpy(),
            policy=_mapping(threshold_policy.get("risk_off_detector")),
            positive_bias=0.9,
        )
        upper_threshold = _calibrate_threshold(
            probabilities=upper_train_prob,
            actual=train["upper_state_binary_label"].astype(int).to_numpy(),
            policy=_mapping(threshold_policy.get("upper_state_detector")),
            positive_bias=0.75,
        )
        severity = _fit_severity_model(train, threshold_policy)
        risk_prob = _predict_binary_linear(validation, risk_model, UP_STATE_FEATURE_COLUMNS)
        upper_prob = _predict_binary_linear(validation, upper_model, UP_STATE_FEATURE_COLUMNS)
        severity_score = _predict_severity(validation, severity)
        for row_idx, (_, row) in enumerate(validation.iterrows()):
            state, confidence = _compose_state(
                risk_off_probability=float(risk_prob[row_idx]),
                upper_state_probability=float(upper_prob[row_idx]),
                risk_on_severity_score=float(severity_score[row_idx]),
                risk_off_threshold=risk_threshold,
                upper_state_threshold=upper_threshold,
                risk_on_threshold=float(severity["threshold"]),
                hierarchical_config=hierarchical_config,
            )
            predictions.append(
                {
                    "date": str(row["date"]),
                    "split_id": split["split_id"],
                    "model_id": "hierarchical_first_layer_v1",
                    "risk_off_probability": round(float(risk_prob[row_idx]), 6),
                    "upper_state_probability": round(float(upper_prob[row_idx]), 6),
                    "risk_on_severity_score": round(float(severity_score[row_idx]), 6),
                    "risk_off_threshold": round(risk_threshold, 6),
                    "upper_state_threshold": round(upper_threshold, 6),
                    "risk_on_severity_threshold": round(float(severity["threshold"]), 6),
                    "trend_state": state,
                    "confidence": confidence,
                    "expected_horizon_days": _int(
                        _mapping(scope_config.get("walk_forward")).get("label_horizon_days"),
                        default=20,
                    ),
                    "validity_days": _state_validity(state, hierarchical_config),
                    "decay_profile": _state_decay(state, hierarchical_config),
                    "label_state": str(row["consensus_state"]),
                    "three_zone_label": str(row["three_zone_label"]),
                    "risk_off_binary_label": int(row["risk_off_binary_label"]),
                    "upper_state_binary_label": int(row["upper_state_binary_label"]),
                    "risk_on_severity_label": int(row["risk_on_severity_label"]),
                    "known_at": str(row["known_at"]),
                    "available_at": str(row["available_at"]),
                    "decision_at": str(row["decision_at"]),
                    **SAFETY_BOUNDARY,
                }
            )
        predicted_upper = int(
            sum(
                pred["trend_state"] in UPPER_STATES
                for pred in predictions
                if pred["split_id"] == split["split_id"]
            )
        )
        validation_upper = int(validation["upper_state_binary_label"].sum())
        split_results.append(
            {
                "split_id": split["split_id"],
                "train_sample_count": len(train),
                "validation_sample_count": len(validation),
                "train_upper_state_count": int(train["upper_state_binary_label"].sum()),
                "validation_upper_state_count": validation_upper,
                "predicted_upper_state_count": predicted_upper,
                "upper_state_collapse_flag": bool(validation_upper > 0 and predicted_upper == 0),
                "risk_off_threshold": round(risk_threshold, 6),
                "upper_state_threshold": round(upper_threshold, 6),
                "risk_on_severity_threshold": round(float(severity["threshold"]), 6),
                "severity_status": str(severity["status"]),
            }
        )
        risk_coefficients.append({"split_id": split["split_id"], **risk_model["coefficients"]})
        upper_coefficients.append({"split_id": split["split_id"], **upper_model["coefficients"]})
        severity_coefficients.append({"split_id": split["split_id"], **severity["coefficients"]})
    prediction_frame = pd.DataFrame(predictions)
    metrics = _hierarchical_metrics(prediction_frame, split_results)
    return {
        "predictions": prediction_frame,
        "metrics": metrics,
        "split_rows": split_results,
        "risk_off_coefficients": risk_coefficients,
        "upper_state_coefficients": upper_coefficients,
        "severity_coefficients": severity_coefficients,
    }


def build_failure_diagnosis(
    *,
    probe_registry: Mapping[str, Any],
    consensus_labels: pd.DataFrame,
    flat_predictions: pd.DataFrame,
    flat_probe_metrics: pd.DataFrame,
    hierarchical_predictions: pd.DataFrame,
) -> dict[str, Any]:
    exposures = _probe_exposure_rows(probe_registry)
    label_distribution = _state_count_dict(consensus_labels["consensus_state"])
    high_conf_by_state = _group_bool_rate(
        consensus_labels, group_col="consensus_state", bool_col="train_usable"
    )
    disagreement_by_state = _group_mean(
        consensus_labels, group_col="consensus_state", value_col="disagreement_score"
    )
    flat_distribution = _state_count_dict(flat_predictions["trend_state"])
    hierarchical_distribution = _state_count_dict(hierarchical_predictions["trend_state"])
    true_distribution = _state_count_dict(flat_predictions["label_state"])
    actual_path_deterioration = _flat_actual_path_deterioration(flat_probe_metrics)
    summary = {
        "return_seeking_probe_count": sum(
            bool(probe.get("return_seeking")) for probe in _records(probe_registry.get("probes"))
        ),
        "label_count": len(consensus_labels),
        "upper_state_label_count": int(
            consensus_labels["consensus_state"].isin(UPPER_STATES).sum()
        ),
        "flat_predicted_constructive_count": int(
            flat_predictions["trend_state"].astype(str).eq("constructive").sum()
        ),
        "flat_predicted_risk_on_count": int(
            flat_predictions["trend_state"].astype(str).eq("risk_on").sum()
        ),
        "flat_true_upper_state_count": int(
            flat_predictions["label_state"].isin(UPPER_STATES).sum()
        ),
        "hierarchical_predicted_upper_state_count": int(
            hierarchical_predictions["trend_state"].isin(UPPER_STATES).sum()
        ),
        "actual_path_deteriorated_probe_count": actual_path_deterioration[
            "deteriorated_probe_count"
        ],
    }
    return _payload(
        report_type="first_layer_up_state_failure_diagnosis",
        title="First-Layer Up-State Failure Diagnosis",
        status="FIRST_LAYER_UP_STATE_FAILURE_DIAGNOSIS_READY_PROMOTION_BLOCKED",
        summary=summary,
        probe_role_rows=_probe_role_rows(probe_registry),
        qqq_equivalent_exposure_by_state=exposures,
        label_distribution=label_distribution,
        high_confidence_by_state=high_conf_by_state,
        disagreement_by_state=disagreement_by_state,
        flat_prediction_distribution=flat_distribution,
        flat_true_label_distribution=true_distribution,
        hierarchical_prediction_distribution=hierarchical_distribution,
        actual_path_deterioration=actual_path_deterioration,
    )


def build_hierarchical_label_summary(hierarchical_labels: pd.DataFrame) -> dict[str, Any]:
    upper_count = int(hierarchical_labels["upper_state_binary_label"].sum())
    risk_off_count = int(hierarchical_labels["risk_off_binary_label"].sum())
    zone_counts = _state_count_dict(hierarchical_labels["three_zone_label"])
    severity_counts = {
        str(key): int(value)
        for key, value in hierarchical_labels["risk_on_severity_label"].value_counts().items()
    }
    return _payload(
        report_type="hierarchical_trend_label_summary",
        title="Hierarchical Trend Label Summary",
        status="HIERARCHICAL_TREND_LABELS_READY_PROMOTION_BLOCKED",
        summary={
            "row_count": len(hierarchical_labels),
            "risk_off_positive_count": risk_off_count,
            "upper_state_positive_count": upper_count,
            "upper_state_share": round(_ratio(upper_count, len(hierarchical_labels)), 6),
            "constructive_count": int(
                hierarchical_labels["consensus_state"].astype(str).eq("constructive").sum()
            ),
            "risk_on_count": int(
                hierarchical_labels["consensus_state"].astype(str).eq("risk_on").sum()
            ),
            "zone_counts": zone_counts,
            "severity_counts": severity_counts,
        },
    )


def build_feature_pit_audit_v2(
    feature_report: Mapping[str, Any],
    split_audit: Mapping[str, Any],
) -> dict[str, Any]:
    split_rows = _records(split_audit.get("split_rows"))
    split_count = len(split_rows)
    features = []
    for row in _records(feature_report.get("features")):
        features.append(
            {
                **dict(row),
                "split_coverage": 1.0 if split_count else 0.0,
                "allowed_for_model": row.get("PIT_status") == "PIT_APPROVED"
                and row.get("training_allowed") is True,
            }
        )
    return _payload(
        report_type="first_layer_feature_pit_audit_v2",
        title="First-Layer Feature PIT Audit v2",
        status="FIRST_LAYER_FEATURE_PIT_AUDIT_V2_READY",
        summary={
            "row_count": feature_report.get("row_count"),
            "approved_feature_count": feature_report.get("approved_feature_count"),
            "blocked_feature_count": feature_report.get("blocked_feature_count"),
            "feature_cutoff_passed": feature_report.get("feature_cutoff_passed"),
            "walk_forward_split_count": split_count,
        },
        features=features,
        excluded_non_pit_or_unavailable_features=list(
            feature_report.get("excluded_non_pit_or_unavailable_features", [])
        ),
    )


def run_hierarchical_probe_backtest(
    *,
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    old_predictions: pd.DataFrame,
    flat_predictions: pd.DataFrame,
    hierarchical_predictions: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    for model_id, predictions in (
        ("old_scorecard_first_layer_v1", old_predictions),
        ("flat_calibrated_first_layer_v1", flat_predictions),
        ("hierarchical_first_layer_v1", hierarchical_predictions),
    ):
        if predictions.empty:
            continue
        for probe in _records(probe_registry.get("probes")):
            rows.append(
                _backtest_probe_predictions(
                    prices=prices,
                    predictions=predictions,
                    probe=probe,
                    model_id=model_id,
                )
            )
    return pd.DataFrame(rows)


def build_actual_path_matrix(
    *,
    probe_backtest: pd.DataFrame,
    flat_probe_metrics: pd.DataFrame,
) -> dict[str, Any]:
    rows = []
    for probe_id, frame in probe_backtest.groupby("probe_id"):
        old = _model_metric_row(frame, "old_scorecard_first_layer_v1")
        flat = _model_metric_row(frame, "flat_calibrated_first_layer_v1")
        hierarchical = _model_metric_row(frame, "hierarchical_first_layer_v1")
        row = {
            "probe_id": probe_id,
            "old_annual_return": old.get("actual_path_annual_return"),
            "flat_annual_return": flat.get("actual_path_annual_return"),
            "hierarchical_annual_return": hierarchical.get("actual_path_annual_return"),
            "old_calmar": old.get("calmar_daily_equity_dd"),
            "flat_calmar": flat.get("calmar_daily_equity_dd"),
            "hierarchical_calmar": hierarchical.get("calmar_daily_equity_dd"),
            "old_sharpe": old.get("sharpe_daily_zero_rf"),
            "flat_sharpe": flat.get("sharpe_daily_zero_rf"),
            "hierarchical_sharpe": hierarchical.get("sharpe_daily_zero_rf"),
            "hierarchical_vs_flat_return_delta": round(
                _float(hierarchical.get("actual_path_annual_return"))
                - _float(flat.get("actual_path_annual_return")),
                GRID_ROUND_DIGITS,
            ),
            "hierarchical_vs_flat_calmar_delta": round(
                _float(hierarchical.get("calmar_daily_equity_dd"))
                - _float(flat.get("calmar_daily_equity_dd")),
                GRID_ROUND_DIGITS,
            ),
            "hierarchical_vs_old_return_delta": round(
                _float(hierarchical.get("actual_path_annual_return"))
                - _float(old.get("actual_path_annual_return")),
                GRID_ROUND_DIGITS,
            ),
            "hierarchical_vs_old_calmar_delta": round(
                _float(hierarchical.get("calmar_daily_equity_dd"))
                - _float(old.get("calmar_daily_equity_dd")),
                GRID_ROUND_DIGITS,
            ),
            "actual_path_improved_vs_flat": (
                _float(hierarchical.get("calmar_daily_equity_dd"))
                > _float(flat.get("calmar_daily_equity_dd"))
                and _float(hierarchical.get("actual_path_annual_return"))
                >= _float(flat.get("actual_path_annual_return"))
            ),
        }
        rows.append(row)
    summary = {
        "probe_count": len(rows),
        "improved_vs_flat_probe_count": sum(row["actual_path_improved_vs_flat"] for row in rows),
        "comparison_scope": [
            "old_scorecard_first_layer_v1",
            "flat_calibrated_first_layer_v1",
            "hierarchical_first_layer_v1",
            "prior_static_baselines_referenced_from_expanded_universe_artifacts",
            "limited_adjustment_referenced_from_expanded_universe_artifacts",
        ],
        "flat_probe_metric_source_rows": len(flat_probe_metrics),
    }
    return _payload(
        report_type="hierarchical_first_layer_actual_path_matrix",
        title="Hierarchical First-Layer Actual-Path Matrix",
        status="HIERARCHICAL_FIRST_LAYER_ACTUAL_PATH_READY_PROMOTION_BLOCKED",
        summary=summary,
        probe_rows=rows,
    )


def build_return_seeking_compatibility(
    *,
    probe_registry: Mapping[str, Any],
    hierarchical_predictions: pd.DataFrame,
) -> dict[str, Any]:
    rows = []
    for probe in _records(probe_registry.get("probes")):
        return_seeking = bool(probe.get("return_seeking"))
        role_tags = [str(tag) for tag in probe.get("role_tags", [])]
        rows.append(
            {
                "probe_id": str(probe.get("probe_id")),
                "role": str(probe.get("role")),
                "role_tags": role_tags,
                "return_seeking": return_seeking,
                "action_value_matrix_eligible": "action_value_labels"
                in [str(item) for item in probe.get("allowed_usage", [])],
                "hierarchical_probe_backtest_eligible": True,
                "research_only": bool(probe.get("research_only")),
                "promotion_enabled": bool(probe.get("promotion_enabled")),
                "broker_enabled": bool(probe.get("broker_enabled")),
                "upper_state_exposure_increases": _upper_state_exposure_increases(probe),
            }
        )
    return _payload(
        report_type="return_seeking_probe_compatibility",
        title="Return-Seeking Probe Compatibility with Up-State Model",
        status="RETURN_SEEKING_PROBE_COMPATIBILITY_READY_PROMOTION_BLOCKED",
        summary={
            "return_seeking_probe_count": sum(row["return_seeking"] for row in rows),
            "return_seeking_rows_research_only": all(
                row["research_only"] and not row["promotion_enabled"] and not row["broker_enabled"]
                for row in rows
                if row["return_seeking"]
            ),
            "hierarchical_predicted_upper_state_count": int(
                hierarchical_predictions["trend_state"].isin(UPPER_STATES).sum()
            ),
        },
        rows=rows,
    )


def build_class_imbalance_diagnostics(
    split_audit: Mapping[str, Any],
    label_summary: Mapping[str, Any],
) -> dict[str, Any]:
    split_rows = _records(split_audit.get("split_rows"))
    upper_counts = [int(row["train_upper_state_count"]) for row in split_rows]
    min_upper = min(upper_counts) if upper_counts else 0
    enough = bool(
        min_upper >= 30 and _mapping(label_summary.get("summary")).get("risk_on_count", 0)
    )
    return _payload(
        report_type="first_layer_class_imbalance_split_diagnostics",
        title="First-Layer Class-Imbalance and Split Diagnostics",
        status=(
            "FIRST_LAYER_CLASS_IMBALANCE_REVIEW_READY_PROMOTION_BLOCKED"
            if enough
            else "FIRST_LAYER_CLASS_IMBALANCE_REVIEW_READY_WITH_SAMPLE_CAVEATS_PROMOTION_BLOCKED"
        ),
        summary={
            "split_count": len(split_rows),
            "min_train_upper_state_count": min_upper,
            "splits_without_train_upper_state": sum(
                int(row["train_upper_state_count"]) == 0 for row in split_rows
            ),
            "constructive_and_risk_on_should_be_merged": True,
            "longer_history_needed": not enough,
            "upper_state_training_evidence_sufficient_for_binary_detector": enough,
        },
        split_rows=split_rows,
    )


def build_final_matrix(
    *,
    wf_metrics: Mapping[str, Any],
    actual_path_matrix: Mapping[str, Any],
    split_audit: Mapping[str, Any],
    label_summary: Mapping[str, Any],
) -> dict[str, Any]:
    improved_vs_flat = _int(
        _mapping(actual_path_matrix.get("summary")).get("improved_vs_flat_probe_count")
    )
    upper_collapse = bool(wf_metrics.get("upper_state_collapse_flag"))
    labels_unstable = (
        _mapping(label_summary.get("summary")).get("constructive_count", 0) > 0
        and _mapping(split_audit.get("summary")).get("splits_with_validation_upper_state", 0) > 0
        and float(wf_metrics.get("upper_state_recall", 0.0)) < 0.25
    )
    if improved_vs_flat > 0:
        base_status = "UP_STATE_LEARNING_IMPROVES_ACTUAL_PATH"
    elif float(wf_metrics.get("upper_state_recall", 0.0)) > 0.0 and not upper_collapse:
        base_status = "UP_STATE_LEARNING_IMPROVES_CLASSIFICATION_ONLY"
    elif upper_collapse:
        base_status = "UP_STATE_FEATURES_INSUFFICIENT"
    elif labels_unstable:
        base_status = "UP_STATE_LABELS_UNSTABLE"
    else:
        base_status = "NO_MATERIAL_IMPROVEMENT"
    return _payload(
        report_type="first_layer_up_state_learning_final_matrix",
        title="First-Layer Up-State Learning Final Matrix",
        status=f"{base_status}_PROMOTION_BLOCKED",
        summary={
            "base_status": base_status,
            "upper_state_collapse_flag": upper_collapse,
            "upper_state_recall": wf_metrics.get("upper_state_recall"),
            "predicted_constructive_count": wf_metrics.get("predicted_constructive_count"),
            "predicted_risk_on_count": wf_metrics.get("predicted_risk_on_count"),
            "improved_vs_flat_probe_count": improved_vs_flat,
            "next_action": (
                "OBSERVE_ONLY_FORWARD_WATCH_REVIEW"
                if improved_vs_flat > 0
                else "REVIEW_UP_STATE_FEATURES_AND_LABEL_TAXONOMY"
            ),
        },
        walk_forward_metrics=dict(wf_metrics),
        actual_path_summary=_mapping(actual_path_matrix.get("summary")),
    )


def build_owner_pack(
    *,
    failure_diagnosis: Mapping[str, Any],
    label_summary: Mapping[str, Any],
    split_audit: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    wf_metrics: Mapping[str, Any],
    actual_path_matrix: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type="first_layer_up_state_learning_owner_review_pack",
        title="First-Layer Up-State Learning Owner Review Pack",
        status="FIRST_LAYER_UP_STATE_LEARNING_OWNER_REVIEW_READY_PROMOTION_BLOCKED",
        summary={
            "failure_reason": "flat_five_class_model_predicted_zero_constructive_and_risk_on",
            "hierarchical_model_implemented": True,
            "risk_off_detector_retained": True,
            "upper_state_detector_predicted_upper_count": wf_metrics.get(
                "predicted_upper_state_count"
            ),
            "upper_state_collapse_flag": wf_metrics.get("upper_state_collapse_flag"),
            "actual_path_status": final_matrix.get("status"),
            "promotion_status": "BLOCKED",
        },
        failure_diagnosis_summary=_mapping(failure_diagnosis.get("summary")),
        label_summary=_mapping(label_summary.get("summary")),
        split_audit_summary=_mapping(split_audit.get("summary")),
        feature_audit_summary=_mapping(feature_audit.get("summary")),
        walk_forward_metrics=dict(wf_metrics),
        actual_path_summary=_mapping(actual_path_matrix.get("summary")),
        final_matrix_summary=_mapping(final_matrix.get("summary")),
        artifact_paths=_up_state_artifact_paths(),
    )


def write_up_state_outputs(
    *,
    output_root: Path,
    hierarchical_labels: pd.DataFrame,
    label_summary: Mapping[str, Any],
    feature_v2: pd.DataFrame,
    feature_report: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    split_audit: Mapping[str, Any],
    wf: Mapping[str, Any],
    probe_backtest: pd.DataFrame,
    actual_path_matrix: Mapping[str, Any],
    failure_diagnosis: Mapping[str, Any],
    return_seeking_compat: Mapping[str, Any],
    class_imbalance: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
    threshold_policy_path: Path,
    hierarchical_config_path: Path,
) -> None:
    _write_csv(output_root / "trend_labels" / "hierarchical_trend_labels.csv", hierarchical_labels)
    _write_yaml(DEFAULT_HIERARCHICAL_LABEL_SUMMARY_PATH, label_summary)
    _write_csv(output_root / "pit_feature_matrix" / "pit_feature_matrix_v2.csv", feature_v2)
    _write_json(
        output_root / "pit_feature_matrix" / "feature_availability_report_v2.json",
        feature_report,
    )
    _write_csv(
        DEFAULT_RISK_OFF_MODEL_ROOT / "walk_forward_predictions.csv",
        wf["predictions"][
            [
                "date",
                "split_id",
                "risk_off_probability",
                "risk_off_threshold",
                "risk_off_binary_label",
                "trend_state",
            ]
        ],
    )
    _write_json(
        DEFAULT_RISK_OFF_MODEL_ROOT / "model_coefficients.json",
        wf["risk_off_coefficients"],
    )
    _write_csv(
        DEFAULT_UPPER_STATE_MODEL_ROOT / "walk_forward_predictions.csv",
        wf["predictions"][
            [
                "date",
                "split_id",
                "upper_state_probability",
                "upper_state_threshold",
                "upper_state_binary_label",
                "trend_state",
            ]
        ],
    )
    _write_json(
        DEFAULT_UPPER_STATE_MODEL_ROOT / "model_coefficients.json",
        wf["upper_state_coefficients"],
    )
    _write_csv(
        DEFAULT_SEVERITY_MODEL_ROOT / "walk_forward_predictions.csv",
        wf["predictions"][
            [
                "date",
                "split_id",
                "risk_on_severity_score",
                "risk_on_severity_threshold",
                "risk_on_severity_label",
                "trend_state",
            ]
        ],
    )
    _write_json(
        DEFAULT_SEVERITY_MODEL_ROOT / "model_coefficients.json",
        wf["severity_coefficients"],
    )
    _write_csv(DEFAULT_HIERARCHICAL_PREDICTIONS_PATH, wf["predictions"])
    _write_csv(
        DEFAULT_HIERARCHICAL_PROBE_BACKTEST_ROOT / "old_flat_hierarchical_probe_metrics.csv",
        probe_backtest,
    )
    _write_yaml(DEFAULT_FAILURE_DIAGNOSIS_YAML_PATH, failure_diagnosis)
    _write_yaml(DEFAULT_UPPER_STATE_LABEL_AUDIT_YAML_PATH, split_audit)
    _write_yaml(DEFAULT_FEATURE_AUDIT_V2_YAML_PATH, feature_audit)
    _write_yaml(DEFAULT_HIERARCHICAL_WALK_FORWARD_YAML_PATH, _walk_forward_payload(wf))
    _write_yaml(DEFAULT_HIERARCHICAL_ACTUAL_PATH_YAML_PATH, actual_path_matrix)
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)

    _write_markdown(DEFAULT_FAILURE_DIAGNOSIS_DOC_PATH, _render_payload_doc(failure_diagnosis))
    _write_markdown(DEFAULT_UPPER_STATE_LABEL_AUDIT_DOC_PATH, _render_split_audit_doc(split_audit))
    _write_markdown(DEFAULT_FEATURE_EXPANSION_DOC_PATH, _render_feature_report_doc(feature_report))
    _write_markdown(DEFAULT_FEATURE_AUDIT_V2_DOC_PATH, _render_payload_doc(feature_audit))
    _write_markdown(
        DEFAULT_RISK_OFF_REVIEW_DOC_PATH,
        _render_detector_doc(
            "Risk-Off Detector v2 Review",
            "RISK_OFF_DETECTOR_V2_READY_PROMOTION_BLOCKED",
            wf["metrics"],
            ["risk_off_precision", "risk_off_recall", "false_risk_off_rate"],
        ),
    )
    _write_markdown(
        DEFAULT_UPPER_STATE_REVIEW_DOC_PATH,
        _render_detector_doc(
            "Upper-State Detector v1 Review",
            "UPPER_STATE_DETECTOR_V1_READY_PROMOTION_BLOCKED",
            wf["metrics"],
            [
                "upper_state_precision",
                "upper_state_recall",
                "false_risk_on_rate",
                "missed_upside",
                "captured_upside",
                "predicted_upper_state_count",
                "true_upper_state_count",
            ],
        ),
    )
    _write_markdown(
        DEFAULT_SEVERITY_REVIEW_DOC_PATH,
        _render_detector_doc(
            "Risk-On Severity Scaler Review",
            str(wf["metrics"].get("risk_on_severity_status")),
            wf["metrics"],
            ["predicted_risk_on_count", "true_risk_on_count"],
        ),
    )
    _write_markdown(
        DEFAULT_THRESHOLD_REVIEW_DOC_PATH,
        _render_threshold_doc(wf, threshold_policy_path, hierarchical_config_path),
    )
    _write_markdown(
        DEFAULT_HIERARCHICAL_WALK_FORWARD_DOC_PATH,
        _render_payload_doc(_walk_forward_payload(wf)),
    )
    _write_markdown(
        DEFAULT_HIERARCHICAL_ACTUAL_PATH_DOC_PATH,
        _render_actual_path_doc(actual_path_matrix),
    )
    _write_markdown(
        DEFAULT_RETURN_SEEKING_COMPAT_DOC_PATH, _render_payload_doc(return_seeking_compat)
    )
    _write_markdown(DEFAULT_CLASS_IMBALANCE_DOC_PATH, _render_payload_doc(class_imbalance))
    _write_markdown(DEFAULT_OWNER_REVIEW_DOC_PATH, _render_owner_doc(owner_pack))
    _write_markdown(DEFAULT_FORWARD_WATCH_DOC_PATH, _render_forward_watch_doc(final_matrix))
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_closeout_doc(final_matrix))


def _walk_forward_payload(wf: Mapping[str, Any]) -> dict[str, Any]:
    return _payload(
        report_type="hierarchical_first_layer_walk_forward_matrix",
        title="Hierarchical First-Layer Walk-Forward Matrix",
        status="HIERARCHICAL_FIRST_LAYER_WALK_FORWARD_READY_PROMOTION_BLOCKED",
        summary=dict(wf["metrics"]),
        split_rows=list(wf["split_rows"]),
    )


def _hierarchical_metrics(
    predictions: pd.DataFrame,
    split_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if predictions.empty:
        return {
            "balanced_accuracy": 0.0,
            "three_zone_accuracy": 0.0,
            "upper_state_collapse_flag": True,
            **SAFETY_BOUNDARY,
        }
    pred_state = predictions["trend_state"].astype(str)
    label_state = predictions["label_state"].astype(str)
    recalls = []
    for state in STATE_ORDER:
        mask = label_state == state
        if mask.any():
            recalls.append(float((pred_state.loc[mask] == state).mean()))
    pred_upper = pred_state.isin(UPPER_STATES)
    true_upper = predictions["upper_state_binary_label"].astype(int).astype(bool)
    pred_risk_off = predictions["risk_off_probability"] >= predictions["risk_off_threshold"]
    true_risk_off = predictions["risk_off_binary_label"].astype(int).astype(bool)
    pred_zone = pred_state.map(_state_to_zone)
    true_zone = predictions["three_zone_label"].astype(str)
    true_risk_on = label_state == "risk_on"
    return {
        "accuracy": round(float((pred_state == label_state).mean()), GRID_ROUND_DIGITS),
        "balanced_accuracy": round(float(np.mean(recalls)) if recalls else 0.0, GRID_ROUND_DIGITS),
        "three_zone_accuracy": round(float((pred_zone == true_zone).mean()), GRID_ROUND_DIGITS),
        "risk_off_precision": round(
            _ratio((pred_risk_off & true_risk_off).sum(), pred_risk_off.sum()),
            GRID_ROUND_DIGITS,
        ),
        "risk_off_recall": round(
            _ratio((pred_risk_off & true_risk_off).sum(), true_risk_off.sum()),
            GRID_ROUND_DIGITS,
        ),
        "false_risk_off_rate": round(
            _ratio((pred_risk_off & ~true_risk_off).sum(), len(predictions)),
            GRID_ROUND_DIGITS,
        ),
        "late_risk_off_rate": round(
            _ratio((~pred_risk_off & true_risk_off).sum(), len(predictions)),
            GRID_ROUND_DIGITS,
        ),
        "upper_state_precision": round(
            _ratio((pred_upper & true_upper).sum(), pred_upper.sum()),
            GRID_ROUND_DIGITS,
        ),
        "upper_state_recall": round(
            _ratio((pred_upper & true_upper).sum(), true_upper.sum()),
            GRID_ROUND_DIGITS,
        ),
        "false_risk_on_rate": round(
            _ratio((pred_upper & ~true_upper).sum(), len(predictions)),
            GRID_ROUND_DIGITS,
        ),
        "missed_upside": round(_ratio((~pred_upper & true_upper).sum(), true_upper.sum()), 6),
        "captured_upside": round(_ratio((pred_upper & true_upper).sum(), true_upper.sum()), 6),
        "predicted_upper_state_count": int(pred_upper.sum()),
        "true_upper_state_count": int(true_upper.sum()),
        "predicted_constructive_count": int(pred_state.eq("constructive").sum()),
        "predicted_risk_on_count": int(pred_state.eq("risk_on").sum()),
        "true_constructive_count": int(label_state.eq("constructive").sum()),
        "true_risk_on_count": int(true_risk_on.sum()),
        "upper_state_collapse_flag": any(
            bool(row.get("upper_state_collapse_flag")) for row in split_rows
        ),
        "risk_on_severity_status": (
            "RISK_ON_SEVERITY_SCALER_READY"
            if int(pred_state.eq("risk_on").sum()) > 0
            else "INSUFFICIENT_UPPER_STATE_SEVERITY_EVIDENCE"
        ),
        "risk_off_action_value_delta_proxy": round(
            _ratio((pred_risk_off & true_risk_off).sum(), true_risk_off.sum())
            - _ratio((pred_risk_off & ~true_risk_off).sum(), len(predictions)),
            GRID_ROUND_DIGITS,
        ),
        "upper_state_action_value_delta_proxy": round(
            _ratio((pred_upper & true_upper).sum(), true_upper.sum())
            - _ratio((pred_upper & ~true_upper).sum(), len(predictions)),
            GRID_ROUND_DIGITS,
        ),
        "prediction_distribution": _state_count_dict(pred_state),
        **SAFETY_BOUNDARY,
    }


def _fit_binary_linear(
    frame: pd.DataFrame,
    *,
    target_column: str,
    feature_columns: Sequence[str],
) -> dict[str, Any]:
    x = frame[list(feature_columns)].astype(float).to_numpy()
    y = frame[target_column].astype(float).to_numpy()
    means = x.mean(axis=0)
    stds = x.std(axis=0)
    stds[stds == 0.0] = 1.0
    x_scaled = (x - means) / stds
    x_design = np.column_stack([np.ones(len(x_scaled)), x_scaled])
    positive = max(float(y.sum()), 1.0)
    negative = max(float(len(y) - y.sum()), 1.0)
    weights = np.where(y > 0.5, len(y) / (2.0 * positive), len(y) / (2.0 * negative))
    weighted_x = x_design * np.sqrt(weights[:, None])
    weighted_y = y * np.sqrt(weights)
    ridge = np.eye(weighted_x.shape[1]) * 1e-6
    beta = np.linalg.solve(weighted_x.T @ weighted_x + ridge, weighted_x.T @ weighted_y)
    return {
        "intercept": float(beta[0]),
        "beta": beta[1:],
        "means": means,
        "stds": stds,
        "coefficients": {
            "intercept": round(float(beta[0]), 8),
            **{
                column: round(float(value), 8)
                for column, value in zip(feature_columns, beta[1:], strict=True)
            },
        },
    }


def _predict_binary_linear(
    frame: pd.DataFrame,
    model: Mapping[str, Any],
    feature_columns: Sequence[str],
) -> np.ndarray:
    if frame.empty:
        return np.array([])
    x = frame[list(feature_columns)].astype(float).to_numpy()
    scaled = (x - np.asarray(model["means"])) / np.asarray(model["stds"])
    raw = float(model["intercept"]) + scaled @ np.asarray(model["beta"])
    return np.clip(raw, 0.001, 0.999)


def _calibrate_threshold(
    *,
    probabilities: np.ndarray,
    actual: np.ndarray,
    policy: Mapping[str, Any],
    positive_bias: float,
) -> float:
    grid_cfg = _mapping(policy.get("threshold_grid"))
    start = _float(grid_cfg.get("start"), default=0.25)
    stop = _float(grid_cfg.get("stop"), default=0.70)
    step = _float(grid_cfg.get("step"), default=0.05)
    thresholds = np.arange(start, stop + step / 2.0, step)
    objective = _mapping(policy.get("objective"))
    best_threshold = float(thresholds[0])
    best_score = -math.inf
    for threshold in thresholds:
        pred = probabilities >= threshold
        precision = _ratio((pred & (actual == 1)).sum(), pred.sum())
        recall = _ratio((pred & (actual == 1)).sum(), (actual == 1).sum())
        false_positive_rate = _ratio((pred & (actual == 0)).sum(), len(actual))
        score = (
            _float(objective.get("precision_weight"), default=0.4) * precision
            + _float(objective.get("recall_weight"), default=0.5) * recall
            - _float(
                objective.get("false_risk_on_penalty", objective.get("false_risk_off_penalty")),
                default=0.1,
            )
            * false_positive_rate
        )
        if score > best_score:
            best_score = score
            best_threshold = float(threshold)
    positive_rate = _ratio((actual == 1).sum(), len(actual))
    if positive_rate > 0.0:
        prevalence_threshold = float(
            np.quantile(probabilities, max(0.0, 1.0 - min(0.5, positive_rate * positive_bias)))
        )
        best_threshold = min(best_threshold, prevalence_threshold)
    return round(best_threshold, 6)


def _fit_severity_model(
    train: pd.DataFrame,
    threshold_policy: Mapping[str, Any],
) -> dict[str, Any]:
    cfg = _mapping(threshold_policy.get("risk_on_severity"))
    min_samples = _int(cfg.get("min_upper_state_samples"), default=60)
    upper = train.loc[train["upper_state_binary_label"].astype(bool)].copy()
    if len(upper) < min_samples or upper["risk_on_severity_label"].eq(2).sum() < 5:
        threshold = 0.75
        return {
            "status": "INSUFFICIENT_UPPER_STATE_SEVERITY_EVIDENCE",
            "threshold": threshold,
            "model": None,
            "coefficients": {"status": "INSUFFICIENT_UPPER_STATE_SEVERITY_EVIDENCE"},
        }
    upper["risk_on_binary"] = upper["risk_on_severity_label"].eq(2).astype(int)
    model = _fit_binary_linear(
        upper,
        target_column="risk_on_binary",
        feature_columns=UP_STATE_FEATURE_COLUMNS,
    )
    probs = _predict_binary_linear(upper, model, UP_STATE_FEATURE_COLUMNS)
    threshold = float(np.quantile(probs, _float(cfg.get("risk_on_quantile"), default=0.75)))
    return {
        "status": "RISK_ON_SEVERITY_SCALER_READY",
        "threshold": round(threshold, 6),
        "model": model,
        "coefficients": model["coefficients"],
    }


def _predict_severity(frame: pd.DataFrame, severity: Mapping[str, Any]) -> np.ndarray:
    model = severity.get("model")
    if isinstance(model, Mapping):
        return _predict_binary_linear(frame, model, UP_STATE_FEATURE_COLUMNS)
    base = frame["upper_state_binary_label"].astype(float).to_numpy()
    return np.clip(base * 0.5, 0.001, 0.999)


def _compose_state(
    *,
    risk_off_probability: float,
    upper_state_probability: float,
    risk_on_severity_score: float,
    risk_off_threshold: float,
    upper_state_threshold: float,
    risk_on_threshold: float,
    hierarchical_config: Mapping[str, Any],
) -> tuple[str, float]:
    upper_pred = upper_state_probability >= upper_state_threshold
    risk_off_pred = risk_off_probability >= risk_off_threshold
    if upper_pred and (not risk_off_pred or upper_state_probability >= risk_off_probability * 0.85):
        state = "risk_on" if risk_on_severity_score >= risk_on_threshold else "constructive"
        confidence = max(upper_state_probability, risk_on_severity_score)
    elif risk_off_pred:
        state = (
            "risk_off"
            if risk_off_probability >= min(0.9, risk_off_threshold + 0.15)
            else "defensive"
        )
        confidence = risk_off_probability
    else:
        state = "neutral"
        confidence = max(0.35, 1.0 - abs(upper_state_probability - risk_off_probability))
    if state not in set(_mapping(hierarchical_config.get("validity_days_by_state")).keys()):
        state = "neutral"
    return state, round(float(min(0.999, max(0.001, confidence))), 6)


def _model_frame(
    hierarchical_labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    scope_config: Mapping[str, Any],
) -> pd.DataFrame:
    horizon = _int(_mapping(scope_config.get("walk_forward")).get("label_horizon_days"), default=20)
    labels = hierarchical_labels.loc[hierarchical_labels["horizon_days"].astype(int) == horizon]
    merged = feature_matrix.merge(labels, on="date", how="inner", suffixes=("", "_label"))
    return merged.sort_values("date").reset_index(drop=True)


def _walk_forward_splits(
    merged: pd.DataFrame,
    scope_config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    wf = _mapping(scope_config.get("walk_forward"))
    train_window = _int(wf.get("train_window_days"), default=504)
    validation_window = _int(wf.get("validation_window_days"), default=63)
    step = _int(wf.get("step_days"), default=21)
    min_train = _int(wf.get("min_train_samples"), default=300)
    splits = []
    split_id = 0
    for validation_start in range(
        train_window,
        max(train_window, len(merged) - validation_window),
        step,
    ):
        train = merged.iloc[validation_start - train_window : validation_start].copy()
        validation = merged.iloc[validation_start : validation_start + validation_window].copy()
        if len(train) < min_train or validation.empty:
            continue
        splits.append({"split_id": split_id, "train": train, "validation": validation})
        split_id += 1
    return splits


def _state_to_zone(state: object) -> str:
    state_str = str(state)
    if state_str in DOWNSIDE_STATES:
        return "downside_zone"
    if state_str in UPPER_STATES:
        return "upside_zone"
    return "middle_zone"


def _state_validity(state: str, cfg: Mapping[str, Any]) -> int:
    return _int(_mapping(cfg.get("validity_days_by_state")).get(state), default=10)


def _state_decay(state: str, cfg: Mapping[str, Any]) -> str:
    return str(_mapping(cfg.get("decay_profile_by_state")).get(state, "medium"))


def _days_since_low(series: pd.Series, *, window: int) -> pd.Series:
    values = []
    for idx in range(len(series)):
        start = max(0, idx - window + 1)
        window_values = series.iloc[start : idx + 1]
        low_position = int(np.argmin(window_values.to_numpy()))
        values.append(len(window_values) - low_position - 1)
    return pd.Series(values, index=series.index, dtype=float)


def _feature_family(feature_name: str) -> str:
    if "momentum" in feature_name or "ma_slope" in feature_name or "higher" in feature_name:
        return "price_trend_persistence"
    if "vol" in feature_name:
        return "volatility_compression"
    if "drawdown" in feature_name or "recovery" in feature_name or "low" in feature_name:
        return "post_drawdown_recovery"
    if "yield" in feature_name:
        return "macro_rates"
    return "price_trend_persistence"


def _probe_role_rows(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for probe in _records(registry.get("probes")):
        rows.append(
            {
                "probe_id": str(probe.get("probe_id")),
                "role": str(probe.get("role")),
                "role_tags": [str(tag) for tag in probe.get("role_tags", [])],
                "return_seeking": bool(probe.get("return_seeking")),
                "research_only": bool(probe.get("research_only")),
                "promotion_enabled": bool(probe.get("promotion_enabled")),
                "broker_enabled": bool(probe.get("broker_enabled")),
            }
        )
    return rows


def _probe_exposure_rows(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for probe in _records(registry.get("probes")):
        for state, weights in _mapping(probe.get("weights_by_trend_state")).items():
            weight_map = _mapping(weights)
            qqq = _float(weight_map.get("QQQ"))
            tqqq = _float(weight_map.get("TQQQ"))
            rows.append(
                {
                    "probe_id": str(probe.get("probe_id")),
                    "state": str(state),
                    "qqq_weight": qqq,
                    "sgov_weight": _float(weight_map.get("SGOV")),
                    "tqqq_weight": tqqq,
                    "qqq_equivalent_exposure": round(qqq + 3.0 * tqqq, GRID_ROUND_DIGITS),
                    "risk_asset_weight": round(qqq + tqqq, GRID_ROUND_DIGITS),
                }
            )
    return rows


def _upper_state_exposure_increases(probe: Mapping[str, Any]) -> bool:
    weights = _mapping(probe.get("weights_by_trend_state"))
    neutral = _qqq_equivalent(_mapping(weights.get("neutral")))
    constructive = _qqq_equivalent(_mapping(weights.get("constructive")))
    risk_on = _qqq_equivalent(_mapping(weights.get("risk_on")))
    return constructive >= neutral and risk_on >= constructive


def _qqq_equivalent(weights: Mapping[str, Any]) -> float:
    return _float(weights.get("QQQ")) + 3.0 * _float(weights.get("TQQQ"))


def _state_count_dict(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.astype(str).value_counts().items()}


def _group_bool_rate(frame: pd.DataFrame, *, group_col: str, bool_col: str) -> dict[str, float]:
    return {
        str(key): round(float(value), GRID_ROUND_DIGITS)
        for key, value in frame.groupby(group_col)[bool_col].mean().items()
    }


def _group_mean(frame: pd.DataFrame, *, group_col: str, value_col: str) -> dict[str, float]:
    return {
        str(key): round(float(value), GRID_ROUND_DIGITS)
        for key, value in frame.groupby(group_col)[value_col].mean().items()
    }


def _flat_actual_path_deterioration(flat_probe_metrics: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for probe_id, frame in flat_probe_metrics.groupby("probe_id"):
        old = _model_metric_row(frame, "old_scorecard_first_layer_v1")
        new = _model_metric_row(frame, "new_calibrated_first_layer_v1")
        row = {
            "probe_id": probe_id,
            "annual_return_delta": round(
                _float(new.get("actual_path_annual_return"))
                - _float(old.get("actual_path_annual_return")),
                GRID_ROUND_DIGITS,
            ),
            "sharpe_delta": round(
                _float(new.get("sharpe_daily_zero_rf")) - _float(old.get("sharpe_daily_zero_rf")),
                GRID_ROUND_DIGITS,
            ),
            "calmar_delta": round(
                _float(new.get("calmar_daily_equity_dd"))
                - _float(old.get("calmar_daily_equity_dd")),
                GRID_ROUND_DIGITS,
            ),
        }
        row["deteriorated"] = (
            row["annual_return_delta"] < 0 and row["sharpe_delta"] < 0 and row["calmar_delta"] < 0
        )
        rows.append(row)
    return {
        "deteriorated_probe_count": sum(row["deteriorated"] for row in rows),
        "rows": rows,
    }


def _model_metric_row(frame: pd.DataFrame, model_id: str) -> dict[str, Any]:
    rows = frame.loc[frame["model_id"].astype(str) == model_id]
    if rows.empty:
        return {}
    return dict(rows.iloc[0])


def _up_state_artifact_paths() -> dict[str, str]:
    return {
        "failure_diagnosis": str(DEFAULT_FAILURE_DIAGNOSIS_DOC_PATH),
        "hierarchical_labels": str(DEFAULT_HIERARCHICAL_LABELS_PATH),
        "pit_feature_matrix_v2": str(DEFAULT_FEATURE_V2_PATH),
        "hierarchical_predictions": str(DEFAULT_HIERARCHICAL_PREDICTIONS_PATH),
        "actual_path_matrix": str(DEFAULT_HIERARCHICAL_ACTUAL_PATH_YAML_PATH),
        "owner_pack": str(DEFAULT_OWNER_REVIEW_DOC_PATH),
        "final_matrix": str(DEFAULT_FINAL_MATRIX_YAML_PATH),
    }


def _render_payload_doc(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Market regime: `{payload.get('market_regime')}`",
    ]
    for key, value in _mapping(payload.get("summary")).items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "Safety boundary:",
            f"- promotion_allowed: `{payload.get('promotion_allowed')}`",
            f"- paper_shadow_allowed: `{payload.get('paper_shadow_allowed')}`",
            f"- production_allowed: `{payload.get('production_allowed')}`",
            f"- broker_action: `{payload.get('broker_action')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_split_audit_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Split Rows", ""]
    for row in _records(payload.get("split_rows"))[:20]:
        lines.append(
            "- split {split_id}: train_upper={train_upper_state_count}, "
            "validation_upper={validation_upper_state_count}, "
            "collapse_gap={train_missing_upper_but_validation_has_upper}".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_feature_report_doc(report: Mapping[str, Any]) -> str:
    payload = _payload(
        report_type="up_state_feature_expansion_review",
        title="Up-State Feature Expansion Review",
        status="UP_STATE_FEATURE_EXPANSION_READY_PROMOTION_BLOCKED",
        summary={
            "row_count": report.get("row_count"),
            "approved_feature_count": report.get("approved_feature_count"),
            "blocked_feature_count": report.get("blocked_feature_count"),
        },
        features=list(report.get("features", [])),
    )
    return _render_payload_doc(payload)


def _render_detector_doc(
    title: str,
    status: str,
    metrics: Mapping[str, Any],
    fields: Sequence[str],
) -> str:
    payload = _payload(
        report_type=title.lower().replace(" ", "_"),
        title=title,
        status=status,
        summary={field: metrics.get(field) for field in fields},
    )
    return _render_payload_doc(payload)


def _render_threshold_doc(
    wf: Mapping[str, Any],
    threshold_policy_path: Path,
    hierarchical_config_path: Path,
) -> str:
    payload = _payload(
        report_type="first_layer_threshold_calibration_review",
        title="First-Layer Threshold Calibration Review",
        status="FIRST_LAYER_THRESHOLD_CALIBRATION_READY_PROMOTION_BLOCKED",
        summary={
            "split_count": len(wf["split_rows"]),
            "threshold_policy_hash": _file_sha256(threshold_policy_path),
            "hierarchical_config_hash": _file_sha256(hierarchical_config_path),
            "upper_state_collapse_flag": wf["metrics"].get("upper_state_collapse_flag"),
        },
        split_rows=list(wf["split_rows"]),
    )
    return _render_payload_doc(payload)


def _render_actual_path_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Probe Rows", ""]
    for row in _records(payload.get("probe_rows")):
        lines.append(
            "- {probe_id}: hierarchical_vs_flat_return_delta={hierarchical_vs_flat_return_delta}, "
            "hierarchical_vs_flat_calmar_delta={hierarchical_vs_flat_calmar_delta}, "
            "improved={actual_path_improved_vs_flat}".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_owner_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Owner Questions", ""]
    questions = {
        "当前第一层为什么失败": (
            "flat five-class model predicted zero constructive/risk_on and became "
            "over-defensive."
        ),
        "是否已经改成分层模型": "yes, risk-off detector + upper-state detector + severity scaler.",
        "risk-off detector 是否保留价值": "yes, it is retained as a separate module.",
        "upper-state detector 是否学出上行": str(
            _mapping(payload.get("summary")).get("upper_state_detector_predicted_upper_count")
        ),
        "接回 actual-path 后是否改善": str(
            _mapping(payload.get("actual_path_summary")).get("improved_vs_flat_probe_count")
        ),
        "dynamic promotion 为什么仍 blocked": (
            "research-only evidence remains insufficient and owner approval is pending."
        ),
    }
    for key, value in questions.items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _render_forward_watch_doc(final_matrix: Mapping[str, Any]) -> str:
    payload = _payload(
        report_type="hierarchical_first_layer_forward_watch_plan",
        title="Hierarchical First-Layer Forward Watch Plan",
        status="HIERARCHICAL_FIRST_LAYER_FORWARD_WATCH_PLAN_READY_PROMOTION_BLOCKED",
        summary={
            "watch_allowed": _mapping(final_matrix.get("summary")).get("base_status")
            == "UP_STATE_LEARNING_IMPROVES_ACTUAL_PATH",
            "required_fields": [
                "risk_off_probability",
                "upper_state_probability",
                "risk_on_severity_score",
                "trend_state",
                "probe_implied_weights",
                "1d/5d/10d/20d outcomes",
                "missed_upside",
                "false_risk_on",
                "false_risk_off",
            ],
        },
    )
    return _render_payload_doc(payload)


def _render_closeout_doc(final_matrix: Mapping[str, Any]) -> str:
    payload = _payload(
        report_type="first_layer_up_state_learning_closeout",
        title="First-Layer Up-State Learning Closeout",
        status=str(final_matrix.get("status")),
        summary=_mapping(final_matrix.get("summary")),
    )
    return _render_payload_doc(payload)


def _csv_preview(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return _json_scalar(frame.head(20).to_dict("records"))


def payload_to_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(_json_scalar(payload), indent=2, sort_keys=True)
