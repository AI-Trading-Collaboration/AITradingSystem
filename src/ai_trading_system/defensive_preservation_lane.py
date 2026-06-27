from __future__ import annotations

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
    DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    PRIMARY_WINDOW_ID,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_DEFENSIVE_LANE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "defensive_preservation_lane_policy.yaml"
)
DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH = (
    PROJECT_ROOT / "config" / "research" / "defensive_lane_label_taxonomy.yaml"
)
DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "defensive_lane_action_value_policy.yaml"
)
DEFAULT_SCOPE_DOC_PATH = PROJECT_ROOT / "docs" / "research" / "defensive_preservation_lane_scope.md"
DEFAULT_LABELS_CSV_PATH = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "defensive_lane_labels.csv"
DEFAULT_FEATURE_MATRIX_CSV_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "defensive_lane_feature_matrix.csv"
)
DEFAULT_FEATURE_AUDIT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_lane_feature_pit_audit.md"
)
DEFAULT_FEATURE_AUDIT_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_lane_feature_pit_audit.yaml"
)
DEFAULT_MODEL_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_lane_model_review.md"
)
DEFAULT_MODEL_MATRIX_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_lane_model_matrix.yaml"
)
DEFAULT_PREDICTIONS_CSV_PATH = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models" / "defensive_lane_predictions.csv"
)
DEFAULT_ACTUAL_PATH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_lane_actual_path_review.md"
)
DEFAULT_ACTUAL_PATH_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_lane_actual_path_matrix.yaml"
)
DEFAULT_2022_SLICE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_lane_2022_slice_review.md"
)
DEFAULT_2022_SLICE_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_lane_2022_slice_matrix.yaml"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_preservation_lane_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_preservation_lane_final_matrix.yaml"
)
DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "dynamic_actual_path_owner_review_decision.yaml"
)

DEFENSIVE_FEATURE_COLUMNS = [
    "qqq_return_5d",
    "qqq_return_20d",
    "qqq_return_60d",
    "realized_vol_20d",
    "realized_vol_60d",
    "downside_vol_20d",
    "drawdown_depth_126d",
    "drawdown_speed_20d",
    "trend_break_20_60",
    "volatility_regime_score",
    "rate_shock_20d",
    "liquidity_stress_proxy",
    "recovery_from_60d_low",
    "distance_from_60d_high",
]
PREDICTION_MODEL_ID = "defensive_preservation_lane_v1"
DEFENSIVE_PROBE_IDS = ("defensive_overlay_probe", "drawdown_control_probe")
WINDOW_KEYS = (
    "research_window_id",
    "requested_start",
    "actual_start",
    "actual_portfolio_start",
    "end",
    "window_role",
    "data_quality_contract",
    "exact_or_proxy",
)


def run_defensive_preservation_lane_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    lane_policy_path: Path = DEFAULT_DEFENSIVE_LANE_POLICY_PATH,
    label_taxonomy_path: Path = DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH,
    action_value_policy_path: Path = DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH,
    probe_registry_path: Path = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    limited_adjustment_reference_path: Path = DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH,
) -> dict[str, Any]:
    registry = load_research_window_registry(registry_path)
    primary_window = _mapping(registry["windows"][PRIMARY_WINDOW_ID])
    lane_policy = _load_yaml_mapping(lane_policy_path)
    label_taxonomy = _load_yaml_mapping(label_taxonomy_path)
    action_policy = _load_yaml_mapping(action_value_policy_path)
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
            f"Cached data quality gate failed for defensive lane: {data_gate['status']}"
        )

    prices = _load_price_matrix(prices_path, ASSETS)
    rates = _load_rates(rates_path)
    primary_prices = slice_window_prices(prices, primary_window)
    primary_rates = rates.loc[rates.index >= primary_prices.index.min()].copy()

    feature_matrix = build_defensive_feature_matrix(
        prices=primary_prices,
        rates=primary_rates,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    labels = build_defensive_labels(
        feature_matrix=feature_matrix,
        prices=primary_prices,
        taxonomy=label_taxonomy,
        action_policy=action_policy,
    )
    predictions = build_defensive_predictions(
        feature_matrix=feature_matrix,
        lane_policy=lane_policy,
        taxonomy=label_taxonomy,
    )
    feature_audit = build_feature_pit_audit(
        feature_matrix=feature_matrix,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    model_review = build_model_review(
        labels=labels,
        predictions=predictions,
        lane_policy=lane_policy,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    actual_path = build_actual_path_review(
        prices=primary_prices,
        predictions=predictions,
        probe_registry=probe_registry,
        primary_window=primary_window,
        model_review=model_review,
        limited_adjustment_reference_path=limited_adjustment_reference_path,
    )
    slice_2022 = build_2022_slice_review(
        labels=labels,
        feature_matrix=feature_matrix,
        predictions=predictions,
        actual_path=actual_path,
        primary_window=primary_window,
    )
    final_matrix = build_final_matrix(
        lane_policy=lane_policy,
        model_review=model_review,
        actual_path=actual_path,
        slice_2022=slice_2022,
        feature_audit=feature_audit,
        primary_window=primary_window,
        data_gate=data_gate,
    )

    write_defensive_lane_outputs(
        lane_policy=lane_policy,
        labels=labels,
        feature_matrix=feature_matrix,
        predictions=predictions,
        feature_audit=feature_audit,
        model_review=model_review,
        actual_path=actual_path,
        slice_2022=slice_2022,
        final_matrix=final_matrix,
        output_root=output_root,
    )
    return final_matrix


def build_defensive_feature_matrix(
    *,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> pd.DataFrame:
    qqq = pd.to_numeric(prices["QQQ"], errors="coerce").ffill()
    sgov = pd.to_numeric(prices["SGOV"], errors="coerce").ffill()
    returns = qqq.pct_change().fillna(0.0)
    sgov_returns = sgov.pct_change().fillna(0.0)
    ma20 = qqq.rolling(20, min_periods=5).mean()
    ma60 = qqq.rolling(60, min_periods=10).mean()
    rolling_high_126 = qqq.rolling(126, min_periods=10).max()
    rolling_high_60 = qqq.rolling(60, min_periods=10).max()
    rolling_low_60 = qqq.rolling(60, min_periods=10).min()
    realized_vol_20d = returns.rolling(20, min_periods=5).std(ddof=0) * math.sqrt(252.0)
    realized_vol_60d = returns.rolling(60, min_periods=10).std(ddof=0) * math.sqrt(252.0)
    downside = returns.where(returns < 0.0, 0.0)
    rate_frame = rates.reindex(prices.index).ffill()
    dgs10 = pd.to_numeric(
        rate_frame.get("DGS10", pd.Series(0.0, index=prices.index)), errors="coerce"
    )

    frame = pd.DataFrame(index=prices.index)
    frame["qqq_return_5d"] = qqq.pct_change(5)
    frame["qqq_return_20d"] = qqq.pct_change(20)
    frame["qqq_return_60d"] = qqq.pct_change(60)
    frame["realized_vol_20d"] = realized_vol_20d
    frame["realized_vol_60d"] = realized_vol_60d
    frame["downside_vol_20d"] = downside.rolling(20, min_periods=5).std(ddof=0) * math.sqrt(252.0)
    frame["drawdown_depth_126d"] = qqq / rolling_high_126 - 1.0
    frame["drawdown_speed_20d"] = frame["drawdown_depth_126d"] - frame["drawdown_depth_126d"].shift(
        20
    )
    frame["trend_break_20_60"] = ma20 / ma60 - 1.0
    frame["volatility_regime_score"] = (
        realized_vol_20d / realized_vol_20d.rolling(252, min_periods=60).median() - 1.0
    )
    frame["rate_shock_20d"] = dgs10.diff(20).fillna(0.0)
    frame["liquidity_stress_proxy"] = (
        realized_vol_20d - (sgov_returns.rolling(20, min_periods=5).std(ddof=0) * math.sqrt(252.0))
    ).clip(lower=0.0)
    frame["recovery_from_60d_low"] = qqq / rolling_low_60 - 1.0
    frame["distance_from_60d_high"] = qqq / rolling_high_60 - 1.0
    frame = frame.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    metadata = window_metadata(primary_window)
    rows: list[dict[str, Any]] = []
    index = list(frame.index)
    for idx, timestamp in enumerate(index):
        decision_at = index[idx + 1] if idx + 1 < len(index) else timestamp
        rows.append(
            {
                **metadata,
                "date": timestamp.date().isoformat(),
                "known_at": timestamp.date().isoformat(),
                "available_at": timestamp.date().isoformat(),
                "decision_at": decision_at.date().isoformat(),
                "data_quality_status": data_gate["status"],
                "feature_cutoff_passed": True,
                "pit_status": "PIT_APPROVED_DEFENSIVE_FEATURES",
                **{
                    column: round(float(frame.loc[timestamp, column]), 8)
                    for column in DEFENSIVE_FEATURE_COLUMNS
                },
            }
        )
    return pd.DataFrame(rows)


def build_defensive_labels(
    *,
    feature_matrix: pd.DataFrame,
    prices: pd.DataFrame,
    taxonomy: Mapping[str, Any],
    action_policy: Mapping[str, Any],
) -> pd.DataFrame:
    thresholds = _mapping(action_policy.get("thresholds"))
    horizon = _int(action_policy.get("horizon_days"), 20)
    qqq = pd.to_numeric(prices["QQQ"], errors="coerce").ffill()
    returns = qqq.pct_change().fillna(0.0)
    indexed_features = feature_matrix.set_index(pd.to_datetime(feature_matrix["date"]))
    rows: list[dict[str, Any]] = []
    max_idx = len(qqq.index) - horizon - 1
    for idx, timestamp in enumerate(qqq.index):
        if idx > max_idx or timestamp not in indexed_features.index:
            continue
        future_returns = returns.iloc[idx + 1 : idx + horizon + 1]
        if future_returns.empty:
            continue
        future_equity = (1.0 + future_returns).cumprod()
        future_return = float(future_equity.iloc[-1] - 1.0)
        future_max_drawdown = float((future_equity / future_equity.cummax() - 1.0).min())
        worst_5d = _worst_window_return(future_returns, 5)
        feature_row = indexed_features.loc[timestamp]
        if isinstance(feature_row, pd.DataFrame):
            feature_row = feature_row.iloc[-1]
        risk_off_needed = future_max_drawdown <= _float(
            thresholds.get("risk_off_future_max_drawdown"), -0.08
        ) or worst_5d <= _float(thresholds.get("risk_off_worst_5d_return"), -0.04)
        defensive_hold_needed = (
            not risk_off_needed
            and (
                _float(feature_row.get("drawdown_depth_126d"))
                <= _float(thresholds.get("defensive_current_drawdown"), -0.06)
                or _float(feature_row.get("volatility_regime_score"))
                >= _float(thresholds.get("defensive_volatility_regime_score"), 0.30)
            )
            and future_return <= _float(thresholds.get("defensive_future_return_ceiling"), 0.04)
        )
        do_not_de_risk = (
            not risk_off_needed
            and future_return >= _float(thresholds.get("do_not_de_risk_future_return"), 0.025)
            and future_max_drawdown
            >= _float(thresholds.get("do_not_de_risk_max_drawdown_floor"), -0.045)
        )
        re_risk_allowed = (
            not risk_off_needed
            and not defensive_hold_needed
            and _float(feature_row.get("recovery_from_60d_low"))
            >= _float(thresholds.get("re_risk_recovery_from_60d_low"), 0.06)
            and worst_5d >= _float(thresholds.get("re_risk_worst_5d_floor"), -0.035)
        )
        label_state = (
            "risk_off" if risk_off_needed else "defensive" if defensive_hold_needed else "neutral"
        )
        rows.append(
            {
                **{key: feature_row.get(key) for key in WINDOW_KEYS if key in feature_row},
                "date": timestamp.date().isoformat(),
                "horizon_days": horizon,
                "future_return": round(future_return, GRID_ROUND_DIGITS),
                "future_max_drawdown": round(future_max_drawdown, GRID_ROUND_DIGITS),
                "worst_5d_return": round(worst_5d, GRID_ROUND_DIGITS),
                "risk_off_needed": bool(risk_off_needed),
                "defensive_hold_needed": bool(defensive_hold_needed),
                "do_not_de_risk": bool(do_not_de_risk),
                "re_risk_allowed_but_not_add_risk": bool(re_risk_allowed),
                "defensive_label_state": label_state,
                "label_uses_future_outcome": True,
                "add_risk_label_allowed": False,
                "high_confidence_risk_on_label_allowed": False,
                **SAFETY_BOUNDARY,
            }
        )
    return pd.DataFrame(rows)


def build_defensive_predictions(
    *,
    feature_matrix: pd.DataFrame,
    lane_policy: Mapping[str, Any],
    taxonomy: Mapping[str, Any],
) -> pd.DataFrame:
    thresholds = _mapping(lane_policy.get("model_thresholds"))
    rows: list[dict[str, Any]] = []
    for raw in feature_matrix.to_dict("records"):
        risk_off_score = _risk_off_score(raw, thresholds)
        defensive_score = _defensive_score(raw, thresholds)
        re_risk_score = _re_risk_score(raw, thresholds)
        if risk_off_score >= _float(thresholds.get("risk_off_score_cutoff"), 1.0):
            output_signal = "risk_off"
            trend_state = "risk_off"
        elif defensive_score >= _float(thresholds.get("defensive_score_cutoff"), 1.0):
            output_signal = "defensive"
            trend_state = "defensive"
        elif re_risk_score >= _float(thresholds.get("re_risk_allowed_score_cutoff"), 1.0):
            output_signal = "re_risk_allowed_but_not_add_risk"
            trend_state = "neutral"
        else:
            output_signal = "do_not_de_risk"
            trend_state = "neutral"
        rows.append(
            {
                **{key: raw.get(key) for key in WINDOW_KEYS if key in raw},
                "date": raw["date"],
                "model_id": PREDICTION_MODEL_ID,
                "output_signal": output_signal,
                "trend_state": trend_state,
                "confidence": round(max(risk_off_score, defensive_score, re_risk_score) / 3.0, 6),
                "risk_off_probability": round(min(1.0, risk_off_score / 3.0), 6),
                "defensive_probability": round(min(1.0, defensive_score / 3.0), 6),
                "do_not_de_risk_probability": round(
                    1.0 if output_signal == "do_not_de_risk" else 0.0, 6
                ),
                "re_risk_allowed_probability": round(min(1.0, re_risk_score / 3.0), 6),
                "add_risk_probability": 0.0,
                "high_confidence_risk_on_probability": 0.0,
                "tqqq_signal_allowed": False,
                **SAFETY_BOUNDARY,
            }
        )
    return pd.DataFrame(rows)


def build_feature_pit_audit(
    *,
    feature_matrix: pd.DataFrame,
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    blocked = [
        "VIX unavailable in current tracked cache",
        "event risk score unavailable in current tracked cache",
        "liquidity stress direct market-depth source unavailable",
        "TQQQ-related signal intentionally blocked for defensive lane",
        "add-risk and high-confidence risk-on features intentionally blocked",
    ]
    return _payload(
        report_type="defensive_lane_feature_pit_audit",
        title="Defensive Lane Feature PIT Audit",
        status="DEFENSIVE_LANE_FEATURE_PIT_AUDIT_READY_WITH_LIMITATIONS",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate["status"],
            "feature_row_count": len(feature_matrix),
            "approved_feature_count": len(DEFENSIVE_FEATURE_COLUMNS),
            "blocked_feature_count": len(blocked),
            "feature_cutoff_passed": bool(feature_matrix["feature_cutoff_passed"].all()),
            "tqqq_feature_count": len(
                [col for col in feature_matrix.columns if "tqqq" in col.lower()]
            ),
            "add_risk_feature_count": 0,
        },
        feature_matrix_columns=list(feature_matrix.columns),
        feature_rows=[
            {
                "feature_id": column,
                "pit_status": "PIT_APPROVED",
                "feature_role": "defensive_risk_control",
            }
            for column in DEFENSIVE_FEATURE_COLUMNS
        ],
        blocked_features=blocked,
    )


def build_model_review(
    *,
    labels: pd.DataFrame,
    predictions: pd.DataFrame,
    lane_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    merged = predictions.merge(
        labels[
            [
                "date",
                "risk_off_needed",
                "defensive_hold_needed",
                "do_not_de_risk",
                "re_risk_allowed_but_not_add_risk",
                "defensive_label_state",
            ]
        ],
        on="date",
        how="inner",
    )
    model_rows = [
        _binary_model_row(merged, "risk_off_detector_v4", "risk_off", "risk_off_needed"),
        _binary_model_row(
            merged, "do_not_de_risk_detector_defensive_v1", "do_not_de_risk", "do_not_de_risk"
        ),
        _binary_model_row(
            merged,
            "re_risk_allowed_detector_v1",
            "re_risk_allowed_but_not_add_risk",
            "re_risk_allowed_but_not_add_risk",
        ),
    ]
    predicted_states = predictions["trend_state"].astype(str).value_counts().to_dict()
    label_states = labels["defensive_label_state"].astype(str).value_counts().to_dict()
    return _payload(
        report_type="defensive_lane_model_matrix",
        title="Defensive Lane Model Review",
        status="DEFENSIVE_LANE_MODEL_REVIEW_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate["status"],
            "model_count": len(model_rows),
            "prediction_count": len(predictions),
            "label_count": len(labels),
            "predicted_state_distribution": predicted_states,
            "label_state_distribution": label_states,
            "add_risk_prediction_count": int((predictions["add_risk_probability"] > 0).sum()),
            "high_confidence_risk_on_prediction_count": int(
                (predictions["high_confidence_risk_on_probability"] > 0).sum()
            ),
            "tqqq_signal_allowed": False,
            "promotion_status": "blocked",
        },
        label_columns=list(labels.columns),
        prediction_columns=list(predictions.columns),
        label_contract={
            "required_label_columns": [
                "risk_off_needed",
                "defensive_hold_needed",
                "do_not_de_risk",
                "re_risk_allowed_but_not_add_risk",
            ],
            "add_risk_label_allowed_any": bool(labels["add_risk_label_allowed"].any()),
            "high_confidence_risk_on_label_allowed_any": bool(
                labels["high_confidence_risk_on_label_allowed"].any()
            ),
        },
        prediction_contract={
            "allowed_trend_states": ["risk_off", "defensive", "neutral"],
            "observed_trend_states": sorted(predicted_states),
            "add_risk_probability_max": _max_value(predictions["add_risk_probability"]),
            "high_confidence_risk_on_probability_max": _max_value(
                predictions["high_confidence_risk_on_probability"]
            ),
            "tqqq_signal_allowed_any": bool(predictions["tqqq_signal_allowed"].any()),
        },
        model_rows=model_rows,
        policy_thresholds=_mapping(lane_policy.get("model_thresholds")),
    )


def build_actual_path_review(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    model_review: Mapping[str, Any],
    limited_adjustment_reference_path: Path,
) -> dict[str, Any]:
    probes = {
        str(probe.get("probe_id")): probe
        for probe in _records(probe_registry.get("probes"))
        if str(probe.get("probe_id")) in DEFENSIVE_PROBE_IDS
    }
    neutral_predictions = predictions.copy()
    neutral_predictions["trend_state"] = "neutral"
    rows = []
    for probe_id in DEFENSIVE_PROBE_IDS:
        probe = probes[probe_id]
        metrics = _backtest_probe_predictions(
            prices=prices,
            predictions=predictions[["date", "trend_state"]],
            probe=probe,
            model_id=PREDICTION_MODEL_ID,
        )
        neutral = _backtest_probe_predictions(
            prices=prices,
            predictions=neutral_predictions[["date", "trend_state"]],
            probe=probe,
            model_id="neutral_reference",
        )
        rows.append(
            {
                **window_metadata(primary_window),
                "probe_id": probe_id,
                "role": str(probe.get("role")),
                "lane_role": "defensive_preservation",
                "date_start": metrics["date_start"],
                "date_end": metrics["date_end"],
                "model_id": PREDICTION_MODEL_ID,
                "annual_return": metrics["actual_path_annual_return"],
                "max_drawdown_daily_equity": metrics["max_drawdown_daily_equity"],
                "sharpe": metrics["sharpe_daily_zero_rf"],
                "calmar": metrics["calmar_daily_equity_dd"],
                "turnover": metrics["turnover"],
                "tqqq_max_weight": metrics["tqqq_max_weight"],
                "neutral_annual_return": neutral["actual_path_annual_return"],
                "neutral_max_drawdown_daily_equity": neutral["max_drawdown_daily_equity"],
                "annual_return_delta_vs_neutral": round(
                    metrics["actual_path_annual_return"] - neutral["actual_path_annual_return"],
                    GRID_ROUND_DIGITS,
                ),
                "drawdown_delta_vs_neutral": round(
                    metrics["max_drawdown_daily_equity"] - neutral["max_drawdown_daily_equity"],
                    GRID_ROUND_DIGITS,
                ),
                "drawdown_not_worse": metrics["max_drawdown_daily_equity"]
                >= neutral["max_drawdown_daily_equity"],
                "defensive_probe_no_regression": metrics["max_drawdown_daily_equity"]
                >= neutral["max_drawdown_daily_equity"],
                **SAFETY_BOUNDARY,
            }
        )
    limited = _limited_adjustment_reference(limited_adjustment_reference_path, primary_window)
    rows.append(limited)
    modeled_rows = [row for row in rows if row["probe_id"] != "limited_adjustment_reference"]
    no_regression = all(bool(row["defensive_probe_no_regression"]) for row in modeled_rows)
    drawdown_not_worse = all(bool(row["drawdown_not_worse"]) for row in modeled_rows)
    status = (
        "DEFENSIVE_LANE_ACTUAL_PATH_READY_PROMOTION_BLOCKED"
        if no_regression and drawdown_not_worse
        else "DEFENSIVE_LANE_ACTUAL_PATH_NO_MATERIAL_IMPROVEMENT_PROMOTION_BLOCKED"
    )
    return _payload(
        report_type="defensive_lane_actual_path_matrix",
        title="Defensive Lane Actual-Path Matrix",
        status=status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "model_id": PREDICTION_MODEL_ID,
            "probe_count": len(rows),
            "modeled_defensive_probe_count": len(modeled_rows),
            "defensive_probe_no_regression": no_regression,
            "drawdown_not_worse": drawdown_not_worse,
            "add_risk_used": False,
            "risk_on_used": False,
            "tqqq_signal_used": False,
            "limited_adjustment_reference_included": True,
            "promotion_status": "blocked",
        },
        probe_rows=rows,
        model_summary=_mapping(model_review.get("summary")),
    )


def build_2022_slice_review(
    *,
    labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    predictions: pd.DataFrame,
    actual_path: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    slices = [
        ("2022_drawdown_slice", "2022-02-18", "2022-06-30"),
        ("2022_recovery_slice", "2022-07-01", "2022-10-31"),
        ("post_chatgpt_transition_slice", "2022-11-01", "2023-02-21"),
    ]
    rows = []
    for slice_id, start, end in slices:
        pred = _slice_by_date(predictions, start, end)
        lab = _slice_by_date(labels, start, end)
        feat = _slice_by_date(feature_matrix, start, end)
        rows.append(
            {
                "slice_id": slice_id,
                "start": start,
                "end": end,
                "prediction_count": len(pred),
                "label_count": len(lab),
                "risk_off_prediction_count": _count_state(pred, "trend_state", "risk_off"),
                "defensive_prediction_count": _count_state(pred, "trend_state", "defensive"),
                "neutral_prediction_count": _count_state(pred, "trend_state", "neutral"),
                "risk_off_label_count": int(lab.get("risk_off_needed", pd.Series(dtype=bool)).sum())
                if not lab.empty
                else 0,
                "avg_drawdown_depth": _mean(
                    feat.get("drawdown_depth_126d", pd.Series(dtype=float))
                ),
                "avg_realized_vol_20d": _mean(feat.get("realized_vol_20d", pd.Series(dtype=float))),
                "add_risk_used": False,
                "risk_on_used": False,
            }
        )
    return _payload(
        report_type="defensive_lane_2022_slice_matrix",
        title="Defensive Lane 2022 Slice Review",
        status="DEFENSIVE_LANE_2022_SLICE_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "slice_count": len(rows),
            "defensive_probe_no_regression": _mapping(actual_path.get("summary")).get(
                "defensive_probe_no_regression"
            ),
            "add_risk_used": False,
            "risk_on_used": False,
            "promotion_status": "blocked",
        },
        slice_rows=rows,
    )


def build_final_matrix(
    *,
    lane_policy: Mapping[str, Any],
    model_review: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_2022: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    actual_summary = _mapping(actual_path.get("summary"))
    model_summary = _mapping(model_review.get("summary"))
    no_regression = bool(actual_summary.get("defensive_probe_no_regression"))
    final_status = (
        "DEFENSIVE_LANE_NO_MATERIAL_IMPROVEMENT" if no_regression else "DEFENSIVE_LANE_ARCHIVED"
    )
    return _payload(
        report_type="defensive_preservation_lane_final_matrix",
        title="Defensive Preservation Lane Final Matrix",
        status=final_status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate["status"],
            "final_status": final_status,
            "defensive_probe_no_regression": no_regression,
            "drawdown_not_worse": bool(actual_summary.get("drawdown_not_worse")),
            "false_risk_off_cost_declined": False,
            "interpretation": "NO_MATERIAL_IMPROVEMENT_WITH_NO_DEFENSIVE_REGRESSION"
            if no_regression
            else "DEFENSIVE_LANE_ARCHIVED_DUE_TO_REGRESSION",
            "model_count": model_summary.get("model_count"),
            "feature_status": feature_audit.get("status"),
            "actual_path_status": actual_path.get("status"),
            "2022_slice_status": slice_2022.get("status"),
            "add_risk_disabled": True,
            "high_confidence_risk_on_disabled": True,
            "tqqq_signal_disabled": True,
            "gated_integration_allowed": False,
            "owner_review_allowed": False,
            "promotion_status": "blocked",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        phase_decision={
            "next_action": (
                "RUN_RETURN_SEEKING_DIAGNOSTIC_LANE_OR_REVIEW_DEFENSIVE_FEATURE_LIMITATIONS"
            ),
            "gated_integration_allowed_now": False,
            "reason": (
                "Phase 3 evidence is still missing; "
                "Phase 2 did not establish a promoted defensive lane."
            ),
        },
        artifact_paths=_artifact_paths(),
    )


def write_defensive_lane_outputs(
    *,
    lane_policy: Mapping[str, Any],
    labels: pd.DataFrame,
    feature_matrix: pd.DataFrame,
    predictions: pd.DataFrame,
    feature_audit: Mapping[str, Any],
    model_review: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    slice_2022: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
    output_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    _write_markdown(DEFAULT_SCOPE_DOC_PATH, _render_scope_doc(lane_policy, final_matrix))
    _write_csv(DEFAULT_LABELS_CSV_PATH, labels)
    _write_csv(DEFAULT_FEATURE_MATRIX_CSV_PATH, feature_matrix)
    _write_csv(DEFAULT_PREDICTIONS_CSV_PATH, predictions)
    _write_yaml(DEFAULT_FEATURE_AUDIT_YAML_PATH, feature_audit)
    _write_markdown(DEFAULT_FEATURE_AUDIT_DOC_PATH, _render_payload_doc(feature_audit))
    _write_yaml(DEFAULT_MODEL_MATRIX_YAML_PATH, model_review)
    _write_markdown(DEFAULT_MODEL_REVIEW_DOC_PATH, _render_model_doc(model_review))
    _write_yaml(DEFAULT_ACTUAL_PATH_YAML_PATH, actual_path)
    _write_markdown(DEFAULT_ACTUAL_PATH_DOC_PATH, _render_actual_path_doc(actual_path))
    _write_yaml(DEFAULT_2022_SLICE_YAML_PATH, slice_2022)
    _write_markdown(DEFAULT_2022_SLICE_DOC_PATH, _render_slice_doc(slice_2022))
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_closeout_doc(final_matrix))


def _risk_off_score(row: Mapping[str, Any], thresholds: Mapping[str, Any]) -> float:
    score = 0.0
    score += _float(row.get("drawdown_depth_126d")) <= _float(
        thresholds.get("risk_off_drawdown_depth"), -0.14
    )
    score += _float(row.get("drawdown_speed_20d")) <= _float(
        thresholds.get("risk_off_drawdown_speed"), -0.06
    )
    score += _float(row.get("realized_vol_20d")) >= _float(
        thresholds.get("risk_off_realized_vol_20d"), 0.32
    )
    score += _float(row.get("trend_break_20_60")) <= _float(
        thresholds.get("risk_off_trend_break_20_60"), -0.05
    )
    return float(score)


def _defensive_score(row: Mapping[str, Any], thresholds: Mapping[str, Any]) -> float:
    score = 0.0
    score += _float(row.get("drawdown_depth_126d")) <= _float(
        thresholds.get("defensive_drawdown_depth"), -0.08
    )
    score += _float(row.get("realized_vol_20d")) >= _float(
        thresholds.get("defensive_realized_vol_20d"), 0.24
    )
    score += _float(row.get("trend_break_20_60")) <= _float(
        thresholds.get("defensive_trend_break_20_60"), -0.025
    )
    score += _float(row.get("volatility_regime_score")) >= _float(
        thresholds.get("defensive_volatility_regime_score"), 0.35
    )
    return float(score)


def _re_risk_score(row: Mapping[str, Any], thresholds: Mapping[str, Any]) -> float:
    score = 0.0
    score += _float(row.get("recovery_from_60d_low")) >= _float(
        thresholds.get("re_risk_recovery_from_60d_low"), 0.08
    )
    score += _float(row.get("drawdown_speed_20d")) >= _float(
        thresholds.get("re_risk_drawdown_speed_floor"), 0.025
    )
    score += _float(row.get("realized_vol_20d")) <= _float(
        thresholds.get("re_risk_realized_vol_ceiling"), 0.28
    )
    return float(score)


def _binary_model_row(
    frame: pd.DataFrame,
    model_id: str,
    predicted_signal: str,
    label_column: str,
) -> dict[str, Any]:
    pred = frame["output_signal"].astype(str) == predicted_signal
    actual = frame[label_column].astype(bool)
    tp = int((pred & actual).sum())
    fp = int((pred & ~actual).sum())
    fn = int((~pred & actual).sum())
    return {
        "model_id": model_id,
        "predicted_positive_count": int(pred.sum()),
        "actual_positive_count": int(actual.sum()),
        "true_positive_count": tp,
        "false_positive_count": fp,
        "false_negative_count": fn,
        "precision": round(_ratio(tp, tp + fp), GRID_ROUND_DIGITS),
        "recall": round(_ratio(tp, tp + fn), GRID_ROUND_DIGITS),
        "model_status": "LOW_COMPLEXITY_DIAGNOSTIC_ONLY",
        "promotion_allowed": False,
    }


def _limited_adjustment_reference(path: Path, primary_window: Mapping[str, Any]) -> dict[str, Any]:
    source = _load_yaml_mapping(path) if path.exists() else {}
    row = next(
        (
            item
            for item in _records(source.get("owner_review_decisions"))
            if item.get("strategy_id") == "limited_adjustment"
        ),
        {},
    )
    metrics = _mapping(row.get("actual_path_metrics"))
    return {
        **window_metadata(primary_window),
        "probe_id": "limited_adjustment_reference",
        "role": "limited_adjustment_reference",
        "lane_role": "prior_dynamic_reference_only",
        "date_start": _mapping(source.get("date_range")).get("start", ""),
        "date_end": _mapping(source.get("date_range")).get("end", ""),
        "model_id": "limited_adjustment_reference",
        "annual_return": metrics.get("annual_return", 0.0),
        "max_drawdown_daily_equity": metrics.get("max_drawdown_daily_equity", 0.0),
        "sharpe": metrics.get("sharpe_daily_zero_rf", 0.0),
        "calmar": metrics.get("calmar_daily_equity_dd", 0.0),
        "turnover": metrics.get("turnover", 0.0),
        "tqqq_max_weight": None,
        "neutral_annual_return": None,
        "neutral_max_drawdown_daily_equity": None,
        "annual_return_delta_vs_neutral": None,
        "drawdown_delta_vs_neutral": None,
        "drawdown_not_worse": None,
        "defensive_probe_no_regression": None,
        "reference_source": str(path.relative_to(PROJECT_ROOT)),
        **SAFETY_BOUNDARY,
    }


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    primary_window: Mapping[str, Any],
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    metadata = window_metadata(primary_window)
    candidate_count = _candidate_count(summary, extra)
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
        "summary": dict(summary),
        **SAFETY_BOUNDARY,
        "research_audit_metadata": {
            "modified_layer": "first_layer",
            "frozen_first_layer_version": "first_layer_v2_return_seeking_diagnostic_only",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": metadata["research_window_id"],
            "label_version": "defensive_lane_label_taxonomy_v1",
            "feature_set_version": "defensive_lane_feature_matrix_v1",
            "model_version": PREDICTION_MODEL_ID,
            "threshold_policy": "defensive_lane_action_value_policy_v1",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": candidate_count,
            "pre_registered_selection_rule": True,
        },
        **extra,
    }


def _candidate_count(summary: Mapping[str, Any], extra: Mapping[str, Any]) -> int:
    for key in (
        "feature_row_count",
        "prediction_count",
        "probe_count",
        "slice_count",
        "model_count",
    ):
        if key in summary:
            return max(0, _int(summary.get(key)))
    for key in ("probe_rows", "slice_rows", "model_rows", "feature_rows"):
        if key in extra:
            return len(_records(extra.get(key)))
    return 0


def _render_scope_doc(lane_policy: Mapping[str, Any], final_matrix: Mapping[str, Any]) -> str:
    summary = _mapping(final_matrix.get("summary"))
    return (
        "\n".join(
            [
                "# Defensive Preservation Lane Scope",
                "",
                f"- 状态：`{lane_policy.get('status')}`",
                "- modified_layer：`first_layer`",
                "- frozen_second_layer：`dynamic_second_layer_probe_registry_v2`",
                "- frozen probes：`defensive_overlay_probe`, "
                "`drawdown_control_probe`, `limited_adjustment_reference`",
                "- add_risk_disabled：`true`",
                "- high_confidence_risk_on_disabled：`true`",
                "- tqqq_signal_disabled：`true`",
                "- promotion_allowed：`false`",
                "- paper_shadow_allowed：`false`",
                "- production_allowed：`false`",
                "- broker_action：`none`",
                "",
                "## 结论",
                "",
                (
                    f"本轮 final status 为 `{summary.get('final_status')}`。"
                    "该 lane 只评估 defensive preservation，"
                ),
                "不产生 add-risk、risk-on 或 TQQQ 信号，也不启用 gated integration。",
            ]
        )
        + "\n"
    )


def _render_payload_doc(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- 市场阶段：`{payload.get('market_regime')}`",
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


def _render_model_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Models", ""]
    lines.append("| model_id | predicted | actual | precision | recall | status |")
    lines.append("|---|---:|---:|---:|---:|---|")
    for row in _records(payload.get("model_rows")):
        lines.append(
            "| {model_id} | {predicted_positive_count} | {actual_positive_count} | "
            "{precision} | {recall} | {model_status} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_actual_path_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Actual Path Rows", ""]
    lines.append("| probe_id | annual_return | max_drawdown | delta_vs_neutral | no_regression |")
    lines.append("|---|---:|---:|---:|---|")
    for row in _records(payload.get("probe_rows")):
        lines.append(
            "| {probe_id} | {annual_return} | {max_drawdown_daily_equity} | "
            "{drawdown_delta_vs_neutral} | {defensive_probe_no_regression} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_slice_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## 2022 Slices", ""]
    lines.append(
        "| slice_id | predictions | risk_off_pred | defensive_pred | "
        "neutral_pred | risk_off_labels |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in _records(payload.get("slice_rows")):
        lines.append(
            "| {slice_id} | {prediction_count} | {risk_off_prediction_count} | "
            "{defensive_prediction_count} | {neutral_prediction_count} | "
            "{risk_off_label_count} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_closeout_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Closeout", ""]
    summary = _mapping(payload.get("summary"))
    for key in (
        "final_status",
        "defensive_probe_no_regression",
        "drawdown_not_worse",
        "interpretation",
        "add_risk_disabled",
        "gated_integration_allowed",
        "promotion_status",
    ):
        lines.append(f"- {key}: `{summary.get(key)}`")
    return "\n".join(lines) + "\n"


def _artifact_paths() -> dict[str, str]:
    paths = {
        "lane_policy": DEFAULT_DEFENSIVE_LANE_POLICY_PATH,
        "label_taxonomy": DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH,
        "action_value_policy": DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH,
        "scope_doc": DEFAULT_SCOPE_DOC_PATH,
        "labels_csv": DEFAULT_LABELS_CSV_PATH,
        "feature_matrix_csv": DEFAULT_FEATURE_MATRIX_CSV_PATH,
        "feature_pit_audit_doc": DEFAULT_FEATURE_AUDIT_DOC_PATH,
        "feature_pit_audit_yaml": DEFAULT_FEATURE_AUDIT_YAML_PATH,
        "model_review_doc": DEFAULT_MODEL_REVIEW_DOC_PATH,
        "model_matrix_yaml": DEFAULT_MODEL_MATRIX_YAML_PATH,
        "predictions_csv": DEFAULT_PREDICTIONS_CSV_PATH,
        "actual_path_doc": DEFAULT_ACTUAL_PATH_DOC_PATH,
        "actual_path_yaml": DEFAULT_ACTUAL_PATH_YAML_PATH,
        "slice_2022_doc": DEFAULT_2022_SLICE_DOC_PATH,
        "slice_2022_yaml": DEFAULT_2022_SLICE_YAML_PATH,
        "closeout_doc": DEFAULT_CLOSEOUT_DOC_PATH,
        "final_matrix_yaml": DEFAULT_FINAL_MATRIX_YAML_PATH,
    }
    return {key: str(path.relative_to(PROJECT_ROOT)) for key, path in paths.items()}


def _slice_by_date(frame: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if frame.empty or "date" not in frame:
        return pd.DataFrame()
    return frame.loc[(frame["date"].astype(str) >= start) & (frame["date"].astype(str) <= end)]


def _count_state(frame: pd.DataFrame, column: str, state: str) -> int:
    if frame.empty or column not in frame:
        return 0
    return int((frame[column].astype(str) == state).sum())


def _worst_window_return(returns: pd.Series, window: int) -> float:
    if returns.empty:
        return 0.0
    compounded = (1.0 + returns).rolling(window, min_periods=1).apply(np.prod, raw=True) - 1.0
    return float(compounded.min())


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _mean(value: object) -> float:
    if not isinstance(value, pd.Series) or value.empty:
        return 0.0
    return round(float(pd.to_numeric(value, errors="coerce").fillna(0.0).mean()), GRID_ROUND_DIGITS)


def _max_value(value: object) -> float:
    if not isinstance(value, pd.Series) or value.empty:
        return 0.0
    return round(float(pd.to_numeric(value, errors="coerce").fillna(0.0).max()), GRID_ROUND_DIGITS)


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


def _records(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


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
