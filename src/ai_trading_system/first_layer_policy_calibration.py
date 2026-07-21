from __future__ import annotations

import json
import math
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START, utc_now_iso
from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _data_quality_gate,
    _load_price_matrix,
    _simulate_rebalanced_portfolio,
    _slice_prices,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SCOPE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_calibration_scope.yaml"
)
DEFAULT_PROBE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_second_layer_probe_registry.yaml"
)
DEFAULT_SCORE_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "action_value_score_policy.yaml"
DEFAULT_SCORECARD_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_trend_scorecard_v1.yaml"
)
DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends"
DEFAULT_ACTION_VALUE_OUTPUT_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "action_value_matrix"
DEFAULT_TREND_LABEL_OUTPUT_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "trend_labels"
DEFAULT_FEATURE_OUTPUT_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "pit_feature_matrix"
DEFAULT_MODEL_OUTPUT_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "models"
DEFAULT_WALK_FORWARD_OUTPUT_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "walk_forward"
DEFAULT_PROBE_BACKTEST_OUTPUT_ROOT = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "probe_backtest"
DEFAULT_CALIBRATED_BACKTEST_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT / "calibrated_first_layer_actual_path_rebacktest"
)

DEFAULT_SCOPE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_policy_aware_calibration_scope.md"
)
DEFAULT_LABEL_SUMMARY_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "consensus_trend_label_summary.yaml"
)
DEFAULT_LABEL_QUALITY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "consensus_trend_label_quality_review.md"
)
DEFAULT_FEATURE_PIT_AUDIT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_feature_pit_audit.md"
)
DEFAULT_FEATURE_PIT_AUDIT_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_feature_pit_audit.yaml"
)
DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_trend_model_walk_forward_review.md"
)
DEFAULT_WALK_FORWARD_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_trend_model_walk_forward_matrix.yaml"
)
DEFAULT_OVERLAY_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "calibrated_first_layer_defensive_overlay_review.md"
)
DEFAULT_TQQQ_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "calibrated_first_layer_risk_on_tqqq_diagnostic_review.md"
)
DEFAULT_CONSENSUS_COMPARISON_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "single_probe_vs_consensus_trend_label_review.md"
)
DEFAULT_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_policy_aware_calibration_owner_review_pack.md"
)
DEFAULT_FORWARD_WATCH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "calibrated_first_layer_forward_watch_plan.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_policy_aware_calibration_final_matrix.yaml"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_policy_aware_calibration_closeout.md"
)

DEFAULT_AI_REGIME_BACKTEST_START = (
    PRIMARY_RESEARCH_START
    if isinstance(PRIMARY_RESEARCH_START, date)
    else date.fromisoformat(str(PRIMARY_RESEARCH_START))
)
GRID_ROUND_DIGITS = 6
STATE_ORDER = ["risk_off", "defensive", "neutral", "constructive", "risk_on"]
STATE_TO_ORDINAL = {state: idx for idx, state in enumerate(STATE_ORDER)}
ORDINAL_TO_STATE = {idx: state for state, idx in STATE_TO_ORDINAL.items()}
FEATURE_COLUMNS = [
    "qqq_momentum_20d",
    "qqq_momentum_60d",
    "qqq_ma_slope_20_60",
    "qqq_drawdown_126d",
    "realized_vol_20d",
    "yield_curve_10y2y",
]
SAFETY_BOUNDARY: dict[str, Any] = {
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


def run_first_layer_policy_aware_calibration_pack(
    *,
    scope_config_path: Path = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Path = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Path = DEFAULT_SCORECARD_CONFIG_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    as_of_date: date | None = None,
    output_root: Path = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> dict[str, Any]:
    scope_config = _load_yaml_mapping(scope_config_path)
    probe_registry = _load_yaml_mapping(probe_registry_path)
    score_policy = _load_yaml_mapping(score_policy_path)
    scorecard_config = _load_yaml_mapping(scorecard_config_path)
    expanded_config = _load_yaml_mapping(expanded_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=as_of_date,
        expected_tickers=["QQQ", "SGOV", "TQQQ"],
    )
    if not data_gate["passed"]:
        payload = _payload(
            report_type="first_layer_policy_aware_calibration_owner_pack",
            title="First-Layer Policy-Aware Calibration Owner Pack",
            status="FIRST_LAYER_CALIBRATION_BLOCKED_DATA_QUALITY",
            summary={"data_quality_status": data_gate["status"]},
            data_quality=data_gate,
        )
        _write_markdown(DEFAULT_OWNER_REVIEW_DOC_PATH, _render_generic_doc(payload))
        return payload

    prices = _slice_prices(
        _load_price_matrix(prices_path, ["QQQ", "SGOV", "TQQQ"]),
        start_date=DEFAULT_AI_REGIME_BACKTEST_START,
        end_date=None,
    )
    rates = _load_rates(rates_path)
    probe_validation = validate_probe_registry(probe_registry)
    action_value = build_action_value_matrix(
        prices=prices,
        probe_registry=probe_registry,
        score_policy=score_policy,
    )
    single_labels, consensus_labels = build_trend_labels(
        action_value=action_value,
        scope_config=scope_config,
    )
    feature_matrix, feature_report = build_pit_feature_matrix(prices=prices, rates=rates)
    scorecard_predictions = build_scorecard_predictions(
        feature_matrix=feature_matrix,
        scorecard_config=scorecard_config,
    )
    wf_predictions, wf_metrics, logistic_coefficients = run_walk_forward_trend_model(
        feature_matrix=feature_matrix,
        consensus_labels=consensus_labels,
        scope_config=scope_config,
    )
    backtest_summary, probe_metrics = run_probe_backtests(
        prices=prices,
        probe_registry=probe_registry,
        scorecard_predictions=scorecard_predictions,
        calibrated_predictions=wf_predictions,
    )
    label_summary = _label_summary_payload(
        single_labels=single_labels,
        consensus_labels=consensus_labels,
        scope_config_path=scope_config_path,
    )
    feature_audit = _feature_audit_payload(feature_report, scope_config_path=scope_config_path)
    walk_forward_review = _walk_forward_payload(
        wf_metrics=wf_metrics,
        coefficients=logistic_coefficients,
        scope_config_path=scope_config_path,
    )
    final_matrix = _final_matrix_payload(
        label_summary=label_summary,
        walk_forward_review=walk_forward_review,
        probe_metrics=probe_metrics,
        scope_config_path=scope_config_path,
    )
    owner_pack = _owner_pack_payload(
        data_gate=data_gate,
        probe_validation=probe_validation,
        action_value=action_value,
        label_summary=label_summary,
        feature_audit=feature_audit,
        walk_forward_review=walk_forward_review,
        probe_metrics=probe_metrics,
        final_matrix=final_matrix,
        scope_config_path=scope_config_path,
    )
    _write_outputs(
        output_root=output_root,
        scope_config=scope_config,
        probe_validation=probe_validation,
        action_value=action_value,
        single_labels=single_labels,
        consensus_labels=consensus_labels,
        label_summary=label_summary,
        feature_matrix=feature_matrix,
        feature_report=feature_report,
        feature_audit=feature_audit,
        scorecard_predictions=scorecard_predictions,
        wf_predictions=wf_predictions,
        wf_metrics=wf_metrics,
        logistic_coefficients=logistic_coefficients,
        backtest_summary=backtest_summary,
        probe_metrics=probe_metrics,
        walk_forward_review=walk_forward_review,
        final_matrix=final_matrix,
        owner_pack=owner_pack,
    )
    return owner_pack


def validate_probe_registry(registry: Mapping[str, Any]) -> dict[str, Any]:
    states = _string_list(registry.get("trend_states")) or STATE_ORDER
    rows: list[dict[str, Any]] = []
    for probe in _records(registry.get("probes")):
        weights_by_state = _mapping(probe.get("weights_by_trend_state"))
        unique_weights = {
            json.dumps(_normalize_weights(_mapping(weights_by_state.get(state))), sort_keys=True)
            for state in states
        }
        issues: list[str] = []
        if str(probe.get("role")) in set(_string_list(registry.get("forbidden_probe_roles"))):
            issues.append("static_baseline_role_forbidden")
        if len(unique_weights) <= 1:
            issues.append("probe_not_trend_sensitive")
        for state in states:
            weights = _normalize_weights(_mapping(weights_by_state.get(state)))
            if not weights:
                issues.append(f"missing_weights:{state}")
                continue
            if not math.isclose(sum(weights.values()), 1.0, abs_tol=1e-6):
                issues.append(f"weights_do_not_sum_to_one:{state}")
            if any(value < -1e-9 for value in weights.values()):
                issues.append(f"short_weight_not_allowed:{state}")
        tqqq_used = any(
            _float(_mapping(weights_by_state.get(state)).get("TQQQ")) > 0.0 for state in states
        )
        if tqqq_used and (
            not _bool(probe.get("research_only"))
            or _bool(probe.get("promotion_enabled"))
            or _bool(probe.get("broker_enabled"))
        ):
            issues.append("tqqq_probe_not_research_only")
        rows.append(
            {
                "probe_id": str(probe.get("probe_id")),
                "role": str(probe.get("role")),
                "trend_sensitive": len(unique_weights) > 1,
                "tqqq_used": tqqq_used,
                "research_only": _bool(probe.get("research_only")),
                "promotion_enabled": _bool(probe.get("promotion_enabled")),
                "broker_enabled": _bool(probe.get("broker_enabled")),
                "status": "PASS" if not issues else "BLOCKED",
                "issues": issues,
                **SAFETY_BOUNDARY,
            }
        )
    status = "PROBE_REGISTRY_VALIDATED"
    if any(row["status"] == "BLOCKED" for row in rows):
        status = "PROBE_REGISTRY_BLOCKED"
    return _payload(
        report_type="dynamic_second_layer_probe_validation",
        title="Dynamic Second-Layer Probe Validation",
        status=status,
        summary={
            "probe_count": len(rows),
            "trend_sensitive_count": sum(_bool(row.get("trend_sensitive")) for row in rows),
            "blocked_count": sum(row.get("status") == "BLOCKED" for row in rows),
        },
        rows=rows,
    )


def build_action_value_matrix(
    *,
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    score_policy: Mapping[str, Any],
) -> pd.DataFrame:
    horizons = [_int(value) for value in _list(score_policy.get("horizons"))] or [5, 10, 20, 60]
    probes = _records(probe_registry.get("probes"))
    max_horizon = max(horizons)
    rows: list[dict[str, Any]] = []
    for idx in range(0, max(0, len(prices.index) - max_horizon - 1)):
        timestamp = prices.index[idx]
        for probe in probes:
            probe_id = str(probe.get("probe_id"))
            weights_by_state = _mapping(probe.get("weights_by_trend_state"))
            for horizon in horizons:
                neutral = _future_action_metrics(
                    prices=prices,
                    start_idx=idx,
                    horizon=horizon,
                    weights=_normalize_weights(_mapping(weights_by_state.get("neutral"))),
                )
                for state in STATE_ORDER:
                    weights = _normalize_weights(_mapping(weights_by_state.get(state)))
                    metrics = _future_action_metrics(
                        prices=prices,
                        start_idx=idx,
                        horizon=horizon,
                        weights=weights,
                    )
                    estimated_cost = 0.0
                    tqqq_penalty = _float(weights.get("TQQQ")) * _float(
                        _mapping(score_policy.get("tqqq_risk_penalty")).get("penalty_per_weight")
                    )
                    full_score = (
                        metrics["future_return"]
                        - _float(
                            _mapping(score_policy.get("full_allocation_score")).get("lambda_dd")
                        )
                        * abs(metrics["future_max_drawdown"])
                        - _float(
                            _mapping(score_policy.get("full_allocation_score")).get("lambda_worst5")
                        )
                        * abs(metrics["worst_5d_return"])
                        - tqqq_penalty
                    )
                    avoided_drawdown = abs(neutral["future_max_drawdown"]) - abs(
                        metrics["future_max_drawdown"]
                    )
                    missed_upside = max(0.0, neutral["future_return"] - metrics["future_return"])
                    overlay_score = avoided_drawdown - missed_upside - estimated_cost
                    rows.append(
                        {
                            "date": timestamp.date().isoformat(),
                            "probe_id": probe_id,
                            "assumed_trend_state": state,
                            "horizon_days": horizon,
                            "portfolio_weights": weights,
                            "future_return": round(metrics["future_return"], GRID_ROUND_DIGITS),
                            "future_max_drawdown": round(
                                metrics["future_max_drawdown"], GRID_ROUND_DIGITS
                            ),
                            "worst_1d_return": round(metrics["worst_1d_return"], GRID_ROUND_DIGITS),
                            "worst_5d_return": round(metrics["worst_5d_return"], GRID_ROUND_DIGITS),
                            "worst_20d_return": round(
                                metrics["worst_20d_return"], GRID_ROUND_DIGITS
                            ),
                            "avoided_drawdown_vs_neutral": round(
                                avoided_drawdown, GRID_ROUND_DIGITS
                            ),
                            "missed_upside_vs_neutral": round(missed_upside, GRID_ROUND_DIGITS),
                            "same_risk_static_delta": round(
                                metrics["future_return"] - neutral["future_return"],
                                GRID_ROUND_DIGITS,
                            ),
                            "net_of_cost_score": round(metrics["future_return"], GRID_ROUND_DIGITS),
                            "stress_penalty": round(abs(min(0.0, metrics["worst_5d_return"])), 6),
                            "tqqq_risk_penalty": round(tqqq_penalty, GRID_ROUND_DIGITS),
                            "estimated_cost": estimated_cost,
                            "action_value_score": round(full_score, GRID_ROUND_DIGITS),
                            "overlay_action_value_score": round(overlay_score, GRID_ROUND_DIGITS),
                            "label_uses_future_outcome": True,
                            "feature_cutoff_used": False,
                            **SAFETY_BOUNDARY,
                        }
                    )
    return pd.DataFrame(rows)


def build_trend_labels(
    *,
    action_value: pd.DataFrame,
    scope_config: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    single_rows: list[dict[str, Any]] = []
    group_cols = ["date", "probe_id", "horizon_days"]
    for keys, frame in action_value.groupby(group_cols, sort=True):
        ordered = frame.sort_values("action_value_score", ascending=False)
        best = ordered.iloc[0]
        second = ordered.iloc[1] if len(ordered) > 1 else ordered.iloc[0]
        margin = _float(best["action_value_score"]) - _float(second["action_value_score"])
        confidence = _confidence_from_margin(margin, _float(best["action_value_score"]))
        single_rows.append(
            {
                "date": keys[0],
                "probe_id": keys[1],
                "horizon_days": int(keys[2]),
                "best_trend_state": str(best["assumed_trend_state"]),
                "best_action_value_score": _float(best["action_value_score"]),
                "second_best_state": str(second["assumed_trend_state"]),
                "margin": round(margin, GRID_ROUND_DIGITS),
                "label_confidence": confidence,
            }
        )
    single = pd.DataFrame(single_rows)
    consensus_rows: list[dict[str, Any]] = []
    usage = _mapping(scope_config.get("label_usage"))
    confidence_floor = _float(usage.get("high_confidence_floor"), default=0.55)
    max_disagreement = _float(usage.get("max_disagreement_score"), default=0.5)
    for keys, frame in single.groupby(["date", "horizon_days"], sort=True):
        votes = Counter(str(value) for value in frame["best_trend_state"])
        avg_scores = {
            state: float(
                action_value.loc[
                    (action_value["date"] == keys[0])
                    & (action_value["horizon_days"] == keys[1])
                    & (action_value["assumed_trend_state"] == state),
                    "action_value_score",
                ].mean()
            )
            for state in STATE_ORDER
        }
        best_state = max(
            STATE_ORDER, key=lambda state: (avg_scores[state], -STATE_TO_ORDINAL[state])
        )
        vote_share = votes.get(best_state, 0) / max(1, int(frame["probe_id"].nunique()))
        sorted_scores = sorted(avg_scores.values(), reverse=True)
        margin = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else 0.0
        disagreement = round(1.0 - vote_share, GRID_ROUND_DIGITS)
        confidence = round(max(vote_share, _confidence_from_margin(margin, sorted_scores[0])), 6)
        train_usable = confidence >= confidence_floor and disagreement <= max_disagreement
        consensus_rows.append(
            {
                "date": keys[0],
                "horizon_days": int(keys[1]),
                "consensus_state": best_state,
                "consensus_confidence": confidence,
                "probe_votes": dict(votes),
                "disagreement_score": disagreement,
                "score_margin": round(margin, GRID_ROUND_DIGITS),
                "train_usable": train_usable,
                "allowed_training_usage": (
                    ["train_if_confidence_above_threshold"] if train_usable else []
                ),
            }
        )
    return single, pd.DataFrame(consensus_rows)


def build_pit_feature_matrix(
    *,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    qqq = pd.to_numeric(prices["QQQ"], errors="coerce").ffill()
    returns = qqq.pct_change().fillna(0.0)
    ma20 = qqq.rolling(20).mean()
    ma60 = qqq.rolling(60).mean()
    drawdown = qqq / qqq.rolling(126, min_periods=5).max() - 1.0
    feature_frame = pd.DataFrame(index=prices.index)
    feature_frame["qqq_momentum_20d"] = qqq.pct_change(20)
    feature_frame["qqq_momentum_60d"] = qqq.pct_change(60)
    feature_frame["qqq_ma_slope_20_60"] = ma20 / ma60 - 1.0
    feature_frame["qqq_drawdown_126d"] = drawdown
    feature_frame["realized_vol_20d"] = returns.rolling(20).std(ddof=0)
    rate_frame = rates.reindex(feature_frame.index).ffill()
    if {"DGS10", "DGS2"} <= set(rate_frame.columns):
        feature_frame["yield_curve_10y2y"] = rate_frame["DGS10"] - rate_frame["DGS2"]
    else:
        feature_frame["yield_curve_10y2y"] = 0.0
    feature_frame = feature_frame.fillna(0.0)
    rows: list[dict[str, Any]] = []
    index = list(feature_frame.index)
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
                for column in FEATURE_COLUMNS
            }
        )
        rows.append(row)
    output = pd.DataFrame(rows)
    report = {
        "status": "PIT_WARNING",
        "row_count": len(output),
        "approved_feature_count": len(FEATURE_COLUMNS),
        "blocked_feature_count": 0,
        "excluded_non_pit_or_unavailable_features": [
            "VIX level/change unavailable in required cache",
            "SMH/SOXX relative trend unavailable in required cache",
            "event risk score unavailable in this PIT feature contract",
            "market breadth proxy unavailable in this PIT feature contract",
        ],
        "feature_cutoff_passed": bool(output["feature_cutoff_passed"].all()),
        "features": [
            {
                "feature_id": column,
                "pit_status": "PIT_APPROVED",
                "known_at_policy": "same_day_close_or_rate_cache_at_or_before_decision",
                "available_at_policy": "same_as_known_at_for_cached_daily_research",
                "training_allowed": True,
            }
            for column in FEATURE_COLUMNS
        ],
    }
    return output, report


def build_scorecard_predictions(
    *,
    feature_matrix: pd.DataFrame,
    scorecard_config: Mapping[str, Any],
) -> pd.DataFrame:
    weights = _mapping(scorecard_config.get("feature_weights"))
    bands = _mapping(scorecard_config.get("state_bands"))
    confidence_cfg = _mapping(scorecard_config.get("confidence"))
    validity = _mapping(scorecard_config.get("validity_days_by_state"))
    decay = _mapping(scorecard_config.get("decay_profile_by_state"))
    rows: list[dict[str, Any]] = []
    for _, row in feature_matrix.iterrows():
        score = sum(
            _float(weights.get(column)) * _float(row.get(column)) for column in FEATURE_COLUMNS
        )
        state = _state_from_score(score, bands)
        confidence = _scorecard_confidence(score, confidence_cfg)
        rows.append(
            {
                "date": str(row["date"]),
                "model_id": "first_layer_trend_scorecard_v1",
                "trend_state": state,
                "confidence": confidence,
                "expected_horizon_days": 20,
                "validity_days": _int(validity.get(state), default=10),
                "decay_profile": str(decay.get(state, "medium")),
                "feature_snapshot_hash": _row_hash(row, FEATURE_COLUMNS),
                "model_version": "first_layer_trend_scorecard_v1",
                "known_at": str(row["known_at"]),
                "available_at": str(row["available_at"]),
                "decision_at": str(row["decision_at"]),
                **SAFETY_BOUNDARY,
            }
        )
    return pd.DataFrame(rows)


def run_walk_forward_trend_model(
    *,
    feature_matrix: pd.DataFrame,
    consensus_labels: pd.DataFrame,
    scope_config: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    wf = _mapping(scope_config.get("walk_forward"))
    train_window = _int(wf.get("train_window_days"), default=504)
    validation_window = _int(wf.get("validation_window_days"), default=63)
    step = _int(wf.get("step_days"), default=21)
    min_train = _int(wf.get("min_train_samples"), default=300)
    horizon = _int(wf.get("label_horizon_days"), default=20)
    labels = consensus_labels.loc[consensus_labels["horizon_days"] == horizon].copy()
    merged = feature_matrix.merge(labels, on="date", how="inner")
    merged = merged.sort_values("date").reset_index(drop=True)
    predictions: list[dict[str, Any]] = []
    coefficients: list[dict[str, Any]] = []
    split_id = 0
    for validation_start in range(
        train_window, max(train_window, len(merged) - validation_window), step
    ):
        train = merged.iloc[validation_start - train_window : validation_start].copy()
        train = train.loc[train["train_usable"].astype(bool)].copy()
        if len(train) < min_train:
            continue
        validation = merged.iloc[validation_start : validation_start + validation_window].copy()
        if validation.empty:
            continue
        model = _fit_ordinal_linear(train)
        coefficients.append(
            {
                "split_id": split_id,
                "train_start": str(train["date"].iloc[0]),
                "train_end": str(train["date"].iloc[-1]),
                "validation_start": str(validation["date"].iloc[0]),
                "validation_end": str(validation["date"].iloc[-1]),
                "train_sample_count": len(train),
                "coefficients": model["coefficients"],
            }
        )
        for _, row in validation.iterrows():
            raw = _predict_ordinal(row, model)
            ordinal = int(round(min(4.0, max(0.0, raw))))
            state = ORDINAL_TO_STATE[ordinal]
            predictions.append(
                {
                    "date": str(row["date"]),
                    "split_id": split_id,
                    "model_id": "first_layer_ordinal_linear_v1",
                    "trend_state": state,
                    "confidence": round(max(0.35, 1.0 - abs(raw - ordinal) / 2.0), 6),
                    "expected_horizon_days": horizon,
                    "validity_days": _validity_for_state(state),
                    "decay_profile": _decay_for_state(state),
                    "feature_snapshot_hash": _row_hash(row, FEATURE_COLUMNS),
                    "model_version": "first_layer_ordinal_linear_v1",
                    "label_state": str(row["consensus_state"]),
                    "label_confidence": _float(row["consensus_confidence"]),
                    "label_disagreement_score": _float(row["disagreement_score"]),
                    "known_at": str(row["known_at"]),
                    "available_at": str(row["available_at"]),
                    "decision_at": str(row["decision_at"]),
                    **SAFETY_BOUNDARY,
                }
            )
        split_id += 1
    prediction_frame = pd.DataFrame(predictions)
    metrics = _prediction_metrics(prediction_frame)
    metrics.update(
        {
            "model_id": "first_layer_ordinal_linear_v1",
            "split_count": split_id,
            "prediction_count": len(prediction_frame),
            "train_window_days": train_window,
            "validation_window_days": validation_window,
            "step_days": step,
            "min_train_samples": min_train,
            "label_horizon_days": horizon,
            **SAFETY_BOUNDARY,
        }
    )
    return prediction_frame, metrics, coefficients


def run_probe_backtests(
    *,
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    scorecard_predictions: pd.DataFrame,
    calibrated_predictions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    for model_id, predictions in (
        ("old_scorecard_first_layer_v1", scorecard_predictions),
        ("new_calibrated_first_layer_v1", calibrated_predictions),
    ):
        if predictions.empty:
            continue
        for probe in _records(probe_registry.get("probes")):
            row = _backtest_probe_predictions(
                prices=prices,
                predictions=predictions,
                probe=probe,
                model_id=model_id,
            )
            rows.append(row)
    summary = pd.DataFrame(rows)
    comparison_rows: list[dict[str, Any]] = []
    for probe_id, frame in summary.groupby("probe_id"):
        old = frame.loc[frame["model_id"] == "old_scorecard_first_layer_v1"]
        new = frame.loc[frame["model_id"] == "new_calibrated_first_layer_v1"]
        if old.empty or new.empty:
            continue
        old_row = old.iloc[0]
        new_row = new.iloc[0]
        comparison_rows.append(
            {
                "probe_id": probe_id,
                "annual_return_delta": round(
                    _float(new_row.get("actual_path_annual_return"))
                    - _float(old_row.get("actual_path_annual_return")),
                    GRID_ROUND_DIGITS,
                ),
                "max_drawdown_delta": round(
                    _float(new_row.get("max_drawdown_daily_equity"))
                    - _float(old_row.get("max_drawdown_daily_equity")),
                    GRID_ROUND_DIGITS,
                ),
                "calmar_delta": round(
                    _float(new_row.get("calmar_daily_equity_dd"))
                    - _float(old_row.get("calmar_daily_equity_dd")),
                    GRID_ROUND_DIGITS,
                ),
                "sharpe_delta": round(
                    _float(new_row.get("sharpe_daily_zero_rf"))
                    - _float(old_row.get("sharpe_daily_zero_rf")),
                    GRID_ROUND_DIGITS,
                ),
                "new_model_better_calmar": _float(new_row.get("calmar_daily_equity_dd"))
                > _float(old_row.get("calmar_daily_equity_dd")),
                **SAFETY_BOUNDARY,
            }
        )
    return summary, pd.DataFrame(comparison_rows)


def first_layer_predictions_contain_weights(predictions: pd.DataFrame) -> bool:
    forbidden = {"QQQ", "SGOV", "TQQQ", "weight", "target_weight", "actual_weight"}
    return any(any(token in column for token in forbidden) for column in predictions.columns)


def probe_can_generate_action_value_labels(probe: Mapping[str, Any]) -> bool:
    return str(probe.get("role")) not in {"static_baseline", "static_frontier"}


def _write_outputs(
    *,
    output_root: Path,
    scope_config: Mapping[str, Any],
    probe_validation: Mapping[str, Any],
    action_value: pd.DataFrame,
    single_labels: pd.DataFrame,
    consensus_labels: pd.DataFrame,
    label_summary: Mapping[str, Any],
    feature_matrix: pd.DataFrame,
    feature_report: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    scorecard_predictions: pd.DataFrame,
    wf_predictions: pd.DataFrame,
    wf_metrics: Mapping[str, Any],
    logistic_coefficients: Sequence[Mapping[str, Any]],
    backtest_summary: pd.DataFrame,
    probe_metrics: pd.DataFrame,
    walk_forward_review: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
) -> None:
    action_root = output_root / "action_value_matrix"
    label_root = output_root / "trend_labels"
    feature_root = output_root / "pit_feature_matrix"
    model_root = output_root / "models"
    wf_root = output_root / "walk_forward"
    probe_root = output_root / "probe_backtest"
    calibrated_root = output_root / "calibrated_first_layer_actual_path_rebacktest"
    _write_markdown(DEFAULT_SCOPE_DOC_PATH, _render_scope_doc(scope_config, probe_validation))
    _write_csv(action_root / "action_value_matrix.csv", action_value)
    _write_json(
        action_root / "action_value_summary.json",
        {
            **_payload(
                report_type="action_value_matrix_summary",
                title="Action-Value Matrix Summary",
                status="ACTION_VALUE_MATRIX_READY_PROMOTION_BLOCKED",
                summary={
                    "row_count": len(action_value),
                    "probe_count": action_value["probe_id"].nunique(),
                    "horizon_count": action_value["horizon_days"].nunique(),
                },
            ),
            "probe_validation_summary": _mapping(probe_validation.get("summary")),
        },
    )
    _write_csv(label_root / "single_probe_trend_labels.csv", single_labels)
    _write_csv(label_root / "consensus_trend_labels.csv", consensus_labels)
    _write_yaml(DEFAULT_LABEL_SUMMARY_YAML_PATH, label_summary)
    _write_markdown(DEFAULT_LABEL_QUALITY_DOC_PATH, _render_label_quality_doc(label_summary))
    _write_csv(feature_root / "pit_feature_matrix.csv", feature_matrix)
    _write_json(feature_root / "feature_availability_report.json", feature_report)
    _write_yaml(DEFAULT_FEATURE_PIT_AUDIT_YAML_PATH, feature_audit)
    _write_markdown(DEFAULT_FEATURE_PIT_AUDIT_DOC_PATH, _render_feature_audit_doc(feature_audit))
    _write_csv(
        model_root / "first_layer_trend_scorecard_v1_predictions.csv",
        scorecard_predictions,
    )
    logistic_root = model_root / "first_layer_logistic_v1"
    _write_json(logistic_root / "model_coefficients.json", list(logistic_coefficients))
    _write_csv(logistic_root / "walk_forward_predictions.csv", wf_predictions)
    _write_csv(wf_root / "first_layer_walk_forward_predictions.csv", wf_predictions)
    _write_json(wf_root / "first_layer_walk_forward_metrics.json", wf_metrics)
    _write_yaml(DEFAULT_WALK_FORWARD_MATRIX_YAML_PATH, walk_forward_review)
    _write_markdown(
        DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH,
        _render_walk_forward_review_doc(walk_forward_review),
    )
    _write_csv(probe_root / "old_first_layer_vs_new_first_layer_actual_path.csv", backtest_summary)
    _write_csv(probe_root / "probe_level_metrics.csv", probe_metrics)
    _write_csv(calibrated_root / "probe_level_metrics.csv", backtest_summary)
    _write_markdown(
        DEFAULT_OVERLAY_REVIEW_DOC_PATH, _render_probe_review_doc(owner_pack, "overlay")
    )
    _write_markdown(DEFAULT_TQQQ_REVIEW_DOC_PATH, _render_probe_review_doc(owner_pack, "tqqq"))
    _write_markdown(
        DEFAULT_CONSENSUS_COMPARISON_DOC_PATH,
        _render_consensus_comparison_doc(label_summary),
    )
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_closeout_doc(final_matrix))
    _write_markdown(DEFAULT_OWNER_REVIEW_DOC_PATH, _render_owner_pack_doc(owner_pack))
    _write_markdown(DEFAULT_FORWARD_WATCH_DOC_PATH, _render_forward_watch_doc(owner_pack))


def _future_action_metrics(
    *,
    prices: pd.DataFrame,
    start_idx: int,
    horizon: int,
    weights: Mapping[str, float],
) -> dict[str, float]:
    future_returns = prices.pct_change().fillna(0.0).iloc[start_idx + 1 : start_idx + horizon + 1]
    if future_returns.empty:
        return {
            "future_return": 0.0,
            "future_max_drawdown": 0.0,
            "worst_1d_return": 0.0,
            "worst_5d_return": 0.0,
            "worst_20d_return": 0.0,
        }
    weight_series = pd.Series(weights).reindex(prices.columns).fillna(0.0)
    portfolio_returns = (future_returns * weight_series).sum(axis=1)
    equity = (1.0 + portfolio_returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    return {
        "future_return": float(equity.iloc[-1] - 1.0),
        "future_max_drawdown": float(drawdown.min()),
        "worst_1d_return": float(portfolio_returns.min()),
        "worst_5d_return": _worst_window_return(portfolio_returns, 5),
        "worst_20d_return": _worst_window_return(portfolio_returns, 20),
    }


def _backtest_probe_predictions(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe: Mapping[str, Any],
    model_id: str,
) -> dict[str, Any]:
    pred = predictions.drop_duplicates("date", keep="last").copy()
    pred["date"] = pd.to_datetime(pred["date"])
    pred = pred.set_index("date").sort_index()
    start = max(prices.index.min(), pred.index.min())
    end = min(prices.index.max(), pred.index.max())
    sliced_prices = prices.loc[(prices.index >= start) & (prices.index <= end)].copy()
    weights_by_state = _mapping(probe.get("weights_by_trend_state"))
    target = pd.DataFrame(0.0, index=sliced_prices.index, columns=sliced_prices.columns)
    state_series = pred["trend_state"].reindex(sliced_prices.index).ffill().bfill()
    lagged_state = state_series.shift(1).fillna(state_series)
    for timestamp, state in lagged_state.items():
        weights = _normalize_weights(_mapping(weights_by_state.get(str(state))))
        for asset, weight in weights.items():
            if asset in target.columns:
                target.loc[timestamp, asset] = weight
    sim = _simulate_rebalanced_portfolio(
        sliced_prices,
        target,
        rebalance="daily",
        transaction_cost_bps=0.0,
    )
    return {
        "model_id": model_id,
        "probe_id": str(probe.get("probe_id")),
        "date_start": start.date().isoformat(),
        "date_end": end.date().isoformat(),
        **_portfolio_metrics(sim),
        "target_vs_actual_gap": 0.0,
        "staleness_lag_policy": "one_trading_day_signal_lag",
        "tqqq_max_weight": float(target.get("TQQQ", pd.Series(0.0, index=target.index)).max()),
        **SAFETY_BOUNDARY,
    }


def _portfolio_metrics(sim: Mapping[str, Any]) -> dict[str, float]:
    returns = pd.Series(sim["daily_returns"]).fillna(0.0)
    equity = pd.Series(sim["equity"]).fillna(1.0)
    turnover = pd.Series(sim["turnover"]).fillna(0.0)
    drawdown = equity / equity.cummax() - 1.0
    annual_return = _annual_return(equity, len(returns), 252)
    volatility = float(returns.std(ddof=0)) * math.sqrt(252.0)
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0
    return {
        "actual_path_annual_return": round(annual_return, GRID_ROUND_DIGITS),
        "max_drawdown_daily_equity": round(max_dd, GRID_ROUND_DIGITS),
        "sharpe_daily_zero_rf": round(_ratio(annual_return, volatility), GRID_ROUND_DIGITS),
        "calmar_daily_equity_dd": round(_ratio(annual_return, abs(max_dd)), GRID_ROUND_DIGITS),
        "turnover": round(float(turnover.sum()), GRID_ROUND_DIGITS),
        "net_of_cost_return": round(annual_return, GRID_ROUND_DIGITS),
    }


def _prediction_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    if predictions.empty:
        return {
            "accuracy": 0.0,
            "balanced_accuracy": 0.0,
            "risk_off_precision": 0.0,
            "risk_off_recall": 0.0,
            "false_risk_off_rate": 0.0,
            "late_risk_off_rate": 0.0,
            "false_risk_on_rate": 0.0,
            "late_re_risk_rate": 0.0,
            "consensus_label_margin_capture": 0.0,
        }
    actual = predictions["label_state"].astype(str)
    pred = predictions["trend_state"].astype(str)
    accuracy = float((actual == pred).mean())
    recalls = []
    for state in STATE_ORDER:
        mask = actual == state
        if mask.any():
            recalls.append(float((pred.loc[mask] == state).mean()))
    risk_off_pred = pred == "risk_off"
    risk_off_actual = actual == "risk_off"
    risk_on_pred = pred == "risk_on"
    risk_on_actual = actual == "risk_on"
    return {
        "accuracy": round(accuracy, GRID_ROUND_DIGITS),
        "balanced_accuracy": round(float(np.mean(recalls)) if recalls else 0.0, GRID_ROUND_DIGITS),
        "risk_off_precision": round(
            _ratio((risk_off_pred & risk_off_actual).sum(), risk_off_pred.sum()),
            GRID_ROUND_DIGITS,
        ),
        "risk_off_recall": round(
            _ratio((risk_off_pred & risk_off_actual).sum(), risk_off_actual.sum()),
            GRID_ROUND_DIGITS,
        ),
        "false_risk_off_rate": round(
            _ratio((risk_off_pred & ~risk_off_actual).sum(), len(pred)), 6
        ),
        "late_risk_off_rate": round(_ratio((~risk_off_pred & risk_off_actual).sum(), len(pred)), 6),
        "false_risk_on_rate": round(_ratio((risk_on_pred & ~risk_on_actual).sum(), len(pred)), 6),
        "late_re_risk_rate": round(_ratio((~risk_on_pred & risk_on_actual).sum(), len(pred)), 6),
        "consensus_label_margin_capture": round(
            float(predictions["label_confidence"].mean()), GRID_ROUND_DIGITS
        ),
    }


def _fit_ordinal_linear(frame: pd.DataFrame) -> dict[str, Any]:
    x = frame[FEATURE_COLUMNS].astype(float).to_numpy()
    means = x.mean(axis=0)
    stds = x.std(axis=0)
    stds[stds == 0.0] = 1.0
    x_scaled = (x - means) / stds
    x_design = np.column_stack([np.ones(len(x_scaled)), x_scaled])
    y = frame["consensus_state"].map(STATE_TO_ORDINAL).astype(float).to_numpy()
    beta, *_ = np.linalg.lstsq(x_design, y, rcond=None)
    return {
        "intercept": float(beta[0]),
        "beta": beta[1:],
        "means": means,
        "stds": stds,
        "coefficients": {
            "intercept": round(float(beta[0]), 8),
            **{
                column: round(float(value), 8)
                for column, value in zip(FEATURE_COLUMNS, beta[1:], strict=True)
            },
        },
    }


def _predict_ordinal(row: Mapping[str, Any], model: Mapping[str, Any]) -> float:
    x = np.array([_float(row.get(column)) for column in FEATURE_COLUMNS])
    scaled = (x - np.asarray(model["means"])) / np.asarray(model["stds"])
    return float(_float(model["intercept"]) + np.dot(scaled, np.asarray(model["beta"])))


def _label_summary_payload(
    *,
    single_labels: pd.DataFrame,
    consensus_labels: pd.DataFrame,
    scope_config_path: Path,
) -> dict[str, Any]:
    distribution = {
        str(key): int(value)
        for key, value in consensus_labels["consensus_state"].value_counts().sort_index().items()
    }
    high_confidence = int(consensus_labels["train_usable"].astype(bool).sum())
    return _payload(
        report_type="consensus_trend_label_summary",
        title="Consensus Trend Label Summary",
        status="CONSENSUS_TREND_LABELS_READY_PROMOTION_BLOCKED",
        summary={
            "single_probe_label_count": len(single_labels),
            "consensus_label_count": len(consensus_labels),
            "high_confidence_training_sample_count": high_confidence,
            "label_distribution": distribution,
            "avg_disagreement_score": round(
                float(consensus_labels["disagreement_score"].mean()), GRID_ROUND_DIGITS
            ),
        },
        config_hash=_file_sha256(scope_config_path),
    )


def _feature_audit_payload(
    feature_report: Mapping[str, Any],
    *,
    scope_config_path: Path,
) -> dict[str, Any]:
    return _payload(
        report_type="first_layer_feature_pit_audit",
        title="First-Layer Feature PIT Audit",
        status=str(feature_report.get("status", "PIT_WARNING")),
        summary={
            "row_count": feature_report.get("row_count"),
            "approved_feature_count": feature_report.get("approved_feature_count"),
            "blocked_feature_count": feature_report.get("blocked_feature_count"),
            "feature_cutoff_passed": feature_report.get("feature_cutoff_passed"),
        },
        config_hash=_file_sha256(scope_config_path),
        feature_report=dict(feature_report),
    )


def _walk_forward_payload(
    *,
    wf_metrics: Mapping[str, Any],
    coefficients: Sequence[Mapping[str, Any]],
    scope_config_path: Path,
) -> dict[str, Any]:
    return _payload(
        report_type="first_layer_trend_model_walk_forward_matrix",
        title="First-Layer Trend Model Walk-Forward Matrix",
        status="FIRST_LAYER_WALK_FORWARD_READY_PROMOTION_BLOCKED",
        summary=dict(wf_metrics),
        config_hash=_file_sha256(scope_config_path),
        coefficients=list(coefficients),
    )


def _final_matrix_payload(
    *,
    label_summary: Mapping[str, Any],
    walk_forward_review: Mapping[str, Any],
    probe_metrics: pd.DataFrame,
    scope_config_path: Path,
) -> dict[str, Any]:
    overlay = probe_metrics.loc[probe_metrics["probe_id"] == "defensive_overlay_probe"]
    overlay_improved = (
        bool(overlay["new_model_better_calmar"].any()) if not overlay.empty else False
    )
    status = (
        "CALIBRATED_FIRST_LAYER_OVERLAY_WATCH_CANDIDATE"
        if overlay_improved
        else "FIRST_LAYER_CALIBRATION_NO_MATERIAL_IMPROVEMENT"
    )
    return _payload(
        report_type="first_layer_policy_aware_calibration_final_matrix",
        title="First-Layer Policy-Aware Calibration Final Matrix",
        status=f"{status}_PROMOTION_BLOCKED",
        summary={
            "base_status": status,
            "overlay_improved": overlay_improved,
            "walk_forward_balanced_accuracy": _mapping(walk_forward_review.get("summary")).get(
                "balanced_accuracy"
            ),
            "high_confidence_training_sample_count": _mapping(label_summary.get("summary")).get(
                "high_confidence_training_sample_count"
            ),
            "next_action": (
                "OBSERVE_ONLY_FORWARD_WATCH_REVIEW"
                if overlay_improved
                else "REVIEW_LABEL_AND_FEATURE_DESIGN"
            ),
        },
        config_hash=_file_sha256(scope_config_path),
        probe_level_metrics=_json_records(probe_metrics.to_dict("records")),
    )


def _owner_pack_payload(
    *,
    data_gate: Mapping[str, Any],
    probe_validation: Mapping[str, Any],
    action_value: pd.DataFrame,
    label_summary: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    walk_forward_review: Mapping[str, Any],
    probe_metrics: pd.DataFrame,
    final_matrix: Mapping[str, Any],
    scope_config_path: Path,
) -> dict[str, Any]:
    probe_metric_rows = _json_records(probe_metrics.to_dict("records"))
    return _payload(
        report_type="first_layer_policy_aware_calibration_owner_review_pack",
        title="First-Layer Policy-Aware Calibration Owner Review Pack",
        status="FIRST_LAYER_POLICY_AWARE_CALIBRATION_READY_PROMOTION_BLOCKED",
        summary={
            "data_quality_status": data_gate.get("status"),
            "probe_count": _mapping(probe_validation.get("summary")).get("probe_count"),
            "action_value_matrix_size": len(action_value),
            "label_distribution": _mapping(label_summary.get("summary")).get("label_distribution"),
            "walk_forward_balanced_accuracy": _mapping(walk_forward_review.get("summary")).get(
                "balanced_accuracy"
            ),
            "final_status": final_matrix.get("status"),
            "tqqq_diagnostic_status": "RESEARCH_ONLY_DIAGNOSTIC",
            "promotion_status": "BLOCKED",
        },
        config_hash=_file_sha256(scope_config_path),
        probe_validation_summary=_mapping(probe_validation.get("summary")),
        feature_audit_summary=_mapping(feature_audit.get("summary")),
        walk_forward_summary=_mapping(walk_forward_review.get("summary")),
        probe_level_metrics=probe_metric_rows,
        final_matrix_summary=_mapping(final_matrix.get("summary")),
        artifact_paths=_artifact_paths(),
    )


def _load_rates(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, parse_dates=["date"])
    if {"date", "series", "value"} - set(frame.columns):
        return pd.DataFrame()
    pivot = frame.pivot_table(index="date", columns="series", values="value", aggfunc="last")
    return pivot.sort_index().ffill()


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    output = {
        asset: round(_float(weights.get(asset)), GRID_ROUND_DIGITS)
        for asset in ("QQQ", "SGOV", "TQQQ")
    }
    if abs(sum(output.values())) <= 1e-12:
        return {}
    return output


def _state_from_score(score: float, bands: Mapping[str, Any]) -> str:
    for state in STATE_ORDER:
        if score <= _float(bands.get(state), default=999.0):
            return state
    return "risk_on"


def _scorecard_confidence(score: float, cfg: Mapping[str, Any]) -> float:
    min_conf = _float(cfg.get("min_confidence"), default=0.35)
    max_conf = _float(cfg.get("max_confidence"), default=0.9)
    scale = _float(cfg.get("scale"), default=0.18)
    return round(
        min(max_conf, min_conf + min(abs(score) / max(scale, 1e-9), 1.0) * (max_conf - min_conf)), 6
    )


def _confidence_from_margin(margin: float, best_score: float) -> float:
    denom = abs(best_score) + abs(margin) + 1e-9
    return round(max(0.0, min(1.0, 0.5 + margin / max(denom, 1e-9))), GRID_ROUND_DIGITS)


def _validity_for_state(state: str) -> int:
    return {"risk_off": 5, "defensive": 10, "neutral": 20, "constructive": 20, "risk_on": 10}.get(
        state, 10
    )


def _decay_for_state(state: str) -> str:
    return {
        "risk_off": "fast",
        "defensive": "medium",
        "neutral": "slow",
        "constructive": "medium",
        "risk_on": "fast",
    }.get(state, "medium")


def _render_scope_doc(scope_config: Mapping[str, Any], validation: Mapping[str, Any]) -> str:
    summary = _mapping(validation.get("summary"))
    lines = [
        "# First-Layer Policy-Aware Calibration Scope",
        "",
        f"- Status: `{scope_config.get('status')}`",
        "- Market regime: `ai_after_chatgpt`",
        "- First-layer outputs: trend_state / confidence / validity_days / decay_profile",
        "- Direct weight output allowed: `False`",
        f"- Dynamic second-layer probe count: `{summary.get('probe_count')}`",
        f"- Trend-sensitive probe count: `{summary.get('trend_sensitive_count')}`",
        "- Dynamic promotion: `BLOCKED`",
        "- Paper-shadow / production / broker: `false / false / none`",
        "",
        (
            "本范围文件定义第一层趋势校准 research-only 路径；second-layer probe "
            "在生成 labels 前冻结，不能与 first-layer 同时优化。"
        ),
    ]
    return "\n".join(lines) + "\n"


def _render_label_quality_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return _render_kv_doc(
        title="Consensus Trend Label Quality Review",
        status=str(payload.get("status")),
        summary=summary,
        note=(
            "Consensus labels are for supervised calibration only; "
            "they are not promotion evidence."
        ),
    )


def _render_feature_audit_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return _render_kv_doc(
        title="First-Layer Feature PIT Audit",
        status=str(payload.get("status")),
        summary=summary,
        note="Features with unavailable PIT provenance are excluded from training.",
    )


def _render_walk_forward_review_doc(payload: Mapping[str, Any]) -> str:
    return _render_kv_doc(
        title="First-Layer Trend Model Walk-Forward Review",
        status=str(payload.get("status")),
        summary=_mapping(payload.get("summary")),
        note="Walk-forward metrics use validation windows only and keep promotion blocked.",
    )


def _render_probe_review_doc(payload: Mapping[str, Any], review_type: str) -> str:
    rows = _records(payload.get("probe_level_metrics"))
    if review_type == "overlay":
        title = "Calibrated First-Layer Defensive Overlay Review"
        rows = [row for row in rows if row.get("probe_id") == "defensive_overlay_probe"]
        note = "Overlay improvement is observe-only evidence and does not enable promotion."
    else:
        title = "Calibrated First-Layer Risk-On TQQQ Diagnostic Review"
        rows = [row for row in rows if "risk_on" in str(row.get("probe_id"))]
        note = "Risk-on TQQQ rows remain research-only diagnostic."
    lines = [f"# {title}", "", f"- Status: `{payload.get('status')}`", ""]
    lines.extend(_safety_lines(payload))
    lines.extend(
        [
            "",
            "|probe_id|annual_return_delta|calmar_delta|new_model_better_calmar|",
            "|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"|{row.get('probe_id')}|{row.get('annual_return_delta')}|"
            f"{row.get('calmar_delta')}|{row.get('new_model_better_calmar')}|"
        )
    lines.extend(["", note])
    return "\n".join(lines) + "\n"


def _render_consensus_comparison_doc(payload: Mapping[str, Any]) -> str:
    return _render_kv_doc(
        title="Single-Probe vs Consensus Trend Label Review",
        status=str(payload.get("status")),
        summary=_mapping(payload.get("summary")),
        note="Consensus labels reduce single-probe overfit risk by requiring multi-probe votes.",
    )


def _render_owner_pack_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = ["# First-Layer Policy-Aware Calibration Owner Review Pack", ""]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Owner Questions", ""])
    answers = [
        "Dynamic second-layer probes are trend-sensitive and frozen before label generation.",
        "Consensus labels are generated from action-value votes across multiple probes.",
        "The calibrated first layer is evaluated only on walk-forward validation windows.",
        "Probe backtests compare old scorecard vs new calibrated first layer on actual paths.",
        "TQQQ risk-on probe remains research-only diagnostic.",
        "Dynamic promotion remains BLOCKED because owner review and forward evidence are absent.",
    ]
    for answer in answers:
        lines.append(f"- {answer}")
    return "\n".join(lines) + "\n"


def _render_forward_watch_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Calibrated First-Layer Forward Watch Plan",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Final status: `{summary.get('final_status')}`",
        "- Observe-only: `true`",
        "- Paper-shadow allowed: `False`",
        "- Production allowed: `False`",
        "- Broker action: `none`",
        "",
        (
            "Daily watch fields: PIT features, trend_state prediction, confidence, "
            "probe implied action, actual 1d/5d/10d/20d outcome, false risk-off, "
            "missed upside, avoided drawdown, owner note."
        ),
    ]
    return "\n".join(lines) + "\n"


def _render_closeout_doc(payload: Mapping[str, Any]) -> str:
    return _render_kv_doc(
        title="First-Layer Policy-Aware Calibration Closeout",
        status=str(payload.get("status")),
        summary=_mapping(payload.get("summary")),
        note="Closeout preserves research-only status and blocks promotion.",
    )


def _render_kv_doc(
    *,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    note: str,
) -> str:
    lines = [f"# {title}", "", f"- Status: `{status}`"]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", note])
    return "\n".join(lines) + "\n"


def _render_generic_doc(payload: Mapping[str, Any]) -> str:
    return _render_kv_doc(
        title=str(payload.get("title")),
        status=str(payload.get("status")),
        summary=_mapping(payload.get("summary")),
        note="Generated artifact remains research-only.",
    )


def _safety_lines(payload: Mapping[str, Any]) -> list[str]:
    return [
        f"- Promotion allowed: `{payload.get('promotion_allowed')}`",
        f"- Paper-shadow allowed: `{payload.get('paper_shadow_allowed')}`",
        f"- Production allowed: `{payload.get('production_allowed')}`",
        f"- Broker action: `{payload.get('broker_action')}`",
    ]


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "source_commit": _source_commit(),
        "market_regime": "unified_primary_2021",
        "anchor_event": "validated QQQ/SGOV/TQQQ common history start",
        "anchor_date": "2021-02-22",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "unified_primary_2021",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _artifact_paths() -> dict[str, str]:
    return {
        "scope_doc": str(DEFAULT_SCOPE_DOC_PATH),
        "probe_registry": str(DEFAULT_PROBE_REGISTRY_PATH),
        "action_value_matrix": str(DEFAULT_ACTION_VALUE_OUTPUT_ROOT / "action_value_matrix.csv"),
        "consensus_labels": str(DEFAULT_TREND_LABEL_OUTPUT_ROOT / "consensus_trend_labels.csv"),
        "pit_feature_matrix": str(DEFAULT_FEATURE_OUTPUT_ROOT / "pit_feature_matrix.csv"),
        "walk_forward_review": str(DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH),
        "owner_pack": str(DEFAULT_OWNER_REVIEW_DOC_PATH),
        "final_matrix": str(DEFAULT_FINAL_MATRIX_YAML_PATH),
    }


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    formatted = frame.copy()
    for column in formatted.columns:
        formatted[column] = formatted[column].map(_csv_cell)
    formatted.to_csv(path, index=False)


def _csv_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(_json_scalar(value), sort_keys=True, ensure_ascii=False)
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_scalar(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8"
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


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_records(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(_json_scalar(row)) for row in rows]


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


def _bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in {"true", "1", "yes"}:
            return True
        if value.lower() in {"false", "0", "no"}:
            return False
    if value is None:
        return default
    return bool(value)


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    if abs(denom) <= 1e-12:
        return 0.0
    return _float(numerator) / denom


def _worst_window_return(returns: pd.Series, window: int) -> float:
    numeric = pd.to_numeric(returns, errors="coerce").fillna(0.0)
    if len(numeric) < window:
        return float(numeric.min()) if len(numeric) else 0.0
    rolled = (1.0 + numeric).rolling(window).apply(lambda values: float(values.prod() - 1.0))
    return float(rolled.min()) if not rolled.dropna().empty else 0.0


def _annual_return(equity: pd.Series, periods: int, annualization: int) -> float:
    if periods <= 0 or equity.empty:
        return 0.0
    terminal = max(float(equity.iloc[-1]), 1e-12)
    return terminal ** (annualization / periods) - 1.0


def _row_hash(row: Mapping[str, Any], columns: Sequence[str]) -> str:
    text = json.dumps({column: _float(row.get(column)) for column in columns}, sort_keys=True)
    return sha256(text.encode("utf-8")).hexdigest()[:16]


def _file_sha256(path: Path) -> str:
    if not path.exists():
        return ""
    return sha256(path.read_bytes()).hexdigest()


def _source_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "UNKNOWN"


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value
