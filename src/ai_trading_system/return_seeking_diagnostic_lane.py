from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

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
    DEFAULT_COMPOSER_PREDICTIONS_PATH,
    DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    PRIMARY_WINDOW_ID,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_RETURN_SEEKING_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "return_seeking_diagnostic_lane_policy.yaml"
)
DEFAULT_SCOPE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_diagnostic_lane_scope.md"
)
DEFAULT_SIGNAL_AUDIT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_signal_audit.md"
)
DEFAULT_SIGNAL_AUDIT_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "return_seeking_signal_audit.yaml"
)
DEFAULT_ACTUAL_PATH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_actual_path_review.md"
)
DEFAULT_ACTUAL_PATH_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "return_seeking_actual_path_matrix.yaml"
)
DEFAULT_BETA_ATTRIBUTION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_beta_tqqq_attribution.md"
)
DEFAULT_BETA_ATTRIBUTION_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "return_seeking_beta_tqqq_attribution.yaml"
)
DEFAULT_CONTRAST_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_2022_vs_2023_contrast.md"
)
DEFAULT_CONTRAST_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "return_seeking_2022_vs_2023_contrast.yaml"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "return_seeking_diagnostic_lane_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "return_seeking_diagnostic_lane_final_matrix.yaml"
)

RETURN_SEEKING_SIGNALS = (
    "stay_constructive_pred",
    "add_risk_pred",
    "high_confidence_risk_on_pred",
)
RETURN_SEEKING_TREND_STATES = ("constructive", "risk_on")
PREDICTION_MODEL_ID = "first_layer_composer_v2"
REFERENCE_MODEL_ID = "no_return_seeking_reference"


def run_return_seeking_diagnostic_lane_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    lane_policy_path: Path = DEFAULT_RETURN_SEEKING_POLICY_PATH,
    probe_registry_path: Path = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    composer_predictions_path: Path = DEFAULT_COMPOSER_PREDICTIONS_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
) -> dict[str, Any]:
    registry = load_research_window_registry(registry_path)
    primary_window = _mapping(registry["windows"][PRIMARY_WINDOW_ID])
    lane_policy = _load_yaml_mapping(lane_policy_path)
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
            f"Cached data quality gate failed for return-seeking lane: {data_gate['status']}"
        )

    prices = slice_window_prices(_load_price_matrix(prices_path, ASSETS), primary_window)
    _load_rates(rates_path)
    predictions = load_composer_predictions(composer_predictions_path, primary_window)
    probes = select_return_seeking_probes(probe_registry, lane_policy)
    reference_predictions = build_no_return_seeking_reference(predictions)

    signal_audit = build_signal_audit(
        predictions=predictions,
        probes=probes,
        lane_policy=lane_policy,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    actual_path = build_actual_path_review(
        prices=prices,
        predictions=predictions,
        reference_predictions=reference_predictions,
        probes=probes,
        lane_policy=lane_policy,
        primary_window=primary_window,
    )
    beta_attribution = build_beta_tqqq_attribution(
        prices=prices,
        predictions=predictions,
        probes=probes,
        actual_path=actual_path,
        lane_policy=lane_policy,
        primary_window=primary_window,
    )
    contrast = build_2022_vs_2023_contrast(
        prices=prices,
        predictions=predictions,
        reference_predictions=reference_predictions,
        probes=probes,
        actual_path=actual_path,
        lane_policy=lane_policy,
        primary_window=primary_window,
    )
    final_matrix = build_final_matrix(
        signal_audit=signal_audit,
        actual_path=actual_path,
        beta_attribution=beta_attribution,
        contrast=contrast,
        lane_policy=lane_policy,
        primary_window=primary_window,
        data_gate=data_gate,
    )
    write_return_seeking_outputs(
        lane_policy=lane_policy,
        signal_audit=signal_audit,
        actual_path=actual_path,
        beta_attribution=beta_attribution,
        contrast=contrast,
        final_matrix=final_matrix,
    )
    return final_matrix


def load_composer_predictions(path: Path, primary_window: Mapping[str, Any]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Composer predictions are required for Phase 3 and were not found: {path}"
        )
    predictions = pd.read_csv(path)
    required = {"date", "trend_state", *RETURN_SEEKING_SIGNALS}
    missing = sorted(required.difference(predictions.columns))
    if missing:
        raise ValueError(f"Composer predictions missing required columns: {missing}")
    predictions["date"] = pd.to_datetime(predictions["date"]).dt.date.astype(str)
    start = str(primary_window.get("actual_portfolio_start") or primary_window.get("actual_start"))
    return predictions.loc[predictions["date"] >= start].copy()


def select_return_seeking_probes(
    probe_registry: Mapping[str, Any],
    lane_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    allowed_roles = set(_strings(lane_policy.get("allowed_probe_roles")))
    probes = []
    for probe in _records(probe_registry.get("probes")):
        if not bool(probe.get("return_seeking")):
            continue
        if str(probe.get("role")) not in allowed_roles:
            continue
        probes.append(probe)
    if not probes:
        raise ValueError("No return-seeking probes matched return-seeking diagnostic policy.")
    return probes


def build_no_return_seeking_reference(predictions: pd.DataFrame) -> pd.DataFrame:
    reference = predictions.copy()
    reference["trend_state"] = reference["trend_state"].replace(
        {"constructive": "neutral", "risk_on": "neutral"}
    )
    for column in RETURN_SEEKING_SIGNALS:
        reference[column] = False
    return reference


def build_signal_audit(
    *,
    predictions: pd.DataFrame,
    probes: list[dict[str, Any]],
    lane_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    signal_rows = []
    for signal in _strings(lane_policy.get("allowed_signals")):
        column = f"{signal}_pred"
        signal_rows.append(
            {
                "signal_name": signal,
                "source_column": column,
                "positive_count": _bool_count(predictions, column),
                "allowed_usage": ["return_seeking_diagnostic"],
                "blocked_usage": _strings(lane_policy.get("blocked_downstream_usage")),
                "defensive_overlay_usage_allowed": False,
                "promotion_allowed": False,
            }
        )
    state_counts = predictions["trend_state"].astype(str).value_counts().to_dict()
    return _payload(
        report_type="return_seeking_signal_audit",
        title="Return-Seeking Signal Audit",
        status="RETURN_SEEKING_SIGNAL_AUDIT_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate["status"],
            "prediction_count": len(predictions),
            "return_seeking_probe_count": len(probes),
            "constructive_state_count": int(state_counts.get("constructive", 0)),
            "risk_on_state_count": int(state_counts.get("risk_on", 0)),
            "stay_constructive_count": _bool_count(predictions, "stay_constructive_pred"),
            "add_risk_count": _bool_count(predictions, "add_risk_pred"),
            "high_confidence_risk_on_count": _bool_count(
                predictions, "high_confidence_risk_on_pred"
            ),
            "defensive_overlay_usage_allowed": False,
            "full_allocation_usage_allowed": False,
            "gated_integration_allowed": False,
            "promotion_status": "blocked",
        },
        signal_rows=signal_rows,
        blocked_downstream_usage=_strings(lane_policy.get("blocked_downstream_usage")),
    )


def build_actual_path_review(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    reference_predictions: pd.DataFrame,
    probes: list[dict[str, Any]],
    lane_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    thresholds = _mapping(lane_policy.get("diagnostic_thresholds"))
    min_delta = _float(thresholds.get("min_actual_path_return_delta_vs_no_return_seeking"))
    max_dd_regression = _float(thresholds.get("max_drawdown_regression_allowed"))
    rows = []
    for probe in probes:
        metrics = _backtest_probe_predictions(
            prices=prices,
            predictions=predictions[["date", "trend_state"]],
            probe=probe,
            model_id=PREDICTION_MODEL_ID,
        )
        reference = _backtest_probe_predictions(
            prices=prices,
            predictions=reference_predictions[["date", "trend_state"]],
            probe=probe,
            model_id=REFERENCE_MODEL_ID,
        )
        return_delta = round(
            metrics["actual_path_annual_return"] - reference["actual_path_annual_return"],
            GRID_ROUND_DIGITS,
        )
        drawdown_delta = round(
            metrics["max_drawdown_daily_equity"] - reference["max_drawdown_daily_equity"],
            GRID_ROUND_DIGITS,
        )
        diagnostic_value = return_delta > min_delta and drawdown_delta >= max_dd_regression
        rows.append(
            {
                **window_metadata(primary_window),
                "probe_id": probe["probe_id"],
                "role": probe["role"],
                "role_tags": _strings(probe.get("role_tags")),
                "tqqq_usage": probe.get("tqqq_usage"),
                "model_id": PREDICTION_MODEL_ID,
                "reference_model_id": REFERENCE_MODEL_ID,
                "annual_return": metrics["actual_path_annual_return"],
                "max_drawdown_daily_equity": metrics["max_drawdown_daily_equity"],
                "sharpe": metrics["sharpe_daily_zero_rf"],
                "calmar": metrics["calmar_daily_equity_dd"],
                "turnover": metrics["turnover"],
                "tqqq_max_weight": metrics["tqqq_max_weight"],
                "reference_annual_return": reference["actual_path_annual_return"],
                "reference_max_drawdown_daily_equity": reference["max_drawdown_daily_equity"],
                "annual_return_delta_vs_no_return_seeking": return_delta,
                "drawdown_delta_vs_no_return_seeking": drawdown_delta,
                "return_seeking_diagnostic_value": bool(diagnostic_value),
                "defensive_overlay_usage_allowed": False,
                "promotion_allowed": False,
                **SAFETY_BOUNDARY,
            }
        )
    positive_rows = [row for row in rows if row["return_seeking_diagnostic_value"]]
    positive_return_rows = [
        row for row in rows if row["annual_return_delta_vs_no_return_seeking"] > min_delta
    ]
    drawdown_regression_rows = [
        row for row in rows if row["drawdown_delta_vs_no_return_seeking"] < max_dd_regression
    ]
    status = (
        "RETURN_SEEKING_ACTUAL_PATH_DIAGNOSTIC_VALUE_PROMOTION_BLOCKED"
        if positive_rows
        else "RETURN_SEEKING_ACTUAL_PATH_UPSIDE_WITH_DRAWDOWN_REGRESSION_PROMOTION_BLOCKED"
        if positive_return_rows
        else "RETURN_SEEKING_ACTUAL_PATH_NO_DIAGNOSTIC_EDGE_PROMOTION_BLOCKED"
    )
    return _payload(
        report_type="return_seeking_actual_path_matrix",
        title="Return-Seeking Actual-Path Review",
        status=status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "model_id": PREDICTION_MODEL_ID,
            "reference_model_id": REFERENCE_MODEL_ID,
            "probe_count": len(rows),
            "diagnostic_value_probe_count": len(positive_rows),
            "positive_return_delta_probe_count": len(positive_return_rows),
            "drawdown_regression_probe_count": len(drawdown_regression_rows),
            "defensive_overlay_usage_allowed": False,
            "full_allocation_usage_allowed": False,
            "gated_integration_allowed": False,
            "promotion_status": "blocked",
        },
        probe_rows=rows,
    )


def build_beta_tqqq_attribution(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probes: list[dict[str, Any]],
    actual_path: Mapping[str, Any],
    lane_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    thresholds = _mapping(lane_policy.get("diagnostic_thresholds"))
    qqq_delta_threshold = _float(thresholds.get("qqq_equivalent_exposure_delta_beta_dependency"))
    tqqq_share_threshold = _float(thresholds.get("tqqq_beta_share_dependency"))
    actual_rows = {
        str(row.get("probe_id")): row for row in _records(actual_path.get("probe_rows"))
    }
    no_tqqq_rows: list[dict[str, Any]] = []
    rows = []
    for probe in probes:
        exposure = _exposure_profile(predictions, probe)
        contribution = _component_return_contribution(prices, predictions, probe)
        row = {
            **window_metadata(primary_window),
            "probe_id": probe["probe_id"],
            "role": probe["role"],
            "tqqq_usage": probe.get("tqqq_usage"),
            "avg_qqq_equivalent_exposure": exposure["avg_qqq_equivalent_exposure"],
            "avg_tqqq_weight": exposure["avg_tqqq_weight"],
            "max_tqqq_weight": exposure["max_tqqq_weight"],
            "tqqq_beta_share": exposure["tqqq_beta_share"],
            "annual_return_delta_vs_no_return_seeking": actual_rows.get(
                str(probe["probe_id"]), {}
            ).get("annual_return_delta_vs_no_return_seeking", 0.0),
            "qqq_contribution_proxy": contribution["QQQ"],
            "sgov_contribution_proxy": contribution["SGOV"],
            "tqqq_contribution_proxy": contribution["TQQQ"],
            "contribution_method": "decision_date_weighted_daily_return_proxy",
            "promotion_allowed": False,
            **SAFETY_BOUNDARY,
        }
        rows.append(row)
        if probe.get("tqqq_usage") == "none":
            no_tqqq_rows.append(row)
    no_tqqq_reference = max(
        [row["avg_qqq_equivalent_exposure"] for row in no_tqqq_rows],
        default=0.0,
    )
    beta_dependent_count = 0
    tqqq_dependent_count = 0
    for row in rows:
        qqq_delta = round(
            row["avg_qqq_equivalent_exposure"] - no_tqqq_reference,
            GRID_ROUND_DIGITS,
        )
        row["qqq_equivalent_exposure_delta_vs_no_tqqq_reference"] = qqq_delta
        row["qqq_beta_dependency_suspected"] = qqq_delta > qqq_delta_threshold
        row["tqqq_beta_dependency_suspected"] = row["tqqq_beta_share"] > tqqq_share_threshold
        beta_dependent_count += int(row["qqq_beta_dependency_suspected"])
        tqqq_dependent_count += int(row["tqqq_beta_dependency_suspected"])
    status = (
        "RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_DEPENDENT_PROMOTION_BLOCKED"
        if beta_dependent_count or tqqq_dependent_count
        else "RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_READY_PROMOTION_BLOCKED"
    )
    return _payload(
        report_type="return_seeking_beta_tqqq_attribution",
        title="Return-Seeking Beta and TQQQ Attribution",
        status=status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "probe_count": len(rows),
            "qqq_beta_dependency_suspected_count": beta_dependent_count,
            "tqqq_beta_dependency_suspected_count": tqqq_dependent_count,
            "no_tqqq_reference_avg_qqq_equivalent_exposure": no_tqqq_reference,
            "promotion_status": "blocked",
        },
        attribution_rows=rows,
    )


def build_2022_vs_2023_contrast(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    reference_predictions: pd.DataFrame,
    probes: list[dict[str, Any]],
    actual_path: Mapping[str, Any],
    lane_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
) -> dict[str, Any]:
    thresholds = _mapping(lane_policy.get("diagnostic_thresholds"))
    min_2023_share = _float(thresholds.get("min_2023_plus_positive_delta_share_for_dependency"))
    slices = [
        ("2022_stress_and_recovery", "2022-02-18", "2022-12-30"),
        ("post_2023_ai_trend", "2023-01-03", "latest"),
    ]
    rows = []
    positive_by_slice: dict[str, int] = {}
    for slice_id, start, end in slices:
        slice_prices = _slice_prices(prices, start, end)
        slice_predictions = _slice_predictions(predictions, start, end)
        slice_reference = _slice_predictions(reference_predictions, start, end)
        positive = 0
        for probe in probes:
            if len(slice_prices) < 30 or len(slice_predictions) < 30:
                return_delta = 0.0
                drawdown_delta = 0.0
            else:
                metrics = _backtest_probe_predictions(
                    prices=slice_prices,
                    predictions=slice_predictions[["date", "trend_state"]],
                    probe=probe,
                    model_id=PREDICTION_MODEL_ID,
                )
                reference = _backtest_probe_predictions(
                    prices=slice_prices,
                    predictions=slice_reference[["date", "trend_state"]],
                    probe=probe,
                    model_id=REFERENCE_MODEL_ID,
                )
                return_delta = round(
                    metrics["actual_path_annual_return"]
                    - reference["actual_path_annual_return"],
                    GRID_ROUND_DIGITS,
                )
                drawdown_delta = round(
                    metrics["max_drawdown_daily_equity"]
                    - reference["max_drawdown_daily_equity"],
                    GRID_ROUND_DIGITS,
                )
            positive += int(return_delta > 0)
            rows.append(
                {
                    **window_metadata(primary_window),
                    "slice_id": slice_id,
                    "start": start,
                    "end": str(slice_prices.index.max().date()) if not slice_prices.empty else end,
                    "probe_id": probe["probe_id"],
                    "prediction_count": len(slice_predictions),
                    "constructive_state_count": _state_count(
                        slice_predictions, "constructive"
                    ),
                    "risk_on_state_count": _state_count(slice_predictions, "risk_on"),
                    "add_risk_count": _bool_count(slice_predictions, "add_risk_pred"),
                    "high_confidence_risk_on_count": _bool_count(
                        slice_predictions, "high_confidence_risk_on_pred"
                    ),
                    "annual_return_delta_vs_no_return_seeking": return_delta,
                    "drawdown_delta_vs_no_return_seeking": drawdown_delta,
                    "promotion_allowed": False,
                    **SAFETY_BOUNDARY,
                }
            )
        positive_by_slice[slice_id] = positive
    total_positive = sum(positive_by_slice.values())
    post_2023_share = (
        positive_by_slice.get("post_2023_ai_trend", 0) / total_positive
        if total_positive
        else 0.0
    )
    depends_on_2023_plus = post_2023_share >= min_2023_share
    return _payload(
        report_type="return_seeking_2022_vs_2023_contrast",
        title="Return-Seeking 2022 vs 2023+ Contrast",
        status="RETURN_SEEKING_2022_VS_2023_CONTRAST_READY_PROMOTION_BLOCKED",
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "slice_count": len(slices),
            "probe_count": len(probes),
            "positive_delta_count_2022": positive_by_slice.get("2022_stress_and_recovery", 0),
            "positive_delta_count_2023_plus": positive_by_slice.get(
                "post_2023_ai_trend", 0
            ),
            "post_2023_positive_delta_share": round(post_2023_share, GRID_ROUND_DIGITS),
            "depends_on_2023_plus": bool(depends_on_2023_plus),
            "actual_path_status": actual_path.get("status"),
            "promotion_status": "blocked",
        },
        contrast_rows=rows,
    )


def build_final_matrix(
    *,
    signal_audit: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    beta_attribution: Mapping[str, Any],
    contrast: Mapping[str, Any],
    lane_policy: Mapping[str, Any],
    primary_window: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    actual_summary = _mapping(actual_path.get("summary"))
    beta_summary = _mapping(beta_attribution.get("summary"))
    contrast_summary = _mapping(contrast.get("summary"))
    diagnostic_probe_count = _int(actual_summary.get("diagnostic_value_probe_count"))
    positive_return_delta_count = _int(actual_summary.get("positive_return_delta_probe_count"))
    drawdown_regression_count = _int(actual_summary.get("drawdown_regression_probe_count"))
    beta_dependent = bool(
        _int(beta_summary.get("qqq_beta_dependency_suspected_count"))
        or _int(beta_summary.get("tqqq_beta_dependency_suspected_count"))
    )
    depends_on_2023_plus = bool(contrast_summary.get("depends_on_2023_plus"))
    if diagnostic_probe_count and (beta_dependent or depends_on_2023_plus):
        final_status = "RETURN_SEEKING_DIAGNOSTIC_RETAINED_DEPENDENT_PROMOTION_BLOCKED"
    elif diagnostic_probe_count:
        final_status = "RETURN_SEEKING_DIAGNOSTIC_RETAINED_PROMOTION_BLOCKED"
    elif positive_return_delta_count and drawdown_regression_count:
        final_status = (
            "RETURN_SEEKING_DIAGNOSTIC_UPSIDE_DEPENDENT_DRAWDOWN_REGRESSED_PROMOTION_BLOCKED"
        )
    else:
        final_status = "RETURN_SEEKING_DIAGNOSTIC_ARCHIVED_PROMOTION_BLOCKED"
    return _payload(
        report_type="return_seeking_diagnostic_lane_final_matrix",
        title="Return-Seeking Diagnostic Lane Final Matrix",
        status=final_status,
        primary_window=primary_window,
        summary={
            **window_metadata(primary_window),
            "data_quality_status": data_gate["status"],
            "signal_audit_status": signal_audit.get("status"),
            "actual_path_status": actual_path.get("status"),
            "beta_attribution_status": beta_attribution.get("status"),
            "contrast_status": contrast.get("status"),
            "diagnostic_value_probe_count": diagnostic_probe_count,
            "positive_return_delta_probe_count": positive_return_delta_count,
            "drawdown_regression_probe_count": drawdown_regression_count,
            "beta_or_tqqq_dependency_suspected": beta_dependent,
            "depends_on_2023_plus": depends_on_2023_plus,
            "defensive_overlay_usage_allowed": False,
            "full_allocation_usage_allowed": False,
            "gated_integration_allowed": False,
            "owner_review_allowed": False,
            "promotion_status": "blocked",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        phase_decision={
            "next_action": "KEEP_AS_FORWARD_DIAGNOSTIC_ONLY_UNLESS_OWNER_REOPENS_POLICY",
            "gated_integration_allowed_now": False,
            "reason": (
                "Phase 2 did not establish material defensive improvement, and Phase 3 "
                "signals remain diagnostic-only with beta/TQQQ or 2023+ dependence."
            ),
        },
        artifact_paths=_artifact_paths(),
        policy_summary={
            "allowed_signals": _strings(lane_policy.get("allowed_signals")),
            "blocked_downstream_usage": _strings(lane_policy.get("blocked_downstream_usage")),
        },
    )


def write_return_seeking_outputs(
    *,
    lane_policy: Mapping[str, Any],
    signal_audit: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    beta_attribution: Mapping[str, Any],
    contrast: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
) -> None:
    _write_markdown(DEFAULT_SCOPE_DOC_PATH, _render_scope_doc(lane_policy, final_matrix))
    _write_yaml(DEFAULT_SIGNAL_AUDIT_YAML_PATH, signal_audit)
    _write_markdown(DEFAULT_SIGNAL_AUDIT_DOC_PATH, _render_payload_doc(signal_audit))
    _write_yaml(DEFAULT_ACTUAL_PATH_YAML_PATH, actual_path)
    _write_markdown(DEFAULT_ACTUAL_PATH_DOC_PATH, _render_actual_path_doc(actual_path))
    _write_yaml(DEFAULT_BETA_ATTRIBUTION_YAML_PATH, beta_attribution)
    _write_markdown(DEFAULT_BETA_ATTRIBUTION_DOC_PATH, _render_attribution_doc(beta_attribution))
    _write_yaml(DEFAULT_CONTRAST_YAML_PATH, contrast)
    _write_markdown(DEFAULT_CONTRAST_DOC_PATH, _render_contrast_doc(contrast))
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_closeout_doc(final_matrix))


def _exposure_profile(predictions: pd.DataFrame, probe: Mapping[str, Any]) -> dict[str, float]:
    weight_map = _mapping(probe.get("weights_by_trend_state"))
    qqq_equivalent = []
    tqqq_weights = []
    for state in predictions["trend_state"].astype(str):
        weights = _mapping(weight_map.get(state))
        qqq = _float(weights.get("QQQ"))
        tqqq = _float(weights.get("TQQQ"))
        qqq_equivalent.append(qqq + 3.0 * tqqq)
        tqqq_weights.append(tqqq)
    avg_qqq_equiv = _mean_list(qqq_equivalent)
    avg_tqqq = _mean_list(tqqq_weights)
    tqqq_beta_share = (3.0 * avg_tqqq / avg_qqq_equiv) if avg_qqq_equiv else 0.0
    return {
        "avg_qqq_equivalent_exposure": round(avg_qqq_equiv, GRID_ROUND_DIGITS),
        "avg_tqqq_weight": round(avg_tqqq, GRID_ROUND_DIGITS),
        "max_tqqq_weight": round(max(tqqq_weights, default=0.0), GRID_ROUND_DIGITS),
        "tqqq_beta_share": round(tqqq_beta_share, GRID_ROUND_DIGITS),
    }


def _component_return_contribution(
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe: Mapping[str, Any],
) -> dict[str, float]:
    returns = prices.pct_change().fillna(0.0)
    weight_map = _mapping(probe.get("weights_by_trend_state"))
    contributions = {asset: 0.0 for asset in ASSETS}
    for raw in predictions.to_dict("records"):
        date = pd.Timestamp(raw["date"])
        if date not in returns.index:
            continue
        weights = _mapping(weight_map.get(str(raw["trend_state"])))
        for asset in ASSETS:
            contributions[asset] += _float(weights.get(asset)) * _float(returns.loc[date, asset])
    return {asset: round(value, GRID_ROUND_DIGITS) for asset, value in contributions.items()}


def _slice_prices(prices: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    start_ts = pd.Timestamp(start)
    end_ts = prices.index.max() if end == "latest" else pd.Timestamp(end)
    return prices.loc[(prices.index >= start_ts) & (prices.index <= end_ts)].copy()


def _slice_predictions(predictions: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    dates = pd.to_datetime(predictions["date"])
    start_ts = pd.Timestamp(start)
    end_ts = dates.max() if end == "latest" else pd.Timestamp(end)
    return predictions.loc[(dates >= start_ts) & (dates <= end_ts)].copy()


def _state_count(predictions: pd.DataFrame, state: str) -> int:
    if predictions.empty:
        return 0
    return int((predictions["trend_state"].astype(str) == state).sum())


def _bool_count(frame: pd.DataFrame, column: str) -> int:
    if column not in frame:
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
    metadata = window_metadata(primary_window)
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
            "label_version": "upper_state_label_taxonomy_v2",
            "feature_set_version": "pit_feature_matrix_v3",
            "model_version": PREDICTION_MODEL_ID,
            "threshold_policy": "return_seeking_diagnostic_lane_policy_v1",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": _candidate_count(summary, extra),
            "pre_registered_selection_rule": True,
        },
        **extra,
    }


def _candidate_count(summary: Mapping[str, Any], extra: Mapping[str, Any]) -> int:
    for key in ("prediction_count", "probe_count", "slice_count"):
        if key in summary:
            return max(0, _int(summary.get(key)))
    for key in ("signal_rows", "probe_rows", "attribution_rows", "contrast_rows"):
        if key in extra:
            return len(_records(extra.get(key)))
    return 0


def _render_scope_doc(lane_policy: Mapping[str, Any], final_matrix: Mapping[str, Any]) -> str:
    summary = _mapping(final_matrix.get("summary"))
    lines = [
        "# Return-Seeking Diagnostic Lane Scope",
        "",
        f"- 状态：`{lane_policy.get('status')}`",
        "- modified_layer：`first_layer`",
        "- frozen_second_layer：`dynamic_second_layer_probe_registry_v2`",
        "- allowed_signals：`stay_constructive`, `add_risk`, `high_confidence_risk_on`",
        "- defensive_overlay_usage_allowed：`false`",
        "- full_allocation_usage_allowed：`false`",
        "- gated_integration_allowed：`false`",
        "- promotion_allowed：`false`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- broker_action：`none`",
        "",
        "## 结论",
        "",
        f"本轮 final status 为 `{summary.get('final_status', final_matrix.get('status'))}`。"
        "该 lane 只保留 return-seeking diagnostic 解释，不输出仓位或晋升结论。",
    ]
    return "\n".join(lines) + "\n"


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


def _render_actual_path_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Actual Path Rows", ""]
    lines.append("| probe_id | return_delta | drawdown_delta | diagnostic_value |")
    lines.append("|---|---:|---:|---|")
    for row in _records(payload.get("probe_rows")):
        lines.append(
            "| {probe_id} | {annual_return_delta_vs_no_return_seeking} | "
            "{drawdown_delta_vs_no_return_seeking} | "
            "{return_seeking_diagnostic_value} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_attribution_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Attribution Rows", ""]
    lines.append("| probe_id | avg_qqq_equiv | tqqq_share | qqq_beta | tqqq_beta |")
    lines.append("|---|---:|---:|---|---|")
    for row in _records(payload.get("attribution_rows")):
        lines.append(
            "| {probe_id} | {avg_qqq_equivalent_exposure} | {tqqq_beta_share} | "
            "{qqq_beta_dependency_suspected} | {tqqq_beta_dependency_suspected} |".format(
                **row
            )
        )
    return "\n".join(lines) + "\n"


def _render_contrast_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Contrast Rows", ""]
    lines.append("| slice_id | probe_id | return_delta | constructive | risk_on |")
    lines.append("|---|---|---:|---:|---:|")
    for row in _records(payload.get("contrast_rows")):
        lines.append(
            "| {slice_id} | {probe_id} | {annual_return_delta_vs_no_return_seeking} | "
            "{constructive_state_count} | {risk_on_state_count} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_closeout_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Closeout", ""]
    summary = _mapping(payload.get("summary"))
    for key in (
        "diagnostic_value_probe_count",
        "beta_or_tqqq_dependency_suspected",
        "depends_on_2023_plus",
        "gated_integration_allowed",
        "promotion_status",
    ):
        lines.append(f"- {key}: `{summary.get(key)}`")
    return "\n".join(lines) + "\n"


def _artifact_paths() -> dict[str, str]:
    paths = {
        "lane_policy": DEFAULT_RETURN_SEEKING_POLICY_PATH,
        "scope_doc": DEFAULT_SCOPE_DOC_PATH,
        "signal_audit_doc": DEFAULT_SIGNAL_AUDIT_DOC_PATH,
        "signal_audit_yaml": DEFAULT_SIGNAL_AUDIT_YAML_PATH,
        "actual_path_doc": DEFAULT_ACTUAL_PATH_DOC_PATH,
        "actual_path_yaml": DEFAULT_ACTUAL_PATH_YAML_PATH,
        "beta_attribution_doc": DEFAULT_BETA_ATTRIBUTION_DOC_PATH,
        "beta_attribution_yaml": DEFAULT_BETA_ATTRIBUTION_YAML_PATH,
        "contrast_doc": DEFAULT_CONTRAST_DOC_PATH,
        "contrast_yaml": DEFAULT_CONTRAST_YAML_PATH,
        "closeout_doc": DEFAULT_CLOSEOUT_DOC_PATH,
        "final_matrix_yaml": DEFAULT_FINAL_MATRIX_YAML_PATH,
    }
    return {key: str(path.relative_to(PROJECT_ROOT)) for key, path in paths.items()}


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _write_markdown(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_scalar(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_scalar(value.item())
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value


def _mean_list(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


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


def _strings(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _records(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)
