from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
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
    _constant_target_frame,
    _data_quality_gate,
    _load_price_matrix,
    _metrics_row,
    _simulate_rebalanced_portfolio,
    _slice_prices,
)
from ai_trading_system.first_layer_policy_calibration import (
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
    STATE_ORDER,
)
from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    validate_research_audit_metadata,
)
from ai_trading_system.research_window_extension import (
    DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    load_research_window_registry,
    window_metadata,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

ASSETS = ["QQQ", "SGOV", "TQQQ"]
PRIMARY_WINDOW_ID = "exact_three_asset_validated"
PROBE_REGISTRY_VERSION = "dynamic_second_layer_probe_registry_v2"
FROZEN_FIRST_LAYER_VERSION = "first_layer_composer_v2"

DEFAULT_PROBE_REGISTRY_V2_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_second_layer_probe_registry_v2.yaml"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_probes" / "second_layer_v2"
DEFAULT_ACTUAL_PATH_OUTPUT_ROOT = DEFAULT_OUTPUT_ROOT / "actual_path_rebacktest"
DEFAULT_EXPOSURE_CSV_PATH = DEFAULT_OUTPUT_ROOT / "qqq_equivalent_exposure_by_state.csv"
DEFAULT_ACTUAL_PATH_CSV_PATH = (
    DEFAULT_ACTUAL_PATH_OUTPUT_ROOT / "probe_actual_path_metrics.csv"
)
DEFAULT_STATIC_FRONTIER_CSV_PATH = (
    DEFAULT_OUTPUT_ROOT / "static_frontier_representatives_primary.csv"
)
DEFAULT_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "models"
    / "first_layer_composer_v2_predictions.csv"
)

DEFAULT_REGISTRY_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dynamic_second_layer_probe_registry_v2_review.md"
)
DEFAULT_EXPOSURE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_exposure_review_v2.md"
)
DEFAULT_EXPOSURE_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "second_layer_probe_exposure_matrix_v2.yaml"
)
DEFAULT_ACTUAL_PATH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_actual_path_review_v2.md"
)
DEFAULT_ACTUAL_PATH_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "second_layer_probe_actual_path_matrix_v2.yaml"
)
DEFAULT_SAME_RISK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_same_risk_frontier_review_v2.md"
)
DEFAULT_SAME_RISK_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "second_layer_probe_same_risk_frontier_matrix_v2.yaml"
)
DEFAULT_TQQQ_STRESS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_tqqq_stress_review_v2.md"
)
DEFAULT_TQQQ_STRESS_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "second_layer_probe_tqqq_stress_matrix_v2.yaml"
)
DEFAULT_READINESS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_action_value_probe_readiness_review_v2.md"
)
DEFAULT_READINESS_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "second_layer_action_value_probe_readiness_v2.yaml"
)
DEFAULT_DEPENDENCY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_calibration_probe_dependency_update.md"
)
DEFAULT_OWNER_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_library_freeze_owner_pack.md"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_library_freeze_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "second_layer_probe_library_freeze_final_matrix.yaml"
)


def run_second_layer_probe_library_freeze_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_V2_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    predictions_path: Path = DEFAULT_PREDICTIONS_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    window_registry = load_research_window_registry(registry_path)
    primary_window = _mapping(window_registry["windows"][PRIMARY_WINDOW_ID])
    probe_registry = load_dynamic_second_layer_probe_registry_v2(probe_registry_path)
    expanded_config = _load_yaml_mapping(expanded_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=as_of_date,
        expected_tickers=ASSETS,
    )
    if not data_gate["passed"]:
        raise RuntimeError(
            f"Cached data quality gate failed for second-layer probe freeze: {data_gate['status']}"
        )

    prices = _slice_prices(
        _load_price_matrix(prices_path, ASSETS),
        start_date=date.fromisoformat(str(primary_window["actual_portfolio_start"])),
        end_date=None,
    ).dropna(subset=ASSETS)
    predictions = _load_frozen_first_layer_predictions(predictions_path)
    probes = _records(probe_registry.get("probes"))
    audit = _research_audit_metadata(candidate_count=len(probes))
    registry_validation = validate_dynamic_second_layer_probe_registry_v2(probe_registry)
    exposure = build_probe_exposure_matrix(probe_registry)
    static_metrics = build_primary_static_frontier(prices=prices, output_root=output_root)
    actual = build_probe_actual_path_metrics(
        prices=prices,
        predictions=predictions,
        probe_registry=probe_registry,
        output_root=output_root,
    )
    same_risk = build_same_risk_frontier_matrix(
        actual_metrics=actual,
        static_metrics=static_metrics,
        readiness_policy=_mapping(probe_registry.get("freeze_readiness_policy")),
    )
    tqqq_stress = build_tqqq_stress_matrix(actual_metrics=actual)
    readiness = build_action_value_probe_readiness(
        probe_registry=probe_registry,
        actual_metrics=actual,
        same_risk=same_risk,
        tqqq_stress=tqqq_stress,
    )
    final = build_probe_library_freeze_final_matrix(
        registry_validation=registry_validation,
        exposure=exposure,
        actual_metrics=actual,
        same_risk=same_risk,
        tqqq_stress=tqqq_stress,
        readiness=readiness,
        data_gate=data_gate,
    )
    write_second_layer_probe_freeze_outputs(
        registry_validation=registry_validation,
        exposure=exposure,
        actual_metrics=actual,
        same_risk=same_risk,
        tqqq_stress=tqqq_stress,
        readiness=readiness,
        final=final,
        audit=audit,
        data_gate=data_gate,
        window=primary_window,
        probe_registry=probe_registry,
    )
    return {
        "status": final["status"],
        "summary": final["summary"],
        "artifact_paths": {
            "probe_registry": str(probe_registry_path),
            "exposure_csv": str(DEFAULT_EXPOSURE_CSV_PATH),
            "actual_path_csv": str(DEFAULT_ACTUAL_PATH_CSV_PATH),
            "owner_pack": str(DEFAULT_OWNER_PACK_DOC_PATH),
            "final_matrix": str(DEFAULT_FINAL_MATRIX_YAML_PATH),
        },
        **SAFETY_BOUNDARY,
    }


def load_dynamic_second_layer_probe_registry_v2(
    path: Path = DEFAULT_PROBE_REGISTRY_V2_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"Probe registry must be a mapping: {path}")
    return raw


def validate_dynamic_second_layer_probe_registry_v2(
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    required_ids = {
        "defensive_overlay_probe",
        "balanced_dynamic_probe",
        "drawdown_control_probe",
        "no_tqqq_return_seeking_probe",
        "low_tqqq_balanced_growth_probe",
        "qqq_heavy_growth_probe",
        "capped_risk_on_diagnostic_probe",
        "asymmetric_risk_on_slow_confirm_probe",
    }
    probes = _records(registry.get("probes"))
    ids = {str(probe.get("probe_id")) for probe in probes}
    rows: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for missing in sorted(required_ids - ids):
        issues.append({"code": "missing_required_probe", "probe_id": missing})
    for probe in probes:
        probe_id = str(probe.get("probe_id"))
        weights_by_state = _mapping(probe.get("weights_by_trend_state"))
        state_exposures = []
        state_valid = True
        for state in STATE_ORDER:
            weights = _normalized_weight_row(weights_by_state.get(state))
            weight_sum = sum(weights.values())
            long_only = all(value >= -1e-12 for value in weights.values())
            sum_valid = math.isclose(weight_sum, 1.0, abs_tol=1e-9)
            state_valid = state_valid and long_only and sum_valid
            state_exposures.append(_qqq_equiv(weights))
            if not long_only:
                issues.append({"code": "short_weight", "probe_id": probe_id, "state": state})
            if not sum_valid:
                issues.append(
                    {
                        "code": "weights_do_not_sum_to_one",
                        "probe_id": probe_id,
                        "state": state,
                        "weight_sum": round(weight_sum, GRID_ROUND_DIGITS),
                    }
                )
        trend_sensitive = len({round(value, 6) for value in state_exposures}) > 1
        if not trend_sensitive:
            issues.append({"code": "probe_not_trend_sensitive", "probe_id": probe_id})
        tqqq_max = max(
            _normalized_weight_row(weights_by_state.get(state)).get("TQQQ", 0.0)
            for state in STATE_ORDER
        )
        no_tqqq_role = "no_tqqq" in _string_list(probe.get("role_tags"))
        if no_tqqq_role and tqqq_max != 0.0:
            issues.append({"code": "no_tqqq_probe_uses_tqqq", "probe_id": probe_id})
        if bool(probe.get("promotion_enabled")) or bool(probe.get("broker_enabled")):
            issues.append({"code": "promotion_or_broker_enabled", "probe_id": probe_id})
        rows.append(
            {
                "probe_id": probe_id,
                "trend_sensitive": trend_sensitive,
                "weight_constraints_pass": state_valid,
                "tqqq_max_weight": round(tqqq_max, GRID_ROUND_DIGITS),
                "research_only": bool(probe.get("research_only")),
                "promotion_enabled": bool(probe.get("promotion_enabled")),
                "broker_enabled": bool(probe.get("broker_enabled")),
                "registry_status": "PASS" if state_valid and trend_sensitive else "FAIL",
            }
        )
    return _payload(
        report_type="dynamic_second_layer_probe_registry_v2_validation",
        title="Dynamic Second-Layer Probe Registry v2 Validation",
        status="DYNAMIC_SECOND_LAYER_PROBE_REGISTRY_V2_READY_PROMOTION_BLOCKED"
        if not issues
        else "DYNAMIC_SECOND_LAYER_PROBE_REGISTRY_V2_INVALID",
        summary={
            "probe_count": len(probes),
            "required_probe_count": len(required_ids),
            "missing_required_probe_count": len(required_ids - ids),
            "constraint_issue_count": len(issues),
            "return_seeking_probe_count": sum(
                bool(probe.get("return_seeking")) for probe in probes
            ),
        },
        validation_rows=rows,
        issues=issues,
    )


def build_probe_exposure_matrix(registry: Mapping[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for probe in _records(registry.get("probes")):
        weights_by_state = _mapping(probe.get("weights_by_trend_state"))
        exposures = [
            _qqq_equiv(_normalized_weight_row(weights_by_state.get(state)))
            for state in STATE_ORDER
        ]
        monotonic = all(
            later >= earlier for earlier, later in zip(exposures, exposures[1:], strict=False)
        )
        for state, exposure in zip(STATE_ORDER, exposures, strict=True):
            weights = _normalized_weight_row(weights_by_state.get(state))
            rows.append(
                {
                    "probe_id": str(probe.get("probe_id")),
                    "role": str(probe.get("role")),
                    "trend_state": state,
                    "QQQ": weights["QQQ"],
                    "SGOV": weights["SGOV"],
                    "TQQQ": weights["TQQQ"],
                    "qqq_equivalent_exposure": round(exposure, GRID_ROUND_DIGITS),
                    "tqqq_weight": weights["TQQQ"],
                    "risk_bucket": _risk_bucket(exposure),
                    "monotonic_risk_profile": monotonic,
                    "allowed_usage": "|".join(_string_list(probe.get("allowed_usage"))),
                    "blocked_usage": "|".join(_string_list(probe.get("blocked_usage"))),
                }
            )
    frame = pd.DataFrame(rows)
    DEFAULT_EXPOSURE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(DEFAULT_EXPOSURE_CSV_PATH, index=False)
    return frame


def build_primary_static_frontier(*, prices: pd.DataFrame, output_root: Path) -> pd.DataFrame:
    rows = []
    for rank, weights in enumerate(_weight_grid(0.10), start=1):
        sim = _simulate_rebalanced_portfolio(
            prices,
            _constant_target_frame(prices.index, prices.columns, weights),
            rebalance="monthly",
            transaction_cost_bps=0.0,
        )
        rows.append(
            {
                **_primary_window_metadata(),
                **_metrics_row(
                    strategy_id="static_"
                    + "_".join(f"{asset}{weight:.1f}" for asset, weight in weights.items()),
                    candidate_family="second_layer_v2_static_frontier",
                    weights=weights,
                    sim=sim,
                    annualization=252,
                    selection_rank=rank,
                ),
            }
        )
    frame = pd.DataFrame(rows)
    output_root.mkdir(parents=True, exist_ok=True)
    frame.to_csv(DEFAULT_STATIC_FRONTIER_CSV_PATH, index=False)
    return frame


def build_probe_actual_path_metrics(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    output_root: Path,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rank, probe in enumerate(_records(probe_registry.get("probes")), start=1):
        sim_payload = _simulate_probe_actual_path(
            prices=prices,
            predictions=predictions,
            probe=probe,
        )
        sim = sim_payload["simulation"]
        weights = sim_payload["average_target_weights"]
        metrics = _metrics_row(
            strategy_id=str(probe.get("probe_id")),
            candidate_family=str(probe.get("role")),
            weights=weights,
            sim=sim,
            annualization=252,
            selection_rank=rank,
        )
        returns = pd.to_numeric(sim["daily_returns"], errors="coerce").fillna(0.0)
        target = sim_payload["target_weights"]
        applied = sim["applied_weights"].reindex(index=target.index, columns=target.columns)
        gap = (target - applied).abs().sum(axis=1).mean() if not target.empty else 0.0
        tqqq_contrib = sim["asset_contributions"].get("TQQQ", pd.Series(0.0, index=returns.index))
        rows.append(
            {
                **_primary_window_metadata(),
                "probe_id": str(probe.get("probe_id")),
                "role": str(probe.get("role")),
                "date_start": sim_payload["date_start"],
                "date_end": sim_payload["date_end"],
                "model_id": FROZEN_FIRST_LAYER_VERSION,
                "annual_return": metrics["annual_return"],
                "max_drawdown_daily_equity": metrics["max_drawdown_daily_equity"],
                "sharpe": metrics["sharpe_daily_zero_rf"],
                "calmar": metrics["calmar_daily_equity_dd"],
                "turnover": metrics["turnover"],
                "net_of_cost_return": metrics["net_annual_return"],
                "worst_5d_return": metrics["worst_5d_return"],
                "realized_volatility": metrics["annual_volatility"],
                "tqqq_max_weight": round(float(target["TQQQ"].max()), GRID_ROUND_DIGITS),
                "tqqq_avg_weight": round(float(target["TQQQ"].mean()), GRID_ROUND_DIGITS),
                "tqqq_exposure": round(float(target["TQQQ"].max()), GRID_ROUND_DIGITS),
                "qqq_equivalent_exposure": round(
                    float((target["QQQ"] + 3.0 * target["TQQQ"]).mean()),
                    GRID_ROUND_DIGITS,
                ),
                "target_vs_actual_gap": round(float(gap), GRID_ROUND_DIGITS),
                "tqqq_return_contribution_sum": round(float(tqqq_contrib.sum()), GRID_ROUND_DIGITS),
                "actual_path_metric_complete": True,
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    frame = pd.DataFrame(rows)
    (output_root / "actual_path_rebacktest").mkdir(parents=True, exist_ok=True)
    frame.to_csv(DEFAULT_ACTUAL_PATH_CSV_PATH, index=False)
    return frame


def build_same_risk_frontier_matrix(
    *,
    actual_metrics: pd.DataFrame,
    static_metrics: pd.DataFrame,
    readiness_policy: Mapping[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, probe in actual_metrics.iterrows():
        static = _same_risk_static_baseline(
            static_metrics=static_metrics,
            qqq_equiv=_float(probe.get("qqq_equivalent_exposure")),
        )
        annual_delta = _float(probe.get("annual_return")) - _float(static.get("annual_return"))
        calmar_delta = _float(probe.get("calmar")) - _float(static.get("calmar_daily_equity_dd"))
        dd_delta = _float(probe.get("max_drawdown_daily_equity")) - _float(
            static.get("max_drawdown_daily_equity")
        )
        verdict = _same_risk_verdict(
            probe=probe,
            annual_delta=annual_delta,
            calmar_delta=calmar_delta,
            readiness_policy=readiness_policy,
        )
        rows.append(
            {
                **_primary_window_metadata(),
                "probe_id": str(probe.get("probe_id")),
                "role": str(probe.get("role")),
                "qqq_equivalent_exposure": _float(probe.get("qqq_equivalent_exposure")),
                "realized_volatility": _float(probe.get("realized_volatility")),
                "max_drawdown_bucket": _drawdown_bucket(
                    _float(probe.get("max_drawdown_daily_equity"))
                ),
                "tqqq_exposure_bucket": _tqqq_bucket(_float(probe.get("tqqq_max_weight"))),
                "same_risk_static_strategy_id": static.get("strategy_id"),
                "same_risk_static_annual_return": _float(static.get("annual_return")),
                "same_risk_static_max_drawdown": _float(static.get("max_drawdown_daily_equity")),
                "same_risk_static_calmar": _float(static.get("calmar_daily_equity_dd")),
                "annual_return_delta": round(annual_delta, GRID_ROUND_DIGITS),
                "calmar_delta": round(calmar_delta, GRID_ROUND_DIGITS),
                "max_drawdown_delta": round(dd_delta, GRID_ROUND_DIGITS),
                "same_risk_verdict": verdict,
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    return pd.DataFrame(rows)


def build_tqqq_stress_matrix(actual_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, probe in actual_metrics.iterrows():
        max_weight = _float(probe.get("tqqq_max_weight"))
        avg_weight = _float(probe.get("tqqq_avg_weight"))
        contribution = _float(probe.get("tqqq_return_contribution_sum"))
        annual_return = _float(probe.get("annual_return"))
        beta_only = max_weight > 0 and contribution > 0 and annual_return <= contribution
        stress_blocked = _float(probe.get("worst_5d_return")) <= -0.12 or _float(
            probe.get("max_drawdown_daily_equity")
        ) <= -0.18
        rows.append(
            {
                **_primary_window_metadata(),
                "probe_id": str(probe.get("probe_id")),
                "role": str(probe.get("role")),
                "has_tqqq": max_weight > 0,
                "tqqq_contribution_to_return": round(contribution, GRID_ROUND_DIGITS),
                "tqqq_contribution_to_drawdown": round(
                    min(0.0, _float(probe.get("max_drawdown_daily_equity")) * avg_weight),
                    GRID_ROUND_DIGITS,
                ),
                "tqqq_max_weight": round(max_weight, GRID_ROUND_DIGITS),
                "tqqq_avg_weight": round(avg_weight, GRID_ROUND_DIGITS),
                "stress_window_loss": _float(probe.get("max_drawdown_daily_equity")),
                "worst_5d_loss": _float(probe.get("worst_5d_return")),
                "tqqq_beta_only": beta_only,
                "stress_risk_blocked": stress_blocked,
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    return pd.DataFrame(rows)


def build_action_value_probe_readiness(
    *,
    probe_registry: Mapping[str, Any],
    actual_metrics: pd.DataFrame,
    same_risk: pd.DataFrame,
    tqqq_stress: pd.DataFrame,
) -> dict[str, Any]:
    probes = {str(probe.get("probe_id")): probe for probe in _records(probe_registry.get("probes"))}
    same_by_id = {str(row["probe_id"]): row for _, row in same_risk.iterrows()}
    stress_by_id = {str(row["probe_id"]): row for _, row in tqqq_stress.iterrows()}
    metric_by_id = {str(row["probe_id"]): row for _, row in actual_metrics.iterrows()}
    rows: list[dict[str, Any]] = []
    for probe_id, probe in probes.items():
        same_row = same_by_id[probe_id]
        stress_row = stress_by_id[probe_id]
        metric_row = metric_by_id[probe_id]
        role_tags = set(_string_list(probe.get("role_tags")))
        blockers: list[str] = []
        if not bool(metric_row.get("actual_path_metric_complete")):
            blockers.append("missing_actual_path_metrics")
        if bool(stress_row.get("stress_risk_blocked")):
            blockers.append("stress_risk_blocked")
        if same_row.get("same_risk_verdict") in {"STATIC_FRONTIER_DOMINATES", "TQQQ_BETA_ONLY"}:
            blockers.append(str(same_row.get("same_risk_verdict")).lower())
        if "diagnostic_only" in role_tags or probe_id == "capped_risk_on_diagnostic_probe":
            status = "ACTION_VALUE_PROBE_DIAGNOSTIC_ONLY"
            usage = "risk_on_diagnostic_only"
        elif blockers:
            status = "ACTION_VALUE_PROBE_DIAGNOSTIC_ONLY"
            usage = "diagnostic_until_blockers_clear"
        else:
            status = "ACTION_VALUE_PROBE_APPROVED"
            usage = "first_layer_action_value_matrix"
        rows.append(
            {
                **_primary_window_metadata(),
                "probe_id": probe_id,
                "role": str(probe.get("role")),
                "readiness_status": status,
                "allowed_action_value_usage": usage,
                "same_risk_verdict": same_row.get("same_risk_verdict"),
                "stress_risk_blocked": bool(stress_row.get("stress_risk_blocked")),
                "tqqq_beta_only": bool(stress_row.get("tqqq_beta_only")),
                "blockers": blockers,
                "next_action": _readiness_next_action(status, blockers),
                "target_path_metrics_used_for_pass": False,
                **SAFETY_BOUNDARY,
            }
        )
    return _payload(
        report_type="second_layer_action_value_probe_readiness_v2",
        title="Second-Layer Action-Value Probe Readiness v2",
        status="SECOND_LAYER_ACTION_VALUE_PROBE_READINESS_READY_PROMOTION_BLOCKED",
        summary={
            "probe_count": len(rows),
            "approved_action_value_probe_count": sum(
                row["readiness_status"] == "ACTION_VALUE_PROBE_APPROVED" for row in rows
            ),
            "diagnostic_only_probe_count": sum(
                row["readiness_status"] == "ACTION_VALUE_PROBE_DIAGNOSTIC_ONLY" for row in rows
            ),
            "rejected_probe_count": sum(
                row["readiness_status"] == "ACTION_VALUE_PROBE_REJECTED" for row in rows
            ),
        },
        probe_rows=rows,
    )


def build_probe_library_freeze_final_matrix(
    *,
    registry_validation: Mapping[str, Any],
    exposure: pd.DataFrame,
    actual_metrics: pd.DataFrame,
    same_risk: pd.DataFrame,
    tqqq_stress: pd.DataFrame,
    readiness: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(readiness.get("summary"))
    approved = _int(summary.get("approved_action_value_probe_count"))
    diagnostic = _int(summary.get("diagnostic_only_probe_count"))
    rejected = _int(summary.get("rejected_probe_count"))
    if str(registry_validation.get("status")).endswith("_INVALID"):
        base_status = "SECOND_LAYER_PROBE_LIBRARY_INSUFFICIENT"
    elif approved > 0 and diagnostic > 0:
        base_status = "SECOND_LAYER_RETURN_SEEKING_PROBES_DIAGNOSTIC_ONLY"
    elif approved > 0:
        base_status = "SECOND_LAYER_RETURN_SEEKING_PROBES_APPROVED"
    else:
        base_status = "SECOND_LAYER_PROBE_LIBRARY_INSUFFICIENT"
    return _payload(
        report_type="second_layer_probe_library_freeze_final_matrix",
        title="Second-Layer Probe Library Freeze Final Matrix",
        status=base_status,
        summary={
            **_primary_window_metadata(),
            "probe_registry_version": PROBE_REGISTRY_VERSION,
            "probe_count": len(actual_metrics),
            "state_exposure_rows": len(exposure),
            "approved_action_value_probe_count": approved,
            "diagnostic_only_probe_count": diagnostic,
            "rejected_probe_count": rejected,
            "same_risk_frontier_summary": _value_counts(same_risk, "same_risk_verdict"),
            "tqqq_probe_count": int(tqqq_stress["has_tqqq"].astype(bool).sum()),
            "tqqq_stress_blocked_count": int(
                tqqq_stress["stress_risk_blocked"].astype(bool).sum()
            ),
            "data_quality_status": data_gate.get("status"),
            "first_layer_dependency": "freeze_dynamic_second_layer_probe_registry_v2",
        },
        readiness_summary=summary,
    )


def write_second_layer_probe_freeze_outputs(
    *,
    registry_validation: Mapping[str, Any],
    exposure: pd.DataFrame,
    actual_metrics: pd.DataFrame,
    same_risk: pd.DataFrame,
    tqqq_stress: pd.DataFrame,
    readiness: Mapping[str, Any],
    final: Mapping[str, Any],
    audit: Mapping[str, Any],
    data_gate: Mapping[str, Any],
    window: Mapping[str, Any],
    probe_registry: Mapping[str, Any],
) -> None:
    metadata = window_metadata(window)
    payloads = {
        DEFAULT_EXPOSURE_YAML_PATH: _table_payload(
            report_type="second_layer_probe_exposure_matrix_v2",
            title="Second-Layer Probe Exposure Matrix v2",
            status="SECOND_LAYER_PROBE_EXPOSURE_MATRIX_READY_PROMOTION_BLOCKED",
            summary={
                **metadata,
                "probe_count": int(exposure["probe_id"].nunique()),
                "row_count": len(exposure),
            },
            rows_key="exposure_rows",
            frame=exposure,
            audit=audit,
        ),
        DEFAULT_ACTUAL_PATH_YAML_PATH: _table_payload(
            report_type="second_layer_probe_actual_path_matrix_v2",
            title="Second-Layer Probe Actual-Path Matrix v2",
            status="SECOND_LAYER_PROBE_ACTUAL_PATH_READY_PROMOTION_BLOCKED",
            summary={
                **metadata,
                "probe_count": len(actual_metrics),
                "data_quality_status": data_gate.get("status"),
                "target_path_metrics_used_for_pass": False,
            },
            rows_key="probe_rows",
            frame=actual_metrics,
            audit=audit,
            extra={"data_quality": data_gate},
        ),
        DEFAULT_SAME_RISK_YAML_PATH: _table_payload(
            report_type="second_layer_probe_same_risk_frontier_matrix_v2",
            title="Second-Layer Probe Same-Risk Frontier Matrix v2",
            status="SECOND_LAYER_PROBE_SAME_RISK_FRONTIER_READY_PROMOTION_BLOCKED",
            summary={
                **metadata,
                "probe_count": len(same_risk),
                "same_risk_frontier_summary": _value_counts(same_risk, "same_risk_verdict"),
            },
            rows_key="probe_rows",
            frame=same_risk,
            audit=audit,
        ),
        DEFAULT_TQQQ_STRESS_YAML_PATH: _table_payload(
            report_type="second_layer_probe_tqqq_stress_matrix_v2",
            title="Second-Layer Probe TQQQ Stress Matrix v2",
            status="SECOND_LAYER_PROBE_TQQQ_STRESS_READY_PROMOTION_BLOCKED",
            summary={
                **metadata,
                "probe_count": len(tqqq_stress),
                "tqqq_probe_count": int(tqqq_stress["has_tqqq"].astype(bool).sum()),
                "stress_blocked_count": int(
                    tqqq_stress["stress_risk_blocked"].astype(bool).sum()
                ),
            },
            rows_key="probe_rows",
            frame=tqqq_stress,
            audit=audit,
        ),
        DEFAULT_READINESS_YAML_PATH: {**readiness, "research_audit_metadata": dict(audit)},
        DEFAULT_FINAL_MATRIX_YAML_PATH: {**final, "research_audit_metadata": dict(audit)},
    }
    for path, payload in payloads.items():
        _assert_audit_metadata(payload)
        _write_yaml(path, payload)

    _write_markdown(DEFAULT_REGISTRY_REVIEW_DOC_PATH, _render_payload_doc(registry_validation))
    _write_markdown(
        DEFAULT_EXPOSURE_DOC_PATH,
        _render_payload_doc(payloads[DEFAULT_EXPOSURE_YAML_PATH]),
    )
    _write_markdown(
        DEFAULT_ACTUAL_PATH_DOC_PATH,
        _render_payload_doc(payloads[DEFAULT_ACTUAL_PATH_YAML_PATH]),
    )
    _write_markdown(
        DEFAULT_SAME_RISK_DOC_PATH,
        _render_payload_doc(payloads[DEFAULT_SAME_RISK_YAML_PATH]),
    )
    _write_markdown(
        DEFAULT_TQQQ_STRESS_DOC_PATH,
        _render_payload_doc(payloads[DEFAULT_TQQQ_STRESS_YAML_PATH]),
    )
    _write_markdown(
        DEFAULT_READINESS_DOC_PATH,
        _render_payload_doc(payloads[DEFAULT_READINESS_YAML_PATH]),
    )
    _write_markdown(DEFAULT_DEPENDENCY_DOC_PATH, _render_dependency_doc(probe_registry))
    _write_markdown(DEFAULT_OWNER_PACK_DOC_PATH, _render_owner_pack(payloads, final))
    _write_markdown(
        DEFAULT_CLOSEOUT_DOC_PATH,
        _render_payload_doc(payloads[DEFAULT_FINAL_MATRIX_YAML_PATH]),
    )


def _simulate_probe_actual_path(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe: Mapping[str, Any],
) -> dict[str, Any]:
    pred = predictions.drop_duplicates("date", keep="last").copy()
    pred["date"] = pd.to_datetime(pred["date"])
    pred = pred.set_index("date").sort_index()
    start = max(prices.index.min(), pred.index.min())
    end = min(prices.index.max(), pred.index.max())
    sliced = prices.loc[(prices.index >= start) & (prices.index <= end)].copy()
    state_series = pred["trend_state"].reindex(sliced.index).ffill().bfill()
    lagged = state_series.shift(1).fillna(state_series)
    adjusted = _apply_entry_rules(lagged, sliced, probe)
    weights_by_state = _mapping(probe.get("weights_by_trend_state"))
    target = pd.DataFrame(0.0, index=sliced.index, columns=sliced.columns)
    for timestamp, state in adjusted.items():
        weights = _normalized_weight_row(weights_by_state.get(str(state)))
        for asset, weight in weights.items():
            if asset in target.columns:
                target.loc[timestamp, asset] = weight
    sim = _simulate_rebalanced_portfolio(
        sliced,
        target,
        rebalance="daily",
        transaction_cost_bps=0.0,
    )
    return {
        "simulation": sim,
        "target_weights": target,
        "average_target_weights": target.mean().to_dict(),
        "date_start": start.date().isoformat(),
        "date_end": end.date().isoformat(),
    }


def _apply_entry_rules(
    states: pd.Series,
    prices: pd.DataFrame,
    probe: Mapping[str, Any],
) -> pd.Series:
    rules = _mapping(probe.get("entry_rules"))
    if not rules:
        return states
    constructive_days = _int(rules.get("constructive_confirmation_days"), default=2)
    risk_on_days = _int(rules.get("risk_on_confirmation_days"), default=3)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    realized_vol = qqq_returns.rolling(20).std(ddof=0).fillna(0.0) * math.sqrt(252.0)
    adjusted: list[str] = []
    constructive_streak = 0
    risk_on_streak = 0
    for timestamp, raw_state in states.items():
        state = str(raw_state)
        if state in {"risk_off", "defensive", "neutral"}:
            constructive_streak = 0
            risk_on_streak = 0
            adjusted.append(state)
            continue
        if state == "constructive":
            constructive_streak += 1
            risk_on_streak = 0
            adjusted.append(
                "constructive" if constructive_streak >= constructive_days else "neutral"
            )
            continue
        if state == "risk_on":
            risk_on_streak += 1
            constructive_streak += 1
            vol_ok = float(realized_vol.get(timestamp, 0.0)) <= 0.45
            event_ok = str(rules.get("event_risk_gate")) != "unavailable_blocks_risk_on_entry"
            if risk_on_streak >= risk_on_days and vol_ok and event_ok:
                adjusted.append("risk_on")
            elif constructive_streak >= constructive_days:
                adjusted.append("constructive")
            else:
                adjusted.append("neutral")
            continue
        adjusted.append("neutral")
    return pd.Series(adjusted, index=states.index)


def _load_frozen_first_layer_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Frozen first-layer predictions not found: {path}")
    frame = pd.read_csv(path)
    required = {"date", "trend_state"}
    if not required <= set(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise ValueError(f"Prediction file missing required columns: {missing}")
    if "research_window_id" in frame.columns:
        frame = frame.loc[frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID].copy()
    return frame[["date", "trend_state"]].copy()


def _same_risk_static_baseline(*, static_metrics: pd.DataFrame, qqq_equiv: float) -> dict[str, Any]:
    frame = static_metrics.copy()
    frame["risk_distance"] = (frame["qqq_equivalent_exposure"].astype(float) - qqq_equiv).abs()
    selected = frame.sort_values(
        ["risk_distance", "annual_return"],
        ascending=[True, False],
    ).iloc[0]
    return dict(selected)


def _same_risk_verdict(
    *,
    probe: Mapping[str, Any],
    annual_delta: float,
    calmar_delta: float,
    readiness_policy: Mapping[str, Any],
) -> str:
    if str(probe.get("probe_id")) == "capped_risk_on_diagnostic_probe":
        return "DIAGNOSTIC_ONLY"
    if _float(probe.get("worst_5d_return")) <= _float(
        readiness_policy.get("stress_worst_5d_floor")
    ):
        return "STRESS_RISK_BLOCKED"
    if annual_delta > 0.0 and calmar_delta >= 0.0:
        return "PROBE_BEATS_SAME_RISK_FRONTIER"
    if annual_delta < 0.0 and _float(probe.get("max_drawdown_daily_equity")) > -0.08:
        return "RISK_REDUCTION_WITH_RETURN_DRAG"
    if _float(probe.get("tqqq_max_weight")) > 0.0 and annual_delta > 0.0:
        return "TQQQ_BETA_ONLY"
    if annual_delta <= 0.0:
        return "STATIC_FRONTIER_DOMINATES"
    return "BETA_EXPOSURE_ONLY"


def _research_audit_metadata(*, candidate_count: int) -> dict[str, Any]:
    return {
        "modified_layer": "second_layer",
        "frozen_first_layer_version": FROZEN_FIRST_LAYER_VERSION,
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v1",
        "research_window_id": PRIMARY_WINDOW_ID,
        "label_version": "upper_state_labels_v2_reference_only",
        "feature_set_version": "pit_feature_matrix_v3_reference_only",
        "model_version": FROZEN_FIRST_LAYER_VERSION,
        "threshold_policy": "first_layer_threshold_policy_v2_frozen",
        "probe_registry_version": PROBE_REGISTRY_VERSION,
        "candidate_count": candidate_count,
        "pre_registered_selection_rule": True,
    }


def _primary_window_metadata() -> dict[str, Any]:
    return {
        "research_window_id": PRIMARY_WINDOW_ID,
        "research_window_alias": "EXACT_THREE_ASSET_VALIDATED_WINDOW",
        "requested_start": "2021-02-22",
        "actual_start": "2021-02-22",
        "actual_portfolio_start": "2021-02-22",
        "end": "latest",
        "window_role": "primary_validated",
        "data_quality_contract": "secondary_cross_checked",
        "exact_or_proxy": "exact",
    }


def _table_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    rows_key: str,
    frame: pd.DataFrame,
    audit: Mapping[str, Any],
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary=summary,
        research_audit_metadata=dict(audit),
        **{rows_key: _frame_rows(frame)},
    )
    if extra:
        payload.update(dict(extra))
    return payload


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **sections: Any,
) -> dict[str, Any]:
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        **_primary_window_metadata(),
        "summary": dict(summary),
        **sections,
        **SAFETY_BOUNDARY,
    }


def _assert_audit_metadata(payload: Mapping[str, Any]) -> None:
    validation = validate_research_audit_metadata(payload, load_research_audit_metadata_schema())
    if validation["status"] != "PASS":
        raise ValueError(f"research_audit_metadata contract failed: {validation['issues']}")


def _render_dependency_doc(probe_registry: Mapping[str, Any]) -> str:
    probe_count = len(_records(probe_registry.get("probes")))
    return "\n".join(
        [
            "# First-Layer Calibration Probe Dependency Update",
            "",
            "- 状态：`FIRST_LAYER_CALIBRATION_DEPENDENCY_UPDATED_PROMOTION_BLOCKED`",
            "- frozen probe registry：`dynamic_second_layer_probe_registry_v2`",
            f"- probe count：`{probe_count}`",
            "- 后续 first-layer label / feature / model / threshold calibration "
            "必须使用本 registry。",
            "- 除非开启新的 second-layer-only research round，不得在 first-layer "
            "校准过程中修改 probe weights 或 entry rules。",
            "- promotion_allowed：`False`；paper_shadow_allowed：`False`；"
            "production_allowed：`False`；broker_action：`none`。",
            "",
        ]
    )


def _render_owner_pack(payloads: Mapping[Path, Mapping[str, Any]], final: Mapping[str, Any]) -> str:
    readiness = _mapping(payloads[DEFAULT_READINESS_YAML_PATH])
    readiness_summary = _mapping(readiness.get("summary"))
    final_summary = _mapping(final.get("summary"))
    approved = readiness_summary.get("approved_action_value_probe_count")
    diagnostic = readiness_summary.get("diagnostic_only_probe_count")
    rejected = readiness_summary.get("rejected_probe_count")
    stress_blocked = final_summary.get("tqqq_stress_blocked_count")
    return "\n".join(
        [
            "# Second-Layer Probe Library Freeze Owner Pack",
            "",
            f"- 状态：`{final.get('status')}`",
            "- 当前 second-layer probe library 是否完整：`yes`，v2 定义 8 类 probes。",
            "- 是否补齐收益型 / risk-on sensitive probes：`yes`，新增 no-TQQQ、"
            "low-TQQQ、QQQ-heavy、capped risk-on 和 slow-confirm probes。",
            f"- 可进入 first-layer action-value matrix 的 probe 数：`{approved}`",
            f"- diagnostic-only probe 数：`{diagnostic}`",
            f"- rejected probe 数：`{rejected}`",
            "- TQQQ 使用是否安全：所有 TQQQ probe 均 research-only，"
            f"promotion/broker disabled；stress blocked count=`{stress_blocked}`。",
            "- 哪些 probe 在 primary window 下稳定：以 same-risk frontier verdict "
            "和 readiness matrix 为准；legacy/sensitivity 不参与本批主结论。",
            "- dynamic promotion 为什么仍 blocked：本批只冻结第二层 probe library，"
            "不是 owner approval、paper-shadow 或 production readiness。",
            "",
        ]
    )


def _render_payload_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {payload.get('title', payload.get('report_type'))}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- report_type：`{payload.get('report_type')}`",
        "- market_regime：`ai_after_chatgpt`",
        "- research_only：`True`",
        "- actual_path_required：`True`",
        "- target_path_metrics_role：`diagnostic_only`",
        "- promotion_allowed：`False`",
        "- paper_shadow_allowed：`False`",
        "- production_allowed：`False`",
        "- broker_action：`none`",
        "",
        "## Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.append("")
    return "\n".join(lines)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _frame_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            str(key): _python_scalar(value)
            for key, value in row.items()
        }
        for row in frame.to_dict(orient="records")
    ]


def _python_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    counts = frame[column].value_counts().sort_index()
    return {str(key): int(value) for key, value in counts.items()}


def _weight_grid(step: float) -> list[dict[str, float]]:
    units = int(round(1.0 / step))
    rows = []
    for qqq_units in range(units + 1):
        for sgov_units in range(units - qqq_units + 1):
            tqqq_units = units - qqq_units - sgov_units
            rows.append(
                {
                    "QQQ": round(qqq_units / units, 6),
                    "SGOV": round(sgov_units / units, 6),
                    "TQQQ": round(tqqq_units / units, 6),
                }
            )
    return rows


def _normalized_weight_row(value: object) -> dict[str, float]:
    mapping = _mapping(value)
    return {asset: round(_float(mapping.get(asset)), GRID_ROUND_DIGITS) for asset in ASSETS}


def _qqq_equiv(weights: Mapping[str, Any]) -> float:
    return _float(weights.get("QQQ")) + 3.0 * _float(weights.get("TQQQ"))


def _risk_bucket(exposure: float) -> str:
    if exposure <= 0.45:
        return "defensive"
    if exposure <= 0.70:
        return "balanced"
    if exposure <= 0.95:
        return "growth"
    return "levered_growth"


def _drawdown_bucket(value: float) -> str:
    loss = abs(value)
    if loss <= 0.08:
        return "low_drawdown"
    if loss <= 0.14:
        return "moderate_drawdown"
    return "high_drawdown"


def _tqqq_bucket(value: float) -> str:
    if value <= 0.0:
        return "no_tqqq"
    if value <= 0.10:
        return "low_tqqq"
    return "diagnostic_tqqq"


def _readiness_next_action(status: str, blockers: Sequence[str]) -> str:
    if status == "ACTION_VALUE_PROBE_APPROVED":
        return "FREEZE_FOR_FIRST_LAYER_ACTION_VALUE_MATRIX"
    if blockers:
        return "KEEP_DIAGNOSTIC_AND_REVIEW_BLOCKERS"
    return "KEEP_DIAGNOSTIC_ONLY"


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML file must be a mapping: {path}")
    return raw


def _records(value: object) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]


def _float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
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
