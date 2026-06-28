from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_POLICY_SCHEMA_PATH = (
    PROJECT_ROOT / "config" / "research" / "base_overlay_veto_policy_schema.yaml"
)
DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_signal_usage_matrix_v2.yaml"
)

RAW_INDICATOR_INPUTS = {
    "QQQ_momentum",
    "VIX",
    "SMH_relative_strength",
    "rates",
    "AI_trend_score",
    "breadth",
}


def load_base_overlay_veto_policy(
    path: Path = DEFAULT_POLICY_SCHEMA_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"Policy schema must be a mapping: {path}")
    return dict(raw)


def load_signal_usage_matrix_v2(
    path: Path = DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"Signal usage matrix must be a mapping: {path}")
    return dict(raw)


def compile_two_layer_policy(
    policy: Mapping[str, Any],
    signal_state: Mapping[str, Any],
    usage_matrix: Mapping[str, Any] | None = None,
    *,
    current_weights: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Compile base + overlay + veto policy into research-only target weights."""

    _reject_raw_indicator_inputs(policy, signal_state)
    usage_by_signal = _usage_by_signal(usage_matrix or {})
    weights = _base_weights(policy)
    trace: list[dict[str, Any]] = [{"event": "base_portfolio_loaded", "weights": dict(weights)}]
    blocked_actions: list[dict[str, str]] = []
    applied_overlays: list[str] = []

    veto = _active_veto(signal_state)
    if veto["active"]:
        trace.append({"event": "risk_veto_active", "reasons": veto["reasons"]})

    defensive_overlay = _mapping(policy.get("defensive_overlay_delta"))
    if _is_active(signal_state.get("risk_off")):
        if _signal_allows_usage("risk_off", "defensive_overlay", usage_by_signal):
            _apply_delta(weights, _float_mapping(defensive_overlay.get("risk_off")))
            applied_overlays.append("defensive_overlay:risk_off")
            trace.append({"event": "overlay_applied", "overlay": "risk_off"})
        else:
            blocked_actions.append(_blocked("risk_off", "defensive_overlay", "usage_contract"))
    elif _is_active(signal_state.get("defensive_hold")):
        if _signal_allows_usage("defensive_hold", "defensive_overlay", usage_by_signal):
            _apply_delta(weights, _float_mapping(defensive_overlay.get("defensive_hold")))
            applied_overlays.append("defensive_overlay:defensive_hold")
            trace.append({"event": "overlay_applied", "overlay": "defensive_hold"})
        else:
            blocked_actions.append(
                _blocked("defensive_hold", "defensive_overlay", "usage_contract")
            )

    growth_overlay = _mapping(policy.get("growth_overlay_delta"))
    for signal_name, raw_delta in growth_overlay.items():
        if not _is_active(signal_state.get(signal_name)):
            continue
        if veto["active"]:
            blocked_actions.append(_blocked(str(signal_name), "growth_overlay", "risk_veto"))
            continue
        if not _signal_allows_usage(str(signal_name), "growth_overlay", usage_by_signal):
            blocked_actions.append(_blocked(str(signal_name), "growth_overlay", "usage_contract"))
            continue
        delta = _float_mapping(raw_delta)
        if _has_positive_tqqq_delta(delta) and not _is_active(
            signal_state.get("tqqq_allowed", True)
        ):
            blocked_actions.append(_blocked(str(signal_name), "TQQQ_delta", "tqqq_veto"))
            delta = {asset: (0.0 if asset == "TQQQ" else value) for asset, value in delta.items()}
        _apply_delta(weights, delta)
        applied_overlays.append(f"growth_overlay:{signal_name}")
        trace.append({"event": "overlay_applied", "overlay": str(signal_name)})

    cap_trace = _apply_caps(weights, _mapping(policy.get("caps")))
    trace.extend(cap_trace)
    normalized_weights, normalize_trace = _normalize_long_only(weights)
    trace.extend(normalize_trace)

    turnover = None
    if current_weights is not None:
        turnover = _turnover(normalized_weights, _float_mapping(current_weights))
        cap = _optional_float(_mapping(policy.get("caps")).get("turnover_max"))
        if cap is not None and turnover > cap:
            blocked_actions.append(_blocked("target_weights", "turnover", "turnover_cap"))
            trace.append({"event": "turnover_cap_exceeded", "turnover": turnover, "cap": cap})

    return {
        "status": "TWO_LAYER_POLICY_COMPILED_RESEARCH_ONLY",
        "target_weights": normalized_weights,
        "audit_trace": trace,
        "applied_overlays": applied_overlays,
        "blocked_actions": blocked_actions,
        "veto_active": veto["active"],
        "veto_reasons": veto["reasons"],
        "turnover": turnover,
        "long_only": all(value >= -1e-12 for value in normalized_weights.values()),
        "sum_to_one": abs(sum(normalized_weights.values()) - 1.0) <= 1e-9,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _reject_raw_indicator_inputs(
    policy: Mapping[str, Any],
    signal_state: Mapping[str, Any],
) -> None:
    input_contract = _mapping(policy.get("second_layer_input_contract"))
    if input_contract.get("raw_indicators_allowed") is True:
        raise ValueError("second layer policy must not allow raw indicator inputs")
    explicit_raw = set(_string_list(policy.get("raw_indicator_inputs")))
    raw_keys = RAW_INDICATOR_INPUTS & set(signal_state)
    if explicit_raw or raw_keys:
        blocked = sorted(explicit_raw | raw_keys)
        raise ValueError(f"second layer cannot use raw indicators: {blocked}")


def _usage_by_signal(matrix: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    signals = matrix.get("signals")
    if not isinstance(signals, list):
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for signal in signals:
        if isinstance(signal, Mapping):
            name = str(signal.get("signal_name", ""))
            if name:
                rows[name] = dict(signal)
    return rows


def _signal_allows_usage(
    signal_name: str,
    usage: str,
    usage_by_signal: Mapping[str, Mapping[str, Any]],
) -> bool:
    contract = usage_by_signal.get(signal_name)
    if not contract:
        return True
    if contract.get("diagnostic_only") is True and usage in {"growth_overlay", "allocation"}:
        return False
    if usage in _string_list(contract.get("blocked_usage")):
        return False
    allowed = _string_list(contract.get("allowed_usage"))
    return not allowed or usage in allowed


def _active_veto(signal_state: Mapping[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    for key in (
        "risk_off_veto",
        "volatility_veto",
        "event_risk_veto",
        "trend_break_veto",
        "tqqq_veto",
    ):
        if _is_active(signal_state.get(key)):
            reasons.append(key)
    if _is_active(signal_state.get("risk_off")):
        reasons.append("risk_off_veto")
    if signal_state.get("growth_allowed") is False:
        reasons.append("growth_allowed_false")
    if signal_state.get("add_risk_allowed") is False:
        reasons.append("add_risk_allowed_false")
    return {"active": bool(reasons), "reasons": sorted(set(reasons))}


def _base_weights(policy: Mapping[str, Any]) -> dict[str, float]:
    base = _mapping(policy.get("base_portfolio"))
    if "weights" in base:
        base = _mapping(base.get("weights"))
    weights = _float_mapping(base)
    if not weights:
        raise ValueError("base_portfolio weights are required")
    return weights


def _apply_delta(weights: dict[str, float], delta: Mapping[str, float]) -> None:
    for asset, value in delta.items():
        weights[asset] = weights.get(asset, 0.0) + float(value)


def _apply_caps(weights: dict[str, float], caps: Mapping[str, Any]) -> list[dict[str, Any]]:
    trace: list[dict[str, Any]] = []
    tqqq_cap = _optional_float(caps.get("TQQQ_max_weight"))
    if tqqq_cap is not None and weights.get("TQQQ", 0.0) > tqqq_cap:
        excess = weights["TQQQ"] - tqqq_cap
        weights["TQQQ"] = tqqq_cap
        weights["SGOV"] = weights.get("SGOV", 0.0) + excess
        trace.append({"event": "tqqq_cap_applied", "cap": tqqq_cap, "excess_to": "SGOV"})

    qqq_equiv_cap = _optional_float(caps.get("QQQ_equivalent_exposure_max"))
    if qqq_equiv_cap is not None:
        exposure = weights.get("QQQ", 0.0) + 3.0 * weights.get("TQQQ", 0.0)
        if exposure > qqq_equiv_cap:
            excess = exposure - qqq_equiv_cap
            tqqq_reduction = min(weights.get("TQQQ", 0.0), excess / 3.0)
            if tqqq_reduction:
                weights["TQQQ"] = weights.get("TQQQ", 0.0) - tqqq_reduction
                weights["SGOV"] = weights.get("SGOV", 0.0) + tqqq_reduction
                excess -= 3.0 * tqqq_reduction
            if excess > 1e-12:
                qqq_reduction = min(weights.get("QQQ", 0.0), excess)
                weights["QQQ"] = weights.get("QQQ", 0.0) - qqq_reduction
                weights["SGOV"] = weights.get("SGOV", 0.0) + qqq_reduction
            trace.append(
                {
                    "event": "qqq_equivalent_cap_applied",
                    "cap": qqq_equiv_cap,
                    "post_exposure": weights.get("QQQ", 0.0) + 3.0 * weights.get("TQQQ", 0.0),
                }
            )
    return trace


def _normalize_long_only(
    weights: Mapping[str, float],
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    if any(value < -1e-12 for value in weights.values()):
        raise ValueError(f"compiled weights are not long-only: {weights}")
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("compiled weights must have positive total weight")
    normalized = {asset: max(0.0, value) / total for asset, value in weights.items()}
    trace = []
    if abs(total - 1.0) > 1e-9:
        trace.append({"event": "weights_normalized", "pre_normalization_sum": total})
    return normalized, trace


def _turnover(target: Mapping[str, float], current: Mapping[str, float]) -> float:
    assets = set(target) | set(current)
    return 0.5 * sum(abs(target.get(asset, 0.0) - current.get(asset, 0.0)) for asset in assets)


def _blocked(signal_name: str, attempted_usage: str, reason: str) -> dict[str, str]:
    return {
        "signal_name": signal_name,
        "attempted_usage": attempted_usage,
        "reason": reason,
    }


def _is_active(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    return bool(value)


def _has_positive_tqqq_delta(delta: Mapping[str, float]) -> bool:
    return delta.get("TQQQ", 0.0) > 0


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _float_mapping(value: object) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): float(raw) for key, raw in value.items()}


def _string_list(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
