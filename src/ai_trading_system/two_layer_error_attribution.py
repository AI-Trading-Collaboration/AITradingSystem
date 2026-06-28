from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

ERROR_TYPES = (
    "false_risk_off",
    "missed_risk_off",
    "false_add_risk",
    "late_re_risk",
    "beta_only_improvement",
)


def attribute_two_layer_errors(observation: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []

    if _flag(observation, "risk_off_signal") and (
        _positive(observation, "missed_upside")
        or _positive(observation, "false_risk_off_cost")
        or _positive(observation, "defensive_switch_regret")
    ):
        errors.append(
            _error(
                "false_risk_off",
                {
                    "missed_upside": _num(observation, "missed_upside"),
                    "false_risk_off_cost": _num(observation, "false_risk_off_cost"),
                    "defensive_switch_regret": _num(observation, "defensive_switch_regret"),
                },
            )
        )

    if not _flag(observation, "risk_off_signal") and (
        _positive(observation, "avoidable_drawdown")
        or _positive(observation, "late_risk_off_cost")
        or _positive(observation, "worst_5d_loss_regression")
    ):
        errors.append(
            _error(
                "missed_risk_off",
                {
                    "avoidable_drawdown": _num(observation, "avoidable_drawdown"),
                    "worst_5d_loss_regression": _num(observation, "worst_5d_loss_regression"),
                    "late_risk_off_cost": _num(observation, "late_risk_off_cost"),
                },
            )
        )

    if _flag(observation, "add_risk_signal") and (
        _positive(observation, "false_add_risk_cost")
        or _positive(observation, "defensive_probe_regression_count")
        or _positive(observation, "drawdown_regression")
        or _positive(observation, "risk_on_false_positive")
    ):
        errors.append(
            _error(
                "false_add_risk",
                {
                    "false_add_risk_cost": _num(observation, "false_add_risk_cost"),
                    "defensive_probe_regression_count": _num(
                        observation, "defensive_probe_regression_count"
                    ),
                    "drawdown_regression": _num(observation, "drawdown_regression"),
                    "risk_on_false_positive": _num(observation, "risk_on_false_positive"),
                },
            )
        )

    if _positive(observation, "re_risk_delay") and (
        _positive(observation, "missed_recovery_upside")
        or _positive(observation, "post_drawdown_recovery_gap")
    ):
        errors.append(
            _error(
                "late_re_risk",
                {
                    "re_risk_delay": _num(observation, "re_risk_delay"),
                    "missed_recovery_upside": _num(observation, "missed_recovery_upside"),
                    "post_drawdown_recovery_gap": _num(
                        observation, "post_drawdown_recovery_gap"
                    ),
                },
            )
        )

    beta_dependency = (
        _positive(observation, "QQQ_equivalent_exposure_delta")
        or _flag(observation, "TQQQ_beta_dependency")
        or _flag(observation, "2023_plus_dependency")
    )
    same_risk_delta = _num(observation, "same_risk_static_frontier_delta")
    if beta_dependency and same_risk_delta <= 0:
        errors.append(
            _error(
                "beta_only_improvement",
                {
                    "same_risk_static_frontier_delta": same_risk_delta,
                    "QQQ_equivalent_exposure_delta": _num(
                        observation, "QQQ_equivalent_exposure_delta"
                    ),
                    "TQQQ_beta_dependency": _flag(observation, "TQQQ_beta_dependency"),
                    "2023_plus_dependency": _flag(observation, "2023_plus_dependency"),
                },
            )
        )

    return {
        "status": "TWO_LAYER_ERROR_ATTRIBUTION_READY",
        "error_types": [row["error_type"] for row in errors],
        "attribution_rows": errors,
        "known_error_types": list(ERROR_TYPES),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def summarize_two_layer_errors(observations: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    counts = dict.fromkeys(ERROR_TYPES, 0)
    rows = []
    for observation in observations:
        attribution = attribute_two_layer_errors(observation)
        rows.extend(attribution["attribution_rows"])
        for error_type in attribution["error_types"]:
            counts[str(error_type)] = counts.get(str(error_type), 0) + 1
    return {
        "status": "TWO_LAYER_ERROR_ATTRIBUTION_SUMMARY_READY",
        "error_type_counts": counts,
        "attribution_rows": rows,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _error(error_type: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {"error_type": error_type, "metrics": dict(metrics)}


def _positive(row: Mapping[str, Any], key: str) -> bool:
    return _num(row, key) > 0


def _num(row: Mapping[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0))
    except (TypeError, ValueError):
        return 0.0


def _flag(row: Mapping[str, Any], key: str) -> bool:
    return bool(row.get(key, False))
