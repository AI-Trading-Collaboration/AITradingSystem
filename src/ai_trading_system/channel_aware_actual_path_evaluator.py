from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def evaluate_channel_aware_actual_path(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    observations = [dict(row) for row in rows]
    defensive_rows = [row for row in observations if row.get("channel") == "defensive"]
    return_rows = [
        row for row in observations if row.get("channel") == "return_seeking_diagnostic"
    ]
    veto_rows = [row for row in observations if row.get("channel") == "risk_veto"]
    second_layer_rows = [row for row in observations if row.get("channel") == "second_layer_policy"]

    defensive_metrics = {
        "row_count": len(defensive_rows),
        "false_risk_off_cost_delta": _sum(defensive_rows, "false_risk_off_cost_delta"),
        "missed_risk_off_cost_delta": _sum(defensive_rows, "missed_risk_off_cost_delta"),
        "defensive_probe_regression_count": int(
            _sum(defensive_rows, "defensive_probe_regression_count")
        ),
    }
    return_metrics = {
        "row_count": len(return_rows),
        "captured_upside": _sum(return_rows, "captured_upside"),
        "false_add_risk_cost": _sum(return_rows, "false_add_risk_cost"),
        "diagnostic_only": all(row.get("diagnostic_only") is True for row in return_rows),
    }
    veto_metrics = {
        "row_count": len(veto_rows),
        "blocked_false_add_risk_count": int(_sum(veto_rows, "blocked_false_add_risk_count")),
        "missed_verified_add_risk_count": int(
            _sum(veto_rows, "missed_verified_add_risk_count")
        ),
    }
    second_layer_metrics = {
        "row_count": len(second_layer_rows),
        "actual_path_return_delta": _sum(second_layer_rows, "actual_path_return_delta"),
        "max_drawdown_delta": _sum(second_layer_rows, "max_drawdown_delta"),
        "turnover": _sum(second_layer_rows, "turnover"),
    }
    same_risk = {
        "same_risk_static_frontier_delta": _sum(
            observations, "same_risk_static_frontier_delta"
        ),
        "QQQ_equivalent_exposure_delta": _sum(observations, "QQQ_equivalent_exposure_delta"),
        "beta_only_dependency_flag": any(
            bool(row.get("TQQQ_beta_dependency")) or bool(row.get("2023_plus_dependency"))
            for row in observations
        ),
    }
    return {
        "status": "CHANNEL_AWARE_ACTUAL_PATH_EVALUATION_READY",
        "defensive_channel_metrics": defensive_metrics,
        "return_seeking_diagnostic_metrics": return_metrics,
        "risk_veto_metrics": veto_metrics,
        "second_layer_actual_path_metrics": second_layer_metrics,
        "same_risk_frontier_comparison": same_risk,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sum(rows: Iterable[Mapping[str, Any]], key: str) -> float:
    total = 0.0
    for row in rows:
        try:
            total += float(row.get(key, 0.0))
        except (TypeError, ValueError):
            continue
    return total
