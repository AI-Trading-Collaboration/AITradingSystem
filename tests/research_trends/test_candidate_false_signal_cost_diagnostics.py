from __future__ import annotations

from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    build_candidate_false_signal_cost_matrix,
)


def _row(candidate: str, error_type: str, forward: float, drawdown: float) -> dict[str, object]:
    return {
        "candidate_id": candidate,
        "validation_eligible": True,
        "alignment_score": -1.0,
        "signal_confidence": 0.9,
        "error_type": error_type,
        "actual_forward_return": forward,
        "actual_max_drawdown": drawdown,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_false_signal_cost_matrix_labels_dominant_costs() -> None:
    rows = [
        _row("risk_appetite", "false_risk_off", 0.08, -0.01),
        _row("risk_appetite", "false_risk_off", 0.07, -0.01),
        _row("volatility_regime", "false_risk_on", -0.05, -0.07),
        _row("volatility_regime", "false_risk_on", -0.04, -0.06),
    ]

    matrix = build_candidate_false_signal_cost_matrix(rows)
    by_candidate = {row["candidate_id"]: row for row in matrix}

    assert by_candidate["risk_appetite"]["false_risk_off_cost_total"] == 0.17
    assert by_candidate["risk_appetite"]["diagnostic_label"] == "FALSE_RISK_OFF_COST_TOO_HIGH"
    assert by_candidate["volatility_regime"]["false_risk_on_cost_total"] == 0.22
    assert by_candidate["volatility_regime"]["diagnostic_label"] == "FALSE_RISK_ON_COST_TOO_HIGH"
    assert by_candidate["risk_appetite"]["cost_asymmetry"] > 0
