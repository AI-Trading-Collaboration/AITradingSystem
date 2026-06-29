from __future__ import annotations

from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    build_candidate_horizon_asset_drilldown,
)


def _row(
    *,
    candidate: str = "baseline_plus_trend_structure",
    asset: str = "QQQ",
    horizon: str = "5d",
    direction: str = "risk_on",
    score: float = 1.0,
    error_type: str = "no_error",
) -> dict[str, object]:
    return {
        "candidate_id": candidate,
        "target_asset": asset,
        "horizon": horizon,
        "signal_direction": direction,
        "signal_confidence": 1.0,
        "validation_eligible": True,
        "alignment_score": score,
        "error_type": error_type,
        "actual_forward_return": 0.02 if score > 0 else -0.03,
        "actual_max_drawdown": -0.01 if score > 0 else -0.05,
        "actual_realized_volatility": 0.2,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_horizon_asset_drilldown_labels_local_edges_and_mixed_horizon() -> None:
    rows = [
        _row(score=1.0),
        _row(score=1.0),
        _row(horizon="10d", score=-1.0, error_type="false_risk_on"),
        _row(horizon="10d", score=-1.0, error_type="false_risk_on"),
        _row(candidate="risk_appetite", score=1.0),
    ]

    matrix = build_candidate_horizon_asset_drilldown(rows, minimum_local_sample=2)
    labels = {(row["candidate_id"], row["horizon"]): row for row in matrix}

    assert labels[("baseline_plus_trend_structure", "5d")]["diagnostic_label"] == "LOCAL_WEAK_EDGE"
    assert (
        labels[("baseline_plus_trend_structure", "10d")]["diagnostic_label"]
        == "LOCAL_NEGATIVE_EDGE"
    )
    assert (
        "MIXED_BY_HORIZON" in labels[("baseline_plus_trend_structure", "5d")]["diagnostic_labels"]
    )
    assert labels[("risk_appetite", "5d")]["diagnostic_label"] == "INSUFFICIENT_LOCAL_SAMPLE"
