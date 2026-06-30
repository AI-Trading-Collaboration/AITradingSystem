from __future__ import annotations

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    build_candidate_direction_scope_matrix,
    build_candidate_high_conviction_scope_matrix,
)


def _direction_rows(
    *,
    candidate_id: str = "generic_refined_confidence_v1",
    original_id: str = "generic",
    direction: str = "risk_on",
    score: float = 1.0,
    high: bool = True,
    error_type: str = "none",
    count: int = 1000,
):
    return [
        {
            "refined_candidate_id": candidate_id,
            "original_candidate_id": original_id,
            "target_asset": "QQQ",
            "horizon": "5d",
            "signal_direction": direction,
            "validation_eligible": True,
            "alignment_score": score,
            "refined_signal_confidence": 0.8,
            "high_conviction_flag": high,
            "error_type": error_type,
            "actual_forward_return": -0.06 if error_type == "false_risk_on" else 0.02,
            "actual_max_drawdown": -0.06 if error_type == "false_risk_on" else -0.01,
        }
        for _ in range(count)
    ]


def test_direction_keep_label() -> None:
    rows = build_candidate_direction_scope_matrix(_direction_rows())

    assert rows[0]["direction_scope_label"] == "DIRECTION_KEEP"


def test_volatility_risk_off_direction_is_risk_cap_only() -> None:
    rows = build_candidate_direction_scope_matrix(
        _direction_rows(
            candidate_id="volatility_regime_refined_confidence_v1",
            original_id="volatility_regime",
            direction="risk_off",
        )
    )

    assert rows[0]["direction_scope_label"] == "DIRECTION_RISK_CAP_ONLY"


def test_baseline_trend_direction_is_confirmation_only() -> None:
    rows = build_candidate_direction_scope_matrix(
        _direction_rows(
            candidate_id="baseline_plus_trend_structure_refined_confidence_v1",
            original_id="baseline_plus_trend_structure",
            direction="trend_confirming",
        )
    )

    assert rows[0]["direction_scope_label"] == "DIRECTION_CONFIRMATION_ONLY"


def test_high_conviction_only_scope_label() -> None:
    rows = build_candidate_high_conviction_scope_matrix(
        _direction_rows(high=True, score=1.0) + _direction_rows(high=False, score=-1.0)
    )

    assert rows[0]["high_conviction_scope_label"] == "HIGH_CONVICTION_SCOPE_KEEP_ONLY"


def test_high_conviction_false_cost_blocked_label() -> None:
    rows = build_candidate_high_conviction_scope_matrix(
        _direction_rows(high=True, score=-1.0, error_type="false_risk_on")
    )

    assert rows[0]["high_conviction_scope_label"] == "HIGH_CONVICTION_FALSE_COST_BLOCKED"
