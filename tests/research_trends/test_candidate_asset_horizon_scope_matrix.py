from __future__ import annotations

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    build_candidate_asset_scope_matrix,
    build_candidate_horizon_scope_matrix,
)


def _rows(*, score: float, count: int = 1000, error_type: str = "none"):
    return [
        {
            "refined_candidate_id": "baseline_plus_trend_structure_refined_confidence_v1",
            "original_candidate_id": "baseline_plus_trend_structure",
            "target_asset": "QQQ",
            "horizon": "5d",
            "signal_direction": "trend_confirming",
            "validation_eligible": True,
            "alignment_score": score,
            "refined_signal_confidence": 0.8,
            "high_conviction_flag": True,
            "error_type": error_type,
            "actual_forward_return": 0.04 if error_type == "false_risk_off" else -0.04,
            "actual_max_drawdown": -0.04,
        }
        for _ in range(count)
    ]


def test_asset_scope_keep_label() -> None:
    rows = build_candidate_asset_scope_matrix(_rows(score=1.0))

    assert rows[0]["scope_label"] == "ASSET_SCOPE_KEEP"


def test_asset_scope_drop_label() -> None:
    rows = build_candidate_asset_scope_matrix(_rows(score=-1.0))

    assert rows[0]["scope_label"] == "ASSET_SCOPE_DROP"


def test_horizon_scope_inconclusive_for_small_sample() -> None:
    rows = build_candidate_horizon_scope_matrix(_rows(score=1.0, count=10))

    assert rows[0]["scope_label"] == "HORIZON_SCOPE_INCONCLUSIVE"


def test_horizon_scope_false_cost_blocked() -> None:
    rows = build_candidate_horizon_scope_matrix(_rows(score=-1.0, error_type="false_risk_on"))

    assert rows[0]["scope_label"] == "HORIZON_SCOPE_FALSE_COST_BLOCKED"
