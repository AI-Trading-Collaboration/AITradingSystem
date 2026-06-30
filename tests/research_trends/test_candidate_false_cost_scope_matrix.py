from __future__ import annotations

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    build_candidate_false_cost_scope_matrix,
    classify_false_cost_label,
)


def test_false_risk_on_cost_too_high_label() -> None:
    assert (
        classify_false_cost_label(
            eligible_record_count=100,
            false_risk_on_cost=6.0,
            false_risk_off_cost=1.0,
        )
        == "FALSE_RISK_ON_COST_TOO_HIGH"
    )


def test_false_risk_off_cost_too_high_label() -> None:
    assert (
        classify_false_cost_label(
            eligible_record_count=100,
            false_risk_on_cost=1.0,
            false_risk_off_cost=6.0,
        )
        == "FALSE_RISK_OFF_COST_TOO_HIGH"
    )


def test_both_false_costs_too_high_label() -> None:
    assert (
        classify_false_cost_label(
            eligible_record_count=100,
            false_risk_on_cost=6.0,
            false_risk_off_cost=6.0,
        )
        == "BOTH_FALSE_COSTS_TOO_HIGH"
    )


def test_false_cost_acceptable_label() -> None:
    assert (
        classify_false_cost_label(
            eligible_record_count=100,
            false_risk_on_cost=1.0,
            false_risk_off_cost=1.0,
        )
        == "FALSE_COST_ACCEPTABLE"
    )


def test_false_cost_recommended_usage_for_volatility_direction() -> None:
    rows = build_candidate_false_cost_scope_matrix(
        asset_rows=[],
        horizon_rows=[],
        direction_rows=[
            {
                "refined_candidate_id": "volatility_regime_refined_confidence_v1",
                "original_candidate_id": "volatility_regime",
                "signal_direction": "risk_off",
                "eligible_record_count": 1000,
                "false_risk_on_cost": 1.0,
                "false_risk_off_cost": 1.0,
                "confidence_weighted_score": 0.1,
            }
        ],
        high_conviction_rows=[],
        regime_rows=[],
    )

    assert rows[0]["false_cost_label"] == "FALSE_COST_ACCEPTABLE"
    assert rows[0]["recommended_usage"] == "risk_cap_only"
