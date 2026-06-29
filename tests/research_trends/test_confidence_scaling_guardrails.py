from __future__ import annotations

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    build_candidate_guardrail_matrix,
)


def test_guardrail_matrix_keeps_research_boundary() -> None:
    rows = build_candidate_guardrail_matrix(
        [
            {
                "candidate_id": "volatility_regime",
                "proposal_id": "volatility_regime_confidence_cap_relaxation",
                "proposal_type": "CONFIDENCE_CAP_RELAXATION",
            }
        ]
    )

    assert rows
    row = rows[0]
    assert 0.0 <= row["max_high_confidence_ratio"] <= 1.0
    assert row["max_false_risk_on_cost_increase"] == 0.0
    assert row["max_false_risk_off_cost_increase"] == 0.0
    assert row["promotion_allowed"] is False
    assert row["paper_shadow_allowed"] is False
    assert row["production_allowed"] is False
    assert row["broker_action"] == "none"
