from __future__ import annotations

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    ALLOWED_PROPOSAL_TYPES,
    build_candidate_confidence_scaling_proposal_matrix,
)


def test_each_candidate_gets_confidence_scaling_proposals() -> None:
    rows = build_candidate_confidence_scaling_proposal_matrix(
        [
            {
                "candidate_id": "baseline_plus_trend_structure",
                "dominant_failure_mode": "INSUFFICIENT_HIGH_CONVICTION_RULE",
            },
            {
                "candidate_id": "risk_appetite",
                "dominant_failure_mode": "INSUFFICIENT_HIGH_CONVICTION_RULE",
            },
            {
                "candidate_id": "volatility_regime",
                "dominant_failure_mode": "OVER_CONSERVATIVE_CONFIDENCE_CAP",
            },
        ]
    )

    counts = {
        candidate_id: 0
        for candidate_id in ("baseline_plus_trend_structure", "risk_appetite", "volatility_regime")
    }
    for row in rows:
        counts[row["candidate_id"]] += 1
        assert row["proposal_type"] in ALLOWED_PROPOSAL_TYPES
        assert row["promotion_allowed"] is False
        assert row["paper_shadow_allowed"] is False
        assert row["production_allowed"] is False
        assert row["broker_action"] == "none"
        assert row["expected_effect"]
        assert row["risk_control_note"]

    assert all(value >= 1 for value in counts.values())
