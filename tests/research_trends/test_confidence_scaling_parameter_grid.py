from __future__ import annotations

from collections import Counter

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    MAX_PARAMETER_SETS_PER_CANDIDATE,
    build_candidate_confidence_distribution_retargeting_matrix,
    build_candidate_confidence_scaling_parameter_grid,
    build_candidate_confidence_scaling_proposal_matrix,
)


def test_parameter_grid_is_bounded_and_does_not_execute_regeneration() -> None:
    diagnosis = [
        {
            "candidate_id": "risk_appetite",
            "dominant_failure_mode": "INSUFFICIENT_HIGH_CONVICTION_RULE",
            "high_confidence_ratio": 0.0,
            "low_confidence_ratio": 0.0,
            "neutral_ratio": 0.4,
            "directional_signal_ratio": 0.6,
        }
    ]
    proposals = build_candidate_confidence_scaling_proposal_matrix(diagnosis)
    retargeting = build_candidate_confidence_distribution_retargeting_matrix(diagnosis)

    rows = build_candidate_confidence_scaling_parameter_grid(
        proposal_rows=proposals,
        retargeting_rows=retargeting,
    )

    assert rows
    counts = Counter(str(row["candidate_id"]) for row in rows)
    assert counts["risk_appetite"] <= MAX_PARAMETER_SETS_PER_CANDIDATE
    for row in rows:
        assert 0.0 <= row["confidence_cap"] <= 1.0
        assert 0.0 <= row["high_confidence_threshold"] <= 1.0
        assert 0.0 < row["neutral_band_width"] <= 0.25
        assert 0.0 <= row["missing_input_penalty"] <= 1.0
        assert row["regeneration_executed"] is False
        assert row["actual_path_validation_executed"] is False
