from __future__ import annotations

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    build_candidate_confidence_distribution_retargeting_matrix,
)


def test_distribution_retargeting_sets_high_confidence_target() -> None:
    rows = build_candidate_confidence_distribution_retargeting_matrix(
        [
            {
                "candidate_id": "risk_appetite",
                "high_confidence_ratio": 0.0,
                "low_confidence_ratio": 0.2,
                "neutral_ratio": 0.4,
                "directional_signal_ratio": 0.6,
                "dominant_failure_mode": "INSUFFICIENT_HIGH_CONVICTION_RULE",
            }
        ]
    )

    assert rows[0]["target_high_confidence_ratio_min"] == 0.1
    assert rows[0]["target_high_confidence_ratio_max"] <= 0.35


def test_distribution_retargeting_sets_neutral_and_directional_targets() -> None:
    row = build_candidate_confidence_distribution_retargeting_matrix(
        [
            {
                "candidate_id": "baseline_plus_trend_structure",
                "high_confidence_ratio": 0.0,
                "low_confidence_ratio": 0.1,
                "neutral_ratio": 0.8,
                "directional_signal_ratio": 0.2,
                "dominant_failure_mode": "NEUTRAL_BAND_TOO_WIDE",
            }
        ]
    )[0]

    assert row["target_neutral_ratio_max"] <= 0.6
    assert row["target_directional_signal_ratio_min"] >= 0.25


def test_distribution_retargeting_does_not_mark_promotion_ready() -> None:
    row = build_candidate_confidence_distribution_retargeting_matrix(
        [
            {
                "candidate_id": "volatility_regime",
                "high_confidence_ratio": 0.0,
                "low_confidence_ratio": 0.1,
                "neutral_ratio": 0.4,
                "directional_signal_ratio": 0.6,
                "dominant_failure_mode": "OVER_CONSERVATIVE_CONFIDENCE_CAP",
            }
        ]
    )[0]

    assert row["promotion_allowed"] is False
    assert row["paper_shadow_allowed"] is False
    assert row["production_allowed"] is False
    assert row["broker_action"] == "none"
