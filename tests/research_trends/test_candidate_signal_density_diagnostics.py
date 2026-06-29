from __future__ import annotations

from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    build_candidate_signal_density_matrix,
)


def _row(direction: str, confidence: float) -> dict[str, object]:
    return {
        "candidate_id": "risk_appetite",
        "target_asset": "QQQ",
        "horizon": "5d",
        "signal_direction": direction,
        "signal_confidence": confidence,
        "validation_eligible": True,
        "alignment_score": 0.0,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_signal_density_ratios_and_labels() -> None:
    rows = [_row("neutral", 0.20) for _ in range(8)] + [
        _row("risk_on", 0.80),
        _row("risk_off", 0.40),
    ]

    matrix = build_candidate_signal_density_matrix(rows)

    assert len(matrix) == 1
    row = matrix[0]
    assert row["neutral_ratio"] == 0.8
    assert row["directional_signal_ratio"] == 0.2
    assert row["high_confidence_ratio"] == 0.1
    assert row["diagnostic_label"] == "POSSIBLE_OVER_NEUTRALIZATION"
    assert "LOW_CONVICTION_SIGNAL_DESIGN" in row["diagnostic_labels"]
