from __future__ import annotations

from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    build_candidate_signal_overlap_matrix,
)


def _row(
    candidate: str,
    day: str,
    value: float,
    direction: str,
    score: float,
) -> dict[str, object]:
    return {
        "candidate_id": candidate,
        "decision_timestamp": f"2023-01-{day}T00:00:00+00:00",
        "target_asset": "QQQ",
        "horizon": "5d",
        "signal_value": value,
        "signal_confidence": abs(value),
        "signal_direction": direction,
        "alignment_score": score,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_signal_overlap_labels_high_redundancy() -> None:
    rows = [
        _row("a", "03", 0.1, "risk_on", 1.0),
        _row("a", "04", 0.2, "risk_on", 1.0),
        _row("b", "03", 0.2, "risk_on", 1.0),
        _row("b", "04", 0.4, "risk_on", 1.0),
    ]

    matrix = build_candidate_signal_overlap_matrix(rows)

    assert matrix[0]["signal_value_correlation"] == 1.0
    assert matrix[0]["signal_direction_agreement_rate"] == 1.0
    assert matrix[0]["diagnostic_label"] == "HIGHLY_REDUNDANT"


def test_signal_overlap_labels_complementary_and_unstable_disagreement() -> None:
    complementary_rows = [
        _row("a", "03", 1.0, "risk_on", 1.0),
        _row("a", "04", 0.0, "risk_off", 1.0),
        _row("b", "03", 0.0, "risk_off", -1.0),
        _row("b", "04", 1.0, "risk_on", -1.0),
    ]
    unstable_rows = [
        _row("c", "03", 0.1, "risk_on", 0.0),
        _row("c", "04", 0.2, "risk_on", 0.0),
        _row("d", "03", 0.1, "risk_off", 0.0),
        _row("d", "04", 0.2, "risk_off", 0.0),
    ]

    assert (
        build_candidate_signal_overlap_matrix(complementary_rows)[0]["diagnostic_label"]
        == "COMPLEMENTARY"
    )
    assert (
        build_candidate_signal_overlap_matrix(unstable_rows)[0]["diagnostic_label"]
        == "UNSTABLE_DISAGREEMENT"
    )
