from __future__ import annotations

from high_intensity_threshold_selection_fixtures import sample_candidate_rows

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    build_high_intensity_threshold_candidate_scoring_matrix,
    select_high_intensity_threshold_candidate,
)


def test_threshold_candidate_scoring_labels_p90_p95_and_composite() -> None:
    rows = build_high_intensity_threshold_candidate_scoring_matrix(
        candidate_rows=sample_candidate_rows(),
        diagnostics={
            "false_cost_missed_upside": {
                "false_cost_label": "FALSE_COST_BLOCKING",
                "missed_upside_label": "MISSED_UPSIDE_BLOCKING",
            },
            "downside": {
                "downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
            },
        },
    )

    labels = {row["threshold_id"]: row["selection_label"] for row in rows}
    assert labels["P90_RISK_CAP_SCORE"] == "TOO_BROAD_OVERBINDING_RISK"
    assert labels["P95_RISK_CAP_SCORE"] == "TOO_NARROW_MISSED_STRESS_RISK"
    assert labels["COMPOSITE_HIGH_INTENSITY_RULE"] == "SELECTED"
    assert sum(row["selection_label"] == "SELECTED" for row in rows) == 1


def test_threshold_candidate_scoring_selects_composite_candidate() -> None:
    rows = build_high_intensity_threshold_candidate_scoring_matrix(
        candidate_rows=sample_candidate_rows(),
        diagnostics={},
    )

    selected = select_high_intensity_threshold_candidate(rows)

    assert selected["threshold_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert selected["selection_score"] > 0
    assert selected["promotion_allowed"] is False
    assert selected["broker_action"] == "none"
