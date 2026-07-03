from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    build_high_intensity_trigger_selection_criteria,
    build_high_intensity_trigger_threshold_candidate_matrix,
)


def _rows() -> list[dict[str, object]]:
    return [
        {
            "risk_cap_triggered": True,
            "risk_cap_score": 0.5 + index * 0.02,
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
        }
        for index in range(30)
    ]


def test_high_intensity_threshold_candidate_matrix_statuses() -> None:
    diagnostics = {
        "summary": {"overbinding_label": "OVERBINDING_BLOCKING"},
        "false_cost_missed_upside": {
            "false_cost_label": "FALSE_COST_BLOCKING",
            "false_risk_cap_cost_proxy": 0.7,
        },
        "downside": {
            "downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
            "downside_protection_proxy": 0.5,
        },
    }
    criteria = build_high_intensity_trigger_selection_criteria(
        alignment_rows=_rows(),
        diagnostics=diagnostics,
        data_quality_status="PASS_WITH_WARNINGS",
    )
    matrix = build_high_intensity_trigger_threshold_candidate_matrix(
        alignment_rows=_rows(),
        diagnostics=diagnostics,
        criteria=criteria,
        data_quality_status="PASS_WITH_WARNINGS",
    )
    by_id = {row["threshold_id"]: row for row in matrix}

    assert set(by_id) == {
        "P90_RISK_CAP_SCORE",
        "P95_RISK_CAP_SCORE",
        "COMPOSITE_HIGH_INTENSITY_RULE",
    }
    assert by_id["P90_RISK_CAP_SCORE"]["recommended_status"] == (
        "TOO_BROAD_OVERBINDING_RISK"
    )
    assert by_id["P95_RISK_CAP_SCORE"]["recommended_status"] == (
        "TOO_NARROW_MISSED_STRESS_RISK"
    )
    assert by_id["COMPOSITE_HIGH_INTENSITY_RULE"]["recommended_status"] == (
        "CANDIDATE_FOR_2335_SELECTION"
    )
    assert all(row["promotion_allowed"] is False for row in matrix)
    assert all(row["broker_action"] == "none" for row in matrix)
