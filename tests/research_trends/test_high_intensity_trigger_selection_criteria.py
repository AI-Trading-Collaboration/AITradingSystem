from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    build_high_intensity_trigger_selection_criteria,
)


def test_high_intensity_trigger_selection_criteria_is_warning_only() -> None:
    criteria = build_high_intensity_trigger_selection_criteria(
        alignment_rows=[
            {
                "risk_cap_triggered": True,
                "risk_cap_score": 0.9,
                "scope_active": True,
                "signal_direction": "portfolio_level_risk_cap",
            }
        ],
        diagnostics={
            "summary": {
                "overall_recommendation": "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
            }
        },
        data_quality_status="PASS_WITH_WARNINGS",
    )

    assert criteria["selected_usage"] == "high_intensity_forward_observe"
    assert "automatic_exposure_cap" in criteria["blocked_usage"]
    assert "target_weight_action" in criteria["blocked_usage"]
    assert criteria["medium_low_policy"]["low_intensity"] == "record_only"
    assert criteria["medium_low_policy"]["medium_intensity"] == "record_only"
    assert criteria["medium_low_policy"]["high_intensity"] == (
        "forward_observe_event_candidate"
    )
    assert criteria["promotion_allowed"] is False
    assert criteria["broker_action"] == "none"
