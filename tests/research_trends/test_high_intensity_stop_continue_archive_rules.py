from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    build_high_intensity_false_warning_missed_stress_framework,
    build_high_intensity_stop_continue_archive_rules,
)


def test_high_intensity_stop_continue_archive_rules_are_complete() -> None:
    framework = build_high_intensity_false_warning_missed_stress_framework()
    rules = build_high_intensity_stop_continue_archive_rules()

    assert rules["minimum_observe_event_count"] == 20
    assert rules["minimum_observe_months"] == 6
    assert rules["minimum_outcome_ready_count"] == 15
    assert rules["continue_conditions"]
    assert rules["archive_conditions"]
    assert rules["refine_threshold_conditions"]
    assert rules["owner_review_conditions"]
    assert "false_warning_rate" in framework["metrics"]
    assert "missed_stress_rate" in framework["metrics"]
    assert "missed_upside_rate" in framework["metrics"]
    assert rules["promotion_allowed"] is False
    assert rules["broker_action"] == "none"
