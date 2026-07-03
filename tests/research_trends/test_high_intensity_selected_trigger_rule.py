from __future__ import annotations

from pathlib import Path

from high_intensity_threshold_selection_fixtures import sample_selected_candidate

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    build_high_intensity_selected_trigger_rule,
    build_high_intensity_trigger_density_guardrail,
)


def test_selected_trigger_rule_contains_expression_and_boundaries(
    tmp_path: Path,
) -> None:
    candidate = sample_selected_candidate()
    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=[{"date": "2023-01-03"}],
    )

    rule = build_high_intensity_selected_trigger_rule(
        selected_candidate=candidate,
        guardrail=guardrail,
        output_path=tmp_path / "selected_rule.json",
    )

    trigger_rule = rule["trigger_rule"]
    assert rule["selected_rule_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert "risk_cap_triggered == true" in trigger_rule["boolean_expression"]
    assert trigger_rule["threshold_value"] == 1.0
    assert "automatic_exposure_cap" in rule["blocked_usage"]
    assert "target_weight_action" in rule["blocked_usage"]
    assert "paper_shadow" in rule["blocked_usage"]
    assert "broker_action" in rule["blocked_usage"]
    assert rule["allowed_usage"] == [
        "research_only_forward_observe",
        "manual_review_context",
    ]
    assert rule["promotion_allowed"] is False
