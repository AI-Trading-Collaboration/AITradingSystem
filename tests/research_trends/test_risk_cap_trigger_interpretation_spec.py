from __future__ import annotations

from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    build_risk_cap_trigger_interpretation_spec,
)


def test_trigger_interpretation_defines_risk_cap_trigger_conditions() -> None:
    spec = build_risk_cap_trigger_interpretation_spec()

    true_when = spec["risk_cap_triggered"]["true_when"]
    assert "scope_active == true" in true_when
    assert "usage_role == risk_cap_only" in true_when
    assert "signal_direction in [risk_off, trend_weakening, volatility_expansion]" in true_when


def test_trigger_interpretation_defines_severity_bands() -> None:
    spec = build_risk_cap_trigger_interpretation_spec()

    assert {"low", "medium", "high"} <= set(spec["trigger_severity"])
    assert {"risk_cap_low", "risk_cap_medium", "risk_cap_high"} <= set(
        spec["trigger_interpretation"]
    )


def test_trigger_interpretation_is_not_sell_or_broker_signal() -> None:
    spec = build_risk_cap_trigger_interpretation_spec()

    boundary = set(spec["interpretation_boundary"])
    assert "risk_cap_trigger_is_not_sell_signal" in boundary
    assert "risk_cap_trigger_is_not_broker_action" in boundary
    assert spec["broker_action"] == "none"


def test_risk_on_and_neutral_are_not_risk_cap_trigger_directions() -> None:
    spec = build_risk_cap_trigger_interpretation_spec()

    false_directions = set(spec["risk_cap_triggered"]["false_when_signal_direction_in"])
    assert "risk_on" in false_directions
    assert "neutral" in false_directions
