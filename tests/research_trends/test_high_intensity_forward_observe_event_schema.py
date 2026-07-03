from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    build_high_intensity_forward_observe_event_schema,
)


def test_high_intensity_event_schema_has_required_fields_and_blocked_usage() -> None:
    schema = build_high_intensity_forward_observe_event_schema(
        timestamp_context={
            "known_at": {"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
            "latency_policy": {"decision_delay": "NEXT_TRADING_DAY_DECISION"},
        }
    )

    assert schema["event_id"]["required"] is True
    assert schema["event_date"]["required"] is True
    assert schema["target_asset"]["required"] is True
    assert schema["known_at_policy"] == "NEXT_SESSION_DECISION_POLICY"
    assert schema["latency_policy"] == "NEXT_TRADING_DAY_DECISION"
    assert schema["pit_policy"] == "PIT_APPROXIMATION_READY"
    assert schema["manual_review_observation_flag"]["required"] is True
    assert "paper_shadow" in schema["blocked_usage"]
    assert "production" in schema["blocked_usage"]
    assert "broker_action" in schema["blocked_usage"]
    assert schema["event_status"] == "OBSERVE_PENDING"
    assert schema["broker_action"] == "none"
