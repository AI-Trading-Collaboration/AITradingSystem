from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    build_high_intensity_observe_continuation_scope,
)


def test_observe_continuation_scope_is_observe_only() -> None:
    scope = build_high_intensity_observe_continuation_scope(
        {
            "manual_review_context_allowed": True,
            "runtime_observe_integration_allowed": True,
        }
    )

    assert scope["continuation_mode"] == "observe_only"
    assert scope["manual_review_context_allowed"] is True
    assert scope["automatic_exposure_cap_allowed"] is False
    assert scope["target_weight_action_allowed"] is False
    assert scope["runtime_scheduler_enabled"] is False
    assert scope["broker_action"] == "none"
