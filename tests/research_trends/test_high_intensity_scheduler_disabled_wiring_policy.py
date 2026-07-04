from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    build_high_intensity_scheduler_disabled_wiring_policy,
)


def test_scheduler_disabled_wiring_policy_blocks_activation_in_2346() -> None:
    policy = build_high_intensity_scheduler_disabled_wiring_policy()

    assert policy["activation_requires_future_task"] is True
    assert policy["activation_requires_owner_review"] is True
    assert policy["activation_not_allowed_in_2346"] is True
    assert policy["paper_shadow_allowed"] is False
    assert policy["broker_action"] == "none"
