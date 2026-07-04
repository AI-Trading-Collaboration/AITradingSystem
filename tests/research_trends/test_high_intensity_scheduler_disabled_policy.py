from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    build_high_intensity_scheduler_disabled_by_default_policy,
    build_high_intensity_scheduler_dry_run_execution_plan,
)


def test_scheduler_disabled_by_default_policy_requires_future_activation() -> None:
    policy = build_high_intensity_scheduler_disabled_by_default_policy()

    assert policy["scheduler_default_enabled"] is False
    assert policy["activation_requires_future_task"] is True
    assert policy["activation_task_not_2344"] is True
    assert policy["activation_requires_owner_review"] is True
    assert policy["activation_requires_dry_run_pass"] is True
    assert policy["activation_requires_safety_gate_pass"] is True
    assert policy["activation_requires_no_paper_shadow"] is True
    assert policy["activation_requires_no_production"] is True
    assert policy["activation_requires_no_broker_action"] is True
    assert policy["promotion_allowed"] is False
    assert policy["broker_action"] == "none"


def test_scheduler_dry_run_execution_plan_remains_observe_only() -> None:
    plan = build_high_intensity_scheduler_dry_run_execution_plan()

    assert plan["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
    )
    assert plan["scheduler_enabled"] is False
    assert "historical_replay_scheduler_cycle" in plan["dry_run_mode"]
    assert "event_detection_job_contract" in plan["required_inputs"]
    assert "scheduler_safety_gate_result" in plan["expected_outputs"]
    assert plan["promotion_allowed"] is False
    assert plan["paper_shadow_allowed"] is False
    assert plan["production_allowed"] is False
    assert plan["broker_action"] == "none"
