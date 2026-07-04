from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    build_high_intensity_runtime_outcome_update_job_plan,
)


def test_runtime_outcome_update_plan_keeps_scheduler_off() -> None:
    plan = build_high_intensity_runtime_outcome_update_job_plan()

    assert plan["update_horizons"] == ["1d", "5d", "10d", "20d"]
    assert plan["scheduler_enabled_in_2342"] is False
    assert plan["data_validation_required_when_enabled"] is True
    assert plan["original_event_log_mutation_allowed"] is False
    assert plan["paper_shadow_allowed"] is False
    assert plan["production_allowed"] is False
