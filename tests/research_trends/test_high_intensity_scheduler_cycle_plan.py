from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_scheduler_cycle_plan,
    build_high_intensity_scheduler_job_dag_validation_report,
)


def test_scheduler_cycle_plan_contains_required_cycle_modes() -> None:
    plan = build_high_intensity_scheduler_cycle_plan()

    assert "historical_replay_scheduler_cycle" in plan["cycle_modes"]
    assert "single_day_scheduler_cycle_fixture" in plan["cycle_modes"]
    assert "fail_closed_safety_fixture" in plan["cycle_modes"]
    assert plan["scheduler_enabled"] is False
    assert plan["skip_non_trading_days"] is True
    assert plan["known_at_policy"] == "NEXT_SESSION_DECISION_POLICY"


def test_scheduler_job_dag_order_is_valid() -> None:
    report = build_high_intensity_scheduler_job_dag_validation_report()

    assert report["dag_validation_status"] == "PASS"
    assert report["job_order_valid"] is True
    assert report["cycle_detected"] is False


def test_scheduler_job_dag_missing_dependency_fails() -> None:
    report = build_high_intensity_scheduler_job_dag_validation_report(
        job_dependencies={"event_detection": ["missing_input"]},
    )

    assert report["dag_validation_status"] == "FAIL"
    assert report["missing_dependency_count"] == 1
