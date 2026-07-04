from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    build_high_intensity_2345_task_route,
)


def test_2345_route_sends_ready_status_to_scheduler_dry_run() -> None:
    route = build_high_intensity_2345_task_route(
        {"readiness_status": "READY_FOR_2345_OBSERVE_ONLY_SCHEDULER_DRY_RUN"}
    )

    assert route["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
    )
    assert route["scheduler_enabled"] is False
    assert route["paper_shadow_allowed"] is False
    assert route["production_allowed"] is False
    assert route["broker_action"] == "none"


def test_2345_route_sends_caveated_ready_status_to_scheduler_dry_run() -> None:
    route = build_high_intensity_2345_task_route(
        {"readiness_status": "READY_FOR_2345_WITH_CAVEATS"}
    )

    assert route["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
    )
    assert "MONTHLY_CONCENTRATION_MONITORING_REQUIRED" in route["route_caveats"]
    assert route["scheduler_default_enabled"] is False


def test_2345_route_sends_plan_issue_to_plan_remediation() -> None:
    route = build_high_intensity_2345_task_route(
        {"readiness_status": "SCHEDULER_PLAN_REMEDIATION_REQUIRED"}
    )

    assert route["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Scheduler_Plan_Remediation"
    )


def test_2345_route_sends_safety_issue_to_safety_remediation() -> None:
    route = build_high_intensity_2345_task_route(
        {"readiness_status": "READY_FOR_2345_WITH_CAVEATS"},
        safety_issue=True,
    )

    assert route["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
    )


def test_2345_route_archives_blocked_status_without_remediation_signal() -> None:
    route = build_high_intensity_2345_task_route(
        {"readiness_status": "SCHEDULER_PLAN_BLOCKED", "readiness_blockers": []}
    )

    assert route["next_task"] == (
        "TRADING-2345_Archive_High_Intensity_Risk_Cap_Runtime_Line"
    )


def test_2345_route_sends_blockers_to_safety_remediation() -> None:
    route = build_high_intensity_2345_task_route(
        {
            "readiness_status": "SCHEDULER_PLAN_BLOCKED",
            "readiness_blockers": ["SCHEDULER_SAFETY_GATE_NOT_PASS"],
        }
    )

    assert route["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
    )
    assert route["route_blockers"] == ["SCHEDULER_SAFETY_GATE_NOT_PASS"]
