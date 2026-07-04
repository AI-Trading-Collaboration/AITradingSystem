from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    build_high_intensity_2347_task_route,
)


def test_2347_route_ready_goes_to_disabled_wiring_implementation() -> None:
    route = build_high_intensity_2347_task_route(
        readiness={
            "readiness_status": "READY_FOR_2347_DISABLED_WIRING_IMPLEMENTATION",
            "readiness_blockers": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2347_High_Intensity_Risk_Cap_Observe_Only_Scheduler_"
        "Disabled_Wiring_Implementation"
    )


def test_2347_route_ready_with_caveats_keeps_disabled_wiring_implementation() -> None:
    route = build_high_intensity_2347_task_route(
        readiness={
            "readiness_status": "READY_FOR_2347_WITH_CAVEATS",
            "readiness_blockers": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2347_High_Intensity_Risk_Cap_Observe_Only_Scheduler_"
        "Disabled_Wiring_Implementation"
    )
    assert "DRY_RUN_ONLY" in route["route_caveats"]


def test_2347_route_safety_issue_goes_to_safety_remediation() -> None:
    route = build_high_intensity_2347_task_route(
        readiness={
            "readiness_status": "WIRING_PLAN_BLOCKED",
            "readiness_blockers": ["WIRING_SAFETY_GATE_TRIGGERED"],
        },
        safety_gate={"safety_gate_status": "FAIL_CLOSED_TRIGGERED"},
    )

    assert route["next_task"] == (
        "TRADING-2347_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
    )


def test_2347_route_wiring_plan_issue_goes_to_plan_remediation() -> None:
    route = build_high_intensity_2347_task_route(
        readiness={
            "readiness_status": "WIRING_PLAN_REMEDIATION_REQUIRED",
            "readiness_blockers": ["MISSING_WIRING_PLAN_ARTIFACT"],
        },
        wiring_plan_issue=True,
    )

    assert route["next_task"] == (
        "TRADING-2347_High_Intensity_Risk_Cap_Scheduler_Wiring_Plan_Remediation"
    )


def test_2347_route_blocked_without_remediation_signal_archives() -> None:
    route = build_high_intensity_2347_task_route(
        readiness={
            "readiness_status": "WIRING_PLAN_BLOCKED",
            "readiness_blockers": ["UNKNOWN_BLOCKER"],
        }
    )

    assert route["next_task"] == "TRADING-2347_Archive_High_Intensity_Risk_Cap_Runtime_Line"
