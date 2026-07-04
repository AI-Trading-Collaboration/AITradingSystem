from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_2346_task_route,
    build_high_intensity_scheduler_fail_closed_safety_gate_result,
)


def test_2346_route_ready_goes_to_scheduler_wiring_plan() -> None:
    route = build_high_intensity_2346_task_route(
        readiness={
            "readiness_status": "READY_FOR_2346_OBSERVE_ONLY_SCHEDULER_WIRING_PLAN",
            "readiness_blockers": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan"
    )


def test_2346_route_ready_with_caveats_keeps_scheduler_wiring_plan() -> None:
    route = build_high_intensity_2346_task_route(
        readiness={
            "readiness_status": "READY_FOR_2346_WITH_CAVEATS",
            "readiness_blockers": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan"
    )
    assert "OBSERVE_ONLY" in route["route_caveats"]


def test_2346_route_safety_issue_goes_to_safety_remediation() -> None:
    safety_gate = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        {"selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE", "broker_action": "BUY"}
    )
    route = build_high_intensity_2346_task_route(
        readiness={
            "readiness_status": "SCHEDULER_DRY_RUN_BLOCKED",
            "readiness_blockers": ["FAIL_CLOSED_SAFETY_GATE_TRIGGERED"],
        },
        safety_gate=safety_gate,
    )

    assert route["next_task"] == (
        "TRADING-2346_High_Intensity_Risk_Cap_Scheduler_Safety_Remediation"
    )


def test_2346_route_dry_run_issue_goes_to_dry_run_remediation() -> None:
    route = build_high_intensity_2346_task_route(
        readiness={
            "readiness_status": "SCHEDULER_DRY_RUN_REMEDIATION_REQUIRED",
            "readiness_blockers": ["JOB_DAG_VALIDATION_FAILED"],
        },
    )

    assert route["next_task"] == (
        "TRADING-2346_High_Intensity_Risk_Cap_Scheduler_Dry_Run_Remediation"
    )


def test_2346_route_blocked_without_remediation_signal_archives() -> None:
    route = build_high_intensity_2346_task_route(
        readiness={
            "readiness_status": "SCHEDULER_DRY_RUN_BLOCKED",
            "readiness_blockers": [],
        }
    )

    assert route["next_task"] == "TRADING-2346_Archive_High_Intensity_Risk_Cap_Runtime_Line"
