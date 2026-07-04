from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    build_high_intensity_2343_task_route,
)


def test_2343_route_ready_goes_to_observe_only_runtime_dry_run() -> None:
    route = build_high_intensity_2343_task_route(
        {
            "readiness_status": "READY_FOR_2343_OBSERVE_ONLY_RUNTIME_DRY_RUN",
            "readiness_blockers": [],
            "readiness_warnings": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run"
    )


def test_2343_route_ready_with_caveats_keeps_dry_run_route() -> None:
    route = build_high_intensity_2343_task_route(
        {
            "readiness_status": "READY_FOR_2343_WITH_CAVEATS",
            "readiness_blockers": [],
            "readiness_warnings": [
                "PARTIAL_COVERAGE_CAVEAT",
                "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
            ],
        }
    )

    assert route["next_task"] == (
        "TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run"
    )
    assert "OBSERVE_ONLY_NO_PAPER_SHADOW" in route["route_caveats"]


def test_2343_route_prerequisite_issue_goes_to_remediation() -> None:
    route = build_high_intensity_2343_task_route(
        {
            "readiness_status": "RUNTIME_PREREQUISITE_REMEDIATION_REQUIRED",
            "readiness_blockers": ["EVENT_SCHEMA_MISSING"],
            "readiness_warnings": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2343_High_Intensity_Risk_Cap_Runtime_Prerequisite_Remediation"
    )


def test_2343_route_blocked_goes_to_archive() -> None:
    route = build_high_intensity_2343_task_route(
        {
            "readiness_status": "RUNTIME_INTEGRATION_PLAN_BLOCKED",
            "readiness_blockers": ["SAFETY_GATE_OPEN"],
            "readiness_warnings": [],
        }
    )

    assert route["next_task"] == "TRADING-2343_Archive_High_Intensity_Risk_Cap_Runtime_Line"
    assert route["promotion_allowed"] is False
    assert route["broker_action"] == "none"
