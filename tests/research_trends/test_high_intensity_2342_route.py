from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    build_high_intensity_2342_task_route,
)


def test_2342_route_ready_goes_to_observe_only_runtime_integration_plan() -> None:
    route = build_high_intensity_2342_task_route(
        {
            "readiness_status": "READY_FOR_2342_OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN",
            "readiness_blockers": [],
            "readiness_warnings": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan"
    )


def test_2342_route_ready_with_caveats_keeps_observe_only_runtime_plan() -> None:
    route = build_high_intensity_2342_task_route(
        {
            "readiness_status": "READY_FOR_2342_WITH_CAVEATS",
            "readiness_blockers": [],
            "readiness_warnings": [
                "PARTIAL_COVERAGE_CAVEAT",
                "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
            ],
        }
    )

    assert route["next_task"] == (
        "TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan"
    )
    assert "PARTIAL_COVERAGE_CAVEAT" in route["route_caveats"]


def test_2342_route_prerequisite_remediation_goes_to_remediation() -> None:
    route = build_high_intensity_2342_task_route(
        {
            "readiness_status": "PREREQUISITE_REMEDIATION_REQUIRED",
            "readiness_blockers": ["EVENT_SCHEMA_MISSING"],
            "readiness_warnings": [],
        }
    )

    assert route["next_task"] == (
        "TRADING-2342_High_Intensity_Risk_Cap_Runtime_Prerequisite_Remediation"
    )


def test_2342_route_blocked_goes_to_archive() -> None:
    route = build_high_intensity_2342_task_route(
        {
            "readiness_status": "CONTINUATION_DECISION_BLOCKED",
            "readiness_blockers": ["SAFETY_GATE_OPEN"],
            "readiness_warnings": [],
        }
    )

    assert route["next_task"] == "TRADING-2342_Archive_High_Intensity_Risk_Cap_Observe_Line"
    assert route["promotion_allowed"] is False
    assert route["broker_action"] == "none"
