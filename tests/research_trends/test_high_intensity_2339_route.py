from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    build_high_intensity_2339_task_route,
)


def test_high_intensity_2339_route_full_ready() -> None:
    route = build_high_intensity_2339_task_route(
        {"readiness_status": "READY_FOR_2339_FORWARD_OUTCOME_REVIEW"}
    )
    assert route["next_task"] == "TRADING-2339_High_Intensity_Risk_Cap_Forward_Outcome_Review"


def test_high_intensity_2339_route_ready_with_warnings() -> None:
    route = build_high_intensity_2339_task_route(
        {"readiness_status": "READY_FOR_2339_FORWARD_OUTCOME_REVIEW_WITH_WARNINGS"}
    )
    assert route["caveat"] == "DATA_QUALITY_OR_PIT_CAVEAT"


def test_high_intensity_2339_route_partial_data_and_blocked() -> None:
    partial = build_high_intensity_2339_task_route(
        {"readiness_status": "PARTIAL_OUTCOME_REVIEW_REQUIRED"}
    )
    data = build_high_intensity_2339_task_route({"readiness_status": "DATA_REMEDIATION_REQUIRED"})
    blocked = build_high_intensity_2339_task_route({"readiness_status": "OUTCOME_BINDER_BLOCKED"})

    assert partial["next_task"].endswith("Partial_Outcome_Readiness_Review")
    assert data["next_task"].endswith("Outcome_Data_Remediation")
    assert blocked["next_task"] == "TRADING-2339_Archive_High_Intensity_Risk_Cap_Observe_Line"
