from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    build_high_intensity_2335_task_route,
)


def test_high_intensity_route_plan_ready_goes_to_event_logger() -> None:
    route = build_high_intensity_2335_task_route(
        {"readiness_status": "PLAN_READY_FOR_2335_EVENT_LOGGER"}
    )

    assert route["next_task"] == (
        "TRADING-2335_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger"
    )


def test_high_intensity_route_threshold_selection_required() -> None:
    route = build_high_intensity_2335_task_route(
        {"readiness_status": "THRESHOLD_SELECTION_REQUIRED"}
    )

    assert route["next_task"] == (
        "TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection"
    )


def test_high_intensity_route_data_contract_remediation() -> None:
    route = build_high_intensity_2335_task_route(
        {"readiness_status": "DATA_CONTRACT_REMEDIATION_REQUIRED"}
    )

    assert route["next_task"] == (
        "TRADING-2335_High_Intensity_Risk_Cap_Data_Contract_Remediation"
    )


def test_high_intensity_route_blocked_archives_and_safety_fields() -> None:
    route = build_high_intensity_2335_task_route({"readiness_status": "PLAN_BLOCKED"})

    assert route["next_task"] == "TRADING-2335_Archive_High_Intensity_Risk_Cap_Observe_Line"
    assert route["promotion_allowed"] is False
    assert route["paper_shadow_allowed"] is False
    assert route["production_allowed"] is False
    assert route["broker_action"] == "none"
