from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    build_high_intensity_2340_task_route,
)


def test_high_intensity_2340_route_ready() -> None:
    route = build_high_intensity_2340_task_route(
        {"readiness_status": "READY_FOR_2340_FORWARD_OUTCOME_REVIEW"}
    )
    assert route["next_task"].endswith("Forward_Outcome_Review")


def test_high_intensity_2340_route_ready_with_partial_caveat() -> None:
    route = build_high_intensity_2340_task_route(
        {"readiness_status": ("READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT")}
    )
    assert route["next_task"].endswith("Forward_Outcome_Review_With_Partial_Coverage_Caveat")


def test_high_intensity_2340_route_wait_data_and_blocked() -> None:
    wait = build_high_intensity_2340_task_route({"readiness_status": "WAIT_FOR_NOT_DUE_HORIZONS"})
    data = build_high_intensity_2340_task_route({"readiness_status": "DATA_REMEDIATION_REQUIRED"})
    blocked = build_high_intensity_2340_task_route({"readiness_status": "READINESS_BLOCKED"})

    assert wait["next_task"].endswith("Wait_For_Not_Due_Horizons")
    assert data["next_task"].endswith("Outcome_Data_Remediation")
    assert blocked["next_task"] == "TRADING-2340_Archive_High_Intensity_Risk_Cap_Observe_Line"
