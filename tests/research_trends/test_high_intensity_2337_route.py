from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    NEXT_ARCHIVE_TASK,
    NEXT_DATA_CONTRACT_REMEDIATION_TASK,
    NEXT_OUTCOME_BINDER_TASK,
    NEXT_ZERO_EVENT_TASK,
    build_high_intensity_2337_task_route,
)


def test_2337_route_ready_goes_to_outcome_binder() -> None:
    route = build_high_intensity_2337_task_route(
        {"readiness_status": "READY_FOR_2337_OUTCOME_BINDER_WITH_WARNINGS"}
    )

    assert route["next_task"] == NEXT_OUTCOME_BINDER_TASK


def test_2337_route_zero_events_goes_to_zero_event_review() -> None:
    route = build_high_intensity_2337_task_route(
        {"readiness_status": "ZERO_EVENT_REVIEW_REQUIRED"}
    )

    assert route["next_task"] == NEXT_ZERO_EVENT_TASK


def test_2337_route_data_contract_issue_goes_to_remediation() -> None:
    route = build_high_intensity_2337_task_route(
        {"readiness_status": "DATA_CONTRACT_REMEDIATION_REQUIRED"}
    )

    assert route["next_task"] == NEXT_DATA_CONTRACT_REMEDIATION_TASK


def test_2337_route_unknown_blocked_state_goes_to_archive() -> None:
    route = build_high_intensity_2337_task_route(
        {"readiness_status": "EVENT_LOGGER_BLOCKED"}
    )

    assert route["next_task"] == NEXT_ARCHIVE_TASK
    assert route["broker_action"] == "none"
