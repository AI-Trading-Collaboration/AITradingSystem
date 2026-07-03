from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    NEXT_ARCHIVE_TASK,
    NEXT_DATA_REMEDIATION_TASK,
    NEXT_EVENT_LOGGER_TASK,
    NEXT_THRESHOLD_REMEDIATION_TASK,
    build_high_intensity_2336_task_route,
)


def _readiness(status: str) -> dict[str, object]:
    return {"readiness_status": status}


def _decision(status: str) -> dict[str, object]:
    return {"threshold_selection_status": status}


def test_2336_route_ready_goes_to_event_logger() -> None:
    route = build_high_intensity_2336_task_route(
        readiness=_readiness("READY_FOR_2336_EVENT_LOGGER"),
        decision=_decision("THRESHOLD_SELECTED_PROMOTION_BLOCKED"),
    )

    assert route["next_task"] == NEXT_EVENT_LOGGER_TASK
    assert route["caveat"] == ""


def test_2336_route_ready_with_caveat_goes_to_event_logger_with_caveat() -> None:
    route = build_high_intensity_2336_task_route(
        readiness=_readiness("READY_FOR_2336_EVENT_LOGGER_WITH_CAVEAT"),
        decision=_decision("THRESHOLD_SELECTED_WITH_WARNINGS_PROMOTION_BLOCKED"),
    )

    assert route["next_task"] == NEXT_EVENT_LOGGER_TASK
    assert route["caveat"] == "THRESHOLD_SELECTION_RESEARCH_ONLY"


def test_2336_route_no_acceptable_threshold_goes_to_remediation() -> None:
    route = build_high_intensity_2336_task_route(
        readiness=_readiness("THRESHOLD_SELECTION_REMEDIATION_REQUIRED"),
        decision=_decision("THRESHOLD_SELECTION_BLOCKED_NO_ACCEPTABLE_CANDIDATE"),
    )

    assert route["next_task"] == NEXT_THRESHOLD_REMEDIATION_TASK


def test_2336_route_data_contract_issue_goes_to_data_remediation() -> None:
    route = build_high_intensity_2336_task_route(
        readiness=_readiness("THRESHOLD_SELECTION_REMEDIATION_REQUIRED"),
        decision=_decision("THRESHOLD_SELECTION_BLOCKED_DATA_CONTRACT"),
    )

    assert route["next_task"] == NEXT_DATA_REMEDIATION_TASK


def test_2336_route_unknown_blocked_state_goes_to_archive() -> None:
    route = build_high_intensity_2336_task_route(
        readiness=_readiness("PLAN_BLOCKED"),
        decision=_decision("THRESHOLD_SELECTION_INCONCLUSIVE"),
    )

    assert route["next_task"] == NEXT_ARCHIVE_TASK
    assert route["broker_action"] == "none"
