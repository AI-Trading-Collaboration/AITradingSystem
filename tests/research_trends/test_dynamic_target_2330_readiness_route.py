from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    build_dynamic_target_2330_readiness_matrix,
    build_dynamic_target_2330_task_route,
)


def _validation(status: str = "PASS") -> dict:
    return {
        "wrapper_generated": True,
        "validation_status": status,
    }


def _pit(caveat: bool = False) -> dict:
    return {
        "pit_policy": "PIT_APPROXIMATION_READY" if caveat else "STRICT_PIT_REMEDIATED_READY",
        "pit_approximation_ready": caveat,
    }


def _alignment(status: str = "WRAPPER_ALIGNMENT_READY") -> dict:
    return {
        "alignment_readiness_status": status,
        "alignment_blockers": [],
    }


def test_ready_wrapper_routes_to_2330_dynamic_dry_run() -> None:
    readiness = build_dynamic_target_2330_readiness_matrix(
        wrapper_validation=_validation(),
        pit_caveat_report=_pit(),
        alignment_readiness=_alignment(),
        action_rows=[{"source_id": "s", "wrapper_allowed": True}],
        selected_source_id="s",
    )
    route = build_dynamic_target_2330_task_route(
        readiness=readiness,
        action_rows=[],
        gap_rows=[],
        selected_source_id="s",
    )

    assert readiness["2330_allowed"] is True
    assert route["next_task"] == (
        "TRADING-2330_Source_Bound_Exposure_Cap_Dry_Run_With_Remediated_Dynamic_Target_Baseline"
    )


def test_ready_with_warnings_keeps_dynamic_route_with_caveat() -> None:
    readiness = build_dynamic_target_2330_readiness_matrix(
        wrapper_validation=_validation("PASS_WITH_WARNINGS"),
        pit_caveat_report=_pit(caveat=True),
        alignment_readiness=_alignment("WRAPPER_ALIGNMENT_READY_WITH_WARNINGS"),
        action_rows=[{"source_id": "s", "wrapper_allowed": True}],
        selected_source_id="s",
    )
    route = build_dynamic_target_2330_task_route(
        readiness=readiness,
        action_rows=[],
        gap_rows=[],
        selected_source_id="s",
    )

    assert readiness["readiness_status"] == "DYNAMIC_WRAPPER_READY_WITH_WARNINGS_FOR_2330"
    assert route["caveat"] == "PIT_OR_ALIGNMENT_WARNINGS"


def test_no_remediable_source_routes_to_source_generation() -> None:
    readiness = build_dynamic_target_2330_readiness_matrix(
        wrapper_validation={"wrapper_generated": False, "validation_status": "FAIL"},
        pit_caveat_report={},
        alignment_readiness=_alignment("WRAPPER_ALIGNMENT_BLOCKED"),
        action_rows=[{"source_id": "s", "wrapper_allowed": False}],
        selected_source_id="",
    )
    route = build_dynamic_target_2330_task_route(
        readiness=readiness,
        action_rows=[],
        gap_rows=[],
        selected_source_id="",
    )

    assert route["next_task"] == "TRADING-2330_Dynamic_Target_Baseline_Source_Generation"


def test_timestamp_gap_routes_to_timestamp_remediation() -> None:
    readiness = {
        "readiness_status": "DYNAMIC_WRAPPER_BLOCKED",
        "readiness_warnings": [],
    }
    route = build_dynamic_target_2330_task_route(
        readiness=readiness,
        action_rows=[],
        gap_rows=[
            {
                "source_id": "s",
                "required_field": "decision_timestamp",
                "field_available": False,
            }
        ],
        selected_source_id="s",
    )

    assert route["next_task"] == "TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation"
