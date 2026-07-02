from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_2331_task_route,
)


def test_ready_wrapper_routes_to_2331_recheck() -> None:
    route = build_dynamic_target_2331_task_route(
        {"readiness_status": "TIMESTAMP_REMEDIATED_READY_FOR_2331"}
    )

    assert route["next_task"] == (
        "TRADING-2331_Dynamic_Target_Baseline_Wrapper_Readiness_Recheck"
    )


def test_ready_with_warnings_routes_to_pit_caveat_recheck() -> None:
    route = build_dynamic_target_2331_task_route(
        {"readiness_status": "TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331"}
    )

    assert route["next_task"] == (
        "TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat"
    )


def test_source_generation_and_static_only_routes() -> None:
    source_route = build_dynamic_target_2331_task_route(
        {"readiness_status": "TIMESTAMP_REMEDIATION_SOURCE_GENERATION_REQUIRED"}
    )
    static_route = build_dynamic_target_2331_task_route(
        {"readiness_status": "TIMESTAMP_REMEDIATION_BLOCKED"}
    )

    assert source_route["next_task"] == (
        "TRADING-2331_Dynamic_Target_Baseline_Source_Generation"
    )
    assert static_route["next_task"] == "TRADING-2331_Continue_Static_Baseline_Only"
    assert static_route["promotion_allowed"] is False
