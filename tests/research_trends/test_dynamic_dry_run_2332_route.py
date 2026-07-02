from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    build_dynamic_dry_run_2332_readiness_matrix,
    build_dynamic_dry_run_2332_task_route,
)


def test_ready_with_pit_caveat_routes_to_2332_dynamic_dry_run() -> None:
    readiness = build_dynamic_dry_run_2332_readiness_matrix(
        selected_source={"baseline_id": "baseline", "source_id": "source"},
        wrapper_validation={"validation_status": "PASS_WITH_WARNINGS"},
        gate_checklist={"gate_status": "DYNAMIC_DRY_RUN_READY_WITH_PIT_CAVEAT", "blockers": []},
    )
    route = build_dynamic_dry_run_2332_task_route(readiness)

    assert readiness["2332_allowed"] is True
    assert route["next_task"] == (
        "TRADING-2332_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline"
    )
    assert route["simulation_executed"] is False


def test_wrapper_failure_routes_to_wrapper_remediation() -> None:
    readiness = build_dynamic_dry_run_2332_readiness_matrix(
        selected_source={"baseline_id": "baseline", "source_id": "source"},
        wrapper_validation={"validation_status": "FAIL"},
        gate_checklist={
            "gate_status": "DYNAMIC_DRY_RUN_READINESS_BLOCKED",
            "blockers": ["wrapper_required_fields"],
        },
    )
    route = build_dynamic_dry_run_2332_task_route(readiness)

    assert readiness["2332_allowed"] is False
    assert route["next_task"] == "TRADING-2332_Dynamic_Target_Baseline_Wrapper_Remediation"
