from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    build_high_intensity_2344_task_route,
)


def _readiness(status: str, blockers: list[str] | None = None) -> dict[str, object]:
    return {
        "readiness_status": status,
        "readiness_blockers": blockers or [],
        "readiness_warnings": [],
    }


def test_2344_route_ready_goes_to_scheduler_integration_plan() -> None:
    route = build_high_intensity_2344_task_route(
        readiness=_readiness(
            "READY_FOR_2344_OBSERVE_ONLY_RUNTIME_SCHEDULER_INTEGRATION_PLAN"
        ),
        safety_gate={"safety_gate_status": "PASS"},
        artifact_registry={"registry_dry_run_status": "PASS"},
    )

    assert route["next_task"] == (
        "TRADING-2344_High_Intensity_Risk_Cap_Observe_Only_Runtime_Scheduler_Integration_Plan"
    )


def test_2344_route_ready_with_caveats_keeps_scheduler_plan() -> None:
    route = build_high_intensity_2344_task_route(
        readiness=_readiness("READY_FOR_2344_WITH_CAVEATS"),
        safety_gate={"safety_gate_status": "PASS"},
        artifact_registry={"registry_dry_run_status": "PASS"},
    )

    assert route["next_task"] == (
        "TRADING-2344_High_Intensity_Risk_Cap_Observe_Only_Runtime_Scheduler_Integration_Plan"
    )


def test_2344_route_safety_failed_goes_to_safety_remediation() -> None:
    route = build_high_intensity_2344_task_route(
        readiness=_readiness("RUNTIME_DRY_RUN_BLOCKED", ["FAIL_CLOSED_SAFETY_GATE"]),
        safety_gate={"safety_gate_status": "FAIL_CLOSED_TRIGGERED"},
        artifact_registry={"registry_dry_run_status": "PASS"},
    )

    assert route["next_task"] == (
        "TRADING-2344_High_Intensity_Risk_Cap_Runtime_Safety_Remediation"
    )


def test_2344_route_registry_failed_goes_to_registry_remediation() -> None:
    route = build_high_intensity_2344_task_route(
        readiness=_readiness("RUNTIME_DRY_RUN_REMEDIATION_REQUIRED"),
        safety_gate={"safety_gate_status": "PASS"},
        artifact_registry={"registry_dry_run_status": "FAIL"},
    )

    assert route["next_task"] == (
        "TRADING-2344_High_Intensity_Risk_Cap_Runtime_Artifact_Registry_Remediation"
    )


def test_2344_route_dry_run_remediation_or_archive() -> None:
    route = build_high_intensity_2344_task_route(
        readiness=_readiness("RUNTIME_DRY_RUN_REMEDIATION_REQUIRED"),
        safety_gate={"safety_gate_status": "PASS"},
        artifact_registry={"registry_dry_run_status": "PASS"},
    )
    archive = build_high_intensity_2344_task_route(
        readiness=_readiness("RUNTIME_DRY_RUN_BLOCKED"),
        safety_gate={"safety_gate_status": "PASS"},
        artifact_registry={"registry_dry_run_status": "PASS"},
    )

    assert route["next_task"] == (
        "TRADING-2344_High_Intensity_Risk_Cap_Runtime_Dry_Run_Remediation"
    )
    assert archive["next_task"] == "TRADING-2344_Archive_High_Intensity_Risk_Cap_Runtime_Line"
