from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_event_logger_fixtures import (
    build_high_intensity_event_logger_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    EXPECTED_2336_TASK,
    EXPECTED_SELECTED_RULE_ID,
    HighIntensityEventLoggerError,
    load_high_intensity_event_logger_inputs,
)


def test_high_intensity_event_logger_loader_accepts_2335_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_event_logger_fixture(tmp_path)

    inputs = load_high_intensity_event_logger_inputs(
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        readiness_dir=fixture["readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
    )

    threshold = inputs["threshold_selection"]
    assert threshold["selected_rule"]["selected_rule_id"] == EXPECTED_SELECTED_RULE_ID
    assert threshold["task_route"]["next_task"] == EXPECTED_2336_TASK
    assert threshold["summary"]["runtime_observe_started"] is False
    assert threshold["summary"]["promotion_allowed"] is False


def test_high_intensity_event_logger_loader_rejects_wrong_2336_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_event_logger_fixture(tmp_path)
    route_path = fixture["threshold_selection_dir"] / "high_intensity_2336_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2336_Wrong_Task"
    write_json(route_path, route)

    with pytest.raises(HighIntensityEventLoggerError, match="threshold route"):
        load_high_intensity_event_logger_inputs(
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        )
