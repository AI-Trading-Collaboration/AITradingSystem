from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_runtime_integration_plan_fixtures import (
    build_high_intensity_runtime_integration_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    HighIntensityRuntimeIntegrationPlanError,
    load_high_intensity_runtime_integration_plan_inputs,
)


def test_runtime_integration_loader_reads_required_artifacts(tmp_path: Path) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)

    loaded = load_high_intensity_runtime_integration_plan_inputs(
        continue_decision_dir=fixture["continue_decision_dir"],
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    assert loaded["continue_decision"]["summary"]["overall_decision"] == (
        "CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT"
    )
    assert loaded["continue_decision"]["selected_rule_contract"]["selected_rule_id"] == (
        "COMPOSITE_HIGH_INTENSITY_RULE"
    )
    assert loaded["continue_decision"]["event_logger_contract"]["contract_id"]
    assert loaded["continue_decision"]["outcome_update_policy"]["update_horizons"] == [
        "1d",
        "5d",
        "10d",
        "20d",
    ]
    assert loaded["continue_decision"]["manual_review_policy"][
        "manual_review_context_allowed"
    ]
    assert loaded["event_logger"]["summary"]["cluster_count"] == 60


def test_runtime_integration_loader_fails_closed_on_bad_2341_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    route_path = fixture["continue_decision_dir"] / "high_intensity_2342_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2342_Archive_High_Intensity_Risk_Cap_Observe_Line"
    write_json(route_path, route)

    with pytest.raises(HighIntensityRuntimeIntegrationPlanError):
        load_high_intensity_runtime_integration_plan_inputs(
            continue_decision_dir=fixture["continue_decision_dir"],
            forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_runtime_integration_loader_fails_closed_on_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    summary_path = (
        fixture["continue_decision_dir"]
        / "high_intensity_continue_observe_decision_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityRuntimeIntegrationPlanError):
        load_high_intensity_runtime_integration_plan_inputs(
            continue_decision_dir=fixture["continue_decision_dir"],
            forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_runtime_integration_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    safety_path = (
        fixture["continue_decision_dir"]
        / "high_intensity_continue_observe_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "submit_order"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensityRuntimeIntegrationPlanError):
        load_high_intensity_runtime_integration_plan_inputs(
            continue_decision_dir=fixture["continue_decision_dir"],
            forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )
