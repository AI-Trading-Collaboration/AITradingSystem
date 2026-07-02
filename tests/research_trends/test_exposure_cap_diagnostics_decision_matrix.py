from __future__ import annotations

from ai_trading_system.exposure_cap_diagnostics_review import (
    build_exposure_cap_2328_task_route,
    build_exposure_cap_diagnostics_decision_matrix,
)


def test_decision_matrix_routes_continue_research() -> None:
    decision = _decision(
        cap_binding_label="MODERATE_BINDING_FREQUENCY",
        tradeoff_label="DRAWDOWN_IMPROVED_RETURN_ACCEPTABLE",
        turnover_label="TURNOVER_COOLDOWN_ACCEPTABLE",
        false_cost_label="FALSE_COST_ACCEPTABLE",
        downside_label="DOWNSIDE_PROTECTION_POSITIVE_PROXY",
    )
    route = build_exposure_cap_2328_task_route(decision)

    assert decision["overall_recommendation"] == "CONTINUE_EXPOSURE_CAP_RESEARCH"
    assert route["next_task"] == (
        "TRADING-2328_Risk_Cap_Forward_Observe_Exposure_Cap_Joint_Evidence_Review"
    )
    assert decision["promotion_allowed"] is False
    assert decision["paper_shadow_allowed"] is False
    assert decision["production_allowed"] is False
    assert decision["broker_action"] == "none"


def test_decision_matrix_routes_dynamic_baseline_preparation() -> None:
    decision = _decision(
        cap_binding_label="EXCESSIVE_BINDING_FREQUENCY",
        tradeoff_label="DRAWDOWN_IMPROVED_RETURN_COSTLY",
        turnover_label="TURNOVER_HIGH_AND_COOLDOWN_COSTLY",
        false_cost_label="FALSE_COST_BLOCKING",
        downside_label="DOWNSIDE_PROTECTION_POSITIVE_PROXY",
    )
    route = build_exposure_cap_2328_task_route(decision)

    assert decision["overall_recommendation"] == (
        "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
    )
    assert route["next_task"] == "TRADING-2328_Dynamic_Target_Baseline_Preparation"


def test_decision_matrix_routes_policy_refinement() -> None:
    decision = _decision(
        cap_binding_label="HIGH_BINDING_FREQUENCY",
        tradeoff_label="DRAWDOWN_IMPROVED_RETURN_ACCEPTABLE",
        turnover_label="TURNOVER_HIGH_AND_COOLDOWN_COSTLY",
        false_cost_label="FALSE_COST_HIGH",
        downside_label="DOWNSIDE_PROTECTION_POSITIVE_PROXY",
    )
    route = build_exposure_cap_2328_task_route(decision)

    assert decision["overall_recommendation"] == "REFINE_EXPOSURE_CAP_POLICY"
    assert route["next_task"] == "TRADING-2328_Exposure_Cap_Policy_Refinement_Plan"


def test_decision_matrix_routes_data_remediation() -> None:
    decision = _decision(
        cap_binding_label="MODERATE_BINDING_FREQUENCY",
        tradeoff_label="NO_MATERIAL_DIFFERENCE",
        turnover_label="TURNOVER_COOLDOWN_ACCEPTABLE",
        false_cost_label="FALSE_COST_ACCEPTABLE",
        downside_label="DOWNSIDE_PROTECTION_INCONCLUSIVE",
        data_quality_status="FAIL",
    )
    route = build_exposure_cap_2328_task_route(decision)

    assert decision["overall_recommendation"] == "DATA_QUALITY_REMEDIATION_REQUIRED"
    assert route["next_task"] == "TRADING-2328_Static_Baseline_Data_Remediation"


def test_decision_matrix_routes_reject_current_mechanics() -> None:
    decision = _decision(
        cap_binding_label="HIGH_BINDING_FREQUENCY",
        tradeoff_label="DRAWDOWN_NOT_IMPROVED_RETURN_COSTLY",
        turnover_label="TURNOVER_HIGH_AND_COOLDOWN_COSTLY",
        false_cost_label="FALSE_COST_BLOCKING",
        downside_label="DOWNSIDE_PROTECTION_NEGATIVE_PROXY",
    )
    route = build_exposure_cap_2328_task_route(decision)

    assert decision["overall_recommendation"] == "REJECT_CURRENT_EXPOSURE_CAP_MECHANICS"
    assert route["next_task"] == "TRADING-2328_Archive_Current_Exposure_Cap_Mechanics"


def _decision(
    *,
    cap_binding_label: str,
    tradeoff_label: str,
    turnover_label: str,
    false_cost_label: str,
    downside_label: str,
    data_quality_status: str = "PASS",
) -> dict[str, object]:
    return build_exposure_cap_diagnostics_decision_matrix(
        cap_binding={"cap_binding_frequency_label": cap_binding_label},
        exposure_reduction={"exposure_reduction_label": "EXPOSURE_REDUCTION_MODEST"},
        return_drawdown={"return_drawdown_tradeoff_label": tradeoff_label},
        turnover_cooldown={"turnover_cooldown_label": turnover_label},
        false_cost={"false_cost_label": false_cost_label},
        downside={"downside_protection_label": downside_label},
        data_quality_status=data_quality_status,
    )
