from __future__ import annotations

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    build_dynamic_2334_task_route,
    build_dynamic_exposure_cap_decision_matrix,
)


def _decision(**overrides: object) -> dict[str, object]:
    payload = {
        "cap_binding": {"cap_binding_frequency_label": "EXCESSIVE_BINDING_FREQUENCY"},
        "overbinding": {"overbinding_label": "OVERBINDING_MODERATE"},
        "exposure_reduction": {
            "exposure_reduction_label": "INCREMENTAL_EXPOSURE_REDUCTION_MODEST"
        },
        "return_drawdown": {
            "return_cost_materiality": "RETURN_COST_HIGH",
            "return_drawdown_tradeoff_label": "DRAWDOWN_IMPROVED_RETURN_COSTLY",
        },
        "turnover_cooldown": {"turnover_cooldown_label": "TURNOVER_COOLDOWN_ACCEPTABLE"},
        "false_cost": {
            "false_cost_label": "FALSE_COST_MODERATE",
            "missed_upside_label": "MISSED_UPSIDE_MODERATE",
        },
        "downside": {"downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY"},
        "strategy_overlap": {"overlap_label": "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY"},
        "static_dynamic": {"comparison_label": "DYNAMIC_BASELINE_INCONCLUSIVE"},
        "pit_boundary": {"pit_approximation_ready": True, "strict_pit_ready": False},
        "data_quality_status": "PASS",
    }
    payload.update(overrides)
    return build_dynamic_exposure_cap_decision_matrix(**payload)  # type: ignore[arg-type]


def test_decision_matrix_policy_refinement_route() -> None:
    decision = _decision()
    route = build_dynamic_2334_task_route(decision)

    assert decision["overall_recommendation"] == "REFINE_EXPOSURE_CAP_POLICY"
    assert route["next_task"] == "TRADING-2334_Dynamic_Exposure_Cap_Policy_Refinement_Plan"


def test_decision_matrix_high_intensity_route() -> None:
    decision = _decision(overbinding={"overbinding_label": "OVERBINDING_BLOCKING"})
    route = build_dynamic_2334_task_route(decision)

    assert decision["overall_recommendation"] == "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
    assert route["next_task"] == (
        "TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan"
    )


def test_decision_matrix_manual_review_route() -> None:
    decision = _decision(
        cap_binding={"cap_binding_frequency_label": "MODERATE_BINDING_FREQUENCY"},
        return_drawdown={
            "return_cost_materiality": "RETURN_COST_MATERIAL",
            "return_drawdown_tradeoff_label": "DRAWDOWN_IMPROVED_RETURN_COSTLY",
        },
    )
    route = build_dynamic_2334_task_route(decision)

    assert decision["overall_recommendation"] == "MANUAL_REVIEW_ONLY_RISK_CAP"
    assert route["next_task"] == "TRADING-2334_Manual_Review_Only_Risk_Cap_Plan"


def test_decision_matrix_archive_route() -> None:
    decision = _decision(
        false_cost={
            "false_cost_label": "FALSE_COST_BLOCKING",
            "missed_upside_label": "MISSED_UPSIDE_BLOCKING",
        },
        downside={"downside_protection_label": "DOWNSIDE_PROTECTION_WEAK_PROXY"},
        strategy_overlap={
            "overlap_label": "RISK_CAP_MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY"
        },
    )
    route = build_dynamic_2334_task_route(decision)

    assert decision["overall_recommendation"] == "ARCHIVE_CURRENT_EXPOSURE_CAP_MECHANICS"
    assert route["next_task"] == "TRADING-2334_Archive_Current_Exposure_Cap_Mechanics"


def test_decision_matrix_data_quality_route_and_safety_fields() -> None:
    decision = _decision(data_quality_status="FAIL")
    route = build_dynamic_2334_task_route(decision)

    assert decision["overall_recommendation"] == "DATA_QUALITY_REMEDIATION_REQUIRED"
    assert route["next_task"] == "TRADING-2334_Dynamic_Target_Baseline_Data_Remediation"
    assert decision["promotion_allowed"] is False
    assert decision["paper_shadow_allowed"] is False
    assert decision["production_allowed"] is False
    assert decision["broker_action"] == "none"
