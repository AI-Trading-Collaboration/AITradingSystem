from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    build_high_intensity_2341_task_route,
    build_high_intensity_continue_refine_archive_decision_matrix,
)


def _decision(
    *,
    downside: str = "DOWNSIDE_CAPTURE_MODERATE",
    false_warning: str = "FALSE_WARNING_MODERATE",
    missed: str = "MISSED_UPSIDE_ACCEPTABLE",
    manual: str = "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY",
    monthly: str = "CONCENTRATION_LOW_IMPACT",
    caveat: str = "LOW",
) -> dict[str, object]:
    return build_high_intensity_continue_refine_archive_decision_matrix(
        source_data_quality={"validate_data_status": "PASS_WITH_WARNINGS"},
        partial_caveat={
            "coverage_caveat_materiality": caveat,
            "caveat_label": f"PARTIAL_COVERAGE_{caveat}_IMPACT",
        },
        false_warning_review={"false_warning_label": false_warning},
        missed_upside_review={"missed_upside_label": missed},
        downside_capture_review={"downside_capture_label": downside},
        manual_review={"manual_review_usefulness_label": manual},
        monthly_review={"monthly_concentration_effect_label": monthly},
        selected_rule_assessment={"rule_outcome_label": "RULE_SHOWS_FORWARD_OBSERVE_VALUE"},
    )


def test_decision_matrix_continue_route() -> None:
    decision = _decision()
    route = build_high_intensity_2341_task_route(decision)

    assert decision["overall_recommendation"] == "CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE"
    assert route["next_task"] == (
        "TRADING-2341_High_Intensity_Risk_Cap_Continue_Forward_Observe_Decision"
    )
    assert decision["promotion_allowed"] is False


def test_decision_matrix_refine_route() -> None:
    decision = _decision(false_warning="FALSE_WARNING_HIGH")

    assert decision["overall_recommendation"] == "REFINE_HIGH_INTENSITY_THRESHOLD"
    assert decision["next_task_recommendation"] == (
        "TRADING-2341_High_Intensity_Risk_Cap_Threshold_Refinement_Plan"
    )


def test_decision_matrix_manual_review_only_route() -> None:
    decision = _decision(
        downside="DOWNSIDE_CAPTURE_ABSENT",
        false_warning="FALSE_WARNING_MODERATE",
        missed="MISSED_UPSIDE_ACCEPTABLE",
    )

    assert decision["overall_recommendation"] == "MANUAL_REVIEW_ONLY_CONTINUE"


def test_decision_matrix_wait_route() -> None:
    decision = _decision(caveat="HIGH")

    assert decision["overall_recommendation"] == "WAIT_FOR_FULL_20D_COVERAGE"


def test_decision_matrix_data_remediation_route() -> None:
    decision = build_high_intensity_continue_refine_archive_decision_matrix(
        source_data_quality={"validate_data_status": "FAIL"},
        partial_caveat={"coverage_caveat_materiality": "LOW", "caveat_label": "x"},
        false_warning_review={"false_warning_label": "FALSE_WARNING_MODERATE"},
        missed_upside_review={"missed_upside_label": "MISSED_UPSIDE_ACCEPTABLE"},
        downside_capture_review={"downside_capture_label": "DOWNSIDE_CAPTURE_MODERATE"},
        manual_review={"manual_review_usefulness_label": "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY"},
        monthly_review={"monthly_concentration_effect_label": "CONCENTRATION_LOW_IMPACT"},
        selected_rule_assessment={"rule_outcome_label": "RULE_INCONCLUSIVE"},
    )

    assert decision["overall_recommendation"] == "DATA_REMEDIATION_REQUIRED"
    assert decision["broker_action"] == "none"
