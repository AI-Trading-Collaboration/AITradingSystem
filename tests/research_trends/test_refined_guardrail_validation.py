from __future__ import annotations

from ai_trading_system.refined_candidate_actual_path_validation import (
    build_refined_candidate_state_recommendation_matrix,
    build_refined_guardrail_validation_matrix,
)


def _comparison(**overrides):
    row = {
        "original_candidate_id": "risk_appetite",
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "high_confidence_ratio_refined": 0.2,
        "false_risk_on_cost_delta": 0.0,
        "false_risk_off_cost_delta": 0.0,
        "neutral_ratio_original": 0.5,
        "neutral_ratio_refined": 0.4,
        "comparison_label": "REFINED_IMPROVED_WITHIN_GUARDRAILS",
    }
    row.update(overrides)
    return row


def _score(**overrides):
    row = {
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "original_candidate_id": "risk_appetite",
        "validation_eligible_record_count": 10,
    }
    row.update(overrides)
    return row


def _guardrail(**overrides):
    row = {
        "candidate_id": "risk_appetite",
        "guardrail_profile": "strict_cost_neutral",
        "max_high_confidence_ratio": 0.35,
        "max_false_risk_on_cost_increase": 0.0,
        "max_false_risk_off_cost_increase": 0.0,
        "max_neutral_ratio_reduction": 0.3,
        "minimum_eligible_records": 10,
        "data_quality_required_status": ["PASS", "PASS_WITH_WARNINGS"],
    }
    row.update(overrides)
    return row


def _matrix(comparison, score=None, guardrail=None):
    return build_refined_guardrail_validation_matrix(
        comparison_rows=[comparison],
        refined_scorecards=[score or _score()],
        guardrail_rows=[guardrail or _guardrail()],
        refined_data_quality_rows=[
            {
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "data_quality_status": "PASS",
            }
        ],
    )[0]


def test_guardrail_passes_when_all_thresholds_met() -> None:
    row = _matrix(_comparison())

    assert row["guardrail_status"] == "PASS"
    assert row["promotion_allowed"] is False


def test_guardrail_fails_high_confidence_ratio() -> None:
    row = _matrix(_comparison(high_confidence_ratio_refined=0.5))

    assert row["guardrail_status"] == "FAIL"
    assert "max_high_confidence_ratio_exceeded" in row["guardrail_fail_reasons"]


def test_guardrail_fails_false_risk_cost_increases() -> None:
    row = _matrix(_comparison(false_risk_on_cost_delta=0.1, false_risk_off_cost_delta=0.1))

    assert row["guardrail_status"] == "FAIL"
    assert "false_risk_on_cost_increase_exceeded" in row["guardrail_fail_reasons"]
    assert "false_risk_off_cost_increase_exceeded" in row["guardrail_fail_reasons"]


def test_guardrail_fails_neutral_ratio_reduction() -> None:
    row = _matrix(_comparison(neutral_ratio_original=0.8, neutral_ratio_refined=0.3))

    assert row["guardrail_status"] == "FAIL"
    assert "neutral_ratio_reduction_exceeded" in row["guardrail_fail_reasons"]


def test_guardrail_fails_minimum_eligible_records() -> None:
    row = _matrix(_comparison(), score=_score(validation_eligible_record_count=5))

    assert row["guardrail_status"] == "FAIL"
    assert "minimum_eligible_records_not_met" in row["guardrail_fail_reasons"]


def test_guardrail_fail_blocks_owner_review_recommendation() -> None:
    guardrail_row = _matrix(_comparison(high_confidence_ratio_refined=0.5))
    state = build_refined_candidate_state_recommendation_matrix(
        comparison_rows=[_comparison()],
        guardrail_rows=[guardrail_row],
        high_conviction_rows=[
            {
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "high_conviction_outcome_label": "HIGH_CONVICTION_EDGE_IMPROVED",
            }
        ],
        refined_scorecards=[_score(validation_eligible_record_count=100)],
        refined_data_quality_rows=[
            {
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "data_quality_status": "PASS",
            }
        ],
    )[0]

    assert state["recommended_research_status"] == "REFINED_GUARDRAIL_BLOCKED"
    assert state["owner_review_candidate_recommendation"] is False
