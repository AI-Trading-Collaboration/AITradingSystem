from __future__ import annotations

from ai_trading_system.refined_candidate_actual_path_validation import (
    build_refined_candidate_state_recommendation_matrix,
)


def _comparison(label: str):
    return {
        "original_candidate_id": "risk_appetite",
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "comparison_label": label,
    }


def _guardrail(status: str):
    return {
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "guardrail_status": status,
    }


def _score(count: int = 100):
    return {
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "validation_eligible_record_count": count,
    }


def _quality(status: str = "PASS"):
    return {
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "data_quality_status": status,
    }


def _state(
    label: str,
    *,
    guardrail: str = "PASS",
    high_label: str = "HIGH_CONVICTION_EDGE_IMPROVED",
    quality: str = "PASS",
):
    return build_refined_candidate_state_recommendation_matrix(
        comparison_rows=[_comparison(label)],
        guardrail_rows=[_guardrail(guardrail)],
        high_conviction_rows=[
            {
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "high_conviction_outcome_label": high_label,
            }
        ],
        refined_scorecards=[_score()],
        refined_data_quality_rows=[_quality(quality)],
    )[0]


def test_improved_within_guardrails_recommends_owner_review_candidate() -> None:
    row = _state("REFINED_IMPROVED_WITHIN_GUARDRAILS")

    assert row["recommended_research_status"] == (
        "REFINED_ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE"
    )
    assert row["owner_review_candidate_recommendation"] is True
    assert row["promotion_allowed"] is False


def test_improved_but_false_cost_worse_continues_research() -> None:
    row = _state("REFINED_IMPROVED_BUT_FALSE_COST_WORSE")

    assert row["recommended_research_status"] == (
        "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH"
    )


def test_no_measurable_improvement_is_inconclusive() -> None:
    row = _state("REFINED_NO_MEASURABLE_IMPROVEMENT")

    assert row["recommended_research_status"] == "REFINED_ACTUAL_PATH_VALIDATED_INCONCLUSIVE"


def test_worse_rejects_refined_candidate() -> None:
    row = _state("REFINED_WORSE")

    assert row["recommended_research_status"] == (
        "REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"
    )


def test_data_quality_fail_blocks_recommendation() -> None:
    row = _state("REFINED_IMPROVED_WITHIN_GUARDRAILS", quality="FAIL")

    assert row["recommended_research_status"] == "REFINED_DATA_QUALITY_BLOCKED"
    assert row["owner_review_candidate_recommendation"] is False


def test_state_recommendations_do_not_emit_forbidden_readiness_labels() -> None:
    statuses = [
        _state("REFINED_IMPROVED_WITHIN_GUARDRAILS")["recommended_research_status"],
        _state("REFINED_WORSE")["recommended_research_status"],
        _state("REFINED_NO_MEASURABLE_IMPROVEMENT")["recommended_research_status"],
    ]

    assert "PROMOTION_READY" not in statuses
    assert "PAPER_SHADOW_READY" not in statuses
    assert "PRODUCTION_READY" not in statuses
