from __future__ import annotations

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    classify_local_edge_label,
)


def test_local_edge_present_label() -> None:
    assert (
        classify_local_edge_label(
            research_status="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            guardrail_status="PASS",
            data_quality_status="PASS",
            high_conviction_eligible_count=1200,
            high_conviction_alignment_rate=0.58,
            high_vs_overall_alignment_delta=0.12,
            high_conviction_confidence_weighted_score=0.10,
            confidence_weighted_score=-0.02,
            false_risk_on_cost_delta=0.0,
            false_risk_off_cost_delta=0.0,
        )
        == "LOCAL_EDGE_PRESENT"
    )


def test_local_edge_weak_label() -> None:
    assert (
        classify_local_edge_label(
            research_status="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            guardrail_status="PASS",
            data_quality_status="PASS",
            high_conviction_eligible_count=1200,
            high_conviction_alignment_rate=0.48,
            high_vs_overall_alignment_delta=0.12,
            high_conviction_confidence_weighted_score=0.08,
            confidence_weighted_score=-0.02,
            false_risk_on_cost_delta=0.0,
            false_risk_off_cost_delta=0.0,
        )
        == "LOCAL_EDGE_WEAK"
    )


def test_local_edge_not_found_label() -> None:
    assert (
        classify_local_edge_label(
            research_status="REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED",
            guardrail_status="PASS",
            data_quality_status="PASS",
            high_conviction_eligible_count=0,
            high_conviction_alignment_rate=0.0,
            high_vs_overall_alignment_delta=0.0,
            high_conviction_confidence_weighted_score=0.0,
            confidence_weighted_score=-0.02,
            false_risk_on_cost_delta=0.0,
            false_risk_off_cost_delta=0.0,
            is_reject_candidate=True,
        )
        == "LOCAL_EDGE_NOT_FOUND"
    )


def test_local_edge_false_cost_blocked_label() -> None:
    assert (
        classify_local_edge_label(
            research_status="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            guardrail_status="PASS",
            data_quality_status="PASS",
            high_conviction_eligible_count=1200,
            high_conviction_alignment_rate=0.58,
            high_vs_overall_alignment_delta=0.12,
            high_conviction_confidence_weighted_score=0.10,
            confidence_weighted_score=-0.02,
            false_risk_on_cost_delta=0.01,
            false_risk_off_cost_delta=0.0,
        )
        == "LOCAL_EDGE_FALSE_COST_BLOCKED"
    )


def test_local_edge_data_quality_blocked_label() -> None:
    assert (
        classify_local_edge_label(
            research_status="REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
            guardrail_status="PASS",
            data_quality_status="FAIL",
            high_conviction_eligible_count=1200,
            high_conviction_alignment_rate=0.58,
            high_vs_overall_alignment_delta=0.12,
            high_conviction_confidence_weighted_score=0.10,
            confidence_weighted_score=-0.02,
            false_risk_on_cost_delta=0.0,
            false_risk_off_cost_delta=0.0,
        )
        == "LOCAL_EDGE_DATA_QUALITY_BLOCKED"
    )
