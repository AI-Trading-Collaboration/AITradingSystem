from __future__ import annotations

from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    build_scope_narrowed_state_recommendation_matrix,
)


def _state(
    *,
    comparison_label: str,
    sample: str = "SAMPLE_SUFFICIENT",
    quality: str = "PASS",
    false_cost: float = 0.0,
):
    return build_scope_narrowed_state_recommendation_matrix(
        comparison_rows=[
            {
                "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
                "usage_role": "confirmation_only",
                "comparison_label": comparison_label,
                "active_vs_inactive_score_delta": 0.1,
                "active_false_cost": false_cost,
                "active_eligible_count": 500,
            }
        ],
        scorecards=[],
        sample_rows=[
            {
                "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
                "sample_sufficiency_status": sample,
            }
        ],
        data_quality_rows=[
            {
                "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
                "data_quality_status": quality,
            }
        ],
    )[0]


def test_local_edge_positive_can_be_forward_observe_candidate() -> None:
    row = _state(comparison_label="ACTIVE_SCOPE_OUTPERFORMS_REFERENCE")

    assert row["recommended_research_status"] in {
        "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE",
        "SCOPE_NARROWED_VALIDATED_LOCAL_EDGE",
        "SCOPE_NARROWED_VALIDATED_CONTINUE_RESEARCH",
    }
    assert row["promotion_allowed"] is False


def test_sample_insufficient_blocks_state() -> None:
    row = _state(
        comparison_label="ACTIVE_SCOPE_OUTPERFORMS_REFERENCE",
        sample="SAMPLE_BLOCKED",
    )

    assert row["recommended_research_status"] == "SCOPE_NARROWED_SAMPLE_BLOCKED"


def test_data_quality_fail_blocks_state() -> None:
    row = _state(
        comparison_label="ACTIVE_SCOPE_OUTPERFORMS_REFERENCE",
        quality="FAIL",
    )

    assert row["recommended_research_status"] == "SCOPE_NARROWED_DATA_QUALITY_BLOCKED"


def test_false_cost_high_rejects_or_inconclusive() -> None:
    row = _state(
        comparison_label="ACTIVE_SCOPE_OUTPERFORMS_REFERENCE",
        false_cost=100.0,
    )

    assert row["recommended_research_status"] == "SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED"


def test_recommendations_never_emit_readiness_states() -> None:
    row = _state(comparison_label="ACTIVE_SCOPE_OUTPERFORMS_REFERENCE")

    assert row["recommended_research_status"] not in {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
    }
    assert row["paper_shadow_allowed"] is False
    assert row["production_allowed"] is False
    assert row["broker_action"] == "none"
