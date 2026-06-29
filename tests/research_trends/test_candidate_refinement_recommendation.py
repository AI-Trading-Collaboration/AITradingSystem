from __future__ import annotations

from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    BANNED_RECOMMENDATIONS,
    build_candidate_refinement_recommendation_matrix,
)


def _recommend(candidate: str, **kwargs: object) -> dict[str, object]:
    rows = build_candidate_refinement_recommendation_matrix(
        candidate_ids=[candidate],
        density_rows=kwargs.get("density_rows", []),
        horizon_asset_rows=kwargs.get("horizon_asset_rows", []),
        direction_rows=kwargs.get("direction_rows", []),
        false_cost_rows=kwargs.get("false_cost_rows", []),
        overlap_rows=kwargs.get("overlap_rows", []),
        data_quality_impact_rows=kwargs.get("data_quality_impact_rows", []),
        regime_rows=kwargs.get("regime_rows", []),
        scorecards=kwargs.get("scorecards", []),
        state_recommendations=[],
    )
    return rows[0]


def test_over_neutralized_recommends_direction_mapping_refinement() -> None:
    row = _recommend(
        "c1",
        density_rows=[{"candidate_id": "c1", "diagnostic_label": "POSSIBLE_OVER_NEUTRALIZATION"}],
    )

    assert row["primary_inconclusive_reason"] == "OVER_NEUTRALIZED_SIGNAL"
    assert row["recommended_next_action"] == "REFINE_SIGNAL_DIRECTION_MAPPING"


def test_false_risk_off_cost_recommends_risk_cap_usage_refinement() -> None:
    row = _recommend(
        "c1",
        false_cost_rows=[
            {"candidate_id": "c1", "diagnostic_label": "FALSE_RISK_OFF_COST_TOO_HIGH"}
        ],
    )

    assert row["primary_inconclusive_reason"] == "FALSE_RISK_OFF_COST_TOO_HIGH"
    assert row["recommended_next_action"] == "REFINE_RISK_CAP_USAGE"


def test_horizon_mismatch_recommends_horizon_targeting() -> None:
    row = _recommend(
        "c1",
        horizon_asset_rows=[{"candidate_id": "c1", "diagnostic_labels": ["MIXED_BY_HORIZON"]}],
    )

    assert row["primary_inconclusive_reason"] == "HORIZON_MISMATCH"
    assert row["recommended_next_action"] == "REFINE_HORIZON_TARGETING"


def test_high_redundancy_recommends_merge_or_drop() -> None:
    row = _recommend(
        "c1",
        overlap_rows=[
            {
                "candidate_id_left": "c1",
                "candidate_id_right": "c2",
                "diagnostic_label": "HIGHLY_REDUNDANT",
            }
        ],
    )

    assert row["primary_inconclusive_reason"] == "CANDIDATE_REDUNDANT_WITH_OTHER_SIGNALS"
    assert row["recommended_next_action"] == "MERGE_OR_DROP_REDUNDANT_CANDIDATE"


def test_no_measurable_edge_recommends_reject_without_promotion_states() -> None:
    row = _recommend(
        "c1",
        scorecards=[
            {
                "candidate_id": "c1",
                "alignment_rate": 0.2,
                "confidence_weighted_alignment_score": -0.1,
                "validation_eligible_record_count": 200,
            }
        ],
    )

    assert row["primary_inconclusive_reason"] == "NO_MEASURABLE_EDGE"
    assert row["recommended_next_action"] == "REJECT_CURRENT_FORM"
    assert row["recommended_next_action"] not in BANNED_RECOMMENDATIONS
    assert row["promotion_allowed"] is False
    assert row["paper_shadow_allowed"] is False
    assert row["production_allowed"] is False
