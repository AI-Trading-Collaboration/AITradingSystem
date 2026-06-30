from __future__ import annotations

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    build_candidate_scope_narrowing_recommendation_matrix,
)


def _local(candidate_id: str, original_id: str, label: str):
    return {
        "refined_candidate_id": candidate_id,
        "original_candidate_id": original_id,
        "research_status_from_2289": "REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH",
        "local_edge_label": label,
    }


def test_local_edge_present_scope_narrows_and_regenerates() -> None:
    rows = build_candidate_scope_narrowing_recommendation_matrix(
        local_edge_rows=[
            _local(
                "baseline_plus_trend_structure_refined_confidence_v1",
                "baseline_plus_trend_structure",
                "LOCAL_EDGE_PRESENT",
            )
        ],
        asset_rows=[],
        horizon_rows=[],
        direction_rows=[
            {
                "refined_candidate_id": "baseline_plus_trend_structure_refined_confidence_v1",
                "signal_direction": "trend_confirming",
                "direction_scope_label": "DIRECTION_CONFIRMATION_ONLY",
            },
            {
                "refined_candidate_id": "baseline_plus_trend_structure_refined_confidence_v1",
                "signal_direction": "risk_off",
                "direction_scope_label": "DIRECTION_KEEP",
            }
        ],
        high_conviction_rows=[
            {
                "refined_candidate_id": "baseline_plus_trend_structure_refined_confidence_v1",
                "high_conviction_scope_label": "HIGH_CONVICTION_SCOPE_KEEP_ONLY",
            }
        ],
        regime_rows=[],
        reject_candidates=(),
    )

    assert rows[0]["recommended_scope_action"] == "SCOPE_NARROW_AND_REGENERATE"
    assert rows[0]["usage_recommendation"] == "confirmation_only"
    assert rows[0]["next_task_recommendation"] == (
        "TRADING-2291_Scope_Narrowed_Candidate_Regeneration"
    )


def test_no_edge_recommends_family_redesign() -> None:
    rows = build_candidate_scope_narrowing_recommendation_matrix(
        local_edge_rows=[
            _local("generic_refined_confidence_v1", "generic", "LOCAL_EDGE_NOT_FOUND")
        ],
        asset_rows=[],
        horizon_rows=[],
        direction_rows=[],
        high_conviction_rows=[],
        regime_rows=[],
        reject_candidates=(),
    )

    assert rows[0]["recommended_scope_action"] == "REDESIGN_CANDIDATE_FAMILY"


def test_risk_appetite_rejects_current_form() -> None:
    rows = build_candidate_scope_narrowing_recommendation_matrix(
        local_edge_rows=[
            _local(
                "risk_appetite_refined_confidence_v1",
                "risk_appetite",
                "LOCAL_EDGE_NOT_FOUND",
            )
        ],
        asset_rows=[],
        horizon_rows=[],
        direction_rows=[],
        high_conviction_rows=[],
        regime_rows=[],
        reject_candidates=("risk_appetite_refined_confidence_v1",),
    )

    assert rows[0]["recommended_scope_action"] == "REJECT_CURRENT_FORM"
    assert rows[0]["usage_recommendation"] == "reject"


def test_volatility_regime_usage_is_risk_cap() -> None:
    rows = build_candidate_scope_narrowing_recommendation_matrix(
        local_edge_rows=[
            _local(
                "volatility_regime_refined_confidence_v1",
                "volatility_regime",
                "LOCAL_EDGE_PRESENT",
            )
        ],
        asset_rows=[],
        horizon_rows=[],
        direction_rows=[
            {
                "refined_candidate_id": "volatility_regime_refined_confidence_v1",
                "signal_direction": "risk_off",
                "direction_scope_label": "DIRECTION_RISK_CAP_ONLY",
            }
        ],
        high_conviction_rows=[
            {
                "refined_candidate_id": "volatility_regime_refined_confidence_v1",
                "high_conviction_scope_label": "HIGH_CONVICTION_SCOPE_KEEP_ONLY",
            }
        ],
        regime_rows=[],
        reject_candidates=(),
    )

    assert rows[0]["usage_recommendation"] == "risk_cap_only"


def test_scope_recommendation_never_emits_ready_states() -> None:
    rows = build_candidate_scope_narrowing_recommendation_matrix(
        local_edge_rows=[
            _local("generic_refined_confidence_v1", "generic", "LOCAL_EDGE_NOT_FOUND")
        ],
        asset_rows=[],
        horizon_rows=[],
        direction_rows=[],
        high_conviction_rows=[],
        regime_rows=[],
        reject_candidates=(),
    )

    assert "PROMOTION_READY" not in str(rows)
    assert "PAPER_SHADOW_READY" not in str(rows)
    assert "PRODUCTION_READY" not in str(rows)
