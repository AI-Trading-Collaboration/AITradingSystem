from __future__ import annotations

from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_CAP_CANDIDATE_ID,
    build_scope_narrowed_sample_sufficiency_report,
    build_scope_narrowed_state_recommendation_matrix,
)


def _row(candidate_id: str, usage: str, *, asset: str = "QQQ", horizon: str = "5d"):
    return {
        "scope_narrowed_candidate_id": candidate_id,
        "usage_role": usage,
        "target_asset": asset,
        "horizon": horizon,
        "signal_direction": "trend_confirming"
        if usage == "confirmation_only"
        else "volatility_expansion",
        "high_conviction_flag": True,
        "validation_eligible": True,
        "alignment_score": 1.0,
    }


def test_confirmation_active_sample_sufficient() -> None:
    rows = [_row(CONFIRMATION_CANDIDATE_ID, "confirmation_only") for _ in range(500)]

    report = build_scope_narrowed_sample_sufficiency_report(
        rows,
        candidate_ids=[CONFIRMATION_CANDIDATE_ID],
        usage_by_candidate={CONFIRMATION_CANDIDATE_ID: "confirmation_only"},
    )[0]

    assert report["sample_sufficiency_status"] == "SAMPLE_SUFFICIENT"


def test_risk_cap_active_sample_sufficient() -> None:
    rows = [_row(RISK_CAP_CANDIDATE_ID, "risk_cap_only") for _ in range(100)]

    report = build_scope_narrowed_sample_sufficiency_report(
        rows,
        candidate_ids=[RISK_CAP_CANDIDATE_ID],
        usage_by_candidate={RISK_CAP_CANDIDATE_ID: "risk_cap_only"},
    )[0]

    assert report["sample_sufficiency_status"] == "SAMPLE_SUFFICIENT"


def test_sparse_asset_horizon_bucket_is_reported() -> None:
    rows = [
        *[
            _row(CONFIRMATION_CANDIDATE_ID, "confirmation_only", asset="QQQ")
            for _ in range(495)
        ],
        *[
            _row(CONFIRMATION_CANDIDATE_ID, "confirmation_only", asset="SPY")
            for _ in range(5)
        ],
    ]

    report = build_scope_narrowed_sample_sufficiency_report(
        rows,
        candidate_ids=[CONFIRMATION_CANDIDATE_ID],
        usage_by_candidate={CONFIRMATION_CANDIDATE_ID: "confirmation_only"},
    )[0]

    assert report["asset_horizon_sparse_bucket_count"] == 1
    assert report["sample_sufficiency_status"] == "SAMPLE_INSUFFICIENT_FOR_SUBGROUPS"


def test_sample_blocked_cannot_recommend_forward_observe() -> None:
    state = build_scope_narrowed_state_recommendation_matrix(
        comparison_rows=[
            {
                "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
                "usage_role": "confirmation_only",
                "comparison_label": "ACTIVE_SCOPE_OUTPERFORMS_REFERENCE",
                "active_vs_inactive_score_delta": 1.0,
                "active_false_cost": 0.0,
                "active_eligible_count": 10,
            }
        ],
        scorecards=[],
        sample_rows=[
            {
                "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
                "sample_sufficiency_status": "SAMPLE_BLOCKED",
            }
        ],
        data_quality_rows=[
            {
                "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
                "data_quality_status": "PASS",
            }
        ],
    )[0]

    assert state["recommended_research_status"] == "SCOPE_NARROWED_SAMPLE_BLOCKED"
    assert state["forward_observe_candidate_recommendation"] is False


def test_thin_but_usable_allows_only_continue_research() -> None:
    state = build_scope_narrowed_state_recommendation_matrix(
        comparison_rows=[
            {
                "scope_narrowed_candidate_id": RISK_CAP_CANDIDATE_ID,
                "usage_role": "risk_cap_only",
                "comparison_label": "ACTIVE_SCOPE_WEAKLY_BETTER",
                "active_vs_inactive_score_delta": 0.2,
                "active_false_cost": 0.0,
                "active_eligible_count": 75,
            }
        ],
        scorecards=[],
        sample_rows=[
            {
                "scope_narrowed_candidate_id": RISK_CAP_CANDIDATE_ID,
                "sample_sufficiency_status": "SAMPLE_THIN_BUT_USABLE",
            }
        ],
        data_quality_rows=[
            {
                "scope_narrowed_candidate_id": RISK_CAP_CANDIDATE_ID,
                "data_quality_status": "PASS",
            }
        ],
    )[0]

    assert state["recommended_research_status"] == "SCOPE_NARROWED_VALIDATED_CONTINUE_RESEARCH"
    assert state["forward_observe_candidate_recommendation"] is False
