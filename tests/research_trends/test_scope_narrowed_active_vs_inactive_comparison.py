from __future__ import annotations

from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    build_scope_narrowed_active_vs_inactive_comparison,
)


def _row(score: float, *, active: bool = True):
    return {
        "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
        "usage_role": "confirmation_only",
        "target_asset": "QQQ",
        "horizon": "5d",
        "validation_eligible": True,
        "alignment_score": score,
        "error_type": "no_error" if score >= 0 else "false_confirmation",
        "actual_forward_return": score * 0.02,
        "actual_max_drawdown": -0.01 if score >= 0 else -0.05,
        "actual_realized_volatility": 0.1,
        "scope_active": active,
    }


def _sample(status: str = "SAMPLE_SUFFICIENT"):
    return {
        "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
        "sample_sufficiency_status": status,
    }


def _quality(status: str = "PASS"):
    return {
        "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
        "data_quality_status": status,
    }


def test_active_and_inactive_scores_and_delta() -> None:
    comparison = build_scope_narrowed_active_vs_inactive_comparison(
        active_outcome_rows=[_row(1.0), _row(0.5)],
        inactive_outcome_rows=[_row(0.0, active=False), _row(-1.0, active=False)],
        scorecards=[],
        sample_rows=[_sample()],
        data_quality_rows=[_quality()],
    )[0]

    assert comparison["active_alignment_score"] == 0.75
    assert comparison["inactive_reference_score"] == -0.5
    assert comparison["active_vs_inactive_score_delta"] == 1.25
    assert comparison["comparison_label"] == "ACTIVE_SCOPE_OUTPERFORMS_REFERENCE"


def test_insufficient_active_sample_label() -> None:
    comparison = build_scope_narrowed_active_vs_inactive_comparison(
        active_outcome_rows=[_row(1.0)],
        inactive_outcome_rows=[_row(0.0, active=False)],
        scorecards=[],
        sample_rows=[_sample("SAMPLE_BLOCKED")],
        data_quality_rows=[_quality()],
    )[0]

    assert comparison["comparison_label"] == "ACTIVE_SCOPE_INSUFFICIENT_SAMPLE"


def test_active_scope_worse_label() -> None:
    comparison = build_scope_narrowed_active_vs_inactive_comparison(
        active_outcome_rows=[_row(-1.0)],
        inactive_outcome_rows=[_row(1.0, active=False)],
        scorecards=[],
        sample_rows=[_sample()],
        data_quality_rows=[_quality()],
    )[0]

    assert comparison["comparison_label"] == "ACTIVE_SCOPE_WORSE"
