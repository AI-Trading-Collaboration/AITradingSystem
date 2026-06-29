from __future__ import annotations

from ai_trading_system.regenerated_candidate_actual_path_validation import (
    build_candidate_scorecard,
)


def _row(
    *,
    score: float,
    error_type: str = "no_error",
    eligible: bool = True,
    warning: bool = False,
    direction: str = "risk_on",
    horizon: str = "5d",
):
    return {
        "candidate_id": "risk_appetite",
        "signal_direction": direction,
        "horizon": horizon,
        "validation_eligible": eligible,
        "data_quality_warning": warning,
        "alignment_score": score,
        "signal_confidence": 0.8,
        "error_type": error_type,
        "actual_forward_return": 0.01 if score >= 0 else -0.03,
        "actual_max_drawdown": -0.01 if score >= 0 else -0.05,
        "actual_realized_volatility": 0.15,
    }


def test_scorecard_calculates_alignment_and_weighted_score() -> None:
    rows = [_row(score=1.0) for _ in range(24)] + [_row(score=-1.0) for _ in range(6)]

    scorecard = build_candidate_scorecard("risk_appetite", rows)

    assert scorecard["record_count"] == 30
    assert scorecard["validation_eligible_record_count"] == 30
    assert scorecard["alignment_rate"] == 0.8
    assert scorecard["confidence_weighted_alignment_score"] == 0.6


def test_scorecard_counts_false_risk_errors_and_warnings() -> None:
    rows = (
        [_row(score=-1.0, error_type="false_risk_on") for _ in range(2)]
        + [_row(score=-1.0, error_type="false_risk_off", direction="risk_off") for _ in range(3)]
        + [_row(score=1.0, warning=True) for _ in range(25)]
    )

    scorecard = build_candidate_scorecard("risk_appetite", rows)

    assert scorecard["false_risk_on_count"] == 2
    assert scorecard["false_risk_off_count"] == 3
    assert scorecard["data_quality_warning_count"] == 25


def test_owner_review_candidate_does_not_allow_promotion() -> None:
    rows = (
        [_row(score=1.0, horizon="5d") for _ in range(12)]
        + [_row(score=1.0, horizon="10d") for _ in range(12)]
        + [_row(score=1.0, horizon="20d") for _ in range(12)]
    )

    scorecard = build_candidate_scorecard("risk_appetite", rows)

    assert scorecard["recommended_research_status"] == (
        "ACTUAL_PATH_VALIDATED_OWNER_REVIEW_CANDIDATE"
    )
    assert scorecard["owner_review_required"] is True
    assert scorecard["promotion_allowed"] is False


def test_inconclusive_candidate_does_not_allow_paper_shadow() -> None:
    rows = [_row(score=1.0) for _ in range(3)]

    scorecard = build_candidate_scorecard("risk_appetite", rows)

    assert scorecard["recommended_research_status"] == "ACTUAL_PATH_VALIDATED_INCONCLUSIVE"
    assert scorecard["paper_shadow_allowed"] is False
