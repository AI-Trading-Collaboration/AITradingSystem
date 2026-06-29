from __future__ import annotations

from ai_trading_system.refined_candidate_actual_path_validation import (
    build_refined_high_conviction_outcome_drilldown,
)


def _row(
    *,
    high: bool,
    score: float,
    error_type: str = "no_error",
    forward_return: float = 0.02,
    drawdown: float = -0.01,
):
    return {
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "original_candidate_id": "risk_appetite",
        "target_asset": "QQQ",
        "horizon": "5d",
        "validation_eligible": True,
        "high_conviction_flag": high,
        "alignment_score": score,
        "error_type": error_type,
        "actual_forward_return": forward_return,
        "actual_max_drawdown": drawdown,
        "actual_realized_volatility": 0.15,
    }


def test_high_conviction_records_are_aggregated_separately() -> None:
    rows = [
        _row(high=True, score=1.0),
        _row(high=True, score=1.0),
        _row(high=False, score=-1.0, error_type="false_risk_on", forward_return=-0.03),
    ]

    drilldown = build_refined_high_conviction_outcome_drilldown(
        rows,
        minimum_high_conviction_records=2,
    )

    assert drilldown[0]["high_conviction_record_count"] == 2
    assert drilldown[0]["high_conviction_eligible_count"] == 2
    assert drilldown[0]["high_conviction_alignment_rate"] == 1.0
    assert drilldown[0]["non_high_conviction_alignment_rate"] == 0.0
    assert drilldown[0]["high_vs_non_high_alignment_delta"] == 1.0
    assert drilldown[0]["high_conviction_outcome_label"] == "HIGH_CONVICTION_EDGE_IMPROVED"


def test_high_conviction_false_risk_on_cost_calculates() -> None:
    rows = [
        _row(
            high=True,
            score=-1.0,
            error_type="false_risk_on",
            forward_return=-0.04,
            drawdown=-0.05,
        ),
        _row(high=True, score=1.0),
    ]

    drilldown = build_refined_high_conviction_outcome_drilldown(
        rows,
        minimum_high_conviction_records=2,
    )

    assert drilldown[0]["high_conviction_false_risk_on_count"] == 1
    assert drilldown[0]["high_conviction_false_risk_on_cost"] == 0.09


def test_high_conviction_false_risk_off_cost_calculates() -> None:
    rows = [
        _row(
            high=True,
            score=-1.0,
            error_type="false_risk_off",
            forward_return=0.05,
            drawdown=-0.01,
        ),
        _row(high=True, score=1.0),
    ]

    drilldown = build_refined_high_conviction_outcome_drilldown(
        rows,
        minimum_high_conviction_records=2,
    )

    assert drilldown[0]["high_conviction_false_risk_off_count"] == 1
    assert drilldown[0]["high_conviction_false_risk_off_cost"] == 0.06


def test_high_conviction_insufficient_sample_label() -> None:
    drilldown = build_refined_high_conviction_outcome_drilldown(
        [_row(high=True, score=1.0)],
        minimum_high_conviction_records=2,
    )

    assert drilldown[0]["high_conviction_outcome_label"] == (
        "HIGH_CONVICTION_INSUFFICIENT_SAMPLE"
    )


def test_high_conviction_false_cost_worse_label() -> None:
    rows = [
        _row(
            high=True,
            score=-1.0,
            error_type="false_risk_on",
            forward_return=-0.04,
            drawdown=-0.05,
        ),
        _row(high=True, score=1.0),
        _row(high=False, score=1.0),
        _row(high=False, score=1.0),
    ]

    drilldown = build_refined_high_conviction_outcome_drilldown(
        rows,
        minimum_high_conviction_records=2,
    )

    assert drilldown[0]["high_conviction_outcome_label"] == (
        "HIGH_CONVICTION_FALSE_COST_WORSE"
    )
