from __future__ import annotations

from ai_trading_system.refined_candidate_actual_path_validation import (
    build_original_vs_refined_actual_path_comparison,
)


def _original_score(**overrides):
    row = {
        "candidate_id": "risk_appetite",
        "record_count": 10,
        "validation_eligible_record_count": 10,
        "alignment_rate": 0.5,
        "weighted_alignment_score": 0.1,
        "confidence_weighted_alignment_score": 0.1,
        "false_risk_on_cost": 0.0,
        "false_risk_off_cost": 0.0,
    }
    row.update(overrides)
    return row


def _refined_score(**overrides):
    row = {
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "original_candidate_id": "risk_appetite",
        "candidate_id": "risk_appetite_refined_confidence_v1",
        "record_count": 10,
        "validation_eligible_record_count": 10,
        "alignment_rate": 0.6,
        "weighted_alignment_score": 0.2,
        "confidence_weighted_alignment_score": 0.2,
        "false_risk_on_cost": 0.0,
        "false_risk_off_cost": 0.0,
    }
    row.update(overrides)
    return row


def _outcome(candidate_id: str, *, direction: str = "risk_on", confidence: float = 0.7):
    return {
        "candidate_id": candidate_id,
        "refined_candidate_id": candidate_id,
        "signal_direction": direction,
        "signal_confidence": confidence,
        "validation_eligible": True,
        "alignment_score": 1.0,
        "high_conviction_flag": confidence >= 0.7,
    }


def test_comparison_maps_original_to_refined_and_calculates_deltas() -> None:
    comparison = build_original_vs_refined_actual_path_comparison(
        original_scorecards=[_original_score()],
        original_outcome_rows=[_outcome("risk_appetite", direction="neutral")],
        refined_scorecards=[_refined_score()],
        refined_outcome_rows=[
            {
                **_outcome("risk_appetite_refined_confidence_v1"),
                "original_candidate_id": "risk_appetite",
            }
        ],
        guardrail_status_by_refined={"risk_appetite_refined_confidence_v1": "PASS"},
    )

    row = comparison[0]
    assert row["original_candidate_id"] == "risk_appetite"
    assert row["refined_candidate_id"] == "risk_appetite_refined_confidence_v1"
    assert row["alignment_rate_delta"] == 0.1
    assert row["confidence_weighted_score_delta"] == 0.1
    assert row["false_risk_on_cost_delta"] == 0.0
    assert row["false_risk_off_cost_delta"] == 0.0
    assert row["comparison_label"] == "REFINED_IMPROVED_WITHIN_GUARDRAILS"


def test_comparison_labels_improved_but_false_cost_worse() -> None:
    comparison = build_original_vs_refined_actual_path_comparison(
        original_scorecards=[_original_score()],
        original_outcome_rows=[],
        refined_scorecards=[_refined_score(false_risk_on_cost=0.2)],
        refined_outcome_rows=[],
        guardrail_status_by_refined={"risk_appetite_refined_confidence_v1": "PASS"},
    )

    assert comparison[0]["comparison_label"] == "REFINED_IMPROVED_BUT_FALSE_COST_WORSE"
    assert comparison[0]["false_risk_on_cost_delta"] == 0.2


def test_comparison_labels_no_measurable_improvement() -> None:
    comparison = build_original_vs_refined_actual_path_comparison(
        original_scorecards=[_original_score(confidence_weighted_alignment_score=0.2)],
        original_outcome_rows=[],
        refined_scorecards=[_refined_score(confidence_weighted_alignment_score=0.205)],
        refined_outcome_rows=[],
        guardrail_status_by_refined={"risk_appetite_refined_confidence_v1": "PASS"},
    )

    assert comparison[0]["comparison_label"] == "REFINED_NO_MEASURABLE_IMPROVEMENT"


def test_comparison_uses_refined_delta_high_confidence_ratios_when_available() -> None:
    comparison = build_original_vs_refined_actual_path_comparison(
        original_scorecards=[_original_score()],
        original_outcome_rows=[_outcome("risk_appetite", confidence=0.7)],
        refined_scorecards=[_refined_score()],
        refined_outcome_rows=[
            {
                **_outcome("risk_appetite_refined_confidence_v1", confidence=0.7),
                "original_candidate_id": "risk_appetite",
            }
        ],
        refined_delta_rows=[
            {
                "original_candidate_id": "risk_appetite",
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "high_confidence_ratio_original": 0.0,
                "high_confidence_ratio_refined": 0.35,
            }
        ],
        guardrail_status_by_refined={"risk_appetite_refined_confidence_v1": "PASS"},
    )

    assert comparison[0]["high_confidence_ratio_original"] == 0.0
    assert comparison[0]["high_confidence_ratio_refined"] == 0.35
