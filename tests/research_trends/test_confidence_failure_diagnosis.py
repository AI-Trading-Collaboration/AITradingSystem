from __future__ import annotations

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    CandidateGeneratorContextRow,
    build_candidate_confidence_failure_diagnosis_matrix,
)


def _record(
    *,
    candidate_id: str = "risk_appetite",
    signal_confidence: float = 0.5,
    signal_direction: str = "neutral",
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "target_asset": "QQQ",
        "horizon": "5d",
        "signal_confidence": signal_confidence,
        "signal_direction": signal_direction,
        "validation_eligible": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_low_confidence_signal_generates_specific_failure_mode() -> None:
    rows = [_record(signal_confidence=0.5, signal_direction="risk_on") for _ in range(10)]

    diagnosis = build_candidate_confidence_failure_diagnosis_matrix(
        candidate_ids=["risk_appetite"],
        outcome_rows=rows,
        recommendation_rows=[
            {
                "candidate_id": "risk_appetite",
                "primary_inconclusive_reason": "LOW_CONFIDENCE_SIGNAL",
                "recommended_next_action": "REFINE_CONFIDENCE_SCALING",
            }
        ],
    )[0]

    assert diagnosis["dominant_failure_mode"] == "INSUFFICIENT_HIGH_CONVICTION_RULE"
    assert diagnosis["high_confidence_ratio"] == 0.0


def test_high_neutral_ratio_marks_neutral_band_too_wide() -> None:
    rows = [_record(signal_direction="neutral") for _ in range(8)]
    rows.extend(_record(signal_direction="risk_on") for _ in range(2))

    diagnosis = build_candidate_confidence_failure_diagnosis_matrix(
        candidate_ids=["baseline_plus_trend_structure"],
        outcome_rows=rows,
        recommendation_rows=[],
    )[0]

    assert "NEUTRAL_BAND_TOO_WIDE" in diagnosis["secondary_failure_modes"]


def test_low_high_confidence_ratio_marks_insufficient_high_conviction() -> None:
    rows = [_record(signal_confidence=0.62, signal_direction="risk_on") for _ in range(10)]

    diagnosis = build_candidate_confidence_failure_diagnosis_matrix(
        candidate_ids=["risk_appetite"],
        outcome_rows=rows,
        recommendation_rows=[],
    )[0]

    assert diagnosis["dominant_failure_mode"] == "INSUFFICIENT_HIGH_CONVICTION_RULE"


def test_low_confidence_cap_marks_over_conservative_cap() -> None:
    rows = [_record(signal_confidence=0.6, signal_direction="risk_on") for _ in range(10)]

    diagnosis = build_candidate_confidence_failure_diagnosis_matrix(
        candidate_ids=["volatility_regime"],
        outcome_rows=rows,
        recommendation_rows=[],
    )[0]

    assert "OVER_CONSERVATIVE_CONFIDENCE_CAP" in diagnosis["secondary_failure_modes"]


def test_missing_proxy_penalty_marks_risk_appetite_failure() -> None:
    rows = [_record(signal_confidence=0.5, signal_direction="risk_on") for _ in range(10)]
    context = CandidateGeneratorContextRow(
        candidate_id="risk_appetite",
        status="complete",
        missing_artifacts=(),
        signal_names=("risk_appetite_score",),
        required_inputs=("QQQ_adjusted_close",),
        missing_inputs=("GLD", "UUP"),
        proxy_input_used=True,
        proxy_limitations=(),
    )

    diagnosis = build_candidate_confidence_failure_diagnosis_matrix(
        candidate_ids=["risk_appetite"],
        outcome_rows=rows,
        recommendation_rows=[],
        generator_context_rows=[context],
    )[0]

    assert "OVER_PENALIZED_MISSING_PROXY_INPUT" in diagnosis["secondary_failure_modes"]
