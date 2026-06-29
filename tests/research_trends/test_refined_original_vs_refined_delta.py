from __future__ import annotations

from ai_trading_system.refined_candidate_generators_regenerate import (
    ParameterApplication,
    build_refined_original_vs_refined_delta,
)


def test_original_vs_refined_delta_calculates_distribution_changes() -> None:
    application = ParameterApplication(
        candidate_id="risk_appetite",
        refined_candidate_id="risk_appetite_refined_confidence_v1",
        selected_proposal_ids=("p1",),
        selected_parameter_set_ids=("s1",),
        rejected_parameter_set_ids=(),
        rejection_reasons={},
        applied_neutral_band_width=0.15,
        applied_confidence_scale_factor=1.5,
        applied_confidence_cap=0.85,
        applied_confidence_floor=0.12,
        applied_high_confidence_threshold=0.65,
        applied_low_confidence_threshold=0.35,
        applied_directional_activation_threshold=0.2,
        applied_missing_input_penalty=0.1,
        expected_high_confidence_ratio=0.2,
        expected_directional_signal_ratio=0.3,
        max_high_confidence_ratio=0.35,
        guardrail_profile="balanced",
        guardrail_compliant=True,
    )
    original = [
        {"signal_confidence": 0.2, "signal_value": 0.0, "signal_direction": "neutral"},
        {"signal_confidence": 0.6, "signal_value": 0.8, "signal_direction": "risk_on"},
    ]
    refined = [
        {
            "refined_signal_confidence": 0.3,
            "refined_signal_value": 0.0,
            "signal_direction": "neutral",
            "direction_changed": False,
            "high_conviction_flag": False,
            "confidence_delta": 0.1,
        },
        {
            "refined_signal_confidence": 0.7,
            "refined_signal_value": 0.8,
            "signal_direction": "risk_on",
            "direction_changed": False,
            "high_conviction_flag": True,
            "confidence_delta": 0.1,
        },
    ]

    delta = build_refined_original_vs_refined_delta(
        original_candidate_id="risk_appetite",
        refined_candidate_id="risk_appetite_refined_confidence_v1",
        original_records=original,
        refined_records=refined,
        application=application,
    )

    assert delta["neutral_ratio_original"] == 0.5
    assert delta["directional_signal_ratio_refined"] == 0.5
    assert delta["high_confidence_ratio_original"] == 0.0
    assert delta["high_confidence_ratio_refined"] == 0.5
    assert delta["average_confidence_delta"] == 0.1
    forbidden = " ".join(delta.keys()).lower()
    assert "actual_path_improvement" not in forbidden
    assert "utility_improvement" not in forbidden
    assert delta["actual_path_validation_executed"] is False
