from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_confidence_scaling_refinement_plan_fixture

from ai_trading_system.refined_candidate_generators_regenerate import (
    ParameterApplication,
    _high_conviction_base_condition,
    run_refined_candidate_generators_regenerate,
)


def test_refined_high_conviction_flags_respect_guardrail(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    output_dir = tmp_path / "refined"

    run_refined_candidate_generators_regenerate(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="refined_regeneration",
        docs_root=tmp_path / "docs",
    )

    delta_summary = json.loads(
        (output_dir / "refined_original_vs_refined_delta_summary.json").read_text(
            encoding="utf-8"
        )
    )
    for row in delta_summary["rows"]:
        assert row["high_confidence_ratio_refined"] <= 0.35
        assert row["guardrail_compliant"] is True


def test_neutral_records_are_not_high_conviction(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    output_dir = tmp_path / "refined"

    run_refined_candidate_generators_regenerate(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="refined_regeneration",
        docs_root=tmp_path / "docs",
    )
    artifact = json.loads(
        (
            output_dir
            / "baseline_plus_trend_structure_refined_confidence_v1"
            / "refined_candidate_prediction_artifact.json"
        ).read_text(encoding="utf-8")
    )

    for record in artifact["prediction_records"]:
        if record["signal_direction"] == "neutral":
            assert record["high_conviction_flag"] is False


def test_directional_high_confidence_records_can_be_high_conviction(
) -> None:
    application = ParameterApplication(
        candidate_id="risk_appetite",
        refined_candidate_id="risk_appetite_refined_confidence_v1",
        selected_proposal_ids=("proposal",),
        selected_parameter_set_ids=("set",),
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

    assert _high_conviction_base_condition(
        {
            "refined_signal_confidence": 0.7,
            "refined_signal_value": 0.8,
            "signal_direction": "risk_on",
        },
        application,
    )
