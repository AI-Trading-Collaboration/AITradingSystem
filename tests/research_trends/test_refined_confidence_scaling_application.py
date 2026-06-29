from __future__ import annotations

from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_confidence_scaling_refinement_plan_fixture

from ai_trading_system.refined_candidate_generators_regenerate import (
    _provenance,
    _scaled_confidence,
    apply_refined_confidence_scaling,
    load_refined_candidate_regeneration_inputs,
    select_refined_parameter_application,
)


def test_selected_parameter_set_applies_cap_floor_and_direction_threshold(
    tmp_path: Path,
) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    inputs = load_refined_candidate_regeneration_inputs(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure",
    )
    original = inputs.original_artifacts["baseline_plus_trend_structure"]
    application = select_refined_parameter_application(
        candidate_id="baseline_plus_trend_structure",
        inputs=inputs,
    )
    rows = apply_refined_confidence_scaling(
        original_records=original.prediction_records[:20],
        original=original,
        application=application,
        provenance_source_paths=["source"],
        provenance_source_hashes=["hash"],
        generated_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
        source_artifact_hash="source_hash",
        input_snapshot_hash="input_hash",
        feature_snapshot_hash="feature_hash",
    )

    assert rows
    for row in rows:
        assert row["refined_candidate_id"].endswith("_refined_confidence_v1")
        assert row["confidence_cap"] == application.applied_confidence_cap
        assert row["confidence_floor"] == application.applied_confidence_floor
        assert 0.0 <= row["refined_signal_confidence"] <= application.applied_confidence_cap
        assert row["directional_activation_threshold"] == (
            application.applied_directional_activation_threshold
        )


def test_missing_input_penalty_reduces_scaled_confidence(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    inputs = load_refined_candidate_regeneration_inputs(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="risk_appetite",
    )
    application = select_refined_parameter_application(candidate_id="risk_appetite", inputs=inputs)
    row = dict(inputs.original_artifacts["risk_appetite"].prediction_records[-1])
    row["signal_value"] = 0.9
    row["signal_confidence"] = 0.62
    clean = _scaled_confidence(
        original_signal_value=0.9,
        original_signal_confidence=0.62,
        refined_signal_direction="risk_on",
        provenance={},
        application=application,
    )
    missing = _scaled_confidence(
        original_signal_value=0.9,
        original_signal_confidence=0.62,
        refined_signal_direction="risk_on",
        provenance={**_provenance(row), "missing_inputs": ["TLT"]},
        application=application,
    )

    assert missing < clean


def test_guardrail_noncompliant_parameter_set_fails(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    inputs = load_refined_candidate_regeneration_inputs(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure",
    )
    implementation = next(
        row
        for row in inputs.implementation_rows
        if row["candidate_id"] == "baseline_plus_trend_structure"
    )
    selected_parameter_set_id = implementation["selected_parameter_set_ids"][0]
    for row in inputs.parameter_grid_rows:
        if row["parameter_set_id"] == selected_parameter_set_id:
            row["expected_high_confidence_ratio"] = 0.99

    with pytest.raises(Exception, match="guardrail"):
        select_refined_parameter_application(
            candidate_id="baseline_plus_trend_structure",
            inputs=inputs,
        )
