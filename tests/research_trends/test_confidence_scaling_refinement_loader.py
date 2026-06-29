from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_regenerated_inconclusive_diagnostics_fixture

from ai_trading_system.candidate_confidence_scaling_refinement_plan import (
    CandidateConfidenceScalingRefinementPlanError,
    load_confidence_scaling_refinement_inputs,
)


def test_loader_reads_2286_2285_and_2284_inputs(tmp_path: Path) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)

    loaded = load_confidence_scaling_refinement_inputs(
        diagnostics_dir=fixture["diagnostics_dir"],
        validation_dir=fixture["validation_dir"],
        generator_dir=fixture["generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
    )

    assert loaded.density_rows
    assert loaded.outcome_rows
    assert loaded.scorecard_rows
    assert loaded.generator_context_status == "complete"


def test_loader_fails_when_diagnostics_summary_is_missing(tmp_path: Path) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)
    (fixture["diagnostics_dir"] / "inconclusive_diagnostics_summary.json").unlink()

    with pytest.raises(CandidateConfidenceScalingRefinementPlanError):
        load_confidence_scaling_refinement_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_actual_path_scorecard_is_missing(tmp_path: Path) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)
    (fixture["validation_dir"] / "candidate_validation_scorecard.json").unlink()

    with pytest.raises(CandidateConfidenceScalingRefinementPlanError):
        load_confidence_scaling_refinement_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_input_attempts_promotion(tmp_path: Path) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)
    path = fixture["diagnostics_dir"] / "inconclusive_diagnostics_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(CandidateConfidenceScalingRefinementPlanError):
        load_confidence_scaling_refinement_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_generator_artifact_attempts_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)
    path = (
        fixture["generator_dir"]
        / "baseline_plus_trend_structure"
        / "candidate_prediction_artifact.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(CandidateConfidenceScalingRefinementPlanError):
        load_confidence_scaling_refinement_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )
