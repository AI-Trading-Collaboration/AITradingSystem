from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_refined_candidate_regeneration_fixture

from ai_trading_system.refined_candidate_actual_path_validation import (
    RefinedCandidateActualPathValidationError,
    load_refined_candidate_actual_path_inputs,
)

REFINED_CANDIDATES = (
    "baseline_plus_trend_structure_refined_confidence_v1,"
    "risk_appetite_refined_confidence_v1,"
    "volatility_regime_refined_confidence_v1"
)


def test_loader_reads_refined_original_and_guardrail_inputs(tmp_path: Path) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)

    inputs = load_refined_candidate_actual_path_inputs(
        refined_generator_dir=fixture["refined_generator_dir"],
        original_validation_dir=fixture["validation_dir"],
        refinement_plan_dir=fixture["refinement_plan_dir"],
        candidates=REFINED_CANDIDATES,
    )

    assert set(inputs.refined_artifacts) == {
        "baseline_plus_trend_structure_refined_confidence_v1",
        "risk_appetite_refined_confidence_v1",
        "volatility_regime_refined_confidence_v1",
    }
    assert inputs.original_validation["scorecards"]
    assert inputs.original_validation["outcome_rows"]
    assert inputs.guardrail_rows


def test_loader_fails_when_refined_prediction_artifact_missing(tmp_path: Path) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    (
        fixture["refined_generator_dir"]
        / "risk_appetite_refined_confidence_v1"
        / "refined_candidate_prediction_artifact.json"
    ).unlink()

    with pytest.raises(RefinedCandidateActualPathValidationError, match="missing"):
        load_refined_candidate_actual_path_inputs(
            refined_generator_dir=fixture["refined_generator_dir"],
            original_validation_dir=fixture["validation_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_when_original_scorecard_missing(tmp_path: Path) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    (fixture["validation_dir"] / "candidate_validation_scorecard.json").unlink()

    with pytest.raises(RefinedCandidateActualPathValidationError, match="original"):
        load_refined_candidate_actual_path_inputs(
            refined_generator_dir=fixture["refined_generator_dir"],
            original_validation_dir=fixture["validation_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_when_guardrail_matrix_missing(tmp_path: Path) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    (fixture["refinement_plan_dir"] / "candidate_guardrail_matrix.json").unlink()

    with pytest.raises(RefinedCandidateActualPathValidationError, match="guardrail"):
        load_refined_candidate_actual_path_inputs(
            refined_generator_dir=fixture["refined_generator_dir"],
            original_validation_dir=fixture["validation_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_promotion_allowed_refined_artifact(tmp_path: Path) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    path = (
        fixture["refined_generator_dir"]
        / "volatility_regime_refined_confidence_v1"
        / "refined_candidate_prediction_artifact.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RefinedCandidateActualPathValidationError, match="promotion_allowed"):
        load_refined_candidate_actual_path_inputs(
            refined_generator_dir=fixture["refined_generator_dir"],
            original_validation_dir=fixture["validation_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates="volatility_regime_refined_confidence_v1",
        )


def test_loader_fails_broker_action_refined_artifact(tmp_path: Path) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    path = (
        fixture["refined_generator_dir"]
        / "volatility_regime_refined_confidence_v1"
        / "refined_candidate_prediction_artifact.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RefinedCandidateActualPathValidationError, match="broker_action"):
        load_refined_candidate_actual_path_inputs(
            refined_generator_dir=fixture["refined_generator_dir"],
            original_validation_dir=fixture["validation_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates="volatility_regime_refined_confidence_v1",
        )
