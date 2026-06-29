from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_confidence_scaling_refinement_plan_fixture

from ai_trading_system.refined_candidate_generators_regenerate import (
    RefinedCandidateRegenerationError,
    load_refined_candidate_regeneration_inputs,
)


def test_refined_regeneration_loader_reads_plan_and_original_artifacts(
    tmp_path: Path,
) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)

    inputs = load_refined_candidate_regeneration_inputs(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
    )

    assert inputs.summary["regeneration_executed"] is False
    assert inputs.proposal_rows
    assert inputs.parameter_grid_rows
    assert inputs.guardrail_rows
    assert inputs.implementation_rows
    assert set(inputs.original_artifacts) == {
        "baseline_plus_trend_structure",
        "risk_appetite",
        "volatility_regime",
    }


def test_refined_regeneration_loader_requires_plan_matrices(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    (fixture["refinement_plan_dir"] / "candidate_confidence_scaling_proposal_matrix.json").unlink()

    with pytest.raises(RefinedCandidateRegenerationError, match="missing"):
        load_refined_candidate_regeneration_inputs(
            refinement_plan_dir=fixture["refinement_plan_dir"],
            original_generator_dir=fixture["original_generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_refined_regeneration_loader_rejects_unsafe_original_artifact(
    tmp_path: Path,
) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    artifact_path = (
        fixture["original_generator_dir"]
        / "baseline_plus_trend_structure"
        / "candidate_prediction_artifact.json"
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["promotion_allowed"] = True
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(RefinedCandidateRegenerationError, match="promotion_allowed"):
        load_refined_candidate_regeneration_inputs(
            refinement_plan_dir=fixture["refinement_plan_dir"],
            original_generator_dir=fixture["original_generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_refined_regeneration_loader_rejects_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    artifact_path = (
        fixture["original_generator_dir"]
        / "risk_appetite"
        / "candidate_prediction_artifact.json"
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["broker_action"] = "buy"
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(RefinedCandidateRegenerationError, match="broker_action"):
        load_refined_candidate_regeneration_inputs(
            refinement_plan_dir=fixture["refinement_plan_dir"],
            original_generator_dir=fixture["original_generator_dir"],
            candidates="risk_appetite",
        )
