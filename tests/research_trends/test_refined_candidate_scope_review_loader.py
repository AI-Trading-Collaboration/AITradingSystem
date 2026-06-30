from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_refined_scope_review_input_fixture

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    RefinedCandidateScopeReviewError,
    load_refined_candidate_scope_review_inputs,
)

CANDIDATES = (
    "baseline_plus_trend_structure_refined_confidence_v1,"
    "risk_appetite_refined_confidence_v1,"
    "volatility_regime_refined_confidence_v1"
)


def _load(tmp_path: Path):
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    return (
        fixture,
        load_refined_candidate_scope_review_inputs(
            refined_validation_dir=fixture["refined_validation_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates=CANDIDATES,
            continue_research_candidates=(
                "baseline_plus_trend_structure_refined_confidence_v1,"
                "volatility_regime_refined_confidence_v1"
            ),
            reject_candidates="risk_appetite_refined_confidence_v1",
        ),
    )


def test_loader_reads_2289_2288_and_2287_inputs(tmp_path: Path) -> None:
    fixture, inputs = _load(tmp_path)

    assert inputs.refined_validation_dir == fixture["refined_validation_dir"]
    assert set(inputs.generator_candidate_payloads) == set(CANDIDATES.split(","))
    assert "guardrail_matrix" in inputs.refinement_plan_payloads
    assert inputs.scorecards
    assert inputs.high_conviction_rows
    assert inputs.comparison_rows
    assert inputs.outcome_rows


def test_loader_fails_when_scorecard_missing(tmp_path: Path) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    (fixture["refined_validation_dir"] / "refined_candidate_validation_scorecard.json").unlink()

    with pytest.raises(RefinedCandidateScopeReviewError, match="missing"):
        load_refined_candidate_scope_review_inputs(
            refined_validation_dir=fixture["refined_validation_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates=CANDIDATES,
            continue_research_candidates="baseline_plus_trend_structure_refined_confidence_v1",
            reject_candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_when_high_conviction_drilldown_missing(tmp_path: Path) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    (fixture["refined_validation_dir"] / "refined_high_conviction_outcome_drilldown.json").unlink()

    with pytest.raises(RefinedCandidateScopeReviewError, match="missing"):
        load_refined_candidate_scope_review_inputs(
            refined_validation_dir=fixture["refined_validation_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates=CANDIDATES,
            continue_research_candidates="baseline_plus_trend_structure_refined_confidence_v1",
            reject_candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_when_original_vs_refined_comparison_missing(tmp_path: Path) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    (fixture["refined_validation_dir"] / "original_vs_refined_actual_path_comparison.json").unlink()

    with pytest.raises(RefinedCandidateScopeReviewError, match="missing"):
        load_refined_candidate_scope_review_inputs(
            refined_validation_dir=fixture["refined_validation_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates=CANDIDATES,
            continue_research_candidates="baseline_plus_trend_structure_refined_confidence_v1",
            reject_candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_when_promotion_allowed_input(tmp_path: Path) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    path = fixture["refined_validation_dir"] / "refined_candidate_validation_scorecard.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["candidate_scorecards"][0]["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RefinedCandidateScopeReviewError, match="promotion_allowed"):
        load_refined_candidate_scope_review_inputs(
            refined_validation_dir=fixture["refined_validation_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates=CANDIDATES,
            continue_research_candidates="baseline_plus_trend_structure_refined_confidence_v1",
            reject_candidates="risk_appetite_refined_confidence_v1",
        )


def test_loader_fails_when_broker_action_input(tmp_path: Path) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    path = fixture["refined_validation_dir"] / "original_vs_refined_actual_path_comparison.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["rows"][0]["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RefinedCandidateScopeReviewError, match="broker_action"):
        load_refined_candidate_scope_review_inputs(
            refined_validation_dir=fixture["refined_validation_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refinement_plan_dir=fixture["refinement_plan_dir"],
            candidates=CANDIDATES,
            continue_research_candidates="baseline_plus_trend_structure_refined_confidence_v1",
            reject_candidates="risk_appetite_refined_confidence_v1",
        )
