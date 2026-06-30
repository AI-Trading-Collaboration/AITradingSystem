from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    ScopeNarrowedCandidateRegenerationError,
    load_scope_narrowed_candidate_regeneration_inputs,
)

INCLUDE = (
    "baseline_plus_trend_structure_refined_confidence_v1,"
    "volatility_regime_refined_confidence_v1"
)
ARCHIVE = "risk_appetite_refined_confidence_v1"


def _load(tmp_path: Path):
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    return (
        fixture,
        load_scope_narrowed_candidate_regeneration_inputs(
            scope_review_dir=fixture["scope_review_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archive_candidates=ARCHIVE,
        ),
    )


def test_loader_reads_scope_review_refined_generator_and_validation(tmp_path: Path) -> None:
    fixture, inputs = _load(tmp_path)

    assert inputs.scope_review_dir == fixture["scope_review_dir"]
    assert set(inputs.refined_candidates) == {
        "baseline_plus_trend_structure_refined_confidence_v1",
        "volatility_regime_refined_confidence_v1",
        "risk_appetite_refined_confidence_v1",
    }
    assert inputs.scope_recommendation_rows
    assert inputs.direction_rows
    assert inputs.state_rows


def test_loader_fails_when_scope_recommendation_missing(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    (fixture["scope_review_dir"] / "candidate_scope_narrowing_recommendation_matrix.json").unlink()

    with pytest.raises(ScopeNarrowedCandidateRegenerationError, match="missing"):
        load_scope_narrowed_candidate_regeneration_inputs(
            scope_review_dir=fixture["scope_review_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archive_candidates=ARCHIVE,
        )


def test_loader_fails_when_risk_appetite_reject_record_missing(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    (fixture["scope_review_dir"] / "risk_appetite_reject_record.json").unlink()

    with pytest.raises(ScopeNarrowedCandidateRegenerationError, match="missing"):
        load_scope_narrowed_candidate_regeneration_inputs(
            scope_review_dir=fixture["scope_review_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archive_candidates=ARCHIVE,
        )


def test_loader_fails_when_input_opens_promotion(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    path = fixture["scope_review_dir"] / "candidate_direction_scope_matrix.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["rows"][0]["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopeNarrowedCandidateRegenerationError, match="promotion_allowed"):
        load_scope_narrowed_candidate_regeneration_inputs(
            scope_review_dir=fixture["scope_review_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archive_candidates=ARCHIVE,
        )


def test_loader_fails_when_input_opens_broker_action(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    path = fixture["scope_review_dir"] / "candidate_false_cost_scope_matrix.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["rows"][0]["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopeNarrowedCandidateRegenerationError, match="broker_action"):
        load_scope_narrowed_candidate_regeneration_inputs(
            scope_review_dir=fixture["scope_review_dir"],
            refined_generator_dir=fixture["refined_generator_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archive_candidates=ARCHIVE,
        )
