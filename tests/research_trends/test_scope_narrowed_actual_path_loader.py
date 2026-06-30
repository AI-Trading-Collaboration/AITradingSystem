from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_actual_path_validation_fixture,
)

from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    ScopeNarrowedCandidateActualPathValidationError,
    load_scope_narrowed_actual_path_inputs,
)

INCLUDE = f"{CONFIRMATION_CANDIDATE_ID},{RISK_CAP_CANDIDATE_ID}"
ARCHIVED = RISK_APPETITE_ARCHIVE_CANDIDATE


def _load(tmp_path: Path):
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    return (
        fixture,
        load_scope_narrowed_actual_path_inputs(
            scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archived_candidates=ARCHIVED,
        ),
    )


def test_loader_reads_2291_2290_and_2289_context(tmp_path: Path) -> None:
    fixture, inputs = _load(tmp_path)

    assert inputs.scope_narrowed_generator_dir == fixture["scope_narrowed_generator_dir"]
    assert set(inputs.scope_narrowed_artifacts) == {
        CONFIRMATION_CANDIDATE_ID,
        RISK_CAP_CANDIDATE_ID,
    }
    assert inputs.scope_review_payloads["scope_recommendation"]["rows"]
    assert inputs.refined_validation_payloads["state"]["candidate_rows"]
    assert inputs.risk_appetite_archive_record["candidate_id"] == ARCHIVED


def test_loader_fails_when_prediction_artifact_missing(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    (
        fixture["scope_narrowed_generator_dir"]
        / CONFIRMATION_CANDIDATE_ID
        / "scope_narrowed_candidate_prediction_artifact.json"
    ).unlink()

    with pytest.raises(ScopeNarrowedCandidateActualPathValidationError, match="missing"):
        load_scope_narrowed_actual_path_inputs(
            scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archived_candidates=ARCHIVED,
        )


def test_loader_fails_when_scope_filter_report_missing(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    (
        fixture["scope_narrowed_generator_dir"]
        / RISK_CAP_CANDIDATE_ID
        / "scope_filter_report.json"
    ).unlink()

    with pytest.raises(ScopeNarrowedCandidateActualPathValidationError, match="missing"):
        load_scope_narrowed_actual_path_inputs(
            scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archived_candidates=ARCHIVED,
        )


def test_loader_fails_when_input_opens_promotion(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    path = (
        fixture["scope_narrowed_generator_dir"]
        / CONFIRMATION_CANDIDATE_ID
        / "scope_narrowed_candidate_prediction_artifact.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["prediction_records"][0]["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopeNarrowedCandidateActualPathValidationError, match="promotion"):
        load_scope_narrowed_actual_path_inputs(
            scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archived_candidates=ARCHIVED,
        )


def test_loader_fails_when_input_opens_broker_action(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    path = (
        fixture["scope_narrowed_generator_dir"]
        / RISK_CAP_CANDIDATE_ID
        / "scope_narrowed_candidate_prediction_artifact.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["prediction_records"][0]["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopeNarrowedCandidateActualPathValidationError, match="broker_action"):
        load_scope_narrowed_actual_path_inputs(
            scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=INCLUDE,
            archived_candidates=ARCHIVED,
        )


def test_loader_fails_when_risk_appetite_is_included(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)

    with pytest.raises(ScopeNarrowedCandidateActualPathValidationError, match="risk_appetite"):
        load_scope_narrowed_actual_path_inputs(
            scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            refined_validation_dir=fixture["refined_validation_dir"],
            include_candidates=f"{INCLUDE},{RISK_APPETITE_ARCHIVE_CANDIDATE}",
            archived_candidates=ARCHIVED,
        )
