from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import (
    build_scope_narrowed_forward_observe_readiness_fixture,
)

from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    ScopeNarrowedForwardObserveReadinessReviewError,
    load_scope_narrowed_forward_observe_readiness_inputs,
)


def _load_fixture(tmp_path: Path):
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    return load_scope_narrowed_forward_observe_readiness_inputs(
        scope_validation_dir=fixture["scope_validation_dir"],
        scope_generator_dir=fixture["scope_narrowed_generator_dir"],
        scope_review_dir=fixture["scope_review_dir"],
        candidate=RISK_CAP_CANDIDATE_ID,
        rejected_candidates=CONFIRMATION_CANDIDATE_ID,
        archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
    )


def test_loader_reads_2292_2291_and_2290_inputs(tmp_path: Path) -> None:
    loaded = _load_fixture(tmp_path)

    assert "risk_cap_scorecard" in loaded.validation_payloads
    assert "lineage_report" in loaded.generator_candidate_payloads
    assert "scope_recommendation" in loaded.scope_review_payloads
    assert loaded.candidate == RISK_CAP_CANDIDATE_ID


def test_loader_fails_when_risk_cap_scorecard_missing(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    (fixture["scope_validation_dir"] / "risk_cap_only_validation_scorecard.json").unlink()

    with pytest.raises(ScopeNarrowedForwardObserveReadinessReviewError):
        _load_fixture_from_paths(fixture)


def test_loader_fails_when_state_recommendation_missing(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    (fixture["scope_validation_dir"] / "scope_narrowed_state_recommendation_matrix.json").unlink()

    with pytest.raises(ScopeNarrowedForwardObserveReadinessReviewError):
        _load_fixture_from_paths(fixture)


def test_loader_fails_when_input_opens_promotion(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    path = fixture["scope_validation_dir"] / "scope_narrowed_actual_path_validation_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopeNarrowedForwardObserveReadinessReviewError):
        _load_fixture_from_paths(fixture)


def test_loader_fails_when_input_opens_broker_action(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    path = fixture["scope_validation_dir"] / "scope_narrowed_data_quality_report.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["candidate_rows"][0]["broker_action"] = "sell"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopeNarrowedForwardObserveReadinessReviewError):
        _load_fixture_from_paths(fixture)


def test_loader_fails_when_rejected_candidate_is_readiness_candidate(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)

    with pytest.raises(ScopeNarrowedForwardObserveReadinessReviewError):
        load_scope_narrowed_forward_observe_readiness_inputs(
            scope_validation_dir=fixture["scope_validation_dir"],
            scope_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            candidate=CONFIRMATION_CANDIDATE_ID,
            rejected_candidates=CONFIRMATION_CANDIDATE_ID,
            archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
            target_assets="QQQ,SPY,SMH",
            horizons="5d,10d,20d",
        )


def test_loader_fails_when_archived_candidate_is_readiness_candidate(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)

    with pytest.raises(ScopeNarrowedForwardObserveReadinessReviewError):
        load_scope_narrowed_forward_observe_readiness_inputs(
            scope_validation_dir=fixture["scope_validation_dir"],
            scope_generator_dir=fixture["scope_narrowed_generator_dir"],
            scope_review_dir=fixture["scope_review_dir"],
            candidate=RISK_APPETITE_ARCHIVE_CANDIDATE,
            rejected_candidates=CONFIRMATION_CANDIDATE_ID,
            archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
            target_assets="QQQ,SPY,SMH",
            horizons="5d,10d,20d",
        )


def _load_fixture_from_paths(fixture: dict[str, Path]):
    return load_scope_narrowed_forward_observe_readiness_inputs(
        scope_validation_dir=fixture["scope_validation_dir"],
        scope_generator_dir=fixture["scope_narrowed_generator_dir"],
        scope_review_dir=fixture["scope_review_dir"],
        candidate=RISK_CAP_CANDIDATE_ID,
        rejected_candidates=CONFIRMATION_CANDIDATE_ID,
        archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
    )
