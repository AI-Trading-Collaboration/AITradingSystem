from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_regenerated_actual_path_validation_fixture

from ai_trading_system.regenerated_candidate_inconclusive_diagnostics import (
    RegeneratedCandidateInconclusiveDiagnosticsError,
    load_regenerated_candidate_inconclusive_diagnostics_inputs,
)


def test_loader_reads_2285_actual_path_outputs(tmp_path: Path) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)

    loaded = load_regenerated_candidate_inconclusive_diagnostics_inputs(
        validation_dir=fixture["validation_dir"],
        generator_dir=fixture["generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
    )

    assert loaded.actual_path_rows
    assert loaded.outcome_rows
    assert loaded.scorecards
    assert loaded.generator_artifact_context_status == "complete"


def test_loader_fails_when_actual_path_matrix_is_missing(tmp_path: Path) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    (fixture["validation_dir"] / "regenerated_candidate_actual_path_matrix.json").unlink()

    with pytest.raises(RegeneratedCandidateInconclusiveDiagnosticsError):
        load_regenerated_candidate_inconclusive_diagnostics_inputs(
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_outcome_matrix_is_missing(tmp_path: Path) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    (fixture["validation_dir"] / "candidate_prediction_outcome_matrix.json").unlink()

    with pytest.raises(RegeneratedCandidateInconclusiveDiagnosticsError):
        load_regenerated_candidate_inconclusive_diagnostics_inputs(
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_scorecard_is_missing(tmp_path: Path) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    (fixture["validation_dir"] / "candidate_validation_scorecard.json").unlink()

    with pytest.raises(RegeneratedCandidateInconclusiveDiagnosticsError):
        load_regenerated_candidate_inconclusive_diagnostics_inputs(
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_input_output_attempts_promotion(tmp_path: Path) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    path = fixture["validation_dir"] / "regenerated_candidate_actual_path_validation_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RegeneratedCandidateInconclusiveDiagnosticsError):
        load_regenerated_candidate_inconclusive_diagnostics_inputs(
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )


def test_loader_fails_when_input_output_attempts_broker_action(tmp_path: Path) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    path = fixture["validation_dir"] / "candidate_prediction_outcome_matrix.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["rows"][0]["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RegeneratedCandidateInconclusiveDiagnosticsError):
        load_regenerated_candidate_inconclusive_diagnostics_inputs(
            validation_dir=fixture["validation_dir"],
            generator_dir=fixture["generator_dir"],
            candidates="baseline_plus_trend_structure",
        )
