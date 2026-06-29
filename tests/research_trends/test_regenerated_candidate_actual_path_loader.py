from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import build_regenerated_artifact_fixture

from ai_trading_system.regenerated_candidate_actual_path_validation import (
    RegeneratedCandidateActualPathValidationError,
    load_regenerated_candidate_artifacts,
)


def test_loader_reads_regenerated_candidate_artifacts(tmp_path: Path) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)

    artifacts = load_regenerated_candidate_artifacts(
        fixture["input_dir"],
        "baseline_plus_trend_structure,risk_appetite,volatility_regime",
    )

    assert set(artifacts) == {
        "baseline_plus_trend_structure",
        "risk_appetite",
        "volatility_regime",
    }
    assert artifacts["baseline_plus_trend_structure"].prediction_records
    assert (
        artifacts["risk_appetite"].prediction_artifact["artifact_role"]
        == "regenerated_executable_candidate_artifact"
    )


def test_loader_fails_when_prediction_artifact_missing(tmp_path: Path) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    (
        fixture["input_dir"]
        / "baseline_plus_trend_structure"
        / "candidate_prediction_artifact.json"
    ).unlink()

    with pytest.raises(RegeneratedCandidateActualPathValidationError):
        load_regenerated_candidate_artifacts(
            fixture["input_dir"],
            "baseline_plus_trend_structure",
        )


def test_loader_fails_schema_invalid_artifact(tmp_path: Path) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    path = fixture["input_dir"] / "risk_appetite" / "candidate_prediction_artifact.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("candidate_id")
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RegeneratedCandidateActualPathValidationError):
        load_regenerated_candidate_artifacts(fixture["input_dir"], "risk_appetite")


def test_loader_fails_promotion_allowed_input(tmp_path: Path) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    path = fixture["input_dir"] / "volatility_regime" / "candidate_prediction_artifact.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["promotion_allowed"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RegeneratedCandidateActualPathValidationError):
        load_regenerated_candidate_artifacts(fixture["input_dir"], "volatility_regime")


def test_loader_fails_broker_action_input(tmp_path: Path) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    path = fixture["input_dir"] / "volatility_regime" / "candidate_prediction_artifact.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["broker_action"] = "buy"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RegeneratedCandidateActualPathValidationError):
        load_regenerated_candidate_artifacts(fixture["input_dir"], "volatility_regime")
