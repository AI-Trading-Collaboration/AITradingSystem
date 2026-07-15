from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_weight_batch_search_helpers import run_signal_diagnosis_foundation_fixture

from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_diagnosis_foundation as diagnosis_foundation,
)
from ai_trading_system.platform.artifacts.validation_session import artifact_validation_session


@pytest.fixture(scope="module", autouse=True)
def shared_validation_session() -> Iterator[None]:
    with artifact_validation_session():
        yield


@pytest.fixture(scope="module")
def foundation_fixture(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    tmp_path = tmp_path_factory.mktemp("signal_diagnosis_foundation")
    return {
        **run_signal_diagnosis_foundation_fixture(tmp_path),
        "tmp_path": tmp_path,
    }


def _path(fixture: dict[str, object]) -> Path:
    value = fixture["tmp_path"]
    assert isinstance(value, Path)
    return value


def test_gate_calibration_review_is_diagnostic_only(
    foundation_fixture: dict[str, object],
) -> None:
    review = foundation_fixture["gate_calibration"]
    assert isinstance(review, dict)
    assert review["manifest"]["status"] == "PASS"
    assert review["manifest"]["official_gate_changed"] is False
    assert review["gate_strictness_diagnosis"]["can_change_official_gate"] is False
    assert review["manifest"]["broker_action_allowed"] is False
    assert "Gate Calibration Review" in review["reader_brief_section"]

    validation = diagnosis_foundation.validate_gate_calibration_review_artifact(
        gate_calibration_id=review["gate_calibration_id"],
        output_dir=_path(foundation_fixture) / "gate_calibration_review",
    )
    assert validation["status"] == "PASS"


def test_scorecard_attribution_preserves_exact_lineage(
    foundation_fixture: dict[str, object],
) -> None:
    attribution = foundation_fixture["scorecard_attribution"]
    assert isinstance(attribution, dict)
    assert attribution["manifest"]["status"] == "PASS"
    assert attribution["manifest"]["variant_count"] > 0
    assert attribution["rejected_variant_component_matrix"]
    assert attribution["family_component_weakness"]["families"]
    assert attribution["manifest"]["broker_action_allowed"] is False

    validation = diagnosis_foundation.validate_scorecard_attribution_artifact(
        scorecard_attribution_id=attribution["scorecard_attribution_id"],
        output_dir=_path(foundation_fixture) / "scorecard_attribution",
    )
    assert validation["status"] == "PASS"


def test_signal_diagnosis_does_not_fabricate_dated_events(
    foundation_fixture: dict[str, object],
) -> None:
    diagnosis = foundation_fixture["signal_diagnosis"]
    assert isinstance(diagnosis, dict)
    summary = diagnosis["signal_instability_summary"]
    assert diagnosis["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert diagnosis["method_signal_stability"]
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["requires_signal_level_fix"] is None
    assert diagnosis["signal_flip_events"] == []
    assert diagnosis["regime_mismatch_events"] == []
    assert diagnosis["manifest"]["broker_action_allowed"] is False

    validation = diagnosis_foundation.validate_signal_instability_diagnosis_artifact(
        signal_diagnosis_id=diagnosis["signal_diagnosis_id"],
        output_dir=_path(foundation_fixture) / "signal_instability_diagnosis",
    )
    assert validation["status"] == "PASS"


def test_consensus_review_preserves_missing_dispersion(
    foundation_fixture: dict[str, object],
) -> None:
    review = foundation_fixture["consensus_review"]
    assert isinstance(review, dict)
    assert review["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert review["consensus_dispersion_summary"]["dispersion_status"] == "INSUFFICIENT_DATA"
    assert review["consensus_dispersion_summary"]["avg_candidate_dispersion"] is None
    assert review["ensemble_method_quality"]
    assert review["consensus_failure_reasons"]["primary_failure_reason"]
    assert review["manifest"]["broker_action_allowed"] is False

    validation = diagnosis_foundation.validate_consensus_quality_review_artifact(
        consensus_review_id=review["consensus_review_id"],
        output_dir=_path(foundation_fixture) / "consensus_quality_review",
    )
    assert validation["status"] == "PASS"


@pytest.mark.parametrize(
    ("artifact_key", "root_name", "view_name", "validator", "id_key", "validator_id_key"),
    [
        (
            "gate_calibration",
            "gate_calibration_review",
            "gate_component_impact.json",
            diagnosis_foundation.validate_gate_calibration_review_artifact,
            "gate_calibration_id",
            "gate_calibration_id",
        ),
        (
            "scorecard_attribution",
            "scorecard_attribution",
            "score_component_distribution.json",
            diagnosis_foundation.validate_scorecard_attribution_artifact,
            "scorecard_attribution_id",
            "scorecard_attribution_id",
        ),
        (
            "signal_diagnosis",
            "signal_instability_diagnosis",
            "signal_instability_summary.json",
            diagnosis_foundation.validate_signal_instability_diagnosis_artifact,
            "signal_diagnosis_id",
            "signal_diagnosis_id",
        ),
        (
            "consensus_review",
            "consensus_quality_review",
            "consensus_dispersion_summary.json",
            diagnosis_foundation.validate_consensus_quality_review_artifact,
            "consensus_review_id",
            "consensus_review_id",
        ),
    ],
)
def test_materialized_view_tamper_fails_closed(
    foundation_fixture: dict[str, object],
    artifact_key: str,
    root_name: str,
    view_name: str,
    validator: object,
    id_key: str,
    validator_id_key: str,
) -> None:
    artifact = foundation_fixture[artifact_key]
    assert isinstance(artifact, dict)
    artifact_id = artifact[id_key]
    root = _path(foundation_fixture) / root_name
    view = root / artifact_id / view_name
    original = view.read_bytes()
    try:
        view.write_bytes(b"{}\n")
        assert callable(validator)
        validation = validator(**{validator_id_key: artifact_id, "output_dir": root})
        assert validation["status"] == "FAIL"
    finally:
        view.write_bytes(original)


def test_live_source_tamper_fails_closed(foundation_fixture: dict[str, object]) -> None:
    diagnosis = foundation_fixture["signal_diagnosis"]
    attribution = foundation_fixture["scorecard_attribution"]
    assert isinstance(diagnosis, dict)
    assert isinstance(attribution, dict)
    tmp_path = _path(foundation_fixture)
    source = (
        tmp_path
        / "scorecard_attribution"
        / attribution["scorecard_attribution_id"]
        / "score_component_distribution.json"
    )
    original = source.read_bytes()
    try:
        source.write_bytes(b"{}\n")
        validation = diagnosis_foundation.validate_signal_instability_diagnosis_artifact(
            signal_diagnosis_id=diagnosis["signal_diagnosis_id"],
            output_dir=tmp_path / "signal_instability_diagnosis",
        )
        assert validation["status"] == "FAIL"
    finally:
        source.write_bytes(original)


def test_policy_tamper_fails_closed(foundation_fixture: dict[str, object]) -> None:
    attribution = foundation_fixture["scorecard_attribution"]
    assert isinstance(attribution, dict)
    tmp_path = _path(foundation_fixture)
    policy_path = tmp_path / "signal_diagnosis_policy.yaml"
    policy_path.write_bytes(
        diagnosis_foundation.DEFAULT_SIGNAL_DIAGNOSIS_FOUNDATION_POLICY_PATH.read_bytes()
    )
    diagnosis = diagnosis_foundation.run_signal_instability_diagnosis(
        scorecard_attribution_id=attribution["scorecard_attribution_id"],
        attribution_dir=tmp_path / "scorecard_attribution",
        output_dir=tmp_path / "policy_tamper_signal",
        generated_at=datetime(2026, 3, 24, 2, tzinfo=UTC),
        policy_path=policy_path,
    )
    policy_path.write_bytes(policy_path.read_bytes() + b"\n")

    validation = diagnosis_foundation.validate_signal_instability_diagnosis_artifact(
        signal_diagnosis_id=diagnosis["signal_diagnosis_id"],
        output_dir=tmp_path / "policy_tamper_signal",
    )
    assert validation["status"] == "FAIL"
