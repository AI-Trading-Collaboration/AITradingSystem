from __future__ import annotations

from typing import Any

from ai_trading_system.candidate_signal_binding_validator import (
    validate_candidate_bound_prediction_artifact,
    validate_candidate_bound_signal_series,
)


def _valid_record() -> dict[str, Any]:
    provenance = {
        "source_paths": ["outputs/research_trends/models/first_layer_composer_v2_predictions.csv"],
        "source_hashes": ["abc123"],
        "regeneration_mode": "schema_migration_poc",
        "pit_policy": "non_pit_source_evidence_only",
        "candidate_binding_method": "rewrap_mapping",
        "source_schema_status": "source_evidence_only",
        "promotion_eligible": False,
    }
    source_path = "outputs/research_trends/models/first_layer_composer_v2_predictions.csv"
    return {
        "candidate_id": "baseline",
        "candidate_family": "first_layer_proxy_candidate",
        "source_experiment_id": "first_layer_composer_v2",
        "source_artifact_id": "first_layer_composer_v2_predictions",
        "source_artifact_path": source_path,
        "source_artifact_hash": "abc123",
        "signal_spec_version": "first_layer_candidate_signal_spec.v1",
        "prediction_schema_version": "candidate_bound_prediction_artifact.v1",
        "generated_at": "2026-06-29T00:00:00+00:00",
        "as_of_timestamp": "2023-02-22T00:00:00+00:00",
        "decision_timestamp": "2023-02-23T00:00:00+00:00",
        "target_asset": "QQQ_SGOV_TQQQ",
        "horizon": "20d",
        "signal_name": "first_layer_composer_v2_trend_state",
        "signal_value": 0.5,
        "signal_direction": "neutral",
        "signal_confidence": 0.5,
        "valid_from": "2023-02-23T00:00:00+00:00",
        "valid_until": "2023-03-05T00:00:00+00:00",
        "input_snapshot_hash": "abc123",
        "feature_snapshot_hash": "feature123",
        "model_or_rule_version": "first_layer_composer_v2",
        "provenance": provenance,
        "promotion_eligible": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "permanently_inconclusive_override_allowed": False,
    }


def test_candidate_signal_binding_validator_requires_binding_and_timestamps() -> None:
    assert validate_candidate_bound_signal_series([_valid_record()]).passed is True

    for field in ("candidate_id", "as_of_timestamp", "decision_timestamp"):
        broken = _valid_record()
        broken.pop(field)

        result = validate_candidate_bound_signal_series([broken])

        assert result.passed is False
        assert any(field in error for error in result.errors)


def test_candidate_signal_binding_validator_requires_source_hash() -> None:
    broken = _valid_record()
    broken.pop("source_artifact_hash")

    result = validate_candidate_bound_signal_series([broken])

    assert result.passed is False
    assert any("source_artifact_hash" in error for error in result.errors)


def test_schema_migration_poc_forces_promotion_eligible_false() -> None:
    broken = _valid_record()
    broken["promotion_eligible"] = True
    broken["provenance"] = {**broken["provenance"], "promotion_eligible": True}

    result = validate_candidate_bound_signal_series([broken])

    assert result.passed is False
    assert any("promotion_eligible=false" in error for error in result.errors)


def test_non_pit_source_evidence_disables_shadow_production_and_broker() -> None:
    broken = _valid_record()
    broken["paper_shadow_allowed"] = True
    broken["production_allowed"] = True
    broken["broker_action"] = "buy"

    result = validate_candidate_bound_signal_series([broken])

    assert result.passed is False
    assert any("paper_shadow_allowed=false" in error for error in result.errors)
    assert any("production_allowed=false" in error for error in result.errors)
    assert any("broker_action=none" in error for error in result.errors)


def test_prediction_artifact_validation_checks_records_and_history_boundary() -> None:
    record = _valid_record()
    artifact = {
        **record,
        "artifact_id": "baseline_rewrapped_candidate_prediction_artifact",
        "artifact_role": "schema_migration_poc",
        "historical_executable_artifact": False,
        "actual_path_validation_ready": False,
        "prediction_records": [record],
    }

    result = validate_candidate_bound_prediction_artifact(artifact)

    assert result.passed is True

    broken = dict(artifact)
    broken["historical_executable_artifact"] = True
    result = validate_candidate_bound_prediction_artifact(broken)

    assert result.passed is False
    assert any("historical executable" in error for error in result.errors)
