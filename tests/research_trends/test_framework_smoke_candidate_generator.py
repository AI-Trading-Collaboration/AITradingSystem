from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.candidate_signal_binding_schema import (
    candidate_bound_prediction_artifact_contract_dict,
    candidate_bound_signal_series_contract_dict,
)
from ai_trading_system.candidate_signal_binding_validator import (
    validate_candidate_bound_prediction_artifact,
    validate_candidate_bound_signal_series,
)
from ai_trading_system.first_layer_candidate_signal_generator import CandidateGeneratorContext
from ai_trading_system.framework_smoke_candidate_generator import (
    FRAMEWORK_SMOKE_CANDIDATE_FAMILY,
    FRAMEWORK_SMOKE_CANDIDATE_ID,
    FrameworkSmokeCandidateGenerator,
)


def _context(tmp_path: Path) -> CandidateGeneratorContext:
    return CandidateGeneratorContext(
        candidate_id=FRAMEWORK_SMOKE_CANDIDATE_ID,
        candidate_family=FRAMEWORK_SMOKE_CANDIDATE_FAMILY,
        target_asset="QQQ",
        start_date=date(2023, 1, 2),
        end_date=date(2023, 1, 6),
        horizon="10d",
        output_dir=tmp_path,
        mode="framework_smoke_test",
        generated_at=datetime(2026, 6, 29, tzinfo=UTC),
        signal_spec_version="first_layer_candidate_signal_spec.v1",
        prediction_schema_version="candidate_bound_prediction_artifact.v1",
        input_snapshot_hash="input_hash",
        feature_snapshot_hash="feature_hash",
        source_paths=(Path(__file__),),
        source_hashes=("source_hash",),
    )


def test_framework_smoke_generator_outputs_deterministic_candidate_bound_records(
    tmp_path: Path,
) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    context = _context(tmp_path)
    spec = generator.build_signal_spec(context)

    first = [record.to_dict() for record in generator.generate_signal_series(context, spec)]
    second = [record.to_dict() for record in generator.generate_signal_series(context, spec)]

    assert first == second
    assert len(first) == 5
    assert first[0]["candidate_id"] == "framework_smoke_candidate"
    assert first[0]["target_asset"] == "QQQ"
    assert first[0]["horizon"] == "10d"
    assert first[0]["signal_value"] == -1.0
    assert first[0]["signal_direction"] == "risk_off"
    assert first[0]["promotion_allowed"] is False
    assert first[0]["paper_shadow_allowed"] is False
    assert first[0]["production_allowed"] is False
    assert first[0]["broker_action"] == "none"
    assert validate_candidate_bound_signal_series(first).passed is True


def test_framework_smoke_signal_series_contains_required_fields(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    context = _context(tmp_path)
    spec = generator.build_signal_spec(context)
    row = generator.generate_signal_series(context, spec)[0].to_dict()

    for field in candidate_bound_signal_series_contract_dict()["required_columns"]:
        assert field in row


def test_framework_smoke_prediction_artifact_contains_required_fields(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    context = _context(tmp_path)
    spec = generator.build_signal_spec(context)
    records = generator.generate_signal_series(context, spec)
    artifact = generator.generate_prediction_artifact(context, spec, records)

    for field in candidate_bound_prediction_artifact_contract_dict()["required_top_level_fields"]:
        assert field in artifact
    assert artifact["artifact_role"] == "framework_smoke_test"
    assert artifact["historical_executable_artifact"] is False
    assert artifact["actual_path_validation_ready"] is False
    assert artifact["promotion_eligible"] is False
    assert artifact["prediction_records"][0]["candidate_id"] == "framework_smoke_candidate"
    assert validate_candidate_bound_prediction_artifact(artifact).passed is True


def test_framework_smoke_validator_fails_missing_pit_timestamp(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    context = _context(tmp_path)
    spec = generator.build_signal_spec(context)
    row = generator.generate_signal_series(context, spec)[0].to_dict()
    row.pop("as_of_timestamp")

    result = validate_candidate_bound_signal_series([row])

    assert result.passed is False
    assert any("as_of_timestamp" in error for error in result.errors)


def test_framework_smoke_validator_fails_missing_source_hash(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    context = _context(tmp_path)
    spec = generator.build_signal_spec(context)
    row = generator.generate_signal_series(context, spec)[0].to_dict()
    row["source_artifact_hash"] = ""

    result = validate_candidate_bound_signal_series([row])

    assert result.passed is False
    assert any("source_artifact_hash" in error for error in result.errors)
