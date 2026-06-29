from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.first_layer_candidate_generator_runtime import (
    run_candidate_generator,
    validate_candidate_generation_bundle,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateGenerationBundle,
    CandidateGeneratorContext,
    CandidateGeneratorError,
)
from ai_trading_system.framework_smoke_candidate_generator import (
    FRAMEWORK_SMOKE_CANDIDATE_FAMILY,
    FRAMEWORK_SMOKE_CANDIDATE_ID,
    FrameworkSmokeCandidateGenerator,
)


def test_runtime_generates_complete_bundle_and_validation_summary(tmp_path: Path) -> None:
    payload = run_candidate_generator(
        candidate_id="framework_smoke_candidate",
        target_asset="QQQ",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 31),
        horizon="10d",
        output_dir=tmp_path,
        mode="framework_smoke_test",
    )

    assert payload["status"] == "FIRST_LAYER_CANDIDATE_GENERATOR_FRAMEWORK_READY_PROMOTION_BLOCKED"
    assert payload["validation_summary"]["status"] == "PASS"
    assert payload["summary"]["signal_record_count"] == 22
    assert payload["summary"]["candidate_binding_validator_reused"] is True
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    for path in payload["artifact_paths"].values():
        assert Path(path).exists()

    validation_path = Path(payload["artifact_paths"]["candidate_validation_summary_json"])
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_runtime_validation_fails_framework_smoke_actual_path_ready(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    context = CandidateGeneratorContext(
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
    spec = generator.build_signal_spec(context)
    records = generator.generate_signal_series(context, spec)
    artifact = generator.generate_prediction_artifact(context, spec, records)
    artifact["actual_path_validation_ready"] = True
    bundle = CandidateGenerationBundle(
        context=context,
        signal_spec=spec,
        signal_records=records,
        prediction_artifact=artifact,
    )

    validation = validate_candidate_generation_bundle(bundle)

    assert validation["status"] == "FAIL"
    assert any("actual-path ready" in error for error in validation["errors"])


def test_context_fails_missing_source_hash(tmp_path: Path) -> None:
    with pytest.raises(CandidateGeneratorError, match="source_hashes must be non-empty"):
        CandidateGeneratorContext(
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
            source_hashes=("",),
        )
