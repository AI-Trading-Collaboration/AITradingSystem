from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateGeneratorContext,
    CandidateGeneratorError,
)
from ai_trading_system.framework_smoke_candidate_generator import (
    FRAMEWORK_SMOKE_CANDIDATE_FAMILY,
    FRAMEWORK_SMOKE_CANDIDATE_ID,
    FrameworkSmokeCandidateGenerator,
)


def _context(
    tmp_path: Path,
    *,
    candidate_id: str = FRAMEWORK_SMOKE_CANDIDATE_ID,
) -> CandidateGeneratorContext:
    return CandidateGeneratorContext(
        candidate_id=candidate_id,
        candidate_family=FRAMEWORK_SMOKE_CANDIDATE_FAMILY,
        target_asset="QQQ",
        start_date=date(2023, 1, 3),
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


def test_generator_interface_builds_signal_spec(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()
    spec = generator.build_signal_spec(_context(tmp_path))
    payload = spec.to_dict()

    assert payload["candidate_id"] == "framework_smoke_candidate"
    assert payload["candidate_family"] == "first_layer_executable_candidate"
    assert payload["generator_id"] == "framework_smoke_candidate"
    assert payload["supported_horizons"] == ["10d"]
    assert payload["output_signal_names"] == ["framework_smoke_signal"]
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"


def test_candidate_generator_context_fails_missing_candidate_id(tmp_path: Path) -> None:
    with pytest.raises(CandidateGeneratorError, match="missing candidate_id"):
        _context(tmp_path, candidate_id="")


def test_candidate_generator_context_requires_timezone_aware_generated_at(tmp_path: Path) -> None:
    with pytest.raises(CandidateGeneratorError, match="timezone-aware"):
        CandidateGeneratorContext(
            candidate_id=FRAMEWORK_SMOKE_CANDIDATE_ID,
            candidate_family=FRAMEWORK_SMOKE_CANDIDATE_FAMILY,
            target_asset="QQQ",
            start_date=date(2023, 1, 3),
            end_date=date(2023, 1, 6),
            horizon="10d",
            output_dir=tmp_path,
            mode="framework_smoke_test",
            generated_at=datetime(2026, 6, 29),
            signal_spec_version="first_layer_candidate_signal_spec.v1",
            prediction_schema_version="candidate_bound_prediction_artifact.v1",
            input_snapshot_hash="input_hash",
            feature_snapshot_hash="feature_hash",
            source_paths=(Path(__file__),),
            source_hashes=("source_hash",),
        )
