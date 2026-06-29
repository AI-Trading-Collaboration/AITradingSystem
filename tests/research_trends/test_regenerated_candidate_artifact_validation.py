from __future__ import annotations

import math
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import (
    build_and_validate_bundle,
    regenerated_context,
    write_price_fixture,
)

from ai_trading_system.baseline_plus_trend_structure_generator import (
    BaselinePlusTrendStructureGenerator,
)
from ai_trading_system.candidate_signal_binding_validator import (
    validate_candidate_bound_prediction_artifact,
    validate_candidate_bound_signal_series,
)
from ai_trading_system.first_layer_candidate_generator_registry import (
    default_candidate_generator_registry,
)
from ai_trading_system.first_layer_candidate_signal_generator import CandidateGeneratorError
from ai_trading_system.framework_smoke_candidate_generator import FrameworkSmokeCandidateGenerator
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_registry_lists_and_gets_regenerated_candidate_generators() -> None:
    registry = default_candidate_generator_registry()
    listed_ids = {item["generator_id"] for item in registry.list_generators()}

    assert {
        "baseline_plus_trend_structure",
        "risk_appetite",
        "volatility_regime",
    }.issubset(listed_ids)
    assert registry.get_generator("baseline_plus_trend_structure").generator_id == (
        "baseline_plus_trend_structure"
    )
    assert registry.get_generator("risk_appetite").generator_id == "risk_appetite"
    assert registry.get_generator("volatility_regime").generator_id == "volatility_regime"
    with pytest.raises(CandidateGeneratorError, match="unknown first-layer candidate"):
        registry.get_generator("unknown_generator")


def test_regenerated_prediction_artifact_fails_closed_when_safety_gates_open(
    tmp_path: Path,
) -> None:
    price_path = write_price_fixture(tmp_path)
    generator = BaselinePlusTrendStructureGenerator()
    context = regenerated_context(
        tmp_path,
        candidate_id="baseline_plus_trend_structure",
        price_path=price_path,
    )
    _, records, artifact, validation = build_and_validate_bundle(generator, context)
    assert validation["status"] == "PASS"

    artifact["actual_path_validation_ready"] = True
    artifact["promotion_eligible"] = True
    artifact["historical_executable_artifact"] = False
    result = validate_candidate_bound_prediction_artifact(artifact)

    assert result.passed is False
    assert any("actual-path ready" in error for error in result.errors)
    assert any("promotion_eligible=false" in error for error in result.errors)
    assert any("historical executable" in error for error in result.errors)

    row = records[0].to_dict()
    row["signal_value"] = math.nan
    row["signal_confidence"] = math.inf
    series_result = validate_candidate_bound_signal_series([row])
    assert series_result.passed is False
    assert any("signal_value" in error for error in series_result.errors)
    assert any("signal_confidence" in error for error in series_result.errors)


def test_framework_smoke_artifact_stays_actual_path_blocked(tmp_path: Path) -> None:
    generator = FrameworkSmokeCandidateGenerator()

    assert generator.generator_id == "framework_smoke_candidate"
    assert generator.generator_version == "framework_smoke_candidate_generator.v1"


def test_report_registry_marks_2284_artifacts_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "first_layer_candidate_generators_regenerated"
    )

    assert entry["command"] == (
        "aits research trends first-layer-candidate-generators-regenerate"
    )
    assert entry["artifact_role"] == "regenerated_executable_candidate_artifact"
    assert entry["promotion_eligible"] is False
    assert entry["actual_path_validation_ready"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["broker_action"] == "none"
