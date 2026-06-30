from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)


def test_scope_narrowed_artifacts_have_lineage_and_safety(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    output_dir = tmp_path / "out"
    run_scope_narrowed_candidate_generators_regenerate(
        scope_review_dir=fixture["scope_review_dir"],
        refined_generator_dir=fixture["refined_generator_dir"],
        refined_validation_dir=fixture["refined_validation_dir"],
        include_candidates=(
            "baseline_plus_trend_structure_refined_confidence_v1,"
            "volatility_regime_refined_confidence_v1"
        ),
        archive_candidates="risk_appetite_refined_confidence_v1",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="scope_narrowed_regeneration",
        docs_root=tmp_path / "docs",
    )
    candidate_dir = output_dir / "baseline_plus_trend_structure_scope_narrowed_confirmation_v1"
    assert (candidate_dir / "scope_narrowed_candidate_signal_spec.json").exists()
    assert (candidate_dir / "scope_narrowed_candidate_signal_series.csv").exists()
    artifact_path = candidate_dir / "scope_narrowed_candidate_prediction_artifact.json"
    assert artifact_path.exists()
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert artifact["candidate_id"] != artifact["refined_candidate_id"]
    assert artifact["scope_narrowed_candidate_id"] == artifact["candidate_id"]
    assert artifact["provenance"]["scope_narrowing_source"]["task_id"] == "TRADING-2290"
    assert artifact["actual_path_validation_ready"] is False
    assert artifact["promotion_allowed"] is False
    assert artifact["paper_shadow_allowed"] is False
    assert artifact["production_allowed"] is False
    assert artifact["broker_action"] == "none"
