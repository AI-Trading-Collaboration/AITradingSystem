from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_confidence_scaling_refinement_plan_fixture

from ai_trading_system.refined_candidate_generators_regenerate import (
    run_refined_candidate_generators_regenerate,
)


def test_refined_candidate_artifacts_are_generated_and_safe(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    output_dir = tmp_path / "refined"

    run_refined_candidate_generators_regenerate(
        refinement_plan_dir=fixture["refinement_plan_dir"],
        original_generator_dir=fixture["original_generator_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="refined_regeneration",
        docs_root=tmp_path / "docs",
    )

    for original_id, refined_id in {
        "baseline_plus_trend_structure": "baseline_plus_trend_structure_refined_confidence_v1",
        "risk_appetite": "risk_appetite_refined_confidence_v1",
        "volatility_regime": "volatility_regime_refined_confidence_v1",
    }.items():
        candidate_dir = output_dir / refined_id
        assert (candidate_dir / "refined_candidate_signal_spec.json").exists()
        assert (candidate_dir / "refined_candidate_signal_series.csv").exists()
        assert (candidate_dir / "refined_candidate_prediction_artifact.json").exists()
        artifact = json.loads(
            (candidate_dir / "refined_candidate_prediction_artifact.json").read_text(
                encoding="utf-8"
            )
        )
        validation = json.loads(
            (candidate_dir / "refined_validation_summary.json").read_text(
                encoding="utf-8"
            )
        )
        assert validation["status"] == "PASS", validation["errors"]
        assert artifact["candidate_id"] == refined_id
        assert artifact["original_candidate_id"] == original_id
        assert artifact["refined_candidate_id"] != artifact["original_candidate_id"]
        assert artifact["artifact_role"] == "refined_regenerated_executable_candidate_artifact"
        assert artifact["provenance"]["refinement_source"]["task_id"] == "TRADING-2287"
        assert artifact["provenance"]["refinement_source"]["parameter_set_ids"]
        assert artifact["promotion_allowed"] is False
        assert artifact["paper_shadow_allowed"] is False
        assert artifact["production_allowed"] is False
        assert artifact["broker_action"] == "none"
        assert artifact["actual_path_validation_ready"] is False
