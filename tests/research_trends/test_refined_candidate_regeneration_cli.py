from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_confidence_scaling_refinement_plan_fixture
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_refined_candidate_regeneration_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "refined-candidate-generators-regenerate" in result.output


def test_refined_candidate_regeneration_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)
    output_dir = tmp_path / "refined"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "refined-candidate-generators-regenerate",
            "--refinement-plan-dir",
            str(fixture["refinement_plan_dir"]),
            "--original-generator-dir",
            str(fixture["original_generator_dir"]),
            "--candidates",
            "baseline_plus_trend_structure,risk_appetite,volatility_regime",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "refined_regeneration",
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "refined_regeneration_run_summary.json").exists()
    assert (output_dir / "refined_regeneration_validation_summary.json").exists()
    assert (output_dir / "refined_original_vs_refined_delta_summary.json").exists()
    summary = json.loads(
        (output_dir / "refined_regeneration_run_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["candidate_count"] == 3
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["actual_path_validation_executed"] is False

    for refined_id in (
        "baseline_plus_trend_structure_refined_confidence_v1",
        "risk_appetite_refined_confidence_v1",
        "volatility_regime_refined_confidence_v1",
    ):
        candidate_dir = output_dir / refined_id
        assert (candidate_dir / "refined_candidate_signal_spec.json").exists()
        assert (candidate_dir / "refined_candidate_signal_series.csv").exists()
        assert (candidate_dir / "refined_candidate_prediction_artifact.json").exists()
        validation = json.loads(
            (candidate_dir / "refined_validation_summary.json").read_text(
                encoding="utf-8"
            )
        )
        assert validation["status"] == "PASS"


def test_refined_candidate_regeneration_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_confidence_scaling_refinement_plan_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "refined-candidate-generators-regenerate",
            "--refinement-plan-dir",
            str(fixture["refinement_plan_dir"]),
            "--original-generator-dir",
            str(fixture["original_generator_dir"]),
            "--candidates",
            "risk_appetite",
            "--target-assets",
            "QQQ",
            "--horizons",
            "5d",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code != 0
