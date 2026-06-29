from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_refined_candidate_regeneration_fixture
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_refined_candidate_actual_path_validation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "refined-candidate-actual-path-validation" in result.output


def test_refined_candidate_actual_path_validation_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)
    output_dir = tmp_path / "refined_actual_path_validation"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "refined-candidate-actual-path-validation",
            "--refined-generator-dir",
            str(fixture["refined_generator_dir"]),
            "--original-validation-dir",
            str(fixture["validation_dir"]),
            "--refinement-plan-dir",
            str(fixture["refinement_plan_dir"]),
            "--candidates",
            (
                "baseline_plus_trend_structure_refined_confidence_v1,"
                "risk_appetite_refined_confidence_v1,"
                "volatility_regime_refined_confidence_v1"
            ),
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "refined_actual_path_validation",
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "refined_candidate_actual_path_validation_summary.json",
        "refined_candidate_actual_path_matrix.json",
        "refined_candidate_prediction_outcome_matrix.json",
        "refined_candidate_validation_scorecard.json",
        "refined_high_conviction_outcome_drilldown.json",
        "refined_guardrail_validation_matrix.json",
        "original_vs_refined_actual_path_comparison.json",
        "refined_candidate_state_recommendation_matrix.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists()

    summary = json.loads(
        (output_dir / "refined_candidate_actual_path_validation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"


def test_refined_candidate_actual_path_validation_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_refined_candidate_regeneration_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "refined-candidate-actual-path-validation",
            "--refined-generator-dir",
            str(fixture["refined_generator_dir"]),
            "--original-validation-dir",
            str(fixture["validation_dir"]),
            "--refinement-plan-dir",
            str(fixture["refinement_plan_dir"]),
            "--candidates",
            "risk_appetite_refined_confidence_v1",
            "--target-assets",
            "QQQ",
            "--horizons",
            "5d",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
        ],
    )

    assert result.exit_code != 0
