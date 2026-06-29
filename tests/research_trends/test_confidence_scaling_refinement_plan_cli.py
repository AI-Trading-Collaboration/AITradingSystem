from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_regenerated_inconclusive_diagnostics_fixture
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_confidence_scaling_refinement_plan_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "candidate-generator-confidence-scaling-refinement-plan" in result.output


def test_confidence_scaling_refinement_plan_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)
    output_dir = tmp_path / "confidence_scaling_refinement_plan"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "candidate-generator-confidence-scaling-refinement-plan",
            "--diagnostics-dir",
            str(fixture["diagnostics_dir"]),
            "--validation-dir",
            str(fixture["validation_dir"]),
            "--generator-dir",
            str(fixture["generator_dir"]),
            "--candidates",
            "baseline_plus_trend_structure,risk_appetite,volatility_regime",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "refinement_plan",
            "--docs-root",
            str(tmp_path / "confidence_scaling_docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "confidence_scaling_refinement_summary.json",
        "candidate_confidence_failure_diagnosis_matrix.json",
        "candidate_confidence_distribution_retargeting_matrix.json",
        "candidate_confidence_scaling_proposal_matrix.json",
        "candidate_confidence_scaling_parameter_grid.json",
        "candidate_guardrail_matrix.json",
        "candidate_2288_implementation_plan.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists()

    summary = json.loads(
        (output_dir / "confidence_scaling_refinement_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["regeneration_executed"] is False
    assert summary["actual_path_validation_executed"] is False


def test_confidence_scaling_refinement_plan_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_regenerated_inconclusive_diagnostics_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "candidate-generator-confidence-scaling-refinement-plan",
            "--diagnostics-dir",
            str(fixture["diagnostics_dir"]),
            "--validation-dir",
            str(fixture["validation_dir"]),
            "--generator-dir",
            str(fixture["generator_dir"]),
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
            str(tmp_path / "confidence_scaling_docs"),
        ],
    )

    assert result.exit_code != 0
