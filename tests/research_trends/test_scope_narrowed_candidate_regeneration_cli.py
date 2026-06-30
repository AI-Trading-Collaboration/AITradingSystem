from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_scope_narrowed_candidate_regeneration_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "scope-narrowed-candidate-generators-regenerate" in result.output


def test_scope_narrowed_candidate_regeneration_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    output_dir = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "scope-narrowed-candidate-generators-regenerate",
            "--scope-review-dir",
            str(fixture["scope_review_dir"]),
            "--refined-generator-dir",
            str(fixture["refined_generator_dir"]),
            "--refined-validation-dir",
            str(fixture["refined_validation_dir"]),
            "--include-candidates",
            (
                "baseline_plus_trend_structure_refined_confidence_v1,"
                "volatility_regime_refined_confidence_v1"
            ),
            "--archive-candidates",
            "risk_appetite_refined_confidence_v1",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "scope_narrowed_regeneration",
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    summary_path = output_dir / "scope_narrowed_regeneration_run_summary.json"
    assert summary_path.exists()
    assert (output_dir / "scope_narrowed_regeneration_validation_summary.json").exists()
    assert (
        output_dir
        / "baseline_plus_trend_structure_scope_narrowed_confirmation_v1"
        / "scope_narrowed_candidate_prediction_artifact.json"
    ).exists()
    assert (
        output_dir
        / "volatility_regime_scope_narrowed_risk_cap_v1"
        / "scope_narrowed_candidate_prediction_artifact.json"
    ).exists()
    assert (
        output_dir
        / "risk_appetite_archive"
        / "risk_appetite_current_form_archive_record.json"
    ).exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["summary"]["actual_path_validation_executed"] is False
