from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_regenerated_artifact_fixture
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_regenerated_candidate_actual_path_validation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "regenerated-candidate-actual-path-validation" in result.output


def test_regenerated_candidate_actual_path_validation_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    output_dir = tmp_path / "actual_path_validation"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regenerated-candidate-actual-path-validation",
            "--input-dir",
            str(fixture["input_dir"]),
            "--candidates",
            "baseline_plus_trend_structure,risk_appetite,volatility_regime",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "actual_path_validation",
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "regenerated_candidate_actual_path_validation_summary.json",
        "regenerated_candidate_actual_path_matrix.json",
        "regenerated_candidate_actual_path_matrix.csv",
        "candidate_prediction_outcome_matrix.json",
        "candidate_prediction_outcome_matrix.csv",
        "candidate_validation_scorecard.json",
        "candidate_error_attribution_seed.json",
        "candidate_data_quality_report.json",
        "candidate_state_recommendation_matrix.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists()

    summary = json.loads(
        (output_dir / "regenerated_candidate_actual_path_validation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"


def test_regenerated_candidate_actual_path_validation_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regenerated-candidate-actual-path-validation",
            "--input-dir",
            str(fixture["input_dir"]),
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
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
        ],
    )

    assert result.exit_code != 0
