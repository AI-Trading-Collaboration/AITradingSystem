from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_regenerated_actual_path_validation_fixture
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_regenerated_candidate_inconclusive_diagnostics_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "regenerated-candidate-inconclusive-diagnostics" in result.output


def test_regenerated_candidate_inconclusive_diagnostics_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)
    output_dir = tmp_path / "inconclusive_diagnostics"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regenerated-candidate-inconclusive-diagnostics",
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
            "inconclusive_diagnostics",
            "--docs-root",
            str(tmp_path / "diagnostics_docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "inconclusive_diagnostics_summary.json",
        "candidate_signal_density_matrix.json",
        "candidate_horizon_asset_drilldown.json",
        "candidate_false_signal_cost_matrix.json",
        "candidate_signal_overlap_matrix.json",
        "candidate_refinement_recommendation_matrix.json",
        "candidate_utility_drilldown_summary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists()

    summary = json.loads(
        (output_dir / "inconclusive_diagnostics_summary.json").read_text(encoding="utf-8")
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"


def test_regenerated_candidate_inconclusive_diagnostics_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_regenerated_actual_path_validation_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "regenerated-candidate-inconclusive-diagnostics",
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
            str(tmp_path / "diagnostics_docs"),
        ],
    )

    assert result.exit_code != 0
