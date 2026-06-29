from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_first_layer_candidate_generators_regenerate_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-candidate-generators-regenerate" in result.output


def test_first_layer_candidate_generators_regenerate_cli_writes_artifacts(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "first-layer-candidate-generators-regenerate",
            "--candidates",
            "baseline_plus_trend_structure,risk_appetite,volatility_regime",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--start-date",
            "2023-01-01",
            "--end-date",
            "2023-01-31",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(tmp_path),
            "--mode",
            "regenerated_candidate_artifacts",
        ],
    )

    assert result.exit_code == 0, result.output
    for candidate_id in (
        "baseline_plus_trend_structure",
        "risk_appetite",
        "volatility_regime",
    ):
        candidate_dir = tmp_path / candidate_id
        assert (candidate_dir / "candidate_signal_spec.json").exists()
        assert (candidate_dir / "candidate_signal_series.csv").exists()
        assert (candidate_dir / "candidate_prediction_artifact.json").exists()
        validation = json.loads(
            (candidate_dir / "validation_summary.json").read_text(encoding="utf-8")
        )
        artifact = json.loads(
            (candidate_dir / "candidate_prediction_artifact.json").read_text(
                encoding="utf-8"
            )
        )
        assert validation["status"] == "PASS"
        assert artifact["artifact_role"] == "regenerated_executable_candidate_artifact"
        assert artifact["historical_executable_artifact"] is True
        assert artifact["actual_path_validation_ready"] is False
        assert artifact["promotion_allowed"] is False
        assert artifact["paper_shadow_allowed"] is False
        assert artifact["production_allowed"] is False
        assert artifact["broker_action"] == "none"

    run_summary = json.loads(
        (tmp_path / "regeneration_run_summary.json").read_text(encoding="utf-8")
    )
    top_validation = json.loads(
        (tmp_path / "validation_summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["candidate_count"] == 3
    assert run_summary["promotion_allowed"] is False
    assert run_summary["paper_shadow_allowed"] is False
    assert run_summary["production_allowed"] is False
    assert run_summary["broker_action"] == "none"
    assert top_validation["status"] == "PASS"


def test_first_layer_candidate_generators_regenerate_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "first-layer-candidate-generators-regenerate",
            "--candidates",
            "baseline_plus_trend_structure",
            "--target-assets",
            "QQQ",
            "--start-date",
            "2023-01-01",
            "--end-date",
            "2023-01-31",
            "--horizons",
            "5d",
            "--output-dir",
            str(tmp_path),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0
