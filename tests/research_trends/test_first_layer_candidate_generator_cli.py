from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_first_layer_candidate_generator_framework_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-candidate-generator-framework" in result.output


def test_first_layer_candidate_generator_framework_cli_writes_artifacts(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "first-layer-candidate-generator-framework",
            "--candidate-id",
            "framework_smoke_candidate",
            "--target-asset",
            "QQQ",
            "--start-date",
            "2023-01-01",
            "--end-date",
            "2023-01-31",
            "--horizon",
            "10d",
            "--output-dir",
            str(tmp_path),
            "--mode",
            "framework_smoke_test",
        ],
    )

    assert result.exit_code == 0, result.output
    expected_paths = [
        tmp_path / "framework_smoke_candidate_signal_spec.json",
        tmp_path / "framework_smoke_candidate_signal_series.csv",
        tmp_path / "framework_smoke_candidate_prediction_artifact.json",
        tmp_path / "framework_smoke_candidate_generation_summary.json",
        tmp_path / "framework_smoke_candidate_validation_summary.json",
        tmp_path / "generator_registry.json",
    ]
    for path in expected_paths:
        assert path.exists()

    validation = json.loads(
        (tmp_path / "framework_smoke_candidate_validation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    artifact = json.loads(
        (tmp_path / "framework_smoke_candidate_prediction_artifact.json").read_text(
            encoding="utf-8"
        )
    )
    assert validation["status"] == "PASS"
    assert artifact["promotion_allowed"] is False
    assert artifact["paper_shadow_allowed"] is False
    assert artifact["production_allowed"] is False
    assert artifact["broker_action"] == "none"
    assert artifact["actual_path_validation_ready"] is False
    assert artifact["historical_executable_artifact"] is False


def test_first_layer_candidate_generator_framework_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "first-layer-candidate-generator-framework",
            "--candidate-id",
            "framework_smoke_candidate",
            "--target-asset",
            "QQQ",
            "--start-date",
            "2023-01-01",
            "--end-date",
            "2023-01-31",
            "--horizon",
            "10d",
            "--output-dir",
            str(tmp_path),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_report_registry_marks_framework_smoke_artifacts_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "first_layer_candidate_generator_framework"
    )

    assert entry["command"] == "aits research trends first-layer-candidate-generator-framework"
    assert entry["artifact_role"] == "framework_smoke_test"
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
