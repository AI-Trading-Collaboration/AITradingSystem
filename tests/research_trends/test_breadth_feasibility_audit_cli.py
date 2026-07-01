from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_breadth_feasibility_audit_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "breadth-participation-candidate-family-feasibility-audit" in result.output


def test_breadth_feasibility_audit_cli_writes_required_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "breadth_audit"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "breadth-participation-candidate-family-feasibility-audit",
            "--target-etfs",
            "QQQ,SPY,SMH",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--candidate-family",
            "breadth_participation",
            "--output-dir",
            str(output_dir),
            "--mode",
            "feasibility_audit",
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "breadth_participation_feasibility_summary.json",
        "breadth_input_data_inventory.json",
        "breadth_input_data_inventory.csv",
        "historical_constituent_pit_gap_matrix.json",
        "historical_constituent_pit_gap_matrix.csv",
        "current_constituents_proxy_risk_matrix.json",
        "current_constituents_proxy_risk_matrix.csv",
        "breadth_candidate_family_design_sketch.json",
        "breadth_candidate_signal_concept_matrix.json",
        "breadth_candidate_signal_concept_matrix.csv",
        "breadth_candidate_validation_route_matrix.json",
        "breadth_candidate_validation_route_matrix.csv",
        "breadth_data_feasibility_recommendation_matrix.json",
        "breadth_2303_task_route.json",
        "breadth_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in [
        "breadth_participation_candidate_family_feasibility_audit.md",
        "breadth_participation_data_inventory.md",
        "breadth_participation_pit_and_bias_risk.md",
        "breadth_participation_candidate_family_design_sketch.md",
        "breadth_participation_2303_task_route.md",
    ]:
        assert (docs_root / filename).exists(), filename

    summary = json.loads(
        (output_dir / "breadth_participation_feasibility_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["generator_implemented"] is False
    assert summary["actual_path_validation_executed"] is False


def test_breadth_feasibility_audit_cli_rejects_wrong_mode(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "breadth-participation-candidate-family-feasibility-audit",
            "--target-etfs",
            "QQQ,SPY,SMH",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--candidate-family",
            "breadth_participation",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0
