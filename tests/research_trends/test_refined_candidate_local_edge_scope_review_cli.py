from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import build_refined_scope_review_input_fixture
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app

CANDIDATES = (
    "baseline_plus_trend_structure_refined_confidence_v1,"
    "risk_appetite_refined_confidence_v1,"
    "volatility_regime_refined_confidence_v1"
)


def test_refined_candidate_local_edge_scope_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "refined-candidate-local-edge-scope-review" in result.output


def test_refined_candidate_local_edge_scope_review_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)
    output_dir = tmp_path / "scope_review"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "refined-candidate-local-edge-scope-review",
            "--refined-validation-dir",
            str(fixture["refined_validation_dir"]),
            "--refined-generator-dir",
            str(fixture["refined_generator_dir"]),
            "--refinement-plan-dir",
            str(fixture["refinement_plan_dir"]),
            "--candidates",
            CANDIDATES,
            "--continue-research-candidates",
            (
                "baseline_plus_trend_structure_refined_confidence_v1,"
                "volatility_regime_refined_confidence_v1"
            ),
            "--reject-candidates",
            "risk_appetite_refined_confidence_v1",
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "local_edge_scope_review",
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    for filename in [
        "local_edge_scope_review_summary.json",
        "candidate_local_edge_matrix.json",
        "candidate_asset_scope_matrix.json",
        "candidate_horizon_scope_matrix.json",
        "candidate_direction_scope_matrix.json",
        "candidate_high_conviction_scope_matrix.json",
        "candidate_false_cost_scope_matrix.json",
        "candidate_scope_narrowing_recommendation_matrix.json",
        "risk_appetite_reject_record.json",
        "candidate_next_task_recommendation_matrix.json",
    ]:
        assert (output_dir / filename).exists()

    summary = json.loads(
        (output_dir / "local_edge_scope_review_summary.json").read_text(encoding="utf-8")
    )
    assert summary["summary"]["risk_appetite_reject_record_generated"] is True
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"


def test_refined_candidate_local_edge_scope_review_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_refined_scope_review_input_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "refined-candidate-local-edge-scope-review",
            "--refined-validation-dir",
            str(fixture["refined_validation_dir"]),
            "--refined-generator-dir",
            str(fixture["refined_generator_dir"]),
            "--refinement-plan-dir",
            str(fixture["refinement_plan_dir"]),
            "--candidates",
            CANDIDATES,
            "--continue-research-candidates",
            "baseline_plus_trend_structure_refined_confidence_v1",
            "--reject-candidates",
            "risk_appetite_refined_confidence_v1",
            "--target-assets",
            "QQQ",
            "--horizons",
            "5d",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0
