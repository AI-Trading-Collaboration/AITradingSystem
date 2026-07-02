from __future__ import annotations

from pathlib import Path

from exposure_cap_diagnostics_review_fixtures import (
    build_exposure_cap_diagnostics_review_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_exposure_cap_diagnostics_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "exposure-cap-vs-no-cap-diagnostics-review" in result.output


def test_exposure_cap_diagnostics_review_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_exposure_cap_diagnostics_review_fixture(tmp_path)
    output_dir = tmp_path / "diagnostics"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "exposure-cap-vs-no-cap-diagnostics-review",
            "--dry-run-dir",
            str(fixture["dry_run_dir"]),
            "--source-binding-dir",
            str(fixture["source_binding_dir"]),
            "--baseline-decision-dir",
            str(fixture["baseline_decision_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "diagnostics_review",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "exposure_cap_diagnostics_review_summary.json",
        "cap_binding_diagnostics_matrix.json",
        "cap_binding_diagnostics_matrix.csv",
        "exposure_reduction_diagnostics_matrix.json",
        "exposure_reduction_diagnostics_matrix.csv",
        "return_drawdown_proxy_diagnostics.json",
        "return_drawdown_proxy_diagnostics.csv",
        "turnover_cooldown_diagnostics.json",
        "turnover_cooldown_diagnostics.csv",
        "false_cost_missed_upside_diagnostics.json",
        "false_cost_missed_upside_diagnostics.csv",
        "downside_protection_diagnostics.json",
        "downside_protection_diagnostics.csv",
        "cap_binding_period_attribution.json",
        "cap_binding_period_attribution.csv",
        "policy_sensitivity_recommendation_matrix.json",
        "dynamic_baseline_readiness_recommendation.json",
        "exposure_cap_diagnostics_decision_matrix.json",
        "exposure_cap_2328_task_route.json",
        "diagnostics_interpretation_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in ("target_weights.csv", "rebalance_instruction.json", "broker_order.json"):
        assert not (output_dir / filename).exists(), filename

    summary = read_json(output_dir / "exposure_cap_diagnostics_review_summary.json")
    assert summary["data_validation_policy"] == (
        "NOT_APPLICABLE_PRIOR_VALIDATED_DRY_RUN_ARTIFACTS_ONLY"
    )
    assert summary["aits_validate_data_executed"] is False
    assert summary["prior_data_quality_report_present"] is True
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "exposure_cap_vs_no_cap_diagnostics_review.md").exists()
