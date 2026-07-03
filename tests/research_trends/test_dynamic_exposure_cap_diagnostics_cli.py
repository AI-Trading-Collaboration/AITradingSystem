from __future__ import annotations

from pathlib import Path

from dynamic_exposure_cap_diagnostics_review_fixtures import (
    build_dynamic_exposure_cap_diagnostics_review_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_dynamic_exposure_cap_diagnostics_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "dynamic-exposure-cap-vs-no-cap-diagnostics-review" in result.output


def test_dynamic_exposure_cap_diagnostics_cli_writes_required_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_exposure_cap_diagnostics_review_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "dynamic-exposure-cap-vs-no-cap-diagnostics-review",
            "--dynamic-dry-run-dir",
            str(fixture["dynamic_dry_run_dir"]),
            "--static-diagnostics-dir",
            str(fixture["static_diagnostics_dir"]),
            "--static-dry-run-dir",
            str(fixture["static_dry_run_dir"]),
            "--readiness-dir",
            str(fixture["dry_run_readiness_dir"]),
            "--timestamp-remediation-dir",
            str(fixture["timestamp_remediation_dir"]),
            "--simulation-policy-dir",
            str(fixture["simulation_policy_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "dynamic_diagnostics_review",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "dynamic_exposure_cap_diagnostics_review_summary.json",
        "dynamic_cap_binding_diagnostics_matrix.json",
        "dynamic_overbinding_diagnostics.json",
        "dynamic_exposure_reduction_diagnostics.json",
        "dynamic_return_drawdown_tradeoff_diagnostics.json",
        "dynamic_false_cost_missed_upside_diagnostics.json",
        "dynamic_downside_protection_diagnostics.json",
        "dynamic_turnover_cooldown_diagnostics.json",
        "dynamic_strategy_overlap_diagnostics.json",
        "static_vs_dynamic_exposure_cap_evidence_comparison.json",
        "dynamic_exposure_cap_decision_matrix.json",
        "dynamic_2334_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "dynamic_exposure_cap_diagnostics_review_summary.json")
    assert summary["data_validation_policy"] == (
        "NOT_APPLICABLE_PRIOR_VALIDATED_DYNAMIC_DRY_RUN_ARTIFACTS_ONLY"
    )
    assert summary["aits_validate_data_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert (docs_root / "dynamic_exposure_cap_vs_no_cap_diagnostics_review.md").exists()
