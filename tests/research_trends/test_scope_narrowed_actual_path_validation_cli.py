from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_actual_path_validation_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
)


def test_scope_narrowed_actual_path_validation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "scope-narrowed-candidate-actual-path-validation" in result.output


def test_scope_narrowed_actual_path_validation_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    output_dir = tmp_path / "scope_narrowed_actual_path_validation"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "scope-narrowed-candidate-actual-path-validation",
            "--scope-narrowed-generator-dir",
            str(fixture["scope_narrowed_generator_dir"]),
            "--scope-review-dir",
            str(fixture["scope_review_dir"]),
            "--refined-validation-dir",
            str(fixture["refined_validation_dir"]),
            "--include-candidates",
            f"{CONFIRMATION_CANDIDATE_ID},{RISK_CAP_CANDIDATE_ID}",
            "--archived-candidates",
            RISK_APPETITE_ARCHIVE_CANDIDATE,
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "scope_narrowed_actual_path_validation",
            "--prices-path",
            str(fixture["prices_path"]),
            "--rates-path",
            str(fixture["rates_path"]),
            "--marketstack-prices-path",
            str(tmp_path / "missing_marketstack.csv"),
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "scope_narrowed_actual_path_validation_summary.json",
        "scope_narrowed_active_actual_path_matrix.json",
        "scope_narrowed_active_prediction_outcome_matrix.json",
        "scope_narrowed_inactive_reference_matrix.json",
        "scope_narrowed_active_vs_inactive_comparison.json",
        "confirmation_only_validation_scorecard.json",
        "risk_cap_only_validation_scorecard.json",
        "scope_narrowed_sample_sufficiency_report.json",
        "scope_narrowed_state_recommendation_matrix.json",
        "risk_appetite_archive_carry_forward.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename

    summary = json.loads(
        (output_dir / "scope_narrowed_actual_path_validation_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"


def test_scope_narrowed_actual_path_validation_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "scope-narrowed-candidate-actual-path-validation",
            "--scope-narrowed-generator-dir",
            str(fixture["scope_narrowed_generator_dir"]),
            "--scope-review-dir",
            str(fixture["scope_review_dir"]),
            "--refined-validation-dir",
            str(fixture["refined_validation_dir"]),
            "--include-candidates",
            CONFIRMATION_CANDIDATE_ID,
            "--archived-candidates",
            RISK_APPETITE_ARCHIVE_CANDIDATE,
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
