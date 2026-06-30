from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_forward_observe_readiness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
)


def test_forward_observe_readiness_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "scope-narrowed-forward-observe-readiness-review" in result.output


def test_forward_observe_readiness_review_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    output_dir = tmp_path / "forward_observe_readiness"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "scope-narrowed-forward-observe-readiness-review",
            "--scope-validation-dir",
            str(fixture["scope_validation_dir"]),
            "--scope-generator-dir",
            str(fixture["scope_narrowed_generator_dir"]),
            "--scope-review-dir",
            str(fixture["scope_review_dir"]),
            "--candidate",
            RISK_CAP_CANDIDATE_ID,
            "--rejected-candidates",
            CONFIRMATION_CANDIDATE_ID,
            "--archived-candidates",
            RISK_APPETITE_ARCHIVE_CANDIDATE,
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "forward_observe_readiness_review",
            "--docs-root",
            str(tmp_path / "docs"),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "forward_observe_readiness_review_summary.json",
        "forward_observe_candidate_readiness_matrix.json",
        "forward_observe_gate_checklist.json",
        "forward_observe_evidence_collection_spec.json",
        "forward_observe_daily_report_contract.json",
        "forward_observe_weekly_review_contract.json",
        "forward_observe_stop_continue_rules.json",
        "forward_observe_operational_boundary.json",
        "risk_cap_forward_observe_metric_spec.json",
        "risk_cap_trigger_interpretation_spec.json",
        "rejected_candidate_carry_forward_matrix.json",
        "archived_candidate_carry_forward_matrix.json",
        "forward_observe_next_task_recommendation.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename

    summary = json.loads(
        (output_dir / "forward_observe_readiness_review_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["forward_observe_started"] is False
    assert summary["candidate_reviewed"] == RISK_CAP_CANDIDATE_ID


def test_forward_observe_readiness_review_cli_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "scope-narrowed-forward-observe-readiness-review",
            "--scope-validation-dir",
            str(fixture["scope_validation_dir"]),
            "--scope-generator-dir",
            str(fixture["scope_narrowed_generator_dir"]),
            "--scope-review-dir",
            str(fixture["scope_review_dir"]),
            "--candidate",
            RISK_CAP_CANDIDATE_ID,
            "--rejected-candidates",
            CONFIRMATION_CANDIDATE_ID,
            "--archived-candidates",
            RISK_APPETITE_ARCHIVE_CANDIDATE,
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0
