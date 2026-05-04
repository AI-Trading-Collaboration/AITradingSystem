from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.rule_governance import (
    build_rule_version_manifest,
    load_rule_card_store,
    lookup_rule_card,
    render_rule_governance_report,
    validate_rule_card_store,
)


def test_default_rule_cards_validate_as_baseline_registry() -> None:
    report = validate_rule_card_store(
        load_rule_card_store(),
        as_of=date(2026, 5, 4),
    )
    markdown = render_rule_governance_report(report)

    assert report.passed is True
    assert report.production_count >= 5
    assert report.candidate_count == 0
    assert "baseline_recorded" in markdown
    assert "scoring.weighted_score.v1" in markdown
    manifest = build_rule_version_manifest(report, applies_to="score-daily")
    assert manifest["registry_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert manifest["production_rule_count"] >= 1
    assert manifest["manifest_scope"] == (
        "current_rule_registry_for_this_run_not_historical_approval_proof"
    )
    rule_ids = {rule["rule_id"] for rule in manifest["rules"]}
    assert "scoring.weighted_score.v1" in rule_ids


def test_rule_card_validation_rejects_duplicate_rule_ids(tmp_path: Path) -> None:
    input_path = tmp_path / "rule_cards.yaml"
    card = _card("scoring.weighted_score.v1")
    input_path.write_text(f"cards:\n{card}{card}", encoding="utf-8")

    report = validate_rule_card_store(
        load_rule_card_store(input_path),
        as_of=date(2026, 5, 4),
    )

    assert report.passed is False
    assert "duplicate_rule_card_id" in {issue.code for issue in report.issues}


def test_candidate_rule_card_requires_rule_experiment(tmp_path: Path) -> None:
    input_path = tmp_path / "rule_cards.yaml"
    input_path.write_text(
        "cards:\n"
        + _card(
            "candidate.position_gate.v2",
            status="candidate",
            approval_status="pending_approval",
            validation_status="pending_validation",
            production_since="",
        ),
        encoding="utf-8",
    )

    report = validate_rule_card_store(
        load_rule_card_store(input_path),
        as_of=date(2026, 5, 4),
    )

    assert report.passed is False
    assert "rule_card_load_error" in {issue.code for issue in report.issues}


def test_feedback_rule_card_cli_validates_and_looks_up_card(tmp_path: Path) -> None:
    input_path = tmp_path / "rule_cards.yaml"
    output_path = tmp_path / "rule_governance.md"
    input_path.write_text("cards:\n" + _card("scoring.weighted_score.v1"), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "validate-rule-cards",
            "--input-path",
            str(input_path),
            "--as-of",
            "2026-05-04",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "规则治理状态：PASS" in result.output
    assert output_path.exists()
    card = lookup_rule_card(input_path, "scoring.weighted_score.v1")
    assert card.status == "production"
    lookup = CliRunner().invoke(
        app,
        [
            "feedback",
            "lookup-rule-card",
            "--input-path",
            str(input_path),
            "--id",
            "scoring.weighted_score.v1",
        ],
    )
    assert lookup.exit_code == 0
    assert "加权模块评分规则" in lookup.output


def _card(
    rule_id: str,
    *,
    status: str = "production",
    approval_status: str = "baseline_recorded",
    validation_status: str = "baseline_tested",
    production_since: str = "2026-05-04",
) -> str:
    production_line = f"    production_since: {production_since}\n" if production_since else ""
    return f"""  - rule_id: {rule_id}
    rule_name: 加权模块评分规则
    rule_type: scoring
    status: {status}
    version: v1
    owner: system_review
    applies_to:
      - score-daily
    source_config_paths:
      - pyproject.toml
    description: 测试规则卡。
{production_line}    retired_at:
    linked_rule_experiment: ""
    approval:
      approval_status: {approval_status}
      approved_by: system_implementation
      approved_at: 2026-05-04
      rationale: baseline record.
    validation:
      validation_status: {validation_status}
      validation_refs:
        - tests
      sample_limitations:
        - sample limited
    rollback:
      procedure: restore previous config
      trigger_conditions:
        - validation failure
    known_limitations:
      - fixture
    last_reviewed_at: 2026-05-04
    next_review_due: 2026-06-04
"""
