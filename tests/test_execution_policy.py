from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_policy import (
    build_execution_advisory,
    load_execution_policy,
    lookup_execution_action,
    render_execution_advisory_section,
    render_execution_policy_report,
    validate_execution_policy,
)


def test_default_execution_policy_validates_and_renders() -> None:
    report = validate_execution_policy(
        load_execution_policy(),
        as_of=date(2026, 5, 4),
    )
    markdown = render_execution_policy_report(report)

    assert report.passed is True
    assert report.status == "PASS"
    assert report.action_count == 6
    assert "Advisory Action Taxonomy" in markdown
    assert "`no_new_position`" in markdown
    assert "不直接执行交易" in markdown


def test_execution_advisory_keeps_missing_baseline_observe_only() -> None:
    policy = _policy()

    advisory = build_execution_advisory(
        policy=policy,
        current_band=(0.40, 0.60),
        previous_band=None,
        confidence_level="high",
        triggered_gate_ids=(),
        report_status="PASS",
    )
    section = render_execution_advisory_section(
        advisory,
        validation_status="PASS",
        validation_report_path=Path("execution_policy.md"),
    )

    assert advisory.action_id == "observe_only"
    assert advisory.production_effect == "none"
    assert "执行政策校验：PASS" in section
    assert "不是自动交易指令" in section


def test_execution_advisory_prioritizes_manual_review_conditions() -> None:
    policy = _policy()

    low_confidence = build_execution_advisory(
        policy=policy,
        current_band=(0.50, 0.70),
        previous_band=(0.30, 0.50),
        confidence_level="low",
        triggered_gate_ids=(),
        report_status="PASS",
    )
    risk_gate = build_execution_advisory(
        policy=policy,
        current_band=(0.50, 0.70),
        previous_band=(0.30, 0.50),
        confidence_level="high",
        triggered_gate_ids=("risk_events",),
        report_status="PASS",
    )

    assert low_confidence.action_id == "wait_manual_review"
    assert risk_gate.action_id == "wait_manual_review"
    assert any("risk_events" in reason for reason in risk_gate.reasons)


def test_execution_advisory_uses_fixed_action_taxonomy_for_band_changes() -> None:
    policy = _policy()

    no_new = build_execution_advisory(
        policy=policy,
        current_band=(0.50, 0.70),
        previous_band=(0.30, 0.50),
        confidence_level="high",
        triggered_gate_ids=("valuation",),
        report_status="PASS",
    )
    increase = build_execution_advisory(
        policy=policy,
        current_band=(0.50, 0.70),
        previous_band=(0.30, 0.50),
        confidence_level="high",
        triggered_gate_ids=(),
        report_status="PASS",
    )
    reduce = build_execution_advisory(
        policy=policy,
        current_band=(0.20, 0.30),
        previous_band=(0.50, 0.70),
        confidence_level="high",
        triggered_gate_ids=(),
        report_status="PASS",
    )
    maintain = build_execution_advisory(
        policy=policy,
        current_band=(0.42, 0.62),
        previous_band=(0.40, 0.60),
        confidence_level="high",
        triggered_gate_ids=(),
        report_status="PASS",
    )

    assert no_new.action_id == "no_new_position"
    assert increase.action_id == "small_increase"
    assert reduce.action_id == "reduce_to_target_range"
    assert maintain.action_id == "maintain"


def test_execution_policy_cli_validates_and_looks_up(tmp_path: Path) -> None:
    report_path = tmp_path / "execution_policy.md"

    result = CliRunner().invoke(
        app,
        [
            "execution",
            "validate",
            "--as-of",
            "2026-05-04",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "执行政策状态：PASS" in result.output
    assert report_path.exists()

    action = lookup_execution_action("config/execution_policy.yaml", "no_new_position")
    assert action.label == "禁止主动加仓"
    lookup = CliRunner().invoke(
        app,
        [
            "execution",
            "lookup",
            "--id",
            "no_new_position",
        ],
    )
    assert lookup.exit_code == 0
    assert "禁止主动加仓" in lookup.output
    assert "Action ID" in lookup.output


def _policy():
    store = load_execution_policy()
    assert store.policy is not None
    return store.policy
