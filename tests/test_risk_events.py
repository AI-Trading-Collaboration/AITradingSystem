from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    RiskEventRuleConfig,
    RiskEventsConfig,
    load_industry_chain,
    load_risk_events,
    load_universe,
    load_watchlist,
)
from ai_trading_system.risk_events import (
    render_risk_events_validation_report,
    validate_risk_events_config,
    write_risk_events_validation_report,
)


def test_validate_risk_events_config_passes_default_config() -> None:
    report = validate_risk_events_config(
        risk_events=load_risk_events(),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.error_count == 0
    assert report.active_rule_count >= 3


def test_validate_risk_events_config_rejects_unknown_node() -> None:
    config = load_risk_events()
    first_rule = config.event_rules[0]
    broken_rule = first_rule.model_copy(update={"affected_nodes": ["unknown_node"]})
    broken = RiskEventsConfig(
        levels=config.levels,
        event_rules=[broken_rule, *config.event_rules[1:]],
    )

    report = validate_risk_events_config(
        risk_events=broken,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "unknown_affected_node" in {issue.code for issue in report.issues}


def test_validate_risk_events_config_rejects_unknown_ticker() -> None:
    config = load_risk_events()
    first_rule = config.event_rules[0]
    broken_rule = first_rule.model_copy(update={"related_tickers": ["UNKNOWN"]})
    broken = RiskEventsConfig(
        levels=config.levels,
        event_rules=[broken_rule, *config.event_rules[1:]],
    )

    report = validate_risk_events_config(
        risk_events=broken,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "unknown_related_ticker" in {issue.code for issue in report.issues}


def test_validate_risk_events_config_warns_on_missing_escalation_conditions() -> None:
    config = load_risk_events()
    broken_rule = RiskEventRuleConfig(
        event_id="test_l2_without_escalation",
        name="测试风险",
        level="L2",
        description="测试风险事件",
        affected_nodes=["export_controls"],
        related_tickers=["NVDA"],
        trigger_examples=["测试触发"],
        recommended_actions=["人工复核"],
        escalation_conditions=[],
        deescalation_conditions=[],
        active=True,
    )
    broken = RiskEventsConfig(levels=config.levels, event_rules=[broken_rule])

    report = validate_risk_events_config(
        risk_events=broken,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert report.warning_count == 2
    assert "missing_escalation_conditions" in {issue.code for issue in report.issues}
    assert "missing_deescalation_conditions" in {issue.code for issue in report.issues}


def test_render_and_write_risk_events_report(tmp_path: Path) -> None:
    report = validate_risk_events_config(
        risk_events=load_risk_events(),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    markdown = render_risk_events_validation_report(report)
    output_path = write_risk_events_validation_report(report, tmp_path / "risk_events.md")

    assert "- 状态：PASS" in markdown
    assert "ai_chip_export_control_upgrade" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_risk_events_cli_validate_and_list(tmp_path: Path) -> None:
    output_path = tmp_path / "risk_events_validation.md"

    validate_result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "validate",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )
    list_result = CliRunner().invoke(app, ["risk-events", "list"])

    assert validate_result.exit_code == 0
    assert list_result.exit_code == 0
    assert output_path.exists()
    assert "风险事件校验状态：PASS" in validate_result.output
    assert "风险事件规则" in list_result.output
    assert "L2" in list_result.output
