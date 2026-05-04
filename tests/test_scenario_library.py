from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_industry_chain, load_risk_events, load_watchlist
from ai_trading_system.scenario_library import (
    load_scenario_library,
    lookup_scenario,
    render_scenario_library_report,
    validate_scenario_library,
)


def test_default_scenario_library_validates_mappings() -> None:
    report = validate_scenario_library(
        load_scenario_library(),
        as_of=date(2026, 5, 4),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
    )
    markdown = render_scenario_library_report(report)

    assert report.passed is True
    assert report.status == "PASS"
    assert report.scenario_count >= 10
    assert "cloud_capex_downshift" in markdown
    assert "情景用于压力测试、脆弱点识别和人工复核提示" in markdown
    assert "risk_events:cap_lower" in markdown


def test_scenario_library_rejects_unknown_node(tmp_path: Path) -> None:
    input_path = tmp_path / "scenario_library.yaml"
    input_path.write_text(
        _library_yaml(affected_nodes="      - missing_node\n"),
        encoding="utf-8",
    )

    report = validate_scenario_library(
        load_scenario_library(input_path),
        as_of=date(2026, 5, 4),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
    )

    assert report.passed is False
    assert "unknown_affected_node" in {issue.code for issue in report.issues}


def test_scenario_library_rejects_probability_forecasts(tmp_path: Path) -> None:
    input_path = tmp_path / "scenario_library.yaml"
    input_path.write_text(
        _library_yaml(not_probability_forecast="false"),
        encoding="utf-8",
    )

    report = validate_scenario_library(
        load_scenario_library(input_path),
        as_of=date(2026, 5, 4),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
    )

    assert report.passed is False
    assert "scenario_probability_forecast_not_allowed" in {
        issue.code for issue in report.issues
    }


def test_scenarios_cli_validates_and_looks_up(tmp_path: Path) -> None:
    report_path = tmp_path / "scenario_library.md"

    result = CliRunner().invoke(
        app,
        [
            "scenarios",
            "validate",
            "--as-of",
            "2026-05-04",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "情景库状态：PASS" in result.output
    assert report_path.exists()

    scenario = lookup_scenario("config/scenario_library.yaml", "export_control_upgrade")
    assert scenario.linked_risk_event_ids == ["ai_chip_export_control_upgrade"]
    lookup = CliRunner().invoke(
        app,
        [
            "scenarios",
            "lookup",
            "--id",
            "export_control_upgrade",
        ],
    )
    assert lookup.exit_code == 0
    assert "出口管制升级" in lookup.output
    assert "risk_events:cap_lower" in lookup.output


def _library_yaml(
    *,
    affected_nodes: str = "      - cloud_capex\n",
    not_probability_forecast: str = "true",
) -> str:
    return f"""library_id: test_scenario_library.v1
version: v1
status: production
owner: test
description: test library
last_reviewed_at: 2026-05-04
next_review_due: 2026-06-04
scenarios:
  - scenario_id: test_scenario
    name: Test scenario
    status: active
    scenario_type: hypothetical_shock
    shock_direction: negative
    severity: medium
    affected_nodes:
{affected_nodes}    affected_tickers:
      - MSFT
    linked_risk_event_ids: []
    linked_thesis_ids: []
    position_gate_impacts:
      - gate_id: risk_events
        expected_effect: cap_lower
        max_ai_exposure_hint: 0.5
        rationale: test gate impact
    observation_conditions:
      - test condition
    review_requirements:
      - test review
    evidence_requirements:
      - test evidence
    interpretation_boundary: test boundary
    not_probability_forecast: {not_probability_forecast}
"""
