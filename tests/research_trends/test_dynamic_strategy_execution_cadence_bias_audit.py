from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    NEXT_ROUTE,
    READY_STATUS,
    SCENARIO_ORDER,
    run_dynamic_strategy_execution_cadence_bias_audit,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_execution_cadence_bias_audit_builder(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_trends" / "cadence_audit"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_execution_cadence_bias_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_root=docs_root,
        strategy_ids=[
            "limited_adjustment",
            "dynamic_v0_5_ai_trend_confirmed_only",
        ],
        as_of_date=as_of,
        turnover_penalty=0.001,
    )

    assert payload["status"] == READY_STATUS
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality"]["passed"] is True
    assert set(payload["scenarios_tested"]) == set(SCENARIO_ORDER)
    assert payload["required_conclusions"]["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["scheduler_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["daily_report_generated"] is False

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "markdown_path",
        "comparison_matrix_json",
        "comparison_matrix_markdown",
        "next_steps_json",
        "next_steps_markdown",
    ):
        assert Path(artifact_paths[key]).exists()

    matrix = payload["comparison_matrix"]
    assert {row["scenario_id"] for row in matrix} >= set(SCENARIO_ORDER)
    monthly = next(row for row in matrix if row["scenario_id"] == "monthly_rebalance")
    event = next(row for row in matrix if row["scenario_id"] == "signal_event_driven")
    valid_until = next(
        row
        for row in payload["scenario_rows"]
        if row["scenario_id"] == "valid_until_window"
    )
    cooldown = next(
        row
        for row in matrix
        if row["scenario_id"] == "cooldown_limited_event_driven"
    )

    assert monthly["old_cadence_reference"] is True
    assert event["uses_future_data"] is False
    assert event["signal_to_execution_lag_days"] >= 1.0
    assert valid_until["staleness_controls"]["staleness_filter_enabled"] is True
    assert valid_until["staleness_controls"]["expired_signal_suppression_rule"] == (
        "suppress_rebalance"
    )
    assert cooldown["cooldown_block_count"] >= 0
    assert "cost_adjusted_improvement" in event
    assert "turnover_adjusted_improvement" in event
    assert "dynamic_vs_static_gap" in event
    assert "missed_signal_count" in event

    saved_payload = json.loads(Path(artifact_paths["json_path"]).read_text(encoding="utf-8"))
    assert saved_payload["status"] == READY_STATUS
    assert saved_payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_execution_cadence_bias_audit_cli(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_trends" / "cadence_audit_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-execution-cadence-bias-audit",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
            "--strategy",
            "limited_adjustment",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_READY" in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert (output_root / "execution_cadence_bias_audit.json").exists()


def test_dynamic_strategy_execution_cadence_bias_audit_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_execution_cadence_bias_audit"]

    assert entry["command"] == (
        "aits research strategies dynamic-strategy-execution-cadence-bias-audit"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "execution_cadence_bias_audit.json" in item for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_execution_cadence_bias_audit" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-execution-cadence-bias-audit" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2364_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_AND_RETEST" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")
    assert "DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_READY" in Path(
        "docs/requirements/TRADING-2364_Dynamic_Strategy_Execution_Cadence_Bias_Audit_And_Retest.md"
    ).read_text(encoding="utf-8")


def _write_execution_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 760)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}

    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0007 + 0.0018 * math.sin(day_index / 19.0)
        if 80 <= day_index <= 125:
            qqq_return -= 0.006
        if 126 <= day_index <= 190:
            qqq_return += 0.004
        if 430 <= day_index <= 470:
            qqq_return -= 0.004
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= max(0.01, 1.0 + qqq_return * 3.0 - 0.00025)
        levels["SGOV"] *= 1.0 + 0.00016
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)

    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0004:.4f}\n")
        rate_rows.append(
            f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0003:.4f}\n"
        )
        rate_rows.append(
            f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n"
        )

    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path, dates[-1]


def _business_dates(start: date, count: int) -> list[date]:
    result = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result
