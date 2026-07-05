from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    CADENCE_ORDER,
    NEXT_ROUTE,
    PRIMARY_EXECUTION_CADENCE,
    READY_STATUS,
    run_dynamic_strategy_event_driven_retest,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    run_dynamic_strategy_execution_cadence_bias_audit,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_event_driven_retest_builder(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_path = _write_source_cadence_audit(
        tmp_path,
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        as_of=as_of,
    )
    output_root = tmp_path / "outputs" / "research_strategies" / "event_retest"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_event_driven_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_cadence_audit_path=source_path,
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
    assert payload["source_cadence_audit"]["ready_for_retest"] is True
    assert payload["primary_execution_cadence"] == PRIMARY_EXECUTION_CADENCE
    assert set(payload["cadences_tested"]) == set(CADENCE_ORDER)
    assert payload["conclusions"]["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["conclusions"]["monthly_results_deprecated"] is True
    assert payload["conclusions"]["cost_turnover_cooldown_sensitivity_required"] is True
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
        "candidate_ranking_json",
        "cadence_comparison_matrix_json",
        "markdown_path",
        "candidate_ranking_markdown",
        "decision_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()

    ranking = payload["candidate_ranking"]
    assert ranking
    assert {row["primary_execution_cadence"] for row in ranking} == {
        PRIMARY_EXECUTION_CADENCE
    }
    required_ranking_fields = {
        "rank",
        "candidate_id",
        "cost_adjusted_return",
        "max_drawdown",
        "turnover",
        "turnover_adjusted_score",
        "dynamic_vs_static_gap",
        "valid_until_vs_monthly_gap",
        "stale_signal_count",
        "false_risk_off_count",
        "missed_upside_count",
        "constraint_hit_count",
        "decision",
        "decision_reason",
    }
    assert required_ranking_fields <= set(ranking[0])
    assert ranking[0]["decision"] in {
        "ACCEPT_FOR_SHADOW_RESEARCH",
        "CONTINUE_RESEARCH",
        "OWNER_REVIEW_REQUIRED",
        "REJECT_FOR_NOW",
    }

    matrix = payload["cadence_comparison_matrix"]
    assert {row["scenario_id"] for row in matrix} >= set(CADENCE_ORDER)
    monthly_rows = [row for row in matrix if row["scenario_id"] == "monthly_rebalance"]
    assert monthly_rows
    assert all(row["deprecated_by_cadence_audit"] is True for row in monthly_rows)
    assert all(row["monthly_result_role"] == "deprecated_reference_only" for row in monthly_rows)
    valid_row = next(row for row in matrix if row["scenario_id"] == PRIMARY_EXECUTION_CADENCE)
    assert "valid_until_vs_monthly_gap" in valid_row
    assert "cost_adjusted_return" in valid_row
    assert "gross_return" in valid_row
    assert "stale_signal_execution_count" in valid_row
    assert valid_row["primary_execution_cadence"] == PRIMARY_EXECUTION_CADENCE

    ranking_payload = json.loads(
        Path(artifact_paths["candidate_ranking_json"]).read_text(encoding="utf-8")
    )
    assert ranking_payload["candidate_ranking"][0]["rank"] == 1


def test_dynamic_strategy_event_driven_retest_cli(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_path = _write_source_cadence_audit(
        tmp_path,
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        as_of=as_of,
    )
    output_root = tmp_path / "outputs" / "research_strategies" / "event_retest_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-event-driven-retest",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--source-cadence-audit",
            str(source_path),
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
    assert READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert (output_root / "event_driven_retest_result.json").exists()
    assert (output_root / "candidate_ranking.json").exists()
    assert (output_root / "cadence_comparison_matrix.json").exists()


def test_dynamic_strategy_event_driven_retest_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_event_driven_retest"]

    assert entry["command"] == "aits research strategies dynamic-strategy-event-driven-retest"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("event_driven_retest_result.json" in item for item in entry["artifact_globs"])
    assert any("candidate_ranking.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_event_driven_retest" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-event-driven-retest" in Path("docs/system_flow.md").read_text(
        encoding="utf-8"
    )
    assert "TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")
    assert READY_STATUS in Path(
        "docs/requirements/TRADING-2365_Dynamic_Strategy_Event_Driven_Retest_And_Candidate_Ranking.md"
    ).read_text(encoding="utf-8")


def _write_source_cadence_audit(
    tmp_path: Path,
    *,
    prices_path: Path,
    marketstack_path: Path,
    rates_path: Path,
    as_of: date,
) -> Path:
    source_output_root = tmp_path / "outputs" / "research_trends" / "cadence_audit"
    source_docs_root = tmp_path / "docs" / "source_research"
    source_payload = run_dynamic_strategy_execution_cadence_bias_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=source_output_root,
        docs_root=source_docs_root,
        strategy_ids=[
            "limited_adjustment",
            "dynamic_v0_5_ai_trend_confirmed_only",
        ],
        as_of_date=as_of,
        turnover_penalty=0.001,
    )
    assert source_payload["status"] == "DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_READY"
    source_payload["summary"]["cadence_bias_detected"] = True
    source_payload["summary"]["old_dynamic_strategy_results_need_retest"] = True
    source_payload["summary"]["recommended_default_execution_cadence"] = (
        PRIMARY_EXECUTION_CADENCE
    )
    source_payload["conclusions"]["cadence_bias_detected"] = True
    source_payload["conclusions"]["old_dynamic_strategy_results_need_retest"] = True
    source_payload["conclusions"]["recommended_default_execution_cadence"] = (
        PRIMARY_EXECUTION_CADENCE
    )
    source_payload["conclusions"]["monthly_rebalance_distorts_signal_response"] = "YES"
    source_path = Path(source_payload["artifact_paths"]["json_path"])
    source_path.write_text(
        json.dumps(source_payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return source_path


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
