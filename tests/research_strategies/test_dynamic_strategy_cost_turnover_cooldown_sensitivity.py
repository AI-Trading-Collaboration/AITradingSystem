from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    NEXT_ROUTE,
    PRIMARY_EXECUTION_CADENCE,
    READY_STATUS,
    run_dynamic_strategy_cost_turnover_cooldown_sensitivity,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    run_dynamic_strategy_event_driven_retest,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    run_dynamic_strategy_execution_cadence_bias_audit,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_cost_turnover_cooldown_sensitivity_builder(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_event_retest(
        tmp_path,
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        as_of=as_of,
    )
    output_root = tmp_path / "outputs" / "research_strategies" / "sensitivity"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_cost_turnover_cooldown_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_event_retest_path=source_paths["result"],
        source_candidate_ranking_path=source_paths["ranking"],
        source_cadence_matrix_path=source_paths["matrix"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=as_of,
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_retest"]["ready_for_sensitivity"] is True
    assert payload["primary_execution_cadence"] == PRIMARY_EXECUTION_CADENCE
    assert payload["top_candidate_from_2365"]
    assert payload["top_candidate_decision_from_2365"]
    assert payload["sensitivity_analysis_ready"] is True
    assert payload["sensitivity_grid_ready"] is True
    assert payload["cost_adjusted_metrics_ready"] is True
    assert payload["turnover_metrics_ready"] is True
    assert payload["cooldown_metrics_ready"] is True
    assert payload["decision_update_ready"] is True
    assert payload["summary"]["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["scheduler_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["daily_report_generated"] is False

    grid = payload["sensitivity_grid"]
    assert grid["grid_type"] == "layered_sensitivity_grid"
    assert grid["scenario_count"] >= 69
    assert {"combined_base", "combined_realistic", "combined_conservative", "combined_harsh"} <= {
        row["scenario_id"] for row in grid["scenarios"]
    }

    matrix = payload["sensitivity_matrix"]
    assert matrix
    dynamic_row = next(row for row in matrix if not row["is_static_baseline"])
    assert dynamic_row["execution_cadence"] in {
        "valid_until_window",
        "cooldown_limited_event_driven",
    }
    assert "cost_adjusted_return" in dynamic_row["cost_metrics"]
    assert "transaction_cost_drag" in dynamic_row["cost_metrics"]
    assert "turnover_reduction_vs_base" in dynamic_row["turnover_metrics"]
    assert "cooldown_block_count" in dynamic_row["cooldown_metrics"]
    assert "cooldown_adjusted_return_gap" in dynamic_row["cooldown_metrics"]

    decision = payload["decision_update"]
    assert decision["decision_update_ready"] is True
    assert decision["top_candidate_after_sensitivity"]
    assert decision["top_candidate_decision_after_sensitivity"] in {
        "ACCEPT_FOR_SHADOW_RESEARCH",
        "OWNER_REVIEW_REQUIRED",
        "CONTINUE_RESEARCH",
        "REJECT_FOR_NOW",
    }
    assert decision["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["summary_findings"]["top_candidate_survives_realistic_cost"] in {
        "YES",
        "NO",
        "UNKNOWN",
    }

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "sensitivity_matrix_json",
        "decision_update_json",
        "markdown_path",
        "sensitivity_matrix_markdown",
        "decision_update_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_cost_turnover_cooldown_sensitivity_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_event_retest(
        tmp_path,
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        as_of=as_of,
    )
    output_root = tmp_path / "outputs" / "research_strategies" / "sensitivity_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-cost-turnover-cooldown-sensitivity",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--source-event-retest",
            str(source_paths["result"]),
            "--source-candidate-ranking",
            str(source_paths["ranking"]),
            "--source-cadence-matrix",
            str(source_paths["matrix"]),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert (output_root / "sensitivity_result.json").exists()
    assert (output_root / "sensitivity_matrix.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_dynamic_strategy_cost_turnover_cooldown_sensitivity_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_cost_turnover_cooldown_sensitivity"]

    assert entry["command"] == (
        "aits research strategies dynamic-strategy-cost-turnover-cooldown-sensitivity"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("sensitivity_result.json" in item for item in entry["artifact_globs"])
    assert any("decision_update.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_cost_turnover_cooldown_sensitivity" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-cost-turnover-cooldown-sensitivity" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2366_DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_ANALYSIS"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert READY_STATUS in Path(
        "docs/requirements/TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis.md"
    ).read_text(encoding="utf-8")


def _write_source_event_retest(
    tmp_path: Path,
    *,
    prices_path: Path,
    marketstack_path: Path,
    rates_path: Path,
    as_of: date,
) -> dict[str, Path]:
    cadence_path = _write_source_cadence_audit(
        tmp_path,
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        as_of=as_of,
    )
    output_root = tmp_path / "outputs" / "research_strategies" / "event_retest"
    docs_root = tmp_path / "docs" / "event_retest_docs"
    payload = run_dynamic_strategy_event_driven_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_cadence_audit_path=cadence_path,
        output_root=output_root,
        docs_root=docs_root,
        strategy_ids=["limited_adjustment"],
        as_of_date=as_of,
    )
    assert payload["status"] == (
        "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
    )
    return {
        "result": Path(payload["artifact_paths"]["json_path"]),
        "ranking": Path(payload["artifact_paths"]["candidate_ranking_json"]),
        "matrix": Path(payload["artifact_paths"]["cadence_comparison_matrix_json"]),
    }


def _write_source_cadence_audit(
    tmp_path: Path,
    *,
    prices_path: Path,
    marketstack_path: Path,
    rates_path: Path,
    as_of: date,
) -> Path:
    output_root = tmp_path / "outputs" / "research_trends" / "cadence_audit"
    docs_root = tmp_path / "docs" / "cadence_docs"
    source_payload = run_dynamic_strategy_execution_cadence_bias_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_root=docs_root,
        strategy_ids=["limited_adjustment"],
        as_of_date=as_of,
    )
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
