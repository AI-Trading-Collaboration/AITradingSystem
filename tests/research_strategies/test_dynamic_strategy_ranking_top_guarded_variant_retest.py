from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    COOLDOWN_BALANCED_REFERENCE,
    GUARDED_VARIANT_IDS,
    LOWER_TURNOVER_REFERENCE,
    NEXT_ROUTE,
    RANKING_TOP_CANDIDATE,
    READY_STATUS,
    VARIANTS_TESTED,
    run_dynamic_strategy_ranking_top_guarded_variant_retest,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2379_READY = "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
SOURCE_2382_READY = (
    "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY"
)
SOURCE_2382_ROUTE = "TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest"


def test_dynamic_strategy_ranking_top_guarded_variant_retest_builder(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "guarded_retest"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_ranking_top_guarded_variant_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_retest_plan_path=source_paths["retest_plan"],
        source_guarded_variant_plan_path=source_paths["guarded_variant_plan"],
        source_variant_evaluation_plan_path=source_paths["variant_evaluation_plan"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        source_variant_retest_path=source_paths["variant_retest"],
        source_optimized_variant_ranking_path=source_paths[
            "optimized_variant_ranking"
        ],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=as_of,
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_guarded_variant_retest"] is True
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_passed"] is True
    assert payload["ranking_top_candidate"] == RANKING_TOP_CANDIDATE
    assert payload["guardrail_reference_candidates"] == [
        LOWER_TURNOVER_REFERENCE,
        COOLDOWN_BALANCED_REFERENCE,
    ]
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert payload["variants_tested"] == list(VARIANTS_TESTED)
    assert set(GUARDED_VARIANT_IDS).issubset(payload["planned_variants_from_2382"])
    assert payload["guarded_variant_retest_ready"] is True
    assert payload["guarded_variant_ranking_ready"] is True
    assert payload["time_slice_matrix_ready"] is True
    assert payload["regime_slice_matrix_ready"] is True
    assert payload["cost_stress_result_ready"] is True
    assert payload["turnover_constraint_result_ready"] is True
    assert payload["decision_update_ready"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["best_guarded_variant"] in GUARDED_VARIANT_IDS
    assert payload["best_guarded_variant_decision"] in {
        "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION",
        "OWNER_REVIEW_REQUIRED",
        "CONTINUE_OPTIMIZATION",
        "REJECT_FOR_NOW",
        "DEPRECATED_BY_GUARDED_RETEST",
    }

    time_ids = {row["scenario_id"] for row in payload["time_slice_matrix"]}
    regime_ids = {row["scenario_id"] for row in payload["regime_slice_matrix"]}
    cost_ids = {row["scenario_id"] for row in payload["cost_stress_result"]}
    constraint_axes = {
        row["constraint_axis"] for row in payload["turnover_constraint_result"]
    }
    assert {
        "full_available_window",
        "recent_period",
        "post_2023_ai_cycle",
        "high_volatility_periods",
        "drawdown_recovery_periods",
    } <= time_ids
    assert {
        "risk_on",
        "risk_off",
        "high_volatility",
        "low_volatility",
        "trend_confirmed",
        "recovery",
    } <= regime_ids
    assert {"base", "realistic", "conservative", "harsh"} <= cost_ids
    assert {
        "cooldown_days",
        "min_holding_days",
        "max_turnover_per_month",
        "max_single_step_weight_delta",
    } <= constraint_axes

    ranking = payload["guarded_variant_ranking"]
    assert {row["variant_id"] for row in ranking} == set(VARIANTS_TESTED)
    assert all("guarded_variant_vs_original_ranking_top_gap" in row for row in ranking)
    assert all("turnover_reduction_vs_original_ranking_top" in row for row in ranking)
    assert all("cost_stress_survival" in row for row in ranking)
    assert all(
        row["monthly_rebalance_allowed_for_primary_decision"] is False
        for row in ranking
    )

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "scheduled_task_created",
        "event_append_enabled",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "production_enabled",
        "broker_action_enabled",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "guarded_variant_ranking_json",
        "time_regime_slice_matrix_json",
        "decision_update_json",
        "markdown_path",
        "ranking_markdown",
        "slice_matrix_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_ranking_top_guarded_variant_retest_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "guarded_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-ranking-top-guarded-variant-retest",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--source-retest-plan",
            str(source_paths["retest_plan"]),
            "--source-guarded-variant-plan",
            str(source_paths["guarded_variant_plan"]),
            "--source-variant-evaluation-plan",
            str(source_paths["variant_evaluation_plan"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
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
    assert "broker_action=none" in result.output
    assert (output_root / "guarded_variant_retest_result.json").exists()
    assert (output_root / "guarded_variant_ranking.json").exists()
    assert (output_root / "time_regime_slice_matrix.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_dynamic_strategy_ranking_top_guarded_variant_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_ranking_top_guarded_variant_retest"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-ranking-top-guarded-variant-retest"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "guarded_variant_retest_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("decision_update.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_ranking_top_guarded_variant_retest" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-ranking-top-guarded-variant-retest" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2383_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST" in task_text


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "retest_plan": _retest_plan(),
        "guarded_variant_plan": _guarded_variant_plan(),
        "variant_evaluation_plan": _variant_evaluation_plan(),
        "candidate_ranking": _candidate_ranking(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": _sensitivity_decision_update(),
        "variant_retest": _variant_retest(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _retest_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2382_READY,
        "recommended_next_research_task": SOURCE_2382_ROUTE,
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "guardrail_reference_candidates": [
            LOWER_TURNOVER_REFERENCE,
            COOLDOWN_BALANCED_REFERENCE,
        ],
        "primary_execution_cadence": "valid_until_window",
        "planned_variants": list(GUARDED_VARIANT_IDS),
        "all_variant_ids_for_2383": list(VARIANTS_TESTED),
        "production_effect": "none",
        "broker_action": "none",
    }


def _guarded_variant_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2382_READY,
        "planned_variants": list(GUARDED_VARIANT_IDS),
        "variants": [
            {"candidate_id": item, "include_in_2383_retest": True}
            for item in GUARDED_VARIANT_IDS
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _variant_evaluation_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2382_READY,
        "variant_evaluation_plan_ready": True,
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "candidate_ranking": [
            {"candidate_id": RANKING_TOP_CANDIDATE, "rank": 1},
            {"candidate_id": LOWER_TURNOVER_REFERENCE, "rank": 2},
        ],
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "robustness_ranking": [
            {"candidate_id": LOWER_TURNOVER_REFERENCE, "robust_rank": 1},
            {"candidate_id": RANKING_TOP_CANDIDATE, "robust_rank": 2},
        ],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sensitivity_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": {
            "decision_update_ready": True,
            "top_candidate_after_sensitivity": LOWER_TURNOVER_REFERENCE,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "primary_execution_cadence": "valid_until_window",
        "ranking_top_reference": RANKING_TOP_CANDIDATE,
        "best_variant_after_retest": COOLDOWN_BALANCED_REFERENCE,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimized_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "best_variant_after_retest": COOLDOWN_BALANCED_REFERENCE,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "production_effect": "none",
        "broker_action": "none",
    }


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
