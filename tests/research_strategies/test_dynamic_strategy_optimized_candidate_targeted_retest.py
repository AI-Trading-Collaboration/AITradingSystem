from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    NEXT_ROUTE,
    PRIMARY_CANDIDATE_ID,
    READY_STATUS,
    run_dynamic_strategy_optimized_candidate_targeted_retest,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2375_READY = (
    "DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY"
)
SOURCE_2375_ROUTE = "TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest"


def test_dynamic_strategy_optimized_candidate_targeted_retest_builder(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "targeted_retest"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_optimized_candidate_targeted_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_event_retest_path=source_paths["event_retest"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_cadence_matrix_path=source_paths["cadence_matrix"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_matrix_path=source_paths["sensitivity_matrix"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        source_optimization_review_path=source_paths["optimization_review"],
        source_optimization_matrix_path=source_paths["optimization_matrix"],
        source_optimization_decision_update_path=source_paths[
            "optimization_decision_update"
        ],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=as_of,
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_targeted_retest"] is True
    assert payload["data_quality_gate_executed"] is True
    assert payload["primary_candidate"] == PRIMARY_CANDIDATE_ID
    assert payload["decision_from_2375"] == "OWNER_REVIEW_REQUIRED"
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert payload["targeted_retest_ready"] is True
    assert payload["time_slice_retest_ready"] is True
    assert payload["regime_slice_retest_ready"] is True
    assert payload["cost_stress_retest_ready"] is True
    assert payload["execution_constraint_stress_ready"] is True
    assert payload["ablation_tests_ready"] is True
    assert payload["candidate_decision_update_ready"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["candidate_decision_after_targeted_retest"] in {
        "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION",
        "OWNER_REVIEW_REQUIRED",
        "CONTINUE_OPTIMIZATION",
        "REJECT_FOR_NOW",
        "DEPRECATED_BY_TARGETED_RETEST",
    }

    time_slice_ids = {row["scenario_id"] for row in payload["time_slice_retest_result"]}
    regime_slice_ids = {
        row["scenario_id"] for row in payload["regime_slice_retest_result"]
    }
    cost_ids = {row["scenario_id"] for row in payload["cost_stress_result"]}
    execution_axes = {
        row["constraint_axis"] for row in payload["execution_constraint_stress_result"]
    }
    ablation_ids = {row["ablation_id"] for row in payload["ablation_test_result"]}

    assert {
        "full_available_window",
        "early_period",
        "middle_period",
        "recent_period",
        "post_2023_ai_cycle",
        "high_volatility_periods",
        "drawdown_recovery_periods",
    } <= time_slice_ids
    assert {
        "risk_on",
        "risk_off",
        "high_volatility",
        "low_volatility",
        "trend_confirmed",
        "trend_uncertain",
        "drawdown",
        "recovery",
    } <= regime_slice_ids
    assert {"base", "realistic", "conservative", "harsh"} <= cost_ids
    assert {
        "cooldown_days",
        "min_holding_days",
        "max_turnover_per_month",
        "max_single_step_weight_delta",
    } <= execution_axes
    assert {
        "no_lower_turnover_guardrail",
        "no_valid_until_window",
        "no_cooldown",
        "no_risk_cap",
        "no_constraint_filter",
        "no_growth_tilt_or_risk_overlay",
    } <= ablation_ids

    primary_cost_rows = [
        row
        for row in payload["cost_stress_result"]
        if row["candidate_id"] == PRIMARY_CANDIDATE_ID
    ]
    assert primary_cost_rows
    assert all("performance_metrics" in row for row in primary_cost_rows)
    assert all("relative_metrics" in row for row in primary_cost_rows)
    assert all("execution_metrics" in row for row in primary_cost_rows)

    decision = payload["decision_update"]
    assert decision["decision_update_ready"] is True
    assert decision["primary_candidate"] == PRIMARY_CANDIDATE_ID
    assert decision["recommended_next_research_task"] == NEXT_ROUTE
    assert decision["monthly_rebalance_allowed_for_primary_decision"] is False

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

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "time_regime_slice_matrix_json",
        "ablation_test_report_json",
        "decision_update_json",
        "markdown_path",
        "slice_markdown",
        "ablation_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_optimized_candidate_targeted_retest_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "targeted_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-optimized-candidate-targeted-retest",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--source-event-retest",
            str(source_paths["event_retest"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-cadence-matrix",
            str(source_paths["cadence_matrix"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-matrix",
            str(source_paths["sensitivity_matrix"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--source-optimization-review",
            str(source_paths["optimization_review"]),
            "--source-optimization-matrix",
            str(source_paths["optimization_matrix"]),
            "--source-optimization-decision-update",
            str(source_paths["optimization_decision_update"]),
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
    assert (output_root / "targeted_retest_result.json").exists()
    assert (output_root / "time_regime_slice_matrix.json").exists()
    assert (output_root / "ablation_test_report.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_dynamic_strategy_optimized_candidate_targeted_retest_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_optimized_candidate_targeted_retest"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-optimized-candidate-targeted-retest"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("targeted_retest_result.json" in item for item in entry["artifact_globs"])
    assert any("decision_update.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_optimized_candidate_targeted_retest" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-optimized-candidate-targeted-retest" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2376_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")
    assert READY_STATUS in Path(
        "docs/requirements/TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "event_retest": _event_retest(),
        "candidate_ranking": _candidate_ranking_document(),
        "cadence_matrix": _cadence_matrix_document(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_matrix": _sensitivity_matrix_document(),
        "sensitivity_decision_update": _sensitivity_decision_update_document(),
        "optimization_review": _optimization_review(),
        "optimization_matrix": _optimization_matrix_document(),
        "optimization_decision_update": _optimization_decision_update_document(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = source_root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _candidate_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": RANKING_TOP,
            "rank": 1,
            "decision": "OWNER_REVIEW_REQUIRED",
            "cost_adjusted_return": 0.214462,
            "dynamic_vs_static_gap": 0.021905,
            "turnover": 1.964574,
            "rebalance_count": 65,
            "average_holding_days": 13.938,
            "max_drawdown": -0.183495,
            "primary_execution_cadence": "valid_until_window",
        },
        {
            "candidate_id": PRIMARY_CANDIDATE_ID,
            "rank": 2,
            "decision": "CONTINUE_RESEARCH",
            "cost_adjusted_return": 0.195375,
            "dynamic_vs_static_gap": 0.002818,
            "turnover": 2.04,
            "rebalance_count": 19,
            "average_holding_days": 47.222,
            "max_drawdown": -0.122632,
            "primary_execution_cadence": "valid_until_window",
        },
    ]


def _robustness_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": PRIMARY_CANDIDATE_ID,
            "robust_rank": 1,
            "source_rank_2365": 2,
            "robustness_score": 11,
            "realistic_dynamic_vs_static_gap": 0.002205,
            "conservative_dynamic_vs_static_gap": 0.001524,
            "harsh_dynamic_vs_static_gap": 0.000843,
            "survives_realistic_cost": True,
            "survives_conservative_cost": True,
            "survives_harsh_cost": True,
            "decision_update": "OWNER_REVIEW_REQUIRED",
        },
        {
            "candidate_id": RANKING_TOP,
            "robust_rank": 2,
            "source_rank_2365": 1,
            "robustness_score": 10,
            "realistic_dynamic_vs_static_gap": 0.021302,
            "conservative_dynamic_vs_static_gap": 0.020633,
            "harsh_dynamic_vs_static_gap": 0.019964,
            "survives_realistic_cost": True,
            "survives_conservative_cost": True,
            "survives_harsh_cost": True,
            "decision_update": "OWNER_REVIEW_REQUIRED",
        },
    ]


def _event_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
        "cadence_comparison_matrix": [{"scenario_id": "valid_until_window"}],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _candidate_ranking_document() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
    }


def _cadence_matrix_document() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "cadence_comparison_matrix": [{"scenario_id": "valid_until_window"}],
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "robustness_ranking": _robustness_rows(),
        "decision_update": _sensitivity_decision_update_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sensitivity_matrix_document() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "sensitivity_matrix": [{"candidate_id": PRIMARY_CANDIDATE_ID}],
    }


def _sensitivity_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": _sensitivity_decision_update_payload(),
    }


def _sensitivity_decision_update_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "top_candidate_after_sensitivity": PRIMARY_CANDIDATE_ID,
        "top_candidate_decision_after_sensitivity": "OWNER_REVIEW_REQUIRED",
        "robustness_ranking": _robustness_rows(),
    }


def _optimization_review() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "primary_execution_cadence": "valid_until_window",
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "best_candidate_after_optimization": PRIMARY_CANDIDATE_ID,
        "recommended_decision_after_optimization": "OWNER_REVIEW_REQUIRED",
        "recommended_next_research_task": SOURCE_2375_ROUTE,
        "candidate_decision_update": _optimization_decision_payload(),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimization_matrix_document() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "optimization_matrix": [
            {
                "candidate_id": PRIMARY_CANDIDATE_ID,
                "scenario_id": "realistic",
                "execution_cadence": "valid_until_window",
            }
        ],
    }


def _optimization_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "candidate_decision_update": _optimization_decision_payload(),
    }


def _optimization_decision_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "best_candidate_after_optimization": PRIMARY_CANDIDATE_ID,
        "recommended_decision_after_optimization": "OWNER_REVIEW_REQUIRED",
        "recommended_next_research_task": SOURCE_2375_ROUTE,
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
