from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DATA_QUALITY_GATE_REASON,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    NEXT_ROUTE,
    READY_STATUS,
    REFERENCE_CANDIDATES,
    run_dynamic_strategy_expanded_candidate_pool_retest,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2379_READY = "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
SOURCE_2383_READY = "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY"
SOURCE_2384_READY = (
    "DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
SOURCE_2385_READY = (
    "DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_"
    "PLAN_READY"
)
SOURCE_2385_ROUTE = (
    "TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening"
)
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
LOWER_TURNOVER = "dynamic_regime_overlay_v0_4_lower_turnover"
COOLDOWN_BALANCED = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
GUARDED_TURNOVER = "equal_risk_growth_tilt_guarded_turnover_v1"
PRIMARY_CADENCE = "valid_until_window"
SIGNAL_FAMILIES = [
    "regime_transition_family",
    "trend_confirmation_family",
    "volatility_aware_family",
    "signal_age_valid_until_family",
    "turnover_budget_family",
    "risk_cap_interaction_family",
]
NEW_CANDIDATES = [
    ("dynamic_regime_reentry_accelerated_v1", "regime_transition_family"),
    ("dynamic_regime_recovery_confirmation_v1", "regime_transition_family"),
    ("dynamic_trend_confirmed_growth_tilt_v1", "trend_confirmation_family"),
    ("dynamic_trend_confirmed_low_turnover_v1", "trend_confirmation_family"),
    ("dynamic_volatility_scaled_growth_tilt_v1", "volatility_aware_family"),
    ("dynamic_volatility_floor_adjusted_v1", "volatility_aware_family"),
    ("dynamic_signal_age_decay_v1", "signal_age_valid_until_family"),
    ("dynamic_valid_until_expiry_strict_v1", "signal_age_valid_until_family"),
    ("dynamic_turnover_budgeted_growth_tilt_v1", "turnover_budget_family"),
    ("dynamic_turnover_budgeted_regime_overlay_v1", "turnover_budget_family"),
    ("dynamic_risk_cap_adaptive_v1", "risk_cap_interaction_family"),
    ("dynamic_risk_cap_trend_conditioned_v1", "risk_cap_interaction_family"),
]


def test_dynamic_strategy_expanded_candidate_pool_retest_builder(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "expanded_retest"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_expanded_candidate_pool_retest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_candidate_pool_expansion_plan_path=source_paths["candidate_pool_plan"],
        source_owner_review_path=source_paths["owner_review"],
        source_guarded_variant_retest_path=source_paths["guarded_variant_retest"],
        source_guarded_variant_ranking_path=source_paths["guarded_variant_ranking"],
        source_guarded_decision_update_path=source_paths["guarded_decision_update"],
        source_variant_retest_path=source_paths["variant_retest"],
        source_optimized_variant_ranking_path=source_paths["optimized_variant_ranking"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=as_of,
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_expanded_candidate_retest"] is True
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_passed"] is True
    assert payload["fresh_market_data_read"] is True
    assert payload["backtest_run"] is True
    assert payload["expanded_candidate_retest_run"] is True
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["primary_execution_cadence"] == PRIMARY_CADENCE
    assert payload["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert payload["reference_candidate_count"] == len(REFERENCE_CANDIDATES)
    assert payload["reference_candidates"] == list(REFERENCE_CANDIDATES)
    assert payload["new_candidates_tested_count"] == 12
    assert payload["total_candidates_tested_count"] == 17
    assert payload["signal_families_tested_count"] == 6
    assert set(payload["signal_families_tested"]) == set(SIGNAL_FAMILIES)
    assert payload["expanded_candidate_retest_ready"] is True
    assert payload["expanded_candidate_ranking_ready"] is True
    assert payload["signal_family_screening_ready"] is True
    assert payload["time_slice_matrix_ready"] is True
    assert payload["regime_slice_matrix_ready"] is True
    assert payload["cost_stress_result_ready"] is True
    assert payload["turnover_constraint_result_ready"] is True
    assert payload["decision_update_ready"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["best_candidate_after_expanded_screening"]
    assert payload["best_candidate_decision"] in {
        "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION",
        "OWNER_REVIEW_REQUIRED",
        "CONTINUE_OPTIMIZATION",
        "REJECT_FOR_NOW",
        "DEPRECATED_BY_EXPANDED_SCREENING",
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

    ranking = payload["expanded_candidate_ranking"]
    assert len(ranking) == 16
    assert "static_baseline" in payload["reference_candidates"]
    assert {row["candidate_id"] for row in ranking} >= set(
        payload["new_candidates_tested"]
    )
    assert all("comparison_vs_reference_candidates" in row for row in ranking)
    assert all("cost_stress_survival" in row for row in ranking)
    assert all(row["monthly_rebalance_allowed_for_primary_decision"] is False for row in ranking)
    assert all(row["valid_until_window_preserved"] is True for row in ranking)

    family_rows = payload["signal_family_screening"]
    assert set(SIGNAL_FAMILIES) <= {row["signal_family"] for row in family_rows}
    assert all("family_failure_reason" in row for row in family_rows)

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
        "expanded_candidate_ranking_json",
        "signal_family_screening_json",
        "time_regime_slice_matrix_json",
        "decision_update_json",
        "markdown_path",
        "ranking_markdown",
        "signal_family_screening_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_expanded_candidate_pool_retest_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "expanded_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-expanded-candidate-pool-retest",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--source-candidate-pool-plan",
            str(source_paths["candidate_pool_plan"]),
            "--source-owner-review",
            str(source_paths["owner_review"]),
            "--source-guarded-variant-retest",
            str(source_paths["guarded_variant_retest"]),
            "--source-guarded-variant-ranking",
            str(source_paths["guarded_variant_ranking"]),
            "--source-guarded-decision-update",
            str(source_paths["guarded_decision_update"]),
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
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
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "expanded_candidate_retest_result.json").exists()
    assert (output_root / "expanded_candidate_ranking.json").exists()
    assert (output_root / "signal_family_screening.json").exists()
    assert (output_root / "time_regime_slice_matrix.json").exists()
    assert (output_root / "decision_update.json").exists()


def test_dynamic_strategy_expanded_candidate_pool_retest_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_expanded_candidate_pool_retest"]

    assert entry["command"] == (
        "aits research strategies dynamic-strategy-expanded-candidate-pool-retest"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "expanded_candidate_retest_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("decision_update.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_expanded_candidate_pool_retest" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-expanded-candidate-pool-retest" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "candidate_pool_plan": _candidate_pool_plan(),
        "owner_review": _owner_review(),
        "guarded_variant_retest": _guarded_variant_retest(),
        "guarded_variant_ranking": _guarded_variant_ranking(),
        "guarded_decision_update": _guarded_decision_update(),
        "variant_retest": _variant_retest(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
        "candidate_ranking": _candidate_ranking(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": _sensitivity_decision_update(),
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


def _candidate_pool_plan() -> dict[str, object]:
    selected = [
        {"candidate_id": candidate_id, "family_id": family_id}
        for candidate_id, family_id in NEW_CANDIDATES
    ]
    return {
        "status": SOURCE_2385_READY,
        "recommended_next_research_task": SOURCE_2385_ROUTE,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "reference_candidates": list(REFERENCE_CANDIDATES),
        "signal_families": SIGNAL_FAMILIES,
        "new_candidates_for_2386": [item["candidate_id"] for item in selected],
        "candidate_pool_expansion_plan": {
            "new_candidates_selected_for_2386": selected,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _owner_review() -> dict[str, object]:
    return {
        "status": SOURCE_2384_READY,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _guarded_variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "data_quality_gate_executed": True,
        "data_quality_passed": True,
        "best_guarded_variant": GUARDED_TURNOVER,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _guarded_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "guarded_variant_ranking": [
            {"variant_id": GUARDED_TURNOVER, "rank": 1},
            {"variant_id": RANKING_TOP, "rank": 2},
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _guarded_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "decision_update": {"decision_update_ready": True},
        "production_effect": "none",
        "broker_action": "none",
    }


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "data_quality_gate_executed": True,
        "data_quality_passed": True,
        "ranking_top_reference": RANKING_TOP,
        "best_variant_after_retest": COOLDOWN_BALANCED,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimized_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "best_variant_after_retest": COOLDOWN_BALANCED,
        "optimized_variant_ranking": [
            {"variant_id": COOLDOWN_BALANCED, "rank": 1},
            {"variant_id": LOWER_TURNOVER, "rank": 2},
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "candidate_ranking": [
            {"candidate_id": RANKING_TOP, "rank": 1},
            {"candidate_id": LOWER_TURNOVER, "rank": 2},
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "sensitivity_matrix": [
            {"candidate_id": RANKING_TOP, "scenario_id": "base"},
            {"candidate_id": LOWER_TURNOVER, "scenario_id": "base"},
        ],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _sensitivity_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": {
            "decision_update_ready": True,
            "top_candidate_after_sensitivity": LOWER_TURNOVER,
        },
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
