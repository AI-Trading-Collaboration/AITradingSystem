from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "simple_baseline_strategy_registry.yaml"
)
DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "simple_baselines"
)
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_SIMPLE_BASELINE_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "simple_baseline_portfolio_control_master_review.md"
)
DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)

SAFETY_BOUNDARY: dict[str, Any] = {
    "production_effect": "none",
    "broker_action": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "manual_review_required": True,
    "research_only": True,
    "observe_only": True,
}

REQUIRED_STRATEGY_FIELDS = (
    "strategy_id",
    "asset_universe",
    "target_weights",
    "rebalance_frequency",
    "risk_control_rule",
    "trend_filter_rule",
    "volatility_filter_rule",
    "drawdown_filter_rule",
    "max_turnover",
    "max_tqqq_weight",
    "uses_leverage_etf",
    "uses_options",
    "production_effect",
    "broker_action",
)

QQQ_SGOV_STRATEGY_IDS = (
    "qqq_100_static",
    "qqq_80_sgov_20",
    "qqq_70_sgov_30",
    "qqq_60_sgov_40",
    "qqq_50_sgov_50",
    "qqq_200dma_risk_off",
    "qqq_100_200dma_trend_filter",
    "qqq_volatility_target",
    "equal_risk_qqq_sgov",
)

TQQQ_SGOV_STRATEGY_IDS = (
    "tqqq_25_sgov_75",
    "tqqq_33_sgov_67",
    "tqqq_40_sgov_60",
    "tqqq_50_sgov_50",
    "tqqq_sgov_quarterly",
    "tqqq_volatility_capped",
    "tqqq_drawdown_capped",
    "qqq_200dma_risk_off",
)


def run_simple_baseline_registry_review(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    strategy_rows = _strategy_rows(config)
    issues = _registry_issues(config, strategy_rows)
    status = "BASELINE_REGISTRY_READY"
    if issues:
        status = "BASELINE_REGISTRY_PARTIAL"
    if not strategy_rows:
        status = "BASELINE_REGISTRY_BLOCKED"

    payload = _payload(
        report_type="simple_baseline_strategy_registry_review",
        title="Simple Baseline Strategy Registry Review",
        status=status,
        summary={
            "strategy_count": len(strategy_rows),
            "issue_count": len(issues),
            "uses_options_count": sum(1 for row in strategy_rows if row.get("uses_options")),
            "leveraged_strategy_count": sum(
                1 for row in strategy_rows if row.get("uses_leverage_etf")
            ),
            "required_safety_fields_present": not issues,
            "config_path": str(config_path),
        },
        registry_policy=_registry_policy(config),
        strategies=strategy_rows,
        issues=issues,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_strategy_registry_review",
            "Simple Baseline Strategy Registry Review",
            "aits research strategies simple-baseline-registry-review",
            "simple_baseline_strategy_registry_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_qqq_sgov_baseline_backtest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=["QQQ", "SGOV"],
    )
    if not data_gate["passed"]:
        payload = _blocked_data_payload(
            report_type="qqq_sgov_baseline_backtest",
            title="QQQ / SGOV Baseline Backtest",
            status="QQQ_SGOV_BASELINE_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    strategy_rows = [
        row
        for row in _strategy_rows(config)
        if str(row.get("strategy_id")) in QQQ_SGOV_STRATEGY_IDS
    ]
    results = _run_backtest_set(
        strategy_rows=strategy_rows,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
        required_tickers=["QQQ", "SGOV"],
    )
    status = "QQQ_SGOV_BASELINE_READY" if results else "QQQ_SGOV_BASELINE_INCONCLUSIVE"
    payload = _payload(
        report_type="qqq_sgov_baseline_backtest",
        title="QQQ / SGOV Baseline Backtest",
        status=status,
        summary=_backtest_summary(results, data_gate),
        data_quality=data_gate,
        strategy_results=results,
        requested_date_range=_requested_range(results, start_date, end_date),
        report_registry_entry=_report_registry_entry(
            "qqq_sgov_baseline_backtest",
            "QQQ / SGOV Baseline Backtest",
            "aits research strategies qqq-sgov-baseline-backtest",
            "qqq_sgov_baseline_backtest",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_tqqq_sgov_risk_controlled_baseline(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_data_payload(
            report_type="tqqq_sgov_risk_controlled_baseline",
            title="TQQQ / SGOV Risk-Controlled Baseline",
            status="TQQQ_BASELINE_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    strategy_rows = [
        row
        for row in _strategy_rows(config)
        if str(row.get("strategy_id")) in TQQQ_SGOV_STRATEGY_IDS
    ]
    results = _run_backtest_set(
        strategy_rows=strategy_rows,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    enriched = [_with_tqqq_risk_fields(row) for row in results]
    status = _tqqq_status(enriched)
    payload = _payload(
        report_type="tqqq_sgov_risk_controlled_baseline",
        title="TQQQ / SGOV Risk-Controlled Baseline",
        status=status,
        summary={
            **_backtest_summary(enriched, data_gate),
            "leverage_decay_risk_note": (
                "TQQQ path returns are path dependent and may decay during volatile sideways "
                "markets."
            ),
            "path_dependency_note": (
                "Metrics are research-only and must be forward aged before any paper-shadow "
                "review."
            ),
        },
        data_quality=data_gate,
        strategy_results=enriched,
        rebalance_frequency_sensitivity=_rebalance_frequency_sensitivity(enriched),
        requested_date_range=_requested_range(enriched, start_date, end_date),
        report_registry_entry=_report_registry_entry(
            "tqqq_sgov_risk_controlled_baseline",
            "TQQQ / SGOV Risk-Controlled Baseline",
            "aits research strategies tqqq-sgov-risk-controlled-baseline",
            "tqqq_sgov_risk_controlled_baseline",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_trend_vol_allocation_policy_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_data_payload(
            report_type="trend_vol_allocation_policy_search",
            title="Trend / Volatility Allocation Policy Search",
            status="POLICY_SEARCH_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    candidate_rows = _dynamic_candidate_strategies(config)
    results = _run_backtest_set(
        strategy_rows=candidate_rows,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    allowed_results = [
        row
        for row in results
        if row["max_tqqq_weight_observed"]
        <= _research_float(config, "dynamic_policy_search", "max_tqqq_weight")
        and row["max_single_rebalance_turnover"]
        <= _research_float(config, "dynamic_policy_search", "max_single_rebalance_turnover")
    ]
    ranked = sorted(allowed_results, key=lambda row: _float(row.get("calmar")), reverse=True)
    status = "POLICY_SEARCH_COMPLETED" if ranked else "POLICY_SEARCH_INCONCLUSIVE"
    payload = _payload(
        report_type="trend_vol_allocation_policy_search",
        title="Trend / Volatility Allocation Policy Search",
        status=status,
        summary={
            **_backtest_summary(ranked, data_gate),
            "searched_candidate_count": len(results),
            "constraint_passing_candidate_count": len(ranked),
            "top_candidate": ranked[0]["strategy_id"] if ranked else None,
        },
        data_quality=data_gate,
        allowed_inputs=[
            "QQQ close",
            "QQQ moving average distance",
            "QQQ 20d / 60d / 120d realized volatility",
            "QQQ drawdown from rolling high",
            "QQQ 100DMA / 200DMA state",
            "TQQQ close",
            "SGOV total return proxy",
        ],
        forbidden_inputs=[
            "future return",
            "future drawdown",
            "future realized volatility",
            "tail-risk label",
            "fallback_triggered",
            "large_loss_case",
            "tail_loss_case",
            "long_horizon_failure_case",
        ],
        strategy_results=ranked,
        requested_date_range=_requested_range(ranked, start_date, end_date),
        report_registry_entry=_report_registry_entry(
            "trend_vol_allocation_policy_search",
            "Trend / Volatility Allocation Policy Search",
            "aits research strategies trend-vol-allocation-policy-search",
            "trend_vol_allocation_policy_search",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_dominance_ranking(
    *,
    qqq_sgov_path: Path | None = None,
    tqqq_sgov_path: Path | None = None,
    policy_search_path: Path | None = None,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    qqq_sgov_path = qqq_sgov_path or output_root / "qqq_sgov_baseline_backtest.json"
    tqqq_sgov_path = tqqq_sgov_path or output_root / "tqqq_sgov_risk_controlled_baseline.json"
    policy_search_path = (
        policy_search_path or output_root / "trend_vol_allocation_policy_search.json"
    )
    inputs = [
        _read_json_or_empty(path) for path in (qqq_sgov_path, tqqq_sgov_path, policy_search_path)
    ]
    missing = [
        str(path)
        for path, payload in zip(
            (qqq_sgov_path, tqqq_sgov_path, policy_search_path), inputs, strict=True
        )
        if not payload
    ]
    rows = []
    for payload in inputs:
        rows.extend(_records(payload.get("strategy_results")))
    if missing or not rows:
        payload = _payload(
            report_type="simple_baseline_dominance_ranking",
            title="Simple Baseline Dominance Ranking",
            status="BASELINE_RANKING_BLOCKED",
            summary={"strategy_count": len(rows), "missing_input_count": len(missing)},
            blockers=missing,
            input_artifacts={
                "qqq_sgov": str(qqq_sgov_path),
                "tqqq_sgov": str(tqqq_sgov_path),
                "policy_search": str(policy_search_path),
            },
            report_registry_entry=_report_registry_entry(
                "simple_baseline_dominance_ranking",
                "Simple Baseline Dominance Ranking",
                "aits research strategies simple-baseline-dominance-ranking",
                "simple_baseline_dominance_ranking",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    scored = [_ranking_row(row) for row in _dedupe_by_strategy(rows)]
    pareto = _pareto_frontier(scored)
    dominated = [
        row for row in scored if row["strategy_id"] not in {item["strategy_id"] for item in pareto}
    ]
    payload = _payload(
        report_type="simple_baseline_dominance_ranking",
        title="Simple Baseline Dominance Ranking",
        status="BASELINE_RANKING_READY" if pareto else "BASELINE_RANKING_INCONCLUSIVE",
        summary={
            "strategy_count": len(scored),
            "pareto_frontier_count": len(pareto),
            "dominated_strategy_count": len(dominated),
            "top_recommended_candidate": pareto[0]["strategy_id"] if pareto else None,
        },
        pareto_frontier=pareto,
        dominated_strategy_list=dominated,
        non_dominated_strategy_list=pareto,
        top_10_by_return=_top(scored, "annual_return"),
        top_10_by_calmar=_top(scored, "calmar"),
        top_10_by_sharpe=_top(scored, "sharpe"),
        top_10_by_low_drawdown=sorted(scored, key=lambda row: abs(_float(row["max_drawdown"])))[
            :10
        ],
        recommended_research_candidates=pareto[:10],
        input_artifacts={
            "qqq_sgov": str(qqq_sgov_path),
            "tqqq_sgov": str(tqqq_sgov_path),
            "policy_search": str(policy_search_path),
        },
        report_registry_entry=_report_registry_entry(
            "simple_baseline_dominance_ranking",
            "Simple Baseline Dominance Ranking",
            "aits research strategies simple-baseline-dominance-ranking",
            "simple_baseline_dominance_ranking",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_pit_boundary_audit(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    issues = []
    for row in _strategy_rows(config):
        strategy_id = str(row.get("strategy_id"))
        forbidden = [
            item
            for item in (
                row.get("trend_filter_rule"),
                row.get("volatility_filter_rule"),
                row.get("drawdown_filter_rule"),
                row.get("risk_control_rule"),
            )
            if _contains_forbidden_pit_input(str(item))
        ]
        if forbidden:
            issues.append({"strategy_id": strategy_id, "forbidden_inputs": forbidden})
    checks = {
        "price_data_availability": "checked_by_data_quality_gate_before_data_dependent_commands",
        "sgov_return_availability": "checked_by_required_price_tickers",
        "moving_average_calculation_boundary": "uses_prior_close_shifted_signal",
        "realized_volatility_calculation_boundary": "uses_rolling_returns_shifted_one_day",
        "drawdown_calculation_boundary": "uses_rolling_high_shifted_one_day",
        "rebalance_date_alignment": "execution_uses_prior_signal_weights",
        "signal_date_vs_execution_date": "t_signal_applies_to_t_plus_1_return",
        "holiday_missing_date_handling": "uses_observed_trading_dates_only",
    }
    status = "PIT_BOUNDARY_PASS" if not issues else "PIT_BOUNDARY_BLOCKED"
    payload = _payload(
        report_type="simple_baseline_pit_boundary_audit",
        title="Simple Baseline PIT Boundary Audit",
        status=status,
        summary={
            "strategy_count": len(_strategy_rows(config)),
            "pit_issue_count": len(issues),
            "signal_execution_lag": "one_observed_trading_day",
        },
        boundary_checks=checks,
        forbidden_input_issues=issues,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_pit_boundary_audit",
            "Simple Baseline PIT Boundary Audit",
            "aits research strategies simple-baseline-pit-boundary-audit",
            "simple_baseline_pit_boundary_audit",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_cost_sensitivity(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_data_payload(
            report_type="simple_baseline_cost_sensitivity",
            title="Simple Baseline Cost Sensitivity",
            status="COST_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    strategy_rows = _strategy_rows(config)
    scenarios = _mapping(_research_policy(config).get("cost_scenarios"))
    results = []
    for scenario_id, scenario in scenarios.items():
        cost_bps = _float(_mapping(scenario).get("cost_bps"))
        backtests = _run_backtest_set(
            strategy_rows=strategy_rows,
            prices_path=prices_path,
            config=config,
            start_date=start_date,
            end_date=end_date,
            cost_bps=cost_bps,
        )
        for row in backtests:
            results.append(
                {
                    "strategy_id": row["strategy_id"],
                    "scenario": scenario_id,
                    "return_after_cost": row["annual_return"],
                    "turnover": row["turnover"],
                    "cost_drag": row["cost_drag"],
                    "rebalance_count": row["rebalance_count"],
                    "performance_degradation": row["performance_degradation"],
                    "cost_sensitivity_score": row["cost_sensitivity_score"],
                }
            )
    max_score = max((_float(row["cost_sensitivity_score"]) for row in results), default=0.0)
    status = "COST_ROBUST"
    if max_score > _research_float(config, "readiness_rules", "max_cost_sensitivity_score"):
        status = "COST_SENSITIVE"
    payload = _payload(
        report_type="simple_baseline_cost_sensitivity",
        title="Simple Baseline Cost Sensitivity",
        status=status,
        summary={
            "strategy_count": len(strategy_rows),
            "scenario_count": len(scenarios),
            "max_cost_sensitivity_score": round(max_score, 6),
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        sensitivity_results=results,
        requested_date_range=_date_range_from_price_path(prices_path, start_date, end_date),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_cost_sensitivity",
            "Simple Baseline Cost Sensitivity",
            "aits research strategies simple-baseline-cost-sensitivity",
            "simple_baseline_cost_sensitivity",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_regime_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_data_payload(
            report_type="simple_baseline_regime_review",
            title="Simple Baseline Regime Review",
            status="REGIME_INSUFFICIENT",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _load_price_matrix(prices_path, _required_tickers(config))
    prices = _slice_prices(prices, start_date=start_date, end_date=end_date)
    strategy_rows = _strategy_rows(config)
    backtests = _run_backtest_set(
        strategy_rows=strategy_rows,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    annualization = _research_policy_int(config, "annualization_trading_days")
    regime_labels = _regime_labels(prices, config)
    regime_rows = []
    for strategy in strategy_rows:
        returns = _strategy_return_series(strategy, prices, config)
        regime_rows.extend(
            _strategy_regime_rows(
                str(strategy["strategy_id"]),
                returns,
                regime_labels,
                annualization=annualization,
            )
        )
    concentration = _regime_concentration_score(regime_rows)
    status = "REGIME_ROBUST"
    if not regime_rows:
        status = "REGIME_INSUFFICIENT"
    elif concentration > _research_float(
        config, "readiness_rules", "max_regime_concentration_score"
    ):
        status = "REGIME_CONCENTRATED"
    elif any(_float(row.get("annual_return")) < 0 for row in regime_rows):
        status = "REGIME_MIXED"
    payload = _payload(
        report_type="simple_baseline_regime_review",
        title="Simple Baseline Regime Review",
        status=status,
        summary={
            "strategy_count": len(strategy_rows),
            "regime_row_count": len(regime_rows),
            "regime_concentration_score": round(concentration, 6),
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        return_by_regime=regime_rows,
        drawdown_by_regime=[
            {
                "strategy_id": row["strategy_id"],
                "regime": row["regime"],
                "max_drawdown": row["max_drawdown"],
            }
            for row in regime_rows
        ],
        sharpe_by_regime=[
            {"strategy_id": row["strategy_id"], "regime": row["regime"], "sharpe": row["sharpe"]}
            for row in regime_rows
        ],
        sample_count_by_regime=[
            {
                "strategy_id": row["strategy_id"],
                "regime": row["regime"],
                "sample_count": row["sample_count"],
            }
            for row in regime_rows
        ],
        regime_concentration_score=round(concentration, 6),
        weak_regime_list=[row for row in regime_rows if _float(row["annual_return"]) < 0],
        strong_regime_list=sorted(
            regime_rows, key=lambda row: _float(row["annual_return"]), reverse=True
        )[:10],
        strategy_results=backtests,
        requested_date_range=_requested_range(backtests, start_date, end_date),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_regime_review",
            "Simple Baseline Regime Review",
            "aits research strategies simple-baseline-regime-review",
            "simple_baseline_regime_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_tracker(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_data_payload(
            report_type="simple_baseline_forward_aging_tracker",
            title="Simple Baseline Forward Aging Tracker",
            status="FORWARD_AGING_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _slice_prices(
        _load_price_matrix(prices_path, _required_tickers(config)),
        start_date=start_date,
        end_date=end_date,
    )
    rows = []
    for strategy in _strategy_rows(config):
        returns = _strategy_return_series(strategy, prices, config)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        rows.extend(_forward_aging_rows(str(strategy["strategy_id"]), returns, weights))
    mature_20d_count = sum(row.get("matured_20d_return") is not None for row in rows)
    status = "FORWARD_AGING_READY"
    if not rows:
        status = "FORWARD_AGING_INSUFFICIENT"
    elif mature_20d_count < _research_float(
        config, "readiness_rules", "min_forward_mature_20d_samples"
    ):
        status = "FORWARD_AGING_PENDING"
    payload = _payload(
        report_type="simple_baseline_forward_aging_tracker",
        title="Simple Baseline Forward Aging Tracker",
        status=status,
        summary={
            "tracking_row_count": len(rows),
            "matured_20d_count": mature_20d_count,
            "matured_60d_count": sum(row.get("matured_60d_return") is not None for row in rows),
            "pending_outcome_count": sum(bool(row["pending_outcomes"]) for row in rows),
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        forward_aging_records=rows,
        requested_date_range=_date_range_from_price_frame(prices),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_tracker",
            "Simple Baseline Forward Aging Tracker",
            "aits research strategies simple-baseline-forward-aging-tracker",
            "simple_baseline_forward_aging_tracker",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_paper_shadow_readiness(
    *,
    pit_audit_path: Path | None = None,
    ranking_path: Path | None = None,
    regime_review_path: Path | None = None,
    cost_sensitivity_path: Path | None = None,
    forward_aging_path: Path | None = None,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    pit_audit_path = pit_audit_path or output_root / "simple_baseline_pit_boundary_audit.json"
    ranking_path = ranking_path or output_root / "simple_baseline_dominance_ranking.json"
    regime_review_path = regime_review_path or output_root / "simple_baseline_regime_review.json"
    cost_sensitivity_path = (
        cost_sensitivity_path or output_root / "simple_baseline_cost_sensitivity.json"
    )
    forward_aging_path = (
        forward_aging_path or output_root / "simple_baseline_forward_aging_tracker.json"
    )
    sources = {
        "pit": _read_json_or_empty(pit_audit_path),
        "ranking": _read_json_or_empty(ranking_path),
        "regime": _read_json_or_empty(regime_review_path),
        "cost": _read_json_or_empty(cost_sensitivity_path),
        "forward": _read_json_or_empty(forward_aging_path),
    }
    missing = [key for key, payload in sources.items() if not payload]
    candidates = _records(sources["ranking"].get("recommended_research_candidates"))
    candidate = candidates[0] if candidates else {}
    blockers = _readiness_blockers(sources, config, missing)
    status = "PAPER_SHADOW_REVIEWABLE_LATER" if candidate and not blockers else "RESEARCH_ONLY"
    if any("PIT" in blocker for blocker in blockers):
        status = "BLOCKED_BY_PIT_RISK"
    elif any("forward" in blocker.lower() for blocker in blockers):
        status = "BLOCKED_BY_INSUFFICIENT_FORWARD"
    elif any("dominance" in blocker.lower() for blocker in blockers):
        status = "BLOCKED_BY_BASELINE_DOMINANCE"
    payload = _payload(
        report_type="simple_baseline_paper_shadow_readiness",
        title="Simple Baseline Paper-Shadow Readiness",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "blocking_reason_count": len(blockers),
            "required_forward_days_before_review": 60,
        },
        candidate_strategy_id=candidate.get("strategy_id"),
        paper_shadow_readiness_status=status,
        blocking_reasons=blockers,
        required_forward_days_before_review=60,
        input_artifacts={
            "pit": str(pit_audit_path),
            "ranking": str(ranking_path),
            "regime": str(regime_review_path),
            "cost": str(cost_sensitivity_path),
            "forward": str(forward_aging_path),
        },
        report_registry_entry=_report_registry_entry(
            "simple_baseline_paper_shadow_readiness",
            "Simple Baseline Paper-Shadow Readiness",
            "aits research strategies simple-baseline-paper-shadow-readiness",
            "simple_baseline_paper_shadow_readiness",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_daily_reader_safety_summary(
    *,
    ranking_path: Path | None = None,
    readiness_path: Path | None = None,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    ranking_path = ranking_path or output_root / "simple_baseline_dominance_ranking.json"
    readiness_path = readiness_path or output_root / "simple_baseline_paper_shadow_readiness.json"
    ranking = _read_json_or_empty(ranking_path)
    readiness = _read_json_or_empty(readiness_path)
    candidate = _first(_records(ranking.get("recommended_research_candidates")))
    blockers = [
        {"reason": str(item)}
        for item in readiness.get("blocking_reasons", [])
        if isinstance(item, (str, int, float))
    ]
    status = "DAILY_SUMMARY_SAFE"
    if not ranking or not readiness:
        status = "DAILY_SUMMARY_BLOCKED"
    payload = _payload(
        report_type="daily_reader_portfolio_control_safety_summary",
        title="Daily Reader Portfolio Control Safety Summary",
        status=status,
        summary={
            "top_simple_baseline_candidate": candidate.get("strategy_id"),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "major_blocker_count": len(blockers),
        },
        portfolio_control_research_status={
            "top_simple_baseline_candidate": candidate.get("strategy_id"),
            "current_research_only_target_weights": candidate.get("average_weights", {}),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "major_blockers": blockers,
            "reader_language_boundary": (
                "research-only / observe-only; no orders or live allocation instructions"
            ),
        },
        input_artifacts={"ranking": str(ranking_path), "readiness": str(readiness_path)},
        report_registry_entry=_report_registry_entry(
            "daily_reader_portfolio_control_safety_summary",
            "Daily Reader Portfolio Control Safety Summary",
            "aits research strategies daily-reader-portfolio-control-safety-summary",
            "daily_reader_portfolio_control_safety_summary",
            required_for_daily_reading=False,
            include_in_reader_brief=True,
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_portfolio_dry_run_mapper(
    *,
    strategy_id: str = "qqq_80_sgov_20",
    hypothetical_portfolio_value: float | None = None,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    strategy = _strategy_by_id(config, strategy_id)
    if not strategy:
        payload = _payload(
            report_type="simple_baseline_portfolio_dry_run_mapper",
            title="Simple Baseline Portfolio Dry-Run Mapper",
            status="DRY_RUN_MAPPING_BLOCKED",
            summary={"strategy_id": strategy_id, "blocking_reason": "strategy_id_not_found"},
            blocking_reasons=["strategy_id_not_found"],
            report_registry_entry=_report_registry_entry(
                "simple_baseline_portfolio_dry_run_mapper",
                "Simple Baseline Portfolio Dry-Run Mapper",
                "aits research strategies simple-baseline-portfolio-dry-run-mapper",
                "simple_baseline_portfolio_dry_run_mapper",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload
    dry_run_policy = _mapping(_research_policy(config).get("dry_run"))
    value = hypothetical_portfolio_value or _float(
        dry_run_policy.get("default_hypothetical_portfolio_value"), 100000.0
    )
    cash_buffer = _float(dry_run_policy.get("cash_buffer_weight"), 0.0)
    weights = _normalise_weights(_mapping(strategy.get("target_weights")))
    position_values = {ticker: round(weight * value, 2) for ticker, weight in weights.items()}
    payload = _payload(
        report_type="simple_baseline_portfolio_dry_run_mapper",
        title="Simple Baseline Portfolio Dry-Run Mapper",
        status="DRY_RUN_MAPPING_READY",
        summary={
            "strategy_id": strategy_id,
            "hypothetical_portfolio_value": round(value, 2),
            "cash_buffer": cash_buffer,
            "broker_action": "none",
        },
        strategy_id=strategy_id,
        target_weight_qqq=weights.get("QQQ", 0.0),
        target_weight_tqqq=weights.get("TQQQ", 0.0),
        target_weight_sgov=weights.get("SGOV", 0.0),
        hypothetical_portfolio_value=round(value, 2),
        hypothetical_position_values=position_values,
        rebalance_delta={ticker: "hypothetical_only_no_account_state_read" for ticker in weights},
        cash_buffer=cash_buffer,
        broker_action="none",
        broker_read_performed=False,
        order_ticket_generated=False,
        production_config_changed=False,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_portfolio_dry_run_mapper",
            "Simple Baseline Portfolio Dry-Run Mapper",
            "aits research strategies simple-baseline-portfolio-dry-run-mapper",
            "simple_baseline_portfolio_dry_run_mapper",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_master_review(
    *,
    ranking_path: Path | None = None,
    cost_sensitivity_path: Path | None = None,
    regime_review_path: Path | None = None,
    forward_aging_path: Path | None = None,
    readiness_path: Path | None = None,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    master_doc_path: Path = DEFAULT_SIMPLE_BASELINE_MASTER_REVIEW_DOC_PATH,
) -> dict[str, Any]:
    ranking_path = ranking_path or output_root / "simple_baseline_dominance_ranking.json"
    cost_sensitivity_path = (
        cost_sensitivity_path or output_root / "simple_baseline_cost_sensitivity.json"
    )
    regime_review_path = regime_review_path or output_root / "simple_baseline_regime_review.json"
    forward_aging_path = (
        forward_aging_path or output_root / "simple_baseline_forward_aging_tracker.json"
    )
    readiness_path = readiness_path or output_root / "simple_baseline_paper_shadow_readiness.json"
    ranking = _read_json_or_empty(ranking_path)
    cost = _read_json_or_empty(cost_sensitivity_path)
    regime = _read_json_or_empty(regime_review_path)
    forward = _read_json_or_empty(forward_aging_path)
    readiness = _read_json_or_empty(readiness_path)
    pareto = _records(ranking.get("pareto_frontier"))
    dominated = _records(ranking.get("dominated_strategy_list"))
    cost_sensitive = _records(cost.get("sensitivity_results"))
    weak_regimes = _records(regime.get("weak_regime_list"))
    status = "PAUSE_RESEARCH"
    if pareto:
        status = "NARROW_TO_TOP_CANDIDATES"
    if readiness.get("status") == "PAPER_SHADOW_REVIEWABLE_LATER":
        status = "PAPER_SHADOW_REVIEWABLE_LATER"
    if any(str(row.get("strategy_id", "")).startswith("tqqq_50") for row in pareto):
        status = "PAUSE_TQQQ_HEAVY"
    answers = {
        "pareto_frontier": [row.get("strategy_id") for row in pareto],
        "dominated_strategies": [row.get("strategy_id") for row in dominated],
        "high_return_unacceptable_drawdown": [
            row.get("strategy_id") for row in pareto if _float(row.get("max_drawdown")) < -0.35
        ],
        "regime_robust_candidates": [
            row.get("strategy_id")
            for row in pareto
            if row.get("strategy_id") not in {weak.get("strategy_id") for weak in weak_regimes}
        ],
        "cost_sensitive_candidates": sorted(
            {
                row.get("strategy_id")
                for row in cost_sensitive
                if _float(row.get("cost_sensitivity_score")) > 0.4
            }
        ),
        "forward_aging_observation_value": forward.get("status"),
        "paper_shadow_reviewable_later": readiness.get("status") == "PAPER_SHADOW_REVIEWABLE_LATER",
        "continue_tqqq_heavy_research": status != "PAUSE_TQQQ_HEAVY",
        "continue_pause_leaps_wheel": True,
        "owner_next_action": (
            "narrow" if status in {"NARROW_TO_TOP_CANDIDATES", "PAUSE_TQQQ_HEAVY"} else "continue"
        ),
    }
    payload = _payload(
        report_type="simple_baseline_master_review",
        title="Simple Baseline Portfolio Control Master Review",
        status=status,
        summary={
            "pareto_frontier_count": len(pareto),
            "dominated_strategy_count": len(dominated),
            "paper_shadow_reviewable_later": answers["paper_shadow_reviewable_later"],
            "owner_next_action": answers["owner_next_action"],
        },
        required_answers=answers,
        input_artifacts={
            "ranking": str(ranking_path),
            "cost_sensitivity": str(cost_sensitivity_path),
            "regime_review": str(regime_review_path),
            "forward_aging": str(forward_aging_path),
            "readiness": str(readiness_path),
        },
        report_registry_entry=_report_registry_entry(
            "simple_baseline_master_review",
            "Simple Baseline Portfolio Control Master Review",
            "aits research strategies simple-baseline-master-review",
            "simple_baseline_master_review",
        ),
    )
    _write_master_doc(payload, master_doc_path)
    payload["master_review_doc_path"] = str(master_doc_path)
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_options_next_stage_gate(
    *,
    master_review_path: Path | None = None,
    forward_aging_path: Path | None = None,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    master_review_path = master_review_path or output_root / "simple_baseline_master_review.json"
    forward_aging_path = (
        forward_aging_path or output_root / "simple_baseline_forward_aging_tracker.json"
    )
    master = _read_json_or_empty(master_review_path)
    forward = _read_json_or_empty(forward_aging_path)
    blockers = []
    allowed_master = set(
        _mapping(_research_policy(config).get("options_gate")).get(
            "minimum_master_review_status", []
        )
    )
    if master.get("status") not in allowed_master:
        blockers.append("simple baseline master review has not narrowed to stable candidates")
    min_forward = _research_float(config, "options_gate", "minimum_forward_mature_60d_samples")
    if _int(_mapping(forward.get("summary")).get("matured_60d_count")) < min_forward:
        blockers.append("forward aging lacks enough matured 60d samples")
    blockers.extend(
        [
            "option chain historical data contract is not established",
            "bid/ask, IV, greeks, expiration, assignment, and early exercise model is not ready",
            "TQQQ Wheel is disallowed as first-stage cash-flow research",
        ]
    )
    payload = _payload(
        report_type="options_next_stage_gate",
        title="Options Next-Stage Gate for LEAPS / Wheel",
        status="OPTIONS_RESEARCH_BLOCKED",
        summary={
            "options_research_allowed": False,
            "leaps_research_allowed": False,
            "wheel_research_allowed": False,
            "blocking_reason_count": len(blockers),
        },
        options_research_allowed=False,
        leaps_research_allowed=False,
        wheel_research_allowed=False,
        blocking_reasons=blockers,
        minimum_required_tasks=[
            "stable QQQ/TQQQ/SGOV paper-shadow candidate after owner review",
            "simple baseline master review narrowed to top candidates",
            "sufficient forward aging mature samples",
            "option chain historical data contract",
            "assignment / early exercise / slippage model",
        ],
        input_artifacts={
            "master_review": str(master_review_path),
            "forward_aging": str(forward_aging_path),
        },
        report_registry_entry=_report_registry_entry(
            "options_next_stage_gate",
            "Options Next-Stage Gate for LEAPS / Wheel",
            "aits research strategies options-next-stage-gate",
            "options_next_stage_gate",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _load_registry(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"simple baseline registry must be a mapping: {path}")
    return dict(raw)


def _strategy_rows(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in _records(config.get("strategies"))]


def _strategy_by_id(config: Mapping[str, Any], strategy_id: str) -> dict[str, Any]:
    for row in _strategy_rows(config):
        if row.get("strategy_id") == strategy_id:
            return row
    return {}


def _registry_issues(
    config: Mapping[str, Any],
    strategy_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if "policy_metadata" not in config:
        issues.append({"issue_id": "missing_policy_metadata"})
    seen: set[str] = set()
    for row in strategy_rows:
        strategy_id = str(row.get("strategy_id") or "UNKNOWN")
        if strategy_id in seen:
            issues.append({"strategy_id": strategy_id, "issue_id": "duplicate_strategy_id"})
        seen.add(strategy_id)
        missing = [field for field in REQUIRED_STRATEGY_FIELDS if field not in row]
        if missing:
            issues.append(
                {"strategy_id": strategy_id, "issue_id": "missing_fields", "fields": missing}
            )
        if row.get("uses_options") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "uses_options_not_false"})
        if row.get("production_effect") != "none" or row.get("broker_action") != "none":
            issues.append({"strategy_id": strategy_id, "issue_id": "unsafe_effect_field"})
        tqqq_weight = max(
            (
                _float(weight)
                for ticker, weight in _mapping(row.get("target_weights")).items()
                if ticker == "TQQQ"
            ),
            default=0.0,
        )
        if tqqq_weight > _float(row.get("max_tqqq_weight")):
            issues.append({"strategy_id": strategy_id, "issue_id": "target_tqqq_exceeds_max"})
    return issues


def _data_quality_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config: Mapping[str, Any],
    as_of_date: date | None,
    expected_tickers: list[str] | None = None,
) -> dict[str, Any]:
    resolved_as_of = as_of_date or _max_price_date(prices_path)
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_tickers or _required_tickers(config),
        expected_rate_series=_required_rate_series(config),
        quality_config=load_data_quality(),
        as_of=resolved_as_of,
        secondary_prices_path=marketstack_prices_path if marketstack_prices_path.exists() else None,
        require_secondary_prices=False,
    )
    return _data_quality_payload(report, prices_path, rates_path, marketstack_prices_path)


def _data_quality_payload(
    report: DataQualityReport,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path,
) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "price_path": str(prices_path),
        "rates_path": str(rates_path),
        "secondary_prices_path": str(marketstack_prices_path),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
        "issues": [
            {
                "severity": str(issue.severity),
                "code": issue.code,
                "message": issue.message,
                "rows": issue.rows,
                "source": issue.source,
            }
            for issue in report.issues
        ],
    }


def _load_price_matrix(path: Path, required_tickers: list[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    missing_columns = {"date", "ticker", "adj_close"} - set(frame.columns)
    if missing_columns:
        raise ValueError(f"price cache missing columns: {sorted(missing_columns)}")
    frame = frame[frame["ticker"].isin(required_tickers)].copy()
    pivot = frame.pivot_table(index="date", columns="ticker", values="adj_close", aggfunc="last")
    pivot = pivot.sort_index()
    missing_tickers = [ticker for ticker in required_tickers if ticker not in pivot.columns]
    if missing_tickers:
        raise ValueError(f"price cache missing required tickers: {missing_tickers}")
    return pivot.ffill()


def _run_backtest_set(
    *,
    strategy_rows: list[dict[str, Any]],
    prices_path: Path,
    config: Mapping[str, Any],
    start_date: date,
    end_date: date | None,
    cost_bps: float = 0.0,
    required_tickers: list[str] | None = None,
) -> list[dict[str, Any]]:
    prices = _load_price_matrix(prices_path, required_tickers or _required_tickers(config))
    prices = _slice_prices(prices, start_date=start_date, end_date=end_date)
    annualization = _research_policy_int(config, "annualization_trading_days")
    results = []
    for strategy in strategy_rows:
        returns = _strategy_return_series(strategy, prices, config, cost_bps=cost_bps)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        results.append(
            _metrics_for_strategy(
                strategy,
                returns,
                weights,
                prices["QQQ"].pct_change(),
                annualization=annualization,
                cost_bps=cost_bps,
            )
        )
    return results


def _strategy_return_series(
    strategy: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    cost_bps: float = 0.0,
) -> pd.Series:
    asset_returns = prices.pct_change().fillna(0.0)
    targets = _target_weight_frame(strategy, prices, config)
    applied = targets.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
    gross = (applied * asset_returns.reindex(columns=applied.columns).fillna(0.0)).sum(axis=1)
    turnover = _turnover_series(applied)
    cost = turnover * (cost_bps / 10000.0)
    return gross - cost


def _target_weight_frame(
    strategy: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    strategy_id = str(strategy.get("strategy_id"))
    base_weights = _normalise_weights(_mapping(strategy.get("target_weights")))
    if strategy_id.startswith("dyn_"):
        return _dynamic_weight_frame(strategy, prices, config)
    if strategy_id == "qqq_200dma_risk_off":
        return _qqq_200dma_weights(prices, config)
    if strategy_id == "qqq_100_200dma_trend_filter":
        return _qqq_100_200dma_weights(prices, config)
    if strategy_id == "qqq_volatility_target":
        return _qqq_vol_target_weights(prices, config)
    if strategy_id == "tqqq_volatility_capped":
        return _tqqq_vol_cap_weights(prices, config)
    if strategy_id == "tqqq_drawdown_capped":
        return _tqqq_drawdown_cap_weights(prices, config)
    if strategy_id == "equal_risk_qqq_sgov":
        return _equal_risk_weights(prices, config)
    frame = pd.DataFrame(index=prices.index, columns=sorted(base_weights), data=0.0)
    for ticker, weight in base_weights.items():
        frame[ticker] = weight
    return _apply_rebalance(frame, str(strategy.get("rebalance_frequency")))


def _qqq_200dma_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    long_window = _research_int(config, "moving_average_windows", "long")
    ma = prices["QQQ"].rolling(long_window, min_periods=long_window).mean().shift(1)
    close = prices["QQQ"].shift(1)
    risk_on = close >= ma
    qqq = risk_on.fillna(False).astype(float)
    return pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)


def _qqq_100_200dma_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    short_window = _research_int(config, "moving_average_windows", "short")
    long_window = _research_int(config, "moving_average_windows", "long")
    close = prices["QQQ"].shift(1)
    ma_short = prices["QQQ"].rolling(short_window, min_periods=short_window).mean().shift(1)
    ma_long = prices["QQQ"].rolling(long_window, min_periods=long_window).mean().shift(1)
    strong = (close >= ma_short) & (ma_short >= ma_long)
    moderate = close >= ma_long
    qqq = pd.Series(0.0, index=prices.index)
    qqq[moderate.fillna(False)] = 0.5
    qqq[strong.fillna(False)] = 1.0
    return pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)


def _qqq_vol_target_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    policy = _mapping(_research_policy(config).get("volatility_target"))
    target_vol = _float(policy.get("annual_vol_target"))
    min_weight = _float(policy.get("min_qqq_weight"))
    max_weight = _float(policy.get("max_qqq_weight"))
    vol = _realized_vol(
        prices["QQQ"],
        _research_int(config, "realized_vol_windows", "medium"),
        _research_policy_int(config, "annualization_trading_days"),
    )
    qqq = (target_vol / vol).clip(lower=min_weight, upper=max_weight).fillna(min_weight)
    return _apply_rebalance(
        pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index), "monthly"
    )


def _tqqq_vol_cap_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    policy = _mapping(_research_policy(config).get("tqqq_volatility_cap"))
    cap = _float(policy.get("annual_vol_cap"))
    max_weight = _float(policy.get("max_tqqq_weight"))
    min_weight = _float(policy.get("min_tqqq_weight"))
    vol = _realized_vol(
        prices["TQQQ"],
        _research_int(config, "realized_vol_windows", "medium"),
        _research_policy_int(config, "annualization_trading_days"),
    )
    tqqq = (cap / vol * max_weight).clip(lower=min_weight, upper=max_weight).fillna(min_weight)
    return _apply_rebalance(
        pd.DataFrame({"TQQQ": tqqq, "SGOV": 1.0 - tqqq}, index=prices.index), "monthly"
    )


def _tqqq_drawdown_cap_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    policy = _mapping(_research_policy(config).get("tqqq_drawdown_cap"))
    high = (
        prices["QQQ"].rolling(_research_int(config, "rolling_high_windows", "long")).max().shift(1)
    )
    dd = prices["QQQ"].shift(1) / high - 1.0
    max_weight = _float(policy.get("max_tqqq_weight"))
    warning_weight = _float(policy.get("warning_tqqq_weight"))
    block_weight = _float(policy.get("block_tqqq_weight"))
    tqqq = pd.Series(max_weight, index=prices.index)
    tqqq[dd <= _float(policy.get("qqq_drawdown_warning"))] = warning_weight
    tqqq[dd <= _float(policy.get("qqq_drawdown_block"))] = block_weight
    tqqq = tqqq.fillna(block_weight)
    return _apply_rebalance(
        pd.DataFrame({"TQQQ": tqqq, "SGOV": 1.0 - tqqq}, index=prices.index), "monthly"
    )


def _equal_risk_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    policy = _mapping(_research_policy(config).get("equal_risk"))
    window = _research_int(config, "realized_vol_windows", "medium")
    annualization = _research_policy_int(config, "annualization_trading_days")
    qqq_vol = _realized_vol(prices["QQQ"], window, annualization)
    sgov_vol = _realized_vol(prices["SGOV"], window, annualization)
    inv_qqq = 1.0 / qqq_vol.replace(0.0, math.nan)
    inv_sgov = 1.0 / sgov_vol.replace(0.0, math.nan)
    qqq = (inv_qqq / (inv_qqq + inv_sgov)).clip(
        lower=_float(policy.get("min_weight")), upper=_float(policy.get("max_weight"))
    )
    qqq = qqq.fillna(0.5)
    return _apply_rebalance(
        pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index), "monthly"
    )


def _dynamic_weight_frame(
    strategy: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    risk_on = _dynamic_risk_on(prices, config)
    risk_on_weights = _normalise_weights(_mapping(strategy.get("risk_on_weights")))
    risk_off_weights = _normalise_weights(_mapping(strategy.get("risk_off_weights")))
    tickers = sorted(set(risk_on_weights) | set(risk_off_weights))
    frame = pd.DataFrame(index=prices.index, columns=tickers, data=0.0)
    for ticker in tickers:
        frame[ticker] = [
            risk_on_weights.get(ticker, 0.0) if state else risk_off_weights.get(ticker, 0.0)
            for state in risk_on
        ]
    return frame


def _dynamic_risk_on(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.Series:
    short_window = _research_int(config, "moving_average_windows", "short")
    long_window = _research_int(config, "moving_average_windows", "long")
    ma_short = prices["QQQ"].rolling(short_window, min_periods=short_window).mean().shift(1)
    ma_long = prices["QQQ"].rolling(long_window, min_periods=long_window).mean().shift(1)
    close = prices["QQQ"].shift(1)
    annualization = _research_policy_int(config, "annualization_trading_days")
    vol = _realized_vol(
        prices["QQQ"],
        _research_int(config, "realized_vol_windows", "short"),
        annualization,
    )
    search = _mapping(_research_policy(config).get("dynamic_policy_search"))
    vol_thresholds = list(search.get("volatility_percentile_thresholds", []))
    drawdown_thresholds = list(search.get("drawdown_thresholds", []))
    vol_cutoff = _float(vol_thresholds[-1] if vol_thresholds else 0.8)
    drawdown_cutoff = _float(drawdown_thresholds[-1] if drawdown_thresholds else -0.1)
    vol_threshold = vol.rolling(_research_int(config, "rolling_high_windows", "long")).quantile(
        vol_cutoff
    )
    high = (
        prices["QQQ"]
        .rolling(_research_int(config, "rolling_high_windows", "medium"))
        .max()
        .shift(1)
    )
    dd = close / high - 1.0
    return (
        (close >= ma_short)
        & (ma_short >= ma_long)
        & (vol <= vol_threshold)
        & (dd >= drawdown_cutoff)
    ).fillna(False)


def _apply_rebalance(weights: pd.DataFrame, frequency: str) -> pd.DataFrame:
    if frequency in {"daily", "daily_signal_monthly_rebalance"}:
        return weights.ffill().fillna(0.0)
    if frequency == "quarterly":
        period = weights.index.to_period("Q").to_series(index=weights.index)
    else:
        period = weights.index.to_period("M").to_series(index=weights.index)
    markers = period != period.shift(1)
    rebalanced = weights.copy()
    rebalanced.loc[~markers] = math.nan
    rebalanced = rebalanced.ffill()
    if not rebalanced.empty:
        rebalanced.iloc[0] = weights.iloc[0]
    return rebalanced.ffill().fillna(0.0)


def _metrics_for_strategy(
    strategy: Mapping[str, Any],
    returns: pd.Series,
    weights: pd.DataFrame,
    benchmark_returns: pd.Series,
    *,
    annualization: int,
    cost_bps: float,
) -> dict[str, Any]:
    returns = returns.dropna()
    if returns.empty:
        return {"strategy_id": strategy.get("strategy_id"), "status": "NO_RETURNS"}
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    annual_return = _annual_return(equity, len(returns), annualization)
    annual_vol = float(returns.std(ddof=0) * math.sqrt(annualization))
    downside = returns[returns < 0]
    downside_vol = (
        float(downside.std(ddof=0) * math.sqrt(annualization)) if not downside.empty else 0.0
    )
    turnover = _turnover_series(weights.reindex(returns.index).ffill())
    cost_drag = float((turnover * (cost_bps / 10000.0)).sum())
    monthly = (1.0 + returns).resample("ME").prod() - 1.0
    quarterly = (1.0 + returns).resample("QE").prod() - 1.0
    benchmark = benchmark_returns.reindex(returns.index).fillna(0.0)
    benchmark_down = benchmark[benchmark < 0]
    strategy_on_down = returns.reindex(benchmark_down.index)
    downside_capture = (
        float(strategy_on_down.sum() / benchmark_down.sum())
        if not benchmark_down.empty and abs(float(benchmark_down.sum())) > 0
        else 0.0
    )
    avg_weights = weights.reindex(returns.index).fillna(0.0).mean().to_dict()
    return {
        "strategy_id": str(strategy.get("strategy_id")),
        "display_name": str(strategy.get("display_name", strategy.get("strategy_id"))),
        "status": "BACKTEST_READY",
        "annual_return": round(annual_return, 6),
        "annual_volatility": round(annual_vol, 6),
        "max_drawdown": round(float(drawdown.min()), 6),
        "sharpe": round(_ratio(annual_return, annual_vol), 6),
        "sortino": round(_ratio(annual_return, downside_vol), 6),
        "calmar": round(_ratio(annual_return, abs(float(drawdown.min()))), 6),
        "ulcer_index": round(float((drawdown.pow(2).mean()) ** 0.5), 6),
        "monthly_win_rate": round(float((monthly > 0).mean()) if len(monthly) else 0.0, 6),
        "worst_month": round(float(monthly.min()) if len(monthly) else 0.0, 6),
        "worst_quarter": round(float(quarterly.min()) if len(quarterly) else 0.0, 6),
        "turnover": round(float(turnover.sum()), 6),
        "max_single_rebalance_turnover": round(float(turnover.max()) if len(turnover) else 0.0, 6),
        "rebalance_count": int((turnover > 0).sum()),
        "cash_drag": round(float(avg_weights.get("SGOV", 0.0)), 6),
        "drawdown_recovery_days": _max_drawdown_recovery_days(equity),
        "downside_capture": round(downside_capture, 6),
        "tail_loss_frequency": round(float((returns <= returns.quantile(0.05)).mean()), 6),
        "average_weights": {
            str(key): round(_float(value), 6) for key, value in avg_weights.items()
        },
        "max_tqqq_weight_observed": round(
            float(weights.get("TQQQ", pd.Series(0.0, index=weights.index)).max()),
            6,
        ),
        "requested_date_range": _date_range_from_series(returns),
        "cost_drag": round(cost_drag, 6),
        "performance_degradation": round(cost_drag, 6),
        "cost_sensitivity_score": round(_ratio(cost_drag, abs(annual_return) + cost_drag), 6),
    }


def _with_tqqq_risk_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    avg = _mapping(enriched.get("average_weights"))
    enriched["effective_qqq_equivalent_exposure"] = round(
        _float(avg.get("QQQ")) + _float(avg.get("TQQQ")) * 3.0,
        6,
    )
    enriched["leverage_decay_risk_note"] = (
        "TQQQ is a daily-reset leveraged ETF; long horizon returns can differ from 3x QQQ."
    )
    enriched["path_dependency_note"] = (
        "Loss and recovery depend on realized path, volatility clustering, and rebalance timing."
    )
    enriched["max_single_month_loss"] = row.get("worst_month")
    enriched["max_single_quarter_loss"] = row.get("worst_quarter")
    return enriched


def _dynamic_candidate_strategies(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    search = _mapping(_research_policy(config).get("dynamic_policy_search"))
    candidates = []
    for row in _records(search.get("candidate_allocations")):
        risk_on = _mapping(row.get("risk_on_weights"))
        risk_off = _mapping(row.get("risk_off_weights"))
        candidates.append(
            {
                "strategy_id": str(row.get("policy_id")),
                "display_name": str(row.get("policy_id")),
                "asset_universe": sorted(set(risk_on) | set(risk_off)),
                "target_weights": risk_on,
                "risk_on_weights": risk_on,
                "risk_off_weights": risk_off,
                "rebalance_frequency": "daily",
                "risk_control_rule": "trend_vol_drawdown_dynamic_allocation",
                "trend_filter_rule": "qqq_100dma_200dma_state",
                "volatility_filter_rule": "qqq_realized_vol_20d_percentile",
                "drawdown_filter_rule": "qqq_120d_rolling_high_drawdown",
                "max_turnover": search.get("max_single_rebalance_turnover"),
                "max_tqqq_weight": search.get("max_tqqq_weight"),
                "uses_leverage_etf": "TQQQ" in risk_on or "TQQQ" in risk_off,
                "uses_options": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return candidates


def _ranking_row(row: Mapping[str, Any]) -> dict[str, Any]:
    simplicity = 1.0
    if "dynamic" in str(row.get("strategy_id")) or str(row.get("strategy_id")).startswith("dyn_"):
        simplicity = 0.5
    elif _float(_mapping(row.get("average_weights")).get("TQQQ")) > 0:
        simplicity = 0.7
    explainability = 1.0 if simplicity >= 0.7 else 0.8
    implementation_cost = 1.0 - simplicity
    return {
        **dict(row),
        "simplicity_score": simplicity,
        "explainability_score": explainability,
        "implementation_cost": implementation_cost,
        "ranking_score": round(
            _float(row.get("calmar"))
            + _float(row.get("sharpe"))
            + simplicity
            - implementation_cost,
            6,
        ),
    }


def _pareto_frontier(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frontier = []
    for row in rows:
        dominated = False
        for other in rows:
            if other is row:
                continue
            better_or_equal = (
                _float(other.get("annual_return")) >= _float(row.get("annual_return"))
                and _float(other.get("sharpe")) >= _float(row.get("sharpe"))
                and _float(other.get("calmar")) >= _float(row.get("calmar"))
                and abs(_float(other.get("max_drawdown"))) <= abs(_float(row.get("max_drawdown")))
                and _float(other.get("turnover")) <= _float(row.get("turnover"))
            )
            strictly_better = (
                _float(other.get("annual_return")) > _float(row.get("annual_return"))
                or _float(other.get("sharpe")) > _float(row.get("sharpe"))
                or _float(other.get("calmar")) > _float(row.get("calmar"))
                or abs(_float(other.get("max_drawdown"))) < abs(_float(row.get("max_drawdown")))
            )
            if better_or_equal and strictly_better:
                dominated = True
                break
        if not dominated:
            frontier.append(row)
    return sorted(frontier, key=lambda item: _float(item.get("ranking_score")), reverse=True)


def _regime_labels(prices: pd.DataFrame, config: Mapping[str, Any]) -> dict[str, pd.Series]:
    qqq = prices["QQQ"]
    returns = qqq.pct_change().fillna(0.0)
    long_window = _research_int(config, "moving_average_windows", "long")
    vol_window = _research_int(config, "realized_vol_windows", "short")
    high_window = _research_int(config, "rolling_high_windows", "medium")
    annualization = _research_policy_int(config, "annualization_trading_days")
    regime_policy = _mapping(_research_policy(config).get("regime_review"))
    trend_threshold = _float(regime_policy.get("trend_return_threshold_60d"))
    drawdown_threshold = _float(regime_policy.get("drawdown_state_threshold"))
    low_vol_quantile = _float(regime_policy.get("low_vol_quantile"))
    high_vol_quantile = _float(regime_policy.get("high_vol_quantile"))
    ma_200 = qqq.rolling(long_window, min_periods=vol_window).mean()
    vol = returns.rolling(vol_window, min_periods=max(1, vol_window // 4)).std() * math.sqrt(
        annualization
    )
    high = qqq.rolling(high_window, min_periods=vol_window).max()
    drawdown = qqq / high - 1.0
    trend_return = returns.rolling(60, min_periods=vol_window).sum()
    return {
        "bull": trend_return > trend_threshold,
        "bear": trend_return < -trend_threshold,
        "range": trend_return.abs() <= trend_threshold,
        "high_vol": vol >= vol.quantile(high_vol_quantile),
        "mid_vol": (vol > vol.quantile(low_vol_quantile)) & (vol < vol.quantile(high_vol_quantile)),
        "low_vol": vol <= vol.quantile(low_vol_quantile),
        "above_200dma": qqq >= ma_200,
        "below_200dma": qqq < ma_200,
        "drawdown_state": drawdown <= drawdown_threshold,
        "recovery_state": (drawdown > drawdown_threshold) & (drawdown < 0.0),
    }


def _strategy_regime_rows(
    strategy_id: str,
    returns: pd.Series,
    regime_labels: Mapping[str, pd.Series],
    *,
    annualization: int,
) -> list[dict[str, Any]]:
    rows = []
    for regime, labels in regime_labels.items():
        selected = returns.reindex(labels.index)[labels.fillna(False)]
        if selected.empty:
            continue
        equity = (1.0 + selected).cumprod()
        drawdown = equity / equity.cummax() - 1.0
        annual_return = _annual_return(equity, len(selected), annualization)
        vol = float(selected.std(ddof=0) * math.sqrt(annualization))
        rows.append(
            {
                "strategy_id": strategy_id,
                "regime": regime,
                "annual_return": round(annual_return, 6),
                "max_drawdown": round(float(drawdown.min()), 6),
                "sharpe": round(_ratio(annual_return, vol), 6),
                "sample_count": int(len(selected)),
            }
        )
    return rows


def _forward_aging_rows(
    strategy_id: str,
    returns: pd.Series,
    weights: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    horizons = {"5d": 5, "10d": 10, "20d": 20, "60d": 60}
    for idx, decision_date in enumerate(returns.index):
        row: dict[str, Any] = {
            "decision_date": decision_date.date().isoformat(),
            "strategy_id": strategy_id,
            "target_weights": {
                str(key): round(_float(value), 6)
                for key, value in weights.loc[decision_date].to_dict().items()
            },
            "pending_outcomes": [],
        }
        for label, horizon in horizons.items():
            if idx + horizon < len(returns):
                outcome = float((1.0 + returns.iloc[idx + 1 : idx + horizon + 1]).prod() - 1.0)
                row[f"matured_{label}_return"] = round(outcome, 6)
            else:
                row[f"matured_{label}_return"] = None
                row["pending_outcomes"].append(label)
        rolling = returns.iloc[max(0, idx - 20) : idx + 1]
        row["rolling_forward_return"] = round(float((1.0 + rolling).prod() - 1.0), 6)
        if rolling.empty:
            row["rolling_forward_drawdown"] = 0.0
        else:
            equity = (1.0 + rolling).cumprod()
            row["rolling_forward_drawdown"] = round(
                float((equity / equity.cummax() - 1.0).min()), 6
            )
        rows.append(row)
    return rows


def _readiness_blockers(
    sources: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
    missing: list[str],
) -> list[str]:
    blockers = [f"missing input artifact: {name}" for name in missing]
    if sources.get("pit", {}).get("status") != "PIT_BOUNDARY_PASS":
        blockers.append("PIT boundary audit has not passed")
    if not _records(sources.get("ranking", {}).get("recommended_research_candidates")):
        blockers.append("baseline dominance ranking has no recommended candidate")
    regime_score = _float(sources.get("regime", {}).get("regime_concentration_score"))
    if regime_score > _research_float(config, "readiness_rules", "max_regime_concentration_score"):
        blockers.append("regime concentration is too high")
    cost_score = _float(
        _mapping(sources.get("cost", {}).get("summary")).get("max_cost_sensitivity_score")
    )
    if cost_score > _research_float(config, "readiness_rules", "max_cost_sensitivity_score"):
        blockers.append("cost sensitivity is too high")
    mature_20d = _int(_mapping(sources.get("forward", {}).get("summary")).get("matured_20d_count"))
    if mature_20d < _research_float(config, "readiness_rules", "min_forward_mature_20d_samples"):
        blockers.append("forward aging lacks enough 20d matured samples")
    return blockers


def _blocked_data_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "data_quality_status": data_gate.get("status"),
            "data_quality_error_count": data_gate.get("error_count"),
            "blocked_reason": "validate_data_cache_failed",
        },
        data_quality=data_gate,
        blockers=["validate_data_cache_failed"],
    )


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    paths = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    payload["artifact_paths"] = paths
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_master_doc(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        "# Simple Baseline Portfolio Control Master Review",
        "",
        f"- 状态：`{payload.get('status')}`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "- promotion_allowed：`false`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "",
        "## 关键结论",
        "",
        f"- Pareto frontier：`{answers.get('pareto_frontier')}`",
        f"- 被支配策略：`{answers.get('dominated_strategies')}`",
        f"- Paper-shadow reviewable later：`{answers.get('paper_shadow_reviewable_later')}`",
        f"- TQQQ-heavy 继续研究：`{answers.get('continue_tqqq_heavy_research')}`",
        f"- LEAPS / Wheel 继续暂缓：`{answers.get('continue_pause_leaps_wheel')}`",
        f"- owner_next_action：`{answers.get('owner_next_action')}`",
        "",
        "本报告只用于 research-only / observe-only 人工复核，不生成真实交易建议、订单或生产仓位。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    *,
    include_in_reader_brief: bool = True,
    required_for_daily_reading: bool = False,
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "reviewer",
        "owner": "system",
        "command": command,
        "artifact_globs": [
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.json",
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.md",
        ],
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "Simple baseline research artifacts are regenerated during manual research review."
        ),
        "owner_action": "review_simple_baseline_research_only_artifact",
        "include_in_reader_brief": include_in_reader_brief,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": required_for_daily_reading,
    }


def _registry_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": config.get("policy_id"),
        "policy_metadata": config.get("policy_metadata"),
        "market_regime": config.get("market_regime"),
        "safety_boundary": config.get("safety_boundary"),
    }


def _research_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return dict(_mapping(config.get("research_policy")))


def _research_float(config: Mapping[str, Any], section: str, field: str) -> float:
    return _float(_mapping(_research_policy(config).get(section)).get(field))


def _research_int(config: Mapping[str, Any], section: str, field: str) -> int:
    return int(_research_float(config, section, field))


def _research_policy_int(config: Mapping[str, Any], field: str) -> int:
    return int(_float(_research_policy(config).get(field)))


def _required_tickers(config: Mapping[str, Any]) -> list[str]:
    tickers = list(_mapping(_research_policy(config)).get("required_price_tickers", []))
    return [str(ticker) for ticker in tickers]


def _required_rate_series(config: Mapping[str, Any]) -> list[str]:
    series = list(_mapping(_research_policy(config)).get("required_rate_series", []))
    return [str(item) for item in series]


def _slice_prices(prices: pd.DataFrame, *, start_date: date, end_date: date | None) -> pd.DataFrame:
    sliced = prices[prices.index.date >= start_date]
    if end_date is not None:
        sliced = sliced[sliced.index.date <= end_date]
    return sliced


def _normalise_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    parsed = {str(key): _float(value) for key, value in weights.items() if _float(value) > 0}
    total = sum(parsed.values())
    if total <= 0:
        return {}
    return {key: value / total for key, value in parsed.items()}


def _realized_vol(series: pd.Series, window: int, annualization: int) -> pd.Series:
    return series.pct_change().rolling(window, min_periods=window).std().shift(1) * math.sqrt(
        annualization
    )


def _turnover_series(weights: pd.DataFrame) -> pd.Series:
    return weights.fillna(0.0).diff().abs().sum(axis=1).fillna(0.0) / 2.0


def _annual_return(equity: pd.Series, observations: int, annualization: int) -> float:
    if observations <= 0 or equity.empty:
        return 0.0
    return float(equity.iloc[-1] ** (annualization / observations) - 1.0)


def _max_drawdown_recovery_days(equity: pd.Series) -> int:
    peak = equity.cummax()
    below = equity < peak
    longest = 0
    current = 0
    for flag in below:
        if bool(flag):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _backtest_summary(
    results: list[dict[str, Any]], data_gate: Mapping[str, Any]
) -> dict[str, Any]:
    best = max(results, key=lambda row: _float(row.get("calmar")), default={})
    return {
        "strategy_count": len(results),
        "best_by_calmar": best.get("strategy_id"),
        "best_calmar": best.get("calmar"),
        "data_quality_status": data_gate.get("status"),
        "data_quality_report_visible": True,
    }


def _tqqq_status(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "TQQQ_BASELINE_INCONCLUSIVE"
    risky = any(_float(row.get("max_drawdown")) < -0.50 for row in rows)
    return "TQQQ_BASELINE_TOO_RISKY" if risky else "TQQQ_BASELINE_RESEARCH_READY"


def _rebalance_frequency_sensitivity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "strategy_id": row.get("strategy_id"),
            "turnover": row.get("turnover"),
            "rebalance_count": row.get("rebalance_count"),
            "annual_return": row.get("annual_return"),
        }
        for row in rows
        if "quarterly" in str(row.get("strategy_id")) or "tqqq_33" in str(row.get("strategy_id"))
    ]


def _top(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: _float(row.get(key)), reverse=True)[:10]


def _dedupe_by_strategy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        strategy_id = str(row.get("strategy_id"))
        if strategy_id not in best or _float(row.get("calmar")) > _float(
            best[strategy_id].get("calmar")
        ):
            best[strategy_id] = row
    return list(best.values())


def _regime_concentration_score(rows: list[dict[str, Any]]) -> float:
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row.get("regime"))] = counts.get(str(row.get("regime")), 0) + _int(
            row.get("sample_count")
        )
    total = sum(counts.values())
    return max((count / total for count in counts.values()), default=0.0) if total else 0.0


def _contains_forbidden_pit_input(value: str) -> bool:
    forbidden = (
        "future",
        "tail_loss_case",
        "large_loss_case",
        "long_horizon_failure_case",
        "fallback_triggered",
    )
    lowered = value.lower()
    return any(item in lowered for item in forbidden)


def _max_price_date(path: Path) -> date:
    if not path.exists():
        return DEFAULT_AI_REGIME_BACKTEST_START
    frame = pd.read_csv(path, usecols=["date"], parse_dates=["date"])
    if frame.empty:
        return DEFAULT_AI_REGIME_BACKTEST_START
    return frame["date"].max().date()


def _date_range_from_price_path(path: Path, start_date: date, end_date: date | None) -> str:
    if not path.exists():
        return f"{start_date.isoformat()}..unknown"
    frame = pd.read_csv(path, usecols=["date"], parse_dates=["date"])
    if frame.empty:
        return f"{start_date.isoformat()}..unknown"
    end = end_date or frame["date"].max().date()
    return f"{start_date.isoformat()}..{end.isoformat()}"


def _date_range_from_price_frame(prices: pd.DataFrame) -> str:
    if prices.empty:
        return "empty"
    return f"{prices.index.min().date().isoformat()}..{prices.index.max().date().isoformat()}"


def _date_range_from_series(series: pd.Series) -> str:
    if series.empty:
        return "empty"
    return f"{series.index.min().date().isoformat()}..{series.index.max().date().isoformat()}"


def _requested_range(rows: list[dict[str, Any]], start_date: date, end_date: date | None) -> str:
    if rows:
        return str(rows[0].get("requested_date_range"))
    end = "open" if end_date is None else end_date.isoformat()
    return f"{start_date.isoformat()}..{end}"


def _ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-12:
        return 0.0
    return numerator / denominator


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return rows[0] if rows else {}


def _float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
