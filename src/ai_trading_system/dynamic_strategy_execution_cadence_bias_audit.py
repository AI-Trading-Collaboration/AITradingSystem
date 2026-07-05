from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.equal_risk_growth_tilt import FOCUSED_GROWTH_TILT_CANDIDATE_ID
from ai_trading_system.execution_semantics import (
    AI_REGIME_SUMMARY,
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
    _actual_position_path,
    _attach_path_return_columns,
    _constant_weight_frame,
    _execution_materiality_thresholds,
    _file_sha256,
    _float,
    _int,
    _load_execution_price_matrix,
    _load_policy_registry,
    _load_registry,
    _mapping,
    _performance_metrics,
    _policies_by_id,
    _policy_cost_bps,
    _signal_target_weight_frame,
    _stable_hash,
    _synthetic_policy,
)
from ai_trading_system.simple_baseline_portfolio_control import _data_quality_gate

TASK_ID = "TRADING-2364"
TASK_REGISTER_ID = "TRADING-2364_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_AND_RETEST"
REPORT_TYPE = "dynamic_strategy_execution_cadence_bias_audit"
SCHEMA_VERSION = "dynamic_strategy_execution_cadence_bias_audit.v1"
READY_STATUS = "DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_READY"
BLOCKED_STATUS = "DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_BLOCKED_DATA_QUALITY"
NEXT_ROUTE = "TRADING-2365_Dynamic_Strategy_Event_Driven_Retest_And_Candidate_Ranking"

DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)

SCENARIO_ORDER: tuple[str, ...] = (
    "static_baseline",
    "monthly_rebalance",
    "weekly_rebalance",
    "daily_rebalance",
    "signal_event_driven",
    "valid_until_window",
    "cooldown_limited_event_driven",
)

SCENARIO_POLICY_IDS: dict[str, str] = {
    "monthly_rebalance": "monthly_eom_v1",
    "weekly_rebalance": "weekly_friday_v1",
    "daily_rebalance": "daily_close_next_day_v1",
    "signal_event_driven": "threshold_drift_5pct_v1",
    "valid_until_window": "validity_10d_v1",
    "cooldown_limited_event_driven": "min_holding_20d_v1",
}

STATIC_BASELINE_STRATEGY_ID = "qqq_60_sgov_40"
STATIC_BASELINE_WEIGHTS: dict[str, float] = {"QQQ": 0.60, "TQQQ": 0.0, "SGOV": 0.40}

DEFAULT_DYNAMIC_AUDIT_STRATEGY_IDS: tuple[str, ...] = (
    "limited_adjustment",
    "defensive_limited_adjustment",
    "dynamic_regime_overlay_v0_4_lower_turnover",
    "dynamic_v0_5_ai_trend_confirmed_only",
    "equal_risk_qqq_sgov",
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
)

ASSET_COLUMNS: tuple[str, ...] = ("QQQ", "TQQQ", "SGOV")


def run_dynamic_strategy_execution_cadence_bias_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Path = DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_DOCS_ROOT,
    strategy_ids: Sequence[str] | None = None,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    transaction_cost_bps: float | None = None,
    slippage_bps: float | None = None,
    turnover_penalty: float = 0.0,
    max_turnover_per_month: float = 1.0,
    min_holding_days: int = 20,
    cooldown_days: int = 20,
    max_single_step_weight_delta: float = 0.75,
    risk_cap_enabled: bool = True,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    config = _load_registry(simple_config_path)
    required_tickers = sorted({"QQQ", "TQQQ", "SGOV"})
    data_quality = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=required_tickers,
    )
    policy_registry = _load_policy_registry(policy_registry_path)
    thresholds = _execution_materiality_thresholds(policy_registry)
    cost_constraint_policy = _cost_constraint_policy(
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
        turnover_penalty=turnover_penalty,
        max_turnover_per_month=max_turnover_per_month,
        min_holding_days=min_holding_days,
        cooldown_days=cooldown_days,
        max_single_step_weight_delta=max_single_step_weight_delta,
        risk_cap_enabled=risk_cap_enabled,
    )
    if not bool(data_quality.get("passed")):
        payload = _base_payload(
            status=BLOCKED_STATUS,
            as_of_date=as_of_date,
            start_date=resolved_start,
            end_date=end_date,
            data_quality=data_quality,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            policy_registry_path=policy_registry_path,
            cost_constraint_policy=cost_constraint_policy,
        )
        payload.update(
            {
                "summary": {
                    "blocked_reason": "data_quality_gate_failed",
                    "data_quality_status": data_quality.get("status"),
                    "scenario_count": 0,
                },
                "scenarios_tested": [],
                "comparison_matrix": [],
                "conclusions": {
                    "monthly_rebalance_distorts_signal_response": "UNKNOWN_BLOCKED",
                    "event_driven_or_valid_until_should_be_default": "UNKNOWN_BLOCKED",
                    "old_dynamic_strategy_results_need_retest": True,
                    "recommended_next_route": NEXT_ROUTE,
                },
            }
        )
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    prices = _load_execution_price_matrix(
        prices_path,
        config,
        start_date=resolved_start,
        end_date=end_date,
    )
    policies = _policies_by_id(policy_registry)
    selected_strategy_ids = _dedupe_strategy_ids(
        strategy_ids or DEFAULT_DYNAMIC_AUDIT_STRATEGY_IDS
    )
    scenario_definitions = _scenario_definitions(cost_constraint_policy)

    static_row = _static_baseline_row(
        prices=prices,
        cost_constraint_policy=cost_constraint_policy,
        thresholds=thresholds,
    )
    scenario_rows: list[dict[str, Any]] = [static_row]
    path_summaries: list[dict[str, Any]] = []
    qqq_metrics = _benchmark_metrics(prices)

    for strategy_id in selected_strategy_ids:
        target_weights = _signal_target_weight_frame(strategy_id, prices)
        for scenario_id in SCENARIO_ORDER:
            if scenario_id == "static_baseline":
                continue
            policy = _scenario_policy(
                scenario_id=scenario_id,
                policies=policies,
                transaction_cost_bps=transaction_cost_bps,
                slippage_bps=slippage_bps,
                max_turnover_per_month=max_turnover_per_month,
                min_holding_days=min_holding_days,
                cooldown_days=cooldown_days,
                max_single_step_weight_delta=max_single_step_weight_delta,
            )
            actual_weights, path_rows = _actual_position_path(
                strategy_id=strategy_id,
                execution_policy_id=str(policy["execution_policy_id"]),
                target_weights=target_weights,
                policy=policy,
                signal_validity_profile=_signal_validity_profile(scenario_id, policy),
                enable_staleness_filter=scenario_id == "valid_until_window",
                stale_action="suppress_rebalance"
                if scenario_id == "valid_until_window"
                else None,
            )
            cost_bps = _policy_cost_bps(policy)
            _attach_path_return_columns(
                prices=prices,
                target_weights=target_weights,
                actual_weights=actual_weights,
                path_rows=path_rows,
                cost_bps=cost_bps,
            )
            row = _scenario_metric_row(
                prices=prices,
                strategy_id=strategy_id,
                scenario_id=scenario_id,
                policy=policy,
                target_weights=target_weights,
                actual_weights=actual_weights,
                path_rows=path_rows,
                static_baseline=static_row,
                qqq_metrics=qqq_metrics,
                thresholds=thresholds,
                cost_constraint_policy=cost_constraint_policy,
            )
            scenario_rows.append(row)
            path_summaries.append(_path_summary(row, path_rows))

    comparison_matrix = _comparison_matrix(
        scenario_rows,
        turnover_penalty=_float(cost_constraint_policy.get("turnover_penalty")),
    )
    strategy_rankings = _strategy_rankings(
        scenario_rows,
        turnover_penalty=_float(cost_constraint_policy.get("turnover_penalty")),
    )
    conclusions = _conclusions(
        scenario_rows=scenario_rows,
        thresholds=thresholds,
        cost_constraint_policy=cost_constraint_policy,
    )
    payload = _base_payload(
        status=READY_STATUS,
        as_of_date=as_of_date,
        start_date=resolved_start,
        end_date=end_date,
        data_quality=data_quality,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        policy_registry_path=policy_registry_path,
        cost_constraint_policy=cost_constraint_policy,
    )
    payload.update(
        {
            "summary": {
                "scenario_count": len(scenario_rows),
                "dynamic_strategy_count": len(selected_strategy_ids),
                "scenarios_tested": list(SCENARIO_ORDER),
                "cadence_bias_detected": conclusions["cadence_bias_detected"],
                "recommended_default_execution_cadence": conclusions[
                    "recommended_default_execution_cadence"
                ],
                "old_dynamic_strategy_results_need_retest": conclusions[
                    "old_dynamic_strategy_results_need_retest"
                ],
                "recommended_next_route": NEXT_ROUTE,
                "data_quality_status": data_quality.get("status"),
            },
            "scenario_definitions": scenario_definitions,
            "scenarios_tested": list(SCENARIO_ORDER),
            "dynamic_strategy_ids": selected_strategy_ids,
            "static_baseline_strategy_id": STATIC_BASELINE_STRATEGY_ID,
            "comparison_matrix": comparison_matrix,
            "scenario_rows": scenario_rows,
            "strategy_rankings": strategy_rankings,
            "path_summaries": path_summaries,
            "materiality_thresholds": thresholds,
            "conclusions": conclusions,
            "required_conclusions": {
                "monthly_rebalance_distorts_signal_response": conclusions[
                    "monthly_rebalance_distorts_signal_response"
                ],
                "event_driven_or_valid_until_should_be_default": conclusions[
                    "event_driven_or_valid_until_should_be_default"
                ],
                "old_dynamic_strategy_results_need_retest": conclusions[
                    "old_dynamic_strategy_results_need_retest"
                ],
                "recommended_next_research_task": NEXT_ROUTE,
            },
            "retest_next_steps": _retest_next_steps(conclusions, strategy_rankings),
            "backtest_run": True,
            "research_quality_status": "MONTHLY_CADENCE_RETEST_REQUIRED"
            if conclusions["old_dynamic_strategy_results_need_retest"]
            else "MONTHLY_CADENCE_REFERENCE_REVIEWED",
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _base_payload(
    *,
    status: str,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
    data_quality: Mapping[str, Any],
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    policy_registry_path: Path,
    cost_constraint_policy: Mapping[str, Any],
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    actual_end = str(data_quality.get("as_of") or end_date or "")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "status": status,
        "generated_at": generated_at,
        "as_of": as_of_date.isoformat() if as_of_date else data_quality.get("as_of"),
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "requested_date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else actual_end,
        },
        "production_effect": SAFETY_BOUNDARY["production_effect"],
        "broker_action": SAFETY_BOUNDARY["broker_action"],
        "promotion_allowed": SAFETY_BOUNDARY["promotion_allowed"],
        "paper_shadow_allowed": SAFETY_BOUNDARY["paper_shadow_allowed"],
        "production_allowed": SAFETY_BOUNDARY["production_allowed"],
        "manual_review_required": SAFETY_BOUNDARY["manual_review_required"],
        "research_only": SAFETY_BOUNDARY["research_only"],
        "observe_only": SAFETY_BOUNDARY["observe_only"],
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_attempted": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "daily_report_generated": False,
        "data_quality": dict(data_quality),
        "data_quality_gate_executed": True,
        "data_sources": {
            "prices_path": str(prices_path),
            "marketstack_prices_path": str(marketstack_prices_path),
            "rates_path": str(rates_path),
            "policy_registry_path": str(policy_registry_path),
            "prices_checksum": data_quality.get("price_checksum"),
            "rates_checksum": data_quality.get("rate_checksum"),
            "policy_registry_sha256": _file_sha256(policy_registry_path),
            "download_timestamp": data_quality.get("checked_at"),
            "price_row_count": data_quality.get("price_row_count"),
            "rate_row_count": data_quality.get("rate_row_count"),
        },
        "cost_constraint_policy": dict(cost_constraint_policy),
        "next_route": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _dedupe_strategy_ids(strategy_ids: Sequence[str]) -> list[str]:
    result: list[str] = []
    for strategy_id in strategy_ids:
        item = str(strategy_id).strip()
        if item and item not in result:
            result.append(item)
    return result or list(DEFAULT_DYNAMIC_AUDIT_STRATEGY_IDS)


def _cost_constraint_policy(
    *,
    transaction_cost_bps: float | None,
    slippage_bps: float | None,
    turnover_penalty: float,
    max_turnover_per_month: float,
    min_holding_days: int,
    cooldown_days: int,
    max_single_step_weight_delta: float,
    risk_cap_enabled: bool,
) -> dict[str, Any]:
    return {
        "policy_id": "dynamic_strategy_execution_cadence_bias_audit_policy_v1",
        "owner": "research_governance",
        "status": "pilot_baseline_owner_review_required",
        "rationale": (
            "Make execution cadence, transaction cost, slippage, turnover cap, "
            "holding-period and cooldown assumptions explicit for research-only "
            "monthly-bias retesting."
        ),
        "intended_effect": (
            "Prevent dynamic monthly target-path conclusions from being interpreted "
            "as execution-ready evidence without actual-path cadence comparison."
        ),
        "validation_evidence": (
            "TRADING-2364 focused tests, real data-quality-gated run, and owner "
            "review before TRADING-2365 candidate ranking."
        ),
        "review_condition": (
            "Review before choosing a default dynamic execution cadence or using "
            "any result for paper-shadow preflight."
        ),
        "transaction_cost_bps_override": transaction_cost_bps,
        "slippage_bps_override": slippage_bps,
        "turnover_penalty": round(float(turnover_penalty), 8),
        "max_turnover_per_month": round(float(max_turnover_per_month), 6),
        "min_holding_days": int(min_holding_days),
        "cooldown_days": int(cooldown_days),
        "max_single_step_weight_delta": round(float(max_single_step_weight_delta), 6),
        "risk_cap_enabled": bool(risk_cap_enabled),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _scenario_definitions(cost_constraint_policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": "static_baseline",
            "strategy_id": STATIC_BASELINE_STRATEGY_ID,
            "execution_policy_id": "static_hold_qqq_60_sgov_40",
            "description": "Static 60% QQQ / 40% SGOV baseline with no dynamic signal.",
            "old_cadence_reference": False,
            "no_lookahead_required": False,
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
        {
            "scenario_id": "monthly_rebalance",
            "execution_policy_id": "monthly_eom_v1",
            "description": "Legacy monthly end-of-month dynamic actual-path reference.",
            "old_cadence_reference": True,
            "no_lookahead_required": True,
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
        {
            "scenario_id": "weekly_rebalance",
            "execution_policy_id": "weekly_friday_v1",
            "description": "Weekly Friday-or-last-session execution sensitivity.",
            "old_cadence_reference": False,
            "no_lookahead_required": True,
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
        {
            "scenario_id": "daily_rebalance",
            "execution_policy_id": "daily_close_next_day_v1",
            "description": "Daily close signal with next trading day execution.",
            "old_cadence_reference": False,
            "no_lookahead_required": True,
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
        {
            "scenario_id": "signal_event_driven",
            "execution_policy_id": "threshold_drift_5pct_v1",
            "description": (
                "Event-driven execution when target/actual drift crosses policy "
                "threshold."
            ),
            "old_cadence_reference": False,
            "no_lookahead_required": True,
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
        {
            "scenario_id": "valid_until_window",
            "execution_policy_id": "validity_10d_v1",
            "description": "Valid-until execution with stale-signal suppression enabled.",
            "old_cadence_reference": False,
            "no_lookahead_required": True,
            "expired_signal_suppression_rule": "suppress_rebalance",
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
        {
            "scenario_id": "cooldown_limited_event_driven",
            "execution_policy_id": "min_holding_20d_v1",
            "description": "Event-driven drift trigger constrained by minimum holding/cooldown.",
            "old_cadence_reference": False,
            "no_lookahead_required": True,
            "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        },
    ]


def _scenario_policy(
    *,
    scenario_id: str,
    policies: Mapping[str, Mapping[str, Any]],
    transaction_cost_bps: float | None,
    slippage_bps: float | None,
    max_turnover_per_month: float,
    min_holding_days: int,
    cooldown_days: int,
    max_single_step_weight_delta: float,
) -> dict[str, Any]:
    policy_id = SCENARIO_POLICY_IDS[scenario_id]
    policy = dict(policies.get(policy_id) or _synthetic_policy(policy_id))
    policy["execution_policy_id"] = policy_id
    cost_model = dict(_mapping(policy.get("cost_model")))
    base_transaction = _float(cost_model.get("explicit_cost_bps"), 1.0)
    base_slippage = _float(cost_model.get("slippage_bps"), 0.0)
    transaction = base_transaction if transaction_cost_bps is None else transaction_cost_bps
    slippage = base_slippage if slippage_bps is None else slippage_bps
    cost_model["transaction_cost_bps"] = round(float(transaction), 6)
    cost_model["slippage_bps"] = round(float(slippage), 6)
    cost_model["explicit_cost_bps"] = round(float(transaction) + float(slippage), 6)
    policy["cost_model"] = cost_model
    existing_cap = _float(policy.get("max_turnover_per_period"), 1.0)
    policy["max_turnover_per_period"] = round(
        min(existing_cap, float(max_single_step_weight_delta)),
        6,
    )
    policy["max_turnover_per_month"] = round(float(max_turnover_per_month), 6)
    if scenario_id == "cooldown_limited_event_driven":
        policy["minimum_holding_period"] = max(
            _int(policy.get("minimum_holding_period"), 0),
            int(min_holding_days),
            int(cooldown_days),
        )
        policy["cooldown_days"] = int(cooldown_days)
        policy["execution_frequency"] = "threshold_with_min_holding"
    return policy


def _signal_validity_profile(
    scenario_id: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    validity_days = max(1, _int(policy.get("validity_period_days"), 10))
    profile = {
        "primary_signal_class": "dynamic_strategy_target_weight",
        "confirmation_required": False,
        "min_validity_days_required_for_execution": 1,
        "stale_after_days": validity_days,
        "near_stale_within_days": 1,
        "stale_action": "hold_previous_position",
        "actual_path_only": True,
    }
    if scenario_id == "valid_until_window":
        profile["stale_action"] = "suppress_rebalance"
    return profile


def _static_baseline_row(
    *,
    prices: pd.DataFrame,
    cost_constraint_policy: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    weights = _constant_weight_frame(prices, STATIC_BASELINE_WEIGHTS)
    metrics = _performance_metrics(prices, weights, cost_bps=0.0)
    returns = _portfolio_return_series(prices, weights, cost_bps=0.0)
    capture = _capture_metrics(prices, returns)
    total_return = _total_return(returns)
    return {
        "scenario_id": "static_baseline",
        "strategy_id": STATIC_BASELINE_STRATEGY_ID,
        "execution_policy_id": "static_hold_qqq_60_sgov_40",
        "old_cadence_reference": False,
        "no_lookahead_evidence": {"uses_future_data": False, "not_applicable": True},
        "performance": _performance_summary(metrics, total_return, capture, returns),
        "execution": {
            "rebalance_count": 0,
            "turnover": 0.0,
            "avg_holding_days": 0.0,
            "signal_to_execution_lag_days": 0.0,
            "missed_signal_count": 0,
            "stale_signal_execution_count": 0,
            "cooldown_block_count": 0,
            "constraint_hit_count": 0,
        },
        "research_quality": {
            "dynamic_vs_static_gap": 0.0,
            "monthly_vs_event_driven_gap": 0.0,
            "weekly_vs_monthly_gap": 0.0,
            "turnover_adjusted_improvement": 0.0,
            "cost_adjusted_improvement": 0.0,
            "false_risk_off_count": 0,
            "missed_upside_count": 0,
        },
        "cost_adjusted": {
            "transaction_cost_bps": 0.0,
            "slippage_bps": 0.0,
            "cost_bps_total": 0.0,
            "cost_drag": 0.0,
        },
        "materiality": {
            "thresholds": dict(thresholds),
            "material_vs_static": False,
        },
        "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
    }


def _scenario_metric_row(
    *,
    prices: pd.DataFrame,
    strategy_id: str,
    scenario_id: str,
    policy: Mapping[str, Any],
    target_weights: pd.DataFrame,
    actual_weights: pd.DataFrame,
    path_rows: list[dict[str, Any]],
    static_baseline: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
    thresholds: Mapping[str, float],
    cost_constraint_policy: Mapping[str, Any],
) -> dict[str, Any]:
    cost_bps = _policy_cost_bps(policy)
    metrics = _performance_metrics(prices, actual_weights, cost_bps=cost_bps)
    target_metrics = _performance_metrics(prices, target_weights, cost_bps=0.0)
    returns = _portfolio_return_series(prices, actual_weights, cost_bps=cost_bps)
    capture = _capture_metrics(prices, returns)
    total_return = _total_return(returns)
    execution = _execution_metrics(path_rows, policy)
    false_risk_off_count, missed_upside_count = _risk_timing_counts(prices, path_rows)
    static_annual = _float(_mapping(static_baseline.get("performance")).get("annualized_return"))
    dynamic_vs_static_gap = round(_float(metrics.get("annual_return")) - static_annual, 6)
    materiality_threshold = _float(thresholds.get("execution_lag_return_cost_abs_pp"), 1.0) / 100.0
    cost_model = _mapping(policy.get("cost_model"))
    no_lookahead = _no_lookahead_evidence(path_rows)
    return {
        "scenario_id": scenario_id,
        "strategy_id": strategy_id,
        "execution_policy_id": str(policy.get("execution_policy_id")),
        "old_cadence_reference": scenario_id == "monthly_rebalance",
        "no_lookahead_evidence": no_lookahead,
        "performance": _performance_summary(metrics, total_return, capture, returns),
        "target_path_diagnostic": {
            "annualized_return": target_metrics.get("annual_return"),
            "max_drawdown": target_metrics.get("max_drawdown_daily_equity"),
            "sharpe": target_metrics.get("sharpe_daily_zero_rf"),
            "target_path_role": "diagnostic_only",
        },
        "execution": execution,
        "research_quality": {
            "dynamic_vs_static_gap": dynamic_vs_static_gap,
            "monthly_vs_event_driven_gap": 0.0,
            "weekly_vs_monthly_gap": 0.0,
            "turnover_adjusted_improvement": 0.0,
            "cost_adjusted_improvement": 0.0,
            "false_risk_off_count": false_risk_off_count,
            "missed_upside_count": missed_upside_count,
        },
        "benchmark_context": {
            "qqq_annualized_return": qqq_metrics.get("annualized_return"),
            "qqq_max_drawdown": qqq_metrics.get("max_drawdown"),
            "qqq_sharpe": qqq_metrics.get("sharpe"),
        },
        "cost_adjusted": {
            "transaction_cost_bps": cost_model.get("transaction_cost_bps"),
            "slippage_bps": cost_model.get("slippage_bps"),
            "cost_bps_total": cost_bps,
            "cost_drag": metrics.get("cost_drag"),
            "cost_adjusted_annualized_return": metrics.get("annual_return"),
        },
        "materiality": {
            "thresholds": dict(thresholds),
            "material_vs_static": abs(dynamic_vs_static_gap) >= materiality_threshold,
            "materiality_threshold_return_abs": round(materiality_threshold, 6),
        },
        "staleness_controls": {
            "staleness_filter_enabled": scenario_id == "valid_until_window",
            "expired_signal_suppression_rule": "suppress_rebalance"
            if scenario_id == "valid_until_window"
            else "not_applicable",
            "expired_signal_suppression_count": execution[
                "expired_signal_suppression_count"
            ],
            "stale_signal_execution_count": execution["stale_signal_execution_count"],
        },
        "cost_constraint_policy_id": cost_constraint_policy["policy_id"],
        "policy_hash": _stable_hash(policy),
    }


def _performance_summary(
    metrics: Mapping[str, Any],
    total_return: float,
    capture: Mapping[str, Any],
    returns: pd.Series,
) -> dict[str, Any]:
    return {
        "total_return": round(total_return, 6),
        "annualized_return": metrics.get("annual_return"),
        "max_drawdown": metrics.get("max_drawdown_daily_equity"),
        "sharpe": metrics.get("sharpe_daily_zero_rf"),
        "sortino": _sortino_ratio(returns, _float(metrics.get("annual_return"))),
        "volatility": metrics.get("volatility_daily_annualized"),
        "downside_capture": capture.get("downside_capture"),
        "upside_capture": capture.get("upside_capture"),
        "worst_month": metrics.get("worst_month"),
    }


def _execution_metrics(
    path_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    executed_indices = [
        index for index, row in enumerate(path_rows) if row.get("rebalance_executed") is True
    ]
    gaps = [
        current - previous
        for previous, current in zip(executed_indices, executed_indices[1:], strict=False)
    ]
    execution_lags = [
        _float(row.get("execution_lag_bdays"))
        for row in path_rows
        if row.get("rebalance_executed") is True
    ]
    max_turnover = _float(policy.get("max_turnover_per_period"), 1.0)
    drift_floor = _float(policy.get("drift_threshold"), 0.05)
    cooldown_days = max(0, _int(policy.get("cooldown_days"), 0))
    return {
        "rebalance_count": len(executed_indices),
        "turnover": round(sum(_float(row.get("turnover")) for row in path_rows), 6),
        "avg_holding_days": round(_mean(gaps), 3),
        "signal_to_execution_lag_days": round(_mean(execution_lags), 3),
        "missed_signal_count": sum(
            1
            for row in path_rows
            if row.get("rebalance_executed") is not True
            and _path_row_weight_gap(row) >= drift_floor
        ),
        "stale_signal_execution_count": sum(
            1
            for row in path_rows
            if row.get("rebalance_executed") is True and row.get("is_signal_stale") is True
        ),
        "expired_signal_suppression_count": sum(
            1 for row in path_rows if row.get("staleness_filter_suppressed") is True
        ),
        "cooldown_block_count": sum(
            1
            for row in path_rows
            if row.get("rebalance_executed") is not True
            and cooldown_days > 0
            and _int(row.get("signal_age_at_execution_days"), 999999) < cooldown_days
            and _path_row_weight_gap(row) >= drift_floor
        ),
        "constraint_hit_count": sum(
            1
            for row in path_rows
            if max_turnover > 0
            and row.get("rebalance_executed") is True
            and _float(row.get("turnover")) >= max_turnover - 0.000001
        ),
        "avg_signal_age_days": round(
            _mean(_float(row.get("signal_age_at_execution_days")) for row in path_rows),
            3,
        ),
    }


def _path_row_weight_gap(row: Mapping[str, Any]) -> float:
    gaps = [
        abs(
            _float(row.get(f"target_weight_{ticker.lower()}"))
            - _float(row.get(f"actual_weight_{ticker.lower()}"))
        )
        for ticker in ASSET_COLUMNS
    ]
    return max(gaps) if gaps else 0.0


def _risk_timing_counts(
    prices: pd.DataFrame,
    path_rows: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0).reset_index(drop=True)
    false_risk_off = 0
    missed_upside = 0
    for index, row in enumerate(path_rows):
        if index >= len(qqq_returns) or qqq_returns.iloc[index] <= 0:
            continue
        target_qqq = _float(row.get("target_weight_qqq"))
        actual_qqq = _float(row.get("actual_weight_qqq"))
        if target_qqq > actual_qqq:
            missed_upside += 1
            if row.get("rebalance_executed") is True:
                false_risk_off += 1
    return false_risk_off, missed_upside


def _portfolio_return_series(
    prices: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    cost_bps: float,
) -> pd.Series:
    frame = weights.reindex(columns=list(ASSET_COLUMNS)).astype(float).fillna(0.0)
    asset_returns = prices.reindex(columns=list(ASSET_COLUMNS)).pct_change().fillna(0.0)
    applied = frame.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
    gross_returns = (applied * asset_returns).sum(axis=1)
    turnover = frame.reindex(asset_returns.index).ffill().diff().abs().sum(axis=1).fillna(0.0) / 2.0
    return gross_returns - turnover * (cost_bps / 10000.0)


def _capture_metrics(prices: pd.DataFrame, returns: pd.Series) -> dict[str, float]:
    benchmark = prices["QQQ"].pct_change().fillna(0.0).reindex(returns.index).fillna(0.0)
    upside = benchmark > 0
    downside = benchmark < 0
    return {
        "upside_capture": _safe_ratio(
            float(returns.loc[upside].sum()),
            float(benchmark.loc[upside].sum()),
        ),
        "downside_capture": _safe_ratio(
            float(returns.loc[downside].sum()),
            float(benchmark.loc[downside].sum()),
        ),
    }


def _total_return(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return float((1.0 + returns.fillna(0.0)).prod() - 1.0)


def _sortino_ratio(returns: pd.Series, annual_return: float) -> float:
    downside = returns[returns < 0]
    if downside.empty:
        return 0.0
    downside_vol = float(downside.std(ddof=0) * math.sqrt(252))
    return round(_safe_ratio(float(annual_return), downside_vol), 6)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return round(numerator / denominator, 6)


def _mean(values: Any) -> float:
    items = [float(value) for value in values if value is not None]
    if not items:
        return 0.0
    return sum(items) / len(items)


def _benchmark_metrics(prices: pd.DataFrame) -> dict[str, Any]:
    qqq_weights = _constant_weight_frame(prices, {"QQQ": 1.0, "TQQQ": 0.0, "SGOV": 0.0})
    returns = _portfolio_return_series(prices, qqq_weights, cost_bps=0.0)
    metrics = _performance_metrics(prices, qqq_weights, cost_bps=0.0)
    return {
        "annualized_return": metrics.get("annual_return"),
        "max_drawdown": metrics.get("max_drawdown_daily_equity"),
        "sharpe": metrics.get("sharpe_daily_zero_rf"),
        "total_return": round(_total_return(returns), 6),
    }


def _no_lookahead_evidence(path_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    violations = []
    for row in path_rows:
        execution_date = row.get("actual_execution_date")
        if not execution_date:
            continue
        if row.get("trigger_reason") == "initial_position":
            continue
        signal_date = str(row.get("signal_observation_date") or row.get("signal_asof_date"))
        first_executable = str(row.get("first_executable_date") or execution_date)
        lag_days = _float(row.get("execution_lag_bdays"))
        lag_violation = (
            signal_date >= str(execution_date)
            if lag_days > 0
            else signal_date > str(execution_date)
        )
        if lag_violation:
            violations.append(
                {
                    "date": row.get("date"),
                    "signal_observation_date": signal_date,
                    "first_executable_date": first_executable,
                    "actual_execution_date": execution_date,
                }
            )
    return {
        "uses_future_data": bool(violations),
        "violation_count": len(violations),
        "sample_violations": violations[:5],
        "signal_to_execution_lag_days_mean": round(
            _mean(
                _float(row.get("execution_lag_bdays"))
                for row in path_rows
                if row.get("rebalance_executed") is True
            ),
            3,
        ),
        "decision_boundary": "after_close_signal_next_session_or_later_execution",
    }


def _comparison_matrix(
    scenario_rows: list[dict[str, Any]],
    *,
    turnover_penalty: float = 0.0,
) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in scenario_rows]
    monthly_by_strategy = {
        row["strategy_id"]: row
        for row in enriched
        if row["scenario_id"] == "monthly_rebalance"
    }
    event_by_strategy = {
        row["strategy_id"]: row
        for row in enriched
        if row["scenario_id"] == "signal_event_driven"
    }
    weekly_by_strategy = {
        row["strategy_id"]: row
        for row in enriched
        if row["scenario_id"] == "weekly_rebalance"
    }
    static_annual = _float(
        _mapping(enriched[0].get("performance")).get("annualized_return")
    ) if enriched else 0.0
    rows = []
    for row in enriched:
        strategy_id = row["strategy_id"]
        monthly = monthly_by_strategy.get(strategy_id)
        event = event_by_strategy.get(strategy_id)
        weekly = weekly_by_strategy.get(strategy_id)
        performance = _mapping(row.get("performance"))
        execution = _mapping(row.get("execution"))
        annual = _float(performance.get("annualized_return"))
        monthly_annual = _float(
            _mapping(monthly.get("performance") if monthly else {}).get(
                "annualized_return"
            )
        )
        event_annual = _float(
            _mapping(event.get("performance") if event else {}).get("annualized_return")
        )
        weekly_annual = _float(
            _mapping(weekly.get("performance") if weekly else {}).get("annualized_return")
        )
        research_quality = dict(_mapping(row.get("research_quality")))
        research_quality["dynamic_vs_static_gap"] = round(annual - static_annual, 6)
        research_quality["monthly_vs_event_driven_gap"] = round(event_annual - monthly_annual, 6)
        research_quality["weekly_vs_monthly_gap"] = round(weekly_annual - monthly_annual, 6)
        row["research_quality"] = research_quality
        rows.append(
            {
                "strategy_id": strategy_id,
                "scenario_id": row["scenario_id"],
                "execution_policy_id": row["execution_policy_id"],
                "old_cadence_reference": row.get("old_cadence_reference", False),
                "total_return": performance.get("total_return"),
                "annualized_return": annual,
                "max_drawdown": performance.get("max_drawdown"),
                "sharpe": performance.get("sharpe"),
                "sortino": performance.get("sortino"),
                "volatility": performance.get("volatility"),
                "upside_capture": performance.get("upside_capture"),
                "downside_capture": performance.get("downside_capture"),
                "rebalance_count": execution.get("rebalance_count"),
                "turnover": execution.get("turnover"),
                "avg_holding_days": execution.get("avg_holding_days"),
                "signal_to_execution_lag_days": execution.get(
                    "signal_to_execution_lag_days"
                ),
                "missed_signal_count": execution.get("missed_signal_count"),
                "stale_signal_execution_count": execution.get(
                    "stale_signal_execution_count"
                ),
                "cooldown_block_count": execution.get("cooldown_block_count"),
                "constraint_hit_count": execution.get("constraint_hit_count"),
                "dynamic_vs_static_gap": research_quality["dynamic_vs_static_gap"],
                "monthly_vs_event_driven_gap": research_quality[
                    "monthly_vs_event_driven_gap"
                ],
                "weekly_vs_monthly_gap": research_quality["weekly_vs_monthly_gap"],
                "turnover_adjusted_improvement": research_quality[
                    "turnover_adjusted_improvement"
                ],
                "cost_adjusted_improvement": research_quality[
                    "cost_adjusted_improvement"
                ],
                "false_risk_off_count": research_quality["false_risk_off_count"],
                "missed_upside_count": research_quality["missed_upside_count"],
                "uses_future_data": _mapping(row.get("no_lookahead_evidence")).get(
                    "uses_future_data"
                ),
            }
        )
    return _apply_gap_metrics(rows, turnover_penalty=turnover_penalty)


def _apply_gap_metrics(
    rows: list[dict[str, Any]],
    *,
    turnover_penalty: float = 0.0,
) -> list[dict[str, Any]]:
    monthly_by_strategy = {
        row["strategy_id"]: row for row in rows if row["scenario_id"] == "monthly_rebalance"
    }
    event_by_strategy = {
        row["strategy_id"]: row for row in rows if row["scenario_id"] == "signal_event_driven"
    }
    weekly_by_strategy = {
        row["strategy_id"]: row for row in rows if row["scenario_id"] == "weekly_rebalance"
    }
    for row in rows:
        strategy_id = row["strategy_id"]
        monthly = monthly_by_strategy.get(strategy_id, {})
        event = event_by_strategy.get(strategy_id, {})
        weekly = weekly_by_strategy.get(strategy_id, {})
        monthly_annual = _float(monthly.get("annualized_return"))
        row["monthly_vs_event_driven_gap"] = round(
            _float(event.get("annualized_return")) - monthly_annual,
            6,
        )
        row["weekly_vs_monthly_gap"] = round(
            _float(weekly.get("annualized_return")) - monthly_annual,
            6,
        )
        row["cost_adjusted_improvement"] = round(
            _float(row.get("annualized_return")) - monthly_annual,
            6,
        )
        row["turnover_adjusted_improvement"] = round(
            _float(row.get("cost_adjusted_improvement"))
            - max(0.0, _float(row.get("turnover")) - _float(monthly.get("turnover")))
            * turnover_penalty,
            6,
        )
    return rows


def _strategy_rankings(
    scenario_rows: list[dict[str, Any]],
    *,
    turnover_penalty: float = 0.0,
) -> list[dict[str, Any]]:
    rows = _comparison_matrix(scenario_rows, turnover_penalty=turnover_penalty)
    dynamic_rows = [row for row in rows if row["scenario_id"] != "static_baseline"]
    return sorted(
        dynamic_rows,
        key=lambda row: (
            _float(row.get("cost_adjusted_improvement")),
            _float(row.get("annualized_return")),
            -_float(row.get("turnover")),
        ),
        reverse=True,
    )


def _conclusions(
    *,
    scenario_rows: list[dict[str, Any]],
    thresholds: Mapping[str, float],
    cost_constraint_policy: Mapping[str, Any],
) -> dict[str, Any]:
    matrix = _comparison_matrix(
        scenario_rows,
        turnover_penalty=_float(cost_constraint_policy.get("turnover_penalty")),
    )
    threshold = _float(thresholds.get("execution_lag_return_cost_abs_pp"), 1.0) / 100.0
    monthly_rows = [row for row in matrix if row["scenario_id"] == "monthly_rebalance"]
    event_rows = [row for row in matrix if row["scenario_id"] == "signal_event_driven"]
    valid_rows = [row for row in matrix if row["scenario_id"] == "valid_until_window"]
    cooldown_rows = [
        row for row in matrix if row["scenario_id"] == "cooldown_limited_event_driven"
    ]
    non_monthly_rows = [
        row
        for row in matrix
        if row["scenario_id"]
        in {
            "weekly_rebalance",
            "daily_rebalance",
            "signal_event_driven",
            "valid_until_window",
            "cooldown_limited_event_driven",
        }
    ]
    max_gap = max(
        (
            abs(_float(row.get("annualized_return")) - _monthly_return(row, monthly_rows))
            for row in non_monthly_rows
        ),
        default=0.0,
    )
    monthly_distorts = max_gap >= threshold
    candidate_rows = event_rows + valid_rows + cooldown_rows
    best_candidate = max(
        candidate_rows,
        key=lambda row: (
            _float(row.get("annualized_return")),
            -_float(row.get("turnover")),
        ),
        default={},
    )
    daily_turnover = max(
        (_float(row.get("turnover")) for row in matrix if row["scenario_id"] == "daily_rebalance"),
        default=0.0,
    )
    cooldown_turnover = max((_float(row.get("turnover")) for row in cooldown_rows), default=0.0)
    old_results_need_retest = monthly_distorts or any(
        _float(row.get("missed_signal_count")) > 0
        or _float(row.get("stale_signal_execution_count")) > 0
        for row in monthly_rows
    )
    recommended_default = str(best_candidate.get("scenario_id") or "monthly_rebalance")
    if daily_turnover > cooldown_turnover and recommended_default == "daily_rebalance":
        recommended_default = "cooldown_limited_event_driven"
    event_or_valid_default = recommended_default in {
        "signal_event_driven",
        "valid_until_window",
        "cooldown_limited_event_driven",
    }
    return {
        "cadence_bias_detected": monthly_distorts,
        "monthly_rebalance_distorts_signal_response": "YES"
        if monthly_distorts
        else "NO_MATERIAL_BIAS_DETECTED",
        "event_driven_or_valid_until_should_be_default": "YES_OWNER_REVIEW_REQUIRED"
        if event_or_valid_default
        else "NO_KEEP_MONTHLY_AS_REFERENCE_PENDING_RETEST",
        "recommended_default_execution_cadence": recommended_default,
        "recommended_default_execution_policy_id": best_candidate.get("execution_policy_id"),
        "old_dynamic_strategy_results_need_retest": old_results_need_retest,
        "monthly_rebalance_gap_abs_max": round(max_gap, 6),
        "materiality_threshold_return_abs": round(threshold, 6),
        "daily_turnover_max": round(daily_turnover, 6),
        "cooldown_turnover_max": round(cooldown_turnover, 6),
        "cost_adjusted_improvement_survives_turnover": any(
            _float(row.get("cost_adjusted_improvement")) > threshold
            and _float(row.get("turnover"))
            <= _float(cost_constraint_policy.get("max_turnover_per_month"), 1.0)
            for row in candidate_rows
        ),
        "recommended_next_route": NEXT_ROUTE,
        "production_boundary": {
            "scheduler_enabled": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    }


def _monthly_return(row: Mapping[str, Any], monthly_rows: Sequence[Mapping[str, Any]]) -> float:
    strategy_id = row.get("strategy_id")
    for monthly in monthly_rows:
        if monthly.get("strategy_id") == strategy_id:
            return _float(monthly.get("annualized_return"))
    return 0.0


def _retest_next_steps(
    conclusions: Mapping[str, Any],
    strategy_rankings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    top_candidates = [
        {
            "strategy_id": row.get("strategy_id"),
            "scenario_id": row.get("scenario_id"),
            "execution_policy_id": row.get("execution_policy_id"),
            "annualized_return": row.get("annualized_return"),
            "turnover": row.get("turnover"),
            "cost_adjusted_improvement": row.get("cost_adjusted_improvement"),
        }
        for row in strategy_rankings[:10]
    ]
    return [
        {
            "step_id": NEXT_ROUTE,
            "priority": "P0",
            "status": "RECOMMENDED",
            "reason": (
                "monthly cadence bias audit 已完成；下一步进入 event-driven "
                "retest / candidate ranking"
            ),
            "recommended_default_execution_cadence": conclusions.get(
                "recommended_default_execution_cadence"
            ),
            "candidate_rows": top_candidates,
            "safety_boundary": {
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
        }
    ]


def _path_summary(
    row: Mapping[str, Any],
    path_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "strategy_id": row.get("strategy_id"),
        "scenario_id": row.get("scenario_id"),
        "execution_policy_id": row.get("execution_policy_id"),
        "row_count": len(path_rows),
        "sample_rows": list(path_rows[:5]),
        "path_schema_fields": sorted(path_rows[0].keys()) if path_rows else [],
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "execution_cadence_bias_audit.json"
    matrix_path = output_root / "execution_cadence_comparison_matrix.json"
    next_steps_path = output_root / "dynamic_strategy_retest_next_steps.json"
    markdown_path = docs_root / "dynamic_strategy_execution_cadence_bias_audit.md"
    matrix_doc_path = docs_root / "dynamic_strategy_execution_cadence_comparison_matrix.md"
    next_steps_doc_path = docs_root / "dynamic_strategy_retest_next_steps.md"

    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
        "comparison_matrix_json": str(matrix_path),
        "comparison_matrix_markdown": str(matrix_doc_path),
        "next_steps_json": str(next_steps_path),
        "next_steps_markdown": str(next_steps_doc_path),
    }
    _write_json(json_path, payload)
    _write_json(matrix_path, _matrix_payload(payload))
    _write_json(next_steps_path, _next_steps_payload(payload))
    markdown_path.write_text(_main_markdown(payload), encoding="utf-8")
    matrix_doc_path.write_text(_matrix_markdown(payload), encoding="utf-8")
    next_steps_doc_path.write_text(_next_steps_markdown(payload), encoding="utf-8")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _matrix_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_execution_cadence_comparison_matrix.v1",
        "report_type": "dynamic_strategy_execution_cadence_comparison_matrix",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "data_quality": payload.get("data_quality"),
        "comparison_matrix": payload.get("comparison_matrix", []),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
        "production_allowed": payload.get("production_allowed"),
    }


def _next_steps_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_retest_next_steps.v1",
        "report_type": "dynamic_strategy_retest_next_steps",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "conclusions": payload.get("conclusions", {}),
        "retest_next_steps": payload.get("retest_next_steps", []),
        "next_route": payload.get("next_route"),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
        "production_allowed": payload.get("production_allowed"),
    }


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    conclusions = _mapping(payload.get("conclusions"))
    artifact_paths = _mapping(payload.get("artifact_paths"))
    scenarios = ", ".join(str(item) for item in payload.get("scenarios_tested", []))
    data_quality_status = _mapping(payload.get("data_quality")).get("status")
    recommended_cadence = summary.get("recommended_default_execution_cadence")
    retest_required = summary.get("old_dynamic_strategy_results_need_retest")
    monthly_distortion = conclusions.get("monthly_rebalance_distorts_signal_response")
    default_candidate = conclusions.get("event_driven_or_valid_until_should_be_default")
    old_results_retest = conclusions.get("old_dynamic_strategy_results_need_retest")
    return "\n".join(
        [
            "# 动态策略执行节奏偏差审计",
            "",
            f"最后更新：{date.today().isoformat()}",
            "",
            "## 摘要",
            "",
            f"- status: `{payload.get('status')}`",
            f"- market_regime: `{payload.get('market_regime')}`",
            f"- data_quality_status: `{data_quality_status}`",
            f"- scenarios_tested: `{scenarios}`",
            f"- cadence_bias_detected: `{summary.get('cadence_bias_detected')}`",
            f"- recommended_default_execution_cadence: `{recommended_cadence}`",
            f"- old_dynamic_strategy_results_need_retest: `{retest_required}`",
            f"- next_route: `{payload.get('next_route')}`",
            "",
            "## 关键结论",
            "",
            f"- 月度 rebalance 是否扭曲信号响应：`{monthly_distortion}`",
            f"- Event-driven / valid-until 是否应成为默认候选：`{default_candidate}`",
            f"- 旧 dynamic strategy 结果是否需要重测：`{old_results_retest}`",
            f"- 推荐下一任务：`{conclusions.get('recommended_next_route')}`",
            "",
            "## 安全边界",
            "",
            "- 本报告只属于 strategy research / actual-path cadence audit。",
            (
                "- scheduler、event append、outcome binding、paper-shadow、production、"
                "broker/order 全部保持关闭。"
            ),
            (
                f"- broker_action: `{payload.get('broker_action')}`；"
                f"production_effect: `{payload.get('production_effect')}`。"
            ),
            "",
            "## 输出",
            "",
            f"- JSON: `{artifact_paths.get('json_path')}`",
            f"- Comparison matrix: `{artifact_paths.get('comparison_matrix_json')}`",
            f"- Retest next steps: `{artifact_paths.get('next_steps_json')}`",
        ]
    ) + "\n"


def _matrix_markdown(payload: Mapping[str, Any]) -> str:
    rows = list(payload.get("comparison_matrix", []))
    lines = [
        "# 动态策略执行节奏对比矩阵",
        "",
        f"最后更新：{date.today().isoformat()}",
        "",
        "|strategy_id|scenario_id|annualized_return|max_drawdown|turnover|rebalance_count|dynamic_vs_static_gap|monthly_vs_event_driven_gap|uses_future_data|",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:80]:
        lines.append(
            "|{strategy_id}|{scenario_id}|{annualized_return}|{max_drawdown}|{turnover}|{rebalance_count}|{dynamic_vs_static_gap}|{monthly_vs_event_driven_gap}|{uses_future_data}|".format(
                **{
                    "strategy_id": row.get("strategy_id"),
                    "scenario_id": row.get("scenario_id"),
                    "annualized_return": row.get("annualized_return"),
                    "max_drawdown": row.get("max_drawdown"),
                    "turnover": row.get("turnover"),
                    "rebalance_count": row.get("rebalance_count"),
                    "dynamic_vs_static_gap": row.get("dynamic_vs_static_gap"),
                    "monthly_vs_event_driven_gap": row.get(
                        "monthly_vs_event_driven_gap"
                    ),
                    "uses_future_data": row.get("uses_future_data"),
                }
            )
        )
    return "\n".join(lines) + "\n"


def _next_steps_markdown(payload: Mapping[str, Any]) -> str:
    steps = list(payload.get("retest_next_steps", []))
    conclusions = _mapping(payload.get("conclusions"))
    recommended_cadence = conclusions.get("recommended_default_execution_cadence")
    lines = [
        "# 动态策略重测后续步骤",
        "",
        f"最后更新：{date.today().isoformat()}",
        "",
        f"- recommended_next_route: `{payload.get('next_route')}`",
        f"- recommended_default_execution_cadence: `{recommended_cadence}`",
        "- promotion / paper-shadow / production / broker：owner review 前全部保持 blocked。",
        "",
        "|step_id|priority|status|reason|",
        "|---|---|---|---|",
    ]
    for step in steps:
        lines.append(
            f"|{step.get('step_id')}|{step.get('priority')}|{step.get('status')}|{step.get('reason')}|"
        )
    return "\n".join(lines) + "\n"
