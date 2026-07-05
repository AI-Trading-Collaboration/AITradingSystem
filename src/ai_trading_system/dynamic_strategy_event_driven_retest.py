from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    DEFAULT_DYNAMIC_AUDIT_STRATEGY_IDS,
    DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT,
    STATIC_BASELINE_STRATEGY_ID,
    _benchmark_metrics,
    _dedupe_strategy_ids,
    _portfolio_return_series,
    _scenario_metric_row,
    _scenario_policy,
    _signal_validity_profile,
    _static_baseline_row,
    _total_return,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    READY_STATUS as SOURCE_CADENCE_AUDIT_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    _cost_constraint_policy as _cadence_cost_constraint_policy,
)
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
)
from ai_trading_system.simple_baseline_portfolio_control import _data_quality_gate

TASK_ID = "TRADING-2365"
TASK_REGISTER_ID = (
    "TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING"
)
REPORT_TYPE = "dynamic_strategy_event_driven_retest"
SCHEMA_VERSION = "dynamic_strategy_event_driven_retest.v1"
READY_STATUS = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
BLOCKED_DATA_QUALITY_STATUS = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_BLOCKED_DATA_QUALITY"
BLOCKED_SOURCE_STATUS = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_BLOCKED_SOURCE_ARTIFACT"
SOURCE_TASK_ID = "TRADING-2364"
NEXT_ROUTE = "TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis"
PRIMARY_EXECUTION_CADENCE = "valid_until_window"

DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_CADENCE_AUDIT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT
    / "execution_cadence_bias_audit.json"
)

CADENCE_ORDER: tuple[str, ...] = (
    "static_baseline",
    "monthly_rebalance",
    "signal_event_driven",
    "valid_until_window",
    "cooldown_limited_event_driven",
)
DYNAMIC_CADENCE_ORDER: tuple[str, ...] = tuple(
    cadence for cadence in CADENCE_ORDER if cadence != "static_baseline"
)
SUPPORTED_DYNAMIC_STRATEGY_IDS: set[str] = set(DEFAULT_DYNAMIC_AUDIT_STRATEGY_IDS)

DECISION_ACCEPT = "ACCEPT_FOR_SHADOW_RESEARCH"
DECISION_CONTINUE = "CONTINUE_RESEARCH"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_REJECT = "REJECT_FOR_NOW"
DECISION_DEPRECATED = "DEPRECATED_BY_CADENCE_AUDIT"


def run_dynamic_strategy_event_driven_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_cadence_audit_path: Path = DEFAULT_SOURCE_CADENCE_AUDIT_PATH,
    output_root: Path = DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_DOCS_ROOT,
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
    source_cadence_audit = _load_source_cadence_audit(source_cadence_audit_path)
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
            status=BLOCKED_DATA_QUALITY_STATUS,
            as_of_date=as_of_date,
            start_date=resolved_start,
            end_date=end_date,
            data_quality=data_quality,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            policy_registry_path=policy_registry_path,
            source_cadence_audit=source_cadence_audit,
            cost_constraint_policy=cost_constraint_policy,
        )
        payload.update(_blocked_sections("data_quality_gate_failed"))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    if not bool(source_cadence_audit.get("ready_for_retest")):
        payload = _base_payload(
            status=BLOCKED_SOURCE_STATUS,
            as_of_date=as_of_date,
            start_date=resolved_start,
            end_date=end_date,
            data_quality=data_quality,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            policy_registry_path=policy_registry_path,
            source_cadence_audit=source_cadence_audit,
            cost_constraint_policy=cost_constraint_policy,
        )
        payload.update(_blocked_sections("source_cadence_audit_not_ready"))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    prices = _load_execution_price_matrix(
        prices_path,
        config,
        start_date=resolved_start,
        end_date=end_date,
    )
    policies = _policies_by_id(policy_registry)
    candidate_discovery = _candidate_discovery(policy_registry, strategy_ids)
    selected_strategy_ids = candidate_discovery["selected_strategy_ids"]

    static_row = _static_baseline_row(
        prices=prices,
        cost_constraint_policy=cost_constraint_policy,
        thresholds=thresholds,
    )
    _attach_static_cost_metrics(static_row)
    scenario_rows: list[dict[str, Any]] = [static_row]
    path_summaries: list[dict[str, Any]] = []
    qqq_metrics = _benchmark_metrics(prices)

    for strategy_id in selected_strategy_ids:
        target_weights = _signal_target_weight_frame(strategy_id, prices)
        for cadence in DYNAMIC_CADENCE_ORDER:
            policy = _scenario_policy(
                scenario_id=cadence,
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
                signal_validity_profile=_signal_validity_profile(cadence, policy),
                enable_staleness_filter=cadence == PRIMARY_EXECUTION_CADENCE,
                stale_action="suppress_rebalance"
                if cadence == PRIMARY_EXECUTION_CADENCE
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
                scenario_id=cadence,
                policy=policy,
                target_weights=target_weights,
                actual_weights=actual_weights,
                path_rows=path_rows,
                static_baseline=static_row,
                qqq_metrics=qqq_metrics,
                thresholds=thresholds,
                cost_constraint_policy=cost_constraint_policy,
            )
            _attach_cost_metrics(
                row=row,
                prices=prices,
                actual_weights=actual_weights,
                policy=policy,
                turnover_penalty=_float(cost_constraint_policy.get("turnover_penalty")),
            )
            scenario_rows.append(row)
            path_summaries.append(_path_summary(row, path_rows))

    cadence_comparison_matrix = _cadence_comparison_matrix(
        scenario_rows,
        turnover_penalty=_float(cost_constraint_policy.get("turnover_penalty")),
    )
    candidate_ranking = _candidate_ranking(
        cadence_comparison_matrix,
        thresholds=thresholds,
        cost_constraint_policy=cost_constraint_policy,
    )
    conclusions = _conclusions(
        source_cadence_audit=source_cadence_audit,
        candidate_ranking=candidate_ranking,
        cadence_comparison_matrix=cadence_comparison_matrix,
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
        source_cadence_audit=source_cadence_audit,
        cost_constraint_policy=cost_constraint_policy,
    )
    payload.update(
        {
            "summary": {
                "candidate_count": len(selected_strategy_ids),
                "scenario_count": len(scenario_rows),
                "cadences_tested": list(CADENCE_ORDER),
                "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
                "candidate_ranking_ready": True,
                "source_cadence_audit_confirmed": True,
                "recommended_next_research_task": NEXT_ROUTE,
                "data_quality_status": data_quality.get("status"),
                "top_candidate_id": conclusions.get("top_candidate_id"),
                "top_candidate_decision": conclusions.get("top_candidate_decision"),
            },
            "candidate_discovery": candidate_discovery,
            "dynamic_strategy_ids": selected_strategy_ids,
            "cadences_tested": list(CADENCE_ORDER),
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "static_baseline_strategy_id": STATIC_BASELINE_STRATEGY_ID,
            "scenario_rows": scenario_rows,
            "cadence_comparison_matrix": cadence_comparison_matrix,
            "candidate_ranking": candidate_ranking,
            "path_summaries": path_summaries,
            "materiality_thresholds": thresholds,
            "conclusions": conclusions,
            "monthly_results_deprecated_by_cadence_audit": [
                row
                for row in cadence_comparison_matrix
                if row.get("scenario_id") == "monthly_rebalance"
            ],
            "required_outputs_ready": {
                "event_driven_retest_result_json": True,
                "candidate_ranking_json": True,
                "cadence_comparison_matrix_json": True,
                "event_driven_retest_markdown": True,
                "candidate_ranking_markdown": True,
                "decision_summary_markdown": True,
                "next_route_markdown": True,
            },
            "backtest_run": True,
            "research_quality_status": "EVENT_DRIVEN_RETEST_READY_REQUIRES_2366_SENSITIVITY",
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
    source_cadence_audit: Mapping[str, Any],
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
        "source_cadence_audit": dict(source_cadence_audit),
        "source_task_id": SOURCE_TASK_ID,
        "cost_constraint_policy": dict(cost_constraint_policy),
        "next_route": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _blocked_sections(reason: str) -> dict[str, Any]:
    return {
        "summary": {
            "blocked_reason": reason,
            "candidate_count": 0,
            "scenario_count": 0,
            "cadences_tested": list(CADENCE_ORDER),
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "candidate_ranking_ready": False,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "candidate_discovery": {
            "selected_strategy_ids": [],
            "included_candidates": [],
            "excluded_candidates": [],
        },
        "dynamic_strategy_ids": [],
        "cadences_tested": list(CADENCE_ORDER),
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "scenario_rows": [],
        "cadence_comparison_matrix": [],
        "candidate_ranking": [],
        "path_summaries": [],
        "materiality_thresholds": {},
        "conclusions": {
            "blocked_reason": reason,
            "candidate_ranking_ready": False,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "monthly_results_deprecated_by_cadence_audit": [],
        "required_outputs_ready": {
            "event_driven_retest_result_json": True,
            "candidate_ranking_json": True,
            "cadence_comparison_matrix_json": True,
            "event_driven_retest_markdown": True,
            "candidate_ranking_markdown": True,
            "decision_summary_markdown": True,
            "next_route_markdown": True,
        },
        "backtest_run": False,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
    }


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
    policy = _cadence_cost_constraint_policy(
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
        turnover_penalty=turnover_penalty,
        max_turnover_per_month=max_turnover_per_month,
        min_holding_days=min_holding_days,
        cooldown_days=cooldown_days,
        max_single_step_weight_delta=max_single_step_weight_delta,
        risk_cap_enabled=risk_cap_enabled,
    )
    policy.update(
        {
            "policy_id": "dynamic_strategy_event_driven_retest_policy_v1",
            "rationale": (
                "Use TRADING-2364 cadence-bias evidence to rank dynamic strategy "
                "candidates under valid-until, event-driven and cooldown-limited "
                "actual-path execution."
            ),
            "intended_effect": (
                "Keep monthly results as deprecated reference rows while making "
                "cost, turnover, staleness and cooldown effects explicit before "
                "any owner review."
            ),
            "validation_evidence": (
                "TRADING-2365 focused tests, source TRADING-2364 artifact binding, "
                "and real data-quality-gated CLI run."
            ),
            "review_condition": (
                "Review together with TRADING-2366 cost, turnover and cooldown "
                "sensitivity before paper-shadow or production consideration."
            ),
        }
    )
    return policy


def _load_source_cadence_audit(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "ready_for_retest": False,
            "blocked_reason": "source_cadence_audit_missing",
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    summary = _mapping(payload.get("summary"))
    conclusions = _mapping(payload.get("conclusions"))
    recommended = str(
        summary.get("recommended_default_execution_cadence")
        or conclusions.get("recommended_default_execution_cadence")
        or ""
    )
    cadence_bias_detected = bool(
        summary.get("cadence_bias_detected")
        or conclusions.get("cadence_bias_detected")
    )
    old_results_need_retest = bool(
        summary.get("old_dynamic_strategy_results_need_retest")
        or conclusions.get("old_dynamic_strategy_results_need_retest")
    )
    ready_for_retest = (
        payload.get("status") == SOURCE_CADENCE_AUDIT_READY_STATUS
        and cadence_bias_detected
        and old_results_need_retest
        and recommended == PRIMARY_EXECUTION_CADENCE
    )
    return {
        "path": str(path),
        "exists": True,
        "source_task_id": payload.get("task_id"),
        "status": payload.get("status"),
        "schema_version": payload.get("schema_version"),
        "sha256": _file_sha256(path),
        "cadence_bias_detected": cadence_bias_detected,
        "old_dynamic_strategy_results_need_retest": old_results_need_retest,
        "recommended_default_execution_cadence": recommended,
        "monthly_rebalance_distorts_signal_response": conclusions.get(
            "monthly_rebalance_distorts_signal_response"
        ),
        "ready_for_retest": ready_for_retest,
        "blocked_reason": None if ready_for_retest else "source_cadence_audit_not_ready",
    }


def _candidate_discovery(
    policy_registry: Mapping[str, Any],
    strategy_ids: Sequence[str] | None,
) -> dict[str, Any]:
    policy_rows = list(policy_registry.get("strategy_execution_policies") or [])
    requested = _dedupe_strategy_ids(strategy_ids) if strategy_ids else []
    if requested:
        selected_source = requested
    else:
        active_registry_ids = [
            str(row.get("strategy_id"))
            for row in policy_rows
            if row.get("strategy_type") == "dynamic"
            and str(row.get("policy_status")) == "active"
        ]
        selected_source = _dedupe_strategy_ids(active_registry_ids)
    selected: list[str] = []
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    row_by_strategy = {str(row.get("strategy_id")): row for row in policy_rows}
    for strategy_id in selected_source:
        row = _mapping(row_by_strategy.get(strategy_id))
        if strategy_id not in SUPPORTED_DYNAMIC_STRATEGY_IDS:
            excluded.append(
                {
                    "strategy_id": strategy_id,
                    "policy_status": row.get("policy_status"),
                    "reason": "unsupported_signal_target_builder",
                }
            )
            continue
        if strategy_id not in selected:
            selected.append(strategy_id)
            included.append(
                {
                    "strategy_id": strategy_id,
                    "policy_status": row.get("policy_status") or "requested",
                    "execution_policy_id": row.get("execution_policy_id"),
                    "selection_reason": "requested" if requested else "active_registry",
                }
            )
    for row in policy_rows:
        strategy_id = str(row.get("strategy_id"))
        if (
            row.get("strategy_type") == "dynamic"
            and strategy_id not in selected
            and strategy_id not in selected_source
        ):
            excluded.append(
                {
                    "strategy_id": strategy_id,
                    "policy_status": row.get("policy_status"),
                    "execution_policy_id": row.get("execution_policy_id"),
                    "reason": "non_active_or_watch_only_candidate",
                }
            )
    if not selected:
        selected = list(DEFAULT_DYNAMIC_AUDIT_STRATEGY_IDS)
        included = [
            {
                "strategy_id": strategy_id,
                "policy_status": "fallback_active_coverage_floor",
                "selection_reason": "default_dynamic_audit_coverage_floor",
            }
            for strategy_id in selected
        ]
    return {
        "selected_strategy_ids": selected,
        "included_candidates": included,
        "excluded_candidates": excluded,
        "candidate_source": "explicit_strategy_option" if requested else "active_registry",
        "supported_strategy_ids": sorted(SUPPORTED_DYNAMIC_STRATEGY_IDS),
    }


def _attach_static_cost_metrics(row: dict[str, Any]) -> None:
    performance = _mapping(row.get("performance"))
    row["cost_metrics"] = {
        "transaction_cost_bps": 0.0,
        "slippage_bps": 0.0,
        "cost_bps_total": 0.0,
        "gross_return": performance.get("total_return"),
        "gross_annualized_return": performance.get("annualized_return"),
        "cost_adjusted_return": performance.get("annualized_return"),
        "turnover_adjusted_score": performance.get("annualized_return"),
        "cost_drag": 0.0,
    }


def _attach_cost_metrics(
    *,
    row: dict[str, Any],
    prices: pd.DataFrame,
    actual_weights: pd.DataFrame,
    policy: Mapping[str, Any],
    turnover_penalty: float,
) -> None:
    cost_model = _mapping(policy.get("cost_model"))
    gross_metrics = _performance_metrics(prices, actual_weights, cost_bps=0.0)
    gross_returns = _portfolio_return_series(prices, actual_weights, cost_bps=0.0)
    performance = _mapping(row.get("performance"))
    execution = _mapping(row.get("execution"))
    cost_adjusted = _mapping(row.get("cost_adjusted"))
    cost_adjusted_return = _float(performance.get("annualized_return"))
    turnover_adjusted_score = cost_adjusted_return - (
        _float(execution.get("turnover")) * turnover_penalty
    )
    row["cost_metrics"] = {
        "transaction_cost_bps": cost_model.get("transaction_cost_bps"),
        "slippage_bps": cost_model.get("slippage_bps"),
        "cost_bps_total": _policy_cost_bps(policy),
        "gross_return": round(_total_return(gross_returns), 6),
        "gross_annualized_return": gross_metrics.get("annual_return"),
        "cost_adjusted_return": round(cost_adjusted_return, 6),
        "turnover_adjusted_score": round(turnover_adjusted_score, 6),
        "cost_drag": cost_adjusted.get("cost_drag"),
    }


def _cadence_comparison_matrix(
    scenario_rows: list[dict[str, Any]],
    *,
    turnover_penalty: float,
) -> list[dict[str, Any]]:
    static_row = next(
        row for row in scenario_rows if row.get("scenario_id") == "static_baseline"
    )
    static_performance = _mapping(static_row.get("performance"))
    static_total = _float(static_performance.get("total_return"))
    static_annual = _float(static_performance.get("annualized_return"))
    static_drawdown = _float(static_performance.get("max_drawdown"))
    rows_by_strategy = _rows_by_strategy_and_cadence(scenario_rows)
    matrix: list[dict[str, Any]] = []
    for row in scenario_rows:
        strategy_id = str(row.get("strategy_id"))
        cadence = str(row.get("scenario_id"))
        performance = _mapping(row.get("performance"))
        execution = _mapping(row.get("execution"))
        research_quality = _mapping(row.get("research_quality"))
        cost_metrics = _mapping(row.get("cost_metrics"))
        strategy_rows = rows_by_strategy.get(strategy_id, {})
        monthly = _matrix_ref(strategy_rows.get("monthly_rebalance"))
        event = _matrix_ref(strategy_rows.get("signal_event_driven"))
        valid_until = _matrix_ref(strategy_rows.get(PRIMARY_EXECUTION_CADENCE))
        cooldown = _matrix_ref(strategy_rows.get("cooldown_limited_event_driven"))
        annual = _float(performance.get("annualized_return"))
        turnover_adjusted_score = _float(cost_metrics.get("turnover_adjusted_score"))
        if "turnover_adjusted_score" not in cost_metrics:
            turnover_adjusted_score = annual - _float(execution.get("turnover")) * turnover_penalty
        matrix.append(
            {
                "candidate_id": None if cadence == "static_baseline" else strategy_id,
                "strategy_id": strategy_id,
                "scenario_id": cadence,
                "execution_policy_id": row.get("execution_policy_id"),
                "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
                "old_cadence_reference": cadence == "monthly_rebalance",
                "deprecated_by_cadence_audit": cadence == "monthly_rebalance",
                "monthly_result_role": "deprecated_reference_only"
                if cadence == "monthly_rebalance"
                else "not_monthly_reference",
                "total_return": performance.get("total_return"),
                "annualized_return": performance.get("annualized_return"),
                "max_drawdown": performance.get("max_drawdown"),
                "volatility": performance.get("volatility"),
                "sharpe": performance.get("sharpe"),
                "sortino": performance.get("sortino"),
                "downside_capture": performance.get("downside_capture"),
                "upside_capture": performance.get("upside_capture"),
                "rebalance_count": execution.get("rebalance_count"),
                "turnover": execution.get("turnover"),
                "average_holding_days": execution.get("avg_holding_days"),
                "signal_to_execution_lag_days": execution.get(
                    "signal_to_execution_lag_days"
                ),
                "stale_signal_execution_count": execution.get(
                    "stale_signal_execution_count"
                ),
                "stale_signal_count": execution.get("stale_signal_execution_count"),
                "missed_signal_count": execution.get("missed_signal_count"),
                "cooldown_block_count": execution.get("cooldown_block_count"),
                "constraint_hit_count": execution.get("constraint_hit_count"),
                "transaction_cost_bps": cost_metrics.get("transaction_cost_bps"),
                "slippage_bps": cost_metrics.get("slippage_bps"),
                "gross_return": cost_metrics.get("gross_return"),
                "gross_annualized_return": cost_metrics.get("gross_annualized_return"),
                "cost_adjusted_return": cost_metrics.get("cost_adjusted_return"),
                "turnover_adjusted_score": round(turnover_adjusted_score, 6),
                "dynamic_vs_static_total_return_gap": round(
                    _float(performance.get("total_return")) - static_total,
                    6,
                ),
                "dynamic_vs_static_annualized_return_gap": round(annual - static_annual, 6),
                "dynamic_vs_static_drawdown_gap": round(
                    _float(performance.get("max_drawdown")) - static_drawdown,
                    6,
                ),
                "valid_until_vs_monthly_gap": round(
                    valid_until["annualized_return"] - monthly["annualized_return"],
                    6,
                ),
                "event_driven_vs_monthly_gap": round(
                    event["annualized_return"] - monthly["annualized_return"],
                    6,
                ),
                "cooldown_vs_event_driven_gap": round(
                    cooldown["annualized_return"] - event["annualized_return"],
                    6,
                ),
                "false_risk_off_count": research_quality.get("false_risk_off_count"),
                "missed_upside_count": research_quality.get("missed_upside_count"),
                "uses_future_data": _mapping(row.get("no_lookahead_evidence")).get(
                    "uses_future_data"
                ),
            }
        )
    return matrix


def _rows_by_strategy_and_cadence(
    scenario_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Mapping[str, Any]]]:
    result: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in scenario_rows:
        result.setdefault(str(row.get("strategy_id")), {})[str(row.get("scenario_id"))] = row
    return result


def _matrix_ref(row: Mapping[str, Any] | None) -> dict[str, float]:
    performance = _mapping(row.get("performance") if row else {})
    return {
        "annualized_return": _float(performance.get("annualized_return")),
        "total_return": _float(performance.get("total_return")),
    }


def _candidate_ranking(
    matrix: Sequence[Mapping[str, Any]],
    *,
    thresholds: Mapping[str, float],
    cost_constraint_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    static_annual = next(
        (
            _float(row.get("annualized_return"))
            for row in matrix
            if row.get("scenario_id") == "static_baseline"
        ),
        0.0,
    )
    primary_rows = [
        row for row in matrix if row.get("scenario_id") == PRIMARY_EXECUTION_CADENCE
    ]
    threshold = _float(thresholds.get("execution_lag_return_cost_abs_pp"), 1.0) / 100.0
    max_turnover = _float(cost_constraint_policy.get("max_turnover_per_month"), 1.0)
    ranking: list[dict[str, Any]] = []
    for row in primary_rows:
        annual = _float(row.get("annualized_return"))
        dynamic_gap = round(annual - static_annual, 6)
        high_turnover = (
            _float(row.get("turnover")) > max_turnover
            or _int(row.get("constraint_hit_count")) > 0
        )
        survives_cost = dynamic_gap >= threshold
        cooldown_gap = _float(row.get("cooldown_vs_event_driven_gap"))
        cooldown_survives = cooldown_gap >= -threshold
        decision, reason = _decision(
            dynamic_gap=dynamic_gap,
            threshold=threshold,
            high_turnover=high_turnover,
            survives_cost=survives_cost,
            cooldown_survives=cooldown_survives,
            uses_future_data=bool(row.get("uses_future_data")),
        )
        ranking.append(
            {
                "rank": 0,
                "candidate_id": row.get("candidate_id"),
                "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
                "cost_adjusted_return": row.get("cost_adjusted_return"),
                "max_drawdown": row.get("max_drawdown"),
                "turnover": row.get("turnover"),
                "turnover_adjusted_score": row.get("turnover_adjusted_score"),
                "dynamic_vs_static_gap": dynamic_gap,
                "valid_until_vs_monthly_gap": row.get("valid_until_vs_monthly_gap"),
                "event_driven_vs_monthly_gap": row.get("event_driven_vs_monthly_gap"),
                "cooldown_vs_event_driven_gap": row.get("cooldown_vs_event_driven_gap"),
                "stale_signal_count": row.get("stale_signal_count"),
                "false_risk_off_count": row.get("false_risk_off_count"),
                "missed_upside_count": row.get("missed_upside_count"),
                "missed_signal_count": row.get("missed_signal_count"),
                "cooldown_block_count": row.get("cooldown_block_count"),
                "constraint_hit_count": row.get("constraint_hit_count"),
                "rebalance_count": row.get("rebalance_count"),
                "signal_to_execution_lag_days": row.get("signal_to_execution_lag_days"),
                "survives_cost_adjustment": survives_cost,
                "relies_on_high_turnover": high_turnover,
                "survives_cooldown_constraints": cooldown_survives,
                "decision": decision,
                "decision_reason": reason,
            }
        )
    ranking.sort(
        key=lambda row: (
            _decision_sort_value(str(row.get("decision"))),
            _float(row.get("turnover_adjusted_score")),
            _float(row.get("cost_adjusted_return")),
            _float(row.get("dynamic_vs_static_gap")),
            -_float(row.get("turnover")),
        ),
        reverse=True,
    )
    for index, row in enumerate(ranking, start=1):
        row["rank"] = index
    return ranking


def _decision(
    *,
    dynamic_gap: float,
    threshold: float,
    high_turnover: bool,
    survives_cost: bool,
    cooldown_survives: bool,
    uses_future_data: bool,
) -> tuple[str, str]:
    if uses_future_data:
        return DECISION_OWNER_REVIEW, "no-lookahead evidence 未通过，需要 owner review。"
    if dynamic_gap <= 0:
        return DECISION_REJECT, "valid-until 结果未跑赢 static baseline。"
    if survives_cost and not high_turnover and cooldown_survives:
        return (
            DECISION_ACCEPT,
            "valid-until edge 通过 cost、turnover 和 cooldown research screen。",
        )
    if survives_cost and high_turnover:
        return (
            DECISION_OWNER_REVIEW,
            "valid-until edge 通过 cost screen，但依赖高 turnover 或 constraint hits。",
        )
    if dynamic_gap > 0 and dynamic_gap < threshold:
        return (
            DECISION_CONTINUE,
            "valid-until 结果为正，但低于 governed materiality threshold。",
        )
    if not cooldown_survives:
        return (
            DECISION_CONTINUE,
            "valid-until 结果需要 cooldown sensitivity 后再进入 owner decision。",
        )
    return DECISION_REJECT, "candidate 未通过当前 research screen。"


def _decision_sort_value(decision: str) -> int:
    return {
        DECISION_ACCEPT: 4,
        DECISION_OWNER_REVIEW: 3,
        DECISION_CONTINUE: 2,
        DECISION_REJECT: 1,
        DECISION_DEPRECATED: 0,
    }.get(decision, 0)


def _conclusions(
    *,
    source_cadence_audit: Mapping[str, Any],
    candidate_ranking: Sequence[Mapping[str, Any]],
    cadence_comparison_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    top = candidate_ranking[0] if candidate_ranking else {}
    deprecated = [
        {
            "candidate_id": row.get("candidate_id"),
            "scenario_id": row.get("scenario_id"),
            "decision": DECISION_DEPRECATED,
            "reason": "monthly cadence is retained only as deprecated reference evidence.",
        }
        for row in cadence_comparison_matrix
        if row.get("scenario_id") == "monthly_rebalance"
    ]
    return {
        "candidate_ranking_ready": bool(candidate_ranking),
        "source_cadence_audit_confirmed": bool(source_cadence_audit.get("ready_for_retest")),
        "cadence_bias_detected": source_cadence_audit.get("cadence_bias_detected"),
        "old_dynamic_strategy_results_need_retest": source_cadence_audit.get(
            "old_dynamic_strategy_results_need_retest"
        ),
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "valid_until_should_be_default_dynamic_backtest_cadence": (
            "YES_OWNER_REVIEW_REQUIRED"
        ),
        "monthly_results_deprecated": True,
        "deprecated_old_results": deprecated,
        "top_candidate_id": top.get("candidate_id"),
        "top_candidate_decision": top.get("decision"),
        "top_candidate_turnover_adjusted_score": top.get("turnover_adjusted_score"),
        "top_candidate_cost_adjusted_return": top.get("cost_adjusted_return"),
        "any_candidate_survives_cost_adjustment": any(
            bool(row.get("survives_cost_adjustment")) for row in candidate_ranking
        ),
        "any_candidate_relies_on_high_turnover": any(
            bool(row.get("relies_on_high_turnover")) for row in candidate_ranking
        ),
        "any_candidate_survives_cooldown_constraints": any(
            bool(row.get("survives_cooldown_constraints")) for row in candidate_ranking
        ),
        "cost_turnover_cooldown_sensitivity_required": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "production_boundary": {
            "scheduler_enabled": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    }


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
    result_path = output_root / "event_driven_retest_result.json"
    ranking_path = output_root / "candidate_ranking.json"
    matrix_path = output_root / "cadence_comparison_matrix.json"
    retest_doc_path = docs_root / "dynamic_strategy_event_driven_retest.md"
    ranking_doc_path = docs_root / "dynamic_strategy_candidate_ranking.md"
    decision_doc_path = docs_root / "dynamic_strategy_retest_decision_summary.md"
    route_doc_path = docs_root / "dynamic_strategy_2366_route.md"
    payload["artifact_paths"] = {
        "json_path": str(result_path),
        "candidate_ranking_json": str(ranking_path),
        "cadence_comparison_matrix_json": str(matrix_path),
        "markdown_path": str(retest_doc_path),
        "candidate_ranking_markdown": str(ranking_doc_path),
        "decision_summary_markdown": str(decision_doc_path),
        "next_route_markdown": str(route_doc_path),
    }
    _write_json(result_path, payload)
    _write_json(ranking_path, _ranking_payload(payload))
    _write_json(matrix_path, _matrix_payload(payload))
    retest_doc_path.write_text(_retest_markdown(payload), encoding="utf-8")
    ranking_doc_path.write_text(_ranking_markdown(payload), encoding="utf-8")
    decision_doc_path.write_text(_decision_markdown(payload), encoding="utf-8")
    route_doc_path.write_text(_route_markdown(payload), encoding="utf-8")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _ranking_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_candidate_ranking.v1",
        "report_type": "dynamic_strategy_candidate_ranking",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "primary_execution_cadence": payload.get("primary_execution_cadence"),
        "candidate_ranking": payload.get("candidate_ranking", []),
        "conclusions": payload.get("conclusions", {}),
        "source_cadence_audit": payload.get("source_cadence_audit", {}),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
        "production_allowed": payload.get("production_allowed"),
    }


def _matrix_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_cadence_comparison_matrix.v1",
        "report_type": "dynamic_strategy_cadence_comparison_matrix",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "primary_execution_cadence": payload.get("primary_execution_cadence"),
        "cadence_comparison_matrix": payload.get("cadence_comparison_matrix", []),
        "source_cadence_audit": payload.get("source_cadence_audit", {}),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
        "production_allowed": payload.get("production_allowed"),
    }


def _retest_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    source = _mapping(payload.get("source_cadence_audit"))
    date_range = _mapping(payload.get("requested_date_range"))
    requested_range = f"{date_range.get('start')} to {date_range.get('end')}"
    return "\n".join(
        [
            "# 动态策略事件驱动重测",
            "",
            f"- status: `{payload.get('status')}`",
            f"- task: `{TASK_REGISTER_ID}`",
            f"- market regime：`{payload.get('market_regime')}`",
            f"- requested date range：`{requested_range}`",
            f"- data quality：`{_mapping(payload.get('data_quality')).get('status')}`",
            f"- TRADING-2364 source ready：`{source.get('ready_for_retest')}`",
            f"- primary execution cadence：`{summary.get('primary_execution_cadence')}`",
            f"- candidates tested：`{summary.get('candidate_count')}`",
            f"- next route：`{NEXT_ROUTE}`",
            "",
            "## 安全边界",
            "",
            "- scheduler、event、outcome、paper-shadow、production、broker action 均保持关闭。",
            "- monthly cadence rows 只保留为 deprecated reference evidence。",
        ]
    )


def _ranking_markdown(payload: Mapping[str, Any]) -> str:
    rows = list(payload.get("candidate_ranking") or [])
    lines = [
        "# 动态策略候选排名",
        "",
        "|rank|candidate|decision|cost_adjusted_return|turnover|valid_until_vs_monthly_gap|reason|",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows[:20]:
        lines.append(
            "|{rank}|`{candidate}`|`{decision}`|{ret}|{turnover}|{gap}|{reason}|".format(
                rank=row.get("rank"),
                candidate=row.get("candidate_id"),
                decision=row.get("decision"),
                ret=_fmt(row.get("cost_adjusted_return")),
                turnover=_fmt(row.get("turnover")),
                gap=_fmt(row.get("valid_until_vs_monthly_gap")),
                reason=str(row.get("decision_reason") or "").replace("|", "/"),
            )
        )
    if not rows:
        lines.append("|n/a|n/a|`BLOCKED`|n/a|n/a|n/a|ranking not available|")
    return "\n".join(lines)


def _decision_markdown(payload: Mapping[str, Any]) -> str:
    conclusions = _mapping(payload.get("conclusions"))
    survives_cost = conclusions.get("any_candidate_survives_cost_adjustment")
    relies_turnover = conclusions.get("any_candidate_relies_on_high_turnover")
    sensitivity_required = conclusions.get("cost_turnover_cooldown_sensitivity_required")
    valid_until_default = conclusions.get(
        "valid_until_should_be_default_dynamic_backtest_cadence"
    )
    return "\n".join(
        [
            "# 动态策略重测决策摘要",
            "",
            f"- candidate ranking ready：`{conclusions.get('candidate_ranking_ready')}`",
            f"- top candidate：`{conclusions.get('top_candidate_id')}`",
            f"- top candidate decision：`{conclusions.get('top_candidate_decision')}`",
            f"- any candidate survives cost adjustment：`{survives_cost}`",
            f"- any candidate relies on high turnover：`{relies_turnover}`",
            f"- cooldown sensitivity required：`{sensitivity_required}`",
            f"- monthly results deprecated：`{conclusions.get('monthly_results_deprecated')}`",
            f"- valid-until default decision：`{valid_until_default}`",
            "",
            (
                "Owner review 只能把本产物作为 research evidence；本产物不批准 "
                "paper-shadow、production、broker/order 或 scheduler enablement。"
            ),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2366 路由",
            "",
            f"- current task status：`{payload.get('status')}`",
            f"- next route：`{NEXT_ROUTE}`",
            (
                "- route reason：owner decision 前必须继续测试 cost、turnover 和 "
                "cooldown sensitivity。"
            ),
            "- production boundary：`none`；broker action：`none`。",
        ]
    )


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return str(value)
