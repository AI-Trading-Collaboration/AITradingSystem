from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    DECISION_ACCEPT,
    DECISION_CONTINUE,
    DECISION_DEPRECATED,
    DECISION_OWNER_REVIEW,
    DECISION_REJECT,
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT,
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_RETEST_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
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

TASK_ID = "TRADING-2366"
TASK_REGISTER_ID = (
    "TRADING-2366_DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_ANALYSIS"
)
REPORT_TYPE = "dynamic_strategy_cost_turnover_cooldown_sensitivity"
SCHEMA_VERSION = "dynamic_strategy_cost_turnover_cooldown_sensitivity.v1"
READY_STATUS = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_BLOCKED_SOURCE_ARTIFACT"
)
SOURCE_TASK_ID = "TRADING-2365"
NEXT_ROUTE = "TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_Shadow_Research_Gate"
SOURCE_EXPECTED_NEXT_ROUTE = (
    "TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis"
)
CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID = "dynamic_regime_overlay_v0_4_lower_turnover"

DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT
    / "event_driven_retest_result.json"
)
DEFAULT_SOURCE_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT / "candidate_ranking.json"
)
DEFAULT_SOURCE_CADENCE_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT
    / "cadence_comparison_matrix.json"
)

SENSITIVITY_CADENCES: tuple[str, ...] = (
    PRIMARY_EXECUTION_CADENCE,
    "cooldown_limited_event_driven",
)
COMBINED_SCENARIO_IDS: tuple[str, ...] = (
    "combined_base",
    "combined_realistic",
    "combined_conservative",
    "combined_harsh",
)


def run_dynamic_strategy_cost_turnover_cooldown_sensitivity(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Path = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Path = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    ),
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_DOCS_ROOT,
    strategy_ids: Sequence[str] | None = None,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    turnover_penalty: float = 0.0,
    risk_cap_enabled: bool = True,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    source_retest = _load_source_retest(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
    )
    config = _load_registry(simple_config_path)
    data_quality = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=sorted({"QQQ", "TQQQ", "SGOV"}),
    )
    policy_registry = _load_policy_registry(policy_registry_path)
    thresholds = _execution_materiality_thresholds(policy_registry)
    sensitivity_policy = _sensitivity_policy(
        turnover_penalty=turnover_penalty,
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
            source_retest=source_retest,
            sensitivity_policy=sensitivity_policy,
        )
        payload.update(_blocked_sections("data_quality_gate_failed"))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload
    if not bool(source_retest.get("ready_for_sensitivity")):
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
            source_retest=source_retest,
            sensitivity_policy=sensitivity_policy,
        )
        payload.update(_blocked_sections("source_event_driven_retest_not_ready"))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    prices = _load_execution_price_matrix(
        prices_path,
        config,
        start_date=resolved_start,
        end_date=end_date,
    )
    policies = _policies_by_id(policy_registry)
    selected_strategy_ids = _select_sensitivity_candidates(source_retest, strategy_ids)
    sensitivity_grid = _sensitivity_grid()
    static_row = _static_baseline_row(
        prices=prices,
        cost_constraint_policy=sensitivity_policy,
        thresholds=thresholds,
    )
    static_performance = _mapping(static_row.get("performance"))
    qqq_metrics = _benchmark_metrics(prices)
    target_by_strategy = {
        strategy_id: _signal_target_weight_frame(strategy_id, prices)
        for strategy_id in selected_strategy_ids
    }

    sensitivity_rows: list[dict[str, Any]] = []
    path_summaries: list[dict[str, Any]] = []
    for scenario in sensitivity_grid["scenarios"]:
        sensitivity_rows.append(
            _static_matrix_row(
                scenario=scenario,
                static_row=static_row,
                static_performance=static_performance,
            )
        )
        for strategy_id in selected_strategy_ids:
            target_weights = target_by_strategy[strategy_id]
            for cadence in SENSITIVITY_CADENCES:
                policy = _scenario_policy_for_sensitivity(
                    cadence=cadence,
                    scenario=scenario,
                    policies=policies,
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
                    cost_constraint_policy=sensitivity_policy,
                )
                matrix_row = _sensitivity_matrix_row(
                    scenario=scenario,
                    cadence=cadence,
                    row=row,
                    prices=prices,
                    actual_weights=actual_weights,
                    policy=policy,
                    static_performance=static_performance,
                    turnover_penalty=_float(sensitivity_policy.get("turnover_penalty")),
                )
                sensitivity_rows.append(matrix_row)
                if scenario["scenario_id"] in COMBINED_SCENARIO_IDS:
                    path_summaries.append(_path_summary(matrix_row, path_rows))

    _apply_relative_sensitivity_metrics(sensitivity_rows)
    robustness_ranking = _robustness_ranking(
        sensitivity_rows=sensitivity_rows,
        source_retest=source_retest,
        thresholds=thresholds,
    )
    decision_update = _decision_update_payload(
        source_retest=source_retest,
        robustness_ranking=robustness_ranking,
        sensitivity_rows=sensitivity_rows,
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
        source_retest=source_retest,
        sensitivity_policy=sensitivity_policy,
    )
    payload.update(
        {
            "summary": {
                "sensitivity_analysis_ready": True,
                "sensitivity_grid_ready": True,
                "candidate_count": len(selected_strategy_ids),
                "sensitivity_scenario_count": len(sensitivity_grid["scenarios"]),
                "matrix_row_count": len(sensitivity_rows),
                "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
                "top_candidate_from_2365": source_retest.get("top_candidate_from_2365"),
                "top_candidate_decision_from_2365": source_retest.get(
                    "top_candidate_decision_from_2365"
                ),
                "top_candidate_after_sensitivity": decision_update.get(
                    "top_candidate_after_sensitivity"
                ),
                "top_candidate_decision_after_sensitivity": decision_update.get(
                    "top_candidate_decision_after_sensitivity"
                ),
                "recommended_next_research_task": NEXT_ROUTE,
                "data_quality_status": data_quality.get("status"),
            },
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "top_candidate_from_2365": source_retest.get("top_candidate_from_2365"),
            "top_candidate_decision_from_2365": source_retest.get(
                "top_candidate_decision_from_2365"
            ),
            "candidate_selection": {
                "selected_strategy_ids": selected_strategy_ids,
                "required_candidate_roles": _candidate_roles(source_retest),
                "static_baseline_candidate_id": STATIC_BASELINE_STRATEGY_ID,
            },
            "sensitivity_grid": sensitivity_grid,
            "sensitivity_matrix": sensitivity_rows,
            "robustness_ranking": robustness_ranking,
            "decision_update": decision_update,
            "path_summaries": path_summaries,
            "required_outputs_ready": {
                "sensitivity_grid": True,
                "cost_adjusted_metrics": True,
                "turnover_metrics": True,
                "cooldown_metrics": True,
                "combined_stress_results": True,
                "ranking_after_sensitivity": True,
                "decision_update": True,
                "top_candidate_after_sensitivity": bool(
                    decision_update.get("top_candidate_after_sensitivity")
                ),
                "top_candidate_decision_after_sensitivity": bool(
                    decision_update.get("top_candidate_decision_after_sensitivity")
                ),
                "recommended_next_research_task": True,
            },
            "summary_findings": decision_update.get("summary_findings", {}),
            "sensitivity_analysis_ready": True,
            "sensitivity_grid_ready": True,
            "cost_adjusted_metrics_ready": True,
            "turnover_metrics_ready": True,
            "cooldown_metrics_ready": True,
            "decision_update_ready": True,
            "backtest_run": True,
            "research_quality_status": (
                "COST_TURNOVER_COOLDOWN_SENSITIVITY_READY_REQUIRES_2367_OWNER_GATE"
            ),
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
    source_retest: Mapping[str, Any],
    sensitivity_policy: Mapping[str, Any],
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
        "source_retest": dict(source_retest),
        "source_task_id": SOURCE_TASK_ID,
        "sensitivity_policy": dict(sensitivity_policy),
        "next_route": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _blocked_sections(reason: str) -> dict[str, Any]:
    return {
        "summary": {
            "blocked_reason": reason,
            "sensitivity_analysis_ready": False,
            "sensitivity_grid_ready": False,
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "top_candidate_from_2365": None,
        "top_candidate_decision_from_2365": None,
        "candidate_selection": {"selected_strategy_ids": []},
        "sensitivity_grid": _sensitivity_grid(),
        "sensitivity_matrix": [],
        "robustness_ranking": [],
        "decision_update": {
            "blocked_reason": reason,
            "decision_update_ready": False,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "path_summaries": [],
        "required_outputs_ready": {
            "sensitivity_grid": True,
            "cost_adjusted_metrics": False,
            "turnover_metrics": False,
            "cooldown_metrics": False,
            "combined_stress_results": False,
            "ranking_after_sensitivity": False,
            "decision_update": False,
            "top_candidate_after_sensitivity": False,
            "top_candidate_decision_after_sensitivity": False,
            "recommended_next_research_task": True,
        },
        "summary_findings": {},
        "sensitivity_analysis_ready": False,
        "sensitivity_grid_ready": True,
        "cost_adjusted_metrics_ready": False,
        "turnover_metrics_ready": False,
        "cooldown_metrics_ready": False,
        "decision_update_ready": False,
        "backtest_run": False,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
    }


def _sensitivity_policy(*, turnover_penalty: float, risk_cap_enabled: bool) -> dict[str, Any]:
    policy = _cadence_cost_constraint_policy(
        transaction_cost_bps=None,
        slippage_bps=None,
        turnover_penalty=turnover_penalty,
        max_turnover_per_month=1.0,
        min_holding_days=0,
        cooldown_days=0,
        max_single_step_weight_delta=1.0,
        risk_cap_enabled=risk_cap_enabled,
    )
    policy.update(
        {
            "policy_id": "dynamic_strategy_cost_turnover_cooldown_sensitivity_policy_v1",
            "rationale": (
                "Use TRADING-2365 ranking evidence to stress dynamic candidates "
                "under explicit cost, slippage, turnover cap, max-weight-delta, "
                "cooldown and min-holding assumptions."
            ),
            "intended_effect": (
                "Separate robust cost-adjusted edge from high-turnover or "
                "cooldown-fragile research artifacts before owner review."
            ),
            "validation_evidence": (
                "TRADING-2366 focused tests, source TRADING-2365 artifact binding, "
                "and real data-quality-gated CLI run."
            ),
            "review_condition": (
                "Review before moving any candidate to owner shadow-research gate."
            ),
        }
    )
    return policy


def _load_source_retest(
    *,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_cadence_matrix_path: Path,
) -> dict[str, Any]:
    if not source_event_retest_path.exists():
        return {
            "event_retest_path": str(source_event_retest_path),
            "exists": False,
            "ready_for_sensitivity": False,
            "blocked_reason": "source_event_driven_retest_missing",
        }
    payload = json.loads(source_event_retest_path.read_text(encoding="utf-8"))
    ranking_payload = _load_optional_json(source_candidate_ranking_path)
    matrix_payload = _load_optional_json(source_cadence_matrix_path)
    ranking_rows = list(
        ranking_payload.get("candidate_ranking")
        if ranking_payload
        else payload.get("candidate_ranking")
        or []
    )
    matrix_rows = list(
        matrix_payload.get("cadence_comparison_matrix")
        if matrix_payload
        else payload.get("cadence_comparison_matrix")
        or []
    )
    conclusions = _mapping(payload.get("conclusions"))
    top_candidate = (
        conclusions.get("top_candidate_id")
        or (ranking_rows[0].get("candidate_id") if ranking_rows else None)
    )
    top_decision = (
        conclusions.get("top_candidate_decision")
        or (ranking_rows[0].get("decision") if ranking_rows else None)
    )
    next_route = (
        conclusions.get("recommended_next_research_task")
        or _mapping(payload.get("summary")).get("recommended_next_research_task")
        or payload.get("next_route")
    )
    ready_for_sensitivity = (
        payload.get("status") == SOURCE_RETEST_READY_STATUS
        and payload.get("primary_execution_cadence") == PRIMARY_EXECUTION_CADENCE
        and next_route == SOURCE_EXPECTED_NEXT_ROUTE
        and bool(ranking_rows)
        and bool(matrix_rows)
    )
    return {
        "event_retest_path": str(source_event_retest_path),
        "candidate_ranking_path": str(source_candidate_ranking_path),
        "cadence_matrix_path": str(source_cadence_matrix_path),
        "exists": True,
        "status": payload.get("status"),
        "schema_version": payload.get("schema_version"),
        "sha256": _file_sha256(source_event_retest_path),
        "candidate_ranking_sha256": _file_sha256(source_candidate_ranking_path)
        if source_candidate_ranking_path.exists()
        else None,
        "cadence_matrix_sha256": _file_sha256(source_cadence_matrix_path)
        if source_cadence_matrix_path.exists()
        else None,
        "primary_execution_cadence": payload.get("primary_execution_cadence"),
        "top_candidate_from_2365": top_candidate,
        "top_candidate_decision_from_2365": top_decision,
        "candidate_ranking": ranking_rows,
        "cadence_comparison_matrix": matrix_rows,
        "source_dynamic_strategy_ids": list(payload.get("dynamic_strategy_ids") or []),
        "recommended_next_research_task": next_route,
        "ready_for_sensitivity": ready_for_sensitivity,
        "blocked_reason": None if ready_for_sensitivity else "source_retest_not_ready",
    }


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _select_sensitivity_candidates(
    source_retest: Mapping[str, Any],
    strategy_ids: Sequence[str] | None,
) -> list[str]:
    if strategy_ids:
        return _dedupe_strategy_ids(strategy_ids)
    ranking = list(source_retest.get("candidate_ranking") or [])
    top_three = [
        str(row.get("candidate_id"))
        for row in ranking[:3]
        if row.get("candidate_id")
    ]
    source_ids = {str(item) for item in source_retest.get("source_dynamic_strategy_ids") or []}
    if CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID in source_ids:
        top_three.append(CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID)
    return _dedupe_strategy_ids(top_three)


def _candidate_roles(source_retest: Mapping[str, Any]) -> list[dict[str, Any]]:
    ranking = list(source_retest.get("candidate_ranking") or [])
    roles: list[dict[str, Any]] = []
    for index, row in enumerate(ranking[:3], start=1):
        roles.append(
            {
                "candidate_id": row.get("candidate_id"),
                "role": "top_candidate_from_2365" if index == 1 else f"top_{index}_from_2365",
                "decision_from_2365": row.get("decision"),
            }
        )
    if CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID in {
        str(item) for item in source_retest.get("source_dynamic_strategy_ids") or []
    }:
        roles.append(
            {
                "candidate_id": CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
                "role": "current_dynamic_default_if_available",
            }
        )
    return roles


def _sensitivity_grid() -> dict[str, Any]:
    scenarios: list[dict[str, Any]] = []
    for transaction_cost_bps in (0, 2, 5, 10, 20):
        for slippage_bps in (0, 2, 5, 10):
            scenarios.append(
                _scenario_definition(
                    group="cost_slippage",
                    scenario_id=f"cost_tc{transaction_cost_bps}_slip{slippage_bps}",
                    transaction_cost_bps=transaction_cost_bps,
                    slippage_bps=slippage_bps,
                    cooldown_days=1,
                    min_holding_days=1,
                    max_turnover_per_month=None,
                    max_single_step_weight_delta=None,
                )
            )
    for cooldown_days in (0, 1, 3, 5, 10):
        for min_holding_days in (0, 1, 3, 5, 10):
            scenarios.append(
                _scenario_definition(
                    group="cooldown_min_holding",
                    scenario_id=f"cooldown_cd{cooldown_days}_hold{min_holding_days}",
                    transaction_cost_bps=5,
                    slippage_bps=5,
                    cooldown_days=cooldown_days,
                    min_holding_days=min_holding_days,
                    max_turnover_per_month=None,
                    max_single_step_weight_delta=None,
                )
            )
    turnover_values: tuple[float | None, ...] = (None, 2.0, 1.0, 0.5, 0.25)
    weight_delta_values: tuple[float | None, ...] = (None, 0.30, 0.20, 0.10)
    for max_turnover_per_month in turnover_values:
        for max_single_step_weight_delta in weight_delta_values:
            scenarios.append(
                _scenario_definition(
                    group="turnover_cap_weight_delta",
                    scenario_id=(
                        "turnover_"
                        f"{_label(max_turnover_per_month, 'unlimited')}_delta_"
                        f"{_label(max_single_step_weight_delta, 'unrestricted')}"
                    ),
                    transaction_cost_bps=5,
                    slippage_bps=5,
                    cooldown_days=3,
                    min_holding_days=3,
                    max_turnover_per_month=max_turnover_per_month,
                    max_single_step_weight_delta=max_single_step_weight_delta,
                )
            )
    scenarios.extend(
        [
            _scenario_definition(
                group="combined_stress",
                scenario_id="combined_base",
                transaction_cost_bps=2,
                slippage_bps=2,
                cooldown_days=1,
                min_holding_days=1,
                max_turnover_per_month=None,
                max_single_step_weight_delta=None,
            ),
            _scenario_definition(
                group="combined_stress",
                scenario_id="combined_realistic",
                transaction_cost_bps=5,
                slippage_bps=5,
                cooldown_days=3,
                min_holding_days=3,
                max_turnover_per_month=1.0,
                max_single_step_weight_delta=None,
            ),
            _scenario_definition(
                group="combined_stress",
                scenario_id="combined_conservative",
                transaction_cost_bps=10,
                slippage_bps=10,
                cooldown_days=5,
                min_holding_days=5,
                max_turnover_per_month=0.5,
                max_single_step_weight_delta=None,
            ),
            _scenario_definition(
                group="combined_stress",
                scenario_id="combined_harsh",
                transaction_cost_bps=20,
                slippage_bps=10,
                cooldown_days=10,
                min_holding_days=10,
                max_turnover_per_month=0.25,
                max_single_step_weight_delta=None,
            ),
        ]
    )
    return {
        "grid_type": "layered_sensitivity_grid",
        "scenario_count": len(scenarios),
        "groups": {
            "cost_slippage": {
                "transaction_cost_bps": [0, 2, 5, 10, 20],
                "slippage_bps": [0, 2, 5, 10],
            },
            "cooldown_min_holding": {
                "cooldown_days": [0, 1, 3, 5, 10],
                "min_holding_days": [0, 1, 3, 5, 10],
            },
            "turnover_cap_weight_delta": {
                "max_turnover_per_month": ["unlimited", 2.0, 1.0, 0.5, 0.25],
                "max_single_step_weight_delta": ["unrestricted", 0.30, 0.20, 0.10],
            },
            "combined_stress": list(COMBINED_SCENARIO_IDS),
        },
        "scenarios": scenarios,
    }


def _scenario_definition(
    *,
    group: str,
    scenario_id: str,
    transaction_cost_bps: float,
    slippage_bps: float,
    cooldown_days: int,
    min_holding_days: int,
    max_turnover_per_month: float | None,
    max_single_step_weight_delta: float | None,
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "scenario_group": group,
        "transaction_cost_bps": float(transaction_cost_bps),
        "slippage_bps": float(slippage_bps),
        "cooldown_days": int(cooldown_days),
        "min_holding_days": int(min_holding_days),
        "max_turnover_per_month": max_turnover_per_month,
        "max_turnover_per_month_label": _label(max_turnover_per_month, "unlimited"),
        "max_single_step_weight_delta": max_single_step_weight_delta,
        "max_single_step_weight_delta_label": _label(
            max_single_step_weight_delta,
            "unrestricted",
        ),
    }


def _label(value: float | None, none_label: str) -> str:
    if value is None:
        return none_label
    text = f"{float(value):.2f}".rstrip("0").rstrip(".")
    return text.replace(".", "p")


def _scenario_policy_for_sensitivity(
    *,
    cadence: str,
    scenario: Mapping[str, Any],
    policies: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    max_turnover = scenario.get("max_turnover_per_month")
    max_delta = scenario.get("max_single_step_weight_delta")
    policy = _scenario_policy(
        scenario_id=cadence,
        policies=policies,
        transaction_cost_bps=_float(scenario.get("transaction_cost_bps")),
        slippage_bps=_float(scenario.get("slippage_bps")),
        max_turnover_per_month=_float(max_turnover, 999.0) if max_turnover is not None else 999.0,
        min_holding_days=_int(scenario.get("min_holding_days")),
        cooldown_days=_int(scenario.get("cooldown_days")),
        max_single_step_weight_delta=_float(max_delta, 1.0) if max_delta is not None else 1.0,
    )
    policy["sensitivity_scenario_id"] = scenario["scenario_id"]
    policy["sensitivity_scenario_group"] = scenario["scenario_group"]
    policy["max_turnover_per_month"] = (
        _float(max_turnover) if max_turnover is not None else 999.0
    )
    if max_delta is not None:
        existing_cap = _float(policy.get("max_turnover_per_period"), 1.0)
        policy["max_turnover_per_period"] = round(min(existing_cap, _float(max_delta)), 6)
    else:
        policy["max_turnover_per_period"] = min(
            _float(policy.get("max_turnover_per_period"), 1.0),
            1.0,
        )
    if cadence == "cooldown_limited_event_driven":
        policy["minimum_holding_period"] = _int(scenario.get("min_holding_days"))
        policy["cooldown_days"] = _int(scenario.get("cooldown_days"))
        policy["execution_frequency"] = "threshold_with_min_holding"
    return policy


def _static_matrix_row(
    *,
    scenario: Mapping[str, Any],
    static_row: Mapping[str, Any],
    static_performance: Mapping[str, Any],
) -> dict[str, Any]:
    execution = _mapping(static_row.get("execution"))
    return {
        "scenario_id": scenario["scenario_id"],
        "scenario_group": scenario["scenario_group"],
        "candidate_id": STATIC_BASELINE_STRATEGY_ID,
        "strategy_id": STATIC_BASELINE_STRATEGY_ID,
        "execution_cadence": "static_baseline",
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "is_static_baseline": True,
        "transaction_cost_bps": scenario["transaction_cost_bps"],
        "slippage_bps": scenario["slippage_bps"],
        "cooldown_days": scenario["cooldown_days"],
        "min_holding_days": scenario["min_holding_days"],
        "max_turnover_per_month": scenario.get("max_turnover_per_month"),
        "max_turnover_per_month_label": scenario["max_turnover_per_month_label"],
        "max_single_step_weight_delta": scenario.get("max_single_step_weight_delta"),
        "max_single_step_weight_delta_label": scenario[
            "max_single_step_weight_delta_label"
        ],
        "performance_metrics": {
            "total_return": static_performance.get("total_return"),
            "annualized_return": static_performance.get("annualized_return"),
            "max_drawdown": static_performance.get("max_drawdown"),
            "volatility": static_performance.get("volatility"),
            "sharpe": static_performance.get("sharpe"),
            "sortino": static_performance.get("sortino"),
        },
        "cost_metrics": {
            "gross_return": static_performance.get("total_return"),
            "transaction_cost_drag": 0.0,
            "slippage_drag": 0.0,
            "cost_adjusted_return": static_performance.get("annualized_return"),
            "cost_adjusted_dynamic_vs_static_gap": 0.0,
        },
        "turnover_metrics": {
            "turnover": execution.get("turnover"),
            "rebalance_count": execution.get("rebalance_count"),
            "average_holding_days": execution.get("avg_holding_days"),
            "turnover_reduction_vs_base": 0.0,
            "turnover_adjusted_score": static_performance.get("annualized_return"),
        },
        "cooldown_metrics": {
            "cooldown_block_count": 0,
            "missed_signal_due_to_cooldown": 0,
            "stale_signal_prevented_count": 0,
            "cooldown_adjusted_return_gap": 0.0,
        },
        "robustness_metrics": {
            "scenario_survives_static": True,
            "uses_future_data": False,
        },
    }


def _sensitivity_matrix_row(
    *,
    scenario: Mapping[str, Any],
    cadence: str,
    row: Mapping[str, Any],
    prices: pd.DataFrame,
    actual_weights: pd.DataFrame,
    policy: Mapping[str, Any],
    static_performance: Mapping[str, Any],
    turnover_penalty: float,
) -> dict[str, Any]:
    performance = _mapping(row.get("performance"))
    execution = _mapping(row.get("execution"))
    cost_adjusted = _mapping(row.get("cost_adjusted"))
    gross_metrics = _performance_metrics(prices, actual_weights, cost_bps=0.0)
    gross_returns = _portfolio_return_series(prices, actual_weights, cost_bps=0.0)
    transaction = _float(scenario.get("transaction_cost_bps"))
    slippage = _float(scenario.get("slippage_bps"))
    total_cost_bps = transaction + slippage
    total_cost_drag = _float(cost_adjusted.get("cost_drag"))
    transaction_drag = total_cost_drag * _safe_ratio(transaction, total_cost_bps)
    slippage_drag = total_cost_drag * _safe_ratio(slippage, total_cost_bps)
    annual = _float(performance.get("annualized_return"))
    static_annual = _float(static_performance.get("annualized_return"))
    turnover = _float(execution.get("turnover"))
    no_lookahead = _mapping(row.get("no_lookahead_evidence"))
    return {
        "scenario_id": scenario["scenario_id"],
        "scenario_group": scenario["scenario_group"],
        "candidate_id": row.get("strategy_id"),
        "strategy_id": row.get("strategy_id"),
        "execution_cadence": cadence,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "is_static_baseline": False,
        "execution_policy_id": row.get("execution_policy_id"),
        "transaction_cost_bps": transaction,
        "slippage_bps": slippage,
        "cooldown_days": scenario["cooldown_days"],
        "min_holding_days": scenario["min_holding_days"],
        "max_turnover_per_month": scenario.get("max_turnover_per_month"),
        "max_turnover_per_month_label": scenario["max_turnover_per_month_label"],
        "max_single_step_weight_delta": scenario.get("max_single_step_weight_delta"),
        "max_single_step_weight_delta_label": scenario[
            "max_single_step_weight_delta_label"
        ],
        "performance_metrics": {
            "total_return": performance.get("total_return"),
            "annualized_return": performance.get("annualized_return"),
            "max_drawdown": performance.get("max_drawdown"),
            "volatility": performance.get("volatility"),
            "sharpe": performance.get("sharpe"),
            "sortino": performance.get("sortino"),
        },
        "cost_metrics": {
            "gross_return": round(_total_return(gross_returns), 6),
            "gross_annualized_return": gross_metrics.get("annual_return"),
            "transaction_cost_drag": round(transaction_drag, 6),
            "slippage_drag": round(slippage_drag, 6),
            "total_cost_drag": total_cost_drag,
            "cost_adjusted_return": performance.get("annualized_return"),
            "cost_adjusted_dynamic_vs_static_gap": round(annual - static_annual, 6),
        },
        "turnover_metrics": {
            "turnover": execution.get("turnover"),
            "rebalance_count": execution.get("rebalance_count"),
            "average_holding_days": execution.get("avg_holding_days"),
            "turnover_reduction_vs_base": 0.0,
            "turnover_adjusted_score": round(annual - turnover * turnover_penalty, 6),
        },
        "cooldown_metrics": {
            "cooldown_block_count": execution.get("cooldown_block_count"),
            "missed_signal_due_to_cooldown": execution.get("cooldown_block_count"),
            "stale_signal_prevented_count": execution.get(
                "expired_signal_suppression_count"
            ),
            "cooldown_adjusted_return_gap": 0.0,
        },
        "robustness_metrics": {
            "scenario_survives_static": annual > static_annual,
            "uses_future_data": bool(no_lookahead.get("uses_future_data")),
            "constraint_hit_count": execution.get("constraint_hit_count"),
            "stale_signal_execution_count": execution.get("stale_signal_execution_count"),
            "missed_signal_count": execution.get("missed_signal_count"),
        },
        "policy_hash": row.get("policy_hash"),
    }


def _apply_relative_sensitivity_metrics(rows: list[dict[str, Any]]) -> None:
    base_by_candidate_cadence = {
        (row.get("candidate_id"), row.get("execution_cadence")): row
        for row in rows
        if row.get("scenario_id") == "combined_base"
    }
    no_cooldown_by_candidate_cadence = {
        (row.get("candidate_id"), row.get("execution_cadence")): row
        for row in rows
        if row.get("scenario_id") == "cooldown_cd0_hold0"
    }
    for row in rows:
        key = (row.get("candidate_id"), row.get("execution_cadence"))
        base = base_by_candidate_cadence.get(key)
        no_cooldown = no_cooldown_by_candidate_cadence.get(key)
        turnover_metrics = _mapping(row.get("turnover_metrics"))
        cooldown_metrics = _mapping(row.get("cooldown_metrics"))
        if base:
            base_turnover = _float(_mapping(base.get("turnover_metrics")).get("turnover"))
            turnover_metrics["turnover_reduction_vs_base"] = round(
                base_turnover - _float(turnover_metrics.get("turnover")),
                6,
            )
        if no_cooldown:
            no_cooldown_return = _float(
                _mapping(no_cooldown.get("performance_metrics")).get("annualized_return")
            )
            cooldown_metrics["cooldown_adjusted_return_gap"] = round(
                _float(_mapping(row.get("performance_metrics")).get("annualized_return"))
                - no_cooldown_return,
                6,
            )
        row["turnover_metrics"] = dict(turnover_metrics)
        row["cooldown_metrics"] = dict(cooldown_metrics)


def _robustness_ranking(
    *,
    sensitivity_rows: Sequence[Mapping[str, Any]],
    source_retest: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> list[dict[str, Any]]:
    primary_rows = [
        row
        for row in sensitivity_rows
        if row.get("execution_cadence") == PRIMARY_EXECUTION_CADENCE
        and not row.get("is_static_baseline")
    ]
    static_row = next(
        row for row in sensitivity_rows if row.get("candidate_id") == STATIC_BASELINE_STRATEGY_ID
    )
    static_perf = _mapping(static_row.get("performance_metrics"))
    static_annual = _float(static_perf.get("annualized_return"))
    static_drawdown = _float(static_perf.get("max_drawdown"))
    drawdown_threshold = _float(thresholds.get("execution_lag_max_drawdown_cost_pp"), 2.0) / 100.0
    return_threshold = _float(thresholds.get("execution_lag_return_cost_abs_pp"), 1.0) / 100.0
    rows_by_candidate: dict[str, list[Mapping[str, Any]]] = {}
    for row in primary_rows:
        rows_by_candidate.setdefault(str(row.get("candidate_id")), []).append(row)
    ranking: list[dict[str, Any]] = []
    source_rank = {
        str(row.get("candidate_id")): _int(row.get("rank"), 9999)
        for row in source_retest.get("candidate_ranking") or []
    }
    for candidate_id, candidate_rows in rows_by_candidate.items():
        combined = {
            str(row.get("scenario_id")): row
            for row in candidate_rows
            if row.get("scenario_id") in COMBINED_SCENARIO_IDS
        }
        base = combined.get("combined_base", {})
        realistic = combined.get("combined_realistic", {})
        conservative = combined.get("combined_conservative", {})
        harsh = combined.get("combined_harsh", {})
        survives_base = _survives_static(base, static_annual)
        survives_realistic = _survives_static(realistic, static_annual)
        survives_conservative = _survives_static(conservative, static_annual)
        survives_harsh = _survives_static(harsh, static_annual)
        realistic_turnover = _float(_mapping(realistic.get("turnover_metrics")).get("turnover"))
        conservative_turnover = _float(
            _mapping(conservative.get("turnover_metrics")).get("turnover")
        )
        realistic_turnover_cap = _float(realistic.get("max_turnover_per_month"), 999.0)
        conservative_turnover_cap = _float(conservative.get("max_turnover_per_month"), 999.0)
        turnover_acceptable = (
            realistic_turnover <= realistic_turnover_cap
            and conservative_turnover <= conservative_turnover_cap
        )
        realistic_drawdown_ok = _drawdown_not_materially_worse(
            _float(_mapping(realistic.get("performance_metrics")).get("max_drawdown")),
            static_drawdown,
            drawdown_threshold,
        )
        conservative_drawdown_ok = _drawdown_not_materially_worse(
            _float(_mapping(conservative.get("performance_metrics")).get("max_drawdown")),
            static_drawdown,
            drawdown_threshold,
        )
        cooldown_gap = _float(
            _mapping(realistic.get("cooldown_metrics")).get("cooldown_adjusted_return_gap")
        )
        severe_cooldown_fragility = cooldown_gap < -return_threshold
        fragility_reason = _fragility_reason(
            survives_realistic=survives_realistic,
            survives_conservative=survives_conservative,
            turnover_acceptable=turnover_acceptable,
            drawdown_ok=realistic_drawdown_ok and conservative_drawdown_ok,
            severe_cooldown_fragility=severe_cooldown_fragility,
        )
        decision, decision_reason = _decision_update(
            survives_base=survives_base,
            survives_realistic=survives_realistic,
            survives_conservative=survives_conservative,
            survives_harsh=survives_harsh,
            turnover_acceptable=turnover_acceptable,
            drawdown_ok=realistic_drawdown_ok and conservative_drawdown_ok,
            severe_cooldown_fragility=severe_cooldown_fragility,
        )
        robustness_score = (
            int(survives_base)
            + int(survives_realistic) * 2
            + int(survives_conservative) * 3
            + int(survives_harsh) * 4
            + int(turnover_acceptable)
            + int(realistic_drawdown_ok and conservative_drawdown_ok)
            - int(severe_cooldown_fragility)
        )
        ranking.append(
            {
                "robust_rank": 0,
                "candidate_id": candidate_id,
                "source_rank_2365": source_rank.get(candidate_id),
                "decision_update": decision,
                "decision_update_reason": decision_reason,
                "fragility_reason": fragility_reason,
                "robustness_score": robustness_score,
                "survives_base_cost": survives_base,
                "survives_realistic_cost": survives_realistic,
                "survives_conservative_cost": survives_conservative,
                "survives_harsh_cost": survives_harsh,
                "turnover_acceptable": turnover_acceptable,
                "cooldown_fragility": "SEVERE" if severe_cooldown_fragility else "NOT_SEVERE",
                "realistic_cost_adjusted_return": _mapping(
                    realistic.get("performance_metrics")
                ).get("annualized_return"),
                "realistic_dynamic_vs_static_gap": _mapping(
                    realistic.get("cost_metrics")
                ).get("cost_adjusted_dynamic_vs_static_gap"),
                "realistic_turnover": realistic_turnover,
                "realistic_max_drawdown": _mapping(
                    realistic.get("performance_metrics")
                ).get("max_drawdown"),
                "conservative_cost_adjusted_return": _mapping(
                    conservative.get("performance_metrics")
                ).get("annualized_return"),
                "conservative_dynamic_vs_static_gap": _mapping(
                    conservative.get("cost_metrics")
                ).get("cost_adjusted_dynamic_vs_static_gap"),
                "harsh_dynamic_vs_static_gap": _mapping(harsh.get("cost_metrics")).get(
                    "cost_adjusted_dynamic_vs_static_gap"
                ),
            }
        )
    ranking.sort(
        key=lambda row: (
            _decision_sort_value(str(row.get("decision_update"))),
            _float(row.get("robustness_score")),
            _float(row.get("realistic_dynamic_vs_static_gap")),
            -_float(row.get("realistic_turnover")),
        ),
        reverse=True,
    )
    for index, row in enumerate(ranking, start=1):
        row["robust_rank"] = index
    return ranking


def _survives_static(row: Mapping[str, Any], static_annual: float) -> bool:
    return _float(_mapping(row.get("performance_metrics")).get("annualized_return")) > static_annual


def _drawdown_not_materially_worse(
    drawdown: float,
    static_drawdown: float,
    threshold: float,
) -> bool:
    return abs(drawdown) <= abs(static_drawdown) + threshold


def _fragility_reason(
    *,
    survives_realistic: bool,
    survives_conservative: bool,
    turnover_acceptable: bool,
    drawdown_ok: bool,
    severe_cooldown_fragility: bool,
) -> str:
    reasons: list[str] = []
    if not survives_realistic:
        reasons.append("realistic cost 后不再优于 static")
    if survives_realistic and not survives_conservative:
        reasons.append("conservative stress 下 edge 消失")
    if not turnover_acceptable:
        reasons.append("turnover 超出 sensitivity cap")
    if not drawdown_ok:
        reasons.append("max drawdown 相对 static 明显恶化")
    if severe_cooldown_fragility:
        reasons.append("cooldown / min-holding 明显削弱收益")
    return "；".join(reasons) if reasons else "未发现严重 sensitivity fragility"


def _decision_update(
    *,
    survives_base: bool,
    survives_realistic: bool,
    survives_conservative: bool,
    survives_harsh: bool,
    turnover_acceptable: bool,
    drawdown_ok: bool,
    severe_cooldown_fragility: bool,
) -> tuple[str, str]:
    if (
        survives_realistic
        and survives_conservative
        and turnover_acceptable
        and drawdown_ok
        and not severe_cooldown_fragility
    ):
        return (
            DECISION_ACCEPT,
            "candidate 通过 realistic / conservative cost、turnover、drawdown 和 cooldown screen。",
        )
    if survives_realistic and (not survives_conservative or not turnover_acceptable):
        return (
            DECISION_OWNER_REVIEW,
            "candidate 优于 static，但 conservative stress 或 turnover 仍需要 owner 风险偏好判断。",
        )
    if survives_base or survives_realistic:
        return (
            DECISION_CONTINUE,
            (
                "candidate 有信号但 sensitivity 不稳定，需要继续研究 "
                "cost / cooldown / risk-cap tuning。"
            ),
        )
    if survives_harsh:
        return (
            DECISION_CONTINUE,
            "harsh scenario 存在异常 surviving edge，需要人工复核 sensitivity path。",
        )
    return DECISION_REJECT, "candidate 在 realistic cost-adjusted comparison 中未保持优势。"


def _decision_sort_value(decision: str) -> int:
    return {
        DECISION_ACCEPT: 4,
        DECISION_OWNER_REVIEW: 3,
        DECISION_CONTINUE: 2,
        DECISION_REJECT: 1,
        DECISION_DEPRECATED: 0,
    }.get(decision, 0)


def _decision_update_payload(
    *,
    source_retest: Mapping[str, Any],
    robustness_ranking: Sequence[Mapping[str, Any]],
    sensitivity_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    top_after = robustness_ranking[0] if robustness_ranking else {}
    top_from_2365 = str(source_retest.get("top_candidate_from_2365") or "")
    top_after_id = str(top_after.get("candidate_id") or "")
    top_from_2365_row = next(
        (row for row in robustness_ranking if row.get("candidate_id") == top_from_2365),
        {},
    )
    ranking_changed = bool(top_after_id and top_after_id != top_from_2365)
    summary_findings = {
        "top_candidate_survives_realistic_cost": _yes_no(
            top_from_2365_row.get("survives_realistic_cost")
        ),
        "top_candidate_survives_conservative_cost": _yes_no(
            top_from_2365_row.get("survives_conservative_cost")
        ),
        "top_candidate_turnover_acceptable": _yes_no(
            top_from_2365_row.get("turnover_acceptable")
        ),
        "top_candidate_cooldown_fragility": top_from_2365_row.get(
            "cooldown_fragility",
            "UNKNOWN",
        ),
        "ranking_changed_after_sensitivity": _yes_no(ranking_changed),
        "upgrade_from_owner_review_recommended": _yes_no(
            top_from_2365_row.get("decision_update") == DECISION_ACCEPT
        ),
    }
    return {
        "schema_version": "dynamic_strategy_sensitivity_decision_update.v1",
        "decision_update_ready": bool(robustness_ranking),
        "source_top_candidate": top_from_2365,
        "source_top_candidate_decision": source_retest.get(
            "top_candidate_decision_from_2365"
        ),
        "top_candidate_after_sensitivity": top_after.get("candidate_id"),
        "top_candidate_decision_after_sensitivity": top_after.get("decision_update"),
        "top_candidate_from_2365_decision_after_sensitivity": top_from_2365_row.get(
            "decision_update"
        ),
        "ranking_changed_after_sensitivity": ranking_changed,
        "robustness_ranking": list(robustness_ranking),
        "combined_stress_results": [
            row
            for row in sensitivity_rows
            if row.get("scenario_id") in COMBINED_SCENARIO_IDS
            and row.get("execution_cadence") == PRIMARY_EXECUTION_CADENCE
            and not row.get("is_static_baseline")
        ],
        "summary_findings": summary_findings,
        "recommended_next_research_task": NEXT_ROUTE,
        "production_boundary": {
            "scheduler_enabled": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    }


def _yes_no(value: Any) -> str:
    if value is True:
        return "YES"
    if value is False:
        return "NO"
    return "UNKNOWN"


def _path_summary(
    row: Mapping[str, Any],
    path_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "scenario_id": row.get("scenario_id"),
        "execution_cadence": row.get("execution_cadence"),
        "row_count": len(path_rows),
        "sample_rows": list(path_rows[:5]),
        "path_schema_fields": sorted(path_rows[0].keys()) if path_rows else [],
    }


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return numerator / denominator


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    result_path = output_root / "sensitivity_result.json"
    matrix_path = output_root / "sensitivity_matrix.json"
    decision_path = output_root / "decision_update.json"
    report_path = docs_root / "dynamic_strategy_cost_turnover_cooldown_sensitivity.md"
    matrix_doc_path = docs_root / "dynamic_strategy_sensitivity_matrix.md"
    decision_doc_path = docs_root / "dynamic_strategy_decision_update_summary.md"
    route_doc_path = docs_root / "dynamic_strategy_2367_route.md"
    payload["artifact_paths"] = {
        "json_path": str(result_path),
        "sensitivity_matrix_json": str(matrix_path),
        "decision_update_json": str(decision_path),
        "markdown_path": str(report_path),
        "sensitivity_matrix_markdown": str(matrix_doc_path),
        "decision_update_markdown": str(decision_doc_path),
        "next_route_markdown": str(route_doc_path),
    }
    _write_json(result_path, payload)
    _write_json(matrix_path, _matrix_payload(payload))
    _write_json(decision_path, _decision_payload(payload))
    report_path.write_text(_main_markdown(payload), encoding="utf-8")
    matrix_doc_path.write_text(_matrix_markdown(payload), encoding="utf-8")
    decision_doc_path.write_text(_decision_markdown(payload), encoding="utf-8")
    route_doc_path.write_text(_route_markdown(payload), encoding="utf-8")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _matrix_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_sensitivity_matrix.v1",
        "report_type": "dynamic_strategy_sensitivity_matrix",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "primary_execution_cadence": payload.get("primary_execution_cadence"),
        "sensitivity_grid": payload.get("sensitivity_grid"),
        "sensitivity_matrix": payload.get("sensitivity_matrix", []),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
        "production_allowed": payload.get("production_allowed"),
    }


def _decision_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_sensitivity_decision_update.v1",
        "report_type": "dynamic_strategy_sensitivity_decision_update",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "decision_update": payload.get("decision_update", {}),
        "summary_findings": payload.get("summary_findings", {}),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
        "production_allowed": payload.get("production_allowed"),
    }


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    findings = _mapping(payload.get("summary_findings"))
    date_range = _mapping(payload.get("requested_date_range"))
    top_after = summary.get("top_candidate_after_sensitivity")
    decision_after = summary.get("top_candidate_decision_after_sensitivity")
    survives_realistic = findings.get("top_candidate_survives_realistic_cost")
    survives_conservative = findings.get("top_candidate_survives_conservative_cost")
    turnover_acceptable = findings.get("top_candidate_turnover_acceptable")
    ranking_changed = findings.get("ranking_changed_after_sensitivity")
    upgrade_recommended = findings.get("upgrade_from_owner_review_recommended")
    return "\n".join(
        [
            "# 动态策略 cost / turnover / cooldown sensitivity",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- source top candidate：`{summary.get('top_candidate_from_2365')}`",
            f"- source decision：`{summary.get('top_candidate_decision_from_2365')}`",
            f"- top candidate after sensitivity：`{top_after}`",
            f"- decision after sensitivity：`{decision_after}`",
            f"- requested date range：`{date_range.get('start')} to {date_range.get('end')}`",
            f"- data quality：`{_mapping(payload.get('data_quality')).get('status')}`",
            "",
            "## Required answers",
            "",
            f"- 2365 top candidate survives realistic cost：`{survives_realistic}`",
            f"- 2365 top candidate survives conservative cost：`{survives_conservative}`",
            f"- 2365 top candidate turnover acceptable：`{turnover_acceptable}`",
            f"- cooldown fragility：`{findings.get('top_candidate_cooldown_fragility')}`",
            f"- ranking changed after sensitivity：`{ranking_changed}`",
            f"- upgrade from OWNER_REVIEW_REQUIRED recommended：`{upgrade_recommended}`",
            "",
            "## 安全边界",
            "",
            (
                "- 本报告只生成 research evidence，不批准 scheduler、paper-shadow、"
                "production 或 broker/order。"
            ),
            f"- next route：`{NEXT_ROUTE}`",
        ]
    )


def _matrix_markdown(payload: Mapping[str, Any]) -> str:
    rows = list(payload.get("robustness_ranking") or [])
    lines = [
        "# 动态策略 sensitivity matrix 摘要",
        "",
        "|rank|candidate|decision|realistic_gap|conservative_gap|turnover|fragility|",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows[:20]:
        lines.append(
            "|{rank}|`{candidate}`|`{decision}`|{realistic_gap}|{conservative_gap}|{turnover}|{fragility}|".format(
                rank=row.get("robust_rank"),
                candidate=row.get("candidate_id"),
                decision=row.get("decision_update"),
                realistic_gap=_fmt(row.get("realistic_dynamic_vs_static_gap")),
                conservative_gap=_fmt(row.get("conservative_dynamic_vs_static_gap")),
                turnover=_fmt(row.get("realistic_turnover")),
                fragility=str(row.get("fragility_reason") or "").replace("|", "/"),
            )
        )
    if not rows:
        lines.append("|n/a|n/a|`BLOCKED`|n/a|n/a|n/a|matrix not available|")
    return "\n".join(lines)


def _decision_markdown(payload: Mapping[str, Any]) -> str:
    decision = _mapping(payload.get("decision_update"))
    top_after = decision.get("top_candidate_after_sensitivity")
    top_decision_after = decision.get("top_candidate_decision_after_sensitivity")
    source_top_decision_after = decision.get(
        "top_candidate_from_2365_decision_after_sensitivity"
    )
    return "\n".join(
        [
            "# 动态策略 sensitivity 决策更新",
            "",
            f"- decision_update_ready：`{decision.get('decision_update_ready')}`",
            f"- source top candidate：`{decision.get('source_top_candidate')}`",
            f"- source top candidate decision：`{decision.get('source_top_candidate_decision')}`",
            f"- top candidate after sensitivity：`{top_after}`",
            f"- top decision after sensitivity：`{top_decision_after}`",
            f"- source top decision after sensitivity：`{source_top_decision_after}`",
            f"- ranking changed：`{decision.get('ranking_changed_after_sensitivity')}`",
            f"- next route：`{NEXT_ROUTE}`",
            "",
            "本决策更新仍是 research-only，不代表 paper-shadow、production 或 broker approval。",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2367 路由",
            "",
            f"- current task status：`{payload.get('status')}`",
            f"- next route：`{NEXT_ROUTE}`",
            (
                "- route reason：top candidate sensitivity 完成后需要 owner review "
                "和 shadow research gate。"
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
