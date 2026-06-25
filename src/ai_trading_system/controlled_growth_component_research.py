from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.qqq_plus_growth_challenger import (
    TQQQ_DAILY_LEVERAGE_MULTIPLIER,
    _benchmark_specs,
    _candidate_limit,
    _drawdown_replay_row,
    _float,
    _growth_candidate_specs,
    _int,
    _mapping,
    _normalise_weights,
    _period_row,
    _records,
    _research_mapping,
    _returns_and_weights,
    _run_metric_set,
    _stable_hash,
    _vol_target_candidate_rows,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _data_quality_gate,
    _load_price_matrix,
    _metrics_for_strategy,
    _slice_prices,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_growth_component_candidate_registry_v2.yaml"
)
DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "growth_components"
)
DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_component_owner_decision_pack.md"
)
DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "research_roadmap_v2_master_review.md"
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


def run_layer2_growth_component_restart_contract(
    *,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    conditions = _minimum_growth_conditions()
    prohibited = [
        "do_not_restore_old_qqq_plus_growth_as_selectable",
        "do_not_promote_tqqq_heavy_as_mainline",
        "do_not_use_leaps_or_wheel",
        "do_not_admit_candidate_without_beta_attribution",
    ]
    blockers = _unsafe_config_blockers(config)
    status = "GROWTH_RESTART_BLOCKED" if blockers else "GROWTH_RESTART_CONTRACT_READY"
    payload = _payload(
        report_type="layer2_growth_component_restart_contract",
        title="Layer-2 Growth Component Restart Contract",
        status=status,
        summary={
            "minimum_condition_count": len(conditions),
            "prohibited_path_count": len(prohibited),
            "owner_manual_review_required": True,
            **_safety_summary(),
        },
        minimum_entry_conditions=conditions,
        prohibited_paths=prohibited,
        owner_manual_review_required=True,
        blockers=blockers,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "layer2_growth_component_restart_contract",
            "Layer-2 Growth Component Restart Contract",
            "aits research strategies layer2-growth-component-restart-contract",
            "layer2_growth_component_restart_contract",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_controlled_growth_component_registry_v2_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    candidates = _growth_candidate_specs(config)
    issues = _registry_v2_issues(config, candidates)
    if not candidates:
        status = "GROWTH_COMPONENT_REGISTRY_V2_BLOCKED"
    elif issues:
        status = "GROWTH_COMPONENT_REGISTRY_V2_PARTIAL"
    else:
        status = "GROWTH_COMPONENT_REGISTRY_V2_READY"
    payload = _payload(
        report_type="controlled_growth_component_registry_v2_review",
        title="Controlled Growth Component Registry V2 Review",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "candidate_type_count": len(list(config.get("candidate_types", []))),
            "issue_count": len(issues),
            "max_tqqq_weight": _candidate_limit(config, "max_tqqq_weight"),
            "max_effective_qqq_beta": _candidate_limit(config, "max_effective_qqq_beta"),
            "minimum_holding_period": _int(
                _research_mapping(config, "candidate_limits").get(
                    "minimum_holding_period_trading_days"
                )
            ),
            "max_switches_per_year": _candidate_limit(config, "max_switches_per_year"),
            **_safety_summary(),
        },
        registry_policy={
            "policy_id": config.get("policy_id"),
            "policy_metadata": config.get("policy_metadata"),
            "market_regime": config.get("market_regime"),
            "safety_boundary": config.get("safety_boundary"),
            "candidate_types": list(config.get("candidate_types", [])),
        },
        candidates=candidates,
        issues=issues,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "controlled_growth_component_registry_v2_review",
            "Controlled Growth Component Registry V2 Review",
            "aits research strategies controlled-growth-component-registry-v2-review",
            "controlled_growth_component_registry_v2_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_beta_adjusted_growth_edge_contract(
    *,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    policy = _research_mapping(config, "beta_adjusted_edge_contract")
    missing = [
        field
        for field in (
            "max_beta_explained_edge_share",
            "drawdown_penalty_weight",
            "turnover_penalty_weight",
            "path_dependency_penalty_weight",
            "net_edge_minimum",
        )
        if field not in policy
    ]
    status = "BETA_EDGE_CONTRACT_BLOCKED" if missing else "BETA_EDGE_CONTRACT_READY"
    payload = _payload(
        report_type="beta_adjusted_growth_edge_contract",
        title="Beta-Adjusted Growth Edge Contract",
        status=status,
        summary={
            "required_output_count": 9,
            "missing_policy_field_count": len(missing),
            "net_edge_minimum": policy.get("net_edge_minimum"),
            **_safety_summary(),
        },
        required_outputs=[
            "raw_return_edge_vs_100_qqq",
            "effective_qqq_beta",
            "beta_adjusted_return_edge",
            "beta_adjusted_calmar_edge",
            "beta_adjusted_sharpe_edge",
            "drawdown_penalty",
            "turnover_penalty",
            "path_dependency_penalty",
            "net_edge_after_penalty",
        ],
        decision_status_values=[
            "BETA_EXPLAINS_EDGE",
            "BETA_ADJUSTED_EDGE_PRESENT",
        ],
        beta_adjusted_edge_policy=policy,
        blockers=missing,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "beta_adjusted_growth_edge_contract",
            "Beta-Adjusted Growth Edge Contract",
            "aits research strategies beta-adjusted-growth-edge-contract",
            "beta_adjusted_growth_edge_contract",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_low_turnover_controlled_growth_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    candidates = [
        _with_controlled_defaults(row)
        for row in _records(config.get("low_turnover_controlled_growth_candidates"))
    ]
    return _run_growth_search(
        report_type="low_turnover_controlled_growth_search",
        title="Low-Turnover Controlled Growth Search",
        command="aits research strategies low-turnover-controlled-growth-search",
        status_found="CONTROLLED_GROWTH_CANDIDATES_FOUND",
        status_no_edge="NO_CONTROLLED_GROWTH_EDGE",
        status_blocked="CONTROLLED_GROWTH_SEARCH_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={
            "search_constraints": _search_constraints(config),
            "monthly_or_threshold_rebalance_only": True,
            "no_daily_flip_flop": True,
        },
    )


def run_volatility_targeted_growth_component_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    candidates = _vol_target_candidate_rows(config)
    return _run_growth_search(
        report_type="volatility_targeted_growth_component_search",
        title="Volatility-Targeted Growth Component Search",
        command="aits research strategies volatility-targeted-growth-component-search",
        status_found="VOL_TARGET_GROWTH_CANDIDATES_FOUND",
        status_no_edge="VOL_TARGET_GROWTH_NO_EDGE",
        status_blocked="VOL_TARGET_GROWTH_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"search_grid": _research_mapping(config, "volatility_targeted_search")},
    )


def run_drawdown_guarded_growth_component_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    candidates = [
        _with_controlled_defaults(row)
        for row in _records(config.get("drawdown_guarded_growth_candidates"))
    ]
    payload = _run_growth_search(
        report_type="drawdown_guarded_growth_component_search",
        title="Drawdown-Guarded Growth Component Search",
        command="aits research strategies drawdown-guarded-growth-component-search",
        status_found="DRAWDOWN_GUARDED_GROWTH_FOUND",
        status_no_edge="DRAWDOWN_GUARDED_NO_EDGE",
        status_blocked="DRAWDOWN_GUARDED_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={
            "rule_candidates": _research_mapping(config, "drawdown_guarded_policy"),
        },
    )
    if payload["status"] == "DRAWDOWN_GUARDED_GROWTH_FOUND":
        over_defensive = all(
            _float(row.get("missed_rebound_cost")) > _float(row.get("calmar_edge"))
            for row in _records(payload.get("candidate_results"))
        )
        if over_defensive:
            payload["status"] = "DRAWDOWN_GUARDED_OVER_DEFENSIVE"
            _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_component_beta_exposure_attribution(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="growth_component_beta_exposure_attribution",
            title="Growth Component Beta Exposure Attribution",
            status="GROWTH_ATTRIBUTION_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload

    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    qqq_annual = float(qqq_returns.mean() * _annualization(config))
    rows = []
    for candidate in _selected_candidates(config, output_root):
        returns, weights = _returns_and_weights(candidate, prices, config)
        metrics = _metrics_for_strategy(
            candidate,
            returns,
            weights,
            qqq_returns,
            annualization=_annualization(config),
            cost_bps=0.0,
        )
        rows.append(_attribution_row(candidate, returns, weights, metrics, qqq_returns, qqq_annual))
    beta_explains = rows and all(
        _float(row["return_attribution_qqq_beta"])
        >= abs(_float(row["return_attribution_timing"]))
        + abs(_float(row["return_attribution_tqqq_overlay"]))
        for row in rows
    )
    status = "BETA_EXPLAINS_MOST_EDGE" if beta_explains else "TIMING_OR_RISK_CONTROL_EDGE_PRESENT"
    if not rows:
        status = "GROWTH_ATTRIBUTION_BLOCKED"
    payload = _payload(
        report_type="growth_component_beta_exposure_attribution",
        title="Growth Component Beta Exposure Attribution",
        status=status,
        summary={
            "candidate_count": len(rows),
            "beta_explains_most_edge": bool(beta_explains),
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        attribution_rows=rows,
        data_quality=data_gate,
        report_registry_entry=_report_registry_entry(
            "growth_component_beta_exposure_attribution",
            "Growth Component Beta Exposure Attribution",
            "aits research strategies growth-component-beta-exposure-attribution",
            "growth_component_beta_exposure_attribution",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_component_period_drawdown_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="growth_component_period_drawdown_validation",
            title="Growth Component Period Drawdown Validation",
            status="GROWTH_PERIOD_DRAWDOWN_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload

    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    qqq_strategy = next(
        (row for row in _benchmark_specs(config) if row.get("strategy_id") == "100_qqq"),
        {},
    )
    qqq_returns, qqq_weights = _returns_and_weights(qqq_strategy, prices, config)
    period_rows: list[dict[str, Any]] = []
    drawdown_rows: list[dict[str, Any]] = []
    candidate_summaries: list[dict[str, Any]] = []
    for candidate in _selected_candidates(config, output_root):
        returns, weights = _returns_and_weights(candidate, prices, config)
        selected_period_rows = [
            _period_row(candidate, returns, weights, qqq_returns, qqq_weights, period, config)
            for period in _records(config.get("periods"))
        ]
        period_rows.extend(selected_period_rows)
        drawdown_rows.extend(
            _drawdown_replay_row(candidate, returns, weights, qqq_returns, episode, config)
            for episode in _drawdown_episode_rows(config, prices)
        )
        candidate_summaries.append(_period_validation_summary(candidate, selected_period_rows))
    concentrated = any(row["only_ai_rally_effective"] for row in candidate_summaries)
    drawdown_risk = any(
        row.get("covered") and _float(row.get("max_drawdown_vs_qqq")) < -0.05
        for row in drawdown_rows
    )
    if drawdown_risk:
        status = "GROWTH_DRAWDOWN_RISK_TOO_HIGH"
    elif concentrated:
        status = "GROWTH_REGIME_CONCENTRATED"
    else:
        status = "GROWTH_PERIOD_DRAWDOWN_VALIDATED"
    payload = _payload(
        report_type="growth_component_period_drawdown_validation",
        title="Growth Component Period Drawdown Validation",
        status=status,
        summary={
            "candidate_count": len(candidate_summaries),
            "period_row_count": len(period_rows),
            "drawdown_episode_count": len(drawdown_rows),
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        required_coverage=[
            "2022_rate_hike_bear_market",
            "2023_recovery",
            "2024_ai_rally",
            "2025_to_latest",
            "largest_qqq_drawdown",
            "largest_tqqq_drawdown",
            "high_rate_sgov_carry_period",
        ],
        candidate_summaries=candidate_summaries,
        period_rows=period_rows,
        drawdown_episode_rows=drawdown_rows,
        required_answers=_period_required_answers(candidate_summaries, drawdown_rows),
        data_quality=data_gate,
        report_registry_entry=_report_registry_entry(
            "growth_component_period_drawdown_validation",
            "Growth Component Period Drawdown Validation",
            "aits research strategies growth-component-period-drawdown-validation",
            "growth_component_period_drawdown_validation",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_component_cost_turnover_sensitivity(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="growth_component_cost_turnover_sensitivity",
            title="Growth Component Cost Turnover Sensitivity",
            status="GROWTH_COST_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload

    base_rows = _metric_rows(
        _selected_candidates(config, output_root),
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    scenario_rows = []
    for row in base_rows:
        scenario_rows.extend(_cost_sensitivity_rows(row, config))
    max_switches = _candidate_limit(config, "max_switches_per_year")
    turnover_too_high = any(
        _float(row.get("switches_per_year")) > max_switches for row in base_rows
    )
    cost_sensitive = any(
        _float(row.get("annual_return_degradation")) > _float(
            _research_mapping(config, "cost_turnover_sensitivity").get(
                "max_annual_return_degradation"
            )
        )
        for row in scenario_rows
    )
    if turnover_too_high:
        status = "GROWTH_TURNOVER_TOO_HIGH"
    elif cost_sensitive:
        status = "GROWTH_COST_SENSITIVE"
    else:
        status = "GROWTH_COST_ROBUST"
    payload = _payload(
        report_type="growth_component_cost_turnover_sensitivity",
        title="Growth Component Cost Turnover Sensitivity",
        status=status,
        summary={
            "candidate_count": len(base_rows),
            "scenario_count": len(scenario_rows),
            "turnover_too_high": turnover_too_high,
            "cost_sensitive": cost_sensitive,
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        scenario_rows=scenario_rows,
        data_quality=data_gate,
        report_registry_entry=_report_registry_entry(
            "growth_component_cost_turnover_sensitivity",
            "Growth Component Cost Turnover Sensitivity",
            "aits research strategies growth-component-cost-turnover-sensitivity",
            "growth_component_cost_turnover_sensitivity",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_component_readiness_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    low = run_low_turnover_controlled_growth_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    period = run_growth_component_period_drawdown_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidate = _best_candidate_row(_records(low.get("candidate_results")))
    blockers: list[str] = []
    warnings: list[str] = []
    if not candidate:
        blockers.append("no_growth_candidate_results")
    else:
        if _float(candidate.get("beta_adjusted_edge")) <= 0.0:
            blockers.append("beta_adjusted_edge_not_present")
        if candidate.get("dominance_status") == "DOMINATED_BY_100_QQQ":
            blockers.append("dominated_by_100_qqq")
        if _float(candidate.get("max_drawdown")) < _float(
            _research_mapping(config, "readiness_gate").get("max_drawdown_floor")
        ):
            blockers.append("max_drawdown_unacceptable")
        if _float(candidate.get("calmar_edge")) < 0.0 and _float(candidate.get("sharpe_edge")) < 0:
            blockers.append("risk_adjusted_metrics_weaker_than_100_qqq")
        if _float(candidate.get("switches_per_year")) > _candidate_limit(
            config, "max_switches_per_year"
        ):
            blockers.append("switch_count_too_high")
        if not candidate.get("policy_hash"):
            blockers.append("definition_hash_missing")
    if period.get("status") == "GROWTH_REGIME_CONCENTRATED":
        blockers.append("period_split_overconcentrated")
    if str(low.get("status", "")).endswith("BLOCKED"):
        blockers.append("data_quality_or_search_blocked")
    if period.get("status") == "GROWTH_DRAWDOWN_RISK_TOO_HIGH":
        blockers.append("drawdown_risk_too_high")
    if period.get("status") == "GROWTH_PERIOD_DRAWDOWN_BLOCKED":
        blockers.append("period_drawdown_validation_blocked")
    component_ready_review_allowed = not blockers
    if str(low.get("status", "")).endswith("BLOCKED"):
        status = "GROWTH_COMPONENT_BLOCKED"
    elif component_ready_review_allowed:
        status = "GROWTH_COMPONENT_REVIEWABLE"
    elif warnings:
        status = "GROWTH_COMPONENT_NEEDS_MORE_REVIEW"
    else:
        status = "NO_GROWTH_COMPONENT_READY"
    payload = _payload(
        report_type="growth_component_readiness_gate",
        title="Growth Component Readiness Gate",
        status=status,
        summary={
            "candidate_id": candidate.get("candidate_id") if candidate else None,
            "component_ready_review_allowed": component_ready_review_allowed,
            "blocking_reason_count": len(blockers),
            "warning_reason_count": len(warnings),
            **_safety_summary(),
        },
        candidate_id=candidate.get("candidate_id") if candidate else None,
        component_ready_review_allowed=component_ready_review_allowed,
        blocking_reasons=blockers,
        warning_reasons=warnings,
        recommended_role=(
            "layer2_component_candidate_review"
            if component_ready_review_allowed
            else "research_only_candidate"
        ),
        selected_candidate=candidate,
        source_statuses={
            "low_turnover_controlled_growth_search": low.get("status"),
            "growth_component_period_drawdown_validation": period.get("status"),
        },
        source_artifacts={
            "low_turnover_controlled_growth_search": low.get("artifact_paths", {}),
            "growth_component_period_drawdown_validation": period.get("artifact_paths", {}),
        },
        report_registry_entry=_report_registry_entry(
            "growth_component_readiness_gate",
            "Growth Component Readiness Gate",
            "aits research strategies growth-component-readiness-gate",
            "growth_component_readiness_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_component_owner_decision_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    readiness = run_growth_component_readiness_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    readiness_allowed = bool(readiness.get("component_ready_review_allowed"))
    selected = _mapping(readiness.get("selected_candidate"))
    answers = {
        "1_material_growth_candidate_exists": readiness_allowed,
        "2_edge_survives_beta_adjustment": _float(selected.get("beta_adjusted_edge")) > 0.0,
        "3_max_drawdown_acceptable": "max_drawdown_unacceptable"
        not in _records(readiness.get("blocking_reasons")),
        "4_not_only_ai_rally_effective": "period_split_overconcentrated"
        not in _records(readiness.get("blocking_reasons")),
        "5_definition_hash_lockable": bool(selected.get("policy_hash")),
        "6_component_ready_review_allowed": readiness_allowed,
        "7_keep_research_only": True,
        "8_continue_pause_layer1_selector": True,
        "9_continue_block_leaps_wheel": True,
    }
    if readiness.get("status") == "GROWTH_COMPONENT_BLOCKED":
        owner_recommendation = "BLOCKED"
    elif readiness_allowed:
        owner_recommendation = "PROMOTE_TO_COMPONENT_REVIEW"
    elif selected:
        owner_recommendation = "KEEP_GROWTH_RESEARCH_ONLY"
    else:
        owner_recommendation = "NO_MATERIAL_GROWTH_EDGE"
    payload = _payload(
        report_type="growth_component_owner_decision_pack",
        title="Growth Component Owner Decision Pack",
        status="GROWTH_COMPONENT_OWNER_DECISION_PACK_READY",
        summary={
            "candidate_id": readiness.get("candidate_id"),
            "owner_recommendation": owner_recommendation,
            "component_ready_review_allowed": readiness_allowed,
            **_safety_summary(),
        },
        required_answers=answers,
        owner_recommendation=owner_recommendation,
        source_statuses={"growth_component_readiness_gate": readiness.get("status")},
        source_artifacts={"growth_component_readiness_gate": readiness.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "growth_component_owner_decision_pack",
            "Growth Component Owner Decision Pack",
            "aits research strategies growth-component-owner-decision-pack",
            "growth_component_owner_decision_pack",
            extra_artifact_globs=["docs/research/growth_component_owner_decision_pack.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Component Owner Decision Pack")
    payload["owner_decision_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_and_growth_dual_track_roadmap(
    *,
    output_root: Path = PROJECT_ROOT / "outputs" / "research_strategies" / "roadmap",
) -> dict[str, Any]:
    payload = _payload(
        report_type="equal_risk_and_growth_dual_track_roadmap",
        title="Equal-Risk And Growth Dual-Track Roadmap",
        status="DUAL_TRACK_ROADMAP_READY",
        summary={
            "current_primary_track": "equal_risk_qqq_sgov_forward_aging",
            "current_growth_track": "controlled_growth_component_research_v2",
            "paused_track_count": 3,
            "blocked_track_count": 3,
            **_safety_summary(),
        },
        current_primary_track={
            "strategy_id": "equal_risk_qqq_sgov",
            "role": "defensive_primary",
            "status": "forward_aging_active_research_only",
        },
        current_growth_track={
            "track_id": "controlled_growth_component_research_v2",
            "status": "research_only",
            "component_ready_review_allowed": False,
        },
        paused_tracks=[
            "layer1_simple_rule_selector_dry_run_only",
            "qqq_plus_growth_research_only_inactive_reference",
            "tqqq_heavy_paused",
        ],
        blocked_tracks=["tail_risk_fallback", "LEAPS", "Wheel"],
        next_minimum_tasks=[
            "run_equal_risk_observation_continuity_check",
            "run_growth_component_readiness_gate",
            "owner_review_growth_component_owner_decision_pack",
        ],
        owner_next_action="review_equal_risk_forward_aging_and_growth_research_only_state",
        report_registry_entry=_report_registry_entry(
            "equal_risk_and_growth_dual_track_roadmap",
            "Equal-Risk And Growth Dual-Track Roadmap",
            "aits research strategies equal-risk-and-growth-dual-track-roadmap",
            "equal_risk_and_growth_dual_track_roadmap",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_research_roadmap_v2_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Path = (
        PROJECT_ROOT / "outputs" / "research_strategies" / "simple_baselines"
    ),
    growth_output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Path = PROJECT_ROOT / "outputs" / "research_strategies" / "roadmap",
    owner_docs_path: Path = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    docs_path: Path = DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    from ai_trading_system.research_roadmap_stabilization import (
        run_equal_risk_forward_aging_scoreboard_safety_gate,
        run_equal_risk_observation_continuity_check,
    )

    continuity = run_equal_risk_observation_continuity_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=simple_output_root,
        as_of_date=as_of_date,
    )
    scoreboard = run_equal_risk_forward_aging_scoreboard_safety_gate(
        output_root=simple_output_root,
    )
    owner = run_growth_component_owner_decision_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=growth_output_root,
        docs_path=owner_docs_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    roadmap = run_equal_risk_and_growth_dual_track_roadmap(output_root=output_root)
    owner_recommendation = str(owner.get("owner_recommendation"))
    if any(
        str(source.get("status", "")).endswith("BLOCKED")
        for source in (continuity, scoreboard, owner)
    ):
        status = "BLOCKED"
    elif owner_recommendation == "PROMOTE_TO_COMPONENT_REVIEW":
        status = "GROWTH_COMPONENT_REVIEWABLE"
    elif owner_recommendation == "NO_MATERIAL_GROWTH_EDGE":
        status = "NO_GROWTH_EDGE_FOUND"
    else:
        status = "CONTINUE_CONTROLLED_GROWTH_RESEARCH"
    final_answers = {
        "1_equal_risk_forward_aging_healthy": continuity.get("status")
        != "OBSERVATION_CONTINUITY_BLOCKED",
        "2_layer1_selector_continues_archived": True,
        "3_new_controlled_growth_component_found": owner_recommendation
        == "PROMOTE_TO_COMPONENT_REVIEW",
        "4_continue_growth_research_if_none": owner_recommendation
        in {"KEEP_GROWTH_RESEARCH_ONLY", "NO_MATERIAL_GROWTH_EDGE"},
        "5_component_ready_review_allowed": owner_recommendation
        == "PROMOTE_TO_COMPONENT_REVIEW",
        "6_continue_block_paper_shadow_production": True,
        "7_next_minimum_task": "owner_review_dual_track_roadmap_and_growth_decision_pack",
    }
    payload = _payload(
        report_type="research_roadmap_v2_master_review",
        title="Research Roadmap V2 Master Review",
        status=status,
        summary={
            "primary_conclusion": status,
            "equal_risk_continuity_status": continuity.get("status"),
            "scoreboard_status": scoreboard.get("status"),
            "growth_owner_recommendation": owner_recommendation,
            **_safety_summary(),
        },
        final_conclusions=[
            "CONTINUE_EQUAL_RISK_FORWARD_AGING",
            status,
            "KEEP_ALL_RESEARCH_ONLY",
        ],
        required_answers=final_answers,
        source_statuses={
            "equal_risk_observation_continuity_check": continuity.get("status"),
            "equal_risk_forward_aging_scoreboard_safety_gate": scoreboard.get("status"),
            "growth_component_owner_decision_pack": owner.get("status"),
            "equal_risk_and_growth_dual_track_roadmap": roadmap.get("status"),
        },
        source_artifacts={
            "equal_risk_observation_continuity_check": continuity.get("artifact_paths", {}),
            "equal_risk_forward_aging_scoreboard_safety_gate": scoreboard.get(
                "artifact_paths", {}
            ),
            "growth_component_owner_decision_pack": owner.get("artifact_paths", {}),
            "equal_risk_and_growth_dual_track_roadmap": roadmap.get("artifact_paths", {}),
        },
        owner_next_action="review_roadmap_v2_and_keep_research_only_boundaries",
        report_registry_entry=_report_registry_entry(
            "research_roadmap_v2_master_review",
            "Research Roadmap V2 Master Review",
            "aits research strategies research-roadmap-v2-master-review",
            "research_roadmap_v2_master_review",
            output_subdir="roadmap",
            extra_artifact_globs=["docs/research/research_roadmap_v2_master_review.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Research Roadmap V2 Master Review")
    payload["roadmap_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def _run_growth_search(
    *,
    report_type: str,
    title: str,
    command: str,
    status_found: str,
    status_no_edge: str,
    status_blocked: str,
    candidates: list[dict[str, Any]],
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config: Mapping[str, Any],
    output_root: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
    extra_payload: Mapping[str, Any],
) -> dict[str, Any]:
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type=report_type,
            title=title,
            status=status_blocked,
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, report_type)
        return payload
    rows = _metric_rows(
        candidates,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    edge_rows = [row for row in rows if _float(row.get("beta_adjusted_edge")) > 0.0]
    status = status_found if edge_rows else status_no_edge
    payload = _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "candidate_count": len(rows),
            "beta_adjusted_edge_count": len(edge_rows),
            "top_candidate": _best_candidate_row(rows).get("candidate_id") if rows else None,
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        data_quality=data_gate,
        candidate_results=rows,
        requested_date_range=_requested_range(rows, start_date, end_date),
        report_registry_entry=_report_registry_entry(report_type, title, command, report_type),
        **dict(extra_payload),
    )
    _write_pair(payload, output_root, report_type)
    return payload


def _metric_rows(
    candidates: list[dict[str, Any]],
    *,
    prices_path: Path,
    config: Mapping[str, Any],
    start_date: date,
    end_date: date | None,
) -> list[dict[str, Any]]:
    base = _run_metric_set(
        [_with_controlled_defaults(row) for row in candidates],
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    benchmarks = _run_metric_set(
        _benchmark_specs(config),
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    qqq = next((row for row in benchmarks if row.get("strategy_id") == "100_qqq"), {})
    enriched = [_controlled_result_row(row, qqq, config) for row in base]
    return sorted(enriched, key=lambda row: _float(row.get("beta_adjusted_edge")), reverse=True)


def _controlled_result_row(
    row: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    qqq_return = _float(qqq_metrics.get("annual_return"))
    raw_edge = _float(row.get("annual_return_vs_qqq"))
    beta = _float(row.get("effective_qqq_beta"))
    beta_explained = max(beta - 1.0, 0.0) * qqq_return
    beta_adjusted = raw_edge - beta_explained
    policy = _research_mapping(config, "beta_adjusted_edge_contract")
    turnover_penalty = max(
        _float(row.get("turnover")) - _candidate_limit(config, "max_switches_per_year"),
        0.0,
    ) * _float(policy.get("turnover_penalty_weight"))
    drawdown_penalty = max(_float(row.get("drawdown_penalty")), 0.0) * _float(
        policy.get("drawdown_penalty_weight")
    )
    path_penalty = _float(row.get("path_dependency_risk")) * _float(
        policy.get("path_dependency_penalty_weight")
    )
    net_edge = beta_adjusted - turnover_penalty - drawdown_penalty - path_penalty
    switch_count = _int(row.get("rebalance_count"))
    years = _years_from_range(str(row.get("requested_date_range")))
    calmar_edge = _float(row.get("calmar_edge"))
    sharpe_edge = _float(row.get("sharpe_edge"))
    dominance = (
        "DOMINATED_BY_100_QQQ"
        if raw_edge <= 0.0 and calmar_edge <= 0.0 and sharpe_edge <= 0.0
        else "CANDIDATE_NON_DOMINATED"
    )
    return {
        "candidate_id": row.get("strategy_id"),
        "strategy_id": row.get("strategy_id"),
        "annual_return": row.get("annual_return"),
        "return_edge_vs_100_qqq": row.get("annual_return_vs_qqq"),
        "raw_return_edge_vs_100_qqq": row.get("annual_return_vs_qqq"),
        "max_drawdown": row.get("max_drawdown"),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "turnover": row.get("turnover"),
        "switch_count": switch_count,
        "switches_per_year": _round(_ratio(switch_count, years)),
        "effective_qqq_beta": row.get("effective_qqq_beta"),
        "beta_adjusted_edge": _round(beta_adjusted),
        "beta_adjusted_return_edge": _round(beta_adjusted),
        "beta_adjusted_calmar_edge": _round(calmar_edge - max(beta - 1.0, 0.0)),
        "beta_adjusted_sharpe_edge": _round(sharpe_edge - max(beta - 1.0, 0.0)),
        "drawdown_penalty": _round(drawdown_penalty),
        "turnover_penalty": _round(turnover_penalty),
        "path_dependency_penalty": _round(path_penalty),
        "net_edge_after_penalty": _round(net_edge),
        "dominance_status": dominance,
        "calmar_edge": row.get("calmar_edge"),
        "sharpe_edge": row.get("sharpe_edge"),
        "average_weights": row.get("average_weights"),
        "max_tqqq_weight": row.get("max_tqqq_weight_observed"),
        "policy_hash": row.get("policy_hash") or _stable_hash(row),
        "definition_hash": row.get("policy_hash") or _stable_hash(row),
        "blocked_reasons": row.get("blocked_reasons", []),
    }


def _attribution_row(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    weights: pd.DataFrame,
    metrics: Mapping[str, Any],
    qqq_returns: pd.Series,
    qqq_annual: float,
) -> dict[str, Any]:
    avg = _mapping(metrics.get("average_weights"))
    tqqq_weight = _float(avg.get("TQQQ"))
    qqq_weight = _float(avg.get("QQQ"))
    sgov_weight = _float(avg.get("SGOV"))
    beta = _beta(returns, qqq_returns)
    qqq_beta_attr = beta * qqq_annual
    overlay_attr = tqqq_weight * qqq_annual * (TQQQ_DAILY_LEVERAGE_MULTIPLIER - 1.0)
    sgov_attr = sgov_weight * _float(returns.mean() * 252)
    timing = _float(metrics.get("annual_return")) - qqq_beta_attr - overlay_attr - sgov_attr
    leverage_drag = min(0.0, overlay_attr - tqqq_weight * qqq_annual)
    return {
        "candidate_id": candidate.get("strategy_id"),
        "effective_qqq_beta": _round(beta),
        "effective_leverage": _round(qqq_weight + tqqq_weight * TQQQ_DAILY_LEVERAGE_MULTIPLIER),
        "average_tqqq_weight": _round(tqqq_weight),
        "max_tqqq_weight": _round(
            _float(weights.get("TQQQ", pd.Series(0.0, index=weights.index)).max())
        ),
        "average_sgov_weight": _round(sgov_weight),
        "return_attribution_qqq_beta": _round(qqq_beta_attr),
        "return_attribution_tqqq_overlay": _round(overlay_attr),
        "return_attribution_timing": _round(timing),
        "return_attribution_sgov_carry": _round(sgov_attr),
        "cash_drag": _round(sgov_weight),
        "leverage_drag": _round(leverage_drag),
        "path_dependency_commentary": (
            "beta dominates observed return edge"
            if beta > 1.0 and abs(qqq_beta_attr) > abs(timing)
            else "timing or risk-control contribution remains visible"
        ),
    }


def _selected_candidates(config: Mapping[str, Any], output_root: Path) -> list[dict[str, Any]]:
    source = _read_json_or_empty(output_root / "low_turnover_controlled_growth_search.json")
    selected_ids = [
        str(row.get("candidate_id"))
        for row in _records(source.get("candidate_results"))
        if row.get("dominance_status") != "DOMINATED_BY_100_QQQ"
    ][:5]
    candidates = _growth_candidate_specs(config)
    low_turnover_rows = _records(config.get("low_turnover_controlled_growth_candidates"))
    if not selected_ids:
        selected_ids = [str(row.get("strategy_id")) for row in candidates[:5]]
    lookup = {str(row.get("strategy_id")): row for row in [*candidates, *low_turnover_rows]}
    return [lookup[item] for item in selected_ids if item in lookup]


def _period_validation_summary(
    candidate: Mapping[str, Any],
    period_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    covered = [row for row in period_rows if row.get("coverage_status") == "COVERED"]
    wins = [row for row in covered if _float(row.get("annual_return_vs_qqq")) > 0.0]
    ai_wins = [
        row
        for row in wins
        if str(row.get("period_id")) in {"2023_recovery", "2024_ai_rally", "ai_rally"}
    ]
    return {
        "candidate_id": candidate.get("strategy_id"),
        "covered_period_count": len(covered),
        "win_period_count": len(wins),
        "only_ai_rally_effective": bool(wins) and len(wins) == len(ai_wins),
        "most_periods_beat_100_qqq": len(wins) > len(covered) / 2 if covered else False,
    }


def _period_required_answers(
    summaries: list[Mapping[str, Any]],
    drawdown_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "1_only_ai_rally_effective": any(row.get("only_ai_rally_effective") for row in summaries),
        "2_2022_or_stress_drawdown_too_high": any(
            row.get("covered") and _float(row.get("max_drawdown_vs_qqq")) < -0.05
            for row in drawdown_rows
        ),
        "3_missed_major_rebound": any(row.get("risk_on_too_slow") for row in drawdown_rows),
        "4_most_periods_beat_100_qqq": any(
            row.get("most_periods_beat_100_qqq") for row in summaries
        ),
        "5_risk_adjusted_value_remains": any(
            not row.get("only_ai_rally_effective") and row.get("most_periods_beat_100_qqq")
            for row in summaries
        ),
    }


def _cost_sensitivity_rows(
    row: Mapping[str, Any], config: Mapping[str, Any]
) -> list[dict[str, Any]]:
    scenarios = _research_mapping(config, "cost_turnover_sensitivity")
    cost_bps = _mapping(scenarios.get("cost_bps"))
    lags = _mapping(scenarios.get("execution_lag_penalty"))
    rebalance_penalty = _mapping(scenarios.get("rebalance_penalty"))
    rows = []
    for scenario, bps in cost_bps.items():
        degradation = _float(row.get("turnover")) * _float(bps) / 10000.0
        rows.append(_sensitivity_row(row, scenario, degradation))
    for scenario, penalty in lags.items():
        rows.append(_sensitivity_row(row, scenario, _float(penalty)))
    for scenario, penalty in rebalance_penalty.items():
        rows.append(_sensitivity_row(row, scenario, _float(penalty)))
    return rows


def _sensitivity_row(
    row: Mapping[str, Any],
    scenario: str,
    degradation: float,
) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "scenario": scenario,
        "base_annual_return": row.get("annual_return"),
        "annual_return_degradation": _round(degradation),
        "scenario_annual_return": _round(_float(row.get("annual_return")) - degradation),
        "turnover": row.get("turnover"),
        "switch_count": row.get("switch_count"),
    }


def _drawdown_episode_rows(
    config: Mapping[str, Any],
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in _records(config.get("drawdown_replay_periods"))]
    if "QQQ" in prices and "TQQQ" in prices:
        rows.extend(_largest_drawdown_episodes(prices))
    return rows


def _largest_drawdown_episodes(prices: pd.DataFrame) -> list[dict[str, str]]:
    episodes = []
    for ticker in ("QQQ", "TQQQ"):
        returns = prices[ticker].pct_change().fillna(0.0)
        equity = (1.0 + returns).cumprod()
        drawdown = equity / equity.cummax() - 1.0
        trough = drawdown.idxmin()
        peak = equity.loc[:trough].idxmax()
        episodes.append(
            {
                "episode_id": f"largest_{ticker.lower()}_drawdown",
                "start": pd.Timestamp(peak).date().isoformat(),
                "end": pd.Timestamp(trough).date().isoformat(),
            }
        )
    return episodes


def _price_matrix(
    prices_path: Path,
    config: Mapping[str, Any],
    *,
    start_date: date,
    end_date: date | None,
) -> pd.DataFrame:
    prices = _load_price_matrix(prices_path, _required_tickers(config))
    return _slice_prices(prices, start_date=start_date, end_date=end_date)


def _data_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config: Mapping[str, Any],
    as_of_date: date | None,
) -> dict[str, Any]:
    return _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )


def _load_config(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"controlled growth registry must be a mapping: {path}")
    return dict(raw)


def _required_tickers(config: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in _research_policy(config).get("required_price_tickers", [])]


def _research_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(config.get("research_policy"))


def _annualization(config: Mapping[str, Any]) -> int:
    return _int(_research_policy(config).get("annualization_trading_days"), 252)


def _with_controlled_defaults(row: Mapping[str, Any]) -> dict[str, Any]:
    target = _mapping(row.get("target_weights"))
    risk_on = _mapping(row.get("risk_on_weights"))
    overlay = _mapping(row.get("overlay_weights"))
    max_tqqq = max(
        [_float(target.get("TQQQ")), _float(risk_on.get("TQQQ")), _float(overlay.get("TQQQ"))]
    )
    return {
        "display_name": row.get("display_name", row.get("strategy_id")),
        "candidate_role": row.get("candidate_role", "growth_challenger"),
        "candidate_type": row.get("candidate_type", "growth_challenger"),
        "rebalance_frequency": row.get("rebalance_frequency", "monthly"),
        "uses_options": False,
        "uses_margin": False,
        "uses_leverage_etf": max_tqqq > 0,
        "max_tqqq_weight": max_tqqq,
        "production_effect": "none",
        "broker_action": "none",
        **dict(row),
    }


def _registry_v2_issues(
    config: Mapping[str, Any],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues = []
    limits = _research_mapping(config, "candidate_limits")
    if _float(limits.get("max_tqqq_weight")) > 0.25:
        issues.append({"issue_id": "default_max_tqqq_weight_above_25pct"})
    if _float(limits.get("max_effective_qqq_beta")) > 1.5:
        issues.append({"issue_id": "default_max_effective_qqq_beta_above_1_5"})
    if _int(limits.get("minimum_holding_period_trading_days")) < 20:
        issues.append({"issue_id": "minimum_holding_period_below_20d"})
    if _float(limits.get("max_switches_per_year")) > 4:
        issues.append({"issue_id": "max_switches_per_year_above_4"})
    seen: set[str] = set()
    for candidate in candidates:
        candidate_id = str(candidate.get("strategy_id"))
        if candidate_id in seen:
            issues.append({"strategy_id": candidate_id, "issue_id": "duplicate_strategy_id"})
        seen.add(candidate_id)
        if candidate.get("uses_options") is not False:
            issues.append({"strategy_id": candidate_id, "issue_id": "uses_options_not_false"})
        if candidate.get("uses_margin") is not False:
            issues.append({"strategy_id": candidate_id, "issue_id": "uses_margin_not_false"})
        max_tqqq = _max_declared_tqqq_weight(candidate)
        if max_tqqq > _float(limits.get("max_tqqq_weight")):
            issues.append({"strategy_id": candidate_id, "issue_id": "max_tqqq_weight_exceeded"})
        if _static_effective_beta(candidate) > _float(limits.get("max_effective_qqq_beta")):
            issues.append(
                {"strategy_id": candidate_id, "issue_id": "effective_qqq_beta_exceeded"}
            )
        if candidate.get("broker_action") != "none":
            issues.append({"strategy_id": candidate_id, "issue_id": "broker_action_not_none"})
    return issues


def _unsafe_config_blockers(config: Mapping[str, Any]) -> list[str]:
    safety = _mapping(config.get("safety_boundary"))
    blockers = []
    required_exclusions = {
        "restore_old_qqq_plus_growth_as_selectable",
        "tqqq_heavy_mainline",
        "LEAPS",
        "Wheel",
        "tail_risk_fallback",
        "options_overlay",
    }
    excluded_paths = {str(item) for item in config.get("excluded_paths", [])}
    if safety.get("paper_shadow_allowed") is not False:
        blockers.append("paper_shadow_allowed_not_false")
    if safety.get("production_allowed") is not False:
        blockers.append("production_allowed_not_false")
    if safety.get("broker_action") != "none":
        blockers.append("broker_action_not_none")
    missing_exclusions = sorted(required_exclusions - excluded_paths)
    if missing_exclusions:
        blockers.append("excluded_paths_missing:" + ",".join(missing_exclusions))
    return blockers


def _minimum_growth_conditions() -> list[str]:
    return [
        "historical_return_materially_above_100_qqq",
        "cost_after_edge_survives",
        "max_drawdown_not_materially_worse",
        "calmar_or_sharpe_not_weaker_than_100_qqq",
        "not_pure_effective_beta_above_one",
        "not_only_ai_rally_effective",
        "turnover_and_switch_count_controlled",
        "definition_hash_locked",
        "data_quality_gate_passed",
        "owner_manual_review",
    ]


def _search_constraints(config: Mapping[str, Any]) -> dict[str, Any]:
    limits = _research_mapping(config, "candidate_limits")
    return {
        "max_tqqq_weight": limits.get("max_tqqq_weight"),
        "max_effective_qqq_beta": limits.get("max_effective_qqq_beta"),
        "minimum_holding_period_trading_days": limits.get(
            "minimum_holding_period_trading_days"
        ),
        "max_switches_per_year": limits.get("max_switches_per_year"),
        "uses_options": False,
        "uses_margin": False,
        "broker_action": "none",
    }


def _max_declared_tqqq_weight(candidate: Mapping[str, Any]) -> float:
    weights = [
        _mapping(candidate.get("target_weights")),
        _mapping(candidate.get("risk_on_weights")),
        _mapping(candidate.get("neutral_weights")),
        _mapping(candidate.get("warning_weights")),
        _mapping(candidate.get("block_weights")),
        _mapping(candidate.get("defensive_weights")),
        _mapping(candidate.get("overlay_weights")),
    ]
    return max([_float(row.get("TQQQ")) for row in weights] or [0.0])


def _static_effective_beta(candidate: Mapping[str, Any]) -> float:
    weights = (
        _normalise_weights(_mapping(candidate.get("target_weights")))
        or _normalise_weights(_mapping(candidate.get("risk_on_weights")))
        or _normalise_weights(_mapping(candidate.get("overlay_weights")))
    )
    return _float(weights.get("QQQ")) + _float(weights.get("TQQQ")) * 3.0


def _best_candidate_row(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    return dict(
        sorted(rows, key=lambda row: _float(row.get("beta_adjusted_edge")), reverse=True)[0]
    )


def _requested_range(rows: list[dict[str, Any]], start_date: date, end_date: date | None) -> str:
    if rows:
        return str(rows[0].get("requested_date_range"))
    end = "open" if end_date is None else end_date.isoformat()
    return f"{start_date.isoformat()}..{end}"


def _years_from_range(value: str) -> float:
    try:
        start_raw, end_raw = value.split("..", 1)
        start = date.fromisoformat(start_raw)
        end = date.fromisoformat(end_raw)
    except (ValueError, AttributeError):
        return 1.0
    return max((end - start).days / 365.25, 1.0 / 252.0)


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    aligned.columns = ["strategy", "benchmark"]
    variance = float(aligned["benchmark"].var(ddof=0))
    if abs(variance) <= 1e-12:
        return 0.0
    return float(aligned["strategy"].cov(aligned["benchmark"]) / variance)


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _blocked_payload(
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
            **_safety_summary(),
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


def _write_pair(payload: dict[str, Any], output_root: Path, artifact_id: str) -> None:
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_owner_doc(payload: Mapping[str, Any], path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        f"# {title}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- owner_recommendation：`{payload.get('owner_recommendation', 'N/A')}`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- manual_review_required：`true`",
        "",
        "## Required Answers",
        "",
    ]
    if answers:
        lines.extend(f"- `{key}`: `{value}`" for key, value in answers.items())
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "本报告仅用于 research-only owner review，不生成交易建议、paper-shadow "
            "activation、production config mutation 或 broker action。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    *,
    output_subdir: str = "growth_components",
    extra_artifact_globs: list[str] | None = None,
) -> dict[str, Any]:
    globs = [
        f"outputs/research_strategies/{output_subdir}/{artifact_slug}.json",
        f"outputs/research_strategies/{output_subdir}/{artifact_slug}.md",
    ]
    globs.extend(extra_artifact_globs or [])
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": globs,
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "TRADING-1031 to 1048 controlled growth component artifacts are "
            "regenerated after candidate registry, data quality, search, validation, "
            "readiness, owner review, or roadmap state changes."
        ),
        "owner_action": "review_controlled_growth_component_research_only_artifact",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _safety_summary() -> dict[str, Any]:
    return {
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if abs(denominator) > 1e-12 else 0.0


def _round(value: object) -> float:
    return round(_float(value), 6)
