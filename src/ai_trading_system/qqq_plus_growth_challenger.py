from __future__ import annotations

import hashlib
import json
import math
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
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    SAFETY_BOUNDARY,
    _data_quality_gate,
    _load_price_matrix,
    _metrics_for_strategy,
    _records,
    _slice_prices,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "qqq_plus_growth_candidate_registry.yaml"
)
DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "qqq_plus_growth"
)
DEFAULT_QQQ_OUTPERFORMANCE_OWNER_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "qqq_outperformance_owner_decision_pack.md"
)
DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)

PRIMARY_BENCHMARK_ID = "100_qqq"
DEFENSIVE_PRIMARY_ID = "equal_risk_qqq_sgov"
STATIC_COMPARATOR_ID = "qqq_60_sgov_40"
EXISTING_CHALLENGER_ID = "dyn_tqqq_capped_trend"
TQQQ_DAILY_LEVERAGE_MULTIPLIER = 3.0


def run_qqq_outperformance_objective_contract(
    *,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    policy = _objective_policy(config)
    missing = [
        field
        for field in (
            "primary_benchmark",
            "secondary_benchmarks",
            "annual_return_edge_min",
            "annual_return_edge_material",
            "max_drawdown_multiplier_vs_qqq",
            "turnover_acceptability_limit",
            "max_regime_win_concentration",
        )
        if field not in policy
    ]
    if not policy:
        status = "QQQ_OUTPERFORMANCE_CONTRACT_BLOCKED"
    elif missing:
        status = "QQQ_OUTPERFORMANCE_CONTRACT_PARTIAL"
    else:
        status = "QQQ_OUTPERFORMANCE_CONTRACT_READY"

    payload = _payload(
        report_type="qqq_outperformance_objective_contract",
        title="QQQ Outperformance Objective Contract",
        status=status,
        summary={
            "primary_benchmark": policy.get("primary_benchmark"),
            "secondary_benchmark_count": len(list(policy.get("secondary_benchmarks", []))),
            "annual_return_edge_min": policy.get("annual_return_edge_min"),
            "annual_return_edge_material": policy.get("annual_return_edge_material"),
            "missing_field_count": len(missing),
        },
        objective_contract={
            "primary_benchmark": policy.get("primary_benchmark"),
            "secondary_benchmarks": list(policy.get("secondary_benchmarks", [])),
            "minimum_research_hurdles": {
                "annual_return": (
                    "candidate annual_return >= 100_qqq annual_return + "
                    f"{policy.get('annual_return_edge_min')}"
                ),
                "calmar": "candidate Calmar >= 100_qqq Calmar",
                "sharpe": (
                    "candidate Sharpe >= 100_qqq Sharpe or drawdown-adjusted improvement"
                ),
                "max_drawdown": (
                    "abs(candidate max_drawdown) <= abs(100_qqq max_drawdown) * "
                    f"{policy.get('max_drawdown_multiplier_vs_qqq')}"
                ),
                "turnover": (
                    "candidate turnover <= "
                    f"{policy.get('turnover_acceptability_limit')}"
                ),
                "regime": (
                    "wins must not be concentrated in AI rally or post-2020 only"
                ),
            },
            "status_values": [
                "QQQ_OUTPERFORMANCE_CONTRACT_READY",
                "QQQ_OUTPERFORMANCE_CONTRACT_PARTIAL",
                "QQQ_OUTPERFORMANCE_CONTRACT_BLOCKED",
            ],
            "policy_metadata": config.get("policy_metadata"),
        },
        blockers=missing,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "qqq_outperformance_objective_contract",
            "QQQ Outperformance Objective Contract",
            "aits research strategies qqq-outperformance-objective-contract",
            "qqq_outperformance_objective_contract",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_qqq_plus_growth_candidate_registry(
    *,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    candidates = _all_candidate_specs(config, include_benchmarks=True)
    growth_candidates = _growth_candidate_specs(config)
    issues = _registry_issues(config, candidates)
    if not candidates:
        status = "QQQ_PLUS_GROWTH_REGISTRY_BLOCKED"
    elif issues:
        status = "QQQ_PLUS_GROWTH_REGISTRY_PARTIAL"
    else:
        status = "QQQ_PLUS_GROWTH_REGISTRY_READY"

    payload = _payload(
        report_type="qqq_plus_growth_candidate_registry",
        title="QQQ Plus Growth Candidate Registry",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "growth_candidate_count": len(growth_candidates),
            "candidate_type_count": len(list(config.get("candidate_types", []))),
            "issue_count": len(issues),
            "config_path": str(config_path),
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
            "qqq_plus_growth_candidate_registry",
            "QQQ Plus Growth Candidate Registry",
            "aits research strategies qqq-plus-growth-candidate-registry",
            "qqq_plus_growth_candidate_registry",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_controlled_tqqq_overlay_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    rows = [dict(row) for row in _records(config.get("controlled_tqqq_overlay_candidates"))]
    return _run_search_payload(
        report_type="controlled_tqqq_overlay_search",
        title="Controlled TQQQ Overlay Search",
        command="aits research strategies controlled-tqqq-overlay-search",
        status_ready="CONTROLLED_TQQQ_OVERLAY_SEARCH_READY",
        status_blocked="CONTROLLED_TQQQ_OVERLAY_SEARCH_BLOCKED",
        candidate_rows=rows,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={
            "required_outputs": [
                "annual_return_vs_qqq",
                "max_drawdown_vs_qqq",
                "effective_qqq_exposure",
                "tqqq_contribution",
                "leverage_drag",
                "path_dependency_risk",
                "calmar_edge",
            ],
        },
    )


def run_trend_gated_leverage_policy_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    rows = [dict(row) for row in _records(config.get("trend_gated_leverage_candidates"))]
    return _run_search_payload(
        report_type="trend_gated_leverage_policy_search",
        title="Trend-Gated Leverage Policy Search",
        command="aits research strategies trend-gated-leverage-policy-search",
        status_ready="TREND_GATED_LEVERAGE_SEARCH_READY",
        status_blocked="TREND_GATED_LEVERAGE_SEARCH_BLOCKED",
        candidate_rows=rows,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={
            "allowed_signals": [
                "QQQ above 100DMA",
                "QQQ above 200DMA",
                "100DMA > 200DMA",
                "drawdown from 252d high",
                "realized volatility percentile",
            ],
            "forbidden_inputs": _forbidden_inputs(),
        },
    )


def run_volatility_targeted_growth_policy_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    rows = _vol_target_candidate_rows(config)
    return _run_search_payload(
        report_type="volatility_targeted_growth_policy_search",
        title="Volatility-Targeted Growth Policy Search",
        command="aits research strategies volatility-targeted-growth-policy-search",
        status_ready="VOL_TARGETED_GROWTH_SEARCH_READY",
        status_blocked="VOL_TARGETED_GROWTH_SEARCH_BLOCKED",
        candidate_rows=rows,
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


def run_drawdown_guarded_growth_policy_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    rows = [dict(row) for row in _records(config.get("drawdown_guarded_growth_candidates"))]
    return _run_search_payload(
        report_type="drawdown_guarded_growth_policy_search",
        title="Drawdown-Guarded Growth Policy Search",
        command="aits research strategies drawdown-guarded-growth-policy-search",
        status_ready="DRAWDOWN_GUARDED_GROWTH_SEARCH_READY",
        status_blocked="DRAWDOWN_GUARDED_GROWTH_SEARCH_BLOCKED",
        candidate_rows=rows,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={
            "review_questions": {
                "avoid_2022_drawdown": "covered by drawdown replay",
                "miss_2023_2024_rebound": "covered by period split and replay",
                "over_switching": "covered by turnover and rebalance_count",
                "higher_calmar_than_qqq": "covered by calmar_edge",
            }
        },
    )


def run_qqq_outperformance_ranking_report(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
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
            report_type="qqq_outperformance_ranking_report",
            title="QQQ Outperformance Ranking Report",
            status="QQQ_OUTPERFORMANCE_RANKING_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    benchmarks = _run_metric_set(
        _benchmark_specs(config),
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    source_paths = _search_artifact_paths(output_root)
    source_payloads = {name: _read_json_or_empty(path) for name, path in source_paths.items()}
    candidates = _collect_candidate_results(source_payloads)
    rows = _dedupe_best_by_strategy([*benchmarks, *candidates])
    missing_inputs = [name for name, payload in source_payloads.items() if not payload]
    if not rows:
        status = "QQQ_OUTPERFORMANCE_RANKING_BLOCKED"
    elif missing_inputs:
        status = "QQQ_OUTPERFORMANCE_RANKING_PARTIAL"
    else:
        status = "QQQ_OUTPERFORMANCE_RANKING_READY"

    ranked = [_ranking_row(row, config) for row in rows]
    dominated = _dominated_rows(ranked)
    non_dominated = [row for row in ranked if row["strategy_id"] not in dominated]
    watchlist = _growth_watchlist_rows(non_dominated, config)
    payload = _payload(
        report_type="qqq_outperformance_ranking_report",
        title="QQQ Outperformance Ranking Report",
        status=status,
        summary={
            "candidate_count": len(rows),
            "growth_candidate_count": sum(_is_growth_candidate(row) for row in rows),
            "non_dominated_count": len(non_dominated),
            "dominated_count": len(dominated),
            "watchlist_count": len(watchlist),
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        comparison_objects=[
            "100_qqq",
            "equal_risk_qqq_sgov",
            "qqq_60_sgov_40",
            "dyn_tqqq_capped_trend",
            "controlled_tqqq_overlay candidates",
            "trend-gated leverage candidates",
            "vol-targeted growth candidates",
            "drawdown-guarded growth candidates",
        ],
        ranking_rows=ranked,
        top_by_annual_return=_top(ranked, "annual_return"),
        top_by_calmar=_top(ranked, "calmar"),
        top_by_sharpe=_top(ranked, "sharpe"),
        top_by_return_over_qqq=_top(ranked, "annual_return_vs_qqq"),
        top_by_return_with_drawdown_constraint=[
            row for row in _top(ranked, "annual_return_vs_qqq", limit=len(ranked))
            if row.get("drawdown_constraint_passed")
        ][:10],
        dominated_candidates=dominated,
        non_dominated_candidates=non_dominated,
        growth_candidate_watchlist=watchlist,
        missing_inputs=missing_inputs,
        input_artifacts={name: str(path) for name, path in source_paths.items()},
        requested_date_range=_requested_range(rows, start_date, end_date),
        report_registry_entry=_report_registry_entry(
            "qqq_outperformance_ranking_report",
            "QQQ Outperformance Ranking Report",
            "aits research strategies qqq-outperformance-ranking-report",
            "qqq_outperformance_ranking_report",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_qqq_outperformance_period_split_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
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
            report_type="qqq_outperformance_period_split_validation",
            title="QQQ Outperformance Period Split Validation",
            status="QQQ_OUTPERFORMANCE_PERIOD_SPLIT_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    candidates = _selected_growth_candidates(config, output_root)
    qqq_strategy = _strategy_by_id(_benchmark_specs(config), PRIMARY_BENCHMARK_ID)
    qqq_returns, qqq_weights = _returns_and_weights(qqq_strategy, prices, config)
    rows = []
    candidate_summaries = []
    period_stability_policy = _research_mapping(config, "period_stability")
    concentration_period_ids = {
        str(item) for item in period_stability_policy.get("regime_concentration_period_ids", [])
    }
    for candidate in candidates:
        returns, weights = _returns_and_weights(candidate, prices, config)
        wins = 0
        valid_periods = 0
        ai_rally_wins = 0
        for period in _records(config.get("periods")):
            period_row = _period_row(
                candidate,
                returns,
                weights,
                qqq_returns,
                qqq_weights,
                period,
                config,
            )
            rows.append(period_row)
            if period_row["coverage_status"] == "COVERED":
                valid_periods += 1
                if period_row["annual_return_vs_qqq"] > 0:
                    wins += 1
                    if period_row["period_id"] in concentration_period_ids:
                        ai_rally_wins += 1
        summary = {
            "strategy_id": candidate["strategy_id"],
            "valid_period_count": valid_periods,
            "win_period_count": wins,
            "ai_rally_win_count": ai_rally_wins,
            "stability_flag": _period_stability_flag(
                wins, valid_periods, ai_rally_wins, config
            ),
        }
        candidate_summaries.append(summary)

    flags = {row["stability_flag"] for row in candidate_summaries}
    if "REGIME_CONCENTRATED" in flags:
        status = "REGIME_CONCENTRATED"
    elif "QQQ_OUTPERFORMANCE_NOT_STABLE" in flags:
        status = "QQQ_OUTPERFORMANCE_NOT_STABLE"
    else:
        status = "QQQ_OUTPERFORMANCE_PERIOD_SPLIT_PASS"

    payload = _payload(
        report_type="qqq_outperformance_period_split_validation",
        title="QQQ Outperformance Period Split Validation",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "period_row_count": len(rows),
            "regime_concentrated_count": sum(
                row["stability_flag"] == "REGIME_CONCENTRATED"
                for row in candidate_summaries
            ),
            "not_stable_count": sum(
                row["stability_flag"] == "QQQ_OUTPERFORMANCE_NOT_STABLE"
                for row in candidate_summaries
            ),
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        candidate_summaries=candidate_summaries,
        period_results=rows,
        requested_date_range=_date_range(prices),
        report_registry_entry=_report_registry_entry(
            "qqq_outperformance_period_split_validation",
            "QQQ Outperformance Period Split Validation",
            "aits research strategies qqq-outperformance-period-split-validation",
            "qqq_outperformance_period_split_validation",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_qqq_outperformance_drawdown_replay(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
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
            report_type="qqq_outperformance_drawdown_replay",
            title="QQQ Outperformance Drawdown Replay",
            status="QQQ_OUTPERFORMANCE_DRAWDOWN_REPLAY_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    candidates = _selected_growth_candidates(config, output_root)
    episodes = [dict(row) for row in _records(config.get("drawdown_replay_periods"))]
    episodes.extend(_largest_drawdown_episodes(prices))
    rows = []
    for candidate in candidates:
        returns, weights = _returns_and_weights(candidate, prices, config)
        qqq_returns = prices["QQQ"].pct_change().fillna(0.0).reindex(returns.index)
        for episode in episodes:
            rows.append(
                _drawdown_replay_row(candidate, returns, weights, qqq_returns, episode, config)
            )

    unacceptable = sum(bool(row.get("tqqq_drawdown_unacceptable")) for row in rows)
    status = (
        "QQQ_OUTPERFORMANCE_DRAWDOWN_REPLAY_MIXED"
        if unacceptable
        else "QQQ_OUTPERFORMANCE_DRAWDOWN_REPLAY_READY"
    )
    payload = _payload(
        report_type="qqq_outperformance_drawdown_replay",
        title="QQQ Outperformance Drawdown Replay",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "episode_count": len(episodes),
            "replay_row_count": len(rows),
            "unacceptable_tqqq_drawdown_count": unacceptable,
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        episode_results=rows,
        required_answers={
            "1_tqqq_caused_unacceptable_drawdown": unacceptable > 0,
            "2_risk_off_timely": all(row["risk_off_timely"] for row in rows if row["covered"]),
            "3_risk_on_too_slow": any(row["risk_on_too_slow"] for row in rows if row["covered"]),
            "4_true_improvement_vs_qqq": any(
                row["strategy_return_vs_qqq"] > 0 and row["max_drawdown_vs_qqq"] >= 0
                for row in rows
                if row["covered"]
            ),
        },
        report_registry_entry=_report_registry_entry(
            "qqq_outperformance_drawdown_replay",
            "QQQ Outperformance Drawdown Replay",
            "aits research strategies qqq-outperformance-drawdown-replay",
            "qqq_outperformance_drawdown_replay",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_growth_edge_significance_review(
    *,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    ranking = _read_json_or_empty(output_root / "qqq_outperformance_ranking_report.json")
    period = _read_json_or_empty(
        output_root / "qqq_outperformance_period_split_validation.json"
    )
    rows = _records(ranking.get("ranking_rows"))
    growth_rows = [row for row in rows if _is_growth_candidate(row)]
    review_rows = [_edge_review_row(row, config, period) for row in growth_rows]
    if not ranking:
        status = "GROWTH_EDGE_BLOCKED"
    elif any(row["edge_status"] == "GROWTH_EDGE_MATERIAL" for row in review_rows):
        status = "GROWTH_EDGE_MATERIAL"
    elif any(row["edge_status"] == "GROWTH_EDGE_REGIME_CONCENTRATED" for row in review_rows):
        status = "GROWTH_EDGE_REGIME_CONCENTRATED"
    else:
        status = "GROWTH_EDGE_WEAK"

    payload = _payload(
        report_type="growth_edge_significance_review",
        title="Growth Edge Significance Review",
        status=status,
        summary={
            "candidate_count": len(review_rows),
            "material_count": sum(
                row["edge_status"] == "GROWTH_EDGE_MATERIAL" for row in review_rows
            ),
            "weak_count": sum(row["edge_status"] == "GROWTH_EDGE_WEAK" for row in review_rows),
            "regime_concentrated_count": sum(
                row["edge_status"] == "GROWTH_EDGE_REGIME_CONCENTRATED"
                for row in review_rows
            ),
        },
        edge_review=review_rows,
        input_artifacts={
            "ranking": str(output_root / "qqq_outperformance_ranking_report.json"),
            "period_split": str(
                output_root / "qqq_outperformance_period_split_validation.json"
            ),
        },
        report_registry_entry=_report_registry_entry(
            "growth_edge_significance_review",
            "Growth Edge Significance Review",
            "aits research strategies growth-edge-significance-review",
            "growth_edge_significance_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_growth_candidate_forward_aging_watchlist(
    *,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    edge = _read_json_or_empty(output_root / "growth_edge_significance_review.json")
    ranking = _read_json_or_empty(output_root / "qqq_outperformance_ranking_report.json")
    max_size = _int(_research_mapping(config, "candidate_limits").get("max_growth_watchlist_size"))
    candidate_ids = {
        row["strategy_id"]
        for row in _records(edge.get("edge_review"))
        if row.get("edge_status") == "GROWTH_EDGE_MATERIAL"
    }
    ranked = [
        row
        for row in _records(ranking.get("growth_candidate_watchlist"))
        if row.get("strategy_id") in candidate_ids
    ][:max_size]
    watchlist = [
        {
            "strategy_id": row["strategy_id"],
            "candidate_role": "growth_challenger",
            "comparators": [PRIMARY_BENCHMARK_ID, STATIC_COMPARATOR_ID],
            "manual_review_required": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "source_ranking_score": row.get("growth_ranking_score"),
        }
        for row in ranked
    ]
    status = (
        "GROWTH_FORWARD_AGING_WATCHLIST_READY"
        if watchlist
        else "GROWTH_FORWARD_AGING_WATCHLIST_EMPTY"
    )
    payload = _payload(
        report_type="growth_candidate_forward_aging_watchlist",
        title="Growth Candidate Forward-Aging Watchlist",
        status=status,
        summary={
            "primary_defensive_candidate": DEFENSIVE_PRIMARY_ID,
            "growth_challenger_count": len(watchlist),
            "max_growth_watchlist_size": max_size,
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
        watchlist=watchlist,
        comparators=[PRIMARY_BENCHMARK_ID, STATIC_COMPARATOR_ID],
        input_artifacts={
            "edge_review": str(output_root / "growth_edge_significance_review.json"),
            "ranking": str(output_root / "qqq_outperformance_ranking_report.json"),
        },
        report_registry_entry=_report_registry_entry(
            "growth_candidate_forward_aging_watchlist",
            "Growth Candidate Forward-Aging Watchlist",
            "aits research strategies growth-candidate-forward-aging-watchlist",
            "growth_candidate_forward_aging_watchlist",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_qqq_plus_risk_budget_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
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
            report_type="qqq_plus_risk_budget_review",
            title="QQQ Plus Risk Budget Review",
            status="QQQ_PLUS_RISK_BUDGET_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    candidates = _selected_growth_candidates(config, output_root)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    rows = []
    for candidate in candidates:
        returns, weights = _returns_and_weights(candidate, prices, config)
        metrics = _metrics_for_strategy(
            candidate,
            returns,
            weights,
            qqq_returns,
            annualization=_annualization(config),
            cost_bps=0.0,
        )
        rows.append(_risk_budget_row(candidate, returns, weights, qqq_returns, metrics, config))

    beta_driven = sum(row["outperformance_mostly_beta"] for row in rows)
    status = "QQQ_PLUS_RISK_BUDGET_BETA_HEAVY" if beta_driven else "QQQ_PLUS_RISK_BUDGET_READY"
    payload = _payload(
        report_type="qqq_plus_risk_budget_review",
        title="QQQ Plus Risk Budget Review",
        status=status,
        summary={
            "candidate_count": len(rows),
            "beta_heavy_count": beta_driven,
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        risk_budget=rows,
        required_answer=(
            "收益超过 QQQ 更像 beta > 1" if beta_driven else "候选未全部依赖 beta > 1"
        ),
        report_registry_entry=_report_registry_entry(
            "qqq_plus_risk_budget_review",
            "QQQ Plus Risk Budget Review",
            "aits research strategies qqq-plus-risk-budget-review",
            "qqq_plus_risk_budget_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_growth_vs_defensive_role_allocation_review(
    *,
    config_path: Path = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> dict[str, Any]:
    watchlist = _read_json_or_empty(output_root / "growth_candidate_forward_aging_watchlist.json")
    ranking = _read_json_or_empty(output_root / "qqq_outperformance_ranking_report.json")
    growth_count = len(_records(watchlist.get("watchlist")))
    payload = _payload(
        report_type="growth_vs_defensive_role_allocation_review",
        title="Growth vs Defensive Role Allocation Review",
        status="ROLE_ALLOCATION_READY",
        summary={
            "defensive_primary": DEFENSIVE_PRIMARY_ID,
            "growth_challenger_count": growth_count,
            "risk_reference": PRIMARY_BENCHMARK_ID,
            "static_comparator": STATIC_COMPARATOR_ID,
        },
        role_allocation={
            DEFENSIVE_PRIMARY_ID: "defensive core",
            "qqq_plus_growth_candidate": "growth challenger",
            PRIMARY_BENCHMARK_ID: "risk reference",
            STATIC_COMPARATOR_ID: "static comparator",
            "tqqq_heavy": "paused",
            "LEAPS": "blocked",
            "Wheel": "blocked",
        },
        forbidden_interpretations=[
            "do not replace defensive core with growth challenger",
            "do not restart TQQQ-heavy as a main line",
            "do not interpret watchlist weights as trading advice",
        ],
        input_artifacts={
            "watchlist": str(output_root / "growth_candidate_forward_aging_watchlist.json"),
            "ranking": str(output_root / "qqq_outperformance_ranking_report.json"),
            "config": str(config_path),
        },
        ranking_status=ranking.get("status"),
        report_registry_entry=_report_registry_entry(
            "growth_vs_defensive_role_allocation_review",
            "Growth vs Defensive Role Allocation Review",
            "aits research strategies growth-vs-defensive-role-allocation-review",
            "growth_vs_defensive_role_allocation_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_qqq_outperformance_owner_decision_pack(
    *,
    output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_QQQ_OUTPERFORMANCE_OWNER_DECISION_DOC_PATH,
) -> dict[str, Any]:
    sources = {
        "contract": output_root / "qqq_outperformance_objective_contract.json",
        "registry": output_root / "qqq_plus_growth_candidate_registry.json",
        "ranking": output_root / "qqq_outperformance_ranking_report.json",
        "period_split": output_root / "qqq_outperformance_period_split_validation.json",
        "drawdown_replay": output_root / "qqq_outperformance_drawdown_replay.json",
        "edge_review": output_root / "growth_edge_significance_review.json",
        "watchlist": output_root / "growth_candidate_forward_aging_watchlist.json",
        "risk_budget": output_root / "qqq_plus_risk_budget_review.json",
        "role_allocation": output_root / "growth_vs_defensive_role_allocation_review.json",
    }
    payloads = {name: _read_json_or_empty(path) for name, path in sources.items()}
    missing = [name for name, payload in payloads.items() if not payload]
    ranking_rows = _records(payloads["ranking"].get("growth_candidate_watchlist"))
    watchlist_rows = _records(payloads["watchlist"].get("watchlist"))
    edge_status = str(payloads["edge_review"].get("status"))
    risk_status = str(payloads["risk_budget"].get("status"))
    period_status = str(payloads["period_split"].get("status"))
    drawdown_status = str(payloads["drawdown_replay"].get("status"))
    answers = {
        "1_exists_historical_return_outperformer": bool(ranking_rows),
        "2_edge_material_after_risk": edge_status == "GROWTH_EDGE_MATERIAL",
        "3_only_higher_beta": risk_status == "QQQ_PLUS_RISK_BUDGET_BETA_HEAVY",
        "4_only_ai_rally_effective": period_status == "REGIME_CONCENTRATED",
        "5_max_drawdown_acceptable": drawdown_status
        != "QQQ_OUTPERFORMANCE_DRAWDOWN_REPLAY_MIXED",
        "6_keep_equal_risk_defensive_primary": True,
        "7_add_one_growth_challenger": bool(watchlist_rows[:1]),
        "8_continue_pause_tqqq_heavy": True,
        "9_continue_block_leaps_wheel": True,
        "10_keep_paper_shadow_and_production_false": True,
    }
    if missing:
        status = "QQQ_OUTPERFORMANCE_OWNER_DECISION_PACK_PARTIAL"
    elif answers["2_edge_material_after_risk"] and answers["7_add_one_growth_challenger"]:
        status = "QQQ_OUTPERFORMANCE_OWNER_DECISION_PACK_READY"
    else:
        status = "QQQ_OUTPERFORMANCE_OWNER_DECISION_PACK_NO_GROWTH_APPROVAL"

    payload = _payload(
        report_type="qqq_outperformance_owner_decision_pack",
        title="QQQ Outperformance Owner Decision Pack",
        status=status,
        summary={
            "missing_input_count": len(missing),
            "growth_watchlist_count": len(watchlist_rows),
            "edge_status": edge_status or "MISSING",
            "period_status": period_status or "MISSING",
            "risk_budget_status": risk_status or "MISSING",
        },
        required_answers=answers,
        recommended_owner_action=_owner_action(answers, missing),
        source_statuses={name: payload.get("status") for name, payload in payloads.items()},
        missing_inputs=missing,
        input_artifacts={name: str(path) for name, path in sources.items()},
        report_registry_entry=_report_registry_entry(
            "qqq_outperformance_owner_decision_pack",
            "QQQ Outperformance Owner Decision Pack",
            "aits research strategies qqq-outperformance-owner-decision-pack",
            "qqq_outperformance_owner_decision_pack",
            extra_artifact_globs=["docs/research/qqq_outperformance_owner_decision_pack.md"],
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    _write_owner_doc(payload, docs_path)
    payload["owner_decision_doc_path"] = str(docs_path)
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _run_search_payload(
    *,
    report_type: str,
    title: str,
    command: str,
    status_ready: str,
    status_blocked: str,
    candidate_rows: list[dict[str, Any]],
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
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    candidates = [_with_candidate_defaults(row) for row in candidate_rows]
    rows = _run_metric_set(
        candidates,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    status = status_ready if rows else status_blocked
    payload = _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "candidate_count": len(rows),
            "screen_pass_count": sum(row["objective_screen_passed"] for row in rows),
            "top_candidate": _top(rows, "growth_ranking_score", limit=1)[0]["strategy_id"]
            if rows
            else None,
            "data_quality_status": data_gate["status"],
        },
        data_quality=data_gate,
        candidate_results=rows,
        requested_date_range=_requested_range(rows, start_date, end_date),
        report_registry_entry=_report_registry_entry(report_type, title, command, report_type),
        **dict(extra_payload),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _run_metric_set(
    candidates: list[dict[str, Any]],
    *,
    prices_path: Path,
    config: Mapping[str, Any],
    start_date: date,
    end_date: date | None,
) -> list[dict[str, Any]]:
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    benchmark_strategy = _strategy_by_id(_benchmark_specs(config), PRIMARY_BENCHMARK_ID)
    qqq_metrics = _metric_row(benchmark_strategy, prices, config, qqq_returns)
    rows = []
    for candidate in candidates:
        row = _metric_row(candidate, prices, config, qqq_returns)
        rows.append(_enrich_vs_qqq(row, qqq_metrics, candidate, prices, config))
    return rows


def _metric_row(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    qqq_returns: pd.Series,
) -> dict[str, Any]:
    returns, weights = _returns_and_weights(candidate, prices, config)
    row = _metrics_for_strategy(
        candidate,
        returns,
        weights,
        qqq_returns,
        annualization=_annualization(config),
        cost_bps=0.0,
    )
    row["candidate_role"] = candidate.get("candidate_role", "growth_challenger")
    row["candidate_type"] = candidate.get("candidate_type", "growth_challenger")
    row["policy_hash"] = _stable_hash(candidate)
    return row


def _enrich_vs_qqq(
    row: dict[str, Any],
    qqq_metrics: Mapping[str, Any],
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    returns, weights = _returns_and_weights(candidate, prices, config)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0).reindex(returns.index)
    tqqq_returns = prices["TQQQ"].pct_change().fillna(0.0).reindex(returns.index)
    sgov_returns = prices["SGOV"].pct_change().fillna(0.0).reindex(returns.index)
    avg = _mapping(row.get("average_weights"))
    effective_exposure = _effective_qqq_exposure(avg)
    max_effective_exposure = _max_effective_qqq_exposure(weights)
    tqqq_weight = _float(avg.get("TQQQ"))
    tqqq_annual = _annualized_mean(tqqq_returns, config)
    qqq_annual = _float(qqq_metrics.get("annual_return"))
    tqqq_contribution = _annualized_contribution(weights, tqqq_returns, "TQQQ", config)
    sgov_contribution = _annualized_contribution(weights, sgov_returns, "SGOV", config)
    leverage_drag = tqqq_weight * (tqqq_annual - qqq_annual * TQQQ_DAILY_LEVERAGE_MULTIPLIER)
    annual_edge = _float(row.get("annual_return")) - qqq_annual
    drawdown_penalty = abs(_float(row.get("max_drawdown"))) - abs(
        _float(qqq_metrics.get("max_drawdown"))
    )
    policy = _objective_policy(config)
    drawdown_limit = abs(_float(qqq_metrics.get("max_drawdown"))) * _float(
        policy.get("max_drawdown_multiplier_vs_qqq")
    )
    turnover_limit = _float(policy.get("turnover_acceptability_limit"))
    row.update(
        {
            "primary_benchmark": PRIMARY_BENCHMARK_ID,
            "annual_return_vs_qqq": _round(annual_edge),
            "max_drawdown_vs_qqq": _round(
                abs(_float(qqq_metrics.get("max_drawdown")))
                - abs(_float(row.get("max_drawdown")))
            ),
            "calmar_edge": _round(_float(row.get("calmar")) - _float(qqq_metrics.get("calmar"))),
            "sharpe_edge": _round(_float(row.get("sharpe")) - _float(qqq_metrics.get("sharpe"))),
            "effective_qqq_exposure": _round(effective_exposure),
            "max_effective_qqq_exposure": _round(max_effective_exposure),
            "effective_qqq_beta": _round(_beta(returns, qqq_returns)),
            "tqqq_contribution": _round(tqqq_contribution),
            "sgov_contribution": _round(sgov_contribution),
            "leverage_drag": _round(leverage_drag),
            "path_dependency_risk": _round(tqqq_weight * abs(_float(row.get("max_drawdown")))),
            "drawdown_penalty": _round(max(drawdown_penalty, 0.0)),
            "drawdown_constraint_passed": abs(_float(row.get("max_drawdown"))) <= drawdown_limit,
            "turnover_acceptable": _float(row.get("turnover")) <= turnover_limit,
            "tqqq_weight_constraint_passed": _float(row.get("max_tqqq_weight_observed"))
            <= _candidate_limit(config, "max_tqqq_weight"),
            "effective_exposure_constraint_passed": max_effective_exposure
            <= _candidate_limit(config, "max_effective_qqq_exposure"),
        }
    )
    row["objective_screen_passed"] = _objective_screen_passed(row, qqq_metrics, config)
    row["growth_candidate_band"] = _growth_candidate_band(row, qqq_metrics, config)
    row["growth_ranking_score"] = _growth_ranking_score(row, config)
    row["blocked_reasons"] = _candidate_blockers(row, config)
    return row


def _returns_and_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> tuple[pd.Series, pd.DataFrame]:
    weights = _weight_frame(candidate, prices, config)
    asset_returns = prices.pct_change().fillna(0.0)
    applied = weights.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
    returns = (applied * asset_returns.reindex(columns=applied.columns).fillna(0.0)).sum(axis=1)
    return returns, weights


def _weight_frame(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    special_policy = str(candidate.get("special_policy") or "")
    candidate_type = str(candidate.get("candidate_type") or "")
    if candidate.get("base_strategy") == DEFENSIVE_PRIMARY_ID:
        return _overlay_equal_risk_weights(candidate, prices, config)
    if special_policy == "equal_risk" or str(candidate.get("strategy_id")) == DEFENSIVE_PRIMARY_ID:
        return _equal_risk_weights(prices, config)
    if special_policy == "trend_gated_leverage" or candidate_type == "trend_gated_tqqq_overlay":
        return _trend_gated_weights(candidate, prices, config)
    if candidate_type == "volatility_targeted_qqq_tqqq_sgov":
        return _vol_target_weights(candidate, prices, config)
    if candidate_type == "drawdown_guarded_growth_allocation":
        return _drawdown_guarded_weights(candidate, prices, config)
    target = _normalise_weights(_mapping(candidate.get("target_weights")))
    frame = pd.DataFrame(index=prices.index, columns=sorted(target), data=0.0)
    for ticker, weight in target.items():
        frame[ticker] = weight
    return _apply_rebalance(frame, str(candidate.get("rebalance_frequency", "monthly")))


def _overlay_equal_risk_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    base = _equal_risk_weights(prices, config)
    overlay = _normalise_weights(_mapping(candidate.get("overlay_weights")))
    overlay_total = sum(overlay.values())
    base_scaled = base * max(1.0 - overlay_total, 0.0)
    for ticker, weight in overlay.items():
        base_scaled[ticker] = weight
    return _apply_rebalance(
        base_scaled.fillna(0.0),
        str(candidate.get("rebalance_frequency", "monthly")),
    )


def _equal_risk_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    policy = _research_mapping(config, "equal_risk")
    window = _int(_research_mapping(config, "realized_vol_windows").get("medium"))
    qqq_vol = _realized_vol(prices["QQQ"], window, _annualization(config))
    sgov_vol = _realized_vol(prices["SGOV"], window, _annualization(config))
    inv_qqq = 1.0 / qqq_vol.replace(0.0, math.nan)
    inv_sgov = 1.0 / sgov_vol.replace(0.0, math.nan)
    qqq = (inv_qqq / (inv_qqq + inv_sgov)).clip(
        lower=_float(policy.get("min_weight")), upper=_float(policy.get("max_weight"))
    )
    qqq = qqq.fillna(0.5)
    return _apply_rebalance(
        pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index),
        "monthly",
    )


def _trend_gated_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    state = _trend_state(prices, config)
    risk_on = _normalise_weights(_mapping(candidate.get("risk_on_weights")))
    neutral = _normalise_weights(_mapping(candidate.get("neutral_weights"))) or _normalise_weights(
        _mapping(candidate.get("risk_off_weights"))
    )
    risk_off = _normalise_weights(_mapping(candidate.get("risk_off_weights")))
    tickers = sorted(set(risk_on) | set(neutral) | set(risk_off))
    frame = pd.DataFrame(index=prices.index, columns=tickers, data=0.0)
    for ticker in tickers:
        frame[ticker] = [
            risk_on.get(ticker, 0.0)
            if value == "risk_on"
            else neutral.get(ticker, 0.0)
            if value == "neutral"
            else risk_off.get(ticker, 0.0)
            for value in state
        ]
    return frame.ffill().fillna(0.0)


def _vol_target_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    target_vol = _float(candidate.get("target_vol"))
    window = _int(candidate.get("realized_vol_window"))
    max_tqqq = _float(candidate.get("max_tqqq_weight"))
    min_sgov = _float(candidate.get("min_sgov_weight"))
    vol = _realized_vol(prices["QQQ"], window, _annualization(config)).replace(0.0, math.nan)
    max_effective = _candidate_limit(config, "max_effective_qqq_exposure")
    effective_exposure = (target_vol / vol).clip(lower=0.0, upper=max_effective)
    tqqq = (
        (effective_exposure - 1.0).clip(lower=0.0) / TQQQ_DAILY_LEVERAGE_MULTIPLIER
    ).clip(upper=max_tqqq)
    qqq = (effective_exposure - tqqq * TQQQ_DAILY_LEVERAGE_MULTIPLIER).clip(
        lower=0.0,
        upper=1.0,
    )
    invested = (qqq + tqqq).clip(upper=1.0 - min_sgov)
    scale = invested / (qqq + tqqq).replace(0.0, math.nan)
    qqq = (qqq * scale).fillna(0.0)
    tqqq = (tqqq * scale).fillna(0.0)
    sgov = (1.0 - qqq - tqqq).clip(lower=min_sgov, upper=1.0)
    frame = pd.DataFrame({"QQQ": qqq, "TQQQ": tqqq, "SGOV": sgov}, index=prices.index)
    if str(candidate.get("rebalance_frequency")) == "threshold":
        band = _float(
            _research_mapping(config, "volatility_targeted_search").get(
                "threshold_rebalance_band"
            )
        )
        return _apply_threshold_rebalance(frame, band)
    return _apply_rebalance(frame, str(candidate.get("rebalance_frequency", "monthly")))


def _drawdown_guarded_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    policy = _research_mapping(config, "drawdown_guarded_policy")
    high = prices["QQQ"].rolling(_rolling_high_window(config), min_periods=20).max().shift(1)
    drawdown = prices["QQQ"].shift(1) / high - 1.0
    close = prices["QQQ"].shift(1)
    ma100 = prices["QQQ"].rolling(_ma_short(config), min_periods=20).mean().shift(1)
    ma200 = prices["QQQ"].rolling(_ma_long(config), min_periods=20).mean().shift(1)
    restored = (close >= ma100) & (close >= ma200)
    risk_on = _normalise_weights(_mapping(candidate.get("risk_on_weights")))
    warning = _normalise_weights(_mapping(candidate.get("warning_weights")))
    block = _normalise_weights(_mapping(candidate.get("block_weights")))
    defensive = _normalise_weights(_mapping(candidate.get("defensive_weights")))
    tickers = sorted(set(risk_on) | set(warning) | set(block) | set(defensive))
    frame = pd.DataFrame(index=prices.index, columns=tickers, data=0.0)
    for idx in prices.index:
        dd = _float(drawdown.loc[idx])
        if dd <= _float(policy.get("defensive_mode_threshold")):
            weights = defensive
        elif dd <= _float(policy.get("zero_tqqq_threshold")):
            weights = block
        elif dd <= _float(policy.get("reduce_tqqq_threshold")):
            weights = warning
        elif bool(restored.loc[idx]) or math.isclose(dd, 0.0):
            weights = risk_on
        else:
            weights = block
        for ticker in tickers:
            frame.loc[idx, ticker] = weights.get(ticker, 0.0)
    return frame.ffill().fillna(0.0)


def _trend_state(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.Series:
    close = prices["QQQ"].shift(1)
    ma100 = prices["QQQ"].rolling(_ma_short(config), min_periods=20).mean().shift(1)
    ma200 = prices["QQQ"].rolling(_ma_long(config), min_periods=20).mean().shift(1)
    vol = _realized_vol(prices["QQQ"], _vol_short(config), _annualization(config))
    trend_policy = _research_mapping(config, "trend_gated_policy")
    vol_threshold = vol.rolling(
        _int(trend_policy.get("volatility_percentile_window"))
    ).quantile(_float(trend_policy.get("volatility_percentile_cutoff")))
    high = prices["QQQ"].rolling(_rolling_high_window(config), min_periods=20).max().shift(1)
    drawdown = close / high - 1.0
    risk_on = (
        (close >= ma100)
        & (ma100 >= ma200)
        & (vol <= vol_threshold)
        & (drawdown >= _float(trend_policy.get("drawdown_warning_threshold")))
    )
    risk_off = (close < ma200) | (
        drawdown <= _float(trend_policy.get("drawdown_block_threshold"))
    )
    state = pd.Series("neutral", index=prices.index)
    state[risk_on.fillna(False)] = "risk_on"
    state[risk_off.fillna(False)] = "risk_off"
    return state


def _period_row(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    weights: pd.DataFrame,
    qqq_returns: pd.Series,
    qqq_weights: pd.DataFrame,
    period: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    start = date.fromisoformat(str(period["start"]))
    raw_end = str(period.get("end"))
    end = returns.index.max().date() if raw_end == "latest" else date.fromisoformat(raw_end)
    selected = returns[(returns.index.date >= start) & (returns.index.date <= end)]
    qqq_selected = qqq_returns.reindex(selected.index).fillna(0.0)
    min_obs = _int(_objective_policy(config).get("minimum_period_observations"))
    if len(selected) < min_obs:
        return {
            "strategy_id": candidate["strategy_id"],
            "period_id": period["period_id"],
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sample_count": int(len(selected)),
            "coverage_status": "INSUFFICIENT_PRICE_COVERAGE",
            "annual_return": 0.0,
            "qqq_annual_return": 0.0,
            "annual_return_vs_qqq": 0.0,
        }
    strategy_metrics = _period_metrics(
        candidate,
        selected,
        weights.reindex(selected.index),
        qqq_selected,
        config,
    )
    qqq_metrics = _period_metrics(
        {"strategy_id": PRIMARY_BENCHMARK_ID, "display_name": "100% QQQ"},
        qqq_selected,
        qqq_weights.reindex(selected.index),
        qqq_selected,
        config,
    )
    return {
        "strategy_id": candidate["strategy_id"],
        "period_id": period["period_id"],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sample_count": int(len(selected)),
        "coverage_status": "COVERED",
        "annual_return": strategy_metrics["annual_return"],
        "qqq_annual_return": qqq_metrics["annual_return"],
        "annual_return_vs_qqq": _round(
            _float(strategy_metrics["annual_return"]) - _float(qqq_metrics["annual_return"])
        ),
        "max_drawdown": strategy_metrics["max_drawdown"],
        "qqq_max_drawdown": qqq_metrics["max_drawdown"],
        "calmar": strategy_metrics["calmar"],
        "qqq_calmar": qqq_metrics["calmar"],
        "turnover": strategy_metrics["turnover"],
    }


def _period_metrics(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    weights: pd.DataFrame,
    benchmark_returns: pd.Series,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    return _metrics_for_strategy(
        candidate,
        returns,
        weights,
        benchmark_returns,
        annualization=_annualization(config),
        cost_bps=0.0,
    )


def _drawdown_replay_row(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    weights: pd.DataFrame,
    qqq_returns: pd.Series,
    episode: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    replay_policy = _research_mapping(config, "drawdown_replay")
    high_tqqq_average_weight = _float(replay_policy.get("high_tqqq_average_weight"), 0.20)
    recovery_episode_ids = {
        str(item) for item in replay_policy.get("recovery_episode_ids", [])
    }
    drawdown_multiplier = _float(
        _objective_policy(config).get("max_drawdown_multiplier_vs_qqq"), 1.10
    )
    start = date.fromisoformat(str(episode["start"]))
    end = date.fromisoformat(str(episode["end"]))
    selected = returns[(returns.index.date >= start) & (returns.index.date <= end)]
    qqq_selected = qqq_returns.reindex(selected.index).fillna(0.0)
    if len(selected) < 2:
        return {
            "strategy_id": candidate["strategy_id"],
            "episode_id": episode["episode_id"],
            "covered": False,
            "coverage_status": "INSUFFICIENT_PRICE_COVERAGE",
            "tqqq_drawdown_unacceptable": False,
            "risk_off_timely": False,
            "risk_on_too_slow": False,
            "strategy_return_vs_qqq": 0.0,
            "max_drawdown_vs_qqq": 0.0,
        }
    equity = (1.0 + selected).cumprod()
    qqq_equity = (1.0 + qqq_selected).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    qqq_drawdown = qqq_equity / qqq_equity.cummax() - 1.0
    episode_weights = weights.reindex(selected.index).fillna(0.0)
    tqqq_avg = _float(episode_weights.get("TQQQ", pd.Series(0.0, index=selected.index)).mean())
    risk_off_timely = (
        tqqq_avg <= high_tqqq_average_weight
        or float(drawdown.min()) >= float(qqq_drawdown.min())
    )
    return {
        "strategy_id": candidate["strategy_id"],
        "episode_id": episode["episode_id"],
        "covered": True,
        "coverage_status": "COVERED",
        "strategy_return": _round(float(equity.iloc[-1] - 1.0)),
        "qqq_return": _round(float(qqq_equity.iloc[-1] - 1.0)),
        "strategy_return_vs_qqq": _round(float(equity.iloc[-1] - qqq_equity.iloc[-1])),
        "max_drawdown": _round(float(drawdown.min())),
        "qqq_max_drawdown": _round(float(qqq_drawdown.min())),
        "max_drawdown_vs_qqq": _round(abs(float(qqq_drawdown.min())) - abs(float(drawdown.min()))),
        "avg_tqqq_weight": _round(tqqq_avg),
        "max_tqqq_weight": _round(
            _float(episode_weights.get("TQQQ", pd.Series(0.0, index=selected.index)).max())
        ),
        "tqqq_drawdown_unacceptable": bool(
            tqqq_avg > high_tqqq_average_weight
            and float(drawdown.min()) < float(qqq_drawdown.min()) * drawdown_multiplier
        ),
        "risk_off_timely": bool(risk_off_timely),
        "risk_on_too_slow": bool(
            str(episode["episode_id"]) in recovery_episode_ids
            and float(equity.iloc[-1]) < float(qqq_equity.iloc[-1])
        ),
    }


def _risk_budget_row(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    weights: pd.DataFrame,
    qqq_returns: pd.Series,
    metrics: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    risk_policy = _research_mapping(config, "risk_budget")
    beta_heavy_beta_threshold = _float(
        risk_policy.get("beta_heavy_beta_threshold"), 1.05
    )
    positive_return_threshold = _float(risk_policy.get("positive_return_threshold"))
    avg = _mapping(metrics.get("average_weights"))
    qqq_weight = _float(avg.get("QQQ"))
    tqqq_weight = _float(avg.get("TQQQ"))
    sgov_weight = _float(avg.get("SGOV"))
    effective_leverage = qqq_weight + tqqq_weight * TQQQ_DAILY_LEVERAGE_MULTIPLIER
    risk_total = abs(qqq_weight) + abs(tqqq_weight * TQQQ_DAILY_LEVERAGE_MULTIPLIER) + abs(
        sgov_weight
    )
    beta = _beta(returns, qqq_returns)
    return {
        "strategy_id": candidate["strategy_id"],
        "effective_qqq_beta": _round(beta),
        "effective_leverage": _round(effective_leverage),
        "tqqq_weight": _round(tqqq_weight),
        "sgov_weight": _round(sgov_weight),
        "annual_volatility": metrics.get("annual_volatility"),
        "max_drawdown": metrics.get("max_drawdown"),
        "tail_loss_frequency": metrics.get("tail_loss_frequency"),
        "risk_contribution_qqq": _round(_ratio(abs(qqq_weight), risk_total)),
        "risk_contribution_tqqq": _round(
            _ratio(abs(tqqq_weight * TQQQ_DAILY_LEVERAGE_MULTIPLIER), risk_total)
        ),
        "risk_contribution_sgov": _round(_ratio(abs(sgov_weight), risk_total)),
        "outperformance_mostly_beta": bool(
            beta > beta_heavy_beta_threshold
            and _float(metrics.get("annual_return")) > positive_return_threshold
        ),
    }


def _edge_review_row(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
    period_payload: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _research_mapping(config, "edge_significance")
    annual_edge = _float(row.get("annual_return_vs_qqq"))
    calmar_edge = _float(row.get("calmar_edge"))
    sharpe_edge = _float(row.get("sharpe_edge"))
    drawdown_penalty = max(_float(row.get("drawdown_penalty")), 0.0)
    turnover_penalty = max(
        _float(row.get("turnover"))
        - _float(_objective_policy(config).get("turnover_acceptability_limit")),
        0.0,
    )
    path_penalty = _float(row.get("path_dependency_risk"))
    complexity_penalty = _complexity_penalty(row, config)
    net = (
        annual_edge * _float(policy.get("annual_return_weight"))
        + calmar_edge * _float(policy.get("calmar_weight"))
        + sharpe_edge * _float(policy.get("sharpe_weight"))
        - drawdown_penalty * _float(policy.get("drawdown_penalty_weight"))
        - turnover_penalty * _float(policy.get("turnover_penalty_weight"))
        - path_penalty * _float(policy.get("tqqq_path_dependency_penalty_weight"))
        - complexity_penalty * _float(policy.get("complexity_penalty_weight"))
    )
    period_summary = _period_summary_for(period_payload, str(row.get("strategy_id")))
    if period_summary.get("stability_flag") == "REGIME_CONCENTRATED":
        status = "GROWTH_EDGE_REGIME_CONCENTRATED"
    elif net >= _float(policy.get("material_score_threshold")) and annual_edge > 0:
        status = "GROWTH_EDGE_MATERIAL"
    elif net >= _float(policy.get("weak_score_threshold")) and annual_edge > 0:
        status = "GROWTH_EDGE_WEAK"
    else:
        status = "GROWTH_EDGE_WEAK"
    return {
        "strategy_id": row.get("strategy_id"),
        "annual_return_edge_vs_qqq": _round(annual_edge),
        "calmar_edge_vs_qqq": _round(calmar_edge),
        "sharpe_edge_vs_qqq": _round(sharpe_edge),
        "drawdown_penalty": _round(drawdown_penalty),
        "turnover_penalty": _round(turnover_penalty),
        "tqqq_path_dependency_penalty": _round(path_penalty),
        "complexity_penalty": _round(complexity_penalty),
        "net_growth_edge_score": _round(net),
        "period_stability_flag": period_summary.get("stability_flag"),
        "edge_status": status,
    }


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
        raise ValueError(f"QQQ plus growth registry must be a mapping: {path}")
    return dict(raw)


def _required_tickers(config: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in _research_policy(config).get("required_price_tickers", [])]


def _required_rate_series(config: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in _research_policy(config).get("required_rate_series", [])]


def _research_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(config.get("research_policy"))


def _research_mapping(config: Mapping[str, Any], key: str) -> dict[str, Any]:
    return _mapping(_research_policy(config).get(key))


def _objective_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return _research_mapping(config, "objective_contract")


def _annualization(config: Mapping[str, Any]) -> int:
    return _int(_research_policy(config).get("annualization_trading_days"), default=252)


def _ma_short(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "moving_average_windows").get("short"))


def _ma_long(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "moving_average_windows").get("long"))


def _vol_short(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "realized_vol_windows").get("short"))


def _rolling_high_window(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "rolling_high_windows").get("long"))


def _candidate_limit(config: Mapping[str, Any], key: str) -> float:
    return _float(_research_mapping(config, "candidate_limits").get(key))


def _benchmark_specs(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [_with_candidate_defaults(row) for row in _records(config.get("benchmarks"))] + [
        _with_candidate_defaults(row) for row in _records(config.get("static_tqqq_sgov_baselines"))
    ]


def _growth_candidate_specs(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    controlled = [
        _with_candidate_defaults(row)
        for row in _records(config.get("controlled_tqqq_overlay_candidates"))
    ]
    trend = [
        _with_candidate_defaults(row)
        for row in _records(config.get("trend_gated_leverage_candidates"))
    ]
    drawdown = [
        _with_candidate_defaults(row)
        for row in _records(config.get("drawdown_guarded_growth_candidates"))
    ]
    return controlled + trend + _vol_target_candidate_rows(config) + drawdown


def _all_candidate_specs(
    config: Mapping[str, Any],
    *,
    include_benchmarks: bool,
) -> list[dict[str, Any]]:
    rows = _growth_candidate_specs(config)
    if include_benchmarks:
        rows = _benchmark_specs(config) + rows
    return rows


def _vol_target_candidate_rows(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    search = _research_mapping(config, "volatility_targeted_search")
    rows: list[dict[str, Any]] = []
    for target_vol in list(search.get("target_vol", [])):
        for window in list(search.get("realized_vol_window", [])):
            for max_tqqq in list(search.get("max_tqqq_weight", [])):
                for min_sgov in list(search.get("min_sgov_weight", [])):
                    for rebalance in list(search.get("rebalance", [])):
                        strategy_id = (
                            "vol_target_growth_"
                            f"tv{int(_float(target_vol) * 100):02d}_"
                            f"w{int(window)}_"
                            f"tqqq{int(_float(max_tqqq) * 100):02d}_"
                            f"sgov{int(_float(min_sgov) * 100):02d}_"
                            f"{rebalance}"
                        )
                        rows.append(
                            _with_candidate_defaults(
                                {
                                    "strategy_id": strategy_id,
                                    "display_name": strategy_id,
                                    "candidate_role": "growth_challenger",
                                    "candidate_type": "volatility_targeted_qqq_tqqq_sgov",
                                    "target_vol": target_vol,
                                    "realized_vol_window": window,
                                    "max_tqqq_weight": max_tqqq,
                                    "min_sgov_weight": min_sgov,
                                    "rebalance_frequency": rebalance,
                                }
                            )
                        )
    return rows


def _with_candidate_defaults(row: Mapping[str, Any]) -> dict[str, Any]:
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


def _registry_issues(
    config: Mapping[str, Any],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    safety = _mapping(config.get("safety_boundary"))
    for field in ("production_effect", "broker_action", "uses_options", "uses_margin"):
        if field not in safety:
            issues.append({"issue_id": "missing_safety_boundary", "field": field})
    seen: set[str] = set()
    for row in candidates:
        strategy_id = str(row.get("strategy_id") or "UNKNOWN")
        if strategy_id in seen:
            issues.append({"strategy_id": strategy_id, "issue_id": "duplicate_strategy_id"})
        seen.add(strategy_id)
        if row.get("uses_options") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "uses_options_not_false"})
        if row.get("uses_margin") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "uses_margin_not_false"})
        if row.get("production_effect") != "none" or row.get("broker_action") != "none":
            issues.append({"strategy_id": strategy_id, "issue_id": "unsafe_effect_field"})
        if _float(row.get("max_tqqq_weight")) > _candidate_limit(config, "max_tqqq_weight"):
            issues.append({"strategy_id": strategy_id, "issue_id": "max_tqqq_weight_exceeded"})
        weights = _normalise_weights(
            _mapping(row.get("target_weights")) or _mapping(row.get("risk_on_weights"))
        )
        exposure_limit = _candidate_limit(config, "max_effective_qqq_exposure")
        if _effective_qqq_exposure(weights) > exposure_limit:
            issues.append(
                {"strategy_id": strategy_id, "issue_id": "effective_qqq_exposure_exceeded"}
            )
    return issues


def _search_artifact_paths(output_root: Path) -> dict[str, Path]:
    return {
        "controlled_overlay": output_root / "controlled_tqqq_overlay_search.json",
        "trend_gated": output_root / "trend_gated_leverage_policy_search.json",
        "vol_targeted": output_root / "volatility_targeted_growth_policy_search.json",
        "drawdown_guarded": output_root / "drawdown_guarded_growth_policy_search.json",
    }


def _collect_candidate_results(payloads: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in payloads.values():
        rows.extend(_records(payload.get("candidate_results")))
    return rows


def _selected_growth_candidates(
    config: Mapping[str, Any],
    output_root: Path,
) -> list[dict[str, Any]]:
    ranking = _read_json_or_empty(output_root / "qqq_outperformance_ranking_report.json")
    selected_ids = [
        str(row.get("strategy_id")) for row in _records(ranking.get("growth_candidate_watchlist"))
    ][:5]
    candidates = _growth_candidate_specs(config)
    if not selected_ids:
        selected_ids = [row["strategy_id"] for row in candidates[:5]]
    lookup = {row["strategy_id"]: row for row in candidates}
    return [lookup[strategy_id] for strategy_id in selected_ids if strategy_id in lookup]


def _strategy_by_id(rows: list[dict[str, Any]], strategy_id: str) -> dict[str, Any]:
    return next((row for row in rows if row.get("strategy_id") == strategy_id), {})


def _dedupe_best_by_strategy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        strategy_id = str(row.get("strategy_id"))
        if strategy_id not in best or _float(row.get("growth_ranking_score")) > _float(
            best[strategy_id].get("growth_ranking_score")
        ):
            best[strategy_id] = dict(row)
    return list(best.values())


def _ranking_row(row: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    enriched.setdefault("growth_ranking_score", _growth_ranking_score(enriched, config))
    enriched.setdefault("complexity_penalty", _complexity_penalty(enriched, config))
    return enriched


def _dominated_rows(rows: list[dict[str, Any]]) -> list[str]:
    dominated: list[str] = []
    for row in rows:
        for other in rows:
            if row is other:
                continue
            better_or_equal = (
                _float(other.get("annual_return")) >= _float(row.get("annual_return"))
                and _float(other.get("calmar")) >= _float(row.get("calmar"))
                and _float(other.get("sharpe")) >= _float(row.get("sharpe"))
                and abs(_float(other.get("max_drawdown"))) <= abs(_float(row.get("max_drawdown")))
            )
            strictly = (
                _float(other.get("annual_return")) > _float(row.get("annual_return"))
                or _float(other.get("calmar")) > _float(row.get("calmar"))
                or abs(_float(other.get("max_drawdown"))) < abs(_float(row.get("max_drawdown")))
            )
            if better_or_equal and strictly:
                dominated.append(str(row.get("strategy_id")))
                break
    return sorted(set(dominated))


def _growth_watchlist_rows(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    max_size = _int(_research_mapping(config, "candidate_limits").get("max_growth_watchlist_size"))
    eligible = [
        row
        for row in rows
        if _is_growth_candidate(row)
        and row.get("objective_screen_passed")
        and row.get("drawdown_constraint_passed")
    ]
    return sorted(eligible, key=lambda row: _float(row.get("growth_ranking_score")), reverse=True)[
        :max_size
    ]


def _objective_screen_passed(
    row: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
    config: Mapping[str, Any],
) -> bool:
    policy = _objective_policy(config)
    return bool(
        _float(row.get("annual_return"))
        >= _float(qqq_metrics.get("annual_return")) + _float(policy.get("annual_return_edge_min"))
        and _float(row.get("calmar")) >= _float(qqq_metrics.get("calmar"))
        and (
            _float(row.get("sharpe")) >= _float(qqq_metrics.get("sharpe"))
            or _float(row.get("max_drawdown_vs_qqq")) > 0
        )
        and row.get("drawdown_constraint_passed")
        and row.get("turnover_acceptable")
        and row.get("tqqq_weight_constraint_passed")
        and row.get("effective_exposure_constraint_passed")
    )


def _growth_candidate_band(
    row: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
    config: Mapping[str, Any],
) -> str:
    if not _is_growth_candidate(row):
        return "benchmark_or_comparator"
    if _objective_screen_passed(row, qqq_metrics, config):
        return "top_growth_candidate"
    if _float(row.get("annual_return")) > _float(qqq_metrics.get("annual_return")):
        return "challenger_only"
    return "did_not_outperform_qqq"


def _growth_ranking_score(row: Mapping[str, Any], config: Mapping[str, Any]) -> float:
    policy = _research_mapping(config, "edge_significance")
    return _round(
        _float(row.get("annual_return_vs_qqq")) * _float(policy.get("annual_return_weight"))
        + _float(row.get("calmar_edge")) * _float(policy.get("calmar_weight"))
        + _float(row.get("sharpe_edge")) * _float(policy.get("sharpe_weight"))
        - max(_float(row.get("drawdown_penalty")), 0.0)
        - _complexity_penalty(row, config) * _float(policy.get("complexity_penalty_weight"))
    )


def _candidate_blockers(row: Mapping[str, Any], config: Mapping[str, Any]) -> list[str]:
    blockers = []
    if _float(row.get("annual_return_vs_qqq")) < _float(
        _objective_policy(config).get("annual_return_edge_min")
    ):
        blockers.append("annual_return_edge_below_minimum")
    if not row.get("drawdown_constraint_passed"):
        blockers.append("drawdown_constraint_failed")
    if not row.get("turnover_acceptable"):
        blockers.append("turnover_above_limit")
    if not row.get("tqqq_weight_constraint_passed"):
        blockers.append("tqqq_weight_above_limit")
    if not row.get("effective_exposure_constraint_passed"):
        blockers.append("effective_qqq_exposure_above_limit")
    return blockers


def _period_stability_flag(
    wins: int, valid_periods: int, ai_rally_wins: int, config: Mapping[str, Any]
) -> str:
    if valid_periods == 0:
        return "QQQ_OUTPERFORMANCE_NOT_STABLE"
    if wins > 0 and wins == ai_rally_wins:
        return "REGIME_CONCENTRATED"
    policy = _research_mapping(config, "period_stability")
    if _ratio(wins, valid_periods) <= _float(policy.get("minimum_win_ratio"), 0.50):
        return "QQQ_OUTPERFORMANCE_NOT_STABLE"
    return "QQQ_OUTPERFORMANCE_STABLE"


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


def _period_summary_for(payload: Mapping[str, Any], strategy_id: str) -> dict[str, Any]:
    return next(
        (
            row
            for row in _records(payload.get("candidate_summaries"))
            if row.get("strategy_id") == strategy_id
        ),
        {},
    )


def _owner_action(answers: Mapping[str, Any], missing: list[str]) -> str:
    if missing:
        return "rerun_missing_growth_research_artifacts"
    if answers.get("2_edge_material_after_risk") and answers.get("7_add_one_growth_challenger"):
        return "review_one_growth_challenger_for_research_only_forward_aging"
    return "do_not_add_growth_challenger_yet"


def _apply_rebalance(weights: pd.DataFrame, frequency: str) -> pd.DataFrame:
    if frequency == "daily":
        return weights.ffill().fillna(0.0)
    period = weights.index.to_period("Q" if frequency == "quarterly" else "M").to_series(
        index=weights.index
    )
    markers = period != period.shift(1)
    rebalanced = weights.copy()
    rebalanced.loc[~markers] = math.nan
    rebalanced = rebalanced.ffill()
    if not rebalanced.empty:
        rebalanced.iloc[0] = weights.iloc[0]
    return rebalanced.ffill().fillna(0.0)


def _apply_threshold_rebalance(weights: pd.DataFrame, band: float) -> pd.DataFrame:
    last = None
    rows = []
    for _, row in weights.iterrows():
        if last is None or (row - last).abs().max() >= band:
            last = row
        rows.append(last)
    return pd.DataFrame(rows, index=weights.index).ffill().fillna(0.0)


def _normalise_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    parsed = {str(key): _float(value) for key, value in weights.items() if _float(value) > 0}
    total = sum(parsed.values())
    if total <= 0:
        return {}
    return {key: value / total for key, value in parsed.items()}


def _realized_vol(series: pd.Series, window: int, annualization: int) -> pd.Series:
    return (
        series.pct_change().rolling(window, min_periods=max(2, window // 4)).std().shift(1)
        * math.sqrt(annualization)
    )


def _effective_qqq_exposure(weights: Mapping[str, Any]) -> float:
    return (
        _float(weights.get("QQQ"))
        + _float(weights.get("TQQQ")) * TQQQ_DAILY_LEVERAGE_MULTIPLIER
    )


def _max_effective_qqq_exposure(weights: pd.DataFrame) -> float:
    qqq = weights.get("QQQ", pd.Series(0.0, index=weights.index))
    tqqq = weights.get("TQQQ", pd.Series(0.0, index=weights.index))
    return float((qqq + tqqq * TQQQ_DAILY_LEVERAGE_MULTIPLIER).max())


def _annualized_mean(series: pd.Series, config: Mapping[str, Any]) -> float:
    return float(series.fillna(0.0).mean() * _annualization(config))


def _annualized_contribution(
    weights: pd.DataFrame,
    returns: pd.Series,
    ticker: str,
    config: Mapping[str, Any],
) -> float:
    if ticker not in weights.columns:
        return 0.0
    applied = weights.shift(1).ffill().reindex(returns.index).fillna(0.0)
    return float((applied[ticker] * returns.fillna(0.0)).mean() * _annualization(config))


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    aligned.columns = ["strategy", "benchmark"]
    variance = float(aligned["benchmark"].var(ddof=0))
    if abs(variance) <= 1e-12:
        return 0.0
    return float(aligned["strategy"].cov(aligned["benchmark"]) / variance)


def _complexity_penalty(row: Mapping[str, Any], config: Mapping[str, Any]) -> float:
    policy = _research_mapping(config, "complexity_penalties")
    candidate_type = str(row.get("candidate_type"))
    if candidate_type == "benchmark":
        return _float(policy.get("benchmark"))
    if candidate_type in policy:
        return _float(policy.get(candidate_type))
    if "trend_gated" in candidate_type:
        return _float(policy.get("trend_gated_tqqq_overlay"), 0.25)
    if _float(_mapping(row.get("average_weights")).get("TQQQ")) > 0:
        return _float(policy.get("tqqq_weighted_candidate"), 0.15)
    return _float(policy.get("other_growth"), 0.10)


def _is_growth_candidate(row: Mapping[str, Any]) -> bool:
    return str(row.get("candidate_role")) == "growth_challenger"


def _top(rows: list[dict[str, Any]], key: str, *, limit: int = 10) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: _float(row.get(key)), reverse=True)[:limit]


def _forbidden_inputs() -> list[str]:
    return [
        "future return",
        "future drawdown",
        "tail-risk label",
        "fallback_triggered",
        "any data after t+1 execution boundary",
    ]


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
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_owner_doc(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        "# QQQ Outperformance Owner Decision Pack",
        "",
        f"- 状态：`{payload.get('status')}`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "",
        "## Owner Questions",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in answers.items())
    lines.extend(
        [
            "",
            "本决策包只用于 research-only growth challenger 复核，不替代 defensive primary，"
            "不生成交易建议、paper-shadow activation、production config mutation "
            "或 broker action。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    *,
    include_in_reader_brief: bool = False,
    extra_artifact_globs: list[str] | None = None,
) -> dict[str, Any]:
    globs = [
        f"outputs/research_strategies/qqq_plus_growth/{artifact_slug}.json",
        f"outputs/research_strategies/qqq_plus_growth/{artifact_slug}.md",
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
            "TRADING-933 to 946 QQQ-plus growth challenger artifacts are regenerated "
            "after candidate, data-quality, search, ranking, or owner review state changes."
        ),
        "owner_action": "review_qqq_plus_growth_research_only_artifact",
        "include_in_reader_brief": include_in_reader_brief,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _requested_range(rows: list[dict[str, Any]], start_date: date, end_date: date | None) -> str:
    if rows:
        return str(rows[0].get("requested_date_range"))
    end = "open" if end_date is None else end_date.isoformat()
    return f"{start_date.isoformat()}..{end}"


def _date_range(prices: pd.DataFrame) -> str:
    if prices.empty:
        return "empty"
    return f"{prices.index.min().date().isoformat()}..{prices.index.max().date().isoformat()}"


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


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
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-12:
        return 0.0
    return numerator / denominator


def _round(value: object) -> float:
    return round(_float(value), 6)


def _stable_hash(value: object) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
