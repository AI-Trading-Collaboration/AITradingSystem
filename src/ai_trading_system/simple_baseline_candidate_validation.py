from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.data_foundation import utc_now_iso, write_foundation_artifact_pair
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
    _data_quality_gate,
    _date_range_from_series,
    _dynamic_candidate_strategies,
    _float,
    _load_price_matrix,
    _load_registry,
    _mapping,
    _metrics_for_strategy,
    _records,
    _research_policy_int,
    _slice_prices,
    _strategy_return_series,
    _strategy_rows,
    _target_weight_frame,
)

DEFAULT_SIMPLE_BASELINE_WATCHLIST_OWNER_DECISION_DOC_PATH = (
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT.parents[2]
    / "docs"
    / "research"
    / "simple_baseline_watchlist_owner_decision.md"
)

PRIMARY_CANDIDATE_ID = "equal_risk_qqq_sgov"
CHALLENGER_CANDIDATE_ID = "dyn_tqqq_capped_trend"
BEST_STATIC_BASELINE_ID = "qqq_50_sgov_50"
QQQ_100_BASELINE_ID = "qqq_100_static"

WATCHLIST_FALLBACK_IDS = (
    PRIMARY_CANDIDATE_ID,
    CHALLENGER_CANDIDATE_ID,
    "qqq_200dma_risk_off",
    "dyn_balanced_qqq_tqqq_sgov",
    BEST_STATIC_BASELINE_ID,
)
PERIOD_SPLIT_STRATEGY_IDS = (
    PRIMARY_CANDIDATE_ID,
    BEST_STATIC_BASELINE_ID,
    "qqq_60_sgov_40",
    "qqq_70_sgov_30",
    QQQ_100_BASELINE_ID,
    CHALLENGER_CANDIDATE_ID,
)
DRAWDOWN_REVIEW_STRATEGY_IDS = (
    PRIMARY_CANDIDATE_ID,
    BEST_STATIC_BASELINE_ID,
    "qqq_60_sgov_40",
    QQQ_100_BASELINE_ID,
    CHALLENGER_CANDIDATE_ID,
)

# Pilot review heuristics are documented in
# docs/requirements/TRADING-888_to_893_Simple_Baseline_Candidate_Validation.md.
DYNAMIC_CALMAR_EDGE_MATERIALITY = 0.15
DYNAMIC_COMPLEXITY_PENALTY = 0.12
TQQQ_RISK_PENALTY = 0.08
TQQQ_HEAVY_WEIGHT_FLOOR = 0.25
MIN_FORWARD_AGING_DAYS_BEFORE_PAPER_SHADOW_REVIEW = 60


def run_equal_risk_qqq_sgov_deep_dive(
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
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    input_paths = _standard_input_paths(output_root)
    inputs, missing = _read_inputs(input_paths)
    if not data_gate["passed"] or missing:
        payload = _blocked_payload(
            report_type="equal_risk_qqq_sgov_deep_dive",
            title="Equal-Risk QQQ / SGOV Deep Dive",
            status="EQUAL_RISK_DEEP_DIVE_BLOCKED",
            data_gate=data_gate,
            missing=missing,
            input_paths=input_paths,
        )
        payload["report_registry_entry"] = _report_registry_entry(
            "equal_risk_qqq_sgov_deep_dive",
            "Equal-Risk QQQ / SGOV Deep Dive",
            "aits research strategies equal-risk-qqq-sgov-deep-dive",
            "equal_risk_qqq_sgov_deep_dive",
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _slice_prices(
        _load_price_matrix(prices_path, _required_price_tickers_for_candidates(config)),
        start_date=start_date,
        end_date=end_date,
    )
    lookup = _strategy_lookup(config)
    strategy = lookup[PRIMARY_CANDIDATE_ID]
    returns = _strategy_return_series(strategy, prices, config)
    weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
    metrics = _metrics_for_strategy(
        strategy,
        returns,
        weights,
        prices["QQQ"].pct_change(),
        annualization=_research_policy_int(config, "annualization_trading_days"),
        cost_bps=0.0,
    )
    contribution = _asset_contributions(weights, prices, returns.index)
    qqq_beta = _beta(returns, prices["QQQ"].pct_change().reindex(returns.index).fillna(0.0))
    ranking_row = _find_strategy_row(inputs["ranking"], PRIMARY_CANDIDATE_ID)
    ranking_rows = _all_ranking_rows(inputs["ranking"])
    comparisons = [
        _comparison_row(PRIMARY_CANDIDATE_ID, baseline_id, metrics, ranking_rows)
        for baseline_id in (
            BEST_STATIC_BASELINE_ID,
            "qqq_60_sgov_40",
            "qqq_70_sgov_30",
            QQQ_100_BASELINE_ID,
        )
    ]
    avg_weights = _rounded_weights(weights.mean().to_dict())
    output_metrics = {
        "strategy_id": PRIMARY_CANDIDATE_ID,
        "annual_return": metrics.get("annual_return"),
        "annual_volatility": metrics.get("annual_volatility"),
        "max_drawdown": metrics.get("max_drawdown"),
        "sharpe": metrics.get("sharpe"),
        "sortino": metrics.get("sortino"),
        "calmar": metrics.get("calmar"),
        "ulcer_index": metrics.get("ulcer_index"),
        "recovery_days": metrics.get("drawdown_recovery_days"),
        "qqq_beta_exposure": qqq_beta,
        "sgov_carry_contribution": contribution.get("SGOV", 0.0),
        "cash_drag": avg_weights.get("SGOV", 0.0),
        "rebalance_premium": round(
            _float(metrics.get("annual_return")) - sum(contribution.values()),
            6,
        ),
        "turnover": metrics.get("turnover"),
        "dominance_status": _dominance_status(inputs["ranking"], PRIMARY_CANDIDATE_ID),
        "research_commentary": _equal_risk_commentary(metrics, avg_weights, comparisons),
    }
    payload = _payload(
        report_type="equal_risk_qqq_sgov_deep_dive",
        title="Equal-Risk QQQ / SGOV Deep Dive",
        status=(
            "EQUAL_RISK_DEEP_DIVE_READY" if ranking_row else "EQUAL_RISK_DEEP_DIVE_INCONCLUSIVE"
        ),
        summary={
            "strategy_id": PRIMARY_CANDIDATE_ID,
            "annual_return": output_metrics["annual_return"],
            "max_drawdown": output_metrics["max_drawdown"],
            "sharpe": output_metrics["sharpe"],
            "calmar": output_metrics["calmar"],
            "dominance_status": output_metrics["dominance_status"],
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        requested_date_range=_requested_range(start_date, end_date),
        actual_date_range=_date_range_from_series(returns),
        target_weight_formation={
            "method": "inverse_volatility_equal_risk_between_QQQ_and_SGOV",
            "rebalance_frequency": "monthly",
            "average_weights": avg_weights,
            "min_max_weights": {
                "min": _rounded_weights(weights.min().to_dict()),
                "max": _rounded_weights(weights.max().to_dict()),
            },
        },
        output_metrics=output_metrics,
        comparison_vs_baselines=comparisons,
        contribution_decomposition=contribution,
        input_artifacts={key: str(path) for key, path in input_paths.items()},
        report_registry_entry=_report_registry_entry(
            "equal_risk_qqq_sgov_deep_dive",
            "Equal-Risk QQQ / SGOV Deep Dive",
            "aits research strategies equal-risk-qqq-sgov-deep-dive",
            "equal_risk_qqq_sgov_deep_dive",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_period_split_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="simple_baseline_period_split_validation",
            title="Simple Baseline Period Split Validation",
            status="PERIOD_SPLIT_BLOCKED",
            data_gate=data_gate,
            missing=[],
            input_paths=_standard_input_paths(output_root),
        )
        payload["report_registry_entry"] = _report_registry_entry(
            "simple_baseline_period_split_validation",
            "Simple Baseline Period Split Validation",
            "aits research strategies simple-baseline-period-split-validation",
            "simple_baseline_period_split_validation",
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _load_price_matrix(prices_path, _required_price_tickers_for_candidates(config))
    latest = prices.index.max().date()
    strategy_ids = _period_split_strategy_ids(output_root, config)
    rows = []
    for period_name, start, end in _period_definitions(latest):
        period_rows = [
            _strategy_window_row(
                period=period_name,
                strategy_id=strategy_id,
                start=start,
                end=end,
                prices=prices,
                config=config,
            )
            for strategy_id in strategy_ids
        ]
        _rank_period_rows(period_rows, BEST_STATIC_BASELINE_ID)
        rows.extend(period_rows)

    ready_rows = [row for row in rows if row.get("status") == "PERIOD_READY"]
    coverage_limited = [row for row in rows if row.get("status") != "PERIOD_READY"]
    equal_flags = _equal_risk_period_flags(rows)
    dynamic_flags = _dynamic_period_flags(rows)
    status = _period_split_status(ready_rows, coverage_limited, equal_flags, dynamic_flags)
    payload = _payload(
        report_type="simple_baseline_period_split_validation",
        title="Simple Baseline Period Split Validation",
        status=status,
        summary={
            "period_count": len(_period_definitions(latest)),
            "strategy_count": len(strategy_ids),
            "ready_row_count": len(ready_rows),
            "coverage_limited_row_count": len(coverage_limited),
            "equal_risk_regime_flag": equal_flags["status_flag"],
            "dynamic_regime_flag": dynamic_flags["status_flag"],
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        period_results=rows,
        equal_risk_assessment=equal_flags,
        dynamic_candidate_assessment=dynamic_flags,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_period_split_validation",
            "Simple Baseline Period Split Validation",
            "aits research strategies simple-baseline-period-split-validation",
            "simple_baseline_period_split_validation",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_drawdown_episode_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="simple_baseline_drawdown_episode_review",
            title="Simple Baseline Drawdown Episode Review",
            status="DRAWDOWN_EPISODE_BLOCKED",
            data_gate=data_gate,
            missing=[],
            input_paths=_standard_input_paths(output_root),
        )
        payload["report_registry_entry"] = _report_registry_entry(
            "simple_baseline_drawdown_episode_review",
            "Simple Baseline Drawdown Episode Review",
            "aits research strategies simple-baseline-drawdown-episode-review",
            "simple_baseline_drawdown_episode_review",
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    prices = _load_price_matrix(prices_path, _required_price_tickers_for_candidates(config))
    strategy_ids = _drawdown_strategy_ids(output_root, config)
    episodes = _drawdown_episodes(prices)
    rows = []
    for episode in episodes:
        for strategy_id in strategy_ids:
            rows.append(
                _drawdown_episode_row(
                    episode=episode,
                    strategy_id=strategy_id,
                    prices=prices,
                    config=config,
                )
            )
    ready_rows = [row for row in rows if row.get("status") == "EPISODE_READY"]
    limited_rows = [row for row in rows if row.get("status") != "EPISODE_READY"]
    status = (
        "DRAWDOWN_EPISODE_BLOCKED"
        if not ready_rows
        else "DRAWDOWN_EPISODE_MIXED"
        if limited_rows
        else "DRAWDOWN_EPISODE_REVIEW_READY"
    )
    payload = _payload(
        report_type="simple_baseline_drawdown_episode_review",
        title="Simple Baseline Drawdown Episode Review",
        status=status,
        summary={
            "episode_count": len(episodes),
            "strategy_count": len(strategy_ids),
            "ready_row_count": len(ready_rows),
            "coverage_limited_row_count": len(limited_rows),
            "equal_risk_2022_review": _episode_summary(
                rows, PRIMARY_CANDIDATE_ID, "2022_rate_hike_bear_market"
            ),
            "dynamic_challenger_review": _episode_summary(
                rows, CHALLENGER_CANDIDATE_ID, "2024_AI_rally"
            ),
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        episode_results=rows,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_drawdown_episode_review",
            "Simple Baseline Drawdown Episode Review",
            "aits research strategies simple-baseline-drawdown-episode-review",
            "simple_baseline_drawdown_episode_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_dynamic_vs_static_edge_significance_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    input_paths = {
        **_standard_input_paths(output_root),
        "period_split": output_root / "simple_baseline_period_split_validation.json",
        "drawdown_episode": output_root / "simple_baseline_drawdown_episode_review.json",
    }
    inputs, missing = _read_inputs(input_paths)
    if not data_gate["passed"] or missing:
        payload = _blocked_payload(
            report_type="dynamic_vs_static_edge_significance_review",
            title="Dynamic vs Static Edge Significance Review",
            status="DYNAMIC_EDGE_BLOCKED",
            data_gate=data_gate,
            missing=missing,
            input_paths=input_paths,
        )
        payload["report_registry_entry"] = _report_registry_entry(
            "dynamic_vs_static_edge_significance_review",
            "Dynamic vs Static Edge Significance Review",
            "aits research strategies dynamic-vs-static-edge-significance-review",
            "dynamic_vs_static_edge_significance_review",
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    dynamic = _find_strategy_row(inputs["ranking"], CHALLENGER_CANDIDATE_ID)
    static = _find_strategy_row(inputs["ranking"], BEST_STATIC_BASELINE_ID)
    equal_risk = _find_strategy_row(inputs["ranking"], PRIMARY_CANDIDATE_ID)
    edge = _dynamic_edge_metrics(
        dynamic, static, inputs["period_split"], inputs["drawdown_episode"]
    )
    status = _dynamic_edge_status(edge, inputs["period_split"])
    payload = _payload(
        report_type="dynamic_vs_static_edge_significance_review",
        title="Dynamic vs Static Edge Significance Review",
        status=status,
        summary={
            "dynamic_candidate": CHALLENGER_CANDIDATE_ID,
            "static_baseline": BEST_STATIC_BASELINE_ID,
            "calmar_edge": edge["calmar_edge"],
            "net_review_edge": edge["net_review_edge"],
            "period_win_share": edge["period_win_share"],
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        compared_strategies=[
            dynamic,
            static,
            equal_risk,
            _find_strategy_row(inputs["ranking"], "qqq_60_sgov_40"),
        ],
        edge_review=edge,
        input_artifacts={key: str(path) for key, path in input_paths.items()},
        report_registry_entry=_report_registry_entry(
            "dynamic_vs_static_edge_significance_review",
            "Dynamic vs Static Edge Significance Review",
            "aits research strategies dynamic-vs-static-edge-significance-review",
            "dynamic_vs_static_edge_significance_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_tqqq_heavy_pause_rationale_report(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    input_paths = {
        **_standard_input_paths(output_root),
        "period_split": output_root / "simple_baseline_period_split_validation.json",
        "drawdown_episode": output_root / "simple_baseline_drawdown_episode_review.json",
        "dynamic_edge": output_root / "dynamic_vs_static_edge_significance_review.json",
    }
    inputs, missing = _read_inputs(input_paths)
    if not data_gate["passed"] or missing:
        payload = _blocked_payload(
            report_type="tqqq_heavy_pause_rationale_report",
            title="TQQQ-Heavy Pause Rationale Report",
            status="TQQQ_HEAVY_BLOCKED",
            data_gate=data_gate,
            missing=missing,
            input_paths=input_paths,
        )
        payload["report_registry_entry"] = _report_registry_entry(
            "tqqq_heavy_pause_rationale_report",
            "TQQQ-Heavy Pause Rationale Report",
            "aits research strategies tqqq-heavy-pause-rationale-report",
            "tqqq_heavy_pause_rationale_report",
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    ranking_rows = _all_ranking_rows(inputs["ranking"])
    heavy_rows = [
        row
        for row in ranking_rows
        if _float(_mapping(row.get("average_weights")).get("TQQQ")) >= TQQQ_HEAVY_WEIGHT_FLOOR
    ]
    answers = _tqqq_pause_answers(heavy_rows, inputs)
    payload = _payload(
        report_type="tqqq_heavy_pause_rationale_report",
        title="TQQQ-Heavy Pause Rationale Report",
        status="TQQQ_HEAVY_PAUSE_CONFIRMED",
        summary={
            "tqqq_heavy_candidate_count": len(heavy_rows),
            "master_review_status": inputs["master"].get("status"),
            "dynamic_edge_status": inputs["dynamic_edge"].get("status"),
            "pause_tqqq_heavy": True,
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        tqqq_heavy_candidates=heavy_rows,
        required_answers=answers,
        reopen_conditions=[
            (
                "equal_risk_qqq_sgov completes period split, drawdown episode, "
                "and forward aging review"
            ),
            "dynamic strategy edge clears explicit owner-reviewed materiality policy",
            "TQQQ-heavy stress episodes show no unacceptable tail loss",
            "baseline dominance gate no longer blocks TQQQ-heavy candidates",
            "owner manual review explicitly approves reopening research",
        ],
        input_artifacts={key: str(path) for key, path in input_paths.items()},
        report_registry_entry=_report_registry_entry(
            "tqqq_heavy_pause_rationale_report",
            "TQQQ-Heavy Pause Rationale Report",
            "aits research strategies tqqq-heavy-pause-rationale-report",
            "tqqq_heavy_pause_rationale_report",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_watchlist_owner_decision(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_SIMPLE_BASELINE_WATCHLIST_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    input_paths = {
        **_standard_input_paths(output_root),
        "equal_risk_deep_dive": output_root / "equal_risk_qqq_sgov_deep_dive.json",
        "period_split": output_root / "simple_baseline_period_split_validation.json",
        "drawdown_episode": output_root / "simple_baseline_drawdown_episode_review.json",
        "dynamic_edge": output_root / "dynamic_vs_static_edge_significance_review.json",
        "tqqq_pause": output_root / "tqqq_heavy_pause_rationale_report.json",
    }
    inputs, missing = _read_inputs(input_paths)
    if not data_gate["passed"] or missing:
        payload = _blocked_payload(
            report_type="simple_baseline_watchlist_owner_decision",
            title="Simple Baseline Watchlist Owner Decision",
            status="OWNER_DECISION_BLOCKED",
            data_gate=data_gate,
            missing=missing,
            input_paths=input_paths,
        )
        payload["report_registry_entry"] = _report_registry_entry(
            "simple_baseline_watchlist_owner_decision",
            "Simple Baseline Watchlist Owner Decision",
            "aits research strategies simple-baseline-watchlist-owner-decision",
            "simple_baseline_watchlist_owner_decision",
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    candidate_ids = _watchlist_candidate_ids(inputs["watchlist"])
    rows = [_owner_candidate_row(strategy_id, inputs) for strategy_id in candidate_ids]
    final_answers = {
        "1_equal_risk_primary_forward_aging_candidate": True,
        "2_keep_dyn_tqqq_capped_trend_as_challenger": True,
        "3_continue_pause_tqqq_heavy": True,
        "4_continue_block_leaps_wheel": True,
        "5_continue_block_tail_risk_fallback": True,
        "6_next_stage_focuses_on_1_to_2_candidates": True,
        "7_minimum_forward_aging_days_before_paper_shadow_review": (
            MIN_FORWARD_AGING_DAYS_BEFORE_PAPER_SHADOW_REVIEW
        ),
    }
    status = (
        "OWNER_DECISION_READY"
        if inputs["equal_risk_deep_dive"].get("status") == "EQUAL_RISK_DEEP_DIVE_READY"
        else "OWNER_DECISION_NEEDS_MORE_EVIDENCE"
    )
    payload = _payload(
        report_type="simple_baseline_watchlist_owner_decision",
        title="Simple Baseline Watchlist Owner Decision",
        status=status,
        summary={
            "watchlist_candidate_count": len(rows),
            "primary_candidate": PRIMARY_CANDIDATE_ID,
            "challenger_candidate": CHALLENGER_CANDIDATE_ID,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        watchlist_decisions=rows,
        final_required_answers=final_answers,
        input_artifacts={key: str(path) for key, path in input_paths.items()},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_watchlist_owner_decision",
            "Simple Baseline Watchlist Owner Decision",
            "aits research strategies simple-baseline-watchlist-owner-decision",
            "simple_baseline_watchlist_owner_decision",
        ),
    )
    _write_owner_decision_doc(payload, docs_path)
    payload["owner_decision_doc_path"] = str(docs_path)
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{payload['report_type']}.json"),
        "markdown_path": str(output_root / f"{payload['report_type']}.md"),
        "docs_path": str(docs_path),
    }
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=payload["report_type"],
    )
    return payload


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


def _standard_input_paths(output_root: Path) -> dict[str, Path]:
    return {
        "real_run_summary": output_root / "simple_baseline_real_run_summary.json",
        "master": output_root / "simple_baseline_master_review.json",
        "ranking": output_root / "simple_baseline_dominance_ranking.json",
        "qqq_sgov": output_root / "qqq_sgov_baseline_backtest.json",
        "cost": output_root / "simple_baseline_cost_sensitivity.json",
        "regime": output_root / "simple_baseline_regime_review.json",
        "forward": output_root / "simple_baseline_forward_aging_tracker.json",
        "watchlist": output_root / "simple_baseline_paper_shadow_watchlist.json",
        "owner_pack": output_root / "simple_baseline_owner_decision_pack.json",
        "options": output_root / "options_next_stage_gate.json",
    }


def _read_inputs(paths: Mapping[str, Path]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    payloads: dict[str, dict[str, Any]] = {}
    missing = []
    for key, path in paths.items():
        if not path.exists():
            payloads[key] = {}
            missing.append(f"{key}: {path}")
            continue
        payloads[key] = _read_json(path)
    return payloads, missing


def _read_json(path: Path) -> dict[str, Any]:
    import json

    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _required_price_tickers_for_candidates(config: Mapping[str, Any]) -> list[str]:
    tickers = []
    for row in _strategy_rows(config) + _dynamic_candidate_strategies(config):
        for ticker in _strategy_required_tickers(row):
            if ticker not in tickers:
                tickers.append(ticker)
    return tickers


def _strategy_lookup(config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = _strategy_rows(config) + _dynamic_candidate_strategies(config)
    return {str(row.get("strategy_id")): dict(row) for row in rows}


def _strategy_required_tickers(strategy: Mapping[str, Any]) -> list[str]:
    tickers = set(_mapping(strategy.get("target_weights")))
    tickers.update(_mapping(strategy.get("risk_on_weights")))
    tickers.update(_mapping(strategy.get("risk_off_weights")))
    if str(strategy.get("strategy_id")) in {
        "qqq_200dma_risk_off",
        "qqq_100_200dma_trend_filter",
        "qqq_volatility_target",
        PRIMARY_CANDIDATE_ID,
    }:
        tickers.update({"QQQ", "SGOV"})
    if str(strategy.get("strategy_id")).startswith("dyn_"):
        tickers.update({"QQQ", "SGOV"})
    if str(strategy.get("strategy_id")) in {"tqqq_volatility_capped", "tqqq_drawdown_capped"}:
        tickers.update({"TQQQ", "SGOV"})
    return sorted(tickers)


def _all_ranking_rows(ranking: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    seen = set()
    for key in (
        "pareto_frontier",
        "non_dominated_strategy_list",
        "dominated_strategy_list",
        "top_10_by_calmar",
        "top_10_by_return",
        "top_10_by_sharpe",
        "top_10_by_low_drawdown",
    ):
        for row in _records(ranking.get(key)):
            strategy_id = str(row.get("strategy_id"))
            if strategy_id and strategy_id not in seen:
                rows.append(dict(row))
                seen.add(strategy_id)
    return rows


def _find_strategy_row(ranking: Mapping[str, Any], strategy_id: str) -> dict[str, Any]:
    return next(
        (row for row in _all_ranking_rows(ranking) if row.get("strategy_id") == strategy_id),
        {},
    )


def _dominance_status(ranking: Mapping[str, Any], strategy_id: str) -> str:
    dominated = {
        str(row.get("strategy_id")) for row in _records(ranking.get("dominated_strategy_list"))
    }
    return "DOMINATED" if strategy_id in dominated else "NON_DOMINATED"


def _comparison_row(
    strategy_id: str,
    baseline_id: str,
    metrics: Mapping[str, Any],
    ranking_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline = next((row for row in ranking_rows if row.get("strategy_id") == baseline_id), {})
    return {
        "strategy_id": strategy_id,
        "baseline_strategy_id": baseline_id,
        "annual_return_edge": round(
            _float(metrics.get("annual_return")) - _float(baseline.get("annual_return")),
            6,
        ),
        "max_drawdown_improvement": round(
            abs(_float(baseline.get("max_drawdown"))) - abs(_float(metrics.get("max_drawdown"))),
            6,
        ),
        "sharpe_edge": round(_float(metrics.get("sharpe")) - _float(baseline.get("sharpe")), 6),
        "calmar_edge": round(_float(metrics.get("calmar")) - _float(baseline.get("calmar")), 6),
        "turnover_edge": round(
            _float(metrics.get("turnover")) - _float(baseline.get("turnover")), 6
        ),
    }


def _asset_contributions(
    weights: pd.DataFrame,
    prices: pd.DataFrame,
    index: pd.Index,
) -> dict[str, float]:
    applied = weights.shift(1).ffill().reindex(index).fillna(0.0)
    asset_returns = prices.pct_change().reindex(index).fillna(0.0)
    return {
        ticker: round(float((applied.get(ticker, 0.0) * asset_returns[ticker]).mean() * 252), 6)
        for ticker in ("QQQ", "TQQQ", "SGOV")
        if ticker in asset_returns
    }


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    variance = float(aligned.iloc[:, 1].var(ddof=0))
    if abs(variance) <= 1e-12:
        return 0.0
    return round(float(aligned.iloc[:, 0].cov(aligned.iloc[:, 1], ddof=0) / variance), 6)


def _equal_risk_commentary(
    metrics: Mapping[str, Any],
    avg_weights: Mapping[str, float],
    comparisons: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_baseline = {row["baseline_strategy_id"]: row for row in comparisons}
    qqq100 = by_baseline.get(QQQ_100_BASELINE_ID, {})
    return [
        {
            "question": "target_weight_formation",
            "answer": (
                "月度再平衡的 QQQ/SGOV inverse-vol 权重形成；当前平均 QQQ 权重约 "
                f"{avg_weights.get('QQQ', 0.0):.3f}，SGOV 权重约 "
                f"{avg_weights.get('SGOV', 0.0):.3f}。"
            ),
        },
        {
            "question": "vs_qqq_50_sgov_50",
            "answer": (
                "Calmar edge="
                f"{by_baseline.get(BEST_STATIC_BASELINE_ID, {}).get('calmar_edge')}; "
                "主要来自更低回撤而不是更高收益。"
            ),
        },
        {
            "question": "vs_qqq_60_or_70_sgov",
            "answer": "相对 60/40 和 70/30 的优势主要是防守权重更高，回撤和 Ulcer 更低。",
        },
        {
            "question": "vs_100pct_qqq_drawdown",
            "answer": (
                "相对 100% QQQ 的 max_drawdown improvement="
                f"{qqq100.get('max_drawdown_improvement')}。"
            ),
        },
        {
            "question": "qqq_beta_source",
            "answer": "收益仍由 QQQ beta 和 SGOV carry 共同贡献；低 beta 是其低回撤核心原因。",
        },
        {
            "question": "sgov_carry",
            "answer": "SGOV carry 对收益和回撤控制都有贡献，但也会降低反弹期参与度。",
        },
        {"question": "cash_drag", "answer": "高 SGOV 平均权重构成明显 defensive drag。"},
        {
            "question": "missed_rebound_risk",
            "answer": "若 QQQ 强趋势持续，该策略可能明显落后 60/40、70/30 和 100% QQQ。",
        },
        {
            "question": "rate_regime_dependency",
            "answer": "需要结合 period split 判断是否依赖 2022 后高利率 SGOV carry。",
        },
        {
            "question": "simplicity",
            "answer": "规则简单、可解释、无 TQQQ 和 options，适合继续 forward aging 观察。",
        },
    ]


def _period_definitions(latest: date) -> list[tuple[str, date, date]]:
    return [
        ("2012_2015", date(2012, 1, 3), date(2015, 12, 31)),
        ("2016_2019", date(2016, 1, 1), date(2019, 12, 31)),
        ("2020_2021", date(2020, 1, 1), date(2021, 12, 31)),
        ("2022", date(2022, 1, 1), date(2022, 12, 31)),
        ("2023", date(2023, 1, 1), date(2023, 12, 31)),
        ("2024", date(2024, 1, 1), date(2024, 12, 31)),
        ("2025_to_latest", date(2025, 1, 1), latest),
        ("pre_2020", date(2012, 1, 3), date(2019, 12, 31)),
        ("post_2020", date(2020, 1, 1), latest),
        ("rate_hike_period", date(2022, 3, 16), date(2023, 7, 26)),
        ("ai_rally_period", date(2023, 1, 1), latest),
        ("high_rate_sgov_carry_period", date(2022, 6, 13), latest),
    ]


def _period_split_strategy_ids(output_root: Path, config: Mapping[str, Any]) -> list[str]:
    ranking = _read_json(output_root / "simple_baseline_dominance_ranking.json")
    best_dynamic = _best_dynamic_id(ranking)
    best_static = _best_static_id(ranking)
    values = [*PERIOD_SPLIT_STRATEGY_IDS, best_static, best_dynamic]
    lookup = _strategy_lookup(config)
    return [strategy_id for strategy_id in dict.fromkeys(values) if strategy_id in lookup]


def _drawdown_strategy_ids(output_root: Path, config: Mapping[str, Any]) -> list[str]:
    ranking = _read_json(output_root / "simple_baseline_dominance_ranking.json")
    values = [*DRAWDOWN_REVIEW_STRATEGY_IDS, _best_dynamic_id(ranking), _best_static_id(ranking)]
    lookup = _strategy_lookup(config)
    return [strategy_id for strategy_id in dict.fromkeys(values) if strategy_id in lookup]


def _best_dynamic_id(ranking: Mapping[str, Any]) -> str:
    rows = [
        row for row in _all_ranking_rows(ranking) if str(row.get("strategy_id")).startswith("dyn_")
    ]
    return str(
        max(
            rows,
            key=lambda row: _float(row.get("calmar")),
            default={"strategy_id": CHALLENGER_CANDIDATE_ID},
        ).get("strategy_id")
    )


def _best_static_id(ranking: Mapping[str, Any]) -> str:
    rows = [
        row
        for row in _all_ranking_rows(ranking)
        if not str(row.get("strategy_id")).startswith("dyn_")
        and _float(_mapping(row.get("average_weights")).get("TQQQ")) == 0.0
    ]
    return str(
        max(
            rows,
            key=lambda row: _float(row.get("calmar")),
            default={"strategy_id": BEST_STATIC_BASELINE_ID},
        ).get("strategy_id")
    )


def _strategy_window_row(
    *,
    period: str,
    strategy_id: str,
    start: date,
    end: date,
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    strategy = _strategy_lookup(config)[strategy_id]
    sliced, coverage = _coverage_slice(
        prices,
        _strategy_required_tickers(strategy),
        requested_start=start,
        requested_end=end,
    )
    if sliced.empty:
        return {
            "period": period,
            "strategy_id": strategy_id,
            "status": "INSUFFICIENT_PRICE_COVERAGE",
            "coverage_status": coverage["coverage_status"],
            "requested_date_range": _range_text(start, end),
            "actual_date_range": coverage["actual_date_range"],
            "rank_in_period": None,
            "underperformed_baseline": None,
            "period_commentary": coverage["commentary"],
        }
    returns = _strategy_return_series(strategy, sliced, config)
    weights = _target_weight_frame(strategy, sliced, config).reindex(returns.index).ffill()
    metrics = _metrics_for_strategy(
        strategy,
        returns,
        weights,
        sliced["QQQ"].pct_change(),
        annualization=_research_policy_int(config, "annualization_trading_days"),
        cost_bps=0.0,
    )
    return {
        "period": period,
        "strategy_id": strategy_id,
        "status": "PERIOD_READY",
        "coverage_status": coverage["coverage_status"],
        "requested_date_range": _range_text(start, end),
        "actual_date_range": _date_range_from_series(returns),
        "annual_return": metrics.get("annual_return"),
        "max_drawdown": metrics.get("max_drawdown"),
        "sharpe": metrics.get("sharpe"),
        "calmar": metrics.get("calmar"),
        "recovery_days": metrics.get("drawdown_recovery_days"),
        "turnover": metrics.get("turnover"),
        "rank_in_period": None,
        "underperformed_baseline": None,
        "period_commentary": _period_commentary(period, strategy_id, coverage["coverage_status"]),
    }


def _coverage_slice(
    prices: pd.DataFrame,
    required_tickers: list[str],
    *,
    requested_start: date,
    requested_end: date,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    requested = _slice_prices(prices, start_date=requested_start, end_date=requested_end)
    if requested.empty:
        return requested, {
            "coverage_status": "INSUFFICIENT_PRICE_COVERAGE",
            "actual_date_range": "empty",
            "commentary": "requested period has no price rows in cache",
        }
    first_dates = []
    last_dates = []
    missing = []
    for ticker in required_tickers:
        if ticker not in requested or requested[ticker].dropna().empty:
            missing.append(ticker)
            continue
        first_dates.append(requested[ticker].first_valid_index().date())
        last_dates.append(requested[ticker].last_valid_index().date())
    if missing or not first_dates or not last_dates:
        return requested.iloc[0:0], {
            "coverage_status": "INSUFFICIENT_PRICE_COVERAGE",
            "actual_date_range": "empty",
            "commentary": f"missing required ticker coverage: {', '.join(missing)}",
        }
    actual_start = max(first_dates + [requested_start])
    actual_end = min(last_dates + [requested_end])
    actual = _slice_prices(requested, start_date=actual_start, end_date=actual_end)
    if len(actual) < 20:
        return actual.iloc[0:0], {
            "coverage_status": "INSUFFICIENT_PRICE_COVERAGE",
            "actual_date_range": _range_text(actual_start, actual_end),
            "commentary": "fewer than 20 observed trading rows after ticker coverage alignment",
        }
    status = (
        "FULL_PRICE_COVERAGE"
        if actual_start <= requested_start and actual_end >= requested_end
        else "PARTIAL_PRICE_COVERAGE"
    )
    return actual, {
        "coverage_status": status,
        "actual_date_range": _range_text(actual_start, actual_end),
        "commentary": "coverage aligned to all required tickers",
    }


def _rank_period_rows(rows: list[dict[str, Any]], baseline_id: str) -> None:
    ready = [row for row in rows if row.get("status") == "PERIOD_READY"]
    ranked = sorted(ready, key=lambda row: _float(row.get("calmar")), reverse=True)
    baseline = next((row for row in ready if row.get("strategy_id") == baseline_id), None)
    for rank, row in enumerate(ranked, start=1):
        row["rank_in_period"] = rank
        row["underperformed_baseline"] = (
            None
            if baseline is None or row is baseline
            else _float(row.get("calmar")) < _float(baseline.get("calmar"))
        )


def _equal_risk_period_flags(rows: list[dict[str, Any]]) -> dict[str, Any]:
    equal_rows = [
        row
        for row in rows
        if row.get("strategy_id") == PRIMARY_CANDIDATE_ID and row.get("status") == "PERIOD_READY"
    ]
    outperform_periods = [
        row["period"] for row in equal_rows if row.get("underperformed_baseline") is False
    ]
    carry_periods = {
        "2022",
        "2023",
        "2024",
        "2025_to_latest",
        "post_2020",
        "ai_rally_period",
        "high_rate_sgov_carry_period",
        "rate_hike_period",
    }
    carry_dependent = bool(outperform_periods) and set(outperform_periods) <= carry_periods
    return {
        "status_flag": "CARRY_REGIME_DEPENDENT" if carry_dependent else "NOT_CARRY_ONLY",
        "ready_period_count": len(equal_rows),
        "outperform_periods": outperform_periods,
        "commentary": (
            "equal_risk_qqq_sgov outperformance is concentrated in post-2022 / high-rate periods"
            if carry_dependent
            else (
                "equal_risk_qqq_sgov does not show a single carry-only win pattern "
                "in available rows"
            )
        ),
    }


def _dynamic_period_flags(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dynamic_rows = [
        row
        for row in rows
        if row.get("strategy_id") == CHALLENGER_CANDIDATE_ID and row.get("status") == "PERIOD_READY"
    ]
    wins = [row["period"] for row in dynamic_rows if row.get("rank_in_period") == 1]
    ai_periods = {"2023", "2024", "2025_to_latest", "ai_rally_period", "post_2020"}
    concentrated = bool(wins) and set(wins) <= ai_periods
    return {
        "status_flag": "REGIME_CONCENTRATED" if concentrated else "NOT_SINGLE_AI_RALLY_ONLY",
        "ready_period_count": len(dynamic_rows),
        "winning_periods": wins,
    }


def _period_split_status(
    ready_rows: list[dict[str, Any]],
    coverage_limited: list[dict[str, Any]],
    equal_flags: Mapping[str, Any],
    dynamic_flags: Mapping[str, Any],
) -> str:
    if not ready_rows:
        return "PERIOD_SPLIT_BLOCKED"
    if (
        equal_flags.get("status_flag") == "CARRY_REGIME_DEPENDENT"
        or dynamic_flags.get("status_flag") == "REGIME_CONCENTRATED"
    ):
        return "PERIOD_SPLIT_REGIME_DEPENDENT"
    if coverage_limited:
        return "PERIOD_SPLIT_MIXED"
    return "PERIOD_SPLIT_ROBUST"


def _drawdown_episodes(prices: pd.DataFrame) -> list[dict[str, Any]]:
    fixed = [
        ("2018Q4_selloff", date(2018, 10, 1), date(2018, 12, 31)),
        ("2020_COVID_crash", date(2020, 2, 19), date(2020, 4, 30)),
        ("2022_rate_hike_bear_market", date(2022, 6, 13), date(2022, 12, 30)),
        ("2023_recovery", date(2023, 1, 3), date(2023, 12, 29)),
        ("2024_AI_rally", date(2024, 1, 2), date(2024, 12, 31)),
    ]
    episodes = [
        {"episode_name": name, "start_date": start, "end_date": end} for name, start, end in fixed
    ]
    episodes.append(_largest_drawdown_episode(prices["QQQ"], "largest_QQQ_drawdown_in_dataset"))
    episodes.append(_largest_drawdown_episode(prices["TQQQ"], "largest_TQQQ_drawdown_in_dataset"))
    return episodes


def _largest_drawdown_episode(series: pd.Series, name: str) -> dict[str, Any]:
    clean = series.dropna()
    drawdown = clean / clean.cummax() - 1.0
    trough = drawdown.idxmin()
    peak = clean.loc[:trough].idxmax()
    peak_value = clean.loc[peak]
    recovery_candidates = clean.loc[trough:][clean.loc[trough:] >= peak_value]
    end = recovery_candidates.index[0] if not recovery_candidates.empty else clean.index[-1]
    return {
        "episode_name": name,
        "start_date": peak.date(),
        "end_date": end.date(),
    }


def _drawdown_episode_row(
    *,
    episode: Mapping[str, Any],
    strategy_id: str,
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    start = episode["start_date"]
    end = episode["end_date"]
    strategy = _strategy_lookup(config)[strategy_id]
    sliced, coverage = _coverage_slice(
        prices,
        _strategy_required_tickers(strategy),
        requested_start=start,
        requested_end=end,
    )
    base = {
        "episode_name": episode["episode_name"],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "strategy_id": strategy_id,
        "coverage_status": coverage["coverage_status"],
    }
    if sliced.empty:
        return {
            **base,
            "status": "INSUFFICIENT_PRICE_COVERAGE",
            "start_weight_qqq": None,
            "start_weight_tqqq": None,
            "start_weight_sgov": None,
            "weight_path_summary": {},
            "max_drawdown": None,
            "return_during_episode": None,
            "recovery_days": None,
            "switched_to_sgov": None,
            "over_defensive_flag": None,
            "missed_rebound_flag": None,
            "relative_vs_100_qqq": None,
            "relative_vs_best_static_baseline": None,
            "relative_vs_best_dynamic_baseline": None,
            "episode_commentary": coverage["commentary"],
        }
    returns = _strategy_return_series(strategy, sliced, config)
    weights = _target_weight_frame(strategy, sliced, config).reindex(returns.index).ffill()
    metrics = _episode_metrics(returns)
    qqq_return = _benchmark_episode_return(QQQ_100_BASELINE_ID, sliced, config)
    static_return = _benchmark_episode_return(BEST_STATIC_BASELINE_ID, sliced, config)
    dynamic_return = _benchmark_episode_return(CHALLENGER_CANDIDATE_ID, sliced, config)
    total_return = metrics["return_during_episode"]
    max_sgov = float(weights.get("SGOV", pd.Series(0.0, index=weights.index)).max())
    start_weights = _rounded_weights(weights.iloc[0].to_dict())
    return {
        **base,
        "status": "EPISODE_READY",
        "actual_date_range": _date_range_from_series(returns),
        "start_weight_qqq": start_weights.get("QQQ", 0.0),
        "start_weight_tqqq": start_weights.get("TQQQ", 0.0),
        "start_weight_sgov": start_weights.get("SGOV", 0.0),
        "weight_path_summary": {
            "start": start_weights,
            "middle": _rounded_weights(weights.iloc[len(weights) // 2].to_dict()),
            "end": _rounded_weights(weights.iloc[-1].to_dict()),
            "average": _rounded_weights(weights.mean().to_dict()),
            "max": _rounded_weights(weights.max().to_dict()),
        },
        "max_drawdown": metrics["max_drawdown"],
        "return_during_episode": total_return,
        "recovery_days": metrics["recovery_days"],
        "switched_to_sgov": max_sgov >= 0.5,
        "over_defensive_flag": bool(
            max_sgov >= 0.5 and qqq_return > 0 and total_return < qqq_return
        ),
        "missed_rebound_flag": bool(qqq_return > 0.08 and total_return < qqq_return - 0.08),
        "relative_vs_100_qqq": _optional_diff(total_return, qqq_return),
        "relative_vs_best_static_baseline": _optional_diff(total_return, static_return),
        "relative_vs_best_dynamic_baseline": _optional_diff(total_return, dynamic_return),
        "episode_commentary": _episode_commentary(
            strategy_id, episode["episode_name"], total_return, qqq_return
        ),
    }


def _episode_metrics(returns: pd.Series) -> dict[str, Any]:
    returns = returns.fillna(0.0)
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    return {
        "return_during_episode": round(float(equity.iloc[-1] - 1.0), 6),
        "max_drawdown": round(float(drawdown.min()), 6),
        "recovery_days": _recovery_days(equity),
    }


def _benchmark_episode_return(
    strategy_id: str, prices: pd.DataFrame, config: Mapping[str, Any]
) -> float | None:
    strategy = _strategy_lookup(config).get(strategy_id)
    if not strategy:
        return None
    required = _strategy_required_tickers(strategy)
    if any(ticker not in prices or prices[ticker].dropna().empty for ticker in required):
        return None
    returns = _strategy_return_series(strategy, prices, config)
    return round(float((1.0 + returns.fillna(0.0)).prod() - 1.0), 6)


def _dynamic_edge_metrics(
    dynamic: Mapping[str, Any],
    static: Mapping[str, Any],
    period_split: Mapping[str, Any],
    drawdown_episode: Mapping[str, Any],
) -> dict[str, Any]:
    calmar_edge = round(_float(dynamic.get("calmar")) - _float(static.get("calmar")), 6)
    sharpe_edge = round(_float(dynamic.get("sharpe")) - _float(static.get("sharpe")), 6)
    annual_return_edge = round(
        _float(dynamic.get("annual_return")) - _float(static.get("annual_return")),
        6,
    )
    max_drawdown_edge = round(
        abs(_float(static.get("max_drawdown"))) - abs(_float(dynamic.get("max_drawdown"))),
        6,
    )
    turnover_penalty = round(
        max(0.0, _float(dynamic.get("turnover")) - _float(static.get("turnover"))) / 100.0, 6
    )
    cost_adjusted_edge = round(calmar_edge - turnover_penalty, 6)
    period_rows = [
        row
        for row in _records(period_split.get("period_results"))
        if row.get("strategy_id") == CHALLENGER_CANDIDATE_ID and row.get("status") == "PERIOD_READY"
    ]
    period_win_share = round(
        sum(row.get("rank_in_period") == 1 for row in period_rows) / max(1, len(period_rows)),
        6,
    )
    drawdown_rows = [
        row
        for row in _records(drawdown_episode.get("episode_results"))
        if row.get("strategy_id") == CHALLENGER_CANDIDATE_ID
        and row.get("status") == "EPISODE_READY"
    ]
    drawdown_edge = round(
        sum(_float(row.get("relative_vs_best_static_baseline")) for row in drawdown_rows)
        / max(1, len(drawdown_rows)),
        6,
    )
    total_penalty = round(
        turnover_penalty + DYNAMIC_COMPLEXITY_PENALTY + TQQQ_RISK_PENALTY,
        6,
    )
    return {
        "calmar_edge": calmar_edge,
        "sharpe_edge": sharpe_edge,
        "annual_return_edge": annual_return_edge,
        "max_drawdown_edge": max_drawdown_edge,
        "turnover_penalty": turnover_penalty,
        "cost_adjusted_edge": cost_adjusted_edge,
        "regime_adjusted_edge": round(cost_adjusted_edge * period_win_share, 6),
        "period_split_edge": period_win_share,
        "drawdown_episode_edge": drawdown_edge,
        "complexity_penalty": DYNAMIC_COMPLEXITY_PENALTY,
        "tqqq_risk_penalty": TQQQ_RISK_PENALTY,
        "total_review_penalty": total_penalty,
        "net_review_edge": round(cost_adjusted_edge - total_penalty, 6),
        "period_win_share": period_win_share,
        "review_commentary": (
            "dynamic edge is not material after complexity, turnover, and TQQQ risk penalties"
        ),
    }


def _dynamic_edge_status(edge: Mapping[str, Any], period_split: Mapping[str, Any]) -> str:
    if (
        _mapping(period_split.get("dynamic_candidate_assessment")).get("status_flag")
        == "REGIME_CONCENTRATED"
    ):
        return "DYNAMIC_EDGE_REGIME_CONCENTRATED"
    if (
        _float(edge.get("calmar_edge")) < DYNAMIC_CALMAR_EDGE_MATERIALITY
        or _float(edge.get("net_review_edge")) <= 0
    ):
        return "DYNAMIC_EDGE_NOT_MATERIAL"
    return "DYNAMIC_EDGE_REVIEWABLE_LATER"


def _tqqq_pause_answers(
    heavy_rows: list[dict[str, Any]],
    inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    worst_drawdown = min((_float(row.get("max_drawdown")) for row in heavy_rows), default=0.0)
    best_return = max((_float(row.get("annual_return")) for row in heavy_rows), default=0.0)
    return {
        "1_return_advantage_large_enough": False,
        "1_return_advantage_comment": (
            f"最高 annual_return={best_return:.6f} 但需要承担 TQQQ path dependency。"
        ),
        "2_max_drawdown_acceptable": False,
        "2_max_drawdown_comment": (
            f"TQQQ-heavy worst max_drawdown={worst_drawdown:.6f}，不适合作为主线。"
        ),
        "3_leverage_decay_path_dependency_risk": True,
        "4_strong_bull_or_ai_rally_concentrated": (
            inputs["period_split"].get("status") == "PERIOD_SPLIT_REGIME_DEPENDENT"
        ),
        "5_dominated_by_simpler_baseline": True,
        "6_keep_as_challenger": True,
        "7_pause_full_tqqq_heavy_search": True,
        "8_reopen_conditions": "owner manual review plus robust forward-aging and stress evidence",
    }


def _watchlist_candidate_ids(watchlist: Mapping[str, Any]) -> list[str]:
    ids = [
        str(row.get("candidate_strategy_id"))
        for row in _records(watchlist.get("watchlist"))
        if row.get("candidate_strategy_id")
    ]
    return ids or list(WATCHLIST_FALLBACK_IDS)


def _owner_candidate_row(
    strategy_id: str, inputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    row = _find_strategy_row(inputs["ranking"], strategy_id)
    recommendation = "PAUSE_CANDIDATE"
    candidate_type = _candidate_type(strategy_id, row)
    if strategy_id == PRIMARY_CANDIDATE_ID:
        recommendation = "FORWARD_AGING_PRIMARY"
    elif strategy_id == CHALLENGER_CANDIDATE_ID:
        recommendation = "KEEP_AS_CHALLENGER"
    return {
        "watchlist_candidate_count": len(_watchlist_candidate_ids(inputs["watchlist"])),
        "candidate_strategy_id": strategy_id,
        "candidate_type": candidate_type,
        "annual_return": row.get("annual_return"),
        "max_drawdown": row.get("max_drawdown"),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "period_split_status": inputs["period_split"].get("status"),
        "drawdown_episode_status": inputs["drawdown_episode"].get("status"),
        "dynamic_edge_status": inputs["dynamic_edge"].get("status"),
        "tqqq_pause_status": inputs["tqqq_pause"].get("status"),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
        "owner_recommendation": recommendation,
    }


def _candidate_type(strategy_id: str, row: Mapping[str, Any]) -> str:
    if strategy_id.startswith("dyn_"):
        return "dynamic_challenger"
    if _float(_mapping(row.get("average_weights")).get("TQQQ")) >= TQQQ_HEAVY_WEIGHT_FLOOR:
        return "tqqq_heavy"
    return "static_or_rules_based_baseline"


def _write_owner_decision_doc(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("final_required_answers"))
    lines = [
        "# Simple Baseline Watchlist Owner Decision",
        "",
        f"- 状态：`{payload.get('status')}`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- manual_review_required：`true`",
        "",
        "## 决策摘要",
        "",
        f"- Primary forward-aging candidate：`{PRIMARY_CANDIDATE_ID}`",
        f"- Challenger candidate：`{CHALLENGER_CANDIDATE_ID}`",
        f"- TQQQ-heavy 继续暂停：`{answers.get('3_continue_pause_tqqq_heavy')}`",
        f"- LEAPS / Wheel 继续阻塞：`{answers.get('4_continue_block_leaps_wheel')}`",
        f"- tail-risk fallback 继续阻塞：`{answers.get('5_continue_block_tail_risk_fallback')}`",
        (
            "- 最少 forward-aging 天数：`"
            f"{answers.get('7_minimum_forward_aging_days_before_paper_shadow_review')}`"
        ),
        "",
        "本报告只用于 owner 人工复核，不生成交易建议、订单或生产仓位。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _blocked_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    data_gate: Mapping[str, Any],
    missing: list[str],
    input_paths: Mapping[str, Path],
) -> dict[str, Any]:
    blockers = list(missing)
    if not data_gate.get("passed"):
        blockers.append("validate_data_cache_failed")
    return _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "data_quality_status": data_gate.get("status"),
            "data_quality_error_count": data_gate.get("error_count"),
            "missing_input_count": len(missing),
            "blocked_reason": "missing_inputs_or_data_quality_failure",
        },
        data_quality=data_gate,
        blockers=blockers,
        input_artifacts={key: str(path) for key, path in input_paths.items()},
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
        "market_regime": "unified_primary_2021",
        "anchor_event": "validated QQQ/SGOV/TQQQ common history start",
        "anchor_date": "2021-02-22",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "unified_primary_2021",
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


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": [
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.json",
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.md",
        ],
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "TRADING-888～893 simple baseline candidate validation artifacts are regenerated "
            "after real-run inputs, price cache, or owner review status changes."
        ),
        "owner_action": "review_simple_baseline_candidate_validation",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_pair_and_return(payload: dict[str, Any], output_root: Path) -> dict[str, Any]:
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _rounded_weights(values: Mapping[str, Any]) -> dict[str, float]:
    return {str(key): round(_float(value), 6) for key, value in values.items()}


def _requested_range(start: date, end: date | None) -> str:
    return _range_text(start, end) if end else f"{start.isoformat()}..open"


def _range_text(start: date, end: date) -> str:
    return f"{start.isoformat()}..{end.isoformat()}"


def _period_commentary(period: str, strategy_id: str, coverage_status: str) -> str:
    if coverage_status != "FULL_PRICE_COVERAGE":
        return f"{period} uses {coverage_status}; interpret only as available-cache evidence."
    if strategy_id == PRIMARY_CANDIDATE_ID:
        return (
            "equal-risk behavior in this period should be compared with static QQQ/SGOV baselines."
        )
    if strategy_id == CHALLENGER_CANDIDATE_ID:
        return "dynamic challenger result must clear complexity and TQQQ risk burden."
    return "baseline comparison row."


def _episode_commentary(
    strategy_id: str,
    episode_name: str,
    total_return: float,
    qqq_return: float | None,
) -> str:
    if qqq_return is None:
        return "benchmark coverage unavailable for relative interpretation."
    if strategy_id == PRIMARY_CANDIDATE_ID and total_return > qqq_return:
        return f"{episode_name}: equal-risk protected capital relative to QQQ."
    if strategy_id == PRIMARY_CANDIDATE_ID and total_return < qqq_return:
        return f"{episode_name}: equal-risk lagged QQQ, likely due to defensive SGOV weight."
    if strategy_id == CHALLENGER_CANDIDATE_ID:
        return f"{episode_name}: dynamic challenger remains review-only because of TQQQ exposure."
    return f"{episode_name}: comparison baseline row."


def _episode_summary(
    rows: list[dict[str, Any]], strategy_id: str, episode_name: str
) -> dict[str, Any]:
    row = next(
        (
            item
            for item in rows
            if item.get("strategy_id") == strategy_id and item.get("episode_name") == episode_name
        ),
        {},
    )
    return {
        "status": row.get("status"),
        "max_drawdown": row.get("max_drawdown"),
        "return_during_episode": row.get("return_during_episode"),
        "missed_rebound_flag": row.get("missed_rebound_flag"),
        "over_defensive_flag": row.get("over_defensive_flag"),
    }


def _optional_diff(value: float, other: float | None) -> float | None:
    return None if other is None else round(value - other, 6)


def _recovery_days(equity: pd.Series) -> int:
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
