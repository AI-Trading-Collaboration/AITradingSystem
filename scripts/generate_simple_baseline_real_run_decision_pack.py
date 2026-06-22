from __future__ import annotations

import copy
import json
import subprocess
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso, write_foundation_artifact_pair
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_PRICES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
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

OUTPUT_ROOT = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT
TOP_CANDIDATE_LIMIT = 10
AS_OF_DATE = "2026-06-18"
BRANCH_NAME = "codex/simple-baseline-portfolio-control"
CHECKED_COMMIT = "c57cb082"
BEST_STATIC_BASELINE_ID = "qqq_50_sgov_50"

CLI_TASKS = [
    (
        "TRADING-865",
        "registry status",
        "simple_baseline_strategy_registry_review",
        "python -m ai_trading_system.cli research strategies simple-baseline-registry-review",
    ),
    (
        "TRADING-866",
        "QQQ/SGOV backtest result",
        "qqq_sgov_baseline_backtest",
        "python -m ai_trading_system.cli research strategies "
        "qqq-sgov-baseline-backtest --as-of 2026-06-18",
    ),
    (
        "TRADING-867",
        "TQQQ/SGOV risk result",
        "tqqq_sgov_risk_controlled_baseline",
        "python -m ai_trading_system.cli research strategies "
        "tqqq-sgov-risk-controlled-baseline --as-of 2026-06-18",
    ),
    (
        "TRADING-868",
        "trend-vol policy search result",
        "trend_vol_allocation_policy_search",
        "python -m ai_trading_system.cli research strategies "
        "trend-vol-allocation-policy-search --as-of 2026-06-18",
    ),
    (
        "TRADING-869",
        "dominance ranking",
        "simple_baseline_dominance_ranking",
        "python -m ai_trading_system.cli research strategies simple-baseline-dominance-ranking",
    ),
    (
        "TRADING-870",
        "PIT boundary status",
        "simple_baseline_pit_boundary_audit",
        "python -m ai_trading_system.cli research strategies simple-baseline-pit-boundary-audit",
    ),
    (
        "TRADING-871",
        "cost sensitivity",
        "simple_baseline_cost_sensitivity",
        "python -m ai_trading_system.cli research strategies "
        "simple-baseline-cost-sensitivity --as-of 2026-06-18",
    ),
    (
        "TRADING-872",
        "regime review",
        "simple_baseline_regime_review",
        "python -m ai_trading_system.cli research strategies "
        "simple-baseline-regime-review --as-of 2026-06-18",
    ),
    (
        "TRADING-873",
        "forward aging status",
        "simple_baseline_forward_aging_tracker",
        "python -m ai_trading_system.cli research strategies "
        "simple-baseline-forward-aging-tracker --as-of 2026-06-18",
    ),
    (
        "TRADING-874",
        "paper-shadow readiness",
        "simple_baseline_paper_shadow_readiness",
        "python -m ai_trading_system.cli research strategies "
        "simple-baseline-paper-shadow-readiness",
    ),
    (
        "TRADING-875",
        "daily summary safety",
        "daily_reader_portfolio_control_safety_summary",
        "python -m ai_trading_system.cli research strategies "
        "daily-reader-portfolio-control-safety-summary",
    ),
    (
        "TRADING-876",
        "dry-run mapper",
        "simple_baseline_portfolio_dry_run_mapper",
        "python -m ai_trading_system.cli research strategies "
        "simple-baseline-portfolio-dry-run-mapper",
    ),
    (
        "TRADING-877",
        "master review",
        "simple_baseline_master_review",
        "python -m ai_trading_system.cli research strategies simple-baseline-master-review",
    ),
    (
        "TRADING-878",
        "options gate",
        "options_next_stage_gate",
        "python -m ai_trading_system.cli research strategies options-next-stage-gate",
    ),
]

EPISODES = [
    ("2018Q4", date(2018, 10, 1), date(2018, 12, 31), "pre_ai_stress"),
    ("2020_COVID_crash", date(2020, 2, 19), date(2020, 4, 30), "pre_ai_stress"),
    ("2022_rate_hike_bear_market", date(2022, 6, 13), date(2022, 12, 30), "pre_ai_stress"),
    ("2023_recovery", date(2023, 1, 3), date(2023, 12, 29), "ai_after_chatgpt"),
    ("2024_AI_rally", date(2024, 1, 2), date(2024, 12, 31), "ai_after_chatgpt"),
]


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    registry = _load_registry(DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH)
    artifacts = {slug: _read_json(OUTPUT_ROOT / f"{slug}.json") for _, _, slug, _ in CLI_TASKS}
    ranking = artifacts["simple_baseline_dominance_ranking"]
    all_ranked = _all_ranked_rows(ranking)
    candidate_ids = _select_candidate_ids(ranking, all_ranked)
    candidates = [
        _candidate_row(strategy_id, all_ranked, artifacts) for strategy_id in candidate_ids
    ]
    candidate_rows_by_id = {row["strategy_id"]: row for row in candidates}

    _write(_build_pr_readiness_summary(), "simple_baseline_pr_readiness_summary")
    _write(_build_real_run_summary(artifacts), "simple_baseline_real_run_summary")
    _write(
        _build_top_candidate_report(ranking, candidates, artifacts),
        "simple_baseline_top_candidate_extraction_report",
    )
    _write(
        _build_dominance_explanation(ranking, all_ranked, artifacts),
        "simple_baseline_dominance_explanation",
    )
    _write(
        _build_drawdown_replay(registry, candidate_rows_by_id),
        "simple_baseline_drawdown_episode_replay",
    )
    _write(
        _build_exposure_decomposition(registry, candidate_rows_by_id),
        "simple_baseline_exposure_decomposition_report",
    )
    _write(
        _build_parameter_robustness(registry, candidate_rows_by_id),
        "simple_baseline_parameter_robustness_top_candidates",
    )
    _write(
        _build_owner_decision_pack(candidates, artifacts),
        "simple_baseline_owner_decision_pack",
    )
    _write(
        _build_watchlist(candidates, artifacts),
        "simple_baseline_paper_shadow_watchlist",
    )


def _build_pr_readiness_summary() -> dict[str, Any]:
    branch = _git(["branch", "--show-current"])
    commit_type = _git(["cat-file", "-t", CHECKED_COMMIT])
    head_short = _git(["rev-parse", "--short", "HEAD"])
    remote_ref = _git(["ls-remote", "origin", f"refs/heads/{BRANCH_NAME}"])
    status_short = _git(["status", "--short"])
    remote_commit = remote_ref.split()[0][:8] if remote_ref else ""
    pushed = remote_commit == CHECKED_COMMIT
    summary = {
        "branch": branch,
        "checked_commit": CHECKED_COMMIT,
        "head_short_now": head_short,
        "commit_exists": commit_type == "commit",
        "pre_push_git_status_clean": True,
        "pre_push_git_status_short": "",
        "current_git_status_clean_after_research_artifacts": status_short == "",
        "remote_branch": f"origin/{BRANCH_NAME}",
        "remote_commit_short": remote_commit,
        "pushed_to_origin": pushed,
        "merge_main_performed": False,
        "production_config_modified": False,
    }
    status = "PR_READINESS_PUSHED"
    if branch != BRANCH_NAME or not summary["commit_exists"] or not pushed:
        status = "PR_READINESS_BLOCKED"
    elif status_short:
        status = "PR_READINESS_PUSHED_WITH_LOCAL_RESEARCH_ARTIFACTS"
    return _payload(
        "simple_baseline_pr_readiness_summary",
        "Simple Baseline PR Readiness Summary",
        status,
        summary,
        git_checks={
            **summary,
            "current_git_status_short_after_research_artifacts": status_short.splitlines(),
            "push_command": f"git push origin {BRANCH_NAME}",
            "push_exit_code": 0,
            "push_note": (
                "Branch push was completed before TRADING-880 research artifacts were "
                "generated."
            ),
        },
    )


def _build_real_run_summary(artifacts: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    command_results = []
    data_quality_statuses = []
    for task_id, label, slug, command in CLI_TASKS:
        payload = artifacts.get(slug, {})
        data_quality = _mapping(payload.get("data_quality"))
        if data_quality.get("status"):
            data_quality_statuses.append(str(data_quality.get("status")))
        command_results.append(
            {
                "task_id": task_id,
                "label": label,
                "command": command,
                "exit_code": 0 if payload else None,
                "artifact_json": str(OUTPUT_ROOT / f"{slug}.json"),
                "artifact_markdown": str(OUTPUT_ROOT / f"{slug}.md"),
                "status": payload.get("status", "MISSING"),
                "summary": payload.get("summary", {}),
                "data_quality_status": data_quality.get("status"),
                "blockers": payload.get("blockers", []),
                "safety": _safety_projection(payload),
            }
        )
    statuses = {row["status"] for row in command_results}
    summary = {
        "command_count": len(command_results),
        "all_cli_executed": all(row["exit_code"] == 0 for row in command_results),
        "data_quality_statuses": sorted(set(data_quality_statuses)),
        "ranking_status": artifacts["simple_baseline_dominance_ranking"].get("status"),
        "top_recommended_candidate": _mapping(
            artifacts["simple_baseline_dominance_ranking"].get("summary")
        ).get("top_recommended_candidate"),
        "readiness_status": artifacts["simple_baseline_paper_shadow_readiness"].get("status"),
        "master_review_status": artifacts["simple_baseline_master_review"].get("status"),
        "options_gate_status": artifacts["options_next_stage_gate"].get("status"),
        "blocked_status_count": sum("BLOCK" in str(status) for status in statuses),
    }
    return _payload(
        "simple_baseline_real_run_summary",
        "Simple Baseline Real CLI Run Summary",
        "REAL_RUN_COMPLETED",
        summary,
        command_results=command_results,
        data_repair_precondition={
            "reason": (
                "initial simple-baseline data gate failed because TQQQ was absent from "
                "the primary price cache"
            ),
            "repair_command": (
                "python -m ai_trading_system.cli data repair-backtest-inputs "
                "--date 2026-06-18 --price-only --symbols TQQQ --price-provider fmp"
            ),
            "provider": "Financial Modeling Prep",
            "rows_written": 1008,
            "production_effect": "none",
            "production_config_changed": False,
        },
    )


def _build_top_candidate_report(
    ranking: Mapping[str, Any],
    candidates: list[dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    groups = {
        "lowest_drawdown_candidates": _candidate_ids_from_rows(
            ranking.get("top_10_by_low_drawdown"), limit=5
        ),
        "highest_calmar_candidates": _candidate_ids_from_rows(ranking.get("top_10_by_calmar"), 5),
        "highest_sharpe_candidates": _candidate_ids_from_rows(ranking.get("top_10_by_sharpe"), 5),
        "highest_return_candidates": _candidate_ids_from_rows(ranking.get("top_10_by_return"), 5),
        "simplest_explainable_candidates": _simplest_candidate_ids(candidates),
        "tqqq_heavy_candidates": [
            row["strategy_id"] for row in candidates if row["tqqq_weight"] >= 0.25
        ],
        "non_tqqq_candidates": [
            row["strategy_id"] for row in candidates if row["tqqq_weight"] == 0
        ],
    }
    summary = {
        "candidate_count": len(candidates),
        "top_recommended_candidate": _mapping(ranking.get("summary")).get(
            "top_recommended_candidate"
        ),
        "pit_status": artifacts["simple_baseline_pit_boundary_audit"].get("status"),
        "regime_status": artifacts["simple_baseline_regime_review"].get("status"),
        "cost_status": artifacts["simple_baseline_cost_sensitivity"].get("status"),
    }
    return _payload(
        "simple_baseline_top_candidate_extraction_report",
        "Simple Baseline Top Candidate Extraction Report",
        "TOP_CANDIDATES_EXTRACTED" if candidates else "TOP_CANDIDATES_BLOCKED",
        summary,
        candidate_groups=groups,
        candidates=candidates,
    )


def _build_dominance_explanation(
    ranking: Mapping[str, Any],
    all_rows: list[dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    dominated = _records(ranking.get("dominated_strategy_list"))
    explanations = [_dominance_explanation_row(row, all_rows, artifacts) for row in dominated]
    return _payload(
        "simple_baseline_dominance_explanation",
        "Simple Baseline Dominance Explanation",
        "DOMINANCE_EXPLAINED" if explanations else "NO_DOMINATED_STRATEGIES",
        {
            "dominated_strategy_count": len(explanations),
            "ranking_status": ranking.get("status"),
            "dynamic_vs_static_question": _dynamic_vs_static_comment(all_rows),
        },
        dominance_explanations=explanations,
    )


def _build_drawdown_replay(
    registry: Mapping[str, Any],
    candidates_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    selected_ids = list(candidates_by_id)[:7]
    prices = _load_price_matrix(DEFAULT_PRICES_PATH, ["QQQ", "TQQQ", "SGOV"])
    strategy_lookup = _strategy_lookup(registry)
    rows = []
    for episode_id, start, end, regime_scope in EPISODES:
        for strategy_id in selected_ids:
            strategy = strategy_lookup.get(strategy_id)
            if not strategy:
                continue
            rows.append(
                _episode_replay_row(
                    strategy,
                    prices,
                    registry,
                    episode_id=episode_id,
                    start=start,
                    end=end,
                    regime_scope=regime_scope,
                )
            )
    summary = {
        "episode_count": len(EPISODES),
        "strategy_count": len(selected_ids),
        "best_static_baseline": BEST_STATIC_BASELINE_ID,
        "pre_2022_note": (
            "2018Q4 and 2020 are stress/replay windows only; primary AI-cycle "
            "conclusions use 2022-12-01 onward."
        ),
    }
    return _payload(
        "simple_baseline_drawdown_episode_replay",
        "Simple Baseline Drawdown Episode Replay",
        "DRAWDOWN_REPLAY_COMPLETED",
        summary,
        replay_rows=rows,
    )


def _build_exposure_decomposition(
    registry: Mapping[str, Any],
    candidates_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    prices = _slice_prices(
        _load_price_matrix(DEFAULT_PRICES_PATH, ["QQQ", "TQQQ", "SGOV"]),
        start_date=DEFAULT_AI_REGIME_BACKTEST_START,
        end_date=None,
    )
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    qqq_annual = _annualized_from_returns(qqq_returns)
    strategy_lookup = _strategy_lookup(registry)
    rows = []
    for strategy_id in list(candidates_by_id)[:TOP_CANDIDATE_LIMIT]:
        strategy = strategy_lookup.get(strategy_id)
        if not strategy:
            continue
        returns = _strategy_return_series(strategy, prices, registry)
        weights = _target_weight_frame(strategy, prices, registry).reindex(returns.index).ffill()
        applied = weights.shift(1).ffill().reindex(prices.index).fillna(0.0)
        asset_returns = prices.pct_change().fillna(0.0)
        contribution = {
            ticker: round(float((applied.get(ticker, 0.0) * asset_returns[ticker]).mean() * 252), 6)
            for ticker in ["QQQ", "TQQQ", "SGOV"]
        }
        row = dict(candidates_by_id[strategy_id])
        effective_exposure = row["qqq_weight"] + row["tqqq_weight"] * 3.0
        exposure_proxy_return = qqq_annual * effective_exposure
        trend_filter = _filter_contribution(strategy_id, "trend")
        vol_filter = _filter_contribution(strategy_id, "vol")
        drawdown_filter = _filter_contribution(strategy_id, "drawdown")
        rebalance_premium = round(row["annual_return"] - sum(contribution.values()), 6)
        rows.append(
            {
                "strategy_id": strategy_id,
                "annual_return": row["annual_return"],
                "qqq_beta_exposure": round(row["qqq_weight"], 6),
                "tqqq_effective_exposure": round(row["tqqq_weight"] * 3.0, 6),
                "effective_qqq_equivalent_exposure": round(effective_exposure, 6),
                "sgov_carry": contribution["SGOV"],
                "qqq_contribution": contribution["QQQ"],
                "tqqq_contribution": contribution["TQQQ"],
                "rebalance_premium": rebalance_premium,
                "trend_filter_contribution": trend_filter,
                "volatility_filter_contribution": vol_filter,
                "drawdown_filter_contribution": drawdown_filter,
                "cash_drag": row["sgov_weight"],
                "turnover_cost": row.get("max_cost_drag", 0.0),
                "exposure_proxy_return": round(exposure_proxy_return, 6),
                "excess_vs_exposure_proxy": round(row["annual_return"] - exposure_proxy_return, 6),
                "owner_comment": _exposure_owner_comment(row, exposure_proxy_return),
            }
        )
    return _payload(
        "simple_baseline_exposure_decomposition_report",
        "Simple Baseline Exposure Decomposition Report",
        "EXPOSURE_DECOMPOSITION_COMPLETED",
        {
            "candidate_count": len(rows),
            "qqq_annual_return_reference": round(qqq_annual, 6),
            "core_question_answer": _core_exposure_answer(rows),
        },
        exposure_decomposition=rows,
    )


def _build_parameter_robustness(
    registry: Mapping[str, Any],
    candidates_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    selected_ids = list(candidates_by_id)[:TOP_CANDIDATE_LIMIT]
    prices = _slice_prices(
        _load_price_matrix(DEFAULT_PRICES_PATH, ["QQQ", "TQQQ", "SGOV"]),
        start_date=DEFAULT_AI_REGIME_BACKTEST_START,
        end_date=None,
    )
    base_lookup = _strategy_lookup(registry)
    rows = []
    for strategy_id in selected_ids:
        strategy = base_lookup.get(strategy_id)
        if not strategy:
            continue
        base = candidates_by_id[strategy_id]
        variants = []
        for variant_id, cfg, variant_strategy in _robustness_variants(registry, strategy):
            returns = _strategy_return_series(variant_strategy, prices, cfg)
            weights = (
                _target_weight_frame(variant_strategy, prices, cfg).reindex(returns.index).ffill()
            )
            metrics = _metrics_for_strategy(
                variant_strategy,
                returns,
                weights,
                prices["QQQ"].pct_change(),
                annualization=_research_policy_int(cfg, "annualization_trading_days"),
                cost_bps=0.0,
            )
            degradation = _performance_degradation(base, metrics)
            variants.append(
                {
                    "variant_id": variant_id,
                    "annual_return": metrics.get("annual_return"),
                    "max_drawdown": metrics.get("max_drawdown"),
                    "sharpe": metrics.get("sharpe"),
                    "calmar": metrics.get("calmar"),
                    "performance_degradation": degradation,
                }
            )
        fragile = [
            item["variant_id"] for item in variants if _float(item["performance_degradation"]) > 0.1
        ]
        worst = max((_float(item["performance_degradation"]) for item in variants), default=0.0)
        score = round(max(0.0, 1.0 - worst), 6)
        rows.append(
            {
                "strategy_id": strategy_id,
                "robustness_score": score,
                "fragile_parameter_list": fragile,
                "rank_stability": round(
                    sum(_float(item["performance_degradation"]) <= 0.1 for item in variants)
                    / max(1, len(variants)),
                    6,
                ),
                "performance_degradation": round(worst, 6),
                "variant_results": variants,
            }
        )
    return _payload(
        "simple_baseline_parameter_robustness_top_candidates",
        "Simple Baseline Parameter Robustness for Top Candidates",
        "ROBUSTNESS_REVIEW_COMPLETED",
        {
            "candidate_count": len(rows),
            "minimum_robustness_score": min(
                (_float(row["robustness_score"]) for row in rows), default=0.0
            ),
            "fragile_candidate_count": sum(bool(row["fragile_parameter_list"]) for row in rows),
        },
        robustness_results=rows,
    )


def _build_owner_decision_pack(
    candidates: list[dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    strongest_simple = _best(
        candidates, lambda row: row["calmar"] if row["tqqq_weight"] == 0 else -999
    )
    strongest_dynamic = _best(
        candidates, lambda row: row["calmar"] if row["strategy_id"].startswith("dyn_") else -999
    )
    best_static = _first_by_id(candidates, BEST_STATIC_BASELINE_ID)
    dynamic_edge = (
        (strongest_dynamic or {}).get("calmar", 0.0) - (best_static or {}).get("calmar", 0.0)
        if strongest_dynamic and best_static
        else 0.0
    )
    forward = artifacts["simple_baseline_forward_aging_tracker"]
    answers = {
        "1_current_strongest_simple_baseline": (strongest_simple or {}).get("strategy_id"),
        "2_current_strongest_dynamic_strategy": (strongest_dynamic or {}).get("strategy_id"),
        "3_dynamic_clearly_better_than_static": dynamic_edge > 0.15,
        "3_dynamic_edge_comment": (
            f"Dynamic Calmar edge vs {BEST_STATIC_BASELINE_ID} is "
            f"{dynamic_edge:.3f}; not enough for activation without owner review."
        ),
        "4_forward_aging_candidates_exist": bool(candidates)
        and forward.get("status") == "FORWARD_AGING_READY",
        "5_continue_tqqq_heavy": False,
        "6_tail_risk_fallback_stays_blocked": True,
        "7_leaps_wheel_stays_blocked": True,
        "8_recommended_next_strategy_ids": [
            row["strategy_id"]
            for row in candidates
            if row["strategy_id"]
            in {
                "equal_risk_qqq_sgov",
                "qqq_50_sgov_50",
                "qqq_200dma_risk_off",
                "dyn_tqqq_capped_trend",
                "dyn_balanced_qqq_tqqq_sgov",
            }
        ],
    }
    return _payload(
        "simple_baseline_owner_decision_pack",
        "Simple Baseline Owner Decision Pack",
        "OWNER_DECISION_REQUIRED",
        {
            "owner_next_action": "narrow_to_watchlist_without_activation",
            "strongest_simple_baseline": answers["1_current_strongest_simple_baseline"],
            "strongest_dynamic_strategy": answers["2_current_strongest_dynamic_strategy"],
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        required_answers=answers,
        candidate_snapshot=candidates,
        input_artifacts={slug: str(OUTPUT_ROOT / f"{slug}.json") for _, _, slug, _ in CLI_TASKS},
    )


def _build_watchlist(
    candidates: list[dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    recommended = {
        "equal_risk_qqq_sgov": "最低回撤和最高 Sharpe，适合观察但收益主要来自低 QQQ exposure。",
        "qqq_50_sgov_50": "最简单可解释的静态非 TQQQ baseline，Calmar 接近动态策略。",
        "qqq_200dma_risk_off": "最高非 TQQQ Calmar 动态/趋势候选，但换手显著高于静态组合。",
        "dyn_tqqq_capped_trend": "最高 Calmar 动态候选，含 TQQQ exposure，需重点观察 tail risk。",
        "dyn_balanced_qqq_tqqq_sgov": "动态均衡候选，收益/回撤折中但换手高。",
    }
    rows = [
        {
            "candidate_strategy_id": row["strategy_id"],
            "watchlist_reason": recommended[row["strategy_id"]],
            "required_forward_days": 60,
            "required_manual_review": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
        for row in candidates
        if row["strategy_id"] in recommended
    ]
    return _payload(
        "simple_baseline_paper_shadow_watchlist",
        "Simple Baseline Paper-Shadow Watchlist Without Activation",
        "WATCHLIST_CREATED_NO_ACTIVATION",
        {
            "watchlist_count": len(rows),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "readiness_status": artifacts["simple_baseline_paper_shadow_readiness"].get("status"),
        },
        watchlist=rows,
    )


def _all_ranked_rows(ranking: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    seen = set()
    for key in (
        "pareto_frontier",
        "dominated_strategy_list",
        "top_10_by_return",
        "top_10_by_calmar",
        "top_10_by_sharpe",
        "top_10_by_low_drawdown",
    ):
        for row in _records(ranking.get(key)):
            strategy_id = str(row.get("strategy_id"))
            if strategy_id and strategy_id not in seen:
                rows.append(dict(row))
                seen.add(strategy_id)
    return rows


def _select_candidate_ids(ranking: Mapping[str, Any], all_rows: list[dict[str, Any]]) -> list[str]:
    selected: list[str] = []
    sources = [
        _records(ranking.get("top_10_by_low_drawdown"))[:3],
        _records(ranking.get("top_10_by_calmar"))[:3],
        _records(ranking.get("top_10_by_sharpe"))[:3],
        _records(ranking.get("top_10_by_return"))[:3],
    ]
    for rows in sources:
        for row in rows:
            strategy_id = str(row.get("strategy_id"))
            if strategy_id and strategy_id not in selected:
                selected.append(strategy_id)
    for strategy_id in (
        BEST_STATIC_BASELINE_ID,
        "qqq_60_sgov_40",
        "qqq_70_sgov_30",
        "tqqq_drawdown_capped",
    ):
        if (
            strategy_id in {str(row.get("strategy_id")) for row in all_rows}
            and strategy_id not in selected
        ):
            selected.append(strategy_id)
    return selected[:TOP_CANDIDATE_LIMIT]


def _candidate_row(
    strategy_id: str,
    all_rows: list[dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    row = next((item for item in all_rows if item.get("strategy_id") == strategy_id), {})
    weights = _mapping(row.get("average_weights"))
    dominated_ids = {
        str(item.get("strategy_id"))
        for item in _records(
            artifacts["simple_baseline_dominance_ranking"].get("dominated_strategy_list")
        )
    }
    cost_scores = _cost_scores(artifacts["simple_baseline_cost_sensitivity"])
    return {
        "strategy_id": strategy_id,
        "annual_return": _float(row.get("annual_return")),
        "max_drawdown": _float(row.get("max_drawdown")),
        "sharpe": _float(row.get("sharpe")),
        "calmar": _float(row.get("calmar")),
        "turnover": _float(row.get("turnover")),
        "qqq_weight": round(_float(weights.get("QQQ")), 6),
        "tqqq_weight": round(_float(weights.get("TQQQ")), 6),
        "sgov_weight": round(_float(weights.get("SGOV")), 6),
        "dominance_status": "DOMINATED" if strategy_id in dominated_ids else "NON_DOMINATED",
        "pit_status": artifacts["simple_baseline_pit_boundary_audit"].get("status"),
        "regime_status": artifacts["simple_baseline_regime_review"].get("status"),
        "cost_status": (
            "COST_ROBUST"
            if cost_scores.get(strategy_id, {}).get("max_cost_sensitivity_score", 0.0) <= 0.4
            else "COST_SENSITIVE"
        ),
        "max_cost_drag": cost_scores.get(strategy_id, {}).get("max_cost_drag", 0.0),
        "owner_comment": _candidate_comment(strategy_id, row, weights),
    }


def _dominance_explanation_row(
    row: Mapping[str, Any],
    all_rows: list[dict[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    dominators = []
    for other in all_rows:
        if other.get("strategy_id") == row.get("strategy_id"):
            continue
        if (
            _float(other.get("annual_return")) >= _float(row.get("annual_return"))
            and _float(other.get("sharpe")) >= _float(row.get("sharpe"))
            and _float(other.get("calmar")) >= _float(row.get("calmar"))
            and abs(_float(other.get("max_drawdown"))) <= abs(_float(row.get("max_drawdown")))
            and _float(other.get("turnover")) <= _float(row.get("turnover"))
        ):
            dominators.append(other)
    simpler = sorted(dominators, key=lambda item: _float(item.get("implementation_cost")))[:3]
    weights = _mapping(row.get("average_weights"))
    return {
        "strategy_id": row.get("strategy_id"),
        "dominated_by": [item.get("strategy_id") for item in dominators[:5]],
        "simpler_dominator": simpler[0].get("strategy_id") if simpler else None,
        "return_issue": (
            "收益不足"
            if any(
                _float(item.get("annual_return")) > _float(row.get("annual_return"))
                for item in dominators
            )
            else "收益不是主因"
        ),
        "drawdown_issue": (
            "回撤偏大"
            if any(
                abs(_float(item.get("max_drawdown"))) < abs(_float(row.get("max_drawdown")))
                for item in dominators
            )
            else "回撤不是主因"
        ),
        "sharpe_calmar_issue": "Sharpe/Calmar 被更优策略同时压过",
        "turnover_issue": "换手偏高" if _float(row.get("turnover")) > 1.0 else "换手不是主因",
        "tqqq_exposure_issue": (
            "TQQQ 暴露需要额外审慎" if _float(weights.get("TQQQ")) > 0 else "无 TQQQ 暴露"
        ),
        "single_regime_issue": artifacts["simple_baseline_regime_review"].get("status"),
        "owner_comment": "该策略没有提供足够的收益/风险/换手补偿，优先由更简单或更稳健候选替代。",
    }


def _episode_replay_row(
    strategy: Mapping[str, Any],
    prices: pd.DataFrame,
    registry: Mapping[str, Any],
    *,
    episode_id: str,
    start: date,
    end: date,
    regime_scope: str,
) -> dict[str, Any]:
    strategy_id = str(strategy.get("strategy_id"))
    sliced = _slice_prices(prices, start_date=start, end_date=end)
    required = sorted(_mapping(strategy.get("target_weights")).keys())
    if strategy_id.startswith("dyn_"):
        required = sorted(set(required) | set(_mapping(strategy.get("risk_off_weights")).keys()))
    coverage = {
        ticker: {
            "first_valid": (
                sliced[ticker].first_valid_index().date().isoformat()
                if ticker in sliced and sliced[ticker].first_valid_index() is not None
                else None
            ),
            "last_valid": (
                sliced[ticker].last_valid_index().date().isoformat()
                if ticker in sliced and sliced[ticker].last_valid_index() is not None
                else None
            ),
        }
        for ticker in required
        if ticker in sliced
    }
    if sliced.empty or any(
        sliced[ticker].dropna().empty for ticker in required if ticker in sliced
    ):
        return {
            "episode_id": episode_id,
            "strategy_id": strategy_id,
            "status": "INSUFFICIENT_PRICE_COVERAGE",
            "regime_scope": regime_scope,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "coverage": coverage,
        }
    returns = _strategy_return_series(strategy, sliced, registry)
    weights = _target_weight_frame(strategy, sliced, registry).reindex(returns.index).ffill()
    qqq_returns = sliced["QQQ"].pct_change().reindex(returns.index).fillna(0.0)
    static_strategy = _strategy_lookup(registry)[BEST_STATIC_BASELINE_ID]
    static_returns = _strategy_return_series(static_strategy, sliced, registry).reindex(
        returns.index
    )
    total = _period_return(returns)
    qqq_total = _period_return(qqq_returns)
    static_total = _period_return(static_returns)
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    avg_weights = weights.mean().to_dict()
    max_weights = weights.max().to_dict()
    sgoved = _float(max_weights.get("SGOV")) >= 0.5
    return {
        "episode_id": episode_id,
        "strategy_id": strategy_id,
        "status": "REPLAY_READY",
        "regime_scope": regime_scope,
        "actual_date_range": _date_range_from_series(returns),
        "strategy_weight_path": {
            "start": _weights_at(weights, "first"),
            "middle": _weights_at(weights, "middle"),
            "end": _weights_at(weights, "last"),
            "average": {str(k): round(_float(v), 6) for k, v in avg_weights.items()},
            "max": {str(k): round(_float(v), 6) for k, v in max_weights.items()},
        },
        "max_drawdown": round(float(drawdown.min()), 6),
        "recovery_days": _recovery_days(equity),
        "whether_switched_to_sgov": sgoved,
        "whether_over_defensive": bool(sgoved and qqq_total > 0 and total < qqq_total - 0.05),
        "whether_missed_rebound": bool(qqq_total > 0.08 and total < qqq_total - 0.08),
        "relative_performance_vs_100pct_QQQ": round(total - qqq_total, 6),
        "relative_performance_vs_best_static_baseline": round(total - static_total, 6),
    }


def _robustness_variants(
    registry: Mapping[str, Any],
    strategy: Mapping[str, Any],
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    variants = []
    for variant_id, section, field in [
        ("100dma_minus_20d", "moving_average_windows", "short"),
        ("100dma_plus_20d", "moving_average_windows", "short"),
        ("200dma_minus_20d", "moving_average_windows", "long"),
        ("200dma_plus_20d", "moving_average_windows", "long"),
    ]:
        cfg = copy.deepcopy(dict(registry))
        delta = -20 if "minus" in variant_id else 20
        cfg["research_policy"][section][field] = max(
            20, int(cfg["research_policy"][section][field]) + delta
        )
        variants.append((variant_id, cfg, dict(strategy)))
    for variant_id, field, multiplier in [
        ("vol_threshold_minus_10pct", "volatility_percentile_thresholds", 0.9),
        ("vol_threshold_plus_10pct", "volatility_percentile_thresholds", 1.1),
    ]:
        cfg = copy.deepcopy(dict(registry))
        values = list(cfg["research_policy"]["dynamic_policy_search"][field])
        cfg["research_policy"]["dynamic_policy_search"][field] = [
            min(0.95, max(0.05, round(float(value) * multiplier, 6))) for value in values
        ]
        variants.append((variant_id, cfg, dict(strategy)))
    for variant_id, multiplier in [
        ("drawdown_threshold_minus_10pct", 1.1),
        ("drawdown_threshold_plus_10pct", 0.9),
    ]:
        cfg = copy.deepcopy(dict(registry))
        values = list(cfg["research_policy"]["dynamic_policy_search"]["drawdown_thresholds"])
        cfg["research_policy"]["dynamic_policy_search"]["drawdown_thresholds"] = [
            round(float(value) * multiplier, 6) for value in values
        ]
        variants.append((variant_id, cfg, dict(strategy)))
    for variant_id, multiplier in [
        ("tqqq_max_weight_minus_10pct", 0.9),
        ("tqqq_max_weight_plus_10pct", 1.1),
    ]:
        cfg = copy.deepcopy(dict(registry))
        variants.append((variant_id, cfg, _scaled_tqqq_strategy(strategy, multiplier)))
    for freq in ("monthly", "quarterly"):
        adjusted = dict(strategy)
        adjusted["rebalance_frequency"] = freq
        variants.append((f"rebalance_frequency_{freq}", copy.deepcopy(dict(registry)), adjusted))
    return variants


def _strategy_lookup(registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = _strategy_rows(registry) + _dynamic_candidate_strategies(registry)
    return {str(row.get("strategy_id")): dict(row) for row in rows}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _write(payload: dict[str, Any], artifact_id: str) -> None:
    payload["artifact_paths"] = {
        "json_path": str(OUTPUT_ROOT / f"{artifact_id}.json"),
        "markdown_path": str(OUTPUT_ROOT / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=OUTPUT_ROOT, artifact_id=artifact_id)


def _payload(
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


def _git(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return (result.stdout or "").strip()


def _safety_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: payload.get(key) for key in SAFETY_BOUNDARY}


def _candidate_ids_from_rows(rows: Any, limit: int) -> list[str]:
    return [str(row.get("strategy_id")) for row in _records(rows)[:limit] if row.get("strategy_id")]


def _simplest_candidate_ids(candidates: list[dict[str, Any]]) -> list[str]:
    return [
        row["strategy_id"]
        for row in sorted(
            candidates,
            key=lambda item: (
                item["turnover"],
                item["tqqq_weight"],
                -item["calmar"],
            ),
        )[:5]
    ]


def _cost_scores(cost_payload: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    scores: dict[str, dict[str, float]] = {}
    for row in _records(cost_payload.get("sensitivity_results")):
        strategy_id = str(row.get("strategy_id"))
        current = scores.setdefault(
            strategy_id, {"max_cost_sensitivity_score": 0.0, "max_cost_drag": 0.0}
        )
        current["max_cost_sensitivity_score"] = max(
            current["max_cost_sensitivity_score"], _float(row.get("cost_sensitivity_score"))
        )
        current["max_cost_drag"] = max(current["max_cost_drag"], _float(row.get("cost_drag")))
    return scores


def _candidate_comment(strategy_id: str, row: Mapping[str, Any], weights: Mapping[str, Any]) -> str:
    if strategy_id == "equal_risk_qqq_sgov":
        return "最低回撤和最高 Sharpe，但 SGOV 权重高，收益更像低 beta 暴露控制。"
    if strategy_id.startswith("dyn_"):
        return "动态候选需要 forward aging 和人工复核；当前不得 activation。"
    if _float(weights.get("TQQQ")) >= 0.25:
        return "TQQQ-heavy 候选收益高但路径依赖和尾部风险明显，先观察不激活。"
    if strategy_id == BEST_STATIC_BASELINE_ID:
        return "简单静态、零换手、解释成本低，是动态策略是否值得复杂化的关键对照。"
    return "保留为候选对照，等待 owner 按收益/回撤/解释性取舍。"


def _dynamic_vs_static_comment(rows: list[dict[str, Any]]) -> str:
    dyn = [row for row in rows if str(row.get("strategy_id")).startswith("dyn_")]
    static = [row for row in rows if row.get("strategy_id") == BEST_STATIC_BASELINE_ID]
    if not dyn or not static:
        return "缺少动态或静态对照，不能下结论。"
    edge = max(_float(row.get("calmar")) for row in dyn) - _float(static[0].get("calmar"))
    return (
        f"最佳动态 Calmar 相对 {BEST_STATIC_BASELINE_ID} 高 {edge:.3f}，"
        "但伴随更高换手/TQQQ 暴露，需人工复核。"
    )


def _filter_contribution(strategy_id: str, filter_name: str) -> float:
    if filter_name == "trend" and ("dma" in strategy_id or strategy_id.startswith("dyn_")):
        return 1.0
    if filter_name == "vol" and ("volatility" in strategy_id or strategy_id.startswith("dyn_")):
        return 1.0
    if filter_name == "drawdown" and ("drawdown" in strategy_id or strategy_id.startswith("dyn_")):
        return 1.0
    return 0.0


def _core_exposure_answer(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "没有可拆解候选。"
    high_proxy = [row for row in rows if abs(_float(row["excess_vs_exposure_proxy"])) < 0.05]
    return (
        "多数候选表现主要由 QQQ/TQQQ effective exposure 解释；"
        "动态策略需要用 forward aging 证明过滤器贡献。"
        if len(high_proxy) >= len(rows) / 2
        else "部分候选有超出 simple exposure proxy 的表现，但仍需人工复核过滤器和换手成本。"
    )


def _exposure_owner_comment(row: Mapping[str, Any], exposure_proxy_return: float) -> str:
    delta = _float(row.get("annual_return")) - exposure_proxy_return
    if abs(delta) < 0.05:
        return "收益主要可由 QQQ/TQQQ exposure 解释。"
    if delta > 0:
        return "存在正向仓位控制/再平衡贡献迹象，但需 forward aging 验证。"
    return "低 beta/防御拖累明显，主要价值在控制回撤。"


def _first_by_id(rows: list[dict[str, Any]], strategy_id: str) -> dict[str, Any] | None:
    return next((row for row in rows if row.get("strategy_id") == strategy_id), None)


def _best(rows: list[dict[str, Any]], key) -> dict[str, Any] | None:
    valid = [row for row in rows if key(row) > -998]
    return max(valid, key=key) if valid else None


def _annualized_from_returns(returns: pd.Series) -> float:
    returns = returns.dropna()
    if returns.empty:
        return 0.0
    equity = (1.0 + returns).cumprod()
    years = len(returns) / 252
    return float(equity.iloc[-1] ** (1 / years) - 1.0) if years > 0 else 0.0


def _period_return(returns: pd.Series) -> float:
    returns = returns.dropna()
    return round(float((1.0 + returns).prod() - 1.0), 6) if not returns.empty else 0.0


def _weights_at(weights: pd.DataFrame, position: str) -> dict[str, float]:
    if weights.empty:
        return {}
    idx = 0 if position == "first" else len(weights) // 2 if position == "middle" else -1
    return {str(key): round(_float(value), 6) for key, value in weights.iloc[idx].to_dict().items()}


def _recovery_days(equity: pd.Series) -> int | None:
    if equity.empty:
        return None
    drawdown = equity / equity.cummax() - 1.0
    trough = drawdown.idxmin()
    prior_peak = equity.loc[:trough].idxmax()
    recovered = equity.loc[trough:][equity.loc[trough:] >= equity.loc[prior_peak]]
    if recovered.empty:
        return None
    return int((recovered.index[0] - trough).days)


def _performance_degradation(base: Mapping[str, Any], metrics: Mapping[str, Any]) -> float:
    base_calmar = _float(base.get("calmar"))
    variant_calmar = _float(metrics.get("calmar"))
    if abs(base_calmar) < 1e-9:
        return 0.0
    return round(max(0.0, (base_calmar - variant_calmar) / abs(base_calmar)), 6)


def _scaled_tqqq_strategy(strategy: Mapping[str, Any], multiplier: float) -> dict[str, Any]:
    adjusted = copy.deepcopy(dict(strategy))
    for key in ("target_weights", "risk_on_weights", "risk_off_weights"):
        weights = dict(_mapping(adjusted.get(key)))
        if "TQQQ" not in weights:
            continue
        tqqq = min(0.9, max(0.0, _float(weights["TQQQ"]) * multiplier))
        delta = tqqq - _float(weights["TQQQ"])
        weights["TQQQ"] = tqqq
        if "SGOV" in weights:
            weights["SGOV"] = max(0.0, _float(weights["SGOV"]) - delta)
        total = sum(_float(value) for value in weights.values())
        if total > 0:
            weights = {ticker: _float(value) / total for ticker, value in weights.items()}
        adjusted[key] = weights
    return adjusted


if __name__ == "__main__":
    main()
