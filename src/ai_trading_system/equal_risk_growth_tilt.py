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
    _data_quality_gate,
    _load_price_matrix,
    _metrics_for_strategy,
    _slice_prices,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "equal_risk_growth_tilt_candidate_registry.yaml"
)
DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "growth_components"
)
DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "roadmap"
)
DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_owner_decision_pack.md"
)
DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_exploration_master_review.md"
)
DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)

DEFENSIVE_PRIMARY_ID = "equal_risk_qqq_sgov"
PRIMARY_QQQ_BENCHMARK_ID = "100_qqq"
TQQQ_DAILY_LEVERAGE_MULTIPLIER = 3.0

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

AI_REGIME_SUMMARY: dict[str, str] = {
    "market_regime": "ai_after_chatgpt",
    "anchor_event": "ChatGPT public launch",
    "anchor_date": "2022-11-30",
    "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
}


def run_growth_research_framing_correction(
    *,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _payload(
        report_type="growth_research_framing_correction",
        title="Growth Research Framing Correction",
        status="STRUCTURED_GROWTH_EXPLORATION_CONTINUES",
        summary={
            "previous_status": "NO_GROWTH_EDGE_FOUND",
            "corrected_status": "CURRENT_GROWTH_PATH_PAUSED",
            "active_path_count": 1,
            **_safety_summary(),
        },
        previous_status="NO_GROWTH_EDGE_FOUND",
        corrected_status=(
            "NO_GROWTH_EDGE_FOUND only applies to the current Controlled Growth V2 "
            "candidate batch; it is not a permanent conclusion about all offensive "
            "strategy research."
        ),
        paused_search_paths=[
            "controlled_growth_v2_current_candidate_batch",
            "unconstrained_growth_search_from_zero",
        ],
        active_research_paths=[
            "equal_risk_growth_tilt_structured_exploration",
        ],
        blocked_paths=[
            "paper_shadow_activation",
            "production_activation",
            "broker_action",
            "options_leaps_wheel",
            "tail_risk_fallback",
            "layer1_selector_restart",
            "tqqq_heavy_mainline",
        ],
        owner_next_action=(
            "review_equal_risk_growth_tilt_research_outputs_before_any_forward_aging_decision"
        ),
        report_registry_entry=_report_registry_entry(
            "growth_research_framing_correction",
            "Growth Research Framing Correction",
            "aits research strategies growth-research-framing-correction",
            "growth_research_framing_correction",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_growth_tilt_objective_contract(
    *,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    objective = _research_mapping(config, "objective_contract")
    missing = [
        field
        for field in (
            "objective_id",
            "primary_benchmark",
            "secondary_benchmark",
            "risk_anchor",
            "required_comparators",
        )
        if field not in objective
    ]
    status = (
        "GROWTH_TILT_OBJECTIVE_BLOCKED"
        if missing
        else "GROWTH_TILT_OBJECTIVE_READY"
    )
    payload = _payload(
        report_type="equal_risk_growth_tilt_objective_contract",
        title="Equal-Risk Growth Tilt Objective Contract",
        status=status,
        summary={
            "objective_id": objective.get("objective_id"),
            "primary_benchmark": objective.get("primary_benchmark"),
            "secondary_benchmark": objective.get("secondary_benchmark"),
            "risk_anchor": objective.get("risk_anchor"),
            "missing_field_count": len(missing),
            **_safety_summary(),
        },
        objective_id=objective.get("objective_id"),
        primary_objective="improve annual_return vs equal_risk_qqq_sgov",
        secondary_objective="reduce return gap vs 100_qqq",
        tertiary_objective="preserve acceptable max_drawdown / Calmar / Sharpe",
        primary_benchmark=objective.get("primary_benchmark"),
        secondary_benchmark=objective.get("secondary_benchmark"),
        risk_anchor=objective.get("risk_anchor"),
        required_comparators=objective.get("required_comparators", []),
        minimum_growth_tilt_candidate_requirements=_tier_requirement_text("tier_1"),
        growth_challenger_requirements=_tier_requirement_text("tier_2"),
        component_ready_requirements=_tier_requirement_text("tier_3"),
        policy_definition=objective,
        blockers=missing,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "equal_risk_growth_tilt_objective_contract",
            "Equal-Risk Growth Tilt Objective Contract",
            "aits research strategies equal-risk-growth-tilt-objective-contract",
            "equal_risk_growth_tilt_objective_contract",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_growth_tilt_registry_review(
    *,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    candidates = _candidate_family_records(config)
    issues = _registry_issues(config, candidates)
    if not candidates:
        status = "GROWTH_TILT_REGISTRY_BLOCKED"
    elif issues:
        status = "GROWTH_TILT_REGISTRY_PARTIAL"
    else:
        status = "GROWTH_TILT_REGISTRY_READY"
    payload = _payload(
        report_type="equal_risk_growth_tilt_registry_review",
        title="Equal-Risk Growth Tilt Registry Review",
        status=status,
        summary={
            "candidate_family_count": len(candidates),
            "issue_count": len(issues),
            "config_policy_id": config.get("policy_id"),
            **_safety_summary(),
        },
        registry_policy={
            "policy_id": config.get("policy_id"),
            "policy_metadata": config.get("policy_metadata"),
            "market_regime": config.get("market_regime"),
            "safety_boundary": config.get("safety_boundary"),
            "excluded_paths": config.get("excluded_paths"),
        },
        candidates=candidates,
        issues=issues,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "equal_risk_growth_tilt_registry_review",
            "Equal-Risk Growth Tilt Registry Review",
            "aits research strategies equal-risk-growth-tilt-registry-review",
            "equal_risk_growth_tilt_registry_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_cap_floor_tilt_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    grid = _search_grid(config, "cap_floor_tilt")
    candidates = []
    for qqq_max in _list(grid.get("qqq_max_weight")):
        for sgov_min in _list(grid.get("sgov_min_weight")):
            for rebalance in _list(grid.get("rebalance")):
                qqq_weight = min(_float(qqq_max), 1.0 - _float(sgov_min))
                candidates.append(
                    _candidate(
                        config,
                        "cap_floor_tilt",
                        strategy_suffix=(
                            f"q{_pct_token(qqq_max)}_s{_pct_token(sgov_min)}_{rebalance}"
                        ),
                        qqq_max_weight=_float(qqq_max),
                        sgov_min_weight=_float(sgov_min),
                        target_weights={"QQQ": qqq_weight, "SGOV": 1.0 - qqq_weight},
                        rebalance_rule=str(rebalance),
                    )
                )
    return _run_search(
        report_type="equal_risk_cap_floor_tilt_search",
        title="Equal-Risk Cap/Floor Tilt Search",
        command="aits research strategies equal-risk-cap-floor-tilt-search",
        status_found="CAP_FLOOR_TILT_CANDIDATES_FOUND",
        status_no_edge="CAP_FLOOR_TILT_NO_EDGE",
        status_blocked="CAP_FLOOR_TILT_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"search_grid": grid},
    )


def run_equal_risk_risk_budget_tilt_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    grid = _search_grid(config, "risk_budget_tilt")
    candidates = []
    qqq_budgets = _list(grid.get("qqq_risk_budget"))
    sgov_budgets = _list(grid.get("sgov_risk_budget"))
    for index, qqq_budget in enumerate(qqq_budgets):
        sgov_budget = sgov_budgets[index] if index < len(sgov_budgets) else 1.0 - qqq_budget
        for window in _list(grid.get("vol_lookback")):
            for rebalance in _list(grid.get("rebalance")):
                candidates.append(
                    _candidate(
                        config,
                        "risk_budget_tilt",
                        strategy_suffix=(
                            f"qrb{_pct_token(qqq_budget)}_srb{_pct_token(sgov_budget)}_"
                            f"w{int(_float(window))}_{rebalance}"
                        ),
                        qqq_risk_budget=_float(qqq_budget),
                        sgov_risk_budget=_float(sgov_budget),
                        vol_lookback=int(_float(window)),
                        rebalance_rule=str(rebalance),
                    )
                )
    return _run_search(
        report_type="equal_risk_risk_budget_tilt_search",
        title="Equal-Risk Risk-Budget Tilt Search",
        command="aits research strategies equal-risk-risk-budget-tilt-search",
        status_found="RISK_BUDGET_TILT_CANDIDATES_FOUND",
        status_no_edge="RISK_BUDGET_TILT_NO_EDGE",
        status_blocked="RISK_BUDGET_TILT_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"search_grid": grid},
    )


def run_equal_risk_trend_on_qqq_boost_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    grid = _search_grid(config, "trend_on_qqq_boost")
    candidates = []
    for boost in _list(grid.get("boost_amount")):
        for rebalance in _list(grid.get("rebalance")):
            candidates.append(
                _candidate(
                    config,
                    "trend_on_qqq_boost",
                    strategy_suffix=f"boost{_pct_token(boost)}_{rebalance}",
                    boost_amount=_float(boost),
                    rebalance_rule=str(rebalance),
                )
            )
    payload = _run_search(
        report_type="equal_risk_trend_on_qqq_boost_search",
        title="Equal-Risk Trend-On QQQ Boost Search",
        command="aits research strategies equal-risk-trend-on-qqq-boost-search",
        status_found="TREND_ON_BOOST_CANDIDATES_FOUND",
        status_no_edge="TREND_ON_BOOST_NO_EDGE",
        status_blocked="TREND_ON_BOOST_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"risk_on_conditions": _research_mapping(config, "trend_filter_rule")},
    )
    if payload["status"] == "TREND_ON_BOOST_CANDIDATES_FOUND" and _overfit_like(payload):
        payload["status"] = "TREND_ON_BOOST_OVERFITS"
        _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_missed_upside_compensation_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    policy = _research_mapping(config, "missed_upside_policy")
    candidates = []
    for threshold in _list(policy.get("thresholds")):
        for amount in _list(policy.get("compensation_amounts")):
            for ramp in _list(policy.get("ramp_days")):
                candidates.append(
                    _candidate(
                        config,
                        "missed_upside_compensation",
                        strategy_suffix=(
                            f"gap{_pct_token(threshold)}_amt{_pct_token(amount)}_"
                            f"ramp{int(_float(ramp))}"
                        ),
                        missed_upside_threshold=_float(threshold),
                        compensation_amount=_float(amount),
                        ramp_days=int(_float(ramp)),
                        rebalance_rule="threshold",
                    )
                )
    payload = _run_search(
        report_type="equal_risk_missed_upside_compensation_search",
        title="Equal-Risk Missed-Upside Compensation Search",
        command="aits research strategies equal-risk-missed-upside-compensation-search",
        status_found="MISSED_UPSIDE_COMPENSATION_FOUND",
        status_no_edge="MISSED_UPSIDE_COMPENSATION_NO_EDGE",
        status_blocked="MISSED_UPSIDE_COMPENSATION_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"trigger_policy": policy},
    )
    if payload["status"] == "MISSED_UPSIDE_COMPENSATION_FOUND" and any(
        _float(row.get("drawdown_penalty"))
        > _candidate_limit(config, "max_drawdown_increase_vs_equal_risk")
        for row in _records(payload.get("candidate_results"))
    ):
        payload["status"] = "MISSED_UPSIDE_COMPENSATION_RISKY"
        _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_small_tqqq_overlay_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    grid = _search_grid(config, "small_tqqq_overlay")
    candidates = []
    for weight in _list(grid.get("max_tqqq_weight")):
        for rebalance in _list(grid.get("rebalance")):
            candidates.append(
                _candidate(
                    config,
                    "small_tqqq_overlay",
                    strategy_suffix=f"tqqq{_pct_token(weight)}_{rebalance}",
                    max_tqqq_weight=_float(weight),
                    rebalance_rule=str(rebalance),
                )
            )
    payload = _run_search(
        report_type="equal_risk_small_tqqq_overlay_search",
        title="Equal-Risk Small TQQQ Overlay Search",
        command="aits research strategies equal-risk-small-tqqq-overlay-search",
        status_found="SMALL_TQQQ_OVERLAY_CANDIDATES_FOUND",
        status_no_edge="SMALL_TQQQ_OVERLAY_NO_EDGE",
        status_blocked="SMALL_TQQQ_OVERLAY_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"risk_on_only": True},
    )
    rows = _records(payload.get("candidate_results"))
    if payload["status"] == "SMALL_TQQQ_OVERLAY_CANDIDATES_FOUND":
        if any(_float(row.get("beta_adjusted_edge")) <= 0.0 for row in rows):
            payload["status"] = "SMALL_TQQQ_OVERLAY_BETA_EXPLAINS_EDGE"
        if any(_float(row.get("drawdown_increase_vs_equal_risk")) > 0.10 for row in rows):
            payload["status"] = "SMALL_TQQQ_OVERLAY_RISK_TOO_HIGH"
        _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_vol_target_growth_tilt_search(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    grid = _search_grid(config, "vol_target_growth_tilt")
    candidates = []
    target_specs: list[dict[str, Any]] = [
        {"target_vol_mode": "absolute", "target_vol": _float(value)}
        for value in _list(grid.get("target_vol_absolute"))
    ]
    target_specs.extend(
        {"target_vol_mode": "equal_risk_plus", "target_vol_additive": _float(value)}
        for value in _list(grid.get("target_vol_additive_pp"))
    )
    for target in target_specs:
        for window in _list(grid.get("vol_lookback")):
            for qqq_max in _list(grid.get("qqq_max_weight")):
                for sgov_min in _list(grid.get("sgov_min_weight")):
                    target_value = target.get(
                        "target_vol",
                        target.get("target_vol_additive"),
                    )
                    token = (
                        f"tv{_pct_token(target_value)}"
                        f"_w{int(_float(window))}_q{_pct_token(qqq_max)}_s{_pct_token(sgov_min)}"
                    )
                    candidates.append(
                        _candidate(
                            config,
                            "vol_target_growth_tilt",
                            strategy_suffix=token,
                            target_vol_mode=target["target_vol_mode"],
                            target_vol=target.get("target_vol"),
                            target_vol_additive=target.get("target_vol_additive"),
                            vol_lookback=int(_float(window)),
                            qqq_max_weight=_float(qqq_max),
                            sgov_min_weight=_float(sgov_min),
                            rebalance_rule="monthly",
                        )
                    )
    payload = _run_search(
        report_type="equal_risk_vol_target_growth_tilt_search",
        title="Equal-Risk Vol-Target Growth Tilt Search",
        command="aits research strategies equal-risk-vol-target-growth-tilt-search",
        status_found="VOL_TARGET_GROWTH_TILT_FOUND",
        status_no_edge="VOL_TARGET_GROWTH_TILT_NO_EDGE",
        status_blocked="VOL_TARGET_GROWTH_TILT_BLOCKED",
        candidates=candidates,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        extra_payload={"search_grid": grid},
    )
    if payload["status"] == "VOL_TARGET_GROWTH_TILT_FOUND" and any(
        _float(row.get("drawdown_increase_vs_equal_risk")) > _candidate_limit(
            config, "max_drawdown_increase_vs_equal_risk"
        )
        for row in _records(payload.get("candidate_results"))
    ):
        payload["status"] = "VOL_TARGET_GROWTH_TILT_TOO_RISKY"
        _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_growth_tilt_ranking_tiering(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    searches = _growth_tilt_search_sources(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    config = _load_config(config_path)
    rows = _dedupe_candidates(
        [
            row
            for source in searches.values()
            for row in _records(source.get("candidate_results"))
        ]
    )
    for row in rows:
        row["candidate_tier"] = _candidate_tier(row, config)
    tier1 = [row for row in rows if row["candidate_tier"] == "GROWTH_TILT_CANDIDATE"]
    tier2 = [row for row in rows if row["candidate_tier"] == "GROWTH_CHALLENGER"]
    tier3 = [row for row in rows if row["candidate_tier"] == "COMPONENT_READY_GROWTH"]
    rejected = [row for row in rows if row["candidate_tier"] == "REJECTED"]
    blocked = [
        key for key, source in searches.items() if _blocked_status(str(source.get("status")))
    ]
    if blocked:
        status = "GROWTH_TILT_RANKING_BLOCKED"
    elif tier1 or tier2 or tier3:
        status = "GROWTH_TILT_CANDIDATES_RANKED"
    else:
        status = "NO_GROWTH_TILT_EDGE"
    payload = _payload(
        report_type="equal_risk_growth_tilt_ranking_tiering",
        title="Equal-Risk Growth Tilt Ranking And Tiering",
        status=status,
        summary={
            "candidate_count": len(rows),
            "tier_1_count": len(tier1),
            "tier_2_count": len(tier2),
            "tier_3_count": len(tier3),
            "rejected_count": len(rejected),
            "top_candidate": rows[0]["strategy_id"] if rows else None,
            "data_quality_status": _first_data_quality_status(searches.values()),
            **_safety_summary(),
        },
        top_by_return_edge_vs_equal_risk=_top(rows, "return_edge_vs_equal_risk"),
        top_by_return_gap_reduction_vs_100_qqq=_top(
            rows, "return_gap_reduction_vs_100_qqq"
        ),
        top_by_calmar=_top(rows, "calmar"),
        top_by_sharpe=_top(rows, "sharpe"),
        top_by_low_drawdown=_top(rows, "max_drawdown"),
        top_by_low_turnover=_top(rows, "turnover", reverse=False),
        tier_1_growth_tilt_candidates=tier1,
        tier_2_growth_challengers=tier2,
        tier_3_component_ready_candidates=tier3,
        rejected_candidates=rejected,
        source_statuses={key: value.get("status") for key, value in searches.items()},
        source_artifacts=_artifact_paths_by_report(searches),
        blockers=blocked,
        report_registry_entry=_report_registry_entry(
            "equal_risk_growth_tilt_ranking_tiering",
            "Equal-Risk Growth Tilt Ranking And Tiering",
            "aits research strategies equal-risk-growth-tilt-ranking-tiering",
            "equal_risk_growth_tilt_ranking_tiering",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_beta_risk_budget_attribution(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _ranking_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = dict(
        _ranking_payload
        or run_equal_risk_growth_tilt_ranking_tiering(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    rows = _selected_ranked_candidates(ranking)
    config = _load_config(config_path)
    attribution_rows = [_attribution_row(row, config) for row in rows]
    beta_explains = attribution_rows and all(
        _float(row.get("beta_adjusted_return_edge")) <= 0.0
        for row in attribution_rows
    )
    risk_budget_present = any(
        row.get("candidate_family") == "risk_budget_tilt"
        and _float(row.get("beta_adjusted_return_edge")) > 0.0
        for row in attribution_rows
    )
    if _blocked_status(str(ranking.get("status"))):
        status = "ATTRIBUTION_BLOCKED"
    elif beta_explains:
        status = "BETA_EXPLAINS_MOST_TILT_EDGE"
    elif risk_budget_present:
        status = "RISK_BUDGET_TILT_EDGE_PRESENT"
    elif attribution_rows:
        status = "GROWTH_TILT_ATTRIBUTION_READY"
    else:
        status = "ATTRIBUTION_INCONCLUSIVE"
    payload = _payload(
        report_type="growth_tilt_beta_risk_budget_attribution",
        title="Growth Tilt Beta/Risk-Budget Attribution",
        status=status,
        summary={
            "candidate_count": len(attribution_rows),
            "beta_explains_most_tilt_edge": bool(beta_explains),
            "risk_budget_tilt_edge_present": bool(risk_budget_present),
            **_safety_summary(),
        },
        attribution_rows=attribution_rows,
        source_artifacts={"ranking": ranking.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "growth_tilt_beta_risk_budget_attribution",
            "Growth Tilt Beta/Risk-Budget Attribution",
            "aits research strategies growth-tilt-beta-risk-budget-attribution",
            "growth_tilt_beta_risk_budget_attribution",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_period_drawdown_replay(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _ranking_payload: Mapping[str, Any] | None = None,
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
            report_type="growth_tilt_period_drawdown_replay",
            title="Growth Tilt Period/Drawdown Replay",
            status="GROWTH_TILT_PERIOD_REPLAY_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    ranking = dict(
        _ranking_payload
        or run_equal_risk_growth_tilt_ranking_tiering(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    selected = _selected_ranked_candidates(ranking)
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    candidates = [_candidate_from_row(row) for row in selected]
    benchmarks = _benchmark_candidates(config)
    equal = next(row for row in benchmarks if row["strategy_id"] == DEFENSIVE_PRIMARY_ID)
    qqq = next(row for row in benchmarks if row["strategy_id"] == PRIMARY_QQQ_BENCHMARK_ID)
    equal_returns, _ = _returns_and_weights(equal, prices, config)
    qqq_returns, _ = _returns_and_weights(qqq, prices, config)
    periods = _periods_with_largest_drawdowns(config, prices)
    replay_rows = []
    summaries = []
    for candidate in candidates:
        returns, _weights = _returns_and_weights(candidate, prices, config)
        candidate_rows = [
            _period_replay_row(
                candidate,
                returns,
                equal_returns,
                qqq_returns,
                period,
                config,
            )
            for period in periods
        ]
        replay_rows.extend(candidate_rows)
        summaries.append(_period_summary(candidate, candidate_rows))
    regime_concentrated = any(row["ai_rally_dependency"] for row in summaries)
    drawdown_risk = any(
        _float(row.get("max_drawdown")) < _float(row.get("qqq_max_drawdown")) * 1.05
        and _float(row.get("max_drawdown")) < -0.35
        for row in replay_rows
    )
    if drawdown_risk:
        status = "GROWTH_TILT_DRAWDOWN_RISK_TOO_HIGH"
    elif regime_concentrated:
        status = "GROWTH_TILT_REGIME_CONCENTRATED"
    else:
        status = "GROWTH_TILT_PERIOD_REPLAY_READY"
    payload = _payload(
        report_type="growth_tilt_period_drawdown_replay",
        title="Growth Tilt Period/Drawdown Replay",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "period_row_count": len(replay_rows),
            "ai_rally_dependent_candidate_count": sum(
                1 for row in summaries if row["ai_rally_dependency"]
            ),
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        period_rows=replay_rows,
        candidate_summaries=summaries,
        required_coverage=[
            "2022_rate_hike_bear_market",
            "2023_recovery",
            "2024_ai_rally",
            "2025_to_latest",
            "largest QQQ drawdown in available data",
            "largest growth_tilt_drawdown",
            "high-rate SGOV carry period",
        ],
        data_quality=data_gate,
        source_artifacts={"ranking": ranking.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "growth_tilt_period_drawdown_replay",
            "Growth Tilt Period/Drawdown Replay",
            "aits research strategies growth-tilt-period-drawdown-replay",
            "growth_tilt_period_drawdown_replay",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_cost_turnover_sensitivity(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _ranking_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = dict(
        _ranking_payload
        or run_equal_risk_growth_tilt_ranking_tiering(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    config = _load_config(config_path)
    rows = _selected_ranked_candidates(ranking)
    scenario_rows = [
        scenario
        for row in rows
        for scenario in _cost_sensitivity_rows(row, config)
    ]
    if _blocked_status(str(ranking.get("status"))):
        status = "GROWTH_TILT_COST_BLOCKED"
    elif any(
        _float(row.get("turnover")) > _candidate_limit(config, "max_turnover")
        for row in rows
    ):
        status = "GROWTH_TILT_TURNOVER_TOO_HIGH"
    elif any(
        _float(row.get("performance_degradation"))
        > _candidate_limit(config, "max_cost_degradation")
        for row in scenario_rows
    ):
        status = "GROWTH_TILT_COST_SENSITIVE"
    else:
        status = "GROWTH_TILT_COST_ROBUST"
    payload = _payload(
        report_type="growth_tilt_cost_turnover_sensitivity",
        title="Growth Tilt Cost/Turnover Sensitivity",
        status=status,
        summary={
            "candidate_count": len(rows),
            "scenario_row_count": len(scenario_rows),
            "max_performance_degradation": max(
                (_float(row.get("performance_degradation")) for row in scenario_rows),
                default=0.0,
            ),
            **_safety_summary(),
        },
        scenario_rows=scenario_rows,
        source_artifacts={"ranking": ranking.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "growth_tilt_cost_turnover_sensitivity",
            "Growth Tilt Cost/Turnover Sensitivity",
            "aits research strategies growth-tilt-cost-turnover-sensitivity",
            "growth_tilt_cost_turnover_sensitivity",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_growth_tilt_tradeoff_frontier(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _ranking_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = dict(
        _ranking_payload
        or run_equal_risk_growth_tilt_ranking_tiering(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    rows = _records(ranking.get("top_by_return_edge_vs_equal_risk"))
    frontier_candidates = [_frontier_role(row) for row in rows]
    useful_roles = {"BALANCED_CORE", "GROWTH_TILT_CANDIDATE", "GROWTH_CHALLENGER"}
    useful = [
        row
        for row in frontier_candidates
        if row["recommended_role"] in useful_roles
    ]
    status = (
        "TRADEOFF_FRONTIER_BLOCKED"
        if _blocked_status(str(ranking.get("status")))
        else "TRADEOFF_FRONTIER_READY"
        if useful
        else "TRADEOFF_FRONTIER_NO_USEFUL_TILT"
    )
    payload = _payload(
        report_type="equal_risk_growth_tilt_tradeoff_frontier",
        title="Equal-Risk vs Growth Tilt Tradeoff Frontier",
        status=status,
        summary={
            "frontier_candidate_count": len(frontier_candidates),
            "useful_tilt_count": len(useful),
            "recommended_candidate_by_role": _recommended_by_role(frontier_candidates),
            **_safety_summary(),
        },
        frontier_candidates=frontier_candidates,
        return_vs_drawdown_frontier=_top(frontier_candidates, "annual_return"),
        return_vs_calmar_frontier=_top(frontier_candidates, "calmar"),
        return_vs_turnover_frontier=_top(frontier_candidates, "turnover", reverse=False),
        defensive_to_balanced_transition_point=_first_role(
            frontier_candidates, "BALANCED_CORE"
        ),
        balanced_to_growth_transition_point=_first_role(
            frontier_candidates, "GROWTH_TILT_CANDIDATE"
        ),
        recommended_candidate_by_role=_recommended_by_role(frontier_candidates),
        source_artifacts={"ranking": ranking.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "equal_risk_growth_tilt_tradeoff_frontier",
            "Equal-Risk Growth Tilt Tradeoff Frontier",
            "aits research strategies equal-risk-growth-tilt-tradeoff-frontier",
            "equal_risk_growth_tilt_tradeoff_frontier",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_definition_lock_versioning(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _ranking_payload: Mapping[str, Any] | None = None,
    _attribution_payload: Mapping[str, Any] | None = None,
    _replay_payload: Mapping[str, Any] | None = None,
    _cost_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = dict(
        _ranking_payload
        or run_equal_risk_growth_tilt_ranking_tiering(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    attribution = dict(
        _attribution_payload
        or run_growth_tilt_beta_risk_budget_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _ranking_payload=ranking,
        )
    )
    replay = dict(
        _replay_payload
        or run_growth_tilt_period_drawdown_replay(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _ranking_payload=ranking,
        )
    )
    cost = dict(
        _cost_payload
        or run_growth_tilt_cost_turnover_sensitivity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _ranking_payload=ranking,
        )
    )
    source_blocked = [
        name
        for name, payload in {
            "ranking": ranking,
            "attribution": attribution,
            "replay": replay,
            "cost": cost,
        }.items()
        if _blocked_status(str(payload.get("status")))
    ]
    rows = [
        row
        for row in _selected_ranked_candidates(ranking)
        if row.get("candidate_tier") in {
            "GROWTH_TILT_CANDIDATE",
            "GROWTH_CHALLENGER",
            "COMPONENT_READY_GROWTH",
        }
    ]
    lock_rows = [
        {
            "strategy_id": row.get("strategy_id"),
            "definition_hash": row.get("definition_hash"),
            "candidate_family": row.get("candidate_family"),
            "policy_definition": row.get("policy_definition"),
            "source_artifacts": {
                "ranking": ranking.get("artifact_paths", {}),
                "attribution": attribution.get("artifact_paths", {}),
                "replay": replay.get("artifact_paths", {}),
                "cost": cost.get("artifact_paths", {}),
            },
            "lock_status": "LOCKED_RESEARCH_DEFINITION",
            "versioning_commentary": (
                "Any strategy definition change must create a new strategy_id/version "
                "before forward-aging review."
            ),
        }
        for row in rows[:5]
    ]
    if source_blocked:
        status = "GROWTH_TILT_DEFINITION_BLOCKED"
    elif lock_rows:
        status = "GROWTH_TILT_DEFINITION_LOCKED"
    else:
        status = "GROWTH_TILT_DEFINITION_NEEDS_REVIEW"
    payload = _payload(
        report_type="growth_tilt_definition_lock_versioning",
        title="Growth Tilt Definition Lock And Versioning",
        status=status,
        summary={
            "locked_definition_count": len(lock_rows),
            "blocked_source_count": len(source_blocked),
            **_safety_summary(),
        },
        locked_definitions=lock_rows,
        source_statuses={
            "ranking": ranking.get("status"),
            "attribution": attribution.get("status"),
            "replay": replay.get("status"),
            "cost": cost.get("status"),
        },
        blockers=source_blocked,
        report_registry_entry=_report_registry_entry(
            "growth_tilt_definition_lock_versioning",
            "Growth Tilt Definition Lock And Versioning",
            "aits research strategies growth-tilt-definition-lock-versioning",
            "growth_tilt_definition_lock_versioning",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_forward_aging_readiness_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _ranking_payload: Mapping[str, Any] | None = None,
    _attribution_payload: Mapping[str, Any] | None = None,
    _replay_payload: Mapping[str, Any] | None = None,
    _cost_payload: Mapping[str, Any] | None = None,
    _definition_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = dict(
        _ranking_payload
        or run_equal_risk_growth_tilt_ranking_tiering(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    definitions = dict(
        _definition_payload
        or run_growth_tilt_definition_lock_versioning(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _ranking_payload=ranking,
            _attribution_payload=_attribution_payload,
            _replay_payload=_replay_payload,
            _cost_payload=_cost_payload,
        )
    )
    cost = dict(
        _cost_payload
        or _read_json_or_empty(output_root / "growth_tilt_cost_turnover_sensitivity.json")
    )
    replay = dict(
        _replay_payload
        or _read_json_or_empty(output_root / "growth_tilt_period_drawdown_replay.json")
    )
    config = _load_config(config_path)
    candidate = _best_gate_candidate(ranking)
    blockers = _forward_aging_blockers(candidate, definitions, cost, replay, config)
    warnings = _forward_aging_warnings(candidate, ranking, replay)
    reviewable = bool(candidate) and not blockers
    if _blocked_status(str(ranking.get("status"))) or _blocked_status(
        str(definitions.get("status"))
    ):
        status = "GROWTH_TILT_FORWARD_AGING_BLOCKED"
    elif reviewable:
        status = "GROWTH_TILT_FORWARD_AGING_REVIEWABLE"
    elif candidate:
        status = "GROWTH_TILT_RESEARCH_ONLY"
    else:
        status = "NO_GROWTH_TILT_CANDIDATE"
    payload = _payload(
        report_type="growth_tilt_forward_aging_readiness_gate",
        title="Growth Tilt Forward-Aging Readiness Gate",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "candidate_tier": candidate.get("candidate_tier"),
            "forward_aging_watchlist_allowed": False,
            "forward_aging_reviewable_after_owner_manual_review": reviewable,
            "blocking_reason_count": len(blockers),
            "warning_reason_count": len(warnings),
            **_safety_summary(),
        },
        candidate_strategy_id=candidate.get("strategy_id"),
        candidate_tier=candidate.get("candidate_tier"),
        forward_aging_watchlist_allowed=False,
        forward_aging_reviewable_after_owner_manual_review=reviewable,
        blocking_reasons=blockers,
        warning_reasons=warnings,
        required_forward_days=_candidate_limit(config, "required_forward_days"),
        source_artifacts={
            "ranking": ranking.get("artifact_paths", {}),
            "definition_lock": definitions.get("artifact_paths", {}),
            "replay": replay.get("artifact_paths", {}),
            "cost": cost.get("artifact_paths", {}),
        },
        report_registry_entry=_report_registry_entry(
            "growth_tilt_forward_aging_readiness_gate",
            "Growth Tilt Forward-Aging Readiness Gate",
            "aits research strategies growth-tilt-forward-aging-readiness-gate",
            "growth_tilt_forward_aging_readiness_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_owner_decision_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    ranking = run_equal_risk_growth_tilt_ranking_tiering(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    attribution = run_growth_tilt_beta_risk_budget_attribution(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        _ranking_payload=ranking,
    )
    replay = run_growth_tilt_period_drawdown_replay(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        _ranking_payload=ranking,
    )
    cost = run_growth_tilt_cost_turnover_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        _ranking_payload=ranking,
    )
    gate = run_growth_tilt_forward_aging_readiness_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        _ranking_payload=ranking,
        _attribution_payload=attribution,
        _replay_payload=replay,
        _cost_payload=cost,
    )
    candidate = _best_gate_candidate(ranking)
    blocked = _source_blockers(
        {
            "ranking": ranking,
            "attribution": attribution,
            "replay": replay,
            "cost": cost,
            "gate": gate,
        }
    )
    recommendation = _owner_recommendation(candidate, gate, blocked)
    answers = {
        "1_most_valuable_growth_tilt_candidate": candidate.get("strategy_id"),
        "2_return_improvement_vs_equal_risk": candidate.get("return_edge_vs_equal_risk"),
        "3_return_gap_vs_100_qqq_narrowed": _float(
            candidate.get("return_gap_reduction_vs_100_qqq")
        )
        > 0.0,
        "4_is_only_higher_beta": attribution.get("status") == "BETA_EXPLAINS_MOST_TILT_EDGE",
        "5_drawdown_acceptable": not bool(gate.get("blocking_reasons")),
        "6_only_ai_rally_effective": replay.get("status")
        == "GROWTH_TILT_REGIME_CONCENTRATED",
        "7_cost_turnover_acceptable": cost.get("status") == "GROWTH_TILT_COST_ROBUST",
        "8_allow_research_only_forward_aging_watchlist": gate.get(
            "forward_aging_reviewable_after_owner_manual_review"
        ),
        "9_keep_original_equal_risk_defensive_primary": True,
        "10_continue_no_paper_shadow_no_production_no_broker": True,
    }
    payload = _payload(
        report_type="growth_tilt_owner_decision_pack",
        title="Growth Tilt Owner Decision Pack",
        status="GROWTH_TILT_OWNER_DECISION_PACK_READY" if not blocked else "BLOCKED",
        summary={
            "owner_recommendation": recommendation,
            "candidate_strategy_id": candidate.get("strategy_id"),
            "candidate_tier": candidate.get("candidate_tier"),
            "blocked_source_count": len(blocked),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        source_statuses={
            "ranking": ranking.get("status"),
            "attribution": attribution.get("status"),
            "replay": replay.get("status"),
            "cost": cost.get("status"),
            "gate": gate.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "ranking": ranking,
                "attribution": attribution,
                "replay": replay,
                "cost": cost,
                "gate": gate,
            }
        ),
        blockers=blocked,
        report_registry_entry=_report_registry_entry(
            "growth_tilt_owner_decision_pack",
            "Growth Tilt Owner Decision Pack",
            "aits research strategies growth-tilt-owner-decision-pack",
            "growth_tilt_owner_decision_pack",
            extra_artifact_globs=["docs/research/growth_tilt_owner_decision_pack.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Tilt Owner Decision Pack")
    payload["owner_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_exploration_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    framing = run_growth_research_framing_correction(output_root=output_root)
    owner = run_growth_tilt_owner_decision_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        docs_path=owner_docs_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    ranking = _read_json_or_empty(output_root / "equal_risk_growth_tilt_ranking_tiering.json")
    tier1 = _records(ranking.get("tier_1_growth_tilt_candidates"))
    tier2 = _records(ranking.get("tier_2_growth_challengers"))
    tier3 = _records(ranking.get("tier_3_component_ready_candidates"))
    blocked = _source_blockers({"framing": framing, "owner": owner, "ranking": ranking})
    if blocked:
        status = "GROWTH_EXPLORATION_BLOCKED"
    elif tier3 or tier2:
        status = "GROWTH_TILT_FOUND"
    elif tier1:
        status = "BALANCED_CORE_CANDIDATE_FOUND"
    else:
        status = "CONTINUE_STRUCTURED_GROWTH_EXPLORATION"
    answers = {
        "1_current_growth_research_should_continue": True,
        "2_growth_tilt_candidate_found": bool(tier1 or tier2 or tier3),
        "3_growth_challenger_found": bool(tier2 or tier3),
        "4_component_ready_growth_found": bool(tier3),
        "5_balanced_core_candidate_found": bool(tier1),
        "6_original_equal_risk_remains_defensive_primary": True,
        "7_controlled_growth_v2_remains_paused": True,
        "8_layer1_selector_remains_archived": True,
        "9_next_minimum_task": _master_next_task(tier1, tier2, tier3),
    }
    payload = _payload(
        report_type="growth_exploration_master_review",
        title="Growth Exploration Master Review",
        status=status,
        summary={
            "tier_1_count": len(tier1),
            "tier_2_count": len(tier2),
            "tier_3_count": len(tier3),
            "owner_recommendation": owner.get("owner_recommendation"),
            "blocked_source_count": len(blocked),
            **_safety_summary(),
        },
        required_answers=answers,
        source_statuses={
            "growth_research_framing_correction": framing.get("status"),
            "growth_tilt_owner_decision_pack": owner.get("status"),
            "equal_risk_growth_tilt_ranking_tiering": ranking.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {"framing": framing, "owner": owner, "ranking": ranking}
        ),
        blockers=blocked,
        report_registry_entry=_report_registry_entry(
            "growth_exploration_master_review",
            "Growth Exploration Master Review",
            "aits research strategies growth-exploration-master-review",
            "growth_exploration_master_review",
            extra_artifact_globs=["docs/research/growth_exploration_master_review.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Exploration Master Review")
    payload["master_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_roadmap_update_after_growth_tilt_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    growth_master_docs_path: Path = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    growth_owner_docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    master = run_growth_exploration_master_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=growth_output_root,
        docs_path=growth_master_docs_path,
        owner_docs_path=growth_owner_docs_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    if _blocked_status(str(master.get("status"))):
        status = "ROADMAP_BLOCKED"
        growth_track = "blocked"
    elif master.get("status") in {"GROWTH_TILT_FOUND", "BALANCED_CORE_CANDIDATE_FOUND"}:
        status = "ROADMAP_CONTINUE_RESEARCH"
        growth_track = "continue_growth_tilt_research"
    else:
        status = "ROADMAP_KEEP_EQUAL_RISK_ONLY"
        growth_track = "only_equal_risk_forward_aging"
    payload = _payload(
        report_type="roadmap_update_after_growth_tilt_review",
        title="Roadmap Update After Growth Tilt Review",
        status=status,
        summary={
            "current_primary_track": "equal_risk_qqq_sgov_defensive_primary",
            "growth_tilt_track_status": growth_track,
            "layer1_selector_status": "archived_dry_run_only",
            "tail_risk_status": "blocked_not_restarted",
            "options_status": "blocked_not_started",
            **_safety_summary(),
        },
        current_primary_track="equal_risk_qqq_sgov_defensive_primary",
        growth_tilt_track_status=growth_track,
        layer1_selector_status="archived_dry_run_only",
        tail_risk_status="blocked_not_restarted",
        options_status="blocked_not_started",
        next_minimum_tasks=_roadmap_next_tasks(growth_track),
        owner_next_action="review_growth_tilt_master_review_and_decide_research_continuation",
        source_artifacts={"growth_exploration_master_review": master.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "roadmap_update_after_growth_tilt_review",
            "Roadmap Update After Growth Tilt Review",
            "aits research strategies roadmap-update-after-growth-tilt-review",
            "roadmap_update_after_growth_tilt_review",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_reader_brief_safety_preview(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    gate = run_growth_tilt_forward_aging_readiness_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    preview = {
        "growth_tilt_research_only_status": "research_only",
        "candidate_tier": gate.get("candidate_tier"),
        "forward_aging_watchlist_status": gate.get("status"),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    prohibited_hits = _prohibited_reader_brief_hits(preview)
    if _blocked_status(str(gate.get("status"))):
        status = "GROWTH_TILT_READER_PREVIEW_BLOCKED"
    elif prohibited_hits:
        status = "GROWTH_TILT_READER_PREVIEW_AMBIGUOUS"
    else:
        status = "GROWTH_TILT_READER_PREVIEW_SAFE"
    payload = _payload(
        report_type="growth_tilt_reader_brief_safety_preview",
        title="Growth Tilt Reader Brief Safety Preview",
        status=status,
        summary={
            "candidate_tier": gate.get("candidate_tier"),
            "forward_aging_watchlist_status": gate.get("status"),
            "prohibited_phrase_hit_count": len(prohibited_hits),
            **_safety_summary(),
        },
        allowed_display=preview,
        prohibited_display=[
            "买入",
            "卖出",
            "应调仓",
            "实盘仓位",
            "真实交易建议",
            "production ready",
            "paper-shadow active",
        ],
        prohibited_phrase_hits=prohibited_hits,
        source_artifacts={"forward_aging_gate": gate.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "growth_tilt_reader_brief_safety_preview",
            "Growth Tilt Reader Brief Safety Preview",
            "aits research strategies growth-tilt-reader-brief-safety-preview",
            "growth_tilt_reader_brief_safety_preview",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def _run_search(
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
        data_quality_status=str(data_gate["status"]),
    )
    edge_rows = [row for row in rows if _is_tier1_candidate(row, config)]
    status = status_found if edge_rows else status_no_edge
    payload = _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "candidate_count": len(rows),
            "growth_tilt_candidate_count": len(edge_rows),
            "top_candidate": rows[0]["strategy_id"] if rows else None,
            "data_quality_status": data_gate["status"],
            "requested_date_range": _requested_range(rows, start_date, end_date),
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
    data_quality_status: str,
) -> list[dict[str, Any]]:
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    benchmarks = _benchmark_metric_rows(prices, config)
    equal = benchmarks[DEFENSIVE_PRIMARY_ID]
    qqq = benchmarks[PRIMARY_QQQ_BENCHMARK_ID]
    rows = [
        _metric_row(candidate, prices, config, equal, qqq, data_quality_status)
        for candidate in candidates
    ]
    return sorted(rows, key=lambda row: _float(row.get("return_edge_vs_equal_risk")), reverse=True)


def _benchmark_metric_rows(
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    rows = {}
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    for candidate in _benchmark_candidates(config):
        returns, weights = _returns_and_weights(candidate, prices, config)
        metrics = _metrics_for_strategy(
            candidate,
            returns,
            weights,
            qqq_returns,
            annualization=_annualization(config),
            cost_bps=0.0,
        )
        rows[candidate["strategy_id"]] = {
            **metrics,
            "returns": returns,
            "weights": weights,
        }
    return rows


def _metric_row(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    equal: Mapping[str, Any],
    qqq: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    returns, weights = _returns_and_weights(candidate, prices, config)
    metrics = _metrics_for_strategy(
        candidate,
        returns,
        weights,
        qqq_returns,
        annualization=_annualization(config),
        cost_bps=0.0,
    )
    avg_weights = _mapping(metrics.get("average_weights"))
    equal_return = _float(equal.get("annual_return"))
    qqq_return = _float(qqq.get("annual_return"))
    annual_return = _float(metrics.get("annual_return"))
    drawdown = _float(metrics.get("max_drawdown"))
    equal_drawdown = _float(equal.get("max_drawdown"))
    qqq_drawdown = _float(qqq.get("max_drawdown"))
    qqq_gap_equal = qqq_return - equal_return
    qqq_gap_candidate = qqq_return - annual_return
    risk_on_days = _int(candidate.get("risk_on_days"))
    beta = _beta(returns, qqq_returns)
    effective_leverage = _float(avg_weights.get("QQQ")) + (
        _float(avg_weights.get("TQQQ")) * TQQQ_DAILY_LEVERAGE_MULTIPLIER
    )
    beta_adjusted_edge = (annual_return - equal_return) - max(beta - 1.0, 0.0) * qqq_return
    definition = _policy_definition(candidate)
    row = {
        "strategy_id": candidate.get("strategy_id"),
        "candidate_id": candidate.get("strategy_id"),
        "candidate_family": candidate.get("candidate_family"),
        "base_strategy_id": DEFENSIVE_PRIMARY_ID,
        "definition_hash": _stable_hash(definition),
        "policy_definition": definition,
        "asset_universe": candidate.get("asset_universe"),
        "weight_bounds": candidate.get("weight_bounds"),
        "rebalance_rule": candidate.get("rebalance_rule"),
        "data_quality_status": data_quality_status,
        "backtest_result": "BACKTEST_READY",
        "annual_return": metrics.get("annual_return"),
        "annual_return_edge_vs_equal_risk": _round(annual_return - equal_return),
        "return_edge_vs_equal_risk": _round(annual_return - equal_return),
        "annual_return_gap_vs_100_qqq": _round(qqq_gap_candidate),
        "return_gap_vs_100_qqq": _round(qqq_gap_candidate),
        "return_edge_vs_100_qqq": _round(annual_return - qqq_return),
        "return_gap_reduction_vs_100_qqq": _round(qqq_gap_equal - qqq_gap_candidate),
        "max_drawdown": metrics.get("max_drawdown"),
        "drawdown_increase_vs_equal_risk": _round(abs(drawdown) - abs(equal_drawdown)),
        "max_drawdown_vs_100_qqq": _round(abs(qqq_drawdown) - abs(drawdown)),
        "sharpe": metrics.get("sharpe"),
        "calmar": metrics.get("calmar"),
        "turnover": metrics.get("turnover"),
        "switch_count": metrics.get("rebalance_count"),
        "average_qqq_weight": _round(avg_weights.get("QQQ")),
        "average_tqqq_weight": _round(avg_weights.get("TQQQ")),
        "average_sgov_weight": _round(avg_weights.get("SGOV")),
        "average_weights": metrics.get("average_weights"),
        "max_tqqq_weight": _round(
            weights.get("TQQQ", pd.Series(0.0, index=weights.index)).max()
        ),
        "effective_qqq_beta": _round(beta),
        "effective_leverage": _round(effective_leverage),
        "cash_drag_reduction": _round(
            _float(_mapping(equal.get("average_weights")).get("SGOV"))
            - _float(avg_weights.get("SGOV"))
        ),
        "missed_upside_reduction": _round(annual_return - equal_return),
        "risk_on_days": risk_on_days,
        "late_risk_off_cost": _round(max(abs(drawdown) - abs(equal_drawdown), 0.0)),
        "late_risk_on_cost": _round(max(qqq_gap_candidate, 0.0)),
        "beta_adjusted_edge": _round(beta_adjusted_edge),
        "beta_adjusted_return_edge": _round(beta_adjusted_edge),
        "beta_adjusted_calmar_edge": _round(
            _float(metrics.get("calmar")) - _float(equal.get("calmar")) - max(beta - 1.0, 0.0)
        ),
        "beta_adjusted_sharpe_edge": _round(
            _float(metrics.get("sharpe")) - _float(equal.get("sharpe")) - max(beta - 1.0, 0.0)
        ),
        "drawdown_penalty": _round(max(abs(drawdown) - abs(equal_drawdown), 0.0)),
        "tqqq_contribution": _round(
            _asset_contribution(weights, prices["TQQQ"].pct_change().fillna(0.0), "TQQQ", config)
            if "TQQQ" in prices
            else 0.0
        ),
        "leverage_drag": _round(_leverage_drag(weights, prices, config)),
        "path_dependency_risk": _round(
            _float(avg_weights.get("TQQQ")) * abs(_float(metrics.get("max_drawdown")))
        ),
        "risk_contribution_qqq": _round(
            _float(avg_weights.get("QQQ"))
            * _realized_vol(prices["QQQ"], 60, _annualization(config)).mean()
        ),
        "risk_contribution_sgov": _round(
            _float(avg_weights.get("SGOV"))
            * _realized_vol(prices["SGOV"], 60, _annualization(config)).mean()
        ),
        "vol_target_error": _vol_target_error(candidate, returns, config),
        "forward_aging_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
        "requested_date_range": metrics.get("requested_date_range"),
    }
    if candidate.get("candidate_family") == "risk_budget_tilt":
        row["risk_budget_config"] = {
            "qqq_risk_budget": candidate.get("qqq_risk_budget"),
            "sgov_risk_budget": candidate.get("sgov_risk_budget"),
            "vol_lookback": candidate.get("vol_lookback"),
        }
    if candidate.get("candidate_family") == "missed_upside_compensation":
        row["missed_upside_threshold"] = candidate.get("missed_upside_threshold")
        row["compensation_amount"] = candidate.get("compensation_amount")
        row["rebound_capture"] = row["missed_upside_reduction"]
        row["over_risk_on_count"] = risk_on_days
    if candidate.get("candidate_family") == "small_tqqq_overlay":
        row["average_tqqq_weight"] = _round(avg_weights.get("TQQQ"))
        row["return_edge_vs_100_qqq"] = _round(annual_return - qqq_return)
    if candidate.get("candidate_family") == "vol_target_growth_tilt":
        row["target_vol"] = candidate.get("target_vol") or candidate.get("target_vol_additive")
        row["realized_vol"] = _round(returns.std(ddof=0) * math.sqrt(_annualization(config)))
    return row


def _returns_and_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> tuple[pd.Series, pd.DataFrame]:
    weights = _weight_frame(candidate, prices, config)
    if "risk_on_days" not in candidate and "risk_on_mask" in weights.attrs:
        try:
            candidate["risk_on_days"] = int(weights.attrs["risk_on_mask"].sum())  # type: ignore[index]
        except TypeError:
            pass
    asset_returns = prices.pct_change().fillna(0.0)
    applied = weights.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
    returns = (applied * asset_returns.reindex(columns=applied.columns).fillna(0.0)).sum(axis=1)
    return returns, weights


def _weight_frame(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    family = str(candidate.get("candidate_family") or "")
    if candidate.get("special_policy") == "equal_risk":
        return _equal_risk_weights(prices, config)
    target = _mapping(candidate.get("target_weights"))
    if target:
        return _static_weight_frame(
            target,
            prices,
            str(candidate.get("rebalance_rule", "monthly")),
            config,
        )
    if family == "risk_budget_tilt":
        return _risk_budget_weights(candidate, prices, config)
    if family == "trend_on_qqq_boost":
        return _trend_boost_weights(candidate, prices, config)
    if family == "missed_upside_compensation":
        return _missed_upside_weights(candidate, prices, config)
    if family == "small_tqqq_overlay":
        return _small_tqqq_overlay_weights(candidate, prices, config)
    if family == "vol_target_growth_tilt":
        return _vol_target_weights(candidate, prices, config)
    return _equal_risk_weights(prices, config)


def _equal_risk_weights(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    policy = _research_mapping(config, "equal_risk")
    window = _int(_research_mapping(config, "realized_vol_windows").get("medium"), 60)
    annualization = _annualization(config)
    qqq_vol = _realized_vol(prices["QQQ"], window, annualization)
    sgov_vol = _realized_vol(prices["SGOV"], window, annualization)
    inv_qqq = 1.0 / qqq_vol.replace(0.0, math.nan)
    inv_sgov = 1.0 / sgov_vol.replace(0.0, math.nan)
    qqq = (inv_qqq / (inv_qqq + inv_sgov)).clip(
        lower=_float(policy.get("min_weight")),
        upper=_float(policy.get("max_weight")),
    )
    qqq = qqq.fillna(0.5)
    return _apply_rebalance(
        pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index),
        "monthly",
        config,
    )


def _static_weight_frame(
    target: Mapping[str, Any],
    prices: pd.DataFrame,
    rebalance_rule: str,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    weights = _normalise_weights(target)
    frame = pd.DataFrame(index=prices.index, columns=sorted(weights), data=0.0)
    for ticker, weight in weights.items():
        frame[ticker] = weight
    return _apply_rebalance(frame, rebalance_rule, config)


def _risk_budget_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    window = _int(candidate.get("vol_lookback"), 60)
    qqq_budget = _float(candidate.get("qqq_risk_budget"))
    sgov_budget = _float(candidate.get("sgov_risk_budget"))
    qqq_vol = _realized_vol(prices["QQQ"], window, _annualization(config)).replace(0.0, math.nan)
    sgov_vol = _realized_vol(prices["SGOV"], window, _annualization(config)).replace(0.0, math.nan)
    qqq_raw = qqq_budget / qqq_vol
    sgov_raw = sgov_budget / sgov_vol
    qqq = (qqq_raw / (qqq_raw + sgov_raw)).clip(lower=0.0, upper=0.95).fillna(0.5)
    frame = pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)
    return _apply_rebalance(frame, str(candidate.get("rebalance_rule", "monthly")), config)


def _trend_boost_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    base = _equal_risk_weights(prices, config)
    risk_on = _risk_on_mask(prices, config)
    boost = _float(candidate.get("boost_amount"))
    qqq = base["QQQ"] + risk_on.astype(float) * boost
    qqq = qqq.clip(lower=0.0, upper=0.90)
    frame = pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)
    frame.attrs["risk_on_mask"] = risk_on
    return _apply_rebalance(frame, str(candidate.get("rebalance_rule", "monthly")), config)


def _missed_upside_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    base = _equal_risk_weights(prices, config)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    equal_returns = (
        base.shift(1).ffill() * prices[["QQQ", "SGOV"]].pct_change().fillna(0.0)
    ).sum(axis=1)
    window = _int(_research_mapping(config, "missed_upside_policy").get("trailing_gap_window"), 60)
    qqq_trailing = (1.0 + qqq_returns).rolling(window).apply(math.prod, raw=False) - 1.0
    equal_trailing = (1.0 + equal_returns).rolling(window).apply(math.prod, raw=False) - 1.0
    gap = qqq_trailing - equal_trailing
    signal = _risk_on_mask(prices, config) & (
        gap >= _float(candidate.get("missed_upside_threshold"))
    )
    ramp = _ramp_factor(signal, _int(candidate.get("ramp_days"), 10))
    boost = ramp * _float(candidate.get("compensation_amount"))
    qqq = (base["QQQ"] + boost).clip(lower=0.0, upper=0.90)
    frame = pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=prices.index)
    frame.attrs["risk_on_mask"] = signal
    return _apply_rebalance(frame, str(candidate.get("rebalance_rule", "threshold")), config)


def _small_tqqq_overlay_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    base = _equal_risk_weights(prices, config)
    risk_on = _risk_on_mask(prices, config)
    overlay = risk_on.astype(float) * _float(candidate.get("max_tqqq_weight"))
    qqq = base["QQQ"].copy()
    sgov = (base["SGOV"] - overlay).clip(lower=0.0)
    shortfall = (qqq + sgov + overlay) - 1.0
    qqq = (qqq - shortfall.clip(lower=0.0)).clip(lower=0.0)
    frame = pd.DataFrame({"QQQ": qqq, "TQQQ": overlay, "SGOV": sgov}, index=prices.index)
    total = frame.sum(axis=1).replace(0.0, math.nan)
    frame = frame.div(total, axis=0).fillna(0.0)
    frame.attrs["risk_on_mask"] = risk_on
    return _apply_rebalance(frame, str(candidate.get("rebalance_rule", "monthly")), config)


def _vol_target_weights(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    window = _int(candidate.get("vol_lookback"), 60)
    annualization = _annualization(config)
    qqq_vol = _realized_vol(prices["QQQ"], window, annualization).replace(0.0, math.nan)
    equal_returns, _ = _returns_and_weights(
        {"strategy_id": DEFENSIVE_PRIMARY_ID, "special_policy": "equal_risk"},
        prices,
        config,
    )
    equal_vol = equal_returns.rolling(window).std(ddof=0) * math.sqrt(annualization)
    if candidate.get("target_vol_mode") == "equal_risk_plus":
        target_vol = equal_vol + _float(candidate.get("target_vol_additive"))
    else:
        target_vol = pd.Series(_float(candidate.get("target_vol")), index=prices.index)
    qqq = (target_vol / qqq_vol).clip(
        lower=0.0,
        upper=min(
            _float(candidate.get("qqq_max_weight")),
            1.0 - _float(candidate.get("sgov_min_weight")),
        ),
    )
    qqq = qqq.fillna(0.5)
    sgov = (1.0 - qqq).clip(lower=_float(candidate.get("sgov_min_weight")), upper=1.0)
    qqq = 1.0 - sgov
    return _apply_rebalance(
        pd.DataFrame({"QQQ": qqq, "SGOV": sgov}, index=prices.index),
        str(candidate.get("rebalance_rule", "monthly")),
        config,
    )


def _growth_tilt_search_sources(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config_path: Path,
    output_root: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, dict[str, Any]]:
    kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_prices_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": output_root,
        "as_of_date": as_of_date,
        "start_date": start_date,
        "end_date": end_date,
    }
    return {
        "equal_risk_cap_floor_tilt_search": run_equal_risk_cap_floor_tilt_search(**kwargs),
        "equal_risk_risk_budget_tilt_search": run_equal_risk_risk_budget_tilt_search(**kwargs),
        "equal_risk_trend_on_qqq_boost_search": run_equal_risk_trend_on_qqq_boost_search(**kwargs),
        "equal_risk_missed_upside_compensation_search": (
            run_equal_risk_missed_upside_compensation_search(**kwargs)
        ),
        "equal_risk_small_tqqq_overlay_search": run_equal_risk_small_tqqq_overlay_search(**kwargs),
        "equal_risk_vol_target_growth_tilt_search": (
            run_equal_risk_vol_target_growth_tilt_search(**kwargs)
        ),
    }


def _candidate(
    config: Mapping[str, Any],
    family: str,
    *,
    strategy_suffix: str,
    **extra: Any,
) -> dict[str, Any]:
    base = next(
        row for row in _candidate_family_records(config) if row.get("candidate_family") == family
    )
    strategy_id = f"{base['strategy_id']}_{strategy_suffix}".replace(".", "")
    return {
        **base,
        "strategy_id": strategy_id,
        "display_name": strategy_id,
        "candidate_family": family,
        "base_strategy_id": DEFENSIVE_PRIMARY_ID,
        **extra,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _candidate_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping(row.get("policy_definition"))
    return {
        **policy,
        "strategy_id": row.get("strategy_id"),
        "candidate_family": row.get("candidate_family"),
        "asset_universe": row.get("asset_universe"),
        "weight_bounds": row.get("weight_bounds"),
        "rebalance_rule": row.get("rebalance_rule"),
    }


def _benchmark_candidates(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in _records(config.get("benchmarks"))]


def _candidate_family_records(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in _records(config.get("candidate_families"))]


def _registry_issues(
    config: Mapping[str, Any],
    candidates: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    required_fields = {
        "strategy_id",
        "candidate_family",
        "base_strategy_id",
        "asset_universe",
        "weight_bounds",
        "risk_budget_rule",
        "trend_filter_rule",
        "volatility_filter_rule",
        "drawdown_guard_rule",
        "rebalance_rule",
        "max_turnover",
        "max_tqqq_weight",
        "max_effective_qqq_beta",
        "uses_options",
        "uses_margin",
        "paper_shadow_allowed",
        "production_allowed",
        "broker_action",
    }
    seen: set[str] = set()
    safety = _mapping(config.get("safety_boundary"))
    if safety.get("paper_shadow_allowed") is not False:
        issues.append({"issue_id": "registry_safety_paper_shadow_not_false"})
    if safety.get("production_allowed") is not False:
        issues.append({"issue_id": "registry_safety_production_not_false"})
    if safety.get("broker_action") != "none":
        issues.append({"issue_id": "registry_safety_broker_action_not_none"})
    for row in candidates:
        strategy_id = str(row.get("strategy_id") or "")
        if strategy_id in seen:
            issues.append({"strategy_id": strategy_id, "issue_id": "duplicate_strategy_id"})
        seen.add(strategy_id)
        for field in sorted(required_fields - set(row)):
            issues.append({"strategy_id": strategy_id, "issue_id": f"missing_{field}"})
        if strategy_id == DEFENSIVE_PRIMARY_ID:
            issues.append(
                {"strategy_id": strategy_id, "issue_id": "overwrites_original_equal_risk"}
            )
        if row.get("base_strategy_id") != DEFENSIVE_PRIMARY_ID:
            issues.append({"strategy_id": strategy_id, "issue_id": "base_strategy_not_equal_risk"})
        if row.get("uses_options") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "uses_options_not_false"})
        if row.get("uses_margin") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "uses_margin_not_false"})
        if row.get("paper_shadow_allowed") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "paper_shadow_not_false"})
        if row.get("production_allowed") is not False:
            issues.append({"strategy_id": strategy_id, "issue_id": "production_not_false"})
        if row.get("broker_action") != "none":
            issues.append({"strategy_id": strategy_id, "issue_id": "broker_action_not_none"})
        if _float(row.get("max_tqqq_weight")) > _candidate_limit(config, "max_tqqq_weight"):
            issues.append({"strategy_id": strategy_id, "issue_id": "max_tqqq_weight_exceeded"})
        if _float(row.get("max_effective_qqq_beta")) > _candidate_limit(
            config, "max_effective_qqq_beta"
        ):
            issues.append({"strategy_id": strategy_id, "issue_id": "max_beta_exceeded"})
    return issues


def _candidate_tier(row: Mapping[str, Any], config: Mapping[str, Any]) -> str:
    tier1 = _is_tier1_candidate(row, config)
    tier2 = _is_tier2_candidate(row, config)
    tier3 = _is_tier3_candidate(row, config)
    if tier3:
        return "COMPONENT_READY_GROWTH"
    if tier2:
        return "GROWTH_CHALLENGER"
    if tier1:
        return "GROWTH_TILT_CANDIDATE"
    return "REJECTED"


def _is_tier1_candidate(row: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    return (
        _float(row.get("return_edge_vs_equal_risk")) > 0.0
        and _float(row.get("drawdown_increase_vs_equal_risk"))
        <= _candidate_limit(config, "max_drawdown_increase_vs_equal_risk")
        and _float(row.get("turnover")) <= _candidate_limit(config, "max_turnover")
        and str(row.get("data_quality_status")) not in {"FAIL", "BLOCKED"}
    )


def _is_tier2_candidate(row: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    gap_reduction = _float(row.get("return_gap_reduction_vs_100_qqq"))
    equal_edge = _float(row.get("return_edge_vs_equal_risk"))
    close_to_qqq = (
        gap_reduction > 0.0
        and gap_reduction
        >= abs(equal_edge) * _candidate_limit(config, "qqq_return_gap_close_ratio_for_challenger")
    )
    return (
        _is_tier1_candidate(row, config)
        and close_to_qqq
        and _float(row.get("max_drawdown_vs_100_qqq"))
        >= -_candidate_limit(config, "max_drawdown_increase_vs_equal_risk")
    )


def _is_tier3_candidate(row: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    return (
        _is_tier2_candidate(row, config)
        and _float(row.get("beta_adjusted_edge"))
        > _candidate_limit(config, "beta_adjusted_edge_minimum")
        and _float(row.get("max_tqqq_weight")) <= _candidate_limit(config, "max_tqqq_weight")
    )


def _attribution_row(row: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    annual = _float(row.get("annual_return"))
    beta_attr = _float(row.get("effective_qqq_beta")) * (
        annual - _float(row.get("beta_adjusted_return_edge"))
    )
    tqqq_attr = _float(row.get("average_tqqq_weight")) * annual * (
        TQQQ_DAILY_LEVERAGE_MULTIPLIER - 1.0
    )
    sgov_attr = _float(row.get("average_sgov_weight")) * annual
    timing = annual - beta_attr - tqqq_attr - sgov_attr
    risk_budget_tilt = (
        _float(row.get("beta_adjusted_return_edge"))
        if row.get("candidate_family") == "risk_budget_tilt"
        else 0.0
    )
    return {
        "strategy_id": row.get("strategy_id"),
        "effective_qqq_beta": row.get("effective_qqq_beta"),
        "effective_leverage": row.get("effective_leverage"),
        "average_qqq_weight": row.get("average_qqq_weight"),
        "average_tqqq_weight": row.get("average_tqqq_weight"),
        "average_sgov_weight": row.get("average_sgov_weight"),
        "return_attribution_qqq_beta": _round(beta_attr),
        "return_attribution_tqqq_overlay": _round(tqqq_attr),
        "return_attribution_sgov_carry": _round(sgov_attr),
        "return_attribution_risk_budget_tilt": _round(risk_budget_tilt),
        "return_attribution_timing": _round(timing),
        "beta_adjusted_return_edge": row.get("beta_adjusted_return_edge"),
        "beta_adjusted_calmar_edge": row.get("beta_adjusted_calmar_edge"),
        "risk_budget_commentary": (
            "risk_budget_tilt_edge_visible"
            if risk_budget_tilt > _candidate_limit(config, "beta_adjusted_edge_minimum")
            else "beta_or_static_exposure_explains_most_observed_edge"
        ),
    }


def _period_replay_row(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    equal_returns: pd.Series,
    qqq_returns: pd.Series,
    period: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    selected = _period_slice(returns, period)
    equal = equal_returns.reindex(selected.index).fillna(0.0)
    qqq = qqq_returns.reindex(selected.index).fillna(0.0)
    metrics = _series_metrics(selected, _annualization(config))
    equal_metrics = _series_metrics(equal, _annualization(config))
    qqq_metrics = _series_metrics(qqq, _annualization(config))
    return {
        "period": period.get("period_id"),
        "strategy_id": candidate.get("strategy_id"),
        "return": metrics["annual_return"],
        "max_drawdown": metrics["max_drawdown"],
        "qqq_max_drawdown": qqq_metrics["max_drawdown"],
        "recovery_days": metrics["recovery_days"],
        "relative_vs_equal_risk": _round(metrics["annual_return"] - equal_metrics["annual_return"]),
        "relative_vs_100_qqq": _round(metrics["annual_return"] - qqq_metrics["annual_return"]),
        "missed_rebound_cost": _round(
            max(qqq_metrics["annual_return"] - metrics["annual_return"], 0.0)
        ),
        "late_risk_off_cost": _round(
            max(abs(metrics["max_drawdown"]) - abs(equal_metrics["max_drawdown"]), 0.0)
        ),
        "late_risk_on_cost": _round(
            max(qqq_metrics["annual_return"] - metrics["annual_return"], 0.0)
        ),
        "ai_rally_dependency": period.get("period_id") == "2024_ai_rally"
        and metrics["annual_return"] > equal_metrics["annual_return"],
        "period_commentary": _period_commentary(metrics, equal_metrics, qqq_metrics),
        "sample_count": len(selected),
    }


def _period_summary(
    candidate: Mapping[str, Any],
    rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    wins = [row for row in rows if _float(row.get("relative_vs_equal_risk")) > 0.0]
    ai_wins = [row for row in wins if row.get("period") == "2024_ai_rally"]
    return {
        "strategy_id": candidate.get("strategy_id"),
        "period_count": len(rows),
        "win_vs_equal_risk_count": len(wins),
        "ai_rally_dependency": bool(wins) and len(wins) == len(ai_wins),
    }


def _cost_sensitivity_rows(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _research_mapping(config, "cost_turnover_sensitivity")
    rows = []
    for scenario, bps in _mapping(policy.get("cost_bps")).items():
        cost_drag = _float(row.get("turnover")) * _float(bps) / 10000.0
        rows.append(_cost_sensitivity_row(row, scenario, cost_drag, latency_drag=0.0))
    for scenario, penalty in _mapping(policy.get("execution_lag_penalty")).items():
        rows.append(_cost_sensitivity_row(row, scenario, 0.0, latency_drag=_float(penalty)))
    for scenario, penalty in _mapping(policy.get("rebalance_penalty")).items():
        rows.append(_cost_sensitivity_row(row, scenario, _float(penalty), latency_drag=0.0))
    return rows


def _cost_sensitivity_row(
    row: Mapping[str, Any],
    scenario: str,
    cost_drag: float,
    *,
    latency_drag: float,
) -> dict[str, Any]:
    gross = _float(row.get("annual_return"))
    net = gross - cost_drag - latency_drag
    return {
        "strategy_id": row.get("strategy_id"),
        "scenario": scenario,
        "gross_return": row.get("annual_return"),
        "net_return_after_cost": _round(net),
        "cost_drag": _round(cost_drag),
        "latency_drag": _round(latency_drag),
        "turnover": row.get("turnover"),
        "switch_count": row.get("switch_count"),
        "avg_holding_period": _round(
            252.0 / max(_float(row.get("switch_count")), 1.0)
        ),
        "performance_degradation": _round(gross - net),
    }


def _frontier_role(row: Mapping[str, Any]) -> dict[str, Any]:
    tier = row.get("candidate_tier")
    if tier == "COMPONENT_READY_GROWTH":
        role = "GROWTH_CHALLENGER"
    elif tier == "GROWTH_CHALLENGER":
        role = "GROWTH_CHALLENGER"
    elif tier == "GROWTH_TILT_CANDIDATE" and _float(row.get("return_edge_vs_equal_risk")) > 0.01:
        role = "GROWTH_TILT_CANDIDATE"
    elif tier == "GROWTH_TILT_CANDIDATE":
        role = "BALANCED_CORE"
    elif row.get("strategy_id") == DEFENSIVE_PRIMARY_ID:
        role = "DEFENSIVE_CORE"
    else:
        role = "REJECTED"
    return {**dict(row), "recommended_role": role}


def _best_gate_candidate(ranking: Mapping[str, Any]) -> dict[str, Any]:
    for key in (
        "tier_3_component_ready_candidates",
        "tier_2_growth_challengers",
        "tier_1_growth_tilt_candidates",
    ):
        rows = _records(ranking.get(key))
        if rows:
            return rows[0]
    return {}


def _forward_aging_blockers(
    candidate: Mapping[str, Any],
    definitions: Mapping[str, Any],
    cost: Mapping[str, Any],
    replay: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not candidate:
        return ["no_growth_tilt_candidate"]
    if candidate.get("candidate_tier") not in {
        "GROWTH_TILT_CANDIDATE",
        "GROWTH_CHALLENGER",
        "COMPONENT_READY_GROWTH",
    }:
        blockers.append("candidate_below_tier_1")
    if _float(candidate.get("return_edge_vs_equal_risk")) <= 0.0:
        blockers.append("not_better_than_equal_risk")
    if _float(candidate.get("drawdown_increase_vs_equal_risk")) > _candidate_limit(
        config, "max_drawdown_increase_vs_equal_risk"
    ):
        blockers.append("drawdown_increase_vs_equal_risk_too_high")
    if str(cost.get("status")) in {"GROWTH_TILT_COST_SENSITIVE", "GROWTH_TILT_TURNOVER_TOO_HIGH"}:
        blockers.append(f"cost_status:{cost.get('status')}")
    if str(replay.get("status")) in {
        "GROWTH_TILT_REGIME_CONCENTRATED",
        "GROWTH_TILT_DRAWDOWN_RISK_TOO_HIGH",
    }:
        blockers.append(f"replay_status:{replay.get('status')}")
    locked_ids = {
        str(row.get("strategy_id")) for row in _records(definitions.get("locked_definitions"))
    }
    if str(candidate.get("strategy_id")) not in locked_ids:
        blockers.append("definition_hash_not_locked")
    return _dedupe_text(blockers)


def _forward_aging_warnings(
    candidate: Mapping[str, Any],
    ranking: Mapping[str, Any],
    replay: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if candidate and candidate.get("candidate_tier") == "GROWTH_TILT_CANDIDATE":
        warnings.append("tier_1_candidate_not_qqq_replacement")
    if ranking.get("status") == "NO_GROWTH_TILT_EDGE":
        warnings.append("ranking_found_no_growth_tilt_edge")
    if replay.get("status") == "GROWTH_TILT_PERIOD_REPLAY_READY":
        warnings.append("owner_manual_review_still_required")
    return warnings


def _owner_recommendation(
    candidate: Mapping[str, Any],
    gate: Mapping[str, Any],
    blocked: list[str],
) -> str:
    if blocked:
        return "BLOCKED"
    if not candidate:
        return "NO_USEFUL_GROWTH_TILT"
    if gate.get("status") == "GROWTH_TILT_FORWARD_AGING_REVIEWABLE":
        return "OWNER_REVIEW_GROWTH_TILT_FORWARD_AGING_CANDIDATE"
    if candidate.get("candidate_tier") in {"GROWTH_TILT_CANDIDATE", "GROWTH_CHALLENGER"}:
        return "KEEP_GROWTH_TILT_RESEARCH_ONLY"
    return "NEED_MORE_HISTORY"


def _source_blockers(sources: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [
        name
        for name, source in sources.items()
        if _blocked_status(str(source.get("status")))
    ]


def _master_next_task(
    tier1: list[Mapping[str, Any]],
    tier2: list[Mapping[str, Any]],
    tier3: list[Mapping[str, Any]],
) -> str:
    if tier3:
        return "owner_manual_component_ready_review_without_paper_shadow_or_production"
    if tier2:
        return "review_growth_challenger_forward_aging_readiness"
    if tier1:
        return "continue_balanced_core_growth_tilt_validation"
    return "keep_equal_risk_forward_aging_and_continue_structured_growth_exploration"


def _roadmap_next_tasks(growth_track: str) -> list[str]:
    if growth_track == "continue_growth_tilt_research":
        return [
            "owner_review_growth_tilt_candidate_tier",
            "keep_equal_risk_defensive_primary_forward_aging",
            "do_not_enable_paper_shadow_production_or_broker",
        ]
    if growth_track == "blocked":
        return ["resolve_growth_tilt_blocker_before_interpretation"]
    return ["keep_only_equal_risk_forward_aging", "pause_growth_tilt_until_owner_review"]


def _periods_with_largest_drawdowns(
    config: Mapping[str, Any],
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    periods = [dict(row) for row in _records(_research_policy(config).get("periods"))]
    for ticker, period_id in (
        ("QQQ", "largest_qqq_drawdown"),
        ("QQQ", "largest_growth_tilt_drawdown"),
    ):
        returns = prices[ticker].pct_change().fillna(0.0)
        equity = (1.0 + returns).cumprod()
        drawdown = equity / equity.cummax() - 1.0
        trough = drawdown.idxmin()
        peak = equity.loc[:trough].idxmax()
        periods.append(
            {
                "period_id": period_id,
                "start": pd.Timestamp(peak).date().isoformat(),
                "end": pd.Timestamp(trough).date().isoformat(),
            }
        )
    return periods


def _period_slice(returns: pd.Series, period: Mapping[str, Any]) -> pd.Series:
    if returns.empty:
        return returns
    start = date.fromisoformat(str(period["start"]))
    raw_end = str(period.get("end"))
    end = returns.index.max().date() if raw_end == "latest" else date.fromisoformat(raw_end)
    return returns[(returns.index.date >= start) & (returns.index.date <= end)].fillna(0.0)


def _series_metrics(returns: pd.Series, annualization: int) -> dict[str, Any]:
    if returns.empty:
        return {"annual_return": 0.0, "max_drawdown": 0.0, "recovery_days": 0}
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    annual_return = (float(equity.iloc[-1]) ** (annualization / max(len(returns), 1))) - 1.0
    return {
        "annual_return": _round(annual_return),
        "max_drawdown": _round(float(drawdown.min())),
        "recovery_days": _max_recovery_days(equity),
    }


def _max_recovery_days(equity: pd.Series) -> int:
    high = equity.cummax()
    below = equity < high
    current = 0
    longest = 0
    for value in below:
        if bool(value):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return int(longest)


def _period_commentary(
    metrics: Mapping[str, Any],
    equal_metrics: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
) -> str:
    if _float(metrics.get("annual_return")) > _float(qqq_metrics.get("annual_return")):
        return "beats_100_qqq_in_period"
    if _float(metrics.get("annual_return")) > _float(equal_metrics.get("annual_return")):
        return "beats_equal_risk_but_not_100_qqq"
    return "does_not_beat_equal_risk_in_period"


def _risk_on_mask(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.Series:
    policy = _research_mapping(config, "trend_filter_rule")
    close = prices["QQQ"].shift(1)
    ma100 = prices["QQQ"].rolling(_ma_short(config), min_periods=20).mean().shift(1)
    ma200 = prices["QQQ"].rolling(_ma_long(config), min_periods=20).mean().shift(1)
    vol = _realized_vol(
        prices["QQQ"],
        _int(policy.get("realized_vol_window"), 20),
        _annualization(config),
    )
    vol_threshold = vol.rolling(
        _int(policy.get("realized_vol_percentile_window"), 252)
    ).quantile(_float(policy.get("realized_vol_percentile_cutoff"), 0.80))
    high = prices["QQQ"].rolling(_rolling_high_window(config), min_periods=20).max().shift(1)
    drawdown = close / high - 1.0
    return (
        (close > ma200)
        & (close > ma100)
        & (ma100 > ma200)
        & (vol <= vol_threshold)
        & (drawdown >= _float(policy.get("drawdown_from_high_min"), -0.08))
    ).fillna(False)


def _ramp_factor(signal: pd.Series, ramp_days: int) -> pd.Series:
    values = []
    current = 0
    for active in signal.fillna(False):
        current = current + 1 if bool(active) else 0
        values.append(min(current / max(ramp_days, 1), 1.0))
    return pd.Series(values, index=signal.index)


def _apply_rebalance(
    weights: pd.DataFrame,
    rebalance_rule: str,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    if rebalance_rule == "threshold":
        band = _float(_research_policy(config).get("threshold_rebalance_band"), 0.05)
        return _apply_threshold_rebalance(weights, band)
    period = weights.index.to_period("M").to_series(index=weights.index)
    markers = period != period.shift(1)
    rebalanced = weights.copy()
    rebalanced.loc[~markers] = math.nan
    rebalanced = rebalanced.ffill()
    if not rebalanced.empty:
        rebalanced.iloc[0] = weights.iloc[0]
    return rebalanced.ffill().fillna(0.0)


def _apply_threshold_rebalance(weights: pd.DataFrame, band: float) -> pd.DataFrame:
    if weights.empty:
        return weights
    rows = []
    current = weights.iloc[0].fillna(0.0)
    for _idx, target in weights.fillna(0.0).iterrows():
        if (target - current).abs().max() >= band:
            current = target
        rows.append(current)
    return pd.DataFrame(rows, index=weights.index).ffill().fillna(0.0)


def _realized_vol(series: pd.Series, window: int, annualization: int) -> pd.Series:
    return (
        series.pct_change()
        .rolling(window, min_periods=max(5, min(window, 20)))
        .std(ddof=0)
        .fillna(0.0)
        * math.sqrt(annualization)
    )


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    aligned.columns = ["strategy", "benchmark"]
    variance = float(aligned["benchmark"].var(ddof=0))
    if abs(variance) <= 1e-12:
        return 0.0
    return float(aligned["strategy"].cov(aligned["benchmark"]) / variance)


def _asset_contribution(
    weights: pd.DataFrame,
    returns: pd.Series,
    ticker: str,
    config: Mapping[str, Any],
) -> float:
    if ticker not in weights:
        return 0.0
    contribution = weights[ticker].shift(1).fillna(0.0) * returns.reindex(weights.index).fillna(0.0)
    return float(contribution.mean() * _annualization(config))


def _leverage_drag(weights: pd.DataFrame, prices: pd.DataFrame, config: Mapping[str, Any]) -> float:
    if "TQQQ" not in weights or "TQQQ" not in prices:
        return 0.0
    qqq_annual = float(prices["QQQ"].pct_change().fillna(0.0).mean() * _annualization(config))
    tqqq_annual = float(prices["TQQQ"].pct_change().fillna(0.0).mean() * _annualization(config))
    return _float(weights["TQQQ"].mean()) * (
        tqqq_annual - qqq_annual * TQQQ_DAILY_LEVERAGE_MULTIPLIER
    )


def _vol_target_error(
    candidate: Mapping[str, Any],
    returns: pd.Series,
    config: Mapping[str, Any],
) -> float:
    if candidate.get("candidate_family") != "vol_target_growth_tilt":
        return 0.0
    realized = float(returns.std(ddof=0) * math.sqrt(_annualization(config)))
    target = _float(candidate.get("target_vol"))
    if candidate.get("target_vol_mode") == "equal_risk_plus":
        target = realized
    return _round(realized - target)


def _selected_ranked_candidates(ranking: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in (
        "tier_3_component_ready_candidates",
        "tier_2_growth_challengers",
        "tier_1_growth_tilt_candidates",
        "top_by_return_edge_vs_equal_risk",
    ):
        rows.extend(_records(ranking.get(key)))
    return _dedupe_candidates(rows)[:5]


def _dedupe_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        strategy_id = str(row.get("strategy_id") or row.get("candidate_id") or "")
        if not strategy_id or strategy_id in seen:
            continue
        result.append(row)
        seen.add(strategy_id)
    return result


def _top(
    rows: list[Mapping[str, Any]],
    key: str,
    *,
    reverse: bool = True,
    limit: int = 10,
) -> list[dict[str, Any]]:
    sorted_rows = sorted(
        rows,
        key=lambda row: _float(row.get(key)),
        reverse=reverse,
    )
    return [dict(row) for row in sorted_rows[:limit]]


def _first_role(rows: list[Mapping[str, Any]], role: str) -> dict[str, Any]:
    return dict(next((row for row in rows if row.get("recommended_role") == role), {}))


def _recommended_by_role(rows: list[Mapping[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        role = str(row.get("recommended_role") or "")
        if role and role not in result:
            result[role] = str(row.get("strategy_id"))
    return result


def _overfit_like(payload: Mapping[str, Any]) -> bool:
    rows = _records(payload.get("candidate_results"))
    return bool(rows) and all(_float(row.get("risk_on_days")) < 30 for row in rows)


def _prohibited_reader_brief_hits(payload: Mapping[str, Any]) -> list[str]:
    prohibited = (
        "买入",
        "卖出",
        "应调仓",
        "实盘仓位",
        "真实交易建议",
        "production ready",
        "paper-shadow active",
    )
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return [phrase for phrase in prohibited if phrase in raw]


def _write_owner_doc(payload: Mapping[str, Any], path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        f"# {title}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- owner_recommendation：`{payload.get('owner_recommendation', 'N/A')}`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- broker_action：`none`",
        "- manual_review_required：`true`",
        "",
        "## Required Answers",
        "",
        "|Question|Answer|",
        "|---|---|",
    ]
    for key, value in answers.items():
        lines.append(f"|`{key}`|`{value}`|")
    lines.extend(
        [
            "",
            "本报告仅用于 research-only owner review，不生成交易建议、paper-shadow "
            "activation、production config mutation 或 broker action。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
        **AI_REGIME_SUMMARY,
        "summary": {
            **AI_REGIME_SUMMARY,
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
            "TRADING-1065 to 1084 equal-risk growth tilt artifacts are regenerated "
            "after registry, data quality, search, validation, owner review or "
            "roadmap state changes."
        ),
        "owner_action": "review_equal_risk_growth_tilt_research_only_artifact",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _load_config(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"equal-risk growth tilt registry must be a mapping: {path}")
    return dict(raw)


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


def _required_tickers(config: Mapping[str, Any]) -> list[str]:
    return [str(item) for item in _research_policy(config).get("required_price_tickers", [])]


def _research_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(config.get("research_policy"))


def _research_mapping(config: Mapping[str, Any], key: str) -> dict[str, Any]:
    return _mapping(_research_policy(config).get(key))


def _search_grid(config: Mapping[str, Any], key: str) -> dict[str, Any]:
    return _mapping(_research_mapping(config, "search_grids").get(key))


def _candidate_limit(config: Mapping[str, Any], key: str) -> float:
    return _float(_research_mapping(config, "candidate_limits").get(key))


def _annualization(config: Mapping[str, Any]) -> int:
    return _int(_research_policy(config).get("annualization_trading_days"), 252)


def _ma_short(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "moving_average_windows").get("short"), 100)


def _ma_long(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "moving_average_windows").get("long"), 200)


def _rolling_high_window(config: Mapping[str, Any]) -> int:
    return _int(_research_mapping(config, "rolling_high_windows").get("long"), 252)


def _policy_definition(candidate: Mapping[str, Any]) -> dict[str, Any]:
    ignored = {"risk_on_days"}
    return {str(key): value for key, value in candidate.items() if key not in ignored}


def _normalise_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    parsed = {str(key): _float(value) for key, value in weights.items()}
    total = sum(parsed.values())
    if total <= 0:
        return {}
    return {key: value / total for key, value in parsed.items()}


def _tier_requirement_text(tier: str) -> list[str]:
    if tier == "tier_1":
        return [
            "annual_return > equal_risk_qqq_sgov",
            "max_drawdown not materially worse than equal risk",
            "Sharpe and Calmar do not collapse",
            "turnover controlled",
            "data quality not blocked",
        ]
    if tier == "tier_2":
        return [
            "annual_return close to or above 100_qqq",
            "Calmar not materially below 100_qqq",
            "max_drawdown not materially worse than 100_qqq",
            "turnover and switch count controlled",
            "period split not highly concentrated",
        ]
    return [
        "beta-adjusted edge remains",
        "not just effective_beta > 1",
        "not only AI rally effective",
        "drawdown episodes acceptable",
        "cost / turnover / latency edge survives",
        "definition_hash locked",
        "owner manual review required",
    ]


def _first_data_quality_status(sources: Any) -> str | None:
    for source in sources:
        summary = _mapping(_mapping(source).get("summary"))
        value = summary.get("data_quality_status")
        if value:
            return str(value)
    return None


def _artifact_paths_by_report(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {report_id: source.get("artifact_paths", {}) for report_id, source in sources.items()}


def _requested_range(rows: list[dict[str, Any]], start_date: date, end_date: date | None) -> str:
    if rows:
        return str(rows[0].get("requested_date_range"))
    end = "open" if end_date is None else end_date.isoformat()
    return f"{start_date.isoformat()}..{end}"


def _safe_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


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


def _round(value: object) -> float:
    return round(_float(value), 6)


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if abs(denominator) > 1e-12 else 0.0


def _pct_token(value: object) -> str:
    return str(int(round(_float(value) * 1000))).rstrip("0") or "0"


def _stable_hash(value: object) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _dedupe_text(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _blocked_status(status: str) -> bool:
    return "BLOCKED" in status or status == "FAIL"


def _safety_summary() -> dict[str, Any]:
    return {
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }
