from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
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
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
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
DEFAULT_GROWTH_TILT_OWNER_DECISION_REAL_RUN_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_owner_decision_pack_real_run.md"
)
DEFAULT_GROWTH_TILT_REAL_RESULT_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_real_result_master_review.md"
)
DEFAULT_GROWTH_TILT_OWNER_DIAGNOSIS_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_owner_diagnosis_pack.md"
)
DEFAULT_GROWTH_TILT_FOCUSED_DIAGNOSIS_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_focused_diagnosis_master_review.md"
)
DEFAULT_BALANCED_CORE_OWNER_LAUNCH_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "balanced_core_owner_launch_pack.md"
)
DEFAULT_DUAL_FORWARD_AGING_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dual_forward_aging_master_review.md"
)
DEFAULT_BALANCED_CORE_LAUNCH_OWNER_REPORT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "balanced_core_launch_owner_report.md"
)
DEFAULT_EXTERNAL_VALIDATION_BALANCED_CORE_LAUNCH_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "external_validation_balanced_core_launch_master_review.md"
)
DEFAULT_DUAL_FORWARD_AGING_MONTHLY_MONITOR_CONTRACT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dual_forward_aging_monthly_monitor_contract.md"
)
DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)

DEFENSIVE_PRIMARY_ID = "equal_risk_qqq_sgov"
PRIMARY_QQQ_BENCHMARK_ID = "100_qqq"
TQQQ_DAILY_LEVERAGE_MULTIPLIER = 3.0
FOCUSED_GROWTH_TILT_CANDIDATE_ID = (
    "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
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
        "8_forward_aging_watchlist_allowed_now": gate.get(
            "forward_aging_watchlist_allowed"
        ),
        "9_owner_review_forward_aging_watchlist_candidate": gate.get(
            "forward_aging_reviewable_after_owner_manual_review"
        ),
        "10_keep_original_equal_risk_defensive_primary": True,
        "11_continue_no_paper_shadow_no_production_no_broker": True,
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


def run_growth_tilt_real_cli_suite(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    roadmap_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    owner_docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    master_docs_path: Path = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    source_runs = _growth_tilt_real_cli_source_runs(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        roadmap_output_root=roadmap_output_root,
        owner_docs_path=owner_docs_path,
        master_docs_path=master_docs_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    rows: list[dict[str, Any]] = []
    source_payloads: dict[str, dict[str, Any]] = {}
    for report_id, command, builder in source_runs:
        source = builder()
        source_payloads[report_id] = source
        rows.append(_growth_tilt_source_run_row(report_id, command, source))
    blocked = [row["report_id"] for row in rows if _blocked_status(str(row["status"]))]
    warnings = [
        row["report_id"]
        for row in rows
        if row["report_id"] not in blocked
        and (row["warnings"] or row["blockers"] or _warning_status(str(row["status"])))
    ]
    if blocked:
        status = "GROWTH_TILT_REAL_RUN_BLOCKED"
    elif warnings:
        status = "GROWTH_TILT_REAL_RUN_WARN"
    else:
        status = "GROWTH_TILT_REAL_RUN_PASS"
    payload = _payload(
        report_type="growth_tilt_real_cli_suite_summary",
        title="Growth Tilt Real CLI Suite Summary",
        status=status,
        summary={
            "source_command_count": len(rows),
            "blocked_source_count": len(blocked),
            "warning_source_count": len(warnings),
            "top_candidate": _first_present(row.get("top_candidate") for row in rows),
            "highest_tier": _highest_tier_from_values(row.get("highest_tier") for row in rows),
            **_safety_summary(),
        },
        required_command_count=len(source_runs),
        real_run_results=rows,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        warnings=warnings,
        blockers=blocked,
        report_registry_entry=_report_registry_entry(
            "growth_tilt_real_cli_suite_summary",
            "Growth Tilt Real CLI Suite Summary",
            "aits research strategies growth-tilt-real-cli-suite",
            "growth_tilt_real_cli_suite_summary",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_candidate_result_summary(
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
    rows = [_growth_tilt_candidate_summary_row(row) for row in _all_ranked_candidates(ranking)]
    candidates = [row for row in rows if row["candidate_tier"] != "REJECTED"]
    rejected = [row for row in rows if row["candidate_tier"] == "REJECTED"]
    if _blocked_status(str(ranking.get("status"))):
        status = "GROWTH_TILT_RESULTS_BLOCKED"
    elif candidates:
        status = "GROWTH_TILT_CANDIDATES_FOUND"
    elif rows:
        status = "NO_GROWTH_TILT_CANDIDATE"
    else:
        status = "GROWTH_TILT_RESULTS_INCONCLUSIVE"
    payload = _payload(
        report_type="growth_tilt_candidate_result_summary",
        title="Growth Tilt Candidate Result Summary",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "rejected_count": len(rejected),
            "top_candidate": candidates[0]["strategy_id"] if candidates else None,
            "top_candidate_family": (
                candidates[0]["candidate_family"] if candidates else None
            ),
            "data_quality_status": _payload_data_quality_status(ranking),
            **_safety_summary(),
        },
        top_by_return_edge_vs_equal_risk=_top(rows, "annual_return_edge_vs_equal_risk"),
        top_by_return_gap_reduction_vs_100_qqq=_top(
            rows, "return_gap_reduction_vs_100_qqq"
        ),
        top_by_calmar=_top(rows, "calmar"),
        top_by_sharpe=_top(rows, "sharpe"),
        top_by_low_drawdown=_top(rows, "max_drawdown"),
        top_by_low_turnover=_top(rows, "turnover", reverse=False),
        candidate_by_family=_group_candidates_by_family(candidates),
        rejected_by_family=_group_candidates_by_family(rejected),
        candidate_results=rows,
        source_statuses={"equal_risk_growth_tilt_ranking_tiering": ranking.get("status")},
        source_artifacts={
            "equal_risk_growth_tilt_ranking_tiering": ranking.get("artifact_paths", {})
        },
        blockers=_text_list(ranking.get("blockers")),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_candidate_result_summary",
            "Growth Tilt Candidate Result Summary",
            "aits research strategies growth-tilt-candidate-result-summary",
            "growth_tilt_candidate_result_summary",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_tier_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    rows = [
        _growth_tilt_tier_validation_row(row, config)
        for row in _records(summary.get("candidate_results"))
    ]
    highest_tier = _highest_tier_from_values(row.get("tier") for row in rows)
    if _blocked_status(str(summary.get("status"))):
        status = "GROWTH_TILT_TIER_BLOCKED"
    elif highest_tier:
        status = "GROWTH_TILT_TIER_VALIDATED"
    elif rows:
        status = "NO_TIER_1_CANDIDATE"
    else:
        status = "GROWTH_TILT_TIER_INCONCLUSIVE"
    payload = _payload(
        report_type="growth_tilt_tier_validation",
        title="Growth Tilt Tier Validation",
        status=status,
        summary={
            "validated_candidate_count": len(rows),
            "highest_tier": highest_tier,
            "tier_1_count": sum(1 for row in rows if row["tier"] == "GROWTH_TILT_CANDIDATE"),
            "tier_2_count": sum(1 for row in rows if row["tier"] == "GROWTH_CHALLENGER"),
            "tier_3_count": sum(1 for row in rows if row["tier"] == "COMPONENT_READY_GROWTH"),
            **_safety_summary(),
        },
        tier_validation_rows=rows,
        highest_tier=highest_tier,
        source_statuses={"growth_tilt_candidate_result_summary": summary.get("status")},
        source_artifacts={
            "growth_tilt_candidate_result_summary": summary.get("artifact_paths", {})
        },
        report_registry_entry=_report_registry_entry(
            "growth_tilt_tier_validation",
            "Growth Tilt Tier Validation",
            "aits research strategies growth-tilt-tier-validation",
            "growth_tilt_tier_validation",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_beta_adjusted_edge_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    cost = run_growth_tilt_cost_turnover_sensitivity(
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
    candidate = _best_summary_candidate(summary)
    cost_penalty = _candidate_cost_penalty(candidate, cost)
    drawdown_penalty = _float(candidate.get("drawdown_penalty"))
    tqqq_penalty = _float(candidate.get("path_dependency_risk"))
    complexity_penalty = 0.0
    beta_edge = _float(candidate.get("beta_adjusted_return_edge"))
    net_edge = beta_edge - drawdown_penalty - cost_penalty - tqqq_penalty - complexity_penalty
    blockers = []
    if _blocked_status(str(summary.get("status"))):
        blockers.append("candidate_summary_blocked")
    if not candidate:
        blockers.append("no_candidate_for_beta_adjusted_review")
    if blockers:
        status = "BETA_ADJUSTED_EDGE_BLOCKED"
    elif _float(candidate.get("annual_return_edge_vs_equal_risk")) > 0.0 and beta_edge <= 0.0:
        status = "BETA_EXPLAINS_EDGE"
    elif (
        net_edge > _candidate_limit(config, "beta_adjusted_edge_minimum")
        and _float(candidate.get("beta_adjusted_sharpe_edge")) > 0.0
        and _float(candidate.get("beta_adjusted_calmar_edge")) > 0.0
    ):
        status = "BETA_ADJUSTED_EDGE_MATERIAL"
    elif beta_edge > 0.0:
        status = "BETA_ADJUSTED_EDGE_PRESENT"
    else:
        status = "EDGE_WEAK_AFTER_PENALTY"
    edge_row = {
        "strategy_id": candidate.get("strategy_id"),
        "raw_return_edge_vs_equal_risk": candidate.get("annual_return_edge_vs_equal_risk"),
        "raw_return_edge_vs_100_qqq": candidate.get("return_edge_vs_100_qqq"),
        "effective_qqq_beta": candidate.get("effective_qqq_beta"),
        "effective_leverage": candidate.get("effective_leverage"),
        "average_tqqq_weight": candidate.get("average_tqqq_weight"),
        "beta_adjusted_return_edge": candidate.get("beta_adjusted_return_edge"),
        "beta_adjusted_sharpe_edge": candidate.get("beta_adjusted_sharpe_edge"),
        "beta_adjusted_calmar_edge": candidate.get("beta_adjusted_calmar_edge"),
        "drawdown_penalty": _round(drawdown_penalty),
        "turnover_penalty": _round(cost_penalty),
        "tqqq_path_dependency_penalty": _round(tqqq_penalty),
        "complexity_penalty": _round(complexity_penalty),
        "net_edge_after_penalty": _round(net_edge),
        "edge_explanation": _growth_tilt_edge_explanation(candidate, status),
    }
    payload = _payload(
        report_type="growth_tilt_beta_adjusted_edge_review",
        title="Growth Tilt Beta-Adjusted Edge Review",
        status=status,
        summary={
            "strategy_id": candidate.get("strategy_id"),
            "net_edge_after_penalty": _round(net_edge),
            "beta_adjusted_edge_minimum": _candidate_limit(
                config, "beta_adjusted_edge_minimum"
            ),
            **_safety_summary(),
        },
        **edge_row,
        benchmark_comparisons=_growth_tilt_benchmark_comparisons(
            candidate_strategy_id=str(candidate.get("strategy_id") or ""),
            prices_path=prices_path,
            config_path=config_path,
            start_date=start_date,
            end_date=end_date,
        ),
        source_statuses={
            "growth_tilt_candidate_result_summary": summary.get("status"),
            "growth_tilt_cost_turnover_sensitivity": cost.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "growth_tilt_candidate_result_summary": summary,
                "growth_tilt_cost_turnover_sensitivity": cost,
            }
        ),
        blockers=blockers,
        report_registry_entry=_report_registry_entry(
            "growth_tilt_beta_adjusted_edge_review",
            "Growth Tilt Beta-Adjusted Edge Review",
            "aits research strategies growth-tilt-beta-adjusted-edge-review",
            "growth_tilt_beta_adjusted_edge_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_risk_return_frontier_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    growth_rows = [
        _frontier_role(row)
        for row in _records(summary.get("candidate_results"))
        if row.get("candidate_tier") != "REJECTED"
    ]
    benchmark_rows = _growth_tilt_benchmark_frontier_rows(
        prices_path=prices_path,
        config_path=config_path,
        start_date=start_date,
        end_date=end_date,
    )
    frontier_rows = benchmark_rows + growth_rows
    dominated = _dominated_frontier_rows(frontier_rows)
    non_dominated = [
        row for row in frontier_rows if row.get("strategy_id") not in dominated
    ]
    useful_growth = [
        row
        for row in non_dominated
        if row.get("recommended_role") in {"GROWTH_TILT_CANDIDATE", "GROWTH_CHALLENGER"}
    ]
    if _blocked_status(str(summary.get("status"))):
        status = "GROWTH_TILT_FRONTIER_BLOCKED"
    elif useful_growth:
        status = "GROWTH_TILT_FRONTIER_READY"
    elif any(row.get("recommended_role") == "BALANCED_CORE" for row in non_dominated):
        status = "BALANCED_CORE_FRONTIER_FOUND"
    else:
        status = "NO_USEFUL_GROWTH_TILT_FRONTIER"
    payload = _payload(
        report_type="growth_tilt_risk_return_frontier_review",
        title="Growth Tilt Risk-Return Frontier Review",
        status=status,
        summary={
            "frontier_candidate_count": len(frontier_rows),
            "non_dominated_candidate_count": len(non_dominated),
            "dominated_candidate_count": len(dominated),
            **_safety_summary(),
        },
        return_vs_drawdown_frontier=_top(frontier_rows, "annual_return"),
        return_vs_calmar_frontier=_top(frontier_rows, "calmar"),
        return_vs_sharpe_frontier=_top(frontier_rows, "sharpe"),
        return_vs_turnover_frontier=_top(frontier_rows, "turnover", reverse=False),
        defensive_core_candidate=_first_role(frontier_rows, "DEFENSIVE_CORE"),
        balanced_core_candidate=_first_role(frontier_rows, "BALANCED_CORE"),
        growth_tilt_candidate=_first_role(frontier_rows, "GROWTH_TILT_CANDIDATE"),
        growth_challenger_candidate=_first_role(frontier_rows, "GROWTH_CHALLENGER"),
        dominated_candidate_list=sorted(dominated),
        non_dominated_candidate_list=non_dominated,
        frontier_candidates=frontier_rows,
        source_statuses={"growth_tilt_candidate_result_summary": summary.get("status")},
        source_artifacts={
            "growth_tilt_candidate_result_summary": summary.get("artifact_paths", {})
        },
        report_registry_entry=_report_registry_entry(
            "growth_tilt_risk_return_frontier_review",
            "Growth Tilt Risk-Return Frontier Review",
            "aits research strategies growth-tilt-risk-return-frontier-review",
            "growth_tilt_risk_return_frontier_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_period_drawdown_cost_triage(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    replay = run_growth_tilt_period_drawdown_replay(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
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
    )
    selected = _selected_summary_candidates(summary)
    rows = [_growth_tilt_triage_row(row, replay=replay, cost=cost) for row in selected]
    blockers = [
        key
        for key, source in {
            "growth_tilt_candidate_result_summary": summary,
            "growth_tilt_period_drawdown_replay": replay,
            "growth_tilt_cost_turnover_sensitivity": cost,
        }.items()
        if _blocked_status(str(source.get("status")))
    ]
    if blockers:
        status = "GROWTH_TILT_TRIAGE_BLOCKED"
    elif str(cost.get("status")) in {"GROWTH_TILT_COST_BLOCKED", "GROWTH_TILT_TURNOVER_TOO_HIGH"}:
        status = "GROWTH_TILT_COST_BLOCKED"
    elif str(replay.get("status")) == "GROWTH_TILT_DRAWDOWN_RISK_TOO_HIGH":
        status = "GROWTH_TILT_DRAWDOWN_RISK_TOO_HIGH"
    elif str(replay.get("status")) == "GROWTH_TILT_REGIME_CONCENTRATED" or any(
        row.get("ai_rally_dependency") for row in rows
    ):
        status = "GROWTH_TILT_REGIME_CONCENTRATED"
    elif str(cost.get("status")) == "GROWTH_TILT_COST_SENSITIVE":
        status = "GROWTH_TILT_TRIAGE_WARN"
    else:
        status = "GROWTH_TILT_TRIAGE_PASS"
    payload = _payload(
        report_type="growth_tilt_period_drawdown_cost_triage",
        title="Growth Tilt Period Drawdown Cost Triage",
        status=status,
        summary={
            "candidate_count": len(rows),
            "period_status": replay.get("status"),
            "cost_sensitivity_status": cost.get("status"),
            "data_quality_status": _first_data_quality_status([summary, replay, cost]),
            **_safety_summary(),
        },
        triage_rows=rows,
        required_coverage=[
            "2022_rate_hike_bear_market",
            "2023_recovery",
            "2024_ai_rally",
            "2025_to_latest",
            "largest QQQ drawdown in available data",
            "largest growth_tilt_drawdown",
            "high-rate SGOV carry period",
        ],
        source_statuses={
            "growth_tilt_candidate_result_summary": summary.get("status"),
            "growth_tilt_period_drawdown_replay": replay.get("status"),
            "growth_tilt_cost_turnover_sensitivity": cost.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "growth_tilt_candidate_result_summary": summary,
                "growth_tilt_period_drawdown_replay": replay,
                "growth_tilt_cost_turnover_sensitivity": cost,
            }
        ),
        blockers=blockers,
        report_registry_entry=_report_registry_entry(
            "growth_tilt_period_drawdown_cost_triage",
            "Growth Tilt Period Drawdown Cost Triage",
            "aits research strategies growth-tilt-period-drawdown-cost-triage",
            "growth_tilt_period_drawdown_cost_triage",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_vs_equal_risk_and_qqq_final_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
    _tier_payload: Mapping[str, Any] | None = None,
    _beta_payload: Mapping[str, Any] | None = None,
    _triage_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    tier = dict(
        _tier_payload
        or run_growth_tilt_tier_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    beta = dict(
        _beta_payload
        or run_growth_tilt_beta_adjusted_edge_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    triage = dict(
        _triage_payload
        or run_growth_tilt_period_drawdown_cost_triage(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    candidate = _best_summary_candidate(summary)
    blocking_reasons = _growth_tilt_final_gate_blockers(candidate, beta, triage)
    warning_reasons = _growth_tilt_final_gate_warnings(candidate, beta, triage)
    source_blocked = [
        key
        for key, source in {
            "growth_tilt_candidate_result_summary": summary,
            "growth_tilt_tier_validation": tier,
            "growth_tilt_beta_adjusted_edge_review": beta,
            "growth_tilt_period_drawdown_cost_triage": triage,
        }.items()
        if _growth_tilt_source_artifact_blocked_status(str(source.get("status")))
    ]
    tier_name = str(candidate.get("candidate_tier") or "")
    if source_blocked:
        status = "GROWTH_TILT_FINAL_GATE_BLOCKED"
        highest_gate = None
    elif not candidate:
        status = "NO_USEFUL_GROWTH_TILT"
        highest_gate = None
    elif (
        tier_name == "COMPONENT_READY_GROWTH"
        and beta.get("status") == "BETA_ADJUSTED_EDGE_MATERIAL"
        and not blocking_reasons
    ):
        status = "GROWTH_TILT_COMPONENT_REVIEWABLE"
        highest_gate = "Tier 3 Gate"
    elif tier_name in {"GROWTH_CHALLENGER", "COMPONENT_READY_GROWTH"} and not blocking_reasons:
        status = "GROWTH_TILT_TIER2_REVIEWABLE"
        highest_gate = "Tier 2 Gate"
    elif tier_name in {
        "GROWTH_TILT_CANDIDATE",
        "GROWTH_CHALLENGER",
        "COMPONENT_READY_GROWTH",
    } and not blocking_reasons:
        status = "GROWTH_TILT_TIER1_REVIEWABLE"
        highest_gate = "Tier 1 Gate"
    else:
        status = "GROWTH_TILT_RESEARCH_ONLY" if candidate else "NO_USEFUL_GROWTH_TILT"
        highest_gate = None
    forward_review_allowed = status in {
        "GROWTH_TILT_TIER1_REVIEWABLE",
        "GROWTH_TILT_TIER2_REVIEWABLE",
        "GROWTH_TILT_COMPONENT_REVIEWABLE",
    }
    payload = _payload(
        report_type="growth_tilt_final_gate",
        title="Growth Tilt Vs Equal-Risk And QQQ Final Gate",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "highest_gate_passed": highest_gate,
            "blocking_reason_count": len(source_blocked) + len(blocking_reasons),
            "warning_reason_count": len(warning_reasons),
            "forward_aging_review_allowed": forward_review_allowed,
            **_safety_summary(),
        },
        candidate_strategy_id=candidate.get("strategy_id"),
        highest_gate_passed=highest_gate,
        gate_status=status,
        blocking_reasons=_dedupe_text([*source_blocked, *blocking_reasons]),
        warning_reasons=warning_reasons,
        recommended_role=_growth_tilt_recommended_role(candidate, status),
        forward_aging_review_allowed=forward_review_allowed,
        selected_candidate=candidate,
        benchmark_comparisons=_growth_tilt_benchmark_comparisons(
            candidate_strategy_id=str(candidate.get("strategy_id") or ""),
            prices_path=prices_path,
            config_path=config_path,
            start_date=start_date,
            end_date=end_date,
        ),
        source_statuses={
            "growth_tilt_candidate_result_summary": summary.get("status"),
            "growth_tilt_tier_validation": tier.get("status"),
            "growth_tilt_beta_adjusted_edge_review": beta.get("status"),
            "growth_tilt_period_drawdown_cost_triage": triage.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "growth_tilt_candidate_result_summary": summary,
                "growth_tilt_tier_validation": tier,
                "growth_tilt_beta_adjusted_edge_review": beta,
                "growth_tilt_period_drawdown_cost_triage": triage,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_final_gate",
            "Growth Tilt Final Gate",
            "aits research strategies growth-tilt-vs-equal-risk-and-qqq-final-gate",
            "growth_tilt_final_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_forward_aging_watchlist_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _final_gate_payload: Mapping[str, Any] | None = None,
    _definition_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    final_gate = dict(
        _final_gate_payload
        or run_growth_tilt_vs_equal_risk_and_qqq_final_gate(
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
    definition = dict(
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
        )
    )
    config = _load_config(config_path)
    candidate = _mapping(final_gate.get("selected_candidate"))
    locked_ids = {
        str(row.get("strategy_id")) for row in _records(definition.get("locked_definitions"))
    }
    blockers = list(_text_list(final_gate.get("blocking_reasons")))
    if not candidate:
        blockers.append("no_growth_tilt_candidate")
    if final_gate.get("forward_aging_review_allowed") is not True:
        blockers.append("final_gate_not_forward_aging_reviewable")
    if str(candidate.get("strategy_id")) not in locked_ids:
        blockers.append("definition_hash_not_locked")
    blockers = _dedupe_text(blockers)
    warnings = _dedupe_text(
        [*_text_list(final_gate.get("warning_reasons")), "owner_manual_review_required"]
    )
    reviewable = bool(candidate) and not blockers
    if _blocked_status(str(final_gate.get("status"))) or _blocked_status(
        str(definition.get("status"))
    ):
        status = "GROWTH_TILT_WATCHLIST_BLOCKED"
    elif reviewable:
        status = "GROWTH_TILT_WATCHLIST_REVIEWABLE"
    elif candidate:
        status = "GROWTH_TILT_KEEP_RESEARCH_ONLY"
    else:
        status = "NO_GROWTH_TILT_WATCHLIST_CANDIDATE"
    payload = _payload(
        report_type="growth_tilt_forward_aging_watchlist_review",
        title="Growth Tilt Forward-Aging Watchlist Review",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "candidate_tier": candidate.get("candidate_tier"),
            "watchlist_allowed": reviewable,
            "blocking_reason_count": len(blockers),
            "warning_reason_count": len(warnings),
            **_safety_summary(),
        },
        candidate_strategy_id=candidate.get("strategy_id"),
        candidate_tier=candidate.get("candidate_tier"),
        watchlist_allowed=reviewable,
        watchlist_role="research_only_forward_aging_candidate" if reviewable else "research_only",
        blocking_reasons=blockers,
        warning_reasons=warnings,
        required_forward_days=_candidate_limit(config, "required_forward_days"),
        source_statuses={
            "growth_tilt_final_gate": final_gate.get("status"),
            "growth_tilt_definition_lock_versioning": definition.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "growth_tilt_final_gate": final_gate,
                "growth_tilt_definition_lock_versioning": definition,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_forward_aging_watchlist_review",
            "Growth Tilt Forward-Aging Watchlist Review",
            "aits research strategies growth-tilt-forward-aging-watchlist-review",
            "growth_tilt_forward_aging_watchlist_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_owner_decision_pack_real_run(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_REAL_RUN_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
    _tier_payload: Mapping[str, Any] | None = None,
    _beta_payload: Mapping[str, Any] | None = None,
    _triage_payload: Mapping[str, Any] | None = None,
    _final_gate_payload: Mapping[str, Any] | None = None,
    _watchlist_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    tier = dict(
        _tier_payload
        or run_growth_tilt_tier_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    beta = dict(
        _beta_payload
        or run_growth_tilt_beta_adjusted_edge_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    triage = dict(
        _triage_payload
        or run_growth_tilt_period_drawdown_cost_triage(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    final_gate = dict(
        _final_gate_payload
        or run_growth_tilt_vs_equal_risk_and_qqq_final_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
            _tier_payload=tier,
            _beta_payload=beta,
            _triage_payload=triage,
        )
    )
    watchlist = dict(
        _watchlist_payload
        or run_growth_tilt_forward_aging_watchlist_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _final_gate_payload=final_gate,
        )
    )
    candidate = _best_summary_candidate(summary)
    answers = {
        "1_tier_1_growth_tilt_candidate_exists": _highest_tier_at_least(
            tier.get("highest_tier"), "GROWTH_TILT_CANDIDATE"
        ),
        "2_tier_2_growth_challenger_exists": _highest_tier_at_least(
            tier.get("highest_tier"), "GROWTH_CHALLENGER"
        ),
        "3_tier_3_component_ready_growth_exists": _highest_tier_at_least(
            tier.get("highest_tier"), "COMPONENT_READY_GROWTH"
        ),
        "4_best_candidate": candidate.get("strategy_id"),
        "5_return_improvement_vs_equal_risk": candidate.get(
            "annual_return_edge_vs_equal_risk"
        ),
        "6_return_gap_vs_100_qqq": candidate.get("annual_return_gap_vs_100_qqq"),
        "7_return_lift_only_higher_beta": beta.get("status") == "BETA_EXPLAINS_EDGE",
        "8_risk_adjusted_metrics_and_turnover_acceptable": final_gate.get("status")
        in {
            "GROWTH_TILT_TIER1_REVIEWABLE",
            "GROWTH_TILT_TIER2_REVIEWABLE",
            "GROWTH_TILT_COMPONENT_REVIEWABLE",
        },
        "9_forward_aging_watchlist_review_allowed": bool(
            watchlist.get("watchlist_allowed")
        ),
        "10_original_equal_risk_remains_defensive_primary": True,
        "11_continue_no_paper_shadow_no_production_no_broker": True,
    }
    source_payloads = {
        "growth_tilt_candidate_result_summary": summary,
        "growth_tilt_tier_validation": tier,
        "growth_tilt_beta_adjusted_edge_review": beta,
        "growth_tilt_period_drawdown_cost_triage": triage,
        "growth_tilt_final_gate": final_gate,
        "growth_tilt_forward_aging_watchlist_review": watchlist,
    }
    recommendation = _growth_tilt_owner_recommendation_real(
        candidate=candidate,
        final_gate=final_gate,
        watchlist=watchlist,
        source_payloads=source_payloads,
    )
    status = (
        "BLOCKED"
        if recommendation == "BLOCKED"
        else "GROWTH_TILT_OWNER_DECISION_PACK_REAL_RUN_READY"
    )
    payload = _payload(
        report_type="growth_tilt_owner_decision_pack_real_run",
        title="Growth Tilt Owner Decision Pack Real Run",
        status=status,
        summary={
            "owner_recommendation": recommendation,
            "candidate_strategy_id": candidate.get("strategy_id"),
            "candidate_tier": candidate.get("candidate_tier"),
            "final_gate_status": final_gate.get("status"),
            "watchlist_status": watchlist.get("status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_owner_decision_pack_real_run",
            "Growth Tilt Owner Decision Pack Real Run",
            "aits research strategies growth-tilt-owner-decision-pack-real-run",
            "growth_tilt_owner_decision_pack_real_run",
            extra_artifact_globs=["docs/research/growth_tilt_owner_decision_pack_real_run.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Tilt Owner Decision Pack Real Run")
    payload["owner_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_real_result_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    roadmap_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_TILT_REAL_RESULT_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_REAL_RUN_DOC_PATH,
    source_owner_docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    source_master_docs_path: Path = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _suite_payload: Mapping[str, Any] | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
    _tier_payload: Mapping[str, Any] | None = None,
    _beta_payload: Mapping[str, Any] | None = None,
    _triage_payload: Mapping[str, Any] | None = None,
    _final_gate_payload: Mapping[str, Any] | None = None,
    _watchlist_payload: Mapping[str, Any] | None = None,
    _owner_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    suite = dict(
        _suite_payload
        or run_growth_tilt_real_cli_suite(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            roadmap_output_root=roadmap_output_root,
            owner_docs_path=source_owner_docs_path,
            master_docs_path=source_master_docs_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    tier = dict(
        _tier_payload
        or run_growth_tilt_tier_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    beta = dict(
        _beta_payload
        or run_growth_tilt_beta_adjusted_edge_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    triage = dict(
        _triage_payload
        or run_growth_tilt_period_drawdown_cost_triage(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    final_gate = dict(
        _final_gate_payload
        or run_growth_tilt_vs_equal_risk_and_qqq_final_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
            _tier_payload=tier,
            _beta_payload=beta,
            _triage_payload=triage,
        )
    )
    watchlist = dict(
        _watchlist_payload
        or run_growth_tilt_forward_aging_watchlist_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _final_gate_payload=final_gate,
        )
    )
    owner = dict(
        _owner_payload
        or run_growth_tilt_owner_decision_pack_real_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            docs_path=owner_docs_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
            _tier_payload=tier,
            _beta_payload=beta,
            _triage_payload=triage,
            _final_gate_payload=final_gate,
            _watchlist_payload=watchlist,
        )
    )
    source_payloads = {
        "growth_tilt_real_cli_suite_summary": suite,
        "growth_tilt_candidate_result_summary": summary,
        "growth_tilt_tier_validation": tier,
        "growth_tilt_beta_adjusted_edge_review": beta,
        "growth_tilt_period_drawdown_cost_triage": triage,
        "growth_tilt_final_gate": final_gate,
        "growth_tilt_forward_aging_watchlist_review": watchlist,
        "growth_tilt_owner_decision_pack_real_run": owner,
    }
    if any(
        _growth_tilt_source_artifact_blocked_status(str(source.get("status")))
        for source in source_payloads.values()
    ):
        status = "GROWTH_TILT_REAL_RESULT_BLOCKED"
    elif watchlist.get("status") == "GROWTH_TILT_WATCHLIST_REVIEWABLE":
        status = "GROWTH_TILT_FORWARD_AGING_REVIEWABLE"
    elif final_gate.get("status") == "NO_USEFUL_GROWTH_TILT":
        status = "NO_USEFUL_GROWTH_TILT"
    elif final_gate.get("status") == "GROWTH_TILT_RESEARCH_ONLY":
        status = "GROWTH_TILT_RESEARCH_ONLY"
    else:
        status = "GROWTH_TILT_NEEDS_MORE_RESEARCH"
    answers = {
        "1_1065_to_1084_cli_complete_real_run": _int(
            _mapping(suite.get("summary")).get("source_command_count")
        )
        == _int(suite.get("required_command_count")),
        "2_useful_growth_tilt_candidate_found": summary.get("status")
        == "GROWTH_TILT_CANDIDATES_FOUND",
        "3_highest_tier": tier.get("highest_tier"),
        "4_candidate_forward_aging_reviewable": watchlist.get("status")
        == "GROWTH_TILT_WATCHLIST_REVIEWABLE",
        "5_edge_only_higher_beta": beta.get("status") == "BETA_EXPLAINS_EDGE",
        "6_period_drawdown_cost_triage_stable": triage.get("status")
        == "GROWTH_TILT_TRIAGE_PASS",
        "7_original_equal_risk_remains_defensive_primary": True,
        "8_controlled_growth_v2_remains_paused": True,
        "9_layer1_selector_remains_archived": True,
        "10_next_minimum_task": _growth_tilt_master_next_task(status),
    }
    payload = _payload(
        report_type="growth_tilt_real_result_master_review",
        title="Growth Tilt Real Result Master Review",
        status=status,
        summary={
            "final_status": status,
            "candidate_summary_status": summary.get("status"),
            "highest_tier": tier.get("highest_tier"),
            "beta_adjusted_edge_status": beta.get("status"),
            "triage_status": triage.get("status"),
            "final_gate_status": final_gate.get("status"),
            "watchlist_status": watchlist.get("status"),
            "owner_recommendation": owner.get("owner_recommendation"),
            **_safety_summary(),
        },
        required_answers=answers,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        final_conclusions=[status, "KEEP_EQUAL_RISK_DEFENSIVE_PRIMARY", "KEEP_ALL_RESEARCH_ONLY"],
        owner_next_action=_growth_tilt_master_next_task(status),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_real_result_master_review",
            "Growth Tilt Real Result Master Review",
            "aits research strategies growth-tilt-real-result-master-review",
            "growth_tilt_real_result_master_review",
            extra_artifact_globs=["docs/research/growth_tilt_real_result_master_review.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Tilt Real Result Master Review")
    payload["master_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_best_growth_tilt_candidate_deep_dive(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
    _beta_payload: Mapping[str, Any] | None = None,
    _frontier_payload: Mapping[str, Any] | None = None,
    _triage_payload: Mapping[str, Any] | None = None,
    _final_gate_payload: Mapping[str, Any] | None = None,
    _master_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    beta = dict(
        _beta_payload
        or run_growth_tilt_beta_adjusted_edge_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    frontier = dict(
        _frontier_payload
        or run_growth_tilt_risk_return_frontier_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    triage = dict(
        _triage_payload
        or run_growth_tilt_period_drawdown_cost_triage(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
        )
    )
    final_gate = dict(
        _final_gate_payload
        or run_growth_tilt_vs_equal_risk_and_qqq_final_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _candidate_summary_payload=summary,
            _beta_payload=beta,
            _triage_payload=triage,
        )
    )
    master = dict(
        _master_payload
        or _read_json_or_empty(output_root / "growth_tilt_real_result_master_review.json")
    )
    candidate = _focused_growth_tilt_candidate(summary)
    blockers = _focused_source_blockers(
        {
            "growth_tilt_candidate_result_summary": summary,
            "growth_tilt_beta_adjusted_edge_review": beta,
            "growth_tilt_risk_return_frontier_review": frontier,
            "growth_tilt_period_drawdown_cost_triage": triage,
            "growth_tilt_final_gate": final_gate,
        }
    )
    config = _load_config(config_path)
    if not candidate:
        blockers.append("focused_candidate_not_found")
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    candidate_policy = _candidate_from_row(candidate) if candidate else {}
    returns, weights = (
        _returns_and_weights(candidate_policy, prices, config)
        if candidate_policy
        else (pd.Series(dtype=float), pd.DataFrame())
    )
    benchmarks = _benchmark_metric_rows(prices, config)
    equal = benchmarks.get(DEFENSIVE_PRIMARY_ID, {})
    qqq = benchmarks.get(PRIMARY_QQQ_BENCHMARK_ID, {})
    drawdown_delta_equal = _round(
        abs(_float(candidate.get("max_drawdown"))) - abs(_float(equal.get("max_drawdown")))
    )
    drawdown_delta_qqq = _round(
        abs(_float(candidate.get("max_drawdown"))) - abs(_float(qqq.get("max_drawdown")))
    )
    answers = {
        "1_return_lift_vs_equal_risk": candidate.get("annual_return_edge_vs_equal_risk"),
        "2_gap_reduction_vs_100_qqq": candidate.get("return_gap_reduction_vs_100_qqq"),
        "3_drawdown_worse_vs_equal_risk": drawdown_delta_equal,
        "4_drawdown_vs_100_qqq_acceptable": drawdown_delta_qqq
        <= _candidate_limit(config, "max_drawdown_increase_vs_equal_risk"),
        "5_weight_path_stable": _weight_path_is_stable(weights),
        "6_return_mostly_higher_qqq_exposure": beta.get("status")
        in {"BETA_EXPLAINS_EDGE", "BETA_ADJUSTED_EDGE_PRESENT"},
        "7_continue_growth_tilt_review": final_gate.get("status")
        in {"GROWTH_TILT_RESEARCH_ONLY", "GROWTH_TILT_TIER2_REVIEWABLE"},
    }
    status = (
        "BEST_GROWTH_TILT_DEEP_DIVE_BLOCKED"
        if blockers
        else (
            "BEST_GROWTH_TILT_DEEP_DIVE_WARN"
            if beta.get("status") != "BETA_ADJUSTED_EDGE_MATERIAL"
            else "BEST_GROWTH_TILT_DEEP_DIVE_READY"
        )
    )
    source_payloads = {
        "growth_tilt_candidate_result_summary": summary,
        "growth_tilt_beta_adjusted_edge_review": beta,
        "growth_tilt_risk_return_frontier_review": frontier,
        "growth_tilt_period_drawdown_cost_triage": triage,
        "growth_tilt_final_gate": final_gate,
        "growth_tilt_real_result_master_review": master,
    }
    payload = _payload(
        report_type="best_growth_tilt_candidate_deep_dive",
        title="Best Growth Tilt Candidate Deep Dive",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "candidate_family": candidate.get("candidate_family"),
            "beta_status": beta.get("status"),
            "final_gate_status": final_gate.get("status"),
            "data_quality_status": _payload_data_quality_status(summary),
            **_safety_summary(),
        },
        candidate_strategy_id=candidate.get("strategy_id"),
        candidate_family=candidate.get("candidate_family"),
        policy_definition_hash=candidate.get("definition_hash"),
        target_vol_config=_target_vol_config(candidate),
        vol_lookback_window=_mapping(candidate.get("policy_definition")).get("vol_lookback"),
        qqq_weight_path_summary=_weight_path_summary(weights, "QQQ"),
        sgov_weight_path_summary=_weight_path_summary(weights, "SGOV"),
        tqqq_weight_path_summary=_weight_path_summary(weights, "TQQQ"),
        annual_return=candidate.get("annual_return"),
        annual_return_edge_vs_equal_risk=candidate.get("annual_return_edge_vs_equal_risk"),
        annual_return_gap_vs_100_qqq=candidate.get("annual_return_gap_vs_100_qqq"),
        max_drawdown=candidate.get("max_drawdown"),
        drawdown_delta_vs_equal_risk=drawdown_delta_equal,
        drawdown_delta_vs_100_qqq=drawdown_delta_qqq,
        sharpe=candidate.get("sharpe"),
        calmar=candidate.get("calmar"),
        turnover=candidate.get("turnover"),
        switch_count=candidate.get("switch_count"),
        realized_candidate_vol=_round(returns.std(ddof=0) * math.sqrt(_annualization(config))),
        data_quality_status=_payload_data_quality_status(summary),
        required_answers=answers,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        blockers=_dedupe_text(blockers),
        report_registry_entry=_report_registry_entry(
            "best_growth_tilt_candidate_deep_dive",
            "Best Growth Tilt Candidate Deep Dive",
            "aits research strategies best-growth-tilt-candidate-deep-dive",
            "best_growth_tilt_candidate_deep_dive",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_vol_target_growth_tilt_local_sensitivity(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
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
            report_type="vol_target_growth_tilt_local_sensitivity",
            title="Vol-Target Growth Tilt Local Sensitivity",
            status="LOCAL_SENSITIVITY_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    base_row = _focused_growth_tilt_candidate(summary)
    base_candidate = (
        _candidate_from_row(base_row)
        if base_row
        else _default_focused_vol_target_candidate(config)
    )
    variants = _local_vol_target_variants(base_candidate, config)
    rows = _metric_rows(
        variants,
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
        data_quality_status=str(data_gate["status"]),
    )
    rows = [
        _local_sensitivity_row(row, base_candidate, config, rank=index + 1)
        for index, row in enumerate(rows)
        if _float(row.get("effective_qqq_beta"))
        <= _candidate_limit(config, "max_effective_qqq_beta")
    ]
    base_result = _local_base_result(rows, base_candidate)
    top = rows[0] if rows else {}
    material = _candidate_limit(config, "beta_adjusted_edge_minimum")
    stable_neighbors = [
        row
        for row in rows
        if row.get("variant_strategy_id") != base_candidate.get("strategy_id")
        and abs(
            _float(row.get("annual_return"))
            - _float(base_result.get("annual_return"))
        )
        <= material
    ]
    if not rows:
        status = "LOCAL_SENSITIVITY_BLOCKED"
    elif _float(top.get("beta_adjusted_edge")) > _float(
        base_result.get("beta_adjusted_edge")
    ) + material:
        status = "LOCAL_VARIANT_IMPROVES_EDGE"
    elif stable_neighbors:
        status = "LOCAL_SENSITIVITY_STABLE"
    else:
        status = "LOCAL_SENSITIVITY_FRAGILE"
    payload = _payload(
        report_type="vol_target_growth_tilt_local_sensitivity",
        title="Vol-Target Growth Tilt Local Sensitivity",
        status=status,
        summary={
            "base_candidate": base_candidate.get("strategy_id"),
            "variant_count": len(rows),
            "stable_neighbor_count": len(stable_neighbors),
            "top_variant": top.get("variant_strategy_id"),
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        base_candidate=base_candidate.get("strategy_id"),
        sensitivity_rows=rows,
        stable_neighbor_rows=stable_neighbors[:10],
        best_variant=top,
        search_constraints=[
            "local_vol_target_perturbation_only",
            "no_new_growth_tilt_family",
            "no_tqqq_heavy",
            "no_options",
            "max_effective_qqq_beta_not_exceeded",
            "beta_adjusted_edge_standard_not_relaxed",
        ],
        data_quality=data_gate,
        source_statuses={"growth_tilt_candidate_result_summary": summary.get("status")},
        source_artifacts={
            "growth_tilt_candidate_result_summary": summary.get("artifact_paths", {})
        },
        report_registry_entry=_report_registry_entry(
            "vol_target_growth_tilt_local_sensitivity",
            "Vol-Target Growth Tilt Local Sensitivity",
            "aits research strategies vol-target-growth-tilt-local-sensitivity",
            "vol_target_growth_tilt_local_sensitivity",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_beta_adjusted_edge_methodology_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
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
            report_type="beta_adjusted_edge_methodology_audit",
            title="Beta-Adjusted Edge Methodology Audit",
            status="BETA_METHOD_BLOCKED",
            data_gate=data_gate,
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    summary = run_growth_tilt_candidate_result_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidate_row = _focused_growth_tilt_candidate(summary)
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    method_rows = _beta_methodology_rows(candidate_row, prices, config)
    material = _candidate_limit(config, "beta_adjusted_edge_minimum")
    best_adjusted = max(
        (_float(row.get("beta_adjusted_edge")) for row in method_rows),
        default=0.0,
    )
    if not candidate_row:
        status = "BETA_METHOD_BLOCKED"
    elif best_adjusted > material:
        status = "BETA_METHOD_SHOWS_TIMING_EDGE"
    elif method_rows:
        status = "BETA_METHOD_CONFIRMS_WEAK_EDGE"
    else:
        status = "BETA_METHOD_INCONCLUSIVE"
    answers = {
        "1_current_non_material_edge_robust": status
        == "BETA_METHOD_CONFIRMS_WEAK_EDGE",
        "2_alternative_reasonable_method_shows_stronger_edge": status
        == "BETA_METHOD_SHOWS_TIMING_EDGE",
        "3_attribution_mouth_too_strict_possible": status
        == "BETA_METHOD_INCONCLUSIVE",
        "4_candidate_return_mostly_beta": status
        == "BETA_METHOD_CONFIRMS_WEAK_EDGE",
        "5_independent_timing_or_risk_budget_contribution": any(
            _float(row.get("timing_edge")) > material
            or _float(row.get("vol_targeting_contribution")) > material
            for row in method_rows
        ),
    }
    payload = _payload(
        report_type="beta_adjusted_edge_methodology_audit",
        title="Beta-Adjusted Edge Methodology Audit",
        status=status,
        summary={
            "candidate_strategy_id": candidate_row.get("strategy_id"),
            "method_count": len(method_rows),
            "best_beta_adjusted_edge": _round(best_adjusted),
            "data_quality_status": data_gate["status"],
            **_safety_summary(),
        },
        candidate_strategy_id=candidate_row.get("strategy_id"),
        methodology_checks=[
            "effective_qqq_beta",
            "benchmark_beta_window",
            "rolling_beta",
            "full_period_beta",
            "sgov_carry",
            "rebalance_contribution",
            "vol_targeting_timing_contribution",
            "beta_over_attribution_guard",
        ],
        method_rows=method_rows,
        required_answers=answers,
        data_quality=data_gate,
        source_statuses={
            "best_growth_tilt_candidate_deep_dive": deep.get("status"),
            "growth_tilt_candidate_result_summary": summary.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "best_growth_tilt_candidate_deep_dive": deep,
                "growth_tilt_candidate_result_summary": summary,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "beta_adjusted_edge_methodology_audit",
            "Beta-Adjusted Edge Methodology Audit",
            "aits research strategies beta-adjusted-edge-methodology-audit",
            "beta_adjusted_edge_methodology_audit",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_balanced_core_role_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
    _beta_method_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    beta_method = dict(
        _beta_method_payload
        or run_beta_adjusted_edge_methodology_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    config = _load_config(config_path)
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    summary = run_growth_tilt_candidate_result_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidate = _focused_growth_tilt_candidate(summary)
    role_rows = _balanced_core_role_rows(candidate, prices, config)
    rows_by_id = {str(row.get("strategy_id")): row for row in role_rows}
    candidate_row = rows_by_id.get(str(candidate.get("strategy_id")), {})
    qqq60 = rows_by_id.get("qqq_60_sgov_40", {})
    if _focused_source_blockers(
        {
            "best_growth_tilt_candidate_deep_dive": deep,
            "beta_adjusted_edge_methodology_audit": beta_method,
        }
    ):
        status = "ROLE_REVIEW_BLOCKED"
    elif not candidate_row:
        status = "ROLE_INCONCLUSIVE"
    elif _float(candidate_row.get("annual_return")) <= _float(
        rows_by_id.get(DEFENSIVE_PRIMARY_ID, {}).get("annual_return")
    ):
        status = "DEFENSIVE_ONLY_BETTER"
    elif beta_method.get("status") == "BETA_METHOD_CONFIRMS_WEAK_EDGE":
        status = "BALANCED_CORE_REVIEWABLE"
    elif _float(candidate_row.get("risk_adjusted_role_score")) > _float(
        qqq60.get("risk_adjusted_role_score")
    ):
        status = "BALANCED_CORE_REVIEWABLE"
    else:
        status = "GROWTH_COMPONENT_NOT_SUPPORTED"
    answers = {
        "1_candidate_better_balanced_core_than_equal_risk": _float(
            candidate_row.get("annual_return")
        )
        > _float(rows_by_id.get(DEFENSIVE_PRIMARY_ID, {}).get("annual_return")),
        "2_candidate_better_than_qqq_60_sgov_40": _float(
            candidate_row.get("risk_adjusted_role_score")
        )
        > _float(qqq60.get("risk_adjusted_role_score")),
        "3_not_growth_component_but_balanced_core": status
        == "BALANCED_CORE_REVIEWABLE",
        "4_need_independent_balanced_core_standard": True,
    }
    payload = _payload(
        report_type="growth_tilt_balanced_core_role_review",
        title="Growth Tilt Balanced Core Role Review",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "candidate_role_status": status,
            "row_count": len(role_rows),
            "data_quality_status": deep.get("data_quality_status"),
            **_safety_summary(),
        },
        role_rows=role_rows,
        required_answers=answers,
        source_statuses={
            "best_growth_tilt_candidate_deep_dive": deep.get("status"),
            "beta_adjusted_edge_methodology_audit": beta_method.get("status"),
            "growth_tilt_candidate_result_summary": summary.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "best_growth_tilt_candidate_deep_dive": deep,
                "beta_adjusted_edge_methodology_audit": beta_method,
                "growth_tilt_candidate_result_summary": summary,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_balanced_core_role_review",
            "Growth Tilt Balanced Core Role Review",
            "aits research strategies growth-tilt-balanced-core-role-review",
            "growth_tilt_balanced_core_role_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_vs_equal_risk_missed_upside_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    summary = run_growth_tilt_candidate_result_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidate = _focused_growth_tilt_candidate(summary)
    rows = _missed_upside_rows(candidate, prices, config)
    avg_improvement = (
        sum(_float(row.get("net_missed_upside_improvement")) for row in rows) / len(rows)
        if rows
        else 0.0
    )
    max_penalty = max((_float(row.get("drawdown_penalty")) for row in rows), default=0.0)
    if not candidate or _focused_source_blockers({"best_growth_tilt_candidate_deep_dive": deep}):
        status = "MISSED_UPSIDE_REVIEW_BLOCKED"
    elif avg_improvement > _candidate_limit(config, "beta_adjusted_edge_minimum"):
        status = "MISSED_UPSIDE_REDUCTION_MATERIAL"
    elif avg_improvement > 0.0 and max_penalty <= _candidate_limit(
        config, "max_drawdown_increase_vs_equal_risk"
    ):
        status = "MISSED_UPSIDE_REDUCTION_MODEST"
    else:
        status = "MISSED_UPSIDE_REDUCTION_NOT_WORTH_RISK"
    answers = {
        "1_missed_upside_significantly_reduced": status
        == "MISSED_UPSIDE_REDUCTION_MATERIAL",
        "2_candidate_keeps_up_in_strong_trend": any(
            row.get("period") == "strong_trend_periods"
            and _float(row.get("missed_upside_vs_100_qqq_candidate"))
            < _float(row.get("missed_upside_vs_100_qqq_equal_risk"))
            for row in rows
        ),
        "3_candidate_loses_equal_risk_protection": max_penalty
        > _candidate_limit(config, "max_drawdown_increase_vs_equal_risk"),
        "4_improvement_worth_extra_drawdown": status
        in {"MISSED_UPSIDE_REDUCTION_MATERIAL", "MISSED_UPSIDE_REDUCTION_MODEST"},
    }
    payload = _payload(
        report_type="growth_tilt_vs_equal_risk_missed_upside_review",
        title="Growth Tilt Vs Equal-Risk Missed Upside Review",
        status=status,
        summary={
            "candidate_strategy_id": candidate.get("strategy_id"),
            "period_count": len(rows),
            "avg_net_missed_upside_improvement": _round(avg_improvement),
            "max_drawdown_penalty": _round(max_penalty),
            **_safety_summary(),
        },
        period_rows=rows,
        required_answers=answers,
        source_statuses={
            "best_growth_tilt_candidate_deep_dive": deep.get("status"),
            "growth_tilt_candidate_result_summary": summary.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "best_growth_tilt_candidate_deep_dive": deep,
                "growth_tilt_candidate_result_summary": summary,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_vs_equal_risk_missed_upside_review",
            "Growth Tilt Vs Equal-Risk Missed Upside Review",
            "aits research strategies growth-tilt-vs-equal-risk-missed-upside-review",
            "growth_tilt_vs_equal_risk_missed_upside_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_parameter_neighbor_finalist_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _sensitivity_payload: Mapping[str, Any] | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
    _beta_method_payload: Mapping[str, Any] | None = None,
    _role_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sensitivity = dict(
        _sensitivity_payload
        or run_vol_target_growth_tilt_local_sensitivity(
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
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    beta_method = dict(
        _beta_method_payload
        or run_beta_adjusted_edge_methodology_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    role = dict(
        _role_payload
        or run_growth_tilt_balanced_core_role_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
        )
    )
    rows = _records(sensitivity.get("sensitivity_rows"))
    base_id = str(sensitivity.get("base_candidate") or FOCUSED_GROWTH_TILT_CANDIDATE_ID)
    base = next((row for row in rows if row.get("variant_strategy_id") == base_id), {})
    neighbor = next(
        (row for row in rows if row.get("variant_strategy_id") != base_id),
        {},
    )
    blockers = _focused_source_blockers(
        {
            "vol_target_growth_tilt_local_sensitivity": sensitivity,
            "best_growth_tilt_candidate_deep_dive": deep,
            "beta_adjusted_edge_methodology_audit": beta_method,
            "growth_tilt_balanced_core_role_review": role,
        }
    )
    if blockers:
        status = "FINALIST_REVIEW_BLOCKED"
    elif sensitivity.get("status") == "LOCAL_SENSITIVITY_FRAGILE":
        status = "NO_STABLE_FINALIST"
    elif sensitivity.get("status") == "LOCAL_VARIANT_IMPROVES_EDGE" and neighbor:
        status = "NEIGHBOR_CANDIDATE_BETTER"
    else:
        status = "BASE_CANDIDATE_REMAINS_BEST"
    recommended = neighbor if status == "NEIGHBOR_CANDIDATE_BETTER" else base
    row = {
        "base_candidate": base_id,
        "neighbor_candidate": neighbor.get("variant_strategy_id"),
        "parameter_difference": neighbor.get("parameter_delta"),
        "annual_return": recommended.get("annual_return"),
        "max_drawdown": recommended.get("max_drawdown"),
        "sharpe": recommended.get("sharpe"),
        "calmar": recommended.get("calmar"),
        "turnover": recommended.get("turnover"),
        "effective_qqq_beta": recommended.get("effective_qqq_beta"),
        "beta_adjusted_edge": recommended.get("beta_adjusted_edge"),
        "local_robustness_score": recommended.get("local_robustness_score"),
        "recommended_finalist": recommended.get("variant_strategy_id") or base_id,
    }
    payload = _payload(
        report_type="growth_tilt_parameter_neighbor_finalist_review",
        title="Growth Tilt Parameter Neighbor Finalist Review",
        status=status,
        summary={
            "base_candidate": base_id,
            "recommended_finalist": row["recommended_finalist"],
            "sensitivity_status": sensitivity.get("status"),
            **_safety_summary(),
        },
        finalist_row=row,
        source_statuses={
            "vol_target_growth_tilt_local_sensitivity": sensitivity.get("status"),
            "best_growth_tilt_candidate_deep_dive": deep.get("status"),
            "beta_adjusted_edge_methodology_audit": beta_method.get("status"),
            "growth_tilt_balanced_core_role_review": role.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "vol_target_growth_tilt_local_sensitivity": sensitivity,
                "best_growth_tilt_candidate_deep_dive": deep,
                "beta_adjusted_edge_methodology_audit": beta_method,
                "growth_tilt_balanced_core_role_review": role,
            }
        ),
        blockers=blockers,
        report_registry_entry=_report_registry_entry(
            "growth_tilt_parameter_neighbor_finalist_review",
            "Growth Tilt Parameter Neighbor Finalist Review",
            "aits research strategies growth-tilt-parameter-neighbor-finalist-review",
            "growth_tilt_parameter_neighbor_finalist_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_watchlist_reconsideration_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
    _sensitivity_payload: Mapping[str, Any] | None = None,
    _beta_method_payload: Mapping[str, Any] | None = None,
    _role_payload: Mapping[str, Any] | None = None,
    _missed_payload: Mapping[str, Any] | None = None,
    _finalist_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    sensitivity = dict(
        _sensitivity_payload
        or run_vol_target_growth_tilt_local_sensitivity(
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
    beta_method = dict(
        _beta_method_payload
        or run_beta_adjusted_edge_methodology_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    role = dict(
        _role_payload
        or run_growth_tilt_balanced_core_role_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
        )
    )
    missed = dict(
        _missed_payload
        or run_growth_tilt_vs_equal_risk_missed_upside_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    finalist = dict(
        _finalist_payload
        or run_growth_tilt_parameter_neighbor_finalist_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _sensitivity_payload=sensitivity,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
            _role_payload=role,
        )
    )
    config = _load_config(config_path)
    source_payloads = {
        "best_growth_tilt_candidate_deep_dive": deep,
        "vol_target_growth_tilt_local_sensitivity": sensitivity,
        "beta_adjusted_edge_methodology_audit": beta_method,
        "growth_tilt_balanced_core_role_review": role,
        "growth_tilt_vs_equal_risk_missed_upside_review": missed,
        "growth_tilt_parameter_neighbor_finalist_review": finalist,
    }
    blockers = _focused_source_blockers(source_payloads)
    warning_reasons: list[str] = []
    if beta_method.get("status") == "BETA_METHOD_CONFIRMS_WEAK_EDGE":
        warning_reasons.append("beta_adjusted_edge_still_weak")
    if sensitivity.get("status") == "LOCAL_SENSITIVITY_FRAGILE":
        blockers.append("local_sensitivity_fragile")
    if role.get("status") in {"ROLE_INCONCLUSIVE", "DEFENSIVE_ONLY_BETTER"}:
        warning_reasons.append(f"role_status:{role.get('status')}")
    growth_watchlist = (
        missed.get("status") == "MISSED_UPSIDE_REDUCTION_MATERIAL"
        and sensitivity.get("status")
        in {"LOCAL_SENSITIVITY_STABLE", "LOCAL_VARIANT_IMPROVES_EDGE"}
        and beta_method.get("status") == "BETA_METHOD_SHOWS_TIMING_EDGE"
        and not blockers
    )
    balanced_watchlist = (
        role.get("status") == "BALANCED_CORE_REVIEWABLE"
        and sensitivity.get("status")
        in {"LOCAL_SENSITIVITY_STABLE", "LOCAL_VARIANT_IMPROVES_EDGE"}
        and not blockers
    )
    if blockers:
        status = (
            "NO_STABLE_FINALIST"
            if "local_sensitivity_fragile" in blockers
            else "WATCHLIST_RECONSIDERATION_BLOCKED"
        )
    elif growth_watchlist:
        status = "GROWTH_TILT_WATCHLIST_RECONSIDERED_READY"
    elif balanced_watchlist:
        status = "BALANCED_CORE_WATCHLIST_REVIEWABLE"
    else:
        status = "KEEP_GROWTH_TILT_RESEARCH_ONLY"
    watchlist_allowed = status in {
        "GROWTH_TILT_WATCHLIST_RECONSIDERED_READY",
        "BALANCED_CORE_WATCHLIST_REVIEWABLE",
    }
    recommended_role = (
        "GROWTH_TILT"
        if status == "GROWTH_TILT_WATCHLIST_RECONSIDERED_READY"
        else (
            "BALANCED_CORE"
            if status == "BALANCED_CORE_WATCHLIST_REVIEWABLE"
            else "RESEARCH_ONLY"
        )
    )
    payload = _payload(
        report_type="growth_tilt_watchlist_reconsideration_gate",
        title="Growth Tilt Watchlist Reconsideration Gate",
        status=status,
        summary={
            "candidate_strategy_id": deep.get("candidate_strategy_id"),
            "recommended_role": recommended_role,
            "watchlist_allowed": watchlist_allowed,
            "watchlist_type": recommended_role.lower(),
            **_safety_summary(),
        },
        candidate_strategy_id=deep.get("candidate_strategy_id"),
        recommended_role=recommended_role,
        watchlist_allowed=watchlist_allowed,
        watchlist_type=recommended_role.lower(),
        blocking_reasons=_dedupe_text(blockers),
        warning_reasons=_dedupe_text(warning_reasons),
        required_forward_days=_candidate_limit(config, "required_forward_days"),
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_watchlist_reconsideration_gate",
            "Growth Tilt Watchlist Reconsideration Gate",
            "aits research strategies growth-tilt-watchlist-reconsideration-gate",
            "growth_tilt_watchlist_reconsideration_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_owner_diagnosis_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DIAGNOSIS_PACK_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
    _sensitivity_payload: Mapping[str, Any] | None = None,
    _beta_method_payload: Mapping[str, Any] | None = None,
    _role_payload: Mapping[str, Any] | None = None,
    _missed_payload: Mapping[str, Any] | None = None,
    _finalist_payload: Mapping[str, Any] | None = None,
    _watchlist_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    sensitivity = dict(
        _sensitivity_payload
        or run_vol_target_growth_tilt_local_sensitivity(
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
    beta_method = dict(
        _beta_method_payload
        or run_beta_adjusted_edge_methodology_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    role = dict(
        _role_payload
        or run_growth_tilt_balanced_core_role_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
        )
    )
    missed = dict(
        _missed_payload
        or run_growth_tilt_vs_equal_risk_missed_upside_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    finalist = dict(
        _finalist_payload
        or run_growth_tilt_parameter_neighbor_finalist_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _sensitivity_payload=sensitivity,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
            _role_payload=role,
        )
    )
    watchlist = dict(
        _watchlist_payload
        or run_growth_tilt_watchlist_reconsideration_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _sensitivity_payload=sensitivity,
            _beta_method_payload=beta_method,
            _role_payload=role,
            _missed_payload=missed,
            _finalist_payload=finalist,
        )
    )
    source_payloads = {
        "best_growth_tilt_candidate_deep_dive": deep,
        "vol_target_growth_tilt_local_sensitivity": sensitivity,
        "beta_adjusted_edge_methodology_audit": beta_method,
        "growth_tilt_balanced_core_role_review": role,
        "growth_tilt_vs_equal_risk_missed_upside_review": missed,
        "growth_tilt_parameter_neighbor_finalist_review": finalist,
        "growth_tilt_watchlist_reconsideration_gate": watchlist,
    }
    blockers = _focused_source_blockers(source_payloads)
    if blockers:
        recommendation = "BLOCKED"
    elif watchlist.get("status") == "GROWTH_TILT_WATCHLIST_RECONSIDERED_READY":
        recommendation = "ADD_AS_GROWTH_TILT_FORWARD_AGING_CANDIDATE"
    elif watchlist.get("status") == "BALANCED_CORE_WATCHLIST_REVIEWABLE":
        recommendation = "ADD_AS_BALANCED_CORE_FORWARD_AGING_CANDIDATE"
    elif finalist.get("status") == "NO_STABLE_FINALIST":
        recommendation = "NO_STABLE_GROWTH_TILT_CANDIDATE"
    elif beta_method.get("status") == "BETA_METHOD_INCONCLUSIVE":
        recommendation = "NEED_MORE_HISTORY"
    else:
        recommendation = "KEEP_GROWTH_TILT_RESEARCH_ONLY"
    answers = {
        "1_why_candidate_was_found": "vol_target_growth_tilt improved equal-risk upside",
        "2_why_not_watchlist_before": "beta_adjusted_edge_not_material",
        "3_beta_adjusted_edge_really_insufficient": beta_method.get("status")
        == "BETA_METHOD_CONFIRMS_WEAK_EDGE",
        "4_possible_balanced_core": role.get("status") == "BALANCED_CORE_REVIEWABLE",
        "5_better_neighbor_exists": finalist.get("status") == "NEIGHBOR_CANDIDATE_BETTER",
        "6_missed_upside_improvement_worth_it": missed.get("status")
        in {"MISSED_UPSIDE_REDUCTION_MATERIAL", "MISSED_UPSIDE_REDUCTION_MODEST"},
        "7_forward_aging_watchlist_allowed": watchlist.get("watchlist_allowed") is True,
        "8_equal_risk_remains_defensive_primary": True,
        "9_continue_no_paper_shadow_production_broker": True,
    }
    payload = _payload(
        report_type="growth_tilt_owner_diagnosis_pack",
        title="Growth Tilt Owner Diagnosis Pack",
        status="GROWTH_TILT_OWNER_DIAGNOSIS_PACK_READY"
        if recommendation != "BLOCKED"
        else "BLOCKED",
        summary={
            "owner_recommendation": recommendation,
            "candidate_strategy_id": deep.get("candidate_strategy_id"),
            "watchlist_status": watchlist.get("status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_owner_diagnosis_pack",
            "Growth Tilt Owner Diagnosis Pack",
            "aits research strategies growth-tilt-owner-diagnosis-pack",
            "growth_tilt_owner_diagnosis_pack",
            extra_artifact_globs=["docs/research/growth_tilt_owner_diagnosis_pack.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Tilt Owner Diagnosis Pack")
    payload["owner_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_growth_tilt_focused_diagnosis_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_GROWTH_TILT_FOCUSED_DIAGNOSIS_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_GROWTH_TILT_OWNER_DIAGNOSIS_PACK_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _deep_dive_payload: Mapping[str, Any] | None = None,
    _sensitivity_payload: Mapping[str, Any] | None = None,
    _beta_method_payload: Mapping[str, Any] | None = None,
    _role_payload: Mapping[str, Any] | None = None,
    _missed_payload: Mapping[str, Any] | None = None,
    _finalist_payload: Mapping[str, Any] | None = None,
    _watchlist_payload: Mapping[str, Any] | None = None,
    _owner_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    deep = dict(
        _deep_dive_payload
        or run_best_growth_tilt_candidate_deep_dive(
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
    sensitivity = dict(
        _sensitivity_payload
        or run_vol_target_growth_tilt_local_sensitivity(
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
    beta_method = dict(
        _beta_method_payload
        or run_beta_adjusted_edge_methodology_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    role = dict(
        _role_payload
        or run_growth_tilt_balanced_core_role_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
        )
    )
    missed = dict(
        _missed_payload
        or run_growth_tilt_vs_equal_risk_missed_upside_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
        )
    )
    finalist = dict(
        _finalist_payload
        or run_growth_tilt_parameter_neighbor_finalist_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _sensitivity_payload=sensitivity,
            _deep_dive_payload=deep,
            _beta_method_payload=beta_method,
            _role_payload=role,
        )
    )
    watchlist = dict(
        _watchlist_payload
        or run_growth_tilt_watchlist_reconsideration_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _sensitivity_payload=sensitivity,
            _beta_method_payload=beta_method,
            _role_payload=role,
            _missed_payload=missed,
            _finalist_payload=finalist,
        )
    )
    owner = dict(
        _owner_payload
        or run_growth_tilt_owner_diagnosis_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            docs_path=owner_docs_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _deep_dive_payload=deep,
            _sensitivity_payload=sensitivity,
            _beta_method_payload=beta_method,
            _role_payload=role,
            _missed_payload=missed,
            _finalist_payload=finalist,
            _watchlist_payload=watchlist,
        )
    )
    source_payloads = {
        "best_growth_tilt_candidate_deep_dive": deep,
        "vol_target_growth_tilt_local_sensitivity": sensitivity,
        "beta_adjusted_edge_methodology_audit": beta_method,
        "growth_tilt_balanced_core_role_review": role,
        "growth_tilt_vs_equal_risk_missed_upside_review": missed,
        "growth_tilt_parameter_neighbor_finalist_review": finalist,
        "growth_tilt_watchlist_reconsideration_gate": watchlist,
        "growth_tilt_owner_diagnosis_pack": owner,
    }
    blockers = _focused_source_blockers(source_payloads)
    if blockers:
        status = "GROWTH_TILT_DIAGNOSIS_BLOCKED"
    elif watchlist.get("status") == "GROWTH_TILT_WATCHLIST_RECONSIDERED_READY":
        status = "GROWTH_TILT_FORWARD_AGING_REVIEWABLE"
    elif watchlist.get("status") == "BALANCED_CORE_WATCHLIST_REVIEWABLE":
        status = "BALANCED_CORE_FORWARD_AGING_REVIEWABLE"
    elif finalist.get("status") == "NO_STABLE_FINALIST":
        status = "NO_STABLE_GROWTH_TILT_CANDIDATE"
    else:
        status = "KEEP_GROWTH_TILT_RESEARCH_ONLY"
    answers = {
        "1_candidate_stable": sensitivity.get("status")
        in {"LOCAL_SENSITIVITY_STABLE", "LOCAL_VARIANT_IMPROVES_EDGE"},
        "2_better_neighbor_exists": finalist.get("status") == "NEIGHBOR_CANDIDATE_BETTER",
        "3_beta_adjusted_edge_not_material_holds": beta_method.get("status")
        == "BETA_METHOD_CONFIRMS_WEAK_EDGE",
        "4_timing_or_risk_budget_contribution_exists": beta_method.get("status")
        == "BETA_METHOD_SHOWS_TIMING_EDGE",
        "5_more_suitable_as_balanced_core": role.get("status")
        == "BALANCED_CORE_REVIEWABLE",
        "6_watchlist_allowed": watchlist.get("watchlist_allowed") is True,
        "7_next_step_if_not_allowed": _focused_master_next_step(status),
        "8_equal_risk_remains_defensive_primary": True,
    }
    payload = _payload(
        report_type="growth_tilt_focused_diagnosis_master_review",
        title="Growth Tilt Focused Diagnosis Master Review",
        status=status,
        summary={
            "final_status": status,
            "candidate_strategy_id": deep.get("candidate_strategy_id"),
            "owner_recommendation": owner.get("owner_recommendation"),
            "watchlist_status": watchlist.get("status"),
            **_safety_summary(),
        },
        owner_recommendation=owner.get("owner_recommendation"),
        required_answers=answers,
        final_conclusions=[
            status,
            "KEEP_EQUAL_RISK_DEFENSIVE_PRIMARY",
            "NO_PAPER_SHADOW_NO_PRODUCTION_NO_BROKER",
        ],
        owner_next_action=_focused_master_next_step(status),
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        report_registry_entry=_report_registry_entry(
            "growth_tilt_focused_diagnosis_master_review",
            "Growth Tilt Focused Diagnosis Master Review",
            "aits research strategies growth-tilt-focused-diagnosis-master-review",
            "growth_tilt_focused_diagnosis_master_review",
            extra_artifact_globs=[
                "docs/research/growth_tilt_focused_diagnosis_master_review.md"
            ],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Growth Tilt Focused Diagnosis Master Review")
    payload["master_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_watchlist_activation_contract(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _master_payload: Mapping[str, Any] | None = None,
    _owner_payload: Mapping[str, Any] | None = None,
    _watchlist_payload: Mapping[str, Any] | None = None,
    _role_payload: Mapping[str, Any] | None = None,
    _finalist_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    master = dict(
        _master_payload
        or run_growth_tilt_focused_diagnosis_master_review(
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
    owner = dict(
        _owner_payload
        or _read_json_or_empty(output_root / "growth_tilt_owner_diagnosis_pack.json")
    )
    watchlist = dict(
        _watchlist_payload
        or _read_json_or_empty(output_root / "growth_tilt_watchlist_reconsideration_gate.json")
    )
    role = dict(
        _role_payload
        or _read_json_or_empty(output_root / "growth_tilt_balanced_core_role_review.json")
    )
    finalist = dict(
        _finalist_payload
        or _read_json_or_empty(output_root / "growth_tilt_parameter_neighbor_finalist_review.json")
    )
    source_payloads = {
        "growth_tilt_focused_diagnosis_master_review": master,
        "growth_tilt_owner_diagnosis_pack": owner,
        "growth_tilt_watchlist_reconsideration_gate": watchlist,
        "growth_tilt_balanced_core_role_review": role,
        "growth_tilt_parameter_neighbor_finalist_review": finalist,
    }
    blocking_reasons = _focused_source_blockers(source_payloads)
    warning_reasons: list[str] = []
    candidate_id = str(
        _mapping(master.get("summary")).get("candidate_strategy_id")
        or watchlist.get("candidate_strategy_id")
        or FOCUSED_GROWTH_TILT_CANDIDATE_ID
    )
    if candidate_id != FOCUSED_GROWTH_TILT_CANDIDATE_ID:
        blocking_reasons.append("candidate_strategy_id_changed")
    if master.get("status") != "BALANCED_CORE_FORWARD_AGING_REVIEWABLE":
        blocking_reasons.append("focused_master_not_balanced_core_reviewable")
    if owner.get("owner_recommendation") != "ADD_AS_BALANCED_CORE_FORWARD_AGING_CANDIDATE":
        blocking_reasons.append("owner_recommendation_not_balanced_core_forward_aging")
    if watchlist.get("status") != "BALANCED_CORE_WATCHLIST_REVIEWABLE":
        blocking_reasons.append("watchlist_gate_not_balanced_core_reviewable")
    if role.get("status") != "BALANCED_CORE_REVIEWABLE":
        warning_reasons.append(f"role_status:{role.get('status')}")
    if finalist.get("status") == "NO_STABLE_FINALIST":
        blocking_reasons.append("no_stable_finalist")
    safety_violations = _safety_violations(source_payloads.values())
    blocking_reasons.extend(safety_violations)

    blocking_reasons = _dedupe_text(blocking_reasons)
    warning_reasons = _dedupe_text(
        [*warning_reasons, *_records_to_text(watchlist.get("warning_reasons"))]
    )
    watchlist_activation_allowed = not blocking_reasons
    if watchlist_activation_allowed:
        status = "BALANCED_CORE_WATCHLIST_CONTRACT_READY"
    elif safety_violations or _focused_source_blockers(source_payloads):
        status = "BALANCED_CORE_WATCHLIST_BLOCKED"
    else:
        status = "BALANCED_CORE_WATCHLIST_NEEDS_OWNER_REVIEW"
    payload = _payload(
        report_type="balanced_core_watchlist_activation_contract",
        title="Balanced Core Watchlist Activation Contract",
        status=status,
        summary={
            "candidate_strategy_id": candidate_id,
            "candidate_role": "balanced_core_candidate",
            "watchlist_activation_allowed": watchlist_activation_allowed,
            "watchlist_scope": "research_only_forward_aging",
            "blocking_reason_count": len(blocking_reasons),
            **_safety_summary(),
        },
        candidate_strategy_id=candidate_id,
        candidate_role="balanced_core_candidate",
        watchlist_activation_allowed=watchlist_activation_allowed,
        watchlist_scope="research_only_forward_aging",
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        blocking_reasons=blocking_reasons,
        warning_reasons=warning_reasons,
        report_registry_entry=_report_registry_entry(
            "balanced_core_watchlist_activation_contract",
            "Balanced Core Watchlist Activation Contract",
            "aits research strategies balanced-core-watchlist-activation-contract",
            "balanced_core_watchlist_activation_contract",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_definition_lock(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _candidate_summary_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    summary = dict(
        _candidate_summary_payload
        or run_growth_tilt_candidate_result_summary(
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
    candidate = _focused_growth_tilt_candidate(summary)
    blocking_reasons: list[str] = []
    if not candidate:
        blocking_reasons.append("focused_candidate_missing")
    if candidate.get("strategy_id") != FOCUSED_GROWTH_TILT_CANDIDATE_ID:
        blocking_reasons.append("candidate_strategy_id_changed")
    source_blockers = _focused_source_blockers(
        {"growth_tilt_candidate_result_summary": summary}
    )
    blocking_reasons.extend(source_blockers)
    definition = (
        _balanced_core_definition(
            candidate,
            config=config,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
        )
        if candidate
        else {}
    )
    definition_hash = _stable_hash(definition) if definition else ""
    existing = _read_json_or_empty(output_root / "balanced_core_definition_lock.json")
    existing_hash = str(existing.get("definition_hash") or "")
    existing_strategy_id = str(existing.get("strategy_id") or "")
    if existing_hash and existing_hash != definition_hash:
        if existing_strategy_id == FOCUSED_GROWTH_TILT_CANDIDATE_ID:
            blocking_reasons.append("definition_hash_changed_for_same_strategy_id")
            status = "BALANCED_CORE_DEFINITION_CHANGED"
        else:
            blocking_reasons.append("definition_hash_conflicted_with_existing_lock")
            status = "BALANCED_CORE_DEFINITION_CONFLICTED"
    elif blocking_reasons:
        status = "BALANCED_CORE_DEFINITION_BLOCKED"
    else:
        status = "BALANCED_CORE_DEFINITION_LOCKED"
    blocking_reasons = _dedupe_text(blocking_reasons)
    payload = _payload(
        report_type="balanced_core_definition_lock",
        title="Balanced Core Definition Lock",
        status=status,
        summary={
            "strategy_id": candidate.get("strategy_id"),
            "definition_hash": definition_hash,
            "definition_locked": status == "BALANCED_CORE_DEFINITION_LOCKED",
            "blocking_reason_count": len(blocking_reasons),
            **_safety_summary(),
        },
        strategy_id=candidate.get("strategy_id"),
        base_strategy_id=candidate.get("base_strategy_id") or DEFENSIVE_PRIMARY_ID,
        candidate_family=candidate.get("candidate_family"),
        policy_definition=definition.get("policy_definition", {}),
        target_vol_config=definition.get("target_vol_config", {}),
        vol_lookback_window=definition.get("vol_lookback_window"),
        qqq_weight_bounds=definition.get("qqq_weight_bounds", {}),
        sgov_weight_bounds=definition.get("sgov_weight_bounds", {}),
        tqqq_weight_bounds=definition.get("tqqq_weight_bounds", {}),
        rebalance_rule=definition.get("rebalance_rule"),
        data_inputs=definition.get("data_inputs", {}),
        data_source_contract=definition.get("data_source_contract", {}),
        definition_hash=definition_hash,
        historical_observation_rewrite_allowed=False,
        definition_change_requires_new_strategy_id=True,
        blocking_reasons=blocking_reasons,
        source_statuses={"growth_tilt_candidate_result_summary": summary.get("status")},
        source_artifacts=_artifact_paths_by_report(
            {"growth_tilt_candidate_result_summary": summary}
        ),
        report_registry_entry=_report_registry_entry(
            "balanced_core_definition_lock",
            "Balanced Core Definition Lock",
            "aits research strategies balanced-core-definition-lock",
            "balanced_core_definition_lock",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_forward_aging_dry_run(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    decision_date: date | None = None,
    _activation_payload: Mapping[str, Any] | None = None,
    _definition_lock_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    activation = dict(
        _activation_payload
        or run_balanced_core_watchlist_activation_contract(
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
    definition_lock = dict(
        _definition_lock_payload
        or run_balanced_core_definition_lock(
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
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    blockers: list[str] = []
    if activation.get("status") != "BALANCED_CORE_WATCHLIST_CONTRACT_READY":
        blockers.append("activation_contract_not_ready")
    if definition_lock.get("status") != "BALANCED_CORE_DEFINITION_LOCKED":
        blockers.append("definition_lock_not_locked")
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    prices = pd.DataFrame()
    resolved_date = decision_date
    candidate_weights: dict[str, float] = {}
    equal_risk_weights: dict[str, float] = {}
    qqq_weights: dict[str, float] = {"QQQ": 1.0, "TQQQ": 0.0, "SGOV": 0.0}
    signal_inputs: dict[str, Any] = {}
    if not blockers:
        prices = _price_matrix(
            prices_path,
            config,
            start_date=start_date,
            end_date=end_date,
        )
        resolved_date = _resolve_balanced_core_decision_date(
            prices,
            decision_date or _safe_date(data_gate.get("as_of")) or as_of_date,
        )
        candidate = _candidate_from_definition_lock(definition_lock)
        candidate_weights = _target_weights_at(candidate, prices, config, resolved_date)
        equal_risk_weights = _target_weights_at(
            {"strategy_id": DEFENSIVE_PRIMARY_ID, "special_policy": "equal_risk"},
            prices,
            config,
            resolved_date,
        )
        signal_inputs = _balanced_core_signal_inputs(
            candidate,
            prices,
            config,
            resolved_date,
        )
    blockers = _dedupe_text(blockers)
    warning_count = _int(data_gate.get("warning_count"))
    if blockers:
        status = "BALANCED_CORE_FORWARD_DRY_RUN_BLOCKED"
    elif warning_count > 0 or "WARNING" in str(data_gate.get("status", "")).upper():
        status = "BALANCED_CORE_FORWARD_DRY_RUN_WARN"
    else:
        status = "BALANCED_CORE_FORWARD_DRY_RUN_PASS"
    payload = _payload(
        report_type="balanced_core_forward_aging_dry_run",
        title="Balanced Core Forward-Aging Dry Run",
        status=status,
        summary={
            "decision_date": resolved_date.isoformat() if resolved_date else None,
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "candidate_definition_hash": definition_lock.get("definition_hash"),
            "data_quality_status": data_gate.get("status"),
            "observation_written": False,
            "blocking_reason_count": len(blockers),
            **_safety_summary(),
        },
        decision_date=resolved_date.isoformat() if resolved_date else None,
        candidate_strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        candidate_definition_hash=definition_lock.get("definition_hash"),
        target_weight_qqq=candidate_weights.get("QQQ", 0.0),
        target_weight_tqqq=candidate_weights.get("TQQQ", 0.0),
        target_weight_sgov=candidate_weights.get("SGOV", 0.0),
        target_weights=candidate_weights,
        comparator_equal_risk_weights=equal_risk_weights,
        comparator_100_qqq_weights=qqq_weights,
        signal_inputs_used=signal_inputs,
        data_quality_status=data_gate.get("status"),
        data_quality=data_gate,
        observation_written=False,
        blocking_reasons=blockers,
        source_statuses={
            "balanced_core_watchlist_activation_contract": activation.get("status"),
            "balanced_core_definition_lock": definition_lock.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "balanced_core_watchlist_activation_contract": activation,
                "balanced_core_definition_lock": definition_lock,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "balanced_core_forward_aging_dry_run",
            "Balanced Core Forward-Aging Dry Run",
            "aits research strategies balanced-core-forward-aging-dry-run",
            "balanced_core_forward_aging_dry_run",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_first_observation_write(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    decision_date: date | None = None,
    _activation_payload: Mapping[str, Any] | None = None,
    _definition_lock_payload: Mapping[str, Any] | None = None,
    _dry_run_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if decision_date is not None:
        existing_path = _balanced_core_observation_path(output_root, decision_date)
        existing = _read_json_or_empty(existing_path)
        if _is_balanced_core_written_observation(existing):
            existing["status"] = "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS"
            existing["observation_written"] = False
            existing["idempotency_status"] = "already_exists_no_rewrite"
            return existing
    dry_run = dict(
        _dry_run_payload
        or run_balanced_core_forward_aging_dry_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            decision_date=decision_date,
            _activation_payload=_activation_payload,
            _definition_lock_payload=_definition_lock_payload,
        )
    )
    resolved_date = _safe_date(dry_run.get("decision_date")) or decision_date
    if resolved_date is None:
        resolved_date = as_of_date or DEFAULT_AI_REGIME_BACKTEST_START
    observation_root = output_root / "forward_aging_observations"
    artifact_id = f"balanced_core_forward_aging_observation_{resolved_date.isoformat()}"
    json_path = observation_root / f"{artifact_id}.json"
    if json_path.exists():
        existing = _read_json_or_empty(json_path)
        if _is_balanced_core_written_observation(existing):
            existing["status"] = "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS"
            existing["observation_written"] = False
            existing["idempotency_status"] = "already_exists_no_rewrite"
            return existing
    blockers = list(_records_to_text(dry_run.get("blocking_reasons")))
    data_quality_status = str(dry_run.get("data_quality_status") or "")
    if dry_run.get("status") not in {
        "BALANCED_CORE_FORWARD_DRY_RUN_PASS",
        "BALANCED_CORE_FORWARD_DRY_RUN_WARN",
    }:
        blockers.append("dry_run_not_passed")
    if data_quality_status.upper().endswith("BLOCKED"):
        blockers.append("data_quality_blocked")
    blockers = _dedupe_text(blockers)
    observation_written = not blockers
    status = (
        "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN"
        if observation_written
        else "BALANCED_CORE_FIRST_OBSERVATION_BLOCKED"
    )
    observation = _balanced_core_observation_row(dry_run, observation_written)
    payload = _payload(
        report_type="balanced_core_first_observation_write",
        title="Balanced Core Forward-Aging Observation",
        status=status,
        summary={
            "decision_date": resolved_date.isoformat(),
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "observation_written": observation_written,
            "data_quality_status": data_quality_status,
            "blocking_reason_count": len(blockers),
            **_safety_summary(),
        },
        decision_date=resolved_date.isoformat(),
        candidate_strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        candidate_definition_hash=dry_run.get("candidate_definition_hash"),
        observation_written=observation_written,
        data_quality_status=data_quality_status,
        data_quality=dry_run.get("data_quality", {}),
        observations=[observation] if observation_written else [],
        blocked_observation=observation if not observation_written else {},
        blocking_reasons=blockers,
        source_statuses={"balanced_core_forward_aging_dry_run": dry_run.get("status")},
        source_artifacts=_artifact_paths_by_report(
            {"balanced_core_forward_aging_dry_run": dry_run}
        ),
        report_registry_entry=_report_registry_entry(
            "balanced_core_first_observation_write",
            "Balanced Core First Observation Write",
            "aits research strategies balanced-core-first-observation-write",
            "forward_aging_observations/balanced_core_forward_aging_observation_*",
        ),
    )
    _write_pair(payload, observation_root, artifact_id)
    return payload


def run_balanced_core_idempotency_duplicate_guard(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    decision_date: date | None = None,
) -> dict[str, Any]:
    resolved_date = decision_date or _latest_balanced_core_observation_date(output_root)
    if resolved_date is None:
        payload = _payload(
            report_type="balanced_core_idempotency_duplicate_guard",
            title="Balanced Core Idempotency Duplicate Guard",
            status="BALANCED_CORE_IDEMPOTENCY_BLOCKED",
            summary={
                "decision_date": None,
                "strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
                "duplicate_detected": False,
                "idempotency_status": "missing_first_observation",
                **_safety_summary(),
            },
            decision_date=None,
            strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            first_observation_exists=False,
            second_run_status=None,
            duplicate_detected=False,
            original_fields_preserved=False,
            definition_hash_preserved=False,
            idempotency_status="missing_first_observation",
            blocking_reasons=["first_observation_missing"],
            report_registry_entry=_report_registry_entry(
                "balanced_core_idempotency_duplicate_guard",
                "Balanced Core Idempotency Duplicate Guard",
                "aits research strategies balanced-core-idempotency-duplicate-guard",
                "balanced_core_idempotency_duplicate_guard",
            ),
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    observation_path = _balanced_core_observation_path(output_root, resolved_date)
    before_payload = _read_json_or_empty(observation_path)
    first_exists = _is_balanced_core_written_observation(before_payload)
    before_hash = _stable_hash(before_payload)
    before_core = _balanced_core_observation_core(before_payload)
    second = run_balanced_core_first_observation_write(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        decision_date=resolved_date,
    )
    after_payload = _read_json_or_empty(observation_path)
    after_hash = _stable_hash(after_payload)
    after_core = _balanced_core_observation_core(after_payload)
    duplicate_detected = second.get("status") == "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS"
    original_fields_preserved = before_hash == after_hash and before_core == after_core
    definition_hash_preserved = before_core.get("definition_hash") == after_core.get(
        "definition_hash"
    )
    blockers = []
    if not first_exists:
        blockers.append("first_observation_missing_or_invalid")
    if not duplicate_detected:
        blockers.append("second_run_did_not_return_already_exists")
    if not original_fields_preserved:
        blockers.append("original_fields_changed")
    if not definition_hash_preserved:
        blockers.append("definition_hash_changed")
    if blockers:
        status = "BALANCED_CORE_IDEMPOTENCY_BLOCKED"
        idempotency_status = "duplicate_guard_failed"
    else:
        status = "BALANCED_CORE_IDEMPOTENCY_PASS"
        idempotency_status = "duplicate_guard_passed"
    payload = _payload(
        report_type="balanced_core_idempotency_duplicate_guard",
        title="Balanced Core Idempotency Duplicate Guard",
        status=status,
        summary={
            "decision_date": resolved_date.isoformat(),
            "strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "duplicate_detected": duplicate_detected,
            "original_fields_preserved": original_fields_preserved,
            "definition_hash_preserved": definition_hash_preserved,
            **_safety_summary(),
        },
        decision_date=resolved_date.isoformat(),
        strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        first_observation_exists=first_exists,
        second_run_status=second.get("status"),
        duplicate_detected=duplicate_detected,
        original_fields_preserved=original_fields_preserved,
        definition_hash_preserved=definition_hash_preserved,
        idempotency_status=idempotency_status,
        before_core_hashes=before_core,
        after_core_hashes=after_core,
        blocking_reasons=blockers,
        report_registry_entry=_report_registry_entry(
            "balanced_core_idempotency_duplicate_guard",
            "Balanced Core Idempotency Duplicate Guard",
            "aits research strategies balanced-core-idempotency-duplicate-guard",
            "balanced_core_idempotency_duplicate_guard",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_maturity_scoreboard_safety_gate(
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
    policy = _balanced_core_forward_policy(config)
    windows = _balanced_core_windows(config)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    observations = _balanced_core_written_observations(output_root)
    matured_counts = {window: 0 for window in windows}
    blockers: list[str] = []
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    else:
        prices = _price_matrix(
            prices_path,
            config,
            start_date=start_date,
            end_date=end_date,
        )
        for observation in observations:
            decision = _safe_date(observation.get("decision_date"))
            if decision is None:
                continue
            for window in windows:
                if _window_is_matured(prices, decision, window):
                    matured_counts[window] += 1
    min_20 = _int(policy.get("minimum_required_20d"))
    min_60 = _int(policy.get("minimum_required_60d"))
    min_120 = _int(policy.get("minimum_required_120d"))
    blocked_conclusions = _balanced_core_blocked_conclusions(
        matured_counts,
        minimum_required_20d=min_20,
        minimum_required_60d=min_60,
        minimum_required_120d=min_120,
    )
    if blockers:
        status = "BALANCED_CORE_SCOREBOARD_BLOCKED"
        scoreboard_status = "BLOCKED"
    elif blocked_conclusions:
        status = "BALANCED_CORE_SCOREBOARD_INSUFFICIENT"
        scoreboard_status = "INSUFFICIENT"
    else:
        status = "BALANCED_CORE_SCOREBOARD_SAFETY_PASS"
        scoreboard_status = "PENDING"
    payload = _payload(
        report_type="balanced_core_maturity_scoreboard_safety_gate",
        title="Balanced Core Maturity Scoreboard Safety Gate",
        status=status,
        summary={
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "observation_count": len(observations),
            "scoreboard_status": scoreboard_status,
            "blocked_conclusion_count": len(blocked_conclusions),
            **_safety_summary(),
        },
        candidate_strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        matured_5d_count=matured_counts.get(5, 0),
        matured_10d_count=matured_counts.get(10, 0),
        matured_20d_count=matured_counts.get(20, 0),
        matured_60d_count=matured_counts.get(60, 0),
        matured_120d_count=matured_counts.get(120, 0),
        minimum_required_20d=min_20,
        minimum_required_60d=min_60,
        minimum_required_120d=min_120,
        scoreboard_status=scoreboard_status,
        blocked_conclusions=blocked_conclusions,
        data_quality_status=data_gate.get("status"),
        data_quality=data_gate,
        observation_files=[str(item.get("_path")) for item in observations],
        blocking_reasons=blockers,
        report_registry_entry=_report_registry_entry(
            "balanced_core_maturity_scoreboard_safety_gate",
            "Balanced Core Maturity Scoreboard Safety Gate",
            "aits research strategies balanced-core-maturity-scoreboard-safety-gate",
            "balanced_core_maturity_scoreboard_safety_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dual_forward_aging_comparator_panel(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    windows = _balanced_core_windows(config)
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    observations = _balanced_core_written_observations(growth_output_root)
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    elif observations:
        prices = _price_matrix(
            prices_path,
            config,
            start_date=start_date,
            end_date=end_date,
        )
        definition_lock = _read_json_or_empty(
            growth_output_root / "balanced_core_definition_lock.json"
        )
        candidate_map = _dual_panel_strategy_maps(definition_lock, config)
        for observation in observations:
            decision = _safe_date(observation.get("decision_date"))
            if decision is None:
                continue
            decision_rows = [
                _dual_panel_row(
                    strategy_id=strategy_id,
                    strategy_role=role,
                    candidate=candidate,
                    prices=prices,
                    config=config,
                    decision_date=decision,
                    windows=windows,
                    data_quality_status=str(data_gate.get("status")),
                    observation=observation,
                )
                for strategy_id, role, candidate in candidate_map
            ]
            _attach_relative_fields(decision_rows, windows)
            rows.extend(decision_rows)
    any_matured = any(
        row.get(f"matured_{window}d_return") is not None
        for row in rows
        for window in windows
    )
    if blockers:
        status = "DUAL_FORWARD_PANEL_BLOCKED"
    elif not observations:
        status = "DUAL_FORWARD_PANEL_INSUFFICIENT"
    elif any_matured:
        status = "DUAL_FORWARD_PANEL_READY"
    else:
        status = "DUAL_FORWARD_PANEL_PENDING"
    payload = _payload(
        report_type="dual_forward_aging_comparator_panel",
        title="Dual Forward-Aging Comparator Panel",
        status=status,
        summary={
            "observation_count": len(observations),
            "panel_row_count": len(rows),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        panel_rows=rows,
        data_quality_status=data_gate.get("status"),
        data_quality=data_gate,
        observation_files=[str(item.get("_path")) for item in observations],
        blocking_reasons=blockers,
        report_registry_entry=_report_registry_entry(
            "dual_forward_aging_comparator_panel",
            "Dual Forward-Aging Comparator Panel",
            "aits research strategies dual-forward-aging-comparator-panel",
            "dual_forward_aging_comparator_panel",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dual_forward_aging_reader_brief_safe_preview(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _maturity_payload: Mapping[str, Any] | None = None,
    _panel_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    maturity = dict(
        _maturity_payload
        or run_balanced_core_maturity_scoreboard_safety_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    panel = dict(
        _panel_payload
        or run_dual_forward_aging_comparator_panel(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    latest_observation_date = _latest_balanced_core_observation_date(growth_output_root)
    preview = {
        "display_scope": "forward-aging research-only",
        "defensive_primary": DEFENSIVE_PRIMARY_ID,
        "balanced_core_candidate": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        "latest_observation_date": latest_observation_date.isoformat()
        if latest_observation_date
        else None,
        "matured_5d_count": maturity.get("matured_5d_count", 0),
        "matured_20d_count": maturity.get("matured_20d_count", 0),
        "scoreboard_status": maturity.get("scoreboard_status"),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    prohibited_hits = _prohibited_reader_brief_hits(preview)
    blockers = []
    if panel.get("status") == "DUAL_FORWARD_PANEL_BLOCKED":
        blockers.append("dual_forward_panel_blocked")
    if maturity.get("status") == "BALANCED_CORE_SCOREBOARD_BLOCKED":
        blockers.append("scoreboard_safety_gate_blocked")
    if prohibited_hits:
        blockers.append("reader_brief_prohibited_phrase_hit")
    if blockers:
        status = "DUAL_READER_BRIEF_BLOCKED"
    elif prohibited_hits:
        status = "DUAL_READER_BRIEF_AMBIGUOUS"
    else:
        status = "DUAL_READER_BRIEF_SAFE"
    payload = _payload(
        report_type="dual_forward_aging_reader_brief_safe_preview",
        title="Dual Forward-Aging Reader Brief Safe Preview",
        status=status,
        summary={
            "latest_observation_date": preview["latest_observation_date"],
            "scoreboard_status": preview["scoreboard_status"],
            "prohibited_phrase_hit_count": len(prohibited_hits),
            **_safety_summary(),
        },
        reader_brief_preview=preview,
        prohibited_phrase_hits=prohibited_hits,
        blocking_reasons=blockers,
        source_statuses={
            "balanced_core_maturity_scoreboard_safety_gate": maturity.get("status"),
            "dual_forward_aging_comparator_panel": panel.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "balanced_core_maturity_scoreboard_safety_gate": maturity,
                "dual_forward_aging_comparator_panel": panel,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "dual_forward_aging_reader_brief_safe_preview",
            "Dual Forward-Aging Reader Brief Safe Preview",
            "aits research strategies dual-forward-aging-reader-brief-safe-preview",
            "dual_forward_aging_reader_brief_safe_preview",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_owner_launch_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_BALANCED_CORE_OWNER_LAUNCH_PACK_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _activation_payload: Mapping[str, Any] | None = None,
    _definition_lock_payload: Mapping[str, Any] | None = None,
    _observation_payload: Mapping[str, Any] | None = None,
    _idempotency_payload: Mapping[str, Any] | None = None,
    _maturity_payload: Mapping[str, Any] | None = None,
    _panel_payload: Mapping[str, Any] | None = None,
    _reader_preview_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    activation = dict(
        _activation_payload
        or run_balanced_core_watchlist_activation_contract(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    definition_lock = dict(
        _definition_lock_payload
        or run_balanced_core_definition_lock(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    observation = dict(
        _observation_payload
        or run_balanced_core_first_observation_write(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _activation_payload=activation,
            _definition_lock_payload=definition_lock,
        )
    )
    idempotency = dict(
        _idempotency_payload
        or run_balanced_core_idempotency_duplicate_guard(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    maturity = dict(
        _maturity_payload
        or run_balanced_core_maturity_scoreboard_safety_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    panel = dict(
        _panel_payload
        or run_dual_forward_aging_comparator_panel(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    reader_preview = dict(
        _reader_preview_payload
        or run_dual_forward_aging_reader_brief_safe_preview(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _maturity_payload=maturity,
            _panel_payload=panel,
        )
    )
    source_payloads = {
        "balanced_core_watchlist_activation_contract": activation,
        "balanced_core_definition_lock": definition_lock,
        "balanced_core_first_observation_write": observation,
        "balanced_core_idempotency_duplicate_guard": idempotency,
        "balanced_core_maturity_scoreboard_safety_gate": maturity,
        "dual_forward_aging_comparator_panel": panel,
        "dual_forward_aging_reader_brief_safe_preview": reader_preview,
    }
    answers = _balanced_core_owner_launch_answers(source_payloads)
    blockers = _balanced_core_launch_blockers(source_payloads)
    if blockers:
        recommendation = (
            "BALANCED_CORE_LAUNCH_BLOCKED"
            if any("blocked" in item for item in blockers)
            else "BALANCED_CORE_LAUNCH_NEEDS_REVIEW"
        )
    else:
        recommendation = "BALANCED_CORE_FORWARD_AGING_LAUNCHED"
    payload = _payload(
        report_type="balanced_core_owner_launch_pack",
        title="Balanced Core Owner Launch Pack",
        status="BALANCED_CORE_OWNER_LAUNCH_PACK_READY"
        if recommendation == "BALANCED_CORE_FORWARD_AGING_LAUNCHED"
        else "BALANCED_CORE_OWNER_LAUNCH_PACK_NEEDS_REVIEW",
        summary={
            "owner_recommendation": recommendation,
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "latest_observation_date": _summary_value(observation, "decision_date"),
            "scoreboard_status": maturity.get("scoreboard_status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        blocking_reasons=blockers,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        report_registry_entry=_roadmap_doc_report_registry_entry(
            "balanced_core_owner_launch_pack",
            "Balanced Core Owner Launch Pack",
            "aits research strategies balanced-core-owner-launch-pack",
            "balanced_core_owner_launch_pack",
            "docs/research/balanced_core_owner_launch_pack.md",
        ),
    )
    _write_json_and_owner_doc(
        payload,
        output_root / "balanced_core_owner_launch_pack.json",
        docs_path,
        "Balanced Core Owner Launch Pack",
    )
    return payload


def run_dual_forward_aging_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_DUAL_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_BALANCED_CORE_OWNER_LAUNCH_PACK_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _owner_launch_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    owner_launch = dict(
        _owner_launch_payload
        or run_balanced_core_owner_launch_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=owner_docs_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    answers = _dual_forward_master_answers(owner_launch)
    recommendation = owner_launch.get("owner_recommendation")
    if recommendation == "BALANCED_CORE_FORWARD_AGING_LAUNCHED":
        final_status = "DUAL_FORWARD_AGING_ACTIVE_RESEARCH_ONLY"
    elif recommendation == "BALANCED_CORE_LAUNCH_BLOCKED":
        final_status = "DUAL_FORWARD_AGING_BLOCKED"
    elif _summary_value(owner_launch, "latest_observation_date"):
        final_status = "DUAL_FORWARD_AGING_NEEDS_OWNER_REVIEW"
    else:
        final_status = "BALANCED_CORE_DRY_RUN_ONLY"
    payload = _payload(
        report_type="dual_forward_aging_master_review",
        title="Dual Forward-Aging Master Review",
        status=final_status,
        summary={
            "final_status": final_status,
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "owner_recommendation": recommendation,
            **_safety_summary(),
        },
        final_status=final_status,
        required_answers=answers,
        next_minimum_task="wait_for_forward_aging_maturity_then_update_scoreboard",
        final_conclusions=[
            final_status,
            "KEEP_EQUAL_RISK_DEFENSIVE_PRIMARY",
            "KEEP_100_QQQ_HARD_BENCHMARK",
            "NO_PAPER_SHADOW_NO_PRODUCTION_NO_BROKER",
        ],
        source_statuses={"balanced_core_owner_launch_pack": owner_launch.get("status")},
        source_artifacts=_artifact_paths_by_report(
            {"balanced_core_owner_launch_pack": owner_launch}
        ),
        report_registry_entry=_roadmap_doc_report_registry_entry(
            "dual_forward_aging_master_review",
            "Dual Forward-Aging Master Review",
            "aits research strategies dual-forward-aging-master-review",
            "dual_forward_aging_master_review",
            "docs/research/dual_forward_aging_master_review.md",
        ),
    )
    _write_json_and_owner_doc(
        payload,
        output_root / "dual_forward_aging_master_review.json",
        docs_path,
        "Dual Forward-Aging Master Review",
    )
    return payload


def run_balanced_core_launch_preflight(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    external_validation_output_root: Path | None = None,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    decision_date: date | None = None,
    _launch_gate_payload: Mapping[str, Any] | None = None,
    _definition_lock_payload: Mapping[str, Any] | None = None,
    _dry_run_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    launch_gate = dict(
        _launch_gate_payload
        or _run_external_validation_to_launch_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=config_path,
            output_root=external_validation_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    definition_lock = dict(
        _definition_lock_payload
        or run_balanced_core_definition_lock(
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
    data_gate = _data_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    blockers: list[str] = []
    warnings: list[str] = []
    if launch_gate.get("launch_allowed") is not True:
        blockers.append("external_validation_launch_gate_not_allowed")
    if launch_gate.get("status") == "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN":
        warnings.append("external_validation_launch_gate_passed_with_warnings")
    if definition_lock.get("strategy_id") != FOCUSED_GROWTH_TILT_CANDIDATE_ID:
        blockers.append("candidate_strategy_id_not_expected_balanced_core")
    if definition_lock.get("status") != "BALANCED_CORE_DEFINITION_LOCKED":
        blockers.append("definition_lock_not_locked")
    if not str(definition_lock.get("definition_hash") or ""):
        blockers.append("definition_hash_missing")
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    dry_run: dict[str, Any] = {}
    if not blockers:
        dry_run = dict(
            _dry_run_payload
            or run_balanced_core_forward_aging_dry_run(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
                decision_date=decision_date,
                _definition_lock_payload=definition_lock,
            )
        )
        if dry_run.get("status") not in {
            "BALANCED_CORE_FORWARD_DRY_RUN_PASS",
            "BALANCED_CORE_FORWARD_DRY_RUN_WARN",
        }:
            blockers.append("balanced_core_dry_run_not_passed")
        if dry_run.get("status") == "BALANCED_CORE_FORWARD_DRY_RUN_WARN":
            warnings.append("balanced_core_dry_run_passed_with_warnings")
        if not _mapping(dry_run.get("comparator_equal_risk_weights")):
            blockers.append("equal_risk_comparator_weights_missing")
        if not _mapping(dry_run.get("comparator_100_qqq_weights")):
            blockers.append("qqq_comparator_weights_missing")
    resolved_date = _safe_date(dry_run.get("decision_date")) or decision_date
    same_day_exists = (
        _is_balanced_core_written_observation(
            _read_json_or_empty(_balanced_core_observation_path(output_root, resolved_date))
        )
        if resolved_date is not None
        else False
    )
    if same_day_exists:
        warnings.append("same_day_observation_already_exists_no_rewrite")
    if _int(data_gate.get("warning_count")) > 0:
        warnings.append("data_quality_passed_with_warnings")
    blockers = _dedupe_text([*blockers, *_safety_violations([launch_gate, definition_lock])])
    warnings = _dedupe_text(warnings)
    if blockers:
        status = "BALANCED_CORE_LAUNCH_PREFLIGHT_BLOCKED"
    elif warnings:
        status = "BALANCED_CORE_LAUNCH_PREFLIGHT_WARN"
    else:
        status = "BALANCED_CORE_LAUNCH_PREFLIGHT_PASS"
    payload = _payload(
        report_type="balanced_core_launch_preflight",
        title="Balanced Core Launch Preflight",
        status=status,
        summary={
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "candidate_role": "balanced_core_candidate",
            "decision_date": resolved_date.isoformat() if resolved_date else None,
            "definition_hash": definition_lock.get("definition_hash"),
            "launch_gate_status": launch_gate.get("status"),
            "same_day_observation_exists": same_day_exists,
            **_safety_summary(),
        },
        candidate_strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        candidate_role="balanced_core_candidate",
        definition_hash=definition_lock.get("definition_hash"),
        launch_gate_status=launch_gate.get("status"),
        launch_allowed=launch_gate.get("launch_allowed") is True,
        decision_date=resolved_date.isoformat() if resolved_date else None,
        same_day_observation_exists=same_day_exists,
        comparator_equal_risk_weights=dry_run.get("comparator_equal_risk_weights", {}),
        comparator_100_qqq_weights=dry_run.get("comparator_100_qqq_weights", {}),
        dry_run_payload=dry_run,
        data_quality_status=data_gate.get("status"),
        data_quality=data_gate,
        blocking_reasons=blockers,
        warning_reasons=warnings,
        source_statuses={
            "external_validation_to_launch_gate": launch_gate.get("status"),
            "balanced_core_definition_lock": definition_lock.get("status"),
            "balanced_core_forward_aging_dry_run": dry_run.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "external_validation_to_launch_gate": launch_gate,
                "balanced_core_definition_lock": definition_lock,
                "balanced_core_forward_aging_dry_run": dry_run,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "balanced_core_launch_preflight",
            "Balanced Core Launch Preflight",
            "aits research strategies balanced-core-launch-preflight",
            "balanced_core_launch_preflight",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_first_observation_write_after_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    external_validation_output_root: Path | None = None,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    decision_date: date | None = None,
    _launch_gate_payload: Mapping[str, Any] | None = None,
    _preflight_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    launch_gate = dict(
        _launch_gate_payload
        or _run_external_validation_to_launch_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=config_path,
            output_root=external_validation_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    preflight = dict(
        _preflight_payload
        or run_balanced_core_launch_preflight(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            decision_date=decision_date,
            _launch_gate_payload=launch_gate,
        )
    )
    blockers = []
    if launch_gate.get("status") not in {
        "EXTERNAL_VALIDATION_LAUNCH_GATE_PASS",
        "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN",
    }:
        blockers.append("external_validation_launch_gate_not_passed")
    if preflight.get("status") not in {
        "BALANCED_CORE_LAUNCH_PREFLIGHT_PASS",
        "BALANCED_CORE_LAUNCH_PREFLIGHT_WARN",
    }:
        blockers.append("balanced_core_launch_preflight_not_passed")
    blockers.extend(_records_to_text(preflight.get("blocking_reasons")))
    blockers = _dedupe_text(blockers)
    if blockers:
        payload = _payload(
            report_type="balanced_core_first_observation_write_after_validation",
            title="Balanced Core First Observation Write After Validation",
            status="BALANCED_CORE_FIRST_OBSERVATION_BLOCKED",
            summary={
                "observation_written": False,
                "external_validation_status": launch_gate.get("status"),
                "preflight_status": preflight.get("status"),
                **_safety_summary(),
            },
            observation_written=False,
            external_validation_status=launch_gate.get("status"),
            preflight_status=preflight.get("status"),
            blocking_reasons=blockers,
            source_statuses={
                "external_validation_to_launch_gate": launch_gate.get("status"),
                "balanced_core_launch_preflight": preflight.get("status"),
            },
            source_artifacts=_artifact_paths_by_report(
                {
                    "external_validation_to_launch_gate": launch_gate,
                    "balanced_core_launch_preflight": preflight,
                }
            ),
            report_registry_entry=_report_registry_entry(
                "balanced_core_first_observation_write_after_validation",
                "Balanced Core First Observation Write After Validation",
                "aits research strategies balanced-core-first-observation-write-after-validation",
                "balanced_core_first_observation_write_after_validation",
                extra_artifact_globs=[
                    "outputs/research_strategies/growth_components/"
                    "forward_aging_observations/"
                    "balanced_core_forward_aging_observation_*.json",
                    "outputs/research_strategies/growth_components/"
                    "forward_aging_observations/"
                    "balanced_core_forward_aging_observation_*.md",
                ],
            ),
        )
        _write_pair(payload, output_root, payload["report_type"])
        return payload
    observation = run_balanced_core_first_observation_write(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        decision_date=decision_date or _safe_date(preflight.get("decision_date")),
        _dry_run_payload=_mapping(preflight.get("dry_run_payload")) or None,
    )
    enriched = dict(observation)
    enriched["external_validation_status"] = launch_gate.get("status")
    enriched["external_validation_master_status"] = _mapping(
        launch_gate.get("source_statuses")
    ).get("external_validation_real_result_status_reader")
    enriched["launch_gate_status"] = launch_gate.get("status")
    enriched["preflight_status"] = preflight.get("status")
    enriched["data_quality_status"] = (
        observation.get("data_quality_status") or preflight.get("data_quality_status")
    )
    enriched["report_registry_entry"] = _report_registry_entry(
        "balanced_core_first_observation_write_after_validation",
        "Balanced Core First Observation Write After Validation",
        "aits research strategies balanced-core-first-observation-write-after-validation",
        "forward_aging_observations/balanced_core_forward_aging_observation_*",
    )
    if observation.get("status") == "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN":
        resolved_date = _safe_date(enriched.get("decision_date"))
        if resolved_date is not None:
            artifact_id = f"balanced_core_forward_aging_observation_{resolved_date.isoformat()}"
            _write_pair(enriched, output_root / "forward_aging_observations", artifact_id)
    return enriched


def run_balanced_core_observation_idempotency_proof(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    decision_date: date | None = None,
    _idempotency_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    idempotency = dict(
        _idempotency_payload
        or run_balanced_core_idempotency_duplicate_guard(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            decision_date=decision_date,
        )
    )
    if idempotency.get("status") == "BALANCED_CORE_IDEMPOTENCY_PASS":
        status = "BALANCED_CORE_OBSERVATION_IDEMPOTENCY_PASS"
    elif idempotency.get("status") == "BALANCED_CORE_IDEMPOTENCY_BLOCKED":
        status = "BALANCED_CORE_OBSERVATION_IDEMPOTENCY_BLOCKED"
    else:
        status = "BALANCED_CORE_OBSERVATION_IDEMPOTENCY_WARN"
    payload = _payload(
        report_type="balanced_core_observation_idempotency_proof",
        title="Balanced Core Observation Idempotency Proof",
        status=status,
        summary={
            "decision_date": idempotency.get("decision_date"),
            "second_run_status": idempotency.get("second_run_status"),
            "original_fields_preserved": idempotency.get("original_fields_preserved"),
            "definition_hash_preserved": idempotency.get("definition_hash_preserved"),
            **_safety_summary(),
        },
        decision_date=idempotency.get("decision_date"),
        second_run_status=idempotency.get("second_run_status"),
        duplicate_detected=idempotency.get("duplicate_detected"),
        original_target_weights_preserved=idempotency.get("original_fields_preserved"),
        definition_hash_preserved=idempotency.get("definition_hash_preserved"),
        comparator_weights_preserved=idempotency.get("original_fields_preserved"),
        external_validation_status_preserved=True,
        duplicate_observation_created=False
        if idempotency.get("duplicate_detected") is True
        else None,
        blocking_reasons=_records_to_text(idempotency.get("blocking_reasons")),
        source_statuses={
            "balanced_core_idempotency_duplicate_guard": idempotency.get("status")
        },
        source_artifacts=_artifact_paths_by_report(
            {"balanced_core_idempotency_duplicate_guard": idempotency}
        ),
        report_registry_entry=_report_registry_entry(
            "balanced_core_observation_idempotency_proof",
            "Balanced Core Observation Idempotency Proof",
            "aits research strategies balanced-core-observation-idempotency-proof",
            "balanced_core_observation_idempotency_proof",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dual_forward_aging_comparator_panel_after_launch(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _panel_payload: Mapping[str, Any] | None = None,
    _maturity_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    panel = dict(
        _panel_payload
        or run_dual_forward_aging_comparator_panel(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    maturity = dict(
        _maturity_payload
        or run_balanced_core_maturity_scoreboard_safety_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    old_status = str(panel.get("status"))
    if old_status == "DUAL_FORWARD_PANEL_BLOCKED":
        status = "DUAL_FORWARD_PANEL_AFTER_LAUNCH_BLOCKED"
    elif old_status == "DUAL_FORWARD_PANEL_READY":
        status = "DUAL_FORWARD_PANEL_AFTER_LAUNCH_READY"
    else:
        status = "DUAL_FORWARD_PANEL_AFTER_LAUNCH_PENDING"
    rows = []
    for row in _records(panel.get("panel_rows")):
        normalized = dict(row)
        normalized["scoreboard_status"] = maturity.get("scoreboard_status")
        normalized.setdefault("data_quality_status", panel.get("data_quality_status"))
        rows.append(normalized)
    payload = _payload(
        report_type="dual_forward_panel_after_launch",
        title="Dual Forward-Aging Comparator Panel After Launch",
        status=status,
        summary={
            "panel_row_count": len(rows),
            "scoreboard_status": maturity.get("scoreboard_status"),
            "data_quality_status": panel.get("data_quality_status"),
            **_safety_summary(),
        },
        panel_rows=rows,
        scoreboard_status=maturity.get("scoreboard_status"),
        data_quality_status=panel.get("data_quality_status"),
        blocking_reasons=_records_to_text(panel.get("blocking_reasons")),
        source_statuses={
            "dual_forward_aging_comparator_panel": panel.get("status"),
            "balanced_core_maturity_scoreboard_safety_gate": maturity.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "dual_forward_aging_comparator_panel": panel,
                "balanced_core_maturity_scoreboard_safety_gate": maturity,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "dual_forward_aging_comparator_panel_after_launch",
            "Dual Forward-Aging Comparator Panel After Launch",
            "aits research strategies dual-forward-aging-comparator-panel-after-launch",
            "dual_forward_panel_after_launch",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dual_forward_aging_scoreboard_safety_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _maturity_payload: Mapping[str, Any] | None = None,
    _panel_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    maturity = dict(
        _maturity_payload
        or run_balanced_core_maturity_scoreboard_safety_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    panel = dict(
        _panel_payload
        or run_dual_forward_aging_comparator_panel(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    role_counts = _dual_panel_matured_counts_by_role(_records(panel.get("panel_rows")))
    blockers = []
    if maturity.get("status") == "BALANCED_CORE_SCOREBOARD_BLOCKED":
        blockers.append("balanced_core_scoreboard_blocked")
    if panel.get("status") == "DUAL_FORWARD_PANEL_BLOCKED":
        blockers.append("dual_forward_panel_blocked")
    insufficient = maturity.get("scoreboard_status") in {"INSUFFICIENT", "PENDING"}
    if blockers:
        status = "DUAL_SCOREBOARD_SAFETY_BLOCKED"
    elif insufficient:
        status = "DUAL_SCOREBOARD_INSUFFICIENT_SAMPLE"
    else:
        status = "DUAL_SCOREBOARD_SAFETY_PASS"
    payload = _payload(
        report_type="dual_forward_aging_scoreboard_safety_review",
        title="Dual Forward-Aging Scoreboard Safety Review",
        status=status,
        summary={
            "scoreboard_status": maturity.get("scoreboard_status"),
            "balanced_core_matured_20d_count": maturity.get("matured_20d_count"),
            "paper_shadow_readiness_displayed": False,
            "production_readiness_displayed": False,
            **_safety_summary(),
        },
        balanced_core_matured_counts={
            "matured_5d_count": maturity.get("matured_5d_count", 0),
            "matured_10d_count": maturity.get("matured_10d_count", 0),
            "matured_20d_count": maturity.get("matured_20d_count", 0),
            "matured_60d_count": maturity.get("matured_60d_count", 0),
            "matured_120d_count": maturity.get("matured_120d_count", 0),
        },
        comparator_matured_counts=role_counts,
        scoreboard_status=maturity.get("scoreboard_status"),
        sample_discipline_ok=insufficient or status == "DUAL_SCOREBOARD_SAFETY_PASS",
        paper_shadow_readiness_displayed=False,
        production_readiness_displayed=False,
        blocking_reasons=blockers,
        source_statuses={
            "balanced_core_maturity_scoreboard_safety_gate": maturity.get("status"),
            "dual_forward_aging_comparator_panel": panel.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "balanced_core_maturity_scoreboard_safety_gate": maturity,
                "dual_forward_aging_comparator_panel": panel,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "dual_forward_aging_scoreboard_safety_review",
            "Dual Forward-Aging Scoreboard Safety Review",
            "aits research strategies dual-forward-aging-scoreboard-safety-review",
            "dual_forward_aging_scoreboard_safety_review",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dual_forward_aging_reader_brief_safe_preview_after_launch(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    external_validation_output_root: Path | None = None,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _launch_gate_payload: Mapping[str, Any] | None = None,
    _scoreboard_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    launch_gate = dict(
        _launch_gate_payload
        or _run_external_validation_to_launch_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=config_path,
            output_root=external_validation_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    scoreboard = dict(
        _scoreboard_payload
        or run_dual_forward_aging_scoreboard_safety_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    latest_observation_date = _latest_balanced_core_observation_date(growth_output_root)
    preview = {
        "display_scope": "forward-aging research-only",
        "defensive_primary": DEFENSIVE_PRIMARY_ID,
        "balanced_core_candidate": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        "latest_observation_date": latest_observation_date.isoformat()
        if latest_observation_date
        else None,
        "matured_counts": scoreboard.get("balanced_core_matured_counts", {}),
        "scoreboard_status": scoreboard.get("scoreboard_status"),
        "external_validation_status": launch_gate.get("status"),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    hits = _prohibited_reader_brief_hits(preview)
    blockers = []
    if scoreboard.get("status") == "DUAL_SCOREBOARD_SAFETY_BLOCKED":
        blockers.append("scoreboard_safety_blocked")
    if launch_gate.get("status") == "EXTERNAL_VALIDATION_LAUNCH_GATE_BLOCKED":
        blockers.append("external_validation_launch_gate_blocked")
    if hits:
        blockers.append("reader_brief_prohibited_phrase_hit")
    if blockers:
        status = "DUAL_READER_BRIEF_AFTER_LAUNCH_BLOCKED"
    elif hits:
        status = "DUAL_READER_BRIEF_AFTER_LAUNCH_AMBIGUOUS"
    else:
        status = "DUAL_READER_BRIEF_AFTER_LAUNCH_SAFE"
    payload = _payload(
        report_type="dual_forward_aging_reader_brief_after_launch_safe_preview",
        title="Dual Forward-Aging Reader Brief Safe Preview After Launch",
        status=status,
        summary={
            "latest_observation_date": preview["latest_observation_date"],
            "scoreboard_status": preview["scoreboard_status"],
            "external_validation_status": launch_gate.get("status"),
            "prohibited_phrase_hit_count": len(hits),
            **_safety_summary(),
        },
        reader_brief_preview=preview,
        prohibited_phrase_hits=hits,
        blocking_reasons=blockers,
        source_statuses={
            "external_validation_to_launch_gate": launch_gate.get("status"),
            "dual_forward_aging_scoreboard_safety_review": scoreboard.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "external_validation_to_launch_gate": launch_gate,
                "dual_forward_aging_scoreboard_safety_review": scoreboard,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "dual_forward_aging_reader_brief_safe_preview_after_launch",
            "Dual Forward-Aging Reader Brief Safe Preview After Launch",
            "aits research strategies dual-forward-aging-reader-brief-safe-preview-after-launch",
            "dual_forward_aging_reader_brief_after_launch_safe_preview",
            output_subdir="roadmap",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_balanced_core_launch_owner_report(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    external_validation_output_root: Path | None = None,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_BALANCED_CORE_LAUNCH_OWNER_REPORT_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _launch_gate_payload: Mapping[str, Any] | None = None,
    _preflight_payload: Mapping[str, Any] | None = None,
    _observation_payload: Mapping[str, Any] | None = None,
    _idempotency_payload: Mapping[str, Any] | None = None,
    _panel_payload: Mapping[str, Any] | None = None,
    _scoreboard_payload: Mapping[str, Any] | None = None,
    _reader_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    launch_gate = dict(
        _launch_gate_payload
        or _run_external_validation_to_launch_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=config_path,
            output_root=external_validation_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    preflight = dict(
        _preflight_payload
        or run_balanced_core_launch_preflight(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _launch_gate_payload=launch_gate,
        )
    )
    observation = dict(
        _observation_payload
        or run_balanced_core_first_observation_write_after_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _launch_gate_payload=launch_gate,
            _preflight_payload=preflight,
        )
    )
    idempotency = dict(
        _idempotency_payload
        or run_balanced_core_observation_idempotency_proof(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=growth_output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    panel = dict(
        _panel_payload
        or run_dual_forward_aging_comparator_panel_after_launch(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    scoreboard = dict(
        _scoreboard_payload
        or run_dual_forward_aging_scoreboard_safety_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    reader = dict(
        _reader_payload
        or run_dual_forward_aging_reader_brief_safe_preview_after_launch(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _launch_gate_payload=launch_gate,
            _scoreboard_payload=scoreboard,
        )
    )
    source_payloads = {
        "external_validation_to_launch_gate": launch_gate,
        "balanced_core_launch_preflight": preflight,
        "balanced_core_first_observation_write_after_validation": observation,
        "balanced_core_observation_idempotency_proof": idempotency,
        "dual_forward_panel_after_launch": panel,
        "dual_forward_aging_scoreboard_safety_review": scoreboard,
        "dual_forward_aging_reader_brief_after_launch_safe_preview": reader,
    }
    answers = _balanced_core_launch_owner_answers(source_payloads)
    blockers = _balanced_core_after_validation_blockers(source_payloads)
    warnings = []
    if launch_gate.get("status") == "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN":
        warnings.append("external_validation_launch_gate_warn")
    if preflight.get("status") == "BALANCED_CORE_LAUNCH_PREFLIGHT_WARN":
        warnings.append("balanced_core_launch_preflight_warn")
    if blockers:
        recommendation = "BALANCED_CORE_LAUNCH_BLOCKED"
    elif warnings:
        recommendation = "BALANCED_CORE_LAUNCH_WARN"
    else:
        recommendation = "BALANCED_CORE_FORWARD_AGING_LAUNCHED"
    payload = _payload(
        report_type="balanced_core_launch_owner_report",
        title="Balanced Core Launch Owner Report",
        status="BALANCED_CORE_LAUNCH_OWNER_REPORT_READY"
        if recommendation != "BALANCED_CORE_LAUNCH_BLOCKED"
        else "BALANCED_CORE_LAUNCH_OWNER_REPORT_BLOCKED",
        summary={
            "owner_recommendation": recommendation,
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            "external_validation_status": launch_gate.get("status"),
            "observation_status": observation.get("status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        blocking_reasons=blockers,
        warning_reasons=_dedupe_text(warnings),
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_artifact_paths_by_report(source_payloads),
        report_registry_entry=_roadmap_doc_report_registry_entry(
            "balanced_core_launch_owner_report",
            "Balanced Core Launch Owner Report",
            "aits research strategies balanced-core-launch-owner-report",
            "balanced_core_launch_owner_report",
            "docs/research/balanced_core_launch_owner_report.md",
        ),
    )
    _write_json_and_owner_doc(
        payload,
        output_root / "balanced_core_launch_owner_report.json",
        docs_path,
        "Balanced Core Launch Owner Report",
    )
    return payload


def run_external_validation_balanced_core_launch_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    external_validation_output_root: Path | None = None,
    growth_output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_BALANCED_CORE_LAUNCH_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_BALANCED_CORE_LAUNCH_OWNER_REPORT_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _owner_report_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    owner = dict(
        _owner_report_payload
        or run_balanced_core_launch_owner_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=owner_docs_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    recommendation = owner.get("owner_recommendation")
    if recommendation == "BALANCED_CORE_FORWARD_AGING_LAUNCHED":
        final_status = "EXTERNAL_VALIDATION_AND_BALANCED_CORE_LAUNCH_PASS"
    elif recommendation == "BALANCED_CORE_LAUNCH_WARN":
        final_status = "EXTERNAL_VALIDATION_AND_BALANCED_CORE_LAUNCH_WARN"
    elif _mapping(owner.get("source_statuses")).get(
        "external_validation_to_launch_gate"
    ) == "EXTERNAL_VALIDATION_LAUNCH_GATE_BLOCKED":
        final_status = "BALANCED_CORE_LAUNCH_BLOCKED_BY_EXTERNAL_VALIDATION"
    else:
        final_status = "BALANCED_CORE_LAUNCH_BLOCKED"
    answers = _external_validation_balanced_core_master_answers(owner)
    payload = _payload(
        report_type="external_validation_balanced_core_launch_master_review",
        title="External Validation Balanced Core Launch Master Review",
        status=final_status,
        summary={
            "final_status": final_status,
            "owner_recommendation": recommendation,
            "candidate_strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
            **_safety_summary(),
        },
        final_status=final_status,
        required_answers=answers,
        next_minimum_task="run_dual_forward_aging_monthly_monitor_contract_then_wait_for_maturity",
        final_conclusions=[
            final_status,
            "KEEP_RESEARCH_ONLY",
            "NO_PAPER_SHADOW_NO_PRODUCTION_NO_BROKER",
        ],
        source_statuses={"balanced_core_launch_owner_report": owner.get("status")},
        source_artifacts=_artifact_paths_by_report({"balanced_core_launch_owner_report": owner}),
        report_registry_entry=_roadmap_doc_report_registry_entry(
            "external_validation_balanced_core_launch_master_review",
            "External Validation Balanced Core Launch Master Review",
            "aits research strategies external-validation-balanced-core-launch-master-review",
            "external_validation_balanced_core_launch_master_review",
            "docs/research/external_validation_balanced_core_launch_master_review.md",
        ),
    )
    _write_json_and_owner_doc(
        payload,
        output_root / "external_validation_balanced_core_launch_master_review.json",
        docs_path,
        "External Validation Balanced Core Launch Master Review",
    )
    return payload


def run_dual_forward_aging_monthly_monitor_contract(
    *,
    output_root: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_DUAL_FORWARD_AGING_MONTHLY_MONITOR_CONTRACT_DOC_PATH,
) -> dict[str, Any]:
    monitor_rules = [
        "do_not_change_strategy_status_from_single_5d_or_10d_window",
        "no_medium_term_conclusion_when_20d_sample_is_insufficient",
        "no_paper_shadow_review_when_120d_sample_is_insufficient",
        "monthly_owner_review_only_updates_research_status",
    ]
    payload = _payload(
        report_type="dual_forward_aging_monthly_monitor_contract",
        title="Dual Forward-Aging Monthly Monitor Contract",
        status="DUAL_FORWARD_MONTHLY_MONITOR_READY",
        summary={
            "monitor_rule_count": len(monitor_rules),
            "production_effect": "none",
            **_safety_summary(),
        },
        monthly_monitor_content=[
            "equal_risk_observation_count",
            "balanced_core_observation_count",
            "matured_5d_10d_20d_60d_120d_count",
            "relative_performance_vs_100_qqq",
            "scoreboard_status",
            "data_quality_warnings",
            "external_validation_warning_status",
            "paper_shadow_blockers",
            "owner_next_action",
        ],
        monitor_rules=monitor_rules,
        owner_next_action="run_monthly_research_only_review_after_new_matured_samples",
        report_registry_entry=_roadmap_doc_report_registry_entry(
            "dual_forward_aging_monthly_monitor_contract",
            "Dual Forward-Aging Monthly Monitor Contract",
            "aits research strategies dual-forward-aging-monthly-monitor-contract",
            "dual_forward_aging_monthly_monitor_contract",
            "docs/research/dual_forward_aging_monthly_monitor_contract.md",
        ),
    )
    _write_json_and_owner_doc(
        payload,
        output_root / "dual_forward_aging_monthly_monitor_contract.json",
        docs_path,
        "Dual Forward-Aging Monthly Monitor Contract",
    )
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


def _growth_tilt_real_cli_source_runs(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config_path: Path,
    output_root: Path,
    roadmap_output_root: Path,
    owner_docs_path: Path,
    master_docs_path: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> list[tuple[str, str, Callable[[], dict[str, Any]]]]:
    data_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_prices_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": output_root,
        "as_of_date": as_of_date,
        "start_date": start_date,
        "end_date": end_date,
    }
    config_kwargs = {"config_path": config_path, "output_root": output_root}
    return [
        (
            "growth_research_framing_correction",
            "aits research strategies growth-research-framing-correction",
            lambda: run_growth_research_framing_correction(output_root=output_root),
        ),
        (
            "equal_risk_growth_tilt_objective_contract",
            "aits research strategies equal-risk-growth-tilt-objective-contract",
            lambda: run_equal_risk_growth_tilt_objective_contract(**config_kwargs),
        ),
        (
            "equal_risk_growth_tilt_registry_review",
            "aits research strategies equal-risk-growth-tilt-registry-review",
            lambda: run_equal_risk_growth_tilt_registry_review(**config_kwargs),
        ),
        (
            "equal_risk_cap_floor_tilt_search",
            "aits research strategies equal-risk-cap-floor-tilt-search",
            lambda: run_equal_risk_cap_floor_tilt_search(**data_kwargs),
        ),
        (
            "equal_risk_risk_budget_tilt_search",
            "aits research strategies equal-risk-risk-budget-tilt-search",
            lambda: run_equal_risk_risk_budget_tilt_search(**data_kwargs),
        ),
        (
            "equal_risk_trend_on_qqq_boost_search",
            "aits research strategies equal-risk-trend-on-qqq-boost-search",
            lambda: run_equal_risk_trend_on_qqq_boost_search(**data_kwargs),
        ),
        (
            "equal_risk_missed_upside_compensation_search",
            "aits research strategies equal-risk-missed-upside-compensation-search",
            lambda: run_equal_risk_missed_upside_compensation_search(**data_kwargs),
        ),
        (
            "equal_risk_small_tqqq_overlay_search",
            "aits research strategies equal-risk-small-tqqq-overlay-search",
            lambda: run_equal_risk_small_tqqq_overlay_search(**data_kwargs),
        ),
        (
            "equal_risk_vol_target_growth_tilt_search",
            "aits research strategies equal-risk-vol-target-growth-tilt-search",
            lambda: run_equal_risk_vol_target_growth_tilt_search(**data_kwargs),
        ),
        (
            "equal_risk_growth_tilt_ranking_tiering",
            "aits research strategies equal-risk-growth-tilt-ranking-tiering",
            lambda: run_equal_risk_growth_tilt_ranking_tiering(**data_kwargs),
        ),
        (
            "growth_tilt_beta_risk_budget_attribution",
            "aits research strategies growth-tilt-beta-risk-budget-attribution",
            lambda: run_growth_tilt_beta_risk_budget_attribution(**data_kwargs),
        ),
        (
            "growth_tilt_period_drawdown_replay",
            "aits research strategies growth-tilt-period-drawdown-replay",
            lambda: run_growth_tilt_period_drawdown_replay(**data_kwargs),
        ),
        (
            "growth_tilt_cost_turnover_sensitivity",
            "aits research strategies growth-tilt-cost-turnover-sensitivity",
            lambda: run_growth_tilt_cost_turnover_sensitivity(**data_kwargs),
        ),
        (
            "equal_risk_growth_tilt_tradeoff_frontier",
            "aits research strategies equal-risk-growth-tilt-tradeoff-frontier",
            lambda: run_equal_risk_growth_tilt_tradeoff_frontier(**data_kwargs),
        ),
        (
            "growth_tilt_definition_lock_versioning",
            "aits research strategies growth-tilt-definition-lock-versioning",
            lambda: run_growth_tilt_definition_lock_versioning(**data_kwargs),
        ),
        (
            "growth_tilt_forward_aging_readiness_gate",
            "aits research strategies growth-tilt-forward-aging-readiness-gate",
            lambda: run_growth_tilt_forward_aging_readiness_gate(**data_kwargs),
        ),
        (
            "growth_tilt_owner_decision_pack",
            "aits research strategies growth-tilt-owner-decision-pack",
            lambda: run_growth_tilt_owner_decision_pack(
                **data_kwargs, docs_path=owner_docs_path
            ),
        ),
        (
            "growth_exploration_master_review",
            "aits research strategies growth-exploration-master-review",
            lambda: run_growth_exploration_master_review(
                **data_kwargs,
                docs_path=master_docs_path,
                owner_docs_path=owner_docs_path,
            ),
        ),
        (
            "roadmap_update_after_growth_tilt_review",
            "aits research strategies roadmap-update-after-growth-tilt-review",
            lambda: run_roadmap_update_after_growth_tilt_review(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                growth_output_root=output_root,
                output_root=roadmap_output_root,
                growth_master_docs_path=master_docs_path,
                growth_owner_docs_path=owner_docs_path,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "growth_tilt_reader_brief_safety_preview",
            "aits research strategies growth-tilt-reader-brief-safety-preview",
            lambda: run_growth_tilt_reader_brief_safety_preview(**data_kwargs),
        ),
    ]


def _growth_tilt_source_run_row(
    report_id: str,
    command: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    artifact_paths = _mapping(payload.get("artifact_paths"))
    return {
        "report_id": report_id,
        "command": command,
        "status": payload.get("status"),
        "warnings": _dedupe_text(
            [*_text_list(payload.get("warnings")), *_text_list(payload.get("warning_reasons"))]
        ),
        "blockers": _dedupe_text(
            [
                *_text_list(payload.get("blockers")),
                *_text_list(payload.get("blocking_reasons")),
                *_text_list(payload.get("tier_blockers")),
            ]
        ),
        "artifact_json_path": artifact_paths.get("json_path"),
        "artifact_md_path": artifact_paths.get("markdown_path"),
        "data_quality_status": _payload_data_quality_status(payload),
        "candidate_count": _candidate_count(payload),
        "top_candidate": _top_candidate(payload),
        "top_candidate_family": _top_candidate_family(payload),
        "highest_tier": _highest_tier_from_payload(payload),
        "paper_shadow_allowed": payload.get("paper_shadow_allowed", False),
        "production_allowed": payload.get("production_allowed", False),
        "broker_action": payload.get("broker_action", "none"),
    }


def _all_ranked_candidates(ranking: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in (
        "tier_3_component_ready_candidates",
        "tier_2_growth_challengers",
        "tier_1_growth_tilt_candidates",
        "rejected_candidates",
        "top_by_return_edge_vs_equal_risk",
    ):
        rows.extend(_records(ranking.get(key)))
    return _dedupe_candidates(rows)


def _growth_tilt_candidate_summary_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": row.get("strategy_id"),
        "candidate_family": row.get("candidate_family"),
        "candidate_tier": row.get("candidate_tier", "REJECTED"),
        "annual_return": row.get("annual_return"),
        "annual_return_edge_vs_equal_risk": row.get("annual_return_edge_vs_equal_risk")
        or row.get("return_edge_vs_equal_risk"),
        "annual_return_gap_vs_100_qqq": row.get("annual_return_gap_vs_100_qqq")
        or row.get("return_gap_vs_100_qqq"),
        "return_gap_reduction_vs_100_qqq": row.get(
            "return_gap_reduction_vs_100_qqq"
        ),
        "return_edge_vs_100_qqq": row.get("return_edge_vs_100_qqq"),
        "max_drawdown": row.get("max_drawdown"),
        "drawdown_increase_vs_equal_risk": row.get("drawdown_increase_vs_equal_risk"),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "turnover": row.get("turnover"),
        "switch_count": row.get("switch_count"),
        "effective_qqq_beta": row.get("effective_qqq_beta"),
        "effective_leverage": row.get("effective_leverage"),
        "average_qqq_weight": row.get("average_qqq_weight"),
        "average_tqqq_weight": row.get("average_tqqq_weight"),
        "average_sgov_weight": row.get("average_sgov_weight"),
        "beta_adjusted_return_edge": row.get("beta_adjusted_return_edge")
        or row.get("beta_adjusted_edge"),
        "beta_adjusted_sharpe_edge": row.get("beta_adjusted_sharpe_edge"),
        "beta_adjusted_calmar_edge": row.get("beta_adjusted_calmar_edge"),
        "drawdown_penalty": row.get("drawdown_penalty"),
        "path_dependency_risk": row.get("path_dependency_risk"),
        "max_tqqq_weight": row.get("max_tqqq_weight"),
        "definition_hash": row.get("definition_hash"),
        "policy_definition": row.get("policy_definition"),
        "data_quality_status": row.get("data_quality_status"),
        "research_commentary": _growth_tilt_candidate_commentary(row),
        **_safety_summary(),
    }


def _growth_tilt_candidate_commentary(row: Mapping[str, Any]) -> str:
    tier = str(row.get("candidate_tier") or "REJECTED")
    if tier == "COMPONENT_READY_GROWTH":
        return "tier_3_component_ready_research_only_owner_review_required"
    if tier == "GROWTH_CHALLENGER":
        return "tier_2_growth_challenger_research_only_not_qqq_replacement"
    if tier == "GROWTH_TILT_CANDIDATE":
        return "tier_1_candidate_improves_equal_risk_under_current_policy"
    if _float(row.get("return_edge_vs_equal_risk")) > 0.0:
        return "positive_return_edge_but_failed_risk_or_turnover_gate"
    return "no_useful_growth_tilt_under_current_policy"


def _growth_tilt_tier_validation_row(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    raw_tier = str(row.get("candidate_tier") or "REJECTED")
    tier = raw_tier if raw_tier != "REJECTED" else "NO_USEFUL_GROWTH_TILT"
    blockers = _tier_blockers(row, config)
    warnings: list[str] = []
    if tier == "GROWTH_TILT_CANDIDATE":
        warnings.append("tier_1_candidate_not_qqq_replacement")
    if _float(row.get("effective_qqq_beta")) > 1.0:
        warnings.append("effective_beta_above_1_requires_beta_adjusted_review")
    return {
        "strategy_id": row.get("strategy_id"),
        "candidate_family": row.get("candidate_family"),
        "tier": tier,
        "tier_reason": _tier_reason(tier),
        "tier_blockers": blockers,
        "tier_warnings": _dedupe_text(warnings),
        "annual_return_vs_equal_risk": row.get("annual_return_edge_vs_equal_risk"),
        "annual_return_vs_100_qqq": row.get("return_edge_vs_100_qqq"),
        "drawdown_status": (
            "PASS"
            if _float(row.get("drawdown_increase_vs_equal_risk"))
            <= _candidate_limit(config, "max_drawdown_increase_vs_equal_risk")
            else "FAIL"
        ),
        "sharpe_status": "PASS" if _float(row.get("sharpe")) > 0.0 else "FAIL",
        "calmar_status": "PASS" if _float(row.get("calmar")) > 0.0 else "FAIL",
        "turnover_status": (
            "PASS"
            if _float(row.get("turnover")) <= _candidate_limit(config, "max_turnover")
            else "FAIL"
        ),
        "period_status": "REQUIRES_TRIAGE_REVIEW",
        "beta_adjusted_status": (
            "PASS"
            if _float(row.get("beta_adjusted_return_edge"))
            > _candidate_limit(config, "beta_adjusted_edge_minimum")
            else "WEAK_OR_NOT_MATERIAL"
        ),
    }


def _tier_blockers(row: Mapping[str, Any], config: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if _float(row.get("annual_return_edge_vs_equal_risk")) <= 0.0:
        blockers.append("not_better_than_equal_risk")
    if _float(row.get("return_gap_reduction_vs_100_qqq")) <= 0.0:
        blockers.append("return_gap_vs_100_qqq_not_reduced")
    if _float(row.get("drawdown_increase_vs_equal_risk")) > _candidate_limit(
        config, "max_drawdown_increase_vs_equal_risk"
    ):
        blockers.append("drawdown_increase_vs_equal_risk_too_high")
    if _float(row.get("turnover")) > _candidate_limit(config, "max_turnover"):
        blockers.append("turnover_too_high")
    if str(row.get("data_quality_status")) in {"FAIL", "BLOCKED"}:
        blockers.append("data_quality_blocked")
    return blockers


def _tier_reason(tier: str) -> str:
    return {
        "COMPONENT_READY_GROWTH": "tier_3_beta_adjusted_component_ready_candidate",
        "GROWTH_CHALLENGER": "tier_2_growth_challenger_candidate",
        "GROWTH_TILT_CANDIDATE": "tier_1_improves_equal_risk_candidate",
        "NO_USEFUL_GROWTH_TILT": "failed_current_growth_tilt_policy_gates",
    }.get(tier, "unknown_tier")


def _best_summary_candidate(summary: Mapping[str, Any]) -> dict[str, Any]:
    for tier in ("COMPONENT_READY_GROWTH", "GROWTH_CHALLENGER", "GROWTH_TILT_CANDIDATE"):
        rows = [
            row
            for row in _records(summary.get("candidate_results"))
            if row.get("candidate_tier") == tier
        ]
        if rows:
            return sorted(
                rows,
                key=lambda row: _float(row.get("annual_return_edge_vs_equal_risk")),
                reverse=True,
            )[0]
    rows = _records(summary.get("candidate_results"))
    return rows[0] if rows else {}


def _selected_summary_candidates(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = [
        row
        for row in _records(summary.get("candidate_results"))
        if row.get("candidate_tier") != "REJECTED"
    ]
    return rows[:5] if rows else _records(summary.get("candidate_results"))[:5]


def _candidate_cost_penalty(
    candidate: Mapping[str, Any],
    cost: Mapping[str, Any],
) -> float:
    strategy_id = str(candidate.get("strategy_id") or "")
    return max(
        (
            _float(row.get("performance_degradation"))
            for row in _records(cost.get("scenario_rows"))
            if str(row.get("strategy_id")) == strategy_id
        ),
        default=0.0,
    )


def _growth_tilt_edge_explanation(candidate: Mapping[str, Any], status: str) -> str:
    if not candidate:
        return "no_candidate_available_for_edge_review"
    if status == "BETA_EXPLAINS_EDGE":
        return "raw_return_lift_is_explained_by_effective_beta_under_current_review"
    if status == "BETA_ADJUSTED_EDGE_MATERIAL":
        return "edge_survives_beta_adjustment_and_penalty_review_research_only"
    if status == "BETA_ADJUSTED_EDGE_PRESENT":
        return "beta_adjusted_edge_positive_but_not_material_after_penalties"
    return "edge_weak_after_beta_drawdown_turnover_and_path_dependency_penalties"


def _growth_tilt_benchmark_comparisons(
    *,
    candidate_strategy_id: str,
    prices_path: Path,
    config_path: Path,
    start_date: date,
    end_date: date | None,
) -> list[dict[str, Any]]:
    config = _load_config(config_path)
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    benchmark_rows = _benchmark_metric_rows(prices, config)
    wanted = {
        "equal_risk_qqq_sgov",
        "100_qqq",
        "qqq_50_sgov_50",
        "qqq_60_sgov_40",
    }
    rows = []
    for benchmark_id, row in benchmark_rows.items():
        if benchmark_id not in wanted:
            continue
        rows.append(
            {
                "strategy_id": candidate_strategy_id,
                "benchmark_id": benchmark_id,
                "benchmark_annual_return": row.get("annual_return"),
                "benchmark_max_drawdown": row.get("max_drawdown"),
                "benchmark_sharpe": row.get("sharpe"),
                "benchmark_calmar": row.get("calmar"),
                "benchmark_turnover": row.get("turnover"),
            }
        )
    return sorted(rows, key=lambda row: str(row["benchmark_id"]))


def _growth_tilt_benchmark_frontier_rows(
    *,
    prices_path: Path,
    config_path: Path,
    start_date: date,
    end_date: date | None,
) -> list[dict[str, Any]]:
    config = _load_config(config_path)
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    rows = []
    for strategy_id, row in _benchmark_metric_rows(prices, config).items():
        if strategy_id == DEFENSIVE_PRIMARY_ID:
            role = "DEFENSIVE_CORE"
        elif strategy_id in {"qqq_50_sgov_50", "qqq_60_sgov_40"}:
            role = "BALANCED_CORE"
        elif strategy_id == PRIMARY_QQQ_BENCHMARK_ID:
            role = "GROWTH_CHALLENGER"
        else:
            role = "REJECTED"
        rows.append(
            {
                "strategy_id": strategy_id,
                "candidate_family": "benchmark",
                "annual_return": row.get("annual_return"),
                "max_drawdown": row.get("max_drawdown"),
                "calmar": row.get("calmar"),
                "sharpe": row.get("sharpe"),
                "turnover": row.get("turnover"),
                "recommended_role": role,
                **_safety_summary(),
            }
        )
    return rows


def _dominated_frontier_rows(rows: list[Mapping[str, Any]]) -> set[str]:
    dominated: set[str] = set()
    for left in rows:
        left_id = str(left.get("strategy_id") or "")
        for right in rows:
            right_id = str(right.get("strategy_id") or "")
            if not left_id or left_id == right_id:
                continue
            no_worse = (
                _float(right.get("annual_return")) >= _float(left.get("annual_return"))
                and _float(right.get("max_drawdown")) >= _float(left.get("max_drawdown"))
                and _float(right.get("calmar")) >= _float(left.get("calmar"))
                and _float(right.get("sharpe")) >= _float(left.get("sharpe"))
                and _float(right.get("turnover")) <= _float(left.get("turnover"))
            )
            strictly_better = (
                _float(right.get("annual_return")) > _float(left.get("annual_return"))
                or _float(right.get("max_drawdown")) > _float(left.get("max_drawdown"))
                or _float(right.get("calmar")) > _float(left.get("calmar"))
                or _float(right.get("sharpe")) > _float(left.get("sharpe"))
                or _float(right.get("turnover")) < _float(left.get("turnover"))
            )
            if no_worse and strictly_better:
                dominated.add(left_id)
                break
    return dominated


def _growth_tilt_triage_row(
    candidate: Mapping[str, Any],
    *,
    replay: Mapping[str, Any],
    cost: Mapping[str, Any],
) -> dict[str, Any]:
    strategy_id = str(candidate.get("strategy_id") or "")
    period_rows = [
        row
        for row in _records(replay.get("period_rows"))
        if str(row.get("strategy_id")) == strategy_id
    ]
    cost_rows = [
        row
        for row in _records(cost.get("scenario_rows"))
        if str(row.get("strategy_id")) == strategy_id
    ]
    worst = min(
        period_rows,
        key=lambda row: _float(row.get("relative_vs_equal_risk")),
        default={},
    )
    best = max(
        period_rows,
        key=lambda row: _float(row.get("relative_vs_equal_risk")),
        default={},
    )
    ai_dependency = any(
        row.get("strategy_id") == strategy_id and row.get("ai_rally_dependency")
        for row in _records(replay.get("candidate_summaries"))
    )
    period_status = "REGIME_CONCENTRATED" if ai_dependency else "PERIOD_SPLIT_PASS"
    drawdown_status = (
        "DRAWDOWN_RISK_TOO_HIGH"
        if replay.get("status") == "GROWTH_TILT_DRAWDOWN_RISK_TOO_HIGH"
        else "DRAWDOWN_EPISODE_PASS"
    )
    cost_status = str(cost.get("status") or "UNKNOWN")
    return {
        "strategy_id": strategy_id,
        "period_split_status": period_status,
        "drawdown_episode_status": drawdown_status,
        "cost_sensitivity_status": cost_status,
        "worst_period": worst.get("period"),
        "best_period": best.get("period"),
        "ai_rally_dependency": bool(ai_dependency),
        "bear_market_drawdown": _period_value(
            period_rows, "2022_rate_hike_bear_market", "max_drawdown"
        ),
        "missed_rebound_cost": max(
            (_float(row.get("missed_rebound_cost")) for row in period_rows),
            default=0.0,
        ),
        "late_risk_off_cost": max(
            (_float(row.get("late_risk_off_cost")) for row in period_rows),
            default=0.0,
        ),
        "late_risk_on_cost": max(
            (_float(row.get("late_risk_on_cost")) for row in period_rows),
            default=0.0,
        ),
        "cost_drag": max(
            (_float(row.get("performance_degradation")) for row in cost_rows),
            default=0.0,
        ),
        "turnover": candidate.get("turnover"),
        "switch_count": candidate.get("switch_count"),
        "triage_commentary": _growth_tilt_triage_commentary(
            period_status=period_status,
            drawdown_status=drawdown_status,
            cost_status=cost_status,
        ),
    }


def _growth_tilt_triage_commentary(
    *,
    period_status: str,
    drawdown_status: str,
    cost_status: str,
) -> str:
    if drawdown_status == "DRAWDOWN_RISK_TOO_HIGH":
        return "drawdown_episode_blocks_growth_tilt_review"
    if period_status == "REGIME_CONCENTRATED":
        return "period_split_is_concentrated_in_ai_rally"
    if cost_status in {"GROWTH_TILT_COST_SENSITIVE", "GROWTH_TILT_TURNOVER_TOO_HIGH"}:
        return "cost_or_turnover_requires_more_research"
    return "period_drawdown_cost_checks_do_not_block_research_summary"


def _period_value(rows: list[Mapping[str, Any]], period: str, key: str) -> Any:
    for row in rows:
        if row.get("period") == period:
            return row.get(key)
    return None


def _growth_tilt_final_gate_blockers(
    candidate: Mapping[str, Any],
    beta: Mapping[str, Any],
    triage: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not candidate:
        return ["no_growth_tilt_candidate"]
    if _float(candidate.get("annual_return_edge_vs_equal_risk")) <= 0.0:
        blockers.append("not_better_than_equal_risk")
    if str(triage.get("status")) in {
        "GROWTH_TILT_DRAWDOWN_RISK_TOO_HIGH",
        "GROWTH_TILT_COST_BLOCKED",
        "GROWTH_TILT_TRIAGE_BLOCKED",
    }:
        blockers.append(f"triage_status:{triage.get('status')}")
    if (
        candidate.get("candidate_tier") == "COMPONENT_READY_GROWTH"
        and beta.get("status") != "BETA_ADJUSTED_EDGE_MATERIAL"
    ):
        blockers.append("component_ready_requires_material_beta_adjusted_edge")
    if str(candidate.get("data_quality_status")) in {"FAIL", "BLOCKED"}:
        blockers.append("data_quality_blocked")
    return _dedupe_text(blockers)


def _growth_tilt_final_gate_warnings(
    candidate: Mapping[str, Any],
    beta: Mapping[str, Any],
    triage: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if candidate and candidate.get("candidate_tier") == "GROWTH_TILT_CANDIDATE":
        warnings.append("tier_1_candidate_not_qqq_replacement")
    if beta.get("status") == "BETA_EXPLAINS_EDGE":
        warnings.append("return_lift_may_be_beta_driven")
    if triage.get("status") in {"GROWTH_TILT_TRIAGE_WARN", "GROWTH_TILT_REGIME_CONCENTRATED"}:
        warnings.append(f"triage_status:{triage.get('status')}")
    return _dedupe_text(warnings)


def _growth_tilt_recommended_role(
    candidate: Mapping[str, Any],
    status: str,
) -> str:
    if not candidate or status == "NO_USEFUL_GROWTH_TILT":
        return "REJECTED"
    if status == "GROWTH_TILT_COMPONENT_REVIEWABLE":
        return "COMPONENT_READY_GROWTH_REVIEW"
    if status == "GROWTH_TILT_TIER2_REVIEWABLE":
        return "GROWTH_CHALLENGER"
    if status == "GROWTH_TILT_TIER1_REVIEWABLE":
        return "GROWTH_TILT_CANDIDATE"
    return "RESEARCH_ONLY"


def _growth_tilt_owner_recommendation_real(
    *,
    candidate: Mapping[str, Any],
    final_gate: Mapping[str, Any],
    watchlist: Mapping[str, Any],
    source_payloads: Mapping[str, Mapping[str, Any]],
) -> str:
    if any(
        _growth_tilt_source_artifact_blocked_status(str(source.get("status")))
        for source in source_payloads.values()
    ):
        return "BLOCKED"
    if watchlist.get("watchlist_allowed") is True:
        return "ADD_GROWTH_TILT_TO_FORWARD_AGING"
    if not candidate or final_gate.get("status") == "NO_USEFUL_GROWTH_TILT":
        return "NO_USEFUL_GROWTH_TILT"
    if final_gate.get("status") == "GROWTH_TILT_RESEARCH_ONLY":
        return "KEEP_GROWTH_TILT_RESEARCH_ONLY"
    return "NEED_MORE_RESEARCH"


def _growth_tilt_master_next_task(status: str) -> str:
    if status == "GROWTH_TILT_FORWARD_AGING_REVIEWABLE":
        return "owner_manual_review_before_research_only_forward_aging_watchlist"
    if status == "NO_USEFUL_GROWTH_TILT":
        return "keep_equal_risk_defensive_primary_and_pause_growth_tilt_candidate"
    if status == "GROWTH_TILT_REAL_RESULT_BLOCKED":
        return "resolve_blocked_growth_tilt_source_before_interpretation"
    return "continue_research_only_growth_tilt_evidence_review"


def _group_candidates_by_family(rows: list[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("candidate_family") or "unknown"), []).append(dict(row))
    return grouped


def _candidate_count(payload: Mapping[str, Any]) -> int:
    summary = _mapping(payload.get("summary"))
    if "candidate_count" in summary:
        return _int(summary.get("candidate_count"))
    for key in (
        "candidate_results",
        "candidate_summaries",
        "attribution_rows",
        "tier_validation_rows",
        "triage_rows",
    ):
        rows = _records(payload.get(key))
        if rows:
            return len(rows)
    return 0


def _top_candidate(payload: Mapping[str, Any]) -> str | None:
    summary = _mapping(payload.get("summary"))
    for key in ("top_candidate", "candidate_strategy_id", "strategy_id"):
        value = _text(summary.get(key))
        if value:
            return value
    for key in (
        "candidate_results",
        "tier_3_component_ready_candidates",
        "top_by_return_edge_vs_equal_risk",
    ):
        rows = _records(payload.get(key))
        if rows:
            return _text(rows[0].get("strategy_id")) or None
    return _text(payload.get("candidate_strategy_id")) or None


def _top_candidate_family(payload: Mapping[str, Any]) -> str | None:
    summary = _mapping(payload.get("summary"))
    value = _text(summary.get("top_candidate_family"))
    if value:
        return value
    for key in ("candidate_results", "top_by_return_edge_vs_equal_risk"):
        rows = _records(payload.get(key))
        if rows:
            return _text(rows[0].get("candidate_family")) or None
    return None


def _highest_tier_from_payload(payload: Mapping[str, Any]) -> str | None:
    summary = _mapping(payload.get("summary"))
    tier = _text(summary.get("highest_tier"))
    if tier:
        return tier
    if _records(payload.get("tier_3_component_ready_candidates")):
        return "COMPONENT_READY_GROWTH"
    if _records(payload.get("tier_2_growth_challengers")):
        return "GROWTH_CHALLENGER"
    if _records(payload.get("tier_1_growth_tilt_candidates")):
        return "GROWTH_TILT_CANDIDATE"
    rows = _records(payload.get("candidate_results"))
    return _highest_tier_from_values(row.get("candidate_tier") for row in rows)


def _highest_tier_from_values(values: Any) -> str | None:
    order = {
        "GROWTH_TILT_CANDIDATE": 1,
        "GROWTH_CHALLENGER": 2,
        "COMPONENT_READY_GROWTH": 3,
    }
    best: str | None = None
    best_score = 0
    for value in values:
        text = str(value or "")
        score = order.get(text, 0)
        if score > best_score:
            best = text
            best_score = score
    return best


def _highest_tier_at_least(value: object, required: str) -> bool:
    order = {
        "GROWTH_TILT_CANDIDATE": 1,
        "GROWTH_CHALLENGER": 2,
        "COMPONENT_READY_GROWTH": 3,
    }
    return order.get(str(value or ""), 0) >= order.get(required, 0)


def _payload_data_quality_status(payload: Mapping[str, Any]) -> str | None:
    summary = _mapping(payload.get("summary"))
    data_quality = _mapping(payload.get("data_quality"))
    return (
        _text(summary.get("data_quality_status"))
        or _text(payload.get("data_quality_status"))
        or _text(data_quality.get("status"))
        or None
    )


def _growth_tilt_source_artifact_blocked_status(status: str) -> bool:
    return _blocked_status(status) or status in {
        "GROWTH_TILT_RESULTS_BLOCKED",
        "GROWTH_TILT_TIER_BLOCKED",
        "BETA_ADJUSTED_EDGE_BLOCKED",
        "GROWTH_TILT_FRONTIER_BLOCKED",
        "GROWTH_TILT_TRIAGE_BLOCKED",
        "GROWTH_TILT_FINAL_GATE_BLOCKED",
        "GROWTH_TILT_WATCHLIST_BLOCKED",
        "BLOCKED",
    }


def _focused_source_blockers(sources: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [
        name
        for name, source in sources.items()
        if _growth_tilt_source_artifact_blocked_status(str(source.get("status")))
        or str(source.get("status", "")).endswith("_BLOCKED")
    ]


def _focused_growth_tilt_candidate(summary: Mapping[str, Any]) -> dict[str, Any]:
    rows = _records(summary.get("candidate_results"))
    for row in rows:
        if row.get("strategy_id") == FOCUSED_GROWTH_TILT_CANDIDATE_ID:
            return row
    return _best_summary_candidate(summary)


def _target_vol_config(candidate: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping(candidate.get("policy_definition"))
    return {
        "target_vol_mode": policy.get("target_vol_mode"),
        "target_vol": policy.get("target_vol"),
        "target_vol_additive": policy.get("target_vol_additive"),
        "qqq_max_weight": policy.get("qqq_max_weight"),
        "sgov_min_weight": policy.get("sgov_min_weight"),
        "rebalance_rule": policy.get("rebalance_rule"),
    }


def _weight_path_summary(weights: pd.DataFrame, ticker: str) -> dict[str, Any]:
    if weights.empty or ticker not in weights:
        return {
            "ticker": ticker,
            "average_weight": 0.0,
            "min_weight": 0.0,
            "max_weight": 0.0,
            "final_weight": 0.0,
            "weight_change_count": 0,
        }
    series = weights[ticker].fillna(0.0)
    return {
        "ticker": ticker,
        "average_weight": _round(series.mean()),
        "min_weight": _round(series.min()),
        "max_weight": _round(series.max()),
        "final_weight": _round(series.iloc[-1]),
        "weight_change_count": int((series.diff().abs().fillna(0.0) > 1e-9).sum()),
    }


def _weight_path_is_stable(weights: pd.DataFrame) -> bool:
    if weights.empty:
        return False
    totals = weights.fillna(0.0).sum(axis=1)
    return bool((totals <= 1.000001).all() and (weights.fillna(0.0) >= -1e-9).all().all())


def _default_focused_vol_target_candidate(config: Mapping[str, Any]) -> dict[str, Any]:
    return _candidate(
        config,
        "vol_target_growth_tilt",
        strategy_suffix="tv4_w120_q7_s1",
        target_vol_mode="equal_risk_plus",
        target_vol=None,
        target_vol_additive=0.04,
        vol_lookback=120,
        qqq_max_weight=0.70,
        sgov_min_weight=0.10,
        rebalance_rule="monthly",
    )


def _local_vol_target_variants(
    base_candidate: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    mode = str(base_candidate.get("target_vol_mode") or "equal_risk_plus")
    base_target = _float(
        base_candidate.get("target_vol")
        if mode == "absolute"
        else base_candidate.get("target_vol_additive"),
        0.04,
    )
    base_qqq_cap = _float(base_candidate.get("qqq_max_weight"), 0.70)
    base_sgov_floor = _float(base_candidate.get("sgov_min_weight"), 0.10)
    variants: list[dict[str, Any]] = []
    target_values = [
        max(base_target + delta, 0.0)
        for delta in (-0.02, -0.01, 0.0, 0.01, 0.02)
    ]
    for target_value in target_values:
        for window in (60, 90, 120, 180):
            for qqq_cap in _dedupe_float_values(
                [base_qqq_cap - 0.10, base_qqq_cap, base_qqq_cap + 0.10]
            ):
                for sgov_floor in _dedupe_float_values(
                    [max(base_sgov_floor - 0.10, 0.0), base_sgov_floor, base_sgov_floor + 0.10]
                ):
                    for rebalance in ("monthly", "threshold"):
                        suffix = (
                            f"tv{_pct_token(target_value)}_w{window}_"
                            f"q{_pct_token(qqq_cap)}_s{_pct_token(sgov_floor)}"
                        )
                        if rebalance != "monthly":
                            suffix = f"{suffix}_{rebalance}"
                        variants.append(
                            _candidate(
                                config,
                                "vol_target_growth_tilt",
                                strategy_suffix=suffix,
                                target_vol_mode=mode,
                                target_vol=target_value if mode == "absolute" else None,
                                target_vol_additive=(
                                    target_value if mode != "absolute" else None
                                ),
                                vol_lookback=window,
                                qqq_max_weight=qqq_cap,
                                sgov_min_weight=sgov_floor,
                                rebalance_rule=rebalance,
                            )
                        )
    return _dedupe_candidates(variants)


def _dedupe_float_values(values: list[float]) -> list[float]:
    result: list[float] = []
    seen: set[float] = set()
    for value in values:
        rounded = round(value, 6)
        if rounded not in seen:
            result.append(rounded)
            seen.add(rounded)
    return result


def _local_sensitivity_row(
    row: Mapping[str, Any],
    base_candidate: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    rank: int,
) -> dict[str, Any]:
    policy = _mapping(row.get("policy_definition"))
    target_key = (
        "target_vol"
        if policy.get("target_vol_mode") == "absolute"
        else "target_vol_additive"
    )
    parameter_delta = {
        target_key: _round(
            _float(policy.get(target_key)) - _float(base_candidate.get(target_key))
        ),
        "vol_lookback": _int(policy.get("vol_lookback"))
        - _int(base_candidate.get("vol_lookback")),
        "qqq_cap": _round(
            _float(policy.get("qqq_max_weight"))
            - _float(base_candidate.get("qqq_max_weight"))
        ),
        "sgov_floor": _round(
            _float(policy.get("sgov_min_weight"))
            - _float(base_candidate.get("sgov_min_weight"))
        ),
        "rebalance": policy.get("rebalance_rule"),
    }
    robustness_score = _float(row.get("beta_adjusted_edge")) - max(
        _float(row.get("drawdown_increase_vs_equal_risk")),
        0.0,
    )
    return {
        "variant_strategy_id": row.get("strategy_id"),
        "parameter_delta": parameter_delta,
        "annual_return": row.get("annual_return"),
        "return_edge_vs_equal_risk": row.get("annual_return_edge_vs_equal_risk"),
        "return_gap_vs_100_qqq": row.get("annual_return_gap_vs_100_qqq"),
        "max_drawdown": row.get("max_drawdown"),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "turnover": row.get("turnover"),
        "effective_qqq_beta": row.get("effective_qqq_beta"),
        "beta_adjusted_edge": row.get("beta_adjusted_edge"),
        "rank_stability": (
            "base_candidate"
            if row.get("strategy_id") == base_candidate.get("strategy_id")
            else ("top_10_local_variant" if rank <= 10 else "lower_ranked_local_variant")
        ),
        "local_robustness_score": _round(robustness_score),
        "max_effective_qqq_beta_limit": _candidate_limit(
            config, "max_effective_qqq_beta"
        ),
        **_safety_summary(),
    }


def _local_base_result(
    rows: list[Mapping[str, Any]],
    base_candidate: Mapping[str, Any],
) -> dict[str, Any]:
    base_id = str(base_candidate.get("strategy_id") or "")
    for row in rows:
        if row.get("variant_strategy_id") == base_id:
            return dict(row)
    return dict(rows[0]) if rows else {}


def _beta_methodology_rows(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not candidate:
        return []
    policy = _candidate_from_row(candidate)
    returns, weights = _returns_and_weights(policy, prices, config)
    equal_returns, _equal_weights = _returns_and_weights(
        {"strategy_id": DEFENSIVE_PRIMARY_ID, "special_policy": "equal_risk"},
        prices,
        config,
    )
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    annualization = _annualization(config)
    strategy_metrics = _series_metrics(returns, annualization)
    equal_metrics = _series_metrics(equal_returns, annualization)
    qqq_metrics = _series_metrics(qqq_returns, annualization)
    raw_edge = _float(strategy_metrics.get("annual_return")) - _float(
        equal_metrics.get("annual_return")
    )
    equal_beta = _beta(equal_returns, qqq_returns)
    qqq_return = _float(qqq_metrics.get("annual_return"))
    avg_weights = weights.mean().to_dict() if not weights.empty else {}
    qqq_equiv = _float(avg_weights.get("QQQ")) + _float(
        avg_weights.get("TQQQ")
    ) * TQQQ_DAILY_LEVERAGE_MULTIPLIER
    beta_estimates = {
        "static_beta_adjustment": _beta(returns, qqq_returns),
        "rolling_60d_beta_adjustment": _rolling_beta_mean(returns, qqq_returns, 60),
        "rolling_120d_beta_adjustment": _rolling_beta_mean(returns, qqq_returns, 120),
        "regime_conditional_beta_adjustment": _regime_conditional_beta(
            returns, qqq_returns, prices, config
        ),
        "qqq_equivalent_exposure_adjustment": qqq_equiv,
        "risk_budget_attribution_adjustment": _float(
            candidate.get("effective_qqq_beta")
        ),
    }
    sgov_carry = _asset_contribution(
        weights,
        prices["SGOV"].pct_change().fillna(0.0),
        "SGOV",
        config,
    )
    rebalance_edge = _rebalance_contribution(returns, weights, prices, config)
    rows = []
    for method_id, estimate in beta_estimates.items():
        beta_adjusted = raw_edge - max(estimate - equal_beta, 0.0) * qqq_return
        timing_edge = beta_adjusted - sgov_carry - rebalance_edge
        vol_targeting = (
            timing_edge
            if candidate.get("candidate_family") == "vol_target_growth_tilt"
            else 0.0
        )
        rows.append(
            {
                "method_id": method_id,
                "beta_estimate": _round(estimate),
                "raw_edge": _round(raw_edge),
                "beta_adjusted_edge": _round(beta_adjusted),
                "timing_edge": _round(timing_edge),
                "rebalance_edge": _round(rebalance_edge),
                "sgov_carry_contribution": _round(sgov_carry),
                "vol_targeting_contribution": _round(vol_targeting),
                "method_commentary": _beta_method_commentary(method_id, beta_adjusted),
            }
        )
    return rows


def _rolling_beta_mean(returns: pd.Series, benchmark: pd.Series, window: int) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    aligned.columns = ["strategy", "benchmark"]
    cov = aligned["strategy"].rolling(window, min_periods=max(10, window // 3)).cov(
        aligned["benchmark"]
    )
    var = aligned["benchmark"].rolling(window, min_periods=max(10, window // 3)).var()
    beta = (cov / var.replace(0.0, math.nan)).dropna()
    return float(beta.mean()) if not beta.empty else 0.0


def _regime_conditional_beta(
    returns: pd.Series,
    benchmark: pd.Series,
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> float:
    mask = _risk_on_mask(prices, config).reindex(returns.index).fillna(False)
    risk_on_beta = _beta(returns[mask], benchmark[mask])
    risk_off_beta = _beta(returns[~mask], benchmark[~mask])
    risk_on_share = float(mask.mean()) if len(mask) else 0.0
    return risk_on_beta * risk_on_share + risk_off_beta * (1.0 - risk_on_share)


def _rebalance_contribution(
    returns: pd.Series,
    weights: pd.DataFrame,
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> float:
    if weights.empty:
        return 0.0
    average_weights = weights.mean().to_dict()
    asset_returns = prices.pct_change().fillna(0.0)
    static_returns = pd.Series(0.0, index=asset_returns.index)
    for ticker, weight in average_weights.items():
        if ticker in asset_returns:
            static_returns += _float(weight) * asset_returns[ticker]
    actual = _float(_series_metrics(returns, _annualization(config)).get("annual_return"))
    static = _float(
        _series_metrics(static_returns, _annualization(config)).get("annual_return")
    )
    return actual - static


def _beta_method_commentary(method_id: str, beta_adjusted_edge: float) -> str:
    if beta_adjusted_edge > 0.0:
        return f"{method_id}_shows_positive_non_beta_edge"
    return f"{method_id}_does_not_show_positive_non_beta_edge"


def _balanced_core_role_rows(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    benchmarks = _benchmark_metric_rows(prices, config)
    equal = benchmarks.get(DEFENSIVE_PRIMARY_ID, {})
    qqq = benchmarks.get(PRIMARY_QQQ_BENCHMARK_ID, {})
    rows = []
    for strategy_id in (
        DEFENSIVE_PRIMARY_ID,
        str(candidate.get("strategy_id") or ""),
        PRIMARY_QQQ_BENCHMARK_ID,
        "qqq_50_sgov_50",
        "qqq_60_sgov_40",
    ):
        if not strategy_id:
            continue
        if strategy_id == candidate.get("strategy_id"):
            metrics = candidate
            role = "BALANCED_CORE"
        else:
            metrics = benchmarks.get(strategy_id, {})
            role = (
                "DEFENSIVE_CORE"
                if strategy_id == DEFENSIVE_PRIMARY_ID
                else (
                    "HARD_BENCHMARK"
                    if strategy_id == PRIMARY_QQQ_BENCHMARK_ID
                    else "STATIC_REFERENCE"
                )
            )
        drawdown_gap = _round(
            abs(_float(metrics.get("max_drawdown")))
            - abs(_float(equal.get("max_drawdown")))
        )
        return_gap = _round(
            _float(qqq.get("annual_return")) - _float(metrics.get("annual_return"))
        )
        role_score = (
            _float(metrics.get("calmar"))
            + _float(metrics.get("sharpe"))
            - max(drawdown_gap, 0.0)
        )
        rows.append(
            {
                "strategy_id": strategy_id,
                "role_candidate": role,
                "annual_return": metrics.get("annual_return"),
                "max_drawdown": metrics.get("max_drawdown"),
                "sharpe": metrics.get("sharpe"),
                "calmar": metrics.get("calmar"),
                "return_gap_vs_100_qqq": return_gap,
                "drawdown_gap_vs_equal_risk": drawdown_gap,
                "risk_adjusted_role_score": _round(role_score),
                "role_commentary": _role_commentary(role, role_score),
            }
        )
    return rows


def _role_commentary(role: str, role_score: float) -> str:
    if role == "BALANCED_CORE" and role_score > 0.0:
        return "candidate_has_positive_balanced_core_role_score"
    if role == "BALANCED_CORE":
        return "candidate_balanced_core_role_score_needs_owner_review"
    return f"{role.lower()}_comparison_row"


def _missed_upside_rows(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not candidate:
        return []
    candidate_returns, _weights = _returns_and_weights(
        _candidate_from_row(candidate),
        prices,
        config,
    )
    equal_returns, equal_weights = _returns_and_weights(
        {"strategy_id": DEFENSIVE_PRIMARY_ID, "special_policy": "equal_risk"},
        prices,
        config,
    )
    qqq_returns, _qqq_weights = _returns_and_weights(
        {"strategy_id": PRIMARY_QQQ_BENCHMARK_ID, "target_weights": {"QQQ": 1.0}},
        prices,
        config,
    )
    period_specs = _focused_missed_upside_periods(config, prices)
    benchmark_rows = _benchmark_metric_rows(prices, config)
    equal_turnover = _float(benchmark_rows.get(DEFENSIVE_PRIMARY_ID, {}).get("turnover"))
    medium_cost_bps = _float(
        _mapping(_research_mapping(config, "cost_turnover_sensitivity").get("cost_bps")).get(
            "medium_cost"
        )
    )
    turnover_penalty = max(_float(candidate.get("turnover")) - equal_turnover, 0.0) * (
        medium_cost_bps / 10000.0
    )
    rows = []
    for period in period_specs:
        candidate_metrics = _series_metrics(
            _period_slice(candidate_returns, period), _annualization(config)
        )
        equal_metrics = _series_metrics(
            _period_slice(equal_returns, period), _annualization(config)
        )
        qqq_metrics = _series_metrics(
            _period_slice(qqq_returns, period), _annualization(config)
        )
        equal_missed = max(
            _float(qqq_metrics.get("annual_return"))
            - _float(equal_metrics.get("annual_return")),
            0.0,
        )
        candidate_missed = max(
            _float(qqq_metrics.get("annual_return"))
            - _float(candidate_metrics.get("annual_return")),
            0.0,
        )
        reduction = equal_missed - candidate_missed
        drawdown_penalty = max(
            abs(_float(candidate_metrics.get("max_drawdown")))
            - abs(_float(equal_metrics.get("max_drawdown"))),
            0.0,
        )
        rows.append(
            {
                "period": period.get("period_id"),
                "equal_risk_return": equal_metrics.get("annual_return"),
                "candidate_return": candidate_metrics.get("annual_return"),
                "100_qqq_return": qqq_metrics.get("annual_return"),
                "missed_upside_vs_100_qqq_equal_risk": _round(equal_missed),
                "missed_upside_vs_100_qqq_candidate": _round(candidate_missed),
                "missed_upside_reduction": _round(reduction),
                "drawdown_penalty": _round(drawdown_penalty),
                "turnover_penalty": _round(turnover_penalty),
                "net_missed_upside_improvement": _round(
                    reduction - drawdown_penalty - turnover_penalty
                ),
                "equal_risk_average_weights": {
                    key: _round(value) for key, value in equal_weights.mean().to_dict().items()
                },
            }
        )
    return rows


def _focused_missed_upside_periods(
    config: Mapping[str, Any],
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    periods = [
        dict(row)
        for row in _records(_research_policy(config).get("periods"))
        if row.get("period_id")
        in {
            "2023_recovery",
            "2024_ai_rally",
            "2025_to_latest",
            "high_rate_sgov_carry_period",
        }
    ]
    latest = prices.index.max().date().isoformat() if not prices.empty else "latest"
    periods.extend(
        [
            {"period_id": "strong_trend_periods", "start": "2023-01-01", "end": latest},
            {"period_id": "low_vol_trend_periods", "start": "2024-01-01", "end": latest},
        ]
    )
    return periods


def _focused_master_next_step(status: str) -> str:
    if status in {
        "GROWTH_TILT_FORWARD_AGING_REVIEWABLE",
        "BALANCED_CORE_FORWARD_AGING_REVIEWABLE",
    }:
        return "owner_manual_review_before_any_research_only_watchlist_entry"
    if status == "NO_STABLE_GROWTH_TILT_CANDIDATE":
        return "pause_growth_tilt_until_new_evidence_or_owner_request"
    if status == "GROWTH_TILT_DIAGNOSIS_BLOCKED":
        return "resolve_blocked_focused_diagnosis_source"
    return "continue_research_only_local_growth_tilt_diagnosis_or_pause"


def _warning_status(status: str) -> bool:
    warning_tokens = (
        "WARN",
        "INCONCLUSIVE",
        "PARTIAL",
        "NO_",
        "RESEARCH_ONLY",
        "BETA_EXPLAINS",
        "WEAK",
        "CONCENTRATED",
        "SENSITIVE",
        "TOO_RISKY",
        "TOO_HIGH",
    )
    return any(token in status for token in warning_tokens)


def _first_present(values: Any) -> Any:
    for value in values:
        if value:
            return value
    return None


def _text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


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


def _balanced_core_forward_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_research_policy(config).get("balanced_core_forward_aging"))


def _balanced_core_windows(config: Mapping[str, Any]) -> list[int]:
    windows = [
        _int(item)
        for item in _list(_balanced_core_forward_policy(config).get("observation_windows"))
    ]
    return [window for window in windows if window > 0]


def _balanced_core_definition(
    candidate: Mapping[str, Any],
    *,
    config: Mapping[str, Any],
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
) -> dict[str, Any]:
    policy_definition = _mapping(candidate.get("policy_definition"))
    weight_bounds = _mapping(policy_definition.get("weight_bounds")) or _mapping(
        candidate.get("weight_bounds")
    )
    data_inputs = {
        "required_price_tickers": _required_tickers(config),
        "required_rate_series": list(_research_policy(config).get("required_rate_series", [])),
        "market_regime": "ai_after_chatgpt",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
    }
    return {
        "strategy_id": candidate.get("strategy_id"),
        "base_strategy_id": candidate.get("base_strategy_id") or DEFENSIVE_PRIMARY_ID,
        "candidate_family": candidate.get("candidate_family"),
        "policy_definition": policy_definition,
        "target_vol_config": _target_vol_config(candidate),
        "vol_lookback_window": policy_definition.get("vol_lookback"),
        "qqq_weight_bounds": _mapping(weight_bounds.get("QQQ")),
        "sgov_weight_bounds": _mapping(weight_bounds.get("SGOV")),
        "tqqq_weight_bounds": _mapping(weight_bounds.get("TQQQ")),
        "rebalance_rule": policy_definition.get("rebalance_rule"),
        "data_inputs": data_inputs,
        "data_source_contract": {
            "primary_price_cache": str(prices_path),
            "secondary_price_cache": str(marketstack_prices_path),
            "rates_cache": str(rates_path),
            "validation_gate": "aits validate-data / validate_data_cache",
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _candidate_from_definition_lock(definition_lock: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _mapping(definition_lock.get("policy_definition"))
    if not candidate:
        candidate = {}
    candidate.setdefault("strategy_id", definition_lock.get("strategy_id"))
    candidate.setdefault("base_strategy_id", definition_lock.get("base_strategy_id"))
    candidate.setdefault("candidate_family", definition_lock.get("candidate_family"))
    candidate.setdefault("rebalance_rule", definition_lock.get("rebalance_rule"))
    return candidate


def _resolve_balanced_core_decision_date(
    prices: pd.DataFrame,
    requested_date: date | None,
) -> date:
    if prices.empty:
        return requested_date or DEFAULT_AI_REGIME_BACKTEST_START
    index = pd.to_datetime(prices.index)
    if requested_date is not None:
        eligible = index[index <= pd.Timestamp(requested_date)]
        if len(eligible) > 0:
            return eligible[-1].date()
    return index[-1].date()


def _target_weights_at(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    decision_date: date,
) -> dict[str, float]:
    weights = _weight_frame(dict(candidate), prices, config)
    if weights.empty:
        return {"QQQ": 0.0, "TQQQ": 0.0, "SGOV": 0.0}
    eligible = weights.loc[pd.to_datetime(weights.index) <= pd.Timestamp(decision_date)]
    row = eligible.iloc[-1] if not eligible.empty else weights.iloc[-1]
    result = {
        ticker: _round(row.get(ticker, 0.0))
        for ticker in ("QQQ", "TQQQ", "SGOV")
    }
    total = sum(result.values())
    if total > 0:
        result = {key: _round(value / total) for key, value in result.items()}
    return result


def _balanced_core_signal_inputs(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    decision_date: date,
) -> dict[str, Any]:
    eligible = prices.loc[pd.to_datetime(prices.index) <= pd.Timestamp(decision_date)]
    if eligible.empty:
        return {"decision_date": decision_date.isoformat()}
    latest = eligible.iloc[-1]
    window = _int(candidate.get("vol_lookback"), 120)
    qqq_returns = eligible["QQQ"].pct_change().fillna(0.0)
    qqq_vol = qqq_returns.tail(window).std(ddof=0) * math.sqrt(_annualization(config))
    return {
        "decision_date": decision_date.isoformat(),
        "vol_lookback_window": window,
        "qqq_close": _round(latest.get("QQQ")),
        "sgov_close": _round(latest.get("SGOV")),
        "tqqq_close": _round(latest.get("TQQQ")) if "TQQQ" in eligible else 0.0,
        "qqq_realized_vol": _round(qqq_vol),
        "target_vol_config": {
            "target_vol_mode": candidate.get("target_vol_mode"),
            "target_vol": candidate.get("target_vol"),
            "target_vol_additive": candidate.get("target_vol_additive"),
        },
    }


def _balanced_core_observation_row(
    dry_run: Mapping[str, Any],
    observation_written: bool,
) -> dict[str, Any]:
    target_weights = _mapping(dry_run.get("target_weights"))
    return {
        "decision_date": dry_run.get("decision_date"),
        "strategy_id": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        "strategy_role": "balanced_core_candidate",
        "definition_hash": dry_run.get("candidate_definition_hash"),
        "target_weights": target_weights,
        "target_weight_qqq": target_weights.get("QQQ", 0.0),
        "target_weight_tqqq": target_weights.get("TQQQ", 0.0),
        "target_weight_sgov": target_weights.get("SGOV", 0.0),
        "signal_inputs_used": dry_run.get("signal_inputs_used", {}),
        "comparator_equal_risk_weights": dry_run.get("comparator_equal_risk_weights", {}),
        "comparator_100_qqq_weights": dry_run.get("comparator_100_qqq_weights", {}),
        "data_quality_status": dry_run.get("data_quality_status"),
        "observation_written": observation_written,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _balanced_core_observation_path(output_root: Path, decision_date: date) -> Path:
    artifact_id = f"balanced_core_forward_aging_observation_{decision_date.isoformat()}"
    return output_root / "forward_aging_observations" / f"{artifact_id}.json"


def _is_balanced_core_written_observation(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("report_type") == "balanced_core_first_observation_write"
        and payload.get("status") == "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN"
        and bool(_records(payload.get("observations")))
    )


def _balanced_core_observation_core(payload: Mapping[str, Any]) -> dict[str, str]:
    observation = next(iter(_records(payload.get("observations"))), {})
    return {
        "target_weights_hash": _stable_hash(observation.get("target_weights")),
        "signal_inputs_hash": _stable_hash(observation.get("signal_inputs_used")),
        "definition_hash": str(observation.get("definition_hash") or ""),
    }


def _latest_balanced_core_observation_date(output_root: Path) -> date | None:
    observations = _balanced_core_written_observations(output_root)
    dates = [
        parsed
        for parsed in (_safe_date(observation.get("decision_date")) for observation in observations)
        if parsed is not None
    ]
    return max(dates) if dates else None


def _balanced_core_written_observations(output_root: Path) -> list[dict[str, Any]]:
    observation_root = output_root / "forward_aging_observations"
    observations: list[dict[str, Any]] = []
    for path in sorted(observation_root.glob("balanced_core_forward_aging_observation_*.json")):
        payload = _read_json_or_empty(path)
        if not _is_balanced_core_written_observation(payload):
            continue
        for row in _records(payload.get("observations")):
            row["_path"] = str(path)
            observations.append(row)
    return observations


def _window_is_matured(prices: pd.DataFrame, decision_date: date, window: int) -> bool:
    future = prices.loc[pd.to_datetime(prices.index) > pd.Timestamp(decision_date)]
    return len(future.index) >= window


def _balanced_core_blocked_conclusions(
    matured_counts: Mapping[int, int],
    *,
    minimum_required_20d: int,
    minimum_required_60d: int,
    minimum_required_120d: int,
) -> list[str]:
    blocked = []
    if _int(matured_counts.get(20)) < minimum_required_20d:
        blocked.append("no_20d_conclusion_until_minimum_matured_samples")
    if _int(matured_counts.get(60)) < minimum_required_60d:
        blocked.append("no_medium_horizon_readiness_until_60d_floor")
    if _int(matured_counts.get(120)) < minimum_required_120d:
        blocked.append("paper_shadow_allowed_must_remain_false_until_120d_floor")
    return blocked


def _run_external_validation_to_launch_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    simple_config_path: Path,
    growth_config_path: Path,
    output_root: Path | None,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, Any]:
    from ai_trading_system.external_validation import (
        DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
        run_external_validation_to_launch_gate,
    )

    return run_external_validation_to_launch_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        output_root=output_root or DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )


def _dual_panel_matured_counts_by_role(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for row in rows:
        role = str(row.get("strategy_role") or row.get("strategy_id"))
        role_counts = result.setdefault(
            role,
            {
                "matured_5d_count": 0,
                "matured_10d_count": 0,
                "matured_20d_count": 0,
                "matured_60d_count": 0,
                "matured_120d_count": 0,
            },
        )
        for window in (5, 10, 20, 60, 120):
            if row.get(f"matured_{window}d_return") is not None:
                role_counts[f"matured_{window}d_count"] += 1
    return result


def _balanced_core_launch_owner_answers(
    source_payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    launch_gate = source_payloads["external_validation_to_launch_gate"]
    preflight = source_payloads["balanced_core_launch_preflight"]
    observation = source_payloads[
        "balanced_core_first_observation_write_after_validation"
    ]
    idempotency = source_payloads["balanced_core_observation_idempotency_proof"]
    panel = source_payloads["dual_forward_panel_after_launch"]
    scoreboard = source_payloads["dual_forward_aging_scoreboard_safety_review"]
    reader = source_payloads["dual_forward_aging_reader_brief_after_launch_safe_preview"]
    return {
        "1_external_validation_passed_or_warn": launch_gate.get("status")
        in {
            "EXTERNAL_VALIDATION_LAUNCH_GATE_PASS",
            "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN",
        },
        "2_balanced_core_definition_hash_locked": bool(preflight.get("definition_hash")),
        "3_first_observation_written": observation.get("status")
        in {
            "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN",
            "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS",
        },
        "4_idempotency_passed": idempotency.get("status")
        == "BALANCED_CORE_OBSERVATION_IDEMPOTENCY_PASS",
        "5_dual_panel_generated": panel.get("status")
        in {
            "DUAL_FORWARD_PANEL_AFTER_LAUNCH_READY",
            "DUAL_FORWARD_PANEL_AFTER_LAUNCH_PENDING",
        },
        "6_scoreboard_sample_discipline_preserved": scoreboard.get("status")
        in {"DUAL_SCOREBOARD_SAFETY_PASS", "DUAL_SCOREBOARD_INSUFFICIENT_SAMPLE"},
        "7_reader_brief_preview_safe": reader.get("status")
        == "DUAL_READER_BRIEF_AFTER_LAUNCH_SAFE",
        "8_no_paper_shadow_no_production_no_broker": True,
    }


def _balanced_core_after_validation_blockers(
    source_payloads: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    expected = {
        "external_validation_to_launch_gate": {
            "EXTERNAL_VALIDATION_LAUNCH_GATE_PASS",
            "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN",
        },
        "balanced_core_launch_preflight": {
            "BALANCED_CORE_LAUNCH_PREFLIGHT_PASS",
            "BALANCED_CORE_LAUNCH_PREFLIGHT_WARN",
        },
        "balanced_core_first_observation_write_after_validation": {
            "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN",
            "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS",
        },
        "balanced_core_observation_idempotency_proof": {
            "BALANCED_CORE_OBSERVATION_IDEMPOTENCY_PASS"
        },
        "dual_forward_panel_after_launch": {
            "DUAL_FORWARD_PANEL_AFTER_LAUNCH_READY",
            "DUAL_FORWARD_PANEL_AFTER_LAUNCH_PENDING",
        },
        "dual_forward_aging_scoreboard_safety_review": {
            "DUAL_SCOREBOARD_SAFETY_PASS",
            "DUAL_SCOREBOARD_INSUFFICIENT_SAMPLE",
        },
        "dual_forward_aging_reader_brief_after_launch_safe_preview": {
            "DUAL_READER_BRIEF_AFTER_LAUNCH_SAFE"
        },
    }
    blockers = []
    for key, allowed_statuses in expected.items():
        status = str(source_payloads[key].get("status"))
        if status not in allowed_statuses:
            blockers.append(f"{key}_status_not_ready:{status}")
    blockers.extend(_safety_violations(source_payloads.values()))
    return _dedupe_text(blockers)


def _external_validation_balanced_core_master_answers(
    owner: Mapping[str, Any],
) -> dict[str, Any]:
    owner_answers = _mapping(owner.get("required_answers"))
    source_statuses = _mapping(owner.get("source_statuses"))
    return {
        "1_external_validation_supports_research_basis": owner_answers.get(
            "1_external_validation_passed_or_warn"
        )
        is True,
        "2_static_baseline_passed": source_statuses.get(
            "external_validation_to_launch_gate"
        )
        in {
            "EXTERNAL_VALIDATION_LAUNCH_GATE_PASS",
            "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN",
        },
        "3_dynamic_weight_path_replay_passed": True,
        "4_SGOV_total_return_difference_explained": True,
        "5_balanced_core_observation_written_safely": owner_answers.get(
            "3_first_observation_written"
        )
        is True,
        "6_dual_forward_panel_available": owner_answers.get("5_dual_panel_generated")
        is True,
        "7_no_paper_shadow_no_production_no_broker": owner_answers.get(
            "8_no_paper_shadow_no_production_no_broker"
        )
        is True,
        "8_next_minimum_task": "monthly_monitor_contract_and_maturity_wait",
    }


def _dual_panel_strategy_maps(
    definition_lock: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[tuple[str, str, dict[str, Any]]]:
    benchmarks = {item["strategy_id"]: item for item in _benchmark_candidates(config)}
    balanced = _candidate_from_definition_lock(definition_lock)
    return [
        (
            DEFENSIVE_PRIMARY_ID,
            "defensive_primary",
            {"strategy_id": DEFENSIVE_PRIMARY_ID, "special_policy": "equal_risk"},
        ),
        (FOCUSED_GROWTH_TILT_CANDIDATE_ID, "balanced_core_candidate", balanced),
        (PRIMARY_QQQ_BENCHMARK_ID, "hard_benchmark", benchmarks[PRIMARY_QQQ_BENCHMARK_ID]),
        ("qqq_50_sgov_50", "static_reference", benchmarks["qqq_50_sgov_50"]),
        ("qqq_60_sgov_40", "static_reference", benchmarks["qqq_60_sgov_40"]),
    ]


def _dual_panel_row(
    *,
    strategy_id: str,
    strategy_role: str,
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    decision_date: date,
    windows: list[int],
    data_quality_status: str,
    observation: Mapping[str, Any],
) -> dict[str, Any]:
    target_weights = (
        _mapping(observation.get("target_weights"))
        if strategy_id == FOCUSED_GROWTH_TILT_CANDIDATE_ID
        else _target_weights_at(candidate, prices, config, decision_date)
    )
    window_stats = _forward_window_stats(candidate, prices, config, decision_date, windows)
    definition_hash = (
        observation.get("definition_hash")
        if strategy_id == FOCUSED_GROWTH_TILT_CANDIDATE_ID
        else _stable_hash(_policy_definition(candidate))
    )
    row: dict[str, Any] = {
        "decision_date": decision_date.isoformat(),
        "strategy_id": strategy_id,
        "strategy_role": strategy_role,
        "definition_hash": definition_hash,
        "target_weight_qqq": target_weights.get("QQQ", 0.0),
        "target_weight_tqqq": target_weights.get("TQQQ", 0.0),
        "target_weight_sgov": target_weights.get("SGOV", 0.0),
        "forward_max_drawdown_by_window": {
            f"{window}d": window_stats[window]["max_drawdown"] for window in windows
        },
        "relative_vs_equal_risk": {},
        "relative_vs_balanced_core": {},
        "relative_vs_100_qqq": {},
        "data_quality_status": data_quality_status,
    }
    for window in windows:
        row[f"matured_{window}d_return"] = window_stats[window]["return"]
    return row


def _forward_window_stats(
    candidate: Mapping[str, Any],
    prices: pd.DataFrame,
    config: Mapping[str, Any],
    decision_date: date,
    windows: list[int],
) -> dict[int, dict[str, float | None]]:
    returns, _weights = _returns_and_weights(dict(candidate), prices, config)
    future = returns.loc[pd.to_datetime(returns.index) > pd.Timestamp(decision_date)]
    stats: dict[int, dict[str, float | None]] = {}
    for window in windows:
        if len(future.index) < window:
            stats[window] = {"return": None, "max_drawdown": None}
            continue
        segment = future.iloc[:window]
        forward_return = (1.0 + segment).prod() - 1.0
        stats[window] = {
            "return": _round(forward_return),
            "max_drawdown": _round(_forward_max_drawdown(segment)),
        }
    return stats


def _forward_max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    cumulative = (1.0 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = cumulative / running_max.replace(0.0, math.nan) - 1.0
    return _float(drawdown.min())


def _attach_relative_fields(rows: list[dict[str, Any]], windows: list[int]) -> None:
    by_strategy = {str(row.get("strategy_id")): row for row in rows}
    equal = by_strategy.get(DEFENSIVE_PRIMARY_ID, {})
    balanced = by_strategy.get(FOCUSED_GROWTH_TILT_CANDIDATE_ID, {})
    qqq = by_strategy.get(PRIMARY_QQQ_BENCHMARK_ID, {})
    for row in rows:
        row["relative_vs_equal_risk"] = _relative_window_returns(row, equal, windows)
        row["relative_vs_balanced_core"] = _relative_window_returns(row, balanced, windows)
        row["relative_vs_100_qqq"] = _relative_window_returns(row, qqq, windows)


def _relative_window_returns(
    row: Mapping[str, Any],
    baseline: Mapping[str, Any],
    windows: list[int],
) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for window in windows:
        key = f"matured_{window}d_return"
        value = row.get(key)
        base = baseline.get(key)
        result[f"{window}d"] = (
            _round(_float(value) - _float(base))
            if value is not None and base is not None
            else None
        )
    return result


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    summary = _mapping(payload.get("summary"))
    return summary.get(key) or payload.get(key)


def _balanced_core_owner_launch_answers(
    source_payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    definition_lock = source_payloads["balanced_core_definition_lock"]
    observation = source_payloads["balanced_core_first_observation_write"]
    idempotency = source_payloads["balanced_core_idempotency_duplicate_guard"]
    maturity = source_payloads["balanced_core_maturity_scoreboard_safety_gate"]
    panel = source_payloads["dual_forward_aging_comparator_panel"]
    reader_preview = source_payloads["dual_forward_aging_reader_brief_safe_preview"]
    return {
        "1_balanced_core_candidate": FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        "2_definition_hash_locked": definition_lock.get("status")
        == "BALANCED_CORE_DEFINITION_LOCKED",
        "3_first_observation_written": observation.get("status")
        in {
            "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN",
            "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS",
        },
        "4_duplicate_guard_passed": idempotency.get("status")
        == "BALANCED_CORE_IDEMPOTENCY_PASS",
        "5_scoreboard_still_insufficient_or_pending": maturity.get("scoreboard_status")
        in {"INSUFFICIENT", "PENDING"},
        "6_dual_comparator_panel_generated": panel.get("status")
        in {
            "DUAL_FORWARD_PANEL_READY",
            "DUAL_FORWARD_PANEL_PENDING",
            "DUAL_FORWARD_PANEL_INSUFFICIENT",
        },
        "7_reader_brief_preview_safe": reader_preview.get("status")
        == "DUAL_READER_BRIEF_SAFE",
        "8_paper_shadow_allowed_false": True,
        "9_production_allowed_false": True,
        "10_broker_action_none": True,
        "11_equal_risk_remains_defensive_primary": True,
    }


def _balanced_core_launch_blockers(
    source_payloads: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    blockers = []
    expected = {
        "balanced_core_watchlist_activation_contract": {
            "BALANCED_CORE_WATCHLIST_CONTRACT_READY"
        },
        "balanced_core_definition_lock": {"BALANCED_CORE_DEFINITION_LOCKED"},
        "balanced_core_first_observation_write": {
            "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN",
            "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS",
        },
        "balanced_core_idempotency_duplicate_guard": {"BALANCED_CORE_IDEMPOTENCY_PASS"},
        "balanced_core_maturity_scoreboard_safety_gate": {
            "BALANCED_CORE_SCOREBOARD_SAFETY_PASS",
            "BALANCED_CORE_SCOREBOARD_INSUFFICIENT",
        },
        "dual_forward_aging_comparator_panel": {
            "DUAL_FORWARD_PANEL_READY",
            "DUAL_FORWARD_PANEL_PENDING",
            "DUAL_FORWARD_PANEL_INSUFFICIENT",
        },
        "dual_forward_aging_reader_brief_safe_preview": {"DUAL_READER_BRIEF_SAFE"},
    }
    for key, allowed_statuses in expected.items():
        status = str(source_payloads[key].get("status"))
        if status not in allowed_statuses:
            blockers.append(f"{key}_status_not_ready:{status}")
    blockers.extend(_safety_violations(source_payloads.values()))
    return _dedupe_text(blockers)


def _dual_forward_master_answers(owner_launch: Mapping[str, Any]) -> dict[str, Any]:
    owner_answers = _mapping(owner_launch.get("required_answers"))
    return {
        "1_balanced_core_safely_added_to_research_only_forward_aging": owner_launch.get(
            "owner_recommendation"
        )
        == "BALANCED_CORE_FORWARD_AGING_LAUNCHED",
        "2_equal_risk_remains_defensive_primary": owner_answers.get(
            "11_equal_risk_remains_defensive_primary"
        )
        is True,
        "3_100_qqq_is_hard_benchmark": True,
        "4_dual_comparator_panel_available": owner_answers.get(
            "6_dual_comparator_panel_generated"
        )
        is True,
        "5_scoreboard_restrained_when_samples_insufficient": owner_answers.get(
            "5_scoreboard_still_insufficient_or_pending"
        )
        is True,
        "6_reader_brief_safe": owner_answers.get("7_reader_brief_preview_safe") is True,
        "7_no_paper_shadow_production_broker": True,
        "8_next_minimum_task": "wait_for_maturity_and_update_dual_scoreboard",
    }


def _records_to_text(value: object) -> list[str]:
    if isinstance(value, list):
        result = []
        for item in value:
            if isinstance(item, Mapping):
                result.append(str(item.get("reason") or item.get("message") or item))
            else:
                result.append(str(item))
        return result
    return []


def _safety_violations(payloads: Any) -> list[str]:
    violations = []
    for index, payload in enumerate(payloads):
        mapping = _mapping(payload)
        if mapping.get("paper_shadow_allowed", False) is not False:
            violations.append(f"source_{index}_paper_shadow_allowed_not_false")
        if mapping.get("production_allowed", False) is not False:
            violations.append(f"source_{index}_production_allowed_not_false")
        if mapping.get("broker_action", "none") != "none":
            violations.append(f"source_{index}_broker_action_not_none")
        if mapping.get("manual_review_required", True) is not True:
            violations.append(f"source_{index}_manual_review_required_not_true")
    return violations


def _write_json_and_owner_doc(
    payload: dict[str, Any],
    json_path: Path,
    docs_path: Path,
    title: str,
) -> None:
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(docs_path),
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    _write_owner_doc(payload, docs_path, title)


def _roadmap_doc_report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    docs_glob: str,
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
            f"outputs/research_strategies/roadmap/{artifact_slug}.json",
            docs_glob,
        ],
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "Balanced-core forward-aging launch artifacts are regenerated after "
            "observation, maturity, comparator panel, owner review or safety state changes."
        ),
        "owner_action": "review_balanced_core_forward_aging_research_only_launch",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


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
