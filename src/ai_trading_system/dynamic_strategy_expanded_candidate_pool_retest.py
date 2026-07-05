from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    _candidate_target_weights,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DATA_QUALITY_GATE_REASON as SOURCE_2385_DATA_QUALITY_REASON,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_OUTPUT_ROOT,
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH,
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH,
    DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH,
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH,
    DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH,
    DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    NEXT_ROUTE as SOURCE_2385_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    READY_STATUS as SOURCE_2385_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    PRIMARY_EXECUTION_CADENCE,
    _scenario_policy_for_sensitivity,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    _portfolio_return_series,
    _signal_validity_profile,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    BEST_GUARDED_VARIANT,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    READY_STATUS as SOURCE_2384_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DECISION_ACCEPT_RESEARCH_ONLY,
    DECISION_CONTINUE_OPTIMIZATION,
    DECISION_OWNER_REVIEW,
    DECISION_REJECT,
    _cost_stress_scenarios,
    _execution_constraint_scenarios,
    _float,
    _full_sample_result_row,
    _regime_slice_definitions,
    _slice_rows,
    _static_bundle,
    _stress_result_row,
    _targeted_retest_policy,
    _time_slice_definitions,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    BEST_LOWER_TURNOVER_VARIANT,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    RANKING_TOP_CANDIDATE,
    _bounded_step_delta,
    _guarded_target_weights,
    _lower_turnover_cooldown_weights,
    _normalized_qqq_sgov,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    READY_STATUS as SOURCE_2383_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    BASE_CANDIDATE_ID,
    RANKING_TOP_REFERENCE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    READY_STATUS as SOURCE_2379_READY_STATUS,
)
from ai_trading_system.execution_semantics import (
    AI_REGIME_SUMMARY,
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    _actual_position_path,
    _attach_path_return_columns,
    _file_sha256,
    _load_execution_price_matrix,
    _load_policy_registry,
    _policies_by_id,
    _policy_cost_bps,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _data_quality_gate,
    _load_registry,
)

TASK_ID = "TRADING-2386"
TASK_REGISTER_ID = (
    "TRADING-2386_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING"
)
REPORT_TYPE = "dynamic_strategy_expanded_candidate_pool_retest"
SCHEMA_VERSION = "dynamic_strategy_expanded_candidate_pool_retest_and_screening.v1"
READY_STATUS = "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_"
    "BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2387_Dynamic_Strategy_Expanded_Candidate_Owner_Review_And_Next_"
    "Research_Decision"
)
DECISION_DEPRECATED = "DEPRECATED_BY_EXPANDED_SCREENING"
REFERENCE_CANDIDATES: tuple[str, ...] = (
    "static_baseline",
    RANKING_TOP_CANDIDATE,
    BASE_CANDIDATE_ID,
    BEST_LOWER_TURNOVER_VARIANT,
    BEST_GUARDED_VARIANT,
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2379",
    "TRADING-2383",
    "TRADING-2384",
    "TRADING-2385",
)
COMPARISON_CADENCES: tuple[str, ...] = (
    PRIMARY_EXECUTION_CADENCE,
    "cooldown_limited_event_driven",
    "signal_event_driven",
)
# TRADING-2386 pilot screening thresholds. These are not promotion policy; they
# only classify research candidates for the owner-review route requested in 2386.
TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN = 0.40
REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN = 0.30
DRAWDOWN_WORSE_TOLERANCE = 0.02
TURNOVER_BUDGET_MAX_MONTHLY = 1.0
RETURN_ADVANTAGE_RETAINED_MIN = 0.50
OBSERVATION_TIME_SLICE_PASS_RATE_MIN = 0.60
OBSERVATION_REGIME_SLICE_PASS_RATE_MIN = 0.50
EXPANDED_STEP_DELTA_CAP = 0.16
CONSERVATIVE_STEP_DELTA_CAP = 0.10
RISK_OFF_DRAWDOWN_THRESHOLD = -0.08
SEVERE_DRAWDOWN_THRESHOLD = -0.12
ROLLING_VOL_WINDOW = 20
TREND_WINDOW = 60
RECOVERY_WINDOW = 20

DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_OUTPUT_ROOT
    / "candidate_pool_expansion_plan.json"
)


def run_dynamic_strategy_expanded_candidate_pool_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_candidate_pool_expansion_plan_path: Path = (
        DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH
    ),
    source_owner_review_path: Path = DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH,
    source_guarded_variant_retest_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH
    ),
    source_guarded_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH
    ),
    source_guarded_decision_update_path: Path = DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH,
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_DOCS_ROOT,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_candidate_pool_expansion_plan_path=(
            source_candidate_pool_expansion_plan_path
        ),
        source_owner_review_path=source_owner_review_path,
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
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
        sources=sources,
    )
    if not bool(data_quality.get("passed")):
        payload["status"] = BLOCKED_DATA_QUALITY_STATUS
        payload.update(_blocked_sections("data_quality_gate_failed", sources))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload
    if not bool(sources["source_ready_for_expanded_candidate_retest"]):
        payload["status"] = BLOCKED_SOURCE_STATUS
        payload.update(_blocked_sections("source_artifact_validation_failed", sources))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    prices = _load_execution_price_matrix(
        prices_path,
        config,
        start_date=resolved_start,
        end_date=end_date,
    )
    policies = _policies_by_id(_load_policy_registry(policy_registry_path))
    candidate_plan = _candidate_universe_from_2385(sources)
    retest_policy = _expanded_retest_policy(candidate_plan)
    retest = _run_expanded_candidate_retest(
        prices=prices,
        policies=policies,
        candidate_plan=candidate_plan,
    )
    ranking = _expanded_candidate_ranking(retest=retest, candidate_plan=candidate_plan)
    family_screening = _signal_family_screening(ranking=ranking, retest=retest)
    decision_update = _decision_update(
        ranking=ranking,
        family_screening=family_screening,
        retest=retest,
    )
    summary = _summary_findings(decision_update)
    payload.update(
        {
            "expanded_candidate_retest_policy": retest_policy,
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "comparison_cadences": list(COMPARISON_CADENCES),
            "monthly_rebalance": {
                "allowed_for_reference": True,
                "allowed_for_primary_decision": False,
            },
            "reference_candidates": list(REFERENCE_CANDIDATES),
            "reference_candidate_count": len(REFERENCE_CANDIDATES),
            "new_candidates_tested": list(candidate_plan["new_candidate_ids"]),
            "new_candidates_tested_count": len(candidate_plan["new_candidate_ids"]),
            "total_candidates_tested_count": len(REFERENCE_CANDIDATES)
            + len(candidate_plan["new_candidate_ids"]),
            "signal_families_tested": list(candidate_plan["signal_families"]),
            "signal_families_tested_count": len(candidate_plan["signal_families"]),
            "expanded_candidate_retest_result": retest[
                "expanded_candidate_retest_result"
            ],
            "expanded_candidate_ranking": ranking,
            "signal_family_screening": family_screening,
            "time_slice_matrix": retest["time_slice_matrix"],
            "regime_slice_matrix": retest["regime_slice_matrix"],
            "time_regime_slice_matrix": retest["time_slice_matrix"]
            + retest["regime_slice_matrix"],
            "cost_stress_result": retest["cost_stress_result"],
            "turnover_constraint_result": retest["turnover_constraint_result"],
            "cadence_comparison_result": retest["cadence_comparison_result"],
            "decision_update": decision_update,
            "summary_findings": summary,
            "expanded_candidate_retest_ready": True,
            "expanded_candidate_ranking_ready": True,
            "signal_family_screening_ready": True,
            "time_slice_matrix_ready": True,
            "regime_slice_matrix_ready": True,
            "cost_stress_result_ready": True,
            "turnover_constraint_result_ready": True,
            "decision_update_ready": True,
            "best_candidate_after_expanded_screening": decision_update[
                "best_candidate_after_expanded_screening"
            ],
            "best_candidate_decision": decision_update["best_candidate_decision"],
            "best_signal_family": decision_update["best_signal_family"],
            "candidate_ready_for_research_only_observation": decision_update[
                "candidate_ready_for_research_only_observation"
            ],
            "recommended_next_research_task": NEXT_ROUTE,
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_candidate_pool_expansion_plan_path: Path,
    source_owner_review_path: Path,
    source_guarded_variant_retest_path: Path,
    source_guarded_variant_ranking_path: Path,
    source_guarded_decision_update_path: Path,
    source_variant_retest_path: Path,
    source_optimized_variant_ranking_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
) -> dict[str, Any]:
    candidate_pool_plan = _load_json_document(source_candidate_pool_expansion_plan_path)
    owner_review = _load_json_document(source_owner_review_path)
    guarded_variant_retest = _load_json_document(source_guarded_variant_retest_path)
    guarded_variant_ranking = _load_json_document(source_guarded_variant_ranking_path)
    guarded_decision_update = _load_json_document(source_guarded_decision_update_path)
    variant_retest = _load_json_document(source_variant_retest_path)
    optimized_variant_ranking = _load_json_document(
        source_optimized_variant_ranking_path
    )
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    source_status = {
        "candidate_pool_plan": candidate_pool_plan.get("status"),
        "owner_review": owner_review.get("status"),
        "guarded_variant_retest": guarded_variant_retest.get("status"),
        "guarded_variant_ranking": guarded_variant_ranking.get("status"),
        "guarded_decision_update": guarded_decision_update.get("status"),
        "variant_retest": variant_retest.get("status"),
        "optimized_variant_ranking": optimized_variant_ranking.get("status"),
        "candidate_ranking": candidate_ranking.get("status"),
        "sensitivity_result": sensitivity_result.get("status"),
        "sensitivity_decision_update": sensitivity_decision_update.get("status"),
    }
    source_files = {
        "candidate_pool_plan": source_candidate_pool_expansion_plan_path,
        "owner_review": source_owner_review_path,
        "guarded_variant_retest": source_guarded_variant_retest_path,
        "guarded_variant_ranking": source_guarded_variant_ranking_path,
        "guarded_decision_update": source_guarded_decision_update_path,
        "variant_retest": source_variant_retest_path,
        "optimized_variant_ranking": source_optimized_variant_ranking_path,
        "candidate_ranking": source_candidate_ranking_path,
        "sensitivity_result": source_sensitivity_result_path,
        "sensitivity_decision_update": source_sensitivity_decision_update_path,
    }
    validation_errors = _source_validation_errors(
        source_status=source_status,
        candidate_pool_plan=candidate_pool_plan,
        owner_review=owner_review,
        guarded_variant_retest=guarded_variant_retest,
        guarded_variant_ranking=guarded_variant_ranking,
        guarded_decision_update=guarded_decision_update,
        variant_retest=variant_retest,
        optimized_variant_ranking=optimized_variant_ranking,
        candidate_ranking=candidate_ranking,
        sensitivity_result=sensitivity_result,
        sensitivity_decision_update=sensitivity_decision_update,
    )
    return {
        "candidate_pool_plan": candidate_pool_plan,
        "owner_review": owner_review,
        "guarded_variant_retest": guarded_variant_retest,
        "guarded_variant_ranking": guarded_variant_ranking,
        "guarded_decision_update": guarded_decision_update,
        "variant_retest": variant_retest,
        "optimized_variant_ranking": optimized_variant_ranking,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "source_ready_for_expanded_candidate_retest": not validation_errors,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if Path(path).exists() else None
            for key, path in source_files.items()
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    candidate_pool_plan: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    guarded_variant_retest: Mapping[str, Any],
    guarded_variant_ranking: Mapping[str, Any],
    guarded_decision_update: Mapping[str, Any],
    variant_retest: Mapping[str, Any],
    optimized_variant_ranking: Mapping[str, Any],
    candidate_ranking: Mapping[str, Any],
    sensitivity_result: Mapping[str, Any],
    sensitivity_decision_update: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "candidate_pool_plan": SOURCE_2385_READY_STATUS,
        "owner_review": SOURCE_2384_READY_STATUS,
        "guarded_variant_retest": SOURCE_2383_READY_STATUS,
        "guarded_variant_ranking": SOURCE_2383_READY_STATUS,
        "guarded_decision_update": SOURCE_2383_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
    }
    for key, expected in expected_status.items():
        if source_status.get(key) != expected:
            errors.append(f"{key}.status expected {expected}, got {source_status.get(key)!r}")

    if candidate_pool_plan.get("recommended_next_research_task") != (
        SOURCE_2385_EXPECTED_ROUTE
    ):
        errors.append("2385 plan must route to TRADING-2386")
    if candidate_pool_plan.get("data_quality_gate_reason") != (
        SOURCE_2385_DATA_QUALITY_REASON
    ):
        errors.append("2385 plan data quality boundary unexpected")
    if candidate_pool_plan.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("2385 primary cadence must be valid_until_window")
    if _as_mapping(candidate_pool_plan.get("monthly_rebalance")).get(
        "allowed_for_primary_decision"
    ) is not False:
        errors.append("2385 monthly rebalance primary decision must be false")

    reference_candidates = set(_as_list(candidate_pool_plan.get("reference_candidates")))
    if not set(REFERENCE_CANDIDATES).issubset(reference_candidates):
        errors.append("2385 plan missing required reference candidates")
    new_candidates = _new_candidates_from_plan(candidate_pool_plan)
    if not new_candidates:
        errors.append("2385 plan missing new_candidates_for_2386")
    if len(new_candidates) > 12:
        errors.append("2385 new candidates exceed 2386 budget")
    family_ids = {item["family_id"] for item in new_candidates if item.get("family_id")}
    if len(family_ids) < 6:
        errors.append("2385 plan must cover at least 6 signal families")

    if owner_review.get("candidate_pool_expansion_recommended") is not True:
        errors.append("2384 must recommend candidate pool expansion")
    if owner_review.get("signal_family_diversification_recommended") is not True:
        errors.append("2384 must recommend signal family diversification")
    if owner_review.get("research_only_observation_approved") is not False:
        errors.append("2384 observation approval must remain false")

    if guarded_variant_retest.get("data_quality_gate_executed") is not True:
        errors.append("2383 data quality gate must have executed")
    if guarded_variant_retest.get("data_quality_passed") is not True:
        errors.append("2383 data quality gate must have passed")
    if guarded_variant_retest.get("best_guarded_variant") != BEST_GUARDED_VARIANT:
        errors.append("2383 best guarded variant mismatch")
    if guarded_variant_retest.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("2383 primary cadence must be valid_until_window")

    if variant_retest.get("data_quality_gate_executed") is not True:
        errors.append("2379 data quality gate must have executed")
    if variant_retest.get("data_quality_passed") is not True:
        errors.append("2379 data quality gate must have passed")
    if variant_retest.get("ranking_top_reference") != RANKING_TOP_REFERENCE:
        errors.append("2379 ranking top reference mismatch")
    if variant_retest.get("best_variant_after_retest") != BEST_LOWER_TURNOVER_VARIANT:
        errors.append("2379 best variant mismatch")

    if _top_candidate_from_candidate_ranking(candidate_ranking) != RANKING_TOP_CANDIDATE:
        errors.append("2365 top candidate must remain ranking top reference")
    if not _as_list(sensitivity_result.get("sensitivity_matrix")):
        errors.append("2366 sensitivity matrix must be non-empty")
    if not isinstance(sensitivity_decision_update.get("decision_update"), Mapping):
        errors.append("2366 decision update must be present")

    for label, source in (
        ("candidate_pool_plan", candidate_pool_plan),
        ("owner_review", owner_review),
        ("guarded_variant_retest", guarded_variant_retest),
        ("guarded_variant_ranking", guarded_variant_ranking),
        ("guarded_decision_update", guarded_decision_update),
        ("variant_retest", variant_retest),
        ("optimized_variant_ranking", optimized_variant_ranking),
        ("candidate_ranking", candidate_ranking),
        ("sensitivity_result", sensitivity_result),
        ("sensitivity_decision_update", sensitivity_decision_update),
    ):
        errors.extend(_side_effect_validation_errors(label, source))
    return errors


def _candidate_universe_from_2385(sources: Mapping[str, Any]) -> dict[str, Any]:
    plan = _as_mapping(sources.get("candidate_pool_plan"))
    new_candidates = _new_candidates_from_plan(plan)
    family_map = {
        item["candidate_id"]: item["family_id"]
        for item in new_candidates
        if item.get("candidate_id") and item.get("family_id")
    }
    return {
        "reference_candidates": list(REFERENCE_CANDIDATES),
        "new_candidate_ids": [item["candidate_id"] for item in new_candidates],
        "all_dynamic_candidates": [
            candidate
            for candidate in [
                *REFERENCE_CANDIDATES[1:],
                *[item["candidate_id"] for item in new_candidates],
            ]
            if candidate != "static_baseline"
        ],
        "candidate_family_map": family_map,
        "signal_families": sorted(set(family_map.values())),
        "candidate_budget": _as_mapping(plan.get("candidate_budget_guardrails")).get(
            "candidate_budget",
            plan.get("candidate_budget", {}),
        ),
    }


def _expanded_retest_policy(candidate_plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": "dynamic_strategy_expanded_candidate_pool_retest_v1",
        "status": "research_only_pilot_expanded_candidate_screening",
        "owner": "research_governance",
        "version": "v1",
        "rationale": (
            "TRADING-2386 tests the TRADING-2385 pre-registered expanded "
            "candidate pool under cached-data-gated actual-path execution."
        ),
        "candidate_budget": dict(_as_mapping(candidate_plan.get("candidate_budget"))),
        "thresholds": {
            "time_slice_pass_rate_acceptable_min": TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "regime_slice_pass_rate_acceptable_min": REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "drawdown_worse_tolerance": DRAWDOWN_WORSE_TOLERANCE,
            "turnover_budget_max_monthly": TURNOVER_BUDGET_MAX_MONTHLY,
            "return_advantage_retained_min": RETURN_ADVANTAGE_RETAINED_MIN,
            "observation_time_slice_pass_rate_min": OBSERVATION_TIME_SLICE_PASS_RATE_MIN,
            "observation_regime_slice_pass_rate_min": OBSERVATION_REGIME_SLICE_PASS_RATE_MIN,
        },
        "transformation_parameters": {
            "expanded_step_delta_cap": EXPANDED_STEP_DELTA_CAP,
            "conservative_step_delta_cap": CONSERVATIVE_STEP_DELTA_CAP,
            "risk_off_drawdown_threshold": RISK_OFF_DRAWDOWN_THRESHOLD,
            "severe_drawdown_threshold": SEVERE_DRAWDOWN_THRESHOLD,
            "rolling_vol_window": ROLLING_VOL_WINDOW,
            "trend_window": TREND_WINDOW,
            "recovery_window": RECOVERY_WINDOW,
        },
        "review_condition": (
            "TRADING-2387 owner review remains mandatory before any research-only "
            "observation. Paper-shadow, scheduler, production and broker actions "
            "remain disabled."
        ),
    }


def _run_expanded_candidate_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_plan: Mapping[str, Any],
) -> dict[str, Any]:
    static = _static_bundle(prices)
    realistic_scenario = _cost_stress_scenarios()[1]
    dynamic_candidates = list(candidate_plan["all_dynamic_candidates"])
    bundles = {
        candidate_id: _candidate_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=realistic_scenario,
        )
        for candidate_id in dynamic_candidates
    }
    ranking_bundle = bundles[RANKING_TOP_CANDIDATE]
    full_rows = [
        _full_sample_result_row(
            candidate_id="static_baseline",
            role="static_baseline",
            bundle=static,
            static_bundle=static,
            ranking_bundle=ranking_bundle,
        )
    ]
    for candidate_id in dynamic_candidates:
        full_rows.append(
            _full_sample_result_row(
                candidate_id=candidate_id,
                role=_candidate_role(candidate_id, candidate_plan),
                bundle=bundles[candidate_id],
                static_bundle=static,
                ranking_bundle=ranking_bundle,
            )
        )
    time_rows = _slice_rows(
        prices=prices,
        slice_group="time_slice",
        slice_definitions=_time_slice_definitions(prices, _targeted_retest_policy()),
        bundles=bundles,
        static_bundle=static,
        ranking_bundle=ranking_bundle,
        primary_candidate=RANKING_TOP_CANDIDATE,
    )
    regime_rows = _slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=_regime_slice_definitions(prices, _targeted_retest_policy()),
        bundles=bundles,
        static_bundle=static,
        ranking_bundle=ranking_bundle,
        primary_candidate=RANKING_TOP_CANDIDATE,
    )
    cost_rows = _cost_stress_rows(
        prices=prices,
        policies=policies,
        candidate_ids=dynamic_candidates,
        static_bundle=static,
    )
    constraint_rows = _constraint_rows(
        prices=prices,
        policies=policies,
        candidate_ids=dynamic_candidates,
        static_bundle=static,
        ranking_bundle=ranking_bundle,
    )
    cadence_rows = _cadence_comparison_rows(
        prices=prices,
        policies=policies,
        candidate_ids=dynamic_candidates,
        static_bundle=static,
        ranking_bundle=ranking_bundle,
    )
    return {
        "expanded_candidate_retest_result": full_rows,
        "time_slice_matrix": time_rows,
        "regime_slice_matrix": regime_rows,
        "cost_stress_result": cost_rows,
        "turnover_constraint_result": constraint_rows,
        "cadence_comparison_result": cadence_rows,
    }


def _candidate_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    cadence: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    target_weights = _expanded_target_weights(candidate_id=candidate_id, prices=prices)
    policy = _scenario_policy_for_sensitivity(
        cadence=cadence,
        scenario=scenario,
        policies=policies,
    )
    actual_weights, path_rows = _actual_position_path(
        strategy_id=candidate_id,
        execution_policy_id=str(policy["execution_policy_id"]),
        target_weights=target_weights,
        policy=policy,
        signal_validity_profile=_signal_validity_profile(cadence, policy),
        enable_staleness_filter=cadence == PRIMARY_EXECUTION_CADENCE,
        stale_action="suppress_rebalance" if cadence == PRIMARY_EXECUTION_CADENCE else None,
    )
    cost_bps = _policy_cost_bps(policy)
    _attach_path_return_columns(
        prices=prices,
        target_weights=target_weights,
        actual_weights=actual_weights,
        path_rows=path_rows,
        cost_bps=cost_bps,
    )
    returns = _portfolio_return_series(prices, actual_weights, cost_bps=cost_bps)
    gross_returns = _portfolio_return_series(prices, actual_weights, cost_bps=0.0)
    return {
        "candidate_id": candidate_id,
        "cadence": cadence,
        "scenario": dict(scenario),
        "policy": policy,
        "target_weights": target_weights,
        "actual_weights": actual_weights,
        "path_rows": path_rows,
        "returns": returns,
        "gross_returns": gross_returns,
        "benchmark_returns": prices["QQQ"].pct_change().fillna(0.0),
        "cost_bps": cost_bps,
    }


def _expanded_target_weights(*, candidate_id: str, prices: pd.DataFrame) -> pd.DataFrame:
    ranking = _candidate_target_weights(
        candidate_id=RANKING_TOP_CANDIDATE,
        ranking_top=RANKING_TOP_CANDIDATE,
        robustness_top=BASE_CANDIDATE_ID,
        prices=prices,
    )
    lower = _candidate_target_weights(
        candidate_id=BASE_CANDIDATE_ID,
        ranking_top=RANKING_TOP_CANDIDATE,
        robustness_top=BASE_CANDIDATE_ID,
        prices=prices,
    )
    cooldown = _lower_turnover_cooldown_weights(prices=prices)
    guarded = _guarded_target_weights(candidate_id=BEST_GUARDED_VARIANT, prices=prices)
    if candidate_id == RANKING_TOP_CANDIDATE:
        return ranking
    if candidate_id == BASE_CANDIDATE_ID:
        return lower
    if candidate_id == BEST_LOWER_TURNOVER_VARIANT:
        return cooldown
    if candidate_id == BEST_GUARDED_VARIANT:
        return guarded

    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    rolling_vol = qqq_returns.rolling(ROLLING_VOL_WINDOW, min_periods=5).std()
    high_vol = rolling_vol.ge(rolling_vol.quantile(0.75)).fillna(False)
    low_vol = rolling_vol.lt(rolling_vol.median()).fillna(False)
    trend = (
        prices["QQQ"].gt(prices["QQQ"].rolling(TREND_WINDOW, min_periods=5).mean())
        & prices["QQQ"].pct_change(TREND_WINDOW).fillna(0.0).gt(0.0)
    ).fillna(False)
    recovery = drawdown.lt(0.0) & qqq_returns.rolling(
        RECOVERY_WINDOW,
        min_periods=5,
    ).sum().gt(0.0)
    risk_off = drawdown.le(RISK_OFF_DRAWDOWN_THRESHOLD) | (high_vol & qqq_returns.lt(0.0))
    near_expiry_proxy = qqq_returns.rolling(5, min_periods=1).sum().abs().lt(0.01)

    if candidate_id == "dynamic_regime_reentry_accelerated_v1":
        qqq = lower["QQQ"] * 0.58 + ranking["QQQ"] * 0.42
        qqq.loc[recovery] = ranking.loc[recovery, "QQQ"] * 0.68 + lower.loc[
            recovery,
            "QQQ",
        ] * 0.32
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"] * 0.86 + ranking.loc[
            risk_off,
            "QQQ",
        ] * 0.14
        qqq = _bounded_step_delta(qqq.rolling(3, min_periods=1).mean(), max_delta=0.18)
        lower_bound, upper_bound = 0.18, 0.86
    elif candidate_id == "dynamic_regime_recovery_confirmation_v1":
        qqq = cooldown["QQQ"] * 0.70 + ranking["QQQ"] * 0.30
        qqq.loc[recovery & trend] = ranking.loc[recovery & trend, "QQQ"] * 0.58 + cooldown.loc[
            recovery & trend,
            "QQQ",
        ] * 0.42
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"]
        qqq = _bounded_step_delta(qqq.rolling(5, min_periods=1).mean(), max_delta=0.14)
        lower_bound, upper_bound = 0.20, 0.82
    elif candidate_id == "dynamic_trend_confirmed_growth_tilt_v1":
        qqq = guarded["QQQ"] * 0.62 + ranking["QQQ"] * 0.38
        qqq.loc[trend & ~risk_off] = ranking.loc[trend & ~risk_off, "QQQ"] * 0.76 + guarded.loc[
            trend & ~risk_off,
            "QQQ",
        ] * 0.24
        qqq.loc[~trend | risk_off] = cooldown.loc[~trend | risk_off, "QQQ"]
        qqq = _bounded_step_delta(qqq, max_delta=EXPANDED_STEP_DELTA_CAP)
        lower_bound, upper_bound = 0.18, 0.88
    elif candidate_id == "dynamic_trend_confirmed_low_turnover_v1":
        qqq = cooldown["QQQ"] * 0.76 + ranking["QQQ"] * 0.24
        qqq.loc[trend & low_vol] = guarded.loc[trend & low_vol, "QQQ"] * 0.55 + cooldown.loc[
            trend & low_vol,
            "QQQ",
        ] * 0.45
        qqq = _bounded_step_delta(qqq.rolling(7, min_periods=1).mean(), max_delta=0.10)
        lower_bound, upper_bound = 0.20, 0.80
    elif candidate_id == "dynamic_volatility_scaled_growth_tilt_v1":
        qqq = ranking["QQQ"] * 0.52 + cooldown["QQQ"] * 0.48
        qqq.loc[high_vol] = lower.loc[high_vol, "QQQ"] * 0.84 + ranking.loc[
            high_vol,
            "QQQ",
        ] * 0.16
        qqq.loc[low_vol & trend] = ranking.loc[low_vol & trend, "QQQ"] * 0.66 + cooldown.loc[
            low_vol & trend,
            "QQQ",
        ] * 0.34
        qqq = _bounded_step_delta(qqq.rolling(4, min_periods=1).mean(), max_delta=0.14)
        lower_bound, upper_bound = 0.18, 0.84
    elif candidate_id == "dynamic_volatility_floor_adjusted_v1":
        qqq = cooldown["QQQ"] * 0.80 + ranking["QQQ"] * 0.20
        qqq.loc[high_vol | risk_off] = lower.loc[high_vol | risk_off, "QQQ"] * 0.92
        qqq.loc[low_vol] = qqq.loc[low_vol] * 0.86 + ranking.loc[low_vol, "QQQ"] * 0.14
        qqq = _bounded_step_delta(qqq.rolling(6, min_periods=1).mean(), max_delta=0.10)
        lower_bound, upper_bound = 0.22, 0.78
    elif candidate_id == "dynamic_signal_age_decay_v1":
        qqq = guarded["QQQ"].rolling(5, min_periods=1).mean()
        expiry_or_high_vol = near_expiry_proxy | high_vol
        qqq.loc[expiry_or_high_vol] = (
            qqq.loc[expiry_or_high_vol] * 0.72
            + lower.loc[expiry_or_high_vol, "QQQ"] * 0.28
        )
        qqq = _bounded_step_delta(qqq, max_delta=0.12)
        lower_bound, upper_bound = 0.18, 0.82
    elif candidate_id == "dynamic_valid_until_expiry_strict_v1":
        qqq = cooldown["QQQ"] * 0.82 + guarded["QQQ"] * 0.18
        qqq.loc[near_expiry_proxy | risk_off] = lower.loc[near_expiry_proxy | risk_off, "QQQ"]
        qqq = _bounded_step_delta(qqq.rolling(8, min_periods=1).mean(), max_delta=0.08)
        lower_bound, upper_bound = 0.22, 0.78
    elif candidate_id == "dynamic_turnover_budgeted_growth_tilt_v1":
        qqq = ranking["QQQ"] * 0.50 + cooldown["QQQ"] * 0.50
        qqq.loc[trend & ~high_vol] = ranking.loc[trend & ~high_vol, "QQQ"] * 0.62 + cooldown.loc[
            trend & ~high_vol,
            "QQQ",
        ] * 0.38
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"] * 0.88 + ranking.loc[
            risk_off,
            "QQQ",
        ] * 0.12
        qqq = _bounded_step_delta(qqq.rolling(6, min_periods=1).mean(), max_delta=0.10)
        lower_bound, upper_bound = 0.18, 0.84
    elif candidate_id == "dynamic_turnover_budgeted_regime_overlay_v1":
        qqq = lower["QQQ"] * 0.70 + guarded["QQQ"] * 0.30
        qqq.loc[recovery & trend] = guarded.loc[recovery & trend, "QQQ"] * 0.58 + lower.loc[
            recovery & trend,
            "QQQ",
        ] * 0.42
        qqq = _bounded_step_delta(qqq.rolling(9, min_periods=1).mean(), max_delta=0.08)
        lower_bound, upper_bound = 0.20, 0.80
    elif candidate_id == "dynamic_risk_cap_adaptive_v1":
        qqq = guarded["QQQ"] * 0.60 + lower["QQQ"] * 0.40
        qqq.loc[drawdown.le(SEVERE_DRAWDOWN_THRESHOLD)] = lower.loc[
            drawdown.le(SEVERE_DRAWDOWN_THRESHOLD),
            "QQQ",
        ] * 0.82
        qqq.loc[recovery & trend] = ranking.loc[recovery & trend, "QQQ"] * 0.50 + guarded.loc[
            recovery & trend,
            "QQQ",
        ] * 0.50
        qqq = _bounded_step_delta(qqq.rolling(4, min_periods=1).mean(), max_delta=0.12)
        lower_bound, upper_bound = 0.18, 0.84
    elif candidate_id == "dynamic_risk_cap_trend_conditioned_v1":
        qqq = cooldown["QQQ"] * 0.66 + guarded["QQQ"] * 0.34
        qqq.loc[trend & ~risk_off] = guarded.loc[trend & ~risk_off, "QQQ"] * 0.68 + cooldown.loc[
            trend & ~risk_off,
            "QQQ",
        ] * 0.32
        qqq.loc[~trend | high_vol] = lower.loc[~trend | high_vol, "QQQ"] * 0.88 + cooldown.loc[
            ~trend | high_vol,
            "QQQ",
        ] * 0.12
        qqq = _bounded_step_delta(qqq.rolling(6, min_periods=1).mean(), max_delta=0.10)
        lower_bound, upper_bound = 0.20, 0.82
    else:
        raise ValueError(f"Unknown TRADING-2386 candidate: {candidate_id}")
    return _normalized_qqq_sgov(qqq, index=prices.index, lower=lower_bound, upper=upper_bound)


def _cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_ids: Sequence[str],
    static_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _cost_stress_scenarios():
        ranking_bundle = _candidate_bundle(
            prices=prices,
            policies=policies,
            candidate_id=RANKING_TOP_CANDIDATE,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=scenario,
        )
        rows.append(
            _stress_result_row(
                candidate_id="static_baseline",
                role="static_baseline",
                scenario=scenario,
                bundle=static_bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=RANKING_TOP_CANDIDATE,
            )
        )
        for candidate_id in candidate_ids:
            bundle = (
                ranking_bundle
                if candidate_id == RANKING_TOP_CANDIDATE
                else _candidate_bundle(
                    prices=prices,
                    policies=policies,
                    candidate_id=candidate_id,
                    cadence=PRIMARY_EXECUTION_CADENCE,
                    scenario=scenario,
                )
            )
            rows.append(
                _stress_result_row(
                    candidate_id=candidate_id,
                    role="expanded_candidate",
                    scenario=scenario,
                    bundle=bundle,
                    static_bundle=static_bundle,
                    ranking_bundle=ranking_bundle,
                    primary_candidate=RANKING_TOP_CANDIDATE,
                )
            )
    return rows


def _constraint_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_ids: Sequence[str],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _execution_constraint_scenarios():
        for candidate_id in candidate_ids:
            bundle = _candidate_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
            )
            row = _stress_result_row(
                candidate_id=candidate_id,
                role="expanded_candidate_constraint_stress",
                scenario=scenario,
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=RANKING_TOP_CANDIDATE,
            )
            execution = _as_mapping(row.get("execution_metrics"))
            max_cap = scenario.get("max_turnover_per_month")
            row["constraint_axis"] = scenario["scenario_group"]
            row["constraint_value"] = scenario["constraint_value"]
            row["observed_max_monthly_turnover"] = execution.get("max_monthly_turnover")
            row["constraint_passed"] = (
                max_cap is None
                or _float(execution.get("max_monthly_turnover")) <= _float(max_cap)
            ) and row["stress_passed"]
            rows.append(row)
    return rows


def _cadence_comparison_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_ids: Sequence[str],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scenario = _cost_stress_scenarios()[1]
    for candidate_id in candidate_ids:
        for cadence in COMPARISON_CADENCES:
            bundle = _candidate_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=cadence,
                scenario=scenario,
            )
            rows.append(
                _stress_result_row(
                    candidate_id=candidate_id,
                    role="cadence_comparison",
                    scenario={
                        **scenario,
                        "scenario_id": cadence,
                        "scenario_group": "cadence_comparison",
                    },
                    bundle=bundle,
                    static_bundle=static_bundle,
                    ranking_bundle=ranking_bundle,
                    primary_candidate=RANKING_TOP_CANDIDATE,
                )
            )
    return rows


def _expanded_candidate_ranking(
    *,
    retest: Mapping[str, Any],
    candidate_plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    full_by_candidate = _rows_by_candidate(retest["expanded_candidate_retest_result"])
    cost_by_candidate = _rows_by_candidate_and_scenario(retest["cost_stress_result"])
    lower_row = full_by_candidate[BASE_CANDIDATE_ID]
    guarded_row = full_by_candidate[BEST_GUARDED_VARIANT]
    ranking_row = full_by_candidate[RANKING_TOP_CANDIDATE]
    static_row = full_by_candidate["static_baseline"]
    ranking_advantage = _annual_gap(ranking_row, static_row)
    rows: list[dict[str, Any]] = []
    for candidate_id in candidate_plan["all_dynamic_candidates"]:
        full = full_by_candidate[candidate_id]
        perf = _as_mapping(full.get("performance_metrics"))
        rel = _as_mapping(full.get("relative_metrics"))
        execution = _as_mapping(full.get("execution_metrics"))
        cost = {
            scenario: cost_by_candidate.get((candidate_id, scenario), {})
            for scenario in ("base", "realistic", "conservative", "harsh")
        }
        realistic_gap = _scenario_gap(cost["realistic"])
        conservative_gap = _scenario_gap(cost["conservative"])
        harsh_gap = _scenario_gap(cost["harsh"])
        time_pass_rate = _pass_rate_for_candidate(retest["time_slice_matrix"], candidate_id)
        regime_pass_rate = _pass_rate_for_candidate(
            retest["regime_slice_matrix"],
            candidate_id,
        )
        annual = _float(perf.get("annualized_return"))
        lower_gap = round(annual - _annual(lower_row), 6)
        guarded_gap = round(annual - _annual(guarded_row), 6)
        ranking_gap = round(annual - _annual(ranking_row), 6)
        return_retained = _safe_ratio(max(0.0, annual - _annual(static_row)), ranking_advantage)
        turnover_budget_passed = (
            _float(execution.get("max_monthly_turnover")) <= TURNOVER_BUDGET_MAX_MONTHLY
            and _float(execution.get("stale_signal_execution_count")) == 0.0
        )
        drawdown_not_materially_worse = abs(_float(perf.get("max_drawdown"))) <= (
            abs(_float(_as_mapping(lower_row.get("performance_metrics")).get("max_drawdown")))
            + DRAWDOWN_WORSE_TOLERANCE
        )
        decision, reasons = _candidate_decision(
            candidate_id=candidate_id,
            realistic_gap=realistic_gap,
            conservative_gap=conservative_gap,
            harsh_gap=harsh_gap,
            time_pass_rate=time_pass_rate,
            regime_pass_rate=regime_pass_rate,
            return_retained=return_retained,
            turnover_budget_passed=turnover_budget_passed,
            drawdown_not_materially_worse=drawdown_not_materially_worse,
            lower_gap=lower_gap,
            guarded_gap=guarded_gap,
            ranking_gap=ranking_gap,
            stale_count=_float(execution.get("stale_signal_execution_count")),
        )
        score = _candidate_score(
            realistic_gap=realistic_gap,
            conservative_gap=conservative_gap,
            harsh_gap=harsh_gap,
            time_pass_rate=time_pass_rate,
            regime_pass_rate=regime_pass_rate,
            return_retained=return_retained,
            turnover=_float(execution.get("turnover")),
            max_drawdown=_float(perf.get("max_drawdown")),
        )
        rows.append(
            {
                "candidate_id": candidate_id,
                "signal_family": _candidate_family(candidate_id, candidate_plan),
                "candidate_type": _candidate_type(candidate_id),
                "total_return": perf.get("total_return"),
                "annualized_return": perf.get("annualized_return"),
                "cost_adjusted_return": perf.get("annualized_return"),
                "max_drawdown": perf.get("max_drawdown"),
                "turnover": execution.get("turnover"),
                "max_monthly_turnover": execution.get("max_monthly_turnover"),
                "time_slice_pass_rate": time_pass_rate,
                "regime_slice_pass_rate": regime_pass_rate,
                "cost_stress_survival": _cost_stress_survival(
                    realistic_passed=realistic_gap > 0.0,
                    conservative_passed=conservative_gap > 0.0,
                    harsh_passed=harsh_gap > 0.0,
                ),
                "realistic_cost_passed": realistic_gap > 0.0,
                "conservative_cost_passed": conservative_gap > 0.0,
                "harsh_cost_passed": harsh_gap > 0.0,
                "turnover_budget_passed": turnover_budget_passed,
                "dynamic_vs_static_gap": rel.get("dynamic_vs_static_gap"),
                "candidate_vs_ranking_top_gap": ranking_gap,
                "candidate_vs_lower_turnover_gap": lower_gap,
                "candidate_vs_guarded_ranking_top_gap": guarded_gap,
                "return_advantage_retained": return_retained,
                "return_gap_reduction": max(0.0, lower_gap),
                "drawdown_gap_vs_static": _drawdown_gap(full, static_row),
                "cost_adjusted_dynamic_vs_static_gap": realistic_gap,
                "valid_until_window_preserved": full.get("execution_cadence")
                == PRIMARY_EXECUTION_CADENCE,
                "no_stale_signal_carry_forward": _float(
                    execution.get("stale_signal_execution_count")
                )
                == 0.0,
                "owner_review_triggered": decision
                in {DECISION_ACCEPT_RESEARCH_ONLY, DECISION_OWNER_REVIEW},
                "comparison_vs_reference_candidates": {
                    "static_baseline_gap": rel.get("dynamic_vs_static_gap"),
                    "ranking_top_gap": ranking_gap,
                    "lower_turnover_gap": lower_gap,
                    "guarded_ranking_top_gap": guarded_gap,
                },
                "screening_score": score,
                "decision": decision,
                "decision_reason": "; ".join(reasons),
                "decision_reasons": reasons,
                "monthly_rebalance_allowed_for_primary_decision": False,
            }
        )
    ranked = sorted(rows, key=_ranking_sort_key, reverse=True)
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return ranked


def _signal_family_screening(
    *,
    ranking: Sequence[Mapping[str, Any]],
    retest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    families = sorted(
        {
            str(row.get("signal_family"))
            for row in ranking
            if row.get("candidate_type") == "new_candidate"
        }
    )
    rows: list[dict[str, Any]] = []
    for family in families:
        family_rows = [
            _as_mapping(row)
            for row in ranking
            if row.get("signal_family") == family
            and row.get("candidate_type") == "new_candidate"
        ]
        best = sorted(family_rows, key=_ranking_sort_key, reverse=True)[0]
        rows.append(
            {
                "signal_family": family,
                "family_best_candidate": best["candidate_id"],
                "family_best_candidate_decision": best["decision"],
                "family_average_score": round(
                    sum(_float(row.get("screening_score")) for row in family_rows)
                    / max(1, len(family_rows)),
                    6,
                ),
                "family_candidate_count": len(family_rows),
                "family_time_slice_pass_rate": round(
                    sum(_float(row.get("time_slice_pass_rate")) for row in family_rows)
                    / max(1, len(family_rows)),
                    6,
                ),
                "family_regime_slice_pass_rate": round(
                    sum(_float(row.get("regime_slice_pass_rate")) for row in family_rows)
                    / max(1, len(family_rows)),
                    6,
                ),
                "family_failure_reason": _family_failure_reason(family_rows),
                "owner_review_candidate_count": sum(
                    1
                    for row in family_rows
                    if row.get("decision")
                    in {DECISION_ACCEPT_RESEARCH_ONLY, DECISION_OWNER_REVIEW}
                ),
            }
        )
    ranked = sorted(rows, key=lambda row: _float(row["family_average_score"]), reverse=True)
    for index, row in enumerate(ranked, start=1):
        row["family_rank"] = index
    return ranked


def _decision_update(
    *,
    ranking: Sequence[Mapping[str, Any]],
    family_screening: Sequence[Mapping[str, Any]],
    retest: Mapping[str, Any],
) -> dict[str, Any]:
    best = _as_mapping(ranking[0]) if ranking else {}
    best_family = _as_mapping(family_screening[0]) if family_screening else {}
    observation_ready = best.get("decision") == DECISION_ACCEPT_RESEARCH_ONLY
    return {
        "schema_version": "dynamic_strategy_expanded_candidate_decision_update.v1",
        "decision_update_ready": True,
        "best_candidate_after_expanded_screening": best.get("candidate_id"),
        "best_candidate_decision": best.get("decision"),
        "best_signal_family": best.get("signal_family"),
        "best_signal_family_screening": best_family,
        "candidate_ready_for_research_only_observation": observation_ready,
        "owner_review_required": best.get("decision")
        in {DECISION_ACCEPT_RESEARCH_ONLY, DECISION_OWNER_REVIEW},
        "top_candidate_ranking_row": best,
        "recommended_next_research_task": NEXT_ROUTE,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "scheduler_enabled": False,
    }


def _summary_findings(decision_update: Mapping[str, Any]) -> dict[str, Any]:
    best = _as_mapping(decision_update.get("top_candidate_ranking_row"))
    return {
        "expanded_pool_found_candidate_above_observation_threshold": _yes_no(
            decision_update.get("best_candidate_decision")
            == DECISION_ACCEPT_RESEARCH_ONLY
        ),
        "best_candidate_beats_static": _yes_no(
            _float(best.get("dynamic_vs_static_gap")) > 0.0
        ),
        "best_candidate_survives_realistic_cost": _yes_no(
            best.get("realistic_cost_passed") is True
        ),
        "best_candidate_survives_conservative_cost": _yes_no(
            best.get("conservative_cost_passed") is True
        ),
        "best_candidate_improves_over_existing_references": _yes_no(
            _float(best.get("candidate_vs_lower_turnover_gap")) > 0.0
            and _float(best.get("candidate_vs_guarded_ranking_top_gap")) >= 0.0
        ),
        "valid_until_window_remains_default": True,
        "paper_shadow_remains_disabled": True,
        "production_remains_disabled": True,
        "broker_remains_disabled": True,
    }


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
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else None,
        "market_regime": AI_REGIME_SUMMARY.get("market_regime", "ai_after_chatgpt"),
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat() if end_date else None,
        },
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_expanded_candidate_retest": bool(
            sources.get("source_ready_for_expanded_candidate_retest")
        ),
        "prices_path": str(prices_path),
        "marketstack_prices_path": str(marketstack_prices_path),
        "rates_path": str(rates_path),
        "policy_registry_path": str(policy_registry_path),
        "data_quality": dict(data_quality),
        "data_quality_gate_executed": True,
        "data_quality_passed": bool(data_quality.get("passed")),
        "data_quality_status": data_quality.get("status"),
        "data_quality_report_path": data_quality.get("report_path"),
        "fresh_market_data_read": True,
        "backtest_run": True,
        "expanded_candidate_retest_run": True,
        "new_signal_generated": False,
        "scoring_run": False,
        "research_only": True,
        "observe_only": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _blocked_sections(reason: str, sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "blocked_reason": reason,
        "reference_candidates": list(REFERENCE_CANDIDATES),
        "signal_families_tested": [],
        "new_candidates_tested": [],
        "new_candidates_tested_count": 0,
        "total_candidates_tested_count": 0,
        "expanded_candidate_retest_result": [],
        "expanded_candidate_ranking": [],
        "signal_family_screening": [],
        "time_slice_matrix": [],
        "regime_slice_matrix": [],
        "time_regime_slice_matrix": [],
        "cost_stress_result": [],
        "turnover_constraint_result": [],
        "decision_update": {
            "decision_update_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "summary_findings": {
            "blocked_reason": reason,
            "valid_until_window_remains_default": True,
            "paper_shadow_remains_disabled": True,
        },
        "expanded_candidate_retest_ready": False,
        "expanded_candidate_ranking_ready": False,
        "signal_family_screening_ready": False,
        "time_slice_matrix_ready": False,
        "regime_slice_matrix_ready": False,
        "cost_stress_result_ready": False,
        "decision_update_ready": False,
        "best_candidate_after_expanded_screening": None,
        "best_candidate_decision": DECISION_REJECT,
        "best_signal_family": None,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "json_path": str(output_root / "expanded_candidate_retest_result.json"),
        "expanded_candidate_ranking_json": str(
            output_root / "expanded_candidate_ranking.json"
        ),
        "signal_family_screening_json": str(
            output_root / "signal_family_screening.json"
        ),
        "time_regime_slice_matrix_json": str(
            output_root / "time_regime_slice_matrix.json"
        ),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_expanded_candidate_pool_retest.md"
        ),
        "ranking_markdown": str(
            docs_root / "dynamic_strategy_expanded_candidate_ranking.md"
        ),
        "signal_family_screening_markdown": str(
            docs_root / "dynamic_strategy_signal_family_screening.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2387_route.md"),
    }
    payload["artifact_paths"] = paths
    _write_json(Path(paths["json_path"]), payload)
    _write_json(
        Path(paths["expanded_candidate_ranking_json"]),
        {
            "report_type": "dynamic_strategy_expanded_candidate_ranking",
            "schema_version": "dynamic_strategy_expanded_candidate_ranking.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "primary_execution_cadence": payload.get("primary_execution_cadence"),
            "expanded_candidate_ranking": payload.get("expanded_candidate_ranking", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(paths["signal_family_screening_json"]),
        {
            "report_type": "dynamic_strategy_signal_family_screening",
            "schema_version": "dynamic_strategy_signal_family_screening.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "signal_family_screening": payload.get("signal_family_screening", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(paths["time_regime_slice_matrix_json"]),
        {
            "report_type": "dynamic_strategy_expanded_candidate_time_regime_slice_matrix",
            "schema_version": "dynamic_strategy_expanded_candidate_time_regime_slice_matrix.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "time_slice_matrix": payload.get("time_slice_matrix", []),
            "regime_slice_matrix": payload.get("regime_slice_matrix", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(paths["decision_update_json"]),
        {
            "report_type": "dynamic_strategy_expanded_candidate_decision_update",
            "schema_version": "dynamic_strategy_expanded_candidate_decision_update.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "decision_update": payload.get("decision_update", {}),
            "summary_findings": payload.get("summary_findings", {}),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(paths["markdown_path"]).write_text(_main_markdown(payload), encoding="utf-8")
    Path(paths["ranking_markdown"]).write_text(
        _ranking_markdown(payload),
        encoding="utf-8",
    )
    Path(paths["signal_family_screening_markdown"]).write_text(
        _family_markdown(payload),
        encoding="utf-8",
    )
    Path(paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("summary_findings"))
    decision = _as_mapping(payload.get("decision_update"))
    return "\n".join(
        [
            "# Dynamic strategy expanded candidate pool retest",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- data quality：`{payload.get('data_quality_status')}`",
            f"- reference candidates：`{payload.get('reference_candidate_count')}`",
            f"- new candidates tested：`{payload.get('new_candidates_tested_count')}`",
            f"- signal families tested：`{payload.get('signal_families_tested_count')}`",
            f"- best candidate：`{payload.get('best_candidate_after_expanded_screening')}`",
            f"- best decision：`{payload.get('best_candidate_decision')}`",
            f"- best signal family：`{payload.get('best_signal_family')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Required answers",
            "",
            (
                "- expanded pool 是否出现 observation-ready candidate："
                f"`{summary.get('expanded_pool_found_candidate_above_observation_threshold')}`"
            ),
            f"- best candidate 是否优于 static：`{summary.get('best_candidate_beats_static')}`",
            (
                "- best candidate 是否穿越 realistic / conservative cost："
                f"`{summary.get('best_candidate_survives_realistic_cost')}` / "
                f"`{summary.get('best_candidate_survives_conservative_cost')}`"
            ),
            (
                "- best candidate 是否改善既有 references："
                f"`{summary.get('best_candidate_improves_over_existing_references')}`"
            ),
            "- valid_until_window 是否仍为默认口径：`true`",
            "- 是否允许 paper-shadow / production / broker：`NO`",
            "",
            "## Decision update",
            "",
            f"- best candidate：`{decision.get('best_candidate_after_expanded_screening')}`",
            f"- decision：`{decision.get('best_candidate_decision')}`",
            f"- best signal family：`{decision.get('best_signal_family')}`",
            f"- owner review required：`{decision.get('owner_review_required')}`",
            "",
            "## Safety boundary",
            "",
            "- 2386 是 strategy research actual retest，不是 observation approval。",
            (
                "- scheduler、event append、outcome binding、paper-shadow、production、"
                "broker/order 和 daily report 全部保持 disabled / false / none。"
            ),
        ]
    )


def _ranking_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic strategy expanded candidate ranking",
        "",
        f"- status：`{payload.get('status')}`",
        "",
        "|rank|candidate|family|decision|annual|gap_static|gap_lower|gap_guarded|mdd|turnover|time|regime|",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _as_list(payload.get("expanded_candidate_ranking")):
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    str(item.get("rank")),
                    f"`{item.get('candidate_id')}`",
                    f"`{item.get('signal_family')}`",
                    f"`{item.get('decision')}`",
                    _fmt(item.get("annualized_return")),
                    _fmt(item.get("dynamic_vs_static_gap")),
                    _fmt(item.get("candidate_vs_lower_turnover_gap")),
                    _fmt(item.get("candidate_vs_guarded_ranking_top_gap")),
                    _fmt(item.get("max_drawdown")),
                    _fmt(item.get("turnover")),
                    _fmt(item.get("time_slice_pass_rate")),
                    _fmt(item.get("regime_slice_pass_rate")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _family_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic strategy signal family screening",
        "",
        f"- status：`{payload.get('status')}`",
        "",
        "|rank|family|best_candidate|decision|avg_score|time|regime|failure|",
        "|---:|---|---|---|---:|---:|---:|---|",
    ]
    for row in _as_list(payload.get("signal_family_screening")):
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    str(item.get("family_rank")),
                    f"`{item.get('signal_family')}`",
                    f"`{item.get('family_best_candidate')}`",
                    f"`{item.get('family_best_candidate_decision')}`",
                    _fmt(item.get("family_average_score")),
                    _fmt(item.get("family_time_slice_pass_rate")),
                    _fmt(item.get("family_regime_slice_pass_rate")),
                    str(item.get("family_failure_reason")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2387 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- best candidate：`{payload.get('best_candidate_after_expanded_screening')}`",
            f"- best decision：`{payload.get('best_candidate_decision')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "- route boundary：owner review and next research decision only。",
            "- paper-shadow / production / broker remain disabled。",
        ]
    )


def _candidate_decision(
    *,
    candidate_id: str,
    realistic_gap: float,
    conservative_gap: float,
    harsh_gap: float,
    time_pass_rate: float,
    regime_pass_rate: float,
    return_retained: float,
    turnover_budget_passed: bool,
    drawdown_not_materially_worse: bool,
    lower_gap: float,
    guarded_gap: float,
    ranking_gap: float,
    stale_count: float,
) -> tuple[str, list[str]]:
    reasons = [
        f"realistic_gap={realistic_gap}",
        f"conservative_gap={conservative_gap}",
        f"harsh_gap={harsh_gap}",
        f"time_slice_pass_rate={time_pass_rate}",
        f"regime_slice_pass_rate={regime_pass_rate}",
        f"return_advantage_retained={return_retained}",
        f"turnover_budget_passed={turnover_budget_passed}",
        f"drawdown_not_materially_worse={drawdown_not_materially_worse}",
        f"lower_gap={lower_gap}",
        f"guarded_gap={guarded_gap}",
        f"ranking_gap={ranking_gap}",
    ]
    if stale_count > 0:
        return DECISION_REJECT, [*reasons, "stale_signal_carry_forward_detected"]
    if realistic_gap <= 0.0 or conservative_gap <= 0.0:
        return DECISION_REJECT, [*reasons, "blocked_by_cost_adjusted_static_gap"]
    time_ok = time_pass_rate >= TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
    regime_ok = regime_pass_rate >= REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
    if not time_ok or not regime_ok:
        return DECISION_CONTINUE_OPTIMIZATION, [*reasons, "slice_stability_not_yet_enough"]
    if not turnover_budget_passed or not drawdown_not_materially_worse:
        return DECISION_CONTINUE_OPTIMIZATION, [
            *reasons,
            "turnover_or_drawdown_guardrail_tradeoff_requires_repair",
        ]
    if (
        candidate_id not in REFERENCE_CANDIDATES
        and lower_gap > 0.0
        and guarded_gap >= 0.0
        and return_retained >= RETURN_ADVANTAGE_RETAINED_MIN
        and time_pass_rate >= OBSERVATION_TIME_SLICE_PASS_RATE_MIN
        and regime_pass_rate >= OBSERVATION_REGIME_SLICE_PASS_RATE_MIN
    ):
        return DECISION_ACCEPT_RESEARCH_ONLY, [
            *reasons,
            "expanded_screening_observation_threshold_met",
        ]
    if (
        candidate_id in REFERENCE_CANDIDATES
        or lower_gap > 0.0
        or guarded_gap >= 0.0
        or ranking_gap >= -0.005
    ):
        return DECISION_OWNER_REVIEW, [*reasons, "credible_candidate_owner_review_required"]
    return DECISION_DEPRECATED, [*reasons, "no_advantage_in_expanded_pool"]


def _candidate_score(
    *,
    realistic_gap: float,
    conservative_gap: float,
    harsh_gap: float,
    time_pass_rate: float,
    regime_pass_rate: float,
    return_retained: float,
    turnover: float,
    max_drawdown: float,
) -> float:
    return round(
        realistic_gap * 4.0
        + conservative_gap * 3.0
        + harsh_gap
        + time_pass_rate
        + regime_pass_rate
        + return_retained * 0.5
        - max(0.0, turnover - 1.0) * 0.05
        - max(0.0, abs(max_drawdown) - 0.15) * 0.5,
        6,
    )


def _candidate_family(candidate_id: str, candidate_plan: Mapping[str, Any]) -> str:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return "reference_ranking_top"
    if candidate_id == BASE_CANDIDATE_ID:
        return "reference_lower_turnover"
    if candidate_id == BEST_LOWER_TURNOVER_VARIANT:
        return "reference_cooldown_balanced"
    if candidate_id == BEST_GUARDED_VARIANT:
        return "reference_guarded_ranking_top"
    return str(_as_mapping(candidate_plan.get("candidate_family_map")).get(candidate_id))


def _candidate_type(candidate_id: str) -> str:
    return "reference_candidate" if candidate_id in REFERENCE_CANDIDATES else "new_candidate"


def _candidate_role(candidate_id: str, candidate_plan: Mapping[str, Any]) -> str:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return "ranking_top_original_reference"
    if candidate_id == BASE_CANDIDATE_ID:
        return "lower_turnover_reference"
    if candidate_id == BEST_LOWER_TURNOVER_VARIANT:
        return "lower_turnover_best_variant_reference"
    if candidate_id == BEST_GUARDED_VARIANT:
        return "guarded_ranking_top_best_variant_reference"
    return f"new_candidate_{_candidate_family(candidate_id, candidate_plan)}"


def _family_failure_reason(rows: Sequence[Mapping[str, Any]]) -> str:
    if any(row.get("decision") == DECISION_ACCEPT_RESEARCH_ONLY for row in rows):
        return "none_observation_threshold_met"
    if any(row.get("decision") == DECISION_OWNER_REVIEW for row in rows):
        return "owner_review_tradeoff_remaining"
    if not any(row.get("conservative_cost_passed") for row in rows):
        return "conservative_cost_failure"
    if not any(
        _float(row.get("time_slice_pass_rate")) >= TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
        for row in rows
    ):
        return "time_slice_stability_failure"
    if not any(
        _float(row.get("regime_slice_pass_rate")) >= REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
        for row in rows
    ):
        return "regime_slice_stability_failure"
    return "relative_reference_advantage_not_enough"


def _ranking_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    decision_priority = {
        DECISION_ACCEPT_RESEARCH_ONLY: 5,
        DECISION_OWNER_REVIEW: 4,
        DECISION_CONTINUE_OPTIMIZATION: 3,
        DECISION_REJECT: 2,
        DECISION_DEPRECATED: 1,
    }
    return (
        decision_priority.get(str(row.get("decision")), 0),
        _float(row.get("screening_score")),
        _float(row.get("conservative_cost_passed")),
        _float(row.get("time_slice_pass_rate")),
        _float(row.get("regime_slice_pass_rate")),
        _float(row.get("cost_adjusted_return")),
        -_float(row.get("turnover")),
    )


def _new_candidates_from_plan(plan: Mapping[str, Any]) -> list[dict[str, str]]:
    candidates = plan.get("new_candidates_for_2386")
    if isinstance(candidates, list) and candidates and isinstance(candidates[0], str):
        family_by_id = {
            str(item.get("candidate_id")): str(item.get("family_id"))
            for item in _as_list(
                _as_mapping(plan.get("candidate_pool_expansion_plan")).get(
                    "new_candidates_selected_for_2386"
                )
            )
            if isinstance(item, Mapping)
        }
        return [
            {
                "candidate_id": str(candidate_id),
                "family_id": family_by_id.get(str(candidate_id), ""),
            }
            for candidate_id in candidates
        ]
    selected = _as_list(
        _as_mapping(plan.get("candidate_pool_expansion_plan")).get(
            "new_candidates_selected_for_2386"
        )
    )
    result: list[dict[str, str]] = []
    for item in selected:
        mapping = _as_mapping(item)
        candidate_id = str(mapping.get("candidate_id", ""))
        family_id = str(mapping.get("family_id", ""))
        if candidate_id:
            result.append({"candidate_id": candidate_id, "family_id": family_id})
    return result


def _side_effect_validation_errors(label: str, source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    false_fields = (
        "scheduler_enabled",
        "scheduled_task_created",
        "event_append_enabled",
        "historical_event_log_mutated",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "production_enabled",
        "broker_action_enabled",
        "order_generated",
        "daily_report_generated",
    )
    for field in false_fields:
        if source.get(field) is True:
            errors.append(f"{label}.{field}_must_remain_false")
    if source.get("broker_action") not in {None, "none"}:
        errors.append(f"{label}.broker_action_must_remain_none")
    if source.get("production_effect") not in {None, "none"}:
        errors.append(f"{label}.production_effect_must_remain_none")
    return errors


def _rows_by_candidate(rows: Any) -> dict[str, dict[str, Any]]:
    return {str(row.get("candidate_id")): _as_mapping(row) for row in _as_list(rows)}


def _rows_by_candidate_and_scenario(rows: Any) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row.get("candidate_id")), str(row.get("scenario_id"))): _as_mapping(row)
        for row in _as_list(rows)
    }


def _pass_rate_for_candidate(rows: Any, candidate_id: str) -> float:
    selected = [
        _as_mapping(row)
        for row in _as_list(rows)
        if _as_mapping(row).get("candidate_id") == candidate_id
    ]
    if not selected:
        return 0.0
    return round(sum(1 for row in selected if bool(row.get("slice_passed"))) / len(selected), 6)


def _scenario_gap(row: Mapping[str, Any]) -> float:
    return _float(_as_mapping(row.get("relative_metrics")).get("dynamic_vs_static_gap"))


def _annual(row: Mapping[str, Any]) -> float:
    return _float(_as_mapping(row.get("performance_metrics")).get("annualized_return"))


def _annual_gap(row: Mapping[str, Any], reference: Mapping[str, Any]) -> float:
    return round(_annual(row) - _annual(reference), 6)


def _drawdown_gap(row: Mapping[str, Any], reference: Mapping[str, Any]) -> float:
    drawdown = abs(_float(_as_mapping(row.get("performance_metrics")).get("max_drawdown")))
    reference_drawdown = abs(
        _float(_as_mapping(reference.get("performance_metrics")).get("max_drawdown"))
    )
    return round(drawdown - reference_drawdown, 6)


def _cost_stress_survival(
    *,
    realistic_passed: bool,
    conservative_passed: bool,
    harsh_passed: bool,
) -> str:
    if harsh_passed:
        return "harsh"
    if conservative_passed:
        return "conservative"
    if realistic_passed:
        return "realistic"
    return "failed"


def _top_candidate_from_candidate_ranking(document: Mapping[str, Any]) -> str | None:
    rows = _as_list(document.get("candidate_ranking"))
    if not rows:
        return None
    return str(_as_mapping(rows[0]).get("candidate_id"))


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return round(numerator / denominator, 6)


def _yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def _fmt(value: Any) -> str:
    if isinstance(value, int | float):
        return f"{float(value):.6f}"
    return "" if value is None else str(value)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _load_json_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required source artifact not found: {path}")
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"Source artifact must be a JSON object: {path}")
    return document


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
