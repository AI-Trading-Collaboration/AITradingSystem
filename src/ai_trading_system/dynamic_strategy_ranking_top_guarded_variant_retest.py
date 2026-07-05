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
    RANKING_TOP_FALLBACK,
    ROBUSTNESS_TOP_FALLBACK,
    _candidate_target_weights,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
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
    ASSET_COLUMNS,
    _portfolio_return_series,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DECISION_ACCEPT_RESEARCH_ONLY,
    DECISION_CONTINUE_OPTIMIZATION,
    DECISION_OWNER_REVIEW,
    DECISION_REJECT,
    _cost_stress_scenarios,
    _execution_constraint_scenarios,
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
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT,
    GUARDED_VARIANT_IDS,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    NEXT_ROUTE as SOURCE_2382_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    READY_STATUS as SOURCE_2382_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
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
    _float,
    _load_execution_price_matrix,
    _load_policy_registry,
    _policies_by_id,
    _policy_cost_bps,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _data_quality_gate,
    _load_registry,
)

TASK_ID = "TRADING-2383"
TASK_REGISTER_ID = (
    "TRADING-2383_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST"
)
REPORT_TYPE = "dynamic_strategy_ranking_top_guarded_variant_retest"
SCHEMA_VERSION = "dynamic_strategy_ranking_top_guarded_variant_retest.v1"
READY_STATUS = "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_"
    "Observation_Decision"
)

RANKING_TOP_CANDIDATE = RANKING_TOP_FALLBACK
LOWER_TURNOVER_REFERENCE = ROBUSTNESS_TOP_FALLBACK
COOLDOWN_BALANCED_REFERENCE = BEST_LOWER_TURNOVER_VARIANT
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2379",
    "TRADING-2381",
    "TRADING-2382",
)
VARIANTS_TESTED: tuple[str, ...] = (RANKING_TOP_CANDIDATE, *GUARDED_VARIANT_IDS)
COMPARISON_CANDIDATES: tuple[str, ...] = (
    "static_baseline",
    RANKING_TOP_CANDIDATE,
    LOWER_TURNOVER_REFERENCE,
    COOLDOWN_BALANCED_REFERENCE,
)
COMPARISON_CADENCES: tuple[str, ...] = (
    PRIMARY_EXECUTION_CADENCE,
    "cooldown_limited_event_driven",
    "signal_event_driven",
)
DECISION_DEPRECATED = "DEPRECATED_BY_GUARDED_RETEST"

# TRADING-2383 pilot policy constants. They define a research-only retest
# baseline and are emitted in the generated policy report for owner review.
TURNOVER_MATERIAL_IMPROVEMENT_MIN = 0.05
DRAWDOWN_MATERIAL_WORSE_TOLERANCE = 0.02
RETURN_ADVANTAGE_RETAINED_MIN = 0.50
TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN = 0.40
REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN = 0.30
HIGH_VOL_DRAWDOWN_TOLERANCE = 0.02
GUARDED_STEP_DELTA_CAP = 0.20
CONSERVATIVE_STEP_DELTA_CAP = 0.12
RISK_OFF_DRAWDOWN_THRESHOLD = -0.08
SEVERE_DRAWDOWN_THRESHOLD = -0.10
ROLLING_VOL_WINDOW = 20
TREND_WINDOW = 60
RECOVERY_WINDOW = 20

DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2382_RETEST_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT
    / "retest_plan_result.json"
)
DEFAULT_SOURCE_2382_GUARDED_VARIANT_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT
    / "guarded_variant_plan.json"
)
DEFAULT_SOURCE_2382_VARIANT_EVALUATION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT
    / "variant_evaluation_plan.json"
)
DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "variant_retest_result.json"
)
DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "optimized_variant_ranking.json"
)


def run_dynamic_strategy_ranking_top_guarded_variant_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_retest_plan_path: Path = DEFAULT_SOURCE_2382_RETEST_PLAN_PATH,
    source_guarded_variant_plan_path: Path = DEFAULT_SOURCE_2382_GUARDED_VARIANT_PLAN_PATH,
    source_variant_evaluation_plan_path: Path = (
        DEFAULT_SOURCE_2382_VARIANT_EVALUATION_PLAN_PATH
    ),
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_retest_plan_path=source_retest_plan_path,
        source_guarded_variant_plan_path=source_guarded_variant_plan_path,
        source_variant_evaluation_plan_path=source_variant_evaluation_plan_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
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
    if not bool(sources["source_ready_for_guarded_variant_retest"]):
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
    retest_policy = _guarded_variant_retest_policy()
    retest = _run_guarded_variant_retest(
        prices=prices,
        policies=policies,
        retest_policy=retest_policy,
    )
    ranking = _guarded_variant_ranking(retest)
    decision_update = _decision_update(ranking=ranking, retest=retest, sources=sources)
    summary = _summary_findings(ranking=ranking, decision_update=decision_update)
    payload.update(
        {
            "guarded_variant_retest_policy": retest_policy,
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "comparison_cadences": list(COMPARISON_CADENCES),
            "monthly_rebalance": {
                "allowed_for_reference": True,
                "allowed_for_primary_decision": False,
            },
            "ranking_top_candidate": RANKING_TOP_CANDIDATE,
            "guardrail_reference_candidates": [
                LOWER_TURNOVER_REFERENCE,
                COOLDOWN_BALANCED_REFERENCE,
            ],
            "comparison_candidates": list(COMPARISON_CANDIDATES),
            "variants_tested": list(VARIANTS_TESTED),
            "guarded_variant_retest_result": retest["guarded_variant_retest_result"],
            "guarded_variant_ranking": ranking,
            "time_slice_matrix": retest["time_slice_matrix"],
            "regime_slice_matrix": retest["regime_slice_matrix"],
            "cost_stress_result": retest["cost_stress_result"],
            "turnover_constraint_result": retest["turnover_constraint_result"],
            "cadence_comparison_result": retest["cadence_comparison_result"],
            "decision_update": decision_update,
            "summary_findings": summary,
            "guarded_variant_retest_ready": True,
            "guarded_variant_ranking_ready": True,
            "time_slice_matrix_ready": True,
            "regime_slice_matrix_ready": True,
            "cost_stress_result_ready": True,
            "turnover_constraint_result_ready": True,
            "decision_update_ready": True,
            "best_guarded_variant": decision_update["best_guarded_variant"],
            "best_guarded_variant_decision": decision_update[
                "best_guarded_variant_decision"
            ],
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
    source_retest_plan_path: Path,
    source_guarded_variant_plan_path: Path,
    source_variant_evaluation_plan_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_variant_retest_path: Path,
    source_optimized_variant_ranking_path: Path,
) -> dict[str, Any]:
    retest_plan = _load_json_document(source_retest_plan_path)
    guarded_variant_plan = _load_json_document(source_guarded_variant_plan_path)
    variant_evaluation_plan = _load_json_document(source_variant_evaluation_plan_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    variant_retest = _load_json_document(source_variant_retest_path)
    optimized_variant_ranking = _load_json_document(source_optimized_variant_ranking_path)
    source_status = {
        "retest_plan": _as_mapping(retest_plan).get("status"),
        "guarded_variant_plan": _as_mapping(guarded_variant_plan).get("status"),
        "variant_evaluation_plan": _as_mapping(variant_evaluation_plan).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "variant_retest": _as_mapping(variant_retest).get("status"),
        "optimized_variant_ranking": _as_mapping(optimized_variant_ranking).get(
            "status"
        ),
    }
    source_files = {
        "retest_plan": source_retest_plan_path,
        "guarded_variant_plan": source_guarded_variant_plan_path,
        "variant_evaluation_plan": source_variant_evaluation_plan_path,
        "candidate_ranking": source_candidate_ranking_path,
        "sensitivity_result": source_sensitivity_result_path,
        "sensitivity_decision_update": source_sensitivity_decision_update_path,
        "variant_retest": source_variant_retest_path,
        "optimized_variant_ranking": source_optimized_variant_ranking_path,
    }
    source_hashes = {
        key: _file_sha256(path) if Path(path).exists() else None
        for key, path in source_files.items()
    }
    planned = _planned_variants_from_2382(
        retest_plan=retest_plan,
        guarded_variant_plan=guarded_variant_plan,
    )
    source_validation_errors = _source_validation_errors(
        source_status=source_status,
        retest_plan=retest_plan,
        guarded_variant_plan=guarded_variant_plan,
        variant_evaluation_plan=variant_evaluation_plan,
        candidate_ranking=candidate_ranking,
        sensitivity_result=sensitivity_result,
        sensitivity_decision_update=sensitivity_decision_update,
        variant_retest=variant_retest,
        optimized_variant_ranking=optimized_variant_ranking,
        planned_variants=planned,
    )
    return {
        "source_status": source_status,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": source_hashes,
        "source_ready_for_guarded_variant_retest": not source_validation_errors,
        "source_validation_errors": source_validation_errors,
        "retest_plan": retest_plan,
        "guarded_variant_plan": guarded_variant_plan,
        "variant_evaluation_plan": variant_evaluation_plan,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "variant_retest": variant_retest,
        "optimized_variant_ranking": optimized_variant_ranking,
        "planned_variants_from_2382": planned,
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    retest_plan: Mapping[str, Any],
    guarded_variant_plan: Mapping[str, Any],
    variant_evaluation_plan: Mapping[str, Any],
    candidate_ranking: Mapping[str, Any],
    sensitivity_result: Mapping[str, Any],
    sensitivity_decision_update: Mapping[str, Any],
    variant_retest: Mapping[str, Any],
    optimized_variant_ranking: Mapping[str, Any],
    planned_variants: Sequence[str],
) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "retest_plan": SOURCE_2382_READY_STATUS,
        "guarded_variant_plan": SOURCE_2382_READY_STATUS,
        "variant_evaluation_plan": SOURCE_2382_READY_STATUS,
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
    }
    for key, expected in expected_status.items():
        if source_status.get(key) != expected:
            errors.append(f"{key}_status_not_ready")
    if _as_mapping(retest_plan).get("recommended_next_research_task") not in {
        SOURCE_2382_EXPECTED_ROUTE,
        None,
    }:
        errors.append("retest_plan_route_not_trading_2383")
    if _as_mapping(retest_plan).get("ranking_top_candidate") != RANKING_TOP_CANDIDATE:
        errors.append("retest_plan_ranking_top_mismatch")
    guardrails = set(_as_list(_as_mapping(retest_plan).get("guardrail_reference_candidates")))
    if not {LOWER_TURNOVER_REFERENCE, COOLDOWN_BALANCED_REFERENCE}.issubset(guardrails):
        errors.append("retest_plan_missing_guardrail_references")
    if _as_mapping(retest_plan).get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("retest_plan_primary_cadence_not_valid_until_window")
    if not set(GUARDED_VARIANT_IDS).issubset(set(planned_variants)):
        errors.append("retest_plan_missing_guarded_variants")
    all_variants = set(_as_list(_as_mapping(retest_plan).get("all_variant_ids_for_2383")))
    if not set(VARIANTS_TESTED).issubset(all_variants):
        errors.append("retest_plan_missing_all_variant_ids_for_2383")
    evaluation = _as_mapping(
        _as_mapping(variant_evaluation_plan).get("variant_evaluation_plan")
    ) or _as_mapping(variant_evaluation_plan)
    monthly = _as_mapping(evaluation.get("monthly_rebalance"))
    if monthly.get("allowed_for_primary_decision") is not False:
        errors.append("variant_evaluation_monthly_rebalance_primary_not_false")
    if (
        _as_mapping(sensitivity_result).get("primary_execution_cadence")
        != PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("sensitivity_primary_cadence_not_valid_until_window")
    if _as_mapping(variant_retest).get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("variant_retest_primary_cadence_not_valid_until_window")
    if _as_mapping(variant_retest).get("ranking_top_reference") != RANKING_TOP_CANDIDATE:
        errors.append("variant_retest_ranking_top_reference_mismatch")
    if _as_mapping(optimized_variant_ranking).get("best_variant_after_retest") not in {
        COOLDOWN_BALANCED_REFERENCE,
        None,
    }:
        errors.append("optimized_variant_ranking_best_variant_unexpected")
    candidate_ids = {
        str(row.get("candidate_id"))
        for row in _as_list(_as_mapping(candidate_ranking).get("candidate_ranking"))
    }
    if RANKING_TOP_CANDIDATE not in candidate_ids:
        errors.append("candidate_ranking_missing_ranking_top")
    for label, source in (
        ("retest_plan", retest_plan),
        ("guarded_variant_plan", guarded_variant_plan),
        ("variant_evaluation_plan", variant_evaluation_plan),
        ("sensitivity_result", sensitivity_result),
        ("sensitivity_decision_update", sensitivity_decision_update),
        ("variant_retest", variant_retest),
        ("optimized_variant_ranking", optimized_variant_ranking),
    ):
        errors.extend(_side_effect_validation_errors(label, _as_mapping(source)))
    return errors


def _guarded_variant_retest_policy() -> dict[str, Any]:
    return {
        "policy_id": "dynamic_strategy_ranking_top_guarded_variant_retest_v1",
        "status": "research_only_pilot_guarded_variant_retest",
        "owner": "research_governance",
        "version": "v1",
        "rationale": (
            "TRADING-2383 tests whether the TRADING-2365 return leader remains "
            "credible after adding turnover, cooldown, risk-cap and valid-until "
            "guardrails required by TRADING-2382."
        ),
        "thresholds": {
            "turnover_material_improvement_min": TURNOVER_MATERIAL_IMPROVEMENT_MIN,
            "drawdown_material_worse_tolerance": DRAWDOWN_MATERIAL_WORSE_TOLERANCE,
            "return_advantage_retained_min": RETURN_ADVANTAGE_RETAINED_MIN,
            "time_slice_pass_rate_acceptable_min": TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "regime_slice_pass_rate_acceptable_min": REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "high_vol_drawdown_tolerance": HIGH_VOL_DRAWDOWN_TOLERANCE,
        },
        "transformation_parameters": {
            "guarded_step_delta_cap": GUARDED_STEP_DELTA_CAP,
            "conservative_step_delta_cap": CONSERVATIVE_STEP_DELTA_CAP,
            "risk_off_drawdown_threshold": RISK_OFF_DRAWDOWN_THRESHOLD,
            "severe_drawdown_threshold": SEVERE_DRAWDOWN_THRESHOLD,
            "rolling_vol_window": ROLLING_VOL_WINDOW,
            "trend_window": TREND_WINDOW,
            "recovery_window": RECOVERY_WINDOW,
        },
        "review_condition": (
            "TRADING-2384 owner review remains mandatory before any research-only "
            "observation. Paper-shadow, scheduler, production and broker actions "
            "remain disabled."
        ),
    }


def _run_guarded_variant_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    retest_policy: Mapping[str, Any],
) -> dict[str, Any]:
    static_bundle = _static_bundle(prices)
    realistic_scenario = _cost_stress_scenarios()[1]
    base_bundles = {
        candidate_id: _variant_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=realistic_scenario,
        )
        for candidate_id in (
            *VARIANTS_TESTED,
            LOWER_TURNOVER_REFERENCE,
            COOLDOWN_BALANCED_REFERENCE,
        )
    }
    ranking_bundle = base_bundles[RANKING_TOP_CANDIDATE]
    full_rows = [
        _full_sample_result_row(
            candidate_id="static_baseline",
            role="static_baseline",
            bundle=static_bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        )
    ]
    for candidate_id in (
        *VARIANTS_TESTED,
        LOWER_TURNOVER_REFERENCE,
        COOLDOWN_BALANCED_REFERENCE,
    ):
        full_rows.append(
            _full_sample_result_row(
                candidate_id=candidate_id,
                role=_candidate_role(candidate_id),
                bundle=base_bundles[candidate_id],
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
            )
        )
    time_rows = _slice_rows(
        prices=prices,
        slice_group="time_slice",
        slice_definitions=_time_slice_definitions(prices, _targeted_retest_policy()),
        bundles=base_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=RANKING_TOP_CANDIDATE,
    )
    regime_rows = _slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=_regime_slice_definitions(prices, _targeted_retest_policy()),
        bundles=base_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=RANKING_TOP_CANDIDATE,
    )
    cost_rows = _variant_cost_stress_rows(
        prices=prices,
        policies=policies,
        static_bundle=static_bundle,
    )
    constraint_rows = _variant_constraint_rows(
        prices=prices,
        policies=policies,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
    )
    cadence_rows = _cadence_comparison_rows(
        prices=prices,
        policies=policies,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
    )
    return {
        "guarded_variant_retest_result": full_rows,
        "time_slice_matrix": time_rows,
        "regime_slice_matrix": regime_rows,
        "cost_stress_result": cost_rows,
        "turnover_constraint_result": constraint_rows,
        "cadence_comparison_result": cadence_rows,
        "guarded_variant_retest_policy": retest_policy,
    }


def _variant_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    cadence: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    target_weights = _guarded_target_weights(candidate_id=candidate_id, prices=prices)
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


def _guarded_target_weights(*, candidate_id: str, prices: pd.DataFrame) -> pd.DataFrame:
    ranking = _candidate_target_weights(
        candidate_id=RANKING_TOP_CANDIDATE,
        ranking_top=RANKING_TOP_CANDIDATE,
        robustness_top=LOWER_TURNOVER_REFERENCE,
        prices=prices,
    )
    lower = _candidate_target_weights(
        candidate_id=LOWER_TURNOVER_REFERENCE,
        ranking_top=RANKING_TOP_CANDIDATE,
        robustness_top=LOWER_TURNOVER_REFERENCE,
        prices=prices,
    )
    cooldown = _lower_turnover_cooldown_weights(prices=prices)
    if candidate_id == RANKING_TOP_CANDIDATE:
        return ranking
    if candidate_id == LOWER_TURNOVER_REFERENCE:
        return lower
    if candidate_id == COOLDOWN_BALANCED_REFERENCE:
        return cooldown

    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    returns = prices["QQQ"].pct_change().fillna(0.0)
    rolling_vol = returns.rolling(ROLLING_VOL_WINDOW, min_periods=5).std()
    high_vol = rolling_vol.ge(rolling_vol.quantile(0.75)).fillna(False)
    low_vol = rolling_vol.lt(rolling_vol.median()).fillna(False)
    trend = (
        prices["QQQ"].gt(prices["QQQ"].rolling(TREND_WINDOW, min_periods=5).mean())
        & prices["QQQ"].pct_change(TREND_WINDOW).fillna(0.0).gt(0.0)
    ).fillna(False)
    recovery = drawdown.lt(0.0) & returns.rolling(
        RECOVERY_WINDOW,
        min_periods=5,
    ).sum().gt(0.0)
    risk_off = drawdown.le(RISK_OFF_DRAWDOWN_THRESHOLD) | (high_vol & returns.lt(0.0))
    risk_on = trend & drawdown.gt(RISK_OFF_DRAWDOWN_THRESHOLD)

    if candidate_id == "equal_risk_growth_tilt_guarded_turnover_v1":
        qqq = _bounded_step_delta(
            ranking["QQQ"].rolling(4, min_periods=1).mean(),
            max_delta=GUARDED_STEP_DELTA_CAP,
        )
        qqq = qqq * 0.86 + lower["QQQ"] * 0.14
        lower_bound, upper_bound = 0.18, 0.88
    elif candidate_id == "equal_risk_growth_tilt_guarded_cooldown_v1":
        qqq = ranking["QQQ"].rolling(7, min_periods=1).mean() * 0.82 + lower["QQQ"] * 0.18
        qqq.loc[~(trend | recovery)] = (
            qqq.loc[~(trend | recovery)] * 0.78 + lower.loc[~(trend | recovery), "QQQ"] * 0.22
        )
        qqq = _bounded_step_delta(qqq, max_delta=GUARDED_STEP_DELTA_CAP)
        lower_bound, upper_bound = 0.18, 0.84
    elif candidate_id == "equal_risk_growth_tilt_guarded_risk_cap_v1":
        qqq = ranking["QQQ"].copy()
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"] * 0.88 + ranking.loc[
            risk_off,
            "QQQ",
        ] * 0.12
        qqq.loc[drawdown.le(SEVERE_DRAWDOWN_THRESHOLD)] = lower.loc[
            drawdown.le(SEVERE_DRAWDOWN_THRESHOLD),
            "QQQ",
        ]
        qqq = qqq.rolling(3, min_periods=1).mean()
        lower_bound, upper_bound = 0.16, 0.82
    elif candidate_id == "equal_risk_growth_tilt_guarded_valid_until_decay_v1":
        qqq = ranking["QQQ"].rolling(5, min_periods=1).mean()
        decay_mask = high_vol | (~trend & returns.lt(0.0))
        qqq.loc[decay_mask] = qqq.loc[decay_mask] * 0.72 + lower.loc[
            decay_mask,
            "QQQ",
        ] * 0.28
        qqq = _bounded_step_delta(qqq, max_delta=GUARDED_STEP_DELTA_CAP)
        lower_bound, upper_bound = 0.18, 0.84
    elif candidate_id == "equal_risk_growth_tilt_lower_turnover_fusion_v1":
        qqq = ranking["QQQ"] * 0.62 + cooldown["QQQ"] * 0.38
        qqq.loc[risk_on | low_vol] = ranking.loc[risk_on | low_vol, "QQQ"] * 0.72 + cooldown.loc[
            risk_on | low_vol,
            "QQQ",
        ] * 0.28
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"] * 0.72 + ranking.loc[
            risk_off,
            "QQQ",
        ] * 0.28
        qqq = _bounded_step_delta(
            qqq.rolling(3, min_periods=1).mean(),
            max_delta=GUARDED_STEP_DELTA_CAP,
        )
        lower_bound, upper_bound = 0.18, 0.86
    elif candidate_id == "equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1":
        qqq = ranking["QQQ"] * 0.48 + cooldown["QQQ"] * 0.52
        qqq.loc[risk_on & low_vol] = ranking.loc[risk_on & low_vol, "QQQ"] * 0.58 + cooldown.loc[
            risk_on & low_vol,
            "QQQ",
        ] * 0.42
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"] * 0.90 + ranking.loc[
            risk_off,
            "QQQ",
        ] * 0.10
        qqq = _bounded_step_delta(
            qqq.rolling(6, min_periods=1).mean(),
            max_delta=CONSERVATIVE_STEP_DELTA_CAP,
        )
        lower_bound, upper_bound = 0.20, 0.80
    else:
        raise ValueError(f"Unknown TRADING-2383 guarded variant: {candidate_id}")
    return _normalized_qqq_sgov(qqq, index=prices.index, lower=lower_bound, upper=upper_bound)


def _lower_turnover_cooldown_weights(*, prices: pd.DataFrame) -> pd.DataFrame:
    ranking = _candidate_target_weights(
        candidate_id=RANKING_TOP_CANDIDATE,
        ranking_top=RANKING_TOP_CANDIDATE,
        robustness_top=LOWER_TURNOVER_REFERENCE,
        prices=prices,
    )
    lower = _candidate_target_weights(
        candidate_id=LOWER_TURNOVER_REFERENCE,
        ranking_top=RANKING_TOP_CANDIDATE,
        robustness_top=LOWER_TURNOVER_REFERENCE,
        prices=prices,
    )
    returns = prices["QQQ"].pct_change().fillna(0.0)
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    recovery = drawdown.lt(0.0) & returns.rolling(
        RECOVERY_WINDOW,
        min_periods=5,
    ).sum().gt(0.0)
    qqq = (lower["QQQ"] * 0.76 + ranking["QQQ"] * 0.24).rolling(4, min_periods=1).mean()
    mask = recovery & drawdown.gt(SEVERE_DRAWDOWN_THRESHOLD)
    qqq.loc[mask] = lower.loc[mask, "QQQ"] * 0.68 + ranking.loc[mask, "QQQ"] * 0.32
    return _normalized_qqq_sgov(qqq, index=prices.index, lower=0.20, upper=0.82)


def _variant_cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _cost_stress_scenarios():
        ranking_bundle = _variant_bundle(
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
        for candidate_id in (
            *VARIANTS_TESTED,
            LOWER_TURNOVER_REFERENCE,
            COOLDOWN_BALANCED_REFERENCE,
        ):
            bundle = ranking_bundle if candidate_id == RANKING_TOP_CANDIDATE else _variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
            )
            rows.append(
                _stress_result_row(
                    candidate_id=candidate_id,
                    role=_candidate_role(candidate_id),
                    scenario=scenario,
                    bundle=bundle,
                    static_bundle=static_bundle,
                    ranking_bundle=ranking_bundle,
                    primary_candidate=RANKING_TOP_CANDIDATE,
                )
            )
    return rows


def _variant_constraint_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _execution_constraint_scenarios():
        for candidate_id in VARIANTS_TESTED:
            bundle = _variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
            )
            row = _stress_result_row(
                candidate_id=candidate_id,
                role=f"{_candidate_role(candidate_id)}_constraint_stress",
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
            row["max_turnover_per_month_enforcement_mode"] = "diagnostic_limit_check"
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
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scenario = _cost_stress_scenarios()[1]
    for cadence in COMPARISON_CADENCES:
        for candidate_id in VARIANTS_TESTED:
            bundle = _variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=cadence,
                scenario=scenario,
            )
            row = _full_sample_result_row(
                candidate_id=candidate_id,
                role=f"{_candidate_role(candidate_id)}_{cadence}",
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
            )
            row["comparison_cadence"] = cadence
            row["monthly_rebalance_allowed_for_primary_decision"] = False
            rows.append(row)
    return rows


def _guarded_variant_ranking(retest: Mapping[str, Any]) -> list[dict[str, Any]]:
    full_by_candidate = {
        str(row.get("candidate_id")): _as_mapping(row)
        for row in _as_list(retest.get("guarded_variant_retest_result"))
    }
    cost_by_candidate = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    ranking_row = full_by_candidate[RANKING_TOP_CANDIDATE]
    lower_row = full_by_candidate[LOWER_TURNOVER_REFERENCE]
    cooldown_row = full_by_candidate[COOLDOWN_BALANCED_REFERENCE]
    static_row = full_by_candidate["static_baseline"]
    ranking_perf = _as_mapping(ranking_row.get("performance_metrics"))
    lower_perf = _as_mapping(lower_row.get("performance_metrics"))
    cooldown_perf = _as_mapping(cooldown_row.get("performance_metrics"))
    static_perf = _as_mapping(static_row.get("performance_metrics"))
    ranking_execution = _as_mapping(ranking_row.get("execution_metrics"))
    ranking_return_advantage_vs_static = _float(
        _as_mapping(ranking_row.get("relative_metrics")).get("dynamic_vs_static_gap")
    )
    ranking_turnover = _float(ranking_execution.get("turnover"))
    ranking_drawdown = _float(ranking_perf.get("max_drawdown"))
    lower_drawdown = _float(lower_perf.get("max_drawdown"))
    high_vol_ranking = _slice_row(
        retest.get("regime_slice_matrix"),
        RANKING_TOP_CANDIDATE,
        "high_volatility",
    )
    ranking_rows: list[dict[str, Any]] = []
    for variant_id in VARIANTS_TESTED:
        row = full_by_candidate[variant_id]
        perf = _as_mapping(row.get("performance_metrics"))
        rel = _as_mapping(row.get("relative_metrics"))
        execution = _as_mapping(row.get("execution_metrics"))
        realistic = cost_by_candidate.get((variant_id, "realistic"), {})
        conservative = cost_by_candidate.get((variant_id, "conservative"), {})
        harsh = cost_by_candidate.get((variant_id, "harsh"), {})
        high_vol = _slice_row(retest.get("regime_slice_matrix"), variant_id, "high_volatility")
        annual = _float(perf.get("annualized_return"))
        ranking_annual = _float(ranking_perf.get("annualized_return"))
        lower_annual = _float(lower_perf.get("annualized_return"))
        cooldown_annual = _float(cooldown_perf.get("annualized_return"))
        turnover = _float(execution.get("turnover"))
        max_drawdown = _float(perf.get("max_drawdown"))
        time_pass = _pass_rate_for_candidate(retest.get("time_slice_matrix"), variant_id)
        regime_pass = _pass_rate_for_candidate(retest.get("regime_slice_matrix"), variant_id)
        dynamic_gap = _float(rel.get("dynamic_vs_static_gap"))
        variant_vs_ranking_gap = round(annual - ranking_annual, 6)
        variant_vs_lower_gap = round(annual - lower_annual, 6)
        variant_vs_cooldown_gap = round(annual - cooldown_annual, 6)
        return_advantage_retained = _return_advantage_retained(
            annual=annual,
            static_annual=_float(static_perf.get("annualized_return")),
            ranking_advantage=ranking_return_advantage_vs_static,
        )
        turnover_reduction = _safe_ratio(ranking_turnover - turnover, ranking_turnover)
        drawdown_improvement = round(abs(ranking_drawdown) - abs(max_drawdown), 6)
        high_vol_drawdown_gap = _high_vol_drawdown_gap(
            high_vol,
            high_vol_ranking,
        )
        realistic_passed = _scenario_gap(realistic) > 0.0
        conservative_passed = _scenario_gap(conservative) > 0.0
        harsh_passed = _scenario_gap(harsh) > 0.0
        valid_until_preserved = _valid_until_guardrail_preserved(row)
        no_stale_signal = _float(execution.get("stale_signal_execution_count")) == 0.0
        turnover_improved = (
            variant_id == RANKING_TOP_CANDIDATE
            or turnover_reduction >= TURNOVER_MATERIAL_IMPROVEMENT_MIN
        )
        drawdown_not_worse_than_lower = (
            abs(max_drawdown) <= abs(lower_drawdown) + DRAWDOWN_MATERIAL_WORSE_TOLERANCE
        )
        high_vol_not_degraded = high_vol_drawdown_gap <= HIGH_VOL_DRAWDOWN_TOLERANCE
        decision, reasons = _variant_decision(
            variant_id=variant_id,
            dynamic_gap=dynamic_gap,
            realistic_passed=realistic_passed,
            conservative_passed=conservative_passed,
            harsh_passed=harsh_passed,
            return_advantage_retained=return_advantage_retained,
            turnover_improved=turnover_improved,
            drawdown_not_worse_than_lower=drawdown_not_worse_than_lower,
            time_pass_rate=time_pass,
            regime_pass_rate=regime_pass,
            valid_until_preserved=valid_until_preserved,
            no_stale_signal=no_stale_signal,
            variant_vs_lower_gap=variant_vs_lower_gap,
            high_vol_not_degraded=high_vol_not_degraded,
        )
        ranking_rows.append(
            {
                "variant_id": variant_id,
                "candidate_id": variant_id,
                "rank": None,
                "total_return": perf.get("total_return"),
                "annualized_return": perf.get("annualized_return"),
                "cost_adjusted_return": perf.get("annualized_return"),
                "max_drawdown": perf.get("max_drawdown"),
                "volatility": perf.get("volatility"),
                "sharpe_or_sortino_if_available": perf.get(
                    "sharpe_or_sortino_if_available"
                ),
                "downside_capture": perf.get("downside_capture"),
                "upside_capture": perf.get("upside_capture"),
                "dynamic_vs_static_gap": dynamic_gap,
                "guarded_variant_vs_original_ranking_top_gap": variant_vs_ranking_gap,
                "guarded_variant_vs_lower_turnover_gap": variant_vs_lower_gap,
                "guarded_variant_vs_cooldown_balanced_gap": variant_vs_cooldown_gap,
                "return_advantage_retained_vs_original_ranking_top": return_advantage_retained,
                "return_gap_reduction_vs_lower_turnover": round(
                    variant_vs_lower_gap
                    - (
                        _float(lower_perf.get("annualized_return"))
                        - _float(ranking_perf.get("annualized_return"))
                    ),
                    6,
                ),
                "drawdown_gap_vs_static": round(
                    abs(max_drawdown) - abs(_float(static_perf.get("max_drawdown"))),
                    6,
                ),
                "cost_adjusted_dynamic_vs_static_gap": dynamic_gap,
                "turnover": turnover,
                "rebalance_count": execution.get("rebalance_count"),
                "average_holding_days": execution.get("average_holding_days"),
                "max_monthly_turnover": execution.get("max_monthly_turnover"),
                "signal_to_execution_lag_days": execution.get(
                    "signal_to_execution_lag_days"
                ),
                "stale_signal_execution_count": execution.get(
                    "stale_signal_execution_count"
                ),
                "missed_signal_count": execution.get("missed_signal_count"),
                "cooldown_block_count": execution.get("cooldown_block_count"),
                "constraint_hit_count": execution.get("constraint_hit_count"),
                "time_slice_pass_rate": time_pass,
                "regime_slice_pass_rate": regime_pass,
                "realistic_cost_passed": realistic_passed,
                "conservative_cost_passed": conservative_passed,
                "harsh_cost_passed": harsh_passed,
                "cost_stress_survival": _cost_stress_survival(
                    realistic_passed=realistic_passed,
                    conservative_passed=conservative_passed,
                    harsh_passed=harsh_passed,
                ),
                "turnover_reduction_vs_original_ranking_top": turnover_reduction,
                "drawdown_improvement_vs_original_ranking_top": drawdown_improvement,
                "turnover_profile_improved_vs_original_ranking_top": turnover_improved,
                "valid_until_guardrail_preserved": valid_until_preserved,
                "no_stale_signal_carry_forward": no_stale_signal,
                "high_volatility_behavior_not_degraded": high_vol_not_degraded,
                "owner_review_triggered": decision == DECISION_OWNER_REVIEW,
                "monthly_rebalance_allowed_for_primary_decision": False,
                "decision": decision,
                "decision_reason": reasons,
            }
        )
    ranking_rows.sort(key=_ranking_sort_key)
    for index, row in enumerate(ranking_rows, start=1):
        row["rank"] = index
    return ranking_rows


def _variant_decision(
    *,
    variant_id: str,
    dynamic_gap: float,
    realistic_passed: bool,
    conservative_passed: bool,
    harsh_passed: bool,
    return_advantage_retained: float,
    turnover_improved: bool,
    drawdown_not_worse_than_lower: bool,
    time_pass_rate: float,
    regime_pass_rate: float,
    valid_until_preserved: bool,
    no_stale_signal: bool,
    variant_vs_lower_gap: float,
    high_vol_not_degraded: bool,
) -> tuple[str, list[str]]:
    reasons = [
        f"dynamic_gap={dynamic_gap}",
        f"realistic_passed={realistic_passed}",
        f"conservative_passed={conservative_passed}",
        f"harsh_passed={harsh_passed}",
        f"return_advantage_retained={return_advantage_retained}",
        f"turnover_improved={turnover_improved}",
        f"drawdown_not_worse_than_lower={drawdown_not_worse_than_lower}",
        f"time_slice_pass_rate={time_pass_rate}",
        f"regime_slice_pass_rate={regime_pass_rate}",
        f"variant_vs_lower_gap={variant_vs_lower_gap}",
    ]
    time_ok = time_pass_rate >= TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
    regime_ok = regime_pass_rate >= REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
    return_retained = return_advantage_retained >= RETURN_ADVANTAGE_RETAINED_MIN
    if variant_id == RANKING_TOP_CANDIDATE:
        return DECISION_OWNER_REVIEW, [*reasons, "original_ranking_top_reference_only"]
    if not dynamic_gap > 0.0 or not realistic_passed:
        return DECISION_REJECT, [*reasons, "blocked_by_static_or_realistic_cost_gap"]
    if not return_retained and variant_vs_lower_gap <= 0.0:
        return DECISION_DEPRECATED, [*reasons, "lost_return_advantage_after_guardrails"]
    if (
        conservative_passed
        and turnover_improved
        and drawdown_not_worse_than_lower
        and valid_until_preserved
        and no_stale_signal
        and time_ok
        and regime_ok
        and return_retained
        and high_vol_not_degraded
    ):
        return DECISION_ACCEPT_RESEARCH_ONLY, [
            *reasons,
            "meets_guarded_research_only_observation_candidate_criteria",
        ]
    if (
        conservative_passed
        and turnover_improved
        and valid_until_preserved
        and no_stale_signal
        and (return_retained or variant_vs_lower_gap > 0.0)
    ):
        return DECISION_OWNER_REVIEW, [
            *reasons,
            "improved_but_owner_review_required",
        ]
    if harsh_passed or return_retained or turnover_improved or time_ok or regime_ok:
        return DECISION_CONTINUE_OPTIMIZATION, [
            *reasons,
            "partial_improvement_but_acceptance_criteria_not_met",
        ]
    return DECISION_REJECT, [*reasons, "insufficient_guarded_retest_result"]


def _decision_update(
    *,
    ranking: Sequence[Mapping[str, Any]],
    retest: Mapping[str, Any],
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    best = _as_mapping(ranking[0]) if ranking else {}
    best_variant = str(best.get("variant_id", ""))
    decision = str(best.get("decision", DECISION_REJECT))
    return {
        "schema_version": "dynamic_strategy_guarded_variant_decision_update.v1",
        "decision_update_ready": True,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "guardrail_reference_candidates": [
            LOWER_TURNOVER_REFERENCE,
            COOLDOWN_BALANCED_REFERENCE,
        ],
        "source_retest_plan_ready": bool(
            sources.get("source_ready_for_guarded_variant_retest")
        ),
        "best_guarded_variant": best_variant,
        "best_guarded_variant_decision": decision,
        "candidate_ready_for_research_only_observation": (
            decision == DECISION_ACCEPT_RESEARCH_ONLY
        ),
        "guarded_variant_ranking": list(ranking),
        "best_guarded_variant_metrics": best,
        "recommended_next_research_task": NEXT_ROUTE,
        "monthly_rebalance_allowed_for_primary_decision": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "decision_reasons": _as_list(best.get("decision_reason")),
        "required_outputs_ready": {
            "variants_tested": bool(retest.get("guarded_variant_retest_result")),
            "guarded_variant_ranking": bool(ranking),
            "time_slice_matrix": bool(retest.get("time_slice_matrix")),
            "regime_slice_matrix": bool(retest.get("regime_slice_matrix")),
            "cost_stress_result": bool(retest.get("cost_stress_result")),
            "turnover_constraint_result": bool(retest.get("turnover_constraint_result")),
        },
    }


def _summary_findings(
    *,
    ranking: Sequence[Mapping[str, Any]],
    decision_update: Mapping[str, Any],
) -> dict[str, Any]:
    best = _as_mapping(ranking[0]) if ranking else {}
    return {
        "best_guarded_variant": decision_update.get("best_guarded_variant"),
        "best_guarded_variant_decision": decision_update.get(
            "best_guarded_variant_decision"
        ),
        "best_variant_preserves_ranking_top_upside": _float(
            best.get("return_advantage_retained_vs_original_ranking_top")
        )
        >= RETURN_ADVANTAGE_RETAINED_MIN,
        "best_variant_reduces_turnover_vs_ranking_top": _float(
            best.get("turnover_reduction_vs_original_ranking_top")
        )
        >= TURNOVER_MATERIAL_IMPROVEMENT_MIN,
        "best_variant_improves_drawdown_vs_ranking_top": _float(
            best.get("drawdown_improvement_vs_original_ranking_top")
        )
        > 0.0,
        "best_variant_improves_cost_adjusted_result": _float(
            best.get("cost_adjusted_dynamic_vs_static_gap")
        )
        > 0.0,
        "best_variant_survives_realistic_cost": bool(best.get("realistic_cost_passed")),
        "best_variant_survives_conservative_cost": bool(
            best.get("conservative_cost_passed")
        ),
        "best_variant_survives_harsh_cost": bool(best.get("harsh_cost_passed")),
        "best_variant_outperforms_lower_turnover_reference": _float(
            best.get("guarded_variant_vs_lower_turnover_gap")
        )
        > 0.0,
        "best_variant_outperforms_cooldown_balanced_reference": _float(
            best.get("guarded_variant_vs_cooldown_balanced_gap")
        )
        > 0.0,
        "valid_until_window_remains_default": True,
        "paper_shadow_remains_disabled": True,
        "production_remains_disabled": True,
        "broker_remains_disabled": True,
        "recommended_next_research_task": NEXT_ROUTE,
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
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else None,
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "requested_date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else None,
        },
        "source_tasks": list(SOURCE_TASKS),
        "source_status": dict(sources.get("source_status", {})),
        "source_files": dict(sources.get("source_files", {})),
        "source_hashes": dict(sources.get("source_hashes", {})),
        "source_ready_for_guarded_variant_retest": bool(
            sources.get("source_ready_for_guarded_variant_retest")
        ),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "planned_variants_from_2382": list(sources.get("planned_variants_from_2382", [])),
        "data_quality_gate_executed": True,
        "data_quality_gate": dict(data_quality),
        "data_quality_status": data_quality.get("status"),
        "data_quality_passed": bool(data_quality.get("passed")),
        "prices_path": str(prices_path),
        "marketstack_prices_path": str(marketstack_prices_path),
        "rates_path": str(rates_path),
        "policy_registry_path": str(policy_registry_path),
        "backtest_run": True,
        "fresh_market_data_read": False,
        "research_only": True,
        "observe_only": False,
        "manual_review_required": True,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "promotion_allowed": False,
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
        "broker_action": "none",
        "production_effect": "none",
        "order_generated": False,
        "daily_report_generated": False,
    }


def _blocked_sections(reason: str, sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "block_reason": reason,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "guardrail_reference_candidates": [
            LOWER_TURNOVER_REFERENCE,
            COOLDOWN_BALANCED_REFERENCE,
        ],
        "comparison_candidates": list(COMPARISON_CANDIDATES),
        "variants_tested": list(VARIANTS_TESTED),
        "guarded_variant_retest_ready": False,
        "guarded_variant_ranking_ready": False,
        "time_slice_matrix_ready": False,
        "regime_slice_matrix_ready": False,
        "cost_stress_result_ready": False,
        "turnover_constraint_result_ready": False,
        "decision_update_ready": False,
        "guarded_variant_retest_result": [],
        "guarded_variant_ranking": [],
        "time_slice_matrix": [],
        "regime_slice_matrix": [],
        "cost_stress_result": [],
        "turnover_constraint_result": [],
        "decision_update": {},
        "summary_findings": {
            "recommended_next_research_task": NEXT_ROUTE,
            "paper_shadow_remains_disabled": True,
            "production_remains_disabled": True,
            "broker_remains_disabled": True,
            "source_validation_errors": list(
                sources.get("source_validation_errors", [])
            ),
        },
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "json_path": str(output_root / "guarded_variant_retest_result.json"),
        "guarded_variant_ranking_json": str(output_root / "guarded_variant_ranking.json"),
        "time_regime_slice_matrix_json": str(
            output_root / "time_regime_slice_matrix.json"
        ),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_ranking_top_guarded_variant_retest.md"
        ),
        "ranking_markdown": str(docs_root / "dynamic_strategy_guarded_variant_ranking.md"),
        "slice_matrix_markdown": str(
            docs_root / "dynamic_strategy_guarded_variant_slice_matrix.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2384_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["guarded_variant_ranking_json"]),
        _ranking_document(payload),
    )
    _write_json(
        Path(artifact_paths["time_regime_slice_matrix_json"]),
        _slice_matrix_document(payload),
    )
    _write_json(
        Path(artifact_paths["decision_update_json"]),
        _decision_document(payload),
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["ranking_markdown"]).write_text(
        _ranking_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["slice_matrix_markdown"]).write_text(
        _slice_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _ranking_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_guarded_variant_ranking.v1",
        "report_type": "dynamic_strategy_guarded_variant_ranking",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "ranking": _as_list(payload.get("guarded_variant_ranking")),
        "best_guarded_variant": payload.get("best_guarded_variant"),
        "best_guarded_variant_decision": payload.get("best_guarded_variant_decision"),
        "recommended_next_research_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _slice_matrix_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_guarded_variant_slice_matrix.v1",
        "report_type": "dynamic_strategy_guarded_variant_slice_matrix",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "time_slice_matrix": _as_list(payload.get("time_slice_matrix")),
        "regime_slice_matrix": _as_list(payload.get("regime_slice_matrix")),
        "cost_stress_result": _as_list(payload.get("cost_stress_result")),
        "turnover_constraint_result": _as_list(
            payload.get("turnover_constraint_result")
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _decision_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_guarded_variant_decision_update.v1",
        "report_type": "dynamic_strategy_guarded_variant_decision_update",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "decision_update": _as_mapping(payload.get("decision_update")),
        "summary_findings": _as_mapping(payload.get("summary_findings")),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _main_markdown(payload: Mapping[str, Any]) -> str:
    ranking = _as_list(payload.get("guarded_variant_ranking"))
    best = _as_mapping(ranking[0]) if ranking else {}
    summary = _as_mapping(payload.get("summary_findings"))
    lines = [
        "# 动态策略 ranking top guarded variant retest",
        "",
        "## Executive summary",
        "",
        f"- status：`{payload.get('status')}`",
        f"- data quality：`{payload.get('data_quality_status')}`",
        f"- ranking top candidate：`{payload.get('ranking_top_candidate')}`",
        f"- best guarded variant：`{payload.get('best_guarded_variant')}`",
        f"- best decision：`{payload.get('best_guarded_variant_decision')}`",
        f"- next route：`{NEXT_ROUTE}`",
        "",
        "## Source retest plan from TRADING-2382",
        "",
        "- TRADING-2382 要求从 lower-turnover 优化线切回 2365 收益 top，"
        "但必须叠加 turnover / cooldown / risk-cap / valid-until guardrails 后重新测试。",
        "- 本次 retest 会运行 backtest，但不会批准 paper-shadow / scheduler / "
        "production / broker。",
        "",
        "## Variant definitions",
        "",
        "|variant|role|",
        "|---|---|",
    ]
    for variant in VARIANTS_TESTED:
        role = (
            "original return leader reference"
            if variant == RANKING_TOP_CANDIDATE
            else "guarded ranking-top variant"
        )
        lines.append(f"|`{variant}`|{role}|")
    lines.extend(
        [
            "",
            "## Retest design",
            "",
            f"- primary execution cadence：`{PRIMARY_EXECUTION_CADENCE}`",
            "- comparison cadences：`valid_until_window`, "
            "`cooldown_limited_event_driven`, `signal_event_driven`",
            "- monthly rebalance：legacy reference only；not primary decision",
            "- cost stress：base / realistic / conservative / harsh",
            "- time/regime slice：覆盖 full / recent / post-2023 AI cycle / "
            "high-volatility / recovery 以及 risk-on/off 等切片。",
            "",
            "## Guarded variant ranking",
            "",
            "|rank|variant|decision|annual_return|turnover|time_pass|regime_pass|cost_survival|",
            "|---:|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in ranking:
        item = _as_mapping(row)
        lines.append(
            "|"
            f"{item.get('rank')}|`{item.get('variant_id')}`|`{item.get('decision')}`|"
            f"{item.get('annualized_return')}|"
            f"{item.get('turnover')}|"
            f"{item.get('time_slice_pass_rate')}|"
            f"{item.get('regime_slice_pass_rate')}|"
            f"`{item.get('cost_stress_survival')}`|"
        )
    lines.extend(
        [
            "",
            "## Key answers",
            "",
            f"- 哪个 guarded variant 最好：`{best.get('variant_id')}`",
            "- 是否保留 ranking top 收益优势："
            f"`{summary.get('best_variant_preserves_ranking_top_upside')}`",
            "- 是否降低换手："
            f"`{summary.get('best_variant_reduces_turnover_vs_ranking_top')}`",
            "- 是否改善 cost-adjusted result："
            f"`{summary.get('best_variant_improves_cost_adjusted_result')}`",
            "- 是否改善 drawdown 或 high-volatility 行为："
            f"`{summary.get('best_variant_improves_drawdown_vs_ranking_top')}` / "
            f"`{best.get('high_volatility_behavior_not_degraded')}`",
            "- 是否优于 lower-turnover reference："
            f"`{summary.get('best_variant_outperforms_lower_turnover_reference')}`",
            "- 是否可以进入 research-only observation："
            f"`{payload.get('candidate_ready_for_research_only_observation')}`",
            "- paper-shadow / production / broker：仍全部 disabled / none",
            "",
            "## Recommended next route",
            "",
            f"`{NEXT_ROUTE}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _ranking_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# 动态策略 guarded variant ranking",
        "",
        f"- status：`{payload.get('status')}`",
        f"- best guarded variant：`{payload.get('best_guarded_variant')}`",
        f"- best decision：`{payload.get('best_guarded_variant_decision')}`",
        "",
        "|rank|variant|cost_return|drawdown|turnover|retained_upside|turnover_reduction|decision|",
        "|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in _as_list(payload.get("guarded_variant_ranking")):
        item = _as_mapping(row)
        lines.append(
            "|"
            f"{item.get('rank')}|`{item.get('variant_id')}`|"
            f"{item.get('cost_adjusted_return')}|"
            f"{item.get('max_drawdown')}|"
            f"{item.get('turnover')}|"
            f"{item.get('return_advantage_retained_vs_original_ranking_top')}|"
            f"{item.get('turnover_reduction_vs_original_ranking_top')}|"
            f"`{item.get('decision')}`|"
        )
    return "\n".join(lines) + "\n"


def _slice_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# 动态策略 guarded variant slice matrix",
        "",
        f"- status：`{payload.get('status')}`",
        "",
        "## Time slice",
        "",
        "|variant|slice|gap_static|gap_ranking|turnover|passed|",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in _as_list(payload.get("time_slice_matrix")):
        item = _as_mapping(row)
        if item.get("candidate_id") not in VARIANTS_TESTED:
            continue
        rel = _as_mapping(item.get("relative_metrics"))
        exe = _as_mapping(item.get("execution_metrics"))
        lines.append(
            f"|`{item.get('candidate_id')}`|`{item.get('scenario_id')}`|"
            f"{rel.get('dynamic_vs_static_gap')}|"
            f"{rel.get('dynamic_vs_ranking_top_gap')}|"
            f"{exe.get('turnover')}|`{item.get('slice_passed')}`|"
        )
    lines.extend(
        [
            "",
            "## Regime slice",
            "",
            "|variant|regime|gap_static|gap_ranking|turnover|passed|",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in _as_list(payload.get("regime_slice_matrix")):
        item = _as_mapping(row)
        if item.get("candidate_id") not in VARIANTS_TESTED:
            continue
        rel = _as_mapping(item.get("relative_metrics"))
        exe = _as_mapping(item.get("execution_metrics"))
        lines.append(
            f"|`{item.get('candidate_id')}`|`{item.get('scenario_id')}`|"
            f"{rel.get('dynamic_vs_static_gap')}|"
            f"{rel.get('dynamic_vs_ranking_top_gap')}|"
            f"{exe.get('turnover')}|`{item.get('slice_passed')}`|"
        )
    return "\n".join(lines) + "\n"


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2384 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best guarded variant：`{payload.get('best_guarded_variant')}`",
            f"- best decision：`{payload.get('best_guarded_variant_decision')}`",
            f"- next route：`{NEXT_ROUTE}`",
            "- route boundary：guarded variant owner review and observation decision；"
            "不是 paper-shadow、scheduler、production、broker 或 daily report approval。",
            "",
        ]
    )


def _load_json_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING_SOURCE_ARTIFACT", "path": str(path)}
    return _as_mapping(json.loads(path.read_text(encoding="utf-8")))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _planned_variants_from_2382(
    *,
    retest_plan: Mapping[str, Any],
    guarded_variant_plan: Mapping[str, Any],
) -> list[str]:
    planned = _as_list(_as_mapping(retest_plan).get("planned_variants"))
    if planned:
        return [str(item) for item in planned]
    nested = _as_mapping(guarded_variant_plan).get("planned_variants")
    if isinstance(nested, list):
        return [str(item) for item in nested]
    return [
        str(row.get("candidate_id"))
        for row in _as_list(_as_mapping(guarded_variant_plan).get("variants"))
        if _as_mapping(row).get("include_in_2383_retest") is True
    ]


def _side_effect_validation_errors(label: str, source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in (
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    ):
        if source.get(field) is True:
            errors.append(f"{label}_{field}_true")
    if source.get("broker_action") not in (None, "none"):
        errors.append(f"{label}_broker_action_not_none")
    if source.get("production_effect") not in (None, "none"):
        errors.append(f"{label}_production_effect_not_none")
    return errors


def _signal_validity_profile(cadence: str, policy: Mapping[str, Any]) -> dict[str, Any]:
    validity_days = max(1, int(_float(policy.get("validity_period_days")) or 10))
    return {
        "primary_signal_class": "dynamic_strategy_target_weight",
        "confirmation_required": False,
        "min_validity_days_required_for_execution": 1,
        "stale_after_days": validity_days,
        "near_stale_within_days": 1,
        "stale_action": "suppress_rebalance"
        if cadence == PRIMARY_EXECUTION_CADENCE
        else "hold_previous_position",
        "actual_path_only": True,
    }


def _normalized_qqq_sgov(
    qqq: pd.Series,
    *,
    index: pd.Index,
    lower: float,
    upper: float,
) -> pd.DataFrame:
    clipped = qqq.reindex(index).ffill().fillna(0.5).clip(lower=lower, upper=upper)
    return pd.DataFrame(
        {
            "QQQ": clipped.astype(float),
            "TQQQ": 0.0,
            "SGOV": 1.0 - clipped.astype(float),
        },
        index=index,
    ).reindex(columns=list(ASSET_COLUMNS)).fillna(0.0)


def _bounded_step_delta(series: pd.Series, *, max_delta: float) -> pd.Series:
    values: list[float] = []
    previous: float | None = None
    for raw in series.ffill().fillna(0.5).astype(float):
        current = float(raw)
        if previous is None:
            values.append(current)
            previous = current
            continue
        delta = max(-max_delta, min(max_delta, current - previous))
        bounded = previous + delta
        values.append(bounded)
        previous = bounded
    return pd.Series(values, index=series.index)


def _rows_by_candidate_and_scenario(value: Any) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row.get("candidate_id")), str(row.get("scenario_id"))): _as_mapping(row)
        for row in _as_list(value)
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


def _slice_row(rows: Any, candidate_id: str, scenario_id: str) -> dict[str, Any]:
    for row in _as_list(rows):
        item = _as_mapping(row)
        if item.get("candidate_id") == candidate_id and item.get("scenario_id") == scenario_id:
            return item
    return {}


def _high_vol_drawdown_gap(
    candidate_row: Mapping[str, Any],
    ranking_row: Mapping[str, Any],
) -> float:
    candidate_drawdown = abs(
        _float(_as_mapping(candidate_row.get("performance_metrics")).get("max_drawdown"))
    )
    ranking_drawdown = abs(
        _float(_as_mapping(ranking_row.get("performance_metrics")).get("max_drawdown"))
    )
    return round(candidate_drawdown - ranking_drawdown, 6)


def _valid_until_guardrail_preserved(row: Mapping[str, Any]) -> bool:
    return (
        row.get("execution_cadence") == PRIMARY_EXECUTION_CADENCE
        and row.get("monthly_rebalance_allowed_for_primary_decision") is False
    )


def _return_advantage_retained(
    *,
    annual: float,
    static_annual: float,
    ranking_advantage: float,
) -> float:
    if ranking_advantage <= 0.0:
        return 0.0
    return round(max(0.0, annual - static_annual) / ranking_advantage, 6)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return round(numerator / denominator, 6)


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


def _ranking_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    decision_priority = {
        DECISION_ACCEPT_RESEARCH_ONLY: 0,
        DECISION_OWNER_REVIEW: 1,
        DECISION_CONTINUE_OPTIMIZATION: 2,
        DECISION_REJECT: 3,
        DECISION_DEPRECATED: 4,
    }
    return (
        1 if row.get("variant_id") == RANKING_TOP_CANDIDATE else 0,
        decision_priority.get(str(row.get("decision")), 9),
        -_float(row.get("return_advantage_retained_vs_original_ranking_top")),
        -_float(row.get("turnover_reduction_vs_original_ranking_top")),
        -_float(row.get("time_slice_pass_rate")),
        -_float(row.get("regime_slice_pass_rate")),
        -_float(row.get("cost_adjusted_return")),
        _float(row.get("turnover")),
    )


def _candidate_role(candidate_id: str) -> str:
    if candidate_id == "static_baseline":
        return "static_baseline"
    if candidate_id == RANKING_TOP_CANDIDATE:
        return "original_return_leader_reference"
    if candidate_id == LOWER_TURNOVER_REFERENCE:
        return "lower_turnover_reference"
    if candidate_id == COOLDOWN_BALANCED_REFERENCE:
        return "cooldown_balanced_reference"
    return "guarded_ranking_top_variant"


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
