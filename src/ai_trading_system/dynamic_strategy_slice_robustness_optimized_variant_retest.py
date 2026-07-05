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
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    READY_STATUS as SOURCE_2375_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
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
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
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
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    READY_STATUS as SOURCE_2376_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT,
    PLANNED_VARIANTS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    NEXT_ROUTE as SOURCE_2378_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    READY_STATUS as SOURCE_2378_READY_STATUS,
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

TASK_ID = "TRADING-2379"
TASK_REGISTER_ID = (
    "TRADING-2379_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST"
)
REPORT_TYPE = "dynamic_strategy_slice_robustness_optimized_variant_retest"
SCHEMA_VERSION = "dynamic_strategy_slice_robustness_optimized_variant_retest.v1"
READY_STATUS = "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_"
    "BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_"
    "Observation_Decision"
)
BASE_CANDIDATE_ID = ROBUSTNESS_TOP_FALLBACK
RANKING_TOP_REFERENCE = RANKING_TOP_FALLBACK
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2375",
    "TRADING-2376",
    "TRADING-2378",
)
VARIANTS_TESTED: tuple[str, ...] = (BASE_CANDIDATE_ID, *PLANNED_VARIANTS)
COMPARISON_CANDIDATES: tuple[str, ...] = (
    "static_baseline",
    BASE_CANDIDATE_ID,
    RANKING_TOP_REFERENCE,
)
DECISION_DEPRECATED = "DEPRECATED_BY_VARIANT_RETEST"

# Research-only pilot thresholds for TRADING-2379. They are not promotion gates;
# the owner-review route remains mandatory before any observation decision.
RETURN_GAP_REDUCTION_MIN = 0.0
TURNOVER_PROFILE_TOLERANCE = 0.25
DRAWDOWN_MATERIAL_WORSE_TOLERANCE = 0.02
TIME_PASS_RATE_IMPROVEMENT_MIN = 0.0
REGIME_PASS_RATE_PRESERVE_MIN = 0.0

DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2378_OPTIMIZATION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT
    / "optimization_plan_result.json"
)
DEFAULT_SOURCE_2378_VARIANT_EVALUATION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT
    / "variant_evaluation_plan.json"
)
DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "targeted_retest_result.json"
)
DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "dynamic_strategy_candidate_optimization_divergence_review"
    / "divergence_review_result.json"
)
DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "dynamic_strategy_candidate_optimization_divergence_review"
    / "candidate_decision_update.json"
)
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_slice_robustness_optimized_variant_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Path = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    source_optimization_review_path: Path = DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH,
    source_optimization_decision_update_path: Path = (
        DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH
    ),
    source_targeted_retest_path: Path = DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_targeted_decision_update_path: Path = (
        DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH
    ),
    source_optimization_plan_path: Path = DEFAULT_SOURCE_2378_OPTIMIZATION_PLAN_PATH,
    source_variant_evaluation_plan_path: Path = (
        DEFAULT_SOURCE_2378_VARIANT_EVALUATION_PLAN_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_decision_update_path=source_optimization_decision_update_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        source_optimization_plan_path=source_optimization_plan_path,
        source_variant_evaluation_plan_path=source_variant_evaluation_plan_path,
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
    if not bool(sources["source_ready_for_variant_retest"]):
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
    policy_registry = _load_policy_registry(policy_registry_path)
    policies = _policies_by_id(policy_registry)
    retest_policy = _variant_retest_policy()
    retest = _run_variant_retest(
        prices=prices,
        policies=policies,
        retest_policy=retest_policy,
        base_candidate=BASE_CANDIDATE_ID,
        ranking_top=RANKING_TOP_REFERENCE,
        variants=VARIANTS_TESTED,
    )
    ranking = _optimized_variant_ranking(retest)
    decision_update = _decision_update(ranking=ranking, retest=retest, sources=sources)
    summary = _summary_findings(ranking=ranking, decision_update=decision_update)
    payload.update(
        {
            "variant_retest_policy": retest_policy,
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "comparison_cadences": [
                PRIMARY_EXECUTION_CADENCE,
                "cooldown_limited_event_driven",
            ],
            "monthly_rebalance": {
                "allowed_for_reference": True,
                "allowed_for_primary_decision": False,
            },
            "base_candidate": BASE_CANDIDATE_ID,
            "ranking_top_reference": RANKING_TOP_REFERENCE,
            "comparison_candidates": list(COMPARISON_CANDIDATES),
            "variants_tested": list(VARIANTS_TESTED),
            "variant_retest_result": retest["variant_retest_result"],
            "optimized_variant_ranking": ranking,
            "time_slice_matrix": retest["time_slice_matrix"],
            "regime_slice_matrix": retest["regime_slice_matrix"],
            "cost_stress_result": retest["cost_stress_result"],
            "turnover_constraint_result": retest["turnover_constraint_result"],
            "cadence_comparison_result": retest["cadence_comparison_result"],
            "decision_update": decision_update,
            "summary_findings": summary,
            "variant_retest_ready": True,
            "optimized_variant_ranking_ready": True,
            "time_slice_matrix_ready": True,
            "regime_slice_matrix_ready": True,
            "cost_stress_result_ready": True,
            "turnover_constraint_result_ready": True,
            "decision_update_ready": True,
            "best_variant_after_retest": decision_update["best_variant_after_retest"],
            "best_variant_decision": decision_update["best_variant_decision"],
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
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_optimization_review_path: Path,
    source_optimization_decision_update_path: Path,
    source_targeted_retest_path: Path,
    source_targeted_decision_update_path: Path,
    source_optimization_plan_path: Path,
    source_variant_evaluation_plan_path: Path,
) -> dict[str, Any]:
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    optimization_review = _load_json_document(source_optimization_review_path)
    optimization_decision_update = _load_json_document(
        source_optimization_decision_update_path
    )
    targeted_retest = _load_json_document(source_targeted_retest_path)
    targeted_decision_update = _load_json_document(source_targeted_decision_update_path)
    optimization_plan = _load_json_document(source_optimization_plan_path)
    variant_evaluation_plan = _load_json_document(source_variant_evaluation_plan_path)
    source_status = {
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "optimization_review": _as_mapping(optimization_review).get("status"),
        "optimization_decision_update": _as_mapping(
            optimization_decision_update
        ).get("status"),
        "targeted_retest": _as_mapping(targeted_retest).get("status"),
        "targeted_decision_update": _as_mapping(targeted_decision_update).get("status"),
        "optimization_plan": _as_mapping(optimization_plan).get("status"),
        "variant_evaluation_plan": _as_mapping(variant_evaluation_plan).get("status"),
    }
    source_files = {
        "event_retest": source_event_retest_path,
        "candidate_ranking": source_candidate_ranking_path,
        "sensitivity_result": source_sensitivity_result_path,
        "sensitivity_decision_update": source_sensitivity_decision_update_path,
        "optimization_review": source_optimization_review_path,
        "optimization_decision_update": source_optimization_decision_update_path,
        "targeted_retest": source_targeted_retest_path,
        "targeted_decision_update": source_targeted_decision_update_path,
        "optimization_plan": source_optimization_plan_path,
        "variant_evaluation_plan": source_variant_evaluation_plan_path,
    }
    source_hashes = {
        key: _file_sha256(path) if Path(path).exists() else None
        for key, path in source_files.items()
    }
    decision_from_2376 = _decision_from_2376(
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
    )
    primary_candidate = str(
        _as_mapping(optimization_plan).get(
            "primary_candidate",
            _as_mapping(targeted_retest).get("primary_candidate", ""),
        )
    )
    ranking_top = str(
        _as_mapping(optimization_plan).get(
            "ranking_top_reference",
            _as_mapping(targeted_retest).get("ranking_top_from_2365", ""),
        )
    )
    source_validation_errors = _source_validation_errors(
        source_status=source_status,
        event_retest=event_retest,
        candidate_ranking=candidate_ranking,
        sensitivity_result=sensitivity_result,
        sensitivity_decision_update=sensitivity_decision_update,
        optimization_review=optimization_review,
        optimization_decision_update=optimization_decision_update,
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
        optimization_plan=optimization_plan,
        variant_evaluation_plan=variant_evaluation_plan,
        decision_from_2376=decision_from_2376,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
    )
    return {
        "source_status": source_status,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": source_hashes,
        "source_ready_for_variant_retest": not source_validation_errors,
        "source_validation_errors": source_validation_errors,
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "optimization_review": optimization_review,
        "optimization_decision_update": optimization_decision_update,
        "targeted_retest": targeted_retest,
        "targeted_decision_update": targeted_decision_update,
        "optimization_plan": optimization_plan,
        "variant_evaluation_plan": variant_evaluation_plan,
        "decision_from_2376": decision_from_2376,
        "primary_candidate": primary_candidate,
        "ranking_top_reference": ranking_top,
        "planned_variants_from_2378": list(_as_list(optimization_plan.get("planned_variants"))),
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    event_retest: Mapping[str, Any],
    candidate_ranking: Mapping[str, Any],
    sensitivity_result: Mapping[str, Any],
    sensitivity_decision_update: Mapping[str, Any],
    optimization_review: Mapping[str, Any],
    optimization_decision_update: Mapping[str, Any],
    targeted_retest: Mapping[str, Any],
    targeted_decision_update: Mapping[str, Any],
    optimization_plan: Mapping[str, Any],
    variant_evaluation_plan: Mapping[str, Any],
    decision_from_2376: str,
    primary_candidate: str,
    ranking_top: str,
) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "event_retest": SOURCE_2365_READY_STATUS,
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
        "optimization_review": SOURCE_2375_READY_STATUS,
        "optimization_decision_update": SOURCE_2375_READY_STATUS,
        "targeted_retest": SOURCE_2376_READY_STATUS,
        "targeted_decision_update": SOURCE_2376_READY_STATUS,
        "optimization_plan": SOURCE_2378_READY_STATUS,
        "variant_evaluation_plan": SOURCE_2378_READY_STATUS,
    }
    for key, expected in expected_status.items():
        if source_status.get(key) != expected:
            errors.append(f"{key}_status_not_ready")
    if decision_from_2376 != DECISION_CONTINUE_OPTIMIZATION:
        errors.append("targeted_retest_decision_not_continue_optimization")
    if primary_candidate != BASE_CANDIDATE_ID:
        errors.append("primary_candidate_not_base_lower_turnover")
    if ranking_top != RANKING_TOP_REFERENCE:
        errors.append("ranking_top_reference_mismatch")
    if _as_mapping(optimization_plan).get("recommended_next_research_task") not in {
        SOURCE_2378_EXPECTED_ROUTE,
        None,
    }:
        errors.append("optimization_plan_next_route_mismatch")
    planned = set(_as_list(_as_mapping(optimization_plan).get("planned_variants")))
    if not set(PLANNED_VARIANTS).issubset(planned):
        errors.append("optimization_plan_missing_required_variants")
    required_tests = _as_mapping(
        _as_mapping(variant_evaluation_plan).get("variant_evaluation_plan")
    ).get("required_2379_tests")
    if not _as_mapping(required_tests):
        errors.append("variant_evaluation_plan_missing_required_tests")
    for label, source in (
        ("event_retest", event_retest),
        ("sensitivity_result", sensitivity_result),
        ("optimization_review", optimization_review),
        ("optimization_decision_update", optimization_decision_update),
        ("targeted_retest", targeted_retest),
        ("targeted_decision_update", targeted_decision_update),
        ("optimization_plan", optimization_plan),
        ("candidate_ranking", candidate_ranking),
        ("sensitivity_decision_update", sensitivity_decision_update),
    ):
        errors.extend(_side_effect_validation_errors(label, _as_mapping(source)))
    return errors


def _variant_retest_policy() -> dict[str, Any]:
    return {
        "policy_id": "dynamic_strategy_slice_robustness_variant_retest_v1",
        "status": "research_only_pilot_variant_retest",
        "owner": "research_governance",
        "version": "v1",
        "rationale": (
            "TRADING-2379 converts the 2378 repair plan into actual-path variant "
            "retests while preserving the no-execution boundary."
        ),
        "thresholds": {
            "return_gap_reduction_min": RETURN_GAP_REDUCTION_MIN,
            "turnover_profile_tolerance": TURNOVER_PROFILE_TOLERANCE,
            "drawdown_material_worse_tolerance": DRAWDOWN_MATERIAL_WORSE_TOLERANCE,
            "time_pass_rate_improvement_min": TIME_PASS_RATE_IMPROVEMENT_MIN,
            "regime_pass_rate_preserve_min": REGIME_PASS_RATE_PRESERVE_MIN,
        },
        "variant_transformation_policy": _variant_transformation_policy(),
        "review_condition": (
            "TRADING-2380 owner review is required before research-only "
            "observation. Paper-shadow, production and broker actions remain disabled."
        ),
    }


def _variant_transformation_policy() -> dict[str, Any]:
    return {
        "policy_id": "trading_2379_variant_target_weight_transformations_v1",
        "status": "research_only_pilot",
        "base_reference": BASE_CANDIDATE_ID,
        "ranking_top_reference": RANKING_TOP_REFERENCE,
        "parameters": {
            "severe_drawdown_threshold": -0.10,
            "drawdown_guard_threshold": -0.08,
            "rolling_volatility_window_sessions": 20,
            "trend_window_sessions": 60,
            "recovery_window_sessions": 20,
        },
        "variant_rules": {
            "dynamic_regime_overlay_v0_4_reentry_repair_v1": (
                "Move part-way toward ranking top during recovery or confirmed "
                "trend, but revert to base exposure in severe drawdowns."
            ),
            "dynamic_regime_overlay_v0_4_upside_capture_guarded_v1": (
                "Increase guarded upside capture in risk-on / low-volatility "
                "windows with drawdown guard preserved."
            ),
            "dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1": (
                "Smooth base exposure and decay toward base in high-volatility "
                "negative-return windows."
            ),
            "dynamic_regime_overlay_v0_4_cooldown_balanced_v1": (
                "Blend base and ranking exposures with short smoothing to reduce "
                "missed upside while limiting churn."
            ),
            "dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1": (
                "Use the largest bounded blend toward ranking top, with drawdown "
                "guardrails still active."
            ),
        },
    }


def _run_variant_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    retest_policy: Mapping[str, Any],
    base_candidate: str,
    ranking_top: str,
    variants: Sequence[str],
) -> dict[str, Any]:
    static_bundle = _static_bundle(prices)
    realistic_scenario = _cost_stress_scenarios()[1]
    base_bundles = {
        candidate_id: _variant_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            ranking_top=ranking_top,
            base_candidate=base_candidate,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=realistic_scenario,
        )
        for candidate_id in (*variants, ranking_top)
    }
    ranking_bundle = base_bundles[ranking_top]
    full_rows = [
        _full_sample_result_row(
            candidate_id="static_baseline",
            role="static_baseline",
            bundle=static_bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        )
    ]
    for candidate_id in (*variants, ranking_top):
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
        primary_candidate=base_candidate,
    )
    regime_rows = _slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=_regime_slice_definitions(prices, _targeted_retest_policy()),
        bundles=base_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=base_candidate,
    )
    cost_rows = _variant_cost_stress_rows(
        prices=prices,
        policies=policies,
        variants=variants,
        static_bundle=static_bundle,
        ranking_top=ranking_top,
        base_candidate=base_candidate,
    )
    constraint_rows = _variant_constraint_rows(
        prices=prices,
        policies=policies,
        variants=variants,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        ranking_top=ranking_top,
        base_candidate=base_candidate,
    )
    cadence_rows = _cadence_comparison_rows(
        prices=prices,
        policies=policies,
        variants=variants,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        ranking_top=ranking_top,
        base_candidate=base_candidate,
    )
    return {
        "variant_retest_result": full_rows,
        "time_slice_matrix": time_rows,
        "regime_slice_matrix": regime_rows,
        "cost_stress_result": cost_rows,
        "turnover_constraint_result": constraint_rows,
        "cadence_comparison_result": cadence_rows,
        "variant_retest_policy": retest_policy,
    }


def _variant_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    ranking_top: str,
    base_candidate: str,
    cadence: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    target_weights = _variant_target_weights(
        candidate_id=candidate_id,
        ranking_top=ranking_top,
        base_candidate=base_candidate,
        prices=prices,
    )
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


def _variant_target_weights(
    *,
    candidate_id: str,
    ranking_top: str,
    base_candidate: str,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    ranking = _candidate_target_weights(
        candidate_id=ranking_top,
        ranking_top=ranking_top,
        robustness_top=base_candidate,
        prices=prices,
    )
    base = _candidate_target_weights(
        candidate_id=base_candidate,
        ranking_top=ranking_top,
        robustness_top=base_candidate,
        prices=prices,
    )
    if candidate_id == ranking_top:
        return ranking
    if candidate_id == base_candidate:
        return base
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    returns = prices["QQQ"].pct_change().fillna(0.0)
    rolling_vol = returns.rolling(20, min_periods=5).std()
    high_vol = rolling_vol.ge(rolling_vol.quantile(0.75)).fillna(False)
    low_vol = rolling_vol.lt(rolling_vol.median()).fillna(False)
    trend = (
        prices["QQQ"].gt(prices["QQQ"].rolling(60, min_periods=5).mean())
        & prices["QQQ"].pct_change(60).fillna(0.0).gt(0.0)
    ).fillna(False)
    recovery = drawdown.lt(0.0) & returns.rolling(20, min_periods=5).sum().gt(0.0)
    risk_on = trend & drawdown.gt(-0.10)
    qqq = base["QQQ"].copy()
    if candidate_id == "dynamic_regime_overlay_v0_4_reentry_repair_v1":
        mask = (recovery | trend) & drawdown.gt(-0.10)
        qqq.loc[mask] = base.loc[mask, "QQQ"] * 0.72 + ranking.loc[mask, "QQQ"] * 0.28
        qqq = qqq.rolling(3, min_periods=1).mean()
        lower, upper = 0.20, 0.82
    elif candidate_id == "dynamic_regime_overlay_v0_4_upside_capture_guarded_v1":
        mask = (risk_on | low_vol) & drawdown.gt(-0.08)
        qqq.loc[mask] = base.loc[mask, "QQQ"] * 0.65 + ranking.loc[mask, "QQQ"] * 0.35
        qqq.loc[drawdown.le(-0.08)] = base.loc[drawdown.le(-0.08), "QQQ"]
        lower, upper = 0.18, 0.85
    elif candidate_id == "dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1":
        qqq = (base["QQQ"] * 0.82 + ranking["QQQ"] * 0.18).rolling(
            6,
            min_periods=1,
        ).mean()
        decay_mask = high_vol & returns.lt(0.0)
        qqq.loc[decay_mask] = base.loc[decay_mask, "QQQ"] * 0.95
        lower, upper = 0.20, 0.80
    elif candidate_id == "dynamic_regime_overlay_v0_4_cooldown_balanced_v1":
        qqq = (base["QQQ"] * 0.76 + ranking["QQQ"] * 0.24).rolling(
            4,
            min_periods=1,
        ).mean()
        qqq.loc[recovery & drawdown.gt(-0.10)] = (
            base.loc[recovery & drawdown.gt(-0.10), "QQQ"] * 0.68
            + ranking.loc[recovery & drawdown.gt(-0.10), "QQQ"] * 0.32
        )
        lower, upper = 0.20, 0.82
    elif candidate_id == "dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1":
        qqq = base["QQQ"] * 0.55 + ranking["QQQ"] * 0.45
        qqq.loc[risk_on | low_vol] = (
            base.loc[risk_on | low_vol, "QQQ"] * 0.45
            + ranking.loc[risk_on | low_vol, "QQQ"] * 0.55
        )
        qqq.loc[drawdown.le(-0.08)] = (
            base.loc[drawdown.le(-0.08), "QQQ"] * 0.85
            + qqq.loc[drawdown.le(-0.08)] * 0.15
        )
        qqq = qqq.rolling(3, min_periods=1).mean()
        lower, upper = 0.18, 0.86
    else:
        raise ValueError(f"Unknown TRADING-2379 variant: {candidate_id}")
    return _normalized_qqq_sgov(qqq, index=prices.index, lower=lower, upper=upper)


def _variant_cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    variants: Sequence[str],
    static_bundle: Mapping[str, Any],
    ranking_top: str,
    base_candidate: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _cost_stress_scenarios():
        ranking_bundle = _variant_bundle(
            prices=prices,
            policies=policies,
            candidate_id=ranking_top,
            ranking_top=ranking_top,
            base_candidate=base_candidate,
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
                primary_candidate=base_candidate,
            )
        )
        for candidate_id in (*variants, ranking_top):
            bundle = ranking_bundle if candidate_id == ranking_top else _variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                ranking_top=ranking_top,
                base_candidate=base_candidate,
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
                    primary_candidate=base_candidate,
                )
            )
    return rows


def _variant_constraint_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    variants: Sequence[str],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
    ranking_top: str,
    base_candidate: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _execution_constraint_scenarios():
        for candidate_id in variants:
            bundle = _variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                ranking_top=ranking_top,
                base_candidate=base_candidate,
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
                primary_candidate=base_candidate,
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
    variants: Sequence[str],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
    ranking_top: str,
    base_candidate: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scenario = _cost_stress_scenarios()[1]
    for cadence in (PRIMARY_EXECUTION_CADENCE, "cooldown_limited_event_driven"):
        for candidate_id in variants:
            bundle = _variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                ranking_top=ranking_top,
                base_candidate=base_candidate,
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


def _optimized_variant_ranking(retest: Mapping[str, Any]) -> list[dict[str, Any]]:
    full_by_candidate = {
        str(row.get("candidate_id")): _as_mapping(row)
        for row in _as_list(retest.get("variant_retest_result"))
    }
    cost_by_candidate = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    base_row = full_by_candidate[BASE_CANDIDATE_ID]
    static_row = full_by_candidate["static_baseline"]
    base_time = _pass_rate_for_candidate(retest.get("time_slice_matrix"), BASE_CANDIDATE_ID)
    base_regime = _pass_rate_for_candidate(
        retest.get("regime_slice_matrix"),
        BASE_CANDIDATE_ID,
    )
    base_ranking_gap = _float(
        _as_mapping(base_row.get("relative_metrics")).get("dynamic_vs_ranking_top_gap")
    )
    base_turnover = _float(
        _as_mapping(base_row.get("execution_metrics")).get("turnover")
    )
    base_drawdown = _float(
        _as_mapping(base_row.get("performance_metrics")).get("max_drawdown")
    )
    static_drawdown = _float(
        _as_mapping(static_row.get("performance_metrics")).get("max_drawdown")
    )
    ranking_rows: list[dict[str, Any]] = []
    for variant_id in VARIANTS_TESTED:
        row = full_by_candidate[variant_id]
        realistic = cost_by_candidate.get((variant_id, "realistic"), {})
        conservative = cost_by_candidate.get((variant_id, "conservative"), {})
        harsh = cost_by_candidate.get((variant_id, "harsh"), {})
        perf = _as_mapping(row.get("performance_metrics"))
        rel = _as_mapping(row.get("relative_metrics"))
        execution = _as_mapping(row.get("execution_metrics"))
        dynamic_gap = _float(rel.get("dynamic_vs_static_gap"))
        ranking_gap = _float(rel.get("dynamic_vs_ranking_top_gap"))
        turnover = _float(execution.get("turnover"))
        max_drawdown = _float(perf.get("max_drawdown"))
        time_pass = _pass_rate_for_candidate(retest.get("time_slice_matrix"), variant_id)
        regime_pass = _pass_rate_for_candidate(
            retest.get("regime_slice_matrix"),
            variant_id,
        )
        return_gap_reduction = round(ranking_gap - base_ranking_gap, 6)
        turnover_profile_preserved = turnover <= base_turnover * (
            1.0 + TURNOVER_PROFILE_TOLERANCE
        )
        lower_turnover_guardrail_preserved = turnover_profile_preserved and abs(
            max_drawdown
        ) <= abs(base_drawdown) + DRAWDOWN_MATERIAL_WORSE_TOLERANCE
        decision, reasons = _variant_decision(
            variant_id=variant_id,
            dynamic_gap=dynamic_gap,
            realistic_gap=_scenario_gap(realistic),
            conservative_gap=_scenario_gap(conservative),
            harsh_gap=_scenario_gap(harsh),
            time_pass_rate=time_pass,
            regime_pass_rate=regime_pass,
            base_time_pass_rate=base_time,
            base_regime_pass_rate=base_regime,
            return_gap_reduction=return_gap_reduction,
            turnover_profile_preserved=turnover_profile_preserved,
            lower_turnover_guardrail_preserved=lower_turnover_guardrail_preserved,
            max_drawdown=max_drawdown,
            base_drawdown=base_drawdown,
            turnover=turnover,
            base_turnover=base_turnover,
        )
        ranking_rows.append(
            {
                "variant_id": variant_id,
                "candidate_id": variant_id,
                "total_return": perf.get("total_return"),
                "annualized_return": perf.get("annualized_return"),
                "cost_adjusted_return": perf.get("annualized_return"),
                "max_drawdown": perf.get("max_drawdown"),
                "volatility": perf.get("volatility"),
                "sharpe_or_sortino_if_available": perf.get(
                    "sharpe_or_sortino_if_available"
                ),
                "turnover": turnover,
                "rebalance_count": execution.get("rebalance_count"),
                "time_slice_pass_rate": time_pass,
                "regime_slice_pass_rate": regime_pass,
                "realistic_cost_passed": _scenario_gap(realistic) > 0.0,
                "conservative_cost_passed": _scenario_gap(conservative) > 0.0,
                "harsh_cost_passed": _scenario_gap(harsh) > 0.0,
                "dynamic_vs_static_gap": dynamic_gap,
                "variant_vs_base_lower_turnover_gap": round(
                    _float(perf.get("annualized_return"))
                    - _float(
                        _as_mapping(base_row.get("performance_metrics")).get(
                            "annualized_return"
                        )
                    ),
                    6,
                ),
                "variant_vs_ranking_top_gap": ranking_gap,
                "return_gap_reduction_vs_base": return_gap_reduction,
                "drawdown_gap_vs_static": round(
                    abs(max_drawdown) - abs(static_drawdown),
                    6,
                ),
                "turnover_profile_preserved": turnover_profile_preserved,
                "lower_turnover_guardrail_preserved": lower_turnover_guardrail_preserved,
                "owner_review_triggered": decision == DECISION_OWNER_REVIEW,
                "decision": decision,
                "decision_reason": reasons,
                "monthly_rebalance_allowed_for_primary_decision": False,
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
    realistic_gap: float,
    conservative_gap: float,
    harsh_gap: float,
    time_pass_rate: float,
    regime_pass_rate: float,
    base_time_pass_rate: float,
    base_regime_pass_rate: float,
    return_gap_reduction: float,
    turnover_profile_preserved: bool,
    lower_turnover_guardrail_preserved: bool,
    max_drawdown: float,
    base_drawdown: float,
    turnover: float,
    base_turnover: float,
) -> tuple[str, list[str]]:
    reasons = [
        f"dynamic_gap={dynamic_gap}",
        f"realistic_gap={realistic_gap}",
        f"conservative_gap={conservative_gap}",
        f"harsh_gap={harsh_gap}",
        f"time_slice_pass_rate={time_pass_rate}",
        f"regime_slice_pass_rate={regime_pass_rate}",
        f"return_gap_reduction_vs_base={return_gap_reduction}",
        f"turnover={turnover}",
        f"base_turnover={base_turnover}",
        f"max_drawdown={max_drawdown}",
        f"base_max_drawdown={base_drawdown}",
    ]
    improves_time = time_pass_rate > base_time_pass_rate + TIME_PASS_RATE_IMPROVEMENT_MIN
    preserves_regime = (
        regime_pass_rate + REGIME_PASS_RATE_PRESERVE_MIN >= base_regime_pass_rate
    )
    reduces_gap = return_gap_reduction > RETURN_GAP_REDUCTION_MIN
    drawdown_ok = (
        abs(max_drawdown) <= abs(base_drawdown) + DRAWDOWN_MATERIAL_WORSE_TOLERANCE
    )
    if dynamic_gap <= 0.0 or realistic_gap <= 0.0:
        return DECISION_REJECT, [*reasons, "blocked_by_non_positive_realistic_gap"]
    if (
        variant_id != BASE_CANDIDATE_ID
        and dynamic_gap <= 0.0
        and return_gap_reduction <= 0.0
    ):
        return DECISION_DEPRECATED, [*reasons, "no_advantage_vs_base_or_static"]
    if (
        conservative_gap > 0.0
        and improves_time
        and preserves_regime
        and reduces_gap
        and turnover_profile_preserved
        and lower_turnover_guardrail_preserved
        and drawdown_ok
    ):
        return DECISION_ACCEPT_RESEARCH_ONLY, [
            *reasons,
            "meets_research_only_observation_candidate_criteria",
        ]
    if (
        conservative_gap > 0.0
        and (improves_time or reduces_gap)
        and turnover_profile_preserved
        and drawdown_ok
    ):
        return DECISION_OWNER_REVIEW, [
            *reasons,
            "improved_but_owner_review_required",
        ]
    if harsh_gap > 0.0 or improves_time or reduces_gap:
        return DECISION_CONTINUE_OPTIMIZATION, [
            *reasons,
            "partial_improvement_but_acceptance_criteria_not_met",
        ]
    return DECISION_REJECT, [*reasons, "insufficient_slice_or_return_gap_improvement"]


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
        "schema_version": "dynamic_strategy_optimized_variant_decision_update.v1",
        "decision_update_ready": True,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "base_candidate": BASE_CANDIDATE_ID,
        "ranking_top_reference": RANKING_TOP_REFERENCE,
        "decision_from_2376": sources.get("decision_from_2376"),
        "decision_from_2378": SOURCE_2378_EXPECTED_ROUTE,
        "best_variant_after_retest": best_variant,
        "best_variant_decision": decision,
        "candidate_ready_for_research_only_observation": (
            decision == DECISION_ACCEPT_RESEARCH_ONLY
        ),
        "optimized_variant_ranking": list(ranking),
        "best_variant_metrics": best,
        "recommended_next_research_task": NEXT_ROUTE,
        "monthly_rebalance_allowed_for_primary_decision": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "decision_reasons": _as_list(best.get("decision_reason")),
        "required_outputs_ready": {
            "variants_tested": bool(retest.get("variant_retest_result")),
            "optimized_variant_ranking": bool(ranking),
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
        "best_variant_after_retest": decision_update.get("best_variant_after_retest"),
        "best_variant_decision": decision_update.get("best_variant_decision"),
        "best_variant_reduces_return_gap": _float(
            best.get("return_gap_reduction_vs_base")
        )
        > 0.0,
        "best_variant_preserves_lower_turnover_profile": bool(
            best.get("turnover_profile_preserved")
        ),
        "best_variant_improves_time_slice_robustness": _float(
            best.get("time_slice_pass_rate")
        )
        > _float(
            next(
                (
                    row.get("time_slice_pass_rate")
                    for row in ranking
                    if row.get("variant_id") == BASE_CANDIDATE_ID
                ),
                0.0,
            )
        ),
        "best_variant_improves_regime_slice_robustness": _float(
            best.get("regime_slice_pass_rate")
        )
        >= _float(
            next(
                (
                    row.get("regime_slice_pass_rate")
                    for row in ranking
                    if row.get("variant_id") == BASE_CANDIDATE_ID
                ),
                0.0,
            )
        ),
        "best_variant_survives_realistic_cost": bool(best.get("realistic_cost_passed")),
        "best_variant_survives_conservative_cost": bool(
            best.get("conservative_cost_passed")
        ),
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
        "source_ready_for_variant_retest": bool(
            sources.get("source_ready_for_variant_retest")
        ),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
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
        "base_candidate": BASE_CANDIDATE_ID,
        "ranking_top_reference": RANKING_TOP_REFERENCE,
        "comparison_candidates": list(COMPARISON_CANDIDATES),
        "variants_tested": list(VARIANTS_TESTED),
        "variant_retest_ready": False,
        "optimized_variant_ranking_ready": False,
        "time_slice_matrix_ready": False,
        "regime_slice_matrix_ready": False,
        "cost_stress_result_ready": False,
        "turnover_constraint_result_ready": False,
        "decision_update_ready": False,
        "variant_retest_result": [],
        "optimized_variant_ranking": [],
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
        "json_path": str(output_root / "variant_retest_result.json"),
        "optimized_variant_ranking_json": str(
            output_root / "optimized_variant_ranking.json"
        ),
        "time_regime_slice_matrix_json": str(
            output_root / "time_regime_slice_matrix.json"
        ),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_slice_robustness_optimized_variant_retest.md"
        ),
        "ranking_markdown": str(
            docs_root / "dynamic_strategy_optimized_variant_ranking.md"
        ),
        "slice_matrix_markdown": str(
            docs_root / "dynamic_strategy_optimized_variant_slice_matrix.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2380_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["optimized_variant_ranking_json"]),
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
        "schema_version": "dynamic_strategy_optimized_variant_ranking.v1",
        "report_type": "dynamic_strategy_optimized_variant_ranking",
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "ranking": _as_list(payload.get("optimized_variant_ranking")),
        "best_variant_after_retest": payload.get("best_variant_after_retest"),
        "best_variant_decision": payload.get("best_variant_decision"),
        "recommended_next_research_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _slice_matrix_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_optimized_variant_slice_matrix.v1",
        "report_type": "dynamic_strategy_optimized_variant_slice_matrix",
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
        "schema_version": "dynamic_strategy_optimized_variant_decision_update.v1",
        "report_type": "dynamic_strategy_optimized_variant_decision_update",
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
    ranking = _as_list(payload.get("optimized_variant_ranking"))
    best = _as_mapping(ranking[0]) if ranking else {}
    summary = _as_mapping(payload.get("summary_findings"))
    lines = [
        "# 动态策略 slice-robustness optimized variant retest",
        "",
        "## Executive summary",
        "",
        f"- status：`{payload.get('status')}`",
        f"- data quality：`{payload.get('data_quality_status')}`",
        f"- base candidate：`{payload.get('base_candidate')}`",
        f"- ranking top reference：`{payload.get('ranking_top_reference')}`",
        f"- best variant：`{payload.get('best_variant_after_retest')}`",
        f"- best decision：`{payload.get('best_variant_decision')}`",
        f"- next route：`{NEXT_ROUTE}`",
        "",
        "## Source optimization plan from TRADING-2378",
        "",
        "- 2378 已要求修复 time/regime slice robustness 与 return gap。",
        "- 本次 retest 只进入 research-only 结论，不批准 paper-shadow / production / broker。",
        "",
        "## Variant definitions",
        "",
        "|variant|role|",
        "|---|---|",
    ]
    for variant in VARIANTS_TESTED:
        role = "base reference" if variant == BASE_CANDIDATE_ID else "optimized variant"
        lines.append(f"|`{variant}`|{role}|")
    lines.extend(
        [
            "",
            "## Retest design",
            "",
            f"- primary execution cadence：`{PRIMARY_EXECUTION_CADENCE}`",
            "- comparison cadences：`valid_until_window`, `cooldown_limited_event_driven`",
            "- monthly rebalance：reference only；not primary decision",
            "- cost stress：base / realistic / conservative / harsh",
            "- time/regime slice：沿用 2376 targeted retest policy，并覆盖 2379 required subset",
            "",
            "## Optimized variant ranking",
            "",
            "|rank|variant|decision|return_gap_reduction|time_pass|regime_pass|turnover|",
            "|---:|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in ranking:
        item = _as_mapping(row)
        lines.append(
            "|"
            f"{item.get('rank')}|`{item.get('variant_id')}`|`{item.get('decision')}`|"
            f"{item.get('return_gap_reduction_vs_base')}|"
            f"{item.get('time_slice_pass_rate')}|"
            f"{item.get('regime_slice_pass_rate')}|"
            f"{item.get('turnover')}|"
        )
    lines.extend(
        [
            "",
            "## Key answers",
            "",
            f"- 哪个 variant 最好：`{best.get('variant_id')}`",
            "- 是否优于 base lower-turnover："
            f"`{_float(best.get('variant_vs_base_lower_turnover_gap')) > 0.0}`",
            "- 是否缩小 vs ranking top return gap："
            f"`{_float(best.get('return_gap_reduction_vs_base')) > 0.0}`",
            "- 是否保持 lower-turnover profile："
            f"`{best.get('turnover_profile_preserved')}`",
            "- 是否改善 time/regime robustness："
            f"`{summary.get('best_variant_improves_time_slice_robustness')}` / "
            f"`{summary.get('best_variant_improves_regime_slice_robustness')}`",
            "- 是否穿越 realistic / conservative cost："
            f"`{best.get('realistic_cost_passed')}` / `{best.get('conservative_cost_passed')}`",
            "- 是否可以升级到 research-only observation："
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
        "# 动态策略 optimized variant ranking",
        "",
        f"- status：`{payload.get('status')}`",
        f"- best variant：`{payload.get('best_variant_after_retest')}`",
        f"- best decision：`{payload.get('best_variant_decision')}`",
        "",
        "|rank|variant|cost_return|drawdown|turnover|return_gap_vs_ranking|decision|",
        "|---:|---|---:|---:|---:|---:|---|",
    ]
    for row in _as_list(payload.get("optimized_variant_ranking")):
        item = _as_mapping(row)
        lines.append(
            "|"
            f"{item.get('rank')}|`{item.get('variant_id')}`|"
            f"{item.get('cost_adjusted_return')}|"
            f"{item.get('max_drawdown')}|"
            f"{item.get('turnover')}|"
            f"{item.get('variant_vs_ranking_top_gap')}|"
            f"`{item.get('decision')}`|"
        )
    return "\n".join(lines) + "\n"


def _slice_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# 动态策略 optimized variant slice matrix",
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
            "# TRADING-2380 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best variant：`{payload.get('best_variant_after_retest')}`",
            f"- best decision：`{payload.get('best_variant_decision')}`",
            f"- next route：`{NEXT_ROUTE}`",
            "- route boundary：optimized variant owner review and observation decision；"
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


def _decision_from_2376(
    *,
    targeted_retest: Mapping[str, Any],
    targeted_decision_update: Mapping[str, Any],
) -> str:
    nested = _as_mapping(targeted_decision_update).get("decision_update")
    if isinstance(nested, Mapping):
        decision = nested.get("candidate_decision_after_targeted_retest")
        if decision:
            return str(decision)
    decision = _as_mapping(targeted_retest).get("candidate_decision_after_targeted_retest")
    if decision:
        return str(decision)
    decision_update = _as_mapping(targeted_retest).get("decision_update")
    if isinstance(decision_update, Mapping):
        return str(decision_update.get("candidate_decision_after_targeted_retest", ""))
    return ""


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


def _ranking_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    decision_priority = {
        DECISION_ACCEPT_RESEARCH_ONLY: 0,
        DECISION_OWNER_REVIEW: 1,
        DECISION_CONTINUE_OPTIMIZATION: 2,
        DECISION_REJECT: 3,
        DECISION_DEPRECATED: 4,
    }
    return (
        decision_priority.get(str(row.get("decision")), 9),
        -_float(row.get("return_gap_reduction_vs_base")),
        -_float(row.get("time_slice_pass_rate")),
        -_float(row.get("regime_slice_pass_rate")),
        -_float(row.get("cost_adjusted_return")),
        _float(row.get("turnover")),
    )


def _candidate_role(candidate_id: str) -> str:
    if candidate_id == "static_baseline":
        return "static_baseline"
    if candidate_id == BASE_CANDIDATE_ID:
        return "base_lower_turnover_reference"
    if candidate_id == RANKING_TOP_REFERENCE:
        return "ranking_top_from_2365"
    return "optimized_variant"


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
