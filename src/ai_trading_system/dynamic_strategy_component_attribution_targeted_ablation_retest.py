from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest as retest_helpers
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    OWNER_DECISION as SOURCE_2391_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision import (
    READY_STATUS as SOURCE_2391_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification import (
    READY_STATUS as SOURCE_2390_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan import (
    COMPONENTS_TO_ATTRIBUTE,
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan import (
    NEXT_ROUTE as SOURCE_2392_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan import (
    READY_STATUS as SOURCE_2392_READY_STATUS,
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
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    BASE_CANDIDATE_ID,
    BEST_GUARDED_VARIANT,
    BEST_LOWER_TURNOVER_VARIANT,
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    RANKING_TOP_CANDIDATE,
    _expanded_target_weights,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    READY_STATUS as SOURCE_2386_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_report_common import (
    json_block as _json_block,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_path as _load_json_document,
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

TASK_ID = "TRADING-2393"
TASK_REGISTER_ID = (
    "TRADING-2393_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST"
)
REPORT_TYPE = "dynamic_strategy_component_attribution_targeted_ablation_retest"
SCHEMA_VERSION = "dynamic_strategy_component_attribution_targeted_ablation_retest.v1"
READY_STATUS = "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_"
    "BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_"
    "Recombination_Decision"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2386",
    "TRADING-2390",
    "TRADING-2391",
    "TRADING-2392",
)
COMPONENTS_TESTED: tuple[str, ...] = tuple(COMPONENTS_TO_ATTRIBUTE)
ABLATION_CANDIDATES: tuple[str, ...] = (
    "growth_tilt_only_reference",
    "growth_tilt_plus_turnover_budget",
    "growth_tilt_plus_valid_until_strict",
    "growth_tilt_plus_turnover_budget_and_valid_until",
    "lower_turnover_without_cooldown",
    "lower_turnover_plus_growth_tilt_component",
)
COMPARISON_CADENCES: tuple[str, ...] = (
    PRIMARY_EXECUTION_CADENCE,
    "cooldown_limited_event_driven",
    "signal_event_driven",
)
COMPONENT_DECISION_REUSABLE = "REUSABLE_COMPONENT"
COMPONENT_DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
COMPONENT_DECISION_GUARDRAIL = "USE_ONLY_AS_GUARDRAIL"
COMPONENT_DECISION_CONTINUE = "CONTINUE_COMPONENT_RESEARCH"
COMPONENT_DECISION_REJECT = "REJECT_COMPONENT_FOR_NOW"

# TRADING-2393 research-only component decision thresholds. These are pilot
# attribution rules, not promotion or observation gates.
MATERIAL_RETURN_IMPROVEMENT_MIN = 0.0005
MATERIAL_TURNOVER_REDUCTION_RATIO_MIN = 0.05
MATERIAL_STALE_REDUCTION_MIN = 1.0
MATERIAL_DRAWDOWN_IMPROVEMENT_MIN = 0.001
RETURN_RETENTION_MIN = 0.50
SLICE_PASS_RATE_MIN = 0.30
TURNOVER_BUDGET_MAX_MONTHLY = 1.0

DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_ranking.json"
)
DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_OUTPUT_ROOT
    / "reclassification_result.json"
)
DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "component_attribution_plan.json"
)


def run_dynamic_strategy_component_attribution_targeted_ablation_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_candidate_ranking_2365_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_2366_path: Path = (
        DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH
    ),
    source_expanded_candidate_retest_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    source_reclassification_result_2390_path: Path = (
        DEFAULT_SOURCE_2390_RECLASSIFICATION_RESULT_PATH
    ),
    source_owner_review_decision_2391_path: Path = (
        DEFAULT_SOURCE_2391_OWNER_REVIEW_DECISION_PATH
    ),
    source_component_attribution_plan_2392_path: Path = (
        DEFAULT_SOURCE_2392_COMPONENT_ATTRIBUTION_PLAN_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_candidate_ranking_2365_path=source_candidate_ranking_2365_path,
        source_sensitivity_result_2366_path=source_sensitivity_result_2366_path,
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_expanded_candidate_ranking_2386_path=(
            source_expanded_candidate_ranking_2386_path
        ),
        source_reclassification_result_2390_path=(
            source_reclassification_result_2390_path
        ),
        source_owner_review_decision_2391_path=(
            source_owner_review_decision_2391_path
        ),
        source_component_attribution_plan_2392_path=(
            source_component_attribution_plan_2392_path
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
    if not bool(sources["source_ready_for_ablation_retest"]):
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
    retest = _run_targeted_ablation_retest(prices=prices, policies=policies)
    component_matrix = _component_attribution_matrix(retest)
    reusable_decision = _reusable_component_decision(component_matrix)
    decision_update = _decision_update(
        component_matrix=component_matrix,
        reusable_decision=reusable_decision,
    )
    payload.update(
        {
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "comparison_cadences": list(COMPARISON_CADENCES),
            "monthly_rebalance": {
                "allowed_for_reference": True,
                "allowed_for_primary_decision": False,
            },
            "components_tested": list(COMPONENTS_TESTED),
            "ablation_candidates_tested": list(ABLATION_CANDIDATES),
            "ablation_retest_design": _ablation_retest_design(),
            "ablation_retest_result": retest["ablation_retest_result"],
            "reference_candidate_result": retest["reference_candidate_result"],
            "time_slice_matrix": retest["time_slice_matrix"],
            "regime_slice_matrix": retest["regime_slice_matrix"],
            "time_regime_slice_matrix": (
                retest["time_slice_matrix"] + retest["regime_slice_matrix"]
            ),
            "cost_stress_result": retest["cost_stress_result"],
            "cadence_comparison_result": retest["cadence_comparison_result"],
            "component_attribution_matrix": component_matrix,
            "reusable_component_decision": reusable_decision,
            "decision_update": decision_update,
            "best_reusable_component": reusable_decision["best_reusable_component"],
            "component_decisions": reusable_decision["component_decisions"],
            "component_decisions_ready": True,
            "ablation_retest_ready": True,
            "component_attribution_matrix_ready": True,
            "reusable_component_decision_ready": True,
            "decision_update_ready": True,
            "recommended_next_research_task": NEXT_ROUTE,
            "research_quality_status": (
                "COMPONENT_ABLATION_READY_REQUIRES_2394_OWNER_REVIEW"
            ),
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(**paths: Path) -> dict[str, Any]:
    source_files = {
        key.removeprefix("source_").removesuffix("_path"): path
        for key, path in paths.items()
    }
    documents = {key: _load_json_document(path) for key, path in source_files.items()}
    source_status = {
        key: _as_mapping(document).get("status") for key, document in documents.items()
    }
    errors = _source_validation_errors(documents, source_status)
    return {
        **documents,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_artifacts": {
            key: retest_helpers._source_artifact(path, documents[key])
            for key, path in source_files.items()
        },
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": source_status,
        "source_validation_errors": errors,
        "source_ready_for_ablation_retest": not errors,
    }


def _source_validation_errors(
    documents: Mapping[str, Any],
    source_status: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "candidate_ranking_2365": SOURCE_2365_READY_STATUS,
        "sensitivity_result_2366": SOURCE_2366_READY_STATUS,
        "expanded_candidate_retest_2386": SOURCE_2386_READY_STATUS,
        "expanded_candidate_ranking_2386": SOURCE_2386_READY_STATUS,
        "reclassification_result_2390": SOURCE_2390_READY_STATUS,
        "owner_review_decision_2391": SOURCE_2391_READY_STATUS,
        "component_attribution_plan_2392": SOURCE_2392_READY_STATUS,
    }
    for source_name, expected in expected_status.items():
        if source_status.get(source_name) != expected:
            errors.append(f"{source_name}_status_not_ready")

    candidate_ranking = _as_mapping(documents.get("candidate_ranking_2365"))
    expanded_retest = _as_mapping(documents.get("expanded_candidate_retest_2386"))
    expanded_ranking = _as_mapping(documents.get("expanded_candidate_ranking_2386"))
    reclassification = _as_mapping(documents.get("reclassification_result_2390"))
    owner_decision = _as_mapping(documents.get("owner_review_decision_2391"))
    component_plan = _as_mapping(documents.get("component_attribution_plan_2392"))

    if _top_candidate(candidate_ranking.get("candidate_ranking")) != RANKING_TOP_CANDIDATE:
        errors.append("2365_top_candidate_not_ranking_top")
    if expanded_retest.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("2386_primary_cadence_not_valid_until_window")
    if expanded_retest.get("data_quality_gate_executed") is not True:
        errors.append("2386_missing_data_quality_gate")
    if expanded_retest.get("best_candidate_after_expanded_screening") != (
        RANKING_TOP_CANDIDATE
    ):
        errors.append("2386_best_candidate_not_ranking_top")
    if reclassification.get("current_best_candidate") != RANKING_TOP_CANDIDATE:
        errors.append("2390_current_best_candidate_mismatch")
    if owner_decision.get("owner_decision") != SOURCE_2391_OWNER_DECISION:
        errors.append("2391_owner_decision_mismatch")
    if owner_decision.get("research_only_observation_approved") is True:
        errors.append("2391_research_only_observation_unexpectedly_approved")
    if component_plan.get("recommended_next_research_task") != SOURCE_2392_EXPECTED_ROUTE:
        errors.append("2392_route_not_trading_2393")
    if component_plan.get("targeted_ablation_retest_plan_ready") is not True:
        errors.append("2392_targeted_ablation_plan_not_ready")
    if set(COMPONENTS_TESTED) - set(component_plan.get("components_to_attribute", [])):
        errors.append("2392_missing_components_to_attribute")
    planned_candidates = {
        str(item.get("candidate_id"))
        for item in _as_list_of_mappings(
            _as_mapping(component_plan.get("targeted_ablation_retest_plan")).get(
                "ablation_test_candidates"
            )
        )
    }
    if set(ABLATION_CANDIDATES) - planned_candidates:
        errors.append("2392_missing_required_ablation_candidates")
    ranking_ids = {
        str(row.get("candidate_id"))
        for row in _as_list_of_mappings(
            expanded_ranking.get("expanded_candidate_ranking")
        )
    }
    for required in (
        RANKING_TOP_CANDIDATE,
        BASE_CANDIDATE_ID,
        BEST_LOWER_TURNOVER_VARIANT,
        BEST_GUARDED_VARIANT,
        "dynamic_turnover_budgeted_growth_tilt_v1",
        "dynamic_valid_until_expiry_strict_v1",
    ):
        if required not in ranking_ids:
            errors.append(f"2386_ranking_missing_{required}")

    for label, source in (
        ("2390", reclassification),
        ("2391", owner_decision),
        ("2392", component_plan),
    ):
        errors.extend(_side_effect_validation_errors(label, source))
    return errors


def _run_targeted_ablation_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    static_bundle = retest_helpers._static_bundle(prices)
    base_scenario = _cost_stress_scenarios()[1]
    reference_bundles = {
        "ranking_top_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=RANKING_TOP_CANDIDATE,
            scenario=base_scenario,
        ),
        "lower_turnover_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=BASE_CANDIDATE_ID,
            scenario=base_scenario,
        ),
        "cooldown_balanced_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=BEST_LOWER_TURNOVER_VARIANT,
            scenario=base_scenario,
        ),
        "guarded_turnover_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=BEST_GUARDED_VARIANT,
            scenario=base_scenario,
        ),
    }
    ranking_bundle = reference_bundles["ranking_top_reference"]
    ablation_bundles = {
        candidate_id: _ablation_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=base_scenario,
        )
        for candidate_id in ABLATION_CANDIDATES
    }
    full_rows = [
        retest_helpers._full_sample_result_row(
            candidate_id="static_baseline",
            role="static_baseline",
            bundle=static_bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        )
    ]
    for role, bundle in reference_bundles.items():
        full_rows.append(
            retest_helpers._full_sample_result_row(
                candidate_id=str(bundle["candidate_id"]),
                role=role,
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
            )
        )
    for candidate_id, bundle in ablation_bundles.items():
        row = retest_helpers._full_sample_result_row(
            candidate_id=candidate_id,
            role=_ablation_role(candidate_id),
            bundle=bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        )
        row["ablation_components"] = _ablation_components(candidate_id)
        row["ablation_purpose"] = _ablation_purpose(candidate_id)
        full_rows.append(row)

    time_rows = retest_helpers._slice_rows(
        prices=prices,
        slice_group="time_slice",
        slice_definitions=retest_helpers._time_slice_definitions(
            prices,
            _component_retest_policy(),
        ),
        bundles=ablation_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=RANKING_TOP_CANDIDATE,
    )
    regime_rows = retest_helpers._slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=retest_helpers._regime_slice_definitions(
            prices,
            _component_retest_policy(),
        ),
        bundles=ablation_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=RANKING_TOP_CANDIDATE,
    )
    return {
        "ablation_retest_result": full_rows,
        "reference_candidate_result": [
            row for row in full_rows if str(row.get("role")).endswith("_reference")
        ],
        "time_slice_matrix": time_rows,
        "regime_slice_matrix": regime_rows,
        "cost_stress_result": _cost_stress_rows(
            prices=prices,
            policies=policies,
            static_bundle=static_bundle,
        ),
        "cadence_comparison_result": _cadence_comparison_rows(
            prices=prices,
            policies=policies,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        ),
    }


def _reference_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    scenario: Mapping[str, Any],
    cadence: str = PRIMARY_EXECUTION_CADENCE,
) -> dict[str, Any]:
    return _bundle_from_target_weights(
        prices=prices,
        policies=policies,
        candidate_id=candidate_id,
        target_weights=_expanded_target_weights(candidate_id=candidate_id, prices=prices),
        cadence=cadence,
        scenario=scenario,
    )


def _ablation_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    cadence: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    return _bundle_from_target_weights(
        prices=prices,
        policies=policies,
        candidate_id=candidate_id,
        target_weights=_ablation_target_weights(candidate_id=candidate_id, prices=prices),
        cadence=cadence,
        scenario=scenario,
    )


def _bundle_from_target_weights(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    target_weights: pd.DataFrame,
    cadence: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
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
        signal_validity_profile=retest_helpers._signal_validity_profile_for_retest(
            cadence,
            policy,
        ),
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
    returns = retest_helpers._portfolio_return_series(prices, actual_weights, cost_bps=cost_bps)
    gross_returns = retest_helpers._portfolio_return_series(
        prices,
        actual_weights,
        cost_bps=0.0,
    )
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
        "benchmark_returns": retest_helpers._benchmark_returns(prices),
        "cost_bps": cost_bps,
    }


def _ablation_target_weights(*, candidate_id: str, prices: pd.DataFrame) -> pd.DataFrame:
    ranking = _expanded_target_weights(candidate_id=RANKING_TOP_CANDIDATE, prices=prices)
    turnover_budgeted = _expanded_target_weights(
        candidate_id="dynamic_turnover_budgeted_growth_tilt_v1",
        prices=prices,
    )
    valid_until_strict = _expanded_target_weights(
        candidate_id="dynamic_valid_until_expiry_strict_v1",
        prices=prices,
    )
    lower = _expanded_target_weights(candidate_id=BASE_CANDIDATE_ID, prices=prices)
    cooldown = _expanded_target_weights(candidate_id=BEST_LOWER_TURNOVER_VARIANT, prices=prices)
    guarded = _expanded_target_weights(candidate_id=BEST_GUARDED_VARIANT, prices=prices)

    if candidate_id == "growth_tilt_only_reference":
        qqq = ranking["QQQ"]
    elif candidate_id == "growth_tilt_plus_turnover_budget":
        qqq = turnover_budgeted["QQQ"]
    elif candidate_id == "growth_tilt_plus_valid_until_strict":
        qqq = valid_until_strict["QQQ"]
    elif candidate_id == "growth_tilt_plus_turnover_budget_and_valid_until":
        qqq = turnover_budgeted["QQQ"] * 0.55 + valid_until_strict["QQQ"] * 0.45
        qqq = _bounded_step_delta(qqq.rolling(4, min_periods=1).mean(), max_delta=0.10)
    elif candidate_id == "lower_turnover_without_cooldown":
        qqq = lower["QQQ"]
    elif candidate_id == "lower_turnover_plus_growth_tilt_component":
        qqq = cooldown["QQQ"] * 0.58 + guarded["QQQ"] * 0.22 + ranking["QQQ"] * 0.20
        qqq = _bounded_step_delta(qqq.rolling(5, min_periods=1).mean(), max_delta=0.12)
    else:
        raise ValueError(f"Unknown TRADING-2393 ablation candidate: {candidate_id}")
    return _normalized_qqq_sgov(qqq, index=prices.index)


def _cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _cost_stress_scenarios():
        ranking_bundle = _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=RANKING_TOP_CANDIDATE,
            scenario=scenario,
        )
        rows.append(
            retest_helpers._stress_result_row(
                candidate_id="static_baseline",
                role="static_baseline",
                scenario=scenario,
                bundle=static_bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=RANKING_TOP_CANDIDATE,
            )
        )
        for candidate_id in ABLATION_CANDIDATES:
            bundle = _ablation_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
            )
            row = retest_helpers._stress_result_row(
                candidate_id=candidate_id,
                role=_ablation_role(candidate_id),
                scenario=scenario,
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=RANKING_TOP_CANDIDATE,
            )
            row["ablation_components"] = _ablation_components(candidate_id)
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
        for candidate_id in ABLATION_CANDIDATES:
            bundle = _ablation_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=cadence,
                scenario=scenario,
            )
            row = retest_helpers._stress_result_row(
                candidate_id=candidate_id,
                role=f"{_ablation_role(candidate_id)}_{cadence}",
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
            row["comparison_cadence"] = cadence
            row["monthly_rebalance_allowed_for_primary_decision"] = False
            rows.append(row)
    return rows


def _component_attribution_matrix(retest: Mapping[str, Any]) -> list[dict[str, Any]]:
    full = _rows_by_candidate(retest.get("ablation_retest_result"))
    cost = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    time_rows = _as_list_of_mappings(retest.get("time_slice_matrix"))
    regime_rows = _as_list_of_mappings(retest.get("regime_slice_matrix"))

    growth = full["growth_tilt_only_reference"]
    turnover = full["growth_tilt_plus_turnover_budget"]
    strict = full["growth_tilt_plus_valid_until_strict"]
    combo = full["growth_tilt_plus_turnover_budget_and_valid_until"]
    lower_no_cooldown = full["lower_turnover_without_cooldown"]
    lower_plus_growth = full["lower_turnover_plus_growth_tilt_component"]
    static = full["static_baseline"]
    ranking = full[RANKING_TOP_CANDIDATE]
    cooldown = full[BEST_LOWER_TURNOVER_VARIANT]
    guarded = full[BEST_GUARDED_VARIANT]

    component_rows = [
        _component_row(
            component_name="turnover_budgeting",
            test_candidate="growth_tilt_plus_turnover_budget",
            source_candidate="growth_tilt_only_reference",
            candidate_row=turnover,
            reference_row=growth,
            static_row=static,
            ranking_row=ranking,
            realistic_row=cost.get(("growth_tilt_plus_turnover_budget", "realistic"), {}),
            conservative_row=cost.get(("growth_tilt_plus_turnover_budget", "conservative"), {}),
            harsh_row=cost.get(("growth_tilt_plus_turnover_budget", "harsh"), {}),
            time_rows=time_rows,
            regime_rows=regime_rows,
            target_metric_improvements={
                "turnover_reduction_vs_base": _turnover_reduction(growth, turnover),
                "transaction_cost_drag_reduction": _cost_drag_delta(growth, turnover),
                "return_retention_after_turnover_budget": _return_retention(turnover, growth),
                "turnover_adjusted_score": _turnover_adjusted_score(turnover),
            },
        ),
        _component_row(
            component_name="valid_until_strictness",
            test_candidate="growth_tilt_plus_valid_until_strict",
            source_candidate="growth_tilt_only_reference",
            candidate_row=strict,
            reference_row=growth,
            static_row=static,
            ranking_row=ranking,
            realistic_row=cost.get(("growth_tilt_plus_valid_until_strict", "realistic"), {}),
            conservative_row=cost.get(("growth_tilt_plus_valid_until_strict", "conservative"), {}),
            harsh_row=cost.get(("growth_tilt_plus_valid_until_strict", "harsh"), {}),
            time_rows=time_rows,
            regime_rows=regime_rows,
            target_metric_improvements={
                "stale_signal_execution_reduction": _stale_reduction(growth, strict),
                "signal_to_execution_lag_reduction": _lag_reduction(growth, strict),
                "near_expiry_execution_reduction": _stale_reduction(growth, strict),
                "upside_capture_loss": _upside_capture_loss(growth, strict),
            },
        ),
        _component_row(
            component_name="growth_tilt_engine",
            test_candidate="growth_tilt_only_reference",
            source_candidate="static_baseline",
            candidate_row=growth,
            reference_row=static,
            static_row=static,
            ranking_row=ranking,
            realistic_row=cost.get(("growth_tilt_only_reference", "realistic"), {}),
            conservative_row=cost.get(("growth_tilt_only_reference", "conservative"), {}),
            harsh_row=cost.get(("growth_tilt_only_reference", "harsh"), {}),
            time_rows=time_rows,
            regime_rows=regime_rows,
            target_metric_improvements={
                "return_contribution": _annual_gap(growth, static),
                "upside_capture": _performance(growth, "upside_capture"),
                "drawdown_penalty": _drawdown_penalty(growth, static),
                "return_per_drawdown_penalty": _return_per_drawdown_penalty(growth, static),
            },
        ),
        _component_row(
            component_name="lower_turnover_guardrail",
            test_candidate="lower_turnover_without_cooldown",
            source_candidate=BEST_LOWER_TURNOVER_VARIANT,
            candidate_row=lower_no_cooldown,
            reference_row=cooldown,
            static_row=static,
            ranking_row=ranking,
            realistic_row=cost.get(("lower_turnover_without_cooldown", "realistic"), {}),
            conservative_row=cost.get(("lower_turnover_without_cooldown", "conservative"), {}),
            harsh_row=cost.get(("lower_turnover_without_cooldown", "harsh"), {}),
            time_rows=time_rows,
            regime_rows=regime_rows,
            target_metric_improvements={
                "cost_stress_survival": _cost_stress_survival(
                    cost,
                    "lower_turnover_without_cooldown",
                ),
                "turnover_profile_preservation": _turnover_reduction(ranking, lower_no_cooldown),
                "drawdown_behavior": _drawdown_delta(ranking, lower_no_cooldown),
                "return_gap_vs_ranking_top": _annual_gap(lower_no_cooldown, ranking),
            },
        ),
        _component_row(
            component_name="guarded_turnover_transfer",
            test_candidate="lower_turnover_plus_growth_tilt_component",
            source_candidate=BEST_GUARDED_VARIANT,
            candidate_row=lower_plus_growth,
            reference_row=guarded,
            static_row=static,
            ranking_row=ranking,
            realistic_row=cost.get(("lower_turnover_plus_growth_tilt_component", "realistic"), {}),
            conservative_row=cost.get(
                ("lower_turnover_plus_growth_tilt_component", "conservative"),
                {},
            ),
            harsh_row=cost.get(("lower_turnover_plus_growth_tilt_component", "harsh"), {}),
            time_rows=time_rows,
            regime_rows=regime_rows,
            target_metric_improvements={
                "return_retention_vs_original_ranking_top": _return_retention(
                    lower_plus_growth,
                    ranking,
                ),
                "turnover_reduction_vs_original_ranking_top": _turnover_reduction(
                    ranking,
                    lower_plus_growth,
                ),
                "drawdown_improvement_vs_original_ranking_top": _drawdown_delta(
                    ranking,
                    lower_plus_growth,
                ),
                "cost_adjusted_gap_vs_static": _relative_gap(lower_plus_growth),
            },
        ),
        _component_row(
            component_name="combined_turnover_budgeting_and_valid_until",
            test_candidate="growth_tilt_plus_turnover_budget_and_valid_until",
            source_candidate="growth_tilt_only_reference",
            candidate_row=combo,
            reference_row=growth,
            static_row=static,
            ranking_row=ranking,
            realistic_row=cost.get(
                ("growth_tilt_plus_turnover_budget_and_valid_until", "realistic"),
                {},
            ),
            conservative_row=cost.get(
                ("growth_tilt_plus_turnover_budget_and_valid_until", "conservative"),
                {},
            ),
            harsh_row=cost.get(
                ("growth_tilt_plus_turnover_budget_and_valid_until", "harsh"),
                {},
            ),
            time_rows=time_rows,
            regime_rows=regime_rows,
            target_metric_improvements={
                "combined_return_retention": _return_retention(combo, growth),
                "combined_turnover_reduction": _turnover_reduction(growth, combo),
                "combined_stale_signal_reduction": _stale_reduction(growth, combo),
                "combined_cost_adjusted_gap_vs_static": _relative_gap(combo),
            },
        ),
    ]
    return [_attach_component_decision(row) for row in component_rows]


def _component_row(
    *,
    component_name: str,
    test_candidate: str,
    source_candidate: str,
    candidate_row: Mapping[str, Any],
    reference_row: Mapping[str, Any],
    static_row: Mapping[str, Any],
    ranking_row: Mapping[str, Any],
    realistic_row: Mapping[str, Any],
    conservative_row: Mapping[str, Any],
    harsh_row: Mapping[str, Any],
    time_rows: Sequence[Mapping[str, Any]],
    regime_rows: Sequence[Mapping[str, Any]],
    target_metric_improvements: Mapping[str, Any],
) -> dict[str, Any]:
    time_pass = _pass_rate(time_rows, test_candidate)
    regime_pass = _pass_rate(regime_rows, test_candidate)
    execution = _as_mapping(candidate_row.get("execution_metrics"))
    reference_execution = _as_mapping(reference_row.get("execution_metrics"))
    annual_gap_reference = _annual_gap(candidate_row, reference_row)
    annual_gap_static = _annual_gap(candidate_row, static_row)
    annual_gap_ranking = _annual_gap(candidate_row, ranking_row)
    turnover_reduction = _turnover_reduction(reference_row, candidate_row)
    stale_reduction = _stale_reduction(reference_row, candidate_row)
    drawdown_delta = _drawdown_delta(reference_row, candidate_row)
    contribution_score = _component_contribution_score(
        annual_gap_reference=annual_gap_reference,
        annual_gap_static=annual_gap_static,
        turnover_reduction=turnover_reduction,
        stale_reduction=stale_reduction,
        drawdown_delta=drawdown_delta,
        time_pass=time_pass,
        regime_pass=regime_pass,
    )
    return {
        "component_name": component_name,
        "test_candidate": test_candidate,
        "source_candidate": source_candidate,
        "candidate_level_approval": False,
        "component_contribution_score": contribution_score,
        "target_metric_improvements": dict(target_metric_improvements),
        "performance_metrics": dict(_as_mapping(candidate_row.get("performance_metrics"))),
        "relative_metrics": {
            "dynamic_vs_static_gap": _relative_gap(candidate_row),
            "candidate_vs_growth_tilt_reference_gap": annual_gap_reference,
            "candidate_vs_lower_turnover_reference_gap": annual_gap_static,
            "candidate_vs_original_ranking_top_gap": annual_gap_ranking,
            "cost_adjusted_dynamic_vs_static_gap": _relative_gap(candidate_row),
            "return_retention_vs_source_candidate": _return_retention(
                candidate_row,
                reference_row,
            ),
            "drawdown_gap_vs_static": _drawdown_delta(static_row, candidate_row),
        },
        "execution_metrics": {
            "turnover": execution.get("turnover"),
            "rebalance_count": execution.get("rebalance_count"),
            "average_holding_days": execution.get("average_holding_days"),
            "max_monthly_turnover": execution.get("max_monthly_turnover"),
            "signal_to_execution_lag_days": execution.get("signal_to_execution_lag_days"),
            "stale_signal_execution_count": execution.get("stale_signal_execution_count"),
            "cooldown_block_count": execution.get("cooldown_block_count"),
            "constraint_hit_count": execution.get("constraint_hit_count"),
            "turnover_reduction_vs_source_candidate": turnover_reduction,
            "stale_signal_execution_reduction_vs_source_candidate": stale_reduction,
            "source_turnover": reference_execution.get("turnover"),
            "source_stale_signal_execution_count": reference_execution.get(
                "stale_signal_execution_count"
            ),
        },
        "cost_stress_metrics": {
            "realistic_gap": _relative_gap(realistic_row),
            "conservative_gap": _relative_gap(conservative_row),
            "harsh_gap": _relative_gap(harsh_row),
            "survival": _cost_stress_survival_for_rows(
                realistic_row,
                conservative_row,
                harsh_row,
            ),
        },
        "slice_metrics": {
            "time_slice_pass_rate": time_pass,
            "regime_slice_pass_rate": regime_pass,
        },
        "component_owner_review_required": True,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _attach_component_decision(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    improvements = _as_mapping(row.get("target_metric_improvements"))
    execution = _as_mapping(row.get("execution_metrics"))
    cost = _as_mapping(row.get("cost_stress_metrics"))
    slices = _as_mapping(row.get("slice_metrics"))
    component = str(row.get("component_name"))
    turnover_reduced = (
        _float(execution.get("turnover_reduction_vs_source_candidate"))
        >= MATERIAL_TURNOVER_REDUCTION_RATIO_MIN
    )
    stale_reduced = (
        _float(execution.get("stale_signal_execution_reduction_vs_source_candidate"))
        >= MATERIAL_STALE_REDUCTION_MIN
    )
    positive_static_gap = _float(_as_mapping(row.get("relative_metrics")).get(
        "dynamic_vs_static_gap"
    )) > 0.0
    cost_survives = str(cost.get("survival")) in {"realistic", "conservative", "harsh"}
    slices_ok = (
        _float(slices.get("time_slice_pass_rate")) >= SLICE_PASS_RATE_MIN
        or _float(slices.get("regime_slice_pass_rate")) >= SLICE_PASS_RATE_MIN
    )
    max_monthly_turnover = _float(execution.get("max_monthly_turnover"))
    turnover_budget_ok = max_monthly_turnover <= TURNOVER_BUDGET_MAX_MONTHLY
    decision = COMPONENT_DECISION_CONTINUE
    if component == "growth_tilt_engine" and positive_static_gap:
        decision = COMPONENT_DECISION_REUSABLE
    elif component in {"turnover_budgeting", "combined_turnover_budgeting_and_valid_until"}:
        if turnover_reduced and cost_survives and positive_static_gap:
            decision = COMPONENT_DECISION_REUSABLE
        elif turnover_reduced:
            decision = COMPONENT_DECISION_GUARDRAIL
    elif component == "valid_until_strictness":
        if stale_reduced and positive_static_gap:
            decision = COMPONENT_DECISION_REUSABLE
        elif stale_reduced or _float(improvements.get("upside_capture_loss")) <= 0.0:
            decision = COMPONENT_DECISION_GUARDRAIL
    elif component == "lower_turnover_guardrail":
        decision = (
            COMPONENT_DECISION_GUARDRAIL
            if turnover_reduced or cost_survives
            else COMPONENT_DECISION_CONTINUE
        )
    elif component == "guarded_turnover_transfer":
        if cost_survives and positive_static_gap and turnover_budget_ok:
            decision = COMPONENT_DECISION_OWNER_REVIEW
        elif cost_survives or slices_ok:
            decision = COMPONENT_DECISION_GUARDRAIL
    if not positive_static_gap and not (turnover_reduced or stale_reduced or cost_survives):
        decision = COMPONENT_DECISION_REJECT
    result["component_reusable"] = decision == COMPONENT_DECISION_REUSABLE
    result["component_failure_reason"] = _component_failure_reason(
        decision=decision,
        positive_static_gap=positive_static_gap,
        turnover_reduced=turnover_reduced,
        stale_reduced=stale_reduced,
        cost_survives=cost_survives,
        slices_ok=slices_ok,
    )
    result["recommended_component_decision"] = decision
    return result


def _reusable_component_decision(
    component_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ranked = sorted(
        component_matrix,
        key=lambda row: (
            row.get("recommended_component_decision") == COMPONENT_DECISION_REUSABLE,
            _float(row.get("component_contribution_score")),
        ),
        reverse=True,
    )
    best = _as_mapping(ranked[0]) if ranked else {}
    decisions = {
        str(row.get("component_name")): str(row.get("recommended_component_decision"))
        for row in component_matrix
    }
    reusable = [
        str(row.get("component_name"))
        for row in component_matrix
        if row.get("recommended_component_decision") == COMPONENT_DECISION_REUSABLE
    ]
    guardrails = [
        str(row.get("component_name"))
        for row in component_matrix
        if row.get("recommended_component_decision") == COMPONENT_DECISION_GUARDRAIL
    ]
    owner_review = [
        str(row.get("component_name"))
        for row in component_matrix
        if row.get("recommended_component_decision") == COMPONENT_DECISION_OWNER_REVIEW
    ]
    return {
        "schema_version": "dynamic_strategy_reusable_component_decision.v1",
        "reusable_component_decision_ready": True,
        "best_reusable_component": best.get("component_name"),
        "best_reusable_component_decision": best.get("recommended_component_decision"),
        "component_decisions": decisions,
        "reusable_components": reusable,
        "guardrail_only_components": guardrails,
        "owner_review_required_components": owner_review,
        "recombination_candidate_direction": (
            "RECOMBINE_GROWTH_TILT_WITH_TURNOVER_AND_VALID_UNTIL_GUARDRAILS"
        ),
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _decision_update(
    *,
    component_matrix: Sequence[Mapping[str, Any]],
    reusable_decision: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_component_ablation_decision_update.v1",
        "decision_update_ready": True,
        "component_decisions_ready": True,
        "best_reusable_component": reusable_decision.get("best_reusable_component"),
        "component_decisions": reusable_decision.get("component_decisions"),
        "component_count": len(component_matrix),
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
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
        "as_of": as_of_date.isoformat() if as_of_date else data_quality.get("as_of"),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifacts": dict(_as_mapping(sources.get("source_artifacts"))),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_ablation_retest": bool(
            sources.get("source_ready_for_ablation_retest")
        ),
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "requested_date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else data_quality.get("as_of"),
        },
        "data_quality": dict(data_quality),
        "data_quality_gate_executed": True,
        "data_quality_passed": bool(data_quality.get("passed")),
        "data_quality_status": data_quality.get("status"),
        "data_quality_report_path": data_quality.get("report_path"),
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
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": list(COMPARISON_CADENCES),
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "fresh_market_data_read": True,
        "backtest_run": True,
        "ablation_retest_run": True,
        "new_signal_generated": False,
        "scoring_run": False,
        "research_only": SAFETY_BOUNDARY["research_only"],
        "observe_only": False,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "production_approved": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "artifact_paths": {},
    }


def _blocked_sections(reason: str, sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "blocked_reason": reason,
        "components_tested": list(COMPONENTS_TESTED),
        "ablation_candidates_tested": [],
        "ablation_retest_design": _ablation_retest_design(),
        "ablation_retest_result": [],
        "reference_candidate_result": [],
        "time_slice_matrix": [],
        "regime_slice_matrix": [],
        "time_regime_slice_matrix": [],
        "cost_stress_result": [],
        "cadence_comparison_result": [],
        "component_attribution_matrix": [],
        "reusable_component_decision": {
            "reusable_component_decision_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "decision_update": {
            "decision_update_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "best_reusable_component": None,
        "component_decisions": {},
        "component_decisions_ready": False,
        "ablation_retest_ready": False,
        "component_attribution_matrix_ready": False,
        "reusable_component_decision_ready": False,
        "decision_update_ready": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
        "source_ready_for_ablation_retest": bool(
            sources.get("source_ready_for_ablation_retest")
        ),
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
        "json_path": str(output_root / "ablation_retest_result.json"),
        "component_attribution_matrix_json": str(
            output_root / "component_attribution_matrix.json"
        ),
        "reusable_component_decision_json": str(
            output_root / "reusable_component_decision.json"
        ),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_component_attribution_targeted_ablation_retest.md"
        ),
        "ablation_result_markdown": str(
            docs_root / "dynamic_strategy_component_ablation_result.md"
        ),
        "reusable_component_decision_markdown": str(
            docs_root / "dynamic_strategy_reusable_component_decision.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2394_route.md"),
    }
    payload["artifact_paths"] = paths
    retest_helpers._write_json(Path(paths["json_path"]), payload)
    retest_helpers._write_json(
        Path(paths["component_attribution_matrix_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_component_attribution_matrix",
            "schema_version": "dynamic_strategy_component_attribution_matrix.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "primary_execution_cadence": payload.get("primary_execution_cadence"),
            "component_attribution_matrix": payload.get(
                "component_attribution_matrix",
                [],
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    retest_helpers._write_json(
        Path(paths["reusable_component_decision_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_reusable_component_decision",
            "schema_version": "dynamic_strategy_reusable_component_decision.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "reusable_component_decision": payload.get("reusable_component_decision"),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    retest_helpers._write_json(
        Path(paths["decision_update_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_component_ablation_decision_update",
            "schema_version": "dynamic_strategy_component_ablation_decision_update.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "decision_update": payload.get("decision_update"),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_markdown(Path(paths["markdown_path"]), _main_markdown(payload))
    _write_markdown(Path(paths["ablation_result_markdown"]), _ablation_markdown(payload))
    _write_markdown(
        Path(paths["reusable_component_decision_markdown"]),
        _reusable_decision_markdown(payload),
    )
    _write_markdown(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略组件归因 targeted ablation retest",
            "",
            f"- status：`{payload.get('status')}`",
            f"- data quality：`{payload.get('data_quality_status')}`",
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            f"- best reusable component：`{payload.get('best_reusable_component')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## 执行摘要",
            "",
            "TRADING-2393 已实际运行 targeted ablation retest，用同一 actual-position path "
            "评估组件级收益、回撤、换手、成本压力、stale signal 与 slice 表现。"
            "本报告不批准 observation、paper-shadow、scheduler、event append、outcome binding、"
            "production 或 broker 动作。",
            "",
            "## 2392 来源计划",
            "",
            "```json",
            _json_block(payload.get("source_status")),
            "```",
            "",
            "## ablation retest 设计",
            "",
            "```json",
            _json_block(payload.get("ablation_retest_design")),
            "```",
            "",
            "## 组件归因矩阵",
            "",
            _component_table(payload.get("component_attribution_matrix")),
            "",
            "## reusable component decision",
            "",
            "```json",
            _json_block(payload.get("reusable_component_decision")),
            "```",
            "",
            "## 明确未批准事项",
            "",
            "- candidate auto-accept：`False`",
            "- research-only observation：`False`",
            "- paper-shadow：`False`",
            "- event append：`False`",
            "- outcome binding：`False`",
            "- scheduler：`False`",
            "- production：`False`",
            "- broker/order：`False`",
        ]
    )


def _ablation_markdown(payload: Mapping[str, Any]) -> str:
    rows = _as_list_of_mappings(payload.get("ablation_retest_result"))
    lines = [
        "# 动态策略组件 ablation result",
        "",
        f"- status：`{payload.get('status')}`",
        "",
        "|candidate|annual_return|max_drawdown|turnover|stale|gap_static|",
        "|---|---|---|---|---|---|",
    ]
    for row in rows:
        if row.get("candidate_id") not in ABLATION_CANDIDATES:
            continue
        perf = _as_mapping(row.get("performance_metrics"))
        exe = _as_mapping(row.get("execution_metrics"))
        rel = _as_mapping(row.get("relative_metrics"))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{row.get('candidate_id')}`",
                    _fmt(perf.get("annualized_return")),
                    _fmt(perf.get("max_drawdown")),
                    _fmt(exe.get("turnover")),
                    str(exe.get("stale_signal_execution_count")),
                    _fmt(rel.get("dynamic_vs_static_gap")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _reusable_decision_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 reusable component decision",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best reusable component：`{payload.get('best_reusable_component')}`",
            "- observation approved：`False`",
            "- paper-shadow enabled：`False`",
            "",
            "```json",
            _json_block(payload.get("reusable_component_decision")),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 TRADING-2394 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 推荐下一路由：`{payload.get('recommended_next_research_task')}`",
            "- 下一步：component ablation owner review and recombination decision",
            "- observation approved：`False`",
            "- paper-shadow enabled：`False`",
            "- production enabled：`False`",
            "- broker action enabled：`False`",
            "",
            "TRADING-2394 只能记录 owner 对组件复用和 recombination 方向的复核决定；"
            "2393 本身不批准执行链路。",
        ]
    )


def _ablation_retest_design() -> dict[str, Any]:
    return {
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": list(COMPARISON_CADENCES),
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "components_tested": list(COMPONENTS_TESTED),
        "ablation_candidates": [
            {
                "candidate_id": candidate_id,
                "components": _ablation_components(candidate_id),
                "purpose": _ablation_purpose(candidate_id),
            }
            for candidate_id in ABLATION_CANDIDATES
        ],
        "cost_stress_scenarios": _cost_stress_scenarios(),
        "time_slices": [
            "full_available_window",
            "recent_period",
            "post_2023_ai_cycle",
            "high_volatility_periods",
            "drawdown_recovery_periods",
        ],
        "regime_slices": [
            "risk_on",
            "risk_off",
            "high_volatility",
            "low_volatility",
            "trend_confirmed",
            "recovery",
        ],
    }


def _component_retest_policy() -> dict[str, Any]:
    policy = retest_helpers._targeted_retest_policy()
    policy["policy_id"] = "dynamic_strategy_component_attribution_ablation_retest_v1"
    policy["rationale"] = (
        "TRADING-2393 tests component-level contribution after TRADING-2392 "
        "planned attribution and gate evidence."
    )
    return policy


def _cost_stress_scenarios() -> list[dict[str, Any]]:
    return retest_helpers._cost_stress_scenarios()


def _normalized_qqq_sgov(
    qqq: pd.Series,
    *,
    index: pd.Index,
    lower: float = 0.0,
    upper: float = 0.95,
) -> pd.DataFrame:
    qqq = qqq.reindex(index).ffill().fillna(0.0).clip(lower, upper)
    return pd.DataFrame({"QQQ": qqq, "SGOV": 1.0 - qqq}, index=index)


def _bounded_step_delta(series: pd.Series, *, max_delta: float) -> pd.Series:
    values: list[float] = []
    previous: float | None = None
    for raw in series.astype(float):
        value = float(raw)
        if previous is not None:
            value = max(previous - max_delta, min(previous + max_delta, value))
        values.append(value)
        previous = value
    return pd.Series(values, index=series.index)


def _ablation_role(candidate_id: str) -> str:
    return f"component_ablation_{candidate_id}"


def _ablation_components(candidate_id: str) -> list[str]:
    mapping = {
        "growth_tilt_only_reference": ["growth_tilt_engine"],
        "growth_tilt_plus_turnover_budget": ["growth_tilt_engine", "turnover_budgeting"],
        "growth_tilt_plus_valid_until_strict": [
            "growth_tilt_engine",
            "valid_until_strictness",
        ],
        "growth_tilt_plus_turnover_budget_and_valid_until": [
            "growth_tilt_engine",
            "turnover_budgeting",
            "valid_until_strictness",
        ],
        "lower_turnover_without_cooldown": ["lower_turnover_guardrail"],
        "lower_turnover_plus_growth_tilt_component": [
            "lower_turnover_guardrail",
            "guarded_turnover_transfer",
            "growth_tilt_engine",
        ],
    }
    return mapping[candidate_id]


def _ablation_purpose(candidate_id: str) -> str:
    mapping = {
        "growth_tilt_only_reference": "measure raw growth tilt engine",
        "growth_tilt_plus_turnover_budget": (
            "test whether turnover budgeting improves execution without killing return"
        ),
        "growth_tilt_plus_valid_until_strict": (
            "test whether strict expiry improves stale signal control"
        ),
        "growth_tilt_plus_turnover_budget_and_valid_until": (
            "test combined component transfer"
        ),
        "lower_turnover_without_cooldown": "measure cooldown contribution",
        "lower_turnover_plus_growth_tilt_component": (
            "test whether lower-turnover reference can gain upside without losing robustness"
        ),
    }
    return mapping[candidate_id]


def _component_contribution_score(
    *,
    annual_gap_reference: float,
    annual_gap_static: float,
    turnover_reduction: float,
    stale_reduction: float,
    drawdown_delta: float,
    time_pass: float,
    regime_pass: float,
) -> float:
    return round(
        annual_gap_static * 4.0
        + annual_gap_reference * 2.0
        + turnover_reduction
        + stale_reduction * 0.01
        + drawdown_delta
        + time_pass * 0.5
        + regime_pass * 0.5,
        6,
    )


def _component_failure_reason(
    *,
    decision: str,
    positive_static_gap: bool,
    turnover_reduced: bool,
    stale_reduced: bool,
    cost_survives: bool,
    slices_ok: bool,
) -> str:
    if decision == COMPONENT_DECISION_REUSABLE:
        return "none_component_has_clear_reusable_value"
    reasons = []
    if not positive_static_gap:
        reasons.append("non_positive_static_gap")
    if not turnover_reduced:
        reasons.append("turnover_not_materially_reduced")
    if not stale_reduced:
        reasons.append("stale_signal_not_materially_reduced")
    if not cost_survives:
        reasons.append("cost_stress_not_survived")
    if not slices_ok:
        reasons.append("time_or_regime_slice_not_robust")
    return ";".join(reasons) if reasons else "owner_review_tradeoff_remaining"


def _cost_stress_survival(
    cost_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    candidate_id: str,
) -> str:
    return _cost_stress_survival_for_rows(
        cost_rows.get((candidate_id, "realistic"), {}),
        cost_rows.get((candidate_id, "conservative"), {}),
        cost_rows.get((candidate_id, "harsh"), {}),
    )


def _cost_stress_survival_for_rows(
    realistic: Mapping[str, Any],
    conservative: Mapping[str, Any],
    harsh: Mapping[str, Any],
) -> str:
    if _relative_gap(harsh) > 0.0:
        return "harsh"
    if _relative_gap(conservative) > 0.0:
        return "conservative"
    if _relative_gap(realistic) > 0.0:
        return "realistic"
    return "failed"


def _cost_drag_delta(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    return round(_relative_gap(candidate) - _relative_gap(reference), 6)


def _turnover_adjusted_score(row: Mapping[str, Any]) -> float:
    return round(_relative_gap(row) - _float(_execution(row, "turnover")) * 0.01, 6)


def _return_retention(candidate: Mapping[str, Any], reference: Mapping[str, Any]) -> float:
    reference_annual = abs(_performance(reference, "annualized_return"))
    if reference_annual == 0.0:
        return 0.0
    return round(_performance(candidate, "annualized_return") / reference_annual, 6)


def _turnover_reduction(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    reference_turnover = _float(_execution(reference, "turnover"))
    if reference_turnover == 0.0:
        return 0.0
    return round(
        (reference_turnover - _float(_execution(candidate, "turnover")))
        / reference_turnover,
        6,
    )


def _stale_reduction(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    return round(
        _float(_execution(reference, "stale_signal_execution_count"))
        - _float(_execution(candidate, "stale_signal_execution_count")),
        6,
    )


def _lag_reduction(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    return round(
        _float(_execution(reference, "signal_to_execution_lag_days"))
        - _float(_execution(candidate, "signal_to_execution_lag_days")),
        6,
    )


def _upside_capture_loss(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    return round(
        _performance(reference, "upside_capture") - _performance(candidate, "upside_capture"),
        6,
    )


def _return_per_drawdown_penalty(
    candidate: Mapping[str, Any],
    reference: Mapping[str, Any],
) -> float:
    penalty = max(0.0, -_drawdown_delta(reference, candidate))
    if penalty == 0.0:
        return 0.0
    return round(_annual_gap(candidate, reference) / penalty, 6)


def _drawdown_penalty(candidate: Mapping[str, Any], reference: Mapping[str, Any]) -> float:
    return max(0.0, -_drawdown_delta(reference, candidate))


def _drawdown_delta(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    reference_drawdown = abs(_performance(reference, "max_drawdown"))
    candidate_drawdown = abs(_performance(candidate, "max_drawdown"))
    return round(reference_drawdown - candidate_drawdown, 6)


def _annual_gap(candidate: Mapping[str, Any], reference: Mapping[str, Any]) -> float:
    return round(
        _performance(candidate, "annualized_return")
        - _performance(reference, "annualized_return"),
        6,
    )


def _relative_gap(row: Mapping[str, Any]) -> float:
    return _float(_as_mapping(row.get("relative_metrics")).get("dynamic_vs_static_gap"))


def _performance(row: Mapping[str, Any], key: str) -> float:
    return _float(_as_mapping(row.get("performance_metrics")).get(key))


def _execution(row: Mapping[str, Any], key: str) -> Any:
    return _as_mapping(row.get("execution_metrics")).get(key)


def _pass_rate(rows: Sequence[Mapping[str, Any]], candidate_id: str) -> float:
    selected = [row for row in rows if row.get("candidate_id") == candidate_id]
    if not selected:
        return 0.0
    return round(sum(1 for row in selected if row.get("slice_passed") is True) / len(selected), 6)


def _rows_by_candidate(rows: Any) -> dict[str, dict[str, Any]]:
    return {str(row.get("candidate_id")): _as_mapping(row) for row in _as_list(rows)}


def _rows_by_candidate_and_scenario(rows: Any) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row.get("candidate_id")), str(row.get("scenario_id"))): _as_mapping(row)
        for row in _as_list(rows)
    }


def _top_candidate(rows: Any) -> str | None:
    values = _as_list_of_mappings(rows)
    if not values:
        return None
    return str(values[0].get("candidate_id"))


def _side_effect_validation_errors(label: str, source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in (
        "candidate_auto_accept_approved",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "paper_shadow_approved",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "event_append_enabled",
        "event_append_approved",
        "outcome_binding_enabled",
        "outcome_binding_approved",
        "outcome_store_mutated",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    ):
        if source.get(field) is True:
            errors.append(f"{label}_{field}_must_remain_false")
    if source.get("production_effect") not in (None, "none"):
        errors.append(f"{label}_production_effect_not_none")
    if source.get("broker_action") not in (None, "none"):
        errors.append(f"{label}_broker_action_not_none")
    return errors


def _component_table(rows: Any) -> str:
    lines = [
        "|component|candidate|decision|score|static_gap|turnover|stale|cost|",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in _as_list_of_mappings(rows):
        rel = _as_mapping(row.get("relative_metrics"))
        exe = _as_mapping(row.get("execution_metrics"))
        cost = _as_mapping(row.get("cost_stress_metrics"))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{row.get('component_name')}`",
                    f"`{row.get('test_candidate')}`",
                    f"`{row.get('recommended_component_decision')}`",
                    _fmt(row.get("component_contribution_score")),
                    _fmt(rel.get("dynamic_vs_static_gap")),
                    _fmt(exe.get("turnover")),
                    str(exe.get("stale_signal_execution_count")),
                    f"`{cost.get('survival')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _write_markdown(path: Path, content: str) -> None:
    path.write_text(content + "\n", encoding="utf-8")


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in _as_list(value) if isinstance(item, Mapping)]


def _fmt(value: Any) -> str:
    if isinstance(value, int | float):
        return f"{float(value):.6f}"
    return str(value)
