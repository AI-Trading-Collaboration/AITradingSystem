from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import ai_trading_system.dynamic_strategy_component_ablation_owner_review_decision as m2394
import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as m2393
import ai_trading_system.dynamic_strategy_component_recombination_candidate_plan as m2395
import ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest as m2386
import ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest as retest_helpers
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
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
    _file_sha256,
    _float,
    _load_execution_price_matrix,
    _load_policy_registry,
    _policies_by_id,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _data_quality_gate,
    _load_registry,
)

TASK_ID = "TRADING-2396"
TASK_REGISTER_ID = (
    "TRADING-2396_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST"
)
REPORT_TYPE = "dynamic_strategy_component_recombination_candidate_retest"
SCHEMA_VERSION = "dynamic_strategy_component_recombination_candidate_retest.v1"
READY_STATUS = "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_"
    "Observation_Decision"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2386", "TRADING-2393", "TRADING-2394", "TRADING-2395")

RECOMBINATION_CANDIDATES: tuple[str, ...] = tuple(
    m2395.PLANNED_RECOMBINATION_CANDIDATES
)
REFERENCE_CANDIDATES: dict[str, dict[str, str]] = dict(m2395.REFERENCE_CANDIDATES)
REFERENCE_CANDIDATE_IDS: tuple[str, ...] = (
    "static_baseline",
    m2386.RANKING_TOP_CANDIDATE,
    m2386.BASE_CANDIDATE_ID,
    m2386.BEST_LOWER_TURNOVER_VARIANT,
    m2386.BEST_GUARDED_VARIANT,
)
COMPARISON_CADENCES: tuple[str, ...] = (
    PRIMARY_EXECUTION_CADENCE,
    "cooldown_limited_event_driven",
    "signal_event_driven",
)

DECISION_OBSERVATION_PREVIEW = "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION_PREVIEW"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_CONTINUE_OPTIMIZATION = "CONTINUE_OPTIMIZATION"
DECISION_COMPONENT_VALUE_ONLY = "COMPONENT_VALUE_ONLY"
DECISION_REJECT = "REJECT_FOR_NOW"

# TRADING-2396 recombination retest classification constants. These are
# research sorting rules for the 2397 owner review package, not observation,
# scheduler, production, or broker approval gates.
OWNER_REVIEW_RETURN_RETENTION_MIN = 0.45
OWNER_REVIEW_TURNOVER_WORSE_TOLERANCE = 0.05
OWNER_REVIEW_DRAWDOWN_WORSE_TOLERANCE = 0.04
PREVIEW_TIME_SLICE_PASS_RATE_MIN = 0.50
PREVIEW_REGIME_EXPECTATION_SCORE_MIN = 0.50
PREVIEW_DRAWDOWN_WORSE_TOLERANCE = 0.03
COMPONENT_VALUE_TURNOVER_REDUCTION_MIN = 0.05
COMPONENT_VALUE_STALE_REDUCTION_MIN = 1.0

# Deterministic candidate-construction knobs from the owner-approved 2395
# plan. They define the six research variants; future statistical calibration
# must be tracked as a separate task before these become policy thresholds.
LOWER_TURNOVER_GUARDED_BLEND = (0.45, 0.35, 0.20)
TURNOVER_BUDGETED_BLEND = (0.80, 0.20)
STRICT_VALID_UNTIL_BLEND = (0.85, 0.15)
TURNOVER_STRICT_BLEND = (0.55, 0.45)
GUARDED_TRANSFER_BLEND = (0.48, 0.32, 0.20)
CONSERVATIVE_GUARDED_BLEND = (0.55, 0.30, 0.15)

DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)

DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH = (
    m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    / "recombination_candidate_plan.json"
)
DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH = (
    m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    / "recombination_candidate_definitions.json"
)
DEFAULT_SOURCE_2395_RETEST_PLAN_PATH = (
    m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    / "retest_plan_2396.json"
)
DEFAULT_SOURCE_2395_ACCEPTANCE_CRITERIA_PATH = (
    m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    / "recombination_acceptance_criteria.json"
)
DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH = (
    m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH = (
    m2394.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "component_recombination_decision.json"
)
DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "ablation_retest_result.json"
)
DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "component_attribution_matrix.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    m2386.DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH = (
    m2386.DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_ranking.json"
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_shadow_allowed",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "event_append_approved",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "outcome_store_mutated",
    "production_enabled",
    "production_approved",
    "production_allowed",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
)


def run_dynamic_strategy_component_recombination_candidate_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_recombination_candidate_plan_2395_path: Path = (
        DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH
    ),
    source_candidate_definitions_2395_path: Path = (
        DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH
    ),
    source_retest_plan_2396_path: Path = DEFAULT_SOURCE_2395_RETEST_PLAN_PATH,
    source_acceptance_criteria_2395_path: Path = (
        DEFAULT_SOURCE_2395_ACCEPTANCE_CRITERIA_PATH
    ),
    source_owner_review_decision_2394_path: Path = (
        DEFAULT_SOURCE_2394_OWNER_REVIEW_DECISION_PATH
    ),
    source_component_recombination_decision_2394_path: Path = (
        DEFAULT_SOURCE_2394_COMPONENT_RECOMBINATION_DECISION_PATH
    ),
    source_ablation_retest_result_2393_path: Path = (
        DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH
    ),
    source_component_attribution_matrix_2393_path: Path = (
        DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH
    ),
    source_expanded_candidate_retest_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_2386_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_recombination_candidate_plan_2395_path=(
            source_recombination_candidate_plan_2395_path
        ),
        source_candidate_definitions_2395_path=source_candidate_definitions_2395_path,
        source_retest_plan_2396_path=source_retest_plan_2396_path,
        source_acceptance_criteria_2395_path=source_acceptance_criteria_2395_path,
        source_owner_review_decision_2394_path=source_owner_review_decision_2394_path,
        source_component_recombination_decision_2394_path=(
            source_component_recombination_decision_2394_path
        ),
        source_ablation_retest_result_2393_path=source_ablation_retest_result_2393_path,
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
        source_expanded_candidate_retest_2386_path=(
            source_expanded_candidate_retest_2386_path
        ),
        source_expanded_candidate_ranking_2386_path=(
            source_expanded_candidate_ranking_2386_path
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
    if not bool(sources["source_ready_for_recombination_retest"]):
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
    retest = _run_recombination_candidate_retest(prices=prices, policies=policies)
    ranking = _candidate_ranking(retest)
    component_evidence = _component_evidence_matrix(retest, ranking)
    decision_update = _decision_update(ranking=ranking, component_evidence=component_evidence)
    payload.update(
        {
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
            "comparison_cadences": list(COMPARISON_CADENCES),
            "monthly_rebalance": {
                "allowed_for_reference": True,
                "allowed_for_primary_decision": False,
            },
            "recombination_candidates_tested": list(RECOMBINATION_CANDIDATES),
            "reference_candidates": _reference_candidate_summary(),
            "recombination_retest_design": _retest_design(),
            "recombination_retest_result": retest["recombination_retest_result"],
            "reference_candidate_result": retest["reference_candidate_result"],
            "time_slice_matrix": retest["time_slice_matrix"],
            "regime_slice_matrix": retest["regime_slice_matrix"],
            "time_regime_slice_matrix": (
                retest["time_slice_matrix"] + retest["regime_slice_matrix"]
            ),
            "cost_stress_result": retest["cost_stress_result"],
            "cadence_comparison_result": retest["cadence_comparison_result"],
            "recombination_candidate_ranking": ranking,
            "component_evidence_matrix": component_evidence,
            "decision_update": decision_update,
            "best_recombination_candidate": decision_update.get(
                "best_recombination_candidate"
            ),
            "best_recombination_decision": decision_update.get(
                "best_recombination_decision"
            ),
            "recombination_retest_ready": True,
            "candidate_ranking_ready": True,
            "component_evidence_matrix_ready": True,
            "decision_update_ready": True,
            "research_quality_status": (
                "RECOMBINATION_RETEST_READY_REQUIRES_2397_OWNER_REVIEW"
            ),
            "recommended_next_research_task": NEXT_ROUTE,
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
        "source_ready_for_recombination_retest": not errors,
    }


def _source_validation_errors(
    documents: Mapping[str, Any],
    source_status: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "recombination_candidate_plan_2395": m2395.READY_STATUS,
        "candidate_definitions_2395": m2395.READY_STATUS,
        "retest_plan_2396": m2395.READY_STATUS,
        "acceptance_criteria_2395": m2395.READY_STATUS,
        "owner_review_decision_2394": m2394.READY_STATUS,
        "component_recombination_decision_2394": m2394.READY_STATUS,
        "ablation_retest_result_2393": m2393.READY_STATUS,
        "component_attribution_matrix_2393": m2393.READY_STATUS,
        "expanded_candidate_retest_2386": m2386.READY_STATUS,
        "expanded_candidate_ranking_2386": m2386.READY_STATUS,
    }
    for source_name, expected in expected_status.items():
        if source_status.get(source_name) != expected:
            errors.append(f"{source_name}_status_not_ready")

    plan = _as_mapping(documents.get("recombination_candidate_plan_2395"))
    definitions_doc = _as_mapping(documents.get("candidate_definitions_2395"))
    retest_plan_doc = _as_mapping(documents.get("retest_plan_2396"))
    acceptance = _as_mapping(documents.get("acceptance_criteria_2395"))
    owner_2394 = _as_mapping(documents.get("owner_review_decision_2394"))
    recombination_2394 = _as_mapping(
        documents.get("component_recombination_decision_2394")
    )
    ablation_2393 = _as_mapping(documents.get("ablation_retest_result_2393"))
    matrix_2393 = _as_mapping(documents.get("component_attribution_matrix_2393"))
    expanded_retest_2386 = _as_mapping(documents.get("expanded_candidate_retest_2386"))
    expanded_ranking_2386 = _as_mapping(documents.get("expanded_candidate_ranking_2386"))

    if plan.get("recommended_next_research_task") != m2395.NEXT_ROUTE:
        errors.append("2395_route_not_trading_2396")
    if plan.get("recombination_candidate_plan_ready") is not True:
        errors.append("2395_candidate_plan_not_ready")
    if plan.get("retest_plan_2396_ready") is not True:
        errors.append("2395_retest_plan_not_ready")
    if set(RECOMBINATION_CANDIDATES) != set(
        _as_list(plan.get("planned_recombination_candidates"))
    ):
        errors.append("2395_planned_candidates_mismatch")
    if set(RECOMBINATION_CANDIDATES) != _candidate_ids_from_definitions(
        definitions_doc.get("recombination_candidate_definitions")
    ):
        errors.append("2395_candidate_definitions_mismatch")

    retest_plan = _as_mapping(retest_plan_doc.get("retest_plan_2396"))
    cadence = _as_mapping(retest_plan.get("execution_cadence"))
    monthly = _as_mapping(cadence.get("monthly_rebalance"))
    if retest_plan.get("next_task") != m2395.NEXT_ROUTE:
        errors.append("2395_retest_plan_next_task_mismatch")
    if cadence.get("primary") != PRIMARY_EXECUTION_CADENCE:
        errors.append("2395_retest_plan_primary_cadence_mismatch")
    if monthly.get("allowed_for_primary_decision") is not False:
        errors.append("2395_retest_plan_monthly_primary_not_blocked")
    if set(RECOMBINATION_CANDIDATES) != set(
        _as_list(retest_plan.get("planned_recombination_candidates"))
    ):
        errors.append("2395_retest_plan_candidates_mismatch")
    if not _as_mapping(acceptance.get("recombination_acceptance_criteria")):
        errors.append("2395_acceptance_criteria_missing")

    if owner_2394.get("owner_decision") != m2394.OWNER_DECISION:
        errors.append("2394_owner_decision_mismatch")
    if owner_2394.get("recombination_plan_approved") is not True:
        errors.append("2394_recombination_plan_not_approved")
    if owner_2394.get("recommended_next_research_task") != m2394.NEXT_ROUTE:
        errors.append("2394_route_not_trading_2395")
    recombination = _as_mapping(
        recombination_2394.get("component_recombination_decision")
    )
    if recombination.get("record_ready") is not True:
        errors.append("2394_recombination_decision_not_ready")

    if ablation_2393.get("data_quality_gate_executed") is not True:
        errors.append("2393_data_quality_gate_missing")
    if ablation_2393.get("best_reusable_component") != m2395.RETURN_ENGINE_COMPONENT:
        errors.append("2393_best_reusable_component_mismatch")
    if ablation_2393.get("recommended_next_research_task") != m2393.NEXT_ROUTE:
        errors.append("2393_route_not_trading_2394")
    decisions = _component_decisions(matrix_2393)
    if decisions.get(m2395.RETURN_ENGINE_COMPONENT) != m2393.COMPONENT_DECISION_REUSABLE:
        errors.append("2393_growth_tilt_engine_not_reusable")
    if decisions.get(m2395.LOWER_TURNOVER_GUARDRAIL) != (
        m2393.COMPONENT_DECISION_GUARDRAIL
    ):
        errors.append("2393_lower_turnover_guardrail_decision_mismatch")
    if decisions.get(m2395.GUARDED_TURNOVER_TRANSFER) != (
        m2393.COMPONENT_DECISION_OWNER_REVIEW
    ):
        errors.append("2393_guarded_turnover_transfer_decision_mismatch")

    if expanded_retest_2386.get("data_quality_gate_executed") is not True:
        errors.append("2386_data_quality_gate_missing")
    if expanded_retest_2386.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("2386_primary_cadence_not_valid_until_window")
    ranking_ids = {
        str(row.get("candidate_id"))
        for row in _as_list_of_mappings(
            expanded_ranking_2386.get("expanded_candidate_ranking")
        )
    }
    for required in REFERENCE_CANDIDATE_IDS[1:]:
        if required not in ranking_ids:
            errors.append(f"2386_ranking_missing_{required}")

    for label, source in (
        ("2395_plan", plan),
        ("2395_definitions", definitions_doc),
        ("2395_retest_plan", retest_plan_doc),
        ("2395_acceptance", acceptance),
        ("2394_owner", owner_2394),
        ("2394_recombination", recombination_2394),
        ("2393_ablation", ablation_2393),
        ("2393_matrix", matrix_2393),
        ("2386_retest", expanded_retest_2386),
        ("2386_ranking", expanded_ranking_2386),
    ):
        errors.extend(_side_effect_validation_errors(label, source))
    return errors


def _run_recombination_candidate_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    static_bundle = retest_helpers._static_bundle(prices)
    base_scenario = retest_helpers._cost_stress_scenarios()[1]
    reference_bundles = {
        "raw_growth_tilt_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=m2386.RANKING_TOP_CANDIDATE,
            scenario=base_scenario,
        ),
        "lower_turnover_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=m2386.BASE_CANDIDATE_ID,
            scenario=base_scenario,
        ),
        "cooldown_balanced_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=m2386.BEST_LOWER_TURNOVER_VARIANT,
            scenario=base_scenario,
        ),
        "guarded_turnover_reference": _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=m2386.BEST_GUARDED_VARIANT,
            scenario=base_scenario,
        ),
    }
    ranking_bundle = reference_bundles["raw_growth_tilt_reference"]
    candidate_bundles = {
        candidate_id: _recombination_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=base_scenario,
        )
        for candidate_id in RECOMBINATION_CANDIDATES
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
    for candidate_id, bundle in candidate_bundles.items():
        row = retest_helpers._full_sample_result_row(
            candidate_id=candidate_id,
            role=_candidate_role(candidate_id),
            bundle=bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        )
        row["recombination_components"] = _candidate_components(candidate_id)
        row["recombination_purpose"] = _candidate_purpose(candidate_id)
        full_rows.append(row)

    full_rows = _attach_relative_candidate_metrics(full_rows)
    time_rows = retest_helpers._slice_rows(
        prices=prices,
        slice_group="time_slice",
        slice_definitions=retest_helpers._time_slice_definitions(
            prices,
            _recombination_retest_policy(),
        ),
        bundles=candidate_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=m2386.RANKING_TOP_CANDIDATE,
    )
    regime_rows = retest_helpers._slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=retest_helpers._regime_slice_definitions(
            prices,
            _recombination_retest_policy(),
        ),
        bundles=candidate_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=m2386.RANKING_TOP_CANDIDATE,
    )
    return {
        "recombination_retest_result": full_rows,
        "reference_candidate_result": [
            row
            for row in full_rows
            if str(row.get("candidate_id")) in set(REFERENCE_CANDIDATE_IDS)
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
    return m2393._bundle_from_target_weights(
        prices=prices,
        policies=policies,
        candidate_id=candidate_id,
        target_weights=m2386._expanded_target_weights(candidate_id=candidate_id, prices=prices),
        cadence=cadence,
        scenario=scenario,
    )


def _recombination_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    cadence: str,
    scenario: Mapping[str, Any],
) -> dict[str, Any]:
    return m2393._bundle_from_target_weights(
        prices=prices,
        policies=policies,
        candidate_id=candidate_id,
        target_weights=_recombination_target_weights(candidate_id=candidate_id, prices=prices),
        cadence=cadence,
        scenario=scenario,
    )


def _recombination_target_weights(
    *,
    candidate_id: str,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    ranking = m2386._expanded_target_weights(
        candidate_id=m2386.RANKING_TOP_CANDIDATE,
        prices=prices,
    )
    lower = m2386._expanded_target_weights(
        candidate_id=m2386.BASE_CANDIDATE_ID,
        prices=prices,
    )
    cooldown = m2386._expanded_target_weights(
        candidate_id=m2386.BEST_LOWER_TURNOVER_VARIANT,
        prices=prices,
    )
    guarded = m2386._expanded_target_weights(
        candidate_id=m2386.BEST_GUARDED_VARIANT,
        prices=prices,
    )
    turnover_budgeted = m2386._expanded_target_weights(
        candidate_id="dynamic_turnover_budgeted_growth_tilt_v1",
        prices=prices,
    )
    valid_until_strict = m2386._expanded_target_weights(
        candidate_id="dynamic_valid_until_expiry_strict_v1",
        prices=prices,
    )
    if candidate_id == "growth_tilt_lower_turnover_guarded_v1":
        qqq = (
            ranking["QQQ"] * LOWER_TURNOVER_GUARDED_BLEND[0]
            + cooldown["QQQ"] * LOWER_TURNOVER_GUARDED_BLEND[1]
            + lower["QQQ"] * LOWER_TURNOVER_GUARDED_BLEND[2]
        )
        qqq = m2393._bounded_step_delta(qqq.rolling(4, min_periods=1).mean(), max_delta=0.12)
        return m2393._normalized_qqq_sgov(qqq, index=prices.index, lower=0.16, upper=0.86)
    if candidate_id == "growth_tilt_turnover_budgeted_v1":
        qqq = (
            turnover_budgeted["QQQ"] * TURNOVER_BUDGETED_BLEND[0]
            + ranking["QQQ"] * TURNOVER_BUDGETED_BLEND[1]
        )
        qqq = m2393._bounded_step_delta(qqq.rolling(6, min_periods=1).mean(), max_delta=0.10)
        return m2393._normalized_qqq_sgov(qqq, index=prices.index, lower=0.18, upper=0.84)
    if candidate_id == "growth_tilt_valid_until_strict_v1":
        qqq = (
            valid_until_strict["QQQ"] * STRICT_VALID_UNTIL_BLEND[0]
            + ranking["QQQ"] * STRICT_VALID_UNTIL_BLEND[1]
        )
        qqq = m2393._bounded_step_delta(qqq.rolling(8, min_periods=1).mean(), max_delta=0.08)
        return m2393._normalized_qqq_sgov(qqq, index=prices.index, lower=0.20, upper=0.80)
    if candidate_id == "growth_tilt_turnover_budgeted_valid_until_strict_v1":
        qqq = (
            turnover_budgeted["QQQ"] * TURNOVER_STRICT_BLEND[0]
            + valid_until_strict["QQQ"] * TURNOVER_STRICT_BLEND[1]
        )
        qqq = m2393._bounded_step_delta(qqq.rolling(5, min_periods=1).mean(), max_delta=0.08)
        return m2393._normalized_qqq_sgov(qqq, index=prices.index, lower=0.20, upper=0.82)
    if candidate_id == "growth_tilt_lower_turnover_guarded_transfer_v1":
        qqq = (
            cooldown["QQQ"] * GUARDED_TRANSFER_BLEND[0]
            + guarded["QQQ"] * GUARDED_TRANSFER_BLEND[1]
            + ranking["QQQ"] * GUARDED_TRANSFER_BLEND[2]
        )
        qqq = m2393._bounded_step_delta(qqq.rolling(5, min_periods=1).mean(), max_delta=0.10)
        return m2393._normalized_qqq_sgov(qqq, index=prices.index, lower=0.18, upper=0.84)
    if candidate_id == "growth_tilt_conservative_guarded_v1":
        qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
        drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
        high_vol = qqq_returns.rolling(20, min_periods=5).std().ge(
            qqq_returns.rolling(20, min_periods=5).std().quantile(0.75)
        )
        risk_off = drawdown.le(-0.10) | (high_vol.fillna(False) & qqq_returns.lt(0.0))
        qqq = (
            cooldown["QQQ"] * CONSERVATIVE_GUARDED_BLEND[0]
            + lower["QQQ"] * CONSERVATIVE_GUARDED_BLEND[1]
            + valid_until_strict["QQQ"] * CONSERVATIVE_GUARDED_BLEND[2]
        )
        qqq.loc[risk_off] = lower.loc[risk_off, "QQQ"] * 0.92
        qqq = m2393._bounded_step_delta(qqq.rolling(8, min_periods=1).mean(), max_delta=0.08)
        return m2393._normalized_qqq_sgov(qqq, index=prices.index, lower=0.20, upper=0.74)
    raise ValueError(f"Unknown TRADING-2396 recombination candidate: {candidate_id}")


def _cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in retest_helpers._cost_stress_scenarios():
        ranking_bundle = _reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=m2386.RANKING_TOP_CANDIDATE,
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
                primary_candidate=m2386.RANKING_TOP_CANDIDATE,
            )
        )
        for candidate_id in RECOMBINATION_CANDIDATES:
            bundle = _recombination_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
            )
            row = retest_helpers._stress_result_row(
                candidate_id=candidate_id,
                role=_candidate_role(candidate_id),
                scenario=scenario,
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=m2386.RANKING_TOP_CANDIDATE,
            )
            row["recombination_components"] = _candidate_components(candidate_id)
            rows.append(row)
    return _attach_relative_candidate_metrics(rows)


def _cadence_comparison_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scenario = retest_helpers._cost_stress_scenarios()[1]
    for cadence in COMPARISON_CADENCES:
        for candidate_id in RECOMBINATION_CANDIDATES:
            bundle = _recombination_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=cadence,
                scenario=scenario,
            )
            row = retest_helpers._stress_result_row(
                candidate_id=candidate_id,
                role=f"{_candidate_role(candidate_id)}_{cadence}",
                scenario={
                    **scenario,
                    "scenario_id": cadence,
                    "scenario_group": "cadence_comparison",
                },
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=m2386.RANKING_TOP_CANDIDATE,
            )
            row["comparison_cadence"] = cadence
            row["monthly_rebalance_allowed_for_primary_decision"] = False
            rows.append(row)
    return _attach_relative_candidate_metrics(rows)


def _candidate_ranking(retest: Mapping[str, Any]) -> list[dict[str, Any]]:
    full = _rows_by_candidate(retest.get("recombination_retest_result"))
    cost = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    time_rows = _as_list_of_mappings(retest.get("time_slice_matrix"))
    regime_rows = _as_list_of_mappings(retest.get("regime_slice_matrix"))
    raw = full.get(m2386.RANKING_TOP_CANDIDATE, {})
    rows: list[dict[str, Any]] = []
    for candidate_id in RECOMBINATION_CANDIDATES:
        row = full.get(candidate_id, {})
        performance = _as_mapping(row.get("performance_metrics"))
        relative = _as_mapping(row.get("relative_metrics"))
        execution = _as_mapping(row.get("execution_metrics"))
        time_pass = _pass_rate(time_rows, candidate_id)
        regime_score = _regime_expectation_score(regime_rows, candidate_id, row, raw)
        survival = _cost_stress_survival(cost, candidate_id)
        decision, reason = _candidate_decision(
            row=row,
            raw_row=raw,
            survival=survival,
            time_pass_rate=time_pass,
            regime_expectation_score=regime_score,
        )
        rows.append(
            {
                "rank": 0,
                "candidate_id": candidate_id,
                "total_return": performance.get("total_return"),
                "annualized_return": performance.get("annualized_return"),
                "cost_adjusted_return": performance.get("annualized_return"),
                "max_drawdown": performance.get("max_drawdown"),
                "turnover": execution.get("turnover"),
                "stale_signal_execution_count": execution.get(
                    "stale_signal_execution_count"
                ),
                "time_slice_pass_rate": time_pass,
                "regime_expectation_score": regime_score,
                "return_retention_vs_raw_growth_tilt": relative.get(
                    "return_retention_vs_raw_growth_tilt"
                ),
                "turnover_reduction_vs_raw_growth_tilt": relative.get(
                    "turnover_reduction_vs_raw_growth_tilt"
                ),
                "cost_stress_survival": survival,
                "decision": decision,
                "decision_reason": reason,
                "valid_until_window_preserved": True,
                "no_stale_signal_carry_forward": _float(
                    execution.get("stale_signal_execution_count")
                )
                == 0.0,
                "monthly_rebalance_allowed_for_primary_decision": False,
                "research_only_observation_approved": False,
                "paper_shadow_enabled": False,
                "production_enabled": False,
                "broker_action_enabled": False,
            }
        )
    rows.sort(
        key=lambda item: (
            _decision_rank(str(item.get("decision"))),
            _cost_survival_rank(str(item.get("cost_stress_survival"))),
            _float(item.get("cost_adjusted_return")),
            _float(item.get("return_retention_vs_raw_growth_tilt")),
            _float(item.get("turnover_reduction_vs_raw_growth_tilt")),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _component_evidence_matrix(
    retest: Mapping[str, Any],
    ranking: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    full = _rows_by_candidate(retest.get("recombination_retest_result"))
    cost = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    time_rows = _as_list_of_mappings(retest.get("time_slice_matrix"))
    regime_rows = _as_list_of_mappings(retest.get("regime_slice_matrix"))
    rank_by_candidate = {str(row.get("candidate_id")): row for row in ranking}
    raw = full.get(m2386.RANKING_TOP_CANDIDATE, {})
    rows: list[dict[str, Any]] = []
    for candidate_id in RECOMBINATION_CANDIDATES:
        row = full.get(candidate_id, {})
        performance = _as_mapping(row.get("performance_metrics"))
        relative = _as_mapping(row.get("relative_metrics"))
        execution = _as_mapping(row.get("execution_metrics"))
        rank_row = _as_mapping(rank_by_candidate.get(candidate_id))
        time_pass = _pass_rate(time_rows, candidate_id)
        regime_score = _regime_expectation_score(regime_rows, candidate_id, row, raw)
        realistic = cost.get((candidate_id, "realistic"), {})
        conservative = cost.get((candidate_id, "conservative"), {})
        harsh = cost.get((candidate_id, "harsh"), {})
        rows.append(
            {
                "candidate_id": candidate_id,
                "components": _candidate_components(candidate_id),
                "purpose": _candidate_purpose(candidate_id),
                "return_engine_metrics": {
                    "return_retention_vs_raw_growth_tilt": relative.get(
                        "return_retention_vs_raw_growth_tilt"
                    ),
                    "upside_capture": performance.get("upside_capture"),
                    "dynamic_vs_static_gap": relative.get("dynamic_vs_static_gap"),
                    "cost_adjusted_dynamic_vs_static_gap": relative.get(
                        "cost_adjusted_dynamic_vs_static_gap"
                    ),
                },
                "guardrail_metrics": {
                    "turnover_reduction_vs_raw_growth_tilt": relative.get(
                        "turnover_reduction_vs_raw_growth_tilt"
                    ),
                    "cost_drag_reduction": _cost_drag_reduction(row, raw),
                    "max_monthly_turnover": execution.get("max_monthly_turnover"),
                    "drawdown_gap_vs_static": relative.get("drawdown_gap_vs_static"),
                    "realistic_cost_passed": _relative_gap(realistic) > 0.0,
                    "conservative_cost_passed": _relative_gap(conservative) > 0.0,
                    "harsh_cost_passed": _relative_gap(harsh) > 0.0,
                },
                "valid_until_metrics": {
                    "stale_signal_execution_count": execution.get(
                        "stale_signal_execution_count"
                    ),
                    "signal_to_execution_lag_days": execution.get(
                        "signal_to_execution_lag_days"
                    ),
                    "near_expiry_signal_behavior": (
                        "NO_STALE_EXECUTION"
                        if _float(execution.get("stale_signal_execution_count")) == 0.0
                        else "STALE_EXECUTION_OBSERVED"
                    ),
                    "no_stale_signal_carry_forward": _float(
                        execution.get("stale_signal_execution_count")
                    )
                    == 0.0,
                    "valid_until_window_preserved": True,
                },
                "recombination_quality": {
                    "cost_adjusted_return": performance.get("annualized_return"),
                    "return_per_drawdown_penalty": _return_per_drawdown_penalty(row),
                    "time_slice_pass_rate": time_pass,
                    "regime_expectation_score": regime_score,
                    "owner_review_required": rank_row.get("decision")
                    in {DECISION_OBSERVATION_PREVIEW, DECISION_OWNER_REVIEW},
                    "candidate_decision": rank_row.get("decision"),
                    "decision_reason": rank_row.get("decision_reason"),
                },
                "candidate_auto_accept_approved": False,
                "research_only_observation_approved": False,
                "paper_shadow_enabled": False,
                "production_enabled": False,
                "broker_action_enabled": False,
            }
        )
    return rows


def _decision_update(
    *,
    ranking: Sequence[Mapping[str, Any]],
    component_evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    best = _as_mapping(ranking[0] if ranking else {})
    decisions = {
        str(row.get("candidate_id")): str(row.get("decision")) for row in ranking
    }
    preview_candidates = [
        str(row.get("candidate_id"))
        for row in ranking
        if row.get("decision") == DECISION_OBSERVATION_PREVIEW
    ]
    owner_review_candidates = [
        str(row.get("candidate_id"))
        for row in ranking
        if row.get("decision") == DECISION_OWNER_REVIEW
    ]
    return {
        "schema_version": "dynamic_strategy_recombination_decision_update.v1",
        "decision_update_ready": True,
        "best_recombination_candidate": best.get("candidate_id"),
        "best_recombination_decision": best.get("decision"),
        "candidate_decisions": decisions,
        "observation_preview_candidates": preview_candidates,
        "owner_review_required_candidates": owner_review_candidates,
        "component_evidence_count": len(component_evidence),
        "candidate_auto_accept_approved": False,
        "research_only_observation_preview_exists": bool(preview_candidates),
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route_reason": (
            "TRADING-2396 can rank recombination candidates, but owner review "
            "must decide observation preview, no-approval, or continued optimization."
        ),
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
        "source_ready_for_recombination_retest": bool(
            sources.get("source_ready_for_recombination_retest")
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
        "recombination_retest_run": True,
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
        "recombination_candidates_tested": [],
        "reference_candidates": _reference_candidate_summary(),
        "recombination_retest_design": _retest_design(),
        "recombination_retest_result": [],
        "reference_candidate_result": [],
        "time_slice_matrix": [],
        "regime_slice_matrix": [],
        "time_regime_slice_matrix": [],
        "cost_stress_result": [],
        "cadence_comparison_result": [],
        "recombination_candidate_ranking": [],
        "component_evidence_matrix": [],
        "decision_update": {
            "decision_update_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "best_recombination_candidate": None,
        "best_recombination_decision": None,
        "recombination_retest_ready": False,
        "candidate_ranking_ready": False,
        "component_evidence_matrix_ready": False,
        "decision_update_ready": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
        "source_ready_for_recombination_retest": bool(
            sources.get("source_ready_for_recombination_retest")
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
        "json_path": str(output_root / "recombination_retest_result.json"),
        "candidate_ranking_json": str(output_root / "recombination_candidate_ranking.json"),
        "component_evidence_matrix_json": str(output_root / "component_evidence_matrix.json"),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_component_recombination_candidate_retest.md"
        ),
        "candidate_ranking_markdown": str(
            docs_root / "dynamic_strategy_recombination_candidate_ranking.md"
        ),
        "component_evidence_markdown": str(
            docs_root / "dynamic_strategy_recombination_component_evidence.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2397_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["candidate_ranking_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_candidate_ranking",
            "schema_version": "dynamic_strategy_recombination_candidate_ranking.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "primary_execution_cadence": payload.get("primary_execution_cadence"),
            "recombination_candidate_ranking": payload.get(
                "recombination_candidate_ranking",
                [],
            ),
            "best_recombination_candidate": payload.get("best_recombination_candidate"),
            "best_recombination_decision": payload.get("best_recombination_decision"),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["component_evidence_matrix_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_component_evidence",
            "schema_version": "dynamic_strategy_recombination_component_evidence.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "component_evidence_matrix": payload.get("component_evidence_matrix", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["decision_update_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_decision_update",
            "schema_version": "dynamic_strategy_recombination_decision_update.v1",
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
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["candidate_ranking_markdown"]),
        _ranking_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["component_evidence_markdown"]),
        _component_evidence_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _attach_relative_candidate_metrics(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = [dict(row) for row in rows]
    by_candidate = {str(row.get("candidate_id")): row for row in result}
    static = by_candidate.get("static_baseline", {})
    raw = by_candidate.get(m2386.RANKING_TOP_CANDIDATE, {})
    lower = by_candidate.get(m2386.BASE_CANDIDATE_ID, {})
    guarded = by_candidate.get(m2386.BEST_GUARDED_VARIANT, {})
    for row in result:
        candidate_id = str(row.get("candidate_id"))
        if candidate_id not in set(RECOMBINATION_CANDIDATES):
            continue
        relative = dict(_as_mapping(row.get("relative_metrics")))
        relative.update(
            {
                "candidate_vs_raw_growth_tilt_gap": _annual_gap(row, raw),
                "candidate_vs_lower_turnover_gap": _annual_gap(row, lower),
                "candidate_vs_guarded_turnover_gap": _annual_gap(row, guarded),
                "return_retention_vs_raw_growth_tilt": _return_retention(row, raw),
                "turnover_reduction_vs_raw_growth_tilt": _turnover_reduction(raw, row),
                "drawdown_gap_vs_static": _drawdown_delta(static, row),
                "cost_adjusted_dynamic_vs_static_gap": relative.get(
                    "cost_adjusted_dynamic_vs_static_gap",
                    relative.get("dynamic_vs_static_gap"),
                ),
            }
        )
        row["relative_metrics"] = relative
        execution = dict(_as_mapping(row.get("execution_metrics")))
        row["gate_metrics"] = {
            "valid_until_window_preserved": True,
            "no_stale_signal_carry_forward": _float(
                execution.get("stale_signal_execution_count")
            )
            == 0.0,
            "turnover_not_materially_worse_than_raw_growth_tilt": _float(
                execution.get("turnover")
            )
            <= _float(_execution(raw, "turnover")) * (
                1.0 + OWNER_REVIEW_TURNOVER_WORSE_TOLERANCE
            ),
            "drawdown_tradeoff_explainable": _float(
                relative.get("drawdown_gap_vs_static")
            )
            >= -OWNER_REVIEW_DRAWDOWN_WORSE_TOLERANCE,
            "monthly_rebalance_allowed_for_primary_decision": False,
        }
    return result


def _candidate_decision(
    *,
    row: Mapping[str, Any],
    raw_row: Mapping[str, Any],
    survival: str,
    time_pass_rate: float,
    regime_expectation_score: float,
) -> tuple[str, str]:
    relative = _as_mapping(row.get("relative_metrics"))
    execution = _as_mapping(row.get("execution_metrics"))
    realistic_positive = _relative_gap(row) > 0.0
    conservative_positive = survival in {"conservative", "harsh"}
    harsh_positive = survival == "harsh"
    no_stale = _float(execution.get("stale_signal_execution_count")) == 0.0
    return_retained = _float(relative.get("return_retention_vs_raw_growth_tilt"))
    turnover_reduction = _float(relative.get("turnover_reduction_vs_raw_growth_tilt"))
    drawdown_gap = _float(relative.get("drawdown_gap_vs_static"))
    raw_turnover = _float(_execution(raw_row, "turnover"))
    turnover_not_worse = _float(execution.get("turnover")) <= raw_turnover * (
        1.0 + OWNER_REVIEW_TURNOVER_WORSE_TOLERANCE
    )
    owner_review_passed = (
        realistic_positive
        and conservative_positive
        and no_stale
        and turnover_not_worse
        and drawdown_gap >= -OWNER_REVIEW_DRAWDOWN_WORSE_TOLERANCE
        and return_retained >= OWNER_REVIEW_RETURN_RETENTION_MIN
    )
    if owner_review_passed and (
        harsh_positive
        and time_pass_rate >= PREVIEW_TIME_SLICE_PASS_RATE_MIN
        and regime_expectation_score >= PREVIEW_REGIME_EXPECTATION_SCORE_MIN
        and drawdown_gap >= -PREVIEW_DRAWDOWN_WORSE_TOLERANCE
    ):
        return (
            DECISION_OBSERVATION_PREVIEW,
            "preview_gate_passed_but_2397_owner_decision_required",
        )
    if owner_review_passed:
        return (
            DECISION_OWNER_REVIEW,
            "owner_review_criteria_passed_observation_not_approved",
        )
    if realistic_positive and (
        turnover_reduction >= COMPONENT_VALUE_TURNOVER_REDUCTION_MIN
        or _stale_reduction(raw_row, row) >= COMPONENT_VALUE_STALE_REDUCTION_MIN
    ):
        return (
            DECISION_COMPONENT_VALUE_ONLY,
            "component_guardrail_value_observed_but_candidate_gate_not_passed",
        )
    if realistic_positive:
        return (
            DECISION_CONTINUE_OPTIMIZATION,
            "positive_static_gap_but_owner_review_criteria_not_met",
        )
    return DECISION_REJECT, "non_positive_realistic_cost_dynamic_vs_static_gap"


def _recombination_retest_policy() -> dict[str, Any]:
    policy = retest_helpers._targeted_retest_policy()
    policy["policy_id"] = "dynamic_strategy_component_recombination_retest_v1"
    policy["rationale"] = (
        "TRADING-2396 retests owner-planned component recombinations after "
        "TRADING-2395, using valid-until execution and no-side-effect research "
        "boundaries before any TRADING-2397 owner decision."
    )
    return policy


def _retest_design() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_component_recombination_retest_design.v1",
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": list(COMPARISON_CADENCES),
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "recombination_candidates": list(RECOMBINATION_CANDIDATES),
        "reference_candidates": _reference_candidate_summary(),
        "cost_stress_scenarios": retest_helpers._cost_stress_scenarios(),
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
        "decision_enums": [
            DECISION_OBSERVATION_PREVIEW,
            DECISION_OWNER_REVIEW,
            DECISION_CONTINUE_OPTIMIZATION,
            DECISION_COMPONENT_VALUE_ONLY,
            DECISION_REJECT,
        ],
    }


def _reference_candidate_summary() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for reference_name, details in REFERENCE_CANDIDATES.items():
        row = {"reference_name": reference_name, **dict(details)}
        if "candidate_id" not in row:
            row["candidate_id"] = "static_baseline"
        rows.append(row)
    return rows


def _component_decisions(matrix_doc: Mapping[str, Any]) -> dict[str, str]:
    rows = _as_list_of_mappings(matrix_doc.get("component_attribution_matrix"))
    return {
        str(row.get("component_name")): str(row.get("recommended_component_decision"))
        for row in rows
    }


def _candidate_ids_from_definitions(value: Any) -> set[str]:
    return {str(row.get("candidate_id")) for row in _as_list_of_mappings(value)}


def _side_effect_validation_errors(label: str, source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if source.get("production_effect") not in (None, "none"):
        errors.append(f"{label}_production_effect_not_none")
    if source.get("broker_action") not in (None, "none"):
        errors.append(f"{label}_broker_action_not_none")
    for field in SAFETY_FALSE_FIELDS:
        if source.get(field) is True:
            errors.append(f"{label}_{field}_must_remain_false")
    return errors


def _candidate_components(candidate_id: str) -> list[str]:
    definitions = {
        row["candidate_id"]: list(row["components"])
        for row in m2395._recombination_candidate_definitions()
    }
    return definitions.get(candidate_id, [])


def _candidate_purpose(candidate_id: str) -> str:
    definitions = {
        row["candidate_id"]: str(row["purpose"])
        for row in m2395._recombination_candidate_definitions()
    }
    return definitions.get(candidate_id, "")


def _candidate_role(candidate_id: str) -> str:
    return f"component_recombination_{candidate_id}"


def _cost_stress_survival(
    cost_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    candidate_id: str,
) -> str:
    if _relative_gap(cost_rows.get((candidate_id, "harsh"), {})) > 0.0:
        return "harsh"
    if _relative_gap(cost_rows.get((candidate_id, "conservative"), {})) > 0.0:
        return "conservative"
    if _relative_gap(cost_rows.get((candidate_id, "realistic"), {})) > 0.0:
        return "realistic"
    return "failed"


def _regime_expectation_score(
    regime_rows: Sequence[Mapping[str, Any]],
    candidate_id: str,
    candidate_row: Mapping[str, Any],
    raw_row: Mapping[str, Any],
) -> float:
    regime_pass = _pass_rate(regime_rows, candidate_id)
    retention = max(
        0.0,
        min(1.0, _float(_as_mapping(candidate_row.get("relative_metrics")).get(
            "return_retention_vs_raw_growth_tilt"
        ))),
    )
    turnover = max(0.0, min(1.0, _turnover_reduction(raw_row, candidate_row)))
    return round((regime_pass + retention + turnover) / 3.0, 6)


def _decision_rank(decision: str) -> int:
    return {
        DECISION_OBSERVATION_PREVIEW: 5,
        DECISION_OWNER_REVIEW: 4,
        DECISION_CONTINUE_OPTIMIZATION: 3,
        DECISION_COMPONENT_VALUE_ONLY: 2,
        DECISION_REJECT: 1,
    }.get(decision, 0)


def _cost_survival_rank(survival: str) -> int:
    return {"harsh": 4, "conservative": 3, "realistic": 2, "base": 1}.get(
        survival,
        0,
    )


def _return_per_drawdown_penalty(row: Mapping[str, Any]) -> float:
    annual = _performance(row, "annualized_return")
    drawdown = abs(_performance(row, "max_drawdown"))
    if drawdown == 0.0:
        return 0.0
    return round(annual / drawdown, 6)


def _cost_drag_reduction(candidate: Mapping[str, Any], raw: Mapping[str, Any]) -> float:
    return round(_relative_gap(candidate) - _relative_gap(raw), 6)


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


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 component recombination candidate retest",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best candidate：`{payload.get('best_recombination_candidate')}`",
            f"- best decision：`{payload.get('best_recombination_decision')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Executive summary",
            "",
            "TRADING-2396 使用 `valid_until_window` 主口径实际 retest 6 个 "
            "component recombination candidates。输出只用于 2397 owner review；"
            "本任务不批准 observation、paper-shadow、scheduler、event append、"
            "outcome binding、production 或 broker。",
            "",
            "## Source plan from TRADING-2395",
            "",
            "```json",
            _json_block(payload.get("source_status")),
            "```",
            "",
            "## Recombination candidate definitions",
            "",
            "```json",
            _json_block(payload.get("recombination_candidates_tested")),
            "```",
            "",
            "## Retest design",
            "",
            "```json",
            _json_block(payload.get("recombination_retest_design")),
            "```",
            "",
            "## Candidate ranking",
            "",
            _ranking_table(payload.get("recombination_candidate_ranking")),
            "",
            "## Component evidence matrix",
            "",
            "```json",
            _json_block(payload.get("component_evidence_matrix")),
            "```",
            "",
            "## Cost / turnover / valid-until evidence",
            "",
            "```json",
            _json_block(
                {
                    "cost_stress_result_count": len(
                        _as_list(payload.get("cost_stress_result"))
                    ),
                    "cadence_comparison_result_count": len(
                        _as_list(payload.get("cadence_comparison_result"))
                    ),
                }
            ),
            "```",
            "",
            "## Time / regime evidence",
            "",
            "```json",
            _json_block(
                {
                    "time_slice_result_count": len(
                        _as_list(payload.get("time_slice_matrix"))
                    ),
                    "regime_slice_result_count": len(
                        _as_list(payload.get("regime_slice_matrix"))
                    ),
                }
            ),
            "```",
            "",
            "## Decision update",
            "",
            "```json",
            _json_block(payload.get("decision_update")),
            "```",
            "",
            "## Explicit non-approval list",
            "",
            "不批准 observation；不进入 paper-shadow；不启用 scheduler；不 append event；"
            "不 bind outcome；不生成 daily report；不启用 production / broker。",
            "",
            "## Recommended next route",
            "",
            f"`{payload.get('recommended_next_research_task')}`",
        ]
    ) + "\n"


def _ranking_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 recombination candidate ranking",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best candidate：`{payload.get('best_recombination_candidate')}`",
            f"- best decision：`{payload.get('best_recombination_decision')}`",
            "",
            _ranking_table(payload.get("recombination_candidate_ranking")),
        ]
    ) + "\n"


def _component_evidence_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 recombination component evidence",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            "```json",
            _json_block(payload.get("component_evidence_matrix")),
            "```",
        ]
    ) + "\n"


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 TRADING-2397 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 推荐下一路由：`{payload.get('recommended_next_research_task')}`",
            f"- best recombination candidate：`{payload.get('best_recombination_candidate')}`",
            f"- best recombination decision：`{payload.get('best_recombination_decision')}`",
            "",
            "TRADING-2396 只生成 owner review package。即使出现 "
            "`ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION_PREVIEW`，也必须在 2397 "
            "单独记录 owner decision；2396 不批准 observation、paper-shadow、"
            "scheduler、production 或 broker。",
        ]
    ) + "\n"


def _ranking_table(rows: Any) -> str:
    lines = [
        "|rank|candidate|decision|annualized|drawdown|turnover|time pass|regime|cost|",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in _as_list_of_mappings(rows):
        lines.append(
            "|"
            + "|".join(
                [
                    str(row.get("rank")),
                    f"`{row.get('candidate_id')}`",
                    f"`{row.get('decision')}`",
                    _fmt(row.get("annualized_return")),
                    _fmt(row.get("max_drawdown")),
                    _fmt(row.get("turnover")),
                    _fmt(row.get("time_slice_pass_rate")),
                    _fmt(row.get("regime_expectation_score")),
                    f"`{row.get('cost_stress_survival')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _load_json_document(path: Path) -> Any:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    return json.loads(path.read_text(encoding="utf-8"))


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
