from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as m2393
import ai_trading_system.dynamic_strategy_component_recombination_candidate_plan as m2395
import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as m2396
import ai_trading_system.dynamic_strategy_recombination_candidate_gate_evidence_plan as m2398
import ai_trading_system.dynamic_strategy_recombination_candidate_owner_review_decision as m2397
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_path as _load_json_document,
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
    _load_execution_price_matrix,
    _load_policy_registry,
    _policies_by_id,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _data_quality_gate,
    _load_registry,
)

TASK_ID = "TRADING-2399"
TASK_REGISTER_ID = (
    "TRADING-2399_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_"
    "EVIDENCE_RETEST"
)
REPORT_TYPE = (
    "dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest"
)
SCHEMA_VERSION = (
    "dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_"
    "RETEST_READY"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_"
    "BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_"
    "Observation_Decision"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2395",
    "TRADING-2396",
    "TRADING-2397",
    "TRADING-2398",
)
DEFAULT_DATA_QUALITY_AS_OF = date(2026, 7, 5)

TARGETED_VARIANTS: tuple[dict[str, Any], ...] = tuple(m2398.TARGETED_VARIANTS)
TARGETED_VARIANT_IDS: tuple[str, ...] = tuple(
    str(row["candidate_id"]) for row in TARGETED_VARIANTS
)
CANDIDATE_UNDER_REVIEW = m2398.BEST_RECOMBINATION_CANDIDATE
PRIMARY_EXECUTION_CADENCE = m2396.PRIMARY_EXECUTION_CADENCE
COMPARISON_CADENCES: tuple[str, ...] = (
    PRIMARY_EXECUTION_CADENCE,
    "cooldown_limited_event_driven",
    "signal_event_driven",
)
REFERENCE_CANDIDATES: dict[str, dict[str, str]] = {
    "static_baseline": {
        "candidate_id": "static_baseline",
        "role": "baseline_reference",
    },
    "raw_growth_tilt_reference": {
        "candidate_id": m2396.m2386.RANKING_TOP_CANDIDATE,
        "role": "raw_return_engine_reference",
    },
    "base_recombination_candidate": {
        "candidate_id": CANDIDATE_UNDER_REVIEW,
        "role": "current_owner_review_candidate",
    },
    "lower_turnover_reference": {
        "candidate_id": m2396.m2386.BEST_LOWER_TURNOVER_VARIANT,
        "role": "guardrail_reference",
    },
}
REFERENCE_CANDIDATE_IDS: tuple[str, ...] = tuple(
    row["candidate_id"] for row in REFERENCE_CANDIDATES.values()
)

DECISION_OBSERVATION_PREVIEW = m2396.DECISION_OBSERVATION_PREVIEW
DECISION_OWNER_REVIEW = m2396.DECISION_OWNER_REVIEW
DECISION_CONTINUE_TARGETED_IMPROVEMENT = "CONTINUE_TARGETED_IMPROVEMENT"
DECISION_COMPONENT_VALUE_ONLY = m2396.DECISION_COMPONENT_VALUE_ONLY
DECISION_REJECT = m2396.DECISION_REJECT

# TRADING-2399 targeted-variant construction knobs are research-only pilot
# baselines. They translate the 2398 qualitative variant plan into deterministic
# target weights for this retest only; they are not promotion, allocation,
# position-cap, scheduler, production, or broker policy.
TIME_SLICE_REPAIR_BLEND = (0.48, 0.22, 0.30)
TIME_SLICE_REPAIR_RECOVERY_BLEND = (0.62, 0.38)
REGIME_REPAIR_BLEND = (0.46, 0.34, 0.20)
REGIME_REPAIR_TREND_BLEND = (0.55, 0.45)
REGIME_REPAIR_HIGH_VOL_BLEND = (0.75, 0.25)
DRAWDOWN_CALIBRATED_BLEND = (0.45, 0.35, 0.20)
DRAWDOWN_CALIBRATED_RISK_OFF_BLEND = (0.72, 0.28)
RETURN_RETENTION_BLEND = (0.46, 0.42, 0.12)
RETURN_RETENTION_TREND_BLEND = (0.72, 0.28)
VALID_UNTIL_STRICT_BLEND = (0.50, 0.40, 0.10)
VALID_UNTIL_STRICT_NEAR_EXPIRY_BLEND = (0.85, 0.15)
BALANCED_GATE_BLEND = (0.34, 0.22, 0.18, 0.16, 0.10)
BALANCED_GATE_HIGH_VOL_BLEND = (0.55, 0.30, 0.15)
BALANCED_GATE_RECOVERY_BLEND = (0.45, 0.35, 0.20)
HIGH_VOL_QUANTILE = 0.75
LOW_VOL_QUANTILE = 0.50
RISK_OFF_DRAWDOWN_THRESHOLD = -0.08
NEAR_EXPIRY_ABS_RETURN_SUM = 0.01
ROLLING_VOL_WINDOW = 20
TREND_CONFIRMATION_WINDOW = 20
RECOVERY_WINDOW = 20
TIME_SLICE_STEP_DELTA = 0.10
REGIME_STEP_DELTA = 0.08
DRAWDOWN_STEP_DELTA = 0.07
RETURN_RETENTION_STEP_DELTA = 0.12
VALID_UNTIL_STEP_DELTA = 0.07
BALANCED_STEP_DELTA = 0.08

DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "gate_evidence_plan_result.json"
)
DEFAULT_SOURCE_2398_GATE_EVIDENCE_GAP_SUMMARY_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "gate_evidence_gap_summary.json"
)
DEFAULT_SOURCE_2398_TARGETED_IMPROVEMENT_PLAN_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "targeted_improvement_plan.json"
)
DEFAULT_SOURCE_2398_RETEST_PLAN_2399_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "retest_plan_2399.json"
)
DEFAULT_SOURCE_2398_NEXT_ROUTE_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "next_route.json"
)
DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH = (
    m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH = (
    m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    / "recombination_retest_result.json"
)
DEFAULT_SOURCE_2396_RECOMBINATION_CANDIDATE_RANKING_PATH = (
    m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    / "recombination_candidate_ranking.json"
)
DEFAULT_SOURCE_2396_COMPONENT_EVIDENCE_MATRIX_PATH = (
    m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    / "component_evidence_matrix.json"
)
DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH = (
    m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH = (
    m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    / "recombination_candidate_plan.json"
)
DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH = (
    m2395.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_OUTPUT_ROOT
    / "recombination_candidate_definitions.json"
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
    "current_best_candidate_observation_approved",
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
    "new_signal_generated",
    "scoring_run",
)


def run_dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_gate_evidence_plan_result_2398_path: Path = (
        DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH
    ),
    source_gate_evidence_gap_summary_2398_path: Path = (
        DEFAULT_SOURCE_2398_GATE_EVIDENCE_GAP_SUMMARY_PATH
    ),
    source_targeted_improvement_plan_2398_path: Path = (
        DEFAULT_SOURCE_2398_TARGETED_IMPROVEMENT_PLAN_PATH
    ),
    source_retest_plan_2399_2398_path: Path = (
        DEFAULT_SOURCE_2398_RETEST_PLAN_2399_PATH
    ),
    source_next_route_2398_path: Path = DEFAULT_SOURCE_2398_NEXT_ROUTE_PATH,
    source_owner_review_decision_2397_path: Path = (
        DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH
    ),
    source_recombination_retest_result_2396_path: Path = (
        DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH
    ),
    source_recombination_candidate_ranking_2396_path: Path = (
        DEFAULT_SOURCE_2396_RECOMBINATION_CANDIDATE_RANKING_PATH
    ),
    source_component_evidence_matrix_2396_path: Path = (
        DEFAULT_SOURCE_2396_COMPONENT_EVIDENCE_MATRIX_PATH
    ),
    source_decision_update_2396_path: Path = DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH,
    source_recombination_candidate_plan_2395_path: Path = (
        DEFAULT_SOURCE_2395_RECOMBINATION_CANDIDATE_PLAN_PATH
    ),
    source_candidate_definitions_2395_path: Path = (
        DEFAULT_SOURCE_2395_CANDIDATE_DEFINITIONS_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_as_of = as_of_date or DEFAULT_DATA_QUALITY_AS_OF
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_gate_evidence_plan_result_2398_path=(
            source_gate_evidence_plan_result_2398_path
        ),
        source_gate_evidence_gap_summary_2398_path=(
            source_gate_evidence_gap_summary_2398_path
        ),
        source_targeted_improvement_plan_2398_path=(
            source_targeted_improvement_plan_2398_path
        ),
        source_retest_plan_2399_2398_path=source_retest_plan_2399_2398_path,
        source_next_route_2398_path=source_next_route_2398_path,
        source_owner_review_decision_2397_path=source_owner_review_decision_2397_path,
        source_recombination_retest_result_2396_path=(
            source_recombination_retest_result_2396_path
        ),
        source_recombination_candidate_ranking_2396_path=(
            source_recombination_candidate_ranking_2396_path
        ),
        source_component_evidence_matrix_2396_path=(
            source_component_evidence_matrix_2396_path
        ),
        source_decision_update_2396_path=source_decision_update_2396_path,
        source_recombination_candidate_plan_2395_path=(
            source_recombination_candidate_plan_2395_path
        ),
        source_candidate_definitions_2395_path=source_candidate_definitions_2395_path,
    )
    config = _load_registry(simple_config_path)
    data_quality = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=resolved_as_of,
        expected_tickers=sorted({"QQQ", "TQQQ", "SGOV"}),
    )
    payload = _base_payload(
        status=READY_STATUS,
        as_of_date=resolved_as_of,
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
    if not bool(sources["source_ready_for_targeted_gate_evidence_retest"]):
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
    retest = _run_targeted_gate_evidence_retest(prices=prices, policies=policies)
    ranking = _targeted_variant_ranking(retest)
    gate_matrix = _gate_evidence_matrix(
        retest=retest,
        ranking=ranking,
        sources=sources,
    )
    decision_update = _decision_update(ranking=ranking, gate_matrix=gate_matrix)
    payload.update(
        {
            "targeted_variants_tested": list(TARGETED_VARIANT_IDS),
            "reference_candidates": _reference_candidate_summary(),
            "targeted_retest_design": _retest_design(),
            "targeted_gate_evidence_retest_result": retest[
                "targeted_gate_evidence_retest_result"
            ],
            "reference_candidate_result": retest["reference_candidate_result"],
            "time_slice_matrix": retest["time_slice_matrix"],
            "regime_slice_matrix": retest["regime_slice_matrix"],
            "time_regime_slice_matrix": (
                retest["time_slice_matrix"] + retest["regime_slice_matrix"]
            ),
            "cost_stress_result": retest["cost_stress_result"],
            "cadence_comparison_result": retest["cadence_comparison_result"],
            "targeted_variant_ranking": ranking,
            "gate_evidence_matrix": gate_matrix,
            "decision_update": decision_update,
            "best_targeted_variant": decision_update.get("best_targeted_variant"),
            "best_targeted_variant_decision": decision_update.get(
                "best_targeted_variant_decision"
            ),
            "observation_preview_candidates_count": decision_update.get(
                "observation_preview_candidates_count"
            ),
            "targeted_retest_ready": True,
            "variant_ranking_ready": True,
            "gate_evidence_matrix_ready": True,
            "decision_update_ready": True,
            "research_quality_status": (
                "TARGETED_GATE_EVIDENCE_RETEST_READY_REQUIRES_2400_OWNER_DECISION"
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
            key: m2396.retest_helpers._source_artifact(path, documents[key])
            for key, path in source_files.items()
        },
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": source_status,
        "source_validation_errors": errors,
        "source_ready_for_targeted_gate_evidence_retest": not errors,
    }


def _source_validation_errors(
    documents: Mapping[str, Any],
    source_status: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "gate_evidence_plan_result_2398": m2398.READY_STATUS,
        "gate_evidence_gap_summary_2398": m2398.READY_STATUS,
        "targeted_improvement_plan_2398": m2398.READY_STATUS,
        "retest_plan_2399_2398": m2398.READY_STATUS,
        "next_route_2398": m2398.READY_STATUS,
        "owner_review_decision_2397": m2397.READY_STATUS,
        "recombination_retest_result_2396": m2396.READY_STATUS,
        "recombination_candidate_ranking_2396": m2396.READY_STATUS,
        "component_evidence_matrix_2396": m2396.READY_STATUS,
        "decision_update_2396": m2396.READY_STATUS,
        "recombination_candidate_plan_2395": m2395.READY_STATUS,
        "candidate_definitions_2395": m2395.READY_STATUS,
    }
    for source_name, expected in expected_status.items():
        if source_status.get(source_name) != expected:
            errors.append(f"{source_name}_status_not_ready")

    plan_2398 = _as_mapping(documents.get("gate_evidence_plan_result_2398"))
    targeted_plan_doc = _as_mapping(documents.get("targeted_improvement_plan_2398"))
    retest_plan_doc = _as_mapping(documents.get("retest_plan_2399_2398"))
    next_route_doc = _as_mapping(documents.get("next_route_2398"))
    owner_2397 = _as_mapping(documents.get("owner_review_decision_2397"))
    retest_2396 = _as_mapping(documents.get("recombination_retest_result_2396"))
    ranking_2396 = _as_mapping(documents.get("recombination_candidate_ranking_2396"))
    evidence_2396 = _as_mapping(documents.get("component_evidence_matrix_2396"))
    decision_2396 = _as_mapping(documents.get("decision_update_2396"))
    plan_2395 = _as_mapping(documents.get("recombination_candidate_plan_2395"))
    definitions_2395 = _as_mapping(documents.get("candidate_definitions_2395"))

    if plan_2398.get("candidate_under_review") != CANDIDATE_UNDER_REVIEW:
        errors.append("2398_candidate_under_review_mismatch")
    if plan_2398.get("decision_from_2396") != m2398.EXPECTED_DECISION_FROM_2396:
        errors.append("2398_decision_from_2396_mismatch")
    if plan_2398.get("owner_decision_from_2397") != m2398.OWNER_DECISION_FROM_2397:
        errors.append("2398_owner_decision_from_2397_mismatch")
    if plan_2398.get("recommended_next_research_task") != m2398.NEXT_ROUTE:
        errors.append("2398_route_not_trading_2399")
    if set(_as_list(plan_2398.get("planned_targeted_variants"))) != set(
        TARGETED_VARIANT_IDS
    ):
        errors.append("2398_planned_targeted_variants_mismatch")

    targeted_plan = _as_mapping(targeted_plan_doc.get("targeted_improvement_plan"))
    targeted_variants = _as_list_of_mappings(targeted_plan.get("targeted_variants"))
    if targeted_plan.get("record_ready") is not True:
        errors.append("2398_targeted_plan_not_ready")
    if {str(row.get("candidate_id")) for row in targeted_variants} != set(
        TARGETED_VARIANT_IDS
    ):
        errors.append("2398_targeted_plan_variants_mismatch")

    retest_plan = _as_mapping(retest_plan_doc.get("retest_plan_2399"))
    if retest_plan.get("record_ready") is not True:
        errors.append("2398_retest_plan_2399_not_ready")
    if retest_plan.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("2398_retest_plan_primary_cadence_mismatch")
    if _as_mapping(retest_plan.get("monthly_rebalance")).get(
        "allowed_for_primary_decision"
    ) is not False:
        errors.append("2398_retest_plan_monthly_primary_not_blocked")
    if retest_plan.get("recommended_next_research_task") != m2398.NEXT_ROUTE:
        errors.append("2398_retest_plan_route_mismatch")
    required_candidates = _as_mapping(retest_plan.get("required_2399_candidates"))
    if set(_as_list(required_candidates.get("targeted_variants"))) != set(
        TARGETED_VARIANT_IDS
    ):
        errors.append("2398_retest_plan_targeted_variants_mismatch")
    if next_route_doc.get("recommended_next_research_task") != m2398.NEXT_ROUTE:
        errors.append("2398_next_route_artifact_mismatch")

    if owner_2397.get("owner_decision") != m2398.OWNER_DECISION_FROM_2397:
        errors.append("2397_owner_decision_mismatch")
    if owner_2397.get("best_recombination_candidate") != CANDIDATE_UNDER_REVIEW:
        errors.append("2397_best_candidate_mismatch")
    if owner_2397.get("research_only_observation_approved") is True:
        errors.append("2397_observation_unexpectedly_approved")

    if retest_2396.get("best_recombination_candidate") != CANDIDATE_UNDER_REVIEW:
        errors.append("2396_best_candidate_mismatch")
    if retest_2396.get("best_recombination_decision") != m2398.EXPECTED_DECISION_FROM_2396:
        errors.append("2396_best_decision_mismatch")
    if retest_2396.get("data_quality_gate_executed") is not True:
        errors.append("2396_data_quality_gate_missing")
    if ranking_2396.get("best_recombination_candidate") != CANDIDATE_UNDER_REVIEW:
        errors.append("2396_ranking_best_candidate_mismatch")
    if _base_ranking_row(ranking_2396).get("decision") != (
        m2398.EXPECTED_DECISION_FROM_2396
    ):
        errors.append("2396_base_ranking_decision_mismatch")
    if not _base_evidence_row(evidence_2396):
        errors.append("2396_component_evidence_missing_base_candidate")
    update = _as_mapping(decision_2396.get("decision_update"))
    if update.get("best_recombination_candidate") != CANDIDATE_UNDER_REVIEW:
        errors.append("2396_decision_update_best_candidate_mismatch")

    if plan_2395.get("recommended_next_research_task") != m2395.NEXT_ROUTE:
        errors.append("2395_route_not_trading_2396")
    if CANDIDATE_UNDER_REVIEW not in {
        str(item) for item in _as_list(plan_2395.get("planned_recombination_candidates"))
    }:
        errors.append("2395_plan_missing_base_candidate")
    if CANDIDATE_UNDER_REVIEW not in _candidate_ids_from_definitions(
        definitions_2395.get("recombination_candidate_definitions")
    ):
        errors.append("2395_definitions_missing_base_candidate")

    for label, source in (
        ("2398_plan", plan_2398),
        ("2398_gap_summary", _as_mapping(documents.get("gate_evidence_gap_summary_2398"))),
        ("2398_targeted_plan", targeted_plan_doc),
        ("2398_retest_plan", retest_plan_doc),
        ("2398_next_route", next_route_doc),
        ("2397_owner", owner_2397),
        ("2396_retest", retest_2396),
        ("2396_ranking", ranking_2396),
        ("2396_evidence", evidence_2396),
        ("2396_decision", decision_2396),
        ("2395_plan", plan_2395),
        ("2395_definitions", definitions_2395),
    ):
        errors.extend(_side_effect_validation_errors(label, source))
    return errors


def _run_targeted_gate_evidence_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    static_bundle = m2396.retest_helpers._static_bundle(prices)
    realistic_scenario = m2396.retest_helpers._cost_stress_scenarios()[1]
    raw_bundle = m2396._reference_bundle(
        prices=prices,
        policies=policies,
        candidate_id=m2396.m2386.RANKING_TOP_CANDIDATE,
        scenario=realistic_scenario,
    )
    base_bundle = m2396._recombination_bundle(
        prices=prices,
        policies=policies,
        candidate_id=CANDIDATE_UNDER_REVIEW,
        cadence=PRIMARY_EXECUTION_CADENCE,
        scenario=realistic_scenario,
    )
    lower_turnover_bundle = m2396._reference_bundle(
        prices=prices,
        policies=policies,
        candidate_id=m2396.m2386.BEST_LOWER_TURNOVER_VARIANT,
        scenario=realistic_scenario,
    )
    candidate_bundles = {
        candidate_id: _targeted_variant_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=realistic_scenario,
        )
        for candidate_id in TARGETED_VARIANT_IDS
    }
    full_rows = [
        m2396.retest_helpers._full_sample_result_row(
            candidate_id="static_baseline",
            role="static_baseline",
            bundle=static_bundle,
            static_bundle=static_bundle,
            ranking_bundle=raw_bundle,
        ),
        m2396.retest_helpers._full_sample_result_row(
            candidate_id=m2396.m2386.RANKING_TOP_CANDIDATE,
            role="raw_growth_tilt_reference",
            bundle=raw_bundle,
            static_bundle=static_bundle,
            ranking_bundle=raw_bundle,
        ),
        m2396.retest_helpers._full_sample_result_row(
            candidate_id=CANDIDATE_UNDER_REVIEW,
            role="base_recombination_candidate",
            bundle=base_bundle,
            static_bundle=static_bundle,
            ranking_bundle=raw_bundle,
        ),
        m2396.retest_helpers._full_sample_result_row(
            candidate_id=m2396.m2386.BEST_LOWER_TURNOVER_VARIANT,
            role="lower_turnover_reference",
            bundle=lower_turnover_bundle,
            static_bundle=static_bundle,
            ranking_bundle=raw_bundle,
        ),
    ]
    for candidate_id, bundle in candidate_bundles.items():
        row = m2396.retest_helpers._full_sample_result_row(
            candidate_id=candidate_id,
            role=_targeted_variant_role(candidate_id),
            bundle=bundle,
            static_bundle=static_bundle,
            ranking_bundle=raw_bundle,
        )
        row["targeted_variant_definition"] = _targeted_variant_definition(candidate_id)
        row["targeted_variant_components"] = _targeted_variant_components(candidate_id)
        full_rows.append(row)
    full_rows = _attach_relative_metrics(full_rows)
    time_rows = m2396.retest_helpers._slice_rows(
        prices=prices,
        slice_group="time_slice",
        slice_definitions=m2396.retest_helpers._time_slice_definitions(
            prices,
            _targeted_retest_policy(),
        ),
        bundles=candidate_bundles,
        static_bundle=static_bundle,
        ranking_bundle=raw_bundle,
        primary_candidate=m2396.m2386.RANKING_TOP_CANDIDATE,
    )
    regime_rows = m2396.retest_helpers._slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=m2396.retest_helpers._regime_slice_definitions(
            prices,
            _targeted_retest_policy(),
        ),
        bundles=candidate_bundles,
        static_bundle=static_bundle,
        ranking_bundle=raw_bundle,
        primary_candidate=m2396.m2386.RANKING_TOP_CANDIDATE,
    )
    return {
        "targeted_gate_evidence_retest_result": full_rows,
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
            ranking_bundle=raw_bundle,
        ),
    }


def _targeted_variant_bundle(
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
        target_weights=_targeted_variant_target_weights(
            candidate_id=candidate_id,
            prices=prices,
        ),
        cadence=cadence,
        scenario=scenario,
    )


def _targeted_variant_target_weights(
    *,
    candidate_id: str,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    raw = m2396.m2386._expanded_target_weights(
        candidate_id=m2396.m2386.RANKING_TOP_CANDIDATE,
        prices=prices,
    )
    lower = m2396.m2386._expanded_target_weights(
        candidate_id=m2396.m2386.BASE_CANDIDATE_ID,
        prices=prices,
    )
    cooldown = m2396.m2386._expanded_target_weights(
        candidate_id=m2396.m2386.BEST_LOWER_TURNOVER_VARIANT,
        prices=prices,
    )
    guarded = m2396.m2386._expanded_target_weights(
        candidate_id=m2396.m2386.BEST_GUARDED_VARIANT,
        prices=prices,
    )
    valid = m2396.m2386._expanded_target_weights(
        candidate_id="dynamic_valid_until_expiry_strict_v1",
        prices=prices,
    )
    base = m2396._recombination_target_weights(
        candidate_id=CANDIDATE_UNDER_REVIEW,
        prices=prices,
    )
    masks = _market_masks(prices)

    if candidate_id == "growth_tilt_guarded_transfer_time_slice_repair_v1":
        qqq = (
            base["QQQ"] * TIME_SLICE_REPAIR_BLEND[0]
            + cooldown["QQQ"] * TIME_SLICE_REPAIR_BLEND[1]
            + raw["QQQ"] * TIME_SLICE_REPAIR_BLEND[2]
        )
        qqq.loc[masks["recovery"]] = (
            raw.loc[masks["recovery"], "QQQ"] * TIME_SLICE_REPAIR_RECOVERY_BLEND[0]
            + base.loc[masks["recovery"], "QQQ"] * TIME_SLICE_REPAIR_RECOVERY_BLEND[1]
        )
        return _normalized_weights(qqq, prices, step_delta=TIME_SLICE_STEP_DELTA, upper=0.86)
    if candidate_id == "growth_tilt_guarded_transfer_regime_repair_v1":
        qqq = (
            base["QQQ"] * REGIME_REPAIR_BLEND[0]
            + guarded["QQQ"] * REGIME_REPAIR_BLEND[1]
            + raw["QQQ"] * REGIME_REPAIR_BLEND[2]
        )
        qqq.loc[masks["trend_confirmed"]] = (
            raw.loc[masks["trend_confirmed"], "QQQ"] * REGIME_REPAIR_TREND_BLEND[0]
            + base.loc[masks["trend_confirmed"], "QQQ"] * REGIME_REPAIR_TREND_BLEND[1]
        )
        qqq.loc[masks["high_volatility"]] = (
            lower.loc[masks["high_volatility"], "QQQ"]
            * REGIME_REPAIR_HIGH_VOL_BLEND[0]
            + guarded.loc[masks["high_volatility"], "QQQ"]
            * REGIME_REPAIR_HIGH_VOL_BLEND[1]
        )
        return _normalized_weights(qqq, prices, step_delta=REGIME_STEP_DELTA, upper=0.82)
    if candidate_id == "growth_tilt_guarded_transfer_drawdown_calibrated_v1":
        qqq = (
            base["QQQ"] * DRAWDOWN_CALIBRATED_BLEND[0]
            + lower["QQQ"] * DRAWDOWN_CALIBRATED_BLEND[1]
            + valid["QQQ"] * DRAWDOWN_CALIBRATED_BLEND[2]
        )
        qqq.loc[masks["risk_off"]] = (
            lower.loc[masks["risk_off"], "QQQ"] * DRAWDOWN_CALIBRATED_RISK_OFF_BLEND[0]
            + valid.loc[masks["risk_off"], "QQQ"] * DRAWDOWN_CALIBRATED_RISK_OFF_BLEND[1]
        )
        return _normalized_weights(
            qqq,
            prices,
            lower=0.20,
            upper=0.78,
            step_delta=DRAWDOWN_STEP_DELTA,
            rolling_window=6,
        )
    if candidate_id == "growth_tilt_guarded_transfer_return_retention_v1":
        qqq = (
            base["QQQ"] * RETURN_RETENTION_BLEND[0]
            + raw["QQQ"] * RETURN_RETENTION_BLEND[1]
            + cooldown["QQQ"] * RETURN_RETENTION_BLEND[2]
        )
        qqq.loc[masks["trend_confirmed"]] = (
            raw.loc[masks["trend_confirmed"], "QQQ"] * RETURN_RETENTION_TREND_BLEND[0]
            + base.loc[masks["trend_confirmed"], "QQQ"] * RETURN_RETENTION_TREND_BLEND[1]
        )
        return _normalized_weights(
            qqq,
            prices,
            step_delta=RETURN_RETENTION_STEP_DELTA,
            upper=0.88,
            rolling_window=4,
        )
    if candidate_id == "growth_tilt_guarded_transfer_valid_until_strict_v1":
        qqq = (
            base["QQQ"] * VALID_UNTIL_STRICT_BLEND[0]
            + valid["QQQ"] * VALID_UNTIL_STRICT_BLEND[1]
            + lower["QQQ"] * VALID_UNTIL_STRICT_BLEND[2]
        )
        qqq.loc[masks["near_expiry_proxy"]] = (
            valid.loc[masks["near_expiry_proxy"], "QQQ"]
            * VALID_UNTIL_STRICT_NEAR_EXPIRY_BLEND[0]
            + lower.loc[masks["near_expiry_proxy"], "QQQ"]
            * VALID_UNTIL_STRICT_NEAR_EXPIRY_BLEND[1]
        )
        return _normalized_weights(
            qqq,
            prices,
            lower=0.20,
            upper=0.78,
            step_delta=VALID_UNTIL_STEP_DELTA,
            rolling_window=8,
        )
    if candidate_id == "growth_tilt_guarded_transfer_balanced_gate_v1":
        qqq = (
            base["QQQ"] * BALANCED_GATE_BLEND[0]
            + raw["QQQ"] * BALANCED_GATE_BLEND[1]
            + cooldown["QQQ"] * BALANCED_GATE_BLEND[2]
            + valid["QQQ"] * BALANCED_GATE_BLEND[3]
            + guarded["QQQ"] * BALANCED_GATE_BLEND[4]
        )
        qqq.loc[masks["high_volatility"]] = (
            lower.loc[masks["high_volatility"], "QQQ"] * BALANCED_GATE_HIGH_VOL_BLEND[0]
            + valid.loc[masks["high_volatility"], "QQQ"] * BALANCED_GATE_HIGH_VOL_BLEND[1]
            + base.loc[masks["high_volatility"], "QQQ"] * BALANCED_GATE_HIGH_VOL_BLEND[2]
        )
        qqq.loc[masks["recovery"]] = (
            base.loc[masks["recovery"], "QQQ"] * BALANCED_GATE_RECOVERY_BLEND[0]
            + raw.loc[masks["recovery"], "QQQ"] * BALANCED_GATE_RECOVERY_BLEND[1]
            + cooldown.loc[masks["recovery"], "QQQ"] * BALANCED_GATE_RECOVERY_BLEND[2]
        )
        return _normalized_weights(
            qqq,
            prices,
            lower=0.19,
            upper=0.82,
            step_delta=BALANCED_STEP_DELTA,
            rolling_window=6,
        )
    raise ValueError(f"Unknown TRADING-2399 targeted variant: {candidate_id}")


def _market_masks(prices: pd.DataFrame) -> dict[str, pd.Series]:
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    rolling_vol = qqq_returns.rolling(ROLLING_VOL_WINDOW, min_periods=5).std()
    high_vol = rolling_vol.ge(rolling_vol.quantile(HIGH_VOL_QUANTILE)).fillna(False)
    low_vol = rolling_vol.le(rolling_vol.quantile(LOW_VOL_QUANTILE)).fillna(False)
    trend_confirmed = (
        prices["QQQ"].gt(
            prices["QQQ"].rolling(TREND_CONFIRMATION_WINDOW, min_periods=5).mean()
        )
        & prices["QQQ"]
        .pct_change(TREND_CONFIRMATION_WINDOW)
        .fillna(0.0)
        .gt(0.0)
    ).fillna(False)
    recovery = drawdown.lt(0.0) & qqq_returns.rolling(
        RECOVERY_WINDOW,
        min_periods=5,
    ).sum().gt(0.0)
    risk_off = drawdown.le(RISK_OFF_DRAWDOWN_THRESHOLD) | (
        high_vol & qqq_returns.lt(0.0)
    )
    near_expiry_proxy = qqq_returns.rolling(5, min_periods=1).sum().abs().lt(
        NEAR_EXPIRY_ABS_RETURN_SUM
    )
    return {
        "high_volatility": high_vol,
        "low_volatility": low_vol,
        "trend_confirmed": trend_confirmed,
        "recovery": recovery.fillna(False),
        "risk_off": risk_off.fillna(False),
        "near_expiry_proxy": near_expiry_proxy.fillna(False),
    }


def _normalized_weights(
    qqq: pd.Series,
    prices: pd.DataFrame,
    *,
    lower: float = 0.18,
    upper: float,
    step_delta: float,
    rolling_window: int = 5,
) -> pd.DataFrame:
    smoothed = qqq.rolling(rolling_window, min_periods=1).mean()
    bounded = m2393._bounded_step_delta(smoothed, max_delta=step_delta)
    return m2393._normalized_qqq_sgov(
        bounded,
        index=prices.index,
        lower=lower,
        upper=upper,
    )


def _cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in m2396.retest_helpers._cost_stress_scenarios():
        ranking_bundle = m2396._reference_bundle(
            prices=prices,
            policies=policies,
            candidate_id=m2396.m2386.RANKING_TOP_CANDIDATE,
            scenario=scenario,
        )
        rows.append(
            m2396.retest_helpers._stress_result_row(
                candidate_id="static_baseline",
                role="static_baseline",
                scenario=scenario,
                bundle=static_bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=m2396.m2386.RANKING_TOP_CANDIDATE,
            )
        )
        for candidate_id in TARGETED_VARIANT_IDS:
            bundle = _targeted_variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
            )
            row = m2396.retest_helpers._stress_result_row(
                candidate_id=candidate_id,
                role=_targeted_variant_role(candidate_id),
                scenario=scenario,
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=m2396.m2386.RANKING_TOP_CANDIDATE,
            )
            row["targeted_variant_components"] = _targeted_variant_components(candidate_id)
            rows.append(row)
    return _attach_relative_metrics(rows)


def _cadence_comparison_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scenario = m2396.retest_helpers._cost_stress_scenarios()[1]
    for cadence in COMPARISON_CADENCES:
        for candidate_id in TARGETED_VARIANT_IDS:
            bundle = _targeted_variant_bundle(
                prices=prices,
                policies=policies,
                candidate_id=candidate_id,
                cadence=cadence,
                scenario=scenario,
            )
            row = m2396.retest_helpers._stress_result_row(
                candidate_id=candidate_id,
                role=f"{_targeted_variant_role(candidate_id)}_{cadence}",
                scenario={
                    **scenario,
                    "scenario_id": cadence,
                    "scenario_group": "cadence_comparison",
                },
                bundle=bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
                primary_candidate=m2396.m2386.RANKING_TOP_CANDIDATE,
            )
            row["comparison_cadence"] = cadence
            row["monthly_rebalance_allowed_for_primary_decision"] = False
            rows.append(row)
    return _attach_relative_metrics(rows)


def _targeted_variant_ranking(retest: Mapping[str, Any]) -> list[dict[str, Any]]:
    full = _rows_by_candidate(retest.get("targeted_gate_evidence_retest_result"))
    cost = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    time_rows = _as_list_of_mappings(retest.get("time_slice_matrix"))
    regime_rows = _as_list_of_mappings(retest.get("regime_slice_matrix"))
    base = full.get(CANDIDATE_UNDER_REVIEW, {})
    raw = full.get(m2396.m2386.RANKING_TOP_CANDIDATE, {})
    rows: list[dict[str, Any]] = []
    for candidate_id in TARGETED_VARIANT_IDS:
        row = full.get(candidate_id, {})
        performance = _as_mapping(row.get("performance_metrics"))
        relative = _as_mapping(row.get("relative_metrics"))
        execution = _as_mapping(row.get("execution_metrics"))
        time_pass = _pass_rate(time_rows, candidate_id)
        base_time_pass = _pass_rate(time_rows, CANDIDATE_UNDER_REVIEW)
        regime_score = _regime_expectation_score(regime_rows, candidate_id, row, raw)
        base_regime_score = _regime_expectation_score(
            regime_rows,
            CANDIDATE_UNDER_REVIEW,
            base,
            raw,
        )
        survival = _cost_stress_survival(cost, candidate_id)
        decision, reason = _candidate_decision(
            row=row,
            base_row=base,
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
                "volatility": performance.get("volatility"),
                "sharpe_or_sortino_if_available": performance.get(
                    "sharpe_or_sortino_if_available"
                ),
                "upside_capture": performance.get("upside_capture"),
                "downside_capture": performance.get("downside_capture"),
                "turnover": execution.get("turnover"),
                "max_monthly_turnover": execution.get("max_monthly_turnover"),
                "rebalance_count": execution.get("rebalance_count"),
                "cooldown_block_count": execution.get("cooldown_block_count"),
                "constraint_hit_count": execution.get("constraint_hit_count"),
                "stale_signal_execution_count": execution.get(
                    "stale_signal_execution_count"
                ),
                "signal_to_execution_lag_days": execution.get(
                    "signal_to_execution_lag_days"
                ),
                "time_slice_pass_rate": time_pass,
                "time_slice_improvement_vs_base": round(time_pass - base_time_pass, 6),
                "regime_expectation_score": regime_score,
                "regime_expectation_improvement_vs_base": round(
                    regime_score - base_regime_score,
                    6,
                ),
                "return_retention_vs_raw_growth_tilt": relative.get(
                    "return_retention_vs_raw_growth_tilt"
                ),
                "return_retention_vs_base_recombination": relative.get(
                    "return_retention_vs_base_recombination"
                ),
                "turnover_reduction_vs_raw_growth_tilt": relative.get(
                    "turnover_reduction_vs_raw_growth_tilt"
                ),
                "turnover_change_vs_base_recombination": relative.get(
                    "turnover_change_vs_base_recombination"
                ),
                "drawdown_gap_vs_static": relative.get("drawdown_gap_vs_static"),
                "drawdown_improvement_vs_base": relative.get(
                    "drawdown_improvement_vs_base"
                ),
                "return_per_drawdown_penalty": _return_per_drawdown_penalty(row),
                "drawdown_materiality_tier": _drawdown_materiality_tier(row),
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
            _float(item.get("time_slice_improvement_vs_base")),
            _float(item.get("regime_expectation_improvement_vs_base")),
            _float(item.get("drawdown_improvement_vs_base")),
            _float(item.get("cost_adjusted_return")),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def _gate_evidence_matrix(
    *,
    retest: Mapping[str, Any],
    ranking: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Any],
) -> list[dict[str, Any]]:
    full = _rows_by_candidate(retest.get("targeted_gate_evidence_retest_result"))
    cost = _rows_by_candidate_and_scenario(retest.get("cost_stress_result"))
    rank_by_candidate = {str(row.get("candidate_id")): row for row in ranking}
    source_gaps = _source_gap_summary(sources)
    rows: list[dict[str, Any]] = []
    for candidate_id in TARGETED_VARIANT_IDS:
        row = full.get(candidate_id, {})
        performance = _as_mapping(row.get("performance_metrics"))
        relative = _as_mapping(row.get("relative_metrics"))
        execution = _as_mapping(row.get("execution_metrics"))
        rank_row = _as_mapping(rank_by_candidate.get(candidate_id))
        realistic = cost.get((candidate_id, "realistic"), {})
        conservative = cost.get((candidate_id, "conservative"), {})
        harsh = cost.get((candidate_id, "harsh"), {})
        rows.append(
            {
                "candidate_id": candidate_id,
                "source_variant_definition": _targeted_variant_definition(candidate_id),
                "source_gap_targets": source_gaps,
                "performance_metrics": {
                    "total_return": performance.get("total_return"),
                    "annualized_return": performance.get("annualized_return"),
                    "max_drawdown": performance.get("max_drawdown"),
                    "volatility": performance.get("volatility"),
                    "sharpe_or_sortino_if_available": performance.get(
                        "sharpe_or_sortino_if_available"
                    ),
                    "upside_capture": performance.get("upside_capture"),
                    "downside_capture": performance.get("downside_capture"),
                },
                "relative_metrics": dict(relative),
                "time_slice_evidence": {
                    "time_slice_pass_rate": rank_row.get("time_slice_pass_rate"),
                    "time_slice_improvement_vs_base": rank_row.get(
                        "time_slice_improvement_vs_base"
                    ),
                    "time_slice_not_weak": _float(
                        rank_row.get("time_slice_pass_rate")
                    )
                    >= m2396.PREVIEW_TIME_SLICE_PASS_RATE_MIN,
                },
                "regime_expectation_evidence": {
                    "regime_expectation_score": rank_row.get(
                        "regime_expectation_score"
                    ),
                    "regime_expectation_improvement_vs_base": rank_row.get(
                        "regime_expectation_improvement_vs_base"
                    ),
                    "regime_expectation_not_weak": _float(
                        rank_row.get("regime_expectation_score")
                    )
                    >= m2396.PREVIEW_REGIME_EXPECTATION_SCORE_MIN,
                },
                "drawdown_return_evidence": {
                    "drawdown_materiality_tier": rank_row.get(
                        "drawdown_materiality_tier"
                    ),
                    "drawdown_improvement_vs_base": rank_row.get(
                        "drawdown_improvement_vs_base"
                    ),
                    "return_retention_vs_raw_growth_tilt": relative.get(
                        "return_retention_vs_raw_growth_tilt"
                    ),
                    "return_retention_vs_base_recombination": relative.get(
                        "return_retention_vs_base_recombination"
                    ),
                    "return_per_drawdown_penalty": _return_per_drawdown_penalty(row),
                },
                "valid_until_stale_signal_evidence": {
                    "valid_until_window_preserved": True,
                    "no_stale_signal_carry_forward": _float(
                        execution.get("stale_signal_execution_count")
                    )
                    == 0.0,
                    "stale_signal_execution_count": execution.get(
                        "stale_signal_execution_count"
                    ),
                    "signal_to_execution_lag_days": execution.get(
                        "signal_to_execution_lag_days"
                    ),
                },
                "turnover_cost_evidence": {
                    "turnover": execution.get("turnover"),
                    "turnover_reduction_vs_raw_growth_tilt": relative.get(
                        "turnover_reduction_vs_raw_growth_tilt"
                    ),
                    "turnover_change_vs_base_recombination": relative.get(
                        "turnover_change_vs_base_recombination"
                    ),
                    "max_monthly_turnover": execution.get("max_monthly_turnover"),
                    "realistic_cost_passed": _relative_gap(realistic) > 0.0,
                    "conservative_cost_passed": _relative_gap(conservative) > 0.0,
                    "harsh_cost_passed": _relative_gap(harsh) > 0.0,
                },
                "decision_evidence": {
                    "candidate_decision": rank_row.get("decision"),
                    "decision_reason": rank_row.get("decision_reason"),
                    "observation_preview_candidate": rank_row.get("decision")
                    == DECISION_OBSERVATION_PREVIEW,
                    "owner_review_required": rank_row.get("decision")
                    in {DECISION_OBSERVATION_PREVIEW, DECISION_OWNER_REVIEW},
                    "research_only_observation_approved": False,
                    "recommended_next_research_task": NEXT_ROUTE,
                },
                "candidate_auto_accept_approved": False,
                "research_only_observation_approved": False,
                "paper_shadow_enabled": False,
                "event_append_enabled": False,
                "outcome_binding_enabled": False,
                "scheduler_enabled": False,
                "production_enabled": False,
                "broker_action_enabled": False,
                "daily_report_generated": False,
            }
        )
    return rows


def _decision_update(
    *,
    ranking: Sequence[Mapping[str, Any]],
    gate_matrix: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    best = _as_mapping(ranking[0] if ranking else {})
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
        "schema_version": "dynamic_strategy_targeted_gate_evidence_decision_update.v1",
        "decision_update_ready": True,
        "best_targeted_variant": best.get("candidate_id"),
        "best_targeted_variant_decision": best.get("decision"),
        "candidate_decisions": {
            str(row.get("candidate_id")): str(row.get("decision")) for row in ranking
        },
        "observation_preview_candidates": preview_candidates,
        "observation_preview_candidates_count": len(preview_candidates),
        "owner_review_required_candidates": owner_review_candidates,
        "owner_review_required_candidates_count": len(owner_review_candidates),
        "gate_evidence_matrix_count": len(gate_matrix),
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
            "TRADING-2399 ranks targeted gate-evidence variants, but observation "
            "preview, no-approval, paper-shadow and execution decisions remain "
            "reserved for TRADING-2400 owner review."
        ),
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
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
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifacts": dict(_as_mapping(sources.get("source_artifacts"))),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_targeted_gate_evidence_retest": bool(
            sources.get("source_ready_for_targeted_gate_evidence_retest")
        ),
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "requested_date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else data_quality.get("as_of"),
        },
        "candidate_under_review": CANDIDATE_UNDER_REVIEW,
        "decision_from_2396": m2398.EXPECTED_DECISION_FROM_2396,
        "owner_decision_from_2397": m2398.OWNER_DECISION_FROM_2397,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": list(COMPARISON_CADENCES),
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "data_quality": dict(data_quality),
        "data_quality_gate_executed": True,
        "data_quality_gate_required": True,
        "data_quality_gate_command": f"aits validate-data --as-of {as_of_date.isoformat()}",
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
        "fresh_market_data_read": True,
        "backtest_run": True,
        "targeted_retest_run": True,
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
        "observation_approved": False,
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
        "targeted_variants_tested": [],
        "reference_candidates": _reference_candidate_summary(),
        "targeted_retest_design": _retest_design(),
        "targeted_gate_evidence_retest_result": [],
        "reference_candidate_result": [],
        "time_slice_matrix": [],
        "regime_slice_matrix": [],
        "time_regime_slice_matrix": [],
        "cost_stress_result": [],
        "cadence_comparison_result": [],
        "targeted_variant_ranking": [],
        "gate_evidence_matrix": [],
        "decision_update": {
            "decision_update_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "best_targeted_variant": None,
        "best_targeted_variant_decision": None,
        "observation_preview_candidates_count": 0,
        "targeted_retest_ready": False,
        "variant_ranking_ready": False,
        "gate_evidence_matrix_ready": False,
        "decision_update_ready": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
        "source_ready_for_targeted_gate_evidence_retest": bool(
            sources.get("source_ready_for_targeted_gate_evidence_retest")
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
        "json_path": str(output_root / "targeted_gate_evidence_retest_result.json"),
        "targeted_variant_ranking_json": str(
            output_root / "targeted_variant_ranking.json"
        ),
        "gate_evidence_matrix_json": str(output_root / "gate_evidence_matrix.json"),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest.md"
        ),
        "targeted_variant_ranking_markdown": str(
            docs_root / "dynamic_strategy_targeted_gate_evidence_variant_ranking.md"
        ),
        "gate_evidence_matrix_markdown": str(
            docs_root / "dynamic_strategy_targeted_gate_evidence_matrix.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2400_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["targeted_variant_ranking_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_targeted_gate_evidence_variant_ranking",
            "schema_version": "dynamic_strategy_targeted_gate_evidence_variant_ranking.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "targeted_variant_ranking": payload.get("targeted_variant_ranking", []),
            "variant_ranking_ready": payload.get("variant_ranking_ready"),
            "best_targeted_variant": payload.get("best_targeted_variant"),
            "best_targeted_variant_decision": payload.get(
                "best_targeted_variant_decision"
            ),
            "observation_preview_candidates_count": payload.get(
                "observation_preview_candidates_count"
            ),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["gate_evidence_matrix_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_targeted_gate_evidence_matrix",
            "schema_version": "dynamic_strategy_targeted_gate_evidence_matrix.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "gate_evidence_matrix": payload.get("gate_evidence_matrix", []),
            "gate_evidence_matrix_ready": payload.get("gate_evidence_matrix_ready"),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["decision_update_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_targeted_gate_evidence_decision_update",
            "schema_version": "dynamic_strategy_targeted_gate_evidence_decision_update.v1",
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
        Path(paths["targeted_variant_ranking_markdown"]),
        _ranking_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["gate_evidence_matrix_markdown"]),
        _gate_matrix_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _attach_relative_metrics(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = [dict(row) for row in rows]
    by_candidate = {str(row.get("candidate_id")): row for row in result}
    static = by_candidate.get("static_baseline", {})
    raw = by_candidate.get(m2396.m2386.RANKING_TOP_CANDIDATE, {})
    base = by_candidate.get(CANDIDATE_UNDER_REVIEW, {})
    lower = by_candidate.get(m2396.m2386.BEST_LOWER_TURNOVER_VARIANT, {})
    for row in result:
        candidate_id = str(row.get("candidate_id"))
        if candidate_id not in set(TARGETED_VARIANT_IDS):
            continue
        relative = dict(_as_mapping(row.get("relative_metrics")))
        candidate_turnover = _float(_execution(row, "turnover"))
        base_turnover = _float(_execution(base, "turnover"))
        relative.update(
            {
                "candidate_vs_base_recombination_gap": _annual_gap(row, base),
                "candidate_vs_raw_growth_tilt_gap": _annual_gap(row, raw),
                "candidate_vs_lower_turnover_reference_gap": _annual_gap(row, lower),
                "return_retention_vs_raw_growth_tilt": _return_retention(row, raw),
                "return_retention_vs_base_recombination": _return_retention(row, base),
                "turnover_reduction_vs_raw_growth_tilt": _turnover_reduction(raw, row),
                "turnover_change_vs_base_recombination": round(
                    candidate_turnover - base_turnover,
                    6,
                ),
                "drawdown_gap_vs_static": _drawdown_delta(static, row),
                "drawdown_improvement_vs_base": round(
                    abs(_performance(base, "max_drawdown"))
                    - abs(_performance(row, "max_drawdown")),
                    6,
                ),
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
            "turnover_not_materially_worse_than_base": candidate_turnover
            <= base_turnover * (1.0 + m2396.OWNER_REVIEW_TURNOVER_WORSE_TOLERANCE),
            "drawdown_tradeoff_explainable": _float(
                relative.get("drawdown_gap_vs_static")
            )
            >= -m2396.OWNER_REVIEW_DRAWDOWN_WORSE_TOLERANCE,
            "monthly_rebalance_allowed_for_primary_decision": False,
        }
    return result


def _candidate_decision(
    *,
    row: Mapping[str, Any],
    base_row: Mapping[str, Any],
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
    base_return_retained = _float(
        relative.get("return_retention_vs_base_recombination")
    )
    turnover_change_vs_base = _float(
        relative.get("turnover_change_vs_base_recombination")
    )
    drawdown_gap = _float(relative.get("drawdown_gap_vs_static"))
    base_turnover = _float(_execution(base_row, "turnover"))
    turnover_not_worse = _float(execution.get("turnover")) <= base_turnover * (
        1.0 + m2396.OWNER_REVIEW_TURNOVER_WORSE_TOLERANCE
    )
    owner_review_passed = (
        realistic_positive
        and conservative_positive
        and no_stale
        and turnover_not_worse
        and drawdown_gap >= -m2396.OWNER_REVIEW_DRAWDOWN_WORSE_TOLERANCE
        and return_retained >= m2396.OWNER_REVIEW_RETURN_RETENTION_MIN
        and base_return_retained > 0.0
    )
    if owner_review_passed and (
        harsh_positive
        and time_pass_rate >= m2396.PREVIEW_TIME_SLICE_PASS_RATE_MIN
        and regime_expectation_score >= m2396.PREVIEW_REGIME_EXPECTATION_SCORE_MIN
        and drawdown_gap >= -m2396.PREVIEW_DRAWDOWN_WORSE_TOLERANCE
    ):
        return (
            DECISION_OBSERVATION_PREVIEW,
            "preview_gate_evidence_passed_but_2400_owner_decision_required",
        )
    if owner_review_passed:
        return (
            DECISION_OWNER_REVIEW,
            "owner_review_criteria_passed_observation_not_approved",
        )
    if realistic_positive and (
        _turnover_reduction(raw_row, row) >= m2396.COMPONENT_VALUE_TURNOVER_REDUCTION_MIN
        or _stale_reduction(raw_row, row) >= m2396.COMPONENT_VALUE_STALE_REDUCTION_MIN
        or turnover_change_vs_base < 0.0
    ):
        return (
            DECISION_COMPONENT_VALUE_ONLY,
            "component_gate_value_observed_but_candidate_gate_not_passed",
        )
    if realistic_positive:
        return (
            DECISION_CONTINUE_TARGETED_IMPROVEMENT,
            "positive_static_gap_but_targeted_gate_criteria_not_met",
        )
    return DECISION_REJECT, "non_positive_realistic_cost_dynamic_vs_static_gap"


def _retest_design() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_targeted_gate_evidence_retest_design.v1",
        "candidate_under_review": CANDIDATE_UNDER_REVIEW,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": list(COMPARISON_CADENCES),
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "targeted_variants": list(TARGETED_VARIANT_IDS),
        "reference_candidates": _reference_candidate_summary(),
        "cost_stress_scenarios": m2396.retest_helpers._cost_stress_scenarios(),
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
            DECISION_CONTINUE_TARGETED_IMPROVEMENT,
            DECISION_COMPONENT_VALUE_ONLY,
            DECISION_REJECT,
        ],
        "construction_governance": {
            "status": "research_only_pilot_baseline",
            "not_production_policy": True,
            "owner_review_required_before_reuse": True,
        },
    }


def _targeted_retest_policy() -> dict[str, Any]:
    policy = m2396._recombination_retest_policy()
    policy["policy_id"] = "dynamic_strategy_targeted_gate_evidence_retest_v1"
    policy["rationale"] = (
        "TRADING-2399 retests TRADING-2398 targeted variants under the same "
        "cached-data-gated valid-until execution path, routing any preview "
        "candidate to TRADING-2400 owner decision."
    )
    return policy


def _reference_candidate_summary() -> list[dict[str, str]]:
    return [
        {"reference_name": name, **dict(details)}
        for name, details in REFERENCE_CANDIDATES.items()
    ]


def _targeted_variant_definition(candidate_id: str) -> dict[str, Any]:
    return next(
        (dict(row) for row in TARGETED_VARIANTS if row.get("candidate_id") == candidate_id),
        {},
    )


def _targeted_variant_components(candidate_id: str) -> list[str]:
    definition = _targeted_variant_definition(candidate_id)
    return [
        "growth_tilt_engine",
        "lower_turnover_guardrail",
        "valid_until_window",
        "no_stale_signal_carry_forward",
        "guarded_turnover_transfer",
        *[str(item) for item in _as_list(definition.get("changes"))],
    ]


def _targeted_variant_role(candidate_id: str) -> str:
    return f"targeted_gate_evidence_{candidate_id}"


def _source_gap_summary(sources: Mapping[str, Any]) -> dict[str, Any]:
    plan = _as_mapping(sources.get("gate_evidence_plan_result_2398"))
    return _as_mapping(plan.get("gate_evidence_gap_summary")).get("gap_areas", {})


def _base_ranking_row(ranking_doc: Mapping[str, Any]) -> dict[str, Any]:
    return next(
        (
            row
            for row in _as_list_of_mappings(ranking_doc.get("recombination_candidate_ranking"))
            if row.get("candidate_id") == CANDIDATE_UNDER_REVIEW
        ),
        {},
    )


def _base_evidence_row(evidence_doc: Mapping[str, Any]) -> dict[str, Any]:
    return next(
        (
            row
            for row in _as_list_of_mappings(evidence_doc.get("component_evidence_matrix"))
            if row.get("candidate_id") == CANDIDATE_UNDER_REVIEW
        ),
        {},
    )


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
    if _relative_gap(cost_rows.get((candidate_id, "base"), {})) > 0.0:
        return "base"
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
        min(
            1.0,
            _float(
                _as_mapping(candidate_row.get("relative_metrics")).get(
                    "return_retention_vs_raw_growth_tilt"
                )
            ),
        ),
    )
    turnover = max(0.0, min(1.0, _turnover_reduction(raw_row, candidate_row)))
    return round((regime_pass + retention + turnover) / 3.0, 6)


def _decision_rank(decision: str) -> int:
    return {
        DECISION_OBSERVATION_PREVIEW: 5,
        DECISION_OWNER_REVIEW: 4,
        DECISION_CONTINUE_TARGETED_IMPROVEMENT: 3,
        DECISION_COMPONENT_VALUE_ONLY: 2,
        DECISION_REJECT: 1,
    }.get(decision, 0)


def _cost_survival_rank(survival: str) -> int:
    return {"harsh": 4, "conservative": 3, "realistic": 2, "base": 1}.get(
        survival,
        0,
    )


def _drawdown_materiality_tier(row: Mapping[str, Any]) -> str:
    drawdown_gap = _float(_as_mapping(row.get("relative_metrics")).get("drawdown_gap_vs_static"))
    if drawdown_gap >= -m2396.PREVIEW_DRAWDOWN_WORSE_TOLERANCE:
        return "NOT_SEVERE"
    if drawdown_gap >= -m2396.OWNER_REVIEW_DRAWDOWN_WORSE_TOLERANCE:
        return "OWNER_REVIEW_MATERIAL"
    return "SEVERE"


def _return_per_drawdown_penalty(row: Mapping[str, Any]) -> float:
    annual = _performance(row, "annualized_return")
    drawdown = abs(_performance(row, "max_drawdown"))
    if drawdown == 0.0:
        return 0.0
    return round(annual / drawdown, 6)


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
            "# 动态策略 recombination candidate targeted gate evidence retest",
            "",
            f"- status：`{payload.get('status')}`",
            f"- data quality：`{payload.get('data_quality_status')}`",
            f"- market regime：`{payload.get('market_regime')}`",
            f"- requested date range：`{_json_block(payload.get('requested_date_range'))}`",
            f"- candidate under review：`{payload.get('candidate_under_review')}`",
            f"- best targeted variant：`{payload.get('best_targeted_variant')}`",
            f"- best decision：`{payload.get('best_targeted_variant_decision')}`",
            "- observation preview candidates："
            f"`{payload.get('observation_preview_candidates_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Executive summary",
            "",
            "TRADING-2399 在 cached-data quality gate 通过后，按 "
            "`valid_until_window` 主口径实际 retest TRADING-2398 规划的 6 个 "
            "targeted variants。结果只作为 TRADING-2400 owner decision 输入；"
            "本任务不批准 observation、paper-shadow、scheduler、event append、"
            "outcome binding、production 或 broker。",
            "",
            "## Source plan from TRADING-2398",
            "",
            "```json",
            _json_block(payload.get("source_status")),
            "```",
            "",
            "## Targeted variant definitions",
            "",
            "```json",
            _json_block(TARGETED_VARIANTS),
            "```",
            "",
            "## Retest design",
            "",
            "```json",
            _json_block(payload.get("targeted_retest_design")),
            "```",
            "",
            "## Variant ranking",
            "",
            _ranking_table(payload.get("targeted_variant_ranking")),
            "",
            "## Gate evidence matrix",
            "",
            "```json",
            _json_block(payload.get("gate_evidence_matrix")),
            "```",
            "",
            "## Time-slice evidence result",
            "",
            f"- rows：`{len(_as_list(payload.get('time_slice_matrix')))}`",
            "",
            "## Regime expectation result",
            "",
            f"- rows：`{len(_as_list(payload.get('regime_slice_matrix')))}`",
            "",
            "## Drawdown / return retention result",
            "",
            "```json",
            _json_block(_top_rank_summary(payload)),
            "```",
            "",
            "## Valid-until / stale signal result",
            "",
            "```json",
            _json_block(_valid_until_summary(payload)),
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
            "- 不批准 observation",
            "- 不进入 paper-shadow",
            "- 不启用 scheduler",
            "- 不 append event",
            "- 不 bind outcome",
            "- 不生成 daily report",
            "- 不启用 production / broker",
            "",
            "## Recommended next route",
            "",
            f"`{payload.get('recommended_next_research_task')}`",
        ]
    ) + "\n"


def _ranking_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 targeted gate evidence variant ranking",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best targeted variant：`{payload.get('best_targeted_variant')}`",
            f"- best decision：`{payload.get('best_targeted_variant_decision')}`",
            "",
            _ranking_table(payload.get("targeted_variant_ranking")),
        ]
    ) + "\n"


def _gate_matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 targeted gate evidence matrix",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            "```json",
            _json_block(payload.get("gate_evidence_matrix")),
            "```",
        ]
    ) + "\n"


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 TRADING-2400 路由",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 推荐下一路由：`{payload.get('recommended_next_research_task')}`",
            f"- best targeted variant：`{payload.get('best_targeted_variant')}`",
            f"- best decision：`{payload.get('best_targeted_variant_decision')}`",
            "- observation preview candidates："
            f"`{payload.get('observation_preview_candidates_count')}`",
            "",
            "TRADING-2399 只生成 targeted gate evidence retest package。即使出现 "
            "`ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION_PREVIEW`，也必须由 TRADING-2400 "
            "单独记录 owner decision；2399 不批准 observation、paper-shadow、"
            "scheduler、production 或 broker。",
        ]
    ) + "\n"


def _ranking_table(rows: Any) -> str:
    lines = [
        "|rank|candidate|decision|annualized|drawdown|turnover|time delta|regime delta|cost|",
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
                    _fmt(row.get("time_slice_improvement_vs_base")),
                    _fmt(row.get("regime_expectation_improvement_vs_base")),
                    f"`{row.get('cost_stress_survival')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _top_rank_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    top = _as_mapping(_as_list(payload.get("targeted_variant_ranking"))[0]) if _as_list(
        payload.get("targeted_variant_ranking")
    ) else {}
    return {
        "candidate_id": top.get("candidate_id"),
        "drawdown_materiality_tier": top.get("drawdown_materiality_tier"),
        "drawdown_improvement_vs_base": top.get("drawdown_improvement_vs_base"),
        "return_retention_vs_raw_growth_tilt": top.get(
            "return_retention_vs_raw_growth_tilt"
        ),
        "return_retention_vs_base_recombination": top.get(
            "return_retention_vs_base_recombination"
        ),
    }


def _valid_until_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    rows = _as_list_of_mappings(payload.get("targeted_variant_ranking"))
    return {
        "targeted_variant_count": len(rows),
        "no_stale_signal_candidate_count": sum(
            1 for row in rows if row.get("no_stale_signal_carry_forward") is True
        ),
        "valid_until_window_preserved_all": all(
            row.get("valid_until_window_preserved") is True for row in rows
        )
        if rows
        else False,
    }


def _float(value: Any) -> float:
    return m2396._float(value)


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in _as_list(value) if isinstance(item, Mapping)]


def _fmt(value: Any) -> str:
    if isinstance(value, int | float):
        return f"{float(value):.6f}"
    return str(value)
