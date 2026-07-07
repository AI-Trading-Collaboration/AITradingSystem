from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as m2396
import ai_trading_system.dynamic_strategy_recombination_candidate_gate_evidence_plan as m2398
import ai_trading_system.dynamic_strategy_recombination_candidate_owner_review_decision as m2397
from ai_trading_system import (
    dynamic_strategy_recombination_candidate_targeted_gate_evidence_retest as m2399,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2400"
TASK_REGISTER_ID = (
    "TRADING-2400_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_AND_"
    "OBSERVATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_targeted_gate_evidence_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_targeted_gate_evidence_owner_review_and_observation_"
    "decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_AND_OBSERVATION_"
    "DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_AND_OBSERVATION_"
    "DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_RETAIN_TARGETED_IMPROVEMENT_VALUE_AND_REQUIRE_"
    "PLATEAU_REVIEW"
)
NEXT_ROUTE = (
    "TRADING-2401_Dynamic_Strategy_Recombination_Line_Plateau_And_Data_Signal_"
    "Quality_Decision"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2396",
    "TRADING-2397",
    "TRADING-2398",
    "TRADING-2399",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA"
)

BASE_CANDIDATE = m2399.CANDIDATE_UNDER_REVIEW
BEST_TARGETED_VARIANT = "growth_tilt_guarded_transfer_valid_until_strict_v1"
EXPECTED_DECISION_FROM_2399 = m2399.DECISION_CONTINUE_TARGETED_IMPROVEMENT
EXPECTED_OBSERVATION_PREVIEW_COUNT = 0
TARGETED_IMPROVEMENT_VALUE_TYPES: tuple[str, ...] = (
    "valid_until_strictness",
    "signal_expiry_discipline",
    "stale_signal_guardrail",
    "guardrail_quality",
)
DEFAULT_NEXT_DIRECTION_OPTION = (
    "OPTION_B_PAUSE_RECOMBINATION_CANDIDATE_LINE_AND_REVIEW_DATA_SIGNAL_QUALITY"
)

OBSERVATION_NON_APPROVAL_REASONS: tuple[str, ...] = (
    "NO_OBSERVATION_PREVIEW_CANDIDATE_FOUND",
    "BEST_TARGETED_VARIANT_REMAINS_CONTINUE_TARGETED_IMPROVEMENT",
    "OWNER_REVIEW_REQUIRED_THRESHOLD_NOT_REACHED_AFTER_TARGETED_RETEST",
    "GATE_EVIDENCE_REMAINS_INSUFFICIENT",
    "PAPER_SHADOW_AND_EXECUTION_PATHS_REMAIN_OUT_OF_SCOPE",
)
EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "candidate_auto_accept",
    "research_only_observation",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "scheduled_task",
    "daily_report",
    "production",
    "broker_order",
    "new_backtest",
    "new_signal",
    "new_scoring",
)

DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2399_TARGETED_RETEST_RESULT_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "targeted_gate_evidence_retest_result.json"
)
DEFAULT_SOURCE_2399_TARGETED_VARIANT_RANKING_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "targeted_variant_ranking.json"
)
DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "gate_evidence_matrix.json"
)
DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH = (
    m2399.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "gate_evidence_plan_result.json"
)
DEFAULT_SOURCE_2398_TARGETED_IMPROVEMENT_PLAN_PATH = (
    m2398.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    / "targeted_improvement_plan.json"
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
DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH = (
    m2396.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_OUTPUT_ROOT
    / "decision_update.json"
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


def run_dynamic_strategy_targeted_gate_evidence_owner_review_decision(
    *,
    source_targeted_retest_result_2399_path: Path = (
        DEFAULT_SOURCE_2399_TARGETED_RETEST_RESULT_PATH
    ),
    source_targeted_variant_ranking_2399_path: Path = (
        DEFAULT_SOURCE_2399_TARGETED_VARIANT_RANKING_PATH
    ),
    source_gate_evidence_matrix_2399_path: Path = (
        DEFAULT_SOURCE_2399_GATE_EVIDENCE_MATRIX_PATH
    ),
    source_decision_update_2399_path: Path = DEFAULT_SOURCE_2399_DECISION_UPDATE_PATH,
    source_gate_evidence_plan_result_2398_path: Path = (
        DEFAULT_SOURCE_2398_GATE_EVIDENCE_PLAN_RESULT_PATH
    ),
    source_targeted_improvement_plan_2398_path: Path = (
        DEFAULT_SOURCE_2398_TARGETED_IMPROVEMENT_PLAN_PATH
    ),
    source_next_route_2398_path: Path = DEFAULT_SOURCE_2398_NEXT_ROUTE_PATH,
    source_owner_review_decision_2397_path: Path = (
        DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH
    ),
    source_recombination_retest_result_2396_path: Path = (
        DEFAULT_SOURCE_2396_RECOMBINATION_RETEST_RESULT_PATH
    ),
    source_decision_update_2396_path: Path = DEFAULT_SOURCE_2396_DECISION_UPDATE_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_targeted_retest_result_2399_path=source_targeted_retest_result_2399_path,
        source_targeted_variant_ranking_2399_path=(
            source_targeted_variant_ranking_2399_path
        ),
        source_gate_evidence_matrix_2399_path=source_gate_evidence_matrix_2399_path,
        source_decision_update_2399_path=source_decision_update_2399_path,
        source_gate_evidence_plan_result_2398_path=(
            source_gate_evidence_plan_result_2398_path
        ),
        source_targeted_improvement_plan_2398_path=(
            source_targeted_improvement_plan_2398_path
        ),
        source_next_route_2398_path=source_next_route_2398_path,
        source_owner_review_decision_2397_path=source_owner_review_decision_2397_path,
        source_recombination_retest_result_2396_path=(
            source_recombination_retest_result_2396_path
        ),
        source_decision_update_2396_path=source_decision_update_2396_path,
    )
    ready = not sources["source_validation_errors"]
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=as_of_date,
        sources=sources,
    )
    payload.update(_ready_sections(sources) if ready else _blocked_sections(sources))
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(**paths: Path) -> dict[str, Any]:
    source_files = {
        key.removeprefix("source_").removesuffix("_path"): path
        for key, path in paths.items()
    }
    documents = {key: _load_json_document(path) for key, path in source_files.items()}
    sources: dict[str, Any] = {
        **documents,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": {
            key: _as_mapping(document).get("status")
            for key, document in documents.items()
        },
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    sources["source_ready_for_owner_review_decision"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "targeted_retest_result_2399": m2399.READY_STATUS,
        "targeted_variant_ranking_2399": m2399.READY_STATUS,
        "gate_evidence_matrix_2399": m2399.READY_STATUS,
        "decision_update_2399": m2399.READY_STATUS,
        "gate_evidence_plan_result_2398": m2398.READY_STATUS,
        "targeted_improvement_plan_2398": m2398.READY_STATUS,
        "next_route_2398": m2398.READY_STATUS,
        "owner_review_decision_2397": m2397.READY_STATUS,
        "recombination_retest_result_2396": m2396.READY_STATUS,
        "decision_update_2396": m2396.READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )

    _validate_2399_targeted_retest(sources, errors)
    _validate_2398_plan(sources, errors)
    _validate_2397_owner_decision(sources, errors)
    _validate_2396_context(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2399_targeted_retest(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("targeted_retest_result_2399"))
    ranking_doc = _as_mapping(sources.get("targeted_variant_ranking_2399"))
    decision_doc = _as_mapping(sources.get("decision_update_2399"))
    decision_update = _as_mapping(decision_doc.get("decision_update"))
    best_row = _best_variant_ranking_row(sources)
    matrix_row = _best_variant_matrix_row(sources)
    decision_evidence = _as_mapping(matrix_row.get("decision_evidence"))

    if retest.get("recommended_next_research_task") != m2399.NEXT_ROUTE:
        errors.append("2399 retest route did not point to TRADING-2400")
    if retest.get("candidate_under_review") != BASE_CANDIDATE:
        errors.append("2399 base candidate mismatch")
    if retest.get("best_targeted_variant") != BEST_TARGETED_VARIANT:
        errors.append("2399 best targeted variant mismatch")
    if retest.get("best_targeted_variant_decision") != EXPECTED_DECISION_FROM_2399:
        errors.append("2399 best targeted variant decision mismatch")
    if retest.get("observation_preview_candidates_count") != (
        EXPECTED_OBSERVATION_PREVIEW_COUNT
    ):
        errors.append("2399 observation preview count mismatch")
    for field in (
        "targeted_retest_ready",
        "variant_ranking_ready",
        "gate_evidence_matrix_ready",
        "decision_update_ready",
    ):
        if retest.get(field) is not True:
            errors.append(f"2399 retest missing ready flag {field}")
    if retest.get("data_quality_gate_executed") is not True:
        errors.append("2399 data quality gate was not executed")
    if retest.get("research_only_observation_approved") is True:
        errors.append("2399 unexpectedly approved research-only observation")

    if ranking_doc.get("best_targeted_variant") != BEST_TARGETED_VARIANT:
        errors.append("2399 ranking best targeted variant mismatch")
    if ranking_doc.get("best_targeted_variant_decision") != EXPECTED_DECISION_FROM_2399:
        errors.append("2399 ranking best targeted variant decision mismatch")
    if best_row.get("rank") != 1:
        errors.append("2399 best targeted variant is not rank 1")
    if best_row.get("decision") != EXPECTED_DECISION_FROM_2399:
        errors.append("2399 ranking row decision mismatch")
    if best_row.get("research_only_observation_approved") is True:
        errors.append("2399 ranking row unexpectedly approved observation")

    if decision_update.get("best_targeted_variant") != BEST_TARGETED_VARIANT:
        errors.append("2399 decision update best variant mismatch")
    if decision_update.get("best_targeted_variant_decision") != (
        EXPECTED_DECISION_FROM_2399
    ):
        errors.append("2399 decision update best decision mismatch")
    if decision_update.get("observation_preview_candidates_count") != (
        EXPECTED_OBSERVATION_PREVIEW_COUNT
    ):
        errors.append("2399 decision update observation preview count mismatch")
    if decision_update.get("research_only_observation_preview_exists") is True:
        errors.append("2399 unexpectedly found observation preview candidate")
    if decision_update.get("research_only_observation_approved") is True:
        errors.append("2399 decision update unexpectedly approved observation")
    if decision_update.get("recommended_next_research_task") != m2399.NEXT_ROUTE:
        errors.append("2399 decision update route mismatch")

    if matrix_row.get("candidate_id") != BEST_TARGETED_VARIANT:
        errors.append("2399 gate evidence matrix missing best variant")
    if decision_evidence.get("candidate_decision") != EXPECTED_DECISION_FROM_2399:
        errors.append("2399 gate evidence matrix decision mismatch")
    if decision_evidence.get("observation_preview_candidate") is True:
        errors.append("2399 gate evidence matrix unexpectedly marks observation preview")


def _validate_2398_plan(sources: Mapping[str, Any], errors: list[str]) -> None:
    plan = _as_mapping(sources.get("gate_evidence_plan_result_2398"))
    targeted_plan_doc = _as_mapping(sources.get("targeted_improvement_plan_2398"))
    next_route_doc = _as_mapping(sources.get("next_route_2398"))
    targeted_plan = _as_mapping(targeted_plan_doc.get("targeted_improvement_plan"))
    targeted_variants = _as_list_of_mappings(targeted_plan.get("targeted_variants"))
    targeted_variant_ids = {str(row.get("candidate_id")) for row in targeted_variants}
    if plan.get("candidate_under_review") != BASE_CANDIDATE:
        errors.append("2398 plan base candidate mismatch")
    if plan.get("recommended_next_research_task") != m2398.NEXT_ROUTE:
        errors.append("2398 plan route did not point to TRADING-2399")
    if set(_as_list(plan.get("planned_targeted_variants"))) != set(
        m2399.TARGETED_VARIANT_IDS
    ):
        errors.append("2398 planned targeted variants mismatch")
    if targeted_plan.get("record_ready") is not True:
        errors.append("2398 targeted improvement plan is not ready")
    if BEST_TARGETED_VARIANT not in targeted_variant_ids:
        errors.append("2398 targeted improvement plan missing best variant")
    if next_route_doc.get("recommended_next_research_task") != m2398.NEXT_ROUTE:
        errors.append("2398 next route artifact mismatch")


def _validate_2397_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2397"))
    if owner.get("owner_decision") != m2397.OWNER_DECISION:
        errors.append("2397 owner decision mismatch")
    if owner.get("best_recombination_candidate") != BASE_CANDIDATE:
        errors.append("2397 best recombination candidate mismatch")
    if owner.get("research_only_observation_approved") is True:
        errors.append("2397 unexpectedly approved research-only observation")
    if owner.get("recommended_next_research_task") != m2397.NEXT_ROUTE:
        errors.append("2397 route did not point to TRADING-2398")


def _validate_2396_context(sources: Mapping[str, Any], errors: list[str]) -> None:
    retest = _as_mapping(sources.get("recombination_retest_result_2396"))
    decision_doc = _as_mapping(sources.get("decision_update_2396"))
    decision_update = _as_mapping(decision_doc.get("decision_update"))
    if retest.get("best_recombination_candidate") != BASE_CANDIDATE:
        errors.append("2396 best recombination candidate mismatch")
    if retest.get("best_recombination_decision") != m2396.DECISION_OWNER_REVIEW:
        errors.append("2396 best recombination decision mismatch")
    if retest.get("recommended_next_research_task") != m2396.NEXT_ROUTE:
        errors.append("2396 route did not point to TRADING-2397")
    if retest.get("research_only_observation_approved") is True:
        errors.append("2396 unexpectedly approved research-only observation")
    if decision_update.get("best_recombination_candidate") != BASE_CANDIDATE:
        errors.append("2396 decision update best candidate mismatch")
    if decision_update.get("best_recombination_decision") != m2396.DECISION_OWNER_REVIEW:
        errors.append("2396 decision update best decision mismatch")


def _validate_source_safety(sources: Mapping[str, Any], errors: list[str]) -> None:
    for source_name in _source_document_names():
        document = _as_mapping(sources.get(source_name))
        if document.get("production_effect") not in (None, "none"):
            errors.append(f"{source_name}: production_effect must be none")
        if document.get("broker_action") not in (None, "none"):
            errors.append(f"{source_name}: broker_action must be none")
        for field in SAFETY_FALSE_FIELDS:
            if document.get(field) is True:
                errors.append(f"{source_name}: safety field must be false: {field}")


def _base_payload(
    *,
    status: str,
    as_of_date: date | None,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "as_of": (as_of_date or date.today()).isoformat(),
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_owner_review_decision": bool(
            sources.get("source_ready_for_owner_review_decision")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY",
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "observe_only": False,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in SAFETY_FALSE_FIELDS},
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    decision_inputs = _decision_inputs(sources)
    owner_items = _owner_decision_items()
    value_summary = _targeted_improvement_value_summary(sources)
    non_approval = _observation_non_approval_record(
        decision_inputs=decision_inputs,
        value_summary=value_summary,
    )
    next_route = _next_route_record()
    return {
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "base_candidate": BASE_CANDIDATE,
        "best_targeted_variant": BEST_TARGETED_VARIANT,
        "best_targeted_variant_decision_from_2399": EXPECTED_DECISION_FROM_2399,
        "observation_preview_candidates_count": EXPECTED_OBSERVATION_PREVIEW_COUNT,
        "research_only_observation_approved": False,
        "targeted_improvement_value_retained": True,
        "targeted_improvement_value_type": list(TARGETED_IMPROVEMENT_VALUE_TYPES),
        "plateau_review_required": True,
        "data_signal_quality_review_recommended": True,
        "threshold_meta_dataset_recommended": True,
        "recommended_next_direction_option": DEFAULT_NEXT_DIRECTION_OPTION,
        "decision_inputs": decision_inputs,
        "owner_decision_items": owner_items,
        "observation_non_approval_reason": list(OBSERVATION_NON_APPROVAL_REASONS),
        "observation_non_approval_record": non_approval,
        "targeted_improvement_value_summary": value_summary,
        "next_direction_options": _next_direction_options(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "source_findings": _source_findings(sources),
        "guardrail_summary": _guardrail_summary(),
        "next_route": next_route,
        "research_quality_status": (
            "OBSERVATION_NOT_APPROVED_PLATEAU_REVIEW_AND_DATA_SIGNAL_QUALITY_"
            "DECISION_REQUIRED"
        ),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_review_decision_recorded": False,
        "owner_decision": None,
        "base_candidate": None,
        "best_targeted_variant": None,
        "best_targeted_variant_decision_from_2399": None,
        "observation_preview_candidates_count": None,
        "research_only_observation_approved": False,
        "targeted_improvement_value_retained": False,
        "targeted_improvement_value_type": [],
        "plateau_review_required": False,
        "data_signal_quality_review_recommended": False,
        "threshold_meta_dataset_recommended": False,
        "recommended_next_direction_option": None,
        "decision_inputs": {},
        "owner_decision_items": _owner_decision_items(),
        "observation_non_approval_reason": [],
        "observation_non_approval_record": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "targeted_improvement_value_summary": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "next_direction_options": _next_direction_options(),
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "source_findings": _source_findings(sources),
        "guardrail_summary": _guardrail_summary(),
        "next_route": {},
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
        "recommended_next_research_task": None,
    }


def _decision_inputs(sources: Mapping[str, Any]) -> dict[str, Any]:
    best_row = _best_variant_ranking_row(sources)
    matrix_row = _best_variant_matrix_row(sources)
    return {
        "base_candidate": {
            "candidate_id": BASE_CANDIDATE,
            "source_task": "TRADING-2396",
            "previous_decision": m2396.DECISION_OWNER_REVIEW,
        },
        "best_targeted_variant": {
            "candidate_id": BEST_TARGETED_VARIANT,
            "source_task": "TRADING-2399",
            "decision_from_2399": EXPECTED_DECISION_FROM_2399,
            "ranking_row": dict(best_row),
            "gate_evidence_matrix_row": dict(matrix_row),
        },
        "observation_preview": {
            "candidates_count": EXPECTED_OBSERVATION_PREVIEW_COUNT,
            "implication": "no_observation_approval",
        },
        "targeted_value": {
            "likely_value_area": list(TARGETED_IMPROVEMENT_VALUE_TYPES),
        },
        "source_tasks": list(SOURCE_TASKS),
    }


def _owner_decision_items() -> dict[str, dict[str, Any]]:
    return {
        "acknowledge_best_targeted_variant": {
            "recommended_decision": (
                "APPROVE_AS_CURRENT_BEST_TARGETED_IMPROVEMENT_VARIANT"
            ),
            "approved": True,
            "candidate": BEST_TARGETED_VARIANT,
        },
        "acknowledge_continue_targeted_improvement_status": {
            "recommended_decision": "APPROVE_CONTINUE_TARGETED_IMPROVEMENT_STATUS",
            "approved": True,
            "reason": (
                "candidate improved directionally but did not meet observation preview"
            ),
        },
        "approve_research_only_observation": {
            "recommended_decision": "REJECT",
            "approved": False,
            "reason": [
                "observation_preview_candidates_count_is_zero",
                "best_variant_remains_continue_targeted_improvement",
                "gate_evidence_still_insufficient",
            ],
        },
        "approve_paper_shadow": {
            "recommended_decision": "REJECT",
            "approved": False,
            "reason": "out_of_scope",
        },
        "approve_event_outcome_mutation": {
            "recommended_decision": "REJECT",
            "approved": False,
            "reason": "out_of_scope",
        },
        "proceed_to_plateau_review": {
            "recommended_decision": "APPROVE",
            "approved": True,
            "next_task": NEXT_ROUTE,
        },
    }


def _observation_non_approval_record(
    *,
    decision_inputs: Mapping[str, Any],
    value_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "record_ready": True,
        "owner_decision": OWNER_DECISION,
        "base_candidate": BASE_CANDIDATE,
        "best_targeted_variant": BEST_TARGETED_VARIANT,
        "best_targeted_variant_decision_from_2399": EXPECTED_DECISION_FROM_2399,
        "observation_preview_candidates_count": EXPECTED_OBSERVATION_PREVIEW_COUNT,
        "research_only_observation_approved": False,
        "targeted_improvement_value_retained": True,
        "plateau_review_required": True,
        "data_signal_quality_review_recommended": True,
        "candidate_auto_accept_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "non_approval_reasons": list(OBSERVATION_NON_APPROVAL_REASONS),
        "decision_inputs": dict(decision_inputs),
        "targeted_improvement_value_summary": dict(value_summary),
    }


def _targeted_improvement_value_summary(sources: Mapping[str, Any]) -> dict[str, Any]:
    best_row = _best_variant_ranking_row(sources)
    matrix_row = _best_variant_matrix_row(sources)
    return {
        "record_ready": True,
        "best_variant": BEST_TARGETED_VARIANT,
        "base_candidate": BASE_CANDIDATE,
        "value_type": list(TARGETED_IMPROVEMENT_VALUE_TYPES),
        "research_value": True,
        "observation_ready": False,
        "recommended_handling": (
            "retain_as_component_or_variant_reference_for_future_review"
        ),
        "decision_from_2399": EXPECTED_DECISION_FROM_2399,
        "ranking_metrics": {
            "rank": best_row.get("rank"),
            "annualized_return": best_row.get("annualized_return"),
            "max_drawdown": best_row.get("max_drawdown"),
            "time_slice_pass_rate": best_row.get("time_slice_pass_rate"),
            "regime_expectation_score": best_row.get("regime_expectation_score"),
            "return_retention_vs_raw_growth_tilt": best_row.get(
                "return_retention_vs_raw_growth_tilt"
            ),
            "turnover_reduction_vs_raw_growth_tilt": best_row.get(
                "turnover_reduction_vs_raw_growth_tilt"
            ),
            "stale_signal_execution_count": best_row.get(
                "stale_signal_execution_count"
            ),
            "no_stale_signal_carry_forward": best_row.get(
                "no_stale_signal_carry_forward"
            ),
        },
        "gate_evidence": dict(matrix_row),
        "interpretation": (
            "valid-until strictness appears to be the strongest targeted direction, "
            "but 2399 did not produce an observation preview candidate."
        ),
    }


def _next_direction_options() -> dict[str, dict[str, Any]]:
    return {
        "OPTION_A_CONTINUE_VALID_UNTIL_STRICT_TARGETED_IMPROVEMENT": {
            "recommended": False,
            "meaning": (
                "continue small valid-until strict improvements, with marginal "
                "benefit decay risk"
            ),
        },
        "OPTION_B_PAUSE_RECOMBINATION_CANDIDATE_LINE_AND_REVIEW_DATA_SIGNAL_QUALITY": {
            "recommended": True,
            "meaning": (
                "pause the current recombination candidate line and review data, "
                "PIT, signal quality and regime labeling"
            ),
        },
        "OPTION_C_BUILD_THRESHOLD_META_DATASET": {
            "recommended": True,
            "meaning": (
                "build threshold calibration meta-dataset to improve gate judgment"
            ),
        },
        "OPTION_D_RETURN_TO_SIGNAL_CONSTRUCTION_FRAMEWORK": {
            "recommended": False,
            "meaning": (
                "return to indicator -> signal -> weight mapping instead of more "
                "strategy-variant combinations"
            ),
        },
        "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW": {
            "recommended": False,
            "meaning": "pause the dynamic strategy research line for now",
        },
    }


def _next_route_record() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "TRADING-2399 did not produce an observation preview candidate after "
            "multiple candidate, guardrail, component, recombination and targeted "
            "retest rounds; the next decision should assess plateau and data/signal "
            "quality rather than auto-generate more variants."
        ),
        "owner_decision": OWNER_DECISION,
        "default_next_direction_option": DEFAULT_NEXT_DIRECTION_OPTION,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    retest = _as_mapping(sources.get("targeted_retest_result_2399"))
    decision = _as_mapping(_as_mapping(sources.get("decision_update_2399")).get("decision_update"))
    plan_2398 = _as_mapping(sources.get("gate_evidence_plan_result_2398"))
    owner_2397 = _as_mapping(sources.get("owner_review_decision_2397"))
    retest_2396 = _as_mapping(sources.get("recombination_retest_result_2396"))
    return {
        "trading_2399": {
            "status": retest.get("status"),
            "base_candidate": retest.get("candidate_under_review"),
            "best_targeted_variant": retest.get("best_targeted_variant"),
            "best_targeted_variant_decision": retest.get(
                "best_targeted_variant_decision"
            ),
            "observation_preview_candidates_count": retest.get(
                "observation_preview_candidates_count"
            ),
            "research_only_observation_approved": retest.get(
                "research_only_observation_approved"
            ),
            "recommended_next_research_task": retest.get(
                "recommended_next_research_task"
            ),
            "owner_review_required_candidates_count": decision.get(
                "owner_review_required_candidates_count"
            ),
        },
        "trading_2398": {
            "status": plan_2398.get("status"),
            "candidate_under_review": plan_2398.get("candidate_under_review"),
            "planned_targeted_variants": plan_2398.get("planned_targeted_variants"),
            "recommended_next_research_task": plan_2398.get(
                "recommended_next_research_task"
            ),
        },
        "trading_2397": {
            "status": owner_2397.get("status"),
            "owner_decision": owner_2397.get("owner_decision"),
            "research_only_observation_approved": owner_2397.get(
                "research_only_observation_approved"
            ),
        },
        "trading_2396": {
            "status": retest_2396.get("status"),
            "best_recombination_candidate": retest_2396.get(
                "best_recombination_candidate"
            ),
            "best_recombination_decision": retest_2396.get(
                "best_recombination_decision"
            ),
        },
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "OWNER_REVIEW_DECISION_RECORD_ONLY",
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "targeted_improvement_value_retained": True,
        "plateau_review_required": True,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
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
        "json_path": str(output_root / "owner_review_decision.json"),
        "observation_non_approval_record_json": str(
            output_root / "observation_non_approval_record.json"
        ),
        "targeted_improvement_value_summary_json": str(
            output_root / "targeted_improvement_value_summary.json"
        ),
        "next_route_json": str(output_root / "next_route.json"),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_targeted_gate_evidence_owner_review_decision.md"
        ),
        "observation_non_approval_markdown": str(
            docs_root / "dynamic_strategy_targeted_variant_non_approval_record.md"
        ),
        "targeted_improvement_value_summary_markdown": str(
            docs_root / "dynamic_strategy_targeted_improvement_value_summary.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2401_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["observation_non_approval_record_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_targeted_variant_non_approval_record",
            "schema_version": (
                "dynamic_strategy_targeted_variant_non_approval_record.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_non_approval_record": payload.get(
                "observation_non_approval_record", {}
            ),
            "research_only_observation_approved": False,
            "candidate_auto_accept_approved": False,
            "paper_shadow_enabled": False,
            "event_append_enabled": False,
            "outcome_binding_enabled": False,
            "scheduler_enabled": False,
            "production_enabled": False,
            "broker_action_enabled": False,
            "daily_report_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["targeted_improvement_value_summary_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_targeted_improvement_value_summary",
            "schema_version": "dynamic_strategy_targeted_improvement_value_summary.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "targeted_improvement_value_summary": payload.get(
                "targeted_improvement_value_summary", {}
            ),
            "targeted_improvement_value_retained": payload.get(
                "targeted_improvement_value_retained"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_route_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_2401_route",
            "schema_version": "dynamic_strategy_2401_route.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "next_route": payload.get("next_route", {}),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["observation_non_approval_markdown"]),
        _non_approval_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["targeted_improvement_value_summary_markdown"]),
        _value_summary_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy targeted gate evidence owner review decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- base candidate：`{payload.get('base_candidate')}`",
            f"- best targeted variant：`{payload.get('best_targeted_variant')}`",
            "- decision from 2399："
            f"`{payload.get('best_targeted_variant_decision_from_2399')}`",
            "- observation preview candidates："
            f"`{payload.get('observation_preview_candidates_count')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            "- research-only observation approved："
            f"`{payload.get('research_only_observation_approved')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2399",
            "",
            "- 2399 best targeted variant 是 "
            f"`{payload.get('best_targeted_variant')}`。",
            "- 2399 decision 是 "
            f"`{payload.get('best_targeted_variant_decision_from_2399')}`。",
            "- observation preview candidate count 为 "
            f"`{payload.get('observation_preview_candidates_count')}`。",
            "",
            "## Best targeted variant review",
            "",
            "- `valid_until_strict` 方向有研究价值，主要来自 signal expiry "
            "discipline、stale signal guardrail 和 valid-until strictness。",
            "- 该方向仍未达到 observation preview，不应进入 research-only "
            "observation 或 paper-shadow。",
            "",
            "## Owner decision",
            "",
            f"- owner decision：`{payload.get('owner_decision')}`",
            "- targeted improvement value retained："
            f"`{payload.get('targeted_improvement_value_retained')}`",
            f"- plateau review required：`{payload.get('plateau_review_required')}`",
            "",
            "## Why observation is not approved",
            "",
            *[
                f"- `{reason}`"
                for reason in payload.get("observation_non_approval_reason", [])
            ],
            "",
            "## Targeted improvement value summary",
            "",
            _json_block(payload.get("targeted_improvement_value_summary", {})),
            "",
            "## Next direction options",
            "",
            _json_block(payload.get("next_direction_options", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
            "## Guardrail summary",
            "",
            f"- paper-shadow enabled：`{payload.get('paper_shadow_enabled')}`",
            f"- event append enabled：`{payload.get('event_append_enabled')}`",
            f"- outcome binding enabled：`{payload.get('outcome_binding_enabled')}`",
            f"- scheduler enabled：`{payload.get('scheduler_enabled')}`",
            f"- production enabled：`{payload.get('production_enabled')}`",
            f"- broker action：`{payload.get('broker_action')}`",
            f"- daily report generated：`{payload.get('daily_report_generated')}`",
            "",
            "## Recommended next route",
            "",
            f"- `{payload.get('recommended_next_research_task')}`",
            "",
        ]
    )


def _non_approval_markdown(payload: Mapping[str, Any]) -> str:
    record = _as_mapping(payload.get("observation_non_approval_record"))
    return "\n".join(
        [
            "# Dynamic strategy targeted variant non-approval record",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- best targeted variant：`{payload.get('best_targeted_variant')}`",
            "- research-only observation approved："
            f"`{record.get('research_only_observation_approved')}`",
            f"- paper-shadow enabled：`{record.get('paper_shadow_enabled')}`",
            f"- event append enabled：`{record.get('event_append_enabled')}`",
            f"- outcome binding enabled：`{record.get('outcome_binding_enabled')}`",
            f"- scheduler enabled：`{record.get('scheduler_enabled')}`",
            f"- production enabled：`{record.get('production_enabled')}`",
            f"- broker action enabled：`{record.get('broker_action_enabled')}`",
            "",
            "## Non-approval reasons",
            "",
            *[f"- `{reason}`" for reason in record.get("non_approval_reasons", [])],
            "",
        ]
    )


def _value_summary_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("targeted_improvement_value_summary"))
    return "\n".join(
        [
            "# Dynamic strategy targeted improvement value summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best targeted variant：`{summary.get('best_variant')}`",
            f"- research value：`{summary.get('research_value')}`",
            f"- observation ready：`{summary.get('observation_ready')}`",
            f"- recommended handling：`{summary.get('recommended_handling')}`",
            "",
            "## Value types",
            "",
            *[f"- `{item}`" for item in summary.get("value_type", [])],
            "",
            "## Metrics and evidence",
            "",
            _json_block(summary),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    route = _as_mapping(payload.get("next_route"))
    return "\n".join(
        [
            "# Dynamic strategy 2401 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{route.get('recommended_next_research_task')}`",
            f"- route reason：{route.get('route_reason')}",
            "- default next direction option："
            f"`{route.get('default_next_direction_option')}`",
            "- research-only observation approved："
            f"`{route.get('research_only_observation_approved')}`",
            f"- paper-shadow enabled：`{route.get('paper_shadow_enabled')}`",
            f"- production enabled：`{route.get('production_enabled')}`",
            f"- broker action enabled：`{route.get('broker_action_enabled')}`",
            "",
        ]
    )


def _best_variant_ranking_row(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_doc = _as_mapping(sources.get("targeted_variant_ranking_2399"))
    rows = _as_list_of_mappings(ranking_doc.get("targeted_variant_ranking"))
    return next((row for row in rows if row.get("candidate_id") == BEST_TARGETED_VARIANT), {})


def _best_variant_matrix_row(sources: Mapping[str, Any]) -> dict[str, Any]:
    matrix_doc = _as_mapping(sources.get("gate_evidence_matrix_2399"))
    rows = _as_list_of_mappings(matrix_doc.get("gate_evidence_matrix"))
    return next((row for row in rows if row.get("candidate_id") == BEST_TARGETED_VARIANT), {})


def _source_document_names() -> tuple[str, ...]:
    return (
        "targeted_retest_result_2399",
        "targeted_variant_ranking_2399",
        "gate_evidence_matrix_2399",
        "decision_update_2399",
        "gate_evidence_plan_result_2398",
        "targeted_improvement_plan_2398",
        "next_route_2398",
        "owner_review_decision_2397",
        "recombination_retest_result_2396",
        "decision_update_2396",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in _as_list(value) if isinstance(item, Mapping)]
