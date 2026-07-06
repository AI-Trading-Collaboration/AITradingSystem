from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_component_ablation_owner_review_decision as m2394
import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as m2393
import ai_trading_system.dynamic_strategy_component_recombination_candidate_plan as m2395
import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as m2396
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2397"
TASK_REGISTER_ID = (
    "TRADING-2397_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_"
    "OBSERVATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_recombination_candidate_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_recombination_candidate_owner_review_and_observation_"
    "decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_"
    "DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_"
    "DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = (
    "KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_"
    "EVIDENCE"
)
NEXT_ROUTE = (
    "TRADING-2398_Dynamic_Strategy_Recombination_Candidate_Gate_Evidence_And_"
    "Targeted_Improvement_Plan"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2393", "TRADING-2394", "TRADING-2395", "TRADING-2396")
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA"
)
BEST_RECOMBINATION_CANDIDATE = "growth_tilt_lower_turnover_guarded_transfer_v1"
EXPECTED_DECISION_FROM_2396 = m2396.DECISION_OWNER_REVIEW

OBSERVATION_NON_APPROVAL_REASONS: tuple[str, ...] = (
    "NO_OBSERVATION_PREVIEW_CANDIDATE_FOUND",
    "BEST_CANDIDATE_REMAINS_OWNER_REVIEW_REQUIRED",
    "GATE_EVIDENCE_GAPS_REMAIN",
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
)

# These reuse TRADING-2396 research sorting constants. They are not promotion,
# observation, production, or broker gates; 2397 only explains why the 2396
# owner-review candidate is still not observation-approved.
TIME_SLICE_EVIDENCE_MIN = m2396.PREVIEW_TIME_SLICE_PASS_RATE_MIN
REGIME_EVIDENCE_MIN = m2396.PREVIEW_REGIME_EXPECTATION_SCORE_MIN
RETURN_RETENTION_MIN = m2396.OWNER_REVIEW_RETURN_RETENTION_MIN
TURNOVER_REDUCTION_SIGN_BOUNDARY = 0.0

DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
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


def run_dynamic_strategy_recombination_candidate_owner_review_decision(
    *,
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
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
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
        source_owner_review_decision_2394_path=source_owner_review_decision_2394_path,
        source_component_recombination_decision_2394_path=(
            source_component_recombination_decision_2394_path
        ),
        source_ablation_retest_result_2393_path=source_ablation_retest_result_2393_path,
        source_component_attribution_matrix_2393_path=(
            source_component_attribution_matrix_2393_path
        ),
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
        "recombination_retest_result_2396": m2396.READY_STATUS,
        "recombination_candidate_ranking_2396": m2396.READY_STATUS,
        "component_evidence_matrix_2396": m2396.READY_STATUS,
        "decision_update_2396": m2396.READY_STATUS,
        "recombination_candidate_plan_2395": m2395.READY_STATUS,
        "candidate_definitions_2395": m2395.READY_STATUS,
        "owner_review_decision_2394": m2394.READY_STATUS,
        "component_recombination_decision_2394": m2394.READY_STATUS,
        "ablation_retest_result_2393": m2393.READY_STATUS,
        "component_attribution_matrix_2393": m2393.READY_STATUS,
    }
    status_map = _as_mapping(sources.get("source_status"))
    for source_name, expected in expected_status.items():
        if status_map.get(source_name) != expected:
            errors.append(
                f"{source_name}: expected status {expected}, "
                f"got {status_map.get(source_name)}"
            )

    _validate_2396_retest(sources, errors)
    _validate_2395_plan(sources, errors)
    _validate_2394_owner_decision(sources, errors)
    _validate_2393_component_context(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2396_retest(sources: Mapping[str, Any], errors: list[str]) -> None:
    retest = _as_mapping(sources.get("recombination_retest_result_2396"))
    ranking_doc = _as_mapping(sources.get("recombination_candidate_ranking_2396"))
    update_doc = _as_mapping(sources.get("decision_update_2396"))
    update = _as_mapping(update_doc.get("decision_update"))
    ranking_row = _ranking_row(sources)
    evidence_row = _component_evidence_row(sources)

    if retest.get("recommended_next_research_task") != m2396.NEXT_ROUTE:
        errors.append("2396 retest route did not point to TRADING-2397")
    if retest.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 best recombination candidate mismatch")
    if retest.get("best_recombination_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 best recombination decision mismatch")
    for field in (
        "recombination_retest_ready",
        "candidate_ranking_ready",
        "component_evidence_matrix_ready",
        "decision_update_ready",
    ):
        if retest.get(field) is not True:
            errors.append(f"2396 retest missing ready flag {field}")
    if retest.get("research_only_observation_approved") is True:
        errors.append("2396 unexpectedly approved research-only observation")

    if ranking_doc.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 ranking best candidate mismatch")
    if ranking_doc.get("best_recombination_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 ranking best decision mismatch")
    if ranking_row.get("rank") != 1:
        errors.append("2396 ranking row for best candidate is not rank 1")
    if ranking_row.get("decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 ranking row decision mismatch")
    if evidence_row.get("candidate_id") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 component evidence missing best candidate")
    quality = _as_mapping(evidence_row.get("recombination_quality"))
    if quality.get("candidate_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 component evidence decision mismatch")

    if update.get("recommended_next_research_task") != m2396.NEXT_ROUTE:
        errors.append("2396 decision update route mismatch")
    if update.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 decision update best candidate mismatch")
    if update.get("best_recombination_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 decision update best decision mismatch")
    if update.get("research_only_observation_preview_exists") is True:
        errors.append("2396 unexpectedly found observation preview candidate")
    if _observation_preview_count(sources) != 0:
        errors.append("2396 observation preview candidate count is not zero")
    if update.get("research_only_observation_approved") is True:
        errors.append("2396 decision update unexpectedly approved observation")


def _validate_2395_plan(sources: Mapping[str, Any], errors: list[str]) -> None:
    plan = _as_mapping(sources.get("recombination_candidate_plan_2395"))
    definitions_doc = _as_mapping(sources.get("candidate_definitions_2395"))
    definitions = _as_list_of_mappings(
        definitions_doc.get("recombination_candidate_definitions")
    )
    best_definition = next(
        (
            row
            for row in definitions
            if row.get("candidate_id") == BEST_RECOMBINATION_CANDIDATE
        ),
        {},
    )

    if plan.get("recommended_next_research_task") != m2395.NEXT_ROUTE:
        errors.append("2395 plan route did not point to TRADING-2396")
    if plan.get("recombination_candidate_plan_ready") is not True:
        errors.append("2395 recombination candidate plan is not ready")
    if plan.get("candidate_definitions_ready") is not True and not definitions:
        errors.append("2395 candidate definitions are not ready")
    if BEST_RECOMBINATION_CANDIDATE not in set(
        str(item) for item in _as_list(plan.get("planned_recombination_candidates"))
    ):
        errors.append("2395 planned candidates missing best recombination candidate")
    if plan.get("return_engine_component") != m2395.RETURN_ENGINE_COMPONENT:
        errors.append("2395 return engine component mismatch")
    if m2395.GUARDED_TURNOVER_TRANSFER not in set(
        str(item) for item in _as_list(plan.get("owner_review_components"))
    ):
        errors.append("2395 owner review components missing guarded turnover transfer")
    for guardrail in (
        m2395.LOWER_TURNOVER_GUARDRAIL,
        m2395.VALID_UNTIL_WINDOW,
        m2395.NO_STALE_SIGNAL_CARRY_FORWARD,
    ):
        if guardrail not in set(str(item) for item in _as_list(plan.get("guardrail_components"))):
            errors.append(f"2395 guardrail components missing {guardrail}")
    if not best_definition:
        errors.append("2395 candidate definitions missing best recombination candidate")
    elif best_definition.get("owner_review_required") is not True:
        errors.append("2395 best candidate definition is not owner-review-required")


def _validate_2394_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2394"))
    recombination_doc = _as_mapping(sources.get("component_recombination_decision_2394"))
    recombination = _as_mapping(
        recombination_doc.get("component_recombination_decision")
    )
    if owner.get("owner_decision") != m2394.OWNER_DECISION:
        errors.append("2394 owner decision mismatch")
    if owner.get("recombination_plan_approved") is not True:
        errors.append("2394 recombination plan was not approved")
    if owner.get("best_reusable_component") != m2394.BEST_REUSABLE_COMPONENT:
        errors.append("2394 best reusable component mismatch")
    if owner.get("research_only_observation_approved") is True:
        errors.append("2394 unexpectedly approved research-only observation")
    if owner.get("recommended_next_research_task") != m2394.NEXT_ROUTE:
        errors.append("2394 route did not point to TRADING-2395")
    if recombination.get("record_ready") is not True:
        errors.append("2394 component recombination decision is not ready")
    if recombination.get("owner_decision") != m2394.OWNER_DECISION:
        errors.append("2394 component recombination owner decision mismatch")


def _validate_2393_component_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    matrix_doc = _as_mapping(sources.get("component_attribution_matrix_2393"))
    decisions = _component_decisions(matrix_doc)
    if retest.get("recommended_next_research_task") != m2393.NEXT_ROUTE:
        errors.append("2393 route did not point to TRADING-2394")
    if retest.get("best_reusable_component") != m2394.BEST_REUSABLE_COMPONENT:
        errors.append("2393 best reusable component mismatch")
    if retest.get("data_quality_gate_executed") is not True:
        errors.append("2393 data quality gate was not executed")
    if retest.get("research_only_observation_approved") is True:
        errors.append("2393 unexpectedly approved research-only observation")
    if decisions.get(m2395.RETURN_ENGINE_COMPONENT) != m2393.COMPONENT_DECISION_REUSABLE:
        errors.append("2393 growth_tilt_engine decision mismatch")
    if decisions.get(m2395.LOWER_TURNOVER_GUARDRAIL) != m2393.COMPONENT_DECISION_GUARDRAIL:
        errors.append("2393 lower_turnover_guardrail decision mismatch")
    if decisions.get(m2395.GUARDED_TURNOVER_TRANSFER) != (
        m2393.COMPONENT_DECISION_OWNER_REVIEW
    ):
        errors.append("2393 guarded_turnover_transfer decision mismatch")


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
    gap_summary = _gate_evidence_gap_summary(sources)
    non_approval = _observation_non_approval_record(
        decision_inputs=decision_inputs,
        gate_evidence_gap_summary=gap_summary,
    )
    next_route = _next_route_record()
    return {
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "best_recombination_candidate": BEST_RECOMBINATION_CANDIDATE,
        "best_recombination_decision_from_2396": EXPECTED_DECISION_FROM_2396,
        "owner_review_required_retained": True,
        "observation_preview_candidates_count": 0,
        "research_only_observation_approved": False,
        "observation_non_approval_reason": list(OBSERVATION_NON_APPROVAL_REASONS),
        "gate_evidence_gap_summary_ready": True,
        "decision_inputs": decision_inputs,
        "owner_decision_items": owner_items,
        "observation_non_approval_record": non_approval,
        "gate_evidence_gap_summary": gap_summary,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "source_findings": _source_findings(sources),
        "guardrail_summary": _guardrail_summary(),
        "next_route": next_route,
        "research_quality_status": (
            "OWNER_REVIEW_RETAINED_OBSERVATION_NOT_APPROVED_GATE_EVIDENCE_REQUIRED"
        ),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "owner_review_decision_recorded": False,
        "owner_decision": None,
        "best_recombination_candidate": None,
        "best_recombination_decision_from_2396": None,
        "owner_review_required_retained": False,
        "observation_preview_candidates_count": _observation_preview_count(sources),
        "research_only_observation_approved": False,
        "observation_non_approval_reason": [],
        "gate_evidence_gap_summary_ready": False,
        "decision_inputs": {},
        "owner_decision_items": _owner_decision_items(),
        "observation_non_approval_record": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "gate_evidence_gap_summary": {
            "record_ready": False,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
        "source_findings": _source_findings(sources),
        "guardrail_summary": _guardrail_summary(),
        "next_route": {},
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
        "recommended_next_research_task": None,
    }


def _decision_inputs(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_row = _ranking_row(sources)
    evidence_row = _component_evidence_row(sources)
    plan = _as_mapping(sources.get("recombination_candidate_plan_2395"))
    return {
        "best_recombination_candidate": {
            "candidate_id": BEST_RECOMBINATION_CANDIDATE,
            "decision_from_2396": EXPECTED_DECISION_FROM_2396,
            "role": "current_best_recombination_candidate",
            "ranking_row": dict(ranking_row),
            "component_evidence_row": dict(evidence_row),
        },
        "observation_preview_candidates": {
            "count": 0,
            "implication": "no automatic observation approval",
        },
        "source_components": {
            "return_engine": plan.get("return_engine_component"),
            "guardrails": list(plan.get("guardrail_components", [])),
            "owner_review_component": list(plan.get("owner_review_components", [])),
        },
        "source_tasks": list(SOURCE_TASKS),
    }


def _owner_decision_items() -> dict[str, dict[str, Any]]:
    return {
        "acknowledge_best_recombination_candidate": {
            "recommended_decision": "APPROVE_AS_CURRENT_BEST_RECOMBINATION_CANDIDATE",
            "approved": True,
            "candidate": BEST_RECOMBINATION_CANDIDATE,
        },
        "acknowledge_owner_review_required": {
            "recommended_decision": "APPROVE_OWNER_REVIEW_REQUIRED_STATUS",
            "approved": True,
            "reason": "candidate improved enough to enter human review layer",
        },
        "approve_research_only_observation": {
            "recommended_decision": "REJECT",
            "approved": False,
            "reason": [
                "observation_preview_candidates_count_is_zero",
                "candidate_did_not_meet_observation_preview_gate",
                "gate_evidence_gaps_remain",
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
        "proceed_to_gate_evidence_plan": {
            "recommended_decision": "APPROVE",
            "approved": True,
            "next_task": NEXT_ROUTE,
        },
    }


def _observation_non_approval_record(
    *,
    decision_inputs: Mapping[str, Any],
    gate_evidence_gap_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "record_ready": True,
        "owner_decision": OWNER_DECISION,
        "best_recombination_candidate": BEST_RECOMBINATION_CANDIDATE,
        "best_recombination_decision_from_2396": EXPECTED_DECISION_FROM_2396,
        "owner_review_required_retained": True,
        "observation_preview_candidates_count": 0,
        "research_only_observation_approved": False,
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
        "gate_evidence_gap_summary": dict(gate_evidence_gap_summary),
    }


def _gate_evidence_gap_summary(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_row = _ranking_row(sources)
    evidence_row = _component_evidence_row(sources)
    time_slice_pass_rate = _to_float(ranking_row.get("time_slice_pass_rate"))
    regime_expectation_score = _to_float(ranking_row.get("regime_expectation_score"))
    return_retention = _to_float(
        ranking_row.get("return_retention_vs_raw_growth_tilt")
    )
    turnover_reduction = _to_float(
        ranking_row.get("turnover_reduction_vs_raw_growth_tilt")
    )
    stale_count = _to_float(ranking_row.get("stale_signal_execution_count"))
    no_stale = ranking_row.get("no_stale_signal_carry_forward") is True
    cost_stress_survival = ranking_row.get("cost_stress_survival")
    return {
        "record_ready": True,
        "best_recombination_candidate": BEST_RECOMBINATION_CANDIDATE,
        "source_task": "TRADING-2396",
        "ranking_metrics": {
            "time_slice_pass_rate": time_slice_pass_rate,
            "regime_expectation_score": regime_expectation_score,
            "return_retention_vs_raw_growth_tilt": return_retention,
            "turnover_reduction_vs_raw_growth_tilt": turnover_reduction,
            "max_drawdown": _to_float(ranking_row.get("max_drawdown")),
            "cost_stress_survival": cost_stress_survival,
            "stale_signal_execution_count": stale_count,
            "no_stale_signal_carry_forward": no_stale,
        },
        "component_evidence": dict(evidence_row),
        "gate_evidence_gaps": {
            "time_slice_evidence": {
                "status": (
                    "GAP_REMAINS"
                    if time_slice_pass_rate < TIME_SLICE_EVIDENCE_MIN
                    else "NO_GAP_DETECTED"
                ),
                "question": "是否仍存在 time slice 稳定性不足？",
                "source_value": time_slice_pass_rate,
                "reference_from_2396": TIME_SLICE_EVIDENCE_MIN,
            },
            "regime_evidence": {
                "status": (
                    "GAP_REMAINS"
                    if regime_expectation_score < REGIME_EVIDENCE_MIN
                    else "NO_GAP_DETECTED"
                ),
                "question": "是否仍存在 regime expectation score 不足？",
                "source_value": regime_expectation_score,
                "reference_from_2396": REGIME_EVIDENCE_MIN,
            },
            "drawdown_materiality": {
                "status": "OWNER_JUDGMENT_REQUIRED",
                "question": "drawdown trade-off 是否仍需要 owner judgment？",
                "source_value": _to_float(ranking_row.get("max_drawdown")),
            },
            "return_retention": {
                "status": (
                    "ADEQUATE"
                    if return_retention >= RETURN_RETENTION_MIN
                    else "GAP_REMAINS"
                ),
                "question": "是否保留足够 growth_tilt return？",
                "source_value": return_retention,
                "reference_from_2396": RETURN_RETENTION_MIN,
            },
            "turnover_guardrail": {
                "status": (
                    "GAP_REMAINS"
                    if turnover_reduction < TURNOVER_REDUCTION_SIGN_BOUNDARY
                    else "NO_GAP_DETECTED"
                ),
                "question": "lower-turnover / guarded transfer 是否有效降低换手？",
                "source_value": turnover_reduction,
                "reference_sign_boundary": TURNOVER_REDUCTION_SIGN_BOUNDARY,
            },
            "valid_until_guardrail": {
                "status": "PASS" if no_stale and stale_count == 0 else "GAP_REMAINS",
                "question": "是否确认 no stale signal carry-forward？",
                "no_stale_signal_carry_forward": no_stale,
                "stale_signal_execution_count": stale_count,
            },
            "cost_stress": {
                "status": "PASS" if cost_stress_survival == "harsh" else "GAP_REMAINS",
                "question": "是否穿越 realistic / conservative cost？",
                "source_value": cost_stress_survival,
            },
        },
        "blocking_summary": [
            "time_slice_evidence remains below 2396 preview reference",
            "regime_evidence remains below 2396 preview reference",
            "turnover_guardrail did not reduce turnover versus raw growth tilt",
            "drawdown materiality still requires owner judgment",
        ],
    }


def _next_route_record() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "best recombination candidate remains OWNER_REVIEW_REQUIRED without "
            "observation preview; targeted gate evidence and improvement plan is "
            "required before any observation decision can be reconsidered"
        ),
        "best_recombination_candidate": BEST_RECOMBINATION_CANDIDATE,
        "owner_decision": OWNER_DECISION,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    retest = _as_mapping(sources.get("recombination_retest_result_2396"))
    plan = _as_mapping(sources.get("recombination_candidate_plan_2395"))
    owner = _as_mapping(sources.get("owner_review_decision_2394"))
    ablation = _as_mapping(sources.get("ablation_retest_result_2393"))
    update = _as_mapping(_as_mapping(sources.get("decision_update_2396")).get("decision_update"))
    return {
        "trading_2396": {
            "status": retest.get("status"),
            "best_recombination_candidate": retest.get("best_recombination_candidate"),
            "best_recombination_decision": retest.get("best_recombination_decision"),
            "observation_preview_candidates_count": len(
                _as_list(update.get("observation_preview_candidates"))
            ),
            "research_only_observation_approved": retest.get(
                "research_only_observation_approved"
            ),
            "recommended_next_research_task": retest.get(
                "recommended_next_research_task"
            ),
        },
        "trading_2395": {
            "status": plan.get("status"),
            "return_engine_component": plan.get("return_engine_component"),
            "guardrail_components": plan.get("guardrail_components"),
            "owner_review_components": plan.get("owner_review_components"),
            "planned_recombination_candidate_count": len(
                _as_list(plan.get("planned_recombination_candidates"))
            ),
        },
        "trading_2394": {
            "status": owner.get("status"),
            "owner_decision": owner.get("owner_decision"),
            "recombination_plan_approved": owner.get("recombination_plan_approved"),
            "research_only_observation_approved": owner.get(
                "research_only_observation_approved"
            ),
        },
        "trading_2393": {
            "status": ablation.get("status"),
            "best_reusable_component": ablation.get("best_reusable_component"),
            "data_quality_status": ablation.get("data_quality_status"),
            "research_only_observation_approved": ablation.get(
                "research_only_observation_approved"
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
        "owner_review_required_retained": True,
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
        "gate_evidence_gap_summary_json": str(
            output_root / "gate_evidence_gap_summary.json"
        ),
        "next_route_json": str(output_root / "next_route.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_recombination_candidate_owner_review_decision.md"
        ),
        "observation_non_approval_markdown": str(
            docs_root
            / "dynamic_strategy_recombination_observation_non_approval_record.md"
        ),
        "gate_evidence_gap_summary_markdown": str(
            docs_root / "dynamic_strategy_recombination_gate_evidence_gap_summary.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2398_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["observation_non_approval_record_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_observation_non_approval_record",
            "schema_version": (
                "dynamic_strategy_recombination_observation_non_approval_record.v1"
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
        Path(paths["gate_evidence_gap_summary_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_gate_evidence_gap_summary",
            "schema_version": (
                "dynamic_strategy_recombination_gate_evidence_gap_summary.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "gate_evidence_gap_summary": payload.get("gate_evidence_gap_summary", {}),
            "gate_evidence_gap_summary_ready": payload.get(
                "gate_evidence_gap_summary_ready"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_route_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_2398_route",
            "schema_version": "dynamic_strategy_2398_route.v1",
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
        Path(paths["gate_evidence_gap_summary_markdown"]),
        _gap_summary_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    gap_summary = _as_mapping(payload.get("gate_evidence_gap_summary"))
    gaps = _as_mapping(gap_summary.get("gate_evidence_gaps"))
    return "\n".join(
        [
            "# Dynamic strategy recombination candidate owner review decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- best recombination candidate：`{payload.get('best_recombination_candidate')}`",
            f"- decision from 2396：`{payload.get('best_recombination_decision_from_2396')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            "- research-only observation approved："
            f"`{payload.get('research_only_observation_approved')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2396",
            "",
            "- 2396 best recombination candidate 是 "
            f"`{payload.get('best_recombination_candidate')}`。",
            "- 2396 best decision 是 "
            f"`{payload.get('best_recombination_decision_from_2396')}`。",
            "- observation preview candidate count="
            f"`{payload.get('observation_preview_candidates_count')}`。",
            "",
            "## Best recombination candidate review",
            "",
            "- 当前候选保留 `OWNER_REVIEW_REQUIRED`，但不等于 research-only observation approval。",
            "- 当前任务只记录 owner decision，不重新运行 backtest 或生成新 signal。",
            "",
            "## Owner review decision",
            "",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- owner review required retained：`{payload.get('owner_review_required_retained')}`",
            "",
            "## Why observation is not approved",
            "",
            "- observation preview candidates count 为 0。",
            "- best candidate 仍停留在 OWNER_REVIEW_REQUIRED。",
            "- gate evidence gaps 仍存在，尤其是 time slice、regime expectation "
            "与 turnover guardrail。",
            "",
            "## Gate evidence gaps",
            "",
            "|Gate|Status|Source value|",
            "|---|---|---|",
            *[
                (
                    f"|`{key}`|`{_as_mapping(value).get('status')}`|"
                    f"`{_as_mapping(value).get('source_value')}`|"
                )
                for key, value in gaps.items()
            ],
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
            "# Dynamic strategy recombination observation non-approval record",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- best candidate：`{payload.get('best_recombination_candidate')}`",
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
            *[
                f"- `{reason}`"
                for reason in record.get("non_approval_reasons", [])
            ],
            "",
        ]
    )


def _gap_summary_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("gate_evidence_gap_summary"))
    gaps = _as_mapping(summary.get("gate_evidence_gaps"))
    return "\n".join(
        [
            "# Dynamic strategy recombination gate evidence gap summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- best candidate：`{summary.get('best_recombination_candidate')}`",
            f"- record ready：`{summary.get('record_ready')}`",
            "",
            "|Gate|Status|Question|",
            "|---|---|---|",
            *[
                (
                    f"|`{key}`|`{_as_mapping(value).get('status')}`|"
                    f"{_as_mapping(value).get('question')}|"
                )
                for key, value in gaps.items()
            ],
            "",
            "## Blocking summary",
            "",
            *[f"- {item}" for item in summary.get("blocking_summary", [])],
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    route = _as_mapping(payload.get("next_route"))
    return "\n".join(
        [
            "# Dynamic strategy 2398 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{route.get('recommended_next_research_task')}`",
            f"- route reason：{route.get('route_reason')}",
            "- research-only observation approved："
            f"`{route.get('research_only_observation_approved')}`",
            f"- paper-shadow enabled：`{route.get('paper_shadow_enabled')}`",
            f"- production enabled：`{route.get('production_enabled')}`",
            f"- broker action enabled：`{route.get('broker_action_enabled')}`",
            "",
        ]
    )


def _ranking_row(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_doc = _as_mapping(sources.get("recombination_candidate_ranking_2396"))
    rows = _as_list_of_mappings(ranking_doc.get("recombination_candidate_ranking"))
    return next(
        (row for row in rows if row.get("candidate_id") == BEST_RECOMBINATION_CANDIDATE),
        {},
    )


def _component_evidence_row(sources: Mapping[str, Any]) -> dict[str, Any]:
    evidence_doc = _as_mapping(sources.get("component_evidence_matrix_2396"))
    rows = _as_list_of_mappings(evidence_doc.get("component_evidence_matrix"))
    return next(
        (row for row in rows if row.get("candidate_id") == BEST_RECOMBINATION_CANDIDATE),
        {},
    )


def _observation_preview_count(sources: Mapping[str, Any]) -> int:
    update_doc = _as_mapping(sources.get("decision_update_2396"))
    update = _as_mapping(update_doc.get("decision_update"))
    return len(_as_list(update.get("observation_preview_candidates")))


def _component_decisions(matrix_doc: Mapping[str, Any]) -> dict[str, str]:
    decisions: dict[str, str] = {}
    for row in _as_list_of_mappings(matrix_doc.get("component_attribution_matrix")):
        component_name = row.get("component_name")
        decision = row.get("recommended_component_decision")
        if component_name and decision:
            decisions[str(component_name)] = str(decision)
    return decisions


def _source_document_names() -> tuple[str, ...]:
    return (
        "recombination_retest_result_2396",
        "recombination_candidate_ranking_2396",
        "component_evidence_matrix_2396",
        "decision_update_2396",
        "recombination_candidate_plan_2395",
        "candidate_definitions_2395",
        "owner_review_decision_2394",
        "component_recombination_decision_2394",
        "ablation_retest_result_2393",
        "component_attribution_matrix_2393",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _as_list_of_mappings(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in _as_list(value) if isinstance(item, Mapping)]


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
