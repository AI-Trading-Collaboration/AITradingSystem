from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_component_attribution_targeted_ablation_retest as m2393
import ai_trading_system.dynamic_strategy_component_recombination_candidate_plan as m2395
import ai_trading_system.dynamic_strategy_component_recombination_candidate_retest as m2396
import ai_trading_system.dynamic_strategy_recombination_candidate_owner_review_decision as m2397
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import (
    json_block as _json_block,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2398"
TASK_REGISTER_ID = (
    "TRADING-2398_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_"
    "TARGETED_IMPROVEMENT_PLAN"
)
REPORT_TYPE = "dynamic_strategy_recombination_candidate_gate_evidence_plan"
SCHEMA_VERSION = (
    "dynamic_strategy_recombination_candidate_gate_evidence_and_targeted_"
    "improvement_plan.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_"
    "IMPROVEMENT_PLAN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_"
    "IMPROVEMENT_PLAN_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2399_Dynamic_Strategy_Recombination_Candidate_Targeted_Gate_"
    "Evidence_Retest"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2393",
    "TRADING-2394",
    "TRADING-2395",
    "TRADING-2396",
    "TRADING-2397",
)
DIRECT_SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2393",
    "TRADING-2395",
    "TRADING-2396",
    "TRADING-2397",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA"
)
BEST_RECOMBINATION_CANDIDATE = m2397.BEST_RECOMBINATION_CANDIDATE
EXPECTED_DECISION_FROM_2396 = m2397.EXPECTED_DECISION_FROM_2396
OWNER_DECISION_FROM_2397 = m2397.OWNER_DECISION
# Sign boundary only: positive values mean the guardrail reduced turnover versus
# raw growth tilt, negative values mean the targeted retest must repair it.
TURNOVER_REDUCTION_SIGN_BOUNDARY = 0.0

TARGETED_VARIANTS: tuple[dict[str, Any], ...] = (
    {
        "candidate_id": "growth_tilt_guarded_transfer_time_slice_repair_v1",
        "base": BEST_RECOMBINATION_CANDIDATE,
        "purpose": "improve weak time slices without changing core return engine",
        "changes": [
            "tune_reentry_timing",
            "reduce_drawdown_recovery_lag",
            "preserve_valid_until_window",
            "preserve_lower_turnover_guardrail",
        ],
    },
    {
        "candidate_id": "growth_tilt_guarded_transfer_regime_repair_v1",
        "base": BEST_RECOMBINATION_CANDIDATE,
        "purpose": "improve behavior in weak regimes",
        "changes": [
            "condition_growth_tilt_on_trend_confirmed",
            "strengthen_high_volatility_risk_cap",
            "avoid_excessive_risk_off_defensiveness",
        ],
    },
    {
        "candidate_id": "growth_tilt_guarded_transfer_drawdown_calibrated_v1",
        "base": BEST_RECOMBINATION_CANDIDATE,
        "purpose": "reduce drawdown materiality gap",
        "changes": [
            "reduce_growth_tilt_intensity_under_high_volatility",
            "add_drawdown_sensitive_de_risking",
            "preserve_turnover_budget",
        ],
    },
    {
        "candidate_id": "growth_tilt_guarded_transfer_return_retention_v1",
        "base": BEST_RECOMBINATION_CANDIDATE,
        "purpose": "preserve more raw growth tilt upside while keeping guardrails",
        "changes": [
            "relax_guarded_transfer_only_under_trend_confirmed",
            "preserve_lower_turnover_guardrail",
            "preserve_no_stale_signal",
        ],
    },
    {
        "candidate_id": "growth_tilt_guarded_transfer_valid_until_strict_v1",
        "base": BEST_RECOMBINATION_CANDIDATE,
        "purpose": "strengthen signal validity evidence",
        "changes": [
            "strict_signal_expiry",
            "near_expiry_signal_decay",
            "block_stale_signal_carry_forward",
        ],
    },
    {
        "candidate_id": "growth_tilt_guarded_transfer_balanced_gate_v1",
        "base": BEST_RECOMBINATION_CANDIDATE,
        "purpose": "balanced candidate targeting observation preview gates",
        "changes": [
            "moderate_growth_tilt",
            "lower_turnover_guardrail",
            "strict_valid_until",
            "high_volatility_risk_cap",
            "cooldown_balancing",
        ],
    },
)

REFERENCE_CANDIDATES: tuple[str, ...] = (
    "static_baseline",
    "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    BEST_RECOMBINATION_CANDIDATE,
    "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
)
COMPARISON_CADENCES: tuple[str, ...] = (
    "valid_until_window",
    "cooldown_limited_event_driven",
    "signal_event_driven",
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

DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH = (
    m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2397_GATE_EVIDENCE_GAP_SUMMARY_PATH = (
    m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "gate_evidence_gap_summary.json"
)
DEFAULT_SOURCE_2397_OBSERVATION_NON_APPROVAL_RECORD_PATH = (
    m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "observation_non_approval_record.json"
)
DEFAULT_SOURCE_2397_NEXT_ROUTE_PATH = (
    m2397.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "next_route.json"
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
DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "ablation_retest_result.json"
)
DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH = (
    m2393.DEFAULT_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_OUTPUT_ROOT
    / "component_attribution_matrix.json"
)


def run_dynamic_strategy_recombination_candidate_gate_evidence_plan(
    *,
    source_owner_review_decision_2397_path: Path = (
        DEFAULT_SOURCE_2397_OWNER_REVIEW_DECISION_PATH
    ),
    source_gate_evidence_gap_summary_2397_path: Path = (
        DEFAULT_SOURCE_2397_GATE_EVIDENCE_GAP_SUMMARY_PATH
    ),
    source_observation_non_approval_record_2397_path: Path = (
        DEFAULT_SOURCE_2397_OBSERVATION_NON_APPROVAL_RECORD_PATH
    ),
    source_next_route_2397_path: Path = DEFAULT_SOURCE_2397_NEXT_ROUTE_PATH,
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
    source_ablation_retest_result_2393_path: Path = (
        DEFAULT_SOURCE_2393_ABLATION_RETEST_RESULT_PATH
    ),
    source_component_attribution_matrix_2393_path: Path = (
        DEFAULT_SOURCE_2393_COMPONENT_ATTRIBUTION_MATRIX_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_owner_review_decision_2397_path=source_owner_review_decision_2397_path,
        source_gate_evidence_gap_summary_2397_path=(
            source_gate_evidence_gap_summary_2397_path
        ),
        source_observation_non_approval_record_2397_path=(
            source_observation_non_approval_record_2397_path
        ),
        source_next_route_2397_path=source_next_route_2397_path,
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
    sources["source_ready_for_gate_evidence_plan"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "owner_review_decision_2397": m2397.READY_STATUS,
        "gate_evidence_gap_summary_2397": m2397.READY_STATUS,
        "observation_non_approval_record_2397": m2397.READY_STATUS,
        "next_route_2397": m2397.READY_STATUS,
        "recombination_retest_result_2396": m2396.READY_STATUS,
        "recombination_candidate_ranking_2396": m2396.READY_STATUS,
        "component_evidence_matrix_2396": m2396.READY_STATUS,
        "decision_update_2396": m2396.READY_STATUS,
        "recombination_candidate_plan_2395": m2395.READY_STATUS,
        "candidate_definitions_2395": m2395.READY_STATUS,
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

    _validate_2397_owner_decision(sources, errors)
    _validate_2396_retest(sources, errors)
    _validate_2395_plan(sources, errors)
    _validate_2393_component_context(sources, errors)
    _validate_source_safety(sources, errors)
    return errors


def _validate_2397_owner_decision(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    owner = _as_mapping(sources.get("owner_review_decision_2397"))
    gap_doc = _as_mapping(sources.get("gate_evidence_gap_summary_2397"))
    gap_summary = _as_mapping(gap_doc.get("gate_evidence_gap_summary"))
    non_approval_doc = _as_mapping(
        sources.get("observation_non_approval_record_2397")
    )
    non_approval = _as_mapping(
        non_approval_doc.get("observation_non_approval_record")
    )
    next_route_doc = _as_mapping(sources.get("next_route_2397"))

    if owner.get("owner_decision") != OWNER_DECISION_FROM_2397:
        errors.append("2397 owner decision mismatch")
    if owner.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2397 best recombination candidate mismatch")
    if owner.get("best_recombination_decision_from_2396") != EXPECTED_DECISION_FROM_2396:
        errors.append("2397 decision from 2396 mismatch")
    if owner.get("research_only_observation_approved") is True:
        errors.append("2397 unexpectedly approved research-only observation")
    if owner.get("gate_evidence_gap_summary_ready") is not True:
        errors.append("2397 gate evidence gap summary is not ready")
    if owner.get("recommended_next_research_task") != m2397.NEXT_ROUTE:
        errors.append("2397 recommended route mismatch")
    if gap_summary.get("record_ready") is not True:
        errors.append("2397 gap summary record not ready")
    if gap_summary.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2397 gap summary candidate mismatch")
    if non_approval.get("research_only_observation_approved") is True:
        errors.append("2397 non-approval record unexpectedly approved observation")
    if non_approval.get("owner_decision") != OWNER_DECISION_FROM_2397:
        errors.append("2397 non-approval owner decision mismatch")
    if next_route_doc.get("recommended_next_research_task") != m2397.NEXT_ROUTE:
        errors.append("2397 next route artifact mismatch")


def _validate_2396_retest(sources: Mapping[str, Any], errors: list[str]) -> None:
    retest = _as_mapping(sources.get("recombination_retest_result_2396"))
    ranking_doc = _as_mapping(sources.get("recombination_candidate_ranking_2396"))
    update_doc = _as_mapping(sources.get("decision_update_2396"))
    update = _as_mapping(update_doc.get("decision_update"))
    ranking_row = _ranking_row(sources)
    evidence_row = _component_evidence_row(sources)

    if retest.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 best recombination candidate mismatch")
    if retest.get("best_recombination_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 best recombination decision mismatch")
    if retest.get("recommended_next_research_task") != m2396.NEXT_ROUTE:
        errors.append("2396 route did not point to TRADING-2397")
    if ranking_doc.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 ranking best candidate mismatch")
    if ranking_row.get("decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 ranking row decision mismatch")
    if evidence_row.get("candidate_id") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 component evidence missing best candidate")
    quality = _as_mapping(evidence_row.get("recombination_quality"))
    if quality.get("candidate_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 component evidence decision mismatch")
    if update.get("best_recombination_candidate") != BEST_RECOMBINATION_CANDIDATE:
        errors.append("2396 decision update best candidate mismatch")
    if update.get("best_recombination_decision") != EXPECTED_DECISION_FROM_2396:
        errors.append("2396 decision update best decision mismatch")
    if update.get("research_only_observation_preview_exists") is True:
        errors.append("2396 unexpectedly found observation preview candidate")
    if _observation_preview_count(sources) != 0:
        errors.append("2396 observation preview candidate count is not zero")


def _validate_2395_plan(sources: Mapping[str, Any], errors: list[str]) -> None:
    plan = _as_mapping(sources.get("recombination_candidate_plan_2395"))
    definitions_doc = _as_mapping(sources.get("candidate_definitions_2395"))
    definitions = _as_list_of_mappings(
        definitions_doc.get("recombination_candidate_definitions")
    )
    if plan.get("recommended_next_research_task") != m2395.NEXT_ROUTE:
        errors.append("2395 plan route did not point to TRADING-2396")
    if plan.get("recombination_candidate_plan_ready") is not True:
        errors.append("2395 recombination candidate plan is not ready")
    if BEST_RECOMBINATION_CANDIDATE not in {
        str(item) for item in _as_list(plan.get("planned_recombination_candidates"))
    }:
        errors.append("2395 planned candidates missing best recombination candidate")
    best_definition = next(
        (
            row
            for row in definitions
            if row.get("candidate_id") == BEST_RECOMBINATION_CANDIDATE
        ),
        {},
    )
    if not best_definition:
        errors.append("2395 candidate definitions missing best recombination candidate")
    elif best_definition.get("owner_review_required") is not True:
        errors.append("2395 best candidate definition is not owner-review-required")


def _validate_2393_component_context(
    sources: Mapping[str, Any],
    errors: list[str],
) -> None:
    retest = _as_mapping(sources.get("ablation_retest_result_2393"))
    matrix_doc = _as_mapping(sources.get("component_attribution_matrix_2393"))
    decisions = _component_decisions(matrix_doc)
    if retest.get("recommended_next_research_task") != m2393.NEXT_ROUTE:
        errors.append("2393 route did not point to TRADING-2394")
    if retest.get("best_reusable_component") != m2395.RETURN_ENGINE_COMPONENT:
        errors.append("2393 best reusable component mismatch")
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
    as_of = as_of_date.isoformat() if as_of_date else None
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "direct_source_tasks": list(DIRECT_SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_gate_evidence_plan": bool(
            sources.get("source_ready_for_gate_evidence_plan")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY",
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "candidate_under_review": BEST_RECOMBINATION_CANDIDATE,
        "best_recombination_candidate": BEST_RECOMBINATION_CANDIDATE,
        "decision_from_2396": EXPECTED_DECISION_FROM_2396,
        "owner_decision_from_2397": OWNER_DECISION_FROM_2397,
        "recommended_next_research_task": NEXT_ROUTE,
        "manual_review_required": True,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "current_best_candidate_observation_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_store_mutated": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "production_approved": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _candidate_metrics(sources)
    gate_gap_summary = _gate_evidence_gap_summary(metrics, sources)
    targeted_plan = _targeted_improvement_plan(gate_gap_summary)
    retest_plan = _retest_plan_2399(targeted_plan)
    return {
        "gate_evidence_gap_summary_ready": True,
        "targeted_improvement_plan_ready": True,
        "retest_plan_2399_ready": True,
        "planned_targeted_variants": [
            row["candidate_id"] for row in targeted_plan["targeted_variants"]
        ],
        "candidate_under_review_context": _candidate_under_review_context(sources),
        "source_findings": _source_findings(sources),
        "candidate_metrics": metrics,
        "gate_evidence_gap_summary": gate_gap_summary,
        "targeted_improvement_plan": targeted_plan,
        "retest_plan_2399": retest_plan,
        "acceptance_criteria_2399": retest_plan["acceptance_criteria"],
        "next_route": _next_route_record(),
        "guardrail_summary": _guardrail_summary(),
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "gate_evidence_gap_summary_ready": False,
        "targeted_improvement_plan_ready": False,
        "retest_plan_2399_ready": False,
        "planned_targeted_variants": [],
        "candidate_under_review_context": {},
        "source_findings": _source_findings(sources),
        "candidate_metrics": {},
        "gate_evidence_gap_summary": {
            "record_ready": False,
            "blocking_reason": "source artifacts failed validation",
            "source_validation_errors": list(
                sources.get("source_validation_errors", [])
            ),
        },
        "targeted_improvement_plan": {"record_ready": False},
        "retest_plan_2399": {"record_ready": False},
        "acceptance_criteria_2399": {},
        "next_route": _next_route_record(),
        "guardrail_summary": _guardrail_summary(),
    }


def _candidate_metrics(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_row = _ranking_row(sources)
    evidence_row = _component_evidence_row(sources)
    quality = _as_mapping(evidence_row.get("recombination_quality"))
    guardrail = _as_mapping(evidence_row.get("guardrail_metrics"))
    return_engine = _as_mapping(evidence_row.get("return_engine_metrics"))
    valid_until = _as_mapping(evidence_row.get("valid_until_metrics"))
    return {
        "time_slice_pass_rate": _first_float(
            ranking_row.get("time_slice_pass_rate"),
            quality.get("time_slice_pass_rate"),
        ),
        "regime_expectation_score": _first_float(
            ranking_row.get("regime_expectation_score"),
            quality.get("regime_expectation_score"),
        ),
        "drawdown_gap_vs_static": _to_float(guardrail.get("drawdown_gap_vs_static")),
        "max_drawdown": _to_float(ranking_row.get("max_drawdown")),
        "return_per_drawdown_penalty": _to_float(
            quality.get("return_per_drawdown_penalty")
        ),
        "return_retention_vs_raw_growth_tilt": _first_float(
            ranking_row.get("return_retention_vs_raw_growth_tilt"),
            return_engine.get("return_retention_vs_raw_growth_tilt"),
        ),
        "return_gap_vs_raw_growth_tilt": round(
            1.0
            - _first_float(
                ranking_row.get("return_retention_vs_raw_growth_tilt"),
                return_engine.get("return_retention_vs_raw_growth_tilt"),
            ),
            6,
        ),
        "upside_capture_gap": round(
            1.0 - _to_float(return_engine.get("upside_capture")),
            6,
        ),
        "turnover": _to_float(ranking_row.get("turnover")),
        "turnover_reduction_vs_raw_growth_tilt": _first_float(
            ranking_row.get("turnover_reduction_vs_raw_growth_tilt"),
            guardrail.get("turnover_reduction_vs_raw_growth_tilt"),
        ),
        "cost_drag_reduction": _to_float(guardrail.get("cost_drag_reduction")),
        "realistic_cost_status": _pass_fail(guardrail.get("realistic_cost_passed")),
        "conservative_cost_status": _pass_fail(
            guardrail.get("conservative_cost_passed")
        ),
        "harsh_cost_status": _pass_fail(guardrail.get("harsh_cost_passed")),
        "cost_stress_survival": str(ranking_row.get("cost_stress_survival")),
        "stale_signal_execution_count": _first_float(
            ranking_row.get("stale_signal_execution_count"),
            valid_until.get("stale_signal_execution_count"),
        ),
        "signal_to_execution_lag_days": _to_float(
            valid_until.get("signal_to_execution_lag_days")
        ),
        "no_stale_signal_carry_forward": bool(
            ranking_row.get("no_stale_signal_carry_forward")
            and valid_until.get("no_stale_signal_carry_forward")
        ),
        "valid_until_window_preserved": bool(
            ranking_row.get("valid_until_window_preserved")
            and valid_until.get("valid_until_window_preserved")
        ),
        "near_expiry_signal_behavior": valid_until.get("near_expiry_signal_behavior"),
    }


def _gate_evidence_gap_summary(
    metrics: Mapping[str, Any],
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    time_slice_status = (
        "GAP_REMAINS"
        if _to_float(metrics.get("time_slice_pass_rate"))
        < m2396.PREVIEW_TIME_SLICE_PASS_RATE_MIN
        else "NO_GAP_DETECTED"
    )
    regime_status = (
        "GAP_REMAINS"
        if _to_float(metrics.get("regime_expectation_score"))
        < m2396.PREVIEW_REGIME_EXPECTATION_SCORE_MIN
        else "NO_GAP_DETECTED"
    )
    turnover_status = (
        "GAP_REMAINS"
        if _to_float(metrics.get("turnover_reduction_vs_raw_growth_tilt"))
        < TURNOVER_REDUCTION_SIGN_BOUNDARY
        else "NO_GAP_DETECTED"
    )
    valid_until_status = (
        "PASS"
        if metrics.get("no_stale_signal_carry_forward") is True
        and _to_float(metrics.get("stale_signal_execution_count")) == 0.0
        and metrics.get("valid_until_window_preserved") is True
        else "GAP_REMAINS"
    )
    return {
        "record_ready": True,
        "source_task": TASK_ID,
        "candidate_under_review": BEST_RECOMBINATION_CANDIDATE,
        "decision_from_2396": EXPECTED_DECISION_FROM_2396,
        "owner_decision_from_2397": OWNER_DECISION_FROM_2397,
        "observation_approved": False,
        "source_2397_gap_summary": _as_mapping(
            _as_mapping(sources.get("gate_evidence_gap_summary_2397")).get(
                "gate_evidence_gap_summary"
            )
        ),
        "gap_areas": {
            "time_slice_evidence_gap": {
                "status": time_slice_status,
                "affected_time_slices": [
                    "full_available_window",
                    "recent_period",
                    "post_2023_ai_cycle",
                    "high_volatility_periods",
                    "drawdown_recovery_periods",
                ],
                "likely_failure_reason": (
                    "aggregate time_slice_pass_rate remains below 2396 preview "
                    "reference; targeted retest must isolate weak slices"
                ),
                "source_value": metrics.get("time_slice_pass_rate"),
                "reference_from_2396": m2396.PREVIEW_TIME_SLICE_PASS_RATE_MIN,
                "improvement_direction": [
                    "tune_reentry_timing",
                    "reduce_drawdown_recovery_lag",
                    "preserve_valid_until_window",
                ],
                "retest_required": True,
            },
            "regime_expectation_gap": {
                "status": regime_status,
                "affected_regimes": [
                    "risk_on",
                    "risk_off",
                    "high_volatility",
                    "trend_confirmed",
                    "recovery",
                ],
                "expected_behavior": [
                    "retain risk-on upside",
                    "avoid risk-off deterioration",
                    "control high-volatility drawdown",
                    "capture trend-confirmed growth tilt",
                ],
                "observed_issue": (
                    "regime_expectation_score remains below 2396 preview reference"
                ),
                "source_value": metrics.get("regime_expectation_score"),
                "reference_from_2396": m2396.PREVIEW_REGIME_EXPECTATION_SCORE_MIN,
                "improvement_direction": [
                    "condition_growth_tilt_on_trend_confirmed",
                    "strengthen_high_volatility_risk_cap",
                    "avoid_excessive_risk_off_defensiveness",
                ],
                "retest_required": True,
            },
            "drawdown_materiality_gap": {
                "status": "OWNER_JUDGMENT_REQUIRED",
                "drawdown_gap_vs_static": metrics.get("drawdown_gap_vs_static"),
                "return_per_drawdown_penalty": metrics.get(
                    "return_per_drawdown_penalty"
                ),
                "drawdown_materiality_tier": (
                    "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST"
                ),
                "targeted_fix": [
                    "reduce_growth_tilt_intensity_under_high_volatility",
                    "add_drawdown_sensitive_de_risking",
                    "preserve_turnover_budget",
                ],
                "retest_required": True,
            },
            "return_retention_gap": {
                "status": (
                    "ADEQUATE_BUT_MONITOR"
                    if _to_float(metrics.get("return_retention_vs_raw_growth_tilt"))
                    >= m2396.OWNER_REVIEW_RETURN_RETENTION_MIN
                    else "GAP_REMAINS"
                ),
                "return_retention_vs_raw_growth_tilt": metrics.get(
                    "return_retention_vs_raw_growth_tilt"
                ),
                "return_gap_vs_raw_growth_tilt": metrics.get(
                    "return_gap_vs_raw_growth_tilt"
                ),
                "upside_capture_gap": metrics.get("upside_capture_gap"),
                "improvement_direction": [
                    "preserve_more_raw_growth_tilt_upside",
                    "relax_guarded_transfer_only_under_trend_confirmed",
                ],
                "retest_required": True,
            },
            "turnover_cost_evidence_gap": {
                "status": turnover_status,
                "turnover_reduction_vs_raw_growth_tilt": metrics.get(
                    "turnover_reduction_vs_raw_growth_tilt"
                ),
                "cost_drag_reduction": metrics.get("cost_drag_reduction"),
                "realistic_cost_status": metrics.get("realistic_cost_status"),
                "conservative_cost_status": metrics.get("conservative_cost_status"),
                "harsh_cost_status": metrics.get("harsh_cost_status"),
                "improvement_direction": [
                    "repair guarded transfer turnover behavior",
                    "keep realistic and conservative cost survival visible",
                ],
                "retest_required": True,
            },
            "valid_until_stale_signal_gap": {
                "status": valid_until_status,
                "stale_signal_execution_count": metrics.get(
                    "stale_signal_execution_count"
                ),
                "signal_to_execution_lag_days": metrics.get(
                    "signal_to_execution_lag_days"
                ),
                "no_stale_signal_carry_forward": metrics.get(
                    "no_stale_signal_carry_forward"
                ),
                "valid_until_window_preserved": metrics.get(
                    "valid_until_window_preserved"
                ),
                "valid_until_fix_required": valid_until_status != "PASS",
                "improvement_direction": [
                    "strict_signal_expiry",
                    "near_expiry_signal_decay",
                    "block_stale_signal_carry_forward",
                ],
                "retest_required": True,
            },
        },
        "blocking_summary": [
            "time_slice evidence must improve before observation preview",
            "regime expectation score must improve before observation preview",
            "drawdown materiality remains owner-review evidence",
            "turnover guardrail behavior must be repaired or explained",
            "return retention is adequate but must remain visible during targeted retest",
            "valid-until evidence currently passes but must be preserved in 2399",
        ],
    }


def _targeted_improvement_plan(
    gate_gap_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "record_ready": True,
        "candidate_under_review": BEST_RECOMBINATION_CANDIDATE,
        "source_gap_summary": dict(_as_mapping(gate_gap_summary.get("gap_areas"))),
        "targeted_variants": [dict(row) for row in TARGETED_VARIANTS],
        "variant_count": len(TARGETED_VARIANTS),
        "shared_constraints": {
            "base_candidate": BEST_RECOMBINATION_CANDIDATE,
            "preserve_return_engine": "growth_tilt_engine",
            "preserve_lower_turnover_guardrail": True,
            "preserve_valid_until_window": True,
            "block_stale_signal_carry_forward": True,
            "monthly_rebalance_allowed_for_primary_decision": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _retest_plan_2399(targeted_plan: Mapping[str, Any]) -> dict[str, Any]:
    targeted_ids = [
        str(row.get("candidate_id"))
        for row in _as_list_of_mappings(targeted_plan.get("targeted_variants"))
    ]
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "candidate_under_review": BEST_RECOMBINATION_CANDIDATE,
        "required_2399_candidates": {
            "reference": list(REFERENCE_CANDIDATES),
            "targeted_variants": targeted_ids,
        },
        "primary_execution_cadence": "valid_until_window",
        "comparison_cadences": list(COMPARISON_CADENCES),
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "stress_tests": {
            "cost": ["base", "realistic", "conservative", "harsh"],
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
            "gate_evidence": [
                "time_slice_pass_rate",
                "regime_expectation_score",
                "return_per_drawdown_penalty",
                "stale_signal_execution_count",
                "turnover_budget_passed",
            ],
        },
        "acceptance_criteria": {
            "owner_review_candidate_criteria": {
                "must": [
                    "cost_adjusted_return_above_static",
                    "survives_realistic_cost",
                    "survives_conservative_cost",
                    "valid_until_window_preserved",
                    "no_stale_signal_carry_forward",
                    "drawdown_tradeoff_explainable",
                ],
                "should": [
                    "improve_time_slice_evidence_vs_base_recombination_candidate",
                    "improve_regime_expectation_score_vs_base_recombination_candidate",
                    "preserve_meaningful_return_retention",
                    "not_materially_increase_turnover",
                    "not_materially_worsen_high_volatility_behavior",
                ],
                "must_not": [
                    "rely_on_monthly_rebalance",
                    "require_scheduler",
                    "require_event_append",
                    "require_outcome_binding",
                    "require_paper_shadow",
                    "require_production_or_broker",
                ],
            },
            "observation_preview_extra_criteria": [
                "time_slice_evidence_not_weak",
                "regime_expectation_score_not_weak",
                "drawdown_materiality_not_severe",
                "no_major_guardrail_failure",
            ],
        },
        "safety_boundary": _guardrail_summary(),
    }


def _candidate_under_review_context(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_row = _ranking_row(sources)
    evidence_row = _component_evidence_row(sources)
    return {
        "candidate_id": BEST_RECOMBINATION_CANDIDATE,
        "source_task": "TRADING-2396",
        "decision_from_2396": EXPECTED_DECISION_FROM_2396,
        "owner_decision_from_2397": OWNER_DECISION_FROM_2397,
        "observation_approved": False,
        "components": {
            "return_engine": ["growth_tilt_engine"],
            "guardrails": [
                "lower_turnover_guardrail",
                "valid_until_window",
                "no_stale_signal_carry_forward",
            ],
            "owner_review_component": ["guarded_turnover_transfer"],
            "source_components": list(_as_list(evidence_row.get("components"))),
        },
        "ranking_row": ranking_row,
        "component_evidence_row": evidence_row,
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    owner_2397 = _as_mapping(sources.get("owner_review_decision_2397"))
    retest_2396 = _as_mapping(sources.get("recombination_retest_result_2396"))
    plan_2395 = _as_mapping(sources.get("recombination_candidate_plan_2395"))
    ablation_2393 = _as_mapping(sources.get("ablation_retest_result_2393"))
    return {
        "trading_2397": {
            "status": owner_2397.get("status"),
            "owner_decision": owner_2397.get("owner_decision"),
            "best_recombination_candidate": owner_2397.get(
                "best_recombination_candidate"
            ),
            "decision_from_2396": owner_2397.get(
                "best_recombination_decision_from_2396"
            ),
            "observation_preview_candidates_count": owner_2397.get(
                "observation_preview_candidates_count"
            ),
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
            "data_quality_status": retest_2396.get("data_quality_status"),
            "research_only_observation_approved": retest_2396.get(
                "research_only_observation_approved"
            ),
        },
        "trading_2395": {
            "status": plan_2395.get("status"),
            "return_engine_component": plan_2395.get("return_engine_component"),
            "guardrail_components": plan_2395.get("guardrail_components"),
            "owner_review_components": plan_2395.get("owner_review_components"),
            "planned_recombination_candidate_count": len(
                _as_list(plan_2395.get("planned_recombination_candidates"))
            ),
        },
        "trading_2393": {
            "status": ablation_2393.get("status"),
            "best_reusable_component": ablation_2393.get("best_reusable_component"),
            "data_quality_status": ablation_2393.get("data_quality_status"),
            "research_only_observation_approved": ablation_2393.get(
                "research_only_observation_approved"
            ),
        },
    }


def _next_route_record() -> dict[str, Any]:
    return {
        "record_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": (
            "TRADING-2398 is plan-only; TRADING-2399 must run targeted gate "
            "evidence retest before observation preview can be reconsidered"
        ),
        "candidate_under_review": BEST_RECOMBINATION_CANDIDATE,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "task_boundary": "PLAN_ONLY_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT",
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
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
        "json_path": str(output_root / "gate_evidence_plan_result.json"),
        "gate_evidence_gap_summary_json": str(
            output_root / "gate_evidence_gap_summary.json"
        ),
        "targeted_improvement_plan_json": str(
            output_root / "targeted_improvement_plan.json"
        ),
        "retest_plan_2399_json": str(output_root / "retest_plan_2399.json"),
        "next_route_json": str(output_root / "next_route.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_recombination_candidate_gate_evidence_plan.md"
        ),
        "gate_evidence_gap_summary_markdown": str(
            docs_root / "dynamic_strategy_recombination_gate_evidence_gap_summary.md"
        ),
        "targeted_improvement_plan_markdown": str(
            docs_root / "dynamic_strategy_recombination_targeted_improvement_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2399_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["gate_evidence_gap_summary_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_gate_evidence_gap_summary",
            "schema_version": (
                "dynamic_strategy_recombination_gate_evidence_gap_summary.v2"
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
        Path(paths["targeted_improvement_plan_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_targeted_improvement_plan",
            "schema_version": (
                "dynamic_strategy_recombination_targeted_improvement_plan.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "targeted_improvement_plan": payload.get("targeted_improvement_plan", {}),
            "targeted_improvement_plan_ready": payload.get(
                "targeted_improvement_plan_ready"
            ),
            "planned_targeted_variants": payload.get("planned_targeted_variants", []),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["retest_plan_2399_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_recombination_retest_plan_2399",
            "schema_version": "dynamic_strategy_recombination_retest_plan_2399.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "retest_plan_2399": payload.get("retest_plan_2399", {}),
            "retest_plan_2399_ready": payload.get("retest_plan_2399_ready"),
            "recommended_next_research_task": payload.get(
                "recommended_next_research_task"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_route_json"]),
        {
            "task_id": TASK_ID,
            "report_type": "dynamic_strategy_2399_route",
            "schema_version": "dynamic_strategy_2399_route.v1",
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
        Path(paths["gate_evidence_gap_summary_markdown"]),
        _gap_summary_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["targeted_improvement_plan_markdown"]),
        _targeted_improvement_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    gap_summary = _as_mapping(payload.get("gate_evidence_gap_summary"))
    targeted_plan = _as_mapping(payload.get("targeted_improvement_plan"))
    retest_plan = _as_mapping(payload.get("retest_plan_2399"))
    return "\n".join(
        [
            "# Dynamic strategy recombination candidate gate evidence plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as_of：`{payload.get('as_of')}`",
            f"- candidate under review：`{payload.get('candidate_under_review')}`",
            f"- decision from 2396：`{payload.get('decision_from_2396')}`",
            f"- owner decision from 2397：`{payload.get('owner_decision_from_2397')}`",
            "- research-only observation approved："
            f"`{payload.get('research_only_observation_approved')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2396 / 2397",
            "",
            "```json",
            _json_block(payload.get("source_findings")),
            "```",
            "",
            "## Candidate under review",
            "",
            "```json",
            _json_block(payload.get("candidate_under_review_context")),
            "```",
            "",
            "## Gate evidence gaps",
            "",
            _gap_table(gap_summary.get("gap_areas")),
            "",
            "## Targeted improvement variants",
            "",
            _variant_table(targeted_plan.get("targeted_variants")),
            "",
            "## 2399 retest plan",
            "",
            "```json",
            _json_block(retest_plan),
            "```",
            "",
            "## Acceptance criteria",
            "",
            "```json",
            _json_block(payload.get("acceptance_criteria_2399")),
            "```",
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
            "## Guardrail summary",
            "",
            "```json",
            _json_block(payload.get("guardrail_summary")),
            "```",
            "",
            "## Recommended next route",
            "",
            f"- `{payload.get('recommended_next_research_task')}`",
            "",
        ]
    )


def _gap_summary_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("gate_evidence_gap_summary"))
    return "\n".join(
        [
            "# Dynamic strategy recombination gate evidence gap summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- candidate：`{summary.get('candidate_under_review')}`",
            f"- record ready：`{summary.get('record_ready')}`",
            "",
            _gap_table(summary.get("gap_areas")),
            "",
            "## Blocking summary",
            "",
            *[f"- {item}" for item in summary.get("blocking_summary", [])],
            "",
        ]
    )


def _targeted_improvement_markdown(payload: Mapping[str, Any]) -> str:
    plan = _as_mapping(payload.get("targeted_improvement_plan"))
    return "\n".join(
        [
            "# Dynamic strategy recombination targeted improvement plan",
            "",
            f"- status：`{payload.get('status')}`",
            f"- candidate：`{plan.get('candidate_under_review')}`",
            f"- variant count：`{plan.get('variant_count')}`",
            "",
            _variant_table(plan.get("targeted_variants")),
            "",
            "## Shared constraints",
            "",
            "```json",
            _json_block(plan.get("shared_constraints")),
            "```",
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    route = _as_mapping(payload.get("next_route"))
    return "\n".join(
        [
            "# Dynamic strategy 2399 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{route.get('recommended_next_research_task')}`",
            f"- route reason：{route.get('route_reason')}",
            "- research-only observation approved："
            f"`{route.get('research_only_observation_approved')}`",
            f"- paper-shadow enabled：`{route.get('paper_shadow_enabled')}`",
            f"- event append enabled：`{route.get('event_append_enabled')}`",
            f"- outcome binding enabled：`{route.get('outcome_binding_enabled')}`",
            f"- scheduler enabled：`{route.get('scheduler_enabled')}`",
            f"- production enabled：`{route.get('production_enabled')}`",
            f"- broker action enabled：`{route.get('broker_action_enabled')}`",
            f"- daily report generated：`{route.get('daily_report_generated')}`",
            "",
            "TRADING-2399 is the first step allowed to run targeted gate evidence retest. "
            "TRADING-2398 itself does not approve observation or execution.",
            "",
        ]
    )


def _gap_table(value: Any) -> str:
    gaps = _as_mapping(value)
    lines = [
        "|Gap|Status|Retest required|Improvement direction|",
        "|---|---|---|---|",
    ]
    for key, row_value in gaps.items():
        row = _as_mapping(row_value)
        direction = ", ".join(str(item) for item in _as_list(row.get("improvement_direction")))
        if not direction and row.get("targeted_fix"):
            direction = ", ".join(str(item) for item in _as_list(row.get("targeted_fix")))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{key}`",
                    f"`{row.get('status')}`",
                    f"`{row.get('retest_required')}`",
                    direction,
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _variant_table(value: Any) -> str:
    lines = ["|Candidate|Purpose|Changes|", "|---|---|---|"]
    for row in _as_list_of_mappings(value):
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{row.get('candidate_id')}`",
                    str(row.get("purpose")),
                    ", ".join(str(item) for item in _as_list(row.get("changes"))),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


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
        "owner_review_decision_2397",
        "gate_evidence_gap_summary_2397",
        "observation_non_approval_record_2397",
        "next_route_2397",
        "recombination_retest_result_2396",
        "recombination_candidate_ranking_2396",
        "component_evidence_matrix_2396",
        "decision_update_2396",
        "recombination_candidate_plan_2395",
        "candidate_definitions_2395",
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


def _first_float(*values: Any) -> float:
    for value in values:
        if isinstance(value, int | float):
            return float(value)
    return 0.0


def _pass_fail(value: Any) -> str:
    if value is True:
        return "PASS"
    if value is False:
        return "FAIL"
    return "UNKNOWN"
