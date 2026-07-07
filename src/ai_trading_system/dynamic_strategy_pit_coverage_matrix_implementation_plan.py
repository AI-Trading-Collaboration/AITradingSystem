from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_data_pit_signal_quality_gap_review as m2402
import ai_trading_system.dynamic_strategy_pit_coverage_signal_construction_review as m2403
import ai_trading_system.dynamic_strategy_recombination_line_plateau_decision as m2401
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
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY

TASK_ID = "TRADING-2404"
TASK_REGISTER_ID = (
    "TRADING-2404_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN"
)
REPORT_TYPE = "dynamic_strategy_pit_coverage_matrix_implementation_plan"
SCHEMA_VERSION = "dynamic_strategy_pit_coverage_matrix_implementation_plan.v1"
READY_STATUS = "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_IMPLEMENTATION_PLAN_ONLY_NO_FRESH_"
    "MARKET_DATA"
)

SOURCE_TASKS: tuple[str, ...] = ("TRADING-2401", "TRADING-2402", "TRADING-2403")
BLOCKING_GAP_INPUTS: tuple[str, ...] = ("growth_tilt_engine", "valid_until_window")
EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "candidate_search_resume",
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
    "new_strategy_backtest",
    "new_trading_signal",
    "new_scoring",
    "clear_blocking_gap_without_evidence",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_search_allowed",
    "research_only_observation_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "candidate_search_resumed",
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
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
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_signal_generated",
    "scoring_run",
)

DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2403_REVIEW_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "pit_signal_review_result.json"
)
DEFAULT_SOURCE_2403_PIT_MATRIX_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "pit_coverage_matrix.json"
)
DEFAULT_SOURCE_2403_SIGNAL_REVIEW_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "signal_construction_review.json"
)
DEFAULT_SOURCE_2403_REGIME_REVIEW_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "regime_labeling_review.json"
)
DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "remediation_matrix.json"
)
DEFAULT_SOURCE_2403_THRESHOLD_GAP_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "threshold_meta_dataset_gap.json"
)
DEFAULT_SOURCE_2402_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "gap_review_result.json"
)
DEFAULT_SOURCE_2402_DATA_QUALITY_GAP_MATRIX_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "data_quality_gap_matrix.json"
)
DEFAULT_SOURCE_2402_PIT_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "pit_coverage_gap_review.json"
)
DEFAULT_SOURCE_2402_SIGNAL_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "signal_quality_gap_review.json"
)
DEFAULT_SOURCE_2402_REGIME_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "regime_labeling_gap_review.json"
)
DEFAULT_SOURCE_2402_THRESHOLD_GAP_REVIEW_PATH = (
    m2402.DEFAULT_DYNAMIC_STRATEGY_DATA_PIT_SIGNAL_QUALITY_GAP_REVIEW_OUTPUT_ROOT
    / "threshold_meta_dataset_gap_review.json"
)
DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH = (
    m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT
    / "plateau_decision_result.json"
)
DEFAULT_SOURCE_2401_NEXT_DIRECTION_PATH = (
    m2401.DEFAULT_DYNAMIC_STRATEGY_RECOMBINATION_LINE_PLATEAU_DECISION_OUTPUT_ROOT
    / "next_research_direction_decision.json"
)


def run_dynamic_strategy_pit_coverage_matrix_implementation_plan(
    *,
    source_review_2403_path: Path = DEFAULT_SOURCE_2403_REVIEW_PATH,
    source_pit_matrix_2403_path: Path = DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_signal_review_2403_path: Path = DEFAULT_SOURCE_2403_SIGNAL_REVIEW_PATH,
    source_regime_review_2403_path: Path = DEFAULT_SOURCE_2403_REGIME_REVIEW_PATH,
    source_remediation_matrix_2403_path: Path = (
        DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH
    ),
    source_threshold_gap_2403_path: Path = DEFAULT_SOURCE_2403_THRESHOLD_GAP_PATH,
    source_gap_review_2402_path: Path = DEFAULT_SOURCE_2402_GAP_REVIEW_PATH,
    source_data_quality_gap_matrix_2402_path: Path = (
        DEFAULT_SOURCE_2402_DATA_QUALITY_GAP_MATRIX_PATH
    ),
    source_pit_gap_review_2402_path: Path = DEFAULT_SOURCE_2402_PIT_GAP_REVIEW_PATH,
    source_signal_gap_review_2402_path: Path = (
        DEFAULT_SOURCE_2402_SIGNAL_GAP_REVIEW_PATH
    ),
    source_regime_gap_review_2402_path: Path = (
        DEFAULT_SOURCE_2402_REGIME_GAP_REVIEW_PATH
    ),
    source_threshold_gap_review_2402_path: Path = (
        DEFAULT_SOURCE_2402_THRESHOLD_GAP_REVIEW_PATH
    ),
    source_plateau_decision_2401_path: Path = DEFAULT_SOURCE_2401_PLATEAU_DECISION_PATH,
    source_next_direction_2401_path: Path = DEFAULT_SOURCE_2401_NEXT_DIRECTION_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_review_2403_path=source_review_2403_path,
        source_pit_matrix_2403_path=source_pit_matrix_2403_path,
        source_signal_review_2403_path=source_signal_review_2403_path,
        source_regime_review_2403_path=source_regime_review_2403_path,
        source_remediation_matrix_2403_path=source_remediation_matrix_2403_path,
        source_threshold_gap_2403_path=source_threshold_gap_2403_path,
        source_gap_review_2402_path=source_gap_review_2402_path,
        source_data_quality_gap_matrix_2402_path=(
            source_data_quality_gap_matrix_2402_path
        ),
        source_pit_gap_review_2402_path=source_pit_gap_review_2402_path,
        source_signal_gap_review_2402_path=source_signal_gap_review_2402_path,
        source_regime_gap_review_2402_path=source_regime_gap_review_2402_path,
        source_threshold_gap_review_2402_path=source_threshold_gap_review_2402_path,
        source_plateau_decision_2401_path=source_plateau_decision_2401_path,
        source_next_direction_2401_path=source_next_direction_2401_path,
    )
    validation_errors = _validate_sources(sources)
    ready = not validation_errors
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        sources=sources,
        as_of_date=as_of_date,
        source_validation_errors=validation_errors,
    )
    if ready:
        payload.update(_ready_sections(sources))
    else:
        payload.update(_blocked_sections())
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_review_2403_path: Path,
    source_pit_matrix_2403_path: Path,
    source_signal_review_2403_path: Path,
    source_regime_review_2403_path: Path,
    source_remediation_matrix_2403_path: Path,
    source_threshold_gap_2403_path: Path,
    source_gap_review_2402_path: Path,
    source_data_quality_gap_matrix_2402_path: Path,
    source_pit_gap_review_2402_path: Path,
    source_signal_gap_review_2402_path: Path,
    source_regime_gap_review_2402_path: Path,
    source_threshold_gap_review_2402_path: Path,
    source_plateau_decision_2401_path: Path,
    source_next_direction_2401_path: Path,
) -> dict[str, Any]:
    sources = {
        "review_2403": _load_json_document(source_review_2403_path),
        "pit_matrix_2403": _load_json_document(source_pit_matrix_2403_path),
        "signal_review_2403": _load_json_document(source_signal_review_2403_path),
        "regime_review_2403": _load_json_document(source_regime_review_2403_path),
        "remediation_matrix_2403": _load_json_document(
            source_remediation_matrix_2403_path
        ),
        "threshold_gap_2403": _load_json_document(source_threshold_gap_2403_path),
        "gap_review_2402": _load_json_document(source_gap_review_2402_path),
        "data_quality_gap_matrix_2402": _load_json_document(
            source_data_quality_gap_matrix_2402_path
        ),
        "pit_gap_review_2402": _load_json_document(source_pit_gap_review_2402_path),
        "signal_gap_review_2402": _load_json_document(source_signal_gap_review_2402_path),
        "regime_gap_review_2402": _load_json_document(source_regime_gap_review_2402_path),
        "threshold_gap_review_2402": _load_json_document(
            source_threshold_gap_review_2402_path
        ),
        "plateau_decision_2401": _load_json_document(source_plateau_decision_2401_path),
        "next_direction_2401": _load_json_document(source_next_direction_2401_path),
    }
    sources["source_paths"] = {
        "review_2403": str(source_review_2403_path),
        "pit_matrix_2403": str(source_pit_matrix_2403_path),
        "signal_review_2403": str(source_signal_review_2403_path),
        "regime_review_2403": str(source_regime_review_2403_path),
        "remediation_matrix_2403": str(source_remediation_matrix_2403_path),
        "threshold_gap_2403": str(source_threshold_gap_2403_path),
        "gap_review_2402": str(source_gap_review_2402_path),
        "data_quality_gap_matrix_2402": str(source_data_quality_gap_matrix_2402_path),
        "pit_gap_review_2402": str(source_pit_gap_review_2402_path),
        "signal_gap_review_2402": str(source_signal_gap_review_2402_path),
        "regime_gap_review_2402": str(source_regime_gap_review_2402_path),
        "threshold_gap_review_2402": str(source_threshold_gap_review_2402_path),
        "plateau_decision_2401": str(source_plateau_decision_2401_path),
        "next_direction_2401": str(source_next_direction_2401_path),
    }
    return sources


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_statuses = {
        "review_2403": m2403.READY_STATUS,
        "pit_matrix_2403": m2403.READY_STATUS,
        "signal_review_2403": m2403.READY_STATUS,
        "regime_review_2403": m2403.READY_STATUS,
        "remediation_matrix_2403": m2403.READY_STATUS,
        "threshold_gap_2403": m2403.READY_STATUS,
        "gap_review_2402": m2402.READY_STATUS,
        "data_quality_gap_matrix_2402": m2402.READY_STATUS,
        "pit_gap_review_2402": m2402.READY_STATUS,
        "signal_gap_review_2402": m2402.READY_STATUS,
        "regime_gap_review_2402": m2402.READY_STATUS,
        "threshold_gap_review_2402": m2402.READY_STATUS,
        "plateau_decision_2401": m2401.READY_STATUS,
        "next_direction_2401": m2401.READY_STATUS,
    }
    for source_name, expected in expected_statuses.items():
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing source artifact {source.get('_path')}")
        elif source.get("status") != expected:
            errors.append(
                f"{source_name}: expected status {expected}, observed {source.get('status')}"
            )
    review_2403 = _as_mapping(sources.get("review_2403"))
    gap_2402 = _as_mapping(sources.get("gap_review_2402"))
    plateau_2401 = _as_mapping(sources.get("plateau_decision_2401"))
    pit_rows = _pit_rows_from_sources(sources)
    blocking = _blocking_gap_rows(pit_rows)
    blocking_ids = {str(row.get("input_id")) for row in blocking}
    if review_2403.get("recommended_next_research_task") != m2403.NEXT_ROUTE:
        errors.append("2403 next route mismatch")
    if review_2403.get("candidate_search_resumed") is not False:
        errors.append("2403 candidate_search_resumed must be false")
    for input_id in BLOCKING_GAP_INPUTS:
        if input_id not in blocking_ids:
            errors.append(f"2403 blocking gap missing: {input_id}")
    if gap_2402.get("recommended_next_research_task") != m2402.NEXT_ROUTE:
        errors.append("2402 next route mismatch")
    if plateau_2401.get("owner_decision") != m2401.OWNER_DECISION:
        errors.append("2401 owner decision mismatch")
    _validate_source_safety(sources, errors)
    return errors


def _validate_source_safety(sources: Mapping[str, Any], errors: list[str]) -> None:
    for source_name in (
        "review_2403",
        "gap_review_2402",
        "plateau_decision_2401",
        "next_direction_2401",
    ):
        source = _as_mapping(sources.get(source_name))
        for field in SAFETY_FALSE_FIELDS:
            if source.get(field) is True:
                errors.append(f"{source_name}: safety field must be false: {field}")
        if source.get("broker_action") not in (None, "none"):
            errors.append(f"{source_name}: broker_action must be none")


def _base_payload(
    *,
    status: str,
    sources: Mapping[str, Any],
    as_of_date: date | None,
    source_validation_errors: list[str],
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
        "source_tasks": list(SOURCE_TASKS),
        "source_paths": dict(_as_mapping(sources.get("source_paths"))),
        "source_validation_errors": source_validation_errors,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "candidate_search_resumed": False,
        "manual_review_required": True,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "paper_shadow_approved": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_approved": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_approved": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "production_approved": False,
        "production_enabled": False,
        "broker_action": "none",
        "broker_action_enabled": False,
        "order_generated": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    pit_rows = _pit_rows_from_sources(sources)
    remediation_rows = _remediation_rows_from_sources(sources)
    blocker_summary = _current_blocker_summary(pit_rows, remediation_rows)
    registry_schema = _pit_input_registry_schema(pit_rows)
    gate_policy = _pit_gate_policy(blocker_summary)
    routes = _remediation_routes()
    implementation_plan = _implementation_plan(registry_schema, gate_policy, routes)
    return {
        "pit_matrix_implementation_plan_ready": True,
        "pit_input_registry_schema_ready": True,
        "pit_gate_policy_ready": True,
        "remediation_routes_ready": True,
        "current_blocker_summary_ready": True,
        "pit_matrix_implementation_plan": implementation_plan,
        "pit_input_registry_schema": registry_schema,
        "pit_gate_policy": gate_policy,
        "remediation_routes": routes,
        "current_blocker_summary": blocker_summary,
        "blocking_gaps": list(BLOCKING_GAP_INPUTS),
        "recommended_next_research_task": NEXT_ROUTE,
        "infrastructure_boundary": _infrastructure_boundary(),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "pit_matrix_implementation_plan_ready": False,
        "pit_input_registry_schema_ready": False,
        "pit_gate_policy_ready": False,
        "remediation_routes_ready": False,
        "current_blocker_summary_ready": False,
        "pit_matrix_implementation_plan": {},
        "pit_input_registry_schema": {},
        "pit_gate_policy": {},
        "remediation_routes": {},
        "current_blocker_summary": {},
        "blocking_gaps": [],
        "recommended_next_research_task": None,
        "infrastructure_boundary": {},
    }


def _pit_rows_from_sources(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    pit_wrapper = _as_mapping(sources.get("pit_matrix_2403"))
    rows = _as_list(pit_wrapper.get("pit_coverage_matrix"))
    if rows:
        return [dict(_as_mapping(row)) for row in rows]
    rows = _as_list(_as_mapping(sources.get("review_2403")).get("pit_coverage_matrix"))
    return [dict(_as_mapping(row)) for row in rows]


def _remediation_rows_from_sources(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    wrapper = _as_mapping(sources.get("remediation_matrix_2403"))
    rows = _as_list(wrapper.get("prioritized_remediation_matrix"))
    if rows:
        return [dict(_as_mapping(row)) for row in rows]
    rows = _as_list(
        _as_mapping(sources.get("review_2403")).get("prioritized_remediation_matrix")
    )
    return [dict(_as_mapping(row)) for row in rows]


def _blocking_gap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("severity") == "BLOCKING"]


def _current_blocker_summary(
    pit_rows: list[dict[str, Any]],
    remediation_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocker_rows = [
        row for row in pit_rows if str(row.get("input_id")) in BLOCKING_GAP_INPUTS
    ]
    blocker_details = {}
    for row in blocker_rows:
        input_id = str(row.get("input_id"))
        blocker_details[input_id] = {
            "severity": row.get("severity"),
            "pit_status": row.get("point_in_time_status"),
            "reason": row.get("recommended_action"),
            "candidate_search_blocked": True,
            "observation_blocked": True,
            "paper_shadow_blocked": True,
            "remediation_required": True,
        }
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_current_pit_blocker_summary.v1",
        "blocking_gaps": list(BLOCKING_GAP_INPUTS),
        "blocking_gap_details": blocker_details,
        "source_remediation_ids": [
            row.get("remediation_id")
            for row in remediation_rows
            if row.get("severity") == "BLOCKING"
        ],
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "reason": [
            "BLOCKING_GAP_GROWTH_TILT_ENGINE",
            "BLOCKING_GAP_VALID_UNTIL_WINDOW",
        ],
    }


def _pit_input_registry_schema(pit_rows: list[dict[str, Any]]) -> dict[str, Any]:
    schema = {
        "record_ready": True,
        "schema_version": "dynamic_strategy_pit_input_registry_schema.v1",
        "recommended_path": "config/research/dynamic_strategy_pit_input_registry.yaml",
        "fields": {
            "input_id": {"type": "string", "required": True},
            "input_type": {
                "type": "enum",
                "values": [
                    "MARKET_DATA",
                    "FEATURE",
                    "SIGNAL",
                    "EXECUTION_SEMANTIC",
                    "REGIME_LABEL",
                    "GATE_INPUT",
                    "REPORTING_INPUT",
                ],
                "required": True,
            },
            "owner_module": {"type": "string", "required": True},
            "source_artifact_or_config": {"type": "string", "required": True},
            "used_by": {"type": "list", "required": True},
            "as_of_field": {"type": "string", "required": False},
            "generated_at_field": {"type": "string", "required": False},
            "valid_from_field": {"type": "string", "required": False},
            "valid_until_field": {"type": "string", "required": False},
            "pit_status": {
                "type": "enum",
                "values": [
                    "TRUE_PIT",
                    "APPROXIMATE_PIT",
                    "NOT_PIT_SAFE",
                    "UNKNOWN",
                    "NOT_APPLICABLE",
                ],
                "required": True,
            },
            "pit_confidence": {
                "type": "enum",
                "values": ["HIGH", "MEDIUM", "LOW", "UNKNOWN"],
                "required": True,
            },
            "risk_flags": {
                "type": "list",
                "values": [
                    "LOOKAHEAD_RISK",
                    "REVISION_RISK",
                    "STALE_DATA_RISK",
                    "MISSING_DATA_RISK",
                    "VALID_UNTIL_UNGROUNDED",
                    "REGIME_LABEL_LOOKAHEAD_RISK",
                    "THRESHOLD_UNCALIBRATED",
                ],
                "required": False,
            },
            "severity": {
                "type": "enum",
                "values": ["BLOCKING", "MATERIAL", "MINOR", "INFO"],
                "required": True,
            },
            "remediation_owner": {"type": "string", "required": False},
            "recommended_action": {"type": "string", "required": True},
        },
        "planned_initial_entries": _planned_registry_entries(pit_rows),
    }
    return schema


def _planned_registry_entries(pit_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries = [_registry_entry_from_pit_row(row) for row in pit_rows]
    required = {
        "no_stale_signal_carry_forward": ("EXECUTION_SEMANTIC", "MATERIAL", "APPROXIMATE_PIT"),
        "signal_to_execution_lag": ("EXECUTION_SEMANTIC", "MATERIAL", "APPROXIMATE_PIT"),
        "regime_risk_on": ("REGIME_LABEL", "MATERIAL", "APPROXIMATE_PIT"),
        "regime_risk_off": ("REGIME_LABEL", "MATERIAL", "APPROXIMATE_PIT"),
        "regime_high_volatility": ("REGIME_LABEL", "MATERIAL", "APPROXIMATE_PIT"),
        "regime_recovery": ("REGIME_LABEL", "MATERIAL", "APPROXIMATE_PIT"),
        "time_slice_pass_rate": ("GATE_INPUT", "MATERIAL", "NOT_APPLICABLE"),
        "regime_expectation_score": ("GATE_INPUT", "MATERIAL", "NOT_APPLICABLE"),
        "drawdown_materiality": ("GATE_INPUT", "MATERIAL", "NOT_APPLICABLE"),
        "threshold_meta_dataset": ("GATE_INPUT", "MATERIAL", "NOT_APPLICABLE"),
    }
    existing = {entry["input_id"] for entry in entries}
    for input_id, (input_type, severity, pit_status) in required.items():
        if input_id in existing:
            continue
        entries.append(
            {
                "input_id": input_id,
                "input_type": input_type,
                "owner_module": "dynamic_strategy_pit_coverage_matrix",
                "source_artifact_or_config": "planned_registry_entry",
                "used_by": ["dynamic_strategy_research"],
                "pit_status": pit_status,
                "pit_confidence": "LOW" if severity == "MATERIAL" else "UNKNOWN",
                "risk_flags": _risk_flags_for(input_id, severity, pit_status),
                "severity": severity,
                "candidate_search_blocker": severity == "BLOCKING",
                "remediation_owner": "research_governance",
                "recommended_action": "include in reusable PIT registry implementation",
            }
        )
    return entries


def _registry_entry_from_pit_row(row: Mapping[str, Any]) -> dict[str, Any]:
    input_id = _text(row.get("input_id"))
    severity = _text(row.get("severity"), "MATERIAL")
    pit_status = _text(row.get("point_in_time_status"), "UNKNOWN")
    return {
        "input_id": input_id,
        "input_type": _registry_input_type(_text(row.get("input_type"))),
        "owner_module": "dynamic_strategy_pit_coverage_matrix",
        "source_artifact_or_config": row.get("source_artifact_or_config"),
        "used_by": row.get("used_by_tasks") or ["dynamic_strategy_research"],
        "as_of_field": "as_of",
        "generated_at_field": "generated_at",
        "valid_from_field": "valid_from" if "valid_until" in input_id else None,
        "valid_until_field": "valid_until" if "valid_until" in input_id else None,
        "pit_status": pit_status,
        "pit_confidence": row.get("pit_confidence", "UNKNOWN"),
        "risk_flags": _risk_flags_for(input_id, severity, pit_status),
        "severity": severity,
        "candidate_search_blocker": severity == "BLOCKING",
        "remediation_owner": "research_governance",
        "recommended_action": row.get("recommended_action"),
    }


def _registry_input_type(input_type: str) -> str:
    mapping = {
        "market_data": "MARKET_DATA",
        "technical_features": "FEATURE",
        "strategy_signals": "SIGNAL",
        "execution_semantics": "EXECUTION_SEMANTIC",
        "regime_labels": "REGIME_LABEL",
        "gate_inputs": "GATE_INPUT",
    }
    return mapping.get(input_type, "REPORTING_INPUT")


def _risk_flags_for(input_id: str, severity: str, pit_status: str) -> list[str]:
    flags: list[str] = []
    if pit_status in {"UNKNOWN", "NOT_PIT_SAFE"}:
        flags.append("LOOKAHEAD_RISK")
    if "valid_until" in input_id:
        flags.append("VALID_UNTIL_UNGROUNDED")
    if input_id.startswith("regime_") or input_id == "regime_labels":
        flags.append("REGIME_LABEL_LOOKAHEAD_RISK")
    if "threshold" in input_id or input_id.endswith("_pass_rate"):
        flags.append("THRESHOLD_UNCALIBRATED")
    if severity == "BLOCKING" and "LOOKAHEAD_RISK" not in flags:
        flags.append("LOOKAHEAD_RISK")
    return flags


def _pit_gate_policy(blocker_summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_pit_gate_policy.v1",
        "scope": "dynamic_strategy",
        "candidate_search": {
            "blocked_if": [
                {"any_input_severity": "BLOCKING"},
                {"required_signal_pit_status": "UNKNOWN"},
                {"required_execution_semantic_pit_status": "UNKNOWN"},
            ]
        },
        "research_only_observation": {
            "blocked_if": [
                {"any_input_severity": "BLOCKING"},
                "any_required_signal_not_true_or_approved_approximate_pit",
                "valid_until_window_not_grounded",
                "stale_signal_rule_not_verifiable",
            ]
        },
        "paper_shadow": {
            "blocked_if": [
                "any_input_severity_not_below_material",
                "observation_not_approved",
                "owner_review_not_recorded",
            ]
        },
        "production": {"blocked_if": ["always_true_for_current_phase"]},
        "current_gate_result": {
            "candidate_search_allowed": False,
            "research_only_observation_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "reason": list(_as_list(blocker_summary.get("reason"))),
        },
    }


def _remediation_routes() -> dict[str, Any]:
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_pit_remediation_routes.v1",
        "routes": {
            "TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation": {
                "purpose": "implement registry-backed PIT matrix generator and gate checker",
                "handles": [
                    "pit_input_registry",
                    "pit_matrix_generator",
                    "pit_severity_gate",
                    "blocker_summary",
                ],
                "default_next_route": True,
            },
            "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan": {
                "purpose": "design remediation for growth_tilt_engine blocking gap",
                "handles": [
                    "source_features",
                    "as_of correctness",
                    "signal horizon",
                    "signal confidence",
                    "false risk-on risk",
                ],
            },
            "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan": {
                "purpose": "design remediation for valid_until_window blocking gap",
                "handles": [
                    "valid_from",
                    "valid_until",
                    "signal expiry",
                    "stale signal carry-forward",
                    "near-expiry decay",
                ],
            },
            "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan": {
                "purpose": "replace coarse regime pass-rate with expectation-aware scoring",
                "handles": [
                    "risk_on expectation",
                    "risk_off expectation",
                    "high_volatility expectation",
                    "recovery expectation",
                ],
            },
            "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan": {
                "purpose": "normalize historical candidate outcomes for threshold calibration",
                "handles": [
                    "candidate x gate x decision matrix",
                    "owner review boundary",
                    "observation preview boundary",
                ],
            },
        },
    }


def _implementation_plan(
    registry_schema: Mapping[str, Any],
    gate_policy: Mapping[str, Any],
    routes: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "record_ready": True,
        "schema_version": "dynamic_strategy_pit_matrix_implementation_plan.v1",
        "implementation_scope": "PLAN_ONLY_NO_ENGINE_IMPLEMENTED",
        "recommended_registry_path": registry_schema.get("recommended_path"),
        "future_generator_cli_options": [
            (
                "aits research quality pit-coverage-matrix generate "
                "--scope dynamic_strategy --as-of YYYY-MM-DD"
            ),
            "aits research strategies dynamic-strategy-pit-coverage-matrix-generate",
        ],
        "generator_inputs": [
            "config/research/dynamic_strategy_pit_input_registry.yaml",
            "latest_validate_data_result",
            "feature_artifact_metadata",
            "signal_artifact_metadata",
            "regime_label_config",
            "threshold_policy_config",
        ],
        "generator_outputs": [
            "outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_coverage_matrix.json",
            "outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_blocker_summary.json",
            "outputs/research_quality/pit_coverage_matrix/dynamic_strategy_pit_remediation_matrix.json",
            "docs/research/dynamic_strategy_pit_coverage_matrix_latest.md",
        ],
        "implementation_steps": [
            "create registry schema and initial entries",
            "implement registry-backed matrix generator",
            "implement PIT severity gate checker",
            "wire blocker summary into candidate-search precondition",
            "publish generated matrix and remediation docs",
        ],
        "gate_policy_summary": gate_policy.get("current_gate_result"),
        "remediation_route_summary": routes.get("routes"),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _infrastructure_boundary() -> dict[str, Any]:
    return {
        "candidate_search_resumed": False,
        "strategy_retest_allowed": False,
        "observation_approval_allowed": False,
        "paper_shadow_allowed": False,
        "broker_allowed": False,
        "allowed_next_work": [
            "PIT matrix reusable implementation",
            "signal construction remediation plan",
            "valid-until semantics remediation plan",
            "regime expectation scoring plan",
            "threshold meta-dataset plan",
        ],
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "implementation_plan_result.json"),
        "pit_input_registry_schema_json": str(
            output_root / "pit_input_registry_schema.json"
        ),
        "pit_gate_policy_json": str(output_root / "pit_gate_policy.json"),
        "remediation_routes_json": str(output_root / "remediation_routes.json"),
        "current_blocker_summary_json": str(output_root / "current_blocker_summary.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_pit_coverage_matrix_implementation_plan.md"
        ),
        "pit_input_registry_schema_markdown": str(
            docs_root / "dynamic_strategy_pit_input_registry_schema.md"
        ),
        "pit_gate_policy_markdown": str(docs_root / "dynamic_strategy_pit_gate_policy.md"),
        "remediation_routes_markdown": str(
            docs_root / "dynamic_strategy_pit_remediation_routes.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2405_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    for path_key, report_type, schema_key, payload_key in (
        (
            "pit_input_registry_schema_json",
            "dynamic_strategy_pit_input_registry_schema",
            "dynamic_strategy_pit_input_registry_schema.v1",
            "pit_input_registry_schema",
        ),
        (
            "pit_gate_policy_json",
            "dynamic_strategy_pit_gate_policy",
            "dynamic_strategy_pit_gate_policy.v1",
            "pit_gate_policy",
        ),
        (
            "remediation_routes_json",
            "dynamic_strategy_pit_remediation_routes",
            "dynamic_strategy_pit_remediation_routes.v1",
            "remediation_routes",
        ),
        (
            "current_blocker_summary_json",
            "dynamic_strategy_current_pit_blocker_summary",
            "dynamic_strategy_current_pit_blocker_summary.v1",
            "current_blocker_summary",
        ),
    ):
        write_json_artifact(
            Path(paths[path_key]),
            {
                "task_id": TASK_ID,
                "status": payload.get("status"),
                "report_type": report_type,
                "schema_version": schema_key,
                payload_key: payload.get(payload_key, {}),
                "production_effect": "none",
                "broker_action": "none",
            },
        )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["pit_input_registry_schema_markdown"]),
        _schema_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["pit_gate_policy_markdown"]),
        _gate_policy_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["remediation_routes_markdown"]),
        _routes_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT coverage matrix implementation plan",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- blocking gaps：`{payload.get('blocking_gaps')}`",
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- data quality gate：not run；reason="
                f"`{payload.get('data_quality_gate_reason')}`"
            ),
            "",
            "## Source findings from TRADING-2403",
            "",
            _json_block(payload.get("current_blocker_summary", {})),
            "",
            "## PIT input registry schema",
            "",
            _json_block(payload.get("pit_input_registry_schema", {})),
            "",
            "## PIT coverage matrix generator design",
            "",
            _json_block(payload.get("pit_matrix_implementation_plan", {})),
            "",
            "## PIT severity gate policy",
            "",
            _json_block(payload.get("pit_gate_policy", {})),
            "",
            "## Remediation routing plan",
            "",
            _json_block(payload.get("remediation_routes", {})),
            "",
            "## Infrastructure boundary",
            "",
            _json_block(payload.get("infrastructure_boundary", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
        ]
    )


def _schema_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT input registry schema",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _json_block(payload.get("pit_input_registry_schema", {})),
            "",
        ]
    )


def _gate_policy_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT gate policy",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _json_block(payload.get("pit_gate_policy", {})),
            "",
        ]
    )


def _routes_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT remediation routes",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _json_block(payload.get("remediation_routes", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2405 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            f"- broker action enabled：`{payload.get('broker_action_enabled')}`",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any, default: str = "") -> str:
    return str(value) if value not in (None, "") else default
