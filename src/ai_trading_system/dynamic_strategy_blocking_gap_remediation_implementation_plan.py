from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan as m2406
import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as m2405
import ai_trading_system.dynamic_strategy_valid_until_window_stale_signal_remediation_plan as m2407
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
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2408"
TASK_REGISTER_ID = (
    "TRADING-2408_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN"
)
REPORT_TYPE = "dynamic_strategy_blocking_gap_remediation_implementation_plan"
SCHEMA_VERSION = "dynamic_strategy_blocking_gap_remediation_implementation_plan.v1"
READY_STATUS = "DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_BLOCKED_SOURCE"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2405", "TRADING-2406", "TRADING-2407")
BLOCKING_GAPS: tuple[str, ...] = ("growth_tilt_engine", "valid_until_window")
NEXT_ROUTE = "TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_REGISTRY_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "clear_growth_tilt_engine_blocking_gap",
    "clear_valid_until_window_blocking_gap",
    "downgrade_any_blocking_gap",
    "mark_any_blocker_true_pit",
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
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "growth_tilt_engine_blocking_gap_resolved",
    "valid_until_window_blocking_gap_resolved",
    "any_blocker_severity_downgraded",
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

DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH = m2407.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH
DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH = (
    m2407.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH
)
DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH = (
    m2407.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH
)
DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH = m2407.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH
DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH = m2407.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH
DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH = (
    m2407.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH
)
DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH = m2407.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH
DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH = (
    m2407.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH
)
DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH = m2407.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH
DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH = (
    m2407.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH
)
DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH = (
    m2407.DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH
)
DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH = m2407.DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH
DEFAULT_SOURCE_2407_REMEDIATION_PLAN_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "remediation_plan_result.json"
)
DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "valid_until_semantics_review.json"
)
DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "stale_signal_risk_audit.json"
)
DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "signal_validity_contract_plan.json"
)
DEFAULT_SOURCE_2407_SEVERITY_DOWNGRADE_CONDITIONS_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "severity_downgrade_conditions.json"
)
DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "validation_plan.json"
)
DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH
)


def run_dynamic_strategy_blocking_gap_remediation_implementation_plan(
    *,
    source_2405_implementation_path: Path = DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Path = DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Path = DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Path = DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Path = DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Path = DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2406_remediation_plan_path: Path = DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2406_source_feature_inventory_path: Path = (
        DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH
    ),
    source_2406_pit_risk_audit_path: Path = DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Path = (
        DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH
    ),
    source_2406_severity_downgrade_conditions_path: Path = (
        DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH
    ),
    source_2406_validation_plan_path: Path = DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH,
    source_2407_remediation_plan_path: Path = DEFAULT_SOURCE_2407_REMEDIATION_PLAN_PATH,
    source_2407_valid_until_semantics_review_path: Path = (
        DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH
    ),
    source_2407_stale_signal_risk_audit_path: Path = (
        DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH
    ),
    source_2407_signal_validity_contract_plan_path: Path = (
        DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH
    ),
    source_2407_severity_downgrade_conditions_path: Path = (
        DEFAULT_SOURCE_2407_SEVERITY_DOWNGRADE_CONDITIONS_PATH
    ),
    source_2407_validation_plan_path: Path = DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH,
    pit_input_registry_path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2405_implementation_path=source_2405_implementation_path,
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        source_2405_remediation_routes_path=source_2405_remediation_routes_path,
        source_2406_remediation_plan_path=source_2406_remediation_plan_path,
        source_2406_source_feature_inventory_path=(
            source_2406_source_feature_inventory_path
        ),
        source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
        source_2406_signal_construction_gap_analysis_path=(
            source_2406_signal_construction_gap_analysis_path
        ),
        source_2406_severity_downgrade_conditions_path=(
            source_2406_severity_downgrade_conditions_path
        ),
        source_2406_validation_plan_path=source_2406_validation_plan_path,
        source_2407_remediation_plan_path=source_2407_remediation_plan_path,
        source_2407_valid_until_semantics_review_path=(
            source_2407_valid_until_semantics_review_path
        ),
        source_2407_stale_signal_risk_audit_path=source_2407_stale_signal_risk_audit_path,
        source_2407_signal_validity_contract_plan_path=(
            source_2407_signal_validity_contract_plan_path
        ),
        source_2407_severity_downgrade_conditions_path=(
            source_2407_severity_downgrade_conditions_path
        ),
        source_2407_validation_plan_path=source_2407_validation_plan_path,
        pit_input_registry_path=pit_input_registry_path,
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
    source_2405_implementation_path: Path,
    source_2405_registry_snapshot_path: Path,
    source_2405_pit_coverage_matrix_path: Path,
    source_2405_pit_gate_result_path: Path,
    source_2405_blocker_summary_path: Path,
    source_2405_remediation_routes_path: Path,
    source_2406_remediation_plan_path: Path,
    source_2406_source_feature_inventory_path: Path,
    source_2406_pit_risk_audit_path: Path,
    source_2406_signal_construction_gap_analysis_path: Path,
    source_2406_severity_downgrade_conditions_path: Path,
    source_2406_validation_plan_path: Path,
    source_2407_remediation_plan_path: Path,
    source_2407_valid_until_semantics_review_path: Path,
    source_2407_stale_signal_risk_audit_path: Path,
    source_2407_signal_validity_contract_plan_path: Path,
    source_2407_severity_downgrade_conditions_path: Path,
    source_2407_validation_plan_path: Path,
    pit_input_registry_path: Path,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "implementation_2405": _load_json_document(source_2405_implementation_path),
        "registry_snapshot_2405": _load_json_document(
            source_2405_registry_snapshot_path
        ),
        "pit_matrix_2405": _load_json_document(source_2405_pit_coverage_matrix_path),
        "pit_gate_result_2405": _load_json_document(source_2405_pit_gate_result_path),
        "blocker_summary_2405": _load_json_document(source_2405_blocker_summary_path),
        "remediation_routes_2405": _load_json_document(
            source_2405_remediation_routes_path
        ),
        "remediation_plan_2406": _load_json_document(source_2406_remediation_plan_path),
        "source_feature_inventory_2406": _load_json_document(
            source_2406_source_feature_inventory_path
        ),
        "pit_risk_audit_2406": _load_json_document(source_2406_pit_risk_audit_path),
        "signal_construction_gap_analysis_2406": _load_json_document(
            source_2406_signal_construction_gap_analysis_path
        ),
        "severity_downgrade_conditions_2406": _load_json_document(
            source_2406_severity_downgrade_conditions_path
        ),
        "validation_plan_2406": _load_json_document(source_2406_validation_plan_path),
        "remediation_plan_2407": _load_json_document(source_2407_remediation_plan_path),
        "valid_until_semantics_review_2407": _load_json_document(
            source_2407_valid_until_semantics_review_path
        ),
        "stale_signal_risk_audit_2407": _load_json_document(
            source_2407_stale_signal_risk_audit_path
        ),
        "signal_validity_contract_plan_2407": _load_json_document(
            source_2407_signal_validity_contract_plan_path
        ),
        "severity_downgrade_conditions_2407": _load_json_document(
            source_2407_severity_downgrade_conditions_path
        ),
        "validation_plan_2407": _load_json_document(source_2407_validation_plan_path),
        "pit_input_registry_config": _load_yaml_document(pit_input_registry_path),
    }
    sources["source_paths"] = {
        "implementation_2405": str(source_2405_implementation_path),
        "registry_snapshot_2405": str(source_2405_registry_snapshot_path),
        "pit_matrix_2405": str(source_2405_pit_coverage_matrix_path),
        "pit_gate_result_2405": str(source_2405_pit_gate_result_path),
        "blocker_summary_2405": str(source_2405_blocker_summary_path),
        "remediation_routes_2405": str(source_2405_remediation_routes_path),
        "remediation_plan_2406": str(source_2406_remediation_plan_path),
        "source_feature_inventory_2406": str(source_2406_source_feature_inventory_path),
        "pit_risk_audit_2406": str(source_2406_pit_risk_audit_path),
        "signal_construction_gap_analysis_2406": str(
            source_2406_signal_construction_gap_analysis_path
        ),
        "severity_downgrade_conditions_2406": str(
            source_2406_severity_downgrade_conditions_path
        ),
        "validation_plan_2406": str(source_2406_validation_plan_path),
        "remediation_plan_2407": str(source_2407_remediation_plan_path),
        "valid_until_semantics_review_2407": str(
            source_2407_valid_until_semantics_review_path
        ),
        "stale_signal_risk_audit_2407": str(source_2407_stale_signal_risk_audit_path),
        "signal_validity_contract_plan_2407": str(
            source_2407_signal_validity_contract_plan_path
        ),
        "severity_downgrade_conditions_2407": str(
            source_2407_severity_downgrade_conditions_path
        ),
        "validation_plan_2407": str(source_2407_validation_plan_path),
        "pit_input_registry_config": str(pit_input_registry_path),
    }
    return sources


def _load_yaml_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    document = safe_load_yaml_path(path)
    if not isinstance(document, Mapping):
        return {"_invalid": True, "_path": str(path), "_observed_type": type(document).__name__}
    return dict(document)


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_statuses = {
        "implementation_2405": m2405.READY_STATUS,
        "registry_snapshot_2405": m2405.READY_STATUS,
        "pit_matrix_2405": m2405.READY_STATUS,
        "pit_gate_result_2405": m2405.READY_STATUS,
        "blocker_summary_2405": m2405.READY_STATUS,
        "remediation_routes_2405": m2405.READY_STATUS,
        "remediation_plan_2406": m2406.READY_STATUS,
        "source_feature_inventory_2406": m2406.READY_STATUS,
        "pit_risk_audit_2406": m2406.READY_STATUS,
        "signal_construction_gap_analysis_2406": m2406.READY_STATUS,
        "severity_downgrade_conditions_2406": m2406.READY_STATUS,
        "validation_plan_2406": m2406.READY_STATUS,
        "remediation_plan_2407": m2407.READY_STATUS,
        "valid_until_semantics_review_2407": m2407.READY_STATUS,
        "stale_signal_risk_audit_2407": m2407.READY_STATUS,
        "signal_validity_contract_plan_2407": m2407.READY_STATUS,
        "severity_downgrade_conditions_2407": m2407.READY_STATUS,
        "validation_plan_2407": m2407.READY_STATUS,
    }
    for source_name, expected in expected_statuses.items():
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing source artifact {source.get('_path')}")
        elif source.get("status") != expected:
            errors.append(
                f"{source_name}: expected status {expected}, observed {source.get('status')}"
            )
    registry = _as_mapping(sources.get("pit_input_registry_config"))
    if registry.get("_missing"):
        errors.append(f"pit_input_registry_config: missing config {registry.get('_path')}")
    if registry.get("_invalid"):
        errors.append(
            f"pit_input_registry_config: invalid config object {registry.get('_path')}"
        )

    implementation_2405 = _as_mapping(sources.get("implementation_2405"))
    if set(BLOCKING_GAPS) - set(_as_list(implementation_2405.get("blocking_gaps"))):
        errors.append("2405 implementation missing current blocking gaps")
    if implementation_2405.get("candidate_search_allowed") is not False:
        errors.append("2405 candidate_search_allowed must be false")
    for field in m2405.SAFETY_FALSE_FIELDS:
        if implementation_2405.get(field) is True:
            errors.append(f"2405 safety field must be false: {field}")

    gate = _as_mapping(
        _as_mapping(sources.get("pit_gate_result_2405")).get("pit_gate_result")
    )
    if gate.get("candidate_search_allowed") is not False:
        errors.append("2405 PIT gate candidate_search_allowed must be false")
    gate_blockers = set(_as_list(gate.get("blockers")))
    if "BLOCKING_GAP_GROWTH_TILT_ENGINE" not in gate_blockers:
        errors.append("2405 PIT gate missing BLOCKING_GAP_GROWTH_TILT_ENGINE")
    if "BLOCKING_GAP_VALID_UNTIL_WINDOW" not in gate_blockers:
        errors.append("2405 PIT gate missing BLOCKING_GAP_VALID_UNTIL_WINDOW")

    blocker_details = _blocking_gap_details(sources)
    for gap in BLOCKING_GAPS:
        detail = _as_mapping(blocker_details.get(gap))
        if detail.get("severity") != "BLOCKING":
            errors.append(f"2405 blocker summary does not keep {gap} BLOCKING")
        if detail.get("candidate_search_blocker") is not True:
            errors.append(f"{gap} must remain candidate search blocker")
        registry_entry = _registry_entry(registry, gap)
        if registry_entry.get("severity") != "BLOCKING":
            errors.append(f"PIT input registry does not keep {gap} BLOCKING")
        if registry_entry.get("candidate_search_blocker") is not True:
            errors.append(f"PIT input registry {gap} must block candidate search")

    remediation_2406 = _as_mapping(sources.get("remediation_plan_2406"))
    if remediation_2406.get("recommended_next_research_task") != m2406.NEXT_ROUTE:
        errors.append("2406 next route must point to TRADING-2407")
    if remediation_2406.get("growth_tilt_engine_blocking_gap_resolved") is not False:
        errors.append("2406 must not resolve growth_tilt_engine blocker")
    if remediation_2406.get("growth_tilt_engine_severity_downgraded") is not False:
        errors.append("2406 must not downgrade growth_tilt_engine")
    for field in m2406.SAFETY_FALSE_FIELDS:
        if remediation_2406.get(field) is True:
            errors.append(f"2406 safety field must be false: {field}")

    remediation_2407 = _as_mapping(sources.get("remediation_plan_2407"))
    if remediation_2407.get("recommended_next_research_task") != m2407.NEXT_ROUTE:
        errors.append("2407 next route must point to TRADING-2408")
    if remediation_2407.get("valid_until_window_blocking_gap_resolved") is not False:
        errors.append("2407 must not resolve valid_until_window blocker")
    if remediation_2407.get("valid_until_window_severity_downgraded") is not False:
        errors.append("2407 must not downgrade valid_until_window")
    for field in m2407.SAFETY_FALSE_FIELDS:
        if remediation_2407.get(field) is True:
            errors.append(f"2407 safety field must be false: {field}")

    if not _as_list(
        _as_mapping(
            _as_mapping(sources.get("source_feature_inventory_2406")).get(
                "source_feature_inventory"
            )
        ).get("source_feature_inventory")
    ) and not _as_list(
        _as_mapping(sources.get("source_feature_inventory_2406")).get(
            "source_feature_inventory"
        )
    ):
        errors.append("2406 source feature inventory missing")
    contract_plan_2407 = _as_mapping(
        _as_mapping(sources.get("signal_validity_contract_plan_2407")).get(
            "signal_validity_contract_plan"
        )
    )
    if "valid_until" not in _as_list(contract_plan_2407.get("required_fields")):
        errors.append("2407 signal validity contract plan missing valid_until field")
    return errors


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
        "blocking_gaps": list(BLOCKING_GAPS),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "automatic_downgrade_allowed": False,
        "owner_review_required_for_any_downgrade": True,
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "candidate_search_resumed": False,
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
        "growth_tilt_engine_blocking_gap_resolved": False,
        "valid_until_window_blocking_gap_resolved": False,
        "any_blocker_severity_downgraded": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    current_gaps = _build_current_blocking_gaps(sources)
    architecture = _build_unified_remediation_architecture()
    contract_schema_plan = _build_contract_schema_plan()
    sequence = _build_implementation_sequence()
    downgrade_workflow = _build_blocker_downgrade_workflow()
    gate_policy = _build_candidate_search_gate_policy()
    return {
        "current_blocking_gaps": current_gaps,
        "unified_remediation_architecture_ready": True,
        "contract_schema_plan_ready": True,
        "implementation_sequence_ready": True,
        "blocker_downgrade_workflow_ready": True,
        "candidate_search_gate_policy_ready": True,
        "unified_remediation_architecture": architecture,
        "contract_schema_plan": contract_schema_plan,
        "implementation_sequence": sequence,
        "blocker_downgrade_workflow": downgrade_workflow,
        "candidate_search_gate_policy": gate_policy,
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "A unified signal as-of / validity schema must exist before "
            "growth_tilt_engine or valid_until_window mappings diverge; replay "
            "validation and PIT gate downgrade evidence depend on shared fields."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "current_blocking_gaps": {},
        "unified_remediation_architecture_ready": False,
        "contract_schema_plan_ready": False,
        "implementation_sequence_ready": False,
        "blocker_downgrade_workflow_ready": False,
        "candidate_search_gate_policy_ready": False,
        "unified_remediation_architecture": {},
        "contract_schema_plan": {},
        "implementation_sequence": {},
        "blocker_downgrade_workflow": {},
        "candidate_search_gate_policy": {},
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _build_current_blocking_gaps(sources: Mapping[str, Any]) -> dict[str, Any]:
    details = _blocking_gap_details(sources)
    registry = _as_mapping(sources.get("pit_input_registry_config"))
    return {
        "growth_tilt_engine": {
            "input_type": "SIGNAL",
            "severity": "BLOCKING",
            "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
            "pit_confidence": _as_mapping(details.get("growth_tilt_engine")).get(
                "pit_confidence",
                _registry_entry(registry, "growth_tilt_engine").get("pit_confidence"),
            ),
            "blocker_reason": [
                "source feature PIT safety not fully established",
                "as-of semantics incomplete",
                "signal horizon not fully grounded",
                "signal confidence / false risk-on risk not validated",
            ],
            "resolved": False,
            "downgraded": False,
        },
        "valid_until_window": {
            "input_type": "EXECUTION_SEMANTIC",
            "severity": "BLOCKING",
            "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
            "pit_confidence": _as_mapping(details.get("valid_until_window")).get(
                "pit_confidence",
                _registry_entry(registry, "valid_until_window").get("pit_confidence"),
            ),
            "blocker_reason": [
                "valid_from / valid_until semantics not fully grounded",
                "stale signal carry-forward contract incomplete",
                "signal-to-execution lag contract incomplete",
                "horizon-to-valid-until mapping not validated",
            ],
            "resolved": False,
            "downgraded": False,
        },
    }


def _build_unified_remediation_architecture() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_unified_remediation_architecture.v1",
        "layers": [
            {
                "layer_id": "layer_1_signal_as_of_contract",
                "purpose": "make every dynamic strategy signal reproducible by as-of date",
                "primarily_remediates": ["growth_tilt_engine"],
                "also_supports": ["valid_until_window"],
            },
            {
                "layer_id": "layer_2_source_feature_traceability",
                "purpose": "map every signal input to source config/artifact/PIT status",
                "primarily_remediates": ["growth_tilt_engine"],
                "also_supports": [],
            },
            {
                "layer_id": "layer_3_signal_validity_contract",
                "purpose": "define valid_from, valid_until, stale_after, expiry_rule",
                "primarily_remediates": ["valid_until_window"],
                "also_supports": ["growth_tilt_engine"],
            },
            {
                "layer_id": "layer_4_stale_signal_and_execution_lag_contract",
                "purpose": "prevent expired signals from influencing future execution",
                "primarily_remediates": ["valid_until_window"],
                "also_supports": ["growth_tilt_engine"],
            },
            {
                "layer_id": "layer_5_as_of_replay_validation",
                "purpose": (
                    "validate deterministic reconstruction of signals and validity "
                    "windows"
                ),
                "remediates": list(BLOCKING_GAPS),
            },
            {
                "layer_id": "layer_6_pit_gate_downgrade_workflow",
                "purpose": "allow severity downgrade only after evidence chain exists",
                "remediates": ["blocker_governance"],
            },
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_contract_schema_plan() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_contract_schema_plan.v1",
        "planned_reusable_modules": [
            "src/ai_trading_system/research_quality/signal_as_of_contract.py",
            "src/ai_trading_system/research_quality/signal_validity_contract.py",
            "src/ai_trading_system/research_quality/signal_replay_validation.py",
            "src/ai_trading_system/research_quality/blocker_downgrade_policy.py",
        ],
        "contracts": {
            "signal_as_of_contract": {
                "required_fields": [
                    "signal_id",
                    "signal_version",
                    "as_of_date",
                    "generated_at",
                    "source_data_cutoff",
                    "source_feature_ids",
                    "source_artifact_ids",
                    "signal_horizon_days",
                    "signal_value",
                    "signal_strength_if_available",
                    "confidence_if_available",
                    "uncertainty_reason_if_available",
                ],
                "required_invariants": [
                    "generated_at >= source_data_cutoff",
                    "as_of_date <= generated_at_or_research_as_of",
                    "source_features_must_have_pit_status",
                    "no_forward_window_dependency_unless_explicitly_marked_not_pit_safe",
                ],
            },
            "source_feature_traceability_contract": {
                "required_fields": [
                    "feature_id",
                    "feature_family",
                    "source_config",
                    "source_data",
                    "as_of_handling",
                    "generated_at_handling",
                    "lookback_window",
                    "forward_window_used",
                    "pit_status",
                    "pit_confidence",
                    "risk_flags",
                    "severity",
                ],
                "required_invariants": [
                    "every_signal_feature_has_registry_entry",
                    "forward_window_used=false_for_true_pit_features",
                    "unknown_pit_feature_blocks_signal_downgrade",
                ],
            },
            "signal_validity_contract": {
                "required_fields": [
                    "signal_id",
                    "signal_version",
                    "valid_from",
                    "valid_until",
                    "stale_after",
                    "horizon_days",
                    "expiry_rule",
                    "carry_forward_rule",
                    "near_expiry_rule",
                    "signal_to_execution_lag_rule",
                ],
                "required_invariants": [
                    "valid_from >= generated_at_or_next_executable_time",
                    "valid_until > valid_from",
                    "stale_after <= valid_until",
                    "expired_signal_cannot_trigger_new_trade",
                    "carry_forward_requires_explicit_rule",
                    "missing_valid_until_blocks_candidate_search_for_dependent_strategy",
                ],
            },
            "signal_replay_validation_contract": {
                "required_checks": [
                    "reconstruct_signal_by_as_of_date",
                    "reconstruct_source_features_by_as_of_date",
                    "reconstruct_valid_from_valid_until",
                    "detect_expired_signal_execution",
                    "detect_unexplained_carry_forward",
                    "detect_signal_to_execution_lag",
                    "compare_replay_hash_stability",
                ],
                "required_outputs": [
                    "replay_validation_result",
                    "as_of_replay_hash",
                    "stale_signal_execution_count",
                    "missing_validity_field_count",
                    "forward_window_dependency_count",
                    "blocker_downgrade_eligibility",
                ],
            },
        },
        "implemented_in_2408": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_implementation_sequence() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_blocking_gap_implementation_sequence.v1",
        "recommended_immediate_next_task": NEXT_ROUTE,
        "phases": [
            {
                "phase": 1,
                "task_id": NEXT_ROUTE,
                "goal": "implement reusable schemas for signal as-of and validity contracts",
                "handles": [
                    "signal_as_of_contract",
                    "source_feature_traceability_contract",
                    "signal_validity_contract",
                ],
                "does_not": [
                    "replay validate",
                    "downgrade blockers",
                    "resume candidate search",
                ],
                "parallelizable": False,
                "depends_on": ["TRADING-2408"],
            },
            {
                "phase": 2,
                "task_id": "TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping",
                "goal": (
                    "map growth_tilt_engine source features to as-of contract "
                    "and traceability registry"
                ),
                "handles": [
                    "source_feature_inventory",
                    "feature PIT status",
                    "signal horizon draft",
                    "risk flag mapping",
                ],
                "does_not": ["mark TRUE_PIT", "downgrade blocker"],
                "parallelizable_after_2409": True,
                "depends_on": [NEXT_ROUTE],
            },
            {
                "phase": 3,
                "task_id": "TRADING-2411_Valid_Until_Window_Signal_Validity_Contract_Mapping",
                "goal": "map valid_until_window to signal validity contract",
                "handles": [
                    "valid_from",
                    "valid_until",
                    "stale_after",
                    "expiry_rule",
                    "carry_forward_rule",
                    "signal-to-execution lag",
                ],
                "does_not": ["clear blocker", "resume candidate search"],
                "parallelizable_after_2409": True,
                "depends_on": [NEXT_ROUTE],
            },
            {
                "phase": 4,
                "task_id": "TRADING-2412_Dynamic_Strategy_As_Of_Signal_Replay_Validation_Dry_Run",
                "goal": (
                    "dry-run replay validation for growth_tilt_engine and "
                    "valid_until_window"
                ),
                "handles": [
                    "reconstruct signals by as_of",
                    "reconstruct validity windows",
                    "detect stale signal execution",
                    "produce blocker downgrade evidence",
                ],
                "does_not": ["downgrade blocker automatically"],
                "parallelizable": False,
                "depends_on": [
                    "TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping",
                    "TRADING-2411_Valid_Until_Window_Signal_Validity_Contract_Mapping",
                ],
            },
            {
                "phase": 5,
                "task_id": "TRADING-2413_Dynamic_Strategy_PIT_Blocker_Downgrade_Owner_Review",
                "goal": "owner review of evidence chain before any severity downgrade",
                "handles": [
                    "growth_tilt_engine downgrade review",
                    "valid_until_window downgrade review",
                    "candidate search gate review",
                ],
                "does_not": ["approve observation", "approve paper-shadow"],
                "parallelizable": False,
                "depends_on": [
                    "TRADING-2412_Dynamic_Strategy_As_Of_Signal_Replay_Validation_Dry_Run"
                ],
            },
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_blocker_downgrade_workflow() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_blocker_downgrade_workflow.v1",
        "automatic_downgrade_allowed": False,
        "owner_review_required_for_any_downgrade": True,
        "steps": [
            {"step_id": "step_1_contract_schema_exists", "required": True},
            {"step_id": "step_2_input_mapping_complete", "required": True},
            {"step_id": "step_3_as_of_replay_validation_passed", "required": True},
            {"step_id": "step_4_pit_gate_result_regenerated", "required": True},
            {"step_id": "step_5_owner_review_recorded", "required": True},
            {
                "step_id": "step_6_registry_severity_updated",
                "allowed_only_after_owner_review": True,
            },
            {
                "step_id": "step_7_candidate_search_gate_re_evaluated",
                "required": True,
                "note": "candidate search may remain blocked if any blocker persists",
            },
        ],
        "downgrade_executed_in_2408": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_candidate_search_gate_policy() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_candidate_search_gate_policy.v1",
        "candidate_search_allowed": False,
        "reason": [
            "growth_tilt_engine remains BLOCKING",
            "valid_until_window remains BLOCKING",
        ],
        "candidate_search_can_be_reconsidered_only_after": [
            "both blockers downgraded from BLOCKING",
            "PIT gate regenerated",
            "owner review recorded",
        ],
        "observation_can_be_reconsidered_only_after": [
            "candidate search restored",
            "candidate retest rerun under remediated contracts",
            "observation preview candidate exists",
            "owner review recorded",
        ],
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _write_outputs(
    payload: dict[str, Any],
    *,
    output_root: Path,
    docs_root: Path,
) -> None:
    paths = {
        "json_path": str(output_root / "implementation_plan_result.json"),
        "unified_remediation_architecture_json": str(
            output_root / "unified_remediation_architecture.json"
        ),
        "contract_schema_plan_json": str(output_root / "contract_schema_plan.json"),
        "implementation_sequence_json": str(output_root / "implementation_sequence.json"),
        "blocker_downgrade_workflow_json": str(
            output_root / "blocker_downgrade_workflow.json"
        ),
        "candidate_search_gate_policy_json": str(
            output_root / "candidate_search_gate_policy.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_blocking_gap_remediation_implementation_plan.md"
        ),
        "contract_schema_plan_markdown": str(
            docs_root / "dynamic_strategy_signal_as_of_and_validity_contract_schema_plan.md"
        ),
        "blocker_downgrade_workflow_markdown": str(
            docs_root / "dynamic_strategy_blocker_downgrade_workflow.md"
        ),
        "implementation_sequence_markdown": str(
            docs_root / "dynamic_strategy_blocking_gap_implementation_sequence.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2409_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    for path_key, report_type, schema_key, payload_key in (
        (
            "unified_remediation_architecture_json",
            "dynamic_strategy_unified_remediation_architecture",
            "dynamic_strategy_unified_remediation_architecture.v1",
            "unified_remediation_architecture",
        ),
        (
            "contract_schema_plan_json",
            "dynamic_strategy_contract_schema_plan",
            "dynamic_strategy_contract_schema_plan.v1",
            "contract_schema_plan",
        ),
        (
            "implementation_sequence_json",
            "dynamic_strategy_blocking_gap_implementation_sequence",
            "dynamic_strategy_blocking_gap_implementation_sequence.v1",
            "implementation_sequence",
        ),
        (
            "blocker_downgrade_workflow_json",
            "dynamic_strategy_blocker_downgrade_workflow",
            "dynamic_strategy_blocker_downgrade_workflow.v1",
            "blocker_downgrade_workflow",
        ),
        (
            "candidate_search_gate_policy_json",
            "dynamic_strategy_candidate_search_gate_policy",
            "dynamic_strategy_candidate_search_gate_policy.v1",
            "candidate_search_gate_policy",
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
        Path(paths["contract_schema_plan_markdown"]),
        _contract_schema_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["blocker_downgrade_workflow_markdown"]),
        _downgrade_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["implementation_sequence_markdown"]),
        _sequence_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy blocking gap remediation implementation plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- blocking gaps：`{payload.get('blocking_gaps')}`",
            (
                "- unified remediation architecture ready："
                f"`{payload.get('unified_remediation_architecture_ready')}`"
            ),
            f"- contract schema plan ready：`{payload.get('contract_schema_plan_ready')}`",
            f"- implementation sequence ready：`{payload.get('implementation_sequence_ready')}`",
            (
                "- blocker downgrade workflow ready："
                f"`{payload.get('blocker_downgrade_workflow_ready')}`"
            ),
            (
                "- candidate search gate policy ready："
                f"`{payload.get('candidate_search_gate_policy_ready')}`"
            ),
            (
                "- growth_tilt_engine blocker resolved："
                f"`{payload.get('growth_tilt_engine_blocking_gap_resolved')}`"
            ),
            (
                "- valid_until_window blocker resolved："
                f"`{payload.get('valid_until_window_blocking_gap_resolved')}`"
            ),
            (
                "- any blocker severity downgraded："
                f"`{payload.get('any_blocker_severity_downgraded')}`"
            ),
            (
                "- automatic downgrade allowed："
                f"`{payload.get('automatic_downgrade_allowed')}`"
            ),
            (
                "- owner review required for any downgrade："
                f"`{payload.get('owner_review_required_for_any_downgrade')}`"
            ),
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- data quality gate：not run；reason="
                f"`{payload.get('data_quality_gate_reason')}`"
            ),
            "",
            "## Source findings from TRADING-2405 / 2406 / 2407",
            "",
            f"- source validation errors：`{payload.get('source_validation_errors')}`",
            "",
            "## Current blocking gaps",
            "",
            _json_block(payload.get("current_blocking_gaps", {})),
            "",
            "## Unified remediation architecture",
            "",
            _json_block(payload.get("unified_remediation_architecture", {})),
            "",
            "## Required contract schemas",
            "",
            _json_block(payload.get("contract_schema_plan", {})),
            "",
            "## Implementation sequence",
            "",
            _json_block(payload.get("implementation_sequence", {})),
            "",
            "## Blocker downgrade workflow",
            "",
            _json_block(payload.get("blocker_downgrade_workflow", {})),
            "",
            "## Candidate search gate policy during remediation",
            "",
            _json_block(payload.get("candidate_search_gate_policy", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
            "## Recommended next route",
            "",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- reason：`{payload.get('recommended_next_research_task_reason')}`",
            "",
        ]
    )


def _contract_schema_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy signal as-of and validity contract schema plan",
            "",
            f"- status：`{payload.get('status')}`",
            f"- ready：`{payload.get('contract_schema_plan_ready')}`",
            "",
            _json_block(payload.get("contract_schema_plan", {})),
            "",
        ]
    )


def _downgrade_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy blocker downgrade workflow",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- automatic downgrade allowed："
                f"`{payload.get('automatic_downgrade_allowed')}`"
            ),
            (
                "- owner review required for any downgrade："
                f"`{payload.get('owner_review_required_for_any_downgrade')}`"
            ),
            "",
            _json_block(payload.get("blocker_downgrade_workflow", {})),
            "",
            "## Candidate search gate policy",
            "",
            _json_block(payload.get("candidate_search_gate_policy", {})),
            "",
        ]
    )


def _sequence_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy blocking gap implementation sequence",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            _json_block(payload.get("implementation_sequence", {})),
            "",
            "## Unified remediation architecture",
            "",
            _json_block(payload.get("unified_remediation_architecture", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2409 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- reason：`{payload.get('recommended_next_research_task_reason')}`",
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


def _blocking_gap_details(sources: Mapping[str, Any]) -> Mapping[str, Any]:
    return _as_mapping(
        _as_mapping(
            _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
        ).get("blocking_gap_details")
    )


def _registry_entry(registry: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    entries = _as_list(registry.get("entries"))
    for entry in (_as_mapping(item) for item in entries):
        if entry.get("input_id") == input_id:
            return entry
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
