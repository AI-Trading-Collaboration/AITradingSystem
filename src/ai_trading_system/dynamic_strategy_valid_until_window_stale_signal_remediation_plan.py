from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan as m2406
import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as m2405
import ai_trading_system.dynamic_strategy_pit_coverage_signal_construction_review as m2403
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

TASK_ID = "TRADING-2407"
TASK_REGISTER_ID = (
    "TRADING-2407_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN"
)
REPORT_TYPE = "dynamic_strategy_valid_until_window_stale_signal_remediation_plan"
SCHEMA_VERSION = "dynamic_strategy_valid_until_window_stale_signal_remediation_plan.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_"
    "REMEDIATION_PLAN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_"
    "REMEDIATION_PLAN_BLOCKED_SOURCE"
)
INPUT_UNDER_REVIEW = "valid_until_window"
INPUT_TYPE = "EXECUTION_SEMANTIC"
CURRENT_SEVERITY = "BLOCKING"
CURRENT_PIT_STATUS = "UNKNOWN_OR_APPROXIMATE_PIT"
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2403", "TRADING-2405", "TRADING-2406")
NEXT_ROUTE = "TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_CONFIG_ONLY_NO_FRESH_MARKET_DATA"
)
FOCUSED_GROWTH_TILT_CANDIDATE_ID = m2406.FOCUSED_GROWTH_TILT_CANDIDATE_ID

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "clear_valid_until_window_blocking_gap",
    "downgrade_valid_until_window_severity",
    "mark_valid_until_window_true_pit",
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
    "valid_until_window_blocking_gap_resolved",
    "valid_until_window_severity_downgraded",
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

DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH = m2406.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH
DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH = (
    m2406.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH
)
DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH = (
    m2406.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH
)
DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH = m2406.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH
DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH = m2406.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH
DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH = (
    m2406.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH
)
DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "remediation_plan_result.json"
)
DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "source_feature_inventory.json"
)
DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "pit_risk_audit.json"
)
DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "signal_construction_gap_analysis.json"
)
DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "severity_downgrade_conditions.json"
)
DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "validation_plan.json"
)
DEFAULT_SOURCE_2403_PIT_MATRIX_PATH = m2406.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH
DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH = (
    m2406.DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH
)
DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH = m2406.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH
DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH = (
    m2405.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH
)
DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "strategy_execution_policy_registry.yaml"
)
DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH = (
    PROJECT_ROOT / "config" / "research" / "signal_validity_taxonomy.yaml"
)


def run_dynamic_strategy_valid_until_window_stale_signal_remediation_plan(
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
    source_2403_pit_matrix_path: Path = DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_signal_construction_review_path: Path = (
        DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH
    ),
    source_2403_remediation_matrix_path: Path = DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    pit_input_registry_path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    execution_policy_registry_path: Path = DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Path = DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT
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
        source_2403_pit_matrix_path=source_2403_pit_matrix_path,
        source_2403_signal_construction_review_path=(
            source_2403_signal_construction_review_path
        ),
        source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
        pit_input_registry_path=pit_input_registry_path,
        execution_policy_registry_path=execution_policy_registry_path,
        signal_validity_taxonomy_path=signal_validity_taxonomy_path,
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
    source_2403_pit_matrix_path: Path,
    source_2403_signal_construction_review_path: Path,
    source_2403_remediation_matrix_path: Path,
    pit_input_registry_path: Path,
    execution_policy_registry_path: Path,
    signal_validity_taxonomy_path: Path,
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
        "pit_matrix_2403": _load_json_document(source_2403_pit_matrix_path),
        "signal_construction_review_2403": _load_json_document(
            source_2403_signal_construction_review_path
        ),
        "remediation_matrix_2403": _load_json_document(
            source_2403_remediation_matrix_path
        ),
        "pit_input_registry_config": _load_yaml_document(pit_input_registry_path),
        "execution_policy_registry": _load_yaml_document(execution_policy_registry_path),
        "signal_validity_taxonomy": _load_yaml_document(signal_validity_taxonomy_path),
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
        "pit_matrix_2403": str(source_2403_pit_matrix_path),
        "signal_construction_review_2403": str(
            source_2403_signal_construction_review_path
        ),
        "remediation_matrix_2403": str(source_2403_remediation_matrix_path),
        "pit_input_registry_config": str(pit_input_registry_path),
        "execution_policy_registry": str(execution_policy_registry_path),
        "signal_validity_taxonomy": str(signal_validity_taxonomy_path),
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
        "pit_matrix_2403": m2403.READY_STATUS,
        "signal_construction_review_2403": m2403.READY_STATUS,
        "remediation_matrix_2403": m2403.READY_STATUS,
    }
    for source_name, expected in expected_statuses.items():
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing source artifact {source.get('_path')}")
        elif source.get("status") != expected:
            errors.append(
                f"{source_name}: expected status {expected}, observed {source.get('status')}"
            )
    for source_name in (
        "pit_input_registry_config",
        "execution_policy_registry",
        "signal_validity_taxonomy",
    ):
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing config {source.get('_path')}")
        if source.get("_invalid"):
            errors.append(f"{source_name}: invalid config object {source.get('_path')}")

    implementation_2405 = _as_mapping(sources.get("implementation_2405"))
    if INPUT_UNDER_REVIEW not in _as_list(implementation_2405.get("blocking_gaps")):
        errors.append("2405 blocking gaps missing valid_until_window")
    if implementation_2405.get("candidate_search_allowed") is not False:
        errors.append("2405 candidate_search_allowed must be false")
    if implementation_2405.get("recommended_next_research_task") != m2405.NEXT_ROUTE:
        errors.append("2405 next route mismatch")
    for field in m2405.SAFETY_FALSE_FIELDS:
        if implementation_2405.get(field) is True:
            errors.append(f"2405 safety field must be false: {field}")

    gate = _as_mapping(
        _as_mapping(sources.get("pit_gate_result_2405")).get("pit_gate_result")
    )
    if gate.get("candidate_search_allowed") is not False:
        errors.append("2405 PIT gate candidate_search_allowed must be false")
    if "BLOCKING_GAP_VALID_UNTIL_WINDOW" not in _as_list(gate.get("blockers")):
        errors.append("2405 PIT gate missing BLOCKING_GAP_VALID_UNTIL_WINDOW")

    blocker_details = _as_mapping(
        _as_mapping(
            _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
        ).get("blocking_gap_details")
    )
    blocker = _as_mapping(blocker_details.get(INPUT_UNDER_REVIEW))
    if blocker.get("severity") != CURRENT_SEVERITY:
        errors.append("2405 blocker summary does not keep valid_until_window BLOCKING")
    if blocker.get("candidate_search_blocker") is not True:
        errors.append("valid_until_window must remain candidate search blocker")

    registry_entry = _registry_entry(
        _as_mapping(sources.get("pit_input_registry_config")),
        INPUT_UNDER_REVIEW,
    )
    if registry_entry.get("severity") != CURRENT_SEVERITY:
        errors.append("PIT input registry does not keep valid_until_window BLOCKING")
    if registry_entry.get("candidate_search_blocker") is not True:
        errors.append("PIT input registry valid_until_window must block candidate search")
    if registry_entry.get("remediation_owner") != TASK_ID:
        errors.append("PIT input registry valid_until_window must route to TRADING-2407")

    remediation_2406 = _as_mapping(sources.get("remediation_plan_2406"))
    if remediation_2406.get("recommended_next_research_task") != m2406.NEXT_ROUTE:
        errors.append("2406 next route must point to TRADING-2407")
    if remediation_2406.get("growth_tilt_engine_blocking_gap_resolved") is not False:
        errors.append("2406 must not resolve growth_tilt_engine blocker")
    if remediation_2406.get("growth_tilt_engine_severity_downgraded") is not False:
        errors.append("2406 must not downgrade growth_tilt_engine")
    if remediation_2406.get("candidate_search_allowed") is not False:
        errors.append("2406 candidate_search_allowed must be false")
    for field in m2406.SAFETY_FALSE_FIELDS:
        if remediation_2406.get(field) is True:
            errors.append(f"2406 safety field must be false: {field}")

    if not _pit_row(sources, INPUT_UNDER_REVIEW):
        errors.append("2405/2403 PIT matrix missing valid_until_window row")
    signal_review = _signal_construction_review(sources)
    source_gap = _as_mapping(signal_review.get("source_2402_signal_gap"))
    if not _as_mapping(source_gap.get("valid_until_strictness")):
        errors.append("2403 signal construction review missing valid_until_strictness")

    execution_policy = _execution_policy_for_focused_candidate(
        _as_mapping(sources.get("execution_policy_registry"))
    )
    signal_policy = _as_mapping(execution_policy.get("signal_policy"))
    if not signal_policy:
        errors.append("execution policy registry missing focused growth tilt signal policy")
    if signal_policy.get("signal_validity_window_bdays") is None:
        errors.append("focused growth tilt signal policy missing validity window")

    taxonomy = _as_mapping(sources.get("signal_validity_taxonomy"))
    if not _as_mapping(taxonomy.get("signal_validity_taxonomy")):
        errors.append("signal validity taxonomy missing signal_validity_taxonomy")
    if not _as_list(taxonomy.get("allowed_stale_actions")):
        errors.append("signal validity taxonomy missing allowed_stale_actions")
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
        "input_under_review": INPUT_UNDER_REVIEW,
        "input_type": INPUT_TYPE,
        "current_severity": CURRENT_SEVERITY,
        "current_pit_status": CURRENT_PIT_STATUS,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
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
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    current_blocker = _current_blocker_record(sources)
    semantics_review = _build_valid_until_semantics_review(sources)
    risk_audit = _build_stale_signal_risk_audit(sources)
    contract_plan = _build_signal_validity_contract_plan(sources)
    alignment_review = _build_growth_tilt_alignment_review(sources)
    remediation_plan = _build_remediation_plan()
    downgrade_conditions = _build_severity_downgrade_conditions()
    validation_plan = _build_validation_plan()
    return {
        "current_blocker": current_blocker,
        "valid_until_semantics_review_ready": True,
        "stale_signal_risk_audit_ready": True,
        "signal_validity_contract_plan_ready": True,
        "growth_tilt_alignment_review_ready": True,
        "remediation_plan_ready": True,
        "valid_until_window_remediation_plan_ready": True,
        "severity_downgrade_conditions_ready": True,
        "validation_plan_ready": True,
        "valid_until_window_validation_plan_ready": True,
        "valid_until_semantics_review": semantics_review,
        "valid_until_semantics_review_count": len(
            _as_list(semantics_review.get("semantics"))
        ),
        "stale_signal_risk_audit": risk_audit,
        "stale_signal_risk_audit_count": len(_as_list(risk_audit.get("risks"))),
        "signal_validity_contract_plan": contract_plan,
        "growth_tilt_alignment_review": alignment_review,
        "remediation_plan": remediation_plan,
        "severity_downgrade_conditions": downgrade_conditions,
        "validation_plan": validation_plan,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2406 and TRADING-2407 now define both current blocking "
            "gap remediation plans; TRADING-2408 should design implementation "
            "sequence for signal validity contracts, as-of replay validation, "
            "and later PIT severity downgrade consideration."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "current_blocker": {
            "input_id": INPUT_UNDER_REVIEW,
            "input_type": INPUT_TYPE,
            "severity": CURRENT_SEVERITY,
            "pit_status": CURRENT_PIT_STATUS,
        },
        "valid_until_semantics_review_ready": False,
        "stale_signal_risk_audit_ready": False,
        "signal_validity_contract_plan_ready": False,
        "growth_tilt_alignment_review_ready": False,
        "remediation_plan_ready": False,
        "valid_until_window_remediation_plan_ready": False,
        "severity_downgrade_conditions_ready": False,
        "validation_plan_ready": False,
        "valid_until_window_validation_plan_ready": False,
        "valid_until_semantics_review": {},
        "valid_until_semantics_review_count": 0,
        "stale_signal_risk_audit": {},
        "stale_signal_risk_audit_count": 0,
        "signal_validity_contract_plan": {},
        "growth_tilt_alignment_review": {},
        "remediation_plan": {},
        "severity_downgrade_conditions": {},
        "validation_plan": {},
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _current_blocker_record(sources: Mapping[str, Any]) -> dict[str, Any]:
    blocker_details = _as_mapping(
        _as_mapping(
            _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
        ).get("blocking_gap_details")
    )
    blocker = dict(_as_mapping(blocker_details.get(INPUT_UNDER_REVIEW)))
    registry_entry = _registry_entry(
        _as_mapping(sources.get("pit_input_registry_config")),
        INPUT_UNDER_REVIEW,
    )
    return {
        "input_id": INPUT_UNDER_REVIEW,
        "input_type": INPUT_TYPE,
        "semantic_role": "signal_validity_and_execution_expiry_gate",
        "severity": blocker.get("severity") or registry_entry.get("severity"),
        "pit_status": CURRENT_PIT_STATUS,
        "source_pit_status": blocker.get("pit_status") or registry_entry.get("pit_status"),
        "pit_confidence": blocker.get("pit_confidence")
        or registry_entry.get("pit_confidence"),
        "candidate_search_blocker": True,
        "observation_blocker": True,
        "paper_shadow_blocker": True,
        "production_blocker": True,
        "risk_flags": list(
            _as_list(blocker.get("risk_flags"))
            or _as_list(registry_entry.get("risk_flags"))
        ),
        "recommended_action": blocker.get("recommended_action")
        or registry_entry.get("recommended_action"),
        "blocker_resolved_in_2407": False,
        "severity_downgraded_in_2407": False,
    }


def _build_valid_until_semantics_review(
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    registry = _as_mapping(sources.get("pit_input_registry_config"))
    execution_policy = _execution_policy_for_focused_candidate(
        _as_mapping(sources.get("execution_policy_registry"))
    )
    signal_policy = _as_mapping(execution_policy.get("signal_policy"))
    rebalance_policy = _as_mapping(execution_policy.get("rebalance_policy"))
    validity_days = signal_policy.get("signal_validity_window_bdays", "TBD")
    lag_days = rebalance_policy.get("execution_lag_bdays", "TBD")
    signal_review = _signal_construction_review(sources)
    source_gap = _as_mapping(signal_review.get("source_2402_signal_gap"))
    valid_until_strictness = _as_mapping(source_gap.get("valid_until_strictness"))
    growth_review = _as_mapping(signal_review.get("growth_tilt_engine"))

    rows = [
        _semantic_row(
            registry,
            semantic_id=INPUT_UNDER_REVIEW,
            semantic_role="primary_signal_expiry_window",
            used_by_candidates=[FOCUSED_GROWTH_TILT_CANDIDATE_ID],
            source_config_or_artifact=(
                "config/research/dynamic_strategy_pit_input_registry.yaml + "
                "config/research/strategy_execution_policy_registry.yaml:"
                f"{FOCUSED_GROWTH_TILT_CANDIDATE_ID}.signal_policy"
            ),
            valid_from_source="not emitted per signal; policy says next_trading_day",
            valid_until_source=f"policy window={validity_days} bdays; per-signal field missing",
            signal_horizon_source=growth_review.get(
                "signal_horizon",
                "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
            ),
            expiry_rule_source=(
                "signal_validity_window_bdays exists but natural signal expiry is not "
                "derived from signal horizon"
            ),
            decay_rule_source=valid_until_strictness.get(
                "near_expiry_signal_behavior",
                "NOT_SEPARATELY_VALIDATED",
            ),
            carry_forward_rule=signal_policy.get("stale_signal_behavior", "TBD"),
            stale_signal_detection_rule=(
                "stale_signal_execution_count exists in prior review but replay "
                "contract is not deterministic"
            ),
            signal_to_execution_lag_rule=f"execution_lag_bdays={lag_days}",
            recommended_action=(
                "define deterministic valid_from, valid_until, stale_after and "
                "expiry policy before candidate search"
            ),
        ),
        _semantic_row(
            registry,
            semantic_id="no_stale_signal_carry_forward",
            semantic_role="stale_signal_suppression_contract",
            used_by_candidates=[FOCUSED_GROWTH_TILT_CANDIDATE_ID],
            source_config_or_artifact="config/research/dynamic_strategy_pit_input_registry.yaml",
            valid_from_source="depends on signal artifact valid_from",
            valid_until_source="depends on signal artifact valid_until",
            signal_horizon_source="must be inherited from signal validity contract",
            expiry_rule_source="owner-approved no-stale carry-forward rule missing",
            decay_rule_source="not separately validated",
            carry_forward_rule="hold_previous_actual_position currently configured",
            stale_signal_detection_rule="expired signal must be blocked or explicitly logged",
            signal_to_execution_lag_rule="must record lag before stale decision",
            recommended_action=(
                "block expired signal execution unless an explicit owner-approved "
                "carry-forward rule exists"
            ),
        ),
        _semantic_row(
            registry,
            semantic_id="signal_to_execution_lag",
            semantic_role="signal_decision_to_execution_delay",
            used_by_candidates=[FOCUSED_GROWTH_TILT_CANDIDATE_ID],
            source_config_or_artifact=(
                "config/research/strategy_execution_policy_registry.yaml:"
                f"{FOCUSED_GROWTH_TILT_CANDIDATE_ID}.rebalance_policy"
            ),
            valid_from_source="next executable time after generated_at",
            valid_until_source="must subtract or compare execution lag against expiry",
            signal_horizon_source="VALID_UNTIL_WINDOW_RESEARCH_POLICY",
            expiry_rule_source="lag currently policy-visible but not contract-bound",
            decay_rule_source="near-expiry lag handling missing",
            carry_forward_rule="lagged execution can hold previous actual position",
            stale_signal_detection_rule="lag must be measured before execution permission",
            signal_to_execution_lag_rule=f"execution_lag_bdays={lag_days}",
            recommended_action="record lag_days on every signal-to-trade decision row",
        ),
        {
            "semantic_id": "strategy_execution_policy_signal_validity_window",
            "semantic_role": "policy_configured_fixed_window",
            "used_by_candidates": [FOCUSED_GROWTH_TILT_CANDIDATE_ID],
            "source_config_or_artifact": (
                "config/research/strategy_execution_policy_registry.yaml:"
                f"{FOCUSED_GROWTH_TILT_CANDIDATE_ID}.signal_policy"
            ),
            "valid_from_source": signal_policy.get("signal_effective_earliest", "TBD"),
            "valid_until_source": f"signal_validity_window_bdays={validity_days}",
            "signal_horizon_source": "policy fixed window, not calibrated signal horizon",
            "expiry_rule_source": "fixed window; needs horizon alignment",
            "decay_rule_source": "none in focused policy",
            "carry_forward_rule": signal_policy.get("stale_signal_behavior", "TBD"),
            "stale_signal_detection_rule": "missing explicit stale_after field",
            "signal_to_execution_lag_rule": f"execution_lag_bdays={lag_days}",
            "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
            "pit_confidence": "LOW",
            "severity": "BLOCKING",
            "recommended_action": (
                "derive per-signal valid_until from signal horizon and generated_at "
                "instead of relying only on fixed window policy"
            ),
        },
        {
            "semantic_id": "signal_validity_taxonomy_profile",
            "semantic_role": "taxonomy_for_signal_half_life_and_stale_actions",
            "used_by_candidates": ["dynamic_strategy_research_profiles"],
            "source_config_or_artifact": "config/research/signal_validity_taxonomy.yaml",
            "valid_from_source": "taxonomy does not define per-signal valid_from",
            "valid_until_source": "taxonomy profiles define stale_after_days only for profiles",
            "signal_horizon_source": "fast / medium / slow / persistent bands",
            "expiry_rule_source": "research_only_pilot_baseline taxonomy",
            "decay_rule_source": "near_stale_within_days for named profiles",
            "carry_forward_rule": "allowed stale actions are governed but not signal-bound",
            "stale_signal_detection_rule": "profile-level stale_after_days",
            "signal_to_execution_lag_rule": "not taxonomy-owned",
            "pit_status": "APPROXIMATE_PIT",
            "pit_confidence": "MEDIUM",
            "severity": "MATERIAL",
            "recommended_action": (
                "bind taxonomy profile to each generated signal version before replay "
                "validation"
            ),
        },
        {
            "semantic_id": "growth_tilt_valid_until_alignment",
            "semantic_role": "growth_tilt_signal_horizon_to_expiry_mapping",
            "used_by_candidates": [FOCUSED_GROWTH_TILT_CANDIDATE_ID],
            "source_config_or_artifact": (
                "TRADING-2406 signal construction gap analysis + TRADING-2403 "
                "signal construction review"
            ),
            "valid_from_source": "missing in standalone growth tilt signal artifact",
            "valid_until_source": "not derived from growth tilt horizon",
            "signal_horizon_source": growth_review.get(
                "signal_horizon",
                "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
            ),
            "expiry_rule_source": "requires TRADING-2408 implementation design",
            "decay_rule_source": "near-expiry behavior not separately validated",
            "carry_forward_rule": signal_policy.get("stale_signal_behavior", "TBD"),
            "stale_signal_detection_rule": "must block or decay stale growth tilt signal",
            "signal_to_execution_lag_rule": f"execution_lag_bdays={lag_days}",
            "pit_status": CURRENT_PIT_STATUS,
            "pit_confidence": "LOW",
            "severity": "BLOCKING",
            "recommended_action": (
                "map growth tilt horizon, confidence and volatility state to "
                "valid_until/stale_after before severity downgrade"
            ),
        },
    ]
    return {
        "schema_version": "dynamic_strategy_valid_until_semantics_review.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "semantics": rows,
        "required_checks": [
            "fixed window vs signal horizon",
            "valid_from generated_at or next executable time",
            "carry-forward across refresh",
            "block after expiry",
            "near-expiry decay/block/lower-confidence",
            "signal-to-execution lag handling",
            "no-stale carry-forward consistency",
            "growth tilt horizon alignment",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _semantic_row(
    registry: Mapping[str, Any],
    *,
    semantic_id: str,
    semantic_role: str,
    used_by_candidates: list[str],
    source_config_or_artifact: str,
    valid_from_source: str,
    valid_until_source: str,
    signal_horizon_source: str,
    expiry_rule_source: str,
    decay_rule_source: str,
    carry_forward_rule: str,
    stale_signal_detection_rule: str,
    signal_to_execution_lag_rule: str,
    recommended_action: str,
) -> dict[str, Any]:
    entry = _registry_entry(registry, semantic_id)
    return {
        "semantic_id": semantic_id,
        "semantic_role": semantic_role,
        "used_by_candidates": used_by_candidates,
        "source_config_or_artifact": source_config_or_artifact,
        "valid_from_source": valid_from_source,
        "valid_until_source": valid_until_source,
        "signal_horizon_source": signal_horizon_source,
        "expiry_rule_source": expiry_rule_source,
        "decay_rule_source": decay_rule_source,
        "carry_forward_rule": carry_forward_rule,
        "stale_signal_detection_rule": stale_signal_detection_rule,
        "signal_to_execution_lag_rule": signal_to_execution_lag_rule,
        "pit_status": entry.get("pit_status", CURRENT_PIT_STATUS),
        "pit_confidence": entry.get("pit_confidence", "LOW"),
        "severity": entry.get("severity", "MATERIAL"),
        "recommended_action": entry.get("recommended_action") or recommended_action,
    }


def _build_stale_signal_risk_audit(sources: Mapping[str, Any]) -> dict[str, Any]:
    signal_review = _signal_construction_review(sources)
    source_gap = _as_mapping(signal_review.get("source_2402_signal_gap"))
    valid_until_strictness = _as_mapping(source_gap.get("valid_until_strictness"))
    lag_days = valid_until_strictness.get("signal_to_execution_lag_days", "TBD")
    stale_count = valid_until_strictness.get("stale_signal_execution_count", "TBD")
    risks = [
        {
            "risk_id": "VUW-STALE-001",
            "category": "VALID_UNTIL_UNGROUNDED",
            "affected_semantic_or_signal": INPUT_UNDER_REVIEW,
            "severity": "BLOCKING",
            "evidence": "valid_until exists as policy window but is not grounded per signal",
            "remediation_required": True,
            "recommended_fix": "emit valid_until from generated_at, valid_from and horizon",
        },
        {
            "risk_id": "VUW-STALE-002",
            "category": "CARRY_FORWARD_RISK",
            "affected_semantic_or_signal": "no_stale_signal_carry_forward",
            "severity": "BLOCKING",
            "evidence": "hold_previous_actual_position can carry stale exposure without owner rule",
            "remediation_required": True,
            "recommended_fix": "block expired carry-forward or require owner-approved rule",
        },
        {
            "risk_id": "VUW-STALE-003",
            "category": "SIGNAL_TO_EXECUTION_LAG_RISK",
            "affected_semantic_or_signal": "signal_to_execution_lag",
            "severity": "MATERIAL",
            "evidence": f"prior review observed lag_days={lag_days}; replay contract missing",
            "remediation_required": True,
            "recommended_fix": "record lag for every signal-to-execution decision",
        },
        {
            "risk_id": "VUW-STALE-004",
            "category": "NEAR_EXPIRY_OVERTRADING_RISK",
            "affected_semantic_or_signal": INPUT_UNDER_REVIEW,
            "severity": "MATERIAL",
            "evidence": "near-expiry signal behavior is not separately validated",
            "remediation_required": True,
            "recommended_fix": "define near-expiry decay, block, or refresh-required behavior",
        },
        {
            "risk_id": "VUW-STALE-005",
            "category": "SIGNAL_REFRESH_COLLISION_RISK",
            "affected_semantic_or_signal": "signal_version",
            "severity": "MATERIAL",
            "evidence": "new signal overlapping old signal lacks deterministic replacement rule",
            "remediation_required": True,
            "recommended_fix": "prefer newer as-of-safe valid signal and log collision decision",
        },
        {
            "risk_id": "VUW-STALE-006",
            "category": "STALE_REGIME_LABEL_RISK",
            "affected_semantic_or_signal": "growth_tilt_engine / regime context",
            "severity": "MATERIAL",
            "evidence": "growth tilt horizon and regime context are not tied to expiry",
            "remediation_required": True,
            "recommended_fix": "bind regime label timestamp and validity to signal contract",
        },
        {
            "risk_id": "VUW-STALE-007",
            "category": "VALID_FROM_MISSING_RISK",
            "affected_semantic_or_signal": "growth_tilt_engine_signal_artifact",
            "severity": "BLOCKING",
            "evidence": "standalone signal artifact lacks valid_from",
            "remediation_required": True,
            "recommended_fix": "emit valid_from as generated_at or next executable time",
        },
        {
            "risk_id": "VUW-STALE-008",
            "category": "VALID_UNTIL_MISSING_RISK",
            "affected_semantic_or_signal": "growth_tilt_engine_signal_artifact",
            "severity": "BLOCKING",
            "evidence": (
                "standalone signal artifact lacks valid_until; prior stale_count="
                f"{stale_count}"
            ),
            "remediation_required": True,
            "recommended_fix": "emit valid_until and stale_after on every signal record",
        },
    ]
    return {
        "schema_version": "dynamic_strategy_stale_signal_risk_audit.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "risks": risks,
        "blocking_risk_count": sum(1 for risk in risks if risk["severity"] == "BLOCKING"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_signal_validity_contract_plan(
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    execution_policy = _execution_policy_for_focused_candidate(
        _as_mapping(sources.get("execution_policy_registry"))
    )
    signal_policy = _as_mapping(execution_policy.get("signal_policy"))
    rebalance_policy = _as_mapping(execution_policy.get("rebalance_policy"))
    validity_days = signal_policy.get("signal_validity_window_bdays", "TBD")
    return {
        "schema_version": "dynamic_strategy_signal_validity_contract_plan.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "contract_plan_ready": True,
        "required_fields": [
            "signal_id",
            "as_of_date",
            "generated_at",
            "source_data_cutoff",
            "valid_from",
            "valid_until",
            "horizon_days",
            "expiry_rule",
            "stale_after",
            "confidence_if_available",
            "signal_version",
        ],
        "example_contract_template": {
            "signal_id": "growth_tilt_engine",
            "as_of_date": "YYYY-MM-DD",
            "generated_at": "YYYY-MM-DDTHH:MM:SSZ",
            "source_data_cutoff": "YYYY-MM-DD",
            "valid_from": "generated_at_or_next_executable_time",
            "valid_until": f"valid_from + governed_horizon(max_policy={validity_days})",
            "horizon_days": "TBD_FROM_SIGNAL_HORIZON",
            "expiry_rule": "BLOCK_AFTER_VALID_UNTIL",
            "stale_after": "valid_until_or_earlier_decay_boundary",
            "confidence_if_available": None,
            "signal_version": "deterministic_signal_version",
        },
        "invariants": [
            "valid_from >= generated_at_or_next_executable_time",
            "valid_until > valid_from",
            "valid_until <= valid_from + max_allowed_horizon",
            "stale_after <= valid_until",
            "expired_signal_cannot_trigger_new_trade",
            (
                "expired_signal_cannot_be_carried_forward_without_explicit_"
                "owner_approved_rule"
            ),
            "signal_to_execution_lag_must_be_recorded",
        ],
        "decision_policy": {
            "current_date > valid_until": "BLOCK_EXECUTION",
            "current_date > stale_after": "BLOCK_OR_DECAY_SIGNAL",
            "near valid_until": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
            "new signal overlaps old": "USE_NEWER_SIGNAL_IF_AS_OF_SAFE_AND_VALID",
            "missing valid_until": "BLOCK_CANDIDATE_SEARCH_FOR_DEPENDENT_STRATEGY",
        },
        "source_policy_context": {
            "signal_validity_window_bdays": validity_days,
            "signal_effective_earliest": signal_policy.get("signal_effective_earliest"),
            "stale_signal_behavior": signal_policy.get("stale_signal_behavior"),
            "execution_lag_bdays": rebalance_policy.get("execution_lag_bdays"),
        },
        "implemented_in_2407": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_growth_tilt_alignment_review(sources: Mapping[str, Any]) -> dict[str, Any]:
    remediation_2406 = _as_mapping(sources.get("remediation_plan_2406"))
    signal_gap = _as_mapping(
        _as_mapping(sources.get("signal_construction_gap_analysis_2406")).get(
            "signal_construction_gap_analysis"
        )
    )
    return {
        "schema_version": "dynamic_strategy_growth_tilt_valid_until_alignment_review.v1",
        "growth_tilt_engine_status_from_2406": remediation_2406.get(
            "current_severity",
            "BLOCKING",
        ),
        "valid_until_window_status": CURRENT_SEVERITY,
        "alignment_questions": [
            "what growth_tilt horizon should valid_until derive from",
            "should valid_until shrink for weak confidence or high volatility",
            "should strong growth tilt use longer validity than weak growth tilt",
            "should recovery regimes require more conservative expiry",
            "how should lag reduce executable remaining validity",
        ],
        "alignment_gap_summary": {
            "growth_tilt_signal_horizon": _as_mapping(
                signal_gap.get("signal_construction")
            ).get("horizon", "TBD"),
            "valid_until_derivation": "missing per-signal deterministic mapping",
            "confidence_to_expiry": "missing",
            "high_volatility_shrink_rule": "missing",
            "recovery_conservatism_rule": "missing",
        },
        "proposed_horizon_to_valid_until_mapping": [
            {
                "signal_horizon_class": "short_growth_tilt",
                "valid_until_rule": "valid_from + short governed horizon",
                "requires_owner_calibration": True,
            },
            {
                "signal_horizon_class": "medium_growth_tilt",
                "valid_until_rule": "valid_from + medium governed horizon",
                "requires_owner_calibration": True,
            },
            {
                "signal_horizon_class": "persistent_growth_tilt",
                "valid_until_rule": "valid_from + capped persistent horizon",
                "requires_owner_calibration": True,
            },
        ],
        "proposed_confidence_to_expiry_mapping": [
            {
                "confidence_band": "LOW_OR_MISSING",
                "expiry_policy": "shorten validity or block until confidence exists",
            },
            {
                "confidence_band": "MEDIUM",
                "expiry_policy": "use base horizon with near-expiry refresh requirement",
            },
            {
                "confidence_band": "HIGH",
                "expiry_policy": "allow base horizon only if replay validates no stale carry",
            },
        ],
        "trading_logic_changed_in_2407": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_remediation_plan() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_valid_until_window_remediation_plan.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "valid_until_window_blocking_gap_resolved": False,
        "plan_items": [
            {
                "plan_id": "P0_signal_validity_contract",
                "priority": "P0",
                "goal": "define required signal validity fields and invariants",
                "expected_result": "candidate search can later verify per-signal expiry",
                "implemented_in_2407": False,
            },
            {
                "plan_id": "P0_no_stale_carry_forward_contract",
                "priority": "P0",
                "goal": "expired signals cannot trigger new trades or silent carry-forward",
                "expected_result": "stale exposure is blocked or explicitly owner-approved",
                "implemented_in_2407": False,
            },
            {
                "plan_id": "P1_signal_to_execution_lag_contract",
                "priority": "P1",
                "goal": "record lag from signal generation to execution decision",
                "expected_result": "lag can be compared against remaining validity",
                "implemented_in_2407": False,
            },
            {
                "plan_id": "P1_near_expiry_signal_handling",
                "priority": "P1",
                "goal": "define decay, block, or refresh-required behavior near expiry",
                "expected_result": "near-expiry overtrading and stale trades are reviewable",
                "implemented_in_2407": False,
            },
            {
                "plan_id": "P1_horizon_to_valid_until_mapping",
                "priority": "P1",
                "goal": "derive valid_until from signal horizon, confidence and regime state",
                "expected_result": "growth tilt expiry matches intended signal half-life",
                "implemented_in_2407": False,
            },
            {
                "plan_id": "P2_replay_validation",
                "priority": "P2",
                "goal": "replay signal validity and stale decisions under as-of constraints",
                "expected_result": "future downgrade evidence can be audited",
                "implemented_in_2407": False,
            },
        ],
        "recommended_implementation_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_severity_downgrade_conditions() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_valid_until_window_severity_conditions.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "downgrade_executed_in_2407": False,
        "downgrade_from_BLOCKING_to_MATERIAL_requires": [
            "signal_validity_contract_defined",
            "valid_from_and_valid_until_fields",
            "stale_after_or_expiry_rule",
            "signal_to_execution_lag_rule",
            "no_stale_carry_forward_contract",
            "mapping_to_signal_horizon",
            "owner_review_recorded",
        ],
        "downgrade_from_MATERIAL_to_APPROVED_APPROXIMATE_PIT_requires": [
            "replay_can_reconstruct_valid_from_and_valid_until",
            "expired_signal_count_measured",
            "no_unexplained_carry_forward",
            "near_expiry_behavior_documented",
            "caveats_documented",
            "owner_approval_recorded",
        ],
        "mark_TRUE_PIT_requires": [
            "deterministic_validity_fields",
            "source_data_cutoff_recorded",
            "no_stale_signal_execution_unless_explicitly_allowed",
            "validation_test_coverage",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_validation_plan() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_valid_until_window_validation_plan.v1",
        "input_under_review": INPUT_UNDER_REVIEW,
        "validation_plan_ready": True,
        "schema_validation": [
            "required fields exist for every generated signal",
            "valid_until > valid_from",
            "stale_after <= valid_until",
            "source_data_cutoff is present and not after as_of_date",
        ],
        "stale_replay": [
            "expired signals do not execute",
            "signal-to-execution lag is measured",
            "near-expiry handling is deterministic",
            "carry-forward is logged or blocked",
        ],
        "candidate_gate": [
            "candidate search remains blocked while valid_until_window is BLOCKING",
            "gate changes only after evidence and owner review",
            "paper-shadow and production remain disabled",
        ],
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "2406 and 2407 define both blockers' remediation plans; 2408 should "
            "design implementation sequence for as-of/signal validity contracts, "
            "replay validation, and PIT severity downgrade consideration."
        ),
        "candidate_search_remains_blocked": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_outputs(
    payload: dict[str, Any],
    *,
    output_root: Path,
    docs_root: Path,
) -> None:
    paths = {
        "json_path": str(output_root / "remediation_plan_result.json"),
        "valid_until_semantics_review_json": str(
            output_root / "valid_until_semantics_review.json"
        ),
        "stale_signal_risk_audit_json": str(output_root / "stale_signal_risk_audit.json"),
        "signal_validity_contract_plan_json": str(
            output_root / "signal_validity_contract_plan.json"
        ),
        "severity_downgrade_conditions_json": str(
            output_root / "severity_downgrade_conditions.json"
        ),
        "validation_plan_json": str(output_root / "validation_plan.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_valid_until_window_stale_signal_remediation_plan.md"
        ),
        "valid_until_semantics_review_markdown": str(
            docs_root / "dynamic_strategy_valid_until_semantics_review.md"
        ),
        "stale_signal_risk_audit_markdown": str(
            docs_root / "dynamic_strategy_stale_signal_risk_audit.md"
        ),
        "signal_validity_contract_markdown": str(
            docs_root / "dynamic_strategy_signal_validity_contract.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2408_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    for path_key, report_type, schema_key, payload_key in (
        (
            "valid_until_semantics_review_json",
            "dynamic_strategy_valid_until_semantics_review",
            "dynamic_strategy_valid_until_semantics_review.v1",
            "valid_until_semantics_review",
        ),
        (
            "stale_signal_risk_audit_json",
            "dynamic_strategy_stale_signal_risk_audit",
            "dynamic_strategy_stale_signal_risk_audit.v1",
            "stale_signal_risk_audit",
        ),
        (
            "signal_validity_contract_plan_json",
            "dynamic_strategy_signal_validity_contract_plan",
            "dynamic_strategy_signal_validity_contract_plan.v1",
            "signal_validity_contract_plan",
        ),
        (
            "severity_downgrade_conditions_json",
            "dynamic_strategy_valid_until_window_severity_conditions",
            "dynamic_strategy_valid_until_window_severity_conditions.v1",
            "severity_downgrade_conditions",
        ),
        (
            "validation_plan_json",
            "dynamic_strategy_valid_until_window_validation_plan",
            "dynamic_strategy_valid_until_window_validation_plan.v1",
            "validation_plan",
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
        Path(paths["valid_until_semantics_review_markdown"]),
        _semantics_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["stale_signal_risk_audit_markdown"]),
        _risk_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["signal_validity_contract_markdown"]),
        _contract_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy valid-until window stale signal remediation plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- input under review：`{payload.get('input_under_review')}`",
            f"- current severity：`{payload.get('current_severity')}`",
            f"- current PIT status：`{payload.get('current_pit_status')}`",
            (
                "- valid-until semantics review ready："
                f"`{payload.get('valid_until_semantics_review_ready')}`"
            ),
            (
                "- stale signal risk audit ready："
                f"`{payload.get('stale_signal_risk_audit_ready')}`"
            ),
            (
                "- signal validity contract plan ready："
                f"`{payload.get('signal_validity_contract_plan_ready')}`"
            ),
            (
                "- growth tilt alignment review ready："
                f"`{payload.get('growth_tilt_alignment_review_ready')}`"
            ),
            f"- remediation plan ready：`{payload.get('remediation_plan_ready')}`",
            (
                "- severity downgrade conditions ready："
                f"`{payload.get('severity_downgrade_conditions_ready')}`"
            ),
            f"- validation plan ready：`{payload.get('validation_plan_ready')}`",
            (
                "- valid_until_window blocking gap resolved："
                f"`{payload.get('valid_until_window_blocking_gap_resolved')}`"
            ),
            (
                "- valid_until_window severity downgraded："
                f"`{payload.get('valid_until_window_severity_downgraded')}`"
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
            "## Current blocker",
            "",
            _json_block(payload.get("current_blocker", {})),
            "",
            "## Valid-until semantics review",
            "",
            _json_block(payload.get("valid_until_semantics_review", {})),
            "",
            "## Stale signal risk audit",
            "",
            _json_block(payload.get("stale_signal_risk_audit", {})),
            "",
            "## Signal validity contract plan",
            "",
            _json_block(payload.get("signal_validity_contract_plan", {})),
            "",
            "## Growth tilt alignment review",
            "",
            _json_block(payload.get("growth_tilt_alignment_review", {})),
            "",
            "## Remediation plan",
            "",
            _json_block(payload.get("remediation_plan", {})),
            "",
            "## Severity downgrade conditions",
            "",
            _json_block(payload.get("severity_downgrade_conditions", {})),
            "",
            "## Validation plan",
            "",
            _json_block(payload.get("validation_plan", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
        ]
    )


def _semantics_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy valid-until semantics review",
            "",
            f"- status：`{payload.get('status')}`",
            f"- semantic row count：`{payload.get('valid_until_semantics_review_count')}`",
            "",
            _json_block(payload.get("valid_until_semantics_review", {})),
            "",
        ]
    )


def _risk_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy stale signal risk audit",
            "",
            f"- status：`{payload.get('status')}`",
            f"- risk count：`{payload.get('stale_signal_risk_audit_count')}`",
            "",
            _json_block(payload.get("stale_signal_risk_audit", {})),
            "",
        ]
    )


def _contract_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy signal validity contract",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _json_block(payload.get("signal_validity_contract_plan", {})),
            "",
            "## Growth tilt alignment review",
            "",
            _json_block(payload.get("growth_tilt_alignment_review", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2408 route",
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


def _pit_row(sources: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    for source_key in ("pit_matrix_2405", "pit_matrix_2403"):
        source = _as_mapping(sources.get(source_key))
        rows = _as_list(
            _as_mapping(source.get("pit_coverage_matrix")).get("pit_coverage_matrix")
        )
        if not rows:
            rows = _as_list(source.get("pit_coverage_matrix"))
        for row in (_as_mapping(item) for item in rows):
            if row.get("input_id") == input_id:
                return row
    return {}


def _signal_construction_review(sources: Mapping[str, Any]) -> Mapping[str, Any]:
    return _as_mapping(
        _as_mapping(sources.get("signal_construction_review_2403")).get(
            "signal_construction_review"
        )
    )


def _registry_entry(registry: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    entries = _as_list(registry.get("entries"))
    for entry in (_as_mapping(item) for item in entries):
        if entry.get("input_id") == input_id:
            return entry
    return {}


def _execution_policy_for_focused_candidate(
    registry: Mapping[str, Any],
) -> Mapping[str, Any]:
    for policy in (
        _as_mapping(item)
        for item in _as_list(registry.get("strategy_execution_policies"))
    ):
        if policy.get("strategy_id") == FOCUSED_GROWTH_TILT_CANDIDATE_ID:
            return policy
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
