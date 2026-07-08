from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411
import ai_trading_system.dynamic_strategy_valid_until_window_stale_signal_remediation_plan as m2407
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as m2414,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_upstream_artifact_closure as m2417,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import (
    json_block as _json_block,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_text_document_or_missing_flag as _load_text_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
    write_section_json_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY
from ai_trading_system.research_quality import (
    growth_tilt_engine_valid_until_dependency_evidence_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2418"
TASK_REGISTER_ID = (
    "TRADING-2418_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE"
)
REPORT_TYPE = "growth_tilt_engine_valid_until_dependency_evidence_closure"
SCHEMA_VERSION = closure.CLOSURE_SCHEMA_VERSION
READY_STATUS = closure.READY_STATUS
BLOCKED_SOURCE_STATUS = (
    "GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_BLOCKED_SOURCE"
)
NEXT_ROUTE = closure.NEXT_ROUTE
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2407",
    "TRADING-2411",
    "TRADING-2414",
    "TRADING-2415",
    "TRADING-2416",
    "TRADING-2417",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_PRIOR_"
    "ARTIFACTS_AND_CONFIGS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "mark_any_source_feature_pit_gate_ready",
    "mark_any_source_feature_contract_ready",
    "downgrade_growth_tilt_engine_blocker",
    "downgrade_valid_until_window_blocker",
    "clear_growth_tilt_engine_blocking_gap",
    "clear_valid_until_window_blocking_gap",
    "resume_candidate_search",
    "approve_research_only_observation",
    "enable_paper_shadow",
    "create_paper_trade",
    "create_shadow_position",
    "enable_scheduler",
    "append_historical_event_log",
    "bind_outcome",
    "mutate_outcome_store",
    "enable_production",
    "call_broker_api",
    "send_order",
    "create_scheduled_task",
    "generate_daily_report",
    "run_new_strategy_backtest",
    "generate_new_trading_signal",
    "run_scoring",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "growth_tilt_engine_blocking_gap_resolved",
    "growth_tilt_engine_severity_downgraded",
    "valid_until_window_blocking_gap_resolved",
    "valid_until_window_severity_downgraded",
    "growth_tilt_engine_blocker_resolved",
    "growth_tilt_engine_blocker_downgraded",
    "valid_until_window_blocker_resolved",
    "valid_until_window_blocker_downgraded",
    "blockers_resolved",
    "blockers_downgraded",
    "candidate_search_enabled",
    "candidate_search_allowed",
    "candidate_search_resumed",
    "observation_enabled",
    "research_only_observation_allowed",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_store_mutated",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_feature_generated",
    "new_signal_generated",
    "new_strategy_backtest_run",
    "scoring_run",
)

DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "closure_result.json"
)
DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "source_traceability_closure_evidence.json"
)
DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "upstream_artifact_closure_evidence.json"
)
DEFAULT_SOURCE_2417_UPDATED_SOURCE_FEATURE_MAPPING_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "updated_source_feature_mapping.json"
)
DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "closure_plan_result.json"
)
DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "remaining_blocker_matrix.json"
)
DEFAULT_SOURCE_2416_VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "valid_until_dependency_closure_plan.json"
)
DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "pit_gate_evidence_requirements.json"
)
DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_snapshot_result.json"
)
DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_matrix.json"
)
DEFAULT_SOURCE_2414_REMEDIATION_RESULT_PATH = (
    m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    / "signal_validity_dependency_remediation_result.json"
)
DEFAULT_SOURCE_2414_CONTRACT_METADATA_PATH = (
    m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    / "signal_validity_dependency_contract_metadata.json"
)
DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "remediation_plan_result.json"
)
DEFAULT_SOURCE_2407_REMEDIATION_PLAN_RESULT_PATH = (
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
DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH = (
    m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "validation_plan.json"
)
DEFAULT_PIT_INPUT_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_strategy_pit_input_registry.yaml"
)
DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "strategy_execution_policy_registry.yaml"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_valid_until_dependency_evidence_closure(
    *,
    source_2417_closure_result_path: Path = DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH,
    source_2417_source_traceability_closure_evidence_path: Path = (
        DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH
    ),
    source_2417_upstream_artifact_closure_evidence_path: Path = (
        DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH
    ),
    source_2417_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2417_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2417_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2416_closure_result_path: Path = DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Path = (
        DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH
    ),
    source_2416_valid_until_dependency_closure_plan_path: Path = (
        DEFAULT_SOURCE_2416_VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_PATH
    ),
    source_2416_pit_gate_evidence_requirements_path: Path = (
        DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH
    ),
    source_2415_readiness_snapshot_result_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH
    ),
    source_2415_readiness_matrix_path: Path = DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2414_remediation_result_path: Path = (
        DEFAULT_SOURCE_2414_REMEDIATION_RESULT_PATH
    ),
    source_2414_contract_metadata_path: Path = (
        DEFAULT_SOURCE_2414_CONTRACT_METADATA_PATH
    ),
    source_2414_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2411_remediation_plan_result_path: Path = (
        DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH
    ),
    source_2407_remediation_plan_result_path: Path = (
        DEFAULT_SOURCE_2407_REMEDIATION_PLAN_RESULT_PATH
    ),
    source_2407_valid_until_semantics_review_path: Path = (
        DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH
    ),
    source_2407_stale_signal_risk_audit_path: Path = (
        DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH
    ),
    source_2407_signal_validity_contract_plan_path: Path = (
        DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH
    ),
    source_2407_validation_plan_path: Path = DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH,
    pit_input_registry_path: Path = DEFAULT_PIT_INPUT_REGISTRY_PATH,
    strategy_execution_policy_registry_path: Path = (
        DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH
    ),
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2417_closure_result_path=source_2417_closure_result_path,
        source_2417_source_traceability_closure_evidence_path=(
            source_2417_source_traceability_closure_evidence_path
        ),
        source_2417_upstream_artifact_closure_evidence_path=(
            source_2417_upstream_artifact_closure_evidence_path
        ),
        source_2417_updated_source_feature_mapping_path=(
            source_2417_updated_source_feature_mapping_path
        ),
        source_2417_remaining_blocker_summary_path=(
            source_2417_remaining_blocker_summary_path
        ),
        source_2416_closure_result_path=source_2416_closure_result_path,
        source_2416_remaining_blocker_matrix_path=(
            source_2416_remaining_blocker_matrix_path
        ),
        source_2416_valid_until_dependency_closure_plan_path=(
            source_2416_valid_until_dependency_closure_plan_path
        ),
        source_2416_pit_gate_evidence_requirements_path=(
            source_2416_pit_gate_evidence_requirements_path
        ),
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2414_remediation_result_path=source_2414_remediation_result_path,
        source_2414_contract_metadata_path=source_2414_contract_metadata_path,
        source_2414_remaining_blocker_summary_path=(
            source_2414_remaining_blocker_summary_path
        ),
        source_2411_remediation_plan_result_path=(
            source_2411_remediation_plan_result_path
        ),
        source_2407_remediation_plan_result_path=(
            source_2407_remediation_plan_result_path
        ),
        source_2407_valid_until_semantics_review_path=(
            source_2407_valid_until_semantics_review_path
        ),
        source_2407_stale_signal_risk_audit_path=(
            source_2407_stale_signal_risk_audit_path
        ),
        source_2407_signal_validity_contract_plan_path=(
            source_2407_signal_validity_contract_plan_path
        ),
        source_2407_validation_plan_path=source_2407_validation_plan_path,
        pit_input_registry_path=pit_input_registry_path,
        strategy_execution_policy_registry_path=strategy_execution_policy_registry_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
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
    source_2417_closure_result_path: Path,
    source_2417_source_traceability_closure_evidence_path: Path,
    source_2417_upstream_artifact_closure_evidence_path: Path,
    source_2417_updated_source_feature_mapping_path: Path,
    source_2417_remaining_blocker_summary_path: Path,
    source_2416_closure_result_path: Path,
    source_2416_remaining_blocker_matrix_path: Path,
    source_2416_valid_until_dependency_closure_plan_path: Path,
    source_2416_pit_gate_evidence_requirements_path: Path,
    source_2415_readiness_snapshot_result_path: Path,
    source_2415_readiness_matrix_path: Path,
    source_2414_remediation_result_path: Path,
    source_2414_contract_metadata_path: Path,
    source_2414_remaining_blocker_summary_path: Path,
    source_2411_remediation_plan_result_path: Path,
    source_2407_remediation_plan_result_path: Path,
    source_2407_valid_until_semantics_review_path: Path,
    source_2407_stale_signal_risk_audit_path: Path,
    source_2407_signal_validity_contract_plan_path: Path,
    source_2407_validation_plan_path: Path,
    pit_input_registry_path: Path,
    strategy_execution_policy_registry_path: Path,
    report_registry_path: Path,
    artifact_catalog_path: Path,
) -> dict[str, Any]:
    report_registry = safe_load_yaml_path(report_registry_path)
    pit_input_registry = safe_load_yaml_path(pit_input_registry_path)
    strategy_policy_registry = safe_load_yaml_path(strategy_execution_policy_registry_path)
    if isinstance(report_registry, dict):
        report_registry = {**report_registry, "path": str(report_registry_path)}
    if isinstance(pit_input_registry, dict):
        pit_input_registry = {**pit_input_registry, "path": str(pit_input_registry_path)}
    if isinstance(strategy_policy_registry, dict):
        strategy_policy_registry = {
            **strategy_policy_registry,
            "path": str(strategy_execution_policy_registry_path),
        }
    return {
        "closure_result_2417": _load_json_document(source_2417_closure_result_path),
        "source_traceability_closure_evidence_2417": _load_json_document(
            source_2417_source_traceability_closure_evidence_path
        ),
        "upstream_artifact_closure_evidence_2417": _load_json_document(
            source_2417_upstream_artifact_closure_evidence_path
        ),
        "updated_source_feature_mapping_2417": _load_json_document(
            source_2417_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2417": _load_json_document(
            source_2417_remaining_blocker_summary_path
        ),
        "closure_result_2416": _load_json_document(source_2416_closure_result_path),
        "remaining_blocker_matrix_2416": _load_json_document(
            source_2416_remaining_blocker_matrix_path
        ),
        "valid_until_dependency_closure_plan_2416": _load_json_document(
            source_2416_valid_until_dependency_closure_plan_path
        ),
        "pit_gate_evidence_requirements_2416": _load_json_document(
            source_2416_pit_gate_evidence_requirements_path
        ),
        "readiness_snapshot_result_2415": _load_json_document(
            source_2415_readiness_snapshot_result_path
        ),
        "readiness_matrix_2415": _load_json_document(source_2415_readiness_matrix_path),
        "signal_validity_dependency_remediation_result_2414": _load_json_document(
            source_2414_remediation_result_path
        ),
        "signal_validity_dependency_contract_metadata_2414": _load_json_document(
            source_2414_contract_metadata_path
        ),
        "remaining_blocker_summary_2414": _load_json_document(
            source_2414_remaining_blocker_summary_path
        ),
        "remediation_plan_result_2411": _load_json_document(
            source_2411_remediation_plan_result_path
        ),
        "remediation_plan_result_2407": _load_json_document(
            source_2407_remediation_plan_result_path
        ),
        "valid_until_semantics_review_2407": _load_json_document(
            source_2407_valid_until_semantics_review_path
        ),
        "stale_signal_risk_audit_2407": _load_json_document(
            source_2407_stale_signal_risk_audit_path
        ),
        "signal_validity_contract_plan_2407": _load_json_document(
            source_2407_signal_validity_contract_plan_path
        ),
        "validation_plan_2407": _load_json_document(source_2407_validation_plan_path),
        "pit_input_registry": pit_input_registry,
        "strategy_execution_policy_registry": strategy_policy_registry,
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "closure_result_2417": str(source_2417_closure_result_path),
            "source_traceability_closure_evidence_2417": str(
                source_2417_source_traceability_closure_evidence_path
            ),
            "upstream_artifact_closure_evidence_2417": str(
                source_2417_upstream_artifact_closure_evidence_path
            ),
            "updated_source_feature_mapping_2417": str(
                source_2417_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2417": str(
                source_2417_remaining_blocker_summary_path
            ),
            "closure_result_2416": str(source_2416_closure_result_path),
            "remaining_blocker_matrix_2416": str(
                source_2416_remaining_blocker_matrix_path
            ),
            "valid_until_dependency_closure_plan_2416": str(
                source_2416_valid_until_dependency_closure_plan_path
            ),
            "pit_gate_evidence_requirements_2416": str(
                source_2416_pit_gate_evidence_requirements_path
            ),
            "readiness_snapshot_result_2415": str(
                source_2415_readiness_snapshot_result_path
            ),
            "readiness_matrix_2415": str(source_2415_readiness_matrix_path),
            "signal_validity_dependency_remediation_result_2414": str(
                source_2414_remediation_result_path
            ),
            "signal_validity_dependency_contract_metadata_2414": str(
                source_2414_contract_metadata_path
            ),
            "remaining_blocker_summary_2414": str(
                source_2414_remaining_blocker_summary_path
            ),
            "remediation_plan_result_2411": str(
                source_2411_remediation_plan_result_path
            ),
            "remediation_plan_result_2407": str(
                source_2407_remediation_plan_result_path
            ),
            "valid_until_semantics_review_2407": str(
                source_2407_valid_until_semantics_review_path
            ),
            "stale_signal_risk_audit_2407": str(
                source_2407_stale_signal_risk_audit_path
            ),
            "signal_validity_contract_plan_2407": str(
                source_2407_signal_validity_contract_plan_path
            ),
            "validation_plan_2407": str(source_2407_validation_plan_path),
            "pit_input_registry": str(pit_input_registry_path),
            "strategy_execution_policy_registry": str(
                strategy_execution_policy_registry_path
            ),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "closure_result_2417",
        "source_traceability_closure_evidence_2417",
        "upstream_artifact_closure_evidence_2417",
        "updated_source_feature_mapping_2417",
        "remaining_blocker_summary_2417",
        "closure_result_2416",
        "remaining_blocker_matrix_2416",
        "valid_until_dependency_closure_plan_2416",
        "pit_gate_evidence_requirements_2416",
        "readiness_snapshot_result_2415",
        "readiness_matrix_2415",
        "signal_validity_dependency_remediation_result_2414",
        "signal_validity_dependency_contract_metadata_2414",
        "remaining_blocker_summary_2414",
        "remediation_plan_result_2411",
        "remediation_plan_result_2407",
        "valid_until_semantics_review_2407",
        "stale_signal_risk_audit_2407",
        "signal_validity_contract_plan_2407",
        "validation_plan_2407",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")

    artifact_catalog = _as_mapping(sources.get("artifact_catalog"))
    if artifact_catalog.get("_missing") is True:
        errors.append(
            f"missing source document: artifact_catalog -> {artifact_catalog.get('_path')}"
        )

    result_2417 = _as_mapping(sources.get("closure_result_2417"))
    if result_2417.get("status") != m2417.READY_STATUS:
        errors.append("2417 closure result must be ready")
    if result_2417.get("recommended_next_research_task") != m2417.NEXT_ROUTE:
        errors.append("2417 closure result must route to TRADING-2418")
    for field, expected in _expected_gate_counts().items():
        if result_2417.get(field) != expected:
            errors.append(f"2417 closure result {field} must be {expected}")
    if result_2417.get("source_traceability_still_blocked_count") != 1:
        errors.append("2417 must keep one source traceability blocker")
    if result_2417.get("valid_until_window_blocking_gap_resolved") is not False:
        errors.append("2417 must not resolve valid_until_window blocker")
    for field in m2417.SAFETY_FALSE_FIELDS:
        if result_2417.get(field) is True:
            errors.append(f"2417 safety field must remain false: {field}")

    summary_2417 = _section(
        _as_mapping(sources.get("remaining_blocker_summary_2417")),
        "remaining_blocker_summary",
    )
    if closure.SOURCE_TRACEABILITY_STILL_BLOCKED_FEATURE not in _as_list(
        summary_2417.get("source_traceability_still_blocked_feature_ids")
    ):
        errors.append("2417 summary must keep growth_tilt_engine_signal_artifact")
    if closure.DEPENDENCY_FEATURE_ID not in _as_list(
        summary_2417.get("valid_until_window_blocked_feature_ids")
    ):
        errors.append("2417 summary must keep execution_signal_validity_policy")

    result_2416 = _as_mapping(sources.get("closure_result_2416"))
    if result_2416.get("status") != m2416.READY_STATUS:
        errors.append("2416 closure plan result must be ready")
    for field, expected in _expected_gate_counts().items():
        if result_2416.get(field) != expected:
            errors.append(f"2416 closure result {field} must be {expected}")

    matrix_2416 = _section(
        _as_mapping(sources.get("remaining_blocker_matrix_2416")),
        "remaining_blocker_matrix",
    )
    matrix_rows = [_as_mapping(row) for row in _as_list(matrix_2416.get("matrix_rows"))]
    dependency_row = _row_by_feature(matrix_rows, closure.DEPENDENCY_FEATURE_ID)
    if len(matrix_rows) != 10:
        errors.append("2416 remaining blocker matrix must contain 10 rows")
    if dependency_row.get("blocked_by_valid_until_window") is not True:
        errors.append("2416 matrix must keep execution_signal_validity_policy blocked")

    valid_until_plan = _section(
        _as_mapping(sources.get("valid_until_dependency_closure_plan_2416")),
        "valid_until_dependency_closure_plan",
    )
    if valid_until_plan.get("dependent_feature_or_signal_count") != 1:
        errors.append("2416 valid-until plan must have one dependent feature")
    if closure.DEPENDENCY_FEATURE_ID not in _as_list(
        valid_until_plan.get("dependent_feature_ids")
    ):
        errors.append("2416 valid-until plan must target execution policy feature")

    result_2415 = _as_mapping(sources.get("readiness_snapshot_result_2415"))
    if result_2415.get("status") != m2415.READY_STATUS:
        errors.append("2415 readiness snapshot must be ready")
    if result_2415.get("blocked_by_valid_until_window_count") != 1:
        errors.append("2415 readiness snapshot must keep one valid_until blocker")
    readiness_matrix = _section(
        _as_mapping(sources.get("readiness_matrix_2415")),
        "pit_gate_readiness_matrix",
    )
    if len(_as_list(readiness_matrix.get("matrix_rows"))) != 10:
        errors.append("2415 readiness matrix must contain 10 rows")

    result_2414 = _as_mapping(
        sources.get("signal_validity_dependency_remediation_result_2414")
    )
    if result_2414.get("status") != m2414.READY_STATUS:
        errors.append("2414 signal validity remediation must be ready")
    if result_2414.get("validity_dependency_blocked_by_valid_until_window_count") != 1:
        errors.append("2414 must keep one valid_until dependency blocker")

    metadata_2414 = _section(
        _as_mapping(sources.get("signal_validity_dependency_contract_metadata_2414")),
        "signal_validity_dependency_contract_metadata",
    )
    metadata_rows = [
        _as_mapping(row) for row in _as_list(metadata_2414.get("metadata_rows"))
    ]
    metadata_row = _row_by_feature(metadata_rows, closure.DEPENDENCY_FEATURE_ID)
    if metadata_row.get("validity_blocking_reason") != "valid_until_window_unresolved":
        errors.append("2414 metadata must keep valid_until_window_unresolved")

    summary_2414 = _section(
        _as_mapping(sources.get("remaining_blocker_summary_2414")),
        "remaining_blocker_summary",
    )
    if summary_2414.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2414 summary must not resolve valid_until_window blocker")

    result_2411 = _as_mapping(sources.get("remediation_plan_result_2411"))
    if result_2411.get("status") != m2411.READY_STATUS:
        errors.append("2411 remediation plan must be ready")
    if result_2411.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2411 must not resolve valid_until_window blocker")

    result_2407 = _as_mapping(sources.get("remediation_plan_result_2407"))
    if result_2407.get("status") != m2407.READY_STATUS:
        errors.append("2407 valid-until plan must be ready")
    if result_2407.get("valid_until_window_blocking_gap_resolved") is not False:
        errors.append("2407 must not resolve valid_until_window blocker")
    if result_2407.get("valid_until_window_severity_downgraded") is not False:
        errors.append("2407 must not downgrade valid_until_window")

    contract_plan = _section(
        _as_mapping(sources.get("signal_validity_contract_plan_2407")),
        "signal_validity_contract_plan",
    )
    if contract_plan.get("contract_plan_ready") is not True:
        errors.append("2407 signal validity contract plan must be ready")
    if not _as_mapping(contract_plan.get("decision_policy")):
        errors.append("2407 contract plan missing decision policy")

    stale_audit = _section(
        _as_mapping(sources.get("stale_signal_risk_audit_2407")),
        "stale_signal_risk_audit",
    )
    if len(_as_list(stale_audit.get("risks"))) < 4:
        errors.append("2407 stale signal risk audit must expose risk rows")

    pit_registry = _as_mapping(sources.get("pit_input_registry"))
    if _pit_registry_severity(pit_registry, "growth_tilt_engine") != "BLOCKING":
        errors.append("PIT input registry must keep growth_tilt_engine BLOCKING")
    if _pit_registry_severity(pit_registry, "valid_until_window") != "BLOCKING":
        errors.append("PIT input registry must keep valid_until_window BLOCKING")

    strategy_registry = _as_mapping(sources.get("strategy_execution_policy_registry"))
    strategy_entry = _strategy_entry(strategy_registry, closure.TARGET_STRATEGY_ID)
    if not strategy_entry:
        errors.append("strategy execution policy registry missing focused strategy")
    if _as_mapping(strategy_entry.get("signal_policy")).get(
        "signal_validity_window_bdays"
    ) is None:
        errors.append("focused strategy signal policy missing validity window")
    if _as_mapping(strategy_entry.get("rebalance_policy")).get(
        "execution_lag_bdays"
    ) is None:
        errors.append("focused strategy rebalance policy missing execution lag")

    report_registry = _as_mapping(sources.get("report_registry"))
    for report_id in (
        "dynamic_strategy_valid_until_window_stale_signal_remediation_plan",
        "growth_tilt_engine_contract_gap_remediation_plan",
        "growth_tilt_engine_signal_validity_dependency_remediation",
        "growth_tilt_engine_pit_gate_readiness_snapshot",
        "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
        "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        REPORT_TYPE,
    ):
        if not _registry_has_report_id(report_registry, report_id):
            errors.append(f"report registry missing required entry: {report_id}")

    catalog_text = str(artifact_catalog.get("text", ""))
    for command in (
        "dynamic-strategy-valid-until-window-stale-signal-remediation-plan",
        "growth-tilt-engine-signal-validity-dependency-remediation",
        "growth-tilt-engine-source-traceability-upstream-artifact-closure",
        "growth-tilt-engine-valid-until-dependency-evidence-closure",
    ):
        if command not in catalog_text:
            errors.append(f"artifact catalog missing command: {command}")
    return errors


def _base_payload(
    *,
    status: str,
    sources: Mapping[str, Any],
    as_of_date: date | None,
    source_validation_errors: list[str],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
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
        "manual_review_required": True,
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "auto_downgrade_blocker": False,
        "owner_review_required_before_downgrade": True,
        "production_effect": "none",
        "broker_action": "none",
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }
    for field in SAFETY_FALSE_FIELDS:
        payload[field] = False
    return payload


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    result = closure.build_growth_tilt_valid_until_dependency_evidence_closure(
        _as_mapping(sources.get("closure_result_2417")),
        _as_mapping(sources.get("remaining_blocker_summary_2417")),
        _as_mapping(sources.get("closure_result_2416")),
        _as_mapping(sources.get("remaining_blocker_matrix_2416")),
        _as_mapping(sources.get("valid_until_dependency_closure_plan_2416")),
        _as_mapping(sources.get("readiness_snapshot_result_2415")),
        _as_mapping(sources.get("readiness_matrix_2415")),
        _as_mapping(sources.get("signal_validity_dependency_remediation_result_2414")),
        _as_mapping(sources.get("signal_validity_dependency_contract_metadata_2414")),
        _as_mapping(sources.get("remaining_blocker_summary_2414")),
        _as_mapping(sources.get("remediation_plan_result_2411")),
        _as_mapping(sources.get("remediation_plan_result_2407")),
        _as_mapping(sources.get("valid_until_semantics_review_2407")),
        _as_mapping(sources.get("stale_signal_risk_audit_2407")),
        _as_mapping(sources.get("signal_validity_contract_plan_2407")),
        _as_mapping(sources.get("validation_plan_2407")),
        pit_input_registry=_as_mapping(sources.get("pit_input_registry")),
        strategy_execution_policy_registry=_as_mapping(
            sources.get("strategy_execution_policy_registry")
        ),
    )
    return {
        **result,
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_feature_count": 0,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 0,
        "blocked_by_source_traceability_count": 0,
        "blocked_by_valid_until_window_count": 0,
        "valid_until_window_dependency_blocker_count_from_2415": 0,
        "valid_until_dependency_evidence_row_count": 0,
        "valid_until_dependency_pre_recheck_evidence_ready_count": 0,
        "valid_until_dependency_still_blocked_count": 0,
        "valid_until_dependency_evidence_ready": False,
        "signal_validity_contract_evidence_ready": False,
        "stale_signal_policy_evidence_ready": False,
        "growth_tilt_valid_until_alignment_evidence_ready": False,
        "remaining_blocker_summary_ready": False,
        "source_traceability_still_blocked": [],
        "valid_until_dependency_evidence": {},
        "signal_validity_contract_evidence": {},
        "stale_signal_policy_evidence": {},
        "growth_tilt_valid_until_alignment_evidence": {},
        "remaining_blocker_summary": {},
        "closure_validation": {},
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "closure_result.json"),
        "valid_until_dependency_evidence_json": str(
            output_root / "valid_until_dependency_evidence.json"
        ),
        "signal_validity_contract_evidence_json": str(
            output_root / "signal_validity_contract_evidence.json"
        ),
        "stale_signal_policy_evidence_json": str(
            output_root / "stale_signal_policy_evidence.json"
        ),
        "growth_tilt_valid_until_alignment_evidence_json": str(
            output_root / "growth_tilt_valid_until_alignment_evidence.json"
        ),
        "remaining_blocker_summary_json": str(
            output_root / "remaining_blocker_summary.json"
        ),
        "markdown_path": str(
            docs_root / "growth_tilt_engine_valid_until_dependency_evidence_closure.md"
        ),
        "signal_validity_contract_evidence_markdown": str(
            docs_root / "growth_tilt_engine_signal_validity_contract_evidence.md"
        ),
        "stale_signal_policy_evidence_markdown": str(
            docs_root / "growth_tilt_engine_stale_signal_policy_evidence.md"
        ),
        "growth_tilt_valid_until_alignment_evidence_markdown": str(
            docs_root / "growth_tilt_engine_valid_until_alignment_evidence.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2419_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_section_json_artifact(
        paths["valid_until_dependency_evidence_json"],
        "growth_tilt_engine_valid_until_dependency_evidence",
        closure.VALID_UNTIL_DEPENDENCY_EVIDENCE_SCHEMA_VERSION,
        payload,
        "valid_until_dependency_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["signal_validity_contract_evidence_json"],
        "growth_tilt_engine_signal_validity_contract_evidence",
        closure.SIGNAL_VALIDITY_CONTRACT_EVIDENCE_SCHEMA_VERSION,
        payload,
        "signal_validity_contract_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["stale_signal_policy_evidence_json"],
        "growth_tilt_engine_stale_signal_policy_evidence",
        closure.STALE_SIGNAL_POLICY_EVIDENCE_SCHEMA_VERSION,
        payload,
        "stale_signal_policy_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["growth_tilt_valid_until_alignment_evidence_json"],
        "growth_tilt_engine_valid_until_alignment_evidence",
        closure.GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_SCHEMA_VERSION,
        payload,
        "growth_tilt_valid_until_alignment_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["remaining_blocker_summary_json"],
        "growth_tilt_engine_remaining_blocker_summary_after_valid_until_closure",
        closure.REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        payload,
        "remaining_blocker_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["signal_validity_contract_evidence_markdown"]),
        _signal_contract_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["stale_signal_policy_evidence_markdown"]),
        _stale_policy_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["growth_tilt_valid_until_alignment_evidence_markdown"]),
        _alignment_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine valid-until dependency evidence closure",
            "",
            "## 摘要",
            "",
            f"- task_id：`{payload.get('task_id')}`",
            f"- status：`{payload.get('status')}`",
            f"- market regime：`{payload.get('market_regime')}`",
            (
                "- valid_until dependency blocker count："
                f"`{payload.get('valid_until_window_dependency_blocker_count_from_2415')}`"
            ),
            (
                "- valid_until evidence ready："
                f"`{payload.get('valid_until_dependency_evidence_ready')}`"
            ),
            (
                "- signal validity contract evidence ready："
                f"`{payload.get('signal_validity_contract_evidence_ready')}`"
            ),
            (
                "- stale signal policy evidence ready："
                f"`{payload.get('stale_signal_policy_evidence_ready')}`"
            ),
            (
                "- growth tilt / valid-until alignment evidence ready："
                f"`{payload.get('growth_tilt_valid_until_alignment_evidence_ready')}`"
            ),
            (
                "- source traceability still blocked："
                f"`{payload.get('source_traceability_still_blocked')}`"
            ),
            f"- PIT gate ready count：`{payload.get('pit_gate_ready_count')}`",
            f"- contract-ready count：`{payload.get('contract_ready_count')}`",
            "",
            "TRADING-2418 只把 `execution_signal_validity_policy` 的",
            "`valid_until_window` dependency blocker 转成可供 TRADING-2419 读取的",
            "pre-recheck evidence。它不标记 PIT gate ready、不标记 contract ready、",
            "不解除或降级任何 blocker。`growth_tilt_engine_signal_artifact` 的",
            "source traceability blocker 继续保留。",
            "",
            "## Source findings from TRADING-2417 / 2416 / 2415",
            "",
            "```json",
            _json_block(
                {
                    "source_feature_count": payload.get("source_feature_count"),
                    "pit_gate_ready_count": payload.get("pit_gate_ready_count"),
                    "contract_ready_count": payload.get("contract_ready_count"),
                    "pit_gate_blocked_count": payload.get("pit_gate_blocked_count"),
                    "blocked_by_source_traceability_count": payload.get(
                        "blocked_by_source_traceability_count"
                    ),
                    "blocked_by_valid_until_window_count": payload.get(
                        "blocked_by_valid_until_window_count"
                    ),
                    "source_traceability_still_blocked": payload.get(
                        "source_traceability_still_blocked"
                    ),
                }
            ),
            "```",
            "",
            "## Valid-until dependency evidence",
            "",
            "```json",
            _json_block(payload.get("valid_until_dependency_evidence", {})),
            "```",
            "",
            "## Signal validity contract evidence",
            "",
            "```json",
            _json_block(payload.get("signal_validity_contract_evidence", {})),
            "```",
            "",
            "## Stale signal policy evidence",
            "",
            "```json",
            _json_block(payload.get("stale_signal_policy_evidence", {})),
            "```",
            "",
            "## Growth tilt / valid-until alignment evidence",
            "",
            "```json",
            _json_block(payload.get("growth_tilt_valid_until_alignment_evidence", {})),
            "```",
            "",
            "## Remaining blocker summary",
            "",
            "```json",
            _json_block(payload.get("remaining_blocker_summary", {})),
            "```",
            "",
            "## PIT gate recheck policy",
            "",
            f"- pit_gate_recheck_required：`{payload.get('pit_gate_recheck_required')}`",
            f"- auto_mark_pit_gate_ready：`{payload.get('auto_mark_pit_gate_ready')}`",
            f"- auto_mark_contract_ready：`{payload.get('auto_mark_contract_ready')}`",
            f"- auto_downgrade_blocker：`{payload.get('auto_downgrade_blocker')}`",
            (
                "- owner_review_required_before_downgrade："
                f"`{payload.get('owner_review_required_before_downgrade')}`"
            ),
            "",
            "## Explicit non-approval list",
            "",
            "```json",
            _json_block(payload.get("explicit_non_approval_list", [])),
            "```",
            "",
            "## Data Quality Gate",
            "",
            f"- executed：`{payload.get('data_quality_gate_executed')}`",
            f"- reason：`{payload.get('data_quality_gate_reason')}`",
            "",
            "本任务仅读取 prior validated artifacts、registry、catalog 和 docs，不读取 fresh",
            "cached market data、不生成 feature/signal/scoring/daily report、不运行新 backtest。",
            "",
        ]
    )


def _signal_contract_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine signal validity contract evidence",
            "",
            "本文件记录 valid_from / valid_until / stale_after / expiry / lag 等字段的",
            "contract evidence。Evidence ready 不表示 standalone signal artifact ready。",
            "",
            "```json",
            _json_block(payload.get("signal_validity_contract_evidence", {})),
            "```",
            "",
        ]
    )


def _stale_policy_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine stale signal policy evidence",
            "",
            "本文件记录 no-stale carry-forward、expired signal block、near-expiry handling",
            "和 signal-to-execution lag policy evidence。",
            "",
            "```json",
            _json_block(payload.get("stale_signal_policy_evidence", {})),
            "```",
            "",
        ]
    )


def _alignment_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine valid-until alignment evidence",
            "",
            "本文件记录 growth tilt horizon / confidence / expiry 与 valid_until_window 的",
            "alignment evidence，并保留 `growth_tilt_engine_signal_artifact` blocker。",
            "",
            "```json",
            _json_block(payload.get("growth_tilt_valid_until_alignment_evidence", {})),
            "```",
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2419 route",
            "",
            f"- source task：`{payload.get('task_id')}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2419 应重新检查 growth tilt engine PIT gate readiness。2418 只补",
            "valid-until dependency evidence，不授权 candidate search、observation、",
            "paper-shadow、scheduler、event append、outcome binding、production 或 broker/order。",
            "",
        ]
    )


def _expected_gate_counts() -> dict[str, int]:
    return {
        "source_feature_count": 10,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 10,
        "blocked_by_source_traceability_count": 5,
        "blocked_by_valid_until_window_count": 1,
    }


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    mapping = _as_mapping(document)
    nested = mapping.get(key)
    return _as_mapping(nested) if isinstance(nested, Mapping) else mapping


def _registry_has_report_id(report_registry: Mapping[str, Any], report_id: str) -> bool:
    return any(
        _as_mapping(report).get("report_id") == report_id
        for report in _as_list(report_registry.get("reports"))
    )


def _pit_registry_severity(registry: Mapping[str, Any], input_id: str) -> str | None:
    for entry in _as_list(registry.get("entries")):
        mapping = _as_mapping(entry)
        if mapping.get("input_id") == input_id:
            severity = mapping.get("severity")
            return severity if isinstance(severity, str) else None
    return None


def _strategy_entry(registry: Mapping[str, Any], strategy_id: str) -> Mapping[str, Any]:
    for entry in _as_list(registry.get("strategy_execution_policies")):
        mapping = _as_mapping(entry)
        if mapping.get("strategy_id") == strategy_id:
            return mapping
    return {}


def _row_by_feature(
    rows: list[Mapping[str, Any]],
    feature_id: str,
) -> Mapping[str, Any]:
    for row in rows:
        observed = row.get("feature_id") or row.get("source_feature_id")
        if observed == feature_id:
            return row
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
