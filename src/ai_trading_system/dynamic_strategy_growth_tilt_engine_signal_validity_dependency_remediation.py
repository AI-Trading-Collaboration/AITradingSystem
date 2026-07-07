from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as m2412,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
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
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY
from ai_trading_system.research_quality import (
    growth_tilt_engine_signal_validity_dependency_remediation as validity_dependency,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2414"
TASK_REGISTER_ID = (
    "TRADING-2414_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION"
)
REPORT_TYPE = "growth_tilt_engine_signal_validity_dependency_remediation"
SCHEMA_VERSION = "growth_tilt_engine_signal_validity_dependency_remediation.v1"
NEXT_ROUTE = validity_dependency.NEXT_ROUTE
TARGET_REMEDIATION_CATEGORY = validity_dependency.TARGET_REMEDIATION_CATEGORY
READY_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS"
)
BLOCKED_SOURCE_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_BLOCKED_SOURCE"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2413", "TRADING-2412", "TRADING-2411")
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_PRIOR_ARTIFACTS_ONLY_"
    "NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "implement_valid_until_window",
    "modify_growth_tilt_engine_scoring_logic",
    "generate_new_feature",
    "generate_dynamic_strategy_signal",
    "run_candidate_search",
    "approve_observation",
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
    "generate_daily_report",
    "run_new_strategy_backtest",
    "run_scoring",
    "resolve_growth_tilt_engine_blocker",
    "downgrade_growth_tilt_engine_blocker",
    "resolve_valid_until_window_blocker",
    "downgrade_valid_until_window_blocker",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
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

DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "source_traceability_remediation_result.json"
)
DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_CONTRACT_METADATA_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "source_traceability_contract_metadata.json"
)
DEFAULT_SOURCE_2413_BEFORE_AFTER_REMEDIATION_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "before_after_source_traceability_remediation.json"
)
DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "updated_source_feature_mapping.json"
)
DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2413_RESEARCH_DOC_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_DOCS_ROOT
    / "growth_tilt_engine_source_traceability_remediation.md"
)
DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH = (
    m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT
    / "as_of_remediation_result.json"
)
DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH = (
    m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT
    / "updated_source_feature_mapping.json"
)
DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2412_RESEARCH_DOC_PATH = (
    m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_DOCS_ROOT
    / "growth_tilt_engine_as_of_semantics_remediation.md"
)
DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "remediation_plan_result.json"
)
DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "ordered_remediation_items.json"
)
DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "unresolved_blocker_summary.json"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_signal_validity_dependency_remediation(
    *,
    source_2413_source_traceability_remediation_result_path: Path = (
        DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH
    ),
    source_2413_source_traceability_contract_metadata_path: Path = (
        DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_CONTRACT_METADATA_PATH
    ),
    source_2413_before_after_remediation_path: Path = (
        DEFAULT_SOURCE_2413_BEFORE_AFTER_REMEDIATION_PATH
    ),
    source_2413_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2413_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2413_research_doc_path: Path = DEFAULT_SOURCE_2413_RESEARCH_DOC_PATH,
    source_2412_as_of_remediation_result_path: Path = (
        DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH
    ),
    source_2412_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2412_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2412_research_doc_path: Path = DEFAULT_SOURCE_2412_RESEARCH_DOC_PATH,
    source_2411_remediation_plan_result_path: Path = (
        DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH
    ),
    source_2411_ordered_remediation_items_path: Path = (
        DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH
    ),
    source_2411_unresolved_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH
    ),
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_source_traceability_contract_metadata_path=(
            source_2413_source_traceability_contract_metadata_path
        ),
        source_2413_before_after_remediation_path=(
            source_2413_before_after_remediation_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2413_research_doc_path=source_2413_research_doc_path,
        source_2412_as_of_remediation_result_path=source_2412_as_of_remediation_result_path,
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2412_research_doc_path=source_2412_research_doc_path,
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=(
            source_2411_ordered_remediation_items_path
        ),
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
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
    source_2413_source_traceability_remediation_result_path: Path,
    source_2413_source_traceability_contract_metadata_path: Path,
    source_2413_before_after_remediation_path: Path,
    source_2413_updated_source_feature_mapping_path: Path,
    source_2413_remaining_blocker_summary_path: Path,
    source_2413_research_doc_path: Path,
    source_2412_as_of_remediation_result_path: Path,
    source_2412_updated_source_feature_mapping_path: Path,
    source_2412_remaining_blocker_summary_path: Path,
    source_2412_research_doc_path: Path,
    source_2411_remediation_plan_result_path: Path,
    source_2411_ordered_remediation_items_path: Path,
    source_2411_unresolved_blocker_summary_path: Path,
    report_registry_path: Path,
    artifact_catalog_path: Path,
) -> dict[str, Any]:
    report_registry = safe_load_yaml_path(report_registry_path)
    if isinstance(report_registry, dict):
        report_registry = {**report_registry, "path": str(report_registry_path)}
    return {
        "source_traceability_remediation_result_2413": _load_json_document(
            source_2413_source_traceability_remediation_result_path
        ),
        "source_traceability_contract_metadata_2413": _load_json_document(
            source_2413_source_traceability_contract_metadata_path
        ),
        "before_after_source_traceability_remediation_2413": _load_json_document(
            source_2413_before_after_remediation_path
        ),
        "updated_source_feature_mapping_2413": _load_json_document(
            source_2413_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2413": _load_json_document(
            source_2413_remaining_blocker_summary_path
        ),
        "research_doc_2413": _load_text_document(source_2413_research_doc_path),
        "as_of_remediation_result_2412": _load_json_document(
            source_2412_as_of_remediation_result_path
        ),
        "updated_source_feature_mapping_2412": _load_json_document(
            source_2412_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2412": _load_json_document(
            source_2412_remaining_blocker_summary_path
        ),
        "research_doc_2412": _load_text_document(source_2412_research_doc_path),
        "remediation_plan_result_2411": _load_json_document(
            source_2411_remediation_plan_result_path
        ),
        "ordered_remediation_items_2411": _load_json_document(
            source_2411_ordered_remediation_items_path
        ),
        "unresolved_blocker_summary_2411": _load_json_document(
            source_2411_unresolved_blocker_summary_path
        ),
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "source_traceability_remediation_result_2413": str(
                source_2413_source_traceability_remediation_result_path
            ),
            "source_traceability_contract_metadata_2413": str(
                source_2413_source_traceability_contract_metadata_path
            ),
            "before_after_source_traceability_remediation_2413": str(
                source_2413_before_after_remediation_path
            ),
            "updated_source_feature_mapping_2413": str(
                source_2413_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2413": str(
                source_2413_remaining_blocker_summary_path
            ),
            "research_doc_2413": str(source_2413_research_doc_path),
            "as_of_remediation_result_2412": str(source_2412_as_of_remediation_result_path),
            "updated_source_feature_mapping_2412": str(
                source_2412_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2412": str(
                source_2412_remaining_blocker_summary_path
            ),
            "research_doc_2412": str(source_2412_research_doc_path),
            "remediation_plan_result_2411": str(source_2411_remediation_plan_result_path),
            "ordered_remediation_items_2411": str(
                source_2411_ordered_remediation_items_path
            ),
            "unresolved_blocker_summary_2411": str(
                source_2411_unresolved_blocker_summary_path
            ),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "source_traceability_remediation_result_2413",
        "source_traceability_contract_metadata_2413",
        "before_after_source_traceability_remediation_2413",
        "updated_source_feature_mapping_2413",
        "remaining_blocker_summary_2413",
        "as_of_remediation_result_2412",
        "updated_source_feature_mapping_2412",
        "remaining_blocker_summary_2412",
        "remediation_plan_result_2411",
        "ordered_remediation_items_2411",
        "unresolved_blocker_summary_2411",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")
    for key in ("research_doc_2413", "research_doc_2412", "artifact_catalog"):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source document: {key} -> {source.get('_path')}")

    result_2413 = _as_mapping(sources.get("source_traceability_remediation_result_2413"))
    if result_2413.get("status") != m2413.READY_STATUS:
        errors.append("2413 source traceability result must be ready with remaining blockers")
    if result_2413.get("recommended_next_research_task") != m2413.NEXT_ROUTE:
        errors.append("2413 source traceability result must route to TRADING-2414")
    if result_2413.get("input_gap_count") != 7:
        errors.append("2413 input_gap_count must be 7 for TRADING-2414")
    if result_2413.get("source_traceability_gap_count") != 7:
        errors.append("2413 source_traceability_gap_count must be 7")
    if result_2413.get("source_traceability_remediated_count") != 2:
        errors.append("2413 must have two remediated source traceability items")
    if result_2413.get("remaining_source_traceability_gap_count") != 5:
        errors.append("2413 must keep five source traceability gaps")
    if result_2413.get("contract_ready_count") != 0:
        errors.append("2413 contract_ready_count must remain 0")
    for field in m2413.SAFETY_FALSE_FIELDS:
        if result_2413.get(field) is True:
            errors.append(f"2413 safety field must remain false: {field}")

    updated_mapping_2413 = _as_mapping(
        _as_mapping(sources.get("updated_source_feature_mapping_2413")).get(
            "updated_source_feature_mapping"
        )
    )
    if not _as_list(updated_mapping_2413.get("mapping_rows")):
        errors.append("2413 updated source feature mapping must contain rows")
    if updated_mapping_2413.get("contract_ready_count") != 0:
        errors.append("2413 updated mapping contract_ready_count must remain 0")

    metadata_2413 = _as_mapping(
        _as_mapping(sources.get("source_traceability_contract_metadata_2413")).get(
            "source_traceability_contract_metadata"
        )
    )
    if not _as_list(metadata_2413.get("metadata_rows")):
        errors.append("2413 source traceability contract metadata rows must be present")

    remaining_2413 = _as_mapping(
        _as_mapping(sources.get("remaining_blocker_summary_2413")).get(
            "remaining_blocker_summary"
        )
    )
    if remaining_2413.get("growth_tilt_engine_blocker_resolved") is not False:
        errors.append("2413 remaining summary must keep growth_tilt_engine unresolved")
    if remaining_2413.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2413 remaining summary must keep valid_until_window unresolved")

    result_2412 = _as_mapping(sources.get("as_of_remediation_result_2412"))
    if result_2412.get("status") != m2412.READY_STATUS:
        errors.append("2412 as-of remediation result must be ready with remaining blockers")
    if result_2412.get("recommended_next_research_task") != m2412.NEXT_ROUTE:
        errors.append("2412 as-of remediation result must route to TRADING-2413")
    if result_2412.get("contract_ready_count") != 0:
        errors.append("2412 contract_ready_count must remain 0")

    result_2411 = _as_mapping(sources.get("remediation_plan_result_2411"))
    if result_2411.get("status") != m2411.READY_STATUS:
        errors.append("2411 remediation plan result must be ready with blockers unresolved")
    if result_2411.get("gap_count") != result_2413.get("input_gap_count"):
        errors.append("2411 gap_count must match 2413 input_gap_count")
    ordered_items = _as_list(
        _as_mapping(sources.get("ordered_remediation_items_2411")).get(
            "ordered_remediation_items"
        )
    )
    validity_items = [
        item
        for item in ordered_items
        if _as_mapping(item).get("remediation_category") == TARGET_REMEDIATION_CATEGORY
        or _as_mapping(item).get("missing_validity_dependency") is True
    ]
    if not validity_items:
        errors.append("2411 must contain validity_dependency_required remediation items")

    remaining_2411 = _as_mapping(
        _as_mapping(sources.get("unresolved_blocker_summary_2411")).get(
            "unresolved_blocker_summary"
        )
    )
    if remaining_2411.get("growth_tilt_engine_blocker_resolved") is not False:
        errors.append("2411 summary must keep growth_tilt_engine unresolved")
    if remaining_2411.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2411 summary must keep valid_until_window unresolved")

    research_doc_2413 = str(_as_mapping(sources.get("research_doc_2413")).get("text", ""))
    if m2413.READY_STATUS not in research_doc_2413:
        errors.append("2413 research doc must reference ready-with-remaining-blockers status")
    research_doc_2412 = str(_as_mapping(sources.get("research_doc_2412")).get("text", ""))
    if m2412.READY_STATUS not in research_doc_2412:
        errors.append("2412 research doc must reference ready-with-remaining-blockers status")

    report_registry = _as_mapping(sources.get("report_registry"))
    for report_id in (
        "growth_tilt_engine_source_traceability_remediation",
        "growth_tilt_engine_as_of_semantics_remediation",
        "growth_tilt_engine_contract_gap_remediation_plan",
    ):
        if not _registry_has_report_id(report_registry, report_id):
            errors.append(f"report registry missing required entry: {report_id}")

    artifact_catalog = str(_as_mapping(sources.get("artifact_catalog")).get("text", ""))
    for command in (
        "growth-tilt-engine-source-traceability-remediation",
        "growth-tilt-engine-as-of-semantics-remediation",
        "growth-tilt-engine-contract-gap-remediation-plan",
    ):
        if command not in artifact_catalog:
            errors.append(f"artifact catalog missing command: {command}")
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
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_feature_generated": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "growth_tilt_engine_blocker_resolved": False,
        "growth_tilt_engine_blocker_downgraded": False,
        "valid_until_window_blocker_resolved": False,
        "valid_until_window_blocker_downgraded": False,
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "candidate_search_enabled": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "observation_enabled": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "production_enabled": False,
        "production_allowed": False,
        "broker_action": "none",
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    source_traceability_result = dict(
        _as_mapping(sources.get("source_traceability_remediation_result_2413"))
    )
    updated_mapping_doc = _as_mapping(sources.get("updated_source_feature_mapping_2413"))
    section_mapping = _as_mapping(updated_mapping_doc.get("updated_source_feature_mapping"))
    if section_mapping:
        source_traceability_result = {
            **source_traceability_result,
            "updated_source_feature_mapping": section_mapping,
        }
    remediation = validity_dependency.build_growth_tilt_signal_validity_dependency_remediation(
        _as_mapping(sources.get("remediation_plan_result_2411")),
        _as_mapping(sources.get("as_of_remediation_result_2412")),
        source_traceability_result,
    )
    validation = _as_mapping(
        remediation.get("signal_validity_dependency_remediation_validation")
    )
    return {
        "source_remediation_plan_ready": True,
        "as_of_remediation_ready": True,
        "source_traceability_remediation_ready": True,
        "signal_validity_dependency_remediation_completed": remediation.get(
            "signal_validity_dependency_remediation_completed"
        ),
        "signal_validity_dependency_contract_metadata_ready": True,
        "before_after_signal_validity_dependency_remediation_ready": True,
        "updated_source_feature_mapping_ready": True,
        "remaining_blocker_summary_ready": True,
        "input_gap_count": remediation.get("input_gap_count"),
        "validity_dependency_gap_count": remediation.get("validity_dependency_gap_count"),
        "validity_dependency_remediated_count": remediation.get(
            "validity_dependency_remediated_count"
        ),
        "validity_dependency_blocked_by_valid_until_window_count": remediation.get(
            "validity_dependency_blocked_by_valid_until_window_count"
        ),
        "validity_dependency_blocked_by_source_traceability_count": remediation.get(
            "validity_dependency_blocked_by_source_traceability_count"
        ),
        "remaining_validity_dependency_gap_count": remediation.get(
            "remaining_validity_dependency_gap_count"
        ),
        "remaining_blocked_or_gap_count": remediation.get("remaining_blocked_or_gap_count"),
        "contract_ready_count": remediation.get("contract_ready_count"),
        "allowed_signal_validity_dependency_remediation_statuses": list(
            validity_dependency.ALLOWED_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_STATUSES
        ),
        "signal_validity_dependency_remediation": remediation,
        "signal_validity_dependency_contract_metadata": remediation.get(
            "signal_validity_dependency_contract_metadata"
        ),
        "before_after_signal_validity_dependency_remediation": remediation.get(
            "before_after_signal_validity_dependency_remediation"
        ),
        "updated_source_feature_mapping": remediation.get("updated_source_feature_mapping"),
        "remaining_blocker_summary": remediation.get("remaining_blocker_summary"),
        "signal_validity_dependency_remediation_validation": validation,
        "validity_dependency_status_unclassified_count": validation.get(
            "validity_dependency_status_unclassified_count"
        ),
        "as_of_status_rollback_count": validation.get("as_of_status_rollback_count"),
        "source_traceability_status_rollback_count": validation.get(
            "source_traceability_status_rollback_count"
        ),
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2414 only remediates signal validity dependency metadata; "
            "TRADING-2415 should take a PIT gate readiness snapshot while candidate "
            "search, observation, paper-shadow, production, and broker paths remain "
            "disabled."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_remediation_plan_ready": False,
        "as_of_remediation_ready": False,
        "source_traceability_remediation_ready": False,
        "signal_validity_dependency_remediation_completed": False,
        "signal_validity_dependency_contract_metadata_ready": False,
        "before_after_signal_validity_dependency_remediation_ready": False,
        "updated_source_feature_mapping_ready": False,
        "remaining_blocker_summary_ready": False,
        "input_gap_count": None,
        "validity_dependency_gap_count": 0,
        "validity_dependency_remediated_count": 0,
        "validity_dependency_blocked_by_valid_until_window_count": 0,
        "validity_dependency_blocked_by_source_traceability_count": 0,
        "remaining_validity_dependency_gap_count": None,
        "remaining_blocked_or_gap_count": None,
        "contract_ready_count": 0,
        "allowed_signal_validity_dependency_remediation_statuses": list(
            validity_dependency.ALLOWED_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_STATUSES
        ),
        "signal_validity_dependency_remediation": {},
        "signal_validity_dependency_contract_metadata": {},
        "before_after_signal_validity_dependency_remediation": {},
        "updated_source_feature_mapping": {},
        "remaining_blocker_summary": {},
        "signal_validity_dependency_remediation_validation": {},
        "validity_dependency_status_unclassified_count": None,
        "as_of_status_rollback_count": None,
        "source_traceability_status_rollback_count": None,
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "signal_validity_dependency_remediation_result.json"),
        "signal_validity_dependency_contract_metadata_json": str(
            output_root / "signal_validity_dependency_contract_metadata.json"
        ),
        "before_after_signal_validity_dependency_remediation_json": str(
            output_root / "before_after_signal_validity_dependency_remediation.json"
        ),
        "updated_source_feature_mapping_json": str(
            output_root / "updated_source_feature_mapping.json"
        ),
        "remaining_blocker_summary_json": str(output_root / "remaining_blocker_summary.json"),
        "markdown_path": str(
            docs_root / "growth_tilt_engine_signal_validity_dependency_remediation.md"
        ),
        "signal_validity_dependency_contract_markdown": str(
            docs_root / "growth_tilt_engine_signal_validity_dependency_contract_metadata.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2415_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    _write_section_json(
        paths["signal_validity_dependency_contract_metadata_json"],
        "growth_tilt_engine_signal_validity_dependency_contract_metadata",
        "growth_tilt_engine_signal_validity_dependency_contract_metadata.v1",
        payload,
        "signal_validity_dependency_contract_metadata",
    )
    _write_section_json(
        paths["before_after_signal_validity_dependency_remediation_json"],
        "growth_tilt_engine_signal_validity_dependency_before_after_remediation",
        "growth_tilt_engine_signal_validity_dependency_before_after_remediation.v1",
        payload,
        "before_after_signal_validity_dependency_remediation",
    )
    _write_section_json(
        paths["updated_source_feature_mapping_json"],
        "growth_tilt_engine_source_feature_mapping_after_signal_validity_dependency",
        "growth_tilt_engine_source_feature_mapping_after_signal_validity_dependency.v1",
        payload,
        "updated_source_feature_mapping",
    )
    _write_section_json(
        paths["remaining_blocker_summary_json"],
        "growth_tilt_engine_signal_validity_dependency_remaining_blocker_summary",
        "growth_tilt_engine_signal_validity_dependency_remaining_blocker_summary.v1",
        payload,
        "remaining_blocker_summary",
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["signal_validity_dependency_contract_markdown"]),
        _signal_validity_dependency_contract_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _write_section_json(
    path: str,
    report_type: str,
    schema_version: str,
    payload: Mapping[str, Any],
    payload_key: str,
) -> None:
    write_json_artifact(
        Path(path),
        {
            "task_id": TASK_ID,
            "status": payload.get("status"),
            "report_type": report_type,
            "schema_version": schema_version,
            payload_key: payload.get(payload_key, {}),
            "production_effect": "none",
            "broker_action": "none",
        },
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine signal validity dependency remediation",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- input gap count：`{payload.get('input_gap_count')}`",
            (
                "- validity dependency gap count："
                f"`{payload.get('validity_dependency_gap_count')}`"
            ),
            (
                "- validity dependency remediated count："
                f"`{payload.get('validity_dependency_remediated_count')}`"
            ),
            (
                "- blocked by valid_until_window count："
                f"`{payload.get('validity_dependency_blocked_by_valid_until_window_count')}`"
            ),
            (
                "- blocked by source traceability count："
                f"`{payload.get('validity_dependency_blocked_by_source_traceability_count')}`"
            ),
            (
                "- remaining blocked or gap count："
                f"`{payload.get('remaining_blocked_or_gap_count')}`"
            ),
            f"- contract ready count：`{payload.get('contract_ready_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "2414 只补 signal validity dependency metadata。valid_until_window、PIT gate "
            "和 remaining source traceability gaps 仍未在本任务中修复，因此 "
            "`growth_tilt_engine` blocker 不能解除或降级。",
            "",
            "## Before / After",
            "",
            "```json",
            _json_block(
                payload.get("before_after_signal_validity_dependency_remediation", {})
            ),
            "```",
            "",
            "## Signal Validity Dependency Contract Metadata",
            "",
            "```json",
            _json_block(payload.get("signal_validity_dependency_contract_metadata", {})),
            "```",
            "",
            "## Remaining Blockers",
            "",
            "```json",
            _json_block(payload.get("remaining_blocker_summary", {})),
            "```",
            "",
            "## Data Quality Boundary",
            "",
            f"- data_quality_gate_executed：`{payload.get('data_quality_gate_executed')}`",
            f"- data_quality_gate_reason：`{payload.get('data_quality_gate_reason')}`",
            "",
            "## Safety Boundary",
            "",
            (
                "- growth_tilt_engine_blocker_resolved："
                f"`{payload.get('growth_tilt_engine_blocker_resolved')}`"
            ),
            (
                "- growth_tilt_engine_blocker_downgraded："
                f"`{payload.get('growth_tilt_engine_blocker_downgraded')}`"
            ),
            (
                "- valid_until_window_blocker_resolved："
                f"`{payload.get('valid_until_window_blocker_resolved')}`"
            ),
            (
                "- valid_until_window_blocker_downgraded："
                f"`{payload.get('valid_until_window_blocker_downgraded')}`"
            ),
            f"- candidate_search_enabled：`{payload.get('candidate_search_enabled')}`",
            f"- observation_enabled：`{payload.get('observation_enabled')}`",
            f"- paper_shadow_enabled：`{payload.get('paper_shadow_enabled')}`",
            f"- production_enabled：`{payload.get('production_enabled')}`",
            f"- broker_enabled：`{payload.get('broker_enabled')}`",
        ]
    )


def _signal_validity_dependency_contract_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine signal validity dependency contract metadata",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- signal validity dependency remediation completed："
                f"`{payload.get('signal_validity_dependency_remediation_completed')}`"
            ),
            (
                "- validity dependency status unclassified count："
                f"`{payload.get('validity_dependency_status_unclassified_count')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("signal_validity_dependency_contract_metadata", {})),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2415 route",
            "",
            f"- 当前任务：`{TASK_REGISTER_ID}`",
            f"- 当前状态：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2415 应生成 growth tilt engine PIT gate readiness snapshot。2415 仍必须保持 "
            "candidate_search=false、observation=false、paper_shadow=false、production=false、"
            "broker=false，且不得解除或降级 blocker，除非后续 owner review 明确批准。",
        ]
    )


def _registry_has_report_id(report_registry: Mapping[str, Any], report_id: str) -> bool:
    for entry in _as_list(report_registry.get("reports")):
        row = _as_mapping(entry)
        if row.get("report_id") == report_id:
            return row.get("production_effect") == "none" and row.get("broker_action") == "none"
    return False


def _load_text_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_path": str(path), "text": ""}
    return {"_path": str(path), "text": path.read_text(encoding="utf-8")}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
