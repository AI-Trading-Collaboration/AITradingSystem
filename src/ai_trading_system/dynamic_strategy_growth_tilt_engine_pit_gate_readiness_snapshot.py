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
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as m2414,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
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
    growth_tilt_engine_pit_gate_readiness_snapshot as pit_readiness,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2415"
TASK_REGISTER_ID = "TRADING-2415_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT"
REPORT_TYPE = "growth_tilt_engine_pit_gate_readiness_snapshot"
SCHEMA_VERSION = "growth_tilt_engine_pit_gate_readiness_snapshot.v1"
NEXT_ROUTE = pit_readiness.NEXT_ROUTE
READY_STATUS = pit_readiness.READY_STATUS
BLOCKED_SOURCE_STATUS = "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_BLOCKED_SOURCE"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2410",
    "TRADING-2411",
    "TRADING-2412",
    "TRADING-2413",
    "TRADING-2414",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PIT_GATE_READINESS_SNAPSHOT_PRIOR_ARTIFACTS_ONLY_"
    "NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "fix_source_features",
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

DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "mapping_result.json"
)
DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "source_feature_contract_mapping.json"
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
DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "source_traceability_remediation_result.json"
)
DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "updated_source_feature_mapping.json"
)
DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2414_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_RESULT_PATH = (
    m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    / "signal_validity_dependency_remediation_result.json"
)
DEFAULT_SOURCE_2414_UPDATED_SOURCE_FEATURE_MAPPING_PATH = (
    m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    / "updated_source_feature_mapping.json"
)
DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_pit_gate_readiness_snapshot(
    *,
    source_2410_mapping_result_path: Path = DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Path = (
        DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH
    ),
    source_2411_remediation_plan_result_path: Path = (
        DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH
    ),
    source_2411_ordered_remediation_items_path: Path = (
        DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH
    ),
    source_2411_unresolved_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH
    ),
    source_2412_as_of_remediation_result_path: Path = (
        DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH
    ),
    source_2412_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2412_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2413_source_traceability_remediation_result_path: Path = (
        DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH
    ),
    source_2413_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2413_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2414_signal_validity_dependency_remediation_result_path: Path = (
        DEFAULT_SOURCE_2414_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_RESULT_PATH
    ),
    source_2414_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2414_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2414_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        source_2411_remediation_plan_result_path=(
            source_2411_remediation_plan_result_path
        ),
        source_2411_ordered_remediation_items_path=(
            source_2411_ordered_remediation_items_path
        ),
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2414_signal_validity_dependency_remediation_result_path=(
            source_2414_signal_validity_dependency_remediation_result_path
        ),
        source_2414_updated_source_feature_mapping_path=(
            source_2414_updated_source_feature_mapping_path
        ),
        source_2414_remaining_blocker_summary_path=(
            source_2414_remaining_blocker_summary_path
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
    source_2410_mapping_result_path: Path,
    source_2410_source_feature_contract_mapping_path: Path,
    source_2411_remediation_plan_result_path: Path,
    source_2411_ordered_remediation_items_path: Path,
    source_2411_unresolved_blocker_summary_path: Path,
    source_2412_as_of_remediation_result_path: Path,
    source_2412_updated_source_feature_mapping_path: Path,
    source_2412_remaining_blocker_summary_path: Path,
    source_2413_source_traceability_remediation_result_path: Path,
    source_2413_updated_source_feature_mapping_path: Path,
    source_2413_remaining_blocker_summary_path: Path,
    source_2414_signal_validity_dependency_remediation_result_path: Path,
    source_2414_updated_source_feature_mapping_path: Path,
    source_2414_remaining_blocker_summary_path: Path,
    report_registry_path: Path,
    artifact_catalog_path: Path,
) -> dict[str, Any]:
    report_registry = safe_load_yaml_path(report_registry_path)
    if isinstance(report_registry, dict):
        report_registry = {**report_registry, "path": str(report_registry_path)}
    return {
        "mapping_result_2410": _load_json_document(source_2410_mapping_result_path),
        "source_feature_contract_mapping_2410": _load_json_document(
            source_2410_source_feature_contract_mapping_path
        ),
        "remediation_plan_result_2411": _load_json_document(
            source_2411_remediation_plan_result_path
        ),
        "ordered_remediation_items_2411": _load_json_document(
            source_2411_ordered_remediation_items_path
        ),
        "unresolved_blocker_summary_2411": _load_json_document(
            source_2411_unresolved_blocker_summary_path
        ),
        "as_of_remediation_result_2412": _load_json_document(
            source_2412_as_of_remediation_result_path
        ),
        "updated_source_feature_mapping_2412": _load_json_document(
            source_2412_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2412": _load_json_document(
            source_2412_remaining_blocker_summary_path
        ),
        "source_traceability_remediation_result_2413": _load_json_document(
            source_2413_source_traceability_remediation_result_path
        ),
        "updated_source_feature_mapping_2413": _load_json_document(
            source_2413_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2413": _load_json_document(
            source_2413_remaining_blocker_summary_path
        ),
        "signal_validity_dependency_remediation_result_2414": _load_json_document(
            source_2414_signal_validity_dependency_remediation_result_path
        ),
        "updated_source_feature_mapping_2414": _load_json_document(
            source_2414_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2414": _load_json_document(
            source_2414_remaining_blocker_summary_path
        ),
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "mapping_result_2410": str(source_2410_mapping_result_path),
            "source_feature_contract_mapping_2410": str(
                source_2410_source_feature_contract_mapping_path
            ),
            "remediation_plan_result_2411": str(
                source_2411_remediation_plan_result_path
            ),
            "ordered_remediation_items_2411": str(
                source_2411_ordered_remediation_items_path
            ),
            "unresolved_blocker_summary_2411": str(
                source_2411_unresolved_blocker_summary_path
            ),
            "as_of_remediation_result_2412": str(
                source_2412_as_of_remediation_result_path
            ),
            "updated_source_feature_mapping_2412": str(
                source_2412_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2412": str(
                source_2412_remaining_blocker_summary_path
            ),
            "source_traceability_remediation_result_2413": str(
                source_2413_source_traceability_remediation_result_path
            ),
            "updated_source_feature_mapping_2413": str(
                source_2413_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2413": str(
                source_2413_remaining_blocker_summary_path
            ),
            "signal_validity_dependency_remediation_result_2414": str(
                source_2414_signal_validity_dependency_remediation_result_path
            ),
            "updated_source_feature_mapping_2414": str(
                source_2414_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2414": str(
                source_2414_remaining_blocker_summary_path
            ),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "mapping_result_2410",
        "source_feature_contract_mapping_2410",
        "remediation_plan_result_2411",
        "ordered_remediation_items_2411",
        "unresolved_blocker_summary_2411",
        "as_of_remediation_result_2412",
        "updated_source_feature_mapping_2412",
        "remaining_blocker_summary_2412",
        "source_traceability_remediation_result_2413",
        "updated_source_feature_mapping_2413",
        "remaining_blocker_summary_2413",
        "signal_validity_dependency_remediation_result_2414",
        "updated_source_feature_mapping_2414",
        "remaining_blocker_summary_2414",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")
    artifact_catalog = _as_mapping(sources.get("artifact_catalog"))
    if artifact_catalog.get("_missing") is True:
        errors.append(
            f"missing source document: artifact_catalog -> {artifact_catalog.get('_path')}"
        )

    result_2410 = _as_mapping(sources.get("mapping_result_2410"))
    if result_2410.get("status") != m2410.READY_STATUS:
        errors.append("2410 mapping result must be ready with blockers unresolved")
    if result_2410.get("recommended_next_research_task") != m2410.NEXT_ROUTE:
        errors.append("2410 mapping result must route to TRADING-2411")
    if result_2410.get("known_source_feature_count") != 10:
        errors.append("2410 known_source_feature_count must be 10")
    if result_2410.get("contract_ready_count") != 0:
        errors.append("2410 contract_ready_count must remain 0")
    for field in m2410.SAFETY_FALSE_FIELDS:
        if result_2410.get(field) is True:
            errors.append(f"2410 safety field must remain false: {field}")

    mapping_2410 = _as_mapping(
        _as_mapping(sources.get("source_feature_contract_mapping_2410")).get(
            "source_feature_contract_mapping"
        )
    )
    if len(_as_list(mapping_2410.get("mapping_rows"))) != 10:
        errors.append("2410 source feature mapping must contain 10 rows")

    result_2411 = _as_mapping(sources.get("remediation_plan_result_2411"))
    if result_2411.get("status") != m2411.READY_STATUS:
        errors.append("2411 remediation plan result must be ready")
    if result_2411.get("recommended_next_research_task") != m2411.NEXT_ROUTE:
        errors.append("2411 remediation plan result must route to TRADING-2412")
    if result_2411.get("gap_count") != 7:
        errors.append("2411 gap_count must be 7")
    for field in m2411.SAFETY_FALSE_FIELDS:
        if result_2411.get(field) is True:
            errors.append(f"2411 safety field must remain false: {field}")

    result_2412 = _as_mapping(sources.get("as_of_remediation_result_2412"))
    if result_2412.get("status") != m2412.READY_STATUS:
        errors.append("2412 as-of remediation result must be ready")
    if result_2412.get("recommended_next_research_task") != m2412.NEXT_ROUTE:
        errors.append("2412 as-of remediation result must route to TRADING-2413")
    if result_2412.get("contract_ready_count") != 0:
        errors.append("2412 contract_ready_count must remain 0")
    for field in m2412.SAFETY_FALSE_FIELDS:
        if result_2412.get(field) is True:
            errors.append(f"2412 safety field must remain false: {field}")

    result_2413 = _as_mapping(sources.get("source_traceability_remediation_result_2413"))
    if result_2413.get("status") != m2413.READY_STATUS:
        errors.append("2413 source traceability remediation result must be ready")
    if result_2413.get("recommended_next_research_task") != m2413.NEXT_ROUTE:
        errors.append("2413 source traceability result must route to TRADING-2414")
    if result_2413.get("source_traceability_remediated_count") != 2:
        errors.append("2413 must have two remediated source traceability rows")
    if result_2413.get("remaining_source_traceability_gap_count") != 5:
        errors.append("2413 must keep five source traceability gaps")
    if result_2413.get("contract_ready_count") != 0:
        errors.append("2413 contract_ready_count must remain 0")
    for field in m2413.SAFETY_FALSE_FIELDS:
        if result_2413.get(field) is True:
            errors.append(f"2413 safety field must remain false: {field}")

    result_2414 = _as_mapping(
        sources.get("signal_validity_dependency_remediation_result_2414")
    )
    if result_2414.get("status") != m2414.READY_STATUS:
        errors.append("2414 signal validity dependency result must be ready")
    if result_2414.get("recommended_next_research_task") != m2414.NEXT_ROUTE:
        errors.append("2414 signal validity dependency result must route to TRADING-2415")
    if result_2414.get("validity_dependency_remediated_count") != 2:
        errors.append("2414 must have two remediated validity dependency rows")
    if result_2414.get("validity_dependency_blocked_by_valid_until_window_count") != 1:
        errors.append("2414 must keep one valid_until_window blocker")
    if result_2414.get("validity_dependency_blocked_by_source_traceability_count") != 5:
        errors.append("2414 must keep five source traceability blockers")
    if result_2414.get("contract_ready_count") != 0:
        errors.append("2414 contract_ready_count must remain 0")
    for field in m2414.SAFETY_FALSE_FIELDS:
        if result_2414.get(field) is True:
            errors.append(f"2414 safety field must remain false: {field}")

    mapping_2414 = _as_mapping(
        _as_mapping(sources.get("updated_source_feature_mapping_2414")).get(
            "updated_source_feature_mapping"
        )
    )
    if len(_as_list(mapping_2414.get("mapping_rows"))) != 10:
        errors.append("2414 updated source feature mapping must contain 10 rows")
    if mapping_2414.get("contract_ready_count") != 0:
        errors.append("2414 updated mapping contract_ready_count must remain 0")

    for key in (
        "unresolved_blocker_summary_2411",
        "remaining_blocker_summary_2412",
        "remaining_blocker_summary_2413",
        "remaining_blocker_summary_2414",
    ):
        section_name = (
            "unresolved_blocker_summary"
            if key == "unresolved_blocker_summary_2411"
            else "remaining_blocker_summary"
        )
        summary = _as_mapping(_as_mapping(sources.get(key)).get(section_name))
        if summary.get("growth_tilt_engine_blocker_resolved") is not False:
            errors.append(f"{key} must keep growth_tilt_engine unresolved")
        if summary.get("valid_until_window_blocker_resolved") is not False:
            errors.append(f"{key} must keep valid_until_window unresolved")

    report_registry = _as_mapping(sources.get("report_registry"))
    for report_id in (
        "growth_tilt_engine_source_feature_contract_mapping",
        "growth_tilt_engine_contract_gap_remediation_plan",
        "growth_tilt_engine_as_of_semantics_remediation",
        "growth_tilt_engine_source_traceability_remediation",
        "growth_tilt_engine_signal_validity_dependency_remediation",
    ):
        if not _registry_has_report_id(report_registry, report_id):
            errors.append(f"report registry missing required entry: {report_id}")

    catalog_text = str(artifact_catalog.get("text", ""))
    for command in (
        "growth-tilt-engine-source-feature-contract-mapping",
        "growth-tilt-engine-contract-gap-remediation-plan",
        "growth-tilt-engine-as-of-semantics-remediation",
        "growth-tilt-engine-source-traceability-remediation",
        "growth-tilt-engine-signal-validity-dependency-remediation",
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
    result_2414 = dict(
        _as_mapping(sources.get("signal_validity_dependency_remediation_result_2414"))
    )
    updated_2414 = _as_mapping(
        _as_mapping(sources.get("updated_source_feature_mapping_2414")).get(
            "updated_source_feature_mapping"
        )
    )
    if updated_2414:
        result_2414 = {**result_2414, "updated_source_feature_mapping": updated_2414}
    snapshot = pit_readiness.build_growth_tilt_pit_gate_readiness_snapshot(
        _as_mapping(sources.get("mapping_result_2410")),
        _as_mapping(sources.get("source_feature_contract_mapping_2410")),
        _as_mapping(sources.get("remediation_plan_result_2411")),
        _as_mapping(sources.get("as_of_remediation_result_2412")),
        _as_mapping(sources.get("source_traceability_remediation_result_2413")),
        result_2414,
    )
    validation = _as_mapping(snapshot.get("pit_gate_readiness_validation"))
    return {
        "source_feature_contract_mapping_ready": True,
        "contract_gap_remediation_plan_ready": True,
        "as_of_remediation_ready": True,
        "source_traceability_remediation_ready": True,
        "signal_validity_dependency_remediation_ready": True,
        "pit_gate_readiness_snapshot_completed": snapshot.get(
            "pit_gate_readiness_snapshot_completed"
        ),
        "pit_gate_readiness_matrix_ready": True,
        "remaining_blocker_summary_ready": True,
        "source_feature_count": snapshot.get("source_feature_count"),
        "as_of_ready_count": snapshot.get("as_of_ready_count"),
        "source_traceability_ready_count": snapshot.get(
            "source_traceability_ready_count"
        ),
        "validity_dependency_ready_count": snapshot.get(
            "validity_dependency_ready_count"
        ),
        "pit_gate_ready_count": snapshot.get("pit_gate_ready_count"),
        "contract_ready_count": snapshot.get("contract_ready_count"),
        "pit_gate_blocked_count": snapshot.get("pit_gate_blocked_count"),
        "blocked_by_source_traceability_count": snapshot.get(
            "blocked_by_source_traceability_count"
        ),
        "blocked_by_valid_until_window_count": snapshot.get(
            "blocked_by_valid_until_window_count"
        ),
        "contract_ready_not_increased": snapshot.get("contract_ready_not_increased"),
        "allowed_pit_gate_statuses": list(pit_readiness.ALLOWED_PIT_GATE_STATUSES),
        "pit_gate_readiness_snapshot": snapshot,
        "pit_gate_readiness_matrix": snapshot.get("pit_gate_readiness_matrix"),
        "pit_gate_readiness_validation": validation,
        "remaining_blocker_summary": snapshot.get("remaining_blocker_summary"),
        "pit_gate_status_unclassified_count": validation.get(
            "pit_gate_status_unclassified_count"
        ),
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2415 only aggregates PIT gate readiness from prior artifacts; "
            "remaining contract blockers require TRADING-2416 remediation planning "
            "while candidate search, observation, paper-shadow, production, and broker "
            "paths remain disabled."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_feature_contract_mapping_ready": False,
        "contract_gap_remediation_plan_ready": False,
        "as_of_remediation_ready": False,
        "source_traceability_remediation_ready": False,
        "signal_validity_dependency_remediation_ready": False,
        "pit_gate_readiness_snapshot_completed": False,
        "pit_gate_readiness_matrix_ready": False,
        "remaining_blocker_summary_ready": False,
        "source_feature_count": 0,
        "as_of_ready_count": 0,
        "source_traceability_ready_count": 0,
        "validity_dependency_ready_count": 0,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 0,
        "blocked_by_source_traceability_count": 0,
        "blocked_by_valid_until_window_count": 0,
        "contract_ready_not_increased": True,
        "allowed_pit_gate_statuses": list(pit_readiness.ALLOWED_PIT_GATE_STATUSES),
        "pit_gate_readiness_snapshot": {},
        "pit_gate_readiness_matrix": {},
        "pit_gate_readiness_validation": {},
        "remaining_blocker_summary": {},
        "pit_gate_status_unclassified_count": None,
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "pit_gate_readiness_snapshot_result.json"),
        "pit_gate_readiness_matrix_json": str(
            output_root / "pit_gate_readiness_matrix.json"
        ),
        "pit_gate_readiness_validation_json": str(
            output_root / "pit_gate_readiness_validation.json"
        ),
        "remaining_blocker_summary_json": str(output_root / "remaining_blocker_summary.json"),
        "markdown_path": str(
            docs_root / "growth_tilt_engine_pit_gate_readiness_snapshot.md"
        ),
        "pit_gate_readiness_matrix_markdown": str(
            docs_root / "growth_tilt_engine_pit_gate_readiness_matrix.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2416_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_section_json_artifact(
        paths["pit_gate_readiness_matrix_json"],
        "growth_tilt_engine_pit_gate_readiness_matrix",
        "growth_tilt_engine_pit_gate_readiness_matrix.v1",
        payload,
        "pit_gate_readiness_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["pit_gate_readiness_validation_json"],
        "growth_tilt_engine_pit_gate_readiness_validation",
        "growth_tilt_engine_pit_gate_readiness_validation.v1",
        payload,
        "pit_gate_readiness_validation",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["remaining_blocker_summary_json"],
        "growth_tilt_engine_pit_gate_remaining_blocker_summary",
        "growth_tilt_engine_pit_gate_remaining_blocker_summary.v1",
        payload,
        "remaining_blocker_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["pit_gate_readiness_matrix_markdown"]),
        _matrix_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine PIT gate readiness snapshot",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- source feature count：`{payload.get('source_feature_count')}`",
            f"- as-of ready count：`{payload.get('as_of_ready_count')}`",
            (
                "- source traceability ready count："
                f"`{payload.get('source_traceability_ready_count')}`"
            ),
            (
                "- validity dependency ready count："
                f"`{payload.get('validity_dependency_ready_count')}`"
            ),
            f"- PIT gate ready count：`{payload.get('pit_gate_ready_count')}`",
            f"- contract ready count：`{payload.get('contract_ready_count')}`",
            f"- PIT gate blocked count：`{payload.get('pit_gate_blocked_count')}`",
            (
                "- blocked by source traceability count："
                f"`{payload.get('blocked_by_source_traceability_count')}`"
            ),
            (
                "- blocked by valid_until_window count："
                f"`{payload.get('blocked_by_valid_until_window_count')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "2415 只从 TRADING-2410～2414 prior artifacts 聚合 PIT gate readiness。"
            "本任务没有修复 source feature、没有实现 valid_until_window、没有补造 PIT "
            "evidence，因此 `growth_tilt_engine` 与 `valid_until_window` blocker 仍保持 "
            "unresolved / undowngraded。",
            "",
            "## PIT Gate Readiness Matrix",
            "",
            "```json",
            _json_block(payload.get("pit_gate_readiness_matrix", {})),
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


def _matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine PIT gate readiness matrix",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- PIT gate readiness snapshot completed："
                f"`{payload.get('pit_gate_readiness_snapshot_completed')}`"
            ),
            (
                "- PIT gate status unclassified count："
                f"`{payload.get('pit_gate_status_unclassified_count')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("pit_gate_readiness_matrix", {})),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2416 route",
            "",
            f"- 当前任务：`{TASK_REGISTER_ID}`",
            f"- 当前状态：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2416 应制定 remaining contract blocker remediation plan。2415 未解除 "
            "`growth_tilt_engine` 或 `valid_until_window` blocker，candidate_search、"
            "observation、paper_shadow、production 和 broker 仍全部 disabled。",
        ]
    )


def _registry_has_report_id(report_registry: Mapping[str, Any], report_id: str) -> bool:
    for entry in _as_list(report_registry.get("reports")):
        row = _as_mapping(entry)
        if row.get("report_id") == report_id:
            return row.get("production_effect") == "none" and row.get("broker_action") == "none"
    return False


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
