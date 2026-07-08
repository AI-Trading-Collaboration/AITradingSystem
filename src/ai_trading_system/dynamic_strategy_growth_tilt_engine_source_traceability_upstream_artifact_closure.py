from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as m2412,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
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
    growth_tilt_engine_source_traceability_upstream_artifact_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2417"
TASK_REGISTER_ID = (
    "TRADING-2417_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE"
)
REPORT_TYPE = "growth_tilt_engine_source_traceability_upstream_artifact_closure"
SCHEMA_VERSION = closure.CLOSURE_SCHEMA_VERSION
READY_STATUS = closure.READY_STATUS
BLOCKED_SOURCE_STATUS = (
    "GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_"
    "BLOCKED_SOURCE"
)
NEXT_ROUTE = closure.NEXT_ROUTE
PIT_RECHECK_ROUTE = closure.PIT_RECHECK_ROUTE
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2410",
    "TRADING-2412",
    "TRADING-2413",
    "TRADING-2415",
    "TRADING-2416",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_PRIOR_"
    "ARTIFACTS_AND_CONFIGS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "mark_any_source_feature_pit_gate_ready",
    "mark_any_source_feature_contract_ready",
    "downgrade_growth_tilt_engine_blocker",
    "downgrade_valid_until_window_blocker",
    "clear_any_blocking_gap",
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

DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "closure_plan_result.json"
)
DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "remaining_blocker_matrix.json"
)
DEFAULT_SOURCE_2416_SOURCE_TRACEABILITY_CLOSURE_PLAN_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "source_traceability_closure_plan.json"
)
DEFAULT_SOURCE_2416_AS_OF_EVIDENCE_CLOSURE_PLAN_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "as_of_evidence_closure_plan.json"
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
DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH = (
    m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT
    / "updated_source_feature_mapping.json"
)
DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "mapping_result.json"
)
DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "source_feature_contract_mapping.json"
)
DEFAULT_PIT_INPUT_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_strategy_pit_input_registry.yaml"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_source_traceability_upstream_artifact_closure(
    *,
    source_2416_closure_result_path: Path = DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Path = (
        DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH
    ),
    source_2416_source_traceability_closure_plan_path: Path = (
        DEFAULT_SOURCE_2416_SOURCE_TRACEABILITY_CLOSURE_PLAN_PATH
    ),
    source_2416_as_of_evidence_closure_plan_path: Path = (
        DEFAULT_SOURCE_2416_AS_OF_EVIDENCE_CLOSURE_PLAN_PATH
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
    source_2415_readiness_matrix_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH
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
    source_2412_updated_source_feature_mapping_path: Path = (
        DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH
    ),
    source_2410_mapping_result_path: Path = DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Path = (
        DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH
    ),
    pit_input_registry_path: Path = DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2416_closure_result_path=source_2416_closure_result_path,
        source_2416_remaining_blocker_matrix_path=(
            source_2416_remaining_blocker_matrix_path
        ),
        source_2416_source_traceability_closure_plan_path=(
            source_2416_source_traceability_closure_plan_path
        ),
        source_2416_as_of_evidence_closure_plan_path=(
            source_2416_as_of_evidence_closure_plan_path
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
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        pit_input_registry_path=pit_input_registry_path,
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
    source_2416_closure_result_path: Path,
    source_2416_remaining_blocker_matrix_path: Path,
    source_2416_source_traceability_closure_plan_path: Path,
    source_2416_as_of_evidence_closure_plan_path: Path,
    source_2416_valid_until_dependency_closure_plan_path: Path,
    source_2416_pit_gate_evidence_requirements_path: Path,
    source_2415_readiness_snapshot_result_path: Path,
    source_2415_readiness_matrix_path: Path,
    source_2413_source_traceability_remediation_result_path: Path,
    source_2413_updated_source_feature_mapping_path: Path,
    source_2413_remaining_blocker_summary_path: Path,
    source_2412_updated_source_feature_mapping_path: Path,
    source_2410_mapping_result_path: Path,
    source_2410_source_feature_contract_mapping_path: Path,
    pit_input_registry_path: Path,
    report_registry_path: Path,
    artifact_catalog_path: Path,
) -> dict[str, Any]:
    report_registry = safe_load_yaml_path(report_registry_path)
    pit_input_registry = safe_load_yaml_path(pit_input_registry_path)
    if isinstance(report_registry, dict):
        report_registry = {**report_registry, "path": str(report_registry_path)}
    if isinstance(pit_input_registry, dict):
        pit_input_registry = {**pit_input_registry, "path": str(pit_input_registry_path)}
    return {
        "closure_result_2416": _load_json_document(source_2416_closure_result_path),
        "remaining_blocker_matrix_2416": _load_json_document(
            source_2416_remaining_blocker_matrix_path
        ),
        "source_traceability_closure_plan_2416": _load_json_document(
            source_2416_source_traceability_closure_plan_path
        ),
        "as_of_evidence_closure_plan_2416": _load_json_document(
            source_2416_as_of_evidence_closure_plan_path
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
        "source_traceability_remediation_result_2413": _load_json_document(
            source_2413_source_traceability_remediation_result_path
        ),
        "updated_source_feature_mapping_2413": _load_json_document(
            source_2413_updated_source_feature_mapping_path
        ),
        "remaining_blocker_summary_2413": _load_json_document(
            source_2413_remaining_blocker_summary_path
        ),
        "updated_source_feature_mapping_2412": _load_json_document(
            source_2412_updated_source_feature_mapping_path
        ),
        "mapping_result_2410": _load_json_document(source_2410_mapping_result_path),
        "source_feature_contract_mapping_2410": _load_json_document(
            source_2410_source_feature_contract_mapping_path
        ),
        "pit_input_registry": pit_input_registry,
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "closure_result_2416": str(source_2416_closure_result_path),
            "remaining_blocker_matrix_2416": str(
                source_2416_remaining_blocker_matrix_path
            ),
            "source_traceability_closure_plan_2416": str(
                source_2416_source_traceability_closure_plan_path
            ),
            "as_of_evidence_closure_plan_2416": str(
                source_2416_as_of_evidence_closure_plan_path
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
            "source_traceability_remediation_result_2413": str(
                source_2413_source_traceability_remediation_result_path
            ),
            "updated_source_feature_mapping_2413": str(
                source_2413_updated_source_feature_mapping_path
            ),
            "remaining_blocker_summary_2413": str(
                source_2413_remaining_blocker_summary_path
            ),
            "updated_source_feature_mapping_2412": str(
                source_2412_updated_source_feature_mapping_path
            ),
            "mapping_result_2410": str(source_2410_mapping_result_path),
            "source_feature_contract_mapping_2410": str(
                source_2410_source_feature_contract_mapping_path
            ),
            "pit_input_registry": str(pit_input_registry_path),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "closure_result_2416",
        "remaining_blocker_matrix_2416",
        "source_traceability_closure_plan_2416",
        "as_of_evidence_closure_plan_2416",
        "valid_until_dependency_closure_plan_2416",
        "pit_gate_evidence_requirements_2416",
        "readiness_snapshot_result_2415",
        "readiness_matrix_2415",
        "source_traceability_remediation_result_2413",
        "updated_source_feature_mapping_2413",
        "remaining_blocker_summary_2413",
        "updated_source_feature_mapping_2412",
        "mapping_result_2410",
        "source_feature_contract_mapping_2410",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")
    artifact_catalog = _as_mapping(sources.get("artifact_catalog"))
    if artifact_catalog.get("_missing") is True:
        errors.append(
            f"missing source document: artifact_catalog -> {artifact_catalog.get('_path')}"
        )

    closure_result_2416 = _as_mapping(sources.get("closure_result_2416"))
    if closure_result_2416.get("status") != m2416.READY_STATUS:
        errors.append("2416 closure plan result must be ready")
    if closure_result_2416.get("recommended_next_research_task") != m2416.NEXT_ROUTE:
        errors.append("2416 closure plan must route to TRADING-2417")
    expected_counts = {
        "source_feature_count": 10,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 10,
        "blocked_by_source_traceability_count": 5,
        "blocked_by_valid_until_window_count": 1,
    }
    for field, expected in expected_counts.items():
        if closure_result_2416.get(field) != expected:
            errors.append(f"2416 closure result {field} must be {expected}")
    for field in m2416.SAFETY_FALSE_FIELDS:
        if closure_result_2416.get(field) is True:
            errors.append(f"2416 safety field must remain false: {field}")

    remaining_matrix = _section(
        _as_mapping(sources.get("remaining_blocker_matrix_2416")),
        "remaining_blocker_matrix",
    )
    matrix_rows = _as_list(remaining_matrix.get("matrix_rows"))
    if len(matrix_rows) != 10:
        errors.append("2416 remaining blocker matrix must contain 10 rows")
    if (
        sum(
            1
            for row in matrix_rows
            if _as_mapping(row).get("blocked_by_source_traceability") is True
        )
        != 5
    ):
        errors.append("2416 matrix must keep five source traceability blockers")

    source_plan = _section(
        _as_mapping(sources.get("source_traceability_closure_plan_2416")),
        "source_traceability_closure_plan",
    )
    if source_plan.get("source_traceability_gap_count") != 5:
        errors.append("2416 source traceability closure plan must keep five gaps")
    if len(_as_list(source_plan.get("closure_rows"))) != 5:
        errors.append("2416 source traceability closure plan must contain five rows")

    snapshot_2415 = _as_mapping(sources.get("readiness_snapshot_result_2415"))
    if snapshot_2415.get("status") != m2415.READY_STATUS:
        errors.append("2415 readiness snapshot must be ready")
    readiness_matrix = _section(
        _as_mapping(sources.get("readiness_matrix_2415")),
        "pit_gate_readiness_matrix",
    )
    if len(_as_list(readiness_matrix.get("matrix_rows"))) != 10:
        errors.append("2415 readiness matrix must contain 10 rows")

    remediation_2413 = _as_mapping(
        sources.get("source_traceability_remediation_result_2413")
    )
    if remediation_2413.get("status") != m2413.READY_STATUS:
        errors.append("2413 source traceability remediation must be ready")
    if remediation_2413.get("remaining_source_traceability_gap_count") != 5:
        errors.append("2413 remediation must keep five remaining traceability gaps")

    summary_2413 = _section(
        _as_mapping(sources.get("remaining_blocker_summary_2413")),
        "remaining_blocker_summary",
    )
    if summary_2413.get("remaining_source_traceability_gap_count") != 5:
        errors.append("2413 remaining summary must keep five traceability gaps")

    mapping_2412 = _as_mapping(sources.get("updated_source_feature_mapping_2412"))
    if mapping_2412.get("status") != m2412.READY_STATUS:
        errors.append("2412 updated mapping must be ready")
    mapping_result_2410 = _as_mapping(sources.get("mapping_result_2410"))
    if mapping_result_2410.get("status") != m2410.READY_STATUS:
        errors.append("2410 mapping result must be ready")
    if mapping_result_2410.get("known_source_feature_count") != 10:
        errors.append("2410 mapping result known_source_feature_count must be 10")
    if mapping_result_2410.get("contract_ready_count") != 0:
        errors.append("2410 mapping result contract_ready_count must be 0")
    contract_mapping_2410 = _section(
        _as_mapping(sources.get("source_feature_contract_mapping_2410")),
        "source_feature_contract_mapping",
    )
    if len(_as_list(contract_mapping_2410.get("mapping_rows"))) != 10:
        errors.append("2410 source feature contract mapping must contain 10 rows")

    pit_registry = _as_mapping(sources.get("pit_input_registry"))
    if _pit_registry_severity(pit_registry, "growth_tilt_engine") != "BLOCKING":
        errors.append("PIT input registry must keep growth_tilt_engine BLOCKING")
    if _pit_registry_severity(pit_registry, "valid_until_window") != "BLOCKING":
        errors.append("PIT input registry must keep valid_until_window BLOCKING")

    report_registry = _as_mapping(sources.get("report_registry"))
    for report_id in (
        "growth_tilt_engine_source_feature_contract_mapping",
        "growth_tilt_engine_as_of_semantics_remediation",
        "growth_tilt_engine_source_traceability_remediation",
        "growth_tilt_engine_pit_gate_readiness_snapshot",
        "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
        REPORT_TYPE,
    ):
        if not _registry_has_report_id(report_registry, report_id):
            errors.append(f"report registry missing required entry: {report_id}")

    catalog_text = str(artifact_catalog.get("text", ""))
    for command in (
        "growth-tilt-engine-source-feature-contract-mapping",
        "growth-tilt-engine-source-traceability-remediation",
        "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan",
        "growth-tilt-engine-source-traceability-upstream-artifact-closure",
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
        "production_effect": "none",
        "broker_action": "none",
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }
    for field in SAFETY_FALSE_FIELDS:
        payload[field] = False
    return payload


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    result = closure.build_growth_tilt_source_traceability_upstream_artifact_closure(
        _as_mapping(sources.get("closure_result_2416")),
        _as_mapping(sources.get("remaining_blocker_matrix_2416")),
        _as_mapping(sources.get("source_traceability_closure_plan_2416")),
        _as_mapping(sources.get("readiness_snapshot_result_2415")),
        _as_mapping(sources.get("readiness_matrix_2415")),
        _as_mapping(sources.get("source_traceability_remediation_result_2413")),
        _as_mapping(sources.get("updated_source_feature_mapping_2413")),
        _as_mapping(sources.get("updated_source_feature_mapping_2412")),
        _as_mapping(sources.get("source_feature_contract_mapping_2410")),
        pit_input_registry=_as_mapping(sources.get("pit_input_registry")),
    )
    return {
        **result,
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2417 only organizes pre-recheck source traceability and "
            "upstream artifact evidence. valid_until_window evidence remains "
            "unclosed and must be handled by TRADING-2418 before the TRADING-2419 "
            "PIT gate readiness recheck."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_feature_count": 0,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 0,
        "blocked_by_source_traceability_count": 0,
        "blocked_by_valid_until_window_count": 0,
        "source_traceability_evidence_row_count": 0,
        "source_traceability_pre_recheck_evidence_ready_count": 0,
        "source_traceability_still_blocked_count": 0,
        "upstream_artifact_closure_evidence_row_count": 0,
        "upstream_artifact_pre_recheck_evidence_ready_count": 0,
        "upstream_artifact_still_blocked_count": 0,
        "source_traceability_closure_evidence_ready": False,
        "upstream_artifact_closure_evidence_ready": False,
        "updated_source_feature_mapping_ready": False,
        "remaining_blocker_summary_ready": False,
        "source_traceability_closure_evidence": {},
        "upstream_artifact_closure_evidence": {},
        "updated_source_feature_mapping": {},
        "remaining_blocker_summary": {},
        "closure_validation": {},
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "closure_result.json"),
        "source_traceability_closure_evidence_json": str(
            output_root / "source_traceability_closure_evidence.json"
        ),
        "upstream_artifact_closure_evidence_json": str(
            output_root / "upstream_artifact_closure_evidence.json"
        ),
        "updated_source_feature_mapping_json": str(
            output_root / "updated_source_feature_mapping.json"
        ),
        "remaining_blocker_summary_json": str(
            output_root / "remaining_blocker_summary.json"
        ),
        "markdown_path": str(
            docs_root
            / "growth_tilt_engine_source_traceability_upstream_artifact_closure.md"
        ),
        "source_traceability_closure_evidence_markdown": str(
            docs_root / "growth_tilt_engine_source_traceability_closure_evidence.md"
        ),
        "upstream_artifact_closure_evidence_markdown": str(
            docs_root / "growth_tilt_engine_upstream_artifact_closure_evidence.md"
        ),
        "updated_source_feature_mapping_markdown": str(
            docs_root / "growth_tilt_engine_updated_source_feature_mapping.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2418_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_section_json_artifact(
        paths["source_traceability_closure_evidence_json"],
        "growth_tilt_engine_source_traceability_closure_evidence",
        closure.SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_SCHEMA_VERSION,
        payload,
        "source_traceability_closure_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["upstream_artifact_closure_evidence_json"],
        "growth_tilt_engine_upstream_artifact_closure_evidence",
        closure.UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_SCHEMA_VERSION,
        payload,
        "upstream_artifact_closure_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["updated_source_feature_mapping_json"],
        "growth_tilt_engine_updated_source_feature_mapping_after_traceability_closure",
        closure.UPDATED_SOURCE_FEATURE_MAPPING_SCHEMA_VERSION,
        payload,
        "updated_source_feature_mapping",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["remaining_blocker_summary_json"],
        "growth_tilt_engine_remaining_blocker_summary_after_traceability_closure",
        closure.REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        payload,
        "remaining_blocker_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["source_traceability_closure_evidence_markdown"]),
        _source_traceability_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["upstream_artifact_closure_evidence_markdown"]),
        _upstream_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["updated_source_feature_mapping_markdown"]),
        _mapping_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine source traceability and upstream artifact closure",
            "",
            "## 摘要",
            "",
            f"- task_id：`{payload.get('task_id')}`",
            f"- status：`{payload.get('status')}`",
            f"- market regime：`{payload.get('market_regime')}`",
            f"- source feature count：`{payload.get('source_feature_count')}`",
            "- source traceability blocker count："
            f"`{payload.get('blocked_by_source_traceability_count')}`",
            "- pre-recheck evidence ready count："
            f"`{payload.get('source_traceability_pre_recheck_evidence_ready_count')}`",
            "- still blocked count："
            f"`{payload.get('source_traceability_still_blocked_count')}`",
            "- valid_until_window blocker count："
            f"`{payload.get('blocked_by_valid_until_window_count')}`",
            f"- PIT gate ready count：`{payload.get('pit_gate_ready_count')}`",
            f"- contract-ready count：`{payload.get('contract_ready_count')}`",
            "",
            "TRADING-2417 只把 2416 暴露的 source traceability / upstream artifact",
            "blockers 整理成 later PIT readiness recheck 可读取的证据。它不标记任何",
            "source feature 为 PIT gate ready 或 contract ready，也不降级",
            "`growth_tilt_engine` / `valid_until_window` blocker。",
            "",
            "## 关键结论",
            "",
            "- `volatility_inputs`、`trend_features`、`drawdown_features`、"
            "`target_vol_policy` 已生成 pre-recheck evidence。",
            "- `growth_tilt_engine_signal_artifact` 仍缺少 standalone upstream "
            "signal artifact metadata。",
            "- `execution_signal_validity_policy` 的 `valid_until_window` blocker "
            "保留给 TRADING-2418。",
            "- TRADING-2419 之前不得恢复 candidate search、observation、paper-shadow、"
            "scheduler、event append、outcome binding、production 或 broker/order。",
            "",
            "## Data Quality Gate",
            "",
            f"- executed：`{payload.get('data_quality_gate_executed')}`",
            f"- reason：`{payload.get('data_quality_gate_reason')}`",
            "",
            "本任务仅读取 prior validated artifacts、registry、catalog 和 docs，不读取 fresh",
            "cached market data、不生成 feature/signal/scoring/daily report、不运行新 backtest。",
            "",
            "## Closure Result JSON",
            "",
            "```json",
            _json_block(
                {
                    "status": payload.get("status"),
                    "source_feature_count": payload.get("source_feature_count"),
                    "pit_gate_ready_count": payload.get("pit_gate_ready_count"),
                    "contract_ready_count": payload.get("contract_ready_count"),
                    "blocked_by_source_traceability_count": payload.get(
                        "blocked_by_source_traceability_count"
                    ),
                    "source_traceability_pre_recheck_evidence_ready_count": payload.get(
                        "source_traceability_pre_recheck_evidence_ready_count"
                    ),
                    "source_traceability_still_blocked_count": payload.get(
                        "source_traceability_still_blocked_count"
                    ),
                    "pit_gate_recheck_required": payload.get(
                        "pit_gate_recheck_required"
                    ),
                    "auto_mark_pit_gate_ready": payload.get(
                        "auto_mark_pit_gate_ready"
                    ),
                    "auto_mark_contract_ready": payload.get(
                        "auto_mark_contract_ready"
                    ),
                    "recommended_next_research_task": payload.get(
                        "recommended_next_research_task"
                    ),
                }
            ),
            "```",
            "",
        ]
    )


def _source_traceability_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine source traceability closure evidence",
            "",
            "## 摘要",
            "",
            "本文件记录 TRADING-2417 对五个 source traceability blockers 的 evidence",
            "整理结果。证据 ready 只表示可供后续 PIT readiness recheck 使用，不表示",
            "contract ready 或 PIT gate ready。",
            "",
            "```json",
            _json_block(payload.get("source_traceability_closure_evidence", {})),
            "```",
            "",
        ]
    )


def _upstream_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine upstream artifact closure evidence",
            "",
            "## 摘要",
            "",
            "本文件记录 source traceability closure 对应的 upstream artifact / registry",
            "evidence。`growth_tilt_engine_signal_artifact` 仍未闭合，因为缺少独立",
            "signal artifact metadata。",
            "",
            "```json",
            _json_block(payload.get("upstream_artifact_closure_evidence", {})),
            "```",
            "",
        ]
    )


def _mapping_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine updated source feature mapping",
            "",
            "## 摘要",
            "",
            "本文件是 TRADING-2417 后的 mapping view。所有 row 仍保持",
            "`contract_ready_after_2417=false`、`pit_gate_ready_after_2417=false`，",
            "并要求后续 TRADING-2418 / TRADING-2419 继续验证。",
            "",
            "```json",
            _json_block(payload.get("updated_source_feature_mapping", {})),
            "```",
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2418 route",
            "",
            f"- source task：`{payload.get('task_id')}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            f"- PIT recheck route：`{payload.get('pit_gate_recheck_route')}`",
            "",
            "TRADING-2418 应关闭 `valid_until_window` dependency evidence。只有在",
            "TRADING-2418 完成后，TRADING-2419 才能重新检查 growth tilt engine 的",
            "PIT gate readiness。TRADING-2417 不授权 candidate search、observation、",
            "paper-shadow、scheduler、event append、outcome binding、production 或 broker/order。",
            "",
        ]
    )


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    mapping = _as_mapping(document)
    nested = mapping.get(key)
    return _as_mapping(nested) if isinstance(nested, Mapping) else mapping


def _pit_registry_severity(registry: Mapping[str, Any], input_id: str) -> str | None:
    for entry in _as_list(registry.get("entries")):
        mapping = _as_mapping(entry)
        if mapping.get("input_id") == input_id:
            severity = mapping.get("severity")
            return severity if isinstance(severity, str) else None
    return None


def _registry_has_report_id(report_registry: Mapping[str, Any], report_id: str) -> bool:
    return any(
        _as_mapping(report).get("report_id") == report_id
        for report in _as_list(report_registry.get("reports"))
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
