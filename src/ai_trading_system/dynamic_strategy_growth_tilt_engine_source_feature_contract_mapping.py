from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan as m2406
import ai_trading_system.dynamic_strategy_signal_as_of_validity_contract_schema as m2409
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
from ai_trading_system.research_quality.growth_tilt_engine_contract_mapping import (
    ALLOWED_MAPPING_STATUSES,
    build_growth_tilt_source_feature_contract_mapping,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2410"
TASK_REGISTER_ID = "TRADING-2410_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING"
REPORT_TYPE = "growth_tilt_engine_source_feature_contract_mapping"
SCHEMA_VERSION = "growth_tilt_engine_source_feature_contract_mapping.v1"
READY_STATUS = "GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY_WITH_BLOCKERS_UNRESOLVED"
BLOCKED_SOURCE_STATUS = "GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_BLOCKED_SOURCE"
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2405", "TRADING-2406", "TRADING-2409")
BLOCKER_UNDER_REVIEW = "growth_tilt_engine"
NEXT_ROUTE = "TRADING-2411_Growth_Tilt_Engine_Contract_Gap_Remediation_Plan"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_SOURCE_FEATURE_CONTRACT_MAPPING_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "resolve_growth_tilt_engine_blocker",
    "downgrade_growth_tilt_engine_severity",
    "mark_growth_tilt_engine_true_pit",
    "generate_dynamic_strategy_signal",
    "run_candidate_search",
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
    "generate_daily_report",
    "run_new_strategy_backtest",
    "run_scoring",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "blockers_resolved",
    "blockers_downgraded",
    "growth_tilt_engine_blocking_gap_resolved",
    "growth_tilt_engine_severity_downgraded",
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
    "new_signal_generated",
    "new_strategy_backtest_run",
    "scoring_run",
)

DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_RESULT_PATH = (
    m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT
    / "contract_schema_result.json"
)
DEFAULT_SOURCE_2409_SOURCE_FEATURE_CONTRACT_SCHEMA_PATH = (
    m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT
    / "source_feature_traceability_contract_schema.json"
)
DEFAULT_SOURCE_2409_SIGNAL_AS_OF_CONTRACT_SCHEMA_PATH = (
    m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT
    / "signal_as_of_contract_schema.json"
)
DEFAULT_SOURCE_2409_SIGNAL_VALIDITY_CONTRACT_SCHEMA_PATH = (
    m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT
    / "signal_validity_contract_schema.json"
)
DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_SNAPSHOT_PATH = (
    m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT
    / "contract_schema_snapshot.json"
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
DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    / "remediation_plan_result.json"
)
DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH = m2406.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH
DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH = m2406.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH
DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH = (
    m2406.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH
)
DEFAULT_EQUAL_RISK_GROWTH_TILT_CANDIDATE_REGISTRY_PATH = (
    m2406.DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
)


def run_growth_tilt_engine_source_feature_contract_mapping(
    *,
    source_2409_contract_schema_result_path: Path = (
        DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_RESULT_PATH
    ),
    source_2409_source_feature_contract_schema_path: Path = (
        DEFAULT_SOURCE_2409_SOURCE_FEATURE_CONTRACT_SCHEMA_PATH
    ),
    source_2409_signal_as_of_contract_schema_path: Path = (
        DEFAULT_SOURCE_2409_SIGNAL_AS_OF_CONTRACT_SCHEMA_PATH
    ),
    source_2409_signal_validity_contract_schema_path: Path = (
        DEFAULT_SOURCE_2409_SIGNAL_VALIDITY_CONTRACT_SCHEMA_PATH
    ),
    source_2409_contract_schema_snapshot_path: Path = (
        DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_SNAPSHOT_PATH
    ),
    source_2406_source_feature_inventory_path: Path = (
        DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH
    ),
    source_2406_pit_risk_audit_path: Path = DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Path = (
        DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH
    ),
    source_2406_remediation_plan_path: Path = DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2405_pit_gate_result_path: Path = DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Path = DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    growth_tilt_candidate_registry_path: Path = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CANDIDATE_REGISTRY_PATH
    ),
    output_root: Path = DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2409_contract_schema_result_path=source_2409_contract_schema_result_path,
        source_2409_source_feature_contract_schema_path=(
            source_2409_source_feature_contract_schema_path
        ),
        source_2409_signal_as_of_contract_schema_path=(
            source_2409_signal_as_of_contract_schema_path
        ),
        source_2409_signal_validity_contract_schema_path=(
            source_2409_signal_validity_contract_schema_path
        ),
        source_2409_contract_schema_snapshot_path=(
            source_2409_contract_schema_snapshot_path
        ),
        source_2406_source_feature_inventory_path=source_2406_source_feature_inventory_path,
        source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
        source_2406_signal_construction_gap_analysis_path=(
            source_2406_signal_construction_gap_analysis_path
        ),
        source_2406_remediation_plan_path=source_2406_remediation_plan_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        pit_input_registry_path=pit_input_registry_path,
        growth_tilt_candidate_registry_path=growth_tilt_candidate_registry_path,
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
    source_2409_contract_schema_result_path: Path,
    source_2409_source_feature_contract_schema_path: Path,
    source_2409_signal_as_of_contract_schema_path: Path,
    source_2409_signal_validity_contract_schema_path: Path,
    source_2409_contract_schema_snapshot_path: Path,
    source_2406_source_feature_inventory_path: Path,
    source_2406_pit_risk_audit_path: Path,
    source_2406_signal_construction_gap_analysis_path: Path,
    source_2406_remediation_plan_path: Path,
    source_2405_pit_gate_result_path: Path,
    source_2405_blocker_summary_path: Path,
    pit_input_registry_path: Path,
    growth_tilt_candidate_registry_path: Path,
) -> dict[str, Any]:
    pit_registry = safe_load_yaml_path(pit_input_registry_path)
    if isinstance(pit_registry, dict):
        pit_registry = {**pit_registry, "path": str(pit_input_registry_path)}
    growth_registry = safe_load_yaml_path(growth_tilt_candidate_registry_path)
    if isinstance(growth_registry, dict):
        growth_registry = {**growth_registry, "path": str(growth_tilt_candidate_registry_path)}
    return {
        "contract_schema_result_2409": _load_json_document(
            source_2409_contract_schema_result_path
        ),
        "source_feature_contract_schema_2409": _load_json_document(
            source_2409_source_feature_contract_schema_path
        ),
        "signal_as_of_contract_schema_2409": _load_json_document(
            source_2409_signal_as_of_contract_schema_path
        ),
        "signal_validity_contract_schema_2409": _load_json_document(
            source_2409_signal_validity_contract_schema_path
        ),
        "contract_schema_snapshot_2409": _load_json_document(
            source_2409_contract_schema_snapshot_path
        ),
        "source_feature_inventory_2406": _load_json_document(
            source_2406_source_feature_inventory_path
        ),
        "pit_risk_audit_2406": _load_json_document(source_2406_pit_risk_audit_path),
        "signal_construction_gap_analysis_2406": _load_json_document(
            source_2406_signal_construction_gap_analysis_path
        ),
        "remediation_plan_2406": _load_json_document(source_2406_remediation_plan_path),
        "pit_gate_result_2405": _load_json_document(source_2405_pit_gate_result_path),
        "blocker_summary_2405": _load_json_document(source_2405_blocker_summary_path),
        "pit_input_registry_config": pit_registry,
        "growth_tilt_candidate_registry_config": growth_registry,
        "source_paths": {
            "contract_schema_result_2409": str(source_2409_contract_schema_result_path),
            "source_feature_contract_schema_2409": str(
                source_2409_source_feature_contract_schema_path
            ),
            "signal_as_of_contract_schema_2409": str(
                source_2409_signal_as_of_contract_schema_path
            ),
            "signal_validity_contract_schema_2409": str(
                source_2409_signal_validity_contract_schema_path
            ),
            "contract_schema_snapshot_2409": str(source_2409_contract_schema_snapshot_path),
            "source_feature_inventory_2406": str(source_2406_source_feature_inventory_path),
            "pit_risk_audit_2406": str(source_2406_pit_risk_audit_path),
            "signal_construction_gap_analysis_2406": str(
                source_2406_signal_construction_gap_analysis_path
            ),
            "remediation_plan_2406": str(source_2406_remediation_plan_path),
            "pit_gate_result_2405": str(source_2405_pit_gate_result_path),
            "blocker_summary_2405": str(source_2405_blocker_summary_path),
            "pit_input_registry_config": str(pit_input_registry_path),
            "growth_tilt_candidate_registry_config": str(
                growth_tilt_candidate_registry_path
            ),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "contract_schema_result_2409",
        "source_feature_contract_schema_2409",
        "signal_as_of_contract_schema_2409",
        "signal_validity_contract_schema_2409",
        "contract_schema_snapshot_2409",
        "source_feature_inventory_2406",
        "pit_risk_audit_2406",
        "signal_construction_gap_analysis_2406",
        "remediation_plan_2406",
        "pit_gate_result_2405",
        "blocker_summary_2405",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")

    contract_result = _as_mapping(sources.get("contract_schema_result_2409"))
    if contract_result.get("status") != m2409.READY_STATUS:
        errors.append("2409 contract schema result must be ready")
    if contract_result.get("route_to_next_task") != m2409.NEXT_ROUTE:
        errors.append("2409 route must point to TRADING-2410")
    for field in m2409.SAFETY_FALSE_FIELDS:
        if contract_result.get(field) is True:
            errors.append(f"2409 safety field must remain false: {field}")

    schema_doc = _as_mapping(sources.get("source_feature_contract_schema_2409"))
    schema_payload = _as_mapping(schema_doc.get("source_feature_traceability_contract_schema"))
    if not schema_payload:
        schema_payload = _as_mapping(
            contract_result.get("source_feature_traceability_contract_schema")
        )
    if schema_payload.get("schema_name") != "source_feature_traceability_contract":
        errors.append("2409 source feature traceability contract schema missing")

    inventory_doc = _as_mapping(sources.get("source_feature_inventory_2406"))
    if inventory_doc.get("status") != m2406.READY_STATUS:
        errors.append("2406 source feature inventory must be ready")
    inventory_rows = _as_list(inventory_doc.get("source_feature_inventory"))
    if not inventory_rows:
        errors.append("2406 source feature inventory must contain rows")
    has_non_growth_tilt_row = any(
        _as_mapping(row).get("used_by_growth_tilt_engine") is not True
        for row in inventory_rows
    )
    if has_non_growth_tilt_row:
        errors.append("all 2406 inventory rows must be growth_tilt_engine source features")

    remediation_2406 = _as_mapping(sources.get("remediation_plan_2406"))
    if remediation_2406.get("status") != m2406.READY_STATUS:
        errors.append("2406 remediation plan must be ready")
    for field in m2406.SAFETY_FALSE_FIELDS:
        if remediation_2406.get(field) is True:
            errors.append(f"2406 safety field must remain false: {field}")

    gate_2405 = _as_mapping(_as_mapping(sources.get("pit_gate_result_2405")).get("pit_gate_result"))
    if gate_2405.get("candidate_search_allowed") is not False:
        errors.append("2405 PIT gate must keep candidate search blocked")
    if "BLOCKING_GAP_GROWTH_TILT_ENGINE" not in set(_as_list(gate_2405.get("blockers"))):
        errors.append("2405 PIT gate missing growth_tilt_engine blocker")

    blocker_summary = _as_mapping(
        _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
    )
    if BLOCKER_UNDER_REVIEW not in _as_list(blocker_summary.get("blocking_gaps")):
        errors.append("2405 blocker summary missing growth_tilt_engine")

    pit_registry = _as_mapping(sources.get("pit_input_registry_config"))
    growth_entry = _registry_entry(pit_registry, BLOCKER_UNDER_REVIEW)
    if not growth_entry:
        errors.append("PIT input registry missing growth_tilt_engine")
    elif growth_entry.get("severity") != "BLOCKING":
        errors.append("PIT input registry growth_tilt_engine must remain BLOCKING")

    growth_registry = _as_mapping(sources.get("growth_tilt_candidate_registry_config"))
    required_price_tickers = _as_list(
        _as_mapping(growth_registry.get("research_policy")).get("required_price_tickers")
    )
    if not {"QQQ", "TQQQ", "SGOV"}.issubset(set(str(ticker) for ticker in required_price_tickers)):
        errors.append("growth tilt candidate registry missing required price tickers")
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
        "blocker_under_review": BLOCKER_UNDER_REVIEW,
        "source_paths": dict(_as_mapping(sources.get("source_paths"))),
        "source_validation_errors": source_validation_errors,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
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
    inventory_rows = [
        _as_mapping(row)
        for row in _as_list(
            _as_mapping(sources.get("source_feature_inventory_2406")).get(
                "source_feature_inventory"
            )
        )
    ]
    schema_payload = _as_mapping(
        _as_mapping(sources.get("contract_schema_result_2409")).get(
            "source_feature_traceability_contract_schema"
        )
    )
    mapping = build_growth_tilt_source_feature_contract_mapping(
        inventory_rows,
        source_feature_contract_schema=schema_payload,
    )
    validation = _as_mapping(mapping.get("contract_mapping_validation"))
    gap_summary = _as_mapping(mapping.get("unresolved_gap_summary"))
    return {
        "source_feature_inventory_ready": True,
        "source_feature_contract_mapping_ready": True,
        "contract_mapping_validation_ready": True,
        "unresolved_gap_summary_ready": True,
        "known_source_feature_count": mapping.get("known_source_feature_count"),
        "mapping_statuses_allowed": list(ALLOWED_MAPPING_STATUSES),
        "source_feature_contract_mapping": mapping,
        "contract_mapping_validation": validation,
        "unresolved_gap_summary": gap_summary,
        "unclassified_feature_count": validation.get("unclassified_feature_count"),
        "contract_ready_count": validation.get("contract_ready_count"),
        "blocked_or_gap_count": validation.get("blocked_or_gap_count"),
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "source feature mapping exposes unresolved as-of, traceability, and "
            "validity-dependency gaps; TRADING-2411 should plan remediation without "
            "downgrading the blocker."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_feature_inventory_ready": False,
        "source_feature_contract_mapping_ready": False,
        "contract_mapping_validation_ready": False,
        "unresolved_gap_summary_ready": False,
        "known_source_feature_count": 0,
        "mapping_statuses_allowed": list(ALLOWED_MAPPING_STATUSES),
        "source_feature_contract_mapping": {},
        "contract_mapping_validation": {},
        "unresolved_gap_summary": {},
        "unclassified_feature_count": None,
        "contract_ready_count": None,
        "blocked_or_gap_count": None,
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "mapping_result.json"),
        "source_feature_contract_mapping_json": str(
            output_root / "source_feature_contract_mapping.json"
        ),
        "contract_mapping_validation_json": str(
            output_root / "contract_mapping_validation.json"
        ),
        "unresolved_gap_summary_json": str(output_root / "unresolved_gap_summary.json"),
        "markdown_path": str(
            docs_root / "growth_tilt_engine_source_feature_contract_mapping.md"
        ),
        "validation_markdown": str(
            docs_root / "growth_tilt_engine_contract_mapping_validation.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2411_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    _write_section_json(
        paths["source_feature_contract_mapping_json"],
        "growth_tilt_engine_source_feature_contract_mapping",
        "growth_tilt_engine_source_feature_contract_mapping.v1",
        payload,
        "source_feature_contract_mapping",
    )
    _write_section_json(
        paths["contract_mapping_validation_json"],
        "growth_tilt_engine_contract_mapping_validation",
        "growth_tilt_engine_source_feature_contract_mapping_validation.v1",
        payload,
        "contract_mapping_validation",
    )
    _write_section_json(
        paths["unresolved_gap_summary_json"],
        "growth_tilt_engine_contract_gap_summary",
        "growth_tilt_engine_contract_gap_summary.v1",
        payload,
        "unresolved_gap_summary",
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["validation_markdown"]), _validation_markdown(payload)
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
            "# Growth tilt engine source feature contract mapping",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- blocker under review：`{payload.get('blocker_under_review')}`",
            f"- 已知 source feature 数：`{payload.get('known_source_feature_count')}`",
            f"- 未分类 feature 数：`{payload.get('unclassified_feature_count')}`",
            f"- contract-ready feature 数：`{payload.get('contract_ready_count')}`",
            f"- blocked / gap feature 数：`{payload.get('blocked_or_gap_count')}`",
            f"- 下一路线：`{payload.get('recommended_next_research_task')}`",
            "",
            "2410 只做 `growth_tilt_engine` source feature 到 contract requirement 的"
            "映射与缺口分类。它不修复 growth tilt engine、不生成新信号、不执行 replay "
            "validation、不清除或降级 blocker、不恢复 candidate search。",
            "",
            "## Mapping Status Policy",
            "",
            "```json",
            _json_block(payload.get("mapping_statuses_allowed", [])),
            "```",
            "",
            "## Source Feature Contract Mapping",
            "",
            "```json",
            _json_block(payload.get("source_feature_contract_mapping", {})),
            "```",
            "",
            "## Contract Mapping Validation",
            "",
            "```json",
            _json_block(payload.get("contract_mapping_validation", {})),
            "```",
            "",
            "## Unresolved Gap Summary",
            "",
            "```json",
            _json_block(payload.get("unresolved_gap_summary", {})),
            "```",
            "",
            "## Data Quality Boundary",
            "",
            f"- data_quality_gate_executed：`{payload.get('data_quality_gate_executed')}`",
            f"- data_quality_gate_reason：`{payload.get('data_quality_gate_reason')}`",
            "",
            "## Safety Boundary",
            "",
            f"- blockers_resolved：`{payload.get('blockers_resolved')}`",
            f"- blockers_downgraded：`{payload.get('blockers_downgraded')}`",
            (
                "- growth_tilt_engine_blocking_gap_resolved："
                f"`{payload.get('growth_tilt_engine_blocking_gap_resolved')}`"
            ),
            f"- candidate_search_enabled：`{payload.get('candidate_search_enabled')}`",
            f"- observation_enabled：`{payload.get('observation_enabled')}`",
            f"- paper_shadow_enabled：`{payload.get('paper_shadow_enabled')}`",
            f"- production_enabled：`{payload.get('production_enabled')}`",
            f"- broker_enabled：`{payload.get('broker_enabled')}`",
        ]
    )


def _validation_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine contract mapping validation",
            "",
            f"- status：`{payload.get('status')}`",
            f"- validation ready：`{payload.get('contract_mapping_validation_ready')}`",
            f"- 下一路线：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("contract_mapping_validation", {})),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2411 route",
            "",
            f"- 当前任务：`{TASK_REGISTER_ID}`",
            f"- 当前状态：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2411 应把 growth tilt source feature mapping 暴露出的"
            "未解除缺口转成 remediation plan。该路线仍不得降级 blocker、恢复 "
            "candidate search、进入 observation / paper-shadow / production，或触发 "
            "broker/order 路径。",
        ]
    )


def _registry_entry(registry: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    for entry in _as_list(registry.get("entries")):
        row = _as_mapping(entry)
        if row.get("input_id") == input_id:
            return row
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
