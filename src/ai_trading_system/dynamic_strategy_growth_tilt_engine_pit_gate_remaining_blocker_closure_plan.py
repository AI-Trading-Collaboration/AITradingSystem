from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
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
    growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as closure_plan,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2416"
TASK_REGISTER_ID = (
    "TRADING-2416_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN"
)
REPORT_TYPE = "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan"
SCHEMA_VERSION = closure_plan.CLOSURE_PLAN_SCHEMA_VERSION
READY_STATUS = closure_plan.READY_STATUS
BLOCKED_SOURCE_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_BLOCKED_SOURCE"
)
NEXT_ROUTE = closure_plan.NEXT_ROUTE
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2410",
    "TRADING-2411",
    "TRADING-2412",
    "TRADING-2413",
    "TRADING-2414",
    "TRADING-2415",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_PRIOR_ARTIFACTS_ONLY_"
    "NO_FRESH_MARKET_DATA"
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

DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_snapshot_result.json"
)
DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_matrix.json"
)
DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_validation.json"
)
DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_PIT_INPUT_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_strategy_pit_input_registry.yaml"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan(
    *,
    source_2415_readiness_snapshot_result_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH
    ),
    source_2415_readiness_matrix_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH
    ),
    source_2415_readiness_validation_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH
    ),
    source_2415_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    pit_input_registry_path: Path = DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2415_readiness_validation_path=source_2415_readiness_validation_path,
        source_2415_remaining_blocker_summary_path=(
            source_2415_remaining_blocker_summary_path
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
    source_2415_readiness_snapshot_result_path: Path,
    source_2415_readiness_matrix_path: Path,
    source_2415_readiness_validation_path: Path,
    source_2415_remaining_blocker_summary_path: Path,
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
        "readiness_snapshot_result_2415": _load_json_document(
            source_2415_readiness_snapshot_result_path
        ),
        "readiness_matrix_2415": _load_json_document(
            source_2415_readiness_matrix_path
        ),
        "readiness_validation_2415": _load_json_document(
            source_2415_readiness_validation_path
        ),
        "remaining_blocker_summary_2415": _load_json_document(
            source_2415_remaining_blocker_summary_path
        ),
        "pit_input_registry": pit_input_registry,
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "readiness_snapshot_result_2415": str(
                source_2415_readiness_snapshot_result_path
            ),
            "readiness_matrix_2415": str(source_2415_readiness_matrix_path),
            "readiness_validation_2415": str(source_2415_readiness_validation_path),
            "remaining_blocker_summary_2415": str(
                source_2415_remaining_blocker_summary_path
            ),
            "pit_input_registry": str(pit_input_registry_path),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "readiness_snapshot_result_2415",
        "readiness_matrix_2415",
        "readiness_validation_2415",
        "remaining_blocker_summary_2415",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")
    artifact_catalog = _as_mapping(sources.get("artifact_catalog"))
    if artifact_catalog.get("_missing") is True:
        errors.append(
            f"missing source document: artifact_catalog -> {artifact_catalog.get('_path')}"
        )

    snapshot = _as_mapping(sources.get("readiness_snapshot_result_2415"))
    if snapshot.get("status") != m2415.READY_STATUS:
        errors.append("2415 readiness snapshot must be ready with blockers unresolved")
    if snapshot.get("recommended_next_research_task") != m2415.NEXT_ROUTE:
        errors.append("2415 readiness snapshot must route to TRADING-2416")
    expected_counts = {
        "source_feature_count": 10,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 10,
        "blocked_by_source_traceability_count": 5,
        "blocked_by_valid_until_window_count": 1,
    }
    for field, expected in expected_counts.items():
        if snapshot.get(field) != expected:
            errors.append(f"2415 snapshot {field} must be {expected}")
    for field in m2415.SAFETY_FALSE_FIELDS:
        if snapshot.get(field) is True:
            errors.append(f"2415 safety field must remain false: {field}")

    matrix = _as_mapping(
        _as_mapping(sources.get("readiness_matrix_2415")).get(
            "pit_gate_readiness_matrix"
        )
    )
    if len(_as_list(matrix.get("matrix_rows"))) != 10:
        errors.append("2415 readiness matrix must contain 10 rows")

    validation = _as_mapping(
        _as_mapping(sources.get("readiness_validation_2415")).get(
            "pit_gate_readiness_validation"
        )
    )
    if validation.get("valid") is not True:
        errors.append("2415 readiness validation must be valid")
    if validation.get("contract_ready_count") != 0:
        errors.append("2415 readiness validation contract_ready_count must be 0")

    summary = _as_mapping(
        _as_mapping(sources.get("remaining_blocker_summary_2415")).get(
            "remaining_blocker_summary"
        )
    )
    if summary.get("growth_tilt_engine_blocker_resolved") is not False:
        errors.append("2415 summary must keep growth_tilt_engine unresolved")
    if summary.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2415 summary must keep valid_until_window unresolved")
    if summary.get("blocked_by_source_traceability_count") != 5:
        errors.append("2415 summary must keep five source traceability blockers")
    if summary.get("blocked_by_valid_until_window_count") != 1:
        errors.append("2415 summary must keep one valid_until_window blocker")

    pit_registry = _as_mapping(sources.get("pit_input_registry"))
    if _pit_registry_severity(pit_registry, "growth_tilt_engine") != "BLOCKING":
        errors.append("PIT input registry must keep growth_tilt_engine BLOCKING")
    if _pit_registry_severity(pit_registry, "valid_until_window") != "BLOCKING":
        errors.append("PIT input registry must keep valid_until_window BLOCKING")

    report_registry = _as_mapping(sources.get("report_registry"))
    for report_id in (
        "growth_tilt_engine_pit_gate_readiness_snapshot",
        "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
    ):
        if not _registry_has_report_id(report_registry, report_id):
            errors.append(f"report registry missing required entry: {report_id}")

    catalog_text = str(artifact_catalog.get("text", ""))
    for command in (
        "growth-tilt-engine-pit-gate-readiness-snapshot",
        "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan",
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
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
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
    plan = closure_plan.build_growth_tilt_pit_gate_remaining_blocker_closure_plan(
        _as_mapping(sources.get("readiness_snapshot_result_2415")),
        _as_mapping(sources.get("readiness_matrix_2415")),
        _as_mapping(sources.get("remaining_blocker_summary_2415")),
        pit_input_registry=_as_mapping(sources.get("pit_input_registry")),
    )
    return {
        "pit_gate_readiness_snapshot_source_ready": True,
        "remaining_blocker_matrix_ready": plan.get("remaining_blocker_matrix_ready"),
        "source_traceability_closure_plan_ready": plan.get(
            "source_traceability_closure_plan_ready"
        ),
        "as_of_evidence_closure_plan_ready": plan.get(
            "as_of_evidence_closure_plan_ready"
        ),
        "valid_until_dependency_closure_plan_ready": plan.get(
            "valid_until_dependency_closure_plan_ready"
        ),
        "pit_gate_evidence_requirements_ready": plan.get(
            "pit_gate_evidence_requirements_ready"
        ),
        "closure_plan": plan,
        "remaining_blocker_matrix": plan.get("remaining_blocker_matrix"),
        "source_traceability_closure_plan": plan.get(
            "source_traceability_closure_plan"
        ),
        "as_of_evidence_closure_plan": plan.get("as_of_evidence_closure_plan"),
        "valid_until_dependency_closure_plan": plan.get(
            "valid_until_dependency_closure_plan"
        ),
        "pit_gate_evidence_requirements": plan.get("pit_gate_evidence_requirements"),
        "closure_priority": plan.get("closure_priority"),
        "proposed_sequence": plan.get("proposed_sequence"),
        "closure_plan_validation": plan.get("closure_plan_validation"),
        "source_feature_count": plan.get("source_feature_count"),
        "pit_gate_ready_count": plan.get("pit_gate_ready_count"),
        "contract_ready_count": plan.get("contract_ready_count"),
        "pit_gate_blocked_count": plan.get("pit_gate_blocked_count"),
        "blocked_by_source_traceability_count": plan.get(
            "blocked_by_source_traceability_count"
        ),
        "blocked_by_valid_until_window_count": plan.get(
            "blocked_by_valid_until_window_count"
        ),
        "as_of_contract_gap_count": plan.get("as_of_contract_gap_count"),
        "upstream_artifact_gap_count": plan.get("upstream_artifact_gap_count"),
        "signal_validity_dependency_gap_count": plan.get(
            "signal_validity_dependency_gap_count"
        ),
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2416 only converts the TRADING-2415 PIT gate readiness snapshot "
            "into remaining blocker closure plans; source traceability and upstream "
            "artifact gaps must be closed in TRADING-2417 before any readiness "
            "recheck or owner downgrade review."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "pit_gate_readiness_snapshot_source_ready": False,
        "remaining_blocker_matrix_ready": False,
        "source_traceability_closure_plan_ready": False,
        "as_of_evidence_closure_plan_ready": False,
        "valid_until_dependency_closure_plan_ready": False,
        "pit_gate_evidence_requirements_ready": False,
        "closure_plan": {},
        "remaining_blocker_matrix": {},
        "source_traceability_closure_plan": {},
        "as_of_evidence_closure_plan": {},
        "valid_until_dependency_closure_plan": {},
        "pit_gate_evidence_requirements": {},
        "closure_priority": {},
        "proposed_sequence": {},
        "closure_plan_validation": {},
        "source_feature_count": 0,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 0,
        "blocked_by_source_traceability_count": 0,
        "blocked_by_valid_until_window_count": 0,
        "as_of_contract_gap_count": 0,
        "upstream_artifact_gap_count": 0,
        "signal_validity_dependency_gap_count": 0,
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "closure_plan_result.json"),
        "remaining_blocker_matrix_json": str(
            output_root / "remaining_blocker_matrix.json"
        ),
        "source_traceability_closure_plan_json": str(
            output_root / "source_traceability_closure_plan.json"
        ),
        "as_of_evidence_closure_plan_json": str(
            output_root / "as_of_evidence_closure_plan.json"
        ),
        "valid_until_dependency_closure_plan_json": str(
            output_root / "valid_until_dependency_closure_plan.json"
        ),
        "pit_gate_evidence_requirements_json": str(
            output_root / "pit_gate_evidence_requirements.json"
        ),
        "markdown_path": str(
            docs_root / "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan.md"
        ),
        "remaining_blocker_matrix_markdown": str(
            docs_root / "growth_tilt_engine_remaining_blocker_matrix.md"
        ),
        "source_traceability_closure_plan_markdown": str(
            docs_root / "growth_tilt_engine_source_traceability_closure_plan.md"
        ),
        "valid_until_dependency_closure_plan_markdown": str(
            docs_root / "growth_tilt_engine_valid_until_dependency_closure_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2417_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_section_json_artifact(
        paths["remaining_blocker_matrix_json"],
        "growth_tilt_engine_pit_gate_remaining_blocker_matrix",
        closure_plan.REMAINING_BLOCKER_MATRIX_SCHEMA_VERSION,
        payload,
        "remaining_blocker_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["source_traceability_closure_plan_json"],
        "growth_tilt_engine_source_traceability_closure_plan",
        closure_plan.SOURCE_TRACEABILITY_CLOSURE_PLAN_SCHEMA_VERSION,
        payload,
        "source_traceability_closure_plan",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["as_of_evidence_closure_plan_json"],
        "growth_tilt_engine_as_of_evidence_closure_plan",
        closure_plan.AS_OF_EVIDENCE_CLOSURE_PLAN_SCHEMA_VERSION,
        payload,
        "as_of_evidence_closure_plan",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["valid_until_dependency_closure_plan_json"],
        "growth_tilt_engine_valid_until_dependency_closure_plan",
        closure_plan.VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_SCHEMA_VERSION,
        payload,
        "valid_until_dependency_closure_plan",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["pit_gate_evidence_requirements_json"],
        "growth_tilt_engine_pit_gate_evidence_requirements",
        closure_plan.PIT_GATE_EVIDENCE_REQUIREMENTS_SCHEMA_VERSION,
        payload,
        "pit_gate_evidence_requirements",
        task_id=TASK_ID,
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["remaining_blocker_matrix_markdown"]),
        _matrix_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["source_traceability_closure_plan_markdown"]),
        _source_traceability_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["valid_until_dependency_closure_plan_markdown"]),
        _valid_until_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine PIT gate remaining blocker closure plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- source feature count：`{payload.get('source_feature_count')}`",
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
            f"- recommended next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2415 没有说明 blocker 已解除。相反，2415 确认全部 10 个 "
            "source features 仍未达到 PIT gate ready / contract ready。2416 只把这些 "
            "remaining blockers 拆成 closure plan，为 2417 的 source traceability / "
            "upstream artifact closure 做准备。",
            "",
            "## Source findings from TRADING-2415",
            "",
            "```json",
            _json_block(payload.get("closure_plan", {}).get("current_readiness_from_2415", {})),
            "```",
            "",
            "## Current readiness snapshot interpretation",
            "",
            "```json",
            _json_block(payload.get("closure_plan", {}).get("readiness_interpretation", {})),
            "```",
            "",
            "## Remaining blocker matrix",
            "",
            "```json",
            _json_block(payload.get("remaining_blocker_matrix", {})),
            "```",
            "",
            "## Source traceability closure plan",
            "",
            "```json",
            _json_block(payload.get("source_traceability_closure_plan", {})),
            "```",
            "",
            "## As-of evidence closure plan",
            "",
            "```json",
            _json_block(payload.get("as_of_evidence_closure_plan", {})),
            "```",
            "",
            "## Valid-until dependency closure plan",
            "",
            "```json",
            _json_block(payload.get("valid_until_dependency_closure_plan", {})),
            "```",
            "",
            "## PIT gate evidence requirements",
            "",
            "```json",
            _json_block(payload.get("pit_gate_evidence_requirements", {})),
            "```",
            "",
            "## Proposed 2417-2420 sequence",
            "",
            "```json",
            _json_block(payload.get("proposed_sequence", {})),
            "```",
            "",
            "## Explicit non-approval list",
            "",
            "```json",
            _json_block(payload.get("explicit_non_approval_list", [])),
            "```",
            "",
            "## Recommended next route",
            "",
            "2417 应优先处理 source traceability / upstream artifact closure，因为 2415 "
            "中最大的明确 blocker group 是 source traceability：5 个 source features "
            "被其阻塞。在这些 evidence 未补齐前，as-of replay、PIT gate readiness 和 "
            "blocker downgrade owner review 都缺证据基础。",
            "",
            "## Data quality boundary",
            "",
            f"- data_quality_gate_executed：`{payload.get('data_quality_gate_executed')}`",
            f"- data_quality_gate_reason：`{payload.get('data_quality_gate_reason')}`",
            "",
            "## Safety boundary",
            "",
            (
                "- growth_tilt_engine_blocking_gap_resolved："
                f"`{payload.get('growth_tilt_engine_blocking_gap_resolved')}`"
            ),
            (
                "- growth_tilt_engine_severity_downgraded："
                f"`{payload.get('growth_tilt_engine_severity_downgraded')}`"
            ),
            (
                "- valid_until_window_blocking_gap_resolved："
                f"`{payload.get('valid_until_window_blocking_gap_resolved')}`"
            ),
            (
                "- valid_until_window_severity_downgraded："
                f"`{payload.get('valid_until_window_severity_downgraded')}`"
            ),
            f"- candidate_search_resumed：`{payload.get('candidate_search_resumed')}`",
            (
                "- research_only_observation_approved："
                f"`{payload.get('research_only_observation_approved')}`"
            ),
            f"- paper_shadow_enabled：`{payload.get('paper_shadow_enabled')}`",
            f"- production_enabled：`{payload.get('production_enabled')}`",
            f"- broker_action_enabled：`{payload.get('broker_action_enabled')}`",
            f"- daily_report_generated：`{payload.get('daily_report_generated')}`",
        ]
    )


def _matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine remaining blocker matrix",
            "",
            f"- status：`{payload.get('status')}`",
            f"- row count：`{payload.get('source_feature_count')}`",
            (
                "- source traceability blocker count："
                f"`{payload.get('blocked_by_source_traceability_count')}`"
            ),
            (
                "- valid_until_window blocker count："
                f"`{payload.get('blocked_by_valid_until_window_count')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("remaining_blocker_matrix", {})),
            "```",
        ]
    )


def _source_traceability_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine source traceability closure plan",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- source traceability blocker count："
                f"`{payload.get('blocked_by_source_traceability_count')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("source_traceability_closure_plan", {})),
            "```",
        ]
    )


def _valid_until_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine valid-until dependency closure plan",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- valid_until_window blocker count："
                f"`{payload.get('blocked_by_valid_until_window_count')}`"
            ),
            "",
            "```json",
            _json_block(payload.get("valid_until_dependency_closure_plan", {})),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2417 route",
            "",
            f"- 当前任务：`{TASK_REGISTER_ID}`",
            f"- 当前状态：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2417 应优先关闭 growth tilt engine 的 source traceability 与 "
            "upstream artifact gaps。2416 未解除或降级 blocker，candidate_search、"
            "observation、paper_shadow、scheduler、event_append、outcome_binding、"
            "production 和 broker 仍全部 disabled。",
        ]
    )


def _pit_registry_severity(registry: Mapping[str, Any], input_id: str) -> str | None:
    for entry in _as_list(registry.get("entries")):
        row = _as_mapping(entry)
        if row.get("input_id") == input_id:
            return str(row.get("severity"))
    return None


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
