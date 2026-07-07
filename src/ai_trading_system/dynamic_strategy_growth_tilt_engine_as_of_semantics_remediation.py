from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
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
from ai_trading_system.research_quality.growth_tilt_engine_as_of_remediation import (
    ALLOWED_AS_OF_REMEDIATION_STATUSES,
    NEXT_ROUTE,
    TARGET_REMEDIATION_CATEGORY,
    build_growth_tilt_as_of_semantics_remediation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2412"
TASK_REGISTER_ID = "TRADING-2412_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION"
REPORT_TYPE = "growth_tilt_engine_as_of_semantics_remediation"
SCHEMA_VERSION = "growth_tilt_engine_as_of_semantics_remediation.v1"
READY_STATUS = "GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS"
BLOCKED_SOURCE_STATUS = "GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_BLOCKED_SOURCE"
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2411", "TRADING-2410", "TRADING-2409")
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_AS_OF_SEMANTICS_REMEDIATION_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "fix_complete_source_traceability",
    "fix_signal_validity_dependency",
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

DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "remediation_plan_result.json"
)
DEFAULT_SOURCE_2411_CONTRACT_GAP_REMEDIATION_PLAN_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "contract_gap_remediation_plan.json"
)
DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "ordered_remediation_items.json"
)
DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT
    / "unresolved_blocker_summary.json"
)
DEFAULT_SOURCE_2411_RESEARCH_DOC_PATH = (
    m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_DOCS_ROOT
    / "growth_tilt_engine_contract_gap_remediation_plan.md"
)
DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "mapping_result.json"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_as_of_semantics_remediation(
    *,
    source_2411_remediation_plan_result_path: Path = (
        DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH
    ),
    source_2411_contract_gap_remediation_plan_path: Path = (
        DEFAULT_SOURCE_2411_CONTRACT_GAP_REMEDIATION_PLAN_PATH
    ),
    source_2411_ordered_remediation_items_path: Path = (
        DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH
    ),
    source_2411_unresolved_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH
    ),
    source_2411_research_doc_path: Path = DEFAULT_SOURCE_2411_RESEARCH_DOC_PATH,
    source_2410_mapping_result_path: Path = DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_contract_gap_remediation_plan_path=(
            source_2411_contract_gap_remediation_plan_path
        ),
        source_2411_ordered_remediation_items_path=(
            source_2411_ordered_remediation_items_path
        ),
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        source_2411_research_doc_path=source_2411_research_doc_path,
        source_2410_mapping_result_path=source_2410_mapping_result_path,
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
        payload.update(_ready_sections(sources, as_of_date=as_of_date))
    else:
        payload.update(_blocked_sections())
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_2411_remediation_plan_result_path: Path,
    source_2411_contract_gap_remediation_plan_path: Path,
    source_2411_ordered_remediation_items_path: Path,
    source_2411_unresolved_blocker_summary_path: Path,
    source_2411_research_doc_path: Path,
    source_2410_mapping_result_path: Path,
    report_registry_path: Path,
    artifact_catalog_path: Path,
) -> dict[str, Any]:
    report_registry = safe_load_yaml_path(report_registry_path)
    if isinstance(report_registry, dict):
        report_registry = {**report_registry, "path": str(report_registry_path)}
    return {
        "remediation_plan_result_2411": _load_json_document(
            source_2411_remediation_plan_result_path
        ),
        "contract_gap_remediation_plan_2411": _load_json_document(
            source_2411_contract_gap_remediation_plan_path
        ),
        "ordered_remediation_items_2411": _load_json_document(
            source_2411_ordered_remediation_items_path
        ),
        "unresolved_blocker_summary_2411": _load_json_document(
            source_2411_unresolved_blocker_summary_path
        ),
        "research_doc_2411": _load_text_document(source_2411_research_doc_path),
        "mapping_result_2410": _load_json_document(source_2410_mapping_result_path),
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "remediation_plan_result_2411": str(source_2411_remediation_plan_result_path),
            "contract_gap_remediation_plan_2411": str(
                source_2411_contract_gap_remediation_plan_path
            ),
            "ordered_remediation_items_2411": str(
                source_2411_ordered_remediation_items_path
            ),
            "unresolved_blocker_summary_2411": str(
                source_2411_unresolved_blocker_summary_path
            ),
            "research_doc_2411": str(source_2411_research_doc_path),
            "mapping_result_2410": str(source_2410_mapping_result_path),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "remediation_plan_result_2411",
        "contract_gap_remediation_plan_2411",
        "ordered_remediation_items_2411",
        "unresolved_blocker_summary_2411",
        "mapping_result_2410",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")

    for key in ("research_doc_2411", "artifact_catalog"):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source document: {key} -> {source.get('_path')}")

    result_2411 = _as_mapping(sources.get("remediation_plan_result_2411"))
    if result_2411.get("status") != m2411.READY_STATUS:
        errors.append("2411 remediation plan result must be ready with blockers unresolved")
    if result_2411.get("recommended_next_research_task") != m2411.NEXT_ROUTE:
        errors.append("2411 remediation plan result must route to TRADING-2412")
    if result_2411.get("gap_count") != 7:
        errors.append("2411 gap_count must be 7 for TRADING-2412")
    if result_2411.get("unclassified_remediation_item_count") != 0:
        errors.append("2411 must have no unclassified remediation items")
    if result_2411.get("silent_gap_resolution_count") != 0:
        errors.append("2411 must have no silent gap resolution")
    if result_2411.get("silent_blocker_downgrade_count") != 0:
        errors.append("2411 must have no silent blocker downgrade")
    for field in m2411.SAFETY_FALSE_FIELDS:
        if result_2411.get(field) is True:
            errors.append(f"2411 safety field must remain false: {field}")

    contract_plan = _as_mapping(
        _as_mapping(sources.get("contract_gap_remediation_plan_2411")).get(
            "contract_gap_remediation_plan"
        )
    )
    ordered_items_doc = _as_mapping(sources.get("ordered_remediation_items_2411"))
    ordered_items = _as_list(ordered_items_doc.get("ordered_remediation_items"))
    if not ordered_items:
        errors.append("2411 ordered remediation items must be present")
    if len(ordered_items) != result_2411.get("remediation_item_count"):
        errors.append("2411 ordered remediation item count must match result")
    as_of_items = [item for item in ordered_items if _as_mapping(item).get(
        "remediation_category"
    ) == TARGET_REMEDIATION_CATEGORY]
    if not as_of_items:
        errors.append("2411 must contain as_of_semantics_required remediation items")
    if contract_plan.get("gap_count") != result_2411.get("gap_count"):
        errors.append("2411 contract gap plan count must match result")

    blocker_summary = _as_mapping(
        _as_mapping(sources.get("unresolved_blocker_summary_2411")).get(
            "unresolved_blocker_summary"
        )
    )
    if blocker_summary.get("growth_tilt_engine_blocker_resolved") is not False:
        errors.append("2411 summary must keep growth_tilt_engine unresolved")
    if blocker_summary.get("valid_until_window_blocker_resolved") is not False:
        errors.append("2411 summary must keep valid_until_window unresolved")

    mapping_result = _as_mapping(sources.get("mapping_result_2410"))
    mapping_rows = _as_list(
        _as_mapping(mapping_result.get("source_feature_contract_mapping")).get(
            "mapping_rows"
        )
    )
    feature_ids = {str(row.get("feature_id")) for row in map(_as_mapping, mapping_rows)}
    for item in as_of_items:
        feature_id = str(_as_mapping(item).get("feature_id"))
        if feature_id not in feature_ids:
            errors.append(f"2410 mapping missing as-of remediation feature: {feature_id}")
    if mapping_result.get("status") != m2410.READY_STATUS:
        errors.append("2410 mapping result must be ready with blockers unresolved")

    research_doc = str(_as_mapping(sources.get("research_doc_2411")).get("text", ""))
    if m2411.READY_STATUS not in research_doc:
        errors.append("2411 research doc must reference ready-with-blockers status")

    report_registry = _as_mapping(sources.get("report_registry"))
    if not _registry_has_2411_entry(report_registry):
        errors.append("report registry missing 2411 remediation plan entry")

    artifact_catalog = str(_as_mapping(sources.get("artifact_catalog")).get("text", ""))
    if "growth-tilt-engine-contract-gap-remediation-plan" not in artifact_catalog:
        errors.append("artifact catalog missing 2411 remediation plan command")
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


def _ready_sections(
    sources: Mapping[str, Any],
    *,
    as_of_date: date | None,
) -> dict[str, Any]:
    remediation = build_growth_tilt_as_of_semantics_remediation(
        _as_mapping(sources.get("remediation_plan_result_2411")),
        _as_mapping(sources.get("mapping_result_2410")),
        as_of_date=as_of_date.isoformat() if as_of_date else None,
    )
    validation = _as_mapping(remediation.get("as_of_remediation_validation"))
    return {
        "source_remediation_plan_ready": True,
        "as_of_remediation_completed": remediation.get("as_of_remediation_completed"),
        "as_of_contract_metadata_ready": True,
        "before_after_remediation_ready": True,
        "updated_source_feature_mapping_ready": True,
        "remaining_blocker_summary_ready": True,
        "input_gap_count": remediation.get("input_gap_count"),
        "as_of_gap_count": remediation.get("as_of_gap_count"),
        "as_of_remediated_count": remediation.get("as_of_remediated_count"),
        "remaining_as_of_gap_count": remediation.get("remaining_as_of_gap_count"),
        "remaining_blocked_or_gap_count": remediation.get("remaining_blocked_or_gap_count"),
        "contract_ready_count": remediation.get("contract_ready_count"),
        "allowed_as_of_remediation_statuses": list(ALLOWED_AS_OF_REMEDIATION_STATUSES),
        "as_of_remediation": remediation,
        "as_of_contract_metadata": remediation.get("as_of_contract_metadata"),
        "before_after_remediation": remediation.get("before_after_remediation"),
        "updated_source_feature_mapping": remediation.get("updated_source_feature_mapping"),
        "remaining_blocker_summary": remediation.get("remaining_blocker_summary"),
        "as_of_remediation_validation": validation,
        "as_of_status_unclassified_count": validation.get(
            "as_of_status_unclassified_count"
        ),
        "lookahead_violation_count": validation.get("lookahead_violation_count"),
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2412 only remediates as-of semantics; TRADING-2413 should address "
            "source traceability while candidate search, observation, paper-shadow, "
            "production, and broker paths remain disabled."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_remediation_plan_ready": False,
        "as_of_remediation_completed": False,
        "as_of_contract_metadata_ready": False,
        "before_after_remediation_ready": False,
        "updated_source_feature_mapping_ready": False,
        "remaining_blocker_summary_ready": False,
        "input_gap_count": None,
        "as_of_gap_count": 0,
        "as_of_remediated_count": 0,
        "remaining_as_of_gap_count": None,
        "remaining_blocked_or_gap_count": None,
        "contract_ready_count": 0,
        "allowed_as_of_remediation_statuses": list(ALLOWED_AS_OF_REMEDIATION_STATUSES),
        "as_of_remediation": {},
        "as_of_contract_metadata": {},
        "before_after_remediation": {},
        "updated_source_feature_mapping": {},
        "remaining_blocker_summary": {},
        "as_of_remediation_validation": {},
        "as_of_status_unclassified_count": None,
        "lookahead_violation_count": None,
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "as_of_remediation_result.json"),
        "as_of_contract_metadata_json": str(output_root / "as_of_contract_metadata.json"),
        "before_after_remediation_json": str(output_root / "before_after_remediation.json"),
        "updated_source_feature_mapping_json": str(
            output_root / "updated_source_feature_mapping.json"
        ),
        "remaining_blocker_summary_json": str(output_root / "remaining_blocker_summary.json"),
        "markdown_path": str(docs_root / "growth_tilt_engine_as_of_semantics_remediation.md"),
        "as_of_contract_markdown": str(
            docs_root / "growth_tilt_engine_as_of_contract_metadata.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2413_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_section_json_artifact(
        paths["as_of_contract_metadata_json"],
        "growth_tilt_engine_as_of_contract_metadata",
        "growth_tilt_engine_as_of_contract_metadata.v1",
        payload,
        "as_of_contract_metadata",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["before_after_remediation_json"],
        "growth_tilt_engine_as_of_before_after_remediation",
        "growth_tilt_engine_as_of_before_after_remediation.v1",
        payload,
        "before_after_remediation",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["updated_source_feature_mapping_json"],
        "growth_tilt_engine_source_feature_mapping_after_as_of",
        "growth_tilt_engine_source_feature_mapping_after_as_of.v1",
        payload,
        "updated_source_feature_mapping",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["remaining_blocker_summary_json"],
        "growth_tilt_engine_as_of_remaining_blocker_summary",
        "growth_tilt_engine_as_of_remaining_blocker_summary.v1",
        payload,
        "remaining_blocker_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["as_of_contract_markdown"]),
        _as_of_contract_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine as-of semantics remediation",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- input gap count：`{payload.get('input_gap_count')}`",
            f"- as-of gap count：`{payload.get('as_of_gap_count')}`",
            f"- as-of remediated count：`{payload.get('as_of_remediated_count')}`",
            (
                "- remaining blocked or gap count："
                f"`{payload.get('remaining_blocked_or_gap_count')}`"
            ),
            f"- contract ready count：`{payload.get('contract_ready_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "2412 只补齐 as-of semantics 和 no-lookahead contract。source "
            "traceability、validity dependency、PIT gate 和 valid_until_window 仍未在本任务中修复，"
            "因此 `growth_tilt_engine` blocker 不能解除或降级。",
            "",
            "## Before / After",
            "",
            "```json",
            _json_block(payload.get("before_after_remediation", {})),
            "```",
            "",
            "## As-Of Contract Metadata",
            "",
            "```json",
            _json_block(payload.get("as_of_contract_metadata", {})),
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


def _as_of_contract_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine as-of contract metadata",
            "",
            f"- status：`{payload.get('status')}`",
            f"- as-of remediation completed：`{payload.get('as_of_remediation_completed')}`",
            f"- lookahead violation count：`{payload.get('lookahead_violation_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("as_of_contract_metadata", {})),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2413 route",
            "",
            f"- 当前任务：`{TASK_REGISTER_ID}`",
            f"- 当前状态：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2413 应处理 source feature traceability remediation。2413 仍必须保持 "
            "candidate_search=false、observation=false、paper_shadow=false、production=false、"
            "broker=false，且不得解除或降级 blocker，除非后续 owner review 明确批准。",
        ]
    )


def _registry_has_2411_entry(report_registry: Mapping[str, Any]) -> bool:
    for entry in _as_list(report_registry.get("reports")):
        row = _as_mapping(entry)
        if row.get("report_id") == "growth_tilt_engine_contract_gap_remediation_plan":
            return row.get("production_effect") == "none" and row.get("broker_action") == "none"
    return False


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
