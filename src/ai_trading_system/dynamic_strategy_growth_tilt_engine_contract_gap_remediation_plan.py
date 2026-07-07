from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

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
from ai_trading_system.research_quality.growth_tilt_engine_gap_remediation import (
    ALLOWED_REMEDIATION_CATEGORIES,
    GAP_MAPPING_STATUSES,
    build_growth_tilt_contract_gap_remediation_plan,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2411"
TASK_REGISTER_ID = "TRADING-2411_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN"
REPORT_TYPE = "growth_tilt_engine_contract_gap_remediation_plan"
SCHEMA_VERSION = "growth_tilt_engine_contract_gap_remediation_plan.v1"
READY_STATUS = "GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED"
BLOCKED_SOURCE_STATUS = "GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_BLOCKED_SOURCE"
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2410", "TRADING-2409", "TRADING-2406")
NEXT_ROUTE = "TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_CONTRACT_GAP_REMEDIATION_PLAN_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "implement_remediation",
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

DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_DOCS_ROOT = (
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
DEFAULT_SOURCE_2410_CONTRACT_MAPPING_VALIDATION_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "contract_mapping_validation.json"
)
DEFAULT_SOURCE_2410_UNRESOLVED_GAP_SUMMARY_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT
    / "unresolved_gap_summary.json"
)
DEFAULT_SOURCE_2410_RESEARCH_DOC_PATH = (
    m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_DOCS_ROOT
    / "growth_tilt_engine_source_feature_contract_mapping.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_contract_gap_remediation_plan(
    *,
    source_2410_mapping_result_path: Path = DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Path = (
        DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH
    ),
    source_2410_contract_mapping_validation_path: Path = (
        DEFAULT_SOURCE_2410_CONTRACT_MAPPING_VALIDATION_PATH
    ),
    source_2410_unresolved_gap_summary_path: Path = (
        DEFAULT_SOURCE_2410_UNRESOLVED_GAP_SUMMARY_PATH
    ),
    source_2410_research_doc_path: Path = DEFAULT_SOURCE_2410_RESEARCH_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        source_2410_contract_mapping_validation_path=(
            source_2410_contract_mapping_validation_path
        ),
        source_2410_unresolved_gap_summary_path=source_2410_unresolved_gap_summary_path,
        source_2410_research_doc_path=source_2410_research_doc_path,
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
    source_2410_contract_mapping_validation_path: Path,
    source_2410_unresolved_gap_summary_path: Path,
    source_2410_research_doc_path: Path,
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
        "contract_mapping_validation_2410": _load_json_document(
            source_2410_contract_mapping_validation_path
        ),
        "unresolved_gap_summary_2410": _load_json_document(
            source_2410_unresolved_gap_summary_path
        ),
        "research_doc_2410": _load_text_document(source_2410_research_doc_path),
        "report_registry": report_registry,
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "source_paths": {
            "mapping_result_2410": str(source_2410_mapping_result_path),
            "source_feature_contract_mapping_2410": str(
                source_2410_source_feature_contract_mapping_path
            ),
            "contract_mapping_validation_2410": str(
                source_2410_contract_mapping_validation_path
            ),
            "unresolved_gap_summary_2410": str(source_2410_unresolved_gap_summary_path),
            "research_doc_2410": str(source_2410_research_doc_path),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(artifact_catalog_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "mapping_result_2410",
        "source_feature_contract_mapping_2410",
        "contract_mapping_validation_2410",
        "unresolved_gap_summary_2410",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")

    for key in ("research_doc_2410", "artifact_catalog"):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source document: {key} -> {source.get('_path')}")

    mapping_result = _as_mapping(sources.get("mapping_result_2410"))
    if mapping_result.get("status") != m2410.READY_STATUS:
        errors.append("2410 mapping result must be ready with blockers unresolved")
    if mapping_result.get("recommended_next_research_task") != m2410.NEXT_ROUTE:
        errors.append("2410 mapping result must route to TRADING-2411")
    if mapping_result.get("blocked_or_gap_count") != 7:
        errors.append("2410 blocked_or_gap_count must be 7 for TRADING-2411")
    if mapping_result.get("unclassified_feature_count") != 0:
        errors.append("2410 mapping result must have no unclassified features")
    for field in m2410.SAFETY_FALSE_FIELDS:
        if mapping_result.get(field) is True:
            errors.append(f"2410 safety field must remain false: {field}")

    contract_mapping = _as_mapping(
        _as_mapping(sources.get("source_feature_contract_mapping_2410")).get(
            "source_feature_contract_mapping"
        )
    )
    mapping_rows = _as_list(contract_mapping.get("mapping_rows"))
    if not mapping_rows:
        errors.append("2410 source feature contract mapping must contain rows")
    gap_rows = [
        row
        for row in mapping_rows
        if _as_mapping(row).get("mapping_status") in GAP_MAPPING_STATUSES
    ]
    if len(gap_rows) != mapping_result.get("blocked_or_gap_count"):
        errors.append("2410 gap row count must match blocked_or_gap_count")

    validation = _as_mapping(
        _as_mapping(sources.get("contract_mapping_validation_2410")).get(
            "contract_mapping_validation"
        )
    )
    if validation.get("valid") is not True:
        errors.append("2410 contract mapping validation must be valid")
    if validation.get("unclassified_feature_count") != 0:
        errors.append("2410 contract mapping validation must have no unclassified rows")

    gap_summary = _as_mapping(
        _as_mapping(sources.get("unresolved_gap_summary_2410")).get(
            "unresolved_gap_summary"
        )
    )
    if gap_summary.get("growth_tilt_engine_blocking_gap_resolved") is not False:
        errors.append("2410 unresolved gap summary must keep growth_tilt_engine unresolved")
    if gap_summary.get("growth_tilt_engine_severity_downgraded") is not False:
        errors.append("2410 unresolved gap summary must keep severity undowngraded")

    research_doc = str(_as_mapping(sources.get("research_doc_2410")).get("text", ""))
    if m2410.READY_STATUS not in research_doc:
        errors.append("2410 research doc must reference ready-with-blockers status")

    report_registry = _as_mapping(sources.get("report_registry"))
    if not _registry_has_2410_entry(report_registry):
        errors.append("report registry missing 2410 mapping entry")

    artifact_catalog = str(_as_mapping(sources.get("artifact_catalog")).get("text", ""))
    if "growth-tilt-engine-source-feature-contract-mapping" not in artifact_catalog:
        errors.append("artifact catalog missing 2410 mapping command")
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
    mapping_result = _as_mapping(sources.get("mapping_result_2410"))
    remediation_plan = build_growth_tilt_contract_gap_remediation_plan(mapping_result)
    validation = _as_mapping(remediation_plan.get("remediation_plan_validation"))
    validation_design = _as_mapping(remediation_plan.get("validation_design"))
    unresolved_summary = _as_mapping(remediation_plan.get("unresolved_blocker_summary"))
    items = _as_list(remediation_plan.get("ordered_remediation_items"))
    return {
        "source_mapping_ready": True,
        "contract_gap_remediation_plan_ready": True,
        "ordered_remediation_items_ready": True,
        "validation_design_ready": True,
        "unresolved_blocker_summary_ready": True,
        "source_blocked_or_gap_count": remediation_plan.get("source_blocked_or_gap_count"),
        "gap_count": remediation_plan.get("gap_count"),
        "remediation_item_count": len(items),
        "allowed_remediation_categories": list(ALLOWED_REMEDIATION_CATEGORIES),
        "contract_gap_remediation_plan": remediation_plan,
        "ordered_remediation_items": items,
        "validation_design": validation_design,
        "unresolved_blocker_summary": unresolved_summary,
        "remediation_plan_validation": validation,
        "unclassified_remediation_item_count": validation.get(
            "unclassified_remediation_item_count"
        ),
        "silent_gap_resolution_count": validation.get("silent_gap_resolution_count"),
        "silent_blocker_downgrade_count": validation.get(
            "silent_blocker_downgrade_count"
        ),
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2411 is plan-only; TRADING-2412 should begin concrete as-of "
            "semantics remediation while keeping candidate search, observation, "
            "paper-shadow, production, and broker paths disabled."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "source_mapping_ready": False,
        "contract_gap_remediation_plan_ready": False,
        "ordered_remediation_items_ready": False,
        "validation_design_ready": False,
        "unresolved_blocker_summary_ready": False,
        "source_blocked_or_gap_count": None,
        "gap_count": 0,
        "remediation_item_count": 0,
        "allowed_remediation_categories": list(ALLOWED_REMEDIATION_CATEGORIES),
        "contract_gap_remediation_plan": {},
        "ordered_remediation_items": [],
        "validation_design": {},
        "unresolved_blocker_summary": {},
        "remediation_plan_validation": {},
        "unclassified_remediation_item_count": None,
        "silent_gap_resolution_count": None,
        "silent_blocker_downgrade_count": None,
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    paths = {
        "json_path": str(output_root / "remediation_plan_result.json"),
        "contract_gap_remediation_plan_json": str(
            output_root / "contract_gap_remediation_plan.json"
        ),
        "ordered_remediation_items_json": str(
            output_root / "ordered_remediation_items.json"
        ),
        "validation_design_json": str(output_root / "validation_design.json"),
        "unresolved_blocker_summary_json": str(
            output_root / "unresolved_blocker_summary.json"
        ),
        "markdown_path": str(
            docs_root / "growth_tilt_engine_contract_gap_remediation_plan.md"
        ),
        "validation_design_markdown": str(
            docs_root / "growth_tilt_engine_contract_gap_validation_design.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2412_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_section_json_artifact(
        paths["contract_gap_remediation_plan_json"],
        "growth_tilt_engine_contract_gap_remediation_plan",
        "growth_tilt_engine_contract_gap_remediation_plan.v1",
        payload,
        "contract_gap_remediation_plan",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["ordered_remediation_items_json"],
        "growth_tilt_engine_ordered_remediation_items",
        "growth_tilt_engine_ordered_remediation_items.v1",
        payload,
        "ordered_remediation_items",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["validation_design_json"],
        "growth_tilt_engine_contract_gap_validation_design",
        "growth_tilt_engine_contract_gap_validation_design.v1",
        payload,
        "validation_design",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        paths["unresolved_blocker_summary_json"],
        "growth_tilt_engine_unresolved_blocker_summary",
        "growth_tilt_engine_unresolved_blocker_summary.v1",
        payload,
        "unresolved_blocker_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["validation_design_markdown"]),
        _validation_design_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine contract gap remediation plan",
            "",
            "## 结论摘要",
            "",
            f"- status：`{payload.get('status')}`",
            f"- gap count：`{payload.get('gap_count')}`",
            f"- remediation item count：`{payload.get('remediation_item_count')}`",
            (
                "- unclassified remediation item count："
                f"`{payload.get('unclassified_remediation_item_count')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "2411 只把 2410 暴露出的 blocked/gap source features 转成 remediation "
            "plan、实施顺序和 validation design。它不执行 remediation、不修改 "
            "`growth_tilt_engine`、不生成 feature / signal / scoring / backtest / "
            "daily report，也不恢复 candidate search 或进入 observation / paper-shadow / "
            "production / broker。",
            "",
            "## Remediation Items",
            "",
            "```json",
            _json_block(payload.get("ordered_remediation_items", [])),
            "```",
            "",
            "## Validation Design",
            "",
            "```json",
            _json_block(payload.get("validation_design", {})),
            "```",
            "",
            "## Unresolved Blocker Summary",
            "",
            "```json",
            _json_block(payload.get("unresolved_blocker_summary", {})),
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


def _validation_design_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth tilt engine contract gap validation design",
            "",
            f"- status：`{payload.get('status')}`",
            f"- validation design ready：`{payload.get('validation_design_ready')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(payload.get("validation_design", {})),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2412 route",
            "",
            f"- 当前任务：`{TASK_REGISTER_ID}`",
            f"- 当前状态：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2412 应从具体 as-of semantics remediation 开始执行。2412 "
            "仍必须保持 candidate_search=false、observation=false、paper_shadow=false、"
            "production=false、broker=false，且不得解除或降级 blocker，除非后续 "
            "owner review 明确批准。",
        ]
    )


def _registry_has_2410_entry(report_registry: Mapping[str, Any]) -> bool:
    for entry in _as_list(report_registry.get("reports")):
        row = _as_mapping(entry)
        if row.get("report_id") == "growth_tilt_engine_source_feature_contract_mapping":
            return row.get("production_effect") == "none" and row.get("broker_action") == "none"
    return False


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
