from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_dry_run_wiring as m2425,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_enablement_plan as m2424,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_schedule_dry_run as m2426,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
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
    growth_tilt_engine_manual_review_packet_dry_run as packet_dry_run,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2427"
TASK_REGISTER_ID = "TRADING-2427_GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN"
REPORT_TYPE = packet_dry_run.REPORT_TYPE
SCHEMA_VERSION = packet_dry_run.SCHEMA_VERSION
READY_STATUS = packet_dry_run.READY_STATUS
BLOCKED_STATUS = packet_dry_run.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_MANUAL_REVIEW_PACKET_DRY_RUN_PRIOR_ARTIFACTS_REGISTRY_"
    "CATALOG_SYSTEM_FLOW_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "generate_trading_advice",
    "generate_actionable_allocation_change",
    "generate_broker_order",
    "modify_actual_portfolio_weights",
    "enable_paper_shadow",
    "enable_paper_shadow_schedule",
    "run_paper_shadow_daily_job",
    "create_scheduled_task",
    "mutate_scheduler",
    "create_paper_trade",
    "create_shadow_position",
    "append_historical_event_log",
    "bind_outcome",
    "mutate_outcome_store",
    "enable_production",
    "call_broker_api",
    "send_order",
    "generate_daily_report",
    "run_new_strategy_backtest",
    "generate_new_trading_signal",
    "run_scoring",
    "read_fresh_cached_market_data",
    "allow_automatic_execution",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_search_enabled",
    "candidate_search_allowed",
    "candidate_search_resumed",
    "observation_enabled",
    "research_only_observation_allowed",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_shadow_schedule_enabled",
    "paper_shadow_daily_job_enabled",
    "paper_shadow_daily_job_run",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "scheduled_task_created",
    "schedule_hook_invoked",
    "schedule_state_mutated",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_store_mutated",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "order_generated",
    "broker_order_generated",
    "daily_report_generated",
    "daily_report_run",
    "new_feature_generated",
    "new_signal_generated",
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "actionable_allocation_generated",
    "allocation_change_generated",
    "recommendation_generated",
    "new_strategy_backtest_run",
    "scoring_run",
    "fresh_market_data_read",
    "backtest_run",
    "portfolio_weight_mutated",
    "actual_portfolio_weights_modified",
    "automatic_execution_allowed",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH = (
    m2426.DEFAULT_OUTPUT_ROOT / "schedule_dry_run_result.json"
)
DEFAULT_SOURCE_2426_SCHEDULE_BOUNDARY_CHECKLIST_PATH = (
    m2426.DEFAULT_OUTPUT_ROOT / "schedule_boundary_checklist.json"
)
DEFAULT_SOURCE_2426_SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_PATH = (
    m2426.DEFAULT_OUTPUT_ROOT / "schedule_no_effect_audit_summary.json"
)
DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH = (
    m2426.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_schedule_dry_run.md"
)
DEFAULT_SOURCE_2426_BOUNDARY_DOC_PATH = (
    m2426.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_schedule_boundary_checklist.md"
)
DEFAULT_SOURCE_2426_NO_EFFECT_DOC_PATH = (
    m2426.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_schedule_no_effect_audit_summary.md"
)
DEFAULT_SOURCE_2426_ROUTE_DOC_PATH = (
    m2426.DEFAULT_DOCS_ROOT / "dynamic_strategy_2427_route.md"
)
DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "dry_run_wiring_result.json"
)
DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "manual_review_handoff_wiring_plan.json"
)
DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_dry_run_wiring.md"
)
DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_manual_review_handoff_wiring_plan.md"
)
DEFAULT_SOURCE_2425_ROUTE_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT / "dynamic_strategy_2426_route.md"
)
DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "enablement_plan_result.json"
)
DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_enablement_plan.md"
)
DEFAULT_SOURCE_2424_ROUTE_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT / "dynamic_strategy_2425_route.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_engine_manual_review_packet_dry_run(
    *,
    source_2426_schedule_dry_run_result_path: Path = (
        DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH
    ),
    source_2426_schedule_boundary_checklist_path: Path = (
        DEFAULT_SOURCE_2426_SCHEDULE_BOUNDARY_CHECKLIST_PATH
    ),
    source_2426_schedule_no_effect_audit_summary_path: Path = (
        DEFAULT_SOURCE_2426_SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_PATH
    ),
    source_2426_research_doc_path: Path = DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH,
    source_2426_boundary_doc_path: Path = DEFAULT_SOURCE_2426_BOUNDARY_DOC_PATH,
    source_2426_no_effect_doc_path: Path = DEFAULT_SOURCE_2426_NO_EFFECT_DOC_PATH,
    source_2426_route_doc_path: Path = DEFAULT_SOURCE_2426_ROUTE_DOC_PATH,
    source_2425_dry_run_wiring_result_path: Path = (
        DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH
    ),
    source_2425_manual_review_handoff_wiring_plan_path: Path = (
        DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH
    ),
    source_2425_research_doc_path: Path = DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH,
    source_2425_manual_review_doc_path: Path = DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH,
    source_2425_route_doc_path: Path = DEFAULT_SOURCE_2425_ROUTE_DOC_PATH,
    source_2424_enablement_plan_result_path: Path = (
        DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH
    ),
    source_2424_research_doc_path: Path = DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH,
    source_2424_route_doc_path: Path = DEFAULT_SOURCE_2424_ROUTE_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2426_schedule_dry_run_result": _load_json_document(
            source_2426_schedule_dry_run_result_path
        ),
        "source_2426_schedule_boundary_checklist": _load_json_document(
            source_2426_schedule_boundary_checklist_path
        ),
        "source_2426_schedule_no_effect_audit_summary": _load_json_document(
            source_2426_schedule_no_effect_audit_summary_path
        ),
        "source_2426_research_doc": _load_text_document(source_2426_research_doc_path),
        "source_2426_boundary_doc": _load_text_document(source_2426_boundary_doc_path),
        "source_2426_no_effect_doc": _load_text_document(
            source_2426_no_effect_doc_path
        ),
        "source_2426_route_doc": _load_text_document(source_2426_route_doc_path),
        "source_2425_dry_run_wiring_result": _load_json_document(
            source_2425_dry_run_wiring_result_path
        ),
        "source_2425_manual_review_handoff_wiring_plan": _load_json_document(
            source_2425_manual_review_handoff_wiring_plan_path
        ),
        "source_2425_research_doc": _load_text_document(source_2425_research_doc_path),
        "source_2425_manual_review_doc": _load_text_document(
            source_2425_manual_review_doc_path
        ),
        "source_2425_route_doc": _load_text_document(source_2425_route_doc_path),
        "source_2424_enablement_plan_result": _load_json_document(
            source_2424_enablement_plan_result_path
        ),
        "source_2424_research_doc": _load_text_document(source_2424_research_doc_path),
        "source_2424_route_doc": _load_text_document(source_2424_route_doc_path),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    source_validation_errors = _source_validation_errors(sources)
    if source_validation_errors:
        payload = _blocked_payload(
            source_validation_errors=source_validation_errors,
            as_of_date=as_of_date,
        )
    else:
        research_doc_texts = {
            name: _as_mapping(document).get("text", "")
            for name, document in sources.items()
            if name.endswith("_doc")
        }
        payload = packet_dry_run.build_growth_tilt_engine_manual_review_packet_dry_run(
            _as_mapping(sources["source_2426_schedule_dry_run_result"]),
            _as_mapping(sources["source_2426_schedule_boundary_checklist"]),
            _as_mapping(sources["source_2426_schedule_no_effect_audit_summary"]),
            _as_mapping(sources["source_2425_dry_run_wiring_result"]),
            _as_mapping(sources["source_2425_manual_review_handoff_wiring_plan"]),
            _as_mapping(sources["source_2424_enablement_plan_result"]),
            report_registry=_as_mapping(sources["report_registry"]),
            artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                "text", ""
            ),
            system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
            research_doc_texts=research_doc_texts,
        )
        payload = _with_runtime_metadata(
            payload,
            source_validation_errors=source_validation_errors,
            as_of_date=as_of_date,
        )
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_yaml_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    return safe_load_yaml_path(path)


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for name, document in sources.items():
        if isinstance(document, Mapping) and document.get("_missing") is True:
            errors.append(f"{name} missing: {document.get('_path')}")
    return errors


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched.update(
        {
            "as_of": str(as_of_date) if as_of_date else enriched.get("as_of"),
            "generated_at": utc_now_iso(),
            "market_regime": AI_REGIME_SUMMARY["market_regime"],
            "market_regime_summary": dict(AI_REGIME_SUMMARY),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "data_quality_gate_executed": False,
            "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
            "manual_review_required": True,
            "manual_review_only": True,
            "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    for field in SAFETY_FALSE_FIELDS:
        enriched[field] = False
    return enriched


def _blocked_payload(
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
) -> dict[str, Any]:
    gaps = [
        {
            "requirement_id": "source_artifact_availability",
            "classification": "missing_manual_review_evidence",
            "gap": (
                "Required prior artifact, registry, catalog, system flow, or "
                "doc is missing."
            ),
            "evidence": {"source_validation_errors": list(source_validation_errors)},
            "production_effect": "none",
            "broker_action": "none",
        }
    ]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": BLOCKED_STATUS,
        "readiness_status": BLOCKED_STATUS,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": packet_dry_run.TARGET_STRATEGY_ID,
        "prior_route": packet_dry_run.PRIOR_ROUTE,
        "source_tasks": ["TRADING-2424", "TRADING-2425", "TRADING-2426"],
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_gap_count": 1,
        "paper_shadow_schedule_dry_run_status": "UNKNOWN",
        "paper_shadow_schedule_dry_run_ready": False,
        "schedule_dry_run_gap_count": 1,
        "paper_shadow_dry_run_wiring_status": "UNKNOWN",
        "paper_shadow_dry_run_wiring_ready": False,
        "enablement_plan_status": "UNKNOWN",
        "enablement_plan_ready": False,
        "manual_review_packet_dry_run_started": False,
        "manual_review_packet_dry_run_completed": False,
        "manual_review_packet_dry_run_ready": False,
        "manual_review_packet_ready": False,
        "manual_review_checklist_ready": False,
        "no_advice_boundary_ready": False,
        "reviewer_handoff_manifest_ready": False,
        "manual_review_packet_gap_count": len(gaps),
        "manual_review_packet_gap_ids": ["source_artifact_availability"],
        "missing_manual_review_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "packet_contract_gap_count": 0,
        "precondition_gap_count": 0,
        "manual_review_packet": {
            "schema_version": packet_dry_run.MANUAL_REVIEW_PACKET_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "manual_review_packet_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "manual_review_checklist": {
            "schema_version": packet_dry_run.MANUAL_REVIEW_CHECKLIST_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "manual_review_checklist_ready": False,
            "checks": [],
            "production_effect": "none",
            "broker_action": "none",
        },
        "no_advice_boundary_summary": {
            "schema_version": packet_dry_run.NO_ADVICE_BOUNDARY_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "manual_review_packet_gap_count": len(gaps),
            "gaps": gaps,
            "no_advice_boundary_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "reviewer_handoff_manifest": {
            "schema_version": packet_dry_run.REVIEWER_HANDOFF_MANIFEST_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "reviewer_handoff_manifest_ready": False,
            "manual_review_required": True,
            "automatic_execution_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "manual_review_required": True,
        "manual_review_only": True,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_run": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "new_signal_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "recommended_next_research_task": packet_dry_run.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; manual review "
            "packet dry-run cannot silently pass."
        ),
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "manual_review_packet_dry_run_result.json"
    packet_json_path = output_root / "manual_review_packet.json"
    checklist_json_path = output_root / "manual_review_checklist.json"
    no_advice_json_path = output_root / "no_advice_boundary_summary.json"
    handoff_json_path = output_root / "reviewer_handoff_manifest.json"
    markdown_path = docs_root / "growth_tilt_engine_manual_review_packet_dry_run.md"
    packet_markdown_path = docs_root / "growth_tilt_engine_manual_review_packet.md"
    checklist_markdown_path = (
        docs_root / "growth_tilt_engine_manual_review_packet_checklist.md"
    )
    no_advice_markdown_path = (
        docs_root
        / "growth_tilt_engine_manual_review_packet_no_advice_boundary_summary.md"
    )
    handoff_markdown_path = (
        docs_root / "growth_tilt_engine_manual_review_packet_reviewer_handoff_manifest.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2428_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "manual_review_packet_json": str(packet_json_path),
        "manual_review_checklist_json": str(checklist_json_path),
        "no_advice_boundary_summary_json": str(no_advice_json_path),
        "reviewer_handoff_manifest_json": str(handoff_json_path),
        "markdown_path": str(markdown_path),
        "manual_review_packet_markdown": str(packet_markdown_path),
        "manual_review_checklist_markdown": str(checklist_markdown_path),
        "no_advice_boundary_summary_markdown": str(no_advice_markdown_path),
        "reviewer_handoff_manifest_markdown": str(handoff_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        packet_json_path,
        "growth_tilt_engine_manual_review_packet",
        packet_dry_run.MANUAL_REVIEW_PACKET_SCHEMA_VERSION,
        payload,
        "manual_review_packet",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        checklist_json_path,
        "growth_tilt_engine_manual_review_packet_checklist",
        packet_dry_run.MANUAL_REVIEW_CHECKLIST_SCHEMA_VERSION,
        payload,
        "manual_review_checklist",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_advice_json_path,
        "growth_tilt_engine_manual_review_packet_no_advice_boundary_summary",
        packet_dry_run.NO_ADVICE_BOUNDARY_SUMMARY_SCHEMA_VERSION,
        payload,
        "no_advice_boundary_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        handoff_json_path,
        "growth_tilt_engine_manual_review_packet_reviewer_handoff_manifest",
        packet_dry_run.REVIEWER_HANDOFF_MANIFEST_SCHEMA_VERSION,
        payload,
        "reviewer_handoff_manifest",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        packet_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Manual Review Packet",
            payload.get("manual_review_packet"),
        ),
    )
    write_markdown_artifact(
        checklist_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Manual Review Packet Checklist",
            payload.get("manual_review_checklist"),
        ),
    )
    write_markdown_artifact(
        no_advice_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Manual Review Packet No-Advice Boundary Summary",
            payload.get("no_advice_boundary_summary"),
        ),
    )
    write_markdown_artifact(
        handoff_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Manual Review Packet Reviewer Handoff Manifest",
            payload.get("reviewer_handoff_manifest"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "paper_shadow_schedule_dry_run_ready": payload.get(
            "paper_shadow_schedule_dry_run_ready"
        ),
        "paper_shadow_dry_run_wiring_ready": payload.get(
            "paper_shadow_dry_run_wiring_ready"
        ),
        "enablement_plan_ready": payload.get("enablement_plan_ready"),
        "manual_review_packet_dry_run_ready": payload.get(
            "manual_review_packet_dry_run_ready"
        ),
        "manual_review_packet_gap_count": payload.get(
            "manual_review_packet_gap_count"
        ),
        "manual_review_packet_ready": payload.get("manual_review_packet_ready"),
        "manual_review_checklist_ready": payload.get("manual_review_checklist_ready"),
        "no_advice_boundary_ready": payload.get("no_advice_boundary_ready"),
        "reviewer_handoff_manifest_ready": payload.get(
            "reviewer_handoff_manifest_ready"
        ),
        "manual_review_required": payload.get("manual_review_required"),
        "trading_advice_generated": payload.get("trading_advice_generated"),
        "actionable_allocation_generated": payload.get(
            "actionable_allocation_generated"
        ),
        "paper_shadow_enabled": payload.get("paper_shadow_enabled"),
        "paper_shadow_schedule_enabled": payload.get("paper_shadow_schedule_enabled"),
        "production_enabled": payload.get("production_enabled"),
        "broker_enabled": payload.get("broker_enabled"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Manual Review Packet Dry-Run",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            "- manual review packet dry-run ready："
            f"`{payload.get('manual_review_packet_dry_run_ready')}`",
            f"- manual review packet gap count：`{payload.get('manual_review_packet_gap_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2427 只生成 manual review packet dry-run 证据。READY 不等于 "
            "trading advice 或 allocation approval；本任务不启用 paper-shadow / schedule，"
            "不生成 actionable allocation，不进入 production 或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Manual Review Packet",
            "",
            "```json",
            _json_block(payload.get("manual_review_packet")),
            "```",
            "",
            "## No-Advice Boundary",
            "",
            "```json",
            _json_block(payload.get("no_advice_boundary_summary")),
            "```",
        ]
    )


def _render_section_markdown(title: str, section: Any) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            "```json",
            _json_block(section),
            "```",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2428 Route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2428 must define an observe-only signal artifact boundary. "
            "TRADING-2427 does not generate trading advice, actionable allocation, "
            "paper-shadow activation, production action, or broker order.",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
