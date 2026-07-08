from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_manual_review_packet_dry_run as m2427,
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
    growth_tilt_engine_observe_only_signal_artifact_boundary as boundary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2428"
TASK_REGISTER_ID = (
    "TRADING-2428_GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY"
)
REPORT_TYPE = boundary.REPORT_TYPE
SCHEMA_VERSION = boundary.SCHEMA_VERSION
READY_STATUS = boundary.READY_STATUS
BLOCKED_STATUS = boundary.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_PRIOR_ARTIFACTS_"
    "REGISTRY_CATALOG_SYSTEM_FLOW_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "generate_real_signal",
    "generate_trading_advice",
    "generate_actionable_allocation_change",
    "generate_broker_order",
    "modify_actual_portfolio_weights",
    "read_fresh_cached_market_data",
    "run_backtest",
    "run_scoring",
    "generate_daily_report",
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
    "allow_automatic_execution",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_search_enabled",
    "candidate_search_allowed",
    "candidate_search_resumed",
    "observation_enabled",
    "research_only_observation_allowed",
    "research_only_observation_approved",
    "signal_artifact_instance_generated",
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

DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH = (
    m2427.DEFAULT_OUTPUT_ROOT / "manual_review_packet_dry_run_result.json"
)
DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_PATH = (
    m2427.DEFAULT_OUTPUT_ROOT / "manual_review_packet.json"
)
DEFAULT_SOURCE_2427_MANUAL_REVIEW_CHECKLIST_PATH = (
    m2427.DEFAULT_OUTPUT_ROOT / "manual_review_checklist.json"
)
DEFAULT_SOURCE_2427_NO_ADVICE_BOUNDARY_SUMMARY_PATH = (
    m2427.DEFAULT_OUTPUT_ROOT / "no_advice_boundary_summary.json"
)
DEFAULT_SOURCE_2427_REVIEWER_HANDOFF_MANIFEST_PATH = (
    m2427.DEFAULT_OUTPUT_ROOT / "reviewer_handoff_manifest.json"
)
DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT / "growth_tilt_engine_manual_review_packet_dry_run.md"
)
DEFAULT_SOURCE_2427_PACKET_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT / "growth_tilt_engine_manual_review_packet.md"
)
DEFAULT_SOURCE_2427_CHECKLIST_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT / "growth_tilt_engine_manual_review_packet_checklist.md"
)
DEFAULT_SOURCE_2427_NO_ADVICE_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_manual_review_packet_no_advice_boundary_summary.md"
)
DEFAULT_SOURCE_2427_HANDOFF_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_manual_review_packet_reviewer_handoff_manifest.md"
)
DEFAULT_SOURCE_2427_ROUTE_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT / "dynamic_strategy_2428_route.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_engine_observe_only_signal_artifact_boundary(
    *,
    source_2427_manual_review_packet_dry_run_result_path: Path = (
        DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH
    ),
    source_2427_manual_review_packet_path: Path = (
        DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_PATH
    ),
    source_2427_manual_review_checklist_path: Path = (
        DEFAULT_SOURCE_2427_MANUAL_REVIEW_CHECKLIST_PATH
    ),
    source_2427_no_advice_boundary_summary_path: Path = (
        DEFAULT_SOURCE_2427_NO_ADVICE_BOUNDARY_SUMMARY_PATH
    ),
    source_2427_reviewer_handoff_manifest_path: Path = (
        DEFAULT_SOURCE_2427_REVIEWER_HANDOFF_MANIFEST_PATH
    ),
    source_2427_research_doc_path: Path = DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH,
    source_2427_packet_doc_path: Path = DEFAULT_SOURCE_2427_PACKET_DOC_PATH,
    source_2427_checklist_doc_path: Path = DEFAULT_SOURCE_2427_CHECKLIST_DOC_PATH,
    source_2427_no_advice_doc_path: Path = DEFAULT_SOURCE_2427_NO_ADVICE_DOC_PATH,
    source_2427_handoff_doc_path: Path = DEFAULT_SOURCE_2427_HANDOFF_DOC_PATH,
    source_2427_route_doc_path: Path = DEFAULT_SOURCE_2427_ROUTE_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2427_manual_review_packet_dry_run_result": _load_json_document(
            source_2427_manual_review_packet_dry_run_result_path
        ),
        "source_2427_manual_review_packet": _load_json_document(
            source_2427_manual_review_packet_path
        ),
        "source_2427_manual_review_checklist": _load_json_document(
            source_2427_manual_review_checklist_path
        ),
        "source_2427_no_advice_boundary_summary": _load_json_document(
            source_2427_no_advice_boundary_summary_path
        ),
        "source_2427_reviewer_handoff_manifest": _load_json_document(
            source_2427_reviewer_handoff_manifest_path
        ),
        "source_2427_research_doc": _load_text_document(source_2427_research_doc_path),
        "source_2427_packet_doc": _load_text_document(source_2427_packet_doc_path),
        "source_2427_checklist_doc": _load_text_document(
            source_2427_checklist_doc_path
        ),
        "source_2427_no_advice_doc": _load_text_document(
            source_2427_no_advice_doc_path
        ),
        "source_2427_handoff_doc": _load_text_document(source_2427_handoff_doc_path),
        "source_2427_route_doc": _load_text_document(source_2427_route_doc_path),
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
        payload = (
            boundary.build_growth_tilt_engine_observe_only_signal_artifact_boundary(
                _as_mapping(
                    sources["source_2427_manual_review_packet_dry_run_result"]
                ),
                _as_mapping(sources["source_2427_manual_review_packet"]),
                _as_mapping(sources["source_2427_manual_review_checklist"]),
                _as_mapping(sources["source_2427_no_advice_boundary_summary"]),
                _as_mapping(sources["source_2427_reviewer_handoff_manifest"]),
                report_registry=_as_mapping(sources["report_registry"]),
                artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                    "text", ""
                ),
                system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
                research_doc_texts=research_doc_texts,
            )
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
            "observe_only": True,
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
            "classification": "missing_observe_only_boundary_evidence",
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
        "target_strategy_id": boundary.TARGET_STRATEGY_ID,
        "prior_route": boundary.PRIOR_ROUTE,
        "source_tasks": ["TRADING-2427"],
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_gap_count": 1,
        "manual_review_packet_dry_run_status": "UNKNOWN",
        "manual_review_packet_dry_run_ready": False,
        "manual_review_packet_gap_count": 1,
        "manual_review_packet_ready": False,
        "manual_review_checklist_ready": False,
        "prior_no_advice_boundary_ready": False,
        "reviewer_handoff_manifest_ready": False,
        "observe_only_signal_artifact_boundary_started": False,
        "observe_only_signal_artifact_boundary_completed": False,
        "observe_only_signal_artifact_boundary_ready": False,
        "signal_artifact_schema_ready": False,
        "valid_until_required": False,
        "valid_until_requirements_ready": False,
        "source_traceability_required": False,
        "source_traceability_requirements_ready": False,
        "pit_contract_manual_review_requirements_ready": False,
        "no_trading_advice_boundary_ready": False,
        "observe_only_signal_artifact_boundary_gap_count": len(gaps),
        "observe_only_signal_artifact_boundary_gap_ids": [
            "source_artifact_availability"
        ],
        "missing_observe_only_boundary_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "signal_artifact_contract_gap_count": 0,
        "precondition_gap_count": 0,
        "requirements": [],
        "gaps": gaps,
        "signal_artifact_schema": {
            "schema_version": boundary.SIGNAL_ARTIFACT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "signal_artifact_schema_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "valid_until_requirements": {
            "schema_version": boundary.VALID_UNTIL_REQUIREMENTS_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "valid_until_requirements_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "source_traceability_requirements": {
            "schema_version": (
                boundary.SOURCE_TRACEABILITY_REQUIREMENTS_SCHEMA_VERSION
            ),
            "status": BLOCKED_STATUS,
            "source_traceability_requirements_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "pit_contract_manual_review_requirements": {
            "schema_version": (
                boundary.PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_SCHEMA_VERSION
            ),
            "status": BLOCKED_STATUS,
            "pit_contract_manual_review_requirements_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "no_trading_advice_boundary": {
            "schema_version": boundary.NO_TRADING_ADVICE_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "observe_only_signal_artifact_boundary_gap_count": len(gaps),
            "gaps": gaps,
            "no_trading_advice_boundary_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "observe_only": True,
        "manual_review_required": True,
        "manual_review_only": True,
        "signal_artifact_instance_generated": False,
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
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "recommended_next_research_task": boundary.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; observe-only "
            "signal artifact boundary cannot silently pass."
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
    json_path = output_root / "observe_only_signal_artifact_boundary_result.json"
    schema_json_path = output_root / "signal_artifact_schema.json"
    valid_until_json_path = output_root / "valid_until_requirements.json"
    traceability_json_path = output_root / "source_traceability_requirements.json"
    pit_contract_json_path = output_root / "pit_contract_manual_review_requirements.json"
    no_advice_json_path = output_root / "no_trading_advice_boundary.json"
    markdown_path = docs_root / "growth_tilt_engine_observe_only_signal_artifact_boundary.md"
    schema_markdown_path = (
        docs_root / "growth_tilt_engine_observe_only_signal_artifact_schema.md"
    )
    valid_until_markdown_path = (
        docs_root / "growth_tilt_engine_observe_only_signal_valid_until_requirements.md"
    )
    traceability_markdown_path = (
        docs_root
        / "growth_tilt_engine_observe_only_signal_source_traceability_requirements.md"
    )
    pit_contract_markdown_path = (
        docs_root
        / "growth_tilt_engine_observe_only_signal_pit_contract_manual_review_requirements.md"
    )
    no_advice_markdown_path = (
        docs_root / "growth_tilt_engine_observe_only_signal_no_trading_advice_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2429_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "signal_artifact_schema_json": str(schema_json_path),
        "valid_until_requirements_json": str(valid_until_json_path),
        "source_traceability_requirements_json": str(traceability_json_path),
        "pit_contract_manual_review_requirements_json": str(pit_contract_json_path),
        "no_trading_advice_boundary_json": str(no_advice_json_path),
        "markdown_path": str(markdown_path),
        "signal_artifact_schema_markdown": str(schema_markdown_path),
        "valid_until_requirements_markdown": str(valid_until_markdown_path),
        "source_traceability_requirements_markdown": str(traceability_markdown_path),
        "pit_contract_manual_review_requirements_markdown": str(
            pit_contract_markdown_path
        ),
        "no_trading_advice_boundary_markdown": str(no_advice_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        schema_json_path,
        "growth_tilt_engine_observe_only_signal_artifact_schema",
        boundary.SIGNAL_ARTIFACT_SCHEMA_VERSION,
        payload,
        "signal_artifact_schema",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        valid_until_json_path,
        "growth_tilt_engine_observe_only_signal_valid_until_requirements",
        boundary.VALID_UNTIL_REQUIREMENTS_SCHEMA_VERSION,
        payload,
        "valid_until_requirements",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        traceability_json_path,
        "growth_tilt_engine_observe_only_signal_source_traceability_requirements",
        boundary.SOURCE_TRACEABILITY_REQUIREMENTS_SCHEMA_VERSION,
        payload,
        "source_traceability_requirements",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        pit_contract_json_path,
        "growth_tilt_engine_observe_only_signal_pit_contract_manual_review_requirements",
        boundary.PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_SCHEMA_VERSION,
        payload,
        "pit_contract_manual_review_requirements",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_advice_json_path,
        "growth_tilt_engine_observe_only_signal_no_trading_advice_boundary",
        boundary.NO_TRADING_ADVICE_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_trading_advice_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        schema_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Observe-Only Signal Artifact Schema",
            payload.get("signal_artifact_schema"),
        ),
    )
    write_markdown_artifact(
        valid_until_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Observe-Only Signal Valid-Until Requirements",
            payload.get("valid_until_requirements"),
        ),
    )
    write_markdown_artifact(
        traceability_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Observe-Only Signal Source Traceability Requirements",
            payload.get("source_traceability_requirements"),
        ),
    )
    write_markdown_artifact(
        pit_contract_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Observe-Only Signal PIT Contract Manual Review Requirements",
            payload.get("pit_contract_manual_review_requirements"),
        ),
    )
    write_markdown_artifact(
        no_advice_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Observe-Only Signal No-Trading-Advice Boundary",
            payload.get("no_trading_advice_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "manual_review_packet_dry_run_ready": payload.get(
            "manual_review_packet_dry_run_ready"
        ),
        "observe_only_signal_artifact_boundary_ready": payload.get(
            "observe_only_signal_artifact_boundary_ready"
        ),
        "observe_only_signal_artifact_boundary_gap_count": payload.get(
            "observe_only_signal_artifact_boundary_gap_count"
        ),
        "signal_artifact_schema_ready": payload.get("signal_artifact_schema_ready"),
        "valid_until_required": payload.get("valid_until_required"),
        "source_traceability_required": payload.get("source_traceability_required"),
        "manual_review_required": payload.get("manual_review_required"),
        "generated_signal": payload.get("generated_signal"),
        "generated_trading_advice": payload.get("generated_trading_advice"),
        "paper_shadow_enabled": payload.get("paper_shadow_enabled"),
        "production_enabled": payload.get("production_enabled"),
        "broker_enabled": payload.get("broker_enabled"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Observe-Only Signal Artifact Boundary",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            "- observe-only signal artifact boundary ready："
            f"`{payload.get('observe_only_signal_artifact_boundary_ready')}`",
            "- boundary gap count："
            f"`{payload.get('observe_only_signal_artifact_boundary_gap_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2428 只定义 observe-only signal artifact boundary。READY 不等于"
            "真实 signal、trading advice、allocation approval、paper-shadow activation、"
            "production action 或 broker order。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Signal Artifact Schema",
            "",
            "```json",
            _json_block(payload.get("signal_artifact_schema")),
            "```",
            "",
            "## No-Trading-Advice Boundary",
            "",
            "```json",
            _json_block(payload.get("no_trading_advice_boundary")),
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
            "# Dynamic Strategy TRADING-2429 Route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2429 must define forward outcome binding for future "
            "observe-only signal artifacts. TRADING-2428 does not generate a "
            "real signal, trading advice, actionable allocation, paper-shadow "
            "activation, production action, or broker order.",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
