from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_readiness_snapshot as m2422,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation as m2421,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_artifact_source_traceability_remediation as m2420,  # noqa: E501
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
    growth_tilt_engine_paper_shadow_preflight as preflight,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2423"
TASK_REGISTER_ID = "TRADING-2423_GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT"
REPORT_TYPE = preflight.REPORT_TYPE
SCHEMA_VERSION = preflight.SCHEMA_VERSION
READY_STATUS = preflight.READY_STATUS
BLOCKED_STATUS = preflight.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PAPER_SHADOW_PREFLIGHT_PRIOR_ARTIFACTS_REGISTRY_CATALOG_"
    "SYSTEM_FLOW_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "enable_paper_shadow",
    "run_paper_shadow_schedule",
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
    "generate_trading_advice",
    "run_scoring",
    "modify_actual_portfolio_weights",
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
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "new_strategy_backtest_run",
    "scoring_run",
    "fresh_market_data_read",
    "backtest_run",
    "actual_portfolio_weights_modified",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH = (
    m2422.DEFAULT_OUTPUT_ROOT / "contract_readiness_snapshot_result.json"
)
DEFAULT_SOURCE_2422_CONTRACT_EVIDENCE_MAP_PATH = (
    m2422.DEFAULT_OUTPUT_ROOT / "contract_evidence_map.json"
)
DEFAULT_SOURCE_2422_CONTRACT_GAP_SUMMARY_PATH = (
    m2422.DEFAULT_OUTPUT_ROOT / "contract_gap_summary.json"
)
DEFAULT_SOURCE_2422_CONTRACT_REQUIREMENTS_PATH = (
    m2422.DEFAULT_OUTPUT_ROOT / "contract_requirements.json"
)
DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH = (
    m2422.DEFAULT_DOCS_ROOT / "growth_tilt_engine_contract_readiness_snapshot.md"
)
DEFAULT_SOURCE_2422_EVIDENCE_MAP_DOC_PATH = (
    m2422.DEFAULT_DOCS_ROOT / "growth_tilt_engine_contract_evidence_map.md"
)
DEFAULT_SOURCE_2422_GAP_SUMMARY_DOC_PATH = (
    m2422.DEFAULT_DOCS_ROOT / "growth_tilt_engine_contract_gap_summary.md"
)
DEFAULT_SOURCE_2422_ROUTE_DOC_PATH = (
    m2422.DEFAULT_DOCS_ROOT / "dynamic_strategy_2423_route.md"
)

DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH = (
    m2421.DEFAULT_OUTPUT_ROOT / "readiness_recheck_after_remediation_result.json"
)
DEFAULT_SOURCE_2421_PIT_GATE_RECHECK_MATRIX_PATH = (
    m2421.DEFAULT_OUTPUT_ROOT / "pit_gate_recheck_after_remediation_matrix.json"
)
DEFAULT_SOURCE_2421_BLOCKER_RESOLUTION_SUMMARY_PATH = (
    m2421.DEFAULT_OUTPUT_ROOT / "blocker_resolution_summary.json"
)
DEFAULT_SOURCE_2421_CONTRACT_READINESS_SNAPSHOT_GATE_PATH = (
    m2421.DEFAULT_OUTPUT_ROOT / "contract_readiness_snapshot_gate.json"
)
DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH = (
    m2421.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation.md"
)
DEFAULT_SOURCE_2421_MATRIX_DOC_PATH = (
    m2421.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_pit_gate_recheck_after_source_traceability_remediation_matrix.md"
)
DEFAULT_SOURCE_2421_BLOCKER_DOC_PATH = (
    m2421.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_source_traceability_blocker_resolution_summary.md"
)
DEFAULT_SOURCE_2421_ROUTE_DOC_PATH = (
    m2421.DEFAULT_DOCS_ROOT / "dynamic_strategy_2422_route.md"
)

DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH = (
    m2420.DEFAULT_OUTPUT_ROOT / "remediation_result.json"
)
DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH = (
    m2420.DEFAULT_OUTPUT_ROOT / "source_traceability_manifest.json"
)
DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH = (
    m2420.DEFAULT_OUTPUT_ROOT / "source_lineage_map.json"
)
DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH = (
    m2420.DEFAULT_OUTPUT_ROOT / "missing_source_evidence_summary.json"
)
DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH = (
    m2420.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_signal_artifact_source_traceability_remediation.md"
)
DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH = (
    m2420.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_signal_artifact_source_traceability_manifest.md"
)
DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH = (
    m2420.DEFAULT_DOCS_ROOT / "growth_tilt_engine_signal_artifact_source_lineage_map.md"
)
DEFAULT_SOURCE_2420_ROUTE_DOC_PATH = (
    m2420.DEFAULT_DOCS_ROOT / "dynamic_strategy_2421_route.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_engine_paper_shadow_preflight(
    *,
    source_2422_contract_readiness_snapshot_path: Path = (
        DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH
    ),
    source_2422_contract_evidence_map_path: Path = (
        DEFAULT_SOURCE_2422_CONTRACT_EVIDENCE_MAP_PATH
    ),
    source_2422_contract_gap_summary_path: Path = (
        DEFAULT_SOURCE_2422_CONTRACT_GAP_SUMMARY_PATH
    ),
    source_2422_contract_requirements_path: Path = (
        DEFAULT_SOURCE_2422_CONTRACT_REQUIREMENTS_PATH
    ),
    source_2422_research_doc_path: Path = DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_evidence_map_doc_path: Path = (
        DEFAULT_SOURCE_2422_EVIDENCE_MAP_DOC_PATH
    ),
    source_2422_gap_summary_doc_path: Path = (
        DEFAULT_SOURCE_2422_GAP_SUMMARY_DOC_PATH
    ),
    source_2422_route_doc_path: Path = DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Path = (
        DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH
    ),
    source_2421_pit_gate_recheck_matrix_path: Path = (
        DEFAULT_SOURCE_2421_PIT_GATE_RECHECK_MATRIX_PATH
    ),
    source_2421_blocker_resolution_summary_path: Path = (
        DEFAULT_SOURCE_2421_BLOCKER_RESOLUTION_SUMMARY_PATH
    ),
    source_2421_contract_readiness_snapshot_gate_path: Path = (
        DEFAULT_SOURCE_2421_CONTRACT_READINESS_SNAPSHOT_GATE_PATH
    ),
    source_2421_research_doc_path: Path = DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_matrix_doc_path: Path = DEFAULT_SOURCE_2421_MATRIX_DOC_PATH,
    source_2421_blocker_doc_path: Path = DEFAULT_SOURCE_2421_BLOCKER_DOC_PATH,
    source_2421_route_doc_path: Path = DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Path = (
        DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH
    ),
    source_2420_source_traceability_manifest_path: Path = (
        DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH
    ),
    source_2420_source_lineage_map_path: Path = (
        DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH
    ),
    source_2420_missing_source_evidence_summary_path: Path = (
        DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH
    ),
    source_2420_research_doc_path: Path = DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Path = DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Path = DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Path = DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2422_contract_readiness_snapshot": _load_json_document(
            source_2422_contract_readiness_snapshot_path
        ),
        "source_2422_contract_evidence_map": _load_json_document(
            source_2422_contract_evidence_map_path
        ),
        "source_2422_contract_gap_summary": _load_json_document(
            source_2422_contract_gap_summary_path
        ),
        "source_2422_contract_requirements": _load_json_document(
            source_2422_contract_requirements_path
        ),
        "source_2422_research_doc": _load_text_document(source_2422_research_doc_path),
        "source_2422_evidence_map_doc": _load_text_document(
            source_2422_evidence_map_doc_path
        ),
        "source_2422_gap_summary_doc": _load_text_document(
            source_2422_gap_summary_doc_path
        ),
        "source_2422_route_doc": _load_text_document(source_2422_route_doc_path),
        "source_2421_readiness_recheck_result": _load_json_document(
            source_2421_readiness_recheck_result_path
        ),
        "source_2421_pit_gate_recheck_matrix": _load_json_document(
            source_2421_pit_gate_recheck_matrix_path
        ),
        "source_2421_blocker_resolution_summary": _load_json_document(
            source_2421_blocker_resolution_summary_path
        ),
        "source_2421_contract_readiness_snapshot_gate": _load_json_document(
            source_2421_contract_readiness_snapshot_gate_path
        ),
        "source_2421_research_doc": _load_text_document(source_2421_research_doc_path),
        "source_2421_matrix_doc": _load_text_document(source_2421_matrix_doc_path),
        "source_2421_blocker_doc": _load_text_document(source_2421_blocker_doc_path),
        "source_2421_route_doc": _load_text_document(source_2421_route_doc_path),
        "source_2420_remediation_result": _load_json_document(
            source_2420_remediation_result_path
        ),
        "source_2420_source_traceability_manifest": _load_json_document(
            source_2420_source_traceability_manifest_path
        ),
        "source_2420_source_lineage_map": _load_json_document(
            source_2420_source_lineage_map_path
        ),
        "source_2420_missing_source_evidence_summary": _load_json_document(
            source_2420_missing_source_evidence_summary_path
        ),
        "source_2420_research_doc": _load_text_document(source_2420_research_doc_path),
        "source_2420_manifest_doc": _load_text_document(source_2420_manifest_doc_path),
        "source_2420_lineage_doc": _load_text_document(source_2420_lineage_doc_path),
        "source_2420_route_doc": _load_text_document(source_2420_route_doc_path),
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
        payload = preflight.build_growth_tilt_engine_paper_shadow_preflight(
            _as_mapping(sources["source_2422_contract_readiness_snapshot"]),
            _as_mapping(sources["source_2421_readiness_recheck_result"]),
            _as_mapping(sources["source_2420_remediation_result"]),
            _as_mapping(sources["source_2420_source_traceability_manifest"]),
            _as_mapping(sources["source_2420_source_lineage_map"]),
            _as_mapping(sources["source_2420_missing_source_evidence_summary"]),
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
            "classification": "missing_preflight_evidence",
            "gap": "Required prior artifact, registry, catalog, system flow, or doc is missing.",
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
        "target_strategy_id": preflight.TARGET_STRATEGY_ID,
        "prior_route": preflight.PRIOR_ROUTE,
        "source_tasks": ["TRADING-2420", "TRADING-2421", "TRADING-2422"],
        "artifact_id": preflight.ARTIFACT_ID,
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "pit_gate_blocked_count": 1,
        "contract_readiness_status": "UNKNOWN",
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_gap_count": 1,
        "remaining_pit_blockers": [preflight.ARTIFACT_ID],
        "remaining_pit_blocker_count": 1,
        "source_traceability_remediation_status": "UNKNOWN",
        "source_traceability_recheck_status": "UNKNOWN",
        "source_traceability_accepted": False,
        "paper_shadow_preflight_started": False,
        "paper_shadow_preflight_completed": False,
        "paper_shadow_preflight_ready": False,
        "paper_shadow_enablement_plan_required": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "generated_signal": False,
        "generated_trading_advice": False,
        "new_signal_generated": False,
        "daily_report_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "preflight_requirement_count": 1,
        "preflight_requirement_pass_count": 0,
        "preflight_requirement_fail_count": 1,
        "preflight_gap_count": len(gaps),
        "preflight_gap_ids": ["source_artifact_availability"],
        "missing_preflight_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "preflight_checklist": {
            "schema_version": preflight.PREFLIGHT_CHECKLIST_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "engine_id": "growth_tilt_engine",
            "target_strategy_id": preflight.TARGET_STRATEGY_ID,
            "check_count": 1,
            "passed_check_count": 0,
            "failed_check_count": 1,
            "checks": [],
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "preflight_gap_summary": {
            "schema_version": preflight.PREFLIGHT_GAP_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "engine_id": "growth_tilt_engine",
            "target_strategy_id": preflight.TARGET_STRATEGY_ID,
            "preflight_gap_count": len(gaps),
            "missing_preflight_evidence_count": len(gaps),
            "safety_boundary_gap_count": 0,
            "gaps": gaps,
            "next_route": preflight.NEXT_ROUTE_BLOCKED,
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "recommended_next_research_task": preflight.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; paper-shadow "
            "preflight cannot silently pass."
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
    json_path = output_root / "paper_shadow_preflight_result.json"
    checklist_json_path = output_root / "preflight_checklist.json"
    gap_summary_json_path = output_root / "preflight_gap_summary.json"
    markdown_path = docs_root / "growth_tilt_engine_paper_shadow_preflight.md"
    checklist_markdown_path = (
        docs_root / "growth_tilt_engine_paper_shadow_preflight_checklist.md"
    )
    gap_summary_markdown_path = (
        docs_root / "growth_tilt_engine_paper_shadow_preflight_gap_summary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2424_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "preflight_checklist_json": str(checklist_json_path),
        "preflight_gap_summary_json": str(gap_summary_json_path),
        "markdown_path": str(markdown_path),
        "preflight_checklist_markdown": str(checklist_markdown_path),
        "preflight_gap_summary_markdown": str(gap_summary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        checklist_json_path,
        "growth_tilt_engine_paper_shadow_preflight_checklist",
        preflight.PREFLIGHT_CHECKLIST_SCHEMA_VERSION,
        payload,
        "preflight_checklist",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        gap_summary_json_path,
        "growth_tilt_engine_paper_shadow_preflight_gap_summary",
        preflight.PREFLIGHT_GAP_SUMMARY_SCHEMA_VERSION,
        payload,
        "preflight_gap_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        checklist_markdown_path,
        _render_checklist_markdown(payload),
    )
    write_markdown_artifact(
        gap_summary_markdown_path,
        _render_gap_summary_markdown(payload),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "pit_gate_ready": payload.get("pit_gate_ready"),
        "contract_ready": payload.get("contract_ready"),
        "contract_gap_count": payload.get("contract_gap_count"),
        "preflight_gap_count": payload.get("preflight_gap_count"),
        "paper_shadow_preflight_started": payload.get(
            "paper_shadow_preflight_started"
        ),
        "paper_shadow_preflight_ready": payload.get("paper_shadow_preflight_ready"),
        "paper_shadow_enabled": payload.get("paper_shadow_enabled"),
        "production_enabled": payload.get("production_enabled"),
        "broker_enabled": payload.get("broker_enabled"),
        "generated_signal": payload.get("generated_signal"),
        "generated_trading_advice": payload.get("generated_trading_advice"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Paper Shadow Preflight",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- PIT gate ready：`{payload.get('pit_gate_ready')}`",
            f"- contract ready：`{payload.get('contract_ready')}`",
            f"- paper-shadow preflight ready：`{payload.get('paper_shadow_preflight_ready')}`",
            f"- preflight gap count：`{payload.get('preflight_gap_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2423 只执行 paper-shadow 启动前 preflight 检查。preflight READY "
            "不等于 paper-shadow enabled；本任务不生成新 signal、不运行 "
            "backtest/scoring/daily report、不启用 production 或 broker/order。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Preflight Gap Summary",
            "",
            "```json",
            _json_block(payload.get("preflight_gap_summary")),
            "```",
        ]
    )


def _render_checklist_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine Paper Shadow Preflight Checklist",
            "",
            "```json",
            _json_block(payload.get("preflight_checklist")),
            "```",
        ]
    )


def _render_gap_summary_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine Paper Shadow Preflight Gap Summary",
            "",
            "```json",
            _json_block(payload.get("preflight_gap_summary")),
            "```",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2424 Route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "若 paper-shadow preflight READY，TRADING-2424 应处理 paper-shadow "
            "enablement plan；2423 不授权 paper-shadow runtime、production 或 broker/order。",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
