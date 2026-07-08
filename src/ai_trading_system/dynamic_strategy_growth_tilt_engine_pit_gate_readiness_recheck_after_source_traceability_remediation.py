from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck as m2419,
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
    growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation as recheck_after,  # noqa: E501
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2421"
TASK_REGISTER_ID = (
    "TRADING-2421_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_"
    "TRACEABILITY_REMEDIATION"
)
REPORT_TYPE = recheck_after.REPORT_TYPE
SCHEMA_VERSION = recheck_after.SCHEMA_VERSION
READY_STATUS = recheck_after.READY_STATUS
BLOCKED_STATUS = recheck_after.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_"
    "REMEDIATION_PRIOR_ARTIFACTS_REGISTRY_CATALOG_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "mark_contract_ready",
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
    "contract_ready",
    "auto_mark_pit_gate_ready",
    "auto_mark_contract_ready",
    "signal_artifact_source_traceability_blocker_downgraded",
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
    "fresh_market_data_read",
    "backtest_run",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

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
DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT
    / "readiness_recheck_result.json"
)
DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT
    / "growth_tilt_engine_pit_gate_readiness_recheck.md"
)
DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT
    / "growth_tilt_engine_signal_artifact_source_traceability_blocker.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation(
    *,
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
    source_2419_recheck_result_path: Path = DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH,
    source_2419_research_doc_path: Path = DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH,
    source_2419_blocker_doc_path: Path = DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
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
        "source_2419_recheck_result": _load_json_document(
            source_2419_recheck_result_path
        ),
        "source_2419_research_doc": _load_text_document(source_2419_research_doc_path),
        "source_2419_blocker_doc": _load_text_document(source_2419_blocker_doc_path),
        "report_registry": safe_load_yaml_path(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
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
            recheck_after.build_growth_tilt_pit_gate_readiness_recheck_after_source_traceability_remediation(
                _as_mapping(sources["source_2420_remediation_result"]),
                _as_mapping(sources["source_2420_source_traceability_manifest"]),
                _as_mapping(sources["source_2420_source_lineage_map"]),
                _as_mapping(sources["source_2420_missing_source_evidence_summary"]),
                _as_mapping(sources["source_2419_recheck_result"]),
                report_registry=_as_mapping(sources["report_registry"]),
                artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                    "text", ""
                ),
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
            "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_blocked": True,
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
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": BLOCKED_STATUS,
        "readiness_status": BLOCKED_STATUS,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": recheck_after.TARGET_STRATEGY_ID,
        "artifact_id": recheck_after.ARTIFACT_ID,
        "source_traceability_remediation_status": "UNKNOWN",
        "source_traceability_recheck_status": "REJECTED",
        "source_traceability_evidence_complete_after_2420": False,
        "source_traceability_blocker_resolved": False,
        "signal_artifact_source_traceability_blocker_resolved": False,
        "signal_artifact_source_traceability_blocker_downgraded": False,
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "resolved_blockers": [],
        "remaining_blockers": [recheck_after.ARTIFACT_ID],
        "remaining_blocker_count": 1,
        "blocker_classification": {recheck_after.ARTIFACT_ID: "source_traceability"},
        "blocker_resolution_error_count": len(source_validation_errors),
        "blocker_resolution_errors": list(source_validation_errors),
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "pit_gate_blocked_count": 1,
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_readiness_snapshot_required": True,
        "pit_gate_recheck_completed": False,
        "pit_gate_recheck_after_remediation_matrix": {
            "schema_version": (
                recheck_after.PIT_GATE_RECHECK_AFTER_REMEDIATION_MATRIX_SCHEMA_VERSION
            ),
            "engine_id": "growth_tilt_engine",
            "row_count": 0,
            "matrix_rows": [],
            "production_effect": "none",
            "broker_action": "none",
        },
        "blocker_resolution_summary": {
            "schema_version": recheck_after.BLOCKER_RESOLUTION_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "artifact_id": recheck_after.ARTIFACT_ID,
            "source_traceability_recheck_status": "REJECTED",
            "resolved_blockers": [],
            "remaining_blockers": [recheck_after.ARTIFACT_ID],
            "blocker_resolution_error_count": len(source_validation_errors),
            "blocker_resolution_errors": list(source_validation_errors),
            "production_effect": "none",
            "broker_action": "none",
        },
        "contract_readiness_snapshot_gate": {
            "schema_version": recheck_after.CONTRACT_READINESS_SNAPSHOT_GATE_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "pit_gate_ready": False,
            "contract_ready": False,
            "contract_ready_count": 0,
            "contract_readiness_snapshot_required": True,
            "next_route": recheck_after.NEXT_ROUTE_BLOCKED,
            "production_effect": "none",
            "broker_action": "none",
        },
        "recommended_next_research_task": recheck_after.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required 2419/2420 source artifacts or documents are missing; "
            "after-remediation PIT gate recheck cannot silently pass."
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
    json_path = output_root / "readiness_recheck_after_remediation_result.json"
    matrix_json_path = output_root / "pit_gate_recheck_after_remediation_matrix.json"
    blocker_json_path = output_root / "blocker_resolution_summary.json"
    contract_gate_json_path = output_root / "contract_readiness_snapshot_gate.json"
    markdown_path = (
        docs_root
        / "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation.md"
    )
    matrix_markdown_path = (
        docs_root
        / "growth_tilt_engine_pit_gate_recheck_after_source_traceability_remediation_matrix.md"
    )
    blocker_markdown_path = (
        docs_root
        / "growth_tilt_engine_source_traceability_blocker_resolution_summary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2422_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "pit_gate_recheck_after_remediation_matrix_json": str(matrix_json_path),
        "blocker_resolution_summary_json": str(blocker_json_path),
        "contract_readiness_snapshot_gate_json": str(contract_gate_json_path),
        "markdown_path": str(markdown_path),
        "pit_gate_recheck_after_remediation_matrix_markdown": str(
            matrix_markdown_path
        ),
        "blocker_resolution_summary_markdown": str(blocker_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_engine_pit_gate_recheck_after_source_traceability_remediation_matrix",
        recheck_after.PIT_GATE_RECHECK_AFTER_REMEDIATION_MATRIX_SCHEMA_VERSION,
        payload,
        "pit_gate_recheck_after_remediation_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        blocker_json_path,
        "growth_tilt_engine_source_traceability_blocker_resolution_summary",
        recheck_after.BLOCKER_RESOLUTION_SUMMARY_SCHEMA_VERSION,
        payload,
        "blocker_resolution_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        contract_gate_json_path,
        "growth_tilt_engine_contract_readiness_snapshot_gate",
        recheck_after.CONTRACT_READINESS_SNAPSHOT_GATE_SCHEMA_VERSION,
        payload,
        "contract_readiness_snapshot_gate",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(matrix_markdown_path, _render_matrix_markdown(payload))
    write_markdown_artifact(blocker_markdown_path, _render_blocker_markdown(payload))
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "source_traceability_recheck_status": payload.get(
            "source_traceability_recheck_status"
        ),
        "resolved_blockers": payload.get("resolved_blockers"),
        "remaining_blockers": payload.get("remaining_blockers"),
        "pit_gate_ready": payload.get("pit_gate_ready"),
        "pit_gate_ready_count": payload.get("pit_gate_ready_count"),
        "contract_ready": payload.get("contract_ready"),
        "contract_ready_count": payload.get("contract_ready_count"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine PIT Gate Readiness Recheck After Source Traceability Remediation",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- source traceability recheck：`{payload.get('source_traceability_recheck_status')}`",
            f"- resolved blockers：`{payload.get('resolved_blockers')}`",
            f"- remaining blockers：`{payload.get('remaining_blockers')}`",
            f"- PIT gate ready：`{payload.get('pit_gate_ready')}`",
            f"- contract ready：`{payload.get('contract_ready')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2421 只做 after-remediation readiness recheck。它可以根据 2420 evidence "
            "计算 PIT gate ready，但不执行 contract readiness 独立复核，也不启用 paper-shadow / "
            "production / broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Blocker Resolution",
            "",
            "```json",
            _json_block(payload.get("blocker_resolution_summary")),
            "```",
        ]
    )


def _render_matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine PIT Gate Recheck After Remediation Matrix",
            "",
            "```json",
            _json_block(payload.get("pit_gate_recheck_after_remediation_matrix")),
            "```",
        ]
    )


def _render_blocker_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine Source Traceability Blocker Resolution Summary",
            "",
            "```json",
            _json_block(payload.get("blocker_resolution_summary")),
            "```",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2422 Route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "若 PIT gate 已 ready，TRADING-2422 应执行 contract readiness snapshot；"
            "2421 不授权 paper-shadow、production 或 broker/order。",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
