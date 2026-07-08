from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_readiness_snapshot as m2422,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_enablement_plan as m2424,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_preflight as m2423,
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
    growth_tilt_engine_paper_shadow_dry_run_wiring as wiring,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2425"
TASK_REGISTER_ID = "TRADING-2425_GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING"
REPORT_TYPE = wiring.REPORT_TYPE
SCHEMA_VERSION = wiring.SCHEMA_VERSION
READY_STATUS = wiring.READY_STATUS
BLOCKED_STATUS = wiring.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PAPER_SHADOW_DRY_RUN_WIRING_PRIOR_ARTIFACTS_REGISTRY_"
    "CATALOG_SYSTEM_FLOW_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "enable_paper_shadow",
    "run_paper_shadow_schedule",
    "run_paper_shadow_daily_job",
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
    "read_fresh_cached_market_data",
    "modify_actual_portfolio_weights",
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

DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "enablement_plan_result.json"
)
DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "paper_shadow_enablement_plan.json"
)
DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_CHECKLIST_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "runtime_boundary_checklist.json"
)
DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_PLAN_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "schedule_boundary_plan.json"
)
DEFAULT_SOURCE_2424_MANUAL_REVIEW_CHECKLIST_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "manual_review_checklist.json"
)
DEFAULT_SOURCE_2424_ROLLBACK_STOP_CONDITION_SUMMARY_PATH = (
    m2424.DEFAULT_OUTPUT_ROOT / "rollback_stop_condition_summary.json"
)
DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_enablement_plan.md"
)
DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_runtime_boundary_checklist.md"
)
DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_schedule_boundary_plan.md"
)
DEFAULT_SOURCE_2424_MANUAL_REVIEW_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_manual_review_checklist.md"
)
DEFAULT_SOURCE_2424_ROLLBACK_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_rollback_stop_condition_summary.md"
)
DEFAULT_SOURCE_2424_ROUTE_DOC_PATH = (
    m2424.DEFAULT_DOCS_ROOT / "dynamic_strategy_2425_route.md"
)

DEFAULT_SOURCE_2423_PREFLIGHT_RESULT_PATH = (
    m2423.DEFAULT_OUTPUT_ROOT / "paper_shadow_preflight_result.json"
)
DEFAULT_SOURCE_2423_RESEARCH_DOC_PATH = (
    m2423.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_preflight.md"
)
DEFAULT_SOURCE_2423_ROUTE_DOC_PATH = (
    m2423.DEFAULT_DOCS_ROOT / "dynamic_strategy_2424_route.md"
)
DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH = (
    m2422.DEFAULT_OUTPUT_ROOT / "contract_readiness_snapshot_result.json"
)
DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH = (
    m2422.DEFAULT_DOCS_ROOT / "growth_tilt_engine_contract_readiness_snapshot.md"
)
DEFAULT_SOURCE_2422_ROUTE_DOC_PATH = (
    m2422.DEFAULT_DOCS_ROOT / "dynamic_strategy_2423_route.md"
)
DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH = (
    m2421.DEFAULT_OUTPUT_ROOT / "readiness_recheck_after_remediation_result.json"
)
DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH = (
    m2421.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation.md"
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


def run_growth_tilt_engine_paper_shadow_dry_run_wiring(
    *,
    source_2424_enablement_plan_result_path: Path = (
        DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH
    ),
    source_2424_enablement_plan_path: Path = DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_PATH,
    source_2424_runtime_boundary_checklist_path: Path = (
        DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_CHECKLIST_PATH
    ),
    source_2424_schedule_boundary_plan_path: Path = (
        DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_PLAN_PATH
    ),
    source_2424_manual_review_checklist_path: Path = (
        DEFAULT_SOURCE_2424_MANUAL_REVIEW_CHECKLIST_PATH
    ),
    source_2424_rollback_stop_condition_summary_path: Path = (
        DEFAULT_SOURCE_2424_ROLLBACK_STOP_CONDITION_SUMMARY_PATH
    ),
    source_2424_research_doc_path: Path = DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH,
    source_2424_runtime_boundary_doc_path: Path = (
        DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_DOC_PATH
    ),
    source_2424_schedule_boundary_doc_path: Path = (
        DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_DOC_PATH
    ),
    source_2424_manual_review_doc_path: Path = (
        DEFAULT_SOURCE_2424_MANUAL_REVIEW_DOC_PATH
    ),
    source_2424_rollback_doc_path: Path = DEFAULT_SOURCE_2424_ROLLBACK_DOC_PATH,
    source_2424_route_doc_path: Path = DEFAULT_SOURCE_2424_ROUTE_DOC_PATH,
    source_2423_preflight_result_path: Path = DEFAULT_SOURCE_2423_PREFLIGHT_RESULT_PATH,
    source_2423_research_doc_path: Path = DEFAULT_SOURCE_2423_RESEARCH_DOC_PATH,
    source_2423_route_doc_path: Path = DEFAULT_SOURCE_2423_ROUTE_DOC_PATH,
    source_2422_contract_readiness_snapshot_path: Path = (
        DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH
    ),
    source_2422_research_doc_path: Path = DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_route_doc_path: Path = DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Path = (
        DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH
    ),
    source_2421_research_doc_path: Path = DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
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
        "source_2424_enablement_plan_result": _load_json_document(
            source_2424_enablement_plan_result_path
        ),
        "source_2424_enablement_plan": _load_json_document(
            source_2424_enablement_plan_path
        ),
        "source_2424_runtime_boundary_checklist": _load_json_document(
            source_2424_runtime_boundary_checklist_path
        ),
        "source_2424_schedule_boundary_plan": _load_json_document(
            source_2424_schedule_boundary_plan_path
        ),
        "source_2424_manual_review_checklist": _load_json_document(
            source_2424_manual_review_checklist_path
        ),
        "source_2424_rollback_stop_condition_summary": _load_json_document(
            source_2424_rollback_stop_condition_summary_path
        ),
        "source_2424_research_doc": _load_text_document(source_2424_research_doc_path),
        "source_2424_runtime_boundary_doc": _load_text_document(
            source_2424_runtime_boundary_doc_path
        ),
        "source_2424_schedule_boundary_doc": _load_text_document(
            source_2424_schedule_boundary_doc_path
        ),
        "source_2424_manual_review_doc": _load_text_document(
            source_2424_manual_review_doc_path
        ),
        "source_2424_rollback_doc": _load_text_document(source_2424_rollback_doc_path),
        "source_2424_route_doc": _load_text_document(source_2424_route_doc_path),
        "source_2423_preflight_result": _load_json_document(
            source_2423_preflight_result_path
        ),
        "source_2423_research_doc": _load_text_document(source_2423_research_doc_path),
        "source_2423_route_doc": _load_text_document(source_2423_route_doc_path),
        "source_2422_contract_readiness_snapshot": _load_json_document(
            source_2422_contract_readiness_snapshot_path
        ),
        "source_2422_research_doc": _load_text_document(source_2422_research_doc_path),
        "source_2422_route_doc": _load_text_document(source_2422_route_doc_path),
        "source_2421_readiness_recheck_result": _load_json_document(
            source_2421_readiness_recheck_result_path
        ),
        "source_2421_research_doc": _load_text_document(source_2421_research_doc_path),
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
        payload = wiring.build_growth_tilt_engine_paper_shadow_dry_run_wiring(
            _as_mapping(sources["source_2424_enablement_plan_result"]),
            _as_mapping(sources["source_2423_preflight_result"]),
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
            "classification": "missing_dry_run_evidence",
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
        "target_strategy_id": wiring.TARGET_STRATEGY_ID,
        "prior_route": wiring.PRIOR_ROUTE,
        "source_tasks": [
            "TRADING-2420",
            "TRADING-2421",
            "TRADING-2422",
            "TRADING-2423",
            "TRADING-2424",
        ],
        "artifact_id": wiring.ARTIFACT_ID,
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "contract_readiness_status": "UNKNOWN",
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_gap_count": 1,
        "paper_shadow_preflight_ready": False,
        "enablement_plan_ready": False,
        "enablement_gap_count": 1,
        "remaining_pit_blockers": [wiring.ARTIFACT_ID],
        "remaining_pit_blocker_count": 1,
        "source_traceability_remediation_status": "UNKNOWN",
        "source_traceability_recheck_status": "UNKNOWN",
        "source_traceability_accepted": False,
        "paper_shadow_dry_run_wiring_started": False,
        "paper_shadow_dry_run_wiring_completed": False,
        "dry_run_wiring_ready": False,
        "dry_run_wiring_gap_count": len(gaps),
        "dry_run_wiring_gap_ids": ["source_artifact_availability"],
        "missing_dry_run_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "wiring_contract_gap_count": 0,
        "precondition_gap_count": 0,
        "input_contract_map_ready": False,
        "output_artifact_contract_map_ready": False,
        "manual_review_handoff_wired": False,
        "schedule_hook_verified_disabled": False,
        "no_effect_audit_ready": False,
        "input_output_contract_map": {
            "schema_version": wiring.INPUT_OUTPUT_CONTRACT_MAP_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "input_contract_map_ready": False,
            "output_artifact_contract_map_ready": False,
            "input_contract_map": [],
            "output_artifact_contract_map": [],
            "production_effect": "none",
            "broker_action": "none",
        },
        "runtime_boundary_manifest": {
            "schema_version": wiring.RUNTIME_BOUNDARY_MANIFEST_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "paper_shadow_enabled": False,
            "paper_shadow_schedule_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "automatic_execution_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "schedule_hook_disabled_verification": {
            "schema_version": wiring.SCHEDULE_HOOK_DISABLED_VERIFICATION_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "schedule_hook_verified_disabled": False,
            "paper_shadow_schedule_enabled": False,
            "scheduler_enabled": False,
            "scheduled_task_created": False,
            "paper_shadow_daily_job_run": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "manual_review_handoff_wiring_plan": {
            "schema_version": wiring.MANUAL_REVIEW_HANDOFF_WIRING_PLAN_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "manual_review_required": True,
            "manual_review_handoff_wired": False,
            "automatic_execution_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "dry_run_no_effect_audit_summary": {
            "schema_version": wiring.NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "dry_run_wiring_gap_count": len(gaps),
            "gaps": gaps,
            "no_effect_audit_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "new_signal_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "recommended_next_research_task": wiring.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; paper-shadow "
            "dry-run wiring cannot silently pass."
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
    json_path = output_root / "dry_run_wiring_result.json"
    contract_map_json_path = output_root / "input_output_contract_map.json"
    runtime_manifest_json_path = output_root / "runtime_boundary_manifest.json"
    schedule_hook_json_path = output_root / "schedule_hook_disabled_verification.json"
    manual_review_json_path = output_root / "manual_review_handoff_wiring_plan.json"
    no_effect_json_path = output_root / "dry_run_no_effect_audit_summary.json"
    markdown_path = docs_root / "growth_tilt_engine_paper_shadow_dry_run_wiring.md"
    contract_map_markdown_path = (
        docs_root / "growth_tilt_engine_paper_shadow_input_output_contract_map.md"
    )
    runtime_manifest_markdown_path = (
        docs_root
        / "growth_tilt_engine_paper_shadow_dry_run_runtime_boundary_manifest.md"
    )
    schedule_hook_markdown_path = (
        docs_root
        / "growth_tilt_engine_paper_shadow_schedule_hook_disabled_verification.md"
    )
    manual_review_markdown_path = (
        docs_root
        / "growth_tilt_engine_paper_shadow_manual_review_handoff_wiring_plan.md"
    )
    no_effect_markdown_path = (
        docs_root / "growth_tilt_engine_paper_shadow_dry_run_no_effect_audit_summary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2426_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "input_output_contract_map_json": str(contract_map_json_path),
        "runtime_boundary_manifest_json": str(runtime_manifest_json_path),
        "schedule_hook_disabled_verification_json": str(schedule_hook_json_path),
        "manual_review_handoff_wiring_plan_json": str(manual_review_json_path),
        "dry_run_no_effect_audit_summary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "input_output_contract_map_markdown": str(contract_map_markdown_path),
        "runtime_boundary_manifest_markdown": str(runtime_manifest_markdown_path),
        "schedule_hook_disabled_verification_markdown": str(
            schedule_hook_markdown_path
        ),
        "manual_review_handoff_wiring_plan_markdown": str(
            manual_review_markdown_path
        ),
        "dry_run_no_effect_audit_summary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        contract_map_json_path,
        "growth_tilt_engine_paper_shadow_input_output_contract_map",
        wiring.INPUT_OUTPUT_CONTRACT_MAP_SCHEMA_VERSION,
        payload,
        "input_output_contract_map",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        runtime_manifest_json_path,
        "growth_tilt_engine_paper_shadow_dry_run_runtime_boundary_manifest",
        wiring.RUNTIME_BOUNDARY_MANIFEST_SCHEMA_VERSION,
        payload,
        "runtime_boundary_manifest",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        schedule_hook_json_path,
        "growth_tilt_engine_paper_shadow_schedule_hook_disabled_verification",
        wiring.SCHEDULE_HOOK_DISABLED_VERIFICATION_SCHEMA_VERSION,
        payload,
        "schedule_hook_disabled_verification",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        manual_review_json_path,
        "growth_tilt_engine_paper_shadow_manual_review_handoff_wiring_plan",
        wiring.MANUAL_REVIEW_HANDOFF_WIRING_PLAN_SCHEMA_VERSION,
        payload,
        "manual_review_handoff_wiring_plan",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_engine_paper_shadow_dry_run_no_effect_audit_summary",
        wiring.NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION,
        payload,
        "dry_run_no_effect_audit_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        contract_map_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Input Output Contract Map",
            payload.get("input_output_contract_map"),
        ),
    )
    write_markdown_artifact(
        runtime_manifest_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Dry-Run Runtime Boundary Manifest",
            payload.get("runtime_boundary_manifest"),
        ),
    )
    write_markdown_artifact(
        schedule_hook_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Schedule Hook Disabled Verification",
            payload.get("schedule_hook_disabled_verification"),
        ),
    )
    write_markdown_artifact(
        manual_review_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Manual Review Handoff Wiring Plan",
            payload.get("manual_review_handoff_wiring_plan"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Dry-Run No-Effect Audit Summary",
            payload.get("dry_run_no_effect_audit_summary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "pit_gate_ready": payload.get("pit_gate_ready"),
        "contract_ready": payload.get("contract_ready"),
        "paper_shadow_preflight_ready": payload.get("paper_shadow_preflight_ready"),
        "enablement_plan_ready": payload.get("enablement_plan_ready"),
        "dry_run_wiring_ready": payload.get("dry_run_wiring_ready"),
        "dry_run_wiring_gap_count": payload.get("dry_run_wiring_gap_count"),
        "input_contract_map_ready": payload.get("input_contract_map_ready"),
        "output_artifact_contract_map_ready": payload.get(
            "output_artifact_contract_map_ready"
        ),
        "manual_review_handoff_wired": payload.get("manual_review_handoff_wired"),
        "schedule_hook_verified_disabled": payload.get(
            "schedule_hook_verified_disabled"
        ),
        "no_effect_audit_ready": payload.get("no_effect_audit_ready"),
        "paper_shadow_enabled": payload.get("paper_shadow_enabled"),
        "paper_shadow_schedule_enabled": payload.get("paper_shadow_schedule_enabled"),
        "production_enabled": payload.get("production_enabled"),
        "broker_enabled": payload.get("broker_enabled"),
        "automatic_execution_allowed": payload.get("automatic_execution_allowed"),
        "generated_signal": payload.get("generated_signal"),
        "generated_trading_advice": payload.get("generated_trading_advice"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Paper Shadow Dry-Run Wiring",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- dry-run wiring ready：`{payload.get('dry_run_wiring_ready')}`",
            f"- dry-run wiring gap count：`{payload.get('dry_run_wiring_gap_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2425 只生成 paper-shadow dry-run wiring 证据。READY 不等于 "
            "paper-shadow enabled；本任务不启用 runtime 或 schedule，不读取 fresh "
            "market data，不生成 signal / trading advice，不进入 production 或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Input / Output Contract Map",
            "",
            "```json",
            _json_block(payload.get("input_output_contract_map")),
            "```",
            "",
            "## No-Effect Audit",
            "",
            "```json",
            _json_block(payload.get("dry_run_no_effect_audit_summary")),
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
            "# Dynamic Strategy TRADING-2426 Route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "若 paper-shadow dry-run wiring READY，TRADING-2426 应处理 "
            "paper-shadow schedule dry-run；2425 不授权 paper-shadow runtime、schedule、"
            "production 或 broker/order。",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
