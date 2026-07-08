from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_dry_run_wiring as m2425,
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
    growth_tilt_engine_paper_shadow_schedule_dry_run as schedule_dry_run,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2426"
TASK_REGISTER_ID = "TRADING-2426_GROWTH_TILT_ENGINE_PAPER_SHADOW_SCHEDULE_DRY_RUN"
REPORT_TYPE = schedule_dry_run.REPORT_TYPE
SCHEMA_VERSION = schedule_dry_run.SCHEMA_VERSION
READY_STATUS = schedule_dry_run.READY_STATUS
BLOCKED_STATUS = schedule_dry_run.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PAPER_SHADOW_SCHEDULE_DRY_RUN_PRIOR_ARTIFACTS_REGISTRY_"
    "CATALOG_SYSTEM_FLOW_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "enable_paper_shadow",
    "enable_paper_shadow_schedule",
    "run_paper_shadow_daily_job",
    "invoke_schedule_hook",
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
    "cron_or_windows_task_created",
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

DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "dry_run_wiring_result.json"
)
DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DISABLED_VERIFICATION_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "schedule_hook_disabled_verification.json"
)
DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_MANIFEST_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "runtime_boundary_manifest.json"
)
DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "manual_review_handoff_wiring_plan.json"
)
DEFAULT_SOURCE_2425_DRY_RUN_NO_EFFECT_AUDIT_SUMMARY_PATH = (
    m2425.DEFAULT_OUTPUT_ROOT / "dry_run_no_effect_audit_summary.json"
)
DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_dry_run_wiring.md"
)
DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_schedule_hook_disabled_verification.md"
)
DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_dry_run_runtime_boundary_manifest.md"
)
DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_manual_review_handoff_wiring_plan.md"
)
DEFAULT_SOURCE_2425_NO_EFFECT_AUDIT_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_paper_shadow_dry_run_no_effect_audit_summary.md"
)
DEFAULT_SOURCE_2425_ROUTE_DOC_PATH = (
    m2425.DEFAULT_DOCS_ROOT / "dynamic_strategy_2426_route.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_engine_paper_shadow_schedule_dry_run(
    *,
    source_2425_dry_run_wiring_result_path: Path = (
        DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH
    ),
    source_2425_schedule_hook_disabled_verification_path: Path = (
        DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DISABLED_VERIFICATION_PATH
    ),
    source_2425_runtime_boundary_manifest_path: Path = (
        DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_MANIFEST_PATH
    ),
    source_2425_manual_review_handoff_wiring_plan_path: Path = (
        DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH
    ),
    source_2425_dry_run_no_effect_audit_summary_path: Path = (
        DEFAULT_SOURCE_2425_DRY_RUN_NO_EFFECT_AUDIT_SUMMARY_PATH
    ),
    source_2425_research_doc_path: Path = DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH,
    source_2425_schedule_hook_doc_path: Path = (
        DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DOC_PATH
    ),
    source_2425_runtime_boundary_doc_path: Path = (
        DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_DOC_PATH
    ),
    source_2425_manual_review_doc_path: Path = (
        DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH
    ),
    source_2425_no_effect_audit_doc_path: Path = (
        DEFAULT_SOURCE_2425_NO_EFFECT_AUDIT_DOC_PATH
    ),
    source_2425_route_doc_path: Path = DEFAULT_SOURCE_2425_ROUTE_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2425_dry_run_wiring_result": _load_json_document(
            source_2425_dry_run_wiring_result_path
        ),
        "source_2425_schedule_hook_disabled_verification": _load_json_document(
            source_2425_schedule_hook_disabled_verification_path
        ),
        "source_2425_runtime_boundary_manifest": _load_json_document(
            source_2425_runtime_boundary_manifest_path
        ),
        "source_2425_manual_review_handoff_wiring_plan": _load_json_document(
            source_2425_manual_review_handoff_wiring_plan_path
        ),
        "source_2425_dry_run_no_effect_audit_summary": _load_json_document(
            source_2425_dry_run_no_effect_audit_summary_path
        ),
        "source_2425_research_doc": _load_text_document(source_2425_research_doc_path),
        "source_2425_schedule_hook_doc": _load_text_document(
            source_2425_schedule_hook_doc_path
        ),
        "source_2425_runtime_boundary_doc": _load_text_document(
            source_2425_runtime_boundary_doc_path
        ),
        "source_2425_manual_review_doc": _load_text_document(
            source_2425_manual_review_doc_path
        ),
        "source_2425_no_effect_audit_doc": _load_text_document(
            source_2425_no_effect_audit_doc_path
        ),
        "source_2425_route_doc": _load_text_document(source_2425_route_doc_path),
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
            schedule_dry_run.build_growth_tilt_engine_paper_shadow_schedule_dry_run(
                _as_mapping(sources["source_2425_dry_run_wiring_result"]),
                _as_mapping(
                    sources["source_2425_schedule_hook_disabled_verification"]
                ),
                _as_mapping(sources["source_2425_runtime_boundary_manifest"]),
                _as_mapping(sources["source_2425_manual_review_handoff_wiring_plan"]),
                _as_mapping(sources["source_2425_dry_run_no_effect_audit_summary"]),
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
            "classification": "missing_schedule_evidence",
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
        "target_strategy_id": schedule_dry_run.TARGET_STRATEGY_ID,
        "prior_route": schedule_dry_run.PRIOR_ROUTE,
        "source_tasks": ["TRADING-2425"],
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_gap_count": 1,
        "paper_shadow_dry_run_wiring_status": "UNKNOWN",
        "paper_shadow_dry_run_wiring_ready": False,
        "dry_run_wiring_gap_count": 1,
        "schedule_hook_verified_disabled": False,
        "runtime_boundary_verified": False,
        "manual_review_handoff_wired": False,
        "prior_no_effect_audit_ready": False,
        "paper_shadow_schedule_dry_run_started": False,
        "paper_shadow_schedule_dry_run_completed": False,
        "paper_shadow_schedule_dry_run_ready": False,
        "schedule_dry_run_plan_ready": False,
        "schedule_boundary_checklist_ready": False,
        "schedule_no_effect_audit_ready": False,
        "schedule_dry_run_gap_count": len(gaps),
        "schedule_dry_run_gap_ids": ["source_artifact_availability"],
        "missing_schedule_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "schedule_contract_gap_count": 0,
        "precondition_gap_count": 0,
        "schedule_dry_run_plan": {},
        "schedule_boundary_checklist": {
            "schema_version": (
                schedule_dry_run.SCHEDULE_BOUNDARY_CHECKLIST_SCHEMA_VERSION
            ),
            "status": BLOCKED_STATUS,
            "schedule_boundary_checklist_ready": False,
            "checks": [],
            "production_effect": "none",
            "broker_action": "none",
        },
        "schedule_no_effect_audit_summary": {
            "schema_version": (
                schedule_dry_run.SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION
            ),
            "status": BLOCKED_STATUS,
            "schedule_dry_run_gap_count": len(gaps),
            "gaps": gaps,
            "schedule_no_effect_audit_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "schedule_hook_invoked": False,
        "schedule_state_mutated": False,
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
        "recommended_next_research_task": schedule_dry_run.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; schedule dry-run "
            "cannot silently pass."
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
    json_path = output_root / "schedule_dry_run_result.json"
    boundary_json_path = output_root / "schedule_boundary_checklist.json"
    no_effect_json_path = output_root / "schedule_no_effect_audit_summary.json"
    markdown_path = docs_root / "growth_tilt_engine_paper_shadow_schedule_dry_run.md"
    boundary_markdown_path = (
        docs_root / "growth_tilt_engine_paper_shadow_schedule_boundary_checklist.md"
    )
    no_effect_markdown_path = (
        docs_root
        / "growth_tilt_engine_paper_shadow_schedule_no_effect_audit_summary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2427_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "schedule_boundary_checklist_json": str(boundary_json_path),
        "schedule_no_effect_audit_summary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "schedule_boundary_checklist_markdown": str(boundary_markdown_path),
        "schedule_no_effect_audit_summary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_engine_paper_shadow_schedule_boundary_checklist",
        schedule_dry_run.SCHEDULE_BOUNDARY_CHECKLIST_SCHEMA_VERSION,
        payload,
        "schedule_boundary_checklist",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_engine_paper_shadow_schedule_no_effect_audit_summary",
        schedule_dry_run.SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION,
        payload,
        "schedule_no_effect_audit_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Schedule Boundary Checklist",
            payload.get("schedule_boundary_checklist"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Paper Shadow Schedule No-Effect Audit Summary",
            payload.get("schedule_no_effect_audit_summary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "paper_shadow_dry_run_wiring_ready": payload.get(
            "paper_shadow_dry_run_wiring_ready"
        ),
        "schedule_hook_verified_disabled": payload.get(
            "schedule_hook_verified_disabled"
        ),
        "runtime_boundary_verified": payload.get("runtime_boundary_verified"),
        "manual_review_handoff_wired": payload.get("manual_review_handoff_wired"),
        "paper_shadow_schedule_dry_run_ready": payload.get(
            "paper_shadow_schedule_dry_run_ready"
        ),
        "schedule_dry_run_gap_count": payload.get("schedule_dry_run_gap_count"),
        "schedule_boundary_checklist_ready": payload.get(
            "schedule_boundary_checklist_ready"
        ),
        "schedule_no_effect_audit_ready": payload.get(
            "schedule_no_effect_audit_ready"
        ),
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
            "# Growth Tilt Engine Paper Shadow Schedule Dry-Run",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- schedule dry-run ready：`{payload.get('paper_shadow_schedule_dry_run_ready')}`",
            f"- schedule dry-run gap count：`{payload.get('schedule_dry_run_gap_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2426 只验证 paper-shadow schedule dry-run wiring。READY 不等于 "
            "paper_shadow_schedule_enabled；本任务不启用 scheduler，不运行 daily job，"
            "不读取 fresh market data，不生成 signal / trading advice，不进入 production "
            "或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Schedule Boundary Checklist",
            "",
            "```json",
            _json_block(payload.get("schedule_boundary_checklist")),
            "```",
            "",
            "## Schedule No-Effect Audit",
            "",
            "```json",
            _json_block(payload.get("schedule_no_effect_audit_summary")),
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
            "# Dynamic Strategy TRADING-2427 Route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2427 must remain a no-advice manual review packet dry-run. "
            "TRADING-2426 does not enable paper-shadow schedule, production, or broker.",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
