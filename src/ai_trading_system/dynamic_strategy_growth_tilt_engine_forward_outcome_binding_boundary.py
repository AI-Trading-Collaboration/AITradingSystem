from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_observe_only_signal_artifact_boundary as m2428,
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
    growth_tilt_engine_forward_outcome_binding_boundary as boundary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2429"
TASK_REGISTER_ID = (
    "TRADING-2429_GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY"
)
REPORT_TYPE = boundary.REPORT_TYPE
SCHEMA_VERSION = boundary.SCHEMA_VERSION
READY_STATUS = boundary.READY_STATUS
BLOCKED_STATUS = boundary.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_FORWARD_OUTCOME_BINDING_BOUNDARY_PRIOR_ARTIFACTS_"
    "REGISTRY_CATALOG_SYSTEM_FLOW_DOCS_ONLY_NO_FRESH_MARKET_OR_OUTCOME_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "generate_real_signal",
    "backfill_real_outcome",
    "bind_real_outcome",
    "mutate_outcome_store",
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
    "scheduler_enabled",
    "scheduled_task_created",
    "schedule_hook_invoked",
    "schedule_state_mutated",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
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

DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "observe_only_signal_artifact_boundary_result.json"
)
DEFAULT_SOURCE_2428_SIGNAL_ARTIFACT_SCHEMA_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "signal_artifact_schema.json"
)
DEFAULT_SOURCE_2428_VALID_UNTIL_REQUIREMENTS_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "valid_until_requirements.json"
)
DEFAULT_SOURCE_2428_SOURCE_TRACEABILITY_REQUIREMENTS_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "source_traceability_requirements.json"
)
DEFAULT_SOURCE_2428_PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "pit_contract_manual_review_requirements.json"
)
DEFAULT_SOURCE_2428_NO_TRADING_ADVICE_BOUNDARY_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "no_trading_advice_boundary.json"
)
DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT / "growth_tilt_engine_observe_only_signal_artifact_boundary.md"
)
DEFAULT_SOURCE_2428_SCHEMA_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT / "growth_tilt_engine_observe_only_signal_artifact_schema.md"
)
DEFAULT_SOURCE_2428_VALID_UNTIL_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_observe_only_signal_valid_until_requirements.md"
)
DEFAULT_SOURCE_2428_TRACEABILITY_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_observe_only_signal_source_traceability_requirements.md"
)
DEFAULT_SOURCE_2428_NO_ADVICE_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT
    / "growth_tilt_engine_observe_only_signal_no_trading_advice_boundary.md"
)
DEFAULT_SOURCE_2428_ROUTE_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT / "dynamic_strategy_2429_route.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_engine_forward_outcome_binding_boundary(
    *,
    source_2428_observe_only_boundary_result_path: Path = (
        DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH
    ),
    source_2428_signal_artifact_schema_path: Path = (
        DEFAULT_SOURCE_2428_SIGNAL_ARTIFACT_SCHEMA_PATH
    ),
    source_2428_valid_until_requirements_path: Path = (
        DEFAULT_SOURCE_2428_VALID_UNTIL_REQUIREMENTS_PATH
    ),
    source_2428_source_traceability_requirements_path: Path = (
        DEFAULT_SOURCE_2428_SOURCE_TRACEABILITY_REQUIREMENTS_PATH
    ),
    source_2428_pit_contract_manual_review_requirements_path: Path = (
        DEFAULT_SOURCE_2428_PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_PATH
    ),
    source_2428_no_trading_advice_boundary_path: Path = (
        DEFAULT_SOURCE_2428_NO_TRADING_ADVICE_BOUNDARY_PATH
    ),
    source_2428_research_doc_path: Path = DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH,
    source_2428_schema_doc_path: Path = DEFAULT_SOURCE_2428_SCHEMA_DOC_PATH,
    source_2428_valid_until_doc_path: Path = DEFAULT_SOURCE_2428_VALID_UNTIL_DOC_PATH,
    source_2428_traceability_doc_path: Path = (
        DEFAULT_SOURCE_2428_TRACEABILITY_DOC_PATH
    ),
    source_2428_no_advice_doc_path: Path = DEFAULT_SOURCE_2428_NO_ADVICE_DOC_PATH,
    source_2428_route_doc_path: Path = DEFAULT_SOURCE_2428_ROUTE_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2428_observe_only_boundary_result": _load_json_document(
            source_2428_observe_only_boundary_result_path
        ),
        "source_2428_signal_artifact_schema": _load_json_document(
            source_2428_signal_artifact_schema_path
        ),
        "source_2428_valid_until_requirements": _load_json_document(
            source_2428_valid_until_requirements_path
        ),
        "source_2428_source_traceability_requirements": _load_json_document(
            source_2428_source_traceability_requirements_path
        ),
        "source_2428_pit_contract_manual_review_requirements": _load_json_document(
            source_2428_pit_contract_manual_review_requirements_path
        ),
        "source_2428_no_trading_advice_boundary": _load_json_document(
            source_2428_no_trading_advice_boundary_path
        ),
        "source_2428_research_doc": _load_text_document(
            source_2428_research_doc_path
        ),
        "source_2428_schema_doc": _load_text_document(source_2428_schema_doc_path),
        "source_2428_valid_until_doc": _load_text_document(
            source_2428_valid_until_doc_path
        ),
        "source_2428_traceability_doc": _load_text_document(
            source_2428_traceability_doc_path
        ),
        "source_2428_no_advice_doc": _load_text_document(
            source_2428_no_advice_doc_path
        ),
        "source_2428_route_doc": _load_text_document(source_2428_route_doc_path),
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
        payload = boundary.build_growth_tilt_engine_forward_outcome_binding_boundary(
            _as_mapping(sources["source_2428_observe_only_boundary_result"]),
            _as_mapping(sources["source_2428_signal_artifact_schema"]),
            _as_mapping(sources["source_2428_valid_until_requirements"]),
            _as_mapping(sources["source_2428_source_traceability_requirements"]),
            _as_mapping(sources["source_2428_pit_contract_manual_review_requirements"]),
            _as_mapping(sources["source_2428_no_trading_advice_boundary"]),
            report_registry=_as_mapping(sources["report_registry"]),
            artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                "text",
                "",
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
            "classification": "missing_forward_outcome_boundary_evidence",
            "gap": (
                "Required TRADING-2428 artifact, registry, catalog, system "
                "flow, or doc is missing."
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
        "source_tasks": ["TRADING-2428"],
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "contract_ready": False,
        "contract_ready_count": 0,
        "observe_only_signal_artifact_boundary_status": "UNKNOWN",
        "observe_only_signal_artifact_boundary_ready": False,
        "prior_signal_artifact_schema_ready": False,
        "prior_valid_until_requirements_ready": False,
        "prior_source_traceability_requirements_ready": False,
        "prior_pit_contract_manual_review_requirements_ready": False,
        "prior_no_trading_advice_boundary_ready": False,
        "forward_outcome_binding_boundary_started": False,
        "forward_outcome_binding_boundary_completed": False,
        "forward_outcome_binding_boundary_ready": False,
        "outcome_horizons": [],
        "outcome_horizon_rules_ready": False,
        "outcome_schema_ready": False,
        "valid_until_binding_ready": False,
        "outcome_decision_rules_ready": False,
        "baseline_comparison_ready": False,
        "signal_to_outcome_linkage_ready": False,
        "no_effect_boundary_ready": False,
        "forward_outcome_binding_boundary_gap_count": len(gaps),
        "forward_outcome_binding_boundary_gap_ids": [
            "source_artifact_availability"
        ],
        "missing_binding_boundary_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "outcome_contract_gap_count": 0,
        "precondition_gap_count": 0,
        "requirements": [],
        "gaps": gaps,
        "outcome_horizon_rules": {
            "schema_version": boundary.OUTCOME_HORIZON_RULES_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "outcome_horizon_rules_ready": False,
        },
        "valid_until_binding_rules": {
            "schema_version": boundary.VALID_UNTIL_BINDING_RULES_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "valid_until_binding_ready": False,
        },
        "outcome_decision_rules": {
            "schema_version": boundary.OUTCOME_DECISION_RULES_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "outcome_decision_rules_ready": False,
        },
        "baseline_comparison_rules": {
            "schema_version": boundary.BASELINE_COMPARISON_RULES_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "baseline_comparison_ready": False,
        },
        "outcome_artifact_schema": {
            "schema_version": boundary.OUTCOME_ARTIFACT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "outcome_schema_ready": False,
        },
        "signal_to_outcome_linkage": {
            "schema_version": boundary.SIGNAL_TO_OUTCOME_LINKAGE_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "signal_to_outcome_linkage_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": boundary.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": boundary.NEXT_ROUTE_BLOCKED,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; forward "
            "outcome binding boundary cannot silently pass."
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
    json_path = output_root / "forward_outcome_binding_boundary_result.json"
    horizon_json_path = output_root / "outcome_horizon_rules.json"
    valid_until_json_path = output_root / "valid_until_binding_rules.json"
    decision_json_path = output_root / "outcome_decision_rules.json"
    baseline_json_path = output_root / "baseline_comparison_rules.json"
    schema_json_path = output_root / "outcome_artifact_schema.json"
    linkage_json_path = output_root / "signal_to_outcome_linkage.json"
    no_effect_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_engine_forward_outcome_binding_boundary.md"
    horizon_markdown_path = (
        docs_root / "growth_tilt_engine_forward_outcome_horizon_rules.md"
    )
    valid_until_markdown_path = (
        docs_root / "growth_tilt_engine_forward_outcome_valid_until_binding_rules.md"
    )
    decision_markdown_path = (
        docs_root / "growth_tilt_engine_forward_outcome_decision_rules.md"
    )
    baseline_markdown_path = (
        docs_root / "growth_tilt_engine_forward_outcome_baseline_comparison_rules.md"
    )
    schema_markdown_path = (
        docs_root / "growth_tilt_engine_forward_outcome_artifact_schema.md"
    )
    linkage_markdown_path = docs_root / "growth_tilt_engine_signal_to_outcome_linkage.md"
    no_effect_markdown_path = (
        docs_root / "growth_tilt_engine_forward_outcome_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2430_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "outcome_horizon_rules_json": str(horizon_json_path),
        "valid_until_binding_rules_json": str(valid_until_json_path),
        "outcome_decision_rules_json": str(decision_json_path),
        "baseline_comparison_rules_json": str(baseline_json_path),
        "outcome_artifact_schema_json": str(schema_json_path),
        "signal_to_outcome_linkage_json": str(linkage_json_path),
        "no_effect_boundary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "outcome_horizon_rules_markdown": str(horizon_markdown_path),
        "valid_until_binding_rules_markdown": str(valid_until_markdown_path),
        "outcome_decision_rules_markdown": str(decision_markdown_path),
        "baseline_comparison_rules_markdown": str(baseline_markdown_path),
        "outcome_artifact_schema_markdown": str(schema_markdown_path),
        "signal_to_outcome_linkage_markdown": str(linkage_markdown_path),
        "no_effect_boundary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        horizon_json_path,
        "growth_tilt_engine_forward_outcome_horizon_rules",
        boundary.OUTCOME_HORIZON_RULES_SCHEMA_VERSION,
        payload,
        "outcome_horizon_rules",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        valid_until_json_path,
        "growth_tilt_engine_forward_outcome_valid_until_binding_rules",
        boundary.VALID_UNTIL_BINDING_RULES_SCHEMA_VERSION,
        payload,
        "valid_until_binding_rules",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        decision_json_path,
        "growth_tilt_engine_forward_outcome_decision_rules",
        boundary.OUTCOME_DECISION_RULES_SCHEMA_VERSION,
        payload,
        "outcome_decision_rules",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        baseline_json_path,
        "growth_tilt_engine_forward_outcome_baseline_comparison_rules",
        boundary.BASELINE_COMPARISON_RULES_SCHEMA_VERSION,
        payload,
        "baseline_comparison_rules",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        schema_json_path,
        "growth_tilt_engine_forward_outcome_artifact_schema",
        boundary.OUTCOME_ARTIFACT_SCHEMA_VERSION,
        payload,
        "outcome_artifact_schema",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        linkage_json_path,
        "growth_tilt_engine_signal_to_outcome_linkage",
        boundary.SIGNAL_TO_OUTCOME_LINKAGE_SCHEMA_VERSION,
        payload,
        "signal_to_outcome_linkage",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_engine_forward_outcome_no_effect_boundary",
        boundary.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        horizon_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Forward Outcome Horizon Rules",
            payload.get("outcome_horizon_rules"),
        ),
    )
    write_markdown_artifact(
        valid_until_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Forward Outcome Valid-Until Binding Rules",
            payload.get("valid_until_binding_rules"),
        ),
    )
    write_markdown_artifact(
        decision_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Forward Outcome Decision Rules",
            payload.get("outcome_decision_rules"),
        ),
    )
    write_markdown_artifact(
        baseline_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Forward Outcome Baseline Comparison Rules",
            payload.get("baseline_comparison_rules"),
        ),
    )
    write_markdown_artifact(
        schema_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Forward Outcome Artifact Schema",
            payload.get("outcome_artifact_schema"),
        ),
    )
    write_markdown_artifact(
        linkage_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Signal To Outcome Linkage",
            payload.get("signal_to_outcome_linkage"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Forward Outcome No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "observe_only_signal_artifact_boundary_ready": payload.get(
            "observe_only_signal_artifact_boundary_ready"
        ),
        "forward_outcome_binding_boundary_ready": payload.get(
            "forward_outcome_binding_boundary_ready"
        ),
        "outcome_horizons": payload.get("outcome_horizons"),
        "outcome_schema_ready": payload.get("outcome_schema_ready"),
        "valid_until_binding_ready": payload.get("valid_until_binding_ready"),
        "baseline_comparison_ready": payload.get("baseline_comparison_ready"),
        "generated_signal": payload.get("generated_signal"),
        "outcome_backfilled": payload.get("outcome_backfilled"),
        "paper_shadow_enabled": payload.get("paper_shadow_enabled"),
        "production_enabled": payload.get("production_enabled"),
        "broker_enabled": payload.get("broker_enabled"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Forward Outcome Binding Boundary",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            "- forward outcome binding boundary ready："
            f"`{payload.get('forward_outcome_binding_boundary_ready')}`",
            "- boundary gap count："
            f"`{payload.get('forward_outcome_binding_boundary_gap_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2429 只定义 future observe-only signal 的 outcome binding "
            "boundary。READY 不等于真实 signal、outcome backfill、trading advice、"
            "paper-shadow activation、production action 或 broker order。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Outcome Artifact Schema",
            "",
            "```json",
            _json_block(payload.get("outcome_artifact_schema", {})),
            "```",
            "",
            "## No-Effect Boundary",
            "",
            "```json",
            _json_block(payload.get("no_effect_boundary", {})),
            "```",
            "",
        ]
    )


def _render_section_markdown(title: str, section: object) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            "```json",
            _json_block(section if isinstance(section, Mapping) else {}),
            "```",
            "",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2430 Route",
            "",
            "- source task：`TRADING-2429`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2430 must review candidate promotion evidence after "
            "paper-shadow dry-run, schedule dry-run, manual review packet, "
            "observe-only signal boundary, and forward outcome binding boundary "
            "artifacts are all available. TRADING-2429 does not generate a real "
            "signal, backfill a real outcome, produce trading advice, activate "
            "paper-shadow, mutate production, or create broker orders.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
