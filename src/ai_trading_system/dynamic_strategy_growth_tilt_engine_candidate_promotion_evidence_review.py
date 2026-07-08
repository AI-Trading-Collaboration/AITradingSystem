from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_forward_outcome_binding_boundary as m2429,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_manual_review_packet_dry_run as m2427,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_observe_only_signal_artifact_boundary as m2428,
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
    growth_tilt_engine_candidate_promotion_evidence_review as review,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2430"
TASK_REGISTER_ID = (
    "TRADING-2430_GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW"
)
REPORT_TYPE = review.REPORT_TYPE
SCHEMA_VERSION = review.SCHEMA_VERSION
NO_PROMOTION_STATUS = review.NO_PROMOTION_STATUS
PROMOTION_CANDIDATE_FOUND_STATUS = review.PROMOTION_CANDIDATE_FOUND_STATUS
BLOCKED_STATUS = review.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_PRIOR_ARTIFACTS_"
    "CANDIDATE_REGISTRY_PRIOR_EVIDENCE_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "enable_paper_shadow",
    "enable_paper_shadow_schedule",
    "create_scheduled_task",
    "generate_real_signal",
    "backfill_real_outcome",
    "bind_real_outcome",
    "generate_trading_advice",
    "generate_actionable_allocation_change",
    "generate_broker_order",
    "modify_actual_portfolio_weights",
    "read_fresh_cached_market_data",
    "run_backtest",
    "run_scoring",
    "generate_daily_report",
    "enable_production",
    "call_broker_api",
    "send_order",
    "treat_engineering_readiness_as_alpha_evidence",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "candidate_search_resumed",
    "observation_enabled",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_shadow_approved",
    "paper_shadow_schedule_enabled",
    "paper_shadow_daily_job_run",
    "scheduler_enabled",
    "scheduled_task_created",
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

DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH = (
    m2426.DEFAULT_OUTPUT_ROOT / "schedule_dry_run_result.json"
)
DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH = (
    m2427.DEFAULT_OUTPUT_ROOT / "manual_review_packet_dry_run_result.json"
)
DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH = (
    m2428.DEFAULT_OUTPUT_ROOT / "observe_only_signal_artifact_boundary_result.json"
)
DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_RESULT_PATH = (
    m2429.DEFAULT_OUTPUT_ROOT / "forward_outcome_binding_boundary_result.json"
)
DEFAULT_CANDIDATE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "equal_risk_growth_tilt_candidate_registry.yaml"
)
DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "dynamic_strategy_calibrated_gate_candidate_owner_review_decision"
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH = (
    m2426.DEFAULT_DOCS_ROOT / "growth_tilt_engine_paper_shadow_schedule_dry_run.md"
)
DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH = (
    m2427.DEFAULT_DOCS_ROOT / "growth_tilt_engine_manual_review_packet_dry_run.md"
)
DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH = (
    m2428.DEFAULT_DOCS_ROOT / "growth_tilt_engine_observe_only_signal_artifact_boundary.md"
)
DEFAULT_SOURCE_2429_RESEARCH_DOC_PATH = (
    m2429.DEFAULT_DOCS_ROOT / "growth_tilt_engine_forward_outcome_binding_boundary.md"
)
DEFAULT_SOURCE_2429_ROUTE_DOC_PATH = (
    m2429.DEFAULT_DOCS_ROOT / "dynamic_strategy_2430_route.md"
)
DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dynamic_strategy_candidate_owner_review_record.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_engine_candidate_promotion_evidence_review(
    *,
    source_2426_schedule_dry_run_result_path: Path = (
        DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH
    ),
    source_2427_manual_review_packet_dry_run_result_path: Path = (
        DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH
    ),
    source_2428_observe_only_boundary_result_path: Path = (
        DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH
    ),
    source_2429_forward_outcome_boundary_result_path: Path = (
        DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_RESULT_PATH
    ),
    candidate_registry_path: Path = DEFAULT_CANDIDATE_REGISTRY_PATH,
    prior_candidate_evidence_path: Path = DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH,
    source_2426_research_doc_path: Path = DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH,
    source_2427_research_doc_path: Path = DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH,
    source_2428_research_doc_path: Path = DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH,
    source_2429_research_doc_path: Path = DEFAULT_SOURCE_2429_RESEARCH_DOC_PATH,
    source_2429_route_doc_path: Path = DEFAULT_SOURCE_2429_ROUTE_DOC_PATH,
    prior_candidate_evidence_doc_path: Path = (
        DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH
    ),
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
        "source_2427_manual_review_packet_dry_run_result": _load_json_document(
            source_2427_manual_review_packet_dry_run_result_path
        ),
        "source_2428_observe_only_boundary_result": _load_json_document(
            source_2428_observe_only_boundary_result_path
        ),
        "source_2429_forward_outcome_boundary_result": _load_json_document(
            source_2429_forward_outcome_boundary_result_path
        ),
        "candidate_registry": _load_yaml_document(candidate_registry_path),
        "prior_candidate_evidence": _load_json_document(prior_candidate_evidence_path),
        "source_2426_research_doc": _load_text_document(
            source_2426_research_doc_path
        ),
        "source_2427_research_doc": _load_text_document(
            source_2427_research_doc_path
        ),
        "source_2428_research_doc": _load_text_document(
            source_2428_research_doc_path
        ),
        "source_2429_research_doc": _load_text_document(
            source_2429_research_doc_path
        ),
        "source_2429_route_doc": _load_text_document(source_2429_route_doc_path),
        "prior_candidate_evidence_doc": _load_text_document(
            prior_candidate_evidence_doc_path
        ),
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
        payload = review.build_growth_tilt_engine_candidate_promotion_evidence_review(
            _as_mapping(sources["source_2426_schedule_dry_run_result"]),
            _as_mapping(sources["source_2427_manual_review_packet_dry_run_result"]),
            _as_mapping(sources["source_2428_observe_only_boundary_result"]),
            _as_mapping(sources["source_2429_forward_outcome_boundary_result"]),
            _as_mapping(sources["candidate_registry"]),
            _as_mapping(sources["prior_candidate_evidence"]),
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
            "classification": "missing_promotion_review_evidence",
            "gap": (
                "Required prior artifact, candidate registry, prior evidence, "
                "registry, catalog, system flow, or doc is missing."
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
        "target_strategy_id": review.TARGET_STRATEGY_ID,
        "source_tasks": ["TRADING-2426", "TRADING-2427", "TRADING-2428", "TRADING-2429"],
        "schedule_dry_run_ready": False,
        "manual_review_packet_dry_run_ready": False,
        "observe_only_signal_artifact_boundary_ready": False,
        "forward_outcome_binding_boundary_ready": False,
        "candidate_registry_ready": False,
        "prior_candidate_evidence_ready": False,
        "promotion_evidence_review_started": False,
        "promotion_evidence_review_completed": False,
        "promotion_evidence_review_ready": False,
        "promotion_candidate_found": False,
        "promotion_candidate_count": 0,
        "candidate_count": 0,
        "candidate_evidence_matrix_ready": False,
        "candidate_decision_summary_ready": False,
        "no_promotion_rationale_ready": False,
        "engineering_readiness_is_alpha_evidence": False,
        "paper_shadow_promotion_allowed_by_registry": False,
        "prior_owner_approved_paper_shadow": False,
        "prior_owner_approved_observation": False,
        "promotion_evidence_review_gap_count": len(gaps),
        "promotion_evidence_review_gap_ids": ["source_artifact_availability"],
        "missing_promotion_review_evidence_count": len(gaps),
        "safety_boundary_gap_count": 0,
        "candidate_evidence_gap_count": 0,
        "precondition_gap_count": 0,
        "requirements": [],
        "gaps": gaps,
        "candidate_evidence_matrix": {
            "schema_version": review.CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "candidate_evidence_matrix_ready": False,
            "candidates": [],
        },
        "candidate_decision_summary": {
            "schema_version": review.CANDIDATE_DECISION_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "promotion_evidence_review_ready": False,
        },
        "no_promotion_rationale": {
            "schema_version": review.NO_PROMOTION_RATIONALE_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_promotion_rationale_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": review.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": review.NEXT_ROUTE_BLOCKED,
        "next_route_if_no_candidate": review.NEXT_ROUTE_NO_CANDIDATE,
        "next_route_if_candidate_found": review.NEXT_ROUTE_CANDIDATE_FOUND,
        "recommended_next_research_task_reason": (
            "Required prior artifacts or documents are missing; promotion "
            "evidence review cannot silently pass."
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
    json_path = output_root / "promotion_evidence_review_result.json"
    matrix_json_path = output_root / "candidate_evidence_matrix.json"
    decision_json_path = output_root / "candidate_decision_summary.json"
    rationale_json_path = output_root / "no_promotion_rationale.json"
    no_effect_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_engine_candidate_promotion_evidence_review.md"
    matrix_markdown_path = docs_root / "growth_tilt_engine_candidate_evidence_matrix.md"
    decision_markdown_path = (
        docs_root / "growth_tilt_engine_candidate_decision_summary.md"
    )
    rationale_markdown_path = docs_root / "growth_tilt_engine_no_promotion_rationale.md"
    no_effect_markdown_path = (
        docs_root / "growth_tilt_engine_candidate_promotion_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2431_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_evidence_matrix_json": str(matrix_json_path),
        "candidate_decision_summary_json": str(decision_json_path),
        "no_promotion_rationale_json": str(rationale_json_path),
        "no_effect_boundary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "candidate_evidence_matrix_markdown": str(matrix_markdown_path),
        "candidate_decision_summary_markdown": str(decision_markdown_path),
        "no_promotion_rationale_markdown": str(rationale_markdown_path),
        "no_effect_boundary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_engine_candidate_evidence_matrix",
        review.CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION,
        payload,
        "candidate_evidence_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        decision_json_path,
        "growth_tilt_engine_candidate_decision_summary",
        review.CANDIDATE_DECISION_SUMMARY_SCHEMA_VERSION,
        payload,
        "candidate_decision_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        rationale_json_path,
        "growth_tilt_engine_no_promotion_rationale",
        review.NO_PROMOTION_RATIONALE_SCHEMA_VERSION,
        payload,
        "no_promotion_rationale",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_engine_candidate_promotion_no_effect_boundary",
        review.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Candidate Evidence Matrix",
            payload.get("candidate_evidence_matrix"),
        ),
    )
    write_markdown_artifact(
        decision_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Candidate Decision Summary",
            payload.get("candidate_decision_summary"),
        ),
    )
    write_markdown_artifact(
        rationale_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine No-Promotion Rationale",
            payload.get("no_promotion_rationale"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt Engine Candidate Promotion No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "promotion_evidence_review_ready": payload.get(
            "promotion_evidence_review_ready"
        ),
        "promotion_candidate_found": payload.get("promotion_candidate_found"),
        "promotion_candidate_count": payload.get("promotion_candidate_count"),
        "engineering_readiness_is_alpha_evidence": payload.get(
            "engineering_readiness_is_alpha_evidence"
        ),
        "paper_shadow_enabled": payload.get("paper_shadow_enabled"),
        "production_enabled": payload.get("production_enabled"),
        "broker_enabled": payload.get("broker_enabled"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Candidate Promotion Evidence Review",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            "- promotion evidence review ready："
            f"`{payload.get('promotion_evidence_review_ready')}`",
            f"- promotion candidate found：`{payload.get('promotion_candidate_found')}`",
            f"- promotion candidate count：`{payload.get('promotion_candidate_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2430 只复核候选晋级证据。工程 readiness 不等于 alpha evidence；"
            "本任务不启用 paper-shadow、schedule、production 或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Decision Summary",
            "",
            "```json",
            _json_block(payload.get("candidate_decision_summary", {})),
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
            "# Dynamic Strategy TRADING-2431 Route",
            "",
            "- source task：`TRADING-2430`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2431 must continue with existing candidate evidence matrix "
            "when no paper-shadow promotion candidate is present. If a future "
            "run finds a candidate, the route may switch to candidate-specific "
            "paper-shadow gate. TRADING-2430 does not approve paper-shadow, "
            "schedule, production, broker, new signal, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
