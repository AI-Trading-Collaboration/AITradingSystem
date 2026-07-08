from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_candidate_promotion_evidence_review as m2430,
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
    growth_tilt_existing_candidate_evidence_matrix as matrix,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2431"
TASK_REGISTER_ID = "TRADING-2431_GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX"
REPORT_TYPE = matrix.REPORT_TYPE
SCHEMA_VERSION = matrix.SCHEMA_VERSION
READY_STATUS = matrix.READY_STATUS
BLOCKED_STATUS = matrix.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_EXISTING_CANDIDATE_EVIDENCE_MATRIX_PRIOR_ARTIFACTS_DOCS_"
    "AND_REGISTRY_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "read_fresh_cached_market_data",
    "run_historical_screen",
    "run_candidate_gauntlet",
    "run_pit_replay",
    "run_backtest",
    "run_scoring",
    "generate_daily_report",
    "generate_real_signal",
    "backfill_real_outcome",
    "bind_real_outcome",
    "generate_trading_advice",
    "generate_actionable_allocation_change",
    "generate_broker_order",
    "modify_actual_portfolio_weights",
    "enable_paper_shadow",
    "enable_paper_shadow_schedule",
    "create_scheduled_task",
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
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "candidate_gauntlet_run",
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
DEFAULT_SOURCE_2430_PROMOTION_REVIEW_RESULT_PATH = (
    m2430.DEFAULT_OUTPUT_ROOT / "promotion_evidence_review_result.json"
)
DEFAULT_CANDIDATE_REGISTRY_PATH = m2430.DEFAULT_CANDIDATE_REGISTRY_PATH
DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH = m2430.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH
DEFAULT_PRIOR_COMPONENT_VALUE_MATRIX_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "dynamic_strategy_component_attribution_gate_evidence_plan"
    / "component_value_matrix.json"
)
DEFAULT_COMPONENT_VALUE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dynamic_strategy_component_value_matrix.md"
)
DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dynamic_strategy_candidate_owner_review_record.md"
)
DEFAULT_CANDIDATE_RECLASSIFICATION_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "dynamic_strategy_calibrated_gate_candidate_reclassification.md"
)
DEFAULT_EXECUTION_SEMANTICS_REVIEW_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "execution_semantics_actual_path_rebacktest_review.md"
)
DEFAULT_GROWTH_TILT_SIGNAL_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_engine_signal_to_outcome_linkage.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_existing_candidate_evidence_matrix(
    *,
    source_2430_promotion_review_result_path: Path = (
        DEFAULT_SOURCE_2430_PROMOTION_REVIEW_RESULT_PATH
    ),
    candidate_registry_path: Path = DEFAULT_CANDIDATE_REGISTRY_PATH,
    prior_candidate_evidence_path: Path = DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH,
    prior_component_value_matrix_path: Path = DEFAULT_PRIOR_COMPONENT_VALUE_MATRIX_PATH,
    component_value_doc_path: Path = DEFAULT_COMPONENT_VALUE_DOC_PATH,
    prior_candidate_evidence_doc_path: Path = DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH,
    candidate_reclassification_doc_path: Path = (
        DEFAULT_CANDIDATE_RECLASSIFICATION_DOC_PATH
    ),
    execution_semantics_review_doc_path: Path = (
        DEFAULT_EXECUTION_SEMANTICS_REVIEW_DOC_PATH
    ),
    growth_tilt_signal_doc_path: Path = DEFAULT_GROWTH_TILT_SIGNAL_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2430_promotion_review": _load_json_document(
            source_2430_promotion_review_result_path
        ),
        "candidate_registry": _load_yaml_document(candidate_registry_path),
        "prior_candidate_evidence": _load_json_document(prior_candidate_evidence_path),
        "prior_component_value_matrix": _load_json_document(
            prior_component_value_matrix_path
        ),
        "component_value_doc": _load_text_document(component_value_doc_path),
        "prior_candidate_evidence_doc": _load_text_document(
            prior_candidate_evidence_doc_path
        ),
        "candidate_reclassification_doc": _load_text_document(
            candidate_reclassification_doc_path
        ),
        "execution_semantics_review_doc": _load_text_document(
            execution_semantics_review_doc_path
        ),
        "growth_tilt_signal_doc": _load_text_document(growth_tilt_signal_doc_path),
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
        payload = matrix.build_growth_tilt_existing_candidate_evidence_matrix(
            _as_mapping(sources["source_2430_promotion_review"]),
            _as_mapping(sources["candidate_registry"]),
            _as_mapping(sources["prior_candidate_evidence"]),
            _as_mapping(sources["prior_component_value_matrix"]),
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
            "classification": "existing_candidate_evidence_gap",
            "gap": (
                "Required prior promotion review, candidate registry, prior "
                "candidate evidence, component matrix, registry, catalog, "
                "system flow, or research doc is missing."
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
        "source_2430_ready": False,
        "candidate_registry_ready": False,
        "prior_candidate_evidence_ready": False,
        "component_value_evidence_ready": False,
        "existing_candidate_evidence_matrix_ready": False,
        "candidate_status_summary_ready": False,
        "candidate_metric_coverage_ready": False,
        "no_effect_boundary_ready": False,
        "candidate_count": 0,
        "required_candidate_group_count": len(matrix.REQUIRED_CANDIDATE_GROUPS),
        "rejected_count": 0,
        "component_value_count": 0,
        "needs_pit_count": 0,
        "promotion_candidate_count": 0,
        "promotion_candidate_found": False,
        "metric_coverage_available_count": 0,
        "metric_coverage_partial_count": 0,
        "metric_coverage_missing_count": 0,
        "engineering_readiness_is_alpha_evidence": False,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "candidate_gauntlet_run": False,
        "requirements": [],
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "candidate_evidence_matrix": {
            "schema_version": matrix.CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "candidate_evidence_matrix_ready": False,
            "candidate_count": 0,
            "candidates": [],
        },
        "candidate_status_summary": {
            "schema_version": matrix.CANDIDATE_STATUS_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "candidate_status_summary_ready": False,
        },
        "candidate_metric_coverage": {
            "schema_version": matrix.CANDIDATE_METRIC_COVERAGE_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "candidate_metric_coverage_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": matrix.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": matrix.BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Required source artifacts or research docs are missing; existing "
            "candidate matrix cannot silently pass."
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
    json_path = output_root / "existing_candidate_evidence_matrix_result.json"
    matrix_json_path = output_root / "candidate_evidence_matrix.json"
    summary_json_path = output_root / "candidate_status_summary.json"
    metric_json_path = output_root / "candidate_metric_coverage.json"
    no_effect_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_existing_candidate_evidence_matrix.md"
    matrix_markdown_path = (
        docs_root / "growth_tilt_existing_candidate_evidence_matrix_table.md"
    )
    summary_markdown_path = (
        docs_root / "growth_tilt_existing_candidate_status_summary.md"
    )
    metric_markdown_path = docs_root / "growth_tilt_existing_candidate_metric_coverage.md"
    no_effect_markdown_path = (
        docs_root / "growth_tilt_existing_candidate_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2432_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_evidence_matrix_json": str(matrix_json_path),
        "candidate_status_summary_json": str(summary_json_path),
        "candidate_metric_coverage_json": str(metric_json_path),
        "no_effect_boundary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "candidate_evidence_matrix_markdown": str(matrix_markdown_path),
        "candidate_status_summary_markdown": str(summary_markdown_path),
        "candidate_metric_coverage_markdown": str(metric_markdown_path),
        "no_effect_boundary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_existing_candidate_evidence_matrix_table",
        matrix.CANDIDATE_EVIDENCE_MATRIX_SCHEMA_VERSION,
        payload,
        "candidate_evidence_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        summary_json_path,
        "growth_tilt_existing_candidate_status_summary",
        matrix.CANDIDATE_STATUS_SUMMARY_SCHEMA_VERSION,
        payload,
        "candidate_status_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        metric_json_path,
        "growth_tilt_existing_candidate_metric_coverage",
        matrix.CANDIDATE_METRIC_COVERAGE_SCHEMA_VERSION,
        payload,
        "candidate_metric_coverage",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_existing_candidate_no_effect_boundary",
        matrix.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Existing Candidate Evidence Matrix",
            payload.get("candidate_evidence_matrix"),
        ),
    )
    write_markdown_artifact(
        summary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Existing Candidate Status Summary",
            payload.get("candidate_status_summary"),
        ),
    )
    write_markdown_artifact(
        metric_markdown_path,
        _render_section_markdown(
            "Growth Tilt Existing Candidate Metric Coverage",
            payload.get("candidate_metric_coverage"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt Existing Candidate No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "existing_candidate_evidence_matrix_ready": payload.get(
            "existing_candidate_evidence_matrix_ready"
        ),
        "candidate_count": payload.get("candidate_count"),
        "component_value_count": payload.get("component_value_count"),
        "needs_pit_count": payload.get("needs_pit_count"),
        "promotion_candidate_count": payload.get("promotion_candidate_count"),
        "promotion_candidate_found": payload.get("promotion_candidate_found"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Existing Candidate Evidence Matrix",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            "- existing candidate evidence matrix ready："
            f"`{payload.get('existing_candidate_evidence_matrix_ready')}`",
            f"- candidate count：`{payload.get('candidate_count')}`",
            f"- component value count：`{payload.get('component_value_count')}`",
            f"- needs PIT count：`{payload.get('needs_pit_count')}`",
            f"- promotion candidate count：`{payload.get('promotion_candidate_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2431 只整理已有候选证据，不运行新的 market-data experiment、"
            "historical screen、PIT replay、gauntlet、backtest 或 scoring。默认不批准 "
            "paper-shadow、schedule、production 或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Matrix",
            "",
            "```json",
            _json_block(payload.get("candidate_evidence_matrix", {})),
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
            "# Dynamic Strategy TRADING-2432 Route",
            "",
            "- source task：`TRADING-2431`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2432 should build the Growth Tilt Candidate Gauntlet Harness. "
            "TRADING-2431 does not run the gauntlet, PIT replay, market-data "
            "experiment, backtest, scoring, paper-shadow, production, broker, "
            "new signal, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
