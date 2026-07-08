from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_false_risk_off_missed_upside_batch_screen as m2433,
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
    growth_tilt_defensive_limited_adjustment_component_validation as validation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2434"
TASK_REGISTER_ID = (
    "TRADING-2434_GROWTH_TILT_DEFENSIVE_LIMITED_ADJUSTMENT_COMPONENT_VALIDATION"
)
REPORT_TYPE = validation.REPORT_TYPE
SCHEMA_VERSION = validation.SCHEMA_VERSION
READY_STATUS = validation.READY_STATUS
BLOCKED_STATUS = validation.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_DEFENSIVE_LIMITED_ADJUSTMENT_COMPONENT_VALIDATION_PRIOR_"
    "ARTIFACTS_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "market_data_component_validation_run",
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
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "broker_order_generated",
    "daily_report_generated",
    "daily_report_run",
    "new_feature_generated",
    "new_signal_generated",
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "actionable_allocation_generated",
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "scoring_run",
    "fresh_market_data_read",
    "backtest_run",
    "portfolio_weight_mutated",
    "automatic_execution_allowed",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH = m2433.DEFAULT_OUTPUT_ROOT / "batch_screen_result.json"
DEFAULT_BATCH_SCREEN_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_false_risk_off_missed_upside_batch_screen.md"
)
DEFAULT_CANDIDATE_SCREEN_MATRIX_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_defensive_limited_adjustment_component_validation(
    *,
    source_2433_batch_screen_path: Path = DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH,
    batch_screen_doc_path: Path = DEFAULT_BATCH_SCREEN_DOC_PATH,
    candidate_screen_matrix_doc_path: Path = DEFAULT_CANDIDATE_SCREEN_MATRIX_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2433_batch_screen": _load_json_document(source_2433_batch_screen_path),
        "batch_screen_doc": _load_text_document(batch_screen_doc_path),
        "candidate_screen_matrix_doc": _load_text_document(
            candidate_screen_matrix_doc_path
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
        payload = validation.build_growth_tilt_defensive_limited_adjustment_component_validation(
            _as_mapping(sources["source_2433_batch_screen"]),
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
    return [
        f"{name} missing: {document.get('_path')}"
        for name, document in sources.items()
        if isinstance(document, Mapping) and document.get("_missing") is True
    ]


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
            "classification": "component_validation_source_gap",
            "gap": (
                "Required TRADING-2433 artifact, docs, registry, catalog, or "
                "system flow is missing."
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
        "source_2433_ready": False,
        "source_candidate_found": False,
        "component_validation_ready": False,
        "component_value_assessment_ready": False,
        "primary_value_matrix_ready": False,
        "validation_boundary_ready": False,
        "component_value_found": False,
        "candidate_status": "needs_pit",
        "promotion_candidate_found": False,
        "promotion_candidate_count": 0,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "component_value_assessment": {
            "schema_version": validation.COMPONENT_VALUE_ASSESSMENT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "component_value_assessment_ready": False,
        },
        "primary_value_matrix": {
            "schema_version": validation.PRIMARY_VALUE_MATRIX_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "primary_value_matrix_ready": False,
        },
        "validation_boundary": {
            "schema_version": validation.VALIDATION_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "validation_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": validation.BLOCKED_ROUTE,
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "component_validation_result.json"
    assessment_json_path = output_root / "component_value_assessment.json"
    primary_json_path = output_root / "primary_value_matrix.json"
    boundary_json_path = output_root / "validation_boundary.json"
    markdown_path = (
        docs_root / "growth_tilt_defensive_limited_adjustment_component_validation.md"
    )
    assessment_markdown_path = (
        docs_root
        / "growth_tilt_defensive_limited_adjustment_component_value_assessment.md"
    )
    primary_markdown_path = (
        docs_root / "growth_tilt_defensive_limited_adjustment_primary_value_matrix.md"
    )
    boundary_markdown_path = (
        docs_root / "growth_tilt_defensive_limited_adjustment_validation_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2435_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "component_value_assessment_json": str(assessment_json_path),
        "primary_value_matrix_json": str(primary_json_path),
        "validation_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "component_value_assessment_markdown": str(assessment_markdown_path),
        "primary_value_matrix_markdown": str(primary_markdown_path),
        "validation_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        assessment_json_path,
        "growth_tilt_defensive_limited_adjustment_component_value_assessment",
        validation.COMPONENT_VALUE_ASSESSMENT_SCHEMA_VERSION,
        payload,
        "component_value_assessment",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        primary_json_path,
        "growth_tilt_defensive_limited_adjustment_primary_value_matrix",
        validation.PRIMARY_VALUE_MATRIX_SCHEMA_VERSION,
        payload,
        "primary_value_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_defensive_limited_adjustment_validation_boundary",
        validation.VALIDATION_BOUNDARY_SCHEMA_VERSION,
        payload,
        "validation_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        assessment_markdown_path,
        _render_section_markdown(
            "Growth Tilt Defensive Limited Adjustment Component Value Assessment",
            payload.get("component_value_assessment"),
        ),
    )
    write_markdown_artifact(
        primary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Defensive Limited Adjustment Primary Value Matrix",
            payload.get("primary_value_matrix"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Defensive Limited Adjustment Validation Boundary",
            payload.get("validation_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "component_value_found": payload.get("component_value_found"),
        "candidate_status": payload.get("candidate_status"),
        "primary_value": payload.get("primary_value"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Defensive Limited Adjustment Component Validation",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- component value found：`{payload.get('component_value_found')}`",
            f"- candidate status：`{payload.get('candidate_status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2434 只验证 component value，不读取 fresh market data，不运行 "
            "PIT replay、backtest 或 scoring，不批准 paper-shadow、production 或 broker。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Component Value Assessment",
            "",
            "```json",
            _json_block(payload.get("component_value_assessment", {})),
            "```",
            "",
            "## Validation Boundary",
            "",
            "```json",
            _json_block(payload.get("validation_boundary", {})),
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
            "# Dynamic Strategy TRADING-2435 Route",
            "",
            "- source task：`TRADING-2434`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2435 should study valid-until outcome hit-rate. TRADING-2434 "
            "does not read fresh market data, run PIT replay, backtest, scoring, "
            "paper-shadow, production, broker, new signal, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
