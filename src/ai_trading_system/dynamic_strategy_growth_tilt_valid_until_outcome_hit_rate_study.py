from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_defensive_limited_adjustment_component_validation as m2434,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_forward_outcome_binding_boundary as m2429,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_valid_until_dependency_evidence_closure as m2418,
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
    growth_tilt_valid_until_outcome_hit_rate_study as study,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2435"
TASK_REGISTER_ID = "TRADING-2435_GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY"
REPORT_TYPE = study.REPORT_TYPE
SCHEMA_VERSION = study.SCHEMA_VERSION
READY_STATUS = study.READY_STATUS
BLOCKED_STATUS = study.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_PRIOR_ARTIFACTS_CONFIG_"
    "DOCS_ONLY_NO_FRESH_MARKET_OR_OUTCOME_DATA"
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "market_data_hit_rate_study_run",
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "scoring_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "backtest_run",
    "computed_new_metrics",
    "real_outcome_binding_run",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
    "outcome_store_mutated",
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
    "portfolio_weight_mutated",
    "automatic_execution_allowed",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2434_COMPONENT_VALIDATION_PATH = (
    m2434.DEFAULT_OUTPUT_ROOT / "component_validation_result.json"
)
DEFAULT_SOURCE_2418_VALID_UNTIL_ALIGNMENT_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "growth_tilt_valid_until_alignment_evidence.json"
)
DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "stale_signal_policy_evidence.json"
)
DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_PATH = (
    m2429.DEFAULT_OUTPUT_ROOT / "forward_outcome_binding_boundary_result.json"
)
DEFAULT_CANDIDATE_SET_2432_PATH = (
    PROJECT_ROOT / "research" / "configs" / "growth_tilt" / "candidate_set_2432.yaml"
)
DEFAULT_COMPONENT_VALIDATION_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_defensive_limited_adjustment_component_validation.md"
)
DEFAULT_VALID_UNTIL_ALIGNMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_engine_valid_until_alignment_evidence.md"
)
DEFAULT_FORWARD_OUTCOME_BOUNDARY_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_engine_forward_outcome_binding_boundary.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_valid_until_outcome_hit_rate_study(
    *,
    source_2434_component_validation_path: Path = (
        DEFAULT_SOURCE_2434_COMPONENT_VALIDATION_PATH
    ),
    source_2418_valid_until_alignment_path: Path = (
        DEFAULT_SOURCE_2418_VALID_UNTIL_ALIGNMENT_PATH
    ),
    source_2418_stale_signal_policy_path: Path = (
        DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_PATH
    ),
    source_2429_forward_outcome_boundary_path: Path = (
        DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_PATH
    ),
    candidate_set_2432_path: Path = DEFAULT_CANDIDATE_SET_2432_PATH,
    component_validation_doc_path: Path = DEFAULT_COMPONENT_VALIDATION_DOC_PATH,
    valid_until_alignment_doc_path: Path = DEFAULT_VALID_UNTIL_ALIGNMENT_DOC_PATH,
    forward_outcome_boundary_doc_path: Path = DEFAULT_FORWARD_OUTCOME_BOUNDARY_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2434_component_validation": _load_json_document(
            source_2434_component_validation_path
        ),
        "source_2418_valid_until_alignment": _load_json_document(
            source_2418_valid_until_alignment_path
        ),
        "source_2418_stale_signal_policy": _load_json_document(
            source_2418_stale_signal_policy_path
        ),
        "source_2429_forward_outcome_boundary": _load_json_document(
            source_2429_forward_outcome_boundary_path
        ),
        "candidate_set_2432": _load_yaml_document(candidate_set_2432_path),
        "component_validation_doc": _load_text_document(component_validation_doc_path),
        "valid_until_alignment_doc": _load_text_document(
            valid_until_alignment_doc_path
        ),
        "forward_outcome_boundary_doc": _load_text_document(
            forward_outcome_boundary_doc_path
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
        payload = study.build_growth_tilt_valid_until_outcome_hit_rate_study(
            _as_mapping(sources["source_2434_component_validation"]),
            _as_mapping(sources["source_2418_valid_until_alignment"]),
            _as_mapping(sources["source_2418_stale_signal_policy"]),
            _as_mapping(sources["source_2429_forward_outcome_boundary"]),
            _as_mapping(sources["candidate_set_2432"]),
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
            "classification": "valid_until_hit_rate_source_gap",
            "gap": (
                "Required prior artifact, config, docs, registry, catalog, or "
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
        "source_2434_ready": False,
        "source_2418_valid_until_evidence_ready": False,
        "source_2429_forward_outcome_boundary_ready": False,
        "candidate_set_valid_until_metric_ready": False,
        "hit_rate_study_ready": False,
        "valid_until_hit_rate_matrix_ready": False,
        "stale_signal_reduction_summary_ready": False,
        "expiry_failure_audit_ready": False,
        "no_effect_boundary_ready": False,
        "valid_until_component_value_found": False,
        "valid_until_hit_rate_delta": study.NO_OBSERVED_OUTCOME_DELTA,
        "stale_signal_reduction": study.NO_OBSERVED_OUTCOME_DELTA,
        "expiry_failure_count": study.NO_OBSERVED_EXPIRY_FAILURE_COUNT,
        "candidate_status": "needs_pit",
        "outcome_sample_count": 0,
        "observed_outcome_hit_rate_available": False,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "valid_until_hit_rate_matrix": {
            "schema_version": study.VALID_UNTIL_HIT_RATE_MATRIX_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "valid_until_hit_rate_matrix_ready": False,
        },
        "stale_signal_reduction_summary": {
            "schema_version": study.STALE_SIGNAL_REDUCTION_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "stale_signal_reduction_summary_ready": False,
        },
        "expiry_failure_audit": {
            "schema_version": study.EXPIRY_FAILURE_AUDIT_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "expiry_failure_audit_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": study.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": study.BLOCKED_ROUTE,
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "hit_rate_study_result.json"
    matrix_json_path = output_root / "valid_until_hit_rate_matrix.json"
    stale_json_path = output_root / "stale_signal_reduction_summary.json"
    expiry_json_path = output_root / "expiry_failure_audit.json"
    boundary_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_valid_until_outcome_hit_rate_study.md"
    matrix_markdown_path = docs_root / "growth_tilt_valid_until_hit_rate_matrix.md"
    stale_markdown_path = (
        docs_root / "growth_tilt_valid_until_stale_signal_reduction_summary.md"
    )
    expiry_markdown_path = docs_root / "growth_tilt_valid_until_expiry_failure_audit.md"
    boundary_markdown_path = (
        docs_root / "growth_tilt_valid_until_outcome_hit_rate_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2436_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "valid_until_hit_rate_matrix_json": str(matrix_json_path),
        "stale_signal_reduction_summary_json": str(stale_json_path),
        "expiry_failure_audit_json": str(expiry_json_path),
        "no_effect_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "valid_until_hit_rate_matrix_markdown": str(matrix_markdown_path),
        "stale_signal_reduction_summary_markdown": str(stale_markdown_path),
        "expiry_failure_audit_markdown": str(expiry_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_valid_until_hit_rate_matrix",
        study.VALID_UNTIL_HIT_RATE_MATRIX_SCHEMA_VERSION,
        payload,
        "valid_until_hit_rate_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        stale_json_path,
        "growth_tilt_valid_until_stale_signal_reduction_summary",
        study.STALE_SIGNAL_REDUCTION_SUMMARY_SCHEMA_VERSION,
        payload,
        "stale_signal_reduction_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        expiry_json_path,
        "growth_tilt_valid_until_expiry_failure_audit",
        study.EXPIRY_FAILURE_AUDIT_SCHEMA_VERSION,
        payload,
        "expiry_failure_audit",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_valid_until_outcome_hit_rate_no_effect_boundary",
        study.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Valid-Until Hit-Rate Matrix",
            payload.get("valid_until_hit_rate_matrix"),
        ),
    )
    write_markdown_artifact(
        stale_markdown_path,
        _render_section_markdown(
            "Growth Tilt Valid-Until Stale Signal Reduction Summary",
            payload.get("stale_signal_reduction_summary"),
        ),
    )
    write_markdown_artifact(
        expiry_markdown_path,
        _render_section_markdown(
            "Growth Tilt Valid-Until Expiry Failure Audit",
            payload.get("expiry_failure_audit"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Valid-Until Outcome Hit-Rate No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "valid_until_component_value_found": payload.get(
            "valid_until_component_value_found"
        ),
        "valid_until_hit_rate_delta": payload.get("valid_until_hit_rate_delta"),
        "stale_signal_reduction": payload.get("stale_signal_reduction"),
        "expiry_failure_count": payload.get("expiry_failure_count"),
        "candidate_status": payload.get("candidate_status"),
        "outcome_sample_count": payload.get("outcome_sample_count"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Valid-Until Outcome Hit-Rate Study",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            "- valid-until component value found："
            f"`{payload.get('valid_until_component_value_found')}`",
            f"- candidate status：`{payload.get('candidate_status')}`",
            f"- outcome sample count：`{payload.get('outcome_sample_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2435 只读取 prior artifacts / config / docs，不读取 fresh market "
            "or outcome data，不运行 PIT replay、backtest、scoring 或 outcome binding。"
            "0 delta 表示本任务未计算真实 outcome hit-rate，不是收益结论。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Valid-Until Hit-Rate Matrix",
            "",
            "```json",
            _json_block(payload.get("valid_until_hit_rate_matrix", {})),
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
            "# Dynamic Strategy TRADING-2436 Route",
            "",
            "- source task：`TRADING-2435`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2436 should study turnover / cooldown parameter plateau. "
            "TRADING-2435 does not read fresh market or outcome data, run PIT replay, "
            "backtest, scoring, paper-shadow, production, broker, new signal, "
            "outcome binding, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
