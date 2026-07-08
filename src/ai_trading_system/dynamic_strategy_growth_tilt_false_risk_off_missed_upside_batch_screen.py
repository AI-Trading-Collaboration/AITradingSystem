from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import dynamic_strategy_growth_tilt_candidate_gauntlet_harness as m2432
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
    growth_tilt_false_risk_off_missed_upside_batch_screen as screen,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2433"
TASK_REGISTER_ID = "TRADING-2433_GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN"
REPORT_TYPE = screen.REPORT_TYPE
SCHEMA_VERSION = screen.SCHEMA_VERSION
READY_STATUS = screen.READY_STATUS
BLOCKED_STATUS = screen.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_PRIOR_ARTIFACTS_"
    "CONFIG_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "read_fresh_cached_market_data",
    "run_historical_screen",
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
    "set_new_investment_threshold_values",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "candidate_search_resumed",
    "market_data_candidate_screen_run",
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
    "new_strategy_backtest_run",
    "scoring_run",
    "fresh_market_data_read",
    "backtest_run",
    "portfolio_weight_mutated",
    "actual_portfolio_weights_modified",
    "automatic_execution_allowed",
    "new_investment_threshold_values_set",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_HARNESS_PATH = (
    m2432.DEFAULT_OUTPUT_ROOT / "candidate_gauntlet_result.json"
)
DEFAULT_CANDIDATE_SET_PATH = (
    PROJECT_ROOT
    / "research"
    / "configs"
    / "growth_tilt"
    / "false_risk_off_missed_upside_2433.yaml"
)
DEFAULT_CANDIDATE_GAUNTLET_HARNESS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_gauntlet_harness.md"
)
DEFAULT_CANDIDATE_SET_2432_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_set_2432.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_false_risk_off_missed_upside_batch_screen(
    *,
    source_2432_candidate_gauntlet_harness_path: Path = (
        DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_HARNESS_PATH
    ),
    candidate_set_path: Path = DEFAULT_CANDIDATE_SET_PATH,
    candidate_gauntlet_harness_doc_path: Path = (
        DEFAULT_CANDIDATE_GAUNTLET_HARNESS_DOC_PATH
    ),
    candidate_set_2432_doc_path: Path = DEFAULT_CANDIDATE_SET_2432_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2432_candidate_gauntlet_harness": _load_json_document(
            source_2432_candidate_gauntlet_harness_path
        ),
        "candidate_set": _load_yaml_document(candidate_set_path),
        "candidate_gauntlet_harness_doc": _load_text_document(
            candidate_gauntlet_harness_doc_path
        ),
        "candidate_set_2432_doc": _load_text_document(candidate_set_2432_doc_path),
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
        payload = screen.build_growth_tilt_false_risk_off_missed_upside_batch_screen(
            _as_mapping(sources["source_2432_candidate_gauntlet_harness"]),
            _as_mapping(sources["candidate_set"]),
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
            candidate_set_path=candidate_set_path,
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
    candidate_set_path: Path,
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
            "candidate_set_path": str(candidate_set_path),
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
            "classification": "false_risk_off_screen_source_gap",
            "gap": (
                "Required TRADING-2432 artifact, candidate-set config, registry, "
                "catalog, system flow, or research doc is missing."
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
        "source_2432_ready": False,
        "candidate_set_ready": False,
        "candidate_set_id": "",
        "batch_screen_ready": False,
        "candidate_screen_matrix_ready": False,
        "batch_decision_summary_ready": False,
        "research_question_coverage_ready": False,
        "no_effect_boundary_ready": False,
        "candidate_count": 0,
        "candidates_screened": 0,
        "rejected_count": 0,
        "component_value_count": 0,
        "pit_candidate_count": 0,
        "promotion_candidate_count": 0,
        "promotion_candidate_found": False,
        "candidate_batch_screen_run": False,
        "screen_contract_gap_count": len(gaps),
        "screen_contract_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "candidate_screen_matrix": {
            "schema_version": screen.CANDIDATE_SCREEN_MATRIX_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "candidate_screen_matrix_ready": False,
            "candidate_count": 0,
            "candidates": [],
        },
        "batch_decision_summary": {
            "schema_version": screen.BATCH_DECISION_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "batch_decision_summary_ready": False,
        },
        "research_question_coverage": {
            "schema_version": screen.RESEARCH_QUESTION_COVERAGE_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "research_question_coverage_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": screen.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": screen.BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Required source artifacts or candidate-set config are missing; "
            "false risk-off / missed upside screen cannot silently pass."
        ),
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
        candidate_set_path=DEFAULT_CANDIDATE_SET_PATH,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "batch_screen_result.json"
    matrix_json_path = output_root / "candidate_screen_matrix.json"
    summary_json_path = output_root / "batch_decision_summary.json"
    coverage_json_path = output_root / "research_question_coverage.json"
    no_effect_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_false_risk_off_missed_upside_batch_screen.md"
    matrix_markdown_path = (
        docs_root
        / "growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix.md"
    )
    summary_markdown_path = (
        docs_root / "growth_tilt_false_risk_off_missed_upside_batch_decision_summary.md"
    )
    coverage_markdown_path = (
        docs_root
        / "growth_tilt_false_risk_off_missed_upside_research_question_coverage.md"
    )
    no_effect_markdown_path = (
        docs_root / "growth_tilt_false_risk_off_missed_upside_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2434_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_screen_matrix_json": str(matrix_json_path),
        "batch_decision_summary_json": str(summary_json_path),
        "research_question_coverage_json": str(coverage_json_path),
        "no_effect_boundary_json": str(no_effect_json_path),
        "markdown_path": str(markdown_path),
        "candidate_screen_matrix_markdown": str(matrix_markdown_path),
        "batch_decision_summary_markdown": str(summary_markdown_path),
        "research_question_coverage_markdown": str(coverage_markdown_path),
        "no_effect_boundary_markdown": str(no_effect_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix",
        screen.CANDIDATE_SCREEN_MATRIX_SCHEMA_VERSION,
        payload,
        "candidate_screen_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        summary_json_path,
        "growth_tilt_false_risk_off_missed_upside_batch_decision_summary",
        screen.BATCH_DECISION_SUMMARY_SCHEMA_VERSION,
        payload,
        "batch_decision_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        coverage_json_path,
        "growth_tilt_false_risk_off_missed_upside_research_question_coverage",
        screen.RESEARCH_QUESTION_COVERAGE_SCHEMA_VERSION,
        payload,
        "research_question_coverage",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        no_effect_json_path,
        "growth_tilt_false_risk_off_missed_upside_no_effect_boundary",
        screen.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt False Risk-Off Missed Upside Candidate Screen Matrix",
            payload.get("candidate_screen_matrix"),
        ),
    )
    write_markdown_artifact(
        summary_markdown_path,
        _render_section_markdown(
            "Growth Tilt False Risk-Off Missed Upside Batch Decision Summary",
            payload.get("batch_decision_summary"),
        ),
    )
    write_markdown_artifact(
        coverage_markdown_path,
        _render_section_markdown(
            "Growth Tilt False Risk-Off Missed Upside Research Question Coverage",
            payload.get("research_question_coverage"),
        ),
    )
    write_markdown_artifact(
        no_effect_markdown_path,
        _render_section_markdown(
            "Growth Tilt False Risk-Off Missed Upside No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "candidate_set_id": payload.get("candidate_set_id"),
        "candidate_count": payload.get("candidate_count"),
        "rejected_count": payload.get("rejected_count"),
        "component_value_count": payload.get("component_value_count"),
        "pit_candidate_count": payload.get("pit_candidate_count"),
        "promotion_candidate_count": payload.get("promotion_candidate_count"),
        "candidate_batch_screen_run": payload.get("candidate_batch_screen_run"),
        "market_data_candidate_screen_run": payload.get("market_data_candidate_screen_run"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt False Risk-Off Missed Upside Batch Screen",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- candidate set id：`{payload.get('candidate_set_id')}`",
            f"- candidate count：`{payload.get('candidate_count')}`",
            f"- promotion candidate count：`{payload.get('promotion_candidate_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2433 只按 governed candidate-set 做 research-only candidate triage，"
            "不读取 fresh market data，不运行 historical screen、PIT replay、backtest 或 scoring。"
            "默认不批准 paper-shadow、schedule、production 或 broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Screen Matrix",
            "",
            "```json",
            _json_block(payload.get("candidate_screen_matrix", {})),
            "```",
            "",
            "## Research Question Coverage",
            "",
            "```json",
            _json_block(payload.get("research_question_coverage", {})),
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
            "# Dynamic Strategy TRADING-2434 Route",
            "",
            "- source task：`TRADING-2433`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2434 should validate defensive_limited_adjustment component value. "
            "TRADING-2433 does not read fresh market data, run PIT replay, backtest, "
            "scoring, paper-shadow, production, broker, new signal, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
