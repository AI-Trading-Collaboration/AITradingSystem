from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_candidate_gauntlet_harness as m2432,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_valid_until_outcome_hit_rate_study as m2435,
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
    growth_tilt_turnover_cooldown_parameter_plateau_study as plateau,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2436"
TASK_REGISTER_ID = (
    "TRADING-2436_GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY"
)
REPORT_TYPE = plateau.REPORT_TYPE
SCHEMA_VERSION = plateau.SCHEMA_VERSION
READY_STATUS = plateau.READY_STATUS
BLOCKED_STATUS = plateau.BLOCKED_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY_PRIOR_ARTIFACTS_"
    "CONFIG_DOCS_ONLY_NO_FRESH_MARKET_OR_OUTCOME_DATA_NO_PARAMETER_SWEEP"
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "parameter_sweep_run",
    "market_data_parameter_plateau_run",
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "scoring_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "backtest_run",
    "computed_new_metrics",
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
DEFAULT_SOURCE_2435_HIT_RATE_STUDY_PATH = (
    m2435.DEFAULT_OUTPUT_ROOT / "hit_rate_study_result.json"
)
DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_PATH = (
    m2432.DEFAULT_OUTPUT_ROOT / "candidate_gauntlet_result.json"
)
DEFAULT_CANDIDATE_SET_2432_PATH = (
    PROJECT_ROOT / "research" / "configs" / "growth_tilt" / "candidate_set_2432.yaml"
)
DEFAULT_HIT_RATE_STUDY_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_valid_until_outcome_hit_rate_study.md"
)
DEFAULT_CANDIDATE_GAUNTLET_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_gauntlet_harness.md"
)
DEFAULT_CANDIDATE_SET_2432_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_set_2432.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_turnover_cooldown_parameter_plateau_study(
    *,
    source_2435_hit_rate_study_path: Path = DEFAULT_SOURCE_2435_HIT_RATE_STUDY_PATH,
    source_2432_candidate_gauntlet_path: Path = (
        DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_PATH
    ),
    candidate_set_2432_path: Path = DEFAULT_CANDIDATE_SET_2432_PATH,
    hit_rate_study_doc_path: Path = DEFAULT_HIT_RATE_STUDY_DOC_PATH,
    candidate_gauntlet_doc_path: Path = DEFAULT_CANDIDATE_GAUNTLET_DOC_PATH,
    candidate_set_2432_doc_path: Path = DEFAULT_CANDIDATE_SET_2432_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2435_hit_rate_study": _load_json_document(
            source_2435_hit_rate_study_path
        ),
        "source_2432_candidate_gauntlet": _load_json_document(
            source_2432_candidate_gauntlet_path
        ),
        "candidate_set_2432": _load_yaml_document(candidate_set_2432_path),
        "hit_rate_study_doc": _load_text_document(hit_rate_study_doc_path),
        "candidate_gauntlet_doc": _load_text_document(candidate_gauntlet_doc_path),
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
        payload = plateau.build_growth_tilt_turnover_cooldown_parameter_plateau_study(
            _as_mapping(sources["source_2435_hit_rate_study"]),
            _as_mapping(sources["source_2432_candidate_gauntlet"]),
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
            "classification": "parameter_plateau_source_gap",
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
        "source_2435_ready": False,
        "source_2432_gauntlet_ready": False,
        "candidate_set_parameter_plateau_contract_ready": False,
        "candidate_set_turnover_cooldown_group_ready": False,
        "candidate_set_required_metrics_ready": False,
        "parameter_plateau_study_ready": False,
        "parameter_plateau_matrix_ready": False,
        "turnover_cooldown_check_summary_ready": False,
        "no_effect_boundary_ready": False,
        "parameter_plateau_found": False,
        "isolated_winner": False,
        "robust_region_count": 0,
        "component_value_found": False,
        "candidate_status": "needs_pit",
        "nearby_parameter_pass_count": 0,
        "turnover_delta": 0.0,
        "whipsaw_delta": 0.0,
        "missed_upside_delta": 0.0,
        "return_degradation": 0.0,
        "drawdown_degradation": 0.0,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "parameter_plateau_matrix": {
            "schema_version": plateau.PARAMETER_PLATEAU_MATRIX_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "parameter_plateau_matrix_ready": False,
        },
        "turnover_cooldown_check_summary": {
            "schema_version": plateau.TURNOVER_COOLDOWN_CHECK_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "turnover_cooldown_check_summary_ready": False,
        },
        "no_effect_boundary": {
            "schema_version": plateau.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "no_effect_boundary_ready": False,
            "gaps": gaps,
        },
        "recommended_next_research_task": plateau.BLOCKED_ROUTE,
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "parameter_plateau_study_result.json"
    matrix_json_path = output_root / "parameter_plateau_matrix.json"
    summary_json_path = output_root / "turnover_cooldown_check_summary.json"
    boundary_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_turnover_cooldown_parameter_plateau_study.md"
    matrix_markdown_path = (
        docs_root / "growth_tilt_turnover_cooldown_parameter_plateau_matrix.md"
    )
    summary_markdown_path = (
        docs_root / "growth_tilt_turnover_cooldown_check_summary.md"
    )
    boundary_markdown_path = (
        docs_root / "growth_tilt_turnover_cooldown_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2437_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "parameter_plateau_matrix_json": str(matrix_json_path),
        "turnover_cooldown_check_summary_json": str(summary_json_path),
        "no_effect_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "parameter_plateau_matrix_markdown": str(matrix_markdown_path),
        "turnover_cooldown_check_summary_markdown": str(summary_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_turnover_cooldown_parameter_plateau_matrix",
        plateau.PARAMETER_PLATEAU_MATRIX_SCHEMA_VERSION,
        payload,
        "parameter_plateau_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        summary_json_path,
        "growth_tilt_turnover_cooldown_check_summary",
        plateau.TURNOVER_COOLDOWN_CHECK_SUMMARY_SCHEMA_VERSION,
        payload,
        "turnover_cooldown_check_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_turnover_cooldown_no_effect_boundary",
        plateau.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Turnover Cooldown Parameter Plateau Matrix",
            payload.get("parameter_plateau_matrix"),
        ),
    )
    write_markdown_artifact(
        summary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Turnover Cooldown Check Summary",
            payload.get("turnover_cooldown_check_summary"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Turnover Cooldown No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "parameter_plateau_found": payload.get("parameter_plateau_found"),
        "isolated_winner": payload.get("isolated_winner"),
        "robust_region_count": payload.get("robust_region_count"),
        "component_value_found": payload.get("component_value_found"),
        "candidate_status": payload.get("candidate_status"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Turnover Cooldown Parameter Plateau Study",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- parameter plateau found：`{payload.get('parameter_plateau_found')}`",
            f"- isolated winner：`{payload.get('isolated_winner')}`",
            f"- robust region count：`{payload.get('robust_region_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2436 只读取 prior artifacts / config / docs，不读取 fresh market "
            "or outcome data，不运行 parameter sweep、PIT replay、backtest 或 scoring。"
            "platform=false 表示本任务未执行真实参数邻域验证，不是策略拒绝结论。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Parameter Plateau Matrix",
            "",
            "```json",
            _json_block(payload.get("parameter_plateau_matrix", {})),
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
            "# Dynamic Strategy TRADING-2437 Route",
            "",
            "- source task：`TRADING-2436`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2437 should run regime slice attribution review. TRADING-2436 "
            "does not read fresh market or outcome data, run parameter sweep, PIT "
            "replay, backtest, scoring, paper-shadow, production, broker, new "
            "signal, outcome binding, or trading advice.",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
