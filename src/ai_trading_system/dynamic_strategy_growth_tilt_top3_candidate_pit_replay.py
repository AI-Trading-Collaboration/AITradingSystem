from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_existing_candidate_evidence_matrix as m2431,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_false_risk_off_missed_upside_batch_screen as m2433,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_regime_slice_attribution_review as m2437,
)
from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
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
    growth_tilt_top3_candidate_pit_replay as pit_replay,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438"
TASK_REGISTER_ID = "TRADING-2438_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY"
REPORT_TYPE = pit_replay.REPORT_TYPE
SCHEMA_VERSION = pit_replay.SCHEMA_VERSION
READY_STATUS = pit_replay.READY_STATUS
BLOCKED_REPLAY_ENGINE_STATUS = pit_replay.BLOCKED_REPLAY_ENGINE_STATUS

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "historical_screen_run",
    "market_data_experiment_run",
    "pit_replay_run",
    "pit_replay_executed",
    "computed_new_metrics",
    "scoring_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "backtest_run",
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
DEFAULT_SOURCE_2437_REGIME_REVIEW_PATH = (
    m2437.DEFAULT_OUTPUT_ROOT / "regime_slice_attribution_review_result.json"
)
DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH = m2433.DEFAULT_OUTPUT_ROOT / "batch_screen_result.json"
DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_PATH = (
    m2431.DEFAULT_OUTPUT_ROOT / "existing_candidate_evidence_matrix_result.json"
)
DEFAULT_CANDIDATE_SET_2433_PATH = (
    PROJECT_ROOT
    / "research"
    / "configs"
    / "growth_tilt"
    / "false_risk_off_missed_upside_2433.yaml"
)
DEFAULT_REGIME_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_regime_slice_attribution_review.md"
)
DEFAULT_BATCH_SCREEN_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_false_risk_off_missed_upside_batch_screen.md"
)
DEFAULT_EXISTING_CANDIDATE_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_existing_candidate_evidence_matrix.md"
)
DEFAULT_CANDIDATE_SET_2433_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_top3_candidate_pit_replay(
    *,
    source_2437_regime_review_path: Path = DEFAULT_SOURCE_2437_REGIME_REVIEW_PATH,
    source_2433_batch_screen_path: Path = DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH,
    source_2431_existing_candidate_evidence_path: Path = (
        DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_PATH
    ),
    candidate_set_2433_path: Path = DEFAULT_CANDIDATE_SET_2433_PATH,
    regime_review_doc_path: Path = DEFAULT_REGIME_REVIEW_DOC_PATH,
    batch_screen_doc_path: Path = DEFAULT_BATCH_SCREEN_DOC_PATH,
    existing_candidate_doc_path: Path = DEFAULT_EXISTING_CANDIDATE_DOC_PATH,
    candidate_set_2433_doc_path: Path = DEFAULT_CANDIDATE_SET_2433_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    data_quality_summary_path: Path | None = None,
    data_quality_output_path: Path | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2437_regime_review": _load_json_document(source_2437_regime_review_path),
        "source_2433_batch_screen": _load_json_document(source_2433_batch_screen_path),
        "source_2431_existing_candidate_evidence": _load_json_document(
            source_2431_existing_candidate_evidence_path
        ),
        "candidate_set_2433": _load_yaml_document(candidate_set_2433_path),
        "regime_review_doc": _load_text_document(regime_review_doc_path),
        "batch_screen_doc": _load_text_document(batch_screen_doc_path),
        "existing_candidate_doc": _load_text_document(existing_candidate_doc_path),
        "candidate_set_2433_doc": _load_text_document(candidate_set_2433_doc_path),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    source_validation_errors = _source_validation_errors(sources)
    if data_quality_summary_path is not None:
        data_quality_summary = _load_data_quality_summary(data_quality_summary_path)
    else:
        data_quality_summary = _run_data_quality_gate(
            prices_path=prices_path,
            rates_path=rates_path,
            as_of_date=as_of_date,
            output_path=data_quality_output_path,
        )
    if source_validation_errors:
        payload = _blocked_payload(
            source_validation_errors=source_validation_errors,
            data_quality_summary=data_quality_summary,
            as_of_date=as_of_date,
        )
    else:
        research_doc_texts = {
            name: _as_mapping(document).get("text", "")
            for name, document in sources.items()
            if name.endswith("_doc")
        }
        payload = pit_replay.build_growth_tilt_top3_candidate_pit_replay(
            _as_mapping(sources["source_2437_regime_review"]),
            _as_mapping(sources["source_2433_batch_screen"]),
            _as_mapping(sources["source_2431_existing_candidate_evidence"]),
            _as_mapping(sources["candidate_set_2433"]),
            data_quality_summary,
            report_registry=_as_mapping(sources["report_registry"]),
            artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                "text",
                "",
            ),
            system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
            research_doc_texts=research_doc_texts,
            candidate_pit_replay_engine_available=False,
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


def _load_data_quality_summary(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {
            "data_quality_gate_executed": False,
            "data_quality_gate_passed": False,
            "data_quality_status": "MISSING",
            "data_quality_report_path": str(path),
        }
    return _as_mapping(json.loads(path.read_text(encoding="utf-8")))


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    output_path: Path | None,
) -> dict[str, Any]:
    universe = load_universe()
    quality_config = load_data_quality()
    resolved_as_of = as_of_date or date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        resolved_as_of,
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=False,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=resolved_as_of,
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=prices_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(report, report_path)
    return {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": report.passed,
        "data_quality_status": report.status,
        "data_quality_report_path": str(report_path),
        "data_quality_as_of": str(report.as_of),
        "data_quality_error_count": report.error_count,
        "data_quality_warning_count": report.warning_count,
        "data_quality_info_count": report.info_count,
    }


def _requires_marketstack_prices(prices_path: Path) -> bool:
    try:
        return prices_path.resolve() == DEFAULT_PRICES_PATH.resolve()
    except OSError:
        return prices_path == DEFAULT_PRICES_PATH


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
    data_quality_summary: Mapping[str, Any],
    as_of_date: date | None,
) -> dict[str, Any]:
    status = pit_replay.BLOCKED_EVIDENCE_STATUS
    gaps = [
        {
            "requirement_id": "source_artifact_availability",
            "classification": "top3_candidate_pit_replay_source_gap",
            "gap": (
                "Required prior artifact, config, docs, registry, catalog, or "
                "system flow is missing."
            ),
            "evidence": {"source_validation_errors": list(source_validation_errors)},
            "production_effect": "none",
            "broker_action": "none",
        }
    ]
    selection = {
        "schema_version": pit_replay.TOP3_CANDIDATE_SELECTION_SCHEMA_VERSION,
        "status": status,
        "top3_candidate_selection_ready": False,
        "candidate_limit": pit_replay.TOP_CANDIDATE_LIMIT,
        "pit_candidates_selected": 0,
        "selected_candidates": [],
        "selection_basis": "blocked_missing_prior_artifact_or_doc",
        "evidence_gap_count": len(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }
    evidence = {
        "schema_version": pit_replay.PIT_REPLAY_EVIDENCE_SCHEMA_VERSION,
        "status": status,
        "pit_replay_evidence_ready": False,
        "pit_replay_executed": False,
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": 0,
        "rows": [],
        "production_effect": "none",
        "broker_action": "none",
    }
    blocker_summary = {
        "schema_version": pit_replay.PIT_REPLAY_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "pit_replay_blocker_summary_ready": True,
        "blocked": True,
        "blocked_candidate_count": 0,
        "blocking_gap_count": len(gaps),
        "blocking_gap_ids": ["source_artifact_availability"],
        "blocking_gap_classifications": {
            "source_artifact_availability": "top3_candidate_pit_replay_source_gap"
        },
        "next_route": pit_replay.BLOCKED_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }
    boundary = {
        "schema_version": pit_replay.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "evidence_gap_count": len(gaps),
        "gaps": gaps,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2437", "TRADING-2433", "TRADING-2431"],
        "source_2437_ready": False,
        "source_2433_batch_screen_ready": False,
        "source_2431_existing_candidate_evidence_ready": False,
        "candidate_set_2433_ready": False,
        "top3_candidate_selection_ready": False,
        "pit_replay_evidence_artifact_ready": False,
        "pit_replay_blocker_summary_ready": True,
        "no_effect_boundary_ready": True,
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_summary_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "pit_candidates_selected": 0,
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": 0,
        "promotion_review_candidate_count": 0,
        "selected_candidates": [],
        "candidate_pit_replay_engine_available": False,
        "candidate_replay_input_specs_ready": False,
        "candidate_source_traceability_manifests_ready": False,
        "candidate_as_of_boundary_specs_ready": False,
        "candidate_valid_until_boundary_specs_ready": False,
        "candidate_outcome_linkage_specs_ready": False,
        "source_traceability_verified_count": 0,
        "as_of_boundary_verified_count": 0,
        "valid_until_boundary_verified_count": 0,
        "outcome_linkage_ready_count": 0,
        "pit_replay_run": False,
        "pit_replay_executed": False,
        "computed_new_metrics": False,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "top3_candidate_selection": selection,
        "pit_replay_evidence": evidence,
        "pit_replay_blocker_summary": blocker_summary,
        "no_effect_boundary": boundary,
        "gaps": gaps,
        "cached_market_data_quality_gate_run": (
            data_quality_summary.get("data_quality_gate_executed") is True
        ),
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "recommended_next_research_task": pit_replay.BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Required prior artifact, config, docs, registry, catalog, or system "
            "flow is missing; remediate inputs before top-3 PIT replay."
        ),
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )


def _data_quality_summary_passed(summary: Mapping[str, Any]) -> bool:
    if summary.get("data_quality_gate_passed") is True:
        return True
    return summary.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"}


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "top3_candidate_pit_replay_result.json"
    selection_json_path = output_root / "top3_candidate_selection.json"
    evidence_json_path = output_root / "pit_replay_evidence.json"
    blocker_json_path = output_root / "pit_replay_blocker_summary.json"
    boundary_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_top3_candidate_pit_replay.md"
    selection_markdown_path = docs_root / "growth_tilt_top3_candidate_selection.md"
    evidence_markdown_path = docs_root / "growth_tilt_top3_candidate_pit_replay_evidence.md"
    blocker_markdown_path = (
        docs_root / "growth_tilt_top3_candidate_pit_replay_blocker_summary.md"
    )
    boundary_markdown_path = (
        docs_root / "growth_tilt_top3_candidate_pit_replay_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438A_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "top3_candidate_selection_json": str(selection_json_path),
        "pit_replay_evidence_json": str(evidence_json_path),
        "pit_replay_blocker_summary_json": str(blocker_json_path),
        "no_effect_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "top3_candidate_selection_markdown": str(selection_markdown_path),
        "pit_replay_evidence_markdown": str(evidence_markdown_path),
        "pit_replay_blocker_summary_markdown": str(blocker_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        selection_json_path,
        "growth_tilt_top3_candidate_selection",
        pit_replay.TOP3_CANDIDATE_SELECTION_SCHEMA_VERSION,
        payload,
        "top3_candidate_selection",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        evidence_json_path,
        "growth_tilt_top3_candidate_pit_replay_evidence",
        pit_replay.PIT_REPLAY_EVIDENCE_SCHEMA_VERSION,
        payload,
        "pit_replay_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        blocker_json_path,
        "growth_tilt_top3_candidate_pit_replay_blocker_summary",
        pit_replay.PIT_REPLAY_BLOCKER_SUMMARY_SCHEMA_VERSION,
        payload,
        "pit_replay_blocker_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_top3_candidate_pit_replay_no_effect_boundary",
        pit_replay.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        selection_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate Selection",
            payload.get("top3_candidate_selection"),
        ),
    )
    write_markdown_artifact(
        evidence_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay Evidence",
            payload.get("pit_replay_evidence"),
        ),
    )
    write_markdown_artifact(
        blocker_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay Blocker Summary",
            payload.get("pit_replay_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "data_quality_status": payload.get("data_quality_status"),
        "pit_candidates_selected": payload.get("pit_candidates_selected"),
        "pit_candidates_tested": payload.get("pit_candidates_tested"),
        "pit_replay_pass_count": payload.get("pit_replay_pass_count"),
        "pit_replay_fail_count": payload.get("pit_replay_fail_count"),
        "promotion_review_candidate_count": payload.get(
            "promotion_review_candidate_count"
        ),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Top-3 Candidate PIT Replay",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            f"- PIT candidates selected：`{payload.get('pit_candidates_selected')}`",
            f"- PIT candidates tested：`{payload.get('pit_candidates_tested')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438 运行数据质量门，选择最多三个 PIT candidates，然后在缺少 "
            "Growth Tilt candidate-specific PIT replay engine 和 replay input "
            "specs 时 fail-closed。本输出不是 replay pass，也不是 alpha 结论。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## PIT Replay Blocker Summary",
            "",
            "```json",
            _json_block(payload.get("pit_replay_blocker_summary", {})),
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
            "# Dynamic Strategy TRADING-2438A Route",
            "",
            "- source task：`TRADING-2438`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438A 应先补齐 Growth Tilt candidate-specific PIT replay "
            "engine 和 candidate replay input specs，然后才允许进入 forward aging "
            "candidate pack。TRADING-2438 不启用 paper-shadow、production、broker、"
            "trading advice 或 portfolio weight mutation。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
