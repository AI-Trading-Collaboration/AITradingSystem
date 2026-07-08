from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_candidate_gauntlet_harness as m2432,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_defensive_limited_adjustment_component_validation as m2434,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_existing_candidate_evidence_matrix as m2431,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_forward_aging_candidate_pack as m2439,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_regime_slice_attribution_review as m2437,
)
from ai_trading_system import dynamic_strategy_growth_tilt_top3_candidate_pit_replay as m2438
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
    growth_tilt_paper_shadow_candidate_promotion_review as promotion_review,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2440"
TASK_REGISTER_ID = "TRADING-2440_GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW"
REPORT_TYPE = promotion_review.REPORT_TYPE
SCHEMA_VERSION = promotion_review.SCHEMA_VERSION
BLOCKED_FORWARD_AGING_STATUS = promotion_review.BLOCKED_FORWARD_AGING_STATUS

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "market_data_experiment_run",
    "forward_aging_observation_started",
    "forward_aging_observation_written",
    "candidate_tracking_started",
    "historical_screen_run",
    "pit_replay_run",
    "backtest_run",
    "scoring_run",
    "daily_report_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
    "outcome_store_mutated",
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
DEFAULT_SOURCE_2431_PATH = (
    m2431.DEFAULT_OUTPUT_ROOT / "existing_candidate_evidence_matrix_result.json"
)
DEFAULT_SOURCE_2432_PATH = m2432.DEFAULT_OUTPUT_ROOT / "candidate_gauntlet_result.json"
DEFAULT_SOURCE_2434_PATH = m2434.DEFAULT_OUTPUT_ROOT / "component_validation_result.json"
DEFAULT_SOURCE_2437_PATH = (
    m2437.DEFAULT_OUTPUT_ROOT / "regime_slice_attribution_review_result.json"
)
DEFAULT_SOURCE_2438_PATH = m2438.DEFAULT_OUTPUT_ROOT / "top3_candidate_pit_replay_result.json"
DEFAULT_SOURCE_2439_PATH = m2439.DEFAULT_OUTPUT_ROOT / "forward_aging_candidate_pack_result.json"
DEFAULT_SOURCE_2431_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_existing_candidate_evidence_matrix.md"
)
DEFAULT_SOURCE_2432_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_gauntlet_harness.md"
)
DEFAULT_SOURCE_2434_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_defensive_limited_adjustment_component_validation.md"
)
DEFAULT_SOURCE_2437_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_regime_slice_attribution_review.md"
)
DEFAULT_SOURCE_2438_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_top3_candidate_pit_replay.md"
)
DEFAULT_SOURCE_2439_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_forward_aging_candidate_pack.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_paper_shadow_candidate_promotion_review(
    *,
    source_2431_existing_candidate_evidence_path: Path = DEFAULT_SOURCE_2431_PATH,
    source_2432_candidate_gauntlet_path: Path = DEFAULT_SOURCE_2432_PATH,
    source_2434_component_validation_path: Path = DEFAULT_SOURCE_2434_PATH,
    source_2437_regime_review_path: Path = DEFAULT_SOURCE_2437_PATH,
    source_2438_pit_replay_path: Path = DEFAULT_SOURCE_2438_PATH,
    source_2439_forward_pack_path: Path = DEFAULT_SOURCE_2439_PATH,
    source_2431_doc_path: Path = DEFAULT_SOURCE_2431_DOC_PATH,
    source_2432_doc_path: Path = DEFAULT_SOURCE_2432_DOC_PATH,
    source_2434_doc_path: Path = DEFAULT_SOURCE_2434_DOC_PATH,
    source_2437_doc_path: Path = DEFAULT_SOURCE_2437_DOC_PATH,
    source_2438_doc_path: Path = DEFAULT_SOURCE_2438_DOC_PATH,
    source_2439_doc_path: Path = DEFAULT_SOURCE_2439_DOC_PATH,
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
        "source_2431_existing_candidate_evidence": _load_json_document(
            source_2431_existing_candidate_evidence_path
        ),
        "source_2432_candidate_gauntlet": _load_json_document(
            source_2432_candidate_gauntlet_path
        ),
        "source_2434_component_validation": _load_json_document(
            source_2434_component_validation_path
        ),
        "source_2437_regime_review": _load_json_document(source_2437_regime_review_path),
        "source_2438_pit_replay": _load_json_document(source_2438_pit_replay_path),
        "source_2439_forward_pack": _load_json_document(source_2439_forward_pack_path),
        "source_2431_doc": _load_text_document(source_2431_doc_path),
        "source_2432_doc": _load_text_document(source_2432_doc_path),
        "source_2434_doc": _load_text_document(source_2434_doc_path),
        "source_2437_doc": _load_text_document(source_2437_doc_path),
        "source_2438_doc": _load_text_document(source_2438_doc_path),
        "source_2439_doc": _load_text_document(source_2439_doc_path),
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
        payload = (
            promotion_review.build_growth_tilt_paper_shadow_candidate_promotion_review(
                _as_mapping(sources["source_2431_existing_candidate_evidence"]),
                _as_mapping(sources["source_2432_candidate_gauntlet"]),
                _as_mapping(sources["source_2434_component_validation"]),
                _as_mapping(sources["source_2437_regime_review"]),
                _as_mapping(sources["source_2438_pit_replay"]),
                _as_mapping(sources["source_2439_forward_pack"]),
                data_quality_summary,
                report_registry=_as_mapping(sources["report_registry"]),
                artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
                    "text",
                    "",
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
    status = promotion_review.BLOCKED_EVIDENCE_STATUS
    gaps = [
        {
            "requirement_id": "source_artifact_availability",
            "classification": "paper_shadow_candidate_promotion_source_gap",
            "gap": (
                "Required prior artifact, docs, registry, catalog, or system "
                "flow is missing."
            ),
            "evidence": {"source_validation_errors": list(source_validation_errors)},
            "production_effect": "none",
            "broker_action": "none",
        }
    ]
    evidence_summary = {
        "schema_version": promotion_review.EVIDENCE_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "evidence_summary_ready": True,
        "source_statuses": {},
        "forward_aging_candidate_count": 0,
        "pit_replay_pass_count": 0,
        "paper_shadow_candidate_evidence_complete": False,
        "evidence_gap_count": len(gaps),
        "blocking_gap_ids": ["source_artifact_availability"],
        "production_effect": "none",
        "broker_action": "none",
    }
    decision_matrix = {
        "schema_version": promotion_review.CANDIDATE_DECISION_MATRIX_SCHEMA_VERSION,
        "status": status,
        "candidate_decision_matrix_ready": True,
        "paper_shadow_candidate_found": False,
        "paper_shadow_candidate_count": 0,
        "selected_candidates": [],
        "rows": [],
        "blocked_reason": "source_artifact_availability",
        "blocking_gap_ids": ["source_artifact_availability"],
        "production_effect": "none",
        "broker_action": "none",
    }
    blocked_route = {
        "schema_version": promotion_review.BLOCKED_PROMOTION_ROUTE_SCHEMA_VERSION,
        "status": status,
        "blocked_promotion_route_ready": True,
        "promotion_review_blocked": True,
        "blocking_gap_count": len(gaps),
        "blocking_gap_ids": ["source_artifact_availability"],
        "next_route": promotion_review.BLOCKED_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }
    boundary = {
        "schema_version": promotion_review.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
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
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
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
        "source_tasks": [
            "TRADING-2431",
            "TRADING-2432",
            "TRADING-2434",
            "TRADING-2437",
            "TRADING-2438",
            "TRADING-2439",
        ],
        "source_2431_ready": False,
        "source_2432_ready": False,
        "source_2434_ready": False,
        "source_2437_ready": False,
        "source_2438_ready": False,
        "source_2439_forward_aging_ready": False,
        "forward_aging_source_status": None,
        "pit_replay_source_status": None,
        "promotion_review_ready": False,
        "evidence_summary_ready": True,
        "candidate_decision_matrix_ready": True,
        "blocked_promotion_route_ready": True,
        "no_effect_boundary_ready": True,
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_summary_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "forward_aging_candidate_count": 0,
        "review_candidate_count": 0,
        "paper_shadow_candidate_found": False,
        "paper_shadow_candidate_count": 0,
        "selected_candidates": [],
        "candidate_decision_rows": [],
        "evidence_summary": evidence_summary,
        "candidate_decision_matrix": decision_matrix,
        "blocked_promotion_route": blocked_route,
        "no_effect_boundary": boundary,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "recommended_next_research_task": promotion_review.BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Required prior artifact, docs, registry, catalog, or system flow is "
            "missing; remediate inputs before promotion review."
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
    json_path = output_root / "promotion_review_result.json"
    evidence_json_path = output_root / "evidence_summary.json"
    decision_json_path = output_root / "candidate_decision_matrix.json"
    blocked_json_path = output_root / "blocked_promotion_route.json"
    boundary_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_paper_shadow_candidate_promotion_review.md"
    evidence_markdown_path = docs_root / "growth_tilt_paper_shadow_candidate_evidence_summary.md"
    decision_markdown_path = docs_root / "growth_tilt_paper_shadow_candidate_decision_matrix.md"
    blocked_markdown_path = docs_root / "growth_tilt_paper_shadow_candidate_blocked_route.md"
    boundary_markdown_path = docs_root / "growth_tilt_paper_shadow_candidate_no_effect_boundary.md"
    route_markdown_path = docs_root / "dynamic_strategy_2440_blocked_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "evidence_summary_json": str(evidence_json_path),
        "candidate_decision_matrix_json": str(decision_json_path),
        "blocked_promotion_route_json": str(blocked_json_path),
        "no_effect_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "evidence_summary_markdown": str(evidence_markdown_path),
        "candidate_decision_matrix_markdown": str(decision_markdown_path),
        "blocked_promotion_route_markdown": str(blocked_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "blocked_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        evidence_json_path,
        "growth_tilt_paper_shadow_candidate_evidence_summary",
        promotion_review.EVIDENCE_SUMMARY_SCHEMA_VERSION,
        payload,
        "evidence_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        decision_json_path,
        "growth_tilt_paper_shadow_candidate_decision_matrix",
        promotion_review.CANDIDATE_DECISION_MATRIX_SCHEMA_VERSION,
        payload,
        "candidate_decision_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        blocked_json_path,
        "growth_tilt_paper_shadow_candidate_blocked_route",
        promotion_review.BLOCKED_PROMOTION_ROUTE_SCHEMA_VERSION,
        payload,
        "blocked_promotion_route",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_paper_shadow_candidate_no_effect_boundary",
        promotion_review.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        evidence_markdown_path,
        _render_section_markdown(
            "Growth Tilt Paper-Shadow Candidate Evidence Summary",
            payload.get("evidence_summary"),
        ),
    )
    write_markdown_artifact(
        decision_markdown_path,
        _render_section_markdown(
            "Growth Tilt Paper-Shadow Candidate Decision Matrix",
            payload.get("candidate_decision_matrix"),
        ),
    )
    write_markdown_artifact(
        blocked_markdown_path,
        _render_section_markdown(
            "Growth Tilt Paper-Shadow Candidate Blocked Route",
            payload.get("blocked_promotion_route"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Paper-Shadow Candidate No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "forward_aging_source_status": payload.get("forward_aging_source_status"),
        "data_quality_status": payload.get("data_quality_status"),
        "paper_shadow_candidate_found": payload.get("paper_shadow_candidate_found"),
        "paper_shadow_candidate_count": payload.get("paper_shadow_candidate_count"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Paper-Shadow Candidate Promotion Review",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- forward aging source status：`{payload.get('forward_aging_source_status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            f"- paper-shadow candidate count：`{payload.get('paper_shadow_candidate_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2440 只允许在 2439 forward aging candidate pack READY 后执行 "
            "paper-shadow candidate promotion review。当前 2439 被 PIT replay gate "
            "阻断，因此本任务 fail-closed，未输出 no-candidate 策略结论。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Decision Matrix",
            "",
            "```json",
            _json_block(payload.get("candidate_decision_matrix", {})),
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
            "# Dynamic Strategy TRADING-2440 Blocked Route",
            "",
            "- source task：`TRADING-2440`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2440 当前不得进入 candidate-specific paper-shadow gate；必须先完成 "
            "TRADING-2438A，并重新运行 2438/2439 获得真实 forward aging candidate pack。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
