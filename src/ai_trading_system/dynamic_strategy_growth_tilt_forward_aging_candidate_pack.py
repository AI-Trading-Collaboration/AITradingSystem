from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import dynamic_strategy_growth_tilt_top3_candidate_pit_replay as m2438
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
from ai_trading_system.research_framework.data_quality_gate import (
    run_growth_tilt_data_quality_gate,
)
from ai_trading_system.research_quality import (
    growth_tilt_forward_aging_candidate_pack as forward_pack,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2439"
TASK_REGISTER_ID = "TRADING-2439_GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK"
REPORT_TYPE = forward_pack.REPORT_TYPE
SCHEMA_VERSION = forward_pack.SCHEMA_VERSION
READY_STATUS = forward_pack.READY_STATUS
BLOCKED_PIT_REPLAY_STATUS = forward_pack.BLOCKED_PIT_REPLAY_STATUS

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "market_data_experiment_run",
    "forward_aging_observation_started",
    "forward_aging_observation_written",
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
DEFAULT_SOURCE_2438_PIT_REPLAY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "top3_candidate_pit_replay_result.json"
)
DEFAULT_PIT_REPLAY_EVIDENCE_PATH = m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_evidence.json"
DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_blocker_summary.json"
)
DEFAULT_PIT_REPLAY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_top3_candidate_pit_replay.md"
)
DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_top3_candidate_pit_replay_evidence.md"
)
DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_pit_replay_blocker_summary.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_forward_aging_candidate_pack(
    *,
    source_2438_pit_replay_path: Path = DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Path = DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Path = DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    pit_replay_doc_path: Path = DEFAULT_PIT_REPLAY_DOC_PATH,
    pit_replay_evidence_doc_path: Path = DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH,
    pit_replay_blocker_doc_path: Path = DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH,
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
        "source_2438_pit_replay": _load_json_document(source_2438_pit_replay_path),
        "pit_replay_evidence": _load_json_document(pit_replay_evidence_path),
        "pit_replay_blocker_summary": _load_json_document(
            pit_replay_blocker_summary_path
        ),
        "pit_replay_doc": _load_text_document(pit_replay_doc_path),
        "pit_replay_evidence_doc": _load_text_document(pit_replay_evidence_doc_path),
        "pit_replay_blocker_doc": _load_text_document(pit_replay_blocker_doc_path),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    source_validation_errors = _source_validation_errors(sources)
    if data_quality_summary_path is not None:
        data_quality_summary = _load_data_quality_summary(data_quality_summary_path)
    else:
        data_quality_summary = run_growth_tilt_data_quality_gate(
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
        payload = forward_pack.build_growth_tilt_forward_aging_candidate_pack(
            _as_mapping(sources["source_2438_pit_replay"]),
            _as_mapping(sources["pit_replay_evidence"]),
            _as_mapping(sources["pit_replay_blocker_summary"]),
            data_quality_summary,
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


def _load_data_quality_summary(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {
            "data_quality_gate_executed": False,
            "data_quality_gate_passed": False,
            "data_quality_status": "MISSING",
            "data_quality_report_path": str(path),
        }
    return _as_mapping(json.loads(path.read_text(encoding="utf-8")))


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
    status = forward_pack.BLOCKED_EVIDENCE_STATUS
    gaps = [
        {
            "requirement_id": "source_artifact_availability",
            "classification": "forward_aging_candidate_pack_source_gap",
            "gap": (
                "Required prior artifact, docs, registry, catalog, or system "
                "flow is missing."
            ),
            "evidence": {"source_validation_errors": list(source_validation_errors)},
            "production_effect": "none",
            "broker_action": "none",
        }
    ]
    pack = {
        "schema_version": forward_pack.FORWARD_AGING_PACK_SCHEMA_VERSION,
        "status": status,
        "forward_aging_candidate_pack_ready": False,
        "forward_aging_candidate_count": 0,
        "observation_horizons": list(forward_pack.OBSERVATION_HORIZONS),
        "candidates": [],
        "valid_until_outcome_capture_ready": False,
        "candidate_evidence_refresh_cadence": "not_started_source_gap",
        "evidence_gap_count": len(gaps),
        "blocking_gap_ids": ["source_artifact_availability"],
        "production_effect": "none",
        "broker_action": "none",
    }
    tracking = {
        "schema_version": forward_pack.CANDIDATE_TRACKING_SCHEMA_VERSION,
        "status": status,
        "candidate_tracking_artifact_ready": True,
        "tracking_started": False,
        "forward_aging_candidate_count": 0,
        "tracking_rows": [],
        "blocked_reason": "source_artifact_availability",
        "blocking_gap_ids": ["source_artifact_availability"],
        "production_effect": "none",
        "broker_action": "none",
    }
    contract = {
        "schema_version": forward_pack.FORWARD_OBSERVATION_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "forward_observation_contract_ready": True,
        "observation_horizons": list(forward_pack.OBSERVATION_HORIZONS),
        "valid_until_outcome_capture_required": True,
        "baseline_comparison_required": True,
        "candidate_evidence_refresh_cadence": "not_started_source_gap",
        "outcome_capture_started": False,
        "forward_observation_started": False,
        "evidence_gap_count": len(gaps),
        "blocking_gap_ids": ["source_artifact_availability"],
        "production_effect": "none",
        "broker_action": "none",
    }
    boundary = {
        "schema_version": forward_pack.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
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
        "source_tasks": ["TRADING-2438"],
        "source_2438_ready": False,
        "pit_replay_source_status": None,
        "pit_replay_pass_candidate_count": 0,
        "pit_replay_pass_count_from_source": 0,
        "pit_replay_tested_count_from_source": 0,
        "pit_replay_blocked_count_from_source": 0,
        "forward_aging_candidate_pack_ready": False,
        "candidate_tracking_artifact_ready": True,
        "forward_observation_contract_ready": True,
        "no_effect_boundary_ready": True,
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_summary_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "forward_aging_candidate_count": 0,
        "forward_aging_candidate_count_if_unblocked": 0,
        "observation_horizons": list(forward_pack.OBSERVATION_HORIZONS),
        "valid_until_outcome_capture_ready": False,
        "candidate_evidence_refresh_cadence": "not_started_source_gap",
        "forward_aging_candidate_pack": pack,
        "candidate_tracking_artifact": tracking,
        "forward_observation_contract": contract,
        "no_effect_boundary": boundary,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": ["source_artifact_availability"],
        "gaps": gaps,
        "recommended_next_research_task": forward_pack.BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Required prior artifact, docs, registry, catalog, or system flow is "
            "missing; remediate inputs before forward aging candidate pack."
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
    json_path = output_root / "forward_aging_candidate_pack_result.json"
    pack_json_path = output_root / "forward_aging_candidate_pack.json"
    tracking_json_path = output_root / "candidate_tracking_artifact.json"
    contract_json_path = output_root / "forward_observation_contract.json"
    boundary_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_forward_aging_candidate_pack.md"
    pack_markdown_path = docs_root / "growth_tilt_forward_aging_candidate_pack_details.md"
    tracking_markdown_path = docs_root / "growth_tilt_forward_aging_candidate_tracking.md"
    contract_markdown_path = docs_root / "growth_tilt_forward_observation_contract.md"
    boundary_markdown_path = docs_root / "growth_tilt_forward_aging_no_effect_boundary.md"
    route_markdown_path = docs_root / "dynamic_strategy_2439_blocked_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "forward_aging_candidate_pack_json": str(pack_json_path),
        "candidate_tracking_artifact_json": str(tracking_json_path),
        "forward_observation_contract_json": str(contract_json_path),
        "no_effect_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "forward_aging_candidate_pack_markdown": str(pack_markdown_path),
        "candidate_tracking_artifact_markdown": str(tracking_markdown_path),
        "forward_observation_contract_markdown": str(contract_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "blocked_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        pack_json_path,
        "growth_tilt_forward_aging_candidate_pack_details",
        forward_pack.FORWARD_AGING_PACK_SCHEMA_VERSION,
        payload,
        "forward_aging_candidate_pack",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        tracking_json_path,
        "growth_tilt_forward_aging_candidate_tracking",
        forward_pack.CANDIDATE_TRACKING_SCHEMA_VERSION,
        payload,
        "candidate_tracking_artifact",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        contract_json_path,
        "growth_tilt_forward_observation_contract",
        forward_pack.FORWARD_OBSERVATION_CONTRACT_SCHEMA_VERSION,
        payload,
        "forward_observation_contract",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_forward_aging_no_effect_boundary",
        forward_pack.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        pack_markdown_path,
        _render_section_markdown(
            "Growth Tilt Forward Aging Candidate Pack Details",
            payload.get("forward_aging_candidate_pack"),
        ),
    )
    write_markdown_artifact(
        tracking_markdown_path,
        _render_section_markdown(
            "Growth Tilt Forward Aging Candidate Tracking",
            payload.get("candidate_tracking_artifact"),
        ),
    )
    write_markdown_artifact(
        contract_markdown_path,
        _render_section_markdown(
            "Growth Tilt Forward Observation Contract",
            payload.get("forward_observation_contract"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Forward Aging No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "pit_replay_source_status": payload.get("pit_replay_source_status"),
        "data_quality_status": payload.get("data_quality_status"),
        "forward_aging_candidate_count": payload.get("forward_aging_candidate_count"),
        "observation_horizons": payload.get("observation_horizons"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Forward Aging Candidate Pack",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- PIT replay source status：`{payload.get('pit_replay_source_status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            f"- forward aging candidate count：`{payload.get('forward_aging_candidate_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2439 只允许为真实通过 PIT replay 的候选生成 forward aging "
            "candidate pack。当前 2438 被 replay engine / input specs blocker 卡住，"
            "因此本任务 fail-closed，未生成 forward aging candidates。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Pack",
            "",
            "```json",
            _json_block(payload.get("forward_aging_candidate_pack", {})),
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
            "# Dynamic Strategy TRADING-2439 Blocked Route",
            "",
            "- source task：`TRADING-2439`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2439 当前不得进入 TRADING-2440 promotion review ready route；"
            "必须先完成 TRADING-2438A，补齐 candidate-specific PIT replay engine、"
            "input specs、source/as-of/valid-until/outcome linkage evidence，并真实执行 replay。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
