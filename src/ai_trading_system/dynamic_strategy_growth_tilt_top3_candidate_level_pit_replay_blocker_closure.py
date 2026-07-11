from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure as m2438e,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as m2438d,
)
from ai_trading_system.config import PROJECT_ROOT
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
from ai_trading_system.research_framework.data_quality_gate import (
    run_growth_tilt_data_quality_gate,
)
from ai_trading_system.research_framework.runtime_metadata import (
    PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS,
    with_pit_replay_observe_only_runtime_metadata,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_level_pit_replay_blocker_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438F"
TASK_REGISTER_ID = (
    "TRADING-2438F_GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE"
)
REPORT_TYPE = closure.REPORT_TYPE
SCHEMA_VERSION = closure.SCHEMA_VERSION
READY_STATUS = closure.READY_STATUS
BLOCKED_STATUS = closure.BLOCKED_STATUS

SAFETY_FALSE_FIELDS = PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438E_RECHECK_PATH = (
    m2438e.DEFAULT_OUTPUT_ROOT / "recheck_after_output_closure_result.json"
)
DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH = (
    m2438d.DEFAULT_OUTPUT_ROOT / "candidate_replay_output_records.json"
)
DEFAULT_CANDIDATE_LEVEL_BLOCKER_SUMMARY_PATH = (
    m2438e.DEFAULT_OUTPUT_ROOT / "candidate_level_blocker_summary.json"
)
DEFAULT_SOURCE_2438E_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure.md"
)
DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_replay_output_records.md"
)
DEFAULT_CANDIDATE_LEVEL_BLOCKER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_candidate_level_replay_blocker_summary.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"


def run_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
    *,
    source_2438e_recheck_path: Path = DEFAULT_SOURCE_2438E_RECHECK_PATH,
    candidate_replay_output_records_path: Path = (
        DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH
    ),
    candidate_level_blocker_summary_path: Path = (
        DEFAULT_CANDIDATE_LEVEL_BLOCKER_SUMMARY_PATH
    ),
    source_2438e_doc_path: Path = DEFAULT_SOURCE_2438E_DOC_PATH,
    candidate_output_records_doc_path: Path = DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    candidate_level_blocker_doc_path: Path = DEFAULT_CANDIDATE_LEVEL_BLOCKER_DOC_PATH,
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
        "source_2438e_recheck": _load_json_document(source_2438e_recheck_path),
        "candidate_replay_output_records": _load_json_document(
            candidate_replay_output_records_path
        ),
        "candidate_level_blocker_summary": _load_json_document(
            candidate_level_blocker_summary_path
        ),
        "source_2438e_doc": _load_text_document(source_2438e_doc_path),
        "candidate_output_records_doc": _load_text_document(
            candidate_output_records_doc_path
        ),
        "candidate_level_blocker_doc": _load_text_document(
            candidate_level_blocker_doc_path
        ),
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
    research_doc_texts = {
        name: _as_mapping(document).get("text", "")
        for name, document in sources.items()
        if name.endswith("_doc")
    }
    payload = closure.build_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
        _as_mapping(sources["source_2438e_recheck"]),
        _as_mapping(sources["candidate_replay_output_records"]),
        _as_mapping(sources["candidate_level_blocker_summary"]),
        data_quality_summary,
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get(
            "text",
            "",
        ),
        system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
        research_doc_texts=research_doc_texts,
        as_of=str(as_of_date) if as_of_date else None,
    )
    payload = with_pit_replay_observe_only_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
        task_register_id=TASK_REGISTER_ID,
        report_type=REPORT_TYPE,
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


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "blocker_closure_result.json"
    records_path = output_root / "candidate_level_blocker_closure_records.json"
    matrix_path = output_root / "candidate_level_before_after_matrix.json"
    unresolved_path = output_root / "unresolved_candidate_blocker_summary.json"
    handoff_path = output_root / "replayability_handoff_manifest.json"
    boundary_path = output_root / "no_effect_boundary.json"
    markdown_path = (
        docs_root / "growth_tilt_top3_candidate_level_pit_replay_blocker_closure.md"
    )
    records_markdown_path = (
        docs_root / "growth_tilt_candidate_level_pit_replay_blocker_closure_records.md"
    )
    matrix_markdown_path = docs_root / "growth_tilt_candidate_level_pit_replay_before_after.md"
    unresolved_markdown_path = (
        docs_root / "growth_tilt_unresolved_candidate_level_pit_replay_blockers.md"
    )
    handoff_markdown_path = (
        docs_root / "growth_tilt_candidate_replayability_handoff_manifest.md"
    )
    boundary_markdown_path = (
        docs_root
        / "growth_tilt_candidate_level_pit_replay_blocker_closure_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438G_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_level_blocker_closure_records_json": str(records_path),
        "candidate_level_before_after_matrix_json": str(matrix_path),
        "unresolved_candidate_blocker_summary_json": str(unresolved_path),
        "replayability_handoff_manifest_json": str(handoff_path),
        "no_effect_boundary_json": str(boundary_path),
        "markdown_path": str(markdown_path),
        "candidate_level_blocker_closure_records_markdown": str(
            records_markdown_path
        ),
        "candidate_level_before_after_matrix_markdown": str(matrix_markdown_path),
        "unresolved_candidate_blocker_summary_markdown": str(
            unresolved_markdown_path
        ),
        "replayability_handoff_manifest_markdown": str(handoff_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    _write_section(records_path, "candidate_level_blocker_closure_records", payload)
    _write_section(matrix_path, "candidate_level_before_after_matrix", payload)
    _write_section(unresolved_path, "unresolved_candidate_blocker_summary", payload)
    _write_section(handoff_path, "replayability_handoff_manifest", payload)
    _write_section(boundary_path, "no_effect_boundary", payload)
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        records_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate-Level PIT Replay Blocker Closure Records",
            payload.get("candidate_level_blocker_closure_records"),
        ),
    )
    write_markdown_artifact(
        matrix_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate-Level PIT Replay Before After",
            payload.get("candidate_level_before_after_matrix"),
        ),
    )
    write_markdown_artifact(
        unresolved_markdown_path,
        _render_section_markdown(
            "Growth Tilt Unresolved Candidate-Level PIT Replay Blockers",
            payload.get("unresolved_candidate_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        handoff_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Replayability Handoff Manifest",
            payload.get("replayability_handoff_manifest"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate-Level PIT Replay Blocker Closure No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _write_section(path: Path, section_name: str, payload: Mapping[str, Any]) -> None:
    section = _as_mapping(payload.get(section_name))
    schema_version = str(section.get("schema_version") or SCHEMA_VERSION)
    write_section_json_artifact(
        path,
        f"growth_tilt_{section_name}",
        schema_version,
        payload,
        section_name,
        task_id=TASK_ID,
    )


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "prior_status": payload.get("prior_status"),
        "data_quality_status": payload.get("data_quality_status"),
        "candidate_replay_outputs_complete": payload.get(
            "candidate_replay_outputs_complete"
        ),
        "candidate_replay_output_record_count": payload.get(
            "candidate_replay_output_record_count"
        ),
        "candidate_level_blocker_count_before": payload.get(
            "candidate_level_blocker_count_before"
        ),
        "candidate_level_blocker_count_after": payload.get(
            "candidate_level_blocker_count_after"
        ),
        "candidate_replayable_after_closure_count": payload.get(
            "candidate_replayable_after_closure_count"
        ),
        "replayability_handoff_ready": payload.get("replayability_handoff_ready"),
        "candidate_replay_pass_count": payload.get("candidate_replay_pass_count"),
        "candidate_replay_fail_count": payload.get("candidate_replay_fail_count"),
        "candidate_replay_blocked_count": payload.get(
            "candidate_replay_blocked_count"
        ),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Top-3 Candidate-Level PIT Replay Blocker Closure",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            "- candidate blocker before / after："
            f"`{payload.get('candidate_level_blocker_count_before')}` / "
            f"`{payload.get('candidate_level_blocker_count_after')}`",
            f"- replayability handoff ready：`{payload.get('replayability_handoff_ready')}`",
            f"- pass / fail / blocked：`{payload.get('candidate_replay_pass_count')}` / "
            f"`{payload.get('candidate_replay_fail_count')}` / "
            f"`{payload.get('candidate_replay_blocked_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438F 只关闭 candidate-level PIT replayability blocker，"
            "为 2438G 独立 recheck 提供 handoff。READY 不代表 replay PASS、"
            "FAIL、no-candidate 或 paper-shadow candidate found。paper-shadow、"
            "production 和 broker 均保持 disabled / none。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Closure Records",
            "",
            "```json",
            _json_block(payload.get("candidate_level_blocker_closure_records", {})),
            "```",
            "",
            "## Before After Matrix",
            "",
            "```json",
            _json_block(payload.get("candidate_level_before_after_matrix", {})),
            "```",
            "",
            "## Replayability Handoff",
            "",
            "```json",
            _json_block(payload.get("replayability_handoff_manifest", {})),
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
    status = payload.get("status")
    next_route = payload.get("recommended_next_research_task")
    if status == READY_STATUS:
        body = (
            "3 个 candidate-level replayability blockers 已闭合。下一步进入 "
            "2438G 独立重判 PASS / FAIL / BLOCKED；本步仍不启用 forward-aging "
            "或 paper-shadow。"
        )
    else:
        body = (
            "仍有 candidate-level PIT replay blocker 或 closure evidence gap。"
            "下一步必须先关闭剩余 blocker，不能进入 replay outcome recheck。"
        )
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2438G Route",
            "",
            "- source task：`TRADING-2438F`",
            f"- source status：`{status}`",
            f"- next route：`{next_route}`",
            "",
            body,
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}
