from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure as m2438b,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay as m2438,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck as m2438c,
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
    growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438D"
TASK_REGISTER_ID = (
    "TRADING-2438D_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE"
)
REPORT_TYPE = closure.REPORT_TYPE
SCHEMA_VERSION = closure.SCHEMA_VERSION
READY_STATUS = closure.READY_STATUS
BLOCKED_STATUS = closure.BLOCKED_STATUS

SAFETY_FALSE_FIELDS = PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438C_RECHECK_PATH = (
    m2438c.DEFAULT_OUTPUT_ROOT / "pit_replay_recheck_result.json"
)
DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH = (
    m2438b.DEFAULT_OUTPUT_ROOT / "blocker_closure_result.json"
)
DEFAULT_SOURCE_2438_PIT_REPLAY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "top3_candidate_pit_replay_result.json"
)
DEFAULT_PIT_REPLAY_EVIDENCE_PATH = m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_evidence.json"
DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_blocker_summary.json"
)
DEFAULT_SOURCE_2438C_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_top3_candidate_pit_replay_recheck.md"
)
DEFAULT_SOURCE_2438B_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_pit_replay_engine_blocker_closure.md"
)
DEFAULT_SOURCE_2438_DOC_PATH = (
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


def run_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
    *,
    source_2438c_recheck_path: Path = DEFAULT_SOURCE_2438C_RECHECK_PATH,
    source_2438b_blocker_closure_path: Path = DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH,
    source_2438_pit_replay_path: Path = DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Path = DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Path = DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438c_doc_path: Path = DEFAULT_SOURCE_2438C_DOC_PATH,
    source_2438b_doc_path: Path = DEFAULT_SOURCE_2438B_DOC_PATH,
    source_2438_doc_path: Path = DEFAULT_SOURCE_2438_DOC_PATH,
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
        "source_2438c_recheck": _load_json_document(source_2438c_recheck_path),
        "source_2438b_blocker_closure": _load_json_document(
            source_2438b_blocker_closure_path
        ),
        "source_2438_pit_replay": _load_json_document(source_2438_pit_replay_path),
        "pit_replay_evidence": _load_json_document(pit_replay_evidence_path),
        "pit_replay_blocker_summary": _load_json_document(
            pit_replay_blocker_summary_path
        ),
        "source_2438c_doc": _load_text_document(source_2438c_doc_path),
        "source_2438b_doc": _load_text_document(source_2438b_doc_path),
        "source_2438_doc": _load_text_document(source_2438_doc_path),
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
    research_doc_texts = {
        name: _as_mapping(document).get("text", "")
        for name, document in sources.items()
        if name.endswith("_doc")
    }
    payload = closure.build_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
        _as_mapping(sources["source_2438c_recheck"]),
        _as_mapping(sources["source_2438b_blocker_closure"]),
        _as_mapping(sources["source_2438_pit_replay"]),
        _as_mapping(sources["pit_replay_evidence"]),
        _as_mapping(sources["pit_replay_blocker_summary"]),
        data_quality_summary,
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get("text", ""),
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
    output_records_path = output_root / "candidate_replay_output_records.json"
    completeness_path = output_root / "output_completeness_closure.json"
    before_after_path = output_root / "before_after_matrix.json"
    blocker_path = output_root / "remaining_output_blocker_summary.json"
    boundary_path = output_root / "no_effect_boundary.json"
    markdown_path = (
        docs_root / "growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.md"
    )
    output_records_markdown_path = (
        docs_root / "growth_tilt_candidate_replay_output_records.md"
    )
    completeness_markdown_path = (
        docs_root / "growth_tilt_candidate_replay_output_completeness_closure.md"
    )
    before_after_markdown_path = (
        docs_root / "growth_tilt_candidate_replay_output_before_after.md"
    )
    blocker_markdown_path = (
        docs_root / "growth_tilt_candidate_replay_output_remaining_blockers.md"
    )
    boundary_markdown_path = (
        docs_root / "growth_tilt_candidate_replay_output_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438E_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "candidate_replay_output_records_json": str(output_records_path),
        "output_completeness_closure_json": str(completeness_path),
        "before_after_matrix_json": str(before_after_path),
        "remaining_output_blocker_summary_json": str(blocker_path),
        "no_effect_boundary_json": str(boundary_path),
        "markdown_path": str(markdown_path),
        "candidate_replay_output_records_markdown": str(output_records_markdown_path),
        "output_completeness_closure_markdown": str(completeness_markdown_path),
        "before_after_matrix_markdown": str(before_after_markdown_path),
        "remaining_output_blocker_summary_markdown": str(blocker_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    _write_section(
        output_records_path,
        "candidate_replay_output_records",
        payload,
    )
    _write_section(
        completeness_path,
        "output_completeness_closure",
        payload,
    )
    _write_section(before_after_path, "before_after_matrix", payload)
    _write_section(blocker_path, "remaining_output_blocker_summary", payload)
    _write_section(boundary_path, "no_effect_boundary", payload)
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        output_records_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Replay Output Records",
            payload.get("candidate_replay_output_records"),
        ),
    )
    write_markdown_artifact(
        completeness_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Replay Output Completeness Closure",
            payload.get("output_completeness_closure"),
        ),
    )
    write_markdown_artifact(
        before_after_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Replay Output Before After",
            payload.get("before_after_matrix"),
        ),
    )
    write_markdown_artifact(
        blocker_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Replay Output Remaining Blockers",
            payload.get("remaining_output_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Candidate Replay Output No-Effect Boundary",
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
        "prior_candidate_replay_outputs_complete": payload.get(
            "prior_candidate_replay_outputs_complete"
        ),
        "data_quality_status": payload.get("data_quality_status"),
        "blocker_closure_ready": payload.get("blocker_closure_ready"),
        "candidate_replay_outputs_complete": payload.get(
            "candidate_replay_outputs_complete"
        ),
        "candidate_replay_output_record_count": payload.get(
            "candidate_replay_output_record_count"
        ),
        "candidate_replay_pass_count": payload.get("candidate_replay_pass_count"),
        "candidate_replay_fail_count": payload.get("candidate_replay_fail_count"),
        "candidate_replay_blocked_count": payload.get(
            "candidate_replay_blocked_count"
        ),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Top-3 Candidate PIT Replay Recheck Blocker Closure",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            "- candidate replay outputs complete："
            f"`{payload.get('candidate_replay_outputs_complete')}`",
            f"- output record count：`{payload.get('candidate_replay_output_record_count')}`",
            f"- pass / fail / blocked：`{payload.get('candidate_replay_pass_count')}` / "
            f"`{payload.get('candidate_replay_fail_count')}` / "
            f"`{payload.get('candidate_replay_blocked_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438D 只关闭 2438C 暴露的 candidate replay output completeness "
            "blocker，并为 3 个 top-3 candidate 生成结构化 `PASS` / `FAIL` / "
            "`BLOCKED` output record。READY 不代表 candidate pass，不代表 "
            "paper-shadow candidate found，也不跳过后续 2438E replay recheck / "
            "forward-aging handoff gate。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Replay Output Records",
            "",
            "```json",
            _json_block(payload.get("candidate_replay_output_records", {})),
            "```",
            "",
            "## Output Completeness Closure",
            "",
            "```json",
            _json_block(payload.get("output_completeness_closure", {})),
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
    next_route = payload.get("recommended_next_research_task")
    if payload.get("blocker_closure_ready") is True:
        body = (
            "TRADING-2438D 已把 3 个 top-3 candidate replay output record 补齐。"
            "下一步进入 2438E，对这些结构化 output record 独立复核 pass/fail/blocked；"
            "当前 READY 不表示 paper-shadow candidate found，也不允许跳过 forward-aging gate。"
        )
    else:
        body = (
            "TRADING-2438D 仍有 candidate replay output blocker。补齐 replay status、"
            "status reason、input/source/evidence/as-of/valid-until/outcome/handoff "
            "refs 后，才能进入 2438E。"
        )
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2438E Route",
            "",
            "- source task：`TRADING-2438D`",
            f"- source status：`{payload.get('status')}`",
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
