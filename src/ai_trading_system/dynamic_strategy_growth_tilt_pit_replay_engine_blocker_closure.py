from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import dynamic_strategy_growth_tilt_top3_candidate_pit_replay as m2438
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_engine_remediation as m2438a,
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
    growth_tilt_pit_replay_engine_blocker_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438B"
TASK_REGISTER_ID = "TRADING-2438B_GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE"
REPORT_TYPE = closure.REPORT_TYPE
SCHEMA_VERSION = closure.SCHEMA_VERSION
READY_STATUS = closure.READY_STATUS
BLOCKED_STATUS = closure.BLOCKED_STATUS

SAFETY_FALSE_FIELDS = PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438A_REMEDIATION_PATH = (
    m2438a.DEFAULT_OUTPUT_ROOT / "remediation_result.json"
)
DEFAULT_SOURCE_2438_PIT_REPLAY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "top3_candidate_pit_replay_result.json"
)
DEFAULT_PIT_REPLAY_EVIDENCE_PATH = m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_evidence.json"
DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_blocker_summary.json"
)
DEFAULT_SOURCE_2438A_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "growth_tilt_top3_candidate_pit_replay_engine_remediation.md"
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


def run_growth_tilt_pit_replay_engine_blocker_closure(
    *,
    source_2438a_remediation_path: Path = DEFAULT_SOURCE_2438A_REMEDIATION_PATH,
    source_2438_pit_replay_path: Path = DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Path = DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Path = DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438a_doc_path: Path = DEFAULT_SOURCE_2438A_DOC_PATH,
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
        "source_2438a_remediation": _load_json_document(
            source_2438a_remediation_path
        ),
        "source_2438_pit_replay": _load_json_document(source_2438_pit_replay_path),
        "pit_replay_evidence": _load_json_document(pit_replay_evidence_path),
        "pit_replay_blocker_summary": _load_json_document(
            pit_replay_blocker_summary_path
        ),
        "source_2438a_doc": _load_text_document(source_2438a_doc_path),
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
    source_2438 = _as_mapping(sources["source_2438_pit_replay"])
    selected_candidates = _selected_candidates(source_2438)
    resolved_as_of = str(
        as_of_date
        or _as_mapping(sources["source_2438a_remediation"]).get("as_of")
        or source_2438.get("as_of")
        or ""
    )
    generation_command = (
        "aits research strategies growth-tilt-pit-replay-engine-blocker-closure "
        f"--as-of {resolved_as_of}"
    )
    closure_artifacts = closure.default_closure_artifacts(
        selected_candidates=selected_candidates,
        as_of=resolved_as_of,
        generation_command=generation_command,
    )
    research_doc_texts = {
        name: _as_mapping(document).get("text", "")
        for name, document in sources.items()
        if name.endswith("_doc")
    }
    payload = closure.build_growth_tilt_pit_replay_engine_blocker_closure(
        _as_mapping(sources["source_2438a_remediation"]),
        source_2438,
        _as_mapping(sources["pit_replay_evidence"]),
        _as_mapping(sources["pit_replay_blocker_summary"]),
        data_quality_summary,
        closure_artifacts=closure_artifacts,
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get("text", ""),
        system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
        research_doc_texts=research_doc_texts,
        as_of=resolved_as_of,
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
    engine_path = output_root / "pit_replay_engine_contract.json"
    input_path = output_root / "input_specs.json"
    evidence_path = output_root / "evidence_completeness_contract.json"
    traceability_path = output_root / "source_traceability_manifest.json"
    as_of_path = output_root / "as_of_boundary_manifest.json"
    valid_until_path = output_root / "valid_until_boundary_manifest.json"
    outcome_path = output_root / "outcome_linkage_map.json"
    handoff_path = output_root / "forward_aging_handoff_contract.json"
    before_after_path = output_root / "blocker_before_after_matrix.json"
    unresolved_path = output_root / "unresolved_blocker_summary.json"
    boundary_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_pit_replay_engine_blocker_closure.md"
    before_after_markdown_path = (
        docs_root / "growth_tilt_pit_replay_engine_blocker_before_after.md"
    )
    unresolved_markdown_path = (
        docs_root / "growth_tilt_pit_replay_engine_unresolved_blockers.md"
    )
    boundary_markdown_path = (
        docs_root / "growth_tilt_pit_replay_engine_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438C_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "pit_replay_engine_contract_json": str(engine_path),
        "input_specs_json": str(input_path),
        "evidence_completeness_contract_json": str(evidence_path),
        "source_traceability_manifest_json": str(traceability_path),
        "as_of_boundary_manifest_json": str(as_of_path),
        "valid_until_boundary_manifest_json": str(valid_until_path),
        "outcome_linkage_map_json": str(outcome_path),
        "forward_aging_handoff_contract_json": str(handoff_path),
        "blocker_before_after_matrix_json": str(before_after_path),
        "unresolved_blocker_summary_json": str(unresolved_path),
        "no_effect_boundary_json": str(boundary_path),
        "markdown_path": str(markdown_path),
        "blocker_before_after_markdown": str(before_after_markdown_path),
        "unresolved_blocker_summary_markdown": str(unresolved_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    _write_section(engine_path, "pit_replay_engine_contract", payload)
    _write_section(input_path, "input_specs", payload)
    _write_section(evidence_path, "evidence_completeness_contract", payload)
    _write_section(traceability_path, "source_traceability_manifest", payload)
    _write_section(as_of_path, "as_of_boundary_manifest", payload)
    _write_section(valid_until_path, "valid_until_boundary_manifest", payload)
    _write_section(outcome_path, "outcome_linkage_map", payload)
    _write_section(handoff_path, "forward_aging_handoff_contract", payload)
    _write_section(before_after_path, "blocker_before_after_matrix", payload)
    _write_section(unresolved_path, "unresolved_blocker_summary", payload)
    _write_section(boundary_path, "no_effect_boundary", payload)
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        before_after_markdown_path,
        _render_section_markdown(
            "Growth Tilt PIT Replay Engine Blocker Before After",
            payload.get("blocker_before_after_matrix"),
        ),
    )
    write_markdown_artifact(
        unresolved_markdown_path,
        _render_section_markdown(
            "Growth Tilt PIT Replay Engine Unresolved Blockers",
            payload.get("unresolved_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt PIT Replay Engine Blocker Closure No-Effect Boundary",
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
        "blocker_closure_ready": payload.get("blocker_closure_ready"),
        "blocker_count_before": payload.get("blocker_count_before"),
        "blocker_count_after": payload.get("blocker_count_after"),
        "closed_blockers": payload.get("closed_blockers"),
        "remaining_blockers": payload.get("remaining_blockers"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt PIT Replay Engine Blocker Closure",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            f"- blocker closure ready：`{payload.get('blocker_closure_ready')}`",
            f"- blocker count before：`{payload.get('blocker_count_before')}`",
            f"- blocker count after：`{payload.get('blocker_count_after')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438B 关闭 PIT replay engine blocker 的 contract、input、"
            "evidence completeness、source traceability、as-of、valid-until、"
            "outcome linkage 和 forward-aging handoff 基础条件。本任务不执行真实 "
            "candidate PIT replay，不宣布 candidate pass，不启用 paper-shadow / "
            "schedule / production / broker，也不生成 trading advice。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Engine Contract",
            "",
            "```json",
            _json_block(payload.get("pit_replay_engine_contract", {})),
            "```",
            "",
            "## Unresolved Blocker Summary",
            "",
            "```json",
            _json_block(payload.get("unresolved_blocker_summary", {})),
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
            "TRADING-2438B closed the PIT replay engine blocker layer. The next "
            "step is TRADING-2438C top-3 candidate PIT replay recheck; that "
            "future recheck must still execute replay evidence before 2439/2440 "
            "can advance."
        )
    else:
        body = (
            "TRADING-2438B remains blocked. Close the remaining PIT replay "
            "engine/input/evidence/traceability/boundary/outcome/handoff gaps "
            "before the 2438C replay recheck."
        )
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2438C Route",
            "",
            "- source task：`TRADING-2438B`",
            f"- source status：`{payload.get('status')}`",
            f"- next route：`{next_route}`",
            "",
            body,
            "",
        ]
    )


def _selected_candidates(source_2438: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    selected = _sequence(source_2438.get("selected_candidates"))
    if not selected:
        selected = _sequence(
            _as_mapping(source_2438.get("top3_candidate_selection")).get(
                "selected_candidates"
            )
        )
    return [candidate for candidate in selected if isinstance(candidate, Mapping)]


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, list | tuple):
        return list(value)
    return []
