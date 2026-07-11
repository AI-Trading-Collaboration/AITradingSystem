from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_forward_aging_candidate_pack as m2439,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_paper_shadow_candidate_promotion_review as m2440,
)
from ai_trading_system import dynamic_strategy_growth_tilt_top3_candidate_pit_replay as m2438
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
    growth_tilt_top3_candidate_pit_replay_engine_remediation as remediation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438A"
TASK_REGISTER_ID = (
    "TRADING-2438A_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION"
)
REPORT_TYPE = remediation.REPORT_TYPE
SCHEMA_VERSION = remediation.SCHEMA_VERSION
READY_STATUS = remediation.READY_STATUS
BLOCKED_STATUS = remediation.BLOCKED_STATUS

SAFETY_FALSE_FIELDS = PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2440_PROMOTION_REVIEW_PATH = (
    m2440.DEFAULT_OUTPUT_ROOT / "promotion_review_result.json"
)
DEFAULT_SOURCE_2439_FORWARD_PACK_PATH = (
    m2439.DEFAULT_OUTPUT_ROOT / "forward_aging_candidate_pack_result.json"
)
DEFAULT_SOURCE_2438_PIT_REPLAY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "top3_candidate_pit_replay_result.json"
)
DEFAULT_PIT_REPLAY_EVIDENCE_PATH = m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_evidence.json"
DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH = (
    m2438.DEFAULT_OUTPUT_ROOT / "pit_replay_blocker_summary.json"
)
DEFAULT_SOURCE_2440_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_paper_shadow_candidate_promotion_review.md"
)
DEFAULT_SOURCE_2439_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_forward_aging_candidate_pack.md"
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


def run_growth_tilt_top3_candidate_pit_replay_engine_remediation(
    *,
    source_2440_promotion_review_path: Path = DEFAULT_SOURCE_2440_PROMOTION_REVIEW_PATH,
    source_2439_forward_pack_path: Path = DEFAULT_SOURCE_2439_FORWARD_PACK_PATH,
    source_2438_pit_replay_path: Path = DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Path = DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Path = DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2440_doc_path: Path = DEFAULT_SOURCE_2440_DOC_PATH,
    source_2439_doc_path: Path = DEFAULT_SOURCE_2439_DOC_PATH,
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
        "source_2440_promotion_review": _load_json_document(
            source_2440_promotion_review_path
        ),
        "source_2439_forward_pack": _load_json_document(source_2439_forward_pack_path),
        "source_2438_pit_replay": _load_json_document(source_2438_pit_replay_path),
        "pit_replay_evidence": _load_json_document(pit_replay_evidence_path),
        "pit_replay_blocker_summary": _load_json_document(
            pit_replay_blocker_summary_path
        ),
        "source_2440_doc": _load_text_document(source_2440_doc_path),
        "source_2439_doc": _load_text_document(source_2439_doc_path),
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
    payload = remediation.build_growth_tilt_top3_candidate_pit_replay_engine_remediation(
        _as_mapping(sources["source_2440_promotion_review"]),
        _as_mapping(sources["source_2439_forward_pack"]),
        _as_mapping(sources["source_2438_pit_replay"]),
        _as_mapping(sources["pit_replay_evidence"]),
        _as_mapping(sources["pit_replay_blocker_summary"]),
        data_quality_summary,
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get("text", ""),
        system_flow_text=_as_mapping(sources["system_flow"]).get("text", ""),
        research_doc_texts=research_doc_texts,
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
    json_path = output_root / "remediation_result.json"
    evidence_json_path = output_root / "remediation_evidence.json"
    before_after_json_path = output_root / "before_after_comparison.json"
    blocker_json_path = output_root / "remaining_blocker_summary.json"
    boundary_json_path = output_root / "no_effect_boundary.json"
    markdown_path = docs_root / "growth_tilt_top3_candidate_pit_replay_engine_remediation.md"
    evidence_markdown_path = (
        docs_root / "growth_tilt_top3_candidate_pit_replay_engine_remediation_evidence.md"
    )
    before_after_markdown_path = (
        docs_root
        / "growth_tilt_top3_candidate_pit_replay_engine_remediation_before_after.md"
    )
    blocker_markdown_path = (
        docs_root
        / "growth_tilt_top3_candidate_pit_replay_engine_remediation_remaining_blockers.md"
    )
    boundary_markdown_path = (
        docs_root
        / "growth_tilt_top3_candidate_pit_replay_engine_remediation_no_effect_boundary.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2438B_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "remediation_evidence_json": str(evidence_json_path),
        "before_after_comparison_json": str(before_after_json_path),
        "remaining_blocker_summary_json": str(blocker_json_path),
        "no_effect_boundary_json": str(boundary_json_path),
        "markdown_path": str(markdown_path),
        "remediation_evidence_markdown": str(evidence_markdown_path),
        "before_after_comparison_markdown": str(before_after_markdown_path),
        "remaining_blocker_summary_markdown": str(blocker_markdown_path),
        "no_effect_boundary_markdown": str(boundary_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        evidence_json_path,
        "growth_tilt_top3_candidate_pit_replay_engine_remediation_evidence",
        remediation.REMEDIATION_EVIDENCE_SCHEMA_VERSION,
        payload,
        "remediation_evidence",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        before_after_json_path,
        "growth_tilt_top3_candidate_pit_replay_engine_remediation_before_after",
        remediation.BEFORE_AFTER_SCHEMA_VERSION,
        payload,
        "before_after_comparison",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        blocker_json_path,
        "growth_tilt_top3_candidate_pit_replay_engine_remediation_remaining_blockers",
        remediation.REMAINING_BLOCKER_SCHEMA_VERSION,
        payload,
        "remaining_blocker_summary",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        boundary_json_path,
        "growth_tilt_top3_candidate_pit_replay_engine_remediation_no_effect_boundary",
        remediation.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        payload,
        "no_effect_boundary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(
        evidence_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay Engine Remediation Evidence",
            payload.get("remediation_evidence"),
        ),
    )
    write_markdown_artifact(
        before_after_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay Engine Remediation Before After",
            payload.get("before_after_comparison"),
        ),
    )
    write_markdown_artifact(
        blocker_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay Engine Remaining Blockers",
            payload.get("remaining_blocker_summary"),
        ),
    )
    write_markdown_artifact(
        boundary_markdown_path,
        _render_section_markdown(
            "Growth Tilt Top-3 Candidate PIT Replay Engine Remediation No-Effect Boundary",
            payload.get("no_effect_boundary"),
        ),
    )
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "prior_promotion_review_status": payload.get("prior_promotion_review_status"),
        "prior_forward_aging_status": payload.get("prior_forward_aging_status"),
        "prior_pit_replay_status": payload.get("prior_pit_replay_status"),
        "data_quality_status": payload.get("data_quality_status"),
        "remediation_ready": payload.get("remediation_ready"),
        "remediation_gap_count": payload.get("remediation_gap_count"),
        "unresolved_engine_blocker_count": payload.get(
            "unresolved_engine_blocker_count"
        ),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Top-3 Candidate PIT Replay Engine Remediation",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- data quality status：`{payload.get('data_quality_status')}`",
            f"- remediation ready：`{payload.get('remediation_ready')}`",
            f"- remaining blocker count：`{payload.get('unresolved_engine_blocker_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2438A 回读 2440 / 2439 / 2438 artifacts，定位 forward aging "
            "gate 被 PIT replay engine / input specs / source traceability / as-of / "
            "valid-until / outcome linkage / handoff blocker 阻断的原因。本任务不启用 "
            "paper-shadow，不生成 trading advice，也不把 blocked 状态标为 no-candidate。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Remaining Blocker Summary",
            "",
            "```json",
            _json_block(payload.get("remaining_blocker_summary", {})),
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
    if payload.get("remediation_ready") is True:
        body = (
            "TRADING-2438A remediation evidence is ready. The next step is to rerun "
            "the top-3 candidate PIT replay recheck before regenerating forward "
            "aging candidate pack evidence."
        )
    else:
        body = (
            "TRADING-2438A remains blocked. Close the remaining PIT replay engine, "
            "input spec, traceability, boundary, and handoff blockers before the "
            "top-3 candidate PIT replay recheck."
        )
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2438B Route",
            "",
            "- source task：`TRADING-2438A`",
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
