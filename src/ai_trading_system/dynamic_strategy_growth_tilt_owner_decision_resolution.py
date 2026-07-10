from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
from ai_trading_system.dynamic_strategy_report_common import (
    load_text_document_or_missing_flag as _load_text_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
    write_section_json_artifact,
)
from ai_trading_system.research_quality import growth_tilt_owner_decision_resolution as resolver
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438M1D1A"
TASK_REGISTER_ID = (
    "TRADING-2438M1D1A_GROWTH_TILT_OWNER_DECISION_RESOLUTION_AND_"
    "CANDIDATE_A_REFRAMING"
)
REPORT_TYPE = resolver.REPORT_TYPE
SCHEMA_VERSION = resolver.SCHEMA_VERSION

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_OWNER_RESOLUTION_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "growth_tilt_owner_decision_resolution.yaml"
)
DEFAULT_CHANNEL_CODE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "channel_specific_first_layer_v3.py"
)
DEFAULT_COMPILER_CODE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "two_layer_policy_compiler.py"
)
DEFAULT_CHANNEL_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "channel_specific_v3"
    / "channel_composer_v3_predictions.csv"
)
DEFAULT_THRESHOLD_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "growth_tilt_candidate_pit_screening_policy.yaml"
)
DEFAULT_M1D1_REPORT_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_baseline_contract_decision_pack.md"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438M1D1A_Growth_Tilt_Owner_Decision_Resolution_And_"
    "Candidate_A_Reframing_Plan.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_owner_decision_resolution(
    *,
    owner_resolution_path: Path = DEFAULT_OWNER_RESOLUTION_PATH,
    channel_code_path: Path = DEFAULT_CHANNEL_CODE_PATH,
    compiler_code_path: Path = DEFAULT_COMPILER_CODE_PATH,
    channel_predictions_path: Path = DEFAULT_CHANNEL_PREDICTIONS_PATH,
    threshold_policy_path: Path = DEFAULT_THRESHOLD_POLICY_PATH,
    m1d1_report_path: Path = DEFAULT_M1D1_REPORT_PATH,
    requirement_doc_path: Path = DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    yaml_paths = {
        "owner_resolution": owner_resolution_path,
        "threshold_policy": threshold_policy_path,
        "report_registry": report_registry_path,
    }
    text_paths = {
        "channel_code_text": channel_code_path,
        "compiler_code_text": compiler_code_path,
        "m1d1_report_text": m1d1_report_path,
        "requirement_text": requirement_doc_path,
        "artifact_catalog_text": artifact_catalog_path,
        "system_flow_text": system_flow_path,
    }
    sources: dict[str, Any] = {
        key: _load_yaml_document(path) for key, path in yaml_paths.items()
    }
    sources.update(
        {
            key: _text_value(_load_text_document(path))
            for key, path in text_paths.items()
        }
    )
    sources["channel_prediction_header"] = _csv_header(channel_predictions_path)
    source_errors = _source_validation_errors(
        yaml_paths, text_paths, {"channel_predictions": channel_predictions_path}
    )
    source_paths = [*yaml_paths.values(), *text_paths.values(), channel_predictions_path]
    resolved_as_of = as_of_date or date.today()
    payload = resolver.build_growth_tilt_owner_decision_resolution(
        sources,
        source_artifacts=_source_artifact_records(source_paths, sources, yaml_paths),
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=str(sources["artifact_catalog_text"]),
        system_flow_text=str(sources["system_flow_text"]),
        requirement_text=str(sources["requirement_text"]),
        as_of=str(resolved_as_of),
    )
    payload = _with_runtime_metadata(
        payload,
        source_validation_errors=source_errors,
        as_of_date=resolved_as_of,
    )
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    strict_errors = source_errors + [
        str(item) for item in payload.get("strict_validation_errors", [])
    ]
    if strict and strict_errors:
        raise ValueError("; ".join(strict_errors))
    return payload


def _load_yaml_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    return safe_load_yaml_path(path)


def _text_value(document: Any) -> str:
    if isinstance(document, Mapping):
        return str(document.get("text", ""))
    return str(document or "")


def _csv_header(path: Path) -> str:
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8-sig") as handle:
        return handle.readline().strip()


def _source_validation_errors(
    yaml_paths: Mapping[str, Path],
    text_paths: Mapping[str, Path],
    csv_paths: Mapping[str, Path],
) -> list[str]:
    return [
        f"{source_id} missing: {path}"
        for source_id, path in [*yaml_paths.items(), *text_paths.items(), *csv_paths.items()]
        if not path.exists()
    ]


def _source_artifact_records(
    paths: list[Path],
    sources: Mapping[str, Any],
    yaml_paths: Mapping[str, Path],
) -> list[dict[str, Any]]:
    source_id_by_path = {path.resolve(): source_id for source_id, path in yaml_paths.items()}
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        source_id = source_id_by_path.get(path.resolve())
        document = _as_mapping(sources.get(source_id)) if source_id else {}
        rows.append(
            {
                "path": str(path.resolve()),
                "sha256": _sha256(path),
                "schema_version": document.get("schema_version"),
                "source_id": source_id,
                "size_bytes": path.stat().st_size,
            }
        )
    return rows


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched.update(
        {
            "as_of": str(as_of_date),
            "generated_at": utc_now_iso(),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "task_register_id": TASK_REGISTER_ID,
            "manual_review_required": True,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return enriched


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    sections = {
        "candidate_disposition_after_owner_resolution": (
            "growth_tilt_candidate_disposition_after_owner_resolution.json"
        ),
        "m1d2_adapter_scope": "growth_tilt_m1d2_adapter_scope.json",
        "replacement_a_readiness": "growth_tilt_replacement_a_readiness.json",
    }
    primary_path = output_root / "growth_tilt_owner_decision_resolution.json"
    markdown_path = docs_root / "growth_tilt_owner_decision_resolution.md"
    payload["artifact_paths"] = {
        "json_path": str(primary_path),
        "markdown_path": str(markdown_path),
        **{section: str(output_root / filename) for section, filename in sections.items()},
    }
    write_json_artifact(primary_path, payload)
    for section_name, filename in sections.items():
        section = _as_mapping(payload.get(section_name))
        write_section_json_artifact(
            output_root / filename,
            section_name,
            str(section.get("schema_version") or SCHEMA_VERSION),
            payload,
            section_name,
            task_id=TASK_ID,
        )
    write_markdown_artifact(markdown_path, _render_markdown(payload))


def _render_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "as_of": payload.get("as_of"),
        "decision_count": payload.get("decision_count"),
        "resolved_decision_count": payload.get("resolved_decision_count"),
        "blocking_decision_ids": payload.get("blocking_decision_ids"),
        "owner_decisions_complete": payload.get("owner_decisions_complete"),
        "m1d2_adapter_implementation_allowed": payload.get(
            "m1d2_adapter_implementation_allowed"
        ),
        "approved_candidate_count": payload.get("approved_candidate_count"),
        "redefine_candidate_count": payload.get("redefine_candidate_count"),
        "withdraw_candidate_count": payload.get("withdraw_candidate_count"),
        "m2_eligible_candidate_count": payload.get("m2_eligible_candidate_count"),
        "strict_validation_error_count": payload.get("strict_validation_error_count"),
        "next_route": payload.get("next_route"),
    }
    return "\n".join(
        [
            "# Growth Tilt Owner Decision Resolution",
            "",
            "D01～D18 已全部决议；`RESOLVED_BLOCKED` 是明确的 owner outcome，"
            "不是缺失输入。A 改为 replacement overlay proposal，B WITHDRAW，C "
            "保留 REDEFINE/out-of-route。该报告不运行 runtime code 或 replay。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Disposition",
            "",
            "```json",
            _json_block(payload.get("candidate_disposition_after_owner_resolution")),
            "```",
            "",
            "## Replacement A Readiness",
            "",
            "```json",
            _json_block(payload.get("replacement_a_readiness")),
            "```",
            "",
            "## M1D2 Scope",
            "",
            "```json",
            _json_block(payload.get("m1d2_adapter_scope")),
            "```",
            "",
            "## 结论",
            "",
            "owner decisions 已完整，但 recovery PIT lineage、threshold、hard veto、"
            "transition trace 和 native scalar 仍有 evidence blockers。M1D2 只获准实现"
            "不改变 baseline 决策的 adapters；replacement A 仍不得批准或 replay。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
