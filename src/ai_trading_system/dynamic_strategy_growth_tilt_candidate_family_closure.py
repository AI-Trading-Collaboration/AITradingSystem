from __future__ import annotations

import hashlib
import json
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
from ai_trading_system.research_quality import (
    growth_tilt_candidate_family_closure as closure,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438N1"
TASK_REGISTER_ID = (
    "TRADING-2438N1_GROWTH_TILT_CANDIDATE_FAMILY_CLOSURE_AND_NEGATIVE_EVIDENCE_LEDGER"
)
REPORT_TYPE = closure.REPORT_TYPE
SCHEMA_VERSION = closure.SCHEMA_VERSION

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_M1E_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_replacement_candidate_contract"
    / "growth_tilt_replacement_candidate_contract.json"
)
DEFAULT_ADAPTERS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_baseline_contract_adapters_readiness"
    / "growth_tilt_baseline_contract_adapters_readiness.json"
)
DEFAULT_OWNER_RESOLUTION_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "growth_tilt_owner_decision_resolution.yaml"
)
DEFAULT_CANDIDATE_SET_PATH = (
    PROJECT_ROOT
    / "research"
    / "configs"
    / "growth_tilt"
    / "false_risk_off_missed_upside_2433.yaml"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438N_Growth_Tilt_Candidate_Family_Closure_And_"
    "Contract_First_Discovery_Pivot.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_candidate_family_closure(
    *,
    m1e_path: Path = DEFAULT_M1E_PATH,
    adapters_path: Path = DEFAULT_ADAPTERS_PATH,
    owner_resolution_path: Path = DEFAULT_OWNER_RESOLUTION_PATH,
    candidate_set_path: Path = DEFAULT_CANDIDATE_SET_PATH,
    requirement_doc_path: Path = DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    structured_paths = {
        "m1e": m1e_path,
        "adapters": adapters_path,
        "owner_resolution": owner_resolution_path,
        "candidate_set": candidate_set_path,
        "report_registry": report_registry_path,
    }
    text_paths = {
        "requirement_text": requirement_doc_path,
        "artifact_catalog_text": artifact_catalog_path,
        "system_flow_text": system_flow_path,
    }
    sources = {
        key: _load_structured_document(path)
        for key, path in structured_paths.items()
    }
    sources.update(
        {
            key: _text_value(_load_text_document(path))
            for key, path in text_paths.items()
        }
    )
    source_errors = _source_validation_errors(structured_paths, text_paths)
    source_paths = [*structured_paths.values(), *text_paths.values()]
    resolved_as_of = as_of_date or date.today()
    payload = closure.build_growth_tilt_candidate_family_closure(
        sources,
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=str(sources["artifact_catalog_text"]),
        system_flow_text=str(sources["system_flow_text"]),
        requirement_text=str(sources["requirement_text"]),
        source_artifacts=_source_artifact_records(source_paths),
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


def _load_structured_document(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    if path.suffix.lower() in {".yaml", ".yml"}:
        return safe_load_yaml_path(path)
    return json.loads(path.read_text(encoding="utf-8"))


def _text_value(document: Any) -> str:
    if isinstance(document, Mapping):
        return str(document.get("text", ""))
    return str(document or "")


def _source_validation_errors(
    structured_paths: Mapping[str, Path], text_paths: Mapping[str, Path]
) -> list[str]:
    return [
        f"{source_id} missing: {path}"
        for source_id, path in [*structured_paths.items(), *text_paths.items()]
        if not path.exists()
    ]


def _source_artifact_records(paths: list[Path]) -> list[dict[str, Any]]:
    return [
        {
            "path": str(path.resolve()),
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for path in paths
        if path.exists()
    ]


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
    primary_path = output_root / "growth_tilt_candidate_family_closure.json"
    ledger_path = output_root / "growth_tilt_candidate_negative_result_ledger.json"
    markdown_path = docs_root / "growth_tilt_candidate_family_closure.md"
    payload["artifact_paths"] = {
        "json_path": str(primary_path),
        "negative_result_ledger_path": str(ledger_path),
        "markdown_path": str(markdown_path),
    }
    write_json_artifact(primary_path, payload)
    ledger = _as_mapping(payload.get("negative_result_ledger"))
    write_section_json_artifact(
        ledger_path,
        "negative_result_ledger",
        str(ledger.get("schema_version") or SCHEMA_VERSION),
        payload,
        "negative_result_ledger",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_markdown(payload))


def _render_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "as_of": payload.get("as_of"),
        "family_id": payload.get("family_id"),
        "closure_status": payload.get("closure_status"),
        "closure_reason_codes": payload.get("closure_reason_codes"),
        "candidate_dispositions": payload.get("candidate_dispositions"),
        "prerequisite_pass_count": payload.get("prerequisite_pass_count"),
        "prerequisite_blocked_count": payload.get("prerequisite_blocked_count"),
        "baseline_adapter_ready_count": payload.get("baseline_adapter_ready_count"),
        "baseline_adapter_blocked_count": payload.get(
            "baseline_adapter_blocked_count"
        ),
        "pit_candidates_tested": payload.get("pit_candidates_tested"),
        "next_route": payload.get("next_route"),
    }
    return "\n".join(
        [
            "# Growth Tilt Candidate Family Closure",
            "",
            "当前 A/B/C/replacement-A family 已正式关闭为 completed negative research "
            "evidence。关闭不是 FAIL 或实现失败；它表示没有 approved、contract-complete、"
            "PIT-executable candidate。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Exact M1E prerequisite matrix",
            "",
            "```json",
            _json_block(payload.get("replacement_a_prerequisite_matrix")),
            "```",
            "",
            "## Negative-result ledger",
            "",
            "```json",
            _json_block(payload.get("negative_result_ledger")),
            "```",
            "",
            "## Reopen policy",
            "",
            "```json",
            _json_block(payload.get("reopen_policy")),
            "```",
            "",
            "## 结论",
            "",
            "旧 family 的 M2 route 已关闭。只有 candidate-independent baseline work产生"
            "受治理的新 capability evidence并重新 owner approval/refreeze policy后，才可"
            "新开 reopen task；当前下一步是 read-only TRADING-2438N2 capability graph。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
