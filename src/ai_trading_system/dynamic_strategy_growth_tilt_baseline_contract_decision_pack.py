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
from ai_trading_system.research_quality import (
    growth_tilt_baseline_contract_decision_pack as decision_pack,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438M1D1"
TASK_REGISTER_ID = "TRADING-2438M1D1_GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK"
REPORT_TYPE = decision_pack.REPORT_TYPE
SCHEMA_VERSION = decision_pack.SCHEMA_VERSION

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_CHANNEL_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_first_layer_v3.yaml"
)
DEFAULT_FINAL_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_first_layer_v3_final_matrix.yaml"
)
DEFAULT_SIGNAL_USAGE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_signal_usage_matrix_v2.yaml"
)
DEFAULT_BASE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "base_overlay_veto_policy_schema.yaml"
)
DEFAULT_RISK_VETO_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_on_veto_policy.yaml"
)
DEFAULT_PROBE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_second_layer_probe_registry_v2.yaml"
)
DEFAULT_OWNER_REVIEW_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "growth_tilt_baseline_contract_decision_review.yaml"
)
DEFAULT_CHANNEL_CODE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "channel_specific_first_layer_v3.py"
)
DEFAULT_COMPILER_CODE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "two_layer_policy_compiler.py"
)
DEFAULT_M1C_REPORT_PATH = (
    PROJECT_ROOT / "docs" / "research" / "growth_tilt_owner_mapping_inventory.md"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438M1D_Growth_Tilt_Baseline_Contract_Decision_Pack_And_"
    "Implementation_Plan.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_baseline_contract_decision_pack(
    *,
    channel_config_path: Path = DEFAULT_CHANNEL_CONFIG_PATH,
    final_matrix_path: Path = DEFAULT_FINAL_MATRIX_PATH,
    signal_usage_matrix_path: Path = DEFAULT_SIGNAL_USAGE_MATRIX_PATH,
    base_policy_path: Path = DEFAULT_BASE_POLICY_PATH,
    risk_veto_policy_path: Path = DEFAULT_RISK_VETO_POLICY_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_PATH,
    owner_review_path: Path = DEFAULT_OWNER_REVIEW_PATH,
    channel_code_path: Path = DEFAULT_CHANNEL_CODE_PATH,
    compiler_code_path: Path = DEFAULT_COMPILER_CODE_PATH,
    m1c_report_path: Path = DEFAULT_M1C_REPORT_PATH,
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
        "channel_config": channel_config_path,
        "final_matrix": final_matrix_path,
        "signal_usage_matrix": signal_usage_matrix_path,
        "base_policy": base_policy_path,
        "risk_veto_policy": risk_veto_policy_path,
        "probe_registry": probe_registry_path,
        "owner_review": owner_review_path,
        "report_registry": report_registry_path,
    }
    text_paths = {
        "channel_code_text": channel_code_path,
        "compiler_code_text": compiler_code_path,
        "m1c_report_text": m1c_report_path,
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
    source_errors = _source_validation_errors(yaml_paths, text_paths)
    source_paths = [*yaml_paths.values(), *text_paths.values()]
    resolved_as_of = as_of_date or date.today()
    payload = decision_pack.build_growth_tilt_baseline_contract_decision_pack(
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


def _source_validation_errors(
    yaml_paths: Mapping[str, Path], text_paths: Mapping[str, Path]
) -> list[str]:
    return [
        f"{source_id} missing: {path}"
        for source_id, path in [*yaml_paths.items(), *text_paths.items()]
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
            "decision_pack_only": True,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return enriched


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    sections = {
        "candidate_disposition_after_baseline_audit": (
            "growth_tilt_candidate_disposition_after_baseline_audit.json"
        ),
        "hard_veto_resolution_matrix": "growth_tilt_hard_veto_resolution_matrix.json",
        "transition_exposure_decision": "growth_tilt_transition_exposure_decision.json",
    }
    primary_path = output_root / "growth_tilt_baseline_contract_decision_pack.json"
    markdown_path = docs_root / "growth_tilt_baseline_contract_decision_pack.md"
    payload["artifact_paths"] = {
        "json_path": str(primary_path),
        "markdown_path": str(markdown_path),
        **{section: str(output_root / name) for section, name in sections.items()},
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
        "market_regime": payload.get("market_regime"),
        "interpretation_correction": payload.get("interpretation_correction"),
        "m1d1_decision_complete": payload.get("m1d1_decision_complete"),
        "m1d2_implementation_allowed": payload.get("m1d2_implementation_allowed"),
        "m1d2_readiness_status": payload.get("m1d2_readiness_status"),
        "m2_eligible_candidate_count": payload.get("m2_eligible_candidate_count"),
        "owner_action_count": payload.get("owner_action_count"),
        "strict_validation_error_count": payload.get("strict_validation_error_count"),
        "next_route": payload.get("next_route"),
    }
    return "\n".join(
        [
            "# Growth Tilt Baseline Contract Decision Pack",
            "",
            "本报告只冻结 baseline contract 决策与缺口，不运行 PIT replay、backtest、"
            "scoring 或六项 runtime metric。`do_not_de_risk_pass=false` 是 offline "
            "selection result，不是当前 runtime value，也不单独证明 producer mapping 失败。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Disposition",
            "",
            "```json",
            _json_block(
                _as_mapping(payload.get("candidate_disposition_after_baseline_audit")).get(
                    "candidates", []
                )
            ),
            "```",
            "",
            "## Baseline Contract Decisions",
            "",
            "```json",
            _json_block(
                {
                    "recovery_persistence": payload.get("recovery_persistence_decision"),
                    "defensive_entry": payload.get("defensive_entry_decision"),
                    "hard_veto": payload.get("hard_veto_resolution_matrix"),
                    "transition_exposure": payload.get("transition_exposure_decision"),
                }
            ),
            "```",
            "",
            "## Owner Actions",
            "",
            "```json",
            _json_block(payload.get("owner_actions", [])),
            "```",
            "",
            "## 结论",
            "",
            "A 保持 APPROVE 但 baseline contracts 未就绪；B 已转为 REDEFINE，但仓库"
            "没有 callable aggregate non-hard defensive request，继续实现前必须由 owner "
            "确认 WITHDRAW 或单独批准新的 baseline behavior；C 保持 REDEFINE。M1D2 与 M2 "
            "均继续 blocked。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
