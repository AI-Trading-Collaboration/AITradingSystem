from __future__ import annotations

import csv
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
from ai_trading_system.research_quality import growth_tilt_owner_mapping_inventory as inventory
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438M1C"
TASK_REGISTER_ID = (
    "TRADING-2438M1C_GROWTH_TILT_BASELINE_RUNTIME_MAPPING_INVENTORY_AND_"
    "OWNER_PREREGISTRATION"
)
REPORT_TYPE = inventory.REPORT_TYPE
SCHEMA_VERSION = inventory.SCHEMA_VERSION
READY_STATUS = inventory.READY_STATUS
BLOCKED_STATUS = inventory.BLOCKED_STATUS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_CHANNEL_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_first_layer_v3.yaml"
)
DEFAULT_SIGNAL_USAGE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_signal_usage_matrix_v2.yaml"
)
DEFAULT_FINAL_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_first_layer_v3_final_matrix.yaml"
)
DEFAULT_COMPOSER_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_composer_v2.yaml"
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
DEFAULT_DYNAMIC_ALLOCATION_POLICY_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_allocation_policy.yaml"
)
DEFAULT_CHANNEL_CODE_PATH = PROJECT_ROOT / "src" / "ai_trading_system" / (
    "channel_specific_first_layer_v3.py"
)
DEFAULT_COMPILER_CODE_PATH = PROJECT_ROOT / "src" / "ai_trading_system" / (
    "two_layer_policy_compiler.py"
)
DEFAULT_BASELINE_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "growth_tilt_baseline_transition_trace_source.csv"
)
DEFAULT_CHANNEL_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "channel_specific_v3"
    / "channel_composer_v3_predictions.csv"
)
DEFAULT_COMPILER_TRACE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "channel_specific_v3"
    / "policy_compiler_dry_run.csv"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438M1C_Growth_Tilt_Owner_Mapping_Resolution_And_"
    "PIT_Replay_Pre_Registration_Plan.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_owner_mapping_inventory(
    *,
    channel_config_path: Path = DEFAULT_CHANNEL_CONFIG_PATH,
    signal_usage_matrix_path: Path = DEFAULT_SIGNAL_USAGE_MATRIX_PATH,
    final_matrix_path: Path = DEFAULT_FINAL_MATRIX_PATH,
    composer_config_path: Path = DEFAULT_COMPOSER_CONFIG_PATH,
    base_policy_path: Path = DEFAULT_BASE_POLICY_PATH,
    risk_veto_policy_path: Path = DEFAULT_RISK_VETO_POLICY_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_PATH,
    dynamic_allocation_policy_path: Path = DEFAULT_DYNAMIC_ALLOCATION_POLICY_PATH,
    channel_code_path: Path = DEFAULT_CHANNEL_CODE_PATH,
    compiler_code_path: Path = DEFAULT_COMPILER_CODE_PATH,
    baseline_predictions_path: Path = DEFAULT_BASELINE_PREDICTIONS_PATH,
    channel_predictions_path: Path = DEFAULT_CHANNEL_PREDICTIONS_PATH,
    compiler_trace_path: Path = DEFAULT_COMPILER_TRACE_PATH,
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
        "signal_usage_matrix": signal_usage_matrix_path,
        "final_matrix": final_matrix_path,
        "composer_config": composer_config_path,
        "base_policy": base_policy_path,
        "risk_veto_policy": risk_veto_policy_path,
        "probe_registry": probe_registry_path,
        "dynamic_allocation_policy": dynamic_allocation_policy_path,
        "report_registry": report_registry_path,
    }
    text_paths = {
        "channel_code_text": channel_code_path,
        "compiler_code_text": compiler_code_path,
        "requirement_text": requirement_doc_path,
        "artifact_catalog_text": artifact_catalog_path,
        "system_flow_text": system_flow_path,
    }
    csv_paths = {
        "baseline_predictions": baseline_predictions_path,
        "channel_predictions": channel_predictions_path,
        "compiler_trace": compiler_trace_path,
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
    csv_rows = {key: _load_csv_rows(path) for key, path in csv_paths.items()}
    source_errors = _source_validation_errors(yaml_paths, text_paths, csv_paths)
    source_paths = [*yaml_paths.values(), *text_paths.values(), *csv_paths.values()]
    resolved_as_of = as_of_date or date.today()
    payload = inventory.build_growth_tilt_owner_mapping_inventory(
        sources,
        baseline_prediction_rows=csv_rows["baseline_predictions"],
        channel_prediction_rows=csv_rows["channel_predictions"],
        compiler_trace_rows=csv_rows["compiler_trace"],
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


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _text_value(document: Any) -> str:
    if isinstance(document, Mapping):
        return str(document.get("text", ""))
    return str(document or "")


def _source_validation_errors(
    yaml_paths: Mapping[str, Path],
    text_paths: Mapping[str, Path],
    csv_paths: Mapping[str, Path],
) -> list[str]:
    return [
        f"{source_id} missing: {path}"
        for source_id, path in [
            *yaml_paths.items(),
            *text_paths.items(),
            *csv_paths.items(),
        ]
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
        schema_version = document.get("schema_version")
        if schema_version is None and path.suffix.lower() == ".csv":
            schema_version = _csv_schema_version(path)
        rows.append(
            {
                "path": str(path.resolve()),
                "sha256": _sha256(path),
                "schema_version": schema_version,
                "source_id": source_id,
                "size_bytes": path.stat().st_size,
            }
        )
    return rows


def _csv_schema_version(path: Path) -> str | None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        row = next(csv.DictReader(handle), None)
    if not row:
        return None
    value = row.get("schema_version")
    return str(value) if value else None


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
            "validation_only": True,
            "read_only_inventory": True,
            "observe_only": True,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return enriched


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    sections = {
        "baseline_signal_inventory": "baseline_signal_inventory.json",
        "baseline_confirmation_inventory": "baseline_confirmation_inventory.json",
        "baseline_veto_inventory": "baseline_veto_inventory.json",
        "baseline_regime_inventory": "baseline_regime_inventory.json",
        "baseline_exposure_unit_inventory": "baseline_exposure_unit_inventory.json",
        "baseline_transition_trace_sample": "baseline_transition_trace_sample.json",
    }
    primary_path = output_root / "owner_mapping_inventory_result.json"
    markdown_path = docs_root / "growth_tilt_owner_mapping_inventory.md"
    artifact_paths = {
        "json_path": str(primary_path),
        "markdown_path": str(markdown_path),
        **{section: str(output_root / filename) for section, filename in sections.items()},
    }
    payload["artifact_paths"] = artifact_paths
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
        "baseline_config_id": _as_mapping(payload.get("baseline")).get("config_id"),
        "baseline_binding_status": _as_mapping(payload.get("baseline")).get(
            "binding_status"
        ),
        "do_not_de_risk_pass": payload.get("do_not_de_risk_pass"),
        "risk_on_veto_pass": payload.get("risk_on_veto_pass"),
        "owner_mapping_ready_count": payload.get("owner_mapping_ready_count"),
        "owner_mapping_required_count": payload.get("owner_mapping_required_count"),
        "m2_mapping_status": payload.get("m2_mapping_status"),
        "m2_eligible_candidate_count": payload.get("m2_eligible_candidate_count"),
        "mapping_blocker_codes": payload.get("mapping_blocker_codes"),
        "required_hard_veto_ids": payload.get("required_hard_veto_ids"),
        "unresolved_hard_veto_ids": payload.get("unresolved_hard_veto_ids"),
        "strict_validation_error_count": payload.get("strict_validation_error_count"),
        "next_route": payload.get("next_route"),
    }
    return "\n".join(
        [
            "# Growth Tilt Owner Mapping Inventory",
            "",
            "本报告是只读 baseline contract inventory，不运行 PIT replay、backtest 或 "
            "scoring，也不代表 owner 已完成 preregistration。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Mapping Readiness",
            "",
            "```json",
            _json_block(payload.get("candidate_mapping_readiness", [])),
            "```",
            "",
            "## 结论",
            "",
            "`re_risk_allowed_probability` 有实际生成路径，但 do-not-de-risk channel "
            "未通过最终 selection，且没有 growth-tilt-bound persistence contract。"
            "仓库也没有可证明为 neutral/constructive -> defensive 唯一触发原因的 "
            "callable PIT soft confirmation。因此 A/B 仍不得进入 M2。",
            "",
            "完整 signal、confirmation、veto、regime、exposure 和 transition sample "
            "见同目录 JSON。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
