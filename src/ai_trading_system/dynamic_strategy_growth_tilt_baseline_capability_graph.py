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
)
from ai_trading_system.research_quality import (
    growth_tilt_baseline_capability_graph as graph,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438N2"
TASK_REGISTER_ID = "TRADING-2438N2_GROWTH_TILT_BASELINE_CAPABILITY_GRAPH"
REPORT_TYPE = graph.REPORT_TYPE
SCHEMA_VERSION = graph.SCHEMA_VERSION

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_GRAPH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "growth_tilt_baseline_capability_graph.yaml"
)
DEFAULT_CLOSURE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_candidate_family_closure"
    / "growth_tilt_candidate_family_closure.json"
)
DEFAULT_ADAPTERS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_baseline_contract_adapters_readiness"
    / "growth_tilt_baseline_contract_adapters_readiness.json"
)
DEFAULT_SIGNAL_INVENTORY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_strategies"
    / "growth_tilt_owner_mapping_inventory"
    / "baseline_signal_inventory.json"
)
DEFAULT_BASE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "base_overlay_veto_policy_schema.yaml"
)
DEFAULT_RISK_VETO_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_on_veto_policy.yaml"
)
DEFAULT_METRIC_CONTRACT_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "growth_tilt_candidate_replay_metric_contract.yaml"
)
DEFAULT_SCREENING_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "growth_tilt_candidate_pit_screening_policy.yaml"
)
DEFAULT_COMPILER_CODE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "two_layer_policy_compiler.py"
)
DEFAULT_EXECUTOR_CODE_PATH = (
    PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "research_quality"
    / "growth_tilt_candidate_overlay_executor.py"
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


def run_growth_tilt_baseline_capability_graph(
    *,
    graph_config_path: Path = DEFAULT_GRAPH_CONFIG_PATH,
    closure_path: Path = DEFAULT_CLOSURE_PATH,
    adapters_path: Path = DEFAULT_ADAPTERS_PATH,
    signal_inventory_path: Path = DEFAULT_SIGNAL_INVENTORY_PATH,
    base_policy_path: Path = DEFAULT_BASE_POLICY_PATH,
    risk_veto_policy_path: Path = DEFAULT_RISK_VETO_POLICY_PATH,
    metric_contract_path: Path = DEFAULT_METRIC_CONTRACT_PATH,
    screening_policy_path: Path = DEFAULT_SCREENING_POLICY_PATH,
    compiler_code_path: Path = DEFAULT_COMPILER_CODE_PATH,
    executor_code_path: Path = DEFAULT_EXECUTOR_CODE_PATH,
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
        "graph_config": graph_config_path,
        "closure": closure_path,
        "adapters": adapters_path,
        "signal_inventory": signal_inventory_path,
        "base_policy": base_policy_path,
        "risk_veto_policy": risk_veto_policy_path,
        "metric_contract": metric_contract_path,
        "screening_policy": screening_policy_path,
        "report_registry": report_registry_path,
    }
    text_paths = {
        "compiler_code_text": compiler_code_path,
        "executor_code_text": executor_code_path,
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
    payload = graph.build_growth_tilt_baseline_capability_graph(
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
    json_path = output_root / "growth_tilt_baseline_capability_graph.json"
    markdown_path = docs_root / "growth_tilt_baseline_capability_graph.md"
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }
    payload["artifact_reload_verified"] = False
    write_json_artifact(json_path, payload)
    payload["artifact_reload_verified"] = (
        json.loads(json_path.read_text(encoding="utf-8")) == payload
    )
    write_json_artifact(json_path, payload)
    if json.loads(json_path.read_text(encoding="utf-8")) != payload:
        raise ValueError("capability graph artifact reload mismatch")
    write_markdown_artifact(markdown_path, _render_markdown(payload))


def _render_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "as_of": payload.get("as_of"),
        "graph_id": payload.get("graph_id"),
        "node_count": payload.get("node_count"),
        "edge_count": payload.get("edge_count"),
        "readiness_status_counts": payload.get("readiness_status_counts"),
        "mutation_ready_capability_count": payload.get(
            "mutation_ready_capability_count"
        ),
        "mutation_ready_capability_ids": payload.get("mutation_ready_capability_ids"),
        "callable_but_unconsumed_capability_ids": payload.get(
            "callable_but_unconsumed_capability_ids"
        ),
        "n3_candidate_generation_allowed": payload.get(
            "n3_candidate_generation_allowed"
        ),
        "n3_status": payload.get("n3_status"),
        "n4_status": payload.get("n4_status"),
        "artifact_reload_verified": payload.get("artifact_reload_verified"),
        "next_route": payload.get("next_route"),
    }
    node_rows = [
        {
            "capability_id": item.get("capability_id"),
            "capability_type": item.get("capability_type"),
            "readiness_status": _as_mapping(item.get("readiness")).get("status"),
            "capability_contract_ready": item.get("capability_contract_ready"),
            "mutation_ready": item.get("mutation_ready"),
            "mutation_blocker_codes": item.get("mutation_blocker_codes"),
        }
        for item in payload.get("nodes", [])
        if isinstance(item, Mapping)
    ]
    return "\n".join(
        [
            "# Growth Tilt Baseline Capability Graph",
            "",
            "该 graph 只读描述 baseline 实际 capability，不创建任何缺失 contract。"
            "Capability READY 也不自动等于 mutation-ready；candidate mutation 还必须"
            "满足 consumption、PIT、runner、mutable dimension和 dependency gates。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Node readiness summary",
            "",
            "```json",
            _json_block(node_rows),
            "```",
            "",
            "## Edges",
            "",
            "```json",
            _json_block(payload.get("edges")),
            "```",
            "",
            "## 结论",
            "",
            "真实 mutation-ready capability 为 0，因此 N3/N4 不启动。现有 callable "
            "signal、单项 hard veto或 cap 不能绕过 authoritative aggregate、requested/"
            "applied transition、native scalar、runner binding和 approved mutable-dimension "
            "gates。后续 baseline capability只能由 candidate-independent project引入。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
