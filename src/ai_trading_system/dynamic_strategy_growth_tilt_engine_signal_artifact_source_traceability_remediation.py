from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck as m2419,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_upstream_artifact_closure as m2417,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_valid_until_dependency_evidence_closure as m2418,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import (
    json_block as _json_block,
)
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
from ai_trading_system.research_quality import (
    growth_tilt_engine_signal_artifact_source_traceability_remediation as remediation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2420"
TASK_REGISTER_ID = (
    "TRADING-2420_GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION"
)
REPORT_TYPE = "growth_tilt_engine_signal_artifact_source_traceability_remediation"
SCHEMA_VERSION = remediation.SCHEMA_VERSION
READY_STATUS = remediation.READY_STATUS
BLOCKED_STATUS = remediation.BLOCKED_MISSING_EVIDENCE_STATUS
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_"
    "PRIOR_ARTIFACTS_REGISTRY_CATALOG_DOCS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "mark_pit_gate_ready",
    "mark_contract_ready",
    "resume_candidate_search",
    "approve_research_only_observation",
    "enable_paper_shadow",
    "create_paper_trade",
    "create_shadow_position",
    "enable_scheduler",
    "append_historical_event_log",
    "bind_outcome",
    "mutate_outcome_store",
    "enable_production",
    "call_broker_api",
    "send_order",
    "create_scheduled_task",
    "generate_daily_report",
    "run_new_strategy_backtest",
    "generate_new_trading_signal",
    "run_scoring",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "pit_gate_ready",
    "contract_ready",
    "auto_mark_pit_gate_ready",
    "auto_mark_contract_ready",
    "blocker_downgraded",
    "candidate_search_enabled",
    "candidate_search_allowed",
    "candidate_search_resumed",
    "observation_enabled",
    "research_only_observation_allowed",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_store_mutated",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_feature_generated",
    "new_signal_generated",
    "new_strategy_backtest_run",
    "scoring_run",
    "fresh_market_data_read",
    "backtest_run",
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT
    / "readiness_recheck_result.json"
)
DEFAULT_SOURCE_2419_BLOCKER_CLASSIFICATION_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT
    / "blocker_classification.json"
)
DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT
    / "growth_tilt_engine_pit_gate_readiness_recheck.md"
)
DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH = (
    m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT
    / "growth_tilt_engine_signal_artifact_source_traceability_blocker.md"
)
DEFAULT_SOURCE_2418_VALID_UNTIL_DEPENDENCY_EVIDENCE_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "valid_until_dependency_evidence.json"
)
DEFAULT_SOURCE_2418_SIGNAL_VALIDITY_CONTRACT_EVIDENCE_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "signal_validity_contract_evidence.json"
)
DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_EVIDENCE_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "stale_signal_policy_evidence.json"
)
DEFAULT_SOURCE_2418_GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "growth_tilt_valid_until_alignment_evidence.json"
)
DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT
    / "growth_tilt_engine_valid_until_dependency_evidence_closure.md"
)
DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "source_traceability_closure_evidence.json"
)
DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "upstream_artifact_closure_evidence.json"
)
DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_DOC_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_DOCS_ROOT
    / "growth_tilt_engine_source_traceability_closure_evidence.md"
)
DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_DOC_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_DOCS_ROOT
    / "growth_tilt_engine_upstream_artifact_closure_evidence.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_signal_artifact_source_traceability_remediation(
    *,
    source_2419_recheck_result_path: Path = DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH,
    source_2419_blocker_classification_path: Path = (
        DEFAULT_SOURCE_2419_BLOCKER_CLASSIFICATION_PATH
    ),
    source_2419_research_doc_path: Path = DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH,
    source_2419_blocker_doc_path: Path = DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH,
    source_2418_valid_until_dependency_evidence_path: Path = (
        DEFAULT_SOURCE_2418_VALID_UNTIL_DEPENDENCY_EVIDENCE_PATH
    ),
    source_2418_signal_validity_contract_evidence_path: Path = (
        DEFAULT_SOURCE_2418_SIGNAL_VALIDITY_CONTRACT_EVIDENCE_PATH
    ),
    source_2418_stale_signal_policy_evidence_path: Path = (
        DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_EVIDENCE_PATH
    ),
    source_2418_growth_tilt_valid_until_alignment_evidence_path: Path = (
        DEFAULT_SOURCE_2418_GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_PATH
    ),
    source_2418_research_doc_path: Path = DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH,
    source_2417_source_traceability_closure_evidence_path: Path = (
        DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH
    ),
    source_2417_upstream_artifact_closure_evidence_path: Path = (
        DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH
    ),
    source_2417_source_traceability_doc_path: Path = (
        DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_DOC_PATH
    ),
    source_2417_upstream_artifact_doc_path: Path = (
        DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_DOC_PATH
    ),
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    artifact_sources = {
        "source_2419_recheck_result": source_2419_recheck_result_path,
        "source_2419_blocker_classification": source_2419_blocker_classification_path,
        "source_2418_valid_until_dependency_evidence": (
            source_2418_valid_until_dependency_evidence_path
        ),
        "source_2418_signal_validity_contract_evidence": (
            source_2418_signal_validity_contract_evidence_path
        ),
        "source_2418_stale_signal_policy_evidence": (
            source_2418_stale_signal_policy_evidence_path
        ),
        "source_2418_growth_tilt_valid_until_alignment_evidence": (
            source_2418_growth_tilt_valid_until_alignment_evidence_path
        ),
        "source_2417_source_traceability_closure_evidence": (
            source_2417_source_traceability_closure_evidence_path
        ),
        "source_2417_upstream_artifact_closure_evidence": (
            source_2417_upstream_artifact_closure_evidence_path
        ),
    }
    document_sources = {
        "source_2419_research_doc": source_2419_research_doc_path,
        "source_2419_blocker_doc": source_2419_blocker_doc_path,
        "source_2418_research_doc": source_2418_research_doc_path,
        "source_2417_source_traceability_doc": source_2417_source_traceability_doc_path,
        "source_2417_upstream_artifact_doc": source_2417_upstream_artifact_doc_path,
    }
    sources: dict[str, Any] = {
        name: _load_json_document(path) for name, path in artifact_sources.items()
    }
    sources.update({name: _load_text_document(path) for name, path in document_sources.items()})
    sources["report_registry"] = safe_load_yaml_path(report_registry_path)
    sources["artifact_catalog"] = _load_text_document(artifact_catalog_path)

    source_file_manifest = _source_file_manifest(
        [
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_pit_gate_readiness_recheck/"
                "readiness_recheck_result.json",
                source_2419_recheck_result_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_pit_gate_readiness_recheck/"
                "blocker_classification.json",
                source_2419_blocker_classification_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_valid_until_dependency_evidence_closure/"
                "valid_until_dependency_evidence.json",
                source_2418_valid_until_dependency_evidence_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_valid_until_dependency_evidence_closure/"
                "signal_validity_contract_evidence.json",
                source_2418_signal_validity_contract_evidence_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_valid_until_dependency_evidence_closure/"
                "stale_signal_policy_evidence.json",
                source_2418_stale_signal_policy_evidence_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_valid_until_dependency_evidence_closure/"
                "growth_tilt_valid_until_alignment_evidence.json",
                source_2418_growth_tilt_valid_until_alignment_evidence_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_source_traceability_upstream_artifact_closure/"
                "source_traceability_closure_evidence.json",
                source_2417_source_traceability_closure_evidence_path,
            ),
            (
                "outputs/research_strategies/"
                "growth_tilt_engine_source_traceability_upstream_artifact_closure/"
                "upstream_artifact_closure_evidence.json",
                source_2417_upstream_artifact_closure_evidence_path,
            ),
        ]
    )
    source_document_manifest = _source_file_manifest(
        [
            (
                "docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md",
                source_2419_research_doc_path,
            ),
            (
                "docs/research/"
                "growth_tilt_engine_signal_artifact_source_traceability_blocker.md",
                source_2419_blocker_doc_path,
            ),
            (
                "docs/research/"
                "growth_tilt_engine_valid_until_dependency_evidence_closure.md",
                source_2418_research_doc_path,
            ),
            (
                "docs/research/"
                "growth_tilt_engine_source_traceability_closure_evidence.md",
                source_2417_source_traceability_doc_path,
            ),
            (
                "docs/research/growth_tilt_engine_upstream_artifact_closure_evidence.md",
                source_2417_upstream_artifact_doc_path,
            ),
        ]
    )
    payload = remediation.build_growth_tilt_signal_artifact_source_traceability_remediation(
        _as_mapping(sources["source_2419_recheck_result"]),
        _as_mapping(sources["source_2418_valid_until_dependency_evidence"]),
        _as_mapping(sources["source_2418_signal_validity_contract_evidence"]),
        _as_mapping(sources["source_2418_stale_signal_policy_evidence"]),
        _as_mapping(sources["source_2418_growth_tilt_valid_until_alignment_evidence"]),
        _as_mapping(sources["source_2417_source_traceability_closure_evidence"]),
        _as_mapping(sources["source_2417_upstream_artifact_closure_evidence"]),
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=_as_mapping(sources["artifact_catalog"]).get("text", ""),
        source_file_manifest=source_file_manifest,
        source_document_manifest=source_document_manifest,
    )
    payload = _with_runtime_metadata(payload, sources=sources, as_of_date=as_of_date)
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    sources: Mapping[str, Any],
    as_of_date: date | None,
) -> dict[str, Any]:
    enriched = dict(payload)
    source_validation_errors = _source_validation_errors(sources)
    if source_validation_errors:
        enriched["status"] = BLOCKED_STATUS
        enriched["remediation_status"] = "BLOCKED"
        enriched["source_traceability_evidence_complete"] = False
        enriched["source_traceability_blocker_resolved"] = False
        enriched["blocker_resolved"] = False
        enriched["recommended_next_research_task"] = remediation.NEXT_ROUTE_BLOCKED
        enriched["recommended_next_research_task_reason"] = (
            "Required source artifacts or documents are missing; remediation cannot pass."
        )
    enriched.update(
        {
            "as_of": str(as_of_date) if as_of_date else enriched.get("as_of"),
            "generated_at": utc_now_iso(),
            "market_regime": AI_REGIME_SUMMARY["market_regime"],
            "market_regime_summary": dict(AI_REGIME_SUMMARY),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "data_quality_gate_executed": False,
            "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
            "manual_review_required": True,
            "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_blocked": True,
        }
    )
    for field in SAFETY_FALSE_FIELDS:
        enriched[field] = False
    return enriched


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for name, document in sources.items():
        if isinstance(document, Mapping) and document.get("_missing") is True:
            errors.append(f"{name} missing: {document.get('_path')}")
    return errors


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "remediation_result.json"
    manifest_json_path = output_root / "source_traceability_manifest.json"
    lineage_json_path = output_root / "source_lineage_map.json"
    missing_json_path = output_root / "missing_source_evidence_summary.json"
    markdown_path = (
        docs_root / "growth_tilt_engine_signal_artifact_source_traceability_remediation.md"
    )
    manifest_markdown_path = (
        docs_root / "growth_tilt_engine_signal_artifact_source_traceability_manifest.md"
    )
    lineage_markdown_path = (
        docs_root / "growth_tilt_engine_signal_artifact_source_lineage_map.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2421_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "source_traceability_manifest_json": str(manifest_json_path),
        "source_lineage_map_json": str(lineage_json_path),
        "missing_source_evidence_summary_json": str(missing_json_path),
        "markdown_path": str(markdown_path),
        "source_traceability_manifest_markdown": str(manifest_markdown_path),
        "source_lineage_map_markdown": str(lineage_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        manifest_json_path,
        "growth_tilt_engine_signal_artifact_source_traceability_manifest",
        remediation.SOURCE_TRACEABILITY_MANIFEST_SCHEMA_VERSION,
        payload,
        "source_traceability_manifest",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        lineage_json_path,
        "growth_tilt_engine_signal_artifact_source_lineage_map",
        remediation.SOURCE_LINEAGE_MAP_SCHEMA_VERSION,
        payload,
        "source_lineage_map",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        missing_json_path,
        "growth_tilt_engine_signal_artifact_missing_source_evidence_summary",
        remediation.MISSING_SOURCE_EVIDENCE_SUMMARY_SCHEMA_VERSION,
        payload,
        "missing_source_evidence_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(manifest_markdown_path, _render_manifest_markdown(payload))
    write_markdown_artifact(lineage_markdown_path, _render_lineage_markdown(payload))
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "remediation_status": payload.get("remediation_status"),
        "artifact_id": payload.get("artifact_id"),
        "source_traceability_evidence_complete": payload.get(
            "source_traceability_evidence_complete"
        ),
        "blocker_resolved": payload.get("blocker_resolved"),
        "pit_gate_ready": payload.get("pit_gate_ready"),
        "contract_ready": payload.get("contract_ready"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine Signal Artifact Source Traceability Remediation"
            "（Source Traceability 修复）",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- artifact_id：`{payload.get('artifact_id')}`",
            "- source traceability complete："
            f"`{payload.get('source_traceability_evidence_complete')}`",
            f"- PIT gate ready：`{payload.get('pit_gate_ready')}`",
            f"- contract ready：`{payload.get('contract_ready')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2420 只补 `growth_tilt_engine_signal_artifact` 的 source traceability "
            "evidence chain。它不生成新 signal、不运行 backtest/scoring、不标记 PIT gate ready，"
            "也不启用 paper-shadow / production / broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Missing Evidence 摘要",
            "",
            "```json",
            _json_block(payload.get("missing_source_evidence_summary")),
            "```",
        ]
    )


def _render_manifest_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine Signal Artifact Source Traceability Manifest（证据链 Manifest）",
            "",
            "```json",
            _json_block(payload.get("source_traceability_manifest")),
            "```",
        ]
    )


def _render_lineage_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine Signal Artifact Source Lineage Map（来源链路图）",
            "",
            "```json",
            _json_block(payload.get("source_lineage_map")),
            "```",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2421 Route（下一跳路线）",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2421 必须独立 recheck PIT gate readiness。2420 的 READY 只表示 "
            "`growth_tilt_engine_signal_artifact` source traceability evidence chain 完整，"
            "不表示 paper-shadow / production / broker 可以启用。",
        ]
    )


def _source_file_manifest(
    path_pairs: list[tuple[str, Path]],
) -> dict[str, Mapping[str, Any]]:
    manifest: dict[str, Mapping[str, Any]] = {}
    for canonical_path, actual_path in path_pairs:
        record = {
            "canonical_path": canonical_path,
            "path": str(actual_path),
            "exists": actual_path.exists(),
            "sha256": _sha256_file(actual_path) if actual_path.exists() else None,
        }
        manifest[canonical_path] = record
        manifest[str(actual_path)] = record
    return manifest


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
