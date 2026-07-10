from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution as m2438m,  # noqa: E501
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
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
from ai_trading_system.research_quality import (
    growth_tilt_candidate_runtime_spec_threshold_policy_approval as approval,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2438M1"
TASK_REGISTER_ID = (
    "TRADING-2438M1_GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_"
    "APPROVAL"
)
REPORT_TYPE = approval.REPORT_TYPE
SCHEMA_VERSION = approval.SCHEMA_VERSION
READY_STATUS = approval.READY_STATUS
BLOCKED_STATUS = approval.BLOCKED_STATUS
REDEFINE_STATUS = approval.REDEFINE_STATUS
WITHDRAW_STATUS = approval.WITHDRAW_STATUS

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_SOURCE_2438M_PATH = (
    m2438m.DEFAULT_OUTPUT_ROOT
    / "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.json"
)
DEFAULT_OWNER_REVIEW_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "growth_tilt_candidate_runtime_spec_threshold_policy_review.yaml"
)
DEFAULT_METRIC_CONTRACT_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "growth_tilt_candidate_replay_metric_contract.yaml"
)
DEFAULT_THRESHOLD_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "growth_tilt_candidate_pit_screening_policy.yaml"
)
DEFAULT_REQUIREMENT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2438M1_M2_Growth_Tilt_Candidate_Research_Contract_And_"
    "PIT_Replay_Development_Plan.md"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_SYSTEM_FLOW_PATH = PROJECT_ROOT / "docs" / "system_flow.md"


def run_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
    *,
    source_2438m_path: Path = DEFAULT_SOURCE_2438M_PATH,
    owner_review_path: Path = DEFAULT_OWNER_REVIEW_PATH,
    metric_contract_path: Path = DEFAULT_METRIC_CONTRACT_PATH,
    threshold_policy_path: Path = DEFAULT_THRESHOLD_POLICY_PATH,
    requirement_doc_path: Path = DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Path = DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    as_of_date: date | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    sources: dict[str, Any] = {
        "source_2438m": _load_json_document(source_2438m_path),
        "owner_review": _load_yaml_document(owner_review_path),
        "metric_contract": _load_yaml_document(metric_contract_path),
        "threshold_policy": _load_yaml_document(threshold_policy_path),
        "requirement_doc": _load_text_document(requirement_doc_path),
        "report_registry": _load_yaml_document(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
        "system_flow": _load_text_document(system_flow_path),
    }
    source_errors = _source_validation_errors(sources)
    source_documents = [
        (source_2438m_path, sources["source_2438m"]),
        (owner_review_path, sources["owner_review"]),
        (metric_contract_path, sources["metric_contract"]),
        (threshold_policy_path, sources["threshold_policy"]),
        (requirement_doc_path, sources["requirement_doc"]),
        (report_registry_path, sources["report_registry"]),
        (artifact_catalog_path, sources["artifact_catalog"]),
        (system_flow_path, sources["system_flow"]),
    ]
    resolved_as_of = as_of_date or _source_as_of(_as_mapping(sources["source_2438m"]))
    payload = approval.build_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
        _as_mapping(sources["source_2438m"]),
        _as_mapping(sources["owner_review"]),
        metric_contract=_as_mapping(sources["metric_contract"]),
        threshold_policy=_as_mapping(sources["threshold_policy"]),
        source_artifacts=_source_artifact_records(source_documents),
        report_registry=_as_mapping(sources["report_registry"]),
        artifact_catalog_text=str(
            _as_mapping(sources["artifact_catalog"]).get("text", "")
        ),
        system_flow_text=str(_as_mapping(sources["system_flow"]).get("text", "")),
        requirement_text=str(_as_mapping(sources["requirement_doc"]).get("text", "")),
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


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    return [
        f"{name} missing: {document.get('_path')}"
        for name, document in sources.items()
        if isinstance(document, Mapping) and document.get("_missing") is True
    ]


def _source_artifact_records(
    source_documents: list[tuple[Path, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "path": str(path.resolve()),
            "sha256": _sha256(path),
            "schema_version": _as_mapping(document).get("schema_version"),
        }
        for path, document in source_documents
        if path.exists()
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_as_of(source: Mapping[str, Any]) -> date:
    value = source.get("as_of") or source.get("as_of_date")
    if value:
        return date.fromisoformat(str(value))
    return date.today()


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
            "manual_review_only": True,
            "validation_only": True,
            "owner_review_only": True,
            "candidate_only": True,
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
        "candidate_runtime_spec_review_matrix": (
            "candidate_runtime_spec_review_matrix.json"
        ),
        "metric_contract_review_matrix": "metric_contract_review_matrix.json",
        "threshold_policy_review_matrix": "threshold_policy_review_matrix.json",
        "owner_review_validation": "owner_review_validation.json",
        "approved_candidate_runtime_specs": "approved_candidate_runtime_specs.json",
        "owner_action_checklist": "owner_action_checklist.json",
        "no_effect_boundary": "no_effect_boundary.json",
    }
    primary_path = output_root / "approval_readiness_result.json"
    markdown_path = (
        docs_root / "growth_tilt_candidate_runtime_spec_threshold_policy_approval.md"
    )
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
        "source_status": payload.get("source_status"),
        "candidate_count": payload.get("candidate_count"),
        "approved_candidate_count": payload.get("approved_candidate_count"),
        "owner_decision_complete_count": payload.get("owner_decision_complete_count"),
        "pending_candidate_count": payload.get("pending_candidate_count"),
        "redefine_candidate_count": payload.get("redefine_candidate_count"),
        "withdraw_candidate_count": payload.get("withdraw_candidate_count"),
        "runtime_spec_ready_count": payload.get("runtime_spec_ready_count"),
        "metric_contract_ready_count": payload.get("metric_contract_ready_count"),
        "threshold_policy_ready_count": payload.get("threshold_policy_ready_count"),
        "m2_eligible_candidate_count": payload.get("m2_eligible_candidate_count"),
        "m2_eligible_candidate_ids": payload.get("m2_eligible_candidate_ids"),
        "selection_basis": payload.get("selection_basis"),
        "performance_ranked": payload.get("performance_ranked"),
        "owner_input_gap_count": payload.get("owner_input_gap_count"),
        "owner_input_gaps_by_code": payload.get("owner_input_gaps_by_code"),
        "next_route": payload.get("recommended_next_research_task"),
        "data_quality_status": payload.get("data_quality_status"),
    }
    candidate_summary = [
        {
            "candidate_id": item.get("candidate_id"),
            "selection_order": item.get("selection_order"),
            "selection_basis": item.get("selection_basis"),
            "performance_ranked": item.get("performance_ranked"),
            "decision": item.get("decision"),
            "review_status": item.get("review_status"),
            "runtime_spec_ready": item.get("runtime_spec_ready"),
            "metric_contract_ready": item.get("metric_contract_ready"),
            "threshold_policy_ready": item.get("threshold_policy_ready"),
            "m2_eligible": item.get("m2_eligible"),
            "gap_codes": item.get("gap_codes"),
        }
        for item in payload.get("candidate_reviews", [])
        if isinstance(item, Mapping)
    ]
    return "\n".join(
        [
            "# Growth Tilt Candidate Research Contract Approval",
            "",
            f"- task_id: `{TASK_ID}`",
            f"- status: `{payload.get('status')}`",
            f"- requested date: `{payload.get('as_of')}`",
            f"- market regime: `{payload.get('market_regime')}`",
            f"- next route: `{payload.get('recommended_next_research_task')}`",
            "",
            "M1 只验证 owner-review 输入契约，不运行 replay/backtest/scoring，不修改 "
            "candidate parameters 或 threshold values。selection order 来自 config "
            "declaration order，不代表业绩排名。M2 只接收 contract 完整的 APPROVE "
            "候选；REDEFINE/WITHDRAW 不阻断其他候选，但自身不得进入 replay。",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Candidate Review Summary",
            "",
            "```json",
            _json_block(candidate_summary),
            "```",
            "",
            "## Owner Action Checklist",
            "",
            "```json",
            _json_block(payload.get("owner_action_checklist", {})),
            "```",
            "",
            "完整 metric/threshold review matrix 与 source provenance 见同目录 JSON。",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
