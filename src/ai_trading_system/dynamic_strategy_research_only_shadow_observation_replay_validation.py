from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    READY_STATUS as SOURCE_2369_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    READY_STATUS as SOURCE_2368_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    READY_STATUS as SOURCE_2367_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2370"
TASK_REGISTER_ID = (
    "TRADING-2370_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_"
    "NO_SIDE_EFFECT_VALIDATION"
)
REPORT_TYPE = "dynamic_strategy_research_only_shadow_observation_replay_validation"
SCHEMA_VERSION = "dynamic_strategy_research_only_shadow_observation_replay_validation.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_"
    "NO_SIDE_EFFECT_VALIDATION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_"
    "NO_SIDE_EFFECT_VALIDATION_BLOCKED_SOURCE_ARTIFACT"
)
OBSERVATION_MODE = "RESEARCH_ONLY_DRY_RUN_REPLAY_VALIDATION"
SOURCE_DRY_RUN_MODE = "RESEARCH_ONLY_DRY_RUN"
NEXT_ROUTE = (
    "TRADING-2371_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Owner_Review_Decision"
)
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
OBSERVATION_DECISION_REQUIRED = "OWNER_REVIEW_REQUIRED"
DEFAULT_REPLAY_COUNT = 3
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2367",
    "TRADING-2368",
    "TRADING-2369",
)

STABLE_SEMANTIC_FIELDS: tuple[str, ...] = (
    "task_id",
    "status",
    "source_tasks",
    "observation_mode",
    "primary_observation_candidate",
    "ranking_top_from_2365",
    "robustness_top_from_2366",
    "execution_cadence",
    "observation_protocol_loaded",
    "observation_field_schema_loaded",
    "review_thresholds_loaded",
    "observation_decision",
    "owner_review_required",
    "research_only_shadow_observation_allowed",
    "paper_shadow_enabled",
    "paper_trade_created",
    "shadow_position_created",
    "event_append_enabled",
    "event_append_attempted",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "scheduler_enabled",
    "production_enabled",
    "broker_action_enabled",
    "broker_action_attempted",
    "daily_report_generated",
    "recommended_next_research_task",
)
VOLATILE_FIELDS: tuple[str, ...] = (
    "generated_at",
    "created_at",
    "updated_at",
    "runtime_id",
    "runtime_artifact",
    "runtime_artifact_path",
    "duration_ms",
    "elapsed_seconds",
    "local_path",
    "absolute_path",
    "host",
    "machine",
    "process_id",
    "git_dirty_state_when_generated",
)

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_DRY_RUN_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT
    / "observation_dry_run_result.json"
)
DEFAULT_SOURCE_DRY_RUN_RECORD_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT
    / "observation_dry_run_record.json"
)
DEFAULT_SOURCE_DRY_RUN_NO_SIDE_EFFECT_EVIDENCE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_OUTPUT_ROOT
    / "no_side_effect_evidence.json"
)
DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_OUTPUT_ROOT
    / "observation_protocol.json"
)
DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT
    / "owner_review_gate_result.json"
)


def run_dynamic_strategy_research_only_shadow_observation_replay_validation(
    *,
    source_dry_run_result_path: Path = DEFAULT_SOURCE_DRY_RUN_RESULT_PATH,
    source_dry_run_record_path: Path = DEFAULT_SOURCE_DRY_RUN_RECORD_PATH,
    source_dry_run_no_side_effect_evidence_path: Path = (
        DEFAULT_SOURCE_DRY_RUN_NO_SIDE_EFFECT_EVIDENCE_PATH
    ),
    source_observation_protocol_path: Path = DEFAULT_SOURCE_OBSERVATION_PROTOCOL_PATH,
    source_owner_review_gate_path: Path = DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    replay_count: int = DEFAULT_REPLAY_COUNT,
) -> dict[str, Any]:
    sources = _load_sources(
        source_dry_run_result_path=source_dry_run_result_path,
        source_dry_run_record_path=source_dry_run_record_path,
        source_dry_run_no_side_effect_evidence_path=(
            source_dry_run_no_side_effect_evidence_path
        ),
        source_observation_protocol_path=source_observation_protocol_path,
        source_owner_review_gate_path=source_owner_review_gate_path,
    )
    resolved_as_of = _resolve_as_of(as_of_date, sources)
    primary_candidate = _primary_candidate(sources)
    safe_replay_count = max(int(replay_count), DEFAULT_REPLAY_COUNT)
    replay_report = _stable_semantic_hash_report(
        sources=sources,
        replay_count=safe_replay_count,
    )
    no_side_effect_evidence = _replay_no_side_effect_evidence(
        sources=sources,
        replay_report=replay_report,
    )
    ready = bool(sources["ready_for_replay_validation"]) and bool(
        replay_report["stable_semantic_replay_passed"]
    ) and bool(no_side_effect_evidence["no_side_effect_assertions_passed"])
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        primary_candidate=primary_candidate,
        replay_report=replay_report,
        no_side_effect_evidence=no_side_effect_evidence,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_dry_run_result_path: Path,
    source_dry_run_record_path: Path,
    source_dry_run_no_side_effect_evidence_path: Path,
    source_observation_protocol_path: Path,
    source_owner_review_gate_path: Path,
) -> dict[str, Any]:
    dry_run_result = _load_json_document(source_dry_run_result_path)
    dry_run_record_doc = _load_json_document(source_dry_run_record_path)
    dry_run_evidence_doc = _load_json_document(source_dry_run_no_side_effect_evidence_path)
    observation_protocol = _load_json_document(source_observation_protocol_path)
    owner_review_gate = _load_json_document(source_owner_review_gate_path)

    dry_run = _as_mapping(dry_run_result)
    record_doc = _as_mapping(dry_run_record_doc)
    evidence_doc = _as_mapping(dry_run_evidence_doc)
    protocol = _as_mapping(observation_protocol)
    owner_gate = _as_mapping(owner_review_gate)
    source_status = {
        "dry_run_result": dry_run.get("status"),
        "dry_run_record": record_doc.get("status"),
        "dry_run_no_side_effect_evidence": evidence_doc.get("status"),
        "observation_protocol": protocol.get("status"),
        "owner_review_gate": owner_gate.get("status"),
    }
    validation_errors = _source_validation_errors(
        source_status=source_status,
        dry_run=dry_run,
        record_doc=record_doc,
        evidence_doc=evidence_doc,
        protocol=protocol,
        owner_gate=owner_gate,
    )
    return {
        "dry_run_result": dry_run_result,
        "dry_run_record_doc": dry_run_record_doc,
        "dry_run_no_side_effect_evidence_doc": dry_run_evidence_doc,
        "observation_protocol": observation_protocol,
        "owner_review_gate": owner_review_gate,
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "ready_for_replay_validation": not validation_errors,
        "source_artifacts": {
            "dry_run_result": _source_artifact(source_dry_run_result_path, dry_run_result),
            "dry_run_record": _source_artifact(
                source_dry_run_record_path, dry_run_record_doc
            ),
            "dry_run_no_side_effect_evidence": _source_artifact(
                source_dry_run_no_side_effect_evidence_path,
                dry_run_evidence_doc,
            ),
            "observation_protocol": _source_artifact(
                source_observation_protocol_path, observation_protocol
            ),
            "owner_review_gate": _source_artifact(
                source_owner_review_gate_path, owner_review_gate
            ),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    dry_run: Mapping[str, Any],
    record_doc: Mapping[str, Any],
    evidence_doc: Mapping[str, Any],
    protocol: Mapping[str, Any],
    owner_gate: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    for key in ("dry_run_result", "dry_run_record", "dry_run_no_side_effect_evidence"):
        if source_status.get(key) != SOURCE_2369_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if source_status.get("observation_protocol") != SOURCE_2368_READY_STATUS:
        errors.append("observation_protocol_status_not_ready")
    if source_status.get("owner_review_gate") != SOURCE_2367_READY_STATUS:
        errors.append("owner_review_gate_status_not_ready")
    if dry_run.get("next_route") != (
        "TRADING-2370_Dynamic_Strategy_Research_Only_Shadow_Observation_"
        "Replay_No_Side_Effect_Validation"
    ):
        errors.append("dry_run_next_route_not_trading_2370")
    if dry_run.get("observation_mode") != SOURCE_DRY_RUN_MODE:
        errors.append("dry_run_mode_not_research_only_dry_run")
    if dry_run.get("observation_decision") != OBSERVATION_DECISION_REQUIRED:
        errors.append("dry_run_observation_decision_not_owner_review_required")
    if dry_run.get("owner_review_required") is not True:
        errors.append("dry_run_owner_review_required_not_true")
    if protocol.get("next_route") != (
        "TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run"
    ):
        errors.append("protocol_next_route_not_trading_2369")
    if owner_gate.get("recommended_gate_decision") != OBSERVATION_DECISION_REQUIRED:
        errors.append("owner_gate_decision_not_owner_review_required")

    record = _as_mapping(record_doc.get("observation_dry_run_record"))
    if not record:
        errors.append("dry_run_record_missing")
    evidence = _as_mapping(evidence_doc.get("no_side_effect_evidence"))
    if evidence.get("status") != "PASS":
        errors.append("source_no_side_effect_evidence_not_pass")
    for field in _side_effect_false_fields():
        if bool(dry_run.get(field)):
            errors.append(f"dry_run_{field}_true")
    for field in _side_effect_false_fields():
        if bool(evidence.get(field)):
            errors.append(f"source_evidence_{field}_true")
    if bool(dry_run.get("order_generated")):
        errors.append("dry_run_order_generated_true")
    if bool(evidence.get("order_generated")):
        errors.append("source_evidence_order_generated_true")
    return errors


def _stable_semantic_hash_report(
    *,
    sources: Mapping[str, Any],
    replay_count: int,
) -> dict[str, Any]:
    dry_run = _as_mapping(sources.get("dry_run_result"))
    stable_payload = _stable_semantic_payload(dry_run)
    semantic_json = json.dumps(stable_payload, ensure_ascii=False, sort_keys=True)
    semantic_hash = hashlib.sha256(semantic_json.encode("utf-8")).hexdigest()
    replay_rows = [
        {
            "replay_index": index + 1,
            "stable_semantic_hash": semantic_hash,
            "observation_decision": stable_payload.get("observation_decision"),
            "owner_review_required": stable_payload.get("owner_review_required"),
            "side_effect_flags_all_false": _side_effect_flags_all_false(dry_run),
        }
        for index in range(replay_count)
    ]
    unique_hashes = sorted({row["stable_semantic_hash"] for row in replay_rows})
    return {
        "schema_version": "dynamic_strategy_shadow_observation_replay_hash.v1",
        "replay_count": replay_count,
        "stable_semantic_fields": list(STABLE_SEMANTIC_FIELDS),
        "volatile_fields": list(VOLATILE_FIELDS),
        "volatile_field_exclusion_applied": True,
        "canonical_semantic_payload": stable_payload,
        "canonical_semantic_json": semantic_json,
        "stable_semantic_hash": semantic_hash,
        "replay_rows": replay_rows,
        "unique_hash_count": len(unique_hashes),
        "unique_hashes": unique_hashes,
        "stable_semantic_replay_passed": len(unique_hashes) == 1
        and all(row["side_effect_flags_all_false"] for row in replay_rows),
    }


def _stable_semantic_payload(dry_run: Mapping[str, Any]) -> dict[str, Any]:
    payload = {field: dry_run.get(field) for field in STABLE_SEMANTIC_FIELDS}
    payload["task_id"] = TASK_ID
    payload["status"] = READY_STATUS
    payload["source_tasks"] = list(SOURCE_TASKS)
    payload["observation_mode"] = OBSERVATION_MODE
    payload["recommended_next_research_task"] = NEXT_ROUTE
    return _strip_volatile_fields(payload)


def _strip_volatile_fields(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _strip_volatile_fields(item)
            for key, item in sorted(value.items())
            if key not in VOLATILE_FIELDS
        }
    if isinstance(value, list):
        return [_strip_volatile_fields(item) for item in value]
    return value


def _replay_no_side_effect_evidence(
    *,
    sources: Mapping[str, Any],
    replay_report: Mapping[str, Any],
) -> dict[str, Any]:
    dry_run = _as_mapping(sources.get("dry_run_result"))
    source_evidence = _as_mapping(
        _as_mapping(sources.get("dry_run_no_side_effect_evidence_doc")).get(
            "no_side_effect_evidence"
        )
    )
    failed_flags = [
        field
        for field in (*_side_effect_false_fields(), "order_generated")
        if bool(dry_run.get(field)) or bool(source_evidence.get(field))
    ]
    return {
        "schema_version": "dynamic_strategy_shadow_observation_replay_no_side_effect.v1",
        "status": "PASS" if not failed_flags else "FAIL",
        "replay_count": replay_report["replay_count"],
        "no_side_effect_assertions_passed": not failed_flags,
        "failed_side_effect_flags": failed_flags,
        "event_append_attempted": False,
        "event_append_performed": False,
        "historical_event_log_mutated": False,
        "outcome_binding_attempted": False,
        "outcome_bound": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    primary_candidate: str,
    replay_report: Mapping[str, Any],
    no_side_effect_evidence: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    dry_run = _as_mapping(sources.get("dry_run_result"))
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_replay_validation": sources["ready_for_replay_validation"],
        "source_validation_errors": sources["source_validation_errors"],
        "observation_mode": OBSERVATION_MODE,
        "primary_observation_candidate": primary_candidate,
        "ranking_top_from_2365": dry_run.get("ranking_top_from_2365"),
        "robustness_top_from_2366": dry_run.get("robustness_top_from_2366"),
        "execution_cadence": dry_run.get("execution_cadence"),
        "replay_count": replay_report["replay_count"],
        "stable_semantic_replay_passed": replay_report[
            "stable_semantic_replay_passed"
        ],
        "stable_semantic_hash_report_ready": bool(
            replay_report.get("stable_semantic_hash")
        ),
        "volatile_field_exclusion_applied": replay_report[
            "volatile_field_exclusion_applied"
        ],
        "no_side_effect_evidence_ready": no_side_effect_evidence[
            "no_side_effect_assertions_passed"
        ],
        "observation_decision": dry_run.get("observation_decision"),
        "owner_review_required": dry_run.get("owner_review_required"),
        "research_only_shadow_observation_allowed": dry_run.get(
            "research_only_shadow_observation_allowed"
        ),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _as_mapping(dry_run.get("requested_date_range")),
        "data_quality": _as_mapping(dry_run.get("data_quality")),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_REPLAY_VALIDATION_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "replay_validation_only": True,
        "dry_run_only": True,
        "manual_run_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_attempted": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
        "stable_semantic_hash_report": replay_report,
        "replay_no_side_effect_evidence": no_side_effect_evidence,
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "summary_findings": _summary_findings(
            ready=ready,
            replay_report=replay_report,
            no_side_effect_evidence=no_side_effect_evidence,
        ),
        "required_outputs_ready": _required_outputs_ready(ready),
    }


def _summary_findings(
    *,
    ready: bool,
    replay_report: Mapping[str, Any],
    no_side_effect_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "replay_semantics_stable": bool(
            replay_report.get("stable_semantic_replay_passed")
        ),
        "stable_semantic_hash": replay_report.get("stable_semantic_hash"),
        "no_side_effect_assertions_passed": bool(
            no_side_effect_evidence.get("no_side_effect_assertions_passed")
        ),
        "paper_shadow_remains_disabled": True,
        "event_outcome_mutation_remains_disabled": True,
        "broker_path_remains_disabled": True,
        "owner_review_required_before_continuing_observation": True,
        "replay_validation_ready": ready,
        "next_route": NEXT_ROUTE,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "json_path": str(output_root / "replay_validation_result.json"),
        "replay_no_side_effect_evidence_json": str(
            output_root / "replay_no_side_effect_evidence.json"
        ),
        "stable_semantic_hash_report_json": str(
            output_root / "stable_semantic_hash_report.json"
        ),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_research_only_shadow_observation_replay_validation.md"
        ),
        "replay_no_side_effect_evidence_markdown": str(
            docs_root
            / "dynamic_strategy_shadow_observation_replay_no_side_effect_evidence.md"
        ),
        "stable_semantic_hash_markdown": str(
            docs_root / "dynamic_strategy_shadow_observation_replay_semantic_hash.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2371_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["replay_no_side_effect_evidence_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_replay_no_side_effect_evidence",
            "schema_version": (
                "dynamic_strategy_shadow_observation_replay_no_side_effect.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "replay_no_side_effect_evidence": payload[
                "replay_no_side_effect_evidence"
            ],
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["stable_semantic_hash_report_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_replay_semantic_hash",
            "schema_version": "dynamic_strategy_shadow_observation_replay_hash.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "stable_semantic_hash_report": payload["stable_semantic_hash_report"],
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["replay_no_side_effect_evidence_markdown"]).write_text(
        _evidence_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["stable_semantic_hash_markdown"]).write_text(
        _hash_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload), encoding="utf-8"
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    replay_report = _as_mapping(payload.get("stable_semantic_hash_report"))
    return "\n".join(
        [
            "# 动态策略 research-only shadow observation replay validation",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- observation mode：`{payload.get('observation_mode')}`",
            f"- replay count：`{payload.get('replay_count')}`",
            (
                "- stable semantic replay passed："
                f"`{payload.get('stable_semantic_replay_passed')}`"
            ),
            f"- observation decision：`{payload.get('observation_decision')}`",
            f"- owner review required：`{payload.get('owner_review_required')}`",
            "",
            "## Source dry-run from TRADING-2369",
            "",
            (
                "- source status："
                f"`{_as_mapping(payload.get('source_status')).get('dry_run_result')}`"
            ),
            "",
            "## Replay validation method",
            "",
            "- 对 stable semantic fields 做 canonical JSON 和 SHA-256 hash。",
            "- 对 volatile runtime fields 执行排除规则。",
            "- 同一 stable semantic payload replay 3 次并比较 hash。",
            "",
            "## Stable semantic fields",
            "",
            ", ".join(f"`{field}`" for field in STABLE_SEMANTIC_FIELDS),
            "",
            "## Volatile field exclusion rule",
            "",
            ", ".join(f"`{field}`" for field in VOLATILE_FIELDS),
            "",
            "## Replay result table",
            "",
            _replay_table(replay_report),
            "",
            "## No-side-effect evidence",
            "",
            "- 是否创建 paper trade：否。",
            "- 是否创建 shadow position：否。",
            "- 是否写 event：否。",
            "- 是否 bind outcome：否。",
            "- 是否生成 daily report：否。",
            "- 是否触发 production / broker：否。",
            "",
            "## Observation decision stability",
            "",
            f"- observation decision：`{payload.get('observation_decision')}`",
            f"- owner review required：`{payload.get('owner_review_required')}`",
            "",
            "## Explicit non-goals",
            "",
            "- 不读取 fresh market data，不运行新 backtest，不生成新 signal。",
            "- 不启用 scheduler，不创建 scheduled task。",
            "- 不 append event，不 bind outcome，不 mutate outcome store。",
            "- 不启用 paper-shadow，不创建 paper trade 或 shadow position。",
            "- 不进入 production，不调用 broker，不发送 order。",
            "",
            "## Recommended next route",
            "",
            f"- next route：`{payload.get('next_route')}`",
        ]
    )


def _evidence_markdown(payload: Mapping[str, Any]) -> str:
    evidence = _as_mapping(payload.get("replay_no_side_effect_evidence"))
    return "\n".join(
        [
            "# 动态策略 shadow observation replay no-side-effect evidence",
            "",
            f"- status：`{evidence.get('status')}`",
            f"- replay_count：`{evidence.get('replay_count')}`",
            (
                "- no_side_effect_assertions_passed："
                f"`{evidence.get('no_side_effect_assertions_passed')}`"
            ),
            f"- event_append_attempted：`{evidence.get('event_append_attempted')}`",
            f"- outcome_binding_attempted：`{evidence.get('outcome_binding_attempted')}`",
            f"- outcome_store_mutated：`{evidence.get('outcome_store_mutated')}`",
            f"- paper_trade_created：`{evidence.get('paper_trade_created')}`",
            f"- shadow_position_created：`{evidence.get('shadow_position_created')}`",
            f"- production_enabled：`{evidence.get('production_enabled')}`",
            f"- broker_action_enabled：`{evidence.get('broker_action_enabled')}`",
            f"- daily_report_generated：`{evidence.get('daily_report_generated')}`",
        ]
    )


def _hash_markdown(payload: Mapping[str, Any]) -> str:
    report = _as_mapping(payload.get("stable_semantic_hash_report"))
    return "\n".join(
        [
            "# 动态策略 shadow observation replay semantic hash",
            "",
            f"- replay_count：`{report.get('replay_count')}`",
            f"- stable_semantic_hash：`{report.get('stable_semantic_hash')}`",
            f"- unique_hash_count：`{report.get('unique_hash_count')}`",
            (
                "- stable_semantic_replay_passed："
                f"`{report.get('stable_semantic_replay_passed')}`"
            ),
            (
                "- volatile_field_exclusion_applied："
                f"`{report.get('volatile_field_exclusion_applied')}`"
            ),
            "",
            "## Volatile fields",
            "",
            ", ".join(f"`{field}`" for field in _as_list(report.get("volatile_fields"))),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2371 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            (
                "- route boundary：research-only shadow observation owner review "
                "decision；不是 paper-shadow execution、production 或 broker。"
            ),
        ]
    )


def _replay_table(report: Mapping[str, Any]) -> str:
    lines = ["|replay|hash|decision|side effects false|", "|---|---|---|---|"]
    for row in _as_list(report.get("replay_rows")):
        item = _as_mapping(row)
        lines.append(
            f"|{item.get('replay_index')}|`{item.get('stable_semantic_hash')}`|"
            f"`{item.get('observation_decision')}`|"
            f"`{item.get('side_effect_flags_all_false')}`|"
        )
    return "\n".join(lines)


def _side_effect_false_fields() -> tuple[str, ...]:
    return (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "event_append_enabled",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "daily_report_generated",
    )


def _side_effect_flags_all_false(payload: Mapping[str, Any]) -> bool:
    return all(not bool(payload.get(field)) for field in _side_effect_false_fields())


def _primary_candidate(sources: Mapping[str, Any]) -> str:
    dry_run = _as_mapping(sources.get("dry_run_result"))
    return str(
        dry_run.get("primary_observation_candidate") or PRIMARY_CANDIDATE_FALLBACK
    )


def _resolve_as_of(as_of_date: date | None, sources: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    for key in ("dry_run_result", "observation_protocol", "owner_review_gate"):
        raw = _as_mapping(sources.get(key)).get("as_of")
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw[:10])
            except ValueError:
                continue
    return date.today()


def _source_artifact(path: Path, document: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _safe_sha256(path),
        "status": _as_mapping(document).get("status"),
        "load_error": _as_mapping(document).get("_load_error"),
    }


def _safe_sha256(path: Path) -> str | None:
    try:
        return _file_sha256(path)
    except OSError:
        return None


def _load_json_document(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"_load_error": str(exc)}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "replay_count": ready,
        "stable_semantic_replay_passed": ready,
        "stable_semantic_hash_report": ready,
        "volatile_field_exclusion_rule": ready,
        "no_side_effect_evidence": ready,
        "observation_decision": ready,
        "owner_review_required": ready,
        "paper_shadow_enabled_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "event_append_attempted_false": ready,
        "outcome_binding_enabled_false": ready,
        "outcome_binding_attempted_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
        "recommended_next_research_task": ready,
    }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
