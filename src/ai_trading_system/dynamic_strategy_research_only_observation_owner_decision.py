from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_replay_validation import (
    READY_STATUS as SOURCE_2370_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2371"
TASK_REGISTER_ID = (
    "TRADING-2371_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_"
    "OWNER_REVIEW_DECISION"
)
REPORT_TYPE = "dynamic_strategy_research_only_shadow_observation_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_research_only_shadow_observation_owner_review_decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_"
    "OWNER_REVIEW_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_"
    "OWNER_REVIEW_DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = "APPROVE_RESEARCH_ONLY_OBSERVATION_CONTINUE_WITH_NO_EXECUTION"
NEXT_ROUTE = (
    "TRADING-2372_Dynamic_Strategy_Research_Only_Observation_"
    "Log_Schema_And_Report_Plan"
)
SOURCE_2370_ROUTE = (
    "TRADING-2371_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Owner_Review_Decision"
)
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
OBSERVATION_DECISION_REQUIRED = "OWNER_REVIEW_REQUIRED"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2367",
    "TRADING-2368",
    "TRADING-2369",
    "TRADING-2370",
)

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_REPLAY_VALIDATION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_VALIDATION_OUTPUT_ROOT
    / "replay_validation_result.json"
)


def run_dynamic_strategy_research_only_shadow_observation_owner_review_decision(
    *,
    source_replay_validation_path: Path = DEFAULT_SOURCE_REPLAY_VALIDATION_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    source = _load_json_document(source_replay_validation_path)
    source_map = _as_mapping(source)
    validation_errors = _source_validation_errors(source_map)
    ready = not validation_errors
    resolved_as_of = _resolve_as_of(as_of_date, source_map)
    decision_record = _owner_decision_record(
        source=source_map,
        as_of_date=resolved_as_of,
        ready=ready,
    )
    evidence = _no_side_effect_evidence(decision_record=decision_record)
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        source=source_map,
        source_path=source_replay_validation_path,
        validation_errors=validation_errors,
        decision_record=decision_record,
        evidence=evidence,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _source_validation_errors(source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if source.get("status") != SOURCE_2370_READY_STATUS:
        errors.append("replay_validation_status_not_ready")
    if source.get("next_route") != SOURCE_2370_ROUTE:
        errors.append("replay_validation_next_route_not_trading_2371")
    if source.get("stable_semantic_replay_passed") is not True:
        errors.append("stable_semantic_replay_not_passed")
    if source.get("no_side_effect_evidence_ready") is not True:
        errors.append("no_side_effect_evidence_not_ready")
    if source.get("observation_decision") != OBSERVATION_DECISION_REQUIRED:
        errors.append("observation_decision_not_owner_review_required")
    if source.get("owner_review_required") is not True:
        errors.append("owner_review_required_not_true")
    if source.get("research_only_shadow_observation_allowed") is not True:
        errors.append("research_only_shadow_observation_not_allowed")
    for field in _side_effect_false_fields():
        if bool(source.get(field)):
            errors.append(f"{field}_true")
    if source.get("broker_action") not in (None, "none"):
        errors.append("broker_action_not_none")
    if source.get("production_effect") not in (None, "none"):
        errors.append("production_effect_not_none")
    return errors


def _owner_decision_record(
    *,
    source: Mapping[str, Any],
    as_of_date: date,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_shadow_observation_owner_decision.v1",
        "decision_id": f"{TASK_ID}_{as_of_date.isoformat()}",
        "generated_by_task": TASK_ID,
        "as_of": as_of_date.isoformat(),
        "source_task": "TRADING-2370",
        "source_status": source.get("status"),
        "source_route_confirmed": source.get("next_route") == SOURCE_2370_ROUTE,
        "owner_review_decision_recorded": ready,
        "owner_decision": OWNER_DECISION,
        "decision_scope": "RESEARCH_ONLY_OBSERVATION_CONTINUATION_NO_EXECUTION",
        "research_only_observation_continue_allowed": ready,
        "primary_observation_candidate": _primary_candidate(source),
        "ranking_top_from_2365": source.get("ranking_top_from_2365")
        or RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": source.get("robustness_top_from_2366")
        or PRIMARY_CANDIDATE_FALLBACK,
        "observation_decision_from_2370": source.get("observation_decision"),
        "owner_review_required_from_2370": source.get("owner_review_required"),
        "non_approved_paths": [
            "paper_shadow",
            "paper_trade",
            "shadow_position",
            "event_append",
            "outcome_binding",
            "scheduler",
            "production",
            "broker",
            "daily_report",
        ],
        "decision_reasons": [
            "TRADING-2370 replay validation is ready",
            "stable semantic hash replay passed",
            "no-side-effect evidence remains pass",
            "ranking top and robustness top still require owner-visible review",
        ],
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _no_side_effect_evidence(
    *,
    decision_record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_shadow_observation_owner_decision_no_side_effect.v1",
        "status": "PASS",
        "decision_id": decision_record.get("decision_id"),
        "owner_decision": decision_record.get("owner_decision"),
        "event_append_enabled": False,
        "event_append_approved": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_enabled": False,
        "production_approved": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    source: Mapping[str, Any],
    source_path: Path,
    validation_errors: list[str],
    decision_record: Mapping[str, Any],
    evidence: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifact": _source_artifact(source_path, source),
        "source_status": source.get("status"),
        "source_ready_for_owner_review_decision": ready,
        "source_validation_errors": validation_errors,
        "owner_review_decision_recorded": ready,
        "owner_decision": decision_record.get("owner_decision"),
        "research_only_observation_continue_allowed": ready,
        "primary_observation_candidate": _primary_candidate(source),
        "ranking_top_from_2365": source.get("ranking_top_from_2365")
        or RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": source.get("robustness_top_from_2366")
        or PRIMARY_CANDIDATE_FALLBACK,
        "execution_cadence": source.get("execution_cadence"),
        "observation_decision_from_2370": source.get("observation_decision"),
        "owner_review_required_from_2370": source.get("owner_review_required"),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _as_mapping(source.get("requested_date_range")),
        "data_quality": _as_mapping(source.get("data_quality")),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "owner_review_decision_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_shadow_attempted": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_approved": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
        "owner_review_decision_record": dict(decision_record),
        "no_side_effect_evidence": dict(evidence),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "summary_findings": {
            "owner_decision_recorded": ready,
            "research_only_observation_continue_allowed": ready,
            "paper_shadow_remains_disallowed": True,
            "event_and_outcome_paths_remain_disabled": True,
            "broker_path_remains_disabled": True,
            "next_route": NEXT_ROUTE,
        },
        "required_outputs_ready": _required_outputs_ready(ready),
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
        "json_path": str(output_root / "owner_review_decision_result.json"),
        "owner_review_decision_record_json": str(
            output_root / "owner_review_decision_record.json"
        ),
        "no_side_effect_evidence_json": str(
            output_root / "no_side_effect_evidence.json"
        ),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_research_only_shadow_observation_owner_review_decision.md"
        ),
        "owner_review_decision_record_markdown": str(
            docs_root / "dynamic_strategy_shadow_observation_owner_decision_record.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2372_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["owner_review_decision_record_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_owner_decision_record",
            "schema_version": "dynamic_strategy_shadow_observation_owner_decision.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "owner_review_decision_record": payload["owner_review_decision_record"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(artifact_paths["no_side_effect_evidence_json"]),
        {
            "report_type": "dynamic_strategy_shadow_observation_owner_decision_no_side_effect",
            "schema_version": (
                "dynamic_strategy_shadow_observation_owner_decision_no_side_effect.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "no_side_effect_evidence": payload["no_side_effect_evidence"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["owner_review_decision_record_markdown"]).write_text(
        _decision_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload), encoding="utf-8"
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 research-only shadow observation owner review decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            (
                "- research-only observation continue allowed："
                f"`{payload.get('research_only_observation_continue_allowed')}`"
            ),
            f"- primary observation candidate：`{payload.get('primary_observation_candidate')}`",
            f"- next route：`{payload.get('next_route')}`",
            "",
            "## Explicit non-approvals",
            "",
            "- 不允许 paper-shadow。",
            "- 不允许 paper trade 或 shadow position。",
            "- 不允许 event append 或 outcome binding。",
            "- 不允许 scheduler 或 scheduled task。",
            "- 不允许 production、broker、order 或 daily report。",
        ]
    )


def _decision_markdown(payload: Mapping[str, Any]) -> str:
    record = _as_mapping(payload.get("owner_review_decision_record"))
    return "\n".join(
        [
            "# 动态策略 shadow observation owner decision record",
            "",
            f"- decision id：`{record.get('decision_id')}`",
            f"- owner decision：`{record.get('owner_decision')}`",
            f"- decision scope：`{record.get('decision_scope')}`",
            (
                "- research-only observation continue allowed："
                f"`{record.get('research_only_observation_continue_allowed')}`"
            ),
            f"- next route：`{record.get('recommended_next_research_task')}`",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2372 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            (
                "- route boundary：research-only observation log schema and report "
                "plan；不是 daily report、scheduler、paper-shadow、production 或 broker。"
            ),
        ]
    )


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


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "owner_review_decision_recorded": ready,
        "owner_decision": ready,
        "research_only_observation_continue_allowed": ready,
        "paper_shadow_enabled_false": ready,
        "paper_shadow_approved_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "event_append_approved_false": ready,
        "outcome_binding_enabled_false": ready,
        "outcome_binding_approved_false": ready,
        "scheduler_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
        "recommended_next_research_task": ready,
    }


def _primary_candidate(source: Mapping[str, Any]) -> str:
    return str(
        source.get("primary_observation_candidate") or PRIMARY_CANDIDATE_FALLBACK
    )


def _resolve_as_of(as_of_date: date | None, source: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    raw = source.get("as_of")
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            pass
    return date.today()


def _source_artifact(path: Path, document: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _safe_sha256(path),
        "status": document.get("status"),
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


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
