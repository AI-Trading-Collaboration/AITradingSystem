from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_research_only_observation_report_dry_run import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_observation_report_dry_run import (
    NEXT_ROUTE as SOURCE_2373_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_observation_report_dry_run import (
    READY_STATUS as SOURCE_2373_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_research_only_observation_report_dry_run import (
    REPORT_MODE as SOURCE_2373_REPORT_MODE,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2374"
TASK_REGISTER_ID = (
    "TRADING-2374_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "OWNER_REASSESSMENT_CHECKPOINT"
)
REPORT_TYPE = "dynamic_strategy_research_only_observation_owner_reassessment"
SCHEMA_VERSION = "dynamic_strategy_research_only_observation_owner_reassessment.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "OWNER_REASSESSMENT_CHECKPOINT_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "OWNER_REASSESSMENT_CHECKPOINT_BLOCKED_SOURCE_ARTIFACT"
)
DEFAULT_REASSESSMENT_CONCLUSION = (
    "OWNER_REASSESSMENT_REQUIRED_BEFORE_CONTINUING_DYNAMIC_STRATEGY_OBSERVATION_LINE"
)
FINAL_ROUTE = "OWNER_REASSESSMENT_REQUIRED_BEFORE_TRADING_2375"
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2364",
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2367",
    "TRADING-2368",
    "TRADING-2369",
    "TRADING-2370",
    "TRADING-2371",
    "TRADING-2372",
    "TRADING-2373",
)

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2373_REPORT_DRY_RUN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN_OUTPUT_ROOT
    / "observation_report_dry_run_result.json"
)


def run_dynamic_strategy_research_only_observation_owner_reassessment(
    *,
    source_report_dry_run_path: Path = DEFAULT_SOURCE_2373_REPORT_DRY_RUN_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    source = _load_json_document(source_report_dry_run_path)
    source_map = _as_mapping(source)
    validation_errors = _source_validation_errors(source_map)
    ready = not validation_errors
    resolved_as_of = _resolve_as_of(as_of_date, source_map)
    checkpoint = _owner_reassessment_checkpoint(
        source=source_map,
        as_of_date=resolved_as_of,
        ready=ready,
    )
    options = _recommended_owner_options()
    evidence = _no_side_effect_evidence()
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        source=source_map,
        source_path=source_report_dry_run_path,
        validation_errors=validation_errors,
        checkpoint=checkpoint,
        options=options,
        evidence=evidence,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _source_validation_errors(source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if source.get("status") != SOURCE_2373_READY_STATUS:
        errors.append("report_dry_run_status_not_ready")
    if source.get("next_route") != SOURCE_2373_EXPECTED_NEXT_ROUTE:
        errors.append("report_dry_run_next_route_not_trading_2374")
    if source.get("report_mode") != SOURCE_2373_REPORT_MODE:
        errors.append("report_mode_not_research_only_manual_dry_run")
    if source.get("observation_record_example_ready") is not True:
        errors.append("observation_record_example_not_ready")
    if source.get("observation_report_dry_run_ready") is not True:
        errors.append("observation_report_dry_run_not_ready")
    if source.get("no_side_effect_evidence_ready") is not True:
        errors.append("no_side_effect_evidence_not_ready")
    if not _primary_candidate(source):
        errors.append("primary_observation_candidate_missing")
    for field in _side_effect_false_fields():
        if bool(source.get(field)):
            errors.append(f"{field}_true")
    if bool(source.get("order_generated")):
        errors.append("order_generated_true")
    if source.get("broker_action") not in (None, "none"):
        errors.append("broker_action_not_none")
    if source.get("production_effect") not in (None, "none"):
        errors.append("production_effect_not_none")
    return errors


def _owner_reassessment_checkpoint(
    *,
    source: Mapping[str, Any],
    as_of_date: date,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_observation_owner_reassessment_checkpoint.v1",
        "checkpoint_id": f"{TASK_ID}_{as_of_date.isoformat()}",
        "generated_by_task": TASK_ID,
        "as_of": as_of_date.isoformat(),
        "source_task": "TRADING-2373",
        "source_status": source.get("status"),
        "source_route_confirmed": source.get("next_route")
        == SOURCE_2373_EXPECTED_NEXT_ROUTE,
        "owner_reassessment_checkpoint_ready": ready,
        "default_conclusion": DEFAULT_REASSESSMENT_CONCLUSION,
        "research_only_observation_line_closed_for_reassessment": ready,
        "continue_linear_observation_tasks": False,
        "next_task_auto_generated": False,
        "trading_2375_auto_created": False,
        "primary_observation_candidate": _primary_candidate(source),
        "required_owner_questions": _required_owner_questions(),
        "recommended_owner_options": [
            option["name"] for option in _recommended_owner_options()
        ],
        "final_route": FINAL_ROUTE,
    }


def _recommended_owner_options() -> list[dict[str, str]]:
    return [
        {
            "option_id": "Option_A",
            "name": "Continue research-only observation",
            "meaning": (
                "Continue manually generated research-only observation reports "
                "without enabling paper-shadow."
            ),
        },
        {
            "option_id": "Option_B",
            "name": "Return to candidate optimization",
            "meaning": (
                "Tune parameters, risk controls, and cooldown rules for "
                "dynamic_regime_overlay_v0_4_lower_turnover."
            ),
        },
        {
            "option_id": "Option_C",
            "name": "Compare robustness top vs ranking top deeper",
            "meaning": (
                "Deepen the risk/return comparison between lower_turnover and "
                "equal_risk_growth_tilt_vol_target_v1."
            ),
        },
        {
            "option_id": "Option_D",
            "name": "Improve data and PIT coverage first",
            "meaning": (
                "Pause strategy-candidate advancement and improve data quality "
                "and point-in-time coverage first."
            ),
        },
        {
            "option_id": "Option_E",
            "name": "Stop observation line",
            "meaning": "Pause research-only observation and do not continue to 2375.",
        },
    ]


def _required_owner_questions() -> list[str]:
    return [
        "是否继续观察 dynamic_regime_overlay_v0_4_lower_turnover？",
        "是否回到候选参数优化？",
        "是否重新比较 equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1？",
        "是否需要引入更多数据源 / 更长 PIT 数据？",
        "是否允许进入真正 paper-shadow？默认不允许。",
        "是否允许 event append / outcome binding？默认不允许。",
        "是否允许 scheduler / daily report？默认不允许。",
        "是否应该暂停 research-only observation，回到策略信号质量研究？",
    ]


def _no_side_effect_evidence() -> dict[str, Any]:
    return {
        "schema_version": (
            "dynamic_strategy_research_only_observation_owner_reassessment_"
            "no_side_effect.v1"
        ),
        "status": "PASS",
        "owner_reassessment_checkpoint_only": True,
        "next_task_auto_generated": False,
        "trading_2375_auto_created": False,
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
    checkpoint: Mapping[str, Any],
    options: list[dict[str, str]],
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
        "source_ready_for_owner_reassessment": ready,
        "source_validation_errors": validation_errors,
        "primary_observation_candidate": _primary_candidate(source),
        "ranking_top_from_2365": source.get("ranking_top_from_2365"),
        "robustness_top_from_2366": source.get("robustness_top_from_2366"),
        "owner_reassessment_checkpoint_ready": ready,
        "owner_reassessment_conclusion": DEFAULT_REASSESSMENT_CONCLUSION,
        "research_only_observation_line_closed_for_reassessment": ready,
        "continue_linear_observation_tasks": False,
        "next_task_auto_generated": False,
        "trading_2375_auto_created": False,
        "recommended_owner_options": [option["name"] for option in options],
        "owner_reassessment_options": options,
        "required_owner_questions": _required_owner_questions(),
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _as_mapping(source.get("requested_date_range")),
        "data_quality": _as_mapping(source.get("data_quality")),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REASSESSMENT_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "owner_reassessment_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
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
        "owner_reassessment_checkpoint": dict(checkpoint),
        "no_side_effect_evidence": dict(evidence),
        "recommended_next_research_task": FINAL_ROUTE,
        "next_route": FINAL_ROUTE,
        "final_route": FINAL_ROUTE,
        "summary_findings": {
            "owner_reassessment_checkpoint_ready": ready,
            "line_closed_for_reassessment": ready,
            "continue_linear_observation_tasks": False,
            "next_task_auto_generated": False,
            "paper_shadow_remains_disallowed": True,
            "event_and_outcome_paths_remain_disabled": True,
            "scheduler_and_daily_report_remain_disabled": True,
            "broker_path_remains_disabled": True,
            "final_route": FINAL_ROUTE,
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
        "json_path": str(output_root / "owner_reassessment_result.json"),
        "owner_reassessment_checkpoint_json": str(
            output_root / "owner_reassessment_checkpoint.json"
        ),
        "no_side_effect_evidence_json": str(
            output_root / "no_side_effect_evidence.json"
        ),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_research_only_observation_owner_reassessment.md"
        ),
        "checkpoint_markdown": str(
            docs_root
            / "dynamic_strategy_research_only_observation_owner_reassessment_checkpoint.md"
        ),
        "options_markdown": str(
            docs_root
            / "dynamic_strategy_research_only_observation_reassessment_options.md"
        ),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["owner_reassessment_checkpoint_json"]),
        {
            "report_type": (
                "dynamic_strategy_research_only_observation_"
                "owner_reassessment_checkpoint"
            ),
            "schema_version": (
                "dynamic_strategy_observation_owner_reassessment_checkpoint.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "owner_reassessment_checkpoint": payload[
                "owner_reassessment_checkpoint"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(artifact_paths["no_side_effect_evidence_json"]),
        {
            "report_type": (
                "dynamic_strategy_research_only_observation_owner_reassessment_"
                "no_side_effect_evidence"
            ),
            "schema_version": (
                "dynamic_strategy_research_only_observation_owner_reassessment_"
                "no_side_effect.v1"
            ),
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "no_side_effect_evidence": payload["no_side_effect_evidence"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["checkpoint_markdown"]).write_text(
        _checkpoint_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["options_markdown"]).write_text(
        _options_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 research-only observation owner reassessment",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- conclusion："
                f"`{payload.get('owner_reassessment_conclusion')}`"
            ),
            (
                "- line closed for reassessment："
                f"`{payload.get('research_only_observation_line_closed_for_reassessment')}`"
            ),
            (
                "- continue linear observation tasks："
                f"`{payload.get('continue_linear_observation_tasks')}`"
            ),
            f"- final route：`{payload.get('final_route')}`",
            "",
            "## Owner options",
            "",
            "\n".join(
                f"- {option.get('name')}"
                for option in _as_list(payload.get("owner_reassessment_options"))
            ),
            "",
            "## Safety boundary",
            "",
            "- 不自动生成 TRADING-2375。",
            "- 不允许 paper-shadow、paper trade 或 shadow position。",
            "- 不允许 event append 或 outcome binding。",
            "- 不允许 scheduler、scheduled task 或 daily report。",
            "- 不允许 production、broker 或 order。",
        ]
    )


def _checkpoint_markdown(payload: Mapping[str, Any]) -> str:
    checkpoint = _as_mapping(payload.get("owner_reassessment_checkpoint"))
    return "\n".join(
        [
            "# 动态策略 research-only observation owner reassessment checkpoint",
            "",
            f"- checkpoint id：`{checkpoint.get('checkpoint_id')}`",
            f"- default conclusion：`{checkpoint.get('default_conclusion')}`",
            (
                "- continue linear observation tasks："
                f"`{checkpoint.get('continue_linear_observation_tasks')}`"
            ),
            f"- TRADING-2375 auto created：`{checkpoint.get('trading_2375_auto_created')}`",
            f"- final route：`{checkpoint.get('final_route')}`",
            "",
            "## Required owner questions",
            "",
            "\n".join(
                f"- {question}" for question in _as_list(checkpoint.get("required_owner_questions"))
            ),
        ]
    )


def _options_markdown(payload: Mapping[str, Any]) -> str:
    lines = ["# 动态策略 research-only observation reassessment options", ""]
    for option in _as_list(payload.get("owner_reassessment_options")):
        item = _as_mapping(option)
        lines.append(f"## {item.get('option_id')} - {item.get('name')}")
        lines.append("")
        lines.append(str(item.get("meaning")))
        lines.append("")
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


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "owner_reassessment_checkpoint": ready,
        "recommended_owner_options": ready,
        "required_owner_questions": ready,
        "continue_linear_observation_tasks_false": ready,
        "next_task_auto_generated_false": ready,
        "paper_shadow_enabled_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "outcome_binding_enabled_false": ready,
        "scheduler_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
        "final_route": ready,
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
        "load_error": document.get("_load_error"),
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


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
