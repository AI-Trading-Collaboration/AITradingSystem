from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    OWNER_DECISION as SOURCE_2371_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    READY_STATUS as SOURCE_2371_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2372"
TASK_REGISTER_ID = (
    "TRADING-2372_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "LOG_SCHEMA_AND_REPORT_PLAN"
)
REPORT_TYPE = "dynamic_strategy_research_only_observation_log_schema_plan"
SCHEMA_VERSION = "dynamic_strategy_research_only_observation_log_schema_plan.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "LOG_SCHEMA_AND_REPORT_PLAN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "LOG_SCHEMA_AND_REPORT_PLAN_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2373_Dynamic_Strategy_Research_Only_Observation_Report_Dry_Run"
)
SOURCE_2371_ROUTE = (
    "TRADING-2372_Dynamic_Strategy_Research_Only_Observation_"
    "Log_Schema_And_Report_Plan"
)
PRIMARY_CANDIDATE_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2367",
    "TRADING-2368",
    "TRADING-2369",
    "TRADING-2370",
    "TRADING-2371",
)

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision_result.json"
)


def run_dynamic_strategy_research_only_observation_log_schema_plan(
    *,
    source_owner_review_decision_path: Path = DEFAULT_SOURCE_OWNER_REVIEW_DECISION_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_LOG_SCHEMA_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    source = _load_json_document(source_owner_review_decision_path)
    source_map = _as_mapping(source)
    validation_errors = _source_validation_errors(source_map)
    ready = not validation_errors
    resolved_as_of = _resolve_as_of(as_of_date, source_map)
    log_schema = _observation_log_schema(source_map)
    report_plan = _observation_report_plan(source_map)
    evidence = _no_side_effect_evidence()
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        source=source_map,
        source_path=source_owner_review_decision_path,
        validation_errors=validation_errors,
        log_schema=log_schema,
        report_plan=report_plan,
        evidence=evidence,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _source_validation_errors(source: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if source.get("status") != SOURCE_2371_READY_STATUS:
        errors.append("owner_review_decision_status_not_ready")
    if source.get("next_route") != SOURCE_2371_ROUTE:
        errors.append("owner_review_decision_next_route_not_trading_2372")
    if source.get("owner_decision") != SOURCE_2371_OWNER_DECISION:
        errors.append("owner_decision_not_research_only_continue")
    if source.get("owner_review_decision_recorded") is not True:
        errors.append("owner_review_decision_not_recorded")
    if source.get("research_only_observation_continue_allowed") is not True:
        errors.append("research_only_observation_continue_not_allowed")
    for field in _side_effect_false_fields():
        if bool(source.get(field)):
            errors.append(f"{field}_true")
    for field in (
        "paper_shadow_approved",
        "event_append_approved",
        "outcome_binding_approved",
    ):
        if bool(source.get(field)):
            errors.append(f"{field}_true")
    if source.get("broker_action") not in (None, "none"):
        errors.append("broker_action_not_none")
    if source.get("production_effect") not in (None, "none"):
        errors.append("production_effect_not_none")
    return errors


def _observation_log_schema(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_research_only_observation_log.v1",
        "schema_mode": "RESEARCH_ONLY_SCHEMA_PLAN",
        "primary_observation_candidate": _primary_candidate(source),
        "field_sections": [
            {
                "section": "identity",
                "required_fields": [
                    "observation_id",
                    "as_of",
                    "generated_by_task",
                    "source_artifact",
                    "candidate_id",
                    "candidate_version",
                    "execution_cadence",
                ],
            },
            {
                "section": "candidate_context",
                "required_fields": [
                    "primary_observation_candidate",
                    "ranking_top_from_2365",
                    "robustness_top_from_2366",
                    "gate_decision_from_2367",
                    "owner_decision_from_2371",
                ],
            },
            {
                "section": "signal_context",
                "required_fields": [
                    "signal_state",
                    "advisory_valid_from",
                    "advisory_valid_until",
                    "signal_horizon",
                    "valid_until_window_state",
                ],
            },
            {
                "section": "portfolio_preview",
                "required_fields": [
                    "reference_weight",
                    "proposed_research_weight",
                    "proposed_weight_delta",
                    "risk_cap_state",
                    "constraint_state",
                    "cooldown_state",
                    "no_trade_reason",
                ],
            },
            {
                "section": "cost_turnover",
                "required_fields": [
                    "expected_turnover",
                    "transaction_cost_bps",
                    "slippage_bps",
                    "estimated_cost_drag",
                    "turnover_cap_state",
                ],
            },
            {
                "section": "comparison",
                "required_fields": [
                    "static_baseline_comparison",
                    "ranking_top_candidate_comparison",
                    "robustness_top_candidate_comparison",
                    "dynamic_vs_static_preview_gap",
                ],
            },
            {
                "section": "review",
                "required_fields": [
                    "observation_decision",
                    "owner_review_required",
                    "review_reason",
                    "escalation_flag",
                ],
            },
            {
                "section": "guardrails",
                "required_fields": [
                    "research_only_observation",
                    "paper_shadow_enabled",
                    "paper_trade_created",
                    "shadow_position_created",
                    "event_append_enabled",
                    "outcome_binding_enabled",
                    "production_enabled",
                    "broker_action_enabled",
                    "daily_report_generated",
                ],
            },
        ],
        "write_policy": {
            "event_append_allowed": False,
            "outcome_binding_allowed": False,
            "paper_trade_allowed": False,
            "shadow_position_allowed": False,
            "daily_report_allowed": False,
            "broker_action_allowed": False,
        },
    }


def _observation_report_plan(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_research_only_observation_report_plan.v1",
        "report_mode": "RESEARCH_ONLY_MANUAL_REPORT_PLAN",
        "primary_observation_candidate": _primary_candidate(source),
        "sections": [
            "Executive summary",
            "Candidate under observation",
            "Signal / valid-until status",
            "Portfolio preview",
            "Static baseline comparison",
            "Ranking top vs robustness top comparison",
            "Cost / turnover / cooldown status",
            "Review flags",
            "Guardrail summary",
            "Explicit non-goals",
        ],
        "required_inputs": [
            "TRADING-2371 owner review decision artifact",
            "TRADING-2369 observation dry-run artifact",
            "TRADING-2370 replay validation artifact",
        ],
        "report_generation_policy": {
            "manual_report_dry_run_only": True,
            "daily_report_generated": False,
            "scheduler_required": False,
            "event_log_write_allowed": False,
            "outcome_binding_allowed": False,
        },
    }


def _no_side_effect_evidence() -> dict[str, Any]:
    return {
        "schema_version": (
            "dynamic_strategy_research_only_observation_schema_plan_no_side_effect.v1"
        ),
        "status": "PASS",
        "schema_only": True,
        "report_plan_only": True,
        "observation_row_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
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
    source: Mapping[str, Any],
    source_path: Path,
    validation_errors: list[str],
    log_schema: Mapping[str, Any],
    report_plan: Mapping[str, Any],
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
        "source_ready_for_schema_plan": ready,
        "source_validation_errors": validation_errors,
        "primary_observation_candidate": _primary_candidate(source),
        "ranking_top_from_2365": source.get("ranking_top_from_2365")
        or RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": source.get("robustness_top_from_2366")
        or PRIMARY_CANDIDATE_FALLBACK,
        "owner_decision_from_2371": source.get("owner_decision"),
        "research_only_observation_continue_allowed": source.get(
            "research_only_observation_continue_allowed"
        ),
        "observation_log_schema_ready": ready,
        "observation_report_plan_ready": ready,
        "schema_only": True,
        "report_plan_only": True,
        "periodic_daily_report_generated": False,
        "event_log_written": False,
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _as_mapping(source.get("requested_date_range")),
        "data_quality": _as_mapping(source.get("data_quality")),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_SCHEMA_PLAN_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
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
        "observation_log_schema": dict(log_schema),
        "observation_report_plan": dict(report_plan),
        "no_side_effect_evidence": dict(evidence),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "summary_findings": {
            "schema_ready": ready,
            "report_plan_ready": ready,
            "daily_report_generated": False,
            "event_log_written": False,
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
        "json_path": str(output_root / "log_schema_plan_result.json"),
        "observation_log_schema_json": str(output_root / "observation_log_schema.json"),
        "observation_report_plan_json": str(output_root / "observation_report_plan.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_research_only_observation_log_schema_plan.md"
        ),
        "observation_log_schema_markdown": str(
            docs_root / "dynamic_strategy_research_only_observation_log_schema.md"
        ),
        "observation_report_plan_markdown": str(
            docs_root / "dynamic_strategy_research_only_observation_report_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2373_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["observation_log_schema_json"]),
        {
            "report_type": "dynamic_strategy_research_only_observation_log_schema",
            "schema_version": "dynamic_strategy_research_only_observation_log.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_log_schema": payload["observation_log_schema"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(artifact_paths["observation_report_plan_json"]),
        {
            "report_type": "dynamic_strategy_research_only_observation_report_plan",
            "schema_version": "dynamic_strategy_research_only_observation_report_plan.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "observation_report_plan": payload["observation_report_plan"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["observation_log_schema_markdown"]).write_text(
        _schema_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["observation_report_plan_markdown"]).write_text(
        _report_plan_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload), encoding="utf-8"
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 research-only observation log schema and report plan",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- observation log schema ready："
                f"`{payload.get('observation_log_schema_ready')}`"
            ),
            (
                "- observation report plan ready："
                f"`{payload.get('observation_report_plan_ready')}`"
            ),
            f"- primary observation candidate：`{payload.get('primary_observation_candidate')}`",
            f"- next route：`{payload.get('next_route')}`",
            "",
            (
                "该产物只定义 schema 与 manual report plan；不生成 daily report、"
                "不写 event log、不 bind outcome、不启用 scheduler。"
            ),
        ]
    )


def _schema_markdown(payload: Mapping[str, Any]) -> str:
    schema = _as_mapping(payload.get("observation_log_schema"))
    lines = [
        "# 动态策略 research-only observation log schema",
        "",
        f"- schema version：`{schema.get('schema_version')}`",
        f"- schema mode：`{schema.get('schema_mode')}`",
        "",
    ]
    for section in _as_list(schema.get("field_sections")):
        item = _as_mapping(section)
        lines.append(f"## {item.get('section')}")
        lines.append("")
        lines.append(", ".join(f"`{field}`" for field in _as_list(item.get("required_fields"))))
        lines.append("")
    return "\n".join(lines)


def _report_plan_markdown(payload: Mapping[str, Any]) -> str:
    plan = _as_mapping(payload.get("observation_report_plan"))
    return "\n".join(
        [
            "# 动态策略 research-only observation report plan",
            "",
            f"- report mode：`{plan.get('report_mode')}`",
            "",
            "## Sections",
            "",
            "\n".join(f"- {section}" for section in _as_list(plan.get("sections"))),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2373 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            (
                "- route boundary：manual research-only observation report dry-run；"
                "不是 daily report、scheduler、event append、outcome binding、"
                "paper-shadow、production 或 broker。"
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
        "observation_log_schema": ready,
        "observation_report_plan": ready,
        "schema_only": ready,
        "report_plan_only": ready,
        "periodic_daily_report_generated_false": ready,
        "event_log_written_false": ready,
        "event_append_enabled_false": ready,
        "outcome_binding_enabled_false": ready,
        "scheduler_enabled_false": ready,
        "paper_shadow_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
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


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
