from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_pit_coverage_matrix_implementation_plan as m2404
import ai_trading_system.dynamic_strategy_pit_coverage_signal_construction_review as m2403
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_report_common import json_block as _json_block
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_document_or_missing_flag as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY
from ai_trading_system.research_quality.pit_coverage_gate import (
    build_pit_blocker_summary,
    evaluate_pit_gate,
)
from ai_trading_system.research_quality.pit_coverage_matrix import (
    build_pit_coverage_matrix,
    build_pit_remediation_matrix,
)
from ai_trading_system.research_quality.pit_input_registry import (
    DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    load_pit_input_registry,
)

TASK_ID = "TRADING-2405"
TASK_REGISTER_ID = (
    "TRADING-2405_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION"
)
REPORT_TYPE = "dynamic_strategy_pit_coverage_matrix_reusable_implementation"
SCHEMA_VERSION = "dynamic_strategy_pit_coverage_matrix_reusable_implementation.v1"
READY_STATUS = "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_BLOCKED_SOURCE"
)
NEXT_ROUTE = (
    "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_REGISTRY_ONLY_NO_FRESH_MARKET_DATA"
)

SOURCE_TASKS: tuple[str, ...] = ("TRADING-2402", "TRADING-2403", "TRADING-2404")
BLOCKING_GAP_INPUTS: tuple[str, ...] = ("growth_tilt_engine", "valid_until_window")
EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "candidate_search_resume",
    "candidate_auto_accept",
    "research_only_observation",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "scheduled_task",
    "daily_report",
    "production",
    "broker_order",
    "new_strategy_backtest",
    "new_trading_signal",
    "new_scoring",
    "clear_blocking_gap_without_evidence",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_search_allowed",
    "research_only_observation_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "candidate_search_resumed",
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "event_append_approved",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "outcome_store_mutated",
    "production_enabled",
    "production_approved",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_signal_generated",
    "scoring_run",
)

DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_RESEARCH_QUALITY_PIT_COVERAGE_MATRIX_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_quality" / "pit_coverage_matrix"
)
DEFAULT_SOURCE_2404_IMPLEMENTATION_PATH = (
    m2404.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "implementation_plan_result.json"
)
DEFAULT_SOURCE_2404_REGISTRY_SCHEMA_PATH = (
    m2404.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "pit_input_registry_schema.json"
)
DEFAULT_SOURCE_2404_GATE_POLICY_PATH = (
    m2404.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "pit_gate_policy.json"
)
DEFAULT_SOURCE_2404_BLOCKER_SUMMARY_PATH = (
    m2404.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "current_blocker_summary.json"
)
DEFAULT_SOURCE_2403_PIT_MATRIX_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "pit_coverage_matrix.json"
)
DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH = (
    m2403.DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_SIGNAL_CONSTRUCTION_REVIEW_OUTPUT_ROOT
    / "remediation_matrix.json"
)


def run_dynamic_strategy_pit_coverage_matrix_reusable_implementation(
    *,
    registry_path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    source_2404_implementation_path: Path = DEFAULT_SOURCE_2404_IMPLEMENTATION_PATH,
    source_2404_registry_schema_path: Path = DEFAULT_SOURCE_2404_REGISTRY_SCHEMA_PATH,
    source_2404_gate_policy_path: Path = DEFAULT_SOURCE_2404_GATE_POLICY_PATH,
    source_2404_blocker_summary_path: Path = DEFAULT_SOURCE_2404_BLOCKER_SUMMARY_PATH,
    source_2403_pit_matrix_path: Path = DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_remediation_matrix_path: Path = DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_OUTPUT_ROOT
    ),
    research_quality_output_root: Path = (
        DEFAULT_RESEARCH_QUALITY_PIT_COVERAGE_MATRIX_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2404_implementation_path=source_2404_implementation_path,
        source_2404_registry_schema_path=source_2404_registry_schema_path,
        source_2404_gate_policy_path=source_2404_gate_policy_path,
        source_2404_blocker_summary_path=source_2404_blocker_summary_path,
        source_2403_pit_matrix_path=source_2403_pit_matrix_path,
        source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
    )
    registry = load_pit_input_registry(registry_path)
    matrix = build_pit_coverage_matrix(registry)
    matrix_rows = _as_list(matrix.get("pit_coverage_matrix"))
    gate_result = evaluate_pit_gate([_as_mapping(row) for row in matrix_rows])
    blocker_summary = build_pit_blocker_summary(
        [_as_mapping(row) for row in matrix_rows],
        gate_result,
    )
    remediation_matrix = build_pit_remediation_matrix(
        [_as_mapping(row) for row in matrix_rows]
    )
    remediation_routes = _remediation_routes(remediation_matrix)
    validation_errors = _validate_sources(sources) + list(
        _as_list(registry.get("validation_errors"))
    )
    ready = not validation_errors
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        sources=sources,
        registry=registry,
        as_of_date=as_of_date,
        source_validation_errors=validation_errors,
    )
    if ready:
        payload.update(
            _ready_sections(
                registry=registry,
                matrix=matrix,
                gate_result=gate_result,
                blocker_summary=blocker_summary,
                remediation_matrix=remediation_matrix,
                remediation_routes=remediation_routes,
            )
        )
    else:
        payload.update(_blocked_sections())
    _write_outputs(
        payload,
        output_root=output_root,
        research_quality_output_root=research_quality_output_root,
        docs_root=docs_root,
    )
    return payload


def _load_sources(
    *,
    source_2404_implementation_path: Path,
    source_2404_registry_schema_path: Path,
    source_2404_gate_policy_path: Path,
    source_2404_blocker_summary_path: Path,
    source_2403_pit_matrix_path: Path,
    source_2403_remediation_matrix_path: Path,
) -> dict[str, Any]:
    sources = {
        "implementation_2404": _load_json_document(source_2404_implementation_path),
        "registry_schema_2404": _load_json_document(source_2404_registry_schema_path),
        "gate_policy_2404": _load_json_document(source_2404_gate_policy_path),
        "blocker_summary_2404": _load_json_document(source_2404_blocker_summary_path),
        "pit_matrix_2403": _load_json_document(source_2403_pit_matrix_path),
        "remediation_matrix_2403": _load_json_document(
            source_2403_remediation_matrix_path
        ),
    }
    sources["source_paths"] = {
        "implementation_2404": str(source_2404_implementation_path),
        "registry_schema_2404": str(source_2404_registry_schema_path),
        "gate_policy_2404": str(source_2404_gate_policy_path),
        "blocker_summary_2404": str(source_2404_blocker_summary_path),
        "pit_matrix_2403": str(source_2403_pit_matrix_path),
        "remediation_matrix_2403": str(source_2403_remediation_matrix_path),
    }
    return sources


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_statuses = {
        "implementation_2404": m2404.READY_STATUS,
        "registry_schema_2404": m2404.READY_STATUS,
        "gate_policy_2404": m2404.READY_STATUS,
        "blocker_summary_2404": m2404.READY_STATUS,
        "pit_matrix_2403": m2403.READY_STATUS,
        "remediation_matrix_2403": m2403.READY_STATUS,
    }
    for source_name, expected in expected_statuses.items():
        source = _as_mapping(sources.get(source_name))
        if source.get("_missing"):
            errors.append(f"{source_name}: missing source artifact {source.get('_path')}")
        elif source.get("status") != expected:
            errors.append(
                f"{source_name}: expected status {expected}, observed {source.get('status')}"
            )
    implementation_2404 = _as_mapping(sources.get("implementation_2404"))
    if implementation_2404.get("recommended_next_research_task") != m2404.NEXT_ROUTE:
        errors.append("2404 next route mismatch")
    if implementation_2404.get("candidate_search_resumed") is not False:
        errors.append("2404 candidate_search_resumed must be false")
    for field in SAFETY_FALSE_FIELDS:
        if implementation_2404.get(field) is True:
            errors.append(f"2404 safety field must be false: {field}")
    if implementation_2404.get("broker_action") not in (None, "none"):
        errors.append("2404 broker_action must be none")
    source_blockers = set(_as_list(implementation_2404.get("blocking_gaps")))
    for input_id in BLOCKING_GAP_INPUTS:
        if input_id not in source_blockers:
            errors.append(f"2404 blocking gap missing: {input_id}")
    pit_rows = _as_list(_as_mapping(sources.get("pit_matrix_2403")).get("pit_coverage_matrix"))
    pit_blockers = {
        row.get("input_id")
        for row in (_as_mapping(item) for item in pit_rows)
        if row.get("severity") == "BLOCKING"
    }
    for input_id in BLOCKING_GAP_INPUTS:
        if input_id not in pit_blockers:
            errors.append(f"2403 PIT matrix blocking gap missing: {input_id}")
    return errors


def _base_payload(
    *,
    status: str,
    sources: Mapping[str, Any],
    registry: Mapping[str, Any],
    as_of_date: date | None,
    source_validation_errors: list[str],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else None,
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "source_tasks": list(SOURCE_TASKS),
        "source_paths": dict(_as_mapping(sources.get("source_paths"))),
        "source_validation_errors": source_validation_errors,
        "pit_input_registry_path": registry.get("path"),
        "pit_input_registry_validation_status": registry.get("validation_status"),
        "pit_input_registry_validation_errors": list(
            _as_list(registry.get("validation_errors"))
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "candidate_search_resumed": False,
        "manual_review_required": True,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "paper_shadow_approved": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_approved": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_approved": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "production_approved": False,
        "production_enabled": False,
        "broker_action": "none",
        "broker_action_enabled": False,
        "order_generated": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections(
    *,
    registry: Mapping[str, Any],
    matrix: Mapping[str, Any],
    gate_result: Mapping[str, Any],
    blocker_summary: Mapping[str, Any],
    remediation_matrix: Mapping[str, Any],
    remediation_routes: Mapping[str, Any],
) -> dict[str, Any]:
    matrix_rows = _as_list(matrix.get("pit_coverage_matrix"))
    blocking_gaps = list(_as_list(blocker_summary.get("blocking_gaps")))
    return {
        "pit_input_registry_created": True,
        "pit_input_registry_ready": True,
        "pit_coverage_matrix_generator_ready": True,
        "pit_gate_checker_ready": True,
        "pit_blocker_summary_ready": True,
        "pit_remediation_matrix_ready": True,
        "pit_coverage_matrix_rows": len(matrix_rows),
        "blocking_gaps": blocking_gaps,
        "candidate_search_allowed": gate_result.get("candidate_search_allowed") is True,
        "research_only_observation_allowed": (
            gate_result.get("research_only_observation_allowed") is True
        ),
        "paper_shadow_allowed": gate_result.get("paper_shadow_allowed") is True,
        "production_allowed": gate_result.get("production_allowed") is True,
        "recommended_next_research_task": NEXT_ROUTE,
        "pit_input_registry_snapshot": dict(registry),
        "pit_coverage_matrix": dict(matrix),
        "pit_gate_result": dict(gate_result),
        "pit_blocker_summary": dict(blocker_summary),
        "pit_remediation_matrix": dict(remediation_matrix),
        "pit_remediation_routes": remediation_routes,
        "gate_policy_note": gate_result.get("policy_note"),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "pit_input_registry_created": False,
        "pit_input_registry_ready": False,
        "pit_coverage_matrix_generator_ready": False,
        "pit_gate_checker_ready": False,
        "pit_blocker_summary_ready": False,
        "pit_remediation_matrix_ready": False,
        "pit_coverage_matrix_rows": 0,
        "blocking_gaps": [],
        "recommended_next_research_task": None,
        "pit_input_registry_snapshot": {},
        "pit_coverage_matrix": {},
        "pit_gate_result": {},
        "pit_blocker_summary": {},
        "pit_remediation_matrix": {},
        "pit_remediation_routes": {},
        "gate_policy_note": "",
    }


def _remediation_routes(remediation_matrix: Mapping[str, Any]) -> dict[str, Any]:
    rows = _as_list(remediation_matrix.get("pit_remediation_matrix"))
    route_by_input = {
        str(row.get("input_id")): {
            "severity": row.get("severity"),
            "next_task": row.get("next_task"),
            "candidate_search_blocker": row.get("candidate_search_blocker") is True,
            "recommended_action": row.get("recommended_action"),
        }
        for row in (_as_mapping(item) for item in rows)
    }
    return {
        "schema_version": "dynamic_strategy_pit_remediation_routes.v1",
        "recommended_next_research_task": NEXT_ROUTE,
        "route_reason": "growth_tilt_engine is the core return-engine blocking PIT gap",
        "routes": {
            "growth_tilt_engine": route_by_input.get(
                "growth_tilt_engine",
                {
                    "severity": "BLOCKING",
                    "next_task": NEXT_ROUTE,
                    "candidate_search_blocker": True,
                },
            ),
            "valid_until_window": route_by_input.get(
                "valid_until_window",
                {
                    "severity": "BLOCKING",
                    "next_task": (
                        "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_"
                        "Signal_Remediation_Plan"
                    ),
                    "candidate_search_blocker": True,
                },
            ),
            "regime_expectation_scoring": {
                "severity": "MATERIAL",
                "next_task": "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan",
            },
            "threshold_meta_dataset": {
                "severity": "MATERIAL",
                "next_task": "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan",
            },
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_outputs(
    payload: dict[str, Any],
    *,
    output_root: Path,
    research_quality_output_root: Path,
    docs_root: Path,
) -> None:
    paths = {
        "json_path": str(output_root / "implementation_result.json"),
        "pit_input_registry_snapshot_json": str(
            output_root / "pit_input_registry_snapshot.json"
        ),
        "pit_coverage_matrix_json": str(output_root / "pit_coverage_matrix.json"),
        "pit_gate_result_json": str(output_root / "pit_gate_result.json"),
        "pit_blocker_summary_json": str(output_root / "pit_blocker_summary.json"),
        "pit_remediation_routes_json": str(output_root / "pit_remediation_routes.json"),
        "research_quality_pit_coverage_matrix_json": str(
            research_quality_output_root / "dynamic_strategy_pit_coverage_matrix.json"
        ),
        "research_quality_pit_gate_result_json": str(
            research_quality_output_root / "dynamic_strategy_pit_gate_result.json"
        ),
        "research_quality_pit_blocker_summary_json": str(
            research_quality_output_root / "dynamic_strategy_pit_blocker_summary.json"
        ),
        "research_quality_pit_remediation_matrix_json": str(
            research_quality_output_root / "dynamic_strategy_pit_remediation_matrix.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_pit_coverage_matrix_reusable_implementation.md"
        ),
        "pit_input_registry_markdown": str(
            docs_root / "dynamic_strategy_pit_input_registry.md"
        ),
        "pit_gate_result_markdown": str(
            docs_root / "dynamic_strategy_pit_gate_result.md"
        ),
        "pit_remediation_routes_markdown": str(
            docs_root / "dynamic_strategy_pit_remediation_routes.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2406_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    for path_key, report_type, schema_key, payload_key in (
        (
            "pit_input_registry_snapshot_json",
            "dynamic_strategy_pit_input_registry_snapshot",
            "dynamic_strategy_pit_input_registry_snapshot.v1",
            "pit_input_registry_snapshot",
        ),
        (
            "pit_coverage_matrix_json",
            "dynamic_strategy_pit_coverage_matrix",
            "dynamic_strategy_pit_coverage_matrix.v1",
            "pit_coverage_matrix",
        ),
        (
            "pit_gate_result_json",
            "dynamic_strategy_pit_gate_result",
            "dynamic_strategy_pit_gate_result.v1",
            "pit_gate_result",
        ),
        (
            "pit_blocker_summary_json",
            "dynamic_strategy_pit_blocker_summary",
            "dynamic_strategy_pit_blocker_summary.v1",
            "pit_blocker_summary",
        ),
        (
            "pit_remediation_routes_json",
            "dynamic_strategy_pit_remediation_routes",
            "dynamic_strategy_pit_remediation_routes.v1",
            "pit_remediation_routes",
        ),
    ):
        write_json_artifact(
            Path(paths[path_key]),
            {
                "task_id": TASK_ID,
                "status": payload.get("status"),
                "report_type": report_type,
                "schema_version": schema_key,
                payload_key: payload.get(payload_key, {}),
                "production_effect": "none",
                "broker_action": "none",
            },
        )
    for path_key, report_type, payload_key in (
        (
            "research_quality_pit_coverage_matrix_json",
            "dynamic_strategy_pit_coverage_matrix",
            "pit_coverage_matrix",
        ),
        (
            "research_quality_pit_gate_result_json",
            "dynamic_strategy_pit_gate_result",
            "pit_gate_result",
        ),
        (
            "research_quality_pit_blocker_summary_json",
            "dynamic_strategy_pit_blocker_summary",
            "pit_blocker_summary",
        ),
        (
            "research_quality_pit_remediation_matrix_json",
            "dynamic_strategy_pit_remediation_matrix",
            "pit_remediation_matrix",
        ),
    ):
        write_json_artifact(
            Path(paths[path_key]),
            {
                "task_id": TASK_ID,
                "status": payload.get("status"),
                "report_type": report_type,
                payload_key: payload.get(payload_key, {}),
                "production_effect": "none",
                "broker_action": "none",
            },
        )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["pit_input_registry_markdown"]),
        _registry_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["pit_gate_result_markdown"]),
        _gate_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["pit_remediation_routes_markdown"]),
        _routes_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT coverage matrix reusable implementation",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- PIT input registry created：`{payload.get('pit_input_registry_created')}`",
            (
                "- PIT coverage matrix generator ready："
                f"`{payload.get('pit_coverage_matrix_generator_ready')}`"
            ),
            f"- PIT gate checker ready：`{payload.get('pit_gate_checker_ready')}`",
            f"- blocking gaps：`{payload.get('blocking_gaps')}`",
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- data quality gate：not run；reason="
                f"`{payload.get('data_quality_gate_reason')}`"
            ),
            "",
            "## Source findings from TRADING-2404",
            "",
            f"- source validation errors：`{payload.get('source_validation_errors')}`",
            f"- policy note：`{payload.get('gate_policy_note')}`",
            "",
            "## PIT input registry implementation",
            "",
            _json_block(payload.get("pit_input_registry_snapshot", {})),
            "",
            "## PIT coverage matrix generator",
            "",
            _json_block(payload.get("pit_coverage_matrix", {})),
            "",
            "## PIT severity gate checker",
            "",
            _json_block(payload.get("pit_gate_result", {})),
            "",
            "## Current gate result",
            "",
            _json_block(
                {
                    "candidate_search_allowed": payload.get(
                        "candidate_search_allowed"
                    ),
                    "research_only_observation_allowed": payload.get(
                        "research_only_observation_allowed"
                    ),
                    "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
                    "production_allowed": payload.get("production_allowed"),
                    "blocking_gaps": payload.get("blocking_gaps"),
                }
            ),
            "",
            "## Blocking gaps",
            "",
            _json_block(payload.get("pit_blocker_summary", {})),
            "",
            "## Remediation routes",
            "",
            _json_block(payload.get("pit_remediation_routes", {})),
            "",
            "## Explicit non-approval list",
            "",
            *[f"- `{item}`" for item in payload.get("explicit_non_approval_list", [])],
            "",
        ]
    )


def _registry_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT input registry",
            "",
            f"- status：`{payload.get('status')}`",
            f"- registry path：`{payload.get('pit_input_registry_path')}`",
            "",
            _json_block(payload.get("pit_input_registry_snapshot", {})),
            "",
        ]
    )


def _gate_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT gate result",
            "",
            f"- status：`{payload.get('status')}`",
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            "",
            _json_block(payload.get("pit_gate_result", {})),
            "",
        ]
    )


def _routes_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy PIT remediation routes",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            _json_block(payload.get("pit_remediation_routes", {})),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2406 route",
            "",
            f"- status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- candidate search allowed：`{payload.get('candidate_search_allowed')}`",
            (
                "- research-only observation allowed："
                f"`{payload.get('research_only_observation_allowed')}`"
            ),
            f"- paper-shadow allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production allowed：`{payload.get('production_allowed')}`",
            f"- broker action enabled：`{payload.get('broker_action_enabled')}`",
            "",
        ]
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
