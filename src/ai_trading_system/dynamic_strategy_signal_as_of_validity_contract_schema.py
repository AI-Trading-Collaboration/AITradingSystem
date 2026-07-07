from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import ai_trading_system.dynamic_strategy_blocking_gap_remediation_implementation_plan as m2408
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
from ai_trading_system.research_quality.signal_as_of_contract import (
    build_signal_as_of_contract_schema,
    valid_signal_as_of_contract_example,
    validate_signal_as_of_contract,
)
from ai_trading_system.research_quality.signal_contract_schema_snapshot import (
    build_pit_gate_integration_plan,
    build_signal_contract_schema_snapshot,
)
from ai_trading_system.research_quality.signal_validity_contract import (
    build_signal_validity_contract_schema,
    valid_signal_validity_contract_example,
    validate_signal_validity_contract,
)
from ai_trading_system.research_quality.source_feature_traceability_contract import (
    build_source_feature_traceability_contract_schema,
    valid_source_feature_traceability_contract_example,
    validate_source_feature_traceability_contract,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2409"
TASK_REGISTER_ID = (
    "TRADING-2409_DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA"
)
REPORT_TYPE = "dynamic_strategy_signal_as_of_validity_contract_schema"
SCHEMA_VERSION = "dynamic_strategy_signal_as_of_validity_contract_schema.v1"
READY_STATUS = "DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_READY"
BLOCKED_SOURCE_STATUS = "DYNAMIC_STRATEGY_SIGNAL_AS_OF_AND_VALIDITY_CONTRACT_SCHEMA_BLOCKED_SOURCE"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2405",
    "TRADING-2406",
    "TRADING-2407",
    "TRADING-2408",
)
BLOCKING_GAPS: tuple[str, ...] = ("growth_tilt_engine", "valid_until_window")
NEXT_ROUTE = "TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_SCHEMA_VALIDATOR_AND_PRIOR_VALIDATED_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "clear_growth_tilt_engine_blocking_gap",
    "clear_valid_until_window_blocking_gap",
    "downgrade_any_blocking_gap",
    "mark_any_blocker_true_pit",
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
    "growth_tilt_engine_blocking_gap_resolved",
    "valid_until_window_blocking_gap_resolved",
    "any_blocker_severity_downgraded",
    "candidate_search_allowed",
    "candidate_search_resumed",
    "research_only_observation_allowed",
    "research_only_observation_approved",
    "observation_approved",
    "paper_shadow_allowed",
    "production_allowed",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "event_append_enabled",
    "event_append_approved",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "outcome_store_mutated",
    "scheduler_enabled",
    "scheduled_task_created",
    "production_enabled",
    "production_approved",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "new_signal_generated",
    "new_strategy_backtest_run",
    "scoring_run",
)

DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SIGNAL_CONTRACT_RESEARCH_QUALITY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_quality" / "signal_contracts"
)
DEFAULT_SOURCE_2408_IMPLEMENTATION_PLAN_PATH = (
    m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "implementation_plan_result.json"
)
DEFAULT_SOURCE_2408_CONTRACT_SCHEMA_PLAN_PATH = (
    m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "contract_schema_plan.json"
)
DEFAULT_SOURCE_2408_CANDIDATE_SEARCH_GATE_POLICY_PATH = (
    m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    / "candidate_search_gate_policy.json"
)
DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH = m2408.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH
DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH = m2408.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH
DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH = m2408.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH
DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH = (
    m2408.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH
)


def run_dynamic_strategy_signal_as_of_validity_contract_schema(
    *,
    source_2408_implementation_plan_path: Path = (
        DEFAULT_SOURCE_2408_IMPLEMENTATION_PLAN_PATH
    ),
    source_2408_contract_schema_plan_path: Path = (
        DEFAULT_SOURCE_2408_CONTRACT_SCHEMA_PLAN_PATH
    ),
    source_2408_candidate_search_gate_policy_path: Path = (
        DEFAULT_SOURCE_2408_CANDIDATE_SEARCH_GATE_POLICY_PATH
    ),
    source_2405_registry_snapshot_path: Path = DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_gate_result_path: Path = DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Path = DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    output_root: Path = DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_DOCS_ROOT,
    research_quality_output_root: Path = DEFAULT_SIGNAL_CONTRACT_RESEARCH_QUALITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_2408_implementation_plan_path=source_2408_implementation_plan_path,
        source_2408_contract_schema_plan_path=source_2408_contract_schema_plan_path,
        source_2408_candidate_search_gate_policy_path=(
            source_2408_candidate_search_gate_policy_path
        ),
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        pit_input_registry_path=pit_input_registry_path,
    )
    validation_errors = _validate_sources(sources)
    ready = not validation_errors
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        sources=sources,
        as_of_date=as_of_date,
        source_validation_errors=validation_errors,
    )
    if ready:
        payload.update(_ready_sections())
    else:
        payload.update(_blocked_sections())
    _write_outputs(
        payload,
        output_root=output_root,
        docs_root=docs_root,
        research_quality_output_root=research_quality_output_root,
    )
    return payload


def _load_sources(
    *,
    source_2408_implementation_plan_path: Path,
    source_2408_contract_schema_plan_path: Path,
    source_2408_candidate_search_gate_policy_path: Path,
    source_2405_registry_snapshot_path: Path,
    source_2405_pit_gate_result_path: Path,
    source_2405_blocker_summary_path: Path,
    pit_input_registry_path: Path,
) -> dict[str, Any]:
    registry_config = safe_load_yaml_path(pit_input_registry_path)
    if isinstance(registry_config, dict):
        registry_config = {**registry_config, "path": str(pit_input_registry_path)}
    return {
        "implementation_plan_2408": _load_json_document(
            source_2408_implementation_plan_path
        ),
        "contract_schema_plan_2408": _load_json_document(
            source_2408_contract_schema_plan_path
        ),
        "candidate_search_gate_policy_2408": _load_json_document(
            source_2408_candidate_search_gate_policy_path
        ),
        "registry_snapshot_2405": _load_json_document(
            source_2405_registry_snapshot_path
        ),
        "pit_gate_result_2405": _load_json_document(source_2405_pit_gate_result_path),
        "blocker_summary_2405": _load_json_document(source_2405_blocker_summary_path),
        "pit_input_registry_config": registry_config,
        "source_paths": {
            "implementation_plan_2408": str(source_2408_implementation_plan_path),
            "contract_schema_plan_2408": str(source_2408_contract_schema_plan_path),
            "candidate_search_gate_policy_2408": str(
                source_2408_candidate_search_gate_policy_path
            ),
            "registry_snapshot_2405": str(source_2405_registry_snapshot_path),
            "pit_gate_result_2405": str(source_2405_pit_gate_result_path),
            "blocker_summary_2405": str(source_2405_blocker_summary_path),
            "pit_input_registry_config": str(pit_input_registry_path),
        },
    }


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in (
        "implementation_plan_2408",
        "contract_schema_plan_2408",
        "candidate_search_gate_policy_2408",
        "registry_snapshot_2405",
        "pit_gate_result_2405",
        "blocker_summary_2405",
    ):
        source = _as_mapping(sources.get(key))
        if source.get("_missing") is True:
            errors.append(f"missing source artifact: {key} -> {source.get('_path')}")
    implementation_2408 = _as_mapping(sources.get("implementation_plan_2408"))
    if implementation_2408.get("status") != m2408.READY_STATUS:
        errors.append("2408 implementation plan must be ready")
    if implementation_2408.get("route_to_next_task") != m2408.NEXT_ROUTE:
        errors.append("2408 route must point to TRADING-2409 signal contract schema")
    for gap in BLOCKING_GAPS:
        if gap not in _as_list(implementation_2408.get("blocking_gaps")):
            errors.append(f"2408 missing blocking gap: {gap}")
    for field in m2408.SAFETY_FALSE_FIELDS:
        if implementation_2408.get(field) is True:
            errors.append(f"2408 safety field must remain false: {field}")

    contract_plan_2408 = _as_mapping(
        _as_mapping(sources.get("contract_schema_plan_2408")).get(
            "contract_schema_plan"
        )
    )
    contracts = _as_mapping(contract_plan_2408.get("contracts"))
    for contract_id in (
        "signal_as_of_contract",
        "source_feature_traceability_contract",
        "signal_validity_contract",
    ):
        if contract_id not in contracts:
            errors.append(f"2408 contract schema plan missing {contract_id}")

    gate_policy_2408 = _as_mapping(
        _as_mapping(sources.get("candidate_search_gate_policy_2408")).get(
            "candidate_search_gate_policy"
        )
    )
    if gate_policy_2408.get("candidate_search_allowed") is not False:
        errors.append("2408 candidate search gate policy must remain blocked")

    pit_gate_result = _as_mapping(
        _as_mapping(sources.get("pit_gate_result_2405")).get("pit_gate_result")
    )
    if pit_gate_result.get("candidate_search_allowed") is not False:
        errors.append("2405 PIT gate must keep candidate search blocked")
    blockers = set(str(value) for value in _as_list(pit_gate_result.get("blockers")))
    for blocker in (
        "BLOCKING_GAP_GROWTH_TILT_ENGINE",
        "BLOCKING_GAP_VALID_UNTIL_WINDOW",
    ):
        if blocker not in blockers:
            errors.append(f"2405 PIT gate missing blocker: {blocker}")

    blocker_summary = _as_mapping(
        _as_mapping(sources.get("blocker_summary_2405")).get("pit_blocker_summary")
    )
    for gap in BLOCKING_GAPS:
        if gap not in _as_list(blocker_summary.get("blocking_gaps")):
            errors.append(f"2405 blocker summary missing gap: {gap}")

    registry_config = _as_mapping(sources.get("pit_input_registry_config"))
    if not registry_config:
        errors.append("PIT input registry config missing or empty")
    for gap in BLOCKING_GAPS:
        entry = _registry_entry(registry_config, gap)
        if not entry:
            errors.append(f"PIT input registry missing entry: {gap}")
            continue
        if entry.get("severity") != "BLOCKING":
            errors.append(f"PIT input registry entry must stay BLOCKING: {gap}")
        if entry.get("candidate_search_blocker") is not True:
            errors.append(f"PIT input registry entry must block candidate search: {gap}")
    return errors


def _base_payload(
    *,
    status: str,
    sources: Mapping[str, Any],
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
        "blocking_gaps": list(BLOCKING_GAPS),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "manual_review_required": True,
        "automatic_downgrade_allowed": False,
        "owner_review_required_for_any_downgrade": True,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
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
        "growth_tilt_engine_blocking_gap_resolved": False,
        "valid_until_window_blocking_gap_resolved": False,
        "any_blocker_severity_downgraded": False,
        "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
    }


def _ready_sections() -> dict[str, Any]:
    signal_as_of_schema = build_signal_as_of_contract_schema()
    source_feature_schema = build_source_feature_traceability_contract_schema()
    signal_validity_schema = build_signal_validity_contract_schema()
    snapshot = build_signal_contract_schema_snapshot()
    pit_gate_plan = build_pit_gate_integration_plan()
    validator_self_tests = {
        "signal_as_of_contract_valid_example": validate_signal_as_of_contract(
            valid_signal_as_of_contract_example()
        ),
        "source_feature_traceability_contract_valid_example": (
            validate_source_feature_traceability_contract(
                valid_source_feature_traceability_contract_example()
            )
        ),
        "signal_validity_contract_valid_example": validate_signal_validity_contract(
            valid_signal_validity_contract_example()
        ),
    }
    return {
        "signal_as_of_contract_schema_ready": True,
        "source_feature_traceability_contract_schema_ready": True,
        "signal_validity_contract_schema_ready": True,
        "schema_validation_helpers_ready": all(
            item.get("valid") is True for item in validator_self_tests.values()
        ),
        "contract_schema_snapshot_ready": True,
        "pit_gate_integration_plan_ready": True,
        "signal_as_of_contract_schema": signal_as_of_schema,
        "source_feature_traceability_contract_schema": source_feature_schema,
        "signal_validity_contract_schema": signal_validity_schema,
        "contract_schema_snapshot": snapshot,
        "schema_validation_helper_behavior": {
            "result_fields": [
                "valid",
                "schema_name",
                "error_count",
                "warning_count",
                "errors",
                "warnings",
            ],
            "validator_self_tests": validator_self_tests,
        },
        "pit_gate_integration_plan": pit_gate_plan,
        "route_to_next_task": NEXT_ROUTE,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "The shared schema is now ready; growth_tilt_engine source features "
            "must next be mapped to the source feature traceability and signal "
            "as-of contracts before replay validation or downgrade review."
        ),
    }


def _blocked_sections() -> dict[str, Any]:
    return {
        "signal_as_of_contract_schema_ready": False,
        "source_feature_traceability_contract_schema_ready": False,
        "signal_validity_contract_schema_ready": False,
        "schema_validation_helpers_ready": False,
        "contract_schema_snapshot_ready": False,
        "pit_gate_integration_plan_ready": False,
        "signal_as_of_contract_schema": {},
        "source_feature_traceability_contract_schema": {},
        "signal_validity_contract_schema": {},
        "contract_schema_snapshot": {},
        "schema_validation_helper_behavior": {},
        "pit_gate_integration_plan": {},
        "route_to_next_task": None,
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": "source validation failed",
    }


def _write_outputs(
    payload: dict[str, Any],
    *,
    output_root: Path,
    docs_root: Path,
    research_quality_output_root: Path,
) -> None:
    paths = {
        "json_path": str(output_root / "contract_schema_result.json"),
        "signal_as_of_contract_schema_json": str(
            output_root / "signal_as_of_contract_schema.json"
        ),
        "source_feature_traceability_contract_schema_json": str(
            output_root / "source_feature_traceability_contract_schema.json"
        ),
        "signal_validity_contract_schema_json": str(
            output_root / "signal_validity_contract_schema.json"
        ),
        "contract_schema_snapshot_json": str(
            output_root / "contract_schema_snapshot.json"
        ),
        "pit_gate_integration_plan_json": str(
            output_root / "pit_gate_integration_plan.json"
        ),
        "quality_signal_as_of_contract_schema_json": str(
            research_quality_output_root / "signal_as_of_contract_schema.json"
        ),
        "quality_source_feature_traceability_contract_schema_json": str(
            research_quality_output_root
            / "source_feature_traceability_contract_schema.json"
        ),
        "quality_signal_validity_contract_schema_json": str(
            research_quality_output_root / "signal_validity_contract_schema.json"
        ),
        "quality_contract_schema_snapshot_json": str(
            research_quality_output_root / "contract_schema_snapshot.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_signal_as_of_validity_contract_schema.md"
        ),
        "signal_as_of_contract_markdown": str(
            docs_root / "dynamic_strategy_signal_as_of_contract_schema.md"
        ),
        "source_feature_traceability_contract_markdown": str(
            docs_root
            / "dynamic_strategy_source_feature_traceability_contract_schema.md"
        ),
        "signal_validity_contract_markdown": str(
            docs_root / "dynamic_strategy_signal_validity_contract_schema.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2410_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    _write_section_json(
        paths["signal_as_of_contract_schema_json"],
        "dynamic_strategy_signal_as_of_contract_schema",
        "signal_as_of_contract_schema.v1",
        payload,
        "signal_as_of_contract_schema",
    )
    _write_section_json(
        paths["source_feature_traceability_contract_schema_json"],
        "dynamic_strategy_source_feature_traceability_contract_schema",
        "source_feature_traceability_contract_schema.v1",
        payload,
        "source_feature_traceability_contract_schema",
    )
    _write_section_json(
        paths["signal_validity_contract_schema_json"],
        "dynamic_strategy_signal_validity_contract_schema",
        "signal_validity_contract_schema.v1",
        payload,
        "signal_validity_contract_schema",
    )
    _write_section_json(
        paths["contract_schema_snapshot_json"],
        "dynamic_strategy_signal_contract_schema_snapshot",
        "signal_contract_schema_snapshot.v1",
        payload,
        "contract_schema_snapshot",
    )
    _write_section_json(
        paths["pit_gate_integration_plan_json"],
        "dynamic_strategy_signal_contract_pit_gate_integration_plan",
        "signal_contract_pit_gate_integration_plan.v1",
        payload,
        "pit_gate_integration_plan",
    )
    for path_key, payload_key in (
        ("quality_signal_as_of_contract_schema_json", "signal_as_of_contract_schema"),
        (
            "quality_source_feature_traceability_contract_schema_json",
            "source_feature_traceability_contract_schema",
        ),
        ("quality_signal_validity_contract_schema_json", "signal_validity_contract_schema"),
        ("quality_contract_schema_snapshot_json", "contract_schema_snapshot"),
    ):
        write_json_artifact(Path(paths[path_key]), _as_mapping(payload.get(payload_key)))

    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["signal_as_of_contract_markdown"]),
        _schema_markdown(
            "Dynamic strategy signal as-of contract schema",
            payload,
            "signal_as_of_contract_schema",
        ),
    )
    write_markdown_artifact(
        Path(paths["source_feature_traceability_contract_markdown"]),
        _schema_markdown(
            "Dynamic strategy source feature traceability contract schema",
            payload,
            "source_feature_traceability_contract_schema",
        ),
    )
    write_markdown_artifact(
        Path(paths["signal_validity_contract_markdown"]),
        _schema_markdown(
            "Dynamic strategy signal validity contract schema",
            payload,
            "signal_validity_contract_schema",
        ),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _write_section_json(
    path: str,
    report_type: str,
    schema_version: str,
    payload: Mapping[str, Any],
    payload_key: str,
) -> None:
    write_json_artifact(
        Path(path),
        {
            "task_id": TASK_ID,
            "status": payload.get("status"),
            "report_type": report_type,
            "schema_version": schema_version,
            payload_key: payload.get(payload_key, {}),
            "production_effect": "none",
            "broker_action": "none",
        },
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy signal as-of and validity contract schema",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- source tasks：`{', '.join(_as_list(payload.get('source_tasks')))}`",
            f"- blocking gaps：`{', '.join(_as_list(payload.get('blocking_gaps')))}`",
            f"- signal as-of schema ready：`{payload.get('signal_as_of_contract_schema_ready')}`",
            (
                "- source feature traceability schema ready："
                f"`{payload.get('source_feature_traceability_contract_schema_ready')}`"
            ),
            _field_line(
                "signal validity schema ready",
                payload,
                "signal_validity_contract_schema_ready",
            ),
            _field_line(
                "schema validation helpers ready",
                payload,
                "schema_validation_helpers_ready",
            ),
            f"- contract snapshot ready：`{payload.get('contract_schema_snapshot_ready')}`",
            _field_line(
                "PIT gate integration plan ready",
                payload,
                "pit_gate_integration_plan_ready",
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "2409 实现 reusable contract schema 与基础 validator。它支持后续 "
            "`growth_tilt_engine` source feature mapping、`valid_until_window` validity "
            "mapping、as-of replay validation 和 owner downgrade review；本任务不清除 "
            "blocker、不恢复 candidate search、不批准 observation / paper-shadow / execution。",
            "",
            "## Source findings from TRADING-2408",
            "",
            f"- 2408 source validation errors：`{payload.get('source_validation_errors')}`",
            f"- data quality gate executed：`{payload.get('data_quality_gate_executed')}`",
            f"- data quality gate reason：`{payload.get('data_quality_gate_reason')}`",
            "",
            "## Signal as-of contract schema",
            "",
            "```json",
            _json_block(payload.get("signal_as_of_contract_schema", {})),
            "```",
            "",
            "## Source feature traceability contract schema",
            "",
            "```json",
            _json_block(payload.get("source_feature_traceability_contract_schema", {})),
            "```",
            "",
            "## Signal validity contract schema",
            "",
            "```json",
            _json_block(payload.get("signal_validity_contract_schema", {})),
            "```",
            "",
            "## Schema validation helper behavior",
            "",
            "```json",
            _json_block(payload.get("schema_validation_helper_behavior", {})),
            "```",
            "",
            "## Contract snapshot",
            "",
            "```json",
            _json_block(payload.get("contract_schema_snapshot", {})),
            "```",
            "",
            "## PIT gate integration plan",
            "",
            "```json",
            _json_block(payload.get("pit_gate_integration_plan", {})),
            "```",
            "",
            "## Explicit non-approval list",
            "",
            "```json",
            _json_block(payload.get("explicit_non_approval_list", [])),
            "```",
            "",
            "## Recommended next route",
            "",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            f"- reason：{payload.get('recommended_next_research_task_reason')}",
            "",
            "## Safety boundary",
            "",
            _field_line(
                "growth_tilt_engine_blocking_gap_resolved",
                payload,
                "growth_tilt_engine_blocking_gap_resolved",
            ),
            _field_line(
                "valid_until_window_blocking_gap_resolved",
                payload,
                "valid_until_window_blocking_gap_resolved",
            ),
            _field_line(
                "any_blocker_severity_downgraded",
                payload,
                "any_blocker_severity_downgraded",
            ),
            f"- candidate_search_allowed：`{payload.get('candidate_search_allowed')}`",
            _field_line(
                "research_only_observation_allowed",
                payload,
                "research_only_observation_allowed",
            ),
            f"- paper_shadow_allowed：`{payload.get('paper_shadow_allowed')}`",
            f"- production_allowed：`{payload.get('production_allowed')}`",
            f"- broker_action：`{payload.get('broker_action')}`",
        ]
    )


def _field_line(label: str, payload: Mapping[str, Any], field: str) -> str:
    return f"- {label}：`{payload.get(field)}`"


def _schema_markdown(
    title: str,
    payload: Mapping[str, Any],
    payload_key: str,
) -> str:
    schema = payload.get(payload_key, {})
    return "\n".join(
        [
            f"# {title}",
            "",
            f"- status：`{payload.get('status')}`",
            f"- schema ready：`{bool(schema)}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "```json",
            _json_block(schema),
            "```",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2410 route",
            "",
            f"- current task：`{TASK_REGISTER_ID}`",
            f"- current status：`{payload.get('status')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2410 应把 `growth_tilt_engine` 的 source features 映射到 "
            "source feature traceability contract 和 signal as-of contract。2410 "
            "仍不得清除 blocker、降级 severity、恢复 candidate search 或进入 "
            "paper-shadow / production / broker 路径。",
        ]
    )


def _registry_entry(registry: Mapping[str, Any], input_id: str) -> Mapping[str, Any]:
    for entry in _as_list(registry.get("entries")):
        row = _as_mapping(entry)
        if row.get("input_id") == input_id:
            return row
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
