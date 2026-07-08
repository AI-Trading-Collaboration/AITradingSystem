from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
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
    growth_tilt_engine_pit_gate_readiness_recheck as recheck,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2419"
TASK_REGISTER_ID = "TRADING-2419_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK"
REPORT_TYPE = "growth_tilt_engine_pit_gate_readiness_recheck"
SCHEMA_VERSION = recheck.SCHEMA_VERSION
READY_STATUS = recheck.BLOCKED_BY_SIGNAL_ARTIFACT_STATUS
BLOCKED_SOURCE_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_BLOCKED_SOURCE_INCOMPLETE"
)
NEXT_ROUTE = recheck.NEXT_ROUTE
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2415",
    "TRADING-2416",
    "TRADING-2417",
    "TRADING-2418",
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PIT_GATE_READINESS_RECHECK_PRIOR_ARTIFACTS_AND_CONFIGS_"
    "ONLY_NO_FRESH_MARKET_DATA"
)

EXPLICIT_NON_APPROVAL_LIST: tuple[str, ...] = (
    "mark_any_source_feature_pit_gate_ready",
    "mark_any_source_feature_contract_ready",
    "resolve_signal_artifact_source_traceability_blocker",
    "downgrade_signal_artifact_source_traceability_blocker",
    "downgrade_growth_tilt_engine_blocker",
    "downgrade_valid_until_window_blocker",
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
    "auto_downgrade_blocker",
    "blockers_resolved",
    "blockers_downgraded",
    "growth_tilt_engine_blocking_gap_resolved",
    "growth_tilt_engine_severity_downgraded",
    "valid_until_window_blocking_gap_resolved",
    "valid_until_window_severity_downgraded",
    "signal_artifact_source_traceability_blocker_resolved",
    "signal_artifact_source_traceability_blocker_downgraded",
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
)

DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2418_CLOSURE_RESULT_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "closure_result.json"
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
DEFAULT_SOURCE_2418_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT
    / "growth_tilt_engine_valid_until_dependency_evidence_closure.md"
)
DEFAULT_SOURCE_2418_ROUTE_DOC_PATH = (
    m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT
    / "dynamic_strategy_2419_route.md"
)
DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "closure_result.json"
)
DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "closure_plan_result.json"
)
DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "remaining_blocker_matrix.json"
)
DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH = (
    m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    / "pit_gate_evidence_requirements.json"
)
DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_snapshot_result.json"
)
DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_matrix.json"
)
DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "pit_gate_readiness_validation.json"
)
DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH = (
    m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT
    / "remaining_blocker_summary.json"
)
DEFAULT_PIT_INPUT_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_strategy_pit_input_registry.yaml"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"


def run_growth_tilt_engine_pit_gate_readiness_recheck(
    *,
    source_2418_closure_result_path: Path = DEFAULT_SOURCE_2418_CLOSURE_RESULT_PATH,
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
    source_2418_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2418_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2418_research_doc_path: Path = DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH,
    source_2418_route_doc_path: Path = DEFAULT_SOURCE_2418_ROUTE_DOC_PATH,
    source_2417_closure_result_path: Path = DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH,
    source_2417_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    source_2416_closure_result_path: Path = DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Path = (
        DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH
    ),
    source_2416_pit_gate_evidence_requirements_path: Path = (
        DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH
    ),
    source_2415_readiness_snapshot_result_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH
    ),
    source_2415_readiness_matrix_path: Path = DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2415_readiness_validation_path: Path = (
        DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH
    ),
    source_2415_remaining_blocker_summary_path: Path = (
        DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH
    ),
    pit_input_registry_path: Path = DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Path = DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = {
        "closure_result_2418": _load_json_document(source_2418_closure_result_path),
        "valid_until_dependency_evidence_2418": _load_json_document(
            source_2418_valid_until_dependency_evidence_path
        ),
        "signal_validity_contract_evidence_2418": _load_json_document(
            source_2418_signal_validity_contract_evidence_path
        ),
        "stale_signal_policy_evidence_2418": _load_json_document(
            source_2418_stale_signal_policy_evidence_path
        ),
        "growth_tilt_valid_until_alignment_evidence_2418": _load_json_document(
            source_2418_growth_tilt_valid_until_alignment_evidence_path
        ),
        "remaining_blocker_summary_2418": _load_json_document(
            source_2418_remaining_blocker_summary_path
        ),
        "research_doc_2418": _load_text_document(source_2418_research_doc_path),
        "route_doc_2418": _load_text_document(source_2418_route_doc_path),
        "closure_result_2417": _load_json_document(source_2417_closure_result_path),
        "remaining_blocker_summary_2417": _load_json_document(
            source_2417_remaining_blocker_summary_path
        ),
        "closure_result_2416": _load_json_document(source_2416_closure_result_path),
        "remaining_blocker_matrix_2416": _load_json_document(
            source_2416_remaining_blocker_matrix_path
        ),
        "pit_gate_evidence_requirements_2416": _load_json_document(
            source_2416_pit_gate_evidence_requirements_path
        ),
        "readiness_snapshot_result_2415": _load_json_document(
            source_2415_readiness_snapshot_result_path
        ),
        "readiness_matrix_2415": _load_json_document(
            source_2415_readiness_matrix_path
        ),
        "readiness_validation_2415": _load_json_document(
            source_2415_readiness_validation_path
        ),
        "remaining_blocker_summary_2415": _load_json_document(
            source_2415_remaining_blocker_summary_path
        ),
        "pit_input_registry": safe_load_yaml_path(pit_input_registry_path),
        "report_registry": safe_load_yaml_path(report_registry_path),
        "artifact_catalog": _load_text_document(artifact_catalog_path),
    }
    source_validation_errors = _validate_sources(sources)
    if source_validation_errors:
        payload = _blocked_payload(
            source_validation_errors=source_validation_errors,
            as_of_date=as_of_date,
        )
    else:
        payload = recheck.build_growth_tilt_pit_gate_readiness_recheck(
            sources["closure_result_2418"],
            sources["valid_until_dependency_evidence_2418"],
            sources["remaining_blocker_summary_2418"],
            sources["closure_result_2417"],
            sources["remaining_blocker_summary_2417"],
            sources["closure_result_2416"],
            sources["remaining_blocker_matrix_2416"],
            sources["pit_gate_evidence_requirements_2416"],
            sources["readiness_snapshot_result_2415"],
            sources["readiness_matrix_2415"],
            sources["readiness_validation_2415"],
            sources["remaining_blocker_summary_2415"],
            pit_input_registry=sources["pit_input_registry"],
        )
        payload = _with_runtime_metadata(
            payload,
            source_validation_errors=source_validation_errors,
            as_of_date=as_of_date,
        )
    _write_outputs(payload, output_root=output_root, docs_root=docs_root)
    return payload


def _validate_sources(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for name, document in sources.items():
        if name in {"pit_input_registry"}:
            continue
        if isinstance(document, Mapping) and document.get("_missing") is True:
            errors.append(f"{name} missing: {document.get('_path')}")
    closure_2418 = _as_mapping(sources.get("closure_result_2418"))
    if closure_2418.get("status") != m2418.READY_STATUS:
        errors.append("TRADING-2418 closure status is not ready")
    if closure_2418.get("recommended_next_research_task") != (
        "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck"
    ):
        errors.append("TRADING-2418 route does not point to TRADING-2419")
    if closure_2418.get("pit_gate_ready_count") != 0:
        errors.append("TRADING-2418 pit_gate_ready_count must remain 0")
    if closure_2418.get("contract_ready_count") != 0:
        errors.append("TRADING-2418 contract_ready_count must remain 0")
    if closure_2418.get("valid_until_dependency_evidence_ready") is not True:
        errors.append("TRADING-2418 valid_until_dependency_evidence_ready is not true")
    if closure_2418.get("valid_until_dependency_still_blocked_count") != 0:
        errors.append("TRADING-2418 valid_until dependency still-blocked count changed")
    if recheck.SIGNAL_ARTIFACT_FEATURE_ID not in set(
        closure_2418.get("source_traceability_still_blocked") or []
    ):
        errors.append("TRADING-2418 source traceability still-blocked feature missing")
    closure_2417 = _as_mapping(sources.get("closure_result_2417"))
    if closure_2417.get("status") != m2417.READY_STATUS:
        errors.append("TRADING-2417 closure status is not ready")
    closure_2416 = _as_mapping(sources.get("closure_result_2416"))
    if closure_2416.get("status") != m2416.READY_STATUS:
        errors.append("TRADING-2416 closure status is not ready")
    snapshot_2415 = _as_mapping(sources.get("readiness_snapshot_result_2415"))
    if snapshot_2415.get("status") != m2415.READY_STATUS:
        errors.append("TRADING-2415 readiness status is not ready")
    matrix_2415 = _section(
        _as_mapping(sources.get("readiness_matrix_2415")),
        "pit_gate_readiness_matrix",
    )
    rows_2415 = matrix_2415.get("matrix_rows") or []
    if len(rows_2415) != 10:
        errors.append("TRADING-2415 readiness matrix must contain 10 rows")
    if not any(
        _as_mapping(row).get("source_feature_id") == recheck.SIGNAL_ARTIFACT_FEATURE_ID
        for row in rows_2415
    ):
        errors.append("TRADING-2415 matrix lacks growth_tilt_engine_signal_artifact row")
    validation_2415 = _section(
        _as_mapping(sources.get("readiness_validation_2415")),
        "pit_gate_readiness_validation",
    )
    if validation_2415.get("valid") is not True:
        errors.append("TRADING-2415 readiness validation is not valid")
    research_doc = _as_mapping(sources.get("research_doc_2418")).get("text", "")
    route_doc = _as_mapping(sources.get("route_doc_2418")).get("text", "")
    if m2418.READY_STATUS not in research_doc:
        errors.append("TRADING-2418 research doc does not include ready status")
    if "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck" not in route_doc:
        errors.append("TRADING-2418 route doc does not point to TRADING-2419")
    report_ids = {
        item.get("report_id")
        for item in _as_list(_as_mapping(sources.get("report_registry")).get("reports"))
        if isinstance(item, Mapping)
    }
    for required_report in (
        REPORT_TYPE,
        m2418.REPORT_TYPE,
        m2417.REPORT_TYPE,
        m2416.REPORT_TYPE,
        m2415.REPORT_TYPE,
    ):
        if required_report not in report_ids:
            errors.append(f"report registry missing {required_report}")
    catalog_text = _as_mapping(sources.get("artifact_catalog")).get("text", "")
    for command in (
        "growth-tilt-engine-pit-gate-readiness-recheck",
        "growth-tilt-engine-valid-until-dependency-evidence-closure",
    ):
        if command not in catalog_text:
            errors.append(f"artifact catalog missing command {command}")
    return errors


def _with_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched.update(
        {
            "as_of": str(as_of_date) if as_of_date else None,
            "generated_at": utc_now_iso(),
            "market_regime": AI_REGIME_SUMMARY["market_regime"],
            "market_regime_summary": dict(AI_REGIME_SUMMARY),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "data_quality_gate_executed": False,
            "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
            "fresh_market_data_read": False,
            "backtest_run": False,
            "scoring_run": False,
            "new_feature_generated": False,
            "new_signal_generated": False,
            "new_strategy_backtest_run": False,
            "candidate_search_enabled": False,
            "observation_enabled": False,
            "paper_shadow_allowed": False,
            "paper_trade_created": False,
            "shadow_position_created": False,
            "scheduled_task_created": False,
            "historical_event_log_mutated": False,
            "outcome_store_mutated": False,
            "production_allowed": False,
            "order_generated": False,
            "manual_review_required": True,
            "explicit_non_approval_list": list(EXPLICIT_NON_APPROVAL_LIST),
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    for field in SAFETY_FALSE_FIELDS:
        enriched[field] = False
    return enriched


def _blocked_payload(
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": BLOCKED_SOURCE_STATUS,
        "readiness_status": BLOCKED_SOURCE_STATUS,
        "source_feature_count": 0,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 0,
        "remaining_blocker_count": 0,
        "remaining_blockers": [],
        "blocker_classification": {
            "schema_version": recheck.BLOCKER_CLASSIFICATION_SCHEMA_VERSION,
            "rows": [],
            "remaining_blockers": [],
            "blocker_classification": {},
            "production_effect": "none",
            "broker_action": "none",
        },
        "pit_gate_recheck_matrix": {
            "schema_version": recheck.PIT_GATE_RECHECK_MATRIX_SCHEMA_VERSION,
            "row_count": 0,
            "matrix_rows": [],
            "production_effect": "none",
            "broker_action": "none",
        },
        "remaining_blocker_summary": {
            "schema_version": recheck.REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_SOURCE_STATUS,
            "remaining_blockers": [],
            "production_effect": "none",
            "broker_action": "none",
        },
        "recheck_validation": {
            "schema_version": "growth_tilt_engine_pit_gate_readiness_recheck_validation.v1",
            "valid": False,
            "error_count": len(source_validation_errors),
            "errors": list(source_validation_errors),
            "production_effect": "none",
            "broker_action": "none",
        },
        "recommended_next_research_task": None,
        "recommended_next_research_task_reason": (
            "Source validation failed; recheck cannot silently pass."
        ),
    }
    return _with_runtime_metadata(
        payload,
        source_validation_errors=source_validation_errors,
        as_of_date=as_of_date,
    )


def _write_outputs(payload: dict[str, Any], *, output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "readiness_recheck_result.json"
    matrix_json_path = output_root / "pit_gate_recheck_matrix.json"
    blocker_json_path = output_root / "blocker_classification.json"
    remaining_json_path = output_root / "remaining_blocker_summary.json"
    markdown_path = docs_root / "growth_tilt_engine_pit_gate_readiness_recheck.md"
    matrix_markdown_path = docs_root / "growth_tilt_engine_pit_gate_recheck_matrix.md"
    blocker_markdown_path = (
        docs_root / "growth_tilt_engine_signal_artifact_source_traceability_blocker.md"
    )
    route_markdown_path = docs_root / "dynamic_strategy_2420_route.md"
    artifact_paths = {
        "json_path": str(json_path),
        "pit_gate_recheck_matrix_json": str(matrix_json_path),
        "blocker_classification_json": str(blocker_json_path),
        "remaining_blocker_summary_json": str(remaining_json_path),
        "markdown_path": str(markdown_path),
        "pit_gate_recheck_matrix_markdown": str(matrix_markdown_path),
        "signal_artifact_blocker_markdown": str(blocker_markdown_path),
        "next_route_markdown": str(route_markdown_path),
    }
    payload["artifact_paths"] = artifact_paths
    write_json_artifact(json_path, payload)
    write_section_json_artifact(
        matrix_json_path,
        "growth_tilt_engine_pit_gate_recheck_matrix",
        recheck.PIT_GATE_RECHECK_MATRIX_SCHEMA_VERSION,
        payload,
        "pit_gate_recheck_matrix",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        blocker_json_path,
        "growth_tilt_engine_pit_gate_recheck_blocker_classification",
        recheck.BLOCKER_CLASSIFICATION_SCHEMA_VERSION,
        payload,
        "blocker_classification",
        task_id=TASK_ID,
    )
    write_section_json_artifact(
        remaining_json_path,
        "growth_tilt_engine_remaining_blocker_summary_after_pit_gate_recheck",
        recheck.REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        payload,
        "remaining_blocker_summary",
        task_id=TASK_ID,
    )
    write_markdown_artifact(markdown_path, _render_main_markdown(payload))
    write_markdown_artifact(matrix_markdown_path, _render_matrix_markdown(payload))
    write_markdown_artifact(blocker_markdown_path, _render_blocker_markdown(payload))
    write_markdown_artifact(route_markdown_path, _render_route_markdown(payload))


def _render_main_markdown(payload: Mapping[str, Any]) -> str:
    summary = {
        "status": payload.get("status"),
        "pit_gate_ready_count": payload.get("pit_gate_ready_count"),
        "contract_ready_count": payload.get("contract_ready_count"),
        "pit_gate_blocked_count": payload.get("pit_gate_blocked_count"),
        "remaining_blockers": payload.get("remaining_blockers"),
        "next_route": payload.get("recommended_next_research_task"),
    }
    return "\n".join(
        [
            "# Growth Tilt Engine PIT Gate Readiness Recheck（PIT Gate Readiness 复核）",
            "",
            "## 摘要",
            "",
            f"- task_id：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- market regime：`{payload.get('market_regime')}`",
            f"- PIT gate ready count：`{payload.get('pit_gate_ready_count')}`",
            f"- contract-ready count：`{payload.get('contract_ready_count')}`",
            f"- remaining blockers：`{payload.get('remaining_blockers')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2419 只做 readiness recheck。它不标记 PIT gate ready、"
            "不标记 contract ready、不解除或降级 blocker、不启用 "
            "paper-shadow / production / broker。",
            "",
            "## 摘要 JSON",
            "",
            "```json",
            _json_block(summary),
            "```",
            "",
            "## Blocker 分类",
            "",
            "```json",
            _json_block(payload.get("blocker_classification")),
            "```",
            "",
            "## 安全边界",
            "",
            "```json",
            _json_block(
                {
                    field: payload.get(field)
                    for field in (
                        "auto_mark_pit_gate_ready",
                        "auto_mark_contract_ready",
                        "blockers_resolved",
                        "blockers_downgraded",
                        "paper_shadow_enabled",
                        "production_enabled",
                        "broker_enabled",
                        "daily_report_generated",
                    )
                }
            ),
            "```",
        ]
    )


def _render_matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine PIT Gate Recheck Matrix（复核矩阵）",
            "",
            "```json",
            _json_block(payload.get("pit_gate_recheck_matrix")),
            "```",
        ]
    )


def _render_blocker_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Growth Tilt Engine Signal Artifact Source Traceability Blocker"
            "（Source Traceability Blocker）",
            "",
            f"- blocker：`{recheck.SIGNAL_ARTIFACT_FEATURE_ID}`",
            "- classification：`source_traceability`",
            f"- status：`{payload.get('status')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "2419 不修复该 blocker，也不降级 severity；2420 才处理 signal artifact "
            "source traceability remediation。",
        ]
    )


def _render_route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy TRADING-2420 Route（下一跳路线）",
            "",
            f"- source task：`{TASK_ID}`",
            f"- source status：`{payload.get('status')}`",
            f"- 下一任务：`{payload.get('recommended_next_research_task')}`",
            "",
            "TRADING-2420 应处理 `growth_tilt_engine_signal_artifact` 的 source "
            "traceability remediation。"
            "2419 不授权 candidate search、observation、paper-shadow、scheduler、event append、"
            "outcome binding、production 或 broker/order。",
        ]
    )


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if isinstance(value, Mapping):
        return value
    return document


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
