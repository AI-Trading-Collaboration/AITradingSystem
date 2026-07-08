from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation.v1"
)
PIT_GATE_RECHECK_AFTER_REMEDIATION_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_recheck_after_source_traceability_remediation_"
    "matrix.v1"
)
BLOCKER_RESOLUTION_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_blocker_resolution_summary.v1"
)
CONTRACT_READINESS_SNAPSHOT_GATE_SCHEMA_VERSION = (
    "growth_tilt_engine_contract_readiness_snapshot_gate.v1"
)

READY_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_"
    "REMEDIATION_READY"
)
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_"
    "REMEDIATION_BLOCKED"
)
NEXT_ROUTE_READY = "TRADING-2422_Growth_Tilt_Engine_Contract_Readiness_Snapshot"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2422_Growth_Tilt_Engine_Source_Traceability_Recheck_Failure_Closure"
)
PRIOR_ROUTE = (
    "TRADING-2420_Growth_Tilt_Engine_Signal_Artifact_Source_Traceability_"
    "Remediation"
)
ARTIFACT_ID = "growth_tilt_engine_signal_artifact"
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY"
)
SOURCE_TRACEABILITY_RECHECK_2419_BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_"
    "BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY"
)

REPORT_TYPE = (
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation"
)
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_signal_artifact_source_traceability_remediation",
    "growth_tilt_engine_pit_gate_readiness_recheck",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies "
    "growth-tilt-engine-pit-gate-readiness-recheck-after-source-traceability-"
    "remediation",
    "aits research strategies "
    "growth-tilt-engine-signal-artifact-source-traceability-remediation",
    "outputs/research_strategies/"
    "growth_tilt_engine_signal_artifact_source_traceability_remediation/"
    "remediation_result.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_signal_artifact_source_traceability_remediation/"
    "source_traceability_manifest.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_signal_artifact_source_traceability_remediation/"
    "source_lineage_map.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_signal_artifact_source_traceability_remediation/"
    "missing_source_evidence_summary.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation/readiness_recheck_after_remediation_result.json",
)


def build_growth_tilt_pit_gate_readiness_recheck_after_source_traceability_remediation(
    remediation_result_2420: Mapping[str, Any],
    source_traceability_manifest_2420: Mapping[str, Any],
    source_lineage_map_2420: Mapping[str, Any],
    missing_source_evidence_summary_2420: Mapping[str, Any],
    readiness_recheck_2419: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    manifest = _section(
        source_traceability_manifest_2420,
        "source_traceability_manifest",
    )
    lineage_map = _section(source_lineage_map_2420, "source_lineage_map")
    missing_summary = _section(
        missing_source_evidence_summary_2420,
        "missing_source_evidence_summary",
    )
    blocker_resolution_errors = _blocker_resolution_errors(
        remediation_result_2420,
        manifest,
        lineage_map,
        missing_summary,
        readiness_recheck_2419,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        research_doc_texts=research_doc_texts or {},
    )
    source_traceability_accepted = not blocker_resolution_errors
    resolved_blockers = [ARTIFACT_ID] if source_traceability_accepted else []
    remaining_blockers = [] if source_traceability_accepted else [ARTIFACT_ID]
    status = READY_STATUS if source_traceability_accepted else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if source_traceability_accepted else NEXT_ROUTE_BLOCKED
    pit_gate_ready_count = 1 if source_traceability_accepted else 0
    pit_gate_blocked_count = 0 if source_traceability_accepted else 1
    blocker_resolution_summary = _blocker_resolution_summary(
        status=status,
        source_traceability_accepted=source_traceability_accepted,
        resolved_blockers=resolved_blockers,
        remaining_blockers=remaining_blockers,
        blocker_resolution_errors=blocker_resolution_errors,
        missing_summary=missing_summary,
    )
    contract_readiness_snapshot_gate = _contract_readiness_snapshot_gate(
        pit_gate_ready=source_traceability_accepted,
        next_route=next_route,
    )
    matrix = _pit_gate_recheck_after_remediation_matrix(
        status=status,
        source_traceability_accepted=source_traceability_accepted,
        resolved_blockers=resolved_blockers,
        remaining_blockers=remaining_blockers,
        next_route=next_route,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2421",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2419", "TRADING-2420"],
        "artifact_id": ARTIFACT_ID,
        "source_traceability_remediation_status": remediation_result_2420.get(
            "remediation_status"
        ),
        "source_traceability_recheck_status": (
            "ACCEPTED" if source_traceability_accepted else "REJECTED"
        ),
        "source_traceability_evidence_complete_after_2420": (
            source_traceability_accepted
        ),
        "source_traceability_blocker_resolved": source_traceability_accepted,
        "signal_artifact_source_traceability_blocker_resolved": (
            source_traceability_accepted
        ),
        "signal_artifact_source_traceability_blocker_downgraded": False,
        "blockers_resolved": source_traceability_accepted,
        "blockers_downgraded": False,
        "resolved_blockers": resolved_blockers,
        "remaining_blockers": remaining_blockers,
        "remaining_blocker_count": len(remaining_blockers),
        "blocker_classification": {
            ARTIFACT_ID: "source_traceability",
        },
        "blocker_resolution_error_count": len(blocker_resolution_errors),
        "blocker_resolution_errors": blocker_resolution_errors,
        "pit_gate_ready": source_traceability_accepted,
        "pit_gate_ready_count": pit_gate_ready_count,
        "pit_gate_blocked_count": pit_gate_blocked_count,
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_readiness_snapshot_required": True,
        "paper_shadow_blocked": True,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "pit_gate_recheck_completed": True,
        "pit_gate_recheck_after_remediation_matrix": matrix,
        "blocker_resolution_summary": blocker_resolution_summary,
        "contract_readiness_snapshot_gate": contract_readiness_snapshot_gate,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "new_feature_generated": False,
        "new_signal_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "TRADING-2421 accepts TRADING-2420 source traceability remediation; "
            "contract readiness still requires an independent snapshot."
            if source_traceability_accepted
            else "TRADING-2421 rejected the after-remediation source traceability "
            "evidence chain; PIT gate remains blocked."
        ),
    }


def _blocker_resolution_errors(
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
    readiness_recheck_2419: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    research_doc_texts: Mapping[str, str],
) -> list[str]:
    errors: list[str] = []
    if remediation_result_2420.get("status") != SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS:
        errors.append("TRADING-2420 remediation result status is not READY")
    if remediation_result_2420.get("remediation_status") != "READY":
        errors.append("TRADING-2420 remediation_status is not READY")
    if remediation_result_2420.get("artifact_id") != ARTIFACT_ID:
        errors.append("TRADING-2420 artifact_id is not growth_tilt_engine_signal_artifact")
    for field in (
        "source_traceability_evidence_complete",
        "source_traceability_blocker_resolved",
        "blocker_resolved",
    ):
        if remediation_result_2420.get(field) is not True:
            errors.append(f"TRADING-2420 {field} is not true")

    if readiness_recheck_2419.get("status") != SOURCE_TRACEABILITY_RECHECK_2419_BLOCKED_STATUS:
        errors.append("TRADING-2419 readiness recheck status is not the expected blocker")
    if ARTIFACT_ID not in set(_as_list(readiness_recheck_2419.get("remaining_blockers"))):
        errors.append("TRADING-2419 does not preserve growth_tilt_engine_signal_artifact")

    if manifest.get("artifact_id") != ARTIFACT_ID:
        errors.append("TRADING-2420 source traceability manifest artifact_id mismatch")
    if manifest.get("traceability_status") != "READY":
        errors.append("TRADING-2420 source traceability manifest is not READY")
    if manifest.get("pit_gate_ready_after_2420") is not False:
        errors.append("TRADING-2420 manifest must not pre-mark PIT gate ready")
    if manifest.get("contract_ready_after_2420") is not False:
        errors.append("TRADING-2420 manifest must not pre-mark contract ready")

    source_artifacts = [_as_mapping(row) for row in _as_list(manifest.get("source_artifacts"))]
    if not source_artifacts:
        errors.append("TRADING-2420 manifest has no source artifacts")
    for record in source_artifacts:
        _append_record_presence_errors(
            errors,
            record,
            record_id=str(record.get("artifact_id") or record.get("path")),
            flags=(
                "report_registry_present",
                "catalog_reference_present",
                "source_file_present",
            ),
        )

    source_documents = [_as_mapping(row) for row in _as_list(manifest.get("source_documents"))]
    if not source_documents:
        errors.append("TRADING-2420 manifest has no source documents")
    for record in source_documents:
        _append_record_presence_errors(
            errors,
            record,
            record_id=str(record.get("document_id") or record.get("path")),
            flags=(
                "report_registry_present",
                "catalog_reference_present",
                "document_present",
            ),
        )

    if lineage_map.get("artifact_id") != ARTIFACT_ID:
        errors.append("TRADING-2420 source lineage map artifact_id mismatch")
    if not _as_list(lineage_map.get("upstream_dependencies")):
        errors.append("TRADING-2420 source lineage map has no upstream dependencies")
    if not _as_list(lineage_map.get("source_documents")):
        errors.append("TRADING-2420 source lineage map has no source documents")
    dependency = _as_mapping(lineage_map.get("dependency_closure_reference"))
    if dependency.get("ready_for_pit_gate_recheck") is not True:
        errors.append("TRADING-2420 dependency closure is not ready for PIT gate recheck")

    for field in (
        "missing_field_count",
        "incomplete_field_count",
        "unresolved_blocker_count",
    ):
        if _int_value(missing_summary.get(field)) != 0:
            errors.append(f"TRADING-2420 missing evidence summary {field} is not 0")
    if missing_summary.get("prior_missing_evidence_closed_by_2420") is not True:
        errors.append("TRADING-2420 prior missing evidence is not closed")

    report_ids = {
        item.get("report_id")
        for item in _as_list(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    for report_id in REQUIRED_REPORT_IDS:
        if report_id not in report_ids:
            errors.append(f"report registry missing {report_id}")
    for reference in REQUIRED_CATALOG_REFERENCES:
        if reference not in artifact_catalog_text:
            errors.append(f"artifact catalog missing {reference}")

    for doc_id, text in research_doc_texts.items():
        if not text:
            errors.append(f"research doc missing text for {doc_id}")
    return errors


def _append_record_presence_errors(
    errors: list[str],
    record: Mapping[str, Any],
    *,
    record_id: str,
    flags: tuple[str, ...],
) -> None:
    for flag in flags:
        if record.get(flag) is not True:
            errors.append(f"{record_id} {flag} is not true")


def _blocker_resolution_summary(
    *,
    status: str,
    source_traceability_accepted: bool,
    resolved_blockers: list[str],
    remaining_blockers: list[str],
    blocker_resolution_errors: list[str],
    missing_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": BLOCKER_RESOLUTION_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "artifact_id": ARTIFACT_ID,
        "source_traceability_recheck_status": (
            "ACCEPTED" if source_traceability_accepted else "REJECTED"
        ),
        "resolved_blockers": resolved_blockers,
        "remaining_blockers": remaining_blockers,
        "blocker_classification": {ARTIFACT_ID: "source_traceability"},
        "missing_field_count": _int_value(missing_summary.get("missing_field_count")),
        "incomplete_field_count": _int_value(
            missing_summary.get("incomplete_field_count")
        ),
        "unresolved_blocker_count": _int_value(
            missing_summary.get("unresolved_blocker_count")
        ),
        "blocker_resolution_error_count": len(blocker_resolution_errors),
        "blocker_resolution_errors": blocker_resolution_errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def _contract_readiness_snapshot_gate(
    *, pit_gate_ready: bool, next_route: str
) -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_READINESS_SNAPSHOT_GATE_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "pit_gate_ready": pit_gate_ready,
        "contract_ready": False,
        "contract_ready_count": 0,
        "contract_readiness_snapshot_required": True,
        "contract_readiness_evaluated_in_2421": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _pit_gate_recheck_after_remediation_matrix(
    *,
    status: str,
    source_traceability_accepted: bool,
    resolved_blockers: list[str],
    remaining_blockers: list[str],
    next_route: str,
) -> dict[str, Any]:
    row = {
        "gate_id": "growth_tilt_engine_pit_gate",
        "artifact_id": ARTIFACT_ID,
        "prior_route": PRIOR_ROUTE,
        "prior_2419_status": SOURCE_TRACEABILITY_RECHECK_2419_BLOCKED_STATUS,
        "source_traceability_recheck_status": (
            "ACCEPTED" if source_traceability_accepted else "REJECTED"
        ),
        "pit_gate_ready_after_recheck": source_traceability_accepted,
        "contract_ready_after_recheck": False,
        "paper_shadow_enabled": False,
        "eligible_for_paper_shadow": False,
        "eligible_for_production": False,
        "eligible_for_broker": False,
        "resolved_blockers": resolved_blockers,
        "remaining_blockers": remaining_blockers,
        "next_route": next_route,
        "recheck_status": status,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {
        "schema_version": PIT_GATE_RECHECK_AFTER_REMEDIATION_MATRIX_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "row_count": 1,
        "matrix_rows": [row],
        "production_effect": "none",
        "broker_action": "none",
    }


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if isinstance(value, Mapping):
        return value
    return document


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int_value(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0
