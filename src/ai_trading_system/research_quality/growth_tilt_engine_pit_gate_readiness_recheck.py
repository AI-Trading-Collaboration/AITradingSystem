from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_pit_gate_readiness_recheck.v1"
PIT_GATE_RECHECK_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_readiness_recheck_matrix.v1"
)
BLOCKER_CLASSIFICATION_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_readiness_recheck_blocker_classification.v1"
)
REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_remaining_blocker_summary_after_pit_gate_recheck.v1"
)

BLOCKED_BY_SIGNAL_ARTIFACT_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_"
    "BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY"
)
READY_WITH_BLOCKERS_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_READY_WITH_BLOCKERS"
)
NEXT_ROUTE = (
    "TRADING-2420_Growth_Tilt_Engine_Signal_Artifact_Source_Traceability_Remediation"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SIGNAL_ARTIFACT_FEATURE_ID = "growth_tilt_engine_signal_artifact"
VALID_UNTIL_DEPENDENCY_FEATURE_ID = "execution_signal_validity_policy"


def build_growth_tilt_pit_gate_readiness_recheck(
    closure_result_2418: Mapping[str, Any],
    valid_until_dependency_evidence_2418: Mapping[str, Any],
    remaining_blocker_summary_2418: Mapping[str, Any],
    closure_result_2417: Mapping[str, Any],
    remaining_blocker_summary_2417: Mapping[str, Any],
    closure_result_2416: Mapping[str, Any],
    remaining_blocker_matrix_2416: Mapping[str, Any],
    pit_gate_evidence_requirements_2416: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    pit_gate_readiness_matrix_2415: Mapping[str, Any],
    pit_gate_readiness_validation_2415: Mapping[str, Any],
    remaining_blocker_summary_2415: Mapping[str, Any],
    *,
    pit_input_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows_2415 = _matrix_rows(pit_gate_readiness_matrix_2415)
    blocker_rows_2416 = _matrix_rows(
        remaining_blocker_matrix_2416,
        section_key="remaining_blocker_matrix",
        row_key="matrix_rows",
    )
    counts = _counts(
        closure_result_2418,
        pit_gate_readiness_snapshot_2415,
        rows_2415,
    )
    valid_until_evidence_ready = _valid_until_evidence_ready(
        closure_result_2418,
        valid_until_dependency_evidence_2418,
    )
    source_traceability_still_blocked = _source_traceability_still_blocked(
        closure_result_2418,
        remaining_blocker_summary_2418,
        remaining_blocker_summary_2417,
    )
    status = (
        BLOCKED_BY_SIGNAL_ARTIFACT_STATUS
        if source_traceability_still_blocked
        else READY_WITH_BLOCKERS_STATUS
    )
    recheck_rows = _recheck_rows(
        rows_2415,
        blocker_rows_2416,
        signal_artifact_still_blocked=source_traceability_still_blocked,
        valid_until_evidence_ready=valid_until_evidence_ready,
    )
    blocker_classification = _blocker_classification(
        source_traceability_still_blocked=source_traceability_still_blocked,
        valid_until_evidence_ready=valid_until_evidence_ready,
    )
    remaining_summary = _remaining_blocker_summary(
        counts,
        blocker_classification=blocker_classification,
        status=status,
        pit_input_registry=pit_input_registry or {},
    )
    validation = _validation(
        closure_result_2418,
        closure_result_2417,
        closure_result_2416,
        pit_gate_readiness_snapshot_2415,
        pit_gate_readiness_validation_2415,
        pit_gate_evidence_requirements_2416,
        counts=counts,
        source_traceability_still_blocked=source_traceability_still_blocked,
        valid_until_evidence_ready=valid_until_evidence_ready,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2419",
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "source_tasks": [
            "TRADING-2415",
            "TRADING-2416",
            "TRADING-2417",
            "TRADING-2418",
        ],
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": counts["source_feature_count"],
        "inherited_blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "inherited_blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "valid_until_dependency_evidence_ready_from_2418": valid_until_evidence_ready,
        "valid_until_dependency_still_blocked_count_after_recheck": (
            0 if valid_until_evidence_ready else 1
        ),
        "remaining_blocker_count": 1 if source_traceability_still_blocked else 0,
        "remaining_blockers": (
            [SIGNAL_ARTIFACT_FEATURE_ID] if source_traceability_still_blocked else []
        ),
        "blocker_classification": blocker_classification,
        "pit_gate_recheck_matrix": {
            "schema_version": PIT_GATE_RECHECK_MATRIX_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "source_tasks": [
                "TRADING-2415",
                "TRADING-2416",
                "TRADING-2417",
                "TRADING-2418",
            ],
            "row_count": len(recheck_rows),
            "matrix_rows": recheck_rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "remaining_blocker_summary": remaining_summary,
        "recheck_validation": validation,
        "readiness_status": status,
        "pit_gate_ready": False,
        "contract_ready": False,
        "paper_shadow_blocked": True,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "pit_gate_recheck_completed": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "auto_downgrade_blocker": False,
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "signal_artifact_source_traceability_blocker_resolved": False,
        "signal_artifact_source_traceability_blocker_downgraded": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "recommended_next_research_task_reason": (
            "TRADING-2419 confirms valid_until dependency evidence exists but "
            "the growth tilt engine PIT gate remains blocked by the standalone "
            "growth_tilt_engine_signal_artifact source traceability gap."
        ),
    }


def _counts(
    closure_result_2418: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    rows_2415: list[Mapping[str, Any]],
) -> dict[str, int]:
    source_feature_count = int(
        closure_result_2418.get("source_feature_count")
        or pit_gate_readiness_snapshot_2415.get("source_feature_count")
        or len(rows_2415)
    )
    return {
        "source_feature_count": source_feature_count,
        "pit_gate_ready_count": int(
            closure_result_2418.get("pit_gate_ready_count")
            or pit_gate_readiness_snapshot_2415.get("pit_gate_ready_count")
            or 0
        ),
        "contract_ready_count": int(
            closure_result_2418.get("contract_ready_count")
            or pit_gate_readiness_snapshot_2415.get("contract_ready_count")
            or 0
        ),
        "pit_gate_blocked_count": int(
            closure_result_2418.get("pit_gate_blocked_count")
            or pit_gate_readiness_snapshot_2415.get("pit_gate_blocked_count")
            or source_feature_count
        ),
        "blocked_by_source_traceability_count": int(
            closure_result_2418.get("blocked_by_source_traceability_count")
            or pit_gate_readiness_snapshot_2415.get(
                "blocked_by_source_traceability_count"
            )
            or 0
        ),
        "blocked_by_valid_until_window_count": int(
            closure_result_2418.get("blocked_by_valid_until_window_count")
            or pit_gate_readiness_snapshot_2415.get(
                "blocked_by_valid_until_window_count"
            )
            or 0
        ),
    }


def _valid_until_evidence_ready(
    closure_result_2418: Mapping[str, Any],
    valid_until_dependency_evidence_2418: Mapping[str, Any],
) -> bool:
    evidence_section = _section(
        valid_until_dependency_evidence_2418,
        "valid_until_dependency_evidence",
    )
    rows = [_as_mapping(row) for row in _as_list(evidence_section.get("evidence_rows"))]
    return bool(
        closure_result_2418.get("valid_until_dependency_evidence_ready") is True
        and closure_result_2418.get("valid_until_dependency_still_blocked_count") == 0
        and any(
            row.get("dependent_feature_or_signal") == VALID_UNTIL_DEPENDENCY_FEATURE_ID
            and row.get("ready_for_pit_gate_recheck") is True
            for row in rows
        )
    )


def _source_traceability_still_blocked(
    closure_result_2418: Mapping[str, Any],
    remaining_blocker_summary_2418: Mapping[str, Any],
    remaining_blocker_summary_2417: Mapping[str, Any],
) -> bool:
    candidates: list[Any] = [
        closure_result_2418.get("source_traceability_still_blocked"),
    ]
    for source in (remaining_blocker_summary_2418, remaining_blocker_summary_2417):
        summary = _section(source, "remaining_blocker_summary")
        candidates.append(summary.get("source_traceability_still_blocked_feature_ids"))
    return any(
        SIGNAL_ARTIFACT_FEATURE_ID in {_text(item) for item in _as_list(candidate)}
        for candidate in candidates
    )


def _recheck_rows(
    rows_2415: list[Mapping[str, Any]],
    blocker_rows_2416: list[Mapping[str, Any]],
    *,
    signal_artifact_still_blocked: bool,
    valid_until_evidence_ready: bool,
) -> list[dict[str, Any]]:
    blocker_by_feature = {
        _text(row.get("source_feature_id")): row for row in blocker_rows_2416
    }
    rows: list[dict[str, Any]] = []
    for row in rows_2415:
        feature_id = _text(row.get("source_feature_id"))
        inherited = blocker_by_feature.get(feature_id, {})
        if feature_id == SIGNAL_ARTIFACT_FEATURE_ID and signal_artifact_still_blocked:
            recheck_status = "blocked_by_signal_artifact_source_traceability"
            blocker = "source_traceability"
            remaining_blocker = True
        elif feature_id == VALID_UNTIL_DEPENDENCY_FEATURE_ID and valid_until_evidence_ready:
            recheck_status = "valid_until_dependency_evidence_ready_but_gate_blocked"
            blocker = "valid_until_dependency_evidence_closed"
            remaining_blocker = False
        else:
            recheck_status = "gate_blocked_by_signal_artifact_source_traceability"
            blocker = "blocked_by_engine_level_signal_artifact"
            remaining_blocker = False
        rows.append(
            {
                "source_feature_id": feature_id,
                "source_feature_type": row.get("source_feature_type"),
                "inherited_pit_gate_status_from_2415": row.get("pit_gate_status"),
                "inherited_blocking_reason_from_2415": row.get(
                    "pit_gate_blocking_reason"
                ),
                "planned_closure_route_from_2416": inherited.get(
                    "recommended_next_task"
                ),
                "recheck_status": recheck_status,
                "blocker_classification_after_recheck": blocker,
                "remaining_blocker_after_recheck": remaining_blocker,
                "pit_gate_ready_after_recheck": False,
                "contract_ready_after_recheck": False,
                "eligible_for_candidate_search": False,
                "eligible_for_observation": False,
                "eligible_for_paper_shadow": False,
                "eligible_for_production": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return rows


def _blocker_classification(
    *,
    source_traceability_still_blocked: bool,
    valid_until_evidence_ready: bool,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if source_traceability_still_blocked:
        rows.append(
            {
                "blocker_id": SIGNAL_ARTIFACT_FEATURE_ID,
                "blocker_classification": "source_traceability",
                "source_task": "TRADING-2417",
                "still_blocked_after_recheck": True,
                "blocking_pit_gate_ready": True,
                "recommended_next_task": NEXT_ROUTE,
                "remediation_allowed_in_2419": False,
                "resolution_or_downgrade_attempted": False,
            }
        )
    rows.append(
        {
            "blocker_id": VALID_UNTIL_DEPENDENCY_FEATURE_ID,
            "blocker_classification": "valid_until_dependency",
            "source_task": "TRADING-2418",
            "still_blocked_after_recheck": not valid_until_evidence_ready,
            "blocking_pit_gate_ready": not valid_until_evidence_ready,
            "recommended_next_task": NEXT_ROUTE if valid_until_evidence_ready else None,
            "remediation_allowed_in_2419": False,
            "resolution_or_downgrade_attempted": False,
        }
    )
    return {
        "schema_version": BLOCKER_CLASSIFICATION_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "remaining_blockers": (
            [SIGNAL_ARTIFACT_FEATURE_ID] if source_traceability_still_blocked else []
        ),
        "blocker_classification": {
            SIGNAL_ARTIFACT_FEATURE_ID: "source_traceability"
        }
        if source_traceability_still_blocked
        else {},
        "valid_until_dependency_evidence_ready": valid_until_evidence_ready,
        "rows": rows,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_blocker_summary(
    counts: Mapping[str, int],
    *,
    blocker_classification: Mapping[str, Any],
    status: str,
    pit_input_registry: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "status": status,
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": counts["source_feature_count"],
        "remaining_blocker_count": len(
            _as_list(blocker_classification.get("remaining_blockers"))
        ),
        "remaining_blockers": list(
            _as_list(blocker_classification.get("remaining_blockers"))
        ),
        "blocker_classification": dict(
            _as_mapping(blocker_classification.get("blocker_classification"))
        ),
        "growth_tilt_engine_pit_input_severity": _pit_input_severity(
            pit_input_registry,
            "growth_tilt_engine",
        ),
        "valid_until_window_pit_input_severity": _pit_input_severity(
            pit_input_registry,
            "valid_until_window",
        ),
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "paper_shadow_blocked": True,
        "recommended_next_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _validation(
    closure_result_2418: Mapping[str, Any],
    closure_result_2417: Mapping[str, Any],
    closure_result_2416: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    pit_gate_readiness_validation_2415: Mapping[str, Any],
    pit_gate_evidence_requirements_2416: Mapping[str, Any],
    *,
    counts: Mapping[str, int],
    source_traceability_still_blocked: bool,
    valid_until_evidence_ready: bool,
) -> dict[str, Any]:
    errors: list[str] = []
    if closure_result_2418.get("status") != (
        "GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY"
    ):
        errors.append("TRADING-2418 closure result status is not ready")
    if closure_result_2418.get("recommended_next_research_task") != (
        "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck"
    ):
        errors.append("TRADING-2418 next route does not point to TRADING-2419")
    if closure_result_2417.get("status") != (
        "GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY"
    ):
        errors.append("TRADING-2417 closure result status is not ready")
    if closure_result_2416.get("status") != (
        "GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY"
    ):
        errors.append("TRADING-2416 closure result status is not ready")
    if pit_gate_readiness_snapshot_2415.get("status") != (
        "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_READY_WITH_BLOCKERS_UNRESOLVED"
    ):
        errors.append("TRADING-2415 readiness snapshot status is not ready")
    validation_section = _section(
        pit_gate_readiness_validation_2415,
        "pit_gate_readiness_validation",
    )
    if validation_section.get("valid") is not True:
        errors.append("TRADING-2415 readiness validation is not valid")
    requirements_section = _section(
        pit_gate_evidence_requirements_2416,
        "pit_gate_evidence_requirements",
    )
    if requirements_section.get("source_feature_count") not in (None, 10):
        errors.append("TRADING-2416 evidence requirements source feature count changed")
    if counts.get("pit_gate_ready_count") != 0:
        errors.append("PIT gate ready count must remain 0")
    if counts.get("contract_ready_count") != 0:
        errors.append("contract-ready count must remain 0")
    if counts.get("source_feature_count") != 10:
        errors.append("source feature count must remain 10")
    if not valid_until_evidence_ready:
        errors.append("TRADING-2418 valid-until dependency evidence is not ready")
    if not source_traceability_still_blocked:
        errors.append("growth_tilt_engine_signal_artifact blocker is not preserved")
    return {
        "schema_version": "growth_tilt_engine_pit_gate_readiness_recheck_validation.v1",
        "valid": not errors,
        "error_count": len(errors),
        "errors": errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def _matrix_rows(
    document: Mapping[str, Any],
    *,
    section_key: str = "pit_gate_readiness_matrix",
    row_key: str = "matrix_rows",
) -> list[Mapping[str, Any]]:
    return [
        _as_mapping(row)
        for row in _as_list(_section(document, section_key).get(row_key))
    ]


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if isinstance(value, Mapping):
        return value
    return document


def _pit_input_severity(
    pit_input_registry: Mapping[str, Any],
    input_id: str,
) -> str | None:
    inputs = _as_list(pit_input_registry.get("pit_inputs"))
    for row in inputs:
        item = _as_mapping(row)
        if item.get("input_id") == input_id:
            return _text(item.get("severity") or item.get("current_severity"))
    return None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return "" if value is None else str(value)
