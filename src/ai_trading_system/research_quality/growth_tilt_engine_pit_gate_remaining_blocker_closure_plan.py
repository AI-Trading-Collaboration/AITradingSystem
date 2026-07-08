from __future__ import annotations

from collections.abc import Mapping
from typing import Any

CLOSURE_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan.v1"
)
REMAINING_BLOCKER_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_remaining_blocker_matrix.v1"
)
SOURCE_TRACEABILITY_CLOSURE_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_closure_plan.v1"
)
AS_OF_EVIDENCE_CLOSURE_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_as_of_evidence_closure_plan.v1"
)
VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_valid_until_dependency_closure_plan.v1"
)
PIT_GATE_EVIDENCE_REQUIREMENTS_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_evidence_requirements.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY"
NEXT_ROUTE = (
    "TRADING-2417_Growth_Tilt_Engine_Source_Traceability_And_Upstream_Artifact_Closure"
)
VALID_UNTIL_ROUTE = "TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure"
READINESS_RECHECK_ROUTE = "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck"
OWNER_REVIEW_ROUTE = "TRADING-2420_Growth_Tilt_Engine_PIT_Blocker_Downgrade_Owner_Review"

AS_OF_EVIDENCE_REQUIRED_FIELDS: tuple[str, ...] = (
    "as_of_date",
    "generated_at",
    "source_data_cutoff",
    "signal_version",
    "feature_version",
    "source_artifact_id",
    "source_config_id",
    "source_feature_ids",
)

PIT_GATE_READY_REQUIRES: tuple[str, ...] = (
    "source_traceability_complete",
    "as_of_contract_fields_complete",
    "source_data_cutoff_available",
    "no_unexplained_forward_window_dependency",
    "upstream_artifact_available",
    "feature_version_available",
    "signal_or_feature_owner_module_known",
    "valid_until_dependency_resolved_if_applicable",
    "PIT_gate_checker_regenerated",
    "owner_review_recorded_if_downgrade_required",
)


def build_growth_tilt_pit_gate_remaining_blocker_closure_plan(
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    pit_gate_readiness_matrix_2415: Mapping[str, Any],
    remaining_blocker_summary_2415: Mapping[str, Any],
    *,
    pit_input_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    matrix = _matrix_payload(
        pit_gate_readiness_snapshot_2415,
        pit_gate_readiness_matrix_2415,
    )
    source_rows = [_as_mapping(row) for row in _as_list(matrix.get("matrix_rows"))]
    blocker_rows = [_remaining_blocker_matrix_row(row) for row in source_rows]
    counts = _counts(blocker_rows, pit_gate_readiness_snapshot_2415)
    remaining_blocker_matrix = {
        "schema_version": REMAINING_BLOCKER_MATRIX_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_tasks": [
            "TRADING-2410",
            "TRADING-2411",
            "TRADING-2412",
            "TRADING-2413",
            "TRADING-2414",
            "TRADING-2415",
        ],
        "row_count": len(blocker_rows),
        "matrix_rows": blocker_rows,
        "production_effect": "none",
        "broker_action": "none",
    }
    source_traceability_closure_plan = _source_traceability_closure_plan(blocker_rows)
    as_of_evidence_closure_plan = _as_of_evidence_closure_plan(blocker_rows)
    valid_until_dependency_closure_plan = _valid_until_dependency_closure_plan(
        blocker_rows,
        pit_input_registry=pit_input_registry or {},
    )
    pit_gate_evidence_requirements = _pit_gate_evidence_requirements()
    validation = _validation(
        blocker_rows,
        counts=counts,
        snapshot=pit_gate_readiness_snapshot_2415,
        remaining_blocker_summary=remaining_blocker_summary_2415,
    )
    return {
        "schema_version": CLOSURE_PLAN_SCHEMA_VERSION,
        "task_id": "TRADING-2416",
        "status": READY_STATUS,
        "engine_id": "growth_tilt_engine",
        "source_tasks": [
            "TRADING-2410",
            "TRADING-2411",
            "TRADING-2412",
            "TRADING-2413",
            "TRADING-2414",
            "TRADING-2415",
        ],
        "current_readiness_from_2415": {
            "source_feature_count": counts["source_feature_count"],
            "pit_gate_ready_count": counts["pit_gate_ready_count"],
            "contract_ready_count": counts["contract_ready_count"],
            "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
            "blocked_by_source_traceability_count": counts[
                "blocked_by_source_traceability_count"
            ],
            "blocked_by_valid_until_window_count": counts[
                "blocked_by_valid_until_window_count"
            ],
            "blockers_unresolved": True,
        },
        "readiness_interpretation": {
            "pit_gate_ready": False,
            "contract_ready": False,
            "blocker_downgrade_allowed": False,
            "candidate_search_allowed": False,
            "observation_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "remaining_blocker_matrix": remaining_blocker_matrix,
        "source_traceability_closure_plan": source_traceability_closure_plan,
        "as_of_evidence_closure_plan": as_of_evidence_closure_plan,
        "valid_until_dependency_closure_plan": valid_until_dependency_closure_plan,
        "pit_gate_evidence_requirements": pit_gate_evidence_requirements,
        "closure_priority": _closure_priority(),
        "proposed_sequence": _proposed_sequence(),
        "closure_plan_validation": validation,
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": counts["pit_gate_ready_count"],
        "contract_ready_count": counts["contract_ready_count"],
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "as_of_contract_gap_count": counts["as_of_contract_gap_count"],
        "upstream_artifact_gap_count": counts["upstream_artifact_gap_count"],
        "signal_validity_dependency_gap_count": counts[
            "signal_validity_dependency_gap_count"
        ],
        "remaining_blocker_matrix_ready": validation["valid"],
        "source_traceability_closure_plan_ready": validation["valid"],
        "as_of_evidence_closure_plan_ready": validation["valid"],
        "valid_until_dependency_closure_plan_ready": validation["valid"],
        "pit_gate_evidence_requirements_ready": validation["valid"],
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _remaining_blocker_matrix_row(row: Mapping[str, Any]) -> dict[str, Any]:
    feature_id = _text(row.get("source_feature_id"))
    pit_gate_status = _text(row.get("pit_gate_status"))
    blocked_by_as_of = _text(row.get("as_of_semantics_status")) != "ready"
    blocked_by_upstream = _blocked_by_upstream_artifact(row)
    blocked_by_valid_until = _blocked_by_valid_until(row)
    blocked_by_source_traceability = _blocked_by_source_traceability(row)
    blocked_by_signal_validity = _blocked_by_signal_validity(row)
    blocked_by_missing_evidence = pit_gate_status != "pit_gate_ready"
    categories = _blocker_categories(
        blocked_by_as_of=blocked_by_as_of,
        blocked_by_source_traceability=blocked_by_source_traceability,
        blocked_by_upstream=blocked_by_upstream,
        blocked_by_valid_until=blocked_by_valid_until,
        blocked_by_signal_validity=blocked_by_signal_validity,
        blocked_by_missing_evidence=blocked_by_missing_evidence,
    )
    return {
        "feature_id": feature_id,
        "feature_family": _feature_family(row),
        "current_pit_gate_status": pit_gate_status,
        "current_contract_status": (
            "ready" if row.get("contract_ready") is True else "blocked"
        ),
        "blocked_by_as_of_contract": blocked_by_as_of,
        "blocked_by_source_traceability": blocked_by_source_traceability,
        "blocked_by_upstream_artifact": blocked_by_upstream,
        "blocked_by_valid_until_window": blocked_by_valid_until,
        "blocked_by_signal_validity_dependency": blocked_by_signal_validity,
        "blocked_by_missing_evidence": blocked_by_missing_evidence,
        "blocker_categories": categories,
        "severity": "BLOCKING",
        "required_closure_evidence": _required_closure_evidence(
            blocked_by_as_of=blocked_by_as_of,
            blocked_by_source_traceability=blocked_by_source_traceability,
            blocked_by_upstream=blocked_by_upstream,
            blocked_by_valid_until=blocked_by_valid_until,
            blocked_by_signal_validity=blocked_by_signal_validity,
        ),
        "recommended_closure_task": _recommended_closure_task(
            blocked_by_source_traceability=blocked_by_source_traceability,
            blocked_by_upstream=blocked_by_upstream,
            blocked_by_valid_until=blocked_by_valid_until,
            blocked_by_as_of=blocked_by_as_of,
            blocked_by_signal_validity=blocked_by_signal_validity,
        ),
        "owner_review_required": True,
        "source_traceability_status": row.get("source_traceability_status"),
        "as_of_semantics_status": row.get("as_of_semantics_status"),
        "validity_dependency_status": row.get("validity_dependency_status"),
        "valid_until_required": bool(row.get("valid_until_required")),
        "valid_until_available": bool(row.get("valid_until_available")),
        "pit_gate_blocking_reason": row.get("pit_gate_blocking_reason"),
        "source_system": row.get("source_system"),
        "upstream_artifact_or_registry_reference": row.get(
            "upstream_artifact_or_registry_reference"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_traceability_closure_plan(
    blocker_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in blocker_rows:
        if row.get("blocked_by_source_traceability") is not True:
            continue
        source_system = _text(row.get("source_system"))
        reference = _text(row.get("upstream_artifact_or_registry_reference")).lower()
        missing_source_config = source_system in {"", "missing_artifact"} or (
            "missing" in reference
        )
        missing_source_artifact = source_system in {
            "derived_research_artifact",
            "missing_artifact",
        } or ("missing" in reference)
        missing_owner_module = source_system not in {
            "governed_config",
            "cached_data_artifact",
        }
        missing_source_data_cutoff = source_system != "governed_config"
        rows.append(
            {
                "feature_id": row.get("feature_id"),
                "missing_source_config": missing_source_config,
                "missing_source_artifact": missing_source_artifact,
                "missing_owner_module": missing_owner_module,
                "missing_generated_at": True,
                "missing_source_data_cutoff": missing_source_data_cutoff,
                "missing_feature_version": True,
                "required_fix": (
                    "Bind the source feature to a governed source config or upstream "
                    "artifact with generated_at, cutoff, owner module, version and "
                    "checksum evidence."
                ),
                "expected_evidence_after_fix": [
                    "source_config_id_or_source_artifact_id",
                    "owner_module",
                    "generated_at",
                    "source_data_cutoff_if_data_derived",
                    "feature_version",
                    "source_feature_ids",
                    "artifact_row_count_and_checksum_where_practical",
                ],
                "recommended_task": NEXT_ROUTE,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return {
        "schema_version": SOURCE_TRACEABILITY_CLOSURE_PLAN_SCHEMA_VERSION,
        "source_traceability_gap_count": len(rows),
        "closure_rows": rows,
        "recommended_next_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _as_of_evidence_closure_plan(
    blocker_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in blocker_rows:
        if row.get("blocked_by_as_of_contract") is not True:
            continue
        missing_fields = _missing_as_of_fields(row)
        rows.append(
            {
                "feature_id": row.get("feature_id"),
                "current_as_of_semantics_status": row.get("as_of_semantics_status"),
                "missing_required_fields": missing_fields,
                "requires_source_traceability_before_recheck": row.get(
                    "blocked_by_source_traceability"
                )
                is True,
                "requires_valid_until_before_recheck": row.get(
                    "blocked_by_valid_until_window"
                )
                is True,
                "recommended_next_task": _recommended_closure_task(
                    blocked_by_source_traceability=bool(
                        row.get("blocked_by_source_traceability")
                    ),
                    blocked_by_upstream=bool(row.get("blocked_by_upstream_artifact")),
                    blocked_by_valid_until=bool(
                        row.get("blocked_by_valid_until_window")
                    ),
                    blocked_by_as_of=True,
                    blocked_by_signal_validity=bool(
                        row.get("blocked_by_signal_validity_dependency")
                    ),
                ),
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return {
        "schema_version": AS_OF_EVIDENCE_CLOSURE_PLAN_SCHEMA_VERSION,
        "required_fields": list(AS_OF_EVIDENCE_REQUIRED_FIELDS),
        "as_of_contract_gap_count": len(rows),
        "required_fields_missing_count": sum(
            len(_as_list(row.get("missing_required_fields"))) for row in rows
        ),
        "closure_rows": rows,
        "recommended_next_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _valid_until_dependency_closure_plan(
    blocker_rows: list[Mapping[str, Any]],
    *,
    pit_input_registry: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [
        row
        for row in blocker_rows
        if row.get("blocked_by_valid_until_window") is True
    ]
    registry_entry = _pit_registry_entry(pit_input_registry, "valid_until_window")
    return {
        "schema_version": VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_SCHEMA_VERSION,
        "valid_until_window_still_blocking": True,
        "dependent_feature_or_signal_count": len(rows),
        "dependent_feature_ids": [row.get("feature_id") for row in rows],
        "requires_signal_validity_contract_evidence": True,
        "requires_valid_from_valid_until_mapping": True,
        "requires_stale_signal_policy_evidence": True,
        "pit_input_registry_severity": registry_entry.get("severity"),
        "pit_input_registry_candidate_search_blocker": registry_entry.get(
            "candidate_search_blocker"
        ),
        "recommended_later_task": VALID_UNTIL_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _pit_gate_evidence_requirements() -> dict[str, Any]:
    return {
        "schema_version": PIT_GATE_EVIDENCE_REQUIREMENTS_SCHEMA_VERSION,
        "pit_gate_ready_requires": list(PIT_GATE_READY_REQUIRES),
        "blocker_downgrade_requires_owner_review": True,
        "auto_downgrade_allowed": False,
        "candidate_search_allowed_before_recheck": False,
        "recommended_recheck_task": READINESS_RECHECK_ROUTE,
        "recommended_owner_review_task": OWNER_REVIEW_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _closure_priority() -> dict[str, list[str]]:
    return {
        "P0": [
            "source_traceability_remaining_gaps",
            "upstream_artifact_gaps",
            "as_of_contract_evidence_gaps",
        ],
        "P1": [
            "valid_until_window_dependency_alignment",
            "signal_validity_dependency_evidence",
            "PIT_gate_evidence_normalization",
        ],
        "P2": [
            "owner_review_preparation",
            "blocker_downgrade_precheck",
        ],
    }


def _proposed_sequence() -> dict[str, dict[str, Any]]:
    return {
        "TRADING-2417": {
            "title": "Growth Tilt Engine Source Traceability And Upstream Artifact Closure",
            "purpose": "close remaining source traceability / upstream artifact gaps",
            "does_not": ["downgrade_blocker", "resume_candidate_search"],
        },
        "TRADING-2418": {
            "title": "Valid Until Window Dependency Evidence Closure",
            "purpose": "close valid_until_window-dependent readiness gaps",
            "does_not": ["approve_observation"],
        },
        "TRADING-2419": {
            "title": "Growth Tilt Engine PIT Gate Readiness Recheck",
            "purpose": "regenerate readiness snapshot after closure evidence",
            "does_not": ["auto_downgrade"],
        },
        "TRADING-2420": {
            "title": "Growth Tilt Engine PIT Blocker Downgrade Owner Review",
            "purpose": "owner review before any severity downgrade",
            "does_not": ["approve_paper_shadow"],
        },
    }


def _validation(
    blocker_rows: list[Mapping[str, Any]],
    *,
    counts: Mapping[str, int],
    snapshot: Mapping[str, Any],
    remaining_blocker_summary: Mapping[str, Any],
) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    if counts["source_feature_count"] != 10:
        errors.append(_error("matrix", "source_feature_count_not_10"))
    if counts["pit_gate_ready_count"] != 0:
        errors.append(_error("matrix", "pit_gate_ready_count_not_zero"))
    if counts["contract_ready_count"] != 0:
        errors.append(_error("matrix", "contract_ready_count_not_zero"))
    if counts["pit_gate_blocked_count"] != 10:
        errors.append(_error("matrix", "pit_gate_blocked_count_not_10"))
    if counts["blocked_by_source_traceability_count"] != 5:
        errors.append(_error("matrix", "source_traceability_blocker_count_not_5"))
    if counts["blocked_by_valid_until_window_count"] != 1:
        errors.append(_error("matrix", "valid_until_blocker_count_not_1"))
    if _int(snapshot.get("pit_gate_ready_count")) != 0:
        errors.append(_error("snapshot", "source_snapshot_marked_pit_ready"))
    summary = _section(remaining_blocker_summary, "remaining_blocker_summary")
    if summary and summary.get("growth_tilt_engine_blocker_resolved") is not False:
        errors.append(_error("summary", "growth_tilt_blocker_resolved_upstream"))
    if summary and summary.get("valid_until_window_blocker_resolved") is not False:
        errors.append(_error("summary", "valid_until_blocker_resolved_upstream"))
    for row in blocker_rows:
        if row.get("current_contract_status") == "ready":
            errors.append(_error(_text(row.get("feature_id")), "contract_ready_row"))
    return {
        "valid": not errors,
        "error_count": len(errors),
        "errors": errors,
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": counts["pit_gate_ready_count"],
        "contract_ready_count": counts["contract_ready_count"],
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _counts(
    blocker_rows: list[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
) -> dict[str, int]:
    return {
        "source_feature_count": len(blocker_rows),
        "pit_gate_ready_count": _count_or_snapshot(
            blocker_rows,
            "pit_gate_ready_count",
            snapshot,
            lambda row: row.get("current_pit_gate_status") == "pit_gate_ready",
        ),
        "contract_ready_count": _count_or_snapshot(
            blocker_rows,
            "contract_ready_count",
            snapshot,
            lambda row: row.get("current_contract_status") == "ready",
        ),
        "pit_gate_blocked_count": _count_or_snapshot(
            blocker_rows,
            "pit_gate_blocked_count",
            snapshot,
            lambda row: row.get("current_pit_gate_status")
            not in {"pit_gate_ready", "pit_gate_not_applicable_non_signal_feature"},
        ),
        "blocked_by_source_traceability_count": sum(
            1 for row in blocker_rows if row.get("blocked_by_source_traceability") is True
        ),
        "blocked_by_valid_until_window_count": sum(
            1 for row in blocker_rows if row.get("blocked_by_valid_until_window") is True
        ),
        "as_of_contract_gap_count": sum(
            1 for row in blocker_rows if row.get("blocked_by_as_of_contract") is True
        ),
        "upstream_artifact_gap_count": sum(
            1 for row in blocker_rows if row.get("blocked_by_upstream_artifact") is True
        ),
        "signal_validity_dependency_gap_count": sum(
            1
            for row in blocker_rows
            if row.get("blocked_by_signal_validity_dependency") is True
        ),
    }


def _count_or_snapshot(
    rows: list[Mapping[str, Any]],
    key: str,
    snapshot: Mapping[str, Any],
    predicate: Any,
) -> int:
    value = snapshot.get(key)
    if value is not None:
        return _int(value)
    return sum(1 for row in rows if predicate(row))


def _matrix_payload(
    snapshot: Mapping[str, Any],
    matrix_document: Mapping[str, Any],
) -> Mapping[str, Any]:
    direct = _section(matrix_document, "pit_gate_readiness_matrix")
    if direct:
        return direct
    return _as_mapping(snapshot.get("pit_gate_readiness_matrix"))


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    section = _as_mapping(document.get(key))
    return section


def _blocked_by_source_traceability(row: Mapping[str, Any]) -> bool:
    status = _text(row.get("pit_gate_status"))
    if status in {
        "pit_gate_blocked_by_missing_source_traceability",
        "pit_gate_blocked_by_missing_upstream_artifact",
    }:
        return True
    source_status = _text(row.get("source_traceability_status"))
    return source_status not in {"ready", "mapped_with_caveats"}


def _blocked_by_upstream_artifact(row: Mapping[str, Any]) -> bool:
    status = _text(row.get("pit_gate_status"))
    source_system = _text(row.get("source_system"))
    reference = _text(row.get("upstream_artifact_or_registry_reference")).lower()
    return (
        status == "pit_gate_blocked_by_missing_upstream_artifact"
        or source_system == "missing_artifact"
        or "missing standalone" in reference
    )


def _blocked_by_valid_until(row: Mapping[str, Any]) -> bool:
    return _text(row.get("pit_gate_status")) == "pit_gate_blocked_by_valid_until_window" or (
        row.get("valid_until_required") is True
        and row.get("valid_until_available") is not True
    )


def _blocked_by_signal_validity(row: Mapping[str, Any]) -> bool:
    status = _text(row.get("validity_dependency_status"))
    remediation_status = _text(row.get("source_validity_dependency_remediation_status"))
    return status not in {"ready", "not_applicable"} or remediation_status.startswith(
        "validity_dependency_blocked"
    )


def _blocker_categories(
    *,
    blocked_by_as_of: bool,
    blocked_by_source_traceability: bool,
    blocked_by_upstream: bool,
    blocked_by_valid_until: bool,
    blocked_by_signal_validity: bool,
    blocked_by_missing_evidence: bool,
) -> list[str]:
    categories: list[str] = []
    if blocked_by_as_of:
        categories.append("AS_OF_CONTRACT_GAP")
    if blocked_by_source_traceability:
        categories.append("SOURCE_TRACEABILITY_GAP")
    if blocked_by_upstream:
        categories.append("UPSTREAM_ARTIFACT_GAP")
    if blocked_by_valid_until:
        categories.append("VALID_UNTIL_DEPENDENCY_GAP")
    if blocked_by_signal_validity:
        categories.append("SIGNAL_VALIDITY_DEPENDENCY_GAP")
    if blocked_by_missing_evidence:
        categories.append("PIT_GATE_EVIDENCE_GAP")
    return categories


def _required_closure_evidence(
    *,
    blocked_by_as_of: bool,
    blocked_by_source_traceability: bool,
    blocked_by_upstream: bool,
    blocked_by_valid_until: bool,
    blocked_by_signal_validity: bool,
) -> list[str]:
    evidence: list[str] = []
    if blocked_by_as_of:
        evidence.extend(["as_of_date", "generated_at", "source_data_cutoff"])
    if blocked_by_source_traceability:
        evidence.extend(["source_config_id", "source_artifact_id", "owner_module"])
    if blocked_by_upstream:
        evidence.extend(["artifact_row_count", "artifact_checksum"])
    if blocked_by_valid_until:
        evidence.extend(["valid_from", "valid_until", "stale_signal_policy"])
    if blocked_by_signal_validity:
        evidence.extend(["signal_version", "signal_validity_contract"])
    evidence.extend(["feature_version", "PIT_gate_checker_regenerated"])
    return list(dict.fromkeys(evidence))


def _recommended_closure_task(
    *,
    blocked_by_source_traceability: bool,
    blocked_by_upstream: bool,
    blocked_by_valid_until: bool,
    blocked_by_as_of: bool,
    blocked_by_signal_validity: bool,
) -> str:
    if blocked_by_valid_until:
        return VALID_UNTIL_ROUTE
    if blocked_by_source_traceability or blocked_by_upstream or blocked_by_as_of:
        return NEXT_ROUTE
    if blocked_by_signal_validity:
        return VALID_UNTIL_ROUTE
    return READINESS_RECHECK_ROUTE


def _missing_as_of_fields(row: Mapping[str, Any]) -> list[str]:
    missing = ["as_of_date", "generated_at", "source_data_cutoff", "feature_version"]
    feature_family = _text(row.get("feature_family"))
    if "signal" in feature_family or "execution" in feature_family:
        missing.append("signal_version")
    if row.get("blocked_by_source_traceability") is True:
        missing.append("source_artifact_id")
    if _text(row.get("source_system")) == "governed_config":
        missing.append("source_config_id")
    missing.append("source_feature_ids")
    return list(dict.fromkeys(missing))


def _feature_family(row: Mapping[str, Any]) -> str:
    feature_type = _text(row.get("source_feature_type")).lower()
    return {
        "market_data": "market_data",
        "technical_features": "derived_price_feature",
        "portfolio_state": "portfolio_state",
        "signal_construction_policy": "signal_construction_policy",
        "behavior_guardrail_context": "behavior_guardrail_context",
        "execution_semantic_dependency": "execution_semantic_dependency",
        "signal_artifact_contract": "signal_artifact_contract",
    }.get(feature_type, feature_type or "unknown")


def _pit_registry_entry(
    registry: Mapping[str, Any],
    input_id: str,
) -> Mapping[str, Any]:
    for entry in _as_list(registry.get("entries")):
        row = _as_mapping(entry)
        if row.get("input_id") == input_id:
            return row
    return {}


def _error(scope: str, code: str) -> dict[str, str]:
    return {"scope": scope, "code": code}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
