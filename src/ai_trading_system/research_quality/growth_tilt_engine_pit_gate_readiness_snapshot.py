from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PIT_GATE_READINESS_SNAPSHOT_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_readiness_snapshot.v1"
)
PIT_GATE_READINESS_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_readiness_matrix.v1"
)
PIT_GATE_READINESS_VALIDATION_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_readiness_validation.v1"
)
REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_pit_gate_remaining_blocker_summary.v1"
)

NEXT_ROUTE = "TRADING-2416_Growth_Tilt_Engine_Remaining_Contract_Blocker_Remediation_Plan"
READY_STATUS = "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_READY_WITH_BLOCKERS_UNRESOLVED"

ALLOWED_PIT_GATE_STATUSES: tuple[str, ...] = (
    "pit_gate_ready",
    "pit_gate_blocked_by_missing_as_of_semantics",
    "pit_gate_blocked_by_missing_source_traceability",
    "pit_gate_blocked_by_missing_validity_dependency",
    "pit_gate_blocked_by_valid_until_window",
    "pit_gate_blocked_by_missing_upstream_artifact",
    "pit_gate_blocked_by_ambiguous_source_boundary",
    "pit_gate_not_applicable_non_signal_feature",
    "pit_gate_unresolved",
)

READY_SOURCE_TRACEABILITY_STATUSES: frozenset[str] = frozenset(
    {"ready", "source_traceability_remediated"}
)
SOURCE_TRACEABILITY_GATE_ACCEPTABLE_STATUSES: frozenset[str] = frozenset(
    {"ready", "source_traceability_remediated", "mapped_with_caveats"}
)
VALIDITY_GATE_ACCEPTABLE_STATUSES: frozenset[str] = frozenset(
    {"ready", "not_applicable"}
)


def build_growth_tilt_pit_gate_readiness_snapshot(
    mapping_result_2410: Mapping[str, Any],
    source_feature_contract_mapping_2410: Mapping[str, Any],
    remediation_plan_result_2411: Mapping[str, Any],
    as_of_remediation_result_2412: Mapping[str, Any],
    source_traceability_remediation_result_2413: Mapping[str, Any],
    signal_validity_dependency_remediation_result_2414: Mapping[str, Any],
) -> dict[str, Any]:
    known_rows = _known_mapping_rows(
        mapping_result_2410,
        source_feature_contract_mapping_2410,
    )
    updated_rows_2414 = _updated_mapping_rows_2414(
        signal_validity_dependency_remediation_result_2414
    )
    updated_by_feature_id = {
        str(row.get("feature_id")): row for row in updated_rows_2414
    }
    matrix_rows = [
        _matrix_row(
            known_row,
            updated_by_feature_id.get(str(known_row.get("feature_id"))),
        )
        for known_row in known_rows
    ]
    validation = validate_growth_tilt_pit_gate_readiness_matrix(
        matrix_rows=matrix_rows,
        expected_feature_count=_expected_feature_count(
            mapping_result_2410,
            source_feature_contract_mapping_2410,
            known_rows,
        ),
        prior_contract_ready_count=_int_value(
            signal_validity_dependency_remediation_result_2414.get(
                "contract_ready_count"
            )
        ),
    )
    counts = _counts(matrix_rows)
    matrix = {
        "schema_version": PIT_GATE_READINESS_MATRIX_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_tasks": [
            "TRADING-2410",
            "TRADING-2411",
            "TRADING-2412",
            "TRADING-2413",
            "TRADING-2414",
        ],
        "row_count": len(matrix_rows),
        "matrix_rows": matrix_rows,
        "allowed_pit_gate_statuses": list(ALLOWED_PIT_GATE_STATUSES),
        "production_effect": "none",
        "broker_action": "none",
    }
    remaining_summary = _remaining_blocker_summary(
        matrix_rows,
        counts=counts,
        remediation_plan_result_2411=remediation_plan_result_2411,
        as_of_remediation_result_2412=as_of_remediation_result_2412,
        source_traceability_remediation_result_2413=(
            source_traceability_remediation_result_2413
        ),
        signal_validity_dependency_remediation_result_2414=(
            signal_validity_dependency_remediation_result_2414
        ),
    )
    return {
        "schema_version": PIT_GATE_READINESS_SNAPSHOT_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_tasks": [
            "TRADING-2410",
            "TRADING-2411",
            "TRADING-2412",
            "TRADING-2413",
            "TRADING-2414",
        ],
        "source_status_2410": mapping_result_2410.get("status"),
        "source_status_2411": remediation_plan_result_2411.get("status"),
        "source_status_2412": as_of_remediation_result_2412.get("status"),
        "source_status_2413": source_traceability_remediation_result_2413.get(
            "status"
        ),
        "source_status_2414": signal_validity_dependency_remediation_result_2414.get(
            "status"
        ),
        "pit_gate_readiness_matrix": matrix,
        "pit_gate_readiness_validation": validation,
        "remaining_blocker_summary": remaining_summary,
        "pit_gate_readiness_snapshot_completed": validation["valid"],
        "source_feature_count": counts["source_feature_count"],
        "as_of_ready_count": counts["as_of_ready_count"],
        "source_traceability_ready_count": counts[
            "source_traceability_ready_count"
        ],
        "validity_dependency_ready_count": counts[
            "validity_dependency_ready_count"
        ],
        "pit_gate_ready_count": counts["pit_gate_ready_count"],
        "contract_ready_count": counts["contract_ready_count"],
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "contract_ready_not_increased": validation["contract_ready_not_increased"],
        "growth_tilt_engine_blocker_resolved": False,
        "growth_tilt_engine_blocker_downgraded": False,
        "valid_until_window_blocker_resolved": False,
        "valid_until_window_blocker_downgraded": False,
        "candidate_search_enabled": False,
        "observation_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "recommended_next_research_task": NEXT_ROUTE,
    }


def validate_growth_tilt_pit_gate_readiness_matrix(
    *,
    matrix_rows: list[Mapping[str, Any]],
    expected_feature_count: int,
    prior_contract_ready_count: int,
) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    feature_ids = [str(row.get("source_feature_id")) for row in matrix_rows]
    duplicate_ids = sorted(
        {feature_id for feature_id in feature_ids if feature_ids.count(feature_id) > 1}
    )
    if len(matrix_rows) != expected_feature_count:
        errors.append(
            _error(
                "matrix",
                "feature_count_mismatch",
                "PIT gate readiness matrix must cover every known source feature.",
            )
        )
    for feature_id in duplicate_ids:
        errors.append(
            _error(
                feature_id,
                "duplicate_source_feature_id",
                "PIT gate readiness matrix contains duplicate source feature id.",
            )
        )

    for row in matrix_rows:
        feature_id = str(row.get("source_feature_id"))
        status = str(row.get("pit_gate_status"))
        if status not in ALLOWED_PIT_GATE_STATUSES:
            errors.append(
                _error(
                    feature_id,
                    "unclassified_pit_gate_status",
                    "PIT gate status must use the allowed taxonomy.",
                )
            )
        if (
            row.get("valid_until_required") is True
            and row.get("valid_until_available") is not True
            and status != "pit_gate_blocked_by_valid_until_window"
        ):
            errors.append(
                _error(
                    feature_id,
                    "valid_until_window_not_blocking",
                    "Missing valid_until must block PIT gate readiness.",
                )
            )
        if row.get("contract_ready") is True and not _row_contract_ready(row):
            errors.append(
                _error(
                    feature_id,
                    "invalid_contract_ready_true",
                    "contract_ready true requires every PIT gate contract dimension ready.",
                )
            )
        for field in (
            "eligible_for_candidate_search",
            "eligible_for_observation",
            "eligible_for_paper_shadow",
            "eligible_for_production",
        ):
            if row.get(field) is True:
                errors.append(
                    _error(
                        feature_id,
                        "eligibility_enabled",
                        f"{field} must remain false in TRADING-2415.",
                    )
                )

    contract_ready_count = sum(1 for row in matrix_rows if row.get("contract_ready") is True)
    contract_ready_not_increased = contract_ready_count <= prior_contract_ready_count
    if not contract_ready_not_increased:
        errors.append(
            _error(
                "matrix",
                "contract_ready_increased",
                "TRADING-2415 must not increase contract_ready_count.",
            )
        )

    return {
        "schema_version": PIT_GATE_READINESS_VALIDATION_SCHEMA_VERSION,
        "valid": not errors,
        "error_count": len(errors),
        "errors": errors,
        "expected_feature_count": expected_feature_count,
        "observed_feature_count": len(matrix_rows),
        "duplicate_source_feature_count": len(duplicate_ids),
        "pit_gate_status_unclassified_count": sum(
            1
            for row in matrix_rows
            if str(row.get("pit_gate_status")) not in ALLOWED_PIT_GATE_STATUSES
        ),
        "contract_ready_count": contract_ready_count,
        "prior_contract_ready_count": prior_contract_ready_count,
        "contract_ready_not_increased": contract_ready_not_increased,
        "candidate_observation_paper_shadow_production_disabled": all(
            row.get("eligible_for_candidate_search") is False
            and row.get("eligible_for_observation") is False
            and row.get("eligible_for_paper_shadow") is False
            and row.get("eligible_for_production") is False
            for row in matrix_rows
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _matrix_row(
    known_row: Mapping[str, Any],
    updated_row: Mapping[str, Any] | None,
) -> dict[str, Any]:
    row = {**dict(known_row), **dict(_as_mapping(updated_row))}
    metadata = _as_mapping(row.get("signal_validity_dependency_contract_metadata"))
    feature_id = str(row.get("feature_id") or metadata.get("source_feature_id"))
    as_of_status = _as_of_status(row, metadata)
    source_status = _source_traceability_status(row, metadata)
    validity_status = _validity_dependency_status(row, metadata)
    valid_until_required = bool(metadata.get("valid_until_required", False))
    valid_until_available = bool(metadata.get("valid_until_available", False))
    pit_gate_status = _pit_gate_status(
        row,
        as_of_status=as_of_status,
        source_traceability_status=source_status,
        validity_dependency_status=validity_status,
        valid_until_required=valid_until_required,
        valid_until_available=valid_until_available,
    )
    matrix_row = {
        "source_feature_id": feature_id,
        "source_feature_name": (
            row.get("feature_name")
            or row.get("source_feature_name")
            or metadata.get("source_feature_name")
            or feature_id
        ),
        "as_of_semantics_status": as_of_status,
        "source_traceability_status": source_status,
        "validity_dependency_status": validity_status,
        "valid_until_required": valid_until_required,
        "valid_until_available": valid_until_available,
        "pit_gate_status": pit_gate_status,
        "pit_gate_blocking_reason": _pit_gate_blocking_reason(pit_gate_status),
        "contract_ready": False,
        "eligible_for_candidate_search": False,
        "eligible_for_observation": False,
        "eligible_for_paper_shadow": False,
        "eligible_for_production": False,
        "source_feature_type": row.get("feature_type"),
        "source_system": row.get("source_system"),
        "mapping_status": row.get("mapping_status"),
        "upstream_artifact_or_registry_reference": row.get(
            "upstream_artifact_or_registry_reference"
        ),
        "source_pit_gate_status_before_2415": row.get("pit_gate_status"),
        "source_validity_dependency_remediation_status": row.get(
            "validity_dependency_remediation_status"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }
    matrix_row["contract_ready"] = _row_contract_ready(matrix_row)
    return matrix_row


def _pit_gate_status(
    row: Mapping[str, Any],
    *,
    as_of_status: str,
    source_traceability_status: str,
    validity_dependency_status: str,
    valid_until_required: bool,
    valid_until_available: bool,
) -> str:
    if valid_until_required and not valid_until_available:
        return "pit_gate_blocked_by_valid_until_window"
    if _non_signal_feature(row):
        return "pit_gate_not_applicable_non_signal_feature"
    if not _source_traceability_gate_acceptable(source_traceability_status):
        if _ambiguous_source_boundary(row):
            return "pit_gate_blocked_by_ambiguous_source_boundary"
        if _missing_upstream_artifact(row):
            return "pit_gate_blocked_by_missing_upstream_artifact"
        return "pit_gate_blocked_by_missing_source_traceability"
    if as_of_status != "ready":
        return "pit_gate_blocked_by_missing_as_of_semantics"
    if not _validity_dependency_gate_acceptable(validity_dependency_status, row):
        if _missing_upstream_artifact(row):
            return "pit_gate_blocked_by_missing_upstream_artifact"
        return "pit_gate_blocked_by_missing_validity_dependency"
    source_pit_status = str(row.get("pit_gate_status") or "").lower()
    if source_pit_status in {"ready", "pit_gate_ready"}:
        return "pit_gate_ready"
    return "pit_gate_unresolved"


def _pit_gate_blocking_reason(status: str) -> str | None:
    return {
        "pit_gate_ready": None,
        "pit_gate_blocked_by_missing_as_of_semantics": "as_of_semantics_unresolved",
        "pit_gate_blocked_by_missing_source_traceability": (
            "source_traceability_unresolved"
        ),
        "pit_gate_blocked_by_missing_validity_dependency": (
            "validity_dependency_unresolved"
        ),
        "pit_gate_blocked_by_valid_until_window": "valid_until_window_unresolved",
        "pit_gate_blocked_by_missing_upstream_artifact": (
            "upstream_artifact_or_source_snapshot_unresolved"
        ),
        "pit_gate_blocked_by_ambiguous_source_boundary": (
            "ambiguous_source_boundary_requires_owner_review"
        ),
        "pit_gate_not_applicable_non_signal_feature": (
            "non_signal_feature_excluded_from_pit_gate_readiness"
        ),
        "pit_gate_unresolved": "pit_gate_evidence_unresolved_after_2415_snapshot",
    }[status]


def _counts(matrix_rows: list[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "source_feature_count": len(matrix_rows),
        "as_of_ready_count": sum(
            1 for row in matrix_rows if row.get("as_of_semantics_status") == "ready"
        ),
        "source_traceability_ready_count": sum(
            1
            for row in matrix_rows
            if row.get("source_traceability_status") == "ready"
        ),
        "validity_dependency_ready_count": sum(
            1
            for row in matrix_rows
            if row.get("validity_dependency_status") == "ready"
        ),
        "pit_gate_ready_count": sum(
            1 for row in matrix_rows if row.get("pit_gate_status") == "pit_gate_ready"
        ),
        "contract_ready_count": sum(
            1 for row in matrix_rows if row.get("contract_ready") is True
        ),
        "pit_gate_blocked_count": sum(
            1
            for row in matrix_rows
            if row.get("pit_gate_status")
            not in {"pit_gate_ready", "pit_gate_not_applicable_non_signal_feature"}
        ),
        "blocked_by_source_traceability_count": sum(
            1
            for row in matrix_rows
            if row.get("pit_gate_status")
            in {
                "pit_gate_blocked_by_missing_source_traceability",
                "pit_gate_blocked_by_missing_upstream_artifact",
            }
        ),
        "blocked_by_valid_until_window_count": sum(
            1
            for row in matrix_rows
            if row.get("pit_gate_status") == "pit_gate_blocked_by_valid_until_window"
        ),
    }


def _remaining_blocker_summary(
    matrix_rows: list[Mapping[str, Any]],
    *,
    counts: Mapping[str, int],
    remediation_plan_result_2411: Mapping[str, Any],
    as_of_remediation_result_2412: Mapping[str, Any],
    source_traceability_remediation_result_2413: Mapping[str, Any],
    signal_validity_dependency_remediation_result_2414: Mapping[str, Any],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for row in matrix_rows:
        status = str(row.get("pit_gate_status"))
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "schema_version": REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_feature_count": counts["source_feature_count"],
        "input_gap_count": _int_value(
            signal_validity_dependency_remediation_result_2414.get("input_gap_count")
            or source_traceability_remediation_result_2413.get("input_gap_count")
            or as_of_remediation_result_2412.get("input_gap_count")
            or remediation_plan_result_2411.get("gap_count")
        ),
        "pit_gate_status_counts": status_counts,
        "pit_gate_ready_count": counts["pit_gate_ready_count"],
        "contract_ready_count": counts["contract_ready_count"],
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "remaining_blocking_reasons": [
            "as_of_semantics_gaps_remain_after_2412",
            "source_traceability_gaps_remain_after_2413",
            "valid_until_window_blocker_not_remediated_in_2414",
            "pit_gate_evidence_not_completed_before_2415",
            "growth_tilt_engine_blocker_not_downgraded_in_2415",
        ],
        "growth_tilt_engine_blocker_resolved": False,
        "growth_tilt_engine_blocker_downgraded": False,
        "valid_until_window_blocker_resolved": False,
        "valid_until_window_blocker_downgraded": False,
        "candidate_search_enabled": False,
        "observation_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "recommended_next_task": NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _known_mapping_rows(
    mapping_result_2410: Mapping[str, Any],
    source_feature_contract_mapping_2410: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    direct = _mapping_rows_from_document(source_feature_contract_mapping_2410)
    if direct:
        return direct
    return _mapping_rows_from_document(mapping_result_2410)


def _mapping_rows_from_document(document: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct_rows = _as_list(document.get("mapping_rows"))
    if direct_rows:
        return [_as_mapping(row) for row in direct_rows]
    for key in (
        "source_feature_contract_mapping",
        "updated_source_feature_mapping",
        "pit_gate_readiness_matrix",
    ):
        section = _as_mapping(document.get(key))
        rows = _as_list(section.get("mapping_rows") or section.get("matrix_rows"))
        if rows:
            return [_as_mapping(row) for row in rows]
    return []


def _updated_mapping_rows_2414(document: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = _mapping_rows_from_document(document)
    if rows:
        return rows
    nested = _as_mapping(document.get("signal_validity_dependency_remediation"))
    return _mapping_rows_from_document(nested)


def _expected_feature_count(
    mapping_result_2410: Mapping[str, Any],
    source_feature_contract_mapping_2410: Mapping[str, Any],
    known_rows: list[Mapping[str, Any]],
) -> int:
    for document in (mapping_result_2410, source_feature_contract_mapping_2410):
        for key in ("known_source_feature_count", "source_feature_count"):
            value = document.get(key)
            if value is not None:
                return _int_value(value)
        section = _as_mapping(document.get("source_feature_contract_mapping"))
        value = section.get("known_source_feature_count")
        if value is not None:
            return _int_value(value)
    return len(known_rows)


def _as_of_status(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    return _status_value(
        row.get("as_of_semantics_status"),
        metadata.get("as_of_semantics_status"),
        default="not_ready",
    )


def _source_traceability_status(
    row: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> str:
    statuses = [
        row.get("source_traceability_status"),
        metadata.get("source_traceability_status"),
        row.get("traceability_status"),
        row.get("source_traceability_remediation_status"),
    ]
    if any(str(status).lower() in READY_SOURCE_TRACEABILITY_STATUSES for status in statuses):
        return "ready"
    return _status_value(*statuses, default="not_ready")


def _validity_dependency_status(
    row: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> str:
    if metadata:
        return _status_value(
            metadata.get("validity_dependency_status"),
            row.get("validity_dependency_status"),
            default="not_ready",
        )
    if str(row.get("validity_dependency", "")).lower() == "none_identified_in_2410":
        return "not_applicable"
    return _status_value(row.get("validity_dependency_status"), default="not_ready")


def _status_value(*values: Any, default: str) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _source_traceability_gate_acceptable(status: str) -> bool:
    return status.lower() in SOURCE_TRACEABILITY_GATE_ACCEPTABLE_STATUSES


def _validity_dependency_gate_acceptable(
    status: str,
    row: Mapping[str, Any],
) -> bool:
    if status.lower() in VALIDITY_GATE_ACCEPTABLE_STATUSES:
        return True
    return str(row.get("validity_dependency", "")).lower() == "none_identified_in_2410"


def _row_contract_ready(row: Mapping[str, Any]) -> bool:
    return (
        row.get("as_of_semantics_status") == "ready"
        and row.get("source_traceability_status") == "ready"
        and row.get("validity_dependency_status") == "ready"
        and row.get("pit_gate_status") == "pit_gate_ready"
        and (
            row.get("valid_until_required") is not True
            or row.get("valid_until_available") is True
        )
    )


def _non_signal_feature(row: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(value)
        for value in (
            row.get("feature_type"),
            row.get("mapping_status"),
            row.get("contract_scope"),
        )
        if value
    ).lower()
    return "non_signal" in text or "excluded_non_signal_feature" in text


def _ambiguous_source_boundary(row: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(value)
        for value in (
            row.get("mapping_status"),
            row.get("feature_type"),
            row.get("source_system"),
            " ".join(str(item) for item in _as_list(row.get("mapping_status_reasons"))),
        )
        if value
    ).lower()
    return "ambiguous" in text


def _missing_upstream_artifact(row: Mapping[str, Any]) -> bool:
    source_system = str(row.get("source_system", "")).lower()
    reference = str(row.get("upstream_artifact_or_registry_reference", "")).lower()
    return source_system == "missing_artifact" or "missing_artifact" in reference


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _error(feature_id: str, code: str, message: str) -> dict[str, str]:
    return {"feature_id": feature_id, "code": code, "message": message}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
