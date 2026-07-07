from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_validity_dependency_remediation.v1"
)
SIGNAL_VALIDITY_DEPENDENCY_VALIDATION_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_validity_dependency_validation.v1"
)
SIGNAL_VALIDITY_DEPENDENCY_CONTRACT_METADATA_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_validity_dependency_contract_metadata.v1"
)
UPDATED_MAPPING_SCHEMA_VERSION = (
    "growth_tilt_engine_source_feature_mapping_after_signal_validity_dependency.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_validity_dependency_before_after_remediation.v1"
)
REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_validity_dependency_remaining_blocker_summary.v1"
)

ALLOWED_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_STATUSES: tuple[str, ...] = (
    "validity_dependency_remediated",
    "validity_dependency_partially_remediated",
    "validity_dependency_blocked_by_valid_until_window",
    "validity_dependency_blocked_by_missing_source_traceability",
    "validity_dependency_blocked_by_missing_upstream_artifact",
    "validity_dependency_blocked_by_ambiguous_signal_boundary",
    "validity_dependency_not_applicable_non_signal_feature",
    "validity_dependency_unresolved",
)
TARGET_REMEDIATION_CATEGORY = "validity_dependency_required"
NEXT_ROUTE = "TRADING-2415_Growth_Tilt_Engine_PIT_Gate_Readiness_Snapshot"

SOURCE_TRACEABILITY_READY_STATUSES: tuple[str, ...] = (
    "ready",
    "source_traceability_remediated",
)
SOURCE_TRACEABILITY_BLOCKED_STATUSES: tuple[str, ...] = (
    "not_ready",
    "blocked",
    "missing",
    "partial",
    "not_ready_missing_source_snapshot",
    "source_traceability_blocked_by_missing_upstream_artifact",
    "source_traceability_blocked_by_missing_registry_entry",
    "source_traceability_blocked_by_ambiguous_source_boundary",
    "source_traceability_unresolved",
)
VALID_UNTIL_REQUIRED_FEATURES: frozenset[str] = frozenset(
    {"execution_signal_validity_policy"}
)
VALIDITY_WINDOW_FEATURES: frozenset[str] = frozenset(
    {
        "execution_signal_validity_policy",
        "growth_tilt_engine_signal_artifact",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
    }
)


def build_growth_tilt_signal_validity_dependency_remediation(
    remediation_plan_result: Mapping[str, Any],
    as_of_remediation_result: Mapping[str, Any],
    source_traceability_remediation_result: Mapping[str, Any],
) -> dict[str, Any]:
    mapping_rows = _mapping_rows(source_traceability_remediation_result)
    row_by_feature_id = {str(row.get("feature_id")): row for row in mapping_rows}
    source_records = _source_traceability_records(source_traceability_remediation_result)
    source_record_by_feature_id = {
        str(record.get("feature_id")): record for record in source_records
    }
    target_items = _validity_dependency_items(
        remediation_plan_result,
        source_traceability_remediation_result,
        mapping_rows,
    )
    remediation_records = [
        build_signal_validity_dependency_remediation_record(
            item,
            row_by_feature_id.get(str(item.get("feature_id")), {}),
            source_record_by_feature_id.get(str(item.get("feature_id")), {}),
            remediation_order=index,
        )
        for index, item in enumerate(target_items, start=1)
    ]
    updated_mapping = build_updated_source_feature_mapping(
        source_traceability_remediation_result,
        mapping_rows,
        remediation_records,
    )
    validation = validate_growth_tilt_signal_validity_dependency_remediation(
        remediation_records,
        updated_mapping,
        expected_validity_dependency_gap_count=len(target_items),
    )
    before_after = build_before_after_remediation(remediation_records)
    remaining_summary = build_remaining_blocker_summary(
        source_traceability_remediation_result,
        remediation_records,
        updated_mapping,
    )
    metadata_rows = [
        record["after"]["signal_validity_dependency_contract_metadata"]
        for record in remediation_records
    ]
    return {
        "schema_version": SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_tasks": ["TRADING-2411", "TRADING-2412", "TRADING-2413"],
        "source_status_2411": remediation_plan_result.get("status"),
        "source_status_2412": as_of_remediation_result.get("status"),
        "source_status_2413": source_traceability_remediation_result.get("status"),
        "input_gap_count": source_traceability_remediation_result.get("input_gap_count")
        or as_of_remediation_result.get("input_gap_count")
        or remediation_plan_result.get("gap_count"),
        "validity_dependency_gap_count": len(target_items),
        "validity_dependency_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_remediated"
        ),
        "validity_dependency_blocked_by_valid_until_window_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_blocked_by_valid_until_window"
        ),
        "validity_dependency_blocked_by_source_traceability_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_blocked_by_missing_source_traceability"
        ),
        "remaining_validity_dependency_gap_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            != "validity_dependency_remediated"
        ),
        "remaining_blocked_or_gap_count": remaining_summary[
            "remaining_blocked_or_gap_count"
        ],
        "contract_ready_count": updated_mapping["contract_ready_count"],
        "allowed_signal_validity_dependency_remediation_statuses": list(
            ALLOWED_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_STATUSES
        ),
        "signal_validity_dependency_remediation_records": remediation_records,
        "signal_validity_dependency_contract_metadata": {
            "schema_version": SIGNAL_VALIDITY_DEPENDENCY_CONTRACT_METADATA_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "metadata_rows": metadata_rows,
            "fresh_market_data_required": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "before_after_signal_validity_dependency_remediation": before_after,
        "updated_source_feature_mapping": updated_mapping,
        "remaining_blocker_summary": remaining_summary,
        "signal_validity_dependency_remediation_validation": validation,
        "signal_validity_dependency_remediation_completed": validation["valid"],
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


def build_signal_validity_dependency_remediation_record(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
    *,
    remediation_order: int,
) -> dict[str, Any]:
    feature_id = str(remediation_item.get("feature_id") or mapping_row.get("feature_id"))
    source_feature_name = (
        remediation_item.get("source_feature_name")
        or mapping_row.get("feature_name")
        or mapping_row.get("source_feature_name")
        or feature_id
    )
    before = {
        "feature_id": feature_id,
        "source_feature_name": source_feature_name,
        "mapping_status": mapping_row.get("mapping_status"),
        "traceability_status": mapping_row.get("traceability_status"),
        "source_traceability_status": mapping_row.get("source_traceability_status"),
        "source_traceability_remediation_status": mapping_row.get(
            "source_traceability_remediation_status"
        ),
        "as_of_semantics_status": mapping_row.get("as_of_semantics_status"),
        "as_of_remediation_status": mapping_row.get("as_of_remediation_status"),
        "validity_dependency_status": mapping_row.get("validity_dependency_status"),
        "validity_dependency_remediation_status": mapping_row.get(
            "validity_dependency_remediation_status"
        ),
        "pit_gate_status": mapping_row.get("pit_gate_status"),
        "contract_ready": False,
    }
    metadata = signal_validity_dependency_contract_metadata(
        remediation_item,
        mapping_row,
        source_traceability_record,
    )
    status = metadata["validity_dependency_remediation_status"]
    validity_ready = status == "validity_dependency_remediated"
    after = {
        "feature_id": feature_id,
        "source_feature_name": source_feature_name,
        "mapping_status": "mapped_with_caveats" if validity_ready else "blocked_unresolved",
        "mapping_status_reason": _mapping_status_reason(status),
        "as_of_semantics_status": before["as_of_semantics_status"],
        "as_of_remediation_status": before["as_of_remediation_status"],
        "source_traceability_status": before["source_traceability_status"],
        "source_traceability_remediation_status": before[
            "source_traceability_remediation_status"
        ],
        "traceability_status": before["traceability_status"],
        "validity_dependency_status": metadata["validity_dependency_status"],
        "validity_dependency_remediation_status": status,
        "pit_gate_status": _pit_gate_status_after_2414(mapping_row),
        "pit_safe": mapping_row.get("pit_safe") or "unknown",
        "contract_ready": False,
        "contract_ready_blocking_dimensions": _contract_ready_blocking_dimensions(
            mapping_row,
            metadata,
        ),
        "signal_validity_dependency_contract_metadata": metadata,
    }
    return {
        "remediation_order": remediation_order,
        "source_remediation_order_2411": remediation_item.get("remediation_order"),
        "source_traceability_remediation_order_2413": source_traceability_record.get(
            "remediation_order"
        ),
        "feature_id": feature_id,
        "validity_dependency_remediation_status": status,
        "validity_dependency_status_before": before.get("validity_dependency_status"),
        "validity_dependency_status_after": after["validity_dependency_status"],
        "as_of_semantics_status_before": before.get("as_of_semantics_status"),
        "as_of_semantics_status_after": after.get("as_of_semantics_status"),
        "source_traceability_status_before": before.get("source_traceability_status")
        or before.get("traceability_status"),
        "source_traceability_status_after": after.get("source_traceability_status")
        or after.get("traceability_status"),
        "contract_ready": False,
        "before": before,
        "after": after,
        "production_effect": "none",
        "broker_action": "none",
    }


def signal_validity_dependency_contract_metadata(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
) -> dict[str, Any]:
    feature_id = str(remediation_item.get("feature_id") or mapping_row.get("feature_id"))
    source_ready = _source_traceability_ready(mapping_row, source_traceability_record)
    valid_until_required = _valid_until_required(feature_id, remediation_item, mapping_row)
    validity_window_required = _validity_window_required(
        feature_id,
        remediation_item,
        mapping_row,
    )

    if _non_signal_feature(mapping_row):
        status = "validity_dependency_not_applicable_non_signal_feature"
        contract_status = "not_applicable"
        blocking_reason = "non_signal_feature_excluded_from_signal_validity_dependency"
    elif _ambiguous_signal_boundary(remediation_item, mapping_row):
        status = "validity_dependency_blocked_by_ambiguous_signal_boundary"
        contract_status = "blocked"
        blocking_reason = "ambiguous_signal_boundary_requires_owner_review"
    elif not source_ready and feature_id not in VALID_UNTIL_REQUIRED_FEATURES:
        status = "validity_dependency_blocked_by_missing_source_traceability"
        contract_status = "blocked"
        blocking_reason = "source_traceability_unresolved"
    elif valid_until_required:
        status = "validity_dependency_blocked_by_valid_until_window"
        contract_status = "blocked"
        blocking_reason = "valid_until_window_unresolved"
    elif _missing_upstream_artifact(mapping_row, source_traceability_record):
        status = "validity_dependency_blocked_by_missing_upstream_artifact"
        contract_status = "blocked"
        blocking_reason = "upstream_artifact_or_source_snapshot_unresolved"
    elif source_ready:
        status = "validity_dependency_remediated"
        contract_status = "ready"
        blocking_reason = None
    else:
        status = "validity_dependency_unresolved"
        contract_status = "unresolved"
        blocking_reason = "validity_dependency_unresolved_after_2414_classification"

    return {
        "schema_version": SIGNAL_VALIDITY_DEPENDENCY_CONTRACT_METADATA_SCHEMA_VERSION,
        "source_feature_id": feature_id,
        "source_feature_name": remediation_item.get("source_feature_name")
        or mapping_row.get("feature_name")
        or mapping_row.get("source_feature_name")
        or feature_id,
        "derived_signal_id": _derived_signal_id(feature_id),
        "validity_dependency_id": (
            f"growth_tilt_engine:{feature_id}:signal_validity_dependency:v1"
        ),
        "validity_basis": _validity_basis(
            feature_id,
            status=status,
            mapping_row=mapping_row,
        ),
        "validity_window_required": validity_window_required,
        "valid_until_required": valid_until_required,
        "valid_until_available": False,
        "validity_start_reference": _validity_start_reference(
            mapping_row,
            source_traceability_record,
            source_ready=source_ready,
        ),
        "validity_end_reference": _validity_end_reference(
            valid_until_required=valid_until_required,
            source_ready=source_ready,
            validity_window_required=validity_window_required,
        ),
        "staleness_policy": _staleness_policy(
            status=status,
            valid_until_required=valid_until_required,
        ),
        "expiration_policy": _expiration_policy(
            status=status,
            valid_until_required=valid_until_required,
        ),
        "recompute_required_on_expiry": True,
        "validity_dependency_status": contract_status,
        "validity_dependency_remediation_status": status,
        "validity_blocking_reason": blocking_reason,
        "source_traceability_status": _source_traceability_status(
            mapping_row,
            source_traceability_record,
        ),
        "as_of_semantics_status": mapping_row.get("as_of_semantics_status"),
        "pit_gate_status": _pit_gate_status_after_2414(mapping_row),
        "contract_ready": False,
        "fresh_market_data_required": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_before_after_remediation(
    remediation_records: list[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": BEFORE_AFTER_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "record_count": len(remediation_records),
        "records": [
            {
                "feature_id": record.get("feature_id"),
                "before": record.get("before"),
                "after": record.get("after"),
            }
            for record in remediation_records
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def build_updated_source_feature_mapping(
    source_traceability_remediation_result: Mapping[str, Any],
    mapping_rows: list[Mapping[str, Any]],
    remediation_records: list[Mapping[str, Any]],
) -> dict[str, Any]:
    record_by_feature_id = {
        str(record.get("feature_id")): record for record in remediation_records
    }
    known_feature_ids = {str(row.get("feature_id")) for row in mapping_rows}
    updated_rows: list[dict[str, Any]] = []
    for row in mapping_rows:
        feature_id = str(row.get("feature_id"))
        updated = dict(row)
        record = _as_mapping(record_by_feature_id.get(feature_id))
        if record:
            after = _as_mapping(record.get("after"))
            updated.update(
                {
                    "mapping_status_before_2414": row.get("mapping_status"),
                    "mapping_status": after.get("mapping_status"),
                    "mapping_status_after_2414": after.get("mapping_status"),
                    "mapping_status_reasons": [after.get("mapping_status_reason")],
                    "validity_dependency_status": after.get(
                        "validity_dependency_status"
                    ),
                    "validity_dependency_remediation_status": record.get(
                        "validity_dependency_remediation_status"
                    ),
                    "signal_validity_dependency_contract_metadata": after.get(
                        "signal_validity_dependency_contract_metadata"
                    ),
                    "as_of_semantics_status": after.get("as_of_semantics_status"),
                    "source_traceability_status": after.get(
                        "source_traceability_status"
                    ),
                    "source_traceability_remediation_status": after.get(
                        "source_traceability_remediation_status"
                    ),
                    "traceability_status": after.get("traceability_status"),
                    "pit_gate_status": after.get("pit_gate_status"),
                    "pit_safe": after.get("pit_safe"),
                    "contract_ready": False,
                    "contract_ready_blocking_dimensions": after.get(
                        "contract_ready_blocking_dimensions"
                    ),
                }
            )
        else:
            updated.setdefault("contract_ready", False)
        updated_rows.append(updated)

    for record in remediation_records:
        feature_id = str(record.get("feature_id"))
        if feature_id in known_feature_ids:
            continue
        after = _as_mapping(record.get("after"))
        updated_rows.append(
            {
                "feature_id": feature_id,
                "feature_name": after.get("source_feature_name") or feature_id,
                "mapping_status": after.get("mapping_status"),
                "mapping_status_after_2414": after.get("mapping_status"),
                "validity_dependency_status": after.get("validity_dependency_status"),
                "validity_dependency_remediation_status": record.get(
                    "validity_dependency_remediation_status"
                ),
                "signal_validity_dependency_contract_metadata": after.get(
                    "signal_validity_dependency_contract_metadata"
                ),
                "pit_gate_status": after.get("pit_gate_status"),
                "contract_ready": False,
                "contract_ready_blocking_dimensions": after.get(
                    "contract_ready_blocking_dimensions"
                ),
                "production_effect": "none",
                "broker_action": "none",
            }
        )

    contract_ready_count = sum(1 for row in updated_rows if row.get("contract_ready") is True)
    return {
        "schema_version": UPDATED_MAPPING_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_task": source_traceability_remediation_result.get("task_id", "TRADING-2413"),
        "source_status": source_traceability_remediation_result.get("status"),
        "known_source_feature_count": len(updated_rows),
        "validity_dependency_remediated_feature_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_remediated"
        ),
        "contract_ready_count": contract_ready_count,
        "remaining_contract_ready_blocker_count": len(
            [row for row in updated_rows if row.get("contract_ready") is not True]
        ),
        "mapping_rows": updated_rows,
        "growth_tilt_engine_blocker_resolved": False,
        "growth_tilt_engine_blocker_downgraded": False,
        "valid_until_window_blocker_resolved": False,
        "valid_until_window_blocker_downgraded": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_growth_tilt_signal_validity_dependency_remediation(
    remediation_records: list[Mapping[str, Any]],
    updated_mapping: Mapping[str, Any],
    *,
    expected_validity_dependency_gap_count: int,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    seen_orders: set[int] = set()
    for record in remediation_records:
        feature_id = str(record.get("feature_id"))
        status = record.get("validity_dependency_remediation_status")
        if status not in ALLOWED_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_STATUSES:
            errors.append(_error(feature_id, "UNCLASSIFIED_VALIDITY_STATUS", str(status)))
        order = record.get("remediation_order")
        if not isinstance(order, int) or order <= 0 or order in seen_orders:
            errors.append(_error(feature_id, "INVALID_REMEDIATION_ORDER", str(order)))
        if isinstance(order, int):
            seen_orders.add(order)
        after = _as_mapping(record.get("after"))
        metadata = _as_mapping(after.get("signal_validity_dependency_contract_metadata"))
        for field in (
            "source_feature_id",
            "derived_signal_id",
            "validity_dependency_id",
            "validity_basis",
            "validity_window_required",
            "valid_until_required",
            "valid_until_available",
            "validity_dependency_status",
            "recompute_required_on_expiry",
        ):
            if field not in metadata:
                errors.append(_error(feature_id, "REQUIRED_METADATA_FIELD_MISSING", field))
        if metadata.get("fresh_market_data_required") is not False:
            errors.append(_error(feature_id, "FRESH_MARKET_DATA_REQUIRED", "must be false"))
        if (
            metadata.get("valid_until_required") is True
            and metadata.get("valid_until_available") is False
        ):
            if metadata.get("validity_dependency_status") != "blocked":
                errors.append(
                    _error(feature_id, "VALID_UNTIL_MISSING_NOT_BLOCKED", "status")
                )
            if metadata.get("validity_blocking_reason") != "valid_until_window_unresolved":
                errors.append(
                    _error(feature_id, "VALID_UNTIL_BLOCKING_REASON_MISSING", "reason")
                )
        if status == "validity_dependency_remediated":
            if metadata.get("validity_dependency_status") != "ready":
                errors.append(_error(feature_id, "REMEDIATED_NOT_READY", "contract status"))
            if _source_traceability_status_from_record(record) not in {"ready", None}:
                errors.append(
                    _error(
                        feature_id,
                        "SOURCE_TRACEABILITY_NOT_READY_BUT_VALIDITY_READY",
                        str(_source_traceability_status_from_record(record)),
                    )
                )
        if (
            record.get("as_of_semantics_status_before") == "ready"
            and record.get("as_of_semantics_status_after") != "ready"
        ):
            errors.append(_error(feature_id, "AS_OF_STATUS_ROLLED_BACK", "as-of ready lost"))
        if (
            record.get("source_traceability_status_before") == "ready"
            and record.get("source_traceability_status_after") != "ready"
        ):
            errors.append(
                _error(
                    feature_id,
                    "SOURCE_TRACEABILITY_STATUS_ROLLED_BACK",
                    "source traceability ready lost",
                )
            )
        if after.get("pit_gate_status") == "ready":
            errors.append(_error(feature_id, "PIT_GATE_MARKED_READY_IN_2414", "pit_gate"))
        if record.get("contract_ready") is True:
            errors.append(_error(feature_id, "CONTRACT_READY_SET_IN_2414", "contract_ready"))
    if len(remediation_records) != expected_validity_dependency_gap_count:
        errors.append(
            _error(
                "growth_tilt_engine",
                "VALIDITY_DEPENDENCY_GAP_COUNT_MISMATCH",
                (
                    f"expected {expected_validity_dependency_gap_count}, "
                    f"got {len(remediation_records)}"
                ),
            )
        )
    if updated_mapping.get("contract_ready_count") not in {0, None}:
        errors.append(
            _error(
                "growth_tilt_engine",
                "CONTRACT_READY_COUNT_CHANGED",
                str(updated_mapping.get("contract_ready_count")),
            )
        )
    return {
        "schema_version": SIGNAL_VALIDITY_DEPENDENCY_VALIDATION_SCHEMA_VERSION,
        "valid": not errors,
        "expected_validity_dependency_gap_count": expected_validity_dependency_gap_count,
        "validity_dependency_remediation_record_count": len(remediation_records),
        "validity_dependency_status_unclassified_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            not in ALLOWED_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_STATUSES
        ),
        "validity_dependency_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_remediated"
        ),
        "validity_dependency_blocked_by_valid_until_window_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_blocked_by_valid_until_window"
        ),
        "validity_dependency_blocked_by_source_traceability_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_blocked_by_missing_source_traceability"
        ),
        "as_of_status_rollback_count": sum(
            1
            for record in remediation_records
            if record.get("as_of_semantics_status_before") == "ready"
            and record.get("as_of_semantics_status_after") != "ready"
        ),
        "source_traceability_status_rollback_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_status_before") == "ready"
            and record.get("source_traceability_status_after") != "ready"
        ),
        "contract_ready_count": updated_mapping.get("contract_ready_count"),
        "error_count": len(errors),
        "errors": errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_remaining_blocker_summary(
    source_traceability_remediation_result: Mapping[str, Any],
    remediation_records: list[Mapping[str, Any]],
    updated_mapping: Mapping[str, Any],
) -> dict[str, Any]:
    input_gap_count = _int_value(source_traceability_remediation_result.get("input_gap_count"))
    return {
        "schema_version": REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "input_gap_count": input_gap_count,
        "validity_dependency_gap_count": len(remediation_records),
        "validity_dependency_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_remediated"
        ),
        "validity_dependency_blocked_by_valid_until_window_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_blocked_by_valid_until_window"
        ),
        "validity_dependency_blocked_by_source_traceability_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            == "validity_dependency_blocked_by_missing_source_traceability"
        ),
        "remaining_validity_dependency_gap_count": sum(
            1
            for record in remediation_records
            if record.get("validity_dependency_remediation_status")
            != "validity_dependency_remediated"
        ),
        "remaining_blocked_or_gap_count": input_gap_count,
        "contract_ready_count": updated_mapping.get("contract_ready_count"),
        "remaining_blocking_reasons": [
            "source_traceability_gaps_remain_after_2413",
            "valid_until_window_blocker_not_remediated_in_2414",
            "pit_gate_evidence_not_completed_in_2414",
            "growth_tilt_engine_blocker_not_downgraded_in_2414",
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


def _validity_dependency_items(
    remediation_plan_result: Mapping[str, Any],
    source_traceability_remediation_result: Mapping[str, Any],
    mapping_rows: list[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    items_by_feature: dict[str, dict[str, Any]] = {}
    source_order: dict[str, tuple[int, int]] = {}
    for item in _ordered_items(remediation_plan_result):
        feature_id = str(item.get("feature_id"))
        if _is_validity_target_item(item):
            items_by_feature[feature_id] = dict(item)
            source_order[feature_id] = (_order_value(item.get("remediation_order")), 0)
    for record in _source_traceability_records(source_traceability_remediation_result):
        feature_id = str(record.get("feature_id"))
        if _is_validity_target_source_traceability_record(record):
            items_by_feature.setdefault(feature_id, _item_from_source_record(record))
            source_order.setdefault(
                feature_id,
                (_order_value(record.get("remediation_order")), 1),
            )
    for index, row in enumerate(mapping_rows, start=1):
        feature_id = str(row.get("feature_id"))
        if _is_validity_gap_row(row) and feature_id not in items_by_feature:
            items_by_feature[feature_id] = _item_from_mapping_row(row)
            source_order[feature_id] = (200 + index, 2)
    return sorted(
        items_by_feature.values(),
        key=lambda item: (
            source_order.get(str(item.get("feature_id")), (9999, 9)),
            str(item.get("feature_id")),
        ),
    )


def _is_validity_target_item(item: Mapping[str, Any]) -> bool:
    return item.get("remediation_category") == TARGET_REMEDIATION_CATEGORY or (
        item.get("missing_validity_dependency") is True
    )


def _is_validity_target_source_traceability_record(record: Mapping[str, Any]) -> bool:
    after = _as_mapping(record.get("after"))
    status = str(after.get("validity_dependency_status", "")).lower()
    return (
        status.startswith("not_assessed")
        or "missing" in status
        or "blocked" in status
        or "unresolved" in status
    )


def _is_validity_gap_row(row: Mapping[str, Any]) -> bool:
    feature_id = str(row.get("feature_id", "")).lower()
    mapping_status = str(row.get("mapping_status", "")).lower()
    validity_status = str(row.get("validity_dependency_status", "")).lower()
    validity_dependency = str(row.get("validity_dependency", "")).lower()
    return (
        mapping_status == "missing_validity_dependency"
        or feature_id == "execution_signal_validity_policy"
        or "validity" in feature_id
        or "valid_until" in validity_dependency
        or validity_status.startswith("not_assessed")
        or validity_status in {"missing", "blocked", "unresolved"}
    )


def _item_from_source_record(record: Mapping[str, Any]) -> dict[str, Any]:
    after = _as_mapping(record.get("after"))
    metadata = _as_mapping(after.get("source_traceability_contract_metadata"))
    return {
        "remediation_order": record.get("remediation_order"),
        "feature_id": str(record.get("feature_id", "")).strip(),
        "source_feature_name": after.get("source_feature_name")
        or str(record.get("feature_id", "")).strip(),
        "current_mapping_status": after.get("mapping_status"),
        "remediation_category": TARGET_REMEDIATION_CATEGORY,
        "missing_validity_dependency": True,
        "required_upstream_artifact": metadata.get("upstream_artifact_id")
        or metadata.get("source_snapshot_reference"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _item_from_mapping_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "feature_id": str(row.get("feature_id", "")).strip(),
        "source_feature_name": str(row.get("feature_name") or row.get("feature_id", "")).strip(),
        "current_mapping_status": row.get("mapping_status"),
        "remediation_category": TARGET_REMEDIATION_CATEGORY,
        "missing_validity_dependency": True,
        "required_upstream_artifact": row.get("upstream_artifact_or_registry_reference"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping_rows(
    source_traceability_remediation_result: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    direct = _as_list(source_traceability_remediation_result.get("mapping_rows"))
    if direct:
        return [_as_mapping(row) for row in direct]
    updated = _as_mapping(
        source_traceability_remediation_result.get("updated_source_feature_mapping")
    )
    wrapped = _as_mapping(updated.get("updated_source_feature_mapping"))
    if wrapped:
        updated = wrapped
    return [_as_mapping(row) for row in _as_list(updated.get("mapping_rows"))]


def _source_traceability_records(
    source_traceability_remediation_result: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    direct = _as_list(
        source_traceability_remediation_result.get("source_traceability_remediation_records")
    )
    if direct:
        return [_as_mapping(record) for record in direct]
    nested = _as_mapping(
        source_traceability_remediation_result.get("source_traceability_remediation")
    )
    return [
        _as_mapping(record)
        for record in _as_list(nested.get("source_traceability_remediation_records"))
    ]


def _ordered_items(remediation_plan_result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct_items = _as_list(remediation_plan_result.get("ordered_remediation_items"))
    if direct_items:
        return _sort_items([_as_mapping(item) for item in direct_items])
    plan = _as_mapping(remediation_plan_result.get("contract_gap_remediation_plan"))
    return _sort_items(
        [_as_mapping(item) for item in _as_list(plan.get("ordered_remediation_items"))]
    )


def _source_traceability_ready(
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
) -> bool:
    statuses = {
        str(mapping_row.get("source_traceability_status", "")).lower(),
        str(mapping_row.get("source_traceability_remediation_status", "")).lower(),
        str(mapping_row.get("traceability_status", "")).lower(),
    }
    after = _as_mapping(source_traceability_record.get("after"))
    statuses.update(
        {
            str(after.get("source_traceability_status", "")).lower(),
            str(after.get("source_traceability_remediation_status", "")).lower(),
            str(after.get("traceability_status", "")).lower(),
        }
    )
    return any(status in SOURCE_TRACEABILITY_READY_STATUSES for status in statuses)


def _source_traceability_status(
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
) -> str | None:
    after = _as_mapping(source_traceability_record.get("after"))
    for value in (
        mapping_row.get("source_traceability_status"),
        after.get("source_traceability_status"),
        mapping_row.get("traceability_status"),
        after.get("traceability_status"),
    ):
        if value:
            return str(value)
    return None


def _source_traceability_status_from_record(record: Mapping[str, Any]) -> str | None:
    after = _as_mapping(record.get("after"))
    value = after.get("source_traceability_status") or after.get("traceability_status")
    return str(value) if value else None


def _valid_until_required(
    feature_id: str,
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
) -> bool:
    if feature_id in VALID_UNTIL_REQUIRED_FEATURES:
        return True
    text = " ".join(
        str(value)
        for value in (
            remediation_item.get("required_upstream_artifact"),
            remediation_item.get("validation_requirement"),
            mapping_row.get("validity_dependency"),
        )
        if value
    ).lower()
    return "valid_until_window" in text and feature_id in VALID_UNTIL_REQUIRED_FEATURES


def _validity_window_required(
    feature_id: str,
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
) -> bool:
    if feature_id in VALIDITY_WINDOW_FEATURES:
        return True
    text = " ".join(
        str(value)
        for value in (
            remediation_item.get("required_upstream_artifact"),
            mapping_row.get("validity_dependency"),
            mapping_row.get("feature_type"),
        )
        if value
    ).lower()
    return "validity" in text or "stale" in text or "window" in text


def _validity_start_reference(
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
    *,
    source_ready: bool,
) -> str | None:
    if mapping_row.get("as_of_semantics_status") == "ready":
        return "as_of_date"
    source_metadata = _source_traceability_metadata(mapping_row, source_traceability_record)
    if source_ready and source_metadata.get("source_snapshot_reference"):
        return "source_snapshot_reference"
    return None


def _validity_end_reference(
    *,
    valid_until_required: bool,
    source_ready: bool,
    validity_window_required: bool,
) -> str | None:
    if valid_until_required:
        return "valid_until_window"
    if source_ready:
        return "next_source_snapshot_or_recompute_boundary"
    if validity_window_required:
        return None
    return "not_applicable_until_signal_dependency_exists"


def _validity_basis(
    feature_id: str,
    *,
    status: str,
    mapping_row: Mapping[str, Any],
) -> str:
    existing = str(mapping_row.get("validity_dependency") or "").strip()
    if existing:
        return existing
    if status == "validity_dependency_blocked_by_valid_until_window":
        return "valid_until_window_contract_required"
    if status == "validity_dependency_blocked_by_missing_source_traceability":
        return "blocked_pending_source_traceability_contract"
    if status == "validity_dependency_remediated":
        return "source_snapshot_valid_until_next_recompute_or_config_snapshot_change"
    if feature_id == "growth_tilt_engine_signal_artifact":
        return "standalone_growth_tilt_signal_artifact_validity_contract_required"
    return "growth_tilt_engine_signal_validity_dependency_contract"


def _staleness_policy(*, status: str, valid_until_required: bool) -> str:
    if valid_until_required:
        return "blocked_pending_valid_until_window_contract"
    if status == "validity_dependency_remediated":
        return "recompute_on_source_snapshot_change"
    if status == "validity_dependency_blocked_by_missing_source_traceability":
        return "blocked_pending_source_traceability_contract"
    if status == "validity_dependency_blocked_by_missing_upstream_artifact":
        return "blocked_pending_upstream_artifact"
    if status == "validity_dependency_blocked_by_ambiguous_signal_boundary":
        return "blocked_pending_owner_boundary_review"
    return "unresolved"


def _expiration_policy(*, status: str, valid_until_required: bool) -> str:
    if valid_until_required:
        return "valid_until_window_required_before_expiration_can_be_evaluated"
    if status == "validity_dependency_remediated":
        return "expire_on_source_snapshot_replacement_or_recompute"
    if status == "validity_dependency_blocked_by_missing_source_traceability":
        return "cannot_define_expiration_without_source_traceability"
    if status == "validity_dependency_blocked_by_missing_upstream_artifact":
        return "cannot_define_expiration_without_upstream_artifact"
    if status == "validity_dependency_blocked_by_ambiguous_signal_boundary":
        return "cannot_define_expiration_without_owner_boundary_review"
    return "unresolved"


def _derived_signal_id(feature_id: str) -> str:
    if feature_id in {"execution_signal_validity_policy", "growth_tilt_engine_signal_artifact"}:
        return "growth_tilt_engine_signal"
    return f"growth_tilt_engine:{feature_id}:source_signal_input"


def _contract_ready_blocking_dimensions(
    mapping_row: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> list[str]:
    blocking: list[str] = []
    if mapping_row.get("as_of_semantics_status") != "ready":
        blocking.append("as_of_semantics_status")
    if metadata.get("source_traceability_status") != "ready":
        blocking.append("source_traceability_status")
    if metadata.get("validity_dependency_status") != "ready":
        blocking.append("validity_dependency_status")
    if _pit_gate_status_after_2414(mapping_row) != "ready":
        blocking.append("pit_gate_status")
    return blocking


def _mapping_status_reason(status: str) -> str:
    return {
        "validity_dependency_remediated": (
            "signal validity dependency metadata remediated in TRADING-2414; PIT gate "
            "evidence remains unresolved"
        ),
        "validity_dependency_partially_remediated": (
            "signal validity dependency metadata partially remediated in TRADING-2414; "
            "remaining contract dimensions keep the feature blocked"
        ),
        "validity_dependency_blocked_by_valid_until_window": (
            "signal validity dependency requires valid_until_window, which remains "
            "unresolved and undowngraded"
        ),
        "validity_dependency_blocked_by_missing_source_traceability": (
            "signal validity dependency cannot be marked ready while source traceability "
            "or source snapshot is unresolved"
        ),
        "validity_dependency_blocked_by_missing_upstream_artifact": (
            "signal validity dependency cannot be marked ready without upstream artifact "
            "or standalone signal artifact evidence"
        ),
        "validity_dependency_blocked_by_ambiguous_signal_boundary": (
            "signal validity dependency blocked by ambiguous signal boundary requiring "
            "owner review"
        ),
        "validity_dependency_not_applicable_non_signal_feature": (
            "feature is excluded from signal validity dependency contract-ready scope"
        ),
        "validity_dependency_unresolved": (
            "signal validity dependency remains unresolved after TRADING-2414 classification"
        ),
    }[status]


def _pit_gate_status_after_2414(mapping_row: Mapping[str, Any]) -> str:
    return str(mapping_row.get("pit_gate_status") or "blocked_pending_pit_evidence")


def _source_traceability_metadata(
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
) -> Mapping[str, Any]:
    row_metadata = _as_mapping(mapping_row.get("source_traceability_contract_metadata"))
    if row_metadata:
        return row_metadata
    after = _as_mapping(source_traceability_record.get("after"))
    return _as_mapping(after.get("source_traceability_contract_metadata"))


def _missing_upstream_artifact(
    mapping_row: Mapping[str, Any],
    source_traceability_record: Mapping[str, Any],
) -> bool:
    metadata = _source_traceability_metadata(mapping_row, source_traceability_record)
    source_system = str(mapping_row.get("source_system", "")).lower()
    return (
        source_system == "missing_artifact"
        and not metadata.get("source_snapshot_reference")
        and not metadata.get("upstream_artifact_path")
    )


def _ambiguous_signal_boundary(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
) -> bool:
    text = " ".join(
        str(value)
        for value in (
            remediation_item.get("current_mapping_status"),
            mapping_row.get("mapping_status"),
            mapping_row.get("feature_type"),
            " ".join(str(item) for item in _as_list(mapping_row.get("mapping_status_reasons"))),
        )
        if value
    ).lower()
    return "ambiguous" in text


def _non_signal_feature(mapping_row: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(value)
        for value in (
            mapping_row.get("feature_type"),
            mapping_row.get("mapping_status"),
            mapping_row.get("contract_scope"),
        )
        if value
    ).lower()
    return "non_signal" in text or "excluded_non_signal_feature" in text


def _sort_items(items: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            _order_value(item.get("remediation_order")),
            str(item.get("feature_id")),
        ),
    )


def _order_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 9999


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
