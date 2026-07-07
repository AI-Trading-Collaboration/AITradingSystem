from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import safe_load_yaml_path

SOURCE_TRACEABILITY_REMEDIATION_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_remediation.v1"
)
SOURCE_TRACEABILITY_VALIDATION_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_validation.v1"
)
SOURCE_TRACEABILITY_CONTRACT_METADATA_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_contract_metadata.v1"
)
UPDATED_MAPPING_SCHEMA_VERSION = (
    "growth_tilt_engine_source_feature_mapping_after_source_traceability.v1"
)
BEFORE_AFTER_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_before_after_remediation.v1"
)
REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_remaining_blocker_summary.v1"
)

ALLOWED_SOURCE_TRACEABILITY_REMEDIATION_STATUSES: tuple[str, ...] = (
    "source_traceability_remediated",
    "source_traceability_partially_remediated",
    "source_traceability_blocked_by_missing_upstream_artifact",
    "source_traceability_blocked_by_missing_registry_entry",
    "source_traceability_blocked_by_ambiguous_source_boundary",
    "source_traceability_not_applicable_non_signal_feature",
    "source_traceability_unresolved",
)
TARGET_REMEDIATION_CATEGORY = "source_traceability_required"
NEXT_ROUTE = "TRADING-2414_Growth_Tilt_Engine_Signal_Validity_Dependency_Remediation"

UPSTREAM_REPORT_REGISTRY_BY_FEATURE: dict[str, str] = {
    "equal_risk_baseline_weights": "growth_tilt_engine_source_feature_contract_mapping",
    "target_vol_policy": "growth_tilt_engine_source_feature_contract_mapping",
    "risk_on_trend_filter_context": "growth_tilt_engine_source_feature_contract_mapping",
    "trend_features": "growth_tilt_engine_source_feature_contract_mapping",
    "volatility_inputs": "growth_tilt_engine_as_of_semantics_remediation",
    "drawdown_features": "growth_tilt_engine_as_of_semantics_remediation",
    "growth_tilt_engine_signal_artifact": "growth_tilt_engine_contract_gap_remediation_plan",
}


def build_growth_tilt_source_traceability_remediation(
    remediation_plan_result: Mapping[str, Any],
    as_of_remediation_result: Mapping[str, Any],
    *,
    project_root: Path,
    report_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    mapping_rows = _mapping_rows(as_of_remediation_result)
    row_by_feature_id = {str(row.get("feature_id")): row for row in mapping_rows}
    source_items = _source_traceability_items(remediation_plan_result, mapping_rows)
    remediation_records = [
        build_source_traceability_remediation_record(
            item,
            row_by_feature_id.get(str(item.get("feature_id")), {}),
            project_root=project_root,
            report_registry=report_registry or {},
            remediation_order=index,
        )
        for index, item in enumerate(source_items, start=1)
    ]
    updated_mapping = build_updated_source_feature_mapping(
        as_of_remediation_result,
        mapping_rows,
        remediation_records,
    )
    validation = validate_growth_tilt_source_traceability_remediation(
        remediation_records,
        updated_mapping,
        expected_source_traceability_gap_count=len(source_items),
    )
    before_after = build_before_after_remediation(remediation_records)
    remaining_summary = build_remaining_blocker_summary(
        as_of_remediation_result,
        remediation_records,
        updated_mapping,
    )
    metadata_rows = [
        record["after"]["source_traceability_contract_metadata"]
        for record in remediation_records
    ]
    return {
        "schema_version": SOURCE_TRACEABILITY_REMEDIATION_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_tasks": ["TRADING-2411", "TRADING-2412"],
        "source_status_2411": remediation_plan_result.get("status"),
        "source_status_2412": as_of_remediation_result.get("status"),
        "input_gap_count": as_of_remediation_result.get("input_gap_count")
        or remediation_plan_result.get("gap_count"),
        "source_traceability_gap_count": len(source_items),
        "source_traceability_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            == "source_traceability_remediated"
        ),
        "remaining_source_traceability_gap_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            != "source_traceability_remediated"
        ),
        "remaining_blocked_or_gap_count": remaining_summary["remaining_blocked_or_gap_count"],
        "contract_ready_count": updated_mapping["contract_ready_count"],
        "allowed_source_traceability_remediation_statuses": list(
            ALLOWED_SOURCE_TRACEABILITY_REMEDIATION_STATUSES
        ),
        "source_traceability_remediation_records": remediation_records,
        "source_traceability_contract_metadata": {
            "schema_version": SOURCE_TRACEABILITY_CONTRACT_METADATA_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "metadata_rows": metadata_rows,
            "fresh_market_data_required": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "before_after_source_traceability_remediation": before_after,
        "updated_source_feature_mapping": updated_mapping,
        "remaining_blocker_summary": remaining_summary,
        "source_traceability_remediation_validation": validation,
        "source_traceability_remediation_completed": validation["valid"],
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


def build_source_traceability_remediation_record(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    *,
    project_root: Path,
    report_registry: Mapping[str, Any],
    remediation_order: int,
) -> dict[str, Any]:
    feature_id = str(remediation_item.get("feature_id") or mapping_row.get("feature_id"))
    before = {
        "feature_id": feature_id,
        "source_feature_name": remediation_item.get("source_feature_name")
        or mapping_row.get("feature_name")
        or feature_id,
        "mapping_status": mapping_row.get("mapping_status"),
        "traceability_status": mapping_row.get("traceability_status"),
        "source_traceability_status": mapping_row.get("source_traceability_status"),
        "as_of_semantics_status": mapping_row.get("as_of_semantics_status"),
        "validity_dependency_status": mapping_row.get("validity_dependency_status"),
        "pit_gate_status": mapping_row.get("pit_gate_status"),
        "contract_ready": False,
    }
    metadata = source_traceability_contract_metadata(
        remediation_item,
        mapping_row,
        project_root=project_root,
        report_registry=report_registry,
    )
    status = metadata["source_traceability_remediation_status"]
    source_ready = status == "source_traceability_remediated"
    after = {
        "feature_id": feature_id,
        "source_feature_name": before["source_feature_name"],
        "mapping_status": "mapped_with_caveats" if source_ready else "blocked_unresolved",
        "mapping_status_reason": _mapping_status_reason(status),
        "as_of_semantics_status": before["as_of_semantics_status"],
        "as_of_remediation_status": mapping_row.get("as_of_remediation_status"),
        "source_traceability_status": "ready" if source_ready else "not_ready",
        "source_traceability_remediation_status": status,
        "traceability_status": "ready" if source_ready else "blocked",
        "validity_dependency_status": _validity_status_after_2413(mapping_row),
        "pit_gate_status": _pit_gate_status_after_2413(mapping_row),
        "pit_safe": mapping_row.get("pit_safe") or "unknown",
        "contract_ready": False,
        "contract_ready_blocking_dimensions": _contract_ready_blocking_dimensions(
            before,
            source_ready=source_ready,
        ),
        "source_traceability_contract_metadata": metadata,
    }
    return {
        "remediation_order": remediation_order,
        "source_remediation_order_2411": remediation_item.get("remediation_order"),
        "feature_id": feature_id,
        "source_traceability_remediation_status": status,
        "source_traceability_status_before": before.get("source_traceability_status")
        or before.get("traceability_status"),
        "source_traceability_status_after": after["source_traceability_status"],
        "as_of_semantics_status_before": before.get("as_of_semantics_status"),
        "as_of_semantics_status_after": after.get("as_of_semantics_status"),
        "contract_ready": False,
        "before": before,
        "after": after,
        "production_effect": "none",
        "broker_action": "none",
    }


def source_traceability_contract_metadata(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    *,
    project_root: Path,
    report_registry: Mapping[str, Any],
) -> dict[str, Any]:
    feature_id = str(remediation_item.get("feature_id") or mapping_row.get("feature_id"))
    source_ref = str(
        mapping_row.get("upstream_artifact_or_registry_reference")
        or remediation_item.get("required_upstream_artifact")
        or ""
    ).strip()
    config_path, config_key = _parse_config_reference(source_ref)
    report_registry_id = UPSTREAM_REPORT_REGISTRY_BY_FEATURE.get(
        feature_id,
        "growth_tilt_engine_source_feature_contract_mapping",
    )
    registry_present = _registry_has_report_id(report_registry, report_registry_id)
    snapshot_reference: str | None = None
    snapshot_hash: str | None = None
    blocking_reason: str | None = None
    artifact_path: str | None = None
    artifact_id = source_ref or f"growth_tilt_engine:{feature_id}:source"

    if config_path:
        artifact_path = config_path
        resolved_config_path = (project_root / config_path).resolve()
        if not resolved_config_path.exists():
            blocking_reason = f"missing governed config path: {config_path}"
        elif config_key and not _config_key_exists(resolved_config_path, config_key):
            blocking_reason = f"missing governed config key: {config_key}"
        else:
            snapshot_hash = f"sha256:{_sha256_file(resolved_config_path)}"
            snapshot_reference = f"{config_path}:{config_key}@{snapshot_hash}"
    elif _is_ambiguous(remediation_item, mapping_row, source_ref):
        blocking_reason = "ambiguous source feature boundary requires owner review"
    else:
        blocking_reason = "missing upstream artifact path or source snapshot reference"

    if not registry_present:
        status = "source_traceability_blocked_by_missing_registry_entry"
        if blocking_reason:
            blocking_reason = f"{blocking_reason}; missing report registry id {report_registry_id}"
        else:
            blocking_reason = f"missing report registry id {report_registry_id}"
    elif _non_signal_feature(mapping_row):
        status = "source_traceability_not_applicable_non_signal_feature"
        blocking_reason = "non-signal feature excluded from source traceability readiness"
    elif _is_ambiguous(remediation_item, mapping_row, source_ref):
        status = "source_traceability_blocked_by_ambiguous_source_boundary"
    elif snapshot_hash and snapshot_reference and artifact_path:
        status = "source_traceability_remediated"
        blocking_reason = None
    elif blocking_reason:
        status = "source_traceability_blocked_by_missing_upstream_artifact"
    else:
        status = "source_traceability_unresolved"
        blocking_reason = "source traceability unresolved after 2413 classification"

    return {
        "schema_version": SOURCE_TRACEABILITY_CONTRACT_METADATA_SCHEMA_VERSION,
        "source_traceability_contract_id": (
            f"growth_tilt_engine:{feature_id}:source_traceability:v1"
        ),
        "source_feature_id": feature_id,
        "source_feature_name": remediation_item.get("source_feature_name")
        or mapping_row.get("feature_name")
        or feature_id,
        "upstream_source_system": mapping_row.get("source_system") or _source_system(source_ref),
        "upstream_artifact_path": artifact_path,
        "upstream_artifact_id": artifact_id,
        "upstream_report_registry_id": report_registry_id,
        "upstream_config_path": config_path,
        "upstream_config_key": config_key,
        "source_snapshot_reference": snapshot_reference,
        "source_snapshot_hash": snapshot_hash,
        "derived_from_prior_artifact": not bool(config_path),
        "fresh_market_data_required": False,
        "traceability_status": (
            "ready" if status == "source_traceability_remediated" else "not_ready"
        ),
        "source_traceability_remediation_status": status,
        "traceability_blocking_reason": blocking_reason,
        "as_of_semantics_status": mapping_row.get("as_of_semantics_status"),
        "validity_dependency_status": _validity_status_after_2413(mapping_row),
        "pit_gate_status": _pit_gate_status_after_2413(mapping_row),
        "contract_ready": False,
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
    as_of_remediation_result: Mapping[str, Any],
    mapping_rows: list[Mapping[str, Any]],
    remediation_records: list[Mapping[str, Any]],
) -> dict[str, Any]:
    record_by_feature_id = {
        str(record.get("feature_id")): record for record in remediation_records
    }
    updated_rows: list[dict[str, Any]] = []
    for row in mapping_rows:
        feature_id = str(row.get("feature_id"))
        updated = dict(row)
        record = _as_mapping(record_by_feature_id.get(feature_id))
        if record:
            after = _as_mapping(record.get("after"))
            updated.update(
                {
                    "mapping_status_before_2413": row.get("mapping_status"),
                    "mapping_status": after.get("mapping_status"),
                    "mapping_status_after_2413": after.get("mapping_status"),
                    "mapping_status_reasons": [after.get("mapping_status_reason")],
                    "source_traceability_status": after.get("source_traceability_status"),
                    "source_traceability_remediation_status": record.get(
                        "source_traceability_remediation_status"
                    ),
                    "traceability_status": after.get("traceability_status"),
                    "source_traceability_contract_metadata": after.get(
                        "source_traceability_contract_metadata"
                    ),
                    "as_of_semantics_status": after.get("as_of_semantics_status"),
                    "validity_dependency_status": after.get("validity_dependency_status"),
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
    contract_ready_count = sum(1 for row in updated_rows if row.get("contract_ready") is True)
    return {
        "schema_version": UPDATED_MAPPING_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_task": as_of_remediation_result.get("task_id", "TRADING-2412"),
        "source_status": as_of_remediation_result.get("status"),
        "known_source_feature_count": len(updated_rows),
        "source_traceability_remediated_feature_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            == "source_traceability_remediated"
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


def validate_growth_tilt_source_traceability_remediation(
    remediation_records: list[Mapping[str, Any]],
    updated_mapping: Mapping[str, Any],
    *,
    expected_source_traceability_gap_count: int,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    seen_orders: set[int] = set()
    for record in remediation_records:
        feature_id = str(record.get("feature_id"))
        status = record.get("source_traceability_remediation_status")
        if status not in ALLOWED_SOURCE_TRACEABILITY_REMEDIATION_STATUSES:
            errors.append(
                _error(feature_id, "UNCLASSIFIED_SOURCE_TRACEABILITY_STATUS", str(status))
            )
        order = record.get("remediation_order")
        if not isinstance(order, int) or order <= 0 or order in seen_orders:
            errors.append(_error(feature_id, "INVALID_REMEDIATION_ORDER", str(order)))
        if isinstance(order, int):
            seen_orders.add(order)
        after = _as_mapping(record.get("after"))
        metadata = _as_mapping(after.get("source_traceability_contract_metadata"))
        if metadata.get("fresh_market_data_required") is not False:
            errors.append(_error(feature_id, "FRESH_MARKET_DATA_REQUIRED", "must be false"))
        if status == "source_traceability_remediated":
            for field in (
                "upstream_artifact_path",
                "upstream_config_path",
                "upstream_config_key",
                "source_snapshot_reference",
                "source_snapshot_hash",
            ):
                if not metadata.get(field):
                    errors.append(_error(feature_id, "REMEDIATED_METADATA_MISSING", field))
        if (
            record.get("as_of_semantics_status_before") == "ready"
            and record.get("as_of_semantics_status_after") != "ready"
        ):
            errors.append(_error(feature_id, "AS_OF_STATUS_ROLLED_BACK", "as-of ready lost"))
        if after.get("validity_dependency_status") == "ready":
            errors.append(_error(feature_id, "VALIDITY_MARKED_READY_IN_2413", "validity"))
        if after.get("pit_gate_status") == "ready":
            errors.append(_error(feature_id, "PIT_GATE_MARKED_READY_IN_2413", "pit_gate"))
        if record.get("contract_ready") is True:
            errors.append(_error(feature_id, "CONTRACT_READY_SET_IN_2413", "contract_ready"))
    if len(remediation_records) != expected_source_traceability_gap_count:
        errors.append(
            _error(
                "growth_tilt_engine",
                "SOURCE_TRACEABILITY_GAP_COUNT_MISMATCH",
                (
                    f"expected {expected_source_traceability_gap_count}, "
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
        "schema_version": SOURCE_TRACEABILITY_VALIDATION_SCHEMA_VERSION,
        "valid": not errors,
        "expected_source_traceability_gap_count": expected_source_traceability_gap_count,
        "source_traceability_remediation_record_count": len(remediation_records),
        "source_traceability_status_unclassified_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            not in ALLOWED_SOURCE_TRACEABILITY_REMEDIATION_STATUSES
        ),
        "source_traceability_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            == "source_traceability_remediated"
        ),
        "blocked_or_unresolved_source_traceability_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            != "source_traceability_remediated"
        ),
        "as_of_status_rollback_count": sum(
            1
            for record in remediation_records
            if record.get("as_of_semantics_status_before") == "ready"
            and record.get("as_of_semantics_status_after") != "ready"
        ),
        "contract_ready_count": updated_mapping.get("contract_ready_count"),
        "error_count": len(errors),
        "errors": errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_remaining_blocker_summary(
    as_of_remediation_result: Mapping[str, Any],
    remediation_records: list[Mapping[str, Any]],
    updated_mapping: Mapping[str, Any],
) -> dict[str, Any]:
    input_gap_count = _int_value(as_of_remediation_result.get("input_gap_count"))
    return {
        "schema_version": REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "input_gap_count": input_gap_count,
        "source_traceability_gap_count": len(remediation_records),
        "source_traceability_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            == "source_traceability_remediated"
        ),
        "remaining_source_traceability_gap_count": sum(
            1
            for record in remediation_records
            if record.get("source_traceability_remediation_status")
            != "source_traceability_remediated"
        ),
        "remaining_blocked_or_gap_count": input_gap_count,
        "contract_ready_count": updated_mapping.get("contract_ready_count"),
        "remaining_blocking_reasons": [
            "derived_window_source_snapshots_not_completed_in_2413",
            "signal_validity_dependency_not_completed_in_2413",
            "pit_gate_evidence_not_completed_in_2413",
            "valid_until_window_blocker_not_remediated_in_2413",
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


def _source_traceability_items(
    remediation_plan_result: Mapping[str, Any],
    mapping_rows: list[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    items_by_feature: dict[str, dict[str, Any]] = {}
    source_order: dict[str, int] = {}
    for item in _ordered_items(remediation_plan_result):
        feature_id = str(item.get("feature_id"))
        if _is_traceability_target_item(item):
            items_by_feature[feature_id] = dict(item)
            source_order[feature_id] = _order_value(item.get("remediation_order"))
    for index, row in enumerate(mapping_rows, start=1):
        feature_id = str(row.get("feature_id"))
        if _is_traceability_gap_row(row) and feature_id not in items_by_feature:
            items_by_feature[feature_id] = _item_from_mapping_row(row)
            source_order[feature_id] = 100 + index
    return sorted(
        items_by_feature.values(),
        key=lambda item: (
            source_order.get(str(item.get("feature_id")), 9999),
            str(item.get("feature_id")),
        ),
    )


def _is_traceability_target_item(item: Mapping[str, Any]) -> bool:
    return item.get("remediation_category") == TARGET_REMEDIATION_CATEGORY or (
        item.get("missing_source_traceability") is True
    )


def _is_traceability_gap_row(row: Mapping[str, Any]) -> bool:
    status = str(row.get("mapping_status", "")).lower()
    source_status = str(row.get("source_traceability_status", "")).lower()
    traceability_status = str(row.get("traceability_status", "")).lower()
    return (
        status in {"missing_source_traceability", "ambiguous_source_feature"}
        or "missing" in source_status
        or "ambiguous" in source_status
        or traceability_status in {"missing", "partial", "ambiguous"}
    )


def _item_from_mapping_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "feature_id": str(row.get("feature_id", "")).strip(),
        "source_feature_name": str(row.get("feature_name") or row.get("feature_id", "")).strip(),
        "current_mapping_status": row.get("mapping_status"),
        "remediation_category": TARGET_REMEDIATION_CATEGORY,
        "missing_source_traceability": True,
        "required_upstream_artifact": row.get("upstream_artifact_or_registry_reference"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping_rows(as_of_remediation_result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct = _as_list(as_of_remediation_result.get("mapping_rows"))
    if direct:
        return [_as_mapping(row) for row in direct]
    updated = _as_mapping(as_of_remediation_result.get("updated_source_feature_mapping"))
    wrapped = _as_mapping(updated.get("updated_source_feature_mapping"))
    if wrapped:
        updated = wrapped
    return [_as_mapping(row) for row in _as_list(updated.get("mapping_rows"))]


def _ordered_items(remediation_plan_result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct_items = _as_list(remediation_plan_result.get("ordered_remediation_items"))
    if direct_items:
        return _sort_items([_as_mapping(item) for item in direct_items])
    plan = _as_mapping(remediation_plan_result.get("contract_gap_remediation_plan"))
    return _sort_items(
        [_as_mapping(item) for item in _as_list(plan.get("ordered_remediation_items"))]
    )


def _sort_items(items: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            _order_value(item.get("remediation_order")),
            str(item.get("feature_id", "")),
        ),
    )


def _order_value(value: Any) -> int:
    return value if isinstance(value, int) else 9999


def _parse_config_reference(source_ref: str) -> tuple[str | None, str | None]:
    if not source_ref.startswith("config/") or ":" not in source_ref:
        return None, None
    path, key = source_ref.split(":", 1)
    return path.strip(), key.strip() or None


def _config_key_exists(path: Path, dotted_key: str) -> bool:
    value: Any = safe_load_yaml_path(path)
    for part in dotted_key.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return False
        value = value[part]
    return True


def _sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _registry_has_report_id(report_registry: Mapping[str, Any], report_id: str) -> bool:
    for entry in _as_list(report_registry.get("reports")):
        if _as_mapping(entry).get("report_id") == report_id:
            return True
    return False


def _is_ambiguous(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    source_ref: str,
) -> bool:
    text = " ".join(
        [
            str(remediation_item.get("current_mapping_status", "")),
            str(mapping_row.get("mapping_status", "")),
            str(mapping_row.get("source_traceability_status", "")),
            str(mapping_row.get("traceability_status", "")),
            source_ref,
            " ".join(str(item) for item in _as_list(mapping_row.get("mapping_status_reasons"))),
        ]
    ).lower()
    return "ambiguous" in text


def _non_signal_feature(mapping_row: Mapping[str, Any]) -> bool:
    return "NON_SIGNAL" in str(mapping_row.get("feature_type", "")).upper()


def _source_system(source_ref: str) -> str:
    if source_ref.startswith("config/"):
        return "governed_config"
    if source_ref:
        return "derived_research_artifact"
    return "missing_artifact"


def _mapping_status_reason(status: str) -> str:
    if status == "source_traceability_remediated":
        return (
            "source traceability remediated in TRADING-2413; validity dependency and "
            "PIT gate evidence remain unresolved"
        )
    return (
        "source traceability explicitly classified in TRADING-2413 but remains blocked; "
        "validity dependency and PIT gate evidence also remain unresolved"
    )


def _validity_status_after_2413(mapping_row: Mapping[str, Any]) -> str:
    value = mapping_row.get("validity_dependency_status")
    if value and value != "ready":
        return str(value)
    return "not_assessed_in_2413"


def _pit_gate_status_after_2413(mapping_row: Mapping[str, Any]) -> str:
    value = mapping_row.get("pit_gate_status")
    if value and value != "ready":
        return str(value)
    return "blocked_pending_pit_evidence"


def _contract_ready_blocking_dimensions(
    before: Mapping[str, Any],
    *,
    source_ready: bool,
) -> list[str]:
    dimensions: list[str] = []
    if before.get("as_of_semantics_status") != "ready":
        dimensions.append("as_of_semantics_status")
    if not source_ready:
        dimensions.append("source_traceability_status")
    dimensions.extend(["validity_dependency_status", "pit_gate_status"])
    return dimensions


def _int_value(value: Any) -> int:
    return value if isinstance(value, int) else 0


def _error(feature_id: str, code: str, message: str) -> dict[str, Any]:
    return {
        "feature_id": feature_id,
        "code": code,
        "message": message,
        "severity": "ERROR",
    }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
