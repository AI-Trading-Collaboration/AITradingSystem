from __future__ import annotations

from collections.abc import Mapping
from typing import Any

AS_OF_REMEDIATION_SCHEMA_VERSION = "growth_tilt_engine_as_of_semantics_remediation.v1"
AS_OF_VALIDATION_SCHEMA_VERSION = "growth_tilt_engine_as_of_semantics_validation.v1"
AS_OF_CONTRACT_METADATA_SCHEMA_VERSION = "growth_tilt_engine_as_of_contract_metadata.v1"
UPDATED_MAPPING_SCHEMA_VERSION = "growth_tilt_engine_source_feature_mapping_after_as_of.v1"
BEFORE_AFTER_SCHEMA_VERSION = "growth_tilt_engine_as_of_before_after_remediation.v1"

ALLOWED_AS_OF_REMEDIATION_STATUSES: tuple[str, ...] = (
    "as_of_semantics_remediated",
    "as_of_semantics_partially_remediated",
    "as_of_semantics_blocked_by_missing_upstream_artifact",
    "as_of_semantics_blocked_by_ambiguous_source_boundary",
    "as_of_semantics_not_applicable_non_signal_feature",
    "as_of_semantics_unresolved",
)
TARGET_REMEDIATION_CATEGORY = "as_of_semantics_required"
NEXT_ROUTE = "TRADING-2413_Growth_Tilt_Engine_Source_Traceability_Remediation"


def build_growth_tilt_as_of_semantics_remediation(
    remediation_plan_result: Mapping[str, Any],
    source_mapping_result: Mapping[str, Any],
    *,
    as_of_date: str | None,
) -> dict[str, Any]:
    items = _ordered_items(remediation_plan_result)
    mapping_rows = _mapping_rows(source_mapping_result)
    row_by_feature_id = {str(row.get("feature_id")): row for row in mapping_rows}
    as_of_items = [item for item in items if _is_as_of_target(item)]
    remediation_records = [
        build_as_of_remediation_record(
            item,
            row_by_feature_id.get(str(item.get("feature_id")), {}),
            as_of_date=as_of_date,
            remediation_order=index,
        )
        for index, item in enumerate(as_of_items, start=1)
    ]
    updated_mapping = build_updated_source_feature_mapping(
        source_mapping_result,
        mapping_rows,
        remediation_records,
    )
    validation = validate_growth_tilt_as_of_semantics_remediation(
        remediation_records,
        updated_mapping,
        expected_as_of_gap_count=len(as_of_items),
    )
    remaining_summary = build_remaining_blocker_summary(
        remediation_plan_result,
        remediation_records,
        updated_mapping,
    )
    before_after = build_before_after_remediation(remediation_records)
    metadata_rows = [record["after"]["as_of_contract_metadata"] for record in remediation_records]
    return {
        "schema_version": AS_OF_REMEDIATION_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_task": "TRADING-2411",
        "source_status": remediation_plan_result.get("status"),
        "input_gap_count": remediation_plan_result.get("gap_count"),
        "as_of_gap_count": len(as_of_items),
        "as_of_remediated_count": sum(
            1
            for record in remediation_records
            if record.get("as_of_remediation_status") == "as_of_semantics_remediated"
        ),
        "remaining_as_of_gap_count": sum(
            1
            for record in remediation_records
            if record.get("as_of_semantics_status_after") != "ready"
        ),
        "remaining_blocked_or_gap_count": remaining_summary["remaining_blocked_or_gap_count"],
        "contract_ready_count": updated_mapping["contract_ready_count"],
        "allowed_as_of_remediation_statuses": list(ALLOWED_AS_OF_REMEDIATION_STATUSES),
        "as_of_remediation_records": remediation_records,
        "as_of_contract_metadata": {
            "schema_version": AS_OF_CONTRACT_METADATA_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "metadata_rows": metadata_rows,
            "lookahead_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "before_after_remediation": before_after,
        "updated_source_feature_mapping": updated_mapping,
        "remaining_blocker_summary": remaining_summary,
        "as_of_remediation_validation": validation,
        "as_of_remediation_completed": validation["valid"],
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


def build_as_of_remediation_record(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    *,
    as_of_date: str | None,
    remediation_order: int,
) -> dict[str, Any]:
    feature_id = str(remediation_item.get("feature_id", "")).strip()
    before = {
        "feature_id": feature_id,
        "source_feature_name": remediation_item.get("source_feature_name") or feature_id,
        "mapping_status": remediation_item.get("current_mapping_status"),
        "as_of_semantics": mapping_row.get("as_of_semantics"),
        "as_of_semantics_status": "missing",
        "traceability_status": mapping_row.get("traceability_status"),
        "validity_dependency": mapping_row.get("validity_dependency"),
        "pit_eligibility": mapping_row.get("pit_eligibility"),
        "contract_ready": False,
    }
    status = as_of_remediation_status(remediation_item, mapping_row)
    metadata = as_of_contract_metadata(
        remediation_item,
        mapping_row,
        as_of_date=as_of_date,
        as_of_remediation_status=status,
    )
    after = {
        "feature_id": feature_id,
        "source_feature_name": remediation_item.get("source_feature_name") or feature_id,
        "mapping_status": "mapped_with_caveats",
        "mapping_status_reason": (
            "as-of semantics remediated in TRADING-2412; source traceability and PIT "
            "gate evidence remain unresolved"
        ),
        "as_of_semantics": metadata["as_of_semantics_contract"],
        "as_of_semantics_status": (
            "ready" if status == "as_of_semantics_remediated" else "not_ready"
        ),
        "as_of_remediation_status": status,
        "source_traceability_status": "not_ready_missing_source_snapshot",
        "validity_dependency_status": "not_assessed_in_2412",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": metadata["pit_safe"],
        "contract_ready": False,
        "contract_ready_blocking_dimensions": [
            "source_traceability_status",
            "pit_gate_status",
        ],
        "as_of_contract_metadata": metadata,
    }
    return {
        "remediation_order": remediation_order,
        "source_remediation_order_2411": remediation_item.get("remediation_order"),
        "feature_id": feature_id,
        "as_of_remediation_status": status,
        "as_of_semantics_status_before": before["as_of_semantics_status"],
        "as_of_semantics_status_after": after["as_of_semantics_status"],
        "lookahead_allowed": metadata["lookahead_allowed"],
        "pit_safe": metadata["pit_safe"],
        "contract_ready": False,
        "before": before,
        "after": after,
        "production_effect": "none",
        "broker_action": "none",
    }


def as_of_remediation_status(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
) -> str:
    feature_type = str(mapping_row.get("feature_type", "")).upper()
    source_ref = str(
        mapping_row.get("upstream_artifact_or_registry_reference")
        or remediation_item.get("required_upstream_artifact")
        or ""
    ).lower()
    reasons = " ".join(str(item) for item in _as_list(mapping_row.get("mapping_status_reasons")))
    reasons = reasons.lower()
    if "NON_SIGNAL" in feature_type:
        return "as_of_semantics_not_applicable_non_signal_feature"
    if "ambiguous" in reasons or "ambiguous" in source_ref:
        return "as_of_semantics_blocked_by_ambiguous_source_boundary"
    if not source_ref or "missing" in source_ref or "tbd" in source_ref:
        return "as_of_semantics_blocked_by_missing_upstream_artifact"
    if _is_as_of_target(remediation_item):
        return "as_of_semantics_remediated"
    return "as_of_semantics_unresolved"


def as_of_contract_metadata(
    remediation_item: Mapping[str, Any],
    mapping_row: Mapping[str, Any],
    *,
    as_of_date: str | None,
    as_of_remediation_status: str,
) -> dict[str, Any]:
    feature_id = str(remediation_item.get("feature_id", "")).strip()
    contract_payload = _as_mapping(mapping_row.get("contract_payload"))
    lookback_window = (
        contract_payload.get("lookback_window")
        or mapping_row.get("lookback_window")
        or remediation_item.get("required_upstream_artifact")
        or "historical_window_ending_at_as_of_date"
    )
    date_text = as_of_date or "UNSPECIFIED_AS_OF_DATE"
    return {
        "schema_version": AS_OF_CONTRACT_METADATA_SCHEMA_VERSION,
        "as_of_contract_id": f"growth_tilt_engine:{feature_id}:as_of:v1",
        "feature_id": feature_id,
        "source_feature_name": remediation_item.get("source_feature_name") or feature_id,
        "as_of_date": date_text,
        "as_of_timestamp": f"{date_text}T00:00:00+00:00",
        "effective_date": "next_growth_tilt_engine_decision_after_as_of_date",
        "known_at": "after_source_rows_with_source_date_lte_as_of_date_are_available",
        "source_observed_at": "source_rows_with_source_date_lte_as_of_date",
        "feature_computed_at": "growth_tilt_engine_research_pipeline_for_as_of_date",
        "lookback_window": str(lookback_window),
        "lookahead_allowed": False,
        "forward_window_used": False,
        "pit_safe": "unknown",
        "pit_safe_reason": (
            "as-of semantics are explicit, but 2412 does not complete source "
            "snapshot traceability or PIT gate evidence"
        ),
        "as_of_semantics_status": (
            "ready" if as_of_remediation_status == "as_of_semantics_remediated" else "not_ready"
        ),
        "as_of_remediation_status": as_of_remediation_status,
        "as_of_semantics_contract": (
            "Feature inputs must be computed only from source rows with source_date <= "
            "as_of_date; forward/evaluation windows after as_of_date are forbidden."
        ),
        "no_lookahead_contract": {
            "input_date_filter": "source_date <= as_of_date",
            "effective_decision_boundary": "next_growth_tilt_engine_decision_after_as_of_date",
            "forbidden_inputs": [
                "future_returns_after_as_of_date",
                "future_drawdown_after_as_of_date",
                "post_signal_evaluation_window",
            ],
            "lookahead_allowed": False,
        },
        "source_traceability_status": "not_ready_missing_source_snapshot",
        "validity_dependency_status": "not_assessed_in_2412",
        "pit_gate_status": "blocked_pending_pit_evidence",
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
    source_mapping_result: Mapping[str, Any],
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
                    "mapping_status_before_2412": row.get("mapping_status"),
                    "mapping_status": after.get("mapping_status"),
                    "mapping_status_after_2412": after.get("mapping_status"),
                    "mapping_status_reasons": [
                        after.get("mapping_status_reason"),
                    ],
                    "as_of_semantics": after.get("as_of_semantics"),
                    "as_of_semantics_status": after.get("as_of_semantics_status"),
                    "as_of_remediation_status": record.get("as_of_remediation_status"),
                    "as_of_contract_metadata": after.get("as_of_contract_metadata"),
                    "source_traceability_status": after.get("source_traceability_status"),
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
        "source_task": source_mapping_result.get("task_id", "TRADING-2410"),
        "source_status": source_mapping_result.get("status"),
        "known_source_feature_count": len(updated_rows),
        "as_of_remediated_feature_count": len(remediation_records),
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


def validate_growth_tilt_as_of_semantics_remediation(
    remediation_records: list[Mapping[str, Any]],
    updated_mapping: Mapping[str, Any],
    *,
    expected_as_of_gap_count: int,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    seen_orders: set[int] = set()
    for record in remediation_records:
        feature_id = str(record.get("feature_id"))
        status = record.get("as_of_remediation_status")
        if status not in ALLOWED_AS_OF_REMEDIATION_STATUSES:
            errors.append(_error(feature_id, "UNCLASSIFIED_AS_OF_REMEDIATION_STATUS", str(status)))
        order = record.get("remediation_order")
        if not isinstance(order, int) or order <= 0 or order in seen_orders:
            errors.append(_error(feature_id, "INVALID_REMEDIATION_ORDER", str(order)))
        if isinstance(order, int):
            seen_orders.add(order)
        metadata = _as_mapping(_as_mapping(record.get("after")).get("as_of_contract_metadata"))
        if metadata.get("lookahead_allowed") is not False:
            errors.append(_error(feature_id, "LOOKAHEAD_NOT_DISABLED", "lookahead_allowed"))
        if metadata.get("forward_window_used") is not False:
            errors.append(_error(feature_id, "FORWARD_WINDOW_NOT_DISABLED", "forward_window_used"))
        if metadata.get("pit_safe") not in {True, False, "unknown"}:
            errors.append(
                _error(feature_id, "INVALID_PIT_SAFE_VALUE", str(metadata.get("pit_safe")))
            )
        if record.get("contract_ready") is True:
            errors.append(_error(feature_id, "CONTRACT_READY_SET_IN_2412", "contract_ready"))
    if len(remediation_records) != expected_as_of_gap_count:
        errors.append(
            _error(
                "growth_tilt_engine",
                "AS_OF_GAP_COUNT_MISMATCH",
                f"expected {expected_as_of_gap_count}, got {len(remediation_records)}",
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
        "schema_version": AS_OF_VALIDATION_SCHEMA_VERSION,
        "valid": not errors,
        "expected_as_of_gap_count": expected_as_of_gap_count,
        "as_of_remediation_record_count": len(remediation_records),
        "as_of_status_unclassified_count": sum(
            1
            for record in remediation_records
            if record.get("as_of_remediation_status")
            not in ALLOWED_AS_OF_REMEDIATION_STATUSES
        ),
        "lookahead_violation_count": sum(
            1 for record in remediation_records if record.get("lookahead_allowed") is not False
        ),
        "contract_ready_count": updated_mapping.get("contract_ready_count"),
        "error_count": len(errors),
        "errors": errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_remaining_blocker_summary(
    remediation_plan_result: Mapping[str, Any],
    remediation_records: list[Mapping[str, Any]],
    updated_mapping: Mapping[str, Any],
) -> dict[str, Any]:
    input_gap_count = _int_value(remediation_plan_result.get("gap_count"))
    return {
        "schema_version": "growth_tilt_engine_as_of_remaining_blocker_summary.v1",
        "engine_id": "growth_tilt_engine",
        "input_gap_count": input_gap_count,
        "as_of_remediated_count": len(
            [
                record
                for record in remediation_records
                if record.get("as_of_remediation_status") == "as_of_semantics_remediated"
            ]
        ),
        "remaining_blocked_or_gap_count": input_gap_count,
        "contract_ready_count": updated_mapping.get("contract_ready_count"),
        "remaining_blocking_reasons": [
            "source_traceability_remediation_not_completed_in_2412",
            "pit_gate_evidence_not_completed_in_2412",
            "valid_until_window_blocker_not_remediated_in_2412",
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


def _is_as_of_target(item: Mapping[str, Any]) -> bool:
    return item.get("remediation_category") == TARGET_REMEDIATION_CATEGORY or (
        item.get("missing_as_of_semantics") is True
    )


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
            item.get("remediation_order")
            if isinstance(item.get("remediation_order"), int)
            else 9999,
            str(item.get("feature_id", "")),
        ),
    )


def _mapping_rows(source_mapping_result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    direct_rows = _as_list(source_mapping_result.get("mapping_rows"))
    if direct_rows:
        return [_as_mapping(row) for row in direct_rows]
    mapping = _as_mapping(source_mapping_result.get("source_feature_contract_mapping"))
    return [_as_mapping(row) for row in _as_list(mapping.get("mapping_rows"))]


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
