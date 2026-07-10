from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

SCHEMA_VERSION = "growth_tilt_baseline_contract_adapters_readiness.v1"
HARD_VETO_SCHEMA_VERSION = "growth_tilt_hard_veto_aggregate_adapter.v1"
TRANSITION_SCHEMA_VERSION = "growth_tilt_regime_transition_trace_adapter.v1"
EXPOSURE_SCHEMA_VERSION = "growth_tilt_native_exposure_scalar_adapter.v1"
RECOVERY_SCHEMA_VERSION = "growth_tilt_recovery_permission_adapter.v1"

REPORT_TYPE = "growth_tilt_baseline_contract_adapters_readiness"
READY_STATUS = "GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_READY_WITH_BLOCKERS"
BLOCKED_STATUS = "GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_BLOCKED_SOURCE_CONTRACT"
NEXT_ROUTE = "TRADING-2438M1E_GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT"

EXPECTED_OWNER_SCHEMA = "growth_tilt_owner_decision_resolution.v1"
EXPECTED_OWNER_STATUS = "OWNER_DECISIONS_RESOLVED_WITH_EXPLICIT_BLOCKERS"
EXPECTED_VETO_IDS = (
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto",
)
EXPECTED_TREND_STATES = (
    "risk_off",
    "defensive",
    "neutral",
    "constructive",
    "risk_on",
)
TRANSITION_PRIORITY = (
    "INVALID_PIT_OR_DATA_CONTRACT_BLOCKED",
    "HARD_VETO_OR_EMERGENCY_RISK_REQUEST",
    "BASELINE_MANDATORY_DEFENSIVE_TRANSITION",
    "BASELINE_ORDINARY_TRANSITION_REQUEST",
    "APPROVED_RECOVERY_OVERLAY_REQUEST",
    "EXPOSURE_OR_RISK_CAP_CLAMP",
)
PIT_LINEAGE_FIELDS = ("as_of", "known_at", "available_at", "source_data_cutoff")

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-baseline-contract-adapters-readiness",
    "growth_tilt_baseline_contract_adapters_readiness.json",
    "growth_tilt_hard_veto_aggregate_adapter.json",
    "growth_tilt_regime_transition_trace_adapter.json",
    "growth_tilt_native_exposure_scalar_adapter.json",
    "growth_tilt_recovery_permission_adapter.json",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438M1D2",
    READY_STATUS,
    "hard-veto aggregate adapter",
    "requested/applied",
    "BLOCKED_NO_GOVERNED_NATIVE_SCALAR",
    "M2 eligible=0",
)


def build_hard_veto_aggregate_adapter(
    component_contracts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_id = {
        str(item.get("veto_id")): item
        for item in component_contracts
        if item.get("veto_id")
    }
    input_ids = [str(item.get("veto_id")) for item in component_contracts]
    blockers: list[str] = []
    if tuple(input_ids) != EXPECTED_VETO_IDS:
        blockers.append("HARD_VETO_COMPONENT_IDENTITY_OR_ORDER_MISMATCH")
    rows: list[dict[str, Any]] = []
    for veto_id in EXPECTED_VETO_IDS:
        source = by_id.get(veto_id, {})
        resolution_status = str(
            source.get("resolution_status") or "BLOCKED_MISSING_COMPONENT_CONTRACT"
        )
        row_blockers: list[str] = []
        not_applicable = resolution_status.startswith("EXPLICITLY_NOT_APPLICABLE")
        callable_resolution = resolution_status == "RESOLVED_CALLABLE"
        if not callable_resolution and not not_applicable:
            row_blockers.append(f"UNRESOLVED_HARD_VETO:{veto_id}:{resolution_status}")
        if callable_resolution:
            if source.get("producer_callable") is not True:
                row_blockers.append(f"HARD_VETO_PRODUCER_NOT_CALLABLE:{veto_id}")
            if not source.get("output_path"):
                row_blockers.append(f"HARD_VETO_OUTPUT_PATH_MISSING:{veto_id}")
            if not source.get("pit_lineage_ref"):
                row_blockers.append(f"HARD_VETO_PIT_LINEAGE_MISSING:{veto_id}")
        if not_applicable and not source.get("not_applicable_rationale"):
            row_blockers.append(f"HARD_VETO_NOT_APPLICABLE_EVIDENCE_MISSING:{veto_id}")
        if source.get("missing_policy") != "BLOCKED_NOT_FALSE":
            row_blockers.append(f"HARD_VETO_MISSING_POLICY_INVALID:{veto_id}")
        if source.get("priority") != "BEFORE_CANDIDATE_OVERLAY":
            row_blockers.append(f"HARD_VETO_PRIORITY_INVALID:{veto_id}")
        blockers.extend(row_blockers)
        rows.append(
            {
                "veto_id": veto_id,
                "resolution_status": resolution_status,
                "producer_callable": source.get("producer_callable"),
                "producer_entrypoint": source.get("producer_entrypoint"),
                "output_path": source.get("output_path"),
                "pit_lineage_ref": source.get("pit_lineage_ref"),
                "missing_policy": "BLOCKED_NOT_FALSE",
                "priority": "BEFORE_CANDIDATE_OVERLAY",
                "contract_ready": not row_blockers,
                "blocker_codes": sorted(set(row_blockers)),
            }
        )
    blockers = sorted(set(blockers))
    return {
        "schema_version": HARD_VETO_SCHEMA_VERSION,
        "status": "READY" if not blockers else "BLOCKED_UNRESOLVED_HARD_VETO_AGGREGATE",
        "required_component_ids": list(EXPECTED_VETO_IDS),
        "components": rows,
        "resolved_component_ids": [
            item["veto_id"] for item in rows if item["contract_ready"]
        ],
        "unresolved_component_ids": [
            item["veto_id"] for item in rows if not item["contract_ready"]
        ],
        "aggregate_materializable": not blockers,
        "missing_component_policy": "BLOCKED_NOT_FALSE",
        "raw_indicator_inputs_allowed": False,
        "candidate_component_removal_allowed": False,
        "candidate_priority_change_allowed": False,
        "blocker_codes": blockers,
    }


def materialize_hard_veto_aggregate(
    adapter: Mapping[str, Any], component_values: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    if adapter.get("aggregate_materializable") is not True:
        return _blocked_hard_veto_trace("BLOCKED_COMPONENT_CONTRACT")
    expected_callable_ids = [
        str(item.get("veto_id"))
        for item in _sequence(adapter.get("components"))
        if isinstance(item, Mapping)
        and item.get("resolution_status") == "RESOLVED_CALLABLE"
    ]
    blockers: list[str] = []
    active_ids: list[str] = []
    values: list[dict[str, Any]] = []
    if set(component_values) - set(expected_callable_ids):
        blockers.append("UNKNOWN_OR_NON_CALLABLE_HARD_VETO_VALUE")
    for veto_id in expected_callable_ids:
        source = _mapping(component_values.get(veto_id))
        value = source.get("value")
        if not isinstance(value, bool):
            blockers.append(f"HARD_VETO_RUNTIME_VALUE_MISSING:{veto_id}")
        if source.get("pit_valid") is not True:
            blockers.append(f"HARD_VETO_RUNTIME_PIT_INVALID:{veto_id}")
        if not all(source.get(field) for field in ("known_at", "available_at")):
            blockers.append(f"HARD_VETO_RUNTIME_LINEAGE_MISSING:{veto_id}")
        if value is True:
            active_ids.append(veto_id)
        values.append(
            {
                "veto_id": veto_id,
                "value": value if isinstance(value, bool) else None,
                "pit_valid": source.get("pit_valid") is True,
                "known_at": source.get("known_at"),
                "available_at": source.get("available_at"),
                "source_artifact_ref": source.get("source_artifact_ref"),
            }
        )
    blockers = sorted(set(blockers))
    return {
        "schema_version": "growth_tilt_hard_veto_aggregate_trace.v1",
        "status": "READY" if not blockers else "BLOCKED",
        "active": bool(active_ids) if not blockers else None,
        "active_component_ids": active_ids if not blockers else [],
        "component_values": values,
        "missing_component_policy": "BLOCKED_NOT_FALSE",
        "candidate_component_removal_allowed": False,
        "blocker_codes": blockers,
    }


def materialize_regime_transition_trace(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    aggregate_blockers: list[str] = []
    for index, row in enumerate(rows):
        blockers: list[str] = []
        current_state = row.get("current_state", row.get("trend_state"))
        requested_state = row.get("requested_target_state")
        applied_state = row.get("applied_target_state")
        if current_state not in EXPECTED_TREND_STATES:
            blockers.append("CURRENT_STATE_INVALID_OR_MISSING")
        if requested_state not in EXPECTED_TREND_STATES:
            blockers.append("REQUESTED_TARGET_STATE_MISSING")
        if applied_state not in EXPECTED_TREND_STATES:
            blockers.append("APPLIED_TARGET_STATE_MISSING")
        request_created_at = row.get("request_created_at")
        applied_at = row.get("applied_at")
        if not all(row.get(field) for field in ("known_at", "available_at")):
            blockers.append("TRANSITION_PIT_LINEAGE_MISSING")
        if requested_state in EXPECTED_TREND_STATES and not request_created_at:
            blockers.append("REQUEST_CREATED_AT_MISSING")
        if applied_state in EXPECTED_TREND_STATES and not applied_at:
            blockers.append("APPLIED_AT_MISSING")
        if request_created_at and applied_at:
            try:
                same_step = datetime.fromisoformat(
                    str(applied_at)
                ) <= datetime.fromisoformat(str(request_created_at))
            except ValueError:
                same_step = True
                blockers.append("TRANSITION_TIME_INVALID")
            if same_step:
                blockers.append("SAME_STEP_APPLICATION_PROHIBITED")
        blockers = sorted(set(blockers))
        aggregate_blockers.extend(blockers)
        records.append(
            {
                "trace_index": index,
                "source_date": row.get("date"),
                "current_state": current_state,
                "requested_target_state": requested_state,
                "applied_target_state": applied_state,
                "known_at": row.get("known_at"),
                "available_at": row.get("available_at"),
                "request_created_at": request_created_at,
                "applied_at": applied_at,
                "status": "READY" if not blockers else "BLOCKED",
                "blocker_codes": blockers,
            }
        )
    aggregate_blockers = sorted(set(aggregate_blockers))
    return {
        "schema_version": TRANSITION_SCHEMA_VERSION,
        "status": "READY" if records and not aggregate_blockers else "BLOCKED",
        "record_count": len(records),
        "ready_record_count": sum(item["status"] == "READY" for item in records),
        "blocked_record_count": sum(item["status"] == "BLOCKED" for item in records),
        "records": records,
        "ordered_priority": list(TRANSITION_PRIORITY),
        "same_step_application_allowed": False,
        "used_adjacent_row_inference": False,
        "candidate_may_supersede_baseline_defensive_request": False,
        "blocker_codes": aggregate_blockers or (["NO_TRANSITION_ROWS"] if not records else []),
    }


def build_native_exposure_scalar_adapter(binding: Mapping[str, Any]) -> dict[str, Any]:
    required_fields = (
        "native_scalar_id",
        "unit",
        "current_scalar_field",
        "requested_target_scalar_field",
        "applied_target_scalar_field",
        "minimum_value",
        "maximum_value",
        "minimum_increment",
        "pit_lineage_ref",
        "owner_semantics_status",
    )
    blockers = [
        f"NATIVE_SCALAR_FIELD_MISSING:{field}"
        for field in required_fields
        if binding.get(field) is None or binding.get(field) == ""
    ]
    if binding.get("owner_semantics_status") != "APPROVED":
        blockers.append("NATIVE_SCALAR_OWNER_SEMANTICS_NOT_APPROVED")
    minimum = binding.get("minimum_value")
    maximum = binding.get("maximum_value")
    increment = binding.get("minimum_increment")
    if not (_finite_number(minimum) and _finite_number(maximum)):
        blockers.append("NATIVE_SCALAR_RANGE_NOT_FINITE")
    elif float(minimum) >= float(maximum):
        blockers.append("NATIVE_SCALAR_RANGE_INVALID")
    if not (_finite_number(increment) and float(increment) > 0):
        blockers.append("NATIVE_SCALAR_INCREMENT_INVALID")
    identity_text = " ".join(
        str(binding.get(field) or "")
        for field in ("native_scalar_id", "unit", "current_scalar_field")
    ).upper()
    if "QQQ" in identity_text or "TQQQ" in identity_text:
        blockers.append("INSTRUMENT_NAME_SCALAR_SUBSTITUTION_PROHIBITED")
    blockers = sorted(set(blockers))
    return {
        "schema_version": EXPOSURE_SCHEMA_VERSION,
        "status": "READY" if not blockers else "BLOCKED_NO_GOVERNED_NATIVE_SCALAR",
        "binding": {field: binding.get(field) for field in required_fields},
        "scalar_binding_ready": not blockers,
        "qqq_equivalent_candidate_delta_allowed": False,
        "tqqq_increase_allowed": False,
        "instrument_name_multiplier_inference_allowed": False,
        "candidate_delta_materialized": False,
        "blocker_codes": blockers,
    }


def materialize_native_exposure_scalar_trace(
    adapter: Mapping[str, Any], row: Mapping[str, Any]
) -> dict[str, Any]:
    if adapter.get("scalar_binding_ready") is not True:
        return {
            "schema_version": "growth_tilt_native_exposure_scalar_trace.v1",
            "status": "BLOCKED_NO_GOVERNED_NATIVE_SCALAR",
            "current_scalar": None,
            "requested_target_scalar": None,
            "applied_target_scalar": None,
            "blocker_codes": ["BLOCKED_NO_GOVERNED_NATIVE_SCALAR"],
        }
    binding = _mapping(adapter.get("binding"))
    values = {
        "current_scalar": row.get(str(binding.get("current_scalar_field"))),
        "requested_target_scalar": row.get(
            str(binding.get("requested_target_scalar_field"))
        ),
        "applied_target_scalar": row.get(str(binding.get("applied_target_scalar_field"))),
    }
    blockers = [
        f"SCALAR_VALUE_INVALID:{field}"
        for field, value in values.items()
        if not _finite_number(value)
    ]
    if row.get("pit_valid") is not True:
        blockers.append("SCALAR_TRACE_PIT_INVALID")
    if not all(row.get(field) for field in ("known_at", "available_at")):
        blockers.append("SCALAR_TRACE_LINEAGE_MISSING")
    if not row.get("source_artifact_ref"):
        blockers.append("SCALAR_TRACE_SOURCE_REF_MISSING")
    if not blockers:
        minimum = float(binding["minimum_value"])
        maximum = float(binding["maximum_value"])
        for field, value in values.items():
            if not minimum <= float(value) <= maximum:
                blockers.append(f"SCALAR_VALUE_OUT_OF_RANGE:{field}")
    blockers = sorted(set(blockers))
    return {
        "schema_version": "growth_tilt_native_exposure_scalar_trace.v1",
        "status": "READY" if not blockers else "BLOCKED",
        **values,
        "unit": binding.get("unit"),
        "blocker_codes": blockers,
    }


def build_recovery_permission_adapter(
    producer: Mapping[str, Any],
    *,
    prediction_header_fields: Sequence[str],
    threshold_decision: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if producer.get("signal_id") != "re_risk_allowed_probability":
        blockers.append("RECOVERY_SIGNAL_ID_MISMATCH")
    if producer.get("producer_callable") is not True:
        blockers.append("RECOVERY_PRODUCER_NOT_CALLABLE")
    if not producer.get("output_path"):
        blockers.append("RECOVERY_OUTPUT_PATH_MISSING")
    semantic_type = "UNSCALED_SCORE"
    missing_pit_fields = sorted(set(PIT_LINEAGE_FIELDS) - set(prediction_header_fields))
    if missing_pit_fields or producer.get("pit_lineage_valid") is not True:
        blockers.append("RECOVERY_PIT_LINEAGE_INVALID_OR_MISSING")
    threshold_status = threshold_decision.get("threshold_status")
    if threshold_status != "APPROVED_PREREGISTERED":
        blockers.append("RECOVERY_THRESHOLD_NOT_APPROVED")
    blockers = sorted(set(blockers))
    return {
        "schema_version": RECOVERY_SCHEMA_VERSION,
        "status": "READY" if not blockers else "BLOCKED",
        "signal_id": "re_risk_allowed_probability",
        "producer_callable": producer.get("producer_callable") is True,
        "output_path": producer.get("output_path"),
        "semantic_type": semantic_type,
        "probability_interpretation_allowed": False,
        "prediction_header_fields": list(prediction_header_fields),
        "required_pit_lineage_fields": list(PIT_LINEAGE_FIELDS),
        "missing_pit_lineage_fields": missing_pit_fields,
        "threshold_status": threshold_status,
        "default_threshold_allowed": False,
        "existing_pilot_threshold_reused": False,
        "trigger_materialized": False,
        "baseline_transition_emitted": False,
        "baseline_recovery_persistence_created": False,
        "blocker_codes": blockers,
    }


def build_growth_tilt_baseline_contract_adapters_readiness(
    sources: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    as_of: str,
) -> dict[str, Any]:
    owner_resolution = _mapping(sources.get("owner_resolution"))
    hard_veto_source = _unwrap_section(
        sources.get("hard_veto_matrix"), "hard_veto_resolution_matrix"
    )
    signal_source = _unwrap_section(sources.get("signal_inventory"), "baseline_signal_inventory")
    exposure_source = _unwrap_section(
        sources.get("exposure_inventory"), "baseline_exposure_unit_inventory"
    )
    hard_veto = build_hard_veto_aggregate_adapter(
        _hard_veto_components_after_owner_resolution(hard_veto_source, owner_resolution)
    )
    transition = materialize_regime_transition_trace(
        [
            item
            for item in _sequence(sources.get("transition_rows"))
            if isinstance(item, Mapping)
        ]
    )
    exposure = build_native_exposure_scalar_adapter(
        _native_scalar_binding_from_inventory(exposure_source)
    )
    recovery_signal = next(
        (
            item
            for item in _sequence(signal_source.get("signals"))
            if isinstance(item, Mapping)
            and item.get("signal_id") == "re_risk_allowed_probability"
        ),
        {},
    )
    threshold_decision = _decision_by_id(owner_resolution, "D04")
    recovery = build_recovery_permission_adapter(
        _mapping(recovery_signal),
        prediction_header_fields=[
            str(item) for item in _sequence(sources.get("prediction_header_fields"))
        ],
        threshold_decision=threshold_decision,
    )
    sections = {
        "hard_veto_aggregate_adapter": hard_veto,
        "regime_transition_trace_adapter": transition,
        "native_exposure_scalar_adapter": exposure,
        "recovery_permission_adapter": recovery,
    }
    strict_errors = _strict_validation_errors(
        owner_resolution,
        sources,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    adapter_ready = {
        key: value.get("status") == "READY" for key, value in sections.items()
    }
    blocker_codes = sorted(
        {
            str(code)
            for value in sections.values()
            for code in _sequence(value.get("blocker_codes"))
        }
    )
    status = BLOCKED_STATUS if strict_errors else READY_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1D2",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "source_artifacts": [dict(item) for item in source_artifacts],
        **sections,
        "adapter_implementation_count": 4,
        "adapter_contract_ready_count": sum(adapter_ready.values()),
        "adapter_contract_blocked_count": sum(not ready for ready in adapter_ready.values()),
        "adapter_contract_readiness": adapter_ready,
        "blocker_codes": blocker_codes,
        "blocker_count": len(blocker_codes),
        "existing_baseline_behavior_exposed": True,
        "new_baseline_behavior_introduced": False,
        "candidate_behavior_implemented": False,
        "replacement_a_ready_for_m1e_approval": all(adapter_ready.values()),
        "approved_candidate_count": 0,
        "m2_eligible_candidate_count": 0,
        "m2_eligible_candidate_ids": [],
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "data_quality_gate_executed": False,
        "data_quality_status": "NOT_APPLICABLE_EXISTING_CONTRACT_ARTIFACT_AUDIT_ONLY",
        **_safety(),
    }


def _blocked_hard_veto_trace(code: str) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_hard_veto_aggregate_trace.v1",
        "status": "BLOCKED",
        "active": None,
        "active_component_ids": [],
        "component_values": [],
        "missing_component_policy": "BLOCKED_NOT_FALSE",
        "candidate_component_removal_allowed": False,
        "blocker_codes": [code],
    }


def _native_scalar_binding_from_inventory(
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    exposure = _mapping(inventory.get("exposure"))
    if exposure.get("scalar_binding_ready") is not True:
        return {}
    return {
        "native_scalar_id": exposure.get("native_scalar_id"),
        "unit": exposure.get("unit"),
        "current_scalar_field": exposure.get("current_scalar_field"),
        "requested_target_scalar_field": exposure.get("requested_target_scalar_field"),
        "applied_target_scalar_field": exposure.get("applied_target_scalar_field"),
        "minimum_value": exposure.get("minimum_value"),
        "maximum_value": exposure.get("maximum_value"),
        "minimum_increment": exposure.get("minimum_increment"),
        "pit_lineage_ref": exposure.get("pit_lineage_ref"),
        "owner_semantics_status": exposure.get("owner_semantics_status"),
    }


def _hard_veto_components_after_owner_resolution(
    matrix: Mapping[str, Any], owner_resolution: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    decision_by_veto_id = {
        "risk_off_veto": _decision_by_id(owner_resolution, "D11").get(
            "current_resolution"
        ),
        "trend_break_veto": _decision_by_id(owner_resolution, "D12").get(
            "current_resolution"
        ),
        "event_risk_veto": _decision_by_id(owner_resolution, "D13").get(
            "current_resolution"
        ),
    }
    rows: list[Mapping[str, Any]] = []
    for item in _sequence(matrix.get("components")):
        if not isinstance(item, Mapping):
            continue
        row = dict(item)
        veto_id = str(row.get("veto_id") or "")
        if decision_by_veto_id.get(veto_id):
            row["resolution_status"] = decision_by_veto_id[veto_id]
        rows.append(row)
    return rows


def _strict_validation_errors(
    owner_resolution: Mapping[str, Any],
    sources: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> list[str]:
    errors: list[str] = []
    if owner_resolution.get("schema_version") != EXPECTED_OWNER_SCHEMA:
        errors.append("owner_resolution_schema_mismatch")
    if owner_resolution.get("status") != EXPECTED_OWNER_STATUS:
        errors.append("owner_resolution_status_mismatch")
    if (
        owner_resolution.get("decision_count") != 18
        or owner_resolution.get("resolved_decision_count") != 18
    ):
        errors.append("owner_resolution_decision_count_mismatch")
    candidate_source = _mapping(owner_resolution.get("candidate_disposition"))
    if (
        candidate_source.get("approved_candidate_count"),
        candidate_source.get("redefine_candidate_count"),
        candidate_source.get("withdraw_candidate_count"),
    ) != (0, 2, 1):
        errors.append("owner_candidate_disposition_mismatch")
    if not _sequence(sources.get("transition_rows")):
        errors.append("transition_source_rows_missing")
    hard_veto = _unwrap_section(
        sources.get("hard_veto_matrix"), "hard_veto_resolution_matrix"
    )
    if hard_veto.get("schema_version") != "growth_tilt_hard_veto_resolution_matrix.v1":
        errors.append("hard_veto_matrix_schema_mismatch")
    signal = _unwrap_section(sources.get("signal_inventory"), "baseline_signal_inventory")
    if signal.get("schema_version") != "growth_tilt_baseline_signal_inventory.v1":
        errors.append("signal_inventory_schema_mismatch")
    exposure = _unwrap_section(
        sources.get("exposure_inventory"), "baseline_exposure_unit_inventory"
    )
    if exposure.get("schema_version") != "growth_tilt_baseline_exposure_unit_inventory.v1":
        errors.append("exposure_inventory_schema_mismatch")
    transition_rows = _sequence(sources.get("transition_rows"))
    if any(
        not isinstance(item, Mapping)
        or item.get("schema_version") != "growth_tilt_baseline_transition_trace_source.v1"
        for item in transition_rows
    ):
        errors.append("transition_source_schema_mismatch")
    prediction_fields = {
        str(item) for item in _sequence(sources.get("prediction_header_fields"))
    }
    if "re_risk_allowed_probability" not in prediction_fields:
        errors.append("recovery_prediction_field_missing")
    report_ids = {
        str(item.get("report_id"))
        for item in _sequence(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    if REPORT_TYPE not in report_ids:
        errors.append("report_registry_alignment_failed")
    if not all(item in artifact_catalog_text for item in REQUIRED_CATALOG_REFERENCES):
        errors.append("artifact_catalog_alignment_failed")
    if not all(item in system_flow_text for item in REQUIRED_FLOW_REFERENCES):
        errors.append("system_flow_alignment_failed")
    if not all(
        item in requirement_text
        for item in (
            "TRADING-2438M1D2",
            "No real PIT replay runs in M1D2",
            "existing baseline behavior exposed through governed adapters",
            "no new baseline decision behavior introduced",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _decision_by_id(owner_resolution: Mapping[str, Any], decision_id: str) -> Mapping[str, Any]:
    return next(
        (
            item
            for item in _sequence(owner_resolution.get("decisions"))
            if isinstance(item, Mapping) and item.get("decision_id") == decision_id
        ),
        {},
    )


def _unwrap_section(value: Any, section_name: str) -> Mapping[str, Any]:
    root = _mapping(value)
    return _mapping(root.get(section_name)) or root


def _safety() -> dict[str, Any]:
    return {
        "adapter_only": True,
        "runtime_code_invoked": False,
        "replay_run": False,
        "runtime_metrics_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "threshold_values_changed": False,
        "baseline_recovery_persistence_created": False,
        "baseline_transition_behavior_changed": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
