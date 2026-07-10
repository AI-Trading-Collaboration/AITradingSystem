from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_replacement_candidate_contract.v1"
PREREQUISITE_SCHEMA_VERSION = "growth_tilt_replacement_candidate_prerequisite_matrix.v1"
DECISION_SCHEMA_VERSION = "growth_tilt_replacement_candidate_decision.v1"

REPORT_TYPE = "growth_tilt_replacement_candidate_contract"
READY_STATUS = "GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT_READY_BLOCKED"
BLOCKED_STATUS = "GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT_BLOCKED_SOURCE_CONTRACT"
NEXT_ROUTE = "TRADING-2438M1E_GROWTH_TILT_REPLACEMENT_CANDIDATE_CONTRACT"
REPLACEMENT_CANDIDATE_ID = "capped_recovery_permission_overlay"

EXPECTED_OWNER_SCHEMA = "growth_tilt_owner_decision_resolution.v1"
EXPECTED_OWNER_STATUS = "OWNER_DECISIONS_RESOLVED_WITH_EXPLICIT_BLOCKERS"
EXPECTED_ADAPTER_SCHEMA = "growth_tilt_baseline_contract_adapters_readiness.v1"
EXPECTED_ADAPTER_STATUS = "GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_READY_WITH_BLOCKERS"
EXPECTED_PREREQUISITE_IDS = (
    "replacement_identity_and_orthogonal_role_frozen",
    "recovery_output_semantic_type_known",
    "recovery_producer_pit_lineage_valid",
    "recovery_threshold_versioned_and_preregistered",
    "hard_veto_aggregate_contract_ready",
    "current_requested_applied_transition_contract_ready",
    "native_exposure_scalar_contract_ready",
    "candidate_cap_expressed_in_native_units",
    "screening_policy_frozen_before_result_visibility",
    "second_owner_approval_recorded",
)

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-replacement-candidate-contract",
    "growth_tilt_replacement_candidate_contract.json",
    "growth_tilt_replacement_candidate_prerequisite_matrix.json",
    "growth_tilt_replacement_candidate_decision.json",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438M1E",
    READY_STATUS,
    REPLACEMENT_CANDIDATE_ID,
    "KEEP_REDEFINED_BLOCKED",
    "approved candidate=0",
    "M2 eligible=0",
)


def build_growth_tilt_replacement_candidate_contract(
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
    adapter_readiness = _mapping(sources.get("adapter_readiness"))
    screening_policy = _mapping(sources.get("screening_policy"))
    decision_by_id = {
        str(item.get("decision_id")): item
        for item in _sequence(owner_resolution.get("decisions"))
        if isinstance(item, Mapping) and item.get("decision_id")
    }
    recovery = _mapping(adapter_readiness.get("recovery_permission_adapter"))
    hard_veto = _mapping(adapter_readiness.get("hard_veto_aggregate_adapter"))
    transition = _mapping(adapter_readiness.get("regime_transition_trace_adapter"))
    exposure = _mapping(adapter_readiness.get("native_exposure_scalar_adapter"))
    conditions = {
        "replacement_identity_and_orthogonal_role_frozen": _replacement_identity_ready(
            owner_resolution
        ),
        "recovery_output_semantic_type_known": recovery.get("semantic_type")
        == "UNSCALED_SCORE",
        "recovery_producer_pit_lineage_valid": recovery.get("status") == "READY"
        and not _sequence(recovery.get("missing_pit_lineage_fields")),
        "recovery_threshold_versioned_and_preregistered": recovery.get(
            "threshold_status"
        )
        == "APPROVED_PREREGISTERED",
        "hard_veto_aggregate_contract_ready": hard_veto.get("status") == "READY",
        "current_requested_applied_transition_contract_ready": transition.get("status")
        == "READY",
        "native_exposure_scalar_contract_ready": exposure.get("status") == "READY",
        "candidate_cap_expressed_in_native_units": exposure.get("status") == "READY"
        and decision_by_id.get("D18", {}).get("native_scalar_resolution")
        == "RESOLVED_GOVERNED_NATIVE_SCALAR",
        "screening_policy_frozen_before_result_visibility": _screening_policy_frozen(
            screening_policy
        ),
        "second_owner_approval_recorded": _second_owner_approval_recorded(
            owner_resolution
        ),
    }
    rows = [
        {
            "prerequisite_id": prerequisite_id,
            "status": "PASS" if conditions[prerequisite_id] else "BLOCKED",
            "ready": conditions[prerequisite_id],
            "blocker_code": None
            if conditions[prerequisite_id]
            else prerequisite_id.upper(),
        }
        for prerequisite_id in EXPECTED_PREREQUISITE_IDS
    ]
    all_ready = all(conditions.values())
    disposition = "APPROVE" if all_ready else "KEEP_REDEFINED_BLOCKED"
    approved_runtime_spec = _approved_runtime_spec(
        decision_by_id, adapter_readiness
    ) if all_ready else None
    approved_executor_binding = (
        {
            "executor_family": "GrowthTiltCandidateOverlayExecutor",
            "operation_type": "CAPPED_RECOVERY_PERMISSION_OVERLAY",
            "input_contract_version": "growth_tilt_candidate_overlay_input.v1",
            "output_contract_version": "growth_tilt_candidate_overlay_output.v1",
        }
        if all_ready
        else None
    )
    prerequisite_matrix = {
        "schema_version": PREREQUISITE_SCHEMA_VERSION,
        "replacement_candidate_id": REPLACEMENT_CANDIDATE_ID,
        "rows": rows,
        "prerequisite_count": len(rows),
        "pass_count": sum(item["status"] == "PASS" for item in rows),
        "blocked_count": sum(item["status"] == "BLOCKED" for item in rows),
        "all_prerequisites_ready": all_ready,
        "blocker_codes": [
            str(item["blocker_code"]) for item in rows if item["blocker_code"]
        ],
    }
    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "replacement_candidate_id": REPLACEMENT_CANDIDATE_ID,
        "disposition": disposition,
        "approval_status": "APPROVED_FOR_M2" if all_ready else "BLOCKED",
        "approved_runtime_spec_emitted": approved_runtime_spec is not None,
        "approved_executor_binding_emitted": approved_executor_binding is not None,
        "policy_approval_metadata_written": False,
        "post_replay_tuning_allowed": False,
        "m2_invocation_allowed": False,
        "next_required_action": (
            "OWNER_RECORDS_SECOND_APPROVAL_AFTER_ALL_CONTRACTS_AND_POLICY_ARE_PROVEN"
            if not all_ready
            else "SEPARATE_OWNER_APPROVAL_ARTIFACT_REQUIRED_BEFORE_M2_BINDING"
        ),
    }
    strict_errors = _strict_validation_errors(
        owner_resolution,
        adapter_readiness,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    status = BLOCKED_STATUS if strict_errors else READY_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1E",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "source_artifacts": [dict(item) for item in source_artifacts],
        "replacement_candidate_id": REPLACEMENT_CANDIDATE_ID,
        "prerequisite_matrix": prerequisite_matrix,
        "replacement_candidate_decision": decision,
        "approved_candidate_runtime_spec": approved_runtime_spec,
        "approved_executor_binding": approved_executor_binding,
        "disposition": disposition,
        "approval_prerequisites_ready": all_ready,
        "approved_candidate_count": int(all_ready),
        "m2_eligible_candidate_count": int(all_ready),
        "m2_eligible_candidate_ids": [REPLACEMENT_CANDIDATE_ID] if all_ready else [],
        "screening_policy_status": screening_policy.get("policy_status"),
        "screening_policy_approval_metadata_complete": _screening_policy_frozen(
            screening_policy
        ),
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "data_quality_gate_executed": False,
        "data_quality_status": "NOT_APPLICABLE_GOVERNANCE_ARTIFACT_AUDIT_ONLY",
        **_safety(),
    }


def _replacement_identity_ready(owner_resolution: Mapping[str, Any]) -> bool:
    candidates = _sequence(
        _mapping(owner_resolution.get("candidate_disposition")).get("candidates")
    )
    return any(
        isinstance(item, Mapping)
        and item.get("candidate_id") == "recovery_reentry_speedup_guard"
        and item.get("decision") == "REDEFINE"
        and item.get("replacement_candidate_id") == REPLACEMENT_CANDIDATE_ID
        for item in candidates
    )


def _approved_runtime_spec(
    decision_by_id: Mapping[str, Mapping[str, Any]],
    adapter_readiness: Mapping[str, Any],
) -> dict[str, Any]:
    d05 = decision_by_id.get("D05", {})
    d06 = decision_by_id.get("D06", {})
    d07 = decision_by_id.get("D07", {})
    d08 = decision_by_id.get("D08", {})
    d18 = decision_by_id.get("D18", {})
    hard_veto = _mapping(adapter_readiness.get("hard_veto_aggregate_adapter"))
    exposure = _mapping(adapter_readiness.get("native_exposure_scalar_adapter"))
    return {
        "schema_version": "growth_tilt_capped_recovery_permission_overlay_spec.v1",
        "candidate_id": REPLACEMENT_CANDIDATE_ID,
        "operation_type": "CAPPED_RECOVERY_PERMISSION_OVERLAY",
        "signal_id": "re_risk_allowed_probability",
        "semantic_type": "UNSCALED_SCORE",
        "candidate_required_consecutive_steps": d05.get(
            "candidate_required_consecutive_steps"
        ),
        "baseline_required_consecutive_steps": None,
        "reset_on_signal_false": d06.get("reset_on_signal_false"),
        "reset_on_missing": d06.get("reset_on_missing"),
        "reset_on_any_hard_veto": d06.get("reset_on_any_hard_veto"),
        "effective_time": d07.get("effective_time"),
        "same_step_application_allowed": d07.get("same_step_application_allowed"),
        "maximum_active_steps": d08.get("maximum_active_steps"),
        "auto_extension_allowed": d08.get("auto_extension_allowed"),
        "hard_veto_ids": hard_veto.get("required_component_ids"),
        "native_scalar_binding": exposure.get("binding"),
        "candidate_delta_cap_formula": d18.get("candidate_delta_cap_formula"),
        "changes_baseline_recovery_persistence": False,
        "changes_hard_veto_behavior": False,
        "tqqq_increase_allowed": False,
    }
def _screening_policy_frozen(policy: Mapping[str, Any]) -> bool:
    return bool(
        policy.get("policy_status") == "APPROVED"
        and policy.get("approved_at")
        and _hex_identifier(policy.get("approved_commit"), 40)
        and _hex_identifier(policy.get("source_hash"), 64)
        and policy.get("result_visibility_at_approval") == "NONE"
    )


def _second_owner_approval_recorded(owner_resolution: Mapping[str, Any]) -> bool:
    approval = _mapping(owner_resolution.get("replacement_candidate_second_approval"))
    return bool(
        approval.get("candidate_id") == REPLACEMENT_CANDIDATE_ID
        and approval.get("decision") == "APPROVE"
        and approval.get("owner")
        and approval.get("approved_at")
        and _hex_identifier(approval.get("source_hash"), 64)
    )


def _strict_validation_errors(
    owner_resolution: Mapping[str, Any],
    adapter_readiness: Mapping[str, Any],
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
    if adapter_readiness.get("schema_version") != EXPECTED_ADAPTER_SCHEMA:
        errors.append("adapter_readiness_schema_mismatch")
    if adapter_readiness.get("status") != EXPECTED_ADAPTER_STATUS:
        errors.append("adapter_readiness_status_mismatch")
    if adapter_readiness.get("m2_eligible_candidate_count") != 0:
        errors.append("adapter_readiness_m2_count_mismatch")
    if adapter_readiness.get("strict_validation_error_count", 0) != 0:
        errors.append("adapter_readiness_strict_errors_present")
    if adapter_readiness.get("source_validation_error_count", 0) != 0:
        errors.append("adapter_readiness_source_errors_present")
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
            "TRADING-2438M1E",
            "replacement A approved only if all prerequisites are proven",
            "screening policy frozen",
            "M2 eligible becomes 1, or remains 0 with exact blocker",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _safety() -> dict[str, Any]:
    return {
        "governance_gate_only": True,
        "runtime_code_invoked": False,
        "replay_run": False,
        "runtime_metrics_generated": False,
        "candidate_behavior_implemented": False,
        "baseline_behavior_changed": False,
        "policy_approval_fabricated": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _hex_identifier(value: Any, length: int) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
