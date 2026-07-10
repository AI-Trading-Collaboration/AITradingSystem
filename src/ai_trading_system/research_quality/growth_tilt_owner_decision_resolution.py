from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_owner_decision_resolution_report.v1"
CANDIDATE_SCHEMA_VERSION = "growth_tilt_candidate_disposition_after_owner_resolution.v1"
ADAPTER_SCOPE_SCHEMA_VERSION = "growth_tilt_m1d2_adapter_scope.v1"
REPLACEMENT_READINESS_SCHEMA_VERSION = "growth_tilt_replacement_a_readiness.v1"

REPORT_TYPE = "growth_tilt_owner_decision_resolution"
READY_STATUS = "GROWTH_TILT_OWNER_DECISION_RESOLUTION_READY_WITH_BLOCKERS"
BLOCKED_STATUS = "GROWTH_TILT_OWNER_DECISION_RESOLUTION_BLOCKED_SOURCE_CONTRACT"
NEXT_ROUTE = "TRADING-2438M1D2_GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_AND_READINESS"
EXPECTED_SOURCE_SCHEMA = "growth_tilt_owner_decision_resolution.v1"
EXPECTED_SOURCE_STATUS = "OWNER_DECISIONS_RESOLVED_WITH_EXPLICIT_BLOCKERS"
EXPECTED_DECISION_IDS = tuple(f"D{index:02d}" for index in range(1, 19))
EXPECTED_CANDIDATE_IDS = (
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator",
)
EXPECTED_CANDIDATE_DECISIONS = ("REDEFINE", "WITHDRAW", "REDEFINE")
EXPECTED_BLOCKING_DECISION_IDS = ("D02", "D04", "D11", "D12", "D13", "D15", "D18")

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-owner-decision-resolution",
    "growth_tilt_owner_decision_resolution.json",
    "growth_tilt_candidate_disposition_after_owner_resolution.json",
    "growth_tilt_m1d2_adapter_scope.json",
    "growth_tilt_replacement_a_readiness.json",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438M1D1A",
    READY_STATUS,
    "capped_recovery_permission_overlay",
    "approved/redefine/withdraw=0/2/1",
    "M2 eligible=0",
)


def build_growth_tilt_owner_decision_resolution(
    sources: Mapping[str, Any],
    *,
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    as_of: str,
) -> dict[str, Any]:
    resolution = _mapping(sources.get("owner_resolution"))
    decisions = [
        dict(item)
        for item in _sequence(resolution.get("decisions"))
        if isinstance(item, Mapping)
    ]
    decision_by_id = {
        str(item.get("decision_id")): item for item in decisions if item.get("decision_id")
    }
    channel_code = str(sources.get("channel_code_text") or "")
    compiler_code = str(sources.get("compiler_code_text") or "")
    channel_prediction_header = str(sources.get("channel_prediction_header") or "")
    threshold_policy = _mapping(sources.get("threshold_policy"))
    evidence = _evidence_checks(
        decision_by_id,
        channel_code,
        compiler_code,
        channel_prediction_header,
        threshold_policy,
    )
    strict_errors = _strict_validation_errors(
        sources,
        evidence,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    candidate_section = _candidate_disposition(resolution)
    adapter_scope = _adapter_scope(resolution)
    replacement_readiness = _replacement_readiness(decision_by_id, threshold_policy)
    blocking_ids = [
        str(item.get("decision_id"))
        for item in decisions
        if item.get("status") == "RESOLVED_BLOCKED"
    ]
    status = BLOCKED_STATUS if strict_errors else READY_STATUS
    safety = _safety()
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1D1A",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "base_commit": resolution.get("base_commit"),
        "market_regime": "ai_after_chatgpt",
        "source_artifacts": [dict(item) for item in source_artifacts],
        "decision_count": len(decisions),
        "resolved_decision_count": sum(
            str(item.get("status", "")).startswith("RESOLVED") for item in decisions
        ),
        "blocking_decision_count": len(blocking_ids),
        "blocking_decision_ids": blocking_ids,
        "decision_matrix": decisions,
        "evidence_validation": evidence,
        "candidate_disposition_after_owner_resolution": {
            "schema_version": CANDIDATE_SCHEMA_VERSION,
            "status": status,
            **candidate_section,
        },
        "m1d2_adapter_scope": {
            "schema_version": ADAPTER_SCOPE_SCHEMA_VERSION,
            "status": status,
            **adapter_scope,
        },
        "replacement_a_readiness": {
            "schema_version": REPLACEMENT_READINESS_SCHEMA_VERSION,
            "status": status,
            **replacement_readiness,
        },
        "owner_decisions_complete": len(decisions) == 18
        and all(str(item.get("status", "")).startswith("RESOLVED") for item in decisions),
        "m1d2_adapter_implementation_allowed": not strict_errors,
        "candidate_behavior_implementation_allowed": False,
        "approved_candidate_count": 0,
        "redefine_candidate_count": 2,
        "withdraw_candidate_count": 1,
        "m2_eligible_candidate_count": 0,
        "m2_eligible_candidate_ids": [],
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "data_quality_gate_executed": False,
        "data_quality_status": "NOT_APPLICABLE_OWNER_DECISION_AND_CODE_AUDIT_ONLY",
        **safety,
    }


def _evidence_checks(
    decisions: Mapping[str, Mapping[str, Any]],
    channel_code: str,
    compiler_code: str,
    channel_prediction_header: str,
    threshold_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    header_fields = {item.strip() for item in channel_prediction_header.split(",") if item}
    pit_fields = {"as_of", "known_at", "available_at", "source_data_cutoff"}
    checks = [
        _check(
            "D02_PIT_LINEAGE_BLOCKED",
            not pit_fields.intersection(header_fields),
            {"header_fields": sorted(header_fields), "missing_pit_fields": sorted(pit_fields)},
        ),
        _check(
            "D03_UNSCALED_SCORE_EVIDENCED",
            'frame["re_risk_allowed_probability"] = frame["do_not_de_risk_probability"]'
            in channel_code
            and 'frame["do_not_de_risk_probability"] = frame["drawdown_recovery_score"].clip'
            in channel_code
            and decisions.get("D03", {}).get("semantic_type") == "UNSCALED_SCORE",
            {},
        ),
        _check(
            "D04_THRESHOLD_NOT_APPROVED",
            decisions.get("D04", {}).get("threshold_status")
            == "BLOCKED_UNCALIBRATED_SCORE"
            and decisions.get("D04", {}).get("default_0_5_allowed") is False,
            {"screening_policy_status": threshold_policy.get("policy_status")},
        ),
        _check(
            "D11_RISK_OFF_ALIAS_BLOCKED",
            '"risk_off_veto": row.get("growth_allowed") is False' in channel_code
            and decisions.get("D11", {}).get("current_resolution")
            == "BLOCKED_AMBIGUOUS_GROWTH_ALLOWED_ALIAS",
            {},
        ),
        _check(
            "D12_TREND_BREAK_PRODUCER_MISSING",
            '"trend_break_veto":' not in channel_code
            and decisions.get("D12", {}).get("current_resolution")
            == "BLOCKED_NO_CALLABLE_PRODUCER",
            {},
        ),
        _check(
            "D13_EVENT_RISK_PRODUCER_MISSING",
            '"event_risk_veto":' not in channel_code
            and decisions.get("D13", {}).get("current_resolution")
            == "BLOCKED_NO_PIT_CONTRACT",
            {},
        ),
        _check(
            "D15_REQUESTED_APPLIED_FIELDS_MISSING",
            not all(
                field in compiler_code
                for field in ("current_state", "requested_target_state", "applied_target_state")
            )
            and decisions.get("D15", {}).get("current_resolution")
            == "BLOCKED_NO_GOVERNED_REQUESTED_APPLIED_FIELDS",
            {},
        ),
        _check(
            "D18_NATIVE_SCALAR_MISSING",
            decisions.get("D18", {}).get("native_scalar_resolution")
            == "BLOCKED_NO_GOVERNED_NATIVE_SCALAR"
            and decisions.get("D18", {}).get("qqq_equivalent_unit_allowed") is False,
            {},
        ),
    ]
    return checks


def _check(check_id: str, passed: bool, evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "evidence": dict(evidence),
    }


def _candidate_disposition(resolution: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(resolution.get("candidate_disposition"))
    candidates = [
        dict(item)
        for item in _sequence(source.get("candidates"))
        if isinstance(item, Mapping)
    ]
    return {
        "approved_candidate_count": source.get("approved_candidate_count"),
        "redefine_candidate_count": source.get("redefine_candidate_count"),
        "withdraw_candidate_count": source.get("withdraw_candidate_count"),
        "m2_eligible_candidate_count": 0,
        "m2_eligible_candidate_ids": [],
        "candidates": candidates,
    }


def _adapter_scope(resolution: Mapping[str, Any]) -> dict[str, Any]:
    scope = _mapping(resolution.get("m1d2_scope"))
    return {
        "implementation_allowed": True,
        "implement": _sequence(scope.get("implement")),
        "implement_conditionally": _sequence(scope.get("implement_conditionally")),
        "prohibited": _sequence(scope.get("prohibited")),
        "candidate_behavior_allowed": False,
        "replay_allowed": False,
    }


def _replacement_readiness(
    decisions: Mapping[str, Mapping[str, Any]], threshold_policy: Mapping[str, Any]
) -> dict[str, Any]:
    conditions = {
        "recovery_producer_pit_lineage_valid": False,
        "output_semantic_type_known": decisions.get("D03", {}).get("semantic_type")
        == "UNSCALED_SCORE",
        "threshold_versioned_and_preregistered": False,
        "risk_off_veto_resolved": False,
        "all_actual_hard_vetoes_resolved_or_not_applicable": False,
        "transition_trace_ready": False,
        "native_exposure_scalar_ready": False,
        "candidate_cap_in_native_units": False,
        "next_step_timing_registered": decisions.get("D07", {}).get("effective_time")
        == "next_executable_evaluation_step",
        "screening_policy_frozen": threshold_policy.get("policy_status") == "APPROVED",
    }
    return {
        "replacement_candidate_id": "capped_recovery_permission_overlay",
        "approval_status": "BLOCKED_PENDING_M1D2_AND_M1E",
        "conditions": conditions,
        "ready_condition_count": sum(conditions.values()),
        "required_condition_count": len(conditions),
        "blocker_codes": [key.upper() for key, ready in conditions.items() if not ready],
        "m2_eligible": False,
    }


def _strict_validation_errors(
    sources: Mapping[str, Any],
    evidence: Sequence[Mapping[str, Any]],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> list[str]:
    errors: list[str] = []
    resolution = _mapping(sources.get("owner_resolution"))
    if resolution.get("schema_version") != EXPECTED_SOURCE_SCHEMA:
        errors.append("owner_resolution_schema_mismatch")
    if resolution.get("status") != EXPECTED_SOURCE_STATUS:
        errors.append("owner_resolution_status_mismatch")
    decisions = _sequence(resolution.get("decisions"))
    decision_ids = [
        str(item.get("decision_id")) for item in decisions if isinstance(item, Mapping)
    ]
    if tuple(decision_ids) != EXPECTED_DECISION_IDS:
        errors.append("decision_identity_or_order_mismatch")
    if resolution.get("decision_count") != 18 or resolution.get("resolved_decision_count") != 18:
        errors.append("decision_count_mismatch")
    candidate_source = _mapping(resolution.get("candidate_disposition"))
    candidate_rows = _sequence(candidate_source.get("candidates"))
    candidate_ids = [
        str(item.get("candidate_id"))
        for item in candidate_rows
        if isinstance(item, Mapping)
    ]
    candidate_decisions = [
        str(item.get("decision"))
        for item in candidate_rows
        if isinstance(item, Mapping)
    ]
    if tuple(candidate_ids) != EXPECTED_CANDIDATE_IDS:
        errors.append("candidate_identity_or_order_mismatch")
    if tuple(candidate_decisions) != EXPECTED_CANDIDATE_DECISIONS:
        errors.append("candidate_decision_mismatch")
    blocking_ids = [
        str(item.get("decision_id"))
        for item in decisions
        if isinstance(item, Mapping) and item.get("status") == "RESOLVED_BLOCKED"
    ]
    if tuple(blocking_ids) != EXPECTED_BLOCKING_DECISION_IDS:
        errors.append("blocking_decision_identity_or_order_mismatch")
    if any(item.get("status") != "PASS" for item in evidence):
        errors.append("owner_decision_evidence_mismatch")
    report_ids = {
        str(item.get("report_id"))
        for item in _sequence(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    if REPORT_TYPE not in report_ids:
        errors.append("report_registry_alignment_failed")
    if not all(ref in artifact_catalog_text for ref in REQUIRED_CATALOG_REFERENCES):
        errors.append("artifact_catalog_alignment_failed")
    if not all(ref in system_flow_text for ref in REQUIRED_FLOW_REFERENCES):
        errors.append("system_flow_alignment_failed")
    if not all(
        ref in requirement_text
        for ref in (
            "TRADING-2438M1D1A",
            "18/18 owner decisions complete",
            "capped_recovery_permission_overlay",
            "No real PIT replay runs in M1D2",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _safety() -> dict[str, Any]:
    return {
        "owner_decision_only": True,
        "replay_run": False,
        "runtime_metrics_generated": False,
        "candidate_behavior_implemented": False,
        "baseline_behavior_changed": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
