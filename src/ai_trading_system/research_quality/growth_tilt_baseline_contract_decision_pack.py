from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_baseline_contract_decision_pack.v1"
CANDIDATE_SCHEMA_VERSION = "growth_tilt_candidate_disposition_after_baseline_audit.v1"
VETO_SCHEMA_VERSION = "growth_tilt_hard_veto_resolution_matrix.v1"
TRANSITION_EXPOSURE_SCHEMA_VERSION = "growth_tilt_transition_exposure_decision.v1"

REPORT_TYPE = "growth_tilt_baseline_contract_decision_pack"
READY_STATUS = "GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK_READY_OWNER_DECISIONS_REQUIRED"
BLOCKED_STATUS = "GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK_BLOCKED_SOURCE_CONTRACT"
M1D2_BLOCKED_STATUS = "BLOCKED_PENDING_OWNER_DECISIONS_AND_BASELINE_CONTRACTS"
NEXT_ROUTE = "TRADING-2438M1D1_GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK"

EXPECTED_CANDIDATE_IDS = (
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator",
)
EXPECTED_DECISIONS = ("APPROVE", "REDEFINE", "REDEFINE")
EXPECTED_VETO_IDS = (
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto",
)
EXPECTED_STATES = ("risk_off", "defensive", "neutral", "constructive", "risk_on")
EXPECTED_SCHEMAS = {
    "channel_config": "channel_specific_first_layer_v3_config.v1",
    "final_matrix": "channel_specific_first_layer_v3_final_matrix.v1",
    "signal_usage_matrix": "first_layer_signal_usage_matrix.v2",
    "base_policy": "base_overlay_veto_policy_schema.v1",
    "risk_veto_policy": "risk_on_veto_policy.v1",
    "probe_registry": "dynamic_second_layer_probe_registry.v2",
    "owner_review": "growth_tilt_baseline_contract_decision_review.v1",
}

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-baseline-contract-decision-pack",
    "growth_tilt_baseline_contract_decision_pack.json",
    "growth_tilt_candidate_disposition_after_baseline_audit.json",
    "growth_tilt_hard_veto_resolution_matrix.json",
    "growth_tilt_transition_exposure_decision.json",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438M1D1",
    READY_STATUS,
    M1D2_BLOCKED_STATUS,
    "do_not_de_risk_pass",
    "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE",
)


def build_growth_tilt_baseline_contract_decision_pack(
    sources: Mapping[str, Any],
    *,
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    as_of: str,
) -> dict[str, Any]:
    strict_errors = _strict_validation_errors(
        sources,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    channel_config = _mapping(sources.get("channel_config"))
    final_matrix = _mapping(sources.get("final_matrix"))
    usage_matrix = _mapping(sources.get("signal_usage_matrix"))
    base_policy = _mapping(sources.get("base_policy"))
    risk_veto_policy = _mapping(sources.get("risk_veto_policy"))
    probe_registry = _mapping(sources.get("probe_registry"))
    owner_review = _mapping(sources.get("owner_review"))
    channel_code = str(sources.get("channel_code_text") or "")
    compiler_code = str(sources.get("compiler_code_text") or "")

    recovery = _recovery_decision(
        channel_config,
        final_matrix,
        usage_matrix,
        owner_review,
        channel_code,
        compiler_code,
    )
    defensive = _defensive_entry_decision(
        owner_review,
        channel_code,
        compiler_code,
    )
    veto_rows = _hard_veto_resolution_matrix(
        risk_veto_policy,
        owner_review,
        channel_code,
        compiler_code,
    )
    transition = _transition_decision(
        probe_registry,
        owner_review,
        channel_code,
        compiler_code,
    )
    exposure = _exposure_decision(base_policy, owner_review, compiler_code)
    candidate_rows = _candidate_disposition(owner_review, recovery, defensive)
    owner_actions = _owner_actions(
        recovery,
        defensive,
        veto_rows,
        transition,
        exposure,
        candidate_rows,
    )
    status = BLOCKED_STATUS if strict_errors else READY_STATUS
    safety = _safety()

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1D1",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "base_commit": owner_review.get("base_commit"),
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": "2022-12-01",
        "source_artifacts": [dict(item) for item in source_artifacts],
        "interpretation_correction": {
            "field": "do_not_de_risk_pass",
            "classification": "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE",
            "offline_selection_pass": _mapping(final_matrix.get("summary")).get(
                "do_not_de_risk_pass"
            )
            is True,
            "mapping_readiness_separate_from_runtime_activation": True,
            "candidate_may_force_signal_true": False,
        },
        "recovery_persistence_decision": recovery,
        "defensive_entry_decision": defensive,
        "candidate_disposition_after_baseline_audit": {
            "schema_version": CANDIDATE_SCHEMA_VERSION,
            "status": status,
            "candidates": candidate_rows,
            "m2_eligible_candidate_count": 0,
            "m2_eligible_candidate_ids": [],
        },
        "hard_veto_resolution_matrix": {
            "schema_version": VETO_SCHEMA_VERSION,
            "status": status,
            "components": veto_rows,
            "complete_baseline_set": all(
                row.get("resolution_status")
                in {"RESOLVED_CALLABLE", "EXPLICITLY_NOT_APPLICABLE"}
                for row in veto_rows
            ),
            "unresolved_component_ids": [
                str(row["veto_id"])
                for row in veto_rows
                if row.get("resolution_status") == "BLOCKED_NO_PIT_CONTRACT"
            ],
            "missing_evidence_policy": "BLOCKED_NOT_FALSE",
        },
        "transition_exposure_decision": {
            "schema_version": TRANSITION_EXPOSURE_SCHEMA_VERSION,
            "status": status,
            "transition": transition,
            "exposure": exposure,
        },
        "owner_action_count": len(owner_actions),
        "owner_actions": owner_actions,
        "m1d1_decision_complete": False,
        "m1d2_implementation_allowed": False,
        "m1d2_readiness_status": M1D2_BLOCKED_STATUS,
        "m2_eligible_candidate_count": 0,
        "m2_eligible_candidate_ids": [],
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "data_quality_gate_executed": False,
        "data_quality_gate_passed": None,
        "data_quality_status": "NOT_APPLICABLE_READ_ONLY_CONTRACT_DECISION_PACK",
        "data_quality_gate_reason": (
            "M1D1 reads versioned config, code, owner-review contracts, and prior "
            "research evidence only; it does not compute features, metrics, or replay."
        ),
        **safety,
    }


def _recovery_decision(
    channel_config: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
    usage_matrix: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    channel_code: str,
    compiler_code: str,
) -> dict[str, Any]:
    usage = _records_by_key(_sequence(usage_matrix.get("signals")), "signal_name")
    re_risk_usage = _mapping(usage.get("re_risk_allowed"))
    review = _mapping(owner_review.get("recovery_persistence"))
    producer_callable = (
        'frame["re_risk_allowed_probability"] = frame["do_not_de_risk_probability"]'
        in channel_code
    )
    output_path_resolved = "re_risk_allowed_probability" in _sequence(
        _mapping(_mapping(channel_config.get("channels")).get("do_not_de_risk")).get(
            "allowed_outputs"
        )
    )
    semantics_registered = (
        "defensive_overlay_neutralization"
        in _sequence(re_risk_usage.get("allowed_usage"))
        and re_risk_usage.get("diagnostic_only") is False
    )
    compiler_consumes_recovery = "re_risk_allowed" in compiler_code
    pit_lineage_ref = review.get("pit_lineage_ref")
    required_steps = review.get("baseline_required_consecutive_steps")
    return {
        "contract_id": review.get("contract_id"),
        "create_contract": review.get("create_contract") is True,
        "signal_id": review.get("signal_id"),
        "producer_entrypoint": (
            "ai_trading_system.channel_specific_first_layer_v3."
            "_build_composer_predictions"
        ),
        "output_path": "channel_composer_v3_predictions.csv:re_risk_allowed_probability",
        "producer_callable": producer_callable,
        "output_path_resolved": output_path_resolved,
        "semantics_registered": semantics_registered,
        "pit_lineage_ref": pit_lineage_ref,
        "pit_lineage_valid": bool(pit_lineage_ref),
        "offline_selection_pass": _mapping(final_matrix.get("summary")).get(
            "do_not_de_risk_pass"
        )
        is True,
        "offline_selection_role": "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE",
        "compiler_consumes_recovery_permission": compiler_consumes_recovery,
        "do_not_de_risk_semantics": review.get("do_not_de_risk_semantics"),
        "baseline_required_consecutive_steps": required_steps,
        "baseline_persistence_at_least_two": _integer_at_least(required_steps, 2),
        "maximum_gap_steps": review.get("maximum_gap_steps"),
        "reset_on_false": review.get("reset_on_false"),
        "reset_on_missing": review.get("reset_on_missing"),
        "reset_on_hard_veto": review.get("reset_on_hard_veto"),
        "evaluation_cadence": review.get("evaluation_cadence"),
        "effective_timing": review.get("effective_timing"),
        "contract_ready": False,
        "blocker_codes": [
            "RECOVERY_PERMISSION_NOT_CONSUMED_BY_BASELINE_COMPILER",
            "RECOVERY_PIT_LINEAGE_REF_MISSING",
            "BASELINE_REQUIRED_PERSISTENCE_OWNER_DECISION_MISSING",
            "RECOVERY_RESET_POLICY_OWNER_DECISION_MISSING",
            "RECOVERY_EFFECTIVE_TIMING_OWNER_DECISION_MISSING",
        ],
    }


def _defensive_entry_decision(
    owner_review: Mapping[str, Any], channel_code: str, compiler_code: str
) -> dict[str, Any]:
    review = _mapping(owner_review.get("defensive_entry"))
    compiler_accepts_defensive_hold = 'signal_state.get("defensive_hold")' in compiler_code
    producer_callable = (
        '"defensive_hold":' in channel_code or "defensive_hold_probability" in channel_code
    )
    return {
        "existing_callable_soft_confirmation_found": False,
        "existing_callable_aggregate_non_hard_request_found": producer_callable,
        "compiler_accepts_defensive_hold_input": compiler_accepts_defensive_hold,
        "defensive_hold_producer_callable": producer_callable,
        "candidate_b_route": review.get("candidate_b_route"),
        "recorded_decision": "REDEFINE",
        "proposed_candidate_id": "non_hard_defensive_entry_persistence_guard",
        "implementation_status": (
            "BLOCKED_NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST"
        ),
        "withdraw_condition_met": not producer_callable,
        "second_owner_decision_required": True,
        "contract_ready": False,
        "blocker_codes": [
            "NO_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION",
            "NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST_PRODUCER",
            "CANDIDATE_B_FINAL_WITHDRAW_OR_BASELINE_DECISION_REQUIRED",
        ],
    }


def _hard_veto_resolution_matrix(
    risk_veto_policy: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    channel_code: str,
    compiler_code: str,
) -> list[dict[str, Any]]:
    required_ids = [str(item) for item in _sequence(risk_veto_policy.get("veto_types"))]
    review = _mapping(owner_review.get("hard_veto"))
    evidence = {
        "risk_off_veto": {
            "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
            "output_path": "signal_state.risk_off_veto",
            "producer_callable": '"risk_off_veto": row.get("growth_allowed") is False'
            in channel_code,
            "semantic_role": "AMBIGUOUS_ALIAS_OF_GROWTH_ALLOWED_FALSE",
            "active_when": "growth_allowed is false",
            "pit_lineage_ref": None,
            "resolution_status": review.get("risk_off_veto_resolution"),
        },
        "volatility_veto": {
            "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
            "output_path": "signal_state.volatility_veto",
            "producer_callable": '"volatility_veto":' in channel_code,
            "semantic_role": "VOLATILITY_RISK_ON_VETO_COMPONENT",
            "active_when": (
                "veto_reasons contains volatility_not_compressed or "
                "high_volatility_regime"
            ),
            "pit_lineage_ref": "channel_specific_first_layer_v3_final_matrix.v1",
            "resolution_status": review.get("volatility_veto_resolution"),
        },
        "event_risk_veto": {
            "producer_entrypoint": None,
            "output_path": "signal_state.event_risk_veto",
            "producer_callable": False,
            "semantic_role": "DECLARED_BASELINE_HARD_VETO_WITHOUT_PRODUCER",
            "active_when": "signal_state.event_risk_veto is truthy",
            "pit_lineage_ref": None,
            "resolution_status": review.get("event_risk_veto_resolution"),
        },
        "trend_break_veto": {
            "producer_entrypoint": None,
            "output_path": "signal_state.trend_break_veto",
            "producer_callable": False,
            "semantic_role": "DECLARED_BASELINE_HARD_VETO_WITHOUT_PRODUCER",
            "active_when": "signal_state.trend_break_veto is truthy",
            "pit_lineage_ref": None,
            "resolution_status": review.get("trend_break_veto_resolution"),
        },
        "tqqq_veto": {
            "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
            "output_path": "signal_state.tqqq_veto",
            "producer_callable": '"tqqq_veto": True' in channel_code,
            "semantic_role": "STATIC_NO_TQQQ_GUARD",
            "active_when": "true static guard in channel-v3 dry run",
            "pit_lineage_ref": "base_overlay_veto_policy_schema.v1",
            "resolution_status": review.get("tqqq_veto_resolution"),
        },
    }
    rows: list[dict[str, Any]] = []
    for veto_id in EXPECTED_VETO_IDS:
        row = dict(evidence[veto_id])
        resolution = row.get("resolution_status")
        row.update(
            {
                "veto_id": veto_id,
                "required_by_baseline": veto_id in required_ids,
                "compiler_consumes_component": f'"{veto_id}"' in compiler_code,
                "priority": "BEFORE_CANDIDATE_OVERLAY",
                "missing_policy": "BLOCKED_NOT_FALSE",
                "not_applicable_rationale": None,
                "ready": (
                    resolution == "RESOLVED_CALLABLE"
                    and row.get("producer_callable") is True
                    and bool(row.get("pit_lineage_ref"))
                ),
            }
        )
        rows.append(row)
    return rows


def _transition_decision(
    probe_registry: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    channel_code: str,
    compiler_code: str,
) -> dict[str, Any]:
    review = _mapping(owner_review.get("transition"))
    states = [str(item) for item in _sequence(probe_registry.get("trend_states"))]
    same_row_mutation = 'frame.loc[mask, "trend_state"] = "neutral"' in channel_code
    requested_fields = all(
        field in compiler_code
        for field in ("requested_state", "applied_state", "requested_at", "applied_at")
    )
    return {
        "contract_id": review.get("contract_id"),
        "create_contract": review.get("create_contract") is True,
        "canonical_state_ids": states,
        "canonical_state_schema_ready": tuple(states) == EXPECTED_STATES,
        "source_field": "first_layer_composer_v2_predictions.csv:trend_state",
        "existing_same_row_label_mutation": same_row_mutation,
        "requested_applied_split_requested": review.get("requested_applied_split") is True,
        "requested_applied_split_callable": requested_fields,
        "effective_timing": review.get("effective_timing"),
        "hard_veto_priority": review.get("hard_veto_priority"),
        "ordinary_request_priority": review.get("ordinary_request_priority"),
        "conflict_resolution": review.get("conflict_resolution"),
        "contract_ready": False,
        "blocker_codes": [
            "REQUESTED_APPLIED_TRANSITION_INTERFACE_MISSING",
            "TRANSITION_EFFECTIVE_TIMING_OWNER_DECISION_MISSING",
            "TRANSITION_PRIORITY_POLICY_OWNER_DECISION_MISSING",
        ],
    }


def _exposure_decision(
    base_policy: Mapping[str, Any],
    owner_review: Mapping[str, Any],
    compiler_code: str,
) -> dict[str, Any]:
    review = _mapping(owner_review.get("exposure"))
    caps = _mapping(base_policy.get("caps"))
    formula_callable = (
        'weights.get("QQQ", 0.0) + 3.0 * weights.get("TQQQ", 0.0)'
        in compiler_code
    )
    native_fields_ready = all(
        review.get(field) is not None
        for field in (
            "native_scalar_id",
            "unit",
            "minimum_value",
            "maximum_value",
            "minimum_increment",
        )
    )
    return {
        "contract_id": review.get("contract_id"),
        "create_contract": review.get("create_contract") is True,
        "native_scalar_id": review.get("native_scalar_id"),
        "unit": review.get("unit"),
        "minimum_value": review.get("minimum_value"),
        "maximum_value": review.get("maximum_value"),
        "minimum_increment": review.get("minimum_increment"),
        "current_scalar_field": None,
        "requested_target_scalar_field": None,
        "applied_target_scalar_field": None,
        "native_scalar_fields_ready": native_fields_ready,
        "qqq_equivalent_supported": (
            review.get("qqq_equivalent_supported") is True and formula_callable
        ),
        "qqq_equivalent_scope": review.get("qqq_equivalent_scope"),
        "qqq_equivalent_formula": "QQQ_weight + 3.0 * TQQQ_weight",
        "qqq_equivalent_formula_ref": review.get("qqq_equivalent_formula_ref"),
        "qqq_equivalent_cap": caps.get("QQQ_equivalent_exposure_max"),
        "qqq_equivalent_formula_callable": formula_callable,
        "candidate_delta_may_use_qqq_equivalent": False,
        "tqqq_increase_allowed": False,
        "contract_ready": False,
        "blocker_codes": [
            "BASELINE_NATIVE_EXPOSURE_SCALAR_NOT_SELECTED",
            "CURRENT_REQUESTED_APPLIED_SCALAR_FIELDS_MISSING",
            "MINIMUM_INCREMENT_OWNER_DECISION_MISSING",
            "QQQ_EQUIVALENT_ONLY_GOVERNED_FOR_CAP_NOT_CANDIDATE_DELTA",
        ],
    }


def _candidate_disposition(
    owner_review: Mapping[str, Any],
    recovery: Mapping[str, Any],
    defensive: Mapping[str, Any],
) -> list[dict[str, Any]]:
    reviews = _records_by_key(
        _sequence(owner_review.get("candidate_disposition")), "candidate_id"
    )
    rows: list[dict[str, Any]] = []
    for candidate_id in EXPECTED_CANDIDATE_IDS:
        review = _mapping(reviews.get(candidate_id))
        row = dict(review)
        row["m2_eligible"] = False
        if candidate_id == EXPECTED_CANDIDATE_IDS[0]:
            row["baseline_contract_ready"] = recovery.get("contract_ready") is True
            row["blocker_codes"] = [
                "RECOVERY_PERSISTENCE_CONTRACT_NOT_READY",
                "HARD_VETO_AGGREGATE_NOT_READY",
                "TRANSITION_CONTRACT_NOT_READY",
                "NATIVE_EXPOSURE_SCALAR_NOT_READY",
                "SCREENING_POLICY_NOT_PREREGISTERED",
            ]
        elif candidate_id == EXPECTED_CANDIDATE_IDS[1]:
            row["baseline_contract_ready"] = defensive.get("contract_ready") is True
            row["blocker_codes"] = list(_sequence(defensive.get("blocker_codes")))
        else:
            row["baseline_contract_ready"] = False
            row["blocker_codes"] = ["SECOND_OWNER_APPROVAL_REQUIRED"]
        rows.append(row)
    return rows


def _owner_actions(
    recovery: Mapping[str, Any],
    defensive: Mapping[str, Any],
    veto_rows: Sequence[Mapping[str, Any]],
    transition: Mapping[str, Any],
    exposure: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for field in (
        "baseline_required_consecutive_steps",
        "maximum_gap_steps",
        "reset_on_false",
        "reset_on_missing",
        "effective_timing",
    ):
        if recovery.get(field) is None:
            actions.append(_action("recovery_persistence", field))
    if defensive.get("withdraw_condition_met") is True:
        actions.append(
            _action(
                "candidate_b",
                "final_disposition",
                "Confirm WITHDRAW or separately approve new baseline aggregate "
                "behavior; do not implement the redefinition against a missing producer.",
            )
        )
    for row in veto_rows:
        if row.get("resolution_status") == "BLOCKED_NO_PIT_CONTRACT":
            actions.append(
                _action(
                    "hard_veto",
                    str(row.get("veto_id")),
                    "Resolve callable PIT lineage or keep M1D2/M2 blocked.",
                )
            )
    for field in (
        "effective_timing",
        "hard_veto_priority",
        "ordinary_request_priority",
        "conflict_resolution",
    ):
        if transition.get(field) is None:
            actions.append(_action("transition", field))
    for field in (
        "native_scalar_id",
        "unit",
        "minimum_value",
        "maximum_value",
        "minimum_increment",
    ):
        if exposure.get(field) is None:
            actions.append(_action("exposure", field))
    if len(candidate_rows) != 3:
        actions.append(_action("candidate_disposition", "candidate_identity"))
    return actions


def _action(area: str, field: str, action: str | None = None) -> dict[str, Any]:
    return {
        "area": area,
        "field": field,
        "status": "OWNER_DECISION_REQUIRED",
        "recommended_action": action or f"Record an explicit owner decision for {area}.{field}.",
    }


def _strict_validation_errors(
    sources: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> list[str]:
    errors: list[str] = []
    for source_id, expected_schema in EXPECTED_SCHEMAS.items():
        if _mapping(sources.get(source_id)).get("schema_version") != expected_schema:
            errors.append(f"source_schema_mismatch:{source_id}")
    owner_review = _mapping(sources.get("owner_review"))
    candidate_rows = _sequence(owner_review.get("candidate_disposition"))
    candidate_ids = [
        str(row.get("candidate_id"))
        for row in candidate_rows
        if isinstance(row, Mapping)
    ]
    decisions = [
        str(row.get("decision")) for row in candidate_rows if isinstance(row, Mapping)
    ]
    if tuple(candidate_ids) != EXPECTED_CANDIDATE_IDS:
        errors.append("candidate_identity_or_order_mismatch")
    if tuple(decisions) != EXPECTED_DECISIONS:
        errors.append("candidate_disposition_mismatch")
    veto_ids = [
        str(item)
        for item in _sequence(_mapping(sources.get("risk_veto_policy")).get("veto_types"))
    ]
    if tuple(veto_ids) != EXPECTED_VETO_IDS:
        errors.append("baseline_veto_identity_or_order_mismatch")
    states = [
        str(item)
        for item in _sequence(_mapping(sources.get("probe_registry")).get("trend_states"))
    ]
    if tuple(states) != EXPECTED_STATES:
        errors.append("baseline_state_identity_or_order_mismatch")
    report_ids = {
        str(row.get("report_id"))
        for row in _sequence(report_registry.get("reports"))
        if isinstance(row, Mapping)
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
            "TRADING-2438M1D1",
            "Mapping readiness and candidate activation must remain separate",
            "Do not add a new soft confirmation",
            "M1D1 completion does not make any candidate M2-eligible",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _safety() -> dict[str, Any]:
    return {
        "decision_pack_only": True,
        "validation_only": True,
        "read_only": True,
        "replay_run": False,
        "runtime_metrics_generated": False,
        "six_metric_runtime_artifact_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "candidate_behavior_implemented": False,
        "candidate_parameters_changed": False,
        "threshold_values_changed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "production_allowed": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _integer_at_least(value: Any, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _records_by_key(rows: Sequence[Any], key: str) -> dict[str, Mapping[str, Any]]:
    return {
        str(row.get(key)): row
        for row in rows
        if isinstance(row, Mapping) and row.get(key)
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
