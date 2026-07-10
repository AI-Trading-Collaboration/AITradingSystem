from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from typing import Any

INPUT_CONTRACT_VERSION = "growth_tilt_candidate_overlay_input.v1"
OUTPUT_CONTRACT_VERSION = "growth_tilt_candidate_overlay_output.v1"
EXECUTOR_ID = "GrowthTiltCandidateOverlayExecutor"
EXECUTOR_VERSION = "v1"

EARLY_REENTRY = "EARLY_REENTRY_PROVISIONAL_EXPOSURE"
DEFENSIVE_GRACE = "DEFENSIVE_SOFT_CONFIRMATION_GRACE"
POST_CONFIRMATION_RAMP = "POST_CONFIRMATION_RAMP_ACCELERATION"
SUPPORTED_OPERATION_TYPES = {
    EARLY_REENTRY,
    DEFENSIVE_GRACE,
    POST_CONFIRMATION_RAMP,
}

EXECUTED = "EXECUTED"
BLOCKED = "BLOCKED"

REQUIRED_INPUT_FIELDS = (
    "as_of",
    "candidate_id",
    "baseline_config_ref",
    "baseline_state",
    "baseline_target_exposure",
    "baseline_decision_trace",
    "pit_signal_snapshot",
    "hard_veto_state",
    "regime_state",
    "candidate_runtime_spec",
    "candidate_runtime_state",
    "transaction_cost_model_ref",
    "input_provenance",
)


class GrowthTiltCandidateOverlayExecutor:
    """Deterministic, research-only overlay over a baseline exposure decision.

    The operation type is read only from the approved runtime contract. Candidate
    identifiers are opaque provenance and never select behavior.
    """

    executor_id = EXECUTOR_ID
    executor_version = EXECUTOR_VERSION
    input_contract_version = INPUT_CONTRACT_VERSION
    output_contract_version = OUTPUT_CONTRACT_VERSION

    def execute(self, inputs: Mapping[str, Any]) -> dict[str, Any]:
        blockers = _input_blockers(inputs)
        spec = _mapping(inputs.get("candidate_runtime_spec"))
        operation_type = str(spec.get("operation_type") or "")
        if operation_type not in SUPPORTED_OPERATION_TYPES:
            blockers.append("CANDIDATE_OPERATION_TYPE_UNREGISTERED")
        if blockers:
            return _blocked_output(inputs, operation_type, sorted(set(blockers)))

        handlers: dict[str, Callable[[Mapping[str, Any]], dict[str, Any]]] = {
            EARLY_REENTRY: _execute_early_reentry,
            DEFENSIVE_GRACE: _execute_defensive_grace,
            POST_CONFIRMATION_RAMP: _execute_post_confirmation_ramp,
        }
        result = handlers[operation_type](inputs)
        result.update(_common_output(inputs, operation_type))
        return result


def _execute_early_reentry(inputs: Mapping[str, Any]) -> dict[str, Any]:
    spec = _mapping(inputs.get("candidate_runtime_spec"))
    parameters = _mapping(spec.get("parameters"))
    state = _mapping(inputs.get("baseline_state"))
    runtime_state = _mapping(inputs.get("candidate_runtime_state"))
    snapshot = _mapping(inputs.get("pit_signal_snapshot"))
    signals = _mapping(snapshot.get("signals"))
    confirmations = _mapping(snapshot.get("confirmations"))
    baseline_target = float(inputs["baseline_target_exposure"])
    blockers = _early_reentry_contract_blockers(spec)
    blockers.extend(_veto_mapping_blockers(inputs, spec))
    blockers.extend(_baseline_binding_blockers(inputs, spec))
    if blockers:
        return _operation_blocked(inputs, EARLY_REENTRY, blockers)

    recovery_signal_id = str(parameters["recovery_signal_id"])
    lagging_id = str(parameters["lagging_soft_confirmation_id"])
    recovery_signal = signals.get(recovery_signal_id)
    lagging_confirmation = confirmations.get(lagging_id)
    other_confirmations = {
        str(key): value for key, value in confirmations.items() if str(key) != lagging_id
    }
    snapshot_blockers: list[str] = []
    if not isinstance(recovery_signal, bool):
        snapshot_blockers.append("RECOVERY_SIGNAL_MISSING_OR_NOT_BOOLEAN")
    if not isinstance(lagging_confirmation, bool):
        snapshot_blockers.append("LAGGING_SOFT_CONFIRMATION_MISSING_OR_NOT_BOOLEAN")
    if any(not isinstance(value, bool) for value in other_confirmations.values()):
        snapshot_blockers.append("RECOVERY_CONFIRMATION_NOT_BOOLEAN")
    recovery_target = state.get("recovery_target_exposure")
    if not _finite_exposure(recovery_target):
        snapshot_blockers.append("BASELINE_RECOVERY_TARGET_EXPOSURE_INVALID")
    recovery_confirmed = state.get("recovery_confirmed")
    current_regime = _mapping(inputs.get("regime_state")).get("regime_id")
    if not isinstance(recovery_confirmed, bool):
        snapshot_blockers.append("BASELINE_RECOVERY_CONFIRMATION_STATE_INVALID")
    if snapshot_blockers:
        return _operation_blocked(inputs, EARLY_REENTRY, snapshot_blockers)

    veto_checks = _veto_checks(inputs, spec)
    any_veto = any(item["active"] is True for item in veto_checks)
    active_steps = int(runtime_state["active_steps"])
    max_active_steps = int(parameters["max_active_steps"])
    guard_checks = [
        _check("recovery_core_signal_true", recovery_signal is True),
        _check("baseline_recovery_not_yet_confirmed", recovery_confirmed is False),
        _check(
            "named_soft_confirmation_is_only_unmet",
            lagging_confirmation is False and all(other_confirmations.values()),
        ),
        _check("all_hard_vetoes_inactive", not any_veto),
        _check(
            "inside_approved_regime_scope",
            current_regime in _text_set(spec.get("applicable_regime_ids")),
        ),
        _check("max_active_steps_not_reached", active_steps < max_active_steps),
        _check("positive_remaining_exposure_gap", float(recovery_target) > baseline_target),
    ]
    triggered = all(item["passed"] is True for item in guard_checks)
    previous_active = active_steps > 0

    if triggered:
        remaining_gap = max(0.0, float(recovery_target) - baseline_target)
        fraction_delta = remaining_gap * float(
            parameters["provisional_exposure_fraction_of_remaining_gap"]
        )
        exposure_delta = min(
            fraction_delta,
            float(parameters["provisional_exposure_absolute_cap"]),
            remaining_gap,
        )
        candidate_target = baseline_target + exposure_delta
        trigger_codes = ["EARLY_REENTRY_PROVISIONAL_EXPOSURE_APPLIED"]
        candidate_state = "PROVISIONAL_REENTRY"
        expiry_state = "ACTIVE"
        rollback_state = "NOT_REQUIRED"
        next_active_steps = active_steps + 1
    else:
        candidate_target = baseline_target
        exposure_delta = 0.0
        trigger_codes = [_early_reentry_no_change_reason(guard_checks, any_veto)]
        candidate_state = "BASELINE_PATH"
        expiry_state = _expiry_state(guard_checks, any_veto)
        rollback_state = "ROLLED_BACK_TO_BASELINE" if previous_active else "NOT_REQUIRED"
        next_active_steps = 0
    return _executed_output(
        inputs,
        candidate_state=candidate_state,
        candidate_target=candidate_target,
        exposure_delta=exposure_delta,
        trigger_reason_codes=trigger_codes,
        guard_check_results=guard_checks,
        veto_check_results=veto_checks,
        expiry_state=expiry_state,
        rollback_state=rollback_state,
        next_runtime_state={"active_steps": next_active_steps},
    )


def _execute_defensive_grace(inputs: Mapping[str, Any]) -> dict[str, Any]:
    spec = _mapping(inputs.get("candidate_runtime_spec"))
    parameters = _mapping(spec.get("parameters"))
    state = _mapping(inputs.get("baseline_state"))
    runtime_state = _mapping(inputs.get("candidate_runtime_state"))
    snapshot = _mapping(inputs.get("pit_signal_snapshot"))
    confirmations = _mapping(snapshot.get("confirmations"))
    regime = _mapping(inputs.get("regime_state"))
    baseline_target = float(inputs["baseline_target_exposure"])
    blockers = _defensive_grace_contract_blockers(spec)
    blockers.extend(_veto_mapping_blockers(inputs, spec))
    blockers.extend(_baseline_binding_blockers(inputs, spec))
    if blockers:
        return _operation_blocked(inputs, DEFENSIVE_GRACE, blockers)

    confirmation_id = str(parameters["relaxed_soft_confirmation_id"])
    confirmation_active = confirmations.get(confirmation_id)
    raw_causes = state.get("defensive_entry_causes")
    causes = [str(item) for item in _sequence(raw_causes)]
    pre_defensive = state.get("pre_defensive_target_exposure")
    defensive_state_active = state.get("defensive_state_active")
    baseline_state_id = state.get("state_id")
    current_regime = regime.get("regime_id")
    snapshot_blockers: list[str] = []
    if not isinstance(confirmation_active, bool):
        snapshot_blockers.append("RELAXED_SOFT_CONFIRMATION_MISSING_OR_NOT_BOOLEAN")
    if not _finite_exposure(pre_defensive):
        snapshot_blockers.append("PRE_DEFENSIVE_TARGET_EXPOSURE_INVALID")
    if not isinstance(defensive_state_active, bool):
        snapshot_blockers.append("BASELINE_DEFENSIVE_STATE_INVALID")
    if not isinstance(raw_causes, Sequence) or isinstance(raw_causes, (str, bytes)):
        snapshot_blockers.append("DEFENSIVE_ENTRY_CAUSES_INVALID")
    if _finite_exposure(pre_defensive) and float(pre_defensive) < baseline_target:
        snapshot_blockers.append("PRE_DEFENSIVE_TARGET_BELOW_BASELINE_DEFENSIVE_TARGET")
    if not current_regime:
        snapshot_blockers.append("REGIME_ID_MISSING")
    if snapshot_blockers:
        return _operation_blocked(inputs, DEFENSIVE_GRACE, snapshot_blockers)

    veto_checks = _veto_checks(inputs, spec)
    any_veto = any(item["active"] is True for item in veto_checks)
    active_steps = int(runtime_state["active_steps"])
    grace_steps = int(parameters["grace_steps"])
    regime_allowed = current_regime in _text_set(spec.get("applicable_regime_ids"))
    guard_checks = [
        _check("selected_soft_confirmation_active", confirmation_active is True),
        _check("selected_confirmation_is_only_defensive_cause", causes == [confirmation_id]),
        _check("baseline_defensive_state_active", defensive_state_active is True),
        _check(
            "baseline_required_state_matches",
            baseline_state_id == parameters["baseline_required_state"],
        ),
        _check("inside_approved_regime_scope", regime_allowed),
        _check("all_hard_vetoes_inactive", not any_veto),
        _check("grace_not_consumed", active_steps < grace_steps),
    ]
    triggered = all(item["passed"] is True for item in guard_checks)
    previous_active = active_steps > 0

    if triggered:
        candidate_target = min(float(pre_defensive), 1.0)
        exposure_delta = candidate_target - baseline_target
        trigger_codes = ["DEFENSIVE_SOFT_CONFIRMATION_ONE_STEP_GRACE_APPLIED"]
        candidate_state = "SOFT_CONFIRMATION_GRACE"
        expiry_state = "ACTIVE_ONE_STEP_ONLY"
        rollback_state = "NOT_REQUIRED"
        next_active_steps = active_steps + 1
    else:
        candidate_target = baseline_target
        exposure_delta = 0.0
        trigger_codes = [_defensive_grace_no_change_reason(guard_checks, any_veto)]
        candidate_state = "BASELINE_DEFENSIVE_PATH"
        expiry_state = _expiry_state(guard_checks, any_veto)
        rollback_state = "ROLLED_BACK_TO_BASELINE" if previous_active else "NOT_REQUIRED"
        next_active_steps = 0
    return _executed_output(
        inputs,
        candidate_state=candidate_state,
        candidate_target=candidate_target,
        exposure_delta=exposure_delta,
        trigger_reason_codes=trigger_codes,
        guard_check_results=guard_checks,
        veto_check_results=veto_checks,
        expiry_state=expiry_state,
        rollback_state=rollback_state,
        next_runtime_state={"active_steps": next_active_steps},
    )


def _execute_post_confirmation_ramp(inputs: Mapping[str, Any]) -> dict[str, Any]:
    spec = _mapping(inputs.get("candidate_runtime_spec"))
    parameters = _mapping(spec.get("parameters"))
    state = _mapping(inputs.get("baseline_state"))
    runtime_state = _mapping(inputs.get("candidate_runtime_state"))
    baseline_target = float(inputs["baseline_target_exposure"])
    blockers = _post_confirmation_contract_blockers(spec)
    blockers.extend(_veto_mapping_blockers(inputs, spec))
    blockers.extend(_baseline_binding_blockers(inputs, spec))
    if blockers:
        return _operation_blocked(inputs, POST_CONFIRMATION_RAMP, blockers)

    previous_target = state.get("previous_baseline_target_exposure")
    recovery_target = state.get("recovery_target_exposure")
    recovery_confirmed = state.get("recovery_confirmed")
    snapshot_blockers: list[str] = []
    if not _finite_exposure(previous_target):
        snapshot_blockers.append("PREVIOUS_BASELINE_TARGET_EXPOSURE_INVALID")
    if not _finite_exposure(recovery_target):
        snapshot_blockers.append("BASELINE_RECOVERY_TARGET_EXPOSURE_INVALID")
    if not isinstance(recovery_confirmed, bool):
        snapshot_blockers.append("BASELINE_RECOVERY_CONFIRMATION_STATE_INVALID")
    if snapshot_blockers:
        return _operation_blocked(inputs, POST_CONFIRMATION_RAMP, snapshot_blockers)

    veto_checks = _veto_checks(inputs, spec)
    any_veto = any(item["active"] is True for item in veto_checks)
    active_steps = int(runtime_state["active_steps"])
    acceleration_steps = int(parameters["acceleration_steps"])
    guard_checks = [
        _check("baseline_recovery_confirmed", recovery_confirmed is True),
        _check("all_hard_vetoes_inactive", not any_veto),
        _check("acceleration_steps_not_reached", active_steps < acceleration_steps),
        _check("baseline_ramp_step_positive", baseline_target > float(previous_target)),
        _check("positive_remaining_exposure_gap", float(recovery_target) > baseline_target),
    ]
    triggered = all(item["passed"] is True for item in guard_checks)
    previous_active = active_steps > 0

    if triggered:
        baseline_step = max(0.0, baseline_target - float(previous_target))
        remaining_gap = max(0.0, float(recovery_target) - baseline_target)
        extra_from_multiplier = baseline_step * (
            float(parameters["ramp_step_multiplier"]) - 1.0
        )
        extra_cap = remaining_gap * float(
            parameters["max_fraction_of_remaining_gap_per_step"]
        )
        exposure_delta = min(extra_from_multiplier, extra_cap, remaining_gap)
        candidate_target = baseline_target + exposure_delta
        trigger_codes = ["POST_CONFIRMATION_RAMP_ACCELERATION_APPLIED"]
        candidate_state = "POST_CONFIRMATION_ACCELERATED_RAMP"
        expiry_state = "ACTIVE"
        rollback_state = "NOT_REQUIRED"
        next_active_steps = active_steps + 1
    else:
        candidate_target = baseline_target
        exposure_delta = 0.0
        trigger_codes = [
            "HARD_VETO_ACTIVE_BASELINE_REQUIRED"
            if any_veto
            else "POST_CONFIRMATION_RAMP_GUARD_NOT_MET"
        ]
        candidate_state = "BASELINE_PATH"
        expiry_state = _expiry_state(guard_checks, any_veto)
        rollback_state = "ROLLED_BACK_TO_BASELINE" if previous_active else "NOT_REQUIRED"
        next_active_steps = 0
    return _executed_output(
        inputs,
        candidate_state=candidate_state,
        candidate_target=candidate_target,
        exposure_delta=exposure_delta,
        trigger_reason_codes=trigger_codes,
        guard_check_results=guard_checks,
        veto_check_results=veto_checks,
        expiry_state=expiry_state,
        rollback_state=rollback_state,
        next_runtime_state={"active_steps": next_active_steps},
    )


def _input_blockers(inputs: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for field in REQUIRED_INPUT_FIELDS:
        if field not in inputs or inputs.get(field) is None:
            blockers.append(f"REQUIRED_INPUT_MISSING:{field}")
    if blockers:
        return blockers
    if not _finite_exposure(inputs.get("baseline_target_exposure")):
        blockers.append("BASELINE_TARGET_EXPOSURE_INVALID")
    for field in (
        "baseline_state",
        "pit_signal_snapshot",
        "hard_veto_state",
        "regime_state",
        "candidate_runtime_spec",
        "candidate_runtime_state",
        "input_provenance",
    ):
        if not isinstance(inputs.get(field), Mapping):
            blockers.append(f"INPUT_NOT_MAPPING:{field}")
    if not isinstance(inputs.get("baseline_decision_trace"), Sequence) or isinstance(
        inputs.get("baseline_decision_trace"), (str, bytes)
    ):
        blockers.append("BASELINE_DECISION_TRACE_INVALID")
    runtime_state = _mapping(inputs.get("candidate_runtime_state"))
    if not _nonnegative_int(runtime_state.get("active_steps")):
        blockers.append("CANDIDATE_RUNTIME_ACTIVE_STEPS_INVALID")
    veto_state = _mapping(inputs.get("hard_veto_state"))
    if any(not isinstance(value, bool) for value in veto_state.values()):
        blockers.append("HARD_VETO_STATE_INVALID")
    if _owner_placeholder_paths(inputs):
        blockers.append("OWNER_PLACEHOLDER_PRESENT")
    return blockers


def _early_reentry_contract_blockers(spec: Mapping[str, Any]) -> list[str]:
    parameters = _mapping(spec.get("parameters"))
    blockers = _common_spec_blockers(spec)
    required = {
        "recovery_signal_id",
        "lagging_soft_confirmation_id",
        "lead_steps",
        "provisional_exposure_fraction_of_remaining_gap",
        "provisional_exposure_absolute_cap",
        "max_active_steps",
        "confirmed_state_ramp_multiplier",
        "target_exposure_override_allowed",
        "hard_veto_bypass_allowed",
    }
    blockers.extend(_missing_parameter_blockers(parameters, required))
    if parameters.get("lead_steps") != 1:
        blockers.append("EARLY_REENTRY_LEAD_STEPS_MUST_EQUAL_ONE")
    if not _fraction(parameters.get("provisional_exposure_fraction_of_remaining_gap")):
        blockers.append("PROVISIONAL_EXPOSURE_FRACTION_OUT_OF_BOUNDS")
    if not _finite_exposure(parameters.get("provisional_exposure_absolute_cap")):
        blockers.append("PROVISIONAL_EXPOSURE_ABSOLUTE_CAP_INVALID")
    if not _positive_int(parameters.get("max_active_steps")):
        blockers.append("MAX_ACTIVE_STEPS_INVALID")
    if parameters.get("confirmed_state_ramp_multiplier") != 1.0:
        blockers.append("CONFIRMED_RAMP_CHANGE_PROHIBITED")
    if parameters.get("target_exposure_override_allowed") is not False:
        blockers.append("TARGET_EXPOSURE_OVERRIDE_PROHIBITED")
    if parameters.get("hard_veto_bypass_allowed") is not False:
        blockers.append("HARD_VETO_BYPASS_PROHIBITED")
    return sorted(set(blockers))


def _defensive_grace_contract_blockers(spec: Mapping[str, Any]) -> list[str]:
    parameters = _mapping(spec.get("parameters"))
    blockers = _common_spec_blockers(spec)
    required = {
        "relaxed_soft_confirmation_id",
        "baseline_required_state",
        "relaxation_mode",
        "grace_steps",
        "remove_confirmation_entirely",
        "defensive_exposure_override_allowed",
        "hard_veto_bypass_allowed",
        "max_active_steps",
    }
    blockers.extend(_missing_parameter_blockers(parameters, required))
    if parameters.get("relaxation_mode") != "ONE_STEP_GRACE":
        blockers.append("RELAXATION_MODE_MUST_BE_ONE_STEP_GRACE")
    if parameters.get("grace_steps") != 1 or parameters.get("max_active_steps") != 1:
        blockers.append("DEFENSIVE_GRACE_MUST_EQUAL_ONE_STEP")
    if parameters.get("remove_confirmation_entirely") is not False:
        blockers.append("CONFIRMATION_REMOVAL_PROHIBITED")
    if parameters.get("defensive_exposure_override_allowed") is not False:
        blockers.append("DEFENSIVE_EXPOSURE_OVERRIDE_PROHIBITED")
    if parameters.get("hard_veto_bypass_allowed") is not False:
        blockers.append("HARD_VETO_BYPASS_PROHIBITED")
    if not _text_set(spec.get("applicable_regime_ids")):
        blockers.append("APPLICABLE_REGIME_SCOPE_MISSING")
    return sorted(set(blockers))


def _post_confirmation_contract_blockers(spec: Mapping[str, Any]) -> list[str]:
    parameters = _mapping(spec.get("parameters"))
    blockers = _common_spec_blockers(spec)
    if spec.get("second_owner_approval_status") != "APPROVED":
        blockers.append("SECOND_OWNER_APPROVAL_REQUIRED")
    required = {
        "trigger_source",
        "trigger_lead_steps",
        "ramp_step_multiplier",
        "max_fraction_of_remaining_gap_per_step",
        "acceleration_steps",
        "target_exposure_override_allowed",
        "hard_veto_bypass_allowed",
        "reset_to_baseline_ramp_on_veto",
    }
    blockers.extend(_missing_parameter_blockers(parameters, required))
    if parameters.get("trigger_source") != "EXACT_BASELINE_RECOVERY_CONFIRMATION":
        blockers.append("POST_CONFIRMATION_TRIGGER_SOURCE_INVALID")
    if parameters.get("trigger_lead_steps") != 0:
        blockers.append("POST_CONFIRMATION_TRIGGER_LEAD_PROHIBITED")
    multiplier = parameters.get("ramp_step_multiplier")
    if not _is_finite_number(multiplier) or float(multiplier) <= 1.0:
        blockers.append("RAMP_STEP_MULTIPLIER_INVALID")
    if not _fraction(parameters.get("max_fraction_of_remaining_gap_per_step")):
        blockers.append("RAMP_REMAINING_GAP_FRACTION_OUT_OF_BOUNDS")
    if not _positive_int(parameters.get("acceleration_steps")):
        blockers.append("ACCELERATION_STEPS_INVALID")
    if parameters.get("target_exposure_override_allowed") is not False:
        blockers.append("TARGET_EXPOSURE_OVERRIDE_PROHIBITED")
    if parameters.get("hard_veto_bypass_allowed") is not False:
        blockers.append("HARD_VETO_BYPASS_PROHIBITED")
    if parameters.get("reset_to_baseline_ramp_on_veto") is not True:
        blockers.append("BASELINE_RAMP_RESET_REQUIRED")
    return sorted(set(blockers))


def _common_spec_blockers(spec: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not spec.get("baseline_config_ref"):
        blockers.append("BASELINE_CONFIG_REF_MISSING")
    if not _text_set(spec.get("hard_veto_ids")):
        blockers.append("HARD_VETO_MAPPING_MISSING")
    if _owner_placeholder_paths(spec):
        blockers.append("OWNER_PLACEHOLDER_PRESENT")
    return blockers


def _missing_parameter_blockers(
    parameters: Mapping[str, Any], required: set[str]
) -> list[str]:
    return [
        f"CANDIDATE_PARAMETER_MISSING:{field}"
        for field in sorted(required)
        if field not in parameters or parameters.get(field) is None
    ]


def _veto_checks(
    inputs: Mapping[str, Any], spec: Mapping[str, Any]
) -> list[dict[str, Any]]:
    veto_state = _mapping(inputs.get("hard_veto_state"))
    return [
        {
            "veto_id": veto_id,
            "mapped": veto_id in veto_state,
            "active": veto_state.get(veto_id),
            "passed": veto_state.get(veto_id) is False,
        }
        for veto_id in sorted(_text_set(spec.get("hard_veto_ids")))
    ]


def _veto_mapping_blockers(
    inputs: Mapping[str, Any], spec: Mapping[str, Any]
) -> list[str]:
    mapped_ids = _text_set(spec.get("hard_veto_ids"))
    available_ids = set(str(item) for item in _mapping(inputs.get("hard_veto_state")))
    return [
        f"HARD_VETO_STATE_MISSING:{veto_id}"
        for veto_id in sorted(mapped_ids - available_ids)
    ]


def _baseline_binding_blockers(
    inputs: Mapping[str, Any], spec: Mapping[str, Any]
) -> list[str]:
    return (
        []
        if inputs.get("baseline_config_ref") == spec.get("baseline_config_ref")
        else ["BASELINE_CONFIG_REF_BINDING_MISMATCH"]
    )


def _common_output(inputs: Mapping[str, Any], operation_type: str) -> dict[str, Any]:
    return {
        "schema_version": OUTPUT_CONTRACT_VERSION,
        "executor_id": EXECUTOR_ID,
        "executor_version": EXECUTOR_VERSION,
        "candidate_id": inputs.get("candidate_id"),
        "operation_type": operation_type or None,
        "baseline_target_exposure": inputs.get("baseline_target_exposure"),
        "input_provenance": dict(_mapping(inputs.get("input_provenance"))),
        "as_of": inputs.get("as_of"),
        "transaction_cost_model_ref": inputs.get("transaction_cost_model_ref"),
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _executed_output(
    inputs: Mapping[str, Any],
    *,
    candidate_state: str,
    candidate_target: float,
    exposure_delta: float,
    trigger_reason_codes: list[str],
    guard_check_results: list[dict[str, Any]],
    veto_check_results: list[dict[str, Any]],
    expiry_state: str,
    rollback_state: str,
    next_runtime_state: dict[str, Any],
) -> dict[str, Any]:
    baseline_trace = [
        dict(item) if isinstance(item, Mapping) else item
        for item in _sequence(inputs.get("baseline_decision_trace"))
    ]
    overlay_trace = {
        "trace_type": "CANDIDATE_OVERLAY",
        "candidate_state": candidate_state,
        "baseline_target_exposure": inputs.get("baseline_target_exposure"),
        "candidate_target_exposure": candidate_target,
        "exposure_delta": exposure_delta,
        "reason_codes": trigger_reason_codes,
    }
    return {
        "candidate_state": candidate_state,
        "candidate_target_exposure": candidate_target,
        "exposure_delta": exposure_delta,
        "trigger_reason_codes": trigger_reason_codes,
        "guard_check_results": guard_check_results,
        "veto_check_results": veto_check_results,
        "expiry_state": expiry_state,
        "rollback_state": rollback_state,
        "next_candidate_runtime_state": next_runtime_state,
        "decision_trace": [*baseline_trace, overlay_trace],
        "runtime_status": EXECUTED,
        "blocker_codes": [],
    }


def _operation_blocked(
    inputs: Mapping[str, Any], operation_type: str, blockers: Sequence[str]
) -> dict[str, Any]:
    result = _blocked_output(inputs, operation_type, blockers)
    for key in (
        "schema_version",
        "executor_id",
        "executor_version",
        "candidate_id",
        "operation_type",
        "baseline_target_exposure",
        "input_provenance",
        "as_of",
        "transaction_cost_model_ref",
        "observe_only",
        "production_effect",
        "broker_action",
    ):
        result.pop(key, None)
    return result


def _blocked_output(
    inputs: Mapping[str, Any], operation_type: str, blockers: Sequence[str]
) -> dict[str, Any]:
    baseline_target = inputs.get("baseline_target_exposure")
    return {
        **_common_output(inputs, operation_type),
        "candidate_state": "BLOCKED_BASELINE_UNCHANGED",
        "candidate_target_exposure": baseline_target,
        "exposure_delta": 0.0 if _is_finite_number(baseline_target) else None,
        "trigger_reason_codes": ["EXECUTION_BLOCKED_BASELINE_UNCHANGED"],
        "guard_check_results": [],
        "veto_check_results": [],
        "expiry_state": "BLOCKED",
        "rollback_state": "BASELINE_UNCHANGED",
        "next_candidate_runtime_state": dict(
            _mapping(inputs.get("candidate_runtime_state"))
        ),
        "decision_trace": [
            *_sequence(inputs.get("baseline_decision_trace")),
            {
                "trace_type": "CANDIDATE_OVERLAY_BLOCKED",
                "blocker_codes": sorted(set(str(item) for item in blockers)),
            },
        ],
        "runtime_status": BLOCKED,
        "blocker_codes": sorted(set(str(item) for item in blockers)),
    }


def _early_reentry_no_change_reason(
    checks: Sequence[Mapping[str, Any]], any_veto: bool
) -> str:
    if any_veto:
        return "HARD_VETO_ACTIVE_BASELINE_REQUIRED"
    failed = {str(item.get("check_id")) for item in checks if item.get("passed") is False}
    if "baseline_recovery_not_yet_confirmed" in failed:
        return "BASELINE_RECOVERY_CONFIRMED_RAMP_UNCHANGED"
    if "max_active_steps_not_reached" in failed:
        return "PROVISIONAL_REENTRY_MAX_ACTIVE_STEPS_REACHED"
    return "EARLY_REENTRY_GUARD_NOT_MET"


def _defensive_grace_no_change_reason(
    checks: Sequence[Mapping[str, Any]], any_veto: bool
) -> str:
    if any_veto:
        return "HARD_VETO_ACTIVE_BASELINE_REQUIRED"
    failed = {str(item.get("check_id")) for item in checks if item.get("passed") is False}
    if "grace_not_consumed" in failed:
        return "SOFT_CONFIRMATION_GRACE_EXHAUSTED"
    if "inside_approved_regime_scope" in failed:
        return "OUTSIDE_APPROVED_REGIME_BASELINE_REQUIRED"
    return "DEFENSIVE_GRACE_GUARD_NOT_MET"


def _expiry_state(checks: Sequence[Mapping[str, Any]], any_veto: bool) -> str:
    if any_veto:
        return "HARD_VETO_ACTIVATED"
    failed = [str(item.get("check_id")) for item in checks if item.get("passed") is False]
    return failed[0].upper() if failed else "INACTIVE"


def _check(check_id: str, passed: bool) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed)}


def _text_set(value: Any) -> set[str]:
    return {
        str(item)
        for item in _sequence(value)
        if isinstance(item, str) and item.strip() and not item.startswith("OWNER_MUST_")
    }


def _owner_placeholder_paths(value: Any) -> list[str]:
    paths: list[str] = []

    def visit(item: Any, prefix: str) -> None:
        if isinstance(item, Mapping):
            for key, nested in item.items():
                visit(nested, f"{prefix}.{key}" if prefix else str(key))
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
            for index, nested in enumerate(item):
                visit(nested, f"{prefix}[{index}]")
        elif isinstance(item, str) and item.startswith("OWNER_MUST_"):
            paths.append(prefix)

    visit(value, "")
    return paths


def _fraction(value: object) -> bool:
    return _is_finite_number(value) and 0.0 < float(value) <= 1.0


def _finite_exposure(value: object) -> bool:
    return _is_finite_number(value) and 0.0 <= float(value) <= 1.0


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_finite_number(value: object) -> bool:
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
