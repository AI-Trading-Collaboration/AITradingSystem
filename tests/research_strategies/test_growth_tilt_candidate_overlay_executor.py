from __future__ import annotations

import copy
from typing import Any

import pytest

from ai_trading_system.research_quality.growth_tilt_candidate_overlay_executor import (
    BLOCKED,
    DEFENSIVE_GRACE,
    EARLY_REENTRY,
    EXECUTED,
    POST_CONFIRMATION_RAMP,
    GrowthTiltCandidateOverlayExecutor,
)


def test_early_reentry_applies_bounded_provisional_exposure() -> None:
    result = _execute(_early_reentry_input())

    assert result["runtime_status"] == EXECUTED
    assert result["candidate_state"] == "PROVISIONAL_REENTRY"
    assert result["baseline_target_exposure"] == 0.40
    assert result["candidate_target_exposure"] == pytest.approx(0.50)
    assert result["exposure_delta"] == pytest.approx(0.10)
    assert result["trigger_reason_codes"] == [
        "EARLY_REENTRY_PROVISIONAL_EXPOSURE_APPLIED"
    ]
    assert result["next_candidate_runtime_state"] == {"active_steps": 1}


def test_early_reentry_does_not_change_confirmed_ramp() -> None:
    inputs = _early_reentry_input()
    inputs["baseline_state"]["recovery_confirmed"] = True
    result = _execute(inputs)

    assert result["runtime_status"] == EXECUTED
    assert result["candidate_target_exposure"] == 0.40
    assert result["exposure_delta"] == 0.0
    assert result["trigger_reason_codes"] == [
        "BASELINE_RECOVERY_CONFIRMED_RAMP_UNCHANGED"
    ]


def test_early_reentry_requires_exactly_one_lagging_confirmation() -> None:
    inputs = _early_reentry_input()
    inputs["pit_signal_snapshot"]["confirmations"]["breadth_confirmation"] = False
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.40
    assert result["trigger_reason_codes"] == ["EARLY_REENTRY_GUARD_NOT_MET"]


def test_early_reentry_allows_named_soft_confirmation_as_only_confirmation() -> None:
    inputs = _early_reentry_input()
    inputs["pit_signal_snapshot"]["confirmations"].pop("breadth_confirmation")
    result = _execute(inputs)

    assert result["runtime_status"] == EXECUTED
    assert result["candidate_target_exposure"] == pytest.approx(0.50)


def test_early_reentry_hard_veto_rolls_back_to_baseline() -> None:
    inputs = _early_reentry_input()
    inputs["candidate_runtime_state"]["active_steps"] = 1
    inputs["hard_veto_state"]["extreme_drawdown_veto"] = True
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.40
    assert result["rollback_state"] == "ROLLED_BACK_TO_BASELINE"
    assert result["expiry_state"] == "HARD_VETO_ACTIVATED"
    assert result["trigger_reason_codes"] == ["HARD_VETO_ACTIVE_BASELINE_REQUIRED"]


def test_early_reentry_expires_at_max_active_steps() -> None:
    inputs = _early_reentry_input()
    inputs["candidate_runtime_state"]["active_steps"] = 3
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.40
    assert result["rollback_state"] == "ROLLED_BACK_TO_BASELINE"
    assert result["trigger_reason_codes"] == [
        "PROVISIONAL_REENTRY_MAX_ACTIVE_STEPS_REACHED"
    ]


def test_early_reentry_cannot_change_confirmed_ramp_multiplier() -> None:
    inputs = _early_reentry_input()
    inputs["candidate_runtime_spec"]["parameters"][
        "confirmed_state_ramp_multiplier"
    ] = 1.5
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "CONFIRMED_RAMP_CHANGE_PROHIBITED" in result["blocker_codes"]
    assert result["candidate_target_exposure"] == 0.40


def test_missing_mapped_hard_veto_blocks_execution() -> None:
    inputs = _early_reentry_input()
    inputs["hard_veto_state"].pop("extreme_drawdown_veto")
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "HARD_VETO_STATE_MISSING:extreme_drawdown_veto" in result["blocker_codes"]


def test_defensive_grace_delays_one_named_soft_confirmation_for_one_step() -> None:
    result = _execute(_defensive_grace_input())

    assert result["runtime_status"] == EXECUTED
    assert result["candidate_state"] == "SOFT_CONFIRMATION_GRACE"
    assert result["candidate_target_exposure"] == 0.60
    assert result["exposure_delta"] == pytest.approx(0.30)
    assert result["trigger_reason_codes"] == [
        "DEFENSIVE_SOFT_CONFIRMATION_ONE_STEP_GRACE_APPLIED"
    ]
    assert result["next_candidate_runtime_state"] == {"active_steps": 1}


def test_defensive_grace_cannot_auto_extend() -> None:
    inputs = _defensive_grace_input()
    inputs["candidate_runtime_state"]["active_steps"] = 1
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.30
    assert result["rollback_state"] == "ROLLED_BACK_TO_BASELINE"
    assert result["trigger_reason_codes"] == ["SOFT_CONFIRMATION_GRACE_EXHAUSTED"]


def test_defensive_grace_requires_selected_confirmation_as_only_cause() -> None:
    inputs = _defensive_grace_input()
    inputs["baseline_state"]["defensive_entry_causes"].append("volatility_break")
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.30
    assert result["trigger_reason_codes"] == ["DEFENSIVE_GRACE_GUARD_NOT_MET"]


def test_defensive_grace_requires_owner_mapped_baseline_state() -> None:
    inputs = _defensive_grace_input()
    inputs["baseline_state"]["state_id"] = "OTHER_BASELINE_STATE"
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.30
    assert result["trigger_reason_codes"] == ["DEFENSIVE_GRACE_GUARD_NOT_MET"]


def test_defensive_grace_is_scoped_to_explicit_regime() -> None:
    inputs = _defensive_grace_input()
    inputs["regime_state"]["regime_id"] = "unapproved_regime"
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.30
    assert result["trigger_reason_codes"] == [
        "OUTSIDE_APPROVED_REGIME_BASELINE_REQUIRED"
    ]


def test_defensive_grace_never_bypasses_hard_veto() -> None:
    inputs = _defensive_grace_input()
    inputs["hard_veto_state"]["extreme_drawdown_veto"] = True
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.30
    assert result["trigger_reason_codes"] == ["HARD_VETO_ACTIVE_BASELINE_REQUIRED"]


def test_defensive_confirmation_removal_is_contract_blocked() -> None:
    inputs = _defensive_grace_input()
    inputs["candidate_runtime_spec"]["parameters"][
        "remove_confirmation_entirely"
    ] = True
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "CONFIRMATION_REMOVAL_PROHIBITED" in result["blocker_codes"]


def test_post_confirmation_ramp_requires_second_owner_approval() -> None:
    inputs = _post_confirmation_input()
    inputs["candidate_runtime_spec"]["second_owner_approval_status"] = "PENDING"
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "SECOND_OWNER_APPROVAL_REQUIRED" in result["blocker_codes"]
    assert result["candidate_target_exposure"] == 0.50


def test_post_confirmation_ramp_changes_speed_not_trigger_or_target() -> None:
    result = _execute(_post_confirmation_input())

    assert result["runtime_status"] == EXECUTED
    assert result["candidate_target_exposure"] == pytest.approx(0.55)
    assert result["exposure_delta"] == pytest.approx(0.05)
    assert result["candidate_target_exposure"] < 0.90
    assert result["trigger_reason_codes"] == [
        "POST_CONFIRMATION_RAMP_ACCELERATION_APPLIED"
    ]


def test_post_confirmation_ramp_cannot_advance_trigger() -> None:
    inputs = _post_confirmation_input()
    inputs["candidate_runtime_spec"]["parameters"]["trigger_lead_steps"] = 1
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "POST_CONFIRMATION_TRIGGER_LEAD_PROHIBITED" in result["blocker_codes"]


def test_post_confirmation_ramp_hard_veto_resets_to_baseline() -> None:
    inputs = _post_confirmation_input()
    inputs["candidate_runtime_state"]["active_steps"] = 1
    inputs["hard_veto_state"]["extreme_drawdown_veto"] = True
    result = _execute(inputs)

    assert result["candidate_target_exposure"] == 0.50
    assert result["rollback_state"] == "ROLLED_BACK_TO_BASELINE"
    assert result["trigger_reason_codes"] == ["HARD_VETO_ACTIVE_BASELINE_REQUIRED"]


def test_candidate_identifier_does_not_select_behavior() -> None:
    original = _early_reentry_input()
    renamed = copy.deepcopy(original)
    renamed["candidate_id"] = "opaque_candidate_without_semantic_name"

    original_result = _execute(original)
    renamed_result = _execute(renamed)

    for field in (
        "operation_type",
        "candidate_state",
        "candidate_target_exposure",
        "exposure_delta",
        "trigger_reason_codes",
        "guard_check_results",
        "veto_check_results",
        "runtime_status",
        "blocker_codes",
    ):
        assert renamed_result[field] == original_result[field]


def test_same_pit_inputs_are_deterministic() -> None:
    inputs = _defensive_grace_input()

    first = _execute(copy.deepcopy(inputs))
    second = _execute(copy.deepcopy(inputs))

    assert first == second


@pytest.mark.parametrize(
    "missing_field",
    [
        "as_of",
        "baseline_state",
        "baseline_target_exposure",
        "pit_signal_snapshot",
        "hard_veto_state",
        "candidate_runtime_spec",
        "candidate_runtime_state",
        "input_provenance",
    ],
)
def test_missing_required_input_is_blocked_without_baseline_mutation(
    missing_field: str,
) -> None:
    inputs = _early_reentry_input()
    inputs.pop(missing_field)
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert f"REQUIRED_INPUT_MISSING:{missing_field}" in result["blocker_codes"]
    assert result["production_effect"] == "none"
    assert result["broker_action"] == "none"


def test_owner_placeholder_is_blocked() -> None:
    inputs = _early_reentry_input()
    inputs["candidate_runtime_spec"]["parameters"][
        "recovery_signal_id"
    ] = "OWNER_MUST_MAP_TO_EXISTING_SIGNAL"
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "OWNER_PLACEHOLDER_PRESENT" in result["blocker_codes"]


def test_unregistered_operation_is_blocked() -> None:
    inputs = _early_reentry_input()
    inputs["candidate_runtime_spec"]["operation_type"] = "INFER_FROM_CANDIDATE_NAME"
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "CANDIDATE_OPERATION_TYPE_UNREGISTERED" in result["blocker_codes"]


def test_runtime_spec_must_bind_the_same_baseline_config() -> None:
    inputs = _early_reentry_input()
    inputs["candidate_runtime_spec"]["baseline_config_ref"] = "different_baseline:v1"
    result = _execute(inputs)

    assert result["runtime_status"] == BLOCKED
    assert "BASELINE_CONFIG_REF_BINDING_MISMATCH" in result["blocker_codes"]


def test_every_nonzero_exposure_delta_has_reason_code_and_decision_trace() -> None:
    for inputs in (
        _early_reentry_input(),
        _defensive_grace_input(),
        _post_confirmation_input(),
    ):
        result = _execute(inputs)
        assert result["exposure_delta"] != 0.0
        assert result["trigger_reason_codes"]
        assert result["decision_trace"][-1]["reason_codes"] == result[
            "trigger_reason_codes"
        ]


def test_executor_is_research_only_and_has_no_side_effect_flags() -> None:
    result = _execute(_early_reentry_input())

    assert result["observe_only"] is True
    assert result["production_effect"] == "none"
    assert result["broker_action"] == "none"
    assert result["input_provenance"]["pit_snapshot_id"] == "fixture_snapshot"


def _execute(inputs: dict[str, Any]) -> dict[str, Any]:
    return GrowthTiltCandidateOverlayExecutor().execute(inputs)


def _common_input(
    *,
    candidate_id: str,
    baseline_target: float,
    operation_type: str,
    runtime_spec: dict[str, Any],
    baseline_state: dict[str, Any],
    signal_snapshot: dict[str, Any],
) -> dict[str, Any]:
    runtime_spec = copy.deepcopy(runtime_spec)
    runtime_spec["operation_type"] = operation_type
    return {
        "as_of": "2026-07-08",
        "candidate_id": candidate_id,
        "baseline_config_ref": "fixture_baseline:v1",
        "baseline_state": baseline_state,
        "baseline_target_exposure": baseline_target,
        "baseline_decision_trace": [
            {"trace_type": "BASELINE", "target_exposure": baseline_target}
        ],
        "pit_signal_snapshot": signal_snapshot,
        "hard_veto_state": {"extreme_drawdown_veto": False},
        "regime_state": {"regime_id": "ai_after_chatgpt_full_window"},
        "candidate_runtime_spec": runtime_spec,
        "candidate_runtime_state": {"active_steps": 0},
        "transaction_cost_model_ref": "fixture_cost_model:v1",
        "input_provenance": {
            "pit_snapshot_id": "fixture_snapshot",
            "baseline_run_id": "fixture_baseline_run",
        },
    }


def _early_reentry_input() -> dict[str, Any]:
    return _common_input(
        candidate_id="recovery_reentry_speedup_guard",
        baseline_target=0.40,
        operation_type=EARLY_REENTRY,
        runtime_spec={
            "baseline_config_ref": "fixture_baseline:v1",
            "hard_veto_ids": ["extreme_drawdown_veto"],
            "applicable_regime_ids": ["ai_after_chatgpt_full_window"],
            "parameters": {
                "recovery_signal_id": "recovery_core",
                "lagging_soft_confirmation_id": "soft_confirmation",
                "lead_steps": 1,
                "provisional_exposure_fraction_of_remaining_gap": 0.25,
                "provisional_exposure_absolute_cap": 0.10,
                "max_active_steps": 3,
                "confirmed_state_ramp_multiplier": 1.0,
                "target_exposure_override_allowed": False,
                "hard_veto_bypass_allowed": False,
            },
        },
        baseline_state={
            "recovery_confirmed": False,
            "recovery_target_exposure": 0.80,
        },
        signal_snapshot={
            "signals": {"recovery_core": True},
            "confirmations": {
                "soft_confirmation": False,
                "breadth_confirmation": True,
            },
        },
    )


def _defensive_grace_input() -> dict[str, Any]:
    return _common_input(
        candidate_id="false_risk_off_confirmation_relaxation",
        baseline_target=0.30,
        operation_type=DEFENSIVE_GRACE,
        runtime_spec={
            "baseline_config_ref": "fixture_baseline:v1",
            "hard_veto_ids": ["extreme_drawdown_veto"],
            "applicable_regime_ids": ["ai_after_chatgpt_full_window"],
            "parameters": {
                "relaxed_soft_confirmation_id": "trend_soft_confirmation",
                "baseline_required_state": "PRE_DEFENSIVE",
                "relaxation_mode": "ONE_STEP_GRACE",
                "grace_steps": 1,
                "remove_confirmation_entirely": False,
                "defensive_exposure_override_allowed": False,
                "hard_veto_bypass_allowed": False,
                "max_active_steps": 1,
            },
        },
        baseline_state={
            "state_id": "PRE_DEFENSIVE",
            "defensive_state_active": True,
            "defensive_entry_causes": ["trend_soft_confirmation"],
            "pre_defensive_target_exposure": 0.60,
        },
        signal_snapshot={
            "signals": {},
            "confirmations": {"trend_soft_confirmation": True},
        },
    )


def _post_confirmation_input() -> dict[str, Any]:
    return _common_input(
        candidate_id="post_confirmation_reentry_ramp_accelerator",
        baseline_target=0.50,
        operation_type=POST_CONFIRMATION_RAMP,
        runtime_spec={
            "baseline_config_ref": "fixture_baseline:v1",
            "hard_veto_ids": ["extreme_drawdown_veto"],
            "applicable_regime_ids": ["ai_after_chatgpt_full_window"],
            "second_owner_approval_status": "APPROVED",
            "parameters": {
                "trigger_source": "EXACT_BASELINE_RECOVERY_CONFIRMATION",
                "trigger_lead_steps": 0,
                "ramp_step_multiplier": 1.5,
                "max_fraction_of_remaining_gap_per_step": 0.50,
                "acceleration_steps": 2,
                "target_exposure_override_allowed": False,
                "hard_veto_bypass_allowed": False,
                "reset_to_baseline_ramp_on_veto": True,
            },
        },
        baseline_state={
            "recovery_confirmed": True,
            "previous_baseline_target_exposure": 0.40,
            "recovery_target_exposure": 0.90,
        },
        signal_snapshot={"signals": {}, "confirmations": {}},
    )
