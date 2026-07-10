# Growth Tilt Baseline Contract Decision Pack

本报告只冻结 baseline contract 决策与缺口，不运行 PIT replay、backtest、scoring 或六项 runtime metric。`do_not_de_risk_pass=false` 是 offline selection result，不是当前 runtime value，也不单独证明 producer mapping 失败。

```json
{
  "as_of": "2026-07-10",
  "interpretation_correction": {
    "candidate_may_force_signal_true": false,
    "classification": "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE",
    "field": "do_not_de_risk_pass",
    "mapping_readiness_separate_from_runtime_activation": true,
    "offline_selection_pass": false
  },
  "m1d1_decision_complete": false,
  "m1d2_implementation_allowed": false,
  "m1d2_readiness_status": "BLOCKED_PENDING_OWNER_DECISIONS_AND_BASELINE_CONTRACTS",
  "m2_eligible_candidate_count": 0,
  "market_regime": "ai_after_chatgpt",
  "next_route": "TRADING-2438M1D1_GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK",
  "owner_action_count": 18,
  "status": "GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK_READY_OWNER_DECISIONS_REQUIRED",
  "strict_validation_error_count": 0
}
```

## Candidate Disposition

```json
[
  {
    "baseline_contract_ready": false,
    "blocker_codes": [
      "RECOVERY_PERSISTENCE_CONTRACT_NOT_READY",
      "HARD_VETO_AGGREGATE_NOT_READY",
      "TRANSITION_CONTRACT_NOT_READY",
      "NATIVE_EXPOSURE_SCALAR_NOT_READY",
      "SCREENING_POLICY_NOT_PREREGISTERED"
    ],
    "candidate_id": "recovery_reentry_speedup_guard",
    "candidate_role": "RECOVERY_REENTRY_TIMING_ACCELERATOR",
    "decision": "APPROVE",
    "decision_rationale": "Keep the orthogonal timing hypothesis, but do not activate it until a governed baseline persistence rule exists and requires at least two consecutive steps.",
    "m2_eligibility": "BLOCKED_PENDING_BASELINE_CONTRACTS",
    "m2_eligible": false
  },
  {
    "baseline_contract_ready": false,
    "blocker_codes": [
      "NO_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION",
      "NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST_PRODUCER",
      "CANDIDATE_B_FINAL_WITHDRAW_OR_BASELINE_DECISION_REQUIRED"
    ],
    "candidate_id": "false_risk_off_confirmation_relaxation",
    "candidate_role": "NON_HARD_DEFENSIVE_ENTRY_PERSISTENCE_GUARD",
    "decision": "REDEFINE",
    "decision_rationale": "No exactly-one callable PIT soft confirmation exists; the candidate may only be redefined around an existing callable aggregate non-hard defensive request.",
    "m2_eligibility": "BLOCKED_PENDING_REDEFINITION_AND_BASELINE_REQUEST",
    "m2_eligible": false,
    "proposed_candidate_id": "non_hard_defensive_entry_persistence_guard",
    "second_owner_approval_required": true,
    "withdraw_if_callable_aggregate_request_absent": true
  },
  {
    "baseline_contract_ready": false,
    "blocker_codes": [
      "SECOND_OWNER_APPROVAL_REQUIRED"
    ],
    "candidate_id": "missed_upside_reentry_accelerator",
    "decision": "REDEFINE",
    "m2_eligibility": false,
    "m2_eligible": false,
    "proposed_candidate_id": "post_confirmation_reentry_ramp_accelerator",
    "second_owner_approval_required": true
  }
]
```

## Baseline Contract Decisions

```json
{
  "defensive_entry": {
    "blocker_codes": [
      "NO_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION",
      "NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST_PRODUCER",
      "CANDIDATE_B_FINAL_WITHDRAW_OR_BASELINE_DECISION_REQUIRED"
    ],
    "candidate_b_route": "REDEFINE_AGGREGATE_PERSISTENCE",
    "compiler_accepts_defensive_hold_input": true,
    "contract_ready": false,
    "defensive_hold_producer_callable": false,
    "existing_callable_aggregate_non_hard_request_found": false,
    "existing_callable_soft_confirmation_found": false,
    "implementation_status": "BLOCKED_NO_CALLABLE_AGGREGATE_NON_HARD_DEFENSIVE_REQUEST",
    "proposed_candidate_id": "non_hard_defensive_entry_persistence_guard",
    "recorded_decision": "REDEFINE",
    "second_owner_decision_required": true,
    "withdraw_condition_met": true
  },
  "hard_veto": {
    "complete_baseline_set": false,
    "components": [
      {
        "active_when": "growth_allowed is false",
        "compiler_consumes_component": true,
        "missing_policy": "BLOCKED_NOT_FALSE",
        "not_applicable_rationale": null,
        "output_path": "signal_state.risk_off_veto",
        "pit_lineage_ref": null,
        "priority": "BEFORE_CANDIDATE_OVERLAY",
        "producer_callable": true,
        "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
        "ready": false,
        "required_by_baseline": true,
        "resolution_status": "BLOCKED_NO_PIT_CONTRACT",
        "semantic_role": "AMBIGUOUS_ALIAS_OF_GROWTH_ALLOWED_FALSE",
        "veto_id": "risk_off_veto"
      },
      {
        "active_when": "veto_reasons contains volatility_not_compressed or high_volatility_regime",
        "compiler_consumes_component": true,
        "missing_policy": "BLOCKED_NOT_FALSE",
        "not_applicable_rationale": null,
        "output_path": "signal_state.volatility_veto",
        "pit_lineage_ref": "channel_specific_first_layer_v3_final_matrix.v1",
        "priority": "BEFORE_CANDIDATE_OVERLAY",
        "producer_callable": true,
        "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
        "ready": true,
        "required_by_baseline": true,
        "resolution_status": "RESOLVED_CALLABLE",
        "semantic_role": "VOLATILITY_RISK_ON_VETO_COMPONENT",
        "veto_id": "volatility_veto"
      },
      {
        "active_when": "signal_state.event_risk_veto is truthy",
        "compiler_consumes_component": true,
        "missing_policy": "BLOCKED_NOT_FALSE",
        "not_applicable_rationale": null,
        "output_path": "signal_state.event_risk_veto",
        "pit_lineage_ref": null,
        "priority": "BEFORE_CANDIDATE_OVERLAY",
        "producer_callable": false,
        "producer_entrypoint": null,
        "ready": false,
        "required_by_baseline": true,
        "resolution_status": "BLOCKED_NO_PIT_CONTRACT",
        "semantic_role": "DECLARED_BASELINE_HARD_VETO_WITHOUT_PRODUCER",
        "veto_id": "event_risk_veto"
      },
      {
        "active_when": "signal_state.trend_break_veto is truthy",
        "compiler_consumes_component": true,
        "missing_policy": "BLOCKED_NOT_FALSE",
        "not_applicable_rationale": null,
        "output_path": "signal_state.trend_break_veto",
        "pit_lineage_ref": null,
        "priority": "BEFORE_CANDIDATE_OVERLAY",
        "producer_callable": false,
        "producer_entrypoint": null,
        "ready": false,
        "required_by_baseline": true,
        "resolution_status": "BLOCKED_NO_PIT_CONTRACT",
        "semantic_role": "DECLARED_BASELINE_HARD_VETO_WITHOUT_PRODUCER",
        "veto_id": "trend_break_veto"
      },
      {
        "active_when": "true static guard in channel-v3 dry run",
        "compiler_consumes_component": true,
        "missing_policy": "BLOCKED_NOT_FALSE",
        "not_applicable_rationale": null,
        "output_path": "signal_state.tqqq_veto",
        "pit_lineage_ref": "base_overlay_veto_policy_schema.v1",
        "priority": "BEFORE_CANDIDATE_OVERLAY",
        "producer_callable": true,
        "producer_entrypoint": "channel_specific_first_layer_v3._policy_compiler_dry_run",
        "ready": true,
        "required_by_baseline": true,
        "resolution_status": "RESOLVED_CALLABLE",
        "semantic_role": "STATIC_NO_TQQQ_GUARD",
        "veto_id": "tqqq_veto"
      }
    ],
    "missing_evidence_policy": "BLOCKED_NOT_FALSE",
    "schema_version": "growth_tilt_hard_veto_resolution_matrix.v1",
    "status": "GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK_READY_OWNER_DECISIONS_REQUIRED",
    "unresolved_component_ids": [
      "risk_off_veto",
      "event_risk_veto",
      "trend_break_veto"
    ]
  },
  "recovery_persistence": {
    "baseline_persistence_at_least_two": false,
    "baseline_required_consecutive_steps": null,
    "blocker_codes": [
      "RECOVERY_PERMISSION_NOT_CONSUMED_BY_BASELINE_COMPILER",
      "RECOVERY_PIT_LINEAGE_REF_MISSING",
      "BASELINE_REQUIRED_PERSISTENCE_OWNER_DECISION_MISSING",
      "RECOVERY_RESET_POLICY_OWNER_DECISION_MISSING",
      "RECOVERY_EFFECTIVE_TIMING_OWNER_DECISION_MISSING"
    ],
    "compiler_consumes_recovery_permission": false,
    "contract_id": "growth_tilt_recovery_persistence_contract_v1",
    "contract_ready": false,
    "create_contract": true,
    "do_not_de_risk_semantics": "DIAGNOSTIC_ONLY",
    "effective_timing": null,
    "evaluation_cadence": "TRADING_DAY_EVALUATION",
    "maximum_gap_steps": null,
    "offline_selection_pass": false,
    "offline_selection_role": "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE",
    "output_path": "channel_composer_v3_predictions.csv:re_risk_allowed_probability",
    "output_path_resolved": true,
    "pit_lineage_ref": null,
    "pit_lineage_valid": false,
    "producer_callable": true,
    "producer_entrypoint": "ai_trading_system.channel_specific_first_layer_v3._build_composer_predictions",
    "reset_on_false": null,
    "reset_on_hard_veto": true,
    "reset_on_missing": null,
    "semantics_registered": true,
    "signal_id": "re_risk_allowed_probability"
  },
  "transition_exposure": {
    "exposure": {
      "applied_target_scalar_field": null,
      "blocker_codes": [
        "BASELINE_NATIVE_EXPOSURE_SCALAR_NOT_SELECTED",
        "CURRENT_REQUESTED_APPLIED_SCALAR_FIELDS_MISSING",
        "MINIMUM_INCREMENT_OWNER_DECISION_MISSING",
        "QQQ_EQUIVALENT_ONLY_GOVERNED_FOR_CAP_NOT_CANDIDATE_DELTA"
      ],
      "candidate_delta_may_use_qqq_equivalent": false,
      "contract_id": "growth_tilt_exposure_scalar_contract_v1",
      "contract_ready": false,
      "create_contract": true,
      "current_scalar_field": null,
      "maximum_value": null,
      "minimum_increment": null,
      "minimum_value": null,
      "native_scalar_fields_ready": false,
      "native_scalar_id": null,
      "qqq_equivalent_cap": 0.75,
      "qqq_equivalent_formula": "QQQ_weight + 3.0 * TQQQ_weight",
      "qqq_equivalent_formula_callable": true,
      "qqq_equivalent_formula_ref": "ai_trading_system.two_layer_policy_compiler._apply_caps",
      "qqq_equivalent_scope": "EXISTING_CAP_ONLY_DERIVED_SCALAR_NOT_CANDIDATE_DELTA",
      "qqq_equivalent_supported": true,
      "requested_target_scalar_field": null,
      "tqqq_increase_allowed": false,
      "unit": null
    },
    "schema_version": "growth_tilt_transition_exposure_decision.v1",
    "status": "GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK_READY_OWNER_DECISIONS_REQUIRED",
    "transition": {
      "blocker_codes": [
        "REQUESTED_APPLIED_TRANSITION_INTERFACE_MISSING",
        "TRANSITION_EFFECTIVE_TIMING_OWNER_DECISION_MISSING",
        "TRANSITION_PRIORITY_POLICY_OWNER_DECISION_MISSING"
      ],
      "canonical_state_ids": [
        "risk_off",
        "defensive",
        "neutral",
        "constructive",
        "risk_on"
      ],
      "canonical_state_schema_ready": true,
      "conflict_resolution": null,
      "contract_id": "growth_tilt_regime_transition_contract_v1",
      "contract_ready": false,
      "create_contract": true,
      "effective_timing": null,
      "existing_same_row_label_mutation": true,
      "hard_veto_priority": null,
      "ordinary_request_priority": null,
      "requested_applied_split_callable": false,
      "requested_applied_split_requested": true,
      "source_field": "first_layer_composer_v2_predictions.csv:trend_state"
    }
  }
}
```

## Owner Actions

```json
[
  {
    "area": "recovery_persistence",
    "field": "baseline_required_consecutive_steps",
    "recommended_action": "Record an explicit owner decision for recovery_persistence.baseline_required_consecutive_steps.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "recovery_persistence",
    "field": "maximum_gap_steps",
    "recommended_action": "Record an explicit owner decision for recovery_persistence.maximum_gap_steps.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "recovery_persistence",
    "field": "reset_on_false",
    "recommended_action": "Record an explicit owner decision for recovery_persistence.reset_on_false.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "recovery_persistence",
    "field": "reset_on_missing",
    "recommended_action": "Record an explicit owner decision for recovery_persistence.reset_on_missing.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "recovery_persistence",
    "field": "effective_timing",
    "recommended_action": "Record an explicit owner decision for recovery_persistence.effective_timing.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "candidate_b",
    "field": "final_disposition",
    "recommended_action": "Confirm WITHDRAW or separately approve new baseline aggregate behavior; do not implement the redefinition against a missing producer.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "hard_veto",
    "field": "risk_off_veto",
    "recommended_action": "Resolve callable PIT lineage or keep M1D2/M2 blocked.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "hard_veto",
    "field": "event_risk_veto",
    "recommended_action": "Resolve callable PIT lineage or keep M1D2/M2 blocked.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "hard_veto",
    "field": "trend_break_veto",
    "recommended_action": "Resolve callable PIT lineage or keep M1D2/M2 blocked.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "transition",
    "field": "effective_timing",
    "recommended_action": "Record an explicit owner decision for transition.effective_timing.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "transition",
    "field": "hard_veto_priority",
    "recommended_action": "Record an explicit owner decision for transition.hard_veto_priority.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "transition",
    "field": "ordinary_request_priority",
    "recommended_action": "Record an explicit owner decision for transition.ordinary_request_priority.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "transition",
    "field": "conflict_resolution",
    "recommended_action": "Record an explicit owner decision for transition.conflict_resolution.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "exposure",
    "field": "native_scalar_id",
    "recommended_action": "Record an explicit owner decision for exposure.native_scalar_id.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "exposure",
    "field": "unit",
    "recommended_action": "Record an explicit owner decision for exposure.unit.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "exposure",
    "field": "minimum_value",
    "recommended_action": "Record an explicit owner decision for exposure.minimum_value.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "exposure",
    "field": "maximum_value",
    "recommended_action": "Record an explicit owner decision for exposure.maximum_value.",
    "status": "OWNER_DECISION_REQUIRED"
  },
  {
    "area": "exposure",
    "field": "minimum_increment",
    "recommended_action": "Record an explicit owner decision for exposure.minimum_increment.",
    "status": "OWNER_DECISION_REQUIRED"
  }
]
```

## 结论

A 保持 APPROVE 但 baseline contracts 未就绪；B 已转为 REDEFINE，但仓库没有 callable aggregate non-hard defensive request，继续实现前必须由 owner 确认 WITHDRAW 或单独批准新的 baseline behavior；C 保持 REDEFINE。M1D2 与 M2 均继续 blocked。
