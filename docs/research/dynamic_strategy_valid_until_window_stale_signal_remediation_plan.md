# Dynamic strategy valid-until window stale signal remediation plan

## Executive summary

- status：`DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`
- input under review：`valid_until_window`
- current severity：`BLOCKING`
- current PIT status：`UNKNOWN_OR_APPROXIMATE_PIT`
- valid-until semantics review ready：`True`
- stale signal risk audit ready：`True`
- signal validity contract plan ready：`True`
- growth tilt alignment review ready：`True`
- remediation plan ready：`True`
- severity downgrade conditions ready：`True`
- validation plan ready：`True`
- valid_until_window blocking gap resolved：`False`
- valid_until_window severity downgraded：`False`
- candidate search allowed：`False`
- research-only observation allowed：`False`
- paper-shadow allowed：`False`
- production allowed：`False`
- next route：`TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan`
- data quality gate：not run；reason=`NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_CONFIG_ONLY_NO_FRESH_MARKET_DATA`

## Current blocker

{
  "blocker_resolved_in_2407": false,
  "candidate_search_blocker": true,
  "input_id": "valid_until_window",
  "input_type": "EXECUTION_SEMANTIC",
  "observation_blocker": true,
  "paper_shadow_blocker": true,
  "pit_confidence": "LOW",
  "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
  "production_blocker": true,
  "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
  "risk_flags": [
    "LOOKAHEAD_RISK",
    "VALID_UNTIL_UNGROUNDED",
    "STALE_DATA_RISK"
  ],
  "semantic_role": "signal_validity_and_execution_expiry_gate",
  "severity": "BLOCKING",
  "severity_downgraded_in_2407": false,
  "source_pit_status": "UNKNOWN"
}

## Valid-until semantics review

{
  "broker_action": "none",
  "input_under_review": "valid_until_window",
  "production_effect": "none",
  "required_checks": [
    "fixed window vs signal horizon",
    "valid_from generated_at or next executable time",
    "carry-forward across refresh",
    "block after expiry",
    "near-expiry decay/block/lower-confidence",
    "signal-to-execution lag handling",
    "no-stale carry-forward consistency",
    "growth tilt horizon alignment"
  ],
  "schema_version": "dynamic_strategy_valid_until_semantics_review.v1",
  "semantics": [
    {
      "carry_forward_rule": "hold_previous_actual_position",
      "decay_rule_source": "NOT_SEPARATELY_VALIDATED",
      "expiry_rule_source": "signal_validity_window_bdays exists but natural signal expiry is not derived from signal horizon",
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
      "semantic_id": "valid_until_window",
      "semantic_role": "primary_signal_expiry_window",
      "severity": "BLOCKING",
      "signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
      "signal_to_execution_lag_rule": "execution_lag_bdays=1",
      "source_config_or_artifact": "config/research/dynamic_strategy_pit_input_registry.yaml + config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
      "stale_signal_detection_rule": "stale_signal_execution_count exists in prior review but replay contract is not deterministic",
      "used_by_candidates": [
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "valid_from_source": "not emitted per signal; policy says next_trading_day",
      "valid_until_source": "policy window=10 bdays; per-signal field missing"
    },
    {
      "carry_forward_rule": "hold_previous_actual_position currently configured",
      "decay_rule_source": "not separately validated",
      "expiry_rule_source": "owner-approved no-stale carry-forward rule missing",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "Make stale-signal suppression verifiable from generated signal metadata.",
      "semantic_id": "no_stale_signal_carry_forward",
      "semantic_role": "stale_signal_suppression_contract",
      "severity": "MATERIAL",
      "signal_horizon_source": "must be inherited from signal validity contract",
      "signal_to_execution_lag_rule": "must record lag before stale decision",
      "source_config_or_artifact": "config/research/dynamic_strategy_pit_input_registry.yaml",
      "stale_signal_detection_rule": "expired signal must be blocked or explicitly logged",
      "used_by_candidates": [
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "valid_from_source": "depends on signal artifact valid_from",
      "valid_until_source": "depends on signal artifact valid_until"
    },
    {
      "carry_forward_rule": "lagged execution can hold previous actual position",
      "decay_rule_source": "near-expiry lag handling missing",
      "expiry_rule_source": "lag currently policy-visible but not contract-bound",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "Keep lag distribution visible in gate evidence before observation approval.",
      "semantic_id": "signal_to_execution_lag",
      "semantic_role": "signal_decision_to_execution_delay",
      "severity": "MATERIAL",
      "signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
      "signal_to_execution_lag_rule": "execution_lag_bdays=1",
      "source_config_or_artifact": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.rebalance_policy",
      "stale_signal_detection_rule": "lag must be measured before execution permission",
      "used_by_candidates": [
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "valid_from_source": "next executable time after generated_at",
      "valid_until_source": "must subtract or compare execution lag against expiry"
    },
    {
      "carry_forward_rule": "hold_previous_actual_position",
      "decay_rule_source": "none in focused policy",
      "expiry_rule_source": "fixed window; needs horizon alignment",
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
      "recommended_action": "derive per-signal valid_until from signal horizon and generated_at instead of relying only on fixed window policy",
      "semantic_id": "strategy_execution_policy_signal_validity_window",
      "semantic_role": "policy_configured_fixed_window",
      "severity": "BLOCKING",
      "signal_horizon_source": "policy fixed window, not calibrated signal horizon",
      "signal_to_execution_lag_rule": "execution_lag_bdays=1",
      "source_config_or_artifact": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
      "stale_signal_detection_rule": "missing explicit stale_after field",
      "used_by_candidates": [
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "valid_from_source": "next_trading_day",
      "valid_until_source": "signal_validity_window_bdays=10"
    },
    {
      "carry_forward_rule": "allowed stale actions are governed but not signal-bound",
      "decay_rule_source": "near_stale_within_days for named profiles",
      "expiry_rule_source": "research_only_pilot_baseline taxonomy",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "bind taxonomy profile to each generated signal version before replay validation",
      "semantic_id": "signal_validity_taxonomy_profile",
      "semantic_role": "taxonomy_for_signal_half_life_and_stale_actions",
      "severity": "MATERIAL",
      "signal_horizon_source": "fast / medium / slow / persistent bands",
      "signal_to_execution_lag_rule": "not taxonomy-owned",
      "source_config_or_artifact": "config/research/signal_validity_taxonomy.yaml",
      "stale_signal_detection_rule": "profile-level stale_after_days",
      "used_by_candidates": [
        "dynamic_strategy_research_profiles"
      ],
      "valid_from_source": "taxonomy does not define per-signal valid_from",
      "valid_until_source": "taxonomy profiles define stale_after_days only for profiles"
    },
    {
      "carry_forward_rule": "hold_previous_actual_position",
      "decay_rule_source": "near-expiry behavior not separately validated",
      "expiry_rule_source": "requires TRADING-2408 implementation design",
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
      "recommended_action": "map growth tilt horizon, confidence and volatility state to valid_until/stale_after before severity downgrade",
      "semantic_id": "growth_tilt_valid_until_alignment",
      "semantic_role": "growth_tilt_signal_horizon_to_expiry_mapping",
      "severity": "BLOCKING",
      "signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
      "signal_to_execution_lag_rule": "execution_lag_bdays=1",
      "source_config_or_artifact": "TRADING-2406 signal construction gap analysis + TRADING-2403 signal construction review",
      "stale_signal_detection_rule": "must block or decay stale growth tilt signal",
      "used_by_candidates": [
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "valid_from_source": "missing in standalone growth tilt signal artifact",
      "valid_until_source": "not derived from growth tilt horizon"
    }
  ]
}

## Stale signal risk audit

{
  "blocking_risk_count": 4,
  "broker_action": "none",
  "input_under_review": "valid_until_window",
  "production_effect": "none",
  "risks": [
    {
      "affected_semantic_or_signal": "valid_until_window",
      "category": "VALID_UNTIL_UNGROUNDED",
      "evidence": "valid_until exists as policy window but is not grounded per signal",
      "recommended_fix": "emit valid_until from generated_at, valid_from and horizon",
      "remediation_required": true,
      "risk_id": "VUW-STALE-001",
      "severity": "BLOCKING"
    },
    {
      "affected_semantic_or_signal": "no_stale_signal_carry_forward",
      "category": "CARRY_FORWARD_RISK",
      "evidence": "hold_previous_actual_position can carry stale exposure without owner rule",
      "recommended_fix": "block expired carry-forward or require owner-approved rule",
      "remediation_required": true,
      "risk_id": "VUW-STALE-002",
      "severity": "BLOCKING"
    },
    {
      "affected_semantic_or_signal": "signal_to_execution_lag",
      "category": "SIGNAL_TO_EXECUTION_LAG_RISK",
      "evidence": "prior review observed lag_days=1.0; replay contract missing",
      "recommended_fix": "record lag for every signal-to-execution decision",
      "remediation_required": true,
      "risk_id": "VUW-STALE-003",
      "severity": "MATERIAL"
    },
    {
      "affected_semantic_or_signal": "valid_until_window",
      "category": "NEAR_EXPIRY_OVERTRADING_RISK",
      "evidence": "near-expiry signal behavior is not separately validated",
      "recommended_fix": "define near-expiry decay, block, or refresh-required behavior",
      "remediation_required": true,
      "risk_id": "VUW-STALE-004",
      "severity": "MATERIAL"
    },
    {
      "affected_semantic_or_signal": "signal_version",
      "category": "SIGNAL_REFRESH_COLLISION_RISK",
      "evidence": "new signal overlapping old signal lacks deterministic replacement rule",
      "recommended_fix": "prefer newer as-of-safe valid signal and log collision decision",
      "remediation_required": true,
      "risk_id": "VUW-STALE-005",
      "severity": "MATERIAL"
    },
    {
      "affected_semantic_or_signal": "growth_tilt_engine / regime context",
      "category": "STALE_REGIME_LABEL_RISK",
      "evidence": "growth tilt horizon and regime context are not tied to expiry",
      "recommended_fix": "bind regime label timestamp and validity to signal contract",
      "remediation_required": true,
      "risk_id": "VUW-STALE-006",
      "severity": "MATERIAL"
    },
    {
      "affected_semantic_or_signal": "growth_tilt_engine_signal_artifact",
      "category": "VALID_FROM_MISSING_RISK",
      "evidence": "standalone signal artifact lacks valid_from",
      "recommended_fix": "emit valid_from as generated_at or next executable time",
      "remediation_required": true,
      "risk_id": "VUW-STALE-007",
      "severity": "BLOCKING"
    },
    {
      "affected_semantic_or_signal": "growth_tilt_engine_signal_artifact",
      "category": "VALID_UNTIL_MISSING_RISK",
      "evidence": "standalone signal artifact lacks valid_until; prior stale_count=0",
      "recommended_fix": "emit valid_until and stale_after on every signal record",
      "remediation_required": true,
      "risk_id": "VUW-STALE-008",
      "severity": "BLOCKING"
    }
  ],
  "schema_version": "dynamic_strategy_stale_signal_risk_audit.v1"
}

## Signal validity contract plan

{
  "broker_action": "none",
  "contract_plan_ready": true,
  "decision_policy": {
    "current_date > stale_after": "BLOCK_OR_DECAY_SIGNAL",
    "current_date > valid_until": "BLOCK_EXECUTION",
    "missing valid_until": "BLOCK_CANDIDATE_SEARCH_FOR_DEPENDENT_STRATEGY",
    "near valid_until": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
    "new signal overlaps old": "USE_NEWER_SIGNAL_IF_AS_OF_SAFE_AND_VALID"
  },
  "example_contract_template": {
    "as_of_date": "YYYY-MM-DD",
    "confidence_if_available": null,
    "expiry_rule": "BLOCK_AFTER_VALID_UNTIL",
    "generated_at": "YYYY-MM-DDTHH:MM:SSZ",
    "horizon_days": "TBD_FROM_SIGNAL_HORIZON",
    "signal_id": "growth_tilt_engine",
    "signal_version": "deterministic_signal_version",
    "source_data_cutoff": "YYYY-MM-DD",
    "stale_after": "valid_until_or_earlier_decay_boundary",
    "valid_from": "generated_at_or_next_executable_time",
    "valid_until": "valid_from + governed_horizon(max_policy=10)"
  },
  "implemented_in_2407": false,
  "input_under_review": "valid_until_window",
  "invariants": [
    "valid_from >= generated_at_or_next_executable_time",
    "valid_until > valid_from",
    "valid_until <= valid_from + max_allowed_horizon",
    "stale_after <= valid_until",
    "expired_signal_cannot_trigger_new_trade",
    "expired_signal_cannot_be_carried_forward_without_explicit_owner_approved_rule",
    "signal_to_execution_lag_must_be_recorded"
  ],
  "production_effect": "none",
  "required_fields": [
    "signal_id",
    "as_of_date",
    "generated_at",
    "source_data_cutoff",
    "valid_from",
    "valid_until",
    "horizon_days",
    "expiry_rule",
    "stale_after",
    "confidence_if_available",
    "signal_version"
  ],
  "schema_version": "dynamic_strategy_signal_validity_contract_plan.v1",
  "source_policy_context": {
    "execution_lag_bdays": 1,
    "signal_effective_earliest": "next_trading_day",
    "signal_validity_window_bdays": 10,
    "stale_signal_behavior": "hold_previous_actual_position"
  }
}

## Growth tilt alignment review

{
  "alignment_gap_summary": {
    "confidence_to_expiry": "missing",
    "growth_tilt_signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
    "high_volatility_shrink_rule": "missing",
    "recovery_conservatism_rule": "missing",
    "valid_until_derivation": "missing per-signal deterministic mapping"
  },
  "alignment_questions": [
    "what growth_tilt horizon should valid_until derive from",
    "should valid_until shrink for weak confidence or high volatility",
    "should strong growth tilt use longer validity than weak growth tilt",
    "should recovery regimes require more conservative expiry",
    "how should lag reduce executable remaining validity"
  ],
  "broker_action": "none",
  "growth_tilt_engine_status_from_2406": "BLOCKING",
  "production_effect": "none",
  "proposed_confidence_to_expiry_mapping": [
    {
      "confidence_band": "LOW_OR_MISSING",
      "expiry_policy": "shorten validity or block until confidence exists"
    },
    {
      "confidence_band": "MEDIUM",
      "expiry_policy": "use base horizon with near-expiry refresh requirement"
    },
    {
      "confidence_band": "HIGH",
      "expiry_policy": "allow base horizon only if replay validates no stale carry"
    }
  ],
  "proposed_horizon_to_valid_until_mapping": [
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "short_growth_tilt",
      "valid_until_rule": "valid_from + short governed horizon"
    },
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "medium_growth_tilt",
      "valid_until_rule": "valid_from + medium governed horizon"
    },
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "persistent_growth_tilt",
      "valid_until_rule": "valid_from + capped persistent horizon"
    }
  ],
  "schema_version": "dynamic_strategy_growth_tilt_valid_until_alignment_review.v1",
  "trading_logic_changed_in_2407": false,
  "valid_until_window_status": "BLOCKING"
}

## Remediation plan

{
  "broker_action": "none",
  "input_under_review": "valid_until_window",
  "plan_items": [
    {
      "expected_result": "candidate search can later verify per-signal expiry",
      "goal": "define required signal validity fields and invariants",
      "implemented_in_2407": false,
      "plan_id": "P0_signal_validity_contract",
      "priority": "P0"
    },
    {
      "expected_result": "stale exposure is blocked or explicitly owner-approved",
      "goal": "expired signals cannot trigger new trades or silent carry-forward",
      "implemented_in_2407": false,
      "plan_id": "P0_no_stale_carry_forward_contract",
      "priority": "P0"
    },
    {
      "expected_result": "lag can be compared against remaining validity",
      "goal": "record lag from signal generation to execution decision",
      "implemented_in_2407": false,
      "plan_id": "P1_signal_to_execution_lag_contract",
      "priority": "P1"
    },
    {
      "expected_result": "near-expiry overtrading and stale trades are reviewable",
      "goal": "define decay, block, or refresh-required behavior near expiry",
      "implemented_in_2407": false,
      "plan_id": "P1_near_expiry_signal_handling",
      "priority": "P1"
    },
    {
      "expected_result": "growth tilt expiry matches intended signal half-life",
      "goal": "derive valid_until from signal horizon, confidence and regime state",
      "implemented_in_2407": false,
      "plan_id": "P1_horizon_to_valid_until_mapping",
      "priority": "P1"
    },
    {
      "expected_result": "future downgrade evidence can be audited",
      "goal": "replay signal validity and stale decisions under as-of constraints",
      "implemented_in_2407": false,
      "plan_id": "P2_replay_validation",
      "priority": "P2"
    }
  ],
  "production_effect": "none",
  "recommended_implementation_task": "TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan",
  "schema_version": "dynamic_strategy_valid_until_window_remediation_plan.v1",
  "valid_until_window_blocking_gap_resolved": false
}

## Severity downgrade conditions

{
  "broker_action": "none",
  "downgrade_executed_in_2407": false,
  "downgrade_from_BLOCKING_to_MATERIAL_requires": [
    "signal_validity_contract_defined",
    "valid_from_and_valid_until_fields",
    "stale_after_or_expiry_rule",
    "signal_to_execution_lag_rule",
    "no_stale_carry_forward_contract",
    "mapping_to_signal_horizon",
    "owner_review_recorded"
  ],
  "downgrade_from_MATERIAL_to_APPROVED_APPROXIMATE_PIT_requires": [
    "replay_can_reconstruct_valid_from_and_valid_until",
    "expired_signal_count_measured",
    "no_unexplained_carry_forward",
    "near_expiry_behavior_documented",
    "caveats_documented",
    "owner_approval_recorded"
  ],
  "input_under_review": "valid_until_window",
  "mark_TRUE_PIT_requires": [
    "deterministic_validity_fields",
    "source_data_cutoff_recorded",
    "no_stale_signal_execution_unless_explicitly_allowed",
    "validation_test_coverage"
  ],
  "production_effect": "none",
  "schema_version": "dynamic_strategy_valid_until_window_severity_conditions.v1"
}

## Validation plan

{
  "broker_action": "none",
  "candidate_gate": [
    "candidate search remains blocked while valid_until_window is BLOCKING",
    "gate changes only after evidence and owner review",
    "paper-shadow and production remain disabled"
  ],
  "candidate_search_remains_blocked": true,
  "input_under_review": "valid_until_window",
  "production_effect": "none",
  "recommended_next_research_task": "TRADING-2408_Dynamic_Strategy_Blocking_Gap_Remediation_Implementation_Plan",
  "recommended_next_research_task_reason": "2406 and 2407 define both blockers' remediation plans; 2408 should design implementation sequence for as-of/signal validity contracts, replay validation, and PIT severity downgrade consideration.",
  "schema_validation": [
    "required fields exist for every generated signal",
    "valid_until > valid_from",
    "stale_after <= valid_until",
    "source_data_cutoff is present and not after as_of_date"
  ],
  "schema_version": "dynamic_strategy_valid_until_window_validation_plan.v1",
  "stale_replay": [
    "expired signals do not execute",
    "signal-to-execution lag is measured",
    "near-expiry handling is deterministic",
    "carry-forward is logged or blocked"
  ],
  "validation_plan_ready": true
}

## Explicit non-approval list

- `clear_valid_until_window_blocking_gap`
- `downgrade_valid_until_window_severity`
- `mark_valid_until_window_true_pit`
- `candidate_search_resume`
- `candidate_auto_accept`
- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker_order`
- `new_strategy_backtest`
- `new_trading_signal`
- `new_scoring`
