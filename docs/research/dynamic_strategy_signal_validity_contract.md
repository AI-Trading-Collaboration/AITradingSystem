# Dynamic strategy signal validity contract

- status：`DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`

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
