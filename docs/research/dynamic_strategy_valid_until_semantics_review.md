# Dynamic strategy valid-until semantics review

- status：`DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_SEMANTICS_AND_STALE_SIGNAL_REMEDIATION_PLAN_READY`
- semantic row count：`6`

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
