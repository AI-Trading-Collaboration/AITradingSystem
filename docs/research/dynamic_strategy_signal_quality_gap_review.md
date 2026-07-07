# Dynamic strategy signal quality gap review

- status：`DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`

{
  "best_signal_family_from_2386": {
    "family_average_score": 0.523172,
    "family_best_candidate": "dynamic_valid_until_expiry_strict_v1",
    "family_best_candidate_decision": "CONTINUE_OPTIMIZATION",
    "family_candidate_count": 2,
    "family_failure_reason": "regime_slice_stability_failure",
    "family_rank": 1,
    "family_regime_slice_pass_rate": 0.0,
    "family_time_slice_pass_rate": 0.214285,
    "owner_review_candidate_count": 0,
    "signal_family": "signal_age_valid_until_family"
  },
  "best_targeted_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
  "best_targeted_variant_decision": "CONTINUE_TARGETED_IMPROVEMENT",
  "candidate_plateau_interpretation": "更可能是 signal / PIT / regime / threshold evidence 质量限制，而不是单纯缺少局部 recombination variants。",
  "growth_tilt_engine": {
    "false_negative_risk": "MATERIAL_MISSED_SIGNAL_COUNT_NONZERO",
    "false_positive_risk": "MATERIAL_REGIME_AND_DRAWDOWN_FAILURE_RISK",
    "historical_stability": "MATERIAL_REVIEW_REQUIRED",
    "signal_confidence_if_available": "NOT_EXPOSED",
    "signal_decay_rule": "APPROXIMATE_VALID_UNTIL_STRICTNESS",
    "signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
    "source_features": [
      "growth_tilt_engine",
      "guarded_turnover_transfer"
    ],
    "valid_until_rule": "validity_10d_v1 / valid_until_window family"
  },
  "lower_turnover_guardrail": {
    "cooldown_rule_source": "min_holding and cooldown policy from prior retests",
    "effect_on_cost_adjusted_return": "cost stress often survives but return gap remains",
    "effect_on_return_gap": "guardrail can preserve cost but may cap growth tilt upside",
    "max_step_delta_source": "targeted variant pilot constants",
    "turnover_budget_source": "research policy / targeted variant construction"
  },
  "observation_preview_candidates_count": 0,
  "record_ready": true,
  "schema_version": "dynamic_strategy_signal_quality_gap_review.v1",
  "valid_until_strictness": {
    "near_expiry_signal_behavior": "NOT_SEPARATELY_VALIDATED",
    "signal_to_execution_lag_days": 1.0,
    "stale_signal_execution_count": 0,
    "strict_expiry_tradeoff": "strict expiry removes stale carry but may increase missed signals or turnover tradeoff"
  }
}
