# Dynamic strategy signal construction review

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY`
- signal construction review ready：`True`
- valid-until stale signal review ready：`True`

## Signal construction

{
  "growth_tilt_engine": {
    "drawdown_risk_evidence": "TRADING-2399 gate evidence still requires drawdown materiality and regime expectation review",
    "false_negative_risk": "MATERIAL_RECOVERY_REENTRY_LAG_OR_MISSED_UPSIDE",
    "false_positive_risk": "MATERIAL_HIGH_VOLATILITY_RISK_ON_OVEREXPOSURE",
    "pit_status": "UNKNOWN_UNTIL_FEATURE_LEVEL_LINEAGE_MATRIX_EXISTS",
    "recommended_fix": "build standalone growth_tilt signal construction review with source features, horizon, confidence, decay and PIT lineage",
    "return_contribution_evidence": {
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
    "signal_horizon": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
    "source_features": [
      "price-derived trend / momentum evidence",
      "growth_tilt_engine component from prior candidate family",
      "guarded_turnover_transfer evidence from recombination line"
    ],
    "valid_until_rule": "validity_10d_v1 / valid_until_window family"
  },
  "record_ready": true,
  "schema_version": "dynamic_strategy_signal_construction_review.v1",
  "signal_reliability_conclusion": "Current signal construction is reviewable but not reliable enough for observation approval because source feature lineage, signal confidence, natural expiry and regime-specific expected behavior are not normalized.",
  "source_2402_signal_gap": {
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
  },
  "turnover_budgeting": {
    "cooldown_rule_source": "min-holding and cooldown variants from prior retests",
    "effect_on_cost_adjusted_return": "guardrails can preserve cost-adjusted behavior but do not solve return-retention gap alone",
    "effect_on_return_gap": "cooldown and lower-turnover constraints may cap growth tilt upside",
    "max_step_delta_source": "targeted variant pilot constants",
    "pit_status": "APPROXIMATE_PIT_POLICY_CONSTANTS_NOT_SIGNAL_CALIBRATED",
    "recommended_fix": "separate turnover guardrail from signal alpha and manage it as an execution constraint",
    "turnover_budget_source": "research policy / candidate construction"
  }
}

## Valid-until / stale signal

{
  "near_expiry_decay_needed": true,
  "no_stale_signal_carry_forward_review": "REQUIRED_BEFORE_OBSERVATION",
  "recommended_fix": "build valid-from / valid-until lineage, signal-age buckets, near-expiry decay handling and stale-signal carry-forward assertions",
  "record_ready": true,
  "schema_version": "dynamic_strategy_valid_until_stale_signal_review.v1",
  "signal_to_execution_lag_days": 1.0,
  "signal_to_execution_lag_rule": "lag appears in execution evidence but lacks formal allowed-lag policy",
  "stale_signal_detection_rule": "count stale executions from TRADING-2399 gate evidence; reusable rule not yet extracted",
  "stale_signal_execution_count": 0,
  "strict_expiry_needed": true,
  "valid_until_pit_status": "APPROXIMATE_PIT_NOT_NATURAL_SIGNAL_EXPIRY",
  "valid_until_source": "research execution assumption from valid_until_window / validity_10d_v1"
}
