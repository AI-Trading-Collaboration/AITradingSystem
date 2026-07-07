# Dynamic strategy regime labeling review

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_AND_SIGNAL_CONSTRUCTION_REVIEW_READY`
- regime labeling review ready：`True`

{
  "current_labels": [
    "risk_on",
    "risk_off",
    "high_volatility",
    "low_volatility",
    "trend_confirmed",
    "recovery"
  ],
  "recommended_fix": "replace raw regime_slice_pass_rate with strategy-specific regime_expectation_score and explicit PIT label rules",
  "record_ready": true,
  "regime_expectation_mapping_proposal": {
    "high_volatility": "control_drawdown",
    "low_volatility": "allow_moderate_upside",
    "recovery": "avoid_excessive_reentry_lag",
    "risk_off": "not_materially_worse_than_static",
    "risk_on": "outperform_static_or_retain_upside",
    "trend_confirmed": "capture_growth_tilt_return"
  },
  "regime_expectation_not_weak": false,
  "regime_expectation_score_from_best_variant": 0.362364,
  "review_questions": {
    "different_expected_behavior_by_regime": true,
    "label_point_in_time_safe": "UNKNOWN_UNTIL_RULE_TIMING_REVIEW",
    "labels_from_explicit_rules": "PARTIAL_PRIOR_RULES_NOT_NORMALIZED",
    "should_growth_tilt_outperform_static_in_all_regimes": false,
    "uses_future_window_confirmation": "MATERIAL_REVIEW_REQUIRED"
  },
  "schema_version": "dynamic_strategy_regime_labeling_review.v1"
}
