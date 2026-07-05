# Dynamic strategy gate policy review

## Summary

- status：`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`
- recommended action：`CALIBRATE_RESEARCH_ONLY_OBSERVATION_GATE_BEFORE_OWNER_PAUSE_DECISION`
- policy update applied：`False`
- rules mutated：`False`

## Policy review JSON

```json
{
  "current_observation_gate_rules": {
    "current_rule_summary": [
      "realistic_or_conservative_gap_less_than_or_equal_zero_rejects_candidate",
      "time_or_regime_slice_below_acceptable_threshold_continues_optimization",
      "research_only_observation_acceptance_requires_non_reference_candidate",
      "research_only_observation_acceptance_requires_lower_gap_positive",
      "research_only_observation_acceptance_requires_guarded_gap_non_negative",
      "research_only_observation_acceptance_requires_return_advantage_retained",
      "research_only_observation_acceptance_requires_time_slice_at_or_above_0_60",
      "research_only_observation_acceptance_requires_regime_slice_at_or_above_0_50"
    ],
    "policy_mutated_by_2387": false,
    "reference_candidate_current_policy": "HARD_BLOCK_ACCEPTANCE",
    "source_policy_id": "dynamic_strategy_expanded_candidate_pool_retest_v1",
    "source_task": "TRADING-2386",
    "thresholds": {
      "drawdown_worse_tolerance": 0.02,
      "observation_regime_slice_pass_rate_min": 0.5,
      "observation_time_slice_pass_rate_min": 0.6,
      "regime_slice_pass_rate_acceptable_min": 0.3,
      "return_advantage_retained_min": 0.5,
      "time_slice_pass_rate_acceptable_min": 0.4,
      "turnover_budget_max_monthly": 1.0
    }
  },
  "drawdown_materiality_review": {
    "drawdown_gap_materiality_tier": "owner_review_required",
    "drawdown_gap_vs_static": 0.043574,
    "drawdown_materiality_review_ready": true,
    "drawdown_not_materially_worse": false,
    "dynamic_vs_static_gap": 0.021302,
    "owner_review_drawdown_compensation_min": 0.25,
    "policy_update_applied": false,
    "recommended_rule_shape": {
      "auto_accept": [
        "drawdown_not_materially_worse=true"
      ],
      "continue_optimization": [
        "drawdown_not_materially_worse=false",
        "return_per_drawdown_penalty_weak=true"
      ],
      "owner_review_required": [
        "drawdown_not_materially_worse=false",
        "cost_adjusted_gap_positive=true",
        "return_per_drawdown_penalty_above_minimum=true"
      ]
    },
    "return_per_drawdown_penalty": 0.48887,
    "return_per_drawdown_penalty_above_minimum": true
  },
  "reference_candidate_policy_review": {
    "auto_accept_allowed_for_reference_candidate": false,
    "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "current_best_is_reference_candidate": true,
    "current_policy": "HARD_BLOCK_ACCEPTANCE",
    "issue": "may_exclude_current_best_candidate_purely_due_to_reference_status",
    "owner_review_allowed_for_reference_candidate": true,
    "policy_update_applied": false,
    "rationale": [
      "reference status should prevent auto-promotion",
      "reference status should not prevent research-only owner review",
      "research-only observation has no execution side effect"
    ],
    "recommended_intermediate_decision_state": "OWNER_REVIEW_FOR_RESEARCH_ONLY_OBSERVATION",
    "recommended_policy": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
    "reference_candidate_policy_review_ready": true
  },
  "regime_slice_threshold_review": {
    "current_acceptable_threshold_used_before_decision": 0.3,
    "current_acceptance_threshold": 0.5,
    "current_best_regime_slice_pass_rate": 0.0,
    "family_level_regime_findings": [
      {
        "family_best_candidate": "dynamic_valid_until_expiry_strict_v1",
        "family_failure_reason": "regime_slice_stability_failure",
        "family_regime_slice_pass_rate": 0.0,
        "signal_family": "signal_age_valid_until_family"
      },
      {
        "family_best_candidate": "dynamic_turnover_budgeted_growth_tilt_v1",
        "family_failure_reason": "regime_slice_stability_failure",
        "family_regime_slice_pass_rate": 0.0,
        "signal_family": "turnover_budget_family"
      },
      {
        "family_best_candidate": "dynamic_volatility_scaled_growth_tilt_v1",
        "family_failure_reason": "time_slice_stability_failure",
        "family_regime_slice_pass_rate": 0.0,
        "signal_family": "volatility_aware_family"
      },
      {
        "family_best_candidate": "dynamic_risk_cap_adaptive_v1",
        "family_failure_reason": "time_slice_stability_failure",
        "family_regime_slice_pass_rate": 0.0,
        "signal_family": "risk_cap_interaction_family"
      },
      {
        "family_best_candidate": "dynamic_trend_confirmed_low_turnover_v1",
        "family_failure_reason": "time_slice_stability_failure",
        "family_regime_slice_pass_rate": 0.0,
        "signal_family": "trend_confirmation_family"
      },
      {
        "family_best_candidate": "dynamic_regime_recovery_confirmation_v1",
        "family_failure_reason": "conservative_cost_failure",
        "family_regime_slice_pass_rate": 0.0,
        "signal_family": "regime_transition_family"
      }
    ],
    "growth_or_risk_on_strategy_should_not_have_to_win_every_regime": true,
    "policy_update_applied": false,
    "regime_expectation_policy": {
      "high_volatility": "drawdown_control",
      "low_volatility": "capture_upside",
      "recovery": "reentry_not_too_slow",
      "risk_off": "not_materially_worse_than_static",
      "risk_on": "outperform_or_match_static",
      "trend_confirmed": "outperform_static"
    },
    "regime_slice_threshold_review_ready": true,
    "single_global_pass_rate_may_be_too_blunt": true
  },
  "research_only_vs_paper_shadow_gate_review": {
    "finding": "The current 2386 observation gate may be too close to a paper-shadow gate because it has no owner-review-only tier.",
    "gate_levels": {
      "paper_shadow": {
        "currently_out_of_scope": true,
        "explicit_owner_approval_required": true,
        "side_effect": "creates_paper_trades_or_shadow_positions",
        "threshold": "higher"
      },
      "production_or_broker": {
        "currently_out_of_scope": true,
        "explicit_owner_approval_required": true,
        "side_effect": "real_execution_or_capital_risk",
        "threshold": "highest"
      },
      "research_only_observation": {
        "artifact_only": true,
        "owner_review_allowed": true,
        "paper_trade_created": false,
        "shadow_position_created": false,
        "side_effect": "none",
        "threshold": "lower_than_paper_shadow"
      }
    },
    "policy_update_applied": false,
    "research_only_vs_paper_shadow_gate_review_ready": true
  },
  "time_slice_threshold_review": {
    "candidate_examples_with_partial_time_evidence": [
      {
        "candidate_id": "dynamic_turnover_budgeted_growth_tilt_v1",
        "candidate_type": "new_candidate",
        "candidate_vs_guarded_ranking_top_gap": -0.013679,
        "candidate_vs_lower_turnover_gap": 0.004736,
        "conservative_cost_passed": true,
        "decision": "CONTINUE_OPTIMIZATION",
        "drawdown_gap_vs_static": -0.000389,
        "drawdown_not_materially_worse": true,
        "dynamic_vs_static_gap": 0.006941,
        "harsh_cost_passed": true,
        "rank": 3,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.325838,
        "signal_family": "turnover_budget_family",
        "time_slice_pass_rate": 0.428571,
        "turnover_budget_passed": true
      },
      {
        "candidate_id": "dynamic_valid_until_expiry_strict_v1",
        "candidate_type": "new_candidate",
        "candidate_vs_guarded_ranking_top_gap": -0.013425,
        "candidate_vs_lower_turnover_gap": 0.00499,
        "conservative_cost_passed": true,
        "decision": "CONTINUE_OPTIMIZATION",
        "drawdown_gap_vs_static": -0.005479,
        "drawdown_not_materially_worse": true,
        "dynamic_vs_static_gap": 0.007195,
        "harsh_cost_passed": true,
        "rank": 4,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.337762,
        "signal_family": "signal_age_valid_until_family",
        "time_slice_pass_rate": 0.428571,
        "turnover_budget_passed": true
      },
      {
        "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
        "candidate_type": "reference_candidate",
        "candidate_vs_guarded_ranking_top_gap": -0.018415,
        "candidate_vs_lower_turnover_gap": 0.0,
        "conservative_cost_passed": true,
        "decision": "CONTINUE_OPTIMIZATION",
        "drawdown_gap_vs_static": -0.017202,
        "drawdown_not_materially_worse": true,
        "dynamic_vs_static_gap": 0.002205,
        "harsh_cost_passed": true,
        "rank": 6,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.103511,
        "signal_family": "reference_lower_turnover",
        "time_slice_pass_rate": 0.428571,
        "turnover_budget_passed": true
      }
    ],
    "current_acceptable_threshold_used_before_decision": 0.4,
    "current_acceptance_threshold": 0.6,
    "current_best_time_slice_failure_is_real": true,
    "current_best_time_slice_pass_rate": 0.0,
    "policy_update_applied": false,
    "threshold_tiering_recommended": true,
    "time_slice_policy_tiers": {
      "accept_for_research_only_observation": {
        "meaning": "stable enough for research-only tracking",
        "threshold": 0.6
      },
      "continue_optimization": {
        "meaning": "insufficient time-slice support",
        "threshold_below": 0.3
      },
      "owner_review_required": {
        "meaning": "partial time-slice evidence, requires human review",
        "threshold": 0.3
      }
    },
    "time_slice_threshold_review_ready": true
  }
}
```
