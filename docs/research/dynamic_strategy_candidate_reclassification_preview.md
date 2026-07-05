# Dynamic strategy candidate reclassification preview

## Summary

- status：`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`
- preview 不修改真实规则，不批准 observation。

|Candidate|Current decision|Preview decision|Auto accept|Owner review|
|---|---|---|---|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`CONTINUE_OPTIMIZATION`|`OWNER_REVIEW_REQUIRED`|`False`|`True`|
|`dynamic_turnover_budgeted_growth_tilt_v1`|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|
|`dynamic_valid_until_expiry_strict_v1`|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|
|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|
|`equal_risk_growth_tilt_guarded_turnover_v1`|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|

## Raw preview

```json
{
  "dynamic_regime_overlay_v0_4_cooldown_balanced_v1": {
    "auto_accept_allowed": false,
    "candidate_type": "reference_candidate",
    "component_attribution_needed": false,
    "current_blockers": [
      "reference_candidate_hard_block",
      "time_slice_pass_rate_below_acceptance",
      "regime_slice_pass_rate_below_acceptance",
      "drawdown_not_materially_worse=false",
      "guarded_gap_negative",
      "return_advantage_retained_below_acceptance"
    ],
    "current_decision": "CONTINUE_OPTIMIZATION",
    "metrics": {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "candidate_type": "reference_candidate",
      "candidate_vs_guarded_ranking_top_gap": -0.010345,
      "candidate_vs_lower_turnover_gap": 0.00807,
      "conservative_cost_passed": true,
      "decision": "CONTINUE_OPTIMIZATION",
      "drawdown_gap_vs_static": 0.002825,
      "drawdown_not_materially_worse": false,
      "dynamic_vs_static_gap": 0.010275,
      "harsh_cost_passed": true,
      "rank": 9,
      "realistic_cost_passed": true,
      "regime_slice_pass_rate": 0.0,
      "return_advantage_retained": 0.482349,
      "signal_family": "reference_cooldown_balanced",
      "time_slice_pass_rate": 0.0,
      "turnover_budget_passed": true
    },
    "owner_review_allowed": false,
    "policy_update_applied": false,
    "preview_decision_under_calibrated_gate": "CONTINUE_OPTIMIZATION",
    "rank": 9,
    "reason": [
      "current decision remains continue optimization under calibrated preview",
      "regime slice still weak",
      "guarded gap negative"
    ]
  },
  "dynamic_turnover_budgeted_growth_tilt_v1": {
    "auto_accept_allowed": false,
    "candidate_type": "new_candidate",
    "component_attribution_needed": true,
    "current_blockers": [
      "time_slice_pass_rate_below_acceptance",
      "regime_slice_pass_rate_below_acceptance",
      "guarded_gap_negative",
      "return_advantage_retained_below_acceptance"
    ],
    "current_decision": "CONTINUE_OPTIMIZATION",
    "metrics": {
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
    "owner_review_allowed": false,
    "policy_update_applied": false,
    "preview_decision_under_calibrated_gate": "CONTINUE_OPTIMIZATION",
    "rank": 3,
    "reason": [
      "current decision remains continue optimization under calibrated preview",
      "regime slice still weak",
      "guarded gap negative",
      "turnover budget component may be useful"
    ]
  },
  "dynamic_valid_until_expiry_strict_v1": {
    "auto_accept_allowed": false,
    "candidate_type": "new_candidate",
    "component_attribution_needed": true,
    "current_blockers": [
      "time_slice_pass_rate_below_acceptance",
      "regime_slice_pass_rate_below_acceptance",
      "guarded_gap_negative",
      "return_advantage_retained_below_acceptance"
    ],
    "current_decision": "CONTINUE_OPTIMIZATION",
    "metrics": {
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
    "owner_review_allowed": false,
    "policy_update_applied": false,
    "preview_decision_under_calibrated_gate": "CONTINUE_OPTIMIZATION",
    "rank": 4,
    "reason": [
      "current decision remains continue optimization under calibrated preview",
      "regime slice still weak",
      "guarded gap negative",
      "valid-until component may be useful"
    ]
  },
  "equal_risk_growth_tilt_guarded_turnover_v1": {
    "auto_accept_allowed": false,
    "candidate_type": "reference_candidate",
    "component_attribution_needed": false,
    "current_blockers": [
      "reference_candidate_hard_block",
      "time_slice_pass_rate_below_acceptance",
      "regime_slice_pass_rate_below_acceptance",
      "drawdown_not_materially_worse=false"
    ],
    "current_decision": "CONTINUE_OPTIMIZATION",
    "metrics": {
      "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
      "candidate_type": "reference_candidate",
      "candidate_vs_guarded_ranking_top_gap": 0.0,
      "candidate_vs_lower_turnover_gap": 0.018415,
      "conservative_cost_passed": true,
      "decision": "CONTINUE_OPTIMIZATION",
      "drawdown_gap_vs_static": 0.036251,
      "drawdown_not_materially_worse": false,
      "dynamic_vs_static_gap": 0.02062,
      "harsh_cost_passed": true,
      "rank": 2,
      "realistic_cost_passed": true,
      "regime_slice_pass_rate": 0.0,
      "return_advantage_retained": 0.967984,
      "signal_family": "reference_guarded_ranking_top",
      "time_slice_pass_rate": 0.0,
      "turnover_budget_passed": true
    },
    "owner_review_allowed": false,
    "policy_update_applied": false,
    "preview_decision_under_calibrated_gate": "CONTINUE_OPTIMIZATION",
    "rank": 2,
    "reason": [
      "current decision remains continue optimization under calibrated preview",
      "regime slice still weak"
    ]
  },
  "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1": {
    "auto_accept_allowed": false,
    "candidate_type": "reference_candidate",
    "component_attribution_needed": false,
    "current_blockers": [
      "reference_candidate_hard_block",
      "time_slice_pass_rate_below_acceptance",
      "regime_slice_pass_rate_below_acceptance",
      "drawdown_not_materially_worse=false"
    ],
    "current_decision": "CONTINUE_OPTIMIZATION",
    "metrics": {
      "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "candidate_type": "reference_candidate",
      "candidate_vs_guarded_ranking_top_gap": 0.000682,
      "candidate_vs_lower_turnover_gap": 0.019097,
      "conservative_cost_passed": true,
      "decision": "CONTINUE_OPTIMIZATION",
      "drawdown_gap_vs_static": 0.043574,
      "drawdown_not_materially_worse": false,
      "dynamic_vs_static_gap": 0.021302,
      "harsh_cost_passed": true,
      "rank": 1,
      "realistic_cost_passed": true,
      "regime_slice_pass_rate": 0.0,
      "return_advantage_retained": 1.0,
      "signal_family": "reference_ranking_top",
      "time_slice_pass_rate": 0.0,
      "turnover_budget_passed": true
    },
    "owner_review_allowed": true,
    "policy_update_applied": false,
    "preview_decision_under_calibrated_gate": "OWNER_REVIEW_REQUIRED",
    "rank": 1,
    "reason": [
      "cost stress passed",
      "turnover budget passed",
      "positive dynamic_vs_static_gap",
      "reference status prevents auto-accept but should not block owner review",
      "drawdown requires owner judgment",
      "slice instability prevents auto-accept"
    ]
  }
}
```
