# Dynamic strategy candidate owner review record

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- current best：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- owner decision：`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
- research-only observation approved：`False`
- candidate auto-accept approved：`False`
- owner-review-required retained：`True`

```json
{
  "calibrated_preview_decision": "OWNER_REVIEW_REQUIRED",
  "candidate_auto_accept_approved": false,
  "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "component_attribution_continue_recommended": true,
  "component_value_candidates": [
    "dynamic_turnover_budgeted_growth_tilt_v1",
    "dynamic_valid_until_expiry_strict_v1"
  ],
  "decision_rationale": [
    "2390 only produced a calibrated reclassification preview",
    "OWNER_REVIEW_REQUIRED does not equal observation approval",
    "time/regime slice instability and drawdown materiality still require owner judgment",
    "no explicit owner approval for research-only observation is present",
    "component-level follow-up remains useful before any observation protocol update"
  ],
  "failure_metrics": {
    "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "candidate_vs_guarded_ranking_top_gap": 0.000682,
    "candidate_vs_ranking_top_gap": 0.0,
    "current_gate_blockers": [
      "reference_candidate_hard_block",
      "time_slice_pass_rate_below_acceptance",
      "regime_slice_pass_rate_below_acceptance",
      "drawdown_not_materially_worse=false"
    ],
    "drawdown_gap_vs_static": 0.043574,
    "drawdown_not_materially_worse": false,
    "regime_slice_pass_rate": 0.0,
    "return_advantage_retained": 1.0,
    "time_slice_pass_rate": 0.0
  },
  "known_risk_reasons": [
    "time_slice_instability",
    "regime_slice_instability",
    "drawdown_materiality_requires_owner_judgment",
    "reference_candidate_auto_accept_blocked"
  ],
  "owner_decision": "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION",
  "owner_decision_option": "OPTION_B_KEEP_OWNER_REVIEW_REQUIRED_NO_OBSERVATION",
  "owner_review_decision_recorded": true,
  "owner_review_required_retained": true,
  "paper_shadow_approved": false,
  "previous_decision": "CONTINUE_OPTIMIZATION",
  "record_ready": true,
  "research_only_observation_approved": false,
  "supporting_metrics": {
    "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "candidate_vs_guarded_ranking_top_gap": 0.000682,
    "candidate_vs_lower_turnover_gap": 0.019097,
    "candidate_vs_ranking_top_gap": 0.0,
    "conservative_cost_passed": true,
    "cost_adjusted_dynamic_vs_static_gap": 0.021302,
    "dynamic_vs_static_gap": 0.021302,
    "harsh_cost_passed": true,
    "max_monthly_turnover": 0.276831,
    "no_stale_signal_carry_forward": true,
    "realistic_cost_passed": true,
    "regime_slice_pass_rate": 0.0,
    "return_advantage_retained": 1.0,
    "time_slice_pass_rate": 0.0,
    "turnover": 1.964574,
    "turnover_budget_passed": true,
    "valid_until_window_preserved": true
  }
}
```