# Dynamic strategy calibrated gate candidate reclassification

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`
- as_of：`2026-07-06`
- source policy：`TRADING-2389`
- reference policy：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`
- current best：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- previous decision：`CONTINUE_OPTIMIZATION`
- preview decision：`OWNER_REVIEW_REQUIRED`
- next route：`TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_And_Observation_Decision`

## Executive summary

TRADING-2390 applies the owner-adopted calibrated research-only gate from TRADING-2389 to the TRADING-2386 expanded candidate pool. The current best candidate is reclassified as owner-review-required in preview, but no candidate is auto-accepted or approved for observation.

## Source findings

```json
{
  "trading_2365": {
    "ranking_top_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "status": "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
  },
  "trading_2366": {
    "status": "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY",
    "top_candidate_from_2365": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
  },
  "trading_2386": {
    "best_candidate_after_expanded_screening": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "best_candidate_decision": "CONTINUE_OPTIMIZATION",
    "candidate_ready_for_research_only_observation": false,
    "status": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY"
  },
  "trading_2388": {
    "reference_candidate_policy_recommendation": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
    "research_only_vs_paper_shadow_gate_separated": true,
    "status": "DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY"
  },
  "trading_2389": {
    "candidate_auto_accept_approved": false,
    "current_best_candidate_observation_approved": false,
    "owner_decision": "ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL",
    "reference_candidate_policy_adopted": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
    "status": "DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY"
  }
}
```

## Calibrated gate policy recap

- Research-only observation gate is artifact-only and side-effect-free.
- Reference candidate policy is `BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`.
- Paper-shadow, production, broker, event append and outcome binding remain disabled.

## Candidate reclassification preview

|Candidate|Role|Previous|Preview|Component value|Auto accept|Observation approved|
|---|---|---|---|---|---|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|current_best_reference_candidate|`CONTINUE_OPTIMIZATION`|`OWNER_REVIEW_REQUIRED`|`False`|`False`|`False`|
|`equal_risk_growth_tilt_guarded_turnover_v1`|guarded_return_reference|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_turnover_budgeted_growth_tilt_v1`|turnover_budget_component_case|`CONTINUE_OPTIMIZATION`|`COMPONENT_VALUE_ONLY`|`True`|`False`|`False`|
|`dynamic_valid_until_expiry_strict_v1`|valid_until_component_case|`CONTINUE_OPTIMIZATION`|`COMPONENT_VALUE_ONLY`|`True`|`False`|`False`|
|`dynamic_signal_age_decay_v1`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_regime_overlay_v0_4_lower_turnover`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_risk_cap_adaptive_v1`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_trend_confirmed_low_turnover_v1`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|robustness_reference|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_turnover_budgeted_regime_overlay_v1`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_volatility_scaled_growth_tilt_v1`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_trend_confirmed_growth_tilt_v1`|expanded_pool_candidate|`CONTINUE_OPTIMIZATION`|`CONTINUE_OPTIMIZATION`|`False`|`False`|`False`|
|`dynamic_volatility_floor_adjusted_v1`|expanded_pool_candidate|`REJECT_FOR_NOW`|`REJECT_FOR_NOW`|`False`|`False`|`False`|
|`dynamic_risk_cap_trend_conditioned_v1`|expanded_pool_candidate|`REJECT_FOR_NOW`|`REJECT_FOR_NOW`|`False`|`False`|`False`|
|`dynamic_regime_recovery_confirmation_v1`|expanded_pool_candidate|`REJECT_FOR_NOW`|`REJECT_FOR_NOW`|`False`|`False`|`False`|
|`dynamic_regime_reentry_accelerated_v1`|expanded_pool_candidate|`REJECT_FOR_NOW`|`REJECT_FOR_NOW`|`False`|`False`|`False`|

## Current best candidate review

```json
{
  "auto_accept_allowed": false,
  "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
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
  "owner_review_allowed": true,
  "preview_decision": "OWNER_REVIEW_REQUIRED",
  "previous_decision": "CONTINUE_OPTIMIZATION",
  "rationale": [
    "positive dynamic_vs_static_gap and cost-stress survival justify owner review",
    "slice instability and drawdown materiality block automatic acceptance",
    "TRADING-2390 cannot approve observation; TRADING-2391 must record owner decision"
  ],
  "research_only_observation_approved": false,
  "source_2388_likely_reclassification": "OWNER_REVIEW_REQUIRED",
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

## Component attribution review

|Component|Source candidates|Reusable|Recommended follow-up|
|---|---|---|---|
|`turnover_budgeting`|`dynamic_turnover_budgeted_growth_tilt_v1`|`True`|reuse turnover budget discipline inside a higher-return candidate without weakening regime-slice evidence|
|`valid_until_strictness`|`dynamic_valid_until_expiry_strict_v1`|`True`|test valid-until strictness as a component overlay rather than a standalone candidate|
|`growth_tilt_engine`|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`True`|keep growth tilt as the owner-review reference while repairing drawdown and slice instability|
|`lower_turnover_guardrail`|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`, `dynamic_regime_overlay_v0_4_lower_turnover`|`True`|reuse lower-turnover guardrails as constraints, not as a direct replacement for the ranking-top return engine|
|`guarded_turnover_transfer`|`equal_risk_growth_tilt_guarded_turnover_v1`|`True`|compare guarded transfer against the original ranking top in owner-review materials without treating it as approved observation|
|`risk_cap_interaction`|`dynamic_risk_cap_adaptive_v1`|`True`|retain only as component-level research until return/ranking gaps improve|
|`regime_transition_reentry`|`dynamic_regime_reentry_accelerated_v1`, `dynamic_regime_recovery_confirmation_v1`|`True`|use as diagnostic input for component-level targeted improvement|

## Owner review recommendation

```json
{
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "component_family_count": 7,
  "component_value_candidates": [
    "dynamic_turnover_budgeted_growth_tilt_v1",
    "dynamic_valid_until_expiry_strict_v1"
  ],
  "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "current_best_candidate_preview_decision": "OWNER_REVIEW_REQUIRED",
  "decision_boundary": "TRADING-2390 is preview and attribution only; any observation decision must be recorded in TRADING-2391.",
  "enter_owner_review_decision": true,
  "owner_review_recommendation_ready": true,
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recommendation": "PROCEED_TO_2391_OWNER_REVIEW_DECISION_WITH_NO_OBSERVATION_APPROVAL_IN_2390",
  "recommended_next_research_task": "TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_And_Observation_Decision",
  "research_only_observation_approved": false
}
```

## Explicit non-approval list

- `candidate_auto_accept`
- `research_only_observation_for_candidate`
- `paper_shadow`
- `event_append`
- `outcome_binding`
- `scheduler`
- `daily_report`
- `production`
- `broker_order`

## Guardrail summary

```json
{
  "broker_action": "none",
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "daily_report_generated": false,
  "event_append_enabled": false,
  "outcome_binding_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "research_only_observation_approved": false,
  "scheduler_enabled": false,
  "task_boundary": "RECLASSIFICATION_PREVIEW_AND_COMPONENT_ATTRIBUTION_ONLY"
}
```