# Dynamic strategy calibrated gate candidate owner review decision

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- as_of：`2026-07-07`
- current best：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- previous decision：`CONTINUE_OPTIMIZATION`
- calibrated preview decision：`OWNER_REVIEW_REQUIRED`
- owner decision：`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
- next route：`TRADING-2392_Dynamic_Strategy_Component_Attribution_And_Gate_Evidence_Plan`

## Executive summary

TRADING-2391 records the owner-review decision for the calibrated gate candidate. The candidate remains `OWNER_REVIEW_REQUIRED`, but research-only observation is not approved because 2390 was only a preview and no explicit owner approval was provided.

## Source findings from TRADING-2390

```json
{
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
  },
  "trading_2390": {
    "calibrated_preview_decision": "OWNER_REVIEW_REQUIRED",
    "component_value_candidates": [
      "dynamic_turnover_budgeted_growth_tilt_v1",
      "dynamic_valid_until_expiry_strict_v1"
    ],
    "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "owner_review_recommendation": "PROCEED_TO_2391_OWNER_REVIEW_DECISION_WITH_NO_OBSERVATION_APPROVAL_IN_2390",
    "previous_decision": "CONTINUE_OPTIMIZATION",
    "research_only_observation_approved": false,
    "status": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY"
  }
}
```

## Calibrated gate policy recap

- Reference policy：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`.
- `OWNER_REVIEW_REQUIRED` is a manual decision layer, not observation approval.
- Research-only observation and paper-shadow gates remain separated.

## Current best candidate owner review

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

## Owner decision

`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`

## Observation approval / non-approval record

```json
{
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "component_attribution_continue_recommended": true,
  "daily_report_generated": false,
  "event_append_approved": false,
  "event_append_enabled": false,
  "non_approval_reasons": [
    "owner approval for observation was not explicitly granted",
    "calibrated preview still carries time/regime instability",
    "drawdown materiality prevents automatic acceptance",
    "paper-shadow and execution gates remain separate and closed"
  ],
  "outcome_binding_approved": false,
  "outcome_binding_enabled": false,
  "owner_decision": "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION",
  "owner_review_required_retained": true,
  "paper_shadow_approved": false,
  "paper_shadow_enabled": false,
  "paper_trade_created": false,
  "production_enabled": false,
  "record_ready": true,
  "research_only_observation_approved": false,
  "scheduler_enabled": false,
  "shadow_position_created": false
}
```

## Component-value follow-up

- `dynamic_turnover_budgeted_growth_tilt_v1`
- `dynamic_valid_until_expiry_strict_v1`

## Explicit non-approval list

- `candidate_auto_accept`
- `research_only_observation_for_candidate`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
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
  "component_attribution_continue_recommended": true,
  "daily_report_generated": false,
  "event_append_enabled": false,
  "outcome_binding_enabled": false,
  "owner_review_required_retained": true,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "research_only_observation_approved": false,
  "scheduler_enabled": false,
  "task_boundary": "OWNER_REVIEW_DECISION_RECORD_ONLY"
}
```

## Recommended next route

```json
{
  "broker_action_enabled": false,
  "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "owner_decision": "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION",
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recommended_next_research_task": "TRADING-2392_Dynamic_Strategy_Component_Attribution_And_Gate_Evidence_Plan",
  "record_ready": true,
  "research_only_observation_approved": false,
  "route_reason": "candidate remains owner-review-required without observation approval; component attribution and gate evidence must continue first"
}
```