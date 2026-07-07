# Dynamic strategy recombination candidate gate evidence plan

## Executive summary

- status：`DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY`
- as_of：`2026-07-07`
- candidate under review：`growth_tilt_lower_turnover_guarded_transfer_v1`
- decision from 2396：`OWNER_REVIEW_REQUIRED`
- owner decision from 2397：`KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE`
- research-only observation approved：`False`
- next route：`TRADING-2399_Dynamic_Strategy_Recombination_Candidate_Targeted_Gate_Evidence_Retest`

## Source findings from TRADING-2396 / 2397

```json
{
  "trading_2393": {
    "best_reusable_component": "growth_tilt_engine",
    "data_quality_status": "PASS_WITH_WARNINGS",
    "research_only_observation_approved": false,
    "status": "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY"
  },
  "trading_2395": {
    "guardrail_components": [
      "lower_turnover_guardrail",
      "valid_until_window",
      "no_stale_signal_carry_forward",
      "turnover_budgeting",
      "cooldown_balancing",
      "max_single_step_weight_delta",
      "risk_cap_preservation"
    ],
    "owner_review_components": [
      "guarded_turnover_transfer"
    ],
    "planned_recombination_candidate_count": 6,
    "return_engine_component": "growth_tilt_engine",
    "status": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY"
  },
  "trading_2396": {
    "best_recombination_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "best_recombination_decision": "OWNER_REVIEW_REQUIRED",
    "data_quality_status": "PASS_WITH_WARNINGS",
    "research_only_observation_approved": false,
    "status": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY"
  },
  "trading_2397": {
    "best_recombination_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "decision_from_2396": "OWNER_REVIEW_REQUIRED",
    "observation_preview_candidates_count": 0,
    "owner_decision": "KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE",
    "research_only_observation_approved": false,
    "status": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
  }
}
```

## Candidate under review

```json
{
  "candidate_id": "growth_tilt_lower_turnover_guarded_transfer_v1",
  "component_evidence_row": {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "guarded_turnover_transfer",
      "valid_until_window"
    ],
    "guardrail_metrics": {
      "conservative_cost_passed": true,
      "cost_drag_reduction": -0.005027,
      "drawdown_gap_vs_static": -0.020611,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.22148,
      "realistic_cost_passed": true,
      "turnover_reduction_vs_raw_growth_tilt": -0.009088
    },
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "purpose": "test guarded_turnover_transfer as owner-review component",
    "recombination_quality": {
      "candidate_decision": "OWNER_REVIEW_REQUIRED",
      "cost_adjusted_return": 0.208832,
      "decision_reason": "owner_review_criteria_passed_observation_not_approved",
      "owner_review_required": true,
      "regime_expectation_score": 0.325498,
      "return_per_drawdown_penalty": 1.299684,
      "time_slice_pass_rate": 0.0
    },
    "research_only_observation_approved": false,
    "return_engine_metrics": {
      "cost_adjusted_dynamic_vs_static_gap": 0.016275,
      "dynamic_vs_static_gap": 0.016275,
      "return_retention_vs_raw_growth_tilt": 0.976494,
      "upside_capture": 0.725894
    },
    "valid_until_metrics": {
      "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  "components": {
    "guardrails": [
      "lower_turnover_guardrail",
      "valid_until_window",
      "no_stale_signal_carry_forward"
    ],
    "owner_review_component": [
      "guarded_turnover_transfer"
    ],
    "return_engine": [
      "growth_tilt_engine"
    ],
    "source_components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "guarded_turnover_transfer",
      "valid_until_window"
    ]
  },
  "decision_from_2396": "OWNER_REVIEW_REQUIRED",
  "observation_approved": false,
  "owner_decision_from_2397": "KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE",
  "ranking_row": {
    "annualized_return": 0.208832,
    "broker_action_enabled": false,
    "candidate_id": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "cost_adjusted_return": 0.208832,
    "cost_stress_survival": "harsh",
    "decision": "OWNER_REVIEW_REQUIRED",
    "decision_reason": "owner_review_criteria_passed_observation_not_approved",
    "max_drawdown": -0.160679,
    "monthly_rebalance_allowed_for_primary_decision": false,
    "no_stale_signal_carry_forward": true,
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "rank": 1,
    "regime_expectation_score": 0.325498,
    "research_only_observation_approved": false,
    "return_retention_vs_raw_growth_tilt": 0.976494,
    "stale_signal_execution_count": 0,
    "time_slice_pass_rate": 0.0,
    "total_return": 0.965667,
    "turnover": 1.982428,
    "turnover_reduction_vs_raw_growth_tilt": -0.009088,
    "valid_until_window_preserved": true
  },
  "source_task": "TRADING-2396"
}
```

## Gate evidence gaps

|Gap|Status|Retest required|Improvement direction|
|---|---|---|---|
|`time_slice_evidence_gap`|`GAP_REMAINS`|`True`|tune_reentry_timing, reduce_drawdown_recovery_lag, preserve_valid_until_window|
|`regime_expectation_gap`|`GAP_REMAINS`|`True`|condition_growth_tilt_on_trend_confirmed, strengthen_high_volatility_risk_cap, avoid_excessive_risk_off_defensiveness|
|`drawdown_materiality_gap`|`OWNER_JUDGMENT_REQUIRED`|`True`|reduce_growth_tilt_intensity_under_high_volatility, add_drawdown_sensitive_de_risking, preserve_turnover_budget|
|`return_retention_gap`|`ADEQUATE_BUT_MONITOR`|`True`|preserve_more_raw_growth_tilt_upside, relax_guarded_transfer_only_under_trend_confirmed|
|`turnover_cost_evidence_gap`|`GAP_REMAINS`|`True`|repair guarded transfer turnover behavior, keep realistic and conservative cost survival visible|
|`valid_until_stale_signal_gap`|`PASS`|`True`|strict_signal_expiry, near_expiry_signal_decay, block_stale_signal_carry_forward|

## Targeted improvement variants

|Candidate|Purpose|Changes|
|---|---|---|
|`growth_tilt_guarded_transfer_time_slice_repair_v1`|improve weak time slices without changing core return engine|tune_reentry_timing, reduce_drawdown_recovery_lag, preserve_valid_until_window, preserve_lower_turnover_guardrail|
|`growth_tilt_guarded_transfer_regime_repair_v1`|improve behavior in weak regimes|condition_growth_tilt_on_trend_confirmed, strengthen_high_volatility_risk_cap, avoid_excessive_risk_off_defensiveness|
|`growth_tilt_guarded_transfer_drawdown_calibrated_v1`|reduce drawdown materiality gap|reduce_growth_tilt_intensity_under_high_volatility, add_drawdown_sensitive_de_risking, preserve_turnover_budget|
|`growth_tilt_guarded_transfer_return_retention_v1`|preserve more raw growth tilt upside while keeping guardrails|relax_guarded_transfer_only_under_trend_confirmed, preserve_lower_turnover_guardrail, preserve_no_stale_signal|
|`growth_tilt_guarded_transfer_valid_until_strict_v1`|strengthen signal validity evidence|strict_signal_expiry, near_expiry_signal_decay, block_stale_signal_carry_forward|
|`growth_tilt_guarded_transfer_balanced_gate_v1`|balanced candidate targeting observation preview gates|moderate_growth_tilt, lower_turnover_guardrail, strict_valid_until, high_volatility_risk_cap, cooldown_balancing|

## 2399 retest plan

```json
{
  "acceptance_criteria": {
    "observation_preview_extra_criteria": [
      "time_slice_evidence_not_weak",
      "regime_expectation_score_not_weak",
      "drawdown_materiality_not_severe",
      "no_major_guardrail_failure"
    ],
    "owner_review_candidate_criteria": {
      "must": [
        "cost_adjusted_return_above_static",
        "survives_realistic_cost",
        "survives_conservative_cost",
        "valid_until_window_preserved",
        "no_stale_signal_carry_forward",
        "drawdown_tradeoff_explainable"
      ],
      "must_not": [
        "rely_on_monthly_rebalance",
        "require_scheduler",
        "require_event_append",
        "require_outcome_binding",
        "require_paper_shadow",
        "require_production_or_broker"
      ],
      "should": [
        "improve_time_slice_evidence_vs_base_recombination_candidate",
        "improve_regime_expectation_score_vs_base_recombination_candidate",
        "preserve_meaningful_return_retention",
        "not_materially_increase_turnover",
        "not_materially_worsen_high_volatility_behavior"
      ]
    }
  },
  "candidate_under_review": "growth_tilt_lower_turnover_guarded_transfer_v1",
  "comparison_cadences": [
    "valid_until_window",
    "cooldown_limited_event_driven",
    "signal_event_driven"
  ],
  "monthly_rebalance": {
    "allowed_for_primary_decision": false,
    "allowed_for_reference": true
  },
  "primary_execution_cadence": "valid_until_window",
  "recommended_next_research_task": "TRADING-2399_Dynamic_Strategy_Recombination_Candidate_Targeted_Gate_Evidence_Retest",
  "record_ready": true,
  "required_2399_candidates": {
    "reference": [
      "static_baseline",
      "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "growth_tilt_lower_turnover_guarded_transfer_v1",
      "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
    ],
    "targeted_variants": [
      "growth_tilt_guarded_transfer_time_slice_repair_v1",
      "growth_tilt_guarded_transfer_regime_repair_v1",
      "growth_tilt_guarded_transfer_drawdown_calibrated_v1",
      "growth_tilt_guarded_transfer_return_retention_v1",
      "growth_tilt_guarded_transfer_valid_until_strict_v1",
      "growth_tilt_guarded_transfer_balanced_gate_v1"
    ]
  },
  "safety_boundary": {
    "backtest_run": false,
    "broker_action": "none",
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "daily_report_generated": false,
    "data_quality_gate_executed": false,
    "data_quality_gate_reason": "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA",
    "event_append_enabled": false,
    "fresh_market_data_read": false,
    "new_signal_generated": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "production_effect": "none",
    "production_enabled": false,
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "scoring_run": false,
    "task_boundary": "PLAN_ONLY_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT"
  },
  "stress_tests": {
    "cost": [
      "base",
      "realistic",
      "conservative",
      "harsh"
    ],
    "gate_evidence": [
      "time_slice_pass_rate",
      "regime_expectation_score",
      "return_per_drawdown_penalty",
      "stale_signal_execution_count",
      "turnover_budget_passed"
    ],
    "regime_slices": [
      "risk_on",
      "risk_off",
      "high_volatility",
      "low_volatility",
      "trend_confirmed",
      "recovery"
    ],
    "time_slices": [
      "full_available_window",
      "recent_period",
      "post_2023_ai_cycle",
      "high_volatility_periods",
      "drawdown_recovery_periods"
    ]
  }
}
```

## Acceptance criteria

```json
{
  "observation_preview_extra_criteria": [
    "time_slice_evidence_not_weak",
    "regime_expectation_score_not_weak",
    "drawdown_materiality_not_severe",
    "no_major_guardrail_failure"
  ],
  "owner_review_candidate_criteria": {
    "must": [
      "cost_adjusted_return_above_static",
      "survives_realistic_cost",
      "survives_conservative_cost",
      "valid_until_window_preserved",
      "no_stale_signal_carry_forward",
      "drawdown_tradeoff_explainable"
    ],
    "must_not": [
      "rely_on_monthly_rebalance",
      "require_scheduler",
      "require_event_append",
      "require_outcome_binding",
      "require_paper_shadow",
      "require_production_or_broker"
    ],
    "should": [
      "improve_time_slice_evidence_vs_base_recombination_candidate",
      "improve_regime_expectation_score_vs_base_recombination_candidate",
      "preserve_meaningful_return_retention",
      "not_materially_increase_turnover",
      "not_materially_worsen_high_volatility_behavior"
    ]
  }
}
```

## Explicit non-approval list

- `candidate_auto_accept`
- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker_order`
- `new_backtest`
- `new_signal`

## Guardrail summary

```json
{
  "backtest_run": false,
  "broker_action": "none",
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "daily_report_generated": false,
  "data_quality_gate_executed": false,
  "data_quality_gate_reason": "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA",
  "event_append_enabled": false,
  "fresh_market_data_read": false,
  "new_signal_generated": false,
  "outcome_binding_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "research_only_observation_approved": false,
  "scheduler_enabled": false,
  "scoring_run": false,
  "task_boundary": "PLAN_ONLY_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT"
}
```

## Recommended next route

- `TRADING-2399_Dynamic_Strategy_Recombination_Candidate_Targeted_Gate_Evidence_Retest`
