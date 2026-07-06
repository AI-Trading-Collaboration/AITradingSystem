# 动态策略 component recombination candidate retest

- status：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY`
- best candidate：`growth_tilt_lower_turnover_guarded_transfer_v1`
- best decision：`OWNER_REVIEW_REQUIRED`
- next route：`TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision`

## Executive summary

TRADING-2396 使用 `valid_until_window` 主口径实际 retest 6 个 component recombination candidates。输出只用于 2397 owner review；本任务不批准 observation、paper-shadow、scheduler、event append、outcome binding、production 或 broker。

## Source plan from TRADING-2395

```json
{
  "ablation_retest_result_2393": "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY",
  "acceptance_criteria_2395": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY",
  "candidate_definitions_2395": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY",
  "component_attribution_matrix_2393": "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY",
  "component_recombination_decision_2394": "DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_RECOMBINATION_DECISION_READY",
  "expanded_candidate_ranking_2386": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY",
  "expanded_candidate_retest_2386": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY",
  "owner_review_decision_2394": "DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_RECOMBINATION_DECISION_READY",
  "recombination_candidate_plan_2395": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY",
  "retest_plan_2396": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY"
}
```

## Recombination candidate definitions

```json
[
  "growth_tilt_lower_turnover_guarded_v1",
  "growth_tilt_turnover_budgeted_v1",
  "growth_tilt_valid_until_strict_v1",
  "growth_tilt_turnover_budgeted_valid_until_strict_v1",
  "growth_tilt_lower_turnover_guarded_transfer_v1",
  "growth_tilt_conservative_guarded_v1"
]
```

## Retest design

```json
{
  "comparison_cadences": [
    "valid_until_window",
    "cooldown_limited_event_driven",
    "signal_event_driven"
  ],
  "cost_stress_scenarios": [
    {
      "constraint_value": null,
      "cooldown_days": 1,
      "max_single_step_weight_delta": null,
      "max_single_step_weight_delta_label": "unrestricted",
      "max_turnover_per_month": null,
      "max_turnover_per_month_label": "unlimited",
      "min_holding_days": 1,
      "scenario_group": "cost_stress",
      "scenario_id": "base",
      "slippage_bps": 2.0,
      "transaction_cost_bps": 2.0
    },
    {
      "constraint_value": null,
      "cooldown_days": 3,
      "max_single_step_weight_delta": null,
      "max_single_step_weight_delta_label": "unrestricted",
      "max_turnover_per_month": 1.0,
      "max_turnover_per_month_label": "1",
      "min_holding_days": 3,
      "scenario_group": "cost_stress",
      "scenario_id": "realistic",
      "slippage_bps": 5.0,
      "transaction_cost_bps": 5.0
    },
    {
      "constraint_value": null,
      "cooldown_days": 5,
      "max_single_step_weight_delta": null,
      "max_single_step_weight_delta_label": "unrestricted",
      "max_turnover_per_month": 0.5,
      "max_turnover_per_month_label": "0p5",
      "min_holding_days": 5,
      "scenario_group": "cost_stress",
      "scenario_id": "conservative",
      "slippage_bps": 10.0,
      "transaction_cost_bps": 10.0
    },
    {
      "constraint_value": null,
      "cooldown_days": 10,
      "max_single_step_weight_delta": null,
      "max_single_step_weight_delta_label": "unrestricted",
      "max_turnover_per_month": 0.25,
      "max_turnover_per_month_label": "0p25",
      "min_holding_days": 10,
      "scenario_group": "cost_stress",
      "scenario_id": "harsh",
      "slippage_bps": 10.0,
      "transaction_cost_bps": 20.0
    }
  ],
  "decision_enums": [
    "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION_PREVIEW",
    "OWNER_REVIEW_REQUIRED",
    "CONTINUE_OPTIMIZATION",
    "COMPONENT_VALUE_ONLY",
    "REJECT_FOR_NOW"
  ],
  "monthly_rebalance": {
    "allowed_for_primary_decision": false,
    "allowed_for_reference": true
  },
  "primary_execution_cadence": "valid_until_window",
  "recombination_candidates": [
    "growth_tilt_lower_turnover_guarded_v1",
    "growth_tilt_turnover_budgeted_v1",
    "growth_tilt_valid_until_strict_v1",
    "growth_tilt_turnover_budgeted_valid_until_strict_v1",
    "growth_tilt_lower_turnover_guarded_transfer_v1",
    "growth_tilt_conservative_guarded_v1"
  ],
  "reference_candidates": [
    {
      "candidate_id": "static_baseline",
      "reference_name": "static_baseline",
      "role": "baseline_reference"
    },
    {
      "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "reference_name": "raw_growth_tilt_reference",
      "role": "raw_return_engine_reference"
    },
    {
      "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
      "reference_name": "lower_turnover_reference",
      "role": "robustness_reference"
    },
    {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "reference_name": "cooldown_balanced_reference",
      "role": "best_lower_turnover_variant_reference"
    },
    {
      "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
      "reference_name": "guarded_turnover_reference",
      "role": "guarded_transfer_reference"
    }
  ],
  "regime_slices": [
    "risk_on",
    "risk_off",
    "high_volatility",
    "low_volatility",
    "trend_confirmed",
    "recovery"
  ],
  "schema_version": "dynamic_strategy_component_recombination_retest_design.v1",
  "time_slices": [
    "full_available_window",
    "recent_period",
    "post_2023_ai_cycle",
    "high_volatility_periods",
    "drawdown_recovery_periods"
  ]
}
```

## Candidate ranking

|rank|candidate|decision|annualized|drawdown|turnover|time pass|regime|cost|
|---|---|---|---|---|---|---|---|---|
|1|`growth_tilt_lower_turnover_guarded_transfer_v1`|`OWNER_REVIEW_REQUIRED`|0.208832|-0.160679|1.982428|0.000000|0.325498|`harsh`|
|2|`growth_tilt_lower_turnover_guarded_v1`|`OWNER_REVIEW_REQUIRED`|0.206981|-0.156761|1.989438|0.000000|0.322613|`harsh`|
|3|`growth_tilt_valid_until_strict_v1`|`CONTINUE_OPTIMIZATION`|0.203167|-0.138743|2.581179|0.428571|0.316668|`harsh`|
|4|`growth_tilt_turnover_budgeted_valid_until_strict_v1`|`CONTINUE_OPTIMIZATION`|0.202087|-0.138774|2.905042|0.428571|0.314985|`harsh`|
|5|`growth_tilt_turnover_budgeted_v1`|`CONTINUE_OPTIMIZATION`|0.201277|-0.150794|2.343922|0.000000|0.313722|`harsh`|
|6|`growth_tilt_conservative_guarded_v1`|`REJECT_FOR_NOW`|0.190951|-0.125600|2.609167|0.142857|0.297628|`failed`|

## Component evidence matrix

```json
[
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_lower_turnover_guarded_v1",
    "components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "valid_until_window",
      "no_stale_signal_carry_forward",
      "max_single_step_weight_delta"
    ],
    "guardrail_metrics": {
      "conservative_cost_passed": true,
      "cost_drag_reduction": -0.006878,
      "drawdown_gap_vs_static": -0.016693,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.252044,
      "realistic_cost_passed": true,
      "turnover_reduction_vs_raw_growth_tilt": -0.012656
    },
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "purpose": "combine primary return engine with lower-turnover execution guardrail",
    "recombination_quality": {
      "candidate_decision": "OWNER_REVIEW_REQUIRED",
      "cost_adjusted_return": 0.206981,
      "decision_reason": "owner_review_criteria_passed_observation_not_approved",
      "owner_review_required": true,
      "regime_expectation_score": 0.322613,
      "return_per_drawdown_penalty": 1.32036,
      "time_slice_pass_rate": 0.0
    },
    "research_only_observation_approved": false,
    "return_engine_metrics": {
      "cost_adjusted_dynamic_vs_static_gap": 0.014424,
      "dynamic_vs_static_gap": 0.014424,
      "return_retention_vs_raw_growth_tilt": 0.967839,
      "upside_capture": 0.718665
    },
    "valid_until_metrics": {
      "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_turnover_budgeted_v1",
    "components": [
      "growth_tilt_engine",
      "turnover_budgeting",
      "valid_until_window"
    ],
    "guardrail_metrics": {
      "conservative_cost_passed": true,
      "cost_drag_reduction": -0.012582,
      "drawdown_gap_vs_static": -0.010726,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.281476,
      "realistic_cost_passed": true,
      "turnover_reduction_vs_raw_growth_tilt": -0.193094
    },
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "purpose": "test whether explicit turnover budget can preserve return while reducing cost drag",
    "recombination_quality": {
      "candidate_decision": "CONTINUE_OPTIMIZATION",
      "cost_adjusted_return": 0.201277,
      "decision_reason": "positive_static_gap_but_owner_review_criteria_not_met",
      "owner_review_required": false,
      "regime_expectation_score": 0.313722,
      "return_per_drawdown_penalty": 1.334781,
      "time_slice_pass_rate": 0.0
    },
    "research_only_observation_approved": false,
    "return_engine_metrics": {
      "cost_adjusted_dynamic_vs_static_gap": 0.00872,
      "dynamic_vs_static_gap": 0.00872,
      "return_retention_vs_raw_growth_tilt": 0.941167,
      "upside_capture": 0.722354
    },
    "valid_until_metrics": {
      "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_valid_until_strict_v1",
    "components": [
      "growth_tilt_engine",
      "valid_until_strictness",
      "no_stale_signal_carry_forward"
    ],
    "guardrail_metrics": {
      "conservative_cost_passed": true,
      "cost_drag_reduction": -0.010692,
      "drawdown_gap_vs_static": 0.001325,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.298075,
      "realistic_cost_passed": true,
      "turnover_reduction_vs_raw_growth_tilt": -0.313862
    },
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "purpose": "test whether stricter signal expiry improves stale-signal discipline",
    "recombination_quality": {
      "candidate_decision": "CONTINUE_OPTIMIZATION",
      "cost_adjusted_return": 0.203167,
      "decision_reason": "positive_static_gap_but_owner_review_criteria_not_met",
      "owner_review_required": false,
      "regime_expectation_score": 0.316668,
      "return_per_drawdown_penalty": 1.464341,
      "time_slice_pass_rate": 0.428571
    },
    "research_only_observation_approved": false,
    "return_engine_metrics": {
      "cost_adjusted_dynamic_vs_static_gap": 0.01061,
      "dynamic_vs_static_gap": 0.01061,
      "return_retention_vs_raw_growth_tilt": 0.950004,
      "upside_capture": 0.691935
    },
    "valid_until_metrics": {
      "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_turnover_budgeted_valid_until_strict_v1",
    "components": [
      "growth_tilt_engine",
      "turnover_budgeting",
      "valid_until_strictness",
      "no_stale_signal_carry_forward"
    ],
    "guardrail_metrics": {
      "conservative_cost_passed": true,
      "cost_drag_reduction": -0.011772,
      "drawdown_gap_vs_static": 0.001294,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.349357,
      "realistic_cost_passed": true,
      "turnover_reduction_vs_raw_growth_tilt": -0.478713
    },
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "purpose": "combine the two most relevant execution guardrails with growth tilt",
    "recombination_quality": {
      "candidate_decision": "CONTINUE_OPTIMIZATION",
      "cost_adjusted_return": 0.202087,
      "decision_reason": "positive_static_gap_but_owner_review_criteria_not_met",
      "owner_review_required": false,
      "regime_expectation_score": 0.314985,
      "return_per_drawdown_penalty": 1.456231,
      "time_slice_pass_rate": 0.428571
    },
    "research_only_observation_approved": false,
    "return_engine_metrics": {
      "cost_adjusted_dynamic_vs_static_gap": 0.00953,
      "dynamic_vs_static_gap": 0.00953,
      "return_retention_vs_raw_growth_tilt": 0.944954,
      "upside_capture": 0.696819
    },
    "valid_until_metrics": {
      "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
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
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_conservative_guarded_v1",
    "components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "strict_risk_cap",
      "cooldown_balancing",
      "valid_until_window",
      "no_stale_signal_carry_forward"
    ],
    "guardrail_metrics": {
      "conservative_cost_passed": false,
      "cost_drag_reduction": -0.022908,
      "drawdown_gap_vs_static": 0.014468,
      "harsh_cost_passed": false,
      "max_monthly_turnover": 0.415531,
      "realistic_cost_passed": false,
      "turnover_reduction_vs_raw_growth_tilt": -0.328108
    },
    "paper_shadow_enabled": false,
    "production_enabled": false,
    "purpose": "conservative recombination for robustness stress",
    "recombination_quality": {
      "candidate_decision": "REJECT_FOR_NOW",
      "cost_adjusted_return": 0.190951,
      "decision_reason": "non_positive_realistic_cost_dynamic_vs_static_gap",
      "owner_review_required": false,
      "regime_expectation_score": 0.297628,
      "return_per_drawdown_penalty": 1.520311,
      "time_slice_pass_rate": 0.142857
    },
    "research_only_observation_approved": false,
    "return_engine_metrics": {
      "cost_adjusted_dynamic_vs_static_gap": -0.001606,
      "dynamic_vs_static_gap": -0.001606,
      "return_retention_vs_raw_growth_tilt": 0.892883,
      "upside_capture": 0.661921
    },
    "valid_until_metrics": {
      "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  }
]
```

## Cost / turnover / valid-until evidence

```json
{
  "cadence_comparison_result_count": 18,
  "cost_stress_result_count": 28
}
```

## Time / regime evidence

```json
{
  "regime_slice_result_count": 48,
  "time_slice_result_count": 42
}
```

## Decision update

```json
{
  "best_recombination_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
  "best_recombination_decision": "OWNER_REVIEW_REQUIRED",
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "candidate_decisions": {
    "growth_tilt_conservative_guarded_v1": "REJECT_FOR_NOW",
    "growth_tilt_lower_turnover_guarded_transfer_v1": "OWNER_REVIEW_REQUIRED",
    "growth_tilt_lower_turnover_guarded_v1": "OWNER_REVIEW_REQUIRED",
    "growth_tilt_turnover_budgeted_v1": "CONTINUE_OPTIMIZATION",
    "growth_tilt_turnover_budgeted_valid_until_strict_v1": "CONTINUE_OPTIMIZATION",
    "growth_tilt_valid_until_strict_v1": "CONTINUE_OPTIMIZATION"
  },
  "component_evidence_count": 6,
  "daily_report_generated": false,
  "decision_update_ready": true,
  "event_append_approved": false,
  "event_append_enabled": false,
  "next_route_reason": "TRADING-2396 can rank recombination candidates, but owner review must decide observation preview, no-approval, or continued optimization.",
  "observation_preview_candidates": [],
  "outcome_binding_approved": false,
  "outcome_binding_enabled": false,
  "owner_review_required_candidates": [
    "growth_tilt_lower_turnover_guarded_transfer_v1",
    "growth_tilt_lower_turnover_guarded_v1"
  ],
  "paper_shadow_approved": false,
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recommended_next_research_task": "TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision",
  "research_only_observation_approved": false,
  "research_only_observation_preview_exists": false,
  "scheduler_enabled": false,
  "schema_version": "dynamic_strategy_recombination_decision_update.v1"
}
```

## Explicit non-approval list

不批准 observation；不进入 paper-shadow；不启用 scheduler；不 append event；不 bind outcome；不生成 daily report；不启用 production / broker。

## Recommended next route

`TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision`
