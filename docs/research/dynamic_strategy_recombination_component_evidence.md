# 动态策略 recombination component evidence

- status：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY`

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
