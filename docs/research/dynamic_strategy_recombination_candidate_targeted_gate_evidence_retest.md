# 动态策略 recombination candidate targeted gate evidence retest

- status：`DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_READY`
- data quality：`PASS_WITH_WARNINGS`
- market regime：`ai_after_chatgpt`
- requested date range：`{
  "end": "2026-07-05",
  "start": "2022-12-01"
}`
- candidate under review：`growth_tilt_lower_turnover_guarded_transfer_v1`
- best targeted variant：`growth_tilt_guarded_transfer_valid_until_strict_v1`
- best decision：`CONTINUE_TARGETED_IMPROVEMENT`
- observation preview candidates：`0`
- next route：`TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision`

## Executive summary

TRADING-2399 在 cached-data quality gate 通过后，按 `valid_until_window` 主口径实际 retest TRADING-2398 规划的 6 个 targeted variants。结果只作为 TRADING-2400 owner decision 输入；本任务不批准 observation、paper-shadow、scheduler、event append、outcome binding、production 或 broker。

## Source plan from TRADING-2398

```json
{
  "candidate_definitions_2395": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY",
  "component_evidence_matrix_2396": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY",
  "decision_update_2396": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY",
  "gate_evidence_gap_summary_2398": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY",
  "gate_evidence_plan_result_2398": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY",
  "next_route_2398": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY",
  "owner_review_decision_2397": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY",
  "recombination_candidate_plan_2395": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY",
  "recombination_candidate_ranking_2396": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY",
  "recombination_retest_result_2396": "DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY",
  "retest_plan_2399_2398": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY",
  "targeted_improvement_plan_2398": "DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY"
}
```

## Targeted variant definitions

```json
[
  {
    "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "candidate_id": "growth_tilt_guarded_transfer_time_slice_repair_v1",
    "changes": [
      "tune_reentry_timing",
      "reduce_drawdown_recovery_lag",
      "preserve_valid_until_window",
      "preserve_lower_turnover_guardrail"
    ],
    "purpose": "improve weak time slices without changing core return engine"
  },
  {
    "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "candidate_id": "growth_tilt_guarded_transfer_regime_repair_v1",
    "changes": [
      "condition_growth_tilt_on_trend_confirmed",
      "strengthen_high_volatility_risk_cap",
      "avoid_excessive_risk_off_defensiveness"
    ],
    "purpose": "improve behavior in weak regimes"
  },
  {
    "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "candidate_id": "growth_tilt_guarded_transfer_drawdown_calibrated_v1",
    "changes": [
      "reduce_growth_tilt_intensity_under_high_volatility",
      "add_drawdown_sensitive_de_risking",
      "preserve_turnover_budget"
    ],
    "purpose": "reduce drawdown materiality gap"
  },
  {
    "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "candidate_id": "growth_tilt_guarded_transfer_return_retention_v1",
    "changes": [
      "relax_guarded_transfer_only_under_trend_confirmed",
      "preserve_lower_turnover_guardrail",
      "preserve_no_stale_signal"
    ],
    "purpose": "preserve more raw growth tilt upside while keeping guardrails"
  },
  {
    "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "candidate_id": "growth_tilt_guarded_transfer_valid_until_strict_v1",
    "changes": [
      "strict_signal_expiry",
      "near_expiry_signal_decay",
      "block_stale_signal_carry_forward"
    ],
    "purpose": "strengthen signal validity evidence"
  },
  {
    "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "candidate_id": "growth_tilt_guarded_transfer_balanced_gate_v1",
    "changes": [
      "moderate_growth_tilt",
      "lower_turnover_guardrail",
      "strict_valid_until",
      "high_volatility_risk_cap",
      "cooldown_balancing"
    ],
    "purpose": "balanced candidate targeting observation preview gates"
  }
]
```

## Retest design

```json
{
  "candidate_under_review": "growth_tilt_lower_turnover_guarded_transfer_v1",
  "comparison_cadences": [
    "valid_until_window",
    "cooldown_limited_event_driven",
    "signal_event_driven"
  ],
  "construction_governance": {
    "not_production_policy": true,
    "owner_review_required_before_reuse": true,
    "status": "research_only_pilot_baseline"
  },
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
    "CONTINUE_TARGETED_IMPROVEMENT",
    "COMPONENT_VALUE_ONLY",
    "REJECT_FOR_NOW"
  ],
  "monthly_rebalance": {
    "allowed_for_primary_decision": false,
    "allowed_for_reference": true
  },
  "primary_execution_cadence": "valid_until_window",
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
      "candidate_id": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "reference_name": "base_recombination_candidate",
      "role": "current_owner_review_candidate"
    },
    {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "reference_name": "lower_turnover_reference",
      "role": "guardrail_reference"
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
  "schema_version": "dynamic_strategy_targeted_gate_evidence_retest_design.v1",
  "targeted_variants": [
    "growth_tilt_guarded_transfer_time_slice_repair_v1",
    "growth_tilt_guarded_transfer_regime_repair_v1",
    "growth_tilt_guarded_transfer_drawdown_calibrated_v1",
    "growth_tilt_guarded_transfer_return_retention_v1",
    "growth_tilt_guarded_transfer_valid_until_strict_v1",
    "growth_tilt_guarded_transfer_balanced_gate_v1"
  ],
  "time_slices": [
    "full_available_window",
    "recent_period",
    "post_2023_ai_cycle",
    "high_volatility_periods",
    "drawdown_recovery_periods"
  ]
}
```

## Variant ranking

|rank|candidate|decision|annualized|drawdown|turnover|time delta|regime delta|cost|
|---|---|---|---|---|---|---|---|---|
|1|`growth_tilt_guarded_transfer_valid_until_strict_v1`|`CONTINUE_TARGETED_IMPROVEMENT`|0.205752|-0.138510|2.608204|0.428571|0.362364|`harsh`|
|2|`growth_tilt_guarded_transfer_balanced_gate_v1`|`CONTINUE_TARGETED_IMPROVEMENT`|0.205031|-0.138552|2.645703|0.285714|0.361240|`harsh`|
|3|`growth_tilt_guarded_transfer_drawdown_calibrated_v1`|`CONTINUE_TARGETED_IMPROVEMENT`|0.195207|-0.135206|2.651344|0.285714|0.304261|`harsh`|
|4|`growth_tilt_guarded_transfer_return_retention_v1`|`CONTINUE_TARGETED_IMPROVEMENT`|0.210039|-0.169851|2.279705|0.000000|0.327379|`harsh`|
|5|`growth_tilt_guarded_transfer_time_slice_repair_v1`|`CONTINUE_TARGETED_IMPROVEMENT`|0.209365|-0.165502|2.521148|0.000000|0.326329|`harsh`|
|6|`growth_tilt_guarded_transfer_regime_repair_v1`|`CONTINUE_TARGETED_IMPROVEMENT`|0.199687|-0.150112|2.498819|0.000000|0.311244|`harsh`|

## Gate evidence matrix

```json
[
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_guarded_transfer_time_slice_repair_v1",
    "daily_report_generated": false,
    "decision_evidence": {
      "candidate_decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "decision_reason": "positive_static_gap_but_targeted_gate_criteria_not_met",
      "observation_preview_candidate": false,
      "owner_review_required": false,
      "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
      "research_only_observation_approved": false
    },
    "drawdown_return_evidence": {
      "drawdown_improvement_vs_base": -0.004823,
      "drawdown_materiality_tier": "NOT_SEVERE",
      "return_per_drawdown_penalty": 1.26503,
      "return_retention_vs_base_recombination": 1.002552,
      "return_retention_vs_raw_growth_tilt": 0.978986
    },
    "event_append_enabled": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "performance_metrics": {
      "annualized_return": 0.209365,
      "downside_capture": 0.740111,
      "max_drawdown": -0.165502,
      "sharpe_or_sortino_if_available": 1.973965,
      "total_return": 0.968759,
      "upside_capture": 0.737912,
      "volatility": 0.148153
    },
    "production_enabled": false,
    "regime_expectation_evidence": {
      "regime_expectation_improvement_vs_base": 0.326329,
      "regime_expectation_not_weak": false,
      "regime_expectation_score": 0.326329
    },
    "relative_metrics": {
      "candidate_vs_base_recombination_gap": 0.000533,
      "candidate_vs_lower_turnover_reference_gap": 0.006533,
      "candidate_vs_raw_growth_tilt_gap": -0.004494,
      "cost_adjusted_dynamic_vs_static_gap": 0.016808,
      "drawdown_gap_vs_static": -0.025434,
      "drawdown_improvement_vs_base": -0.004823,
      "dynamic_vs_ranking_top_gap": -0.004494,
      "dynamic_vs_static_gap": 0.016808,
      "retest_slice_vs_full_sample_gap": 0.0,
      "return_retention_vs_base_recombination": 1.002552,
      "return_retention_vs_raw_growth_tilt": 0.978986,
      "turnover_change_vs_base_recombination": 0.53872,
      "turnover_reduction_vs_raw_growth_tilt": -0.283305
    },
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "source_gap_targets": {
      "drawdown_materiality_gap": {
        "drawdown_gap_vs_static": -0.020611,
        "drawdown_materiality_tier": "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST",
        "retest_required": true,
        "return_per_drawdown_penalty": 1.299684,
        "status": "OWNER_JUDGMENT_REQUIRED",
        "targeted_fix": [
          "reduce_growth_tilt_intensity_under_high_volatility",
          "add_drawdown_sensitive_de_risking",
          "preserve_turnover_budget"
        ]
      },
      "regime_expectation_gap": {
        "affected_regimes": [
          "risk_on",
          "risk_off",
          "high_volatility",
          "trend_confirmed",
          "recovery"
        ],
        "expected_behavior": [
          "retain risk-on upside",
          "avoid risk-off deterioration",
          "control high-volatility drawdown",
          "capture trend-confirmed growth tilt"
        ],
        "improvement_direction": [
          "condition_growth_tilt_on_trend_confirmed",
          "strengthen_high_volatility_risk_cap",
          "avoid_excessive_risk_off_defensiveness"
        ],
        "observed_issue": "regime_expectation_score remains below 2396 preview reference",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.325498,
        "status": "GAP_REMAINS"
      },
      "return_retention_gap": {
        "improvement_direction": [
          "preserve_more_raw_growth_tilt_upside",
          "relax_guarded_transfer_only_under_trend_confirmed"
        ],
        "retest_required": true,
        "return_gap_vs_raw_growth_tilt": 0.023506,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "status": "ADEQUATE_BUT_MONITOR",
        "upside_capture_gap": 0.274106
      },
      "time_slice_evidence_gap": {
        "affected_time_slices": [
          "full_available_window",
          "recent_period",
          "post_2023_ai_cycle",
          "high_volatility_periods",
          "drawdown_recovery_periods"
        ],
        "improvement_direction": [
          "tune_reentry_timing",
          "reduce_drawdown_recovery_lag",
          "preserve_valid_until_window"
        ],
        "likely_failure_reason": "aggregate time_slice_pass_rate remains below 2396 preview reference; targeted retest must isolate weak slices",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.0,
        "status": "GAP_REMAINS"
      },
      "turnover_cost_evidence_gap": {
        "conservative_cost_status": "PASS",
        "cost_drag_reduction": -0.005027,
        "harsh_cost_status": "PASS",
        "improvement_direction": [
          "repair guarded transfer turnover behavior",
          "keep realistic and conservative cost survival visible"
        ],
        "realistic_cost_status": "PASS",
        "retest_required": true,
        "status": "GAP_REMAINS",
        "turnover_reduction_vs_raw_growth_tilt": -0.009088
      },
      "valid_until_stale_signal_gap": {
        "improvement_direction": [
          "strict_signal_expiry",
          "near_expiry_signal_decay",
          "block_stale_signal_carry_forward"
        ],
        "no_stale_signal_carry_forward": true,
        "retest_required": true,
        "signal_to_execution_lag_days": 1.0,
        "stale_signal_execution_count": 0.0,
        "status": "PASS",
        "valid_until_fix_required": false,
        "valid_until_window_preserved": true
      }
    },
    "source_variant_definition": {
      "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "candidate_id": "growth_tilt_guarded_transfer_time_slice_repair_v1",
      "changes": [
        "tune_reentry_timing",
        "reduce_drawdown_recovery_lag",
        "preserve_valid_until_window",
        "preserve_lower_turnover_guardrail"
      ],
      "purpose": "improve weak time slices without changing core return engine"
    },
    "time_slice_evidence": {
      "time_slice_improvement_vs_base": 0.0,
      "time_slice_not_weak": false,
      "time_slice_pass_rate": 0.0
    },
    "turnover_cost_evidence": {
      "conservative_cost_passed": true,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.272623,
      "realistic_cost_passed": true,
      "turnover": 2.521148,
      "turnover_change_vs_base_recombination": 0.53872,
      "turnover_reduction_vs_raw_growth_tilt": -0.283305
    },
    "valid_until_stale_signal_evidence": {
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_guarded_transfer_regime_repair_v1",
    "daily_report_generated": false,
    "decision_evidence": {
      "candidate_decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "decision_reason": "positive_static_gap_but_targeted_gate_criteria_not_met",
      "observation_preview_candidate": false,
      "owner_review_required": false,
      "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
      "research_only_observation_approved": false
    },
    "drawdown_return_evidence": {
      "drawdown_improvement_vs_base": 0.010567,
      "drawdown_materiality_tier": "NOT_SEVERE",
      "return_per_drawdown_penalty": 1.330253,
      "return_retention_vs_base_recombination": 0.956209,
      "return_retention_vs_raw_growth_tilt": 0.933732
    },
    "event_append_enabled": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "performance_metrics": {
      "annualized_return": 0.199687,
      "downside_capture": 0.719484,
      "max_drawdown": -0.150112,
      "sharpe_or_sortino_if_available": 1.977318,
      "total_return": 0.913191,
      "upside_capture": 0.714742,
      "volatility": 0.141427
    },
    "production_enabled": false,
    "regime_expectation_evidence": {
      "regime_expectation_improvement_vs_base": 0.311244,
      "regime_expectation_not_weak": false,
      "regime_expectation_score": 0.311244
    },
    "relative_metrics": {
      "candidate_vs_base_recombination_gap": -0.009145,
      "candidate_vs_lower_turnover_reference_gap": -0.003145,
      "candidate_vs_raw_growth_tilt_gap": -0.014172,
      "cost_adjusted_dynamic_vs_static_gap": 0.00713,
      "drawdown_gap_vs_static": -0.010044,
      "drawdown_improvement_vs_base": 0.010567,
      "dynamic_vs_ranking_top_gap": -0.014172,
      "dynamic_vs_static_gap": 0.00713,
      "retest_slice_vs_full_sample_gap": 0.0,
      "return_retention_vs_base_recombination": 0.956209,
      "return_retention_vs_raw_growth_tilt": 0.933732,
      "turnover_change_vs_base_recombination": 0.516391,
      "turnover_reduction_vs_raw_growth_tilt": -0.271939
    },
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "source_gap_targets": {
      "drawdown_materiality_gap": {
        "drawdown_gap_vs_static": -0.020611,
        "drawdown_materiality_tier": "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST",
        "retest_required": true,
        "return_per_drawdown_penalty": 1.299684,
        "status": "OWNER_JUDGMENT_REQUIRED",
        "targeted_fix": [
          "reduce_growth_tilt_intensity_under_high_volatility",
          "add_drawdown_sensitive_de_risking",
          "preserve_turnover_budget"
        ]
      },
      "regime_expectation_gap": {
        "affected_regimes": [
          "risk_on",
          "risk_off",
          "high_volatility",
          "trend_confirmed",
          "recovery"
        ],
        "expected_behavior": [
          "retain risk-on upside",
          "avoid risk-off deterioration",
          "control high-volatility drawdown",
          "capture trend-confirmed growth tilt"
        ],
        "improvement_direction": [
          "condition_growth_tilt_on_trend_confirmed",
          "strengthen_high_volatility_risk_cap",
          "avoid_excessive_risk_off_defensiveness"
        ],
        "observed_issue": "regime_expectation_score remains below 2396 preview reference",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.325498,
        "status": "GAP_REMAINS"
      },
      "return_retention_gap": {
        "improvement_direction": [
          "preserve_more_raw_growth_tilt_upside",
          "relax_guarded_transfer_only_under_trend_confirmed"
        ],
        "retest_required": true,
        "return_gap_vs_raw_growth_tilt": 0.023506,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "status": "ADEQUATE_BUT_MONITOR",
        "upside_capture_gap": 0.274106
      },
      "time_slice_evidence_gap": {
        "affected_time_slices": [
          "full_available_window",
          "recent_period",
          "post_2023_ai_cycle",
          "high_volatility_periods",
          "drawdown_recovery_periods"
        ],
        "improvement_direction": [
          "tune_reentry_timing",
          "reduce_drawdown_recovery_lag",
          "preserve_valid_until_window"
        ],
        "likely_failure_reason": "aggregate time_slice_pass_rate remains below 2396 preview reference; targeted retest must isolate weak slices",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.0,
        "status": "GAP_REMAINS"
      },
      "turnover_cost_evidence_gap": {
        "conservative_cost_status": "PASS",
        "cost_drag_reduction": -0.005027,
        "harsh_cost_status": "PASS",
        "improvement_direction": [
          "repair guarded transfer turnover behavior",
          "keep realistic and conservative cost survival visible"
        ],
        "realistic_cost_status": "PASS",
        "retest_required": true,
        "status": "GAP_REMAINS",
        "turnover_reduction_vs_raw_growth_tilt": -0.009088
      },
      "valid_until_stale_signal_gap": {
        "improvement_direction": [
          "strict_signal_expiry",
          "near_expiry_signal_decay",
          "block_stale_signal_carry_forward"
        ],
        "no_stale_signal_carry_forward": true,
        "retest_required": true,
        "signal_to_execution_lag_days": 1.0,
        "stale_signal_execution_count": 0.0,
        "status": "PASS",
        "valid_until_fix_required": false,
        "valid_until_window_preserved": true
      }
    },
    "source_variant_definition": {
      "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "candidate_id": "growth_tilt_guarded_transfer_regime_repair_v1",
      "changes": [
        "condition_growth_tilt_on_trend_confirmed",
        "strengthen_high_volatility_risk_cap",
        "avoid_excessive_risk_off_defensiveness"
      ],
      "purpose": "improve behavior in weak regimes"
    },
    "time_slice_evidence": {
      "time_slice_improvement_vs_base": 0.0,
      "time_slice_not_weak": false,
      "time_slice_pass_rate": 0.0
    },
    "turnover_cost_evidence": {
      "conservative_cost_passed": true,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.407018,
      "realistic_cost_passed": true,
      "turnover": 2.498819,
      "turnover_change_vs_base_recombination": 0.516391,
      "turnover_reduction_vs_raw_growth_tilt": -0.271939
    },
    "valid_until_stale_signal_evidence": {
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_guarded_transfer_drawdown_calibrated_v1",
    "daily_report_generated": false,
    "decision_evidence": {
      "candidate_decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "decision_reason": "positive_static_gap_but_targeted_gate_criteria_not_met",
      "observation_preview_candidate": false,
      "owner_review_required": false,
      "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
      "research_only_observation_approved": false
    },
    "drawdown_return_evidence": {
      "drawdown_improvement_vs_base": 0.025473,
      "drawdown_materiality_tier": "NOT_SEVERE",
      "return_per_drawdown_penalty": 1.443775,
      "return_retention_vs_base_recombination": 0.934756,
      "return_retention_vs_raw_growth_tilt": 0.912784
    },
    "event_append_enabled": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "performance_metrics": {
      "annualized_return": 0.195207,
      "downside_capture": 0.67771,
      "max_drawdown": -0.135206,
      "sharpe_or_sortino_if_available": 2.092478,
      "total_return": 0.887855,
      "upside_capture": 0.677946,
      "volatility": 0.132011
    },
    "production_enabled": false,
    "regime_expectation_evidence": {
      "regime_expectation_improvement_vs_base": 0.304261,
      "regime_expectation_not_weak": false,
      "regime_expectation_score": 0.304261
    },
    "relative_metrics": {
      "candidate_vs_base_recombination_gap": -0.013625,
      "candidate_vs_lower_turnover_reference_gap": -0.007625,
      "candidate_vs_raw_growth_tilt_gap": -0.018652,
      "cost_adjusted_dynamic_vs_static_gap": 0.00265,
      "drawdown_gap_vs_static": 0.004862,
      "drawdown_improvement_vs_base": 0.025473,
      "dynamic_vs_ranking_top_gap": -0.018652,
      "dynamic_vs_static_gap": 0.00265,
      "retest_slice_vs_full_sample_gap": 0.0,
      "return_retention_vs_base_recombination": 0.934756,
      "return_retention_vs_raw_growth_tilt": 0.912784,
      "turnover_change_vs_base_recombination": 0.668916,
      "turnover_reduction_vs_raw_growth_tilt": -0.349577
    },
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "source_gap_targets": {
      "drawdown_materiality_gap": {
        "drawdown_gap_vs_static": -0.020611,
        "drawdown_materiality_tier": "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST",
        "retest_required": true,
        "return_per_drawdown_penalty": 1.299684,
        "status": "OWNER_JUDGMENT_REQUIRED",
        "targeted_fix": [
          "reduce_growth_tilt_intensity_under_high_volatility",
          "add_drawdown_sensitive_de_risking",
          "preserve_turnover_budget"
        ]
      },
      "regime_expectation_gap": {
        "affected_regimes": [
          "risk_on",
          "risk_off",
          "high_volatility",
          "trend_confirmed",
          "recovery"
        ],
        "expected_behavior": [
          "retain risk-on upside",
          "avoid risk-off deterioration",
          "control high-volatility drawdown",
          "capture trend-confirmed growth tilt"
        ],
        "improvement_direction": [
          "condition_growth_tilt_on_trend_confirmed",
          "strengthen_high_volatility_risk_cap",
          "avoid_excessive_risk_off_defensiveness"
        ],
        "observed_issue": "regime_expectation_score remains below 2396 preview reference",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.325498,
        "status": "GAP_REMAINS"
      },
      "return_retention_gap": {
        "improvement_direction": [
          "preserve_more_raw_growth_tilt_upside",
          "relax_guarded_transfer_only_under_trend_confirmed"
        ],
        "retest_required": true,
        "return_gap_vs_raw_growth_tilt": 0.023506,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "status": "ADEQUATE_BUT_MONITOR",
        "upside_capture_gap": 0.274106
      },
      "time_slice_evidence_gap": {
        "affected_time_slices": [
          "full_available_window",
          "recent_period",
          "post_2023_ai_cycle",
          "high_volatility_periods",
          "drawdown_recovery_periods"
        ],
        "improvement_direction": [
          "tune_reentry_timing",
          "reduce_drawdown_recovery_lag",
          "preserve_valid_until_window"
        ],
        "likely_failure_reason": "aggregate time_slice_pass_rate remains below 2396 preview reference; targeted retest must isolate weak slices",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.0,
        "status": "GAP_REMAINS"
      },
      "turnover_cost_evidence_gap": {
        "conservative_cost_status": "PASS",
        "cost_drag_reduction": -0.005027,
        "harsh_cost_status": "PASS",
        "improvement_direction": [
          "repair guarded transfer turnover behavior",
          "keep realistic and conservative cost survival visible"
        ],
        "realistic_cost_status": "PASS",
        "retest_required": true,
        "status": "GAP_REMAINS",
        "turnover_reduction_vs_raw_growth_tilt": -0.009088
      },
      "valid_until_stale_signal_gap": {
        "improvement_direction": [
          "strict_signal_expiry",
          "near_expiry_signal_decay",
          "block_stale_signal_carry_forward"
        ],
        "no_stale_signal_carry_forward": true,
        "retest_required": true,
        "signal_to_execution_lag_days": 1.0,
        "stale_signal_execution_count": 0.0,
        "status": "PASS",
        "valid_until_fix_required": false,
        "valid_until_window_preserved": true
      }
    },
    "source_variant_definition": {
      "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "candidate_id": "growth_tilt_guarded_transfer_drawdown_calibrated_v1",
      "changes": [
        "reduce_growth_tilt_intensity_under_high_volatility",
        "add_drawdown_sensitive_de_risking",
        "preserve_turnover_budget"
      ],
      "purpose": "reduce drawdown materiality gap"
    },
    "time_slice_evidence": {
      "time_slice_improvement_vs_base": 0.285714,
      "time_slice_not_weak": false,
      "time_slice_pass_rate": 0.285714
    },
    "turnover_cost_evidence": {
      "conservative_cost_passed": true,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.532262,
      "realistic_cost_passed": true,
      "turnover": 2.651344,
      "turnover_change_vs_base_recombination": 0.668916,
      "turnover_reduction_vs_raw_growth_tilt": -0.349577
    },
    "valid_until_stale_signal_evidence": {
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_guarded_transfer_return_retention_v1",
    "daily_report_generated": false,
    "decision_evidence": {
      "candidate_decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "decision_reason": "positive_static_gap_but_targeted_gate_criteria_not_met",
      "observation_preview_candidate": false,
      "owner_review_required": false,
      "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
      "research_only_observation_approved": false
    },
    "drawdown_return_evidence": {
      "drawdown_improvement_vs_base": -0.009172,
      "drawdown_materiality_tier": "NOT_SEVERE",
      "return_per_drawdown_penalty": 1.236607,
      "return_retention_vs_base_recombination": 1.00578,
      "return_retention_vs_raw_growth_tilt": 0.982138
    },
    "event_append_enabled": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "performance_metrics": {
      "annualized_return": 0.210039,
      "downside_capture": 0.750051,
      "max_drawdown": -0.169851,
      "sharpe_or_sortino_if_available": 1.932681,
      "total_return": 0.97267,
      "upside_capture": 0.74647,
      "volatility": 0.150713
    },
    "production_enabled": false,
    "regime_expectation_evidence": {
      "regime_expectation_improvement_vs_base": 0.327379,
      "regime_expectation_not_weak": false,
      "regime_expectation_score": 0.327379
    },
    "relative_metrics": {
      "candidate_vs_base_recombination_gap": 0.001207,
      "candidate_vs_lower_turnover_reference_gap": 0.007207,
      "candidate_vs_raw_growth_tilt_gap": -0.00382,
      "cost_adjusted_dynamic_vs_static_gap": 0.017482,
      "drawdown_gap_vs_static": -0.029783,
      "drawdown_improvement_vs_base": -0.009172,
      "dynamic_vs_ranking_top_gap": -0.00382,
      "dynamic_vs_static_gap": 0.017482,
      "retest_slice_vs_full_sample_gap": 0.0,
      "return_retention_vs_base_recombination": 1.00578,
      "return_retention_vs_raw_growth_tilt": 0.982138,
      "turnover_change_vs_base_recombination": 0.297277,
      "turnover_reduction_vs_raw_growth_tilt": -0.160407
    },
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "source_gap_targets": {
      "drawdown_materiality_gap": {
        "drawdown_gap_vs_static": -0.020611,
        "drawdown_materiality_tier": "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST",
        "retest_required": true,
        "return_per_drawdown_penalty": 1.299684,
        "status": "OWNER_JUDGMENT_REQUIRED",
        "targeted_fix": [
          "reduce_growth_tilt_intensity_under_high_volatility",
          "add_drawdown_sensitive_de_risking",
          "preserve_turnover_budget"
        ]
      },
      "regime_expectation_gap": {
        "affected_regimes": [
          "risk_on",
          "risk_off",
          "high_volatility",
          "trend_confirmed",
          "recovery"
        ],
        "expected_behavior": [
          "retain risk-on upside",
          "avoid risk-off deterioration",
          "control high-volatility drawdown",
          "capture trend-confirmed growth tilt"
        ],
        "improvement_direction": [
          "condition_growth_tilt_on_trend_confirmed",
          "strengthen_high_volatility_risk_cap",
          "avoid_excessive_risk_off_defensiveness"
        ],
        "observed_issue": "regime_expectation_score remains below 2396 preview reference",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.325498,
        "status": "GAP_REMAINS"
      },
      "return_retention_gap": {
        "improvement_direction": [
          "preserve_more_raw_growth_tilt_upside",
          "relax_guarded_transfer_only_under_trend_confirmed"
        ],
        "retest_required": true,
        "return_gap_vs_raw_growth_tilt": 0.023506,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "status": "ADEQUATE_BUT_MONITOR",
        "upside_capture_gap": 0.274106
      },
      "time_slice_evidence_gap": {
        "affected_time_slices": [
          "full_available_window",
          "recent_period",
          "post_2023_ai_cycle",
          "high_volatility_periods",
          "drawdown_recovery_periods"
        ],
        "improvement_direction": [
          "tune_reentry_timing",
          "reduce_drawdown_recovery_lag",
          "preserve_valid_until_window"
        ],
        "likely_failure_reason": "aggregate time_slice_pass_rate remains below 2396 preview reference; targeted retest must isolate weak slices",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.0,
        "status": "GAP_REMAINS"
      },
      "turnover_cost_evidence_gap": {
        "conservative_cost_status": "PASS",
        "cost_drag_reduction": -0.005027,
        "harsh_cost_status": "PASS",
        "improvement_direction": [
          "repair guarded transfer turnover behavior",
          "keep realistic and conservative cost survival visible"
        ],
        "realistic_cost_status": "PASS",
        "retest_required": true,
        "status": "GAP_REMAINS",
        "turnover_reduction_vs_raw_growth_tilt": -0.009088
      },
      "valid_until_stale_signal_gap": {
        "improvement_direction": [
          "strict_signal_expiry",
          "near_expiry_signal_decay",
          "block_stale_signal_carry_forward"
        ],
        "no_stale_signal_carry_forward": true,
        "retest_required": true,
        "signal_to_execution_lag_days": 1.0,
        "stale_signal_execution_count": 0.0,
        "status": "PASS",
        "valid_until_fix_required": false,
        "valid_until_window_preserved": true
      }
    },
    "source_variant_definition": {
      "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "candidate_id": "growth_tilt_guarded_transfer_return_retention_v1",
      "changes": [
        "relax_guarded_transfer_only_under_trend_confirmed",
        "preserve_lower_turnover_guardrail",
        "preserve_no_stale_signal"
      ],
      "purpose": "preserve more raw growth tilt upside while keeping guardrails"
    },
    "time_slice_evidence": {
      "time_slice_improvement_vs_base": 0.0,
      "time_slice_not_weak": false,
      "time_slice_pass_rate": 0.0
    },
    "turnover_cost_evidence": {
      "conservative_cost_passed": true,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.30112,
      "realistic_cost_passed": true,
      "turnover": 2.279705,
      "turnover_change_vs_base_recombination": 0.297277,
      "turnover_reduction_vs_raw_growth_tilt": -0.160407
    },
    "valid_until_stale_signal_evidence": {
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_guarded_transfer_valid_until_strict_v1",
    "daily_report_generated": false,
    "decision_evidence": {
      "candidate_decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "decision_reason": "positive_static_gap_but_targeted_gate_criteria_not_met",
      "observation_preview_candidate": false,
      "owner_review_required": false,
      "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
      "research_only_observation_approved": false
    },
    "drawdown_return_evidence": {
      "drawdown_improvement_vs_base": 0.022169,
      "drawdown_materiality_tier": "NOT_SEVERE",
      "return_per_drawdown_penalty": 1.485467,
      "return_retention_vs_base_recombination": 0.985251,
      "return_retention_vs_raw_growth_tilt": 0.962092
    },
    "event_append_enabled": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "performance_metrics": {
      "annualized_return": 0.205752,
      "downside_capture": 0.688254,
      "max_drawdown": -0.13851,
      "sharpe_or_sortino_if_available": 2.16364,
      "total_return": 0.947882,
      "upside_capture": 0.693339,
      "volatility": 0.135294
    },
    "production_enabled": false,
    "regime_expectation_evidence": {
      "regime_expectation_improvement_vs_base": 0.362364,
      "regime_expectation_not_weak": false,
      "regime_expectation_score": 0.362364
    },
    "relative_metrics": {
      "candidate_vs_base_recombination_gap": -0.00308,
      "candidate_vs_lower_turnover_reference_gap": 0.00292,
      "candidate_vs_raw_growth_tilt_gap": -0.008107,
      "cost_adjusted_dynamic_vs_static_gap": 0.013195,
      "drawdown_gap_vs_static": 0.001558,
      "drawdown_improvement_vs_base": 0.022169,
      "dynamic_vs_ranking_top_gap": -0.008107,
      "dynamic_vs_static_gap": 0.013195,
      "retest_slice_vs_full_sample_gap": 0.0,
      "return_retention_vs_base_recombination": 0.985251,
      "return_retention_vs_raw_growth_tilt": 0.962092,
      "turnover_change_vs_base_recombination": 0.625776,
      "turnover_reduction_vs_raw_growth_tilt": -0.327618
    },
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "source_gap_targets": {
      "drawdown_materiality_gap": {
        "drawdown_gap_vs_static": -0.020611,
        "drawdown_materiality_tier": "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST",
        "retest_required": true,
        "return_per_drawdown_penalty": 1.299684,
        "status": "OWNER_JUDGMENT_REQUIRED",
        "targeted_fix": [
          "reduce_growth_tilt_intensity_under_high_volatility",
          "add_drawdown_sensitive_de_risking",
          "preserve_turnover_budget"
        ]
      },
      "regime_expectation_gap": {
        "affected_regimes": [
          "risk_on",
          "risk_off",
          "high_volatility",
          "trend_confirmed",
          "recovery"
        ],
        "expected_behavior": [
          "retain risk-on upside",
          "avoid risk-off deterioration",
          "control high-volatility drawdown",
          "capture trend-confirmed growth tilt"
        ],
        "improvement_direction": [
          "condition_growth_tilt_on_trend_confirmed",
          "strengthen_high_volatility_risk_cap",
          "avoid_excessive_risk_off_defensiveness"
        ],
        "observed_issue": "regime_expectation_score remains below 2396 preview reference",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.325498,
        "status": "GAP_REMAINS"
      },
      "return_retention_gap": {
        "improvement_direction": [
          "preserve_more_raw_growth_tilt_upside",
          "relax_guarded_transfer_only_under_trend_confirmed"
        ],
        "retest_required": true,
        "return_gap_vs_raw_growth_tilt": 0.023506,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "status": "ADEQUATE_BUT_MONITOR",
        "upside_capture_gap": 0.274106
      },
      "time_slice_evidence_gap": {
        "affected_time_slices": [
          "full_available_window",
          "recent_period",
          "post_2023_ai_cycle",
          "high_volatility_periods",
          "drawdown_recovery_periods"
        ],
        "improvement_direction": [
          "tune_reentry_timing",
          "reduce_drawdown_recovery_lag",
          "preserve_valid_until_window"
        ],
        "likely_failure_reason": "aggregate time_slice_pass_rate remains below 2396 preview reference; targeted retest must isolate weak slices",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.0,
        "status": "GAP_REMAINS"
      },
      "turnover_cost_evidence_gap": {
        "conservative_cost_status": "PASS",
        "cost_drag_reduction": -0.005027,
        "harsh_cost_status": "PASS",
        "improvement_direction": [
          "repair guarded transfer turnover behavior",
          "keep realistic and conservative cost survival visible"
        ],
        "realistic_cost_status": "PASS",
        "retest_required": true,
        "status": "GAP_REMAINS",
        "turnover_reduction_vs_raw_growth_tilt": -0.009088
      },
      "valid_until_stale_signal_gap": {
        "improvement_direction": [
          "strict_signal_expiry",
          "near_expiry_signal_decay",
          "block_stale_signal_carry_forward"
        ],
        "no_stale_signal_carry_forward": true,
        "retest_required": true,
        "signal_to_execution_lag_days": 1.0,
        "stale_signal_execution_count": 0.0,
        "status": "PASS",
        "valid_until_fix_required": false,
        "valid_until_window_preserved": true
      }
    },
    "source_variant_definition": {
      "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "candidate_id": "growth_tilt_guarded_transfer_valid_until_strict_v1",
      "changes": [
        "strict_signal_expiry",
        "near_expiry_signal_decay",
        "block_stale_signal_carry_forward"
      ],
      "purpose": "strengthen signal validity evidence"
    },
    "time_slice_evidence": {
      "time_slice_improvement_vs_base": 0.428571,
      "time_slice_not_weak": false,
      "time_slice_pass_rate": 0.428571
    },
    "turnover_cost_evidence": {
      "conservative_cost_passed": true,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.300906,
      "realistic_cost_passed": true,
      "turnover": 2.608204,
      "turnover_change_vs_base_recombination": 0.625776,
      "turnover_reduction_vs_raw_growth_tilt": -0.327618
    },
    "valid_until_stale_signal_evidence": {
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  },
  {
    "broker_action_enabled": false,
    "candidate_auto_accept_approved": false,
    "candidate_id": "growth_tilt_guarded_transfer_balanced_gate_v1",
    "daily_report_generated": false,
    "decision_evidence": {
      "candidate_decision": "CONTINUE_TARGETED_IMPROVEMENT",
      "decision_reason": "positive_static_gap_but_targeted_gate_criteria_not_met",
      "observation_preview_candidate": false,
      "owner_review_required": false,
      "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
      "research_only_observation_approved": false
    },
    "drawdown_return_evidence": {
      "drawdown_improvement_vs_base": 0.022127,
      "drawdown_materiality_tier": "NOT_SEVERE",
      "return_per_drawdown_penalty": 1.479813,
      "return_retention_vs_base_recombination": 0.981799,
      "return_retention_vs_raw_growth_tilt": 0.95872
    },
    "event_append_enabled": false,
    "outcome_binding_enabled": false,
    "paper_shadow_enabled": false,
    "performance_metrics": {
      "annualized_return": 0.205031,
      "downside_capture": 0.709142,
      "max_drawdown": -0.138552,
      "sharpe_or_sortino_if_available": 2.083339,
      "total_return": 0.94373,
      "upside_capture": 0.709748,
      "volatility": 0.139062
    },
    "production_enabled": false,
    "regime_expectation_evidence": {
      "regime_expectation_improvement_vs_base": 0.36124,
      "regime_expectation_not_weak": false,
      "regime_expectation_score": 0.36124
    },
    "relative_metrics": {
      "candidate_vs_base_recombination_gap": -0.003801,
      "candidate_vs_lower_turnover_reference_gap": 0.002199,
      "candidate_vs_raw_growth_tilt_gap": -0.008828,
      "cost_adjusted_dynamic_vs_static_gap": 0.012474,
      "drawdown_gap_vs_static": 0.001516,
      "drawdown_improvement_vs_base": 0.022127,
      "dynamic_vs_ranking_top_gap": -0.008828,
      "dynamic_vs_static_gap": 0.012474,
      "retest_slice_vs_full_sample_gap": 0.0,
      "return_retention_vs_base_recombination": 0.981799,
      "return_retention_vs_raw_growth_tilt": 0.95872,
      "turnover_change_vs_base_recombination": 0.663275,
      "turnover_reduction_vs_raw_growth_tilt": -0.346706
    },
    "research_only_observation_approved": false,
    "scheduler_enabled": false,
    "source_gap_targets": {
      "drawdown_materiality_gap": {
        "drawdown_gap_vs_static": -0.020611,
        "drawdown_materiality_tier": "OWNER_REVIEW_REQUIRED_FROM_2396_RECOMBINATION_RETEST",
        "retest_required": true,
        "return_per_drawdown_penalty": 1.299684,
        "status": "OWNER_JUDGMENT_REQUIRED",
        "targeted_fix": [
          "reduce_growth_tilt_intensity_under_high_volatility",
          "add_drawdown_sensitive_de_risking",
          "preserve_turnover_budget"
        ]
      },
      "regime_expectation_gap": {
        "affected_regimes": [
          "risk_on",
          "risk_off",
          "high_volatility",
          "trend_confirmed",
          "recovery"
        ],
        "expected_behavior": [
          "retain risk-on upside",
          "avoid risk-off deterioration",
          "control high-volatility drawdown",
          "capture trend-confirmed growth tilt"
        ],
        "improvement_direction": [
          "condition_growth_tilt_on_trend_confirmed",
          "strengthen_high_volatility_risk_cap",
          "avoid_excessive_risk_off_defensiveness"
        ],
        "observed_issue": "regime_expectation_score remains below 2396 preview reference",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.325498,
        "status": "GAP_REMAINS"
      },
      "return_retention_gap": {
        "improvement_direction": [
          "preserve_more_raw_growth_tilt_upside",
          "relax_guarded_transfer_only_under_trend_confirmed"
        ],
        "retest_required": true,
        "return_gap_vs_raw_growth_tilt": 0.023506,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "status": "ADEQUATE_BUT_MONITOR",
        "upside_capture_gap": 0.274106
      },
      "time_slice_evidence_gap": {
        "affected_time_slices": [
          "full_available_window",
          "recent_period",
          "post_2023_ai_cycle",
          "high_volatility_periods",
          "drawdown_recovery_periods"
        ],
        "improvement_direction": [
          "tune_reentry_timing",
          "reduce_drawdown_recovery_lag",
          "preserve_valid_until_window"
        ],
        "likely_failure_reason": "aggregate time_slice_pass_rate remains below 2396 preview reference; targeted retest must isolate weak slices",
        "reference_from_2396": 0.5,
        "retest_required": true,
        "source_value": 0.0,
        "status": "GAP_REMAINS"
      },
      "turnover_cost_evidence_gap": {
        "conservative_cost_status": "PASS",
        "cost_drag_reduction": -0.005027,
        "harsh_cost_status": "PASS",
        "improvement_direction": [
          "repair guarded transfer turnover behavior",
          "keep realistic and conservative cost survival visible"
        ],
        "realistic_cost_status": "PASS",
        "retest_required": true,
        "status": "GAP_REMAINS",
        "turnover_reduction_vs_raw_growth_tilt": -0.009088
      },
      "valid_until_stale_signal_gap": {
        "improvement_direction": [
          "strict_signal_expiry",
          "near_expiry_signal_decay",
          "block_stale_signal_carry_forward"
        ],
        "no_stale_signal_carry_forward": true,
        "retest_required": true,
        "signal_to_execution_lag_days": 1.0,
        "stale_signal_execution_count": 0.0,
        "status": "PASS",
        "valid_until_fix_required": false,
        "valid_until_window_preserved": true
      }
    },
    "source_variant_definition": {
      "base": "growth_tilt_lower_turnover_guarded_transfer_v1",
      "candidate_id": "growth_tilt_guarded_transfer_balanced_gate_v1",
      "changes": [
        "moderate_growth_tilt",
        "lower_turnover_guardrail",
        "strict_valid_until",
        "high_volatility_risk_cap",
        "cooldown_balancing"
      ],
      "purpose": "balanced candidate targeting observation preview gates"
    },
    "time_slice_evidence": {
      "time_slice_improvement_vs_base": 0.285714,
      "time_slice_not_weak": false,
      "time_slice_pass_rate": 0.285714
    },
    "turnover_cost_evidence": {
      "conservative_cost_passed": true,
      "harsh_cost_passed": true,
      "max_monthly_turnover": 0.420202,
      "realistic_cost_passed": true,
      "turnover": 2.645703,
      "turnover_change_vs_base_recombination": 0.663275,
      "turnover_reduction_vs_raw_growth_tilt": -0.346706
    },
    "valid_until_stale_signal_evidence": {
      "no_stale_signal_carry_forward": true,
      "signal_to_execution_lag_days": 1.0,
      "stale_signal_execution_count": 0,
      "valid_until_window_preserved": true
    }
  }
]
```

## Time-slice evidence result

- rows：`42`

## Regime expectation result

- rows：`48`

## Drawdown / return retention result

```json
{
  "candidate_id": "growth_tilt_guarded_transfer_valid_until_strict_v1",
  "drawdown_improvement_vs_base": 0.022169,
  "drawdown_materiality_tier": "NOT_SEVERE",
  "return_retention_vs_base_recombination": 0.985251,
  "return_retention_vs_raw_growth_tilt": 0.962092
}
```

## Valid-until / stale signal result

```json
{
  "no_stale_signal_candidate_count": 6,
  "targeted_variant_count": 6,
  "valid_until_window_preserved_all": true
}
```

## Decision update

```json
{
  "best_targeted_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
  "best_targeted_variant_decision": "CONTINUE_TARGETED_IMPROVEMENT",
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "candidate_decisions": {
    "growth_tilt_guarded_transfer_balanced_gate_v1": "CONTINUE_TARGETED_IMPROVEMENT",
    "growth_tilt_guarded_transfer_drawdown_calibrated_v1": "CONTINUE_TARGETED_IMPROVEMENT",
    "growth_tilt_guarded_transfer_regime_repair_v1": "CONTINUE_TARGETED_IMPROVEMENT",
    "growth_tilt_guarded_transfer_return_retention_v1": "CONTINUE_TARGETED_IMPROVEMENT",
    "growth_tilt_guarded_transfer_time_slice_repair_v1": "CONTINUE_TARGETED_IMPROVEMENT",
    "growth_tilt_guarded_transfer_valid_until_strict_v1": "CONTINUE_TARGETED_IMPROVEMENT"
  },
  "daily_report_generated": false,
  "decision_update_ready": true,
  "event_append_approved": false,
  "event_append_enabled": false,
  "gate_evidence_matrix_count": 6,
  "next_route_reason": "TRADING-2399 ranks targeted gate-evidence variants, but observation preview, no-approval, paper-shadow and execution decisions remain reserved for TRADING-2400 owner review.",
  "observation_preview_candidates": [],
  "observation_preview_candidates_count": 0,
  "outcome_binding_approved": false,
  "outcome_binding_enabled": false,
  "owner_review_required_candidates": [],
  "owner_review_required_candidates_count": 0,
  "paper_shadow_approved": false,
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recommended_next_research_task": "TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision",
  "research_only_observation_approved": false,
  "research_only_observation_preview_exists": false,
  "scheduler_enabled": false,
  "schema_version": "dynamic_strategy_targeted_gate_evidence_decision_update.v1"
}
```

## Explicit non-approval list

- 不批准 observation
- 不进入 paper-shadow
- 不启用 scheduler
- 不 append event
- 不 bind outcome
- 不生成 daily report
- 不启用 production / broker

## Recommended next route

`TRADING-2400_Dynamic_Strategy_Targeted_Gate_Evidence_Owner_Review_And_Observation_Decision`
