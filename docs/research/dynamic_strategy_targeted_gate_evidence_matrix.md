# 动态策略 targeted gate evidence matrix

- status：`DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_TARGETED_GATE_EVIDENCE_RETEST_READY`

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
