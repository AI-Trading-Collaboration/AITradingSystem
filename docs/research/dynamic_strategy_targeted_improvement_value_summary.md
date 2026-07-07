# Dynamic strategy targeted improvement value summary

- status：`DYNAMIC_STRATEGY_TARGETED_GATE_EVIDENCE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- best targeted variant：`growth_tilt_guarded_transfer_valid_until_strict_v1`
- research value：`True`
- observation ready：`False`
- recommended handling：`retain_as_component_or_variant_reference_for_future_review`

## Value types

- `valid_until_strictness`
- `signal_expiry_discipline`
- `stale_signal_guardrail`
- `guardrail_quality`

## Metrics and evidence

{
  "base_candidate": "growth_tilt_lower_turnover_guarded_transfer_v1",
  "best_variant": "growth_tilt_guarded_transfer_valid_until_strict_v1",
  "decision_from_2399": "CONTINUE_TARGETED_IMPROVEMENT",
  "gate_evidence": {
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
  "interpretation": "valid-until strictness appears to be the strongest targeted direction, but 2399 did not produce an observation preview candidate.",
  "observation_ready": false,
  "ranking_metrics": {
    "annualized_return": 0.205752,
    "max_drawdown": -0.13851,
    "no_stale_signal_carry_forward": true,
    "rank": 1,
    "regime_expectation_score": 0.362364,
    "return_retention_vs_raw_growth_tilt": 0.962092,
    "stale_signal_execution_count": 0,
    "time_slice_pass_rate": 0.428571,
    "turnover_reduction_vs_raw_growth_tilt": -0.327618
  },
  "recommended_handling": "retain_as_component_or_variant_reference_for_future_review",
  "record_ready": true,
  "research_value": true,
  "value_type": [
    "valid_until_strictness",
    "signal_expiry_discipline",
    "stale_signal_guardrail",
    "guardrail_quality"
  ]
}
