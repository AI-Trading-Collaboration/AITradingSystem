# Dynamic strategy calibrated gate component attribution review

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY`
- component value candidates：`['dynamic_turnover_budgeted_growth_tilt_v1', 'dynamic_valid_until_expiry_strict_v1']`

|Component|Source candidates|Reusable|Recommended follow-up|
|---|---|---|---|
|`turnover_budgeting`|`dynamic_turnover_budgeted_growth_tilt_v1`|`True`|reuse turnover budget discipline inside a higher-return candidate without weakening regime-slice evidence|
|`valid_until_strictness`|`dynamic_valid_until_expiry_strict_v1`|`True`|test valid-until strictness as a component overlay rather than a standalone candidate|
|`growth_tilt_engine`|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`True`|keep growth tilt as the owner-review reference while repairing drawdown and slice instability|
|`lower_turnover_guardrail`|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`, `dynamic_regime_overlay_v0_4_lower_turnover`|`True`|reuse lower-turnover guardrails as constraints, not as a direct replacement for the ranking-top return engine|
|`guarded_turnover_transfer`|`equal_risk_growth_tilt_guarded_turnover_v1`|`True`|compare guarded transfer against the original ranking top in owner-review materials without treating it as approved observation|
|`risk_cap_interaction`|`dynamic_risk_cap_adaptive_v1`|`True`|retain only as component-level research until return/ranking gaps improve|
|`regime_transition_reentry`|`dynamic_regime_reentry_accelerated_v1`, `dynamic_regime_recovery_confirmation_v1`|`True`|use as diagnostic input for component-level targeted improvement|

```json
[
  {
    "component_name": "turnover_budgeting",
    "component_value_hypothesis": [
      "explicit turnover budget",
      "cost-aware growth tilt",
      "better turnover discipline"
    ],
    "failure_metrics": [
      {
        "candidate_id": "dynamic_turnover_budgeted_growth_tilt_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.013679,
        "candidate_vs_ranking_top_gap": -0.014361,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": -0.000389,
        "drawdown_not_materially_worse": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.325838,
        "time_slice_pass_rate": 0.428571
      }
    ],
    "recommended_followup": "reuse turnover budget discipline inside a higher-return candidate without weakening regime-slice evidence",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "dynamic_turnover_budgeted_growth_tilt_v1"
    ],
    "supporting_metrics": [
      {
        "candidate_id": "dynamic_turnover_budgeted_growth_tilt_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.013679,
        "candidate_vs_lower_turnover_gap": 0.004736,
        "candidate_vs_ranking_top_gap": -0.014361,
        "conservative_cost_passed": true,
        "cost_adjusted_dynamic_vs_static_gap": 0.006941,
        "dynamic_vs_static_gap": 0.006941,
        "harsh_cost_passed": true,
        "max_monthly_turnover": 0.640534,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.325838,
        "time_slice_pass_rate": 0.428571,
        "turnover": 2.866904,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      }
    ]
  },
  {
    "component_name": "valid_until_strictness",
    "component_value_hypothesis": [
      "stale signal prevention",
      "stricter signal expiry",
      "reduced carry-forward risk"
    ],
    "failure_metrics": [
      {
        "candidate_id": "dynamic_valid_until_expiry_strict_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.013425,
        "candidate_vs_ranking_top_gap": -0.014107,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": -0.005479,
        "drawdown_not_materially_worse": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.337762,
        "time_slice_pass_rate": 0.428571
      }
    ],
    "recommended_followup": "test valid-until strictness as a component overlay rather than a standalone candidate",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "dynamic_valid_until_expiry_strict_v1"
    ],
    "supporting_metrics": [
      {
        "candidate_id": "dynamic_valid_until_expiry_strict_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.013425,
        "candidate_vs_lower_turnover_gap": 0.00499,
        "candidate_vs_ranking_top_gap": -0.014107,
        "conservative_cost_passed": true,
        "cost_adjusted_dynamic_vs_static_gap": 0.007195,
        "dynamic_vs_static_gap": 0.007195,
        "harsh_cost_passed": true,
        "max_monthly_turnover": 0.464838,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.337762,
        "time_slice_pass_rate": 0.428571,
        "turnover": 3.175612,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      }
    ]
  },
  {
    "component_name": "growth_tilt_engine",
    "component_value_hypothesis": [
      "return advantage",
      "upside capture",
      "risk-on responsiveness"
    ],
    "failure_metrics": [
      {
        "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
        "candidate_vs_guarded_ranking_top_gap": 0.000682,
        "candidate_vs_ranking_top_gap": 0.0,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": 0.043574,
        "drawdown_not_materially_worse": false,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 1.0,
        "time_slice_pass_rate": 0.0
      }
    ],
    "recommended_followup": "keep growth tilt as the owner-review reference while repairing drawdown and slice instability",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
    ],
    "supporting_metrics": [
      {
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
    ]
  },
  {
    "component_name": "lower_turnover_guardrail",
    "component_value_hypothesis": [
      "turnover reduction",
      "cost stress resilience",
      "execution discipline"
    ],
    "failure_metrics": [
      {
        "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.010345,
        "candidate_vs_ranking_top_gap": -0.011027,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": 0.002825,
        "drawdown_not_materially_worse": false,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.482349,
        "time_slice_pass_rate": 0.0
      },
      {
        "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
        "candidate_vs_guarded_ranking_top_gap": -0.018415,
        "candidate_vs_ranking_top_gap": -0.019097,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": -0.017202,
        "drawdown_not_materially_worse": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.103511,
        "time_slice_pass_rate": 0.428571
      }
    ],
    "recommended_followup": "reuse lower-turnover guardrails as constraints, not as a direct replacement for the ranking-top return engine",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "dynamic_regime_overlay_v0_4_lower_turnover"
    ],
    "supporting_metrics": [
      {
        "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.010345,
        "candidate_vs_lower_turnover_gap": 0.00807,
        "candidate_vs_ranking_top_gap": -0.011027,
        "conservative_cost_passed": true,
        "cost_adjusted_dynamic_vs_static_gap": 0.010275,
        "dynamic_vs_static_gap": 0.010275,
        "harsh_cost_passed": true,
        "max_monthly_turnover": 0.348909,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.482349,
        "time_slice_pass_rate": 0.0,
        "turnover": 2.537332,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      },
      {
        "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
        "candidate_vs_guarded_ranking_top_gap": -0.018415,
        "candidate_vs_lower_turnover_gap": 0.0,
        "candidate_vs_ranking_top_gap": -0.019097,
        "conservative_cost_passed": true,
        "cost_adjusted_dynamic_vs_static_gap": 0.002205,
        "dynamic_vs_static_gap": 0.002205,
        "harsh_cost_passed": true,
        "max_monthly_turnover": 0.6,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.103511,
        "time_slice_pass_rate": 0.428571,
        "turnover": 2.04,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      }
    ]
  },
  {
    "component_name": "guarded_turnover_transfer",
    "component_value_hypothesis": [
      "partial transfer of lower-turnover guardrail to ranking top",
      "reduced fragility relative to original ranking top"
    ],
    "failure_metrics": [
      {
        "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
        "candidate_vs_guarded_ranking_top_gap": 0.0,
        "candidate_vs_ranking_top_gap": -0.000682,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": 0.036251,
        "drawdown_not_materially_worse": false,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.967984,
        "time_slice_pass_rate": 0.0
      }
    ],
    "recommended_followup": "compare guarded transfer against the original ranking top in owner-review materials without treating it as approved observation",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "equal_risk_growth_tilt_guarded_turnover_v1"
    ],
    "supporting_metrics": [
      {
        "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
        "candidate_vs_guarded_ranking_top_gap": 0.0,
        "candidate_vs_lower_turnover_gap": 0.018415,
        "candidate_vs_ranking_top_gap": -0.000682,
        "conservative_cost_passed": true,
        "cost_adjusted_dynamic_vs_static_gap": 0.02062,
        "dynamic_vs_static_gap": 0.02062,
        "harsh_cost_passed": true,
        "max_monthly_turnover": 0.277073,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.967984,
        "time_slice_pass_rate": 0.0,
        "turnover": 1.897603,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      }
    ]
  },
  {
    "component_name": "risk_cap_interaction",
    "component_value_hypothesis": [
      "risk-cap interaction can reduce downside exposure",
      "risk-cap logic may be reusable as a veto-like component"
    ],
    "failure_metrics": [
      {
        "candidate_id": "dynamic_risk_cap_adaptive_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.015118,
        "candidate_vs_ranking_top_gap": -0.0158,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": -0.007627,
        "drawdown_not_materially_worse": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.258286,
        "time_slice_pass_rate": 0.285714
      }
    ],
    "recommended_followup": "retain only as component-level research until return/ranking gaps improve",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "dynamic_risk_cap_adaptive_v1"
    ],
    "supporting_metrics": [
      {
        "candidate_id": "dynamic_risk_cap_adaptive_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.015118,
        "candidate_vs_lower_turnover_gap": 0.003297,
        "candidate_vs_ranking_top_gap": -0.0158,
        "conservative_cost_passed": true,
        "cost_adjusted_dynamic_vs_static_gap": 0.005502,
        "dynamic_vs_static_gap": 0.005502,
        "harsh_cost_passed": true,
        "max_monthly_turnover": 0.593284,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.258286,
        "time_slice_pass_rate": 0.285714,
        "turnover": 3.550438,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      }
    ]
  },
  {
    "component_name": "regime_transition_reentry",
    "component_value_hypothesis": [
      "regime transition handling may reduce late re-risk behavior",
      "reentry confirmation may improve recovery participation"
    ],
    "failure_metrics": [
      {
        "candidate_id": "dynamic_regime_reentry_accelerated_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.01953,
        "candidate_vs_ranking_top_gap": -0.020212,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": 0.006471,
        "drawdown_not_materially_worse": false,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.051169,
        "time_slice_pass_rate": 0.0
      },
      {
        "candidate_id": "dynamic_regime_recovery_confirmation_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.022875,
        "candidate_vs_ranking_top_gap": -0.023557,
        "current_gate_blockers": [],
        "drawdown_gap_vs_static": -0.004803,
        "drawdown_not_materially_worse": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.0,
        "time_slice_pass_rate": 0.142857
      }
    ],
    "recommended_followup": "use as diagnostic input for component-level targeted improvement",
    "reusable_in_future_candidate": true,
    "source_candidates": [
      "dynamic_regime_reentry_accelerated_v1",
      "dynamic_regime_recovery_confirmation_v1"
    ],
    "supporting_metrics": [
      {
        "candidate_id": "dynamic_regime_reentry_accelerated_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.01953,
        "candidate_vs_lower_turnover_gap": -0.001115,
        "candidate_vs_ranking_top_gap": -0.020212,
        "conservative_cost_passed": false,
        "cost_adjusted_dynamic_vs_static_gap": 0.00109,
        "dynamic_vs_static_gap": 0.00109,
        "harsh_cost_passed": false,
        "max_monthly_turnover": 0.677841,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": true,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.051169,
        "time_slice_pass_rate": 0.0,
        "turnover": 3.647931,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      },
      {
        "candidate_id": "dynamic_regime_recovery_confirmation_v1",
        "candidate_vs_guarded_ranking_top_gap": -0.022875,
        "candidate_vs_lower_turnover_gap": -0.00446,
        "candidate_vs_ranking_top_gap": -0.023557,
        "conservative_cost_passed": false,
        "cost_adjusted_dynamic_vs_static_gap": -0.002255,
        "dynamic_vs_static_gap": -0.002255,
        "harsh_cost_passed": false,
        "max_monthly_turnover": 0.806271,
        "no_stale_signal_carry_forward": true,
        "realistic_cost_passed": false,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 0.0,
        "time_slice_pass_rate": 0.142857,
        "turnover": 3.971205,
        "turnover_budget_passed": true,
        "valid_until_window_preserved": true
      }
    ]
  }
]
```