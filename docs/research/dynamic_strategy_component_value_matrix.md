# 动态策略组件价值矩阵

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`
- observation approved：`False`

|组件|类别|来源候选|候选级状态|2393 动作|
|---|---|---|---|---|
|`turnover_budgeting`|`EXECUTION_GUARDRAIL`|`dynamic_turnover_budgeted_growth_tilt_v1`|`COMPONENT_VALUE_ONLY`|`TARGETED_ABLATION_RETEST`|
|`valid_until_strictness`|`EXECUTION_GUARDRAIL`|`dynamic_valid_until_expiry_strict_v1`|`COMPONENT_VALUE_ONLY`|`TARGETED_ABLATION_RETEST`|
|`growth_tilt_engine`|`RETURN_ENGINE`|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`OWNER_REVIEW_REQUIRED_PREVIEW`|`TARGETED_ABLATION_RETEST`|
|`lower_turnover_guardrail`|`EXECUTION_GUARDRAIL`|`dynamic_regime_overlay_v0_4_lower_turnover`, `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|`GUARDRAIL_REFERENCE_ONLY`|`TARGETED_ABLATION_RETEST`|
|`guarded_turnover_transfer`|`RISK_GUARDRAIL`|`equal_risk_growth_tilt_guarded_turnover_v1`|`GUARDRAIL_TRANSFER_REFERENCE_ONLY`|`TARGETED_ABLATION_RETEST`|

```json
[
  {
    "can_independently_add_return": false,
    "candidate_level_approval": false,
    "candidate_level_status": "COMPONENT_VALUE_ONLY",
    "component_class": "EXECUTION_GUARDRAIL",
    "component_name": "turnover_budgeting",
    "component_value_only": true,
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
    "mainly_reduces_risk_or_cost": true,
    "possible_component_value": [
      "explicit turnover budget",
      "cost-aware growth tilt",
      "turnover discipline"
    ],
    "problem_solved": "reduce turnover and cost drag without deleting growth tilt",
    "recommended_2393_action": "TARGETED_ABLATION_RETEST",
    "reusable_in_future_candidate": true,
    "reuse_mode": "GUARDRAIL_ONLY",
    "secondary_classes": [
      "COMPONENT_VALUE_ONLY"
    ],
    "source_candidate_failure_context": [
      "realistic_gap=0.006941",
      "conservative_gap=0.00598",
      "harsh_gap=0.00502",
      "time_slice_pass_rate=0.428571",
      "time_slice_pass_rate=0.428571",
      "regime_slice_pass_rate=0.0",
      "drawdown_not_materially_worse=True",
      "return_advantage_retained=0.325838"
    ],
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
    ],
    "target_candidate_or_signal_family": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "targeted_ablation_retest_recommended": true,
    "time_regime_slice_impact_to_measure": "REQUIRES_TARGETED_ABLATION_EVIDENCE",
    "turnover_impact_to_measure": "SHOULD_REDUCE_OR_BUDGET_TURNOVER",
    "valid_until_window_conflict": "NO_KNOWN_CONFLICT_PLAN_TO_VERIFY"
  },
  {
    "can_independently_add_return": false,
    "candidate_level_approval": false,
    "candidate_level_status": "COMPONENT_VALUE_ONLY",
    "component_class": "EXECUTION_GUARDRAIL",
    "component_name": "valid_until_strictness",
    "component_value_only": true,
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
    "mainly_reduces_risk_or_cost": true,
    "possible_component_value": [
      "strict signal expiry",
      "stale signal prevention",
      "near-expiry risk control"
    ],
    "problem_solved": "prevent stale signal carry-forward and near-expiry execution",
    "recommended_2393_action": "TARGETED_ABLATION_RETEST",
    "reusable_in_future_candidate": true,
    "reuse_mode": "GUARDRAIL_ONLY",
    "secondary_classes": [
      "SIGNAL_FILTER",
      "COMPONENT_VALUE_ONLY"
    ],
    "source_candidate_failure_context": [
      "realistic_gap=0.007195",
      "conservative_gap=0.006125",
      "harsh_gap=0.005056",
      "time_slice_pass_rate=0.428571",
      "time_slice_pass_rate=0.428571",
      "regime_slice_pass_rate=0.0",
      "drawdown_not_materially_worse=True",
      "return_advantage_retained=0.337762"
    ],
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
    ],
    "target_candidate_or_signal_family": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "targeted_ablation_retest_recommended": true,
    "time_regime_slice_impact_to_measure": "REQUIRES_TARGETED_ABLATION_EVIDENCE",
    "turnover_impact_to_measure": "MAY_REDUCE_STALE_ACTIONS_BUT_CAN_LOWER_UPSIDE",
    "valid_until_window_conflict": "NO_KNOWN_CONFLICT_PLAN_TO_VERIFY"
  },
  {
    "can_independently_add_return": true,
    "candidate_level_approval": false,
    "candidate_level_status": "OWNER_REVIEW_REQUIRED_PREVIEW",
    "component_class": "RETURN_ENGINE",
    "component_name": "growth_tilt_engine",
    "component_value_only": false,
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
    "mainly_reduces_risk_or_cost": false,
    "possible_component_value": [
      "return advantage",
      "upside capture",
      "risk-on responsiveness"
    ],
    "problem_solved": "retain the return engine that ranks above static baseline",
    "recommended_2393_action": "TARGETED_ABLATION_RETEST",
    "reusable_in_future_candidate": true,
    "reuse_mode": "RETURN_ENGINE_WITH_GUARDRAILS",
    "secondary_classes": [],
    "source_candidate_failure_context": [
      "realistic_gap=0.021302",
      "conservative_gap=0.020633",
      "harsh_gap=0.019964",
      "time_slice_pass_rate=0.0",
      "time_slice_pass_rate=0.0",
      "regime_slice_pass_rate=0.0",
      "drawdown_not_materially_worse=False",
      "return_advantage_retained=1.0"
    ],
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
    ],
    "target_candidate_or_signal_family": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "targeted_ablation_retest_recommended": true,
    "time_regime_slice_impact_to_measure": "REQUIRES_TARGETED_ABLATION_EVIDENCE",
    "turnover_impact_to_measure": "CAN_RELY_ON_HIGHER_TURNOVER",
    "valid_until_window_conflict": "NO_KNOWN_CONFLICT_PLAN_TO_VERIFY"
  },
  {
    "can_independently_add_return": false,
    "candidate_level_approval": false,
    "candidate_level_status": "GUARDRAIL_REFERENCE_ONLY",
    "component_class": "EXECUTION_GUARDRAIL",
    "component_name": "lower_turnover_guardrail",
    "component_value_only": false,
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
    "mainly_reduces_risk_or_cost": true,
    "possible_component_value": [
      "turnover reduction",
      "cost stress resilience",
      "execution discipline"
    ],
    "problem_solved": "lower execution churn and cost stress exposure",
    "recommended_2393_action": "TARGETED_ABLATION_RETEST",
    "reusable_in_future_candidate": true,
    "reuse_mode": "GUARDRAIL_ONLY",
    "secondary_classes": [
      "RISK_GUARDRAIL"
    ],
    "source_candidate_failure_context": [
      "realistic_gap=0.002205",
      "conservative_gap=0.001524",
      "harsh_gap=0.000843",
      "time_slice_pass_rate=0.428571",
      "realistic_gap=0.010275",
      "conservative_gap=0.00942",
      "harsh_gap=0.008565",
      "time_slice_pass_rate=0.0"
    ],
    "source_candidates": [
      "dynamic_regime_overlay_v0_4_lower_turnover",
      "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
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
    ],
    "target_candidate_or_signal_family": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "targeted_ablation_retest_recommended": true,
    "time_regime_slice_impact_to_measure": "REQUIRES_TARGETED_ABLATION_EVIDENCE",
    "turnover_impact_to_measure": "SHOULD_REDUCE_TURNOVER",
    "valid_until_window_conflict": "NO_KNOWN_CONFLICT_PLAN_TO_VERIFY"
  },
  {
    "can_independently_add_return": false,
    "candidate_level_approval": false,
    "candidate_level_status": "GUARDRAIL_TRANSFER_REFERENCE_ONLY",
    "component_class": "RISK_GUARDRAIL",
    "component_name": "guarded_turnover_transfer",
    "component_value_only": false,
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
    "mainly_reduces_risk_or_cost": true,
    "possible_component_value": [
      "partial transfer of lower-turnover guardrail to ranking top",
      "reduced fragility relative to original ranking top"
    ],
    "problem_solved": "reduce ranking-top fragility while retaining return engine",
    "recommended_2393_action": "TARGETED_ABLATION_RETEST",
    "reusable_in_future_candidate": true,
    "reuse_mode": "GUARDRAIL_TRANSFER",
    "secondary_classes": [
      "EXECUTION_GUARDRAIL"
    ],
    "source_candidate_failure_context": [
      "realistic_gap=0.02062",
      "conservative_gap=0.019974",
      "harsh_gap=0.019328",
      "time_slice_pass_rate=0.0",
      "time_slice_pass_rate=0.0",
      "regime_slice_pass_rate=0.0",
      "drawdown_not_materially_worse=False",
      "return_advantage_retained=0.967984"
    ],
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
    ],
    "target_candidate_or_signal_family": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "targeted_ablation_retest_recommended": true,
    "time_regime_slice_impact_to_measure": "REQUIRES_TARGETED_ABLATION_EVIDENCE",
    "turnover_impact_to_measure": "SHOULD_LOWER_FRAGILITY_AND_CONTROL_TURNOVER",
    "valid_until_window_conflict": "NO_KNOWN_CONFLICT_PLAN_TO_VERIFY"
  }
]
```