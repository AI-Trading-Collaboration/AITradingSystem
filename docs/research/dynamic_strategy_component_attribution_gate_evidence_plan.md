# 动态策略组件归因与门禁证据计划

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`
- as_of：`2026-07-07`
- 2391 owner decision：`DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION`
- 下一路由：`TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest`

## 执行摘要

TRADING-2392 将组件级证据与候选级批准分离：本任务只定义组件归因、门禁证据和后续 targeted ablation retest 计划，不运行 ablation retest，也不批准 observation、paper-shadow、production 或 broker 动作。

## TRADING-2390 / TRADING-2391 来源结论

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
    "status": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY"
  },
  "trading_2390": {
    "component_value_candidates": [
      "dynamic_turnover_budgeted_growth_tilt_v1",
      "dynamic_valid_until_expiry_strict_v1"
    ],
    "current_best_candidate_preview_decision": "OWNER_REVIEW_REQUIRED",
    "status": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY"
  },
  "trading_2391": {
    "component_attribution_continue_recommended": true,
    "owner_decision": "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION",
    "research_only_observation_approved": false,
    "status": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
  }
}
```

## 组件归因范围

|组件|类别|来源候选|候选级状态|2393 动作|
|---|---|---|---|---|
|`turnover_budgeting`|`EXECUTION_GUARDRAIL`|`dynamic_turnover_budgeted_growth_tilt_v1`|`COMPONENT_VALUE_ONLY`|`TARGETED_ABLATION_RETEST`|
|`valid_until_strictness`|`EXECUTION_GUARDRAIL`|`dynamic_valid_until_expiry_strict_v1`|`COMPONENT_VALUE_ONLY`|`TARGETED_ABLATION_RETEST`|
|`growth_tilt_engine`|`RETURN_ENGINE`|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`OWNER_REVIEW_REQUIRED_PREVIEW`|`TARGETED_ABLATION_RETEST`|
|`lower_turnover_guardrail`|`EXECUTION_GUARDRAIL`|`dynamic_regime_overlay_v0_4_lower_turnover`, `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|`GUARDRAIL_REFERENCE_ONLY`|`TARGETED_ABLATION_RETEST`|
|`guarded_turnover_transfer`|`RISK_GUARDRAIL`|`equal_risk_growth_tilt_guarded_turnover_v1`|`GUARDRAIL_TRANSFER_REFERENCE_ONLY`|`TARGETED_ABLATION_RETEST`|

## 组件价值矩阵

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

## 门禁证据计划

```json
{
  "component_gate_evidence": [
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "turnover_budgeting",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "turnover_budget_ablation",
        "cost_drag_reduction_test",
        "return_retention_test",
        "max_monthly_turnover_stress",
        "valid_until_window_preservation_check"
      ],
      "success_criteria": [
        "reduces_turnover_vs_source_candidate",
        "does_not_destroy_cost_adjusted_return",
        "improves_or_preserves_realistic_cost_gap",
        "does_not_increase_stale_signal_execution"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "valid_until_strictness",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "stale_signal_prevention_ablation",
        "near_expiry_signal_decay_test",
        "signal_to_execution_lag_test",
        "return_gap_impact_test",
        "regime_slice_impact_test"
      ],
      "success_criteria": [
        "reduces_stale_signal_execution_count",
        "preserves_positive_dynamic_vs_static_gap",
        "does_not_excessively_reduce_upside_capture",
        "improves_signal_expiry_discipline"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "growth_tilt_engine",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "growth_tilt_ablation",
        "risk_on_upside_capture_test",
        "high_volatility_drawdown_test",
        "trend_confirmed_gate_test",
        "drawdown_compensation_test"
      ],
      "success_criteria": [
        "preserves_return_advantage",
        "improves_upside_capture",
        "does_not_materially_worsen_drawdown_after_guardrails",
        "passes_realistic_and_conservative_cost"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "lower_turnover_guardrail",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "lower_turnover_ablation",
        "cooldown_ablation",
        "max_step_weight_delta_test",
        "turnover_cap_stress",
        "return_gap_tradeoff_test"
      ],
      "success_criteria": [
        "reduces_turnover",
        "improves_cost_resilience",
        "does_not_destroy_return_gap_too_much",
        "preserves_valid_until_window"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "guarded_turnover_transfer",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "guarded_transfer_ablation",
        "ranking_top_return_retention_test",
        "drawdown_fragility_test",
        "turnover_tradeoff_test"
      ],
      "success_criteria": [
        "reduces_fragility_relative_to_ranking_top",
        "keeps_return_gap_close_to_ranking_top",
        "does_not_add_stale_signal_execution",
        "preserves_valid_until_window"
      ]
    }
  ],
  "plan_ready": true
}
```

## targeted ablation retest 计划

```json
{
  "ablation_test_candidates": [
    {
      "add": [],
      "base": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "candidate_id": "growth_tilt_only_reference",
      "purpose": "measure raw growth tilt engine",
      "remove": [
        "guarded_turnover",
        "strict_valid_until",
        "turnover_budget"
      ]
    },
    {
      "add": [
        "turnover_budgeting"
      ],
      "base": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "candidate_id": "growth_tilt_plus_turnover_budget",
      "purpose": "test whether turnover budgeting improves execution without killing return",
      "remove": []
    },
    {
      "add": [
        "valid_until_strictness"
      ],
      "base": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "candidate_id": "growth_tilt_plus_valid_until_strict",
      "purpose": "test whether strict expiry improves stale signal control",
      "remove": []
    },
    {
      "add": [
        "turnover_budgeting",
        "valid_until_strictness"
      ],
      "base": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "candidate_id": "growth_tilt_plus_turnover_budget_and_valid_until",
      "purpose": "test combined component transfer",
      "remove": []
    },
    {
      "add": [],
      "base": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "candidate_id": "lower_turnover_without_cooldown",
      "purpose": "measure cooldown contribution",
      "remove": [
        "cooldown_balancing"
      ]
    },
    {
      "add": [
        "guarded_growth_tilt_engine"
      ],
      "base": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "candidate_id": "lower_turnover_plus_growth_tilt_component",
      "purpose": "test whether lower-turnover reference can gain upside without losing robustness",
      "remove": []
    }
  ],
  "acceptance_criteria": {
    "component_owner_review_required": {
      "condition": [
        "improves_return_or_risk_but_tradeoff_requires_human_judgment"
      ]
    },
    "component_to_reject": {
      "condition": [
        "no_clear_metric_improvement",
        "or_worsens_drawdown_without_return_compensation",
        "or_increases_turnover_without_cost_adjusted_return_gain",
        "or_increases_stale_signal_execution"
      ]
    },
    "reusable_component": {
      "must": [
        "improves_one_target_metric",
        "does_not_materially_worsen_core_guardrail",
        "preserves_valid_until_window",
        "does_not_require_scheduler_or_paper_shadow"
      ]
    }
  },
  "broker_action_enabled": false,
  "must_not_approve_observation": true,
  "must_run_data_quality_gate_if_2393_reads_cached_market_data": true,
  "paper_shadow_enabled": false,
  "plan_ready": true,
  "plan_type": "TARGETED_ABLATION_RETEST_PLAN_FOR_2393",
  "production_enabled": false,
  "scheduler_enabled": false,
  "target_task": "TRADING-2393_Dynamic_Strategy_Component_Attribution_Targeted_Ablation_Retest"
}
```

## 组件验收标准

```json
{
  "component_owner_review_required": {
    "condition": [
      "improves_return_or_risk_but_tradeoff_requires_human_judgment"
    ]
  },
  "component_to_reject": {
    "condition": [
      "no_clear_metric_improvement",
      "or_worsens_drawdown_without_return_compensation",
      "or_increases_turnover_without_cost_adjusted_return_gain",
      "or_increases_stale_signal_execution"
    ]
  },
  "reusable_component": {
    "must": [
      "improves_one_target_metric",
      "does_not_materially_worsen_core_guardrail",
      "preserves_valid_until_window",
      "does_not_require_scheduler_or_paper_shadow"
    ]
  }
}
```

## 明确未批准事项

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

## 安全边界摘要

```json
{
  "ablation_retest_executed": false,
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
  "task_boundary": "COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_ONLY"
}
```