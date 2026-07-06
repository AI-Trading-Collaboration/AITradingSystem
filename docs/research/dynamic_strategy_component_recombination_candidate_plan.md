# 动态策略 component recombination candidate plan

- status：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY`
- return engine：`growth_tilt_engine`
- next route：`TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest`

## Executive summary

TRADING-2395 只设计 recombination candidates：以 `growth_tilt_engine` 作为收益引擎，把 `lower_turnover_guardrail` 放在 execution / risk guardrail 层，并保留 `guarded_turnover_transfer` 为 owner-review component。本任务不运行 backtest、不生成 signal、不批准 observation、paper-shadow、scheduler、event append、outcome binding、production 或 broker。

## Source findings from TRADING-2393 / 2394

```json
{
  "trading_2390": {
    "owner_review_recommendation_ready": true,
    "recommended_next_research_task": "TRADING-2391_Dynamic_Strategy_Calibrated_Gate_Candidate_Owner_Review_And_Observation_Decision",
    "status": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY"
  },
  "trading_2391": {
    "component_attribution_continue_recommended": true,
    "owner_decision": "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION",
    "research_only_observation_approved": false,
    "status": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
  },
  "trading_2392": {
    "component_value_candidates": [
      "dynamic_turnover_budgeted_growth_tilt_v1",
      "dynamic_valid_until_expiry_strict_v1"
    ],
    "components_to_attribute": [
      "turnover_budgeting",
      "valid_until_strictness",
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "guarded_turnover_transfer"
    ],
    "status": "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY",
    "targeted_ablation_retest_plan_ready": true
  },
  "trading_2393": {
    "best_reusable_component": "growth_tilt_engine",
    "component_decisions": {
      "combined_turnover_budgeting_and_valid_until": "CONTINUE_COMPONENT_RESEARCH",
      "growth_tilt_engine": "REUSABLE_COMPONENT",
      "guarded_turnover_transfer": "OWNER_REVIEW_REQUIRED",
      "lower_turnover_guardrail": "USE_ONLY_AS_GUARDRAIL",
      "turnover_budgeting": "CONTINUE_COMPONENT_RESEARCH",
      "valid_until_strictness": "CONTINUE_COMPONENT_RESEARCH"
    },
    "data_quality_status": "PASS_WITH_WARNINGS",
    "status": "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY"
  },
  "trading_2394": {
    "owner_decision": "APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL",
    "recombination_plan_approved": true,
    "recommended_next_research_task": "TRADING-2395_Dynamic_Strategy_Component_Recombination_Candidate_Plan",
    "status": "DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_RECOMBINATION_DECISION_READY"
  }
}
```

## Recombination principles

```json
{
  "guardrail_layer": {
    "components": [
      "lower_turnover_guardrail",
      "valid_until_window",
      "no_stale_signal_carry_forward",
      "cooldown_balancing",
      "max_single_step_weight_delta",
      "turnover_budgeting_if_supported",
      "risk_cap_preservation"
    ]
  },
  "monthly_rebalance": {
    "allowed_for_primary_decision": false,
    "allowed_for_reference": true
  },
  "must_not": [
    "optimize_only_for_total_return",
    "remove_risk_guardrails",
    "allow_stale_signal_carry_forward",
    "rely_on_monthly_rebalance",
    "approve_observation_without_recombined_retest"
  ],
  "must_preserve": [
    "valid_until_window",
    "no_stale_signal_carry_forward",
    "cost_stress_testing",
    "turnover_budget",
    "paper_shadow_disabled",
    "scheduler_disabled",
    "broker_disabled"
  ],
  "owner_review_layer": {
    "components": [
      "guarded_turnover_transfer"
    ]
  },
  "primary_execution_cadence": "valid_until_window",
  "primary_return_engine": {
    "component": "growth_tilt_engine",
    "purpose": "preserve upside capture and return advantage"
  }
}
```

## Source component map

```json
{
  "growth_tilt_engine": {
    "adopted_role": "RETURN_ENGINE",
    "source_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "source_tasks": [
      "TRADING-2365",
      "TRADING-2386",
      "TRADING-2393",
      "TRADING-2394"
    ],
    "status": "ADOPTED_AS_REUSABLE_RETURN_ENGINE"
  },
  "guarded_turnover_transfer": {
    "adopted_role": "OWNER_REVIEW_COMPONENT",
    "source_candidate": "equal_risk_growth_tilt_guarded_turnover_v1",
    "source_tasks": [
      "TRADING-2383",
      "TRADING-2393",
      "TRADING-2394"
    ],
    "status": "OWNER_REVIEW_REQUIRED"
  },
  "lower_turnover_guardrail": {
    "adopted_role": "EXECUTION_AND_RISK_GUARDRAIL",
    "source_candidates": [
      "dynamic_regime_overlay_v0_4_lower_turnover",
      "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
    ],
    "source_tasks": [
      "TRADING-2379",
      "TRADING-2393",
      "TRADING-2394"
    ],
    "status": "USE_ONLY_AS_GUARDRAIL"
  },
  "no_stale_signal_carry_forward": {
    "adopted_role": "HARD_RESEARCH_GUARDRAIL",
    "source_tasks": [
      "TRADING-2357",
      "TRADING-2388",
      "TRADING-2392"
    ],
    "status": "REQUIRED"
  },
  "valid_until_window": {
    "adopted_role": "HARD_RESEARCH_EXECUTION_GUARDRAIL",
    "source_tasks": [
      "TRADING-2364",
      "TRADING-2357",
      "TRADING-2388"
    ],
    "status": "REQUIRED"
  }
}
```

## Recombination candidate definitions

```json
[
  {
    "candidate_id": "growth_tilt_lower_turnover_guarded_v1",
    "components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "valid_until_window",
      "no_stale_signal_carry_forward",
      "max_single_step_weight_delta"
    ],
    "expected_tradeoff": [
      "return may decline vs raw ranking top",
      "turnover / drawdown should improve"
    ],
    "hypothesis": [
      "preserve meaningful upside from growth_tilt_engine",
      "reduce turnover and cost drag using lower_turnover_guardrail",
      "avoid stale signal execution"
    ],
    "owner_review_required": false,
    "purpose": "combine primary return engine with lower-turnover execution guardrail"
  },
  {
    "candidate_id": "growth_tilt_turnover_budgeted_v1",
    "components": [
      "growth_tilt_engine",
      "turnover_budgeting",
      "valid_until_window"
    ],
    "expected_tradeoff": [
      "some upside may be lost",
      "turnover budget should improve robustness"
    ],
    "hypothesis": [
      "turnover budget can reduce unnecessary rebalances",
      "cost-adjusted return improves relative to raw growth tilt"
    ],
    "owner_review_required": false,
    "purpose": "test whether explicit turnover budget can preserve return while reducing cost drag"
  },
  {
    "candidate_id": "growth_tilt_valid_until_strict_v1",
    "components": [
      "growth_tilt_engine",
      "valid_until_strictness",
      "no_stale_signal_carry_forward"
    ],
    "expected_tradeoff": [
      "stricter expiry may reduce return",
      "signal discipline should improve"
    ],
    "hypothesis": [
      "stale signal execution decreases",
      "near-expiry overreaction decreases",
      "upside capture remains acceptable"
    ],
    "owner_review_required": false,
    "purpose": "test whether stricter signal expiry improves stale-signal discipline"
  },
  {
    "candidate_id": "growth_tilt_turnover_budgeted_valid_until_strict_v1",
    "components": [
      "growth_tilt_engine",
      "turnover_budgeting",
      "valid_until_strictness",
      "no_stale_signal_carry_forward"
    ],
    "expected_tradeoff": [
      "possible upside reduction",
      "improved observation-gate evidence if tradeoff is acceptable"
    ],
    "hypothesis": [
      "combined execution guardrails improve cost-adjusted robustness",
      "return remains positive vs static",
      "stale signal and turnover both improve"
    ],
    "owner_review_required": false,
    "purpose": "combine the two most relevant execution guardrails with growth tilt"
  },
  {
    "candidate_id": "growth_tilt_lower_turnover_guarded_transfer_v1",
    "components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "guarded_turnover_transfer",
      "valid_until_window"
    ],
    "expected_tradeoff": [
      "owner review required due to transfer uncertainty"
    ],
    "hypothesis": [
      "guarded transfer may preserve more ranking-top upside than lower-turnover guardrail alone",
      "turnover and drawdown should improve vs raw ranking top"
    ],
    "owner_review_required": true,
    "purpose": "test guarded_turnover_transfer as owner-review component"
  },
  {
    "candidate_id": "growth_tilt_conservative_guarded_v1",
    "components": [
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "strict_risk_cap",
      "cooldown_balancing",
      "valid_until_window",
      "no_stale_signal_carry_forward"
    ],
    "expected_tradeoff": [
      "lower upside capture",
      "potentially better gate stability"
    ],
    "hypothesis": [
      "robust under conservative / harsh cost",
      "drawdown improves",
      "return gap may remain"
    ],
    "owner_review_required": false,
    "purpose": "conservative recombination for robustness stress"
  }
]
```

## Forbidden recombination paths

```json
[
  "raw_growth_tilt_without_guardrails",
  "use_monthly_rebalance_as_primary",
  "allow_stale_signal_carry_forward",
  "remove_valid_until_window",
  "remove_risk_cap_without_replacement",
  "remove_turnover_constraints_without_cost_stress",
  "optimize_only_for_total_return",
  "accept_candidate_without_static_baseline_comparison",
  "accept_candidate_without_raw_growth_tilt_reference_comparison",
  "accept_candidate_without_lower_turnover_reference_comparison",
  "accept_candidate_without_cost_stress",
  "approve_research_only_observation_in_plan_task",
  "enable_paper_shadow_or_scheduler"
]
```

## TRADING-2396 retest plan

```json
{
  "component_evidence_metrics": {
    "guardrail_layer": [
      "turnover_reduction_vs_raw_growth_tilt",
      "cost_drag_reduction",
      "drawdown_gap_vs_static",
      "max_monthly_turnover"
    ],
    "recombination_quality": [
      "cost_adjusted_return",
      "return_per_drawdown_penalty",
      "time_slice_pass_rate",
      "regime_expectation_score",
      "owner_review_required"
    ],
    "return_engine": [
      "return_retention_vs_raw_growth_tilt",
      "upside_capture",
      "dynamic_vs_static_gap"
    ],
    "valid_until_layer": [
      "stale_signal_execution_count",
      "signal_to_execution_lag_days",
      "near_expiry_signal_behavior"
    ]
  },
  "cost_stress": {
    "base": {
      "slippage_bps": 2,
      "transaction_cost_bps": 2
    },
    "conservative": {
      "slippage_bps": 10,
      "transaction_cost_bps": 10
    },
    "harsh": {
      "slippage_bps": 10,
      "transaction_cost_bps": 20
    },
    "realistic": {
      "slippage_bps": 5,
      "transaction_cost_bps": 5
    }
  },
  "execution_cadence": {
    "comparison": [
      "valid_until_window",
      "cooldown_limited_event_driven",
      "signal_event_driven"
    ],
    "monthly_rebalance": {
      "allowed_for_primary_decision": false,
      "allowed_for_reference": true
    },
    "primary": "valid_until_window"
  },
  "must_not": [
    "run_without_data_quality_gate_in_2396",
    "use_monthly_rebalance_as_primary",
    "approve_observation_inside_retest",
    "enable_scheduler_or_paper_shadow",
    "call_broker_or_generate_order"
  ],
  "next_task": "TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest",
  "plan_ready": true,
  "planned_recombination_candidates": [
    "growth_tilt_lower_turnover_guarded_v1",
    "growth_tilt_turnover_budgeted_v1",
    "growth_tilt_valid_until_strict_v1",
    "growth_tilt_turnover_budgeted_valid_until_strict_v1",
    "growth_tilt_lower_turnover_guarded_transfer_v1",
    "growth_tilt_conservative_guarded_v1"
  ],
  "reference_candidates": {
    "cooldown_balanced_reference": {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "role": "best_lower_turnover_variant_reference"
    },
    "guarded_turnover_reference": {
      "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
      "role": "guarded_transfer_reference"
    },
    "lower_turnover_reference": {
      "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
      "role": "robustness_reference"
    },
    "raw_growth_tilt_reference": {
      "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "role": "raw_return_engine_reference"
    },
    "static_baseline": {
      "role": "baseline_reference"
    }
  },
  "schema_version": "dynamic_strategy_component_recombination_retest_plan.v1",
  "slice_regime_tests": {
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
  "observation_preview_criteria": {
    "must": [
      "owner_review_candidate_criteria_passed",
      "time_slice_evidence_not_weak",
      "regime_expectation_score_not_weak",
      "drawdown_materiality_not_severe",
      "no_major_guardrail_failure"
    ],
    "note": "actual observation approval must remain separate owner decision"
  },
  "owner_review_candidate_criteria": {
    "must": [
      "cost_adjusted_return_above_static",
      "survives_realistic_cost",
      "survives_conservative_cost",
      "valid_until_window_preserved",
      "no_stale_signal_carry_forward",
      "turnover_not_materially_worse_than_raw_growth_tilt",
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
      "preserve_meaningful_growth_tilt_return",
      "reduce_turnover_vs_raw_growth_tilt",
      "improve_drawdown_vs_raw_growth_tilt",
      "improve_or_preserve_time_slice_evidence",
      "improve_or_preserve_regime_evidence"
    ]
  },
  "schema_version": "dynamic_strategy_recombination_acceptance_criteria.v1"
}
```

## Explicit non-approval list

```json
[
  "candidate_auto_accept",
  "research_only_observation",
  "paper_shadow",
  "paper_trade",
  "shadow_position",
  "event_append",
  "outcome_binding",
  "scheduler",
  "scheduled_task",
  "daily_report",
  "production",
  "broker_order",
  "new_backtest",
  "new_signal",
  "scoring"
]
```

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
  "task_boundary": "RECOMBINATION_CANDIDATE_PLAN_ONLY"
}
```

## Recommended next route

`TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest`