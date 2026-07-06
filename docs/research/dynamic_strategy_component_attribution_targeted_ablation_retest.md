# 动态策略组件归因 targeted ablation retest

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`
- data quality：`PASS_WITH_WARNINGS`
- primary execution cadence：`valid_until_window`
- best reusable component：`growth_tilt_engine`
- next route：`TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision`

## 执行摘要

TRADING-2393 已实际运行 targeted ablation retest，用同一 actual-position path 评估组件级收益、回撤、换手、成本压力、stale signal 与 slice 表现。本报告不批准 observation、paper-shadow、scheduler、event append、outcome binding、production 或 broker 动作。

## 2392 来源计划

```json
{
  "candidate_ranking_2365": "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY",
  "component_attribution_plan_2392": "DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY",
  "expanded_candidate_ranking_2386": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY",
  "expanded_candidate_retest_2386": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY",
  "owner_review_decision_2391": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY",
  "reclassification_result_2390": "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_RECLASSIFICATION_AND_COMPONENT_ATTRIBUTION_READY",
  "sensitivity_result_2366": "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
}
```

## ablation retest 设计

```json
{
  "ablation_candidates": [
    {
      "candidate_id": "growth_tilt_only_reference",
      "components": [
        "growth_tilt_engine"
      ],
      "purpose": "measure raw growth tilt engine"
    },
    {
      "candidate_id": "growth_tilt_plus_turnover_budget",
      "components": [
        "growth_tilt_engine",
        "turnover_budgeting"
      ],
      "purpose": "test whether turnover budgeting improves execution without killing return"
    },
    {
      "candidate_id": "growth_tilt_plus_valid_until_strict",
      "components": [
        "growth_tilt_engine",
        "valid_until_strictness"
      ],
      "purpose": "test whether strict expiry improves stale signal control"
    },
    {
      "candidate_id": "growth_tilt_plus_turnover_budget_and_valid_until",
      "components": [
        "growth_tilt_engine",
        "turnover_budgeting",
        "valid_until_strictness"
      ],
      "purpose": "test combined component transfer"
    },
    {
      "candidate_id": "lower_turnover_without_cooldown",
      "components": [
        "lower_turnover_guardrail"
      ],
      "purpose": "measure cooldown contribution"
    },
    {
      "candidate_id": "lower_turnover_plus_growth_tilt_component",
      "components": [
        "lower_turnover_guardrail",
        "guarded_turnover_transfer",
        "growth_tilt_engine"
      ],
      "purpose": "test whether lower-turnover reference can gain upside without losing robustness"
    }
  ],
  "comparison_cadences": [
    "valid_until_window",
    "cooldown_limited_event_driven",
    "signal_event_driven"
  ],
  "components_tested": [
    "turnover_budgeting",
    "valid_until_strictness",
    "growth_tilt_engine",
    "lower_turnover_guardrail",
    "guarded_turnover_transfer"
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
  "monthly_rebalance": {
    "allowed_for_primary_decision": false,
    "allowed_for_reference": true
  },
  "primary_execution_cadence": "valid_until_window",
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
```

## 组件归因矩阵

|component|candidate|decision|score|static_gap|turnover|stale|cost|
|---|---|---|---|---|---|---|---|
|`turnover_budgeting`|`growth_tilt_plus_turnover_budget`|`CONTINUE_COMPONENT_RESEARCH`|-0.202011|0.006941|2.866904|0|`harsh`|
|`valid_until_strictness`|`growth_tilt_plus_valid_until_strict`|`CONTINUE_COMPONENT_RESEARCH`|-0.352534|0.007195|3.175612|0|`harsh`|
|`growth_tilt_engine`|`growth_tilt_only_reference`|`REUSABLE_COMPONENT`|0.084238|0.021302|1.964574|0|`harsh`|
|`lower_turnover_guardrail`|`lower_turnover_without_cooldown`|`USE_ONLY_AS_GUARDRAIL`|0.422998|0.002205|2.040000|0|`harsh`|
|`guarded_turnover_transfer`|`lower_turnover_plus_growth_tilt_component`|`OWNER_REVIEW_REQUIRED`|0.000919|0.015112|2.026245|0|`harsh`|
|`combined_turnover_budgeting_and_valid_until`|`growth_tilt_plus_turnover_budget_and_valid_until`|`CONTINUE_COMPONENT_RESEARCH`|-0.212252|0.008873|2.910022|0|`harsh`|

## reusable component decision

```json
{
  "best_reusable_component": "growth_tilt_engine",
  "best_reusable_component_decision": "REUSABLE_COMPONENT",
  "broker_action_enabled": false,
  "component_decisions": {
    "combined_turnover_budgeting_and_valid_until": "CONTINUE_COMPONENT_RESEARCH",
    "growth_tilt_engine": "REUSABLE_COMPONENT",
    "guarded_turnover_transfer": "OWNER_REVIEW_REQUIRED",
    "lower_turnover_guardrail": "USE_ONLY_AS_GUARDRAIL",
    "turnover_budgeting": "CONTINUE_COMPONENT_RESEARCH",
    "valid_until_strictness": "CONTINUE_COMPONENT_RESEARCH"
  },
  "guardrail_only_components": [
    "lower_turnover_guardrail"
  ],
  "owner_review_required_components": [
    "guarded_turnover_transfer"
  ],
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recombination_candidate_direction": "RECOMBINE_GROWTH_TILT_WITH_TURNOVER_AND_VALID_UNTIL_GUARDRAILS",
  "recommended_next_research_task": "TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision",
  "research_only_observation_approved": false,
  "reusable_component_decision_ready": true,
  "reusable_components": [
    "growth_tilt_engine"
  ],
  "schema_version": "dynamic_strategy_reusable_component_decision.v1"
}
```

## 明确未批准事项

- candidate auto-accept：`False`
- research-only observation：`False`
- paper-shadow：`False`
- event append：`False`
- outcome binding：`False`
- scheduler：`False`
- production：`False`
- broker/order：`False`
