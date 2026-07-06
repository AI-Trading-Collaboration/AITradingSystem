# 动态策略 component recombination decision

- status：`DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW_AND_RECOMBINATION_DECISION_READY`
- recombination plan approved：`True`
- owner decision：`APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL`

```json
{
  "broker_action_enabled": false,
  "decision_inputs": {
    "best_reusable_component": {
      "component": "growth_tilt_engine",
      "decision": "REUSABLE_COMPONENT",
      "source_task": "TRADING-2393"
    },
    "component_attribution_components": [
      "turnover_budgeting",
      "valid_until_strictness",
      "growth_tilt_engine",
      "lower_turnover_guardrail",
      "guarded_turnover_transfer"
    ],
    "component_value_candidates": [
      "dynamic_turnover_budgeted_growth_tilt_v1",
      "dynamic_valid_until_expiry_strict_v1"
    ],
    "guarded_turnover_transfer": {
      "component": "guarded_turnover_transfer",
      "decision": "OWNER_REVIEW_REQUIRED",
      "source_task": "TRADING-2393"
    },
    "lower_turnover_guardrail": {
      "component": "lower_turnover_guardrail",
      "decision": "USE_ONLY_AS_GUARDRAIL",
      "source_task": "TRADING-2393"
    },
    "ranking_top_source_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "recombination_candidate_direction": "RECOMBINE_GROWTH_TILT_WITH_TURNOVER_AND_VALID_UNTIL_GUARDRAILS",
    "source_tasks": [
      "TRADING-2391",
      "TRADING-2392",
      "TRADING-2393"
    ]
  },
  "design_direction": "RECOMBINE_GROWTH_TILT_RETURN_ENGINE_WITH_LOWER_TURNOVER_GUARDRAILS",
  "must_not_start_retest_in_2394": true,
  "owner_decision": "APPROVE_COMPONENT_RECOMBINATION_PLAN_WITH_NO_OBSERVATION_APPROVAL",
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recombination_plan_approved": true,
  "recombination_principles": {
    "candidate_plan_boundary": "TRADING-2395 may design recombination candidates; TRADING-2394 does not run retest or approve observation",
    "guardrail_layer": {
      "primary": [
        "lower_turnover_guardrail",
        "valid_until_window",
        "no_stale_signal_carry_forward",
        "turnover_budgeting_if_supported",
        "cooldown_balancing_if_supported"
      ]
    },
    "must_not": [
      "use_monthly_rebalance_as_primary",
      "optimize_only_for_total_return",
      "remove_risk_guardrails",
      "approve_observation_without_recombined_retest"
    ],
    "must_preserve": [
      "valid_until_window",
      "cost_stress_testing",
      "turnover_budget",
      "no_paper_shadow",
      "no_scheduler",
      "no_broker"
    ],
    "owner_review_layer": [
      "guarded_turnover_transfer"
    ],
    "return_engine": {
      "primary": "growth_tilt_engine",
      "source": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
    },
    "schema_version": "dynamic_strategy_recombination_principles.v1"
  },
  "recommended_next_research_task": "TRADING-2395_Dynamic_Strategy_Component_Recombination_Candidate_Plan",
  "record_ready": true,
  "research_only_observation_approved": false,
  "schema_version": "dynamic_strategy_component_recombination_decision.v1"
}
```