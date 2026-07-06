# 动态策略 recombination principles

```json
{
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
}
```