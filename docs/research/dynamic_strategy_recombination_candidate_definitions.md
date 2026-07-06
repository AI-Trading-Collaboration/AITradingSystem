# 动态策略 recombination candidate definitions

- status：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY`

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