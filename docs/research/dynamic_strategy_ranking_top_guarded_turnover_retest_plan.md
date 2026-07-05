# Dynamic strategy ranking top guarded-turnover retest plan

## Executive summary

- status：`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY`
- ranking top candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- next direction from 2381：`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`
- primary execution cadence：`valid_until_window`
- planned guarded variants：`6`
- recommended route：`TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest`
- paper-shadow / production / broker：`false` / `false` / `none`

## Source decision from TRADING-2381

2381 已确认 lower-turnover 优化线进入局部 plateau，默认下一步切到 `OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`。

## Why switch back to ranking top

2365 ranking top 仍保留收益优势，但不得裸用；2382 只规划 guarded variants，要求在 2383 中用 turnover、cooldown、risk cap 和 valid-until guardrails 重测。

## Ranking top fragility diagnosis

- `turnover_risk`：`evaluate=True`；stress monthly turnover caps and single-step deltas
- `drawdown_risk`：`evaluate=True`；compare drawdown against lower-turnover reference
- `cooldown_fragility`：`evaluate=True`；test cooldown and min-holding variants
- `cost_fragility`：`evaluate=True`；run base, realistic, conservative and harsh cost stress
- `stale_signal_risk`：`evaluate=True`；preserve valid-until and test expiry decay

## Lower-turnover guardrail transfer plan

|guardrail|required|purpose|
|---|---|---|
|`valid_until_window`|`True`|`prevent_stale_signal_carry_forward`|
|`cooldown_balancing`|`True`|`reduce_over_trading_and_noise_response`|
|`max_single_step_weight_delta`|`True`|`limit_single_step_weight_jump`|
|`turnover_cap`|`True`|`control_monthly_turnover`|
|`risk_cap_preservation`|`True`|`avoid_growth_tilt_drawdown_amplification_in_high_risk_regimes`|
|`trend_confirmation_gate`|`True`|`allow_higher_growth_tilt_only_when_trend_is_confirmed`|
|`no_stale_signal_execution`|`True`|`block_execution_after_signal_expiry`|

## Guarded ranking-top variant plan

|candidate|role|guarded|purpose|
|---|---|---|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`base_ranking_top_reference`|`False`|2365 ranking top reference, not guarded|
|`equal_risk_growth_tilt_guarded_turnover_v1`|`guarded_turnover_v1`|`True`|ranking top + max turnover cap + max single-step weight delta|
|`equal_risk_growth_tilt_guarded_cooldown_v1`|`guarded_cooldown_v1`|`True`|ranking top + cooldown / min holding constraints|
|`equal_risk_growth_tilt_guarded_risk_cap_v1`|`guarded_risk_cap_v1`|`True`|ranking top + stricter risk cap in high volatility / risk-off states|
|`equal_risk_growth_tilt_guarded_valid_until_decay_v1`|`guarded_valid_until_decay_v1`|`True`|ranking top + signal decay near valid-until expiry|
|`equal_risk_growth_tilt_lower_turnover_fusion_v1`|`guarded_fusion_v1`|`True`|ranking top return engine + lower-turnover execution guardrails|
|`equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1`|`guarded_fusion_conservative_v1`|`True`|conservative guarded fusion for robustness testing|

## Forbidden optimization paths

- `use_monthly_rebalance_as_primary`
- `remove_valid_until_window`
- `allow_stale_signal_carry_forward`
- `increase_growth_tilt_without_risk_cap`
- `remove_cooldown_entirely_as_final_candidate`
- `ignore_transaction_costs`
- `ignore_slippage`
- `optimize_only_for_total_return`
- `accept_variant_that_improves_return_but_materially_worsens_drawdown`
- `accept_variant_that_requires_scheduler_or_paper_shadow`

## 2383 retest plan

- primary execution cadence：`valid_until_window`
- monthly rebalance allowed for primary decision：`false`
- comparison cadences：`valid_until_window, cooldown_limited_event_driven, signal_event_driven`

## Acceptance criteria

### must

- `cost_adjusted_return_above_static`
- `survives_realistic_cost`
- `survives_conservative_cost`
- `return_gap_vs_original_ranking_top_not_too_large`
- `drawdown_not_materially_worse_than_lower_turnover_reference`
- `turnover_materially_lower_than_original_ranking_top`
- `valid_until_window_preserved`
- `no_stale_signal_carry_forward`

### should

- `preserve_most_of_ranking_top_upside`
- `improve_robustness_vs_original_ranking_top`
- `improve_return_vs_lower_turnover_reference`
- `not_degrade_high_volatility_behavior`
- `not_depend_on_single_time_slice`

### must_not

- `rely_on_monthly_rebalance`
- `require_event_append`
- `require_outcome_binding`
- `require_paper_shadow`
- `require_scheduler`
- `require_production_or_broker`

## Explicit non-goals

- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker`
- `order`
- `new_backtest`
- `new_signal`
- `fresh_cached_market_data`
- `scoring`

## Recommended next route

`TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest`
