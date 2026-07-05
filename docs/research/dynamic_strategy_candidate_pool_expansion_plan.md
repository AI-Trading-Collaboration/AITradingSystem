# Dynamic strategy candidate pool expansion plan

## Executive summary

- status：`DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`
- owner decision from 2384：`DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED`
- candidate pool expansion recommended：`true`
- signal family diversification recommended：`true`
- primary execution cadence：`valid_until_window`
- monthly rebalance primary decision：`false`
- next route：`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`

## Why local optimization is no longer enough

- `lower_turnover_line_remains_continue_optimization_after_2379_and_2380`
- `ranking_top_guarded_line_remains_continue_optimization_after_2383`
- `neither_line_reached_research_only_observation_threshold`
- `further_micro_tuning_risks_overfitting_without_signal_family_diversity`
- `owner_decision_2384_requires_candidate_pool_expansion_before_observation`

## Existing candidate line recap

- lower-turnover base：`dynamic_regime_overlay_v0_4_lower_turnover`
- lower-turnover best variant：`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- lower-turnover decision：`CONTINUE_OPTIMIZATION`
- lower-turnover observation approved：`false`
- ranking-top guarded base：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- ranking-top guarded best variant：`equal_risk_growth_tilt_guarded_turnover_v1`
- ranking-top guarded decision：`CONTINUE_OPTIMIZATION`
- ranking-top guarded observation approved：`false`

## Candidate pool expansion plan

- reference candidates：`5`
- signal families：`6`
- new candidates selected for 2386：`12`

## Signal family diversification plan

- `regime_transition_family`：improve behavior around risk-off to recovery transitions
- `trend_confirmation_family`：allow growth tilt only under confirmed trend conditions
- `volatility_aware_family`：improve high-volatility drawdown behavior while preserving low-volatility upside
- `signal_age_valid_until_family`：tune signal decay and expiry behavior
- `turnover_budget_family`：convert turnover control into an explicit optimization constraint
- `risk_cap_interaction_family`：test whether risk cap is too strict or too loose under different signals

## Candidate budget and anti-overfit guardrails

- max total new candidates for 2386：`12`
- post-hoc metric cherry-picking forbidden：`true`
- unbounded parameter grid forbidden：`true`

## 2386 retest plan

- required reference candidates：
- `static_baseline`
- `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- `dynamic_regime_overlay_v0_4_lower_turnover`
- `dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- `equal_risk_growth_tilt_guarded_turnover_v1`
- required new candidates：
- `dynamic_regime_reentry_accelerated_v1`
- `dynamic_regime_recovery_confirmation_v1`
- `dynamic_trend_confirmed_growth_tilt_v1`
- `dynamic_trend_confirmed_low_turnover_v1`
- `dynamic_volatility_scaled_growth_tilt_v1`
- `dynamic_volatility_floor_adjusted_v1`
- `dynamic_signal_age_decay_v1`
- `dynamic_valid_until_expiry_strict_v1`
- `dynamic_turnover_budgeted_growth_tilt_v1`
- `dynamic_turnover_budgeted_regime_overlay_v1`
- `dynamic_risk_cap_adaptive_v1`
- `dynamic_risk_cap_trend_conditioned_v1`

## Acceptance criteria

- must：
- `cost_adjusted_return_above_static`
- `survives_realistic_cost`
- `survives_conservative_cost`
- `valid_until_window_preserved`
- `no_stale_signal_carry_forward`
- `turnover_within_budget`
- `max_drawdown_not_materially_worse_than_reference`
- should：
- `improve_return_vs_lower_turnover_reference`
- `reduce_turnover_vs_original_ranking_top`
- `improve_time_slice_pass_rate`
- `improve_regime_slice_pass_rate`
- `preserve_most_of_ranking_top_upside`
- must_not：
- `rely_on_monthly_rebalance`
- `require_scheduler`
- `require_event_append`
- `require_outcome_binding`
- `require_paper_shadow`
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
- `scoring`

## Data quality gate boundary

- data_quality_gate_executed：`false`
- data_quality_gate_reason：`NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA`
- reason：本任务只读取 prior artifacts，不读取 fresh cached market data，不重新 backtest，不生成新 signal / scoring / daily report。

## Recommended next route

`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`
