# 动态策略 slice robustness and return-gap optimization plan

## Executive summary

- status：`DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY`
- primary candidate：`dynamic_regime_overlay_v0_4_lower_turnover`
- ranking top reference：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- decision from 2377：`KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION`
- primary execution cadence：`valid_until_window`
- planned variant count：`5`
- next route：`TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest`

## Source findings from TRADING-2376 / 2377

- 2376 decision remained `CONTINUE_OPTIMIZATION`.
- 2377 owner decision kept the line research-only and approved optimization.
- 2376 did not justify research-only observation or paper-shadow.

## Why optimization continues

- cost stress remained positive, so the candidate is not discarded.
- time/regime slice robustness failed, so observation is not approved.
- return gap vs ranking top remains negative, so variant retest is required.

## Time-slice gap diagnosis

|affected_slice|issue|gap_static|gap_ranking|fix|turnover|retest|
|---|---|---:|---:|---|---|---|
|`early_period`|`early_period_underperformance`|0.011049|-0.069704|`reentry_repair_v1`|`low_to_moderate`|`True`|
|`middle_period`|`middle_period_underperformance`|0.017353|0.029479|`preserve_current_guardrails`|`none`|`False`|
|`recent_period`|`recent_period_underperformance`|-0.025276|-0.028777|`upside_capture_guarded_v1`|`moderate`|`True`|
|`post_2023_ai_cycle`|`post_2023_ai_cycle_underperformance`|0.00097|-0.002215|`return_gap_repair_fusion_v1`|`moderate`|`True`|
|`high_volatility_periods`|`high_volatility_period_underperformance`|-0.074194|-0.076763|`valid_until_decay_tuned_v1`|`low`|`True`|
|`drawdown_recovery_periods`|`drawdown_recovery_lag`|-0.032893|-0.007148|`reentry_repair_v1`|`low_to_moderate`|`True`|

## Regime-slice gap diagnosis

|affected_regime|issue|gap_static|gap_ranking|fix|turnover|retest|
|---|---|---:|---:|---|---|---|
|`risk_on`|`risk_on_too_defensive`|0.021829|-0.043513|`Allow guarded risk-on weight step when trend and valid-until agree.`|`moderate turnover and drawdown watch`|`True`|
|`risk_off`|`risk_off_not_defensive_enough`|-0.040424|0.031362|`Preserve risk cap and tune expiry/decay instead of raising gross exposure.`|`possible missed upside if too defensive`|`True`|
|`high_volatility`|`high_volatility_reaction_too_late`|-0.020305|-0.038624|`Tune valid-until decay and high-volatility confirmation lag.`|`cost stress required`|`True`|
|`low_volatility`|`low_volatility_upside_capture_insufficient`|0.027161|0.005354|`Add modest growth tilt only under low volatility with risk cap preserved.`|`growth tilt drawdown watch`|`True`|
|`trend_confirmed`|`trend_confirmed_reentry_too_slow`|0.038923|-0.062361|`Shorten re-entry delay and allow small step-up under confirmed trend.`|`small turnover increase`|`True`|
|`trend_uncertain`|`trend_uncertain_false_signal`|-0.07137|0.051926|`Block stale signals and decay near valid-until expiry.`|`possible missed late signal`|`True`|
|`recovery`|`recovery_phase_reentry_lag`|-0.032893|-0.007148|`Use reentry repair with cooldown preserved.`|`whipsaw risk if re-entry is early`|`True`|

## Return gap decomposition

|component|assessment|variant|retest|
|---|---|---|---|
|`upside_capture_gap`|`repairable_with_moderate_turnover_increase`|`upside_capture_guarded_v1`|`True`|
|`risk_on_weight_gap`|`repairable_with_moderate_turnover_increase`|`return_gap_repair_fusion_v1`|`True`|
|`reentry_delay_gap`|`repairable_without_turnover_increase`|`reentry_repair_v1`|`True`|
|`excessive_defensive_floor_gap`|`repair_requires_higher_drawdown_risk`|`return_gap_repair_fusion_v1`|`True`|
|`valid_until_expiry_gap`|`repairable_without_turnover_increase`|`valid_until_decay_tuned_v1`|`True`|
|`cooldown_block_gap`|`repairable_without_turnover_increase`|`cooldown_balanced_v1`|`True`|
|`turnover_cap_opportunity_cost`|`not_recommended_to_repair`|`preserve_conservative_turnover_cap`|`False`|
|`growth_tilt_intensity_gap`|`repairable_with_moderate_turnover_increase`|`upside_capture_guarded_v1`|`True`|

## Variant plan

|variant|purpose|turnover|cost|priority|
|---|---|---|---|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|current robustness top reference|`none`|`none`|`reference`|
|`dynamic_regime_overlay_v0_4_reentry_repair_v1`|reduce recovery / trend-confirmed reentry lag|`low_to_moderate`|`low`|`high`|
|`dynamic_regime_overlay_v0_4_upside_capture_guarded_v1`|improve low-volatility and risk-on upside capture|`moderate`|`moderate_cost_stress_required`|`high`|
|`dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1`|reduce stale signal and near-expiry drag|`low`|`low`|`medium`|
|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|reduce missed upside caused by cooldown without overtrading|`low_to_moderate`|`moderate_cost_stress_required`|`medium`|
|`dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1`|combine lower-turnover guardrail with limited growth tilt|`moderate`|`moderate_to_high_cost_stress_required`|`high`|

## Forbidden optimization paths

- `remove_lower_turnover_guardrail_without_replacement`
- `use_monthly_rebalance_as_primary`
- `allow_stale_signal_carry_forward`
- `increase_growth_tilt_without_risk_cap`
- `remove_cooldown_entirely_as_final_candidate`
- `increase_turnover_without_cost_stress`
- `optimize_only_for_total_return`
- `ignore_drawdown_or_regime_slice_failures`

## 2379 evaluation plan

- variants must beat static on cost-adjusted basis.
- variants must improve time-slice robustness vs base candidate.
- variants must improve or preserve regime-slice robustness.
- variants must reduce return gap vs ranking top.
- variants must preserve lower-turnover profile and survive realistic/conservative cost.

## Explicit non-goals

- 不启用 scheduler，不创建 scheduled task。
- 不 append event，不 bind outcome，不 mutate outcome store。
- 不启用 paper-shadow，不创建 paper trade 或 shadow position。
- 不进入 production，不调用 broker，不生成 order。
- 不生成 daily report。

## Recommended next route

`TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest`