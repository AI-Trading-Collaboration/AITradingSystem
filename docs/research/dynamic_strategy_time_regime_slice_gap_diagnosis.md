# 动态策略 time/regime slice gap diagnosis

- status：`DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY`
- primary candidate：`dynamic_regime_overlay_v0_4_lower_turnover`

## Time slices

|affected_slice|issue|gap_static|gap_ranking|fix|turnover|retest|
|---|---|---:|---:|---|---|---|
|`early_period`|`early_period_underperformance`|0.011049|-0.069704|`reentry_repair_v1`|`low_to_moderate`|`True`|
|`middle_period`|`middle_period_underperformance`|0.017353|0.029479|`preserve_current_guardrails`|`none`|`False`|
|`recent_period`|`recent_period_underperformance`|-0.025276|-0.028777|`upside_capture_guarded_v1`|`moderate`|`True`|
|`post_2023_ai_cycle`|`post_2023_ai_cycle_underperformance`|0.00097|-0.002215|`return_gap_repair_fusion_v1`|`moderate`|`True`|
|`high_volatility_periods`|`high_volatility_period_underperformance`|-0.074194|-0.076763|`valid_until_decay_tuned_v1`|`low`|`True`|
|`drawdown_recovery_periods`|`drawdown_recovery_lag`|-0.032893|-0.007148|`reentry_repair_v1`|`low_to_moderate`|`True`|

## Regime slices

|affected_regime|issue|gap_static|gap_ranking|fix|turnover|retest|
|---|---|---:|---:|---|---|---|
|`risk_on`|`risk_on_too_defensive`|0.021829|-0.043513|`Allow guarded risk-on weight step when trend and valid-until agree.`|`moderate turnover and drawdown watch`|`True`|
|`risk_off`|`risk_off_not_defensive_enough`|-0.040424|0.031362|`Preserve risk cap and tune expiry/decay instead of raising gross exposure.`|`possible missed upside if too defensive`|`True`|
|`high_volatility`|`high_volatility_reaction_too_late`|-0.020305|-0.038624|`Tune valid-until decay and high-volatility confirmation lag.`|`cost stress required`|`True`|
|`low_volatility`|`low_volatility_upside_capture_insufficient`|0.027161|0.005354|`Add modest growth tilt only under low volatility with risk cap preserved.`|`growth tilt drawdown watch`|`True`|
|`trend_confirmed`|`trend_confirmed_reentry_too_slow`|0.038923|-0.062361|`Shorten re-entry delay and allow small step-up under confirmed trend.`|`small turnover increase`|`True`|
|`trend_uncertain`|`trend_uncertain_false_signal`|-0.07137|0.051926|`Block stale signals and decay near valid-until expiry.`|`possible missed late signal`|`True`|
|`recovery`|`recovery_phase_reentry_lag`|-0.032893|-0.007148|`Use reentry repair with cooldown preserved.`|`whipsaw risk if re-entry is early`|`True`|