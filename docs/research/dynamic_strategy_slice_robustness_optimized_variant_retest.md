# 动态策略 slice-robustness optimized variant retest

## Executive summary

- status：`DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY`
- data quality：`PASS_WITH_WARNINGS`
- base candidate：`dynamic_regime_overlay_v0_4_lower_turnover`
- ranking top reference：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- best variant：`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- best decision：`CONTINUE_OPTIMIZATION`
- next route：`TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_Observation_Decision`

## Source optimization plan from TRADING-2378

- 2378 已要求修复 time/regime slice robustness 与 return gap。
- 本次 retest 只进入 research-only 结论，不批准 paper-shadow / production / broker。

## Variant definitions

|variant|role|
|---|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|base reference|
|`dynamic_regime_overlay_v0_4_reentry_repair_v1`|optimized variant|
|`dynamic_regime_overlay_v0_4_upside_capture_guarded_v1`|optimized variant|
|`dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1`|optimized variant|
|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|optimized variant|
|`dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1`|optimized variant|

## Retest design

- primary execution cadence：`valid_until_window`
- comparison cadences：`valid_until_window`, `cooldown_limited_event_driven`
- monthly rebalance：reference only；not primary decision
- cost stress：base / realistic / conservative / harsh
- time/regime slice：沿用 2376 targeted retest policy，并覆盖 2379 required subset

## Optimized variant ranking

|rank|variant|decision|return_gap_reduction|time_pass|regime_pass|turnover|
|---:|---|---|---:|---:|---:|---:|
|1|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|`CONTINUE_OPTIMIZATION`|0.00807|0.0|0.0|2.537332|
|2|`dynamic_regime_overlay_v0_4_lower_turnover`|`CONTINUE_OPTIMIZATION`|0.0|0.428571|0.0|2.04|
|3|`dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1`|`CONTINUE_OPTIMIZATION`|-0.000123|0.285714|0.0|2.81193|
|4|`dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1`|`REJECT_FOR_NOW`|-0.000917|0.285714|0.0|3.207619|
|5|`dynamic_regime_overlay_v0_4_upside_capture_guarded_v1`|`REJECT_FOR_NOW`|-0.002242|0.142857|0.0|3.82393|
|6|`dynamic_regime_overlay_v0_4_reentry_repair_v1`|`REJECT_FOR_NOW`|-0.002272|0.142857|0.0|3.195687|

## Key answers

- 哪个 variant 最好：`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- 是否优于 base lower-turnover：`True`
- 是否缩小 vs ranking top return gap：`True`
- 是否保持 lower-turnover profile：`True`
- 是否改善 time/regime robustness：`False` / `True`
- 是否穿越 realistic / conservative cost：`True` / `True`
- 是否可以升级到 research-only observation：`False`
- paper-shadow / production / broker：仍全部 disabled / none

## Recommended next route

`TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_Observation_Decision`
