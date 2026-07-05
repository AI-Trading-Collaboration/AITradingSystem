# 动态策略 candidate optimization and ranking-robustness divergence review

## Executive summary

- status：`DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`
- data quality：`PASS_WITH_WARNINGS`
- primary execution cadence：`valid_until_window`
- 2365 ranking top：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- 2366 robustness top：`dynamic_regime_overlay_v0_4_lower_turnover`
- divergence detected：`True`
- best candidate after optimization：`dynamic_regime_overlay_v0_4_lower_turnover`
- recommended decision：`OWNER_REVIEW_REQUIRED`
- next route：`TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest`

## Required answers

- 收益 top 为什么领先：2365 ranking top leads because it has the largest valid-until cost-adjusted return and dynamic-vs-static gap, but it accepts higher drawdown and more frequent rebalances.
- 稳健 top 为什么更稳：2366 robustness top ranks first because it keeps lower drawdown, longer average holding days, fewer false risk-off events and positive stress survival, while giving up upside.
- 收益 top 是否可通过降风险变稳：`YES`
- 稳健 top 是否可通过 growth tilt 提升收益：`NO`
- fusion candidate 是否优于二者：`NO`
- valid_until_window 是否继续作为默认执行口径：`YES`
- 是否允许 paper-shadow / production / broker：`NO`
- 下一步：`TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest`

## Updated candidate decision table

|candidate|role|realistic_gap|conservative_gap|harsh_gap|mdd|turnover|score|decision|
|---|---|---:|---:|---:|---:|---:|---:|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|ranking_top_from_2365|0.021302|0.020633|0.019964|-0.183642|1.964574|0.055785|`OWNER_REVIEW_REQUIRED`|
|`dynamic_regime_overlay_v0_4_lower_turnover`|robustness_top_from_2366|0.002205|0.001524|0.000843|-0.122866|2.04|0.075353|`OWNER_REVIEW_REQUIRED`|
|`dynamic_regime_growth_tilt_lower_turnover_fusion_v1`|fusion_candidate|0.002321|0.00143|0.00054|-0.171566|2.651467|0.021763|`CONTINUE_OPTIMIZATION`|
|`dynamic_regime_growth_tilt_valid_until_cooldown_v1`|fusion_candidate|0.002202|0.001405|0.000609|-0.155376|2.374301|0.039173|`CONTINUE_OPTIMIZATION`|
|`equal_risk_growth_tilt_lower_turnover_guarded_v1`|fusion_candidate|0.003059|0.002053|0.001047|-0.153203|3.004482|0.042665|`CONTINUE_OPTIMIZATION`|

## Safety boundary

- 本报告只生成 strategy research evidence。
- monthly rebalance 只允许作为旧口径 reference，不允许进入 primary ranking。
- scheduler、event append、outcome binding、paper-shadow、production、broker/order 和 daily report 全部保持 disabled / false / none。