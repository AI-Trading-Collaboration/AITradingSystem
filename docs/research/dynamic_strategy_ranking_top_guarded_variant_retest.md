# 动态策略 ranking top guarded variant retest

## Executive summary

- status：`DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY`
- data quality：`PASS_WITH_WARNINGS`
- ranking top candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- best guarded variant：`equal_risk_growth_tilt_guarded_turnover_v1`
- best decision：`CONTINUE_OPTIMIZATION`
- next route：`TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_Observation_Decision`

## Source retest plan from TRADING-2382

- TRADING-2382 要求从 lower-turnover 优化线切回 2365 收益 top，但必须叠加 turnover / cooldown / risk-cap / valid-until guardrails 后重新测试。
- 本次 retest 会运行 backtest，但不会批准 paper-shadow / scheduler / production / broker。

## Variant definitions

|variant|role|
|---|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|original return leader reference|
|`equal_risk_growth_tilt_guarded_turnover_v1`|guarded ranking-top variant|
|`equal_risk_growth_tilt_guarded_cooldown_v1`|guarded ranking-top variant|
|`equal_risk_growth_tilt_guarded_risk_cap_v1`|guarded ranking-top variant|
|`equal_risk_growth_tilt_guarded_valid_until_decay_v1`|guarded ranking-top variant|
|`equal_risk_growth_tilt_lower_turnover_fusion_v1`|guarded ranking-top variant|
|`equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1`|guarded ranking-top variant|

## Retest design

- primary execution cadence：`valid_until_window`
- comparison cadences：`valid_until_window`, `cooldown_limited_event_driven`, `signal_event_driven`
- monthly rebalance：legacy reference only；not primary decision
- cost stress：base / realistic / conservative / harsh
- time/regime slice：覆盖 full / recent / post-2023 AI cycle / high-volatility / recovery 以及 risk-on/off 等切片。

## Guarded variant ranking

|rank|variant|decision|annual_return|turnover|time_pass|regime_pass|cost_survival|
|---:|---|---|---:|---:|---:|---:|---|
|1|`equal_risk_growth_tilt_guarded_turnover_v1`|`CONTINUE_OPTIMIZATION`|0.213177|1.897603|0.0|0.0|`harsh`|
|2|`equal_risk_growth_tilt_guarded_valid_until_decay_v1`|`CONTINUE_OPTIMIZATION`|0.212693|1.948307|0.0|0.0|`harsh`|
|3|`equal_risk_growth_tilt_guarded_cooldown_v1`|`CONTINUE_OPTIMIZATION`|0.209439|2.063125|0.0|0.0|`harsh`|
|4|`equal_risk_growth_tilt_guarded_risk_cap_v1`|`CONTINUE_OPTIMIZATION`|0.209163|2.827357|0.285714|0.125|`harsh`|
|5|`equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1`|`CONTINUE_OPTIMIZATION`|0.2003|2.208162|0.0|0.0|`harsh`|
|6|`equal_risk_growth_tilt_lower_turnover_fusion_v1`|`CONTINUE_OPTIMIZATION`|0.198569|2.868816|0.0|0.0|`harsh`|
|7|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`OWNER_REVIEW_REQUIRED`|0.213859|1.964574|0.0|0.0|`harsh`|

## Key answers

- 哪个 guarded variant 最好：`equal_risk_growth_tilt_guarded_turnover_v1`
- 是否保留 ranking top 收益优势：`True`
- 是否降低换手：`False`
- 是否改善 cost-adjusted result：`True`
- 是否改善 drawdown 或 high-volatility 行为：`True` / `True`
- 是否优于 lower-turnover reference：`True`
- 是否可以进入 research-only observation：`False`
- paper-shadow / production / broker：仍全部 disabled / none

## Recommended next route

`TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_Observation_Decision`
