# 动态策略 top candidate owner review and shadow research gate

## Executive summary

- status：`DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY`
- 2365 ranking top：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- 2366 robustness top：`dynamic_regime_overlay_v0_4_lower_turnover`
- ranking / robustness divergence：`True`
- recommended gate candidate：`dynamic_regime_overlay_v0_4_lower_turnover`
- recommended gate decision：`OWNER_REVIEW_REQUIRED`
- research-only shadow observation allowed：`True`
- owner review required：`True`
- data quality carried forward：`PASS_WITH_WARNINGS`

## Required answers

- 2365 收益 top 是否仍然推荐：`YES_WITH_OWNER_REVIEW`
- 2366 robustness top 是否应替代收益 top：`YES_RESEARCH_ONLY`
- 是否存在 ranking / robustness divergence：`YES`
- 哪个候选最适合 research-only shadow observation：`dynamic_regime_overlay_v0_4_lower_turnover`
- 是否允许真正 paper-shadow：`NO`
- 是否允许 broker / production：`NO`
- 下一步：`TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol`

## Candidate owner review table

|candidate|roles|rank2365|robust_rank|gate_decision|cost_adj|gap|mdd|turnover|fragility|
|---|---|---:|---:|---|---:|---:|---:|---:|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|robustness_top_from_2366, current_dynamic_default|2|1|`OWNER_REVIEW_REQUIRED`|0.194762|0.002205|-0.122866|2.04|NOT_SEVERE|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|ranking_top_from_2365|1|2|`OWNER_REVIEW_REQUIRED`|0.213859|0.021302|-0.183642|1.96457|NOT_SEVERE|
|`static_baseline`|static_baseline|||`CONTINUE_RESEARCH`|0.192557|0|-0.140068|0|NOT_APPLICABLE_STATIC_BASELINE|
|`limited_adjustment`|review_candidate|3|3|`REJECT_FOR_NOW`|0.186255|-0.006302|-0.116361|2.4|NOT_SEVERE|
|`dynamic_v0_5_ai_trend_confirmed_only`|review_candidate|4||`REJECT_FOR_NOW`|0.179889|-0.012668|-0.09768|11.25|UNKNOWN|
|`defensive_limited_adjustment`|review_candidate|5||`REJECT_FOR_NOW`|0.160494|-0.032063|-0.089685|3.4|UNKNOWN|
|`equal_risk_qqq_sgov`|review_candidate|6||`REJECT_FOR_NOW`|0.05857|-0.133987|-0.055981|0.4|UNKNOWN|

## 安全边界

- 本报告只生成 research-only owner review evidence。
- `research_only_shadow_observation_allowed=true` 不等于 paper-shadow execution。
- paper trade、shadow position、event append、outcome binding、production 和 broker/order 全部保持 disabled / false / none。