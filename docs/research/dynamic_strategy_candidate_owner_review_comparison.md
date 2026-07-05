# 动态策略 candidate owner review comparison

|candidate|roles|rank2365|robust_rank|gate_decision|cost_adj|gap|mdd|turnover|fragility|
|---|---|---:|---:|---|---:|---:|---:|---:|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|robustness_top_from_2366, current_dynamic_default|2|1|`OWNER_REVIEW_REQUIRED`|0.194762|0.002205|-0.122866|2.04|NOT_SEVERE|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|ranking_top_from_2365|1|2|`OWNER_REVIEW_REQUIRED`|0.213859|0.021302|-0.183642|1.96457|NOT_SEVERE|
|`static_baseline`|static_baseline|||`CONTINUE_RESEARCH`|0.192557|0|-0.140068|0|NOT_APPLICABLE_STATIC_BASELINE|
|`limited_adjustment`|review_candidate|3|3|`REJECT_FOR_NOW`|0.186255|-0.006302|-0.116361|2.4|NOT_SEVERE|
|`dynamic_v0_5_ai_trend_confirmed_only`|review_candidate|4||`REJECT_FOR_NOW`|0.179889|-0.012668|-0.09768|11.25|UNKNOWN|
|`defensive_limited_adjustment`|review_candidate|5||`REJECT_FOR_NOW`|0.160494|-0.032063|-0.089685|3.4|UNKNOWN|
|`equal_risk_qqq_sgov`|review_candidate|6||`REJECT_FOR_NOW`|0.05857|-0.133987|-0.055981|0.4|UNKNOWN|

## Owner review checklist

- `RANKING_ROBUSTNESS_DIVERGENCE`：owner_review_required=`True`；确认是否优先选择 robustness top 进入 research-only observation。
- `TURNOVER_ACCEPTABILITY`：owner_review_required=`True`；确认 turnover 是否可接受，或是否先继续优化执行约束。
- `DRAWDOWN_TOLERANCE`：owner_review_required=`True`；确认收益排名 top 的 drawdown 是否仍可接受。
- `RESEARCH_ONLY_SHADOW_BOUNDARY`：owner_review_required=`True`；确认下一步仅进入 research-only protocol，不创建 paper trade / shadow position。