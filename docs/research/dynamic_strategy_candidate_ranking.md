# 动态策略候选排名

|rank|candidate|decision|cost_adjusted_return|turnover|valid_until_vs_monthly_gap|reason|
|---|---|---|---|---|---|---|
|1|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`OWNER_REVIEW_REQUIRED`|0.214462|1.964574|0.003743|valid-until edge 通过 cost screen，但依赖高 turnover 或 constraint hits。|
|2|`dynamic_regime_overlay_v0_4_lower_turnover`|`CONTINUE_RESEARCH`|0.195375|2.040000|0.006176|valid-until 结果为正，但低于 governed materiality threshold。|
|3|`limited_adjustment`|`REJECT_FOR_NOW`|0.186973|2.400000|-0.004467|valid-until 结果未跑赢 static baseline。|
|4|`dynamic_v0_5_ai_trend_confirmed_only`|`REJECT_FOR_NOW`|0.179889|11.250000|0.001076|valid-until 结果未跑赢 static baseline。|
|5|`defensive_limited_adjustment`|`REJECT_FOR_NOW`|0.160494|3.400000|0.001259|valid-until 结果未跑赢 static baseline。|
|6|`equal_risk_qqq_sgov`|`REJECT_FOR_NOW`|0.058570|0.400000|-0.012054|valid-until 结果未跑赢 static baseline。|