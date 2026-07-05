# 动态策略 sensitivity matrix 摘要

|rank|candidate|decision|realistic_gap|conservative_gap|turnover|fragility|
|---|---|---|---|---|---|---|
|1|`dynamic_regime_overlay_v0_4_lower_turnover`|`OWNER_REVIEW_REQUIRED`|0.002205|0.001524|2.040000|turnover 超出 sensitivity cap|
|2|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`OWNER_REVIEW_REQUIRED`|0.021302|0.020633|1.964574|turnover 超出 sensitivity cap；max drawdown 相对 static 明显恶化|
|3|`limited_adjustment`|`REJECT_FOR_NOW`|-0.006302|-0.007100|2.400000|realistic cost 后不再优于 static；turnover 超出 sensitivity cap|