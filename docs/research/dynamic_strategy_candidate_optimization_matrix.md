# 动态策略 candidate optimization matrix

- status：`DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY`
- primary execution cadence：`valid_until_window`
- monthly rebalance primary ranking：`false`

|candidate|scenario|role|cost_adj|gap|mdd|turnover|holding_days|cooldown_blocks|
|---|---|---|---:|---:|---:|---:|---:|---:|
|`static_baseline`|base|static_baseline|0.192557|0|-0.140068|0|0|0|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|base|ranking_top_from_2365|0.214261|0.021704|-0.183544|1.964574|13.938|0|
|`dynamic_regime_overlay_v0_4_lower_turnover`|base|robustness_top_from_2366|0.195171|0.002614|-0.12271|2.04|47.222|0|
|`dynamic_regime_growth_tilt_lower_turnover_fusion_v1`|base|fusion_candidate|0.195413|0.002856|-0.171152|2.651467|13.373|0|
|`dynamic_regime_growth_tilt_valid_until_cooldown_v1`|base|fusion_candidate|0.195236|0.002679|-0.155058|2.374301|13.176|0|
|`equal_risk_growth_tilt_lower_turnover_guarded_v1`|base|fusion_candidate|0.196221|0.003664|-0.153065|3.004482|12.986|0|
|`static_baseline`|realistic|static_baseline|0.192557|0|-0.140068|0|0|0|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|realistic|ranking_top_from_2365|0.213859|0.021302|-0.183642|1.964574|13.938|0|
|`dynamic_regime_overlay_v0_4_lower_turnover`|realistic|robustness_top_from_2366|0.194762|0.002205|-0.122866|2.04|47.222|0|
|`dynamic_regime_growth_tilt_lower_turnover_fusion_v1`|realistic|fusion_candidate|0.194878|0.002321|-0.171566|2.651467|13.373|0|
|`dynamic_regime_growth_tilt_valid_until_cooldown_v1`|realistic|fusion_candidate|0.194759|0.002202|-0.155376|2.374301|13.176|0|
|`equal_risk_growth_tilt_lower_turnover_guarded_v1`|realistic|fusion_candidate|0.195616|0.003059|-0.153203|3.004482|12.986|0|
|`static_baseline`|conservative|static_baseline|0.192557|0|-0.140068|0|0|0|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|conservative|ranking_top_from_2365|0.21319|0.020633|-0.183805|1.964574|13.938|0|
|`dynamic_regime_overlay_v0_4_lower_turnover`|conservative|robustness_top_from_2366|0.194081|0.001524|-0.123125|2.04|47.222|0|
|`dynamic_regime_growth_tilt_lower_turnover_fusion_v1`|conservative|fusion_candidate|0.193987|0.00143|-0.172256|2.651467|13.373|0|
|`dynamic_regime_growth_tilt_valid_until_cooldown_v1`|conservative|fusion_candidate|0.193962|0.001405|-0.155908|2.374301|13.176|0|
|`equal_risk_growth_tilt_lower_turnover_guarded_v1`|conservative|fusion_candidate|0.19461|0.002053|-0.153432|3.004482|12.986|0|
|`static_baseline`|harsh|static_baseline|0.192557|0|-0.140068|0|0|0|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|harsh|ranking_top_from_2365|0.212521|0.019964|-0.183968|1.964574|13.938|0|
|`dynamic_regime_overlay_v0_4_lower_turnover`|harsh|robustness_top_from_2366|0.1934|0.000843|-0.123384|2.04|47.222|0|
|`dynamic_regime_growth_tilt_lower_turnover_fusion_v1`|harsh|fusion_candidate|0.193097|0.00054|-0.172945|2.651467|13.373|0|
|`dynamic_regime_growth_tilt_valid_until_cooldown_v1`|harsh|fusion_candidate|0.193166|0.000609|-0.156439|2.374301|13.176|0|
|`equal_risk_growth_tilt_lower_turnover_guarded_v1`|harsh|fusion_candidate|0.193604|0.001047|-0.153661|3.004482|12.986|0|