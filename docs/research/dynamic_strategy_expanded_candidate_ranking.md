# Dynamic strategy expanded candidate ranking

- status：`DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY`

|rank|candidate|family|decision|annual|gap_static|gap_lower|gap_guarded|mdd|turnover|time|regime|
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
|1|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`reference_ranking_top`|`CONTINUE_OPTIMIZATION`|0.213859|0.021302|0.019097|0.000682|-0.183642|1.964574|0.000000|0.000000|
|2|`equal_risk_growth_tilt_guarded_turnover_v1`|`reference_guarded_ranking_top`|`CONTINUE_OPTIMIZATION`|0.213177|0.020620|0.018415|0.000000|-0.176319|1.897603|0.000000|0.000000|
|3|`dynamic_turnover_budgeted_growth_tilt_v1`|`turnover_budget_family`|`CONTINUE_OPTIMIZATION`|0.199498|0.006941|0.004736|-0.013679|-0.139679|2.866904|0.428571|0.000000|
|4|`dynamic_valid_until_expiry_strict_v1`|`signal_age_valid_until_family`|`CONTINUE_OPTIMIZATION`|0.199752|0.007195|0.004990|-0.013425|-0.134589|3.175612|0.428571|0.000000|
|5|`dynamic_signal_age_decay_v1`|`signal_age_valid_until_family`|`CONTINUE_OPTIMIZATION`|0.211303|0.018746|0.016541|-0.001874|-0.166345|2.443806|0.000000|0.000000|
|6|`dynamic_regime_overlay_v0_4_lower_turnover`|`reference_lower_turnover`|`CONTINUE_OPTIMIZATION`|0.194762|0.002205|0.000000|-0.018415|-0.122866|2.040000|0.428571|0.000000|
|7|`dynamic_risk_cap_adaptive_v1`|`risk_cap_interaction_family`|`CONTINUE_OPTIMIZATION`|0.198059|0.005502|0.003297|-0.015118|-0.132441|3.550438|0.285714|0.000000|
|8|`dynamic_trend_confirmed_low_turnover_v1`|`trend_confirmation_family`|`CONTINUE_OPTIMIZATION`|0.202558|0.010001|0.007796|-0.010619|-0.150411|2.222882|0.000000|0.000000|
|9|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|`reference_cooldown_balanced`|`CONTINUE_OPTIMIZATION`|0.202832|0.010275|0.008070|-0.010345|-0.142893|2.537332|0.000000|0.000000|
|10|`dynamic_turnover_budgeted_regime_overlay_v1`|`turnover_budget_family`|`CONTINUE_OPTIMIZATION`|0.202471|0.009914|0.007709|-0.010706|-0.142701|2.794822|0.000000|0.000000|
|11|`dynamic_volatility_scaled_growth_tilt_v1`|`volatility_aware_family`|`CONTINUE_OPTIMIZATION`|0.200242|0.007685|0.005480|-0.012935|-0.140913|3.086472|0.000000|0.000000|
|12|`dynamic_trend_confirmed_growth_tilt_v1`|`trend_confirmation_family`|`CONTINUE_OPTIMIZATION`|0.197962|0.005405|0.003200|-0.015215|-0.145608|4.312044|0.000000|0.000000|
|13|`dynamic_volatility_floor_adjusted_v1`|`volatility_aware_family`|`REJECT_FOR_NOW`|0.193448|0.000891|-0.001314|-0.019729|-0.126272|3.101299|0.285714|0.000000|
|14|`dynamic_risk_cap_trend_conditioned_v1`|`risk_cap_interaction_family`|`REJECT_FOR_NOW`|0.191035|-0.001522|-0.003727|-0.022142|-0.132313|3.937585|0.142857|0.000000|
|15|`dynamic_regime_recovery_confirmation_v1`|`regime_transition_family`|`REJECT_FOR_NOW`|0.190302|-0.002255|-0.004460|-0.022875|-0.135265|3.971205|0.142857|0.000000|
|16|`dynamic_regime_reentry_accelerated_v1`|`regime_transition_family`|`REJECT_FOR_NOW`|0.193647|0.001090|-0.001115|-0.019530|-0.146539|3.647931|0.000000|0.000000|