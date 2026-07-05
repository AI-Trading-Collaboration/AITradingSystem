# 动态策略 targeted retest slice report

- status：`DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY`
- primary candidate：`dynamic_regime_overlay_v0_4_lower_turnover`

## Time slices

|candidate|slice|annual|gap_static|gap_ranking|mdd|turnover|pass|fragility|
|---|---|---:|---:|---:|---:|---:|---|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`full_available_window`|0.194762|0.002205|-0.019097|-0.122866|2.04|`True`|underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`early_period`|0.269304|0.011049|-0.069704|-0.07882|0.72|`False`|underperforms_2365_ranking_top_on_return; drawdown_worse_than_static; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`middle_period`|0.056688|0.017353|0.029479|-0.122866|0.42|`True`|missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`recent_period`|0.271282|-0.025276|-0.028777|-0.057731|0.9|`False`|cost_adjusted_static_gap_non_positive; underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`post_2023_ai_cycle`|0.173873|0.00097|-0.002215|-0.122866|1.32|`True`|underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`high_volatility_periods`|0.172116|-0.074194|-0.076763|-0.094423|0.78|`False`|cost_adjusted_static_gap_non_positive; underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`drawdown_recovery_periods`|-0.018014|-0.032893|-0.007148|-0.150624|0.42|`False`|cost_adjusted_static_gap_non_positive; underperforms_2365_ranking_top_on_return; drawdown_worse_than_static; missed_signal_count_nonzero|

## Regime slices

|candidate|slice|annual|gap_static|gap_ranking|mdd|turnover|pass|fragility|
|---|---|---:|---:|---:|---:|---:|---|---|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`risk_on`|0.297267|0.021829|-0.043513|-0.062985|1.02|`False`|underperforms_2365_ranking_top_on_return; drawdown_worse_than_static|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`risk_off`|-0.059287|-0.040424|0.031362|-0.161213|1.02|`False`|cost_adjusted_static_gap_non_positive; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`high_volatility`|0.206297|-0.020305|-0.038624|-0.100562|1.62|`False`|cost_adjusted_static_gap_non_positive; underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`low_volatility`|0.204789|0.027161|0.005354|-0.056361|0.42|`False`|drawdown_worse_than_static; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`trend_confirmed`|0.39082|0.038923|-0.062361|-0.049203|0.78|`False`|underperforms_2365_ranking_top_on_return; drawdown_worse_than_static|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`trend_uncertain`|-0.431431|-0.07137|0.051926|-0.129119|0.24|`False`|cost_adjusted_static_gap_non_positive; drawdown_worse_than_static|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`drawdown`|-0.304736|-0.028073|0.127604|-0.163103|0.36|`False`|cost_adjusted_static_gap_non_positive; missed_signal_count_nonzero|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`recovery`|-0.018014|-0.032893|-0.007148|-0.150624|0.42|`False`|cost_adjusted_static_gap_non_positive; underperforms_2365_ranking_top_on_return; drawdown_worse_than_static; missed_signal_count_nonzero|