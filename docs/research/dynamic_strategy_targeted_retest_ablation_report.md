# 动态策略 targeted retest ablation report

- status：`DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY`
- primary candidate：`dynamic_regime_overlay_v0_4_lower_turnover`

|ablation|annual|gap_static|mdd|turnover|dependency|supports_guardrail|fragility|
|---|---:|---:|---:|---:|---:|---|---|
|`no_lower_turnover_guardrail`|0.209394|0.016837|-0.16519|1.961886|0.042324|`True`|underperforms_2365_ranking_top_on_return; drawdown_worse_than_static; missed_signal_count_nonzero|
|`no_valid_until_window`|0.194531|0.001974|-0.118301|3|0.009831|`True`|underperforms_2365_ranking_top_on_return; stale_signal_execution_detected|
|`no_cooldown`|0.19443|0.001873|-0.124974|2.2|0.00404|`True`|underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`no_risk_cap`|0.218461|0.025904|-0.183642|1.637918|0.060776|`True`|drawdown_worse_than_static; missed_signal_count_nonzero|
|`no_constraint_filter`|0.194631|0.002074|-0.124447|2.1|0.002312|`True`|underperforms_2365_ranking_top_on_return; missed_signal_count_nonzero|
|`no_growth_tilt_or_risk_overlay`|0.192557|0|-0.140068|0|0.019407|`True`|cost_adjusted_static_gap_non_positive; underperforms_2365_ranking_top_on_return|

- ablation rows 是 research-only diagnostic target-path transformation，不是正式策略配置。
- 任何 observation / paper-shadow / production / broker 讨论仍需 TRADING-2377 owner review。