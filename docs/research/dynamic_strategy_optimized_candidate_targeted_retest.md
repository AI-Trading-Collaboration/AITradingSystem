# 动态策略 optimized candidate targeted retest

## Executive summary

- status：`DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY`
- data quality：`PASS_WITH_WARNINGS`
- primary candidate：`dynamic_regime_overlay_v0_4_lower_turnover`
- decision from 2375：`OWNER_REVIEW_REQUIRED`
- primary execution cadence：`valid_until_window`
- candidate decision after targeted retest：`CONTINUE_OPTIMIZATION`
- next route：`TRADING-2377_Dynamic_Strategy_Targeted_Retest_Owner_Review_And_Observation_Decision`

## Required answers

- 是否仍优于 static baseline：`YES` (realistic cost-adjusted basis)
- 是否穿越 time slices：`NO`
- 是否穿越 market regimes：`NO`
- 是否穿越 realistic / conservative cost：`YES` / `YES`
- lower-turnover guardrail 是否有贡献：`YES`
- valid_until_window 是否仍必要：`YES`
- ablation 是否支持 guardrails：`YES`
- 是否可升级到 research-only observation：`NO`
- 是否允许 paper-shadow / production / broker：`NO`

## Decision update

- time slice pass rate：`0.428571`
- regime slice pass rate：`0.0`
- ablation support rate：`1.0`
- realistic gap：`0.002205`
- conservative gap：`0.001524`
- harsh gap：`0.000843`
- decision reasons：`realistic_gap=0.002205; conservative_gap=0.001524; harsh_gap=0.000843; time_slice_pass_rate=0.428571; regime_slice_pass_rate=0.0; ablation_support_rate=1.0; turnover=2.04; dynamic_vs_ranking_top_gap=-0.019097`

## Safety boundary

- 本报告只生成 strategy research evidence。
- monthly rebalance 只允许作为旧口径 reference，不允许进入 primary decision。
- scheduler、event append、outcome binding、paper-shadow、production、broker/order 和 daily report 全部保持 disabled / false / none。