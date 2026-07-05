# 动态策略事件驱动重测

- status: `DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY`
- task: `TRADING-2365_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING`
- market regime：`ai_after_chatgpt`
- requested date range：`2022-12-01 to 2026-07-05`
- data quality：`PASS_WITH_WARNINGS`
- TRADING-2364 source ready：`True`
- primary execution cadence：`valid_until_window`
- candidates tested：`6`
- next route：`TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis`

## 安全边界

- scheduler、event、outcome、paper-shadow、production、broker action 均保持关闭。
- monthly cadence rows 只保留为 deprecated reference evidence。