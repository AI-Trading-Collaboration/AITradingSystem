# 动态策略 TRADING-2397 路由

- status：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_RETEST_READY`
- 推荐下一路由：`TRADING-2397_Dynamic_Strategy_Recombination_Candidate_Owner_Review_And_Observation_Decision`
- best recombination candidate：`growth_tilt_lower_turnover_guarded_transfer_v1`
- best recombination decision：`OWNER_REVIEW_REQUIRED`

TRADING-2396 只生成 owner review package。即使出现 `ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION_PREVIEW`，也必须在 2397 单独记录 owner decision；2396 不批准 observation、paper-shadow、scheduler、production 或 broker。
