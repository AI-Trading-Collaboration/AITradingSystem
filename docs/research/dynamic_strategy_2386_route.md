# Dynamic strategy 2386 route

- current task：`TRADING-2385_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN`
- status：`DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_DIVERSIFICATION_PLAN_READY`
- next route：`TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening`
- next route type：expanded candidate pool retest and screening
- primary execution cadence：`valid_until_window`
- monthly rebalance primary decision：`false`
- scheduler enabled：`false`
- event append enabled：`false`
- outcome binding enabled：`false`
- paper shadow enabled：`false`
- production enabled：`false`
- broker action enabled：`false`
- daily report generated：`false`

TRADING-2386 只能在 cached-data quality gate 通过后执行 expanded candidate pool retest；2385 本身不批准 observation、paper-shadow、production 或 broker/order。
