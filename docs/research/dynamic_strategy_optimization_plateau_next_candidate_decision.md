# Dynamic strategy optimization plateau and next candidate decision

## Executive summary

- status：`DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY`
- optimization plateau：`LOWER_TURNOVER_LOCAL_PLATEAU_DETECTED`
- next direction：`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`
- recommended route：`TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan`
- research-only observation approved：`false`
- paper-shadow / production / broker：`false` / `false` / `none`

## Source findings from TRADING-2379 / 2380

- best variant from 2379：`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`
- best variant decision：`CONTINUE_OPTIMIZATION`
- observation approved from 2380：`False`
- return gap reduction vs base：`0.00807`
- variant vs ranking top gap：`-0.011027`
- time slice pass rate：`0.0`
- regime slice pass rate：`0.0`

## Lower-turnover optimization review

当前 lower-turnover 线仍有研究价值，但多轮优化后 best decision 仍为 `CONTINUE_OPTIMIZATION`，不满足 observation 门槛。

## Plateau assessment

- plateau scope：`lower_turnover_local_optimization_line`
- `LOWER_TURNOVER_LINE_HAS_MULTIPLE_POST_2376_OPTIMIZATION_PASSES`
- `BEST_VARIANT_REMAINS_CONTINUE_OPTIMIZATION`
- `OBSERVATION_REJECTED_BY_2380_OWNER_REVIEW`
- `TIME_AND_REGIME_SLICE_ROBUSTNESS_STILL_FAIL`
- `RANKING_TOP_RETURN_ADVANTAGE_REMAINS`

## Why observation is still not approved

- `TIME_SLICE_ROBUSTNESS_NOT_READY`
- `REGIME_SLICE_ROBUSTNESS_NOT_READY`
- `RETURN_GAP_VS_RANKING_TOP_REMAINS`
- `OBSERVATION_ACCEPTANCE_CRITERIA_NOT_MET`

## Next-direction options

|option|decision|reason|
|---|---|---|
|`OPTION_A_CONTINUE_LOWER_TURNOVER_OPTIMIZATION`|`DEFER`|Current lower-turnover line has value, but repeated local variants still failed observation criteria.|
|`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`|`SELECT`|Ranking top still has return advantage; next work should test guarded-turnover and risk-cap controls around that candidate.|
|`OPTION_C_EXPAND_CANDIDATE_POOL`|`DEFER`|Useful after guarded ranking-top retest clarifies local evidence.|
|`OPTION_D_PAUSE_AND_IMPROVE_DATA_PIT_COVERAGE`|`NOT_SELECTED`|No new data-quality blocker is introduced by this prior-artifact decision task.|
|`OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW`|`NOT_SELECTED`|Evidence is insufficient for observation but still supports research-only candidate work.|

## Recommended next direction

`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`。下一步应回到 2365 收益 top，用 lower-turnover guardrail、risk cap 和 cooldown 约束修复其脆弱性。

## Explicit non-goals

- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker`
- `order`
- `new_backtest`
- `new_signal`

## Guardrail summary

- scheduler_enabled：`false`
- event_append_enabled：`false`
- outcome_binding_enabled：`false`
- paper_shadow_enabled：`false`
- production_enabled：`false`
- broker_action_enabled：`false`
- daily_report_generated：`false`

## Recommended next route

`TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan`
