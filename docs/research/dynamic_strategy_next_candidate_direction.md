# Dynamic strategy next candidate direction

- status：`DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY`
- next direction：`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`
- default direction：`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`
- next route：`TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan`

## Options

|option|decision|reason|
|---|---|---|
|`OPTION_A_CONTINUE_LOWER_TURNOVER_OPTIMIZATION`|`DEFER`|Current lower-turnover line has value, but repeated local variants still failed observation criteria.|
|`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`|`SELECT`|Ranking top still has return advantage; next work should test guarded-turnover and risk-cap controls around that candidate.|
|`OPTION_C_EXPAND_CANDIDATE_POOL`|`DEFER`|Useful after guarded ranking-top retest clarifies local evidence.|
|`OPTION_D_PAUSE_AND_IMPROVE_DATA_PIT_COVERAGE`|`NOT_SELECTED`|No new data-quality blocker is introduced by this prior-artifact decision task.|
|`OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW`|`NOT_SELECTED`|Evidence is insufficient for observation but still supports research-only candidate work.|
