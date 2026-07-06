# 动态策略组件 ablation result

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`

|candidate|annual_return|max_drawdown|turnover|stale|gap_static|
|---|---|---|---|---|---|
|`growth_tilt_only_reference`|0.213859|-0.183642|1.964574|0|0.021302|
|`growth_tilt_plus_turnover_budget`|0.199498|-0.139679|2.866904|0|0.006941|
|`growth_tilt_plus_valid_until_strict`|0.199752|-0.134589|3.175612|0|0.007195|
|`growth_tilt_plus_turnover_budget_and_valid_until`|0.201430|-0.139565|2.910022|0|0.008873|
|`lower_turnover_without_cooldown`|0.194762|-0.122866|2.040000|0|0.002205|
|`lower_turnover_plus_growth_tilt_component`|0.207669|-0.157040|2.026245|0|0.015112|
