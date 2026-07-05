# 动态策略 shadow observation review thresholds

|trigger|condition|action|
|---|---|---|
|`drawdown_trigger`|`candidate_drawdown_materially_worse_than_static_baseline`|`OWNER_REVIEW_REQUIRED`|
|`turnover_trigger`|`expected_turnover_above_owner_accepted_threshold`|`OWNER_REVIEW_REQUIRED`|
|`cost_fragility_trigger`|`edge_disappears_under_realistic_cost_assumptions`|`OWNER_REVIEW_REQUIRED`|
|`divergence_trigger`|`ranking_top_and_robustness_top_disagree_repeatedly`|`OWNER_REVIEW_REQUIRED`|
|`stale_signal_trigger`|`signal_executes_outside_valid_until_window`|`BLOCK_OBSERVATION_AND_REPORT`|
|`guardrail_trigger`|`any_paper_shadow_production_or_broker_flag_true`|`HARD_FAIL`|

## Policy note

- 当前只定义 qualitative owner-review triggers。
- 未引入新的 numeric investment threshold。
- 任何 paper-shadow、production 或 broker flag 变为 true 都必须 hard fail。