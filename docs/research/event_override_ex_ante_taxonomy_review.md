# Event Override Ex-Ante Taxonomy Review

- 状态：`EVENT_OVERRIDE_EX_ANTE_TAXONOMY_READY_WITH_RUNTIME_GAPS`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- event_override_role：`watch_only`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Ex-Ante Taxonomy

|event_type|source|price_independent|future_return_independent|status|gate_impact|
|---|---|---|---|---|---|
|macro_policy_shock|FOMC statement or press conference;BLS CPI release;BEA PCE release;BLS payroll release|True|True|PASS|watch_only_taxonomy_rule|
|geopolitical_shock|official government sanction notice;exchange halt notice;major verified wire-service headline|True|True|PASS|watch_only_taxonomy_rule|
|credit_liquidity_shock|FDIC bank failure notice;Federal Reserve emergency facility announcement;public credit spread or funding stress monitor|True|True|PASS|watch_only_taxonomy_rule|
|AI_semiconductor_supply_chain_shock|BIS export control notice;company guidance release;verified supplier disruption disclosure|True|True|PASS|watch_only_taxonomy_rule|
|earnings_shock|SEC 8-K;company earnings release;company guidance call transcript|True|True|PASS|watch_only_taxonomy_rule|

## Runtime Trace Guard

|strategy_id|event_review_count|missing_event_type_count|failed_no_lookahead_count|status|gate_impact|
|---|---|---|---|---|---|
|limited_adjustment_event_override_v1|9|9|0|WARN|blocks_event_override_preflight|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|27|27|0|WARN|blocks_event_override_preflight|

## Gate 结论

Event severity 不得由未来价格下跌或未来收益反推；risk-off override 只能降低风险，risk-on override 默认禁用或慢确认。当前结论只允许 watch-only owner review，不能解锁 paper-shadow。
