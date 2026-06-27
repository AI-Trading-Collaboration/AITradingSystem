# Transaction Cost Cash Yield Audit

- 状态：`TRANSACTION_COST_CASH_YIELD_REVIEW_READY_WITH_BLOCKERS`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Review Table

|strategy_id|gross_annual_return|net_annual_return|turnover_cost|cash_yield_contribution|sgov_dividend_contribution|net_cost_gate_status|
|---|---|---|---|---|---|---|
|no_trade|0.04723|0.04723|0.0|0.0|0.163743|REVIEWABLE|
|100_qqq|0.288913|0.288913|0.0|0.0|0.0|REVIEWABLE|
|qqq_60_sgov_40|0.191987|0.191987|0.0|0.0|0.065497|REVIEWABLE|
|qqq_50_sgov_50|0.16773|0.16773|0.0|0.0|0.081871|REVIEWABLE|
|limited_adjustment|0.192739|0.192416|0.000323|0.0|0.060909|REVIEWABLE|
|dynamic_v0_5_ai_trend_confirmed_only|0.182912|0.180811|0.002101|0.0|0.065792|REVIEWABLE|
|limited_adjustment_event_override_v1|0.190802|0.18989|0.000912|0.0|0.061366|REVIEWABLE|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|0.185534|0.181393|0.004141|0.0|0.067057|REVIEWABLE|

## Gate 结论

本报告只用于 research-only owner review；任何 positive row 都不是 paper-shadow、production、broker 或 dynamic promotion approval。
