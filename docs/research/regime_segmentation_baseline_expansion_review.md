# Regime Segmentation Baseline Expansion Review

- 状态：`REGIME_BASELINE_EXPANSION_REVIEW_READY_WITH_BLOCKERS`
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

|baseline_id|annual_return|max_drawdown|sharpe|calmar|turnover|
|---|---|---|---|---|---|
|simple_event_risk_off_baseline|0.274555|-0.21044|1.487584|1.30467|8.5|
|qqq_200dma_trend_filter|0.267042|-0.13557|1.585686|1.969769|9.0|
|qqq_90_sgov_9|0.264736|-0.206457|1.45647|1.282279|0.0|
|drawdown_derisk_baseline|0.258948|-0.180357|1.422451|1.435748|9.0|
|qqq_80_sgov_19|0.240509|-0.184787|1.488566|1.30155|0.0|
|qqq_100dma_trend_filter|0.23821|-0.134079|1.516304|1.776636|29.0|
|qqq_70_sgov_30|0.216253|-0.162659|1.529617|1.329485|0.0|
|volatility_targeting_baseline|0.211782|-0.168199|1.363977|1.259117|6.87944|
|qqq_60_sgov_40|0.191987|-0.140068|1.584257|1.370666|0.0|
|qqq_50_sgov_50|0.16773|-0.117008|1.660816|1.433493|0.0|

## Gate 结论

本报告只用于 research-only owner review；任何 positive row 都不是 paper-shadow、production、broker 或 dynamic promotion approval。
