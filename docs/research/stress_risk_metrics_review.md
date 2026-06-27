# Stress Risk Metrics Review

- 状态：`STRESS_RISK_METRICS_REVIEW_READY_WITH_BLOCKERS`
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

|strategy_id|worst_1d_loss|worst_5d_loss|worst_20d_loss|crash_reaction_delay|stress_gate_status|
|---|---|---|---|---|---|
|no_trade|-0.000231|0.000345|0.002613|815|REVIEWABLE|
|100_qqq|-0.062098|-0.11981|-0.139258|307|REVIEWABLE|
|qqq_60_sgov_40|-0.037134|-0.072633|-0.083535|307|REVIEWABLE|
|qqq_50_sgov_50|-0.030893|-0.060632|-0.069335|307|REVIEWABLE|
|limited_adjustment|-0.031095|-0.0546|-0.088075|14|BLOCKED|
|dynamic_v0_5_ai_trend_confirmed_only|-0.035925|-0.048062|-0.079751|2|BLOCKED|
|limited_adjustment_event_override_v1|-0.031095|-0.0546|-0.089809|14|BLOCKED|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|-0.035925|-0.048062|-0.081629|2|BLOCKED|

## Gate 结论

本报告只用于 research-only owner review；任何 positive row 都不是 paper-shadow、production、broker 或 dynamic promotion approval。
