# Dynamic Strategy Walk-Forward Validation

- 状态：`WALK_FORWARD_VALIDATION_READY_WITH_BLOCKERS`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Strategy Stability

|strategy_id|completed_split_count|top_half_rate|baseline_beat_rate|rank_std|verdict|
|---|---|---|---|---|---|
|limited_adjustment|4|0.75|0.5|0.866025|STABLE_ACROSS_WINDOWS|
|dynamic_v0_5_ai_trend_confirmed_only|4|0.25|0.25|1.089725|REGIME_OVERFITTED|
|limited_adjustment_event_override_v1|4|0.75|0.5|1.414214|PARAMETER_SENSITIVE|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|4|0.5|0.25|1.920286|PARAMETER_SENSITIVE|

## Gate 结论

Walk-forward validation 只重算 actual position path 的 realized metrics。未通过 OOS / stability / baseline beat 要求前，不得进入 paper-shadow preflight。
