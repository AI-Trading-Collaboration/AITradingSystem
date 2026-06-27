# PIT Data Availability Audit

- 状态：`PIT_DATA_AVAILABILITY_REVIEW_READY_WITH_CAVEATS`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Signal PIT Inventory

|signal_id|strategy_id|pit_risk_level|promotion_gate_blocker|promotion_gate_impact|
|---|---|---|---|---|
|market_price_close_QQQ_TQQQ_SGOV|multiple|PIT_APPROXIMATED|False|watch_only_caveat_exact_vendor_release_time_not_persisted|
|treasury_rate_series|multiple|PIT_REVISED_DATA_RISK|False|blocks_if_promoted_as_direct_signal_without_lag_policy|
|target_path_metrics|multiple|PIT_SAFE|False|target_path_metrics_forbidden_for_promotion|
|limited_adjustment:close_based_dynamic_v3_target|limited_adjustment|PIT_APPROXIMATED|False|watch_only_caveat_date_level_pit|
|dynamic_v0_5_ai_trend_confirmed_only:close_based_ai_trend_confirmation|dynamic_v0_5_ai_trend_confirmed_only|PIT_APPROXIMATED|False|watch_only_caveat_date_level_pit|
|limited_adjustment_event_override_v1:close_based_dynamic_v3_target|limited_adjustment_event_override_v1|PIT_APPROXIMATED|False|watch_only_caveat_date_level_pit|
|limited_adjustment_event_override_v1:event_override_decision|limited_adjustment_event_override_v1|PIT_APPROXIMATED|False|watch_only_until_ex_ante_taxonomy_review|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1:close_based_ai_trend_confirmation|dynamic_v0_5_ai_trend_confirmed_event_override_v1|PIT_APPROXIMATED|False|watch_only_caveat_date_level_pit|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1:event_override_decision|dynamic_v0_5_ai_trend_confirmed_event_override_v1|PIT_APPROXIMATED|False|watch_only_until_ex_ante_taxonomy_review|

## Gate 结论

任何 `PIT_UNKNOWN` 或 `PIT_BLOCKING` signal 都不得进入 promotion gate；`PIT_APPROXIMATED` signal 只能作为 watch-only evidence，并必须带 caveat。
Target-path metrics 继续保持 diagnostic-only，不能用于晋级。
