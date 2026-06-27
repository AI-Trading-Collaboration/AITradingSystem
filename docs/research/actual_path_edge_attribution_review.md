# Actual Path Edge Attribution Review

- 状态：`EDGE_ATTRIBUTION_REVIEW_READY`
- market_regime：`ai_after_chatgpt`
- date_range：`2022-12-01` to `2026-06-26`
- data_quality_status：`PASS_WITH_WARNINGS`
- promotion_decision_source：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- dynamic_promotion：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Attribution Summary

|strategy_id|risk_off_event_count|risk_off_net_contribution|risk_off_avoided_drawdown|risk_off_missed_upside|false_risk_off_count|risk_on_recovery_delay_days|qqq_exposure_drag|sgov_allocation_benefit|turnover_drag|actual_vs_static_return_gap|actual_vs_static_risk_gap|verdict|
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|limited_adjustment|6|-0.038911|0.0|0.038911|6|18.333|0.096255|0.060909|0.00024|0.000671|-0.023864|EDGE_SURVIVES_ACTUAL_PATH|
|dynamic_v0_5_ai_trend_confirmed_only|18|-0.153163|0.048438|0.201601|13|16.778|0.106527|0.065792|0.001575|-0.009601|-0.04865|FALSE_RISK_OFF_DOMINATES|
|limited_adjustment_event_override_v1|9|-0.045268|0.010158|0.055426|7|12.333|0.098339|0.061366|0.00068|-0.001413|-0.024844|FALSE_RISK_OFF_DOMINATES|
|dynamic_v0_5_ai_trend_confirmed_event_override_v1|27|-0.142427|0.079971|0.222398|16|11.519|0.104416|0.067057|0.003105|-0.00749|-0.048332|FALSE_RISK_OFF_DOMINATES|

## 结论

- `limited_adjustment` verdict=`EDGE_SURVIVES_ACTUAL_PATH`，主要差距：QQQ exposure drag=`0.096255`，false risk-off cost=`0.038911`，risk-on recovery delay cost=`0.038911`。
- `dynamic_v0_5_ai_trend_confirmed_only` verdict=`FALSE_RISK_OFF_DOMINATES`，主要差距：QQQ exposure drag=`0.106527`，false risk-off cost=`0.201601`，risk-on recovery delay cost=`0.201601`。
- `limited_adjustment_event_override_v1` verdict=`FALSE_RISK_OFF_DOMINATES`，主要差距：QQQ exposure drag=`0.098339`，false risk-off cost=`0.055426`，risk-on recovery delay cost=`0.055426`。
- `dynamic_v0_5_ai_trend_confirmed_event_override_v1` verdict=`FALSE_RISK_OFF_DOMINATES`，主要差距：QQQ exposure drag=`0.104416`，false risk-off cost=`0.222398`，risk-on recovery delay cost=`0.222398`。

本报告只使用 actual-path metrics 和 actual position path 做 ranking / attribution。Target-path metrics 仅可用于 diagnostic，不得进入 promotion gate。
