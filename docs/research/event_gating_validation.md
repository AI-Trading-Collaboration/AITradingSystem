# Event Gating Validation

TRADING-2320 承接 TRADING-2319，但当前没有 executable event gating signal 或 event gating signal series。本报告是 source-blocked validation readiness package，不读取 event rows、market data、turnover records 或 exposure records，不执行 event gating validation，不生成效果结论，不进入 paper-shadow、production 或 broker path。

- status: `EVENT_GATING_VALIDATION_SOURCE_BLOCKED_NOT_EXECUTED`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `source_blocked_static_validation_package`
- data_quality_status: `NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_VALIDATION`
- source_status: `EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL`
- source_signal_spec_status: `SOURCE_BLOCKED_INACTIVE_SPEC_ONLY`
- validation_objective_count: `3`
- blocked_objective_count: `3`
- data_requirement_count: `15`
- executable_validation_ready: `False`
- event_rows_consumed: `False`
- gating_signal_consumed: `False`
- event_gating_signal_series_consumed: `False`
- market_data_consumed: `False`
- event_gating_validation_executed: `False`
- validation_result_generated: `False`
- event_outcome_prediction_allowed: `False`
- trading_direction_prediction_allowed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Objective Readiness

|validation_objective|readiness_status|blocked_reason|
|---|---|---|
|`earnings_cluster_exposure_risk`|`SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED`|missing_issuer_event_manifest_and_cluster_exposure_records|
|`event_window_overtrading`|`SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED`|missing_event_window_turnover_records_and_gating_signal_series|
|`pre_event_false_risk_on`|`SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED`|missing_pit_event_rows_and_gating_signal_series|

## Data Requirements

|validation_objective|requirement_id|requirement_status|
|---|---|---|
|`earnings_cluster_exposure_risk`|`issuer_event_source_manifest`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`earnings_cluster_exposure_risk`|`executable_gating_signal_series`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`earnings_cluster_exposure_risk`|`earnings_cluster_exposure_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`earnings_cluster_exposure_risk`|`cluster_window_drawdown_outcomes`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`earnings_cluster_exposure_risk`|`cached_market_data_quality_report`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`event_window_overtrading`|`pit_event_rows_with_known_at`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`event_window_overtrading`|`executable_gating_signal_series`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`event_window_overtrading`|`event_window_turnover_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`event_window_overtrading`|`comparable_non_event_window_turnover_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`event_window_overtrading`|`cached_market_data_quality_report`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`pre_event_false_risk_on`|`pit_event_rows_with_known_at`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`pre_event_false_risk_on`|`executable_gating_signal_series`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`pre_event_false_risk_on`|`risk_on_add_decision_records`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`pre_event_false_risk_on`|`post_window_return_outcomes`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|
|`pre_event_false_risk_on`|`cached_market_data_quality_report`|`MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED`|

## Boundary

退出 source-blocked validation 状态的条件是补齐 provider-specific event source manifest、PIT event rows、known_at / available_at timestamp、executable gating signal series、event-window outcome / turnover / exposure records，并在读取 cached market data 的 validation workflow 中执行 `aits validate-data` 或同源 data-quality gate。当前不得把本报告解读为 event gating 有效性结论。
