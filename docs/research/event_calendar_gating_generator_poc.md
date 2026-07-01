# Event Calendar Gating Generator POC

TRADING-2319 承接 TRADING-2318，但当前没有 PIT-ready event source。本报告是 source-blocked generator POC package，不下载 event rows，不生成 gating signal，不预测事件结果或交易方向，不进入仓位、paper-shadow、production 或 broker path。

- status: `EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `source_blocked_static_generator_poc`
- data_quality_status: `NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_GENERATOR`
- source_status: `EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY`
- source_pit_ready_source_count: `0`
- source_blocker_count: `8`
- use_case_readiness_count: `4`
- blocked_use_case_count: `4`
- executable_generator_ready: `False`
- signal_spec_status: `SOURCE_BLOCKED_INACTIVE_SPEC_ONLY`
- event_rows_consumed: `False`
- gating_signal_generated: `False`
- event_gating_signal_series_generated: `False`
- event_outcome_prediction_allowed: `False`
- trading_direction_prediction_allowed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Use-Case Readiness

|use_case_id|readiness_status|blocked_event_families|
|---|---|---|
|`earnings_cluster_risk`|`SOURCE_BLOCKED_NO_GENERATOR`|NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS,TSM_MONTHLY_REVENUE,SEMICONDUCTOR_EARNINGS_WINDOW|
|`manual_review_trigger`|`SOURCE_BLOCKED_NO_GENERATOR`|FOMC,CPI,PCE,NFP,NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS,TSM_MONTHLY_REVENUE|
|`post_event_confirmation_window`|`SOURCE_BLOCKED_NO_GENERATOR`|FOMC,CPI,PCE,NFP,NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS|
|`pre_event_no_add`|`SOURCE_BLOCKED_NO_GENERATOR`|FOMC,CPI,PCE,NFP,NVDA_EARNINGS,AI_MEGA_CAP_EARNINGS|

## Source Blockers

|source_id|event_family|pit_status|blocker_status|
|---|---|---|---|
|`ai_mega_cap_earnings_calendar`|`AI_MEGA_CAP_EARNINGS`|`BLOCKED_PENDING_AS_KNOWN_BEFORE_EARNINGS_SOURCE`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`cpi_release_calendar`|`CPI`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`fomc_calendar`|`FOMC`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`nfp_release_calendar`|`NFP`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`nvda_earnings_calendar`|`NVDA_EARNINGS`|`BLOCKED_PENDING_AS_KNOWN_BEFORE_EARNINGS_SOURCE`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`pce_release_calendar`|`PCE`|`KNOWN_AT_POLICY_REQUIRED_BEFORE_GENERATOR`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`semiconductor_earnings_window`|`SEMICONDUCTOR_EARNINGS_WINDOW`|`BLOCKED_PENDING_UNIVERSE_AND_AS_KNOWN_SOURCE`|`SOURCE_BLOCKED_NO_GENERATOR`|
|`tsm_monthly_revenue_calendar`|`TSM_MONTHLY_REVENUE`|`BLOCKED_PENDING_RELEASE_TIME_ARCHIVE`|`SOURCE_BLOCKED_NO_GENERATOR`|

## Boundary

退出 source-blocked 状态的条件是补齐 provider-specific source manifest、event row schema、known_at / available_at timestamp、row count 和 checksum，然后重新运行 TRADING-2318 和 TRADING-2319。当前不得把 inactive spec 接入 no-add、manual review、post-event confirmation、scoring、report、paper-shadow、production 或 broker workflow。
