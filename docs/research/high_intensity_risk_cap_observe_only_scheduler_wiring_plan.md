# High-Intensity Risk-Cap Observe-Only Scheduler Wiring Plan

- status: `OBSERVE_ONLY_SCHEDULER_WIRING_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`
- source_2345_status: `OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`
- selected_rule_id: `COMPOSITE_HIGH_INTENSITY_RULE`
- scheduler_enabled: `False`
- scheduler_default_enabled: `False`
- manual_run_only: `True`
- dry_run_default: `True`
- source_validate_data_as_of: `2026-06-29`
- source_validate_data_status: `PASS_WITH_WARNINGS`
- source_validate_data_error_count: `0`
- would_append_event_count: `0`
- append_reason: `DEDUP_AGAINST_EXISTING_HISTORICAL_EVENT_LOG`
- next_task: `TRADING-2347_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Disabled_Wiring_Implementation`

TRADING-2346 只生成 observe-only scheduler wiring plan，不实现 wiring，
不写入 enabled scheduler config，不 append event，不绑定 outcome，
不输出 target weight / rebalance / broker action。promotion、paper-shadow、
production 和 broker action 全部继续阻断。