# High-Intensity Observe Event Schema Usage

`high_intensity_observe_trigger_day_log` 保存 selected rule 命中的日级/资产级记录；`high_intensity_observe_event_log` 只保存事件簇的primary observe event。所有事件初始 `event_status=OBSERVE_PENDING`。

核心字段包括 `event_id`、`event_cluster_id`、`event_date`、`target_asset`、`selected_rule_id`、`risk_cap_score`、`manual_review_observation_flag`、`monthly_event_count` 和 `consecutive_trigger_days`。

这些字段只能用于 research-only forward observe 和 manual review context，不能解释为 target weight、rebalance instruction、paper-shadow 或 production signal。
