# High-Intensity Observe Event Clustering and Dedup

TRADING-2336 对同一 `selected_rule_id + target_asset` 的连续触发按 `3` 个 calendar days 内 continuation 合并为同一 event cluster；每个 cluster 只生成一个 primary observe event。

- monthly_event_guardrail: `3`
- monthly_concentration_status: `PASS_WITH_WARNINGS`
- monthly_concentration_warnings: `['MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL']`

聚类和去重是后续 actual-path review 的样本控制，不是信号有效性证明。
