# Calendar Event PIT Contract

- 状态：`CALENDAR_EVENT_PIT_CONTRACT_READY`

必需字段：

- `event_date`
- `scheduled_release_time`
- `source_published_at`
- `known_at`
- `available_at`
- `timezone`
- `revision_policy`

如果无法确认 `source_published_at`，事件必须标记 `PIT_WARNING` 且 `diagnostic_only`。
