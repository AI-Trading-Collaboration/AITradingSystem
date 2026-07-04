# High-Intensity Scheduler Cadence And Input Contract

- scheduler_enabled_in_2344: `False`
- event_detection_frequency: `trading_day`
- outcome_update_frequency: `trading_day`
- required_input_count: `12`

Cadence plan 只定义 future observe-only scheduler 的候选运行顺序，并要求 trading-calendar gating；2344 不激活该 scheduler。