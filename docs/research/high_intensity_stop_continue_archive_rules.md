# High-Intensity Stop Continue Archive Rules

本报告定义 forward observe 后如何评价 high-intensity trigger 的质量，以及何时继续、调整 threshold、降级 manual-review-only 或 archive。

- minimum_observe_event_count: `20`
- minimum_observe_months: `6`
- minimum_outcome_ready_count: `15`

## Metrics

- `high_intensity_event_count`
- `outcome_ready_count`
- `false_warning_count`
- `missed_stress_count`
- `missed_upside_count`
- `downside_capture_count`
- `precision_proxy`
- `recall_proxy`
- `false_warning_rate`
- `missed_stress_rate`
- `missed_upside_rate`
- `downside_capture_rate`

这些规则只用于 research-only observe line，不允许 promotion、paper-shadow、production 或 broker action。
