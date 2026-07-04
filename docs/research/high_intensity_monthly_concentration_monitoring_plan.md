# High-Intensity Monthly Concentration Monitoring Plan

- monitoring_required: `True`
- inherited_warning: `MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`
- monitoring_status: `MONITORING_REQUIRED_WITH_STRICT_GUARDRAILS`
- guardrails: `{'max_monthly_event_count': 3, 'max_monthly_cluster_count': 3, 'max_consecutive_trigger_days': 5}`

Monthly concentration warning 不阻断继续 observe，但必须成为 2342 runtime integration plan 的 hard monitoring field。
