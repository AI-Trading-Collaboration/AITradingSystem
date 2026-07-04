# High-Intensity Scheduler Disabled Wiring Guardrails

- guardrail_status: `PASS`
- safety_error_count: `0`
- assertion_status: `PASS`
- real_scheduler_fields: `{'external_scheduler_entry_created': False, 'cron_entry_created': False, 'windows_task_created': False, 'github_action_schedule_created': False, 'daily_scheduler_entry_created': False, 'real_scheduler_created': False}`

Guardrails 对 real scheduler creation、event append、outcome binding、
paper-shadow、production、broker action、target weight 和 rebalance
instruction 全部 fail closed。