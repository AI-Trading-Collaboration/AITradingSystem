# High-Intensity Scheduler Smoke Dry-Run Guardrails

- guardrail_status: `PASS`
- guardrail_assertions_passed: `True`
- side_effect_status: `PASS`
- side_effect_assertions_passed: `True`
- safety_error_count: `0`
- side_effect_violation_count: `0`

Guardrails 对 real scheduler creation、cron、Windows Task、GitHub
Actions schedule、event append、outcome binding、paper-shadow、
production、broker action、target weight 和 rebalance instruction
全部 fail closed。