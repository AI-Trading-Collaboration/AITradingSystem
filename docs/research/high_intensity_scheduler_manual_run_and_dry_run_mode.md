# High-Intensity Scheduler Manual Run And Dry-Run Mode

- command_candidate: `aits research trends high-intensity-risk-cap-observe-only-scheduler-run`
- allowed_modes: `['dry_run', 'validate_only']`
- blocked_modes: `['live', 'paper_shadow', 'production']`
- dry_run_only_mode_required: `True`
- event_append_executed: `False`

命令名称只是 future implementation candidate。TRADING-2346 不实现该命令；
future manual run 也只能 dry_run / validate_only，不能 live、paper_shadow 或 production。