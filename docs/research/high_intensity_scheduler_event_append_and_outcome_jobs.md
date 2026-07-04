# High-Intensity Scheduler Event Append And Outcome Jobs

- append_mode: `append_only`
- cluster_update_mode: `append_or_extend_open_cluster`
- pending_registry_update_mode: `append_only`
- outcome_update_requires_validate_data: `True`

Event append / cluster / pending outcome jobs 只定义 future observe-only increments；actual-path outcome update job 只是 future candidate，启用时必须先运行 canonical validate-data。