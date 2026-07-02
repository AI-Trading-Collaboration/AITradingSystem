# Recommended Dynamic Target Baseline Spec

- status: `DYNAMIC_TARGET_BASELINE_PREPARATION_READY_PROMOTION_BLOCKED`
- selected_dynamic_baseline_id: `None`
- selected_source_id: `None`
- pit_status: `BLOCKED`
- replayability_status: `NOT_REPLAYABLE`
- 2329_allowed: `False`
- readiness_status: `DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED`
- next_task: `TRADING-2329_Dynamic_Target_Baseline_Source_Remediation`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

若未选出 source，后续只能进入 source remediation 或 schema adapter，不能执行 dynamic baseline dry-run，也不能把 static baseline 结论外推为 dynamic strategy 结论。
