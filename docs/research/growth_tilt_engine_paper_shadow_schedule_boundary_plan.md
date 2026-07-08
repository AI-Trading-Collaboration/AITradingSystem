# Growth Tilt Engine Paper Shadow Schedule Boundary Plan

```json
{
  "allowed_future_schedule_actions": [
    "paper_shadow_schedule_dry_run",
    "schedule_artifact_contract_review"
  ],
  "blocked_runtime_actions": [
    "create_cron_or_windows_task",
    "append_historical_event_log",
    "run_paper_shadow_daily_job",
    "bind_outcome_store"
  ],
  "broker_action": "none",
  "dry_run_schedule_planning_allowed": true,
  "engine_id": "growth_tilt_engine",
  "next_route": "TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring",
  "paper_shadow_daily_job_enabled": false,
  "paper_shadow_daily_job_run": false,
  "paper_shadow_schedule_enabled": false,
  "production_effect": "none",
  "schedule_creation_allowed": false,
  "scheduled_task_created": false,
  "scheduler_enabled": false,
  "schema_version": "growth_tilt_engine_paper_shadow_schedule_boundary_plan.v1",
  "status": "GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY",
  "target_strategy_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
}
```