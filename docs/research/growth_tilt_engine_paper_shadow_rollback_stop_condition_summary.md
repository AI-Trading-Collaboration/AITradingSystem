# Growth Tilt Engine Paper Shadow Rollback Stop Condition Summary

```json
{
  "broker_action": "none",
  "enablement_gap_count": 0,
  "enablement_plan_ready": true,
  "engine_id": "growth_tilt_engine",
  "gaps": [],
  "next_route": "TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring",
  "production_effect": "none",
  "rollback_actions": [
    "keep paper-shadow runtime disabled",
    "keep scheduler disabled",
    "keep production and broker paths disabled",
    "route to enablement gap remediation when gaps exist"
  ],
  "rollback_required_now": false,
  "schema_version": "growth_tilt_engine_paper_shadow_rollback_stop_condition_summary.v1",
  "status": "GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY",
  "stop_conditions": [
    "any paper_shadow_enabled flag becomes true before owner approval",
    "paper_shadow_schedule_enabled becomes true before dry-run wiring",
    "production_enabled or broker_enabled becomes true",
    "generated_signal or generated_trading_advice becomes true",
    "source artifact chain no longer matches TRADING-2420 through TRADING-2423"
  ],
  "target_strategy_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
}
```