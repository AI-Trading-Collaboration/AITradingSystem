# Dynamic strategy observation non-approval record

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY`
- research-only observation approved：`False`
- paper-shadow enabled：`False`
- event append enabled：`False`
- outcome binding enabled：`False`
- scheduler enabled：`False`
- production enabled：`False`
- broker action enabled：`False`
- daily report generated：`False`

```json
{
  "broker_action_enabled": false,
  "candidate_auto_accept_approved": false,
  "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "component_attribution_continue_recommended": true,
  "daily_report_generated": false,
  "event_append_approved": false,
  "event_append_enabled": false,
  "non_approval_reasons": [
    "owner approval for observation was not explicitly granted",
    "calibrated preview still carries time/regime instability",
    "drawdown materiality prevents automatic acceptance",
    "paper-shadow and execution gates remain separate and closed"
  ],
  "outcome_binding_approved": false,
  "outcome_binding_enabled": false,
  "owner_decision": "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_COMPONENT_ATTRIBUTION",
  "owner_review_required_retained": true,
  "paper_shadow_approved": false,
  "paper_shadow_enabled": false,
  "paper_trade_created": false,
  "production_enabled": false,
  "record_ready": true,
  "research_only_observation_approved": false,
  "scheduler_enabled": false,
  "shadow_position_created": false
}
```