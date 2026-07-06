# Dynamic strategy calibrated gate non-approval record

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- candidate auto-accept approved：`false`
- current best candidate observation approved：`false`
- paper-shadow approved：`false`
- event append / outcome binding approved：`false`
- scheduler / production / broker approved：`false`

```json
{
  "broker_action_approved": false,
  "candidate_auto_accept_approved": false,
  "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "current_best_candidate_decision": "CONTINUE_OPTIMIZATION",
  "current_best_candidate_observation_approved": false,
  "daily_report_approved": false,
  "event_append_approved": false,
  "explicit_non_approval_list": [
    "research_only_observation_for_candidate",
    "candidate_auto_accept",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "scheduled_task",
    "daily_report",
    "production",
    "broker",
    "order"
  ],
  "outcome_binding_approved": false,
  "owner_decision": "ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL",
  "paper_shadow_approved": false,
  "production_approved": false,
  "reason": [
    "2389 adopts methodology and records owner decision only",
    "candidate approval requires a separate calibrated reclassification step",
    "paper-shadow / event / outcome / scheduler paths remain out of scope"
  ],
  "research_only_observation_approved": false,
  "scheduler_approved": false
}
```
