# Dynamic strategy gate taxonomy

- status：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`
- research-only observation / paper-shadow / production-broker gates are separated.

```json
{
  "paper_shadow_gate": {
    "broker_must_remain_disabled": true,
    "event_outcome_policy_required": true,
    "explicit_owner_approval_required": true,
    "paper_shadow_enabled": false,
    "paper_shadow_in_scope": false,
    "side_effect": "creates_paper_trade_or_shadow_position",
    "stable_slice_evidence_required": true,
    "threshold_level": "high"
  },
  "production_broker_gate": {
    "broker_action_enabled": false,
    "currently_out_of_scope": true,
    "explicit_owner_approval_required": true,
    "production_enabled": false,
    "side_effect": "real_execution_or_capital_risk",
    "threshold_level": "highest"
  },
  "research_only_observation_gate": {
    "artifact_only": true,
    "auto_accept_allowed": "very_limited",
    "broker_action": false,
    "event_append": false,
    "outcome_binding": false,
    "owner_review_allowed": true,
    "paper_trade": false,
    "principle": "Research-only observation should not use paper-shadow-like thresholds because it observes without execution side effects.",
    "side_effect": "none",
    "threshold_level": "moderate"
  }
}
```
