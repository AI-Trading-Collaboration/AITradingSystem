# Dynamic strategy TRADING-2390 route

- source task：`TRADING-2389`
- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- recommended next route：`TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution`
- route type：`calibrated_reclassification_and_component_attribution`
- observation approved：`false`
- paper-shadow enabled：`false`
- production enabled：`false`
- broker action：`none`

```json
{
  "allowed_actions": [
    "calibrated_reclassification_preview",
    "component_level_attribution",
    "owner_review_candidate_identification"
  ],
  "candidate_reclassification_targets": [
    {
      "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "component_value_type": "current_best_reference_return_leader",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "OWNER_REVIEW_REQUIRED"
    },
    {
      "candidate_id": "dynamic_turnover_budgeted_growth_tilt_v1",
      "component_value_type": "turnover_budget_component_value",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    },
    {
      "candidate_id": "dynamic_valid_until_expiry_strict_v1",
      "component_value_type": "valid_until_component_value",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    },
    {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "component_value_type": "robustness_repair_variant",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    },
    {
      "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
      "component_value_type": "guarded_return_reference",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    }
  ],
  "forbidden_actions": [
    "observation_approval",
    "paper_shadow_enablement",
    "event_append",
    "outcome_binding",
    "scheduler_enablement",
    "production_or_broker_action"
  ],
  "purpose": [
    "apply calibrated gate preview to 2386 candidates",
    "distinguish candidate failure from component value",
    "identify candidates eligible for OWNER_REVIEW_REQUIRED",
    "recommend whether current best candidate should enter owner-review-only decision"
  ],
  "recommended_next_research_task": "TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution",
  "task_name": "Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution"
}
```
