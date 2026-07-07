# Dynamic strategy blocker downgrade workflow

- status：`DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY`
- automatic downgrade allowed：`False`
- owner review required for any downgrade：`True`

{
  "automatic_downgrade_allowed": false,
  "broker_action": "none",
  "downgrade_executed_in_2408": false,
  "owner_review_required_for_any_downgrade": true,
  "production_effect": "none",
  "schema_version": "dynamic_strategy_blocker_downgrade_workflow.v1",
  "steps": [
    {
      "required": true,
      "step_id": "step_1_contract_schema_exists"
    },
    {
      "required": true,
      "step_id": "step_2_input_mapping_complete"
    },
    {
      "required": true,
      "step_id": "step_3_as_of_replay_validation_passed"
    },
    {
      "required": true,
      "step_id": "step_4_pit_gate_result_regenerated"
    },
    {
      "required": true,
      "step_id": "step_5_owner_review_recorded"
    },
    {
      "allowed_only_after_owner_review": true,
      "step_id": "step_6_registry_severity_updated"
    },
    {
      "note": "candidate search may remain blocked if any blocker persists",
      "required": true,
      "step_id": "step_7_candidate_search_gate_re_evaluated"
    }
  ]
}

## Candidate search gate policy

{
  "broker_action": "none",
  "candidate_search_allowed": false,
  "candidate_search_can_be_reconsidered_only_after": [
    "both blockers downgraded from BLOCKING",
    "PIT gate regenerated",
    "owner review recorded"
  ],
  "observation_can_be_reconsidered_only_after": [
    "candidate search restored",
    "candidate retest rerun under remediated contracts",
    "observation preview candidate exists",
    "owner review recorded"
  ],
  "paper_shadow_allowed": false,
  "production_allowed": false,
  "reason": [
    "growth_tilt_engine remains BLOCKING",
    "valid_until_window remains BLOCKING"
  ],
  "schema_version": "dynamic_strategy_candidate_search_gate_policy.v1"
}
