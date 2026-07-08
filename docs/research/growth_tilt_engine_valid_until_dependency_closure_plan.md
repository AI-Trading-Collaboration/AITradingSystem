# Growth tilt engine valid-until dependency closure plan

- status：`GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY`
- valid_until_window blocker count：`1`

```json
{
  "broker_action": "none",
  "dependent_feature_ids": [
    "execution_signal_validity_policy"
  ],
  "dependent_feature_or_signal_count": 1,
  "pit_input_registry_candidate_search_blocker": true,
  "pit_input_registry_severity": "BLOCKING",
  "production_effect": "none",
  "recommended_later_task": "TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure",
  "requires_signal_validity_contract_evidence": true,
  "requires_stale_signal_policy_evidence": true,
  "requires_valid_from_valid_until_mapping": true,
  "schema_version": "growth_tilt_engine_valid_until_dependency_closure_plan.v1",
  "valid_until_window_still_blocking": true
}
```