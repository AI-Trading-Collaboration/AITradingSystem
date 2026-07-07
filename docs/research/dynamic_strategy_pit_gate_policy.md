# Dynamic strategy PIT gate policy

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`

{
  "candidate_search": {
    "blocked_if": [
      {
        "any_input_severity": "BLOCKING"
      },
      {
        "required_signal_pit_status": "UNKNOWN"
      },
      {
        "required_execution_semantic_pit_status": "UNKNOWN"
      }
    ]
  },
  "current_gate_result": {
    "candidate_search_allowed": false,
    "paper_shadow_allowed": false,
    "production_allowed": false,
    "reason": [
      "BLOCKING_GAP_GROWTH_TILT_ENGINE",
      "BLOCKING_GAP_VALID_UNTIL_WINDOW"
    ],
    "research_only_observation_allowed": false
  },
  "paper_shadow": {
    "blocked_if": [
      "any_input_severity_not_below_material",
      "observation_not_approved",
      "owner_review_not_recorded"
    ]
  },
  "production": {
    "blocked_if": [
      "always_true_for_current_phase"
    ]
  },
  "record_ready": true,
  "research_only_observation": {
    "blocked_if": [
      {
        "any_input_severity": "BLOCKING"
      },
      "any_required_signal_not_true_or_approved_approximate_pit",
      "valid_until_window_not_grounded",
      "stale_signal_rule_not_verifiable"
    ]
  },
  "schema_version": "dynamic_strategy_pit_gate_policy.v1",
  "scope": "dynamic_strategy"
}
