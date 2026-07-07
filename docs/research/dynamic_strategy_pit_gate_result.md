# Dynamic strategy PIT gate result

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`
- candidate search allowed：`False`
- research-only observation allowed：`False`
- paper-shadow allowed：`False`
- production allowed：`False`

{
  "blockers": [
    "BLOCKING_GAP_GROWTH_TILT_ENGINE",
    "BLOCKING_GAP_VALID_UNTIL_WINDOW"
  ],
  "broker_action": "none",
  "candidate_search": {
    "allowed": false,
    "blocked_if": [
      "any_required_input_severity_BLOCKING",
      "core_return_signal_pit_status_UNKNOWN_or_NOT_PIT_SAFE",
      "execution_validity_semantic_pit_status_UNKNOWN_or_NOT_PIT_SAFE"
    ],
    "reasons": [
      "BLOCKING_GAP_GROWTH_TILT_ENGINE",
      "BLOCKING_GAP_VALID_UNTIL_WINDOW",
      "REQUIRED_SIGNAL_PIT_STATUS_UNKNOWN_OR_NOT_SAFE",
      "REQUIRED_EXECUTION_SEMANTIC_PIT_STATUS_UNKNOWN_OR_NOT_SAFE"
    ]
  },
  "candidate_search_allowed": false,
  "gate_derivation_sources": {
    "empirical_status": [
      "policy_derived_safety_gate",
      "not_statistically_calibrated_yet",
      "threshold_meta_dataset_required_for_future_calibration"
    ],
    "phase_based": [
      "candidate search allows limited approximate PIT but not blocking core inputs",
      "research-only observation requires true or owner-approved approximate PIT",
      "paper-shadow requires stronger evidence",
      "production remains blocked"
    ],
    "principle_based": [
      "no lookahead",
      "no future outcome dependency",
      "no stale signal carry-forward without explicit rule"
    ],
    "role_based": [
      "core return signal has stricter threshold",
      "execution semantic has stricter threshold",
      "regime label affects evaluation but not necessarily signal generation",
      "reporting input has lower severity"
    ]
  },
  "paper_shadow": {
    "allowed": false,
    "blocked_if": [
      "research_only_observation_not_approved",
      "any_material_or_blocking_pit_gap",
      "owner_review_not_recorded"
    ],
    "reasons": [
      "RESEARCH_ONLY_OBSERVATION_NOT_APPROVED",
      "ANY_MATERIAL_OR_BLOCKING_PIT_GAP",
      "OWNER_REVIEW_NOT_RECORDED"
    ]
  },
  "paper_shadow_allowed": false,
  "policy_note": "PIT gate is a policy-derived safety gate, not a statistically calibrated empirical threshold.",
  "production": {
    "allowed": false,
    "blocked_if": [
      "current_phase_production_disabled"
    ],
    "reasons": [
      "CURRENT_PHASE_PRODUCTION_DISABLED"
    ]
  },
  "production_allowed": false,
  "production_effect": "none",
  "research_only_observation": {
    "allowed": false,
    "blocked_if": [
      "any_required_input_severity_BLOCKING",
      "valid_until_window_not_grounded",
      "stale_signal_rule_not_verifiable",
      "core_return_signal_not_true_or_owner_approved_approximate_pit"
    ],
    "reasons": [
      "BLOCKING_GAP_GROWTH_TILT_ENGINE",
      "BLOCKING_GAP_VALID_UNTIL_WINDOW",
      "REQUIRED_SIGNAL_PIT_STATUS_UNKNOWN_OR_NOT_SAFE",
      "REQUIRED_EXECUTION_SEMANTIC_PIT_STATUS_UNKNOWN_OR_NOT_SAFE",
      "VALID_UNTIL_WINDOW_NOT_GROUNDED",
      "STALE_SIGNAL_RULE_NOT_VERIFIABLE",
      "CORE_RETURN_SIGNAL_NOT_TRUE_OR_OWNER_APPROVED_APPROXIMATE_PIT"
    ]
  },
  "research_only_observation_allowed": false,
  "schema_version": "dynamic_strategy_pit_gate_result.v1",
  "scope": "dynamic_strategy"
}
