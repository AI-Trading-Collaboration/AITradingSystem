# Dynamic strategy signal as-of and validity contract schema plan

- status：`DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY`
- ready：`True`

{
  "broker_action": "none",
  "contracts": {
    "signal_as_of_contract": {
      "required_fields": [
        "signal_id",
        "signal_version",
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "source_feature_ids",
        "source_artifact_ids",
        "signal_horizon_days",
        "signal_value",
        "signal_strength_if_available",
        "confidence_if_available",
        "uncertainty_reason_if_available"
      ],
      "required_invariants": [
        "generated_at >= source_data_cutoff",
        "as_of_date <= generated_at_or_research_as_of",
        "source_features_must_have_pit_status",
        "no_forward_window_dependency_unless_explicitly_marked_not_pit_safe"
      ]
    },
    "signal_replay_validation_contract": {
      "required_checks": [
        "reconstruct_signal_by_as_of_date",
        "reconstruct_source_features_by_as_of_date",
        "reconstruct_valid_from_valid_until",
        "detect_expired_signal_execution",
        "detect_unexplained_carry_forward",
        "detect_signal_to_execution_lag",
        "compare_replay_hash_stability"
      ],
      "required_outputs": [
        "replay_validation_result",
        "as_of_replay_hash",
        "stale_signal_execution_count",
        "missing_validity_field_count",
        "forward_window_dependency_count",
        "blocker_downgrade_eligibility"
      ]
    },
    "signal_validity_contract": {
      "required_fields": [
        "signal_id",
        "signal_version",
        "valid_from",
        "valid_until",
        "stale_after",
        "horizon_days",
        "expiry_rule",
        "carry_forward_rule",
        "near_expiry_rule",
        "signal_to_execution_lag_rule"
      ],
      "required_invariants": [
        "valid_from >= generated_at_or_next_executable_time",
        "valid_until > valid_from",
        "stale_after <= valid_until",
        "expired_signal_cannot_trigger_new_trade",
        "carry_forward_requires_explicit_rule",
        "missing_valid_until_blocks_candidate_search_for_dependent_strategy"
      ]
    },
    "source_feature_traceability_contract": {
      "required_fields": [
        "feature_id",
        "feature_family",
        "source_config",
        "source_data",
        "as_of_handling",
        "generated_at_handling",
        "lookback_window",
        "forward_window_used",
        "pit_status",
        "pit_confidence",
        "risk_flags",
        "severity"
      ],
      "required_invariants": [
        "every_signal_feature_has_registry_entry",
        "forward_window_used=false_for_true_pit_features",
        "unknown_pit_feature_blocks_signal_downgrade"
      ]
    }
  },
  "implemented_in_2408": false,
  "planned_reusable_modules": [
    "src/ai_trading_system/research_quality/signal_as_of_contract.py",
    "src/ai_trading_system/research_quality/signal_validity_contract.py",
    "src/ai_trading_system/research_quality/signal_replay_validation.py",
    "src/ai_trading_system/research_quality/blocker_downgrade_policy.py"
  ],
  "production_effect": "none",
  "schema_version": "dynamic_strategy_contract_schema_plan.v1"
}
