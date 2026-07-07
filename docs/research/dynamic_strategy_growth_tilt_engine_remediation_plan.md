# Dynamic strategy growth tilt engine remediation plan

- status：`DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_REMEDIATION_PLAN_READY`
- blocking gap resolved：`False`
- severity downgraded：`False`

{
  "broker_action": "none",
  "growth_tilt_engine_blocking_gap_resolved": false,
  "input_under_review": "growth_tilt_engine",
  "plan_items": [
    {
      "expected_result": [
        "reduce UNKNOWN PIT status",
        "make signal reproducible by as_of date"
      ],
      "goal": "every growth_tilt signal must carry explicit as_of and generated_at",
      "implemented_in_2406": false,
      "plan_id": "P0_as_of_contract",
      "priority": "P0",
      "required_fields": [
        "as_of_date",
        "generated_at",
        "source_data_cutoff",
        "signal_valid_from",
        "signal_horizon_days"
      ]
    },
    {
      "goal": "map every growth_tilt source feature to source config and PIT status",
      "implemented_in_2406": false,
      "plan_id": "P0_source_feature_traceability",
      "priority": "P0",
      "required_outputs": [
        "source_feature_inventory",
        "feature_pit_status",
        "lookahead_risk_flag",
        "revision_risk_flag"
      ]
    },
    {
      "goal": "define what period growth_tilt signal is intended to forecast",
      "implemented_in_2406": false,
      "plan_id": "P1_signal_horizon_definition",
      "priority": "P1",
      "required_decisions": [
        "horizon_days",
        "expected_decay",
        "valid_from",
        "valid_until_dependency"
      ]
    },
    {
      "goal": "separate weak growth tilt from strong growth tilt",
      "implemented_in_2406": false,
      "plan_id": "P1_signal_confidence",
      "priority": "P1",
      "required_outputs": [
        "signal_strength_score",
        "confidence_band",
        "uncertainty_reason"
      ]
    },
    {
      "goal": "reduce growth tilt false positives under high volatility or recovery traps",
      "implemented_in_2406": false,
      "plan_id": "P1_false_risk_on_guardrail",
      "priority": "P1",
      "required_outputs": [
        "high_volatility_condition",
        "trend_confirmation_condition",
        "drawdown_state_guardrail"
      ]
    },
    {
      "goal": "rerun component-level validation after remediation before candidate search",
      "implemented_in_2406": false,
      "note": "not part of 2406; candidate search remains blocked",
      "plan_id": "P2_component_revalidation",
      "priority": "P2"
    }
  ],
  "production_effect": "none",
  "recommended_implementation_task": "TRADING-2408_Growth_Tilt_Engine_PIT_Remediation_Implementation",
  "schema_version": "dynamic_strategy_growth_tilt_engine_remediation_plan.v1"
}

## Severity downgrade conditions

{
  "broker_action": "none",
  "downgrade_executed_in_2406": false,
  "downgrade_from_BLOCKING_to_MATERIAL_requires": [
    "source_feature_inventory_complete",
    "no_known_lookahead_risk_in_required_features",
    "as_of_date_available",
    "generated_at_or_source_cutoff_available",
    "signal_horizon_defined",
    "signal_valid_from_defined",
    "no_unexplained_future_window_dependency",
    "owner_review_recorded"
  ],
  "downgrade_from_MATERIAL_to_APPROVED_APPROXIMATE_PIT_requires": [
    "approximate_PIT_caveats_explicitly_documented",
    "validation_replay_can_reconstruct_signal_as_of",
    "stale_signal_behavior_defined",
    "false_risk_on_risk_reviewed",
    "owner_approval_recorded"
  ],
  "input_under_review": "growth_tilt_engine",
  "mark_TRUE_PIT_requires": [
    "all_source_features_true_PIT",
    "no_revision_or_backfill_dependency",
    "deterministic_as_of_replay",
    "validation_test_coverage"
  ],
  "production_effect": "none",
  "schema_version": "dynamic_strategy_growth_tilt_engine_severity_downgrade_conditions.v1"
}
