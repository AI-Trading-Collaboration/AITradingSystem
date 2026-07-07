# Dynamic strategy blocking gap remediation implementation plan

## Executive summary

- status：`DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY`
- blocking gaps：`['growth_tilt_engine', 'valid_until_window']`
- unified remediation architecture ready：`True`
- contract schema plan ready：`True`
- implementation sequence ready：`True`
- blocker downgrade workflow ready：`True`
- candidate search gate policy ready：`True`
- growth_tilt_engine blocker resolved：`False`
- valid_until_window blocker resolved：`False`
- any blocker severity downgraded：`False`
- automatic downgrade allowed：`False`
- owner review required for any downgrade：`True`
- candidate search allowed：`False`
- research-only observation allowed：`False`
- paper-shadow allowed：`False`
- production allowed：`False`
- next route：`TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema`
- data quality gate：not run；reason=`NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_REGISTRY_ONLY_NO_FRESH_MARKET_DATA`

## Source findings from TRADING-2405 / 2406 / 2407

- source validation errors：`[]`

## Current blocking gaps

{
  "growth_tilt_engine": {
    "blocker_reason": [
      "source feature PIT safety not fully established",
      "as-of semantics incomplete",
      "signal horizon not fully grounded",
      "signal confidence / false risk-on risk not validated"
    ],
    "downgraded": false,
    "input_type": "SIGNAL",
    "pit_confidence": "LOW",
    "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
    "resolved": false,
    "severity": "BLOCKING"
  },
  "valid_until_window": {
    "blocker_reason": [
      "valid_from / valid_until semantics not fully grounded",
      "stale signal carry-forward contract incomplete",
      "signal-to-execution lag contract incomplete",
      "horizon-to-valid-until mapping not validated"
    ],
    "downgraded": false,
    "input_type": "EXECUTION_SEMANTIC",
    "pit_confidence": "LOW",
    "pit_status": "UNKNOWN_OR_APPROXIMATE_PIT",
    "resolved": false,
    "severity": "BLOCKING"
  }
}

## Unified remediation architecture

{
  "broker_action": "none",
  "layers": [
    {
      "also_supports": [
        "valid_until_window"
      ],
      "layer_id": "layer_1_signal_as_of_contract",
      "primarily_remediates": [
        "growth_tilt_engine"
      ],
      "purpose": "make every dynamic strategy signal reproducible by as-of date"
    },
    {
      "also_supports": [],
      "layer_id": "layer_2_source_feature_traceability",
      "primarily_remediates": [
        "growth_tilt_engine"
      ],
      "purpose": "map every signal input to source config/artifact/PIT status"
    },
    {
      "also_supports": [
        "growth_tilt_engine"
      ],
      "layer_id": "layer_3_signal_validity_contract",
      "primarily_remediates": [
        "valid_until_window"
      ],
      "purpose": "define valid_from, valid_until, stale_after, expiry_rule"
    },
    {
      "also_supports": [
        "growth_tilt_engine"
      ],
      "layer_id": "layer_4_stale_signal_and_execution_lag_contract",
      "primarily_remediates": [
        "valid_until_window"
      ],
      "purpose": "prevent expired signals from influencing future execution"
    },
    {
      "layer_id": "layer_5_as_of_replay_validation",
      "purpose": "validate deterministic reconstruction of signals and validity windows",
      "remediates": [
        "growth_tilt_engine",
        "valid_until_window"
      ]
    },
    {
      "layer_id": "layer_6_pit_gate_downgrade_workflow",
      "purpose": "allow severity downgrade only after evidence chain exists",
      "remediates": [
        "blocker_governance"
      ]
    }
  ],
  "production_effect": "none",
  "schema_version": "dynamic_strategy_unified_remediation_architecture.v1"
}

## Required contract schemas

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

## Implementation sequence

{
  "broker_action": "none",
  "phases": [
    {
      "depends_on": [
        "TRADING-2408"
      ],
      "does_not": [
        "replay validate",
        "downgrade blockers",
        "resume candidate search"
      ],
      "goal": "implement reusable schemas for signal as-of and validity contracts",
      "handles": [
        "signal_as_of_contract",
        "source_feature_traceability_contract",
        "signal_validity_contract"
      ],
      "parallelizable": false,
      "phase": 1,
      "task_id": "TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema"
    },
    {
      "depends_on": [
        "TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema"
      ],
      "does_not": [
        "mark TRUE_PIT",
        "downgrade blocker"
      ],
      "goal": "map growth_tilt_engine source features to as-of contract and traceability registry",
      "handles": [
        "source_feature_inventory",
        "feature PIT status",
        "signal horizon draft",
        "risk flag mapping"
      ],
      "parallelizable_after_2409": true,
      "phase": 2,
      "task_id": "TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping"
    },
    {
      "depends_on": [
        "TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema"
      ],
      "does_not": [
        "clear blocker",
        "resume candidate search"
      ],
      "goal": "map valid_until_window to signal validity contract",
      "handles": [
        "valid_from",
        "valid_until",
        "stale_after",
        "expiry_rule",
        "carry_forward_rule",
        "signal-to-execution lag"
      ],
      "parallelizable_after_2409": true,
      "phase": 3,
      "task_id": "TRADING-2411_Valid_Until_Window_Signal_Validity_Contract_Mapping"
    },
    {
      "depends_on": [
        "TRADING-2410_Growth_Tilt_Engine_Source_Feature_Contract_Mapping",
        "TRADING-2411_Valid_Until_Window_Signal_Validity_Contract_Mapping"
      ],
      "does_not": [
        "downgrade blocker automatically"
      ],
      "goal": "dry-run replay validation for growth_tilt_engine and valid_until_window",
      "handles": [
        "reconstruct signals by as_of",
        "reconstruct validity windows",
        "detect stale signal execution",
        "produce blocker downgrade evidence"
      ],
      "parallelizable": false,
      "phase": 4,
      "task_id": "TRADING-2412_Dynamic_Strategy_As_Of_Signal_Replay_Validation_Dry_Run"
    },
    {
      "depends_on": [
        "TRADING-2412_Dynamic_Strategy_As_Of_Signal_Replay_Validation_Dry_Run"
      ],
      "does_not": [
        "approve observation",
        "approve paper-shadow"
      ],
      "goal": "owner review of evidence chain before any severity downgrade",
      "handles": [
        "growth_tilt_engine downgrade review",
        "valid_until_window downgrade review",
        "candidate search gate review"
      ],
      "parallelizable": false,
      "phase": 5,
      "task_id": "TRADING-2413_Dynamic_Strategy_PIT_Blocker_Downgrade_Owner_Review"
    }
  ],
  "production_effect": "none",
  "recommended_immediate_next_task": "TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema",
  "schema_version": "dynamic_strategy_blocking_gap_implementation_sequence.v1"
}

## Blocker downgrade workflow

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

## Candidate search gate policy during remediation

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

## Explicit non-approval list

- `clear_growth_tilt_engine_blocking_gap`
- `clear_valid_until_window_blocking_gap`
- `downgrade_any_blocking_gap`
- `mark_any_blocker_true_pit`
- `candidate_search_resume`
- `candidate_auto_accept`
- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker_order`
- `new_strategy_backtest`
- `new_trading_signal`
- `new_scoring`

## Recommended next route

- next task：`TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema`
- reason：`A unified signal as-of / validity schema must exist before growth_tilt_engine or valid_until_window mappings diverge; replay validation and PIT gate downgrade evidence depend on shared fields.`
