# Dynamic strategy blocking gap implementation sequence

- status：`DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_READY`
- next route：`TRADING-2409_Dynamic_Strategy_Signal_As_Of_And_Validity_Contract_Schema`

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
