# Dynamic strategy PIT remediation routes

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`
- next route：`TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan`

{
  "broker_action": "none",
  "production_effect": "none",
  "recommended_next_research_task": "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan",
  "route_reason": "growth_tilt_engine is the core return-engine blocking PIT gap",
  "routes": {
    "growth_tilt_engine": {
      "candidate_search_blocker": true,
      "next_task": "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan",
      "recommended_action": "Review source features, as-of semantics, signal horizon, and PIT safety before resuming candidate search.",
      "severity": "BLOCKING"
    },
    "regime_expectation_scoring": {
      "next_task": "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan",
      "severity": "MATERIAL"
    },
    "threshold_meta_dataset": {
      "next_task": "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan",
      "severity": "MATERIAL"
    },
    "valid_until_window": {
      "candidate_search_blocker": true,
      "next_task": "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan",
      "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
      "severity": "BLOCKING"
    }
  },
  "schema_version": "dynamic_strategy_pit_remediation_routes.v1"
}
