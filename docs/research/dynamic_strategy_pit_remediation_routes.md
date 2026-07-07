# Dynamic strategy PIT remediation routes

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`

{
  "record_ready": true,
  "routes": {
    "TRADING-2405_Dynamic_Strategy_PIT_Coverage_Matrix_Reusable_Implementation": {
      "default_next_route": true,
      "handles": [
        "pit_input_registry",
        "pit_matrix_generator",
        "pit_severity_gate",
        "blocker_summary"
      ],
      "purpose": "implement registry-backed PIT matrix generator and gate checker"
    },
    "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan": {
      "handles": [
        "source_features",
        "as_of correctness",
        "signal horizon",
        "signal confidence",
        "false risk-on risk"
      ],
      "purpose": "design remediation for growth_tilt_engine blocking gap"
    },
    "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan": {
      "handles": [
        "valid_from",
        "valid_until",
        "signal expiry",
        "stale signal carry-forward",
        "near-expiry decay"
      ],
      "purpose": "design remediation for valid_until_window blocking gap"
    },
    "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan": {
      "handles": [
        "risk_on expectation",
        "risk_off expectation",
        "high_volatility expectation",
        "recovery expectation"
      ],
      "purpose": "replace coarse regime pass-rate with expectation-aware scoring"
    },
    "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan": {
      "handles": [
        "candidate x gate x decision matrix",
        "owner review boundary",
        "observation preview boundary"
      ],
      "purpose": "normalize historical candidate outcomes for threshold calibration"
    }
  },
  "schema_version": "dynamic_strategy_pit_remediation_routes.v1"
}
