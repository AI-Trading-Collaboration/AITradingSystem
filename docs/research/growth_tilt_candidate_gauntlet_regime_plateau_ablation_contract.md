# Growth Tilt Candidate Gauntlet Regime Plateau Ablation Contract

```json
{
  "ablation_output": {
    "outputs": [
      "full_candidate",
      "without_false_risk_off_filter",
      "without_missed_upside_reentry",
      "without_turnover_cooldown",
      "without_valid_until_guard"
    ],
    "ready": true
  },
  "ablation_output_ready": true,
  "broker_action": "none",
  "computed_in_2432": false,
  "parameter_plateau_check": {
    "dimensions": [
      "trend_window",
      "risk_off_threshold",
      "re_risk_speed",
      "turnover_cooldown",
      "valid_until_days"
    ],
    "ready": true,
    "threshold_source": "future_screen_policy_required",
    "threshold_value": null
  },
  "parameter_plateau_check_ready": true,
  "production_effect": "none",
  "regime_slice_check": {
    "ready": true,
    "slices": [
      "ai_after_chatgpt_full_window",
      "risk_off_drawdown_windows",
      "growth_recovery_windows",
      "sideways_whipsaw_windows"
    ]
  },
  "regime_slices_ready": true,
  "schema_version": "growth_tilt_candidate_gauntlet_regime_plateau_ablation_contract.v1",
  "status": "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
}
```
