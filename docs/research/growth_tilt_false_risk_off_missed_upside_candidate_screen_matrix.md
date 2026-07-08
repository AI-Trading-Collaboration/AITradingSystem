# Growth Tilt False Risk-Off Missed Upside Candidate Screen Matrix

```json
{
  "broker_action": "none",
  "candidate_count": 6,
  "candidate_screen_matrix_ready": true,
  "candidates": [
    {
      "batch_decision": "component_value",
      "broker_action": "none",
      "candidate_family": "defensive_limited_adjustment",
      "candidate_id": "defensive_limited_adjustment_false_risk_off_reducer",
      "computed_new_metrics": false,
      "decision_rationale": "Prior evidence suggests defensive limited adjustment may reduce over-defensive behavior, but it still needs component validation.",
      "market_data_metrics_available": false,
      "market_data_screen_run": false,
      "next_validation_route": "TRADING-2434_Defensive_Limited_Adjustment_Component_Validation",
      "paper_shadow_enabled": false,
      "production_effect": "none",
      "production_enabled": false,
      "research_questions": [
        "over_defensive_entry",
        "false_defensive_day_reduction",
        "missed_upside_without_drawdown_damage"
      ],
      "threshold_source": "future_pit_or_component_validation_policy_required",
      "threshold_value": null
    },
    {
      "batch_decision": "pit_candidate",
      "broker_action": "none",
      "candidate_family": "recovery_reentry",
      "candidate_id": "recovery_reentry_speedup_guard",
      "computed_new_metrics": false,
      "decision_rationale": "Re-entry speed directly targets missed upside and needs PIT replay before any owner decision.",
      "market_data_metrics_available": false,
      "market_data_screen_run": false,
      "next_validation_route": "TRADING-2438_Growth_Tilt_Top-3_Candidate_PIT_Replay",
      "paper_shadow_enabled": false,
      "production_effect": "none",
      "production_enabled": false,
      "research_questions": [
        "slow_growth_recovery_reentry",
        "missed_upside_without_drawdown_damage"
      ],
      "threshold_source": "future_pit_or_component_validation_policy_required",
      "threshold_value": null
    },
    {
      "batch_decision": "pit_candidate",
      "broker_action": "none",
      "candidate_family": "false_risk_off_filter",
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "computed_new_metrics": false,
      "decision_rationale": "Relaxing confirmation may reduce false risk-off but must prove drawdown protection is preserved.",
      "market_data_metrics_available": false,
      "market_data_screen_run": false,
      "next_validation_route": "TRADING-2438_Growth_Tilt_Top-3_Candidate_PIT_Replay",
      "paper_shadow_enabled": false,
      "production_effect": "none",
      "production_enabled": false,
      "research_questions": [
        "over_defensive_entry",
        "false_defensive_day_reduction"
      ],
      "threshold_source": "future_pit_or_component_validation_policy_required",
      "threshold_value": null
    },
    {
      "batch_decision": "pit_candidate",
      "broker_action": "none",
      "candidate_family": "missed_upside_reentry",
      "candidate_id": "missed_upside_reentry_accelerator",
      "computed_new_metrics": false,
      "decision_rationale": "Accelerator hypothesis is directly aligned with missed upside but lacks PIT evidence.",
      "market_data_metrics_available": false,
      "market_data_screen_run": false,
      "next_validation_route": "TRADING-2438_Growth_Tilt_Top-3_Candidate_PIT_Replay",
      "paper_shadow_enabled": false,
      "production_effect": "none",
      "production_enabled": false,
      "research_questions": [
        "slow_growth_recovery_reentry",
        "missed_upside_without_drawdown_damage"
      ],
      "threshold_source": "future_pit_or_component_validation_policy_required",
      "threshold_value": null
    },
    {
      "batch_decision": "component_value",
      "broker_action": "none",
      "candidate_family": "turnover_cooldown",
      "candidate_id": "turnover_cooldown_false_risk_off_balancer",
      "computed_new_metrics": false,
      "decision_rationale": "Existing lower-turnover evidence supports component-level value, not paper-shadow promotion.",
      "market_data_metrics_available": false,
      "market_data_screen_run": false,
      "next_validation_route": "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study",
      "paper_shadow_enabled": false,
      "production_effect": "none",
      "production_enabled": false,
      "research_questions": [
        "false_defensive_day_reduction",
        "missed_upside_without_drawdown_damage"
      ],
      "threshold_source": "future_pit_or_component_validation_policy_required",
      "threshold_value": null
    },
    {
      "batch_decision": "component_value",
      "broker_action": "none",
      "candidate_family": "valid_until_strictness",
      "candidate_id": "stale_defensive_valid_until_tightener",
      "computed_new_metrics": false,
      "decision_rationale": "Existing valid-until strictness evidence supports stale defensive signal reduction as component value.",
      "market_data_metrics_available": false,
      "market_data_screen_run": false,
      "next_validation_route": "TRADING-2435_Growth_Tilt_Valid_Until_Outcome_Hit_Rate_Study",
      "paper_shadow_enabled": false,
      "production_effect": "none",
      "production_enabled": false,
      "research_questions": [
        "over_defensive_entry",
        "false_defensive_day_reduction"
      ],
      "threshold_source": "future_pit_or_component_validation_policy_required",
      "threshold_value": null
    }
  ],
  "computed_new_metrics": false,
  "market_data_candidate_screen_run": false,
  "production_effect": "none",
  "schema_version": "growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix.v1",
  "status": "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY"
}
```
