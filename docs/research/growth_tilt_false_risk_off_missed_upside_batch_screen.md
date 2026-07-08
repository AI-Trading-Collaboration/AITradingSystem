# Growth Tilt False Risk-Off Missed Upside Batch Screen

## 摘要

- task_id：`TRADING-2433`
- status：`GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY`
- candidate set id：`false_risk_off_missed_upside_2433`
- candidate count：`6`
- promotion candidate count：`0`
- next route：`TRADING-2434_Defensive_Limited_Adjustment_Component_Validation`

TRADING-2433 只按 governed candidate-set 做 research-only candidate triage，不读取 fresh market data，不运行 historical screen、PIT replay、backtest 或 scoring。默认不批准 paper-shadow、schedule、production 或 broker。

## 摘要 JSON

```json
{
  "candidate_batch_screen_run": true,
  "candidate_count": 6,
  "candidate_set_id": "false_risk_off_missed_upside_2433",
  "component_value_count": 3,
  "market_data_candidate_screen_run": false,
  "next_route": "TRADING-2434_Defensive_Limited_Adjustment_Component_Validation",
  "pit_candidate_count": 3,
  "promotion_candidate_count": 0,
  "rejected_count": 0,
  "status": "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY"
}
```

## Candidate Screen Matrix

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

## Research Question Coverage

```json
{
  "broker_action": "none",
  "coverage": [
    {
      "candidate_ids": [
        "defensive_limited_adjustment_false_risk_off_reducer",
        "false_risk_off_confirmation_relaxation",
        "stale_defensive_valid_until_tightener"
      ],
      "covered": true,
      "research_question_id": "over_defensive_entry"
    },
    {
      "candidate_ids": [
        "recovery_reentry_speedup_guard",
        "missed_upside_reentry_accelerator"
      ],
      "covered": true,
      "research_question_id": "slow_growth_recovery_reentry"
    },
    {
      "candidate_ids": [
        "defensive_limited_adjustment_false_risk_off_reducer",
        "false_risk_off_confirmation_relaxation",
        "turnover_cooldown_false_risk_off_balancer",
        "stale_defensive_valid_until_tightener"
      ],
      "covered": true,
      "research_question_id": "false_defensive_day_reduction"
    },
    {
      "candidate_ids": [
        "defensive_limited_adjustment_false_risk_off_reducer",
        "recovery_reentry_speedup_guard",
        "missed_upside_reentry_accelerator",
        "turnover_cooldown_false_risk_off_balancer"
      ],
      "covered": true,
      "research_question_id": "missed_upside_without_drawdown_damage"
    }
  ],
  "covered_count": 4,
  "production_effect": "none",
  "research_question_count": 4,
  "research_question_coverage_ready": true,
  "schema_version": "growth_tilt_false_risk_off_missed_upside_research_question_coverage.v1",
  "status": "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY"
}
```

## No-Effect Boundary

```json
{
  "actionable_allocation_generated": false,
  "automatic_execution_allowed": false,
  "backtest_run": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "candidate_batch_screen_run": true,
  "computed_new_metrics": false,
  "daily_report_run": false,
  "fresh_market_data_read": false,
  "gaps": [],
  "generated_signal": false,
  "generated_trading_advice": false,
  "historical_screen_run": false,
  "market_data_candidate_screen_run": false,
  "market_data_experiment_run": false,
  "new_signal_generated": false,
  "no_effect_boundary_ready": true,
  "outcome_backfilled": false,
  "outcome_binding_executed": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "pit_replay_run": false,
  "portfolio_weight_mutated": false,
  "production_effect": "none",
  "production_enabled": false,
  "scheduled_task_created": false,
  "scheduler_enabled": false,
  "schema_version": "growth_tilt_false_risk_off_missed_upside_no_effect_boundary.v1",
  "scoring_run": false,
  "screen_contract_gap_count": 0,
  "status": "GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY",
  "trading_advice_generated": false
}
```
