# Growth Tilt Candidate Gauntlet Harness

## 摘要

- task_id：`TRADING-2432`
- status：`GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY`
- candidate set id：`growth_tilt_batch_2432`
- candidates tested：`0`
- harness ready：`True`
- next route：`TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen`

TRADING-2432 只建立 batch gauntlet harness contract，不执行真实 candidate batch screen、historical screen、PIT replay、backtest 或 scoring。默认不批准 paper-shadow、schedule、production 或 broker。

## 摘要 JSON

```json
{
  "ablation_output_ready": true,
  "baseline_ready": true,
  "candidate_set_id": "growth_tilt_batch_2432",
  "candidates_tested": 0,
  "harness_ready": true,
  "kill_criteria_ready": true,
  "metrics_ready": true,
  "next_route": "TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen",
  "parameter_plateau_check_ready": true,
  "promotion_criteria_ready": true,
  "regime_slices_ready": true,
  "status": "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
}
```

## Candidate Set Snapshot

```json
{
  "broker_action": "none",
  "candidate_group_count": 6,
  "candidate_groups": [
    {
      "candidate_family": "defensive_limited_adjustment",
      "candidate_group_id": "defensive_limited_adjustment",
      "default_2431_status": "component_value",
      "execution_status": "not_executed_in_2432",
      "included_in_2432_harness": true,
      "source_candidate_ids": [
        "defensive_limited_adjustment"
      ]
    },
    {
      "candidate_family": "lower_turnover_guardrail",
      "candidate_group_id": "lower_turnover_variants",
      "default_2431_status": "component_value",
      "execution_status": "not_executed_in_2432",
      "included_in_2432_harness": true,
      "source_candidate_ids": [
        "dynamic_regime_overlay_v0_4_lower_turnover",
        "dynamic_regime_growth_tilt_lower_turnover_fusion_v1",
        "equal_risk_growth_tilt_lower_turnover_guarded_v1",
        "growth_tilt_lower_turnover_guarded_transfer_v1"
      ]
    },
    {
      "candidate_family": "valid_until_strictness",
      "candidate_group_id": "dynamic_valid_until_expiry_strict_v1",
      "default_2431_status": "component_value",
      "execution_status": "not_executed_in_2432",
      "included_in_2432_harness": true,
      "source_candidate_ids": [
        "dynamic_valid_until_expiry_strict_v1"
      ]
    },
    {
      "candidate_family": "turnover_budgeting",
      "candidate_group_id": "dynamic_turnover_budgeted_growth_tilt_v1",
      "default_2431_status": "component_value",
      "execution_status": "not_executed_in_2432",
      "included_in_2432_harness": true,
      "source_candidate_ids": [
        "dynamic_turnover_budgeted_growth_tilt_v1"
      ]
    },
    {
      "candidate_family": "vol_target_growth_tilt",
      "candidate_group_id": "equal_risk_growth_tilt_vol_target_variants",
      "default_2431_status": "needs_pit",
      "execution_status": "not_executed_in_2432",
      "included_in_2432_harness": true,
      "source_candidate_ids": [
        "equal_risk_growth_tilt_vol_target_v1",
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ]
    },
    {
      "candidate_family": "growth_tilt_engine_signal",
      "candidate_group_id": "growth_tilt_engine_signal_variants",
      "default_2431_status": "needs_pit",
      "execution_status": "not_executed_in_2432",
      "included_in_2432_harness": true,
      "source_candidate_ids": [
        "growth_tilt_engine_signal",
        "growth_tilt_engine_signal_artifact"
      ]
    }
  ],
  "candidate_set_id": "growth_tilt_batch_2432",
  "candidate_set_ready": true,
  "candidates_tested": 0,
  "production_effect": "none",
  "schema_version": "growth_tilt_candidate_gauntlet_candidate_set_snapshot.v1",
  "status": "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
}
```

## Criteria Contract

```json
{
  "broker_action": "none",
  "computed_in_2432": false,
  "criteria_threshold_values_all_null": true,
  "kill_criteria": [
    {
      "criterion_id": "missing_pit_traceability_kill",
      "criterion_type": "hard_fail",
      "rationale": "Candidate cannot pass if PIT/source traceability evidence is missing.",
      "threshold_source": "contract_boolean",
      "threshold_value": null
    },
    {
      "criterion_id": "stale_signal_valid_until_kill",
      "criterion_type": "hard_fail",
      "rationale": "Candidate cannot pass if valid-until semantics are missing or stale.",
      "threshold_source": "contract_boolean",
      "threshold_value": null
    },
    {
      "criterion_id": "net_cost_and_turnover_policy_required",
      "criterion_type": "governed_threshold_required",
      "rationale": "Candidate execution screen must define reviewed cost and turnover policy.",
      "threshold_source": "future_screen_policy_required",
      "threshold_value": null
    }
  ],
  "kill_criteria_ready": true,
  "new_investment_threshold_values_set": false,
  "production_effect": "none",
  "promotion_criteria": [
    {
      "criterion_id": "positive_net_of_cost_edge_required",
      "criterion_type": "governed_threshold_required",
      "rationale": "Promotion needs a reviewed positive net-of-cost edge threshold.",
      "threshold_source": "future_screen_policy_required",
      "threshold_value": null
    },
    {
      "criterion_id": "drawdown_not_materially_worse_required",
      "criterion_type": "governed_threshold_required",
      "rationale": "Promotion needs a reviewed drawdown tolerance policy.",
      "threshold_source": "future_screen_policy_required",
      "threshold_value": null
    },
    {
      "criterion_id": "regime_and_parameter_robustness_required",
      "criterion_type": "governed_threshold_required",
      "rationale": "Promotion needs reviewed regime and parameter robustness thresholds.",
      "threshold_source": "future_screen_policy_required",
      "threshold_value": null
    }
  ],
  "promotion_criteria_ready": true,
  "schema_version": "growth_tilt_candidate_gauntlet_criteria_contract.v1",
  "status": "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY",
  "threshold_policy_required_for_execution": true
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
  "candidate_batch_screen_run": false,
  "candidate_gauntlet_harness_ready": true,
  "candidate_gauntlet_run": false,
  "candidates_tested": 0,
  "contract_gap_count": 0,
  "daily_report_run": false,
  "fresh_market_data_read": false,
  "gaps": [],
  "generated_signal": false,
  "generated_trading_advice": false,
  "historical_screen_run": false,
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
  "schema_version": "growth_tilt_candidate_gauntlet_no_effect_boundary.v1",
  "scoring_run": false,
  "status": "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY",
  "trading_advice_generated": false
}
```
