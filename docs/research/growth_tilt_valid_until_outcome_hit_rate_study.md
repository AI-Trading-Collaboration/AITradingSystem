# Growth Tilt Valid-Until Outcome Hit-Rate Study

- task_id：`TRADING-2435`
- status：`GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY`
- valid-until component value found：`True`
- candidate status：`component_value`
- outcome sample count：`0`
- next route：`TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study`

TRADING-2435 只读取 prior artifacts / config / docs，不读取 fresh market or outcome data，不运行 PIT replay、backtest、scoring 或 outcome binding。0 delta 表示本任务未计算真实 outcome hit-rate，不是收益结论。

```json
{
  "candidate_status": "component_value",
  "expiry_failure_count": 0,
  "next_route": "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study",
  "outcome_sample_count": 0,
  "stale_signal_reduction": 0.0,
  "status": "GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY",
  "valid_until_component_value_found": true,
  "valid_until_hit_rate_delta": 0.0
}
```

## Valid-Until Hit-Rate Matrix

```json
{
  "baseline_hit_rate": null,
  "broker_action": "none",
  "computed_new_metrics": false,
  "measurement_basis": "not_computed_prior_artifact_contract_only_no_real_outcome_binding",
  "observed_outcome_hit_rate_available": false,
  "outcome_sample_count": 0,
  "production_effect": "none",
  "schema_version": "growth_tilt_valid_until_outcome_hit_rate_matrix.v1",
  "status": "GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY",
  "valid_until_hit_rate": null,
  "valid_until_hit_rate_delta": 0.0,
  "valid_until_hit_rate_matrix_ready": true
}
```

## No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "backtest_run": false,
  "broker_action": "none",
  "broker_enabled": false,
  "computed_new_metrics": false,
  "evidence_gap_count": 0,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [],
  "generated_signal": false,
  "generated_trading_advice": false,
  "historical_screen_run": false,
  "no_effect_boundary_ready": true,
  "outcome_backfilled": false,
  "outcome_binding_executed": false,
  "outcome_store_mutated": false,
  "paper_shadow_enabled": false,
  "pit_replay_run": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_valid_until_outcome_hit_rate_no_effect_boundary.v1",
  "scoring_run": false,
  "status": "GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY"
}
```
