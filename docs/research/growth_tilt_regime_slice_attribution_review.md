# Growth Tilt Regime Slice Attribution Review

- task_id：`TRADING-2437`
- status：`GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY`
- regime robustness score：`0.0`
- single regime dependency detected：`False`
- next route：`TRADING-2438_Growth_Tilt_Top3_Candidate_PIT_Replay`

TRADING-2437 只读取 prior artifacts / config / docs，不读取 fresh market or outcome data，不运行真实 regime attribution、PIT replay、backtest 或 scoring。所有 recommended regime slice status=inconclusive 表示本任务未执行真实分层归因，不是策略通过、失败或 promotion 结论。

```json
{
  "candidate_status_by_regime": {
    "growth_bull": "inconclusive",
    "growth_drawdown": "inconclusive",
    "liquidity_stress": "inconclusive",
    "mega_cap_concentration": "inconclusive",
    "post_drawdown_recovery": "inconclusive",
    "rate_shock": "inconclusive",
    "semiconductor_leadership": "inconclusive",
    "sideways_chop": "inconclusive",
    "volatility_spike": "inconclusive"
  },
  "next_route": "TRADING-2438_Growth_Tilt_Top3_Candidate_PIT_Replay",
  "regime_robustness_score": 0.0,
  "single_regime_dependency_detected": false,
  "status": "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY"
}
```

## Regime Slice Attribution Matrix

```json
{
  "broker_action": "none",
  "computed_new_metrics": false,
  "production_effect": "none",
  "recommended_regime_slice_count": 9,
  "regime_attribution_run": false,
  "regime_robustness_score": 0.0,
  "regime_slice_attribution_matrix_ready": true,
  "rows": [
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "growth_bull",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "growth_drawdown",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "rate_shock",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "volatility_spike",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "liquidity_stress",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "post_drawdown_recovery",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "sideways_chop",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "semiconductor_leadership",
      "single_regime_dependency_detected": false
    },
    {
      "attribution_available": false,
      "candidate_status": "inconclusive",
      "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
      "regime_robustness_score": 0.0,
      "regime_slice": "mega_cap_concentration",
      "single_regime_dependency_detected": false
    }
  ],
  "schema_version": "growth_tilt_regime_slice_attribution_matrix.v1",
  "single_regime_dependency_assessed": false,
  "single_regime_dependency_detected": false,
  "status": "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY"
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
  "market_data_regime_attribution_run": false,
  "no_effect_boundary_ready": true,
  "outcome_backfilled": false,
  "outcome_binding_executed": false,
  "paper_shadow_enabled": false,
  "pit_replay_run": false,
  "production_effect": "none",
  "production_enabled": false,
  "regime_attribution_run": false,
  "schema_version": "growth_tilt_regime_slice_attribution_no_effect_boundary.v1",
  "scoring_run": false,
  "status": "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY"
}
```
