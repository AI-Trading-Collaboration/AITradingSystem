# Growth Tilt Candidate Gauntlet Metric Contract

```json
{
  "broker_action": "none",
  "computed_in_2432": false,
  "metrics": [
    {
      "computed_in_2432": false,
      "direction": "higher_is_better",
      "metric_id": "return_delta_vs_baseline"
    },
    {
      "computed_in_2432": false,
      "direction": "higher_is_better",
      "metric_id": "sharpe_delta_vs_baseline"
    },
    {
      "computed_in_2432": false,
      "direction": "lower_is_better",
      "metric_id": "max_drawdown_delta_vs_baseline"
    },
    {
      "computed_in_2432": false,
      "direction": "lower_is_better",
      "metric_id": "turnover_delta_vs_baseline"
    },
    {
      "computed_in_2432": false,
      "direction": "lower_is_better",
      "metric_id": "false_risk_off_delta"
    },
    {
      "computed_in_2432": false,
      "direction": "lower_is_better",
      "metric_id": "missed_upside_delta"
    },
    {
      "computed_in_2432": false,
      "direction": "lower_is_better",
      "metric_id": "whipsaw_delta"
    },
    {
      "computed_in_2432": false,
      "direction": "higher_is_better",
      "metric_id": "valid_until_hit_rate"
    },
    {
      "computed_in_2432": false,
      "direction": "higher_is_better",
      "metric_id": "parameter_robustness_score"
    },
    {
      "computed_in_2432": false,
      "direction": "higher_is_better",
      "metric_id": "regime_robustness_score"
    },
    {
      "computed_in_2432": false,
      "direction": "higher_is_better",
      "metric_id": "forward_aging_score"
    }
  ],
  "metrics_ready": true,
  "production_effect": "none",
  "required_metrics": [
    "return_delta_vs_baseline",
    "sharpe_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
    "turnover_delta_vs_baseline",
    "false_risk_off_delta",
    "missed_upside_delta",
    "whipsaw_delta",
    "valid_until_hit_rate",
    "parameter_robustness_score",
    "regime_robustness_score",
    "forward_aging_score"
  ],
  "schema_version": "growth_tilt_candidate_gauntlet_metric_contract.v1",
  "status": "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
}
```
