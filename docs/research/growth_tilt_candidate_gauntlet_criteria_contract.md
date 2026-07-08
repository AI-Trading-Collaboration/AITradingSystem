# Growth Tilt Candidate Gauntlet Criteria Contract

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
