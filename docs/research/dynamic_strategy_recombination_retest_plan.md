# 动态策略 recombination retest plan

- status：`DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION_CANDIDATE_PLAN_READY`
- next route：`TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest`

```json
{
  "component_evidence_metrics": {
    "guardrail_layer": [
      "turnover_reduction_vs_raw_growth_tilt",
      "cost_drag_reduction",
      "drawdown_gap_vs_static",
      "max_monthly_turnover"
    ],
    "recombination_quality": [
      "cost_adjusted_return",
      "return_per_drawdown_penalty",
      "time_slice_pass_rate",
      "regime_expectation_score",
      "owner_review_required"
    ],
    "return_engine": [
      "return_retention_vs_raw_growth_tilt",
      "upside_capture",
      "dynamic_vs_static_gap"
    ],
    "valid_until_layer": [
      "stale_signal_execution_count",
      "signal_to_execution_lag_days",
      "near_expiry_signal_behavior"
    ]
  },
  "cost_stress": {
    "base": {
      "slippage_bps": 2,
      "transaction_cost_bps": 2
    },
    "conservative": {
      "slippage_bps": 10,
      "transaction_cost_bps": 10
    },
    "harsh": {
      "slippage_bps": 10,
      "transaction_cost_bps": 20
    },
    "realistic": {
      "slippage_bps": 5,
      "transaction_cost_bps": 5
    }
  },
  "execution_cadence": {
    "comparison": [
      "valid_until_window",
      "cooldown_limited_event_driven",
      "signal_event_driven"
    ],
    "monthly_rebalance": {
      "allowed_for_primary_decision": false,
      "allowed_for_reference": true
    },
    "primary": "valid_until_window"
  },
  "must_not": [
    "run_without_data_quality_gate_in_2396",
    "use_monthly_rebalance_as_primary",
    "approve_observation_inside_retest",
    "enable_scheduler_or_paper_shadow",
    "call_broker_or_generate_order"
  ],
  "next_task": "TRADING-2396_Dynamic_Strategy_Component_Recombination_Candidate_Retest",
  "plan_ready": true,
  "planned_recombination_candidates": [
    "growth_tilt_lower_turnover_guarded_v1",
    "growth_tilt_turnover_budgeted_v1",
    "growth_tilt_valid_until_strict_v1",
    "growth_tilt_turnover_budgeted_valid_until_strict_v1",
    "growth_tilt_lower_turnover_guarded_transfer_v1",
    "growth_tilt_conservative_guarded_v1"
  ],
  "reference_candidates": {
    "cooldown_balanced_reference": {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "role": "best_lower_turnover_variant_reference"
    },
    "guarded_turnover_reference": {
      "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
      "role": "guarded_transfer_reference"
    },
    "lower_turnover_reference": {
      "candidate_id": "dynamic_regime_overlay_v0_4_lower_turnover",
      "role": "robustness_reference"
    },
    "raw_growth_tilt_reference": {
      "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "role": "raw_return_engine_reference"
    },
    "static_baseline": {
      "role": "baseline_reference"
    }
  },
  "schema_version": "dynamic_strategy_component_recombination_retest_plan.v1",
  "slice_regime_tests": {
    "regime_slices": [
      "risk_on",
      "risk_off",
      "high_volatility",
      "low_volatility",
      "trend_confirmed",
      "recovery"
    ],
    "time_slices": [
      "full_available_window",
      "recent_period",
      "post_2023_ai_cycle",
      "high_volatility_periods",
      "drawdown_recovery_periods"
    ]
  }
}
```