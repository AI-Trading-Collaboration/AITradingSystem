# 动态策略门禁证据计划

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_AND_GATE_EVIDENCE_PLAN_READY`
- ablation retest executed：`False`
- paper-shadow enabled：`False`

```json
{
  "component_gate_evidence": [
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "turnover_budgeting",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "turnover_budget_ablation",
        "cost_drag_reduction_test",
        "return_retention_test",
        "max_monthly_turnover_stress",
        "valid_until_window_preservation_check"
      ],
      "success_criteria": [
        "reduces_turnover_vs_source_candidate",
        "does_not_destroy_cost_adjusted_return",
        "improves_or_preserves_realistic_cost_gap",
        "does_not_increase_stale_signal_execution"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "valid_until_strictness",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "stale_signal_prevention_ablation",
        "near_expiry_signal_decay_test",
        "signal_to_execution_lag_test",
        "return_gap_impact_test",
        "regime_slice_impact_test"
      ],
      "success_criteria": [
        "reduces_stale_signal_execution_count",
        "preserves_positive_dynamic_vs_static_gap",
        "does_not_excessively_reduce_upside_capture",
        "improves_signal_expiry_discipline"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "growth_tilt_engine",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "growth_tilt_ablation",
        "risk_on_upside_capture_test",
        "high_volatility_drawdown_test",
        "trend_confirmed_gate_test",
        "drawdown_compensation_test"
      ],
      "success_criteria": [
        "preserves_return_advantage",
        "improves_upside_capture",
        "does_not_materially_worsen_drawdown_after_guardrails",
        "passes_realistic_and_conservative_cost"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "lower_turnover_guardrail",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "lower_turnover_ablation",
        "cooldown_ablation",
        "max_step_weight_delta_test",
        "turnover_cap_stress",
        "return_gap_tradeoff_test"
      ],
      "success_criteria": [
        "reduces_turnover",
        "improves_cost_resilience",
        "does_not_destroy_return_gap_too_much",
        "preserves_valid_until_window"
      ]
    },
    {
      "candidate_level_approval_required_after_evidence": true,
      "component_name": "guarded_turnover_transfer",
      "execution_paths_enabled": false,
      "paper_shadow_gate_separate": true,
      "required_future_tests": [
        "guarded_transfer_ablation",
        "ranking_top_return_retention_test",
        "drawdown_fragility_test",
        "turnover_tradeoff_test"
      ],
      "success_criteria": [
        "reduces_fragility_relative_to_ranking_top",
        "keeps_return_gap_close_to_ranking_top",
        "does_not_add_stale_signal_execution",
        "preserves_valid_until_window"
      ]
    }
  ],
  "plan_ready": true
}
```