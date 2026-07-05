# Dynamic strategy research filter threshold methodology review

## 1. Executive summary

- status：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`
- current gate may be too strict for research-only observation：`True`
- reference candidate policy recommendation：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`
- next route：`TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`
- 2388 不批准 observation，不修改真实 gate，不进入 paper-shadow / production / broker。

## 2. Why threshold methodology matters

- 当前验收标准并非完全基于历史样本统计；它混合了风险先验、工程安全边界、项目内回测经验、人工保守判断和 owner risk preference。
- 因此 threshold 本身需要被版本化、分类、解释和校准。

## 3. Current threshold inventory

```json
{
  "cost_turnover_thresholds": {
    "conservative_cost_passed": {
      "current_status": "required_for_observation_consideration",
      "evidence_level": "project_empirical",
      "needs_calibration": false,
      "source": "TRADING-2366",
      "threshold_type": "research_quality_gate"
    },
    "harsh_cost_passed": {
      "current_status": "informative_not_required",
      "evidence_level": "stress_test_heuristic",
      "needs_calibration": true,
      "source": "TRADING-2366",
      "threshold_type": "owner_review_signal"
    },
    "realistic_cost_passed": {
      "current_status": "required_for_continue",
      "evidence_level": "project_empirical",
      "needs_calibration": false,
      "source": "TRADING-2366",
      "threshold_type": "research_quality_gate"
    },
    "turnover_budget_passed": {
      "current_status": "required_for_observation_consideration",
      "current_threshold": 1.0,
      "evidence_level": "project_empirical",
      "needs_calibration": true,
      "source": "TRADING-2366/TRADING-2386",
      "threshold_type": "research_quality_gate"
    }
  },
  "current_2386_threshold_constants": {
    "primary_execution_cadence": "valid_until_window",
    "regime_slice_pass_rate_acceptable_min": 0.3,
    "return_advantage_retained_min": 0.5,
    "time_slice_pass_rate_acceptable_min": 0.4
  },
  "drawdown_thresholds": {
    "drawdown_gap_vs_static": {
      "current_status": "measured",
      "evidence_level": "metric",
      "needs_calibration": true,
      "source": "TRADING-2386",
      "threshold_type": "research_quality_signal"
    },
    "drawdown_not_materially_worse": {
      "current_status": "required_for_auto_accept",
      "current_threshold": 0.02,
      "evidence_level": "conservative_heuristic",
      "needs_calibration": true,
      "source": "TRADING-2386/TRADING-2387",
      "threshold_type": "owner_review_gate"
    },
    "return_per_drawdown_penalty": {
      "current_status": "proposed",
      "evidence_level": "methodology_proposal",
      "needs_calibration": true,
      "source": "TRADING-2387",
      "threshold_type": "owner_review_gate"
    }
  },
  "execution_cadence_thresholds": {
    "monthly_rebalance_not_primary": {
      "current_status": "required",
      "evidence_level": "project_empirical",
      "needs_calibration": false,
      "source": "TRADING-2364",
      "threshold_type": "research_quality_gate"
    },
    "no_stale_signal_carry_forward": {
      "current_status": "required",
      "evidence_level": "engineering_and_research_prior",
      "needs_calibration": false,
      "source": "TRADING-2357/TRADING-2358/TRADING-2364",
      "threshold_type": "hard_research_guardrail"
    },
    "valid_until_window_required": {
      "current_status": "required",
      "evidence_level": "project_empirical",
      "needs_calibration": false,
      "source": "TRADING-2364",
      "threshold_type": "research_quality_gate"
    }
  },
  "reference_candidate_thresholds": {
    "proposed_reference_policy": {
      "needs_calibration": true,
      "recommended_policy": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
      "source": "TRADING-2387",
      "threshold_type": "owner_review_gate"
    },
    "reference_candidate_cannot_accept": {
      "current_status": "hard_block",
      "evidence_level": "conservative_process_rule",
      "needs_calibration": true,
      "source": "TRADING-2386",
      "threshold_type": "process_gate"
    }
  },
  "relative_candidate_thresholds": {
    "must_beat_guarded_reference": {
      "current_status": "currently_used_in_some_gates",
      "evidence_level": "conservative_heuristic",
      "needs_calibration": true,
      "source": "TRADING-2383/TRADING-2386",
      "threshold_type": "owner_review_gate"
    },
    "must_beat_static": {
      "current_status": "required",
      "evidence_level": "research_common_sense",
      "needs_calibration": false,
      "source": "TRADING-2365/TRADING-2386",
      "threshold_type": "research_quality_gate"
    },
    "must_compare_against_lower_turnover_reference": {
      "current_status": "required",
      "evidence_level": "project_empirical",
      "needs_calibration": false,
      "source": "TRADING-2376/TRADING-2379/TRADING-2386",
      "threshold_type": "robustness_reference_gate"
    },
    "must_compare_against_ranking_top_reference": {
      "current_status": "required",
      "evidence_level": "project_empirical",
      "needs_calibration": false,
      "source": "TRADING-2365/TRADING-2375/TRADING-2386",
      "threshold_type": "return_reference_gate"
    }
  },
  "slice_stability_thresholds": {
    "regime_expectation_score": {
      "current_status": "proposed",
      "evidence_level": "methodology_proposal",
      "needs_calibration": true,
      "source": "TRADING-2387",
      "threshold_type": "calibrated_research_gate"
    },
    "regime_slice_pass_rate": {
      "current_status": "required_for_auto_accept",
      "current_threshold": 0.5,
      "evidence_level": "conservative_heuristic",
      "needs_calibration": true,
      "source": "TRADING-2386/TRADING-2387",
      "threshold_type": "research_quality_gate"
    },
    "time_slice_pass_rate": {
      "current_status": "required_for_auto_accept",
      "current_threshold": 0.6,
      "evidence_level": "conservative_heuristic",
      "needs_calibration": true,
      "source": "TRADING-2386/TRADING-2387",
      "threshold_type": "research_quality_gate"
    }
  }
}
```

## 4. Threshold source classification

- project_empirical examples：`['valid_until_window_required', 'monthly_rebalance_not_primary', 'realistic_cost_passed', 'conservative_cost_passed']`
- conservative_heuristic examples：`['time_slice_pass_rate >= 0.60', 'regime_slice_pass_rate >= 0.50', 'reference_candidate_cannot_accept']`
- methodology_proposal examples：`['regime_expectation_score', 'return_per_drawdown_penalty', 'OWNER_REVIEW_FOR_RESEARCH_ONLY_OBSERVATION']`

## 5. Gate taxonomy

```json
{
  "paper_shadow_gate": {
    "broker_must_remain_disabled": true,
    "event_outcome_policy_required": true,
    "explicit_owner_approval_required": true,
    "paper_shadow_enabled": false,
    "paper_shadow_in_scope": false,
    "side_effect": "creates_paper_trade_or_shadow_position",
    "stable_slice_evidence_required": true,
    "threshold_level": "high"
  },
  "production_broker_gate": {
    "broker_action_enabled": false,
    "currently_out_of_scope": true,
    "explicit_owner_approval_required": true,
    "production_enabled": false,
    "side_effect": "real_execution_or_capital_risk",
    "threshold_level": "highest"
  },
  "research_only_observation_gate": {
    "artifact_only": true,
    "auto_accept_allowed": "very_limited",
    "broker_action": false,
    "event_append": false,
    "outcome_binding": false,
    "owner_review_allowed": true,
    "paper_trade": false,
    "principle": "Research-only observation should not use paper-shadow-like thresholds because it observes without execution side effects.",
    "side_effect": "none",
    "threshold_level": "moderate"
  }
}
```

## 6. Candidate threshold outcome matrix

|Candidate|Decision|Static gap|Time|Regime|Blockers|Reclassification|
|---|---|---:|---:|---:|---|---|
|`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`|`CONTINUE_OPTIMIZATION`|`0.021302`|`0.0`|`0.0`|`['reference_candidate_hard_block', 'time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'drawdown_not_materially_worse=false']`|`OWNER_REVIEW_REQUIRED`|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`CONTINUE_OPTIMIZATION`|`0.002205`|`0.428571`|`0.0`|`['reference_candidate_hard_block', 'time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'guarded_gap_negative']`|`CONTINUE_OPTIMIZATION`|
|`dynamic_regime_overlay_v0_4_cooldown_balanced_v1`|`CONTINUE_OPTIMIZATION`|`0.010275`|`0.0`|`0.0`|`['reference_candidate_hard_block', 'time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'drawdown_not_materially_worse=false', 'guarded_gap_negative', 'return_advantage_retained_below_acceptance']`|`CONTINUE_OPTIMIZATION`|
|`equal_risk_growth_tilt_guarded_turnover_v1`|`CONTINUE_OPTIMIZATION`|`0.02062`|`0.0`|`0.0`|`['reference_candidate_hard_block', 'time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'drawdown_not_materially_worse=false']`|`CONTINUE_OPTIMIZATION`|
|`dynamic_turnover_budgeted_growth_tilt_v1`|`CONTINUE_OPTIMIZATION`|`0.006941`|`0.428571`|`0.0`|`['time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'guarded_gap_negative', 'return_advantage_retained_below_acceptance']`|`CONTINUE_OPTIMIZATION`|
|`dynamic_valid_until_expiry_strict_v1`|`CONTINUE_OPTIMIZATION`|`0.007195`|`0.428571`|`0.0`|`['time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'guarded_gap_negative', 'return_advantage_retained_below_acceptance']`|`CONTINUE_OPTIMIZATION`|

## 7. Research-only observation vs paper-shadow separation

- research-only observation 是 artifact-only/no-side-effect，门槛应低于 paper-shadow gate，并允许 `OWNER_REVIEW_REQUIRED`。
- paper-shadow 会创建 paper trades 或 shadow positions，必须保持更高门槛和 explicit owner approval。

## 8. Reference candidate policy review

- reference candidate 是否应 hard-block：`False`
- recommendation：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`

## 9. Time / regime / drawdown threshold methodology

- time/regime/drawdown thresholds need calibration：`True`
- thresholds requiring calibration：`['time_slice_pass_rate', 'regime_expectation_score', 'drawdown_materiality', 'return_per_drawdown_penalty', 'owner_review_required_vs_continue_optimization_boundary']`

## 10. Recommended gate policy proposal

```json
{
  "future_statistical_calibration_needed": [
    "time_slice_pass_rate_threshold",
    "regime_expectation_score_threshold",
    "drawdown_materiality_threshold",
    "return_per_drawdown_penalty_threshold",
    "turnover_budget_materiality",
    "reference_candidate_reclassification_policy",
    "owner_review_required_vs_continue_optimization_boundary"
  ],
  "next_owner_decision_route": "TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision",
  "policy_update_applied": false,
  "reference_candidate_policy": {
    "current": "HARD_BLOCK_ACCEPTANCE",
    "recommended": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW"
  },
  "research_only_observation": {
    "auto_accept": {
      "allowed": true,
      "requirements": [
        "cost_adjusted_return_above_static",
        "realistic_cost_passed",
        "conservative_cost_passed",
        "turnover_budget_passed",
        "no_hard_guardrail_failure",
        "time_slice_pass_rate >= calibrated_threshold",
        "regime_expectation_score >= calibrated_threshold",
        "drawdown_risk_acceptable"
      ]
    },
    "continue_optimization": {
      "conditions": [
        "positive_evidence_exists",
        "but_slice_robustness_relative_reference_or_drawdown_tradeoff_insufficient"
      ]
    },
    "owner_review_required": {
      "allowed": true,
      "conditions": [
        "cost_and_turnover_pass",
        "positive_dynamic_vs_static_gap",
        "but_slice_or_drawdown_or_reference_status_requires_judgment"
      ]
    },
    "reject_for_now": {
      "conditions": [
        "realistic_or_conservative_cost_failed",
        "severe_drawdown_deterioration",
        "invalid_execution_assumption",
        "no_positive_dynamic_vs_static_gap"
      ]
    }
  },
  "rules_mutated": false
}
```

## 11. Future statistical calibration needs

- `['time_slice_pass_rate_threshold', 'regime_expectation_score_threshold', 'drawdown_materiality_threshold', 'return_per_drawdown_penalty_threshold', 'turnover_budget_materiality', 'reference_candidate_reclassification_policy', 'owner_review_required_vs_continue_optimization_boundary']`
- optional future route：`TRADING-2390_Dynamic_Strategy_Threshold_Meta_Dataset_And_Historical_Gate_Backtest`

## 12. Explicit non-goals

- `{"append_historical_event_log": false, "approve_broker_action": false, "approve_observation": false, "approve_paper_shadow": false, "bind_outcome": false, "call_broker_api": false, "create_paper_trade": false, "create_scheduled_task": false, "create_shadow_position": false, "enable_paper_shadow": false, "enable_production": false, "enable_scheduler": false, "generate_daily_report": false, "mutate_outcome_store": false, "send_order": false}`

## 13. Recommended next route

- `TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`
