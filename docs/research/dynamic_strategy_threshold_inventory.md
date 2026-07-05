# Dynamic strategy threshold inventory

- status：`DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY`

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
