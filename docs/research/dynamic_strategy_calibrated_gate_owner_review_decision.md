# Dynamic strategy calibrated gate owner review decision

## 1. Executive summary

- status：`DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY`
- owner decision：`ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL`
- next task：`TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution`

## 2. Source findings from TRADING-2387 / 2388

```json
{
  "trading_2386": {
    "candidate_ready_for_research_only_observation": false,
    "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
    "current_best_decision": "CONTINUE_OPTIMIZATION",
    "status": "DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING_READY"
  },
  "trading_2387": {
    "observation_approved": false,
    "policy_update_applied": false,
    "reference_candidate_policy_recommendation": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
    "research_only_gate_may_be_too_strict": true,
    "status": "DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY"
  },
  "trading_2388": {
    "current_gate_may_be_too_strict_for_research_only_observation": true,
    "reference_candidate_policy_recommendation": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
    "status": "DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY",
    "threshold_methodology_review_ready": true,
    "thresholds_requiring_statistical_calibration": [
      "time_slice_pass_rate",
      "regime_expectation_score",
      "drawdown_materiality",
      "return_per_drawdown_penalty",
      "owner_review_required_vs_continue_optimization_boundary"
    ]
  }
}
```

## 3. Owner decision

```json
{
  "adopt_threshold_methodology_review": {
    "decision": "APPROVE",
    "source": "TRADING-2388"
  },
  "allow_calibrated_reclassification_preview": {
    "decision": "APPROVE",
    "next_task": "TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution"
  },
  "allow_candidate_auto_accept": {
    "decision": "REJECT",
    "reason": "calibrated policy still requires owner review"
  },
  "approve_current_best_candidate_for_observation": {
    "decision": "REJECT_FOR_NOW",
    "reason": "2389 is policy decision, not candidate approval"
  },
  "require_component_attribution_review": {
    "decision": "APPROVE",
    "reason": "failed candidates may contain useful components"
  },
  "require_statistical_threshold_calibration": {
    "decision": "APPROVE_AS_FUTURE_RESEARCH",
    "reason": "current thresholds are not fully historical-calibrated"
  },
  "separate_research_only_and_paper_shadow_gates": {
    "decision": "APPROVE",
    "source": "TRADING-2388"
  },
  "update_reference_candidate_policy": {
    "decision": "APPROVE_FOR_RESEARCH_ONLY_GATE",
    "policy": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW"
  }
}
```

## 4. Calibrated gate adoption record

```json
{
  "calibrated_research_only_gate_policy": {
    "paper_shadow": {
      "currently_disabled": true,
      "explicit_owner_approval_required": true,
      "side_effect": "paper_trade_or_shadow_position",
      "threshold_level": "high"
    },
    "production_broker": {
      "currently_out_of_scope": true,
      "side_effect": "capital_risk",
      "threshold_level": "highest"
    },
    "research_only_observation": {
      "auto_accept_allowed": "limited",
      "owner_review_allowed": true,
      "side_effect": "none",
      "threshold_level": "moderate"
    }
  },
  "decision_mapping": {
    "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION": {
      "allowed": true,
      "requires": [
        "calibrated_gate_passed",
        "owner_review_recorded",
        "no_side_effect_flags_confirmed"
      ]
    },
    "CONTINUE_OPTIMIZATION": {
      "allowed": true,
      "meaning": "evidence exists but still insufficient for owner review or observation"
    },
    "DEPRECATED": {
      "allowed": true,
      "meaning": "candidate or result should no longer guide ranking"
    },
    "OWNER_REVIEW_REQUIRED": {
      "allowed": true,
      "meaning": "candidate has enough evidence for human review, not automatic approval"
    },
    "REJECT_FOR_NOW": {
      "allowed": true,
      "meaning": "cost / drawdown / execution / evidence failure"
    }
  },
  "owner_decision": "ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL",
  "policy_update_applied": false,
  "reference_candidate_policy": {
    "adopted_policy": "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW",
    "does_not_apply_to": [
      "paper_shadow",
      "production",
      "broker"
    ],
    "old_policy": "HARD_BLOCK_ACCEPTANCE",
    "scope": "RESEARCH_ONLY_OBSERVATION_GATE_ONLY"
  },
  "rules_mutated": false,
  "source_gate_taxonomy": {
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
  },
  "source_recommended_gate_policy_proposal": {
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
  },
  "source_thresholds_requiring_statistical_calibration": [
    "time_slice_pass_rate",
    "regime_expectation_score",
    "drawdown_materiality",
    "return_per_drawdown_penalty",
    "owner_review_required_vs_continue_optimization_boundary"
  ],
  "threshold_methodology_adopted": true
}
```

## 5. Reference candidate policy decision

- adopted policy：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`
- scope：`RESEARCH_ONLY_OBSERVATION_GATE_ONLY`
- paper-shadow / production / broker：not covered by this adoption.

## 6. Research-only vs paper-shadow separation

- separated：`True`
- research-only observation remains no-side-effect and owner-review gated.
- paper-shadow remains disabled and requires separate explicit owner approval.

## 7. Explicit non-approval list

```json
{
  "broker_action_approved": false,
  "candidate_auto_accept_approved": false,
  "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
  "current_best_candidate_decision": "CONTINUE_OPTIMIZATION",
  "current_best_candidate_observation_approved": false,
  "daily_report_approved": false,
  "event_append_approved": false,
  "explicit_non_approval_list": [
    "research_only_observation_for_candidate",
    "candidate_auto_accept",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "scheduled_task",
    "daily_report",
    "production",
    "broker",
    "order"
  ],
  "outcome_binding_approved": false,
  "owner_decision": "ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL",
  "paper_shadow_approved": false,
  "production_approved": false,
  "reason": [
    "2389 adopts methodology and records owner decision only",
    "candidate approval requires a separate calibrated reclassification step",
    "paper-shadow / event / outcome / scheduler paths remain out of scope"
  ],
  "research_only_observation_approved": false,
  "scheduler_approved": false
}
```

## 8. Next reclassification route

```json
{
  "allowed_actions": [
    "calibrated_reclassification_preview",
    "component_level_attribution",
    "owner_review_candidate_identification"
  ],
  "candidate_reclassification_targets": [
    {
      "candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
      "component_value_type": "current_best_reference_return_leader",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "OWNER_REVIEW_REQUIRED"
    },
    {
      "candidate_id": "dynamic_turnover_budgeted_growth_tilt_v1",
      "component_value_type": "turnover_budget_component_value",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    },
    {
      "candidate_id": "dynamic_valid_until_expiry_strict_v1",
      "component_value_type": "valid_until_component_value",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    },
    {
      "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
      "component_value_type": "robustness_repair_variant",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    },
    {
      "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
      "component_value_type": "guarded_return_reference",
      "latest_decision": "CONTINUE_OPTIMIZATION",
      "likely_reclassification_under_calibrated_gate": "CONTINUE_OPTIMIZATION"
    }
  ],
  "forbidden_actions": [
    "observation_approval",
    "paper_shadow_enablement",
    "event_append",
    "outcome_binding",
    "scheduler_enablement",
    "production_or_broker_action"
  ],
  "purpose": [
    "apply calibrated gate preview to 2386 candidates",
    "distinguish candidate failure from component value",
    "identify candidates eligible for OWNER_REVIEW_REQUIRED",
    "recommend whether current best candidate should enter owner-review-only decision"
  ],
  "recommended_next_research_task": "TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution",
  "task_name": "Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution"
}
```

## 9. Guardrail summary

```json
{
  "broker_action": "none",
  "broker_action_enabled": false,
  "daily_report_generated": false,
  "event_append_enabled": false,
  "observation_approved": false,
  "outcome_binding_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "scheduler_enabled": false
}
```

## 10. Recommended next task

- `TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_Component_Attribution`
