# Growth Tilt Existing Candidate Evidence Matrix

```json
{
  "broker_action": "none",
  "candidate_count": 6,
  "candidate_evidence_matrix_ready": true,
  "candidates": [
    {
      "broker_action": "none",
      "candidate_family": "defensive_limited_adjustment",
      "candidate_group_id": "defensive_limited_adjustment",
      "candidate_status": "component_value",
      "component_value_evidence_present": false,
      "known_blockers": [
        "not_paper_shadow_promotion_candidate",
        "paper_shadow_not_approved",
        "production_not_approved",
        "broker_not_approved",
        "trading_2430_no_promotion_candidate",
        "prior_owner_observation_not_approved",
        "requires_gauntlet_or_pit_before_paper_shadow_review"
      ],
      "metric_coverage": [
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "return_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "sharpe_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "max_drawdown_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "turnover_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "false_risk_off_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "missed_upside_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "whipsaw_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "valid_until_hit_rate",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "parameter_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "regime_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "forward_aging_score",
          "value": null
        }
      ],
      "metric_coverage_status": "partial",
      "next_validation_route": "TRADING-2434_Defensive_Limited_Adjustment_Component_Validation",
      "paper_shadow_enabled": false,
      "paper_shadow_promotion_candidate": false,
      "primary_value": "drawdown_control_research_hypothesis",
      "prior_doc_evidence_present": true,
      "prior_evidence_references": [
        "defensive_limited_adjustment"
      ],
      "production_effect": "none",
      "production_enabled": false,
      "registry_candidate_overlap": [],
      "source_candidate_ids": [
        "defensive_limited_adjustment"
      ],
      "status_rationale": "Prior evidence supports component-level value for defensive_limited_adjustment; this is not paper-shadow approval."
    },
    {
      "broker_action": "none",
      "candidate_family": "lower_turnover_guardrail",
      "candidate_group_id": "lower_turnover_variants",
      "candidate_status": "component_value",
      "component_value_evidence_present": false,
      "known_blockers": [
        "not_paper_shadow_promotion_candidate",
        "paper_shadow_not_approved",
        "production_not_approved",
        "broker_not_approved",
        "trading_2430_no_promotion_candidate",
        "prior_owner_observation_not_approved",
        "requires_gauntlet_or_pit_before_paper_shadow_review"
      ],
      "metric_coverage": [
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "return_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "sharpe_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "max_drawdown_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "turnover_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "false_risk_off_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "missed_upside_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "whipsaw_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "valid_until_hit_rate",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "parameter_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "regime_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "forward_aging_score",
          "value": null
        }
      ],
      "metric_coverage_status": "partial",
      "next_validation_route": "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study",
      "paper_shadow_enabled": false,
      "paper_shadow_promotion_candidate": false,
      "primary_value": "turnover_and_whipsaw_control",
      "prior_doc_evidence_present": true,
      "prior_evidence_references": [
        "dynamic_regime_overlay_v0_4_lower_turnover"
      ],
      "production_effect": "none",
      "production_enabled": false,
      "registry_candidate_overlap": [],
      "source_candidate_ids": [
        "dynamic_regime_overlay_v0_4_lower_turnover",
        "dynamic_regime_growth_tilt_lower_turnover_fusion_v1",
        "equal_risk_growth_tilt_lower_turnover_guarded_v1",
        "growth_tilt_lower_turnover_guarded_transfer_v1"
      ],
      "status_rationale": "Prior evidence supports component-level value for lower_turnover_variants; this is not paper-shadow approval."
    },
    {
      "broker_action": "none",
      "candidate_family": "valid_until_strictness",
      "candidate_group_id": "dynamic_valid_until_expiry_strict_v1",
      "candidate_status": "component_value",
      "component_value_evidence_present": true,
      "known_blockers": [
        "not_paper_shadow_promotion_candidate",
        "paper_shadow_not_approved",
        "production_not_approved",
        "broker_not_approved",
        "trading_2430_no_promotion_candidate",
        "prior_owner_observation_not_approved",
        "requires_gauntlet_or_pit_before_paper_shadow_review"
      ],
      "metric_coverage": [
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "return_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "sharpe_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "max_drawdown_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "turnover_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "false_risk_off_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "missed_upside_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "whipsaw_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "valid_until_hit_rate",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "parameter_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "regime_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "forward_aging_score",
          "value": null
        }
      ],
      "metric_coverage_status": "partial",
      "next_validation_route": "TRADING-2435_Growth_Tilt_Valid_Until_Outcome_Hit_Rate_Study",
      "paper_shadow_enabled": false,
      "paper_shadow_promotion_candidate": false,
      "primary_value": "stale_signal_reduction",
      "prior_doc_evidence_present": true,
      "prior_evidence_references": [
        "dynamic_valid_until_expiry_strict_v1"
      ],
      "production_effect": "none",
      "production_enabled": false,
      "registry_candidate_overlap": [],
      "source_candidate_ids": [
        "dynamic_valid_until_expiry_strict_v1"
      ],
      "status_rationale": "Prior evidence supports component-level value for dynamic_valid_until_expiry_strict_v1; this is not paper-shadow approval."
    },
    {
      "broker_action": "none",
      "candidate_family": "turnover_budgeting",
      "candidate_group_id": "dynamic_turnover_budgeted_growth_tilt_v1",
      "candidate_status": "component_value",
      "component_value_evidence_present": true,
      "known_blockers": [
        "not_paper_shadow_promotion_candidate",
        "paper_shadow_not_approved",
        "production_not_approved",
        "broker_not_approved",
        "trading_2430_no_promotion_candidate",
        "prior_owner_observation_not_approved",
        "requires_gauntlet_or_pit_before_paper_shadow_review"
      ],
      "metric_coverage": [
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "return_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "sharpe_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "max_drawdown_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "turnover_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "false_risk_off_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "missed_upside_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "whipsaw_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "valid_until_hit_rate",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "parameter_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "regime_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "forward_aging_score",
          "value": null
        }
      ],
      "metric_coverage_status": "partial",
      "next_validation_route": "TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study",
      "paper_shadow_enabled": false,
      "paper_shadow_promotion_candidate": false,
      "primary_value": "turnover_budget_discipline",
      "prior_doc_evidence_present": true,
      "prior_evidence_references": [
        "dynamic_turnover_budgeted_growth_tilt_v1"
      ],
      "production_effect": "none",
      "production_enabled": false,
      "registry_candidate_overlap": [],
      "source_candidate_ids": [
        "dynamic_turnover_budgeted_growth_tilt_v1"
      ],
      "status_rationale": "Prior evidence supports component-level value for dynamic_turnover_budgeted_growth_tilt_v1; this is not paper-shadow approval."
    },
    {
      "broker_action": "none",
      "candidate_family": "vol_target_growth_tilt",
      "candidate_group_id": "equal_risk_growth_tilt_vol_target_variants",
      "candidate_status": "needs_pit",
      "component_value_evidence_present": false,
      "known_blockers": [
        "not_paper_shadow_promotion_candidate",
        "paper_shadow_not_approved",
        "production_not_approved",
        "broker_not_approved",
        "trading_2430_no_promotion_candidate",
        "prior_owner_observation_not_approved",
        "requires_gauntlet_or_pit_before_paper_shadow_review"
      ],
      "metric_coverage": [
        {
          "computed_in_2431": false,
          "coverage_status": "prior_metric_available",
          "metric_id": "return_delta_vs_baseline",
          "value": 0.021302
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "sharpe_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_metric_available",
          "metric_id": "max_drawdown_delta_vs_baseline",
          "value": 0.043574
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_metric_available",
          "metric_id": "turnover_delta_vs_baseline",
          "value": 1.964574
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "false_risk_off_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "missed_upside_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "whipsaw_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_metric_available",
          "metric_id": "valid_until_hit_rate",
          "value": 1.0
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "parameter_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_metric_available",
          "metric_id": "regime_robustness_score",
          "value": 0.0
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "forward_aging_score",
          "value": null
        }
      ],
      "metric_coverage_status": "partial",
      "next_validation_route": "TRADING-2438_Growth_Tilt_Top-3_Candidate_PIT_Replay",
      "paper_shadow_enabled": false,
      "paper_shadow_promotion_candidate": false,
      "primary_value": "return_gap_repair_candidate",
      "prior_doc_evidence_present": true,
      "prior_evidence_references": [
        "equal_risk_growth_tilt_vol_target_v1",
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "production_effect": "none",
      "production_enabled": false,
      "registry_candidate_overlap": [
        "equal_risk_growth_tilt_vol_target_v1"
      ],
      "source_candidate_ids": [
        "equal_risk_growth_tilt_vol_target_v1",
        "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
      ],
      "status_rationale": "equal_risk_growth_tilt_vol_target_variants needs batch gauntlet or PIT replay; current best candidate is equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1 but owner approval is absent."
    },
    {
      "broker_action": "none",
      "candidate_family": "growth_tilt_engine_signal",
      "candidate_group_id": "growth_tilt_engine_signal_variants",
      "candidate_status": "needs_pit",
      "component_value_evidence_present": false,
      "known_blockers": [
        "not_paper_shadow_promotion_candidate",
        "paper_shadow_not_approved",
        "production_not_approved",
        "broker_not_approved",
        "trading_2430_no_promotion_candidate",
        "prior_owner_observation_not_approved",
        "requires_gauntlet_or_pit_before_paper_shadow_review"
      ],
      "metric_coverage": [
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "return_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "sharpe_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "max_drawdown_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "turnover_delta_vs_baseline",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "false_risk_off_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "missed_upside_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "whipsaw_delta",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "valid_until_hit_rate",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "parameter_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "regime_robustness_score",
          "value": null
        },
        {
          "computed_in_2431": false,
          "coverage_status": "prior_doc_reference_only",
          "metric_id": "forward_aging_score",
          "value": null
        }
      ],
      "metric_coverage_status": "partial",
      "next_validation_route": "TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness",
      "paper_shadow_enabled": false,
      "paper_shadow_promotion_candidate": false,
      "primary_value": "observe_only_signal_candidate_family",
      "prior_doc_evidence_present": true,
      "prior_evidence_references": [
        "growth_tilt_engine_signal"
      ],
      "production_effect": "none",
      "production_enabled": false,
      "registry_candidate_overlap": [],
      "source_candidate_ids": [
        "growth_tilt_engine_signal",
        "growth_tilt_engine_signal_artifact"
      ],
      "status_rationale": "growth_tilt_engine_signal_variants needs batch gauntlet or PIT replay; current best candidate is equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1 but owner approval is absent."
    }
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_existing_candidate_evidence_matrix_table.v1",
  "status": "GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY"
}
```
