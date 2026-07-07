# Growth tilt engine contract gap remediation plan

## 结论摘要

- status：`GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`
- gap count：`7`
- remediation item count：`7`
- unclassified remediation item count：`0`
- next route：`TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation`

2411 只把 2410 暴露出的 blocked/gap source features 转成 remediation plan、实施顺序和 validation design。它不执行 remediation、不修改 `growth_tilt_engine`、不生成 feature / signal / scoring / backtest / daily report，也不恢复 candidate search 或进入 observation / paper-shadow / production / broker。

## Remediation Items

```json
[
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "equal_risk_baseline_weights blocks contract-ready and PIT gate readiness until remediated.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "missing_source_traceability",
    "dependency_ordering": "phase_1_source_traceability",
    "feature_id": "equal_risk_baseline_weights",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": false,
    "missing_contract_dimension": [
      "source_traceability"
    ],
    "missing_source_traceability": true,
    "missing_validity_dependency": false,
    "pit_eligibility_risk": "material_approximate_pit_evidence",
    "production_effect": "none",
    "remediation_action": "Create a source feature manifest for equal_risk_baseline_weights with upstream artifact, source cutoff, generated_at, checksum, and lineage fields.",
    "remediation_category": "source_traceability_required",
    "remediation_order": 1,
    "required_code_doc_config_change": "Add governed manifest/config reference and docs for equal_risk_baseline_weights; wire validator fixtures before implementation.",
    "required_upstream_artifact": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
    "requires_owner_review": false,
    "source_feature_name": "equal_risk_baseline_weights",
    "validation_requirement": "Focused validator must reject equal_risk_baseline_weights without generated_at, source cutoff, checksum, and lineage; pass only after manifest coverage exists."
  },
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "target_vol_policy blocks contract-ready and PIT gate readiness until remediated.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "missing_source_traceability",
    "dependency_ordering": "phase_1_source_traceability",
    "feature_id": "target_vol_policy",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": false,
    "missing_contract_dimension": [
      "source_traceability"
    ],
    "missing_source_traceability": true,
    "missing_validity_dependency": false,
    "pit_eligibility_risk": "material_approximate_pit_evidence",
    "production_effect": "none",
    "remediation_action": "Create a source feature manifest for target_vol_policy with upstream artifact, source cutoff, generated_at, checksum, and lineage fields.",
    "remediation_category": "source_traceability_required",
    "remediation_order": 2,
    "required_code_doc_config_change": "Add governed manifest/config reference and docs for target_vol_policy; wire validator fixtures before implementation.",
    "required_upstream_artifact": "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
    "requires_owner_review": false,
    "source_feature_name": "target_vol_policy",
    "validation_requirement": "Focused validator must reject target_vol_policy without generated_at, source cutoff, checksum, and lineage; pass only after manifest coverage exists."
  },
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "trend_features blocks contract-ready and PIT gate readiness until remediated.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "missing_source_traceability",
    "dependency_ordering": "phase_1_source_traceability",
    "feature_id": "trend_features",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": false,
    "missing_contract_dimension": [
      "source_traceability"
    ],
    "missing_source_traceability": true,
    "missing_validity_dependency": false,
    "pit_eligibility_risk": "material_approximate_pit_evidence",
    "production_effect": "none",
    "remediation_action": "Create a source feature manifest for trend_features with upstream artifact, source cutoff, generated_at, checksum, and lineage fields.",
    "remediation_category": "source_traceability_required",
    "remediation_order": 3,
    "required_code_doc_config_change": "Add governed manifest/config reference and docs for trend_features; wire validator fixtures before implementation.",
    "required_upstream_artifact": "historical price trend / momentum windows",
    "requires_owner_review": false,
    "source_feature_name": "trend_features",
    "validation_requirement": "Focused validator must reject trend_features without generated_at, source cutoff, checksum, and lineage; pass only after manifest coverage exists."
  },
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "drawdown_features blocks contract-ready and PIT gate readiness until remediated.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "missing_as_of_semantics",
    "dependency_ordering": "phase_2_as_of_semantics",
    "feature_id": "drawdown_features",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": true,
    "missing_contract_dimension": [
      "as_of_semantics"
    ],
    "missing_source_traceability": false,
    "missing_validity_dependency": false,
    "pit_eligibility_risk": "material_approximate_pit_evidence",
    "production_effect": "none",
    "remediation_action": "Define explicit as-of semantics for drawdown_features, including decision-time availability and no-forward-window assertions.",
    "remediation_category": "as_of_semantics_required",
    "remediation_order": 4,
    "required_code_doc_config_change": "Add explicit as-of contract fields and replay-test fixtures for drawdown_features.",
    "required_upstream_artifact": "historical drawdown windows",
    "requires_owner_review": false,
    "source_feature_name": "drawdown_features",
    "validation_requirement": "As-of contract test must prove drawdown_features is known at decision time and does not use forward windows."
  },
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "volatility_inputs blocks contract-ready and PIT gate readiness until remediated.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "missing_as_of_semantics",
    "dependency_ordering": "phase_2_as_of_semantics",
    "feature_id": "volatility_inputs",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": true,
    "missing_contract_dimension": [
      "as_of_semantics"
    ],
    "missing_source_traceability": false,
    "missing_validity_dependency": false,
    "pit_eligibility_risk": "material_approximate_pit_evidence",
    "production_effect": "none",
    "remediation_action": "Define explicit as-of semantics for volatility_inputs, including decision-time availability and no-forward-window assertions.",
    "remediation_category": "as_of_semantics_required",
    "remediation_order": 5,
    "required_code_doc_config_change": "Add explicit as-of contract fields and replay-test fixtures for volatility_inputs.",
    "required_upstream_artifact": "rolling price-derived volatility features",
    "requires_owner_review": false,
    "source_feature_name": "volatility_inputs",
    "validation_requirement": "As-of contract test must prove volatility_inputs is known at decision time and does not use forward windows."
  },
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "execution_signal_validity_policy keeps growth_tilt_engine blocked and remains coupled to the valid_until_window blocker.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "blocked_unresolved",
    "dependency_ordering": "phase_3_validity_dependency",
    "feature_id": "execution_signal_validity_policy",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": false,
    "missing_contract_dimension": [
      "validity_dependency",
      "pit_gate_requirement"
    ],
    "missing_source_traceability": false,
    "missing_validity_dependency": true,
    "pit_eligibility_risk": "blocking_unknown_pit_status",
    "production_effect": "none",
    "remediation_action": "Bind execution_signal_validity_policy to the future valid-until contract and stale-signal handling boundary before PIT gate reconsideration.",
    "remediation_category": "validity_dependency_required",
    "remediation_order": 6,
    "required_code_doc_config_change": "Add validity dependency contract link for execution_signal_validity_policy; do not enable execution until valid_until_window remains separately remediated.",
    "required_upstream_artifact": "signal_validity_contract artifact and valid_until_window remediation result",
    "requires_owner_review": true,
    "source_feature_name": "execution_signal_validity_policy",
    "validation_requirement": "Validity contract test must prove execution_signal_validity_policy has valid_from, valid_until, stale_after, and carry-forward behavior before gate reconsideration."
  },
  {
    "blocker_downgraded_in_2411": false,
    "blocker_impact": "growth_tilt_engine_signal_artifact is a blocking summary artifact; it cannot downgrade until source feature and validity remediation evidence exists.",
    "blocks_contract_ready": true,
    "blocks_pit_gate": true,
    "broker_action": "none",
    "can_be_implemented_without_fresh_market_data": true,
    "current_mapping_status": "blocked_unresolved",
    "dependency_ordering": "phase_8_blocked_summary_review",
    "feature_id": "growth_tilt_engine_signal_artifact",
    "gap_resolved_in_2411": false,
    "missing_as_of_semantics": false,
    "missing_contract_dimension": [
      "prior_gap_remediation",
      "pit_gate_requirement"
    ],
    "missing_source_traceability": false,
    "missing_validity_dependency": false,
    "pit_eligibility_risk": "blocking_unknown_pit_status",
    "production_effect": "none",
    "remediation_action": "Keep growth_tilt_engine_signal_artifact blocked until upstream source feature and validity remediation items have passed validation.",
    "remediation_category": "blocked_pending_prior_remediation",
    "remediation_order": 7,
    "required_code_doc_config_change": "Track growth_tilt_engine_signal_artifact as blocked until prerequisite remediation items close.",
    "required_upstream_artifact": "completed prerequisite source-feature remediation evidence pack",
    "requires_owner_review": true,
    "source_feature_name": "growth_tilt_engine_signal_artifact",
    "validation_requirement": "Dependency test must keep growth_tilt_engine_signal_artifact unresolved until prerequisite source-feature and validity items pass."
  }
]
```

## Validation Design

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_search_enabled": false,
  "covered_remediation_categories": [
    "source_traceability_required",
    "as_of_semantics_required",
    "validity_dependency_required",
    "blocked_pending_prior_remediation"
  ],
  "engine_id": "growth_tilt_engine",
  "observation_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_engine_contract_gap_validation_design.v1",
  "validation_goal": "prove contract evidence before any blocker downgrade review",
  "validation_stages": [
    {
      "acceptance": "all source_traceability_required items have manifest, checksum, generated_at, and source cutoff",
      "required_before": "as_of_semantics_remediation",
      "stage_id": "source_traceability_manifest_validation"
    },
    {
      "acceptance": "as-of rows prove decision-time availability and no forward-window usage",
      "required_before": "validity_dependency_remediation",
      "stage_id": "as_of_semantics_contract_validation"
    },
    {
      "acceptance": "valid_from, valid_until, stale_after, and carry-forward rules are explicit",
      "required_before": "pit_gate_reconsideration",
      "stage_id": "validity_dependency_contract_validation"
    },
    {
      "acceptance": "PIT gate remains blocking until all contract evidence passes",
      "required_before": "owner_downgrade_review",
      "stage_id": "pit_gate_dry_run_validation"
    }
  ]
}
```

## Unresolved Blocker Summary

```json
{
  "blocked_feature_ids": [
    "equal_risk_baseline_weights",
    "target_vol_policy",
    "trend_features",
    "drawdown_features",
    "volatility_inputs",
    "execution_signal_validity_policy",
    "growth_tilt_engine_signal_artifact"
  ],
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_search_enabled": false,
  "engine_id": "growth_tilt_engine",
  "growth_tilt_engine_blocker_downgraded": false,
  "growth_tilt_engine_blocker_resolved": false,
  "observation_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "recommended_next_task": "TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation",
  "remediation_item_count": 7,
  "schema_version": "growth_tilt_engine_unresolved_blocker_summary.v1",
  "valid_until_window_blocker_downgraded": false,
  "valid_until_window_blocker_resolved": false
}
```

## Data Quality Boundary

- data_quality_gate_executed：`False`
- data_quality_gate_reason：`NOT_APPLICABLE_CONTRACT_GAP_REMEDIATION_PLAN_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## Safety Boundary

- growth_tilt_engine_blocker_resolved：`False`
- growth_tilt_engine_blocker_downgraded：`False`
- valid_until_window_blocker_resolved：`False`
- valid_until_window_blocker_downgraded：`False`
- candidate_search_enabled：`False`
- observation_enabled：`False`
- paper_shadow_enabled：`False`
- production_enabled：`False`
- broker_enabled：`False`