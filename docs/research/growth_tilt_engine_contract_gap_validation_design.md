# Growth tilt engine contract gap validation design

- status：`GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED`
- validation design ready：`True`
- next route：`TRADING-2412_Growth_Tilt_Engine_As_Of_Semantics_Remediation`

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