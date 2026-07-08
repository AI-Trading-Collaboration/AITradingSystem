# Growth Tilt Engine Signal Artifact Source Traceability Manifest（证据链 Manifest）

```json
{
  "artifact_id": "growth_tilt_engine_signal_artifact",
  "as_of": "2026-07-08",
  "broker_action": "none",
  "contract_ready_after_2420": false,
  "dependency_closure_reference": {
    "artifact_path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/valid_until_dependency_evidence.json",
    "dependency_feature_id": "execution_signal_validity_policy",
    "dependency_id": "growth_tilt_engine:execution_signal_validity_policy:signal_validity_dependency:v1",
    "evidence_status": "CLOSED_WITH_EVIDENCE",
    "ready_for_pit_gate_recheck": true,
    "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
    "source_reference": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
    "source_task": "TRADING-2418"
  },
  "pit_gate_ready_after_2420": false,
  "prior_blocker_classification": {
    "blocker_classification": "source_traceability",
    "recommended_next_task": "TRADING-2420_Growth_Tilt_Engine_Signal_Artifact_Source_Traceability_Remediation",
    "remaining_blocker": true,
    "source_task": "TRADING-2419",
    "status": "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY",
    "still_blocked_after_recheck": true
  },
  "prior_missing_evidence_reference": {
    "prior_still_blocked_reason": "missing standalone growth_tilt_engine signal artifact with source config, owner module, generated_at, source_data_cutoff, feature version and source snapshot evidence",
    "prior_traceability_closure_status": "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT",
    "prior_upstream_artifact_closure_status": "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT",
    "source_artifact_after_2417": null,
    "source_task": "TRADING-2417",
    "source_traceability_evidence_ready_before_2420": false,
    "upstream_artifact_evidence_ready_before_2420": false
  },
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_signal_artifact_source_traceability_manifest.v1",
  "source_artifacts": [
    {
      "artifact_id": "growth_tilt_engine_pit_gate_readiness_recheck_result",
      "catalog_reference_present": true,
      "evidence_type": "pit_gate_readiness_recheck_blocker_state",
      "path": "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/readiness_recheck_result.json",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
      "report_registry_present": true,
      "source_file_checksum": "sha256:ee8155433f6336d5bf0ca07be7fd0daf4ba49154b6fbe40db54a579aed71d432",
      "source_file_present": true,
      "source_task": "TRADING-2419"
    },
    {
      "artifact_id": "growth_tilt_engine_pit_gate_recheck_blocker_classification",
      "catalog_reference_present": true,
      "evidence_type": "blocker_classification",
      "path": "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/blocker_classification.json",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
      "report_registry_present": true,
      "source_file_checksum": "sha256:574bddd8acf387cacc08c373347de27126e1bfd2d84d83a8bc10d0e39185e5d4",
      "source_file_present": true,
      "source_task": "TRADING-2419"
    },
    {
      "artifact_id": "growth_tilt_engine_valid_until_dependency_evidence",
      "catalog_reference_present": true,
      "evidence_type": "valid_until_dependency_closure",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/valid_until_dependency_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "report_registry_present": true,
      "source_file_checksum": "sha256:f1e5730ef6d31b309ec279241429360ffe7035a468b421801898038b308fadfa",
      "source_file_present": true,
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_signal_validity_contract_evidence",
      "catalog_reference_present": true,
      "evidence_type": "signal_validity_contract",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/signal_validity_contract_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "report_registry_present": true,
      "source_file_checksum": "sha256:95c50567acc8492513fd767899e91c23185be78831c5b8c44637f6094c8e6b6b",
      "source_file_present": true,
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_stale_signal_policy_evidence",
      "catalog_reference_present": true,
      "evidence_type": "stale_signal_policy",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/stale_signal_policy_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "report_registry_present": true,
      "source_file_checksum": "sha256:8e5350dab166178b309396ad550a53c783ded60c6258201f6497720afa7034f9",
      "source_file_present": true,
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_valid_until_alignment_evidence",
      "catalog_reference_present": true,
      "evidence_type": "growth_tilt_valid_until_alignment",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/growth_tilt_valid_until_alignment_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "report_registry_present": true,
      "source_file_checksum": "sha256:8074645164ad7a66ff26fb4a82f70142633d1548fece4751668a6cb0bf578fac",
      "source_file_present": true,
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_source_traceability_closure_evidence",
      "catalog_reference_present": true,
      "evidence_type": "source_traceability_pre_recheck_closure",
      "path": "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/source_traceability_closure_evidence.json",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
      "report_registry_present": true,
      "source_file_checksum": "sha256:915047c54e5a1deee47af42ab2e5924e1b96627d649a4438b1d6ac5eb2619f14",
      "source_file_present": true,
      "source_task": "TRADING-2417"
    },
    {
      "artifact_id": "growth_tilt_engine_upstream_artifact_closure_evidence",
      "catalog_reference_present": true,
      "evidence_type": "upstream_artifact_pre_recheck_closure",
      "path": "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/upstream_artifact_closure_evidence.json",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
      "report_registry_present": true,
      "source_file_checksum": "sha256:d5507308a7d7beeb47126713d220222f33d7f218c51838ae1acd825338983986",
      "source_file_present": true,
      "source_task": "TRADING-2417"
    }
  ],
  "source_documents": [
    {
      "catalog_reference_present": true,
      "document_id": "growth_tilt_engine_pit_gate_readiness_recheck_doc",
      "document_present": true,
      "path": "docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
      "report_registry_present": true,
      "source_task": "TRADING-2419"
    },
    {
      "catalog_reference_present": true,
      "document_id": "growth_tilt_engine_signal_artifact_blocker_doc",
      "document_present": true,
      "path": "docs/research/growth_tilt_engine_signal_artifact_source_traceability_blocker.md",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
      "report_registry_present": true,
      "source_task": "TRADING-2419"
    },
    {
      "catalog_reference_present": true,
      "document_id": "growth_tilt_engine_valid_until_dependency_evidence_doc",
      "document_present": true,
      "path": "docs/research/growth_tilt_engine_valid_until_dependency_evidence_closure.md",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "report_registry_present": true,
      "source_task": "TRADING-2418"
    },
    {
      "catalog_reference_present": true,
      "document_id": "growth_tilt_engine_source_traceability_closure_evidence_doc",
      "document_present": true,
      "path": "docs/research/growth_tilt_engine_source_traceability_closure_evidence.md",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
      "report_registry_present": true,
      "source_task": "TRADING-2417"
    },
    {
      "catalog_reference_present": true,
      "document_id": "growth_tilt_engine_upstream_artifact_closure_evidence_doc",
      "document_present": true,
      "path": "docs/research/growth_tilt_engine_upstream_artifact_closure_evidence.md",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
      "report_registry_present": true,
      "source_task": "TRADING-2417"
    }
  ],
  "source_evidence_type": "standalone_signal_artifact_traceability_manifest",
  "source_generation_commands": [
    {
      "broker_action": "none",
      "catalog_reference_present": true,
      "command": "aits research strategies growth-tilt-engine-signal-artifact-source-traceability-remediation",
      "production_effect": "none",
      "report_id": "growth_tilt_engine_signal_artifact_source_traceability_remediation"
    },
    {
      "broker_action": "none",
      "catalog_reference_present": true,
      "command": "aits research strategies growth-tilt-engine-pit-gate-readiness-recheck",
      "production_effect": "none",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck"
    },
    {
      "broker_action": "none",
      "catalog_reference_present": true,
      "command": "aits research strategies growth-tilt-engine-valid-until-dependency-evidence-closure",
      "production_effect": "none",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure"
    },
    {
      "broker_action": "none",
      "catalog_reference_present": true,
      "command": "aits research strategies growth-tilt-engine-source-traceability-upstream-artifact-closure",
      "production_effect": "none",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure"
    }
  ],
  "source_registry_entries": [
    {
      "artifact_globs": [
        "outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/remediation_result.json",
        "outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/source_traceability_manifest.json",
        "outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/source_lineage_map.json",
        "outputs/research_strategies/growth_tilt_engine_signal_artifact_source_traceability_remediation/missing_source_evidence_summary.json",
        "docs/research/growth_tilt_engine_signal_artifact_source_traceability_remediation.md",
        "docs/research/growth_tilt_engine_signal_artifact_source_traceability_manifest.md",
        "docs/research/growth_tilt_engine_signal_artifact_source_lineage_map.md",
        "docs/research/dynamic_strategy_2421_route.md"
      ],
      "broker_action": "none",
      "command": "aits research strategies growth-tilt-engine-signal-artifact-source-traceability-remediation",
      "command_catalog_reference_present": true,
      "production_effect": "none",
      "registry_present": true,
      "report_id": "growth_tilt_engine_signal_artifact_source_traceability_remediation"
    },
    {
      "artifact_globs": [
        "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/readiness_recheck_result.json",
        "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/pit_gate_recheck_matrix.json",
        "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/blocker_classification.json",
        "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/remaining_blocker_summary.json",
        "docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md",
        "docs/research/growth_tilt_engine_pit_gate_recheck_matrix.md",
        "docs/research/growth_tilt_engine_signal_artifact_source_traceability_blocker.md",
        "docs/research/dynamic_strategy_2420_route.md"
      ],
      "broker_action": "none",
      "command": "aits research strategies growth-tilt-engine-pit-gate-readiness-recheck",
      "command_catalog_reference_present": true,
      "production_effect": "none",
      "registry_present": true,
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck"
    },
    {
      "artifact_globs": [
        "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/closure_result.json",
        "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/valid_until_dependency_evidence.json",
        "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/signal_validity_contract_evidence.json",
        "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/stale_signal_policy_evidence.json",
        "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/growth_tilt_valid_until_alignment_evidence.json",
        "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/remaining_blocker_summary.json",
        "docs/research/growth_tilt_engine_valid_until_dependency_evidence_closure.md",
        "docs/research/growth_tilt_engine_signal_validity_contract_evidence.md",
        "docs/research/growth_tilt_engine_stale_signal_policy_evidence.md",
        "docs/research/growth_tilt_engine_valid_until_alignment_evidence.md",
        "docs/research/dynamic_strategy_2419_route.md"
      ],
      "broker_action": "none",
      "command": "aits research strategies growth-tilt-engine-valid-until-dependency-evidence-closure",
      "command_catalog_reference_present": true,
      "production_effect": "none",
      "registry_present": true,
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure"
    },
    {
      "artifact_globs": [
        "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/closure_result.json",
        "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/source_traceability_closure_evidence.json",
        "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/upstream_artifact_closure_evidence.json",
        "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/updated_source_feature_mapping.json",
        "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/remaining_blocker_summary.json",
        "docs/research/growth_tilt_engine_source_traceability_upstream_artifact_closure.md",
        "docs/research/growth_tilt_engine_source_traceability_closure_evidence.md",
        "docs/research/growth_tilt_engine_upstream_artifact_closure_evidence.md",
        "docs/research/growth_tilt_engine_updated_source_feature_mapping.md",
        "docs/research/dynamic_strategy_2418_route.md"
      ],
      "broker_action": "none",
      "command": "aits research strategies growth-tilt-engine-source-traceability-upstream-artifact-closure",
      "command_catalog_reference_present": true,
      "production_effect": "none",
      "registry_present": true,
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure"
    }
  ],
  "source_timestamp_boundary": {
    "as_of": "2026-07-08",
    "fresh_market_data_read": false,
    "generated_at": "2026-07-08T14:12:43Z",
    "source_data_cutoff": "2026-07-08"
  },
  "traceability_status": "READY",
  "valid_until_boundary": {
    "boundary_explicit": true,
    "broker_action": "none",
    "dependency_id": "growth_tilt_engine:execution_signal_validity_policy:signal_validity_dependency:v1",
    "execution_lag_bdays": 1,
    "growth_tilt_alignment_ready": true,
    "missing_field_count": 0,
    "policy_window_bdays": 10,
    "production_effect": "none",
    "remaining_gap": "growth_tilt_engine_signal_artifact remains source-traceability blocked; PIT recheck must keep this blocker until signal artifact metadata exists",
    "required_fields": [
      "signal_id",
      "signal_version",
      "as_of_date",
      "generated_at",
      "source_data_cutoff",
      "valid_from",
      "valid_until",
      "stale_after",
      "horizon_days",
      "expiry_rule",
      "carry_forward_rule",
      "near_expiry_rule",
      "signal_to_execution_lag_rule"
    ],
    "stale_after_source": "valid_until_or_earlier_decay_boundary",
    "stale_policy_ready": true,
    "valid_from_source": "not emitted per signal; policy says next_trading_day",
    "valid_until_source": "policy window=10 bdays; per-signal field missing"
  }
}
```