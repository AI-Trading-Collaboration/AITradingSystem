# Growth Tilt Engine Signal Artifact Source Lineage Map（来源链路图）

```json
{
  "artifact_id": "growth_tilt_engine_signal_artifact",
  "broker_action": "none",
  "broker_enabled": false,
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
  "downstream_consumers": [
    "growth_tilt_engine_pit_gate_readiness",
    "TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_After_Source_Traceability_Remediation"
  ],
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_engine_signal_artifact_source_lineage_map.v1",
  "source_documents": [
    {
      "document_id": "growth_tilt_engine_pit_gate_readiness_recheck_doc",
      "path": "docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md",
      "source_task": "TRADING-2419"
    },
    {
      "document_id": "growth_tilt_engine_signal_artifact_blocker_doc",
      "path": "docs/research/growth_tilt_engine_signal_artifact_source_traceability_blocker.md",
      "source_task": "TRADING-2419"
    },
    {
      "document_id": "growth_tilt_engine_valid_until_dependency_evidence_doc",
      "path": "docs/research/growth_tilt_engine_valid_until_dependency_evidence_closure.md",
      "source_task": "TRADING-2418"
    },
    {
      "document_id": "growth_tilt_engine_source_traceability_closure_evidence_doc",
      "path": "docs/research/growth_tilt_engine_source_traceability_closure_evidence.md",
      "source_task": "TRADING-2417"
    },
    {
      "document_id": "growth_tilt_engine_upstream_artifact_closure_evidence_doc",
      "path": "docs/research/growth_tilt_engine_upstream_artifact_closure_evidence.md",
      "source_task": "TRADING-2417"
    }
  ],
  "upstream_dependencies": [
    {
      "artifact_id": "growth_tilt_engine_pit_gate_readiness_recheck_result",
      "evidence_type": "pit_gate_readiness_recheck_blocker_state",
      "path": "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/readiness_recheck_result.json",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
      "source_task": "TRADING-2419"
    },
    {
      "artifact_id": "growth_tilt_engine_pit_gate_recheck_blocker_classification",
      "evidence_type": "blocker_classification",
      "path": "outputs/research_strategies/growth_tilt_engine_pit_gate_readiness_recheck/blocker_classification.json",
      "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
      "source_task": "TRADING-2419"
    },
    {
      "artifact_id": "growth_tilt_engine_valid_until_dependency_evidence",
      "evidence_type": "valid_until_dependency_closure",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/valid_until_dependency_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_signal_validity_contract_evidence",
      "evidence_type": "signal_validity_contract",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/signal_validity_contract_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_stale_signal_policy_evidence",
      "evidence_type": "stale_signal_policy",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/stale_signal_policy_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_valid_until_alignment_evidence",
      "evidence_type": "growth_tilt_valid_until_alignment",
      "path": "outputs/research_strategies/growth_tilt_engine_valid_until_dependency_evidence_closure/growth_tilt_valid_until_alignment_evidence.json",
      "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
      "source_task": "TRADING-2418"
    },
    {
      "artifact_id": "growth_tilt_engine_source_traceability_closure_evidence",
      "evidence_type": "source_traceability_pre_recheck_closure",
      "path": "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/source_traceability_closure_evidence.json",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
      "source_task": "TRADING-2417"
    },
    {
      "artifact_id": "growth_tilt_engine_upstream_artifact_closure_evidence",
      "evidence_type": "upstream_artifact_pre_recheck_closure",
      "path": "outputs/research_strategies/growth_tilt_engine_source_traceability_upstream_artifact_closure/upstream_artifact_closure_evidence.json",
      "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
      "source_task": "TRADING-2417"
    }
  ],
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