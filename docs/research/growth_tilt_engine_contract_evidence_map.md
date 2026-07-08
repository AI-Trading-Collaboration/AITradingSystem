# Growth Tilt Engine Contract Evidence Map

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "evidence_row_count": 11,
  "evidence_rows": [
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "pit_gate_ready": true,
        "pit_gate_ready_count": 1,
        "status": "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_REMEDIATION_READY"
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "pit_gate_ready_after_2421",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "remaining_blocker_count": 0,
        "remaining_blockers": []
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "remaining_pit_blockers_closed",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "2420_remediation_status": "READY",
        "2420_status": "GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY",
        "2421_recheck_status": "ACCEPTED",
        "incomplete_field_count": 0,
        "lineage_dependency_count": 8,
        "manifest_traceability_status": "READY",
        "missing_field_count": 0,
        "unresolved_blocker_count": 0
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "source_traceability_remediation_ready",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "missing_contract_evidence",
      "evidence": {
        "missing_report_ids": []
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "report_registry_registered",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "missing_contract_evidence",
      "evidence": {
        "missing_catalog_references": []
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "artifact_catalog_registered",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "missing_contract_evidence",
      "evidence": {
        "missing_system_flow_references": []
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "system_flow_registered",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "missing_contract_evidence",
      "evidence": {
        "missing_research_docs": []
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "research_docs_registered",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "2420_paper_shadow_enabled": false,
        "2421_paper_shadow_enabled": false
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "paper_shadow_boundary_disabled",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "2420_production_enabled": false,
        "2421_production_enabled": false
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "production_boundary_disabled",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "2420_broker_action": "none",
        "2420_broker_enabled": false,
        "2421_broker_action": "none",
        "2421_broker_enabled": false
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "broker_boundary_disabled",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    },
    {
      "broker_action": "none",
      "classification": "incomplete_contract_field",
      "evidence": {
        "2420_manual_review_required": true,
        "2421_manual_review_required": true
      },
      "evidence_status": "PASS",
      "production_effect": "none",
      "requirement_id": "manual_review_boundary_required",
      "source_tasks": [
        "TRADING-2420",
        "TRADING-2421"
      ]
    }
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_contract_evidence_map.v1",
  "status": "GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY",
  "target_strategy_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
}
```