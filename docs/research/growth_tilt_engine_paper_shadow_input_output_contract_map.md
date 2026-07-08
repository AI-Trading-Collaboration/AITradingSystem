# Growth Tilt Engine Paper Shadow Input Output Contract Map

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "input_contract_count": 6,
  "input_contract_map": [
    {
      "artifact_id": "growth_tilt_engine_signal_artifact",
      "contract_id": "source_traceability_artifact_chain",
      "required": true,
      "resolved": true,
      "source_task": "TRADING-2420"
    },
    {
      "contract_id": "pit_gate_readiness_state",
      "expected_ready": true,
      "required": true,
      "resolved": true,
      "source_task": "TRADING-2421"
    },
    {
      "contract_id": "contract_readiness_snapshot",
      "expected_gap_count": 0,
      "required": true,
      "resolved": true,
      "source_task": "TRADING-2422"
    },
    {
      "contract_id": "paper_shadow_preflight_state",
      "expected_ready": true,
      "required": true,
      "resolved": true,
      "source_task": "TRADING-2423"
    },
    {
      "contract_id": "paper_shadow_enablement_plan_state",
      "expected_gap_count": 0,
      "required": true,
      "resolved": true,
      "source_task": "TRADING-2424"
    },
    {
      "contract_id": "manual_review_boundary_state",
      "manual_review_required": true,
      "required": true,
      "resolved": true,
      "source_task": "TRADING-2424"
    }
  ],
  "input_contract_map_ready": true,
  "output_artifact_contract_count": 6,
  "output_artifact_contract_map": [
    {
      "broker_action": "none",
      "contract_id": "dry_run_wiring_result",
      "production_effect": "none",
      "required": true,
      "resolved": true
    },
    {
      "broker_action": "none",
      "contract_id": "input_output_contract_map",
      "production_effect": "none",
      "required": true,
      "resolved": true
    },
    {
      "broker_action": "none",
      "contract_id": "runtime_boundary_manifest",
      "production_effect": "none",
      "required": true,
      "resolved": true
    },
    {
      "broker_action": "none",
      "contract_id": "schedule_hook_disabled_verification",
      "production_effect": "none",
      "required": true,
      "resolved": true
    },
    {
      "broker_action": "none",
      "contract_id": "manual_review_handoff_wiring_plan",
      "production_effect": "none",
      "required": true,
      "resolved": true
    },
    {
      "broker_action": "none",
      "contract_id": "dry_run_no_effect_audit_summary",
      "production_effect": "none",
      "required": true,
      "resolved": true
    }
  ],
  "output_artifact_contract_map_ready": true,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_paper_shadow_input_output_contract_map.v1",
  "status": "GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_READY",
  "target_strategy_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
}
```