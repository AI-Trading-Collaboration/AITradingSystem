# Growth tilt engine signal validity contract evidence

本文件记录 valid_from / valid_until / stale_after / expiry / lag 等字段的
contract evidence。Evidence ready 不表示 standalone signal artifact ready。

```json
{
  "auto_mark_contract_ready": false,
  "broker_action": "none",
  "decision_policy": {
    "current_date > stale_after": "BLOCK_OR_DECAY_SIGNAL",
    "current_date > valid_until": "BLOCK_EXECUTION",
    "missing valid_until": "BLOCK_CANDIDATE_SEARCH_FOR_DEPENDENT_STRATEGY",
    "near valid_until": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
    "new signal overlaps old": "USE_NEWER_SIGNAL_IF_AS_OF_SAFE_AND_VALID"
  },
  "engine_id": "growth_tilt_engine",
  "evidence_available_count": 13,
  "field_evidence_rows": [
    {
      "evidence_available": true,
      "evidence_source": "growth_tilt_engine",
      "field": "signal_id",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "deterministic_signal_version",
      "field": "signal_version",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "YYYY-MM-DD",
      "field": "as_of_date",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "YYYY-MM-DDTHH:MM:SSZ",
      "field": "generated_at",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "YYYY-MM-DD",
      "field": "source_data_cutoff",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "generated_at_or_next_executable_time",
      "field": "valid_from",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "valid_from + governed_horizon(max_policy=10)",
      "field": "valid_until",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "valid_until_or_earlier_decay_boundary",
      "field": "stale_after",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "TBD_FROM_SIGNAL_HORIZON",
      "field": "horizon_days",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "BLOCK_AFTER_VALID_UNTIL",
      "field": "expiry_rule",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_SIGNAL_VALIDITY_CONTRACT_PLAN"
    },
    {
      "evidence_available": true,
      "evidence_source": "hold_previous_actual_position",
      "field": "carry_forward_rule",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_VALID_UNTIL_SEMANTICS_REVIEW"
    },
    {
      "evidence_available": true,
      "evidence_source": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
      "field": "near_expiry_rule",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "TRADING-2407_VALID_UNTIL_SEMANTICS_REVIEW"
    },
    {
      "evidence_available": true,
      "evidence_source": "execution_lag_bdays=1",
      "field": "signal_to_execution_lag_rule",
      "implementation_status": "DEFINED_FOR_RECHECK_NOT_SIGNAL_ARTIFACT_READY",
      "source_task": "CONFIG_STRATEGY_EXECUTION_POLICY_REGISTRY"
    }
  ],
  "invariants": [
    "valid_from >= generated_at_or_next_executable_time",
    "valid_until > valid_from",
    "valid_until <= valid_from + max_allowed_horizon",
    "stale_after <= valid_until",
    "expired_signal_cannot_trigger_new_trade",
    "expired_signal_cannot_be_carried_forward_without_explicit_owner_approved_rule",
    "signal_to_execution_lag_must_be_recorded"
  ],
  "missing_field_count": 0,
  "pit_gate_recheck_required": true,
  "production_effect": "none",
  "ready_for_recheck": true,
  "required_field_count": 13,
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
  "schema_version": "growth_tilt_engine_signal_validity_contract_evidence.v1",
  "signal_id": "growth_tilt_engine",
  "source_policy_context": {
    "execution_lag_bdays": 1,
    "signal_effective_earliest": "next_trading_day",
    "signal_validity_window_bdays": 10,
    "stale_signal_behavior": "hold_previous_actual_position"
  },
  "target_strategy_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
}
```
