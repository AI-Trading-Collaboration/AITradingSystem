# Growth tilt engine valid-until dependency evidence closure

## 摘要

- task_id：`TRADING-2418`
- status：`GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY`
- market regime：`ai_after_chatgpt`
- valid_until dependency blocker count：`1`
- valid_until evidence ready：`True`
- signal validity contract evidence ready：`True`
- stale signal policy evidence ready：`True`
- growth tilt / valid-until alignment evidence ready：`True`
- source traceability still blocked：`['growth_tilt_engine_signal_artifact']`
- PIT gate ready count：`0`
- contract-ready count：`0`

TRADING-2418 只把 `execution_signal_validity_policy` 的
`valid_until_window` dependency blocker 转成可供 TRADING-2419 读取的
pre-recheck evidence。它不标记 PIT gate ready、不标记 contract ready、
不解除或降级任何 blocker。`growth_tilt_engine_signal_artifact` 的
source traceability blocker 继续保留。

## Source findings from TRADING-2417 / 2416 / 2415

```json
{
  "blocked_by_source_traceability_count": 5,
  "blocked_by_valid_until_window_count": 1,
  "contract_ready_count": 0,
  "pit_gate_blocked_count": 10,
  "pit_gate_ready_count": 0,
  "source_feature_count": 10,
  "source_traceability_still_blocked": [
    "growth_tilt_engine_signal_artifact"
  ]
}
```

## Valid-until dependency evidence

```json
{
  "auto_mark_contract_ready": false,
  "auto_mark_pit_gate_ready": false,
  "broker_action": "none",
  "dependency_feature_id": "execution_signal_validity_policy",
  "dependent_feature_ids": [
    "execution_signal_validity_policy"
  ],
  "engine_id": "growth_tilt_engine",
  "evidence_rows": [
    {
      "auto_mark_contract_ready": false,
      "auto_mark_pit_gate_ready": false,
      "before_status_from_2416": {
        "blocked_by_valid_until_window": true,
        "current_pit_gate_status": "pit_gate_blocked_by_valid_until_window",
        "required_closure_evidence": [
          "as_of_date",
          "generated_at",
          "source_data_cutoff",
          "valid_from",
          "valid_until",
          "stale_signal_policy",
          "signal_version",
          "signal_validity_contract",
          "feature_version",
          "PIT_gate_checker_regenerated"
        ],
        "valid_until_available": false,
        "valid_until_required": true,
        "validity_dependency_status": "blocked"
      },
      "broker_action": "none",
      "carry_forward_rule_source": "hold_previous_actual_position",
      "contract_metadata_from_2414": {
        "expiration_policy": "valid_until_window_required_before_expiration_can_be_evaluated",
        "staleness_policy": "blocked_pending_valid_until_window_contract",
        "valid_until_available": false,
        "valid_until_required": true,
        "validity_basis": "depends_on_valid_until_window_contract",
        "validity_blocking_reason": "valid_until_window_unresolved",
        "validity_end_reference": "valid_until_window"
      },
      "contract_ready_after_2418": false,
      "dependency_id": "growth_tilt_engine:execution_signal_validity_policy:signal_validity_dependency:v1",
      "dependency_type": [
        "SIGNAL_VALIDITY_CONTRACT",
        "VALID_FROM_MAPPING",
        "VALID_UNTIL_MAPPING",
        "STALE_AFTER_MAPPING",
        "EXPIRY_RULE",
        "CARRY_FORWARD_RULE",
        "SIGNAL_TO_EXECUTION_LAG",
        "NEAR_EXPIRY_POLICY",
        "GROWTH_TILT_HORIZON_ALIGNMENT"
      ],
      "dependent_feature_or_signal": "execution_signal_validity_policy",
      "evidence_status": "CLOSED_WITH_EVIDENCE",
      "execution_lag_bdays": 1,
      "expiry_rule_source": "signal_validity_window_bdays exists but natural signal expiry is not derived from signal horizon",
      "growth_tilt_horizon_alignment_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
      "near_expiry_rule_source": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
      "pit_gate_ready_after_2418": false,
      "policy_window_bdays": 10,
      "production_effect": "none",
      "ready_for_pit_gate_recheck": true,
      "remaining_gap": "evidence is ready for PIT gate recheck, but valid_until_window blocker and PIT/contract readiness remain unchanged until TRADING-2419 and owner review",
      "signal_to_execution_lag_source": "execution_lag_bdays=1",
      "source_reference": "config/research/strategy_execution_policy_registry.yaml:equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1.signal_policy",
      "source_system": "governed_config",
      "stale_after_source": "valid_until_or_earlier_decay_boundary",
      "valid_from_source": "not emitted per signal; policy says next_trading_day",
      "valid_until_source": "policy window=10 bdays; per-signal field missing"
    }
  ],
  "evidence_scope": "pre_recheck_valid_until_dependency_evidence",
  "pit_gate_recheck_required": true,
  "pre_recheck_evidence_ready_count": 1,
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_valid_until_dependency_evidence.v1",
  "still_blocked_count": 0,
  "valid_until_window_dependency_blocker_count_from_2415": 1
}
```

## Signal validity contract evidence

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

## Stale signal policy evidence

```json
{
  "broker_action": "none",
  "deterministic_next_executable_time_policy_ready": true,
  "engine_id": "growth_tilt_engine",
  "policy_evidence_rows": [
    {
      "carry_forward_requires_owner_approval": true,
      "carry_forward_rule": "hold_previous_actual_position",
      "evidence_status": "CLOSED_WITH_EVIDENCE",
      "expired_signal_execution_rule": "BLOCK_EXECUTION",
      "near_expiry_rule": "APPLY_NEAR_EXPIRY_DECAY_OR_REQUIRE_REFRESH",
      "policy_id": "growth_tilt_engine_valid_until_window_no_stale_policy_v1",
      "remaining_gap": "replay validation and owner review are still required before any blocker downgrade or observation approval",
      "risk_rows": [
        {
          "affected_semantic_or_signal": "no_stale_signal_carry_forward",
          "category": "CARRY_FORWARD_RISK",
          "evidence": "hold_previous_actual_position can carry stale exposure without owner rule",
          "recommended_fix": "block expired carry-forward or require owner-approved rule",
          "remediation_required": true,
          "risk_id": "VUW-STALE-002",
          "severity": "BLOCKING"
        },
        {
          "affected_semantic_or_signal": "signal_to_execution_lag",
          "category": "SIGNAL_TO_EXECUTION_LAG_RISK",
          "evidence": "prior review observed lag_days=1.0; replay contract missing",
          "recommended_fix": "record lag for every signal-to-execution decision",
          "remediation_required": true,
          "risk_id": "VUW-STALE-003",
          "severity": "MATERIAL"
        },
        {
          "affected_semantic_or_signal": "valid_until_window",
          "category": "NEAR_EXPIRY_OVERTRADING_RISK",
          "evidence": "near-expiry signal behavior is not separately validated",
          "recommended_fix": "define near-expiry decay, block, or refresh-required behavior",
          "remediation_required": true,
          "risk_id": "VUW-STALE-004",
          "severity": "MATERIAL"
        },
        {
          "affected_semantic_or_signal": "signal_version",
          "category": "SIGNAL_REFRESH_COLLISION_RISK",
          "evidence": "new signal overlapping old signal lacks deterministic replacement rule",
          "recommended_fix": "prefer newer as-of-safe valid signal and log collision decision",
          "remediation_required": true,
          "risk_id": "VUW-STALE-005",
          "severity": "MATERIAL"
        }
      ],
      "signal_refresh_collision_rule": "USE_NEWER_SIGNAL_IF_AS_OF_SAFE_AND_VALID",
      "stale_after_rule": "BLOCK_OR_DECAY_SIGNAL",
      "validation_plan_stale_replay": [
        "expired signals do not execute",
        "signal-to-execution lag is measured",
        "near-expiry handling is deterministic",
        "carry-forward is logged or blocked"
      ]
    }
  ],
  "production_effect": "none",
  "ready_for_recheck": true,
  "replay_validation_required": true,
  "required_policy_invariants": {
    "carry_forward_requires_explicit_rule": true,
    "expired_signal_cannot_trigger_new_trade": true,
    "missing_valid_until_blocks_dependent_strategy_recheck": true,
    "owner_review_required_for_carry_forward_in_observation_context": true
  },
  "schema_version": "growth_tilt_engine_stale_signal_policy_evidence.v1",
  "signal_to_execution_lag_policy_ready": true,
  "stale_carry_forward_policy_ready": true
}
```

## Growth tilt / valid-until alignment evidence

```json
{
  "alignment_rows": [
    {
      "alignment_questions": [
        "what growth_tilt horizon should valid_until derive from",
        "should valid_until shrink for weak confidence or high volatility",
        "should strong growth tilt use longer validity than weak growth tilt",
        "should recovery regimes require more conservative expiry",
        "how should lag reduce executable remaining validity"
      ],
      "alignment_status": "BLOCKED_BY_SOURCE_SIGNAL_ARTIFACT",
      "confidence_to_expiry_mapping_available": true,
      "contract_ready_after_2418": false,
      "growth_tilt_signal_horizon_source": "VALID_UNTIL_WINDOW_RESEARCH_POLICY",
      "growth_tilt_signal_id": "growth_tilt_engine",
      "high_volatility_expiry_adjustment_available": false,
      "horizon_to_valid_until_mapping_available": true,
      "pit_gate_ready_after_2418": false,
      "ready_for_pit_gate_recheck": true,
      "recovery_state_expiry_adjustment_available": false,
      "remaining_gap": "growth_tilt_engine_signal_artifact remains source-traceability blocked; PIT recheck must keep this blocker until signal artifact metadata exists",
      "valid_until_window_source": "not derived from growth tilt horizon"
    }
  ],
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "production_effect": "none",
  "proposed_confidence_to_expiry_mapping": [
    {
      "confidence_band": "LOW_OR_MISSING",
      "expiry_policy": "shorten validity or block until confidence exists"
    },
    {
      "confidence_band": "MEDIUM",
      "expiry_policy": "use base horizon with near-expiry refresh requirement"
    },
    {
      "confidence_band": "HIGH",
      "expiry_policy": "allow base horizon only if replay validates no stale carry"
    }
  ],
  "proposed_horizon_to_valid_until_mapping": [
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "short_growth_tilt",
      "valid_until_rule": "valid_from + short governed horizon"
    },
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "medium_growth_tilt",
      "valid_until_rule": "valid_from + medium governed horizon"
    },
    {
      "requires_owner_calibration": true,
      "signal_horizon_class": "persistent_growth_tilt",
      "valid_until_rule": "valid_from + capped persistent horizon"
    }
  ],
  "ready_for_recheck": true,
  "schema_version": "growth_tilt_engine_valid_until_alignment_evidence.v1",
  "source_traceability_still_blocked": [
    "growth_tilt_engine_signal_artifact"
  ]
}
```

## Remaining blocker summary

```json
{
  "blocked_by_source_traceability_count": 5,
  "blocked_by_valid_until_window_count": 1,
  "blocker_category_rows": [
    {
      "before_count": 1,
      "blocker_category": "SOURCE_TRACEABILITY_GAP",
      "closure_evidence_added_count": 0,
      "known_still_blocked_feature_ids": [
        "growth_tilt_engine_signal_artifact"
      ],
      "recommended_next_task": "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
      "requires_owner_review_count": 1,
      "requires_pit_gate_recheck_count": 1,
      "still_blocked_count": 1
    },
    {
      "before_count": 1,
      "blocker_category": "VALID_UNTIL_DEPENDENCY_GAP",
      "closure_evidence_added_count": 1,
      "known_still_blocked_feature_ids": [],
      "recommended_next_task": "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
      "requires_owner_review_count": 1,
      "requires_pit_gate_recheck_count": 1,
      "still_blocked_count": 0
    },
    {
      "before_count": 10,
      "blocker_category": "PIT_GATE_EVIDENCE_GAP",
      "closure_evidence_added_count": 1,
      "known_still_blocked_feature_ids": [],
      "recommended_next_task": "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
      "requires_owner_review_count": 10,
      "requires_pit_gate_recheck_count": 10,
      "still_blocked_count": 10
    },
    {
      "before_count": 2,
      "blocker_category": "OWNER_REVIEW_GAP",
      "closure_evidence_added_count": 0,
      "known_still_blocked_feature_ids": [
        "growth_tilt_engine",
        "valid_until_window"
      ],
      "recommended_next_task": "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
      "requires_owner_review_count": 2,
      "requires_pit_gate_recheck_count": 2,
      "still_blocked_count": 2
    }
  ],
  "broker_action": "none",
  "contract_ready_count": 0,
  "engine_id": "growth_tilt_engine",
  "growth_tilt_engine_blocking_gap_resolved": false,
  "growth_tilt_engine_pit_input_severity": "BLOCKING",
  "growth_tilt_engine_severity_downgraded": false,
  "pit_gate_blocked_count": 10,
  "pit_gate_ready_count": 0,
  "pit_gate_recheck_required": true,
  "production_effect": "none",
  "recommended_next_task": "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
  "schema_version": "growth_tilt_engine_remaining_blocker_summary_after_valid_until_closure.v1",
  "source_feature_count": 10,
  "source_traceability_still_blocked_feature_ids": [
    "growth_tilt_engine_signal_artifact"
  ],
  "valid_until_window_blocking_gap_resolved": false,
  "valid_until_window_dependency_evidence_added_feature_ids": [
    "execution_signal_validity_policy"
  ],
  "valid_until_window_pit_input_severity": "BLOCKING",
  "valid_until_window_severity_downgraded": false
}
```

## PIT gate recheck policy

- pit_gate_recheck_required：`True`
- auto_mark_pit_gate_ready：`False`
- auto_mark_contract_ready：`False`
- auto_downgrade_blocker：`False`
- owner_review_required_before_downgrade：`True`

## Explicit non-approval list

```json
[
  "mark_any_source_feature_pit_gate_ready",
  "mark_any_source_feature_contract_ready",
  "downgrade_growth_tilt_engine_blocker",
  "downgrade_valid_until_window_blocker",
  "clear_growth_tilt_engine_blocking_gap",
  "clear_valid_until_window_blocking_gap",
  "resume_candidate_search",
  "approve_research_only_observation",
  "enable_paper_shadow",
  "create_paper_trade",
  "create_shadow_position",
  "enable_scheduler",
  "append_historical_event_log",
  "bind_outcome",
  "mutate_outcome_store",
  "enable_production",
  "call_broker_api",
  "send_order",
  "create_scheduled_task",
  "generate_daily_report",
  "run_new_strategy_backtest",
  "generate_new_trading_signal",
  "run_scoring"
]
```

## Data Quality Gate

- executed：`False`
- reason：`NOT_APPLICABLE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_PRIOR_ARTIFACTS_AND_CONFIGS_ONLY_NO_FRESH_MARKET_DATA`

本任务仅读取 prior validated artifacts、registry、catalog 和 docs，不读取 fresh
cached market data、不生成 feature/signal/scoring/daily report、不运行新 backtest。
