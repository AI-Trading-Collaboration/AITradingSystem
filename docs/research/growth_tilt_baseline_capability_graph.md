# Growth Tilt Baseline Capability Graph

该 graph 只读描述 baseline 实际 capability，不创建任何缺失 contract。Capability READY 也不自动等于 mutation-ready；candidate mutation 还必须满足 consumption、PIT、runner、mutable dimension和 dependency gates。

```json
{
  "artifact_reload_verified": true,
  "as_of": "2026-07-10",
  "callable_but_unconsumed_capability_ids": [
    "signal_re_risk_allowed_probability",
    "expiry_rule_signal_validity",
    "replay_runner_candidate_overlay_executor"
  ],
  "edge_count": 15,
  "graph_id": "growth_tilt_baseline_capability_graph_v1",
  "mutation_ready_capability_count": 0,
  "mutation_ready_capability_ids": [],
  "n3_candidate_generation_allowed": false,
  "n3_status": "NOT_STARTED_NO_MUTATION_READY_CAPABILITY",
  "n4_status": "NOT_STARTED_NO_CONTRACT_READY_CANDIDATE",
  "next_route": "TRADING-2438N3_NOT_STARTED_NO_MUTATION_READY_CAPABILITY",
  "node_count": 21,
  "readiness_status_counts": {
    "BLOCKED": 9,
    "DIAGNOSTIC_ONLY": 2,
    "NOT_APPLICABLE": 4,
    "READY": 6
  },
  "status": "GROWTH_TILT_BASELINE_CAPABILITY_GRAPH_READY_NO_MUTATION_READY_CAPABILITY"
}
```

## Node readiness summary

```json
[
  {
    "capability_contract_ready": false,
    "capability_id": "signal_re_risk_allowed_probability",
    "capability_type": "SIGNAL",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": true,
    "capability_id": "signal_growth_allowed",
    "capability_type": "SIGNAL",
    "mutation_blocker_codes": [
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "READY"
  },
  {
    "capability_contract_ready": true,
    "capability_id": "signal_first_layer_trend_state",
    "capability_type": "SIGNAL",
    "mutation_blocker_codes": [
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "READY"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "hard_veto_risk_off",
    "capability_type": "HARD_VETO",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": true,
    "capability_id": "hard_veto_volatility",
    "capability_type": "HARD_VETO",
    "mutation_blocker_codes": [
      "NO_APPROVED_MUTABLE_DIMENSION",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "READY"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "hard_veto_event_risk",
    "capability_type": "HARD_VETO",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "hard_veto_trend_break",
    "capability_type": "HARD_VETO",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": true,
    "capability_id": "hard_veto_tqqq",
    "capability_type": "HARD_VETO",
    "mutation_blocker_codes": [
      "NO_APPROVED_MUTABLE_DIMENSION",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "READY"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "hard_veto_aggregate",
    "capability_type": "HARD_VETO",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "DEPENDENCY_NOT_READY:hard_veto_event_risk",
      "DEPENDENCY_NOT_READY:hard_veto_risk_off",
      "DEPENDENCY_NOT_READY:hard_veto_trend_break",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "decision_request_non_hard_defensive",
    "capability_type": "DECISION_REQUEST",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:NOT_APPLICABLE",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "NOT_APPLICABLE"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "persistence_recovery",
    "capability_type": "PERSISTENCE_RULE",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:NOT_APPLICABLE",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "NOT_APPLICABLE"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "transition_regime_current_requested_applied",
    "capability_type": "REGIME_TRANSITION",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "exposure_scalar_native",
    "capability_type": "EXPOSURE_SCALAR",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": true,
    "capability_id": "exposure_cap_qqq_equivalent",
    "capability_type": "EXPOSURE_CAP",
    "mutation_blocker_codes": [
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "READY"
  },
  {
    "capability_contract_ready": true,
    "capability_id": "exposure_cap_turnover",
    "capability_type": "EXPOSURE_CAP",
    "mutation_blocker_codes": [
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "READY"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "ramp_rule_recovery",
    "capability_type": "RAMP_RULE",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:NOT_APPLICABLE",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "NOT_APPLICABLE"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "cooldown_rule_transition",
    "capability_type": "COOLDOWN_RULE",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:NOT_APPLICABLE",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "NOT_APPLICABLE"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "expiry_rule_signal_validity",
    "capability_type": "EXPIRY_RULE",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:DIAGNOSTIC_ONLY",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "DIAGNOSTIC_ONLY"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "metric_contract_six_runtime_metrics",
    "capability_type": "METRIC",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "replay_runner_candidate_overlay_executor",
    "capability_type": "REPLAY_RUNNER",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:DIAGNOSTIC_ONLY",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING"
    ],
    "mutation_ready": false,
    "readiness_status": "DIAGNOSTIC_ONLY"
  },
  {
    "capability_contract_ready": false,
    "capability_id": "replay_runner_growth_tilt_pit",
    "capability_type": "REPLAY_RUNNER",
    "mutation_blocker_codes": [
      "CAPABILITY_STATUS_NOT_READY:BLOCKED",
      "DEPENDENCY_NOT_READY:exposure_scalar_native",
      "DEPENDENCY_NOT_READY:hard_veto_aggregate",
      "DEPENDENCY_NOT_READY:transition_regime_current_requested_applied",
      "NOT_CONSUMED_BY_BASELINE",
      "NO_APPROVED_MUTABLE_DIMENSION",
      "PIT_LINEAGE_NOT_VALID",
      "RUNNER_BINDING_MISSING",
      "RUNTIME_NOT_CALLABLE",
      "RUNTIME_NOT_DETERMINISTIC",
      "SEMANTICS_NOT_APPROVED"
    ],
    "mutation_ready": false,
    "readiness_status": "BLOCKED"
  }
]
```

## Edges

```json
[
  {
    "blocker_codes": [
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "signal_growth_allowed",
    "pit_valid": false,
    "relation": "PRODUCES",
    "runtime_resolvable": true,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "hard_veto_risk_off"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "hard_veto_risk_off",
    "pit_valid": false,
    "relation": "PRODUCES",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "hard_veto_aggregate"
  },
  {
    "blocker_codes": [],
    "edge_ready": true,
    "from_capability": "hard_veto_volatility",
    "pit_valid": true,
    "relation": "PRODUCES",
    "runtime_resolvable": true,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "hard_veto_aggregate"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "hard_veto_event_risk",
    "pit_valid": false,
    "relation": "PRODUCES",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "hard_veto_aggregate"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "hard_veto_trend_break",
    "pit_valid": false,
    "relation": "PRODUCES",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "hard_veto_aggregate"
  },
  {
    "blocker_codes": [],
    "edge_ready": true,
    "from_capability": "hard_veto_tqqq",
    "pit_valid": true,
    "relation": "PRODUCES",
    "runtime_resolvable": true,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "hard_veto_aggregate"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "signal_re_risk_allowed_probability",
    "pit_valid": false,
    "relation": "REQUESTS",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "persistence_recovery"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE"
    ],
    "edge_ready": false,
    "from_capability": "signal_first_layer_trend_state",
    "pit_valid": true,
    "relation": "REQUESTS",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "transition_regime_current_requested_applied"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "hard_veto_aggregate",
    "pit_valid": false,
    "relation": "GUARDED_BY",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "transition_regime_current_requested_applied"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "transition_regime_current_requested_applied",
    "pit_valid": false,
    "relation": "APPLIES",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "exposure_scalar_native"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "exposure_cap_qqq_equivalent",
    "pit_valid": false,
    "relation": "CAPS",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "exposure_scalar_native"
  },
  {
    "blocker_codes": [],
    "edge_ready": true,
    "from_capability": "exposure_cap_turnover",
    "pit_valid": true,
    "relation": "CAPS",
    "runtime_resolvable": true,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "transition_regime_current_requested_applied"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "transition_regime_current_requested_applied",
    "pit_valid": false,
    "relation": "REPLAYED_BY",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "replay_runner_candidate_overlay_executor"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "replay_runner_candidate_overlay_executor",
    "pit_valid": false,
    "relation": "CONSUMED_BY",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "replay_runner_growth_tilt_pit"
  },
  {
    "blocker_codes": [
      "EDGE_RUNTIME_NOT_RESOLVABLE",
      "EDGE_PIT_INVALID"
    ],
    "edge_ready": false,
    "from_capability": "replay_runner_growth_tilt_pit",
    "pit_valid": false,
    "relation": "MEASURED_BY",
    "runtime_resolvable": false,
    "schema_version": "growth_tilt_baseline_capability_edge.v1",
    "to_capability": "metric_contract_six_runtime_metrics"
  }
]
```

## 结论

真实 mutation-ready capability 为 0，因此 N3/N4 不启动。现有 callable signal、单项 hard veto或 cap 不能绕过 authoritative aggregate、requested/applied transition、native scalar、runner binding和 approved mutable-dimension gates。后续 baseline capability只能由 candidate-independent project引入。
