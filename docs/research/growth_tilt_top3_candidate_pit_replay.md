# Growth Tilt Top-3 Candidate PIT Replay

- task_id：`TRADING-2438`
- status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`
- data quality status：`PASS_WITH_WARNINGS`
- PIT candidates selected：`3`
- PIT candidates tested：`0`
- next route：`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`

TRADING-2438 运行数据质量门，选择最多三个 PIT candidates，然后在缺少 Growth Tilt candidate-specific PIT replay engine 和 replay input specs 时 fail-closed。本输出不是 replay pass，也不是 alpha 结论。

```json
{
  "data_quality_status": "PASS_WITH_WARNINGS",
  "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
  "pit_candidates_selected": 3,
  "pit_candidates_tested": 0,
  "pit_replay_fail_count": 0,
  "pit_replay_pass_count": 0,
  "promotion_review_candidate_count": 0,
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
}
```

## PIT Replay Blocker Summary

```json
{
  "blocked": true,
  "blocked_candidate_count": 3,
  "blocking_gap_classifications": {
    "candidate_as_of_boundary_specs_ready": "candidate_as_of_boundary_gap",
    "candidate_outcome_linkage_specs_ready": "candidate_outcome_linkage_gap",
    "candidate_pit_replay_engine_available": "candidate_pit_replay_engine_gap",
    "candidate_replay_input_specs_ready": "candidate_pit_replay_input_gap",
    "candidate_source_traceability_manifests_ready": "candidate_source_traceability_gap",
    "candidate_valid_until_boundary_specs_ready": "candidate_valid_until_boundary_gap"
  },
  "blocking_gap_count": 6,
  "blocking_gap_ids": [
    "candidate_pit_replay_engine_available",
    "candidate_replay_input_specs_ready",
    "candidate_source_traceability_manifests_ready",
    "candidate_as_of_boundary_specs_ready",
    "candidate_valid_until_boundary_specs_ready",
    "candidate_outcome_linkage_specs_ready"
  ],
  "broker_action": "none",
  "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
  "pit_replay_blocker_summary_ready": true,
  "production_effect": "none",
  "schema_version": "growth_tilt_top3_candidate_pit_replay_blocker_summary.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
}
```

## No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 6,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_engine_gap",
      "evidence": {
        "current_engine": null,
        "required_engine": "growth_tilt_candidate_specific_pit_replay_engine"
      },
      "gap": "candidate_pit_replay_engine_available did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_pit_replay_engine_available"
    },
    {
      "broker_action": "none",
      "classification": "candidate_pit_replay_input_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_replay_input_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_replay_input_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_source_traceability_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_source_traceability_manifests_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_source_traceability_manifests_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_as_of_boundary_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_as_of_boundary_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_as_of_boundary_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_valid_until_boundary_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_valid_until_boundary_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_valid_until_boundary_specs_ready"
    },
    {
      "broker_action": "none",
      "classification": "candidate_outcome_linkage_gap",
      "evidence": {
        "selected_candidate_count": 3
      },
      "gap": "candidate_outcome_linkage_specs_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "candidate_outcome_linkage_specs_ready"
    }
  ],
  "generated_signal": false,
  "generated_trading_advice": false,
  "no_effect_boundary_ready": true,
  "outcome_binding_executed": false,
  "outcome_store_mutated": false,
  "paper_shadow_enabled": false,
  "paper_shadow_schedule_enabled": false,
  "portfolio_weight_mutated": false,
  "production_effect": "none",
  "production_enabled": false,
  "schema_version": "growth_tilt_top3_candidate_pit_replay_no_effect.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
}
```
