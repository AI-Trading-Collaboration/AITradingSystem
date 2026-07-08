# Growth Tilt Forward Aging Candidate Pack

- task_id：`TRADING-2439`
- status：`GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE`
- PIT replay source status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`
- data quality status：`PASS_WITH_WARNINGS`
- forward aging candidate count：`0`
- next route：`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`

TRADING-2439 只允许为真实通过 PIT replay 的候选生成 forward aging candidate pack。当前 2438 被 replay engine / input specs blocker 卡住，因此本任务 fail-closed，未生成 forward aging candidates。

```json
{
  "data_quality_status": "PASS_WITH_WARNINGS",
  "forward_aging_candidate_count": 0,
  "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
  "observation_horizons": [
    "1d",
    "5d",
    "10d",
    "20d"
  ],
  "pit_replay_source_status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
  "status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
}
```

## Candidate Pack

```json
{
  "blocking_gap_ids": [
    "source_2438_top3_pit_replay_ready",
    "pit_replay_pass_candidate_available"
  ],
  "broker_action": "none",
  "candidate_evidence_refresh_cadence": "not_started_pit_replay_gate_blocked",
  "candidates": [],
  "evidence_gap_count": 2,
  "forward_aging_candidate_count": 0,
  "forward_aging_candidate_pack_ready": false,
  "observation_horizons": [
    "1d",
    "5d",
    "10d",
    "20d"
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_forward_aging_candidate_pack_details.v1",
  "status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE",
  "valid_until_outcome_capture_ready": false
}
```

## No-Effect Boundary

```json
{
  "automatic_execution_allowed": false,
  "broker_action": "none",
  "broker_enabled": false,
  "broker_order_generated": false,
  "evidence_gap_count": 2,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "fresh_market_data_read": false,
  "fresh_outcome_data_read": false,
  "gaps": [
    {
      "broker_action": "none",
      "classification": "pit_replay_gate_gap",
      "evidence": {
        "next_route": "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation",
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
      },
      "gap": "source_2438_top3_pit_replay_ready did not pass.",
      "production_effect": "none",
      "requirement_id": "source_2438_top3_pit_replay_ready"
    },
    {
      "broker_action": "none",
      "classification": "pit_replay_gate_gap",
      "evidence": {
        "pit_replay_pass_candidate_count": 0
      },
      "gap": "pit_replay_pass_candidate_available did not pass.",
      "production_effect": "none",
      "requirement_id": "pit_replay_pass_candidate_available"
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
  "schema_version": "growth_tilt_forward_aging_no_effect.v1",
  "status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
}
```
