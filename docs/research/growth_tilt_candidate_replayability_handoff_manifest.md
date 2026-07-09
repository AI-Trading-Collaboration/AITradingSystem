# Growth Tilt Candidate Replayability Handoff Manifest

```json
{
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_replayable_after_closure_count": 3,
  "forward_aging_handoff_ready": false,
  "forward_aging_observation_started": false,
  "forward_aging_observation_written": false,
  "handoff_policy": "REPLAYABILITY_ONLY_2438G_DECIDES_PASS_FAIL_BLOCKED",
  "next_route": "TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure",
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "replayability_handoff_ready": true,
  "replayable_candidates": [
    {
      "candidate_id": "recovery_reentry_speedup_guard",
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:recovery_reentry_speedup_guard",
      "replay_status_after_closure": "BLOCKED"
    },
    {
      "candidate_id": "false_risk_off_confirmation_relaxation",
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:false_risk_off_confirmation_relaxation",
      "replay_status_after_closure": "BLOCKED"
    },
    {
      "candidate_id": "missed_upside_reentry_accelerator",
      "closure_evidence_ref": "TRADING-2438F:replayability_handoff:missed_upside_reentry_accelerator",
      "replay_status_after_closure": "BLOCKED"
    }
  ],
  "schema_version": "growth_tilt_candidate_replayability_handoff_manifest.v1",
  "status": "GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY"
}
```
