# Dynamic Strategy TRADING-2439A Or 2438D Route

- source task：`TRADING-2438C`
- source status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED`
- next route：`TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`

PIT replay recheck 仍被阻塞，因为 candidate replay outputs 不完整。任何 forward-aging handoff 之前，必须先关闭 recheck blockers。
