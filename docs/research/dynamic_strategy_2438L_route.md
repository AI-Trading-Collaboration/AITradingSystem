# Dynamic Strategy TRADING-2438L Route

- source task: `TRADING-2438K`
- source status: `GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY`
- next route: `TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Runtime_Remediation`

2438K 不重判 replay outcome，不启动 forward-aging、paper-shadow、production 或 broker。下一步必须由 2438L 独立执行 candidate PIT replay recheck；若 runtime materialization 不完整，则先继续 runtime blocker remediation。
