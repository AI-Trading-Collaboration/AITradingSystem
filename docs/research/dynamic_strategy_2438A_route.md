# Dynamic Strategy TRADING-2438A Route

- source task：`TRADING-2438`
- source status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`
- next route：`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`

TRADING-2438A 应先补齐 Growth Tilt candidate-specific PIT replay engine 和 candidate replay input specs，然后才允许进入 forward aging candidate pack。TRADING-2438 不启用 paper-shadow、production、broker、trading advice 或 portfolio weight mutation。
