# High-Intensity Forward Observe Event Schema

本 schema 定义未来 observe-only event logger 应记录的字段。2334 只定义 schema，不启动 logger。

- event_id: required
- event_date: required
- target_asset: required
- known_at_policy: `NEXT_SESSION_DECISION_POLICY`
- latency_policy: `next trading day`
- pit_policy: `PIT_APPROXIMATION_READY`
- event_status: `OBSERVE_PENDING`
- manual_review_observation_flag: required

该 flag 只是研究观察标记，不是减仓建议、rebalance instruction 或 broker action。
