# Indicator Family Interaction Warning Review

状态：`INTERACTION_RESEARCH_OPTIONAL`

该报告为 research-only evidence，不产生 target weights，不触发 promotion、paper-shadow、production 或 broker。

| interaction | status | allowed_next_step |
|---|---|---|
| trend_persistence + volatility_compression | INTERACTION_NOT_ENOUGH_EVIDENCE | future diagnostic only |
| relative_strength + breadth_participation | INTERACTION_NOT_ENOUGH_EVIDENCE | blocked until breadth PIT source approved |
| drawdown_recovery + volatility_compression | INTERACTION_RESEARCH_OPTIONAL | future veto/defensive interaction review |
| rates_liquidity + event_risk | INTERACTION_NOT_ENOUGH_EVIDENCE | blocked until event PIT source approved |
