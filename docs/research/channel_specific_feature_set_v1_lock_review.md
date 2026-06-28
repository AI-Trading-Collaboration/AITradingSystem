# Channel Specific Feature Set v1 Lock Review

状态：`CHANNEL_SPECIFIC_FEATURE_SET_V1_LOCKED_PROMOTION_BLOCKED`

- `do_not_de_risk` allowed families: `drawdown_recovery`。
- `risk_on_veto` allowed families: `volatility_compression, rates_liquidity`。
- diagnostic-only families: `trend_persistence, relative_strength`。
- blocked families: `breadth_participation, event_risk`。
- `can_emit_weights=false`，promotion / paper-shadow / production / broker 继续 blocked。
