# Indicator Family Registry Review

状态：`INDICATOR_FAMILY_REGISTRY_READY`

综合模型训练前必须先做 family-level action-value audit。本 registry 覆盖七类 family：`trend_persistence`、`relative_strength`、`volatility_compression`、`drawdown_recovery`、`breadth_participation`、`rates_liquidity`、`event_risk`。

每个 family 必须声明：

- `features`
- `PIT_required=true`
- `allowed_labels`
- `blocked_usage`

Ablation 需要回答它是否帮助 `do_not_de_risk`、`stay_constructive`、`add_risk`，是否降低 `false_add_risk`，是否只在 2023+ 有效，是否通过 primary window / 2022 slice，是否改善 actual-path。

当前 registry 只允许 diagnostic-only review，不允许直接生成 weights、promotion evidence、production config 或 broker action。
