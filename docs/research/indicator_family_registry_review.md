# Indicator Family Registry Review

状态：`INDICATOR_FAMILY_REGISTRY_READY`

综合模型训练前必须先做 family-level action-value audit。本 registry 覆盖七类 family：`trend_persistence`、`relative_strength`、`volatility_compression`、`drawdown_recovery`、`breadth_participation`、`rates_liquidity`、`event_risk`。

每个 family 必须声明：

- `features`
- `PIT_required=true`
- `allowed_labels`
- `allowed_channels`
- `blocked_channels`
- `blocked_usage`
- `earliest_available_date`
- `window_coverage`

Ablation 需要回答它是否帮助 `do_not_de_risk`、`stay_constructive`、`add_risk`，是否降低 `false_add_risk` / `false_risk_off`，是否可作为 `risk_on_veto`，是否只在 2023+ 有效，是否依赖 beta / TQQQ，是否通过 primary window / 2022 slice，是否改善 actual-path。

TRADING-1946～1975 后，registry 的可用 family 已改为真实 PIT feature matrix 列；`breadth_participation` 和 `event_risk` 仍因缺少 PIT-approved local source 保持 blocked。Registry 只能驱动 family-level evidence 和下一轮 research-only channel feature whitelist，不允许直接生成 weights、promotion evidence、production config 或 broker action。
