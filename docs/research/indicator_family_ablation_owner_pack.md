# Indicator Family Ablation Owner Pack

## 结论

- 有真实 PIT/action-value evidence 的 family 数：`5`。
- PIT / coverage blocker：`breadth_participation, event_risk`。
- 2023+ dependent：`trend_persistence, relative_strength`。
- beta / TQQQ dependent：`relative_strength`。
- do_not_de_risk：`drawdown_recovery`。
- risk_on_veto：`rates_liquidity, volatility_compression`。
- return_seeking_diagnostic：`relative_strength, trend_persistence`。
- add_risk：没有 family 获准进入 allocation 或 add-risk model；只保留 diagnostic。

## 为什么 promotion 仍 blocked

本批只选择下一轮 research-only channel feature families。它没有 owner-reviewed candidate、没有 forward paper-shadow、没有 production approval，也没有 broker action。所有 family 均 `can_emit_weights=false`。
