# Risk-On Veto Diagnostic Contract

状态：`observe_only`

`risk_on_veto` 是 veto / blocker。它表示当前环境下不应轻易执行 add-risk、growth overlay 或 TQQQ exposure。

它可以用于 return-seeking diagnostic review、future gated-overlay blocker research、false add-risk analysis 和 forward diagnostic logging。

它不能用于 allocation signal、add-risk signal、buy signal、target weights、trade advice、owner review candidate、promotion evidence、paper-shadow、production 或 broker action。

固定边界：

- `can_emit_weights=false`
- `can_emit_trade_advice=false`
- `can_enable_growth_overlay=false`
- `can_enable_tqqq=false`
- `owner_review_allowed=false`
- `promotion_enabled=false`
- `paper_shadow_enabled=false`
- `production_enabled=false`
- `broker_enabled=false`

允许 family 仅为 `volatility_compression` 与 `rates_liquidity`。`drawdown_recovery` 只保留为 `do_not_de_risk v3` 归档证据。
