# Channel-Specific First-Layer v3 Scope

本批只研究 `do_not_de_risk` 与 `risk_on_veto` 两个 channel。

## 允许范围

- `do_not_de_risk`: `drawdown_recovery`。
- `risk_on_veto`: `volatility_compression`, `rates_liquidity`。

## 禁止范围

- 不训练 universal first-layer 或 add-risk allocation model。
- 不输出 portfolio weights、target allocation、trade action 或 broker action。
- 不启用 growth overlay、TQQQ allocation、paper-shadow、production 或 promotion。

最终 candidate_count：`0`。
