# Boundary-Aware Two-Layer Strategy Boundary Contract

状态：`BOUNDARY_CONTRACT_READY`

本合同承接 TRADING-1806～1885 的结论：不恢复 universal first-layer，不把 return-seeking diagnostic 转为 allocation signal，不启用 gated integration，不进入 promotion、paper-shadow、production 或 broker。

## 第一层边界

| Channel | 允许输出 | 禁止输出 | 当前用途 |
|---|---|---|---|
| `defensive` | `risk_off`、`defensive_hold`、`do_not_de_risk`、`re_risk_allowed` | `add_risk`、`risk_on_diagnostic`、`tqqq_signal` | defensive overlay / drawdown-control diagnostic |
| `return_seeking_diagnostic` | `stay_constructive`、`add_risk`、`risk_on_diagnostic` | target weights / defensive overlay signal | forward diagnostic log / family review |
| `risk_veto` | `growth_allowed`、`tqqq_allowed`、`add_risk_allowed`、`veto_reasons` | direct allocation delta | veto / blocker |

所有 first-layer signal 都必须声明 `allowed_usage`、`blocked_usage`、`required_veto`、`diagnostic_only` 和 `can_emit_weights=false`。

## 第二层边界

第二层只能读取 first-layer channel output、usage matrix、channel policy 和 base/overlay/veto schema。第二层不得直接读取 `QQQ_momentum`、`VIX`、`SMH_relative_strength`、`rates`、`AI_trend_score` 或 breadth 等 raw indicators。

第二层输出必须 long-only、sum-to-one，并写出 audit trace。当前任何 target weights 都是 research-only framework output，不是 owner-review candidate 或交易建议。

## 评估边界

优化目标必须先声明错误类型：`false_risk_off`、`missed_risk_off`、`false_add_risk`、`late_re_risk`、`beta_only_improvement`。没有预注册 selection rule 的结果不能成为 candidate。

安全状态固定为：`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、`dynamic_promotion_status=BLOCKED`。
