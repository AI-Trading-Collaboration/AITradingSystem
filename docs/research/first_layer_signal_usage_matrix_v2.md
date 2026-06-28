# First-Layer Signal Usage Matrix V2

状态：`SIGNAL_USAGE_MATRIX_V2_READY_PROMOTION_BLOCKED`

V2 matrix 明确每个 signal 的 channel、允许用途、禁止用途、required veto、`diagnostic_only` 和 `can_emit_weights=false`。它替代早期 universal trend-state 解释方式，但不放行任何 allocation candidate。

| Signal | Channel | 当前允许用途 | allocation |
|---|---|---|---|
| `risk_off` | defensive | defensive overlay / risk veto input | blocked |
| `defensive_hold` | defensive | defensive overlay | blocked |
| `do_not_de_risk` | defensive | false risk-off reduction research | blocked |
| `re_risk_allowed` | defensive | defensive overlay neutralization | blocked from add-risk |
| `stay_constructive` | return-seeking diagnostic | diagnostic log / family review | blocked |
| `add_risk` | return-seeking diagnostic | false add-risk attribution | blocked |
| `risk_on_diagnostic` | return-seeking diagnostic | beta/TQQQ dependency review | blocked |
| `growth_allowed` | risk veto | growth overlay blocker | standalone allocation blocked |
| `tqqq_allowed` | risk veto | TQQQ blocker | standalone allocation blocked |

关键结论：

- Defensive channel 不允许 `add_risk`、`risk_on_diagnostic` 或 TQQQ increase。
- Return-seeking diagnostic channel 不能驱动 defensive overlay、allocation、owner review、promotion、paper-shadow、production 或 broker。
- Risk veto channel 只做 blocker，不做 return boost。
- 所有 signal 都不能直接输出 portfolio weights。
