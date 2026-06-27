# First-Layer Signal Usage Matrix

- 状态：`SIGNAL_USAGE_MATRIX_READY_PROMOTION_BLOCKED`
- market_regime：`ai_after_chatgpt`
- promotion_allowed：`false`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## 摘要

每个 first-layer signal 都必须有明确的 `allowed_usage`、`blocked_usage` 和 `required_gate`。`add_risk` 与 `high_confidence_risk_on` 只可保留为 return-seeking diagnostic 或 future gated growth overlay 输入；它们不得进入 defensive overlay、universal trend layer、promotion、paper-shadow、production 或 broker。

## Matrix

| signal | source_model | allowed_usage | blocked_usage | required_gate |
|---|---|---|---|---|
| `risk_off_probability` | `future_defensive_preservation_lane` | defensive overlay / growth blocker | add-risk allocation、promotion、paper-shadow、production、broker | blocker signal |
| `defensive_probability` | `future_defensive_preservation_lane` | defensive overlay / growth blocker | add-risk allocation、promotion、paper-shadow、production、broker | blocker signal |
| `do_not_de_risk_probability` | `future_defensive_preservation_lane` | defensive overlay / growth blocker | add-risk allocation、promotion、paper-shadow、production、broker | blocker signal |
| `re_risk_allowed_but_not_add_risk` | `future_defensive_preservation_lane` | defensive overlay diagnostic | add-risk allocation、universal layer、promotion、paper-shadow、production、broker | defensive lane not risk-off |
| `stay_constructive` | `first_layer_v2_return_seeking_diagnostic` | return-seeking diagnostic / future gated growth overlay | defensive overlay、universal layer、balanced allocation、promotion、paper-shadow、production、broker | risk-off veto clear |
| `add_risk` | `first_layer_v2_return_seeking_diagnostic` | return-seeking diagnostic / future gated growth overlay | defensive overlay、universal layer、balanced allocation、owner review、promotion、paper-shadow、production、broker | risk-off veto clear |
| `high_confidence_risk_on` | `first_layer_v2_return_seeking_diagnostic` | return-seeking diagnostic / future gated growth overlay | defensive overlay、universal layer、balanced allocation、owner review、promotion、paper-shadow、production、broker | risk-off veto clear and TQQQ cap review |

## 解释边界

`gated_growth_overlay` 出现在 allowed usage 中只是 future research 条件输入，不代表本批启用 allocation。当前 final matrix 仍固定 dynamic promotion `BLOCKED`，owner review disabled，paper-shadow / production / broker disabled。
