# Two-Lane Signal Policy

- policy_id：`two_lane_signal_policy_v1`
- 状态：`LANE_SEPARATION_POLICY_READY`
- 市场阶段：`ai_after_chatgpt`
- promotion_allowed：`false`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## 政策结论

后续不再让一个 first-layer 五分类状态同时服务所有 second-layer probes。Current first-layer v2 被降级为 return-seeking diagnostic；防御用途必须走单独 defensive channel，且 defensive channel 对 growth overlay 有 veto power。

## Channel

| channel | 允许 | 禁止 |
|---|---|---|
| `defensive_channel` | `risk_off`、`defensive`、`do_not_de_risk`、`re_risk_allowed_but_not_add_risk` | `add_risk`、`high_confidence_risk_on`、promotion、paper-shadow、production、broker |
| `return_seeking_channel` | `stay_constructive`、`add_risk`、`high_confidence_risk_on` 的 diagnostic 使用 | defensive overlay、universal trend layer、balanced allocation、promotion、paper-shadow、production、broker |
| `gated_integration_channel` | future policy-defined research only | owner review、promotion、paper-shadow、production、broker |

## Risk-Off Veto

`risk_off`、event risk high、volatility high 或 defensive probe stress state 任一成立时，growth overlay 必须被阻断。该 veto 优先级高于 `add_risk` 和 `high_confidence_risk_on`。

## 未启用范围

本政策只定义 lane separation。它不训练 defensive lane 模型，不运行 return-seeking actual-path allocation，不启用 gated integration，也不恢复任何 promotion / paper-shadow / production / broker 路径。
