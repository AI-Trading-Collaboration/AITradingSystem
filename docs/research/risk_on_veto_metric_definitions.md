# Risk-On Veto Metric Definitions

`risk_on_veto` 的 active cost 不能单独解释为 veto 有效或无效。Veto 本来就应该在更危险的环境中触发，因此 active raw false-add-risk cost 高于 inactive mean 是可能且可解释的。

本批报告必须同时披露：

- `raw_false_add_risk_cost_when_veto_active`
- `raw_false_add_risk_cost_when_veto_inactive`
- `avoided_false_add_risk_cost_due_to_veto`
- `captured_upside_lost_due_to_veto`
- `net_veto_benefit`
- `veto_hit_rate`
- `veto_false_positive_rate`
- `veto_false_negative_rate`

`raw_false_add_risk_cost_when_veto_active` 衡量 veto active 环境本身的危险程度；它不是 veto 造成的 benefit。

`avoided_false_add_risk_cost_due_to_veto` 只在参考路径本来会 add-risk 且 veto 阻断时计入。

`captured_upside_lost_due_to_veto` 衡量 veto 阻断后错过的上行 proxy；在当前 policy 中，该字段同时承担 opportunity-cost proxy，直到 forward diagnostic 有足够样本校准更细的 opportunity-cost 模型。

`net_veto_benefit = avoided_false_add_risk_cost_due_to_veto - captured_upside_lost_due_to_veto`。它仍然只是 diagnostic，不是 allocation 或 promotion evidence。
