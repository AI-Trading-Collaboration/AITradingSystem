# Risk-On Veto Observe-Only Diagnostic Review

状态：`RISK_ON_VETO_V3_OBSERVE_ONLY_DIAGNOSTIC`

本报告由 `aits research trends risk-on-veto-diagnostic` 生成。`risk_on_veto` 是 veto / blocker，不是 allocation、add-risk、buy 或 TQQQ signal。

## 关键指标

- data_quality_status: `PASS_WITH_WARNINGS`
- episode_count: `2771`
- veto_active_rate: `0.395383`
- raw active false-add-risk cost: `0.007518`
- raw inactive false-add-risk cost: `0.006566`
- avoided false-add-risk cost total: `1.07859`
- captured upside lost total: `3.421701`
- net_veto_benefit_total: `-2.343111`
- veto_hit_rate: `0.259366`
- veto_false_positive_rate: `0.740634`
- veto_false_negative_rate: `0.288115`

## 解释边界

active raw cost 高于 inactive mean 不能单独解释为 veto 失败；active 行本身可能处于更危险环境。只有当参考路径本来会 add-risk 且 veto 阻断时，才计入 avoided cost 和 captured-upside lost。

本报告不生成 weights、strategy candidate、trade action、paper-shadow action、production action 或 broker action。
