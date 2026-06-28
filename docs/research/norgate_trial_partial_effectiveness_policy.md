# Norgate Trial Partial Effectiveness Policy

本 policy 只治理 Norgate trial 2Y 局部有效性诊断，不是 production threshold，
也不是 promotion gate。

## 诊断口径

- Coverage floor：`member_day_coverage_ratio >= 0.9` 才允许
  `feature_numeric_validated=true`。
- Breadth bucket：使用 `pct_above_ma50` 的 trial-window tertile，分为
  `low` / `mid` / `high`。
- Deterioration：使用 `breadth_momentum` bottom tertile。
- Baseline proxy：`QQQ close < QQQ MA50`。
- Baseline + breadth：baseline risk-off、low breadth 或 breadth deterioration
  任一触发即为 risk-off diagnostic。
- False risk-off：risk-off 后 next 20D QQQ return 为正。
- False risk-on：risk-on 后 future 20D drawdown 落入 bottom quartile。

## 安全边界

- `primary_window_validated=false`
- `model_ready_for_2021_primary_window=false`
- `reopen_gate_allowed=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`

2Y trial 可以回答“是否值得继续购买正式历史”，不能回答“第一层策略是否已通过正式验收”。
