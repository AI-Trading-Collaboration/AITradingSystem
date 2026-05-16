# Forward Shadow Candidate Lifecycle

最后更新：2026-05-17

## 背景

`shadow-only iteration` MVP 已经能把 search output 转成 registry、日报式报告和只读 dashboard
JSON。下一步需要让候选跨交易日持续观察：候选先显式登记为
`FORWARD_SHADOW_ACTIVE`，随后每日评估 forward 表现和治理动作，而不是每天只看到新的
search result。

## 范围

P0 实现：

1. `aits feedback run-shadow-iteration` 对同一 `as_of` 和同一 registry 状态幂等。
2. `aits feedback register-forward-shadow` 只更新 shadow registry，状态为
   `FORWARD_SHADOW_ACTIVE`。
3. 新增 `aits feedback evaluate-forward-shadow --as-of YYYY-MM-DD`。
4. registry 状态支持 `FORWARD_SHADOW_ACTIVE`、`RETIRED` 和 `REVIEW_PENDING`。
5. 生成 `outputs/reports/forward_shadow_evaluation_YYYY-MM-DD.md/json`。
6. 评估动作只允许 `CONTINUE`、`RETIRE`、`REVIEW_GATE_POLICY`、
   `REVIEW_WEIGHT_CANDIDATE`。
7. 不修改 production weights、production gates、approved overlay 或正式
   `prediction_ledger.csv`。

## 生命周期规则

- `FORWARD_SHADOW_ACTIVE`：候选已被人工或流程显式登记，继续收集前向证据。
- `REVIEW_PENDING`：候选达到复核触发条件，但仍无 production effect。
- `RETIRED`：候选因类型、driver、contract/retirement evidence 或表现恶化退出观察。

评估动作：

- `CONTINUE`：继续 forward shadow。
- `RETIRE`：登记为 `RETIRED`，不再作为 active forward shadow 候选。
- `REVIEW_GATE_POLICY`：仅限 `gate_only`，登记为 `REVIEW_PENDING`，进入 gate policy review。
- `REVIEW_WEIGHT_CANDIDATE`：仅限 `weight_only` 且 `primary_driver=weight`，登记为
  `REVIEW_PENDING`，进入权重候选复核；不代表 owner approval 或 production 变更。

`weight_gate_bundle`、`gate_only` 和 `primary_driver != weight` 的候选不得进入
weight review。

## 验收标准

- 重复运行 `run-shadow-iteration` 不产生重复 registry 行，也不重复累计同一
  `as_of` 的 missing-top-group 证据。
- `gate_only` 不会进入 weight review。
- `weight_gate_bundle` 不会进入 weight review。
- `primary_driver != weight` 不会进入 weight review。
- forward evaluation 报告和 JSON 均声明 `production_effect=none`。
- production 权重、gate、approved overlay 和正式 prediction ledger checksum 不变。

## 进展记录

- 2026-05-17：新增需求文档并进入实现。
- 2026-05-17：实现完成并进入 VALIDATING。已新增 `aits feedback evaluate-forward-shadow`、
  `REVIEW_PENDING` 生命周期、`forward_shadow_evaluation_YYYY-MM-DD.md/json` 输出、
  run-shadow registry 幂等保护和 production safety 测试；验证通过目标 shadow iteration
  测试、daily task dashboard 相关测试、全量 `ruff check src tests`、全量 `pytest -q`
  572 passed 和 `git diff --check`。
