# TRADING-1526 Expanded Actual-Path Candidate Failure Matrix

最后更新：2026-06-27

## 状态

`VALIDATING`

## 背景

TRADING-1506～1525 已生成 11 个 expanded universe actual-path candidates，
但 owner 需要把每个 candidate 的失败原因拆到可审计字段，而不是只看 aggregate
survival count。

## 安全边界

- 该矩阵只解释 research-only actual-path candidates。
- dynamic promotion 固定 `BLOCKED`。
- target-path metrics 仍为 diagnostic-only。
- 不写 production config，不进入 paper-shadow，不触发 broker。

## 字段契约

矩阵必须至少输出：

- `candidate_id`
- `weights_by_state`
- `tqqq_weight_profile`
- `qqq_equivalent_exposure`
- `actual_return`
- `max_dd`
- `sharpe`
- `calmar`
- `same_risk_baseline`
- `delta_vs_same_risk_baseline`
- `failure_reason`
- `next_action`

并显式拆分：

- verdict 是什么；
- 被哪个 static frontier baseline 支配；
- 是否只是 TQQQ beta；
- 是否同风险下不占优；
- 是否 walk-forward 失效；
- 是否 stress risk 过高；
- 是否 net-of-cost 后失效。

## 验收标准

- 新增可重复生成的 CLI / artifact。
- 输出 11 行，与 actual-path leaderboard 候选数一致。
- 每行都有 weights、TQQQ profile、actual metrics、same-risk baseline 和 delta。
- 失败原因不能只写 `BLOCKED`，必须拆成 typed blockers。
- 所有输出继续声明 `ai_after_chatgpt`、actual-path-only、promotion blocked 和 no paper-shadow / no production / no broker。

## 进展记录

- 2026-06-27：新增并进入 `IN_PROGRESS`。Owner 要求输出候选级失败原因矩阵。
- 2026-06-27：实现完成并进入 `VALIDATING`。新增
  `aits research strategies expanded-universe candidate-failure-matrix`，真实输出
  11 行 candidate failure matrix；status=`CANDIDATE_FAILURE_MATRIX_READY_PROMOTION_BLOCKED`，
  artifact 包括 runtime CSV、tracked YAML 和 tracked Markdown。所有候选仍为
  research-only，dynamic promotion 继续 `BLOCKED`。
