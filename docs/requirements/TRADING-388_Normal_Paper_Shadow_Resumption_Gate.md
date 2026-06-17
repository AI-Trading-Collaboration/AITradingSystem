# TRADING-388 Normal Paper Shadow Resumption Gate

最后更新：2026-06-17

## 背景

TRADING-385/386 已恢复 signal input completeness 到 warning-only 状态，TRADING-387
已重跑 evidence staleness、shadow continuation readiness 和 canonical paper-shadow
health。TRADING-387 的真实恢复链仍输出 `MANUAL_REVIEW_REQUIRED`，因此后续必须增加一层
显式 normal paper-shadow resumption gate，避免把 warning/manual review 状态误解释为
promotion、extended shadow、live trading 或 official target weight approval。

## 范围

本任务新增只读 gate artifact：

- `aits etf dynamic-v3-rescue normal-paper-shadow-resumption-gate run`
- `aits etf dynamic-v3-rescue normal-paper-shadow-resumption-gate report --latest`
- `aits etf dynamic-v3-rescue validate-normal-paper-shadow-resumption-gate --latest`

Gate 只读取 readiness/health recovery artifact 和 owner decision evidence。它不运行
promotion board，不执行 extended-shadow protocol，不刷新数据，不补造 owner decision，不写
official target weights，不触发 broker/order，不修改 paper account 或 production state。

## Gate Requirements

Resumption 前必须同时满足：

- signal input completeness not BLOCKING；
- evidence staleness not BLOCKING；
- shadow continuation readiness not BLOCKED；
- canonical paper-shadow health not BLOCKED；
- safety boundary not BLOCKED；
- owner action 是 `hold` 或 `continue_normal_shadow`，且不得是 promotion /
  extended-shadow / live / broker / official-target action；
- normal paper-shadow observation 真正恢复前，必须存在人工 owner review；
- `hold` 是安全非 promotion 动作，但不授权恢复；只有
  `continue_normal_shadow` 且人工 review completed 才可恢复 normal observation。

## Statuses

- `RESUME_NORMAL_SHADOW_ALLOWED`
- `RESUME_NORMAL_SHADOW_WITH_WARNINGS`
- `RESUME_NORMAL_SHADOW_BLOCKED`

`RESUME_NORMAL_SHADOW_ALLOWED` 和 `RESUME_NORMAL_SHADOW_WITH_WARNINGS` 只允许 normal
paper-shadow observation。它们不是 extended shadow、promotion、official target、broker、
order ticket、live trading 或 production mutation approval。

## Validation

Validator 检查：

- required artifact files 存在；
- status enum 有效；
- source recovery、owner action 和 resumption requirements 可见；
- status 与 blockers/warnings/owner action 一致；
- 非 blocked status 必须有 `manual_owner_review_completed=true` 和
  `owner_action=continue_normal_shadow`；
- `hold` 不得让 `normal_paper_shadow_may_resume=true`；
- promotion、extended shadow、live trading、official target weights、broker/order 和
  production mutation 固定 forbidden；
- Reader Brief section 暴露 status、resume flag、owner action 和 next action。

## Progress

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增需求文档，准备实现 normal paper-shadow resumption gate。|
|2026-06-17|DONE|新增 gate module、CLI、report/validate artifact、Reader Brief section、report registry、artifact catalog、operations runbook、system flow 和 focused tests。真实 artifact `normal-paper-shadow-resumption-gate_092cb50466657186` 读取 recovery `readiness-health-recovery_4c4fa150becc7305`，输出 `RESUME_NORMAL_SHADOW_BLOCKED`、`normal_paper_shadow_may_resume=false`、`owner_action=` empty、`manual_owner_review_completed=false`、validation PASS；blocking reasons 为 missing manual owner review / owner action authorization。该状态继续禁止 promotion、extended shadow、live trading、official target weights、broker/order 和 production mutation。|
