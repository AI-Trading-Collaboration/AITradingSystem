# TRADING-2445 Task Register `BASELINE_DONE` Active Semantics

最后更新：2026-07-16

## 状态

`DONE`

## 背景

`docs/task_register.md` 是未完成工作、owner/data 依赖和基础版遗留项的事实源。
项目工程规则同时规定：`BASELINE_DONE` 表示基础闭环已经存在，但长期数据源、
验证、设计或覆盖缺口仍未关闭。因此它不是终态，也不能仅因基础版可用而从 active
register 消失。

现有 task-register consistency checker 把 `DONE`、`BASELINE_DONE` 和 `DROPPED`
全部当作 terminal status，导致仍需推进的九个 `BASELINE_DONE` 任务被错误要求归档，
并使两条已被后续 canonical 迁移结论取代的旧 `DONE` 记录与 active source truth 重复。

## 决策

- terminal status 只包含 `DONE` 与 `DROPPED`；二者必须位于 completed register。
- `BASELINE_DONE` 必须保留在 active register，直到剩余缺口被关闭并转为 `DONE`，
  或经明确决策转为其他 active/terminal status。
- completed register 不接受 `BASELINE_DONE`；历史上错误归档的记录应回到 active，
  或在已有更新 active row 时删除过时的 completed duplicate。
- consistency report 必须显式披露 terminal 与 active-baseline status 语义，继续对
  duplicate task id、active terminal row、completed non-terminal row fail closed。

## 实施步骤

1. 更新 task-register 使用规则与当前任务说明。
2. 修正 consistency checker 的 status boundary、报告文案与 methodology metadata。
3. 补充 `BASELINE_DONE` active PASS、completed FAIL、`DONE` active FAIL 的 focused tests。
4. 删除 `TRADING-261_to_265...` 与 `TRADING-266_to_270...` 的过时 completed duplicates，
   保留较新的 active `BASELINE_DONE` canonical rows。
5. 将 completed register 中其余 82 条历史 `BASELINE_DONE` 原样迁回 active register；
   不改变 task id、优先级、状态、owner、验收标准或历史备注，不把缺口静默改写为 `DONE`。
6. 更新 `docs/system_flow.md`，运行 focused parallel pytest、真实 register consistency
   build/validate、documentation checks 和最终 validation tiers。

## 验收标准

- active register 中 `BASELINE_DONE` 不触发 archive/missing-completed 错误。
- completed register 中 `BASELINE_DONE` 触发 blocking failure。
- active register 中 `DONE` 或 `DROPPED` 仍触发 blocking failure。
- active/completed task id 仍必须全局唯一。
- 当前仓库 task-register consistency payload 与 validation 均为 `PASS`。
- 变更不修改任务业务状态、不运行投资研究、不刷新数据，`production_effect=none`。

## 进展记录

- 2026-07-16：登记并进入 `IN_PROGRESS`；根因确认是 checker 与项目工程规则的
  `BASELINE_DONE` 定义冲突，不采用批量误归档作为绕行方案。
- 2026-07-16：迁移前审计发现 completed register 另有 82 条历史
  `BASELINE_DONE`。已保持原状态和整行上下文迁回 active register；任务总数不变，
  仅修正 active/completed 可见性，并删除两条被较新 canonical rows 取代的旧 `DONE`
  duplicate。
- 2026-07-16：focused parallel pytest `10 passed`；真实仓库 consistency 与 validation
  均为 `PASS`，active/completed/total=`428/440/868`，active `BASELINE_DONE=91`、
  completed `BASELINE_DONE=0`、blocking/warning=`0/0`。任务验收完成，
  `production_effect=none`。
