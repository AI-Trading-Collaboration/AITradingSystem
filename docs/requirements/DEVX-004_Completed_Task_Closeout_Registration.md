# DEVX-004：Completed Task Closeout Registration

## 状态

- status：BASELINE_DONE
- priority：P0
- owner：engineering line
- last updated：2026-07-27

## 问题

标准流程要求最终提交同时更新 task register 状态；任务转入
`docs/task_register_completed.md` 后，`run-governed-development` 的
`CLOSEOUT --remote-action` 仍只检查 active register，导致已正确归档的任务被误报为
`TASK_NOT_REGISTERED`。OPS-069 的真实 closeout 首次暴露该缺口。

## 设计

1. `START`、`LANE`、`INTEGRATION` 继续只接受 active register，避免归档任务重新进入开发。
2. 仅 `CLOSEOUT` 可接受 active 或 completed register。
3. preflight 输出 `task_registration_source`，区分 `ACTIVE`、
   `COMPLETED_CLOSEOUT_ONLY`、`NONE` 与 `READ_ONLY`。
4. canonical 与 installed skill bundle 必须 byte-identical。

## 验收

- completed-only task 在 clean local `main` 的 coordinator `CLOSEOUT --remote-action`
  通过；
- 同一 completed-only task 在 `LANE` 继续返回 `TASK_NOT_REGISTERED`；
- active task 的既有行为不回归；
- focused skill tests、bundle parity、skill validation、Architecture 与 Contract 通过；
- 不执行 merge/rebase/force-push，不改变 production、broker、数据、策略或投资解释。

## 完成证据

- focused skill：`30 passed`；
- canonical/installed parity：`PASS / 5 files`；
- Architecture：`684 passed`；
- Contract：`275 passed`；
- OPS-069 的真实 completed-only closeout 已以
  `task_registration_source=COMPLETED_CLOSEOUT_ONLY` 通过并普通推送。
