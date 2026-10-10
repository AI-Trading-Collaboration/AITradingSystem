# OPS-082：确定性 Windows 调度替代 Codex automation

最后更新：2026-10-11

稳定任务 ID：`OPS-082_DETERMINISTIC_WINDOWS_SCHEDULER`

状态：`PROPOSED`

原始总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`

GOV-008 后依据：`docs/requirements/GOV-008_Research_First_Governance_Refactor.md` 第 19～21 节；
`docs/operations/operations_runbook.md` 第 1、3 节；任务记录
`tasks/OPS-082_DETERMINISTIC_WINDOWS_SCHEDULER.yaml`。

## 1. 问题

生产 daily 的入口 `aits ops daily-run` 本身是确定性的；GOV-008 后，运维手册第 1 节仍记录：此前外部触发是
Codex automation `aitradingsystem-pit`，在独立运行副本里调用 `aits ops daily-run`。pi 没有调度器；若不重新指定外部触发者，
daily 的启动仍依赖 owner 对 Codex automation 的启停决定。

GOV-008 已删除原设计依赖的旧发布与调度机制，旧方案不能按 2026-09-23 文本直接实施：

- release promotion / release candidate / canary / promote 与旧 validation tier、fence、lease 机制已删除；发布改由
  `python tools/gov008/ship.py` 完成。
- `ops_release_promotion.py`、`ops_scheduler_checkout.py`、scheduler checkout preflight、Codex scheduler observation 已删除。
- 旧 runbook 中的 deployment acceptance 命令已删除；`daily-run` 不再持有 checkout guard，`--manual-execution` 选项保留但已无作用。
- workflow health 遥测与周任务已删除。

## 2. Owner 决定

`owner_decision:GOV-007:2026-09-23:pre_migration_convergence_v1` 第 2 条：改为确定性调度，过渡期保留
Codex automation。本任务需要改动 AGENTS.md / runbook 中"不得使用 Windows 任务计划程序"的现行规则，
实施前在本文档写明规则变更并取得 Owner 确认。

GOV-008 后，AGENTS.md 与 runbook 已不再维持上述旧禁令；但 OPS-082 的具体调度规则、Codex automation 停用条件与恢复路径变更，
仍需 owner 另行确认。本文只整理现状，不新增 owner 决定。

## 3. 目标

GOV-008 后的方向只记录为待 owner 确认的事实性候选，不在本文替 owner 作新决定：

1. 外部入口仍保持唯一：一个确定性系统调度入口调用一次 `aits ops daily-run`，weekly / biweekly / monthly / ad hoc 任务继续通过
   `daily-run` 的日期与条件门禁派发，而不是登记成独立系统计划任务。
2. 当前候选是 Windows Task Scheduler 调用一个受控 PowerShell 包装脚本，在干净的 `main` checkout / runtime 上设置环境合同，调用
   runtime 的 `aits.exe ops daily-run` 一次，并用代码生成状态摘要；LLM 至多做中文润色，不参与成功/失败判断。
3. `scheduler-observe-windows` 与新的 observation schema 如仍需要，必须重按 GOV-008 后的运维手册设计；它不再替代已删除的
   release promotion、scheduler checkout preflight 或 Codex scheduler observation 流程。
4. 桥接目标仍是新调度通过一次 ordinary daily 验收后停用 `aitradingsystem-pit`；停用前任一时刻只能有一个调度来源。

## 4. 依赖

GOV-008 第 21 节把 OPS-082 标为下一个要做的运营任务；OPS-078（Codex automation 的隔离 carrier 与同日 rescue 窗口）建议并入
OPS-082，由 owner 决定新调度是否保留第二个 rescue 窗口。OPS-070、OPS-072、OPS-073、OPS-074、OPS-079 等真实 daily
运营验收项建议改为在 OPS-082 新调度下第一次 ordinary daily 全链后验证。

终止恢复仍有一个明确限制：`aits ops daily-run --recovery-parent-run-id ... --recovery-from-step ... --recovery-reason-code ...`
目前仍要求环境变量 `AITS_OPS_DEPLOYMENT_RECEIPT` 指向 deployment receipt；生成该 receipt 的 deployment acceptance 命令已删除，
所以现阶段只能使用现存 receipt。OPS-082 后续设计需要移除或替代这一依赖，但本文不决定具体实现。

## 5. 验收标准

- 新调度触发的 ordinary daily 全链 PASS，DQ 门禁与 report 质量状态可见；
- 任一时刻只有一个调度来源；
- Codex automation 停用后，日常运行不依赖 Codex automation；
- 终止恢复不再依赖已删除的 deployment acceptance / deployment receipt 生成路径；
- 如调度或恢复行为改变，`docs/system_flow.md` 与 runbook 同步更新；
- production_effect 只限 daily 的既有运营边界，不新增交易或 broker 行为。

## 6. 仍需 Owner 确认的问题

- 是否确认 Windows Task Scheduler 为唯一外部调度入口，并停用 Codex automation `aitradingsystem-pit` 的条件与时间点。
- Windows 计划任务的触发时间、运行身份、环境变量/secret 合同、checkout/runtime 路径和失败通知边界。
- 新调度是否保留 OPS-078 提到的同日 rescue 窗口；若保留，如何保证不形成第二个独立外部调度入口。
- 终止恢复如何去掉 `AITS_OPS_DEPLOYMENT_RECEIPT` / deployment receipt 依赖，以及现存 receipt 在过渡期的使用边界。
- ordinary daily 验收成功后，哪些 OPS-070/072/073/074/079 项转入验证或关闭。

## 7. 进度

- 2026-09-23：登记（GOV-007 P0-B）。
- 2026-10-11：按 GOV-008 后现状重述记录：原方案依赖的 release promotion、scheduler checkout preflight、Codex scheduler observation、deployment acceptance 与 workflow health 机制已删除；当前方向是唯一外部调度入口调用一次 `aits ops daily-run`，Windows Task Scheduler 与包装脚本仍需 owner 确认；终止恢复仍受 `AITS_OPS_DEPLOYMENT_RECEIPT` / 现存 receipt 限制。
