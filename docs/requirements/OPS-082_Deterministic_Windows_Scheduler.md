# OPS-082：确定性 Windows 调度替代 Codex automation

最后更新：2026-09-23

稳定任务 ID：`OPS-082_DETERMINISTIC_WINDOWS_SCHEDULER`

状态：`PROPOSED`

总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`

## 1. 问题

生产 daily 的入口 `aits ops daily-run` 本身是确定性的，但目前由 Codex automation `aitradingsystem-pit`
按一段长 prompt 触发，并由 LLM 判断失败类型。配置和 `ops_scheduler_checkout.py` 写死了
`codex_automation` provider，发布流程还会调用 Codex automation。迁移到 pi 后（pi 没有调度器），不改的话
daily 就没有触发者。

## 2. Owner 决定

`owner_decision:GOV-007:2026-09-23:pre_migration_convergence_v1` 第 2 条：改为确定性调度，过渡期保留
Codex automation。本任务需要改动 AGENTS.md / runbook 中"不得使用 Windows 任务计划程序"的现行规则，
实施前在本文档写明规则变更并取得 Owner 确认。

## 3. 目标

1. 新 provider（例如 `windows_task_scheduler`），`windows_task_scheduler_entries_allowed: true`。
2. PowerShell 包装脚本：设置环境合同 → 调用 runtime 的 `aits.exe ops daily-run` 一次 → post-stage →
   gap 对账 → 用代码生成摘要。可选调用 LLM 做中文润色，但 LLM 不参与成功/失败判断。
3. `scheduler-observe-windows`（读取 `schtasks /query /xml`）替代 `scheduler-observe-codex`，并定义新的
   observation schema；runbook 与 release-promotion 流程同步更新。
4. 桥接：新调度通过一次 ordinary daily 验收后，停用 `aitradingsystem-pit`。停用前两者不能同时触发。

## 4. 依赖

OPS-077/078/081 达到 `OPERATIONALLY_ACCEPTED`（先用现行调度得到一次正常 daily 作为对照基线）。

## 5. 验收标准

- 新调度触发的 ordinary daily 全链 PASS，DQ 门禁与 report 质量状态可见；
- 任一时刻只有一个调度来源；
- Codex automation 停用后，scheduler 观察、release promotion 和 workflow-health 不再依赖 Codex；
- `docs/system_flow.md` 与 runbook 更新；
- production_effect 只限 daily 的既有运营边界，不新增交易或 broker 行为。

## 6. 进度

- 2026-09-23：登记（GOV-007 P0-B）。
