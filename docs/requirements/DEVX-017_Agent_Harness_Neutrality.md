# DEVX-017：Agent harness 中立化（pi 迁移前置）

最后更新：2026-09-23

稳定任务 ID：`DEVX-017_AGENT_HARNESS_NEUTRALITY`

状态：`PROPOSED`

总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`

## 1. 问题

仓库规则、skills、合同字段和遥测都假设执行者是 Codex：AGENTS.md 点名 Codex；skills 放在
`tools/codex_skills` 并与 `$CODEX_HOME` 做一致性校验；证据合同里有 `PROJECT_OWNER_CURRENT_CODEX_DIALOG`、
`codex_*_coordinator` 等取值；workflow-health 从 `~/.codex/sessions` 统计 token；Web Pro review skill
依赖 Codex 自带的浏览器控制。换成其他 harness 后，新证据要么只能冒用 Codex 名义，要么被校验拒绝。

## 2. Owner 决定

`owner_decision:GOV-007:2026-09-23:pre_migration_convergence_v1` 第 3 条：`codex/` 分支前缀和已有
`*_CODEX_*` 枚举值保留，当作历史命名；本任务只做新增与抽象，不改旧值。

## 3. 目标

1. AGENTS.md 把 "Codex" 改为 "the coding agent"，保留全部规则。
2. skills 迁到 `.agents/skills/`（pi 和 Codex 都能发现）；bundle parity 的根目录可配置；同步更新
   compatibility authority 和测试。
3. 新证据 schema 增加 `agent_harness` 字段（`codex|claude_code|pi|human`），旧枚举值保留且只读。
4. workflow_health 的 token collector 抽象成接口，Codex 作为其中一个实现；pi 的实现迁移后再做。
5. runbook 写明 `thread_id` 与其他 harness 会话 id 的映射。
6. Web Pro review skill 标注为 Codex-only，或改为人工流程。

## 4. 依赖

DEVX-016（先降低发布成本，再做这组跨文件改动）。

## 5. 验收标准

- 旧证据全部仍能通过校验；新证据可以写 `agent_harness` 而不冒用 Codex 身份；
- skills 在新位置可被发现，parity 校验通过；
- workflow-health 在没有 `~/.codex/sessions` 时降级为"遥测不可用"而不是 BLOCKED；
- 相关测试与 `docs/system_flow.md`（若数据流受影响）更新。

## 6. 进度

- 2026-09-23：登记（GOV-007 P0-B）。GOV-007 期间的临时做法：actor 使用中性的
  `integration-coordinator`，任务备注写明 `agent_harness=claude_code`。
