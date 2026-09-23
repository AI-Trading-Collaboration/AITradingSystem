# GOV-007：迁移 pi 前的工程收敛总控

最后更新：2026-09-23

稳定任务 ID：`GOV-007_PRE_MIGRATION_CONVERGENCE_PROGRAM`

状态：`IN_PROGRESS`

## 1. 背景与目标

项目计划把 coding agent harness 从 Codex 迁移到 pi（pi.dev coding agent）。迁移前需要先收敛在途工作、
消除只存在于本地的唯一内容、降低治理开销，并去掉对 Codex 调度和身份的硬依赖。本任务是这组工作的总控，
只负责目标状态、阶段顺序、子任务登记和进度记录；具体实现由子任务承担。

本任务不改变任何评分、回测、DQ/PIT、研究窗口、阈值、production 或 broker 行为。

## 2. Owner 决定

决策 id：`owner_decision:GOV-007:2026-09-23:pre_migration_convergence_v1`

1. DEVX-015 做完全部 106 项验收，不缩小范围；
2. 调度改为确定性调度（OPS-082），过渡期保留 Codex automation `aitradingsystem-pit`；
3. `codex/` 分支前缀和已有 `*_CODEX_*` 枚举值保留，当作历史命名，不改名；
4. 迁移前的默认治理模式是"单 agent + 受保护 main + CI"（DEVX-016）；
5. 执行者：原定由 Codex 执行、Claude 只读验收。2026-09-23 Owner 追加授权由 Claude Code
   直接执行后续任务，Codex 不再修改文件。

关于第 5 条的记录方式：在 DEVX-017 引入 `agent_harness` 字段之前，canonical 事件的 actor 继续使用
中性角色 `integration-coordinator`；实际执行 harness 在每个变更的任务备注和本文档进度记录中写明
（`agent_harness=claude_code`），不冒用 Codex 名义。

## 3. 目标状态（全部满足才进入 pi 迁移）

| # | 验收项 | 怎么核对 |
|---|---|---|
| E1 | 主 checkout 在 `main`，`main = origin/main`，workflow-health 回执为 PASS | 回执 JSON，`git rev-parse` |
| E2 | 没有只存在于本地的唯一内容：未合并分支要么合入，要么在 bundle 与 canonical 记录中登记后删除；worktree 只保留在用的 | 分支、worktree、refs 清单 |
| E3 | 没有非终态的 publication 事务，没有未过期的 ACTIVE lease（正在执行的除外） | fence 与 lease 的 replay |
| E4 | DEVX-015 为 DONE（106/106），OPS-080 达到 `OPS080_ENGINEERING_READY` 并完成运营验收 | 任务注册表，acceptance manifest |
| E5 | OPS-077/078/081（以及适用的 072–074）为 `OPERATIONALLY_ACCEPTED` | 新的 ordinary daily 全链 PASS |
| E6 | 每个 IN_PROGRESS 任务都已处理：终态，或显式 DEFERRED / BLOCKED 并写明退出条件，且没有进行中的 lane | 注册表 |
| E7 | DEVX-016 治理简化已落地：精确计数 ratchet 改为单调检查，发布流程精简 | 需求文档与测试 |
| E8 | OPS-082 确定性调度已通过运营验收，Codex automation 已停用 | 调度观察记录，daily 回执 |
| E9 | DEVX-017 harness 中立化：AGENTS.md 中性化、skills 迁到 `.agents/skills`、新证据带 harness 字段、token 遥测来源可替换 | 代码、测试 |
| E10 | 清理收尾：owner 决策包已处理，VALIDATING / BASELINE_DONE 大幅收敛，删除 shadow 任务存储，拆分超大文档，处理陈旧根文档 | 注册表统计，文件大小 |

## 4. 阶段顺序（单 lane 串行）

```
阶段0 冻结/备份/登记 ──► 阶段1 收敛在途（DEVX-015 做完、OPS 验收、分支清理）
      ──► 阶段2 DEVX-016 治理简化 ──► 阶段3 OPS-082 确定性调度
      ──► 阶段4 DEVX-017 去 Codex 化 ──► 阶段5 GOV-007 清理收尾 ──► 迁移 pi
```

Owner 决策包可以与阶段0/1 并行处理。DEVX-016 排在 DEVX-015 之后，因为 DEVX-015 正处在最终候选阶段，
途中改动 ratchet 和 fence 合同会触发串行合同波次和重算。

## 5. 子任务

| ID | 标题 | 优先级 | 初始状态 | 依赖 | 需求文档 |
|---|---|---|---|---|---|
| DEVX-016_SINGLE_AGENT_GOVERNANCE_SIMPLIFICATION | 单 agent 默认治理与 ratchet 单调化 | P0 | PROPOSED | DEVX-015 DONE | `docs/requirements/DEVX-016_Single_Agent_Governance_Simplification.md` |
| OPS-082_DETERMINISTIC_WINDOWS_SCHEDULER | 确定性 Windows 调度替代 Codex automation | P0 | PROPOSED | OPS-077/078/081 运营验收 | `docs/requirements/OPS-082_Deterministic_Windows_Scheduler.md` |
| DEVX-017_AGENT_HARNESS_NEUTRALITY | Agent harness 中立化（pi 迁移前置） | P1 | PROPOSED | DEVX-016 | `docs/requirements/DEVX-017_Agent_Harness_Neutrality.md` |

GOV-006 的 N2（按等待条件/任务族收敛）和 N3（清理当前路线语义）并入本任务阶段5。

DEVX-015 的"做完全部 106 项"决定不在本变更里写入 DEVX-015 的任务行：DEVX-015 的在途分支
`codex/devx-015-main6498-reconciliation` 正在修改同一个 canonical 任务 fragment，现在写入会给 DEVX-015
集成制造一次额外的领域重叠。该决定在阶段1（P1-C）DEVX-015 自己的下一次发布中写入。

## 6. 阶段0 进度

### P0-A 盘点与仓库外备份（2026-09-23，READ_ONLY，已完成）

- 备份目录：`D:\Work\AITradingSystem_backups\2026-09-23\`（仓库外，未提交）；全部文件的 SHA-256 在
  `SHA256SUMS.txt`。
  - `aits-all-refs.bundle`：160 个 ref，verify OK，SHA-256
    `1092fceb1044b2fbd1f12376aa246b15b019c08e604ba23d1ce2b43bb1edc31b`；
  - `aits-stash-reflog.bundle`：stash@{0}/stash@{1}，verify OK，SHA-256
    `ba820d23fe406f1c16a5dd1b0b53cb176aab2ab5eb2203bef25313a6d18b53ba`；
  - `worktree_dirty/`：devx015_checkpoint、ops080_input_closure、trading2559_integration 的未提交内容；
  - `codex_state/`、`codex_chat_evidence/`：Codex automation 配置与 memory、DEVX-015 "当前 chat" 证据；
  - 盘点报告 `P0-A_inventory_report.md`，SHA-256
    `21d13a2a5497db75953c59504d9b70000e2335722e2589049a0fc8e95d32e7ed`。
- 分支分类（43 个领先 main 的分支）：UNIQUE_KEEP 3（DEVX-015 main6498、OPS-077 scheduler-binding-repair、
  TRADING-2564 S2b）、UNIQUE_ARCHIVE_ONLY 1（TRADING-2526 browser acceptance 延期决定）、SUPERSEDED 33、
  内容已在 main 6；另有 57 个分支是 main 的祖先。
- 非终态 publication 事务 3 个：`devx-009-publication-20260824-v1`、`trading-2557-f1-failure-fix-20260903-v6`、
  `ops-077-scheduler-binding-repair-20260909-v2`；S4D lease store ACTIVE lease 0。
- Codex automation：`aitradingsystem-pit` ACTIVE（09:30/17:30），另两个 PAUSED。

### 审计事件：known-unrelated exclusion 路径被直接读取（2026-09-23）

- 事件：Owner 要求清理主 checkout 中 `docs/research/growth_tilt_owner_diagnosis_pack.md` 的未提交改动。
  该路径登记在 `config/architecture/arch_005_s4d_checkout_guard.yaml` 的 `known_unrelated_exclusions`。
  执行 `git restore` 之前，Claude Code 对该路径直接运行了一次 `git diff`。
- 影响：输出为空（改动只是 CRLF 换行差异），没有显示、hash 或复制文件内容；随后按 Owner 指示
  `git restore` 了该文件。
- 后续：该文件在主 checkout 已干净。是否移除这条 exclusion 在阶段5 处理；此后仓库级检查一律使用
  `architecture_arch005_checkout_guard.py worktree-audit`。

### P0-B 任务登记（本变更）

- 登记 DEVX-016、OPS-082、DEVX-017 和本任务；新增 4 份需求文档；GOV-006 记录 N2/N3 并入本任务。
- 不改代码和行为；`docs/system_flow.md` 不需要更新。
- 使用既有 coordinator worktree `D:\Work\AITradingSystem_devx014_source_preservation`，不新建临时工作区。

## 7. 退出条件

E1–E10 全部满足，迁移到 pi 的第一个低风险任务完成完整的"预检 → 提交 → 合入 main → 推送"。
