# GOV-007：迁移 pi 前的工程收敛总控

最后更新：2026-09-24

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

追加决定（2026-09-24）：

- `owner_decision:GOV-007:2026-09-24:p0c_unreplayable_transaction_disposition_v1`：采纳"受管处置记录"方案，
  不迁移 ops077 worktree 的 lease store、不改 fence 工具；E3 判定按第 3 节改写。
- `owner_decision:GOV-007:2026-09-24:main_checkout_lease_arbiter_migration_v1`：同意新增 sibling
  checkout 迁移入口，并授权对 `D:/Work/AITradingSystem` 的 lease arbiter store 执行一次显式静默迁移
  （旧目录原样归档、业务 lease 事件不变）。

关于第 5 条的记录方式：在 DEVX-017 引入 `agent_harness` 字段之前，canonical 事件的 actor 继续使用
中性角色 `integration-coordinator`；实际执行 harness 在每个变更的任务备注和本文档进度记录中写明
（`agent_harness=claude_code`），不冒用 Codex 名义。

## 3. 目标状态（全部满足才进入 pi 迁移）

| # | 验收项 | 怎么核对 |
|---|---|---|
| E1 | 主 checkout 在 `main`，`main = origin/main`，workflow-health 回执为 PASS | 回执 JSON，`git rev-parse` |
| E2 | 没有只存在于本地的唯一内容：未合并分支要么合入，要么在 bundle 与 canonical 记录中登记后删除；worktree 只保留在用的 | 分支、worktree、refs 清单 |
| E3 | 没有可回放的非终态 publication 事务，没有未过期的 ACTIVE lease（正在执行的除外）；无法用官方工具收口的遗留事务已原样归档并在第 6 节登记处置 | fence 与 lease 的 replay，第 6 节处置表 |
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

### P0-B 任务登记（2026-09-23，已发布，见下节结果）

- 登记 DEVX-016、OPS-082、DEVX-017 和本任务；新增 4 份需求文档。
- GOV-006 的需求文档没有改：它的 SHA-256 冻结在 `inputs/architecture/arch_004_compatibility_baseline.yaml`
  中，改动它需要新增 baseline 段落。N2/N3 并入的记录只保存在本文档；阶段5 执行 N2/N3 时再同步
  GOV-006 的任务行和文档。
- 不改代码和行为；`docs/system_flow.md` 不需要更新。
- 使用既有 coordinator worktree `D:\Work\AITradingSystem_devx014_source_preservation`，不新建临时工作区。
- 执行环境：必须使用项目 venv（`D:\Work\AITradingSystem\.venv`，Python 3.11.9），并把 `PYTHONPATH` 指向
  当前 worktree 的 `src`。系统 Python 3.14 会在 lease arbiter 的文件身份检查上失败
  （`path.stat()` 与 `os.fstat()` 字段不一致）；venv 的 editable 安装指向主 checkout 的 `src`，
  主 checkout 不在 main 时会加载过期代码。P0-A 的 READ_ONLY 预检和 lease 回放是在这种过期代码下运行的，
  用正确代码重新回放的结果同样是 ACTIVE lease 0。
- 登记方式：fence 事务一次只允许修改自身 task_id，所以按 TRADING-2561/2562 的既有做法，
  为 GOV-007、DEVX-016、OPS-082、DEVX-017 各开一个只用于登记的事务，登记后以 failed 终态收口；
  生成器重建、正式验证和发布在事务 `gov-007-p0b-publication-20260923-v1` 中完成。这 4 个 FAILED
  不是验证失败，记为 DEVX-016 的简化样本。

### P0-B 结果（2026-09-24，已发布）

- main `03d10b4a2` → `1278c6be0`（普通推送，local main = origin/main = candidate）；发布事务
  `gov-007-p0b-publication-20260924-v4` 为 RELEASED。Full：12903 passed / 6 skipped / 0 failed
  （`failure_fix_rerun`，parent 为 v2 的失败 Full）。
- 操作要点（记入 runbook 候选，供后续 coordinator 使用）：
  - Full 必须显式设置 `AITS_NAMED_DQ_PUBLICATION_TRANSACTION` 与 `AITS_NAMED_DQ_SOURCE_LEASE_ID`
    为当前事务路径与其 lease id，并先单独跑 named DQ 相关测试作为父关联正例；缺值时 43 个测试按设计
    typed FAIL。
  - 发布事务必须声明 `outputs/architecture/trading_2564_s3b_prospective_capture/synthetic` 与
    `outputs/architecture/trading_2560_composer_known_snapshot/synthetic`，否则 named DQ 父关联报
    `NAMED_PARENT_LEASE_SCOPE_INVALID`。
  - 生成器若产生 tracked 改动，提交后候选会变化，Full readiness 会以 Atlas 绑定不一致拒绝；应在所有
    tracked 改动都已提交的 lane head 上 acquire，再跑生成器（此时不再产生改动）。

### P0-C 遗留事务处置（2026-09-24）

| 事务 | 处置 | 原因 | 证据 |
|---|---|---|---|
| `trading-2557-f1-failure-fix-20260903-v6` | 官方 `release --outcome failed`，终态 FAILED | 已被后续尝试替代，无任务依赖 | 主 checkout `outputs/architecture/gov_007_pre_migration/p0c_abandoned_transaction_*.json` |
| `devx-009-publication-20260824-v1` | `UNREPLAYABLE_LEGACY_POLICY`：原样归档，不再尝试收口 | 创建时的 fence 策略哈希 `4ae0d15a…` 对应一份从未提交的草稿，无法回放，官方 release 必然拒绝 | `D:/Work/AITradingSystem_backups/2026-09-23/p0c_unreplayable_transactions/`，tar SHA-256 `93f998b7…295a` |
| `ops-077-scheduler-binding-repair-20260909-v2` | `LEGACY_STORE_UNMIGRATED`：连同其 worktree 的 lease store 原样归档 | 该 worktree 的 lease store 为旧格式，写入需先迁移；按 owner 决定不迁移 | 同上目录，tar SHA-256 `a267fc7b…55a5` |

两个未收口事务都没有对应的 ACTIVE lease，也没有任务依赖。影响：只在各自 checkout 的运行时目录里保留
非终态记录；退出条件：所在 worktree 在 P1-B 按审计流程移除（ops077），或主 checkout 的历史运行时目录
随清理归档（devx-009）。OPS-077 以后从 coordinator checkout 用新事务继续。

### P1-A 前置：主 checkout lease store 迁移（2026-09-24）

- 问题：workflow-health 固定检查 `D:/Work/AITradingSystem` 位于 main 且 HEAD=main=origin/main；fence 的
  推送阶段也要求 coordinator checkout 位于 main。同一时间只有一个 checkout 能在 main 上，所以主 checkout
  必须成为唯一 coordinator。但它的 lease store 仍是旧目录格式（只有 devx014 worktree 在 2026-09-06 迁移过），
  不迁移就无法开新事务。
- DEVX-014 的迁移入口写死了 DEVX-014 的 owner 指令，且其授权明确排除物理主目录，不能借用。
- 方案：新增 `scripts/architecture_arch005_sibling_lease_store_migration.py` 与授权清单
  `config/architecture/lease_arbiter_sibling_migration_authorizations.yaml`（仅登记主 checkout 一条），复用
  未改动的 `migrate_legacy_arbiter`；DEVX-014 入口和它在 compatibility authority 中的合同不变。
- 只读预检（2026-09-24）：目标 store 回放 PASS，453 个 lease head，ACTIVE 0；旧 owner 为 RELEASED
  （2026-09-05），SHA-256 `8a3a43de…fe82`，格式符合迁移函数要求。
- 执行时机：本变更发布推送后、事务释放前（`CLEANUP_PRE`），避开 `aitradingsystem-pit` 的 09:30/17:30。
  执行前把整个 leases 目录打包进备份。回滚：把 `arbiter-migrations/<id>/legacy` 挪回 `arbiter.lock`，
  删除新建的锚点文件、`arbiter.owner.json` 与迁移目录。
- 迁移后：devx014 改为 detached，主 checkout 把 `Claude outputs/` 移入备份后切回 main，跑 workflow-health；
  devx014 的运行时证据保全后按清理流程移除。

### P1-A 主 checkout 回到 main（2026-09-24，已完成，E1 满足）

- 主 checkout lease store 迁移：migration id `gov-007-main-checkout-os-arbiter-20260924-v1`，PASS；迁移前后
  453 个 lease head 不变、ACTIVE 0；旧目录保留在 `arbiter-migrations/<id>/legacy/`，迁移前整目录备份
  `D:/Work/AITradingSystem_backups/2026-09-24/main_checkout_lease_store_pre_migration/leases.tar`
  （SHA-256 `14618b60…d8f4c8`）。发布事务 `gov-007-sibling-migration-publication-20260924-v1` RELEASED，
  main `d7a20519`。
- devx014 worktree 改为 detached，主 checkout 切回 main（HEAD=main=origin/main）；`Claude outputs/`
  已确认与备份一致后删除。
- `aits reports ensure-workflow-health --as-of 2026-09-24`：回执 PASS / GENERATED，无 blocker；验证为
  PASS_WITH_WARNINGS（1 处遥测缺口，属 DEVX-017 范围）。
- devx014 worktree 不移除：DEVX-014 任务行登记其为 TRADING-2564 S2b 唯一 coordinator，退出条件是 S2b
  发布并完成证据保全，当前未满足。此后主 checkout 为唯一 coordinator。

### P1-B 分支与 worktree 清理第一批（2026-09-24）

- 删除 50 个已是 main 祖先的分支、20 个已被替代且任务为 DONE 的分支（逐个核对 bundle SHA）；移除
  worktree `AITradingSystem_trading2522_integration`（满足 TRADING-2522 登记的退出条件，忽略内容整体
  归档，tar SHA-256 `de8a22af…6c19`）；删除两个 stash（已在 stash bundle）。
- 保留：所有在途或未终态任务的 worktree；ops073/ops078/ops081 等待 OPS 运营验收；
  ops_runtime_20260725 待单独证据审计；t2463 待 owner 另行授权释放排除项；`refs/codex/*` 待停用 Codex 后删除。
- 记录：`D:/Work/AITradingSystem_backups/2026-09-24/P1-B_cleanup_record.md`。

### Owner 决策包（2026-09-24）

决策 id：`owner_decision:GOV-007:2026-09-24:decision_pack_v1`。Owner 选择"全部按建议默认值批准"。
执行口径如下（第 13 条在第 1～12 条发布后单独执行）：

| # | 任务 | 处置 |
|---|---|---|
| 1 | OPS-062 | 本机全盘未找到 2026-07-16 exact archive；按批准处置：canonical 2026-07-16 永久保持 FAILED 并标记为缺口，不做 canonical 恢复；受限重建已获批准但无下游用途，不执行 → DONE |
| 2 | TRADING-2563 | 授权一次项目根内的 corrected DQ retry（R1，有界、零下单）→ IN_PROGRESS，待执行 |
| 3 | TRADING-2542 / A / B / C | DEFERRED，退出条件：迁移 pi 后重启 growth action-value 研究线 |
| 4 | TRADING-2542H、TRADING-2554 | DEFERRED，同上 |
| 5 | TRADING-2527 | DEFERRED，退出条件：owner 给出 human comprehension pilot policy |
| 6 | TRADING-1155～1164 | DEFERRED，退出条件：owner 提供外部平台导出 |
| 7 | TRADING-505～520、511A～511D、511E～520 | 按 hard-stop checkpoint 收口为 DONE，结论为 inconclusive/blocked 也视为终态 |
| 8 | TRADING-420～428 | DROPPED（dynamic v3 rescue 已不是主线） |
| 9 | PROD-002 | 采用 RISK-008 的 backlog-only 边界 → DONE |
| 10 | PROD-005 | 批准当前 production rule baseline → DONE；promotion/retirement 条件以后另议 |
| 11 | THESIS-002 | DEFERRED |
| 12 | 11 条 BLOCKED_EXTERNAL | DEFERRED，退出条件：外部条件出现时重开 |
| 13 | VALIDATING 批量规则 | 实现完成、已有 formal PASS、只等 owner 复核且 30 天无新证据 → DONE（`closed_without_owner_review`）；等 forward/shadow 样本 → DEFERRED；涉及 production/broker/阈值的保留给 owner 逐条复核 |

## 7. 退出条件

E1–E10 全部满足，迁移到 pi 的第一个低风险任务完成完整的"预检 → 提交 → 合入 main → 推送"。
