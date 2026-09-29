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
| 2 | TRADING-2563 | 授权已记录但前提过时：该 corrected DQ retry 已按独立精确授权执行过一次且 FAIL（evaluated 至 2026-07-23，receipt `dq_execution_d4229d2a…`，见 TRADING-2564 需求 §2），不再执行；任务改为 DEFERRED，依赖 TRADING-2564 提供覆盖 as-of 的可接纳 DQ 输入 |
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

### 决策包第13条执行（2026-09-24）

Owner 在对话中确认分类并授权执行与推送（"确认，按这个分类执行并推送"）。执行前 VALIDATING 共 273 条：

| 处置 | 数量 | 说明 |
|---|---|---|
| DONE（`closed_without_owner_review`） | 216 | 实现已在 main、相关测试包含在最近一次 Full PASS 中、仅剩 owner 复核且 30 天以上无新证据；不代表批准任何 promotion、paper、production 或 broker 动作 |
| DEFERRED | 15 | 等待 forward/shadow 样本成熟；退出条件：迁移 pi 后重启对应研究线或样本成熟到可评估时重新打开 |
| 保留 VALIDATING（owner 逐条复核） | 33 | 涉及 production 配置/权重、paper/shadow/仓位、阈值/gate/promotion 策略 |
| 保留 VALIDATING（不在本规则范围） | 9 | 在途运营验收与 release，由 P1-E 处理 |

- "已有 formal PASS" 的口径：这些旧任务早于正式验证分级，约 50 条的任务行未记录验证结果；判定依据为实现在 main 且测试包含于最近一次 Full PASS。
- "30 天无新证据" 的判定：任务文本中出现的最大日期与需求文档最后提交日期均早于 2026-08-25。
- DEFERRED：TRADING-1119_to_1128, TRADING-1141_to_1154, TRADING-151_to_155, TRADING-156_to_160, TRADING-174_to_178, TRADING-179_to_183, TRADING-184_to_188, TRADING-189_to_198, TRADING-760_to_764, TRADING-765_to_769, TRADING-775_to_779, TRADING-837, TRADING-894_to_910, TRADING-911_to_922, TRADING-923_to_932。
- 保留给 owner：CALIBRATION-003, CALIBRATION-004, CALIBRATION-005, LLM-005, TRADING-078, TRADING-082, TRADING-087, TRADING-126_to_130, TRADING-131_to_135, TRADING-136_to_140, TRADING-199_to_203, TRADING-204_to_208, TRADING-209_to_213, TRADING-214_to_218, TRADING-219_to_223, TRADING-2274, TRADING-2275, TRADING-2276, TRADING-2277, TRADING-229_to_233, TRADING-350, TRADING-693, TRADING-695, TRADING-696, TRADING-697, TRADING-698, TRADING-699, TRADING-700, TRADING-701, TRADING-707, TRADING-708, TRADING-724, TRADING-834。
- 运营范围外：DATA-001, OPS-070, OPS-072, OPS-073, OPS-074, OPS-077, OPS-078, OPS-081, PROD-004。
- 执行方式：每个任务一个只用于登记的 fence 事务（登记后以 failed 终态收口），change id `gov-007-owner-decision-pack-item13-20260924-v1`；分类清单与逐任务事务/提交记录在主 checkout
  `outputs/architecture/gov_007_pre_migration/decision_pack_item13_*`；完整的任务→提交索引见 `decision_pack_item13_commit_index_v1.json`（231 条：216 DONE / 15 DEFERRED，与分类计划逐条一致）。
- 执行中断（2026-09-25）：本机两次蓝屏打断批量执行（00:10 0xBE、01:06 0x3B）。第一次中断发生在两条任务之间，没有半写状态；
  第二次中断时 TRADING-2304 的 permit 事务刚到 `TASK_SOURCE_PRE_WRITE`，任务源尚未写入。该事务按原模式以 `failed` 终态收口，
  TRADING-2304 及其后 135 条改用 `…-20260925-v1` 事务续跑，change id 不变。本批执行只调用 fence、任务源与 git，不涉及注册表重命名；两次蓝屏的调用栈均为
  `NtRenameKey → CmRenameKey → CmpKeySecurityIncrementReferenceCount`，触发候选是 DEVX-015 的原生 `RegRenameKey` 测试，
  已另立 `DEVX-015A_HOST_REGISTRY_SINGLE_VALUE_ANCHOR_V1`（P0，PROPOSED）处理，并与本批次一同发布。本机禁止再运行原生 `RegRenameKey`。
- 发布验证中发现（2026-09-25）：architecture-fitness 中 `test_final_import_preserves_ambiguous_legacy_row_bytes_in_view`
  失败。该测试只取第一个"历史导入单元格边界有歧义"的片段，断言原始导入行仍逐字节出现在视图中；本批把其中的 TRADING-816_to_820
  合法更新为 DONE，行内容随之重渲染。实测规律：歧义片段中只有导入事件的（23 个）全部保留原始字节，已追加治理事件的全部重渲染。
  修正为逐个检查全部从未更新过的歧义片段（比原先只查一个更严格），不放宽断言。首个发布事务
  `gov-007-dp13-publication-20260925-v1` 以 failed 收口，修正后用新事务重跑全部分级与 Full。

执行中发现：Atlas live snapshot 的任务状态映射（代码 `_STATUS_MAPPING` 与 `config/atlas/live_snapshot.yaml` 必须一致）
没有 `DEFERRED`，Atlas 覆盖范围内的任务一旦转为 DEFERRED，Atlas 生成器即 fail closed。按 owner 已批准的处置补充
`DEFERRED → SKIPPED`（主动暂缓、不执行），只影响 Atlas 阅读页状态展示，不改变投资解读、研究窗口或 DQ/PIT。

### P1-C DEVX-015 lane 修复与首次集成（2026-09-25/26）

- Owner 决定写入 DEVX-015 任务行（本次发布）：做完全部 106 项验收；worker 账户范围 A（只用于测试和 Full，
  `owner_decision:DEVX-015:2026-09-24:worker_account_scope_full_only_v1`）。主机/账户/HKLM/ACL 步骤以管理员执行包交 owner 运行。
- lane `codex/devx-015-main6498-reconciliation` 上完成 v386–v389（详见 DEVX-015 V3 文档）：受保护 Full 门禁、
  DEVX-015A 父键单值锚点、活体执行仲裁重试窗口、隔离 profile 检查器下的调用方已加载源码自检（恢复 V02 语义）
  及一批陈旧测试夹具修复。首次回归的 54 个失败节点全部串行或 `-n 2` 通过；重型整链节点并行度不超过 2。
- 集成：冻结基线 `03d10b4a2`，lane head `69729e6ff`，最新 main `cbc31cdff`（DEVX-016 S1-early 发布后）。
  revalidation 计划 `integration-revalidation-d1b239e06f110ea1409c` 为 `RECONCILIATION_REQUIRED`（无阻塞、无契约冲突）；
  重叠仅限生成物、system_flow 及其封印、两份固定常量测试，在单一最新 main 候选上对齐后重新生成。
- 本机 HKCU 遗留测试键 3 个交 owner 清理（两个蓝屏遗留、一个 v387 回归中止遗留），见 DEVX-015A 进展。
- 本次 P1-C 不代表 DEVX-015 完成：剩余 13 项验收（I05×2、L03×7、X05×4）、管理员执行包、最终候选 Full/发布及 OPS-080 W3/W4。

### P1-C Codex 接手与 v8 验证准备（2026-09-28）

- Owner 在原性能优化讨论中授权 Codex 接手：先完成当前 GOV-007 P1-C 基线候选，再按
  S1 → P1 → P4 → 重测瓶颈 → 有限 T1 推进。接手仅覆盖本次基线发布和性能优化，不代表
  GOV-007 全部阶段或 DEVX-015 剩余验收完成；管理员执行包、OPS-080 运营验收边界不变。
- 接手时 HEAD 为 `970d028fd446b198459552978baf9fd8ede28dcb`，local main 与 origin/main
  均为 `cbc31cdffcfb8cda1cf106f6da2a302f87183255`。当前候选由 `fe6e27d7c` 从该 main
  建立，历史 lane 的 reconciliation 计划 `integration-revalidation-b47776469f7c72fd5d7d`
  已重新验证。继续复用当前候选和工作区，不重建 lane 或改变历史计划的 lane head。
- v7 Full 在 `b8e6eb01347a13f089ac09dd73024f367f43ae11` 上为 PASS：14503 passed、
  4 skipped、29625.67 秒。其后 `970d028fd` 修复非 DEVX-015 的 unbound Full profile
  发布检查，并将完整 runtime profile 的证据预算设为 256 MiB。原新增回归有并行 PASS
  日志，v7 的证据保持原样；旧发布事务因候选已被替代，以 FAILED 发布终态释放，不能将
  该终态描述为 v7 测试失败，也不能把 v7 当成当前候选的 Full。
- 本次先更新接手记录、重建生成物、完成便宜的 readiness/归属检查，然后在冻结候选上运行
  required tiers 与 v8 Full。正式运行期间不修改候选 tracked 文件、Python 环境或依赖。
  同一候选通过正式验证后才推进 local-main fast-forward、普通推送和 SHA 相等核验。
- 性能优化在基线发布后分别纳入 ARCH-004G2（S1）与拟登记的 DEVX-018（P1/P4）：S1 使用
  实际完整 runtime profile 更新文件耗时权重；P1 只在一次 replay 内复用纯结构校验，仍逐条
  校验事件哈希、链关系、actor、时间与状态转移；P4 保持审计能力，将重复清单移为持久化且
  不可变的引用证据，保留 v1 replay、原生文件句柄、进程与 Job 校验。各阶段单独验收，
  代表性重型用例的前后实测决定收益，不将单次 replay 的降幅外推为整个 Full 的降幅。
- A1/A2 的完整运行时托管单独设计，不与 P4 合并；代码摘要去重仅考虑单次源码验证调用内
  的 immutable code object/catalogue 重复工作，不缓存跨调用的运行时身份或校验 PASS。
- 连续性证据位于 `outputs/architecture/integration_revalidation/devx015-v389/` 的
  `codex_takeover_20260928.json`、`codex_handoff_preflight.json` 与后续 v8 运行证据。
  该目录继续保留到发布、证据归档及依赖审计完成，不新建临时 checkout。

### P1-C v8 中断后的最小修复（2026-09-28）

状态：`IN_PROGRESS`。Owner 已授权继续原接手工作；本节只修复基线验证发现的两个执行边界，
不提前实施 S1/P1/P4，不代表 DEVX-015 的 93/106 已完成，也不授权管理员、运营或交易动作。

- v8 候选 `e05581b7a0b12f620082039f3803edf2249b0bd4` 的 required tiers 通过，
  architecture-fitness 为 1525 passed / 25325.51 秒；Full 未完成，不能用于发布。
  Windows System 记录约 10:11 异常关机、11:01 重启及 bugcheck `0x0000000A`；
  本次内核原因未确定，不能把蓝屏归因于某个测试或驱动。本机原生 `RegRenameKey` 禁令保持。
- 中断前记录两项失败：发布 unchanged 恢复的
  `PUBLICATION_PREPARATION_RESOLUTION_CHANGED`（以及后续 fixture 清理的非终态拒绝）；
  task checkpoint RELEASED 变体的 `WORKFLOW_EXECUTION_JOB_PROCESS_LIST_BUDGET`。
  前者定位到两次原进程终态观测字典的全等比较；两次具体原始返回值没有留存。
  后者尚缺 API 的原始计数，须先做诊断，不能仅扩大预算或把不完整列表视为退出证明。
- 官方 `run_validation_tier.py full --recover-full --recover-full-action observe` 已确认原
  launcher 死亡、原 Job 为 ABSENT，原事务收口 FAILED、租约 RELEASED；技术状态保持
  `INSUFFICIENT`，returncode 保持 null。原进度、日志和 request/identity 保持原样；
  未伪造缺失的 Full summary。正式证明为原 v8 事务的 `full_incomplete_recovery.json`。

阶段与验收：

1. 先登记本节和 canonical 任务事件，通过新事务 preflight 后实施。
   新事务 `gov-007-p1c-v8-recovery-fix-20260928-v1` 沿原 task、coordinator 和路径声明，
   在 `TASK_SOURCE_PRE_WRITE` 绑定 v8 中断证明及原恢复证明。
2. 发布恢复只比较原请求/准备/launch 的稳定身份及仍然成立的终态事实。
   两次均须独立观察原 PID + creation time，拒绝 RUNNING、UNKNOWN、未知 lock、
   拓扑或证据变化；允许同一已死亡原进程的 EXITED/REUSED/原生错误表示合法变化。
   保留原始观测用于审计，不缓存 PASS、不共享其他候选的 Full。
   确定性回归须覆盖终态表示切换、非终态/未知结果、准备证据和锁变化。
3. Job 列表先以受控小进程诊断 `ok/error/assigned/count/capacity/active` 和被持有句柄的
   signaled 状态。仅使用任务自有 Job，不做注册表、账户或主机修改。
   修复须区分真正缓冲区不足、并发退出及无法确认的观测；保留进程身份、Job membership、
   ActiveProcesses 和全部 retained handles signaled 的退出门禁。负例覆盖权限错误、
   活跃 Job、未 signaled 句柄、越界及真正预算耗尽，不能无限重试。
   2026-09-28 原生探针的单进程、父先退出和17个子进程场景均未复现预算错误；
   原 checkpoint 的 CAPTURED/RELEASED 两个节点在短路径下通过（2 passed / 53.45秒）。
   当前不改变列表完整性判定或65536上限，只增加受限诊断，不能宣称预算错误已修复。
   `execution_failure.json` 可选新增 `job_process_list_diagnostics`，仅保存指定异常携带的
   最多13条有限查询计数、retained/active数量或固定accounting失败码。未知字段、字符串
   和不合法类型一律不输出；不保存任意异常文本。现有消费者只按完整bytes/SHA清点，
   不解析此诊断字段；原schema标识、错误码、异常优先级与release条件保持。真实worker
   退出后的确定性API seam回归验证该诊断能穿过原包装落盘，不能把它当作原生故障复现。
4. 先跑最小确定性回归和原失败节点，再按最终候选重建生成物、执行所有 required tiers
   与 Full，最后沿原 local-publish 路径普通发布并核验 SHA。新候选及新 Full 有独立身份；
   原 v8 缺失合格 parent summary，不得伪造 `failure_fix_rerun` parent。
   静态复核确认现有入口仅允许完整summary/profile，没有中断父运行入口。须先完成下述
   最小串行契约修复，不能将失败修复重跑改称第二次自然Full。

中断父运行契约修复（归属本次GOV-007基线恢复；实施前更换事务补齐声明路径）：

- 保留 `failure_fix_rerun`、whole-envelope CLI/env和原direct/portable summary验收。
  `parent_run`新增互斥的 `full_incomplete_recovery` 绑定，只接收原事务固定同目录下的
  `full_incomplete_recovery.json`；不支持portable incomplete import，不生成summary/profile
  摘要或虚构pytest退出码。状态固定INSUFFICIENT，失败依据为缺少原Full结果承诺。
- 只读重放原事务、原任务/仓库/intent、v2 FULL_DISPATCHED claim、已RELEASED租约和非空
  RESULT_RECORDED execution；后者必须没有full_result_commitment。request/candidate/Job名、
  launcher PID+creation、execution摘要及原exit须与原claim、证明严格一致。run_id来自原claim，
  不能使用事务目录名。只复用已有的受登记终态租约来源，不新建锁或选取任意旧lease根。
- 捕获证明bytes的SHA/size须同时命中原FAILED事件evidence与closeout receipt；持久receipt
  必须等于原事务和RELEASED租约推导出的receipt。绑定原proof、transaction、lease、terminal
  event、execution/request、candidate和run-id；路径逃逸、reparse/别名、替换、未知字段拒绝。
- 新事务及新候选独立冻结，并以原 `full_parent` 字节绑定消费该proof。FORMAL_VALIDATION_PRE
  和FULL_DISPATCHED最终启动前都重新验证整个父链，与已冻结parent对象exact compare。
  profile plugin共用严格union validator；不以单份JSON自述或本次通过结果代替原链。
- 不要求磁盘没有松散summary/profile：这些原字节继续保留，始终不可采纳。父运行验证不得
  调用恢复、派发、终止Job、租约写入或发布；它只提供重跑来源，不提升原v8证据资格。
- 验收覆盖原summary/import兼容、新完整原链正例，错task/candidate/request/claim/哈希/receipt、
  ACTIVE或空execution、有commitment、unstarted proof、未知/混合字段、路径逃逸及首次读取后
  证据替换的拒绝；两个派发消费点与runtime profile均须消费同一确切对象。

当前聚焦结果：发布恢复确定性red为12 failed/15 passed，修复后的首批44 passed；包括原
checkpoint两个边界与诊断持久化的54项通过（100.99秒），最后类型边界13项通过（6.44秒）。
Ruff通过。三个生产模块strict mypy仍报21项，使用HEAD影子源比对确认全部已存在；无新增，
本次不扩大到这些无关类型修复，保留 `v8_fix_typecheck_01.log` 和 `v8_fix_mypy_base/` 供后续
DEVX类型清理评估。原发布两节点的第一次聚焦运行在进入恢复步骤前被实现来源门禁拒绝，
不代表修复验收通过；根因是临时Git夹具在主仓库内向上发现主pyproject的pythonpath。

后续验证（2026-09-28）：外部短路径下原发布两个参数全部通过，2 passed / 3172.06秒；
正常终态恢复通过，index被替换的分支只登记稳定失败并允许只读重放，始终禁止继续发布。
日志为 `v8_fix_original_publication_02.log`。中断parent的协议/旧summary/import/profile组合
首次93项通过、6项因单元文件autouse替身未进入真实runner而失败；修正测试接线后，两处
真实派发消费点的12个拒绝场景全部通过（5.93秒）。原生summary-0、exit-unrecorded、
custody-0三节点通过（101.07秒），包括基于真实终态链的缺失、篡改及二次读取替换拒绝。
对应日志为 `v8_fix_parent_unit_01.log`、`v8_fix_parent_consumer_02.log`、
`v8_fix_parent_native_01.log`。新增provenance/runner严格类型检查和Ruff通过；独立静态复核
未发现新增阻塞。F2预算错误仍未复现，以上不能作为其根因已修复的声明。

下一步沿既有准备/正式事务边界提交新候选：准备事务只记录生成物和source commit；Atlas
要求已提交的exact source，因此准备记录不得标成正式生成/验证PASS。提交后结束准备事务，
新正式事务按同一顺序重建全部五个生成器并要求PASS、clean candidate和readiness通过；
再串行执行named-parent positive、contract、integration、reproducibility、architecture和Full。
新Full使用 `failure_fix_rerun` 并重新核验原v8固定proof，不能复用v8已通过的tiers代替新候选
验证。`run_codex_gov007_v9.py`仅为本次串行派发driver，不增加调度器或租约；独立复核覆盖
候选/事务/父证据、逐stage派发前检查、失败停止、已有artifact拒绝和原driver的进度归属。
当前仅完成聚焦验证，基线未发布，DEVX-015仍为93/106，S1/P1/P4仍在基线发布之后。
准备重建首次因本次system-flow说明增加9行、source seal仍为旧值而被
`RCF_SOURCE_SEAL_DRIFT`拒绝，原日志完整保留。按既有monolith source-of-truth规则只更新
system_flow的byte_count、SHA-256、Git blob与对应固定测试摘要，1490个entry边界保持不变；
inactive shadow、100% coverage、禁止silent drop与精确字节重建门禁保持。元数据变化记录
于 `v8_fix_system_flow_reseal.json`，随后从第一项重新执行声明的生成器顺序。

证据与生命周期：继续使用
`D:/Work/AITradingSystem/outputs/architecture/integration_revalidation/devx015-v389/`；
中断证据为 `codex_v8_interruption_20260928T1419.json`，恢复日志为
`codex_v8_recovery_20260928T1423.log`。诊断输出和隔离 pytest 临时目录仅建在该目录下，
使用 `v8_fix_*` / `pytest-v8-fix-*` 名称并记录命令、结果及退出条件。Git 夹具使用较短的
`D:/Work/AITradingSystem/outputs/validation_runtime/v8fix/` 作为本任务专用 basetemp 父目录；
首次长路径尝试在 Git add 阶段报 `Filename too long`，尚未进入 SUT，原日志和目录保留。
该目录不改变主机 longpaths 配置，每次命令使用独立子目录，生命周期同本节其余诊断目录。
涉及嵌套pytest的发布/Full夹具改用 `D:/Work/aits_gov007_v8fix/` 的独立短子目录（owner GOV-007，
purpose 原失败发布回归/中断parent契约验证），避免向上发现主仓库pytest配置。已有f1目录
及 `v8_fix_original_publication_01.log`（2 failed/2 teardown errors，481.54秒）保留；只纠正
临时工作区位置，不删除或覆盖证据、不改Python依赖或身份校验。仓库外目录同样须在修复
发布且证据归档核验、无进程依赖后清理，不能因位于仓库外跳过生命周期审计。
两个原失败 fixture
位于 `C:/Users/32739/AppData/Local/Temp/pytest-of-JACK/pytest-20416/`，在所需证据归档
并核对摘要前不得清理；聚焦验证使用显式的新 basetemp，避免自动保留策略删除原 fixture。
临时诊断目录在修复发布且证据完成归档、无进程依赖后按原治理规则清理。

本次修复涉及现有执行观察、恢复及上述中断父运行契约；保留现有CLI参数和schema标识，
严格扩充parent union。诊断envelope的可选字段与父链消费路径已同步写入
`docs/system_flow.md`，不改变业务数据流或提升原失败证据的资格。

### 2026-09-28 v9 中断后的来源闭包修复

v9 在 architecture-fitness 阶段已观察到失败，随后验证进程消失；系统没有再次重启。
Owner 确认重启过 Codex，但尚不能确定进程退出的因果。原日志/进度保持不变，事务已沿
原 fence 收口 FAILED/RELEASED，v9 正式 Full 未派发，main 未发布。

聚焦验证入口 127 项和源码保全/checkpoint/集成计划/架构生成物 357 项通过；剩余架构
首错诊断得到 681 passed、1 failed（363.85 秒）。具体失败为
`test_ops_081_cached_source_closure_still_rechecks_live_hash`：新增严格中断 parent 所修改的
`src/ai_trading_system/platform/validation_trigger_provenance.py` 和
`tests/test_validation_trigger_provenance.py` 未登记到当前 DEVX-015 来源闭包。

修复步骤与验收：先将两个确切路径纳入现有当前来源生成器及精确集合回归，重建生成物；
历史来源哈希保持不变，当前来源仍逐次验证，未知路径、漏项和哈希篡改仍拒绝。随后运行
完整 refactor-policy 回归，确认这一原因解释的失败范围；剩余失败单独定位，不能以此
断言原 v9 所有失败已解决。新候选需新事务及全部正式验证，不复用聚焦 PASS 或旧 Full。
本修复仅补齐已实现契约的审计来源清单，不改变数据流，故无需再次修改 system_flow。

诊断日志保存在原 devx015-v389 证据目录；仓库外 `D:/Work/aits_gov007_v8fix/` 下
v9diagnosis1/2/3 保留至证据归档校验、无进程依赖及发布后的生命周期审计完成。
补齐两路径后的完整 refactor-policy/compatibility-authority 回归为 352 passed、1 failed
（846.32 秒）；剩余 OPS-068 断言的差集仅为 `tests/test_validation_trigger_provenance.py`。
原因是旧全体后继来源范围止于 OPS-081；将其接到现有 DEVX-015 结构验证后的明确来源
集合，继续保留旧历史集合及哈希，不加入任意路径豁免。随后验证原失败断言及当前来源
hash 复核，并在正式候选上完成全套门禁。

S1/P1/P4 尚未实现，DEVX-015 仍为 93/106，不将本修复登记为运营或正式验收完成。

### 2026-09-29 v10 witness 发布竞态修复

v10 architecture-fitness 得到 1466 passed、1 failed、1 teardown error（4165.48 秒）。
`index-replaced-full-profile-publish` 的 worker 直接写入 witness，父进程仅等待文件存在，
因而在文件创建后、写完前读取到空 JSON；失败文件实测为 0 字节。随后 Job 清理导致
`LEASE_EXECUTION_NOT_TERMINAL`，保留原拒绝，不放宽终态门禁。v10 已 FAILED/RELEASED，
正式 Full 未派发；证据在 devx015-v389/v10_witness_failure_20260929，原夹具保留。

按原 GOV 任务的新准备事务先登记，再实施测试同步修复：worker 将完整 JSON 写到同目录
临时文件，关闭后原子发布 witness，父进程仍严格解析与核验内容。确定性回归须证明写入
暂停时目标不可见、发布后内容完整、写入失败不会暴露目标；不能吞 JSON 错误、增加 sleep、
跳过原 Full 或削弱进程/Job/lease 校验。之后复验原真实发布用例，新候选新事务执行全部
required tiers 与 Full，父证据仍为原 v8 incomplete Full。源码测试同步不改变系统数据流。

本阶段临时测试目录使用已登记 D:/Work/aits_gov007_v8fix/ 下 v10witness-* 子目录；
owner 为 GOV-007，退出条件为发布后证据归档校验、无进程依赖及生命周期审计完成。
S1/P1/P4 尚未实施，本修复不代表基线发布或目标完成。

实现复用现有 `write_json_atomic`，测试 worker 与确定性回归执行同一 writer source。
回归在原子替换前阻塞，验证目标不可见，再释放并检查完整 JSON；另注入 fsync 失败，
确认目标及临时文件均不残留。4 项聚焦检查通过（7.29 秒），Ruff 通过；日志为
`v10_witness_fix_unit_01.log`。真实发布用例两个参数均通过（2 passed /3742.62 秒），
日志 `v10_witness_fix_native_01.log`，使用 -n16/loadfile；原进程正常结束。
新候选全部 required tiers 和正式 Full 仍待完成，聚焦结果不替代正式验收。

### 2026-09-29 v11 Full 测试生命周期修复

候选 `935ad29c3d84cdaddb911c9637302ca71dbd9142` 的四项 required tiers 通过，
正式 Full 得到 4 failed、14597 passed、4 skipped（36553.55 秒）。原 v11 事务已
FAILED/RELEASED；正式 summary 的 provenance 为 PASS，但测试 FAIL，不能发布。
完整日志、summary/profile、原 v8 incomplete parent 及 `pytest-20495` 失败夹具均保留。

本轮按原 GOV 任务和新准备事务分两步修复，再形成新候选：

1. checkpoint 的三个失败都发生在清理函数重新打开旧 PID 后，创建时间已经不同。
   清理必须区分原实例存活、已退出、PID 复用以及无法确认；绝不能终止复用后的进程。
   以确定性回归覆盖身份分支、句柄释放和拒绝未知状态，再验证原真实中断恢复用例；
   保留真实 launcher/worker/Job、恢复结果和事件链断言，不放宽生产身份校验。
2. `expired-full-profile-publish` 在 `_admission_transaction` 的 LOCAL_MAIN_FF_PRE
   准备阶段触发 PUBLICATION_LEASE_EXPIRED，未进入预期的 live PASS → 真实过期负例。
   先核对保留事件链的续租及耗时，修复测试准备阶段的 lease 生命周期；必须先得到同一
   候选的完整 live 基线，再停止续租并等待真实墙钟过期，验证原 typed rejection。
   不修改生产时钟、不事后更改冻结策略、不缩减真实 Full 或共享不同候选的通过结果。

两步聚焦验证通过后更新记录、生成物并提交新候选；新正式事务绑定合法失败 Full 父证据，
执行全部 required tiers 与 Full。旧 v11 不重启、不覆盖；聚焦 PASS 不替代正式结果。
本次测试生命周期修复不改变业务数据流，故无需修改 system_flow。

首轮聚焦回归为 9 passed、1 failed（241.79 秒）：三个原 PID 用例及六项清理分支
通过，expired 用例暴露测试续租线程与内层 Full 的 LEASE_ARBITER_BUSY。
续租作用域因此限定到真实 profile 只读准备和只读 preflight；准备返回前停止并 join，
再由原 checkpoint 进入 atomic transition，Full 期间仅由原 runner 管理租约。
不重试仲裁失败、不放宽原生锁，原失败日志保留，新一轮单独验证 expired 用例。

第二轮 expired 聚焦回归通过（1 passed /1657.22 秒）；其内部真实 Full 为
PASS/exit0/provenance PASS（1029.9 秒），正向 admission、停止续租后的真实过期及
PUBLICATION_LEASE_EXPIRED 拒绝均通过。结合首轮 9 项 PASS，四个原失败 node 和
六项清理分支已覆盖；Ruff 通过。证据位于 devx015-v389 的
v11_fixture_fix_native_01.log 和 v11_fixture_fix_expired_02.log。
下一步为新候选全部正式验证，尚未发布，不能复用旧候选的 required tiers PASS。

临时聚焦目录归 GOV-007，位于既有 `D:/Work/aits_gov007_v8fix/` 下 `v11fixture-*`；
退出条件为发布后证据归档与摘要核对完成、无进程依赖并通过生命周期审计。原失败夹具
在保留证据完成前不清理。基线发布后的 S1、P1、P4 及重测目标仍未完成。

### 2026-09-30 v12 中断与 DEVX-018 O1/O2 并入基线候选

- 执行方改为 Claude Code（`agent_harness=claude_code`，actor `integration-coordinator`）。
  v12 候选 `01d2d14f8` 的 named-parent、contract、integration、reproducibility 通过；
  architecture-fitness 在 93%（gw4 单 worker 串行执行 publication_fence 重型尾部）时于
  2026-09-30 02:33 整树终止，无终态记录、无 artifact，Full 未派发。系统日志同一时刻显示
  Microsoft Store 更新 OpenAI.Codex 应用（02:21 因包占用 0x80073D02 失败，02:33 成功），
  driver 由 Codex 启动；因果相关但未证实，与 v9 的 Codex 重启中断同类。v12 已沿原 fence
  收口 FAILED/RELEASED：`codex_v12_interruption_20260930.json`、`codex_v12_failed_release.json`。
- Owner 决定不原样重跑，先落地 DEVX-018 的 O1+S1+O2，再以一个 GOV-007 候选完成全部 required
  tiers 与 Full；基线与 O1/O2 同次验收，替代原"基线后分阶段"安排。依据 v11 profile：墙钟
  10.15 小时对 43.2 worker 小时，loadfile 把 568 分钟的 publication_fence 文件固定在单 worker。
  DEVX-018 经独立许可事务登记（`6c121cfae`），详见
  `docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md`。
- 实施：受审清单 `config/architecture/devx_018_validation_scheduling.yaml`（8 个拆分文件、
  37 个 `real_full_chain` 函数、重型持有者上限 6）；Full plugin 的拆分调度器与标注；runtime
  profile `scheduler.split_scope` 契约及 live/candidate 字节绑定；architecture-fitness 排除
  `real_full_chain`；S1 以 v7 PASS Full 刷新 seed v26。首轮真实 xdist 集成测试暴露"持有重型
  单元的 worker 不补派"导致 xdist 末项永不执行的死锁，已改为持有者只接受一个后继单元，且上限
  按持有 worker 计；原失败保留在聚焦日志。
- 下一轮正式运行的 driver 与桌面 agent 应用脱钩启动。P1/P4/O3 与重测仍在本次发布之后。

## 7. 退出条件

E1–E10 全部满足，迁移到 pi 的第一个低风险任务完成完整的"预检 → 提交 → 合入 main → 推送"。
