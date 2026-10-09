# DEVX-016：单 agent 默认治理、ratchet 单调化与发布流程减负

最后更新：2026-09-25（v2：分阶段计划与验收指标经 owner 批准执行；S0 完成，S1-early 实现）

稳定任务 ID：`DEVX-016_SINGLE_AGENT_GOVERNANCE_SIMPLIFICATION`

状态：`IN_PROGRESS`（S0 DONE；S1-early 发布中；S1–S4 依赖 DEVX-015 合入 main）

总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`（对应退出条件 E7）

## 1. 问题

治理机制本身已成为主要开发成本。它的保护作用没有问题，问题在于大量与安全无关的耦合，
让每一次普通发布都要付出额外的提交、事务和 Full 重跑。

### 1.1 量化基线（2026-09-25 盘点，粗略口径）

| 指标 | 数值 | 来源 |
|---|---|---|
| 最近 30 天提交数 | 612，其中 25 个只做 "refresh generated authorities" | `git log --since=2026-08-25` |
| 本地 publication 事务目录 | 673 个（09-23 P0-A 盘点：639 个中 554 个 FAILED） | `outputs/architecture/arch_005_integration_publication_fence/transactions` |
| 决策包第 13 条 | 231 个任务对应 231 个只用于登记的事务和 231 个提交，执行跨越约 3 小时 | GOV-007 §6 |
| 单次 Full 耗时 | 26–54 分钟 | 本文 v1 |
| 治理相关代码量（按文件名匹配的估算） | src 约 2.6 万行，tests 约 7.4 万行 | 文件名匹配 `arch_00x/devx/gov/fence/lease/guard/atlas/task_source` |
| 测试中硬编码的 64 位十六进制串 | 639 处（分布在 26 个以上测试文件） | `grep` |

### 1.2 耦合点清单（已确认）

1. **一个事务只能改一个 task_id。** `integration_publication_fence.py:376` 抛出 `PUBLICATION_TASK_MISMATCH`。
   登记或批量转移 N 个任务，就要开 N 个 permit 事务，再逐个以 FAILED 终态收口。
   "FAILED"被用来表示正常操作，污染了失败统计和 E3 盘点。
2. **测试写死了 live authority 的 SHA 和计数，任何无关变更都会触发失败。**
   - 修改 `docs/system_flow.md` 时，除了要同步
     `config/architecture/devx_006d_report_catalog_flow_authority.yaml` 中的封印，
     还要同步 `tests/test_devx_006d_report_catalog_flow_authority.py` 里写死的 SHA；
   - 新增任意测试文件，就要修改 `tests/test_arch_004g_deprecation.py` 中的 WAVE21 清单 id 和测试数；
   - GOV-006 需求文档的哈希冻结在 `inputs/architecture/arch_004_compatibility_baseline.yaml` 中，
     在文档末尾追加一段进度说明就导致 118 个 node 失败（样本 2）。
3. **生成链顺序和候选身份互相依赖。** Atlas 生成器拒绝在有未提交改动时运行，生成物提交后候选又会变化，
   Full readiness 随之在 Atlas 绑定上失败。现行做法只能靠人工记住顺序：先提交全部改动，
   在该 HEAD 上 acquire，跑完 5 个生成器且要求零 diff，然后跑 Full。
4. **Full 依赖隐式运行环境。** Full 需要手动设置 `AITS_NAMED_DQ_PUBLICATION_TRANSACTION`、
   `AITS_NAMED_DQ_SOURCE_LEASE_ID` 两个环境变量，事务必须声明两个 synthetic 目录，
   各 tier 必须带 `--boundary-id`，还必须使用 venv 里的 Python 3.11（系统 Python 3.14 会使 lease
   身份检查失败）。以上任何一项缺失，都只能等到运行中途才发现。
5. **fence 有 12 个阶段，全靠手动逐个推进。** `config/architecture/arch_005_integration_publication_fence.yaml`
   中的 `phase_order` 需要协调者按顺序逐阶段调用。这些阶段本身有审计价值，但由人（agent）逐个驱动，
   是出错和返工的主要来源。
6. **DUAL_LANE 和多阶段 fence 是为多 agent 并行设计的。** 默认模式改为单 agent 后，这部分开销大多不再需要。

## 2. Owner 决定

- `owner_decision:DEVX-016:2026-09-23:single_agent_default_v1`：迁移前的默认治理模式是
  "单 agent + 受保护 main + CI"。
- `owner_decision:DEVX-016:2026-09-25:s0_early_start_v1`：Owner 在审阅草案 v2 后表示"在不影响当前开发任务的
  情况下我肯定希望能尽快处理"。据此，开放问题 1 的决定是：S0（只读）立即开始，不等待 DEVX-015 DONE；
  S0 另外增加一项工作，与 DEVX-015 在途 lane 做路径重叠分析，筛出可以提前进行、且不与其冲突的 S1 子集。
  开放问题 2–4 只影响 S1 及以后，在 S1 开始前再由 owner 决定。

## 3. 目标

1. 默认模式改为 SINGLE_LANE；DUAL_LANE 改为显式 opt-in，并要求写明路径互不相交的理由。
2. 精确计数和 live authority 的 SHA 断言，改为单调检查，或者改为"生成物与已提交内容一致"的检查
   （只禁止变差或不同步，不锁死当前值）。
3. 生成的 authority 只在最终候选上重建一次，由单一检查验证一致性。
4. 一个 publication 事务可以声明一组任务，批量登记或批量转移状态只需一个事务，并以 COMPLETED 收口。
5. 用一条可幂等续跑的发布命令驱动 fence 的全部阶段。各阶段的记录和校验照常保留，只是不再由人逐个推进。
6. 同一候选只跑一次 Full；运行环境的前置条件在启动前一次性检查。

## 4. 不变量（任何阶段都不得削弱）

- 安全边界：`production_effect=none`、`broker_action=none`，自动 rebase/merge/cherry-pick、force push
  和远端分歧修复仍然禁止；
- DQ/PIT 门禁、研究窗口（2021-02-22）、阈值治理规则；
- canonical 任务事件 append-only；每个任务的每次状态转移仍各自形成一个事件，
  并绑定 actor、change id、时间戳、base commit 和前一事件；
- lease 互斥、expected-main 新鲜度、候选 SHA 绑定、local main 与 remote main 的 SHA 相等校验；
- **不可变证据的哈希冻结保持不变**：研究产物、owner 决策包、预注册文件、历史 Full artifact 等哈希锁定是
  证据完整性要求，不属于本任务的简化范围。本任务只处理那些随无关变更而改变的 live authority 断言；
- 历史事务和历史 PASS 不改写、不重标。

## 5. 分阶段计划（单 lane 串行）

### S0：盘点与分类（只读）

- 逐条分类测试中的 SHA、计数和清单断言：
  (a) 不可变证据冻结，保留；
  (b) live authority 与已提交内容的一致性，改为生成式一致性检查；
  (c) 计数或清单 ratchet，改为单调检查。
  产出 `outputs/architecture/devx_016/pin_inventory_v1.json`，每条记录包含文件、行号、分类和理由。
- 按"有独立审计价值"和"只是逐步驱动的步骤"两类，给 fence 的 12 个阶段分类。
- 固定基线指标：每次发布的事务数、生成物刷新提交数、Full 次数和耗时；
  另外选取 5 个历史失败样本（DEVX-010、TRADING-2555、OPS-081、样本 1、样本 2），作为后续重放用例。

- 与 DEVX-015 在途 lane（`D:/Work/AITradingSystem_devx015_integration`）做路径重叠分析：只读比较它相对
  merge-base 的已提交改动和未提交文件名清单（不打开、不修改它的内容），标出与 S1 目标文件相交的部分。
  不相交的 S1 条目列为"可提前"。

验收：清单覆盖全部 639 处硬编码哈希，以及所有写死计数的测试；(b)/(c) 类的每一条都说明替代检查方式；
重叠分析列出每个 S1 候选文件是否与 DEVX-015 相交；不修改任何被跟踪的代码。

### S1：ratchet 单调化与生成式一致性

- 把 (b) 类断言改为"生成器重新生成的结果与已提交内容逐字节相同"，由一个统一入口检查，不在各个测试里写死 SHA。
  首批：system_flow 封印、WAVE21 清单、legacy row 视图。
- 把 (c) 类断言改为单调检查，例如测试数只许增、不许减，安全类测试集合不许缩小。
- 同步更新 memory 和 runbook 里"改 X 必须同时改 Y"的发布坑说明。

验收：
- 在一个临时分支上依次做 3 个无关变更：修改 `docs/system_flow.md`、新增一个测试文件、登记一个无关任务。
  每一次在重新生成 authority 后都没有测试失败；
- 故意让某个生成物与已提交内容不一致，统一入口会失败；
- 安全边界、DQ/PIT 门禁相关的测试 node 数不少于 S0 基线。

### S2：多任务事务

- fence 事务增加 `task_ids` 集合声明，旧的单 `task_id` 视为只含一个元素的集合，保持向后兼容；
  改动集合之外的任务仍然抛出 `PUBLICATION_TASK_MISMATCH`。
- 任务源写入器在一个事务内，为集合中每个任务各自追加独立的 canonical 事件，事件链语义不变。
- 只用于登记或状态转移的事务以 COMPLETED 收口，从此不再出现"permit 后 FAILED"的模式。

验收：
- 登记 N=5 个任务只需 1 个事务、0 个 FAILED；
- 越界写入集合外任务的请求被拒绝；
- 旧的单任务事务重放仍然通过；
- 重放决策包第 13 条中的任意 10 条，结果与原结果逐事件等价（同一 task、同一状态、同一 change id）。

### S3：单一发布命令

- 新增 `scripts/architecture_arch005_publish.py run|resume|status`，内部按 `phase_order` 驱动现有 fence，
  不另建第二套锁、队列或 scheduler：
  1. 检查工作区只含已声明的改动，并提交；
  2. 在该 HEAD 上 acquire；
  3. 按声明顺序运行 5 个生成器，要求零 diff；若有 diff 则停下并报告，不自动再提交；
  4. 依次运行各正式 tier 和 Full；
  5. local main fast-forward、普通 push、SHA 相等校验、release。
- 启动前一次性检查所有前置条件：Python 版本和解释器路径、`--basetemp` 路径长度、所需环境变量
  （由命令自己从事务中注入，不再手动设置）、synthetic 目录声明（从 policy 读取）、main 与 origin/main 的关系。
- 每个阶段都持久化实际结果；进程中断（包括蓝屏）后，`resume` 先只读重放已记录的状态，
  再从下一个阶段继续，不重复已完成的外部动作。

验收：
- 一次普通任务发布只需 1 条命令，产出的阶段记录与手动流程等价；
- 在 Full 前后分别模拟进程被杀，`resume` 都能正确续跑，不会重复 push；
- 任一前置条件缺失时，在 acquire 之前就失败，并给出可读的原因。

### S4：默认 SINGLE_LANE 与文档同步

- AGENTS.md、`run-governed-development` skill、`docs/operations/operations_runbook.md`、`docs/system_flow.md`
  统一改为：默认单 lane，标准发布走 S3 的命令；DUAL_LANE 和 base-drift 规则保留，但作为显式 opt-in。
- 本任务在 S3 之后的所有发布都走新命令，以此作为 dogfood 证据。

验收：以上文档与实际行为一致；DEVX-017（harness 中立化）可以直接基于新入口开展。

### 阶段依赖

S0（已完成）→ S1-early（3 个 Atlas 测试，可提前）→〔等待 DEVX-015 合入 main〕→ S1 → S2 → S3 → S4。
S1、S2 会修改 ratchet 和 fence 合同；S0 发现 18 个目标文件里有 14 个与 DEVX-015 lane 重叠，因此除 S1-early 外，
其余阶段必须在 DEVX-015 合入之后进行。S1-early 的 Full 也不得与 DEVX-015 的 Full 同时运行。

## 6. 整体验收标准

| # | 指标 | 基线 | 目标 |
|---|---|---|---|
| A1 | 5 个历史失败样本在新规则下重放 | 全部误报 | 0 误报 |
| A2 | 无关变更（登记任务、新增测试文件、修改 system_flow）导致的测试失败 | 1～118 个 node | 0 |
| A3 | 登记或转移 N 个任务所需的事务数 / FAILED 数 | N / N | 1 / 0 |
| A4 | 每次发布的 generated refresh 提交数 | 常为 2 个以上 | ≤1 |
| A5 | 每个最终候选的 Full 次数（不含真实失败后的修复重跑） | 常为 2 次以上 | 1 |
| A6 | 安全边界与 DQ/PIT 相关测试 node 数 | S0 基线 | 不减少 |
| A7 | 标准发布所需的人工步骤 | 约 12 个阶段的手动推进，外加环境设置 | 1 条命令 |
| A8 | AGENTS.md、skill、runbook、system_flow 同步 | — | 一致 |

## 7. 开放问题（需 owner 决定）

1. **S0 能否在 DEVX-015 DONE 之前开始？** S0 是只读的，不触及合同，提前做可以缩短 GOV-007 的关键路径。
   建议：允许。
2. **GOV-006 文档的哈希冻结是否保留？** 如果它是 owner 决策的证据，就属于第 4 节的不可变证据，保留冻结，
   后续进度改写到新文档里；如果只是兼容基线，则按 (b) 类改为生成式检查。建议：保留冻结，属于证据。
3. **Full 通过后，只改文档或生成物的修复，能否只重跑受影响的 tier 加一致性检查？**
   这会改变"重型验证绑定最终候选"的证据准入规则，本草案不把它列为目标，只记录为候选优化。建议：暂不做。
4. **DUAL_LANE 代码是保留为 opt-in，还是在 GOV-007 阶段 5 删除？** 建议：本任务只改为 opt-in；
   是否删除在迁移 pi 之后根据实际使用情况再定。

## 8. 依赖

S1（S1-early 除外）至 S4 依赖 DEVX-015 合入 main，以免在 DEVX-015 最终候选阶段改动 ratchet 与 fence 合同、
触发串行合同波次。S0 按 `owner_decision:DEVX-016:2026-09-25:s0_early_start_v1` 已提前完成；
S1-early 与 DEVX-015 lane 不存在路径重叠（见第 9 节 S0 结论 4）。

## 9. 进度

- 2026-09-23：登记（GOV-007 P0-B）。DEVX-015 执行期间，每次 Full 失败若属于计数/SHA 类 ratchet 不同步，
  在本节追加一条样本，作为简化的证据。
- 样本 1（2026-09-23，GOV-007 P0-B）：串行登记 4 个任务用了 5 个 fence 事务，其中 4 个只用于登记、
  以 FAILED 终态收口，因为一个事务只允许修改自身 task_id。
- 样本 2（2026-09-23，GOV-007 P0-B）：在 GOV-006 需求文档末尾追加一段进度说明，导致
  `tests/test_arch_004_refactor_policy.py` 118 个 node 失败，因为该文档的 SHA-256 冻结在
  `arch_004_compatibility_baseline.yaml` 中。处理方式是撤回该文档改动。
- 样本 3（2026-09-24/25，决策包第 13 条）：231 个任务状态转移用了 231 个 permit 事务（全部以 FAILED 收口）
  和 231 个提交。执行期间两次蓝屏中断，续跑需要人工判断断点；这是 S2 和 S3 `resume` 要解决的场景。
- 样本 4（2026-09-25，决策包第 13 条发布）：合法的状态转移触发视图重渲染，使只抽查第一个歧义片段的
  legacy row 测试失败，首个发布事务以 FAILED 收口，全部 tier 和 Full 重跑。断言本身已改为更严格的逐片段检查，
  但这说明视图字节级断言对无关状态变化很敏感，S0 需要把它纳入分类。
- 样本 5（2026-09-25，决策包第 13 条）：Atlas 状态映射缺少 `DEFERRED`，生成器 fail closed。
  这是正确的失败，不属于误报，记录下来作为 S1 的反例：不应被"简化"掉的检查。
- 2026-09-25：**S0 完成**（READ_ONLY，`agent_harness=claude_code`，base `7c0274267`）。产物是
  `outputs/architecture/devx_016/s0_inventory_v1.json`（sha256 `f7b0c61b6d5f0a54ebd9f4646287f6bf6489ca444b6c6184558016d1b1ca230e`），
  生成脚本保存在会话 scratchpad，发布时一并归档。结论：
  1. **硬编码哈希不是主要问题。** tests 共有 655 处硬编码哈希，分布在 91 个文件里：644 处属于 (a) 不可变证据，
     保留；只有 11 处属于 (b)，分布在 `atlas/test_historical_projection_review.py`、`test_devx_006d_…`、
     `test_arch_004g2_etf_cli_contract.py` 3 个文件。第 1.1 节"639 处"只是个粗略数字，不能说明问题出在哪里。
  2. **反复改动集中在 10 个测试文件。** 30 天内每个文件被改 18–89 次，第 11 名起不超过 5 次。依次是：
     `test_devx_006d`（89）、`test_arch_004g`（58）、`test_arch_004_refactor_policy`（49）、
     `atlas/test_live_snapshot`（48）、`test_arch_005_s5_task_source_cutover`（37）、`atlas/test_page_effectiveness`（25）、
     `test_devx_006c`（24）、`atlas/test_cited_query_renderer`（23）、`test_trading2452_architecture_contract`（19）、
     `atlas/test_historical_projection_review`（18）。
  3. **归纳出 5 种反复改动模式，S1 按模式处理，不按哈希逐条处理：**
     - P1：写死 live 文档的 SHA 和条目数；
     - P2：精确计数，每新增一个模块、测试或 Atlas 覆盖任务就要加 1；
     - P3：逐字复制 live 状态文案；
     - P4：每个治理 wave 都手写一个新 baseline section，外加 `LATEST_*` 常量和专属断言；
     - P5：硬编码有序任务列表和生成物的字节长度。
     其中 P4 占改动量的大头，涉及 refactor_policy、006c、006d、2452 四个文件。S1 需要把"逐 wave 手写"
     改为"通用 section 链校验"，这是 S1 中改动最大的一块。
  4. **与 DEVX-015 lane 重叠严重。** S1–S3 的 18 个目标文件里有 14 个已被 DEVX-015 lane 修改，其中 fence 实现
     +1326/−107 行、`run_validation_tier.py` +1612/−49 行。不相交的只有 3 个 Atlas 测试
     （`test_page_effectiveness`、`test_cited_query_renderer`、`test_historical_projection_review`，
     都属于 P2/P5）和 fence policy yaml；lane 在这些路径上也没有未提交改动。
     因此：S1 的主体、S2、S3 必须在 DEVX-015 合入 main 之后进行，否则会给 DEVX-015 的集成制造领域重叠；
     只有"S1-early：Atlas 覆盖计数和任务列表改为单调或生成式检查"可以提前。
  5. fence 阶段分类：8 个阶段有独立审计记录价值，4 个只是驱动步骤。S3 自动驱动全部 12 个阶段，不删除任何阶段。
- 2026-09-25：**S1-early 实现**（SINGLE_LANE，branch `claude/devx-016-s0-s1-early`，base `7c0274267`）。
  Owner 批准执行方案（"可以，按这个方案执行"），把需求文档 v2、S0 结论、canonical 任务更新和 S1-early 合在一次发布里。
  - `tests/atlas/test_page_effectiveness.py`：两处 `len(...) == 94`，以及两份逐项硬编码的任务列表，改为
    `ATLAS_TASK_SOURCE_FLOOR` 加 `_assert_monotonic_task_sequence`，检查三点：无重复、按受治理的任务标识顺序排列、
    包含 floor 中的全部任务。manifest 的覆盖列表改为与 policy 的 `task_sources` 逐项相等（生成式一致）。
    已有任务的 coverage 分类断言保持不变。
  - `tests/atlas/test_cited_query_renderer.py`：`data-task-coverage-count="94"` 改为与 policy 的 `task_sources` 数量相等。
  - `tests/atlas/test_historical_projection_review.py`：硬编码的有序任务列表改为与 policy 的 `task_sources` 逐项相等；
    legacy 分支里的历史页面哈希属于 (a) 类，保留。
  - 定向验证：3 个文件共 45 项 PASS，其中首轮 2 项 FAIL。原因是 helper 把截断后的短 id 传给了
    `page_task_identity_sort_key`，而该函数只接受完整 task id；修正 helper 后，`test_page_effectiveness.py`
    14 项 PASS。反向检查：删除已覆盖任务、打乱顺序、重复任务都会失败，追加新任务可以通过。
  - 效果：登记一个新的 Atlas 覆盖任务时，不再需要修改这 3 个测试文件。它们在过去 30 天共被修改 66 次。
- 2026-09-25：草案 v2，补充量化基线、耦合点清单、S0–S4 分阶段计划、整体验收指标和开放问题，待 owner 审阅。
  本版只修改了本文档，canonical 任务记录尚未更新；审阅通过后，在下一次 publication 中用
  `architecture_arch005_task_source.py update` 同步 blocker/next step 和验收标准。

## 10. 实施分解（2026-10-07；DEVX-015 基线已发布，S1 主体解锁；owner 2026-10-06 选择「按原计划做 P4」）

本节细化第 5 节的 S1–S4，不改变已被 owner 接受的范围与不变量（第 4 节）。依据是 2026-10-04 至 10-06 三次真实发布里实际发生的手工步骤与摩擦点（见下），以及每个候选的固定成本。

### 10.1 实测摩擦点（三个候选的手工流程，2026-10-04 至 10-06）
| 摩擦点 | 实测代价 | 对应步骤 |
|---|---|---|
| 改 `docs/system_flow.md`：手工重算 `devx_006d` 封印的 4 个字段、改测试里钉死的 SHA（和条目数），再跑生成器 | 每次约 10 分钟，易错（P1 候选与 DEVX-023 各一次） | F1 |
| 新增模块/测试文件：手改 `tests/test_arch_004g_deprecation.py` 的清单 id 与计数，生成器链多一轮重封提交 | 每次多约 10 分钟与一个提交（P1 候选） | F2 |
| 改动历史哈希固定源（如 `checkout_telemetry.py`）：须同时往 `compatibility_authority.py` 的 V3 清单和测试的固定集合各加一行，否则 118 个 `test_arch_004_refactor_policy` 哈希授权测试失败 | 发现靠整批回归（约 22 分钟）；改动本身 2 行 | P4（目标：不再需要） |
| 任务行更新：每个任务一个围栏事务（acquire → TASK_SOURCE_PRE_WRITE → update → release failed），且必须先暂存其他改动 | 每个约 2:46；一次候选前后常有 3–5 个 | S2 |
| 准备事务 + 正式事务 + 逐步检查点 + 生成器 + 预检 + 就绪度 + 冻结驱动 + WMI 启动 + 发布后 8 个步骤，全部靠手写脚本与记忆 | 约 15 分钟（准备）+ 约 10 分钟（发布收尾）手工，出错就丢一轮 | S3 |
| `acquire` 的 owned/shared 路径靠「从上一个事务拷贝」 | 隐式依赖历史事务文件，换人/换机会断 | S3（范围清单） |

### 10.2 每个候选的固定成本与排序依据
准备链约 15 分钟 + 预验证 40 分钟 + Full 约 2 小时 25 分 + 发布 1–1.5 小时 ≈ 4.5–5 小时。所以：(1) 把风险同类的步骤合并进同一个候选；(2) 先做会降低后续所有候选成本的步骤；(3) 存储形式/围栏合同这类「会让真实库出现新形式」的变更各自独立成候选，并在发布后立刻做真实库消费者冒烟（DEVX-023 第 7 节的清单）。

| 候选 | 内容 | 性质 | 备注 |
|---|---|---|---|
| C1 | F1 封印重算命令 + F2 钉死值改下限 + S2 多任务事务 | F1/F2 是测试与脚本；S2 是围栏合同（事务 schema） | 合并：都不改租约事件存储；S2 须兼容旧事务重放 |
| C2 | S3 单一发布命令（`architecture_arch005_publish.py run|resume|status`）+ 发布范围清单 | 新 CLI，不改围栏阶段 | 之后所有候选 dogfood；须 owner 评审「发布范围清单」 |
| C3 | P4 通用 section 链校验（先加后删，等价性由变异测试证明） | 测试重构；会改动一个历史哈希固定的测试文件 | 最大、最需要谨慎；可拆成 P4-0（盘点）与 P4-1…P4-4 |
| C4 | S4 默认 SINGLE_LANE 与文档同步 | AGENTS.md/skill/runbook/system_flow | 之后 DEVX-017 才能开始 |
DEVX-017 依赖 DEVX-016 全部完成；OPS-082 须 owner 先确认「不得使用 Windows 任务计划程序」规则变更，且依赖 OPS-077/078/081 的运营验收；阶段 5 在其后。

### 10.3 F1：封印重算命令（S1 的 P1 模式）
- 现状：`architecture_report_catalog_flow_authority.py build` 只**验证**封印（`RCF_SOURCE_SEAL_DRIFT`）；重算是手工的。
- 方案：新增 `reseal [--target report_registry|artifact_catalog|system_flow ...] [--write]`，实现放进 `report_catalog_flow_authority.py`（复用 `_split_report_registry`、`_split_markdown`、`_digest`、`_git_blob_id`），默认只打印「旧 → 新」，`--write` 只重写策略 yaml 里该目标的五个字段，其余字节不动。
  测试里钉死的字面量（SHA、条目数）改为：(1) 一个**算法**不变量测试——对合成输入的已知答案，钉住封印算法本身；(2) 对 live 文件：策略里的封印必须等于重算值（已有 `build` 的校验），条目数用下限而不是字面量。
- 验收：改 `docs/system_flow.md` 后运行 `reseal --write` 加生成器链，测试零失败；故意让封印与文件不一致，`build` 与测试都失败；算法不变量测试在改动封印实现时失败。

### 10.4 F2：清单 id 与计数 → 单调下限 + 生成式一致
- 现状：`tests/test_arch_004g_deprecation.py` 钉死 `WAVE21_CURRENT_INVENTORY_ID` 与 `python_module_count`、`python_test_file_count`、`direct_writer_current_count`。
- 方案：计数改为 `>= 上一已评审值`（单调：只许增、不许减；安全类集合不许缩小），inventory id 不再写字面量：断言「同一次扫描重复两次 id 相同」「id 格式」「`assert_frozen_deprecation_inventory` 的冻结项不变」；保留 `removal_ready_count == 0`、`direct_writer_violation_count == 0` 等安全断言。
- 验收：新增一个测试文件和一个模块后，不改任何钉死值即可通过；删除一个已评审的模块使下限测试失败；安全断言不减。

### 10.5 S2：多任务事务（原 S2，细化）
- 改动面（读码）：`IntegrationPublicationFence.acquire(task_id=...)` 把单个 `task_id` 写进 `transaction.json`；`validate(task_id=...)` 抛 `PUBLICATION_TASK_MISMATCH`；`architecture_arch005_task_source.py` 的 `_require_publication_transaction` 把要修改的任务 id 交给 `validate`；租约本身的 `task_id` 是租约的主任务。
- 方案：事务新增可选 `task_ids`（排序、去重、含主任务；缺省 = 只含 `task_id` 的单元素集合，旧事务重放与哈希不变）；`validate(task_id=X)` 改为「X 属于集合」，集合外仍抛 `PUBLICATION_TASK_MISMATCH`；`acquire` 增加重复的 `--task-id-extra`；任务源写入器无需改动（它已经按任务各写一个 canonical 事件，事件链语义不变）。
  **COMPLETED 收口**：新增事务种类 `TASK_SOURCE_ONLY`（阶段子集 `ACQUIRED → TASK_SOURCE_PRE_WRITE → RELEASED`，不允许进入候选/验证阶段），只有该种类的事务才能在 `TASK_SOURCE_PRE_WRITE` 之后 `release --outcome completed`；其余种类保持原有规则（completed 只在 `CLEANUP_PRE`）。
- 风险：围栏合同变更（事务 schema 与重放兼容、policy 哈希）→ 串行合同波次；`release` 的终态规则放宽必须有负向测试（候选事务不能借此绕过验证）。
- 验收：登记 N=5 个任务只用 1 个事务、0 个 FAILED；越界写入集合外任务被拒；旧单任务事务重放仍通过；重放决策包第 13 条的任意 10 条，结果与原结果逐事件等价；`TASK_SOURCE_ONLY` 事务无法推进到 `CANDIDATE_COMMIT_PRE` 及之后。

### 10.6 S3：单一发布命令（原 S3，细化）
- 命令：`scripts/architecture_arch005_publish.py run|resume|status`，只编排现有围栏阶段与现有 CLI，不新建第二套锁/队列/调度器（沿用 AGENTS.md 的发布围栏纪律）。
- 手工流程到步骤的映射（三次真实发布验证过）：
  1. 前置检查（一次性，acquire 之前失败）：解释器是 `.venv` 的 3.11、工作树只含已声明改动、main 与 origin/main 的关系、`--basetemp` 路径长度、无其他 pytest/python.exe、磁盘余量、known-unrelated 排除集；
  2. 准备事务：acquire（范围来自**发布范围清单**）→ `TASK_SOURCE_PRE_WRITE` → `GENERATED_REBUILD_PRE` → 五个生成器 → `GENERATED_REBUILD_POST` → 提交生成物（模板提交信息）→ 重跑生成器直到零差异 → release failed；
  3. 正式事务：acquire（`failure_fix_rerun` 时带 `--full-parent`）→ 各检查点 → 生成器零差异 → `CANDIDATE_COMMIT_PRE` → `FORMAL_VALIDATION_PRE` → 治理预检 + 就绪度 → 冻结候选与事务哈希；
  4. 验证驱动：各 tier 与 Full（WMI 脱离，自带心跳，写进度文件）；
  5. 发布前清单（6 项）→ `LOCAL_MAIN_FF_PRE` → `local-publish`（WMI 脱离）→ fetch → `REMOTE_PUSH_PRE` → CLOSEOUT 预检 → 普通推送（**仅当存在 owner 授权记录**，否则停在「等待授权」）→ 校验 SHA → `CLEANUP_PRE` → release completed → 发布后消费者冒烟。
- 每一步把实际结果写入 `publish_state.json`（含上一步结果的哈希）；`resume` 先只读重放已记录的状态，再从下一步继续，已完成的外部动作（push、合并）不重做。
- **发布范围清单**（新配置，须 owner 评审）：声明一次「普通发布允许触碰的 owned/shared 路径与生成器」，取代「从上一个事务拷贝」的做法；清单有版本、owner、变更需评审。
- 验收：一次普通任务发布只需 1 条命令，产出的阶段记录与手动流程等价（逐阶段对照三次真实发布的事务）；在 Full 前后分别模拟进程被杀，`resume` 都能续跑且不重复 push；任一前置条件缺失时在 acquire 之前失败并给出可读原因；没有授权记录时永不推送。
- 风险：命令本身成为新的单点，所以只做编排、每一步仍由原围栏命令验证；测试用夹具仓库（沿用现有围栏夹具），并用「故意缺前置条件」「模拟中断」的负向用例。

### 10.7 P4：通用 section 链校验（S1 的主体，owner 已确认保留）
- 规模（2026-10-06 实测）：`inputs/architecture/arch_004_compatibility_baseline.yaml` 49,126 行、299 个顶层段；`tests/test_arch_004_refactor_policy.py` 40,507 行，其中 100 个逐 wave 的 `*_hash_authority` 测试（11,622 行）、744 个私有 helper、1,011 个模块常量；`compatibility_authority.py` 4,028 行；另有 006c/006d/2452 三个文件。
- 目标性质（通用、由数据自描述）：(a) **不可变**：每个段记录它之前的文件前缀哈希（已有 `prior_sections_immutability`），逐段验证，不再需要测试里的常量；(b) **当前哈希归属**：每条被固定的源路径，取「最后一个列出它的段」，其记录的哈希必须等于实时文件，更早的段若不一致必须被更晚的段显式取代；
  (c) **顺序与 schema**：段顺序、字段集合、状态转移与安全标志（`production_effect: none`、`broker_action: none`）按段类型通用校验；(d) 各 wave 特有的语义期望（任务 id、边界 id、状态）作为数据放进段里，由通用校验读取，而不是写成 100 个测试。
- **等价性（防止弱化保护）**：先「加」后「删」。P4-1 新增通用校验与参数化测试，与现有逐 wave 测试并存；P4-2 建变异测试夹具（篡改历史段字节、改动固定源、改记录哈希、删除取代声明、调换段顺序、去掉安全标志……至少 15 类），对每个现有逐 wave 测试记录它能抓到的变异集合，要求通用校验抓到其**超集**；
  P4-3 把逐 wave 特有语义期望迁移成段内数据；P4-4 删除逐 wave 测试与常量。每一步一个候选，P4-4 之前不删除任何现有断言。
- 风险：`tests/test_arch_004_refactor_policy.py` 自身是历史哈希固定的测试文件，改动/删除它需要由最新授权段接管（同 S6 与 `checkout_telemetry.py` 的模式）；P4-4 的删除量很大（约 2.9 万行），所以须在 P4-2 的等价性证据评审通过后再做。
- 验收：A1–A2（第 6 节）：5 个历史失败样本重放 0 误报；无关变更（登记任务、新增测试文件、修改 system_flow、改动历史固定源）导致的测试失败为 0；变异测试对每一类篡改都失败。

### 10.8 S4：默认 SINGLE_LANE 与文档同步（原 S4）
- 不变，须在 S3 之后；AGENTS.md、`run-governed-development` skill、`docs/operations/operations_runbook.md`、`docs/system_flow.md` 与实际行为一致；DEVX-017 才能开始。

### 10.9 步骤表（依赖与验收）
| 步骤 | 依赖 | 验收摘要 |
|---|---|---|
| F1、F2 | DEVX-023 已发布 | 见 10.3、10.4 |
| S2 | F1、F2（同候选 C1） | 见 10.5；旧事务重放不变 |
| S3 | S2、发布范围清单经 owner 评审 | 见 10.6；dogfood：S3 之后的候选全部走新命令 |
| P4-0 盘点 | 无（只读，可与 C1 并行） | 逐个现有逐 wave 测试的断言分类（不可变/源哈希/取代/顺序/语义/安全）与对应的变异集合 |
| P4-1…P4-4 | P4-0 | 见 10.7 |
| 封印 S（DEVX-023 第 10 节；取代原先排在这里的 W-DQ） | S3 已发布；owner 先决定是否改「不落盘已校验标记」的边界并评审信任模型 | owner 2026-10-07 改为排在 C3 之前；验收见 DEVX-023 第 10.4 节；W-DQ 降为可选（第 11 节） |
| S4 | S3、P4 | 见 10.8 |

### 10.10 开放问题（需 owner 决定）
1. **发布范围清单**的内容与评审人：普通发布允许触碰哪些路径？（S3 的前提。）
2. `TASK_SOURCE_ONLY` 事务种类的 COMPLETED 收口规则是否可接受（S2）？
3. P4-4 的大删除是否要求 owner 逐批评审，还是以变异等价性证据为准？
4. 候选合并粒度：C1 合并 F1+F2+S2 是否接受？
5. （2026-10-07 owner 已决定，晚间更正）路线顺序：C2（S3）→ **封印 S（DEVX-023 第 10 节，先出设计、owner 评审信任模型与边界）→ C3（P4）→ C4（S4）**。原先排在这里的 W-DQ 依据的是「stage 1 耗时随证明体积增长」的错误归因（DEVX-022 第 17.7 节的逐调用计时否定了它），已降为可选。

- 2026-10-07：按三次真实发布（DEVX-015 基线、DEVX-020/021/022、DEVX-021 P1 + S3b 回归修复、DEVX-023）的实测摩擦点补充了第 10 节的实施分解；owner 2026-10-06 选择「按原计划做 P4」，不降级；第 10.10 节列出需 owner 决定的开放问题。下一步：C1（F1 + F2 + S2）。

- 2026-10-07：**C1（F1 + F2 + S2）实现完成，候选验证中**（base `d3d34872b`，分支 `claude/gov007-post-baseline-followups`）。每步先写失败测试，再实现。
  - **F1（封印重算命令）**：`report_catalog_flow_authority.reseal_policy_seals(repository_root, targets, write)` 与 CLI
    `architecture_report_catalog_flow_authority.py reseal [--target ...] [--write]`。算法与 `build` 的校验同源（`_split_report_registry`、
    `_split_markdown`、`_digest`、`_git_blob_id`）；默认只输出「旧 → 新」，`--write` 只重写该目标的五个封印字段行
    （`byte_count`、`file_sha256`、`lf_sha256`、`git_blob`、`entry_count`），其余字节与换行风格（LF/CRLF）不动，写后自验，失败则恢复原文件；
    未知目标（`RCF_RESEAL_TARGET_UNKNOWN`）与无法被无损拆分的源文件一律拒绝，不会被封印。测试：封印算法的已知答案、对三个目标逐个做影子渲染字节同一性与完整覆盖（生成式）、
    reseal 只改五行且之后 `build` 通过（LF 与 CRLF 两种换行）、拒绝未知目标与无法无损拆分的源；条目数字面量改为 `ENTRY_COUNT_FLOORS` 单调下限。
    dogfood：本候选给 `docs/system_flow.md` 加一段后，用 `reseal --target system_flow --write` 重算封印，`build` 通过，策略文件只改了这五行。
  - **F2（钉死值改下限）**：`tests/test_arch_004g_deprecation.py` 的 `WAVE21_CURRENT_INVENTORY_ID` 字面量删除，改为「与冻结 yaml 自身的
    `inventory_id` 相等」并断言同一次扫描重复两次 id 相同；`python_module_count`、`python_test_file_count` 从精确钉死改为
    `WAVE21_REPOSITORY_COUNT_FLOORS` 单调下限（只许增长，下限只能随评审抬高，不得为掩盖删除而降低）；
    `direct_writer_current_count`、`removal_ready_count == 0`、`direct_writer_violation_count == 0` 等安全断言不动。
  - **S2（多任务事务）**：围栏事务新增可选字段 `task_ids`（`acquire --task-id-extra`，排序去重并含主任务）与 `kind`
    （`acquire --task-source-only`）；两者仅在使用时写入事务体，因此历史事务的字节与哈希不变，旧单任务事务的重放不受影响。
    `validate(task_id=X)` 改为「X 属于集合」，集合外仍抛 `PUBLICATION_TASK_MISMATCH`；租约、Full profile 与全部发布阶段仍只绑定主任务，
    任务源写入器无需改动。`TASK_SOURCE_ONLY` 的阶段链固定为 `ACQUIRED → TASK_SOURCE_PRE_WRITE → RELEASED`，不得声明 integration plan 或
    Full parent（`PUBLICATION_TASK_SOURCE_ONLY_SCOPE`）；候选/验证/发布阶段在**任何远端或 Full profile 预处理之前**就被拒绝（共享守卫
    `_reject_task_source_only_phase` 同时放在装饰器的两个预处理函数和 checkpoint 本体，避免先报「缺少 candidate」这类副作用性错误）；
    只有该种类的事务可在 `TASK_SOURCE_PRE_WRITE` 之后 `release --outcome completed`，其余事务的 completed 仍只在 `CLEANUP_PRE`。
    种类与任务集合都进入事务哈希：篡改种类使重放失败（测试覆盖）。9 项新测试：多任务成员与越界拒绝、单任务原形式不变、acquire
    幂等与冲突、COMPLETED 收口与幂等、无法进入候选阶段、不得声明 plan/parent 且普通事务仍需 `CLEANUP_PRE`、哈希绑定、未知种类、CLI 暴露。
  - 开放问题 10.10 的第 2、4 项（`TASK_SOURCE_ONLY` 的 COMPLETED 收口规则、C1 合并粒度）按推荐方案实现，仍待 owner 确认；若 owner 否决，
    S2 的行为改动在一个候选内可回退，F1/F2 不受影响。
  - 审计记录（AGENTS.md 已登记 `known_unrelated_exclusions`）：本轮有两次仓库检查没有带完整的精确排除集，一次是带 `docs` 目录路径的限定路径
    `git status`，一次是不带路径的 `git status --porcelain --untracked-files=all`。两次输出里都没有出现被排除路径，没有读取、复制或修改其内容，
    按规则登记为审计事件；其后的全库检查一律使用 `architecture_arch005_checkout_guard.py worktree-audit`（PASS，仅 10 个 C1 路径为脏，
    暂存/未暂存 diff 检查均 PASS）。LANE 阶段 governed preflight（SINGLE_LANE，coordinator）PASS，证据 `claude_c1_lane_preflight.json`。

- 2026-10-07：**owner 决定（AskUserQuestion，第 10.10 节）**：(1) C1 发布——通过后直接发布（只限候选 `7ead2d6ff`，普通推送，无 PR/force-push，任一项不绿即停下汇报）；
  该授权同时接受 S2 的 `TASK_SOURCE_ONLY` COMPLETED 收口规则与 C1 合并粒度（第 2、4 项）。(2) 发布范围清单（第 1 项）：核心清单由 owner 评审一次，owned 路径由候选 diff 自动派生。
  (3) P4-4 删除（第 3 项）：以变异等价性证据为准，分批提交，owner 抽查，不要求逐批批准。
- 2026-10-07：**C1 已发布**（候选 `7ead2d6ff`，main = origin/main，普通推送 `d3d34872b..7ead2d6ff`；正式事务 `gov-007-d24-formal-20261007-v1`；无恢复、无人工修复；最终的 `git push` 由 owner 在终端执行（见事件记录））。
  - **验收对照**：F1 reseal 命令在本候选自己给 `docs/system_flow.md` 加一段时 dogfood 通过（只改策略 5 行，`build` PASS）；F2 新增一个测试文件后只需**一轮**生成器提交就达到零差异
    （此前新增测试文件需要两轮）；S2 验收 A3 以真实事务验证：一个 `TASK_SOURCE_ONLY` 事务更新 DEVX-016 与 GOV-007 两行，COMPLETED 收口，0 个 FAILED，2 分 41 秒
    （此前每个任务约 2 分 46 秒、各一个 FAILED 事务）。
  - **候选链实际经过**：prep 共 3 个事务——v1 在 architecture-manifests 生成器就失败（reseal 用了 `Path.write_bytes`，触发 arch_004c 依赖门
    `NEW_DIRECT_ARTIFACT_WRITER_FORBIDDEN`，已改用 `write_bytes_atomic`）；v2 之后第二轮聚焦回归发现三处钉死的 system_flow 总条目数（006d 测试两处、refactor_policy 一处）已改为下限/一致性检查；
    v3 一轮提交后零差异。另有一次 S2 行事务因 S2 测试夹具提到 task register 使 consumer inventory 失效而 FAILED 收口，夹具改为中性路径后重试成功。
  - **耗时（对照 DEVX-022 第 15.1 节基线）**：S2 行事务 2:41；prep 每轮约 7 分；正式事务到 FORMAL_VALIDATION_PRE 3:35（基线 4:19）；预检 0:42 + 就绪度 1:00；
    stage 1 **751 s**（基线 440–502、d21b 505、d23 579；趋势 505→579→751）、contract 292（d23 264）、integration 74、reproducibility 48、architecture-fitness 1,370（基线 1,216–1,412）、
    Full pytest **8,820 s = 2:27:00**（d23 8,737 s）；发布阶段：LOCAL_MAIN_FF_PRE 3:44、`local-publish` **57 分 3 秒**（d23 54 分、d21b 86 分，目标 ≤ 60 分达成）、REMOTE_PUSH_PRE 3:32、CLOSEOUT 预检 0:48、CLEANUP_PRE 0:41、completed 释放 0:43。
  - **stage 1 的分析**（2026-10-07 复核后更正并补全；完整数据与时间线见 DEVX-022 第 17.6 节）：两次对照的主机计数器（`devx022-d23/d24-counters.csv`，32 个逻辑核）里
    python 平均约 1.25 个核（采样峰值 3.5 / 4.1 个核）、曲线形状几乎一致，python 核·秒 706 → 923（+31%，与时长 +30% 同步），所以是工作量变长而不是等待。
    （更正：本段此前写作「python 约 3 个核全程占用、1,737→2,313 核·秒」，那是把采样峰值当成了平均值；结论不变，数字以此为准。）d24 窗口里 python 平均核数与 d23 相同，
    没有可见的额外 python 负载，所以与我误起的杂散 pytest 重叠不是原因。真实租约库用旧代码（d3d34872b）和新代码重放耗时相同（约 24 s，负载下；d23 发布后空闲时 15.5 s），
    C1 不改变重放成本；事件数只增加 0.8%（5,813→5,860），租约头 +12，不足以解释 +30%。**定位到的原因**：stage 1 只有两个测试（同一文件，同一 worker 串行），它们的每个组件
    （7 次活体证明、3 个生产 CLI 子进程）都同比例变慢（d23 → d24 +22% 到 +38%），与活体证明文件的体积同步增长：`test_parent_pre_guard.json` 35.70 → 46.60 → 57.52 MB
    （每次发布 +约 10.9 MB，来自整库重放内嵌的已释放 head 托管表），阶段约 12 s / MB，近似 86 s + 11.3 s / MB。外推下一次发布约 860–890 s（告警线 900 s），再下一次越线；
    根治是 DEVX-023 第 8 节第 2 条（证明不再内嵌整库重放，须 owner 评审）。证明体积如何变成耗时的机制仍需在**正式窗口内**逐调用测量（stage 1 要求有 FORMAL_VALIDATION_PRE 的活事务，
    发布后无法再跑）：C2 的正式窗口里先用 `stage1_timing_plugin.py` 单独跑一次 stage 1，记录测试进程里 store.replay / live_parent_proof 的次数与总耗时，再决定改动。
  - **事件记录（如实披露）**：(1) 本轮有两次仓库检查没有带完整的已登记排除集（已在上一条记录）；(2) 我误起过一次范围过大的 pytest（含两个文件里全部 Full 规模重节点，21 + 18 个，`--dist loadfile` 下单 worker 串行），
    尝试用 `taskkill` 终止被权限分类器拒绝，我没有绕过，它在 12,867 秒（3 小时 34 分）后自己跑完：289 通过，2 失败（一个是当时生成物过期，已被生成器轮次修好；
    一个重节点是在我同时改仓库期间失败，在最终干净树上单独重跑通过，18 分 13 秒）；(3) 3.5 小时的会话中断使发布前检查第 6 项（Full 结束后 2 小时内开始发布）超窗 27 分钟，
    owner 在核实租约仍有效（还剩 3.5 小时、`local-publish` 最长约 86 分钟）后明确批准按例外继续，其余五项全部通过；(4) 最终的 `git push origin main` 被权限分类器拒绝（Out-of-Place Publication），我没有换别的方式绕过，由 owner 在终端执行；证据文件 `claude_d24_push.log` 如实标注它不是 git push 的输出，而是我在 owner 推送后的只读 SHA 校验（本地 main = origin/main = 远端 tip = 候选）；(5) `D:/Work/Remove-AitsDevx015TestRegistryRoots.ps1` 仍待 owner 执行（5 个 HKCU 测试根）。
  - **临时资源（生命周期记录，2026-10-07 17:42 收口）**：按精确绝对路径白名单删除（脚本 `scratchpad/cleanup_d24.py`，日志 `D:/Work/devx024-c1-cleanup.log`，释放约 28.24 GB，用时 8 分 44 秒，
    其中本次 Full 的 basetemp 28.03 GB / 703,505 个文件 389 s）：`%TEMP%\pytest-of-JACK\pytest-23203`，以及 `D:/Work/ptc1`、`ptc1b`、`ptc1c`、`ptc1h`、`ptc1s2`、`ptc_c2`（C1 聚焦回归的 basetemp）和 `D:/Work/devx024-old`（旧代码导出）。
    清理前确认无其他 python 进程、无进行中的验证，必需证据都已在 `outputs/architecture/integration_revalidation/devx015-v389/` 与 `outputs/validation_runtime/gov-007-d24-*`。
    保留：`D:/Work/devx022-d24-counters.csv`、`devx022-d24-pub-counters.csv`（逐进程计数器）、`devx015-*`（已登记证据根）、`devx020-k`（随 DEVX-020 关闭时清理）、`devx022-t1`；
    `pytest-of-JACK` 下的空目录 `pytest-23356/-23357/-23358` 与 `pytest-current` 由 pytest 自管，未动。可恢复性：删除的都是可再生的临时目录，不需要恢复。
- 2026-10-07：**S3 发布范围清单的草案依据（只读测量，4 个正式事务的声明范围对比）**。`gov-007-p1c-devx015-baseline-publication-…-v26`、
  `gov-007-d21b-formal-…`、`gov-007-d23-formal-…`、`gov-007-d24-formal-…` 四个事务声明的 owned/shared/生成器/必需 tier 对比：
  shared 路径 20 项、生成器 5 个、必需 tier 5 个在四个事务里**完全相同**；owned 路径的稳定核心是 59 项，四个事务的并集是 67 项，
  逐候选新增只有 8 项，全部是「本候选 diff 里实际改动、且不在核心里」的文件（需求文档 DEVX-016/020/021/022/023，以及本候选新增或改动的
  F1 模块与脚本、S2 测试）。结论：清单不必逐候选手写，可由「核心清单 + 候选 diff 派生的 extras」组成：
  1. 核心清单（policy，owner 评审一次）：shared 路径集合、生成器顺序、必需 tier、owned 路径的允许前缀/glob（`docs/requirements/`、`src/`、
     `scripts/`、`tests/`、`config/` 的受限子集）与**禁止路径**（围栏 policy yaml、AGENTS.md、known_unrelated 排除项、`.git`）；
     带 owner、版本、状态、理由、复审条件（AGENTS.md 启发式治理）。
  2. extras = `git diff-tree frozen_base..lane_head` 的路径 − 核心 − 生成物范围；每个 extra 必须落在允许 glob 内，否则在 acquire **之前**失败并给出
     可读原因（候选改动了清单不允许的路径），而不是在后续阶段才因归属失败。
  3. 命令输出把「核心版本 + extras 清单 + 它们的哈希」写进 `publish_state.json`，发布后与事务的声明范围逐项对照。
  这样 d24 手写的 8 项 EXTRA 与「从上一个事务拷贝」都被取代。待 owner 决定：核心清单的评审人与「禁止路径」的范围。
- 2026-10-07：**P4-0 盘点（只读 AST 分类，草稿；脚本在会话 scratchpad，发布时归档）**。对象：`tests/test_arch_004_refactor_policy.py`
  （40,508 行、221 个测试、744 个私有 helper、969 个模块级常量）里名称含 `hash_authority`/`successor`/`retained` 的 112 个逐 wave 测试
  （12,771 行；每个中位数 112 行、24 条断言、12 个钉死常量，最大 287 行/82 条断言/18 个常量），以及 006c（16 个，640 行）与 2452（13 个，235 行）。
  断言分类（语句数，同一语句可属于多类）：来源集合与 delta 840、语义身份（schema/status/boundary/task_ids/owner_decisions）511、
  来源哈希 245、安全标志 246、不可变前缀 155、取代声明 104、段顺序 87、语义内容（implementation/validation）103、篡改负向测试 51、
  其余 603（多为逐 wave 的专属计数与字面量）。覆盖到的测试数（112 个里）：来源集合 112、语义身份 112、安全 112、来源哈希 110、不可变前缀 104、
  取代 104、语义内容 92、顺序 76、篡改 51；既无不可变前缀也无篡改断言的 8 个测试在 P4-2 里需要单独评估它们实际抓到什么。
  含义：来源集合、来源哈希、不可变前缀、取代、顺序、安全标志这六类（合计约 1,700 条语句）是逐 wave 重复的**同一组通用性质**，可由段数据自描述的
  通用校验替代；语义身份与语义内容（约 600 条）是各 wave 的专属期望，应迁成段内数据；其余 603 条需要逐类归并，不能一次性删除。
  「其余」的再分类（只数 assert，约数）：被取代集合的集合代数约 94、最新段 `next(reversed(...))` 的顺序检查约 51、循环 `all(...)` 约 43、
  validation 状态词表（PENDING/PASS 等）约 50、逐 wave 专属块约 100（`generated_fragment_authority` 35、`owner_authorization` 及其变体约 30、
  `preview_artifacts` 7 等）。也就是说「其余」里大部分仍是上面六类通用性质的另一种写法，真正逐 wave 专属、必须迁成段内数据的约 100–150 条。
  下一步（P4-1 之前）：为 P4-2 的变异夹具列出至少 15 类篡改与每个现有测试的捕获集合。
- 2026-10-07：**S3 实现草案（C2 的代码布局）；已在会话 scratchpad 离线写完并通过 164 项测试，待移植进仓库**。
  - 布局：`src/ai_trading_system/platform/architecture/publication_orchestrator.py`（纯逻辑：步骤表、状态机、范围派生、前置检查）、
    `scripts/architecture_arch005_publish.py`（`run|resume|status` CLI，只编排现有围栏命令）、`config/architecture/devx_016_publication_scope.v1.yaml`
    （发布范围清单 policy）、`tests/test_devx016_publication_orchestrator.py`（夹具仓库 + 假执行器）。全部新文件；写文件只走
    `write_bytes_atomic`（dependency gate），测试夹具不提 task register（consumer inventory）。
  - 步骤契约：每步 = 编号、前置条件、命令 argv、期望产物、成功判定、幂等键。每步实际结果写入 `publish_state.json`（argv、退出码、stdout/stderr 路径、
    产物哈希、起止时间、上一步结果哈希）。`resume` 先只读重放状态并校验已记录产物的哈希，再从第一个未完成步骤继续；有外部副作用的步骤
    （本地 main 快进、推送）执行前先查实际状态（`git rev-parse main`、`git ls-remote`）判断是否已完成，已完成则只记录不重做。
  - 授权门：`run` 在推送前停在 `AWAITING_OWNER_AUTHORIZATION`；只有存在 `--authorization-record`（候选 sha、范围=该候选的普通推送、授权时间、来源）且 sha 与冻结候选
    相等才继续；没有记录永不推送，PR/force-push/历史改写不在命令里（AGENTS.md 单独授权）。
  - 失败策略：任一步失败只记录 FAILED 与下一步动作，不自动重试有副作用的步骤；`status` 输出可读原因。
  - 耗时：每步结果与 DEVX-022 第 15.1 节基线（任务行事务 2:45、prep 链约 7 分、正式事务到 FORMAL_VALIDATION_PRE 4:20、stage 1 440–502 s、contract 258–281 s、
    integration 56–78 s、repro 36–48 s、architecture-fitness 1,216–1,412 s、Full 8,495–8,738 s、local-publish 54–86 分）自动对照，超过 1.25 倍时在
    `status` 与终端摘要里标 `SLOW_STEP`，对应 owner「流程耗时超出预期要继续分析」的要求，并把实测写回基线表。
  - 验收（补充）：用假执行器在夹具仓库里跑通全流程；逐阶段与 d21b/d23 真实事务的阶段序列对照；在 acquire 之后、Full 前后、local-publish 之后分别模拟进程被杀，`resume` 续跑且不重复推送；
    缺少前置条件时在 acquire 之前失败；dogfood：C2 之后的候选全部走新命令。
- 2026-10-07：**P4-0 数据模型事实（只读，合并后的兼容性权威）**。合并视图共 334 个顶层条目：9 个头字段（`schema_version`、`baseline_id`、`status`、`as_of`、
  `base_commit`、`capture_mode`、`production_effect`、`frozen_sources`、`parity_requirements`）与 325 个段。段是**异构**的：138 种不同的顶层键集合、没有任何一个键出现在每个段里、
  159 个 schema 族、`safety` 块出现在 170 个段里共 465 种键。来源记录共 10,009 条、涉及 2,004 个不同路径（每段中位数 24 条、最多 358 条；5,890 条标注 `git_eol_lf`，
  4,119 条是更早的未标注哈希）；130 个段带 `prior_sections_immutability`；`supersession` 有 4 种键集合（14/116/3/25 个段）。每个路径的「最后一个列出它的段」里，一个 OPS-073 段占 333 条、
  DEVX-015 V3 段占 148 条。含义：(1) 通用校验不能假设统一 schema，只能校验**段自己声明的**性质（来源哈希、不可变前缀、取代链、安全标志）；(2) 历史段的内容字面量（schema_version、task_ids、
  safety 字典、implementation 字典）被逐 wave 测试重复断言，但只要「累计不可变前缀」一次性覆盖所有历史字节，且前缀哈希绑定到真实的 `source_commit` 与 git blob，内容字面量就是冗余的——
  这正是 P4-2 变异等价性要证明的命题，不是预设结论；(3) 变异夹具必须覆盖「历史段字节被篡改」「当前来源被改动但没有后继段取代」「后继段取代了来源却记录了错误哈希」等类别，并对每个现有
  逐 wave 测试记录它能抓到的变异集合，通用校验抓到的必须是其超集。
  - 离线实现状态：十个新模块（`publication_scope|journal|orchestrator|commands|steps|checks|validation|publish_steps|services|cli`）、两个脚本（`architecture_arch005_publish.py`、`architecture_arch005_validate_candidate.py`）、范围策略草案（状态 `PROPOSED_PENDING_OWNER_REVIEW`）和 164 项测试（整条 run 用脚本化的仓库/围栏/远端/启动器走完 A–E）。真实 `plan` 在 C1 候选上干跑：78 个 diff 路径派生出 9 个 owned、69 个 shared，对比手写的 67 个 owned。移植规则：只新增文件；测试命名 `test_arch_005_publication_*.py`（归入 architecture-fitness 套件）；任何 `.py` 都不出现 `task_register.md` 字样（consumer inventory）；写文件只走 `write_bytes_atomic`（依赖门，C1 第一轮 prep 因此失败过）；范围策略文件自身在普通发布的禁止路径里，所以它的批准必须发生在 C2 候选冻结**之前**。
- 2026-10-07：**C2（S3）已从会话 scratchpad 移植进仓库，候选尚未冻结**。新增文件全部是新路径，不改任何受哈希固定的历史源码：十个模块`src/ai_trading_system/platform/architecture/publication_{scope,journal,orchestrator,commands,steps,checks,validation,publish_steps,services,cli}.py`、两个脚本 `scripts/architecture_arch005_publish.py` 与 `scripts/architecture_arch005_validate_candidate.py`、范围策略草案 `config/architecture/devx_016_publication_scope.v1.yaml`（状态 `PROPOSED_PENDING_OWNER_REVIEW`）、十个测试文件 `tests/test_arch_005_publication_*.py` 与共享夹具 `tests/publication_run_support.py`。
  - **移植验证**：仓库环境（项目 venv 3.11.9，`-n 16 --dist loadfile`）169 项通过（64 s）；ruff、black、mypy strict 对新文件全部通过（移植时发现并修正了离线开发没覆盖的项目规范：22 处 E501/I001、`publication_scope.py` 里同名变量复用两种类型、`publication_cli.py` 的 `Any` 返回、一个测试里写死的 `D:\Work\AITradingSystem` 根路径改为 `Path(__file__).resolve().parents[1]`）；`architecture_devex.py validate`：依赖门 PASS（0 个违规），模块与测试归属 0 个孤儿，只剩两条预期的清单新鲜度项（模块清单、测试清单，等生成器重建）。LANE 阶段 governed preflight（SINGLE_LANE，coordinator，26 项声明）PASS，证据 `claude_c2_lane_preflight.json`。
  - **新增的设计决定（owner 推送默认模式）**：第五次发布里，harness 的权限分类器拒绝了我自己执行的 `git push origin main`，最终由 owner 在终端推送。所以 S3 命令默认**从不推送**：E58 授权门在默认模式下记 `NOT_REQUIRED_OWNER_PUSHES`；E59 在远端 tip 不是冻结候选时停在 AWAITING_AUTHORIZATION，并给出 `git push origin main`；owner 推送后 `resume` 只用远端 tip 证明（`pushed_by = OWNER_TERMINAL`），再做三方 SHA 核对与收尾。`--push-by-command` 是 owner 的显式选择，此时仍要求与冻结候选、基线、范围完全匹配的授权记录。这样 agent 不会再用命令包装去达成同一个被拒绝的动作。新增 5 个测试（默认从不推送、owner 的推送被远端证明后收尾、远端被推到别的提交时不接受、CLI 默认流程、旗标是显式选择）。
  - **system_flow**：已加 C2 一段并用 F1 `reseal` 重算封印（只改策略 5 行；`build` PASS；生成物留给生成器链）。
  - **owner 决定（2026-10-07，会话中）**：DEVX-023 的「命名 DQ 父证明不再内嵌整库重放」排在 C3 之前（见 10.10 第 5 项与 DEVX-023 文档）。C2 照常先发布，之后是该契约波次，再 C3。
  - **待办**：范围策略的核心清单须在冻结候选之前由 owner 评审并把状态改为 `OWNER_APPROVED_ENFORCED`（该文件本身在普通发布的禁止路径里）；C2 的正式窗口里先用 `stage1_timing_plugin.py` 单独测一次 stage 1 的构成；C2 自身只能走手工链（策略此时仍是 PROPOSED），C2 之后的候选才用这条命令发布。
- 2026-10-07：**owner 批准范围策略核心清单（AskUserQuestion，按草案）**。`config/architecture/devx_016_publication_scope.v1.yaml` 的状态改为 `OWNER_APPROVED_ENFORCED`，`approval_ref` 记录该决定，复审条件仍是围栏策略/生成器集合/共享生成权威/checkout guard 排除项变化时，或不晚于 2027-01-07；新增一个仓库级测试（真实策略必须能在不带 PROPOSED 许可的情况下加载、状态为已批准、受保护路径都真实存在）。`plan` 在 C2 自己的候选上仍然拒绝（策略文件在它自己的禁止路径里），这是设计：策略的批准发生在候选冻结之前，C2 走手工链，C2 之后的候选才用这条命令发布。owner 同时确认「推送由 owner 在自己的终端执行」的最后一步不变。
- 2026-10-07：**C2 已发布**（候选 `0834ed957`，main = origin/main，普通推送 `7ead2d6ff..0834ed957`；正式事务 `gov-007-d25-formal-20261007-v1`；无恢复、无人工修复；最后的 `git push origin main` 由 owner 在终端执行）。
  - **候选链**：一个 prep 事务（`gov-007-d25-prep-20261007-v1`：第一轮生成器产生 59 个脏路径、提交 34 个文件，第二轮零差异，failed 释放）；一个正式事务。owned 声明 = d24 正式事务模板的 67 项 + 由**已批准策略从提交范围 diff 派生**的 24 项（含策略文件自身，手工加入）；
    派生出的共享路径全部落在模板共享集合内（0 项差异）。这是派生逻辑第一次对真实候选的对照：与手写清单一致。
  - **耗时**：对照表见 DEVX-022 第 17.7 节。stage 1 785.5 s（诊断运行 761.1 s）、Full pytest 8,883.6 s（14,974 通过 / 4 跳过 / 0 失败）、`local-publish` 61 分 9 秒。
  - **stage 1 的更正**：本文上一条「定位到的原因：命名 DQ 证明体积」被 C2 窗口的逐调用计时否定（DEVX-022 第 17.7 节）：stage 1 ≈ 约 32 次租约库重放 × 约 21 s，证明构造与序列化约 3%；旧外推（860–890 s）作废。
  - **owner 决定（会话中）**：(1) 范围策略按草案批准；(2) 路线改为 C2 → 封印 S（先出设计，owner 评审边界与信任模型）→ C3 → C4，W-DQ 降为可选。
  - **事件记录（如实披露）**：(1) stage 1 的归因：我先前（DEVX-022 第 17.6 节、DEVX-016/023、任务行与给 owner 的报告）把 stage 1 变慢归因于命名 DQ 证明体积，那只是三个数据点的相关性；C2 窗口的逐调用计时否定了它，已更正并重新请 owner 选择路线（owner 据我的错误归因做的 W-DQ 排序决定随后改为封印 S）；(2) 封印 S 设计页第一版把封印写成「沿用 O3 的边界」，不准确（封印本身就是落盘的已校验结论），已更正并重发；(3) 本次没有启动逐进程计数器采样，没有 CPU 小时数据；(4) 冻结驱动的 stage 1（785.5 s）期间我并发跑了一次 32 s 的 cProfile 重放和两次重放探针，对它有轻微扰动（无并发的诊断运行是 761.1 s）；(5) 最后的 `git push origin main` 由 owner 在终端执行（23:50:48；我在 REMOTE_PUSH_PRE 与 CLOSEOUT 预检全部通过后请求）；(6) 为避免租约在 owner 不在场时到期，我起了一个脱离的 `heartbeat` 保活进程（每 20 分钟一次，共续期 1 次，推送后自动退出）；这是既有的受审命令，围栏每个检查点本来就会做同样的续期；(7) 两次 `git diff --cached --stat`（C2 移植提交与 prep 第 1 轮生成物提交各一次，都在显式路径 `git add` 之后、暂存集只含我自己的路径）没有带完整的已登记排除集，按规则登记为审计事件：没有打开、哈希、复制或修改被排除路径，输出里也没有出现该路径。
  - **S3 的后续小项**（登记，不阻塞）：(a) E59 在 owner 模式下等待 owner 推送时应对租约做心跳，避免 6 小时租约窗口在 owner 不在场时到期（本次由我手工保活）；(b) `plan` 对「候选改了策略文件」给出更明确的提示（需要手工链）；(c) 可选的 stage 1 计时诊断步骤。
  - **临时资源（生命周期记录，2026-10-08 00:07 收口）**：按精确绝对路径白名单删除（脚本 `scratchpad/cleanup_d25.py`，日志 `D:/Work/devx025-c2-cleanup.log`，释放约 28.06 GB，用时 8 分 18 秒，其中本次 Full 的 basetemp 28.06 GB / 708,119 个文件 359 s）：`%TEMP%\pytest-of-JACK\pytest-23364`，以及 `D:/Work/ptc2`、`D:/Work/ptc_s1t`、`D:/Work/ptc_plug`（聚焦回归、stage 1 计时运行与插件冒烟的 basetemp）。清理前确认无其他 python 进程，必需证据都已在 `outputs/architecture/integration_revalidation/devx015-v389/` 与 `outputs/validation_runtime/gov-007-d25-*`。保留：`pytest-of-JACK` 下的空目录 `pytest-23466/-23467/-23468` 与 `pytest-current`（pytest 自管）、`D:/Work/devx015-*`（已登记证据根）、`devx020-k`、`devx022-t1`，以及 `outputs/architecture/trading_2564_s3b_prospective_capture/synthetic/` 下命名 DQ 测试写出的 62 个目录（受治理的合成证据根，未动；每次 stage 1 运行约写 0.7 GB，体积卫生见 DEVX-023 第 11 节）。可恢复性：删除的都是可再生的临时目录，不需要恢复。
- 2026-10-08：**S3 的第一次真实运行（用来发布 DEVX-023 P 候选）发现一个缺陷，登记为 C2.1 并修复**。run `p-20261008-v1` 的 A00–A05 全部通过（依赖门 28 s），B10 prep_acquire 已经执行（prep 事务已 acquire、租约 ACTIVE），随后追加日志失败：`PermissionError: [WinError 5]`。原因：`PublicationJournal.append` 用 `write_bytes_atomic` 重写整个文件，而我为监视进度开的 `tail -F`（以及 Monitor 结束后遗留的孤儿 tail 进程）一直打开着 journal；Windows 上 `os.replace` 的目标被别的进程打开就拒绝，原子写入的有界重试只能应付瞬时占用。后果：事务 acquire 了而 journal 没有 DONE，租约留在 ACTIVE（我手工 `release --outcome failed`，证据 `claude_p4_v1_prep_release.json`）。resume 语义本来能处理这种情形（B10 是有副作用的步骤，带 `already_done` 探测），但这次改用新 run。
  - **修复（C2.1，随 DEVX-023 P 候选发布）**：journal 改为只追加——`O_APPEND` 单次写一行加 fsync，不再整文件替换（哈希链与单写者锁保证完整性与互斥，这是只追加日志的正当例外，依赖门只统计 `write_text` / `write_bytes` / `json.dump`）；重放时把不以换行结尾的最后一段视为撕裂的尾巴并忽略，下一次追加先把它截掉。新增测试：读者占着文件时仍可追加（在 Windows 上复现了这个缺陷）、撕裂的尾巴被忽略并被下一次追加修复；既有的篡改/删行/重排测试不变。
  - **教训（已写入记忆）**：监视 journal 用短读轮询，不要用 `tail -F`；Monitor 结束后孤儿 `tail.exe` 会继续占着文件（本次清理了两个，一个是 C2 驱动日志的旧监视遗留）。
- 2026-10-08：**第七次发布完成：DEVX-023 P + C2.1**（候选 `5150efbac`，S3 命令 run `p-20261008-v3`，普通推送 `0834ed957..5150efbac`，正式事务 `p-20261008-v3-formal` RELEASED/COMPLETED，owner 在终端推送）。**S3 命令第一次完整跑通 A–E**：
  A–C 699 s 无人值守，验证驱动 11,795 s（Full 15,027 通过 / 4 跳过 / 0 失败），`local-publish` 3,777 s，E52–E57 的其余步骤约 480 s；E59 停在 AWAITING_AUTHORIZATION 约 3 小时 31 分，owner 推送后 `resume` 只用远端 tip 证明（`pushed_by = OWNER_TERMINAL`），
  E60 三方 SHA 核对（本地 main = origin/main = 远端 tip = 候选）、E61 CLEANUP_PRE、E62 释放 COMPLETED、E63 恢复分支共 110 s。journal 70 条、哈希链完整；C2.1 的只追加修复在真实运行里没有再出现写入失败。耗时对照见 DEVX-022 第 17.8 节。
  - **与手工链的对照**：同样的 A–E 在 C2 是手工脚本拼接（含多次我手工等待与续期）；这次从启动到停在 E59 没有任何手工干预（run 之间的暂停是 DEVX-023 的开关开启诊断运行，属于该任务）。
  - **事件记录（如实披露）**：(1) `p-20261008-v1`：我用 `tail -F` 监视 journal，prep acquire 后写入失败，租约留在 ACTIVE，手工 `release --outcome failed`（登记为 C2.1，已修复）；(2) `p-20261008-v2` 在 `C26.readiness` 后暂停做 DEVX-023 的开关开启诊断，发现命名 DQ 的受限子进程拒绝导入新模块（`NAMED_BOOTSTRAP_UNREVIEWED_IMPORT`），修复内核钩子（导入失败串行回退）后 v2 的正式事务按失败释放，用新 run id v3 重做候选链；
    (3) owner 等待期间我外挂的保活进程（脱离式 PowerShell，每 20 分钟一次 `checkout_guard heartbeat`）在 09:24:46 的最后一次心跳之后、约 09:45 之前无记录地消失（日志没有退出行），原因未查明；租约当时仍有约 6 小时余量，owner 推送后的 `checkpoint` 也会重新心跳，所以没有影响。结论：旁路保活进程不可靠，E59 应由命令自己心跳（后续小项 (a)）；
    (4) E52 / E56 被标 SLOW 是基线常量（60 s）设低了，实测约 190–220 s（C2 链也是 205 / 218 s），不是变慢（后续小项 (b)）；(5) 本次启动了逐进程计数器采样，补上 C2 的缺口。
  - **S3 后续小项**（登记，不阻塞，随 C3 候选一起做）：(a) E59 在 owner 模式下等待时由命令自己对租约心跳；(b) E52 / E56 的基线改为约 210 s（具名常量，仅用于报告）；(c) `plan` 对「候选改了策略文件」的提示；(d) 可选的 stage 1 计时诊断步骤。
  - **路线**：C3（P4 通用 section 链校验）→ C4（S4）；DEVX-023 P5/P6 与 DEVX-022 的时长种子刷新穿插，种子刷新是 Full 敏感输入，用逐文件起止时间做单独归因。
  - **临时资源（生命周期记录，2026-10-08 10:15 收口）**：按精确绝对路径白名单删除（脚本 `scratchpad/cleanup_pub7.py`，日志 `D:/Work/devx027-pub7-cleanup.log`，释放约 28.08 GB，用时约 8 分 30 秒）：`%TEMP%\pytest-of-JACK\pytest-23474`（本次 Full 的 basetemp，28.07 GB / 710,065 个文件，366 s）、`D:/Work/ptp1`、`D:/Work/ptp_s1`（stage 1 开关开启诊断运行的 basetemp）。清理前确认无其他 python 进程，必需证据都已在 `outputs/architecture/integration_revalidation/devx015-v389/`、`outputs/architecture/publication_runs/p-20261008-v3/` 与 `outputs/validation_runtime/p-20261008-v3-*`。保留：计数器 CSV（含 `devx026-p4-counters.csv`）与各清理日志、`D:/Work/devx015-*`（已登记证据根）、`devx020-k`、`devx022-s1`、`devx022-t1`；`pytest-of-JACK` 下的空目录与 `pytest-current` 由 pytest 自管。
- 2026-10-08：**P4 的真实数据（只读）与修订的分步计划**。动手设计 P4-1 之前，用与 `tests/test_arch_004_refactor_policy.py::_latest_active_source_mismatches` 相同的账本代数（按段顺序、`removed_live_source_paths` / `superseded_source_paths` 弹出、后写覆盖前写、忽略 `historical_*` 记录）在整个合并后的权威上对照实时文件：
  - **事实**：325 个段，1,992 条「活动」来源记录；与实时文件不一致 383 条（不含各段的追溯调整）：**375 条是已退役的任务影子注册表**（`registry/development_tasks_shadow/**`，S5 cutover 之后内容已改写），**2 条是易变的生成物**（`docs/task_register.md`、`inputs/architecture/arch_005_task_registry_index.yaml`，每次任务更新都会变），**6 条是真实代码文件**（`scripts/architecture_compatibility_authority.py`、`scripts/architecture_report_catalog_flow_authority.py`、`src/ai_trading_system/platform/architecture/report_catalog_flow_authority.py`、`src/ai_trading_system/atlas/__init__.py`、`src/ai_trading_system/atlas/cited_query_renderer.py`、`src/ai_trading_system/contracts/__init__.py`）。
  - **这些不一致今天不会让任何测试失败**——原因是 `_latest_active_source_mismatches` 的「追溯调整」：凡是被约 30 个「权威段」（名单写死在测试里）的 `superseded_live_source_paths` 登记过的路径，之后的漂移一律免除（`mismatches - (retroactive_paths - recorded_superseded_paths)`），除非最新段再次取代它；另有两个写死在测试里的 Python 常量集合（TRADING-2488 后继影子路径、TRADING-2493 后继路径）。
    例：C1（2026-10-07）改动了 `scripts/architecture_report_catalog_flow_authority.py`，权威没有要求重新登记，所有测试通过。也就是说，现行的「当前哈希权威」有相当一部分活在**测试代码的常量与追溯规则里，而不是数据里**，并且比表面上弱：被某个权威段登记过的路径，之后的修改不会被这些测试发现。
    `_source_sha256` 本身是一个约 200 行、按「哪些段存在」逐层嵌套的解析函数（`tests/test_arch_004_refactor_policy.py` 第 14483 行起）。
  - **含义**：(1) P4 不是「把断言改写成通用校验」就完事；必须先把「谁拥有哪条路径的当前哈希」「哪些路径已退役」「哪些路径是易变的生成物」做成**经评审的数据**——新的策略文件，按启发式治理要求写明 owner、版本/状态、理由、预期效果、验证证据和复审条件；
    (2) 通用的终态账本不变量（每条活动记录 = 实时文件，退役与易变路径显式声明）比现行逻辑**更严**，当前仓库要通过它，需要对上面 6 个真实文件做一次「采纳当前内容」的账本更新——这是 owner 应评审的决定，不是我能默默做的；
    (3) 旧测试里的字面量断言之所以多数冗余，是因为历史段由不可变前缀/索引链固定，这一点由变异测试证明（P4-2），不是预设。
  - **修订的分步（每一步一个候选，P4-4 之前不删除任何现有断言；候选里搭载 S3 后续小项与 DEVX-022 的时长种子刷新）**：
    - **P4-1（C3a，纯增量）**：新模块 `compat_ledger.py`（活动账本代数、终态不变量、退役/易变声明的读取，返回带类型码的违规列表）；策略文件草案 `config/architecture/devx_016_compat_ledger_policy.v1.yaml`（状态 `PROPOSED_PENDING_OWNER_REVIEW`：退役前缀、易变路径、待采纳的 6 个路径及理由）；新测试（全是新文件）：合成夹具上的变异测试（至少 15 类：篡改历史记录、删除/重复/调换段、记录哈希错误、取代了却没有重新登记、`new_source_paths` 与既有记录冲突、`removed_live_source_paths` 指向仍存在的文件、安全标志被去掉……每类都必须产生预期的违规码）、真实仓库的终态不变量（策略未生效时以「报告」模式运行并断言例外集合恰为上面的 8 条，生效后断言为零）；
      线下差分对账脚本（证据，不进 CI）：对每个停止段比较通用代数与旧 `_latest_active_source_mismatches` / `_prior_active_source_mismatches` 的结果。
    - **P4-2（C3b）**：变异等价性证据——在 detached worktree 上对每类变异分别跑旧的逐 wave 测试子集与新的通用测试，记录各自抓到的变异集合，要求通用 ⊇ 旧；产出逐测试的等价性表（分类沿用 P4-0）。
    - **P4-3（C3c）**：owner 评审策略后，把策略切到 `OWNER_APPROVED_ENFORCED`，由生成器追加一个账本采纳段（只列 6 个真实文件的当前内容），逐 wave 专属的语义期望若确有必要则迁成段内数据。
    - **P4-4（C3d…）**：分批删除逐 wave 测试与常量，每批附等价性证据，owner 抽查；`tests/test_arch_004_refactor_policy.py` 本身由最新授权段接管（同 S6 的模式）。
  - **需要 owner 决定的两件事（P4-3 前）**：(a) 策略文件的内容评审（退役前缀、易变路径、6 个待采纳路径）；(b) 是否接受「通用终态不变量比现行逻辑更严」这一结果（现行逻辑对被登记过的路径之后的漂移是免除的）。
- 2026-10-08：**C3a（P4-1 + S3 后续小项）已实现，候选尚未冻结**。
  - **S3 后续小项**（提交 `96180dc6c`）：(a) `resume --wait-for-owner-push [--owner-wait-minutes N]`——E59 在 owner 模式下保持存活（默认上限 720 分钟），每 60 s 探测远端 tip，每 20 分钟用受审的 `checkout_guard heartbeat` 续租约，owner 推送后自动完成 E60–E63；连续 5 次 `ls-remote` 失败或一次续租失败即失败关闭；owner 模式的 E59 不再标记为有副作用的步骤，被打断的等待可以直接再运行；
    (b) E52 / E56 的 SLOW 基线 60 s → 210 s（具名常量 `BASELINE_FENCE_CLOSURE_SECONDS`，仅用于报告）；(c) `plan` / `run` 对越出范围策略的候选拒绝时附 `next_action`（走手工链）。新增 8 项测试，ruff、black、mypy strict 通过；(d) 可选的 stage 1 计时诊断步骤仍留待以后。
  - **P4-1**（提交 `2ee0059dd`，纯增量，不删除任何既有断言）：新模块 `compat_ledger.py`（活动账本代数、终态不变量、策略读取，返回带类型码的违规）、策略草案 `config/architecture/devx_016_compat_ledger_policy.v1.yaml`（状态 `PROPOSED_PENDING_OWNER_REVIEW`）、`tests/test_arch_005_compat_ledger.py`（60 项）与 `tests/compat_ledger_support.py`；`docs/system_flow.md` 加一段并重算封印。
    真实仓库：325 个段，1,992 条活动记录，383 条与实时文件不一致 = 375 条被 S5 退役 + 2 条易变生成物 + 6 条已承认的陈旧代码文件；策略下零违规、零失效条目；S5 之后 25 个生成段的严格结构检查零违规；全部 325 个段的安全检查零违规。20+ 类变异（合成权威）各自产生预期的违规码。
  - **线下差分证据**（`outputs/architecture/integration_revalidation/devx015-v389/claude_c3a_ledger_differential.json`，脚本 `scratchpad/p4_differential.py`，不进 CI）：在全部 324 个停止段上，把通用代数的差异集合与旧 `_latest_active_source_mismatches` 逐段对照——**旧结果在每个停止段都是通用结果的子集（0 个漏报）**，54 个停止段两者完全相同；旧函数最多报告 411 条、通用代数最多 508 条，差额正是旧逻辑里「追溯调整」免除的路径。
    也就是说：通用校验不会漏掉旧测试会抓到的实时漂移，并且更严（P4-2 还要对「逐 wave 测试的其余断言」做同样的变异等价性对照）。
  - **发现与处理**：(1) 新的真实仓库测试（r02）和既有的约 95 个 hash-authority 测试一样，在改动被固定的源文件之后、生成器链刷新之前会失败（重算 system_flow 封印后，`docs/system_flow.md` 与 006d 配置相对 V3 记录漂移）——预期行为，不是缺陷，生成器链之后恢复；
    (2) **时长种子刷新（DEVX-022 第 17.8 节）没有搭载在本候选里**：自动模式的分类器拒绝了我对受治理时长种子清单的写入；该改动是 Full 敏感输入，也尚未得到 owner 的明确确认，所以只保留 v27 预览（刷新工具自己的构造函数生成，sha256 `e3c8545f77dfad878d14767ce616d17db5bdb22f000b5afe0f8317bbf07d8e35`，与工具 dry-run 打印的 `output_sha256` 一致；命名 DQ 候选文件在新种子里排第 5），等 owner 确认后作为独立步骤。
- 2026-10-08（下午）：**C3a 的 Full v5 全绿，但发布阶段被宿主卡死中断；恢复后以 v6 重发**。
  - **v4 → v5**：owner 回复「做」后把时长种子刷新（DEVX-022 M5，刷新工具 `--write` 成功，分类器没有再拦）并入候选；v4 run 当时刚停在 `C26.readiness`，尚未派发任何验证阶段，所以以 FAILED 释放并重跑为 v5（披露：我原说「作为独立步骤」，这是为了不多等一轮 4.5 小时的偏离，种子提交可单独还原；C3a 其余内容几乎不增加 Full 时长，归因仍然干净）。
    我的两个失误（都没有后果）：v5 第一次启动时一个 `git rev-parse --short A B` 写错，链在第一步就退出；后续阶段启动命令里 `--parent-run` 的路径写法与记录不一致，被 `PUBLICATION_RUN_RECORD_CONFLICT` 拒绝，立刻改正重启。
  - **v5 结果**：stage 1 909.6 s、contract 284.1 s、integration 68.0 s、reproducibility 47.0 s、architecture-fitness 1,374.1 s、Full 15,095 通过 / 4 跳过 / 0 失败（pytest 9,343 s，窗口 9,289 s，+2.9%）；种子的效果与归因见 DEVX-022 第 17.9 节（起跑提前 1,665 s，但该文件自身变长 28%）。
    `--wait-for-owner-push` 没有来得及在真实环境里被验证（E54 就被中断）；owner 的推送授权（仅候选 `5480507f1`）随该候选作废，v6 到 E59 再问。
  - **事故与恢复**：见 DEVX-021 第 10 节（显示唤醒卡死杀死 `local-publish`；两个残留锁经 owner 批准审计后删除；`local-publication-recover` 收口；`p-20261008-v5-formal` 以 FAILED 释放；什么都没有发布）。
  - **v6 候选**：新提交 = S3 小项 (e)（`WmiDetachedLauncher` 用 `Win32_ProcessStartup.ShowWindow = 0` 隐藏窗口，owner 要求；测试断言启动信息先于 Create 构造且不使用被 WMI 拒绝的 `CreateFlags`）+ 本节与 DEVX-021/022 的记录。S3 小项 (f)（worker 被杀时的 `next_action` 提示）与 (d) 留待以后。
- 2026-10-08（夜）：**C3a 已发布（第八次发布）**：候选 `e30c62a97`（S3 run `p-20261008-v6`；v5 事故后重发），普通推送 `5150efbac..e30c62a97`，本地 main = origin/main = ls-remote = 候选；正式事务 `p-20261008-v6-formal` COMPLETED；journal 68 条，run COMPLETE。
  - **推送由 Claude 执行**：owner 在 17:38 的 AskUserQuestion 里选了「仅本候选，由你(Claude)推」（证据 `claude_c3a_v6_push_authorization.json`，上次对 `5480507f1` 的授权随旧候选作废，所以重新问了一次）；E59 前核对 Full 全绿（15,096 / 4 / 0）、E57 预检 PASS、远端 main 是候选祖先、本地 main = HEAD = 候选、无 `.git` 锁文件；执行 `git push origin main` 一次，输出与核对记录在 `claude_c3a_v6_push.log`（S3 的 journal 只能证明远端 tip，不能证明是谁推的，所以另记）。
  - **交付**：S3 小项 (a) E59 的 owner 模式等待 + 租约心跳（本次因我在 E59 开始 1 分钟后就推送，只验证了 60 s 探测与自动收尾 E60–E63；20 分钟心跳仍未在真实环境里被长等待验证）、(b) E52/E56 基线 210 s（E56 仍标 SLOW：273 s）、(c) 拒绝时的 `next_action`、**(e) 隐藏窗口启动器**（真实验证见 DEVX-022 第 17.10 节）；P4-1（`compat_ledger.py` + 策略草案 `PROPOSED` + 60 项测试，Full 里常驻）；DEVX-022 M5 时长种子刷新。
  - **耗时**：见 DEVX-022 第 17.10 节。要点：Full 全绿、pytest 9,250 s；stage 1 873.6 s（未越 900 s 告警线）；`local-publish` 82 分钟（上次 63，SLOW）——重放主导的工作随租约库增长在变慢，下一个候选应先做 DEVX-023 P6。
  - **仍欠**：S3 小项 (d)（stage 1 计时诊断步骤）、(f)（worker 被杀时的 `next_action` 提示）；P4-2…P4-4（P4-3 之前须 owner 评审 compat 账本策略）；C4（S4）。
- 2026-10-09（凌晨）：**DEVX-023 P6 已发布（第九次发布）**：候选 `a63827b28`（S3 run `p-20261008-v7`），普通推送 `e30c62a97..a63827b28` 由 Claude 在 owner「仅本候选」授权下执行，本地 main = origin/main = ls-remote = 候选；journal 68 条，`slow_steps` 为空。对本任务的意义：S3 链的「发布命令 + `local-publish`」两段快了一半（`local-publish` 82 → 38.8 分钟，E52 / E56 约 −38%，整条链 5 小时 20 分 → 4 小时 30 分），之后每个候选都走这条更快的链；Full 现在占链的约八成。详见 DEVX-022 第 17.11 节与 DEVX-023 第 12.6.1 节。
  - **路线**：回到 C3：下一个候选 **C3b（P4-2 变异等价性证据）**，然后 C3c（P4-3，owner 先评审 compat 账本策略）→ C3d（P4-4）→ C4（S4）→ DEVX-017 → OPS-082 → 阶段 5。S3 小项 (d)(f) 与 E59 的 20 分钟心跳长等待验证仍欠。
- 2026-10-09（夜）：**第十次发布完成（DEVX-023 封印 S 候选 A）与 C3b（P4-2）夹具**。发布本身见 DEVX-022 第 17.12、17.13 节与 GOV-007，这里只记与本任务相关的两点。
  - **S3 后续小项 (g)（新增）**：E50 `pre_publish_checks` 第一次 FAILED——检查 4「没有活动的 git/python 进程」看到两个 `git.exe`（命令行带 `core.hooksPath`、`safe.directory=*`、`core.fsmonitor=` 等 `-c` 参数，是桌面应用自己的 git 状态轮询，既不是发布命令的子进程也不是我起的）。显式 `resume --retry E50.pre_publish_checks` 约 1 分钟后通过（该步 11.4 s）。设计：把 E50 与 A03 共用的进程检查改为二选一——「放行带该签名且父进程不在本命令进程树里的 `git.exe`」或「步骤内有界重试」（次数与间隔为具名常量，属允许的低风险常量）；不放宽对 python 进程的检查。随下一个有代码改动的候选一起做。
  - **C3b（P4-2）夹具与迷你根目录自测**（会话 scratchpad `c3b/`；发布时归档为证据，不进仓库代码）：`Authority` 读取一份权威副本（策略、遗留 YAML、索引、片段），改动后按生产渲染函数（`render_fragment`、`render_index`）重新封印；三个通道——RAW（只改字节、不重封，必须被加载器的哈希钉拦下）、CONSISTENT（改内容后按生成器规则重封，模拟一次错误的重新生成，只有语义检查能抓）、LIVE（只改被登记的真实文件）。19 类变异 = 28 个用例（含未变异对照 C00 与三个应被容忍的对照 L03–L05）。
    迷你根目录（约 4 MB：策略、遗留 YAML、索引、28 个片段、账本策略；live 文件走真实检出的叠加哈希器）上的**新侧**自测（加载器 + 活动账本 + 结构检查，不含 pytest）：渲染器对未改动的权威强制重封后逐字节还原 32 个文件；基线零违规（1,992 条活动记录，381 条不一致全部被策略容忍）；
    **RAW 5/5 被加载器拦下**（`AUTHORITY_LEGACY_HASH_DRIFT`、`AUTHORITY_FRAGMENT_HASH_DRIFT`、`AUTHORITY_INDEX_ORDER_INVALID` ×2、`AUTHORITY_FILE_MISSING`）；
    **CONSISTENT 18 个用例里账本本身抓到 8 个**：M06 ×2 `LEDGER_SAFETY_FLAG`、M11 ×2 `LEDGER_HISTORY_REWRITTEN`、M03 `LEDGER_LIVE_DRIFT`、M04 `LEDGER_SECTION_SOURCES_BEYOND_DECLARED`、M14 截断 `LEDGER_LIVE_DRIFT`、M12 四个码；
    **没抓到的 10 个**：M01、M16 ×5（遗留 schema / 任务 id / 状态词、片段的 owner_decision 与状态词）、M09 / M10（遗留 `prior_sections_immutability` 被破坏 / 去掉）、M05（调换相邻片段）、M08（删除中间片段）、M13（`removed_live_source_paths` 指向仍存在的文件）。它们本来就不是账本这一层的职责：由遗留前缀钉（`test_legacy_prefix_bytes_equal_exact_start_base` 等）、生成器新鲜度测试和旧的逐 wave 测试各自负责，哪一层抓到、旧测试是否冗余，要等克隆上的实跑；其中 M05、M08、M13 三类很可能是真缺口（账本既不校验顺序、也不校验被删除的中间段、也不要求被移除的路径真的不存在），是 P4-3 要补的通用检查的候选。
    LIVE：M02、M15 被抓（`LEDGER_LIVE_DRIFT`），M17 / M18 / M19 的对照被正确容忍。
  - **下一步与临时工作区（生命周期记录）**：在已发布 main 的本地克隆上运行同一批用例，同时跑旧的逐 wave 测试（`tests/test_arch_004_refactor_policy.py` 全文件、006c、2452）与保留侧测试（`test_arch_005_compat_ledger.py`），产出逐用例 / 逐测试矩阵与差距清单；不删任何断言。任务 DEVX-016 P4-2；路径 `D:/Work/devx016-p42/clone`（`git clone --local`，对象硬链接）、`D:/Work/devx016-p42/bt`（pytest basetemp）、`D:/Work/devx016-p42/results`（逐用例 JSON 与 junit）；退出条件：矩阵与差距清单写入本文档并核对，证据复制到 `outputs/architecture/devx_016/p42/` 后，按精确路径白名单删除克隆与 basetemp。运行期间不与发布链并行。
