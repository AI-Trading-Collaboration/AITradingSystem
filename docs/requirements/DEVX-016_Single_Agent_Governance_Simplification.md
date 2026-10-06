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
| S4 | S3、P4 | 见 10.8 |

### 10.10 开放问题（需 owner 决定）
1. **发布范围清单**的内容与评审人：普通发布允许触碰哪些路径？（S3 的前提。）
2. `TASK_SOURCE_ONLY` 事务种类的 COMPLETED 收口规则是否可接受（S2）？
3. P4-4 的大删除是否要求 owner 逐批评审，还是以变异等价性证据为准？
4. 候选合并粒度：C1 合并 F1+F2+S2 是否接受？

- 2026-10-07：按三次真实发布（DEVX-015 基线、DEVX-020/021/022、DEVX-021 P1 + S3b 回归修复、DEVX-023）的实测摩擦点补充了第 10 节的实施分解；owner 2026-10-06 选择「按原计划做 P4」，不降级；第 10.10 节列出需 owner 决定的开放问题。下一步：C1（F1 + F2 + S2）。
