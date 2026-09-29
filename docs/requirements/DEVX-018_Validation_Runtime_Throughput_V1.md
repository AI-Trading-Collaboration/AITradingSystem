# DEVX-018 验证运行时吞吐优化 V1

状态：`IN_PROGRESS`（2026-09-30 登记）。任务 ID `DEVX-018_VALIDATION_RUNTIME_THROUGHPUT`。
本文由 v12 运行期间的仓库外草稿整理而来。GOV-007 文档第 281–287 行原计划中的 S1/P1/P4
改由本任务统一承接。

- Owner：project owner；执行：Claude Code（`agent_harness=claude_code`，actor `integration-coordinator`）
- 优先级：P0（决定 GOV-007 后续所有正式发布的节奏）
- 依赖：无外部阻塞；O1/O2 与 GOV-007 P1-C 基线在同一候选上正式验收（见第 1 节第 6、7 条）
- 关联：ARCH-004G2（S1 耗时权重刷新并入本任务 O1）、GOV-007（P1/P4 原计划）

## 1. Owner 决策（2026-09-30）

1. 同意调整优化顺序：O1（重型文件按节点分发）与 S1 同批优先，先于 P1/P4。
2. 同意 O2：正式 Full 之前的 tier 排除"内部运行真实 Full"的重型节点，由 Full 作为这些节点的唯一权威。
3. 若 v12 Full 失败，下一轮之前先落地 O1。
4. 总体原则：尽可能先把整个验证流程优化好，再开始大规模跑测。
5. v12 按原计划跑完并发布基线；优化开发在其运行期间并行准备，重型实测等 Full 结束后进行。
6. 2026-09-30 02:33 v12 在 architecture-fitness 93% 时被整树终止（系统日志显示同时 Microsoft Store
   更新了 OpenAI.Codex 应用，driver 由 Codex 启动；相关但未证实），Full 未派发。v12 已沿原 fence 收口
   FAILED/RELEASED，证据为 devx015-v389 的 `codex_v12_interruption_20260930.json` 与
   `codex_v12_failed_release.json`。
7. Owner 随后决定：先落地 O1+O2（含 S1），再跑正式验证；基线与 O1/O2 合入同一 GOV-007 候选、
   一次正式验证验收，替代原"基线与优化分阶段验收"安排。后续正式运行的 driver 须与桌面
   agent 应用脱钩启动。

## 2. 问题与实测证据

证据：`outputs/validation_runtime/gov-007-p1c-devx015-full-20260929-v11/test_runtime_profile.json`
（v11 Full，FAIL 于 4 个夹具节点，但遥测完整）。

| 指标 | 值 |
|---|---|
| 节点 | 14605，1334 文件 |
| 节点耗时合计 | 43.2 worker 小时 |
| 16 worker 理想均分 | 2.7 小时 |
| 实际墙钟 | 10.15 小时 |
| `tail_idle_max_seconds` | 33575（9.3 小时） |
| >300 秒的重型节点 | 108 个，合计 29.4 小时（68%），分布在 30 个文件 |
| 最长单节点 | 1.38 小时 |
| 最大文件 | `test_arch_005_integration_publication_fence.py` 568 分钟，135 节点全在 gw13 |
| 其后 | devx015_workflow_execution 358、coordination 355、governed_development_skill 265 分钟 |

根因：`--dist loadfile` 把整个文件固定到一个 worker；scheduler 契约
（`scripts/run_validation_tier.py` 约 2968–2999、3080–3095 行；`scripts/pytest_runtime_profile.py`
`resolve_scheduler_decision`）把 `xdist_dist == "loadfile"` 与"文件级耗时降序"写成正式证据条件。
因此 S1 单独刷新权重无法突破"最大文件 568 分钟"的下限。

v11 中重型节点并发分布：多数时间 3–4 个，峰值 14 个，尾部 3.69 小时只有 1 个。

重复执行：v12 architecture-fitness（49 文件、1540 节点）含 19 个重型节点共 9.34 小时，
与 Full 重复；去掉重型节点后，该 tier 仍被 `test_arch_005_task_checkpoint.py`（66.7 分钟）卡住。

失败回路：v8→v12 连续 5 轮，每轮 pre-Full tiers + Full 约 15–20 小时，夹具问题多在末段暴露。

## 3. 模拟估算（基于 v11 节点耗时，未考虑负载变化）

| 方案 | 墙钟 |
|---|---|
| 当前 loadfile（模拟 / 实际） | 9.47 / 10.15 小时 |
| 重型文件拆分，无并发上限 | 2.7 小时（不可行：CPU 过载） |
| 重型文件拆分，重型并发 K=4 | 7.37 小时 |
| **K=6** | **4.96 小时** |
| K=8 | 3.72 小时 |

K 的取值必须用实测决定：每个重型节点内部会以 -n16 运行一次真实的 Full；此前实测，DEVX-015 的重型节点在 -n4 下会触发 600 秒挂起保护；
v11 已经在 3–4 个重型节点并发下稳定运行。估算不外推为承诺，以 O1 验收实测为准。

## 4. 阶段拆分

### O1 + S1：可审计的节点级拆分调度（契约变更，最小串行契约波次）

- 设计：新增受审配置清单（建议 `config/architecture/devx_018_validation_scheduling.yaml`，含 owner、
  版本、理由、预期效果、复审条件），列出"按节点拆分的文件"与重型节点并发上限 K。
- 实现：通过 `pytest_xdist_make_scheduler` 提供 `LoadScopeScheduling` 子类：
  - `_split_scope`：清单内文件返回 nodeid（节点级单元），其余返回文件路径（保持 loadfile 语义）；
  - `_assign_work_unit`：按收集顺序取第一个合格单元，重型单元在途数达到 K 时跳过取轻量单元；
  - 排序：清单内文件按节点耗时、其余按文件耗时，统一稳定降序（S1 权重来自最新完整 profile）。
- 契约：runtime profile 新增 scheduler policy（如 `governed_split_scope_duration_descending_stable`），
  记录清单版本/哈希、K、单元定义；验证器同时接受旧 loadfile 契约（历史证据不变）与新契约。
  "loadfile assignment spans workers" 检查改为：非清单文件仍须单 worker，清单文件允许跨 worker。
- 前置核查：已确认四个最大文件无 module/session 级 fixture、无模块级可变全局；实施时对全部
  清单文件再做一次静态核查并记录。
- 验收：
  1. 单元测试覆盖拆分、K 上限、排序、契约验证（新旧两类 profile）；
  2. 代表性实测：publication_fence 文件单独以新调度运行，墙钟与 v11 同文件比较；
  3. 完整 Full 实测墙钟与 tail idle，不低于同等失败率；无挂起保护触发；
  4. 性能证据状态 PASS，policy/清单版本出现在 summary 中。

### O2：pre-Full tier 排除重型真实 Full 节点（策略变更，owner 已批准）

- 新增 pytest marker（建议 `real_full_chain`），由受审清单或显式标注确定，不用耗时阈值自动推断；
  architecture-fitness 等 pre-Full tier 使用 `-m "not real_full_chain"`；Full 不排除。
- tier summary 必须显示被排除的节点数和清单版本，保证审计可见；Full 仍为这些节点的唯一权威。
- 与 O1 合用后，architecture-fitness 预计从约 4–5 小时降到约 15–20 分钟（task_checkpoint 文件
  同样列入拆分清单）。
- 验收：tier 覆盖报告与 marker 清单一致；Full 集合不变（collection set SHA 相同）。

### O5：失败回路前移（轻量）

- 正式 Full 前以 -n2 运行"上一轮失败节点 + 本次改动影响到的重型节点"的风险冒烟；不改变
  "Full 之后不做部分重跑"的既有决策。

### P1 / P4 / O3：降低单个重型节点的成本（沿用 GOV-007 第 281–287 行约束）

- P1：单次 replay 内复用纯结构校验，仍逐条校验事件哈希、链、actor、时间、状态转移。
- P4：重复清单（如 `hook_capsule.ready.inputs.read_file_custodies` 约 10MB 被复制进每个后续事件）
  外置为持久化、不可变的引用证据，保留 v1 replay、原生句柄、进程与 Job 校验。
- O3：单次调用内的运行时身份计算去重（`acceptance_runtime_identity` 每次哈希 92 个发行版、约 1.65 万文件）；
  不跨调用缓存运行时身份或校验 PASS。
- 验收：代表性重型节点前后实测，不把单次 replay 的降幅外推为整个 Full 的降幅。

### O4：真实 Full 夹具模板复用（最后，按需）

- 例如 `test_full_transaction_replays_candidate_publish_and_closeout_receipt` 的 9 个变体各自
  生成一次真实 Full（约 17 分钟/个）；评估每 session 生成不可变模板再复制是否破坏绝对路径与
  身份绑定。仅在 O1–O3 之后仍有明显长尾时实施。

## 5. 执行顺序与工作区

1. 本任务登记：独立许可事务 `devx-018-registration-permit-20260930-v1` 写入任务行与本文。
2. 实施沿 GOV-007 候选分支 `claude/devx015-v389-integration`（基于 main `cbc31cdff`），
   以 GOV-007 准备事务承载代码、测试、配置、system_flow 与生成物；不另建 worktree。
3. 顺序：O1+S1 → O2 → O5 → 聚焦验证 → 新 GOV-007 正式事务（全部 required tiers + Full）
   → 基线与 O1/O2 一并发布 → P1/P4/O3 → 重测 → O4 按需。
4. 正式运行 driver 与桌面 agent 应用脱钩启动；K 与清单版本进入配置而非代码字面量。
## 附录 A：`real_full_chain` 候选静态分类（2026-09-30，只读，基于 v11 的 108 个 >300 秒节点）

共享 fixture `canonical_merge_repository`（`tests/test_devx015_workflow_integration.py:3637`）用显式的
param 决定是否运行真实整链：`whole_profile`（`full-profile*`、`full-readiness-profile`、
`native-*-full-profile-publish`）表示内含真实 Full profile；`source_job`（`source-job*`）表示内含真实 source job。
所以标注建议放在 param 层面（`pytest.param(..., marks=real_full_chain)`），不按耗时阈值判定。

| 类别 | 判定依据 | 节点 | v11 耗时 | 处理 |
|---|---|---|---|---|
| A 真实 Full profile | param 含 full-profile / full-readiness | 45 | 17.7 小时 | 标注 `real_full_chain`；O1 拆分 |
| B 真实 source job 整链 | param 含 source-job | 20 | 5.3 小时 | 标注 `real_full_chain`；O1 拆分 |
| A? 待逐个确认 | 治理类文件、无上述 param（如 `test_mandatory_acceptance_actual_runner_chain` ×10、`test_fixed_candidate_actual_runner_result_survives_main_advance` ×5） | 15 | 1.9 小时 | 实施时读代码确认内含 actual runner 再标注 |
| C 研究/运营流水线慢测 | smoothed_*、paper_shadow_*、refined_method 等单节点（不在 pre-Full tier） | 28 | 4.6 小时 | 不标注；只受 O1 排序影响；其 fixture 复用另立任务评估 |

## 6. 已确认事项（owner，2026-09-30）

1. S1 并入 DEVX-018 O1 交付；ARCH-004G2 的 S1 行改为引用本任务。
2. 初始 K = 6（v11 已稳定承受 3–4，短时 14），以 O1 实测结果调整并记入配置清单。
3. `real_full_chain` 初始清单：以 v11 的 108 个 >300 秒节点为候选，逐个确认它确实内含真实 Full 或整链后再标注；
   不用耗时阈值自动判定。
