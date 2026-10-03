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

## 7. 进展记录

### 2026-09-30 O1 + S1 + O2 实施（待正式验证）

- 纯解析器 `src/ai_trading_system/platform/validation_scheduling.py`：严格 YAML、未知字段/重复键拒绝、
  列表须排序去重、每个 `real_full_chain` 函数必须位于拆分文件内；清单缺失（最小夹具仓库）时策略
  不启用，存在但非法时 pytest `UsageError` 失败关闭；正式 Full 选择下，已收集文件中缺少清单函数也失败关闭。
- Full plugin（`scripts/pytest_runtime_profile.py`）：在 `-m` 取消选择前按清单打 `real_full_chain`；
  `pytest_xdist_make_scheduler` 仅在 controller、loadfile 下返回拆分调度器。collection 顺序不变。
- 调度器首版"持有重型单元的 worker 不再补派"在真实 xdist 集成测试中死锁：xdist worker 须知道下一项
  或收到 shutdown 才执行末项。修正后上限按"持有重型单元的 worker 数"计，持有者只再接受一个后继单元
  （最短的轻量单元，没有则下一个重型单元，同 worker 串行不增加并发）；重型完成后唤醒全部 worker。
- 契约：profile `scheduler.split_scope`（null 或 10 字段证据）；null 保持原单 worker 校验，否则只豁免
  清单文件；runner live reader 重读 repo 清单，Full 正式检查以受保护 git 语法读取 candidate blob，
  二者与证据逐字段比较。
- O2：`architecture-fitness` 以 `-p scripts.pytest_runtime_profile -m "not real_full_chain"` 运行；
  summary 新增 `test_selection_policy`（排除 marker、清单路径/SHA/版本、正式权威=full），控制台与 Reader
  Brief 同步披露。Full 禁止排除 marker。
- S1：`refresh_partial_duration_profile.py` 以 v7 PASS Full（`b8e6eb013`，14507 nodes / 1334 files）
  写入 `devx_018_s1_full_duration_partial_seed` v26。
- 聚焦验证：新测试文件含真实 xdist（-n3、cap=1）集成与 marker 排除用例；runtime profile / tier script /
  duration refresh / devex 共 330 项中仅 architecture fitness 因生成物待重建失败，待 generator 后复验。

### 2026-09-30 v13 Full 实测与 v14 修正

- v13（K=6）Full 4.95 小时（v11 10.15 小时），architecture-fitness 20.5 分钟（v11 >4 小时）：拆分调度
  达到目标量级；31 failed + 3 errors 均定位到四类根因，无一是测试语义错误：plugin `rootpath` 缺失（18）、
  宿主全局资源并发（固定 Job 名、HKCU 测试根视图）、K=6 过载触发固定超时、Job 进程列表 F2。
- 清单 v2：`heavy_concurrency_cap` 6→4（v11 稳态水平，owner 批准的"以实测调整"）；新增
  `exclusive_groups`（`fixed_publication_binding_job` 1 项、`host_registry_view` 15 项），组内单元
  由调度器保证不并发，用以复现 loadfile 下文件内串行的隐含互斥；清单成员须位于拆分文件内、排序去重，
  清单函数在已收集文件中缺失时同样失败关闭。
- F2：`_JobProcesses.collect` 仅在重复成功列表、等于 `ActiveProcesses`、缺口不超过已发信号的保留句柄
  数时接受 `assigned > listed`；回归测试位于 `tests/test_devx015_workflow_execution.py`。
- 已知风险：`test_real_16_worker_diagnostics_cross_runner_pipe_before_release_and_session_exit` 的 60 秒
  收尾看门狗在 16 worker 满载时可能超时（聚焦批次一次，空载 15.7 秒）；如 v14 Full 复现再评估。
- 重测时以实测 profile 重新校准 K 与分组；O3/P1/P4 仍待基线发布后。

### 2026-09-30 v14 Full 死锁与 v15 设计（Design F）

- 现象：v14（候选 `e71db2646`，清单 v2）Full 在约 18:26 JST 后无任何完成事件；此前"还需 1 到 2 小时"的判断
  错误——该次运行不是慢，而是控制器等待 19 个永不运行的节点。已以 `--recover-full-action
  terminate_frozen_job` 收尾（`full_incomplete_recovery.json`，事务 FAILED/RELEASED），全部证据保留。
- 根因（已用离线离散事件模拟复现）：v2 的跨 worker exclusive group 占用与 heavy 上限、xdist"worker 只在收到
  后继或 shutdown 后才运行队尾项"叠加成循环等待。`host_registry_view` 被 gw4 一个未开始的轻量队首占用；
  四个重型持有者 gw0–gw3 各持一个未开始的队首，其唯一可分配的后继都是被组阻塞的重型单元；其余 11 个
  worker 各持一个未开始的末位轻量项。没有任何 worker 能开始，因而没有完成事件去唤醒调度。
- Design F：每个 exclusive group 固定为**一个串行复合工作单元**（scope `exclusive-group::<name>`，成员按
  collection 顺序在同一 worker 上顺序运行，天然互斥）；复合单元为重型当且仅当任一成员为 `real_full_chain`；
  调度器删除跨 worker 组占用计数。持有重型单元的 worker 总能得到后继（轻量或重型），因此活性成立；
  非持有者在只剩重型单元且已达上限时只是等待某个重型完成，推迟的是轻量结果而不是 makespan。解析器
  拒绝一个函数同时属于两个 group（否则无法固定到单个复合单元）。
- 软启动（清单 v3 `heavy_start_interval_seconds`，pilot 基线）：v13/v14 在 t=0 同时启动 4–6 个重型链，
  重型内部 -n16 与每次 `acceptance_runtime_identity()`（约 16,554 文件 / 442 MB；空载约 11 秒，中载
  19–22 秒，重载 116–194 秒）叠加，触发固定的 180 秒 child-ready、120 秒 readiness 子进程、60 秒 fence CLI、
  360 秒 inspector 与 10 秒 `terminate` 超时；v11 重型并发在约 60 分钟内自然爬升 0→5，同类失败很少。
  规则：非持有 worker 仅当距上一次"非持有者取得重型单元"已过该间隔，或已无轻量工作时才取得重型单元
  （后者保证活性）。代价估算约 +0.25 小时。这是有评估证据但主观的启动策略，值 600 秒；重新校准条件：
  P1/P4/O3 之后的首个实测 profile。
- 预计墙钟：99 个链节点合计约 25.75（v11）/ 29.11（v13）小时，K=4 下界约 6.4–7.3 小时；K=3/2 分别约 9.7 /
  14.5 小时，不可行。因此 v15 正式 Full 预计 5–7 小时，之后仍需 P1/P4/O3 才能进一步降低。
- 测试：新增 fake-node 事件模拟属性测试（随机耗时与种子：无死锁、`max_heavy ≤ cap`、每个 group 全程串行、
  启动间隔生效）与真实 xdist 组互斥用例；`test_devx015_workflow_execution.py` 句柄计数用例在取基线前后
  `gc.collect()`（v14 出现 381≠383 抖动，非泄漏）。
- 限时 pilot（非正式验证，不产生发布证据）：在候选提交上，于独立 detached worktree
  `D:/Work/devx018-pilot-v15`（任务 DEVX-018；目的：在不占用主 checkout 的前提下观察新调度器首 75 分钟
  是否复现 v14 首小时失败簇；退出条件：分析后 `git worktree remove` 并 `git worktree prune`，
  证据摘要写回本节）运行 `pytest -n 16 --dist loadfile -p scripts.pytest_runtime_profile ... --no-loadscope-reorder
  --basetemp D:/Work/devx018-pilot-<n>`，约 75 分钟后终止。该运行为验证笔记中的显式例外，不替代 Full。
- 已接受的变通：暂无。固定超时与每调用 identity 哈希未改动；如 pilot 仍复现负载相关失败，再与 owner 讨论
  测试侧超时按负载校准（须记录原因、影响、风险、验证、退出条件，退出条件绑定 P1/P4/O3）或提前 O3
  （触及"不做跨调用 identity 缓存"的设计，须 owner 决定）。

### 2026-09-30 v15 实现进展

- 已实现：解析器 `exclusive_group_of` / `is_heavy_scope` / `heavy_start_interval_seconds`（默认 0，
  非负整数，布尔与字符串拒绝）、一个函数至多属于一个 group；调度器复合单元 scope
  `exclusive-group::<name>`、复合优先的重单元启动次序、软启动（注入式 clock，测试中用模拟时钟）；
  清单 v3（`heavy_start_interval_seconds: 600`）。split_scope 证据字段保持 10 个不变，间隔由清单 SHA-256 绑定。
- 验证：`tests/test_devx018_validation_scheduling.py` 57 项 parallel pytest 通过，含 fake-node 事件模拟属性
  测试（12 种子 x 间隔 0/600：无死锁、`max_heavy <= cap`、每 group 单 worker 串行、间隔生效）、v14 死锁形状回归、
  真实 xdist 组/重型组用例；对调度器做两次变异（去掉间隔、去掉上限）后测试均失败，说明测试有效。
  以 v11/v13 实测 profile 做离散事件模拟（K=4、16 worker）：6.8 / 7.63 小时，`max_heavy=4`，组无重叠。
- 待办：限时 pilot、正式 v15 Full；P1/P4/O3 与重测后再校准 K 与启动间隔。

### 2026-09-30 v15 pilot 证据与负载校准（超时校准，待 owner 复核）

- pilot 证据（非正式验证，`D:/Work/devx018-pilot-v15`，启动 20:33 JST，候选 `3d9379a56`；原型为
  `pytest -n 16 --dist loadfile` 全量，不含 git 忽略的 retained evidence，named-DQ/composer/simple_baseline
  三类测试因缺 `AITS_NAMED_DQ_*` 而失败，按 env 因素剔除）：新调度器在 16 worker 下未出现死锁形态，
  进度越过 v14 前 20%；但 v14 的负载失败簇复现：custody ×2（3%）、`test_publication_lifecycle_binds_original_actual_full`
  FAILED+ERROR（16%）、`test_original_publication_cli_recovers_independent_main_advance` FAILED+ERROR（20%）、
  `test_original_publication_cli_interrupted_after_main_commit` FAILED（23%）。原始 gc 句柄计数抖动未再出现。
  串行 lifecycle 复现曾于 21:24 启动、约 21:35 因 pilot 证据已足够而终止（日志为空），属被放弃的串行例外，
  不作为验证结论。
- 根因（v13 完整 traceback、v14 日志与 pilot 交叉确认）：三类固定期限均按空载主机校准，而每个重型链节点内部再跑
  嵌套 Full，约 16 个外层 worker + 4 个重型 × 内部 `-n16` 约 80 个进程争用 32 核，Defender 拖慢逐文件 I/O，
  `acceptance_runtime_identity()`（16,554 文件）空载约 11 秒、负载下 116–194 秒：
  (A) 生产侧 `FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS` / `..._PROTECTED_...` 为 360 秒（空载 59–75 秒）→
  `PUBLICATION_FULL_CLOSURE_INVALID`（ff_only）、publisher 到不了 `heads_switched`（race-window）；
  (B) 终止确认期限 10 秒：`TerminateJobObject` 之后 `_JobProcesses.exited()` 需要所有保留句柄变信号，
  观察到 `job_active_process_count: 0` 而句柄 wait_status 仍为 258 超过 10 秒 → custody、lifecycle
  （CLEANUP_UNCONFIRMED 与 `LEASE_EXECUTION_NOT_TERMINAL` teardown ERROR）、interrupted（`'ACTIVE' == 'EMPTY'`）；
  (C) 测试侧挂起探测器：`DEADLINE = 120`、custody readiness 180 秒、`_v03_fence_cli` 默认 60 秒、
  compat/authority/readiness 子进程 60/120 秒、显式 10 秒终止。
- 决定（记录为暂行变通，`PROVISIONAL_PENDING_OWNER_REVIEW`，owner 需在 v15 结果报告中复核）：
  把上述期限改为按负载校准的命名常量，成功路径行为不变，只延后"确认失败"的判定。
  - 生产：`CONTAINED_TERMINATION_CONFIRMATION_SECONDS = 180`（`workflow_execution.py`，`terminate` /
    `terminate_process` / `observe_job(terminate=True)` 默认值及 `task_checkpoint`、`workflow_coordination`、
    `workflow_integration` 中显式的 10/20 秒调用）；`FULL_PROFILE_INSPECTION_TIMEOUT_SECONDS` 与受保护 inspector
    上限 360 → 900 秒。
  - 测试：各测试文件的 hang-detector 上限同步放大并保持测试侧 CLI 上限严格大于生产内层 inspector 上限（嵌套约束）。
  - 原因：v13/v14/pilot 三次独立复现同一失败簇，且都有"期限到达时目标状态实际在推进"的证据；
    行为影响：仅在负载下延长失败判定时间，超期仍 fail closed（`CLEANUP_UNCONFIRMED`、
    `PUBLICATION_FULL_CLOSURE_INVALID`）；
    风险：真实挂起最多晚 180/540 秒被发现；仍有"新增固定期限被更大负载击穿"的边际风险；
    验证覆盖：受影响单测、`test_arch_005` / `test_devx015_*` 已知簇在并行负载下重跑、正式 v15 Full；
    退出条件：P1（identity/hook 重放去重）、P4（大 manifest 外置）与 O3（identity 缓存，需 owner 决定，
    触及"不做跨调用 identity 缓存"的设计）落地并重测后，用首个实测 profile 决定是否把常量降回；
    在此之前不得再放大任何期限而不回写本节。

### 2026-10-01 v15 pilot 结论与已知簇校准验证

- pilot 收尾（非正式验证，候选 `3d9379a56`、未含校准代码）：02:02 JST 停止，历时 5 小时 28 分，
  已完成 14,635/14,673（99.74%），PASSED 14,577 / FAILED 53 / ERROR 2，4 个在飞、34 个未启动。停止原因：剩余为
  串行尾部，且后续结果因未含校准而无信息量。失败归类：43 项为 pilot 环境因素（无 git 忽略的 retained evidence，
  named-DQ / composer / named_simple_baseline 缺 `AITS_NAMED_DQ_PUBLICATION_TRANSACTION` /
  `AITS_NAMED_DQ_SOURCE_LEASE_ID`，正式 worktree 不受影响）；1 项为 Atlas canonical 页面相对任务登记索引过期
  （`SEMANTIC_SOURCE_DRIFT`，正式 Full 的 `atlas-authority` 生成器会在最终 head 重建）；9 项为负载类
  （即上节 A/B/C 三类期限），由本次校准处理。
- 尾部观察（供重测/O4 使用，不在本次范围内处理）：新调度器约 1.5 小时到达 98%，其后约 4 小时为串行尾部，
  主机 CPU 仅 19–26%：关键路径是串行链长度而不是 CPU，主要是 `host_registry_view` 复合单元（15 个函数串行）与若干
  链节点。可行方向：读者/写者式锁使只有"断言整个 HKCU 视图"的测试独占，或为各测试提供隔离的 HKCU 根。
  据此把正式 Full 预期修订为约 6.5–8 小时（pilot 5.5 小时 + 约 38 个未完成尾部测试）。
- 已知簇校准验证：第一次簇运行（stock `--dist loadfile`、与 pilot 并发）无效——未启用治理调度器、与 pilot 争用
  主机全局资源（固定 Job 名、HKCU 测试根视图），吞吐 2.5 小时仅 10 项并出现一次无法归因的 `F`，已终止进程树并作废。
  干净重跑在 pilot 停止后执行：`-p scripts.pytest_runtime_profile -n 12 --dist loadfile`，对象为 v13/v14/pilot
  失败/抖动的 16 个负载类测试，结果 16/16 PASSED，用时 2:11:01。
- 证据边界（诚实披露）：该重跑只有 12 个 worker、16 个测试、主机相对安静，证明校准后的代码在已知失败形状下
  正确，但不是重负载通过证明；负载通过证明只能由正式 v15 Full 给出。pilot 未含校准代码，其后续结果不作为
  校准证据。另新增不变量测试
  `test_loaded_host_hang_guards_stay_above_the_production_inspection_bound`：测试侧 CLI 超时必须严格大于生产内层
  inspector 上限，防止测试侧超时先于生产超时触发而掩盖 fail-closed 诊断。
- 临时工作区生命周期（owner：Claude Code coordinator；退出条件：v15 正式 Full 结果落定并把所需证据归入规范位置后，
  逐一审计再清理）：
  - `D:/Work/devx018-pilot-v15`（detached `3d9379a56`，约 15 GB，pilot worktree；清理前先 `git -C` 审计状态，
    再 `git worktree remove`）与其 basetemp `D:/Work/devx018-pilot-v15-tmp`；pilot 日志
    `D:/Work/devx018-pilot-v15-out*.log` 与 `-err.log` 保留为证据；
  - `D:/Work/devx018-custody-repro`、`D:/Work/devx018-lifecycle-repro`（及其 `.log`）、
    `D:/Work/devx018-cluster-tmp`、`D:/Work/devx018-cluster-tmp2`、`D:/Work/devx018-atlas-tmp`：均为单次复现/簇验证
    的 basetemp，可再生；
  - 不删除 `D:/Work/devx015-*` 证据目录与 5 个遗留 HKCU `AITS-DEVX015-Test-*` 根（owner 清理事项）。
- 下一步：提交本校准与文档，做一轮 reseal，然后以 `failure_fix_rerun` 事务在最终 head 上启动正式 v15 Full；
  P1/P4/O3 与重测（重新校准 K、启动间隔、上述超时常量与串行尾部的锁粒度）在基线发布之后进行。

### 2026-10-01 v15 Full 结果与 v16 修复

- v15 正式 Full（候选 `97e73769cc83`，事务 `gov-007-p1c-devx015-baseline-publication-20260930-v15`，
  `failure_fix_rerun`，Design F 调度 + 负载校准）：20:35 JST 启动，历时 7 小时 23 分（26,601 秒），
  `14664 passed, 6 failed, 4 skipped`，对比 v11 的 10.15 小时；校准后的三类期限在重负载下全部成立
  （无 `PUBLICATION_FULL_CLOSURE_INVALID`、无 `CLEANUP_UNCONFIRMED`、无终止确认超时、无生产侧 hang-detector
  触发）。v15 事务已作为 FAILED 终态证据释放（证据：事务目录与 `outputs/validation_runtime/gov-007-p1c-devx015-full-20260930-v15/`）。
- 6 个失败全部是校准提交（`3195f99b8`）自身的覆盖缺口，不是生产行为回归：
  - 5 项 `NameError`：`tests/test_devx015_workflow_coordination.py` 里有两处**字符串内嵌源码**——native
    registry 子进程 driver（`r"""..."""`）与 M06 注入的 `ExecutionLifecycle.recover` 方法源码——校准时被整体替换成了
    模块常量 `LOADED_HOST_WAIT_TIMEOUT_SECONDS`，而子进程/注入作用域看不到该模块全局名。影响节点：
    `test_native_registered_guard_competition[linked-publication|linked-full|repositories-full]`、
    `test_m04_native_per_checkout_store_hits_original_shared_store_assertion[original]`、
    `test_m06_actual_redispatch_hits_original_recovery_assertion[M06]`。
  - 1 项 `subprocess.TimeoutExpired`：`test_m05_actual_pytest_failure_mutant_hits_original_exit_assertion[failure-mutant]`
    启动嵌套 `pytest -n 2 --dist loadfile`，测试侧 90 秒期限是校准遗漏。
- 为什么 pilot 与已知簇验证没有发现：这些全是 `real_full_chain` 重型节点，O2 把它们排除在 pre-Full tier 之外
  （Full 为唯一权威），pilot 因缺 `AITS_NAMED_DQ_*` 环境而剔除了同类节点，已知簇重跑只覆盖了 16 个负载类失败节点。
  结论：**改动重型测试后，pre-Full tier 不会执行被改动的节点**，必须在正式 Full 前对受影响重型节点做定向 smoke
  （即 O5 的前移：失败节点 + 受影响重型节点，`-n 2`，不与其他 pytest 会话并发）。
- v16 修复（测试侧，生产代码与清单不变）：
  - 内嵌源码的常量以占位符 `__LOADED_HOST_WAIT_TIMEOUT_SECONDS__` 写入，由
    `_embed_loaded_host_wait()` 在使用前把值代入文本（单一常量，不复制字面量；占位符缺失时断言失败）；
  - `test_m05` 嵌套 pytest 的 `timeout=90` → `LOADED_HOST_CLI_TIMEOUT_SECONDS`（1800 秒）；同文件另两处启动
    Python 子进程的短期限 15 / 20 秒 → `DEADLINE`（600 秒），均只放宽"确认失败"的判定，成功路径不变；
  - 新增不变量测试 `test_loaded_host_guard_names_are_not_referenced_inside_embedded_sources`：对三个测试文件的
    所有字符串常量做 AST 扫描，裸 `LOADED_HOST_*_TIMEOUT_SECONDS` / `DEADLINE` 引用即失败（对 v15 候选的文件会报
    2 处，修复后为 0，已验证检测器有效）。新增函数在既有文件内，不触发 WAVE21 清单变更。
- 已知残余（诚实披露，不在本次修复内）：`test_devx015_workflow_execution.py` 中仍有若干 10–60 秒测试侧短期限
  （git `cat-file` 的 30 秒、`-c pass` 子进程的 `wait_exit(timeout=30)`、60 秒 release 等待等），它们在 v15 的
  重负载下通过，因此保持原值；若 v16 再有同类超时，它们是首批候选。校准整体仍为
  `PROVISIONAL_PENDING_OWNER_REVIEW`，O3 identity 缓存仍待 owner 决定。
- v16 验证计划：先重跑 6 个失败节点与其余本文件内嵌源码节点（`-p scripts.pytest_runtime_profile -n 2`，
  串行化由调度清单保证，不与其它 pytest 会话并发）、新增不变量测试与 `test_arch_005` 相关单测；
  再按既定序走 reseal（test manifest、compat authority）→ `failure_fix_rerun` 事务绑定 v15 失败 Full
  → 正式 v16 Full（预期 7–8 小时）。
- 临时工作区生命周期补充：`D:/Work/devx018-focus-tmp`（v15 前聚焦重跑的 basetemp，可再生，owner：Claude Code
  coordinator，退出条件同上）加入清理清单；v15 Full 保留的失败测试 tmp 目录
  `C:/Users/32739/AppData/Local/Temp/pytest-of-JACK/pytest-21009/` 仅作诊断，已用完则随 pytest 自身轮转清理。
- v16 修复后验证（2026-10-01，空闲主机，`-n 2 --dist loadfile`，`--basetemp D:/Work/devx018-smoke-v16`）：
  - 定向重型节点 smoke 58 通过 / 0 失败（501.99 秒）：覆盖 6 个 v15 失败节点（含 `native_registered_guard_competition`
    三个参数、M04/M06 原始断言、`m05` 的 4 个参数）、15/20 秒探针的调用者（hardlink 只读保管、独立文件保管探针系列）以及两个
    不变量测试；`m05[pass-original]` 在空闲主机上单节点即 480.70 秒（嵌套完整 pytest），记入后续剩余时间预算。
  - 影响面单测 68 通过 / 0 失败（55.35 秒，`-n 16 --dist loadfile`）：`test_devx018_validation_scheduling.py`、
    `test_arch_004g_deprecation.py`、`test_arch_005_integration_publication_fence.py` 中两个 loaded-host 不变量测试。
    `test_arch_005_integration_publication_fence.py` 全文件未在此处重跑：本次改动只新增一个测试与导入，生产代码未变，
    且该文件在 Full 中累计约 43,192 秒（`--dist loadfile` 下单 worker 串行，不适合作为聚焦验证）。
  - 一次聚焦运行因误含该文件全量而被终止（我方进程树，运行约 30 分钟、无失败），HKCU 测试根仍为既有 5 个，无新增泄漏。
  - 本次 smoke 的临时目录 `D:/Work/devx018-smoke-v16`、`D:/Work/devx018-focused-v16`（basetemp，可再生，owner：
    Claude Code coordinator，退出条件同上）加入清理清单。

### 2026-10-01 v16 Full 结果与 v17 修复（发布阶段 parent 绑定缺陷）

- v16 正式 Full（候选 `8cf97a9d7eef`，事务 `gov-007-p1c-devx015-baseline-publication-20261001-v16`，
  `failure_fix_rerun`，绑定 v15 失败 Full 的 `test_runtime_summary.json` 为 `full_parent`）：
  `14671 passed, 4 skipped, 0 failed`，历时 7 小时 15 分 10 秒，退出码 0；v15 的 6 个失败全部消除，校准后的期限在
  重负载下成立。Full 产物保留在 `outputs/validation_runtime/gov-007-p1c-devx015-full-20261001-v16/`。
- 但发布被阻断：`checkpoint --phase LOCAL_MAIN_FF_PRE` 以
  `PUBLICATION_FULL_CLOSURE_INVALID: Full publication profile rejected: PUBLICATION_FULL_PARENT_MISMATCH: missing-side`
  拒绝，事务状态保持在 `FORMAL_VALIDATION_RESULT`（PASS），没有任何 local-main / remote 变更。
- 根因（设计缺陷，非环境问题；与 DEVX-010 同类，该任务只修复了派发侧）：
  - `IntegrationPublicationFence.validate(validation_tier=<heavyweight>)` 在 `parent_path is None` 且事务记录了
    `full_parent` 时按 `missing-side` 拒绝。派发侧（`run_validation_tier.py` 两处 `_FullCommandRunner` /
    dispatch）会用 `_publication_parent_path(provenance)` 显式传入 parent，因此 v16 的 `FULL_DISPATCHED` 通过；
  - 发布侧有两处没有传入 parent：① `inspect_full_publication_profile` 起始处的 `fence.validate(...)`；
    ② fence 自身 `_prepare_current_full_profile` 在 `LOCAL_MAIN_FF_PRE` 阶段的探测（`validation_tier=heavy`）。
    任何带 `full_parent` 的 `failure_fix_rerun` 事务因此都无法通过 `FORMAL_VALIDATION_RESULT → LOCAL_MAIN_FF_PRE`
    及其后的 profile 校验；
  - 既有测试只覆盖「无 parent 的 Full 发布」（`test_unbound_full_profile_admits_non_devx015_publication`）与
    「parent 仅被显式消费」（`test_bound_full_parent_is_optional_until_explicitly_consumed`），从未让带 parent 的事务
    走完发布阶段，所以 v13–v16 都没能提前发现。
- 为什么 v16 的 PASS 不能直接复用：修复必然改变候选提交，Full 结果绑定 `candidate_sha`、事务 sha 与租约，旧结果不能
  迁移到新候选；这是 fail-closed 设计，不绕过。代价是再跑一次正式 Full（约 7–8 小时）。
- v17 修复设计（先记录、后实施；生产行为只收紧检查，不放宽）：
  1. `IntegrationPublicationFence.validate` 新增显式参数 `verify_recorded_parent`（默认 `False`，现有调用方行为
     不变）：为真且调用方未传 `parent_path` 时，用事务已记录的 `full_parent.path` 做同一套 exact-byte
     （path/sha256/size）比对，因此文件被篡改、删除或替换仍然 fail closed；传了 `parent_path` 时仍以调用方为准
     （`missing-side` 对「调用方传了 parent 而事务没有」继续成立）。
  2. `_prepare_current_full_profile` 在 `LOCAL_MAIN_FF_PRE` 的探测传入 `verify_recorded_parent=True`；
     `inspect_full_publication_profile` 起始 validate 同样传入。
  3. `inspect_full_publication_profile` 读取原 PASS Full summary 的 `validation_provenance` 后，新增交叉校验：
     用与派发侧相同的 `_publication_parent_path(provenance)` 得到 parent，再用同一 fence validate（显式 `parent_path`）
     与 `_require_new_incomplete_parent_candidate` 复核；即「原 Full 声明的 parent」必须与「事务冻结的 parent」逐字节一致，
     不一致（含 Full 声明了 parent 而事务没有、或反之）一律拒绝。
  4. 测试：fence 层单测（带记录 parent 的事务：不带标志仍 `missing-side`、带标志通过、parent 被篡改后拒绝、事务无
     parent 时标志无影响）；inspector 交叉校验的单元测试；以及真实链路演练
     `test_full_profile_with_bound_parent_admits_failure_fix_rerun_publication`（复用
     `_run_actual_profile_full` 的真实 canonical/Job/profile 链，事务带 `full_parent`、provenance 为 `failure_fix_rerun`，
     跑到 `LOCAL_MAIN_FF_PRE` 之前的 inspector 与 checkpoint 探测）——目的就是避免第三次 7–8 小时才暴露同类缺陷。
- 验收：上述测试通过；预 Full 的四个前置 tier 与重型演练节点 smoke 通过；新事务（`failure_fix_rerun`，绑定 v16 Full
  summary 为 `full_parent`）的正式 Full 通过并成功走完 `LOCAL_MAIN_FF_PRE → local-publish → REMOTE_PUSH_PRE →
  普通 push → CLEANUP_PRE → RELEASED`；`local main = remote main = candidate`。
- 状态/风险：校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`；O3 identity 缓存待 owner 决定；本修复不改变任何投资
  解释、评分、回测或数据路径，`production_effect=none`。
- 临时工作区：本段无新增；v15/v16 的清单沿用上节（发布完成后统一审计清理）。v16 事务已作为 FAILED 终态证据释放
  （证据文件 `outputs/architecture/integration_revalidation/devx015-v389/claude_v16_local_main_ff_pre.json`，
  sha256 `67be89a4a6cc29422c2817ccaa88298e1763acbb081be758903ed71f1bcb4174`）。

### 2026-10-02 v17 Full 提前终止与 v18 修复（内嵌固定期限）

- v17（候选 `597fe2bdb`，事务 `gov-007-p1c-devx015-baseline-publication-20261002-v17`，`failure_fix_rerun`，
  绑定 v15 失败 Full summary 为 `full_parent`）：四个前置 tier 与 architecture-fitness 均通过（architecture-fitness 在
  01:26 因宿主死机重启被中断，未派发 Full，事务仍在 `FORMAL_VALIDATION_PRE`、租约 ACTIVE、HEAD 未变，审计干净；
  随后用同一事务的恢复驱动重跑 architecture-fitness 并通过，再派发 Full。中断日志保留为
  `claude_v17_validation_architecture-fitness.interrupted-reboot.log`，说明见 `claude_v17_reboot_interruption.md`）。
- Full 于 02:17 TST 派发，02:24 TST 出现第一个失败节点
  `tests/test_devx015_workflow_execution.py::test_complete_runtime_native_custody_inherited_and_released[success]`
  （worker gw1）：`TimeoutError: inherited direct child has not exited`（测试内嵌子进程脚本的
  `child.wait_exit(timeout=30)`）。保留目录 `pytest-21247/popen-gw1/test_complete_runtime_native_c0` 显示子进程在释放后
  约 80 ms 就写出 `runtime-child.json`（说明子进程正常运行），但它继承了 84,927 个句柄（16,554 个运行时文件托管句柄的继承），
  进程销毁（关闭这些句柄）在 Full 负载下超过 30 秒。这与 v15/v16 已校准的「负载下的固定期限」同类，只是该处是写在
  内嵌 Python 源码字符串里的字面量，v15 的批量替换按设计跳过了内嵌源码（避免 NameError），因此漏校准；v16 同一节点通过只是
  时序幸运。
- 一个失败节点即令本次 Full 必然 FAIL（pytest 非零退出），继续跑完约 7 小时不会改变结论，且主工作树在 Full 期间不得修改，
  无法并行开发。处置：停止我自己启动的驱动进程树，再用既有恢复路径
  `run_validation_tier.py full --recover-full --recover-full-action terminate_frozen_job` 收尾；事务以 `FAILED`、
  `full_incomplete_recovery.json`（`FULL_VALIDATION_COMMITMENT_MISSING`、技术状态 INSUFFICIENT）终态释放，租约
  `RELEASED`，不伪造 pytest FAIL。该文件将作为 v18 `failure_fix_rerun` 的 `full_parent`（新候选，非复用）。
  终止说明见 `claude_v17_full_abort_note.md`。
- v18 修复范围（类修复，不逐点打补丁；不改生产代码）：
  1. 测试内嵌子进程源码中的固定亚分钟期限统一改为已校准的 `LOADED_HOST_WAIT_TIMEOUT_SECONDS`（600 秒，沿用
     coordination 测试的同名常量与占位符 `__LOADED_HOST_WAIT_TIMEOUT_SECONDS__` 替换机制，避免内嵌源码的 NameError）：
     `wait_exit(timeout=30)`、等待释放文件的 `time.monotonic()+60/+20` 循环，涉及
     `tests/test_devx015_workflow_execution.py` 与 `tests/test_arch_005_integration_publication_fence.py` 的内嵌源码；
  2. 新增不变量测试：上述测试文件的内嵌源码字符串中不得再出现小于 600 的 `wait_exit(timeout=<n>)`
     与 `monotonic()+<n>` 字面量（避免同类漏校准再次只在 7–8 小时的正式 Full 才暴露）；
  3. 预 Full 的 O2 排除不覆盖这些重节点，所以实施后先做重节点 smoke
     （`-p scripts.pytest_runtime_profile -n 2 --dist loadfile`，不与其他 pytest 并发），并补一个 recovery 型 parent 的
     发布阶段交叉校验单测（v18 的 `full_parent` 是 `full_incomplete_recovery.json`，此前真实发布阶段只走过 summary 型）。
- 验收：不变量测试与 smoke 通过；reseal 后新候选的前置 tiers 与正式 Full 通过，并成功走完
  `LOCAL_MAIN_FF_PRE → local-publish → REMOTE_PUSH_PRE → 普通 push → CLEANUP_PRE → RELEASED`，`local main = remote main = candidate`。
- 状态/风险：超时校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`（本次新增的内嵌期限同属该临时校准，退出条件不变）；O3 identity
  缓存待 owner 决定；继承 8.5 万句柄导致子进程销毁耗时随负载放大，属 DEVX-015 v3 文件托管设计的已知代价，本次不改生产行为，
  留待 O3/后续重测评估。本修复不改变任何投资解释、评分、回测或数据路径，`production_effect=none`。
- 临时工作区：本段无新增（`D:/Work/devx018-focus17*`、`-smoke17`、`-rehearsal17`、`-fence17*` 等沿用上节清单，发布完成后统一
  审计清理）。v17 事务已作为 FAILED 终态证据释放（`full_incomplete_recovery.json` sha256
  `020957685c938ae7f2bdb56fa504c3b891b25b737619a9115acae33e44335d41`）。

### 2026-10-02 v18 Full 通过与 v19 修复（发布 worker 的 16 MiB 读取预算）

- v18（候选 `a77a96e0a`，事务 `gov-007-p1c-devx015-baseline-publication-20261002-v18`，`failure_fix_rerun`，绑定 v17 的
  `full_incomplete_recovery.json` 为 `full_parent`）：五个前置 tier 全部通过；正式 Full 技术上 PASS，
  `14677 passed, 4 skipped, 0 failed`，pytest 历时 7:04:00（驱动 25590 秒，退出码 0），v17 的内嵌固定期限修复在重负载下成立。
  `LOCAL_MAIN_FF_PRE` checkpoint 通过，即 v17 的「记录 parent」修复在真实带 parent 的事务上得到验证。
- 但 `local-publish`（原始发布 worker Job）在 `PUBLICATION_STAGE hooks_created` 之后、`ready_held` 之前以
  `WORKFLOW_ARTIFACT_BUDGET` 被阻断（worker 结果 `FAIL`，CLI 退出码 2，`RECOVERY_REQUIRED`），本地 main/索引**未变**
  （`main = origin/main = cbc31cdf`）。已用既有恢复路径 `local-publication-recover` 收尾
  （`STABLE_FAILED_ATTEMPT`、`ORIGINAL_UNCHANGED`），事务以 FAILED 终态释放，租约 RELEASED；证据
  `claude_v18_local_publish.json`、`claude_v18_local_publication_recover.json`，worker stdout/result 保留在
  `outputs/validation_runtime/gov-007-p1c-devx015-full-20261002-v18/publication-*.{stdout,result.json}`。
- 根因（既存缺陷，非环境问题）：`PublicationLifecycle.hold_hook_capsule_inputs`
  （`workflow_coordination.py`）对 profile 检查所捕获的每个证据文件用默认预算读取并托管
  （`bounded_regular_bytes(path)`、`hold_bound_read_file(...)` 默认 16 MiB），而真实 Full 的
  `test_runtime_profile.json` 为 27,370,306 字节（v15 27,363,065、v16 27,360,659；O1 逐节点 profile 含约 1.47 万节点
  及各 phase 记录）。profile 检查器本身已使用 `FULL_PROFILE_EVIDENCE_BUDGET_BYTES`（256 MiB），托管上限为 64 MiB，只有
  hook 输入托管这一处仍是默认 16 MiB。此前没有任何一次真实 Full 走到 `local-publish`（v13–v15 Full 失败，v16 死于
  `LOCAL_MAIN_FF_PRE` 的 parent 缺陷），而发布测试夹具的 profile 只有几十 KB，所以该缺陷一直潜伏。
- 为什么必须再跑 Full：修复要改 `src/` 代码，候选必变，Full 结果绑定候选 sha、事务与租约，旧 PASS 不可复用（fail-closed）。
  代价约 7–8 小时。为避免「每修一个潜伏缺陷就再等一轮 7 小时」，本轮把真实规模的发布阶段先在夹具里完整演练。
- v19 修复设计（先记录、后实施；只放宽到既有的托管上限，不改变任何校验语义）：
  1. 在 hook 输入托管路径为捕获文件使用显式、命名的预算：读取用 `FULL_PROFILE_EVIDENCE_BUDGET_BYTES` 的同源语义，
     托管用 `hold_bound_read_file` 的 64 MiB 上限；超过上限时 fail closed 并给出明确错误码，而不是笼统的
     `ARTIFACT_BUDGET`。同类位置（`prepare_git_launch` 等）逐一核对，只改确实可能触及 profile 的读取。
  2. 新增大 profile 发布演练：夹具模式 `large-full-profile-publish`（与 `full-profile-publish` 相同，内层 Full 的测试文件
     额外参数化大量节点，使 `test_runtime_profile.json` 超过 16 MiB），端到端测试走真实
     `local-publish → hooks → ff-only → adoption → recover`，先在未修复代码上复现 `WORKFLOW_ARTIFACT_BUDGET`，
     修复后通过；演练暴露的下游缺陷（hooks、git launch、adoption）在启动 Full 之前一并修复。
  3. profile 增长余量：27.4 MB 对 64 MiB 托管上限约 2.4 倍余量，节点数增至约 3.4 万会再次触顶；本次不改 profile 结构，
     登记为后续任务（评估精简逐节点 phase 记录或分层存储），并让超限错误可读。
  4. v19 的 `full_parent` 仍用 v17 `full_incomplete_recovery.json`：v18 的 Full 是 PASS，不满足 parent 必须是失败结果的
     约束；该 parent 对新候选并非复用（候选、事务、租约均不同）。
- 验收：大 profile 演练在修复前红、修复后绿；既有发布夹具与 smoke 通过；reseal 后新候选的前置 tiers 与正式 Full 通过，
  并走完 `LOCAL_MAIN_FF_PRE → local-publish → REMOTE_PUSH_PRE → 普通 push → CLEANUP_PRE → RELEASED`，
  `local main = remote main = candidate`。
- 状态/风险：超时校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`；O3 identity 缓存待 owner 决定；本修复不改变任何投资解释、
  评分、回测或数据路径，`production_effect=none`。
- 临时工作区：沿用上节清单；本节新增 `D:/Work/devx018-smoke18`、`-unit18*`（发布完成后统一审计清理）。
  v18 事务已作为 FAILED 终态证据释放。

#### v19 实现与演练结果（2026-10-02）

- 实现（只放宽到既有托管上限，校验语义不变）：`workflow_coordination.py` 新增命名常量
  `PUBLICATION_CAPTURE_BUDGET_BYTES`（64 MiB，读取器硬上限），`hold_hook_capsule_inputs` 对 profile 检查捕获文件使用该预算读取并托管；
  hook ready 记录校验对「捕获文件行」的大小上限由 16 MiB 同步为该常量，其余非捕获行（创建的 hook 文件、事务与策略）仍为 16 MiB。
  超过 64 MiB 的捕获文件仍 fail closed（`PUBLICATION_HOOK_READY_FILE`）。运行时代码文件原本就是 64 MiB，未改。
- 大 profile 发布演练：夹具模式 `large-full-profile-publish`（内层 Full 测试模板追加 160 个参数化节点，复杂度可忽略），
  `_run_actual_profile_full` 按模板自动列出 padding 节点以满足完整耗时 profile 的集合/顺序校验；新增重节点
  `test_full_transaction_closeout_admits_full_profile_above_default_read_budget`（真实 local-publish → hooks → ff-only →
  adoption → 远端 push → closeout receipt，并断言 profile 大于 16 MiB），已登记到调度清单（版本 4，重节点）。
  修复前：同一夹具（20,304,612 字节 profile）复现与生产完全一致的 `PUBLICATION_STAGE hooks_created` 后
  `WORKFLOW_ARTIFACT_BUDGET`；修复后全链路通过（15 分 49 秒，空闲主机）。`test_publication_ready_binding_shape` 的
  `file-budget` 改为钉住非捕获行的 16 MiB 上限，并新增 `capture-budget` 钉住捕获行 64 MiB 上限。
- 聚焦回归：`tests/test_validation_tier_script.py`、`test_arch_005_integration_publication_fence.py`、
  `test_devx015_workflow_integration.py`、`test_devx018_validation_scheduling.py`、
  `test_arch_005_s5_task_source_cutover.py`（排除 real_full_chain）813 passed。
- 仍待验证：新候选的前置 tiers 与正式 Full（含新增重节点），以及真实 27.4 MB profile 的发布（夹具为 20.3 MB）。

### 2026-10-02 v19 Full 提前终止与 v20 修复（句柄编号巧合的测试）

- v19（候选 `d2d7fb67a`，事务 `gov-007-p1c-devx015-baseline-publication-20261002-v19`，`failure_fix_rerun`，绑定 v17
  `full_incomplete_recovery.json`）：五个前置 tier 全部通过；正式 Full 于 13:33 TST 派发，约 4 分钟后出现第一个失败节点
  `tests/test_devx015_workflow_execution.py::test_nonallowlisted_inheritable_event_handle_does_not_reach_child`（gw9）：
  `assert {'result': 1, 'error': 0} == {'result': 0, 'error': 6}`。
- 根因（测试设计缺陷，非产品缺陷）：用例在父进程创建可继承 Event，让子进程对「该 Event 在父进程中的句柄数值」调用 `SetEvent`
  并要求失败（ERROR_INVALID_HANDLE）。句柄数值只在进程内有意义；子进程恰好在同一数值上持有自己的某个 Event 时，调用会成功。
  同一用例里真正的泄漏检查 `WaitForSingleObject(event, 0) == 258`（父进程 Event 未被置位）已通过，说明没有句柄泄漏。
  它之前在 v16/v18 通过只是句柄编号运气，worker 的句柄编号随调度顺序而变。
- 一个失败节点即令本次 Full 必然 FAIL；与 v17 相同处置：停止驱动树，用既有恢复路径
  `run_validation_tier.py full --recover-full --recover-full-action terminate_frozen_job` 以 `FAILED` /
  `full_incomplete_recovery.json`（sha256 `279fdf3f3edaa2fe0c76e31d41b7d8c413a2caea27e7a3b33edf835fe35af6dd`，INSUFFICIENT）
  终态收尾，租约 RELEASED，main/origin/main 未变；说明见 `claude_v19_full_abort_note.md`。
- v20 修复（只改该测试，不改生产代码）：事件改为具名 Event；子进程用 `OpenEventW` 按名字打开自己的句柄，再用
  `CompareObjectHandles(父进程句柄数值, 自己的句柄)` 判断是否同一内核对象：被继承则为真，无效句柄或碰巧同数值的其他对象为假，
  与句柄编号无关。保留父进程侧的 `stdout` 显式重定向断言。并用对照实验（`subprocess.Popen(close_fds=False)` 让子进程真实继承）
  证明新探针能发现真正的泄漏，避免测试变成永远通过。同时全量审计其余用例是否有同类「按数值探测句柄」的写法。
- v19 同候选还验证了前置 tier 的稳定性；v19 的 Full 因被提前终止，发布路径（含 27.4 MB profile 的真实发布）仍待下一次 Full 通过后验证。
- 验收：新探针对照实验能检出继承泄漏、在 gw 重负载与空闲下都稳定；reseal 后新候选（parent 为 v19 recovery，因为 v19 是最近一次
  失败的 Full）的前置 tiers 与正式 Full 通过，并走完 `LOCAL_MAIN_FF_PRE → local-publish → REMOTE_PUSH_PRE → 普通 push →
  CLEANUP_PRE → RELEASED`，`local main = remote main = candidate`。
- 状态/风险：超时校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`；O3 identity 缓存待 owner 决定；`production_effect=none`。

### 2026-10-02 v20 Full 结果与 v21 修复（作业进程列表收集器没有稳定窗口）

- v20（候选 `eb17c78d5`，事务 `gov-007-p1c-devx015-baseline-publication-20261002-v20`，`failure_fix_rerun`，绑定 v19 的
  `full_incomplete_recovery.json`）：五个前置 tier 通过；正式 Full 跑完 `14679 passed, 1 failed, 4 skipped`，pytest 7:24:38
  （驱动 26838 秒，退出码 1）。v20 的 test-only 修复在重负载下成立；前 5.7 小时无失败，唯一失败出现在重型尾部：
  `tests/test_devx015_workflow_coordination.py::test_readiness_input_replaced_after_real_inspection_cannot_publish[full-profile-retained]`（gw0）。
  事务按 `FORMAL_VALIDATION_RESULT(FAIL)` → `release --outcome failed` 释放（证据 `test_runtime_summary.json`，sha256
  `dddf472b3016cf98df69972a87df01def1b9788568b2d2c57f29188fe8b45ad7`），main/origin/main 未变。该摘要（status FAIL、
  exit_code 1）是 v21 `failure_fix_rerun` 的合法 `full_parent`。
- 直接原因（来自保留现场与最终 pytest 报告）：夹具内的内层 Full 启动 3 分 10 秒后被 `terminate()`（退出码 1067）强制终止，
  因为外层运行器的 `_run_leased_command → WindowsJobProcess.wait → _JobProcesses.collect()` 抛出
  `WORKFLOW_EXECUTION_JOB_PROCESS_LIST_BUDGET`：`active_process_count=36`，查询序列
  `assigned=37, count=36, ok=true` 反复出现，`retained_process_count=0`，容量从 16 翻倍到 37888 的全部重试在不到 1 毫秒内用完。
- 根因（既存设计缺口，非环境问题；与 GOV-007 F2 同一函数）：`collect()` 把「已分配 > 已列出」视为短列表，只有当差值被**本实例保留的
  已退出句柄**解释时才接受（F2）。`wait(timeout=0.25)` 每次调用新建实例，起始没有任何保留句柄；一旦出现瞬时差值
  （例如某个成员正处于退出过程中、活动计数已减一而分配计数未减，重负载下该窗口可被拉长），收集器**不等待**就连续重查并在
  1 毫秒内放弃，进而杀掉整个 Job。同一收集器还以 0.25 秒间隔轮询正式 Full 自己的 Job 达 7 小时，因此同一缺陷也可能直接杀掉正式 Full；
  v16、v18 只是没有撞上。估计每次 Full 约三分之一的失败概率（3 次 Full 中 1 次），不能靠重跑碰运气。
- 复现尝试（空闲主机，原生 Job）：不断创建并退出子进程并以每次新实例紧密轮询 253,511 次、叠加 56 个 CPU 烧机进程 82,211 次、
  并发长寿收集器 51,518 次、外部观察者对每个成员保留句柄后观察 assigned/listed/active，均**未**出现持续的 assigned>listed
  （本机上退出成员即使被句柄持有也很快从列表消失）。因此差值是极少见的瞬时状态，无法在本机确定性复现；v13 的差值由保留句柄解释，
  v20 的差值来源未能确证。这一点如实记录，不把推测当作结论。
- v21 修复设计（最小政策变更；不放宽任何判定）：
  1. 在 `_JobProcesses.collect()` 里，对「`ok` 但未被解释的短列表」的连续重查之间加有界退避等待（1、2、4……1024 毫秒，总窗口约 2 秒），
     容量增长（`ok=false/234`）路径不加等待，所以普通轮询无额外延迟；
  2. 保持 13 次查询结构、容量序列、F2 接受规则（重复且等于 ActiveProcesses 且差值被保留句柄解释）、诊断载荷字段不变，
     窗口耗尽后仍以同一错误 fail closed；
  3. 等待用可注入的 sleep，避免测试变慢；新增确定性假 API 测试：瞬时差值在若干次查询后消失则成功并记录退避序列、持续差值在窗口耗尽后仍失败、
     容量增长路径不睡眠；原有 `short-success`/F2 用例改为注入 sleep。
  4. 不做的事（需 owner 决策，记为选项）：把接受条件放宽为「已列出 == ActiveProcesses 且稳定」。该规则被现有测试刻意钉为 fail-closed
     （`active=1` 且无保留句柄仍失败），放宽等于改变既定的容器安全判定，未经 owner 评审不实施。
  5. 风险：若真实差值持续超过 2 秒，该失败仍会复发；因此 v21 通过不构成该缺陷已消除的证明，复发时会再拿到同样的诊断载荷，
     再升级为上面的选项。
- 验收：新单测通过且原钉住的 fail-closed 语义仍成立；对受影响重型节点做 smoke；reseal 后新候选（parent 为 v20 的失败摘要）
  的前置 tiers 与正式 Full 通过，并走完 `LOCAL_MAIN_FF_PRE → local-publish → REMOTE_PUSH_PRE → 普通 push → CLEANUP_PRE → RELEASED`，
  `local main = remote main = candidate`。
- 状态/风险：超时与等待校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`；本修复新增的退避窗口属同一类工程边界（非投资启发式），
  同样待 owner 复核；O3 identity 缓存仍待 owner 决定；`production_effect=none`。
- 关联：DEVX-020 已于 2026-10-02 登记（c23fef466），排查在基线发布后的安静主机上进行。

### 2026-10-03 v21 Full 通过与 v22 修复（hook ready 入口脚本未进入非 DEVX-015 任务的捕获清单）

- v21（候选 `ee20d0647`，事务 `gov-007-p1c-devx015-baseline-publication-20261003-v21`，`failure_fix_rerun`，绑定 v20 的失败摘要为
  `full_parent`）：五个前置 tier 通过；正式 Full 技术上 PASS：`14683 passed, 4 skipped, 0 failed`，pytest 7:21:37（驱动 26647 秒，
  退出码 0）；v21 的 Job 进程列表退避修复期间无复发。`LOCAL_MAIN_FF_PRE` 通过。v19 的 16 MiB 预算修复在真实 27.4 MB profile 上得到验证：
  发布 worker 越过了此前被拦住的 hook 输入读取/托管阶段。
- 发布仍被拦住：`local-publish`（原始发布 worker Job）在 `PUBLICATION_STAGE hooks_created` 之后以
  `LEASE_EXECUTION_PUBLICATION_HOOK_READY_ENTRYPOINT: execution lifecycle rejected` 失败（CLI 退出码 2，`RECOVERY_REQUIRED`），
  本地 main/索引**未变**（`main = origin/main = cbc31cdf`）。已用 `local-publication-recover` 收尾（`STABLE_FAILED_ATTEMPT`、
  `ORIGINAL_UNCHANGED`），事务 FAILED 释放，租约 RELEASED；v21 Full 摘要 sha256
  `a67b6559952c05d6baf09b532593958e5f25fc89c6c31c3900700f7e5df5dd8a`（PASS，不能作 parent）。
- 根因（既存设计缺口；与 v18 的预算缺陷同类：真实非 DEVX-015 任务第一次走到发布 worker 的下一阶段）：
  `_validate_hook_capsule` 要求 hook 定义的 `entrypoint_path`（`scripts/architecture_arch005_publication_fence.py`）必须出现在 profile 检查的
  捕获清单（`profile_inspection.captures`）中，使入口脚本的字节被哈希绑定。现场核对：该入口路径不在 v21 的 1,375 个捕获项里（精确与
  忽略大小写/分隔符均不在）。检查器 `inspect_full_publication_profile` 的固定清单只含 `run_validation_tier.py`、
  `validation_readiness.py`、`integration_publication_fence.py`，入口脚本只会随 DEVX-015 任务的 mandatory 验收行（`runner_identity`）被带入；
  GOV-007 等非 DEVX-015 任务没有这些行，所以永远缺这一项。
- 为什么此前没发现（含我自己的疏漏，如实记录）：①现有非 DEVX-015 回归测试 `test_unbound_full_profile_admits_non_devx015_publication`
  只做到 `LOCAL_MAIN_FF_PRE` 就停止，从未执行 `local-publish` 的 worker 阶段；②所有走 worker 的测试都用 DEVX-015 的 mandatory 夹具，
  入口脚本恰好被带进清单；③v19 我先写过一个真实走 `local-publish` 的大 profile 演练（修复前红、修复后绿），随后为了「一并覆盖远端与 closeout」
  换成 `test_full_transaction_closeout_admits_full_profile_above_default_read_budget`，而它复用的 closeout 辅助函数用**普通 `git merge`**
  快进，并不执行发布 worker；因此 v19 文档里「全链路通过」对 worker 路径的表述是夸大的，此处更正。
- v22 修复设计（只增加绑定证据，不放宽任何校验）：
  1. 在 `inspect_full_publication_profile` 的固定捕获清单加入 `scripts/architecture_arch005_publication_fence.py`（hook/worker 实际执行的入口），
     与现有校验保持一致：被捕获即被哈希绑定，并在检查期间复核未变；
  2. 恢复并加强演练：closeout 辅助函数新增 `publish_via_worker`（用真实 `local-publish` 替代普通 `git merge`）与 `mandatory`
     参数；大 profile 重型测试改为「非 DEVX-015（`mandatory=False`）+ 大 profile + 真实 worker + 远端 push + closeout」，并保留
     DEVX-015 的既有变体；先在未修复代码上复现 `HOOK_READY_ENTRYPOINT` 红，再修复转绿；
  3. 演练若暴露 worker 后续阶段（git launch、hooks、adoption）的其他缺口，在启动 Full 之前一并修复，避免再付 7 小时。
- parent：仍用 v20 的失败摘要（v21 是 PASS，不满足「parent 必须是失败结果」）。
- 验收：演练在修复前红、修复后绿；既有发布夹具与 smoke 通过；reseal 后新候选的前置 tiers 与正式 Full 通过并走完
  `LOCAL_MAIN_FF_PRE → local-publish → REMOTE_PUSH_PRE → 普通 push → CLEANUP_PRE → RELEASED`，`local main = remote main = candidate`。
- 状态/风险：校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`；O3 identity 缓存待 owner 决定；`production_effect=none`。

#### v22 实现与演练结果（2026-10-03）

- 实现：`scripts/run_validation_tier.py` 的 `inspect_full_publication_profile` 固定捕获清单加入
  `scripts/architecture_arch005_publication_fence.py`（hook/worker 入口）。只增加被哈希绑定、并在检查期间复核未变的证据；
  `_validate_hook_capsule` 的入口校验、预算与其余判定不变。DEVX-015 任务原本就经 mandatory 行带入该文件，捕获表按路径去重，行为不变。
- 演练（更正 v19 的覆盖）：`_assert_full_transaction_replays_candidate_publish_and_closeout_receipt` / `_run_actual_publication_fixture`
  新增 `mandatory` 与 `publish_via_worker` 参数；重型测试
  `test_full_transaction_closeout_admits_full_profile_above_default_read_budget` 改为「非 DEVX-015（`mandatory=False`）+ 大 profile
  （20.3 MB，>16 MiB）+ 真实 `local-publish` worker + 远端 push + closeout receipt」。未修复代码上复现与生产一致的
  `PUBLICATION_STAGE hooks_created` 后 `LEASE_EXECUTION_PUBLICATION_HOOK_READY_ENTRYPOINT`；修复后全链路通过（35 分 11 秒，空闲主机），
  worker 依次经过 `ready_held`、`heads_switched`、`merge_resumed`、`merge_exit`，随后 adoption、REMOTE_PUSH_PRE、普通 push、closeout。
  v19 的「大 profile 演练」此前确实走过 `local-publish`（红→绿），后被只用普通 `git merge` 的 closeout 变体取代；现在两者合一。
- 回归：`tests/test_validation_tier_script.py`、`test_arch_005_integration_publication_fence.py`、`test_devx015_workflow_integration.py`、
  `test_devx018_validation_scheduling.py`（排除 real_full_chain）778 passed；相关重型节点 smoke
  （`test_unbound_full_profile_admits_non_devx015_publication`、`test_full_profile_with_bound_parent_admits_failure_fix_rerun_publication`、
  `test_actual_full_profile_preserves_real_whole_readiness`、`test_original_publication_cli_ff_only_and_independent_recovery` 两个变体）6 passed。
- 仍待验证：新候选的前置 tiers 与正式 Full；真实仓库与真实远端上的 `local-publish`、CLOSEOUT 治理预检（夹具使用本地裸远端且不含该预检）、
  普通 push 与收尾。
