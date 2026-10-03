# DEVX-020 Full 成本拆解、主机遥测与变体冗余审计 V1

- 任务：`DEVX-020_FULL_COST_PROFILING_AND_VARIANT_AUDIT`
- 优先级：P1（开发吞吐，不影响投资解释、评分、回测或数据路径；`production_effect=none`）
- 状态：PROPOSED（DEVX-018 基线发布完成、主机空闲后转 IN_PROGRESS）
- 下一责任方：Claude Code coordinator（`agent_harness=claude_code`）
- 来源：owner 2026-10-02 在 v20 Full 运行期间要求把「具体排查整个验证流程中的任务能否优化」登记为任务，并在 v20 跑完后开始。

## 1. 已有证据（只读，来自 v18 正式 Full 的 runtime profile）

数据：`outputs/validation_runtime/gov-007-p1c-devx015-full-20261002-v18/test_runtime_profile.json`
（候选 `a77a96e0a`，14,675 个节点，墙钟 7.05 小时，节点耗时合计 45.2 worker 小时：setup 5.1、call 39.9、teardown 0.2；
16 个 worker 的平均利用率 40%）。

- 重型节点（调度清单 `real_full_chain`）99 个合计 26.8 小时；轻量节点 14,582 个合计 18.4 小时。
- 并发时间线（每 30 分钟平均并发）：约 1.5 小时后轻量节点全部结束，其后 5.5 小时始终只有 4 个重型节点在运行，其余 12 个 worker 无任务。
  即墙钟只由重型成本与并发上限 K 决定。
- 用同一批节点耗时、LPT 调度（启动间隔只用于最初 K 个）模拟：K=4 为 6.96 小时，实测 7.05 小时，模型可信。

重型成本集中的 7 个测试族（合计 15.9 worker 小时，占重型 59%）：

| 测试族 | 节点数 | 合计 | 单节点均值 | 说明 |
|---|---|---|---|---|
| `test_full_transaction_replays_candidate_publish_and_closeout_receipt`（publication_fence） | 9 | 3.54 h | 1418 s | 每个变体各建夹具、各跑一次真实 Full |
| `test_completed_admission_full_entry_rejects_real_invalid_context`（governed_development_skill） | 11 | 3.26 h | 1067 s | 同上；setup 约 151 s |
| `test_original_publication_cli_ff_only_and_independent_recovery` | 2 | 2.58 h | 4653 s | 租约事件回放（P1/P4） |
| `test_mandatory_acceptance_actual_runner_chain`（devx015_workflow_execution） | 28 | 2.36 h | 304 s | 内层 Full 的运行时身份哈希（O3） |
| `test_publication_lifecycle_binds_original_actual_full` | 2 | 1.72 h | 3104 s | 回放 |
| `test_original_publication_cli_interrupted_after_main_commit` | 1 | 1.28 h | 4624 s | 回放 |
| `test_original_publication_cli_recovers_independent_main_advance` | 1 | 1.15 h | 4146 s | 回放 |

其他观察：
- `test_current_canonical_preflight_identity_and_uncommitted_state` 6 个节点 setup 约 272 秒/个而测试体几乎不耗时，合计约 0.45 小时，是纯夹具成本。
- 34 个非重型节点各自 ≥300 秒，合计 5.1 小时，主要是 `smoothed_*` 系列（单个 10–25 分钟）；不在关键路径上，但在前 1.5 小时与重型节点争 CPU。
- 前置 5 个 tier 约 35 分钟，其测试是 Full 的子集，作用是提前发现问题。

按重型成本与 K 的模拟墙钟（不含 K 增大带来的负载上升）：

| 重型成本 | K=4 | K=6 |
|---|---|---|
| 现状 | 6.96 h | 4.89 h |
| −35% | 4.61 h | 3.33 h |
| −55% | 3.27 h | 2.43 h |

## 2. 调查步骤与验收（须在安静主机上进行；Full 运行期间不得并行跑，以免干扰正式验证）

1. 函数级计时：选 4 个代表重型节点（ff-only、closeout 的 `normal` 变体、`mandatory_acceptance_actual_runner_chain` 一个变体、
   准入检查一个变体），在空闲主机上用函数级计时（cProfile/阶段计时）量出运行时身份哈希、租约回放、git、进程启动、夹具建立各占多少；
   再在受控并发下重复，得到空闲与高负载的放大系数。验收：每节点有可复核的耗时拆解产物与结论，且与 profile 中该节点耗时对得上。
2. 主机遥测：在受控重负载 pilot（或下一次正式 Full）期间每 30 秒采样 CPU、磁盘队列、句柄数，叠加重型并发时间线，
   写出 K 与启动间隔的建议及超时风险。验收：建议由数据支撑，并给出「放大系数 ↔ 触发固定期限」的对照，不再靠「超时有没有触发」判断。
3. 变体冗余审计：对 28、11、9、14 个变体的测试族，列出变体维度与昂贵前置步骤（夹具、内层 Full）的关系，判断能否在不降低覆盖的前提下共享；
   必须附覆盖等价性论证。验收：审计表 + 风险；任何测试结构调整须先经 owner 评审，另立任务实施。
4. 汇总：按「实测节省 worker 小时 / 风险」排序，写回 DEVX-018 的 P1/P4/O3/O4 优先级与重测方案。

## 3. 边界与风险

- 不改变 Full 的验收语义，不放宽任何校验，不缓存跨调用的运行时身份或 PASS（O3 仍待 owner 决定）。
- 排查本身不改生产行为；任何调度、并发、测试结构改动都要另立任务、评审并以正式 Full 重测。
- 计时与遥测需要安静主机：v20 Full 与任何其他正式验证运行期间不开展。
- 本节数字来自一次 Full 的 profile，节点耗时随负载变化；节省量必须在步骤 1–2 用实测确认后才进入验收。

## 4. 进展记录

- 2026-10-02：登记；证据来自 v18 Full profile 的只读拆解（本文第 1 节）。v20 Full 运行中，排查待其结束后开始。

## 5. 排查结果（2026-10-03，安静主机，owner 要求最高优先级）

owner 于 2026-10-03 指示「每次 7 小时的推进效率太低，需要最高优先级优化」。v23 链在前置 tier 阶段（Full 尚未派发）被停止并以失败释放，
让出安静主机；排查用独立 worktree `D:/Work/devx020-prof`（HEAD `a6d110f48`，仅在 `workflow_execution.py` 末尾加了不提交的临时计时钩子，
由环境变量 `AITS_PROFILE_TRACE` 触发）。以下均为实测，不是外推。

### 5.1 运行时身份计算（`acceptance_runtime_identity`）单次成本
- 空闲主机每次 9.6–9.9 秒（1.6554 万个文件、442 MB）；cProfile：`_acceptance_distribution_code` 10.0 秒，其中主线程等待读取线程 7.8 秒；
  sha256 仅 0.34 秒；`_verify_loaded_python_sources` 0.86 秒。
- 读取阶段对线程数饱和：串行 17.3 秒，4/8/16/32 线程分别 7.4/7.5/6.9/7.5 秒。瓶颈是每个文件固定的 Python/系统调用开销（0.54 毫秒/文件），
  不是线程数，也不是哈希。
- `bounded_regular_bytes` 每个文件：`nt.stat` 约占 55%（约 11 次 `lstat`，来自对每一级祖先目录的重解析点检查）；函数自身 Python 开销约 19%
  （每次调用重建 `ctypes.WinDLL` 与 argtypes）。`_acceptance_distribution_code` 的注释明确要求「文件之间不共享祖先/stat 或字节缓存」，
  所以祖先检查去重是**设计层面的取舍**，需要评审；每次调用重建 ctypes 属于纯重构，可直接做。

### 5.2 身份计算的调用次数与并发（真实内层 Full，DEVX-015 mandatory 插件）
- `test_mandatory_acceptance_actual_runner_chain[pass]`（安静主机）：节点 94.8 秒，9 次身份计算合计约 100 秒进程时间（两个 worker 并行），
  即该节点的时间几乎全部是身份计算。28 个变体 × 304 秒（Full 实测）因此主要是身份成本。
- `test_full_transaction_replays_candidate_publish_and_closeout_receipt[normal]`（安静主机）：节点 951 秒；40 次身份计算，其中 35 次来自内层 Full 的
  16 个 worker 在 `pytest_configure` 与 `pytest_sessionfinish` 各一次，其余来自协调器与两次 profile 检查；**最多 16 个并发**，单次耗时
  12.7–78.1 秒（中位 73.5 秒）。单独跑一次 12 秒、16 个并发时每个 73 秒，说明系统范围的文件打开吞吐饱和（约 3.6 千文件/秒），并发越多越慢，
  这也是正式 Full 里 worker 启动要 6.5 分钟、触发各种写死期限的根因。

### 5.3 发布节点的阶段时间线（同一个 951 秒节点，安静主机）
夹具 78 秒；Atlas 渲染约 69 秒；readiness 约 40 秒；真实生成器约 37 秒；内层 Full 274 秒；取结果与写产物 22 秒；
`FORMAL_VALIDATION_RESULT` + `LOCAL_MAIN_FF_PRE` 检查 209 秒；远端/收尾 221 秒。
同一测试族的所有变体都会重复「Atlas + readiness + 生成器 + 内层 Full」约 420 秒（44%）。

### 5.4 调度（K）
v18 Full 的并发时间线：约 1.5 小时后只剩 4 个重型节点（K=4），此前实测尾段 CPU 仅 19–26%（32 个逻辑核）。K=4 是在固定超时只有 60–360 秒时定下的；
超时已全部校准到 ≥1800 秒（驱动 3600 秒）。调度模拟（v18 节点耗时，LPT）：K=4 为 6.96 小时，K=6 为 4.89 小时，K=8 为 3.94 小时。

### 5.5 结论与候选方案（需 owner 对策略性项拍板）
1. **纯重构、无语义变化（可直接做）**：每次调用只建立一次 `ctypes` 绑定、减少 `pathlib` 构造；预期单次身份约 −15–20%。
2. **祖先检查在同一次调用内共享（设计取舍，需评审）**：已存在的「打开后核对最终路径」仍会发现祖先被重定向，祖先 `lstat` 是更早、更具体的错误码；
   预期单次再 −30–35%。
3. **worker 侧不再整体重算（策略取舍，需评审）**：控制器完整字节哈希一次，worker 仅对同一清单做元数据指纹比对（路径、大小、mtime_ns、文件 ID），
   不一致即 fail closed；可消除 16×2 次并发爆发（内层 Full 节点 −16% 到 −60%），并降低系统范围争用，从而允许提高 K。
4. **提高 K（配置变更，需 pilot 验证）**：4 → 6 预期墙钟 7.0 → 4.9 小时；风险是负载相关偶发增加，须配合上面的减负。
5. **缩小被哈希的集合（环境/策略取舍）**：92 个发行版全部纳入；若验证用 venv 精简，文件数可显著下降。

## 6. 进展记录（续）

- 2026-10-03：转为 IN_PROGRESS；owner 要求最高优先级；上述 5.1–5.4 为实测结果，5.5 的 2–5 项等待 owner 决策。临时工作区
  `D:/Work/devx020-prof`（git worktree，含不提交的计时钩子与从主检出复制的 `outputs/research`、`outputs/atlas`、
  `outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z`）、`D:/Work/devx020-bt1`、`D:/Work/devx020-bt2`、
  `D:/Work/devx020-trace-*.jsonl`：排查结束后审计并清理（`git worktree remove`），不得提交计时钩子。
