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
