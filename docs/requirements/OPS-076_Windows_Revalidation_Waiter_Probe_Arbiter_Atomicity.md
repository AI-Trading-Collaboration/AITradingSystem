# OPS-076 Windows Revalidation Waiter Probe Arbiter Atomicity

## 状态

- task id：`OPS-076_WINDOWS_REVALIDATION_WAITER_PROBE_ARBITER_ATOMICITY`
- priority：`P0`
- status：`IN_PROGRESS`
- owner：data platform / TRADING-2514 failure-fix coordinator
- production effect：`none`
- broker action：`none`

## 问题与证据

TRADING-2514 final-tree exclusive Full
`outputs/validation_runtime/full_20260812T134447Z/test_runtime_summary.json`
仅失败于
`tests/test_external_request_cache_revalidation_coordination.py::test_two_spawned_processes_same_key_make_one_live_request[11]`：
两个 child 同时完成初始 probe 后，一个进程正常以 `WINNER_PUBLISHED` 结束，另一个进程在读取
测试共享 cache pointer `shared.json` 时抛出 `FileNotFoundError`。该 Full 为
`8873 passed / 1 failed / 3 skipped`；随后相同 coordination 测试文件完整重跑
`41 passed`，说明这是低概率 Windows 并发竞态，而非 TRADING-2514 admission 业务语义失败。

OPS-075 已把 arbiter lock file 的 open/create contention 纳入有界重试，但明确要求后续不同异常
另建任务。本次只治理 waiter probe 与 winner publish 之间的 cache pointer 一致性边界。

## 根因

winner 的 `publish_if_current_owner()` 在 per-key arbiter lock 内依次执行 cache atomic publish、
post-publish probe 和 terminal evidence publication；但 `execute()` 的 waiter loop 先在 arbiter lock
之外调用 `probe()`，随后才进入同一 lock replay lease。Windows 上 `os.replace()` 更新 pointer 时，
未持锁 waiter 可能在极短的 destination visibility/contention 窗口读取目标路径，从而绕过协调器的
typed integrity/timeout contract。

## 设计决策

1. waiter 的 cache `probe()` 与 lease replay 必须位于同一个 per-key arbiter critical section，
   与 winner publish 串行并共享既有 reviewed arbiter timeout/poll policy。
2. `REUSABLE`、`INVALID`、active lease、expired lease 与 takeover 语义保持不变；正常 return/raise
   必须通过 context manager 释放 lock。
3. 不新增 sleep、文件存在性 fallback、异常吞噬或第二套 retry；无法读取的 source 继续 fail closed。
4. 不改变 live request budget、lease TTL、fencing token、provider wrapper、DQ、cache identity、
   investment policy、production 或 broker 边界。
5. `docs/system_flow.md` 不更新：本修复只收紧既有协调器内部 critical section，不改变系统输入、
   输出、CLI、schema、DQ gate 或数据流。

## 验收标准

- waiter probe 与 replay 在同一个 arbiter lock 内执行；
- deterministic regression 证明 waiter probe 只发生在 arbiter critical section；
- spawned-process same-key 20 轮以及 coordination 全文件并行 PASS，仍只有一次 live request；
- stale takeover、timeout、owner failure、non-reusable response 与 fencing tests 无回归；
- compatibility/deprecation、静态检查、最终五级门禁全部 PASS；
- failure-fix Full 使用 `failure_fix_rerun` 并绑定 parent
  `outputs/validation_runtime/full_20260812T134447Z/test_runtime_summary.json`；
- tracked final bytes 在 Full 后冻结，并随 TRADING-2514 coordinator 以普通 non-force push 收口。

## 进展

- 2026-08-12：完成 Full failure root-cause audit，登记 P0 最小 failure-fix；尚未修改运行时代码。
- 2026-08-12：waiter probe 与 lease replay 已合并到同一 arbiter critical section；新增确定性
  lock-depth regression。完整 coordination 文件以相同 `-n 16 --dist loadfile` 并行覆盖
  `42 passed in 27.87s`，Ruff 与 strict mypy PASS。
- 2026-08-12：2514/OPS-076/task-registry 邻接首轮 `79 passed / 1 failed`，唯一失败为新任务登记后
  canonical exact task count 仍冻结为 984；最小提升到真实值 985、重建 generated/compat authority 后，
  完全相同 80-test `-n 16 --dist loadfile` 覆盖 `80 passed in 72.89s`。最终
  compatibility/deprecation 原覆盖 `211 passed in 303.02s`；首轮 exact-count failure 只作
  failure-fix 证据，不作正式门禁证据。
- 2026-08-13：修复后 first final-tree Architecture/Contract/Integration/Reproducibility
  `865/276/995/24 PASS`；exclusive Full
  `8874 passed / 1 failed / 3 skipped`，唯一失败为 historical TRADING-2452 audit 找到的最新
  successor authority 仍是 TRADING-2501 的旧 coordinator source hash。运行时 coordination 与
  OPS-076 focused tests 无失败。最小 durable authority fix 仅把 coordinator module 与对应 test 加入
  active DEVX-006C fragment source set，使生成器从 live LF bytes 派生 current hash；不修改 legacy prefix、
  historical hashes 或 audit 规则。新 final tree 必须重跑完整五级，Full 绑定 parent
  `outputs/validation_runtime/full_20260812T150211Z/test_runtime_summary.json`。
- 2026-08-13：active fragment source-set 修复后，原失败 node、相邻 TRADING-2453、DEVX-006C
  authority 与 coordination 合计 `58 passed`；完整 compatibility/deprecation 原覆盖
  `211 passed in 418.78s`。这些结果在正式五级前封存，后续 tracked bytes 冻结。
