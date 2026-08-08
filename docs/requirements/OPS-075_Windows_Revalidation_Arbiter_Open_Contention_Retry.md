# OPS-075 Windows Revalidation Arbiter Open Contention Retry

## 状态

- task id：`OPS-075_WINDOWS_REVALIDATION_ARBITER_OPEN_CONTENTION_RETRY`
- priority：`P0`
- status：`BASELINE_DONE`
- owner：data platform（后续异常另建任务）
- production effect：`none`
- broker action：`none`

## 问题与证据

TRADING-2501 final-tree exclusive Full 在
`outputs/validation_runtime/full_20260808T085520Z/test_runtime_summary.json`
出现 3 个失败。其中唯一运行时失败为
`tests/test_external_request_cache_revalidation_coordination.py::test_two_spawned_processes_same_key_make_one_live_request[2]`：
一个 Windows child 在同 key 并发协调期间抛出 `PermissionError`，另一个 child 正常以
`WINNER_PUBLISHED` 完成。其余两项失败是 2501 append-only historical
compatibility successor ceiling 未更新，不属于 external-request cache 语义失败。

只读根因分析确认 `_exclusive_file_lock` 已对 byte-range lock 的瞬时竞争执行有界重试，
但 lock file 的首次 `open("a+b")` 位于该重试边界之外。Windows 可在另一个进程刚创建或
占用同一路径时，于 open/create 阶段返回 transient access/sharing violation；当前实现让该
异常绕过 typed arbiter timeout contract，直接终止 child。

## 设计决策

1. 将 lock file 的首次 open/create 纳入与 byte-range lock 相同的 monotonic deadline。
2. 仅把现有 transient `PermissionError` 以及 Windows access/sharing/lock violation
   （WinError 5、32、33）视为可重试竞争；其他错误继续 fail closed 原样抛出。
3. open 与 byte-lock 共用 reviewed `timeout_seconds` 与 `poll_seconds`，不得通过第二个期限
   隐式延长等待。
4. deadline 耗尽时继续使用 typed `RevalidationCoordinationTimeout` / `ARBITER_TIMEOUT`，
   不增加静默 fallback。
5. 不修改 lease、fencing token、provider request、DQ gate、cache identity/publication、
   model policy、投资解释或 production/broker 边界。
6. `docs/system_flow.md` 不更新：本修复只收紧既有协调器内部文件锁的异常边界，不改变
   输入、输出、CLI、cache schema、DQ gate 或系统数据流。

## 实施步骤

1. 登记本任务并通过 governed `SINGLE_LANE` preflight。
2. 把 open/create transient contention 纳入单一 deadline 重试，并增加确定性 focused
   regression coverage。
3. 修复 2501 compatibility latest-successor ceiling，刷新 append-only current authority、
   DevEx manifests 与 task shadow。
4. 在最终字节运行 focused spawned-process tests、compatibility/deprecation 与静态检查。
5. 从最终树重跑 Architecture、Contract、Integration、Reproducibility，以及独占 Full；
   Full 使用 `failure_fix_rerun` 并精确绑定失败父 artifact。
6. 通过 governed integration、普通 non-force main push、verify 与 cleanup 收口。

## 验收标准

- initial lock-file open/create transient contention 使用有界 retry；
- open 与 byte-lock 共用同一 deadline；
- deadline exhaustion 输出 typed `ARBITER_TIMEOUT`；
- 同 key 二进程只发生一次 live request，winner/loser evidence 保持 canonical；
- lease/fencing/provider/DQ/cache publication semantics 无漂移；
- 2501 historical compatibility current-authority tests 全部通过；
- final-tree 五级门禁与 parent-bound exclusive Full 全部 PASS；
- tracked final bytes 在 Full 后冻结并以普通 non-force push 收口。

## 进展

- 2026-08-08：登记任务；failure parent 已保存，尚未实施运行时代码修复。
- 2026-08-08：open/create transient contention 已纳入 byte-lock 的同一 monotonic
  deadline；新增一次瞬时失败后成功与 deadline exhaustion 的确定性测试。
- 2026-08-08：direct coordination focused=`22 passed`（含 20 次 spawned-process
  same-key regression），compatibility/deprecation failure-fix chain=`116 failed / 165 passed`
  → `84 failed / 124 passed` → `1 failed / 207 passed` → `208 passed`；级联均来自 current
  authority/successor freeze 断言，运行时协调器相邻测试无额外失败。
- 2026-08-08：候选转 `BASELINE_DONE`；最终五级及 parent-bound Full 证据只写
  canonical runtime artifacts，Full 后不再修改 tracked bytes。
