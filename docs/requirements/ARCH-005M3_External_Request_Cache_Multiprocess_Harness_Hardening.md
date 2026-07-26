# ARCH-005M3：External Request Cache Multiprocess Harness Hardening

最后更新：2026-07-27

稳定任务 ID：
`ARCH-005M3_EXTERNAL_REQUEST_CACHE_MULTIPROCESS_HARNESS_HARDENING`

Owner continuation：
`owner_continuation:ARCH-005M3:2026-07-27:continue_long_term_engineering_goal`

状态：`DONE`

## 1. 问题与目标

OPS-065 已证明 external request cache revalidation coordination 在相同 cache key 下只允许
一个 live request，并允许 loser 通过 `WAITER_REUSE` 或
`WINNER_DOUBLE_CHECK_REUSE` 消费同一 published generation。现有 multiprocess test
harness 仍依赖多个彼此独立的 15 秒 `Queue.get` / `Process.join` timeout：

- child 在写入 first-probe 或 result 前异常退出时，parent 只能等待 blind timeout；
- result 不携带 PID，无法机械绑定到 exact child；
- 每个 child 分别获得完整 timeout，整体失败时间随 worker 数增长；
- assertion 或 queue timeout 发生时没有统一 `finally` 回收路径，可能遗留 process、
  queue feeder thread 或未关闭 queue；
- race-dependent合法分支虽然已接受，但 orchestration failure 与 production
  coordination failure不够清晰。

本任务只加固测试基础设施，不改变
`ExternalRequestRevalidationCoordinator`、policy、lease、heartbeat、takeover、unlock、
cache schema、provider request、request budget、DQ/PIT 或生产行为。

## 2. 权威边界

- production coordination语义继续由
  `src/ai_trading_system/external_request_cache_revalidation_coordination.py` 与 reviewed
  policy定义，本任务不得修改；
- harness只在
  `tests/test_external_request_cache_revalidation_coordination.py` 内运行；
- waiter与late-contender仍是同一 stale first-probe 后的两种合法调度结果；
- `WINNER_PUBLISHED` 必须恰好一个；loser不得发起第二个live request；
- no periodic operations、no real provider request、no cached market-data mutation；
- `production_effect=none`、`broker_action=none`。

## 3. 实施步骤

### S0：结构化 child protocol

- first-probe record和terminal result都携带exact child PID；
- PASS result显式记录coordination status/value，FAIL result显式记录exception type/message；
- parent拒绝unknown PID、duplicate PID、missing PID或duplicate terminal result。

### S1：统一 orchestration deadline

- 整个barrier、result collection与normal join共享一个monotonic deadline；
- queue轮询期间检查所有child `exitcode`，child在所需record前退出时立即报出
  PID/exit code/phase；
- deadline failure显示明确phase，不按worker数量叠加blind timeout。

### S2：确定性 cleanup

- 所有成功/失败路径都进入同一个`finally`等价cleanup；
- cleanup先release barrier，再terminate仍存活child、bounded join，必要时kill并再次join；
- 两个Queue都执行`close`与`join_thread`；
- cleanup若仍发现alive child必须作为独立harness failure暴露，不能覆盖已有primary failure。

### S3：回归和重复稳定性

- same-key test重复20次，每次验证exactly one live request与exactly one winner；
- loser只允许`WAITER_REUSE`或`WINNER_DOUBLE_CHECK_REUSE`，并精确验证对应event generation；
- different-key overlap与non-reusable serial paths保持；
- 新增child early-exit与stalled-before-probe负例，证明快速诊断、统一deadline和无残留child；
- 完整测试文件、Ruff、architecture/contract与风险相称的Full通过。

## 4. 验收标准

- structured probe/result record均绑定exact PID，parent机械验证一child一record；
- child early exit不等待完整blind timeout，错误包含phase、PID与exit code；
- 一个orchestration deadline覆盖barrier/result/join；
- 任意异常后无task-owned child或queue残留；
- same-key 20次重复均为一个live request、一个winner、一个合法reuse；
- different-key仍并行，non-reusable response仍严格串行；
- production module和policy bytes不变；
- no provider/cache/DQ/strategy/production/broker side effect。

## 5. 分支与生命周期

- frozen base：`970d5189f707a3e7b1fd62a7d96c24cbbda79d4b`；
- task branch：`codex/arch-005m3-multiprocess-harness`；
- 不创建额外worktree、clone或长期cache；
- pytest temporary directories由pytest回收；spawn children与queues必须由harness在每次
  run结束时回收；
- 既有`D:\Work\AITradingSystem_ops_runtime_20260725`不得使用或删除；
- known-unrelated owner文档不得读取、修改或提交；
- task branch在main集成和ordinary push验证后删除，可由main历史完整恢复。

## 6. 进度

- 2026-07-27：READ_ONLY与`SINGLE_LANE START` preflight均PASS，
  `main=origin/main=970d5189f707a3e7b1fd62a7d96c24cbbda79d4b`，active lease=0。
  任务由`PROPOSED`进入`IN_PROGRESS`；本轮只修改multiprocess test harness，不推进
  DATA-GOV C3、ARCH-005 S5、production coordination或provider runtime。
- 2026-07-27：structured probe/result dataclass、exact PID mapping、统一monotonic
  orchestration deadline、first-probe/result阶段child exit诊断及确定性cleanup已实现。
  same-key参数化重复20次并保留`WAITER_REUSE`/`WINNER_DOUBLE_CHECK_REUSE`两种合法路径；
  early nonzero/zero exit、terminal-result前exit与stalled-before-probe负例均PASS。Ruff PASS，
  完整coordination文件=`39 passed`，coordination+cache regression=`65 passed`；
  production module与reviewed policy相对frozen base exact bytes不变。状态进入
  `VALIDATING`。
- 2026-07-27：兼容性authority已追加且历史prefix byte-identical；完整兼容性回归
  `88 passed`。修正历史source跟踪断言：仅允许后续compatibility section通过精确
  `removed_live_source_paths`正式登记的路径缺失，当前M3 authority仍要求全部live source
  hash匹配。累计focused evidence=`153 passed`；Architecture=`725 passed`、Contract=
  `275 passed`，下一步运行Full gate。
- 2026-07-27：唯一required Full以审计trigger
  `natural_integration_boundary`通过：`7494 passed / 3 skipped / 643 warnings`；
  任务验收满足并归档DONE。归档后的最终树继续执行post-Full Architecture/Contract，
  不重新运行Full。
