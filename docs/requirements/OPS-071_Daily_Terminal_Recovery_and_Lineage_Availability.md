# OPS-071：Daily Terminal Recovery 与 Lineage Availability

最后更新：2026-07-28

稳定任务 ID：`OPS-071_DAILY_TERMINAL_RECOVERY_AND_LINEAGE_AVAILABILITY`

Owner 决定：
`owner_decision:OPS-071:2026-07-28:implement_governed_same_as_of_recovery_v1`

状态：`IN_PROGRESS`

## 1. 问题

2026-07-28 的统一 daily trigger 为 `as_of=2026-07-27` 形成 run
`daily_ops_run:2026-07-27:20260728T003327Z`。Capture、strict DQ、PIT 和 score
已经通过，但 `validate_artifact_lineage` 因独立 ops runtime 没有历史
cache catalog、refresh audit、paper-shadow、weekly、readiness 与 owner-review
artifact 而失败。

现有 control plane 同时存在两个恢复缺口：

1. `max_run_attempts=2`，但默认 idempotent daily step 的 `max_attempts=1`；
   terminal FAILED 后，run-level 第二次机会会被 step budget 立即阻断。
2. state 在 resume 时覆盖当前 terminal 记录，没有 immutable parent attempt、
   recovery reason、release/deployment basis 和 selective replay boundary。

因此现有系统能够 fail closed，却不能在修复后对同一 `as_of` 做受控恢复。

同时，artifact lineage 把“没有历史/未到期/manual artifact”与“lineage graph
结构或安全边界损坏”都解释为 blocking FAIL。该行为与 operations contract
“未到期或上游不足记录 `SKIPPED` / `LIMITED` / `INSUFFICIENT_DATA`，不得补造结论”
不一致，也让一个新建但没有历史研究 artifact 的独立 runtime 永久无法完成 daily
Reader Brief。

## 2. 目标

保留唯一外部入口 `aits ops daily-run`，增加显式、有限、可审计的 same-`as_of`
recovery attempt：

- 原 terminal state、ledger 和 run bundle 保持 immutable；
- recovery child run 必须引用 exact parent run/idempotency key、原 terminal state
  SHA-256、当前 release commit、active deployment receipt SHA-256、reason code 和
  replay-from step；
- 仅当 parent 为 `FAILED` / `BLOCKED`、run attempt budget 尚未耗尽、current release
  与 parent manifest release 不同且 replay slice 全部 idempotent 时允许恢复；
- 已 PASS 且位于 replay boundary 之前的步骤继续复用；从显式
  `recovery_from_step` 开始的已完成步骤及其 downstream 重新执行；
- recovery attempt 拥有独立的 per-attempt step budget，lifetime attempt history
  由 immutable parent archive/receipt 保存；
- non-idempotent、production weight、active shadow weight、broker/trading action
  或未经允许的 provider/cache replay 继续 fail closed；
- 每个 terminal parent 最多一个 recovery child，失败后不得无限重试。

Artifact lineage 必须把 topology/safety validation 与 availability/freshness
disposition 分开：

- malformed graph、duplicate nodes、unsafe production effect、未知 family/edge contract
  继续 blocking FAIL；
- required family placeholder 与 required edge 必须仍可见，不得删除 lineage；
- 缺少未到期、manual、历史 paper-shadow/readiness/owner artifact 时输出
  `PASS_WITH_WARNINGS` / `INSUFFICIENT_DATA`，并保留 missing family/edge warning；
- 不把 missing artifact 标记为 available，不复制旧 checkout bytes，不生成伪造
  paper-shadow、weekly 或 owner decision；
- strict DQ、PIT 和当日真实 source artifact 的 gate 不放宽。

## 3. 实施步骤

### S0：Contract wave

- 扩展 runtime policy，登记 terminal recovery 开关、child attempt budget、显式
  replay boundary 和 release-change要求；
- 新增 versioned recovery request/receipt contract；
- 保持现有 v1 execution state 可读，新增字段或 sidecar 必须 backward compatible；
- CLI 只在显式 recovery 参数完整时构造 recovery request，普通 scheduler run 行为不变。

### S1：Runtime control

- acquisition 验证 parent terminal identity、state hash、run manifest release、
  active deployment receipt 和 replay slice；
- 在任何 current state 变更前冻结 parent state/ledger；
- invalid recovery request、同 release、parent mismatch、非幂等 replay、budget exhausted、
  missing/tampered receipt 全部 fail closed；
- child run 只禁用 recovery boundary 之前的 completed steps。

### S2：Lineage availability

- 以 policy 明确 required family 的 availability role；
- missing placeholder/edge 继续进入 graph 和 Reader Brief limitation；
- validation 对结构/安全 FAIL，对 governed absence 输出 warning；
- 测试证明旧 `2026-07-27` graph 在不补造 artifact 的情况下可得到
  `PASS_WITH_WARNINGS`，同时 unsafe production effect 和 malformed topology 仍 FAIL。

### S3：Validation、release 与 operational acceptance

- focused tests 覆盖 terminal parent archive、child binding、selective replay、same-release
  block、non-idempotent block、attempt exhaustion、tamper 和 lineage availability；
- 更新 operations runbook、scheduled orchestration、system flow 和相关 artifact catalog；
- Fast、Architecture、Contract、Integration、Reproducibility、Full 按最终 candidate 运行；
- fast-forward local main、普通 push并验证 local/remote SHA；
- 形成新 exact release receipt、promotion/deployment acceptance；
- 更新唯一 automation 的 exact release 和 recovery 描述；
- 仅调用一次新 release 的
  `D:\Work\AITradingSystem_ops_runtime\.venv\Scripts\aits.exe ops daily-run`
  recovery trigger，恢复 `as_of=2026-07-27`；
- 验收 capture/DQ/PIT/score复用边界、artifact lineage、report index、
  Reader Brief、quality gates 和 terminal PASS。

## 4. 验收标准

1. 原 `operations_run_99cfc2ed5eeb43200a1cb637` terminal bytes 有 immutable archive，
   recovery receipt 可验证 parent SHA、release变化与 replay boundary。
2. 同 release、错误 parent、第二 child、non-idempotent/provider-sensitive replay 或
   receipt tamper 在 runner/provider call 前阻断。
3. 新 release 对 `as_of=2026-07-27` 从 `artifact_lineage` 开始恢复；不重复 capture、
   provider、DQ、PIT 或 score。
4. lineage 对真实 missing family 输出 `PASS_WITH_WARNINGS` / `INSUFFICIENT_DATA`，
   不声称 artifact available；结构和安全错误继续 FAIL。
5. daily terminal status 为 PASS 或 contract 允许的 PASS_WITH_SKIPS；Reader Brief 和
   两类 quality validation 完成。
6. `production_effect=none`；不写 production weights / active shadow weights，不触发
   broker/order/trading action。

## 5. 路径与生命周期

- mode：`SINGLE_LANE`，最小 serial contract wave；
- task branch：`codex/ops-071-daily-terminal-recovery`；
- task-owned paths：runtime control/operations contracts、daily CLI/executor、
  artifact lineage、focused tests；
- coordinator paths：task register、requirement、runbook、system flow、catalog/registry、
  generated governance state和formal validation evidence；
- 临时 worktree如创建，必须登记 absolute path 并在 closeout 按 DEVX-001 审计清理；
- ops runtime 只通过 reviewed promotion 改变，不从 development checkout 执行业务命令。

## 6. 安全边界

- 本任务不改变 scoring、research window、threshold、portfolio policy 或投资结论；
- recovery 只恢复报告/审计链，不授权 provider budget扩张；
- 不删除或改写旧 FAILED state/bundle；
- 不把旧 development artifacts复制进 ops runtime；
- 不写 production/active-shadow weights；
- 不执行 broker/order/trading action。

## 7. 进度

- 2026-07-28：READ_ONLY、START 与 LANE preflight 均 PASS，mode=`SINGLE_LANE`，
  frozen base=`96a3efbfa8f48dea8c9f56a7e83be9f9279562ef`。
- 2026-07-28：完成 recovery request/receipt、runtime acquisition/archive/selective replay、
  CLI explicit recovery args 与 lineage topology/availability separation。
- 2026-07-28：focused parallel tests `65 passed`；使用 2026-07-27 真实失败 state/ledger
  的临时副本验证得到 child attempt=2、复用 23 个 upstream steps，并确认只重放
  `artifact_lineage` 起的 11 个 report-tail steps。
- 2026-07-28：旧 lineage graph 由 8 个 missing families、11 个 unavailable edges 与
  2 个 stale signal warnings 形成 `PASS_WITH_WARNINGS`，blocking issue=0；未补造任何
  missing artifact。
- 2026-07-28：最新 main 协调候选的正式 Integration 首轮为
  `994 passed / 1 failed`；唯一失败是并行 worker 清理 `__pycache__` 时与
  safety-boundary test 的 `Path.rglob()` 递归扫描竞态，业务 recovery/scheduler 测试均
  已通过。按 owner 要求直接修复该 validation blocker：源码安全扫描改为确定性
  `os.walk()` 并在递归前排除 `__pycache__`，随后必须重新生成兼容性权威并从 Fast
  起重跑六档正式 validation；不得以串行重跑或忽略失败替代修复。
- 2026-07-28：竞态修复后 focused=`205 passed`；新候选 Fast 首轮为
  `340 passed / 1 failed`，失败码为 `S0_DOCUMENT_DRIFT`。根因是本次 blocker
  进度写入 `docs/task_register.md` 后尚未刷新 ARCH-005 S0/S1 generated registry，
  不属于业务逻辑失败。直接刷新 task registry baseline/index/shadows、devex 与兼容性
  当前哈希权威后，必须再次从 Fast 起跑完整六档。
- 待完成：正式六档 validation、新 exact release promotion、automation exact release 更新
  与 2026-07-27 runtime recovery operational acceptance。
