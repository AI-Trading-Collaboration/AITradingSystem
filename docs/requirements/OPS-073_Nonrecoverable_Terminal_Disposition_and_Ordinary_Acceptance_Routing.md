# OPS-073：不可恢复 Terminal Disposition 与 Ordinary Acceptance 路由

最后更新：2026-08-02

稳定任务 ID：`OPS-073_NONRECOVERABLE_TERMINAL_DISPOSITION_AND_ORDINARY_ACCEPTANCE_ROUTING`

Owner 决定：
`owner_decision:OPS-073:2026-08-02:add_terminal_disposition_and_single_scheduler_acceptance_v1`

状态：`IN_PROGRESS`

## 1. 问题

2026-08-01 的 canonical daily parent
`daily_ops_run:2026-07-31:20260801T014510Z` 在 `score_daily` 前因
`official_policy_sources` transient TLS EOF 形成 terminal `FAILED`。OPS-072 已完成耐久修复、
exact release promotion 与 deployment acceptance，但该 parent 不满足 OPS-071 same-`as_of`
recovery contract：当前 allowlist 从 `artifact_lineage` 尾链开始，并要求 capture、strict DQ、
PIT 与 score 已 PASS；本 parent 的 `score_daily` 为 `SKIPPED`。

现有 Codex automation `aitradingsystem-pit` 的 prompt 把“存在上一 terminal FAILED/BLOCKED”
无条件路由到 recovery child，没有先区分 parent 是否存在合法 replay boundary。这会导致：

1. parent 不可恢复时，scheduler 只能反复 fail closed；
2. resolver 已进入严格晚于 parent 的新 provider-ready `as_of` 后，仍可能错误尝试旧 parent
   recovery，而不是使用新 idempotency key 执行 ordinary daily；
3. operator 容易把“下一新日期的运营验收”误称为 recovery，并产生扩大 allowlist、重复
   provider/capture 或删除旧 state 的压力。

这是 terminal disposition / scheduler routing 缺口，不是 OPS-071 recovery allowlist 缺陷。

## 2. 决策与不变量

统一 external scheduler 与业务 trigger 均保持唯一：

- scheduler id：`aitradingsystem-pit`；
- trigger：runtime-local `aits.exe ops daily-run`；
- 不新增第二个会调用业务 trigger 的 automation、Windows Task Scheduler entry、独立
  non-daily trigger 或手工绕过入口。

Scheduler 在调用唯一 trigger 前必须把最新 terminal parent 归入以下互斥 disposition：

1. `RECOVERABLE_SAME_AS_OF_TAIL`
   - parent 为 terminal `FAILED` / `BLOCKED`；
   - exact recovery request/receipt、attempt、release/receipt、manifest/state/ledger 和
     reviewed idempotent replay boundary 全部满足 OPS-071；
   - 本 invocation 只能调用一次带完整 recovery 参数的现有 trigger。
2. `WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY`
   - parent 没有合法 recovery boundary，或修复需要 provider/capture/DQ/PIT/score replay；
   - parent state、ledger、manifest、diagnostic 与原始 bytes 保持 immutable；
   - 当 resolver 仍返回 `parent.as_of` 时，本 invocation 记录等待并且不调用业务 trigger；
   - 只有 resolver 返回严格晚于 `parent.as_of` 的 provider-ready trading day，且新
     workflow spec/idempotency key 无 state、无 active lock，active deployment receipt 与
     exact release 验证通过时，才使用同一 existing automation 执行一次不带 recovery 参数的
     ordinary daily；
   - 新运行可以在 operator summary 中关联 incident parent，但不得伪装为 recovery child。
3. `READY_FOR_NEW_AS_OF_ORDINARY`
   - resolver 已返回严格晚于 parent 的 provider-ready trading day；
   - 新 workflow spec/idempotency key 无 state、无 active lock；
   - active deployment receipt 与 exact release 均通过；
   - 本 invocation 只调用一次不带 recovery 参数的 existing trigger。
4. `BLOCKED_EXTERNAL_OR_OWNER`
   - secret/API 权限、外部 provider 持续不可用、owner-only investment/policy 决策、
     non-idempotent boundary、unsafe production effect 或其他无法在授权内安全解除的条件；
   - 记录根因、影响、验证覆盖、退出条件和 next owner，不调用 trigger。

`terminal_recovery_allowed_from_step_ids` 保持不变。不得为本事故加入 capture、DQ、PIT、
score 或 provider replay；不得删除或改写旧 state/ledger、伪造新 key、提高旧 attempt budget、
写 production/active-shadow weights 或触发 broker/order/trading。

## 3. 当前事件的确定性计划

- parent `as_of`：`2026-07-31`；
- parent key：`operations_run_66162bb4f69d48ee56aa73a4`；
- disposition：`WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY`；
- next trading day：`2026-08-03`；
- provider-ready：`2026-08-03T19:00:00-04:00` / `2026-08-04T08:00:00+09:00`；
- existing automation schedule：每日 `09:30 Asia/Tokyo`；
- first eligible planned invocation：`2026-08-04T09:30:00+09:00`；
- expected fresh key under current workflow spec：
  `operations_run_f05cabc34a8e7f2c5e04f0d1`；登记时该 key 无 state、无 lock。

这是一条 ordinary operational acceptance 计划，不是 recovery child。2026-08-02 与
2026-08-03 的 09:30 invocation 若 resolver 仍返回 `2026-07-31`，只能记录等待，不得调用
`daily-run`。

## 4. 实施步骤

### S0：登记与合同冻结

- 登记 OPS-073 task row 与本 requirement；
- 保留 OPS-071 same-`as_of` recovery contract 和 allowlist；
- 明确 automation 原位更新，不新增第二 scheduler。

### S1：Runbook、system flow 与 scheduler prompt

- 在 operations runbook 增加 terminal disposition 决策顺序和 ordinary rollover gate；
- 更新 `docs/system_flow.md`，展示 terminal parent 在 recovery、等待新 `as_of`、external/owner
  blocked 三条分支；
- 使用 Codex automation API 原位更新 `aitradingsystem-pit` prompt：先解析 disposition，再
  决定 recovery、等待或新日期 ordinary；保持原 schedule、project、model、execution
  environment 与唯一入口不变；
- prompt 必须明确不可恢复 parent 不得标成 `READY_FOR_RECOVERY`，新日期 ordinary 不携带
  recovery 参数。

### S2：回归与安全验证

- 增加 focused contract regression，覆盖三种 disposition、日期严格递增、新 key/state/lock、
  active release/receipt、single scheduler 与 unchanged recovery allowlist；
- 负例覆盖同 `as_of` ordinary、不可恢复 parent recovery、第二 scheduler、allowlist 扩大、
  provider/capture replay、production/active-shadow weight write 和 broker/trading；
- 按风险运行 Fast Unit、Architecture Fitness、Contract Validation、Integration、
  Reproducibility 与 Full；失败不得用串行 pytest 静默替代。

### S3：发布与计划验收

- validated candidate fast-forward local `main` 并普通 non-force push；
- 因 runtime runbook 与 scheduler contract 发生变化，创建 exact release candidate、promotion
  transaction 和 owner-accepted deployment receipt；
- 使用 automation API 原位切换 exact release pin；不得直接编辑 runtime checkout；
- 在 `2026-08-04T09:30:00+09:00` 或之后的首个合法 existing automation invocation 执行
  ordinary daily acceptance；若 resolver、新 key、lock、deployment 或 provider gate 不满足，
  继续 fail closed并记录退出条件。

## 5. 验收标准

1. 不可恢复 terminal parent 不再强制进入 impossible recovery；
2. 同 `as_of` 不会 ordinary rerun，旧 parent bytes 保持 immutable；
3. 新 provider-ready `as_of` 使用 fresh key 执行 ordinary daily，且不携带 recovery 参数；
4. recovery 仍只覆盖 reviewed same-`as_of` idempotent tail；
5. scheduler entry count 保持 1，外部业务 trigger 仍只有 runtime-local `aits ops daily-run`；
6. task/runbook/system-flow/tests/automation prompt 与 active deployment exact release 可追溯；
7. `production_effect=none`，无 production/active-shadow weight write、broker/order/trading action。

## 6. 临时工作区生命周期

- task branch：`codex/ops-073-terminal-disposition`；
- worktree：`D:\Work\AITradingSystem_ops073_terminal_disposition`；
- integration worktree：`D:\Work\AITradingSystem_ops073_integration`，仅用于在最新 local
  `main` 上执行 governed drift/attribution 检查、`--ff-only` 集成、普通 push 与 release
  promotion/acceptance 准备；创建前要求该路径不存在，完成或放弃后按相同证据与进程门清理；
- owner：OPS-073 coordinator；
- purpose：隔离当前主 checkout 的其他任务改动，完成本任务登记、实现、验证和集成；
- exit condition：候选进入 validated local/remote main，required evidence 已进入 canonical
  location，active deployment/automation pin 完成且无进程依赖后，审计并删除 worktree、prune
  worktree metadata 与删除已合并 branch；若无法清理，必须在 task row 记录原因和下一 owner。

## 7. 进度记录

- 2026-08-02：Owner 要求补充规则并在合适时点安排执行。范围收敛为 terminal disposition
  与 existing single-scheduler ordinary acceptance routing；不新增第二 scheduler，不扩大
  recovery allowlist。只读 preflight PASS；当前主 checkout 属于 TRADING-2477 且有其未提交
  paths，本任务从 exact local `main=9e90ccfb9a079473bab0e4f2af0c665c8a7a3ea1` 建立独立
  worktree，未触碰其他任务改动。
- 2026-08-02：scheduler checkout policy 升级为 v3，增加四类 reviewed disposition 与 pure
  resolver；`WAIT` 与 `READY` 分离，避免“日期已到但仍显示等待”的模糊状态。Resolver 只返回
  routing evidence，不调用 trigger、不写 state、不运行 provider。
- 2026-08-02：focused Ruff/Black 对 task-owned source/test PASS；并行 pytest 覆盖 scheduler
  policy、四类 resolver、文档合同与 task shadow=`31 passed`。Bootstrap handoff 在隔离 worktree
  首次因四份 ignored validation artifact 缺失 fail closed；随后只从主 checkout 按 handoff exact
  path/SHA复制对应 immutable artifacts，四份 SHA-256 全部匹配。Task shadow generate/validate
  PASS：942 tasks、437 active、505 completed、v1/v2 byte-identical、legacy Markdown 仍为 source
  of truth、production effect none。任务进入 `VALIDATING`，下一步提交 exact candidate 并运行
  formal tiers。
