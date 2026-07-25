# OPS-070：Objective Blocker 与 Consumer Dependency DAG

## 状态

- priority：P0
- status：VALIDATING（S1～S4 engineering complete；external deployment / operational acceptance pending）
- owner：operations owner + data platform owner
- started：2026-07-25
- upstream：OPS-069、DATA-GOV-001_D0B2B
- production effect：none

## 问题

OPS-069 已把交易日输入改为逐源尝试并保留成功来源，但 daily executor 仍是
线性 fail-fast：

1. `capture_daily_inputs` 只要一个 required component 失败就返回非零；
2. runtime-control 在任一步失败时立即终结 lease；
3. 后续步骤没有显式声明 step dependency 和 capture component dependency；
4. 因而一个来源、一个 consumer 或一个报告故障会阻止无关来源的验证和运营收口。

这会降低故障可观测性，并可能让已可获得的关键数据缺少同日验证证据。修复不得
弱化 strict DQ、PIT、SEC、valuation、score、Reader Brief 或 finalization gate。

## 目标

将周期任务拆为以下四个平面：

1. scheduler admission：只决定 `as_of`、XNYS session/provider-ready 和唯一运行租约；
2. source capture：逐源、source-scoped retry、成功来源立即保全；
3. consumer DAG：每个 consumer 只依赖自己声明的 upstream step 和 capture component；
4. operator closure：即使投资消费链失败，仍执行获准的只读健康/安全收口。

最终日任务只要 required consumer 未完成，仍必须是 `BLOCKED` 或 `FAIL`；允许继续
独立分支不等于允许消费缺失数据，也不等于 terminal PASS。

## Objective blocker taxonomy

只有以下条件可阻断对应 source capture：

- XNYS decision session 或 provider-ready window 尚未到达；
- 同一 source/as_of 的有效 lease 或幂等冲突；
- reviewed quota/request budget 不允许继续请求；
- provider credential、permission、endpoint availability 或 schema 客观不可用；
- filesystem/integrity/security 写入条件不满足；
- source-scoped reviewed retry budget 已耗尽。

以下条件不得阻断无关 source capture：

- 另一个 provider/key 缺失或失败；
- DQ、PIT、SEC、valuation、score、report 或 dashboard consumer 失败；
- optional artifact/人工复核尚未完成；
- 开发 checkout 脏状态或晚段 CLI/report defect。

这些非 source blocker 仍需记录并阻断自己的 dependents，禁止静默绕过。

## 分阶段实施

### S1：显式依赖与非全局失败传播

- `config/scheduled_tasks.yaml` 为 daily task 声明 `dependencies`、
  `required_capture_components` 和 `always_run`；
- `capture_daily_inputs` 的进程成功仅表示所有 component 已尝试且 manifest、
  validation、gap ledger 已可靠写出；`PARTIAL_CAPTURE` 不再伪装成 `CAPTURED`；
- executor 在运行 step 前检查 upstream step 结果和 capture component；
- 未满足的 consumer 记录 `BLOCKED`，其独立 sibling 继续；
- runtime-control 在 branch failure 后继续记账，最后统一写 terminal
  `BLOCKED/FAILED`；
- `pipeline_health` 与 `secret_hygiene` 作为 always-run operator closure，禁止生成
  投资结论或交易副作用。

### S2：逐源 lease、退避与 provider-ready evidence

- 将 component retry、quota、credential、provider response 分类为稳定 blocker code；
- 增加 per-source lease/idempotency 与 retry-after evidence；
- 同一 source 的失败不得重复污染其他 source 的 attempt budget。

S2 使用 `config/operations/daily_input_capture.yaml` v2 作为唯一策略权威。每个
`source/session` 有独立 state、attempt history、active lease 与幂等 key；已验证 PASS
可复用，活动 lease、stale takeover、非 retryable 失败和 attempt exhaustion 必须留下稳定
blocker code。只有 reviewed `retryable_blocker_codes` 可以在本次 source budget 内重试，
每次 attempt 的 blocker、delay、起止时间和 sanitized error 都进入 manifest。lease TTL、
attempt budget 和 retry delay 均不得在代码中作为投资/运营判断的未解释字面量。

### S3：缺口恢复队列

- 从 gap ledger 生成 source/session 级恢复队列；
- 只恢复 immutable raw bytes、checksum 和可审计派生物；
- 历史缺口不得补造成当日可见的 strict PIT，不得改写旧 terminal state。

恢复队列必须逐 `session/component` 记录 `recovery_mode`：

- `market_macro=IMMUTABLE_RAW_BACKFILL` 可进入人工恢复准备队列；
- `sec_companyfacts=MANUAL_NON_PIT_RAW_REVIEW` 只允许 owner 评估非 PIT raw archive；
- FMP forward PIT、FMP valuation 与 official policy sources 为
  `HISTORICAL_RECAPTURE_FORBIDDEN`。

本阶段只生成和验证 queue，不自动请求 provider、不改旧 capture manifest/run-control
terminal、不授权 DQ/PIT/score。任何未来 executor 必须是独立 reviewed task。

### S4：隔离运营 checkout

- 外部 scheduler 使用 pinned、clean、独立 ops checkout；
- 开发 worktree 状态不参与 production daily admission；
- deployment/credential/lease 需要 owner 审核后才能切换，本任务不得静默模拟。

S4 工程边界是 reviewed checkout policy、静态/运行时 preflight 与
`aits ops daily-run` scheduler-mode admission：必须使用与开发工作区不同的绝对路径、
clean Git checkout、exact 40-char release commit、reviewed remote 和唯一 trigger。
preflight PASS 只表示候选可运行；在 owner 完成 scheduler/credential deployment 前，
`activation_authorized=false`，不得自动安装或启用系统 scheduler。

## S1 dependency policy

### Capture component gates

| consumer | required component |
|---|---|
| `validate_data` | `market_macro` |
| `pit_snapshots_build_manifest` | `fmp_forward_pit` |
| `sec_metrics` | `sec_companyfacts` |
| `score_daily` | `fmp_valuation`, `official_policy_sources` |

`score_daily` 还必须依赖 strict `validate_data`、`pit_snapshots_validate` 和
`sec_metrics_validation`。Closed-market plan 没有 capture umbrella 时，现有 live fetch
steps 继续作为等价 step dependency，不能读取不存在的 capture manifest。

### Always-run closure

- `pipeline_health`
- `secret_hygiene`

两者只输出本地只读/安全检查 artifact；它们的 PASS 不得覆盖其他 branch 的
`FAIL/BLOCKED`。

## Acceptance criteria

1. 某一个 capture component 失败时，其余 components 全部尝试并保留；
2. market/macro 失败只阻断 DQ 及其 dependents，PIT/SEC 独立验证仍执行；
3. FMP PIT 失败只阻断 PIT/score/report dependents，DQ/SEC 独立验证仍执行；
4. SEC 失败只阻断 SEC/score/report dependents，DQ/PIT 独立验证仍执行；
5. 任一 required branch 不完整时 overall 不得为 PASS/PASS_WITH_SKIPS；
6. always-run closure 在 branch failure 后执行，但不授权 Reader Brief 或 production；
7. runtime-control ledger、diagnostic、metadata 显示 dependency/blocker 原因；
8. resumed PASS step 可满足 dependency，blocked/failed step 不可；
9. closed-market plan 的 conditional fetch dependency 正确；
10. focused、architecture、contract、integration、reproducibility 和 Full parallel
    validation 通过；
11. 不写 production weights/active shadow weights，不触发 broker/trading action。

## 风险与退出条件

- S1 已解决非全局失败传播；S2 已完成逐源 lease/idempotency、blocker taxonomy 和
  reviewed retry；S3 已完成只生成、不自动执行的缺口恢复队列；S4 已完成独立
  checkout policy/preflight，但 scheduler、credential 与 checkout 部署尚未激活；
- dependency 配置错误可能造成误跑或过度阻断，因此未知 dependency、环和未声明 daily
  dependency 必须 fail closed；
- append-only compatibility authority 必须只覆盖归属清晰的 live mismatch；当前共享
  worktree 混入 DEVX-001 的 `AGENTS.md` / task-registry 改动，不能把它们静默登记为
  OPS-070 authority；
- external scheduler deployment 必须由 owner 在独立绝对路径 checkout 上配置 exact
  release commit、reviewed origin、credentials 与唯一 `aits ops daily-run` trigger；
  preflight PASS 本身不授权安装或启用 scheduler；
- 只有新合法 provider-ready XNYS session 的真实 `aits ops daily-run` 证明 capture、
  branch isolation、strict gates 和 closure 符合预期后，才可考虑 operational acceptance。

## 临时 closeout worktree 生命周期

- owning task：`OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG`
- absolute path：`D:\Work\AITradingSystem_ops070_closeout_20260725`
- purpose：从 `fc6313416d78f56a29519f41ca564eaa1f90e8ce` 建立 clean worktree，只移植
  OPS-070 归属明确的实现、测试、配置、文档和生成 authority；不得移植 DEVX-001、
  owner research 文件或其他共享 dirty change。
- exit condition：纯 OPS-070 compatibility authority、architecture-fitness、Full、
  local commit 与正常 push 完成后，确认没有唯一未提交证据或活动进程，使用
  `git worktree remove` 清理并执行 `git worktree prune`。若失败或含未审计修改，
  保留该目录并在本节记录具体原因、风险、next owner 和下一退出条件。

## 状态记录

- 2026-07-25：S1 实现与 fault-injection 完成。focused=`122 passed`，
  contract-validation=`275 passed`，integration=`994 passed`，
  reproducibility=`23 passed`；Ruff 与 `git diff --check` 通过。architecture-fitness
  最近一次为 `607 passed / 13 failed`：其中 stale architecture manifest 已重新生成，
  其余失败要求追加当前 compatibility hash authority；但工作区同时存在不属于本任务的
  `DEVX-001` 对 `AGENTS.md` 与共享 task registry 的修改，当前 mismatch 集无法只归属
  OPS-070。按 no-silent-workaround 不把该并发修改登记为 OPS-070 authority，不提交或
  推送混合变更。待 DEVX-001 独立收敛后追加 OPS-070 baseline、重跑
  architecture-fitness 与 Full，再完成 S1 工程闭环。
- 2026-07-25：S2～S4 engineering 完成。`daily_input_capture` policy v2 已加入
  per-source/session state、exclusive lease、stale lease audit、deterministic
  idempotency、稳定 blocker taxonomy、reviewed retry budget 与 terminal PASS reuse；
  gap ledger 已生成并验证 source/session recovery queue，只有 market raw backfill 可
  进入人工准备，SEC 仅 owner review，FMP/official historical recapture fail closed；
  新增独立 ops checkout policy 与 `aits ops scheduler-checkout-preflight`，scheduler-mode
  `daily-run` 在 provider/cache 访问前验证 exact commit、origin、clean、独立路径和当前
  process checkout，且 `activation_authorized=false`。
- 2026-07-25：最终聚焦测试=`133 passed`；contract-validation=`275 passed`，
  integration=`994 passed`，reproducibility=`23 passed`；Ruff 与
  `git diff --check` PASS。architecture-fitness=`608 passed / 12 failed`，12 项全部为
  同一 append-only compatibility authority mismatch，没有新增 architecture writer、
  module boundary 或 documentation contract failure。由于 live mismatch 仍混入并发
  DEVX-001 的 `AGENTS.md` / task registry，未签发混合 authority；Full 按验证顺序
  `SKIPPED_BLOCKED_BY_ARCHITECTURE_AUTHORITY`。代码侧已无可安全继续的 S1～S4工作，
  任务转入 `VALIDATING`，等待 DEVX-001 独立收敛、owner 部署独立 ops checkout /
  credentials / 唯一 scheduler trigger，以及首个合法 provider-ready XNYS session
  运营验收。
- 2026-07-25：为避免把并发 DEVX-001/owner dirty change 签入 OPS-070 authority，
  按已登记生命周期建立 `D:\Work\AITradingSystem_ops070_closeout_20260725` clean
  worktree；从 `fc6313416d78f56a29519f41ca564eaa1f90e8ce` 只移植 23 个 OPS-070
  归属文件，并逐文件验证与主工作区 SHA-256 一致。task registry 在隔离树为
  `902 total / 413 active / 489 completed`，未包含 DEVX-001；DevEx fitness 为
  `1018 modules / 1184 test files / 856 direct writers / 0 violations`。
- 2026-07-25：追加纯 OPS-070 compatibility authority，历史 prefix
  `1,521,808 bytes / sha256=4afcb86b621d45c0dbd2120167ef23009512d2a31937f9e7321c215810aee803`
  与 `fc6313416` Git blob byte-identical，authority 仅在 EOF 追加，并显式排除
  `docs/research/growth_tilt_owner_diagnosis_pack.md`。正式验证最终 PASS：
  focused=`133`、architecture-fitness=`622`、contract-validation=`275`、
  integration=`994`、reproducibility=`23`、Full=`7,233 passed / 4 skipped /
  642 warnings / 1,173.83s`。S1～S4 engineering closeout 完成；任务保持
  `VALIDATING`，仅等待 owner 部署独立 ops checkout / scoped credentials / 唯一
  scheduler trigger，以及首个合法 provider-ready XNYS session 运营验收。
