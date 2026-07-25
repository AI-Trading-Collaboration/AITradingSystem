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
- 2026-07-25：真实部署验收首先证明两层 checkout guard 生效：共享
  `data/outputs` junction 的尝试被 `CHECKOUT_RUNTIME_ROOT_OUTSIDE` 在 provider request、
  cache/report mutation 前阻断；将完整 canonical runtime state 复制到独立 checkout 后，
  scheduler preflight PASS 且 release checkout 保持 clean。随后唯一外部入口
  `aits ops daily-run` 自动选择合法最新 XNYS session `2026-07-24`，但 capture 在任何
  source attempt 前被 `tracking_start=2026-07-27` 全局拒绝，33 个 DAG step 均取得终态，
  `secret_hygiene` 仍 PASS，provider request 为零。该日期门禁是非客观全局阻断，不是
  provider/DQ/PIT blocker；reviewed forward tracking 起点改为首个实际可验收 latest
  decision session `2026-07-24`。旧 FAILED state/ledger 保留，新 policy/spec 必须生成
  新 idempotency key 后从唯一 daily trigger 重跑。
- 2026-07-25：`tracking_start` 修复发布为 `8f88156f9c4f65dd921848ce86b0e1e4d523b09c`
  后，scheduler checkout preflight 与 33-step daily plan 均 PASS/READY，但唯一入口在
  provider 调用前返回 `RUN_CONTROL_BLOCKED_RETRY_EXHAUSTED`。根因是 canonical
  `WorkflowSpec.spec_id` 只绑定 scheduled step topology/commands，没有绑定会改变 capture
  资格与 source-control 行为的 `daily_input_capture` reviewed policy revision；因此
  tracking policy 修正仍错误复用了旧 FAILED key。最佳修复不是删除旧 state、提高旧
  attempt budget 或伪造新 key，而是把 scheduled policy version 与 capture policy version
  作为显式 runtime semantic revision 纳入 `WorkflowSpec` 哈希；reviewed capture policy
  同步升级版本。验收须证明旧 state/ledger 保留、新 spec/key 可追踪、未变 policy 的
  重触发仍受原 budget 约束，并再次只从 `aits ops daily-run` 执行真实 provider-ready
  session。
- 2026-07-25：semantic revision 修复发布为
  `e60ee55ee6fef18284294a2887e4e9ecbf7b7fba` 后，唯一入口取得新
  `workflow_spec_640b3af307093293ee1d` / run-control key，并完成 33 个 step 的真实
  终态记录。capture 对五个 component 均发起真实请求：
  `fmp_forward_pit`、`sec_companyfacts`、`fmp_valuation` 与
  `official_policy_sources` PASS，合计保留 86 个 component artifact refs；
  `market_macro` 在两个 reviewed attempts 后因
  `DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH` 保持 FAIL。manifest 为
  `PARTIAL_CAPTURE`，validation artifact 使用真实 contract
  `schema_version=daily_input_capture_validation.v1` 且自身状态 PASS，
  `consumer_cutover_allowed=false`。
- 2026-07-25：上述真实 run 同时暴露第三个非客观全局阻断：
  `_post_step_artifact_status_error` 把 capture validation 错误套用
  `schema_version=integer:1` 和 `report_type` 的 report artifact contract，因而把本应
  `LIMITED` 的 validated partial capture 误记为 FAIL，继续阻断了已经具备 PASS
  component 的 PIT/SEC consumer。最佳修复是让 JSON artifact gate 显式声明各 artifact
  自身的 schema version 与可选 report type，保持 as-of、status、
  `production_effect=none` 和 strict JSON 校验不变；同步升级 scheduled reviewed policy
  revision 以生成可审计的新 workflow spec/key。重跑必须复用四个 PASS source state，
  不重复 provider 请求；market/macro 的 exhausted integrity blocker 保留，DQ/score/
  Reader Brief 继续 fail closed，而 PIT/SEC sibling 必须实际执行并留下验证证据。
- 2026-07-25：capture validation contract 对齐与 `scheduled_tasks_v5` 工程验证 PASS：
  focused=`117 passed`、fast-unit=`340 passed`、architecture-fitness=`624 passed`、
  contract-validation=`275 passed`；严格 schema/as-of/status/production-effect 负向断言、
  partial-capture branch isolation、runtime-control 与 compatibility authority 均通过。
  下一步只通过唯一 `aits ops daily-run` 对同一 provider-ready session 取得新 spec/key，
  验证四个 PASS source state 幂等复用且 PIT/SEC branch 实际继续。
- 2026-07-25：release `c51804917d3a7dad2ba80ce65ecf56f0550100fa` 在独立 clean
  ops checkout 通过 preflight，并从唯一 `aits ops daily-run` 完成真实验收 run
  `daily_ops_run:2026-07-24:20260725T100612Z`。新
  `workflow_spec_5bbc63024052102cdef3` / `operations_run_d0cb72506a653878a6ee81b9`
  与旧 key 隔离，33/33 steps 取得 terminal 记录。capture 被正确登记为 `LIMITED`；
  五个 source state 均 `idempotency_reused=true`，没有重复 provider 请求；四个 PASS
  components 继续保留原 86 个 artifacts。
- 2026-07-25：真实 branch isolation 达到 S1 验收目标：
  `pit_snapshots_build_manifest`、`pit_snapshots_validate`、`sec_metrics`、
  `tsm_ir_sec_metrics_merge`、`sec_metrics_validation`、SEC PIT observe/monitor 均实际
  PASS；PIT validation 为 40 snapshots / 13,052 raw rows / 0 errors / 0 warnings，
  SEC companyfacts 为 17/17 companies / 0 errors / 0 warnings，最终 SEC metrics 覆盖
  196/198，只有 AMZN R&D annual/quarterly 两项 warning。`validate_data` 仅因
  `market_macro=FAIL` 被 component gate BLOCKED，`score_daily`、dashboard、Reader Brief
  与 finalization 继续 fail closed。`secret_hygiene` 扫描 30,543 files 并 PASS。
- 2026-07-25：任务保持 `VALIDATING`，不宣称 full operational PASS。剩余 blocker：
  market/macro 的
  `DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH (NOT_COMMITTED)` 需要 data platform
  owner 修复 canonical publication transaction binding；此外 `pipeline_health` 仍要求旧
  canonical `fmp_forward_pit_<as_of>.csv` / fetch report 路径，虽然同日 capture raw 与
  processed bytes 已保留，但当前 PIT manifest PASS 只覆盖
  `fmp_valuation_expectations` 40 snapshots，尚未证明 forward PIT component 被该 consumer
  contract 纳入。下一阶段必须对齐 capture-to-canonical PIT projection 与 health contract，
  并要求 source-specific coverage fail closed；不得把当前 PASS 扩大解释为 forward PIT
  consumer acceptance。
- 2026-07-25：剩余两项进入实现，根因和阶段验收冻结如下。
  1. publication blocker 不是 provider 数据冲突，而是 immutable
     `download_manifest.csv.output_path` 保存了发布 checkout 的绝对路径；canonical
     publication 被复制到隔离 runtime checkout 后，resolver 用当前 root 重算该字段，
     因而把同一 transaction 的合法 relocation 误判为 current-generation mismatch。
     修复必须保持 checksum、row count、source events、transaction id 和 artifact filename
     exact，只允许 resolver 对已由 transaction SHA 绑定的历史绝对 `output_path` 做
     filename-scoped relocation 校验；pre-commit 新 generation 仍要求当前 root 的 exact
     path，tamper 仍 fail closed。
  2. trading-day capture 已保留 17 个 FMP forward raw payload、日期隔离 normalized CSV
     与 fetch report；下游缺口是没有从 retained raw bytes 到 canonical
     `data/processed/pit_snapshots/`、`outputs/reports/` 和 canonical PIT manifest 的
     derived projection。新增步骤只能读取 PASS capture component 的已留存 bytes，
     重算 normalized rows、核对 capture normalized exact bytes、写 canonical projection，
     `provider_request_performed=false`；不得重复请求 FMP。
  3. PIT build/validate 与 pipeline-health 必须显式要求 `fmp_forward_pit` snapshot kind
     非空，不能再由同属 `fmp_valuation_expectations` source_id 的 analyst/history rows
     代替。缺该 kind 时 build/validate/health 均 fail closed。
  4. 为真实复验只重开已耗尽的 `market_macro` source/session attempt，capture policy
     使用 component-scoped reviewed source revision：四个未变 PASS component 延续原 revision
     并复用原 state；market/macro 以显式 superseding revision 归档旧 terminal state 后取得
     新 idempotency key。旧 state/ledger 不删除、不改写，PIT-sensitive source 不重抓。
  5. 完成 focused、fast-unit、architecture-fitness、contract-validation 后发布新
     scheduled/capture reviewed revisions；只从唯一 `aits ops daily-run` 触发一次真实复验。
     验收要求 canonical publication 可在 runtime checkout resolve、market/macro capture
     PASS、forward projection/required-kind/PIT validation PASS、pipeline-health 不再报告旧
     PIT 路径缺失；DQ/score/Reader Brief 仍按自身真实门禁决定，绝不补造结论。

## 临时 live-fix worktree 生命周期

- owning task：`OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG`
- absolute path：`D:\Work\AITradingSystem_ops070_livefix_20260725`
- purpose：修正真实验收暴露的 `tracking_start` 非客观全局阻断，并让 canonical
  workflow spec 显式绑定 scheduled/capture reviewed policy revisions；更新对应测试、
  任务状态、requirement、system flow、compatibility authority 与确定性生成清单；不得
  混入主工作区 DEVX/owner 变更。
- exit condition：focused/architecture/required validation PASS、commit/push 完成、独立
  ops checkout pin 到新 exact release commit 且真实 daily-run 取得可审计终态后，确认
  无唯一未提交证据或活动进程，再用 `git worktree remove` 清理并 prune。若验证或运营
  验收失败，保留 blocker evidence，并在本节追加风险、owner 与下一退出条件。

- 2026-07-25：两项 live-fix engineering 与 formal validation 完成。Publication resolver
  在 retained runtime publication 上成功解析原 transaction，仍拒绝 filename tamper；FMP
  forward retained replay 验证 17 个 raw payload、12,822 normalized rows，capture 与
  canonical bytes SHA-256 均为
  `771dde3a3856347574693fb71afb60358cd6ec8f85896c36e02ce9dd4e591fd4`，
  `provider_request_performed=false`。聚焦回归=`174 + 119 passed`，Ruff/diff-check PASS，
  fast-unit=`340 passed`，architecture-fitness 首轮仅因 generator 后测试 inventory id
  更新造成 stale manifest（`623 passed / 1 failed`），重建确定性清单后正式
  architecture-fitness=`624 passed`，contract-validation=`275 passed`。未用串行 pytest
  覆盖失败。当前仍不宣称运营 PASS；下一步发布 exact commit、pin 独立 runtime checkout，
  然后只调用一次唯一 `aits ops daily-run` 取得真实 DQ/PIT/score/Reader Brief 终态。
- 2026-07-25：release `23d4da3aaa547b9a07e920f01ebcf56f8ccbc910` 已推送并 pin
  到独立 clean runtime checkout；scheduler preflight PASS。唯一一次真实
  `aits ops daily-run --as-of 2026-07-24` 取得
  `workflow_spec_f9765ef710d1373267c5` /
  `operations_run_330847ca56adc301c4ee1d6d`，34/34 steps terminal，overall `FAILED`。
  `market_macro` 新 revision 真实 PASS，capture/strict DQ、SEC、freshness、schedule observe
  与 secret hygiene 均 PASS，证明 publication relocation 修复有效。没有 production/active
  shadow weight 或 broker/trading action。
- 同一 run 暴露新的 source-artifact ownership blocker：旧
  `fmp_forward_pit` terminal PASS state 的 21 个 artifacts 中，20 个 source-owned
  raw/normalized/report bytes 仍 exact，只有
  `data/raw/daily_input_capture/2026-07-24/pit_snapshot_manifest.csv` 从原 SHA
  `30abc0ad...` 变为 `3ea94279...`。该文件曾被旧 PIT consumer 扩展为跨-kind aggregate，
  不是 provider/raw drift；因此 component 被 `SOURCE_STATE_INVALID` 阻断，projection/PIT/
  score/report 未运行，pipeline health 诚实 FAIL。最佳修复是把 idempotent PASS reuse
  绑定到 component-owned artifacts，明确把会由 consumer 重建的 aggregate manifest 排除
  在 source-state reuse authority 之外；仍须要求 raw directory 的完整 path/SHA/size 集、
  normalized CSV 和 component reports exact，并在复用结果中披露 excluded aggregate。
  旧 state/manifest/run ledger 均不改写，不重抓 FMP，不重跑本次 daily key；修复后只做
  retained-state fault-injection 与正式工程验证，运营复验留给下一合法 scheduler run。
- 2026-07-25：artifact ownership 修复已落地到
  `daily_input_capture_policy.v5`。真实 retained-state 只读复验对
  `2026-07-24/fmp_forward_pit/state.json` 的 20 个 source-owned artifacts 全部 exact
  PASS，`artifact_reuse_scope=SOURCE_OWNED_ONLY`，唯一
  `excluded_non_authoritative_artifacts` 为 cross-kind
  `pit_snapshot_manifest.csv`；state SHA-256
  `ca76ee9172fe2ae154dae442d388846946bb6fec93b108e8dfd72b231107117e`
  前后不变，`provider_request_performed=false`。两条 fault-injection 同时证明 aggregate
  consumer mutation 可复用、normalized source tamper 仍 `SOURCE_STATE_INVALID`。
  正式验证为 focused=`114 + 68 passed`、fast-unit=`340 passed`、
  architecture-fitness 首轮 stale authority=`623 passed / 1 failed`，刷新 OPS-070
  current hash 后正式=`624 passed`、contract-validation=`275 passed`、Ruff PASS。
  当前工程 blocker 已关闭，但不宣称 2026-07-24 旧 run 转为 PASS，也不再调用
  `daily-run`；下一合法 XNYS scheduler run 必须真实通过 projection、PIT、score、
  dashboard、Reader Brief 与 finalization 后，OPS-070 才能完成运营验收。
