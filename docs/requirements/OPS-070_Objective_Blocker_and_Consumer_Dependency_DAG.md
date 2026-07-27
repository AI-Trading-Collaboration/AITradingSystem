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

## 2026-07-27 S4 稳定部署补强

### 新 blocker 与最佳方案

本轮只读审计确认 `D:\Work\AITradingSystem_ops_runtime_20260725` 虽然 clean、detached
且路径独立，但仍是开发仓库的 linked worktree，共享同一 Git common dir；其 HEAD
落后当前 local/remote main 50 commits。系统 `aits.exe` 还是 Python 3.11 全局 editable
entrypoint，实际 import 指向 `D:\Work\AITradingSystem\src\ai_trading_system`，旧 runtime
没有自己的 Python environment。仅修改 scheduler cwd 会继续执行开发 checkout 代码，
不能形成稳定隔离。

最佳方案是完成最小 serial public-contract wave：

1. release candidate receipt 冻结 owner-reviewed exact remote/main commit、该 exact
   commit 上的 `fast-unit`、`architecture-fitness`、`contract-validation`、
   `integration`、`reproducibility`、`full` 六类正式 PASS validation artifacts 及其
   SHA-256（集合不得缺失、重复或混入其他 tier）、关键 scheduler/policy/CLI hashes、
   previous release 和 `production_effect=none`；所有 commitment 使用 checkout-relative
   path，promotion 原子迁移六类 evidence 到 permanent runtime 后再从该 root live
   revalidate，不允许 active receipt 长期依赖可清理 development lane 的绝对路径；
2. deployment acceptance receipt 冻结独立 clone identity、runtime-local executable/import
   provenance、唯一 scheduler action/cadence/working directory/env contract、最小权限
   credential attestation 和 owner decision，不记录 secret value；
3. 显式 promote/rollback transaction 使用 promotion lock，先排除 active daily
   lease/process，再验证 candidate、fetch reviewed remote、切换 exact detached commit、
   建立/验证 pinned runtime environment，并在任何失败时恢复 previous accepted release；
4. scheduler mode 必须同时有 marker 与 active deployment receipt；缺任一项 fail closed。
   仍只保留现有 Codex automation `aitradingsystem-pit` 作为唯一 external trigger，不安装
   Windows Task Scheduler，也不允许 daily-run 隐式选择 latest main；
5. checkout clean 检查复用 target-bound、known-unrelated-aware audit；任何 excluded path
   不得被打开、hash、复制或修改。所有 activation 前 mutation 只产生
   `production_effect=none` 的工程/部署证据，不触发 provider、weights、broker 或 trading。

现有 linked runtime 和其中 153,363 个 ignored runtime files 保留为 canonical
运营证据源，不 reset/clean/delete；新独立 clone 只有在证据迁移、hash/identity 校验和
owner acceptance 均通过后才能成为 active runtime。下一合法 provider-ready XNYS session
仍是最终 operational acceptance，工程验证不得提前重跑 `daily-run`。

### 稳定性验收

- linked worktree、wrong common-dir、wrong origin/ref/ancestry、tampered receipt/config/hash、
  global editable executable、wrong import root、missing marker/receipt 均 fail closed；
- release validation tier 集合不完整、重复、未知或任一 artifact 的 `git_commit` 与
  candidate commit 不同均 fail closed，不得用旧提交的 PASS 或 focused-only 结果背书；
- required critical path 集合不完整/重复、commitment 越出 checkout、promotion evidence
  copy 冲突/漂移、runtime installed-distribution inventory/fingerprint 缺失或变化均
  fail closed；
- missing/duplicate/disabled/wrong-action scheduler entry、credential over-scope 或 secret
  value 泄漏均 fail closed；
- promotion 与 active daily lease/process 冲突时不切换；prepare/switch/verify 任一中断
  可恢复 previous release，并保留 immutable transaction evidence；
- repeated promotion/rollback 幂等；旧 state、ledger、capture bytes、DQ/PIT/report evidence
  不删除、不改写，ignored runtime data 不丢失；
- preflight/diagnostic 写入顺序遵守 WRITE guard，known-unrelated exact exclusion 不被读取；
- focused、Architecture、Contract、Integration、Reproducibility 与 Full parallel validation
  在最终 integration candidate 通过；生成视图只由 coordinator 在最终树刷新一次；
- automation 原位切换前保持当前 external state，不产生第二 scheduler；切换后先做纯
  identity/deployment preflight，再等待下一合法 session 的唯一 daily-run 验收。

### 稳定部署 workspace 生命周期

- owning task：`OPS-070_OBJECTIVE_BLOCKER_AND_CONSUMER_DEPENDENCY_DAG`
- temporary development lane：
  `D:\Work\AITradingSystem_ops070_stability_dev_20260727`
- purpose：从 exact local main 建立 clean `SINGLE_LANE` worktree，实现和验证上述 serial
  contract wave；不得混入当前 DATA-GOV-002C2P checkout 或 excluded owner research。
- exit condition：最终 candidate 通过正式验证、local main fast-forward、remote main 普通
  push、独立 runtime 部署证据完成后，审计无唯一未提交/ignored evidence或活动进程，
  使用 `git worktree remove` 清理并 `git worktree prune`。失败时保留并在本节记录
  blocker、risk、next owner 与下一退出条件。
- permanent runtime clone：`D:\Work\AITradingSystem_ops_runtime`
- purpose：独立 Git common dir、owner-approved exact release、runtime-local pinned Python
  environment、唯一 Codex automation 的长期运行根；不得通过 junction 共享开发 checkout
  的 data/outputs，不得使用全局 editable `aits`。
- exit condition：只有 owner 明确退役，且全部 canonical state/ledger/data/outputs/
  deployment transactions 已迁移并逐项校验、无 active process/lease/scheduler 引用、
  previous release 与 rollback evidence 有受治理归宿后才可删除；否则永久保留。

### 2026-07-27 实现与验证进度

- 已完成 release candidate、transactional promotion/rollback、deployment acceptance、
  independent Git common-dir、runtime-local Python/import provenance、唯一 Codex scheduler
  observed-state、credential name-only attestation、WRITE guard before preflight write 与显式
  `--manual-execution` 合同；release receipt 额外要求六类 required validation tier
  集合 exact 且每个 PASS artifact 的 `git_commit` 与 candidate commit 完全一致，禁止
  拿旧提交、缺失 tier 或 focused-only 的 PASS 为新 release 背书；commitment 已改为
  checkout-relative，promotion 会迁移 validation evidence 并在永久 runtime 重验；required
  critical path set 与 installed-distribution environment fingerprint 也已固定；
  相关 focused regression 为 `96 passed`，追加 credential/
  canonical candidate evidence 后的 scheduler/promotion focused 为 `22 passed`，最终树将
  统一复验。
- 初次门禁定位 stale authority 后，task shadow 与 DevEx generated views 已由 coordinator
  刷新；task registry=`917 tasks / 420 active / 497 completed / 55 ambiguous legacy rows /
  131 consumers`，DevEx=`1033 modules / 1200 tests / 0 ownership violations`。本进度记录会
  改变 task source bytes，因此在 final tree 还需执行最后一次 deterministic refresh；
  此后不再在 lane 中间反复刷新。
- clean lane 缺少四个被 Git ignore、但 bootstrap handoff 明确绑定的历史 validation
  artifacts。按 handoff 的 exact path/SHA 从主 checkout 复制到 lane 后才允许生成 task
  registry；四个 SHA 分别为
  `5afc81...`（fast-unit）、`a7c070...`（architecture-fitness）、
  `6994b8...`（contract）和 `1785c2...`（full）。它们仅用于 deterministic
  generated-view bootstrap，不进入 tracked change，也未读取 known-unrelated exclusion。
- `fast-unit` 已在刷新 task registry 后正式通过 `340 passed`，artifact 为
  `outputs/validation_runtime/fast-unit_20260727T022631Z/test_runtime_summary.json`。
  首轮 Architecture 为 `703 passed / 36 failed`，失败均为预期的 DevEx manifest stale
  与 append-only compatibility current-hash authority drift；未降级门禁，已运行 canonical
  DevEx generator，并正在追加新的 OPS-070 current authority 后重跑。
- 当前状态转为 `VALIDATING`。在 formal Architecture/Contract/Integration/
  Reproducibility/Full、local-main fast-forward、普通 push、独立 clone evidence migration、
  runtime-local environment、automation 原位切换与 active deployment receipt 全部闭合前，
  不得把 stable deployment 写成 accepted；下一合法 daily scheduler run 仍是运营终验。

### 2026-07-27 runtime ignore contract blocker

- candidate `992734147b4e25a300694f07c1d7323d37641501` 已完成六类 exact-commit PASS
  validation、local-main fast-forward 与普通 push；Full=`7547 passed / 5 skipped`。
- 独立 runtime 已建立且 152,402 个旧 ignored files（36,596,615,302 bytes）完成逐文件
  SHA-256 迁移，旧 linked runtime 保留。首次迁移 attempt 因并发 target parent
  `Path.resolve()` false negative 失败关闭；reparse scan=0，第二 attempt 完整重验后 PASS。
- candidate checkout 复核发现其中 8,129 个文件（5,817,538,553 bytes）只因旧共享
  `.git/info/exclude` 才被视为 ignored；独立 clone 未继承该隐藏 local state，因此
  checkout guard 正确将它们列为 dirty。直接复制旧 exclude、添加未审计
  `.git/info/exclude`、放宽 clean gate、junction 或删除历史 state 都不是可接受方案。
- 最佳直接修复是追加最小 serial contract wave：reviewed policy 固定 runtime-only
  Git exclude patterns（仅 `/outputs/`、`/artifacts/`、`/data/derived/`），installer 只能
  在 independent clone 的 exact Git path 原子写入 canonical managed block；任何 existing
  unknown line、缺失/额外 pattern、path/common-dir drift 都 fail closed。deployment receipt
  与 scheduler preflight 必须冻结并 live revalidate exclude path/bytes/SHA-256/patterns。
  Dev checkout 的 `.gitignore`、known-unrelated exclusion 与 dirty semantics 均不改变。
- 该 contract 通过新 candidate 的 focused/Architecture/Contract/Integration/
  Reproducibility/Full 后，才允许安装 runtime exclude、重验 migrated state、promotion、
  automation 切换与 activation。当前仍 `VALIDATING`，无 daily/provider/weights/broker/
  trading effect。

### 2026-07-27 cross-release promotion policy blocker

- runtime-only exclude contract candidate
  `f462836e3d599ad7e718a487045c3cc1d2ed20a8` 已通过
  Fast=`340`、Architecture=`743`、Contract=`275`、Integration=`995`、
  Reproducibility=`23`、Full=`7550 passed / 5 skipped`，并已 fast-forward/push，
  `local main = origin/main = candidate`。permanent runtime 的 exact exclude installation
  receipt=`ops_runtime_git_exclude_8f16227e8bb4f4715f0cb0111cad901f5acfef76`，
  migrated 8,129 个 runtime paths 随后由 runtime-self audit 证明 clean。
- 第一次真实 promotion 从已接受旧 release
  `925315059b88ee781e9dae7960d232714a610566` 启动时，在 checkout 切换、evidence copy
  和 transaction event 之前 fail closed。根因是 `_governed_dirty_paths` 从旧 runtime
  tree 读取 `arch_005_s4d_checkout_guard_policy.v1`，而新 promotion implementation
  只接受当前 policy schema；runtime HEAD 未改变、promotion lock 已释放、
  scheduler 未激活。初次非递归 inventory 曾把 transaction file count 误报为 0；
  递归复核确认系统按既有异常路径留下一个 immutable
  `04_ROLLED_BACK.json` no-op rollback event（`previous_commit=null`），但没有
  `PREPARED`、`SWITCHED` 或 candidate evidence。该更正保留失败审计，不把 no-op
  rollback 误述为 checkout 已发生切换。
- 最佳直接修复是让 pre-switch dirty inventory 使用已验证 coordinator candidate 的
  checkout-guard policy，同时把 audited repository 明确绑定为 permanent runtime。
  runtime 自带旧 policy 不得成为跨 release 切换的解析 authority，但 target dirty paths、
  known-unrelated exclusions、unstaged/staged diff checks 仍必须完整执行。禁止以手工
  checkout candidate、跳过 clean gate、临时复制 policy 或捕获 schema error 后继续作为
  workaround。
- 新 regression 必须建立真实 ancestor runtime commit，其旧 checkout policy schema 与
  candidate 不同，并证明 promotion 可从该旧 release 事务式切换；同时注入 runtime
  dirty path 证明 coordinator policy 不会降低 fail-closed dirty gate。该修复必须追加
  immutable compatibility section、重新生成 views、形成新的 exact-commit 六档正式
  validation 与 release receipt，之后才重试 promotion。当前仍 `VALIDATING`，
  `production_effect=none`，无 provider/weights/broker/trading action。

### 2026-07-27 post-switch runtime self-containment blocker

- candidate `a994a9d03028263d00d1682a39bcdded93c58c47` 已通过 Fast=`340`、
  Architecture=`747`、Contract=`275`、Integration=`995`、Reproducibility=`23`、
  Full=`7555 passed / 5 skipped`，并完成 local-main fast-forward 与普通 remote push。
  第一次晋升重试因 coordinator 临时 `PYTHONPATH` 被 runtime probe 子进程继承而
  `RUNTIME_PACKAGE_OUTSIDE_CHECKOUT`，事务已回滚且保留 immutable event；清除调用进程的
  临时路径后，第二次晋升到 `PROMOTED_NOT_ACTIVATED`，runtime self-audit 为 clean。
- 部署前复核进一步确认：post-switch `inspect_runtime_provenance` 仍从 temporary
  development lane 读取 checkout policy。即使 runtime release 完全自足，按既定 exit
  condition 清理该 lane 后，active receipt 的 live validation 也会因开发目录缺失或后续
  policy drift 失效。把临时 lane 永久保留会违反本节生命周期，也会制造新的隐藏 scheduler
  依赖，不能作为 workaround。
- 最佳直接修复是把 pre-switch audit 与 post-switch audit 分开：promotion 在切换前继续
  使用 exact coordinator candidate policy 审计旧 runtime；切换成功后，runtime provenance
  必须只使用已切换 exact release 自身的 checkout policy，development root 仅证明 Git
  common-dir 独立。同时 runtime probe 必须从子进程环境移除 `PYTHONPATH` 与
  `PYTHONHOME`，防止 coordinator import path 污染 runtime-local executable 证明。
  regression 必须证明 retired/invalid development policy 不影响 post-switch self-audit，
  并证明 probe environment 不携带两项 Python path override。完成新的 exact-commit 六档
  正式门禁、release/promotion/acceptance 后，才可清理 temporary lane、激活唯一 automation；
  当前保持 `VALIDATING`、`scheduler_activation=false`、`daily_run=false`、
  `production_effect=none`。
