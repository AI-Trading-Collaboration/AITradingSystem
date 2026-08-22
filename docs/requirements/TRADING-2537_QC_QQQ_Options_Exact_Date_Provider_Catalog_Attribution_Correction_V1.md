# TRADING-2537 — QQQ Options exact-date provider catalog attribution correction V1

- priority: `P0`
- status: `DONE`（V2 attribution terminal 已实证 RESOLVED；durable repair 转入 TRADING-2541）
- owner: Codex capability coordinator（evidence closure）；TRADING-2541（subscription/transport repair）
- governed mode: `SINGLE_LANE`
- contract change: `true`（provider attribution 的 consumer-visible typed contract）
- registration base: `fb246ab362e6942e3f4948c1e1cd9247212f9897`
- production effect: `none`
- broker action: `none`

## 1. 问题与目标

TRADING-2532 已证明 `2021-02-22..2025-12-02` 的 1202 个 XNYS session 中存在
`1201 PRESENT / 1 MISSING`，但封存 aggregate 没有导出唯一 never-chain 日期。TRADING-2535
因此建立了 zero-order、export-safe attribution proposal，其候选代码在每个 chainless Slice 中
调用 `QCAlgorithm.option_chain(canonical_option_symbol)`，并按返回 contract count 直接归因为
provider catalog `AVAILABLE` 或 `EMPTY`。

该 helper 不是 exact-date availability authority：LEAN 的 chain history 路径在当前 universe 文件
不可用时可以返回最近交易日的可用 universe。2535 没有检查返回数据的 source/end date，因此可能把
前一交易日的 catalog 误归因为目标 session 可用，也会在约 1020 个曾出现 chainless Slice 的 session
上重复执行不必要的 provider history probe。现有静态测试只确认 helper token 存在，没有证明
cross-date fallback 会 fail closed。

本任务作为不可回退 TRADING-2535 sealed proposal 的 successor serial correction，生成独立的
exact-date V2 sealed package。它只修复诊断器和 typed attribution contract，不改写 2532～2536
历史证据，不访问 QuantConnect，不修改 Cloud project，不运行 backtest，不读取或导出 raw option rows。

## 2. 权威输入与不变量

- predecessor local main：`fb246ab362e6942e3f4948c1e1cd9247212f9897`；
- source backtest id：`acf111f24d09a41870f9a23e93fcbe3b`；
- requested/evaluated research range：`2021-02-22..2025-12-02`；
- exchange calendar / exact session count：`XNYS / 1202`；
- expected final never-chain count：`1`；
- 2535 policy、run scope、proposal、project code 与 package manifest bytes 均保持 immutable；
- 2536 Atlas classification 作为历史 proposal/unexecuted 证据保留，不冒充 2537 已执行；
- DQ/PIT、selection、engine、orders、fills、portfolio、production 与 broker 边界不变。
- LEAN source authority：
  [`BaseChainUniverseData.EndTime`](https://github.com/QuantConnect/Lean/blob/master/Common/Data/UniverseSelection/BaseChainUniverseData.cs)
  明确定义为 `Time + OneDay`，构造器以 source file `date` 初始化 `Time`；
  [`OptionUniverse`](https://github.com/QuantConnect/Lean/blob/master/Common/Data/UniverseSelection/OptionUniverse.cs)
  继承该语义。

任何 predecessor hash、window、session identity、package inventory 或 safety flag drift 必须 fail closed。

## 3. Corrected exact-date probe contract

候选代码必须分两阶段执行：

1. `on_data` 只记录每个 expected session 的 Slice count、canonical QQQ equity Slice presence 与
   subscribed `Slice.option_chains` non-empty event count；不得在 chainless Slice 中调用 provider history；
2. `on_end_of_algorithm` 先证明 observed session count=`1202` 且 unique final never-chain count=`1`，
   然后仅对该目标日期执行一次 bounded `History[OptionUniverse]` exact-date query；
3. V2 只把 `OptionUniverse.time.date()` 等于目标日期、且
   `OptionUniverse.end_time.date() = OptionUniverse.time.date() + 1 day` 的日级 record 纳入
   exact-date count；`Time` 是 source trading date，`EndTime` 只证明该日级数据的次日可用时间。
   任何真正来自其他 source date 的结果或 availability invariant 异常都必须记录为 cross-date
   fallback 并 fail closed，不得归类为目标日 `AVAILABLE`；
4. 目标日期需要同时导出其窗口位置 `START_BOUNDARY / INTERIOR / END_BOUNDARY`，为后续 pre-roll、
   subscription repair、provider remediation 或 post-roll 路径提供有界证据；
5. provider probe 必须是 count-only：不导出 symbol、contract identifier、strike、expiry、right、
   bid、ask、last、quote、Greeks、IV、OI、volume、raw rows、异常正文、日志载荷或 Object Store 内容。

provider query 的 evaluated interval 必须覆盖目标 session 且不改变 research conclusion window；实际
requested/evaluated date range 必须在 bounded terminal statistic 中显式记录。

## 4. Typed attribution contract

只允许以下归因：

- `EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING`：存在 source date 精确匹配的
  `OptionUniverse` record，exact-date contract count > 0，且 subscribed chain event count = 0；
- `EXACT_DATE_CATALOG_EMPTY`：存在 source date 精确匹配的 record，且 exact-date contract count = 0；
- `NO_EXACT_DATE_PROVIDER_EVIDENCE`：没有精确匹配 record，或只发现跨日 fallback；
- `PROVIDER_PROBE_ERROR`：目标日 bounded query 抛出异常，仅导出稳定 reason code；
- `ATTRIBUTION_INDETERMINATE`：session/window/identity/count 不完整，或不满足以上 exact 条件。

`execution_terminal=COMPLETE` 只表示 session/account/safety 边界完整；必须另行导出
`attribution_terminal=RESOLVED / INDETERMINATE / ERROR`，不得把运行完整误写为归因已解决。以上状态
仅用于 provider availability / subscription transport 归因，不改变当前 `chain_presence=FAIL`、
DQ=`FAIL`、PIT=`NOT_EVALUATED` 与 `POLICY_BLOCKED_CASH_PRESERVATION`。

## 5. 实施步骤与归属

### S0 — Registration / contract freeze

- 建立 canonical task row 与本 requirement；
- 以 exact local-main base 通过 governed `SINGLE_LANE` start/lane preflight；
- 本文即最小 serial consumer-contract correction，2535 sealed bytes 不改写。

### S1 — Offline V2 sealed package

Task-owned：

- `config/research/qc_qqq_options_exact_date_provider_catalog_attribution_correction_v1.yaml`；
- `config/research/qc_qqq_options_exact_date_provider_catalog_attribution_correction_v2.yaml`；
- `src/ai_trading_system/qqq_options_research/exact_date_provider_catalog_attribution_correction.py`；
- `tests/test_qqq_options_exact_date_provider_catalog_attribution_correction.py`；
- `inputs/research/qqq_options/trading_2537_exact_date_provider_catalog_attribution_correction_v1/**`；
- `inputs/research/qqq_options/trading_2537_exact_date_provider_catalog_attribution_correction_v2/**`；
- 本 supporting requirement 与 task-specific architecture fragments。

Coordinator-owned：canonical task registry/index、generated task views、`docs/system_flow.md`、
Atlas page-effectiveness classification/reader summary、generated architecture/compatibility/deprecation
authority 与 formal validation artifacts。

V1 已成为 executed predecessor，其 policy、run scope、proposal、candidate `main.py`、unsigned owner
request 与 package manifest bytes 必须保持 immutable。V2 生成独立 deterministic package，绑定 V1
package/project-code exact identities 和 2532 evidence identities；本轮不得增加任何 external counter。

### S2 — Validation / publication

- unit tests 至少覆盖 exact-date contracts available、exact-date empty record、no record、prior-date-only
  fallback、provider error、target count 非 1、session count 不完整、package replay/tamper rejection；
- candidate compile，静态拒绝 `self.option_chain(...)`、orders、logs、Object Store、network 与 raw fields；
- focused pytest 使用 `-n 16 --dist loadfile`；运行适用 Architecture、Contract、Integration、
  Reproducibility 与 Full final-tree validation；
- 更新 `docs/system_flow.md` 与 Atlas successor classification，明确 proposal ready / external execution
  unexecuted；
- 完成 task terminal event、task branch commit、local-main ff-only、ordinary non-force push、SHA verify
  与 branch cleanup。

## 6. 外部动作与后续真实修复边界

本节的历史 exact-token gate 由 DEVX-008 successor policy 保留为历史事实，不再是 V2 当前执行的唯一
授权形式。V2 corrected package ordinary-pushed 后，Project Owner 已要求继续该诊断且 exact manifest
固定 existing clone、zero-order 和 action maxima，因此当前一次运行属于
`R1_BOUNDED_RESEARCH_SANDBOX / STANDING_OWNER_SCOPE`。Codex 自动重放 final exact hashes，不再要求
Owner 将机器生成的 hashes 粘贴回对话。

在 DEVX-008 policy ordinary-pushed 前：

- `maximum_project_mutations=0`；
- `maximum_cloud_backtests=0`；
- `maximum_orders=0`；
- `maximum_fills=0`；
- `external_action=none`。

publication 后当前 standing scope 只允许 clone `35444189` 的一次 V2 mutation/save/automatic build、
一次 zero-order backtest 和一次 provider query；原 project mutation/new clone/orders/fills 均为 0，
禁止自动 retry。authorization state 与 technical evidence state 必须分轴记录；结果是否准入由 exact
identity、runtime provenance、terminal completeness 和可复现验证决定。

未来一次获批 zero-order run 只负责定位目标日期和归因。若目标是 start boundary，后继可提出一个
pre-roll trading session 但 evaluation 仍从 `2021-02-22` 开始；若是 end boundary，可提出一个 post-roll
session 但 evaluation 仍止于 `2025-12-02`；若 interior exact-date catalog available，修复 subscription /
transport；若没有 exact-date provider evidence，必须走 provider remediation/backfill 或治理后的替代
primary source，不得 forward-fill、静默排除该日或把 derived seal 冒充 provider evidence。

## 7. 生命周期

- 任务分支：`codex/trading-2537-exact-date-provider-attribution-correction`；
- 本任务不创建额外 worktree、clone、cache 或 supervised-run workspace；
- 当前 checkout 的 known-unrelated exclusion
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、stage 或修改；
- 生成的 sealed package 是 canonical governed evidence，任务关闭后保留；task branch 在普通推送和
  unique-content audit 后删除，可从 local/remote main 恢复。

## 8. 进度记录

- 2026-08-20：READ_ONLY 排查确认 2535 `option_chain` probe 缺少 source-date 校验并可能跨日回退；
  Project Owner 指示继续推进修复。本任务登记为 P0 serial correction，external action 仍为 none。
- 2026-08-20：corrected candidate 已实现：`on_data` 不再查询 provider；完整 1202-session scope 与
  unique target 成立后，只执行一次 `History[OptionUniverse]` target-date query；每个返回 collection
  必须以 `end_time.date()` 精确匹配目标日期，prior-date-only 与 mixed-date response 均 fail closed 为
  `NO_EXACT_DATE_PROVIDER_EVIDENCE`。execution 与 attribution terminal 已分离。
- 2026-08-20：V2 sealed identities：policy file/canonical SHA-256=
  `405e09dbdc58d7037e35de4d047bf4b80f9ced7030e69df96e56c727fb1af8c9` /
  `5abae42535973e59f5064288e091e9c9ddcfdab416bb5eb7e9a40fc321c03229`；run scope
  content/canonical SHA-256=`dc83b410fcb844e6c05193f81b6c46e10359c9cb5af0a2eb83fcf6a26d9a2019` /
  `132f1f8e82d73db8b77e8dd69daced2b12a39ad6cb9d45d365ef46fdbcc60f0a`；proposal
  content/canonical SHA-256=`7ecfda585fb1c84b4967193b624310292bd2efac55ce22e54fa19c79101f95a7` /
  `82a70010a7231ffeb15de833a2926c5032b39fba389af172341ba4d7a79609dd`；project code=
  `26223 bytes` / `86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4`；
  manifest content/canonical SHA-256=`d2cfac9c2b66a9e3e8203537cb2ed2a9bcec5ef6a7d17c9e8d40eee41c4c8737` /
  `9beffa28d1cfcd97548fc14b025dbb05e3e88f4f0ca35f66c6cfd37dbea65118`。
- 2026-08-20：candidate/fallback/package/2535 immutability/Atlas/authority focused 最终为
  `87 passed`（另有 local canonical ignored artifact exact-commit test 按设计留到 candidate commit
  后重建）；Ruff PASS；Architecture 首轮 `862 passed / 3 failed` 与第二轮
  `864 passed / 1 failed` 都是新增 task/module 后的 explicit snapshot/frozen inventory refresh，失败
  artifacts 保留在 `architecture-fitness_20260819T183422Z` 与
  `architecture-fitness_20260819T184611Z`。生成 authority 修正后 Architecture=
  `865 passed`，Contract=`276 passed`，Integration=`995 passed`，Reproducibility=`24 passed`。
- 2026-08-20：final committed tree 的 Full authority 固定为
  `outputs/validation_runtime/trading_2537_full_final_v1/test_runtime_summary.json`；该 artifact 缺失或
  status 非 PASS 时不得进入 local-main integration。当前 external counters 仍为 `0/0/0/0`，没有
  QuantConnect、Cloud、browser、raw rows、Object Store、orders、fills 或 broker action。
- 2026-08-20：提交 `64cd439c4d5aa2c7d73c129689baf0a0259e3002` 的首次 Full 为
  `9243 passed / 4 failed / 3 skipped / 643 warnings`，parent artifact 即上述
  `trading_2537_full_final_v1/test_runtime_summary.json`。四项失败均为生成权威新鲜度缺口：一次
  ARCH-004E aggregate shadow 未在 architecture fragment 尾部格式修正后重建，另三项来自
  DEVX-006C compatibility authority 尚未吸收本任务对 `docs/system_flow.md`、deprecation test 与
  canonical task-source fragment 的当前哈希。exact-date probe 行为、2535 immutable predecessor、
  Atlas exact-commit 页面和其余 9243 项均未失败。修复必须重建并验证这两条 authority，创建新的
  final commit，按新 commit 重建 ignored Atlas sidecars，再以该失败 artifact 为 parent 运行完整
  `failure_fix_rerun`；不得用 focused PASS 代替 Full。
- 2026-08-20：authority-refresh commit `ac432c5cf458f5f5d70c080c71124a8286abfc91`
  的首次 parent-bound Full rerun 为 `9246 passed / 1 failed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/trading_2537_full_failure_fix_rerun_v1/test_runtime_summary.json`。此前四项
  新鲜度失败全部通过；唯一新暴露项是 `tests/test_devx_006d_report_catalog_flow_authority.py` 的
  compatibility-consumer 冻结断言仍保留旧 `entry_count=2956`，与同文件及已验证 authority 的当前
  `2960` 不一致。修复仅允许把该精确 consumer 断言同步为 `2960`，刷新由该测试哈希影响的 DevEx /
  compatibility authority，创建新 final commit 并重建 ignored Atlas sidecars；随后必须以本次失败
  artifact 为 parent 再运行完整 Full。
- 2026-08-22：TRADING-2539 的唯一 Cloud run 定位 missing session=`2022-08-26`，但 V1 runtime 把
  唯一返回 record 的 `EndTime=2022-08-27` 当作 source date，输出
  `CROSS_DATE_FALLBACK / NO_EXACT_DATE_PROVIDER_EVIDENCE`。复核 LEAN
  `BaseChainUniverseData` 后确认日级 universe 的 `EndTime` getter 固定返回 `Time + OneDay`；因此该
  terminal 是 V1 探针实现的确定性误判，不是 provider 缺少 2022-08-26 数据的证据。
- 2026-08-22：V1 package 与 2539 execution evidence 保持 immutable；同一 non-terminal task 以
  append-only V2 serial correction 继续。V2 使用 `OptionUniverse.Time` 归属 source trading date，
  同时要求 `EndTime.date = Time.date + 1 day`，并保留原有 count-only、zero-order、单 query、typed
  terminal 与 export-safe 边界。创建分支时 `git switch` 仅打印 known-unrelated exclusion
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 的路径名和 `M` 状态；未读取、hash、diff、stage
  或修改其内容，按 audit incident 记录，后续 repository-wide inspection 只使用 governed audit。
- 2026-08-22：V2 sealed identities：policy file/canonical SHA-256=
  `ae027dbe396fee789b84b67362c2d9ba1f0f6ffdfeee3ae1f5fa730983ac4d02` /
  `b6eaa91520a73aa74450c3b5c2725a9e0848bb8484062d35f60e9749b1868520`；run scope
  content/canonical SHA-256=`d2c0e124b27777f0492f11912dd3c10ba1a9c4be11e16f6dfd9b0955b6cfb280` /
  `04376415dd9f6310aa796a930465f875d51effd6f681cfb2ac9da21bbde7191a`；proposal
  content/canonical SHA-256=`bd4ae649dd98baac7a1bdda3d6c21bba236a3f2fb852b0ad2fbdda9351e98893` /
  `cab386f66151ed7e9d48b46d8c03fac6f81424f707aaed8766e58b4c002d2fff`；project code=
  `26587 bytes` / `06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2`；
  manifest content/canonical SHA-256=
  `03d0107a8de280781b3742e3deac653cdbb92730b65b6808c16d1aed8d611bd2` /
  `3d7af489e4c6dabe710b045a94152f81c21f65bd922f7e101119f24fb18f713d`。目标固定为
  existing clone `35444189`；原项目 `34808569` mutation 上限为 0，新 clone 上限为 0。
- 2026-08-22：V1/V2 focused + historical execution + task-source parallel suite=`48 passed`；Ruff、
  package replay、V1 immutable golden 与 task-source validate PASS。Architecture 首轮=
  `864 passed / 1 failed`，唯一失败是修改 module/test/system-flow 后三个 ARCH-004E generated
  manifest stale；官方 generator 刷新后 architecture fitness=`PASS / violation_count=0`，完整 final
  Architecture rerun=`865 passed`。Contract=`276 passed`、Integration=`995 passed`、
  Reproducibility=`24 passed`。Full 必须绑定本轮 committed final tree 后才能 ordinary publication。
- 2026-08-22：commit `96579e79cf2b377a289d5f4ef655cfb642dd6c78` 的首次 V2 Full 为
  `9253 passed / 13 failed / 3 skipped / 644 warnings`，parent artifact=
  `outputs/validation_runtime/trading_2537_source_time_v2_full_final_v1/test_runtime_summary.json`。
  13 项失败全部属于新增 `system_flow` 与 TRADING-2539 后的 DEVX-006D/DEVX-006C/Atlas 生成权威
  新鲜度：V2 行为、V1 immutable evidence 与 9253 个其余测试均未失败。修复范围固定为更新
  `system_flow` seal、Atlas 2537/2539 reader contract、对应冻结断言，并按官方顺序重建 report-flow、
  compatibility 与 ARCH-004E authority；随后必须创建新 commit，以该失败 artifact 为 parent 运行完整
  `failure_fix_rerun`，不得用 focused PASS 代替。
- 2026-08-22：首次 5-file authority/Atlas focused rerun 为 `36 passed / 24 failed`：2539 canonical
  task 的结构化 `requirement_refs` 为空使 Atlas fail closed，且 compatibility build 早于最终
  ARCH-004E test-manifest refresh。通过官方 task-source append-only update 补齐 requirement binding，
  再按 `ARCH-004E -> report-flow validate -> compatibility build/validate` 重建后，同覆盖为
  `58 passed / 2 failed`；剩余一项是 renderer 冻结计数仍为 59（当前应为 60），另一项是本机 ignored
  canonical Atlas sidecar 必须绑定即将创建的新 commit。冻结计数同步后，同覆盖排除 ignored-sidecar
  exact-commit test 为 `58 passed / 1 failed`，进一步暴露 renderer 的 audit 列表仍硬编码到 2538；该
  共享上界已扩展到 2539。sidecar 只允许在 final commit 后重建，不得修改 tracked source 或代签人工
  acceptance。最终 tracked 5-file 原失败面（排除必须等 commit 的一项）=`59 passed`。
- 2026-08-22：authority-refresh commit=`ef05618d8c3b2d5e45750dfcf7731e0b5577441f`；ignored Atlas
  page/sidecars 已绑定该 exact commit，historical exact-commit test=`1 passed`，三条独立 acceptance
  原样保持 `NOT_EXECUTED / PENDING_REVIEW / PENDING_REVIEW`。以首次 13-failure Full 为 parent 的
  `failure_fix_rerun` 为 `9264 passed / 2 failed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/trading_2537_source_time_v2_full_failure_fix_rerun_v1/test_runtime_summary.json`。
  两项失败仅为运行期间 local `main` 与 `origin/main` 被并发推进到
  `a55ebb43778cf6579e1086d62743481b40ecc019`，使 Wave14/15 carrier 检查对 frozen-lane HEAD 报
  `CARRIER_PUSH_DRIFT`；V2、原 13 项 authority/Atlas failure 与其余 9264 项均通过。不得修改测试或
  回退 remote-tracking ref；按 ARCH-005 base-drift 流程生成真实 frozen-base/lane-head/latest-main plan，
  在 latest-main coordinator candidate 重建共享 authority 和 ignored Atlas sidecars后，以本次两项失败
  artifact 为新 parent 完整复跑 Full。
- 临时 integration drift workspace 固定为
  `outputs/validation_runtime/trading_2537_source_time_v2_integration_drift_v1/`，owner=TRADING-2537，
  purpose=保存 ignored `change_manifest.json` 与 `integration_revalidation_plan.json` 直到 final candidate
  integration；exit condition=final main ordinary push/SHA verify 后确认无唯一证据再删除，未完成前不得
  创建第二个 drift workspace。
- 2026-08-22：validated drift plan id=`integration-revalidation-2b87125fffe202420ffe`、SHA-256=
  `2b87125fffe202420ffef10ce226dd28897659408b473d0a64a938573b707a47`，decision=
  `RECONCILIATION_REQUIRED`。精确 overlap 仅为 generated `docs/task_register.md`（planner 分类
  `DOMAIN_OVERLAP`）与 `inputs/architecture/arch_005_task_registry_index.yaml`（`COORDINATOR_REFRESH`）；
  reviewed reconciliation 只允许在 latest-main candidate 上从 canonical fragments 重建二者，不手工
  合并 generated bytes。创建 integration branch 时 `git switch` 再次只打印 known-unrelated exclusion
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 的路径名与 `M` 状态；未读取、hash、diff、stage 或
  修改其内容，作为第二次 audit incident 记录。
- 2026-08-22：在 `a55ebb43778cf6579e1086d62743481b40ecc019` integration candidate 的聚焦验证期间，
  local `main` 与 `origin/main` 又由并发 TRADING-2540 推进到
  `309db390f9010e3b2801d79dbac9ab1b833b8e45`。新增 mainline delta 只更新 TRADING-2540 requirement、
  canonical task fragment/views 与 task-count test；不得在旧 candidate 上绕过 Wave14/15 carrier drift。
  该 candidate 对最新 registry 的 Atlas fail-closed 结果同时证明 TRADING-2540 尚未被 successor policy
  分类，因此协调修复将其登记为与 QQQ options 缺链归因分离、仅 preregistration、data-lane blocked 的
  disclosed successor，并把 Atlas task coverage 从 60 更新为 61；这不授权 2540 cache/empirical/backtest
  动作，也不扩大 2537 的 QuantConnect 权限。Atlas focused=`25 passed`；旧 HEAD 上仅余的 Wave14/15
  `CARRIER_PUSH_DRIFT` 必须在基于 `309db390...` 的唯一最新-main candidate 中消除。当前树先形成可审计
  中间 lane commit，再用 frozen-base / committed-lane-head / latest-main 重新生成并验证 drift plan。
- 2026-08-22：正确 frozen-base/lane-head/latest-main 三元组为
  `9717949319e619952c192e39c4ed2db1ee9f9eab` /
  `62b01bbc1ee45c10f7a09f038fab3f8c2399eb67` /
  `309db390f9010e3b2801d79dbac9ab1b833b8e45`；最终 validated plan id=
  `integration-revalidation-946ab65c1ae18858946f`、SHA-256=
  `946ab65c1ae18858946f09ef2faebce26771a195c8d2bc961a4bfba942f71d07`、decision=
  `RECONCILIATION_REQUIRED`。overlap 仍只含 generated `docs/task_register.md` 与
  `inputs/architecture/arch_005_task_registry_index.yaml`；TRADING-2540 requirement/fragment 与
  `tests/test_arch_005_s5_task_source_cutover.py` 均为 `MAINLINE_UNRELATED`，最终 candidate 从 main
  原样保留并用 official task events 重建 2539/2537 projection。exact reviewed-reconciliation preflight=
  `PASS`。切到 frozen lane 和再切到 `309db390...` final candidate 时，Git 各打印一次同一
  known-unrelated exclusion 路径及 `M`，分别记录为第三、第四次 audit incident；仍未读取、hash、diff、
  stage 或修改该文件内容。
- 2026-08-22：Project Owner 判定逐次 exact-token 限制过重，要求正确性从实际证据出发，并要求同步项目
  后继续中断工作。DEVX-008 successor policy 将本 V2 existing-clone、zero-order、bounded run 分类为
  `R1_BOUNDED_RESEARCH_SANDBOX / STANDING_OWNER_SCOPE`；不修改 V1 历史授权/运行/evidence bytes，V2
  publication 后由 Codex 自动核验 manifest 并记录 actual counters，无需 Owner 再粘贴长 token。
- 2026-08-22：V2 standing scope 在 published policy commit
  `d122b4d3a9ba0a87ae8c9a009439482159ffbbf6` 上通过 manifest replay 与 editor exact readback；clone
  `35444189` 的 candidate 为 `26587 LF bytes` / SHA-256=
  `06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2`。唯一 save 产生 Build
  `d432a0-8b195b`，唯一 zero-order Cloud backtest `Calm Violet Jackal` / id=
  `351d818182ef42b62f4d968016035854`，Lean=`2.5.0.0.18024`。
- V2 terminal 完整覆盖 requested/evaluated `2021-02-22..2025-12-02` 和 `1202/1202` sessions：唯一
  target=`2022-08-26`（`INTERIOR`），equity Slice present=`true`，subscribed chain events=`0`；唯一
  provider query 的 exact-date records/contracts=`1/6496`，non-target records=`0`，cross-date fallback=
  `false`。因此 attribution=`EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING`，attribution terminal=
  `RESOLVED`，execution terminal=`COMPLETE`。orders/fills=`0/0`，portfolio invested=`false`，没有 raw
  rows、contract identifiers、individual fields、Logs-as-data、Object Store、broker 或 production effect。
- 该结果完成的是根因归因，不是 durable data repair。`chain_presence=FAIL`、DQ=`FAIL`、PIT=
  `NOT_EVALUATED`、engine=`POLICY_BLOCKED_CASH_PRESERVATION` 继续成立。same-date recovery contract、
  adapter、offline validation 与后续新的 bounded R1 validation 由
  [TRADING-2541](TRADING-2541_QC_QQQ_Options_Exact_Date_Subscription_Missing_Remediation_V1.md) 承接。
  V2 证据封存在
  `inputs/research/qqq_options/trading_2537_existing_clone_exact_date_execution_v2/`；scope 已消费并关闭，
  不允许 retry。
- repository evidence closure 的 focused suite=`121 passed`，Ruff PASS；正式 Architecture=
  `865 passed`、Contract=`276 passed`、Integration=`995 passed`、Reproducibility=`24 passed`。这些验证
  确认 sealed evidence、canonical task projection、Atlas successor disclosure、report-flow 与 compatibility
  authority 一致；最终 Full 必须绑定 committed tree 后执行，不能由上述 focused/formal subset 代替。
