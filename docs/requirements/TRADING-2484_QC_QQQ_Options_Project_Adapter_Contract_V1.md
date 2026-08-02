# TRADING-2484：QuantConnect QQQ Options Project Adapter Contract V1

任务 ID：`TRADING-2484_QC_QQQ_OPTIONS_PROJECT_ADAPTER_CONTRACT_V1`

## 1. 目标与非目标

本任务冻结一个离线、typed、可 canonical replay 的 QuantConnect project adapter contract。它消费并严格重验
TRADING-2483 的 immutable signal package，绑定 TRADING-2480 的 capability admission receipt，明确
QQQ subscription、LEAN engine identity、Free project-file 和后续 manual result evidence 的接口边界。

唯一成功出口是：

`QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN`

该状态只表示合同、loader 和 descriptor 可以供后续模块消费，不表示 QuantConnect 账号 entitlement、
input artifact admission、QQQ option coverage、真实 engine/backtest identity、license/export 权限或 cloud run
已获确认。

本任务明确不做：

- 登录 QuantConnect、创建或修改 cloud project、调用 API/CLI、运行 backtest；
- 使用 Object Store、远程 HTTP、secret、账号 token 或本机用户凭据；
- 选择 option contract、生成 order、模拟 fill、计算收益或形成投资结论；
- 导出 raw option chain、minute quote、open interest 或 Greeks；
- 授权 paper/live/broker/production/promotion。

## 2. Exact authority 与继承边界

- exact frozen base / local main / origin main：
  `96061fc6fdec5e606fef27c9f6777cba5fbd31d0`；
- TRADING-2480：必须调用既有
  `verify_qc_qqq_options_capability_admission_receipt()`，不得只解析 caller 自报的 receipt；
- TRADING-2481 contract SHA-256：
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- TRADING-2481 shared policy SHA-256：
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- TRADING-2482 DQ/PIT policy SHA-256：
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；
- TRADING-2483 signal export policy SHA-256：
  `cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9`；
- TRADING-2483 package layout：
  `daily_signals/<YYYY-MM-DD>.json`、`package_receipt.json`、`run_manifest.json`、
  `signal_index.json`；
- primary requested/evaluated start 必须保持 `2021-02-22`；当前 approved non-primary authority
  count 为 0，`2022-12-01` 不是默认值；
- option-event DQ/PIT 继续为 `NOT_EVALUATED`，不得由 adapter 自动提升为 PASS；
- external Owner token 继续为 `NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`。

任何 breaking change 到 2480 receipt、2481 shared contract、2482 DQ/PIT identity 或 2483 package
语义，都必须回到最小 reviewed serial contract wave；本任务不得重定义前置 authority。

## 3. 官方平台事实与证据分层

2026-08-02 只读复核以下 QuantConnect 官方页面：

- Projects / Files：Free organization 单文件最大 32KB：
  <https://www.quantconnect.com/docs/v2/cloud-platform/projects/files>；
- Object Store：只对 paid organizations 开放写入，Free write 会被拒绝：
  <https://www.quantconnect.com/docs/v2/cloud-platform/object-store>；
- Equity Options universe：option underlying 使用 RAW normalization，option universe/data 默认 minute
  语义：<https://www.quantconnect.com/docs/v2/writing-algorithms/universes/equity-options>；
- individual option contract 默认 resolution 为 minute：
  <https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/equity-options/requesting-data/individual-contracts>；
- Backtesting / Results：GUI 可下载 Results、Orders、Trades、Logs、Report；Orders/Trades/Results
  下载时间为 UTC，而 logs 使用 algorithm timezone：
  <https://www.quantconnect.com/docs/v2/cloud-platform/backtesting/results>。

这些页面只允许形成 public-doc contract facts。它们不能证明当前账号的 entitlement、实际文件上传、
project/backtest/LEAN identity、实际 QQQ coverage、下载完整度或 field license。后者仍由 2480/2489/2492
的真实平台 evidence 与独立 Owner token 决定。

## 4. 冻结的 adapter contract

### 4.1 Strict signal-package loader

loader 必须：

1. 拒绝非目录、root/file symlink、extra/missing/non-regular file；
2. 用 `QQQOptionsSignalPackageReceipt.from_json_bytes()`、
   `QQQOptionsSignalIndex.from_json_bytes()`、`RunManifestRecord.from_json_bytes()` 和
   `DailySignalRecord.from_json_bytes()` 复放 canonical bytes；
3. 按 receipt/index 的 exact relative path、SHA-256 与 byte count 校验 inventory；
4. 校验 run id、policy/code/source/lineage、requested/evaluated range、QQQ、CASH、DAILY signal、
   MINUTE execution、DQ/PIT/export/safety 的跨文件一致性；
5. 绑定当前 exact-byte 2483 signal policy，拒绝 policy 或 predecessor hash drift；
6. primary start 不等于 `2021-02-22`、non-primary 无 reviewed authority、option-event DQ/PIT 被伪升
   或 ETF mapping 被隐式引入时 fail closed。

### 4.2 Project adapter descriptor

descriptor 只保存运行所需的 content-bound identity，不嵌入 daily signal rows 或 raw option data。至少绑定：

- adapter policy id/version/SHA-256；
- signal package receipt/index/run-manifest SHA-256、run id、signal count、requested/evaluated range；
- repository code SHA、2481/2482/2483 hashes；
- verified 2480 receipt id、policy/evidence hashes、decision 与 blocking reason codes；
- subscription、engine metadata、input admission 和 result mapping requirements；
- safety、`cloud_run_authorized=false`、`production_effect=none`、`broker_action=none`；
- caller 不可提供的 `content_sha256` 与 deterministic canonical bytes。

descriptor 自身必须小于等于 32768 bytes；这不等于完整 signal package 已进入 Free project。实际 input
artifact transport 状态保持 `UNKNOWN_REQUIRES_PLATFORM_EVIDENCE`。

### 4.3 Subscription freeze

- platform：`QuantConnect`；language：`Python`；market：`USA`；underlying：`QQQ`；
- QQQ Equity：`MINUTE` + `RAW`；
- QQQ Equity Option universe/contracts：`MINUTE`；
- upstream signal cadence：`DAILY`；execution data cadence：`MINUTE`；
- storage timezone：`UTC`；exchange timezone：`America/New_York`；
- 不在 2484 定义 DTE/moneyness/delta/spread/OI/volume selection threshold；这些属于 2485 的
  reviewed policy。

### 4.4 Input admission / Free project-file boundary

- offline descriptor generation：允许；
- project-file maximum：32768 bytes per file；
- descriptor input mode：`CONTENT_BOUND_DESCRIPTOR_ONLY`；
- complete signal input admission：`UNKNOWN_REQUIRES_PLATFORM_EVIDENCE`；
- Object Store/API/CLI/remote HTTP/secret/raw-data embedding：全部禁止或不可假设；
- 2480 当前 `CAPABILITY_OR_LICENSE_BLOCKED` receipt 允许构建 offline adapter descriptor，但不得把
  `cloud_run_authorized` 设为 true；即使未来 receipt confirmed，2484 自身也无权授权 cloud run。

### 4.5 Engine metadata 与 manual result mapping

后续真实 evidence 必须分别绑定 project id、backtest id、LEAN engine identity/version、algorithm language、
code SHA、adapter descriptor SHA、requested/evaluated range、resource/runtime telemetry。缺任一项不得用
requested dates 或文件名推断。

2484 只声明 2489 后续采集的 mapping slots：Results JSON、Orders CSV、Trades CSV、Logs、Report、Project
Files。Orders/Trades/Results 下载时间统一声明 UTC；logs 明确是 algorithm timezone。所有 raw option
chain/quote/OI rows 保持 `QC_ONLY_NOT_EXPORTED` 或 `EXPORT_PROHIBITED`，derived evidence 仍需 license
review，不能因“可下载”推断“可重新分发”。

## 5. Planned public API

- `QCProjectAdapterContractError`；
- `QCProjectAdapterPolicy` / `QCProjectAdapterPolicyLoadResult`；
- `LoadedQQQOptionsSignalPackage`；
- `QCProjectAdapterDescriptor`；
- `load_qc_qqq_options_project_adapter_policy()`；
- `load_qqq_options_signal_package_for_qc()`；
- `build_qc_qqq_options_project_adapter_descriptor()`。

所有 public typed model 使用 strict validation；descriptor 提供 `seal()`、`canonical_bytes`、
`from_json_bytes()` 与 content-derived SHA-256。

## 6. 阶段、依赖与验收

|阶段|工作|依赖|验收|
|---|---|---|---|
|S0|Supporting requirement、task row、claims/preflight|2483 exact push|START/LANE PASS，contract-change 显式|
|S1|Policy 与 strict package loader|2480–2483 authority|canonical replay 与 inventory/hash/range negatives PASS|
|S2|Typed descriptor、subscription/engine/input/result boundary|S1|descriptor <=32KB，blocked receipt 不得授权 cloud|
|S3|Architecture fragments、system flow、generated state|S2|DevEx/task shadow/compat/deprecation freshness PASS|
|S4|Formal gates、commit、integration、ordinary push/cleanup|S3|Architecture/Contract/Integration/Reproducibility/Full final-tree PASS|

完成标准：

- focused adapter/security/contract tests 以 `-n 16 --dist loadfile` PASS；
- 2480/2481/2482/2483 adjacent tests PASS；
- Ruff、mypy、compileall、DevEx、task shadow、compatibility/deprecation PASS；
- `docs/system_flow.md` 展示 2483 package -> 2484 strict loader -> adapter descriptor -> 2485 的流程，
  并把 cloud run 明确画成 blocked external gate；
- final candidate 五级 canonical gates PASS，Full 独占；
- ordinary non-force push 后 `task commit = local main = origin/main`；
- exit=`QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN`。

## 7. Stop conditions 与 owner-dependent boundary

出现以下任一情况立即 fail closed：

- 2483 policy/package/hash/range/code/lineage 不一致；
- capability receipt 未通过既有 2480 verifier；
- input transport、engine identity、project/backtest identity 或 field license 被从官方文档推断为 confirmed；
- descriptor 超过 32KB 或含 secret/raw option rows；
- 要求登录平台、创建项目、上传文件、运行 backtest、下载证据、paper/live/broker/production；
- 需要改变 shared schema、DQ/PIT、research window 或 export/license boundary；
- heavyweight Full 与其他任务竞争。

上述 external 行为只能由 TRADING-2492 的独立、不可歧义 Owner token 授权；当前 token 未授予。

## 8. Governed execution 与临时工作区生命周期

- mode：`SINGLE_LANE`；`contract_change=true`；
- frozen base：`96061fc6fdec5e606fef27c9f6777cba5fbd31d0`；
- branch：`codex/trading-2484-qc-project-adapter-contract`；
- 复用 clean checkout：`D:\Work\AITradingSystem_ops073_integration`；不新建 task worktree；
- task-owned：本 requirement、policy、adapter module、public exports、focused tests、module/flow fragments；
- coordinator-owned：task register、system flow、`inputs/architecture/**`、task shadow、compat/deprecation tests；
- exit condition：final evidence 进入 canonical runtime location、ordinary push/remote SHA 验证完成后，删除
  task branch；复用 checkout 返回 clean main。Git main/SHA/reflog 是实现恢复边界。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、复制、stage
或修改；repo-wide inspection 只用 governed worktree audit。

## 9. 进度记录

- 2026-08-02：2483 完成 ordinary push，exact main=`96061fc6fdec5e606fef27c9f6777cba5fbd31d0`；
  final runner=0，Full 资源释放。
- 2026-08-02：`SINGLE_LANE START` 在 clean exact main 上 PASS；无 drift、lease、serial blocker；
  创建 task branch，任务由 `PROPOSED` 转 `IN_PROGRESS`。本阶段没有外部 QuantConnect 动作。
- 2026-08-02：offline adapter policy/module/public exports、strict package replay、canonical
  descriptor、architecture fragments 与 system flow 已实现。policy SHA-256=
  `b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`；focused=
  `23 passed`，2480–2484 adjacent=`114 passed`，Ruff 与 compileall PASS。
- 2026-08-02：普通 mypy 依赖图检查确认 task-owned adapter/test 无错误，但暴露 6 个既有传递
  模块的 34 条基线错误；未把该命令伪报为 PASS。`--follow-imports=silent` 保留导入类型信息、
  仅抑制传递模块自身诊断后，对两个 task-owned source 精确检查为 `Success: no issues found in
  2 source files`。compatibility/deprecation 首轮 104/181 PASS 的 77 FAIL 为两条未漂移 2483
  task-shadow 多报引起的单一 authority 级联；移除多报后原样并行覆盖 `181 passed`。
- 2026-08-02：首个未写回 closeout status 的 final tree 五级门禁全部 PASS：Architecture=`828`、
  Contract=`276`、Integration=`995`、Reproducibility=`24`、Full=`8050 passed / 5 skipped /
  644 warnings`。随后审计发现 task row 仍为 `IN_PROGRESS`；该结果保留为真实通过证据，但不被
  错误复用为状态写回后树的 promotion evidence。现已在正式收口前转 `BASELINE_DONE`，重建
  task shadow、DevEx、deprecation/compatibility current hashes，并要求在最终提交树完整重跑五级
  门禁；Full 继续使用合法 `natural_integration_boundary` provenance 且保持独占。
