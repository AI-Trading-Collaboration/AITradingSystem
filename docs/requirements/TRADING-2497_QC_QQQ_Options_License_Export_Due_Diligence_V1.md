# TRADING-2497：QQQ Options License / Export Due Diligence V1

最后更新：2026-08-07

稳定任务 ID：`TRADING-2497_QC_QQQ_OPTIONS_LICENSE_EXPORT_DUE_DILIGENCE_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

exact base：`967d3524876b34c11ee8235b2913ba841cf94b36`

production effect：`none`

broker action：`none`

## 1. 目标

本任务处理 TRADING-2493 五项 UNKNOWN 中的第一项：QuantConnect Free tier 对 QQQ
Options 的 license、account entitlement、historical retention 与 export 边界。任务建立一个
strictly offline、typed、canonical、可重放的 due-diligence contract，区分：

- 官方公开文档明确陈述的 platform capability；
- 只能由账户页面、provider agreement 或人工书面确认关闭的 entitlement；
- cloud use、local download、derived backtest result export 与 raw-data redistribution；
- documented fact、inference、unknown 和 Owner decision。

工程输出不构成法律意见，不把公开文档升级为账户 entitlement PASS，也不解除 2489/2490、
DQ/PIT、resource cap、range expansion 或 primary-window blocker。

## 2. Inherited authority

2497 必须只通过 TRADING-2493 canonical public API 读取：

- Owner attestation file SHA-256=
  `9b1592289b579dacb0608aeb18d73aac940ad92795484c2377f7f6e8ba2f4aa6`；
- terminal signoff file/content SHA-256=
  `dd9c9332d57e48de7541ca316a4b64594b1ecf03f0910551f1e63a4e60174d02` /
  `a6824fc8264d4719023dd23ae17f5deb1f64e9ee5e35dd87d8144519050f059f`；
- terminal status=`SIGNED_NO_GO`；
- aggregate=`NO_GO_KEEP_BLOCKED`；
- `LICENSE_EXPORT=NO_GO`、`RANGE_EXPANSION=NO_GO`；
- `further_cloud_action_authorized=false`、`paid_tier_upgrade_authorized=false`、
  `investment_interpretation_allowed=false`。

2497 不复制或重定义 2481 shared records、2482 DQ/PIT、2484 adapter、2489 bundle、2490
reconciliation、2492 evidence 或 2493 stage-gate records。

## 3. 官方公开 reference boundary

本任务仅登记以下 QuantConnect 官方 URL 的 reference metadata，不下载或保存页面正文：

1. `https://www.quantconnect.com/docs/v2/cloud-platform/datasets/licensing`
2. `https://www.quantconnect.com/docs/v2/cloud-platform/organizations/tier-features`
3. `https://www.quantconnect.com/docs/v2/cloud-platform/organizations/resources`
4. `https://www.quantconnect.com/docs/v2/writing-algorithms/datasets/algoseek/us-equity-options`
5. `https://www.quantconnect.com/docs/v2/cloud-platform/backtesting/results`
6. `https://www.quantconnect.com/terms/`

检索日为 `2026-08-07`。Terms 对 automated agent/script copying、monitoring、scraping 或 mining
设置限制，因此实现必须固定：

- `capture_mode=PUBLIC_REFERENCE_METADATA_ONLY_NO_PAGE_COPY`；
- `source_content_checksum_status=NOT_CAPTURED_AUTOMATION_PROHIBITED`；
- 不创建 HTML/PDF/page snapshot、mirror、cache 或 raw response；
- claim 只保存短的工程摘要、exact URL、检索时间、source role、review owner 与 exit condition；
- 任何需要正文逐字复核的 claim 保持 `PENDING_MANUAL_OWNER_REVIEW`。

没有 webpage checksum 不能被伪装成 checksum PASS；它是显式 evidence limitation。

## 4. 冻结 assessment axes

| Axis | 工程默认 | 允许推出的最强结论 |
|---|---|---|
| `FREE_CLOUD_DATA_CLASS_ACCESS` | `PUBLIC_DOCS_CONDITIONAL_SUPPORT` | Free tier 文档描述 minute–daily cloud data class，可支持受限 backtest/research capability 判断。 |
| `QQQ_OPTIONS_ACCOUNT_ENTITLEMENT` | `UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED` | 公共文档不证明 project/account 对 QQQ Options 的完整 entitlement。 |
| `PRIMARY_WINDOW_HISTORICAL_RETENTION` | `UNKNOWN_ACCOUNT_SPECIFIC_EVIDENCE_REQUIRED` | dataset start 不等于账户可用 retention；不得宣称 `2021-02-22` 全窗可回测。 |
| `RAW_OPTIONS_LOCAL_DOWNLOAD` | `NO_GO_SEPARATE_DOWNLOAD_LICENSE_REQUIRED` | cloud access 不等于 download license；当前无 download 授权。 |
| `RAW_OPTIONS_REDISTRIBUTION` | `NO_GO_PROHIBITED` | 不保存、转换、重建或再分发 raw option rows。 |
| `DERIVED_BACKTEST_RESULT_EXPORT` | `CONDITIONAL_DOCUMENTED_UI_EXPORT_ONLY` | 仅承认官方 Results UI 文档列出的 report/orders/trades/logs/results 等 derived artifacts；仍须 2489 security/license gate。 |
| `API_CLI_ACCESS` | `NO_GO_CURRENT_FREE_TIER` | 不调用 API/CLI；公开 tier 文档把相关能力置于 paid tier，2497 不推导升级价值。 |

aggregate 固定为 `LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED`。任意单轴 conditional support、
2492 一日 pilot、2493 capability conditional-go 或 caller 自报 account access 都不能提升 aggregate。

## 5. Public contract

计划新增 task-owned policy/API：

- `LicenseEvidenceSourceRecord`：source id、URL、retrieved-at、role、capture mode、checksum status、
  manual reviewer 与 limitations；
- `LicenseClaimRecord`：claim id、source ids、fact/inference/unknown classification、短摘要、
  allowed conclusion、owner 与 exit condition；
- `LicenseAxisAssessment`：冻结 axis、status、supporting claim ids 与 blocker；
- sealed `QCQQQOptionsLicenseExportDueDiligenceReport`；
- strict policy loader、2493 replay、report builder 与 typed contract error。

sealed report 必须提供 `seal`、`canonical_bytes`、`canonical_sha256` 与 `from_json_bytes`；输入排列、
JSON formatting 或 caller 自报 PASS 不得改变 identity 或放宽 gate。

## 6. Data quality、PIT 与 research window

- public reference metadata 不等同 market data DQ；option-event DQ/PIT 保持 `NOT_EVALUATED`；
- 不运行 `aits validate-data`，因为本任务不消费 cached market/macro data，也不产出技术特征、
  score、backtest 或 daily report；
- primary default 始终为 `2021-02-22`；本任务不运行或批准该窗口；
- dataset 文档中的起始年份只可作为 provider coverage claim，不得替代 requested/evaluated range、
  account entitlement 或 actual run evidence。

## 7. Safety boundary

全部输出固定：

- `quantconnect_login_performed=false`；
- `cloud_backtest_performed=false`；
- `project_mutation_performed=false`；
- `api_cli_http_object_store_used=false`；
- `raw_options_data_downloaded=false`；
- `range_expansion_allowed=false`；
- `paid_tier_upgrade_authorized=false`；
- `investment_interpretation_allowed=false`；
- `paper_allowed=false`、`live_allowed=false`、`production_allowed=false`；
- `broker_action=none`。

本任务不复用 2480/2492 single-use tokens，不授权浏览登录态页面，不联系 vendor，不购买 license，
不创建 support ticket。

## 8. Acceptance criteria

- exact 2493 attestation/signoff hash、canonical seal、axis/safety replay PASS；
- 六个 official source records、冻结 claims 与七个 assessment axes complete/ordered/unique；
- source URL scheme/host/path allowlist、capture mode、checksum limitation 与 retrieved-at 可验证；
- missing source/manual reviewer/exit condition、content checksum 伪造、unknown→PASS、account
  entitlement 冒充、dataset-start→primary-window promotion、raw export/redistribution、API/CLI、
  paid-upgrade、wrong 2493 hash、tamper/noncanonical/permutation negatives fail closed；
- report deterministic/canonical；aggregate 始终为
  `LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED`；
- system flow、architecture fragments、task register、generated/task shadow/current authority 同步；
- focused/adjacent/compatibility 与 final-tree formal gates PASS；
- 外部 QuantConnect/platform/provider/billing/production/broker 动作均为 none。

## 9. Sequencing

1. S0：task row、requirement 与 exact public-reference boundary；
2. S1：policy、typed records、2493 canonical replay 与 deterministic report；
3. S2：negative/property/golden tests 与 system-flow wiring；
4. S3：生成 Owner-review proposal；未收到新的 exact token 前保持
   `OWNER_REVIEW_REQUIRED`；
5. S4：generated/current authority、formal gates、ordinary main push 与 cleanup。

2026-08-07：READ_ONLY preflight 在 exact main
`967d3524876b34c11ee8235b2913ba841cf94b36` PASS；`TRADING-2494..2496` 已占用，2497 无
owner 冲突。官方公开资料只支持 conditional cloud capability，不支持 account entitlement、完整
primary-window retention 或 raw export。当前仅执行 S0 登记，外部 QuantConnect 动作均为 none。

SINGLE_LANE START/LANE 随后在同一 exact base PASS，`contract_change=false`。policy、strict loader、
2493 canonical replay、六 source/九 claim/七 axis assessment、sealed report builder、architecture fragments
与 system flow 已实现；不保存网页正文或 source-content checksum。focused 首轮=`15 passed / 2 failed`，
根因是 `Literal[date]` JSON round-trip；改为 exact date validator 后第二轮=`16 passed / 1 failed`，剩余
仅为 test fixture 未使用 canonical Unicode serializer；修复 fixture 后同覆盖=`17 passed`，2493–2497
adjacent=`34 passed`。两轮失败均为 focused failure-fix evidence，不是正式门禁；Ruff、format、strict
mypy、compileall PASS。

implementation commit=`2e63070771afb48eb5b6873806bc84ff560c10d4`。canonical tracked report 已由该
exact implementation authority deterministic 生成：file SHA-256=
`5e8063754bae6e9e4cb3cca02dacd064e3ce368a1cdba9612df707a83ed48e80`，content SHA-256=
`31e244287ed631a88617f72ddf6720925f4fd58d20f75066a57805c00f4afd7a`，policy file/canonical
SHA-256=`a657c5d0314561ef2c8b9898d0a907174eb6d6e8364da9788ba84dd60cb3bb3f` /
`8e6e57d5dba9f0a91c5165c245e340be4b77abe7ec2adafea022dc379cbf5504`，authority set
SHA-256=`375da2b0720ed7da7bea67bf684ba38b334a88f1a32fcc7c6703177136f0a647`。tracked report
canonical replay 后 focused=`18 passed`；aggregate 保持
`LICENSE_EXPORT_NO_GO_OWNER_REVIEW_REQUIRED`。工程 baseline 已闭环，因此任务转
`BASELINE_DONE`；剩余缺口仅能由 project owner 对 official pages/account entitlement/provider terms
作人工复核并提交新的 exact proposal 关闭，2497 本身不授权任何外部动作。

final-tree adjacent 2493+2497=`35 passed`。compatibility/deprecation 同一并行覆盖 200 tests：首轮
`120 passed / 80 failed / 211.06s`，根因是生成新 EOF authority 前误带两条已由 2493 successor
规则接管的 task-shadow path；删除虚假 source-delta 后第二轮=`198 passed / 2 failed / 125.85s`，
剩余为新 EOF 缺少 transitive fragment authority 与 2493 仍直接比较 raw live hash；补 963-fragment
authority 并改用既有 current-authority resolver 后第三轮=`200 passed / 129.18s`。两轮失败均为
focused failure-fix evidence；历史 prefix exact-byte、source-hash 与 validation strength 未降低。
