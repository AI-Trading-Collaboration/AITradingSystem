# TRADING-2480：QuantConnect QQQ Options Capability / License / Evidence Spike V1

最后更新：2026-08-04

稳定任务 ID：
`TRADING-2480_QC_QQQ_OPTIONS_CAPABILITY_LICENSE_EVIDENCE_SPIKE_V1`

优先级：`P0`

状态：`IN_PROGRESS`

当前授权边界：

```text
owner_authorization_state:ACTIVE_SINGLE_NO_ORDER_CAPABILITY_DISCOVERY
owner_authorization_id:owner_decision:TRADING-2480:2026-08-04:authorize_single_no_order_qc_capability_discovery_run_v1
```

production effect：`none`

broker action：`none`

## 1. 目标

本任务是 QQQ options research capability 的串行 admission gate。它先把 QuantConnect Free
entitlement、QQQ options data、输入、输出、资源、engine identity 与字段许可证据收敛为 typed、
可重算且 fail-closed 的 receipt，再决定是否允许进入后续 bounded pilot 准备。

2026-08-03 Owner 已授权登录 QuantConnect，并只读核验 Free tier、QQQ Options 数据 entitlement、
engine/resource、result artifact 与 license/export 边界。授权明确禁止创建或修改 project、运行 cloud
backtest、API/CLI、下载 raw options data、paper/live/broker/production；完成本次 evidence 核验即失效，
并需要 independent reviewer 复核。此前完成的离线 admission baseline 包括：

1. reviewed policy manifest；
2. typed evidence、field-export matrix 与 admission receipt；
3. deterministic evaluator 和 exact-byte verifier；
4. public-doc/template input，用 `UNKNOWN` 显式保留真实平台证据缺口；
5. negative/tamper/determinism tests。

这不是 cloud pilot、策略实现、收益回测或 source registration。外部核验仍须独立 Owner token。

## 2. 权威依赖与优先级

- 父规划：`TRADING-2478_QUANTCONNECT_QQQ_DAILY_OPTIONS_BACKTEST_CAPABILITY_TECHNICAL_PLAN_V1`；
- critical path：`2480 -> 2481 -> 2482 -> 2484 -> ... -> 2492 -> 2493`；
- 当前 Atlas canonical projection 仍需独立授权，不能与本任务互相代替；
- `TRADING-2481` shared schema freeze 不得在本 admission receipt 里被提前宣称完成；
- active research default 仍为 `2021-02-22`，本任务不运行任何 research/backtest window。

## 3. Evidence contract

### 3.1 能力项目

每个 required item 必须具有稳定 `item_id`、typed `status`、证据类型、locator、观察时间、观察者、
证据摘要，以及未确认时的 exit condition。允许状态只有：

- `CONFIRMED`：证据直接支持该项；
- `UNKNOWN`：当前证据不足；
- `CONTRADICTED`：证据直接否定该项。

公共文档只能确认公开 tier、dataset、resource、results 与 license 规则，不能替代账户 entitlement、
QQQ 实际覆盖、project/backtest/LEAN identity、evaluated range、真实 runtime 或人工导出完整度。

### 3.2 Field export/license matrix

每个候选字段必须同时记录 capability status 与 export classification：

- `QC_ONLY_NOT_EXPORTED`；
- `EXPORT_ALLOWED_DERIVED`；
- `UNKNOWN_REQUIRES_LICENSE_REVIEW`；
- `EXPORT_PROHIBITED`。

`raw_option_chain` 与 `raw_minute_quote` 在 v1 policy 中必须保持 `QC_ONLY_NOT_EXPORTED`；任何
`UNKNOWN_REQUIRES_LICENSE_REVIEW`、`EXPORT_PROHIBITED` 或与 policy 不一致的分类都阻断 admission。
不得用 logs、screenshots 或 summary CSV 重建、转储或再分发受限 raw data。

### 3.3 Admission output

只允许两种顶层结论：

- `CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT`；
- `CAPABILITY_OR_LICENSE_BLOCKED`。

只有全部 required capability items 为 `CONFIRMED`、field matrix 完整且与 reviewed policy 一致、
external action 有精确 Owner authorization、identity/checksum/timestamp 均有效时，才允许第一种结论。
任一缺失、`UNKNOWN`、`CONTRADICTED`、重复、tamper 或 license mismatch 均返回第二种结论或 typed
contract error。该结论只允许进入后续合同/试点准备，不授权 `TRADING-2492` cloud pilot。

## 4. 安全不变量

receipt 必须固定：

```text
research_only=true
manual_review_required=true
promotion_allowed=false
paper_shadow_allowed=false
production_allowed=false
raw_options_data_download_allowed=false
strategy_execution_allowed=false
bounded_cloud_pilot_authorized=false
production_effect=none
broker_action=none
```

artifact 不得包含 secret、cookie、credential、organization member PII、broker/account identifier、
raw option-chain rows、raw quotes、收益指标或投资结论。

## 5. 实施范围

Task-owned：

- `docs/requirements/TRADING-2480_QC_QQQ_Options_Capability_License_Evidence_Spike_V1.md`；
- `src/ai_trading_system/contracts/qc_qqq_options_capability_admission.py`；
- `src/ai_trading_system/qqq_options_capability_admission.py`；
- `src/ai_trading_system/contracts/qc_qqq_options_capability_discovery_authorization.py`；
- `src/ai_trading_system/qqq_options_capability_discovery_authorization.py`；
- `config/research/qc_qqq_options_capability_admission_v1.yaml`；
- `config/research/qc_qqq_options_capability_discovery_authorization_v1.yaml`；
- `inputs/external_validation/qc_qqq_options_capability_evidence.template.json`；
- `inputs/external_validation/qc_qqq_options_capability_evidence_20260803.json`；
- `inputs/external_validation/qc_qqq_options_admission_e3a987b2b671e922175b35783dded6f4bbfa51dd5aaa523f415547026434ba04.json`；
- `tests/test_qc_qqq_options_capability_admission.py`；
- 对应 architecture module/flow fragments。

Coordinator-owned：

- `docs/task_register.md`；
- `docs/system_flow.md`；
- architecture generated manifests/views；
- task-shadow generated state；
- formal validation artifacts。

本任务不修改 `external_validation.py`，避免继续扩大该共享 legacy 模块。

## 6. 阶段与验收

### S0：离线合同

- supporting requirement、policy、typed models 和 public-doc/template evidence 完成；
- policy/contract 明确哪些事实只能由真实平台证据闭合；
- 未授权状态 deterministic 输出 `CAPABILITY_OR_LICENSE_BLOCKED`。

### S1：Evaluator 与验证

- evaluator 绑定 policy SHA-256、evidence SHA-256 与 canonical receipt id；
- exact-byte receipt replay；
- missing/duplicate/extra item、unknown/contradicted、wrong export classification、错误授权 token、
  unsafe boundary、path/timestamp/checksum tamper 全部 fail closed；
- focused pytest 通过；system flow 和 architecture fragment 同步。

### S2：受限外部核验（2026-08-03 已授权）

本次只读核验使用以下精确 Owner token：

```text
owner_decision:TRADING-2480:2026-08-03:authorize_bounded_qc_capability_license_evidence_probe_v1
```

该阶段只允许 research-only、登录后只读 UI capability probe；不得创建或修改 project、运行任何 cloud
backtest、调用 API/CLI、下载 raw options data、paper/live/broker/production，也不得运行 primary full
window。只读 UI 无法直接证明的 QQQ 单标的实际覆盖、project/backtest/LEAN identity、evaluated range、
真实 runtime 与 result exports 必须保持 `UNKNOWN`，不得用 dataset-wide catalog 或公开文档替代。
真实 evidence 写入 canonical artifact 后重跑 evaluator，输出
`CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT` 或保持 blocked；完成证据核验后本 token 失效并等待 independent
reviewer。

### S3：单无订单 capability-discovery cloud run（2026-08-04 已授权）

本阶段使用以下精确 Owner token：

```text
owner_decision:TRADING-2480:2026-08-04:authorize_single_no_order_qc_capability_discovery_run_v1
```

本 token 只授权一次 `CAPABILITY_DISCOVERY_NO_ORDER_NOT_RESEARCH_CONCLUSION` 运行：

- `requested_start=requested_end=2025-12-02`，该日必须由 reviewed XNYS calendar 重算为
  `NORMAL_TRADING_DAY`，且位于 `historical_seen_2025_sample=2025-01-02..2025-12-31`；
- `maximum_runtime_minutes=10`、`maximum_projects=1`、`maximum_cloud_backtests=1`；
- `maximum_order_count=0`、`maximum_contract_quantity=0`；selector、execution、accounting、
  lifecycle 均不激活，任何 order/fill/position/cash mutation 均为违规并停止；
- compute budget 固定 `FREE_TIER_ONLY_ABORT_BEFORE_PAID_OR_UPGRADE`；任何付费、升级、额度异常或
  第二次 run 都不在授权内；
- collector=`codex_pilot_coordinator`，independent reviewer=`project_owner`；两者必须保持不同
  `attested_by`，reviewer 只能在 evidence bundle 关闭后对 exact bytes/hash 进行复核；
- 允许登录后通过 UI 创建或修改一个隔离 project、执行一次 Free Cloud backtest、观察
  QQQ-specific option visibility/derived chain counts、project/backtest/LEAN identity、node/runtime
  telemetry 和 Results/Orders/Trades/Logs 制品是否存在；
- 禁止 API/CLI/direct HTTP/Object Store、raw option chain/minute quote/OpenInterest/Greeks 下载或
  导出、raw-row logging、optimization、paper/live/broker/production、收益结论或范围扩张；
- token 在首次 cloud run terminal（PASS/FAIL/cancel/timeout）、首次 evidence capture 完成、Owner
  revoke 或边界违规中最早发生者时立即失效；project 在 reviewer 完成前不删除。

为保留 2026-08-03 admission policy/evidence/receipt 的 exact-byte replay，S3 通过独立、strict、
content-bound authorization overlay 绑定历史 blocked receipt，不改写它们。Overlay 只能放行
无订单 capability discovery，不得把当前 `CAPABILITY_OR_LICENSE_BLOCKED` 提升为 pilot-ready。
外部动作前必须先完成 overlay strict loader/tamper tests、applicable final-tree validation、ordinary
main push 与 exact SHA 复核。

## 7. Governed execution 与生命周期

- mode：`SINGLE_LANE`；
- frozen base：`d48dfca936cc8bd13a7dbf2cc9fb2d302b3d4488`；
- branch：`codex/trading-2480-qc-capability-discovery`；
- 使用 `D:/Work/AITradingSystem` checkout，不创建额外 worktree、clone 或 cache；
- pre-run policy wave 的 external browser/account action：`none`；只在 validated overlay ordinary-pushed exact main
  之后才能执行 S3 授权的一次 UI/project/cloud action；
- 任务代码与文档由 Git 恢复；runtime validation evidence 进入 canonical
  `outputs/validation_runtime`；
- pre-run exit condition：authorization overlay focused/current-authority/formal PASS、validated commit ff-only
  到 local main、普通 push 与 exact SHA 复核成功；完成前不得触碰 QuantConnect project/run；
- post-run exit condition：首次 terminal 后 token 立即失效，export-safe evidence 进入 governed bundle，
  `project_owner` 对 exact bytes/hash 独立复核；project 在复核前保留且不得执行第二次 run。

## 8. 进度记录

- 2026-08-02：父任务已把本项登记为 P0 serial admission；Owner 尚未授予外部平台访问 token。
- 2026-08-02：为继续推进不依赖外部授权的工作，选择 offline admission baseline；READ_ONLY audit 与
  SINGLE_LANE START/LANE preflight 均在 exact base `fea75a2a…` PASS，active lease=[]，没有启动
  QuantConnect、data download、backtest、production 或 broker action。
- 2026-08-02：offline policy/typed contract/evaluator/public-doc template 与 6 项 focused tests 已完成；
  未授权 template deterministic 输出 `CAPABILITY_OR_LICENSE_BLOCKED`。任务进入
  `BASELINE_DONE`，下一 exit condition 仍是 Project Owner 提供精确 external evidence probe token；
  在此之前不访问 QuantConnect，也不进入 cloud pilot。
- 2026-08-03：Owner 授予精确只读 probe token。登录账户 UI 直接显示 Free tier；organization resources
  显示 1 个 Community B-MICRO backtest node、1 个 Community R-MICRO research node、0 个 live node；
  US Equity Options catalog/detail 显示 cloud 免费、约 4,000 symbols、Minute/Hourly/Daily、TradeBar/
  QuoteBar/OpenInterest、2012-01 起始边界；license UI 明确 cloud 内部使用免费、local LEAN download
  按文件收费且只限 licensed organization 内部使用，禁止 redistribution/conversion。本次未保留账户或
  organization identifiers、credit、cookie、secret、截图或 raw rows。
- 2026-08-03：dataset catalog 不能直接证明 QQQ 单标的 entitlement/chain coverage；Data Explorer 链接
  未形成可复核的 QQQ-specific supported-asset 事实，且禁止创建 project 或运行 cloud backtest。因此
  `qqq_option_dataset_visibility`、`qqq_option_chain_coverage`、actual range、project/backtest/LEAN identity、
  runtime telemetry 与 Results/Orders/Trades/Logs exports 均继续为 `UNKNOWN`；evaluator 必须保持
  `CAPABILITY_OR_LICENSE_BLOCKED`。下一 exit condition=governed canonical evidence + policy token binding
  完成、focused/formal PASS、ordinary push、independent reviewer 复核。
- 2026-08-03：canonical evidence SHA-256=`a9f3eb13e298cbf0d1eaa10609c9d88699673428e9e060c4604854af61586053`；
  policy v1.1.0 SHA-256=`e2a429e7a6e2537c064261546f32771d4f824449f548a905befe5a93f1a6b2cc`；
  deterministic receipt id=`qc_qqq_options_admission_e3a987b2b671e922175b35783dded6f4bbfa51dd5aaa523f415547026434ba04`、
  receipt file SHA-256=`9c16be766a58a571b0378534b80a61d0f94b0e923ce305fcf75ac8ec53cfd000`。
  Owner token 已精确匹配，但仅 `7/21` capability items 与 `3/12` field export rules 为 confirmed；
  receipt 因 typed unknown/source/classification blockers 继续输出 `CAPABILITY_OR_LICENSE_BLOCKED`，
  `bounded_pilot_preparation_allowed=false`。
- 2026-08-03：focused 同覆盖
  `python -m pytest -n 16 --dist loadfile tests/test_qc_qqq_options_capability_admission.py
  tests/test_qc_qqq_options_platform_evidence_bundle.py tests/test_qc_qqq_options_bounded_cloud_pilot.py`
  failure-fix terminal 依次为 `35 passed / 18 failed in 5.84s`、`37/16 in 5.14s`、
  `37/16 in 5.53s`、`51/2 in 5.41s`、`53 passed in 5.47s`；失败均来自 2489→2492 exact-hash、
  cross-layer golden/corpus 或 derived identity 的严格级联，未降低验证。扩展 2480/2489/2490/2491/2492
  adjacent 首轮 `112 passed / 2 failed in 6.33s`，修复 deterministic corpus expected hash 后同覆盖
  `114 passed in 6.15s`。正式五级仍需在 final generated/current-authority tree 串行运行；任务状态写回
  `BASELINE_DONE` 后 tracked bytes 必须在 Full 前冻结。
- 2026-08-04：Owner 授予 S3 精确 token，并授权 coordinator 按 reviewed calendar 选择日期。
  Coordinator 选定 `2025-12-02`；本地 XNYS authority 重算为 Tuesday/
  `NORMAL_TRADING_DAY`/16:00 ET，且位于 reviewed `historical_seen_2025_sample`。Owner 指定
  `project_owner` 为 independent reviewer；collector 固定为不同身份 `codex_pilot_coordinator`。
  SINGLE_LANE START/LANE preflight 均在 exact main
  `d48dfca936cc8bd13a7dbf2cc9fb2d302b3d4488` PASS，active lease=[]。当前只实现并验证
  pre-run overlay，在其 ordinary push 前不操作 QuantConnect。邻接 authority 审计发现 2492 exact-bind
  旧 admission contract module；因此 overlay 改为全新独立 contract/runtime module，旧 admission
  policy/evidence/receipt/module/fragment 均恢复 exact base bytes，不制造 2489–2492 hash cascade。
- 2026-08-04：independent overlay policy/contract/loader raw SHA-256 分别为
  `7eb3d18aab85d49e04ce0d369c895d0bb6d622b5f843c6a6f2d209b8227fa333`、
  `f8798f5ddbb4d4fb32d7c1474b6c6cc8184f8a426b9237510b0bafa34ed45b22`、
  `a28018378c44c3504ec828a01656ca3a8f2c8eca377d2fbca1700805c06c422a`；canonical
  authorization SHA-256=`0d68713647bd0eed7e7a0d143cfbf0648487f400f844d34f8d2f5673f3c7e291`。
  Focused 首轮 `19 passed / 1 failed in 3.33s`，唯一失败是 Python 3.14 `Path.relative_to`
  对 `..` 保留导致 escape 在文件存在性检查处先报错；最小修复为访问前显式拒绝 `..`，同覆盖
  failure-fix=`20 passed in 2.91s`，独立模块重构后 final=`20 passed in 2.93s`。2480/2489–2492
  邻接覆盖=`127 passed in 6.74s`。Compatibility/deprecation 首轮
  `190 passed / 2 failed in 122.06s`，仅为新 module/test 导致 frozen inventory 与旧 EOF/current-hash
  authority stale；采用 append-only 新 section 接管，不改历史 payload 或降低 exact-byte/hash 验证。
  Final-authority 同覆盖重跑=`193 passed in 113.97s`；另有一次 10 秒外层 wrapper timeout 在 pytest
  terminal 前中止、无 node FAIL，不计验证证据。正式 Architecture 只允许在状态写回、generated/source
  hashes 刷新和同覆盖 final replay 再次通过后启动。
