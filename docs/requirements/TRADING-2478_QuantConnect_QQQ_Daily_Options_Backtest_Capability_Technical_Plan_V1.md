# TRADING-2478：QuantConnect QQQ Daily Options Backtest Capability Technical Plan V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2478_QUANTCONNECT_QQQ_DAILY_OPTIONS_BACKTEST_CAPABILITY_TECHNICAL_PLAN_V1`

Owner intent：
`owner_intent:TRADING-2478:2026-08-02:plan_quantconnect_qqq_daily_options_backtest_capability_v1`

状态：`BASELINE_DONE`

优先级：`P1`

production effect：`none`

broker action：`none`

## 1. 目标

当前工程线只有 QQQ/SGOV/TQQQ 日线研究、外部平台 feasibility/preflight 和一个明确阻断
LEAPS/Wheel 的 options next-stage gate。Owner 希望补充一条研究级 QQQ 期权回测能力：

- 给定固定初始现金；
- strategy signal 仍以已完成交易日为最小决策输入；
- 每日程序化选择、买入、卖出或滚动不同 QQQ option contracts；
- 允许用 minute QuoteBar 作为 execution/fill 证据，但不把任务扩张为 intraday alpha；
- 先使用 QuantConnect Free Cloud / LEAN 做 capability planning 和后续 research-only pilot；
- 不把免费云数据误写成本仓库可下载、可再分发或 canonical local cache 的数据源。

本任务只完成 Web Pro advisory、技术设计与后续开发任务拆解，不实现模块、不运行真实回测、
不访问 broker/account、不下载 vendor data，也不改变任何投资结论或候选状态。

## 2. 当前权威事实

1. 项目 future active research/backtest default 从 `2021-02-22` 开始。
2. 当前策略研究的核心价格、收益和 target-path 语义是 daily grid；已存在
   `daily_close_next_day_v1`，signal-to-execution lag 必须可见且不得 same-day lookahead。
3. `TRADING-878` 仍以 `OPTIONS_RESEARCH_BLOCKED` fail closed；明确缺少 option-chain
   historical data contract，以及 bid/ask、IV、Greeks、expiration、assignment、early exercise
   与 slippage model。
4. `TRADING-1129_to_1140` 与 `TRADING-1155_to_1164` 已提供 external validation、
   QuantConnect dry-run/preflight 和 manual evidence 框架，但明确把 LEAPS/Wheel/Options 排除在
   已实现范围之外；真实 dynamic external replay 仍需 custom engine / QuantConnect-style implementation。
5. QuantConnect 公开文档显示 Free tier 允许在 Cloud 中使用 minute-to-daily datasets；US
   Equity Options 提供 trade、quote、open interest，Option Universe 提供 daily model-derived
   IV/Greeks。Free tier 不等于本地 API/CLI 下载或 unrestricted repo-native reuse。

## 3. Web Pro exact-Git 审阅

### 3.1 冻结快照

- repository：`https://github.com/AI-Trading-Collaboration/AITradingSystem`
- exact commit：`82e197399667f483aed6b5d87b20221e663e859e`
- exact tree：
  `https://github.com/AI-Trading-Collaboration/AITradingSystem/tree/82e197399667f483aed6b5d87b20221e663e859e`
- ref source：`origin/main`
- public verification：PASS

网页审阅不得读取 moving `main`。本地任务登记和本文件比冻结提交更新，不能被提示为网页已从
Git 读取的事实；它们只能作为 Owner-authorized planning question 的 prompt-supplied context。

### 3.2 最小必要 outbound scope

计划只发送 public exact-commit URLs 与以下已核验事实：daily decision / next-bar execution、
current options gate、external validation precedent、`2021-02-22` default、no production / no broker。
不发送 secrets、cookies、private paths、known-unrelated exclusions、local cache 内容或任何账户信息。

### 3.3 审阅输出要求

Web Pro 必须覆盖：

- `MODEL_IDENTITY_AND_ROUTING_RISK` 与 `CANNOT_VERIFY_EXACT_BACKEND_ROUTE` 规则；
- exact-blob retrieval 成功/部分/失败清单；
- Free Cloud 与 repo-native data source/license boundary；
- architecture boundary、schemas、state machine、cash/margin、contract selection；
- daily signal / minute execution / no-lookahead event timeline；
- quote quality、stale/zero bid、OI/IV/Greeks availability 与 PIT semantics；
- fill/slippage/fee/exercise/assignment/dividend/corporate-action model；
- deterministic artifacts、external reconciliation、metrics 和 falsification；
- staged task sequence、serial contract wave、Owner gates、stop conditions；
- 近期八周内可执行排序和最重要风险。

### 3.4 审阅结果

- conversation：`https://chatgpt.com/c/6a6e3e7a-18ac-83ee-aca5-27f92aa0fef2`
- account plan label：`Pro`
- composer model label：`Pro`（checked）
- generation label：`Pro 思考中`
- response self-report：`GPT-5.6 Pro`
- route classification：`UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED`
- exact backend route：`CANNOT_VERIFY_EXACT_BACKEND_ROUTE`
- fallback：未观察到明确提示，但缺少 authoritative fallback/route audit，不能验证未发生 fallback
- required exact blobs：Web Pro 逐项报告 `8/8 success`；本地 fixed-SHA authority 复核一致
- canonical advisory：
  `docs/research/quantconnect_qqq_daily_options_backtest_web_pro_advisory.md`

网页最终建议为 `CONDITIONAL_GO_FOR_CONTRACT_WAVE_AND_BOUNDED_CAPABILITY_PILOT`，同时保持
`NO_GO_FOR_FULL_RANGE_OPTIONS_RESEARCH_OR_ANY_PROMOTION`。本地已用 QuantConnect 官方文档复核
Free Cloud、minute options data、daily universe Greeks/OI、B-MICRO/12h/10K orders/log quota、
manual results download、Object Store/API/CLI 和 dataset licensing 边界。

## 4. 对账后的目标架构（后续合同波次 proposal，不是已实现 flow）

```text
Internal governed daily signal/export
  -> immutable signal/input manifest
  -> QuantConnect project adapter or generated project bundle
  -> QQQ underlying + filtered option universe
  -> daily contract-selection decision
  -> minute quote execution/fill model
  -> lifecycle state machine (open/hold/roll/expire/exercise/assignment)
  -> result/event/transaction export
  -> local ingestion + independent validator + reconciliation
  -> research-only review artifact
```

初步边界：

- Internal system 保持策略事实、窗口、policy、input identity 和 acceptance authority；
- QuantConnect 只作为 external execution/data environment，不能成为无证据的唯一真相；
- Cloud backtest output 必须通过显式 evidence package 回流，不能由日志文本替代 typed artifacts；
- Free tier 若不能提供所需导出、版本或证据，必须返回 typed `INSUFFICIENT_PLATFORM_EVIDENCE`，
  不得静默改用本地模拟价格或未授权 provider；
- 第一 capability slice 只允许 QQQ、research-only、single-underlying、long-premium baseline；
  short premium、multi-leg、Wheel、assignment-sensitive strategies 需要独立 gate。

## 5. 规划产物

本任务预计只修改：

- `docs/task_register.md`；
- 本 supporting requirement；
- `docs/research/quantconnect_qqq_daily_options_backtest_web_pro_advisory.md`；
- task-register consistency 所需的 canonical generated task-shadow / index / append-only authority。

不更新 `docs/system_flow.md`：本任务不改变 runtime data flow、CLI、cache、DQ、scoring、backtest
或 report behavior。任何后续 capability implementation 必须在同一实现变更中更新 system flow。

## 6. 后续任务拆解合同

跨线协调已把 `TRADING-2479` 保留给 Atlas historical projection review pack，因此 Web Pro advisory 的
占位任务整体加一并冻结为以下本地 stable IDs：

|任务|阶段|目标|主要依赖|
|---|---|---|---|
|`TRADING-2480_QC_QQQ_OPTIONS_CAPABILITY_LICENSE_EVIDENCE_SPIKE_V1`|serial admission|闭合 Free entitlement、QQQ data、input/output、resource、engine identity 与 license/export 边界。|Owner 对受控平台核验的独立授权。|
|`TRADING-2481_QQQ_OPTIONS_SHARED_SCHEMA_POLICY_FREEZE_V1`|serial contract|冻结 run/signal/candidate/selection/order/fill/lifecycle/portfolio/DQ/evidence/reconciliation schema 与 policy envelope。|2480。|
|`TRADING-2482_QQQ_OPTIONS_DQ_PIT_CACHE_EVIDENCE_IDENTITY_V1`|serial contract|冻结 chain/quote/OI/Greeks/calendar/mapping/signal chronology/engine/manual evidence 的 DQ/PIT/cache identity。|2480、2481。|
|`TRADING-2483_QQQ_OPTIONS_SIGNAL_RUN_MANIFEST_EXPORT_V1`|parallel lane|把现有 daily signal 导出为 immutable signal/run package。|2481、2482。|
|`TRADING-2484_QC_QQQ_OPTIONS_PROJECT_ADAPTER_CONTRACT_V1`|parallel lane|实现前冻结 QC manifest loader、subscriptions、engine metadata、安全和 project-file boundary。|2480–2483。|
|`TRADING-2485_QQQ_OPTION_UNIVERSE_DETERMINISTIC_SELECTION_V1`|engineering lane|long call/put deterministic selection、stable SID/tie-break 与 no-contract behavior。|2481、2482、2484。|
|`TRADING-2486_QQQ_OPTIONS_MINUTE_EXECUTION_REALITY_MODEL_V1`|engineering lane|next-independent-minute、quote-side marketable limit、fill/slippage/latency/partial-fill。|2481、2482、2484、2485。|
|`TRADING-2487_QQQ_OPTIONS_CASH_PREMIUM_SETTLEMENT_ACCOUNTING_V1`|engineering lane|multiplier、premium、fee、reservation、settled/unsettled cash 和 accounting invariants。|2481、2486。|
|`TRADING-2488_QQQ_OPTIONS_LIFECYCLE_EXPIRY_CORPORATE_ACTION_SAFETY_V1`|engineering lane|position state machine、expiry guard、exercise/assignment/corporate action scope gates。|2485–2487。|
|`TRADING-2489_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_MANUAL_BUNDLE_V1`|parallel evidence lane|Free results/orders/trades/logs/report/screenshots/attestation/checksum bundle。|2480–2482。|
|`TRADING-2490_QC_QQQ_OPTIONS_LOCAL_INGEST_VALIDATOR_RECONCILIATION_V1`|integration lane|独立重算 lineage/order/fill/cash/lifecycle/metrics 与 difference taxonomy。|2483、2487–2489。|
|`TRADING-2491_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_HARNESS_V1`|shared QA|synthetic fixture、golden/property/contract/integration 与 cloud smoke checklist。|2481、2482；持续消费各 lane。|
|`TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1`|manual external gate|短窗口、低订单、preregistered Free Cloud pilot 与独立 evidence collection。|2480–2491、Owner 明确授权。|
|`TRADING-2493_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_SIGNOFF_V1`|decision gate|决定扩窗、付费升级、停止或保持 blocked；不批准 production/short/Wheel。|2492 完整证据。|

Critical path：`2480 -> 2481 -> 2482 -> 2484 -> 2485 -> 2486 -> 2487 -> 2488 ->
2490 -> 2492 -> 2493`。`2483`、`2489`、`2491` 在合同冻结后可并行；任何 shared schema、
public contract、DQ/PIT、cache identity 或 license boundary 变化都必须回到最小 serial contract wave。

每项均在 `docs/task_register.md` 登记为 `PROPOSED`；被选中实施时必须先创建或更新独立 supporting
requirement、运行 governed preflight、声明 claims，并同步受影响的 `docs/system_flow.md`。

## 7. 安全与停止条件

必须保持：

- `research_only=true`
- `manual_review_required=true`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `production_effect=none`

以下任一条件必须停止后续 capability/pilot，而不是临时绕过：

- QuantConnect exact project/backtest identity、code version、LEAN version 或 data resolution 不可证明；
- QQQ option-chain coverage、quote/OI/Greeks availability 或 actual evaluated range 不完整；
- vendor terms 不允许目标 evidence export / internal use；
- decision-time visibility、bar timing 或 fill timing 有 same-bar/lookahead 歧义；
- bid/ask、stale quote、zero bid、liquidity、fee/slippage、exercise/assignment 未建模；
- output 无法被本地 independent validator 重算或与事件/交易 ledger 对账；
- Free node resource limits使完整 primary-window run 不可重复完成；
- 任何步骤要求真实账户、broker credential、live order 或 production state mutation。

## 8. Governed execution 与生命周期

- mode：`SINGLE_LANE`
- frozen base：`82e197399667f483aed6b5d87b20221e663e859e`
- branch：`codex/trading-2478-quantconnect-qqq-options-plan`
- 不创建额外 worktree；使用 clean main checkout 切换 task branch；
- Web Pro conversation 保留在用户 Chrome 中，URL 记录到 canonical advisory；
- tracked planning docs 由 Git 恢复；无本地 vendor data、temporary clone/cache 或 runtime process；
- closeout 前运行 governed worktree audit；验证通过后按项目默认规则 fast-forward local main、
  ordinary push remote main，并清理已合并 task branch。

## 9. 验收标准

1. Web Pro 使用 public exact commit 且 required blobs retrieval 逐项披露；
2. UI selection、model self-report 和 backend-route evidence 分层记录；
3. advisory 足够详细，覆盖 architecture、data/license、DQ/PIT、execution/reality models、artifacts、
   validation、failure matrix、security/operations 和 staged roadmap；
4. 本地对账明确采纳、修正、拒绝和 Owner-decision-required 项；
5. 后续开发任务具备 stable IDs、依赖、claims、acceptance、stop conditions 和安全边界；
6. 不实现 adapter/backtest、不访问 QuantConnect/broker account data、不下载或导出 vendor data；
7. task/register/docs consistency 与适用 Architecture/Contract validation PASS；
8. `production_effect=none`、`broker_action=none`。

## 10. 进度记录

- 2026-08-02：Owner 明确要求工程线补充 QQQ 日频期权回测能力，先向网页 GPT Pro 提交初步
  设想形成详细技术文档，再依据 advisory 规划后续模块任务。READ_ONLY preflight=`PASS`；
  local main = origin main = `82e197399667f483aed6b5d87b20221e663e859e`；active lease=[]；
  worktree audit=`PASS`。本任务登记为 `IN_PROGRESS`，仅授权 planning/documentation。
- 2026-08-02：Web Pro exact-Git 审阅完成；对话 URL、UI/self-report/route evidence、8/8 exact-blob
  retrieval 和 27m7s advisory 已记录。官方页面复核确认 Free cloud minute-to-daily、US options
  minute TradeBar/QuoteBar/OI、previous-day universe IV/Greeks、B-MICRO/12h/10K orders/log quota、
  manual result download、Free Object Store/API/CLI 与 download-license 边界。形成 reconciled advisory，
  后续任务重映射为 `TRADING-2480..2493`，`TRADING-2479` 保留给 Atlas 线。本任务转
  `BASELINE_DONE`；未访问 QuantConnect、未运行 backtest、未下载 market data、未改 runtime flow。
- 2026-08-02：首次 validation 命令误用展示名 `Architecture`，在 argparse 阶段 fail closed，未启动
  pytest。随后 canonical `architecture-fitness` 在 OPS-073 的 16-worker Full 独占窗口内误启动；收到
  资源协调通知后立即终止经边界标识核实的根 PID `50508` 及其 17 个后代，并复核无
  TRADING-2478 Architecture 进程残留。两次结果均标记为资源竞争窗口内的非正式、不可集成证据，
  不串接 `contract-validation` 或其他 gate；待 OPS-073 正式 Full、integration 与 `main` push 完成后，
  必须在 final latest-main 树完成 drift/reconciliation、重建共享 task shadow，并重跑正式验证。
- 2026-08-02：OPS-073 已完成五级门与 ordinary push，`local main = origin/main =
  4a0d168fbeb773be3ced8065cfe1b3194902543f`。TRADING-2478 保留 frozen lane，不创建 replacement
  v2/v3 branch；登记唯一 latest-main coordinator worktree
  `D:\Work\AITradingSystem_trading2478_integration`，用途为一次性 drift/reconciliation、shared task
  shadow 重建、final-tree validation 与 integration candidate。退出条件：validated candidate 已
  fast-forward/push 到 `main`，canonical evidence 已保留，worktree 无独有 tracked/untracked/ignored
  内容且无活动进程依赖；满足后用 `git worktree remove` 清理并 `git worktree prune`。
  只读 drift control artifacts 使用 Git-ignored repository-internal 路径
  `D:\Work\AITradingSystem\.git\trading2478\change_manifest.json` 与
  `D:\Work\AITradingSystem\.git\trading2478\integration_plan.json`；integration preflight 验证完
  plan id 后删除，Git 历史、runtime validation artifacts 与本 progress note 保留可恢复性边界。
- 2026-08-02：reviewed drift plan=`integration-revalidation-814b67cf630da451a673`，SHA-256=
  `814b67cf630da451a673e62e108d7aebc977d7a49d354df0239c6b96153c4f6b`；唯一 domain overlap 为
  `docs/task_register.md`，其余 12 项为 `COORDINATOR_REFRESH`。首次 candidate task-shadow generation
  因新 worktree 缺少 frozen historical artifact
  `outputs/validation_runtime/fast-unit_20260719T184434Z/test_runtime_summary.json` fail closed；补齐后又
  fail closed 于同一 bootstrap handoff 的 Architecture artifact，确认依赖单位是 authority
  `inputs/architecture/arch_005_bootstrap_handoff.yaml` 冻结的 Fast/Architecture/Contract/Full 四文件
  bundle，而非单一文件。四个 exact bytes 在 main 与 OPS-073 worktree 均按 authority SHA-256 核验；
  candidate 只从 main checkout 复制完整 frozen bundle 作为历史 validation dependency，不修改其
  bytes、不纳入 task commit，并在 candidate worktree closeout 时清理。Fast SHA-256=
  `5afc81ae21909ed465d11c670fabc19f15dac0d83d5b2c69748facb68db153ca`；其余 SHA 以 handoff
  authority 中 `a7c070c9…`、`6994b8ed…`、`1785c2c6…` 三条完整 digest 为准。
- 2026-08-02：latest-main candidate 首次正式 Architecture=`819 passed / 2 failed`，证据保留于
  `outputs/validation_runtime/architecture-fitness_20260801T211311Z/test_runtime_summary.json`；未启动
  后续 gate。两项失败均为 append-only current-authority drift：DEVX-006 历史测试仍把当前 Task
  Shadow v2 fragment count 固定为 `942`，OPS-073 EOF authority 仍持有变更前 `docs/task_register.md`
  hash。修复采用新的 TRADING-2478 compatibility EOF section：冻结 OPS-073 历史 prefix，不改写
  DEVX-006 的历史 `928` fragment authority；初始接管本候选实际漂移的 14 个既有 source paths，登记
  两份新文档及 `15 v1 + 15 v2` stable task fragments，并把当前 task count/fragment count 权威设为
  `957`。本次失败是正式 fail-closed 证据；修复后从 Architecture 开始在同一 final tree 重跑五级门。
- 2026-08-02：Architecture v2=`821 passed / 1 failed`，证据保留于
  `outputs/validation_runtime/architecture-fitness_20260801T212955Z/test_runtime_summary.json`；失败仅为
  `arch_004e_test_manifest.yaml` 未覆盖新增 compatibility authority test。canonical devex generate 后只有
  该 test manifest 发生 deterministic byte drift，fitness validate=`PASS / 0 violations`；TRADING-2478
  current authority 因此从 14 个扩为 15 个 superseded paths。针对 DEVX-006、OPS-073 predecessor、
  TRADING-2478 current authority 与 devex freshness 的回归=`4 passed`。Contract 及后续门仍未启动；
  Architecture v3 必须在这棵最终内容树上 PASS 后才可继续。
- 2026-08-02：final-tree Architecture/Contract/Integration/Reproducibility 分别以
  `822/276/995/24 passed` 通过；首次 Full=`7907 passed / 5 skipped / 9 failed`，9 项全部在
  `test_o1_relative_opportunity_event_attempt_ledger.py` 的 fixture setup 因 candidate worktree 缺少
  policy-referenced ignored artifact
  `outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_dq_gate.json` 而失败，未执行被测
  ledger 逻辑。该 artifact 由 tracked policy
  `config/research/o1_relative_opportunity_capability_audit_v1.yaml` 固定 SHA-256=
  `ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`；main checkout 的 4057-byte
  文件与 authority 完全一致。candidate 只允许从 main 复制该 exact byte fixture，复制前后复核 SHA，
  不纳入 task commit；它与 candidate 的 ignored validation outputs 在 validated commit/push 后一并清理，
  main 中 canonical retained artifact 不删除。Full 失败 artifact=`full_20260801T214256Z`；补齐依赖后
  先做 focused replay，再以 `failure_fix_rerun` 和该 parent artifact 重跑 Full，未 PASS 前禁止集成。
