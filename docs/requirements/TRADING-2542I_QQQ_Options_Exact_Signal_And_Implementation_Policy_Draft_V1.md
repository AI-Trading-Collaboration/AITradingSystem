# TRADING-2542I：QQQ Options Exact Signal And Implementation Policy Draft V1

最后更新：2026-08-29

稳定任务 ID：
`TRADING-2542I_QQQ_OPTIONS_EXACT_SIGNAL_AND_IMPLEMENTATION_POLICY_DRAFT_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

Owner 指令：Project Owner 已确认继续按“既有趋势信号只负责方向，QuantConnect 只负责期权实现与
收益计算”的路径推进。该指令授权形成可复核草案，不等同于 exact policy freeze，也不授权真实
DQ、QuantConnect project mutation/backtest、raw option payload、orders、fills、positions、paper、live、
production 或 broker action。

production effect：`none`

broker action：`none`

## 1. 目标

承接 TRADING-2542H 的 S1 独立 serial wave，完成两个此前混在一起的 Owner 输入面：

1. 对现有趋势研究链中的候选方向信号做 exact-source readiness 审计，明确推荐的 source enum、
   mapping 和仍缺的 immutable artifact；
2. 把 TRADING-2509 中与 baseline 相关的 selection、execution、accounting、lifecycle 与 acceptance
   slots 展开为一份完整、可机械校验但不可执行的 Owner review draft。

本任务不把 option chain、IV、Greeks、OI、volume、quote、option return 或结果 bucket 送回趋势模型。
期权链路仍只是已有方向信号的 implementation retest。

## 2. 已核验的现状

### 2.1 可复用权威

- TRADING-2483 已冻结 normalized `LONG_CALL/LONG_PUT/FLAT` daily signal package、lag=1、XNYS、
  exact coverage、canonical DQ receipt 与 immutable run-package mechanics；
- TRADING-2485..2488 已冻结 deterministic selection、next-independent-minute execution、cash
  accounting 与 expiry/lifecycle mechanics，但所有投资解释数值仍为
  `OWNER_REVIEW_REQUIRED_BASELINE`；
- TRADING-2509 已冻结 37-slot successor inventory、DAG 与 evidence classes，没有冻结 per-slot values；
- TRADING-2541 V3 只证明 2021-02-22..2025-12-02 的 1202/1202 QC chain transport coverage，不证明
  signal、selection、fill、lifecycle 或 return；
- TRADING-2542H 已把 FMP/Cboe/Fed/BLS/BEA 降为 optional result-blind overlay，不再阻塞 baseline。

### 2.2 现有趋势信号候选的限制

`first_layer_composer_v2` 是当前最接近所需方向语义的现有 producer：

- source field=`first_layer_composer_v2_predictions.csv:trend_state`；
- enum=`risk_on/constructive/neutral/defensive/risk_off`；
- policy status=`pilot_baseline`、`research_only=true`，只输出状态，不输出权重；
- 既有 current-state evidence 的 actual signal range 是 2023-02-22..2026-03-27，不能覆盖
  primary start=2021-02-22；
- 既有 baseline rewrap 是 `schema_migration_poc`、`non_pit_source_evidence_only`，明确不能冒充历史
  executable candidate artifact；
- 当前 retained source 曾包含重复日期，不能直接作为 exact one-row-per-session package。

因此本任务可以推荐其 enum 作为 baseline direction semantics，但不能声称已有 1202-session exact signal
artifact。后继冻结必须从受治理 producer、exact code/config/input/DQ identity 重新生成完整 package，不能把
POC rewrap、缺口回填、跨日 forward-fill 或手工 CSV 当作 authority。

## 3. 推荐 baseline mapping（草案，不是授权）

推荐映射：

| source `trend_state` | option action | 理由 |
|---|---|---|
| `risk_on` | `LONG_CALL` | 与明确增持风险的方向语义一致。 |
| `constructive` | `LONG_CALL` | 与保持/增加 QQQ 风险暴露的语义一致。 |
| `neutral` | `FLAT` | 不从中性状态创造 option alpha。 |
| `defensive` | `FLAT` | 既有语义是降风险/转 SGOV，不等同看空或买 put。 |
| `risk_off` | `FLAT` | baseline 验证 long-premium call implementation；不把去风险偷偷改成净空头。 |

`LONG_PUT` 不在 baseline 使用；未来如需检验 bearish implementation，必须独立标记
`LONG_PUT_SENSITIVITY`、独立 manifest/结果 lane 和独立 Owner freeze。unknown/missing/non-session source
signal 直接 typed `INVALID_SOURCE_SIGNAL`，不得映射为 FLAT。

effective session 继承 TRADING-2483：D session close 后生成，D+1 首个有效 XNYS session 才可生效。

## 4. 推荐 option implementation（草案，不是授权）

### 4.1 Selection

- QQQ、long-premium、single-leg、call-only baseline；FLAT 不选约；
- DTE inclusive range=`30..45`，target=`35`；
- prior-completed-session model absolute delta inclusive range=`0.45..0.60`，target=`0.50`；
- moneyness 仅作 sanity eligibility，`underlying_price/strike` inclusive range=`0.90..1.10`；
- selection quote 必须 two-sided、non-crossed、ask>0，age<=`60 seconds`；
- relative spread=`(ask-bid)/mid`，上限=`0.20`；
- prior-session OI floor=`100`；volume floor 建议 `G5_NOT_APPLICABLE`，因为 selection-time 当日 volume
  会引入 lookahead，prior-day volume 在已有 daily OI + live quote baseline 中不再重复设硬门槛；
- stable rank=`abs(delta-0.50) -> abs(DTE-35) -> relative spread -> OI descending -> expiry -> strike -> SID`；
- 无合格合约=`NO_ELIGIBLE_CONTRACT`，保持 cash，不放宽任何条件。

这些值是便于首次 bounded baseline 的 pilot proposal，不是经验最优值，也不得从后验收益选择。

### 4.2 Execution

- D+1 第一个完整 minute bar 后选约；下一独立 minute 才能 submit；
- entry buy 使用有效 ask-side marketable limit，exit sell 使用有效 bid-side marketable limit；
- baseline price stress proposal=`USD 0.01/share` adverse adjustment，另行保留 spread/fixed-bps/delay
  sensitivities，不从 sensitivity 中挑最佳 baseline；
- order timeout=`5 minutes`，cancel 后当日不重试、不换约；reject/no-fill 保持 cash；
- 单笔最多 `1 contract`，因此不存在 fractional-contract partial fill；任何平台异常 partial state fail closed；
- fee proposal=`USD 0.65/contract/side`；最终 freeze 必须绑定 fee source/effective dates；
- 不使用 market order、mid/last、daily close、same-bar 或 fill-forward。

### 4.3 Accounting

- initial cash proposal=`USD 100,000`；cash account、USD、no margin；
- 每次 premium budget 上限=`2% of pre-trade NAV`，同时最多 `1 contract`；不足购买一张即 no order；
- reservation=`limit premium * platform multiplier + fee buffer`；实际 multiplier 必须来自 QC symbol
  properties 且等于 `100`，否则 run invalid；
- fee buffer=`USD 0.65/contract/side`，cash quantum=`USD 0.01`，rounding=`ROUND_HALF_EVEN`；
- cost basis=`FIFO`，fees included；sell proceeds settlement lag proposal=`1 XNYS session`，必须在后继
  platform-semantics freeze 中复核；
- optionized baseline 的闲置 cash 不静默获得 SGOV return；`SGOV_CARRY_COMPARATOR` 单独报告，不并入
  option ledger；
- negative settled cash、QQQ share delivery、short option/underlying 和 hidden leverage 全部禁止。

### 4.4 Lifecycle

- 同时最多一个 open option position；source 变为 FLAT 时在下一合法 execution event 发起 exit；
- pre-expiry mandatory exit guard proposal=`7 XNYS sessions`；
- 禁止 atomic/same-session roll；若 exit 后 source 仍是 LONG_CALL，只能在下一有效 session 按完整 fresh
  signal/selection/execution 流程重新进入；
- exercise、assignment、share delivery、unresolved expiry、corporate-action mapping ambiguity 均使 run invalid；
- terminal/exit valuation 使用有效 bid liquidation mark，quote age<=`60 seconds`；没有有效 mark 时为
  `INSUFFICIENT_PLATFORM_EVIDENCE`，不得用 daily close 或本地价格替代。

### 4.5 Result admission

- primary requested/evaluated range 固定 2021-02-22..2025-12-02、exact 1202 sessions；
- no-contract/no-fill/cancel session 作为 cash-preservation facts 保留，不从样本删除；
- DQ/PIT/lineage/chronology/accounting/lifecycle invalid run 不进入 aggregate return conclusion；
- baseline 只允许一个 preregistered policy；所有 sensitivities 独立编号并受 multiplicity disclosure；
- same signal identity 下并列 `UNDERLYING_IMPLEMENTATION` 与 `OPTIONIZED_IMPLEMENTATION`；
- 结果最多支持 research comparison，不允许 investment promotion、自动参数更新或策略结论；
- raw option rows、完整 chain 与 contract-level quote history 不回流本仓库。

## 5. 机械合同与停止条件

新增 strict draft policy/loader 必须：

1. 绑定 2483、2485..2488、2499、2509、2541 V3 与 2542H exact file hashes；
2. 完整覆盖 2509 的 37 个 successor slots，逐项标记 `PROPOSE_G2` 或带 rationale 的
   `PROPOSE_G5_NOT_APPLICABLE`；
3. 校验 source candidate、五态 mapping、baseline action set、日期/session、pilot values、rank order、
   execution/accounting/lifecycle/result admission 与 safety boundary；
4. 拒绝 extra/missing slot、duplicate slot、unknown mapping、LONG_PUT baseline、engine default、raw export、
   executable flag 或 external action；
5. 固定 terminal=`OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST`。

只有 Owner 后续对 exact draft file SHA/canonical SHA 明确冻结，并且 exact 1202-session signal package、
manifest replay 与所有 predecessor hashes PASS 后，才能另立 executable-research-only manifest。该 freeze
仍不等于 QuantConnect run authorization。

## 6. 实施阶段

### S0：任务登记与 source readiness audit

- 登记本任务和 supporting requirement；
- 固化 first-layer candidate 的可用语义与不可冒充事实；
- 不读取/下载 raw option payload，不运行真实 DQ/backtest。

### S1：non-executable policy draft

- 新增 strict policy、loader 与 negative/golden tests；
- 覆盖完整 mapping、37-slot inventory 与 safety boundary；
- 更新 system flow 与 Atlas 当前 blocker/next action。

### S2：Owner exact freeze（后续）

- Owner 审阅 exact file/canonical SHA 与全部 policy rows；
- exact-freeze source producer、完整 1202-session signal package 和 mapping；
- 所有 policy predecessor/slot/manifest replay PASS；
- 本阶段仍保持 QC run/order/fill/position=0。

### S3：bounded QuantConnect run（需再次单独授权）

- 只在 S2 PASS 后提交固定 project/code/manifest/maxima 的 R1 research sandbox run；
- 本地只接纳 export-safe aggregate/identity evidence；
- paper/live/production/broker/orders outside QC simulation=`0`。

## 7. 文件权属与生命周期

Task-owned：

- 本 supporting requirement；
- `config/research/qc_qqq_options_exact_signal_implementation_policy_draft_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/exact_signal_implementation_policy_draft.py`；
- `tests/test_qqq_options_exact_signal_implementation_policy_draft.py`；
- 对应 architecture module/flow fragments。

Coordinator-owned：canonical task source/index/views、`docs/system_flow.md`、Atlas current-state projection、
generated architecture/report-flow/compatibility authority 与 formal validation artifacts。

复用主 checkout，不创建额外 worktree/clone/cache。known-unrelated exclusion
`docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 diff、不 stage、不修改。

## 8. 当前安全状态

- `scope=non-executable DATA_RESEARCH`；
- `draft_authorized=true`；
- `owner_exact_freeze=false`；
- `exact_1202_session_signal_package_present=false`；
- `manifest_generation_authorized=false`；
- `real_dq/qc_backtest/qc_project_mutation/provider_query=false`；
- `raw_option_payload_download_or_export=false`；
- `orders/fills/positions=0`；
- `paper/live/production/broker=false/none`；
- terminal=`OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST`。

## 9. 进度记录

- 2026-08-29：Owner 指示继续推进既有趋势信号到 QuantConnect option implementation retest 的后续计划。
  READ_ONLY governed preflight PASS；local main=origin/main=`57abd3c65b88d740f7e50b9eff06c3e9bc1cb42e`、
  active lease=0、worktree audit PASS。审计确认 first-layer 五态可作为推荐语义来源，但现有 artifact 仅为
  2023-02-22 起的 source evidence/POC，不能冒充 2021-02-22 起 exact package。本波只形成严格
  non-executable draft，不访问 QuantConnect、不运行真实 DQ/backtest。
- 2026-08-29：strict draft policy 已实现，file/canonical SHA-256=
  `22335aa324ffb13c9917b65ad57f51916831ecd95c05fe357f7faa13f74b57d0`/
  `45c247010f47ad3172215f90aa7c9cd40044b5332284e1789095d230075a5d83`。五态 call-or-flat mapping、
  37-slot exact-once inventory、pilot selection/execution/accounting/lifecycle/result proposals、predecessor
  hashes 与 zero-execution safety 均可机械重放；focused policy + Atlas suite=`38 passed`，Ruff、strict
  mypy、py_compile PASS。任务转为 `BLOCKED_OWNER_INPUT`：Owner 尚未 exact-freeze 本草案，完整
  1202-session signal package 尚未生成；QC/provider/real DQ/backtest/orders/fills/positions/production/broker=0。
