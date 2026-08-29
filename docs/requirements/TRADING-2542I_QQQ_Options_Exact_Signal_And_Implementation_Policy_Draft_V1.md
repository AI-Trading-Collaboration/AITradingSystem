# TRADING-2542I：QQQ Options Exact Signal And Implementation Policy Draft V1

最后更新：2026-08-29

稳定任务 ID：
`TRADING-2542I_QQQ_OPTIONS_EXACT_SIGNAL_AND_IMPLEMENTATION_POLICY_DRAFT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

Owner 指令：Project Owner 已确认继续按“既有趋势信号只负责方向，QuantConnect 只负责期权实现与
收益计算”的路径推进，并于 2026-08-29 指示“好的，那就冻结吧，你继续推进”。该指令 exact-freeze
`qc_qqq_options_exact_signal_implementation_policy_draft_v1@1.0.0-draft.1` 的 file/canonical SHA
`22335aa324ffb13c9917b65ad57f51916831ecd95c05fe357f7faa13f74b57d0`/
`45c247010f47ad3172215f90aa7c9cd40044b5332284e1789095d230075a5d83` 及全部 37 个 proposal rows，
授权继续推进 non-executable `DATA_RESEARCH` signal-package preparation。Project Owner 随后于同日对
此前请求的“真实 DQ-backed producer regeneration/admission”回复“授权”，因此 S2B 允许读取现有真实
缓存、运行 exact-window canonical DQ、按原有 `first_layer_composer_v2` 语义再生成并接纳候选 package；
不得为了补齐期权输入而修改趋势模型 policy、缩短训练窗口、使用 warm-start diagnostic、填充 `FLAT`
或跨 session forward-fill。QuantConnect project mutation/backtest、raw option payload、orders、fills、
positions、paper、live、production 与 broker action 仍未授权。

Project Owner 在 generic operational forecast producer 修复并发布后回复“授权了”。本次授权精确绑定为：
允许读取现有真实 cache，按分段资产可用期运行 `2018-01-02..2025-12-02` extended-history canonical DQ，
物化并接纳 exact `1202/1202` operational source，生成 TRADING-2483 immutable signal package 并执行
canonical manifest replay；只有上述 replay=`PASS` 后，才允许提交一次 fixed-project/fixed-code/fixed-manifest/
fixed-maxima 的 bounded QuantConnect `DATA_RESEARCH` backtest。该授权不允许购买数据、provider query、raw
option payload 导出、本地 option repricing、趋势模型重设计、paper/live/production/broker，QC simulation
之外的 orders/fills/positions 必须为 0。

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
5. 原 draft loader 固定 predecessor terminal=
   `OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST`；独立 freeze admission 在双 SHA 与
   37/37 replay PASS 后固定 successor terminal=
   `OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST`。

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

### S2：Owner exact freeze（当前）

- Owner 已审阅并 exact-freeze draft file/canonical SHA、五态 mapping 与全部 37 个 policy rows；
- 原 draft bytes/status/version 保持 immutable，不在原文件内改写 `owner_frozen`；
- 新增独立 freeze admission，机械绑定 Owner 指令、draft 双 SHA、mapping 与完整 slot inventory；
- signal package 尚未生成或接纳；当前 retained source 从 2023-02-22 开始，不能补齐、forward-fill 或
  伪装成 2021-02-22 起 exact 1202-session authority；
- 本阶段仍保持 QC run/order/fill/position=0。

### S2B：exact signal package preparation（当前后继）

- 只允许构建/复核 fail-closed producer、coverage、lineage 与 manifest replay 门禁；
- 只有 governed producer 同时绑定 exact code/config/input/DQ/PIT identity、覆盖
  2021-02-22..2025-12-02 全部 1202 个 XNYS sessions 且一日一行时，才能接纳 package；
- 缺 session、重复 session、unknown state、POC rewrap、手工 CSV、跨日填充或无 DQ/PIT identity
  一律 typed reject；
- Owner 已单独授权读取现有真实 cache、运行 canonical real DQ 与既有 producer regeneration/admission；
- regeneration 必须保持原有趋势模型 policy、训练窗口、feature/label 语义和 PIT cutoff 不变，不得把
  coverage 问题转化为针对期权的趋势模型重设计；
- 若原 producer 在冻结语义下仍不能覆盖 1202 sessions，则形成 exact typed blocker 与 DQ/coverage
  证据，不生成 signal package，不以 `FLAT`、warm-start diagnostic、POC rewrap 或手工行绕过。

S2B 已按 Owner 授权执行。exact-window canonical DQ=`PASS`，说明 QQQ/SGOV/TQQQ、
DGS10/DGS2/DTWEXBGS 与 secondary cross-check 数据本身不是当前 blocker；冻结 producer 再生成后只有
`630/1202` 个唯一 session，缺 `572`，存在 `588` 个重复 session / `1134` 个多余行，并有 `73` 行
`decision_at` 落在非 XNYS session。source admission 因此为 `REJECT`，TRADING-2483 package writer、
manifest replay 与 QuantConnect 均未运行。下一步需要 Owner 审阅独立 operational forecast producer
合同：是否允许声明 evaluation window 之前的训练历史、把 label construction 与 prediction emission
解耦以覆盖末端、从 overlapping walk-forward folds 形成唯一 out-of-sample session row，以及统一
next-XNYS timing；这些都不能作为“期权适配”静默改进现有趋势 policy。

### S2C：generic operational forecast producer repair（当前）

Owner 已指示“先修复”，授权范围绑定到通用 first-layer producer 合同、实现与 synthetic validation，
不是趋势到期权策略转换，也不授权 extended real-cache materialization 或 QuantConnect/backtest。V1 合同：

- 保持 `MODEL_SPECS` 四个 scorecard、`first_layer_composer_v2` 五态 precedence、504-session rolling
  train window、20-session label horizon、0.65 positive-score quantile、各 model positive sample floor 与
  21-session refit cadence；
- evaluation window 固定为 `2021-02-22..2025-12-02` 的 1202 个 XNYS sessions；训练历史从
  `2018-01-02` 开始，以容纳 126-session feature warm-up、504 个成熟训练样本与 20-session label maturity；
- SGOV 上市前只允许 SHY daily return 作为显式 `TRAINING_INITIALIZATION_ONLY` cash-reference proxy，
  通过 return splice 连接到 SGOV；evaluation window 内 proxy row 必须为 0，不能把 proxy 结果冒充 exact
  evaluation input；
- 每次 fit 只选择 `label_end_session <= fit_session` 的最新 504 个样本，消除原 offline validation 把
  尚未成熟 forward outcome 带入训练的 PIT 风险；
- 每个 evaluation session 只使用最近一次 fit，禁止 overlapping validation rows 重复发射；prediction
  不 join 当日/未来 label，因此末端 2025-12-02 仍可合法发射；
- `date/known_at/available_at` 表示已完成 feature session，`decision_at` 必须是 exact next valid XNYS
  session，不再使用普通 `BDay(1)`；
- producer 必须先看到 extended-history DQ=`PASS` 与 exact DQ identity 才允许运行；本开发波只用 synthetic
  frame 验证上述合同，不读取或物化扩展真实训练窗，不生成 options signal package。

这不是对旧 research artifact 的覆写。旧 `first_layer_composer_v2_predictions.csv` 继续作为 immutable
research/diagnostic evidence；新 producer 仅复用其方向语义和冻结 policy，以单独版本输出 operational
forecast source。任何 scorecard weight、composer precedence、training window、label horizon、cash proxy、
evaluation window 或 executable scope 变更都必须重新审阅。

### S2D：extended-history real materialization 与 manifest replay（当前）

- S2D-0 先执行共享 XNYS calendar contract audit。首次真实 attempt v1 已 fail-closed：canonical DQ
  把 `2018-12-05` 识别为 `prices_internal_trading_day_gap`；NYSE 官方 `RB-18-06` 明确该日因
  President George H. W. Bush National Day of Mourning 全日休市。必须先把该事实加入 reviewed
  `us_equity_special_closure_registry` 并通过 calendar/DQ 回归；旧 `1.0.0` bytes 按 exact SHA 归档供旧
  attribution/package replay，新 package/adapter policy v2 只绑定 `1.1.0`；不得删除、覆盖或把 v1 FAIL
  改写为 PASS；
- 以独立 execution policy 绑定已发布的 `first_layer_operational_forecast_producer_v1@1.0.0`，不回写其
  synthetic-development safety bytes；
- canonical DQ 分成三个互补且不伪造上市前或 secondary-source 历史的 scope：`QQQ/TQQQ/SHY + rates`
  从 2018-01-02 且要求 secondary；`SGOV + rates` 从 2020-05-28 为 primary-only（本地 Marketstack SGOV
  从 2021-02-22 才开始）；`QQQ/TQQQ/SGOV + rates` primary evaluation 从 2021-02-22 且要求 secondary；
- 每个 scope 必须通过 typed download publication、canonical receipt verification 与 exact checksum replay；
- real producer receipt 必须绑定三组 DQ、源 cache、policy、code、predictions 与 fit audit identity，并证明
  `1202/1202`、unique、label maturity、terminal emission、evaluation proxy rows=0、forward-label columns=0；
- 只有 existing exact source admission 与 TRADING-2483 package canonical replay 同时 PASS，才进入 S3。

### S3：bounded QuantConnect run（已授权，仍受 S2D PASS 前置约束）

- 只在 S2D PASS 后提交一次固定 project/code/manifest/maxima 的 R1 research sandbox run；
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
- `owner_exact_freeze=true`；
- `exact_1202_session_signal_package_present=true`；
- `signal_package_preparation_authorized=true`；
- `real_dq_and_existing_producer_regeneration_authorized=true`；
- `generic_operational_forecast_contract_and_implementation_authorized=true`；
- `extended_training_history_real_dq_or_materialization_authorized=true`；
- `manifest_generation_authorized=true` only when exact source/DQ/PIT identity and 1202/1202 coverage PASS；
- `qc_backtest_authorized=true` only after exact source admission and TRADING-2483 manifest replay PASS；
- `qc_project_mutation_authorized=true` only for one fixed bounded DATA_RESEARCH project action after that PASS；
- `provider_query=false`；
- `raw_option_payload_download_or_export=false`；
- `orders/fills/positions=0`；
- `paper/live/production/broker=false/none`；
- terminal=`EXACT_SIGNAL_PACKAGE_REPLAY_PASS_QC_FIXED_MANIFEST_REQUIRED`。
- `exact_window_canonical_dq_status=PASS`；
- `regenerated_signal_unique_sessions=630/1202`；
- `regenerated_signal_missing_sessions=572`；
- `regenerated_signal_duplicate_sessions/excess_rows=588/1134`；
- `regenerated_signal_non_xnys_decision_at_rows=73`；
- `signal_source_admission=REJECT`；
- `signal_package_writer/manifest_replay/quantconnect=NOT_RUN`。
- `operational_forecast_development_validation=SYNTHETIC_ONLY`；
- `operational_forecast_real_materialization=V3_PASS`；
- `v1_failed_dq_receipt_sha256=84bf76d8634732dbd7dcb482e96591a848fea706aa3745346ba138ccd0de7a05`；
- `v2_failed_dq_receipt_sha256=ea039e75f0e17ee8bffe3fdc90e891d5afa776fac0abd362b3ac7ed69d98ac55`；
- `v3_real_materialization_receipt_sha256=f508581f98b1fa64763b8488568cf0631bfa260fea2b7aa55f9d7f5a0590a230`；
- `v3_manifest_replay_receipt_sha256=1106de7d6e9b63a20d9e68d7228267ea6777a84b2ea1de16215699d3fa7cd9bc`；
- `conditional_quantconnect_backtest=AUTHORIZED_NOT_RUN_READY_FOR_SEPARATE_FIXED_MANIFEST_WAVE`；

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
- 2026-08-29：Project Owner 指示“好的，那就冻结吧，你继续推进”，因此 exact-freeze 上述 draft 双 SHA、
  五态 call-or-flat mapping 与 37/37 proposal rows。为保持已批准 bytes 不变，冻结状态由独立 admission
  表达，不回写原 draft。任务转回 `IN_PROGRESS`，继续实现 non-executable freeze replay 与 exact signal
  readiness gate；现有 source 缺少 2021-02-22..2023-02-21 历史且真实 DQ 未授权，禁止 gap fill、POC
  rewrap、manifest dispatch 或 QuantConnect backtest。
- 2026-08-29：独立 freeze admission 已实现，file/canonical SHA-256=
  `a89c3c245795bda3733b9579cbb0f78cf16b5f30ec6115217acab10b26b72d34`/
  `86c6e774e387782788281e370a844dec1f0061d784d62e14ffe02b4e44087017`。loader 机械重放获批
  draft 双 SHA、完整 section inventory、五态 mapping 与 37/37 slot IDs，并证明原 draft 中
  `owner_frozen=false`/`owner_exact_freeze=false` bytes 未被回写。focused freeze/draft/Atlas suite=
  `62 passed`；Ruff、strict mypy、py_compile PASS。exact signal package 仍为 0/1202 admitted，下一步
  需要 Owner 单独授权真实 DQ-backed producer regeneration；QuantConnect backtest 仍需再单独授权。
- 2026-08-29：Project Owner 对上述单独授权请求回复“授权”。本轮把授权精确绑定到现有真实缓存的
  exact-window canonical DQ、冻结 `first_layer_composer_v2` 语义下的 regeneration、1202-session
  coverage/lineage/manifest replay 与 package admission；不授权修改趋势模型以适配期权，也不授权
  QuantConnect、raw option payload 或任何 execution/broker action。
- 2026-08-29：S2B 真实复核完成。先对当前 full cache 直接运行 exact historical DQ，确认其因 current
  manifest window、截止日后的 price/rate rows 与 out-of-scope VIX session 记录而 fail closed；随后使用
  独立 exact-date/identity projection，绑定 full-cache SHA/row lineage，以
  `LEGACY_LOCAL_CACHE_IMPORT / OPAQUE_LEGACY` 发布 isolated canonical download transaction，未冒充 live
  provider 或原始下载。最终 exact requested/evaluated `2021-02-22..2025-12-02` canonical DQ=`PASS`，
  receipt/report SHA-256=`425c10b33fe1d5e5868cab3496cb7c8ff7ce971ebc60b74a6a4333486f72a841`/
  `715934220efc952e04a7868f18ba5dbf4c874e9772377c5e9330308b01d60ce8`。
- 2026-08-29：在不改 `first_layer_composer_v2` policy、训练窗口、feature/label 语义或 PIT cutoff 的
  条件下完成 producer 再生成；producer terminal=`WINDOW_COVERAGE_INCOMPLETE`。生成 source SHA-256=
  `731f77584c5cb6edb8dce878937e5c918192f3fbbd9c89c907d870c43d08d677`，共 1764 行，但只覆盖
  630 个唯一 XNYS session（2023-02-22..2025-08-26），缺 572，重复 session=588、excess rows=1134，
  non-XNYS `decision_at` rows=73。exact admission receipt/report/lineage SHA-256=
  `c61ae393b2ed08021ce15da927b3cef25c66289fb38511ab1faa4bc196bb9837`/
  `4ecabbf06c0b7d572010d9c493a7e9e6b72af5f53eb83ef47627cab1888547f4`/
  `6475186902cb48b4bf9029bc2d2f5148601f19aac6ba0eaa06c284771c063cde`。terminal typed
  `REJECT / NOT_RUN_SOURCE_REJECTED`；没有调用 package writer、manifest replay 或 QuantConnect。
  任务转为 `BLOCKED_OWNER_INPUT`，等待单独审阅 operational forecast producer 合同，而不是用期权
  数据重做方向模型。
- 2026-08-29：publication transaction
  `trading-2542i-exact-signal-admission-20260829-v15` 在 `TASK_SOURCE_PRE_WRITE` 后按 `FAILED` 释放，
  candidate/full/main/push 均未发生。原因是现有 producer runner 把若干 review authority 写到全局 tracked
  路径，且 Atlas authority 要求 page source 先形成 exact lane commit。所有额外 tracked side effect 已按
  pre-run HEAD 精确复原并经 governed worktree audit 确认不再 dirty；真实再生成的 source、DQ 与 admission
  证据保留在本任务隔离输出和上述 immutable hashes。后继 transaction 必须从 exact lane commit 重开，
  不绕过 Atlas exact-commit 或 declared-path gate。
- 2026-08-29：publication transaction
  `trading-2542i-exact-signal-admission-20260829-v17` 在 `GENERATED_REBUILD_PRE` 后按 `FAILED` 释放，
  candidate/full/main/push 均未发生。`docs/system_flow.md` 本次新增 flow 后，report-flow source seal 从旧值
  发生预期漂移；重算结果为 byte count=`2309725`、file/LF SHA-256=
  `a4248239b3f3b61c4971e819c91c5381070767b996ba281afa85c981d3ef365a`、git blob=
  `f2b382eeacd169cbd30f22b603dd8855701e9e11`，entry count 仍为 `1159`。由于对应 frozen test 路径未在
  v17 transaction 声明，未越权写入；v17 已释放 lease。后继 transaction 必须显式声明
  `tests/test_devx_006d_report_catalog_flow_authority.py`，并从包含 seal config/test 更新的 exact lane head
  重跑 architecture、Atlas、report-flow 与 compatibility authority。
- 2026-08-29：publication transaction
  `trading-2542i-exact-signal-admission-20260829-v18` 完成五类 generated authority 重建并形成候选
  `148e2c59ee78ed29df72f87778eff66ac4f96eb5`，但首个 formal `architecture-fitness` 在并行执行中
  以 `876 passed / 2 failed` fail closed；runtime artifact 位于
  `outputs/validation_runtime/architecture-fitness_20260829T051448Z/test_runtime_summary.json`。两项失败同源：
  Atlas coverage frozen assertion 在 `GENERATED_REBUILD_POST` 后加入，使 architecture manifest stale；同时
  TRADING-2542I 新增 module/test 后 deprecation repository count 应从 `1172/1332` 更新为 `1173/1333`，
  inventory id 实测为 `arch_004g_deprecation_inventory_ed9b563f19b9fa354286`。由于对应
  `tests/test_arch_004g_deprecation.py` 未在 v18 声明，不在原 transaction 上越权修补；contract、
  integration、reproducibility、full、local main 与 push 均未运行。后继 transaction 必须显式声明该
  frozen test，重跑全部 generated authority 和五级 formal validation。
- 2026-08-29：publication transaction
  `trading-2542i-exact-signal-admission-20260829-v19` 形成候选
  `ec394443d873426b6ced0422d6c042442bc3baee`；architecture-fitness=`878 passed`、
  contract-validation=`278 passed`、integration=`995 passed`、reproducibility=`24 passed`。Full 未实际
  启动：coordinator 提前手动 checkpoint 到 `FULL_DISPATCHED`，而 `run_validation_tier.py full` 要求由
  runner 从 `FORMAL_VALIDATION_PRE` 原子校验并自行推进该阶段，因此以
  `PUBLICATION_PHASE_MISMATCH` fail closed。没有 Full pytest、local main、push、DQ/backtest 或外部动作。
  v19 必须按 `FAILED` 释放；后继 transaction 不再手动发送 `FULL_DISPATCHED`，由 Full runner 管理该
  checkpoint 与 `FORMAL_VALIDATION_RESULT`。
- 2026-08-29：Owner 指示“好的，那先修复吧”，任务恢复为 `IN_PROGRESS`。授权仅覆盖通用 first-layer
  operational forecast producer 的 policy、实现和 synthetic validation；不把期权结果作为趋势 feature/
  label，不运行 extended real-cache materialization、TRADING-2483 manifest replay、QuantConnect 或回测。
  V1 选择显式 training-only SHY→SGOV return splice，使 2018 起现有训练历史能够容纳 126-session feature
  warm-up、504 个成熟样本和 20-session label maturity；evaluation 从 2021-02-22 起仅使用 SGOV。producer
  把 label construction 与 feature-only emission 解耦，每 21 sessions refit、每个 session 只选最近 fit，
  并以 exact next-XNYS 取代 `BDay(1)`。
- 2026-08-29：publication transaction
  `trading-2542i-operational-forecast-20260829-v2` 在 `TASK_SOURCE_PRE_WRITE` 后按 `FAILED` 释放，
  candidate/formal validation/local main/push 均未发生。原因是 transaction 在 task-owned implementation
  尚未形成 exact lane commit 前取得；Atlas authority 必须绑定包含 producer policy、实现、task event 与
  live status 的 exact commit，不能把 dirty working-tree bytes 归到旧 HEAD。实现和 focused validation
  结果保留并先形成可审计 lane commit；后继 transaction 从该 exact lane head 重新取得，再按 generator
  order 重建 architecture、Atlas、report-flow 与 compatibility authority。
- 2026-08-29：generic operational forecast producer 已通过 final Full=`9940 passed / 3 skipped` 并以
  `a45cf6acbf95c1e0617ad5ff315dda9803c8b39e` 发布到 local/remote `main`。Project Owner 随后回复
  “授权了”；本轮把该指令绑定到 S2D extended-history real DQ/materialization、exact source admission、
  TRADING-2483 package canonical replay，以及仅在全部 PASS 后的一次 bounded QuantConnect DATA_RESEARCH
  backtest。R3、付费/provider/raw payload、本地 option repricing 与 QC 之外的 order/fill/position 仍为 0。
- 2026-08-29：首次 S2D real attempt v1 在 `training_proxy_history` canonical DQ fail-closed，唯一 blocking
  issue 为 `prices_internal_trading_day_gap`，样例是 QQQ/SHY/TQQQ 均缺 `2018-12-05`。DQ receipt SHA-256=
  `84bf76d8634732dbd7dcb482e96591a848fea706aa3745346ba138ccd0de7a05`；没有 producer、signal package、
  manifest replay 或 QuantConnect dispatch。官方 NYSE `RB-18-06` 证明该日为全日休市，因此先执行最小
  shared-calendar contract correction；v1 现场保留，新 attempt 使用独立 v2 identity，禁止覆盖旧证据。
- 2026-08-29：attempt v2 已证明 `training_proxy_history` DQ 通过，随后 `exact_sgov_history` 以
  `DQ_WINDOW_MISMATCH` fail-closed。primary SGOV/rates 覆盖 `2020-05-28..2025-12-02`，但本地 Marketstack
  SGOV 从 `2021-02-22` 才开始，导致 common coverage 被截断；这不是 SGOV 主源缺行。v2 receipt SHA-256=
  `ea039e75f0e17ee8bffe3fdc90e891d5afa776fac0abd362b3ac7ed69d98ac55`。v3 对 pre-evaluation SGOV
  scope 明确 `require_secondary_prices=false`，primary evaluation 仍保持 mandatory secondary reconciliation；
  禁止填充 secondary 历史或删除 v2 现场，QuantConnect 仍为 NOT_RUN。
- 2026-08-29：attempt v3 对 SGOV pre-evaluation scope 使用显式 primary-only DQ，training 与 primary
  evaluation scope 仍要求 secondary；三段 canonical DQ 均 PASS。generic producer 以 exact code
  `328c0f0cc68921212f56640e0b8a4fce01e44ee4` 生成 `1202/1202` unique sessions、evaluation proxy
  rows=`0`；exact source admission=`PASS`，TRADING-2483 signal package canonical reconstruction=`PASS`。
  materialization receipt/manifest replay SHA-256 分别为
  `f508581f98b1fa64763b8488568cf0631bfa260fea2b7aa55f9d7f5a0590a230` /
  `1106de7d6e9b63a20d9e68d7228267ea6777a84b2ea1de16215699d3fa7cd9bc`。本波不具 dispatch
  authority，QuantConnect 仍为 `AUTHORIZED_LATER_WAVE_NOT_RUN`；下一步先发布本证据，再建立 exact
  project/code/package/maxima 的单次 QC manifest wave。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v6` 完成 canonical task、architecture manifest 与
  Atlas rebuild 后，report-flow authority 以 `RCF_SOURCE_SEAL_DRIFT` fail closed；原因是本任务新增的
  artifact catalog 条目使 `docs/artifact_catalog.md` 从 frozen source identity 合法前进。v6 已按
  `FAILED` 释放，candidate/formal validation/local main/push/QuantConnect 均未发生。reviewed successor
  seal 为 byte count=`2003593`、file/LF SHA-256=
  `bb26e2c07d9bf80b58338548921c002e5d9827c1bd83d4783f8ffc00e8198b38`、git blob=
  `4c451ddc0b2f021715d834867c25f93d5f134e3f`，entry count 仍为 `558`；下一 transaction 从包含该
  seal config/test 的 exact lane head 重放全部五类 generator，不绕过 source seal gate。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v7` 在 artifact-catalog successor seal 已通过后，继续
  以 `RCF_SOURCE_SEAL_DRIFT` 拒绝旧 `docs/system_flow.md` identity；新增 real materialization/DQ/package
  flow 使 byte count=`2312640`、file/LF SHA-256=
  `d2e60201fd2ad741025b7b936c829af567cba57e7be83ed363da69dceddaba5b`、git blob=
  `712f7a43019d2775867cc4d73cf1b662999a126d`、entry count=`1160`。v7 已按 `FAILED` 释放，未形成
  candidate、formal validation、main/push 或 QuantConnect dispatch；下一 transaction 从包含两项
  successor seal 的 exact lane head 完整重放五类 generator。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v8` 完成全部五类 generated authority rebuild，随后
  focused/adjacent 回归以 `260 passed / 3 failed` fail closed。失败均为已声明且可精确修正的 frozen
  freshness assertion：report-flow successor 总 entry count 应由 `3088` 更新为 `3089`；deprecation
  repository count 应为 `1175 modules / 1335 tests`，inventory id=
  `arch_004g_deprecation_inventory_c78d774fd3eba11a1b82`，direct-writer count 仍为 `856`。v8 已按
  `FAILED` 释放，未形成 candidate、formal validation、main/push 或 QuantConnect dispatch；后继
  transaction 必须从包含上述 frozen assertion 的 exact lane head 重建 architecture manifest 后再验证。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v9` 的 focused/adjacent=`263 passed`，候选=
  `5ce80319b7bc4499b40cbd62a95061912d57f78d`；首个 formal Architecture=`763 passed / 115 failed`。
  115 项失败均由同一 compatibility successor coverage 缺口级联：最新 2542D source-hash authority 未接管
  本任务改动的 10 个既有 live-source 路径（日历 registry/policy、DQ attribution、signal package、QC
  adapter 及其 tests）。失败 runtime summary SHA-256=
  `0f5ce8e5c421ef7e7548a8b96e6df5158dfc95da66a1308aee40897c5b780150`。v9 已按 `FAILED`
  释放；Contract/Integration/Reproducibility/Full/main/push/QuantConnect 均未运行。修复仅向最新
  successor 的 `superseded_live_source_paths`/`sources` 增加精确 10 路径，不改写任何历史 section，且不改变
  数据流、DQ/PIT、策略或执行边界。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v10` 在 successor coverage 修复后，代表性 compatibility
  focused=`16 passed`，formal Architecture 收敛至 `877 passed / 1 failed`。唯一失败为 Batch4 历史冻结
  测试仍直接要求 active `us_equity_special_closure_registry.yaml` 等于旧 1.0.0 commit 字节，未识别本任务已把
  exact 1.0.0 bytes 归档到 `config/data/archive/us_equity_special_closure_registry_1_0_0.yaml` 后才把 active
  policy 升至 1.1.0。失败 artifact SHA-256=
  `e5f883c74d33179010c42bf38039c1ad5ae0140780d259a3ce9e824f0a030ba8`；v10 已按 `FAILED`
  释放。修复必须让该历史断言用 archive path 对比原 commit path，保留 active 1.1.0 与历史 1.0.0 双重
  immutability；不得回退日历修复或改写历史 compatibility section。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v11` 的 formal Architecture=`878 passed`、
  Contract=`278 passed`、Integration=`995 passed`、Reproducibility=`24 passed`；Full=
  `9852 passed / 3 skipped / 101 failed`，runtime summary SHA-256=
  `077a7e491eac2b398338c20b5fa839031b9fd1034a714899a8cadce6f5677e3f`。失败集中于两个治理兼容性
  根因：DQ execution discovery fixture 复制 active calendar 1.1.0 bytes 却仍声明 version 1.0.0；新 calendar
  identity 支持直接改变了 TRADING-2483/2484 已 exact-freeze 的 v1 module bytes。v11 已按 `FAILED` 释放，
  未发生 local main/push/QuantConnect dispatch。修复边界冻结为：把 v1 module 恢复到原 LF SHA-256
  `ada45b6768b50180f2c21a54b0bed8c3bbf2b1a16ca965767b32569c0fce0cac` /
  `86420ad9875cac47c5317ceeeda8892f5aa8d2ad310a0fe84ac7762fa4cf90a8`，把 1.1.0 支持隔离到 v2 module，
  并让 current-default DQ fixture 声明 1.1.0；不得改写历史政策或放宽 replay/DQ gate。聚焦回归进一步
  证明 frozen v1 module 必须继续解析其原 1.0.0 calendar，而 current DQ/trading/v2 consumer 必须解析
  active 1.1.0；因此 calendar module 提供显式 legacy/current path 分离，且 2026-08-04 capability-discovery
  authorization 仅在 path 与旧 hash 同时精确匹配时解析 immutable 1.0.0 archive。ARCH-005M1 Batch4 的
  历史 `trading_calendar.py` bytes 以 SHA-256
  `ec34df0b571b9250579499d4227f08c809cb79ec6f648c37445488a2c82de8d7` 存入 architecture archive snapshot，
  使历史 replay 与 active current-calendar 文案/路径可同时成立。
- 2026-08-29：v13 formal Architecture=`878 passed`、Contract=`278 passed`、Integration=`995 passed`、
  Reproducibility=`24 passed`；Full=`9945 passed / 3 skipped / 8 failed`，runtime summary SHA-256=
  `7af9ba0ca723138ffe01ee10932d8fdf4a2dd71c702c2a26c7e39980bb753d3b`。8 项失败是同一内容派生证据
  根因：`quality_execution.py` current/legacy calendar path 分离后，tracked DQ issue attribution inventory
  仍绑定旧 source SHA，继而使 rate issue review pack 的 current-inventory gate 级联失败。v13 已按 `FAILED`
  释放，未发生 main/push/QuantConnect dispatch。已按既有官方 builder 顺序重建 inventory 与 rate review
  pack，两者 validation 均 `PASS`；不改变 DQ issue semantics、source-owner decisions 或 isolation authority。
- 2026-08-29：publication transaction
  `trading-2542i-materialization-publication-20260829-v14` 在 refreshed DQ evidence 已形成候选
  `7322ba0f3e95fcd565e026c8bea941511a1e37a1` 后运行 Architecture，结果为
  `766 passed / 112 failed`，runtime summary SHA-256=
  `6c48eaae2b96ed201f3bfec53357f54898e7121c9cba9cefcb8765a6a3230f27`。112 项失败同源：最新
  TRADING-2542D compatibility successor 尚未接管两份 DQ 派生报告、两份 JSON evidence 与两份
  validation JSON 的新 hash；历史 section 与 DQ/PIT/策略/执行语义本身没有失败。Contract 在确认
  Architecture 失败后中止，Integration、Reproducibility、Full、local main、push 与 QuantConnect 均未运行。
  v15 只把上述 6 个精确路径加入最新 successor 的 `superseded_live_source_paths`/`sources` 并重建
  authority，不改写历史 section，也不改变趋势或期权逻辑。
- 2026-08-29：v15 将 6 路径 compatibility 修复形成 exact lane commit
  `3e8df2093b32c2cda1b09a9336dd9710fa9819a0`；定向 compatibility suite=`218 passed`。v16 formal
  Architecture=`878 passed`、Contract=`278 passed`、Integration=`995 passed`、Reproducibility=`24 passed`；
  Full=`9945 passed / 3 skipped / 8 failed`，runtime summary SHA-256=
  `9496cace36c2a030385664da65b83a1e196cf40825e411c315d6ce5b763bc26d`。8 项失败同源：approved rate
  attribution decision 仍绑定旧 review-pack id/SHA，而内容派生的新 pack 已以 id
  `dq_rate_issue_attribution_review_34ea0d1bce5e7a0bc67d83b5`、SHA-256
  `008e085a64ee3867472daf2f4fd9a328d393eb9d75c7f5398919c1be27266996` validation PASS。v17 只重绑
  decision config、runtime constant 与 exact-binding test，并让最新 compatibility successor 接管这 3 个
  live paths；decision id/version、6 个 approved sites、阈值、scope、DQ/PIT 与执行边界保持不变。
