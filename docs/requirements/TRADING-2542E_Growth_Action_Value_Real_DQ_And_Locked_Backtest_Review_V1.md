# TRADING-2542E：Growth Action Value 真实 DQ 与锁定回测复核 V1

最后更新：2026-08-25

稳定任务 ID：`TRADING-2542E_GROWTH_ACTION_VALUE_REAL_DQ_AND_LOCKED_BACKTEST_REVIEW_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`（授权已收到但未消费；缺少 executable contributor、signal 与 action policy）；

governed mode：`SINGLE_LANE`

`production_effect=none`；`broker_action=none`。

## 1. Owner 授权与解释边界

2026-08-25，Project Owner 在完成 TRADING-2542D 的 non-executable 合同冻结后明确要求：
“我授权，继续推进复核”。本任务把该指令解释并固定为：

- 授权对 frozen DQ/PIT V3 与 exact sheet V4 执行一次范围锁定的真实数据研究复核；
- 授权同一预注册 hypothesis 的一次 locked、zero-order Cloud backtest/research evaluation；
- backtest 只有在同一候选输出的 canonical DQ/PIT 全局终态为 `GLOBAL_PASS` 时才可被技术
  admission 接纳；DQ 非 PASS 时所有策略测量结果必须作废；
- 不授权 candidate/parameter search、第二候选、重跑、结果后调阈值、holdout 扩张、原项目修改、
  raw option row 导出、Object Store、公开分享、paper/live、broker、真实订单、真实成交、持仓或生产部署；
- frozen V3/V4 bytes 不原地修改。执行授权由独立 R1 manifest 绑定，研究结果不能把
  `OWNER_FROZEN_NON_EXECUTABLE_DATA_RESEARCH` 静默改写为生产或投资 authority。

Owner decision id：
`owner_decision:TRADING-2542E:2026-08-25:authorize_one_locked_real_dq_and_zero_order_backtest_review_v1`。

## 2. 上游 authority 与不可变身份

- DQ/PIT V3：
  `config/research/strategy_growth_action_value_canonical_dq_pit_contract_v3.yaml`；
  file/canonical SHA-256 分别为
  `96eafe7525704a8e0e260c9ed344adf3420f7e1c977e877a557856258fee3144` /
  `e8e180b147e1a88dad3776f886b8eb7398481b1518785b6a2243ae795f4a6ede`；
- exact sheet V4：
  `config/research/strategy_growth_action_value_threshold_exact_value_sheet_v4.yaml`；
  file/canonical SHA-256 分别为
  `c90c4cc22b8918e90641bf0553416a68458433bea750bd2064fcf98df7886215` /
  `00198bb84cd57f518d0370035b5a5a38b12c9804880d7bf1e475ddd80a77bfc2`；
- hypothesis：`BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1`；
- baseline：`equal_risk_qqq_sgov`；comparator：`exposure_matched_no_signal`；
- action universe 仅 `QQQ/SGOV`，不使用 options position、QLD/TQQQ、借入杠杆或隐藏杠杆；
- selected evidence lane：`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`；
- primary requested/evaluated range：`2021-02-22..2025-12-02`，exact 1202 sessions；
- first-target prior：独立 `2021-02-19`，不得扩入 target inventory；
- session inventory LF SHA-256：
  `d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0`。

TRADING-2541 已证明 transport completeness=`1202/1202`，并以 exact source date 恢复
`2022-08-26`：availability=`2022-08-27`、6496 contracts、provider query=1、orders/fills=0/0。
该既有证据优先作为 lineage 输入；它不单独等于 V3 DQ/PIT PASS。

## 3. 单次 R1 执行范围

目标为现有 QuantConnect research clone `35444189`；原项目 `34808569` 不得修改。候选必须在
任何新结果可见前完成本地静态验证、exact hash 封存、普通 Git 发布和 manifest replay。

候选 action maxima：

- existing clone project mutation：最多 1；
- save：最多 1；candidate build：最多 1；
- zero-order Cloud backtest：最多 1；retry：0；new clone：0；
- exact-date provider history recovery query：最多 1，仅允许 target source date
  `2022-08-26`；cross-date fallback：0；
- original project mutations：0；raw option rows exported：0；contract identifiers exported：0；
- Object Store writes：0；public shares：0；orders：0；fills：0；portfolio invested：false。

环境在候选输入前出现的 automatic startup build 必须单独记账；若实际环境行为超出 manifest
预先固定的环境上限，候选不得输入并 fail closed。backtest dispatch 是本轮授权的消费点；在此之前
被外部环境阻塞时授权保持未消费。

## 4. DQ/PIT 执行与导出边界

候选必须从 frozen V3 exact numeric policy 机械派生四项阈值：quote age `120 seconds`、relative
spread `0.20`、prior-session open interest `10`、decision-as-of cumulative volume `1`。不允许
caller-supplied 或结果后阈值。

逐 observation 在 exclusion 之前验证 identity、PIT、timestamp、quote domain 和 available-at；
只导出 derived check terminal、session terminal、window terminal、identity/checksum 和计数，不导出
raw row 或 contract identifier。必须绑定：

- provider、LEAN engine/tier、exchange calendar、symbol mapping、normalization、repository/candidate
  code SHA、source evidence 与 aggregate manifest；
- exact 1202 target session inventory 与独立 pre-window prior；
- 每 session sorted/unique expected contributor manifest 的 derived checksum 与 count；
- exact-source-date、quote/volume same-session、OI exact-prior-session 和 available-at 语义；
- deterministic replay report 与 artifact checksum catalog。

只有 exact `1202/1202` session PASS 才是 `GLOBAL_PASS`。identity/PIT/manifest/session-set 违规优先为
`GLOBAL_INVALID`；numeric FAIL 或 UNKNOWN 继续固定 inventory，但禁止后续策略结论 admission。

## 5. Locked zero-order backtest 测量

同一个预先锁定候选可计算 V4 八轴所需的 derived-only counterfactual series，但不调用交易 API，
不生成 broker order/fill。候选、baseline、comparator、growth state、defensive veto、cost model、
session joins、event/episode construction、random seed=`2542` 与 10,000 bootstrap resamples 必须在
manifest 中 exact 绑定。

八轴按 V4 原顺序机械聚合：

1. `NON_BETA_ACTION_VALUE`；
2. `NET_OF_COST_RETURN`；
3. `ACTUAL_PATH_DRAWDOWN_REGRESSION`；
4. `FALSE_RISK_OFF_COST`；
5. `CANONICAL_DQ_PIT`；
6. `SAMPLE_AND_WINDOW_DEPENDENCE`；
7. `ACTUAL_PATH_TURNOVER`；
8. `LEVERAGE_BETA_ATTRIBUTION`。

joint precedence 固定为 `INVALID > FAIL > INSUFFICIENT > PASS`；不允许七取八、加权补偿或结果后
救援。即使 `GLOBAL_PASS`，结论最高也只能是 `READY_FOR_OWNER_GROWTH_HYPOTHESIS_REVIEW_NOT_PROMOTION`，
不生成 official weights、投资建议或部署授权。

## 6. 分阶段实施

### S0：任务与授权合同

- canonical task registration 与本 requirement；
- 新建 versioned R1 execution manifest、run scope、typed validator 和 negative tests；
- 记录 exact code/data identity、action maxima、zero-order/zero-fill boundary 与退出条件。

### S1：locked candidate

- 从已验证的 2541 exact-date recovery candidate 继承 transport 与 same-date recovery；
- 增加 V3 derived-only DQ/PIT、run authority、artifact catalog 和 deterministic replay；
- 增加 V4 locked research measurement，不调用交易 API；
- 本地 fixture/replay 只用 synthetic rows，不读取 provider 或真实 cache。

### S2：predispatch publication

- 更新 `docs/system_flow.md` 与 canonical task state；
- focused、Ruff、strict mypy、Architecture/Contract/Integration/Reproducibility 与适用 Full 在同一
  candidate 上 PASS；
- local `main` fast-forward、ordinary push、remote SHA 等值后，自动 replay R1 manifest。

### S3：唯一外部 run 与复核

- 只读核验 clone、LEAN/tier 和 startup-build accounting；
- 在 action maxima 内输入 exact candidate、save、build、dispatch 一次 zero-order backtest；
- 封存实际 counters、export-safe terminal evidence、run/identity/checksum/replay artifacts；
- 本地 admission 先评估 V3 global terminal，再决定 V4 八轴/联合 terminal 是否可接纳；
- 更新 task、Atlas 与复核报告；不执行第二 run。

## 7. 验收与 stop conditions

- frozen V3/V4 file/canonical hashes保持不变；
- candidate 与 manifest 在结果前锁定并普通发布；
- requested/evaluated range 均显式为 `2021-02-22..2025-12-02`；
- action maxima、实际 counters、授权状态和技术状态分轴记录；
- any candidate hash、session inventory、prior、identity、manifest、PIT、timestamp、threshold、unit、
  event/episode 或 reconciliation drift 均 fail closed；
- DQ 非 `GLOBAL_PASS` 时 V4 策略 terminal 必须为不可接纳，不得解释收益；
- 无 retry、无 raw row、无订单/成交/持仓/生产/broker 行为；
- 外部平台不可用、登录/控制不可用、unexpected build 或 source identity 不可核验时，保留 evidence
  并以 typed blocker 停止，不实施 workaround；
- final report 分别披露 authorization state、technical validation state、DQ/PIT terminal、八轴 terminal、
  requested/evaluated range、production effect 与 broker action。

## 8. 生命周期

- repository branch：`codex/trading-2542e-real-review`；
- repository workspace：复用 `D:\Work\AITradingSystem`，不创建额外 worktree/clone/cache；
- external sandbox：仅现有 QuantConnect clone `35444189`；原项目 `34808569` 只读且不修改；
- 当前阻塞态退出条件：predispatch policy gap、未消费 authorization 与全零 external counters 已进入
  validated local/remote main；task branch 无独有内容后删除；publication lease 释放。只有在 Owner
  先 review 新的 executable policy pack 后，后续状态转换才可改为生成 manifest 并消费唯一 run；
- recovery：tracked bytes 由 Git/main 恢复，外部执行只由 immutable manifest 与 export-safe evidence
  重放，不依赖 raw provider row。

## 9. 进度记录

- 2026-08-25：Owner 授权继续真实 DQ 与 locked backtest 复核。只读审计确认 2542D 冻结任务不能
  原地提升 executable flags；本任务作为独立 R1 successor 登记。尚未连接 provider/QuantConnect，
  尚未读取真实 cache，external counters 全为 0。
- 2026-08-25：predispatch authority inventory 发现 authorization 与 executable policy 仍是两个独立
  维度。`qqq_options_deterministic_selection_v1` 仍为 `selection_authorized=false`，DTE、moneyness、
  delta 与 rank policy 全部是 `UNKNOWN_REQUIRES_POLICY_REVIEW`；`qqq_options_signal_export_v1` 仍为
  `etf_signal_mapping_allowed=false`；preregistration 只定义“bounded QQQ/SGOV reallocation”，没有
  冻结 growth-state formula、trigger、目标 QQQ/SGOV 权重或 action sizing。因此无法构造 V3 要求的
  per-session expected contributor manifest，也无法形成结果前锁定的 candidate return series。
- 2026-08-25：QuantConnect 官方 Equity Options handling-data 文档说明 `OptionContract.open_interest`
  是每日计算一次的 latest value，并另提供 `History<OpenInterest>` 获取历史 OI，但没有证明当前 daily
  chain 属性本身携带 V3 所要求的 exact-prior-session source/available-at identity；同页示例可读取
  bid/ask，但没有为当前 2541 daily-chain candidate 建立 `quote_end` lineage。官方参考：
  `https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/equity-options/handling-data`
  与
  `https://www.quantconnect.com/docs/v2/writing-algorithms/historical-data/asset-classes/equity-options`。
  在这些语义被 versioned adapter contract 明确前，不能把 event time 或 latest OI 静默当作 frozen
  V3 的 quote-end/exact-prior evidence。
- 2026-08-25：本次 external authorization 保持
  `EXACT_PREAUTHORIZED / UNCONSUMED_NO_BACKTEST_DISPATCH`；technical state 为
  `BLOCKED_PRE_DISPATCH_POLICY_INPUT`。clone mutation/save/build/backtest/provider query/order/fill
  counters 全为 0。解除阻塞必须在任何真实结果可见前由 Owner review 一个新 versioned policy pack，
  精确固定：(1) contributor eligibility 与 deterministic rank；(2) derived options growth-state 与
  effective-session mapping；(3) QQQ/SGOV action sizing/target weights；(4) quote-end 与 exact-prior OI
  adapter lineage。随后才能生成 exact candidate/R1 manifest 并消费唯一 run。
- 2026-08-25：publication transaction `trading-2542e-predispatch-20260825-v1` 在 generated
  authority focused validation 中发现 `tests/test_devx_006d_report_catalog_flow_authority.py` 的三项
  静态断言仍绑定旧 `system_flow` 哈希与条目数；该路径未在 v1 声明，因此 v1 以 path-scope
  incomplete 失败释放，没有越权修改。v2 扩展精确 owned path 后只同步生成权威断言，不改变研究、
  DQ、策略或 authorization consumption state。
