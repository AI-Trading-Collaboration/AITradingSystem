# TRADING-2487：QQQ Options Cash / Premium / Settlement Accounting V1

任务 ID：`TRADING-2487_QQQ_OPTIONS_CASH_PREMIUM_SETTLEMENT_ACCOUNTING_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：
`owner_decision:TRADING-2487:2026-08-02:build_offline_accounting_mechanics_without_unreviewed_numeric_policy`

production effect：`none`

broker action：`none`

## 1. 目标与非目标

本任务在 TRADING-2481 shared contract 与 TRADING-2486 sealed minute execution result 上实现纯离线、
Decimal-only、可 canonical replay 的 QQQ long-premium cash accounting。它从一个 canonical
`RunManifestRecord` 的 initial cash 开始，按确定顺序重放 intent、order、fill、reservation、fee、premium、
settlement 与 liquidation-mark facts，生成 shared `PortfolioSnapshotRecord`、task-local ledger/position 明细与
可重算 identity。

V1 必须覆盖：

- actual contract multiplier，而不是把 `100` 当作 runtime 常量；
- BUY_TO_OPEN premium 与 fee 的 settled-cash debit；
- SELL_TO_CLOSE proceeds、fee、unsettled cash 与 reviewed settlement-session release；
- intent reservation、price-improvement surplus、reject/cancel/unfilled remainder release；
- partial fill 的逐笔 reservation/cash/quantity/cost-basis 变化；
- long-only position quantity、cost basis、realized/unrealized PnL 与 bid-side liquidation valuation；
- insufficient cash、negative settled cash、short option、short QQQ、margin、missing/stale/crossed valuation、
  duplicate/conflicting execution identity 的 typed fail-closed result；
- platform/local recomputation 所需的每条 ledger entry、source checksum、policy lineage 与 canonical hash。

当前没有 Owner-reviewed initial cash、premium budget、max contracts、fee buffer、settlement lag、cost-basis
method、fee-in-cost-basis treatment 或 valuation freshness 数值/规则。因此 tracked default 只能返回
`ACCOUNTING_POLICY_REVIEW_REQUIRED`、cash preservation、无 ledger mutation、无 portfolio conclusion。
实现 mechanics 不等于激活投资 facing accounting baseline。

本任务不做：

- 不修改/复制 2481 envelope、`RunManifestRecord`、`OrderIntentRecord`、`OrderEventRecord`、
  `FillEventRecord` 或 `PortfolioSnapshotRecord`；
- 不修改 2482 DQ/PIT、2484 adapter、2485 selector 或 2486 execution semantics，不激活任何 blocked policy；
- 不处理 expiry/exercise/assignment/corporate action 或 position lifecycle state machine；这些属于 2488；
- 不登录 QuantConnect，不调用 API/CLI/HTTP，不创建 cloud project，不下载或导出 raw options data；
- 不运行 cloud backtest、paper/live/broker/production，不允许 margin、short option、QQQ shares 或负现金；
- 不计算策略级收益结论、promotion gate 或参数优化。

## 2. Exact authority 与继承边界

- frozen base / local main / origin main：
  `3e21bb33f56763f3fbea4539abddd5674817b5ee`；
- TRADING-2481 contract schema SHA-256：
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- TRADING-2481 shared policy SHA-256：
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- TRADING-2486 execution policy SHA-256：
  `8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a`；
- TRADING-2486 module LF SHA-256：
  `e2f50a6fef80295d79d28fba1f40b7943ec2a2d14e0d2375084c6c87c4e13feb`；
- primary requested/evaluated start=`2021-02-22`；`2022-12-01` 不是新 run 默认；
- external Owner token=`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`。

2487 只消费 shared records 的 `.seal()` / `.from_json_bytes()` / `.canonical_bytes` / content hash，以及
2486 `QQQOptionExecutionResult.from_json_bytes()` 的 canonical authority。caller 不能用普通 dict、自报 hash、
重构 record 或绕过 replay。任何 shared schema、execution chronology 或 public predecessor semantics 变化，
必须另开 reviewed serial contract wave。

## 3. Governed policy 与授权

tracked policy schema 固定为
`qqq_options_cash_premium_settlement_accounting_policy.v1`，默认必须满足：

- `status=OWNER_REVIEW_REQUIRED_BASELINE`；
- `accounting_authorized=false`；
- `approved_initial_cash_usd`、`premium_budget_usd`、`max_contracts_per_order`、
  `fee_buffer_per_contract_usd`、`sell_proceeds_settlement_lag_sessions`、
  `max_valuation_quote_age_ms`、`cost_basis_method`、`include_fees_in_cost_basis` 全部为
  `UNKNOWN_REQUIRES_POLICY_REVIEW`；
- predecessor contract/execution hashes、primary window、cash-account、long-premium 与 research-only boundary
  exact-bound；
- margin、negative settled cash、short option、short QQQ、same-bar/daily-close valuation、raw export、cloud、
  external order、production 全部禁止。

只有 `OWNER_REVIEWED_ACTIVE` 且 `accounting_authorized=true` 的 tracked policy 才能形成现实 accounting
baseline。测试可使用 `SYNTHETIC_TEST_ONLY` criteria 验证纯算术，但输出必须标记
`investment_interpretation_allowed=false`、`reality_baseline=false`，不能写入 tracked default 或报告为项目参数。

## 4. Canonical inputs 与 replay admission

`QQQOptionCashAccountingRequest` 至少绑定：

- canonical `RunManifestRecord` 与其 content/file hash；
- 一个或多个 canonical 2486 execution result bytes/file hashes；
- snapshot/session as-of、reviewed exchange-session calendar 与 calendar source checksum；
- 每个 open option SID 的 valuation bid/ask、quote end UTC、source id/checksum；
- producer version、lineage id 与 policy/contract/execution hashes。

admission 必须重验：

- run id、repository SHA、policy/contract、requested/evaluated range、timezone、安全边界跨所有 record 一致；
- 2486 result 的 policy hash、selection hash、quote-set hash、intent/order/fill canonical replay 与 event sequence；
- execution result 的 `accounting_status=NOT_EVALUATED`，2487 不接受 caller 预先宣称 accounting PASS；
- execution blocked/no-order/no-fill result 只能产生 typed no-mutation cash-preservation outcome；
- execution results 按 `(intent.created_at_utc, intent_id, result.content_sha256)` canonical-sort，输入排列不得改变输出；
- duplicate result hash、duplicate intent/order/fill identity、同一 identity 不同 bytes、跨 run/SID/side lineage 漂移
  一律 fail closed。

## 5. Cash、reservation 与 settlement mechanics

### 5.1 Reservation

- reservation 不改变 total settled cash，只减少可用 settled cash；
- BUY_TO_OPEN exact required reservation =
  `limit_price_per_share * contract_multiplier * contracts + fee_buffer_per_contract_usd * contracts`；
- shared intent `reserved_cash_usd` 必须与 required reservation exact 相等，且不超过 reviewed premium budget、
  available settled cash 与 max-contract rule；
- SELL_TO_CLOSE 不得预留 premium cash，reserved cash 必须为零；
- reject/cancel/no-fill 释放全部 reservation；partial fill 逐笔释放该 fill 对应的 limit-price reservation，实际
  premium/fee 从 settled cash 扣除；terminal 时释放所有未使用 remainder 与 price-improvement surplus。

### 5.2 Fill cash identity

每条 canonical fill 必须满足：

- `gross = fill_price_per_share * contract_multiplier * filled_contracts`；
- BUY_TO_OPEN：`settled_delta = -(gross + fee)`；
- SELL_TO_CLOSE：`unsettled_delta = gross - fee`，在 reviewed settlement due session 前不得进入 settled cash；
- actual fee 只能来自 sealed fill，且不得超过 active policy 的 reservation fee buffer；
- 任一步不得使 settled cash、unsettled cash、reserved cash 或 open quantity 为负；
- SELL_TO_CLOSE quantity 不得超过同 SID 已开 long contracts，不允许 implicit short 或 QQQ share creation。

### 5.3 Settlement calendar

settlement lag 不硬编码。active policy 的 exact lag 与 reviewed exchange-session calendar 一起确定 due session；
周末/假日不得按自然日平滑。`as_of_session >= due_session` 时，net proceeds 从 unsettled 转入 settled；否则保留
unsettled。calendar 缺失、乱序、重复、未覆盖 fill/due/as-of session 或 checksum drift 一律 fail closed。

## 6. Position cost basis、PnL 与 liquidation valuation

- V1 engine 支持 policy-declared cost-basis method；tracked default不选择方法；
- active synthetic/reviewed V1 首先实现 `FIFO`，每个 BUY fill 形成 exact lot，SELL fill 按 FIFO 消耗；
- `include_fees_in_cost_basis` 必须由 policy 显式决定，不能由代码静默选择；
- realized PnL、remaining cost basis 与 fees paid 必须逐 fill 可重算；
- open long option 只用 contemporaneous BID 做 liquidation market value；不使用 ask/mid/last/daily close；
- valuation quote 必须非缺失、非 stale、非 crossed、非 future，且 source/hash 唯一；
- `option_market_value_usd = bid_per_share * actual_multiplier * open_contracts`；
- unrealized PnL = liquidation value - remaining governed cost basis；
- missing/invalid valuation 时不得把 market value 伪装为零，也不得生成投资 facing snapshot。

shared `PortfolioSnapshotRecord` 是 canonical portfolio output；task-local ledger/position records只保存其可重算
明细与 identity，不能成为另一份不一致的 cash truth。

## 7. Public API 规划

- `UnresolvedCashAccountingCriteria` / `ActiveCashAccountingCriteria`；
- `QQQOptionCashAccountingSafety`；
- `QQQOptionCashAccountingPolicy` / `QQQOptionCashAccountingPolicyLoadResult`；
- `QQQOptionValuationQuoteInput`；
- `QQQOptionCashAccountingRequest`；
- `QQQOptionCashLedgerEntry`；
- `QQQOptionAccountingLot` / `QQQOptionAccountingPosition`；
- `QQQOptionCashAccountingResult`；
- `QQQOptionCashAccountingContractError`；
- `load_qqq_options_cash_accounting_policy()`；
- `build_qqq_option_cash_accounting_input_sha256()`；
- `replay_qqq_option_cash_accounting()`。

task-local models 使用 strict validation、canonical JSON、computed SHA-256 与 exact-byte replay。所有 monetary
field 使用 finite Decimal canonical strings；禁止 float、NaN/Infinity、caller-supplied content hash。

## 8. Typed outcome 与安全状态

至少冻结：

- `ACCOUNTING_POLICY_REVIEW_REQUIRED`；
- `EXECUTION_BLOCKED_CASH_PRESERVED`；
- `ACCOUNTING_REPLAY_READY`；
- `INSUFFICIENT_SETTLED_CASH`；
- `PREMIUM_BUDGET_EXCEEDED`；
- `RESERVATION_MISMATCH`；
- `FEE_BUFFER_EXCEEDED`；
- `NEGATIVE_CASH_PROHIBITED`；
- `SHORT_OPTION_PROHIBITED`；
- `SETTLEMENT_CALENDAR_INVALID`；
- `VALUATION_QUOTE_REQUIRED`；
- `EXECUTION_IDENTITY_INVALID`。

default unauthorized 与 blocked execution 两条路径都必须 cash preservation、无 ledger mutation、无 position、
无 shared snapshot。任何 FAIL 不能被 platform result、later settlement 或 valuation PASS 覆盖。

## 9. 测试与验收

|阶段|工作|验收|
|---|---|---|
|S0|Requirement、task row、claims、START/LANE|exact base、contract-change、lease、paths PASS|
|S1|Policy/loader/default blocked|extra/hash/status/safety/全部 unresolved 字段 negatives PASS|
|S2|Canonical execution admission|forged/noncanonical/hash/run/policy/event/fill/duplicate/permutation tests PASS|
|S3|Reservation/cash/partial/reject/cancel|Decimal identities、release、price improvement、fee buffer、no negative cash PASS|
|S4|Settlement/position/valuation|reviewed-session T+1 fixture、FIFO、no short、bid liquidation、missing/stale/crossed PASS|
|S5|Golden/reconciliation identity|platform/local recomputation、input permutation、canonical replay/tamper PASS|
|S6|System flow/generated/formal/integration|focused、compatibility、five-tier final tree、ordinary push/cleanup PASS|

至少覆盖：default unauthorized、execution blocked、one BUY full fill、partial then fill、partial then cancel、reject、
price improvement、insufficient cash、premium budget、max contracts、fee buffer、actual multiplier、one/multiple SELL、
FIFO lots、sell-before-buy、T+1/weekend calendar、pre/post settlement、missing/stale/crossed/future valuation、duplicate
result/fill、input permutation、shared snapshot replay、golden identity。

工程退出：`QQQ_OPTIONS_CASH_ACCOUNTING_V1_READY_POLICY_BLOCKED`。

完整退出：`QQQ_OPTIONS_CASH_ACCOUNTING_V1_READY`，只有 Owner-reviewed active policy 与独立 platform/local
reconciliation evidence 后才能声称；没有数值 authority 时任务以 `BASELINE_DONE` 收口并保留 blocker。

## 10. Governed execution、claims 与生命周期

- mode：`SINGLE_LANE`；`contract_change=true`；
- frozen base：`3e21bb33f56763f3fbea4539abddd5674817b5ee`；
- branch：`codex/trading-2487-qqq-options-cash-accounting`；
- 复用 clean checkout：`D:\Work\AITradingSystem_ops073_integration`；不新建 worktree；
- task-owned：本 requirement、accounting policy、cash accounting module、focused tests、module/flow fragments；
- coordinator-owned：task register、system flow、ARCH-004 compatibility/current authority、ARCH-004G inventory/
  tests、ARCH-005 registry/index、DevEx/generated state 与 2487 task shadow；Full failure-fix 另精确接管
  `tests/atlas/test_historical_projection_review.py` 的 current-checkout artifact locator，不改 2479 policy、
  historical payload 或 Atlas production code；
- known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、复制、stage 或修改；
- external QuantConnect/platform action：`none`；
- exit condition：final evidence 进入 canonical runtime location，ordinary push/remote SHA verify 完成后删除 task
  branch；复用 checkout 返回 clean main；Git main/SHA/reflog 是恢复边界。

## 11. 进度记录

- 2026-08-02：TRADING-2486 exact task commit/local main/origin main=
  `3e21bb33f56763f3fbea4539abddd5674817b5ee`；final-tree five tiers PASS，task branch 已删除，runner=0，
  external platform action=none。
- 2026-08-02：Owner 要求继续推进后续任务；2487 作为 critical-path 单线任务开始登记。初步审计确认
  predecessor shared/execution authority 可直接消费，不需要改写 2481/2486；所有影响投资解释的 accounting
  数值和方法继续 unresolved/policy-blocked。
- 2026-08-02：START/LANE preflight 均从 exact base
  `3e21bb33f56763f3fbea4539abddd5674817b5ee` PASS，无 lease、blocker、warning 或 serial requirement；
  worktree 仅含声明的 2487 task/coordinator paths，known-unrelated exclusion 未读取或修改。
- 2026-08-02：focused 首轮完全并行命令
  `python -m pytest -n 16 --dist loadfile tests/test_qqq_options_cash_accounting.py` 为
  `4 passed / 31 failed in 5.25s`。失败为 shared fixture authority 的两项级联：错误使用不存在的
  `ACCEPTED` lifecycle enum，以及把 engine identity status 写成非 shared enum；未作为 formal evidence。
  只把 fixture 改为 `CREATED -> SUBMITTED -> terminal` 与 `UNKNOWN` 后原覆盖为
  `33 passed / 2 failed in 4.20s`；余下两项分别是 duplicate-record negative fixture 将 nested sealed records
  错转为 dict，以及 golden placeholder。保持生产语义不变修复 fixture/hash 后同覆盖 `35 passed in 7.98s`。
- 2026-08-02：进一步冻结 2486 exact created/submitted/terminal chronology、mixed no-fill no-mutation、reservation
  mismatch 与 settlement due-calendar coverage，最终同命令 `38 passed in 4.42s`；2480–2487 adjacent 完全并行
  组合 `208 passed in 9.92s`。Ruff、py_compile PASS；direct strict mypy 未报告 `cash_accounting.py` 本地错误，
  但命令仍因现存 imported/shared/Atlas 模块错误非零，未将其伪记为 PASS。
- 2026-08-02：tracked policy/module exact SHA-256 分别为
  `faa2659ee141cb2209686c3eadee31059ee660c3cc6d6dd3e63e259f23b1484e` /
  `562c4933f609d022035e444290548e1610d87ca015cfc2d2a56feefc08b2c0e8`。工程退出达到
  `QQQ_OPTIONS_CASH_ACCOUNTING_V1_READY_POLICY_BLOCKED`，状态写回 `BASELINE_DONE` 后再重建 generated/
  compatibility authority 并运行 final-tree formal gates；完整退出仍由 Owner-reviewed policy/evidence 阻塞。
- 2026-08-02：DevEx generate/validate=`1073 modules / 1240 tests / 856 direct writers / 0 violations`；
  task shadow generate/validate=`958 total / 453 active / 505 completed`，legacy/v2 byte-identical。compatibility/
  deprecation 首轮固定 `-n 16 --dist loadfile` 两文件覆盖为 `101 passed / 82 failed in 211.06s`：81 项由
  latest current-authority 仍止于 2486、未接管新 `e9...` shadow paths 的同一差集断言级联，另 1 项为
  ARCH-004G inventory 对新增 module/test 的预期 stale；没有 historical payload、prefix/hash 规则或产品行为
  失败。修复只追加 2487 authority、把 predecessor successor 引用提升到 2487、刷新 frozen inventory 与
  current test hashes；首轮保留为 failure-fix evidence，不作为 formal gate。
- 2026-08-02：修复后相同 compatibility/deprecation 覆盖最终 `184 passed in 119.42s`；formal final-tree
  Architecture/Contract/Integration/Reproducibility 分别 `831/276/995/24` PASS。首个独占 Full 为
  `8146 passed / 1 failed / 5 skipped / 643 warnings in 1201.02s`，唯一失败是
  `test_local_canonical_page_identity_when_available`：测试把 local canonical locator 固定解析到兄弟
  `D:\Work\AITradingSystem` checkout，因而读取了另一任务已更新的 ignored HTML（`114555 bytes / d29a3c...`），
  而不是当前 2487 checkout；冻结 2479 identity 仍为 `92180 bytes / b7540d...`。这是跨 checkout ignored
  artifact collision，不是 accounting/compatibility semantic failure。failure-fix 不移动、删除或改写该 ignored
  artifact；测试改为从 2479 policy 的 repository-relative path 在当前 checkout root 解析，保留“存在则严格
  校验、不存在则 skip”语义，并在刷新 compatibility source hash 后用 parent Full artifact 完整重跑五级门禁。
