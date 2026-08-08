# TRADING-2502：QQQ Options Owner-Reviewed Backtest Policy Decision Pack V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2502_QQQ_OPTIONS_OWNER_REVIEWED_BACKTEST_POLICY_DECISION_PACK_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

计划模式：`SINGLE_LANE`

## 1. 背景与目标

TRADING-2500 已用 FREE account、五个 reviewed XNYS sessions、完整 DAILY option chain aggregate、
Greeks/IV 与正 open interest 证明 `GO_FOR_DAILY_ENGINEERING_ONLY`。TRADING-2499 随后完成 strictly
offline DAILY primary backtest contract，固定 primary research start=`2021-02-22`、canonical DQ/PIT、
no-lookahead chronology 与 `POLICY_BLOCKED_CASH_PRESERVATION`。

当前阻塞不再是 QuantConnect DAILY capability，而是会影响投资解释的 selection、execution、accounting、
lifecycle policy 尚未经过 Owner review。TRADING-2502 的唯一目标是生成一份可审阅、可逐项签署的 decision
pack，使 Owner 能明确选择“继续阻塞”“采用 Owner 提供并审阅的 policy”或“先补证据再校准”。本任务本身
不选择数值、不生成正式 policy、不实现或运行真实日级 backtest engine。

## 2. 权威与继承边界

实现阶段必须从当时 exact latest main 逐文件重放 TRADING-2481 至 TRADING-2499 的 canonical authority，
不得复制或重定义 shared record、DQ/PIT、signal package、adapter、selector、execution、accounting、lifecycle
或 DAILY contract。至少继承：

- TRADING-2481 shared contract schema SHA-256=
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- TRADING-2481 shared policy SHA-256=
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- TRADING-2482 DQ/PIT policy SHA-256=
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；
- TRADING-2484 adapter policy SHA-256=
  `b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`；
- TRADING-2499 DAILY contract policy SHA-256=
  `4a060600ef9d532e75449a09628a54b84c9b68eca41989e1e4ed18de54b3109a`。

其余 2483–2498 authority 必须从 tracked policy/requirement/compatibility sources 读取 exact current hash；
本文不凭聊天记录补造未列出的 hash。TRADING-2493 的 aggregate `NO_GO_KEEP_BLOCKED` 继续支配 broader
minute/license/export/full-history 路径；TRADING-2500 的 GO 只覆盖 DAILY engineering。

## 3. Decision pack 必须覆盖的待决项

Pack 必须逐项分组，说明该决定影响什么、需要什么证据、缺失时如何 fail closed，以及 Owner 可选择的治理
路径。不得填入未经 Owner review 的数值或“建议阈值”。

### 3.1 Selection

- `DTE` window；
- moneyness definition/range；
- delta source/model/range；
- maximum spread 或 spread ratio；
- minimum open interest；
- minimum volume；
- quote freshness 与 two-sided quote requirement。

### 3.2 Execution

- signal session 到 intent/submit 的 DAILY chronology；
- marketable-limit side/bounds；
- slippage model；
- latency sensitivity；
- partial-fill treatment；
- cancel/reject/no-fill policy；
- stale/missing/single-sided quote disposition。

### 3.3 Accounting

- fee schedule/source/as-of；
- initial cash；
- sizing rule 与 maximum contract quantity/exposure；
- buying-power/cash reservation；
- fill/fee/slippage identity 与 rounding；
- cash preservation、insufficient-cash 与 reconciliation policy。

### 3.4 Lifecycle and acceptance

- expiry、exercise、assignment 与 settlement treatment；
- close/hold/roll decision boundary；
- stale position/quote 与 terminal valuation；
- result inclusion/exclusion 与 incomplete-run treatment；
- sample/coverage、DQ/PIT、reproducibility 与 acceptance gate；
- investment-facing conclusion/promotability boundary。

## 4. 每项允许呈现的治理选项

Decision pack 可呈现下列非数值选项，但不得替 Owner 作选择：

1. `KEEP_UNRESOLVED_BLOCKED`：保留当前 cash-preservation，engine 不解锁；
2. `OWNER_SUPPLIED_REVIEWED_POLICY`：Owner 提供具体值、rationale、intended effect、evidence、version/status 与
   review/expiry condition，后继 serial contract wave 再实现；
3. `EVIDENCE_CALIBRATION_REQUIRED`：先定义独立、无投资结论的 calibration evidence task，证据充分后再回到
   Owner decision；
4. `SENSITIVITY_ONLY_NOT_REALITY_BASELINE`：只允许 isolation/sensitivity 使用，不得成为 reality baseline、
   acceptance gate 或投资结论；
5. `NOT_APPLICABLE_WITH_REVIEWED_RATIONALE`：仅当 Owner 明确说明为何不适用且不产生隐式默认时采用。

任何字段都不得以空值、代码默认、QuantConnect engine default、historical artifact 或测试 fixture 代替 Owner
决定。zero fee/slippage/latency 只能作为明确标注的 isolation sensitivity，不能成为 reality baseline。

## 5. 证据需求

Decision pack 至少要把以下证据需求映射到相关待决项：

- 2021-02-22 起 primary window 的 requested/evaluated sessions 与完整性；
- quote/Greeks/IV/OI/volume 可得率、缺失率、freshness 与分布，不导出 raw option rows；
- spread、DTE、moneyness、delta 与候选数量的 session-level derived aggregates；
- fee schedule、effective date、source/license 与历史适用性；
- slippage/latency/partial-fill/cancel 的 bounded sensitivities 与 no-fill frequency；
- expiry/exercise/assignment/settlement 的 event coverage 与 engine semantics；
- sizing/cash policy 在不同 premium/exposure 情景下的 cash-preservation 证明；
- DQ/PIT status、source checksum、repository/policy/contract lineage 与 deterministic replay；
- sample sufficiency、out-of-sample/holdout 边界及 acceptance uncertainty。

TRADING-2500 的五 session capability evidence 只证明 DAILY capability，不能单独校准上述投资阈值或证明完整
历史 coverage。若证据需要新的 QuantConnect/cloud run、下载、API/CLI/HTTP 或 raw export，必须另立任务并取得
新的 exact Owner authorization；本任务不得执行。

## 6. 默认安全状态与 engine 解锁条件

在所有 required decisions 获得 reviewed authority 前，系统必须保持：

- `policy_status=OWNER_REVIEW_REQUIRED`；
- `engine_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `selection_authorized=false`；
- `orders=0`、`fills=0`；
- investment interpretation/promotion/paper/live/broker/production=`false/none`。

真实 DAILY engine 只有在独立后继 serial contract wave 同时满足以下条件后才能讨论解锁：

1. required decision inventory 无遗漏，每项有 typed Owner decision；
2. exact Owner token 绑定 pack、authority set、policy content/hash 与 decision values；
3. versioned policy manifest 记录 owner、status、rationale、intended effect、evidence 与 review/expiry condition；
4. DQ/PIT、window、chronology、adapter/selector/execution/accounting/lifecycle lineage 全部 exact cross-bound；
5. unresolved/UNKNOWN/NOT_EVALUATED 不得转换成 PASS；
6. unit/property/golden/tamper、focused/compatibility/formal gates 在同一 final tree PASS；
7. engine 实现、外部 run 与投资结论分别取得其自身任务与授权，不能由本 pack 隐式授予。

## 7. 非目标与禁止事项

本任务明确不做：

- 不自行给出或推荐任何数值阈值；
- 不创建正式可执行 strategy policy；
- 不实现、运行或模拟真实 DAILY engine；
- 不激活 TRADING-2485 selection 或后续 execution/accounting/lifecycle；
- 不宣称任何 policy 已 Owner reviewed；
- 不执行 QuantConnect login、cloud project/backtest、API、CLI、HTTP、Object Store；
- 不下载/记录/导出 raw options rows；
- 不执行 paper/live/broker/production 或投资解释；
- 不登记、修改或占用 `TRADING-2503_ATLAS_QQQ_OPTIONS_SERIAL_PROJECTION_CONTRACT_RENDERER_CONSUMER_V1`。

## 8. 计划阶段与验收

1. R0 registration boundary：task row、本文、task shadow/generated freshness、focused registration validation、
   ordinary push；
2. R1 authority inventory：从 latest main 重放 2481–2499 exact policies/contracts/hashes；
3. R2 decision matrix：逐字段形成非数值选项、证据需求、风险与 fail-closed 结果；
4. R3 Owner review pack：生成 reader-safe pack 与 canonical validation，不写 Owner decision；
5. R4 Owner handoff：等待 project owner 提供 exact decision token；
6. R5 successor planning：只在 Owner decision 后另立 serial policy contract wave，再另立 engine task。

R0 完成不代表进入 R1–R5。R1 之后是否实施须由 coordinator 在 registration push 后重新决定并执行新的
governed START/LANE。本文状态在未收到 Owner decision 前不得写成 policy reviewed 或 engine unblocked。

## 9. Registration boundary

- exact base：`af3a185157951cddf4b439729bef9b06628192c6`；
- task branch：`codex/trading-2502-qqq-options-policy-decision-pack-registration`；
- contract change：`false`；
- 首次允许写入：本文与 `docs/task_register.md` 中的 2502 row；
- 后续 coordinator-only generated paths 只在 task registration 后的 SINGLE_LANE preflight PASS 后重建；
- external/production/broker effect：`none`。

2026-08-08：2501/OPS-075 已 ordinary push 并释放 Full 资源；2489 重复交接已明确撤回。2502 由本工程
coordinator 登记，2503 保留给 Atlas coordinator。

## 10. R1 exact authority inventory

本 pack 从 exact main `6f54ee742bc2f0d6633a6e4c33957388d358e0ac` 重放 2481–2499 supporting
requirements。哈希算法固定为：读取每个文件完整文本、把 CRLF/CR 归一为 LF、用无 BOM UTF-8 编码；按
task ID 升序生成 `task_id|relative_path|lf_sha256`，用单个 LF 连接且末尾不追加 LF。

- canonical source-set byte count：`3001`；
- authority-set SHA-256：`1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c`。

| Task | Requirement LF SHA-256 | 本 pack 中的继承角色 |
|---|---|---|
|TRADING-2481|`810346be4cfca2ce4f11af303bb6f0167caa04c48ceb7d2b032bb35c03f70d88`|shared records、envelope、enum、canonical seal/replay 与 safety boundary。|
|TRADING-2482|`26b7f6c19f631ee0af31a265a7793e5d43837838093f1b33b90a6f5c0a47b511`|DQ/PIT、cache/evidence identity 与 UNKNOWN fail-closed。|
|TRADING-2483|`4d48cfda1c753bb13de0ea80fa6bc3cb398dd13b6aa79521217b2ef30c5682b8`|DAILY signal package、run manifest、export-safe derived artifact boundary。|
|TRADING-2484|`1475f8a6b51c171888f8cf496af116a12a1bdbc1731e7453954a8618e7bd4983`|QC adapter contract 与 no-pretend-engine/no-cloud boundary。|
|TRADING-2485|`4415635e5598243f009ab662baa32aaeae14cad0d565db4e1e0b6e5b19821c6f`|deterministic selector mechanics；numeric/rank policy 继续 unresolved。|
|TRADING-2486|`a3264e0fe69bfed24f9ac8d7f5269ba770c1500fddcf308ebb218485f6b9d9e1`|independent-minute execution mechanics 与 reality-policy blocker。|
|TRADING-2487|`36feab593c5b2d4fa668ffd20c751d74f4582b66146b3ad4386ef0a5509c2a71`|cash/premium/fee/settlement accounting mechanics 与 unresolved amounts。|
|TRADING-2488|`dafbfdc5b9a04844c9ebe2499d0c1fc90f4c25be392be7a7c38c507e027a8793`|expiry/exercise/assignment/corporate-action fail-closed lifecycle。|
|TRADING-2489|`f4d7ec5558b170a7ff28379a7525147ae30d98122dec00cde7d182223afae2da`|strict manual evidence bundle；不得扩展为 raw export authority。|
|TRADING-2490|`c93653d8329f4aae5a781312edd81814f4f21b83f6df49e2603b1f4c2eca315a`|local ingest/reconciliation boundary；未获完整输入不得伪造 reconciliation PASS。|
|TRADING-2491|`18d11ab3a8c9ba062e3322b2078dd11f3b19fd7eb05203863a6e34b0de1434eb`|cross-layer exact authority replay 与 golden/tamper boundary。|
|TRADING-2492|`ca539cd05687e4acf5dd82b044a1668d6aef491d9ef4f06672a35a07b009b469`|bounded pilot facts及其 scope violation；不能支持阈值校准。|
|TRADING-2493|`467b8ecec45b6c94124bba595e89a2d7ae318f0a9ae74711878f8c8bdbabec56`|aggregate `NO_GO_KEEP_BLOCKED` 对 broader minute/license/export/range 路径继续支配。|
|TRADING-2494|`bf2ed7e46fa8bf88d24ac5303eed06c0786dcceaaac52c139304f008feb1cc96`|Atlas historical projection evidence only；不是 engine policy authority。|
|TRADING-2495|`e643b2646b14490c0dac8081160a0ad2088b5f6249184c8a1421773bd7504479`|Atlas typed reader-status explanation only；不提升 domain status。|
|TRADING-2496|`40e9e4f55a520d712bbbf6fba01d3a806a396a1b11aab027e9230ae17e8abbf5`|Atlas renderer consumer only；视觉/页面状态不得变成 policy PASS。|
|TRADING-2497|`083d3332decc299b9da82ebc5e454124732b46db94847ba47f56393240ff63c3`|license/export due diligence 与 free-cloud/download distinction。|
|TRADING-2498|`bcddcfb8463457b98b555ea92a04d67d96474429224445c7e74ac01db4e8df74`|第一次 DAILY capability gate 的受控边界；后续 retry 不能追溯改写该事实。|
|TRADING-2499|`48dd16539d647534dbc284cb7617aed79634677dae842433fa553493e8c14ebd`|offline DAILY primary contract 与本 pack 的直接 predecessor。|

真正约束 engine 的 current policy hashes 继续由 2499 exact policy 绑定：shared contract=
`c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`、shared policy=
`d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`、DQ/PIT=
`1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`、signal export=
`cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9`、adapter=
`b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`、selection=
`bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30`、execution=
`8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a`、accounting=
`faa2659ee141cb2209686c3eadee31059ee660c3cc6d6dd3e63e259f23b1484e`、lifecycle=
`1798b6696e0f31571f9242a4276a06530fb951d15f250a2ef6756ac547037582`、DAILY contract=
`4a060600ef9d532e75449a09628a54b84c9b68eca41989e1e4ed18de54b3109a`。

## 11. 已冻结机制与仍待 Owner 决定的边界

以下不是待选阈值，后继 policy 不得把它们作为“参数偏好”静默改写：primary start=`2021-02-22`；
QQQ/RAW/XNYS；prior completed session 产生 signal/model inputs；selection 严格早于 intent；submit 不早于
intent；fill 必须来自独立后续 session/bar；daily-close 与 same-bar fill 禁止；UNKNOWN/NOT_EVALUATED 不得
转成 PASS；只允许 long-premium/cash-preservation，short/margin/paper/live/broker/production 均禁止。

真正待决的是下面 28 个 policy slots。`当前状态` 全部为 `UNRESOLVED`，Owner 可以选 G1–G5，但不能用
空值、fixture、engine default 或历史页面代替决定。

### 11.1 Selection decision matrix

| Decision ID | 待决定内容 | 投资/工程影响 | 必需证据 | 缺失时结果 |
|---|---|---|---|---|
|SEL_DTE_WINDOW|min/target/max DTE 与边界包含规则|决定持有期、theta/expiry 风险和候选覆盖|primary-window session-level DTE 分布、候选数、expiry coverage|G1，selection=false|
|SEL_MONEYNESS_RANGE|moneyness 定义、基准价格与允许范围|改变方向性暴露、premium 与候选集|derived moneyness 分布、定义稳定性、缺失/异常比例|G1，selection=false|
|SEL_DELTA_SOURCE_RANGE|delta 模型/source/as-of 与范围|改变风险暴露和 contract identity|prior-session Greeks coverage、模型/source identity、误差/缺失率|G1，selection=false|
|SEL_SPREAD_LIMIT|absolute/relative spread 口径与上限|影响可交易性和隐含成本|two-sided quote coverage、derived spread 分布、stale/single-sided 频率|G1，selection=false|
|SEL_OPEN_INTEREST_FLOOR|prior-session OI 口径与下限|影响 liquidity proxy 与 survivor bias|OI freshness/completeness/distribution、mapping checks|G1，selection=false|
|SEL_VOLUME_FLOOR|当日/前日 volume 口径与下限|影响 liquidity proxy 与 lookahead 风险|PIT-safe volume availability/distribution、missing-rate|G1，selection=false|
|SEL_QUOTE_FRESHNESS|quote age、two-sided/integrity requirement|决定可用 quote 与 stale risk|timestamp lineage、age distribution、crossed/locked/single-sided counts|G1，selection=false|
|SEL_RANK_PRIORITY|多候选 rank components、tie-break 与稳定顺序|直接改变被选 contract|候选敏感性、tie frequency、permutation/golden replay|G1，selection=false|

### 11.2 Execution decision matrix

| Decision ID | 待决定内容 | 投资/工程影响 | 必需证据 | 缺失时结果 |
|---|---|---|---|---|
|EXE_MARKETABLE_LIMIT|buy/sell quote side、limit buffer 与边界|决定成交可达性和最坏价格|derived bid/ask/spread、limit-bound/no-fill sensitivity|G1，no order/no fill|
|EXE_SLIPPAGE|slippage source/model 与 scenario roles|改变每笔收益和策略排序|bounded non-zero sensitivities、spread/size 分层、no-fill 对照|G1；zero 仅 G4|
|EXE_LATENCY|submission/fill latency 与 scenario roles|影响 quote staleness和成交率|event-time evidence、bounded latency sensitivities、quote survival|G1；zero 仅 G4|
|EXE_PARTIAL_FILL|partial-fill fraction/sequence/carry policy|影响仓位、现金与重放 identity|partial replay cases、size/liquidity evidence、permutation/golden|G1，no fill|
|EXE_CANCEL_REJECT_NO_FILL|timeout、cancel/reject/no-fill disposition|影响交易频率与选择偏差|cancel/reject/no-fill frequencies 与 typed reason coverage|G1，cash preserved|
|EXE_QUOTE_DISPOSITION|stale/missing/single-sided/crossed quote 处理|决定是否跳过、取消或失败|quote-integrity DQ report 与 derived incident aggregates|G1，no order/no fill|

### 11.3 Accounting decision matrix

| Decision ID | 待决定内容 | 投资/工程影响 | 必需证据 | 缺失时结果 |
|---|---|---|---|---|
|ACC_FEE_SCHEDULE|fee source、effective date、per-contract components|改变净收益与策略容量|primary fee schedule、历史适用期、license/source citation|G1，accounting=false|
|ACC_INITIAL_CASH|initial cash 与 currency/as-of role|改变可买数量、路径依赖与比较基准|研究目标、scenario rationale、cash-preservation cases|G1，cash unchanged|
|ACC_SIZING_EXPOSURE|premium budget、max quantity/exposure 与 rounding|改变风险和结果尺度|premium/exposure derived distribution、stress/sensitivity evidence|G1，no quantity|
|ACC_CASH_RESERVATION|fee buffer、reservation/release、insufficient-cash policy|决定下单可行性和负现金风险|reservation/release golden、partial/cancel/reject cases|G1，no order|
|ACC_IDENTITY_ROUNDING|cash quantum、rounding、fee/slippage/fill identity|影响 deterministic ledger 与 reconciliation|currency/Decimal authority、cross-engine replay、rounding diffs|G1，accounting=false|
|ACC_SETTLEMENT_COST_BASIS|settlement lag、cost basis、fees-in-basis|改变 realized/unrealized P&L|market/legal settlement source、FIFO/alternative replay|G1，accounting=false|

### 11.4 Lifecycle and acceptance decision matrix

| Decision ID | 待决定内容 | 投资/工程影响 | 必需证据 | 缺失时结果 |
|---|---|---|---|---|
|LIFE_EXPIRY_EXIT_GUARD|pre-expiry exit guard 与 expiry source|决定尾部风险和强制退出|expiry calendar/source coverage、exit/no-quote cases|G1，lifecycle=false|
|LIFE_EXERCISE_ASSIGNMENT|exercise/assignment/settlement disposition|可能产生 underlying exposure|reviewed engine semantics、event coverage、cash/share stress|G1，不允许完成 lifecycle|
|LIFE_CLOSE_HOLD_ROLL|close/hold/roll scope 与决策边界|改变持有期、频率和风险|separate strategy rationale、coverage、roll/close sensitivities|G1；roll=false|
|LIFE_TERMINAL_VALUATION|stale quote、bid liquidation 与 terminal mark|改变期末资产和 drawdown|quote-age/availability、bid/alternative mark reconciliation|G1，不出投资结果|
|ACC_RESULT_INCLUSION|incomplete/failed/no-fill run 纳入与排除规则|影响样本选择和胜率|typed run-status inventory、exclusion impact report|G1，不出 aggregate conclusion|
|ACC_SAMPLE_COVERAGE|sample/coverage sufficiency 与 holdout boundary|影响统计可信度|primary-window coverage、holdout design、uncertainty analysis|G1，不接受结果|
|ACC_DQ_PIT_REPRO|DQ/PIT/reproducibility acceptance gate|决定结果是否可审计|canonical reports、hash lineage、deterministic replay|G1，不接受结果|
|ACC_INVESTMENT_PROMOTION|investment-facing conclusion/promotability boundary|决定研究是否可影响投资决策|独立 review、OOS evidence、风险/限制披露|G1，promotion=false|

## 12. G1–G5 Owner decision semantics

| Code | Owner 选择的含义 | 后继动作 |
|---|---|---|
|G1 `KEEP_UNRESOLVED_BLOCKED`|该 slot 保持 unresolved。|不建 policy value；cash-preservation 继续。|
|G2 `OWNER_SUPPLIED_REVIEWED_POLICY`|Owner 提供具体值/规则及完整治理元数据。|另立 serial contract wave；本 pack 不执行。|
|G3 `EVIDENCE_CALIBRATION_REQUIRED`|先补足 derived、export-safe calibration evidence。|另立 evidence task；需要外部动作时另取 exact authorization。|
|G4 `SENSITIVITY_ONLY_NOT_REALITY_BASELINE`|只允许明确标注的 isolation/sensitivity。|不得成为默认、reality baseline、acceptance 或投资结论。|
|G5 `NOT_APPLICABLE_WITH_REVIEWED_RATIONALE`|Owner 明确证明该 slot 不适用且不会形成隐式默认。|后继 contract 记录 rationale 与影响分析。|

为降低 Owner 负担，G1、G3 或 G4 可按 group 一次选择，并机械展开到该 group 全部 slots；G2 必须逐 slot
提供值/规则、rationale、intended effect、evidence、version/status、review/expiry condition，不能只给 group
许可；G5 只能逐 slot 使用。任何 group choice 与 slot override 冲突时 fail closed，不替 Owner 推断优先级。

## 13. Owner review form

本节是签署格式，不是授权。Owner 在 review 前应先核对最终 ordinary-pushed main、本文最终 LF SHA-256 与
authority-set SHA-256。最小可接受 token 结构如下；尖括号内容必须由 Owner 明确填写，不能由 coordinator
代填：

```text
owner_decision:TRADING-2502:<YYYY-MM-DD>:review_qqq_options_backtest_policy_decision_pack_v1
exact_main_sha:<FINAL_ORDINARY_PUSHED_MAIN_SHA>
pack_requirement_lf_sha256:<FINAL_PACK_LF_SHA256>
authority_set_sha256:1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c
selection_group:<G1|G3|G4|PER_SLOT>
execution_group:<G1|G3|G4|PER_SLOT>
accounting_group:<G1|G3|G4|PER_SLOT>
lifecycle_group:<G1|G3|G4|PER_SLOT>
acceptance_group:<G1|G3|G4|PER_SLOT>
slot_overrides:<NONE_OR_EXPLICIT_DECISION_ID_TO_G1_G2_G3_G4_G5_MAPPING>
owner_supplied_policy_values:<NONE_OR_EXPLICIT_VALUE_RULE_AND_GOVERNANCE_METADATA>
confirmed_no_engine_activation:true
confirmed_no_external_action:true
independent_reviewer:project_owner
```

若 Owner 只希望先推进最保守路线，可对五个 group 全部选择 G3；这只授权后续建立 calibration evidence
任务，不授权任何阈值、engine、cloud run 或投资解释。本文不推荐 G1–G5 中的任何一项，也不把“继续推进”
自然语言解释为选择。

## 14. R1–R4 进度与当前 blocker

- R0 registration boundary：`DONE`；
- R1 authority inventory：`DONE`，source-set exact replay 如第 10 节；
- R2 decision matrix：`DONE`，28 个 slots 均有影响、证据与 fail-closed 结果；
- R3 reader-safe Owner review pack：`DONE`，本文第 10–13 节即 canonical pack；
- R4 Owner handoff：`BLOCKED_OWNER_INPUT`；等待 exact hash-bound Owner token；
- R5 successor planning：`NOT_STARTED`；不得在 R4 前登记 policy contract wave 或 engine task。

2026-08-08：首次 canonical Architecture 为 `855 passed / 1 failed`，唯一失败是 TRADING-2503 EOF
compatibility section 仍保存变更前 `docs/task_register.md` hash。该结果保留为 failure-fix parent，不作为
promotion evidence；durable fix 只 append TRADING-2502 current-authority section，并保持 2503 historical
prefix exact-byte 不变，不降低 source/hash 验证。

本阶段只修改本文、task register 与其 generated task shadow。没有 CLI、critical config、cache/report schema、
backtest behavior、market interpretation 或 major module 变化，因此 `docs/system_flow.md` 不需要更新。2503 的
Atlas contract/renderer 已在 predecessor exact main 完成，本任务不修改其 policy、页面或 artifacts。

本阶段默认状态保持 `OWNER_REVIEW_REQUIRED` / `POLICY_BLOCKED_CASH_PRESERVATION` / selection=false /
orders=0 / fills=0；QuantConnect、cloud、API、CLI、HTTP、Object Store、raw export、paper、live、broker、
production 动作为 `none/false`。
