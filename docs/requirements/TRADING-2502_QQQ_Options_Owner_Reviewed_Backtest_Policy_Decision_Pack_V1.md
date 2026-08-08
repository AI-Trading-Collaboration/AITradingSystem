# TRADING-2502：QQQ Options Owner-Reviewed Backtest Policy Decision Pack V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2502_QQQ_OPTIONS_OWNER_REVIEWED_BACKTEST_POLICY_DECISION_PACK_V1`

优先级：`P0`

状态：`READY`

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
