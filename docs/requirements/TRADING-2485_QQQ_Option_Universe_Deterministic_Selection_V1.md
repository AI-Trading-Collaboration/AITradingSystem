# TRADING-2485：QQQ Option Universe Deterministic Selection V1

任务 ID：`TRADING-2485_QQQ_OPTION_UNIVERSE_DETERMINISTIC_SELECTION_V1`

## 1. 目标与非目标

本任务在 TRADING-2481–2484 的冻结 authority 之上实现一个离线、typed、可 canonical replay 的
QQQ long-premium 单合约选择引擎。输入是已经绑定 2484 adapter descriptor 的 daily signal、候选合约事实
和 canonical 2482 `DQReportRecord`；输出只使用 2481 已冻结的
`ContractCandidateSnapshotRecord` 与 `SelectionDecisionRecord`，相同输入和同一 reviewed policy 必须产生
相同 candidate-set hash、stable rank 与 selected SID。

本任务同时冻结“未获 Owner 审核时必须停”的默认行为。当前 DTE、moneyness、delta、quote age、spread、
OI、volume 数值以及 rank component priority 均未获得 Owner-reviewed policy，默认配置不得选择合约，只能产生
typed `SELECTION_POLICY_REVIEW_REQUIRED` no-contract decision；该结果保持现金、不生成 order，也不把
option-event DQ/PIT 提升为 PASS。

本任务不做：

- 不登录 QuantConnect、不创建或修改 cloud project、不调用 API/CLI、不运行 backtest；
- 不下载、导出或提交 raw option chain、minute quote、OI、volume 或 Greeks；
- 不生成 order、不模拟 fill/slippage、不计算 premium/PnL/收益；
- 不 roll、不 multi-leg、不 short option、不做 assignment/exercise/corporate action；
- 不替 Project Owner 选择投资阈值、rank priority、position budget 或 acceptance threshold；
- 不修改或重定义 2481 shared envelope/enums、2482 DQ/PIT/cache identity、2483 signal package 或
  2484 adapter descriptor。

## 2. Exact authority 与继承边界

- frozen base / local main / origin main：
  `5241055a5f624d115d46ff1557fdcf532108f298`；
- TRADING-2481 shared contract SHA-256：
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- TRADING-2481 shared policy SHA-256：
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- TRADING-2482 DQ/PIT policy SHA-256：
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；
- TRADING-2483 signal export policy SHA-256：
  `cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9`；
- TRADING-2484 adapter policy SHA-256：
  `b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`；
- 2484 descriptor schema：`qc_qqq_options_project_adapter_descriptor.v1`；decision 必须为
  `QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN`；
- subscription 必须保持 `QQQ Equity MINUTE RAW + Option MINUTE + DAILY signal/MINUTE execution`；
- primary requested/evaluated start 必须保持 `2021-02-22`；`2022-12-01` 不是默认值；
- adapter input admission、engine evidence 与 option-event DQ/PIT 当前仍是 UNKNOWN / NOT_EVALUATED；
- external Owner token 继续为 `NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`。

任何 shared envelope、DQ/PIT、signal package、adapter descriptor 或 research-window breaking change 必须
另开最小 reviewed serial contract wave；2485 只消费这些 authority。

## 3. Policy 与授权状态

### 3.1 默认 policy

tracked 默认 policy 必须为 `OWNER_REVIEW_REQUIRED_BASELINE`，并满足：

- `selection_authorized=false`；
- min/target/max DTE、moneyness distance、absolute delta、maximum quote age、relative spread、minimum OI、
  minimum volume 与 rank components 全部为 `UNKNOWN_REQUIRES_POLICY_REVIEW`；
- 所有 predecessor SHA、primary window、long-only/single-leg/no-roll/no-short safety 都 exact-bound；
- loader 接受该 baseline 以生成 fail-closed no-contract decision，但 selector 不得执行 active filtering/ranking。

### 3.2 后续 Owner-reviewed active policy

同一 V1 schema 可以读取另一个显式 `OWNER_REVIEWED_ACTIVE` policy，但只有满足以下条件才允许选择：

- `selection_authorized=true`；owner、owner decision、rationale、intended effect、validation plan、
  review condition、expiry condition 均非空；
- DTE、moneyness、absolute delta、quote age、relative spread、OI、volume 全部为 canonical typed numeric 值，
  range/target 关系合法；
- rank components 是完整、唯一、显式顺序，最后稳定 tie-break 必须是 `option_sid`；
- policy hash 进入 candidate-set identity 与所有输出 envelope；
- 未经 reviewed tracked change 不得把 synthetic test fixture 当成项目 active policy。

本任务不会创建上述真实 active policy。测试中的数值只能位于临时 fixture，必须标识
`SYNTHETIC_TEST_ONLY`，不得作为投资或生产默认值。

## 4. Typed input、DQ/PIT 与 selection-stage 边界

### 4.1 Candidate input

每个候选必须显式携带：

- run id、option SID、CALL/PUT、expiry、strike、contract multiplier；
- selection session/snapshot、underlying price 与 absolute delta；`dte` 由 expiry/session 精确计算，
  `moneyness` 由 strike/underlying price 精确计算，不信任 caller 重复提供的派生值；
- prior-day model/Greeks as-of session、OI/volume value 与 as-of session；
- bid/ask、quote end UTC；
- canonical `DQReportRecord` bytes 及其 checksum；
- 2484 descriptor identity、provider/source identity 与 export classification。

float、naive/non-UTC datetime、negative/crossed/single-sided quote、future quote、wrong/right mismatch、
duplicate SID、bad hash、extra field、wrong run/window/code/policy/safety 均 fail closed。

### 4.2 DQ/PIT 不得伪升

2482 的 15-check report 覆盖 signal→selection→intent→submit→fill 生命周期。selection 阶段尚未发生
order/fill，因此 2485 不得伪造整份 report PASS。规则如下：

- canonical bytes 必须通过 `DQReportRecord.from_json_bytes()` 复验；
- report 的 run/range/code/policy/contract/source/safety 必须与 descriptor/candidate cross-bind；
- selection 前可判定的 chain、quote、quote freshness、prior-day model/OI、calendar/mapping、
  signal-selection chronology、cache/engine/source identity checks 只接受显式 PASS；
- order/fill chronology 等后继 checks 可以保持 `NOT_EVALUATED`，但输出 envelope 的全局
  option-event `dq_status/pit_status` 继续为 `NOT_EVALUATED`；
- 任一 selection-stage FAIL/NOT_EVALUATED/缺失均使候选不 eligible；不得将 UNKNOWN 降级为 warning；
- local cached-data DQ PASS 不能替代 option-event checks，adapter descriptor 也不能替代 2482 report。

## 5. Deterministic selection 与输出

1. `FLAT` signal 直接输出 `FLAT_SIGNAL_CASH`；
2. 默认 unresolved policy 输出 `SELECTION_POLICY_REVIEW_REQUIRED`；候选存在也不得选约；
3. active policy 先按 signal 方向限制 CALL/PUT，再逐项执行 structural、DQ/PIT、DTE、moneyness、
   delta、spread、OI、volume gates；
4. 所有 eligible candidates 使用 policy 声明的 ordered rank components 排序，最终以 exact
   `option_sid` 稳定 tie-break；
5. 没有 eligible candidate 时输出 `NO_ELIGIBLE_CONTRACT_CASH`，不得扩宽阈值或改变 right；
6. candidate-set SHA-256 必须绑定 sorted semantic inputs、descriptor hash、DQ report hashes、
   selection policy hash 与 selection timestamp；重复运行 byte-identical；
7. candidate snapshots 与 decision 只通过 2481 `.seal()` 生成，caller 不得提供 content hash；
8. no-contract decision 不生成 order intent，cash/accounting 由 2487 继续实现；2485 只声明
   `cash_preservation_required=true`。

## 6. Public API 规划

- `QQQOptionSelectionPolicy` / `QQQOptionSelectionPolicyLoadResult`；
- `QQQOptionSelectionCandidateInput`；
- `QQQOptionSelectionRequest`；
- `QQQOptionSelectionResult`；
- `QQQOptionSelectionContractError`；
- `load_qqq_option_selection_policy()`；
- `build_qqq_option_selection_candidate_set_sha256()`；
- `select_qqq_option_contract()`。

任务自有 models 使用 strict/frozen validation；不加入或修改 2481 record registry。输出继续使用
`ContractCandidateSnapshotRecord`、`SelectionDecisionRecord` 的 canonical seal/replay。

## 7. 阶段、依赖与验收

|阶段|工作|依赖|验收|
|---|---|---|---|
|S0|Requirement、task row、claims/preflight|2484 exact push|START/LANE PASS，contract-change 显式|
|S1|Policy schema/loader 与 unresolved default|2481–2484 authority|exact hash、extra/hash/status/range/safety negatives PASS|
|S2|Typed candidate/request/result 与 deterministic engine|S1|same inputs same SID/hash；FLAT/unresolved/no-eligible cash paths PASS|
|S3|DQ/PIT cross-binding、property/golden coverage|S2|invalid/stale/crossed/duplicate/wrong prior session/unknown checks fail closed|
|S4|Architecture/system flow/generated authority|S3|DevEx/task shadow/compat/deprecation freshness PASS|
|S5|Formal gates、commit、integration、ordinary push/cleanup|S4|final-tree five-tier PASS；Full 独占|

工程基线退出：`QQQ_SINGLE_LEG_SELECTOR_ENGINE_V1_READY_POLICY_BLOCKED`。

完整任务退出：`QQQ_SINGLE_LEG_SELECTION_V1_READY`，只有 tracked default policy 获得 Owner review、
active policy tests 与独立 evidence 后才能使用。若数值 authority 仍未提供，任务必须以 `BASELINE_DONE`
收口并保留 blocker，不能声称完整退出。

## 8. Stop conditions 与 owner-dependent boundary

出现以下任一情况立即停止或 fail closed：

- 要求自行选择/复制 DTE、moneyness、delta、quote age、spread、OI、volume 或 rank 数值；
- 试图用 test fixture、QuantConnect sample 或网页建议冒充 Owner-reviewed policy；
- 2481–2484 hashes、range、run id、code、lineage、DQ/PIT、safety 或 subscription 不一致；
- 需要 raw option export、cloud project/run、API/CLI、paper/live/broker/production；
- 需要改变 shared schema 或让 selection-stage 输出伪装成 complete DQ/PIT PASS；
- heavyweight Full 与其他任务竞争。

## 9. Governed execution 与临时工作区生命周期

- mode：`SINGLE_LANE`；`contract_change=true`；
- frozen base：`5241055a5f624d115d46ff1557fdcf532108f298`；
- branch：`codex/trading-2485-qqq-option-deterministic-selection`；
- 复用 clean checkout：`D:\Work\AITradingSystem_ops073_integration`；不新建 task worktree；
- task-owned：本 requirement、policy、selector module、focused tests、module/flow fragments；
- coordinator-owned：task register、system flow、predecessor current-authority sections、
  `inputs/architecture/**`、task shadow 与 compatibility/deprecation tests；
- exit condition：final evidence 进入 canonical runtime location、ordinary push/remote SHA 验证完成后，
  删除 task branch；复用 checkout 返回 clean main。Git main/SHA/reflog 是恢复边界。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、复制、stage
或修改；repo-wide inspection 只使用 governed worktree audit。

## 10. 进度记录

- 2026-08-02：TRADING-2484 完成 ordinary push/cleanup，exact main=
  `5241055a5f624d115d46ff1557fdcf532108f298`；final Full 资源释放，external QuantConnect action=none。
- 2026-08-02：READ_ONLY、SINGLE_LANE START/LANE preflight 均 PASS；无 drift、lease、blocker，
  `contract_change=true`。创建任务分支，状态转 `IN_PROGRESS`；尚未引入任何数值 policy。
- 2026-08-02：完成 offline selector engine、unresolved default policy、DQ/PIT cross-binding、
  canonical shared record 输出与 architecture fragments；focused=`30 passed`，2481–2485 adjacent=
  `132 passed`，2480–2485 adjacent=`138 passed`，Ruff/mypy/compileall、DevEx 与 task shadow
  generate/validate 均 PASS。tracked policy 仍不授权 selection；工程退出为
  `QQQ_SINGLE_LEG_SELECTOR_ENGINE_V1_READY_POLICY_BLOCKED`，任务按 `BASELINE_DONE` 收口，
  完整退出继续由 Owner-reviewed numeric/rank policy 阻塞。正式 final-tree gates 尚待 compatibility
  authority 重建后执行；外部 QuantConnect action=none。
