# TRADING-2517：QC QQQ Options Refresh Authorization Admission 与 Bounded Collection Lifecycle V1

最后更新：2026-08-13

稳定任务 ID：
`TRADING-2517_QC_QQQ_OPTIONS_REFRESH_AUTHORIZATION_ADMISSION_AND_BOUNDED_COLLECTION_LIFECYCLE_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

contract change：`true`

production effect：`none`

broker action：`none`

## 1. 问题与目标

TRADING-2516 已 ordinary-push fresh versioned Owner authorization request，并严格拒绝旧、倒签、过期、
tampered 或错误 scope 的 token。它只生成
`QCQQQOptionsAuthorizationRefreshOwnerDecisionCandidate`，固定
`authorization_consumed=false`、`external_action_performed=false`，不执行外部动作。

TRADING-2514 的历史 admission policy 则按设计冻结旧 2513/v1 token。2516 fresh v2 token 与 2514
expected token 不同，不能直接进入既有 authorization、external-action ledger、Results parser 与 DQ/PIT
lifecycle。绕过 token identity、改写 2514 历史 policy 或把 dry-run candidate 当作真实 Owner 授权都不允许。

本任务建立最小 versioned successor contract：

1. strict admission Project Owner 实际提供的 2516 v2 token bytes；
2. 把已验证的 2516 candidate 转换为继承 2514/2512 frozen semantics 的 successor collector authorization；
3. 冻结 single-use、action cap、chronology、consumption、manual Results JSON 与 DQ/PIT admission 状态机；
4. 在 Owner token、外部动作或 evidence 任一未发生时继续 fail closed；
5. 为另行授权后的唯一 bounded QuantConnect collection 提供可审计入口。

本任务的离线 contract implementation 不等于 Owner 已签署 token，不授权登录、项目修改、Cloud run 或
Results 下载。真实外部动作只能在 Project Owner 通过当前 Codex 对话提供 exact token 后，由单一 coordinator
按本任务门禁执行。

## 2. 冻结 authority

### 2.1 2516 refresh authority

- predecessor ordinary-pushed main：`0d1d614e01a040661050329cef48ac7ecab06bda`；
- decision token：
  `owner_decision:TRADING-2516:2026-08-13:authorize_single_zero_order_primary_window_derived_aggregate_collection_v2`；
- refresh policy file/canonical SHA-256：
  `4aa2983a6cb6c0ac02d03d18a807ea3bdf553770ac545130011911bf83caca77` /
  `acd849fd8189256d4908cc162eb0c9bfe4162c669760577f21d6c960919b4882`；
- refresh package manifest file/content SHA-256：
  `7373474ee0279f70dcc678f6325935c82e96b90e5e46da82613bb8fcb106d924` /
  `0978dceaefb1acec33e2da2681075128c880d19ce4b01a194a7b38961f943381`；
- unsigned request file SHA-256：
  `d351ed7c54eb0531a29bdd5d27e5e518a1870ef89aefd53041ad91fd6c45457e`。

### 2.2 2512–2514 collection/evidence authority

- proposal content SHA-256：
  `f48732afc0d69656fbe5c62b1965296feccda30caa3279c80b9d1c20ce272240`；
- run-scope content SHA-256：
  `80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e`；
- project code LF SHA-256 / bytes：
  `d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6` / `26074`；
- collector policy file/canonical SHA-256：
  `48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f` /
  `3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f`；
- transport map SHA-256：
  `60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5`；
- 2514 admission policy file/canonical SHA-256：
  `8e7103680884288574b5cc0c0813085e47396f244c4fce9db275477013760a91` /
  `a4e399ea022c04b579bbaaeb12bdc922e332ceb1badb0e4ba9740f17e11f824a`。

本任务继续继承 2481 shared records/envelope、2482 canonical DQ/PIT、2484 QC adapter、2499 DAILY
chronology、2500 DAILY capability、2509 v2 slot catalog、2510 evidence admission、2511 generator、2512
collector、2513 proposal、2514 evidence lifecycle 与 2515 `KEEP_CLOSED + PREREGISTRATION_ONLY`。不得复制、
重定义、弱化或用 successor token 改写这些历史 authority。

## 3. 冻结 run scope 与安全边界

- target project id：`34808569`；
- requested/evaluated range：`2021-02-22..2025-12-02`；
- primary role / calendar：`PRIMARY` / `XNYS`；
- exact session count：`1202`；
- maximum project mutations / cloud backtests：`1 / 1`；
- maximum orders / fills：`0 / 0`；
- result carrier：Project Owner manual `Download Results` JSON only；
- collector / independent reviewer：`codex_capability_coordinator` / `project_owner`；
- owner policy value count：`0`；
- engine：`POLICY_BLOCKED_CASH_PRESERVATION`；selection：`false`。

Allowed actions exact：

1. `QUANTCONNECT_LOGIN`；
2. `MODIFY_EXISTING_DEDICATED_PROJECT_ONCE`；
3. `RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST`；
4. `EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION`。

Prohibited actions exact：

- `API`、`CLI`、`HTTP`、`OBJECT_STORE`；
- `RAW_OPTIONS_DATA_DOWNLOAD`、`RAW_OPTION_ROW_LOGGING_OR_EXPORT`；
- `PURCHASE_OR_SUBSCRIPTION`、`SECOND_CLOUD_BACKTEST`；
- `INVESTMENT_INTERPRETATION`、`PAPER`、`LIVE`、`BROKER`、`PRODUCTION`。

## 4. Successor authorization 与状态机

### 4.1 Token admission

- 必须调用 2516 canonical validator；不得复制 parser 或信任调用者构造的 candidate；
- Owner token bytes 必须来自 Project Owner 当前对话，且 exact file/content SHA-256 写入 receipt；
- 本地生成或 dry-run 的相同文本不构成授权事实，不得写入真实 admission receipt；
- ordinary-pushed main、refresh policy/package、proposal/scope/project-code、project/range/session/cap/reviewer/
  expiry/single-use 任一 mismatch 都 fail closed；
- token review 只产生 `OWNER_AUTHORIZATION_ADMITTED_UNUSED`，不自动消费、不表示 external action 已发生。

### 4.2 External action ledger

每项动作必须记录 ordinal、action id/type、UTC time、project id、可用时的 project-code hash、backtest id、
status 与 typed failure/scope verdict。动作必须按时间和 ordinal 单调，且发生在 authorization admission 之后、
expiry 之前。

- login 最多记录一次 completed lifecycle observation；
- project mutation count `<=1`；只能把 existing dedicated project 变为 exact reviewed `main.py` bytes；
- Cloud backtest count `<=1`，orders/fills 必须为零；
- Results JSON 只能由 Owner 在 UI 手工下载一次并按原始 bytes checksum 封存；
- action 失败、第二次 mutation/run、project/code/range mismatch 或 forbidden action 立即产生 typed NO-GO。

### 4.3 Consumption

single-use 的含义是不得发起第二个 Cloud run。第一次 Cloud backtest 被实际提交后，即使平台失败、Results
缺失或 DQ/PIT 不通过，也不能重用该 token 再跑；后续只能以新的 reviewed proposal/token 启动新 lifecycle。

consumption receipt 必须绑定 authorization、ledger、project/backtest id 与 first-run fact。Results evidence 成功收集后
同时记录 `authorization_invalidated_after_evidence_collection=true`。任何 reuse、duplicate consumption、ledger
rollback 或 token/file hash drift 均 fail closed。

### 4.4 Evidence 与 DQ/PIT

- 只复用 2512 strict Download Results parser；不得复制 transport parser；
- exact 10-series、1202 sessions、timestamp/ordinal、finite/domain/max-min/count、project/backtest/range/code/policy/
  proposal identity、orders/fills/fees/portfolio/raw/log/Object Store facts必须全部重验；
- parser PASS 只产生 `RESULT_PARSED_DQ_NOT_EVALUATED`；
- 必须调用 2482 canonical 15-check DQ/PIT path，从真实 report facts 派生 status；调用者自报 PASS、arbitrary
  report bytes、UNKNOWN、NOT_EVALUATED、FAIL、scope/as-of/checksum mismatch 均停止；
- evidence admission 即使 PASS，也只允许进入 2511/2510 的 per-slot evidence review，不提供 G2 policy values，
  不激活 selection、engine 或投资解释。

## 5. 实现计划

### S0：registration boundary

- canonical task row + 本 supporting requirement；
- task shadow/DevEx/current authority validate；
- ordinary non-force push registration boundary；
- 从包含 registration 的 exact latest main 重新运行 governed START/LANE preflight。

### S1：versioned successor contract

- task-owned policy、strict loader、typed authorization admission/consumption/lifecycle records；
- 2516 candidate → 2512 collector authorization 的 deterministic adapter；
- canonical seal/from-json/replay 与 file/content identity；
- 缺 token 时 typed `OWNER_REFRESH_TOKEN_NOT_PROVIDED`。

### S2：ledger/evidence wiring

- 复用 2514 action/receipt models 或建立只扩展 token authority 的 versioned successor；
- 不改写 2514 frozen policy/token/history；
- 复用 2512 Results parser、2482 DQ/PIT 与 2510/2511 handoff；
- 提供真实 lifecycle 执行前的 offline pre-admission/plan builder。

### S3：fail-closed coverage

- unit/property/golden：canonical replay、input key permutation、exact 2516 token admission；
- old 2513 token、local dry-run token、missing/duplicate/expired/reused token、main/hash/scope/project/session/cap/
  reviewer/action mismatch；
- second run/mutation、chronology rollback、failed run、missing Results、order/fill/raw/log/Object Store、DQ
  semantic FAIL/UNKNOWN/NOT_EVALUATED 与 scope/as-of/checksum mismatch；
- default unauthorized 与任一 failure 都产生 cash-preservation/no-order/no-fill result。

### S4：shared wiring 与 closeout

- `docs/system_flow.md`、architecture fragments、canonical task registry/generated/compatibility authority；
- Atlas 只披露“successor admission contract 已实现，Owner token/run/evidence/DQ/PIT 尚未发生”；
- focused/adjacent/compatibility 后在 final tree 串行运行 Architecture → Contract → Integration →
  Reproducibility → exclusive Full；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 6. 验收标准

1. 2516 v2 token 只能经 canonical validator admission；2514 v1 history exact bytes 不变。
2. 没有 Project Owner 实际提供的 exact token 时，authorization/run/evidence/DQ/PIT 全部 fail closed。
3. token validation 不消费授权；第一次 Cloud run fact 阻止任何第二次 run，无论 run/evidence/DQ 结果如何。
4. exact project/range/1202-session/code/policy/proposal/cap/reviewer/action chronology 全部 cross-bind。
5. Results JSON 只经 2512 strict parser；DQ/PIT 只经 2482 canonical 15-check path。
6. UNKNOWN、NOT_EVALUATED、FAIL、tamper、scope/as-of/checksum mismatch、order/fill/raw/prohibited action 均不能
   产生 evidence admission PASS。
7. 不新增或猜测 DTE/moneyness/delta/spread/OI/volume/freshness/fee/slippage/latency/partial-fill 等投资阈值。
8. `owner_policy_value_count=0`、`selection_authorized=false`、engine cash-preservation、orders/fills=0；无投资解释。
9. 当前离线实现期间 external action、production effect、broker action 均为 `none`。
10. focused、generated/compatibility、Atlas 与 final five-tier gates PASS；ordinary push/cleanup 完成。

## 7. Path claims

Task-owned：

- `docs/requirements/TRADING-2517_QC_QQQ_Options_Refresh_Authorization_Admission_and_Bounded_Collection_Lifecycle_V1.md`；
- `config/research/qc_qqq_options_refresh_authorization_admission_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/refresh_authorization_admission.py`；
- `tests/test_qqq_options_refresh_authorization_admission.py`；
- task-owned offline package/fixtures（实现时精确声明；不得包含真实 Owner token 或 Results evidence）。

Coordinator-owned shared paths：

- canonical task registry/index 与 generated task shadows；
- `docs/system_flow.md`；
- Atlas page-effectiveness task coverage/disclosure；
- architecture fragments、DevEx seal、compatibility/current-authority sources。

## 8. 当前状态与 blocker

- 2026-08-13：READ_ONLY audit 确认 `TRADING-2517` 未占用；registration base=
  `0d1d614e01a040661050329cef48ac7ecab06bda`，main/origin一致、checkout clean、lease=0；
- 2026-08-13：2516 exact token template 通过 in-memory canonical parser dry-validation；该 dry-run 未写盘、未消费、
  未产生 Owner 授权事实，`external_action_performed=false`；
- 2026-08-13：确认 2516 v2 decision token 与冻结 2514 v1 expected token 不同，必须通过本 successor contract
  显式衔接，不得修改历史 token 或绕过 admission。

当前 blocker：`OWNER_REFRESH_TOKEN_NOT_PROVIDED`。该 blocker 不妨碍离线 successor contract 实现，但在 Project
Owner 提供 exact token 前，禁止 QuantConnect login/project mutation/Cloud run/Results collection；真实 evidence、
DQ/PIT 与 policy review 均保持未发生。
