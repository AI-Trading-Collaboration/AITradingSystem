# TRADING-2486：QQQ Options Minute Execution Reality Model V1

任务 ID：`TRADING-2486_QQQ_OPTIONS_MINUTE_EXECUTION_REALITY_MODEL_V1`

## 1. 目标与非目标

本任务在 TRADING-2481/2482/2484/2485 的冻结 authority 上实现一个纯离线、typed、可 canonical
replay 的 minute execution reality model。它消费已获准的 canonical selection decision、下一独立分钟
quote/event facts 与 canonical DQ/PIT evidence，模拟 quote-side marketable-limit 的 submit、partial fill、
reject、cancel 与 no-fill 路径。相同 semantic inputs、policy 与 lineage 必须产生 byte-identical 的 intent、
order events、fill events、result identity 与 replay。

当前没有 Owner-reviewed latency、slippage、fee、quote freshness、marketable-limit buffer、partial-fill
capacity 或 cancel/no-fill timeout 数值 policy。因此 tracked default 只允许输出 typed
`EXECUTION_POLICY_REVIEW_REQUIRED` cash-preservation/no-order/no-fill 结果；不能声称 reality baseline 已激活。
zero slippage 只允许出现在显式 `ISOLATION_SENSITIVITY` 测试角色，且 `reality_baseline=false`。

本任务不做：

- 不登录 QuantConnect、不创建或修改 cloud project、不调用 API/CLI/HTTP、不下载或导出 raw option data；
- 不运行 cloud backtest、paper/live/broker/production，不产生任何外部 order；
- 不修改或复制 2481 shared records/enums，不重定义 2482 DQ/PIT/cache identity、2484 adapter 或
  2485 selector；
- 不激活 2485 tracked selection：其 `OWNER_REVIEW_REQUIRED_BASELINE`、
  `selection_authorized=false` 必须原样保留；
- 不做 cash ledger、position sizing、PnL、expiry/assignment/corporate action；这些由 2487/2488 继续；
- 不用 daily close 代替 minute quote，不允许 same-bar fill，不把 missing/stale/crossed quote 平滑或回填。

## 2. Exact authority 与继承边界

- frozen base / local main / origin main：
  `361855107b89423e6501368185ac9ee08b9331a7`；
- TRADING-2481 shared contract SHA-256：
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- TRADING-2481 shared policy SHA-256：
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- TRADING-2482 DQ/PIT policy SHA-256：
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；
- TRADING-2484 adapter policy SHA-256：
  `b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`；
- TRADING-2485 selection policy SHA-256：
  `bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30`；
- primary requested/evaluated start 必须保持 `2021-02-22`；`2022-12-01` 不是默认值；
- external Owner token 继续为 `NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`。

2486 只使用 2481 `OrderIntentRecord`、`OrderEventRecord`、`FillEventRecord` 的 `.seal()`、
`.from_json_bytes()` 与 canonical bytes/hash authority。不得复制 shared record、建立 task-local 等价 record、
修改 shared registry 或在输出外围伪造 content hash。任何 shared schema/DQ/PIT/adapter/selector breaking
change 必须另开最小 reviewed serial contract wave。

## 3. Policy、阈值与授权状态

### 3.1 Tracked default policy

tracked policy schema 为 `qqq_options_minute_execution_reality_policy.v1`，默认必须满足：

- `status=OWNER_REVIEW_REQUIRED_BASELINE`；
- `execution_authorized=false`；
- submission latency、fill latency、maximum quote age、marketable-limit buffer、slippage、fee、
  per-quote partial-fill capacity、cancel/no-fill timeout 全部为
  `UNKNOWN_REQUIRES_POLICY_REVIEW`；
- safety 显式禁止 daily-close fill、same-bar fill、future quote、fill-forward ambiguity、cloud/external action；
- 所有 predecessor hashes、primary window、long-premium 与 research-only lineage exact-bound。

loader 可以读取该 policy 以产生 fail-closed typed result，但 simulator 不得据此提交或成交。默认未授权与
2485 `selection_authorized=false`/no-contract 两条路径都必须输出：

- `cash_preservation_required=true`；
- `order_intent=None`；
- `order_events=()`；
- `fill_events=()`；
- 可区分的 typed reason code。

### 3.2 后续 Owner-reviewed policy 与 sensitivity

只有显式 `OWNER_REVIEWED_ACTIVE`、`execution_authorized=true` 的 policy 才可运行 execution mechanics。
policy 必须携带 owner、decision、rationale、intended effect、validation plan、review/expiry condition，以及
所有影响投资解释的 canonical typed 数值。测试可在临时 fixture 中使用 `SYNTHETIC_TEST_ONLY` 数值，但
不得写入 tracked default 或作为投资结论。

`slippage=0` 仅能用于 `ISOLATION_SENSITIVITY`，必须有 DQ caveat、`reality_baseline=false`，且报告不得
把它称为 reality/default。其他起点或参数情景同样必须具备 reviewed sensitivity/proxy/stress role。

## 4. Typed input、DQ/PIT 与阶段状态

request 必须 cross-bind：run/repository identity、2481 contract/shared policy、2482 DQ/PIT、2484 adapter、
2485 selector、2486 execution policy、primary requested/evaluated range、signal/selection/intent timestamps、
option SID、side、contracts、limit/reservation inputs、canonical selection decision bytes/hash 与 source
checksum。quote input 必须显式携带 stable source id/checksum、bid/ask、quote start/end UTC、可用 contracts
和原始序列 identity；caller 不得提供 task output content hash。

execution-stage DQ 只能从 canonical `DQReportRecord`/receipt bytes 的事实派生：

- 必须用 2481 `DQReportRecord.from_json_bytes()` 严格解析并重验 canonical bytes/hash；
- report 的 schema/status/scope/as-of/range/run/code/policy/contract/source/safety 必须 cross-bind；
- 必须调用 2482 同一 evaluator/validation path 补充 execution chronology 事实，不信任 caller 自报 PASS；
- selection-stage checks PASS 不能伪造全生命周期 DQ PASS；未发生或未提供事实的 check 保持
  `NOT_EVALUATED`，semantic FAIL/UNKNOWN 必须 fail closed；
- local cached-data DQ、adapter admission 与 selection decision 均不能替代 execution event DQ/PIT；
- missing/stale/crossed/future/fill-forward-ambiguous quote、scope/as-of/hash mismatch 均 no-fill/fail closed。

## 5. Deterministic execution 与 identity

### 5.1 Chronology 与 quote-side rule

结构性 chronology 固定为：

`signal < selection < intent <= submit < fill_quote_end <= fill`。

- intent 只能在 signal/selection 后创建，`not_before_utc` 必须落在 intent 后的下一独立 minute boundary；
- submit/fill quote 不能来自 intent bar、daily close 或 future bar；同一 bar 即使价格可成交也不得 fill；
- `BUY_TO_OPEN` 只以 ASK 判断与记账，marketable limit 必须不低于 ask，fill price 不得高于 limit；
- `SELL_TO_CLOSE` 只以 BID 判断与记账，marketable limit 必须不高于 bid，fill price 不得低于 limit；
- 不可成交、missing/stale/crossed quote 必须得到 typed no-fill/cancel/reject，不能静默使用 mid/last/close；
- event sequence、fill sequence、cumulative filled contracts 必须严格单调，partial 总和不能超过 intent qty。

### 5.2 Canonical identity 与 replay

deterministic identity 必须绑定并 canonical-sort：

- 所有 intent→submit→quote→partial/fill/cancel/reject 时间与 sequence；
- 每条 quote/source checksum、quote-set checksum、canonical DQ report checksum；
- contract/shared-policy/DQ-PIT/adapter/selector/execution-policy hashes；
- run/repository/range/option SID/side/quantity/limit/fee/slippage/latency/capacity facts；
- 生成的 2481 intent/order/fill record hashes 与最终 typed result reason/status。

输入 quote/event 排列不得改变 replay；重复或 conflicting stable identities 必须 fail closed。partial replay 必须
byte-identical，并保持 order/fill cumulative identity。fee、slippage 与 fill price 的 exact identity 必须可从
policy facts、quote side 和 shared record canonical bytes 重建，不能由 caller 隐式注入。

## 6. Public API 规划

- `UnresolvedExecutionCriteria` / `ActiveExecutionCriteria`；
- `QQQOptionMinuteExecutionSafety`；
- `QQQOptionMinuteExecutionPolicy` / `QQQOptionMinuteExecutionPolicyLoadResult`；
- `QQQOptionExecutionQuoteInput`；
- `QQQOptionExecutionRequest`；
- `QQQOptionExecutionResult`；
- `QQQOptionMinuteExecutionContractError`；
- `load_qqq_options_minute_execution_policy()`；
- `build_qqq_option_execution_quote_set_sha256()`；
- `simulate_qqq_option_minute_execution()`。

task-owned models 使用 strict/frozen validation。canonical lifecycle outputs 仅为 2481 shared records；2486
result 只包装这些 records 与 identity/reason/safety，不建立另一个 lifecycle truth。

## 7. 测试与验收

|阶段|工作|验收|
|---|---|---|
|S0|Requirement、task row、claims、START/LANE|exact base、contract-change、lease、paths PASS|
|S1|Policy/loader 与 unresolved default|hash/extra/status/safety/zero-slippage role negatives PASS|
|S2|Typed input、DQ/PIT cross-binding|forged PASS、semantic FAIL/UNKNOWN、scope/as-of/hash negatives PASS|
|S3|Intent/order/fill engine|next-independent-minute、ASK/BID、limit、partial/reject/cancel/no-fill unit PASS|
|S4|Determinism/property/golden|input permutation invariant、partial replay、fee/slippage/fill identity PASS|
|S5|System flow/generated/formal/integration|focused、compatibility、five-tier final tree、ordinary push/cleanup PASS|

至少覆盖：default unauthorized、2485 selection blocked、same-bar、daily-close、stale/missing/crossed quote、
future quote、not-marketable limit、buy/sell limit bound、partial then fill、partial then cancel、reject、duplicate/
conflicting source id、event/fill sequence、quote permutation、canonical replay、fee/slippage identity、zero-slippage
isolation-not-reality negatives。

工程退出：`MINUTE_QUOTE_EXECUTION_MODEL_V1_READY_POLICY_BLOCKED`。

完整退出：`MINUTE_QUOTE_EXECUTION_MODEL_V1_READY`，只有 Owner-reviewed tracked policy 与独立 evidence 后
才能使用。若数值 authority 仍未提供，本任务以 `BASELINE_DONE` 收口并保留 blocker，不能声称完整退出。

## 8. Governed execution、claims 与生命周期

- mode：`SINGLE_LANE`；`contract_change=true`；
- frozen base：`361855107b89423e6501368185ac9ee08b9331a7`；
- branch：`codex/trading-2486-qqq-options-minute-execution`；
- 复用 clean checkout：`D:\Work\AITradingSystem_ops073_integration`；不新建 worktree；
- task-owned：本 requirement、execution policy、minute execution module、focused tests、module/flow fragments；
- coordinator-owned：task register、system flow、ARCH-004 compatibility/current authority、ARCH-004G frozen
  inventory/tests、ARCH-005 registry/index、DevEx/generated state、2486 task shadow；
- task shadow id SHA-256：
  `36425631db16a417871e71de9cf0ae6018a233d1a3c9d7c9523a8d0f3bb20693`；
- exit condition：final evidence 进入 canonical runtime location，ordinary push/remote SHA 验证完成后删除
  task branch；复用 checkout 返回 clean main。Git main/SHA/reflog 是恢复边界。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、复制、stage
或修改；repo-wide inspection 只使用 governed worktree audit。

## 9. 进度记录

- 2026-08-02：TRADING-2485 完成 ordinary push/cleanup，exact main=
  `361855107b89423e6501368185ac9ee08b9331a7`；Full 资源释放，external QuantConnect action=none。
- 2026-08-02：READ_ONLY checkout/preflight PASS；确认 `SINGLE_LANE`、`contract_change=true` 与上述
  claims。阈值审计确认所有 execution 数值仍为 `UNKNOWN_REQUIRES_POLICY_REVIEW`，task row 转
  `IN_PROGRESS`；START/LANE 随后均 PASS，无 lease/blocker/warning/serial requirement。
- 2026-08-02：focused 首轮同覆盖 `-n 16 --dist loadfile` 为 `24 passed / 2 failed in 3.86s`
  （命令 wall time 49.7s，含 worker 启动）。失败节点仅为
  `test_selection_policy_hash_lineage_tamper_fails_closed` 的 expected regex 与更精确 typed code 不一致、
  `test_golden_execution_identity_is_stable` 的待录入 golden placeholder；无级联、无 product-code failure。
  只修正两处 test literal 后以完全相同覆盖重跑，`26 passed in 3.77s`。首轮保留为 focused
  failure-fix evidence，不作为 formal gate；Architecture 继续暂停。
- 2026-08-02：policy/module LF SHA-256 分别为
  `8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a` /
  `e2f50a6fef80295d79d28fba1f40b7943ec2a2d14e0d2375084c6c87c4e13feb`。Ruff、mypy、
  compileall PASS；2481–2486 adjacent 首轮 `164 passed in 8.76s`，仅 mypy Optional narrowing 失败；
  修正 `_utc()` 显式 `offset is None` narrowing 后完全相同组合 `164 passed in 8.80s`。DevEx=
  `1072 modules / 1239 tests / 856 writers / 0 violations`，task shadow=`958 / 453 / 505`
  byte-identical。tracked default 未授权，工程退出达到
  `MINUTE_QUOTE_EXECUTION_MODEL_V1_READY_POLICY_BLOCKED`，状态转 `BASELINE_DONE`；formal final-tree
  gates 仍待 compatibility/current-authority 收口后执行。
- 2026-08-02：compatibility/deprecation 首轮使用
  `python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py tests/test_arch_004g_deprecation.py`
  得到 `166 passed / 17 failed in 126.05s`。16 个级联节点覆盖 2470–2484 current/successor-authority
  测试（`test_trading_2470_cited_query_consumer_is_current_hash_authority`、
  `test_trading_2471_flow_focus_is_current_hash_authority`、
  `test_trading_2472_status_provenance_is_current_hash_authority`、
  `test_trading_2473_evidence_drilldown_is_current_hash_authority`、
  `test_trading_2474_result_ledger_is_current_hash_authority`、
  `test_trading_2475_historical_coverage_is_current_hash_authority`、
  `test_trading_2476_adapter_review_is_current_hash_authority`、
  `test_ops_072_transport_is_current_hash_authority`、
  `test_trading_2477_historical_adapter_is_predecessor_hash_authority`、
  `test_trading_2478_quantconnect_planning_is_current_hash_authority`、
  `test_trading_2479_historical_projection_review_is_current_hash_authority`、
  `test_trading_2480_qc_capability_admission_is_current_hash_authority`、
  `test_trading_2481_qqq_options_shared_contract_has_2482_successor_authority`、
  `test_trading_2482_qqq_options_dq_pit_identity_has_2483_successor_authority`、
  `test_trading_2483_qqq_options_signal_package_has_2484_successor_authority`、
  `test_trading_2484_qc_project_adapter_has_2485_successor_authority`），另 1 个节点为
  `test_trading_2485_qqq_option_selection_has_2486_successor_authority`。根因是新增 2486 section 后，
  current authority union 仍止于 2485，且 2485 测试仍要求其位于 EOF；不是 execution 产品行为失败。
  修复仅恢复完整历史 authority union、将七处 successor/current-authority 引用提升到 2486、把 2485
  EOF 断言收窄为 2485→2486 邻接，并刷新 2486 test source hash；未改历史 payload、prefix、exact-byte、
  hash 规则或 superseded/source delta。以完全相同命令和 183-node 覆盖重跑为
  `183 passed in 109.49s`；首轮保留为 focused failure-fix evidence，不作为 formal gate。
