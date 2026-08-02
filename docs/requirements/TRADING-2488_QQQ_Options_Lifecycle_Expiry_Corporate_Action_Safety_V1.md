# TRADING-2488：QQQ Options Lifecycle、Expiry 与 Corporate-Action Safety V1

最后更新：2026-08-03

稳定任务 ID：
`TRADING-2488_QQQ_OPTIONS_LIFECYCLE_EXPIRY_CORPORATE_ACTION_SAFETY_V1`

优先级：`P1`

状态：`BASELINE_DONE`

工程授权：

```text
owner_decision:TRADING-2488:2026-08-03:implement_offline_long_premium_lifecycle_safety_policy_blocked_v1
```

production effect：`none`

broker action：`none`

## 1. 目标与边界

本任务实现 QQQ long-premium research 的 offline position lifecycle safety slice：把已冻结的
contract selection、minute execution 和 cash accounting 事实重放为 deterministic
`FLAT / INTENT_PENDING / OPEN_PARTIAL / OPEN / EXIT_PENDING / EXIT_BLOCKED / CLOSED /
SCOPE_VIOLATION / INVALID_RUN` 状态序列，并对 pre-expiry、expiry、exercise、assignment 与
corporate action 建立 fail-closed scope gate。

本任务只实现机械状态、identity、DQ/PIT 继承、cash/cost-basis 对账与安全退出。它不替 Project Owner
选择 pre-expiry buffer、quote/observation freshness、自动 exercise threshold、expiry settlement source、
position sizing 或任何投资解释规则。tracked default 必须保持 unauthorized；工程退出为
`LONG_PREMIUM_LIFECYCLE_V1_READY_POLICY_BLOCKED`，完整退出仍依赖 reviewed policy 与 platform evidence。

明确不实现：

- short premium、assignment-sensitive strategy、multi-leg、roll、LEAPS 或 Wheel；
- 真实 exercise、assignment、QQQ share delivery、short underlying、corporate-action contract adjustment；
- 新 order intent、fill、broker mutation 或用 daily close/same-bar price 伪造 exit；
- QuantConnect 登录、project、API、CLI、HTTP、cloud run、raw options export；
- paper/live/broker/production 或投资结论、promotion、参数优化；
- 把 TRADING-2494 Atlas 页面状态当作 2481–2493 工程验收 authority。

## 2. Exact inherited authority

2488 必须继承且不得重定义：

|Authority|Exact identity|2488 用法|
|---|---|---|
|2481 shared record schema|`c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`|只用 12-record contract 的 `seal/from_json_bytes/canonical_bytes/content_sha256`；生命周期只发出既有 `PositionLifecycleEventRecord` 与可选 downstream `PortfolioSnapshotRecord`。|
|2481 shared policy|`d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`|继承 research-only、no promotion/export/execution safety。|
|2482 DQ/PIT policy|`1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`|只从 canonical report facts 派生 lifecycle-stage DQ/PIT，UNKNOWN 永不升级为 PASS。|
|2484 adapter policy|`b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616`|保留 QQQ/MINUTE、no cloud/no pretend engine boundary。|
|2485 selector policy|`bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30`|candidate snapshot 提供 selected SID 的 right/expiry/strike/multiplier；默认 selection 仍 blocked。|
|2486 execution policy|`8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a`|只重放 canonical `QQQOptionExecutionResult`、intent/order/fill；不生成新成交。|
|2487 accounting policy|`faa2659ee141cb2209686c3eadee31059ee660c3cc6d6dd3e63e259f23b1484e`|只消费 sealed accounting result/ledger/positions/snapshot，不复制会计逻辑。|

baseline policy 必须绑定以上 exact hashes。测试中的 `OWNER_REVIEWED_ACTIVE` fixture 可以显式绑定同一次
synthetic reviewed predecessor-policy chain，但不得修改 tracked policies，也不得把 fixture identity 写入生产默认。

## 3. Primary Research Window 与 chronology

- primary requested/evaluated start 默认且只能是 `2021-02-22`；
- `2022-12-01` 不是默认、minimum start 或结论边界；
- 其他起点只允许经 reviewed sensitivity/proxy/stress role 与 DQ caveat；V1 tracked policy 不批准任何此类 authority；
- event chronology 必须保持 `signal < selection < intent <= submit < quote < fill <= accounting snapshot <= lifecycle evaluation`；
- expiry、pre-expiry 与 settlement day 只通过 sorted/unique reviewed exchange sessions 计算，不能用自然日或周末补齐；
- input tuple 排列不得改变 lifecycle input hash、event identity、terminal state 或 downstream snapshot。

## 4. Policy 与 threshold governance

新增 tracked policy：
`config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml`。

default：

```text
status=OWNER_REVIEW_REQUIRED_BASELINE
lifecycle_authorized=false
criteria.mode=UNRESOLVED
scenario_role=UNKNOWN_REQUIRES_POLICY_REVIEW
pre_expiry_guard_sessions=UNKNOWN_REQUIRES_POLICY_REVIEW
max_exit_quote_age_ms=UNKNOWN_REQUIRES_POLICY_REVIEW
expiry_settlement_source_policy=UNKNOWN_REQUIRES_POLICY_REVIEW
reality_baseline=false
```

任何数值或方法字段只有在 policy 同时记录 owner、version/status、rationale、intended effect、validation plan、
review/expiry condition 与 exact predecessor hashes 后才能进入 `ACTIVE`。zero buffer、daily close、missing/stale
quote 回填、synthetic fixture 均不能成为 reality baseline。

不依赖可调数值的机械安全 invariant：

- strictly positive intrinsic value 的到期 long call/put 可能产生 underlying exposure，V1 直接 fail closed；
- unexpected exercise/assignment 一律 scope violation；
- 影响 open option 的 split/special dividend/merger/symbol change/contract adjustment 一律 invalid，V1 不调整合约；
- no shares、no short underlying、no short option、no margin、no negative cash、no roll；
- non-positive intrinsic value 只有在 reviewed expiry settlement observation 与 DQ/PIT PASS 时才可记为
  `EXPIRED_WORTHLESS`；缺证据不得猜测。

## 5. Canonical input admission

Request 至少包含：

- canonical `RunManifestRecord` bytes + file SHA-256；
- sealed `QQQOptionCashAccountingResult` bytes + file SHA-256；
- 2486 execution result artifacts（bytes + file SHA-256）；
- open/filled SID 对应的 2485 `ContractCandidateSnapshotRecord` artifacts；
- evaluation UTC、as-of session、reviewed exchange sessions 与 calendar source checksum；
- pre-expiry quote、expiry settlement、exercise/assignment、corporate-action observations；
- producer version 与 lineage id。

每个 artifact 必须 exact-byte replay、file/content hash、schema、run、code、range、policy、contract、SID、multiplier、
source checksum 与 lineage cross-bind。2487 portfolio snapshot 中的 run-manifest/calendar/execution source list 必须与
request exact 相等。duplicate semantic identity、duplicate file hash、missing/extra SID、wrong policy、tamper、naive/future
time、unsorted/duplicate/weekend calendar 全部 fail closed。

DQ gate 不信任 caller status：每个 market/external observation 必须携带 canonical `DQReportRecord` bytes/hash，
严格验证 schema、scope、run/code/range/as-of、source id/checksum 和 report content seal；stage status 从 report facts 与
predecessor records 的最差状态派生。`FAIL` 或 `NOT_EVALUATED` 不得产生 valid expiry/exit conclusion。

## 6. State-machine semantics

只使用 2481 已冻结 transition；不得修改 enum 或复制 record：

|事实|状态迁移|关键 reason / quantity / cash|
|---|---|---|
|BUY/SELL intent，终端无 fill/reject/cancel|2487 保持 `EXECUTION_BLOCKED_CASH_PRESERVED`，2488 不发布 lifecycle state|typed cash preservation；不得凭 intent 猜测 FLAT/open/exit。|
|BUY partial 后 cancel|`FLAT -> INTENT_PENDING -> OPEN_PARTIAL`|quantity 为 canonical fills 合计；cash 与 2487 ledger exact。|
|BUY full fill|`FLAT -> INTENT_PENDING -> OPEN`|quantity、multiplier、premium、fee 与 2486/2487 exact。|
|SELL_TO_CLOSE intent|`OPEN/OPEN_PARTIAL -> EXIT_PENDING`|不得在没有 canonical intent 时伪造 order。|
|SELL full fill|`EXIT_PENDING/EXIT_BLOCKED -> CLOSED`|quantity 减到 0；cash 与 2487 sell proceeds/fees 对账。|
|SELL no/partial fill|`EXIT_PENDING -> EXIT_BLOCKED`|保留剩余 quantity；不伪造 fill。|
|进入 reviewed pre-expiry guard 且无完成 exit|`OPEN/OPEN_PARTIAL -> EXIT_PENDING`，必要时 `-> EXIT_BLOCKED`|仅发 safety state；不生成 order/fill。|
|reviewed settlement 下 OTM/ATM expiry|`OPEN/OPEN_PARTIAL -> EXIT_PENDING -> CLOSED`|`EXPIRED_WORTHLESS`，cash delta 0；剩余 cost basis 从 unrealized 转 realized。|
|ITM expiry / unexpected exercise or assignment|`OPEN -> SCOPE_VIOLATION -> INVALID_RUN`；partial/open-blocked 使用现有合法 path 到 `INVALID_RUN`|不得模拟 QQQ shares、short 或现金交割。|
|影响 open position 的 corporate action|合法路径到 `SCOPE_VIOLATION/INVALID_RUN`|不做 ratio/strike/multiplier adjustment。|
|expiry settlement missing 或 DQ/PIT 非 PASS|`EXIT_PENDING -> EXIT_BLOCKED -> INVALID_RUN`|typed unresolved expiry；不得沿用旧 quote。|

每个 shared lifecycle event 的 occurred time、sequence、source checksums、policy/contract/adapter/selector/execution/
accounting lineage 都进入 canonical identity。事件序列、terminal quantity 与 2487 position/ledger 必须闭合。

## 7. Output contract

task-owned sealed result 至少暴露：

- policy/predecessor/input/accounting-result hashes；
- `lifecycle_authorized`、`investment_interpretation_allowed`、`cash_preservation_required`；
- typed reason、`run_valid`、DQ/PIT status；
- ordered shared `PositionLifecycleEventRecord`；
- sorted task-owned terminal position summaries；
- optional downstream shared `PortfolioSnapshotRecord`；
- `new_order_intent_count=0`、`new_fill_count=0` 与 no-external safety；
- canonical `seal/canonical_bytes/from_json_bytes/content_sha256`。

default unauthorized、blocked accounting 与 invalid input 路径不得暴露 partial state；必须给出 typed
cash-preservation/no-new-order/no-new-fill 结果。active valid output只有在所有 predecessor、DQ/PIT、calendar、state、cash
与 policy cross-binding 通过后才可产生。`investment_interpretation_allowed` 还必须继承 2487 approved reality baseline；
synthetic fixture 永远为 false。

## 8. Verification matrix

focused unit/property/golden 至少覆盖：

1. tracked default exact unresolved、unauthorized、cash preservation、no events/order/fill；
2. blocked 2485/2486/2487 不能被 2488 激活；
3. BUY full、partial cancel、reject/no fill、SELL full、partial/no-fill state replay；
4. CALL/PUT ITM expiry invalid、OTM/ATM expiry closed、realized/unrealized/cash snapshot reconciliation；
5. pre-expiry guard default unresolved、reviewed session calculation、missing/stale/crossed quote；
6. unexpected exercise、unexpected assignment、underlying split 与其他 corporate action invalid；
7. settlement observation/report missing、semantic FAIL/NOT_EVALUATED、scope/as-of/source/hash mismatch；
8. forged/tampered/noncanonical predecessor bytes、duplicate identities、wrong SID/multiplier/policy/lineage/calendar；
9. input permutation property、canonical result replay、extra field/tamper rejection、stable golden hashes；
10. default window `2021-02-22`、unreviewed non-primary start FAIL、`2022-12-01` not default；
11. no daily-close/same-bar fill、no share/short/margin/roll、no external action；
12. adjacent 2481–2488、compatibility/deprecation、DevEx/task shadow、five-tier final-tree validation。

## 9. Governed stages 与 path claims

mode：`SINGLE_LANE`；`contract_change=true`；frozen base：
`13292726540dc78039a85f17a39f64ddbee956d1`；branch：
`codex/trading-2488-qqq-options-lifecycle`。

|阶段|工作|退出条件|
|---|---|---|
|S0|task row、requirement、claims、START/LANE|registered、exact base、lease/path/contract preflight PASS。|
|S1|policy/loader/default blocked|全部 unresolved、authority/safety/extra/hash negatives PASS。|
|S2|canonical predecessor admission|2485–2487 exact-byte/hash/run/range/policy/lineage/tamper tests PASS。|
|S3|state replay 与 pre-expiry|open/partial/exit/blocked/closed chronology、reviewed calendar、no new order/fill PASS。|
|S4|expiry/exercise/assignment/corporate action|ITM/OTM、missing evidence、split、scope violation/invalid PASS。|
|S5|snapshot、property、golden|quantity/cash/PnL reconciliation、permutation、canonical/tamper PASS。|
|S6|shared wiring 与 closeout|system flow、generated/current authority、focused/formal、ordinary push/cleanup PASS。|

task-owned paths：

```text
config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml
src/ai_trading_system/qqq_options_research/position_lifecycle.py
tests/test_qqq_options_position_lifecycle.py
docs/requirements/TRADING-2488_QQQ_Options_Lifecycle_Expiry_Corporate_Action_Safety_V1.md
config/architecture/fragments/flows/qqq_options_position_lifecycle.yaml
config/architecture/fragments/modules/qqq_options_position_lifecycle.yaml
```

coordinator-owned paths：

```text
docs/task_register.md
docs/system_flow.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
tests/test_trading2452_architecture_contract.py
```

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、copy、stage 或修改。

## 10. Status log

- 2026-08-03：TRADING-2494 ordinary push/cleanup RELEASE 后，从 exact latest main
  `13292726540dc78039a85f17a39f64ddbee956d1` 建立 2488 SINGLE_LANE；只读 authority/API/policy/test
  设计完成。本次写入仅登记 row 与 requirement；START/LANE PASS 前不写 implementation。
- 2026-08-03：START/LANE preflight PASS 后完成 task-owned policy/module/tests 与 system-flow/architecture
  fragments。tracked default 继续 `OWNER_REVIEW_REQUIRED_BASELINE` / `lifecycle_authorized=false`，policy
  SHA-256=`1798b6696e0f31571f9242a4276a06530fb951d15f250a2ef6756ac547037582`。focused
  final=`29 passed`；保留三次 failure-fix 记录：`20 passed / 2 failed`（fixture enum/window）、
  `26 passed / 1 failed`（正确识别 no-fill 由 2487 cash-preservation 截止）、`28 passed / 1 failed`
  （扩展 helper 后 golden identity 刷新）。实现覆盖 open/full/partial、SELL full/partial、no-fill blocked、
  fresh/missing/stale/DQ-fail pre-expiry quote、OTM/ITM CALL/PUT expiry、missing/FAIL/NOT_EVALUATED
  settlement、exercise/assignment/corporate action、source/calendar/tamper、permutation 与 golden replay；
  formal 前 generated/adjacent/compat authority 与 final-tree gates 仍待执行。
- 2026-08-03：2480–2488 adjacent 完全并行组合 `237 passed in 10.01s`。task shadow
  generate/validate=`959 total / 454 active / 505 completed`，legacy/v2 byte-identical；DevEx
  generate/validate=`1075 modules / 1242 tests / 856 direct writers / 0 violations`。compatibility/
  deprecation 固定 `python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py
  tests/test_arch_004g_deprecation.py` 首轮 `101 passed / 84 failed in 227.53s`：83 项为新 2488
  task-shadow 尚未进入 current authority 的同一差集级联，1 项为新增 module/test 后 frozen inventory stale。
  第二轮 `107 passed / 78 failed in 202.12s` 定位到首次 current-authority 扣除范围过宽；第三、四轮均为
  `184 passed / 1 failed`，分别补齐 frozen deprecation inventory 与其 2488 exact authority path。历史 payload、
  prefix、exact-byte/hash 规则均未改弱；最终相同覆盖 `185 passed in 140.96s`。formal 前不再修改 tracked
  bytes；canonical five-tier final-tree validation 尚待串行执行，Full 必须独占。
- 2026-08-03：首个 final-tree Full 以 exact provenance
  `natural_integration_boundary / TRADING-2488_QQQ_OPTIONS_LIFECYCLE_EXPIRY_CORPORATE_ACTION_SAFETY_V1 /
  TRADING-2488-QQQ-OPTIONS-LIFECYCLE-EXPIRY-CORPORATE-ACTION-SAFETY-V1` 运行，terminal
  `8183 passed / 2 failed / 6 skipped / 644 warnings in 1285.62s`，parent artifact=
  `outputs/validation_runtime/full_20260802T163726Z/test_runtime_summary.json`。两项 exact node 为
  `tests/test_trading2452_architecture_contract.py::test_trading2452_compatibility_sources_are_current_and_auditable`
  与 `::test_trading2453_w8e1_compatibility_sources_are_current_and_auditable`；独立 historical-source helper
  只识别 compatibility baseline 中 2494 captured hash，未继承 2488 exact successor current-authority paths，
  因而首先拒绝 `docs/system_flow.md` 与 `tests/test_arch_004g_deprecation.py`。这不是 lifecycle/accounting
  semantic failure；failure-fix 只允许该 test 的 exact current-authority set、2488 compatibility test authority
  与相应 generated refresh，不改历史 payload、baseline prefix/hash、产品行为或 known-unrelated bytes。旧 Full
  仅作 failure-fix parent，final tree 必须从 Architecture 开始重跑完整五级门。
- 2026-08-03：Full failure-fix focused 使用完全并行组合
  `python -m pytest -n 16 --dist loadfile tests/test_trading2452_architecture_contract.py
  tests/test_qqq_options_position_lifecycle.py`，首轮 `33 passed / 2 failed in 4.85s`；唯一 fixture-authority
  错误是把 2488 exact current path 的 prior append-only owner 误限定为 2494，而该 test 本身的最近 historical
  owner 是 `ARCH-005S4E`。修复为 path 必须属于 2488 exact set、prior owner 必须处于现存 2494 latest chain
  之前/当期；非 set path 仍严格要求 latest captured hash 等于 live bytes。相同覆盖最终
  `35 passed in 5.38s`。
- 2026-08-03：failure-fix requirement 写回并重建 generated authority 后，compatibility/deprecation
  首次有效同覆盖重跑为 `134 passed / 51 failed in 179.17s`；此前一次命令仅因本地执行器 5 秒
  timeout 被终止、无 pytest node terminal，不计作验证证据。51 项全部是同一新增 current path
  `tests/test_trading2452_architecture_contract.py` 未被早期 section 的 mismatch helper 识别为 2488
  post-2494 successor authority 所致。修复只在 2494 chain 存在时从历史 mismatch 集扣除
  `TRADING_2488_QQQ_OPTIONS_POSITION_LIFECYCLE_SUCCESSOR_SHADOW_PATHS` 三条 exact path；不改历史
  baseline、prefix、captured hash 或产品行为，非 exact-set live mismatch 仍 fail closed。相同 185-test
  覆盖必须再次 terminal PASS 后方可启动 Architecture。
- 2026-08-03：上述 exact successor-current 修复与 generated rebuild 完成后，相同 compatibility/
  deprecation 覆盖 `185 passed in 152.06s`；2480–2488 九文件 adjacent `237 passed in 10.59s`；
  `test_trading2452_architecture_contract.py + test_qqq_options_position_lifecycle.py` failure-fix
  组合 `35 passed in 4.96s`。下一步仅在 rebuilt-generated、source-hash/prefix、lint/compile、checkout
  与 runner pre-audit 全 PASS 后，从最终 tracked tree 重跑 Architecture→Contract→Integration→
  Reproducibility→exclusive failure-fix Full。
