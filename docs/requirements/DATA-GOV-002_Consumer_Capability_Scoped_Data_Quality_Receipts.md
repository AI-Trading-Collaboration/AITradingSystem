# DATA-GOV-002：Consumer Capability-Scoped Data Quality Receipts

最后更新：2026-07-28

稳定任务 ID：`DATA-GOV-002_CONSUMER_CAPABILITY_SCOPED_DATA_QUALITY_RECEIPTS`

Owner 决定：
`owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1`

状态：`IN_PROGRESS_PHASE_C_CONTRACT_BASELINES_DONE_ADOPTION_REVIEW_PENDING`

## 1. 问题与目标

当前 canonical `daily_default.v1` DQ profile 以整份 market/macro cache 为验证与消费边界。
这对 daily operation 是正确边界，但会把只消费少量、预先冻结输入的 research consumer 与无关
instrument 的数据问题绑定。例如 TRADING-2460 Label Foundation 只消费 QQQ、SPY、SGOV
价格，却会被 `^VIX` 的 `prices_non_market_session_date` 阻断。首个真实 pilot 进一步确认：
该标签消费者不读取任何 rates series，因此 policy 的 exact rate scope 必须为空，不能为了贴合
全局 DQ profile 而虚构 DGS3MO 依赖。

本任务建立长期的 consumer capability receipt 层，目标是：

1. 保留 `aits validate-data` / `validate_data_cache` 的 canonical 全局运行与问题披露；
2. 允许预先冻结且经过 Owner review 的 consumer 只对自己的 transitive input closure 取得
   strict `PASS` capability receipt；
3. 把“该 capability 可消费”与“全局 cache PASS”“daily operation 可运行”“其他 consumer
   可复用”严格分离；
4. 让策略研究只依赖 immutable、content-addressed、可重放的输入 capability，而不依赖 daily
   orchestration 内部状态；
5. 逐步把 instrument-scoped DQ issue 从非结构化文字升级为可机器判定的 affected-instrument
   事实，任何无法证明 scope 的错误继续全局阻断。

这不是 DQ exception、silent filtering 或 D0B2B 替代方案。D0B2B 仍负责完整 daily profile 的
operational acceptance；本任务不改变 daily score、periodic operation、production 或 broker 门禁。

## 2. 长期目标架构

```text
immutable canonical snapshot/publication
                 |
                 +--> full canonical DQ report/receipt
                 |         |
                 |         +--> daily_default consumer（仍要求完整 strict PASS）
                 |
                 +--> reviewed capability policy
                           |
                           +--> exact source-byte capture
                           +--> predeclared row/column/window projection
                           +--> same-code-path scoped DQ
                           +--> content-derived capability receipt
                                      |
                                      +--> one exact authorized consumer
```

每份 capability receipt 必须绑定：

- `capability_id`、version、consumer id/version、Owner decision；
- policy path、policy SHA-256、数据质量 policy path/SHA-256；
- requested/evaluated window 和 `as_of`；
- canonical source role/path/SHA-256/size/row count；
- exact required price tickers、rate series、字段和 calendar semantics；
- full canonical DQ status、report path/hash、全部 blocker code；
- scoped strict DQ status、report path/hash、全部 blocker code；
- materialized panel/rates path/hash/size/row count；
- structured global-error isolation result；
- `global_cache_pass_claimed=false`（当 full status 非 `PASS`）；
- `cross_consumer_reuse_allowed=false`、`daily_operation_authorized=false`、
  `production_effect=none`、`broker_action=none`。

## 3. Fail-closed 判定规则

### 3.1 Scope freeze

Capability policy 必须在读取运行结果之前固定：

- consumer identity/version；
- required instruments/series/fields；
- research window 与 calendar；
- accepted scoped status，首版只允许 exact `PASS`；
- 可隔离的 global issue code；
- 每个可隔离 issue 所需的结构化 attribution 类型；
- review/expiry condition。

运行时参数只能等于 policy，不能扩大或缩小 scope。不得在发现错误后临时删除 ticker、series、
日期、字段或 source role。

### 3.2 Full report 仍必须运行

每次 materialization 先从一次 immutable byte capture 运行 full canonical DQ，并保存报告。
Full report 的 provenance、manifest、publication、schema、parse 或无结构化 attribution 的 ERROR
一律阻断 capability。只有 policy 明确允许、且 issue 自带 non-empty structured
`affected_instruments`、并与 frozen required instruments 完全不相交时，才可进入 scoped
validation。

首个 pilot 只允许隔离：

- `prices_non_market_session_date`；
- attribution rule=`ALL_AFFECTED_INSTRUMENTS_OUTSIDE_REQUIRED_SCOPE`。

不得按 message/sample 文本猜测 affected ticker。

### 3.3 Scoped strict validation

Scoped panel 必须由同一批 canonical source bytes 机械投影，并再次调用
`validate_data_cache`。以下任一情况均不得发布 PASS receipt：

- status 不是 exact `PASS`；
- required ticker/series 缺失或出现额外 ticker/series；
- source bytes、policy bytes、DQ policy bytes或 materialized bytes漂移；
- panel 不是 canonical source 的 exact frozen-scope projection；
- global blocker 未被 policy + structured attribution 完整解释；
- report、receipt id、checksum、row count、window、`as_of` 或安全字段不一致。

## 4. 分阶段实现

### Phase A：typed contract 与 TRADING-2460 pilot

依赖：Owner 当前决定；不依赖 D0B2B operational acceptance。

交付：

1. `data_quality_consumer_capability_policy.v1` reviewed policy；
2. `data_quality_consumer_capability_receipt.v1` typed/content-derived contract；
3. immutable source capture、scoped materializer 与 verifier；
4. `DataQualityIssue.affected_instruments` 的向后兼容扩展，并先覆盖
   `prices_non_market_session_date`；
5. TRADING-2460 v2 label policy 与 source package binding；
6. clean、required-instrument failure、out-of-scope VIX failure、unstructured error、
   scope drift、policy/source/report/panel/receipt tamper tests；
7. system flow、artifact/report registry 和任务状态同步。

Phase A 退出：

- global exact `PASS` 时 capability strict PASS；
- global 因仅 `^VIX` non-session ERROR 失败、scoped QQQ/SPY/SGOV strict PASS 时，receipt
  可以是 PASS，但所有 artifacts 明确 full DQ FAIL、global PASS claim=false；
-同一 error 触及 QQQ/SPY/SGOV、缺少 structured attribution、出现 manifest/publication/schema
  blocker或 scoped warning/error 时 fail closed；
- TRADING-2460 只能消费 exact capability id/version/consumer binding；
- required focused、architecture、contract、integration、reproducibility 与 natural-boundary
  Full 按项目规则通过；
- `production_effect=none`、`broker_action=none`。

### Phase B：generic consumer adapter

依赖：Phase A 至少一个真实 canonical run 和 tamper evidence PASS，且复核没有误隔离。

Phase B 按以下顺序实施，避免把共享合同与 consumer 行为迁移混在同一原子变更：

#### B1：generic dependency/discovery/preflight contract

交付：

- `data_quality_consumer_dependency.v1`：冻结 consumer、capability、policy、DQ policy 与
  exact consumer-side safety boundary；
- `data_quality_consumer_capability_discovery_pointer.v1`：只负责按
  consumer/version/as-of 定位 content-addressed immutable receipt，不携带或推导 PASS 权威；
- verifier-only `VerifiedConsumerDataCapabilityPreflight`：必须重新验证 pointer bytes、
  receipt bytes、content-addressed path、policy/source/projection/report 和 strict PASS 后才可创建；
- publication 必须先确认 immutable receipt 已存在且 byte-identical，再原子更新可变 discovery
  pointer；pointer 缺失、路径越界、symlink/junction、SHA/size/id/as-of/consumer/policy drift
  全部 fail closed；
- focused contract/tamper/containment/idempotency tests 与 system-flow 更新。

B1 不迁移任何 consumer runner，不改变现有 TRADING-2460 输出或 receipt bytes。

#### B2：第二 read-only consumer pilot

冻结候选为 `TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC@1.0.0`，原因：

- 它在 evaluator 前直接读取 canonical price cache 并运行 DQ，且所有输出保持
  diagnostic/segmentation-only、`production_effect=none`；
- 它使用 trailing-only price features 生成当时可见的 regime label，与 Phase A 使用未来区间
  构建 decision-target label 的时间方向和消费逻辑不同；
- 它不依赖 QLD，不复用 QLD scoped evidence，也不是 daily/periodic consumer；
- 其 exact transitive input closure 必须由现有 reviewed regime-label policy 和实际读取路径复核，
  rates 若只作为旧 global gate 输入而不被 evaluator 消费，不得虚构为 required series。

B2 交付：

- reviewed capability policy 与 consumer dependency declaration；
- runner 在 evaluator 和任何输出写入前只接受 exact verified preflight；
- legacy direct-DQ 与 capability 路径的成功/失败、安全字段和产物语义 characterization；
- required-scope/global-out-of-scope/tamper/wrong-consumer/wrong-as-of tests；
- 至少一次真实 canonical build/verify；没有真实 strict PASS 时只能保持 `BLOCKED_INPUT`，
  不得用 fixture PASS 宣称 pilot 完成。

不得自动迁移 daily/periodic consumer。

### Phase C：DQ issue attribution 扩展

依赖：逐 issue code 的 source owner review。

交付：

- price ticker、rate series、source role、window/field 等 typed attribution；
- source-wide 与 row-scoped issue taxonomy；
- 不再依赖 message/sample parsing；
- attribution completeness/tamper tests。

未迁移 issue 默认 `GLOBAL_OR_UNKNOWN_SCOPE` 并阻断。

### Phase D：consumer migration 与治理收敛

依赖：至少两个真实 consumer、多个真实批次、false-isolation review 和 Owner 独立授权。

交付：

- 逐 consumer migration matrix；
- capability inventory/expiry/revocation；
- duplicate scoped materialization/cache reuse 的 content-addressed 优化；
- global/consumer status dashboard。

不删除 full canonical receipt，不把局部 PASS 聚合为全局 PASS。

### Phase C → Phase D 过渡决策门（尚未授权）

截至 2026-07-29，工程前置事实已更新为：

- price C3P 与 rate C3 typed attribution contract 均已转 `BASELINE_DONE`；
- price contract 只覆盖 reviewed US equity calendar 约束下、checksum-bound
  `primary_market_prices` 的 exact approved site；rate contract只覆盖
  checksum-bound `primary_macro_rates` 的 exact approved six-site bundle；
- 任一 source/ticker-or-series/date/window/field/row/threshold/predecessor 等必需维度
  不完整时，现行行为仍保持 `GLOBAL_OR_UNKNOWN_SCOPE`；
- D0B2B/OPS-067 在 exact `main=b646fc9ae169f266a6b93fda572af20ebdfcffe8`
  已证明 canonical as-of=`2026-07-27` 的 strict DQ=`PASS`，receipt=
  `dq_execution_28af63a1e747ba675e17d3001d8028592b6ec0ef63e823bcfa9463889b0cb5c4`，
  errors/warnings=`0/0`；该 PASS 只恢复已审阅的 daily score consumer，不自动授权
  generic consumer cutover、automatic non-daily dispatch 或 capability adoption；
- 两个 read-only pilot 已存在，但“多个真实批次 + false-isolation review”尚未形成
  Phase D 的独立 Owner 决策证据。

下一次 Owner 决策必须针对 consumer-specific adoption review，而不是泛化批准。决策上下文
至少要绑定：

1. exact capability id/version、consumer id/version 与 dependency closure；
2. exact capability policy artifact/checksum、拟采用的 issue site/code 和 typed
   attribution contract/decision bytes；
3. 每个真实批次的 full/scoped receipt、source artifact/checksum、requested/evaluated
   window 与 attribution completeness；
4. false-isolation 正例、反例、unknown/incomplete 样本以及撤销/回滚条件；
5. 明确选择 `AUTHORIZE_CONSUMER_SPECIFIC_ADOPTION_REVIEW_WAVE`、
   `HOLD_FOR_MORE_FALSE_ISOLATION_EVIDENCE` 或 `REJECT_ADOPTION`。

在该决定形成前，不得修改 active capability policy、classifier allowlist、consumer，
不得启用 QLD automatic selection、daily/periodic generic cutover、production 或 broker
行为。Phase D 仍须在多个真实批次证据闭合后另获 Owner 授权。

## 5. 安全和投资边界

- 本任务只改变数据消费授权粒度，不改变任何 strategy target、feature、threshold、模型、权重或
  action universe。
- Capability PASS 不是策略有效、模型可学、promotion、paper-shadow 或生产就绪结论。
- TRADING-2460 pilot 仍为 historical-seen label foundation，不训练模型、不搜索 candidate、不运行
  strategy backtest。
- QLD scoped evidence/exception不得复用。
- daily operation、D0B2B、G4C、其他 consumer、production 与 broker 保持原门禁。
- 任何临时 workaround 必须另行 Owner 讨论并记录；本任务不允许 fallback 到 silent filter。

## 6. 进度记录

- 2026-07-26：Owner 要求工程线针对研究/工程互相阻塞问题制定长期方案并推进实现；建立
  DATA-GOV-002，批准先实施 typed capability receipt 与 TRADING-2460 pilot。
- 2026-07-26：Phase A 已实现 reviewed policy、typed/content-derived receipt、immutable source
  capture、full/scoped same-code-path DQ、structured affected-instrument attribution、exact
  materializer/verifier、TRADING-2460 v2 binding 与 tamper tests。真实 pilot 初次把 DGS3MO
  错列为 required input 时 scoped DQ 以 `rates_empty` 正确 fail closed；复核 label 数据依赖后，
  policy 修正为 price-only QQQ/SPY/SGOV，required rate scope 为空。
- 2026-07-26：真实 `2021-02-22..2026-07-24` 重建得到 receipt
  `dq_capability_e7f233ca6e0c41ce9506df46f067e56348004a19f953cf74626d5c9936ccb059`。
  Full canonical DQ 保持 `FAIL`，唯一 ERROR 为
  `prices_non_market_session_date`、affected instrument=`^VIX`；scoped DQ=`PASS`，
  `global_cache_pass_claimed=false`，禁止跨 consumer 复用及 daily/production/broker 授权。
  TRADING-2460 label status=`LABEL_FOUNDATION_READY`，common sessions=`1362`、
  rows=`5412`，content-derived validation 0 errors。
- 2026-07-26：Phase A 正式验证完成。Focused DQ/consumer regression=`118 passed`；
  architecture-fitness=`654 passed`，report-validation=`57 passed / 62 warnings`，
  reproducibility=`23 passed`，contract-validation=`275 passed`，
  integration=`995 passed / 642 warnings`。首次 natural-boundary Full 发现 7 个旧 consumer
  future-as-of fixture 回归；根因是 deterministic `checked_at` 约束误作用于未显式传参的调用。
  修正为只校验调用方显式 `checked_at` 后，目标回归=`70 passed`，带 parent-run 证明的 Full
  修复重跑=`7292 passed / 3 skipped / 643 warnings`，runtime artifact=
  `outputs/validation_runtime/full_20260726T022008Z/test_runtime_summary.json`。
  Phase A 转 `BASELINE_DONE`；Phase B 需另行选择第二个性质不同的 read-only consumer，
  不由本次验收自动启动。
- 2026-07-26：Project owner 要求继续按长期工程目标推进；Phase B 启动为两个原子阶段。
  B1 先冻结 generic dependency/discovery/verifier-only preflight contract，不迁 consumer；
  B2 候选冻结为 `TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC@1.0.0`。选择依据是其
  trailing-only、diagnostic-only、真实读取 canonical price cache 的消费闭包，与 Phase A
  future decision-target label 性质不同；B2 实现前仍须从 reviewed policy 与实际读取路径证明
  exact ticker/field/window closure，禁止把旧 global gate 中未被 evaluator 消费的 rates
  虚构为依赖。当前状态进入 `IN_PROGRESS_PHASE_B1_GENERIC_ADAPTER_CONTRACT`。
- 2026-07-26：B1 generic adapter 已实现 typed dependency、consumer/version/as-of scoped
  discovery pointer、content-addressed immutable receipt retention，以及 verifier-only sealed
  preflight。Consumer 只能在 pointer、receipt、policy、source、projection、full/scoped report
  和 strict `PASS` 全部重新验证后取得 preflight；missing、path escape、symlink/junction、
  SHA/size/id/as-of/consumer/policy drift、pointer regression 和 immutable collision 均 fail
  closed。Focused contract/tamper regression=`8 passed`；architecture-fitness=`664 passed`，
  contract-validation=`275 passed`，report-validation=`57 passed / 62 warnings`，
  reproducibility=`23 passed`，integration=`995 passed / 643 warnings`。当前进入
  required Full。首次 Full 的 `7320 passed / 3 skipped / 642 warnings` 后发现一项历史
  observability 断言被 B1 状态回填补丁误改；目标修复回归=`3 passed`，随后使用失败 summary
  作为 parent 的 Full 修复重跑=`7321 passed / 3 skipped / 642 warnings`，runtime artifact=
  `outputs/validation_runtime/full_20260726T080607Z/test_runtime_summary.json`。B1 转
  `BASELINE_DONE_PHASE_B1_GENERIC_ADAPTER_CONTRACT_B2_PENDING`；未迁移 runner，B2 仍须在
  独立原子变更中实施。
- 2026-07-26：B1 commit=`b8463faac3579f9b3084458f62a27d2a4f21b2b1` 已
  fast-forward 至 `main` 并推送 `origin/main`，任务分支清理完成。随后以该 exact base
  启动 B2 独立分支。对 `TRADING-2316` runner、policy 和 evaluator 的读取闭包审计确认：
  evaluator 只消费 QQQ、SMH、SPY 的 trailing `adj_close`；price projection 为运行
  same-code-path scoped DQ 仍须保留 date/ticker/OHLCV/source 等结构字段；rates、
  configured rate universe 和 optional marketstack 只属于旧 global DQ gate/披露，不进入
  feature 或 label 计算。因此 B2 reviewed capability 的 `required_rate_series` 必须为空，
  不得把旧 gate 的 rates 依赖带入 receipt。当前进入
  `IN_PROGRESS_PHASE_B2_REGIME_LABEL_PILOT`。
- 2026-07-26：B2 capability-only runner、tracked dependency/policy、verified materialized-input
  re-read 与 fail-closed tests 已实现；focused parallel regression=`16 passed`。真实 canonical
  build/verify 生成 receipt
  `dq_capability_b453834493d1951868c5474f379942461cce29c61b74fd37b9aab69167759ab3`，
  window=`2021-02-22..2026-07-24`，full DQ=`FAIL`、scoped DQ=`PASS`、
  `global_cache_pass_claimed=false`、isolated error=`prices_non_market_session_date`、
  unisolated errors=`[]`、required rates=`[]`。真实 TRADING-2316 run 只消费 receipt-bound
  QQQ/SMH/SPY bytes，生成 label/distribution/transition rows=`7416/30/123`，安全边界保持
  diagnostic-only、no reuse/daily/production/broker。当前转
  `VALIDATING_PHASE_B2_REGIME_LABEL_PILOT`；Phase C typed attribution、Phase D migration/
  inventory/expiry/revocation/cache reuse/dashboard 均未由 B2 自动启动。
- 2026-07-26：B2 首轮 Full 在运行期间因另一项已复核任务把 `main/origin/main` 从
  `b8463faac` 前移至 `281c8236b`，由 remote-carrier freshness tests 正确 FAIL；失败 run
  `outputs/validation_runtime/full_20260726T093310Z/test_runtime_summary.json` 保留。B2 任务增量
  以 task-scoped recovery stash
  `a57e13a0b579c7c2c4d405166fafafa4dd56f813` 暂存，任务分支随后 `--ff-only` 前移到
  `281c8236b`，从新基线恢复业务增量，并重新构建 append-only compatibility authority 与
  generated manifests。Recovery stash 只在新基线 Full、提交、main 集成和远端推送全部确认后
  删除。
- 2026-07-26：新基线正式门禁完成。Focused capability regression=`16 passed`，
  authority regression=`3 passed`，Black/Ruff/strict mypy PASS；architecture 首轮因
  formatter 后 test manifest stale 为 `669 passed / 1 failed`，直接重新生成 manifest 后复验
  `670 passed`。Contract/report/reproducibility/integration 分别为
  `275 passed`、`57 passed / 62 warnings`、`23 passed`、
  `995 passed / 642 warnings`。以旧 remote-drift 失败 summary 为 parent 的 Full 修复重跑为
  `7350 passed / 3 skipped / 643 warnings`，runtime artifact=
  `outputs/validation_runtime/full_20260726T102147Z/test_runtime_summary.json`。B2 转
  `BASELINE_DONE_PHASE_B2_REGIME_LABEL_PILOT_PHASE_C_PENDING`；下一长期切片优先准备
  Phase C typed issue attribution 与逐 issue source-owner review，Phase D consumer migration
  和 daily/periodic/production/broker 仍未自动授权。
- 2026-07-26：Phase C1 readiness inventory 已进入正式验证。AST scanner从canonical
  `quality.py`和`quality_execution.py`机械枚举69个emission sites，其中static/template/dynamic
  分别为56/11/2；只有`prices_non_market_session_date`对应的1个site已有reviewed authority，
  其余68个固定`OWNER_REVIEW_REQUIRED`。Tracked JSON/validation/Markdown由source/policy bytes
  content-derived重建，未新增allowed code、typed schema或consumer migration。Focused DQ
  regression=`104 passed`；正式architecture/contract/report/reproducibility/Full仍待本切片
  final-tree闭合。
- 2026-07-26：Phase C1正式门禁闭合并转`BASELINE_DONE`。Architecture首轮
  `668 passed / 6 failed`只捕获新增report后的冻结计数、documentation coverage、deprecation
  inventory和历史supersession ratchet漂移；逐项修复后architecture=`674 passed`。
  Contract/report/reproducibility/integration=`275/57/23/995 passed`，natural-boundary
  Full=`7376 passed / 3 skipped / 643 warnings`，artifact=
  `outputs/validation_runtime/full_20260726T130144Z/test_runtime_summary.json`。Phase C整体仍未
  完成；下一步是独立C2 source-owner review，不得把C1 inventory解释为typed attribution或
  新隔离授权。
- 2026-07-26：Phase C2 已建立 exact six-site rate source-owner review pack。C2 绑定 C1
  inventory、canonical `quality.py`、reviewed DQ policy 和 proposal bytes，区分4个
  single-row与2个current-plus-previous-observation move sites；所有decision仍为
  `PENDING_SOURCE_OWNER_DECISION`。本阶段不修改`DataQualityIssue`、capability
  policy/classifier或full/scoped DQ。只有逐site决定完成后才可另建C3 serial contract wave；
  window/row-level isolation、consumer migration、daily/periodic、production和broker仍未授权。
- 2026-07-26：Phase C2 工程基线正式闭合。Pack ID=
  `dq_rate_issue_attribution_review_0957cfde306cb37b760f1005`；focused DQ/current C2=
  `166/11 passed`，architecture/contract/report/reproducibility/integration=
  `676/275/57/23/995 passed`，唯一 natural-boundary Full=
  `7389 passed / 3 skipped / 643 warnings`，artifact=
  `outputs/validation_runtime/full_20260726T142413Z/test_runtime_summary.json`。Parent 转
  `BASELINE_DONE_PHASE_C2_REVIEW_PACK_SOURCE_OWNER_DECISION_PENDING`；下一责任人是 rate
  source owner，对 6 个 site 逐项 `APPROVE_FOR_CONTRACT_WAVE`、`REVISE` 或 `REJECT`。
  在决定齐备前不得建立 C3 runtime contract wave，也不得把工程建议解释为现行隔离权威。
- 2026-07-27：Phase C2P price review-pack工程基线已闭合并转`BASELINE_DONE`。
  唯一自然边界Full=`7578 passed / 3 skipped`，post-Full Architecture/Contract=
  `751/275 passed`；review pack现交price source owner对exact price site作
  `APPROVE_FOR_CONTRACT_WAVE`、`REVISE`或`REJECT`决定。Decision仍pending，
  不授权runtime contract、隔离或consumer migration。
  Exact scope仅为C1既有instrument-level pilot site
  `dq_issue_site_312625a26da21428b763 / prices_non_market_session_date`；pack显式区分现有
  `rows` distinct-date count、前10个sample dates和完整trigger source-row set，并提出
  ticker/rate/source/date/field/row六维规则。除C1/quality/DQ policy/proposal外，实际
  trigger predicate所依赖的calendar runtime、special-closure loader和reviewed registry亦
  逐byte绑定，避免calendar authority漂移。Pack ID=
  `dq_price_issue_attribution_review_dff1943fa21f6aeaf9f15714`；Ruff、strict mypy、
  focused与Architecture/Contract/Report/Reproducibility/Integration=
  `15/741/275/57/23/995 passed`。首轮Architecture的`725 passed / 16 failed`准确暴露
  reporting/deprecation ratchet、generated freshness与compatibility authority漂移，直接修正后
  闭合；未降级或绕过门禁。Source-owner decision仍pending，C3/runtime/schema/isolation/
  consumer migration均未授权。
- 2026-07-27：price source owner对C2P exact site决定
  `APPROVE_FOR_CONTRACT_WAVE`，C3P最小serial contract wave进入验证。新增decision
  authority绑定原review pack exact SHA、`primary_market_prices`、reviewed XNYS calendar
  function AST与special-closure policy；typed issue contract记录exact source
  artifact/checksum、requested window、ticker/rate/source/date/field/row六维scope及全部
  trigger row identities。`canonical_row_digest.v1`字段、type-tagged normalization和
  snapshot-local ordinal语义已冻结。Secondary/unapproved source或任一归因维度不完整时清空
  legacy affected scope并保持`GLOBAL_OR_UNKNOWN_SCOPE`。C3P未修改capability policy YAML、
  classifier、receipt schema或consumer；任何dimensional adoption仍须另行owner review。
- 2026-07-28：Owner 以 rate source-owner 身份批准 current C2 exact pack
  `dq_rate_issue_attribution_review_b44f93b62baac6d1022bc698` 的六个 site 全部进入最小
  C3 serial typed contract wave。Decision=
  `owner_decision:DATA-GOV-002C3:2026-07-28:approve_rate_row_issue_attribution_contract_wave_v1`。
  C3仅实现 checksum-bound rate source/series/date/field/row/threshold/predecessor contract
  与false-isolation evidence；active capability policy adoption、consumer migration、Phase D、
  daily/periodic、production和broker仍未授权。
- 2026-07-28：C3 rate typed contract正式闭合并转`BASELINE_DONE`。六个exact rate
  site保留canonical constructor identity，typed scope绑定primary source checksum、
  series/date/field/row、versioned digest、实际threshold及move predecessor；任何缺项继续
  `GLOBAL_OR_UNKNOWN_SCOPE`。Architecture/Contract/Report/Reproducibility/Integration=
  `775/276/57/24/995 passed`，parent-bound Full=`7629 passed / 3 skipped`。随后真实
  canonical `aits validate-data`（as-of=`2026-07-27`）仍为strict `FAIL`：manifest/price
  window停在`2026-07-24`、primary `^VIX`保留31个non-XNYS rows、26个ticker缺
  `2026-07-27`。因此Phase C contract完成不等于global DQ恢复；QLD自动选择、capability
  adoption、Phase D及生产治理继续等待独立canonical strict PASS和新Owner评审。
- 2026-07-29：在后续 D0B2B/OPS-067 operational closeout 已进入
  `main=b646fc9ae169f266a6b93fda572af20ebdfcffe8` 后重对齐 parent 状态。Canonical
  as-of=`2026-07-27` strict DQ 已由 receipt
  `dq_execution_28af63a1e747ba675e17d3001d8028592b6ec0ef63e823bcfa9463889b0cb5c4`
  证明为 `PASS`（errors/warnings=`0/0`）；这取代上一条在 C3 closeout 当时记录的
  `FAIL` 作为当前 operational fact，但不改写该历史失败证据。Price C3P 与 rate C3
  contract 均保持 `BASELINE_DONE`，本次只对齐 parent/task/append-only governance
  authority并展开下一次 consumer-specific adoption review 决策上下文；未修改 runtime、
  capability policy、classifier、consumer、cached data 或 system flow，
  `production_effect=none`、`broker_action=none`。短生命周期隔离 worktree=
  `D:\Work\AITradingSystem_worktrees\data-gov-002-phase-c-parent-reconciliation-20260729`；
  退出条件为验证、提交、local-main/remote-main安全同步、唯一内容审计和租约释放完成，
  届时移除该 worktree 与任务分支。该 worktree 的 task-registry generator 需要 tracked
  bootstrap handoff 引用的 4 份 ignored historical validation runtime summaries；从 canonical
  root 按 tracked path 只复制 exact bytes，并在复制前后校验 tracked SHA-256，不修改证据内容、
  handoff 或验证结论。这 4 份副本只服务隔离生成/验证，随 worktree 一并删除，canonical root
  证据保留。
