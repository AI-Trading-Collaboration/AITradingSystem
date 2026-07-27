# DATA-GOV-002C2P：Price Non-Market-Session Attribution Source-Owner Review Pack

最后更新：2026-07-27

稳定任务 ID：
`DATA-GOV-002C2P_PRICE_NON_MARKET_SESSION_ATTRIBUTION_SOURCE_OWNER_REVIEW_PACK`

上位 Owner 决定：
`owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1`

Owner continuation：
`owner_continuation:DATA-GOV-002C2P:2026-07-27:continue_long_term_engineering_goal`

状态：`BASELINE_DONE`

## 1. 背景与问题

DATA-GOV-002 Phase A/B 已允许
`prices_non_market_session_date` 在具有 non-empty
`affected_instruments` 且与 consumer required instruments 完全不相交时，按 reviewed
pilot policy 形成 capability-scoped 隔离。DATA-GOV-002C1 随后证明该 exact site
`dq_issue_site_312625a26da21428b763`仍缺少 Phase C 要求的完整六维 source-owner
review：

- `affected_price_tickers`；
- `affected_rate_series`；
- `affected_source_roles`；
- affected date/window；
- `affected_fields`；
- affected row identity。

现有授权只覆盖 instrument-level rule，不授权把 source、date、field 或 row proposal
写入 `DataQualityIssue`、capability classifier 或新的隔离规则。直接进入 runtime
contract wave 会把工程推断误当作 source-owner 决定。

## 2. 本切片目标

建立 deterministic、content-derived、非授权性的单-site review pack：

1. exact scope 只包含
   `dq_issue_site_312625a26da21428b763 / prices_non_market_session_date /
   _check_price_market_calendar_dates`；
2. 绑定 C1 inventory、canonical `quality.py`、reviewed `config/data_quality.yaml`、
   US equity calendar runtime、special-closure loader/registry 和 proposal manifest 的
   exact bytes/SHA-256；
3. 明确该 issue 是由 requested window 内所有 non-session dates 及其全部触发行共同形成，
   `rows` 当前表示 distinct date count，不能误当成 source row count；
4. 给出 price tickers、empty rate scope、source role、exact trigger dates、fields 和 row
   identity 的候选规则及 attribution incomplete 条件；
5. 输出 price source owner 的 `APPROVE_FOR_CONTRACT_WAVE`、`REVISE` 或 `REJECT`
   决策槽，但保持 `PENDING_SOURCE_OWNER_DECISION`；
6. validator 从全部绑定来源重建并拒绝 site、function、proposal、policy 或输出漂移；
7. 为后续独立 serial contract wave 提供 reviewed 输入，不在本任务内改变 runtime。

## 3. Exact review scope

只包含：

- site id=`dq_issue_site_312625a26da21428b763`；
- issue code=`prices_non_market_session_date`；
- emitter function=`_check_price_market_calendar_dates`；
- proposed taxonomy=`DISTINCT_NON_SESSION_DATE_ROW_SET`；
- proposed canonical source role=`primary_market_prices`；
- proposed price-ticker rule=
  `DISTINCT_NORMALIZED_NON_EMPTY_TICKERS_FROM_ALL_TRIGGER_ROWS`；
- proposed rate scope=`[]`；
- proposed date rule=`DISTINCT_NON_SESSION_DATES_WITHIN_REQUESTED_WINDOW`；
- proposed affected fields=`date`；
- proposed row rule=
  `ALL_TRIGGER_ROWS_WITH_SOURCE_ORDINAL_AND_CANONICAL_ROW_DIGEST`。

所有其他 price、rate、publication、manifest、freshness 和 completeness issue 均不在本
切片。C2 的六个 rate decisions 继续 pending，不由 C2P 代签或合并。

## 4. Review contract

Proposal status 固定为 `PROPOSED_FOR_SOURCE_OWNER_REVIEW`。候选必须披露：

- exact site/code/function 与 caller-supplied severity expression；
- trigger predicate 与 distinct-date aggregation；
- source role、price/rate domain assertion；
- defect、identity、derived fields；
- trigger-date set、requested-window dependency 与 source-row dependency；
- attribution completeness requirements 和 fail-closed `incomplete_when`；
- false-isolation risks；
- required runtime contract tests；
- source-owner questions 与 pending decision slot。

Review pack 不是批准记录。即使工程建议为 `CONTRACT_WAVE_CANDIDATE`，以下字段仍必须
保持 false：

- `review_pack_is_authorization`；
- `source_owner_decision_recorded`；
- `runtime_contract_change_authorized`；
- `new_issue_isolation_authorized`；
- `window_or_row_level_isolation_authorized`；
- `capability_policy_change_authorized`；
- `consumer_migration_authorized`。

## 5. 实施步骤

1. 新增单-site proposal manifest；
2. 实现 builder、content-derived validator、中文 Markdown projector 与独立 CLI；
3. 增加 exact-site、function AST、instrument/date/row completeness、proposal/source/policy/
   output tamper 和路径 containment 测试；
4. 生成 tracked JSON、validation JSON 与中文 review pack；
5. 更新 parent requirement、task register、system flow、artifact/report registry；
6. 刷新 DevEx/task generated views与 append-only compatibility authority；
7. 运行 focused、Ruff、strict mypy、Architecture、Contract、Report、Reproducibility、
   Integration 和自然边界 Full；
8. 验证后提交任务分支、fast-forward local `main`、普通推送并清理任务分支。

## 6. 验收标准

- pack 恰好覆盖一个 exact C1 site，site/code/function 与 source/inventory一致；
- `rows` distinct-date count 与 affected source-row identity 明确区分；
- price tickers 必须来自全部 trigger rows，且 blank/missing ticker 使 attribution
  incomplete；
- affected rate series 必须为空，source role 必须是 proposal 中的 exact price role；
- exact trigger dates 必须可解析、非空并位于 requested window 内；不得把 message/sample
  解析成 scope；
- row identity 必须包含 source ordinal 与 canonical row digest；缺失时不得隔离；
- proposal、C1 inventory、canonical source、DQ policy 或输出漂移均由 validator拒绝；
- `is_us_equity_trading_day`实现及special-closure loader/registry漂移均由validator拒绝，
  防止只绑定emitter却遗漏实际calendar authority；
- decision slot保持 pending，所有 runtime/isolation/migration authorization 为 false；
- 不修改 `DataQualityIssue`、`validate_data_cache`、capability policy/classifier、cached
  data、consumer runner、full/scoped DQ 或策略结论；
- `production_effect=none`、`broker_action=none`。

## 7. 生命周期与安全边界

- 模式：`SINGLE_LANE`；
- 计划分支：`codex/data-gov-002c2p-price-calendar-review-pack`；
- frozen base：`00d98ddaa2828852c1086ea9176935643e11e205`（governed START preflight
  `PASS`）；
- 不创建额外 worktree、clone、stash 或 cache；
- 不读取、hash、复制、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- 本任务完成条件是 review pack 可供 price source owner 决策，不是 source owner 已批准；
- 只有显式 source-owner decision 完成后，才能另建最小 serial runtime contract wave；
- `production_effect=none`、`broker_action=none`。

## 8. 进度

- 2026-07-27：最新五次自然 Full 的 Smoothed Weekly 热点仍未形成符合
  ARCH-004G2 退出门槛的有界优化候选；工程主线回到 DATA-GOV-002 Phase C。
  只读审计确认 price site 已有 instrument-level pilot authority，但 C1 明确标记
  `phase_c_migration_eligible=false`，原因是六维 source-owner review 未完成。建立 C2P
  非授权 review-pack 任务并进入 `IN_PROGRESS`；runtime DQ contract保持冻结。
- 2026-07-27：实现完成并转`VALIDATING`。新增单-site proposal、content-derived
  builder/validator、独立CLI、tracked JSON/validation/中文Markdown和15项focused tests。
  实施审计发现trigger predicate实际依赖`trading_calendar.py`与special-closure registry，
  因此除原计划的C1/quality/DQ policy/proposal外，pack额外绑定calendar runtime、
  special-closure loader和reviewed registry，关键emitter/calendar function均绑定AST hash。
  Pack ID=`dq_price_issue_attribution_review_dff1943fa21f6aeaf9f15714`；首轮Ruff、
  strict mypy和focused parallel pytest=`15 passed`。所有decision/runtime/schema/isolation/
  consumer authorization仍为false/pending，正在执行generated governance与formal gates。
- 2026-07-27：generated governance 与 pre-Full formal gates 闭合，任务保持`VALIDATING`
  直至自然边界 Full 完成。首轮 Architecture 准确暴露新增 report 对 reporting/
  deprecation ratchet、generated manifest freshness 与 append-only compatibility authority
  的影响，保留`725 passed / 16 failed`证据：
  `outputs/validation_runtime/architecture-fitness_20260727T011331Z/test_runtime_summary.json`。
  未绕过门禁；更新精确 report/module/test inventory、deprecation frozen inventory，并让
  历史 compatibility tests 显式承认最新 C2P current-hash authority。最终
  Architecture/Contract/Report/Reproducibility/Integration=
  `741/275/57/23/995 passed`。一次 Contract 外层命令在 124 秒处超时且无残留进程，
  同一范围以充足外层时限重跑为`275 passed`，不是测试失败。自然边界 Full 与
  post-Full Architecture/Contract 将基于最终治理状态执行；所有 source-owner
  decision 与 runtime/schema/isolation/consumer migration authority 继续保持 pending/false。
- 2026-07-27：恢复工程线后，installed governed `SINGLE_LANE/coordinator/LANE`
  preflight 在 frozen base=`00d98ddaa2828852c1086ea9176935643e11e205`、latest
  main=`8d1effbd77b34e8e7dc6a95a751562b46746c3d7`上 PASS，并明确
  `BASE_DRIFT_DEFERRED_TO_INTEGRATION_PLAN`；无 active lease、无未归属路径。重新执行
  focused parallel pytest=`15 passed`、Ruff、strict mypy及tracked pack content-derived
  check均PASS，pack ID仍为`dq_price_issue_attribution_review_dff1943fa21f6aeaf9f15714`。
  本 lane 只形成可恢复的task commit，不在旧基线重复运行Full。只读drift evidence将写入
  `outputs/integration_revalidation/data_gov_002c2p_20260727/change_manifest.json`与
  `integration_revalidation_plan.json`；owner为本任务，purpose为分类frozen lane与latest
  main真实Git delta，exit condition为plan重建验证PASS、唯一latest-main candidate完成
  final-tree gates并进入local/remote main。两份文件是可重建的忽略态审计证据，不授权
  merge/rebase/cherry-pick/push或策略、production、broker行为。
- 2026-07-27：frozen lane task commit
  `5245821eba0c010c1ae6567d03a5c15e49b91b84`完成；只读 drift plan
  `integration-revalidation-9808139c73d5d06e411b`独立验证为
  `RECONCILIATION_REQUIRED`，0 blockers、0 contract conflicts、0 undeclared task
  paths。Coordinator从latest main=`8d1effbd77b34e8e7dc6a95a751562b46746c3d7`
  建立唯一candidate，只协调plan列出的5个domain overlaps，并在最终树一次性刷新
  DevEx、task shadow、deprecation inventory和append-only compatibility authority；
  line-number派生刷新额外更新OPS-070 active fragment `98/...yaml`，未改变其任务内容。
  初次集成focused准确暴露12项历史supersession断言缺少C2P后继authority，
  结果为`135 passed / 12 failed`；直接补齐append-only历史断言后同范围
  `147 passed`，未绕过或串行替代。
- 2026-07-27：最终工程基线闭合并转`BASELINE_DONE`。Ruff、strict mypy、pack
  content-derived check均PASS；Architecture/Contract/Report/Reproducibility/
  Integration=`751/275/57/23/995 passed`；唯一自然边界Full=
  `7578 passed / 3 skipped / 642 warnings`，证据位于
  `outputs/validation_runtime/full_20260727T092214Z/test_runtime_summary.json`；
  final metadata刷新后的post-Full Architecture/Contract=
  `751/275 passed`。Review pack已满足本任务完成条件，但
  `PENDING_SOURCE_OWNER_DECISION`及所有runtime/schema/isolation/consumer
  authorization仍保持pending/false；下一责任方为price source owner，任何runtime
  contract wave必须等待其显式决定并另建serial任务。
