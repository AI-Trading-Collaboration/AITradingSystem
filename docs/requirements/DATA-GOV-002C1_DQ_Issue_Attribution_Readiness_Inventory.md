# DATA-GOV-002C1：DQ Issue Attribution Readiness Inventory

最后更新：2026-07-26

稳定任务 ID：`DATA-GOV-002C1_DQ_ISSUE_ATTRIBUTION_READINESS_INVENTORY`

上位 Owner 决定：
`owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1`

Owner continuation：
`owner_continuation:DATA-GOV-002C1:2026-07-26:continue_long_term_engineering_goal`

状态：`BASELINE_DONE`

## 1. 背景

DATA-GOV-002 Phase A/B 已证明 consumer capability receipt 可以在保留 full canonical
DQ=`FAIL` 披露的同时，让 exact frozen scope 通过 same-code-path strict validation。
当前唯一经过 Owner review、允许结构化隔离的 issue 是
`prices_non_market_session_date`，且必须具有 non-empty
`affected_instruments`。

Phase C 的目标是把 ticker、rate series、source role、window、field 和 row scope
升级为 typed attribution；但其前置条件是逐 issue code 的 source-owner review。当前
canonical emitter 同时包含：

- 静态 code；
- 由 `label`、`role`、`column` 等变量形成的模板 code；
- 从 manifest/publication 等下游结果传入的动态 code；
- 只有 message/sample/source/rows、没有 typed attribution 的 legacy evidence。

因此不能从 message/sample 文本猜 scope，也不能因为代码名称包含 `prices` 或 `rates`
就自动批准隔离。

## 2. 本切片目标

建立 deterministic、content-derived 的 Phase C readiness inventory，使 source owner
可以逐 emission site 审查，而不改变任何当前 DQ 或 capability 行为：

1. 机械枚举 canonical DQ emitter source 与 emission site；
2. 区分 static literal、template 和 dynamic code expression；
3. 记录 emitter module/function、source expression、现有 rows/sample 和 typed field；
4. 明确当前 policy-authorized issue code，其他项默认
   `GLOBAL_OR_UNKNOWN_SCOPE`；
5. 生成 source-owner review queue、summary 和 Markdown reader brief；
6. validator 从 source bytes 和 policy bytes 重建 inventory，拒绝遗漏、篡改和漂移；
7. 为下一切片选择首批 review candidate，但本切片不批准任何新 issue 隔离。

## 3. 权威与扫描边界

### 3.1 Canonical emitter scope

首版只扫描直接参与 canonical `DataQualityReport` 的 emitter：

- `src/ai_trading_system/data/quality.py` 中的 `DataQualityIssue(...)`；
- `src/ai_trading_system/data/quality_execution.py` 中向 canonical report 追加 issue 的
  `_provenance_issue(...)` 调用。

其他模块中用于 research/scoring/local diagnostic 的 `DataQualityIssue` 不自动进入
canonical capability attribution scope；inventory 必须披露 excluded constructor count，
防止误把“未扫描”解释为“不存在”。

### 3.2 Existing policy authority

当前 policy-authorized issue code 只能从已 reviewed
`data_quality_consumer_capability_policy.v1` 文件机械汇总。首版预期仅为：

- `prices_non_market_session_date`。

Inventory 不得新增、推导或修改 allowed code。不同 reviewed policy 对同一 code 的
attribution rule 不一致时，validator 必须 fail closed。

### 3.3 Site identity

每个 site 使用 source-relative path、enclosing function、emitter kind、normalized code
expression 和 occurrence 形成 content-derived stable id。Line number 只作导航信息，不作为
唯一 identity。

## 4. 输出合同

主 JSON 使用 `data_quality_issue_attribution_readiness_inventory.v1`，至少包含：

- source/policy bindings 与 SHA-256；
- canonical/excluded source scope；
- total/static/template/dynamic site counts；
- exact static code count；
- policy-authorized code set；
- typed attribution present/missing counts；
- 每个 site 的 identity、path、function、line、code kind/expression、severity/source
  expression、rows/sample/affected-instruments presence；
- `scope_status`；
- `owner_review_status`；
- `isolation_eligible=false`，除已 reviewed exact code 的现状披露外不得推导新权限；
- missing-dimension list；
- production/broker/safety fields。

Markdown 只投影 JSON，不重算或改变结论。Validation report 必须列出 deterministic rebuild、
source/policy binding、site completeness、unique identity、status/safety 和 tamper 结果。

## 5. 实施步骤

1. 实现 AST-based canonical emitter scanner 与 stable site identity；
2. 从 reviewed capability policy 汇总当前 allowed code/rule；
3. 实现 JSON/Markdown builder 和 content-derived validator；
4. 生成 tracked source-owner review baseline；
5. 增加 static/template/dynamic、factory、omission、source/policy/output tamper tests；
6. 更新 report registry、artifact catalog、system flow 和 generated architecture/task views；
7. 运行 focused、architecture、contract、report、reproducibility 和适用 Full；
8. 任务分支验证后 fast-forward local main，执行 governed remote closeout 和普通 push。

## 6. 验收标准

- declared canonical emitter source 的每个目标 call site 恰好进入 inventory；
- excluded `DataQualityIssue` constructor 数量可见且不会被纳入 capability 权威；
- static/template/dynamic code expression 分类确定性；
- source/policy/site/output 任一漂移或删除均由 validator 拒绝；
- 未 reviewed site 全部保持 `GLOBAL_OR_UNKNOWN_SCOPE` 和
  `OWNER_REVIEW_REQUIRED`；
- inventory 不修改 `DataQualityIssue`、`validate_data_cache`、capability policy/receipt、
  consumer runner、cached data 或任何 full/scoped DQ 结果；
- 不解析 message/sample 推导 scope；
- `production_effect=none`、`broker_action=none`；
- 下一迁移切片只能使用 inventory 中经 source owner 明确 review 的 exact site/code。

## 7. 安全与非目标

- 不新增 typed attribution schema 字段；该变更属于后续 reviewed contract wave；
- 不迁移任何新 issue code，不扩大 `allowed_global_error_codes`；
- 不迁移 daily/periodic consumer，不启动 Phase D；
- 不改变 strategy、feature、label、threshold、model、weight、backtest 或投资结论；
- 不读取、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- 本任务不创建额外 worktree、clone、stash 或 cache。

## 8. 进度

- 2026-07-26：DEVX-003 收口后回到 DATA-GOV-002 Phase C 主线。只读审计确认
  canonical emitter 同时存在 static/template/dynamic code；除
  `prices_non_market_session_date` 外，现有 site 未形成 Phase C typed attribution
  authority。建立 C1 readiness inventory，状态转 `IN_PROGRESS`。
- 2026-07-26：Builder、validator、CLI、tracked JSON/validation/Markdown和治理接线已实现。
  Baseline inventory id=`dq_issue_attribution_inventory_03821e2e3dc483446249d52c`；
  canonical sites=`69`、static/template/dynamic=`56/11/2`、policy-authorized sites=`1`、
  owner-review-required sites=`68`、noncanonical constructor count=`2`。完整source/policy
  rebuild与输出check为`PASS/0 errors`，focused DQ regression=`104 passed`。全仓AST性能问题
  已在不改变计数/identity的前提下修正为只对包含`DataQualityIssue`标记的文件parse，
  单文件focused由`166.96s`降至`5.23s`。当前状态转`VALIDATING`，下一步闭合append-only
  compatibility authority和正式validation tiers。
- 2026-07-26：正式验证闭合。Architecture首轮=`668 passed / 6 failed`，全部为新增report和
  append-only authority对应的预期ratchet drift；修复后architecture=`674 passed`。
  Contract/report/reproducibility/integration=`275/57/23/995 passed`；唯一natural-boundary
  Full=`7376 passed / 3 skipped / 643 warnings`，provenance task/boundary均绑定本C1。
  状态转`BASELINE_DONE`，没有第二次Full。

## 9. 下一批 owner-review candidate

基于最终inventory，首批建议选择“rate row-level value/move family”，而不是先处理
source-wide staleness、window或manifest问题。候选固定为以下6个exact sites：

- `dq_issue_site_0e7f3d74bfa489801c83`：`rates_invalid_date`；
- `dq_issue_site_f337897b3d0d0b8e2842`：`rates_invalid_value`；
- `dq_issue_site_dcc6dcab7a17c225b404`：`rates_non_finite_value`；
- `dq_issue_site_6421117ee905a6da1438`：`rates_out_of_range`；
- `dq_issue_site_85549de0f1e9ab739a74`：`rates_extreme_daily_change`；
- `dq_issue_site_df1c184d09e3c55d3e71`：`rates_suspicious_daily_change`。

选择理由：

- 六项均为static code，避免首批同时引入template/dynamic code identity问题；
- 六项已有rows/sample evidence，可审查`affected_rate_series`、`affected_fields`和
  `affected_rows`，适合作为首个非ticker typed-attribution合同；
- 对price-only consumer有明确长期unblock价值，但只有在source owner逐site确认并完成独立
  contract wave、completeness/tamper验证后才能形成隔离权威；
- `rates_future_dates`、`rates_stale`和`rates_empty`暂留后批，因为其语义涉及window或
  source-wide影响，不能与row-level family混合批准。

本选择只是C2 review queue建议，所有6项仍保持`GLOBAL_OR_UNKNOWN_SCOPE`、
`OWNER_REVIEW_REQUIRED`和`phase_c_migration_eligible=false`。
