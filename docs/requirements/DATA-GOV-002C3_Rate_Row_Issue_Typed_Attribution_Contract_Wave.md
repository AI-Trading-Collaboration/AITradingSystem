# DATA-GOV-002C3：Rate Row Issue Typed Attribution Contract Wave

最后更新：2026-07-28

稳定任务 ID：
`DATA-GOV-002C3_RATE_ROW_ISSUE_TYPED_ATTRIBUTION_CONTRACT_WAVE`

Owner 决定：
`owner_decision:DATA-GOV-002C3:2026-07-28:approve_rate_row_issue_attribution_contract_wave_v1`

状态：`BASELINE_DONE`

## 1. Owner 决策与 exact authority

Project owner 以 `data_platform_rate_quality_owner` 身份批准当前 C2 exact bundle 的六个
site 全部进入最小 serial typed contract wave：

- review pack id：
  `dq_rate_issue_attribution_review_b44f93b62baac6d1022bc698`；
- review pack path：
  `inputs/data_quality/rate_issue_attribution_review_pack_v1.json`；
- review pack SHA-256：
  `f2294ea5651259857c500c2161a8dbb3e78879c0132fe58e36bd4b24cea5b318`；
- canonical DQ source SHA-256：
  `5d3221f06c3d3523dc1e62a17fc12a9295503986b4d84f6b227e739d3f957e5e`。

逐 site 决定均为 `APPROVE_FOR_CONTRACT_WAVE`：

|Site ID|Issue code|Owner conditions|
|---|---|---|
|`dq_issue_site_0e7f3d74bfa489801c83`|`rates_invalid_date`|只允许 series-level attribution；永不参与 window-level isolation。|
|`dq_issue_site_f337897b3d0d0b8e2842`|`rates_invalid_value`|日期不可解析时降级为 series-level；归因不完整保持 global。|
|`dq_issue_site_dcc6dcab7a17c225b404`|`rates_non_finite_value`|日期不可解析时降级为 series-level；归因不完整保持 global。|
|`dq_issue_site_6421117ee905a6da1438`|`rates_out_of_range`|evidence 必须保存实际命中的 series-specific threshold。|
|`dq_issue_site_85549de0f1e9ab739a74`|`rates_extreme_daily_change`|同时绑定 trigger row、previous valid row 与实际命中 threshold。|
|`dq_issue_site_df1c184d09e3c55d3e71`|`rates_suspicious_daily_change`|使用完整 row-pair attribution；`WARNING` 不扩大隔离权威。|

共同条件：

- 只适用于 checksum-bound `primary_macro_rates`；
- source ordinal 只在 exact source snapshot 内有意义；
- row identity 必须使用 versioned、content-derived digest；
- normalized rate series 必须 non-empty；
- 任一要求不完整时固定 `GLOBAL_OR_UNKNOWN_SCOPE`；
- 本决定只批准 typed runtime contract 与 false-isolation contract evidence；
- capability policy adoption、active classifier allowlist、consumer migration、daily/periodic、
  production 和 broker 均未授权。

## 2. 问题与目标

C3P 已为一个 price issue 建立 source/date/field/row typed attribution，但当前 contract 仍
把 row 类型、calendar 和 price-only 不变量写死。六个 rate issue 继续只输出 message/sample，
不能形成可机器验证的 rate series、source、date、field、row 或 predecessor scope。

本任务的目标是：

1. 以 exact Owner decision artifact 绑定 C2 pack 和六个 site；
2. 在不破坏 C3P price contract 的前提下，把共享 attribution schema 扩展到 rate rows；
3. 为 single-row 和 row-pair issue 生成 checksum-bound typed attribution；
4. 对实际命中的 range/move threshold 和 predecessor identity 留下结构化证据；
5. 不完整、未批准 source 或 authority drift 时清空 legacy affected scope并保持 global；
6. 证明保守的
   `ALL_AFFECTED_RATE_SERIES_OUTSIDE_REQUIRED_SCOPE` 判定不会把 unknown/incomplete issue
   误隔离，但不把任何 rate issue 加入 active capability policy。

## 3. 分阶段实施

### S0：Decision authority 与任务登记

- 建立 reviewed decision artifact，绑定 exact C2 pack id/path/SHA；
- 记录六项逐 site 决定和共同 conditions；
- 更新 C2、parent requirement 和 task register；
- 运行 governed `SINGLE_LANE` preflight 后才进入 runtime 实现。

### S1：Typed rate attribution contract

- 定义 `primary_macro_rates` source role；
- 定义 versioned rate row digest，字段固定为 `date/series/value`，显式编码 null、invalid
  lexical value、finite 和 non-finite 值；
- single-row 记录 exact trigger row；
- move issue 同时记录 trigger 与 previous valid same-series row；
- requested window、source checksum、source-local ordinal、series/date/field、threshold
  authority与decision bytes全部可审计；
- C3P price serialization 与 validator 保持兼容。

### S2：Canonical runtime emission

- `_validate_rates` 为 exact source snapshot 建立 source ordinal；
- 六个 approved emitters 只在 decision、source role、checksum、window 和 row identity
  完整时填充 typed attribution；
- blank series、missing window、missing predecessor、unapproved source、digest/threshold
  authority drift 或其他 contract error 均 fail closed 为
  `GLOBAL_OR_UNKNOWN_SCOPE`；
- message、sample 与展示文本不得成为 scope authority。

### S3：False-isolation contract 与报告

- 提供纯 contract-level disjointness 判定并验证：
  affected rate series 完整、非空且与 required rate series 完全不相交时才返回 eligible；
- 不修改 active capability policy allowlist，不迁移任何 consumer；
- JSON/Markdown DQ 报告展示 decision、source、rate series、date/field/row、
  threshold/predecessor 和 incomplete reason；
- 更新 system flow、artifact/report registry及生成治理视图。

### S4：验证与收口

- focused rate/price contract、tamper、incomplete、false-isolation tests；
- Black、Ruff、strict mypy；
- Architecture、Contract、Report、Reproducibility、Integration；
- natural-boundary Full；
- validated task commit、local-main fast-forward、ordinary remote push、SHA复核和任务分支清理。

## 4. 验收标准

- decision loader 拒绝 wrong pack id/path/SHA、wrong site/code/source、缺项、重复项和
  condition drift；
- 六项 runtime issue 的 typed scope 与 exact trigger rows一致；
- `rates_invalid_date` 永不形成 window-level isolation authority；
- invalid value/non-finite 的日期不可用时最多保留完整 series-level authority；
- range evidence 绑定实际 min/max threshold；
- move evidence 绑定 trigger、previous valid row、change、suspicious/extreme threshold；
- `WARNING` 不改变 eligibility 规则；
- incomplete attribution 始终保持 global，legacy affected scope为空；
- price C3P contract和既有 full/scoped DQ行为无非预期回归；
- capability policy、consumer、cached data、strategy、weights、production和broker不变；
- `production_effect=none`、`broker_action=none`。

## 5. 开放问题与明确非范围

- C3 完成后是否把某个 rate issue code 加入某个 consumer capability policy，必须另开
  adoption task并重新做 false-isolation review；
- `rates_future_dates`、`rates_stale`、`rates_empty`、missing/duplicate/expected-series 和
  其他未批准 issue 继续 `GLOBAL_OR_UNKNOWN_SCOPE`；
- window-level 或 row-level partial consumer isolation 未授权；
- Phase D inventory/expiry/revocation/cache reuse/dashboard 未授权；
- canonical full-cache strict PASS 仍须由真实 `aits validate-data` 独立证明。

## 6. 生命周期

- governed mode：`SINGLE_LANE`；
- planned branch：`codex/data-gov-002c3-rate-attribution-contract`；
- 不创建额外 worktree、clone、stash 或 cache；
- 不读取、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- closeout 前按 governed audit 检查 task-owned content、validation evidence 和恢复边界。

## 7. 进度

- 2026-07-28：Owner 批准 exact C2 rate bundle；S0 登记开始。C3 runtime/schema、
  classifier contract和consumer仍未在 preflight 前修改。
- 2026-07-28：S1/S2 首轮实现后，C1 scanner 曾因 helper factory 取代 canonical
  `DataQualityIssue` constructor而把site总数从69降至64，C2 freshness正确fail closed。
  未接受该site identity漂移；实现改为保留六个canonical constructor并只委托typed builder，
  重建后site总数恢复69、六个approved site id/code/taxonomy全部不变。Final-tree derived C2
  pack=`dq_rate_issue_attribution_review_216045a1ebe282194028e1f8`、SHA-256=
  `1c50337d9443fdea422373c2a275cfa87265d753140be0d66d5db44488752abf`。
  Decision artifact由coordinator绑定该fresh pack；Owner原批准的pre-contract pack id/SHA和
  六项conditions继续保留在§1，未扩大site、scope或policy authority。
- 2026-07-28：S1-S3 focused rate/price/DQ=`74 passed`；Black、Ruff、strict mypy PASS。
  C1/C2 content-derived validators均PASS，inventory保持69 sites，final fresh C2 pack=
  `dq_rate_issue_attribution_review_216045a1ebe282194028e1f8`。六项runtime complete/unknown
  分支、non-finite digest、threshold/predecessor evidence、decision tamper、pure
  series-disjointness、price compatibility、Markdown和active-policy no-adoption均有覆盖。
  当前进入generated governance与formal validation。
- 2026-07-28：S4 正式门禁闭合。Architecture / Contract / Report / Reproducibility /
  Integration 分别为 `775 / 276 / 57 / 24 / 995 passed`。首轮 Full 为
  `7627 passed / 2 failed / 3 skipped`，失败只出现在
  `external_request_cache_revalidation_coordination` 的两个 Windows 双进程同-key时序样本；
  未修改无关实现、未改用serial，定向并行复现 `20 passed`。随后以首轮失败artifact为
  parent运行failure-fix Full，结果为 `7629 passed / 3 skipped / 643 warnings`，
  runtime artifact=`outputs/validation_runtime/full_20260728T151034Z/test_runtime_summary.json`。
  Rate contract wave转`BASELINE_DONE`；capability policy、consumer、production与broker仍未
  授权。
- 2026-07-28：按operations runbook运行真实无显式日期的canonical
  `aits validate-data`，provider-ready as-of=`2026-07-27`，结果仍为strict `FAIL`
  （3 errors、0 warnings）。Receipt=
  `dq_execution_6a6d64e52368d511db4237ca9fb2d7e370cb51013624732b96255b01e832eeef`；
  blockers为manifest requested-window末端仍是`2026-07-24`、primary `^VIX`
  31个non-XNYS-session rows、以及26个ticker缺`2026-07-27`覆盖。该结果不否定本任务的
  typed contract闭合，但继续阻断QLD自动选择、capability adoption和生产治理复审。
