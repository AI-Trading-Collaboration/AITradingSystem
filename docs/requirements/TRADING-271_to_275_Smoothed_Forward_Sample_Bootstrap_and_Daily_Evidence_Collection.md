# TRADING-271 to 275 Smoothed Forward Sample Bootstrap and Daily Evidence Collection

最后更新：2026-07-14

## 状态

BASELINE_DONE（ARCH-004G2.4CS=`COMPLETE_G2_4_CONTINUES`；等待真实 forward/PIT/DQ/cost/holdout 证据）

## 背景

TRADING-266 to 270 已经建立 smoothed forward progress、weekly dashboard、event
monitor、switch readiness recheck 和 owner renewal pack，但这些 artifact 只汇总已有
forward evidence，不负责生成新的 forward observation samples。当前
`smooth_3d_vs_limited`、`smooth_3d_sideways_choppy_improvement` 和
`smooth_3d_recovery_lag_watch` 的样本仍为 0，原因是系统尚未建立 daily target
emission、outcome due scan、真实 forward outcome update 和 sideways / recovery
classification 的闭环。

本阶段把 `smooth_weights_3d_limited_adjustment` 的 forward observation events
从每日 research target snapshot 开始记录，并在 outcome window 到期后更新真实结果。
所有输出仍是 research-only / paper-shadow evidence，不写 official target weights，不修改
`position_advisory_v1.yaml`，不生成 order ticket，不调用 broker，不产生 production effect。

## 目标

1. 每日生成 smoothed forward observation event，并同时记录 3d smoothed、5d
   smoothed、limited adjustment、static baseline 和 no-trade baseline 权重。
2. 扫描 1 / 5 / 10 / 20 trading-day outcome windows 是否到期，并防止 future-as-of 更新。
3. 对 due scanner 标记为 `can_update=true` 的 window 计算真实 method returns、relative
   metrics 和 drawdown metrics。
4. 将 updated outcomes 分类为 sideways_choppy、strong_recovery、fast_regime_change、
   normal 或 unknown，用于推进 sideways / recovery progress。
5. 串联 daily emission、due scan、outcome update、classification、forward progress、
   weekly dashboard、event monitor、switch readiness 和 owner renewal，形成周度安全 runner。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-271|Smoothed Daily Target Emission|BASELINE_DONE_G2_4CS|`smoothed-daily-emission run/report` 和 `validate-smoothed-daily-emission` 可运行；candidate/targets 只来自 validated Binding；event 权重合法，`future_data_used=false`，不写 official target weights。|
|TRADING-272|Smoothed Forward Outcome Due Scanner|BASELINE_DONE_G2_4CS|`smoothed-outcome-due scan/report` 和 `validate-smoothed-outcome-due` 可运行；只消费 snapshot 明确绑定、同 Binding lineage 且 validator PASS 的 emission；due / not due / price missing / future-as-of blocker 可审计。|
|TRADING-273|Smoothed Forward Outcome Updater|BASELINE_DONE_G2_4CS|`smoothed-outcome-update run/report` 和 `validate-smoothed-outcome-update` 可运行；只更新 `can_update=true` windows，输出可复算 relative metrics 和 delta summary。|
|TRADING-274|Sideways / Recovery Forward Sample Classifier|BASELINE_DONE_G2_4CS|`smoothed-forward-classify run/report` 和 `validate-smoothed-forward-classify` 可运行；sideways / recovery / fast regime change / unknown 分类、阈值来源和置信度可审计。|
|TRADING-275|Smoothed Forward Evidence Weekly Runner|BASELINE_DONE_G2_4CS|`smoothed-forward-weekly-run run/report` 和 `validate-smoothed-forward-weekly-run` 可运行；精确绑定九段产物，无 candidate / due windows 时也生成 summary，且 `can_execute_switch=false`。|

## 数据与日期规则

- 默认市场 regime 是 `ai_after_chatgpt`，默认研究窗口从 2022-12-01 开始；报告必须披露
  requested `as_of` / `week_ending`。
- Daily emission 的 `as_of` 不得晚于当前可用价格日期，不使用未来价格。
- Due scanner 只根据 existing event `as_of`、trading-day calendar 和 scanner `as_of`
  判断 window 是否到期；`expected_end_date > scanner_as_of` 时不得更新。
- Outcome updater 只读取 due scanner 中 `can_update=true` 的 windows，且 end price 必须可用。
- Classification 不使用 outcome window 之外的未来信息判断 event `as_of` regime；tag
  不明确时使用价格行为 proxy 并标记 `classification_confidence=LOW`。

## Safety Boundary

所有新增 artifacts、CLI 摘要和 Reader Brief section 必须固定：

- `research_target_only=true`
- `paper_shadow_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

本阶段不做 broker API、broker import、自动下单、自动 owner approval、official target
weights、production target weights、自动切换 paper-shadow primary candidate、修改
`position_advisory_v1.yaml` 或 order ticket 生成。

## ARCH-004G2.4CS canonical exit contract（2026-07-14）

旧 2026-06-13 baseline 把 `smooth_weights_3d_limited_adjustment`、3d/5d/limited
角色和 10/5/5 progress requirements 写死，并从整个 emission 目录扫描 event。该前提已经被
G2.4CQ/CR 当前 source truth（`candidate_method=null`、Binding=`NOT_REGISTERED`、
`targets=[]`、Progress requirements/events 为空）取代。G2.4CS 不得继续把历史 fixture 假设
包装成当前 forward sample。

本 slice 的完成边界固定如下：

1. 15 个 callback 从 legacy CLI root 迁入独立 canonical interface；daily、due、update、
   classification、weekly 的业务实现迁入独立 sample-bootstrap domain，legacy domain 仅保留
   lazy compatibility wrapper，用户可见 CLI tree/help/default/flag contract 不漂移。
2. 五类 artifact 分别写入
   `smoothed_daily_emission_input_snapshot.v2`、
   `smoothed_outcome_due_input_snapshot.v2`、
   `smoothed_outcome_update_input_snapshot.v2`、
   `smoothed_forward_classification_input_snapshot.v2` 与
   `smoothed_forward_weekly_run_input_snapshot.v2`。Snapshot 只冻结本段实际读取的 bounded
   business views、validator result、semantic cutoff、价格数据切片与 source checksum，不递归
   复制上游 snapshot 正文。
3. Daily emission 的 candidate/targets 唯一来自 validated CQ Binding。Binding 为
   `NOT_REGISTERED` 或 candidate 为 null 时，正确结果是 0 event / 0 weights、
   `event_status=NOT_REGISTERED`；不得从 Model Target、固定 method id、历史 emission 或 latest
   directory 内容补造 candidate。只有 Binding 有真实 target 时，才允许读取 validator PASS 的
   Model Target，并要求 candidate method、target method、baseline、as-of 与权重路径一致。
4. 读取 cached prices 的 producer 在正式写件前执行与 `aits validate-data` 相同的质量路径并
   冻结结果；价格行按 `(date,ticker)` 唯一、有限、正值、requested cutoff 截断。未来日期、
   stale/missing 数据、重复键、无效 schema、source generated/as-of 晚于 consumer 均 fail closed。
5. Due scan 不再信任裸目录扫描结果。发现阶段只按 semantic timestamp/id 选择 cutoff 前、
   validator PASS、与当前 Binding 完全同 lineage 的 emission，并把精确 emission ids/source
   commitments 写入 snapshot；latest relevant invalid、duplicate event/window、cross-lineage、
   ambiguous selection 或 future-as-of 一律阻断。0 target 时 emission source 必须为空。
6. Outcome Update 只消费 validated Due 及其明确列出的 emission sources；window return 使用
   `sum(weight * (end/start - 1))`，drawdown 使用固定 event weights 下 daily portfolio return 的
   cumulative-equity peak-to-trough。缺价格、非有限值、权重不守恒或 `can_update!=true` 不得写
   AVAILABLE；missing 保持 null/明确 skip reason，不得以 0 伪装结果。
7. Classification 只消费 validated Update 及同 lineage emission；regime/context、relative return、
   turnover identity 和 lag warning 均从 snapshot 重算。阈值继续作为已命名且有相邻 rationale
   的 reporting-only invariant；不得成为 promotion/position/order gate，也不得在缺证据时输出
   HIGH confidence。
8. Weekly runner 以显式 Binding/Switch/Owner ids 为 authority，依次生成并冻结 daily→due→
   update→classification→progress→dashboard→monitor→recheck→renewal 九段精确 artifact binding；
   不使用 mtime/latest 结果替代 caller-specified lineage。当前 null candidate 必须得到
   emitted/due/updated/classified=0、Progress/Dashboard/Monitor=`NOT_REGISTERED`、Recheck=
   `NO_ELIGIBLE_CANDIDATE`、Renewal=`request_more_forward_data`。
9. 五类 validator 必须重验 live immutable upstream、snapshot schema/chronology/lineage/safety，
   并逐 byte 重建全部 JSON/JSONL/Markdown/Reader Brief；source/output 任一 byte tamper、future、
   duplicate、missing、non-finite、candidate mismatch 均 fail closed。PASS 只证明 artifact 可复算，
   不表示策略有效、evidence 充足、candidate 可晋升或 production ready。
10. 完成 focused、CLI contract、architecture-fitness、contract-validation、Ruff、compileall、
    module/test/writer manifests、deprecation inventory、source hashes 与 full validation；单 slice
    完成仍是 `COMPLETE_G2_4_CONTINUES`，不触发 ARCH-005 handoff，不进入 G2.5。

## Progress Notes

- 2026-07-14: G2.4CS=`COMPLETE_G2_4_CONTINUES`。15 callbacks 与旧业务实现已迁独立
  canonical sample-bootstrap interface/domain；五类 bounded v2 snapshots、producer 写前 live
  source/DQ/cutoff validation、Binding-only candidate/targets、exact emission/event/window
  lineage、null-preserving return/drawdown、动态 classification identity、九段 weekly binding 和
  全部 views 逐 byte rebuild 已闭合。当前 source truth 为 candidate=null、Binding=
  `NOT_REGISTERED`、0 targets，因此 emitted/due/updated/classified 均为 0，Progress/Dashboard/
  Monitor=`NOT_REGISTERED`、Recheck=`NO_ELIGIBLE_CANDIDATE`、Renewal=
  `request_more_forward_data`、`can_execute_switch=false`；workflow PASS 不是投资结论。性能方面，
  全 smoothed regression `270.04s→100.98s`（-62.61%/2.67x），readiness chain
  `245.98s→86.34s`（-64.90%），通过不可变 fixture cache 路径隔离与 content-fingerprint
  validation session 保持 tamper fail-closed。Focused/formula+smoothed/docs/CLI/architecture/
  contract/full=`15/41/19/123/280/203/6,012 passed`；runtime artifacts 为
  `architecture-fitness_20260714T172925Z`、`contract-validation_20260714T173059Z`、
  `full_20260714T173208Z`。本任务转 `BASELINE_DONE`，真实策略结论仍等待独立 forward/PIT/DQ/
  cost/holdout 证据；整个 G2.4 继续，不触发 ARCH-005 handoff，不进入 G2.5，
  `production_effect=none`。

- 2026-07-14: ARCH-004G2.4CS contract freeze，任务重新进入 `IN_PROGRESS`。审计确认旧链固定
  3d candidate/roles、扫描整个 emission 目录、没有 input snapshot/pre-output upstream replay，
  validators 不能复算来源、公式或 Markdown；这会与 CQ/CR 的 null candidate/0 target source
  truth 冲突。退出边界改为 Binding-only authority、五类 bounded v2 snapshots、DQ/PIT/cutoff、
  exact event/window lineage、null-preserving calculations、九段 weekly bindings 与 all-view byte
  rebuild。G2.4继续，不进入G2.5，`production_effect=none`。

- 2026-06-13: 新增需求文档并进入 IN_PROGRESS；owner 要求完成 TRADING-271～275，
  聚焦让 smoothed forward observation events 从 0 开始可生成、到期、更新、分类并进入
  weekly evidence。当前实现仍必须保持 no broker / no order / no official target weights /
  no production。
- 2026-06-13: baseline 实现完成并转入 VALIDATING；新增 daily emission、outcome due
  scanner、outcome updater、forward classifier、weekly runner、Reader Brief sample bootstrap
  section、report registry、artifact catalog、system flow、operations runbook、README 和 focused
  tests。真实当前日期链路在 `2026-06-13` 通过 data quality gate
  `PASS_WITH_WARNINGS`，输出 daily emission
  `smoothed-daily-emission_460e5c855929a93f`（`emitted_event_count=0`，
  `event_status=INSUFFICIENT_DATA`）、due scan
  `smoothed-outcome-due_1c9de63c4dd50178`（0 due windows）、outcome update
  `smoothed-outcome-update_004ef2fe4d7189d6`（0 updated/skipped windows）、
  classification `smoothed-forward-classification_16036a564efe6d24`（0 classified
  events）和 weekly runner `smoothed-forward-weekly-run_815263ee6f0634d2`
 （forward 0/10、sideways 0/5、recovery 0/5、`can_execute_switch=false`、
  `weekly_recommendation=continue_observation`）。附件指定的 `2026-06-20` due scan /
  weekly runner 因当前日期仍为 `2026-06-13` 且 cache 最新价格为 `2026-06-12`、
  rates 最新为 `2026-06-11`，被 `aits validate-data` 正确阻断为 `FAIL`
 （`prices_stale` / `rates_stale`）；解除条件是在 2026-06-20 或之后刷新缓存并重跑
  validate-data 后复验，不允许用未来数据或绕过质量门禁。
- 2026-06-14: 完成续验审计；全量 `python -m pytest tests -q` 首次跑完后发现
  documentation contract warning（3 个新 sample bootstrap artifact catalog 行缺少显式
  `schema_version` / `status` 术语），已补齐 `smoothed_outcome_update`、
  `smoothed_forward_classification` 和 `smoothed_forward_weekly_run` 的 catalog
  schema/status 描述，并复验 documentation contract / default contract test。
