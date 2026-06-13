# TRADING-271 to 275 Smoothed Forward Sample Bootstrap and Daily Evidence Collection

最后更新：2026-06-13

## 状态

VALIDATING

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
|TRADING-271|Smoothed Daily Target Emission|BASELINE_DONE|`smoothed-daily-emission run/report` 和 `validate-smoothed-daily-emission` 可运行；event 权重合法，`future_data_used=false`，不写 official target weights。|
|TRADING-272|Smoothed Forward Outcome Due Scanner|BASELINE_DONE|`smoothed-outcome-due scan/report` 和 `validate-smoothed-outcome-due` 可运行；due / not due / price missing / future-as-of blocker 可审计。|
|TRADING-273|Smoothed Forward Outcome Updater|BASELINE_DONE|`smoothed-outcome-update run/report` 和 `validate-smoothed-outcome-update` 可运行；只更新 `can_update=true` windows，输出 relative metrics 和 delta summary。|
|TRADING-274|Sideways / Recovery Forward Sample Classifier|BASELINE_DONE|`smoothed-forward-classify run/report` 和 `validate-smoothed-forward-classify` 可运行；sideways / recovery / fast regime change / unknown 分类和置信度可读。|
|TRADING-275|Smoothed Forward Evidence Weekly Runner|BASELINE_DONE|`smoothed-forward-weekly-run run/report` 和 `validate-smoothed-forward-weekly-run` 可运行；无 due windows 时也生成 summary，且 `can_execute_switch=false`。|

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

## Progress Notes

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
