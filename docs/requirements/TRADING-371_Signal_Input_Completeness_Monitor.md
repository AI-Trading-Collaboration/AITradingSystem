# TRADING-371 Signal Input Completeness Monitor

最后更新：2026-06-16

## 背景

Paper-shadow daily observation 和 weekly review 现在会读取最新 signal / feature artifact。
如果 signal 文件缺失、schema 不兼容、series 为空、覆盖不完整或已经 stale，下游仍可能把
paper-shadow 结论误读为当前可用证据。TRADING-371 建立一个只读 completeness guard，在
paper-shadow 和 continuation readiness 前显式暴露 signal input 状态。

## 目标

- 新增 governed signal input manifest，定义 required signal files、schema / feature version、
  required columns、coverage universe 和 stale thresholds。
- 新增 `signal-input-completeness run/report/validate` CLI，输出 `OK|WARNING|BLOCKING`
  severity。
- 检查 missing signal files、stale signal files、incompatible schema version、empty signal
  series、partial market coverage 和 missing required feature columns。
- 集成 daily paper-shadow runner、paper-shadow weekly review、evidence staleness monitor、
  shadow continuation readiness report 和 Reader Brief。
- 注册 artifact family / report registry，并更新 README、operations runbook、system flow、
  artifact catalog、requirements 和 task register。

## 非目标

- 不刷新 signal、feature、market panel、price cache 或任何上游 artifact。
- 不重跑 signal generator、candidate scoring、backtest、paper-shadow drift monitor 或 weekly review。
- 不创建 official target weights、order ticket、broker action、paper account mutation、candidate
  promotion/rejection 或 production state mutation。
- 不把 `WARNING` 或 `OK` 解释为 production approval。

## Severity Policy

Threshold 和 required input 定义必须位于
`config/etf_portfolio/dynamic_v3_rescue/signal_input_completeness_v1.yaml`，并包含 owner、
version/status、rationale、intended effect、validation evidence 和 review condition。代码只应用
policy，不引入会改变投资解释的隐藏 threshold。

|Severity|含义|下游行为|
|---|---|---|
|`OK`|所有 required inputs 存在、schema/columns/coverage/date checks 通过|允许继续 paper-shadow，但仍不是 production approval|
|`WARNING`|存在 non-blocking stale / status warning，但 required inputs 可读|Reader Brief 和 readiness 披露 warning，owner 复核|
|`BLOCKING`|required input 缺失、不可读、schema 不兼容、series 为空、required columns 缺失、required coverage 缺失或超过 blocking stale threshold|daily paper-shadow fail closed；weekly/staleness/readiness 标记阻断|

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/signal_input_completeness/<monitor_id>/`

- `signal_input_completeness_manifest.json`
- `signal_input_completeness_report.json`
- `signal_input_completeness_findings.jsonl`
- `signal_input_completeness_report.md`
- `reader_brief_section.md`
- `signal_input_completeness_validation.json`
- `signal_input_completeness_validation.md`

所有输出固定：

- `signal_input_completeness_monitor_only=true`
- `read_only_signal_input_check=true`
- `manual_review_only=true`
- `data_downloaded_by_monitor=false`
- `pipelines_executed_by_monitor=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路能生成 completeness report 并 validate PASS。
- Missing/incompatible required signal input 会输出 `signal_input_status=BLOCKING`，daily
  paper-shadow fail closed。
- Weekly review、evidence staleness monitor 和 shadow continuation readiness 披露 signal
  completeness status，并在 blocking/missing 时阻断 continuation。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 signal input artifact。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task
  register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief
  validation 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为只读 signal input completeness guard，不刷新数据、不重跑上游、不触发 broker/order/paper account/production mutation。
- 2026-06-16：实现完成并归档为 DONE。真实 monitor `signal-input-completeness_2fe124e7367a3282` 输出 `signal_input_status=BLOCKING`，原因是 `etf_feature_matrix`、`etf_signal_series` 和 `latest_signal_snapshot` stale；`validate-signal-input-completeness` PASS。真实 evidence staleness `evidence-staleness-monitor_f1d783ee2c383e7f` 和 shadow continuation readiness `shadow-continuation-readiness_0061ea84c77efcd8` 均只读传播该 blocker，分别输出 `evidence_freshness_status=BLOCKING` 与 `shadow_continuation_readiness=BLOCKED_STALE_DATA`，validate 均 PASS。Reader Brief 2026-06-16 使用显式 2026-06-16 report index 和 latest real 2026-06-15 decision snapshot 生成 `LIMITED_READER_CONTEXT`，已展示 signal input `BLOCKING`、stale input ids 和 stop action；limited context 仅来自缺少 2026-06-16 decision snapshot。
