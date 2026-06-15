# TRADING-354C Shadow Continuation Readiness Report

最后更新：2026-06-16

## 背景

TRADING-354B 修复了 paper-shadow freshness 对 latest completed U.S. equity market date 的误判，TRADING-353A 又把一日 recovery weekly review 显式标记为 `RECOVERY_MODE_REVIEW`，因此 freshness monitor 可以显示 `ACCEPTABLE`，但 `safe_to_continue_shadow=false`。项目需要一个单一 advisory report 回答 paper-shadow 是否可以继续，而不是让 owner 同时拼读 daily、drift、weekly、staleness 和 data quality artifacts。

本任务只聚合已有 artifact，不刷新数据、不运行上游、不执行 paper account 或 broker workflow。

## 目标

- 新增 `shadow-continuation-readiness run/report` 和 `validate-shadow-continuation-readiness` CLI。
- 聚合 latest paper-shadow daily observation、drift monitor、weekly review、evidence staleness monitor、latest data quality report 和可用 safety-boundary payloads。
- 输出 `shadow_continuation_readiness`、`safe_to_continue_shadow`、`missing_artifacts`、`blocking_artifacts`、`stale_artifacts`、`coverage_status`、`manual_review_required`、`next_required_action`。
- 写出 manifest、JSON report、Markdown report、Reader Brief section 和 validation JSON/Markdown。
- 接入 report registry、artifact catalog、Reader Brief、README、operations runbook、system flow 和 task register。

## 非目标

- 不运行 `aits validate-data`，只读取 latest data quality report 并披露其 status。
- 不刷新 price、market panel、paper-shadow、staleness 或 research artifacts。
- 不创建 official target weights、order ticket、broker action、paper account mutation、candidate promotion/rejection 或 production state mutation。
- 不把 `READY_WITH_WARNINGS` 解释为 production approval。

## Decision States

|状态|含义|`safe_to_continue_shadow`|下一步|
|---|---|---:|---|
|`READY_TO_CONTINUE`|所有必需 source 存在、无 stale/blocking/missing、coverage PASS、staleness safe、data quality PASS、safety PASS|true|继续 paper-shadow observation|
|`READY_WITH_WARNINGS`|同上，但 data quality 为 `PASS_WITH_WARNINGS` 或 warning_count > 0|true|继续前人工复核 warning|
|`MANUAL_REVIEW_REQUIRED`|source/freshness/safety 无硬阻断，但 weekly coverage 或 staleness 明确要求人工复核|false|完成 full weekly review 或记录 manual coverage override|
|`BLOCKED_MISSING_ARTIFACTS`|必需 source 或 staleness report 的 required artifact 缺失|false|恢复缺失 artifacts|
|`BLOCKED_STALE_DATA`|staleness report 有 stale/blocking artifact，或 data validation 不是 `PASS` / `PASS_WITH_WARNINGS`|false|刷新或重新生成 stale inputs|
|`BLOCKED_SAFETY_BOUNDARY`|任一可用 source payload 显示 broker/order/official-target/production mutation 等不安全标志|false|停止，恢复 safety boundary|

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/shadow_continuation_readiness/<readiness_id>/`

- `shadow_continuation_readiness_manifest.json`
- `shadow_continuation_readiness_report.json`
- `shadow_continuation_readiness_report.md`
- `reader_brief_section.md`
- `shadow_continuation_readiness_validation.json`
- `shadow_continuation_readiness_validation.md`

所有输出固定：

- `shadow_continuation_readiness_only=true`
- `advisory_only=true`
- `paper_shadow_only=true`
- `manual_review_only=true`
- `data_downloaded_by_readiness=false`
- `pipelines_executed_by_readiness=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路应输出 `MANUAL_REVIEW_REQUIRED`，因为 latest weekly review coverage 为 `RECOVERY_MODE_REVIEW` / `MANUAL_REVIEW_REQUIRED`。
- Missing source 会 fail closed 为 `BLOCKED_MISSING_ARTIFACTS`。
- Data quality `PASS_WITH_WARNINGS` 在其他条件安全时输出 `READY_WITH_WARNINGS`，不是 production approval。
- Reader Brief 只读 latest artifact 并显示 readiness fields；缺失时显示 `MISSING`。
- Report registry、artifact catalog、README、operations runbook、system flow 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief validation、git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 advisory aggregation，不改变 freshness policy、不运行数据刷新、不触发 broker/order/paper account/production mutation。
- 2026-06-16：实现完成；真实 artifact `shadow-continuation-readiness_34cc39b45acfc208` 输出 `shadow_continuation_readiness=MANUAL_REVIEW_REQUIRED`、`safe_to_continue_shadow=false`、missing/blocking/stale artifacts 为空、`coverage_status=MANUAL_REVIEW_REQUIRED`、`next_required_action=complete_full_weekly_review_or_record_manual_coverage_override`、`data_validation_status=PASS_WITH_WARNINGS`、`safety_boundary_status=PASS`；`validate-shadow-continuation-readiness --latest` PASS。Focused pytest、Ruff、compileall、documentation contract、report index、Reader Brief 和 Reader Brief quality 均通过。
