# TRADING-383 Canonical Paper Shadow Health Rerun

最后更新：2026-06-16

## 背景

TRADING-368、TRADING-369 和 TRADING-371 已分别建立 data source fallback policy、
checksum/cache catalog 和 signal input completeness guard。Paper-shadow continuation
现在有多个 fail-closed 输入，需要一个 canonical health report 把 latest 数据、signal、
fallback、cache、daily/drift/weekly/staleness/readiness 状态合并成单一人工复核入口。

## 目标

- 解析 latest valid price data、market panel、signal input completeness、daily paper-shadow、
  drift monitor、weekly review、evidence staleness、shadow continuation readiness、fallback policy、
  cache catalog 和 data refresh audit 状态。
- 输出 canonical `paper_shadow_health_status`、`safe_to_continue_shadow`、data/signal/fallback/cache/
  weekly/drift 状态、blocking reasons、warnings 和 next required action。
- 新增 `paper-shadow-health run/report/validate` CLI。
- 新增 Reader Brief section、report registry entry、artifact catalog、README、operations runbook、
  system flow、requirements 和 focused tests。

## 非目标

- 不刷新 price、market panel、signal、feature、fallback、cache 或 paper-shadow artifacts。
- 不运行 upstream daily/drift/weekly/staleness/readiness 生成器。
- 不修改 candidate ledger、paper account、official target weights、portfolio、broker/order 或 production state。
- 不把 health status 解释为 production approval。

## Status Policy

Canonical health status 限定为：

|Status|含义|
|---|---|
|`HEALTHY`|所有 required health inputs 可用且 continuation safe。|
|`HEALTHY_WITH_WARNINGS`|无 blocker，但存在 warning 或 manual review note。|
|`MANUAL_REVIEW_REQUIRED`|无 data/signal/drift/safety hard blocker，但 coverage/readiness 仍需人工复核。|
|`BLOCKED_DATA`|price/market/data refresh/fallback/cache/evidence freshness 阻断。|
|`BLOCKED_SIGNAL_INPUTS`|signal input completeness 缺失或 `BLOCKING`。|
|`BLOCKED_DRIFT`|drift monitor 输出 blocking severity。|
|`BLOCKED_SAFETY`|任一 source artifact safety boundary 不通过。|

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_health/<health_id>/`

- `paper_shadow_health_manifest.json`
- `paper_shadow_health_report.json`
- `paper_shadow_health_report.md`
- `reader_brief_section.md`
- `paper_shadow_health_validation.json`
- `paper_shadow_health_validation.md`

所有输出固定：

- `paper_shadow_health_check_only=true`
- `read_only_health_aggregation=true`
- `manual_review_only=true`
- `data_downloaded_by_health_check=false`
- `pipelines_executed_by_health_check=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路能生成 canonical health report 并 validate PASS。
- Report 输出 required health statuses、blocking reasons、warnings、next action 和 source artifact links。
- Signal input/fallback/cache/readiness blockers 能映射到 expected canonical health status。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 health report。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为只读 canonical paper-shadow health aggregation，不刷新数据、不重跑上游、不触发 broker/order/paper account/production mutation。
- 2026-06-16：实现完成并转为 `DONE`。真实 artifact `paper-shadow-health_6389f9f8f0254c06` 输出 `paper_shadow_health_status=BLOCKED_SIGNAL_INPUTS`、`safe_to_continue_shadow=false`、`data_freshness_status=BLOCKING`、`signal_input_status=BLOCKING`、`fallback_status=PRIMARY_OK`、`cache_integrity_status=OK`、`weekly_review_coverage_status=MANUAL_REVIEW_REQUIRED`、`drift_status=NONE`、`readiness_status=BLOCKED_STALE_DATA`、`data_refresh_audit_status=PASS_WITH_WARNINGS`、blocking reason `signal_input_completeness:blocking`、next action `stop_paper_shadow_until_signal_inputs_are_restored`，validation `PASS` / failed=0。CLI run/report/validate、focused pytest 27 passed、Ruff、compileall、documentation contract、report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0 和 Reader Brief `--latest` + explicit 2026-06-16 report index 验证通过；Reader Brief JSON/HTML 已显示 `paper_shadow_health_*` 字段。默认 2026-06-16 Reader Brief 仍因本机缺 `decision_snapshot_2026-06-16.json` fail closed，使用 latest 2026-06-15 decision snapshot 是显式验证路径，不补造 decision snapshot。
