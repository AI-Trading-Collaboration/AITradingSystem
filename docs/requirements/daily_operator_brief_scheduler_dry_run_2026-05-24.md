# TRADING-026: Daily Operator Brief Scheduler Dry Run

最后更新：2026-05-24

关联任务：`TRADING-026`

状态：`DONE`

## 目标

在创建任何真实定时任务前，新增一个只读 scheduler dry-run 层，验证 Daily Trading System Operator Brief 是否具备未来每日定时生成条件。

本任务只读取既有 artifact，不运行 `TRADING-021` / `TRADING-022` / `TRADING-023` / `TRADING-024`，不创建 Windows Task Scheduler、cron 或 GitHub Actions workflow。

## 输入

- Required：latest `TRADING-021` parameter governance daily digest。
- Optional but preferred：latest `TRADING-023` pipeline health summary。
- Optional but preferred：latest `TRADING-024` data freshness summary。
- Optional：latest existing `TRADING-022` daily trading system operator brief。

日期判断优先使用 JSON `date` 字段，其次使用文件名 `YYYY-MM-DD` / `YYYY_MM_DD`，最后使用 modified time。默认 freshness window 为 2 天。

## 输出

- `data/derived/operator_briefs/scheduler_dry_run/daily_operator_brief_scheduler_dry_run_YYYY-MM-DD.json`
- `data/derived/operator_briefs/scheduler_dry_run/daily_operator_brief_scheduler_dry_run_YYYY-MM-DD.md`
- `data/derived/operator_briefs/scheduler_dry_run/logs/daily_operator_brief_scheduler_dry_run_log_YYYY-MM-DD.json`
- `data/derived/operator_briefs/scheduler_dry_run/logs/daily_operator_brief_scheduler_dry_run_log_YYYY-MM-DD.md`

核心字段：

- `dry_run_decision`: `READY` / `READY_WITH_WARNINGS` / `NOT_READY` / `SAFETY_BLOCKED` / `ERROR`
- `dry_run_status`: `OK` / `WATCH` / `ACTION_REQUIRED` / `SAFETY_BLOCKED` / `ERROR`
- `dependency_check`
- `safety_check`
- `expected_operator_brief_behavior`
- `schedule_plan`
- `input_artifacts`

## 安全边界

所有 TRADING-026 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `scheduler_dry_run_only=true`
- `read_only=true`
- `scheduler_created=false`
- `operator_brief_executed_by_scheduler_dry_run=false`
- `pipelines_executed_by_scheduler_dry_run=false`
- `data_downloaded_by_scheduler_dry_run=false`
- `apply_executed_by_scheduler_dry_run=false`
- `rollback_executed_by_scheduler_dry_run=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`
- `safe_for_scheduler=true`

`safe_for_scheduler=true` 只表示 dry-run 本身可以被定期执行，不表示真实 scheduler 已创建或被授权。

## Decision 规则

| 场景 | dry_run_decision | dry_run_status |
|---|---|---|
| TRADING-021/023/024 均存在、fresh、safety pass | READY | OK |
| TRADING-021 存在且 safety pass，但 023/024 缺失或 stale | READY_WITH_WARNINGS | WATCH |
| TRADING-021 缺失、stale 或 strict dependency failed | NOT_READY | ACTION_REQUIRED |
| 任何输入 artifact safety field 异常 | SAFETY_BLOCKED | SAFETY_BLOCKED |
| 运行异常 | ERROR | ERROR |

## Dashboard

Daily task dashboard 新增 `Daily Operator Brief Scheduler Dry Run` 只读卡片，只读取 latest TRADING-026 artifact。卡片展示：

- latest `dry_run_decision`
- `dry_run_status`
- `summary_level`
- `expected_run_time_local`
- `dependency_check.status`
- `safety_check.status`
- missing required / optional / stale count
- Markdown path

Dashboard 禁止触发 018B-025、026 script、operator brief、scheduler creation、market/backtest/scoring/data download/broker/replay/trading。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-026，本文记录目标、边界、decision 规则和验收。|
|2. 核心 dry-run builder|DONE|实现 artifact 查找、freshness、safety validation、decision mapping、JSON/Markdown/run log。|
|3. CLI|DONE|新增 `scripts/run_daily_operator_brief_scheduler_dry_run.py`，支持 date、data-root、run time、timezone、lookback、strict、fail-on-missing-required。|
|4. Dashboard|DONE|新增只读卡片，只读取 TRADING-026 artifact，不 import 或运行 dry-run builder/CLI。|
|5. 文档|DONE|更新 runbook、system flow 和 artifact catalog。|
|6. 测试与验证|DONE|覆盖 decision mapping、safety validation、no execution、Markdown、dashboard 和 safety invariants；最终收尾验证已完成；全仓 Black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 进展记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。本阶段只允许新增只读 scheduler dry-run artifact，不创建真实 scheduler、不执行 operator brief 或上游任务、不触发任何交易或数据流水线。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 builder、CLI、JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；验证通过 `tests/trading_engine/test_daily_operator_brief_scheduler_dry_run.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest、ruff 和 repo 外 READY / READY_WITH_WARNINGS / NOT_READY / SAFETY_BLOCKED smoke；全仓 Black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断。
- 2026-05-24：最终收尾验证完成并改为 `DONE`。repo 外 smoke 再次确认 READY / READY_WITH_WARNINGS / NOT_READY / SAFETY_BLOCKED 的 decision/status、13 项 dry-run 安全 invariant 和 Markdown banner/section；dashboard import guard 确认只读读取 TRADING-026 artifact，不触发 018B-025、026 script、operator brief、scheduler creation、market/backtest/scoring/data download/broker/replay/trading；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；全仓 Black check 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
