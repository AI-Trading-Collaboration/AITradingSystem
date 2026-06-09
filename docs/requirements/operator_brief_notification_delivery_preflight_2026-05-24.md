# TRADING-031: Operator Brief Notification Delivery Preflight

状态：`BASELINE_DONE`

最后更新：2026-06-09

关联任务：`TRADING-031`

## 背景

TRADING-030 已能从 TRADING-022 Daily Trading System Operator Brief 生成
review-only notification draft metadata、email draft、chat draft 和 mobile summary。
当前仍缺少发送前只读预检层，用于判断这些草稿是否可进入人工发送或未来更严格的
Gmail draft / webhook dry-run 任务。

## 安全边界

TRADING-031 只读取 TRADING-030 metadata 与其声明的 draft 文件，以及可选
recipient/channel/approval config。它不发送 email，不创建或修改 Gmail draft，
不调用 Slack / Discord webhook，不发送 mobile push，不运行 operator brief、
notification draft generator、market/backtest/scoring/data download pipeline、
apply、rollback、broker、replay 或 trading execution。

所有 TRADING-031 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `notification_delivery_preflight_only=true`
- `read_only=true`
- `email_sent=false`
- `gmail_draft_created=false`
- `gmail_draft_modified=false`
- `slack_sent=false`
- `discord_sent=false`
- `webhook_called=false`
- `mobile_push_sent=false`
- `operator_brief_executed_by_delivery_preflight=false`
- `notification_draft_executed_by_delivery_preflight=false`
- `pipelines_executed_by_delivery_preflight=false`
- `data_downloaded_by_delivery_preflight=false`
- `apply_executed_by_delivery_preflight=false`
- `rollback_executed_by_delivery_preflight=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`
- `safe_for_scheduler=true`

`safe_for_scheduler=true` 只表示 preflight 本身可定期运行，不表示允许自动发送通知。

## 输出

- `data/derived/operator_briefs/notifications/delivery_preflight/operator_brief_notification_delivery_preflight_YYYY-MM-DD.json`
- `data/derived/operator_briefs/notifications/delivery_preflight/operator_brief_notification_delivery_preflight_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/delivery_preflight/logs/operator_brief_notification_delivery_preflight_run_YYYY-MM-DD.json`
- `data/derived/operator_briefs/notifications/delivery_preflight/logs/operator_brief_notification_delivery_preflight_run_YYYY-MM-DD.md`

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-031，本文记录目标、边界、输出和测试要求。|
|2. 核心 preflight 模块|DONE|加载 latest TRADING-030 metadata、校验安全字段、读取 draft 文件、扫描敏感字段、加载可选 config、生成 readiness 和 JSON/Markdown/run log。|
|3. CLI|DONE|新增 `scripts/run_operator_brief_notification_delivery_preflight.py`，支持指定日期、data root、metadata/config 路径和 strict urgent approval flag。|
|4. Dashboard|DONE|Daily task dashboard 新增 `Operator Brief Notification Delivery Preflight` 只读卡片，只读取 TRADING-031 artifact。|
|5. 文档更新|DONE|更新 runbook、system flow 和 artifact catalog，明确 022 -> 030 -> 031 且 031 不发送通知。|
|6. 测试与 smoke|DONE|覆盖输入状态、draft 校验、redaction、approval、channel config、不发送通知、dashboard 只读和 output safety invariants。|
|7. 验证收尾|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff、Black check 和 smoke 结果已记录；Black 仍被既有 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。|

## 状态记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。原因：TRADING-030 已能生成 review-only 通知草稿，但缺少独立 delivery preflight 来检查草稿存在性、安全边界、redaction、approval need 和 channel readiness；本任务仍严格只读，不发送通知、不创建 Gmail draft、不调用 webhook。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 preflight builder、CLI、JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；临时目录 smoke 覆盖 PASS、NEEDS_APPROVAL、SAFETY_BLOCKED、BLOCKED，确认所有 sent/webhook/execution flags 为 false；验证通过 `tests/trading_engine/test_operator_brief_notification_delivery_preflight.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-06-09：从 `VALIDATING` 改为 `BASELINE_DONE`。原因：本轮先读取
  `docs/operations/operations_runbook.md` 并执行 `aits validate-data`，状态为
  `PASS_WITH_WARNINGS` / 错误数 0；临时真实形态 fixture smoke 输出 `PASS`、
  `READY_FOR_MANUAL_REVIEW`、`production_effect=none`、所有 email/Gmail/webhook/broker/
  replay/trading/pipeline flags 为 false；缺输入 smoke 输出 `INPUT_MISSING` / `UNKNOWN`
  并保持 fail-closed；目标 preflight + dashboard pytest 50 passed，Ruff 和 scoped Black
  通过。当前本机没有可审计真实 `TRADING-030` notification draft artifacts，真实 artifact
  观察已拆分为 `TRADING-031A`。
