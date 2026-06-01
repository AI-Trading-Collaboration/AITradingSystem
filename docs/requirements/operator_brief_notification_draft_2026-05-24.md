# TRADING-030: Operator Brief Notification Draft

最后更新：2026-05-24

关联任务：`TRADING-030`

状态：`DONE`

## 目标

新增只读、draft-only 的 Operator Brief Notification Draft generator。它读取 latest TRADING-022 Daily Trading System Operator Brief，并可选读取 TRADING-021 Parameter Governance Daily Digest、TRADING-023 Pipeline Health Summary、TRADING-024 Data Freshness Summary 和 TRADING-026 Scheduler Dry Run，然后生成可人工审阅的 email / chat / mobile 通知草稿、metadata JSON、summary Markdown 和 run log。

TRADING-030 不发送 email、不创建 Gmail draft、不调用 Slack / Discord webhook、不发送 mobile push、不运行 TRADING-021/022/023/024/026、不运行 scheduler、不运行 market / backtest / scoring / data download / apply / rollback / broker / replay / trading。

## 输入

- 必需：`data/derived/operator_briefs/daily_trading_system_operator_brief_YYYY-MM-DD.json`
- 可选：`data/derived/weight_iterations/governance/digests/parameter_governance_daily_digest_YYYY-MM-DD.json`
- 可选：`data/derived/pipeline_health/pipeline_health_summary_YYYY-MM-DD.json`
- 可选：`data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.json`
- 可选：`data/derived/operator_briefs/scheduler_dry_run/daily_operator_brief_scheduler_dry_run_YYYY-MM-DD.json`

默认自动查找不晚于 `--date` 的 latest artifact。显式传入文件时只读取该文件，不运行生成脚本。

## 输出

- `data/derived/operator_briefs/notifications/operator_brief_notification_draft_YYYY-MM-DD.json`
- `data/derived/operator_briefs/notifications/operator_brief_notification_draft_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/email/operator_brief_email_draft_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/chat/operator_brief_chat_draft_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/mobile/operator_brief_mobile_summary_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/logs/operator_brief_notification_draft_run_YYYY-MM-DD.json`
- `data/derived/operator_briefs/notifications/logs/operator_brief_notification_draft_run_YYYY-MM-DD.md`

## 安全边界

所有 TRADING-030 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `notification_draft_only=true`
- `read_only=true`
- `safe_for_scheduler=true`
- `email_sent=false`
- `gmail_draft_created=false`
- `slack_sent=false`
- `discord_sent=false`
- `mobile_push_sent=false`
- `operator_brief_executed_by_notification_draft=false`
- `pipelines_executed_by_notification_draft=false`
- `data_downloaded_by_notification_draft=false`
- `apply_executed_by_notification_draft=false`
- `rollback_executed_by_notification_draft=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

`safe_for_scheduler=true` 只表示 draft generator 本身可以被定时生成，不表示任何通知可以自动发送。

## 状态规则

|状态|含义|
|---|---|
|`GENERATED`|TRADING-022 operator brief 存在、安全校验通过、草稿全部生成。|
|`GENERATED_WITH_WARNINGS`|可选 artifact 缺失、operator brief 有 warning、或敏感字段 redaction 后仍成功生成草稿。|
|`INPUT_MISSING`|TRADING-022 operator brief 缺失。|
|`INPUT_INVALID`|operator brief JSON 非法、不是对象或 `task_id != TRADING-022`。|
|`SAFETY_BLOCKED`|operator brief 安全边界异常，例如 broker/replay/trading/apply/rollback 标志异常。|
|`ERROR`|运行异常。|

## Severity 规则

|Operator brief status|Notification severity|
|---|---|
|`OK`|`NORMAL`|
|`WATCH`|`WATCH`|
|`ACTION_REQUIRED`|`ACTION`|
|`URGENT`|`URGENT`|
|`SAFETY_BLOCKED`|`BLOCKED`|
|`INPUT_MISSING` / `INPUT_INVALID` / `ERROR`|`UNKNOWN`|

## 内容安全

通知草稿必须对疑似敏感字段值做 redaction，至少覆盖 `api_key`、`secret`、`token`、`password`、`credential`、`account_id`。Redaction 不应默认失败，但 metadata 必须记录 warning，并将 `draft_status` 调整为 `GENERATED_WITH_WARNINGS`。

## Dashboard

Daily task dashboard 新增 `Operator Brief Notification Draft` 只读卡片，只读取 latest TRADING-030 metadata artifact。卡片展示：

- `draft_status`
- `notification_severity`
- `headline`
- email / chat / mobile draft path
- `manual_review_required`
- email / Slack / Discord / mobile push sent flags

Dashboard 禁止触发 018B-029、TRADING-030 script、operator brief、email/Gmail/Slack/Discord/mobile push、market/backtest/scoring/data download/broker/replay/trading。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-030，本文记录目标、边界、输出和验收。|
|2. 核心 draft generator|DONE|实现 operator brief 加载、安全校验、optional artifact 读取、severity、redaction、draft content、metadata 和 run log。|
|3. CLI|DONE|新增 `scripts/generate_operator_brief_notification_draft.py`。|
|4. Dashboard|DONE|新增只读卡片，只读取 TRADING-030 metadata artifact。|
|5. 文档|DONE|新增 runbook，更新 system flow 和 artifact catalog。|
|6. 测试与验证|DONE|覆盖输入状态、severity、草稿输出、不发送通知、redaction、dashboard 只读和 output safety invariants；全仓 Black check 仍被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 进展记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。本阶段只生成人工审阅通知草稿，不发送任何通知、不创建 Gmail draft、不调用 webhook、不推送手机、不运行 operator brief、scheduler、数据、评分、回测或交易执行流水线。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 generator、CLI、metadata JSON、summary Markdown、email/chat/mobile 草稿、run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；临时目录 smoke 覆盖 NORMAL、ACTION、URGENT、SAFETY_BLOCKED 和 REDACTION，确认 sent flags 全为 false 且 redaction 未泄露构造的 secret；验证通过 `tests/trading_engine/test_operator_brief_notification_draft.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-05-24：最终收尾验证完成并改为 `DONE`。repo 外 smoke 再次确认 NORMAL 生成 email/chat/mobile/summary/metadata，ACTION draft 包含 `Action Required`，URGENT draft 包含 `URGENT` banner，SAFETY_BLOCKED draft 明确标记 blocked，REDACTION 场景中构造的 `api_key` / `token` / `password` / `credential` 值未出现在 draft 输出中；五路径 metadata 均确认 `production_effect=none`、`manual_review_only=true`、`notification_draft_only=true`、`read_only=true`、`email_sent=false`、`gmail_draft_created=false`、`slack_sent=false`、`discord_sent=false`、`mobile_push_sent=false`、`operator_brief_executed_by_notification_draft=false`、`pipelines_executed_by_notification_draft=false`、`data_downloaded_by_notification_draft=false`、`apply_executed_by_notification_draft=false`、`rollback_executed_by_notification_draft=false`、`broker_execution=false`、`replay_execution=false`、`trading_execution=false`；dashboard import guard 确认 Operator Brief Notification Draft 卡片只读读取 TRADING-030 metadata artifact，不触发 018B-029、TRADING-030 script、operator brief、email/Gmail/Slack/Discord/mobile push、market/backtest/scoring/data download/broker/replay/trading；收尾验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff；`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
