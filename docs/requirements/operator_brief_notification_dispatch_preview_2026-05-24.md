# TRADING-032: Operator Brief Notification Dry-run Dispatch Preview

关联任务：`TRADING-032`

最后更新：2026-06-09

## 背景

TRADING-031 已提供 Operator Brief notification delivery preflight，用于判断
TRADING-030 notification drafts 是否具备发送前审查条件。TRADING-032 在此基础上新增
只读 dry-run dispatch preview，回答如果进入未来真实发送阶段，将形成怎样的 channel plan、
message preview、safety decision 和人工审核动作。

## 安全边界

TRADING-032 只读取既有 artifacts，不执行真实发送，不调用外部服务，不读取 `.env` secrets，
不创建 Gmail draft，不调用 SMTP / Slack / Telegram / Discord / webhook，不读取私密邮箱，
不修改 scheduler、production weights、production config 或任何交易执行路径。

所有 TRADING-032 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `dispatch_preview_only=true`
- `read_only=true`
- `external_side_effects=false`
- `network_access_required=false`
- `secrets_required=false`
- `email_sent=false`
- `gmail_draft_created=false`
- `gmail_draft_modified=false`
- `slack_sent=false`
- `telegram_sent=false`
- `discord_sent=false`
- `webhook_called=false`
- `mobile_push_sent=false`
- `operator_brief_executed_by_dispatch_preview=false`
- `notification_draft_executed_by_dispatch_preview=false`
- `delivery_preflight_executed_by_dispatch_preview=false`
- `pipelines_executed_by_dispatch_preview=false`
- `data_downloaded_by_dispatch_preview=false`
- `apply_executed_by_dispatch_preview=false`
- `rollback_executed_by_dispatch_preview=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

`dispatch_preview_only=true` 只表示生成可人工审阅的 dry-run plan，不表示允许自动发送。

## 输入

- latest TRADING-031 delivery preflight JSON；
- latest TRADING-022 Daily Trading System Operator Brief JSON；
- matching Operator Brief Markdown；
- TRADING-031 artifact 中声明的 TRADING-030 notification draft metadata 和草稿文件；
- 可选显式 CLI 输入路径。

所有显式路径必须解析在 repo 根目录内。路径逃逸 repo 根目录时输出 `SAFETY_BLOCKED`，且
不得读取或写入逃逸路径。

## 输出

- `data/derived/operator_briefs/notifications/dispatch_preview/operator_brief_notification_dispatch_preview_YYYY-MM-DD.json`
- `data/derived/operator_briefs/notifications/dispatch_preview/operator_brief_notification_dispatch_preview_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/dispatch_preview/latest.json`
- `data/derived/operator_briefs/notifications/dispatch_preview/latest.md`
- `data/derived/operator_briefs/notifications/dispatch_preview/run.log`

## 状态规则

|状态|触发条件|含义|
|---|---|---|
|`WOULD_SEND`|TRADING-031 为 `PASS`，Operator Brief artifact 可读，message preview 生成成功，至少一个 channel enabled，且没有 safety block|未来真实 dispatch 阶段理论上可以发送；当前仍然只是 dry-run。|
|`NEEDS_APPROVAL`|TRADING-031 delivery readiness 为 `NEEDS_APPROVAL`、approval required、ACTION/URGENT 内容、模板/配置要求人工审批，或 body 超过审阅阈值|可形成发送计划，但必须人工批准。|
|`SAFETY_BLOCKED`|TRADING-031 safety blocked、路径逃逸、未脱敏 recipient、疑似 secret/private key/token/password、可疑 endpoint 或其他 safety finding|安全原因阻止发送。|
|`BLOCKED`|TRADING-031 blocked/input invalid/error、必要 brief artifact 缺失或不可读、schema 不合法、channel plan 不可形成、message preview 不可生成|当前无法形成可靠 dispatch plan。|
|`NOOP`|notification 明确 disabled，或 upstream 明确 no report / no trading day / no update|系统正常运行，但当日无须发送。|

`BODY_APPROVAL_REVIEW_THRESHOLD_CHARS` 是保守人工复核边界：当 dry-run body 极长时，preview
升级为 `NEEDS_APPROVAL`，避免人工审核者误把长通知当成低风险自动发送计划。该阈值不用于投资评分、
仓位或交易结论。

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-032，本文记录目标、边界、输入、输出和测试要求。|
|2. 核心 dispatch preview 模块|DONE|读取 TRADING-031 / TRADING-022 / TRADING-030 artifacts，生成结构化 JSON、Markdown、latest copies 和 run.log；覆盖 5 种 final status。|
|3. CLI|DONE|新增 `scripts/run_operator_brief_notification_dispatch_preview.py`，默认可 smoke 运行，支持 date、data-root、input-preflight、operator brief、output-dir。|
|4. Dashboard|DONE|Daily task dashboard 新增只读卡片，只读取 TRADING-032 latest/dater artifact，不重跑 pipeline 或发送逻辑。|
|5. 文档更新|DONE|更新 runbook、system flow、artifact catalog 和 requirements 索引。|
|6. 测试与验证|DONE|覆盖状态映射、安全边界、recipient masking、excerpt limit、run.log、dashboard 只读和验证命令。|

## 状态记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。原因：TRADING-031 只能判断通知是否具备发送条件，仍缺少真正进入邮件、Slack、Telegram、Webhook 或其他通知发送前的 dry-run dispatch plan；本任务严格 preview-only/read-only，不执行真实发送、不访问外部网络、不读取 secrets、不修改生产配置。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 dispatch preview builder、CLI、JSON/Markdown/latest/run.log 输出、dashboard 只读卡片、runbook、system flow、artifact catalog、requirements 索引和专项测试；验证通过 TRADING-032 目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 diff check；`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-06-09：从 `VALIDATING` 改为 `BASELINE_DONE`。本轮先读取 operations runbook 并运行 `aits validate-data`，状态为 `PASS_WITH_WARNINGS` / 错误数 0；临时真实形态 fixture smoke 输出 `WOULD_SEND`，3 个 channel 均为 dry-run would-send，`production_effect=none`，所有 email/Gmail/webhook/broker/replay/trading flags 为 false；缺输入 smoke 输出 `BLOCKED` 且正常写出可审计 artifact，副作用字段保持 false；目标 dispatch preview + dashboard pytest 33 passed，Ruff 和 scoped Black 通过。当前缺真实 `TRADING-031` delivery preflight artifacts，剩余真实 artifact 观察已登记为 `TRADING-032A`。
