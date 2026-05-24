# TRADING-033: Operator Brief Notification Approval Gate

关联任务：`TRADING-033`

最后更新：2026-05-24

## 背景

TRADING-032 已生成只读 dry-run dispatch preview，展示未来真实 dispatch 阶段可能形成的
channel plan、message preview 和 final decision。TRADING-033 在此基础上新增人工审批门控，
回答当前 preview 是否已被人工批准、审批是否仍有效、审批是否匹配当前 preview hash，以及是否
允许后续真实 dispatch 任务进入发送前执行阶段。

TRADING-033 depends on TRADING-032；TRADING-032 depends on TRADING-031；
TRADING-031 depends on TRADING-030。

## 安全边界

TRADING-033 只读取 TRADING-032 dispatch preview artifact 和可选本地 approval marker。
它不发送邮件、不发送 webhook、不调用 Gmail API / SMTP / Slack / Telegram / Discord、不创建
draft、不读取 secrets、不访问网络、不自动审批、不修改 TRADING-032 artifact、不修改 production
weights，也不把 approval gate 状态误标为真实发送结果。

所有 TRADING-033 输出必须固定：

- `production_effect=none`
- `manual_review_only=true`
- `approval_gate_only=true`
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
- `operator_brief_executed_by_approval_gate=false`
- `notification_draft_executed_by_approval_gate=false`
- `delivery_preflight_executed_by_approval_gate=false`
- `dispatch_preview_executed_by_approval_gate=false`
- `pipelines_executed_by_approval_gate=false`
- `data_downloaded_by_approval_gate=false`
- `apply_executed_by_approval_gate=false`
- `rollback_executed_by_approval_gate=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

`allowed_to_enter_dispatch=true` 只代表后续真实 dispatch 任务可以把该 gate artifact 当作审批证据；
TRADING-033 本身仍不执行发送。

## 输入

- 默认读取 `data/derived/operator_briefs/notifications/dispatch_preview/latest.json`；
- 可选显式 `--input-preview` 路径；
- 可选本地 approval marker：
  `data/derived/operator_briefs/notifications/approval_gate/approval_marker.json`；
- 可选日期目录下 marker：
  `data/derived/operator_briefs/notifications/approval_gate/approvals/operator_brief_notification_approval_YYYY-MM-DD.json`。

approval marker 示例：

```json
{
  "task_id": "TRADING-033",
  "approval_type": "manual",
  "approved": true,
  "approved_by": "operator",
  "approved_at": "2026-05-24T12:00:00Z",
  "expires_at": "2026-05-25T12:00:00Z",
  "preview_hash": "sha256:...",
  "approval_note": "Reviewed dispatch preview and approved dry-run transition to dispatch stage."
}
```

## 输出

- `data/derived/operator_briefs/notifications/approval_gate/operator_brief_notification_approval_gate_YYYY-MM-DD.json`
- `data/derived/operator_briefs/notifications/approval_gate/operator_brief_notification_approval_gate_YYYY-MM-DD.md`
- `data/derived/operator_briefs/notifications/approval_gate/latest.json`
- `data/derived/operator_briefs/notifications/approval_gate/latest.md`
- `data/derived/operator_briefs/notifications/approval_gate/run.log`

## 状态规则

|状态|触发条件|含义|
|---|---|---|
|`APPROVED`|TRADING-032 final status 为 `WOULD_SEND`；marker 存在且 `approved=true`；`preview_hash` 匹配当前 preview hash；审批未过期；没有 safety block 或 schema 错误|当前通知已通过人工审批，可以供后续真实 dispatch 阶段读取；本任务仍不发送。|
|`APPROVAL_REQUIRED`|TRADING-032 final status 为 `WOULD_SEND` 但 marker 缺失、TRADING-032 final status 为 `NEEDS_APPROVAL`、marker `approved` 不是 true，或 marker 缺少必要字段|需要人工审批，不能进入真实 dispatch。|
|`APPROVAL_EXPIRED`|marker 存在且 hash 匹配，但 `expires_at` 已早于当前时间|审批曾有效但已过期，需要重新审批。|
|`APPROVAL_MISMATCH`|marker 中 `preview_hash` 与当前 dispatch preview hash 不一致|dispatch preview 已变化，旧审批不能继续使用。|
|`SAFETY_BLOCKED`|TRADING-032 final status 为 `SAFETY_BLOCKED`、preview safety 字段存在阻断，或 marker 试图覆盖 safety block|安全原因阻断，审批不能覆盖。|
|`BLOCKED`|TRADING-032 final status 为 `BLOCKED`、preview artifact 缺失或 JSON/schema 不合法、hash 计算失败、marker JSON 不合法|当前无法可靠判断审批状态。|
|`NOOP`|TRADING-032 final status 为 `NOOP`|当前无须发送，不需要审批。|

## Hash 规则

TRADING-033 对 TRADING-032 JSON 生成稳定 hash，防止“审批了 A，但发送了 B”。

- hash algorithm: `sha256`
- hash scope: `canonical_dispatch_preview_json`
- canonical serialization 使用 sorted keys 和稳定 separators；
- volatile 字段不进入 hash，包括 `metadata.generated_at`、`metadata.preview_generated_at`、
  `audit.created_at`、`output_artifacts` 和运行日志路径；
- `generated_at` 变化不影响 hash；
- message preview、channel plan、status、safety 或 decision 变化必须影响 hash。

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-033，本文记录目标、边界、输入、输出和测试要求。|
|2. 核心 approval gate 模块|DONE|读取 TRADING-032 latest/dater artifact 和可选 marker，生成稳定 hash、状态决策、JSON/Markdown/latest/run.log。|
|3. CLI|DONE|新增 `scripts/run_operator_brief_notification_approval_gate.py`，支持 date、data-root、input-preview、approval-marker、output-dir。|
|4. Dashboard|DONE|Daily task dashboard 新增只读卡片，只读取 TRADING-033 latest/dater artifact，不重跑 approval gate 或发送逻辑。|
|5. 文档更新|DONE|更新 runbook、system flow、artifact catalog 和 requirements 索引。|
|6. 测试与验证|DONE|覆盖 7 种状态、hash 稳定性、marker 过期/不匹配、safety 不可覆盖、Markdown、CLI 和 dashboard 只读。|

## 状态记录

- 2026-05-24：新增并进入 `IN_PROGRESS`。原因：TRADING-032 已能生成 dry-run dispatch preview，
  但真实发送任务前仍缺少独立人工审批门控 artifact；本任务严格 approval-gate-only/read-only，
  不发送通知、不访问网络、不读取 secrets、不自动审批。
- 2026-05-24：实现完成并进入 `VALIDATING`。新增核心 approval gate builder、CLI、
  JSON/Markdown/latest/run.log 输出、稳定 preview hash、approval marker 校验、dashboard 只读卡片、
  runbook、system flow、artifact catalog、requirements 索引和专项测试；验证通过 TRADING-033 目标
  pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和 diff check；
  `python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，
  未混入无关格式化 diff。
