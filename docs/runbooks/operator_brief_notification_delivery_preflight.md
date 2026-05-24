# Operator Brief Notification Delivery Preflight

## 1. 目的

TRADING-031 在人工发送或未来 delivery dry-run 前，只读检查 TRADING-030 notification draft 是否具备进入后续人工流程的条件。

它读取：

- latest TRADING-030 notification draft metadata
- metadata 声明的 email draft
- metadata 声明的 chat draft
- metadata 声明的 mobile summary
- 可选 recipient config
- 可选 channel config
- 可选 approval policy config

它不发送任何通知，不创建或修改 Gmail draft，不调用 webhook，不推送 mobile notification。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "notification_delivery_preflight_only": true,
  "read_only": true,
  "email_sent": false,
  "gmail_draft_created": false,
  "gmail_draft_modified": false,
  "slack_sent": false,
  "discord_sent": false,
  "webhook_called": false,
  "mobile_push_sent": false,
  "operator_brief_executed_by_delivery_preflight": false,
  "notification_draft_executed_by_delivery_preflight": false,
  "pipelines_executed_by_delivery_preflight": false,
  "data_downloaded_by_delivery_preflight": false,
  "apply_executed_by_delivery_preflight": false,
  "rollback_executed_by_delivery_preflight": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示 preflight 本身可以定期运行，不表示允许自动发送通知。

## 2. 如何运行

默认读取 latest TRADING-030 metadata：

```bash
python scripts/run_operator_brief_notification_delivery_preflight.py --date 2026-05-24
```

指定输入：

```bash
python scripts/run_operator_brief_notification_delivery_preflight.py \
  --date 2026-05-24 \
  --notification-draft-metadata-file data/derived/operator_briefs/notifications/operator_brief_notification_draft_2026-05-24.json
```

可选 config：

```bash
python scripts/run_operator_brief_notification_delivery_preflight.py \
  --date 2026-05-24 \
  --recipient-config-file config/notification_recipients.example.json \
  --channel-config-file config/notification_channels.example.json \
  --approval-config-file config/notification_approval_policy.example.json
```

严格 urgent approval 检查：

```bash
python scripts/run_operator_brief_notification_delivery_preflight.py \
  --date 2026-05-24 \
  --fail-on-urgent-without-approval
```

## 3. 输出位置

```text
data/derived/operator_briefs/notifications/delivery_preflight/
  operator_brief_notification_delivery_preflight_YYYY-MM-DD.json
  operator_brief_notification_delivery_preflight_YYYY-MM-DD.md
  logs/operator_brief_notification_delivery_preflight_run_YYYY-MM-DD.json
  logs/operator_brief_notification_delivery_preflight_run_YYYY-MM-DD.md
```

## 4. preflight_status

|状态|含义|处理|
|---|---|---|
|`PASS`|TRADING-030 metadata 与草稿安全可读。|可进入人工审阅，不代表允许自动发送。|
|`PASS_WITH_WARNINGS`|草稿安全，但存在 approval 或 config warning。|先处理 warning，再人工判断。|
|`BLOCKED`|draft 缺失、空文件、BLOCKED severity 或 strict approval 阻断。|不要发送，先修复输入。|
|`INPUT_MISSING`|找不到 TRADING-030 metadata。|先生成或定位 TRADING-030 artifact。|
|`INPUT_INVALID`|metadata JSON 非法或 `task_id != TRADING-030`。|修复 metadata，不要绕过。|
|`SAFETY_BLOCKED`|metadata 安全字段异常、config 允许自动发送、或 draft 含未 redacted secret。|停止发送流程并调查。|
|`ERROR`|运行异常。|查看 run log 后修复。|

## 5. delivery_readiness

|状态|含义|
|---|---|
|`READY_FOR_MANUAL_REVIEW`|可人工审阅草稿和 recipient/channel。|
|`NEEDS_APPROVAL`|`ACTION` / `URGENT` 等 severity 需要 approval 判断。|
|`BLOCKED`|不能进入发送前人工流程。|
|`SAFETY_BLOCKED`|存在安全阻断，禁止发送。|
|`UNKNOWN`|输入缺失、非法或运行错误，无法判断 readiness。|

## 6. Draft Validation

检查项：

- TRADING-030 metadata 是否存在且 `task_id=TRADING-030`
- metadata safety fields 是否符合只读边界
- email/chat/mobile draft path 是否存在
- draft 文件是否可读、非空、可计算 sha256
- 内容是否包含未 redacted 敏感字段

未 redacted 敏感字段包括：

- `api_key=`
- `apiKey=`
- `secret=`
- `token=`
- `password=`
- `credential=`
- `broker_token=`
- `account_id=`
- `private_key=`

`api_key=[REDACTED]`、`password=[REDACTED]`、`token=[REDACTED]` 视为已 redacted。

## 7. Approval Validation

TRADING-031 不执行 approval，也不生成 approval artifact。它只判断当前 severity 是否需要 approval。

默认规则：

- `NORMAL` / `WATCH`：可进入人工审阅。
- `ACTION` / `URGENT`：`delivery_readiness=NEEDS_APPROVAL`。
- `BLOCKED`：不能进入 delivery。

如果 approval policy 缺失，`ACTION` / `URGENT` 会输出 warning；如果使用 `--fail-on-urgent-without-approval`，`URGENT` 缺 policy 会被阻断。

## 8. Channel Readiness

Channel readiness 只说明草稿能否进入人工审阅或手工复制流程。

- `email.can_send_automatically` 永远为 `false`。
- `chat.can_send_automatically` 永远为 `false`。
- `mobile.can_send_automatically` 永远为 `false`。
- `manual_send_only` 必须为 `true`。

recipient/channel config 缺失不会自动失败，但 channel 会保留 manual-review-only warning。

如果 channel config 或 approval config 中出现 `auto_send_allowed=true`、`gmail_draft_creation_allowed=true`、`webhook_send_allowed=true`、`mobile_push_allowed=true`，或 `manual_send_only=false`，preflight 必须 `SAFETY_BLOCKED`。

Webhook URL 不应进入 config；如果出现，TRADING-031 只记录 redacted warning，不在输出中回显 URL。

## 9. NEEDS_APPROVAL 处理

1. 打开 TRADING-031 Markdown。
2. 打开 TRADING-030 metadata 和 email/chat/mobile 草稿。
3. 打开 source TRADING-022 operator brief。
4. 核对 `ACTION` / `URGENT` 的原因是否仍成立。
5. 人工确认 recipient/channel。
6. 不要从 TRADING-031 自动发送。

## 10. BLOCKED 处理

常见原因：

- TRADING-030 metadata 存在但 draft 文件缺失。
- draft 文件为空。
- notification severity 为 `BLOCKED`。
- strict urgent approval policy 缺失。

处理方式：

1. 修复缺失或空的 TRADING-030 artifact。
2. 不要手工伪造 draft readiness。
3. 重新运行 TRADING-031。

## 11. SAFETY_BLOCKED 处理

常见原因：

- TRADING-030 metadata safety field 异常。
- draft 中出现 `api_key=abc123`、`password=rawpass`、`token=rawtoken` 等未 redacted 值。
- config 允许自动发送或关闭 manual-only 边界。

处理方式：

1. 停止 delivery review。
2. 查阅 `alerts.critical` 和 `safety_validation.blocking_reasons`。
3. 回到 TRADING-030 或 config 源头修复。
4. 不要通过编辑 TRADING-031 输出绕过安全阻断。

## 12. 为什么不发送通知

发送 email、创建 Gmail draft、调用 Slack / Discord webhook 或 mobile push 都会改变外部系统状态。TRADING-031 的职责是 preflight，不是 delivery。真实 Gmail draft、webhook dry-run 或发送动作必须拆成后续独立任务，并要求更严格的人工 approval。

## 13. 后续 Gmail draft / webhook dry-run 设计

未来任务可以拆分为：

- Gmail Draft Creation Preflight：仍不创建 Gmail draft，只检查是否满足创建 draft 的条件。
- Webhook Dry-run Preflight：仍不调用真实 webhook，只验证 payload、channel policy 和 redaction。
- Real Delivery：必须单独审批，显式记录 recipient、channel、approval artifact 和外部系统写入结果。

任何后续任务不得绕过 TRADING-031 的 safety fields、TRADING-030 metadata 和 TRADING-022 source operator brief。
