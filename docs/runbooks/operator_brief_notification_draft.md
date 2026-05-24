# Operator Brief Notification Draft

## 1. 目的

TRADING-030 将 Daily Trading System Operator Brief 转换成可人工审阅的通知草稿：

- email draft Markdown
- Slack / Discord copy draft Markdown
- mobile push style short summary
- notification metadata JSON
- notification summary Markdown

本任务只生成草稿，不发送任何通知。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "notification_draft_only": true,
  "read_only": true,
  "email_sent": false,
  "gmail_draft_created": false,
  "slack_sent": false,
  "discord_sent": false,
  "mobile_push_sent": false,
  "operator_brief_executed_by_notification_draft": false,
  "pipelines_executed_by_notification_draft": false,
  "data_downloaded_by_notification_draft": false,
  "apply_executed_by_notification_draft": false,
  "rollback_executed_by_notification_draft": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 只表示草稿生成器本身可以被定时生成，不表示任何通知可以自动发送。

## 2. 生成草稿

默认读取 latest TRADING-022 operator brief：

```bash
python scripts/generate_operator_brief_notification_draft.py --date 2026-05-24
```

指定输入：

```bash
python scripts/generate_operator_brief_notification_draft.py \
  --date 2026-05-24 \
  --operator-brief-file data/derived/operator_briefs/daily_trading_system_operator_brief_2026-05-24.json \
  --include-links
```

可选输入包括 TRADING-021 digest、TRADING-023 health summary、TRADING-024 freshness summary 和 TRADING-026 scheduler dry run。显式路径只会被读取，不会运行对应生成脚本。

## 3. 输出位置

```text
data/derived/operator_briefs/notifications/
  operator_brief_notification_draft_YYYY-MM-DD.json
  operator_brief_notification_draft_YYYY-MM-DD.md
  email/operator_brief_email_draft_YYYY-MM-DD.md
  chat/operator_brief_chat_draft_YYYY-MM-DD.md
  mobile/operator_brief_mobile_summary_YYYY-MM-DD.md
  logs/operator_brief_notification_draft_run_YYYY-MM-DD.json
  logs/operator_brief_notification_draft_run_YYYY-MM-DD.md
```

## 4. draft_status

|状态|处理|
|---|---|
|`GENERATED`|草稿生成成功，可进入人工审阅。|
|`GENERATED_WITH_WARNINGS`|先阅读 warnings，通常来自 operator brief warning 或敏感字段 redaction。|
|`INPUT_MISSING`|先生成或定位 TRADING-022 operator brief。|
|`INPUT_INVALID`|修复 operator brief JSON 或确认 `task_id=TRADING-022`。|
|`SAFETY_BLOCKED`|停止发送流程，检查 operator brief safety fields。|
|`ERROR`|查看 run log 后重试。|

## 5. notification_severity

|Severity|含义|
|---|---|
|`NORMAL`|系统状态正常，通常无需人工动作。|
|`WATCH`|建议观察，发送前应确认 warning。|
|`ACTION`|存在待处理事项，发送前必须阅读 operator brief。|
|`URGENT`|需要人工关注，发送前必须核对源 brief 和关键 artifact。|
|`BLOCKED`|安全校验阻断，不应发送正常通知。|
|`UNKNOWN`|输入缺失、非法或运行异常。|

## 6. 查看草稿

- Email：`data/derived/operator_briefs/notifications/email/operator_brief_email_draft_YYYY-MM-DD.md`
- Chat：`data/derived/operator_briefs/notifications/chat/operator_brief_chat_draft_YYYY-MM-DD.md`
- Mobile：`data/derived/operator_briefs/notifications/mobile/operator_brief_mobile_summary_YYYY-MM-DD.md`
- Summary：`data/derived/operator_briefs/notifications/operator_brief_notification_draft_YYYY-MM-DD.md`

## 7. ACTION / URGENT / BLOCKED 处理

`ACTION`：

1. 打开 source operator brief。
2. 核对 pending manual actions。
3. 确认通知内容没有遗漏关键阻断原因。
4. 再决定是否人工发送。

`URGENT`：

1. 先核对 underlying operator brief 和上游 health/freshness artifact。
2. 不要只凭通知草稿做操作决策。
3. 确认是否需要电话、即时消息或其他人工升级路径。

`BLOCKED`：

1. 不发送正常通知文案。
2. 阅读 metadata 的 `safety_validation.blocking_reasons`。
3. 修复 TRADING-022 operator brief 或其输入安全异常后重新生成。

## 8. 为什么不发送通知

发送 email、创建 Gmail draft、调用 Slack / Discord webhook 或发送 mobile push 都会改变外部系统状态，且可能传播未复核的投资系统状态。TRADING-030 的边界是生成可审阅文本，真实发送必须由人工或后续独立 preflight / approval 任务控制。

## 9. 手动发送前 checklist

- [ ] `draft_status` 不是 `INPUT_MISSING`、`INPUT_INVALID`、`SAFETY_BLOCKED` 或 `ERROR`。
- [ ] `email_sent=false`、`slack_sent=false`、`discord_sent=false`、`mobile_push_sent=false`。
- [ ] 已打开并阅读 TRADING-022 source operator brief。
- [ ] 已确认 `notification_severity` 与 source brief 一致。
- [ ] 已确认草稿中没有 API key、token、password、account id 或 broker credential。
- [ ] `URGENT` / `ACTION` 通知已经人工核对，不只依赖摘要。

## 10. 内容安全与 redaction

生成器会 redacted 以下疑似敏感字段值：

- `api_key`
- `secret`
- `token`
- `password`
- `credential`
- `account_id`

出现 redaction 时，metadata 会记录 warning，`draft_status` 会变为 `GENERATED_WITH_WARNINGS`。不要手工把敏感值再补回通知草稿。

## 11. 未来真实发送接入

未来如果接入真实 delivery，也应先新增独立任务，例如 TRADING-031 delivery preflight。推荐边界：

- 先检查 recipient config。
- 检查 draft metadata safety。
- 默认仍不发送。
- 任何 Gmail draft、webhook 或 push 行为都需要单独 approval。
- delivery 任务不得绕过 TRADING-030 metadata 和 source brief 审查。
