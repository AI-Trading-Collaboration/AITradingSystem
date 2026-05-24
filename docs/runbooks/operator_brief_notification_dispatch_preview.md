# Operator Brief Notification Dispatch Preview

最后更新：2026-05-24

## 1. 目的

TRADING-032 在真实通知发送前生成只读 dry-run dispatch preview。它聚合 TRADING-031
delivery preflight、TRADING-022 operator brief 和 TRADING-030 notification draft 内容，
回答未来 dispatch 会面向哪些 channel、目标如何脱敏、subject/title/body 摘要是什么，以及当前
状态是 `WOULD_SEND`、`NEEDS_APPROVAL`、`SAFETY_BLOCKED`、`BLOCKED` 还是 `NOOP`。

它不发送任何通知，不创建 Gmail draft，不调用 SMTP、Slack、Telegram、Discord 或 webhook，
不访问外部网络，不读取 `.env` secrets，不读取私密邮箱。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "dispatch_preview_only": true,
  "read_only": true,
  "external_side_effects": false,
  "network_access_required": false,
  "secrets_required": false,
  "email_sent": false,
  "gmail_draft_created": false,
  "gmail_draft_modified": false,
  "slack_sent": false,
  "telegram_sent": false,
  "discord_sent": false,
  "webhook_called": false,
  "mobile_push_sent": false,
  "operator_brief_executed_by_dispatch_preview": false,
  "notification_draft_executed_by_dispatch_preview": false,
  "delivery_preflight_executed_by_dispatch_preview": false,
  "pipelines_executed_by_dispatch_preview": false,
  "data_downloaded_by_dispatch_preview": false,
  "apply_executed_by_dispatch_preview": false,
  "rollback_executed_by_dispatch_preview": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

## 2. 如何运行

默认按当前日期 smoke 运行：

```bash
python scripts/run_operator_brief_notification_dispatch_preview.py
```

指定日期：

```bash
python scripts/run_operator_brief_notification_dispatch_preview.py --date 2026-05-24
```

指定输入：

```bash
python scripts/run_operator_brief_notification_dispatch_preview.py \
  --date 2026-05-24 \
  --input-preflight data/derived/operator_briefs/notifications/delivery_preflight/operator_brief_notification_delivery_preflight_2026-05-24.json \
  --operator-brief-json data/derived/operator_briefs/daily_trading_system_operator_brief_2026-05-24.json
```

指定输出目录时，路径必须留在 repo root 内：

```bash
python scripts/run_operator_brief_notification_dispatch_preview.py \
  --date 2026-05-24 \
  --output-dir data/derived/operator_briefs/notifications/dispatch_preview
```

## 3. 输出位置

```text
data/derived/operator_briefs/notifications/dispatch_preview/
  operator_brief_notification_dispatch_preview_YYYY-MM-DD.json
  operator_brief_notification_dispatch_preview_YYYY-MM-DD.md
  latest.json
  latest.md
  run.log
```

## 4. 状态含义

|状态|含义|处理|
|---|---|---|
|`WOULD_SEND`|preflight 为 PASS，brief 可读，message preview 生成成功，至少一个 channel enabled，且无安全阻断。|只代表未来真实 dispatch 理论可发送；当前没有发送。|
|`NEEDS_APPROVAL`|preflight 或内容要求人工审批，例如 ACTION/URGENT、approval required 或 body 过长。|先取得人工 approval，再考虑后续真实 delivery 任务。|
|`SAFETY_BLOCKED`|preflight safety blocked、路径逃逸、`.env` 路径、疑似 secret/private key/token/password、可疑 endpoint 或其他 safety finding。|停止发送审查，修复源 artifact 或配置。|
|`BLOCKED`|必要 artifact 缺失/非法、brief 不可读、channel plan 不可形成或 message preview 不可生成。|恢复上游 artifact 后重跑 TRADING-031 和 TRADING-032。|
|`NOOP`|notification 明确 disabled，或 upstream 明确 no report / no trading day / no update。|无需发送；保留 artifact 供审计。|

## 5. 人工审核步骤

1. 打开 TRADING-032 Markdown。
2. 核对 `Final Decision`、`Preflight Summary`、`Dispatch Preview` 和 `Safety`。
3. 确认所有 `target_ref` 已脱敏，且没有完整邮箱、token、key、password 或 private key。
4. 打开 TRADING-031 preflight、TRADING-030 draft 和 TRADING-022 operator brief 交叉核对。
5. 如果状态不是 `WOULD_SEND`，不要进入真实发送任务。
6. 如果状态是 `WOULD_SEND`，仍必须由人工在独立后续任务中批准真实发送。

## 6. Dashboard 行为

Daily task dashboard 只读取 TRADING-032 artifact，展示：

- final status
- preflight status
- dispatch status
- channel count
- would_send channel count
- human_action_required
- next_recommended_action
- latest artifact path
- generated_at

Dashboard 不运行 TRADING-032 script、不重跑 TRADING-031、不运行 TRADING-030 或 TRADING-022，
不发送通知、不调用 webhook、不读取 secrets、不修改 artifact。

## 7. 故障处理

`SAFETY_BLOCKED`：

1. 查看 `safety.sensitive_content_flags`。
2. 如果是路径逃逸或 `.env` 路径，修正 CLI 参数或上游 artifact path。
3. 如果是疑似 secret，回到 TRADING-030 draft 或 TRADING-022 source artifact 修复 redaction。

`BLOCKED`：

1. 查看 `preflight_summary.reasons`。
2. 先恢复 TRADING-031 preflight 和 TRADING-022 operator brief。
3. 不要手工编辑 TRADING-032 JSON 来绕过缺失输入。

`NEEDS_APPROVAL`：

1. 核对 ACTION/URGENT 或 approval policy 原因。
2. 记录人工审批证据。
3. 在独立真实 delivery 任务中引用该审批证据；不要让 TRADING-032 自动审批。
