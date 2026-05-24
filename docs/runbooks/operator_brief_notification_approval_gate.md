# Operator Brief Notification Approval Gate

最后更新：2026-05-24

## 1. 目的

TRADING-033 在 TRADING-032 dry-run dispatch preview 之后生成只读人工审批门控 artifact。
它读取当前 dispatch preview 和可选本地 approval marker，计算稳定 preview hash，并判断是否允许
后续真实 dispatch 任务进入发送前执行阶段。

它不发送任何通知，不创建 Gmail draft，不调用 SMTP、Slack、Telegram、Discord 或 webhook，
不访问外部网络，不读取 `.env` secrets，不读取私密邮箱，不自动审批，也不修改 TRADING-032 artifact。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "approval_gate_only": true,
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
  "operator_brief_executed_by_approval_gate": false,
  "notification_draft_executed_by_approval_gate": false,
  "delivery_preflight_executed_by_approval_gate": false,
  "dispatch_preview_executed_by_approval_gate": false,
  "pipelines_executed_by_approval_gate": false,
  "data_downloaded_by_approval_gate": false,
  "apply_executed_by_approval_gate": false,
  "rollback_executed_by_approval_gate": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

## 2. 如何运行

默认按当前日期运行：

```bash
python scripts/run_operator_brief_notification_approval_gate.py
```

指定日期：

```bash
python scripts/run_operator_brief_notification_approval_gate.py --date 2026-05-24
```

指定输入：

```bash
python scripts/run_operator_brief_notification_approval_gate.py \
  --date 2026-05-24 \
  --input-preview data/derived/operator_briefs/notifications/dispatch_preview/latest.json \
  --approval-marker data/derived/operator_briefs/notifications/approval_gate/approval_marker.json
```

指定输出目录时，路径必须留在 repo root 内：

```bash
python scripts/run_operator_brief_notification_approval_gate.py \
  --date 2026-05-24 \
  --output-dir data/derived/operator_briefs/notifications/approval_gate
```

## 3. 输出位置

```text
data/derived/operator_briefs/notifications/approval_gate/
  operator_brief_notification_approval_gate_YYYY-MM-DD.json
  operator_brief_notification_approval_gate_YYYY-MM-DD.md
  latest.json
  latest.md
  run.log
```

## 4. 状态含义

|状态|含义|处理|
|---|---|---|
|`APPROVED`|TRADING-032 为 `WOULD_SEND`，approval marker 存在、`approved=true`、hash 匹配且未过期。|后续真实 dispatch 任务可读取本 gate artifact；TRADING-033 本身没有发送。|
|`APPROVAL_REQUIRED`|缺少 marker、marker 未批准、marker 缺必要字段，或 TRADING-032 仍为 `NEEDS_APPROVAL`。|人工复核 TRADING-032 Markdown 后创建新的 marker。|
|`APPROVAL_EXPIRED`|marker hash 匹配但 `expires_at` 已过期。|重新复核 preview 并创建新的 marker。|
|`APPROVAL_MISMATCH`|marker `preview_hash` 与当前 preview hash 不一致。|说明 preview 已更新；旧审批作废，必须重新审批。|
|`SAFETY_BLOCKED`|TRADING-032 safety blocked，或 marker 试图覆盖 safety block。|先修复 TRADING-032 safety finding；审批不能覆盖安全阻断。|
|`BLOCKED`|TRADING-032 缺失/非法、marker JSON 非法、hash 无法计算，或输入不可可信。|恢复有效 artifact 后重跑。|
|`NOOP`|TRADING-032 为 `NOOP`。|无需审批；保留 artifact 供审计。|

## 5. Hash 与人工 marker

TRADING-033 输出 `hashes.dispatch_preview_hash`，格式为 `sha256:<hex>`，scope 为
`canonical_dispatch_preview_json`。hash 不包含 `generated_at`、`preview_generated_at`、
`audit.created_at`、`output_artifacts` 和 run log 路径；message、channel、status、safety 或
decision 变化会改变 hash。

本阶段不提供自动审批 CLI。需要人工审批时，先打开 TRADING-032 Markdown 和 TRADING-033 Markdown，
确认内容后手工创建本地 marker：

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

推荐保存为：

```text
data/derived/operator_briefs/notifications/approval_gate/approval_marker.json
```

也可以按日期保存为：

```text
data/derived/operator_briefs/notifications/approval_gate/approvals/operator_brief_notification_approval_YYYY-MM-DD.json
```

## 6. Dashboard 行为

Daily task dashboard 只读取 TRADING-033 artifact，展示：

- approval gate status
- allowed_to_enter_dispatch
- human_action_required
- dispatch preview status
- approval marker exists
- hash matches
- expired
- generated_at
- latest artifact path
- next_recommended_action

Dashboard 不运行 TRADING-033 script、不重跑 TRADING-032、不写 approval marker、不执行 dispatch、
不访问网络、不读取 secrets、不发送通知。
