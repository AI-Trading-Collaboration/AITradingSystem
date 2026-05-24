# Operator Brief Notification Draft Dispatch

最后更新：2026-05-25

## 1. 目的

TRADING-034 在 TRADING-033 approval gate 之后生成只读 draft dispatch artifact。
它读取当前 approval gate 和 dispatch preview，校验 approval status、preview hash、channel、
message 和 safety boundary，输出真实发送前最后一个本地可审阅草稿。

它不发送任何通知，不创建 Gmail draft，不调用 Gmail API、SMTP、Slack、Telegram、Discord
或 webhook，不访问外部网络，不读取 `.env` secrets，不读取私密邮箱，不自动审批，也不修改
TRADING-032 / TRADING-033 artifact 或 approval marker。

固定安全边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "draft_dispatch_only": true,
  "read_only": true,
  "external_side_effects": false,
  "network_access_required": false,
  "secrets_required": false,
  "email_sent": false,
  "gmail_draft_created": false,
  "gmail_draft_modified": false,
  "smtp_called": false,
  "webhook_called": false,
  "mobile_push_sent": false,
  "operator_brief_executed_by_draft_dispatch": false,
  "notification_draft_executed_by_draft_dispatch": false,
  "delivery_preflight_executed_by_draft_dispatch": false,
  "dispatch_preview_executed_by_draft_dispatch": false,
  "approval_gate_executed_by_draft_dispatch": false,
  "pipelines_executed_by_draft_dispatch": false,
  "data_downloaded_by_draft_dispatch": false,
  "apply_executed_by_draft_dispatch": false,
  "rollback_executed_by_draft_dispatch": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

## 2. 如何运行

默认按当前日期运行：

```bash
python scripts/run_operator_brief_notification_draft_dispatch.py
```

指定日期：

```bash
python scripts/run_operator_brief_notification_draft_dispatch.py --date 2026-05-25
```

指定输入：

```bash
python scripts/run_operator_brief_notification_draft_dispatch.py \
  --date 2026-05-25 \
  --input-approval-gate data/derived/operator_briefs/notifications/approval_gate/latest.json \
  --input-dispatch-preview data/derived/operator_briefs/notifications/dispatch_preview/latest.json
```

指定输出目录时，路径必须留在 repo root 内：

```bash
python scripts/run_operator_brief_notification_draft_dispatch.py \
  --date 2026-05-25 \
  --output-dir data/derived/operator_briefs/notifications/draft_dispatch
```

## 3. 输出位置

```text
data/derived/operator_briefs/notifications/draft_dispatch/
  operator_brief_notification_draft_dispatch_YYYY-MM-DD.json
  operator_brief_notification_draft_dispatch_YYYY-MM-DD.md
  latest.json
  latest.md
  run.log
```

## 4. 状态含义

|状态|含义|处理|
|---|---|---|
|`DRAFT_READY`|TRADING-033 为 `APPROVED`，`allowed_to_enter_dispatch=true`，TRADING-032 为 `WOULD_SEND`，preview hash 对齐，至少一个 channel draft-ready，subject/body 可生成且无安全阻断。|人工复核本地 draft dispatch artifact；未来真实发送任务只能读取该 ready artifact。|
|`APPROVAL_REQUIRED`|TRADING-033 未批准，或 TRADING-032 仍为 `NEEDS_APPROVAL`。|先完成 TRADING-033 approval gate。|
|`APPROVAL_EXPIRED`|TRADING-033 approval 已过期。|重新复核 preview 并生成新的 approval gate artifact。|
|`APPROVAL_MISMATCH`|TRADING-033 记录的 preview hash 与当前 TRADING-032 hash 不一致。|旧审批作废，重新审批当前 preview。|
|`SAFETY_BLOCKED`|上游 safety blocked、目标未脱敏、疑似 secret/private key、路径逃逸或输出路径不安全。|修复源 artifact 或路径后重跑。|
|`BLOCKED`|必要 artifact 缺失/非法、message 缺失、channel 缺失或 hash 无法计算。|恢复有效输入后重跑。|
|`NOOP`|TRADING-033 和 TRADING-032 均为 `NOOP`。|无需发送；保留 artifact 供审计。|

## 5. Hash 规则

TRADING-034 输出 `hashes.draft_hash`，格式为 `sha256:<hex>`，scope 为
`canonical_draft_dispatch_json`。hash 不包含 `generated_at`、`audit.created_at`、
`output_artifacts`、run log 路径、本地绝对路径、`draft_hash` 或 `draft_id`。

subject、body、channel、approval gate 状态、dispatch preview hash 或 final status 变化会改变
draft hash。只有 `generated_at` 变化不应改变 draft hash。

## 6. 人工审核步骤

1. 打开 TRADING-034 Markdown。
2. 核对 `Final Decision`、`Approval Gate Summary`、`Draft Summary`、`Channels` 和 `Safety`。
3. 确认 `final_status=DRAFT_READY`、`ready_for_actual_dispatch=true`，且 `approval_gate_status=APPROVED`。
4. 核对 `dispatch_preview_hash` 与 TRADING-033 / TRADING-032 对齐。
5. 检查 subject、body 和所有 channel target。target 必须脱敏，不得包含 token、key、password 或 private key。
6. 如果状态不是 `DRAFT_READY`，不要进入未来 actual dispatch。

## 7. Dashboard 行为

Daily task dashboard 只读取 TRADING-034 `latest.json`，展示：

- final status
- ready_for_actual_dispatch
- approval gate status
- channel count
- draft-ready channel count
- draft hash
- generated_at
- latest artifact path
- next recommended action

Dashboard 不运行 TRADING-034 script、不重跑 TRADING-033 / TRADING-032 / TRADING-031 / TRADING-030，
不访问网络、不读取 secrets、不修改 approval marker、不修改 draft artifact、不发送通知。
