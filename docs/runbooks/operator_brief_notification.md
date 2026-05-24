# Operator Brief Notification Runbook

最后更新：2026-05-25

Operator Brief notification 链路当前是只读、人工复核优先的分阶段流程：

1. TRADING-030 Notification Draft：生成 email/chat/mobile 本地草稿。
2. TRADING-031 Delivery Preflight：检查草稿、channel、redaction 和审批需求。
3. TRADING-032 Dry-run Dispatch Preview：生成发送前 dry-run plan 和 masked targets。
4. TRADING-033 Approval Gate：绑定当前 preview hash 和人工 approval 状态。
5. TRADING-034 Draft Dispatch：生成最终本地可审阅 draft dispatch artifact。
6. Future Actual Dispatch：尚未实现；未来只能读取 `DRAFT_READY` draft dispatch artifact。

各阶段 runbook：

- `docs/runbooks/operator_brief_notification_draft.md`
- `docs/runbooks/operator_brief_notification_delivery_preflight.md`
- `docs/runbooks/operator_brief_notification_dispatch_preview.md`
- `docs/runbooks/operator_brief_notification_approval_gate.md`
- `docs/runbooks/operator_brief_notification_draft_dispatch.md`

整条链路在 TRADING-034 结束前都必须保持：

- `production_effect=none`
- `manual_review_only=true`
- 不发送 email
- 不创建或修改 Gmail draft
- 不调用 Gmail API、SMTP 或 webhook
- 不访问网络
- 不读取 `.env` secrets
- 不读取真实邮箱
- 不触发 broker、replay 或 trading execution

Dashboard 只读展示各阶段 latest artifact，不运行任何 notification pipeline。
