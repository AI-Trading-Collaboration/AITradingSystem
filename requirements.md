# Requirements Index

最后更新：2026-05-24

本文件记录当前新增需求文档入口。详细任务登记仍以 `docs/task_register.md` 为准。

## TRADING-032

- 需求文档：`docs/requirements/operator_brief_notification_dispatch_preview_2026-05-24.md`
- 任务：Operator Brief Notification Dry-run Dispatch Preview
- 状态：`VALIDATING`
- 安全边界：只读 dry-run preview，不发送通知，不创建 Gmail draft，不调用 webhook，不访问外部网络，不读取 `.env` secrets，不修改 scheduler 或 production 配置。
- 主要输出：`data/derived/operator_briefs/notifications/dispatch_preview/operator_brief_notification_dispatch_preview_YYYY-MM-DD.json`、`.md`、`latest.json`、`latest.md`、`run.log`。
