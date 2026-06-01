# TRADING-035 Notification Delivery Audit Summary

最后更新：2026-05-25

## 背景

TRADING-030 已生成 operator brief notification draft，TRADING-031 已完成 delivery preflight，TRADING-034 已生成只读 draft dispatch artifact。当前缺口是缺少统一审计摘要，把 draft、preflight、draft dispatch 和 latest 指针串成可追溯生命周期记录。

TRADING-035 只生成 audit summary，不发送通知，不创建或修改 Gmail draft，不调用 Slack / Discord / webhook / mobile push，不运行 TRADING-022/030/031/034 或任何 market、backtest、scoring、data download、broker、replay、trading pipeline。

## 目标

1. 新增 `src/ai_trading_system/trading_engine/notification_delivery_audit_summary.py`。
2. 新增 `scripts/run_notification_delivery_audit_summary.py`。
3. 输出 `data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json` 和 `.md`。
4. 输出 `data/derived/operator_briefs/notifications/delivery_audit/logs/notification_delivery_audit_summary_run_YYYY-MM-DD.json` 和 `.md`。
5. Daily task dashboard 新增只读 `Notification Delivery Audit Summary` 卡片，只读取 TRADING-035 artifact。
6. 更新 runbook、system flow、artifact catalog、task register 和测试。

## 安全边界

- `production_effect=none`
- `manual_review_only=true`
- `notification_delivery_audit_only=true`
- `read_only=true`
- 所有 email/Gmail/Slack/Discord/webhook/mobile side-effect flags 固定为 false
- 所有 upstream execution、pipeline、data download、apply、rollback、broker、replay、trading flags 固定为 false
- `safe_for_scheduler=true` 只表示 audit summary 本身可定时生成，不表示允许自动发送通知
- Dashboard 只能读取 TRADING-035 audit artifact，不运行 018B-034、TRADING-035 script 或任何上游/发送/交易路径

## Artifact 输入

1. latest TRADING-030 notification draft metadata。
2. latest TRADING-031 delivery preflight artifact。
3. TRADING-034 `draft_dispatch/latest.json`。
4. TRADING-034 dated draft dispatch artifact；如 explicit `--dispatch-file` 提供则使用该文件。
5. 可选 email/chat/mobile draft files 只用于可用性摘要，不执行发送。

当前项目 TRADING-034 已落地目录为 `data/derived/operator_briefs/notifications/draft_dispatch/`。CLI 保留显式 `--dispatch-latest-file` / `--dispatch-file`，但默认遵循现有 `draft_dispatch` 路径。

## 状态枚举

`audit_status` 只允许：

- `PASS`
- `PASS_WITH_WARNINGS`
- `INCOMPLETE`
- `MISMATCH`
- `SAFETY_BLOCKED`
- `ERROR`

`notification_lifecycle_status` 只允许：

- `DRAFT_ONLY`
- `PREFLIGHT_READY`
- `DRAFT_READY`
- `BLOCKED`
- `APPROVAL_MISMATCH`
- `SAFETY_BLOCKED`
- `INCOMPLETE`
- `UNKNOWN`

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记与需求文档|DONE|task register 新增 TRADING-035，本文记录范围、安全边界、状态枚举和验证计划。|
|2. 核心模块与 CLI|DONE|可只读加载 030/031/034 artifacts，校验 path/hash/date/latest/side effects，写 JSON/Markdown/run log。|
|3. Dashboard 只读卡片|DONE|payload 和 HTML 展示 audit_status、lifecycle、draft/preflight/dispatch、hash/latest match、side effect audit 和 alert count；import guard 证明不触发上游或发送路径。|
|4. 文档更新|DONE|runbook、system flow、artifact catalog 更新，并说明 TRADING-035 不发送通知、不创建 Gmail draft。|
|5. 测试与 smoke|DONE|专项测试覆盖 PASS、INCOMPLETE、MISMATCH、SAFETY_BLOCKED、Markdown banners、dashboard 只读和 safety invariants；完成要求中的 smoke 路径。|
|6. 收尾验证|VALIDATING|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 已通过；全仓 black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 验收标准

- TRADING-035 artifact 明确回答 draft 是否生成、preflight 是否通过、draft dispatch 是否 ready、hash/latest 是否一致、是否 approval mismatch、是否 safety blocked、是否有外部副作用、是否可进入下一阶段人工处理、生命周期是否可追溯。
- `fail_on_safety_anomaly` 在 `SAFETY_BLOCKED`、`MISMATCH`、`ERROR` 时让 CLI 非零退出。
- `allow_missing_dispatch=true` 时缺少 TRADING-034 dispatch 可输出 `PASS_WITH_WARNINGS`，但不得伪造 dispatch ready。
- 所有输出都显式写入只读安全 invariants。
- 不混入 data/derived、outputs/parameter_search、`.venv_old_20260517_194727` 既有残留。

## 进展记录

- 2026-05-25：新增并进入 IN_PROGRESS。原因：owner 要求实现 TRADING-035，将 TRADING-030/031/034 串成只读 notification delivery audit summary；当前阶段严格 audit-only/read-only，不发送通知、不创建 Gmail draft、不调用 webhook、不运行上游或交易流水线。
- 2026-05-25：进入 VALIDATING。已完成核心模块、CLI、audit JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow、artifact catalog 和专项测试；验证通过 `tests/trading_engine/test_notification_delivery_audit_summary.py`、`tests/test_daily_task_dashboard.py`、`tests/trading_engine`、全量 pytest 和 ruff；全仓 black check 仅被既有无关 `tests/test_market_data.py` baseline 阻断，未格式化该无关文件。
